"""本地确定性推理后端：离线运行，结果确定，不依赖外部服务。"""
from __future__ import annotations

import asyncio
import hashlib
from typing import Any, Protocol


class InferenceBackend(Protocol):
    """推理后端协议：批量输入 -> 与输入一一对应的批量输出。

    实现必须保证：输出列表与输入列表等长且顺序对应；
    单个输入的失败以该位置的异常表达，不得影响其他位置。
    """

    async def infer_batch(
        self, model: str, version: str, inputs: list[dict[str, Any]]
    ) -> list[Any]:
        ...


def _deterministic_embedding(model: str, version: str, text: str, dims: int = 8) -> list[float]:
    """由 (model, version, text) 纯函数式推导出的确定性向量。"""
    seed = hashlib.sha256(f"{model}::{version}::{text}".encode("utf-8")).digest()
    values: list[float] = []
    counter = 0
    while len(values) < dims:
        block = hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
        for i in range(0, len(block), 4):
            if len(values) >= dims:
                break
            raw = int.from_bytes(block[i : i + 4], "big")
            values.append(round((raw / 0xFFFFFFFF) * 2.0 - 1.0, 6))
        counter += 1
    return values


class DeterministicLocalBackend:
    """确定性本地后端。

    推理结果只取决于 (model, version, 输入内容)，完全离线、可复现。
    支持 item 级错误：输入含 "fail": true 时该条目失败，不影响同批其他条目。
    """

    def __init__(self, latency_seconds: float = 0.0) -> None:
        self._latency = latency_seconds

    async def infer_batch(
        self, model: str, version: str, inputs: list[dict[str, Any]]
    ) -> list[Any]:
        if self._latency > 0:
            await asyncio.sleep(self._latency)
        results: list[Any] = []
        for item in inputs:
            # 不共享可变输入：每个条目基于拷贝计算，绝不原地修改调用方数据。
            payload = dict(item)
            if payload.get("fail"):
                results.append(RuntimeError("item requested failure"))
                continue
            text = str(payload.get("prompt", ""))
            tokens = text.split()
            results.append(
                {
                    "model": model,
                    "version": version,
                    "token_count": len(tokens),
                    "embedding": _deterministic_embedding(model, version, text),
                }
            )
        return results


class ScriptedBackend:
    """测试用后端：可编排整批失败次数与延迟，用于熔断/恢复验证。"""

    def __init__(self, inner: InferenceBackend | None = None) -> None:
        self._inner = inner or DeterministicLocalBackend()
        self.remaining_batch_failures = 0
        self.latency_seconds = 0.0
        self.calls: list[list[dict[str, Any]]] = []

    async def infer_batch(
        self, model: str, version: str, inputs: list[dict[str, Any]]
    ) -> list[Any]:
        self.calls.append([dict(i) for i in inputs])
        if self.latency_seconds > 0:
            await asyncio.sleep(self.latency_seconds)
        if self.remaining_batch_failures > 0:
            self.remaining_batch_failures -= 1
            raise RuntimeError("scripted backend failure")
        return await self._inner.infer_batch(model, version, inputs)
