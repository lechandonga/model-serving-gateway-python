"""本地确定性推理后端：离线运行，结果确定，支持故障注入用于测试。"""

from __future__ import annotations

import hashlib
import threading


class DeterministicBackend:
    """基于哈希的确定性“模型”：输出完全由 (model, version, inputs) 决定。

    可通过 fail_next(n) 注入连续 n 次失败，用于熔断/降级测试。
    """

    def __init__(self, seed: str = "local") -> None:
        self._seed = seed
        self._lock = threading.Lock()
        self._fail_remaining = 0

    def fail_next(self, n: int) -> None:
        """让接下来的 n 次 infer_batch 调用抛出 RuntimeError。"""
        raise NotImplementedError

    def infer_batch(self, model: str, version: str, batch: list[tuple[float, ...]]) -> list[tuple[float, ...]]:
        """对一批输入做确定性推理，逐条返回结果。"""
        raise NotImplementedError

    def _score(self, model: str, version: str, inputs: tuple[float, ...]) -> tuple[float, ...]:
        digest = hashlib.sha256(
            (self._seed + "|" + model + "|" + version + "|" + ",".join(map(repr, inputs))).encode()
        ).digest()
        return tuple(b / 255.0 for b in digest[:4])
