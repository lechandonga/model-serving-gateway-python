"""动态批量执行器：同一 (model, version, tenant) 的并发请求合并执行。

保证：
- 每个 Batcher 实例只服务一个 (model, version, tenant) 组合，
  不同版本/租户的请求物理上不可能被合并。
- 队列有界：超过 max_queue 立即以 OVERLOADED 拒绝，内存占用有界。
- 队列等待超时：入队超过 queue_timeout 的请求以 QUEUE_TIMEOUT 失败。
- 批内失败隔离：单个条目的异常只影响对应请求的 future。
- 不共享可变输入：提交与执行前均做深拷贝。
"""
from __future__ import annotations

import asyncio
import copy
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .errors import ErrorCode, GatewayError

RunBatch = Callable[[list[dict[str, Any]]], Awaitable[list[Any]]]

_STOP = object()


@dataclass
class _Item:
    payload: dict[str, Any]
    future: asyncio.Future
    deadline: float  # monotonic 截止时间


class Batcher:
    """单 (model, version, tenant) 组合的动态批量执行器。"""

    def __init__(
        self,
        model: str,
        version: str,
        tenant: str,
        run_batch: RunBatch,
        max_batch_size: int = 8,
        max_wait_ms: float = 5.0,
        max_queue: int = 64,
        queue_timeout: float = 1.0,
    ) -> None:
        if max_batch_size < 1 or max_queue < 1:
            raise ValueError("max_batch_size and max_queue must be >= 1")
        self.model = model
        self.version = version
        self.tenant = tenant
        self._run_batch = run_batch
        self.max_batch_size = max_batch_size
        self.max_wait_ms = max_wait_ms
        self.max_queue = max_queue
        self.queue_timeout = queue_timeout
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self._accepting = True
        self._worker: asyncio.Task | None = None

    def start(self) -> None:
        self._worker = asyncio.create_task(self._run(), name=f"batcher:{self.model}/{self.version}/{self.tenant}")

    async def submit(self, payload: dict[str, Any]) -> Any:
        """提交一个请求并等待其独立结果。"""
        if not self._accepting:
            raise GatewayError(ErrorCode.OVERLOADED, "batcher is closing")
        loop = asyncio.get_running_loop()
        item = _Item(
            payload=copy.deepcopy(payload),  # 不共享调用方的可变输入
            future=loop.create_future(),
            deadline=time.monotonic() + self.queue_timeout,
        )
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            raise GatewayError(ErrorCode.OVERLOADED, "batch queue is full") from None
        try:
            # 排队等待本身受 queue_timeout 约束：超时及时失败，
            # 不依赖 worker 何时取到该条目，等待内存占用有界。
            return await asyncio.wait_for(asyncio.shield(item.future), self.queue_timeout)
        except asyncio.TimeoutError:
            item.future.cancel()  # worker 稍后会跳过已取消的条目
            raise GatewayError(ErrorCode.QUEUE_TIMEOUT, "queue wait exceeded") from None
        except asyncio.CancelledError:
            item.future.cancel()  # 调用方整体超时/取消，条目不再有效
            raise

    async def close(self) -> None:
        """停止接收新请求，排空已入队请求后退出。"""
        self._accepting = False
        if self._worker is not None:
            await self._queue.put(_STOP)
            await self._worker

    async def _run(self) -> None:
        while True:
            first = await self._queue.get()
            if first is _STOP:
                return
            batch: list[_Item] = [first]
            deadline = asyncio.get_running_loop().time() + self.max_wait_ms / 1000.0
            while len(batch) < self.max_batch_size:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    nxt = await asyncio.wait_for(self._queue.get(), remaining)
                except asyncio.TimeoutError:
                    break
                if nxt is _STOP:
                    await self._queue.put(_STOP)  # 留给下一轮循环处理退出
                    break
                batch.append(nxt)
            await self._execute(batch)

    async def _execute(self, batch: list[_Item]) -> None:
        now = time.monotonic()
        live: list[_Item] = []
        for item in batch:
            if item.future.cancelled() or item.future.done():
                continue
            if now > item.deadline:
                item.future.set_exception(
                    GatewayError(ErrorCode.QUEUE_TIMEOUT, "queue wait exceeded")
                )
                continue
            live.append(item)
        if not live:
            return
        # 执行前再次深拷贝，后端拿不到调用方可变状态。
        payloads = [copy.deepcopy(i.payload) for i in live]
        try:
            results = await self._run_batch(payloads)
        except Exception as exc:  # 整批失败：每个请求独立收到错误
            for item in live:
                if not item.future.done():
                    item.future.set_exception(
                        GatewayError(ErrorCode.BACKEND_FAILURE, str(exc))
                    )
            return
        if len(results) != len(live):
            for item in live:
                if not item.future.done():
                    item.future.set_exception(
                        GatewayError(ErrorCode.BACKEND_FAILURE, "backend returned misaligned results")
                    )
            return
        for item, result in zip(live, results):
            if item.future.done():
                continue
            if isinstance(result, Exception):
                # 批内单条失败只影响该请求
                item.future.set_exception(
                    GatewayError(ErrorCode.BACKEND_FAILURE, str(result))
                )
            else:
                item.future.set_result(result)
