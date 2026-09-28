"""动态批处理器：按 (model, version, tenant) 分组合批，有界队列背压。

- 仅同模型、同版本、同租户的请求可合批；其余请求进入各自桶，绝不合并。
- 批次达到 max_batch_size 立即执行，否则等待 max_wait_s 窗口后执行。
- 全局在队请求数超过 max_queue 时以 OVERLOADED 拒绝（背压），内存占用有界。
- 单条请求输入超过 max_inputs 时以 BATCH_CONSTRAINT 拒绝，不参与合批。
- 批内单条失败（infer_fn 返回 Exception 元素）只影响该条，不影响同批其他请求。
- 每条请求的 inputs 在执行前拷贝为 tuple，请求间不共享可变状态。
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field

from .errors import ErrorCode, GatewayError
from .types import InferRequest, InferResponse


@dataclass
class _PendingItem:
    request: InferRequest
    resolved_version: str
    event: threading.Event = field(default_factory=threading.Event)
    response: InferResponse | None = None
    error: BaseException | None = None
    done: bool = False  # 在锁内标记，防止超时与执行竞争导致重复完成


class DynamicBatcher:
    def __init__(
        self,
        infer_fn,
        *,
        max_batch_size: int = 8,
        max_wait_s: float = 0.05,
        max_queue: int = 64,
        max_inputs: int = 1024,
    ) -> None:
        self._infer_fn = infer_fn
        self._max_batch_size = max_batch_size
        self._max_wait_s = max_wait_s
        self._max_queue = max_queue
        self._max_inputs = max_inputs
        self._lock = threading.Lock()
        self._queues: dict[tuple[str, str, str], deque[_PendingItem]] = defaultdict(deque)
        self._timers: dict[tuple[str, str, str], threading.Timer] = {}
        self._total_queued = 0
        self._closed = False

    # ---- 提交 ----

    def submit(self, request: InferRequest, resolved_version: str) -> InferResponse:
        """提交请求并阻塞等待结果；超限/超时/失败时抛出可区分的 GatewayError。"""
        if len(request.inputs) > self._max_inputs:
            raise GatewayError(
                ErrorCode.BATCH_CONSTRAINT,
                f"inputs length {len(request.inputs)} exceeds limit {self._max_inputs}",
            )
        item = _PendingItem(request=request, resolved_version=resolved_version)
        key = (request.model, resolved_version, request.tenant)
        with self._lock:
            if self._closed:
                raise GatewayError(ErrorCode.OVERLOADED, "batcher is closed")
            if self._total_queued >= self._max_queue:
                raise GatewayError(
                    ErrorCode.OVERLOADED,
                    f"queue full: {self._total_queued} >= {self._max_queue}",
                )
            queue = self._queues[key]
            queue.append(item)
            self._total_queued += 1
            batch = None
            if len(queue) >= self._max_batch_size:
                batch = self._pop_batch_locked(key)
            elif key not in self._timers:
                timer = threading.Timer(self._max_wait_s, self._flush, args=(key,))
                timer.daemon = True
                self._timers[key] = timer
                timer.start()
        if batch is not None:
            self._execute(key, batch)
        return self._await(item)

    def _await(self, item: _PendingItem) -> InferResponse:
        deadline = item.request.enqueued_at + item.request.timeout_s
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not item.event.wait(timeout=remaining):
            # 超时：尝试从队列摘除，释放队列容量（有界内存）
            with self._lock:
                if not item.done:
                    item.done = True
                    key = (
                        item.request.model,
                        item.resolved_version,
                        item.request.tenant,
                    )
                    queue = self._queues.get(key)
                    if queue is not None:
                        try:
                            queue.remove(item)
                            self._total_queued -= 1
                        except ValueError:
                            pass
            raise GatewayError(
                ErrorCode.REQUEST_TIMEOUT,
                f"request {item.request.request_id} timed out after {item.request.timeout_s}s",
            )
        if item.error is not None:
            raise item.error
        assert item.response is not None
        return item.response

    # ---- 批执行 ----

    def _flush(self, key: tuple[str, str, str]) -> None:
        with self._lock:
            batch = self._pop_batch_locked(key)
        if batch:
            self._execute(key, batch)

    def _pop_batch_locked(self, key: tuple[str, str, str]) -> list[_PendingItem]:
        queue = self._queues.get(key)
        if not queue:
            return []
        timer = self._timers.pop(key, None)
        if timer is not None:
            timer.cancel()
        batch: list[_PendingItem] = []
        while queue and len(batch) < self._max_batch_size:
            item = queue.popleft()
            self._total_queued -= 1
            if item.done:  # 已超时摘除失败时残留，跳过
                continue
            item.done = True
            batch.append(item)
        if queue and key not in self._timers:
            # 队列仍有剩余：重新安排窗口定时器
            timer = threading.Timer(self._max_wait_s, self._flush, args=(key,))
            timer.daemon = True
            self._timers[key] = timer
            timer.start()
        return batch

    def _execute(self, key: tuple[str, str, str], batch: list[_PendingItem]) -> None:
        model, version, _tenant = key
        # 拷贝输入，批内请求间不共享可变状态
        inputs = [tuple(item.request.inputs) for item in batch]
        try:
            results = self._infer_fn(model, version, inputs)
            if len(results) != len(batch):
                raise RuntimeError(
                    f"backend returned {len(results)} results for {len(batch)} requests"
                )
        except Exception as exc:  # 整批失败：每条各自收到相同错误，互不影响后续批次
            for item in batch:
                item.error = GatewayError(ErrorCode.BACKEND_FAILURE, str(exc))
                item.event.set()
            return
        for item, result in zip(batch, results):
            if isinstance(result, Exception):
                # 批内单条失败：仅该条失败。这是该输入自身的问题（如含 NaN），
                # 属于客户端非法输入而非后端整体故障，backend_fault=False：
                # 错误码保持 BACKEND_FAILURE 可区分于普通校验，但不计熔断/回退。
                item.error = GatewayError(
                    ErrorCode.BACKEND_FAILURE, str(result), backend_fault=False
                )
            else:
                item.response = InferResponse(
                    request_id=item.request.request_id,
                    model=model,
                    version=version,
                    outputs=tuple(result),
                )
            item.event.set()

    # ---- 关闭 ----

    def close(self) -> None:
        with self._lock:
            self._closed = True
            timers = list(self._timers.values())
            self._timers.clear()
            pending = [
                (key, self._pop_batch_locked(key))
                for key in list(self._queues.keys())
            ]
        for timer in timers:
            timer.cancel()
        for key, batch in pending:
            if batch:
                self._execute(key, batch)
