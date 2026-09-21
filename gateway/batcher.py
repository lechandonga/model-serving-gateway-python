"""动态批处理器：按 (model, version, tenant) 分组合批，有界队列背压。"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field

from .types import InferRequest, InferResponse


@dataclass
class _PendingItem:
    request: InferRequest
    event: threading.Event = field(default_factory=threading.Event)
    response: InferResponse | None = None
    error: BaseException | None = None


class DynamicBatcher:
    """按 (model, version, tenant) 分桶的批处理器。

    - 批次大小达到 max_batch_size 或等待窗口超过 max_wait_s 即触发执行。
    - 队列长度超过 max_queue 时以 OVERLOADED 拒绝（背压）。
    - 批内单条失败不影响其他条目。
    """

    def __init__(
        self,
        infer_fn,
        *,
        max_batch_size: int = 8,
        max_wait_s: float = 0.05,
        max_queue: int = 64,
    ) -> None:
        self._infer_fn = infer_fn
        self._max_batch_size = max_batch_size
        self._max_wait_s = max_wait_s
        self._max_queue = max_queue
        self._lock = threading.Lock()
        self._queues: dict[tuple[str, str, str], deque[_PendingItem]] = defaultdict(deque)
        self._timers: dict[tuple[str, str, str], threading.Timer] = {}
        self._total_queued = 0
        self._closed = False

    def submit(self, request: InferRequest, resolved_version: str) -> InferResponse:
        """提交请求并阻塞等待结果；超限时抛 GatewayError。"""
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError
