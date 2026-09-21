"""熔断器：连续失败触发熔断，熔断期快速失败，冷却后半开探测恢复。"""

from __future__ import annotations

import threading
import time
from enum import Enum


class BreakerState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout_s: float = 1.0) -> None:
        self._failure_threshold = failure_threshold
        self._recovery_timeout_s = recovery_timeout_s
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._half_open_inflight = False

    @property
    def state(self) -> BreakerState:
        raise NotImplementedError

    def allow_request(self) -> bool:
        """是否放行请求。半开状态只允许一个探测请求。"""
        raise NotImplementedError

    def record_success(self, *, probe: bool = False) -> None:
        raise NotImplementedError

    def record_failure(self, *, probe: bool = False) -> None:
        raise NotImplementedError
