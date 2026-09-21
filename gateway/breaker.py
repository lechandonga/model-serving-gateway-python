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
    """连续 failure_threshold 次失败后熔断 recovery_timeout_s。

    - OPEN 期间 allow_request 直接拒绝（快速失败）。
    - 冷却结束后进入 HALF_OPEN，只放行一个探测请求；
      探测成功则闭合，探测失败则重新熔断并重新计时。
    - 成功会清零连续失败计数，因此单次抖动不会永久熔断。
    - 探测流量通过 probe=True 标记，不计入正常成功统计。
    """

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
        with self._lock:
            self._maybe_half_open()
            return self._state

    def _maybe_half_open(self) -> None:
        if (
            self._state == BreakerState.OPEN
            and time.monotonic() - self._opened_at >= self._recovery_timeout_s
        ):
            self._state = BreakerState.HALF_OPEN
            self._half_open_inflight = False

    def allow_request(self) -> tuple[bool, bool]:
        """返回 (是否放行, 是否为探测请求)。半开状态只放行一个探测。"""
        with self._lock:
            self._maybe_half_open()
            if self._state == BreakerState.CLOSED:
                return True, False
            if self._state == BreakerState.OPEN:
                return False, False
            # HALF_OPEN
            if self._half_open_inflight:
                return False, False
            self._half_open_inflight = True
            return True, True

    def record_success(self, *, probe: bool = False) -> None:
        with self._lock:
            if probe:
                # 探测成功：闭合熔断器
                self._state = BreakerState.CLOSED
                self._consecutive_failures = 0
                self._half_open_inflight = False
            else:
                self._consecutive_failures = 0

    def record_failure(self, *, probe: bool = False) -> None:
        with self._lock:
            if probe:
                # 探测失败：重新熔断并重新计时
                self._state = BreakerState.OPEN
                self._opened_at = time.monotonic()
                self._half_open_inflight = False
                self._consecutive_failures = self._failure_threshold
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._failure_threshold:
                self._state = BreakerState.OPEN
                self._opened_at = time.monotonic()
