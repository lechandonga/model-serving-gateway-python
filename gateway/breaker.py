"""熔断器：连续失败熔断、冷却后半开探测恢复。

语义：
- CLOSED：正常放行；连续失败达到阈值才熔断，单次抖动不会熔断；
  任意一次成功即清零连续失败计数。
- OPEN：快速失败，不放行正常流量；冷却期满后允许一个探测请求进入 HALF_OPEN。
- HALF_OPEN：同一时刻只允许一个探测；探测成功恢复 CLOSED，失败重新 OPEN。
  探测请求通过 try_acquire 返回的 is_probe 标记识别，不计入正常成功流量。
"""
from __future__ import annotations

import asyncio
import enum
import time


class BreakerState(enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, cooldown_seconds: float = 1.0) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False
        self._lock = asyncio.Lock()

    @property
    def state(self) -> BreakerState:
        return self._state

    async def try_acquire(self) -> tuple[bool, bool]:
        """返回 (是否放行, 是否探测请求)。"""
        async with self._lock:
            if self._state is BreakerState.CLOSED:
                return True, False
            if self._state is BreakerState.OPEN:
                if time.monotonic() - self._opened_at >= self.cooldown_seconds:
                    self._state = BreakerState.HALF_OPEN
                    self._probe_in_flight = True
                    return True, True
                return False, False
            # HALF_OPEN：只允许一个探测
            if not self._probe_in_flight:
                self._probe_in_flight = True
                return True, True
            return False, False

    async def record_success(self, was_probe: bool) -> None:
        async with self._lock:
            if was_probe:
                self._probe_in_flight = False
            self._consecutive_failures = 0
            self._state = BreakerState.CLOSED

    async def record_failure(self, was_probe: bool) -> None:
        async with self._lock:
            if was_probe:
                self._probe_in_flight = False
                self._state = BreakerState.OPEN
                self._opened_at = time.monotonic()
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.failure_threshold:
                self._state = BreakerState.OPEN
                self._opened_at = time.monotonic()
