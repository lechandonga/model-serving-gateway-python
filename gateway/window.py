"""滑动窗口统计：按版本统计请求数、成功数、真实后端失败数与各类拒绝原因。

- 每个 (model, version) 一个独立窗口，灰度与稳定版本各自统计、互不串数。
- 事件带时间戳，读取时按窗口长度裁剪过期数据；所有追加/裁剪都在窗口锁内完成，
  并发更新不漏算、不重复。
- 拒绝原因按错误码分桶记录，便于区分过载、超时、熔断等。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable


class SlidingWindow:
    """单 (model, version) 的滑动窗口计数。

    每条进入该版本的请求先记一次 request，终态再记 success / backend_failure /
    rejection 之一，因此请求数与终态数都可单独核对（在途请求计入请求数、
    暂未计入终态数，语义明确）。
    """

    def __init__(self, window_s: float = 60.0, *, time_func: Callable[[], float] = time.monotonic) -> None:
        if window_s <= 0:
            raise ValueError("window_s must be positive")
        self._window_s = window_s
        self._time = time_func
        self._lock = threading.Lock()
        # (timestamp, kind, reason)；kind ∈ request/success/backend_failure/rejection
        self._events: deque[tuple[float, str, str | None]] = deque()

    def _prune_locked(self, now: float) -> None:
        cutoff = now - self._window_s
        while self._events and self._events[0][0] <= cutoff:
            self._events.popleft()

    def _append(self, kind: str, reason: str | None = None) -> None:
        with self._lock:
            now = self._time()
            self._prune_locked(now)
            self._events.append((now, kind, reason))

    def record_request(self) -> None:
        self._append("request")

    def record_success(self) -> None:
        self._append("success")

    def record_backend_failure(self) -> None:
        self._append("backend_failure")

    def record_rejection(self, reason: str) -> None:
        self._append("rejection", reason)

    def summary(self) -> dict:
        with self._lock:
            self._prune_locked(self._time())
            requests = successes = backend_failures = 0
            rejections: dict[str, int] = {}
            for _ts, kind, reason in self._events:
                if kind == "request":
                    requests += 1
                elif kind == "success":
                    successes += 1
                elif kind == "backend_failure":
                    backend_failures += 1
                elif kind == "rejection":
                    if reason is not None:
                        rejections[reason] = rejections.get(reason, 0) + 1
        return {
            "window_s": self._window_s,
            "requests": requests,
            "successes": successes,
            "backend_failures": backend_failures,
            "rejections": dict(sorted(rejections.items())),
        }


class WindowRegistry:
    """按 (model, version) 持有彼此隔离的滑动窗口。"""

    def __init__(self, window_s: float = 60.0, *, time_func: Callable[[], float] = time.monotonic) -> None:
        self._window_s = window_s
        self._time = time_func
        self._lock = threading.Lock()
        self._windows: dict[tuple[str, str], SlidingWindow] = {}

    def get(self, model: str, version: str) -> SlidingWindow:
        with self._lock:
            win = self._windows.get((model, version))
            if win is None:
                win = SlidingWindow(self._window_s, time_func=self._time)
                self._windows[(model, version)] = win
            return win

    def summary_all(self) -> dict[str, dict]:
        with self._lock:
            keys = list(self._windows.keys())
        return {
            f"{m}/{v}": self._windows[(m, v)].summary()
            for m, v in sorted(keys)
        }
