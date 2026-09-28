"""滑动窗口统计：按 (model, version) 统计窗口内请求/成功/后端失败/各类拒绝。

- 所有事件在同一把锁内追加，并发下不漏算不重复算。
- 结局分三类：success（后端成功）、failure（真实后端失败）、rejection（网关拒绝）。
- 后端失败率只在拿到后端确定结果的样本（success + failure）上计算；
  客户端非法输入、等待超时、背压、熔断等拒绝样本不进分母也不进分子，
  避免少量坏请求或排队抖动把灰度误判为故障。
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass(frozen=True)
class _Event:
    """窗口内的一条请求结局事件。kind: success / failure / rejection。"""

    ts: float
    kind: str
    reason: str | None = None


class SlidingWindowStats:
    """按 (model, version) 键控的有界滑动窗口计数器。

    - window_s：统计窗口长度；只保留最近 window_s 秒的事件。
    - time_func 可注入时钟，便于单测确定性地推进窗口。
    - 每个事件在锁内追加并顺带淘汰过期事件，占用内存有上界。
    """

    def __init__(self, window_s: float = 10.0, time_func=time.monotonic) -> None:
        if window_s <= 0:
            raise ValueError("window_s must be positive")
        self._window = window_s
        self._now = time_func
        self._lock = threading.Lock()
        self._events: dict[tuple[str, str], deque[_Event]] = defaultdict(deque)

    # ---- 记录（结局互斥，每条请求恰好记一次） ----

    def record_success(self, model: str, version: str) -> None:
        self._append((model, version), _Event(self._now(), "success"))

    def record_backend_failure(self, model: str, version: str) -> None:
        self._append((model, version), _Event(self._now(), "failure"))

    def record_rejection(self, model: str, version: str, code: str) -> None:
        self._append((model, version), _Event(self._now(), "rejection", reason=code))

    def _append(self, key: tuple[str, str], event: _Event) -> None:
        with self._lock:
            self._prune_locked(key, event.ts)
            self._events[key].append(event)

    # ---- 查询 ----

    def window_view(self, model: str, version: str) -> dict:
        """返回某版本当前窗口视图。"""
        key = (model, version)
        with self._lock:
            self._prune_locked(key, self._now())
            events = list(self._events.get(key, ()))
        return self._summarize(model, version, events)

    def snapshot(self) -> dict:
        """返回全部 (model, version) 的窗口视图，键形如 "m/v1"。"""
        with self._lock:
            now = self._now()
            keys = list(self._events.keys())
            views: dict[str, dict] = {}
            for key in keys:
                self._prune_locked(key, now)
                views[f"{key[0]}/{key[1]}"] = self._summarize(
                    key[0], key[1], list(self._events.get(key, ()))
                )
            return views

    # ---- 内部 ----

    def _prune_locked(self, key: tuple[str, str], now: float) -> None:
        dq = self._events.get(key)
        if dq is None:
            return
        cutoff = now - self._window
        while dq and dq[0].ts < cutoff:
            dq.popleft()
        if not dq:
            # 无在窗事件：移除空桶，内存占用随版本/时间有界
            self._events.pop(key, None)

    def _summarize(
        self, model: str, version: str, events: list[_Event]
    ) -> dict:
        success = 0
        failures = 0
        rejections: dict[str, int] = defaultdict(int)
        for e in events:
            if e.kind == "success":
                success += 1
            elif e.kind == "failure":
                failures += 1
            else:
                rejections[e.reason or "UNKNOWN"] += 1
        decided = success + failures
        return {
            "model": model,
            "version": version,
            "window_s": self._window,
            "requests": len(events),
            "success": success,
            "backend_failures": failures,
            "rejections": dict(rejections),
            "decided": decided,
            # 真实后端失败率：仅在拿到后端确定结果的样本上计算
            "failure_rate": (failures / decided) if decided else 0.0,
        }
