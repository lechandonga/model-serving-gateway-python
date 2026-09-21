"""可观测性：请求、拒绝、批量、超时、熔断与版本状态的计数与快照。

所有计数更新都在同一把 asyncio.Lock 下完成，并发更新不会丢失或重复。
"""
from __future__ import annotations

import asyncio
from typing import Any


class Metrics:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        # (指标名, 标签对) -> 计数
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}

    async def inc(self, name: str, amount: int = 1, **labels: str) -> None:
        key = (name, tuple(sorted(labels.items())))
        async with self._lock:
            self._counters[key] = self._counters.get(key, 0) + amount

    async def snapshot(self, versions: dict[str, dict[str, object]] | None = None) -> dict[str, Any]:
        async with self._lock:
            counters: dict[str, dict[str, int]] = {}
            for (name, labels), value in sorted(self._counters.items()):
                label_str = ",".join(f"{k}={v}" for k, v in labels) or "-"
                counters.setdefault(name, {})[label_str] = value
        return {"counters": counters, "versions": versions or {}}
