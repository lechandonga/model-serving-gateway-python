"""可观测性：线程安全计数器与版本状态快照。"""

from __future__ import annotations

import threading
from collections import defaultdict


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = defaultdict(int)

    def inc(self, name: str, value: int = 1) -> None:
        raise NotImplementedError

    def snapshot(self) -> dict[str, int]:
        raise NotImplementedError
