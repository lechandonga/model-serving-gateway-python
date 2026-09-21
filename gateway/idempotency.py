"""幂等存储：按 (model, version) 隔离，容量有界，LRU 淘汰。"""

from __future__ import annotations

import threading
from collections import OrderedDict

from .types import InferResponse


class IdempotencyStore:
    def __init__(self, capacity: int = 1024) -> None:
        self._capacity = capacity
        self._lock = threading.Lock()
        # (model, version, key) -> InferResponse，按插入/访问顺序淘汰
        self._records: OrderedDict[tuple[str, str, str], InferResponse] = OrderedDict()

    def get(self, model: str, version: str, key: str) -> InferResponse | None:
        raise NotImplementedError

    def put(self, model: str, version: str, key: str, response: InferResponse) -> None:
        raise NotImplementedError

    def evict_version(self, model: str, version: str) -> None:
        """版本下线时清除其全部幂等记录，防止复用旧结果。"""
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError
