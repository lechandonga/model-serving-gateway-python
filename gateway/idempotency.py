"""幂等存储：按 (model, version, tenant) 隔离，容量有界，LRU 淘汰。"""

from __future__ import annotations

import threading
from collections import OrderedDict

from .types import InferResponse


class IdempotencyStore:
    """相同幂等标识只产生一次推理效果并返回一致结果。

    - 记录按 (model, version, tenant, key) 隔离：版本切换后不会命中旧版本结果；
      多租户共用同一标识时也各算各的，绝不会把 A 租户的结果返回给 B 租户。
    - 容量有界，超出时按 LRU 淘汰，内存占用有上界。
    - 版本下线时通过 evict_version 清除该版本全部记录。
    """

    def __init__(self, capacity: int = 1024) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._lock = threading.Lock()
        self._records: OrderedDict[
            tuple[str, str, str, str], InferResponse
        ] = OrderedDict()

    def get(
        self, model: str, version: str, tenant: str, key: str
    ) -> InferResponse | None:
        with self._lock:
            k = (model, version, tenant, key)
            resp = self._records.get(k)
            if resp is not None:
                self._records.move_to_end(k)
            return resp

    def put(
        self,
        model: str,
        version: str,
        tenant: str,
        key: str,
        response: InferResponse,
    ) -> None:
        with self._lock:
            k = (model, version, tenant, key)
            self._records[k] = response
            self._records.move_to_end(k)
            while len(self._records) > self._capacity:
                self._records.popitem(last=False)

    def evict_version(self, model: str, version: str) -> None:
        """版本下线时清除其全部幂等记录，防止复用旧版本结果。"""
        with self._lock:
            doomed = [k for k in self._records if k[0] == model and k[1] == version]
            for k in doomed:
                del self._records[k]

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)
