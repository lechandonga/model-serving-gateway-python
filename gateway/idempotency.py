"""幂等记录：相同幂等键只产生一次推理效果并返回一致结果。

约束：
- 记录键包含 (model, version, tenant, key)，天然按版本隔离；
  版本下线时 drop_version 清除其全部记录，不复用旧版本结果。
- 容量受限（LRU 逐出），内存占用有界；进行中的记录不参与逐出。
- 并发相同键：首个请求为 leader 执行推理，其余请求等待同一 future，
  每个等待者拿到的是结果的深拷贝，不共享可变状态。
"""
from __future__ import annotations

import asyncio
import copy
from collections import OrderedDict
from typing import Any

Key = tuple[str, str, str, str]  # (model, version, tenant, idempotency_key)


class _Record:
    __slots__ = ("future", "done")

    def __init__(self) -> None:
        self.future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.done = False


class IdempotencyStore:
    def __init__(self, capacity: int = 1024) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self._records: OrderedDict[Key, _Record] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get_or_begin(self, key: Key) -> tuple[_Record, bool]:
        """返回 (记录, 是否 leader)。leader 负责执行推理并调用 complete/fail。"""
        async with self._lock:
            rec = self._records.get(key)
            if rec is not None:
                self._records.move_to_end(key)
                return rec, False
            rec = _Record()
            self._records[key] = rec
            self._evict_locked()
            return rec, True

    async def complete(self, key: Key, record: _Record, result: Any) -> None:
        async with self._lock:
            record.done = True
            if not record.future.done():
                # 存深拷贝，之后每次取用再深拷贝，调用方之间互不可见。
                record.future.set_result(copy.deepcopy(result))

    async def fail(self, key: Key, record: _Record, error: BaseException) -> None:
        """leader 失败：移除记录，让后续相同键请求可以重试。"""
        async with self._lock:
            self._records.pop(key, None)
            if not record.future.done():
                record.future.set_exception(error)

    async def result_of(self, record: _Record) -> Any:
        """等待并返回结果的独立拷贝。"""
        value = await asyncio.shield(record.future)
        return copy.deepcopy(value)

    async def drop_version(self, model: str, version: str) -> int:
        """版本下线：清除该版本全部幂等记录，返回清除条数。"""
        async with self._lock:
            doomed = [k for k in self._records if k[0] == model and k[1] == version]
            for k in doomed:
                rec = self._records.pop(k)
                if not rec.future.done():
                    rec.future.cancel()
            return len(doomed)

    async def size(self) -> int:
        async with self._lock:
            return len(self._records)

    def _evict_locked(self) -> None:
        while len(self._records) > self.capacity:
            # 跳过进行中的记录，避免让等待者失去结果。
            for k, rec in list(self._records.items()):
                if rec.done:
                    del self._records[k]
                    break
            else:
                break
