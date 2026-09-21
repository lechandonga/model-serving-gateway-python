"""模型版本注册表：加载/就绪/卸载生命周期与稳定版本路由。

并发约束：所有状态迁移与同一把 asyncio.Lock 下的 in_flight 计数绑定，
resolve+acquire 是原子操作，因此卸载开始后不会再有新请求进入，
也不会出现半加载/半释放状态被外部观察到。
"""
from __future__ import annotations

import asyncio
import enum
from dataclasses import dataclass, field

from .errors import ErrorCode, GatewayError


class VersionState(enum.Enum):
    LOADING = "LOADING"
    READY = "READY"
    UNLOADING = "UNLOADING"
    RETIRED = "RETIRED"


@dataclass
class VersionEntry:
    model: str
    version: str
    state: VersionState = VersionState.LOADING
    in_flight: int = 0


class VersionRegistry:
    """版本注册表。"""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], VersionEntry] = {}
        self._stable: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._drained = asyncio.Condition(self._lock)

    async def register(self, model: str, version: str) -> VersionEntry:
        """登记新版本，初始状态 LOADING。"""
        async with self._lock:
            key = (model, version)
            if key in self._entries and self._entries[key].state is not VersionState.RETIRED:
                raise GatewayError(ErrorCode.VERSION_NOT_READY, f"{model}/{version} already registered")
            entry = VersionEntry(model=model, version=version)
            self._entries[key] = entry
            return entry

    async def mark_ready(self, model: str, version: str) -> None:
        async with self._lock:
            entry = self._get(model, version)
            if entry.state is not VersionState.LOADING:
                raise GatewayError(ErrorCode.VERSION_NOT_READY, f"{model}/{version} not loading")
            entry.state = VersionState.READY
            # 首个就绪版本自动成为稳定版本，保证回退规则确定。
            self._stable.setdefault(model, version)

    async def set_stable(self, model: str, version: str) -> None:
        """切换稳定版本；目标必须 READY，切换立即生效且确定。"""
        async with self._lock:
            entry = self._get(model, version)
            if entry.state is not VersionState.READY:
                raise GatewayError(ErrorCode.VERSION_NOT_READY, f"{model}/{version} not ready")
            self._stable[model] = version

    async def resolve(self, model: str, version: str | None) -> VersionEntry:
        """解析请求目标版本并原子地占用一个 in_flight 槽位。"""
        async with self._lock:
            if not any(k[0] == model for k in self._entries):
                raise GatewayError(ErrorCode.MODEL_NOT_FOUND, f"unknown model {model!r}")
            target = version if version is not None else self._stable.get(model)
            if target is None:
                raise GatewayError(ErrorCode.VERSION_NOT_FOUND, f"model {model!r} has no stable version")
            key = (model, target)
            if key not in self._entries:
                raise GatewayError(ErrorCode.VERSION_NOT_FOUND, f"unknown version {model}/{target}")
            entry = self._entries[key]
            if entry.state is VersionState.RETIRED:
                raise GatewayError(ErrorCode.VERSION_RETIRED, f"{model}/{target} retired")
            if entry.state is not VersionState.READY:
                raise GatewayError(
                    ErrorCode.VERSION_NOT_READY, f"{model}/{target} is {entry.state.value}"
                )
            entry.in_flight += 1
            return entry

    async def release(self, entry: VersionEntry) -> None:
        """释放在途槽位；卸载等待者会被唤醒。"""
        async with self._drained:
            entry.in_flight -= 1
            if entry.in_flight == 0:
                self._drained.notify_all()

    async def unload(self, model: str, version: str) -> None:
        """卸载版本：先置 UNLOADING 拒绝新请求，等待在途结束后置 RETIRED。"""
        async with self._drained:
            entry = self._get(model, version)
            if entry.state is VersionState.RETIRED:
                return
            entry.state = VersionState.UNLOADING
            if self._stable.get(model) == version:
                del self._stable[model]
            while entry.in_flight > 0:
                await self._drained.wait()
            entry.state = VersionState.RETIRED

    async def snapshot(self) -> dict[str, dict[str, object]]:
        async with self._lock:
            return {
                f"{m}/{v}": {"state": e.state.value, "in_flight": e.in_flight}
                for (m, v), e in sorted(self._entries.items())
            }

    def _get(self, model: str, version: str) -> VersionEntry:
        key = (model, version)
        if key not in self._entries:
            raise GatewayError(ErrorCode.VERSION_NOT_FOUND, f"unknown version {model}/{version}")
        return self._entries[key]
