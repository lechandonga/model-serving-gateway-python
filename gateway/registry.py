"""模型版本注册表：生命周期管理与稳定版本路由。

状态机：LOADING -> READY -> UNLOADING -> RETIRED
- LOADING / UNLOADING：明确拒绝新请求（VERSION_NOT_READY）。
- UNLOADING：等待在途请求归零后才转为 RETIRED 并释放资源。
- 所有状态迁移在同一把锁内完成，并发切换不会出现半加载/半释放状态。
- 稳定版本回退规则确定：稳定指针失效时，按版本号字典序取最大的 READY 版本。
"""

from __future__ import annotations

import threading

from .errors import ErrorCode, GatewayError
from .types import VersionState


class VersionRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        # model -> version -> state
        self._versions: dict[str, dict[str, VersionState]] = {}
        # model -> 当前稳定版本（显式指针）
        self._stable: dict[str, str] = {}
        # (model, version) -> 在途请求数
        self._inflight: dict[tuple[str, str], int] = {}

    # ---- 生命周期 ----

    def register(self, model: str, version: str) -> None:
        """注册新版本，初始状态 LOADING。"""
        with self._lock:
            versions = self._versions.setdefault(model, {})
            if version in versions and versions[version] != VersionState.RETIRED:
                raise GatewayError(
                    ErrorCode.VERSION_NOT_READY,
                    f"version {model}/{version} already registered",
                )
            versions[version] = VersionState.LOADING
            self._inflight[(model, version)] = 0

    def mark_ready(self, model: str, version: str, *, set_stable: bool = False) -> None:
        with self._lock:
            self._require_state(model, version, VersionState.LOADING)
            self._versions[model][version] = VersionState.READY
            if set_stable or model not in self._stable:
                self._stable[model] = version

    def retire(self, model: str, version: str, *, timeout_s: float = 10.0) -> None:
        """READY -> UNLOADING，等待在途请求结束 -> RETIRED。"""
        with self._cond:
            self._require_state(model, version, VersionState.READY)
            self._versions[model][version] = VersionState.UNLOADING
            # 稳定指针失效：回退规则在切换后保持确定（见 resolve）
            if self._stable.get(model) == version:
                del self._stable[model]
            key = (model, version)
            drained = self._cond.wait_for(
                lambda: self._inflight.get(key, 0) == 0, timeout=timeout_s
            )
            if not drained:
                # 回滚为 READY，避免卡在半释放状态
                self._versions[model][version] = VersionState.READY
                raise GatewayError(
                    ErrorCode.REQUEST_TIMEOUT,
                    f"retire {model}/{version} timed out waiting for inflight requests",
                )
            self._versions[model][version] = VersionState.RETIRED
            self._inflight.pop(key, None)
            self._cond.notify_all()

    # ---- 路由与在途计数 ----

    def require_ready(self, model: str, version: str) -> None:
        """校验某版本存在且 READY，否则抛出与 resolve 一致的可区分错误。"""
        with self._lock:
            versions = self._versions.get(model)
            if versions is None:
                raise GatewayError(ErrorCode.MODEL_NOT_FOUND, f"unknown model: {model}")
            state = versions.get(version)
            if state is None:
                raise GatewayError(
                    ErrorCode.VERSION_NOT_FOUND, f"unknown version: {model}/{version}"
                )
            if state == VersionState.RETIRED:
                raise GatewayError(
                    ErrorCode.VERSION_RETIRED, f"version retired: {model}/{version}"
                )
            if state != VersionState.READY:
                raise GatewayError(
                    ErrorCode.VERSION_NOT_READY,
                    f"version {model}/{version} is {state.value}, not serving",
                )

    def resolve(self, model: str, version: str | None) -> tuple[str, str]:
        """解析 (model, version)；version 为 None 时按确定规则落到稳定版本。"""
        with self._lock:
            versions = self._versions.get(model)
            if versions is None:
                raise GatewayError(ErrorCode.MODEL_NOT_FOUND, f"unknown model: {model}")
            if version is None:
                version = self._fallback_locked(model)
            state = versions.get(version)
            if state is None:
                raise GatewayError(
                    ErrorCode.VERSION_NOT_FOUND, f"unknown version: {model}/{version}"
                )
            if state == VersionState.RETIRED:
                raise GatewayError(
                    ErrorCode.VERSION_RETIRED, f"version retired: {model}/{version}"
                )
            if state != VersionState.READY:
                raise GatewayError(
                    ErrorCode.VERSION_NOT_READY,
                    f"version {model}/{version} is {state.value}, not serving",
                )
            return model, version

    def _fallback_locked(self, model: str) -> str:
        """稳定版本回退：优先稳定指针，否则取字典序最大的 READY 版本（确定性）。"""
        versions = self._versions[model]
        stable = self._stable.get(model)
        if stable is not None and versions.get(stable) == VersionState.READY:
            return stable
        ready = sorted(v for v, s in versions.items() if s == VersionState.READY)
        if not ready:
            raise GatewayError(
                ErrorCode.VERSION_NOT_READY, f"model {model} has no READY version"
            )
        return ready[-1]

    def acquire(self, model: str, version: str) -> None:
        """在途请求 +1，仅 READY 状态可获取；与 retire 的等待互斥。"""
        with self._cond:
            state = self._versions.get(model, {}).get(version)
            if state != VersionState.READY:
                code = (
                    ErrorCode.VERSION_RETIRED
                    if state == VersionState.RETIRED
                    else ErrorCode.VERSION_NOT_READY
                )
                raise GatewayError(code, f"version {model}/{version} not serving")
            self._inflight[(model, version)] = self._inflight.get((model, version), 0) + 1

    def release(self, model: str, version: str) -> None:
        """在途请求 -1，唤醒可能正在等待卸载的 retire。"""
        with self._cond:
            key = (model, version)
            self._inflight[key] = self._inflight.get(key, 1) - 1
            self._cond.notify_all()

    # ---- 观测 ----

    def snapshot(self) -> dict:
        with self._lock:
            return {
                model: {
                    "stable": self._stable.get(model),
                    "versions": {v: s.value for v, s in versions.items()},
                    "inflight": {
                        v: self._inflight.get((model, v), 0) for v in versions
                    },
                }
                for model, versions in self._versions.items()
            }

    # ---- 内部 ----

    def _require_state(self, model: str, version: str, expected: VersionState) -> None:
        state = self._versions.get(model, {}).get(version)
        if state is None:
            raise GatewayError(
                ErrorCode.VERSION_NOT_FOUND, f"unknown version: {model}/{version}"
            )
        if state != expected:
            raise GatewayError(
                ErrorCode.VERSION_NOT_READY,
                f"version {model}/{version} is {state.value}, expected {expected.value}",
            )
