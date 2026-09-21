"""模型版本注册表：生命周期管理与稳定版本路由。"""

from __future__ import annotations

import threading

from .types import VersionState


class VersionRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        # model -> version -> state
        self._versions: dict[str, dict[str, VersionState]] = {}
        # model -> 当前稳定版本
        self._stable: dict[str, str] = {}
        # (model, version) -> 在途请求数
        self._inflight: dict[tuple[str, str], int] = {}

    def register(self, model: str, version: str, *, set_stable: bool = False) -> None:
        """注册新版本，初始状态 LOADING。"""
        raise NotImplementedError

    def mark_ready(self, model: str, version: str, *, set_stable: bool = False) -> None:
        raise NotImplementedError

    def retire(self, model: str, version: str, *, timeout_s: float = 10.0) -> None:
        """进入 UNLOADING，等待在途请求结束后转为 RETIRED。"""
        raise NotImplementedError

    def resolve(self, model: str, version: str | None) -> tuple[str, str]:
        """解析 (model, version)；version 为 None 时落到当前稳定版本。

        返回 (model, resolved_version)，不可服务时抛出对应 GatewayError。
        """
        raise NotImplementedError

    def acquire(self, model: str, version: str) -> None:
        """在途请求 +1，仅 READY 状态可获取。"""
        raise NotImplementedError

    def release(self, model: str, version: str) -> None:
        """在途请求 -1，可能唤醒卸载等待。"""
        raise NotImplementedError

    def snapshot(self) -> dict:
        raise NotImplementedError
