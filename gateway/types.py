"""请求/响应数据结构与版本生命周期状态。"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class VersionState(str, Enum):
    LOADING = "LOADING"      # 加载中：拒绝新请求
    READY = "READY"          # 就绪：可服务
    UNLOADING = "UNLOADING"  # 卸载中：拒绝新请求，等待在途请求结束
    RETIRED = "RETIRED"      # 已下线：拒绝并给出可区分错误


@dataclass(frozen=True)
class InferRequest:
    """推理请求。inputs 在网关内会被拷贝，请求间不共享可变状态。"""

    model: str
    inputs: tuple[float, ...]
    version: str | None = None          # None 表示落到当前稳定版本
    tenant: str = "default"
    idempotency_key: str | None = None
    timeout_s: float = 5.0
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    enqueued_at: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class InferResponse:
    request_id: str
    model: str
    version: str
    outputs: tuple[float, ...]
    from_idempotency_cache: bool = False
