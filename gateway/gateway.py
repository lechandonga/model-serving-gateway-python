"""网关门面：组合注册表、熔断器、幂等存储、批处理器与指标。"""

from __future__ import annotations

from .batcher import DynamicBatcher
from .breaker import CircuitBreaker
from .errors import GatewayError
from .idempotency import IdempotencyStore
from .metrics import Metrics
from .registry import VersionRegistry
from .types import InferRequest, InferResponse


class Gateway:
    def __init__(self) -> None:
        raise NotImplementedError

    def infer(self, request: InferRequest) -> InferResponse:
        raise NotImplementedError
