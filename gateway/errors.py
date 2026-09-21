"""网关错误类型：所有失败原因必须可区分。"""
from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    MODEL_NOT_FOUND = "MODEL_NOT_FOUND"
    VERSION_NOT_FOUND = "VERSION_NOT_FOUND"
    VERSION_RETIRED = "VERSION_RETIRED"
    VERSION_NOT_READY = "VERSION_NOT_READY"
    OVERLOADED = "OVERLOADED"
    QUEUE_TIMEOUT = "QUEUE_TIMEOUT"
    CIRCUIT_OPEN = "CIRCUIT_OPEN"
    BACKEND_FAILURE = "BACKEND_FAILURE"


class GatewayError(Exception):
    """携带可区分错误码的网关异常。"""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
