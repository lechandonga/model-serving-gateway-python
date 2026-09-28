"""网关错误类型：每种失败原因都有可区分的错误码。"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    MODEL_NOT_FOUND = "MODEL_NOT_FOUND"          # 模型不存在
    VERSION_NOT_FOUND = "VERSION_NOT_FOUND"      # 版本未知
    VERSION_RETIRED = "VERSION_RETIRED"          # 版本已下线
    VERSION_NOT_READY = "VERSION_NOT_READY"      # 版本加载中/卸载中
    OVERLOADED = "OVERLOADED"                    # 队列/等待窗口超限，背压拒绝
    REQUEST_TIMEOUT = "REQUEST_TIMEOUT"          # 等待批次或执行超时
    CIRCUIT_OPEN = "CIRCUIT_OPEN"                # 熔断中，快速失败
    BACKEND_FAILURE = "BACKEND_FAILURE"          # 后端推理失败
    BATCH_CONSTRAINT = "BATCH_CONSTRAINT"        # 超出批次约束，无法合批


class GatewayError(Exception):
    """所有网关错误的基类，携带可区分的错误码。"""

    def __init__(
        self, code: ErrorCode, message: str, *, backend_fault: bool = True
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        # 是否为“真实后端故障”。同为 BACKEND_FAILURE 错误码的批内单条非法输入
        # （如 NaN）属于客户端坏请求，backend_fault=False：它不应计入熔断与
        # 灰度自动回退，否则少数坏请求就能把版本打回去。
        self.backend_fault = backend_fault

    def to_dict(self) -> dict:
        return {"error": {"code": self.code.value, "message": self.message}}
