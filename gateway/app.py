"""FastAPI 接入层：请求协议与错误码到 HTTP 状态的映射。"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import ErrorCode, GatewayError
from .gateway import Gateway
from .types import InferRequest

_STATUS_BY_CODE = {
    ErrorCode.MODEL_NOT_FOUND: 404,
    ErrorCode.VERSION_NOT_FOUND: 404,
    ErrorCode.VERSION_RETIRED: 410,
    ErrorCode.VERSION_NOT_READY: 409,
    ErrorCode.OVERLOADED: 429,
    ErrorCode.REQUEST_TIMEOUT: 504,
    ErrorCode.CIRCUIT_OPEN: 503,
    ErrorCode.BACKEND_FAILURE: 502,
    ErrorCode.BATCH_CONSTRAINT: 400,
}


class InferRequestBody(BaseModel):
    model: str
    inputs: list[float]
    version: str | None = None
    tenant: str = "default"
    idempotency_key: str | None = None
    timeout_s: float = Field(default=5.0, gt=0)


class CanaryRequestBody(BaseModel):
    stable_version: str
    canary_version: str
    percent: int = Field(ge=0, le=100)


def create_app(gateway: Gateway | None = None) -> FastAPI:
    gw = gateway or Gateway()
    app = FastAPI(title="model-serving-gateway")
    app.state.gateway = gw

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_req: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=_STATUS_BY_CODE.get(exc.code, 500), content=exc.to_dict()
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(_req: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"error": {"code": "INVALID_ARGUMENT", "message": str(exc)}},
        )

    @app.post("/v1/infer")
    def infer(body: InferRequestBody) -> dict:
        resp = gw.infer(
            InferRequest(
                model=body.model,
                inputs=tuple(body.inputs),
                version=body.version,
                tenant=body.tenant,
                idempotency_key=body.idempotency_key,
                timeout_s=body.timeout_s,
            )
        )
        return {
            "request_id": resp.request_id,
            "model": resp.model,
            "version": resp.version,
            "outputs": list(resp.outputs),
            "from_idempotency_cache": resp.from_idempotency_cache,
        }

    @app.post("/admin/models/{model}/versions/{version}/load")
    def load_version(model: str, version: str, set_stable: bool = False) -> dict:
        gw.load_version(model, version, set_stable=set_stable)
        return {"model": model, "version": version, "state": "READY"}

    @app.post("/admin/models/{model}/versions/{version}/retire")
    def retire_version(model: str, version: str, timeout_s: float = 10.0) -> dict:
        gw.retire_version(model, version, timeout_s=timeout_s)
        return {"model": model, "version": version, "state": "RETIRED"}

    @app.put("/admin/models/{model}/canary")
    def enable_canary(model: str, body: CanaryRequestBody) -> dict:
        """开启或更新灰度：指定稳定版本、灰度版本与灰度比例（0..100）。"""
        cfg = gw.enable_canary(
            model, body.stable_version, body.canary_version, body.percent
        )
        return {
            "model": model,
            "stable_version": cfg.stable_version,
            "canary_version": cfg.canary_version,
            "percent": cfg.percent,
            "epoch": cfg.epoch,
        }

    @app.post("/admin/models/{model}/canary/percent")
    def adjust_canary_percent(model: str, percent: int) -> dict:
        """随时在线调整灰度比例；percent=0 即全部干净回到稳定版本。"""
        cfg = gw.adjust_canary_percent(model, percent)
        return {
            "model": model,
            "canary_version": cfg.canary_version,
            "percent": cfg.percent,
            "epoch": cfg.epoch,
        }

    @app.delete("/admin/models/{model}/canary")
    def disable_canary(model: str, reason: str = "manual") -> dict:
        """关闭灰度（下线灰度版本或人工回退），全部流量回到稳定版本。"""
        gw.disable_canary(model, reason=reason)
        return {"model": model, "canary": None}

    @app.get("/admin/models/{model}/rollbacks")
    def rollback_history(model: str) -> dict:
        return {
            "model": model,
            "rollbacks": [
                {
                    "timestamp": r.timestamp,
                    "canary_version": r.canary_version,
                    "stable_version": r.stable_version,
                    "reason": r.reason,
                    "failure_rate": r.failure_rate,
                    "samples": r.samples,
                    "epoch": r.epoch,
                }
                for r in gw.rollback_history(model)
            ],
        }

    @app.get("/admin/snapshot")
    def snapshot() -> dict:
        return gw.snapshot()

    @app.on_event("shutdown")
    def shutdown() -> None:
        gw.close()

    return app


app = create_app()
