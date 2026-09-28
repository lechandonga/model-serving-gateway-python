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


class CanaryConfigBody(BaseModel):
    canary_version: str
    percentage: int = Field(ge=0, le=100)
    stable_version: str | None = None


class CanaryAdjustBody(BaseModel):
    percentage: int = Field(ge=0, le=100)


def create_app(gateway: Gateway | None = None) -> FastAPI:
    gw = gateway or Gateway()
    app = FastAPI(title="model-serving-gateway")
    app.state.gateway = gw

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_req: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=_STATUS_BY_CODE.get(exc.code, 500), content=exc.to_dict()
        )

    @app.exception_handler(KeyError)
    async def key_error_handler(_req: Request, exc: KeyError) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={"error": {"code": "CANARY_NOT_CONFIGURED", "message": str(exc.args[0])}},
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(_req: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"error": {"code": "INVALID_CANARY_CONFIG", "message": str(exc)}},
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

    # ---- 灰度发布 ----

    @app.post("/admin/models/{model}/canary")
    def configure_canary(model: str, body: CanaryConfigBody) -> dict:
        config = gw.configure_canary(
            model,
            body.canary_version,
            body.percentage,
            stable_version=body.stable_version,
        )
        return {
            "model": model,
            "stable_version": config.stable_version,
            "canary_version": config.canary_version,
            "percentage": config.percentage,
        }

    @app.post("/admin/models/{model}/canary/adjust")
    def adjust_canary(model: str, body: CanaryAdjustBody) -> dict:
        config = gw.adjust_canary_percentage(model, body.percentage)
        return {
            "model": model,
            "stable_version": config.stable_version,
            "canary_version": config.canary_version,
            "percentage": config.percentage,
        }

    @app.post("/admin/models/{model}/canary/rollback")
    def rollback_canary(model: str) -> dict:
        record = gw.rollback_canary(model)
        if record is None:
            return {"model": model, "rolled_back": False}
        return {
            "model": model,
            "rolled_back": True,
            "canary_version": record.canary_version,
            "timestamp": record.timestamp,
            "reason": record.reason,
            "failure_rate": record.failure_rate,
            "decided_requests": record.decided_requests,
        }

    @app.delete("/admin/models/{model}/canary")
    def disable_canary(model: str) -> dict:
        gw.disable_canary(model)
        return {"model": model, "canary": "disabled"}

    @app.get("/admin/models/{model}/canary/rollbacks")
    def canary_rollbacks(model: str) -> dict:
        records = gw.canary_rollback_records(model)
        return {
            "model": model,
            "rollbacks": [
                {
                    "timestamp": r.timestamp,
                    "canary_version": r.canary_version,
                    "reason": r.reason,
                    "failure_rate": r.failure_rate,
                    "decided_requests": r.decided_requests,
                }
                for r in records
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
