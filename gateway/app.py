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


def create_app(gateway: Gateway | None = None) -> FastAPI:
    gw = gateway or Gateway()
    app = FastAPI(title="model-serving-gateway")
    app.state.gateway = gw

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_req: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=_STATUS_BY_CODE.get(exc.code, 500), content=exc.to_dict()
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

    @app.get("/admin/snapshot")
    def snapshot() -> dict:
        return gw.snapshot()

    @app.on_event("shutdown")
    def shutdown() -> None:
        gw.close()

    return app


app = create_app()
