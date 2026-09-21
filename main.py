"""本地模型推理网关 HTTP 入口。

请求协议见 docs/API.md；所有失败响应都带可区分的 code 字段。
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from gateway.backend import DeterministicLocalBackend
from gateway.errors import ErrorCode, GatewayError
from gateway.gateway import Gateway, GatewayConfig, InferRequest

ERROR_TO_HTTP = {
    ErrorCode.MODEL_NOT_FOUND: 404,
    ErrorCode.VERSION_NOT_FOUND: 404,
    ErrorCode.VERSION_RETIRED: 410,
    ErrorCode.VERSION_NOT_FOUND: 404,
    ErrorCode.VERSION_NOT_READY: 409,
    ErrorCode.OVERLOADED: 429,
    ErrorCode.QUEUE_TIMEOUT: 504,
    ErrorCode.CIRCUIT_OPEN: 503,
    ErrorCode.BACKEND_FAILURE: 502,
}


class InferIn(BaseModel):
    model: str
    input: dict[str, Any]
    tenant: str = "default"
    version: str | None = None
    idempotency_key: str | None = None
    timeout: float | None = Field(default=None, gt=0)


class VersionIn(BaseModel):
    model: str
    version: str


def create_app(gateway: Gateway | None = None) -> FastAPI:
    gw = gateway or Gateway(DeterministicLocalBackend(), GatewayConfig())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await gw.close()

    app = FastAPI(title="model-serving-gateway", lifespan=lifespan)
    app.state.gateway = gw

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=ERROR_TO_HTTP.get(exc.code, 500),
            content={"error": {"code": exc.code.value, "message": exc.message}},
        )

    @app.post("/v1/infer")
    async def infer(body: InferIn) -> dict[str, Any]:
        resp = await gw.infer(
            InferRequest(
                model=body.model,
                input=body.input,
                tenant=body.tenant,
                version=body.version,
                idempotency_key=body.idempotency_key,
                timeout=body.timeout,
            )
        )
        return {
            "model": resp.model,
            "version": resp.version,
            "replayed": resp.replayed,
            "result": resp.result,
        }

    @app.post("/admin/versions/load")
    async def load_version(body: VersionIn) -> dict[str, str]:
        await gw.load_version(body.model, body.version)
        return {"status": "ready", "model": body.model, "version": body.version}

    @app.post("/admin/versions/stable")
    async def set_stable(body: VersionIn) -> dict[str, str]:
        await gw.set_stable(body.model, body.version)
        return {"status": "stable", "model": body.model, "version": body.version}

    @app.post("/admin/versions/unload")
    async def unload_version(body: VersionIn) -> dict[str, str]:
        await gw.unload_version(body.model, body.version)
        return {"status": "retired", "model": body.model, "version": body.version}

    @app.get("/v1/versions")
    async def versions() -> dict[str, Any]:
        return {"versions": await gw.registry.snapshot()}

    @app.get("/v1/metrics")
    async def metrics() -> dict[str, Any]:
        return await gw.snapshot()

    return app


app = create_app()
