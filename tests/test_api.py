"""HTTP 协议层：错误码可区分、指标与版本快照可观测。"""
import logging

import pytest
from httpx import ASGITransport, AsyncClient

from gateway.backend import DeterministicLocalBackend
from gateway.gateway import Gateway, GatewayConfig
from main import create_app

log = logging.getLogger(__name__)


@pytest.fixture
async def client():
    gw = Gateway(DeterministicLocalBackend(), GatewayConfig(max_wait_ms=1))
    app = create_app(gw)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await gw.load_version("m", "v1")
        yield c
    await gw.close()


async def test_infer_happy_path(client: AsyncClient):
    r = await client.post("/v1/infer", json={"model": "m", "input": {"prompt": "hello world"}})
    assert r.status_code == 200
    body = r.json()
    log.info("infer response: %s", body)
    assert body["version"] == "v1" and body["result"]["token_count"] == 2


async def test_error_codes_are_distinguishable(client: AsyncClient):
    cases = [
        ({"model": "ghost", "input": {}}, 404, "MODEL_NOT_FOUND"),
        ({"model": "m", "version": "vX", "input": {}}, 404, "VERSION_NOT_FOUND"),
    ]
    for payload, status, code in cases:
        r = await client.post("/v1/infer", json=payload)
        log.info("payload=%s -> %s %s", payload, r.status_code, r.json()["error"]["code"])
        assert r.status_code == status
        assert r.json()["error"]["code"] == code
    await client.post("/admin/versions/unload", json={"model": "m", "version": "v1"})
    r = await client.post("/v1/infer", json={"model": "m", "version": "v1", "input": {}})
    assert r.status_code == 410 and r.json()["error"]["code"] == "VERSION_RETIRED"
    r = await client.post("/v1/infer", json={"model": "m", "input": {}})
    assert r.status_code == 404 and r.json()["error"]["code"] == "VERSION_NOT_FOUND"


async def test_metrics_and_versions_endpoints(client: AsyncClient):
    await client.post("/v1/infer", json={"model": "m", "input": {"prompt": "a"}})
    await client.post("/v1/infer", json={"model": "ghost", "input": {}})
    m = (await client.get("/v1/metrics")).json()
    v = (await client.get("/v1/versions")).json()
    log.info("metrics=%s versions=%s", m, v)
    assert m["counters"]["requests_total"]["model=m,tenant=default"] == 1
    assert m["counters"]["rejected_total"]["reason=MODEL_NOT_FOUND"] == 1
    assert m["versions"]["m/v1"]["state"] == "READY"
    assert v["versions"]["m/v1"]["state"] == "READY"
