"""版本路由、生命周期与切换的确定性。"""
import asyncio
import logging

import pytest

from gateway.errors import ErrorCode, GatewayError
from gateway.gateway import Gateway, InferRequest

log = logging.getLogger(__name__)


async def test_unspecified_version_falls_back_to_stable(gateway: Gateway):
    await gateway.load_version("m", "v1")
    await gateway.load_version("m", "v2")
    r = await gateway.infer(InferRequest(model="m", input={"prompt": "x"}))
    log.info("no version specified -> resolved to %s (first ready = stable)", r.version)
    assert r.version == "v1"
    await gateway.set_stable("m", "v2")
    r2 = await gateway.infer(InferRequest(model="m", input={"prompt": "x"}))
    log.info("after set_stable(v2) -> resolved to %s", r2.version)
    assert r2.version == "v2"
    # 回退规则确定：连续请求结果一致
    versions = {
        (await gateway.infer(InferRequest(model="m", input={"prompt": "x"}))).version
        for _ in range(5)
    }
    assert versions == {"v2"}


async def test_unknown_and_retired_versions_are_distinguishable(gateway: Gateway):
    await gateway.load_version("m", "v1")
    with pytest.raises(GatewayError) as e1:
        await gateway.infer(InferRequest(model="ghost", input={}))
    with pytest.raises(GatewayError) as e2:
        await gateway.infer(InferRequest(model="m", version="vX", input={}))
    await gateway.unload_version("m", "v1")
    with pytest.raises(GatewayError) as e3:
        await gateway.infer(InferRequest(model="m", version="v1", input={}))
    codes = {e1.value.code, e2.value.code, e3.value.code}
    log.info("error codes: %s", {c.value for c in codes})
    assert codes == {ErrorCode.MODEL_NOT_FOUND, ErrorCode.VERSION_NOT_FOUND, ErrorCode.VERSION_RETIRED}


async def test_unload_waits_for_in_flight_then_rejects(gateway: Gateway):
    await gateway.load_version("m", "v1")
    await gateway.load_version("m", "v2")
    started = asyncio.Event()
    release = asyncio.Event()
    original = gateway.backend.infer_batch

    async def blocking(model, version, inputs):
        started.set()
        await release.wait()
        return await original(model, version, inputs)

    gateway.backend.infer_batch = blocking  # type: ignore[assignment]
    in_flight = asyncio.create_task(
        gateway.infer(InferRequest(model="m", version="v1", input={"prompt": "slow"}))
    )
    await started.wait()
    unload_task = asyncio.create_task(gateway.unload_version("m", "v1"))
    await asyncio.sleep(0.05)
    assert not unload_task.done(), "unload must wait for in-flight request"
    # 卸载期间新请求被拒绝
    with pytest.raises(GatewayError) as e:
        await gateway.infer(InferRequest(model="m", version="v1", input={"prompt": "new"}))
    assert e.value.code is ErrorCode.VERSION_NOT_READY
    log.info("during unload: new request rejected with %s", e.value.code)
    release.set()
    resp = await in_flight
    assert resp.version == "v1"
    await unload_task
    snap = await gateway.registry.snapshot()
    log.info("after unload: %s", snap)
    assert snap["m/v1"]["state"] == "RETIRED"
    # 幂等记录随版本清除
    assert await gateway.idempotency.size() == 0


async def test_version_switch_serves_both_sides_consistently(gateway: Gateway):
    await gateway.load_version("m", "v1")
    await gateway.load_version("m", "v2")
    before = await gateway.infer(InferRequest(model="m", version="v1", input={"prompt": "same"}))
    await gateway.set_stable("m", "v2")
    after_old = await gateway.infer(InferRequest(model="m", version="v1", input={"prompt": "same"}))
    after_new = await gateway.infer(InferRequest(model="m", version="v2", input={"prompt": "same"}))
    log.info("v1 result stable across switch: %s", before.result == after_old.result)
    assert before.result == after_old.result  # 同版本结果确定
    assert before.result != after_new.result  # 不同版本结果可区分
