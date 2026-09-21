"""幂等性：相同键只产生一次推理效果，记录按版本隔离且容量有界。"""
import asyncio
import logging

from gateway.gateway import Gateway, InferRequest

log = logging.getLogger(__name__)


async def test_same_key_produces_single_effect(gateway: Gateway):
    await gateway.load_version("m", "v1")
    calls = 0
    original = gateway.backend.infer_batch

    async def counting(model, version, inputs):
        nonlocal calls
        calls += len(inputs)
        return await original(model, version, inputs)

    gateway.backend.infer_batch = counting  # type: ignore[assignment]
    req = InferRequest(model="m", input={"prompt": "idem"}, idempotency_key="key-1")
    log.info("submitting 5 concurrent requests with idempotency_key=%s", req.idempotency_key)
    responses = await asyncio.gather(*[gateway.infer(req) for _ in range(5)])
    assert calls == 1, f"expected exactly one inference, got {calls}"
    results = {str(r.result) for r in responses}
    assert len(results) == 1, "all responses must be identical"
    assert sum(1 for r in responses if r.replayed) == 4
    log.info("single backend effect, %d replays returned identical result", 4)


async def test_idempotency_isolated_per_version(gateway: Gateway):
    await gateway.load_version("m", "v1")
    await gateway.load_version("m", "v2")
    r1 = await gateway.infer(
        InferRequest(model="m", version="v1", input={"prompt": "x"}, idempotency_key="k")
    )
    r2 = await gateway.infer(
        InferRequest(model="m", version="v2", input={"prompt": "x"}, idempotency_key="k")
    )
    log.info("same key on v1 vs v2: replayed=%s/%s", r1.replayed, r2.replayed)
    assert not r2.replayed, "same key on different version must not replay"
    assert r1.result != r2.result


async def test_unloaded_version_records_are_not_reused(gateway: Gateway):
    await gateway.load_version("m", "v1")
    r1 = await gateway.infer(
        InferRequest(model="m", version="v1", input={"prompt": "x"}, idempotency_key="k")
    )
    await gateway.unload_version("m", "v1")
    assert await gateway.idempotency.size() == 0
    await gateway.load_version("m", "v1")  # 同名版本重新上线
    r2 = await gateway.infer(
        InferRequest(model="m", version="v1", input={"prompt": "x"}, idempotency_key="k")
    )
    log.info("after unload+reload: replayed=%s (old record must not be reused)", r2.replayed)
    assert not r2.replayed


async def test_idempotency_capacity_is_bounded(gateway: Gateway):
    await gateway.load_version("m", "v1")
    cap = gateway.config.idempotency_capacity
    for i in range(cap * 3):
        await gateway.infer(
            InferRequest(model="m", input={"prompt": f"p{i}"}, idempotency_key=f"k{i}")
        )
    size = await gateway.idempotency.size()
    log.info("after %d keys, store size=%d (capacity=%d)", cap * 3, size, cap)
    assert size <= cap
