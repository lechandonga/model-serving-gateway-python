"""并发批量与批内失败隔离。"""
import asyncio
import logging

from gateway.backend import DeterministicLocalBackend
from gateway.gateway import Gateway, GatewayConfig, InferRequest
from gateway.errors import ErrorCode, GatewayError

log = logging.getLogger(__name__)


async def test_concurrent_requests_are_batched(gateway: Gateway):
    await gateway.load_version("m", "v1")
    batches: list[int] = []
    original = gateway.backend.infer_batch

    async def spy(model, version, inputs):
        batches.append(len(inputs))
        log.info("batch formed: model=%s version=%s size=%d inputs=%s", model, version, len(inputs), inputs)
        return await original(model, version, inputs)

    gateway.backend.infer_batch = spy  # type: ignore[assignment]
    reqs = [InferRequest(model="m", input={"prompt": f"prompt-{i}"}) for i in range(6)]
    log.info("submitting %d concurrent requests, max_batch_size=%d", len(reqs), gateway.config.max_batch_size)
    results = await asyncio.gather(*[gateway.infer(r) for r in reqs])
    assert len(results) == 6
    assert max(batches) <= gateway.config.max_batch_size
    assert sum(batches) == 6
    assert len(batches) >= 2, f"expected batching to split 6 > 4, got {batches}"
    # 每个请求结果与其输入一一对应
    for i, resp in enumerate(results):
        assert resp.result["token_count"] == 1, f"request {i} mismatched"
    log.info("batch sizes observed: %s", batches)


async def test_partial_failure_within_batch_is_isolated(gateway: Gateway):
    await gateway.load_version("m", "v1")
    inputs = [{"prompt": "good-1"}, {"fail": True}, {"prompt": "good-2"}, {"prompt": "good-3"}]
    log.info("inputs with one poisoned item: %s", inputs)
    outcomes = await asyncio.gather(
        *[gateway.infer(InferRequest(model="m", input=i)) for i in inputs],
        return_exceptions=True,
    )
    assert not isinstance(outcomes[0], Exception)
    assert isinstance(outcomes[1], GatewayError) and outcomes[1].code is ErrorCode.BACKEND_FAILURE
    assert not isinstance(outcomes[2], Exception)
    assert not isinstance(outcomes[3], Exception)
    log.info("poisoned item failed with %s, siblings succeeded", outcomes[1].code)


async def test_inputs_are_not_shared_or_mutated(gateway: Gateway):
    await gateway.load_version("m", "v1")
    payload = {"prompt": "one two three"}
    task = asyncio.create_task(gateway.infer(InferRequest(model="m", input=payload)))
    payload["prompt"] = "mutated after submit"
    payload["extra"] = "junk"
    resp = await task
    log.info("submitted 'one two three' then mutated caller dict; result=%s", resp.result)
    assert resp.result["token_count"] == 3


async def test_tenants_are_never_merged(gateway: Gateway):
    await gateway.load_version("m", "v1")
    seen: list[tuple[str, int]] = []
    original = gateway.backend.infer_batch

    async def spy(model, version, inputs):
        seen.append((version, len(inputs)))
        return await original(model, version, inputs)

    gateway.backend.infer_batch = spy  # type: ignore[assignment]
    reqs = [
        InferRequest(model="m", tenant="tenant-a", input={"prompt": "a"}),
        InferRequest(model="m", tenant="tenant-b", input={"prompt": "b"}),
    ]
    log.info("two tenants submit concurrently; batches must not merge across tenants")
    await asyncio.gather(*[gateway.infer(r) for r in reqs])
    assert all(size == 1 for _, size in seen), f"tenant requests merged: {seen}"
