"""过载背压、超时与熔断恢复。"""
import asyncio
import logging

import pytest

from gateway.backend import DeterministicLocalBackend, ScriptedBackend
from gateway.errors import ErrorCode, GatewayError
from gateway.gateway import Gateway, GatewayConfig, InferRequest

log = logging.getLogger(__name__)


async def test_overload_rejection_is_distinguishable(gateway: Gateway):
    await gateway.load_version("m", "v1")
    backend_started = asyncio.Event()
    release = asyncio.Event()
    original = gateway.backend.infer_batch

    async def blocking(model, version, inputs):
        backend_started.set()
        await release.wait()
        return await original(model, version, inputs)

    gateway.backend.infer_batch = blocking  # type: ignore[assignment]
    tasks: list[asyncio.Task] = []
    try:
        # 阶段 1：提交 max_batch_size 个，worker 取走并阻塞在后端
        for i in range(gateway.config.max_batch_size):
            tasks.append(
                asyncio.create_task(gateway.infer(InferRequest(model="m", input={"prompt": f"p{i}"})))
            )
        await asyncio.wait_for(backend_started.wait(), 2.0)
        # 阶段 2：再提交 max_queue 个，恰好占满队列（worker 阻塞中不再消费）
        for i in range(gateway.config.max_queue):
            tasks.append(
                asyncio.create_task(gateway.infer(InferRequest(model="m", input={"prompt": f"q{i}"})))
            )
        await asyncio.sleep(0.05)  # 确保全部入队
        log.info("worker holds a batch, queue full; submitting overflow")
        with pytest.raises(GatewayError) as e:
            await gateway.infer(InferRequest(model="m", input={"prompt": "overflow"}))
        assert e.value.code is ErrorCode.OVERLOADED
        log.info("overflow rejected with %s (distinct from timeout/backend errors)", e.value.code)
    finally:
        release.set()
    n = len(tasks)
    results = await asyncio.gather(*tasks, return_exceptions=True)
    ok = [r for r in results if not isinstance(r, Exception)]
    log.info("after release: %d/%d queued requests completed", len(ok), n)
    assert len(ok) == n  # 背压拒绝不拖累已接受的请求
    snap = await gateway.snapshot()
    assert snap["counters"]["rejected_total"]["reason=OVERLOADED"] >= 1


async def test_queue_timeout_is_distinguishable(fast_config: GatewayConfig):
    cfg = GatewayConfig(
        **{**fast_config.__dict__, "queue_timeout": 0.05, "request_timeout": 1.0}
    )
    gw = Gateway(DeterministicLocalBackend(), cfg)
    await gw.load_version("m", "v1")
    release = asyncio.Event()
    original = gw.backend.infer_batch

    async def blocking(model, version, inputs):
        await release.wait()
        return await original(model, version, inputs)

    gw.backend.infer_batch = blocking  # type: ignore[assignment]
    log.info("queue_timeout=%.2fs with blocked backend", cfg.queue_timeout)
    try:
        with pytest.raises(GatewayError) as e:
            await gw.infer(InferRequest(model="m", input={"prompt": "will time out"}))
        assert e.value.code is ErrorCode.QUEUE_TIMEOUT
        log.info("queue wait exceeded -> %s", e.value.code)
    finally:
        release.set()
    snap = await gw.snapshot()
    assert snap["counters"]["rejected_total"]["reason=QUEUE_TIMEOUT"] == 1
    await gw.close()


async def test_circuit_breaker_opens_and_recovers():
    backend = ScriptedBackend()
    cfg = GatewayConfig(
        max_wait_ms=1, breaker_failure_threshold=3, breaker_cooldown_seconds=0.05
    )
    gw = Gateway(backend, cfg)
    await gw.load_version("m", "v1")

    backend.remaining_batch_failures = 2
    for i in range(2):
        with pytest.raises(GatewayError) as e:
            await gw.infer(InferRequest(model="m", input={"prompt": f"f{i}"}))
        assert e.value.code is ErrorCode.BACKEND_FAILURE
    breaker = gw._breakers[("m", "v1")]
    log.info("after 2 consecutive failures (threshold=3): state=%s", breaker.state.value)
    assert breaker.state.value == "CLOSED", "single jitter must not trip the breaker"

    backend.remaining_batch_failures = 10
    with pytest.raises(GatewayError):
        await gw.infer(InferRequest(model="m", input={"prompt": "f3"}))
    assert breaker.state.value == "OPEN"
    calls_before = len(backend.calls)
    with pytest.raises(GatewayError) as e:
        await gw.infer(InferRequest(model="m", input={"prompt": "fast-fail"}))
    assert e.value.code is ErrorCode.CIRCUIT_OPEN
    assert len(backend.calls) == calls_before, "open circuit must fail fast without backend call"
    log.info("circuit OPEN: fast fail with %s, backend untouched", e.value.code)

    backend.remaining_batch_failures = 0
    await asyncio.sleep(0.06)
    resp = await gw.infer(InferRequest(model="m", input={"prompt": "probe"}))
    assert resp.result["token_count"] == 1
    assert breaker.state.value == "CLOSED"
    snap = await gw.snapshot()
    probes = snap["counters"]["breaker_probe_total"]["model=m,version=v1"]
    log.info("recovered via probe; probe count=%d (not counted as normal success traffic)", probes)
    assert probes == 1
    await gw.close()


async def test_failed_probe_reopens_circuit():
    backend = ScriptedBackend()
    cfg = GatewayConfig(
        max_wait_ms=1, breaker_failure_threshold=1, breaker_cooldown_seconds=0.05
    )
    gw = Gateway(backend, cfg)
    await gw.load_version("m", "v1")
    backend.remaining_batch_failures = 100
    with pytest.raises(GatewayError):
        await gw.infer(InferRequest(model="m", input={"prompt": "x"}))
    await asyncio.sleep(0.06)
    with pytest.raises(GatewayError) as e:
        await gw.infer(InferRequest(model="m", input={"prompt": "probe fails"}))
    assert e.value.code is ErrorCode.BACKEND_FAILURE
    breaker = gw._breakers[("m", "v1")]
    log.info("failed probe -> state=%s (re-opened, not stuck)", breaker.state.value)
    assert breaker.state.value == "OPEN"
    await gw.close()
