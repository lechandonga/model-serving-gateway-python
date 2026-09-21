"""并发批量执行与批内部分失败。"""

from __future__ import annotations

import math
import threading

import pytest

from gateway import ErrorCode, GatewayError, InferRequest
from gateway.gateway import Gateway

from .conftest import log_decision


@pytest.fixture
def gw():
    g = Gateway(max_batch_size=4, max_wait_s=0.3, max_queue=64)
    g.load_version("m", "v1", set_stable=True)
    yield g
    g.close()


def test_concurrent_requests_are_batched(gw):
    """8 个同桶并发请求应被合并为少量批次，且结果与逐条执行一致。"""
    barrier = threading.Barrier(8)
    results: dict[int, object] = {}
    errors: dict[int, BaseException] = {}

    def worker(i: int) -> None:
        req = InferRequest(model="m", inputs=(float(i),), tenant="t")
        log_decision("submit", request_id=req.request_id[:8], inputs=req.inputs, tenant="t")
        barrier.wait()
        try:
            results[i] = gw.infer(req)
        except GatewayError as e:  # pragma: no cover
            errors[i] = e

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(results) == 8
    # 结果确定性：与直接逐条推理一致
    for i, resp in results.items():
        expected = gw.backend.infer_batch("m", "v1", [(float(i),)])[0]
        assert resp.outputs == tuple(expected)
    batches = gw.metrics.snapshot()["batches.executed"]
    log_decision("batching decision", total_requests=8, batches_executed=batches,
                 max_batch_size=4)
    assert 2 <= batches <= 4  # 被合并，而非逐条执行


def test_different_tenants_and_versions_not_merged(gw):
    """不同租户/版本的请求不得合并到同一批。"""
    gw.load_version("m", "v2")
    # 每批最多 4 条，但 4 个请求分属 4 个桶，必须各自成批
    reqs = [
        InferRequest(model="m", inputs=(1.0,), tenant="a", version="v1"),
        InferRequest(model="m", inputs=(1.0,), tenant="b", version="v1"),
        InferRequest(model="m", inputs=(1.0,), tenant="a", version="v2"),
        InferRequest(model="m", inputs=(1.0,), tenant="b", version="v2"),
    ]
    before = gw.metrics.snapshot().get("batches.executed", 0)
    results = [gw.infer(r) for r in reqs]
    after = gw.metrics.snapshot()["batches.executed"]
    log_decision("isolation decision", buckets=4, batches_executed=after - before)
    assert after - before == 4  # 每个桶独立成批
    assert {r.version for r in results} == {"v1", "v2"}


def test_partial_failure_within_batch(gw):
    """批内单条失败不影响同批其他请求。"""
    barrier = threading.Barrier(4)
    outcomes: dict[str, object] = {}

    def worker(name: str, x: float) -> None:
        req = InferRequest(model="m", inputs=(x,), tenant="t")
        log_decision("submit", name=name, inputs=req.inputs)
        barrier.wait()
        try:
            outcomes[name] = ("ok", gw.infer(req).outputs)
        except GatewayError as e:
            outcomes[name] = ("err", e.code)

    threads = [
        threading.Thread(target=worker, args=(f"good{i}", float(i))) for i in range(3)
    ]
    threads.append(threading.Thread(target=worker, args=("bad", math.nan)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log_decision("partial failure", outcomes={k: v[0] for k, v in outcomes.items()})
    assert outcomes["bad"] == ("err", ErrorCode.BACKEND_FAILURE)
    for i in range(3):
        assert outcomes[f"good{i}"][0] == "ok"


def test_requests_do_not_share_mutable_input_state(gw):
    """请求输入被拷贝，批内互不共享可变状态。"""
    inputs = [1.0, 2.0]
    resp = gw.infer(InferRequest(model="m", inputs=tuple(inputs)))
    inputs[0] = 999.0  # 事后修改不影响已得结果
    again = gw.infer(InferRequest(model="m", inputs=(1.0, 2.0)))
    assert resp.outputs == again.outputs
