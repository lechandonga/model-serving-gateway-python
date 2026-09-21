"""幂等性：相同标识只产生一次推理效果，记录有界且随版本隔离。"""

from __future__ import annotations

import threading

import pytest

from gateway import InferRequest
from gateway.backend import DeterministicBackend
from gateway.gateway import Gateway

from .conftest import log_decision


class CountingBackend(DeterministicBackend):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0
        self._lock = threading.Lock()

    def infer_batch(self, model, version, batch):
        with self._lock:
            self.calls += 1
        return super().infer_batch(model, version, batch)


@pytest.fixture
def gw():
    backend = CountingBackend()
    g = Gateway(backend=backend, max_batch_size=1, max_wait_s=0.01,
                idempotency_capacity=4)
    g.load_version("m", "v1", set_stable=True)
    yield g, backend
    g.close()


def test_same_key_produces_single_effect_and_consistent_result(gw):
    g, backend = gw
    req = lambda: InferRequest(model="m", inputs=(1.0,), idempotency_key="k1")
    r1 = g.infer(req())
    calls_after_first = backend.calls
    r2 = g.infer(req())
    r3 = g.infer(req())
    log_decision("idempotency decision", backend_calls=backend.calls,
                 first_cached=r1.from_idempotency_cache,
                 replay_cached=r2.from_idempotency_cache)
    assert backend.calls == calls_after_first  # 重放不再触发推理
    assert r1.outputs == r2.outputs == r3.outputs
    assert not r1.from_idempotency_cache
    assert r2.from_idempotency_cache and r3.from_idempotency_cache


def test_concurrent_same_key_executes_once(gw):
    g, backend = CountingBackend(), None
    # 用较慢窗口让并发请求重叠
    g2 = Gateway(backend=CountingBackend(), max_batch_size=1, max_wait_s=0.05)
    g2.load_version("m", "v1", set_stable=True)
    barrier = threading.Barrier(6)
    results: list = []

    def worker() -> None:
        barrier.wait()
        results.append(
            g2.infer(InferRequest(model="m", inputs=(7.0,), idempotency_key="k")))
    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    outputs = {r.outputs for r in results}
    log_decision("concurrent idempotency", responses=len(results),
                 distinct_outputs=len(outputs),
                 backend_calls=g2.backend.calls)
    assert len(outputs) == 1  # 所有等待者拿到一致结果
    assert g2.backend.calls == 1  # 只产生一次推理效果
    g2.close()


def test_records_bounded_and_isolated_by_version(gw):
    g, _ = gw
    for i in range(6):  # 容量为 4，超出部分被淘汰
        g.infer(InferRequest(model="m", inputs=(float(i),), idempotency_key=f"k{i}"))
    log_decision("capacity decision", size=len(g.idempotency), capacity=4)
    assert len(g.idempotency) == 4

    g.load_version("m", "v2", set_stable=True)
    r_v2 = g.infer(InferRequest(model="m", inputs=(0.0,), idempotency_key="k0"))
    # 相同 key 在不同版本下不复用旧结果
    assert not r_v2.from_idempotency_cache
    assert r_v2.version == "v2"


def test_retired_version_records_are_evicted(gw):
    g, backend = gw
    g.infer(InferRequest(model="m", inputs=(1.0,), idempotency_key="k1"))
    assert len(g.idempotency) == 1
    g.load_version("m", "v2", set_stable=True)
    g.retire_version("m", "v1")
    log_decision("eviction decision", size=len(g.idempotency))
    assert len(g.idempotency) == 0  # 旧版本记录已清除
