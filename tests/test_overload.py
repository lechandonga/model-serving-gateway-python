"""过载、背压与超时：拒绝原因可区分，等待有界。"""

from __future__ import annotations

import threading
import time

import pytest

from gateway import ErrorCode, GatewayError, InferRequest
from gateway.backend import DeterministicBackend
from gateway.gateway import Gateway

from .conftest import log_decision


class BlockingBackend(DeterministicBackend):
    """可阻塞的后端，用于占满队列制造过载。"""

    def __init__(self) -> None:
        super().__init__()
        self.gate = threading.Event()

    def infer_batch(self, model, version, batch):
        self.gate.wait(timeout=10)
        return super().infer_batch(model, version, batch)


@pytest.fixture
def overloaded_gw():
    backend = BlockingBackend()
    # 批次大、窗口长、队列小：请求会在队列中堆积
    g = Gateway(backend=backend, max_batch_size=100, max_wait_s=5.0, max_queue=2)
    g.load_version("m", "v1", set_stable=True)
    yield g, backend
    backend.gate.set()
    g.close()


def test_queue_full_rejects_with_overloaded(overloaded_gw):
    g, backend = overloaded_gw
    # 两个请求占满队列（在后台线程中等待）
    holders: list = []
    threads = [
        threading.Thread(
            target=lambda i=i: holders.append(
                g.infer(InferRequest(model="m", inputs=(float(i),), timeout_s=30))
            )
        )
        for i in range(2)
    ]
    for t in threads:
        t.start()
    time.sleep(0.3)  # 等待两个请求入队

    try:
        g.infer(InferRequest(model="m", inputs=(9.0,)))
        raise AssertionError("expected OVERLOADED")
    except GatewayError as e:
        log_decision("backpressure decision", code=e.code.value, message=e.message)
        assert e.code == ErrorCode.OVERLOADED

    snap = g.snapshot()["counters"]
    assert snap["rejections.OVERLOADED"] == 1
    backend.gate.set()
    for t in threads:
        t.join(timeout=10)
    assert len(holders) == 2  # 已入队请求最终正常完成


def test_wait_timeout_is_distinct_from_overload(overloaded_gw):
    g, _backend = overloaded_gw
    # 超时等待的请求应得到 REQUEST_TIMEOUT，而非 OVERLOADED
    try:
        g.infer(InferRequest(model="m", inputs=(1.0,), timeout_s=0.2))
        raise AssertionError("expected REQUEST_TIMEOUT")
    except GatewayError as e:
        log_decision("timeout decision", code=e.code.value, message=e.message)
        assert e.code == ErrorCode.REQUEST_TIMEOUT
    snap = g.snapshot()["counters"]
    assert snap["rejections.REQUEST_TIMEOUT"] == 1
    assert "rejections.OVERLOADED" not in snap


def test_batch_constraint_rejected():
    g = Gateway(max_batch_size=2, max_wait_s=0.01)
    g.load_version("m", "v1", set_stable=True)
    try:
        g.infer(InferRequest(model="m", inputs=tuple(range(2000))))
        raise AssertionError("expected BATCH_CONSTRAINT")
    except GatewayError as e:
        log_decision("constraint decision", code=e.code.value, inputs_len=2000)
        assert e.code == ErrorCode.BATCH_CONSTRAINT
    g.close()


def test_queue_memory_is_bounded(overloaded_gw):
    """在队请求数永不超过 max_queue，等待过程不无界占用内存。"""
    g, backend = overloaded_gw
    threads = [
        threading.Thread(
            target=lambda i=i: g.infer(
                InferRequest(model="m", inputs=(float(i),), timeout_s=30)
            )
        )
        for i in range(2)
    ]
    for t in threads:
        t.start()
    time.sleep(0.3)
    rejected = 0
    for i in range(20):
        try:
            g.infer(InferRequest(model="m", inputs=(float(i),), timeout_s=0.05))
        except GatewayError as e:
            rejected += 1
            assert e.code in (ErrorCode.OVERLOADED, ErrorCode.REQUEST_TIMEOUT)
    log_decision("bounded queue", max_queue=2, rejected=rejected)
    assert rejected == 20
    assert g.batcher._total_queued <= 2
    backend.gate.set()
    for t in threads:
        t.join(timeout=10)
