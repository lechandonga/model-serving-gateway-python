"""熔断与恢复：连续失败熔断、快速失败、探测恢复、单次抖动不永久熔断。"""

from __future__ import annotations

import time

import pytest

from gateway import ErrorCode, GatewayError, InferRequest
from gateway.gateway import Gateway

from .conftest import log_decision


@pytest.fixture
def gw():
    g = Gateway(
        max_batch_size=1,
        max_wait_s=0.01,
        breaker_failure_threshold=3,
        breaker_recovery_timeout_s=0.2,
    )
    g.load_version("m", "v1", set_stable=True)
    yield g
    g.close()


def _expect(gw: Gateway, code: ErrorCode) -> None:
    try:
        gw.infer(InferRequest(model="m", inputs=(1.0,)))
        raise AssertionError(f"expected {code}")
    except GatewayError as e:
        log_decision("breaker decision", code=e.code.value,
                     breaker=gw.snapshot()["breakers"])
        assert e.code == code


def test_consecutive_failures_open_circuit_then_fast_fail(gw):
    gw.backend.fail_next(3)
    for _ in range(3):
        _expect(gw, ErrorCode.BACKEND_FAILURE)
    assert gw.snapshot()["breakers"]["m/v1"] == "OPEN"
    # 熔断期间快速失败：不再触达后端（后端故障注入已用完，若触达则会成功）
    _expect(gw, ErrorCode.CIRCUIT_OPEN)
    _expect(gw, ErrorCode.CIRCUIT_OPEN)
    snap = gw.snapshot()["counters"]
    assert snap["rejections.CIRCUIT_OPEN"] == 2
    assert snap["breaker.failures"] == 3


def test_recovery_probe_restores_service_and_is_not_counted_as_normal_success(gw):
    gw.backend.fail_next(3)
    for _ in range(3):
        _expect(gw, ErrorCode.BACKEND_FAILURE)
    time.sleep(0.25)  # 冷却结束，进入半开
    success_before = gw.metrics.snapshot().get("requests.success", 0)
    resp = gw.infer(InferRequest(model="m", inputs=(2.0,)))  # 探测请求，后端已恢复
    assert resp.version == "v1"
    snap = gw.snapshot()
    log_decision("recovery decision", breaker=snap["breakers"],
                 probes=snap["counters"].get("breaker.probes"),
                 probe_success=snap["counters"].get("breaker.probe_success"))
    assert snap["breakers"]["m/v1"] == "CLOSED"
    assert snap["counters"]["breaker.probes"] == 1
    assert snap["counters"]["breaker.probe_success"] == 1
    # 探测成功不计入正常成功流量
    assert snap["counters"].get("requests.success", 0) == success_before
    # 熔断器闭合后正常流量恢复
    gw.infer(InferRequest(model="m", inputs=(3.0,)))
    assert gw.metrics.snapshot()["requests.success"] == success_before + 1


def test_failed_probe_reopens_circuit(gw):
    gw.backend.fail_next(4)  # 3 次熔断 + 1 次探测失败
    for _ in range(3):
        _expect(gw, ErrorCode.BACKEND_FAILURE)
    time.sleep(0.25)
    _expect(gw, ErrorCode.BACKEND_FAILURE)  # 探测失败
    assert gw.snapshot()["breakers"]["m/v1"] == "OPEN"
    _expect(gw, ErrorCode.CIRCUIT_OPEN)  # 重新熔断，快速失败


def test_single_blip_does_not_permanently_trip(gw):
    gw.backend.fail_next(1)
    _expect(gw, ErrorCode.BACKEND_FAILURE)  # 1 次失败，未达阈值
    resp = gw.infer(InferRequest(model="m", inputs=(1.0,)))  # 成功会清零计数
    assert resp.version == "v1"
    gw.backend.fail_next(2)  # 再失败 2 次仍不达阈值（计数已被成功清零）
    for _ in range(2):
        _expect(gw, ErrorCode.BACKEND_FAILURE)
    log_decision("blip decision", breaker=gw.snapshot()["breakers"])
    assert gw.snapshot()["breakers"]["m/v1"] == "CLOSED"
    assert gw.infer(InferRequest(model="m", inputs=(1.0,))).version == "v1"
