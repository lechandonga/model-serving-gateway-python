"""版本路由、生命周期与切换。"""

from __future__ import annotations

import threading
import time

import pytest

from gateway import ErrorCode, GatewayError, InferRequest
from gateway.backend import DeterministicBackend
from gateway.gateway import Gateway

from .conftest import log_decision


@pytest.fixture
def gw():
    g = Gateway(max_batch_size=2, max_wait_s=0.01)
    g.load_version("m", "v1", set_stable=True)
    yield g
    g.close()


def _code(fn) -> ErrorCode:
    try:
        fn()
    except GatewayError as e:
        log_decision("rejected", code=e.code.value, message=e.message)
        return e.code
    raise AssertionError("expected GatewayError")


def test_unspecified_version_routes_to_stable(gw):
    resp = gw.infer(InferRequest(model="m", inputs=(1.0,)))
    log_decision("route decision", requested=None, resolved=resp.version)
    assert resp.version == "v1"


def test_unknown_and_retired_versions_rejected_with_distinct_codes(gw):
    assert _code(lambda: gw.infer(InferRequest(model="nope", inputs=(1.0,)))) \
        == ErrorCode.MODEL_NOT_FOUND
    assert _code(lambda: gw.infer(InferRequest(model="m", inputs=(1.0,), version="vX"))) \
        == ErrorCode.VERSION_NOT_FOUND
    gw.retire_version("m", "v1")
    assert _code(lambda: gw.infer(InferRequest(model="m", inputs=(1.0,), version="v1"))) \
        == ErrorCode.VERSION_RETIRED


def test_loading_version_rejects_new_requests():
    g = Gateway()
    g.registry.register("m", "v1")  # 仅注册，未 mark_ready：LOADING
    assert _code(lambda: g.infer(InferRequest(model="m", inputs=(1.0,)))) \
        == ErrorCode.VERSION_NOT_READY
    g.close()


def test_fallback_is_deterministic_after_version_switch(gw):
    """稳定版本切换与下线后，默认路由规则保持确定。"""
    gw.load_version("m", "v2", set_stable=True)
    assert gw.infer(InferRequest(model="m", inputs=(1.0,))).version == "v2"
    gw.retire_version("m", "v2")
    # 稳定指针失效：确定性回退到字典序最大的 READY 版本（此处为 v1）
    for _ in range(3):
        resp = gw.infer(InferRequest(model="m", inputs=(1.0,)))
        log_decision("fallback decision", resolved=resp.version)
        assert resp.version == "v1"


def test_retire_waits_for_inflight_requests():
    """卸载等待在途请求结束，期间拒绝新请求，不出现半释放状态。"""
    entered = threading.Event()
    proceed = threading.Event()

    class SlowBackend(DeterministicBackend):
        def infer_batch(self, model, version, batch):
            entered.set()
            proceed.wait(timeout=5)
            return super().infer_batch(model, version, batch)

    g = Gateway(backend=SlowBackend(), max_batch_size=1, max_wait_s=0.01)
    g.load_version("m", "v1", set_stable=True)

    result_holder: list = []
    t_req = threading.Thread(
        target=lambda: result_holder.append(g.infer(InferRequest(model="m", inputs=(1.0,))))
    )
    t_req.start()
    assert entered.wait(timeout=5)  # 请求已进入后端（在途）

    retired = threading.Event()
    t_retire = threading.Thread(
        target=lambda: (g.retire_version("m", "v1"), retired.set())
    )
    t_retire.start()
    time.sleep(0.2)
    log_decision("retire decision", inflight=g.snapshot()["versions"]["m"]["inflight"],
                 state=g.snapshot()["versions"]["m"]["versions"]["v1"])
    assert not retired.is_set()  # 在途未结束，卸载未完成
    assert g.snapshot()["versions"]["m"]["versions"]["v1"] == "UNLOADING"
    # 卸载中拒绝新请求
    assert _code(lambda: g.infer(InferRequest(model="m", inputs=(2.0,), version="v1"))) \
        == ErrorCode.VERSION_NOT_READY

    proceed.set()  # 在途请求完成
    t_req.join(timeout=5)
    t_retire.join(timeout=5)
    assert retired.is_set()
    assert result_holder and result_holder[0].version == "v1"
    assert g.snapshot()["versions"]["m"]["versions"]["v1"] == "RETIRED"
    g.close()
