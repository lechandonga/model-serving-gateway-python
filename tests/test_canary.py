"""灰度发布：比例分流、比例调整、租户粘性、版本隔离、自动回退与回退后稳定可用。"""

from __future__ import annotations

import math
import threading

import pytest

from gateway import ErrorCode, GatewayError, InferRequest
from gateway.backend import DeterministicBackend
from gateway.gateway import Gateway

from .conftest import log_decision


class VersionFaultBackend(DeterministicBackend):
    """只对指定版本注入整批故障：模拟灰度版本独立部署的真实后端故障。"""

    def __init__(self, fault_version: str) -> None:
        super().__init__()
        self.fault_version = fault_version
        self._remaining = 0
        self._lock = threading.Lock()
        self.calls = 0

    def fail_next(self, n: int) -> None:
        with self._lock:
            self._remaining += n

    def infer_batch(self, model, version, batch):
        if version == self.fault_version:
            with self._lock:
                if self._remaining > 0:
                    self._remaining -= 1
                    raise RuntimeError("injected canary backend failure")
        with self._lock:
            self.calls += 1
        return super().infer_batch(model, version, batch)


class BlockingFaultBackend(DeterministicBackend):
    """可阻塞且故障只作用于指定版本的后端。"""

    def __init__(self, fault_version: str) -> None:
        super().__init__()
        self.fault_version = fault_version
        self.gate = threading.Event()

    def infer_batch(self, model, version, batch):
        self.gate.wait(timeout=10)
        return super().infer_batch(model, version, batch)


@pytest.fixture
def gw():
    backend = VersionFaultBackend("v2")
    g = Gateway(
        backend=backend,
        max_batch_size=1,
        max_wait_s=0.01,
        canary_window_s=60.0,
        canary_failure_rate_threshold=0.5,
        canary_min_requests=4,
    )
    g.load_version("m", "v1", set_stable=True)
    g.load_version("m", "v2")
    yield g, backend
    g.close()


def _routes(g: Gateway, n: int = 400) -> dict[str, set[str]]:
    routes: dict[str, set[str]] = {}
    for i in range(n):
        tenant = f"tenant-{i:04d}"
        version = g.infer(InferRequest(model="m", inputs=(1.0,), tenant=tenant)).version
        routes.setdefault(version, set()).add(tenant)
    return routes


def test_traffic_split_by_percentage(gw):
    """灰度比例决定分流规模：0/50/100 三档行为正确。"""
    g, _ = gw
    g.configure_canary("m", "v2", 0)
    assert set(_routes(g, 100)) == {"v1"}

    g.adjust_canary_percentage("m", 50)
    routes = _routes(g, 400)
    canary_share = len(routes.get("v2", set())) / 400
    log_decision("split decision", canary_share=canary_share)
    assert 0.35 <= canary_share <= 0.65

    g.adjust_canary_percentage("m", 100)
    routes = _routes(g, 100)
    assert set(routes) == {"v2"} and len(routes["v2"]) == 100


def test_percentage_adjustment_takes_effect_and_zero_returns_all_to_stable(gw):
    """比例可随时调整；清零后全部干净回到稳定版本。"""
    g, _ = gw
    g.configure_canary("m", "v2", 100)
    assert g.infer(InferRequest(model="m", inputs=(1.0,), tenant="anyone")).version == "v2"
    g.adjust_canary_percentage("m", 0)
    for tenant in ("anyone", "other", "third"):
        resp = g.infer(InferRequest(model="m", inputs=(1.0,), tenant=tenant))
        log_decision("zero percentage", tenant=tenant, resolved=resp.version)
        assert resp.version == "v1"
    # 快照显示灰度已不活跃
    assert g.snapshot()["canaries"]["m"] == {
        "stable_version": "v1",
        "canary_version": "v2",
        "percentage": 0,
        "active": False,
    }


def test_tenant_stickiness_within_a_canary_round(gw):
    """同一租户在配置不变期间固定落在同一版本，多轮请求结果一致。"""
    g, _ = gw
    g.configure_canary("m", "v2", 50)
    decisions: dict[str, str] = {}
    for round_no in range(5):
        for i in range(60):
            tenant = f"sticky-{i}"
            version = g.infer(
                InferRequest(model="m", inputs=(float(round_no),), tenant=tenant)
            ).version
            if tenant in decisions:
                assert decisions[tenant] == version
            decisions[tenant] = version
    canary_tenants = {t for t, v in decisions.items() if v == "v2"}
    log_decision("stickiness decision", tenants=len(decisions),
                 canary_tenants=len(canary_tenants))
    assert canary_tenants  # 确实有一部分租户在灰度侧


def test_concurrent_same_tenant_requests_route_consistently(gw):
    """同一批并发请求：同租户不会一会儿去这边一会儿去那边。"""
    g, _ = gw
    g.configure_canary("m", "v2", 50)
    tenant = "concurrent-tenant"
    barrier = threading.Barrier(8)
    versions: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        v = g.infer(InferRequest(model="m", inputs=(1.0,), tenant=tenant)).version
        with lock:
            versions.append(v)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log_decision("concurrent split decision", versions=versions)
    assert len(set(versions)) == 1


def test_canary_failure_does_not_affect_stable(gw):
    """灰度版本的过载/后端故障/熔断只影响灰度流量，稳定版本照常服务。"""
    g, backend = gw
    g.configure_canary("m", "v2", 100)
    backend.fail_next(3)
    for _ in range(3):
        with pytest.raises(GatewayError) as exc:
            g.infer(InferRequest(model="m", inputs=(1.0,), tenant="c"))
        assert exc.value.code == ErrorCode.BACKEND_FAILURE
    # 灰度侧熔断打开
    assert g.snapshot()["breakers"]["m/v2"] == "OPEN"
    # 显式指定稳定版本：熔断器独立，服务正常
    resp = g.infer(InferRequest(model="m", inputs=(1.0,), version="v1"))
    assert resp.version == "v1"
    # 稳定版本窗口没有灰度的失败
    stable_window = g.snapshot()["windows"]["m/v1"]
    assert stable_window["backend_failures"] == 0
    log_decision("isolation decision",
                 canary_breaker=g.snapshot()["breakers"]["m/v2"],
                 stable_window=stable_window)


def test_batches_are_isolated_by_version(gw):
    """批处理按 (model, version, tenant) 分桶，灰度与稳定绝不合批。"""
    g, backend = gw
    g.configure_canary("m", "v2", 100)
    before = g.metrics.snapshot().get("batches.executed", 0)
    # 一个灰度版本请求 + 一个显式稳定版本请求：两个桶，两批
    r_canary = g.infer(InferRequest(model="m", inputs=(1.0,), tenant="t"))
    r_stable = g.infer(InferRequest(model="m", inputs=(1.0,), tenant="t", version="v1"))
    after = g.metrics.snapshot()["batches.executed"]
    log_decision("batch isolation", batches=after - before,
                 versions=(r_canary.version, r_stable.version))
    assert r_canary.version == "v2" and r_stable.version == "v1"
    assert after - before == 2


def test_idempotency_results_not_shared_across_versions(gw):
    """同一幂等标识不能把稳定版本结果复用给灰度版本。"""
    g, _ = gw
    g.configure_canary("m", "v2", 100)
    first = g.infer(InferRequest(model="m", inputs=(3.0,), tenant="t", idempotency_key="k"))
    assert first.version == "v2" and not first.from_idempotency_cache
    # 切到 0%，同一 key 落到稳定版本：必须重新计算，不得命中灰度结果
    g.adjust_canary_percentage("m", 0)
    second = g.infer(InferRequest(model="m", inputs=(3.0,), tenant="t", idempotency_key="k"))
    log_decision("version idempotency isolation",
                 first_version=first.version, second_version=second.version,
                 cached=second.from_idempotency_cache)
    assert second.version == "v1" and not second.from_idempotency_cache
    # 再回到灰度版本，仍应命中灰度侧自己的记录
    g.adjust_canary_percentage("m", 100)
    third = g.infer(InferRequest(model="m", inputs=(3.0,), tenant="t", idempotency_key="k"))
    assert third.version == "v2" and third.from_idempotency_cache
    assert third.outputs == first.outputs


def test_idempotency_isolated_between_tenants(gw):
    """不同租户即便使用相同幂等标识也各算各的，绝不串结果。"""
    g, backend = gw
    g.configure_canary("m", "v2", 0)  # 都在稳定版本上，排除版本差异
    r_a = g.infer(InferRequest(model="m", inputs=(5.0,), tenant="A", idempotency_key="same"))
    r_b = g.infer(InferRequest(model="m", inputs=(9.0,), tenant="B", idempotency_key="same"))
    log_decision("tenant idempotency isolation",
                 cached_a=r_a.from_idempotency_cache, cached_b=r_b.from_idempotency_cache,
                 outputs_equal=r_a.outputs == r_b.outputs)
    assert not r_a.from_idempotency_cache and not r_b.from_idempotency_cache
    # 输入不同 => 结果不同；若串用缓存这里会相等
    assert r_a.outputs != r_b.outputs
    # 各租户重放只命中自己的记录
    r_a2 = g.infer(InferRequest(model="m", inputs=(5.0,), tenant="A", idempotency_key="same"))
    assert r_a2.from_idempotency_cache and r_a2.outputs == r_a.outputs
    r_b2 = g.infer(InferRequest(model="m", inputs=(9.0,), tenant="B", idempotency_key="same"))
    assert r_b2.from_idempotency_cache and r_b2.outputs == r_b.outputs


def test_auto_rollback_on_backend_failure_rate(gw):
    """灰度版本真实后端失败率超阈值 -> 比例自动清零，留下可查回退记录。"""
    g, backend = gw
    g.configure_canary("m", "v2", 100)
    backend.fail_next(4)
    codes = []
    for i in range(5):
        try:
            g.infer(InferRequest(model="m", inputs=(float(i),), tenant=f"c{i}"))
        except GatewayError as e:
            codes.append(e.code.value)
    log_decision("auto rollback codes", codes=codes)
    records = g.canary_rollback_records("m")
    assert len(records) == 1
    record = records[0]
    assert record.canary_version == "v2"
    assert record.failure_rate > 0.5
    assert record.decided_requests >= 4
    assert "failure rate" in record.reason
    assert record.timestamp > 0
    # 自动回退只留一条记录：后续稳定版本成功不会再产生回退记录
    resp = g.infer(InferRequest(model="m", inputs=(1.0,), tenant="late"))
    assert resp.version == "v1"
    assert len(g.canary_rollback_records("m")) == 1
    assert g.snapshot()["counters"].get("canary.rolled_back") == 1


def test_invalid_inputs_do_not_trigger_rollback(gw):
    """客户端非法输入（NaN 批内单条失败）不算灰度后端故障，不触发回退。"""
    g, backend = gw
    g.configure_canary("m", "v2", 100)
    for _ in range(8):
        with pytest.raises(GatewayError) as exc:
            g.infer(InferRequest(model="m", inputs=(math.nan,), tenant="nan-tenant"))
        assert exc.value.code == ErrorCode.BACKEND_FAILURE
        assert exc.value.backend_fault is False
    window = g.snapshot()["windows"]["m/v2"]
    log_decision("invalid input decision", window=window)
    assert window["backend_failures"] == 0
    assert window["rejections"].get("INVALID_INPUT", 0) == 8
    assert g.canary_rollback_records("m") == []
    # 灰度仍在 100% 服务
    assert g.infer(InferRequest(model="m", inputs=(1.0,), tenant="healthy")).version == "v2"


def test_overload_and_timeout_do_not_trigger_rollback(gw):
    """背压拒绝与等待超时不是后端故障，不能把灰度打回。"""
    backend = BlockingFaultBackend("v2")
    g = Gateway(
        backend=backend,
        max_batch_size=100,
        max_wait_s=5.0,
        max_queue=2,
        canary_failure_rate_threshold=0.5,
        canary_min_requests=2,
    )
    g.load_version("m", "v1", set_stable=True)
    g.load_version("m", "v2")
    g.configure_canary("m", "v2", 100)

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
    # 背压拒绝（队列满）与等待超时
    for _ in range(3):
        try:
            g.infer(InferRequest(model="m", inputs=(9.0,)))
        except GatewayError:
            pass
    for _ in range(3):
        try:
            g.infer(InferRequest(model="m", inputs=(9.0,), timeout_s=0.05))
        except GatewayError:
            pass
    window = g.snapshot()["windows"]["m/v2"]
    log_decision("overload/timeout decision", window=window)
    assert window["backend_failures"] == 0
    assert g.canary_rollback_records("m") == []
    backend.gate.set()
    for t in threads:
        t.join(timeout=10)
    g.close()


def test_stable_version_still_serves_after_rollback(gw):
    """回退后稳定版本持续可用，窗口统计、熔断都不受灰度影响。"""
    g, backend = gw
    g.configure_canary("m", "v2", 100)
    backend.fail_next(4)
    for i in range(4):
        with pytest.raises(GatewayError):
            g.infer(InferRequest(model="m", inputs=(float(i),), tenant=f"c{i}"))
    assert g.canary_rollback_records("m")
    for i in range(6):
        resp = g.infer(InferRequest(model="m", inputs=(float(i),), tenant=f"s{i}"))
        assert resp.version == "v1"
    stable_window = g.snapshot()["windows"]["m/v1"]
    log_decision("post-rollback stable", window=stable_window)
    assert stable_window["successes"] >= 6
    assert stable_window["backend_failures"] == 0


def test_retiring_canary_version_returns_all_to_stable(gw):
    """灰度版本下线：干净回到稳定版本，无无人承接/两边都算的中间状态。"""
    g, _ = gw
    g.configure_canary("m", "v2", 100)
    assert g.infer(InferRequest(model="m", inputs=(1.0,), tenant="x")).version == "v2"
    g.retire_version("m", "v2")
    assert g.snapshot()["canaries"] == {}  # 灰度配置随版本下线被移除
    for tenant in ("x", "y", "z"):
        assert g.infer(InferRequest(model="m", inputs=(1.0,), tenant=tenant)).version == "v1"
    # 已下线灰度版本显式请求仍按既有生命周期拒绝
    with pytest.raises(GatewayError) as exc:
        g.infer(InferRequest(model="m", inputs=(1.0,), version="v2"))
    assert exc.value.code == ErrorCode.VERSION_RETIRED


def test_manual_rollback_leaves_record(gw):
    """手动清零比例不留回退记录；显式手动回退接口会记录原因。"""
    g, _ = gw
    g.configure_canary("m", "v2", 30)
    g.adjust_canary_percentage("m", 0)
    assert g.canary_rollback_records("m") == []
    g.adjust_canary_percentage("m", 30)
    record = g.rollback_canary("m", reason="operator panic button")
    assert record is not None and record.reason == "operator panic button"
    assert g.infer(InferRequest(model="m", inputs=(1.0,), tenant="z")).version == "v1"


def test_retiring_stable_version_fails_over_to_canary_without_gap(gw):
    """灰度进行中下线稳定版本：灰度配置移除，全部流量由灰度版本承接，不空窗。"""
    g, _ = gw
    g.configure_canary("m", "v2", 50)
    # 下线稳定版本（灰度 v2 承接；无在途 v1 请求时立即完成）
    g.retire_version("m", "v1")
    assert g.snapshot()["canaries"] == {}
    for tenant in ("a", "b", "c"):
        assert g.infer(InferRequest(model="m", inputs=(1.0,), tenant=tenant)).version == "v2"
    with pytest.raises(GatewayError) as exc:
        g.infer(InferRequest(model="m", inputs=(1.0,), version="v1"))
    assert exc.value.code == ErrorCode.VERSION_RETIRED


def test_canary_can_be_re_armed_after_rollback(gw):
    """回退记录与新一轮灰度互不干扰：重新放量后再次故障可再次回退并各留一条记录。"""
    g, backend = gw
    g.configure_canary("m", "v2", 100)
    backend.fail_next(4)
    for i in range(4):
        with pytest.raises(GatewayError):
            g.infer(InferRequest(model="m", inputs=(float(i),), tenant=f"c{i}"))
    assert len(g.canary_rollback_records("m")) == 1
    # 重新放量：新一轮灰度，比例非零
    g.adjust_canary_percentage("m", 100)
    assert g.snapshot()["canaries"]["m"]["active"] is True
    backend.fail_next(4)
    # 新一轮故障（首个可能是半开探测，探测失败即重新熔断并触发回退），
    # 循环请求直到再次回退即可，不假定具体失败次数。
    for i in range(8):
        try:
            g.infer(InferRequest(model="m", inputs=(float(i),), tenant=f"d{i}"))
        except GatewayError:
            pass
        if len(g.canary_rollback_records("m")) >= 2:
            break
    records = g.canary_rollback_records("m")
    log_decision("re-arm rollbacks", count=len(records))
    assert len(records) == 2
    assert g.infer(InferRequest(model="m", inputs=(1.0,), tenant="final")).version == "v1"
