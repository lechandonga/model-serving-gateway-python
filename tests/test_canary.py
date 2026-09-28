"""灰度发布：按比例分流、租户粘性、版本隔离、自动回退与回退后稳定可用。"""

from __future__ import annotations

import threading

import pytest

from gateway import ErrorCode, GatewayError, InferRequest
from gateway.backend import DeterministicBackend
from gateway.canary import CanaryRouter, RollbackRecord, bucket_for_tenant
from gateway.gateway import Gateway
from gateway.window import SlidingWindowStats

from .conftest import log_decision


class VersionFailBackend(DeterministicBackend):
    """可按版本注入故障的后端：只让指定版本持续失败，其余版本正常。"""

    def __init__(self) -> None:
        super().__init__()
        self._lock2 = threading.Lock()
        self.fail_versions: set[str] = set()

    def set_failing(self, version: str | None, failing: bool) -> None:
        with self._lock2:
            if failing:
                self.fail_versions.add(version)
            else:
                self.fail_versions.discard(version)

    def infer_batch(self, model, version, batch):
        with self._lock2:
            failing = version in self.fail_versions
        if failing:
            raise RuntimeError(f"injected backend failure on {version}")
        return super().infer_batch(model, version, batch)


@pytest.fixture
def gw():
    backend = VersionFailBackend()
    g = Gateway(
        backend=backend,
        max_batch_size=1,
        max_wait_s=0.01,
        breaker_failure_threshold=99,  # 测试聚焦灰度回退，先不让普通熔断介入
        breaker_recovery_timeout_s=1.0,
        canary_failure_threshold=0.5,
        canary_min_samples=3,
    )
    g.load_version("m", "v1", set_stable=True)
    g.load_version("m", "v2")
    yield g, backend
    g.close()


def _infer(g: Gateway, tenant: str, **kw):
    return g.infer(InferRequest(model="m", inputs=(1.0,), tenant=tenant, **kw))


# ---- 纯分流组件 ----

def test_bucket_is_deterministic_per_epoch():
    b1 = bucket_for_tenant("m", 1, "tA")
    assert bucket_for_tenant("m", 1, "tA") == b1
    assert 0 <= b1 < 100
    # 不同租户大概率落到不同桶（这里只断言不同租户结果稳定且可复现）
    assert bucket_for_tenant("m", 1, "tB") == bucket_for_tenant("m", 1, "tB")
    # 换轮次后桶位可能重排（用另一组输入空间），但结果仍确定
    assert bucket_for_tenant("m", 2, "tA") == bucket_for_tenant("m", 2, "tA")


def test_percent_zero_and_disabled_route_all_to_stable():
    r = CanaryRouter()
    r.enable("m", "v1", "v2", 100)
    r.adjust_percent("m", 0)
    tenants = [f"t{i}" for i in range(30)]
    assert all(r.route("m", t)[0] is None for t in tenants)
    r.disable("m")
    assert r.route("m", "tA") == (None, None)


def test_split_ratio_is_approximately_honored():
    r = CanaryRouter()
    r.enable("m", "v1", "v2", 30)
    tenants = [f"tenant-{i}" for i in range(2000)]
    canary = sum(1 for t in tenants if r.route("m", t)[0] == "v2")
    ratio = canary / len(tenants)
    log_decision("split ratio", target=0.30, actual=round(ratio, 3))
    assert 0.25 < ratio < 0.35


# ---- 网关级分流与粘性 ----

def test_traffic_split_and_deterministic_routing(gw):
    g, _ = gw
    g.enable_canary("m", "v1", "v2", 50)
    tenants = [f"t{i}" for i in range(40)]
    first = {t: _infer(g, t).version for t in tenants}
    # 同一批视角下分流结果确定：重复多次结果完全一致，不忽左忽右
    for _ in range(5):
        assert {t: _infer(g, t).version for t in tenants} == first
    canary_tenants = [t for t, v in first.items() if v == "v2"]
    stable_tenants = [t for t, v in first.items() if v == "v1"]
    log_decision("split", canary=len(canary_tenants), stable=len(stable_tenants))
    assert canary_tenants and stable_tenants  # 两边都有流量


def test_tenant_stickiness_within_one_round(gw):
    g, _ = gw
    g.enable_canary("m", "v1", "v2", 40)
    tenant = "sticky-tenant"
    versions = {_infer(g, tenant).version for _ in range(20)}
    assert len(versions) == 1  # 同一租户在一轮灰度内固定一个版本


def test_adjusting_percent_moves_tenant_set_monotonically(gw):
    g, _ = gw
    g.enable_canary("m", "v1", "v2", 20)
    tenants = [f"t{i}" for i in range(300)]
    at20 = {t for t in tenants if _infer(g, t).version == "v2"}
    g.adjust_canary_percent("m", 80)  # 放量：同 epoch，分界线移动
    at80 = {t for t in tenants if _infer(g, t).version == "v2"}
    log_decision("ramp up", at20=len(at20), at80=len(at80))
    assert at20.issubset(at80)  # 已在灰度的租户不被踢回稳定版本
    g.adjust_canary_percent("m", 0)
    assert all(_infer(g, t).version == "v1" for t in tenants)  # 清零干净回退


def test_explicit_version_bypasses_canary(gw):
    g, _ = gw
    g.enable_canary("m", "v1", "v2", 100)
    # 即使灰度 100%，显式指定 v1 仍直达 v1
    assert _infer(g, "tA", version="v1").version == "v1"


def test_retiring_canary_version_cleanly_returns_to_stable(gw):
    g, _ = gw
    g.enable_canary("m", "v1", "v2", 100)
    assert _infer(g, "tA").version == "v2"
    g.retire_version("m", "v2")  # 灰度版本下线：配置原子摘除，无空窗
    assert g.canary.get("m") is None
    for t in [f"t{i}" for i in range(10)]:
        assert _infer(g, t).version == "v1"


# ---- 灰度与稳定互相隔离 ----

def test_canary_backend_failure_does_not_affect_stable(gw):
    g, backend = gw
    g.enable_canary("m", "v1", "v2", 100)
    backend.set_failing("v2", True)
    canary_err = 0
    for i in range(6):
        try:
            _infer(g, f"c{i}")
        except GatewayError as e:
            canary_err += 1
            assert e.code == ErrorCode.BACKEND_FAILURE
    # 自动回退已发生
    assert len(g.rollback_history("m")) == 1
    # 稳定版本熔断器未被灰度故障带崩，且稳定版本正常服务
    assert g.snapshot()["breakers"].get("m/v1", "CLOSED") == "CLOSED"
    for i in range(5):
        assert _infer(g, f"s{i}", version="v1").version == "v1"
    backend.set_failing("v2", False)


def test_breakers_are_isolated_per_version(gw):
    g, backend = gw
    g.enable_canary("m", "v1", "v2", 0)  # 全部走 v1，v2 单独压测
    backend.set_failing("v2", True)
    for _ in range(3):
        with pytest.raises(GatewayError) as ei:
            _infer(g, "x", version="v2")
        assert ei.value.code == ErrorCode.BACKEND_FAILURE
    # v2 熔断不影响 v1
    assert _infer(g, "x", version="v1").version == "v1"
    assert g.snapshot()["breakers"]["m/v1"] == "CLOSED"
    backend.set_failing("v2", False)


def test_idempotency_isolated_by_version_and_tenant(gw):
    g, _ = gw
    g.enable_canary("m", "v1", "v2", 100)
    # 灰度版本算出 key=k
    r_canary = _infer(g, "tA", idempotency_key="k")
    assert r_canary.version == "v2" and not r_canary.from_idempotency_cache
    # 稳定版本同 key 不复用灰度结果
    r_stable = _infer(g, "tA", version="v1", idempotency_key="k")
    assert r_stable.version == "v1" and not r_stable.from_idempotency_cache
    assert r_stable.outputs != r_canary.outputs
    # 另一租户同 key 也不能拿到 tA 的结果
    r_other = _infer(g, "tB", idempotency_key="k")
    assert r_other.version == "v2" and not r_other.from_idempotency_cache
    # 原租户重放命中缓存
    assert _infer(g, "tA", idempotency_key="k").from_idempotency_cache


# ---- 自动回退判定 ----

def test_non_backend_failures_do_not_rollback(gw):
    """非法输入 / 等待超时 / 背压拒绝都不能把灰度打回去。"""
    g, _backend = gw
    # 灰度 v2 完全正常；制造的都是非后端故障
    g.enable_canary("m", "v1", "v2", 100)
    # 非法输入（NaN -> 批内单条失败仍算后端失败，故改用批次约束/超时/背压）
    with pytest.raises(GatewayError) as ei:
        g.infer(InferRequest(model="m", inputs=tuple(range(2000)), tenant="tA"))
    assert ei.value.code == ErrorCode.BATCH_CONSTRAINT
    for _ in range(5):
        with pytest.raises(GatewayError) as ei2:
            g.infer(InferRequest(model="m", inputs=(1.0,), tenant="tA", timeout_s=0.0))
        assert ei2.value.code == ErrorCode.REQUEST_TIMEOUT
    assert g.rollback_history("m") == []  # 未回退
    # 灰度仍在，且真实请求成功
    assert _infer(g, "tA").version == "v2"
    view = g.window.window_view("m", "v2")
    log_decision("window view", **{k: view[k] for k in
                 ("requests", "success", "backend_failures", "rejections")})
    assert view["backend_failures"] == 0
    assert view["rejections"].get("BATCH_CONSTRAINT") == 1
    assert view["rejections"].get("REQUEST_TIMEOUT") == 5


def test_backend_failures_trigger_single_rollback_record(gw):
    g, backend = gw
    g.enable_canary("m", "v1", "v2", 100)
    backend.set_failing("v2", True)
    failures = 0
    for i in range(8):
        try:
            _infer(g, f"c{i}")
        except GatewayError:
            failures += 1  # 回退前的灰度请求真实失败
    records = g.rollback_history("m")
    log_decision("rollback", failures=failures, records=len(records),
                 reason=records[0].reason)
    assert failures >= 3
    assert len(records) == 1  # 同一轮只留一条回退记录
    rec = records[0]
    assert rec.canary_version == "v2" and rec.stable_version == "v1"
    assert rec.failure_rate >= 0.5 and rec.samples >= 3
    assert rec.timestamp > 0 and rec.reason
    backend.set_failing("v2", False)


def test_min_samples_prevents_premature_rollback(gw):
    g, backend = gw
    g.enable_canary("m", "v1", "v2", 100)
    backend.set_failing("v2", True)
    for i in range(2):  # 仅 2 个样本，低于 min_samples=3，即使 100% 失败也不回退
        with pytest.raises(GatewayError):
            _infer(g, f"c{i}")
    assert g.rollback_history("m") == []
    assert g.canary.get("m") is not None
    backend.set_failing("v2", False)


def test_stable_still_serves_after_rollback(gw):
    g, backend = gw
    g.enable_canary("m", "v1", "v2", 100)
    backend.set_failing("v2", True)
    failures = 0
    for i in range(4):
        try:
            _infer(g, f"c{i}")
        except GatewayError:
            failures += 1
    assert failures >= 1
    assert g.canary.get("m") is None
    backend.set_failing("v2", False)
    # 回退后所有租户都在稳定版本上连续成功
    for t in ["a", "b", "c", "d"]:
        assert _infer(g, t).version == "v1"
    snap = g.snapshot()
    assert snap["canary"]["canaries"] == {}
    assert snap["windows"]["m/v1"]["backend_failures"] == 0


def test_rollback_recorded_even_when_breaker_would_open():
    """高失败率下灰度自动回退独立于熔断：达到窗口阈值即回退并留档。"""
    backend = VersionFailBackend()
    g = Gateway(
        backend=backend, max_batch_size=1, max_wait_s=0.01,
        breaker_failure_threshold=2, breaker_recovery_timeout_s=30.0,
        canary_failure_threshold=0.5, canary_min_samples=3,
    )
    g.load_version("m", "v1", set_stable=True)
    g.load_version("m", "v2")
    g.enable_canary("m", "v1", "v2", 100)
    backend.set_failing("v2", True)
    failures = 0
    for i in range(4):
        try:
            _infer(g, f"c{i}")
        except GatewayError:
            failures += 1
    assert failures >= 1
    assert len(g.rollback_history("m")) == 1
    assert _infer(g, "x").version == "v1"  # 熔断/回退后稳定版本照常
    backend.set_failing("v2", False)
    g.close()


# ---- 并发窗口统计 ----

def test_window_counts_are_exact_under_concurrency():
    stats = SlidingWindowStats(window_s=10.0)
    n_per = 200
    threads = []

    def worker(kind: int) -> None:
        for _ in range(n_per):
            if kind == 0:
                stats.record_success("m", "v2")
            elif kind == 1:
                stats.record_backend_failure("m", "v2")
            else:
                stats.record_rejection("m", "v2", "OVERLOADED")

    for kind in range(3):
        for _ in range(4):
            threads.append(threading.Thread(target=worker, args=(kind,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    view = stats.window_view("m", "v2")
    log_decision("concurrent window", **{k: view[k] for k in
                 ("requests", "success", "backend_failures")})
    assert view["success"] == 4 * n_per
    assert view["backend_failures"] == 4 * n_per
    assert view["rejections"]["OVERLOADED"] == 4 * n_per
    assert view["requests"] == 12 * n_per
