"""网关门面：组合注册表、灰度路由、熔断器、幂等存储、批处理器、窗口统计与指标。

请求处理顺序：
1. 版本选择（显式版本直达；否则按灰度比例+租户粘性分流），再做版本解析
   （未知/下线/未就绪在此拒绝，错误码可区分；灰度版本恰好失效时安全落稳定版本）
2. 幂等命中检查（按 model/version/tenant/key 隔离，含在途去重，只产生一次推理效果）
3. 在途计数 acquire（与卸载互斥）
4. 熔断检查（OPEN 快速失败；HALF_OPEN 仅放行探测）
5. 动态批处理执行（背压/超时/批次约束在此拒绝）
6. 按版本记录窗口统计，必要时触发灰度自动回退；结果写幂等存储并释放资源
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .backend import DeterministicBackend
from .batcher import DynamicBatcher
from .breaker import BreakerState, CircuitBreaker
from .canary import CanaryConfig, CanaryRouter, RollbackRecord
from .errors import ErrorCode, GatewayError
from .idempotency import IdempotencyStore
from .metrics import Metrics
from .registry import VersionRegistry
from .types import InferRequest, InferResponse
from .window import SlidingWindowStats


@dataclass
class _Inflight:
    event: threading.Event
    response: InferResponse | None = None
    error: BaseException | None = None


class Gateway:
    def __init__(
        self,
        *,
        backend: DeterministicBackend | None = None,
        max_batch_size: int = 8,
        max_wait_s: float = 0.05,
        max_queue: int = 64,
        idempotency_capacity: int = 1024,
        breaker_failure_threshold: int = 3,
        breaker_recovery_timeout_s: float = 1.0,
        canary_window_s: float = 10.0,
        canary_failure_threshold: float = 0.5,
        canary_min_samples: int = 3,
    ) -> None:
        self.backend = backend or DeterministicBackend()
        self.registry = VersionRegistry()
        self.metrics = Metrics()
        self.idempotency = IdempotencyStore(capacity=idempotency_capacity)
        self.canary = CanaryRouter(time_func=time.monotonic)
        self.window = SlidingWindowStats(window_s=canary_window_s, time_func=time.monotonic)
        self._rollback_conf = (canary_failure_threshold, canary_min_samples)
        self._breaker_conf = (breaker_failure_threshold, breaker_recovery_timeout_s)
        self._breakers: dict[tuple[str, str], CircuitBreaker] = {}
        self._breakers_lock = threading.Lock()
        self._inflight_idem: dict[tuple[str, str, str, str], _Inflight] = {}
        self._inflight_idem_lock = threading.Lock()

        def counting_infer(model: str, version: str, batch: list[tuple[float, ...]]):
            results = self.backend.infer_batch(model, version, batch)
            self.metrics.inc("batches.executed")
            self.metrics.inc("batches.items", len(batch))
            return results

        self.batcher = DynamicBatcher(
            counting_infer,
            max_batch_size=max_batch_size,
            max_wait_s=max_wait_s,
            max_queue=max_queue,
        )

    # ---- 版本生命周期管理 ----

    def load_version(self, model: str, version: str, *, set_stable: bool = False) -> None:
        self.registry.register(model, version)
        self.registry.mark_ready(model, version, set_stable=set_stable)
        self.metrics.inc("versions.loaded")

    def retire_version(self, model: str, version: str, *, timeout_s: float = 10.0) -> None:
        # 若下线的是当前灰度版本，先原子摘除灰度配置，全部流量干净回到稳定版本，
        # 再执行卸载；不会出现路由仍指向灰度、灰度却拒绝服务的空窗。
        cfg = self.canary.get(model)
        if cfg is not None and cfg.canary_version == version:
            self.canary.disable(
                model, reason=f"canary version {version} retired"
            )
            self.metrics.inc("canary.disabled")
        self.registry.retire(model, version, timeout_s=timeout_s)
        # 版本下线后清除其幂等记录，不得复用旧版本结果
        self.idempotency.evict_version(model, version)
        self.metrics.inc("versions.retired")

    # ---- 灰度发布 ----

    def enable_canary(
        self, model: str, stable_version: str, canary_version: str, percent: int
    ) -> CanaryConfig:
        """为模型开启/更新灰度。两个版本都必须处于 READY 才能放量。"""
        self.registry.require_ready(model, stable_version)
        self.registry.require_ready(model, canary_version)
        cfg = self.canary.enable(model, stable_version, canary_version, percent)
        self.metrics.inc("canary.configured")
        return cfg

    def adjust_canary_percent(self, model: str, percent: int) -> CanaryConfig:
        """随时在线调整灰度比例（0 即全部回到稳定版本）。"""
        cfg = self.canary.adjust_percent(model, percent)
        self.metrics.inc("canary.configured")
        if percent == 0:
            self.metrics.inc("canary.disabled")
        return cfg

    def disable_canary(self, model: str, *, reason: str = "manual") -> None:
        if self.canary.disable(model, reason=reason) is not None:
            self.metrics.inc("canary.disabled")

    def rollback_history(self, model: str | None = None):
        return self.canary.rollback_history(model)

    # ---- 推理 ----

    def infer(self, request: InferRequest) -> InferResponse:
        self.metrics.inc("requests.total")
        try:
            return self._infer(request)
        except GatewayError as e:
            self.metrics.inc(f"rejections.{e.code.value}")
            raise

    def _infer(self, request: InferRequest) -> InferResponse:
        if request.version is not None:
            # 显式指定版本：直达该版本，不参与灰度分流
            model, version = self.registry.resolve(request.model, request.version)
            return self._after_resolve(request, model, version, None)

        # 未指定版本：先按灰度规则分流，再解析（灰度版本恰好失效时安全落到稳定版本）
        model = request.model
        target, cfg = self.canary.route(model, request.tenant)
        try:
            resolved_model, version = self.registry.resolve(model, target)
        except GatewayError as e:
            if target is not None and e.code in (
                ErrorCode.VERSION_NOT_FOUND,
                ErrorCode.VERSION_RETIRED,
                ErrorCode.VERSION_NOT_READY,
            ):
                # 与下线/摘除并发：该请求改走稳定版本，不留空窗、不返回错误
                self.metrics.inc("canary.fallback_to_stable")
                resolved_model, version = self.registry.resolve(model, None)
            else:
                raise
        if target is not None and version == target:
            self.metrics.inc("canary.routed_canary")
        else:
            self.metrics.inc("canary.routed_stable")
        return self._after_resolve(request, resolved_model, version, cfg)

    def _after_resolve(
        self,
        request: InferRequest,
        model: str,
        version: str,
        cfg: CanaryConfig | None,
    ) -> InferResponse:
        if request.idempotency_key is not None:
            cached = self.idempotency.get(
                model, version, request.tenant, request.idempotency_key
            )
            if cached is not None:
                self.metrics.inc("idempotency.hit")
                return InferResponse(
                    request_id=request.request_id,
                    model=cached.model,
                    version=cached.version,
                    outputs=cached.outputs,
                    from_idempotency_cache=True,
                )
            return self._infer_idempotent(request, model, version, cfg)
        return self._execute(request, model, version, cfg)

    def _infer_idempotent(
        self,
        request: InferRequest,
        model: str,
        version: str,
        cfg: CanaryConfig | None,
    ) -> InferResponse:
        """相同幂等 key 的在途请求去重：只执行一次，等待者共享结果。"""
        key = (model, version, request.tenant, request.idempotency_key)  # type: ignore[arg-type]
        with self._inflight_idem_lock:
            holder = self._inflight_idem.get(key)
            if holder is None:
                holder = _Inflight(event=threading.Event())
                self._inflight_idem[key] = holder
                leader = True
            else:
                leader = False
                self.metrics.inc("idempotency.inflight_waiters")
        if not leader:
            holder.event.wait(timeout=request.timeout_s + 30)
            if holder.error is not None:
                raise holder.error
            assert holder.response is not None
            self.metrics.inc("idempotency.hit")
            return InferResponse(
                request_id=request.request_id,
                model=holder.response.model,
                version=holder.response.version,
                outputs=holder.response.outputs,
                from_idempotency_cache=True,
            )
        try:
            response = self._execute(request, model, version, cfg)
            self.idempotency.put(
                model, version, request.tenant, request.idempotency_key, response  # type: ignore[arg-type]
            )
            holder.response = response
            return response
        except BaseException as e:
            holder.error = e
            raise
        finally:
            with self._inflight_idem_lock:
                self._inflight_idem.pop(key, None)
            holder.event.set()

    def _execute(
        self,
        request: InferRequest,
        model: str,
        version: str,
        cfg: CanaryConfig | None = None,
    ) -> InferResponse:
        is_canary = cfg is not None and cfg.canary_version == version
        self.registry.acquire(model, version)
        try:
            breaker = self._breaker(model, version)
            allowed, probe = breaker.allow_request()
            if not allowed:
                self.metrics.inc("breaker.rejected")
                self.window.record_rejection(model, version, ErrorCode.CIRCUIT_OPEN.value)
                if is_canary:
                    # 灰度版本熔断打开（只可能由真实后端连续失败导致）：作为
                    # 失败率判定的补充信号立即回退，避免快速失败攒不够窗口样本
                    self._rollback_canary_if_unhealthy(
                        model, version, cfg, breaker_open=True
                    )
                raise GatewayError(
                    ErrorCode.CIRCUIT_OPEN,
                    f"circuit open for {model}/{version}, fast failing",
                )
            if probe:
                self.metrics.inc("breaker.probes")
            try:
                response = self.batcher.submit(request, version)
            except GatewayError as e:
                # 只有后端故障计入熔断；背压/超时/批次约束不计
                if e.code == ErrorCode.BACKEND_FAILURE:
                    breaker.record_failure(probe=probe)
                    self.metrics.inc("breaker.failures")
                    # 窗口统计：只认真实后端故障
                    self.window.record_backend_failure(model, version)
                    if is_canary:
                        self._rollback_canary_if_unhealthy(
                            model,
                            version,
                            cfg,
                            breaker_open=breaker.state == BreakerState.OPEN,
                        )
                else:
                    # 非法输入/等待超时/背压等拒绝：分原因记录，但不进失败率
                    self.window.record_rejection(model, version, e.code.value)
                raise
            breaker.record_success(probe=probe)
            self.window.record_success(model, version)
            if probe:
                # 恢复探测成功不计入正常成功流量
                self.metrics.inc("breaker.probe_success")
            else:
                self.metrics.inc("requests.success")
            return response
        finally:
            self.registry.release(model, version)

    def _rollback_canary_if_unhealthy(
        self,
        model: str,
        version: str,
        cfg: CanaryConfig,
        *,
        breaker_open: bool,
    ) -> None:
        """灰度版本不健康时原子回退并留档；满足任一信号即触发。

        触发信号（都只源于灰度版本自身的真实后端故障）：
        1. 窗口内真实后端失败率达阈值且样本数足够；
        2. 该版本熔断器已打开（连续后端失败），用于样本尚未攒够但故障
           已很明确的情形，避免熔断快速失败导致回退永远无法触发。
        非法输入、等待超时、背压等拒绝都不会进入这两条路径。

        按 epoch 去重：同一轮灰度最多一条回退记录、只摘除一次。
        """
        threshold, min_samples = self._rollback_conf
        view = self.window.window_view(model, version)
        rate_ok = view["decided"] >= min_samples and view["failure_rate"] >= threshold
        if not (rate_ok or breaker_open):
            return
        removed = self.canary.disable_if_epoch(model, cfg.epoch)
        if removed is None:
            return  # 已被其他失败线程/人工操作处理
        if rate_ok:
            reason = (
                f"backend failure rate {view['failure_rate']:.0%} over threshold "
                f"{threshold:.0%} within {view['window_s']}s window "
                f"({view['backend_failures']}/{view['decided']} decided requests)"
            )
        else:
            reason = (
                f"circuit breaker opened for {model}/{version} due to consecutive "
                f"backend failures before window reached {min_samples} samples"
            )
        record = RollbackRecord(
            timestamp=time.time(),
            model=model,
            canary_version=version,
            stable_version=removed.stable_version,
            reason=reason,
            failure_rate=view["failure_rate"],
            samples=view["decided"],
            epoch=cfg.epoch,
        )
        self.canary.record_rollback(record)
        self.metrics.inc("canary.rollbacks")
        self.metrics.inc("canary.disabled")

    def _breaker(self, model: str, version: str) -> CircuitBreaker:
        with self._breakers_lock:
            br = self._breakers.get((model, version))
            if br is None:
                threshold, recovery = self._breaker_conf
                br = CircuitBreaker(
                    failure_threshold=threshold, recovery_timeout_s=recovery
                )
                self._breakers[(model, version)] = br
            return br

    # ---- 观测 ----

    def snapshot(self) -> dict:
        with self._breakers_lock:
            breakers = {
                f"{m}/{v}": br.state.value for (m, v), br in self._breakers.items()
            }
        return {
            "counters": self.metrics.snapshot(),
            "versions": self.registry.snapshot(),
            "breakers": breakers,
            "idempotency_size": len(self.idempotency),
            "canary": self.canary.snapshot(),
            "windows": self.window.snapshot(),
        }

    def close(self) -> None:
        self.batcher.close()
