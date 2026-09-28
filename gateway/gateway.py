"""网关门面：组合注册表、熔断器、幂等存储、批处理器、指标与灰度发布。

请求处理顺序：
1. 版本解析（未指定版本时先走灰度分流；未知/下线/未就绪在此拒绝，错误码可区分）
2. 幂等命中检查（含在途相同 key 的去重，只产生一次推理效果；按版本+租户隔离）
3. 在途计数 acquire（与卸载互斥）
4. 熔断检查（OPEN 快速失败；HALF_OPEN 仅放行探测；熔断器按版本各自独立）
5. 动态批处理执行（背压/超时/批次约束在此拒绝）
6. 结果写幂等存储、记录窗口统计、按窗口失败率评估灰度自动回退、释放资源
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .backend import DeterministicBackend
from .batcher import DynamicBatcher
from .breaker import CircuitBreaker
from .canary import CanaryConfig, CanaryManager, RollbackRecord
from .errors import ErrorCode, GatewayError
from .idempotency import IdempotencyStore
from .metrics import Metrics
from .registry import VersionRegistry
from .types import InferRequest, InferResponse, VersionState
from .window import WindowRegistry


@dataclass
class _Inflight:
    event: threading.Event
    response: InferResponse | None = None
    error: BaseException | None = None


# 批内单条非法输入（错误码仍为 BACKEND_FAILURE，但 backend_fault=False）
# 在窗口统计里使用独立原因，避免被误读为真实后端故障。
_INVALID_INPUT_REASON = "INVALID_INPUT"


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
        canary_window_s: float = 60.0,
        canary_failure_rate_threshold: float = 0.5,
        canary_min_requests: int = 5,
    ) -> None:
        self.backend = backend or DeterministicBackend()
        self.registry = VersionRegistry()
        self.metrics = Metrics()
        self.idempotency = IdempotencyStore(capacity=idempotency_capacity)
        self._breaker_conf = (breaker_failure_threshold, breaker_recovery_timeout_s)
        self._breakers: dict[tuple[str, str], CircuitBreaker] = {}
        self._breakers_lock = threading.Lock()
        self._inflight_idem: dict[tuple[str, str, str, str], _Inflight] = {}
        self._inflight_idem_lock = threading.Lock()

        # 灰度发布
        self.canary = CanaryManager()
        self.windows = WindowRegistry(window_s=canary_window_s)
        self._canary_threshold = canary_failure_rate_threshold
        self._canary_min_requests = canary_min_requests

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
        # 若被下线的版本正参与灰度，先在锁外移除灰度配置：后续未指定版本的请求
        # 按既有稳定/回退规则路由，不出现“无人承接”或两边都算的中间状态。
        config = self.canary.get(model)
        if config is not None and version in (
            config.stable_version,
            config.canary_version,
        ):
            self.canary.disable(model)
            self.metrics.inc("canary.disabled")
        self.registry.retire(model, version, timeout_s=timeout_s)
        # 版本下线后清除其幂等记录，不得复用旧版本结果
        self.idempotency.evict_version(model, version)
        self.metrics.inc("versions.retired")

    # ---- 灰度发布管理 ----

    def configure_canary(
        self,
        model: str,
        canary_version: str,
        percentage: int,
        *,
        stable_version: str | None = None,
    ) -> CanaryConfig:
        """为模型配置灰度：稳定版本缺省取注册表当前稳定指针，两版本都须 READY。"""
        if stable_version is None:
            stable = self.registry.stable_version(model)
            if stable is None:
                raise GatewayError(
                    ErrorCode.VERSION_NOT_READY,
                    f"model {model} has no stable READY version",
                )
            stable_version = stable
        self._require_ready(model, stable_version, "stable")
        self._require_ready(model, canary_version, "canary")
        config = self.canary.configure(
            model, stable_version, canary_version, percentage
        )
        self.metrics.inc("canary.configured")
        return config

    def adjust_canary_percentage(self, model: str, percentage: int) -> CanaryConfig:
        """随时调整灰度比例（0..100）；调整原子生效，分流结果只取决于新配置。"""
        config = self.canary.adjust_percentage(model, percentage)
        self.metrics.inc("canary.adjusted")
        return config

    def rollback_canary(
        self, model: str, *, reason: str = "manual rollback"
    ) -> RollbackRecord | None:
        """手动把灰度比例清零并留回退记录。"""
        record = self.canary.rollback(model, reason)
        if record is not None:
            self.metrics.inc("canary.rolled_back")
        return record

    def disable_canary(self, model: str) -> None:
        """彻底关闭某模型灰度（配置移除），之后全部走稳定版本。"""
        self.canary.disable(model)
        self.metrics.inc("canary.disabled")

    def canary_rollback_records(self, model: str | None = None) -> list[RollbackRecord]:
        return self.canary.rollback_records(model)

    def _require_ready(self, model: str, version: str, role: str) -> None:
        state = self.registry.version_state(model, version)
        if state is None:
            raise GatewayError(
                ErrorCode.VERSION_NOT_FOUND,
                f"unknown {role} version: {model}/{version}",
            )
        if state != VersionState.READY:
            raise GatewayError(
                ErrorCode.VERSION_NOT_READY,
                f"{role} version {model}/{version} is {state.value}, not serving",
            )

    # ---- 推理 ----

    def infer(self, request: InferRequest) -> InferResponse:
        self.metrics.inc("requests.total")
        try:
            return self._infer(request)
        except GatewayError as e:
            self.metrics.inc(f"rejections.{e.code.value}")
            raise

    def _infer(self, request: InferRequest) -> InferResponse:
        model, version = self._route(request)
        window = self.windows.get(model, version)
        window.record_request()
        try:
            response = self._infer_idempotency_checked(request, model, version)
        except GatewayError as e:
            self._record_window_failure(model, version, e)
            self._maybe_auto_rollback(model, version)
            raise
        window.record_success()
        self._maybe_auto_rollback(model, version)
        return response

    def _route(self, request: InferRequest) -> tuple[str, str]:
        """解析目标版本：显式版本直接用；未指定版本先走灰度分流。

        分流只依赖 CanaryManager 中原子替换的配置快照与租户哈希，因此同一批
        并发请求、同一租户的结果确定。灰度版本若处于非 READY（例如正在下线），
        未指定版本的请求干净地回到稳定版本，不产生无人承接的空档。
        """
        if request.version is not None:
            return self.registry.resolve(request.model, request.version)

        chosen = self.canary.select(request.model, request.tenant)
        if chosen is None:
            return self.registry.resolve(request.model, None)

        # 灰度可能正在被下线：检测到非 READY 时，在同一处理路径内回落到该配置
        # 的稳定版本；稳定版本同样不可用时按正常错误码拒绝。
        state = self.registry.version_state(request.model, chosen)
        if state == VersionState.READY:
            return request.model, chosen
        config = self.canary.get(request.model)
        fallback = (
            config.stable_version
            if config is not None and config.stable_version != chosen
            else None
        )
        if fallback is not None and self.registry.version_state(
            request.model, fallback
        ) == VersionState.READY:
            self.metrics.inc("canary.failover_to_stable")
            return request.model, fallback
        return self.registry.resolve(request.model, chosen)

    def _infer_idempotency_checked(
        self, request: InferRequest, model: str, version: str
    ) -> InferResponse:
        if request.idempotency_key is None:
            return self._execute(request, model, version)
        cached = self.idempotency.get(model, version, request.tenant, request.idempotency_key)
        if cached is not None:
            self.metrics.inc("idempotency.hit")
            return InferResponse(
                request_id=request.request_id,
                model=cached.model,
                version=cached.version,
                outputs=cached.outputs,
                from_idempotency_cache=True,
            )
        return self._infer_idempotent(request, model, version)

    def _infer_idempotent(
        self, request: InferRequest, model: str, version: str
    ) -> InferResponse:
        """相同幂等 key 的在途请求去重：只执行一次，等待者共享结果。

        去重键含租户：不同租户即便使用相同标识也各自独立计算。
        """
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
            response = self._execute(request, model, version)
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

    def _execute(self, request: InferRequest, model: str, version: str) -> InferResponse:
        self.registry.acquire(model, version)
        try:
            breaker = self._breaker(model, version)
            allowed, probe = breaker.allow_request()
            if not allowed:
                self.metrics.inc("breaker.rejected")
                raise GatewayError(
                    ErrorCode.CIRCUIT_OPEN,
                    f"circuit open for {model}/{version}, fast failing",
                )
            if probe:
                self.metrics.inc("breaker.probes")
            try:
                response = self.batcher.submit(request, version)
            except GatewayError as e:
                # 只有真实后端故障计入熔断；批内非法输入、背压/超时/批次约束不计
                if e.code == ErrorCode.BACKEND_FAILURE and e.backend_fault:
                    breaker.record_failure(probe=probe)
                    self.metrics.inc("breaker.failures")
                raise
            breaker.record_success(probe=probe)
            if probe:
                # 恢复探测成功不计入正常成功流量
                self.metrics.inc("breaker.probe_success")
            else:
                self.metrics.inc("requests.success")
            return response
        finally:
            self.registry.release(model, version)

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

    # ---- 灰度窗口统计与自动回退 ----

    def _record_window_failure(
        self, model: str, version: str, error: GatewayError
    ) -> None:
        window = self.windows.get(model, version)
        if error.code == ErrorCode.BACKEND_FAILURE and error.backend_fault:
            # 只认灰度/该版本自身的真实后端故障
            window.record_backend_failure()
        else:
            # 非法输入、等待超时、背压、熔断等只按拒绝原因分桶，不算后端失败
            if error.code == ErrorCode.BACKEND_FAILURE:
                reason = _INVALID_INPUT_REASON
            else:
                reason = error.code.value
            window.record_rejection(reason)

    def _maybe_auto_rollback(self, model: str, version: str) -> None:
        """仅对当前灰度版本生效：窗口内真实后端失败率超阈值即自动回退。

        失败率分母只计“由后端健康状况定论”的请求（成功 + 真实后端失败 +
        熔断快速失败）：熔断快速失败（CIRCUIT_OPEN）本身就是该版本后端连续故障
        的直接信号，计入失败信号，避免熔断打开后再也凑不够判定样本；而非法输入、
        等待超时、背压拒绝等没有触达后端的请求不计入，既不会稀释失败率，
        也不会把客户端/排队问题误算到灰度头上。
        """
        config = self.canary.get(model)
        if config is None or config.canary_version != version or config.percentage == 0:
            return
        stats = self.windows.get(model, version).summary()
        circuit_open = stats["rejections"].get(ErrorCode.CIRCUIT_OPEN.value, 0)
        failures = stats["backend_failures"] + circuit_open
        decided = stats["successes"] + failures
        if decided < self._canary_min_requests:
            return
        rate = failures / decided
        if rate > self._canary_threshold:
            reason = (
                f"backend failure rate {rate:.0%} over threshold "
                f"{self._canary_threshold:.0%} within window"
            )
            record = self.canary.rollback(
                model, reason, failure_rate=rate, decided_requests=decided
            )
            if record is not None:
                self.metrics.inc("canary.rolled_back")

    # ---- 观测 ----

    def snapshot(self) -> dict:
        with self._breakers_lock:
            breakers = {
                f"{m}/{v}": br.state.value for (m, v), br in self._breakers.items()
            }
        canaries: dict[str, dict] = {}
        for model, config in self._all_canary_configs():
            canaries[model] = {
                "stable_version": config.stable_version,
                "canary_version": config.canary_version,
                "percentage": config.percentage,
                "active": config.percentage > 0,
            }
        rollbacks = [
            {
                "timestamp": r.timestamp,
                "model": r.model,
                "canary_version": r.canary_version,
                "reason": r.reason,
                "failure_rate": r.failure_rate,
                "decided_requests": r.decided_requests,
            }
            for r in self.canary.rollback_records()
        ]
        return {
            "counters": self.metrics.snapshot(),
            "versions": self.registry.snapshot(),
            "breakers": breakers,
            "idempotency_size": len(self.idempotency),
            "canaries": canaries,
            "canary_rollbacks": rollbacks,
            "windows": self.windows.summary_all(),
        }

    def _all_canary_configs(self) -> list[tuple[str, CanaryConfig]]:
        return list(self.canary.snapshot_configs().items())

    def close(self) -> None:
        self.batcher.close()
