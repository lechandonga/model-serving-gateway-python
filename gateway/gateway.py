"""网关门面：组合注册表、熔断器、幂等存储、批处理器与指标。

请求处理顺序：
1. 版本解析（未知/下线/未就绪在此拒绝，错误码可区分）
2. 幂等命中检查（含在途相同 key 的去重，只产生一次推理效果）
3. 在途计数 acquire（与卸载互斥）
4. 熔断检查（OPEN 快速失败；HALF_OPEN 仅放行探测）
5. 动态批处理执行（背压/超时/批次约束在此拒绝）
6. 结果写幂等存储并释放资源
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .backend import DeterministicBackend
from .batcher import DynamicBatcher
from .breaker import BreakerState, CircuitBreaker
from .errors import ErrorCode, GatewayError
from .idempotency import IdempotencyStore
from .metrics import Metrics
from .registry import VersionRegistry
from .types import InferRequest, InferResponse


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
    ) -> None:
        self.backend = backend or DeterministicBackend()
        self.registry = VersionRegistry()
        self.metrics = Metrics()
        self.idempotency = IdempotencyStore(capacity=idempotency_capacity)
        self._breaker_conf = (breaker_failure_threshold, breaker_recovery_timeout_s)
        self._breakers: dict[tuple[str, str], CircuitBreaker] = {}
        self._breakers_lock = threading.Lock()
        self._inflight_idem: dict[tuple[str, str, str], _Inflight] = {}
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
        self.registry.retire(model, version, timeout_s=timeout_s)
        # 版本下线后清除其幂等记录，不得复用旧版本结果
        self.idempotency.evict_version(model, version)
        self.metrics.inc("versions.retired")

    # ---- 推理 ----

    def infer(self, request: InferRequest) -> InferResponse:
        self.metrics.inc("requests.total")
        try:
            return self._infer(request)
        except GatewayError as e:
            self.metrics.inc(f"rejections.{e.code.value}")
            raise

    def _infer(self, request: InferRequest) -> InferResponse:
        model, version = self.registry.resolve(request.model, request.version)

        if request.idempotency_key is not None:
            cached = self.idempotency.get(model, version, request.idempotency_key)
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
        return self._execute(request, model, version)

    def _infer_idempotent(
        self, request: InferRequest, model: str, version: str
    ) -> InferResponse:
        """相同幂等 key 的在途请求去重：只执行一次，等待者共享结果。"""
        key = (model, version, request.idempotency_key)  # type: ignore[arg-type]
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
            self.idempotency.put(model, version, request.idempotency_key, response)  # type: ignore[arg-type]
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
                # 只有后端故障计入熔断；背压/超时/批次约束不计
                if e.code == ErrorCode.BACKEND_FAILURE:
                    breaker.record_failure(probe=probe)
                    self.metrics.inc("breaker.failures")
                raise
            breaker.record_success(probe=probe)
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
        }

    def close(self) -> None:
        self.batcher.close()
