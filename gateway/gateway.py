"""网关编排：路由 -> 幂等 -> 熔断 -> 动态批量 -> 指标。

一次请求的确定性路径：
1. registry.resolve 确定目标版本（未指定时落到当前稳定版本）；
2. 幂等键命中则直接复用结果，未命中则首个请求成为 leader；
3. 熔断器放行后进入该 (model, version, tenant) 专属的 Batcher；
4. 结果写入幂等记录并释放版本在途槽位。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from .backend import InferenceBackend
from .batcher import Batcher
from .breaker import CircuitBreaker
from .errors import ErrorCode, GatewayError
from .idempotency import IdempotencyStore
from .metrics import Metrics
from .registry import VersionEntry, VersionRegistry


@dataclass(frozen=True)
class GatewayConfig:
    max_batch_size: int = 8
    max_wait_ms: float = 5.0
    max_queue: int = 64
    queue_timeout: float = 1.0
    request_timeout: float = 5.0
    breaker_failure_threshold: int = 3
    breaker_cooldown_seconds: float = 1.0
    idempotency_capacity: int = 1024


@dataclass(frozen=True)
class InferRequest:
    model: str
    input: dict[str, Any]
    tenant: str = "default"
    version: str | None = None
    idempotency_key: str | None = None
    timeout: float | None = None


@dataclass(frozen=True)
class InferResponse:
    result: Any
    model: str
    version: str
    replayed: bool  # 是否命中幂等记录


class Gateway:
    def __init__(self, backend: InferenceBackend, config: GatewayConfig | None = None) -> None:
        self.config = config or GatewayConfig()
        self.backend = backend
        self.registry = VersionRegistry()
        self.metrics = Metrics()
        self.idempotency = IdempotencyStore(self.config.idempotency_capacity)
        self._breakers: dict[tuple[str, str], CircuitBreaker] = {}
        self._batchers: dict[tuple[str, str, str], Batcher] = {}
        self._lock = asyncio.Lock()

    # ---- 版本生命周期管理 ----

    async def load_version(self, model: str, version: str) -> None:
        await self.registry.register(model, version)
        # 本地后端无需真实加载，登记后即就绪；真实场景此处做权重加载。
        await self.registry.mark_ready(model, version)

    async def set_stable(self, model: str, version: str) -> None:
        await self.registry.set_stable(model, version)

    async def unload_version(self, model: str, version: str) -> None:
        """卸载：等待在途请求结束，清理幂等记录与批量器，状态确定无中间态。"""
        await self.registry.unload(model, version)
        removed = await self.idempotency.drop_version(model, version)
        if removed:
            await self.metrics.inc("idempotency_dropped_total", removed, model=model, version=version)
        async with self._lock:
            keys = [k for k in self._batchers if k[0] == model and k[1] == version]
            batchers = [self._batchers.pop(k) for k in keys]
        for b in batchers:
            await b.close()

    # ---- 推理入口 ----

    async def infer(self, request: InferRequest) -> InferResponse:
        cfg = self.config
        await self.metrics.inc("requests_total", model=request.model, tenant=request.tenant)
        entry: VersionEntry | None = None
        try:
            entry = await self.registry.resolve(request.model, request.version)
        except GatewayError as exc:
            await self._reject(exc)
            raise
        try:
            return await self._infer_on_entry(entry, request)
        finally:
            await self.registry.release(entry)

    async def _infer_on_entry(self, entry: VersionEntry, request: InferRequest) -> InferResponse:
        model, version = entry.model, entry.version
        idem_key = None
        if request.idempotency_key is not None:
            idem_key = (model, version, request.tenant, request.idempotency_key)
            record, is_leader = await self.idempotency.get_or_begin(idem_key)
            if not is_leader:
                await self.metrics.inc("idempotency_hits_total", model=model, version=version)
                result = await self.idempotency.result_of(record)
                return InferResponse(result=result, model=model, version=version, replayed=True)
        try:
            result = await self._execute(entry, request)
        except GatewayError as exc:
            if idem_key is not None:
                await self.idempotency.fail(idem_key, record, exc)
            await self._reject(exc)
            raise
        if idem_key is not None:
            await self.idempotency.complete(idem_key, record, result)
        return InferResponse(result=result, model=model, version=version, replayed=False)

    async def _execute(self, entry: VersionEntry, request: InferRequest) -> Any:
        model, version = entry.model, entry.version
        breaker = await self._breaker_for(model, version)
        allowed, is_probe = await breaker.try_acquire()
        if not allowed:
            await self.metrics.inc("circuit_rejected_total", model=model, version=version)
            raise GatewayError(ErrorCode.CIRCUIT_OPEN, f"circuit open for {model}/{version}")
        if is_probe:
            await self.metrics.inc("breaker_probe_total", model=model, version=version)
        batcher = await self._batcher_for(model, version, request.tenant)
        timeout = request.timeout if request.timeout is not None else self.config.request_timeout
        try:
            return await asyncio.wait_for(batcher.submit(request.input), timeout)
        except asyncio.TimeoutError:
            await self.metrics.inc("timeouts_total", model=model, version=version)
            raise GatewayError(ErrorCode.QUEUE_TIMEOUT, "request timed out in queue") from None

    async def _run_batch(self, model: str, version: str, payloads: list[dict[str, Any]]) -> list[Any]:
        breaker = await self._breaker_for(model, version)
        is_probe = breaker.state.value == "HALF_OPEN"
        try:
            results = await self.backend.infer_batch(model, version, payloads)
        except Exception:
            await breaker.record_failure(is_probe)
            if breaker.state.value == "OPEN":
                await self.metrics.inc("circuit_open_total", model=model, version=version)
            raise
        await breaker.record_success(is_probe)
        await self.metrics.inc("batches_total", model=model, version=version)
        await self.metrics.inc("batch_items_total", len(payloads), model=model, version=version)
        return results

    async def _breaker_for(self, model: str, version: str) -> CircuitBreaker:
        key = (model, version)
        async with self._lock:
            if key not in self._breakers:
                self._breakers[key] = CircuitBreaker(
                    self.config.breaker_failure_threshold,
                    self.config.breaker_cooldown_seconds,
                )
            return self._breakers[key]

    async def _batcher_for(self, model: str, version: str, tenant: str) -> Batcher:
        key = (model, version, tenant)
        async with self._lock:
            batcher = self._batchers.get(key)
            if batcher is None:
                batcher = Batcher(
                    model,
                    version,
                    tenant,
                    run_batch=lambda payloads, m=model, v=version: self._run_batch(m, v, payloads),
                    max_batch_size=self.config.max_batch_size,
                    max_wait_ms=self.config.max_wait_ms,
                    max_queue=self.config.max_queue,
                    queue_timeout=self.config.queue_timeout,
                )
                batcher.start()
                self._batchers[key] = batcher
            return batcher

    async def _reject(self, exc: GatewayError) -> None:
        await self.metrics.inc("rejected_total", reason=exc.code.value)

    # ---- 可观测性 ----

    async def snapshot(self) -> dict[str, Any]:
        versions = await self.registry.snapshot()
        return await self.metrics.snapshot(versions)

    async def close(self) -> None:
        async with self._lock:
            batchers = list(self._batchers.values())
            self._batchers.clear()
        for b in batchers:
            await b.close()
