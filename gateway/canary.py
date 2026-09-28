"""灰度发布：按比例分流、租户粘性、原子调整与自动回退记录。

分流语义：
- 每个模型一份不可变 CanaryConfig；配置变更以整体替换方式原子生效，
  并发请求不会读到半更新配置，因此同一批并发请求的分流结果一致。
- 分流键为 (model, tenant) 的稳定哈希：同一租户在一次灰度期间（配置未变）
  固定落在同一版本，获得租户粘性；比例调整时只在边界处迁移尽量少的租户，
  不随请求随机抖动。
- 比例为 0（或灰度被下线/回退）时全部落到稳定版本，不出现双算或无人承接。
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass

# 哈希桶数量：租户映射到 [0, _BUCKETS) 的固定桶，按比例切前缀给灰度。
_BUCKETS = 1 << 16


def _tenant_bucket(model: str, tenant: str) -> int:
    digest = hashlib.sha256(f"{model}|{tenant}".encode("utf-8")).digest()
    return int.from_bytes(digest[:2], "big")


@dataclass(frozen=True)
class CanaryConfig:
    """单个模型的灰度配置（不可变快照，替换时整体原子切换）。"""

    model: str
    stable_version: str
    canary_version: str
    percentage: int = 0  # 0..100，灰度流量百分比；0 表示全部回稳定版本

    def canary_cutoff(self) -> int:
        """灰度承接的桶前缀长度（桶下标 < cutoff 的租户去灰度版本）。"""
        return (self.percentage * _BUCKETS) // 100


@dataclass(frozen=True)
class RollbackRecord:
    """一次灰度回退的可查询记录（什么时候、哪个版本、因为什么回退）。"""

    timestamp: float
    model: str
    canary_version: str
    reason: str
    failure_rate: float
    decided_requests: int


class CanaryManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._configs: dict[str, CanaryConfig] = {}
        self._rollback_records: list[RollbackRecord] = []

    # ---- 配置（整体原子替换） ----

    def configure(
        self,
        model: str,
        stable_version: str,
        canary_version: str,
        percentage: int,
    ) -> CanaryConfig:
        """设置/替换某模型的灰度配置；非法比例或同版本立即报错。"""
        if not 0 <= percentage <= 100:
            raise ValueError("percentage must be within [0, 100]")
        if stable_version == canary_version:
            raise ValueError("stable and canary versions must differ")
        config = CanaryConfig(
            model=model,
            stable_version=stable_version,
            canary_version=canary_version,
            percentage=percentage,
        )
        with self._lock:
            self._configs[model] = config
        return config

    def adjust_percentage(self, model: str, percentage: int) -> CanaryConfig:
        """随时调整灰度比例；配置不存在或比例非法时报错。"""
        if not 0 <= percentage <= 100:
            raise ValueError("percentage must be within [0, 100]")
        with self._lock:
            current = self._configs.get(model)
            if current is None:
                raise KeyError(f"no canary configured for model {model}")
            updated = CanaryConfig(
                model=current.model,
                stable_version=current.stable_version,
                canary_version=current.canary_version,
                percentage=percentage,
            )
            self._configs[model] = updated
        return updated

    def get(self, model: str) -> CanaryConfig | None:
        with self._lock:
            return self._configs.get(model)

    # ---- 分流 ----

    def select(self, model: str, tenant: str) -> str | None:
        """返回该租户本次应去的版本；无灰度配置返回 None。

        结果只取决于配置快照与稳定哈希：配置不变则结果确定且租户粘性。
        """
        with self._lock:
            config = self._configs.get(model)
        if config is None:
            return None
        if config.percentage > 0 and _tenant_bucket(model, tenant) < config.canary_cutoff():
            return config.canary_version
        return config.stable_version

    # ---- 回退与下线 ----

    def rollback(
        self,
        model: str,
        reason: str,
        *,
        failure_rate: float = 0.0,
        decided_requests: int = 0,
    ) -> RollbackRecord | None:
        """把灰度比例原子清零并留一条回退记录；无配置返回 None。"""
        with self._lock:
            current = self._configs.get(model)
            # 无配置或该轮灰度比例已为 0（已回退过）：不再重复留记录，
            # 保证并发触发自动回退时只有一条记录、只生效一次。
            if current is None or current.percentage == 0:
                return None
            record = RollbackRecord(
                timestamp=time.time(),
                model=model,
                canary_version=current.canary_version,
                reason=reason,
                failure_rate=failure_rate,
                decided_requests=decided_requests,
            )
            self._rollback_records.append(record)
            self._configs[model] = CanaryConfig(
                model=current.model,
                stable_version=current.stable_version,
                canary_version=current.canary_version,
                percentage=0,
            )
        return record

    def disable(self, model: str) -> None:
        """彻底移除灰度配置（灰度版本下线时调用），之后全部走稳定版本。"""
        with self._lock:
            self._configs.pop(model, None)

    def snapshot_configs(self) -> dict[str, CanaryConfig]:
        """返回全部模型灰度配置的快照（观测用）。"""
        with self._lock:
            return dict(self._configs)

    def rollback_records(self, model: str | None = None) -> list[RollbackRecord]:
        """查询回退记录，可按模型过滤；按发生时间先后返回。"""
        with self._lock:
            if model is None:
                return list(self._rollback_records)
            return [r for r in self._rollback_records if r.model == model]
