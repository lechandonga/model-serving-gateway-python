"""灰度发布：稳定版本与灰度版本之间按比例、按租户粘性的确定性分流。

- 分流由 (model, epoch, tenant) 的稳定哈希决定：
  同一租户在同一轮灰度（epoch）内固定落在同一个桶；比例调整只改变
  "前 percent 个桶去灰度"这道分界线，因此只迁移跨线租户，其余租户不跳版本。
- 配置快照不可变，读取在锁内取整份快照：并发请求看到的要么是旧配置、
  要么是新配置，不会看到半更新状态。
- 灰度下线/自动回退统一走 disable（移除配置）；比例清零走 adjust_percent(0)
  （保留配置）。两种方式后 route 都只返回稳定版本，切换不存在
  两边都算或无人承接的中间状态。
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class CanaryConfig:
    """某模型的一轮灰度配置（不可变快照）。"""

    model: str
    stable_version: str
    canary_version: str
    percent: int                      # 0..100，灰度流量百分比
    epoch: int                        # 灰度轮次代号；版本换新或重新开启时递增
    updated_at: float


@dataclass(frozen=True)
class RollbackRecord:
    """一次自动回退的可查记录：何时、哪个模型/灰度版本、为何回退。"""

    timestamp: float
    model: str
    canary_version: str
    stable_version: str
    reason: str
    failure_rate: float
    samples: int
    epoch: int


def bucket_for_tenant(model: str, epoch: int, tenant: str) -> int:
    """把租户确定性映射到 [0, 100) 的桶；同一轮灰度内结果固定。

    结果只依赖 (model, epoch, tenant)，不依赖当前比例：因此调整比例时
    同一租户的桶位不变，只是桶位与分界线的关系可能变化。
    """
    digest = hashlib.sha256(f"{model}|{epoch}|{tenant}".encode()).digest()
    return int.from_bytes(digest[:4], "big") % 100


class CanaryRouter:
    """管理每个模型的灰度配置并执行确定性、租户粘性的分流。"""

    def __init__(self, time_func=time.monotonic) -> None:
        self._now = time_func
        self._lock = threading.Lock()
        self._configs: dict[str, CanaryConfig] = {}
        self._rounds: dict[str, int] = {}       # 每模型单调递增的轮次
        self._rollbacks: list[RollbackRecord] = []

    # ---- 配置管理 ----

    def enable(
        self, model: str, stable_version: str, canary_version: str, percent: int
    ) -> CanaryConfig:
        """开启或更新灰度；参数非法时抛 ValueError，返回新配置快照。

        同 (stable, canary) 的重复开启沿用同一 epoch（仅改比例）；
        更换灰度版本或重新开启则进入新一轮（epoch+1）。
        """
        if not 0 <= percent <= 100:
            raise ValueError("percent must be within [0, 100]")
        if stable_version == canary_version:
            raise ValueError("stable_version and canary_version must differ")
        with self._lock:
            cur = self._configs.get(model)
            if (
                cur is not None
                and cur.stable_version == stable_version
                and cur.canary_version == canary_version
            ):
                epoch = cur.epoch
            else:
                epoch = self._rounds.get(model, 0) + 1
                self._rounds[model] = epoch
            cfg = CanaryConfig(
                model=model,
                stable_version=stable_version,
                canary_version=canary_version,
                percent=percent,
                epoch=epoch,
                updated_at=self._now(),
            )
            self._configs[model] = cfg
            return cfg

    def adjust_percent(self, model: str, percent: int) -> CanaryConfig:
        """在线调整灰度比例（0 即干净回退到稳定版本，配置保留以便重新放量）。"""
        if not 0 <= percent <= 100:
            raise ValueError("percent must be within [0, 100]")
        with self._lock:
            cur = self._configs.get(model)
            if cur is None:
                raise ValueError(f"no canary configured for model {model}")
            cfg = CanaryConfig(
                model=cur.model,
                stable_version=cur.stable_version,
                canary_version=cur.canary_version,
                percent=percent,
                epoch=cur.epoch,
                updated_at=self._now(),
            )
            self._configs[model] = cfg
            return cfg

    def disable(self, model: str, *, reason: str = "") -> CanaryConfig | None:
        """关闭灰度（灰度版本下线/自动回退的统一入口）。

        原子移除配置：之后所有 route 都只回稳定版本，不存在半开启状态。
        返回被关闭的配置；本来就没有配置时返回 None。
        """
        with self._lock:
            return self._configs.pop(model, None)

    def disable_if_epoch(self, model: str, epoch: int) -> CanaryConfig | None:
        """仅当当前灰度轮次与 epoch 一致时原子摘除（自动回退去重用）。

        多个失败线程同时触发、或期间已被人工调整到新一轮时，只有一次摘除生效，
        保证同一轮灰度最多产生一条回退记录。
        """
        with self._lock:
            cur = self._configs.get(model)
            if cur is None or cur.epoch != epoch:
                return None
            return self._configs.pop(model)

    def get(self, model: str) -> CanaryConfig | None:
        with self._lock:
            return self._configs.get(model)

    # ---- 分流 ----

    def route(self, model: str, tenant: str) -> tuple[str | None, CanaryConfig | None]:
        """返回 (目标版本, 生效的灰度配置)。

        - 命中灰度：(canary_version, config)
        - 未命中或灰度关闭：(None, config_or_None)，None 表示由调用方按稳定版本路由
        """
        with self._lock:
            cfg = self._configs.get(model)
            if cfg is None or cfg.percent <= 0:
                return None, cfg
            if bucket_for_tenant(model, cfg.epoch, tenant) < cfg.percent:
                return cfg.canary_version, cfg
            return None, cfg

    # ---- 回退记录 ----

    def record_rollback(self, record: RollbackRecord) -> None:
        with self._lock:
            self._rollbacks.append(record)

    def rollback_history(self, model: str | None = None) -> list[RollbackRecord]:
        with self._lock:
            if model is None:
                return list(self._rollbacks)
            return [r for r in self._rollbacks if r.model == model]

    # ---- 观测 ----

    def snapshot(self) -> dict:
        with self._lock:
            canaries = {
                model: {
                    "stable_version": cfg.stable_version,
                    "canary_version": cfg.canary_version,
                    "percent": cfg.percent,
                    "epoch": cfg.epoch,
                    "updated_at": cfg.updated_at,
                }
                for model, cfg in self._configs.items()
            }
            rollbacks = [
                {
                    "timestamp": r.timestamp,
                    "model": r.model,
                    "canary_version": r.canary_version,
                    "stable_version": r.stable_version,
                    "reason": r.reason,
                    "failure_rate": r.failure_rate,
                    "samples": r.samples,
                    "epoch": r.epoch,
                }
                for r in self._rollbacks
            ]
        return {"canaries": canaries, "rollbacks": rollbacks}
