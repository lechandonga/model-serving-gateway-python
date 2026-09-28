# 本地模型推理网关（model-serving-gateway-python）

一套可离线运行的本地模型推理网关：并发请求在版本切换、动态批量执行、过载背压与后端故障降级时，都能得到一致且可解释的结果。推理后端为本地确定性哈希模型，不依赖外部服务、网络模型仓库或真实凭据。

## 快速开始

```bash
uv sync
uv run uvicorn main:app          # 启动服务（默认 127.0.0.1:8000）
uv run pytest tests/ -q          # 运行全部场景测试
```

## 请求协议

### 推理：`POST /v1/infer`

```json
{
  "model": "m",                 // 必填，模型名
  "inputs": [1.0, 2.0],         // 必填，浮点输入向量（上限 1024 维）
  "version": "v1",              // 可选；缺省路由到当前稳定版本
  "tenant": "default",          // 可选，租户隔离合批
  "idempotency_key": "k1",      // 可选，幂等标识
  "timeout_s": 5.0              // 可选，排队+执行总等待上限
}
```

成功响应：`{request_id, model, version, outputs, from_idempotency_cache}`。

失败响应：`{"error": {"code": ..., "message": ...}}`，错误码与 HTTP 状态一一对应、互相可区分：

| 错误码 | HTTP | 含义 |
|---|---|---|
| `MODEL_NOT_FOUND` | 404 | 模型不存在 |
| `VERSION_NOT_FOUND` | 404 | 版本号未知 |
| `VERSION_RETIRED` | 410 | 版本已下线 |
| `VERSION_NOT_READY` | 409 | 版本加载中/卸载中 |
| `OVERLOADED` | 429 | 队列满，背压拒绝 |
| `REQUEST_TIMEOUT` | 504 | 等待批次/执行超时 |
| `CIRCUIT_OPEN` | 503 | 熔断中，快速失败 |
| `BACKEND_FAILURE` | 502 | 后端推理失败 |
| `BATCH_CONSTRAINT` | 400 | 输入超出批次约束 |

### 管理接口

- `POST /admin/models/{model}/versions/{version}/load?set_stable=true`：加载并就绪版本。
- `POST /admin/models/{model}/versions/{version}/retire`：卸载版本（等待在途请求结束）。
- `PUT /admin/models/{model}/canary`：开启/更新灰度，body 为 `{stable_version, canary_version, percent}`。
- `POST /admin/models/{model}/canary/percent?percent=30`：随时在线调整灰度比例（0 即干净回到稳定版本）。
- `DELETE /admin/models/{model}/canary?reason=manual`：关闭灰度，全部流量回到稳定版本。
- `GET /admin/models/{model}/rollbacks`：查询该模型的自动回退记录。
- `GET /admin/snapshot`：可观测性快照（计数器、版本状态、在途数、熔断器状态、幂等记录数、灰度配置/回退、窗口统计）。

## 版本生命周期

```
LOADING --mark_ready--> READY --retire--> UNLOADING --在途归零--> RETIRED
```

- `LOADING`/`UNLOADING` 明确拒绝新请求（`VERSION_NOT_READY`）；`RETIRED` 返回 `VERSION_RETIRED`，与未知版本（`VERSION_NOT_FOUND`）可区分。
- 卸载先切换状态拒绝新请求，再等待在途请求归零后才释放资源；所有状态迁移在同一把锁内完成，并发切换不会出现半加载/半释放。
- 未指定版本时路由到稳定指针；稳定版本下线后确定性回退到字典序最大的 READY 版本，规则在切换前后保持一致。

## 灰度发布（Canary）

在"整体切版本"之外，支持把一个模型的流量按比例分到**稳定版本**与一个**灰度版本**之间，可随时放量、缩量，并在灰度版本自身出故障时自动退回稳定版本。

### 配置方式

- `PUT /admin/models/{model}/canary`，body：`{"stable_version": "v1", "canary_version": "v2", "percent": 10}`。两个版本都必须已处于 READY。
- `percent` 取值 0..100，表示灰度流量百分比，可随时用 `POST .../canary/percent?percent=N` 在线调整：`percent=0` 即全部干净回到稳定版本（灰度配置保留，可再放量）；`DELETE .../canary` 为彻底关闭。
- 未指定 `version` 的请求才参与灰度分流；显式带 `version` 的请求直达指定版本，不受灰度影响。

### 比例与租户粘性怎么理解

- 分流是**确定性哈希**：对 `(model, 灰度轮次 epoch, tenant)` 做稳定哈希，映射到 0..99 共 100 个桶；桶号小于 `percent` 的租户走灰度版本。
- 因此同一批并发请求不会忽左忽右；**同一个租户在同一轮灰度里固定落在同一个版本**，避免它自己的状态在两个版本间来回跳。
- 桶位与比例无关，调整比例只是移动"分界线"：放量时原有灰度租户继续留在灰度（不会被踢回），只把跨线的新租户迁入；缩量同理。
- 更换灰度版本或关闭后重新开启会进入新一轮（epoch+1），桶位重新分配。

### 干净切换，无中间空窗

配置以不可变快照在锁内整体读取，请求看到的要么是旧配置、要么是新配置。灰度版本被下线（retire）或比例清零时，分流原子摘除/置零，所有请求立即回到稳定版本，不会出现"两边都算"或"谁都不接"的状态；即使与摘除并发、个别请求已选中灰度版本而它恰好不可用，该请求也会安全改走稳定版本。

### 灰度与稳定严格隔离

- **健康/熔断按版本分开**：灰度版本的过载、后端故障、未就绪只影响它那部分流量；稳定版本的熔断器、在途计数、窗口统计完全独立，不会被灰度带崩。
- **批处理按版本分桶**：合批键为 `(model, version, tenant)`，灰度与稳定、不同租户的请求绝不合到同一批。
- **幂等按版本且按租户隔离**：记录键为 `(model, version, tenant, idempotency_key)`。同一幂等标识不会把稳定版本的结果复用给灰度版本；不同租户即便用了相同的 key 也各算各的，绝不把 A 租户的结果返回给 B 租户。

### 自动回退条件

网关为每个 `(model, version)` 维护最近一个滑动窗口（默认 10s，可由 `canary_window_s` 配置）的统计：请求数、成功数、**真实后端失败数**与各类拒绝原因。满足以下任一信号即自动把灰度流量降为 0、原子回退到稳定版本，并写一条可查记录（`GET /admin/models/{model}/rollbacks`，含时间、模型、灰度/稳定版本、原因、失败率、样本数、轮次）：

1. 灰度版本在窗口内拿到后端确定结果的样本（成功+后端失败）达到 `canary_min_samples`（默认 3），且其中**真实后端失败率**达到 `canary_failure_threshold`（默认 50%）；
2. 灰度版本的熔断器因连续后端失败而打开（只可能由真实后端故障导致）——用于样本还没攒够但故障已很明确的场景，避免熔断快速失败后永远攒不够样本而退不回去。

**只认真实后端故障**（后端返回失败）。客户端非法输入（`BATCH_CONSTRAINT`）、请求等待超时（`REQUEST_TIMEOUT`）、背压拒绝（`OVERLOADED`）、熔断快速失败（`CIRCUIT_OPEN`）都只按拒绝原因计数，既不进失败率分子也不进分母，少量坏请求或排队抖动不会把灰度打回去。回退后稳定版本继续正常服务，不受影响；同一轮灰度最多产生一条回退记录。

### 观测快照里的灰度字段

`GET /admin/snapshot` 除原有字段外新增：

- `canary.canaries`：每个开启灰度的模型 -> `{stable_version, canary_version, percent, epoch, updated_at}`；无灰度时为空对象。
- `canary.rollbacks`：全部自动回退记录列表。
- `windows`：每个 `model/version` 的窗口视图 -> `{requests, success, backend_failures, rejections:{<错误码>: 次数}, decided, failure_rate, window_s}`，灰度版本与稳定版本各自一条。
- `counters` 中新增 `canary.configured`、`canary.disabled`、`canary.rollbacks`、`canary.routed_canary`、`canary.routed_stable`、`canary.fallback_to_stable`。

## 批量执行

- 仅同 `(model, version, tenant)` 的请求可合批；其余请求各自成桶，绝不合并。
- 批大小达到 `max_batch_size`（默认 8）立即执行，否则等待 `max_wait_s`（默认 50ms）窗口。
- 批内单条失败（如输入含 NaN）只影响该条；每条请求的输入在执行前拷贝，请求间不共享可变状态。

## 背压与熔断

- **背压**：全局在队请求数超过 `max_queue`（默认 64）即以 `OVERLOADED` 拒绝；等待超时返回 `REQUEST_TIMEOUT`；超时请求会被移出队列，内存占用有上界。
- **熔断**：同一 `(model, version)` 后端连续失败达到阈值（默认 3 次）即熔断，熔断期请求以 `CIRCUIT_OPEN` 快速失败；冷却（默认 1s）后进入半开，只放行一个探测请求——探测成功则闭合（探测成功不计入正常成功流量，单独记 `breaker.probe_success`），探测失败则重新熔断计时。任何成功都会清零连续失败计数，单次抖动不会永久熔断。

## 幂等性

- 相同 `(model, version, idempotency_key)` 的请求只产生一次推理效果：在途相同 key 的请求会去重，等待者共享首个请求的结果；已完成的记录写入有界 LRU 存储（默认容量 1024）。
- 记录按版本隔离，版本下线时清除该版本全部记录，不会复用旧版本结果。

## 可观测性

`GET /admin/snapshot` 返回：

- `counters`：`requests.total`、`requests.success`、`rejections.<CODE>`、`batches.executed`、`batches.items`、`breaker.*`、`idempotency.*`、`versions.*`、`canary.*` 等计数，全部在锁内递增，并发更新不丢失不重复。
- `versions`：各模型的版本状态与在途请求数。
- `breakers`：各 `(model, version)` 熔断器状态。
- `idempotency_size`：当前幂等记录数。
- `canary`：灰度配置与自动回退记录；`windows`：各版本滑动窗口的请求/成功/后端失败/拒绝统计。

## 与既有能力的兼容范围

- 不配置灰度时行为与之前完全一致：未指定版本即走稳定版本，生命周期、批处理、背压、熔断、幂等逻辑不变。
- 灰度是在路由前新增的一层"版本选择"，下游全部既有机制（注册表状态机、动态批处理、熔断器、幂等存储、错误码映射）原样复用，且按版本天然隔离。
- 显式指定 `version` 的请求不参与灰度，仍可直达任意 READY 版本；幂等键维度从 `(model, version, key)` 扩展为 `(model, version, tenant, key)`，对默认租户 `default` 的既有调用结果无影响。

## 验证方法

```bash
uv run pytest tests/ -v -s   # -s 可查看单测日志中的请求输入与关键判定依据
```

| 测试文件 | 覆盖场景 |
|---|---|
| `tests/test_batching.py` | 并发合批、租户/版本隔离、批内部分失败、输入状态隔离 |
| `tests/test_versioning.py` | 稳定版本路由、未知/下线/未就绪拒绝、确定性回退、卸载等待在途 |
| `tests/test_overload.py` | 队列满背压、超时可区分、批次约束、队列内存有界 |
| `tests/test_breaker.py` | 连续失败熔断、快速失败、探测恢复、探测失败重熔断、单次抖动不熔断 |
| `tests/test_idempotency.py` | 单次推理效果、并发去重、容量有界、版本隔离与下线清除 |
| `tests/test_canary.py` | 按比例分流、比例调整、租户粘性、灰度/稳定隔离（熔断/幂等）、非后端故障不回退、自动回退（失败率与熔断两种触发）、回退后稳定版本仍可用、并发窗口统计精确 |

本地手动验证灰度与自动回退：

```bash
uv run uvicorn main:app --port 8000 &
curl -s -XPOST 'localhost:8000/admin/models/m/versions/v1/load?set_stable=true'
curl -s -XPOST 'localhost:8000/admin/models/m/versions/v2/load'
curl -s -XPUT  localhost:8000/admin/models/m/canary \
  -H 'Content-Type: application/json' \
  -d '{"stable_version":"v1","canary_version":"v2","percent":10}'
for i in $(seq 1 20); do
  curl -s -XPOST localhost:8000/v1/infer -H 'Content-Type: application/json' \
    -d "{\"model\":\"m\",\"inputs\":[1.0],\"tenant\":\"t$((i % 5))\"}"
done
curl -s 'localhost:8000/admin/models/m/canary/percent?percent=50' -XPOST  # 在线放量
curl -s localhost:8000/admin/snapshot                                    # 看 canary/windows
```

## 代码结构

```
gateway/
  errors.py       错误码与异常类型
  types.py        请求/响应结构与版本状态枚举
  backend.py      本地确定性推理后端（支持故障注入）
  registry.py     版本生命周期与稳定版本路由
  batcher.py      动态批处理与背压
  breaker.py      熔断器
  idempotency.py  有界幂等存储（按 model/version/tenant/key 隔离）
  metrics.py      线程安全计数器
  window.py       按版本的滑动窗口统计（成功/后端失败/各类拒绝）
  canary.py       灰度配置、按比例+租户粘性分流、回退记录
  gateway.py      组合上述组件的门面（含自动回退判定）
  app.py          FastAPI 接入层
```
