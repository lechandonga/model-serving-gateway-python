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
- `POST /admin/models/{model}/canary`：配置灰度（body：`canary_version`、`percentage`、可选 `stable_version`）。
- `POST /admin/models/{model}/canary/adjust`：随时调整灰度比例（body：`percentage`，0..100）。
- `POST /admin/models/{model}/canary/rollback`：手动回退灰度（比例清零并留记录）。
- `DELETE /admin/models/{model}/canary`：彻底关闭灰度配置。
- `GET /admin/models/{model}/canary/rollbacks`：查询某模型的灰度回退记录。
- `GET /admin/snapshot`：可观测性快照（计数器、版本状态、在途数、熔断器状态、幂等记录数、灰度配置与窗口统计）。

## 版本生命周期

```
LOADING --mark_ready--> READY --retire--> UNLOADING --在途归零--> RETIRED
```

- `LOADING`/`UNLOADING` 明确拒绝新请求（`VERSION_NOT_READY`）；`RETIRED` 返回 `VERSION_RETIRED`，与未知版本（`VERSION_NOT_FOUND`）可区分。
- 卸载先切换状态拒绝新请求，再等待在途请求归零后才释放资源；所有状态迁移在同一把锁内完成，并发切换不会出现半加载/半释放。
- 未指定版本时路由到稳定指针；稳定版本下线后确定性回退到字典序最大的 READY 版本，规则在切换前后保持一致。

## 批量执行

- 仅同 `(model, version, tenant)` 的请求可合批；其余请求各自成桶，绝不合并。
- 批大小达到 `max_batch_size`（默认 8）立即执行，否则等待 `max_wait_s`（默认 50ms）窗口。
- 批内单条失败（如输入含 NaN）只影响该条；每条请求的输入在执行前拷贝，请求间不共享可变状态。

## 背压与熔断

- **背压**：全局在队请求数超过 `max_queue`（默认 64）即以 `OVERLOADED` 拒绝；等待超时返回 `REQUEST_TIMEOUT`；超时请求会被移出队列，内存占用有上界。
- **熔断**：同一 `(model, version)` 后端连续失败达到阈值（默认 3 次）即熔断，熔断期请求以 `CIRCUIT_OPEN` 快速失败；冷却（默认 1s）后进入半开，只放行一个探测请求——探测成功则闭合（探测成功不计入正常成功流量，单独记 `breaker.probe_success`），探测失败则重新熔断计时。任何成功都会清零连续失败计数，单次抖动不会永久熔断。

## 幂等性

- 相同 `(model, version, tenant, idempotency_key)` 的请求只产生一次推理效果：在途相同 key 的请求会去重，等待者共享首个请求的结果；已完成的记录写入有界 LRU 存储（默认容量 1024）。
- 记录同时按版本与租户隔离：版本切换不会命中旧版本结果；多租户共用同一标识也各算各的，绝不会把 A 租户的结果返回给 B 租户。版本下线时清除该版本全部记录。

## 灰度发布（Canary）

在“整体切版本”之外新增一层按比例分流：为一个模型指定当前稳定版本、一个灰度版本和灰度比例，先放一小部分流量到新版本，观察无误后逐步放量；出问题自动或手动退回稳定版本。

### 配置与比例

```python
gw.load_version("m", "v1", set_stable=True)
gw.load_version("m", "v2")
gw.configure_canary("m", "v2", 10)          # 10% 流量到 v2，稳定版本默认取当前稳定指针
gw.adjust_canary_percentage("m", 50)        # 随时放量到 50%（原子生效）
gw.adjust_canary_percentage("m", 0)         # 比例清零：全部干净回到稳定版本
gw.rollback_canary("m", reason="...")       # 显式手动回退（同样清零，但留回退记录）
gw.disable_canary("m")                      # 彻底关闭灰度
```

- 只对**未指定 `version`** 的请求分流；显式带版本号的请求仍严格路由到指定版本，灰度不改变既有行为。
- 配置是不可变快照，调整/回退以整体替换方式原子生效，并发请求读不到半更新配置，不会出现一段“谁都不接”或“两边都算”的中间状态。
- 灰度版本被下线（retire）时灰度配置自动移除，后续请求干净地回到稳定版本。

### 比例与租户粘性怎么理解

- 分流不是每次请求随机掷骰子，而是对 `(model, tenant)` 做稳定哈希并落到固定桶空间，再取桶前缀给灰度。因此：
  - **确定性**：同一批并发请求、同一租户在配置不变期间永远落在同一版本，不会来回跳；
  - **租户粘性**：同一租户在一轮灰度里固定使用同一个版本，避免它自己的状态在两个版本间反复横跳；
  - **放量平滑**：调整比例时只迁移哈希边界上的租户，已经在灰度侧的租户尽量保持不动；比例 100% 全去灰度，0% 全回稳定。
- 若灰度版本处于非 READY（例如正在下线），本应分到灰度的请求在同一处理路径内直接回落稳定版本。

### 灰度与稳定严格隔离

- **健康/熔断**：熔断器、滑动窗口统计均按 `(model, version)` 独立。灰度版本过载、后端故障、熔断只影响灰度那部分流量；灰度熔断打开期间，稳定版本照常服务。
- **批处理**：合批桶键为 `(model, version, tenant)`，灰度与稳定版本绝不合批。
- **幂等**：幂等记录按版本+租户隔离，同一标识不会把稳定版本结果复用给灰度版本，也不会跨租户串结果。

### 自动回退条件

- 网关为每个 `(model, version)` 维护最近一个窗口（默认 60s，可由 `canary_window_s` 配置）的统计：请求数、成功数、真实后端失败数，以及各类拒绝原因（`OVERLOADED`、`REQUEST_TIMEOUT`、`CIRCUIT_OPEN`、`INVALID_INPUT` 等），所有更新在锁内完成，并发不漏算、不重复。
- 仅对**当前灰度版本**评估：窗口内由后端健康状况定论的请求（成功 + 真实后端失败 + 熔断快速失败）达到最小样本数（`canary_min_requests`，默认 5）后，若失败信号占比严格大于阈值（`canary_failure_rate_threshold`，默认 0.5），自动把灰度比例降到 0 并回退稳定版本。
- **只认真实后端故障**：客户端非法输入（如 NaN，错误码仍是 `BACKEND_FAILURE` 但标记为非后端故障，窗口中记为 `INVALID_INPUT`）、请求等待超时（`REQUEST_TIMEOUT`）、背压拒绝（`OVERLOADED`）都不计入失败信号，少量坏请求无法把整个灰度打回。熔断快速失败（`CIRCUIT_OPEN`）是该版本后端连续故障的直接信号，计入失败信号，避免熔断打开后凑不够判定样本。
- 每次自动回退只生效一次、只留一条记录（并发触发由配置锁去重），记录包含：发生时间（`timestamp`）、模型、灰度版本、原因、当时失败率与判定样本数，可通过 `gw.canary_rollback_records(model)` 或 `GET /admin/models/{model}/canary/rollbacks` 查询。
- 回退后稳定版本继续正常服务，其窗口统计与熔断器不受灰度故障影响。

### 快照中与灰度有关的字段

`GET /admin/snapshot` 在原有字段外新增：

- `canaries`：每个配置了灰度的模型 → `{stable_version, canary_version, percentage, active}`，`active` 为比例是否大于 0。
- `canary_rollbacks`：全部灰度回退记录列表（时间、模型、灰度版本、原因、失败率、判定样本数）。
- `windows`：每个 `model/version` 的滑动窗口摘要 `{window_s, requests, successes, backend_failures, rejections}`，灰度与稳定版本各自一条。
- `counters` 中新增灰度相关计数：`canary.configured`、`canary.adjusted`、`canary.rolled_back`、`canary.disabled`、`canary.failover_to_stable`。

### 与既有能力的兼容范围

- 未配置灰度的模型行为与之前完全一致：未指定版本仍走稳定指针与确定性回退；显式版本请求、批处理、背压、熔断、幂等规则均不变。
- 灰度只作用于“未指定版本”的请求；显式 `version` 永远直达指定版本，不受比例影响。
- 既有错误码与 HTTP 映射保持不变。批内单条非法输入（如 NaN）仍返回 `BACKEND_FAILURE`（502），仅在内部新增“是否真实后端故障”的标记，用于把它排除出熔断与灰度回退判定；观测上它在窗口拒绝原因中显示为 `INVALID_INPUT`。
- 灰度版本与稳定版本走的是同一套生命周期、批处理、熔断与幂等组件，只是按版本各自独立计数与隔离；版本下线仍会等待在途请求并清理其幂等记录。

## 可观测性

`GET /admin/snapshot` 返回：

- `counters`：`requests.total`、`requests.success`、`rejections.<CODE>`、`batches.executed`、`batches.items`、`breaker.*`、`idempotency.*`、`versions.*`、`canary.*` 等计数，全部在锁内递增，并发更新不丢失不重复。
- `versions`：各模型的版本状态与在途请求数。
- `breakers`：各 `(model, version)` 熔断器状态。
- `idempotency_size`：当前幂等记录数。
- `canaries` / `canary_rollbacks` / `windows`：灰度配置、回退记录与按版本的滑动窗口统计（见“灰度发布”一节）。

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
| `tests/test_canary.py` | 比例分流、比例调整与清零、租户粘性、并发一致性、灰度/稳定隔离（熔断/批处理/幂等/跨租户）、真实故障自动回退、非法输入/背压/超时不回退、回退后稳定可用、灰度版本下线回落、手动回退记录 |
| `tests/test_window.py` | 滑动窗口并发计数不漏不重、过期事件滑出窗口 |

也可用 HTTP 接口手工验证灰度：先 `load` 两个版本，再调用 `POST /admin/models/m/canary` 配置比例，用不同 `tenant` 反复请求 `/v1/infer` 观察响应中的 `version`，调整比例或给灰度版本注入故障后查看 `/admin/snapshot` 的 `canaries`、`windows` 与 `canary_rollbacks`。

## 代码结构

```
gateway/
  errors.py       错误码与异常类型（含“是否真实后端故障”标记）
  types.py        请求/响应结构与版本状态枚举
  backend.py      本地确定性推理后端（支持故障注入）
  registry.py     版本生命周期与稳定版本路由
  batcher.py      动态批处理与背压
  breaker.py      熔断器（按版本各自独立）
  idempotency.py  有界幂等存储（按版本+租户隔离）
  metrics.py      线程安全计数器
  canary.py       灰度配置、按比例分流、租户粘性、回退记录
  window.py       按版本的滑动窗口统计
  gateway.py      组合上述组件的门面（含灰度自动回退）
  app.py          FastAPI 接入层（含灰度管理接口）
```
