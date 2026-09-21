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
- `GET /admin/snapshot`：可观测性快照（计数器、版本状态、在途数、熔断器状态、幂等记录数）。

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

- 相同 `(model, version, idempotency_key)` 的请求只产生一次推理效果：在途相同 key 的请求会去重，等待者共享首个请求的结果；已完成的记录写入有界 LRU 存储（默认容量 1024）。
- 记录按版本隔离，版本下线时清除该版本全部记录，不会复用旧版本结果。

## 可观测性

`GET /admin/snapshot` 返回：

- `counters`：`requests.total`、`requests.success`、`rejections.<CODE>`、`batches.executed`、`batches.items`、`breaker.*`、`idempotency.*`、`versions.*` 等计数，全部在锁内递增，并发更新不丢失不重复。
- `versions`：各模型的版本状态与在途请求数。
- `breakers`：各 `(model, version)` 熔断器状态。
- `idempotency_size`：当前幂等记录数。

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

## 代码结构

```
gateway/
  errors.py       错误码与异常类型
  types.py        请求/响应结构与版本状态枚举
  backend.py      本地确定性推理后端（支持故障注入）
  registry.py     版本生命周期与稳定版本路由
  batcher.py      动态批处理与背压
  breaker.py      熔断器
  idempotency.py  有界幂等存储
  metrics.py      线程安全计数器
  gateway.py      组合上述组件的门面
  app.py          FastAPI 接入层
```
