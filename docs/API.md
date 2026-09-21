# 本地模型推理网关

完全离线运行的模型推理网关：确定性本地后端、动态批量、版本生命周期管理、
背压控制、熔断恢复、幂等去重与可观测性。不依赖任何外部服务、网络模型仓库或凭据。

## 请求协议

### POST /v1/infer

```json
{
  "model": "m",                // 必填，模型名
  "input": {"prompt": "..."},  // 必填，推理输入（"fail": true 可模拟条目级失败）
  "version": "v1",             // 可选；缺省路由到当前稳定版本
  "tenant": "default",         // 可选；批量合并不跨租户
  "idempotency_key": "k1",     // 可选；相同键只产生一次推理效果
  "timeout": 5.0               // 可选；请求整体超时（秒）
}
```

成功响应 `200`：

```json
{"model": "m", "version": "v1", "replayed": false, "result": {"token_count": 2, "embedding": [...]}}
```

失败响应带有可区分的 `error.code`：

| code | HTTP | 含义 |
|---|---|---|
| `MODEL_NOT_FOUND` | 404 | 模型不存在 |
| `VERSION_NOT_FOUND` | 404 | 版本不存在（或无稳定版本可回退） |
| `VERSION_RETIRED` | 410 | 版本已下线 |
| `VERSION_NOT_READY` | 409 | 版本加载中或卸载中 |
| `OVERLOADED` | 429 | 队列已满，背压拒绝 |
| `QUEUE_TIMEOUT` | 504 | 排队等待超时 |
| `CIRCUIT_OPEN` | 503 | 熔断中，快速失败 |
| `BACKEND_FAILURE` | 502 | 后端推理失败 |

### 管理接口

- `POST /admin/versions/load` `{model, version}` — 加载并就绪一个版本
- `POST /admin/versions/stable` `{model, version}` — 切换稳定版本（目标须 READY）
- `POST /admin/versions/unload` `{model, version}` — 卸载（等待在途请求结束）
- `GET /v1/versions` — 版本状态快照（state、in_flight）
- `GET /v1/metrics` — 计数器与版本状态快照

## 版本生命周期

```
LOADING --mark_ready--> READY --unload--> UNLOADING --在途归零--> RETIRED
```

- 未指定版本的请求解析到**当前稳定版本**；首个就绪版本自动成为稳定版本，
  `set_stable` 切换立即生效，之后所有缺省请求确定地落到新版本。
- `LOADING`/`UNLOADING` 状态明确拒绝新请求（`VERSION_NOT_READY`）。
- `unload` 先将版本置为 `UNLOADING`（拒绝新请求），等待在途请求结束后置为
  `RETIRED`；状态迁移与在途计数在同一把锁下完成，不存在半加载/半释放中间态。
- 版本下线后其幂等记录被清除，重新上线同名版本不会复用旧结果。

## 动态批量

- 每个 `(model, version, tenant)` 组合有独立的批量执行器，不同版本/租户的
  请求物理上不可能合并。
- 批大小上限 `max_batch_size`，等待窗口 `max_wait_ms`：窗口内凑满即发车，
  凑不满到点也发车。
- 批内单条失败（如输入含 `"fail": true`）只影响对应请求，同批其他请求正常返回。
- 提交与执行前均对输入深拷贝，请求之间不共享可变状态。

## 背压与超时

- 每个批量执行器的队列有界（`max_queue`）：满时立即以 `OVERLOADED` 拒绝，
  等待内存占用有界。
- 排队等待超过 `queue_timeout` 的请求以 `QUEUE_TIMEOUT` 失败；
  请求整体还受 `timeout`（默认 `request_timeout`）约束。
- `OVERLOADED`、`QUEUE_TIMEOUT`、`VERSION_NOT_READY`、`BACKEND_FAILURE`
  是互不相同的错误码，调用方可据此区分重试策略。

## 熔断与恢复

- 每个 `(model, version)` 一个熔断器。**连续**失败达到
  `breaker_failure_threshold` 才熔断；任意一次成功即清零计数，
  单次抖动不会熔断。
- `OPEN` 期间请求快速失败（`CIRCUIT_OPEN`），不触碰后端。
- 冷却 `breaker_cooldown_seconds` 后进入半开，只允许**一个**探测请求：
  成功则恢复 `CLOSED`，失败则重新 `OPEN`。
- 探测请求计入 `breaker_probe_total`，不计入正常成功流量。

## 幂等性

- 携带 `idempotency_key` 的相同请求只产生一次推理效果；
  并发相同键的请求等待同一结果，响应 `replayed: true`。
- 记录键为 `(model, version, tenant, key)`，天然按版本/租户隔离。
- 存储容量有界（`idempotency_capacity`，LRU 逐出已完成记录），
  版本下线时清除该版本全部记录。

## 可观测性

`GET /v1/metrics` 返回：

- `requests_total`、`rejected_total{reason}`、`batches_total`、`batch_items_total`
- `timeouts_total`、`circuit_open_total`、`circuit_rejected_total`、`breaker_probe_total`
- `idempotency_hits_total`、`idempotency_dropped_total`
- `versions`：各版本状态与在途数快照

所有计数在同一锁下递增，并发更新不丢失、不重复。

## 验证方法

```bash
uv run pytest tests/ -q          # 全部测试
uv run pytest tests/ -q -s       # 打印请求输入与关键判定依据
uv run uvicorn main:app          # 本地启动，http://127.0.0.1:8000/docs
```

测试覆盖：并发批量合并、批内部分失败隔离、输入不共享、租户不合并、
稳定版本回退与切换、卸载等待在途、过载拒绝、排队超时、
熔断/恢复/探测失败重开、幂等单次效果与版本隔离、HTTP 错误码区分。
