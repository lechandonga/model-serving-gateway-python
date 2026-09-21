# model-serving-gateway-python

本地离线运行的模型推理网关（FastAPI + asyncio）：

- 按模型与版本路由，缺省落到当前稳定版本，未知/已下线版本给出可区分错误
- 同 `(model, version, tenant)` 并发请求动态批量执行，批内失败隔离
- 版本加载/就绪/卸载生命周期，卸载等待在途请求，无中间态
- 有界队列背压（`OVERLOADED`）与排队超时（`QUEUE_TIMEOUT`）
- 后端连续失败熔断、冷却后半开探测恢复
- 幂等键去重：相同键只产生一次推理效果，记录按版本隔离且容量有界
- `/v1/metrics`、`/v1/versions` 可观测性快照
- 确定性本地后端，无需外部服务即可验证

## 快速开始

```bash
uv run pytest tests/ -q        # 运行测试
uv run uvicorn main:app        # 启动服务
```

详细协议、生命周期、背压与熔断规则见 [docs/API.md](docs/API.md)。
