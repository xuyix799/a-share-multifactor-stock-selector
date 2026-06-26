# A股中长线多因子选股系统

本仓库当前只实现 Goal 1：本地 Docker 基础设施与项目骨架。

系统边界：

- 不做自动交易。
- 不接券商账户。
- 不实现真实行情采集、因子计算、回测或 LLM 调用。
- Python 负责后续量化计算能力，本轮只做配置、PostgreSQL、MinIO 和 smoke 命令。
- Spring Boot 本轮只做健康检查和基础连通性。
- PostgreSQL 只保存任务状态、配置、结果摘要和对象 key，不保存全量行情或全量因子。
- MinIO bucket 使用 `stock-raw`、`stock-processed`、`stock-backtest`。

## 本地启动

```powershell
docker compose config
docker compose up -d --build
docker compose ps
```

## Python CLI

```powershell
docker compose run --rm stock-python python -m stock_selector.cli validate-date --trade-date 2026-06-19
docker compose run --rm stock-python python -m stock_selector.cli init-db
docker compose run --rm stock-python python -m stock_selector.cli init-storage
docker compose run --rm stock-python python -m stock_selector.cli health-check
docker compose run --rm stock-python python -m stock_selector.cli storage-smoke --trade-date 2026-06-19
```

非法日期必须失败：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli validate-date --trade-date "../bad-date"
```

## Spring Boot API

```powershell
curl http://localhost:18080/actuator/health
curl http://localhost:18080/api/health
```
