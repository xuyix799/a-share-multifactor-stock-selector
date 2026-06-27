# A股中长线多因子选股系统

本仓库是一个本地 Docker 部署的 A股中长线多因子选股系统。当前已推进到 Goal 7：综合评分、风险等级、规则化解释和 `selection_result`。

系统定位：

- 不做自动交易。
- 不接券商账户。
- 不做短线预测。
- 不让大模型直接预测股价。
- 不输出“必涨”“稳赚”“满仓”“目标价”“无脑买入”等确定性投资结论。
- `selection_result` 只是中长线候选池结果，不代表自动交易信号。
- Python 负责数据采集、清洗、因子、评分、候选池生成和后续回测能力。
- Spring Boot 当前只保留基础 API / 任务服务骨架，不承载复杂业务逻辑。
- PostgreSQL 只保存任务状态、配置、摘要和对象 key，不保存全量行情、全量因子或回测明细。
- MinIO 保存 Parquet、报告、图表和后续回测明细。
- DuckDB 查询 Parquet。

## 当前进度

- Goal 1：Docker 基础设施，包含 `stock-postgres`、`stock-minio`、`stock-python`、`stock-api`。
- Goal 2：mock 数据安全闭环，支持 Parquet / MinIO / `update_log` / DuckDB 查询 / 幂等重跑。
- Goal 3：provider adapter 层，已实现 `MockProvider`、schema contract、schema mapping、provider pipeline；真实 Tushare / AKShare / Baostock 只保留骨架且默认禁用。
- Goal 4：clean snapshot 层，已实现 `adjusted_price`、financial as-of join、ST 历史判断、`clean_daily_snapshot`。
- Goal 5：风险过滤与候选股票池输入层，已实现 `risk_filter`、`eligible_universe`、`factor_input_table`。
- Goal 6：中长线基础因子计算层，已实现 `factor_daily`，包含质量、成长、估值、趋势、行业强度和五个子评分。
- Goal 7：综合评分与候选结果层，已实现 `total_score`、`risk_level`、规则化 `reason` / `suggestion`、`selection_result` 和 PostgreSQL `selection_snapshot` 摘要。

当前未实现：

- 真实行情 API 接入。
- 正式回测。
- LLM 解释。
- 复杂 Spring Boot 业务 API。
- 自动交易或券商账户接入。

## 服务

```text
stock-postgres  PostgreSQL 16，本地端口 15432
stock-minio     MinIO，本地端口 19000，控制台 19001
stock-python    Python 计算引擎
stock-api       Spring Boot API，本地端口 18080
```

MinIO bucket：

- `stock-raw`
- `stock-processed`
- `stock-backtest`

## 数据链路

当前主链路：

```text
mock provider
  -> raw provider datasets
  -> adjusted_price
  -> clean_daily_snapshot
  -> risk_filter / eligible_universe / factor_input_table
  -> factor_daily
  -> selection_result
```

核心数据集：

- raw：`stock_basic`、`daily_price`、`adj_factor`、`daily_basic`、`financial`、`st_history`、`benchmark_price`
- clean：`adjusted_price`、`clean_daily_snapshot`
- universe：`risk_filter`、`eligible_universe`、`factor_input_table`
- factors：`factor_daily`
- scoring：`selection_result`

`factor_daily` 当前包含：

- 基础信息：`stock_code`、`trade_date`、`industry`、`market_type`
- 质量因子：`quality_roe`、`quality_gross_margin`、`quality_debt_ratio`、`quality_cashflow_profit_ratio`
- 成长因子：`growth_revenue_yoy`、`growth_net_profit_yoy`
- 估值因子：`valuation_pe_ttm`、`valuation_pb`、`valuation_ps_ttm`、`valuation_pe_percentile_3y`、`valuation_pb_percentile_3y`
- 趋势因子：`trend_ret_20d`、`trend_ret_60d`、`trend_ret_120d`、`trend_ma20`、`trend_ma60`、`trend_ma120`、`trend_price_ma60_ratio`
- 行业强度：`industry_ret_60d`、`industry_ret_120d`、`industry_strength_60d`、`industry_strength_120d`
- 流动性：`liquidity_amount`、`liquidity_turnover_rate`
- 子评分：`quality_score`、`growth_score`、`valuation_score`、`trend_score`、`industry_score`

`factor_daily` 不包含 `total_score`。`total_score` 在 Goal 7 的 `selection_result` 中计算。

`selection_result` 当前包含：

- 基础信息：`stock_code`、`trade_date`、`industry`、`market_type`
- 子评分：`quality_score`、`growth_score`、`valuation_score`、`trend_score`、`industry_score`
- 综合结果：`total_score`、`risk_level`、`rank`
- 规则化文本：`suggestion`、`reason`
- 风险透传：`exclude_reasons`、`risk_flags`

PostgreSQL `selection_snapshot` 只保存摘要和对象 key：

- `trade_date`
- `top_n`
- `object_key`
- `stock_count`
- `avg_total_score`
- `max_total_score`
- `min_total_score`
- `created_at`

`selection_result` 明细只写 Parquet / MinIO，不写入 PostgreSQL。

## 本地启动

```powershell
docker compose config
docker compose up -d --build
docker compose ps
```

Spring Boot 健康检查：

```powershell
curl http://localhost:18080/actuator/health
curl http://localhost:18080/api/health
```

## Python CLI 链路

初始化与健康检查：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli init-db
docker compose run --rm stock-python python -m stock_selector.cli init-storage
docker compose run --rm stock-python python -m stock_selector.cli health-check
```

运行当前 mock 闭环：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli update-provider-data --trade-date 2026-06-19 --provider mock --force

docker compose run --rm stock-python python -m stock_selector.cli build-adjusted-price --trade-date 2026-06-19 --force
docker compose run --rm stock-python python -m stock_selector.cli build-clean-snapshot --trade-date 2026-06-19 --force
docker compose run --rm stock-python python -m stock_selector.cli build-universe-inputs --trade-date 2026-06-19 --force

docker compose run --rm stock-python python -m stock_selector.cli build-factors --trade-date 2026-06-19
docker compose run --rm stock-python python -m stock_selector.cli build-factors --trade-date 2026-06-19 --force
docker compose run --rm stock-python python -m stock_selector.cli validate-factors --trade-date 2026-06-19

docker compose run --rm stock-python python -m stock_selector.cli build-selection --trade-date 2026-06-19
docker compose run --rm stock-python python -m stock_selector.cli build-selection --trade-date 2026-06-19 --force
docker compose run --rm stock-python python -m stock_selector.cli validate-selection --trade-date 2026-06-19
```

查询 Parquet：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli query-parquet --dataset factor_daily --trade-date 2026-06-19
docker compose run --rm stock-python python -m stock_selector.cli query-parquet --dataset selection_result --trade-date 2026-06-19
docker compose run --rm stock-python python -m stock_selector.cli show-update-log --trade-date 2026-06-19
```

## 幂等与重跑

所有关键派生任务通过 PostgreSQL `update_log` 记录状态：

- `provider_data:<dataset>`
- `cleaning:adjusted_price`
- `cleaning:clean_daily_snapshot`
- `universe:inputs`
- `factors:factor_daily`
- `scoring:selection_result`

默认行为：

- 已 `done` 的步骤会跳过。
- 加 `--force` 会重跑。
- 失败时记录 `failed` 和错误信息。
- Parquet 写入继续使用 partition builder 和 atomic writer，避免半成品覆盖正式文件。

## 测试

容器内测试：

```powershell
docker compose run --rm stock-python pytest python-engine/tests -q
```

本地测试：

```powershell
cd python-engine
$env:PYTHONPATH='src;tests'
python -m pytest
```

当前基线：`121 passed`。

## 开发约束

- 所有测试必须能在 mock provider、无网络、无真实 token 下通过。
- 真实 Tushare / AKShare / Baostock provider 默认禁用。
- 价格类因子只使用 `adjusted_price`。
- 财务因子只使用已清洗的 as-of 结果，不读取未来财务数据。
- benchmark 比较使用 `benchmark_price`，默认指数 `000300.SH`。
- 历史不足时因子字段为空，不使用未来数据补齐。
- `selection_result` 只能使用 `factor_daily`、`risk_filter`、`eligible_universe`、`factor_input_table` 等已清洗数据。
- `total_score` 从 `config/factor_weights.yaml` 读取权重，权重和必须为 1。
- 子评分缺失时按 `scoring.null_score_policy: neutral` 使用中性分。
- `reason` / `suggestion` 由规则生成，不调用 LLM。
- 根目录的 `A股多因子选股系统第一版方案.md` 已被 `.gitignore` 精确忽略，不进入 GitHub 仓库。
