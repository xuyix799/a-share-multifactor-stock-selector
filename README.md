# A股中长线多因子选股系统

本仓库是一个本地 Docker 部署的 A股中长线多因子选股系统。当前系统已完成 Docker 本地 mock/offline 端到端闭环验证。Python CLI、PostgreSQL、MinIO、Spring Boot API 已在容器环境中联通，mock 数据可以完成 provider、raw、复权、clean snapshot、universe、factor_daily、selection_result、validate-selection 和 run-backtest，并可通过 Spring API 查询选股摘要和回测摘要。

真实数据方面，AKShare、Baostock、Tushare 仍未进入真实选股或真实回测主链路。Tushare 的 `daily`、`stk_limit`、`adj_factor`、`daily_basic`、`trade_cal`、`suspend_d` 等关键接口已完成 smoke、candidate/staging、coverage expansion、`suspend_d` full coverage audit、promotion preflight 和 Goal 14 小范围 `daily_price` promotion validator；默认仍只生成 dry-run 报告，不写标准 `daily_price`。

系统定位：

- 不做自动交易。
- 不接券商账户。
- 不做短线预测。
- 不让大模型直接预测股价。
- 不输出“必涨”“稳赚”“满仓”“目标价”“无脑买入”等确定性投资结论。
- `selection_result` 只是中长线候选池结果，不代表自动交易信号。
- Python 负责数据采集、清洗、因子、评分、候选池生成和后续回测能力。
- Spring Boot 当前只提供健康检查、结果摘要查询和任务状态查询 API，不承载复杂业务逻辑。
- PostgreSQL 只保存任务状态、配置、摘要和对象 key，不保存全量行情、全量因子或回测明细。
- MinIO 保存 Parquet、报告、图表和后续回测明细。
- DuckDB 查询 Parquet。

## 当前进度

- Goal 1：Docker 基础设施，包含 `stock-postgres`、`stock-minio`、`stock-python`、`stock-api`。
- Goal 2：mock 数据安全闭环，支持 Parquet / MinIO / `update_log` / DuckDB 查询 / 幂等重跑。
- Goal 3：provider adapter 层，已实现 `MockProvider`、schema contract、schema mapping、provider pipeline；真实 provider 默认禁用。
- Goal 4：clean snapshot 层，已实现 `adjusted_price`、financial as-of join、ST 历史判断、`clean_daily_snapshot`。
- Goal 5：风险过滤与候选股票池输入层，已实现 `risk_filter`、`eligible_universe`、`factor_input_table`。
- Goal 6：中长线基础因子计算层，已实现 `factor_daily`，包含质量、成长、估值、趋势、行业强度和五个子评分。
- Goal 7：综合评分与候选结果层，已实现 `total_score`、`risk_level`、规则化 `reason` / `suggestion`、`selection_result` 和 PostgreSQL `selection_snapshot` 摘要。
- Goal 8：回测核心层，已实现 T 日信号、T+1 成交、月度/季度调仓、交易成本、滑点、印花税、涨跌停/停牌约束、三指数 benchmark 对比、回测明细 Parquet 和 PostgreSQL `backtest_summary` 摘要。
- Goal 9：Spring Boot 最小结果查询 API，已实现 PostgreSQL 摘要查询，不读取 MinIO / Parquet，不执行 Python 计算或回测。
- Goal 10 / 10R：Tushare 真实数据 smoke，当前以 2000 积分账号重新探测 `stock_basic`、`daily`、`stk_limit`、`adj_factor`、`daily_basic`、`index_daily`、`fina_indicator`；真实数据只写 `smoke/tushare/...`，不伪造 `limit_up` / `limit_down` / `is_paused`。
- Goal 12B：Tushare `trade_cal` / `suspend_d` smoke 与 `suspension_status_candidate` 契约验证；只写 `smoke/tushare/trade_cal/...` 和 `smoke/tushare/suspend_d/...`，不写标准 `daily_price`。
- Goal 12C：Tushare `daily_price_candidate` join dry-run；只读已有 `daily` / `stk_limit` / `adj_factor` / `trade_cal` / `suspend_d` smoke，输出诊断报告，不写标准 `daily_price`，详见 `docs/goal12C_tushare_daily_price_candidate_dry_run.md`。
- Goal 12D：Tushare `suspension_status_candidate` staging 与 coverage audit；输出 candidate-only 停牌状态和覆盖审计报告，当前 sample-truncated `suspend_d` 不能生成 `false_candidate`，仍未写 standard `daily_price` 或 standard `suspension_status`，真实数据仍未进入正式选股/回测主链路，详见 `docs/goal12D_suspension_status_candidate_coverage_audit.md`。
- Goal 13：Tushare 小范围 real-provider candidate/staging batch 与 DQ3 readiness audit；只写 `candidate/tushare/...` manifest、staging、candidate batch、coverage report 和 DQ3 audit，不写 standard `daily_price` / `suspension_status`，不进入真实选股或真实回测，详见 `docs/goal13_tushare_candidate_staging_batch_dq3_readiness.md`。
- Goal 13B：新增 Tushare candidate batch coverage expansion 与 fetch semantics audit；真实数据仍未进入 standard `daily_price` 或真实回测主链路，详见 `docs/goal13B_tushare_candidate_batch_coverage_expansion.md`。Goal 13B 已在 `main` 形成稳定基线，基线提交为 `fadf8be` / `goal-13B-tushare-coverage-expansion-verified`。
- Goal 13C：新增 `suspend_d` full coverage audit 与 promotion preflight；只有 date-level full event coverage 被确认时，`suspend_d` miss 才能生成 `false_candidate`，并且最多只能进入 `READY_FOR_PROMOTION_VALIDATOR`，不写 standard `daily_price` / standard `suspension_status`，不进入真实选股或真实回测，详见 `docs/goal13C_suspend_d_full_coverage_preflight.md`。
- Goal 14：新增小范围 standard `daily_price` promotion validator；只读取 Goal 13C 产物，验证 `daily_price_candidate_batch` 是否满足标准 `daily_price` 契约并默认输出 dry-run 报告。标准写入必须显式传 `--goal14-execute-standard-write`，不写 standard `suspension_status`，不进入真实 clean/factor/selection/backtest，详见 `docs/goal14_daily_price_promotion_validator.md`。
- Goal 10B：AKShare / Baostock 最小真实数据 smoke，已验证 AKShare `benchmark_price` 可标准化写入 `smoke/akshare/...` 并通过 DuckDB 查询；字段不足的数据集不会绕过 validator 写入标准层。
- Goal 11：AKShare / Baostock 真实数据能力矩阵与日线 smoke，新增 smoke-only `daily_price_raw_smoke`，只允许写入 `smoke/<provider>/daily_price_raw_smoke/...`，不进入标准 `raw/daily_price/...`。
- Goal 12A：真实数据标准层契约与数据质量等级冻结，详见 `docs/goal12A_real_data_contract.md`；本阶段只新增契约、守门规则和测试，不接入真实 provider 主链路，不做真实回测。

当前未实现：

- 大范围真实标准 `daily_price` promotion。
- 真实财务数据接入。
- 真实 clean/factor/selection/backtest 主链路。
- LLM 解释。
- 复杂 Spring Boot 业务 API / 前端页面。
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
- `top_stocks`：前 N 只股票的 `stock_code`、`rank`、`total_score`
- `avg_total_score`
- `max_total_score`
- `min_total_score`
- `created_at`

`selection_result` 明细只写 Parquet / MinIO，不写入 PostgreSQL；当前对象路径为 `processed/selection_result/trade_date=<YYYY-MM-DD>/part.parquet`。

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

## Spring Boot 查询 API

Spring Boot 只查询 PostgreSQL 中的摘要和任务状态表，不读取 MinIO 对象，不解析 Parquet，不生成 presigned URL，不执行 Python、因子计算或回测。

结果摘要查询：

```powershell
curl "http://localhost:18080/api/selections?limit=20&offset=0"
curl http://localhost:18080/api/selections/2026-06-19
curl "http://localhost:18080/api/backtests?limit=20&offset=0&status=done&rebalanceMode=monthly"
curl http://localhost:18080/api/backtests/<runKey>
```

任务状态查询：

```powershell
curl http://localhost:18080/api/update-logs/2026-06-19
curl "http://localhost:18080/api/task-logs?status=done&taskType=backtest&limit=20"
```

API 说明：

- `GET /api/selections` 查询 `selection_snapshot` 列表，参数 `limit` 为 `1..100`、默认 `20`，`offset` 为 `>=0`、默认 `0`。
- `GET /api/selections/{tradeDate}` 查询单日 `selection_snapshot`，`tradeDate` 必须是合法 `YYYY-MM-DD` 日期。
- `GET /api/backtests` 查询 `backtest_summary` 列表，参数 `limit` 为 `1..100`、默认 `20`，`offset` 为 `>=0`、默认 `0`，`status` 可选 `pending` / `running` / `done` / `failed`，`rebalanceMode` 可选 `monthly` / `quarterly`。
- `GET /api/backtests/{runKey}` 查询单个回测摘要，`runKey` 必须是 16 位小写 hex。
- `GET /api/update-logs/{tradeDate}` 是数据链路任务状态查询接口，查询指定日期 `update_log`。
- `GET /api/task-logs` 是通用任务状态查询接口，参数 `status` 可选 `pending` / `running` / `done` / `failed`，`limit` 为 `1..100`、默认 `20`。
- 错误响应统一为 `{ "code": "...", "message": "...", "path": "..." }`，不会向客户端暴露数据库密码、连接串或 stack trace。

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

## 可选真实 Provider Smoke

真实 provider smoke 只用于验证真实数据经过 provider adapter、schema mapping、`data_validator`、MinIO / Parquet 和 DuckDB 查询；它不是实盘策略入口，也不会自动调度。所有非 mock provider 都必须加 `--smoke`，只允许写入：

```text
smoke/<provider>/<dataset>/trade_date=YYYY-MM-DD/part.parquet
```

不允许真实 provider 直接写标准 `raw/<dataset>/...` 路径。

### AKShare benchmark smoke

AKShare 不需要 token。当前只把 `benchmark_price` 作为标准化 smoke 数据集：AKShare 指数日线提供 `date/open/high/low/close`，系统用前一交易日 close 计算 `pct_chg`，写出三只 benchmark 指数 `000300.SH`、`000905.SH`、`000906.SH`。

```powershell
$env:STOCK_AKSHARE_ENABLED='1'
$env:AKSHARE_SMOKE_TRADE_DATE='2024-06-19'

docker compose run --rm stock-python python -m stock_selector.cli update-provider-data `
  --provider akshare `
  --trade-date $env:AKSHARE_SMOKE_TRADE_DATE `
  --dataset benchmark_price `
  --smoke `
  --force

docker compose run --rm stock-python python -m stock_selector.cli query-parquet --dataset benchmark_price --trade-date $env:AKSHARE_SMOKE_TRADE_DATE --smoke-provider akshare
```

可选真实集成测试默认跳过；只有同时设置 `RUN_AKSHARE_SMOKE=1`、`STOCK_AKSHARE_ENABLED=1` 和 `AKSHARE_SMOKE_TRADE_DATE` 时才运行：

```powershell
$env:RUN_AKSHARE_SMOKE='1'
docker compose run --rm stock-python pytest python-engine/tests/test_akshare_smoke_integration.py -q
```

AKShare `stock_basic` 当前缺少标准层要求的 `list_date`、`industry`、`market_type`、`is_st` 等稳定全量字段；AKShare `daily_price` 缺少标准层必需的 `limit_up` / `limit_down` / `is_paused`。这些 dataset 会明确输出 provider capability 不足，不会伪造字段或绕过 validator。

### Goal 11 daily raw smoke

`daily_price_raw_smoke` 是 smoke-only 数据集，只用于真实 provider 字段探测和最小日线连通性验证。它不是标准 `daily_price`，不会进入 `raw/daily_price/...`，也不会进入清洗、因子、评分或回测链路。

写入路径固定为：

```text
smoke/<provider>/daily_price_raw_smoke/trade_date=YYYY-MM-DD/part.parquet
```

当前 raw smoke 字段：

```text
stock_code, trade_date, open, high, low, close, volume, amount, pct_chg, source_symbol
```

AKShare 日线 raw smoke：

```powershell
$env:STOCK_AKSHARE_ENABLED='1'
$env:AKSHARE_SMOKE_TRADE_DATE='2024-06-19'

docker compose run --rm stock-python python -m stock_selector.cli update-provider-data `
  --provider akshare `
  --trade-date $env:AKSHARE_SMOKE_TRADE_DATE `
  --dataset daily_price_raw_smoke `
  --smoke `
  --force

docker compose run --rm stock-python python -m stock_selector.cli query-parquet --dataset daily_price_raw_smoke --trade-date $env:AKSHARE_SMOKE_TRADE_DATE --smoke-provider akshare
```

Baostock 日线 raw smoke：

```powershell
$env:STOCK_BAOSTOCK_ENABLED='1'
$env:BAOSTOCK_SMOKE_TRADE_DATE='2024-06-19'

docker compose run --rm stock-python python -m stock_selector.cli update-provider-data `
  --provider baostock `
  --trade-date $env:BAOSTOCK_SMOKE_TRADE_DATE `
  --dataset daily_price_raw_smoke `
  --smoke `
  --force

docker compose run --rm stock-python python -m stock_selector.cli query-parquet --dataset daily_price_raw_smoke --trade-date $env:BAOSTOCK_SMOKE_TRADE_DATE --smoke-provider baostock
```

如果 Baostock 登录返回 `10002007 网络接收错误`，保持 Baostock smoke blocked；不要改写为标准 `daily_price`，也不要补假字段。

可选真实集成测试默认跳过；只有显式设置对应环境变量才运行：

```powershell
$env:RUN_AKSHARE_SMOKE='1'
docker compose run --rm stock-python pytest python-engine/tests/test_akshare_smoke_integration.py -q

$env:RUN_BAOSTOCK_SMOKE='1'
docker compose run --rm stock-python pytest python-engine/tests/test_baostock_smoke_integration.py -q
```

### Tushare Smoke

Goal 10R 使用 2000 积分账号重新探测 Tushare 接口能力。Tushare smoke 只用于验证真实接口权限、字段覆盖、MinIO / Parquet 落盘和 DuckDB 查询；它不是实盘策略入口，也不会自动调度。

默认 mock 链路不需要 token，也不会访问网络。只有显式设置以下环境变量时才启用 Tushare：

```powershell
$env:STOCK_TUSHARE_ENABLED='1'
$env:TUSHARE_TOKEN='<your-token>'
$env:TUSHARE_SMOKE_TRADE_DATE='YYYY-MM-DD'
```

最小真实探测覆盖七个接口，并且只写 `smoke/tushare/<interface>/trade_date=YYYY-MM-DD/part.parquet`：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli probe-tushare-goal10r `
  --trade-date $env:TUSHARE_SMOKE_TRADE_DATE `
  --sample-limit 5 `
  --sleep-seconds 12
```

写入后可用 DuckDB 查询已落地的 Parquet：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli query-parquet --dataset daily --trade-date $env:TUSHARE_SMOKE_TRADE_DATE --smoke-provider tushare
docker compose run --rm stock-python python -m stock_selector.cli query-parquet --dataset stk_limit --trade-date $env:TUSHARE_SMOKE_TRADE_DATE --smoke-provider tushare
```

可选真实集成测试默认跳过；只有同时设置 `RUN_TUSHARE_SMOKE=1`、`STOCK_TUSHARE_ENABLED=1`、`TUSHARE_TOKEN` 和 `TUSHARE_SMOKE_TRADE_DATE` 时才运行：

```powershell
$env:RUN_TUSHARE_SMOKE='1'
docker compose run --rm stock-python pytest python-engine/tests/test_tushare_smoke_integration.py -q
```

即使 `daily` + `stk_limit` + `daily_basic` + `adj_factor` 字段完整，只要没有可信 `is_paused` 来源，Tushare stock daily 最多只能判为 DQ2，不能晋级 DQ3 标准 `daily_price`。Goal 10 / 10B / 10R 不做十年全量数据，不执行因子、选股、回测或自动交易。

### Tushare Goal 12B Suspension Smoke

Goal 12B 只验证 Tushare `trade_cal` / `suspend_d` 作为标准层候选来源的可用性，不写标准 `daily_price`，不写真实 raw 主链路，不进入清洗、因子、选股或真实回测。

状态含义：

- `PASS_WITH_ROWS`：接口可用且字段满足契约，并返回样本行。
- `PASS_EMPTY`：接口可用且字段满足契约，但该日期没有事件行；`suspend_d` 空结果不是失败。
- `BLOCKED`：权限、积分、频率或配置阻塞。
- `API_ERROR`：接口调用发生其他异常。
- `SCHEMA_MISMATCH`：接口可达，但返回字段不满足本系统契约。

运行：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli probe-tushare-goal12b `
  --trade-date $env:TUSHARE_SMOKE_TRADE_DATE `
  --sample-limit 5 `
  --sleep-seconds 12
```

Goal 12B 只允许以下 smoke 路径：

```text
smoke/tushare/trade_cal/trade_date=YYYY-MM-DD/part.parquet
smoke/tushare/suspend_d/trade_date=YYYY-MM-DD/part.parquet
```

写入后可用 DuckDB 查询：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli query-parquet --dataset trade_cal --trade-date $env:TUSHARE_SMOKE_TRADE_DATE --smoke-provider tushare
docker compose run --rm stock-python python -m stock_selector.cli query-parquet --dataset suspend_d --trade-date $env:TUSHARE_SMOKE_TRADE_DATE --smoke-provider tushare
```

`trade_cal` 只是 `trading_calendar_candidate` 证据；`suspend_d` 只是 `suspension_status_candidate` 事件来源候选。`suspend_d` 命中可以成立为 `is_paused=true` candidate；`suspend_d` 未命中不能推断为 `is_paused=false`，必须等后续标准层 staging、join dry-run、覆盖范围审计和 validator 验证完成。

### Tushare Goal 13 / 13B / 13C Candidate Batch

Goal 13 可以显式调用 Tushare，构建小范围 candidate/staging batch。Goal 13B 在同一 candidate/staging 边界内增加 coverage expansion、fetch semantics audit 和 coverage gap report。Goal 13C 继续停留在 candidate/staging 边界内，只增加 `suspend_d_full_coverage_report` 和 `promotion_preflight_report`：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli build-tushare-candidate-staging-batch `
  --start-date 2024-06-01 `
  --end-date 2024-07-31 `
  --codes 000001.SZ,600519.SH,300750.SZ,000333.SZ,601318.SH,600036.SH,000858.SZ,601899.SH,600900.SH,002415.SZ `
  --max-trade-days 20 `
  --sleep-seconds 12 `
  --coverage-expansion `
  --fetch-semantics-audit `
  --goal13c-preflight
```

该命令只允许写入：

```text
candidate/tushare/batch_manifest/batch_id=<batch_id>/manifest.json
candidate/tushare/*_staging/batch_id=<batch_id>/...
candidate/tushare/daily_price_candidate_batch/batch_id=<batch_id>/part.parquet
candidate/tushare/suspension_status_candidate_batch/batch_id=<batch_id>/part.parquet
candidate/tushare/provider_coverage_report/batch_id=<batch_id>/report.json
candidate/tushare/fetch_semantics_report/batch_id=<batch_id>/report.json
candidate/tushare/coverage_gap_report/batch_id=<batch_id>/report.json
candidate/tushare/dq3_readiness_audit/batch_id=<batch_id>/report.json
candidate/tushare/suspend_d_full_coverage_report/batch_id=<batch_id>/report.json
candidate/tushare/promotion_preflight_report/batch_id=<batch_id>/report.json
```

Goal 13B 中 `stk_limit` coverage expansion 按目标交易日拉取后过滤目标股票，`adj_factor`、`daily`、`daily_basic` 保持按股票区间拉取。`--no-provider-call --reuse-existing-staging` 可以只复用已有 candidate/staging 重建审计报告；`--fail-on-incomplete-critical-coverage` 只在关键 price coverage 不完整时让 CLI 非零退出。

Goal 13C 中 `suspend_d` 按目标 open trade date 审计 full market event set。只有 `DATE_FULL_MARKET_EVENT_SET` 或 `CODE_DATE_EXPLICIT_FULL_UNIVERSE` 被确认、provider fetch 成功、无 empty retry exhaustion、无 schema/truncation/scope 风险时，`suspend_d` miss 才能生成 `false_candidate`，且会记录 `SUSPEND_D_FULL_COVERAGE_MISS_AS_FALSE_CANDIDATE`。如果 query scope unknown、partial universe、provider incomplete、`PROVIDER_EMPTY_AFTER_RETRIES`、schema incomplete 或 truncation 风险存在，miss 必须保持 `unknown`。

`promotion_preflight_report.status=READY_FOR_PROMOTION_VALIDATOR` 只表示可以进入下一 Goal 的 promotion validator，不表示允许写 standard 表。`standard_daily_price_write_performed`、`standard_suspension_status_write_performed`、`real_backtest_performed`、`ready_for_standard_write`、`ready_for_real_backtest` 和 `production_ready` 必须保持 `false`。

这些对象不是 `raw/daily_price` 或标准 `suspension_status`，不会被 `clean_daily_snapshot`、`factor_input_table`、`factor_daily`、`selection_result` 或 `run-backtest` 读取。`suspend_d` 未命中仍不能直接推断为 `is_paused=false`；缺少完整事件覆盖审计时，promotion preflight 必须保持 blocked。

### Tushare Goal 14 Daily Price Promotion Validator

Goal 14 读取 Goal 13C 的 `promotion_preflight_report`、`daily_price_candidate_batch` 和 `suspension_status_candidate_batch`，执行小范围标准 `daily_price` promotion validator。默认模式只写 validator report 和 dry-run report：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli build-tushare-daily-price-promotion-validator `
  --batch-id <batch_id>
```

默认输出：

```text
candidate/tushare/daily_price_promotion_validator_report/batch_id=<batch_id>/report.json
candidate/tushare/standard_daily_price_promotion_dry_run_report/batch_id=<batch_id>/report.json
```

小范围 guard 默认限制为 `max_codes=5`、`max_trade_days=10`、`max_rows=50`。validator 会阻断 preflight 未 ready、price coverage 不完整、`is_paused` 未解析、候选行缺字段、重复 code-date、非 open trade date、OHLC/成交量异常、`limit_up` / `limit_down` / `pre_close` / `is_paused` 来源不可审计等情况。

标准 `daily_price` 写入必须显式传入：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli build-tushare-daily-price-promotion-validator `
  --batch-id <batch_id> `
  --goal14-execute-standard-write
```

显式执行时会额外写入：

```text
candidate/tushare/standard_daily_price_promotion_execution_report/batch_id=<batch_id>/report.json
```

即便显式执行，Goal 14 也只允许小范围、幂等、可审计地写 standard `daily_price`；不写 standard `suspension_status`，不启动 `clean_daily_snapshot`、`factor_input_table`、`factor_daily`、`selection_result` 或真实回测。

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
docker compose run --rm --no-deps stock-api mvn test
docker compose run --rm stock-python pytest python-engine/tests -q
```

本地测试：

```powershell
cd python-engine
$env:PYTHONPATH='src;tests'
python -m pytest
```

当前测试基线：Python `249 passed, 5 skipped`；Spring Maven `mvn test -q` exit 0。

## 开发约束

- 所有测试必须能在 mock provider、无网络、无真实 token 下通过。
- 真实 Tushare / AKShare / Baostock provider 默认禁用；Tushare smoke 必须显式 opt-in。
- 价格类因子只使用 `adjusted_price`。
- 财务因子只使用已清洗的 as-of 结果，不读取未来财务数据。
- benchmark 比较使用 `benchmark_price`，默认指数 `000300.SH`。
- 历史不足时因子字段为空，不使用未来数据补齐。
- `selection_result` 只能使用 `factor_daily`、`risk_filter`、`eligible_universe`、`factor_input_table` 等已清洗数据。
- `total_score` 从 `config/factor_weights.yaml` 读取权重，权重和必须为 1。
- 子评分缺失时按 `scoring.null_score_policy: neutral` 使用中性分。
- `reason` / `suggestion` 由规则生成，不调用 LLM。
- 根目录的 `A股多因子选股系统第一版方案.md` 已被 `.gitignore` 精确忽略，不进入 GitHub 仓库。
