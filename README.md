# A股中长线多因子选股系统

本仓库是一个本地 Docker 部署的 A股中长线多因子选股系统。当前系统已完成 Docker 本地 mock/offline 端到端闭环验证。Python CLI、PostgreSQL、MinIO、Spring Boot API 已在容器环境中联通，mock 数据可以完成 provider、raw、复权、clean snapshot、universe、factor_daily、selection_result、validate-selection 和 run-backtest，并可通过 Spring API 查询选股摘要和回测摘要。

真实数据方面，AKShare、Baostock、Tushare 仍未进入真实选股或真实回测主链路。Tushare 的 `daily`、`stk_limit`、`adj_factor`、`daily_basic`、`trade_cal`、`suspend_d` 等关键接口已完成 smoke、candidate/staging、coverage expansion、`suspend_d` full coverage audit、promotion preflight、Goal 14 小范围 `daily_price` promotion validator、Goal 15 显式 `--apply` 小范围标准写入路径、Goal 17 小批次 `daily_price` landing 编排和 Goal 18 标准输入 landing；Goal 20 统一审计真实 clean 所需七项输入，Goal 21 补充确定性、可恢复历史回填框架，Goal 22 则让已可信的标准输入以默认 dry-run、显式 `--apply` 的方式进入 processed `adjusted_price`、clean snapshot、universe 与 `factor_input_table`。只有历史语义、覆盖、DQ、写入和读回全部可信时才能声明对应范围 ready；Goal 22 仍不自动启动 `factor_daily`、选股或回测。

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
- Goal 14：新增小范围 standard `daily_price` promotion validator；只读取 Goal 13C 产物，验证 `daily_price_candidate_batch` 是否满足标准 `daily_price` 契约并默认输出 dry-run 报告。标准写入必须进入 Goal 15 并显式传 `--apply`，不写 standard `suspension_status`，不进入真实 clean/factor/selection/backtest，详见 `docs/goal14_daily_price_promotion_validator.md`。
- Goal 15：新增安全 apply 模式；默认仍 dry-run，只有显式传 `--apply` 才会把已验证的 Tushare candidate 行按 code/date 小范围 upsert 到 canonical `raw/daily_price/...`，并执行 read-back verification。Goal 15 不自动启动 clean/factor/selection/backtest，详见 `docs/goal15_tushare_daily_price_apply.md`。
- Goal 17：新增小批次 Tushare `daily_price` landing 编排；默认不调用 provider、默认 dry-run，可复用已有 Goal 13C candidate/preflight 产物，或显式 `--provider-call` 小范围构建，再运行 Goal 14/15 validator/apply 路径。canonical 写入仍必须传 `--apply`，不写 standard `suspension_status`，不自动启动 clean/factor/selection/backtest，详见 `docs/goal17_tushare_small_batch_daily_price_landing.md`。
- Goal 18：新增 Tushare master/fundamental/financial 标准输入 landing；默认不调用 provider、默认 dry-run，`daily_basic` 和通过 as-of 校验的 `financial` 只有显式 `--apply` 才可小范围写入 `raw/daily_basic/...` 和 `raw/financial/...`。`stock_basic` 当前快照和 ST 状态只写 candidate，不写 standard `stock_basic` / `st_history` / `suspension_status`，不自动启动 clean/factor/selection/backtest，详见 `docs/goal18_tushare_standard_inputs_landing.md`。
- Goal 20：新增真实 clean 所需七项标准输入统一 readiness 审计，覆盖 `stock_basic`、`daily_price`、`adj_factor`、`daily_basic`、`financial`、`st_history` 和 `benchmark_price`。默认不调用 provider、默认 dry-run；provider 访问必须显式传 `--provider-call`，标准写入必须显式传 `--apply`。当前快照、smoke-only benchmark、不可读历史证据或任一缺失/语义不可靠输入都会保持 blocked，不启动 clean/factor/selection/backtest，详见 `docs/goal20_real_clean_input_readiness.md`。
- Goal 21：新增七项标准输入的确定性、可恢复历史回填框架，并完成评审修复：live 响应先写入带 checksum/语义证据哈希的不可变 raw landing；失败审计按 attempt 隔离；`suspend_d` 必须证明完整分页终止；READY checkpoint 可硬中断恢复；canonical 按逻辑 scope 精确替换；financial 使用公告日增量和严格 predecessor proof；v2 自然轴计划避免全市场十年 Cartesian 膨胀。命令默认 plan-only、不会构造或调用 provider；provider 和标准写入仍分别要求 `--provider-call`、`--apply`。当前 live adapter 无法证明历史语义的输入保持 blocked，本轮不声称已经完成真实十年抓取，也不启动 clean/factor/selection/backtest，详见 `docs/goal21_historical_backfill.md`。
- Goal 22：新增 `run-real-clean-universe-range` 范围编排，逐日复用现有 adjusted price、clean snapshot 和 universe builders。命令强制接收显式交易日和 checksum 绑定的 Goal 20 readiness receipt；默认只计算并写 range manifest/逐日 DQ。显式 `--apply` 先写五项不可变 generation，全部读回后再以单个日期 commit marker 原子发布。财务严格按公告日 as-of，股票成员按历史 list/delist，ST 使用历史 `[start_date, end_date)`，停牌只接受显式布尔标准字段；单日失败隔离且可恢复，合法空 universe 可发布，不自动进入 `factor_daily`、`selection_result` 或 backtest，详见 `docs/goal22_real_clean_universe.md`。
- Goal 10B：AKShare / Baostock 最小真实数据 smoke，已验证 AKShare `benchmark_price` 可标准化写入 `smoke/akshare/...` 并通过 DuckDB 查询；字段不足的数据集不会绕过 validator 写入标准层。
- Goal 11：AKShare / Baostock 真实数据能力矩阵与日线 smoke，新增 smoke-only `daily_price_raw_smoke`，只允许写入 `smoke/<provider>/daily_price_raw_smoke/...`，不进入标准 `raw/daily_price/...`。
- Goal 12A：真实数据标准层契约与数据质量等级冻结，详见 `docs/goal12A_real_data_contract.md`；本阶段只新增契约、守门规则和测试，不接入真实 provider 主链路，不做真实回测。

当前未实现：

- 大范围真实标准 `daily_price` promotion。
- 大范围真实财务数据接入和真实财务主链路使用。
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

Goal 15 起，标准 `daily_price` 写入使用显式 `--apply`：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli build-tushare-daily-price-promotion-validator `
  --batch-id <batch_id> `
  --apply
```

可以进一步限制 apply 范围：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli build-tushare-daily-price-promotion-validator `
  --batch-id <batch_id> `
  --apply `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04
```

显式 apply 时会额外写入：

```text
candidate/tushare/standard_daily_price_promotion_apply_report/batch_id=<batch_id>/report.json
```

即便显式 apply，Goal 15 也只允许小范围、幂等、可审计地 upsert standard `daily_price`；不写 standard `suspension_status`，不启动 `clean_daily_snapshot`、`factor_input_table`、`factor_daily`、`selection_result` 或真实回测。apply 后会 read-back verification：校验 promoted row count、canonical key 去重、trade_date 范围、必需字段、OHLC/pre_close、`is_paused` 和涨跌停语义。

### Tushare Goal 17 Small-Batch Daily Price Landing

Goal 17 新增小批次编排命令：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-daily-price-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04
```

默认行为不调用 Tushare provider，只复用已有 `candidate/tushare/...` Goal 13C candidate/preflight 产物并运行 Goal 14 validator dry-run。显式复用 staging 时：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-daily-price-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04 `
  --no-provider-call `
  --reuse-existing-staging
```

只有显式 `--provider-call` 且 Tushare 已启用并配置 token 时，命令才会小范围调用 provider 构建 Goal 13C candidate/preflight。canonical `raw/daily_price/...` 写入仍必须额外传 `--apply`，并继续走 Goal 15 的 idempotent upsert 与 read-back verification。

Goal 17 会写小批次 run report：

```text
candidate/tushare/daily_price_small_batch_run_report/batch_id=<batch_id>/report.json
```

该报告记录 batch、请求范围、provider 是否启用、source artifact keys、candidate/preflight status、validator status、apply/read-back 结果、blocked reasons 和 downstream firewalls。Goal 17 不写 standard `suspension_status`，不启动 `clean_daily_snapshot`、`factor_input_table`、`factor_daily`、`selection_result` 或真实回测。

### Tushare Goal 18 Standard Inputs Landing

Goal 18 新增标准输入 landing 命令：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-standard-inputs-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04
```

默认行为不调用 Tushare provider，只读取已有 staging 并写 run report。显式 provider 调用必须传 `--provider-call`，显式标准层写入必须额外传 `--apply`。

Goal 18 标准写入边界：

- `daily_basic`：通过覆盖、重复键、数值和标准 schema 校验后，可小范围 upsert 到 `raw/daily_basic/...`。
- `financial`：只有严格满足标准字段并且 `announce_date <= start_date` 时，才可按 trade_date 小范围写入 `raw/financial/...`。
- `stock_basic`：Tushare 当前快照不能声明为历史 stock universe，只写 candidate。
- `st_history`：当前 ST/name 快照不能声明为历史 ST 区间，不写 standard `st_history` 或 `suspension_status`。

Goal 18 报告路径：

```text
candidate/tushare/standard_inputs_run_report/batch_id=<batch_id>/report.json
```

该报告记录 batch、请求范围、provider/apply 状态、每个 dataset 的 source key、行数、校验状态、写入状态、blocked reason、DQ 标记、read-back verification 和 downstream firewalls。Goal 18 不启动 `clean_daily_snapshot`、`factor_input_table`、`factor_daily`、`selection_result` 或真实回测。

### Goal 20 Real Clean Input Readiness

Goal 20 用一个小范围命令统一审计 `build_clean_daily_snapshot` 所需七项真实标准输入：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-real-clean-inputs-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04
```

默认不构造或调用真实 provider，也不写 `raw/...`；只审计已有 canonical/Goal 20 staging 并原子写 readiness report 和 manifest。复用 Goal 13 adj-factor staging 时需显式传 `--reuse-existing-staging`，并同时验证对应 Goal 13 manifest。

显式抓取小范围 `adj_factor` 与三指数 benchmark staging 必须传 `--provider-call`；显式 canonical 写入必须另传 `--apply`。两道门禁相互独立，不能绕过历史语义、DQ、coverage、幂等 upsert 或 read-back 校验。

| 输入 | 可接受来源与必要校验 | 必须 blocked 的情况 |
| --- | --- | --- |
| `stock_basic` | 带逐日 snapshot date、可读 upstream evidence key 和 `POINT_IN_TIME_HISTORICAL_SNAPSHOT` 语义的历史 staging；保留 `list_date` / `delist_date`，完整覆盖 code/date | 当前快照冒充历史股票池，或 evidence 不可读/不一致 |
| `daily_price` | 复用 Goal 17 canonical；完整 code/date、唯一键和现有标准 validator | 缺行、重复键或标准契约失败 |
| `adj_factor` | Goal 20 provider staging、有效 Goal 13 staging/manifest 或已验证 canonical；唯一 code/date、有限且严格大于 0 | 非正/非有限值、缺失覆盖、重复键或 Goal 13 manifest 无效 |
| `daily_basic` | 复用 Goal 18 canonical；完整 code/date、唯一键和标准 schema | 缺失、重复或 validator 失败 |
| `financial` | 复用 Goal 18 canonical；唯一财务键并满足逐分区 as-of 校验 | 未来披露、覆盖不足或 schema 失败 |
| `st_history` | 可读 upstream 历史区间 evidence、`HISTORICAL_INTERVAL_SOURCE`、有效 `[start_date, end_date)` 和完整范围证明 | 用当前名称/当前 ST 状态倒推，或证据/覆盖不可靠 |
| `benchmark_price` | 每个请求日完整且唯一覆盖 `000300.SH`、`000905.SH`、`000906.SH` | 缺任一指数、重复键、数值无效，或只有 smoke 产物 |

控制与 staging 对象键：

```text
candidate/real_clean_inputs/manifest/batch_id=<batch_id>/manifest.json
candidate/real_clean_inputs/readiness_report/batch_id=<batch_id>/report.json
candidate/real_clean_inputs/adj_factor_staging/batch_id=<batch_id>/trade_date=YYYY-MM-DD/part.parquet
candidate/real_clean_inputs/benchmark_price_staging/batch_id=<batch_id>/trade_date=YYYY-MM-DD/part.parquet
candidate/real_clean_inputs/stock_basic_history_staging/batch_id=<batch_id>/trade_date=YYYY-MM-DD/part.parquet
candidate/real_clean_inputs/st_history_interval_staging/batch_id=<batch_id>/part.parquet
candidate/real_clean_inputs/st_history_interval_staging/batch_id=<batch_id>/coverage.json
```

`st_history` 在确实没有任何区间行时可以保持空集，但必须由 `coverage.json` 和可读 upstream 空集 evidence 共同证明全 code/date 覆盖；不会为了通过非空校验伪造 ST 行。`ready_for_apply=true` 只表示七项均已有可信 canonical 或已验证、可安全写入的来源；`ready_for_clean=true` 还要求七项 canonical 在请求范围内完整读回并逐值匹配。任一输入缺失、语义不可信、写入/读回失败时 readiness 为 `false`。Goal 20 不调用 `build_clean_daily_snapshot`，也不启动 universe、factor、selection 或 backtest。

### Goal 21 Resumable Historical Backfill

Goal 21 为 Goal 20 的七项输入补充历史回填控制面：

```text
stock_basic daily_price adj_factor daily_basic financial st_history benchmark_price
```

默认命令只生成不可变 plan 和 root manifest，不构造 provider、不访问 provider 网络、不写 staging Parquet 或 canonical：

```powershell
docker compose build stock-python
docker compose run --rm stock-python python -m stock_selector.cli run-real-history-backfill `
  --run-id <run_id> `
  --start-date 2019-01-01 `
  --end-date 2024-12-31 `
  --codes 000001.SZ,600519.SH
```

单日增量使用新的 `run_id`，并把起止日期设为同一天；下面仍是安全的 plan-only，审核 plan 后才按需追加显式门禁：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-real-history-backfill `
  --run-id goal21-2025-01-02 `
  --start-date 2025-01-02 `
  --end-date 2025-01-02 `
  --universe-key raw/universe/history.parquet
```

必须提供 `--run-id`、起止日期，以及互斥的 `--codes` 或安全 `.parquet` `--universe-key`。默认 `--resume`；可显式传 `--no-resume` 或 `--force`，但它们不会自动打开 provider/写入门禁。v2 planner 的有效批量参数及默认值为：

```text
--code-batch-size 250
--date-batch-days 31
--financial-announce-days 31
```

`--report-period-months 3` 仅为旧 v1 runbook 保留解析兼容；v2 会校验其为正数，但不会使用或把它重解释为公告期。

四种模式彼此清晰隔离：

| 模式 | 参数 | 行为 |
| --- | --- | --- |
| plan-only | 无 opt-in | 只写 control artifacts；无 provider 调用、无 canonical 写入 |
| provider-only | `--provider-call` | 写不可变 attempt staging；不写 `raw/...` |
| apply-only | `--apply` | 不访问 provider，只消费已核验 staging；缺失/不匹配则 blocked |
| combined | `--provider-call --apply` | 先 staging，再校验、幂等 upsert 和 canonical read-back |

控制产物位于：

```text
candidate/real_history_backfill/run_id=<run_id>/plan.json
candidate/real_history_backfill/run_id=<run_id>/manifest.json
candidate/real_history_backfill/run_id=<run_id>/dataset=<dataset>/chunk_id=<chunk_id>/manifest.json
candidate/real_history_backfill/run_id=<run_id>/dataset=<dataset>/chunk_id=<chunk_id>/attempt=<attempt>/report.json
candidate/real_history_backfill/run_id=<run_id>/dataset=<dataset>/chunk_id=<chunk_id>/attempt=<attempt>/part.parquet
```

live provider 响应先写入 `raw/provider_landing/provider=<provider>/run_id=<run_id>/endpoint=<endpoint>/request=<hash>/response=<hash>/evidence=<hash>/part.parquet` 并读回校验；`suspend_d` 的 completeness/truncation/pagination 也绑定在 evidence hash 中。chunk manifest 保持兼容的单数 `source_key`，完整有序 `source_keys` 和 `provider_calls` 保存在不可变 attempt report，并由 `staging_attempt` 关联。

canonical 仍为 `raw/<dataset>/trade_date=YYYY-MM-DD/part.parquet`，由单写者串行执行 atomic、逻辑 scope 精确替换和读回核验。运维上禁止并发运行同一 `run_id`，也禁止并发执行会覆盖同一 canonical partition 的 `--apply` 作业；atomic replace 只能防止半写对象，不能防止两个进程 read-merge-write 时丢失彼此更新。v2 对 5,000 股票、2015-01-01 至 2024-12-31 的全七项计划实际为 592 chunks，preflight 保守估计 710 chunks、13,520 provider calls 和 36,557 canonical reads；超过硬预算会在任何 provider/canonical 操作前 blocked，market sidecar cache 也只保留当前日期窗口。

当前内置 live 路由只在证据完整时接受 Tushare 的 `daily_price` / `adj_factor` / `daily_basic`；AKShare contract 虽只允许三指数 `benchmark_price`，但当前 CLI 尚无可证明完整语义的公开历史 range adapter，因此 live benchmark 仍 blocked，smoke 产物不会被冒充正式历史。当前 `stock_basic` 快照、未解决字段/单位语义的 `financial`、当前名称/状态倒推的 `st_history` 同样保持 `SEMANTIC_SOURCE_UNAVAILABLE`。Goal 21 不宣称已完成真实十年抓取；Goal 20 继续负责七输入 readiness，Goal 25 负责补齐完整 live capability。

所有 chunk manifest 都保留 source、row count、schema、DQ、coverage、checksum、validation、write/read-back、failure category 和 state；敏感 token/credential 会被脱敏。每次新 provider attempt 不继承旧失败证据；atomic staging 写入后即使读回失败也保留 key/checksum 供审计。`--resume` 会先严格核验可恢复的 `READY_TO_CHECKPOINT` report，并且只有 staging checksum 和 canonical exact-scope evidence 均匹配时才跳过；`--force` 也不能绕过显式门禁或历史语义校验。下游 `clean_daily_snapshot`、factor、selection、backtest 防火墙始终关闭。完整说明见 `docs/goal21_historical_backfill.md`。

### Goal 22 Real Clean, Universe and Factor Input

Goal 22 只消费已经落在 standard `raw/...` 的七项输入，并逐交易日复用现有函数执行：

```text
adjusted_price -> clean_daily_snapshot -> risk_filter -> eligible_universe -> factor_input_table
```

默认 dry-run 会完成输入校验和内存计算，但不写 processed Parquet：

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-real-clean-universe-range `
  --run-id <run_id> `
  --start-date 2024-01-02 `
  --end-date 2024-01-03 `
  --trade-dates 2024-01-02,2024-01-03 `
  --readiness-report-key candidate/real_clean_inputs/readiness_report/batch_id=<batch-id>/report.json
```

`--trade-dates` 与至少一个 `--readiness-report-key` 都是必需门禁；不再从 raw 市场分区猜测日期。只有显式追加 `--apply` 才将五项 `processed` 输出写入不可变 generation，全部 schema、行数与 checksum 读回通过后，再原子写 `processed/_goal22_commits/trade_date=YYYY-MM-DD/commit.json` 发布整日结果。范围 manifest 位于 `candidate/real_clean_universe/run_id=<run_id>/manifest.json`，逐日 DQ 位于同一 run 下的 `trade_date=.../dq_report.json`，记录 readiness lineage、输入版本、排除原因、缺失率、generation/commit key、checksum 和恢复状态。任一必需输入缺失、receipt/checksum 漂移或历史语义/DQ/读回失败只阻断对应日期；其他日期继续。完整合同见 `docs/goal22_real_clean_universe.md`。

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
