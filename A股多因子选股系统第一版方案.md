# A股中长线多因子选股系统第一版方案

## 1. 项目定位

本系统第一版不做“自动预测股价”或“自动交易”，而是做一个面向中长线投资者的：

> A股中长线多因子选股 + 回测验证 + 大模型解释系统

核心目标是：

1. 自动获取A股日线、估值、财务、行业等数据；
2. 计算适合中长线的质量、成长、估值、行业和趋势确认因子；
3. 生成股票综合评分和候选股票池；
4. 剔除高风险、低流动性、财务质量明显恶化的股票；
5. 回测中长线评分策略在过去是否有稳定优势；
6. 使用大模型生成选股理由、风险解释和复盘报告；
7. 通过页面展示候选股票池、个股分析、调仓建议和回测结果。

第一版系统不追求复杂，而追求：

```text
数据正确
逻辑不作弊
回测可信
结果可解释
适合中长线持有
```

第一版默认投资周期：

```text
调仓频率：每月或每季度
持有周期：3个月、6个月、12个月
目标：寻找质量较好、成长稳定、估值合理、行业不过度逆风的股票
不追求：明日涨跌、短线热点、高频择时
```

---

## 2. 系统边界

### 2.1 第一版要做的内容

```text
日线级别数据
基础财务数据
估值数据
行业数据
质量因子
成长因子
估值因子
行业强度因子
中长线趋势确认因子
流动性过滤
股票评分
风险过滤
中长线历史回测
大模型解释
页面展示
Docker部署
```

### 2.2 第一版暂时不做的内容

```text
分钟线
盘口数据
逐笔成交
自动下单
强化学习交易
复杂机器学习预测
高频交易
复杂投研平台
券商账户接入
大模型直接预测股价
周频短炒
分钟级择时
```

---

## 3. 技术栈推荐

| 模块    | 技术                           |
| ----- | ---------------------------- |
| 主语言   | Python                       |
| 数据源   | Tushare Pro、AKShare、Baostock |
| 数据处理  | Pandas、Polars                |
| 数据存储  | Parquet                      |
| 数据查询  | DuckDB                       |
| 元数据存储 | PostgreSQL（本地已有），轻量模式可用 SQLite |
| 对象存储  | MinIO（本地已有），用于 Parquet、报告、图表 |
| 回测    | vectorbt                     |
| 服务后端  | Spring Boot（本地已有，可作为 API/任务服务） |
| 页面展示  | Streamlit 或前端页面              |
| 大模型接口 | OpenAI API 或其他大模型 API        |
| 配置管理  | YAML                         |
| 日志    | loguru                       |
| 环境变量  | python-dotenv                |
| 部署    | Docker / Docker Compose       |

第一版的核心计算仍然建议放在 Python。
原因是：数据处理、因子计算、回测和大模型调用都更适合 Python 生态。

推荐第一版组合：

```text
Python量化引擎 + Parquet/MinIO + DuckDB + PostgreSQL + Spring Boot API + Streamlit/前端 + Docker
```

本地已有组件的作用：

```text
Python：
  负责数据采集、清洗、因子计算、评分、回测、大模型解释。

PostgreSQL：
  负责存储任务记录、股票池快照、回测摘要、用户配置、关注列表。
  不建议存储全量日线和全量因子明细。

MinIO：
  负责存储 Parquet 数据、回测明细、报告文件、图表文件。
  Docker部署后比直接写本地 data/ 目录更稳定。

Spring Boot：
  负责提供 API、任务触发、查询股票池、查询回测结果、管理配置。
  不负责因子计算和回测核心逻辑。

Docker Compose：
  负责统一启动 PostgreSQL、MinIO、Python worker、Spring Boot API、页面服务。
```

---

## 4. 系统总体架构

```text
数据源
  ├── Tushare
  ├── AKShare
  └── Baostock
        ↓
Python 数据采集模块
        ↓
原始数据 Parquet（本地或 MinIO）
        ↓
Python 数据清洗模块
        ↓
清洗后数据 Parquet（本地或 MinIO）
        ↓
中长线因子计算模块
        ↓
因子表 Parquet
        ↓
硬风险过滤
        ↓
股票评分模块
        ↓
候选股票池 / 调仓股票池
        ↓
中长线回测模块
        ↓
回测报告（MinIO） + 回测摘要（PostgreSQL）
        ↓
大模型解释模块
        ↓
Spring Boot API / Streamlit / 前端页面
```

大模型只放在后面：

```text
Python 量化系统负责计算
Spring Boot 负责服务化
PostgreSQL 负责元数据和结果索引
MinIO 负责文件和 Parquet 对象存储
大模型负责解释
```

不要让大模型直接计算全市场数据，也不要直接问大模型“某股票会不会涨”。

---

## 5. 项目目录结构

```text
stock-selector-v1/
├── README.md
├── requirements.txt
├── .env
├── config/
│   ├── settings.yaml
│   └── factor_weights.yaml
├── data/
│   ├── raw/
│   │   ├── stock_basic/
│   │   ├── daily_price/
│   │   ├── adj_factor/
│   │   ├── daily_basic/
│   │   ├── financial/
│   │   └── industry/
│   ├── processed/
│   │   ├── adjusted_price/
│   │   ├── factors/
│   │   ├── selection/
│   │   └── reports/
│   └── backtest/
├── src/
│   ├── main.py
│   ├── data/
│   │   ├── fetch_tushare.py
│   │   ├── fetch_akshare.py
│   │   ├── data_cleaner.py
│   │   └── data_validator.py
│   ├── factors/
│   │   ├── trend_factors.py
│   │   ├── volume_factors.py
│   │   ├── valuation_factors.py
│   │   ├── growth_factors.py
│   │   └── industry_factors.py
│   ├── scoring/
│   │   ├── score_engine.py
│   │   └── risk_filter.py
│   ├── backtest/
│   │   ├── backtest_engine.py
│   │   └── metrics.py
│   ├── llm/
│   │   ├── report_generator.py
│   │   ├── stock_explainer.py
│   │   └── announcement_parser.py
│   ├── app/
│   │   └── streamlit_app.py
│   └── utils/
│       ├── logger.py
│       ├── trading_calendar.py
│       └── file_utils.py
└── notebooks/
    ├── 01_data_check.ipynb
    ├── 02_factor_check.ipynb
    └── 03_backtest_check.ipynb
```

---

## 6. 数据设计

### 6.1 股票基础信息表

用途：判断股票是否存在、是否上市、是否退市、属于什么行业。

字段设计：

```text
stock_code        股票代码
stock_name        股票名称
exchange          交易所
list_date         上市日期
delist_date       退市日期
industry          行业
market_type       主板 / 创业板 / 科创板 / 北交所
is_st             是否ST
```

---

### 6.6 ST 状态历史表（v1.1 新增）

用途：记录每只股票 ST 状态的起止时间，回测时按日期精确过滤，避免用当前快照倒推过去。

字段设计：

```text
stock_code        股票代码
st_type           类型：ST / *ST / PT
start_date        ST 生效日期
end_date          摘帽日期（NULL 表示当前仍在 ST）
source            数据来源
```

关键规则：

```text
回测时，在 trade_date 当天：
  - 若 start_date <= trade_date
    且 (end_date IS NULL 或 end_date > trade_date)，
    则该股票属于 ST，应当剔除
  - 否则该股票正常

risk_filter 在回测模式下使用此表，
而非 stock_basic 的快照 is_st 字段。
```

数据来源：Tushare `stk_managers_new` 接口或 AKShare `stock_st_*` 系列接口，每日更新时一并拉取。

---

### 6.7 公告解析结果表（v1.1 新增）

用途：存储大模型对个股公告的解析结果，供 risk_filter 查询近期负面公告。

字段设计：

```text
stock_code        股票代码
announce_date     公告日期
event_type        事件类型（股东减持、业绩预亏、立案调查等）
sentiment         positive / neutral / negative
risk_level        low / medium / high
summary           大模型摘要
need_remove       是否建议从股票池剔除
created_at        解析时间
```

risk_filter 查询规则：

```text
在 trade_date 当天：
  - 查找该股票过去 30 个自然日内 need_remove = true 的公告
  - 若存在，直接剔除
```

---

### 6.2 日线行情表

用途：计算涨跌幅、均线、成交量、趋势等因子。

字段设计：

```text
stock_code        股票代码
trade_date        交易日期
open              开盘价
high              最高价
low               最低价
close             收盘价
pre_close         昨收价
volume            成交量
amount            成交额
pct_chg           涨跌幅
```

---

### 6.3 复权因子表

用途：处理分红、送股、转增导致的价格断层。

字段设计：

```text
stock_code        股票代码
trade_date        交易日期
adj_factor        复权因子
```

生成复权价：

```text
adj_close = close * adj_factor
adj_open  = open  * adj_factor
adj_high  = high  * adj_factor
adj_low   = low   * adj_factor
```

第一版所有技术指标都使用复权价格计算。

---

### 6.4 每日估值表

用途：判断股票当前估值水平。

字段设计：

```text
stock_code        股票代码
trade_date        交易日期
pe_ttm            市盈率TTM
pb                市净率
ps_ttm            市销率TTM
total_mv          总市值
circ_mv           流通市值
turnover_rate     换手率
```

---

### 6.5 财务指标表

用途：判断公司质量、成长性、负债水平和现金流情况。

字段设计：

```text
stock_code              股票代码
report_period           财报所属期
announce_date           公告日期
revenue_yoy             营收同比
net_profit_yoy          净利润同比
roe                     净资产收益率
gross_margin            毛利率
debt_ratio              资产负债率
operating_cashflow      经营现金流
```

关键点：

```text
选股当天只能使用 announce_date <= 当前日期 的财务数据
```

不能用还没有公告的财报数据，否则就是未来函数。

---

## 7. 数据存储方案

第一版采用：

```text
大量行情和因子数据：Parquet（本地文件或 MinIO 对象存储）
查询和分析：DuckDB
配置、任务记录、结果索引：PostgreSQL
```

推荐存储方式：

```text
data/raw/daily_price/trade_date=2026-06-19/part.parquet
data/raw/daily_basic/trade_date=2026-06-19/part.parquet
data/processed/factors/trade_date=2026-06-19/factors.parquet
data/processed/selection/trade_date=2026-06-19/selection.parquet
```

不建议第一版把所有行情数据都塞进 PostgreSQL。

Docker部署时推荐：

```text
容器和数据卷独立命名：
  stock-postgres        新系统专用 PostgreSQL 容器
  stock-minio           新系统专用 MinIO 容器
  stock_pgdata          新系统专用 PostgreSQL volume
  stock_minio_data      新系统专用 MinIO volume 或数据目录

MinIO bucket:
  stock-raw/             原始行情、估值、财务、公告数据
  stock-processed/       复权价格、因子、股票池
  stock-backtest/        回测明细、收益曲线、图表、报告

PostgreSQL:
  update_log             数据更新步骤状态
  selection_snapshot     每期股票池摘要
  backtest_summary       回测指标摘要
  factor_config          因子权重和版本
  user_watchlist         用户关注股票
```

这样设计的好处是：

```text
明细数据适合批量读取和重算，放 Parquet/MinIO
结果索引适合页面查询和服务接口，放 PostgreSQL
DuckDB 可以直接分析 Parquet，不必把所有数据导入关系库
```

不要复用其他项目的数据库容器、MinIO 容器、volume 或 bucket。
例如本机已有的 `tss-postgres`、`minio-tss` 应视为 TSS 项目资源，不作为本系统默认依赖。

---

## 8. 数据采集模块

### 8.1 目标

每天收盘后自动更新当日数据。

### 8.2 数据更新流程

```text
1. 获取交易日历
2. 判断指定日期是否交易日
3. 拉取股票基础信息
4. 拉取日线行情
5. 拉取复权因子
6. 拉取每日估值
7. 拉取财务数据
8. 保存为 Parquet
9. 进行数据校验
10. 记录日志
```

### 8.3 核心函数设计

```python
def update_daily_data(trade_date: str):
    """
    更新指定交易日的数据。
    """
    fetch_stock_basic()
    fetch_daily_price(trade_date)
    fetch_adj_factor(trade_date)
    fetch_daily_basic(trade_date)
    fetch_financial_data_if_needed(trade_date)
    validate_daily_data(trade_date)
```

### 8.4 数据校验规则

```text
stock_code + trade_date 不能重复
close 不能小于等于 0
volume 不能小于 0
amount 不能小于 0
adj_factor 不能为空
trade_date 格式必须正确
股票代码格式必须正确
关键字段缺失时必须记录日志
```

---

### 8.5 数据更新幂等性与断点续传（v1.1 新增）

每次更新写入前检查是否已完成，支持失败后安全重跑。

步骤完成状态记录在 SQLite 的 `update_log` 表中：

```text
CREATE TABLE update_log (
    trade_date TEXT,
    step_name  TEXT,
    status     TEXT,       -- 'done' / 'failed'
    updated_at TEXT,
    PRIMARY KEY (trade_date, step_name)
);
```

核心更新函数：

```python
def update_daily_data(trade_date: str):
    """
    幂等更新：每步写入前检查是否已存在。
    失败步骤可单独重跑，已完成的步骤自动跳过。
    """
    steps = [
        ("stock_basic",    fetch_stock_basic),
        ("daily_price",    lambda: fetch_daily_price(trade_date)),
        ("adj_factor",     lambda: fetch_adj_factor(trade_date)),
        ("daily_basic",    lambda: fetch_daily_basic(trade_date)),
        ("financial",      lambda: fetch_financial_data_if_needed(trade_date)),
        ("st_history",     lambda: fetch_st_history(trade_date)),
        ("announcements",  lambda: fetch_announcements(trade_date)),
    ]

    for step_name, step_fn in steps:
        if is_step_done(step_name, trade_date):
            logger.info(f"跳过已完成步骤: {step_name}")
            continue
        try:
            step_fn()
            mark_step_done(step_name, trade_date)
        except Exception as e:
            mark_step_failed(step_name, trade_date)
            logger.error(f"步骤 {step_name} 失败: {e}")
            raise

    validate_daily_data(trade_date)
```

Parquet 写入使用临时文件 + 原子 rename，避免写入中途崩溃留下半截文件：

```python
tmp_path = f"data/raw/daily_price/trade_date={trade_date}/.tmp.parquet"
final_path = f"data/raw/daily_price/trade_date={trade_date}/part.parquet"
df.to_parquet(tmp_path)
os.rename(tmp_path, final_path)  # 同文件系统内为原子操作
```

---

## 9. 数据清洗模块

### 9.1 清洗目标

把原始数据处理成可以计算因子的标准数据。

### 9.2 清洗内容

| 清洗项    | 说明                                     |
| ------ | -------------------------------------- |
| 复权处理   | 生成 adj_open、adj_high、adj_low、adj_close |
| 停牌处理   | 停牌股票不参与买卖                              |
| ST处理   | 第一版默认剔除 ST / *ST                       |
| 新股过滤   | 上市不足120个交易日剔除                          |
| 退市处理   | 回测时不能用当前股票池倒推过去                        |
| 缺失值处理  | 缺少关键因子的股票不参与评分                         |
| 低流动性过滤 | 20日平均成交额太低的股票剔除                        |

---

## 10. 因子设计

第一版做五类核心因子和一类过滤因子：

```text
质量因子
成长因子
估值因子
行业强度因子
中长线趋势确认因子
流动性过滤因子
风险过滤因子
```

---

### 10.1 趋势因子

目标：判断股票是否处于明显破位或长期弱势状态。

中长线系统里，趋势因子只做确认，不做主导。不能因为短期上涨快就给过高分，也不能把系统做成追涨模型。

| 因子               | 说明             |
| ---------------- | -------------- |
| ma20             | 20日均线          |
| ma60             | 60日均线          |
| ma120            | 120日均线         |
| ret_20d          | 近20日涨跌幅        |
| ret_60d          | 近60日涨跌幅        |
| price_ma60_ratio | 当前价格相对60日均线的偏离 |
| high_60d_ratio   | 当前价格相对60日高点的位置 |

示例逻辑：

```python
df["ma20"] = df.groupby("stock_code")["adj_close"].rolling(20).mean()
df["ma60"] = df.groupby("stock_code")["adj_close"].rolling(60).mean()
df["ma120"] = df.groupby("stock_code")["adj_close"].rolling(120).mean()

df["ret_20d"] = df.groupby("stock_code")["adj_close"].pct_change(20)
df["ret_60d"] = df.groupby("stock_code")["adj_close"].pct_change(60)
```

---

### 10.2 量能因子

目标：判断股票是否具备基本流动性。

中长线第一版不建议把量能作为核心加分项，而是作为过滤条件使用。

| 因子            | 说明               |
| ------------- | ---------------- |
| vol_ma20      | 20日平均成交量         |
| vol_ratio     | 当前成交量 / 20日平均成交量 |
| amount_ma20   | 20日平均成交额         |
| turnover_ma20 | 20日平均换手率         |

第一版不要过度依赖“主力资金”数据。
成交额、换手率、成交量变化更基础，也更容易回测。

建议规则：

```text
20日平均成交额过低：剔除
长期停牌或近期频繁停牌：剔除
成交额突然异常放大：不直接加分，只作为风险提示
```

---

### 10.3 估值因子

目标：判断股票是否过贵。

| 因子               | 说明        |
| ---------------- | --------- |
| pe_ttm           | 市盈率TTM    |
| pb               | 市净率       |
| ps_ttm           | 市销率TTM    |
| pe_percentile_3y | 当前PE近3年分位 |
| pb_percentile_3y | 当前PB近3年分位 |
| fcf_yield        | 自由现金流收益率，可后续扩展 |

估值不能简单理解成 PE 越低越好。

更合理的是看：

```text
当前估值处于该股票历史估值的什么位置
当前估值是否匹配公司的质量和成长
```

例如：

```text
PE近3年分位 = 30%
```

表示当前估值比过去三年多数时候都低。

中长线系统不能只买低 PE。
低估值可能来自市场错误定价，也可能来自基本面永久恶化。
估值因子必须和质量、成长、行业一起使用。

---

### 10.4 质量因子

目标：筛选盈利能力、资产质量和现金流质量较好的公司。

| 因子                 | 说明     |
| ------------------ | ------ |
| roe                | 净资产收益率 |
| roic               | 投入资本回报率 |
| gross_margin       | 毛利率    |
| net_margin         | 净利率    |
| debt_ratio         | 资产负债率  |
| operating_cashflow | 经营现金流  |
| cashflow_profit_ratio | 经营现金流 / 净利润 |

第一版基础规则：

```text
ROE 和 ROIC 不能长期为负
经营现金流不能长期显著弱于净利润
资产负债率不能过高
毛利率和净利率不能持续恶化
```

### 10.5 成长因子

目标：判断公司是否具备较稳定的中长期增长能力，而不是只看单季高增。

| 因子                 | 说明     |
| ------------------ | ------ |
| revenue_yoy        | 营收同比   |
| net_profit_yoy     | 净利润同比  |
| revenue_cagr_3y    | 近3年营收复合增速 |
| profit_cagr_3y     | 近3年净利润复合增速 |
| growth_stability   | 成长稳定性 |
| earnings_revision  | 盈利预期变化，可后续扩展 |

第一版基础规则：

```text
不追求单期暴增
优先选择连续多个报告期稳定增长的公司
营收增长和利润增长不能长期背离
净利润高增但经营现金流差的股票要降权
```

---

### 10.6 行业强度因子

目标：判断股票所属行业是否处于明显逆风或相对强势状态。

计算逻辑：

```text
行业近60日涨幅
行业近120日涨幅
行业相对沪深300强度
个股在行业内的涨幅排名
行业内盈利增速变化
```

示例：

```text
industry_strength_120d = industry_return_120d - hs300_return_120d
```

如果结果大于 0，说明该行业近120日强于沪深300。

中长线第一版不需要一开始就做复杂行业景气模型，但至少要避免在行业明显下行时只因为个股估值低就重仓入选。

---

## 11. 股票评分模型

第一版采用规则评分，不直接上机器学习。

### 11.1 总分公式

```text
综合评分 = 质量评分 × 30%
        + 成长评分 × 25%
        + 估值评分 × 20%
        + 行业强度评分 × 15%
        + 趋势确认评分 × 10%
```

### 11.2 权重设计

| 评分项    |  权重 |
| ------ | --: |
| 质量评分   | 30% |
| 成长评分   | 25% |
| 估值评分   | 20% |
| 行业强度评分 | 15% |
| 趋势确认评分 | 10% |

量能不进入综合评分，只作为流动性过滤和风险提示。

中长线系统的排序逻辑是：

```text
先排除不能买的股票
再寻找质量和成长可靠的股票
再判断估值是否合理
最后用行业和趋势确认买入时点
```

---

### 11.3 趋势确认评分示例

```python
def calc_trend_score(row):
    score = 0

    if row["adj_close"] > row["ma20"]:
        score += 20

    if row["ma20"] > row["ma60"]:
        score += 20

    if row["ma60"] > row["ma120"]:
        score += 20

    if row["ret_20d"] > 0:
        score += 15

    if row["ret_60d"] > 0:
        score += 15

    if row["ret_20d"] > 0.35:
        score -= 20

    return max(0, min(100, score))
```

短期涨幅过大要扣分，避免系统变成单纯追高。

趋势确认评分只占 10%，即使趋势很好，也不能弥补质量、成长和估值的明显缺陷。

---

## 12. 风险过滤模块

风险过滤要放在评分之前。

### 12.1 必须剔除的股票

```text
ST / *ST
退市整理股
上市不足120个交易日的新股
停牌股
成交额过低的股票
连续亏损严重的股票
资产负债率过高的股票
财务数据缺失或未按公告日生效的股票
近期有重大风险公告的股票（v1.1 增强）
```

### 12.2 v1.1 过滤规则

```python
def risk_filter(
    df,
    trade_date: str,
    mode: str = "live",
    announcement_filter_enabled: bool = False,
):
    """
    风险过滤。
    mode='live'  使用 stock_basic 快照 is_st；
    mode='backtest' 使用 st_status_history 按日期过滤。
    """
    # 1. 剔除 ST（回测模式下按日期查历史表）
    if mode == "backtest":
        st_stocks = load_st_stocks_on_date(trade_date)
        df = df[~df["stock_code"].isin(st_stocks)]
    else:
        df = df[df["is_st"] == False]

    # 2. 剔除上市不足 120 个交易日的新股
    df = df[df["listed_days"] >= 120]

    # 3. 剔除 20 日均成交额 < 1 亿的低流动性股票
    df = df[df["amount_ma20"] >= 100_000_000]

    # 4. 剔除资产负债率 > 80% 的股票
    df = df[df["debt_ratio"] <= 0.80]

    # 5. 剔除 ROE < 0（当期亏损）的股票
    df = df[df["roe"] >= 0]

    # 6. v1.1 增强：剔除近 30 日内有负面公告且建议剔除的股票
    # 第一版核心回测可以先关闭该规则，避免 LLM 解析结果影响可复现性。
    if announcement_filter_enabled:
        removed_by_announcement = load_announcement_removed_stocks(trade_date)
        df = df[~df["stock_code"].isin(removed_by_announcement)]

    return df
```

### 12.3 risk_level 计算规则（v1.1 新增）

`risk_level` 字段基于剩余风险暴露度计算，分三档：

```python
def calc_risk_level(row) -> str:
    """
    综合评分 + 单项因子底线决定风险等级。
    已经过 risk_filter 的股票不会出现极端差的基本面，
    风险等级反映的是选股信心而非绝对风险。
    """
    # 硬底线：任一核心维度严重偏低 → high
    if row["quality_score"] < 40:
        return "high"
    if row["growth_score"] < 30:
        return "high"
    if row["debt_ratio"] > 0.70:
        return "high"
    if row["trend_score"] < 30:
        return "high"

    # 综合评分分档
    if row["total_score"] >= 75:
        return "low"
    elif row["total_score"] >= 55:
        return "medium"
    else:
        return "high"
```

规则意图：

```text
low：    综合评分高，无硬伤，入选信心较强
medium： 综合评分中等，需结合其他信息判断
high：   存在明显短板或评分偏低，即使未被剔除也应谨慎
```

### 12.4 公告负面信号的集成路径（v1.1 新增）

完整链路：

```text
1. 每日数据更新时拉取近期公告
       ↓
2. announcement_parser.py 调用大模型解析公告
       ↓
3. 解析结果写入 announcement_results 表（§6.7）
       ↓
4. risk_filter() 查询近 30 日 need_remove=true 的股票并剔除
       ↓
5. 被剔除的股票不出现在当日股票池中
```

公告拉取与解析在数据更新阶段完成，不在选股阶段实时调用大模型。

---

## 13. 选股结果设计

### 13.1 输出字段

```text
trade_date
stock_code
stock_name
industry
total_score
quality_score
growth_score
valuation_score
industry_score
trend_score
risk_level         计算规则见 §12.3，取值 low / medium / high
suggestion         选股建议，LLM 模式下由大模型生成，否则由规则引擎生成（§16.4）
reason             入选理由，LLM 模式下由大模型生成，否则由规则引擎生成（§16.4）
```

### 13.2 输出示例

```json
{
  "trade_date": "2026-06-19",
  "stock_code": "000938.SZ",
  "stock_name": "紫光股份",
  "industry": "ICT设备",
  "total_score": 78,
  "quality_score": 76,
  "growth_score": 74,
  "valuation_score": 62,
  "industry_score": 82,
  "trend_score": 68,
  "risk_level": "medium",
  "suggestion": "观察，不追高",
  "reason": "公司质量和成长表现较稳，行业相对强度较高，但估值优势不明显，适合等待更好的买入位置。"
}
```

---

## 14. 回测模块设计

回测是第一版最重要的部分。

系统必须验证：

```text
当前评分模型在中长线持有周期内是否有稳定优势
```

---

### 14.1 回测策略A：每月调仓，买评分前20

```text
每月第一个交易日选股
买入综合评分前20
等权买入
持有一个月
下月重新调仓
```

---

### 14.2 回测策略B：每季度调仓，买评分前30

```text
每季度第一个交易日选股
买入综合评分前30
等权买入
持有一个季度
下季度重新调仓
```

---

### 14.3 回测策略C：质量成长估值组合

```text
只买满足以下条件的股票：
综合评分 > 75
质量评分 > 70
成长评分 > 60
估值评分 > 50
行业强度评分 > 60
趋势评分 > 40
```

策略C用于验证“高质量 + 稳定成长 + 合理估值”组合是否优于单纯追趋势。

---

### 14.4 回测成交时点

必须明确交易时点，避免无意中使用未来数据。

第一版统一规则：

```text
T日收盘后计算因子和股票池
T+1交易日开盘或 VWAP 买入
调仓卖出也在 T+1交易日执行
遇到停牌、涨停无法买入、跌停无法卖出时，按不可成交处理
```

---

### 14.5 回测成本设置

第一版成本假设：

```text
买入佣金：可配置，默认 0.03%
卖出佣金：可配置，默认 0.03%
卖出印花税：按交易日期配置，默认 0.05%
滑点：可配置，默认 0.10%
```

买入价格：

```text
实际买入价格 = 理论买入价格 × (1 + 佣金 + 滑点)
```

卖出价格：

```text
实际卖出价格 = 理论卖出价格 × (1 - 佣金 - 印花税 - 滑点)
```

---

### 14.6 回测指标

| 指标          | 说明            |
| ----------- | ------------- |
| 累计收益        | 总收益           |
| 年化收益        | 平均每年收益        |
| 最大回撤        | 从最高点到最低点的最大亏损 |
| 胜率          | 盈利交易占比        |
| 盈亏比         | 平均盈利 / 平均亏损   |
| 换手率         | 交易频率          |
| 波动率         | 收益曲线稳定性       |
| 夏普比率        | 单位风险对应收益      |
| 相对沪深300超额收益 | 是否跑赢市场基准      |
| 相对中证500超额收益 | 是否跑赢中盘基准      |
| 相对中证800超额收益 | 是否跑赢宽基基准      |
| 年度胜率       | 每年是否跑赢基准      |
| 持仓平均周期     | 是否符合中长线目标     |

对新手来说，最大回撤比年化收益更重要。

中长线第一版建议至少回测 5 年，条件允许时回测 8-10 年，覆盖牛市、熊市和震荡市。

---

## 15. 防止回测作弊的规则

### 15.1 不能使用未来数据

在某个交易日选股时，只能使用该日期之前已经知道的数据。

错误：

```python
features = ["pe", "roe", "ma60", "future_return_20d"]
```

正确：

```python
usable_prices = prices[prices["trade_date"] <= current_date]
usable_financials = financials[financials["announce_date"] <= current_date]
```

---

### 15.2 财务数据必须按公告日生效

例如：

```text
财报所属期：2023-12-31
公告日期：2024-04-20
```

那么：

```text
2024-04-20 之前不能使用这份财报
2024-04-20 之后才能使用这份财报
```

---

### 15.3 不能用当前股票池倒推过去

错误：

```python
current_stocks = get_current_stock_list()
backtest(current_stocks, start="2015-01-01")
```

正确：

```python
stocks_at_date = get_stock_list_by_date(current_date)
```

即：

```text
2018年选股，只能从2018年当时存在的股票中选
```

---

### 15.4 不能随机划分训练集

股票数据是时间序列，不能随机打乱。

错误：

```python
train_test_split(df, test_size=0.2, shuffle=True)
```

正确：

```text
训练集：2015-2020
验证集：2021-2022
测试集：2023-2025
```

---

## 16. 大模型模块设计

大模型第一版只做三件事：

```text
生成本期选股报告
解释个股为什么入选
生成回测和调仓复盘
```

公告风险解析放在 v1.1 增强功能中，不参与第一版核心回测和评分。

---

### 16.1 大模型不做的事

```text
不直接预测明天涨跌
不直接给目标价
不直接建议满仓
不替代回测
不直接计算全市场数据
不编造系统没有提供的数据
不直接决定股票是否入选
不参与第一版核心回测的可复现规则
```

---

### 16.2 个股解释 Prompt 模板

```text
你是一个A股选股系统的解释模块。

你只能根据输入数据进行解释，不能编造数据。
你不能预测具体目标价。
你不能使用“必涨、稳赚、确定性机会”等表达。
你不能建议满仓或无脑买入。

任务：
1. 总结该股票为什么入选；
2. 指出主要优势；
3. 指出主要风险；
4. 判断当前更适合“观察、等待回踩、分批建仓、回避”中的哪一种；
5. 给出后续需要观察的指标。

输入数据：
{stock_factor_json}

输出格式：
## 综合判断
## 入选理由
## 主要风险
## 操作倾向
## 后续观察指标
```

---

### 16.3 公告解析输出格式（v1.1 增强）

```json
{
  "event_type": "股东减持",
  "sentiment": "negative",
  "risk_level": "medium",
  "summary": "公司股东计划减持，短期可能影响市场情绪。",
  "need_remove_from_pool": true
}
```

---

### 16.4 LLM 禁用时的规则降级（v1.1 新增）

当 `llm.enable = false` 时，`suggestion` 和 `reason` 字段由规则引擎填充，确保系统在无 API 环境下仍可输出完整选股结果。

```python
def generate_suggestion(row) -> str:
    """
    基于中长线评分和短期涨幅的规则化建议。
    """
    total = row["total_score"]
    trend = row["trend_score"]
    ret_20d = row.get("ret_20d", 0)

    if total >= 80:
        if ret_20d > 0.30:
            return "观察，不追高"
        return "可作为中长线候选"
    elif total >= 65:
        if trend >= 50:
            return "观察，等待合适买点"
        return "继续观察基本面和趋势修复"
    elif total >= 50:
        return "暂不建议，关注基本面变化"
    else:
        return "回避"


def generate_reason(row) -> str:
    """
    基于因子阈值的模板化理由。
    """
    points = []

    if row["quality_score"] >= 70:
        points.append("盈利质量较好")
    elif row["quality_score"] < 40:
        points.append("盈利质量偏弱")

    if row["valuation_score"] >= 70:
        points.append("估值相对合理")
    elif row["valuation_score"] < 40:
        points.append("估值偏高")

    if row["growth_score"] >= 70:
        points.append("成长表现较稳定")
    elif row["growth_score"] < 40:
        points.append("成长表现有待改善")

    if row["industry_score"] >= 70:
        points.append("行业相对强度较高")

    if row["trend_score"] >= 70:
        points.append("中期趋势确认较强")
    elif row["trend_score"] < 40:
        points.append("趋势仍需修复")

    return "；".join(points) + "。"
```

---

## 17. 页面设计

第一版使用 Streamlit。

### 17.1 首页

展示：

```text
最新市场状态
最新强势行业
最新弱势行业
本期入选股票数量
本期风险股票数量
当前调仓周期
```

---

### 17.2 调仓股票池页面

字段：

```text
股票代码
股票名称
行业
综合评分
质量评分
成长评分
估值评分
行业评分
趋势评分
风险等级
建议
```

筛选条件：

```text
行业
综合评分区间
风险等级
是否适合观察
是否等待合适买点
调仓周期
```

---

### 17.3 个股详情页面

展示：

```text
K线图
均线
成交量
因子分数
估值分位
财务摘要
入选理由
风险提示
大模型解释
```

---

### 17.4 回测页面

展示：

```text
收益曲线
最大回撤
年化收益
胜率
盈亏比
换手率
沪深300对比
```

---

### 17.5 公告风险页面

展示：

```text
股票名称
公告标题
事件类型
情绪判断
风险等级
大模型摘要
是否剔除股票池
```

---

## 18. 配置文件设计

### 18.1 factor_weights.yaml

```yaml
quality_score: 0.30
growth_score: 0.25
valuation_score: 0.20
industry_score: 0.15
trend_score: 0.10
```

---

### 18.2 settings.yaml

```yaml
market:
  benchmark: "000300.SH"
  min_listed_days: 120
  min_amount_ma20: 100000000
  exclude_st: true

backtest:
  init_cash: 100000
  max_position_per_stock: 0.10
  commission: 0.0003
  stamp_tax: 0.0005
  slippage: 0.001
  rebalance: "monthly"      # monthly / quarterly
  execution: "next_open"    # T日信号，T+1执行
  holding_months: [3, 6, 12]

storage:
  parquet_backend: "minio"  # local / minio
  minio_bucket_raw: "stock-raw"
  minio_bucket_processed: "stock-processed"
  minio_bucket_backtest: "stock-backtest"

service:
  api_backend: "springboot"
  metadata_db: "postgresql"

llm:
  enable: true
  mode: "explain_only"
  allow_price_prediction: false
  allow_decision_making: false
```

---

## 19. 核心函数设计

### 19.1 更新数据

```python
def update_data(trade_date: str):
    fetch_stock_basic()
    fetch_daily_price(trade_date)
    fetch_adj_factor(trade_date)
    fetch_daily_basic(trade_date)
    fetch_financial_data(trade_date)
    save_to_parquet()
```

---

### 19.2 计算因子

```python
def calculate_factors(trade_date: str):
    price = load_price_until(trade_date)
    financial = load_financial_announced_until(trade_date)
    industry = load_industry_data_until(trade_date)

    trend = calc_trend_factors(price)
    liquidity = calc_liquidity_factors(price)
    valuation = calc_valuation_factors(price)
    quality = calc_quality_factors(financial)
    growth = calc_growth_factors(financial)
    industry_strength = calc_industry_factors(industry)

    factors = merge_all_factors(
        quality,
        growth,
        trend,
        liquidity,
        valuation,
        industry_strength
    )

    save_factors(trade_date, factors)
```

---

### 19.3 选股

```python
def select_stocks(trade_date: str, mode: str = "live", llm_enabled: bool = True):
    factors = load_factors(trade_date)

    # 风险过滤（回测模式下使用 ST 历史表，见 §12.2）
    factors = apply_risk_filter(factors, trade_date, mode=mode)

    # 综合评分
    factors["total_score"] = (
        factors["quality_score"] * 0.30
        + factors["growth_score"] * 0.25
        + factors["valuation_score"] * 0.20
        + factors["industry_score"] * 0.15
        + factors["trend_score"] * 0.10
    )

    # 风险等级（§12.3）
    factors["risk_level"] = factors.apply(calc_risk_level, axis=1)

    # 选股建议与理由
    if llm_enabled:
        factors["suggestion"] = factors.apply(call_llm_suggestion, axis=1)
        factors["reason"] = factors.apply(call_llm_reason, axis=1)
    else:
        # 规则降级（§16.4）
        factors["suggestion"] = factors.apply(generate_suggestion, axis=1)
        factors["reason"] = factors.apply(generate_reason, axis=1)

    selected = factors.sort_values("total_score", ascending=False).head(50)

    save_selection(trade_date, selected)

    return selected
```

---

### 19.4 回测

```python
def run_backtest(start_date: str, end_date: str):
    portfolio = init_portfolio()

    for date in get_rebalance_dates(start_date, end_date):
        # 回测必须使用历史状态过滤，不能使用 live 快照。
        selected = select_stocks(date, mode="backtest", llm_enabled=False)
        execution_date = get_next_trade_date(date)
        portfolio.rebalance(selected, execution_date)

    result = calc_backtest_metrics(portfolio)

    return result
```

---

## 20. 第一版开发顺序

### 阶段零：本地基础设施

目标：

```text
用 Docker Compose 启动 PostgreSQL 和 MinIO
创建数据 bucket 和元数据表
Python 程序可以读写 MinIO 和 PostgreSQL
容器、volume、bucket 均使用 stock-* 独立命名
```

验收标准：

```text
能写入一个测试 Parquet 到 MinIO
能写入一条 update_log 到 PostgreSQL
DuckDB 能读取本地或 MinIO 上的 Parquet
不会连接或写入既有 TSS 项目的 tss-postgres / minio-tss
```

---

### 阶段一：数据采集

目标：

```text
能下载指定交易日全A日线数据
能保存为 Parquet 到本地或 MinIO
能用 DuckDB 查询
```

验收标准：

```text
输入 trade_date
输出 daily_price.parquet
可以查询任意股票历史行情
PostgreSQL 记录更新状态
```

---

### 阶段二：数据清洗

目标：

```text
生成复权价格
剔除 ST、停牌、新股、低成交额股票
```

验收标准：

```text
任意股票可以画出连续复权价格曲线
```

---

### 阶段三：中长线因子计算

目标：

```text
计算质量、成长、估值、行业强度、趋势确认和流动性过滤因子
```

验收标准：

```text
每天每只股票都有一行 factor_daily
```

---

### 阶段四：股票评分

目标：

```text
每月或每季度生成综合评分前50股票池
```

验收标准：

```text
输出 selection_result.parquet
每只股票有分数、风险等级和入选理由
```

---

### 阶段五：中长线回测

目标：

```text
验证过去5-10年评分策略表现
验证3个月、6个月、12个月持有周期
```

验收标准：

```text
输出年化收益、最大回撤、胜率、收益曲线、沪深300/中证500/中证800对比
明确 T日信号、T+1成交
```

---

### 阶段六：Spring Boot API 与结果查询

目标：

```text
Spring Boot 查询 PostgreSQL 中的股票池摘要和回测摘要
从 MinIO 获取报告文件和图表文件
提供股票池、个股详情、回测报告 API
```

验收标准：

```text
接口可以查询指定调仓日的股票池
接口可以查询指定回测任务的指标摘要
接口可以返回 MinIO 中的报告文件地址
```

---

### 阶段七：大模型解释

目标：

```text
把结构化因子数据转成自然语言解释
```

验收标准：

```text
输入一只股票的因子JSON
输出入选理由、风险点、操作倾向
```

---

### 阶段八：页面展示

目标：

```text
本地打开页面查看股票池、个股详情、回测结果
```

验收标准：

```text
运行 streamlit run src/app/streamlit_app.py
可以看到完整页面
```

如果后续要做正式前端，可以让前端只调用 Spring Boot API，不直接读取 Parquet 或数据库。

---

### 阶段九：Docker Compose 一键部署

目标：

```text
一条命令启动 PostgreSQL、MinIO、Python worker、Spring Boot API、页面服务
```

验收标准：

```text
docker compose up 后可以完成一次数据更新、一次选股、一次回测结果查询
```

---

## 21. requirements.txt

```txt
pandas
polars
duckdb
pyarrow
numpy
tushare
akshare
baostock
vectorbt
plotly
streamlit
pydantic
python-dotenv
openai
loguru
tqdm
pyyaml
sqlalchemy
psycopg2-binary
minio
s3fs
```

---

## 22. 第一版完成标准

第一版完成时，系统至少要满足：

```text
1. 能自动更新指定交易日数据
2. 能保存并查询历史数据
3. 能计算至少15个中长线基础因子
4. 能生成月度或季度股票评分
5. 能输出前50股票池
6. 能解释每只股票为什么入选
7. 能跑过去5-10年回测
8. 回测没有明显未来函数
9. 能和沪深300、中证500、中证800做对比
10. 能验证3个月、6个月、12个月持有周期
11. 能把 Parquet、报告和图表写入 MinIO
12. 能把任务状态、股票池摘要和回测摘要写入 PostgreSQL
13. 能通过 Spring Boot 或 Streamlit 查询结果
14. 能用大模型生成解释和复盘报告
```

---

## 23. 后续第二版方向

第一版完成后，再考虑第二版：

```text
LightGBM 排名模型
公告和新闻RAG
行业景气度跟踪
个人持仓管理
策略参数自动优化
正式前端后台
权限和用户体系
组合持仓跟踪
定时任务自动运行
```

第二版可以引入机器学习，但预测目标不要设计成“明天涨不涨”。

更合理的目标：

```text
未来20个交易日是否跑赢沪深300
未来60个交易日是否跑赢行业指数
未来一段时间相对收益排名
```

---

## 24. 最终总结

第一版系统的核心不是大模型，也不是复杂算法，也不是复杂前后端。

核心是：

```text
干净的数据
不会作弊的回测
适合中长线的质量、成长、估值因子
保守的风险过滤
稳定的月度或季度输出
可复现的调仓和回测结果
```

大模型的作用是：

```text
解释选股结果
总结风险
生成调仓和回测复盘
解析公告（v1.1增强）
辅助复盘
```

第一版不要做成：

```text
AI自动预测股票系统
```

而应该做成：

```text
A股中长线多因子选股与回测系统
```

这是更适合股票新手和程序员的路线。

本地已有的 Spring Boot、PostgreSQL、Python、MinIO 对该系统有帮助，但分工必须清晰：

```text
Python 做计算
PostgreSQL 做元数据和结果索引
MinIO 做 Parquet 和报告存储
Spring Boot 做 API 和任务服务
Docker Compose 做一键部署
```

不要让 Spring Boot 承担量化计算，也不要把全量行情和因子明细全部写入 PostgreSQL。
