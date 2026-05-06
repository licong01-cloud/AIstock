# AIstock 统一非日频事件信号框架详细设计

日期：2026-05-06
状态：Phase 0 详细设计草案，仅设计，不改业务代码
工作树：`F:\Dev\AIstock_worktrees\unified-event-signal-design-20260506`

## 1. 背景与目标

AIstock 已经完成公告结构化元数据补齐、标题规则分类和公告风险信号雏形，但当前设计仍以公告为中心：

```text
market.anns
  -> market.ann_event_classification
  -> market.ann_risk_signal
```

用户进一步提出：公告只是非日频事件信号的一个来源。未来需要把公告、业绩预告、业绩快报、正式财务指标、新闻、互动问答、监管事件等统一到一套回测、模拟盘、实盘可复用的事件信号框架中，在原始 alpha 信号之外提供风险规避、预警和逐步验证后的 alpha 增强。

本设计的目标是先把公告和 Tushare 业绩相关数据源统一起来：

- 公告：沿用已补齐的 `market.anns` 和标题分类结果。
- Tushare `forecast` / `forecast_vip`：业绩预告。
- Tushare `express` / `express_vip`：业绩快报。
- Tushare `fina_indicator` / `fina_indicator_vip`：财务指标。
- LLM/PDF：本阶段不实现，只保留后续接入点。

最终消费者只读取统一表 `market.event_signal`，而不是直接扫描公告表或财务源表。

## 2. 设计原则

1. **同一引擎用于回测和实盘**：同一输入、同一规则版本、同一时间语义，必须得到同一信号。
2. **风险预警优先**：第一阶段只启用 `block_buy`、`warn_high`、`warn_review`、`record_only`，正向 alpha 增益默认禁用。
3. **公告不是日频因子**：公告和业绩数据是事件流；生产交易链路把它们作为风险/事件 overlay，不直接混入普通日频 alpha 因子。
4. **点时间可得性优先**：所有信号必须有 `available_at`、`effective_trade_date`、`time_mode`、`source_time_quality`。
5. **数据源和信号解耦**：原始源表负责保真，`event_fact` 负责标准化事实，`event_signal` 负责交易可消费信号。
6. **原始表不可污染**：Tushare raw 表只保存源字段、raw payload 和同步观测元数据，所有事件、风险、时间语义、信号和特征字段都写入衍生表。
7. **阶段性隔离**：当前阶段只生成和验证 `event_signal` 数据，不修改现有回测、Selection、Paper v2 或模拟盘程序。
8. **规则版本化**：任何分类、阈值、事件关联、信号动作都必须绑定 `rule_version`。
9. **先简单后增强**：MVP 只做确定性规则和结构化字段；LLM、PDF、OCR、新闻语义模型在下一阶段进入。
10. **DB 注释强制**：所有新增/修改表、字段必须有 `COMMENT ON TABLE` / `COMMENT ON COLUMN`，并用测试检查。
11. **可审计与可自愈**：数据集同步完成后写 `market.dataset_date_refresh_audit`；信号生成写 `market.event_signal_run`。
12. **不污染现有 alpha**：风险信号与 alpha 分数分层消费，正向信号进入 `alpha_hint` 或 `record_only`，需要事件研究验证后才启用。

## 2.1 当前阶段开发边界

当前阶段开发范围仅限“统一事件信号模块”和它直接需要的数据入库/派生能力。不得改动 QE 实验、现有回测、Selection Center、Paper v2、模拟盘或实盘执行链路。

允许修改或新增：

| 范围 | 说明 |
|---|---|
| `backend/services/event_signal/**` | 新建统一事件事实、关系、信号生成模块 |
| `backend/db/init_unified_event_signal_schema.py`、`backend/migrations/*event_signal*` | 新建本模块 schema 和 comment 完整性检查 |
| `scripts/create_tushare_*_raw_table.py`、`scripts/sync_tushare_*_raw.py` | 新建三个 Tushare raw 数据集的表初始化和同步脚本 |
| `backend/services/tushare_dataset_specs.py`、本地数据管理 ingestion/scheduler 相关配置 | 仅新增 raw 数据集同步、审计、调度入口，不触发交易消费 |
| `backend/services/announcements/**`、公告到 event_signal adapter | 仅作为本模块输入适配，不改变公告原始同步语义 |
| `tests/*event_signal*`、`tests/*tushare_*_raw*`、本地数据管理审计测试 | 本模块测试和 DB comment/字段隔离测试 |
| `docs/architecture/**`、`docs/analysis/**` | 本模块设计、验证、样本分析文档 |

禁止修改或接入：

| 禁止范围 | 当前阶段要求 |
|---|---|
| `backend/services/quantevolver/**`、QE 相关 router/script | 不修改 QE 实验提交、运行、指标、归档、风险策略 artifact |
| `backend/services/rdagent*`、`rdagent_assets/**` | 不修改 RD-Agent 实验链路 |
| `backend/services/selection_center/**` | 不接入 `event_signal` provider，不改变选股候选过滤 |
| `backend/services/paper_trading/**`、`backend/services/paper_trading_v2/**` | 不修改模拟盘/Paper v2 下单、readiness、day runner、session scheduler |
| `backend/services/strategy_package*`、`strategy_pkg` schema | 不改变 StrategyPackage manifest 或运行契约 |
| `backend/infra/qmt_client.py`、`backend/routers/qmt.py` | 不接入实盘/QMT 执行 |
| 任何生产交易调度 | 不让事件信号自动影响订单、组合、仓位或候选池 |

当前阶段产物边界：

```text
Tushare raw tables
  -> event_fact / event_relation / event_signal
  -> API/报告/样本审核/离线事件研究
  -> 不进入 QE、回测、Selection、Paper v2、模拟盘、实盘执行
```

如果后续要接入 QE、Selection、Paper v2 或模拟盘，必须另起 Phase 6 之后的新任务，先基于事件研究和样本审核确认，再单独设计、单独开关、单独测试。

## 3. 已有本地证据

### 3.1 公告数据与分类状态

来源文档/代码：

- `docs/architecture/announcement_event_risk_signal_top_level_design.md`
- `docs/analysis/announcement_backfill_classification_realtime_plan_20260505.md`
- `docs/analysis/announcement_title_classification_v0_20260505.md`
- `backend/services/announcements/title_classifier.py`
- `backend/migrations/announcement_event_signal_schema_20260505.sql`
- `backend/migrations/announcement_observation_time_fields_20260505.sql`

当前结果摘要：

| 项目 | 当前结论 |
|---|---|
| 公告结构化范围 | `2018-08-01` 至 `2026-04-30` |
| 公告结构化行数 | `5,131,329` |
| 来源组合 | Eastmoney 主历史补齐，cninfo 补官方 URL/校验，Tushare `anns_d` 当前本地 token 权限不足 |
| rec_time 非空 | `4,930,306` |
| rec_time 为空 | `201,023`，回测默认下一交易日生效 |
| rec_time = 00:00:00 | `36,253`，回测默认下一交易日生效，除非多源验证是真实午夜披露 |
| 标题规则版本 | `aistock_announcement_title_rules_v0_20260505` |
| P0/P1/P2 风险行数 | `25,451` / `89,648` / `1,240,414` |
| P3 正向候选 | `398,387`，阶段 1 不做 alpha 增益 |
| P4 中性归档 | `3,377,429` |

已有公告时间字段：

- `market.anns.first_seen_at` / `last_seen_at`
- `market.anns.first_seen_source` / `last_seen_source`
- `market.anns.first_seen_job_id` / `last_seen_job_id`
- `market.anns.observed_time_quality`
- `market.ann_event_classification.available_at` / `time_mode`
- `market.ann_risk_signal.available_at` / `time_mode`

已有关键修复：公告分类唯一键已包含 `(ann_id, rule_version, time_mode)`，避免 live/paper 行覆盖 backtest 行。

### 3.2 本地调度与数据集审计

来源文档/代码：

- `docs/analysis/paper_v2_dataset_refresh_audit_handoff_20260505.md`
- `backend/migrations/dataset_refresh_audit_enhancement_20260505.sql`
- `backend/services/data_refresh_audit.py`
- `backend/services/audit_backed_data_health.py`
- `backend/ingestion/tdx_scheduler.py`
- `backend/services/tushare_sync_engine.py`

设计影响：

- Tushare 业绩数据源必须进入本地数据管理调度与 `market.dataset_date_refresh_audit`。
- 对 `forecast` / `express` / `fina_indicator` 这类非每日全量但按公告日或报告期更新的数据集，成功空结果也可能是有效刷新，需要审计表支持 sparse event dataset。
- 实盘/模拟盘不应每次扫描全表判断最新，而应读取审计表、信号生成 run 表和最终 `event_signal` 的最新覆盖状态。

### 3.3 现有交易风险策略接入口


本节只作为未来接入参考。当前阶段不得修改 `selection_center`、`paper_trading_v2`、QE 或模拟盘代码，也不得启用任何事件信号消费。
来源代码：

- `backend/services/selection_center/risk_policy.py`
- `backend/services/selection_center/runtime_profile.py`
- `scripts/qe_event_risk_policy.py`

现状：

- 已有统一 `RiskDecision` 合约，当前实现 `st_pit`，预留 `announcement_risk` 但尚未实现。
- 这正好可以扩展为 `event_signal` provider：Selection Center、Paper v2、未来 QE/实盘都通过同一个风险决策接口消费事件信号。

## 4. 外部参考与可借鉴点

### 4.1 Tushare 接口事实

参考 Tushare 官方文档与本地 Tushare skill references：

| 接口 | 权限/限制 | 适合用途 | 设计影响 |
|---|---|---|---|
| `anns_d` | 单独权限；单次最大 2000；字段含 `ann_date/ts_code/name/title/url/rec_time` | 全量公告元数据和 PDF URL | 目前本地 token 无权限，不能阻塞框架；继续保留源适配器 |
| `forecast` | 至少 2000 积分；当前普通接口主要按单只股票历史获取；`forecast_vip` 可按季度全市场，需 5000 积分 | 业绩预告、预增/预减/扭亏/首亏/续亏等 | 有 `summary/change_reason`，但阶段 1 先用结构化数值与类型 |
| `express` | 至少 2000 积分；普通接口按单只股票；`express_vip` 可按季度全市场，需 5000 积分 | 业绩快报、初步实际结果 | 有收入、净利润、ROE、同比、是否审计 |
| `fina_indicator` | 至少 2000 积分；普通接口单次最多 100 条；普通接口按单只股票；`fina_indicator_vip` 可按季度全市场，需 5000 积分 | 正式财务指标和质量指标 | 字段很多；MVP 需要明确核心字段，后续可扩宽 |

用户当前已有 10000 积分，因此 `forecast_vip` / `express_vip` / `fina_indicator_vip` 理论上应作为全市场历史补齐首选；但是否已经开通 VIP 接口仍需用短区间 smoke 验证。若 VIP 不可用，则退回 `BY_CODE` 普通接口历史补齐。

### 4.2 学术与机构实践

| 参考 | 可明确借鉴 | 不适合直接照搬 |
|---|---|---|
| Ball & Brown 1968 事件研究基础 | 用公告日附近异常收益验证信号有效性 | 不能直接假设所有公告都有方向性 alpha |
| Brown & Warner 1985 日频事件研究 | 用日频收益窗口、异常收益、显著性检验 | 不能忽略 A 股涨跌停、停牌、T+1、交易成本 |
| PEAD / SUE / revenue surprise 文献 | 业绩公告后漂移、盈利 surprise、收入 surprise 可作为研究模板 | 不能未经 A 股本地验证就启用正向增益 |
| Jegadeesh & Livnat revenue surprises | 财报信号不只看利润，也看收入与利润是否背离 | 阈值和持有期不能照搬美股 |
| Loughran-McDonald 金融文本词典 | 金融文本不能用通用情感词；后续 LLM/PDF 可用金融词表校验 | 第一阶段不做全文情感信号 |
| Feng Li 年报可读性 | 定期报告正文可用于风险早预警 | 当前先用结构化财务指标，不把普通年报标题送 LLM |
| 中国公告细粒度分类论文 | 触发词、共现词、事件模板、54 类事件可作为 taxonomy seed | 论文基于 Eastmoney 中国股市公告新闻，不等于 AIstock 全市场交易信号分类；收益结论必须本地重测 |
| RavenPack/Bloomberg/FactSet 事件数据实践 | 事件 taxonomy、relevance、novelty、source time、status、point-in-time 版本化 | 不依赖其商业数据；只借鉴架构思想 |
| ESG controversy 实践 | 严重负面事件是状态，可能持续多日，不是一日标签 | ESG 专题不进入第一阶段 |

中国公告 54 类论文中的事件类型可作为参考种子：

```text
垃圾焚烧、增资扩股、业绩预告、责令改正、权益分派、股票解禁、到期失效、不确定性、届满、可转换债券、补助、犯罪、辞职、一致性评价、侦查、违纪、行政处罚、拨付款、投产、拘留、盈利、预增、改制、减值、减持、建成、清仓、吞吐量、预中标、转增股、中标、吸收合并、扩建、诉讼、发起设立、投建、罢免、药品临床、筹划、并购、转让、净利、补贴、收购、增持、质押、罚款、违法、冻结、签署签订、回购、出售、设立公司、股票激励。
```

AIstock 不应照搬这 54 类，而应按本地 `market.anns` 标题分布、风险优先级和交易可执行性重新组织。现有 v0 规则已经更适合交易风险，例如补充了 `ST/退市`、`资金占用/违规担保`、`非标审计/内控缺陷`、`债务违约` 等硬风险。

## 5. 总体架构

目标架构：

```text
Raw source tables
  market.anns
  market.tushare_forecast_raw
  market.tushare_express_raw
  market.tushare_fina_indicator_raw
  future: news, irm_qa, research_report, regulatory, ESG, media sentiment

Source adapters
  AnnouncementFactAdapter
  ForecastFactAdapter
  ExpressFactAdapter
  FinaIndicatorFactAdapter

Standard facts
  market.event_fact
  market.event_relation

Signal engine
  EventSignalEngine
  market.event_signal_rule_set
  market.event_signal_run
  market.event_signal

Future consumers (deferred; not implemented in current phase)
  Backtest risk overlay
  Selection Center risk policy
  Paper v2 risk policy
  Future live warning center
  UI event dashboard
  Event-study validation
  Future LLM/PDF extraction queue
```

核心约束：

```text
策略、回测、模拟盘、实盘不得直接扫 source-specific 表。
所有交易可消费结论只从 market.event_signal 读取。
```

## 6. 数据层分工

### 6.1 Raw Source Layer

Raw 层只负责保真保存源接口返回、入库观测时间和同步审计信息，不直接表达事件、风险、交易动作或 alpha 含义。

本设计已确认：3 个 Tushare 原始数据集使用 3 张独立 raw 表，不合并成一张通用表。原因是 `forecast`、`express`、`fina_indicator` 的字段规模、更新节奏、接口延迟和权限/限流情况不同，独立表更便于日常调度、失败隔离、补数和审计。

公告：沿用 `market.anns`，公告自身已有独立结构化表和观测时间字段。

新增 Tushare 原始表建议：

| raw 表 | 对应源接口 | 同步模式 | 设计要求 |
|---|---|---|---|
| `market.tushare_forecast_raw` | `forecast_vip` / `forecast` | 优先按报告期全市场 VIP；失败退回按股票代码普通接口 | 只保存业绩预告源数据、raw payload、观测元数据和同步任务信息 |
| `market.tushare_express_raw` | `express_vip` / `express` | 优先按报告期全市场 VIP；失败退回按股票代码普通接口 | 只保存业绩快报源数据、raw payload、观测元数据和同步任务信息 |
| `market.tushare_fina_indicator_raw` | `fina_indicator_vip` / `fina_indicator` | 优先按报告期全市场 VIP；失败退回按股票代码普通接口 | 只保存财务指标源数据、raw payload、观测元数据和同步任务信息 |

每张 raw 表必须有独立的：

- `DatasetSpec` / 同步配置。
- `market.dataset_date_refresh_audit` 记录。
- 调度节奏和重试策略。
- 权限失败、限流失败、空结果有效性判断。
- 源数据修订检测。

raw 表只允许保存以下类型字段：

| 字段类型 | 示例字段 | 说明 |
|---|---|---|
| 本地主键 | `raw_observation_id` | AIstock 本地 raw 观测行主键 |
| 源接口标识 | `source_api`、`fetch_params` | 记录本行来自 `forecast_vip` 还是普通接口，以及调用参数 |
| 源业务键 | `source_record_key`、`ts_code`、`ann_date`、`report_period` | 用于去重、增量、查询和审计；`report_period` 对应 Tushare `end_date` |
| 源内容版本 | `source_row_hash`、`raw_payload` | `raw_payload` 保存 Tushare 返回的完整原始 JSON；hash 用于发现上游修订 |
| 本地观测时间 | `first_seen_at`、`last_seen_at`、`observed_at` | 记录 AIstock 何时第一次/最近一次看到该源记录 |
| 同步任务信息 | `first_seen_job_id`、`last_seen_job_id`、`job_id` | 关联 ingestion job 或调度任务 |
| 系统时间 | `created_at`、`updated_at` | 数据库写入/更新时间 |

raw 表禁止保存以下字段：

```text
事件类型: event_type, event_family, event_status
交易时间语义: effective_trade_date, available_at, time_mode
风险/动作: risk_level, action, signal_type, severity_score, confidence
alpha: alpha_score, alpha_boost, alpha_penalty
跨源衍生: forecast_mid, actual_yoy, miss_vs_mid, relation_type
LLM/PDF 结论: needs_llm, llm_summary, extracted_amount, impact_conclusion
```

raw 表版本策略：

- 幂等键建议使用 `(source_record_key, source_row_hash)`。
- 同一源业务键、同一 hash 重复同步时，只更新 `last_seen_at/last_seen_job_id/updated_at` 等观测元数据，不新增重复 raw payload。
- 同一源业务键但 hash 变化时，插入新 raw 版本，不覆盖旧 raw payload。
- 信号引擎只基于选定 time_mode 下可见的最新 raw 版本生成衍生表。
- raw 表可以被重建，但重建不应影响已经按 `rule_version/time_mode` 固化的衍生信号；需要重算时显式跑 signal run。

`fina_indicator` 字段特别处理：

- 第一阶段不把 Tushare `fina_indicator` 的 100+ 字段全部做成 raw 表 typed 宽列。
- raw 表保留最小索引字段 + `raw_payload`，避免后续字段变化频繁迁移 raw 表。
- 信号需要的核心指标由 adapter 从 `raw_payload` 解析后写入 `market.event_fact.facts` 或后续专门的衍生解析表。
- 如果后续模型训练需要更多财务字段，从 `raw_payload` 提升到版本化 feature snapshot 或专门衍生表，不修改 raw 表。

所有衍生字段必须进入新表：

```text
market.event_fact
market.event_relation
market.event_signal
market.event_signal_run
future: market.financial_indicator_feature_snapshot, market.event_review_queue, market.event_llm_extract
```

### 6.2 Event Fact Layer

`event_fact` 是标准化事实层，不表达交易动作。一个原始记录可以产生一个或多个 fact。

例子：

- 一条公告标题 `关于收到行政处罚决定书的公告` -> `event_type=regulatory_penalty`。
- 一条业绩预告 `type=预增, p_change_min=100, p_change_max=150` -> `event_type=financial_forecast_large_growth`。
- 一条正式财务指标 `dt_netprofit_yoy=50` -> `event_type=financial_actual_growth`。
- 预告 + 正式指标关系显示 `actual_yoy=50` 低于 forecast_mid=125 -> 关系事实 `financial_positive_but_miss_expectation`。

### 6.3 Event Relation Layer

`event_relation` 负责跨源关系，不直接发交易动作。

核心关系：

| relation_type | 左侧 | 右侧 | 用途 |
|---|---|---|---|
| `same_report_period` | forecast | express/fina_indicator | 同公司同报告期事实归并 |
| `formalizes_forecast` | forecast | fina_indicator | 正式数据兑现预告 |
| `formalizes_express` | express | fina_indicator | 正式财务指标兑现快报 |
| `announcement_metadata_of_fact` | announcement | forecast/express/fina_indicator | 公告标题和结构化源互证 |
| `revises_prior_forecast` | forecast | forecast | 同一报告期多次预告修正 |
| `misses_prior_expectation` | forecast | express/fina_indicator | 预告高增长但正式增长不及预期 |
| `beats_prior_expectation` | forecast | express/fina_indicator | 正式结果超预告区间 |

### 6.4 Event Signal Layer

`event_signal` 是唯一给交易系统消费的表。

它可以来自单个 fact，也可以来自 fact + relation，例如：

```text
forecast: 净利润预增 100%-150%
fina_indicator: 扣非净利润同比 +50%
=> event_signal: financial_positive_but_miss_expectation, direction=mixed_to_negative, action=warn_review
```

## 7. 统一时间语义

### 7.1 时间字段

所有 fact/signal 都必须有：

| 字段 | 语义 |
|---|---|
| `source_event_date` | 源事件自然日期，如公告日、披露日 |
| `report_period` | 报告期，如 `20261231`，非财报事件可空 |
| `available_at` | AIstock 判定本事件可得的时间戳 |
| `effective_trade_date` | 第一个允许交易系统消费的交易日 |
| `time_mode` | `backtest` / `paper` / `live` / `observed` |
| `source_time_quality` | `EXACT` / `MIDNIGHT_DEFAULT` / `MISSING` / `DATE_ONLY` / `LOCAL_FIRST_SEEN` / `BACKFILL_UNKNOWN` |

### 7.2 回测模式规则

```text
EXACT 且盘前可得：当日生效
EXACT 且非盘前：下一交易日生效
00:00:00：默认下一交易日生效，除非多源验证是真实时间
空值/日期-only：下一交易日生效
Tushare forecast/express/fina_indicator：只有 ann_date，无精确发布时间，默认下一交易日生效
```

建议保持现有公告分类器的 `pre_open_cutoff=09:25`。是否改为 `09:30` 需要用户确认；建议沿用 `09:25`，与集合竞价前风险控制一致。

### 7.3 Paper/Live 模式规则

```text
若源记录有 first_seen_at：以 first_seen_at 作为 available_at
first_seen_at <= 09:25 且当日为交易日：当日生效
first_seen_at > 09:25：下一交易日生效
没有 first_seen_at：退回日期-only保守规则，下一交易日生效
```

这解决了用户提出的场景：如果实盘早上 07:00 本地同步到了公告或结构化财务事件，`first_seen_at=07:00`，则可以当天生效；历史回测不能伪造该时间，只能用保守下一交易日。

## 8. 核心表设计草案

以下是逻辑字段设计，不是最终 SQL。实施时必须补全所有 `COMMENT ON TABLE/COLUMN` 并加入注释完整性测试。

### 8.1 `market.event_fact`

用途：标准化事件事实层，一个源记录可对应多条 fact。

| 字段 | 类型建议 | comment 草案 |
|---|---|---|
| `event_id` | `BIGSERIAL PRIMARY KEY` | Local surrogate primary key for one standardized event fact. |
| `event_key` | `TEXT UNIQUE NOT NULL` | Stable idempotency key built from source_type, source_pk, event_type, report_period, time_mode, and rule_version. |
| `ts_code` | `TEXT NOT NULL` | A-share security code affected by this event fact. |
| `source_type` | `TEXT NOT NULL` | Source family: announcement, tushare_forecast, tushare_express, tushare_fina_indicator, or future source. |
| `source_table` | `TEXT NOT NULL` | Physical source table name, for example market.anns or market.tushare_forecast_raw. |
| `source_pk` | `TEXT NOT NULL` | Source table primary-key value serialized as text for traceability. |
| `event_family` | `TEXT NOT NULL` | Broad event family such as announcement_risk, financial_forecast, financial_actual, governance, legal, or operation. |
| `event_type` | `TEXT NOT NULL` | Stable event type generated by source adapter, for example financial_forecast_large_growth. |
| `event_status` | `TEXT NOT NULL DEFAULT 'new'` | Event lifecycle state: new, progress, revised, resolved, cancelled, or unknown. |
| `source_event_date` | `DATE NOT NULL` | Natural source event date such as announcement date; may be non-trading day. |
| `report_period` | `DATE` | Financial report period end date when applicable. |
| `available_at` | `TIMESTAMPTZ` | Timestamp when the event fact is available under the selected time_mode. |
| `effective_trade_date` | `DATE NOT NULL` | First trading date when downstream consumers may use this fact. |
| `time_mode` | `TEXT NOT NULL` | Availability mode: backtest, paper, live, or observed. |
| `source_time_quality` | `TEXT NOT NULL` | Quality of source/observed time used to derive available_at and effective_trade_date. |
| `rule_version` | `TEXT NOT NULL` | Adapter/rule version that generated this fact. |
| `facts` | `JSONB NOT NULL DEFAULT '{}'` | Normalized source facts, numeric metrics, title evidence, and quality flags used by signal rules. |
| `quality_flags` | `JSONB NOT NULL DEFAULT '[]'` | Data quality flags such as missing_actual_yoy, date_only_time, duplicate_source, or estimated_metric. |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Database timestamp when this event fact row was first inserted. |
| `updated_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Database timestamp when this event fact row was last updated. |

建议索引：

```text
UNIQUE(event_key)
(ts_code, effective_trade_date, event_type)
(source_type, source_pk)
(report_period, ts_code, event_family)
(time_mode, effective_trade_date)
```

### 8.2 `market.event_relation`

用途：跨源/跨版本事件关系。

| 字段 | 类型建议 | comment 草案 |
|---|---|---|
| `relation_id` | `BIGSERIAL PRIMARY KEY` | Local surrogate primary key for one relationship between event facts. |
| `relation_key` | `TEXT UNIQUE NOT NULL` | Stable idempotency key for the relationship. |
| `relation_type` | `TEXT NOT NULL` | Relationship type such as formalizes_forecast, misses_prior_expectation, or same_report_period. |
| `left_event_id` | `BIGINT NOT NULL` | Left event fact id. |
| `right_event_id` | `BIGINT NOT NULL` | Right event fact id. |
| `ts_code` | `TEXT NOT NULL` | A-share security code shared by both related facts. |
| `report_period` | `DATE` | Financial report period end date for relation matching. |
| `relation_score` | `NUMERIC(6,4) NOT NULL DEFAULT 1` | Confidence score in [0,1] for the relation match. |
| `rule_version` | `TEXT NOT NULL` | Relation-builder rule version. |
| `evidence` | `JSONB NOT NULL DEFAULT '{}'` | Trace data explaining why the two facts were linked. |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Database timestamp when this relation row was inserted. |

建议索引：

```text
UNIQUE(relation_key)
(ts_code, report_period, relation_type)
(left_event_id)
(right_event_id)
```

### 8.3 `market.event_signal`

用途：统一交易可消费信号表。

| 字段 | 类型建议 | comment 草案 |
|---|---|---|
| `signal_id` | `BIGSERIAL PRIMARY KEY` | Local surrogate primary key for one event-derived trading overlay signal. |
| `signal_key` | `TEXT UNIQUE NOT NULL` | Stable idempotency key built from source facts, event_type, time_mode, and rule_version. |
| `ts_code` | `TEXT NOT NULL` | A-share security code affected by this signal. |
| `source_type` | `TEXT NOT NULL` | Dominant source family that generated this signal; multi_source for relation-derived signals. |
| `source_table` | `TEXT` | Dominant physical source table, nullable for multi-source relation signals. |
| `source_pk` | `TEXT` | Dominant source primary key, nullable for multi-source relation signals. |
| `event_family` | `TEXT NOT NULL` | Broad event family used for grouping and UI filters. |
| `event_type` | `TEXT NOT NULL` | Stable signal event type such as delisting_or_risk_warning or financial_result_below_forecast_range. |
| `signal_type` | `TEXT NOT NULL` | Signal class: risk_block, risk_warning, review_required, alpha_hint, record_only, or neutral_archive. |
| `direction` | `TEXT NOT NULL` | Direction of expected impact: negative, positive, mixed, neutral, or unknown. |
| `risk_level` | `TEXT NOT NULL` | Risk level: P0_BLOCK, P1_HIGH, P2_REVIEW, P3_POSITIVE_CANDIDATE, or P4_NEUTRAL. |
| `action` | `TEXT NOT NULL` | Consumer action such as block_buy, warn_high, warn_review, record_only, alpha_hint_disabled, or discard_or_archive. |
| `severity_score` | `NUMERIC(6,4) NOT NULL` | Normalized risk severity in [0,1]. |
| `alpha_score` | `NUMERIC(8,4) NOT NULL DEFAULT 0` | Research-only alpha hint score; production boost is disabled until event-study validation. |
| `confidence` | `NUMERIC(6,4) NOT NULL` | Confidence in [0,1] for the signal conclusion. |
| `source_event_date` | `DATE NOT NULL` | Natural event date copied from the fact or dominant source. |
| `report_period` | `DATE` | Financial report period end date when applicable. |
| `available_at` | `TIMESTAMPTZ` | Timestamp when this signal is available under time_mode. |
| `effective_trade_date` | `DATE NOT NULL` | First trading date when this signal can be consumed. |
| `expiry_trade_date` | `DATE` | Last trading date when this signal remains active; NULL means engine default. |
| `time_mode` | `TEXT NOT NULL` | Availability mode: backtest, paper, live, or observed. |
| `source_time_quality` | `TEXT NOT NULL` | Quality of time source used for available_at/effective_trade_date. |
| `horizon_days` | `INTEGER NOT NULL DEFAULT 20` | Intended maximum signal horizon in trading days before decay/expiry. |
| `decay_policy` | `TEXT NOT NULL DEFAULT 'step_until_expiry'` | Signal decay rule such as step_until_expiry, linear_decay, or one_day_only. |
| `status` | `TEXT NOT NULL DEFAULT 'ACTIVE'` | Signal lifecycle status: ACTIVE, EXPIRED, SUPERSEDED, RESOLVED, or SUPPRESSED. |
| `rule_version` | `TEXT NOT NULL` | Signal rule version that produced this row. |
| `source_event_ids` | `BIGINT[] NOT NULL` | Event fact ids that supported this signal. |
| `relation_ids` | `BIGINT[] NOT NULL DEFAULT '{}'` | Event relation ids used by this signal when applicable. |
| `evidence` | `JSONB NOT NULL DEFAULT '{}'` | Human/audit evidence including title, metrics, thresholds, relation comparisons, and quality flags. |
| `generated_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Timestamp when this signal was generated. |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Database timestamp when this signal row was first inserted. |
| `updated_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Database timestamp when this signal row was last updated. |

建议索引：

```text
UNIQUE(signal_key)
(ts_code, time_mode, effective_trade_date, risk_level, action)
(time_mode, effective_trade_date, risk_level, status)
(event_type, effective_trade_date)
(source_type, source_event_date)
GIN(evidence)
```

### 8.4 `market.event_signal_rule_set`

用途：版本化规则配置。

| 字段 | 类型建议 | comment 草案 |
|---|---|---|
| `rule_version` | `TEXT PRIMARY KEY` | Stable version id for one event signal rule set. |
| `engine_name` | `TEXT NOT NULL` | Engine implementation name, for example EventSignalEngine. |
| `source_types` | `TEXT[] NOT NULL` | Source types covered by this rule set. |
| `config_hash` | `TEXT NOT NULL` | SHA256 hash of normalized rule config for reproducibility. |
| `config` | `JSONB NOT NULL` | Serialized deterministic rule configuration and thresholds. |
| `is_active` | `BOOLEAN NOT NULL DEFAULT TRUE` | Whether this version is active for new paper/live generation. |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Database timestamp when this rule set was inserted. |
| `updated_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Database timestamp when this rule set was last updated. |

### 8.5 `market.event_signal_run`

用途：信号生成审计。

| 字段 | 类型建议 | comment 草案 |
|---|---|---|
| `run_id` | `UUID PRIMARY KEY` | Unique id for one event fact/signal generation run. |
| `rule_version` | `TEXT NOT NULL` | Rule version used by the run. |
| `time_mode` | `TEXT NOT NULL` | Availability mode used by the run. |
| `source_types` | `TEXT[] NOT NULL` | Source types included in the run. |
| `start_date` | `DATE NOT NULL` | Inclusive source event date lower bound. |
| `end_date` | `DATE NOT NULL` | Inclusive source event date upper bound. |
| `input_rows` | `BIGINT NOT NULL DEFAULT 0` | Number of raw input rows scanned. |
| `fact_rows` | `BIGINT NOT NULL DEFAULT 0` | Number of event facts inserted or updated. |
| `relation_rows` | `BIGINT NOT NULL DEFAULT 0` | Number of event relations inserted or updated. |
| `signal_rows` | `BIGINT NOT NULL DEFAULT 0` | Number of event signals inserted or updated. |
| `status` | `TEXT NOT NULL` | Run status: running, success, failed, partial, or skipped. |
| `error_message` | `TEXT` | Failure or partial-run diagnostic message. |
| `metadata` | `JSONB NOT NULL DEFAULT '{}'` | Extended audit context such as dataset audit coverage, rule hash, and source row counts. |
| `started_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Timestamp when the run started. |
| `finished_at` | `TIMESTAMPTZ` | Timestamp when the run finished. |

## 9. Tushare 业绩源字段映射

### 9.1 `forecast` / `forecast_vip`

核心字段：

| 字段 | 用途 |
|---|---|
| `ts_code` | 股票代码 |
| `ann_date` | 公告日期，日期-only 时间语义 |
| `end_date` | 报告期 |
| `type` | 预告类型：预增/预减/扭亏/首亏/续亏/续盈/略增/略减 |
| `p_change_min` / `p_change_max` | 净利润同比变动区间，核心 signal 字段 |
| `net_profit_min` / `net_profit_max` | 预告净利润区间，单位万元 |
| `last_parent_net` | 上年同期归母净利润 |
| `first_ann_date` | 首次公告日，可用于识别修正/重复预告 |
| `summary` | 摘要，阶段 1 只入库不 LLM |
| `change_reason` | 变动原因，阶段 1 只入库不 LLM |

初始事实类型：

| 条件 | event_type | 初始风险/动作 |
|---|---|---|
| `type in 首亏/续亏` | `financial_forecast_loss` | P1/P2，`warn_high` 或 `warn_review` |
| `type=预减` 或 `p_change_max <= -50` | `financial_forecast_large_decline` | P1/P2，`warn_review` |
| `p_change_min >= 50` | `financial_forecast_large_growth` | P3，`record_only` |
| `type=扭亏` | `financial_forecast_turnaround` | P3，`record_only`，待验证 |
| 区间过宽或上下限跨 0 | `financial_forecast_uncertain_range` | P2，`warn_review` |
| 多次预告下修 | `financial_forecast_downward_revision` | P1/P2，`warn_high`/`warn_review` |

### 9.2 `express` / `express_vip`

核心字段：

| 字段 | 用途 |
|---|---|
| `ts_code` / `ann_date` / `end_date` | 标的、披露日、报告期 |
| `revenue` / `n_income` | 收入、净利润，单位元 |
| `diluted_eps` / `diluted_roe` | EPS、ROE |
| `yoy_sales` | 营收同比 |
| `yoy_dedu_np` | 归母净利润同比，核心 actual_yoy 候选 |
| `yoy_eps` / `yoy_roe` | 每股收益和 ROE 同比/增减 |
| `perf_summary` | 业绩简要说明，阶段 1 只入库不 LLM |
| `is_audit` | 是否审计，快报多为未审计；用于质量提示 |

初始事实类型：

| 条件 | event_type | 初始风险/动作 |
|---|---|---|
| `yoy_dedu_np <= -50` | `financial_express_large_decline` | P1/P2，`warn_review` |
| `yoy_dedu_np >= 50` | `financial_express_large_growth` | P3，`record_only` |
| `yoy_sales > 20` 且 `yoy_dedu_np < 0` | `financial_revenue_profit_divergence` | P2，`warn_review` |
| `yoy_dedu_np > 30` 但 `yoy_sales < 0` | `financial_profit_quality_risk` | P2，`warn_review` |
| `is_audit = 0` | `financial_express_unaudited` | P4/P3，质量标记，不单独预警 |

### 9.3 `fina_indicator` / `fina_indicator_vip`

第一阶段核心字段：

| 字段 | 用途 |
|---|---|
| `ts_code` / `ann_date` / `end_date` | 标的、披露日、报告期 |
| `eps` / `dt_eps` / `q_eps` | EPS 和单季 EPS |
| `profit_dedt` / `q_dtprofit` | 扣非净利润和单季扣非净利润 |
| `gross_margin` / `grossprofit_margin` / `q_gsprofit_margin` | 毛利/毛利率 |
| `roe` / `roe_dt` / `q_roe` / `q_dt_roe` | ROE 与扣非 ROE |
| `ocfps` / `ocf_to_profit` / `q_ocf_to_sales` | 现金流质量 |
| `debt_to_assets` / `current_ratio` / `quick_ratio` | 负债和偿债压力 |
| `netprofit_yoy` / `dt_netprofit_yoy` / `q_netprofit_yoy` | 归母/扣非/单季净利润同比 |
| `tr_yoy` / `or_yoy` / `q_sales_yoy` | 营收同比 |
| `ocf_yoy` | 经营现金流同比 |
| `impai_ttm` / `q_impair_to_gr_ttm` | 减值压力 |
| `rd_exp` | 研发费用，后续行业化使用 |
| `update_flag` | Tushare 更新标识 |

初始事实类型：

| 条件 | event_type | 初始风险/动作 |
|---|---|---|
| `dt_netprofit_yoy <= -50` 或 `q_netprofit_yoy <= -50` | `financial_actual_large_decline` | P1/P2，`warn_high`/`warn_review` |
| `dt_netprofit_yoy >= 50` 且现金流质量不差 | `financial_actual_large_growth` | P3，`record_only` |
| `or_yoy > 20` 且 `dt_netprofit_yoy < 0` | `financial_revenue_profit_divergence` | P2，`warn_review` |
| `dt_netprofit_yoy > 30` 且 `ocf_yoy < -30` | `financial_cashflow_profit_divergence` | P2，`warn_review` |
| `grossprofit_margin` 同比/环比显著恶化 | `financial_margin_deterioration` | P2，`warn_review` |
| `roe_dt` 明显下降 | `financial_roe_deterioration` | P2，`warn_review` |
| `debt_to_assets >= 80` 且现金流弱 | `financial_debt_pressure` | P1/P2，`warn_review` |
| `impai_ttm` 或公告标题命中减值 | `financial_impairment_pressure` | P2，`warn_review` |

## 10. 业绩预期兑现与不及预期规则

用户提出的关键例子：

```text
预告盈利增长 100%
正式财报只增长 50%
```

这不能简单判定为利好。框架应输出：

```text
absolute result: positive growth
relative-to-expectation: miss expectation
final signal: mixed_to_negative / warn_review
```

建议计算：

```text
forecast_mid = mean(p_change_min, p_change_max)
forecast_low = p_change_min
forecast_high = p_change_max
actual_yoy = prefer express.yoy_dedu_np
             else fina_indicator.dt_netprofit_yoy
             else fina_indicator.netprofit_yoy
             else fina_indicator.q_netprofit_yoy
miss_vs_mid = actual_yoy - forecast_mid
```

初始规则：

| 条件 | event_type | 风险动作 |
|---|---|---|
| `actual_yoy < forecast_low - 10` | `financial_result_below_forecast_range` | P1/P2，`warn_high`/`warn_review` |
| `actual_yoy < forecast_mid - 30` | `financial_result_miss_forecast_mid` | P2，`warn_review` |
| `actual_yoy > forecast_high + 10` | `financial_result_beat_forecast_range` | P3，`record_only` |
| `actual_yoy > 0` 且 `actual_yoy < forecast_mid - 30` | `financial_positive_but_miss_expectation` | P2，`warn_review` |
| forecast `type=扭亏` 但 actual_yoy/利润仍亏 | `financial_turnaround_failed` | P1/P2，`warn_high` |
| forecast 首亏/续亏且 actual 更差 | `financial_loss_worse_than_forecast` | P1，`warn_high` |

这些规则第一阶段只产生风险预警和记录，不做正向增益。

## 11. 公告分类与统一事件 taxonomy 的关系

现有公告 v0 分类可以直接映射到 `event_signal`：

| 现有 event_type | 统一 event_family | 统一处理 |
|---|---|---|
| `delisting_or_risk_warning` | `listing_status_risk` | P0 `block_buy`，无需 PDF/LLM |
| `bankruptcy_restructuring` | `solvency_risk` | P0 `block_buy`，无需 PDF/LLM |
| `regulatory_investigation_penalty` | `regulatory_risk` | P1 `warn_high`，解释可选 |
| `debt_default_overdue` | `solvency_risk` | P1 `warn_high` |
| `audit_opinion_internal_control_risk` | `financial_reporting_risk` | P1 `warn_high` |
| `capital_occupation_illegal_guarantee` | `governance_risk` | P1 `warn_high` |
| `performance_forecast_revision_impairment` | `financial_forecast_or_impairment` | P2 `warn_review`，可与 Tushare 结构化数据联动 |
| `periodic_report_neutral` | `routine_disclosure` | P4 归档；由结构化财务数据产生风险/质量信号 |
| `positive_contract_order_project` | `operation_positive_candidate` | P3 记录，事件研究后再考虑增益 |

普通年报、季报标题仍不直接送 LLM，因为标题没有足够方向信息；财报风险应先由 `fina_indicator` 结构化指标和预告兑现关系判断。后续若要分析管理层讨论、审计意见、可读性、重大不确定性，再进入 PDF/LLM 阶段。

## 12. Signal 未来消费方式（当前阶段不实施）

本章描述未来消费方式，仅用于保证 schema 和语义预留。当前阶段不修改任何回测、Selection、Paper v2、模拟盘或实盘程序，也不把 `event_signal` 接入交易决策。

### 12.1 Backtest（未来阶段）

回测只读取：

```sql
SELECT *
FROM market.event_signal
WHERE time_mode = 'backtest'
  AND effective_trade_date <= :trade_date
  AND status = 'ACTIVE'
  AND ts_code IN (:universe)
```

消费逻辑：

| action | 回测处理 |
|---|---|
| `block_buy` | 当日禁止新买入；持仓处理策略由用户确认 |
| `warn_high` | 默认禁止新买入或进入高风险观察；可配置 |
| `warn_review` | 记录风险，默认不强制卖出；可配置降低权重 |
| `record_only` | 只记录，不影响交易 |
| `alpha_hint_disabled` | 研究记录，不影响交易 |

### 12.2 Paper/Live（未来阶段）

Paper/Live 使用 `time_mode='paper'/'live'`，允许使用 `first_seen_at` 产生当天盘前可用信号。

实时增量流程：

```text
1. 数据源轮询或 Tushare 增量入库。
2. 写 raw source + dataset_date_refresh_audit。
3. EventSignalEngine 只处理新增/更新源行。
4. 写 event_fact/event_relation/event_signal。
5. RiskPolicy provider 读取当日 ACTIVE 信号。
6. UI warning center 推送/展示。
```

### 12.3 Selection Center / Paper v2 风险策略（未来阶段）

将 `backend/services/selection_center/risk_policy.py` 中预留的 `announcement_risk` 扩展为更通用的 `event_signal` provider。

建议 provider 输出：

| signal action | `RiskDecision` 映射 |
|---|---|
| `block_buy` | `can_buy=False`, `reason_codes += event_type` |
| `warn_high` | 可配置 `can_buy=False` 或 `hold_only=True` |
| `warn_review` | `reason_codes` + `max_weight_multiplier` 可选 |
| `force_exit`（未来） | `force_exit=True`, `position_target_override=0`，需用户确认 |
| `record_only` | 不改变交易，只附加 `source_events` |

### 12.4 风险动作策略开关

可以并且应该增加开关，但开关应在“消费层策略”中实现，而不是改写历史 `event_signal`。`event_signal.action` 表示规则引擎给出的推荐动作；回测、Paper、Live 再用同一个 policy profile 把推荐动作映射为实际交易约束。

建议初始策略模式：

| policy mode | 语义 | `RiskDecision` 映射 |
|---|---|---|
| `alert_only` | 只预警，不影响买卖 | `can_buy=True`，只写 `reason_codes/source_events` |
| `block_new_buy` | 未持仓禁止新买，已持仓允许维持或卖出 | 无持仓 `can_buy=False`；有持仓不提高目标仓位 |
| `block_add` | 禁止加仓，比 `block_new_buy` 更严格地限制净买入 | 目标仓位上限 clamp 到当前持仓；卖出允许 |
| `reduce_cap` | 不强制清仓，但限制最大权重 | 设置 `max_weight_multiplier` 或目标权重上限 |
| `force_exit` | 强制退出，需显式开启 | `force_exit=True`、`sell_only=True`、`position_target_override=0` |

建议默认映射：

| risk_level | 默认 policy mode | 原因 |
|---|---|---|
| `P0_BLOCK` | `block_add` | 先禁止新买和加仓，强制卖出默认关闭，避免未经验证造成过度交易 |
| `P1_HIGH` | `block_new_buy` 或 `block_add` | 高风险先阻止扩大风险敞口 |
| `P2_REVIEW` | `alert_only` | 需要更多证据，阶段 1 只预警 |
| `P3_POSITIVE_CANDIDATE` | `alert_only` / `record_only` | 正向增益未验证，不影响交易 |
| `P4_NEUTRAL` | `record_only` | 不影响交易 |

开关设计约束：

- 同一个 policy profile 必须可用于回测、Paper、Live；回测结果和实盘行为才可比较。
- 每次回测/Paper run 必须冻结 policy profile 到 run metadata，避免事后修改开关导致不可复现。
- Live `force_exit` 必须默认关闭，建议需要二次确认和白名单；实际卖出仍要受停牌、跌停、流动性、订单风控限制。
- UI 可以提供“只预警 / 禁止新买或加仓 / 强制卖出”的简化选项，底层仍保存为版本化 JSON policy。

## 13. LLM/PDF 延后设计

本阶段不下载 PDF，不调用 LLM。

保留后续表/队列设计：

```text
market.event_review_queue
market.event_document_extract
market.event_llm_extract
```

后续只对以下集合触发：

- P2 且当前持仓/拟买入命中。
- 结构化业绩与公告标题冲突或缺字段。
- 预告兑现不及预期但需要原因解释。
- 诉讼/冻结/担保/关联交易/减值等金额比例决定风险级别的公告。
- 抽样 QA，用于改进标题规则。

LLM 只能输出结构化事实，交易动作仍由确定性规则计算。

## 14. 与现有本地数据管理/调度集成

### 14.1 同步计划

本阶段调度只负责 raw 数据刷新和 `event_signal` 派生生成，不触发 QE、Selection、Paper v2、模拟盘或实盘消费。

| 数据集 | 历史补齐 | 增量刷新 | 审计日期语义 |
|---|---|---|---|
| `tushare_forecast_raw` | `2018-08-01` 至最新交易日；按季度 VIP 或按代码普通接口 | 每小时/每日，回看最近 7-30 自然日公告日和最近报告期 | 独立 sparse event dataset，按自身源延迟和权限状态审计 |
| `tushare_express_raw` | 同上 | 每小时/每日，回看最近 7-30 自然日 | 独立 sparse event dataset，不与 forecast 同步状态互相覆盖 |
| `tushare_fina_indicator_raw` | 同上；字段多，建议先按季度 VIP | 每日 18:00、20:30、22:00 多次尝试；财报季可加密 | 独立 sparse event dataset，不与 forecast/express 同步状态互相覆盖 |
| `event_signal` | 源数据补齐后批量生成 | 源数据成功后触发增量生成 | 写 `event_signal_run`，必要时也写 dataset audit |

### 14.2 调度建议

第一阶段建议：

```text
09:00-09:30: 检查前夜/盘前公告和业绩源，生成 observed/paper/live time_mode 的事件信号数据，仅用于观察和审核，不接入交易
12:30: 午间轻量增量检查
18:00: 收盘后首次公告/业绩数据刷新
20:30: 财报/公告高峰二次刷新
22:30: 日终兜底刷新与 cninfo/公告校验
每小时: 公告 metadata 增量 + event_signal 增量，可设置低频轮询
```

财报季可临时加密；非财报季保持简单。

### 14.3 数据源失败自愈

- VIP 接口失败：退回普通接口 BY_CODE，不直接失败全任务。
- 单只股票失败：记录失败代码，后续 retry，只要核心数据集可覆盖大部分全市场，状态可为 `partial`。
- Tushare 权限失败：在审计表标记 `provider_permission_denied`，不反复高频重试。
- 空结果：对 sparse dataset 可为 `empty_valid`，不能无限回补同一天。
- 源数据修正：通过 `source_row_hash` 检测，重新生成对应 facts/signals。

## 15. 实施阶段计划

### Phase 1：统一 schema

范围：

- 新增 `event_fact`、`event_relation`、`event_signal`、`event_signal_rule_set`、`event_signal_run`。
- 增加初始化脚本/迁移 SQL。
- 增加 DB comment 完整性测试。

验证：

- 表存在。
- 每张表和每个字段都有 PostgreSQL comment。
- 唯一键和索引存在。
- `py_compile` 和 schema smoke 通过。

### Phase 2：公告 adapter

范围：

- 不删除现有 `ann_*` 表。
- 把现有 `ann_event_classification` / `ann_risk_signal` 映射成 `event_fact` / `event_signal`。
- 保持 `time_mode` 隔离，禁止 live/paper 覆盖 backtest。

验证：

- 同一公告、同一规则版本、不同 `time_mode` 可共存。
- P0/P1/P2 计数与现有公告风险结果一致。
- `event_signal` 的 effective date 与公告分类结果一致。

### Phase 3：Tushare 业绩 raw 数据集

范围：

- 新增 `tushare_forecast_raw`、`tushare_express_raw`、`tushare_fina_indicator_raw` 三张独立 raw 表和同步配置。
- raw 表只写源字段、`raw_payload`、观测元数据和任务审计信息；不得写入任何衍生事件/风险/信号字段。
- 优先验证 `forecast_vip` / `express_vip` / `fina_indicator_vip` 是否可用。
- 接入本地数据管理、自动补齐、调度、审计。

验证：

- 短日期/单报告期 smoke 成功。
- `market.dataset_date_refresh_audit` 有 success/empty_valid/failed 记录。
- 前端本地数据管理能看到三个数据集。
- 字段 comment 完整。
- raw 表不存在 `event_type/risk_level/action/effective_trade_date/alpha_score` 等衍生字段。

### Phase 4：财务事件 fact/signal engine

范围：

- `ForecastFactAdapter`
- `ExpressFactAdapter`
- `FinaIndicatorFactAdapter`
- `FinancialEventSignalRules v0`
- forecast -> express/fina relation builder

验证：

- 预增/预减/首亏/续亏/扭亏样本分类正确。
- 预告 + 正式结果不及预期样本可生成 `financial_positive_but_miss_expectation`。
- 信号 run 表记录输入/输出行数。
- 回测模式日期-only 全部下一交易日生效。

### Phase 5：统一 API 与 UI

范围限定：API/UI 只读取和展示 `event_signal`，不得调用 QE、Selection、Paper v2 或模拟盘接口，不得改变候选池、订单或仓位。

范围：

- `GET /api/v1/event-signals/effective`
- `GET /api/v1/event-signals/by-symbol/{ts_code}`
- `GET /api/v1/event-signals/summary`
- `GET /api/v1/event-signals/runs`
- 本地数据管理或单独事件中心 UI 展示最新 P0/P1/P2。

验证：

- API 按日期、股票、风险等级查询正确。
- UI 可查看 evidence、source_type、rule_version、effective_trade_date。

### Phase 6：风险策略集成（延后执行）

注意：当前阶段不修改任何回测、Selection、Paper v2 或模拟盘程序。Phase 6 只有在 raw 数据、event_signal、样本审核和离线事件研究通过后才执行。

范围：

- 实现 `event_signal` risk provider。
- Selection Center/Paper v2 通过 runtime profile 开关启用。
- QE/StrategyPackage 可冻结某日期前可见的 `event_signal` 快照。

验证：

- P0 `block_buy` 股票不会进入最终候选。
- record_only 不影响排序。
- 数据缺失时 fail-fast 或按配置告警，不静默放行。

### Phase 7：事件研究与正向 alpha 验证

范围：

- CAR/BHAR、市场/行业中性异常收益。
- 窗口：`[-5,-1]`、`[0,1]`、`[2,5]`、`[6,20]`、`[21,60]`。
- 分事件类型、分市值、分行业、分财报季、分市场状态。
- 与现有 alpha 相关性和边际贡献分析。

验证：

- P0/P1 风险规避是否降低回撤、踩雷、跌停、停牌暴露。
- P3 正向候选是否有稳定且交易后仍存在的超额收益。
- 未验证前不得启用 `alpha_score` 对生产交易排序的正向影响。

## 16. 不适用或暂不做的内容

| 内容 | 原因 |
|---|---|
| 全量 PDF 下载 | 成本高、下载成功率受源 URL/反爬/历史失效影响；第一阶段标题+结构化数据足够覆盖大部分风险 |
| 普通年报/季报标题 LLM | 标题没有方向信息，先用结构化财务指标和预告兑现关系 |
| 正向 alpha 自动加分 | 未经事件研究验证，可能引入追高或数据泄露 |
| 商业新闻数据直接依赖 | 第一阶段不引入商业数据成本和接口复杂度 |
| 复杂在线学习模型 | 当前需要可解释、可回放、可审计的规则 v0 |
| 强制卖出 | 对交易影响大，需要用户确认，并需要回测验证 |

## 17. 已确认决策与待确认问题

已确认：

1. Tushare 三个原始数据集使用三张独立 raw 表：`tushare_forecast_raw`、`tushare_express_raw`、`tushare_fina_indicator_raw`。
2. raw 表不得写入衍生字段；事件、关系、信号、特征、LLM/PDF 结果全部写入新衍生表。
3. 当前阶段先生成 `event_signal` 数据并做质量验证，不修改现有回测、Selection、Paper v2 或模拟盘程序。
4. 风险动作支持策略开关，但开关在消费层 policy profile 实现，不改写历史 `event_signal`。

仍需后续确认：


1. **P0/P1 对已有持仓的动作**：只预警、禁止加仓、降低权重，还是允许强制卖出？建议第一阶段不强制卖出。
2. **盘前 cutoff**：沿用 `09:25`，还是改为 `09:30`？建议沿用 `09:25`。
3. **北交所处理**：公告历史已按全市场可入库；交易信号是否继续默认排除 `.BJ`？建议第一阶段排除北交所交易信号，但保留原始数据。
4. **Tushare VIP 接口优先级**：是否允许先 smoke `forecast_vip/express_vip/fina_indicator_vip`，失败再退回普通接口？建议允许。
5. **财务规则阈值**：初始阈值用 `50%/30%/10pct`，后续通过事件研究校准。
6. **UI 入口**：先集成本地数据管理，还是新建事件预警中心？建议先 API + 本地数据管理摘要，后续独立事件中心。

## 18. 初始规则版本建议

```text
unified_event_signal_rules_v0_20260506
```

包含：

- 公告 v0 adapter：引用 `aistock_announcement_title_rules_v0_20260505`。
- 财务 forecast/express/fina rules v0。
- 预告兑现关系 rules v0。
- 时间语义 rules v0。

规则 hash 应由完整 JSON 配置生成，写入 `event_signal_rule_set.config_hash`。

## 19. 验证清单

实施后至少执行：

```text
python -m py_compile backend/db/init_unified_event_signal_schema.py backend/services/event_signal/*.py scripts/create_tushare_*_table.py
pytest tests/test_announcement_event_schema.py tests/test_announcement_title_classifier.py tests/test_event_signal_schema.py tests/test_event_signal_engine.py
nox -s local_data_management_audit
git diff --check
```

DB smoke：

```sql
-- 所有新增表存在
-- 所有新增列 comment 非空
-- event_signal backtest/live/paper 唯一键互不覆盖
-- source raw 表 data_stats_config 存在
-- 三张 Tushare raw 表不包含事件、风险、动作、effective date 或 alpha 衍生列
-- dataset_date_refresh_audit 有新数据集刷新记录
-- event_signal_run 成功记录 input/fact/signal 行数
-- 当前阶段没有新增 QE/Selection/Paper v2/模拟盘消费 event_signal 的调用路径
```

业务 smoke：

```text
1. 选 1 个公告 P0 样本：event_signal 产生 block_buy。
2. 选 1 个业绩预告大幅预增样本：只 record_only，不影响交易。
3. 选 1 个预告大增但正式结果低于预告中值样本：产生 warn_review。
4. 在 backtest 与 paper time_mode 下重复生成：互不覆盖，effective date 符合规则。
5. 当前阶段隔离检查：确认未修改 QE、Selection、Paper v2、模拟盘相关文件，`event_signal` 不被任何交易链路读取。
6. 未来 Phase 6 才验证 Selection/Paper provider：P0 候选被剔除，record_only 候选保留。
```

## 20. 参考链接

- Tushare `forecast` 业绩预告：https://tushare.pro/document/2?doc_id=45
- Tushare `express` 业绩快报：https://tushare.pro/document/2?doc_id=46
- Tushare `fina_indicator` 财务指标：https://tushare.pro/document/2?doc_id=79
- Tushare `anns_d` 上市公司公告：https://tushare.pro/document/2?doc_id=176
- Fine-Grained Classification of Announcement News Events in the Chinese Stock Market：https://www.mdpi.com/2079-9292/11/13/2058
- Jegadeesh & Livnat, Post-Earnings-Announcement Drift: The Role of Revenue Surprises：https://papers.ssrn.com/sol3/papers.cfm?abstract_id=903767
- Loughran-McDonald financial text analysis dictionary/project：https://sraf.nd.edu/loughranmcdonald-master-dictionary/
- RavenPack event analytics：https://www.ravenpack.com/
- Bloomberg Event-Driven Feeds：https://www.bloomberg.com/professional/products/data/enterprise-catalog/event-driven-feeds/
- FactSet Events and transcripts/event data：https://www.factset.com/
