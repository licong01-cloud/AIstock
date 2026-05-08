# AIstock 早期业绩/财务风险信号研究设计

日期：2026-05-07  
状态：Research Design v1，仅研究和只读验证，不接入交易消费者  
工作树：`F:\Dev\AIstock_worktrees\event-signal-policy-20260507`  

## 1. 结论先行

当前方向应当是：

```text
先做研究 -> 用本地真实数据脚本验证 -> 明确可用/不可用边界 -> 再决定是否生成正式衍生信号或接入回测/模拟盘
```

不建议把 `业绩预告 / 业绩快报 / 财务指标` 第一阶段做成硬禁止买入或强制卖出规则。更合理的定位是：

- 用它们作为 ST、退市风险、重大财务恶化的早期预警候选源。
- 先验证“提前发现风险”的召回率、命中率、领先时间、信号后收益分布、与正式 ST 公告的关系。
- 只有经过滚动年份、分行业、分市值、分事件类型验证后，才考虑 `score_down`、禁止加仓、降低权重等软约束。
- 正向增长信号第一阶段继续 `record_only`，不做 alpha 增益。

本设计只新增研究文档和只读研究脚本，不修改 QE、Selection Center、Paper v2、模拟盘、QMT 或任何交易执行路径。

## 2. Phase 0 文档发现与本地事实

已读取或复用的本地资料：

- `docs/codex_project_memory.md`
  - 工作树隔离、交易相关变更高风险、DB comment 标准、不得触碰生产 `8001`。
- `docs/architecture/unified_event_signal_architecture_20260506.md`
  - raw source -> `event_fact` -> `event_relation` -> `event_signal` 的统一非日频事件信号架构。
- `docs/architecture/event_signal_st_first_and_llm_preprocessing_design_20260506.md`
  - ST-first、PDF/LLM 延后、结构化财务数据优先。
- `docs/architecture/event_signal_policy_lifecycle_and_qe_validation_design_20260507.md`
  - policy/effect/state/overlay/validation 表设计，信号逐一叠加验证。
- `backend/services/event_signal/financial_event_study.py`
  - 已有财务事件收益研究脚本，按 `market.event_signal` 计算事件窗口收益，不写 DB。
- `backend/services/event_signal/financial_signal_policy_diagnostics.py`
  - 已有研究型策略建议脚本，输出 warning / record-only 建议，不接入交易。
- `tests/aistock_validation/history/local_data_management/20260506_235145_l2_financial-structured-event-signal-study.md`
  - 已有 2024-01-01 至 2026-05-06 财务事件研究验证。
- `tests/aistock_validation/history/data_ingestion/20260506_2030_l2_tushare-financial-event-full-window-sync.md`
  - 已有 2018-08-01 至 2026-05-06 三个 Tushare 业绩 raw 数据集补齐验证。

本次只读 DB 快照，查询时间为 2026-05-07，环境为本地 `aistock` PostgreSQL：

```text
┌──────────────────────────────────┬──────────┬────────────┬────────────┬────────────┬────────────┬──────────────┐
│ 数据集 / 信号范围                 │ 行数     │ 最早公告日 │ 最晚公告日 │ 最早报告期 │ 最晚报告期 │ 股票数       │
├──────────────────────────────────┼──────────┼────────────┼────────────┼────────────┼────────────┼──────────────┤
│ market.tushare_forecast_raw       │ 66,837   │ 2018-04-10 │ 2026-04-29 │ 2018-06-30 │ 2026-03-31 │ 5,694        │
│ market.tushare_express_raw        │ 14,115   │ 2018-07-03 │ 2026-05-07 │ 2018-06-30 │ 2026-03-31 │ 4,137        │
│ market.tushare_fina_indicator_raw │ 302,327  │ 2018-07-10 │ 2026-05-07 │ 2018-06-30 │ 2026-03-31 │ 7,087        │
│ 财务结构化 active event_signal    │ 145,571  │ 2018-04-10 │ 2026-05-01 │ --         │ --         │ --           │
│ ST-first active event_signal      │ 12,048   │ 2018-08-01 │ 2026-05-06 │ --         │ --         │ --           │
└──────────────────────────────────┴──────────┴────────────┴────────────┴────────────┴────────────┴──────────────┘
```

注意：

- `market.trading_calendar` 已包含未来交易日模板到 2026-12-31，不能把 calendar 最大日期当作数据最新日期。
- `market.kline_daily_raw` 与 `market.stk_limit` 当前最大数据日期均为 2026-05-07。
- Tushare 结构化接口只给 `ann_date`，没有更准确的真实披露时间；模拟盘/实盘可用 `first_seen_at/observed_at`，回测只能用 `ann_date` 和统一时间规则。

## 3. 外部论文与机构实践可借鉴点

本阶段借鉴“方法论”，不照搬阈值和交易动作。

```text
┌──────────────────────────────┬──────────────────────────────┬──────────────────────────────────────────────┐
│ 来源                         │ 可借鉴                       │ AIstock 第一阶段处理                         │
├──────────────────────────────┼──────────────────────────────┼──────────────────────────────────────────────┤
│ Altman Z-score               │ 财务困境应使用多财务变量组合 │ 不用单个亏损/下降字段做硬规则                │
│ Ohlson O-score               │ 用概率/评分表达破产风险      │ 输出 risk_score / warning，而非直接卖出       │
│ Shumway hazard model         │ 财务困境是时间到事件问题     │ 研究 future_ST_90/180/365d 和领先时间         │
│ Beneish M-score              │ 财务造假/操纵是筛查器        │ 后续加入财务质量/异常项，不直接定性           │
│ Piotroski F-score            │ 盈利、杠杆、流动性、效率组合 │ 后续做综合质量恶化分，不依赖单项指标          │
│ PEAD / SUE                   │ 盈利 surprise 需要事件研究   │ 正向/负向业绩信号必须验证多窗口收益           │
│ 中国 ST 财务困境预测论文     │ ST 可作为 A 股财务困境标签   │ 以正式 ST/退市风险公告构造本地 target cycles │
│ 商业事件数据实践             │ taxonomy/relevance/novelty   │ 后续给事件信号加 novelty、dedupe、状态解除    │
└──────────────────────────────┴──────────────────────────────┴──────────────────────────────────────────────┘
```

关键参考链接：

- Altman, 1968, Financial Ratios, Discriminant Analysis and the Prediction of Corporate Bankruptcy: https://www.scirp.org/reference/referencespapers?referenceid=2502061
- Ohlson, 1980, Financial Ratios and the Probabilistic Prediction of Bankruptcy: https://econpapers.repec.org/RePEc:bla:joares:v:18:y:1980:i:1:p:109-131
- Piotroski, 2000, Value Investing: The Use of Historical Financial Statement Information to Separate Winners from Losers: https://econpapers.repec.org/article/blajoares/v_3a38_3ay_3a2000_3ai_3a_3ap_3a1-41.htm
- Bernard & Thomas, 1989, Post-Earnings-Announcement Drift: https://explore.openaire.eu/search/result?pid=10.2307%2F2491062
- Predicting financial distress of Chinese listed companies using machine learning: To what extent does textual disclosure matter?: https://www.sciencedirect.com/science/article/abs/pii/S1057521923002867
- RavenPack event data practice on event relevance / novelty: https://www.ravenpack.com/blog/new-ravenpack-analytics-event-detection

对 AIstock 最重要的外部结论是：A 股财务困境研究常用 ST 作为困境标签；结构化财务数据往往强于文本，文本/LLM 的增量价值要在结构化数据基准上再验证。因此第一阶段优先研究 Tushare 结构化数据，而不是先下载 PDF 或调用 LLM。

## 4. 研究问题

本阶段要回答的不是“如何交易”，而是以下研究问题：

1. `业绩预告 / 业绩快报 / 财务指标` 是否能在正式 ST 或退市风险公告前，提前给出高召回的风险预警？
2. 不同来源的领先时间分布怎样：`forecast`、`express`、`fina_indicator`、`financial_relation` 哪个更早、哪个更准？
3. 如果只看负面财务事件，未来 90/180/365 天进入 ST 或退市风险的命中率是多少？相对基准有多少 lift？
4. 多源共振是否明显提高命中率，例如同一股票 120 天内 forecast + fina_indicator 都给出负面信号？
5. 这些信号发布后 1/5/10/20/60 个交易日收益如何？是否只是已经滞后的风险提示？
6. 对于未来正式 ST 的股票，财务信号出现后到 ST 公告前，股价是否已经下跌？是否存在“提前消化”？
7. 是否有少数类型可以升级为 `P1_HIGH` 或 `block_add_candidate`，还是只能保持 `P2_REVIEW`？

## 5. 数据与标签设计

### 5.1 输入数据

研究脚本只读取，不写入 DB：

- 财务结构化信号：
  - `market.event_signal`
  - `rule_version='unified_event_signal_rules_v0_20260506'`
  - `source_type IN ('tushare_forecast','tushare_express','tushare_fina_indicator','financial_relation')`
  - `time_mode='backtest'`
  - `signal_status='ACTIVE'`
- ST / 退市风险目标事件：
  - `market.event_signal`
  - `rule_version='unified_event_signal_rules_st_first_v1_20260506'`
  - `event_type IN ('stock_st_imposed','stock_st_added_or_continued','stock_delisting_risk_warning','stock_delisting_confirmed')`
  - `time_mode='backtest'`
- 收益验证：
  - `market.kline_daily_raw`
  - 指数基准可继续使用 `000300.SH`
  - 停牌、跌停状态可复用现有 `financial_event_study.py` 的处理。

### 5.2 Target cycles

ST/退市风险公告有大量重复公告，不能逐条当成独立标签。研究脚本按股票聚合为 cycles：

```text
同一 ts_code 的 ST/退市风险事件按 effective_trade_date 排序；
如果与上一个 target 事件间隔 <= 180 自然日，视为同一 risk cycle；
如果间隔 > 180 自然日，开启新的 risk cycle。
```

cycle 的代表日期：

- `cycle_start_event_date`：cycle 内最早 `source_event_date`。
- `cycle_start_effective_trade_date`：cycle 内最早 `effective_trade_date`。
- `cycle_primary_event_type`：cycle 首个事件类型。

### 5.3 财务风险候选

第一阶段只研究负面/风险型财务信号：

```text
financial_forecast_loss
financial_forecast_large_decline
financial_express_loss
financial_express_large_decline
financial_indicator_large_decline
financial_positive_but_miss_expectation
```

暂不把以下正向或研究型信号纳入硬风险研究：

```text
financial_forecast_large_growth
financial_forecast_turnaround
financial_express_large_growth
financial_indicator_large_growth
```

这些正向信号继续作为 `record_only`，后续按 PEAD/SUE 类事件研究单独验证。

## 6. 指标设计

研究脚本第一版输出以下指标：

```text
┌──────────────────────┬──────────────────────────────────────────────────────────────┐
│ 指标                 │ 含义                                                         │
├──────────────────────┼──────────────────────────────────────────────────────────────┤
│ ST cycle coverage    │ 未来 ST/退市风险 cycles 中，过去 365 天是否有财务风险信号   │
│ lead time            │ 财务风险信号到 ST cycle start 的自然日/交易日领先时间       │
│ source contribution  │ forecast / express / fina_indicator / relation 的覆盖贡献   │
│ precision@horizon    │ 财务风险信号后 90/180/365 天内是否进入 ST/退市风险 cycle    │
│ base-rate lift       │ precision 相对全市场/全财务信号基准的提升                  │
│ source-combo lift    │ 1 个来源 vs 2 个以上来源共振时命中率是否提升                │
│ event return         │ 信号后 T+1/T+5/T+10/T+20/T+60 收益与异常收益               │
│ pre-ST return        │ 财务信号到正式 ST 前一交易日的累计收益                     │
│ yearly stability     │ 分年份指标是否稳定，避免只在某一年有效                     │
└──────────────────────┴──────────────────────────────────────────────────────────────┘
```

## 7. 决策门槛

在没有通过以下门槛前，不允许进入交易消费者：

```text
┌────────────────────┬──────────────────────────────────────────────┬────────────────────────────┐
│ 候选动作           │ 最低研究门槛                                 │ 第一阶段默认                │
├────────────────────┼──────────────────────────────────────────────┼────────────────────────────┤
│ warning_only       │ 逻辑合理且样本充足                            │ 可保留                      │
│ score_down         │ 多年份稳定负收益，且 precision/lift 明显       │ 暂不启用                    │
│ block_add_candidate│ 高风险类型 precision@180/365 明显高于基准      │ 仅作为研究候选              │
│ block_buy          │ 必须接近 ST/退市正式风险，且回测收益/回撤改善  │ 财务信号不启用              │
│ force_exit         │ 必须是正式重大风险且实际回测验证通过           │ 财务信号不启用              │
│ score_up           │ PEAD/SUE 类正向收益多窗口稳定，扣成本仍有效    │ 暂不启用                    │
└────────────────────┴──────────────────────────────────────────────┴────────────────────────────┘
```

## 8. 只读脚本设计

新增研究脚本建议路径：

```text
backend/services/event_signal/early_financial_distress_research.py
```

脚本职责：

- 读取本地 `event_signal` 财务信号和 ST-first 信号。
- 构建 ST/退市风险 cycles。
- 统计过去 365 天财务信号对未来 ST cycles 的覆盖率和领先时间。
- 统计财务风险信号在未来 90/180/365 天的命中率。
- 按 source_type、event_type、risk bucket、年份输出聚合。
- 生成 JSON / Markdown 报告到 `reports/event_signal/early_financial_distress/`。
- 默认不输出大明细 CSV；需要时通过 `--write-details` 才输出。
- 不写 DB、不改 schema、不调用 Tushare、不下载 PDF、不调用 LLM、不触碰交易模块。

命令形态：

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:TDX_DB_HOST='127.0.0.1'
$env:TDX_DB_PORT='5432'
$env:TDX_DB_NAME='aistock'
$env:TDX_DB_USER='postgres'
$env:TDX_DB_PASSWORD='***'
python -m backend.services.event_signal.early_financial_distress_research `
  --start-date 2018-08-01 `
  --end-date 2026-05-07 `
  --lookback-days 365 `
  --cycle-gap-days 180 `
  --output-dir reports/event_signal/early_financial_distress
```

## 9. 验证计划

### L0 / L1 单元验证

- ST cycle 聚合：同一股票间隔 180 天内合并，超过 180 天拆分。
- 财务信号匹配：只匹配 target 前的信号，不能使用 target 后信号。
- precision horizon：信号后 90/180/365 天内命中 target 才算命中。
- source_type/event_type 分组计数正确。
- 空数据范围报错明确。

### L2 本地只读 DB 验证

- 跑全窗口 `2018-08-01` 至 `2026-05-07`。
- 生成 JSON / Markdown。
- 报告包含数据快照、cycle 数、coverage、precision、lead bucket、yearly split。
- 与已有财务事件收益研究脚本结果互补，不重复大明细。

### 隔离验证

必须通过以下检查：

```powershell
rg -n "early_financial_distress|financial_distress" `
  backend/services/selection_center `
  backend/services/paper_trading_v2 `
  backend/services/quantevolver `
  backend/infra/qmt_client.py `
  backend/routers/qmt.py -S

python -m py_compile backend/services/event_signal/early_financial_distress_research.py
pytest backend/tests/event_signal/test_early_financial_distress_research.py -q
git diff --check
```

预期结果：

- 交易消费者目录无引用。
- 测试通过。
- 报告生成成功。
- 生产端口 `8001` 未触碰。

## 10. 后续演进路径

```text
Phase A：早期财务困境研究脚本
  -> 产出 coverage / precision / lead time / yearly split

Phase B：事件收益和 QE Loop1 离线 overlay 双重验证
  -> 财务信号仍不硬禁止，最多研究 score_down/block_add_candidate

Phase C：阈值和组合规则研究
  -> 多源共振、连续亏损、正式财务兑现不及预期、现金流背离

Phase D：生成研究型衍生信号
  -> 写 event_signal 或 policy effect rule，但状态为 RESEARCH/DISABLED

Phase E：小范围接入回测 overlay
  -> 仍不改模拟盘/实盘，先做独立回放

Phase F：经过验证后再进入 Selection/Paper/QE 统一 risk provider
  -> 必须冻结 policy profile，保证回测和实盘一致
```

## 11. 当前明确不做

- 不把财务结构化信号做成硬禁止买入。
- 不把财务结构化信号做成强制卖出。
- 不启用任何正向 alpha 增益。
- 不下载 PDF。
- 不调用 LLM。
- 不修改 QE、Selection Center、Paper v2、模拟盘、QMT。
- 不新增 DB 表。
- 不修改 raw 表。

## 12. 第一轮执行产物

本设计文档完成后，下一步执行：

1. 新增 `backend/services/event_signal/early_financial_distress_research.py`。
2. 新增 `backend/tests/event_signal/test_early_financial_distress_research.py`。
3. 跑 2018-08-01 至 2026-05-07 全窗口只读研究。
4. 生成 `reports/event_signal/early_financial_distress/*.json` 和 `*.md`。
5. 写入一份验证记录到 `tests/aistock_validation/history/local_data_management/`。
6. 根据实际结果决定是否进入“阈值/多源共振研究”，而不是直接接入交易。
