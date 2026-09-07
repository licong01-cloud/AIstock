# 持仓与自选池择时建议系统 F2 蓝图

> 版本：v2.5
> 日期：2026-09-07
> Feature tier：F2
> 状态：`FIRST_RELEASE_RUNTIME_VERIFIED_L2_AUDIT_COMPLETED_L4B1_AUDIT_COMPLETED_INSUFFICIENT_DATA_PT_NEXT_004_DESIGN_PLANNED_NOT_STARTED`
> objective contract：`POSITION_TIMING_ADVICE_V1`
> 演进设计：`POSITION_TIMING_ACTION_VALUE_V2`（设计完成，未实现；不修改 v1 历史契约）
> decision use：`HUMAN_TRADING_ADVICE`
> 对照蓝图：`F:/Dev/AIstock_worktrees/stock-timing-strategy-blueprint-20260831/docs/architecture/stock_timing_strategy_system_blueprint_f2_20260831.md`
> 权威规范：`docs/standards/aistock_development_standard_v1.5_20260523.md`

`DESIGN_VERIFIED` 只表示对应设计条款已经闭合。实现块一由 PR `#4277` 合入；包含 L1a、prospective outcome 与轻量分析范围管理的完整首发源码由 PR `#4287` 合入（merge commit `a8922fb82d4163ab4194c2efdde2ae584e5100a4`）。2026-09-06 的 backend-main 重启后收据已验证 BUG-1365 修复所在 merge commit `19b45dbe38bfea40339bd9d2d9eadc9b1e005fcc` 为运行进程身份的祖先，业务探针与身份摘要均通过，close-sync PR `#4311` 已合入。首发之后的 `PT-NEXT-002` 已完成一次冻结的离线 L2 可学性审计；结果为 `INCONCLUSIVE`、`selected_model_id=null`，不产生运行时模型，也不改变 L1/L1a。用户完成本轮重启后，`/api/v1/position-timing/evidence` 已读回同一正式 L2 引用。`PT-NEXT-003` 的单文件离线 L4b-1 pipeline 已实现；首次双方向 audit 随后被可达性复核证明含有现行 L1 不会生成的 AT_OPEN BUY 假设，因此只保留为历史。修订后的权威 request `ptl4b1req_f55a3338e602c2dd42a32c17` 绑定干净提交 `03e3db5ec53aa433bfbc3c114a3c3215d3cd3ed6`，只注册可达的 risk-exit `EXIT/SELL/AT_OPEN` 单假设；当前两个 `CARD_ISSUED` 均为 HOLD，正式结果为 `INSUFFICIENT_PROSPECTIVE_ACTION_CARDS`、selected side 为空。该结果不授权自动交易、数据库变更、运行时分钟新信号或 L4b-2，也不得把尚未成熟的 prospective 结果表述为个股模型有效性证据。

2026-09-07 的运行态只读复核仍读到决策日 2026-09-04、目标日 2026-09-07 的两张 `HOLD` 卡；analysis scope 有效标的为 2，显式自选为 0，intent 为 0，产品层级仍是 `RULE_BASED_RISK_MANAGEMENT`，L2 为 `OFFLINE_PIPELINE_AVAILABLE_NO_RUNTIME_MODEL`，HMM 为 `CONTEXT_ONLY_NOT_WIRED_IN_BLOCK_ONE`。本次 GET evidence 再核到 `CARD_ISSUED=2`、paired matured=0、pending horizons=10、intervention-intent=0，不能据此声称已有超额收益。v2.4 只安排总体 shadow 报告，尚未闭合模型到个股建议；本版把 `PT-NEXT-004` 修订为日频模型建议完整闭环，`PT-NEXT-005` 再强化执行真实性与可选信息增量。**本次只更新设计**：未启动代码、训练、artifact、registry、依赖、数据库或运行态变更，现有卡片不变。

## 1. Background / 背景与结论

### 1.1 终极目标

本系统只为“全部当前持仓股 ∪ 用户从已确认自选池中显式选择的股票”解决一个问题：

> 在不替用户选股、不自动下单的前提下，于 T 日收盘后给出每只目标股票在 T+1 的明确行动、原始价格触发条件、建议数量或仓位、不可执行原因和成本；盘中只提醒日频卡片已经冻结的买卖点，由用户决定是否交易。

首发已形成“收盘出计划、盘中到价提醒、事后可评价”的规则型人工决策闭环，但尚未证明 alpha。后续研发的终点是：对用户指定股票，模型能判断是否建立、增加、保持、减少或退出敞口，并验证其**扣除费用及执行摩擦后的择时增量价值**，不是不断增加研究报告或卡片数量。股票范围由用户与唯一持仓 authority 决定；预算只约束数量，不应要求用户先填“我要买”才能分析是否值得买。Selection、HMM 与多智能体均不是主线前置依赖。

收益比较必须同标的、同起始资本、同评价区间，计算完整现金与持股路径：主报告同时比较“同股固定买入持有”与“冻结 L1 政策”；市场/行业相对收益、最大回撤、换手、现金占用和错过反弹为解释维度，不能用降低敞口带来的低回撤或横截面 IC 替代择时净增量。目标是取得可重复的成本后超额收益证据，不保证每只股票或每个市场阶段盈利。历史 §5 的单卡边际 outcome 和 §6.2 的退出 audit 均不能单独充当连续策略收益。

### 1.2 现有证据给出的方向

1. N2 exit hindsight oracle 显示较大的事后动作空间，但冻结 Ridge 政策没有捕获正值下界，且审计功效不足。这支持先交付规则型风险与执行纪律，并积累 prospective outcome，而不是先建复杂模型平台。
2. N2 entry 中 `FIXED_5_CASH` 与零不可分辨，`DYNAMIC_Q90_CASH` 显著为负。它们是研究臂，不是可直接搬入运行时的 guard。
3. N3 分钟信息集的 `selected_trial_count=0` 只否定该次 `ALPHA_RANKING` 横截面增量，不回答特定股票、同方向同规模下的执行时点问题。
4. 因此产品边界冻结为：第一批交付 L1 规则行动卡与 L1a 实时报价提醒；L2 数据契约同批冻结、训练管线后移为独立离线任务。该 L2 任务现已完成且未选出模型；L4b-1 分钟执行研究也已作为独立离线任务启动，仍不进入首发运行面。
5. HMM 尚未形成可消费的稳定运行输入，不应等待它。下一优先任务先用长历史可核验的价量、波动、相对强弱和持仓/资金状态训练共享模型，对目标个股推断；资金流、筹码、行业、事件与 HMM 为逐块检验的可选增强，不能以未经覆盖核验的固定字段清单堵住核心研究。
6. 现有多智能体股票分析可以复用其解释思路，但当前入口会持久化结果、输出含自由文本且新闻/公告链路尚不能直接证明完整 PIT。它不能直接成为择时模型 authority；结构化事件特征与可选解释层必须分离。

### 1.3 证据语义目录

本设计引用任何 N 线实验臂、政策名、`result_class` 或契约常量时，首次定义语义必须引用代码，首次引用数值必须引用 receipt；不得仅凭名称推断含义。后文可引用本目录锚点。

| evidence_id | 语义与已核事实 | 权威路径 |
|---|---|---|
| `EVID-N2-EXIT-ACTION` | 1930 个 baseline episode，1928 个可评价，其中 1349 个存在正的 hindsight intervention；oracle mean `386.6023 bps` | `F:/Dev/AIstock_model_artifacts/advisory_n2_entry_exit_formal_v1_20260902/action_audit_bundles/5c5946a7adfb1e41c5287d5781f240fa290d690c63be0679edb5b00960556f2c/exit_summary.json` |
| `EVID-N2-EXIT-LEARN` | Ridge 政策点估计 `-56.8073 bps`，95% CI `[-200.3336, 52.6247]`，`mde_bps=181.2860`，`oracle_capture_ratio=-0.14694`，`evidence_state=INCONCLUSIVE`，`result_class=EXPLORATORY`，`selected_trial_count=0` | `F:/Dev/AIstock_model_artifacts/advisory_n2_exit_learnability_formal_v1_20260902/exit_learnability_bundles/03d17a18f01af0c6d9055c1efb8f977bee1e26b19fa9cb823e79495071633937/learnability_receipt.json` |
| `EVID-ENTRY-SEMANTICS` | `FIXED_5_CASH` 是 `FIXED_GAP_5`（500 bps 上限）加 `CASH` 填充；可产生 `REDUCE`，`SKIP/WAITING` 时槽位留现金。`FROZEN_DYNAMIC` 是通用模式，本身不等于 Q90 | `backend/services/advisory_model_first/entry_guard_decision.py:17`、`:54`、`:239`、`:300`；`backend/services/advisory_model_first/entry_exit_formal_contracts.py:178`、`:184`；`backend/services/advisory_model_first/entry_exit_formal_pipeline.py:223`、`:598`、`:627` |
| `EVID-ENTRY-Q90` | `DYNAMIC_Q90_CASH` 才把通用 `FROZEN_DYNAMIC` 实例化为 `max(0, entry_gap_q90) × 10000 bps` 并采用 `CASH`；其 lift `-23.5491 bps`，CI `[-32.6066, -5.7124]`。该结果不得外推到所有 frozen-dynamic 定义 | `backend/services/advisory_model_first/entry_exit_formal_pipeline.py:401`；`F:/Dev/AIstock_model_artifacts/advisory_n2_entry_exit_formal_v1_20260902/action_audit_bundles/5c5946a7adfb1e41c5287d5781f240fa290d690c63be0679edb5b00960556f2c/entry_summary.json` |
| `EVID-ENTRY-FIXED5` | `FIXED_5_CASH` lift `2.4587 bps`，CI `[-0.7481, 7.3808]`；只能得出与零不可分辨，不能假定它机械增加或推迟一轮交易 | 同上 `entry_summary.json` |
| `EVID-N3-MINUTE` | candidate RankIC `0.090195`，parent/comparator `0.122839`；Top5 成本后净超额 `128.3145` 对 `443.6526 bps`；family-wise 四项均未通过，`selected_trial_count=0`，objective 为 `ALPHA_RANKING` | `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/model_summary.json`、`learnability_receipt.json`、`request.json` |
| `EVID-N0-CONTROL` | 全局 N0 registry 是只读历史背景；L2 正式 request 冻结时为 36 条，route 绑定 `trial_registry_sha256`。既有 N1/N2 delivery 在 append 后重建 route，故择时不得写该控制面 | `backend/services/advisory_model_first/research_control.py:97`、`:876`；`backend/services/advisory_model_first/entry_exit_formal_pipeline.py:1032`；`backend/services/advisory_model_first/exit_learnability_pipeline.py:1442`；`F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830/trial_registry.jsonl`、`current_route.md` |
| `EVID-L2-FORMAL` | 96 cohorts、388,035 episodes、7,351,727 review rows；Ridge 为 `NEGATIVE`（`-45.3852 bps`，adjusted CI `[-86.6868,-7.7560]`），GBDT 为 `INCONCLUSIVE`（`-35.0801 bps`，adjusted CI `[-82.3353,10.4406]`），两者 power 均 `ADEQUATE`；study `INCONCLUSIVE`、无 selected model | `F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/l2_learnability_bundles/eef1f771a5d8ae3c002feaf6ed46007df9a0ee3726894893abdc93cddc8f3f51/learnability_receipt.json`、`source_identity_receipt.json`、`manifest.json` |
| `EVID-L4B1-FIRST-AUDIT` | 首份 request 绑定代码 `e00e624797ae627c7d425a376377938a46e43b68` 与 2026-08-31 minute candidate，得到 2 张 NON_ACTION/HOLD、0 eligible。随后可达性复核发现该 request 把 BUY/SELL 同列为 AT_OPEN family，但现行 L1 买入全部是 ON_PRICE_TRIGGER；该 bundle 保留为历史审计，不再作为当前 hypothesis contract | `F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/l4b1_execution_window_bundles/78ee08b9d712c3b62517dc740ce6f2fac3ad39ec19670d5c7ee8c72c6e1f0816/receipt.json`、`backend/services/position_timing/service.py:1920`、`:2060`、`:2083` |
| `EVID-L4B1-REACHABILITY` | 买入路径固定 `ON_PRICE_TRIGGER`；`risk_exit` 先固定 `target_qty=0/action=EXIT`，随后只有该 EXIT/SELL 固定 `AT_OPEN`，普通卖出同样是 `ON_PRICE_TRIGGER`。故首个 L4b-1 正式 family 只能是 risk-exit EXIT/SELL 单假设；不得用 T+1 realized branch 反推固定数量 | `backend/services/position_timing/service.py:1868`、`:1920`、`:2060`、`:2083` |
| `EVID-L4B1-FORMAL` | 修订 request 绑定代码 `03e3db5ec53aa433bfbc3c114a3c3215d3cd3ed6` 与同一 2026-08-31 minute candidate；family count 为 1，人口为 2 张 NON_ACTION/HOLD、0 eligible，故 status `INSUFFICIENT_PROSPECTIVE_ACTION_CARDS`、SELL 为 `INSUFFICIENT_DATA/UNDERPOWERED`、selected sides 为空。receipt SHA-256 为 `6e7f7c876ae02c844a4eaed268ea31596447f3fc623d7adc0ba9b905def13b6d`；inspect 与 exact retry 通过，runtime policy/card/event/order/current route 均未写 | `F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/l4b1_execution_window_bundles/450f8c82300c4c86199097c9e10eb8b61113b50e068c0c7b27cde8ce00acb7f4/receipt.json`、`request.json`、`manifest.json` |
| `EVID-TDX-CONTRACT` | 批量报价上限 50，最大陈旧度 5 分钟，最大未来偏斜 30 秒 | `backend/services/simulation_data/contracts.py:47` |
| `EVID-TDX-MINUTE` | 当前 `fetch_minute_kline_tdx` 调用无日期/count 参数的 `/api/kline-all/tdx`，取得端点完整响应后在客户端筛 `trade_date`；第一阶段不得调用 | `backend/data_service/tdx_adapter.py:191` |
| `EVID-GUARD-DEFAULTS` | 当前 `PriceGuardPolicy` 与 `ExitGuardPolicy` 的 `rule_v1/rule_default` 默认值 | `backend/services/trading_core/price_guard.py:77`、`:81`、`:102`、`:112`；`backend/services/trading_core/exit_guard.py:28`、`:38`、`:42`、`:46`、`:50` |
| `EVID-PRICE-BASIS` | 仓库已有 `market.kline_daily_raw + market.adj_factor` 的 qfq 链路；数据服务在复权因子缺失时抛错而非默认为 1 | `backend/data_service/api.py:355`、`:379`、`:389`；`backend/data_service/qe_data_service.py:354`、`:471`、`:508`；`backend/qlib_exporter/db_reader.py:1026`、`:1075`、`:1099` |
| `EVID-FEE-MODEL` | ledger 以 `(order_id, symbol, side)` 累计多次 fill，但 `FeeModel` 把最低 5 元作用于总费率，不等于本设计“佣金下限、规费另加”的分项公式 | `backend/services/trading_core/ledger.py:92`、`:419`、`:458` |
| `EVID-BOARD-LOT` | 沪深主板和创业板 `(min=100, increment=100)`；科创板 `(min=200, increment=1)`；只有卖出全部剩余持仓时才允许零股尾单 | `backend/execution_algos/board_lot.py:36`、`:71`；`backend/services/trading_core/ledger.py:373` |
| `EVID-UNIVERSE` | 第一阶段 legacy 持仓来自 `portfolio_manager.get_all_stocks`；`notification_service.py` 位于仓库根目录；watchlist 生命周期为 `CANDIDATE/ENTERED/HOLDING/EXITED` | `backend/routers/portfolio.py:6`、`:8`、`:45`；`portfolio_manager.py:166`；`backend/services/advisory_lifecycle.py:29` |
| `EVID-MINUTE-SNAPSHOT` | 最新只读候选 `20260831-qe_hmm_full_v2-direct-20260905-candidate` 的 minute component 为 PASS，SH/SZ、排除 BJ，日历 `2024-01-02 09:30:00` 至 `2026-08-31 15:00:00`，5,136 个 physical feature instruments、155,292 calendar rows；生产写入与 pointer change 均为 0 | `X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260905-candidate/direct_monthly_state.json`、`components/minute_bin_candidate/meta_export.json`、`calendars/1min.txt` |
| `EVID-MINUTE-BASIS` | minute Bin 的 OHLC 为 `raw × qfq_factor`，volume 为 `raw_shares / qfq_factor`，amount 为原始人民币成交额；因此 raw open=`$open/$factor`，raw VWAP=`Σ$amount/Σ($volume×$factor)` | `backend/qlib_exporter/authoritative_bin_exporter.py:1222`、`:1231` |
| `EVID-FACTOR-LIBRARY-20260907` | 只读分析显示因子库共 789 项、可用 584 项，覆盖 MOM/VOL/LIQ/VAL/QUAL/CORR/TECH/SIZE/STAT/MF/CHIP/ML；数据字段使用率为 daily_pv 100%、daily_basic 68.8%、moneyflow 50%、bak_basic 73.3%、cyq_perf 100%、sector_data 95.7%、static_factors 0%。这些百分比是“因子定义使用了多少字段”，不是行级/日期覆盖率，也不是择时有效性证据 | `python scripts/analyze_factor_library.py --json`；`scripts/analyze_factor_library.py` |
| `EVID-HMM-RUNTIME-20260907` | 运行态 evidence 把 HMM 标为 `CONTEXT_ONLY_NOT_WIRED_IN_BLOCK_ONE`，市场 regime methods 当前 `available=[]`；这是当前接入准备度，不是永久否定 HMM 方法 | `GET /api/v1/position-timing/evidence`；`GET /api/v1/market/regime-label/methods` |
| `EVID-MULTI-AGENT-BOUNDARY` | 当前股票分析默认关闭 news/announcement；response 含 `agents_raw/discussion/final_decision` 自由结构，`analyze_stock` 还会写 `app.analysis_records`。新闻内部调用未把 `analysis_date` 传入 `_get_news_data`，Eastmoney 公告内部调用也未传日期窗口；趋势分析仍声明为 mock skeleton。因此择时不得直接调用该有副作用 POST，也不得把原始 LLM 文本当模型输入或决策 authority | `backend/agents/stock_analysis.py:39`、`:47`；`backend/models/analysis.py:24`、`:29`；`backend/services/analysis_service.py:208`、`:599`、`:643`；`backend/core/qstock_news_data_impl.py:70`；`backend/core/unified_data_access_impl.py:1693` |
| `EVID-EVENT-PIT-BASE` | timestamp 与发行人绑定纯实现可复用，但不证明旧事件值可还原；公告 adapter 对 signal_key upsert 会更新 available_at、status、severity 等，DATE_ONLY 也不等于精确可见时刻。历史特征还需当时版本/可重建源，否则只作前瞻快照或排除 | `backend/services/event_signal/time_semantics.py`；`backend/services/event_signal/announcement_issuer_binding.py:202`；`backend/services/event_signal/announcement_adapter.py:568` |
| `EVID-CPCV-BOUNDARY` | 既有 CPCV 从 validation blocks 的补集选训练再 purge/embargo，允许训练日期晚于验证日期；这是组合稳健性诊断，不是历史可部署的滚动收益证明，v1 receipt 保持原值 | `backend/services/advisory_model_first/policy_cpcv.py:73`；`EVID-L2-FORMAL` 同 bundle 的 `cpcv_paths.json` |
| `EVID-V2-SOURCE-NAMES` | 候选公式名不等于真实字段名，例如 cyq 的 `cost_50pct` 导出为 `cp_cost_50pct`；必须核对源字段、单位、覆盖与发布时间后冻结，不凭简称建 schema | `backend/qlib_exporter/field_map.py:117` |

### 1.4 数据可行性结论

- **L1/L1a 可实现**：持仓、自选、PIT 日线、复权因子、交易日、ST/停牌/涨跌停、board-lot 与 TDX quote 均已有本地 authority 或纯实现，不需要新表或荐股模块先成熟。
- **L2 已完成一次正式离线审计、未得到可用模型**：canonical-v2 日频/QE 候选足以构造完整 synthetic episode；正式结果为 Ridge `NEGATIVE`、GBDT `INCONCLUSIVE`、study `INCONCLUSIVE`，没有 selected model。该结论不削弱 L1/L1a，也不支持 L3。
- **分钟执行研究管线已经实现，真实 action-card 样本尚未成熟**：最新 candidate 已覆盖至 2026-08-31，本次没有重建分钟数据；历史 `EVID-L4B1-FIRST-AUDIT` 的双方向 family 已由可达性复核取代。修订后的 `EVID-L4B1-FORMAL` 只接受 risk-exit `EXIT/SELL/AT_OPEN` card，并确认当前两个生产 `CARD_ISSUED` 都是 HOLD、0 eligible；不使用 synthetic/QE/Selection 方向补齐。第一阶段盘中提醒只用 quote，不受该研究样本状态影响。
- **日频核心可在 HMM 缺席时启动设计与实现**：现有 raw/复权日线和 QE 导出是核心起点，但下一轮仍需冻结实际覆盖。daily_basic、moneyflow、sector_data、cyq_perf、事件仅是增强候选；“存在表/字段”不代表全历史可用或 PIT 成立。584 个可用因子目录与横截面 IC 均不是择时证据。
- **多智能体适合可选解释，不适合直接掌权**：现有服务不能直接嵌入。事件通过因果验证后可作为模型增强信息；Agent 摘要只解释已绑定事实，不独立改变 action、数量或触发价，也不把多个 Agent 的一致意见当多份独立证据。
- **盘中新方向尚无依据**：现有 N3 不能回答个股执行择时，也不能支持 L4b-2；后者保持范围外。

## 2. Scope / 范围

### 2.1 第一批可运行交付

1. L1：每日规则行动卡，覆盖唯一持仓账本与显式选择的已确认自选。
2. 轻量 analysis scope：只负责从 confirmed watchlist 选择实际出卡标的；全部真实持仓始终覆盖。
3. L1a：前端打开期间的分钟级批量报价轮询与到价提示；只消费 L1 冻结方向、规模和触发价。
4. timing-owned 不可变卡片 artifact 与 append-only 事件：`CARD_ISSUED`、`ALERT_EMISSION_AUTHORIZED`、`OUTCOME_EVALUATED`。
5. outcome 物化与证据页面，持续积累 candidate 对 do-nothing 的 prospective 配对结果。
6. 首发当时只冻结 L2 的人口、抽样、字段、模型、政策与统计分类契约；后续 `PT-NEXT-002` 已在不改变首发运行面的独立离线入口中完成。

### 2.2 后续范围

- L2：`PT-NEXT-002` 已完成一次预注册 Ridge + GBDT 可学性审计；结果见 `EVID-L2-FORMAL`，不再追加同 family 搜索。
- L3：当前没有 `SUPPORTED` 模型，未实现；§6.6 已设计下一任务的本地模型推断与支持态切换，不再等以后另建模型平台。只有适用 objective 与实际服务政策获得支持，才标 `MODEL_ASSISTED`。
- L4b-1：`PT-NEXT-003` 在独立离线管线中研究同方向、同规模、同 horizon 下的一个冻结分钟执行窗口；无足够真实 action-card 时返回 typed insufficient-data 终态，不反向阻塞 L1/L1a。
- **下一优先任务 `PT-NEXT-004`（只设计、未启动）**：核心特征、前向训练评价、本地模型 artifact、特定个股推断、统一方向/规模建议、既有到价提醒与连续路径评价一起交付；保留入场/退出两个监督目标，按同一现金持仓状态合成唯一建议。无支持结果仍交付明确标注的实验分析及 L1 正式卡，不伪装 alpha。
- **随后 `PT-NEXT-005`（只设计、未启动）**：基于已有日频闭环强化离线分钟执行回放、延迟/价差/未成交敏感性与可选信息块增量，不做运行时分钟新方向。
- HMM 增量：在其 timestamp-causal 状态概率、停留期和转换特征有稳定 artifact 后，才作为 `PT-NEXT-004` 之后的一个冻结 feature-block 假设；不等待它，不只用硬标签取代概率，不做四路消融。
- 多智能体解释：只在结构化事件与 provenance 契约闭合后复用为可选解释层；不直接调用当前有副作用的股票分析入口，不让 LLM 自由文本进入训练特征或运行决策。
- L4b-2：第二阶段盘中新方向；只有独立证据证明逐腿成本后正下界时才进入设计与实现。

## 3. Non-goals / 非目标

1. 不选股、不替代荐股模块，不要求荐股模块先成熟。
2. 不自动下单，不生成订单、StrategyPackage、自动执行权重或 MiniQMT/OMS 输入。此限制不禁止 timing 自有本地模型 artifact、人工建议数量和每日推断；当前 v1 audit 没有运行模型是事实，不是未来的永久禁令。
3. 不接 SmartMonitor 的任务库、engine、`auto_trade` 或 trade 记录。
4. 日频运行面不读取分钟 K 线、不从分钟数据生成方向；允许显式离线使用已有分钟导出验证日频计划的触发/成交假设，不调用 TDX 分钟历史链路。
5. 不写 QE、Selection、Advisory、Watchlist、Legacy Portfolio、Paper v2、MiniQMT 的既有表、artifact、registry、route 或调度状态。
6. 不新建数据库表，不注册既有调度器，不新建 worker，不控制进程。
7. 不引入 SSE、WebSocket、外部消息通道、TCN、Transformer、Offline RL、DeepLOB 或 HMM 四路消融。
8. 不把 L1 规则卡包装成已验证 alpha，不把研究级统计量包装成个股置信度。
9. 不把 sealed holdout、MDE、最低成交额或人工复核变成第一批、L2 或 L3 的批准门禁。
10. `PT-NEXT-004` 不扫描全部 584 个因子，不做阈值网格、模型动物园、自动特征生成、深度模型或按回测收益事后回选。
11. `PT-NEXT-004` 不改写 L1 v1 默认规则及历史卡片、L2 v1/L4b-1 artifact，不写全局 N0 registry/current route；新模型/政策仅在 timing 自有版本化路径中实现并接入人工建议。
12. 不把入场和退出监督目标混成含义不明的分数；允许显式换算为同资金尺度的动作净价值后，在唯一政策中决策，禁止同股同时给出互斥买卖建议。

## 4. Architecture / 整体架构

### 4.1 轻量独立产品边界

`position_timing` 是独立 namespace 与 artifact owner，但不是第二套研究平台：

```text
Legacy Portfolio ────────────────┐
Active Watchlist ─> scope filter ┼─> position_timing service ─> immutable cards/events ─> position-timing page ─> human
Daily/PIT data ──────────────────┤             │                         │
guard pure APIs ─────────────────┤             └─ realtime quote poll ───┘
optional context ────────────────┘

QE / Selection / Advisory / Paper / MiniQMT  <── no reverse dependency, no timing write
global N0 registry / current_route           <── read-only background, zero write
```

边界选择兼顾两件事：独立 namespace、registry 与 artifact 防止污染既有模块；成本、交易日、涨跌停、PIT、guard 和板块交易单位仍调用现有纯实现或权威服务，避免复制算法。

### 4.2 最小实现面

第一批只使用一个服务包、一个路由和一个页面；文件是职责映射而非强制拆分数量。后续分析范围管理继续修改这些既有文件，不增加新服务文件或页面：

```text
backend/services/position_timing/
    contracts.py          # card, intent, event, API DTO
    policy.py             # guard snapshots, componentized cost policy, deterministic mapping
    artifact_store.py     # immutable cards, append-only events, atomic idempotency/coverage state
    service.py            # universe, L1 card, outcome materialization
    alerts.py             # quote eligibility and atomic alert claim
backend/routers/position_timing.py
frontend/src/app/position-timing/layout.tsx  # only imports the existing paper-v2 visual tokens
frontend/src/app/position-timing/page.tsx
backend/tests/position_timing/
frontend/tests/position-timing/
```

仅允许两处现有 composition root 做薄接线：`backend/main.py` 注册 router（统一 `/api/v1` 前缀），`frontend/src/lib/navigation/nav-groups.ts` 增加页面入口。新路由的 `layout.tsx` 只导入既有 `paper-v2.css`，不复制样式或承载业务逻辑。两处既有接线文件不得包含择时业务逻辑；除 composition root 外，任何既有业务模块不得 import `position_timing`。

已完成 L2 v1 新增 `backend/services/position_timing/learnability_pipeline.py`，是显式 CLI 的 request/materialize/CPCV/inference/bundle 入口，没有 final-fit/runtime model；这是历史实施边界。研究代码留在 timing namespace，只复用 `AdvisoryResearchTrialRegistryV1` 的纯 append/identity，实现写自有路径，绝不写全局 registry/current route。

`PT-NEXT-004` 仍只有一个服务包、现有 router 和页面。离线 CLI 负责训练，本地 inference 由现有日频物化入口调用；允许按实际职责增加少量 `features.py`、predictor/decision 模块，避免把所有功能继续塞入现有大文件。离线与运行态共享纯特征/决策实现，不把整套训练管线 import 到 API；不建 feature store、模型服务、独立数据库、调度平台或训练 API。文件数不是低复杂度的验收指标，职责清楚、单一 authority 与直接测试才是。

### 4.3 数据和基础设施复用

| 能力 | 复用方式 |
|---|---|
| 持仓 | 第一阶段唯一 authority 为 `portfolio_manager.get_all_stocks()` / `app.portfolio_stocks`，只读 |
| 已确认自选 | 只读 watchlist；`advisory_enabled=true` 且 `lifecycle_status` 属于 `CANDIDATE/ENTERED/HOLDING` |
| 交易日 | `TradingCalendarStatusService`、`TradeCalendarProvider` |
| PIT 股票身份与日频行情 | L1 标的身份由 holdings/watchlist 与公共 symbol validator 决定，不把研究 universe 当产品准入门；raw 日线读取本地权威服务。历史 L2 才绑定 `aistock_equity_pit_canonical_v2` / `shsz_a_252td_st_delist_asof_v2` 与 QE 导出 manifest/H5/Parquet |
| 已确认终止上市 | 只读 `market.event_signal` 中符合 `issuer_bound_stock_delisting_v2` 的 timestamp-causal confirmed event；`market.stock_basic` 只在 `list_status=D AND delist_date <= decision_trade_date` 时作为已生效终态兜底；研究态 event overlay 不接入 runtime |
| 双价格/公司行动 | raw 使用 `market.kline_daily_raw`；跨日经济值复用 `market.adj_factor` / `AdjFactorProvider` 或已绑定同源 factor 的 Qlib 导出，按 `EVID-PRICE-BASIS` fail closed |
| 涨跌停、停牌、ST | `a_share_live_limit_rule.py` 与现有 daily-limit/suspend authority |
| 风险与价格规则 | 只调用 `trading_core.exit_guard.evaluate`、`trading_core.price_guard.evaluate` |
| 手数 | `execution_algos.board_lot.board_lot_rule/round_to_board_lot` |
| 实时报价 | TDX batch quote，按 `EVID-TDX-CONTRACT` 校验 |
| HMM/市场态势 | 只读、可缺失、仅 context |
| 择时专用日频特征 | `PT-NEXT-004` 以 raw/复权日线等可核验核心为起点；其余数据逐块验证覆盖与历史可见版本，落 timing-owned immutable snapshot；不回写源表 |
| 多智能体股票分析 | 只复用结构化解释思路与 UI 展示方式；当前有副作用 POST、自由文本 response、新闻/公告抓取链路均不直接调用 |
| 研究证据 | 只读 N 线 receipt、QE 导出与 N0 历史计数；择时另有 registry |

数据优先级按用途冻结，禁止静默换源：

1. 当前用户状态只取 legacy portfolio、active watchlist、timing analysis scope 与 timing intent。
2. T 日产品卡的价格、ST、停牌和 limit 只取本地 PIT/daily authority；缺失即 typed unavailable，不用实时 T+1 报价倒填 T 日特征。
3. 历史 L2 优先消费带 manifest/hash 的 QE/Qlib/H5/Parquet 导出；如需用本地数据库补数据，先导出新的 immutable dataset identity，再进入同一次研究，不把 DB “最新值”直接混入旧 bundle。
4. T+1 运行观察只取绑定的 TDX batch quote；它不反向改写日频数据、card 或历史 dataset。
5. `PT-NEXT-004` 的 feature snapshot 必须绑定实际字段顺序、as-of/available-at、source manifest/hash 与缺失状态；因子库目录只做候选发现，不能替代实际数据覆盖或 PIT 验证。

### 4.4 隔离矩阵

| 模块 | timing 可读 | timing 可写 | 反向依赖 |
|---|---|---|---|
| QE / Qlib exports | 已冻结 dataset、manifest、日频数据 | 否 | 禁止 |
| Selection | 可选排名上下文 | 否 | 禁止 |
| Advisory N 线 | 纯实现、receipt、N0 历史计数 | **全局 registry/current_route 零写入** | 禁止 |
| Watchlist | 已确认 active rows | 否 | 禁止 |
| Legacy Portfolio | 第一阶段唯一持仓 authority | 否 | 禁止 |
| Paper v2 | 仅未来显式 `PAPER_V2_PREVIEW` 独立运行 | 否 | 禁止 |
| MiniQMT | 可选只读 daily-limit authority | 否；不读其持仓、不下单 | 禁止 |
| SmartMonitor | UI 风格可参考 | 否；不调用 engine/task/trade | 禁止 |
| TDX | L1a 批量报价只读 | 否 | 禁止 |
| market.event_signal | `PT-NEXT-004` 只读 issuer-bound、timestamp-causal 有限字段 | 否；不回写事件或分析结果 | 禁止 |
| 多智能体股票分析 | 只参考解释 schema/UI；当前服务入口不调用 | 否；不写 `app.analysis_records` | 禁止 |

同一运行只能选择一个 `position_source`。第一阶段正式模式固定为 `LEGACY_PORTFOLIO`；`PAPER_V2_PREVIEW` 若后续实现，必须使用独立 card set 和 artifact identity，绝不与 legacy 或 MiniQMT 持仓拼账。

### 4.5 六类 HARD 约束

1. **因果与新鲜度**：所有市场/特征输入满足 `feature_available_at <= decision_as_of`；报价陈旧、未来戳或源失败时禁止弹窗并返回 typed 状态。
2. **控制面隔离**：零既有表写入、零 DDL、零既有调度注册、零进程控制、N0 registry/current_route 零写入；不调用会写 `app.analysis_records` 的现有多智能体入口；除 backend/frontend composition root 的路由与导航接线外，既有业务模块不得反向依赖 timing。
3. **账本与身份唯一**：一次 card set 只有一个持仓 authority；canonical symbol 去重；card artifact 不可变并绑定全部输入与 policy hash。
4. **共享实现不漂移**：不修改 guard 默认值；timing 使用显式、版本化、hash-bound snapshot 调用同一 `evaluate` 实现。
5. **无交易副作用**：不产出订单、经纪商部署包或自动执行输入；允许 timing 自有本地模型和明确标注的人工仓位建议，绝不写 Paper/MiniQMT 交易账本。
6. **证据完整性**：事件 append-only；同一幂等键最多一条；物化缺失、报价失败和字段缺失不得伪装成零结果或成功。研究特征/模型必须绑定 objective、feature order、source、PIT、code 与环境 identity，不能把自由文本或横截面 IC 冒充择时证据。

六类之外的 MDE、sealed holdout、成本敏感性、小额成交和 UI 弹窗去重均为报告、范围或 advisory，不得升级为批准门禁。

## 5. Contracts / 核心契约

§5 记录已实现的 v1 合约；共有的隔离、数量/价格/费用和事件完整性约束继续适用。§6.6 明确列出的 v2 决策时钟、方向生成、模型/正式卡版本及连续研究评价是未来增量，不将其倒写为当前 API 已具备的行为。

### 5.1 决策时钟与 PIT

本节原有条款描述已实现 v1；未来 v2 采用 §6.6.2 的独立晚间 cutoff/版本，不追溯改变 v1 的 15:00 时钟。晚间数据必须证明当时已可得，不能仅凭 trade_date 放宽 PIT。

- T 日收盘后生成服务 T+1 的卡片；`decision_as_of = T 15:00:00 Asia/Shanghai`。
- `created_at` 可以晚于 15:00；市场、公告、Selection、HMM 等特征仍不得使用 `available_at > decision_as_of` 的内容。
- 用户持仓与意图另记 `position_snapshot_as_of` / `intent_snapshot_as_of`，只描述用户状态，不得作为晚到市场特征绕过 PIT。
- T+1 盘中只评价已冻结 card，不刷新方向、目标仓位或触发价。
- intent 在 card 首次签发后不创建或改写同一 decision date 的 card；新 intent 正式进入下一交易日。它可以立即使旧提醒失效：盘中若 legacy 持仓 hash 或 intent hash 已不同于 card snapshot，返回 `POSITION_SNAPSHOT_CHANGED/INTENT_SNAPSHOT_CHANGED` 并禁止旧卡弹窗，等待下一张卡重新决策。
- card 在 T+1 收盘失效。T+1 停牌、T+1 不可卖或方向性一字板记 `POLICY_FILL_UNAVAILABLE_EXPIRED`，不得把旧卡顺延到 T+2；T+1 收盘后由新卡重新决策。
- 若 suspend authority 证明标的在 T 日停牌且不存在 T 日 bar，card 只能使用 T 日之前最近一根可执行 raw close，并显式记 `DECISION_DAY_SUSPENDED_USING_LAST_EXECUTABLE_CLOSE`；非停牌标的的旧 bar 不得冒充 T 日成熟数据，也不得因此提前冻结 unavailable card set。
- 第一阶段不调用 `fetch_minute_kline_tdx`、`TdxCausalMinuteProvider` 或任何分钟 feature builder。

### 5.2 Universe、用户意图与去重

`PositionTimingUniverseV1 = holdings ∪ confirmed_watchlist`：

1. holdings 从 `LEGACY_PORTFOLIO` 读取，数量与成本以该账本为准。
2. confirmed watchlist 定义为 `advisory_enabled=true` 且 lifecycle 为 `CANDIDATE/ENTERED/HOLDING`；`EXITED` 排除。
3. 统一 canonical symbol 后去重。若同时是持仓与自选，`HOLDING` 身份优先，自选只追加 `source_provenance`，不得生成两张卡。
4. 未识别代码、BJ 或非 SH/SZ 标的第一阶段返回 `UNSUPPORTED_SYMBOL/UNAVAILABLE`，不得默认套 100 股或 10% 涨跌幅。
5. `PositionTimingIntentV1` 为 timing-owned 用户输入，至少含 `canonical_symbol`、`planned_full_notional_cny`、`desired_target_exposure` 与更新时间。允许 exposure 为 `{0, 0.25, 0.50, 1.00}`。
6. 持仓缺少 intent 时默认目标等于当前持仓，仅给风险型 `HOLD/EXIT`，并以 `pre_action_qty × reference_price_raw` 记录当卡的 `planned_full_notional_cny`；非持仓自选缺少 sizing intent 时仍生成 `WAIT` 卡并返回 `SIZING_INPUT_UNAVAILABLE`，不得伪造默认仓位，也不影响其他股票出卡。

#### 5.2.1 轻量分析范围管理（已实现并合入，见 §9.3）

历史块一曾把全部 confirmed watchlist 纳入出卡集合；已合入的首发块二在不改变持仓 authority 的前提下，把“候选发现”和“实际择时分析”分开：

```text
PositionTimingDiscoveryUniverseV1 = holdings ∪ confirmed_watchlist
PositionTimingAnalysisUniverseV1  = holdings ∪ (confirmed_watchlist ∩ explicitly_selected_watchlist)
```

1. `PositionTimingUniverseV1` 保留为块一历史契约名，语义等同 discovery universe；它继续用于展示可选择股票、校验代码和读取 intent，不再等同于实际出卡集合。
2. 全部 `LEGACY_PORTFOLIO` 持仓始终进入 analysis universe。这是风险建议覆盖，不是第二个持仓池，也不允许 scope 记录覆盖数量、成本或持仓身份。
3. 仅自选标的只有在当前仍满足 confirmed watchlist 条件且被用户显式选择时才进入 analysis universe。没有 scope 状态时默认为 `NOT_SELECTED`，不生成该标的行动卡；这是显式 opt-in，不是数据失败或审批门禁。
4. 新增一个 timing-owned `PositionTimingAnalysisScopeV1` 当前态，字段只包含 `schema_version`、排序去重后的 `selected_watchlist_symbols`、timezone-aware `updated_at` 与 `scope_sha256`；hash 由除自身外的三个字段计算。它保存于既有 artifact root 的 `analysis_scope/current.json`；PUT 在同一文件锁内完成 read-modify-write 与原子替换，相同请求不更新时间也不重写。不建表、不写 Watchlist/Portfolio、不追加新事件、不另建 registry。
5. 未初始化 scope 使用确定性的 `EMPTY_EXPLICIT_SCOPE_V1` identity，等价于空的自选选择集；不得把全部 watchlist 当作静默兼容默认值。scope 文件损坏或 hash 不一致返回 `ANALYSIS_SCOPE_INVALID`，不得将其误读为空集合。
6. scope 中已经选择、但当前不再满足 watchlist lifecycle/advisory 条件的代码保留在用户当前态中，effective analysis 为 false；`GET /intents` 顶层 `scope_warnings[]` 返回 `SELECTED_SOURCE_INELIGIBLE` 与 canonical symbol。不得自动删除，也不得绕过 watchlist authority 出卡；重新满足条件后可恢复生效。
7. canonical symbol 同时成为持仓时仍按 `HOLDING` 身份唯一出卡；scope 只保留来源意图，不能生成第二张卡。对持仓请求关闭分析返回 typed `HOLDING_ALWAYS_INCLUDED`，不写一份虚假的 disabled 状态。
8. scope 更新只影响下一次尚未签发的 card set。已经签发的 card、当日 alert eligibility、后续 outcome 与历史事件保持不可变；API 返回 `effective_card_policy=NEXT_CARD_SET_ONLY`。
9. card set 的 `input_identity` 保留 `universe_identity_sha256` 并令其明确等于 effective analysis universe identity，同时嵌入完整 canonical `analysis_scope_snapshot`，并含 `discovery_universe_identity_sha256`、`analysis_scope_snapshot_sha256` 与同值别名 `analysis_universe_identity_sha256`。旧 scope 当前态被改写后，历史 card set 仍能回读当时选择；现有 `PositionTimingCardSetV1` 的 content hash 已覆盖 `input_identity`，不另建 scope 平台。
10. 不设置最大选择数、最低样本数、审批或人工放行。范围越大只影响页面信息量与只读计算量，不构成业务准入门禁。

API/UI 只做最小扩展：在现有 `GET /intents` 每行增加 `analysis_selected`、`analysis_effective`、`analysis_locked`、`analysis_reason_code`，顶层 `scope_warnings[]` 只列 scope 当前态中来源已失效的有限 symbol/reason 集合；GET 与缺省 scope 解析均保持零写入。新增唯一写接口 `PUT /api/v1/position-timing/analysis-scope/{symbol}`，请求体仅为 `analysis_enabled: bool`，响应返回 `UPDATED/UNCHANGED`、effective 状态、scope hash 与 `effective_card_policy=NEXT_CARD_SET_ONLY`。启用只接受当前 confirmed watchlist；取消允许作用于 scope 中已经存在但来源已失效的代码；持仓行显示“持仓始终分析”且不可关闭。不开新页面、不提供标签、分组、排序规则、批量工作流、虚拟持仓、组合编辑器或第二套通知设置。

集合与写入真值表冻结如下：

| 当前持仓 | active confirmed watchlist | scope selected | effective analysis | PUT 关闭/启用语义 |
|---|---|---|---|---|
| 是 | 任意 | 任意 | 是，`HOLDING_ALWAYS_INCLUDED` | 关闭返回同名 typed 状态且零写；启用为 `UNCHANGED` |
| 否 | 是 | 是 | 是，`SELECTED` | 可幂等关闭 |
| 否 | 是 | 否或未初始化 | 否，`NOT_SELECTED` | 可幂等启用 |
| 否 | 否 | 是（历史残留） | 否，`SELECTED_SOURCE_INELIGIBLE` | 允许关闭；禁止重新启用 |
| 否 | 否 | 否 | 不在 discovery/analysis universe | 启用返回现有 `SYMBOL_OUTSIDE_TIMING_UNIVERSE`，零写 |

所谓“专用持仓股票池”本轮明确不实现。若未来需要录入假设成本和数量，必须作为独立 `MANUAL_TIMING_PREVIEW` position source、独立 card set/artifact identity 另行设计，绝不能与 `LEGACY_PORTFOLIO` 拼账；普通“我想分析这只股票”应先进入现有 confirmed watchlist，再由 analysis scope 选择。

### 5.3 行动卡

`PositionTimingCardV1` 至少冻结：

| 字段组 | 必备字段 |
|---|---|
| 身份 | `card_id`、`card_set_id`、`canonical_symbol`、`primary_source_role`、`source_roles`、`position_source` |
| 时钟 | `decision_trade_date`、`decision_as_of`、`target_trade_date`、`valid_until` |
| 当前与目标 | `pre_action_qty`、`pre_action_exposure`、`planned_full_notional_cny`、`desired_target_exposure`、`requested_delta_qty`、`requested_leg_notional_cny` |
| 建议 | `action=OPEN/ADD/HOLD/REDUCE/EXIT/WAIT/UNAVAILABLE`、`execution_window=AT_OPEN/ON_PRICE_TRIGGER/WAIT_UNAVAILABLE` |
| 触发 | `triggers[]`；每项含 `trigger_id`、side/operator/raw price、共享 guard action/reason 条件、分支对应的 `planned_delta_qty`、`planned_leg_notional_cny` 与合法 target exposure；另含 `reference_price_raw` |
| 可执行性 | `tradability_status`、`st_flag`、`t1_sellable_qty`、`limit_up_raw`、`limit_down_raw`、typed reason codes |
| 成本 | 买卖逐腿估算、parent-order 情景、`SMALL_TRADE_COST_HEAVY`、cost policy identity |
| 上下文 | `holding_trading_days`、`holding_age_bucket`、`market_regime=DOWN/UP_OR_FLAT/UNKNOWN`，已确认退市 flag/status，以及 Selection/HMM status 与 evidence ref；缺失必须 typed |
| 证据 | `evidence_tier`、`historical_base_rate_status`；L1 固定 `RULE_BASED_RISK_MANAGEMENT` |
| 可复现性 | dataset、calendar、limit、delist、intent、guard snapshot、cost policy、code commit 的 hash/provenance；card set 同时保存完整 input/policy identity 与 `cards_sha256` |

卡片本身不显示 MDE/oracle 比值、L2 总体置信区间或“个股胜率”。这些研究级结论只在页面证据区展示。

`holding_age_bucket` 只用于展示与 deployment weighting，边界冻结为 `AGE_0/AGE_1_3/AGE_4_5/AGE_6_10/AGE_11_20/AGE_21_PLUS/UNKNOWN`；`market_regime` 使用与 L2 相同的 benchmark 日频规则，不使用 HMM state。二者不得因样本结果事后改桶。

### 5.4 L1 决策映射

L1 只做确定性风险与执行映射，不从历史 PnL 回选规则：

1. 用户 intent 决定希望向哪个 exposure 移动；Selection 不决定 universe，也不替用户发起新股票方向。
2. 持仓先调用冻结的 `ExitGuardPolicy`。硬止损、可用的 alpha decay 或 timestamp-causal 已确认终止上市事实可把用户目标覆盖为 `EXIT`；仅自选标的命中已确认终止上市时为 `CONFIRMED_DELISTING_BUY_UNAVAILABLE`。Selection 缺失时 alpha-decay 不运行，不能用默认排名代替；T+1 不可卖时为 `WAIT_UNAVAILABLE`。
3. T 日只使用已知的 signal close、limit 与 snapshot 生成有限个条件分支；已验证的 T 日停牌按 §5.1 使用更早的最近可执行 close。T+1 报价到来后才调用同一个冻结 `PriceGuardPolicy.evaluate`，并以 evaluator 的 action/reason 在卡片内选择唯一分支；不能把相互重叠的价格上界单独解释为多个同时成立的建议。buy-side guard 的 `REDUCE` 分支表示缩小本次 `OPEN/ADD` 数量，不表示卖出现有持仓。
4. 冲突顺序固定为：方向性可执行性 > exit 风险 > 用户目标移动 > price guard 规模调整。
5. `OPEN/ADD` 卡预先冻结 green/yellow/skip 对应的 trigger 与合法数量，使用 `ON_PRICE_TRIGGER`；风险型 `EXIT` 使用 `AT_OPEN`，非风险型 `REDUCE/EXIT` 可使用 `ON_PRICE_TRIGGER`；`HOLD/WAIT/UNAVAILABLE` 使用 `WAIT_UNAVAILABLE` 并给出 no-trade/原因码。
6. T+1 runtime 只选择已冻结 trigger branch 或返回不可执行，不能创建新方向、新价格阈值或新规模；因此 L1a 是提醒器而不是第二个决策器。
7. Selection 缺失时禁用本次 alpha-decay 分支并标 `selection_context_status=UNAVAILABLE`，硬止损和用户意图仍可生成；HMM 缺失不改变方向。

`HMMContextV1` 可同时携带市场态势与板块轮动摘要、`as_of`、source artifact/hash 和 `hmm_context_status=AVAILABLE/UNAVAILABLE/NOT_APPLICABLE`。第一阶段只展示该字段；它不改变 action、target、trigger 或整卡 status。L2 v1 feature vector 也不含 HMM；若未来要验证其增量，只能在更新本设计后作为一个冻结 feature block 假设，而不是四路消融。

### 5.5 冻结 guard snapshot

v1 guard 运行 authority 唯一为共享 `evaluate`，但每张卡绑定 timing-owned 显式快照，禁止依赖未来会变化的 default factory；v2 模型决定候选动作价值，guard/执行映射仍只用同一纯实现。

`PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1`：

- 通用：`contract=execution_price_guard_v1`、`enabled=true`、`mode=rule_v1`、`price_basis=raw`、`guidance_status=rule_default`。
- signal reference：buy/sell=`signal_close`，intraday=`arrival_price`。
- buy：`max_open_gap_bps=300`、`yellow_open_gap_bps=150`、`yellow_size_multiplier=0.5`、`max_chase_bps=100`、`yellow_chase_bps=50`、`near_limit_up_skip_bps=80`、`allow_partial=true`。
- breakout addon：`enabled=false`、`require_momentum_regime=true`、`min_score_bucket=top5`、`dist_to_limit_up_lt_bps=200`、`min_volume_ratio_open=1.5`、`add_size_multiplier=0.5`、`min_fill_probability=0.6`。
- sell：`rebalance_max_slippage_bps=150`、`risk_exit_max_slippage_bps=500`、`near_limit_down_rebalance_skip_bps=80`、`allow_partial=true`。

`EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1`：

- 通用：`contract=exit_guard_v1`、`enabled=true`、`mode=rule_v1`、`price_basis=raw`、`t1_handling=defer_to_next_tradable_day`、`guidance_status=rule_default`。
- stop loss：`enabled=true`、`max_loss_bps=600`、`soft_loss_bps=400`、`volatility_multiple=2.5`、`reference=actual_entry_cost`。
- take profit：`enabled=false`、`take_profit_bps=1200`、`trailing_stop_bps=500`。
- alpha decay：`enabled=true`、`rank_drop_below=top40%`、`confirm_days=2`。
- time stop：`enabled=false`、`max_holding_days=10`。

当前 `evaluate` 未必消费 snapshot 中每个保留字段；字段完整序列化用于防漂移，不得据字段名宣称不存在的运行效果。

snapshot 中共享 `t1_handling=defer_to_next_tradable_day` 只描述 guard 在当日不可卖时返回 defer 语义；position-timing 将其映射为本卡 `WAIT_UNAVAILABLE/POLICY_FILL_UNAVAILABLE_EXPIRED`，由下一交易日的新卡重新评价，绝不携带旧 card。这保留共享 guard 的返回含义，同时服从本产品 T+1 单日有效期。

快照级 provenance 固定含：`source_module`、`source_symbol`、`source_repository_commit=f870debe3b963d9d3d41ce9663db9722af921e80`、`source_captured_at`、`source_defaults_sha256`、`timing_policy_sha256`。未来共享默认值变化只能创建 snapshot v2，历史卡继续绑定 v1。

`entry_guard_decision` 的 `FIXED_*` / `FROZEN_DYNAMIC` 仅作 `EVID-ENTRY-*` 研究证据，不叠加为第二套运行时 guard。

### 5.6 双价格与公司行动

价格用途必须分离：决策触发、涨跌停判断、模拟成交与费用一律使用 raw CNY；跨日收益、趋势和经济结果使用带明确 identity 的 total-return/复权口径。不得用 raw 价格比直接跨越除权除息日，也不得在 adjustment factor 缺失时默认为 `1.0`。

| 用途 | 冻结口径 |
|---|---|
| card 参考价、trigger、limit、quote | raw CNY |
| 成交名义金额与逐腿费用 | raw CNY × 当时合法数量 |
| T 日跨日特征 | `available_at <= decision_as_of` 的 qfq/total-return source identity |
| prospective/L2 经济结果 | raw fill 加可复现的公司行动数量/现金流路径，或与其等价且 hash-bound 的 total-return valuation |

`OUTCOME_EVALUATED` 绑定用于该 horizon 的 corporate-action/adjustment source、版本、覆盖区间与 hash。该信息属于事后标签，可在 outcome materialization 时使用当时已成熟的权威数据，但不得反向进入旧 card。缺失、版本冲突或无法把 raw fill 与 terminal valuation 对齐时记 `UNAVAILABLE_AT_HORIZON`。

块一日频卡不计算跨日收益，因而 card 的 `adjustment_identity` 固定为 `NOT_APPLICABLE / BLOCK_ONE_CARD_USES_RAW_PRICE_ONLY`；复权因子缺失不得阻塞 L1 出卡。只有实现块二的 outcome 评价才读取并强制绑定 adjustment/corporate-action identity。

### 5.7 可交易性、ST 与交易单位

- ST 是涨跌幅比例与风险属性，不等于不可交易。
- 一字涨停只阻断买入；一字跌停只阻断卖出。相反方向不得被笼统判 `UNAVAILABLE`。
- T 日已验证停牌且有更早可执行 close 时保留风险方向并要求 T+1 重验；连最近可执行 close 也缺失、涨跌停权威缺失或 T+1 可卖数量不足时返回独立 typed reason。
- 买入用 `round_to_board_lot(..., side="BUY")` 向下取合法数量。
- 任一板块卖出全部剩余持仓时可按实际剩余数量一次退出；否则沪深主板/创业板买入与部分卖出按 100 股倍数。
- 科创板买入与部分卖出至少 200 股，超过 200 后可按 1 股递增；不足 200 的余额只通过上述全量退出处理。
- `SMALL_TRADE_COST_HEAVY` 使用手数处理后的预计实际逐腿成交额判定；满仓金额档位只作预筛。

交易单位依据为[深交所交易规则（2026 年修订）](https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf)、[上交所交易规则（2026 年修订）](https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/c_20260424_10816482.shtml)及[上交所科创板交易规则说明](https://edu.sse.com.cn/tib/ysptj/c/4869120.shtml)。

### 5.8 分项成本政策

冻结 `PERSONAL_MANUAL_COMPONENT_COST_V1`：

```text
net_commission_rate        = 0.000085   # 0.85 bps，双边
minimum_commission_cny     = 5
transfer_fee_rate          = 0.000010   # 0.10 bps，双边
regulatory_fee_rate        = 0.000020   # 0.20 bps，双边
handling_fee_rate          = 0.0000341  # 0.341 bps，双边
stamp_duty_sell_rate       = 0.000500   # 5.00 bps，仅卖出
commission_quote_basis     = NET_EX_REGULATORY_FEES
min_commission_scope       = PER_PARENT_ORDER
min_commission_scope_verification = BROKER_UNVERIFIED
assumed_parent_order_count = 1
```

对每个用户实际提交的父订单 `j`：

```text
commission_j = max(5, 0.000085 × notional_j)
common_regulatory_j = 0.0000641 × notional_j
buy_cost_j  = commission_j + common_regulatory_j
sell_cost_j = commission_j + common_regulatory_j + 0.0005 × notional_j
```

不触发最低佣金时，买入单边 `1.491 bps`，卖出单边 `6.491 bps`，等名义金额完整往返 `7.982 bps`。已持仓股票的历史买入腿是沉没成本，EXIT 边际比较只计未来实际腿；只有政策确实形成“卖出 + 再买回”循环时才使用完整往返尺度。

`FeeModel` 只复用 parent-order 多 fill 聚合身份，不复用其数值公式。把 `1.491/6.491 bps` 直接填入现有 `FeeModel` 会让 5 元下限覆盖规费，产生错误拐点。timing 必须在自身 `policy.py` 分项计算；只有 Paper/LocalSim 也绑定同一 componentized policy identity 时，结果才可直接数值比较。

冻结字段还包括 `fee_schedule_as_of=2026-09-03`、`fee_source_refs`、`cost_policy_version`、`cost_policy_sha256`。净佣与 5 元下限来自用户账户报价；真实券商对父订单/部分成交的结算归集仍披露 `BROKER_UNVERIFIED`，不阻塞设计或建议，也不得宣称估算就是最终交割费用。卡片/UI 固定标注“按单一委托估算”，并提供下述 2/3 个父订单敏感性，不把基准口径写成已核券商结算事实。

官方规费来源：[上交所收费一览表（2026 年 1 月）](https://www.sse.com.cn/services/tradingservice/charge/ssecharge/)、[深交所收费及代收税费标准（2026 年 1 月）](https://www.szse.cn/marketServices/deal/payFees/)、[中国结算上海市场收费表](https://www.chinaclear.cn/zdjs/fbzyls/202506/9d22b74d9f2e40edb67b44d1f6596f18/files/%E4%B8%8A%E6%B5%B7%E5%B8%82%E5%9C%BA%E8%AF%81%E5%88%B8%E7%99%BB%E8%AE%B0%E7%BB%93%E7%AE%97%E4%B8%9A%E5%8A%A1%E6%94%B6%E8%B4%B9%E5%8F%8A%E4%BB%A3%E6%94%B6%E7%A8%8E%E8%B4%B9%E4%B8%80%E8%A7%88%E8%A1%A8.pdf)、[中国结算深圳市场收费表](https://www.chinaclear.cn/zdjs/fbzyls/202506/ab6384ba25514554a7eceaee3e521032/files/%E6%B7%B1%E5%9C%B3%E5%B8%82%E5%9C%BA%E8%AF%81%E5%88%B8%E7%99%BB%E8%AE%B0%E7%BB%93%E7%AE%97%E4%B8%9A%E5%8A%A1%E6%94%B6%E8%B4%B9%E5%8F%8A%E4%BB%A3%E6%94%B6%E7%A8%8E%E8%B4%B9%E4%B8%80%E8%A7%88%E8%A1%A8.pdf)、[证券交易印花税减半公告](https://shanghai.chinatax.gov.cn/zcfw/zcfgk/yhs/202308/t468451.html)。

### 5.9 最低佣金拐点与敏感性

阈值不得写成业务常量，必须用 `Decimal` 从组件派生，并只在最终人民币元处向上取整：

```text
planned_full_notional_threshold(f)
  = ceil(minimum_commission_cny / (net_commission_rate × f))
```

| exposure step `f` | 满仓金额基准阈值 |
|---|---:|
| `1.00` | 58,824 元 |
| `0.50` | 117,648 元 |
| `0.25` | 235,295 元 |

`235,295 × 0.25 × 0.000085 = 5.00001875`，而 `235,294` 对应 `4.99999750`；旧值 235,296 是“先 ceil 再乘 4”的双重取整，已废止。

UI 的成本友好档位建议为：满仓金额至少 235,295 元可显示四档；117,648 至 235,294 元优先 `{0, 0.50, 1.00}`；58,824 至 117,647 元优先 `{0, 1.00}`；更低金额仍可给 `{0, 1.00}`，但必报实际成本。该建议不删除用户已经冻结的 exposure intent，也不阻塞卡片。

等名义金额、每腿一个父订单的说明性成本：

| 每腿名义金额 | 买入成本 | 卖出成本 | 往返成本率 |
|---:|---:|---:|---:|
| 5,000 | 5.3205 元 | 7.8205 元 | 26.282 bps |
| 10,000 | 5.6410 元 | 10.6410 元 | 16.282 bps |
| 20,000 | 6.2820 元 | 16.2820 元 | 11.282 bps |
| 30,000 | 6.9230 元 | 21.9230 元 | 9.615 bps |
| 50,000 | 8.2050 元 | 33.2050 元 | 8.282 bps |
| 58,824 及以上 | 按 1.491 bps | 按 6.491 bps | 7.982 bps |

卡片仍输出低于阈值的建议，同时标 `SMALL_TRADE_COST_HEAVY`、绝对费用与“可考虑合并委托”。不得用该标签拦截建议。

每个 receipt 固定报告三个非新增 trial 的费用情景：

- `ONE_PARENT_ORDER_BASE`
- `TWO_PARENT_ORDERS_NEAR_EQUAL_LEGAL_QTY`
- `THREE_PARENT_ORDERS_NEAR_EQUAL_LEGAL_QTY`

三者保持相同总合法数量和方向，按板块 increment 尽量等分；卖出零股尾数只进入最后一个全量退出父订单。若 base 情景为 `SUPPORTED`，但任一拆单情景的 adjusted lower bound 不再为正，只加 `COST_ASSUMPTION_SENSITIVE`，不改变 effect 分类、不构成门禁。

### 5.10 Artifact、事件与幂等

timing 唯一写入根为以下目录（此处列已实现 v1 布局；v2 仅在该根新增 §6.6.4 的版本化子路径）：

```text
F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/
    intents/
    policy_snapshots/
    cards/<decision_trade_date>/<card_set_id>/card_set-<artifact_sha256>.json
    events/<yyyy-mm>.jsonl
    research_registry/timing_trial_registry_v1.jsonl
    materialization_state.json
```

- cards 与 policy snapshots content-addressed、不可变、hash-bound；card set 保存完整 input/policy identity，并用 `cards_sha256` 检出卡片内容篡改。
- event log append-only，使用文件锁和 fsync；不改写旧事件。
- intents 与 `materialization_state.json` 是 timing-owned 当前态，使用临时文件 + 原子替换；它们不冒充 append-only 证据。
- `CARD_ISSUED` 幂等键为 `card_id`。
- `ALERT_EMISSION_AUTHORIZED` 幂等键为 `(card_id, trigger_id)`。
- `OUTCOME_EVALUATED` 幂等键为 `(card_id, horizon_trading_days)`。
- `(position_source, decision_trade_date)` 只有一个 current card set。相同 semantic identity 重试返回原 artifact；首次发布后若同一逻辑键出现不同 input/policy hash，返回 `CARD_SET_IDENTITY_CONFLICT`，不得改写旧卡或静默切换 current 指针。
- POST 副作用只允许落上述 timing-owned 路径；相同 input/policy identity exact-idempotent，不得写 N0 或既有模块。

### 5.11 Outcome 评价

第一批冻结并实现：

```text
evaluation_horizons_trading_days = (1, 3, 5, 10, 20)
primary_horizon_trading_days     = 20
terminal_exit_max_defer_trading_days = 5
```

两只时钟不得混用：

- 行动卡只在 T+1 有效，绝不顺延。
- outcome 的终值在 nominal horizon 缺失、停牌、一字跌停不可卖或对应限价 authority 不可核验时，最多向后找 5 个交易日；这只延长标签终值观察，不延长建议。终值卖出必须绑定该历史区间独立的 `market.stk_limit` identity，不得沿用 T+1 card 的单日 limit identity。

由于 `ON_PRICE_TRIGGER` 可能发生在 T+1 开盘后，outcome 将 `target_trade_date` 计为 holding session 1，并以第 h 个 session 的官方 raw close 作为 terminal valuation 时点，再按 §5.6 纳入公司行动路径；这与现有 N 线 T+20 open 标签价格端点不同，分叉理由是避免 h=1 终值先于盘中触发。若 OPEN/ADD 新增数量在 h=1 仍受 T+1 卖出锁定，terminal liquidation proxy 顺延到首个可卖交易日并记 `DEFERRED_THEN_MATURED/TERMINAL_T1_LOCKED`。日期、价格端点、数量/现金流路径和分叉理由必须写入 receipt。

每个 `(card_id, horizon)` 追加一个 `OUTCOME_EVALUATED`，至少含：

- `policy_fill_status=FILLED/SKIPPED_BY_GUARD/POLICY_FILL_UNAVAILABLE_EXPIRED/NO_ACTION`
- `maturity_status=MATURED/DEFERRED_THEN_MATURED/UNAVAILABLE_AT_HORIZON`
- selected `trigger_id`、`planned_delta_qty`、effective target exposure 与 fill raw price/time policy；无动作时显式为空并给 reason
- nominal/effective terminal trade date、deferred trading days、typed reason
- candidate 与 do-nothing 的数量/现金流路径、逐腿成本、gross/net CNY 与 lift bps
- card、dataset、calendar、limit、board-lot、corporate-action/adjustment、cost-policy hash

T+1 因方向性不可交易而未执行时采用 intention-to-treat：candidate 自该点继续 do-nothing 路径，paired lift 为零并保留 `POLICY_FILL_UNAVAILABLE_EXPIRED`，不得删除失败动作。输入/终值本身无法评价时才是 `UNAVAILABLE_AT_HORIZON`。

paired path 只评价卡片动作造成的边际数量，不重算未受动作影响的共同持仓：

- `planned_delta_qty > 0`：candidate 在冻结 fill 买入该数量并持有到 terminal proxy，do-nothing 不买；candidate 计实际买入腿和 terminal 估算卖出腿。
- `planned_delta_qty < 0`：candidate 在冻结 fill 卖出绝对数量并持有现金，do-nothing 持有同一数量到 terminal proxy 后卖出；两边只计各自未来卖出腿，历史买入腿均为沉没成本。
- `planned_delta_qty = 0`：两条路径相同，lift 为零但仍保留 typed action/fill 状态。
- v1 现金收益固定为 0；分红、送转和拆并股由 §5.6 的公司行动路径进入持股侧，不得遗漏或通过 raw 价格比重复计算。

第一批用 `DAILY_OHLC_CONSERVATIVE_FILL_V1` 物化 prospective policy，而不是用 alert 是否送达决定成交：

- `AT_OPEN` 在方向可交易时取 T+1 raw open；风险退出不因 rebalance 最低卖价而回退。
- buy `price <= trigger` 若 open 已满足则取 open；否则仅当 raw low 证明触价时按冻结 trigger price 成交。
- sell `price >= trigger` 若 open 已满足则取 open；否则仅当 raw high 证明触价时按冻结 trigger price 成交；其他 operator 必须在 outcome policy 中逐项显式映射，禁止笼统交换 high/low。
- 一根日线内若多个规模分支都可能成立但顺序不可辨，选择最小合法成交数量和对 candidate 最不利的允许价格；reason 记 `INTRADAY_SEQUENCE_UNOBSERVED_CONSERVATIVE_FILL`。
- alert event、页面是否打开和轮询是否观察到只衡量 delivery/system observation，不进入该 policy-fill 判定。

该规则只读成熟后的日线 OHLC，不引入分钟信号；若以后以分钟路径替换，必须成为新的 outcome policy version，不能改写旧事件。

读取态分四类：

1. 尚未到 expected maturity：`PENDING_DERIVED`。
2. 已到期但晚于成功扫描水位：`PENDING_MATERIALIZATION`。
3. 已被成功扫描覆盖、应有事件但缺失：`MATERIALIZATION_MISSING`。
4. 已有事件但没有可用终值：事件内 `UNAVAILABLE_AT_HORIZON`。

页面首次打开时调用同一个 exact-idempotent materialize POST，顺带扫描全部到期 key；不建 scheduler。独立 operational state 至少含 `last_successful_materialization_scan_through_trade_date`、`last_run_at`、`expected_due_count`、`accounted_outcome_count`。只有本次范围内全部 due key 都已有唯一 `OUTCOME_EVALUATED`（包括 typed unavailable）才推进水位；数据尚未成熟、计算失败、写入失败或幂等冲突均不推进。

聚合必须同时报告 matured、pending、unavailable、materialization-missing 计数；均值只使用 paired matured。全体 card 的 intention-to-treat 与“至少一个冻结 trigger branch 的 `planned_delta_qty != 0`”的 intervention-intent 子集分开报告，避免大量 `HOLD` 的零 lift 稀释动作效果。未到期表现为“没有事件行”，读取器不得把缺失当作零 lift。

卡片基率只取相同 `primary_source_role + action_side + holding_age_bucket` 的 paired matured intervention-intent 样本，展示 `N`、正 lift 数、正 lift 比例与中位 lift；`N < 30` 时固定 `INSUFFICIENT_HISTORY`。该阈值只控制展示措辞，不阻塞出卡或提醒。

第一批不实现 `actual_user_execution_event`。prospective outcome 衡量冻结政策的可执行反事实，不声称用户实际按建议成交。

### 5.12 L1a 报价提醒

1. 页面每 60 秒调用只读 GET；服务按最多 50 只分批取 TDX quote。
2. GET 返回 `system_edge_eligibility`、quote 时间、source、staleness、`already_alerted` 与 `eligibility_identity`；后者 hash-bound 到 card/trigger、quote payload、evaluation time 及当前 position/intent hash。GET 不得写事件。
3. 超过 5 分钟、未来偏斜超过 30 秒、源失败、字段不全或当前 position/intent hash 已偏离 card 时，不弹窗；API/UI 显式返回 `QUOTE_STALE/QUOTE_FUTURE_SKEW/QUOTE_UNAVAILABLE/POSITION_SNAPSHOT_CHANGED/INTENT_SNAPSHOT_CHANGED`。
4. 新 eligible trigger edge 先调用 atomic claim POST。POST 必须重验 eligibility identity、card/current position/current intent 与报价年龄/future-skew，不重新解释方向、规模或阈值；成功追加唯一 `ALERT_EMISSION_AUTHORIZED` 后返回 `granted=true`，页面再弹出 toast。
5. 事件只保存重算 eligibility 所需的有界标量：`card_id`、`card_artifact_sha256`、`trigger_id`、`quote_price_raw`、`quote_open_raw`、`quote_observed_at`、`alert_evaluated_at`、`quote_source`、`staleness_state`、可选 `quote_age_seconds`、`user_seen_evidence=false`；不保存无界上游 payload。某 trigger 所需的 open/current 字段缺失时不得 claim。
6. 事件语义是“服务端授予一次提醒发送权”，不是 user-seen delivery receipt；第一批不加 ACK。
7. 若 claim 后页面崩溃，后续 GET 仍返回 `already_alerted=true` 的可执行边，页面以非模态条目展示但不重复弹窗。artifact at-most-once 是 HARD；UI 弹窗 at-most-once 仅 ADVISORY。
8. 浏览器 Notification 是可选增强；页面 toast 是主通道。`notification_service.py` 虽存在，外部投递配置与健康未验证，第一批完全不依赖。

三种 estimand 必须分开：

- `market_touch_opportunity`：事后日线/分钟线判断市场是否触及冻结价。
- `system_edge_eligibility`：页面实际打开、轮询实际观察且报价合格时是否出现新边；第一阶段不承诺离线完整重建。
- `alert_emission`：是否成功取得 `ALERT_EMISSION_AUTHORIZED`。

缺失 alert event 不能解释为“市场未触价”或“系统未观察到”；三者不得混用。

### 5.13 API 与 UI

同一 `backend/routers/position_timing.py` 提供：

| API | 副作用 |
|---|---|
| `GET /api/v1/position-timing/intents` | 只读 |
| `PUT /api/v1/position-timing/intents/{symbol}` | 仅 timing-owned intent，幂等 |
| `PUT /api/v1/position-timing/analysis-scope/{symbol}` | 仅 timing-owned scope 当前态，原子且幂等；不改已签发 card |
| `POST /api/v1/position-timing/materialize` | 仅 timing-owned cards/events/coverage state，exact-idempotent |
| `GET /api/v1/position-timing/cards/current` | 只读 |
| `GET /api/v1/position-timing/evidence` | 只读 |
| `GET /api/v1/position-timing/alerts/poll` | 只读报价与 edge |
| `POST /api/v1/position-timing/alerts/{trigger_id}/claim` | 原子追加唯一 alert authorization |

页面只设一个产品入口，使用现有 shadcn-compatible token。主要区域为：当前/最近行动卡（明确显示 `UPCOMING/VALID_TODAY/EXPIRED`）、非模态已提醒边、typed 数据状态、成本明细、研究证据。不得出现“一键下单”、自动交易开关或把 `46.9%` 显示成个股置信度。

## 6. L2/L3 研究与模型建议演进契约

§6.1～§6.5 保留**已完成 L2 v1** 的冻结人口、参数、CPCV、单调退出政策及 receipt 语义，不代表后续所有模型永久只能减仓、禁止重训或禁止本地模型。§6.6 是尚未实施的 v2 替代设计；新结论不重分类旧 trial。

### 6.1 为什么 L2 在首发后独立实现

首批先交付 L1/L1a 并开始积累真实 deployment outcome；若事件字段以后才补，会永久失去早期样本。因此第一批实现日志和评价并冻结 L2 population/sampling/model/inference spec。`PT-NEXT-002` 随后只增加一个离线 pipeline 并完成正式 audit；prospective outcome 为 0 如实写入 context，但不作为开跑门禁，也不让 L2 结果阻塞产品。

### 6.2 L2 population 与 baseline

`POSITION_TIMING_L2_POPULATION_V1`：

首个 L2 audit 的 objective 固定为 held-position `EXIT/REDUCE versus HOLD`，不宣称验证 watchlist 的入场 alpha。第一批仍保存 OPEN/ADD outcome，以便未来另立 entry objective，而不得把两种 estimand 混成一个 trial。

- 研究范围：`2018-08-01..2026-06-30` 内具备冻结日频 source identity 的 SH/SZ A 股。正式 source 绑定 `20260831-qe_hmm_full_v2-direct-20260905-candidate` 的 canonical-v2 daily/calendar/PIT/suspend/CSI300 文件及逐文件 hash；父 candidate 自身没有 full-history content hash，故不得把 derived bundle 的 content hash 反向宣称成父 candidate 已全量 hash。
- episode 的起点 `entry_decision_date=E`：从起始交易日起，每第 20 个全局交易日取一个 cohort；每个 cohort 纳入 E 时点 PIT active、E+1 有可判定入场状态的全部 canonical symbols。
- 不因当前是否属于 Selection Top20 而筛选。正式 canonical-v2 entry universe 明确 `exclude_st=true/st_pit=true`，故 v1 synthetic episode 的 `st_flag=false`；prospective ST deployment cell 必须显式 unsupported，不得伪造历史 ST 样本。L1 仍按方向处理 ST，二者不得混同；停牌/缺失按 typed status 保留。
- episode baseline：E 决策、E+1 raw open 建立合成满仓，持有至第 20 个 holding session 的 raw close；terminal 不可用最多顺延 5 个交易日。
- review row：从第 1 至第 19 个 holding session 收盘逐日形成 `review_decision_date=R`，只用 R 收盘时已知特征；候选 `target_action_date` 固定为 R+1。监督 row label 是在 R+1 raw open 卖出该 episode 的完整合法初始数量、相对继续持有同一数量至 baseline terminal 的逐腿成本后增量 bps。
- policy path：`MONOTONE_EXPOSURE_V1` 把 OOF 预测映射为目标 exposure，实际目标固定为 `min(previous_effective_exposure, mapped_exposure)`，因此只能 HOLD/REDUCE/EXIT、不能重新加仓。每次 reduction 先按 symbol 的卖出交易单位向下得到合法实际数量；全量退出才允许最终零股规则。每次实际 reduction 是独立父订单并逐腿收费；剩余实际数量在 terminal 统一卖出。R+1 不可卖时该次动作过期并留在原 exposure，下一 review row 重新决策，不顺延旧动作。
- 两条路径共享的 E+1 合成买入腿在 lift 中相消；study 只比较未来卖出路径。所有 raw fill、公司行动与 terminal valuation 继续服从 §5.6，不因使用合成 episode 改回 raw-price return。
- `episode_id = hash(population_identity, canonical_symbol, entry_decision_date, entry_trade_date)`，且 `(canonical_symbol, entry_decision_date)` 唯一；sampling identity、calendar hash、universe hash、预计与实际 episode count 写 receipt。
- 正式结果为 96 cohorts、预期与实际均 388,035 episodes、7,351,727 review rows，其中 386,737 episodes 可做 paired policy evaluation；精确数量是运行结果，不是开跑门禁。

最低佣金依赖名义金额，故 L2 不得用“1 元归一化仓位”计算成本。`L2_DEPLOYMENT_NOTIONAL_ASSIGNMENT_V1` 在 request 冻结：从 study cutoff 前 held-position `CARD_ISSUED` 的正 `planned_full_notional_cny` 构造按 `(card_id, value)` 排序的 deployment notional 序列及 hash；以 `{episode_id, distribution_sha256}` 的 canonical JSON hash 对序列长度取模，为每个 synthetic episode 确定性分配一个 full notional，再按 E+1 raw open 与板块规则得到合法初始数量。无可用 notional 序列、合法数量为零或 source/hash 不闭合时保留 typed unavailable，不以任意固定金额代替；这不影响 L1/L1a。该赋值不看 outcome，不新增 hypothesis。

这不是“消除分布偏移”，而是以合成入场人口替换 Selection Top20 入场偏差。正式报告同时给 synthetic population 未加权结果，以及只按 prospective `CARD_ISSUED` 中 `pre_action_qty > 0` 的 held-position 卡片所形成的 source role、action side、holding-age bucket、regime、ST 状态 deployment-population 加权结果；不支撑的 cell 显式 unavailable。本次两个真实卡片都属于 `HOLDING|NONE|UNKNOWN|UNKNOWN|0`，deployment-weighted 值因此接近 0 且只能标 `DIAGNOSTIC_ONLY`，不是模型效果证据。

`CARD_ISSUED` 因而必须冻结 `pre_action_qty`、`planned_full_notional_cny`、每个 trigger branch 的 planned delta、reference raw price、持有期、来源角色、sizing/board-lot/cost-policy hash，保证后续 weighting、名义金额赋值和同方向同规模执行反事实可计算。

### 6.3 两个模型、一个政策、两个假设

两模型一次性冻结，不设置“MDE 恶化则回落单模型”的条件分支：

1. `SKLEARN_RIDGE_V1`：`scikit-learn=1.8.0`、`alpha=100`、`fit_intercept=true`、`solver=svd`。
2. `LIGHTGBM_GBDT_V1`：`lightgbm=4.6.0`、`boosting_type=gbdt`、`objective=regression_l2`、`n_estimators=300`、`learning_rate=0.03`、`num_leaves=15`、`max_depth=4`、`min_child_samples=100`、`subsample=1.0`、`subsample_freq=0`、`colsample_bytree=1.0`、`reg_alpha=0`、`reg_lambda=1`、`random_state=20260903`、`n_jobs=1`、`deterministic=true`、`force_col_wise=true`、early stopping disabled。

两模型都必须在 request 中序列化并 hash 完整 estimator `get_params(deep=false)`、预处理规格、feature order、target、package version 与随机性设置；request 另冻结 Python、NumPy、Pandas、PyArrow、SciPy、Qlib、threadpoolctl 和已加载 BLAS/OpenMP pool 身份。上列显式参数是稳定语义摘要，不允许未记录的库默认值改变 trial identity。Ridge 数字列使用训练折 median 后 `StandardScaler`，GBDT 使用训练折 median 但不缩放；预编码类别固定为 `DOWN/UP_OR_FLAT/UNKNOWN`，未知值只进入 `UNKNOWN`，不由验证折扩展 vocabulary。

监督目标固定为每个 review row 的 `full_exit_incremental_net_value_bps`；study estimand 才是完整 monotone policy path 相对 do-nothing baseline 的 primary-horizon `net_lift_bps`。数字特征均在训练折内 median impute；`market_regime` 固定 one-hot 为 `DOWN/UP_OR_FLAT/UNKNOWN`；两模型使用以下同一有序 feature vector，不加入 HMM 或 Selection score：

首次正式 request 曾按旧稿包含 `selection_rank/selection_score`，但唯一不可变排名导出只覆盖 `2024-07-04..2026-03-09`，使一个时间训练折两列必然全缺失并以 `POSITION_TIMING_L2_FEATURE_UNAVAILABLE` 在任何完整 hypothesis/receipt 产生前停止。仓库不存在覆盖 2018～2026 的同语义排名导出，因此 v1 一次性删除这两列并重新冻结 request；这是数据可用性修订，不读取局部收益、不缩短人口，也不允许据此追加替代特征搜索。

```text
holding_trading_days_elapsed
holding_fraction_of_time_stop
unrealized_close_return_bps
relative_return_since_entry_bps
return_1d_bps
return_3d_bps
return_5d_bps
return_10d_bps
realized_vol_5d_bps
realized_vol_10d_bps
realized_vol_20d_bps
drawdown_from_peak_since_entry_bps
runup_from_entry_peak_bps
distance_to_stop_bps
distance_to_take_profit_bps
distance_to_trailing_stop_bps
intraday_range_bps
close_location_in_day
volume_ratio_5d_to_20d
market_regime_down
market_regime_up_or_flat
market_regime_unknown
```

四个 guard-relative 特征必须从 request 绑定的 exit snapshot 派生而非写死第二套参数：`holding_fraction_of_time_stop = holding_session / max_holding_days`，`distance_to_stop = unrealized_return + max_loss_bps`，`distance_to_take_profit = take_profit_bps - unrealized_return`，`distance_to_trailing_stop = drawdown_from_peak + trailing_stop_bps`。snapshot 中相应 rule 即使 disabled，数值仍只作为上下文特征；不得据此开启运行时 guard。

每折同时输出 feature availability report；某数字列在训练折全缺失、固定类别无法编码或 feature order/hash 漂移时，该 L2 bundle typed failed，不删除列、不以零填充、不切换模型。该失败不影响 L1/L1a。

交叉验证冻结为 8 blocks、2 validation blocks、20-trading-day embargo、28 paths、每行 7 个 OOF；block 以 `entry_decision_date` 分配，同一 episode 的全部 review rows 必须留在同一折，同一 entry date 的所有股票不得拆分。七个 raw OOF 预测取算术均值并写诊断；每个 path 必须先用自身训练预测的正值分位点映射四档 exposure，最终政策 exposure 取七个 path exposure 的中位数（第 4 顺序统计量）。这保留“某 path 无正训练预测则该 path 全 HOLD”的语义，且结果仍只在 `{0,0.25,0.50,1.00}`；无 final refit、无参数搜索。

唯一 selected-eligible 政策 `MONOTONE_EXPOSURE_V1`：

- `predicted_full_exit_incremental_net_value_bps <= 0`：target exposure `1.00`。
- 正预测在训练折正值分布的 `(0, q50]`：`0.50`。
- `(q50, q75]`：`0.25`。
- `> q75`：`0.00`。

quantile 只由对应训练折生成并应用到验证折，政策形态、分位点和档位事前固定。Ridge 与 GBDT 各运行同一政策，共两个 hypotheses。`A0_DO_NOTHING` 是基线不计 hypothesis；既有 `A1_FIRST_CROSSING_5BPS` 若保留只作历史兼容 diagnostic，不参与 selected；holding-age 条件臂删除。

若某训练折没有正预测，`q50/q75` 不可定义，该折固定全部输出 exposure `1.00` 并记 `NO_POSITIVE_TRAIN_PREDICTIONS`；不改阈值、不借验证折估计分位点、不切换政策。

GBDT 的作用只是缩小“只有线性模型错设”的歧义。双阴性只能表述为“在两个预注册函数族及冻结规格中未发现成本后优势”，不得表述为不存在可学习信号。holding-age 与 regime 分层只作 diagnostic，不得反向选择模型、政策或阈值。

### 6.4 统计分类

L2 冻结 `economic_threshold_bps=0.0`，因为 estimand 已扣完逐腿成本。现有 N2 exit 的阈值是 `5.0`；这里只复用 inference 结构，不宣称取值语义一致。

每个 `entry_decision_date` 先对该日全部 paired evaluable episode 等权平均，形成两个模型各自的 cohort policy-lift series；unavailable 只进入 coverage，不按零值混入。由于 cohort 每隔 20 个交易日而 terminal 最多延至第 25 日，相邻 cohort 仍可重叠，区间冻结为按时间排序 cohort 的 circular moving-block percentile bootstrap：`block_length_cohorts=2`、`bootstrap_repetitions=2000`、`bootstrap_seed=20260903 + model_offset(0/1)`、`confidence_level=0.95`、`target_power=0.8`。nominal interval 使用 `alpha=0.05`；family-wise interval 对两个冻结 hypothesis 使用 Bonferroni `alpha=0.05/2`，不得运行后改变 family 或 seed。

- 每个 hypothesis 独立分类：adjusted lower `> 0` 为 `SUPPORTED`；adjusted upper `<= 0` 为 `NEGATIVE`；其余为 `INCONCLUSIVE`。
- study-level `effect_evidence=SUPPORTED`：至少一个 hypothesis 为 `SUPPORTED`。
- study-level `effect_evidence=NEGATIVE`：两个 hypothesis 都为 `NEGATIVE`。
- study-level `effect_evidence=INCONCLUSIVE`：其余组合；不得用一个模型的负结果覆盖另一个模型的不可分辨结果。
- nominal interval 为正而 adjusted interval 跨零时为 `INCONCLUSIVE`，reason `MULTIPLICITY_ADJUSTMENT_ERASED_NOMINAL_SIGNAL`。
- `power_status=UNDERPOWERED` 当 `mde_bps / oracle_mean_lift_bps > 0.25`，否则 `ADEQUATE`。MDE 是跑后报告义务，不是运行准入。

receipt 同时报 nominal per-model interval、两个假设的 family-wise adjusted interval、MDE/oracle、逐腿成本情景与 `COST_ASSUMPTION_SENSITIVE`。2/3 父订单情景必须复用与卡片相同的合法数量拆分；无法按要求拆分的 episode 不伪造等分金额，而是进入 partial/unavailable coverage。只有与 base 完全同 population 的可用情景才可触发 `COST_ASSUMPTION_SENSITIVE`。新增第二个假设会扩大 simultaneous interval，这个功效代价如实报告，不触发回落分支。

本次 simultaneous family 固定 `familywise_hypothesis_count=2`。全局 N0 的 request-era 实际历史记录数以 `historical_registry_context_count` 只读披露（本次为 36），提醒解释者已有搜索历史；由于 objective、population 与 trial family 不同，不把它们伪装成这次两个预注册假设，也不从历史记录回选候选。

trial 只写 `position_timing_advice_v1/research_registry/timing_trial_registry_v1.jsonl`。全局 N0 registry/current_route 只读历史计数，绝不写入或重算。

实现复用 `AdvisoryResearchTrialRegistryV1` 的 append/identity 机制，但路径独立、不得调用 `generate_current_route`。为兼容现有 record schema，两条模型 trial record 均冻结 `objective_contract=RISK_MANAGED_ADVISORY`、`study_type=LEARNABILITY_AUDIT`；产品 objective 仍为 `POSITION_TIMING_ADVICE_V1` 并单独写 study receipt。每个模型的 `SUPPORTED/NEGATIVE/INCONCLUSIVE` 分别映射 `result_class=CONTROL_READY/NEGATIVE/EXPLORATORY`；前两者 `decision_use=DIRECTION_GATE`，后者 `NAVIGATION_ONLY`。这里的 `DIRECTION_GATE` 只控制该模型能否进入 L3 标签，不阻塞 L1/L1a、L2 运行、PR 或发布，也不调用全局 route。

正式 `EVID-L2-FORMAL` 的结论是：Ridge `NEGATIVE + ADEQUATE`，GBDT `INCONCLUSIVE + ADEQUATE`，study `INCONCLUSIVE`，`selected_model_id=null`。Ridge/GBDT 的 base point 分别为 `-45.3852/-35.0801 bps`；两模型 `mde/oracle` 分别约 `7.17%/8.09%`。该结论只表示两个冻结函数族和政策没有得到正的 family-wise 下界；不证明不存在其他信号，也不允许在同一 family 追加搜索。bundle inspect 与 exact retry 均通过，retry 对两条自有 registry 记录为 duplicate no-op；全局 N0 registry SHA-256 前后均为 `53ca1338ee5f725be38eeb217b06ebc33d99a6a75904ea1efc4c8607fbd5e067`。

### 6.5 L3

只有 `effect_evidence=SUPPORTED` 的模型/政策可让对应 evidence block 与适用卡片进入 `MODEL_ASSISTED`。首个 L2 objective 只覆盖 held-position EXIT/REDUCE-versus-HOLD，因此即使 SUPPORTED 也不得把 watchlist `OPEN/ADD` 卡改称 model-assisted。`NEGATIVE/INCONCLUSIVE` 或任意 `power_status` 均不影响 L1/L1a 继续运行；结论与 reason 在页面证据区可见，卡片保持 `RULE_BASED_RISK_MANAGEMENT`。

本次正式 audit 没有 `SUPPORTED` hypothesis，故 L3 保持未实现，页面/API 也不加载任何 L2 模型。evidence API 仅返回 `POSITION_TIMING_L2_FORMAL_AUDIT_REFERENCE_V1` 的 hash-bound 总体状态，页面证据区显示 study `INCONCLUSIVE`、Ridge `NEGATIVE`、GBDT `INCONCLUSIVE` 与“无入选/运行模型”；这些字段不得进入个股卡片。该结果不是等待人工批准的门禁，而是本轮研究的终态。

若两个模型都 `SUPPORTED`，固定优先 Ridge；只有 Ridge 非 `SUPPORTED` 且 GBDT `SUPPORTED` 时选择 GBDT，不按历史点估计、Sharpe 或区间宽度事后回选。每个模型 record 固定 `planned/generated/evaluated_trial_count=1`；只有上述唯一被选模型的 `selected_trial_count=1`，其余为 0，study-level selected 数等于两条 record 之和且最多为 1。

sealed holdout 是用户可选择的一次性确认动作，读后记录 consumed；不读取不妨碍 L1/L1a、L2 运行或上述 effect 分类。

### 6.6 下一优先任务 PT-NEXT-004：日频模型到个股建议的闭环（设计，未启动）

本任务交付可运行的研究到建议闭环，不以“增加一个总体 shadow 报告”代替个股推断，也不以“必须先获得正收益”阻断研发。只在现有 namespace、CLI、router、页面及自有 artifact 内实现；没有新表、worker、模型服务或自动交易。

```text
本地 DB / QE immutable exports → 同一纯特征构建 → 前向训练/评价 → 本地版本化模型
                                           ↓                  ↓
用户分析范围 + 唯一持仓/预算 → 当晚同口径特征 → 个股动作价值 → 唯一日频建议
                                                           ↓
                              连续现金/持仓评价 ← 既有报价触发提醒 → 人工决策
```

#### 6.6.1 收益目标、人口与唯一决策

**评价目标**：`POSITION_TIMING_ACTION_VALUE_V2`。同一目标股、起始资金和持仓下，比较每个交易日重新决策的政策净资产路径。研究是 pooled stock/date 训练、特定股票推断，不为每只自选股从零训练一个模型，也不把横截面排名改名为择时。

- 历史训练使用 PIT SH/SZ 股票，不用今天的自选成员倒填过去。沿用可核验日频导出与确定性名义金额赋值思路，另冻 v2 人口；v1 缺 ST 历史、用户持仓人口偏移均显式报告。真实当前持仓不要求新建合成入场日，`holding_age=UNKNOWN` 是合法输入状态，不强制 20 日退出。历史训练的资本/持仓状态由事前固定的 cohort 起点及不看未来的持有/持币路径产生，不读取验证期预测反填训练状态；未支持的资金/持有期 cell 标外推，不宣称全持仓人口已有证据。
- 每次 T 日 review 的 primary horizon 为**从 T+1 起第 20 个交易日收盘**，不是合成入场后的第 20 日；其他 `(1,3,5,10)` 仅 diagnostic。单次监督标签可使用 §5.11 的最多五日 terminal-liquidation proxy 顺延；连续策略主报在共同日期按可核验的经济价值估值，不为每笔新动作重置资本或改变报告终点，不能把尚不能清仓的市值当已实现现金。
- 两个监督 objective：`ENTRY_ACTION_VALUE_V2` 评价 OPEN/ADD 相对保留现金/当前仓位的净增量；`EXIT_ACTION_VALUE_V2` 评价 REDUCE/EXIT 相对保持当前仓位的净增量。每个候选合法动作单独建 action-conditioned row，输入含动作规模、费用与持仓状态，标签为该动作按冻结执行政策实施后至共同 horizon 的净资产差（bps 分母为相同起始资本），不以 hindsight 最优动作先筛样本。
- 分别使用一个 `LIGHTGBM_GBDT_V2_ENTRY/EXIT`，参数沿用 §6.3 完整固定规格，不搜索超参；仅 objective、feature/schema、label 与 model id 更新。相同 symbol/review 的不同动作及标签区间不得跨训练/验证边界。动作输入只能用 cutoff 已知的计划数量、触发分支和预计成本，不将 T+1 realized fill/fee 放进特征；实际成交量价仅用于标签。两套监督目标可共享纯特征构建，但必须带 objective identity；研究模型数不等于统计假设数。
- 唯一 `DAILY_ACTION_VALUE_POLICY_V2`：枚举预算允许的 `{0,0.25,0.50,1.00}` 目标敞口以及“保持当前实际数量”候选，按板块规则、可卖量、资金和用户风险上限得到合法实际数量并去重。增加/减少分别由对应模型评价相对不交易的同单位净增量，保持动作值为 0；取严格大于 0 的最大预测值，否则 HOLD/WAIT。并列时优先少换手，再按固定目标敞口升序；无模型时不以零预测伪装成功。一个 symbol/T 只给一个行动，不平均两个含义不同的分数、不同时发买卖指令。
- WAIT 不强制次日买入：每日重新预测，允许连续持币、多日等待、退出后再入场；动作仍只在 T+1 有效。卖出后仅在股票仍是用户有效已选自选时继续分析，不能为了回补研究自动扩大生产 scope。历史连续 replay 的研究成员在起点固定并记录退出规则；该研究人口与当前用户 scope 的差异必须披露。

**两条主比较基线同时保留**：①同股固定买入持有（已有持仓保持；现金起点在首个计划日尝试买入，未成交后按冻结规则逐日重试），②同样起始资本、scope、预算和输入的冻结 L1 v1 政策。两基线均适用同一可交易性、公司行动、实际数量、逐腿费用和执行摩擦，L1 不补造用户买入意图；其无 intent 时可能保持现金，这正是需另报买入持有基线的原因。市场/行业、持仓时间、现金比例、回撤、换手与错过上涨分开诊断，不替代上述比较。

单卡 action label 与 policy performance 不同：先用监督学习估计有限动作价值，再用**连续、资金守恒、可回补**的 out-of-time 路径评价完整政策。不能把重叠单卡收益相加、把早期卖出所得现金反复分配，或只挑成功成交行。股票间不默认共享无限预算：主研究按起点冻结的独立标的资金 sleeve 聚合；每个 sleeve 资金只注入一次、月度重训不重置仓位，最后一个共同报告日未平仓市值/现金与退出成本敏感性分别展示。用户未提供总资金时只给逐股建议，不宣称整组合可同时执行。

v2.4 的 `OPEN_T1 vs WAIT_ONE_SESSION_OPEN_T2` 撤出主目标。在同数量、共同终值、无公司行动的简化情形，两者终值抵消，买入净差仅为 `Q×(P_T2-P_T1) + buy_fee_T2-buy_fee_T1`，不能回答“未来是否值得持有”。若保留该旧提案，只作执行价格 diagnostic，不能把它的正结果升级为入场 alpha。§6.2 的 monotone exit 同样只保留为历史 audit，不是 v2 的永久持仓限制。

#### 6.6.2 核心信息、晚间时钟与历史版本

`TimingFeatureSnapshotV2` 分为核心与可选增强，不再强制“22+12”全字段。核心选择依据为经济机制与不看收益的字段覆盖，不按因子库 IC/PnL 排名回选：

| 信息块 | 首个 v2 处置 | 机制与约束 |
|---|---|---|
| 自身价格/成交量/波动 | 核心：短中期收益、趋势偏离、区间位置、量比、下行波动 | 共享复权/单位定义；少量互补窗口，不堆同义技术指标 |
| 相对指数与市场状态 | 核心：相对 CSI300 收益及指数连续收益/波动 | 不等 HMM；市场宽度依赖全市场覆盖，作为可选增强 |
| 持仓/动作/资金成本 | 核心：当前数量、现金、可卖量、动作规模、费用；持有期/浮盈缺失带 mask | 预算缺失影响定量建议，持有期未知不能直接拒绝分析；不伪造入场成本 |
| 行业/资金流/筹码/宽度 | 可选 block，逐个新版本检验 | 核查历史行业归属、流量单位、筹码价格基准、全市场 coverage |
| 新闻公告结构化事件 | 可选 block，历史版本不可证时只前瞻采集或排除 | 必须同时满足 issuer、timestamp、source version 与扫描完整性 |
| HMM / Agent 解释 | 后接、非依赖 | HMM 独立增量；Agent 只解释已绑定事实 |

v2.4 的 EMA20 slope/距离、ATR14、20 日 downside semivol、相对 CSI300/申万二级、60 日换手分位、大单净流入、筹码成本/获利盘、宽度、事件衰减 **12 项保留为候选机制目录，不再是强制可用字段或已证有效因子**。具体 source field（如 `cp_cost_50pct`）、公式、窗口、单位、顺序、缺失规则在首次读取研究收益前，以 source/schema coverage 固定到 request。核心缺口只结束对应模型计算并显式返回原因；可选块缺席不阻塞 core，运行中不得静默删列、换模型或重新定义字段。

v2 每个交易日固定 `decision_as_of=T 20:00 Asia/Shanghai`、服务 T+1。20:00 前 materialize 返回 `DECISION_CUTOFF_NOT_REACHED`，不提前锁死新卡；错过 cutoff 后可物化，但仍只用 cutoff 前真正可见且可重建的数据，不能以 created_at 替换信息时钟。按 trade_date 存储的资金流/筹码不自动等于 20:00 前可得：不能证明当晚可见时使用已证可见的滞后版本或排除该字段。旧 v1 卡的 15:00 identity 不变，同一正式日期不并发签发两个策略版本。

快照同时冻结 `feature_available_at`、source manifest/hash、历史源版本、公式/feature order、预处理、calendar、cost/guard 与代码身份；同一纯构建器用于训练和运行。数值填补只在训练数据上拟合并保存 mask/参数；required core 列全缺时 typed failed，不运行后替换候选。日期级未知可见时间不能升级成精确 timestamp。

`market.event_signal` 当前行会被 upsert/suppress 改写，仅检查旧 `available_at` 不足以还原当时 severity/status。只有历史版本化数据或可从当时已知源材料确定性重建的规则结果能进入历史特征；否则该块不进 core，可用 timing-owned 前瞻有界快照积累，无需新表。“无事件=0”还要求该 issuer/time window 已完整扫描；扫描未知、分类后改、时间不精确均显式标注，不交给 LLM 猜测。

#### 6.6.3 前向验证、统计与有限迭代

- **主证据使用 walk-forward**：首个训练窗口为数据起点起 756 个交易日，此后按月扩展；每月最后交易日 20:00 为训练 cutoff，新模型最早用于下一月。训练只含 label 的真实可用终点（含 terminal 顺延及发布延迟）不晚于 cutoff 的 row；任何与验证信息区间重叠的 label 都 purge。训练迟到时沿用最近已发布且身份完整的同政策版本模型并标 age；没有可用模型才 L1 fallback，日期仍计入部署路径，不把迟到的模型回填为当时已服务。
- 该训练日程、起点、标签时钟与固定参数是首次运行前的设计选择，不是已证最优参数，不因 PnL 改动。历史 replay 将模型最早可用时点固定为 cutoff 后首个交易日 20:00，下一张卡才可消费，明确 `SIMULATED_TRAINING_LATENCY_ONE_SESSION`，不声称模型历史上实际存在；真实运行再校验 trained_at/published_at 不晚于 card cutoff，迟到只从后续卡生效。可以显式 CLI 批量执行历史月份和当前 final fit；不建定时 worker，页面不训练。计划月度重训不是事后收益选参，漏跑时显示模型 age/训练日期，不新增人工放行流程。
- 既有 8-block CPCV 只作可选稳健性诊断。其训练可能来自验证日期之后，purge/embargo 不会使其自动成为历史部署路径；不更改 v1 receipt，也不把 CPCV OOF 收益接到 v2 主净值曲线。
- 主报告为固定日历区间、固定资本下连续政策相对两基线的净值/收益差，现金收益基准为 0，费用按实际未来腿计；另报滑点/延迟情景。推断序列为各日“candidate 净资产增量减 baseline 净资产增量”除以相同总起始资本的 bps，累计值对应期间净增量，日均 CI 与期间累计收益分别命名。相同日期跨股票先聚合，再按连续交易日做时间 block 推断；不把数百万重叠 review/action rows 当独立样本。首个 request 固定 25 交易日 moving blocks、5,000 次、seed=20260907、95% interval；同时报告有效交易日数、覆盖、block 假设局限，不以此保证有效样本量充足。
- **计数按可被选择的策略/比较，不按模型文件数**：首轮固定 2 个监督模型、1 个联合政策、相对 2 条主基线的 2 个主比较（Bonferroni simultaneous alpha=0.05/2）；两个 head、单卡 horizon、holding-age/regime 与最近 252 日是 diagnostic-only，不能据其结果另选政策。历史搜索、v1 结果和后续 feature-block 尝试在自有 registry 显式累计披露，不宣称只剩两个无历史背景的新发现。
- `economic_threshold_bps=0.0`；每条主比较沿用 adjusted lower>0 为 SUPPORTED、upper<=0 为 NEGATIVE、其余 INCONCLUSIVE；**联合人工建议政策只有两条主比较均 SUPPORTED 才标支持态**，否则研究区逐条显示结果，不能把某一个 head 支持等同整个服务政策支持。MDE/cost sensitivity 均为报告义务；v2 的日均增量与 v1 的 episode oracle 不是同单位 estimand，禁止直接相除。首个 v2 报日均 MDE，power classification 在没有事前同口径效应尺度时为 `NOT_COMPUTABLE`，不额外建设最优控制 oracle、不伪造 ADEQUATE、也不阻塞运行。
- 前向时间、PIT/特征完整性、真实执行路径是证据有效性，不是人工审批。离线 candidate 路径使用该折训练出的可用模型，不等待尚未完成的全研究 SUPPORTED 分类，否则会形成自锁；只有实际服务政策按最终证据决定支持标签。无正结果仍完成研发和发布实验分析；L1 不受影响。研究区展示阴性/不可分辨与成本敏感性，不隐藏失败，也不把研究置信区间当个股胜率。
- 冻结只约束同一次实验，不把研发永久锁死。新的机制/可选特征块需新 request、code/data identity、显式 trial 计数和同一前向比较；禁止看测试收益改参数后仍沿用原“无搜索”名义。最近 slice/漂移只标 `CURRENT_MARKET_ALIGNMENT_UNCERTAIN` 等解释，不触发事后重训分支或批准门禁。

#### 6.6.4 本地推断、方向与数量分离、支持态接线

本任务必须实现 local model → 所选个股 daily prediction，而不是只有 aggregate receipt。模型存 timing-owned `models_v2/<model_bundle_sha256>/`，绑定 estimator/预处理、目标、特征、训练 cutoff、代码环境、cost/decision policy 与 forward evidence identity；只加载本任务 CLI 生成并校验的可信本地 artifact，不接受外部上传的可执行 pickle。训练环境校验 LightGBM 4.6.0，缺包 typed failure，不静默换模型。

1. 用户只需选择分析股票即可看方向研究；已有持仓默认数量来自唯一账本。没有预算的自选仍需给模型明确资金输入：以 request 冻结的 `DIRECTION_REFERENCE_NOTIONAL_CNY=100000` 作为**公开的参考情景**在内部做合法数量估算，只展示该情景下的方向，不把它写进用户 intent/仓位或 card 数量；标 `DIRECTION_ONLY/SIZING_INPUT_UNAVAILABLE/REFERENCE_NOT_USER_BUDGET`，不得称用户成本后支持或发可执行买入提醒。参考金额只为让方向分析可计算，不参与预算适用性结论、不得按收益调参；参考情景本身不可评价时直接 typed unavailable。预算补齐后才给个性化合法数量/成本，不用预先买入 intent 决定模型该预测哪一边。旧 v1 无 intent→WAIT 保留为基线，不冒充新能力。
2. `position_source`、T+1、限价/停牌/手数、用户预算/风险上限是不可绕过的可执行性边界；L1 中止损、止盈、趋势/价格护栏参数则是冻结的**政策基线**，不是永远最佳的市场真理。首个 v2 明确保留 v1 exit guard 的风险覆盖和 price guard 执行映射，顺序为可交易性/用户上限 → 冻结风险退出 → 模型候选 → 冻结价格分支，回放与服务同序。只有未被风险退出覆盖的行由模型选动作；若以后改变经济规则，作为新政策版本整体评价，不修改共享 defaults 或历史 snapshot，也不把所有旧参数永远写成不可优化的 HARD。
3. 新卡冻结 T 日的模型版本、候选价值、唯一方向和合法分支，T+1 只观察已有 quote trigger。预测值是模型估计，不是保证收益/概率。对同一标的先展示一个主建议，再给规则/模型差异原因，不能出现两张互相竞争的正式卡。
4. 未获联合支持时，L1 继续为正式建议；个股模型结果作为同页 `EXPERIMENTAL_MODEL_ADVICE` 只读影子分析（含数据缺口），不生成可执行 alert、不中途改 L1。支持态获得后，显式 CLI 发布绑定证据的自有 `serving_policy_v2` 版本，下一尚未签发的交易日卡可标 `MODEL_ASSISTED` 并复用原提醒链路；这是普通版本发布，不要求新增审批或双人确认。
5. 同一正式日期只使用一个 serving policy：发布不覆盖已签发 v1/v2 卡。模型不可加载、核心输入缺失或身份不符时，显式降为 L1 并显示 `MODEL_UNAVAILABLE_RULE_FALLBACK`；已签发模型卡若完整性受损则拒绝提醒，不伪造同日替代卡。训练过旧标日期/age，不借机静默换模型。历史模型/卡片/事件可回读；回退只影响下一张未签发卡。
6. 既有 `GET /evidence` 增加研究与部署版本区别，current/materialize 同页扩展方向/数量/模型来源；如兼容需要新增 schema version，必须明确 v1/v2 reader 和幂等键，不能把旧 v1 内容 hash 偷换。研究 shadow 位于独立 `research/...`，正式 v2 卡与事件使用 timing-owned 版本化子路径，旧目录/事件不改写，不为新模型创建第二套通知或账本。

支持证据绑定整个冻结训练/重训程序、核心字段、政策、适用人口和执行假设，不声称最新 final-fit 权重已有独立历史验证。同规格月度更新写新 model hash 与 cutoff、继承程序证据且明确适用边界；改特征/政策/时钟则是新研究版本，不能借旧支持态背书。仅日线 proxy 可用时，所有收益说明仍带执行假设标签，不能升级成真实人工成交证明。

#### 6.6.5 执行真实性、可选信息及下一块边界

首个 v2 即需遵守与服务一致的 `AT_OPEN/ON_PRICE_TRIGGER` 分支、方向性 no-fill、T+1、真实数量与分项费用；不能训练/评价一律假定开盘成交，却在线按到价条件提醒。复用 §5.11 保守日线 proxy 开始长历史回放，同时对已有分钟覆盖的固定连续留出区间做离线触发顺序/可成交性核对；分钟覆盖不足的结论标 `DAILY_FILL_PROXY/EXECUTION_REALISM_UNVERIFIED`，不得包装为真实成交超额收益。基础连续路径评价不能后移到第二块。

合法未成交不是未知数据：保留原现金/持仓，进入 intention-to-treat 主路径，计漏买、漏卖与机会成本；市场数据确实未知则报告 unknown coverage，不按 0 填补、也不只报告幸存者收益。共同已知覆盖下的 paired 统计必须连同全人口分母、缺失时段和收益可计算范围展示。费用之外还需冻结价差/滑点/提醒延迟情景，数值由源能力与人工执行假设在看收益前决定，并披露而非凭空声称“无冲击”。

`PT-NEXT-005` 再扩大分钟回放覆盖、检验执行假设及可选块增量。它不是等待真实 risk-exit 卡攒够才做所有研究：可对 v2 前向生成的日频计划做模型标记的历史回放，但与 §7 的真实 prospective SELL-only audit 分开，不伪造成用户卡。分钟方向/自适应执行/分钟链路改造仍不在当前两块范围。

资金流、筹码、历史行业、结构化事件、HMM 逐块与冻结 core 比较；有 causal OOF 的 HMM 概率/dwell/transition 才进入新 request，不以 HMM 未实现阻塞 core，也不做四路消融。事件按 §6.6.2 验证历史版本，可影响后续模型但不是首个模型的必需条件。

多智能体只做可选解释，输入为卡片/结构化事实/source refs 只读副本，输出 schema-bound 摘要并绑定 model/prompt/source/as-of hash，标 `EXPERIMENTAL_CONTEXT`。不调用当前有副作用 `/analysis/stock`、不写 `app.analysis_records`、不用 trend mock；Agent 一致意见不算独立统计证据，不将自由文本分数送入核心模型。未来可用小型手工成交记录/导入核对实际建议收益，但不接自动交易、不新建券商账本；当前所有 policy outcome 仍不是用户实际成交回报。

## 7. 分钟研究：既有独立审计与后续机制

§7.1～§7.2 是已完成的真实 risk-exit SELL-only L4b-1 契约，不覆盖所有日频触发建议。§6.6.5/§9.7 的日频计划离线分钟回放属于执行真实性验证，不生成分钟新方向，也不修改本节历史 audit 的人口/receipt。

### 7.1 L4b-1 执行窗口

L4b-1 只能通过独立预注册反事实回答：在 card 已固定 symbol、方向、合法数量和 horizon 后，T+1 的静态分钟执行窗口相对 `AT_OPEN` 的成交价差是否在逐腿成本与缺失处理后为正。它不是分钟新信号、订单执行器或 L1 guard，不改变 action、quantity、target date、horizon 或 parent-order count。

首个且唯一窗口冻结为：

- benchmark `AT_OPEN_RAW_V1`：目标交易日 09:30 首个完整分钟的 raw open，即 `$open/$factor`；
- challenger `OPENING_30M_VWAP_RAW_V1`：09:30～09:59 可交易分钟的 `Σ$amount/Σ($volume×$factor)`；
- primary horizon 固定 20 个交易日，但两条路径的方向、数量和终值相同，故执行现金差是该 horizon 下的唯一增量；不借 horizon 改写收益标签；
- 两条路径都固定一个 parent order，按 `PERSONAL_MANUAL_COMPONENT_COST_V1` 分别重算实际成交金额的佣金、过户费、证管费、经手费与卖出印花税。

正式人口只接受 timing-owned `CARD_ISSUED` 所绑定的完整 immutable card，且必须同时满足：`execution_window=AT_OPEN`，action 为 `EXIT`，side 为 SELL，`requested_delta_qty < 0`，并且只有一个与该数量完全一致的 `RISK_EXIT_AT_OPEN/ALWAYS` trigger（`sell_reason=risk_exit`）；同时要求 card/event hash 一致，card-bound cost policy 与研究实现兼容，target date 在冻结 minute snapshot 内。`HOLD/WAIT/UNAVAILABLE` 不是执行反事实；现行 OPEN/ADD 和普通 REDUCE/EXIT 调仓均为 `ON_PRICE_TRIGGER`，其 branch/数量会被 T+1 行情决定，混入会破坏“方向与规模固定”，故另列人口排除原因，不改成 synthetic action。当前持仓、自选、QE、Selection、L2 未支持模型均不得补造方向。

价格和市场状态沿用分钟执行标准：raw OHLC 与 raw limit 同基准；有效 bar 的 factor 缺失、非有限或小于等于 0 是 data error；买入涨停、卖出跌停是方向性 no-fill，不伪装为坏数据。直接 Bin reader 必须先验证 target date 位于 `instruments/all.txt` 的该 symbol PIT span，禁止读取物理文件中被 ST/退市/暂停 universe 排除的行。benchmark 的 09:30 原始成交量与 challenger 的可交易窗口原始成交量都必须不小于 card quantity；challenger 只用方向上可交易且 amount、raw volume 有效的窗口 bar。无可交易 bar 或任一路径容量不足均记 typed no-fill。研究假设在该容量条件内的小额人工订单不产生市场冲击，并在 receipt 标 `NO_MARKET_IMPACT_ASSUMED`；不据此生成自动拆单。

净改善定义为同一 SELL card 的 `challenger net proceeds - benchmark net proceeds` 除以 benchmark gross notional，正值代表 challenger 更好。首个 family 只有一个可达的 risk-exit SELL hypothesis，`familywise_hypothesis_count=1`；不再枚举方向、窗口、阈值、模型或时间段，nominal 与 family-wise interval 因而相同。统计先按 target trade date 聚合，再对按日期排序的有效观测用冻结的 5 个连续 observed-target-date moving blocks、5,000 次、seed 20260906 形成 95% 区间。lower bound > 0 为 `SUPPORTED`，upper bound < 0 为 `NEGATIVE`，其余为 `INCONCLUSIVE`；不足 30 张 paired card 或 20 个 observed target trade dates 时为 `INSUFFICIENT_DATA/UNDERPOWERED`，仍交付 coverage 与 point estimate，不触发补样本审批或阻塞现有功能。

任一 data-error episode 都进入显式 coverage；只要存在未解释 data error，SELL 不得标 `SUPPORTED`，但合法 market no-fill 只作为业务覆盖率报告。只有 SELL 为 `SUPPORTED` 时，才允许后续设计把该静态窗口用于 risk-exit 卖出卡；本任务本身不写运行 policy、card、event、订单、N0 route 或任何既有模块。BUY 若未来需要研究，必须先定义一个在 T 日已冻结且不依赖 T+1 realized trigger 的数量契约，并另立 request/family；不得复用本次 SELL 结论。

多 horizon outcome 只提供 signal-decay/holding sensitivity context，不能充当 L4b-1 的 `SUPPORTED/NEGATIVE` 证据。第一阶段的义务只是冻结足够字段，使未来同方向同规模反事实可计算。

若以后得到支持，卡片只需按 side 输出这个已冻结的静态 execution window；T+1 运行时仍只读实时报价，不调用 `fetch_minute_kline_tdx`。本轮窗口是一次性设计选择，不由 Almgren–Chriss 或事后 PnL 网格挑选；该文献只提供 arrival-price、成本与执行风险背景。

### 7.2 数据现状

`EVID-MINUTE-SNAPSHOT` 证明 2026-08-31 candidate 可用于当前离线管线，但 candidate identity 不是永久能力声明。目标日在 cutoff 之后的卡记 `MINUTE_COVERAGE_PENDING`，待下一次既有 candidate 月更后以新 request 重新冻结；不原地修改旧 request/bundle，也不把数据月更变成 L1/L1a 或源码合入门禁。

管线只对 request 中的 card/date/字段直接读取 Qlib Bin；不加载全市场分钟 DataFrame，不调用 DB、TDX `kline-all`、Paper/QE adapter、worker 或 scheduler。request 绑定 immutable card artifact、对应 `CARD_ISSUED` semantic hash、minute meta/calendar/instruments 与实际消费的 feature-file hashes；bundle、receipt 和 `timing_execution_window_registry_v1.jsonl` 只写 `position_timing_advice_v1/research*` 自有路径，exact retry 为 no-op。

当前 `fetch_minute_kline_tdx` 的 `kline-all` 请求会随 symbol × poll 次数放大。第一阶段完全绕开；第二阶段若确需自适应当日分钟链路，才设计当日增量/缓存，不提前建设共享 poller 或新 worker。

### 7.3 L4b-2 盘中新方向

L4b-2 会直接增加交易次数与真实亏损敞口，因此只有独立审计证明逐腿成本后 adjusted lower bound 为正时才重新设计。该证据条件不扩散为 L1/L1a/L2/L3 的审批或人工放行。

N3 只回答 `ALPHA_RANKING`；不得用 N3 否决 L4b-1，也不得用未来执行窗口正结果推翻 N3。

## 8. 方法论与文献映射

可行原理按“是否直接服务人工建议”分层，不因技术新颖而进入实现：

| 原理 | 能回答的问题 | 本蓝图处置 |
|---|---|---|
| 风险/可交易性规则 | 已持仓是否应止损、能否交易、用户目标如何合法落地 | L1 正式采用；明确标为 rule-based risk management，不冒充 alpha |
| 趋势、相对强弱与波动 | 当前持仓继续持有的状态是否恶化 | 只取 T 时点可见的固定特征进入 L2；不单独搜索阈值 |
| 缺口/短期均值回归 | 既定方向是否应等待更好的当日价格 | L1 只执行现有 price guard；L4b-1 才独立检验分钟执行价差 |
| 动作增量价值监督学习 | 建立、增减或保持敞口是否有成本后价值 | v1 为单调退出 audit；v2 用入场/退出监督模型与唯一日频政策，评价连续净值路径 |
| meta-label / act-or-hold | 已有动作或候选是否优于不交易 | 可作为建模解释，不另建第三套模型；不能把买入推迟一天当完整入场目标 |
| barrier、hazard、最优停止 | 退出风险何时集中、删失如何处理 | 可作未来 challenger；第一批与首个 L2 不需要 |
| HMM / regime conditioning | 市场或板块状态是否改变规则有效性 | 第一批 context-only；首个 L2 不入模也不按 HMM 分层，未来增量必须另立一个冻结 feature-block 假设 |
| 成本约束动态仓位/执行 | 应一次完成还是分档、何时成交 | L1 用确定性 exposure 与逐腿成本；L4b-1 研究执行窗口 |
| TCN/Transformer/Offline RL | 是否能从长分钟序列学到更复杂控制 | 当前不采用；现有本地证据和人工建议目标不足以抵偿复杂度与模拟器风险 |

1. [Moskowitz, Ooi & Pedersen (2012), Time Series Momentum](https://doi.org/10.1016/j.jfineco.2011.11.003)研究的是期货的一至十二月收益持续性，只支持把中期趋势当作背景，不能据此声称“日内到数日不稳健”或否决分钟机制。
2. [Gârleanu & Pedersen (2013), Dynamic Trading with Predictable Returns and Transaction Costs](https://doi.org/10.1111/jofi.12080)提供有交易成本时向目标部分移动的研究背景；在本设计中启发有限仓位动作与逐腿成本，不证明单调退出最优，也不提供 A 股现成参数。
3. family-wise 结构可复用；主证据改用训练严格先于预测的 walk-forward，CPCV 只作稳健性诊断。purge/embargo 与时间先后是不同要求，不能混称历史部署因果证据；不增加人工审批。
4. [Almgren & Chriss, Optimal Execution of Portfolio Transactions](https://www.smallake.kr/wp-content/uploads/2016/03/optliq.pdf)只作为第二阶段执行成本/风险背景，不声称它定义 `AT_OPEN/ON_PRICE_TRIGGER/WAIT_UNAVAILABLE`。
5. HMM 可作为市场态势 context 与后续 diagnostic slice，但在没有 L2 支持前不参与方向、校准或四路消融。
6. [AQR, Trading Costs](https://www.aqr.com/insights/research/working-paper/trading-costs)以机构真实成交研究成本随交易规模、股票和时间变化。这支持本设计区分手续费与执行摩擦，但不能把其发达市场机构成本参数直接套给 A 股个人账户，也不证明当前择时模型有效。

方法选择以终极产品目标和本地证据为准，不以“技术更新”本身为目标。

## 9. Implementation Plan / 分阶段实施

### 9.1 已完成：蓝图冻结

- 建立证据语义目录，修正臂名、fee、board-lot、minute fetch、到期时钟与统计分类。
- 冻结 F2 contracts、Design Acceptance Index、验证路径与隔离矩阵。
- 蓝图文档已通过 F2 validator 并作为本分支实现权威；文档合入本身不曾代表功能实现。

### 9.2 已合入：实现块一 L1 contracts、artifact 与规则卡

1. 实现 `contracts.py`、`policy.py`、`artifact_store.py`。
2. 实现唯一持仓 authority、自选去重、intent 与方向性可交易性。
3. 生成 immutable card set 与 `CARD_ISSUED`。
4. 实现 componentized cost、board-lot、guard snapshots 与 typed errors。
5. 实现 materialize/current/evidence/intents API 与页面行动卡。

块一还完成了四项收口：已确认终止上市的 PIT 只读输入及买卖方向映射；已验证 T 日停牌时用更早的最近可执行 close 保留风险方向，而非停牌旧 bar 不会锁死卡片；green/yellow/skip 分支由冻结 guard 参数派生、以 guard action/reason 消歧并按各自触发价估算成本；card set 保存完整 input/policy identity 和 `cards_sha256`。复权因子不参与日频卡，明确为 `NOT_APPLICABLE`，不构成出卡门禁。PR `#4277` 的 required checks 已通过并合入；该状态仍只是完整首发内的块一源码，source merge 不改变范围判断。

### 9.3 已实现：第一批 B 的 L1a 与 prospective outcome

1. 先在同一个 `position_timing` 包、router 和页面内实现 §5.2.1 的轻量 scope 当前态与过滤；它只是本任务的一个小条目，不拆新阶段或服务。
2. 实现批量 quote GET、edge 状态机与 atomic claim POST。
3. 页面 toast、already-alerted 非模态条目、typed stale/unavailable。
4. 实现五 horizon `OUTCOME_EVALUATED`、coverage watermark 与基率聚合。
5. 完成隔离、并发、PIT、费用、scope 与 UI 结果验证。

第一批结束的用户可见结果是：日频明确卡、盘中到点提示、失败原因、成本和持续累积的结果证据。L2 当时未实现不降低这一定义；后续离线 audit 也不改变首发定义。

2026-09-05 的首发源码实现保持一个 namespace、一个页面和一个 artifact root：F-027 scope、8 个 API、`ALERT_EMISSION_AUTHORIZED`、五 horizon `OUTCOME_EVALUATED`、四态 evidence 与集中 `position_timing_first_release` 验证均已落地。块二唯一新增服务文件为 `alerts.py`；outcome 仍在既有 `service.py`，没有新增数据库、worker、scheduler、SSE、通知平台或订单接口。该状态表示首发源码已实现并完成本地验证，不表示生产进程已经加载。

2026-09-06 的运行态收口已完成：backend-main 健康、运行身份通过 `origin_main_descendant` 证明、`/api/v1/position-timing/intents` 的 target-owned collection 语义探针识别 468 个条目，canonical BUG-1365 状态为 `verified`。实时读回仍是 2 个持仓进入分析范围、2 张目标日 2026-09-07 的 `HOLD` 卡；非交易日返回 `NO_VALID_CARD_TODAY`，没有虚构触价或送达。随后 L2 audit 在 prospective outcome 为 0 的情况下按报告义务而非门禁执行，结果不构成 L1/L1a 门禁。

### 9.4 已完成：后续 C 的 L2 audit；L3 不进入实现

- `backend/services/position_timing/learnability_pipeline.py` 已按冻结 spec 实现两个模型、一个政策、两个假设，没有 router、worker、scheduler、final refit 或 runtime model；既有 evidence API/页面只展示 hash-bound 总体审计引用。
- 首个 request 因 2018～2026 人口与 2024～2026 Selection ranking 覆盖矛盾，在生成任何完整结果前 typed fail；删除两项不可用 Selection 特征后重新冻结，正式 audit 无搜索运行一次并写 timing-owned immutable bundle/registry。
- 正式结果为 Ridge `NEGATIVE`、GBDT `INCONCLUSIVE`、study `INCONCLUSIVE`、selected 0；因此 L3 不实现，L1/L1a 继续保持规则卡。
- 若未来要基于已保存的 OPEN/ADD outcome 研究 entry objective，必须使用独立设计、request、trial family 与结论；不得与本次 exit 两个假设合并或事后追加到同一 family。

### 9.5 已完成可达性修订：后续 D 的 L4b-1；L4b-2 维持范围外

- 直接绑定已 PASS 的 2026-08-31 candidate，不重复重建数据；实现一个 offline `minute_execution_pipeline.py`，不新增 router/page/worker/scheduler。
- 历史首轮 request `ptl4b1req_f74c814a87a01dce5ecc20c2` 只消费真实 prospective card；bundle `78ee08b9d712c3b62517dc740ce6f2fac3ad39ec19670d5c7ee8c72c6e1f0816` 的 immutable typed receipt 状态为 `INSUFFICIENT_PROSPECTIVE_ACTION_CARDS`、selected side 为空。该 bundle 保留可读，但双方向假设不可达，不能作为修订后 SELL-only 研究权威。
- 修订 request `ptl4b1req_f55a3338e602c2dd42a32c17` 绑定提交 `03e3db5ec53aa433bfbc3c114a3c3215d3cd3ed6`，只注册 risk-exit SELL 单假设。正式 bundle 为 `450f8c82300c4c86199097c9e10eb8b61113b50e068c0c7b27cde8ce00acb7f4`，receipt 为 `6e7f7c876ae02c844a4eaed268ea31596447f3fc623d7adc0ba9b905def13b6d`；人口仍为 2 张 HOLD、0 eligible，状态为 `INSUFFICIENT_PROSPECTIVE_ACTION_CARDS`。
- 新旧 bundle 的独立 inspect 均返回 `BUNDLE_VALID`；新 request exact retry 命中同一 bundle/receipt，own registry 由历史一条增加为两条，retry 不再追加。N0 registry/current_route SHA-256 仍分别为 `53ca1338ee5f725be38eeb217b06ebc33d99a6a75904ea1efc4c8607fbd5e067`、`5041048ee09c501ba0e5a75e8da49b08f246af27b99d6675ee10f3c20068ac29`；生产 cards/events SHA-256 仍分别为 `06c057667a9421195cd6bc434737783484322689f39a5bcc242147aad4b82bb6`、`6ac9fe1cf1d51472f7db5af05f96b98c573fd6036bd638235a69710920a5850e`。
- 后续 minute candidate 覆盖真实 risk-exit `EXIT/SELL/AT_OPEN` card 的 target date 后，按同一 CLI 重新冻结新 request 并运行；这是一项数据驱动的重复审计，不拆成新产品阶段。
- L4b-2 继续维持范围外，除非其独立证据条件成立且用户决定扩展范围。

### 9.6 下一项：PT-NEXT-004 日频模型建议闭环（设计已更新，实施未启动）

一个实施块完成：①不看收益的 core coverage/schema 与正确 action-value/两基线契约；②共享特征构建、前向训练、连续政策回放、基本分钟子区间核对及费用摩擦报告；③本地版本化模型对所选股/持仓股推断，方向与数量分离，同页实验/支持态、既有卡片/提醒接线与显式 fallback。两监督 head 组成一个联合政策，统计计数见 §6.6.3。不等 HMM，不新建 Timing Lab、模型服务、调度平台、数据库表或新页面。

完成标准分开报告：工程闭环可运行且与设计一致；研究是 SUPPORTED/NEGATIVE/INCONCLUSIVE 的哪一种；是否仍仅 L1 正式建议；运行进程是否加载新代码。阴性结果仍是完成的研究，不阻断工程交付；只有总体 shadow summary 而无个股预测，不能称本块完成。正收益是终极研发目标而不是任何一次模型实验必然交付的承诺。

本版只确定方向、契约、隔离与验证范围。它没有创建源码、request、feature snapshot、model、receipt 或 registry，也没有安装 `lightgbm`、运行训练、修改卡片或激活运行态。用户明确要求本次合入后暂停，故 `PT-NEXT-004` 的实现必须由后续单独任务启动，不得把本次文档合入报告成该功能已完成。

### 9.7 随后一项：PT-NEXT-005 执行真实性与信息增量（设计排队，未启动）

不细拆产品阶段。在已可用日频闭环上，扩大已有导出分钟的触发路径回放，量化价差/延迟/未成交及错过反弹；按不看测试收益的源准备度确定一个可选信息块，冻结新 trial 后与 core 做增量比较。优先可因果重建、覆盖充分的字段，不预设 HMM、事件或筹码必然更优。若没有 ready block，仍可独立完成执行真实性工作，不能再制造对外部模块的等待链。

基础资金守恒、两基线、前向评价与实际日频触发语义已属 PT-NEXT-004，不得留到此处才补。L4b-1 SELL-only 有真实样本及分钟覆盖后可按原 CLI 新 request 重审，但它不是此块的唯一人口；不改旧 receipt、不用历史合成卡冒充真实 prospective。新分钟方向、完整 Agent 平台和自动交易继续范围外。

## 10. Verification Plan / 验证方案

### 10.1 文档 gate

```powershell
python scripts/aistock_feature_workflow.py validate --design docs/architecture/position_timing_advice_f2_redesign_20260903.md --tier F2
git diff --check
```

### 10.2 第一批实现 gate

1. `backend/tests/position_timing/test_artifact_store.py` 与 `test_api.py`：card/intent/event schema、枚举、完整 input/policy/card hash identity，以及三个 GET 在空 artifact 根上的零写入。
2. `backend/tests/position_timing/test_universe.py`：唯一 authority、watchlist active 过滤、holding 优先去重、Paper/MiniQMT 隔离。
3. `backend/tests/position_timing/test_pit_clock.py`：`feature_available_at <= decision_as_of`，同日 15:00 后信息也 fail closed。
4. `backend/tests/position_timing/test_tradability.py`：停牌、ST、买入一字涨停、卖出一字跌停、相反方向、T+1 可卖量。
5. `backend/tests/position_timing/test_policy_snapshot.py`：v1 与 `EVID-GUARD-DEFAULTS` 逐项一致；共享默认值未修改；旧 card 不受未来默认变化影响。
6. `backend/tests/position_timing/test_cost_policy.py`：逐腿分项、父订单累计、三种拆单、阈值 58,824/117,648/235,295、买卖 lot 不对称。
7. `backend/tests/position_timing/test_card_service.py`：L1 mapping、已确认终止上市的买卖方向、真实停牌缺 T 日 bar 的最近可执行 close、非停牌旧 bar 不锁卡、green/yellow/skip 唯一映射、逐分支触发价成本、缺 Selection/HMM 的字段级降级，以及 adjustment 非门禁。
8. `backend/tests/position_timing/test_artifact_store.py`：immutable conflict、input/policy/cards/intent 篡改 fail closed、append-only、三类事件的复合幂等键/hash/时区契约与并发 lock。
9. `backend/tests/position_timing/test_outcome_materialization.py`：两只时钟、五 horizons、5 日 terminal defer、h=1 新买数量 T+1 锁定、保守日线 fill、边际 paired path、公司行动/双价格、intention-to-treat、四种读取态、水位不越过部分失败。
10. `backend/tests/position_timing/test_alerts.py`：GET 零写、50-symbol chunk、5 分钟/30 秒、有界 open/current quote identity、position/intent 漂移、atomic claim、多标签页、already-alerted 可见条目、无 minute fetch。
11. `backend/tests/position_timing/test_isolation.py`：N0/现有表/调度/SmartMonitor/Paper/MiniQMT 零写；除 `backend/main.py` 与前端导航外零反向依赖；无 order/runtime-weight 输出。
12. `frontend/tests/position-timing/position-timing.spec.ts`：卡片、typed errors、toast、研究证据区、无交易按钮。

分析范围管理不新增测试文件或独立验证 lane，直接补入既有矩阵：`test_universe.py` 验证 discovery/analysis 集合与持仓强制纳入，`test_artifact_store.py` 验证 scope 原子更新/hash，`test_api.py` 验证唯一新增接口和三个 GET 零写，`test_card_service.py` 验证 scope snapshot 进入 card-set identity，前端既有 spec 验证复选框与“下一卡片生效”。

### 10.3 已完成的 L2 v1 audit 历史验证

- `backend/tests/position_timing/test_l2_population.py`：source/episode/review identity、确定性 notional assignment、合法 board-lot、deployment weighting 与历史覆盖 fail-closed。
- `backend/tests/position_timing/test_l2_model_specs.py`：冻结 feature order、Ridge/GBDT 完整参数、环境身份与无 final fit/runtime model。
- `backend/tests/position_timing/test_l2_cpcv_identity.py`：8-block/2-validation、20 日 embargo、28 paths、每行 7 个 OOF 与 path-local exposure mapping。
- `backend/tests/position_timing/test_l2_inference.py`：threshold 0、nominal/family-wise、effect/power 正交分类、合法父订单成本敏感性与 own-registry/N0 隔离。
- 正式 request 为 `research/l2_requests/ptl2req_163c90896899e65cf3183e5e.json`，绑定代码提交 `e26881abb7366f1726b2365a398d0d04786c6eb7`；immutable bundle 为 `research/l2_learnability_bundles/eef1f771a5d8ae3c002feaf6ed46007df9a0ee3726894893abdc93cddc8f3f51`。独立 `inspect` 返回 `BUNDLE_VALID`，exact retry 返回同一 bundle/receipt 且两条 registry append 均为 duplicate no-op。
- 验证是审计证据，不是新 gate：study `INCONCLUSIVE` 直接结束本次 family，L1/L1a 继续运行，L3 不实现。
- 该次历史交付记录为 position-timing 88 tests、相邻 Advisory CPCV/registry/control 16 tests、集中 `position_timing_first_release` nox（含 TypeScript、lint、production build、Playwright）和当时 F2 validator 27/27 通过；不是本次 v2.5 重跑结果，也不证明未实现的 v2 能力。仓库既有 frontend hook/autoprefixer、Browserslist、npm audit 提示不被改写成新增发布门禁。

### 10.4 L4b-1 离线执行窗口验证

- `backend/tests/position_timing/test_minute_execution_pipeline.py`：12 项覆盖 request/card/event/minute source identity、raw price/VWAP 单位、SELL 现金差、逐腿成本、方向性跌停、缺 bar/factor/volume typed 状态、SELL 单假设统计分类、买入 ON_PRICE_TRIGGER 排除、非 risk-exit AT_OPEN EXIT 排除、零 action-card insufficient receipt、旧两方向 bundle 兼容、exact retry 与 own-registry/N0/card-event 零写入。
- 历史双方向 run 继续可 inspect，但不作为研究权威。修订后的 `EVID-L4B1-FORMAL` 已绑定干净提交与 `EVID-MINUTE-SNAPSHOT`；新旧 bundle inspect、新 request exact retry、专用 registry 两行与 N0/current-route/card/event 零写 readback 均通过。完整 position-timing 后端 100 项与 `position_timing_first_release`（TypeScript、lint、production build、目标 Playwright）通过。
- 本项只有一个 backend offline 文件和一个直接测试文件，无 router、页面、runtime import、数据库、模型、依赖或执行算法改动；继续由既有 `position_timing_backend/position_timing_first_release` lane 覆盖，不建新 nox/CI lane。

### 10.5 PT-NEXT-004 后续实现验证契约

后续实现沿用现有 position-timing validation lane，不新建 CI 平台；至少覆盖：

1. core/optional 的确定性 coverage 冻结、公式/order/hash/单位、训练与运行同构；20:00 cutoff、实际 available-at/历史版本与滞后规则，旧 v1 15:00 不变。
2. 事件 mutable upsert、DATE_ONLY、历史行业归属、资金流单位/筹码基准缺失用反例验证；optional 不阻 core，required core 缺失不能伪装成零或动态删列。
3. 两监督目标和动作候选 identity 清楚，统一政策只输出一个 action；连续 WAIT、长期/未知持有期、卖出后合法回补、无 scope 不扩池、无预算仍可方向分析但无数量/触发提醒。
4. 月度前向训练只见成熟标签、同股同日所有动作不跨折、训练/模型实际可用时间不越过服务时点；final fit/模型 hash、包版本、预处理可回读，CPCV 不混入历史部署曲线。
5. 同股同资本的买入持有/L1 两基线、动作标签与连续策略差异、现金守恒、真实数量、逐腿费用、公司行动、未成交保留、漏卖/错过上涨、unknown coverage 与模型失败 fallback 日期均进入可解释结果。
6. 两监督模型/一个联合政策/两个主比较及多重校正如实计数；overlap 不按 episode 独立、阴性/不可分辨/功效/成本敏感性不隐藏、不变门禁。
7. 本地模型→实验个股分析→证据绑定支持态→下一未签发正式卡→现有 alert 全链；无支持仍保留 L1，旧卡不重写，失败 typed、不可执行 artifact 不加载；研究统计不得显示成个股胜率。
8. v2 仅写 timing-owned 新子路径和 serving 当前态；L2 v1/L4b-1/历史卡事件/全局 N0 不改，既有 DB/模块零写入和零反向依赖；共享 defaults 不改、历史 snapshot 不漂移。
9. ON_PRICE_TRIGGER/AT_OPEN 评价与服务分支一致，分钟覆盖子区间的触发顺序、成交价、可交易性及价差/延迟差异留证，历史 replay 与真实 prospective 卡分开。
10. 不调用多智能体副作用 POST、不写 `app.analysis_records`、不使用原始自由文本或 mock trend；只读解释不改变决策。因子目录只作候选发现，不遍历 IC/PnL 回选。

上述为**计划测试，尚未实现/执行**：优先扩展 `backend/tests/position_timing/test_l2_population.py`、`test_l2_model_specs.py`、`test_l2_inference.py`、`test_card_service.py`、`test_api.py`、`test_isolation.py` 和既有前端 spec；前向/连续路径可单独建少量直接测试，不以旧文件名限制职责。PT-NEXT-005 沿用同一 lane 增补执行/feature-block 对照，不新建 CI 平台。

### 10.6 v2.5 文档复核记录

本轮只做文档与只读证据核验，不重跑训练或全量产品测试。审核按三轮收口：目标/架构、数值与因果契约、跨章节/验收范围。F2 validator 结果为 34 design items/34 matrix rows、0 warnings；逐腿费率 Decimal 复算为 1.491/6.491/7.982 bps，三个阈值为 58,824/117,648/235,295，与原成本契约一致；最终 diff 检查和提交身份记录于 PR。静态 validator 只检查设计结构/引用，不证明模型有效、代码完成或运行态已加载。

| 轮次 | 已发现并修订的设计偏差 | 对照位置 |
|---|---|---|
| 1 目标/产品 | shadow 无个股闭环、自动交易禁令误伤本地模型、scope 已实现却写待实施、强制 12 字段制造依赖 | §1～§5、§6.6.2/4、§9 |
| 2 收益/因果 | 一天买价差冒充入场目标、单调退出遗漏回补、入场年龄当未来 horizon、CPCV 当可部署收益、事件当前行冒充历史值 | §6.6.1～3/5、EVID-CPCV-BOUNDARY、EVID-EVENT-PIT-BASE |
| 3 一致性/隔离 | 历史与未来版本、正式/实验模型、监督目标与统计 family、共享规则与可交易性、计划与已验证证据逐项分开 | §10.5、§12～§15 |

DESIGN-COMPLIANCE-001 的四项逐条结论见 §15；设计更正全部在本文件，不存在另一份未同步实施计划。下一实现仍须交付 §10.5 的真实代码/API/UI/研究证据，不能引用此表代替实现验收。

## 11. Risks / 风险与失败模式

| 风险 | 设计处置 |
|---|---|
| 用户未打开页面，卡片/outcome 未及时物化 | 页面显示成功扫描水位与 `PENDING_MATERIALIZATION`；下次打开补扫，不伪装成尚未到期 |
| 发卡后用户已手工改仓或改 intent | poll 比对 snapshot hash，旧卡不弹窗并返回 typed changed 状态；新意图下一张卡生效 |
| TDX 免费源无 SLA | stale/future/source failure typed；禁止弹窗，不静默隐藏状态 |
| claim 后浏览器崩溃漏弹窗 | event 明确是 authorization；仍有效的已 claim edge 以非模态条目可见；不引入 ACK 基础设施 |
| 多标签页重复视觉提示 | 服务端 event exact-once；UI 弹窗 at-most-once 只尽力而为，数据完整性优先 |
| 荐股/Selection 进展慢 | universe 与 intent 不依赖荐股；Selection 缺失只禁用 alpha-decay context |
| HMM 缺失或漂移 | 字段级 `hmm_context_status=UNAVAILABLE`；方向不受影响 |
| 等待 HMM 导致主线停滞 | `PT-NEXT-004` 使用连续日频市场上下文直接实施；HMM 仅作为以后一个冻结增量 block |
| 把 584 个因子库变成搜索空间或以缺事件阻塞 core | 核心按不看收益的 coverage/schema 冻结；可选 block 后接；新尝试显式版本/计数，禁止冒充原无搜索结果 |
| 退出与入场 estimand 混用 | 两监督目标保持 identity，以同单位净动作价值组成唯一政策；不得把某 head 支持当联合政策支持 |
| 只优化早买一天、避跌或短期回撤 | 同股同资本的连续净值比较买入持有与 L1；计现金、回补、missed upside 和全部实际交易腿 |
| CPCV 或事件最新值造成伪历史证据 | 前向训练/成熟标签主验证；事件须还原当时版本，optional 缺失不阻 core |
| 多智能体自由文本形成不可复现信号 | 首个 v2 不用文本；事件只用 timestamp-causal 结构化字段，解释层 schema/hash-bound 且无决策权 |
| LightGBM 声明与执行环境不一致 | 后续 implementation receipt 校验 `lightgbm==4.6.0`；缺失 typed failure，不改模型、不阻塞 L1/L1a |
| 退市研究 overlay 尚非运行 authority | L1 只消费 issuer-bound、timestamp-causal confirmed terminal event，并仅用已生效 `stock_basic` 终态兜底；研究 profile 不接入；仅自选买入与持仓卖出按方向处理 |
| 用户未提供 planned full notional | 已实现 v1 为 WAIT/SIZING_INPUT_UNAVAILABLE；v2 设计保留方向研究但不虚构数量、不称成本后支持、不发买入触发提醒 |
| 未显式选择的自选股被误当成数据缺失或仍批量出卡 | discovery 与 analysis universe 分离；缺 scope 为 `NOT_SELECTED`，只对显式选择的 active watchlist 出卡；持仓始终覆盖 |
| scope 更新改写当日卡或变成第二套 watchlist | `NEXT_CARD_SET_ONLY`、card immutable；scope 只保存 selected symbol set，成员资格仍由只读 watchlist authority 决定 |
| 最低佣金实际聚合口径与假设不同 | `BROKER_UNVERIFIED` 披露 + 1/2/3 parent-order sensitivity；不阻塞 |
| 费率变化使手写阈值漂移 | Decimal 从 versioned components 派生并写 receipt，禁止业务常量 |
| 除权除息使 raw 收益失真 | raw 只用于触发/成交/费用；经济结果绑定公司行动或等价 total-return identity，缺失即 unavailable |
| 复权数据暂未成熟 | 不阻塞只使用 raw 的块一 card；实现块二 outcome 到期评价时才 fail closed 为 `UNAVAILABLE_AT_HORIZON` |
| 文件 artifact 并发冲突 | content address、file lock、fsync、atomic replace；冲突 fail closed |
| 规则卡或实验模型被误读成 alpha | L1 固定 RULE_BASED；v2 实验明确标注，只有适用联合政策支持才 MODEL_ASSISTED；研究统计只在证据区 |
| L2 双模型没有正下界 | 正式结果如实记录 Ridge `NEGATIVE + ADEQUATE`、GBDT `INCONCLUSIVE + ADEQUATE`、study `INCONCLUSIVE`；L1/L1a 不受影响，不追加同 family 搜索 |
| 合成 L2 population 与用户人口仍偏移 | 同时报 synthetic 与 prospective deployment-weighted；不用“消除偏移”措辞 |
| 分钟 candidate 截至 2026-08-31，晚于 cutoff 的 action-card 尚不可评价 | 记 `MINUTE_COVERAGE_PENDING`，后续月更后新建 immutable request；不影响 L1/L1a |
| `kline-all` 放大请求 | 第一阶段零调用；第二阶段需要时才设计增量/缓存 |

## 12. Rollout / Rollback / Production gates

### 12.1 蓝图文档历史合入边界

- 蓝图初次合入只包含 Markdown；随后首发和离线审计已分别实现，见 §9。当前 v2.5 文档分支再次仅改 Markdown，不以历史首发状态暗示本次实现了新模型。
- `production_ddl_gate=noop`。
- `production_dependency_gate=noop`。
- `runtime_activation=noop`。
- `backend_restart=noop`。
- `frontend_activation=noop`。
- 蓝图初次合入当时 DB、artifact、registry 和运行进程均未修改；该历史事实不得用于否认后续首发 artifact 或历史 timing-owned L2 research bundle/registry 的已授权写入。

文档回滚通过后续 PR revert；不删除任何 artifact 或历史证据。

### 12.2 当前首发运行面与离线 L2 边界

块一已由 PR `#4277` 合入，块二及运行态收口见 §9.3。`PT-NEXT-002` 只在显式 CLI 下写 `F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/` 与自有 `research_registry/`，没有改生产 card/event、DB、N0 或进程，没有运行模型。v2.5 已设计 timing-owned 本地模型与人工建议接线，但尚未实施；不需要新表或新服务。后续依赖/训练/激活按实际任务范围分别报告，数据库或进程变更不从本次文档授权推定。

### 12.3 本次 v2.5 文档合入边界

- 仅修改本蓝图 Markdown；`PT-NEXT-004` implementation、training、artifact、registry、dependency install、数据库与运行激活均为 `NOT_STARTED`。
- `production_ddl_gate=noop`、`production_dml_gate=noop`、`production_dependency_gate=noop`、`runtime_activation=noop`、`backend_restart=noop`、`frontend_activation=noop`。
- 本次回滚只需 revert 文档 PR；不删除或改写首发、L2 v1、L4b-1 的任何 artifact/receipt。
- 后续开始 `PT-NEXT-004` 时必须从最新主线建立独立任务，按 §6.6 与 §10.5 实现和验证；本次合入不自动授权依赖安装、训练、运行接线或生产变更。

## 13. Design Acceptance Index / 设计验收索引

`legacy_review_ref` 保留旧稿 PT-ID 的评审谱系；F-ID 是唯一规范身份。

| design_item | legacy_review_ref | level | requirement |
|---|---|---|---|
| F-001 | PT-015 | HARD | 只产出人工建议，不生成订单、经纪商部署包或自动执行权重；不禁止 timing-owned 本地模型 |
| F-002 | PT-001, PT-014 | HARD | 轻量独立 namespace/artifact；仅 composition root 薄接线，既有业务模块零反向依赖、零既有写入 |
| F-003 | PT-002 | HARD | 唯一 `LEGACY_PORTFOLIO` authority，active watchlist 合并并按 holding 优先去重 |
| F-004 | PT-003 | HARD | timestamp 级 PIT、T+1 单日 card 时钟与旧卡不顺延 |
| F-005 | PT-001, PT-002 | SCOPE | 复用本地 DB、QE exports、交易日、限价/停牌/ST 与纯 guard 实现 |
| F-006 | PT-005, PT-006, PT-013 | SCOPE | L1 由用户 intent + 冻结 guard 形成明确行动卡，不把研究臂当 runtime guard |
| F-007 | PT-003, PT-005 | HARD | ST 与一字板按方向判定，board-lot 与 T+1 可卖量正确 |
| F-008 | PT-007 | HARD | timing-owned 明细 snapshot/hash/provenance，shared defaults 零修改 |
| F-009 | PT-004 | HARD | componentized 逐腿成本、parent-order 假设与 FeeModel 边界 |
| F-010 | PT-017 | ADVISORY | Decimal 派生阈值、实际合法逐腿金额标注，不拦截小额建议 |
| F-011 | PT-008, PT-014 | HARD | own artifact/registry、immutable cards、append-only events、N0 零写入 |
| F-012 | PT-014 | HARD | 页面触发 exact-idempotent materialization，不建 scheduler/worker |
| F-013 | PT-008 | HARD | 五 horizons 的 `OUTCOME_EVALUATED`、candidate/do-nothing 逐腿配对与两只时钟 |
| F-014 | PT-008 | HARD | 四种 outcome 读取态与成功扫描水位，物化失败不可伪装为 pending |
| F-015 | PT-018, PT-019 | HARD | L1a 只用 batch quote，分钟级 poll；stale/future/unavailable 显式且禁止弹窗 |
| F-016 | PT-019 | HARD | alert claim artifact exact-once；UI at-most-once 仅 advisory，already-alerted 仍可见 |
| F-017 | PT-002, PT-016 | SCOPE | 首发 HMM/Selection 只作可缺失 context，不阻塞整卡或决定 universe；未来增量见 F-032 |
| F-018 | PT-008, PT-010 | SCOPE | 第一批冻结 L2 population/sampling/outcome 字段；后续一次性离线 audit 必须遵守同一 identity |
| F-019 | PT-009, PT-010, PT-011 | SCOPE | 已完成 L2 v1 的 Ridge+GBDT、一个 monotone policy、两个 hypotheses，无搜索、own registry；不是 v2 永久限制 |
| F-020 | PT-009, PT-010 | SCOPE | threshold 0、effect/power 正交、nominal/family-wise 与 cost-sensitive 标签 |
| F-021 | PT-009, PT-012, PT-017 | ADVISORY | MDE 报告、sealed 可选、小额标注均非批准门禁 |
| F-022 | PT-018, PT-020 | SCOPE | L4b-1 只用真实 risk-exit EXIT/SELL/AT_OPEN card、一个冻结 30 分钟 raw VWAP challenger、一个可达假设和 own artifact；其他 action/ON_PRICE_TRIGGER 不事后选 branch，N3 objective 不混用，L4b-2 保持范围外 |
| F-023 | PT-016, PT-018 | SCOPE | 第一批仅 L1/L1a/outcome 与轻量 analysis scope；无 SSE、SmartMonitor engine 或复杂模型 |
| F-024 | PT-003, PT-019 | HARD | 所有缺失/陈旧/不支持/物化失败均 typed，不静默、不空结果冒充成功 |
| F-025 | PT-001, PT-004 | HARD | card/receipt 绑定实际消费的 dataset、calendar、limit、delist、policy、fee 与 code provenance；card 未消费的 adjustment 明确 NOT_APPLICABLE，outcome 必须绑定 adjustment/corporate-action |
| F-026 | PT-014 | HARD | 文档与未来实现具备逐条测试、隔离、回滚和 production gate 证据 |
| F-027 | NEW-20260904 | SCOPE | discovery 与 analysis universe 分离；全部真实持仓始终分析，仅显式选择的 confirmed watchlist 出卡；scope 为 timing-owned 原子当前态并绑定 card-set identity，不建第二持仓池或新平台 |
| F-028 | PT-NEXT-004/005 | SCOPE | 下一块交付日频训练到个股建议闭环；随后增强执行真实性与可选信息，不等 HMM；本版不实施 |
| F-029 | PT-NEXT-004 | HARD | core/optional 分层，覆盖先于收益，精确字段/PIT/历史版本/formula/order/hash；v2 晚间 20:00 与 v1 15:00 分版本 |
| F-030 | PT-NEXT-004 | HARD | 两监督目标形成唯一动作价值政策；从当前 review 的 forward horizon、同股同资本两基线、连续现金/持仓与可回补路径 |
| F-031 | PT-NEXT-004 | SCOPE | 2 固定 GBDT/1 联合政策/2 主比较；walk-forward/月度训练/成熟标签与 local final fit；个股实验/支持态、方向数量分离与显式 fallback |
| F-032 | PT-NEXT-004/005 | HARD | 可选事件/HMM 验证增量与历史版本，不阻 core；有副作用多智能体 POST、自由文本、mock 不进入决策 authority |
| F-033 | PT-NEXT-004 | HARD | 新模型/研究/正式建议只写 timing-owned 版本化路径；旧 L2/L4b-1/card/event 不改写，N0/既有模块零写入及零反向依赖 |
| F-034 | PT-NEXT-004/005 | HARD | 验证闭环、时序/收益/执行真实性/隔离与支持态；代码完成、证据支持、上线状态分开；不新增平台门禁 |

## 14. Design Acceptance Matrix / 设计验收矩阵

`DESIGN_VERIFIED` 只证明设计条款已经闭合；`FIRST_RELEASE_*_VERIFIED` 表示首发适用范围已有直接代码和测试；`L2_*_VERIFIED` 只表示离线审计实现及其 evidence 闭合，不表示存在可部署模型。源码验证仍不表示生产进程已加载。带 `APPROVED_BY_USER` 的 gap 记录用户明确后移的实现或第二阶段研究，不是新增门禁；特别是 F-028～F-034 均不得被引用为 `PT-NEXT-004` 已有代码或模型。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/position_timing/contracts.py`；router output boundary | `backend/tests/position_timing/test_isolation.py` | BLOCK_ONE_VERIFIED | none |
| F-002 | `backend/services/position_timing/`、own artifact root、composition wiring | `backend/tests/position_timing/test_isolation.py` | BLOCK_ONE_VERIFIED | none |
| F-003 | `backend/services/position_timing/service.py` universe adapter | `backend/tests/position_timing/test_universe.py` | BLOCK_ONE_VERIFIED | none |
| F-004 | `PositionTimingCardV1` decision clock 与 PIT checks | `backend/tests/position_timing/test_pit_clock.py`；`backend/tests/position_timing/test_card_service.py` | BLOCK_ONE_VERIFIED | none |
| F-005 | service read adapters；`learnability_pipeline.py` canonical-v2 candidate loader；§4.3 reuse table | `backend/tests/position_timing/test_api.py`；`backend/tests/position_timing/test_l2_population.py`；`F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/l2_learnability_bundles/eef1f771a5d8ae3c002feaf6ed46007df9a0ee3726894893abdc93cddc8f3f51/source_identity_receipt.json` | L2_SOURCE_BINDING_VERIFIED | none |
| F-006 | `backend/services/position_timing/policy.py` 与 `service.py` L1 mapping | `backend/tests/position_timing/test_card_service.py`（含 confirmed delist 买卖方向） | BLOCK_ONE_VERIFIED | none |
| F-007 | shared limit/suspend/board-lot adapters、冻结方向分支与 target-day quote recheck | `backend/tests/position_timing/test_tradability.py`；`test_card_service.py`；`test_alerts.py`；`test_outcome_materialization.py` | FIRST_RELEASE_VERIFIED | none |
| F-008 | timing-owned guard snapshot artifact/hash/provenance | `backend/tests/position_timing/test_policy_snapshot.py` | BLOCK_ONE_VERIFIED | none |
| F-009 | componentized `PERSONAL_MANUAL_COMPONENT_COST_V1` | `backend/tests/position_timing/test_cost_policy.py` | BLOCK_ONE_VERIFIED | none |
| F-010 | Decimal threshold、合法数量与 cost-heavy label | `backend/tests/position_timing/test_cost_policy.py`；`backend/tests/position_timing/test_card_service.py` | BLOCK_ONE_VERIFIED | none |
| F-011 | `backend/services/position_timing/artifact_store.py` | `backend/tests/position_timing/test_artifact_store.py`（input/policy/cards/intent tamper 与跨月幂等）；`backend/tests/position_timing/test_isolation.py` | BLOCK_ONE_VERIFIED | none |
| F-012 | router `POST /materialize` 的 card + due outcome exact-idempotent publication | `backend/tests/position_timing/test_api.py`；`test_artifact_store.py`；`test_outcome_materialization.py` | FIRST_RELEASE_VERIFIED | none |
| F-013 | `OutcomeEvaluatedEventV1` 与 `service.py` 五 horizon paired materializer | `backend/tests/position_timing/test_outcome_materialization.py`（两只时钟、保守 fill、逐腿路径、公司行动、终值一字跌停顺延与历史 limit identity） | FIRST_RELEASE_VERIFIED | none |
| F-014 | `materialization_state.json`、四态 reader 与水位推进 | `backend/tests/position_timing/test_outcome_materialization.py`（partial 不推进、missing 不冒充 pending） | FIRST_RELEASE_VERIFIED | none |
| F-015 | `position_timing/alerts.py`、GET poll 与 TDX batch constants | `backend/tests/position_timing/test_alerts.py`（50 只、5 分钟/30 秒、漂移与 typed failure） | FIRST_RELEASE_VERIFIED | none |
| F-016 | atomic claim 与 `ALERT_EMISSION_AUTHORIZED` append | `backend/tests/position_timing/test_alerts.py`；`test_api.py`；`test_artifact_store.py` | FIRST_RELEASE_VERIFIED | none |
| F-017 | card context fields and typed field-level unavailability | `backend/tests/position_timing/test_card_service.py` | BLOCK_ONE_VERIFIED | none |
| F-018 | `contracts.py` population/sampling schema；`learnability_pipeline.py` materializer | `backend/tests/position_timing/test_l2_population.py`；formal `learnability_receipt.json` 96 cohorts/388,035 episodes/7,351,727 review rows | L2_POPULATION_VERIFIED | none |
| F-019 | frozen Ridge/GBDT/monotone policy offline pipeline；no final fit/runtime model | `backend/tests/position_timing/test_l2_model_specs.py`；`backend/tests/position_timing/test_l2_cpcv_identity.py`；formal `manifest.json` and CLI inspect receipt | L2_PIPELINE_VERIFIED_NO_RUNTIME_MODEL | none |
| F-020 | threshold/effect/power/inference implementation、immutable receipt 与 hash-bound evidence summary | `backend/tests/position_timing/test_l2_inference.py`；`backend/tests/position_timing/test_api.py`；formal `learnability_receipt.json`；frontend target spec | L2_INFERENCE_AND_VISIBILITY_VERIFIED | none |
| F-021 | frozen non-gating semantics；own registry delivery | `backend/tests/position_timing/test_l2_population.py`；`backend/tests/position_timing/test_l2_inference.py`；formal `learnability_receipt.json` and unchanged N0 SHA-256 | L2_NON_GATING_VERIFIED | none |
| F-022 | §7 corrected reachable EXIT/SELL population/window/statistics/source/artifact contract；`backend/services/position_timing/minute_execution_pipeline.py` | `backend/tests/position_timing/test_l2_population.py`；`backend/tests/position_timing/test_minute_execution_pipeline.py`（12 项）；old count=2 bundle inspect；`EVID-L4B1-FIRST-AUDIT` retained as history；`EVID-L4B1-FORMAL` | L4B1_AUDIT_COMPLETED_INSUFFICIENT_DATA | none |
| F-023 | 首发 8 API/one page、块二新增 `alerts.py`；L2 v1 新增 offline `learnability_pipeline.py`，后续 L4b-1 另有 minute pipeline；v2 演进单列 F-028～F-034 | `backend/tests/position_timing/test_api.py`；`backend/tests/position_timing/test_isolation.py`；`backend/tests/position_timing/test_l2_model_specs.py`；`frontend/tests/position-timing/position-timing.spec.ts` | FIRST_RELEASE_SCOPE_AND_L2_ISOLATION_VERIFIED | none |
| F-024 | card/scope/quote/claim/outcome typed reason and coverage states | `backend/tests/position_timing/test_api.py`；`backend/tests/position_timing/test_alerts.py`；`backend/tests/position_timing/test_outcome_materialization.py` | FIRST_RELEASE_TYPED_FAILURES_VERIFIED | none |
| F-025 | immutable card identities；outcome identities；L2 request/dataset/derived/contract/code/receipt identities | `backend/tests/position_timing/test_artifact_store.py`；`backend/tests/position_timing/test_policy_snapshot.py`；formal `source_identity_receipt.json`/`manifest.json`；exact retry readback | FIRST_RELEASE_AND_L2_IDENTITY_VERIFIED | none |
| F-026 | module-owned concentrated nox/catalog/F2 routing、L2 target tests and isolation | `python -m nox -s position_timing_first_release`；`backend/tests/position_timing/`；`backend/tests/position_timing/test_isolation.py`；`python scripts/aistock_feature_workflow.py validate --design docs/architecture/position_timing_advice_f2_redesign_20260903.md --tier F2` | FIRST_RELEASE_AND_L2_LOCAL_GATE_VERIFIED | PRODUCTION_RUNTIME_ACTIVATION_SEPARATE_APPROVED_BY_USER |
| F-027 | `PositionTimingAnalysisScopeV1`、单一 PUT、既有页面复选框与 card-set scope identities | `backend/tests/position_timing/test_universe.py`；`backend/tests/position_timing/test_artifact_store.py`；`backend/tests/position_timing/test_api.py`；`backend/tests/position_timing/test_card_service.py`；`frontend/tests/position-timing/position-timing.spec.ts` | FIRST_RELEASE_VERIFIED | none |
| F-028 | 设计 §2.2、§6.6、§9.6/7；代码未实现 | artifact: 本文件 §10.6 文档复核；`EVID-HMM-RUNTIME-20260907` | DESIGN_VERIFIED | PT_NEXT_004_005_IMPLEMENTATION_NOT_STARTED_APPROVED_BY_USER |
| F-029 | 设计 §6.6.2；代码未实现 | artifact: 本文件 §10.6；`EVID-EVENT-PIT-BASE`、`EVID-V2-SOURCE-NAMES`；§10.5 计划测试 | DESIGN_VERIFIED | PT_NEXT_004_IMPLEMENTATION_NOT_STARTED_APPROVED_BY_USER |
| F-030 | 设计 §1.1、§6.6.1/5；代码未实现 | artifact: 本文件 §10.6 收益复核；§10.5 计划连续路径/资金守恒测试 | DESIGN_VERIFIED | PT_NEXT_004_IMPLEMENTATION_NOT_STARTED_APPROVED_BY_USER |
| F-031 | 设计 §6.6.3/4；代码未实现 | artifact: 本文件 §10.6；`EVID-CPCV-BOUNDARY`；§10.5 计划前向/个股推断/API/UI 测试 | DESIGN_VERIFIED | PT_NEXT_004_IMPLEMENTATION_NOT_STARTED_APPROVED_BY_USER |
| F-032 | 设计 §6.6.2/5；可选接入未实现 | artifact: 本文件 §10.6；`backend/services/event_signal/announcement_adapter.py`；`backend/services/analysis_service.py` | DESIGN_VERIFIED | PT_NEXT_004_005_IMPLEMENTATION_NOT_STARTED_APPROVED_BY_USER |
| F-033 | 设计 §4.4、§6.6.4；新版本接线未实现 | artifact: 本文件 §10.6；§10.5 计划历史 hash/零写入/反向依赖测试 | DESIGN_VERIFIED | PT_NEXT_004_IMPLEMENTATION_NOT_STARTED_APPROVED_BY_USER |
| F-034 | 设计 §10.5、§12.3；未来实现验证未执行 | artifact: 本文件 §10.6；F2 validator 与 diff check 结果见本次文档 PR | DESIGN_VERIFIED | PT_NEXT_004_005_IMPLEMENTATION_NOT_STARTED_APPROVED_BY_USER |

## 15. DESIGN-COMPLIANCE-001 最终复核

1. **禁止简化交付**：已完成首发、L2 v1 与 L4b-1 的历史事实/receipt 保留；本轮只交付蓝图，不把计划测试当已通过、不把 2 张 HOLD/0 成熟结果当收益证据。PT-NEXT-004/005 明确未实现；未来只有总体研究摘要而没有个股推断不能称第一块完成。代码交付、研究支持与运行激活分开。
2. **禁止静默错误**：保留 v1 typed PIT/source/scope/quote/outcome 语义；新设计追加历史可见版本、晚间 cutoff、forward-label 可用时间、no-fill 与 unknown、模型失败 fallback 的明确区分。CPCV 不冒充历史部署收益，未预算的方向不冒充成本后可执行建议。
3. **禁止改变业务逻辑**：按用户本轮授权把评审结论同步到唯一蓝图：日频模型闭环、同股净超额目标、core-first、前向训练、连续回补和本地推断；不修改共享 defaults、旧 v1 卡/receipt 或既有模块。新版本允许人工模型建议而不允许自动交易；支持态仅影响下一未签发卡，不改当日承诺。
4. **禁止私增门禁审批**：无样本/MDE、最低金额、HMM/事件、券商核验、sealed holdout 或人工审批阻断。因果/数量/身份错误只阻止对应无效计算或提醒；收益支持控制证据措辞与正式模型政策，不阻断研究、实验分析和 L1 发布。只分两个后续实施块，不新增平台或部署审批链；L4b-2 仍范围外。

结论：首发规则能力已上线，L2 v1 无入选模型、L4b-1 无 eligible prospective action-card；目前尚无个股模型 alpha 的已验证运行功能。下一块 PT-NEXT-004 实现日频训练到个股建议和评价完整闭环，PT-NEXT-005 强化执行真实性及可选信息增量；不等 HMM、不做模型平台、不承诺单次研究必有正收益。本次仅交付 v2.5 蓝图，后续 implementation/training/artifact/registry/dependency/DB/runtime 未启动。production DDL/DML/dependency 与运行激活均 noop；最终合入与 root 同步以文档 PR 实际状态报告。
