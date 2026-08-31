# HMM Evolution Phase 2 风险监控与预警 F2 实现级详细设计

- 文档类型：F2 从属实现级详细设计 / Feature Card
- 日期：2026-07-22
- 修订日期：2026-08-31
- 状态：`G2_A_HR1_FORMAL_NOT_AVAILABLE_RW1_EXACT_CONTRACT_SOURCE_IMPLEMENTED_VERIFIED_PENDING_PR_AND_FORMAL_24_FIT`
- 父级唯一产品目标权威：`docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md` v2.36
- 上游权威：`docs/architecture/hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md` v2.8
- Feature tier：F2
- Design Acceptance Index：F-011、F-012、F-013
- 当前边界：既有C-001～C-010、B3 D3-D7、P6/D5/D6、TRAIN-STABILITY与TRANSITION-DWELL-B均保持原始identity和blocked结论；P2-3A/P2-3B仍为`NOT_AVAILABLE_FOR_PROMOTION`，P2-3C/P2-4以acceptance canonical=`16004b245346ae05a770433efdc42a7e7dccc8f93ec91a5edfb369168d787c87`和`status=NOT_AVAILABLE`终结。C-013 P1/P2A/BUG-1193、`C-013-G2A-DATA-A`及601日0-fit预检已经闭合。C-012-RL1/HR1正式历史回放已在冻结输入上执行：fresh process 1完成五fold的10个fit后，因`median spread=0.002724888242<0.003`、OOF Rank IC NW t=`1.014589794950<1.645`、OOF spread NW t=`0.385222508333<1.645`而以`hmm_risk_rotation_l1_development_effect_unavailable`停止；未执行第二进程、final fit、holdout、selection或model/READY。用户已精确批准RW1 D1～D6：252日rolling Ridge、120日feature warmup和historical pre-frozen eligibility；授权源码与测试，不授权正式24-fit、model/READY、数据库或runtime。

本文只细化总体蓝图已批准的 Phase 2。它不建立第二套产品方向，不修改 Selection、Advisory、
Paper v2、MiniQMT、StrategyPackage、QE 或现有 `hmm_risk_gate_v1` 消费者的业务语义。
Phase 2 的输出是研究分析事实，不是交易门禁、可买性、调仓或模型晋级结论。

本文是从属实现展开，不是第二份产品目标权威。若本文中的历史诊断、artifact、receipt、实施顺序或状态描述与父蓝图v2.36冲突，以父蓝图为准并修订本文。§23及各历史DIAG/REFIT章节仅保存决策与审计来源，不构成后续任务清单，不得以继续扩展这些章节为产品交付。

## 0. Feature Card / 功能卡

### 0.1 用户结果

用户可以在 `/hmm-risk` 查看指定 HMM candidate 在最新共同完成交易日的申万 L1/L2 状态、
最近 7 个完整交易日热力图、今日预警、固定详情、事件生命周期和版本化回测报告。所有页面事实来自
`hmm_risk.*` 的真实 API；数据缺失、输入漂移、部分失败和 renderer 错误均显式展示。

### 0.2 成功边界

- F-011：canonical product bundle、共享PIT/identity、market-regime component、rotation L1/L2、risk L1/L2、四层独立能力验收与coverage/abstention完整。P2-4已正式`NOT_AVAILABLE`且未形成model/READY；不得以L1 directional局部通过、L2 Rank IC显著、coverage局部通过、插补、默认neutral或已消费holdout重试冒充CAPABILITY_AVAILABLE、FULL_READY或Phase 2完成。共同水位、revision/dedupe、迟到数据重算属于P2-7，不再作为F-011模型验收前置。
- F-012：所有生成、查询和报告均为 advisory-only，只写 `hmm_risk.*`，不产生任何交易副作用。
- F-013：G2-A先以真实 API/UI 完成一个历史完整交易日的L1轮动热力图、能力状态、coverage/abstention、validation basis与forward status；G2-B再扩展L1/L2、7日历史、今日预警、固定详情、状态分布、事件与产品指标。G2-A纵切可独立验收，但不冒充完整F-013或FULL_READY。

### 0.3 交付顺序

交付顺序以父蓝图v2.36 Gate 2的三个业务闭环为唯一优先级：G2-A的C-013输入闭包已完成，HR1 expanding-window正式回放已以`ROTATION_L1_NOT_AVAILABLE`终止。当前按已批准RW1 fixed rolling-Ridge精确合同实施源码和测试；正式24-fit另行授权。通过后继续单历史交易日真实prediction、最小repository/read API、真实L1热力图和浏览器验收，失败则终止该模型方向。G2-B在该真实纵切上扩展多日历史、transition/severity、预警、产品指标、详情及后续已验收能力；G2-B通过后才进入G2-C日任务与集成。数据输入、fit、bundle和首个API/UI仍是同一闭环的内部动作，不得重新拆阶段。未来forward必须回到`as_published_pit`，不得把historical backcast或historical eligibility冒充forward因果事实。不得采用“先完成模型bundle、以后才做真实功能”“先完成平台、最后才验证预测”、重跑旧HR1、window grid、已消费holdout调参、四能力并行搜索或把局部能力冒充FULL_READY的顺序。

## 1. Background / 背景与当前代码事实

### 1.1 可复用事实

- `hmm_evolution.candidate` 已提供 content-addressed `candidate_id`、`manifest_hash`、
  `artifact_sha256`、`algorithm_version`、`source_ref.snapshot_id/config_id` 和 lifecycle。
- `backend/services/hmm_evolution/market_repository.py` 已有只读 transaction 与
  `latest_common_completed` watermark 模式，可复用连接与错误语义，但 Phase 2 需增加 sector/index 数据集。
- `scripts/precompute_hmm_risk_gate.py` 能恢复 legacy sector HMM、构建 observation、执行 causal
  forward-filter 并输出 `hmm_risk_gate_v1`。
- `backend/services/selection_center/hmm_risk_gate_runtime.py`、
  `backend/services/selection_center/hmm_risk_gate.py` 和
  `backend/services/quantevolver/hmm_risk_gate_qe.py` 是旧 artifact 消费路径。
- `HMMResearchNavigation` 当前只注册已验收的“演进实验室”，符合未完成阶段不注册死页的边界。

### 1.2 当前缺陷不得复制

- 旧 precompute 对单 sector 恢复、feature mismatch 或 forward-filter 异常只打印 warning 并跳过；
  Phase 2 不允许把这种结果标为完整成功。
- 旧 Selection provider 会把 artifact/date 不可用转成空决策；Phase 2 API 不允许把不可用转成空热力图。
- QE composer 会按 glob、任意旧 artifact 和 `.codex_tmp` 路径寻找 v1 文件；Phase 2 不使用该发现规则。
- 当前模型文件至少存在两类结构；不能按条目数量、路径名或“最新”推断 parser contract。
- 多数现有 candidate config 没有显式 `sector_level`；不能静默猜测为 L1/L2。
- 旧脚本使用 `DISTINCT ON` 选取 sector duplicate 行；Phase 2 必须先证明同一 sector/date 的聚合字段一致，
  不一致时 fail loud。

### 1.3 唯一权威决策

Phase 2 新增域内的唯一计算入口为 `HMMRiskStateGenerator`，版本 `hmm_risk_state_generator_v1`。
新 API、worker、CLI 和测试均调用同一纯计算服务；任何 router、页面或 Phase 2 独立脚本不得复制
observation、posterior、state、transition、severity 或 revision 逻辑。旧 Selection/QE/precompute 路径保持冻结，
不纳入本次“唯一入口”改造，也不以重构名义迁移。

`market.sector_data` 保持只有股票/日期与 22 个 `sw2_*` 事实字段，不持久化 `l1_code`、`l2_code` 或
`mapping_in_date`。物化层只取所有 ready 的既有实盘滚动池与 QE 不可变 PIT 池在目标日的 eligible 并集，
不绑定单一 pool key；Phase 2 消费端仍按请求冻结的精确股票池解析当日 eligible 股票，再按历史日动态关联
`market.sw_index_member`；完整 mapping rows 与 canonical hash 冻结在 `hmm_risk` InputManifest/result evidence。
股票池 eligibility 与行业归属是两个已有权威，禁止在 `sector_data` 建立第三套派生身份或股票池规则。

## 2. Scope / 范围

### 2.1 In scope

- 新 schema `hmm_risk`、exact bootstrap/verify、repository 和 current views。
- candidate/model/input identity 解析、共同水位、PIT mapping/content hashes。
- 复用现有全局股票池 PIT，并动态关联 `sw_index_member`；`sector_data` schema 保持 fact-only。
- C-001-A candidate capability、C-002-A direct L1/L2 state-model-set、C-007-A stock-fact-first direct L1 observation、状态/transition/severity；禁止跨层 posterior aggregation。
- durable daily generation job、显式失败、idempotency、lease/fencing 和迟到数据重算。
- alerts、risk event lifecycle、retrospective report。
- parent blueprint 定义的 overview/heatmap/alerts/timeline/event/preview/run API。
- `/hmm-risk` 真实页面、两级 sector、7 日热力图、固定详情和可访问结构化证据。
- changed-file catalog 中新增 `hmm.risk` owner、module 和直接 test plans。

### 2.2 Non-goals / 非目标与边界

- 不返回或写入 `RiskDecision`、`can_buy`、order、cash、position、portfolio、profile、策略配置或 snapshot 状态。
- 不修改、迁移、包装、退役或改变旧 `precompute_hmm_risk_gate.py`、Selection/QE gate 的启用、protect-top、block duration、fallback 或 artifact 语义；任何旧业务路径变化均须用户另行明确确认。
- 日常 API/worker 不训练或重训 HMM，不挑选“最佳”candidate，不自动使用最新 snapshot，不淘汰研究方向；Slice 0 经批准的受控 offline direct L1 artifact preparation 是唯一例外，且不修改 candidate lifecycle。
- 不新增 heat score、资金强弱、可买性或第四种 HMM state。
- 不把 severity 当 state，不把颜色、收益或 severity 合成为伪 confidence。
- 不自动注册 scheduler，不在 FastAPI startup 启动 worker，不自动执行首次生产日任务。
- 不注册 `/hmm-research-training` 死页或 disabled tab；Phase 3 未验收前不展示该入口。
- 不把 Phase 2 的回测报告变成晋级、保护率或研究停止硬门禁。

## 3. Architecture / 架构与所有权

### 3.1 组件

```text
POST preview/run ──> HMMRiskJobService ──> hmm_risk.daily_generation_run
                                         │
manual worker --once/--drain ────────────┘
                 │ claim + fencing
                 v
HMMRiskInputResolver (read-only)
  ├─ hmm_evolution.candidate + model_train_configs/snapshots
  ├─ model artifact bytes + parser contract + SHA-256
  ├─ market calendar/index/sector inputs
  └─ PIT sw_index_member snapshot
                 │ frozen InputManifest
                 v
HMMRiskStateGenerator (pure, unique authority)
  ├─ candidate-specific direct state-evidence adapters
  ├─ L1/L2 semantic posterior validation
  ├─ transition/severity
  └─ deterministic result hashes
                 │ Transaction C: successful persistence
                 v
sector_state_timeline ─> daily_alert ─> risk_event
                 └────────────────────> retrospective_report
                                         │
GET APIs ──> /hmm-risk real UI            └─ no trading consumer
```

### 3.2 Module catalog

首个实现 PR 必须先为下列路径建立 `hmm.risk` mapping；未映射时 fail-closed，不改跑全仓测试：

- `backend/services/hmm_risk/**`
- `backend/db/init_hmm_risk_schema.py`
- `backend/routers/hmm_risk.py`
- `backend/tests/hmm_risk/**`
- `frontend/src/app/hmm-risk/**`
- `frontend/src/components/hmm-risk/**`
- `frontend/src/lib/hmm-risk/**`
- `frontend/tests/hmm-risk/**`
- `scripts/hmm_risk/**`

`module_registry.yaml` 登记 required plan `hmm_risk_backend`，recommended plan `hmm_risk_ui`；
`test_plans.yaml` 的两个 plan 只能包含上述模块的直接测试。

## 4. Contracts / 身份、输入与版本契约

### 4.1 Candidate identity

job request 必须显式提供 `candidate_id`，禁止 `latest`、display name 或排名隐式解析。resolver 必须冻结：

- `candidate_id`、`manifest_hash`、lifecycle、algorithm_version；
- `artifact_manifest.schema_version/artifact_sha256/detected_format/coverage`；
- `source_ref.snapshot_id/config_id/artifact_name`；
- snapshot status、config id、model path 的内容 SHA-256；
- config 的 `train_start/train_end/sector_level/obs_features/rolling_window` 等计算字段；
- model parser contract 与每个 sector 的 state_labels、means/covars/transmat/startprob/feature definition。

只有当前权威枚举中的 `research_only` lifecycle 可运行。snapshot 必须 completed，
config/snapshot/candidate identity 必须互相一致。model file 缺失、hash 漂移、parser 不支持、train_end 缺失或
`train_end > as_of_date` 均终止 job。路径只是定位信息，不进入权威 identity；identity 使用内容 hash。

### 4.2 Candidate capability 与 model parser contract（C-001-A）

`hmm_evolution.candidate` 的权威 artifact 全部是 `hmm_sector_coefficients`；coefficient 不等于 state。
2026-07-22 production 只读审核覆盖全部 17 个 `research_only` candidate，并按 snapshot model 内容形成
`hmm_risk_candidate_state_evidence_matrix_v1`：

| capability | candidate ids | state model evidence | approved behavior |
|---|---|---|---|
| `DIRECT_STATE_PRODUCER`，legacy/covfix family | `hmmc_947fdd0c87bfd59e5c9d1fab`; `hmmc_51125769a3e34f2a8dee4888`; `hmmc_2e0544a2211cfe070ca88fc5`; `hmmc_42966cb2bf4a89b7dc8e7e7e`; `hmmc_5260d6c9aa865290f281fe17`; `hmmc_6b0c45f51fda23121cf40852`; `hmmc_c43146fc5cf03b5574768c62`; `hmmc_4d0eb6a0a7467d7997e45b33`; `hmmc_9819877c675a2e9322b817cf` | L2 model SHA-256 `1b2179f3267c441c99fcdf7b514272991007f28e196e8b835b2f00c67644bf63`；131 个 L2 entry；每项含 means/covars/transmat/state_labels；labels 严格为三态 | 解析 model posterior；九个 candidate 共享同一 state-model identity，不能伪称九套不同状态 |
| `DIRECT_STATE_PRODUCER`，autocycle family | `hmmc_7ff01b89a2cc97e101e163ac`; `hmmc_f13f7cb4f507a4907dbae049`; `hmmc_51c740b59086c181706442a3`; `hmmc_573b2dd8892f8736e624dcf5` | L2 model SHA-256 `a0f2df5b801b20e4a725adaa7df82d01de1a8c5207c84c409a382da9b0d453ad`；131 个 L2 entry；完整 emission/transition/state_labels；labels 严格为三态 | 解析 model posterior；四个 candidate 共享同一 state-model identity |
| `COEFFICIENT_ONLY_NOT_STATE_PRODUCER` | `hmmc_646b89f809a65e1f1939f0d2`; `hmmc_fa47b5fa387cdc9862ffe01d`; `hmmc_6614b6938e0c85a6beeee32d`; `hmmc_a69ae30f0992c819cb894f8a` | pooled 4-state研究摘要，仅有 candidate/preprocess/state_utilities/transmat 等汇总，缺 startprob/means/covars，不能执行日度 posterior | API/UI 显式返回 `hmm_risk_candidate_not_state_capable`；不生成 neutral、空成功或按 coefficient/utility 猜状态 |

resolver 必须以 candidate→snapshot→model bytes 的实际 SHA-256 重新核验上述 capability，不能只信数据库路径或表格常量。
13 个 direct candidate 实际对应两个 L2 state-model identity；UI/API 同时返回 `candidate_id`、`state_model_set_id`、
`l2_model_sha256`，相同 model 的结果允许共享计算但保持 candidate 请求/审计 identity。四个 coefficient-only candidate
仍保留在 candidate inventory，不隐藏、不退役、不改变 Phase 1 生命周期；它们只是不属于 Phase 2 state producer。

### 4.3 Direct L1/L2 state model set（C-002-A）

Phase 2 不聚合 L2 posterior 生成 L1。每个可运行 family 必须先生成一个内容寻址的
`hmm_risk_state_model_set_v1`，成对包含 direct L1 与 direct L2 model artifact：

- `state_model_set_id`、family/version、producer commit、created_at；
- L1/L2 artifact URI、SHA-256、size、parser contract、sector level、expected sector set/hash；
- train start/end、共同数据水位、dataset/mapping manifest/hash、feature definition/hash、preprocess、random seed；
- 每个 sector 的 startprob/transmat/means/covars/state_labels、observation version、training row count；
- L1 每项使用的 PIT L2 constituent set/hash；两层 semantic labels 均须严格覆盖 `trending/neutral/fading`。

L2 使用上表两份已核验 model family。L1 由受控 offline artifact-preparation slice 使用现有 trainer 的 direct L1
路径生成：按 L1 下属 L2 日度行情构造 observation 后独立训练 HMM；不得复制、平均或投票 L2 posterior。
artifact preparation 与日常 worker 分离，daily worker 永不训练。每个 family 的 L1/L2 必须使用同一训练窗口、
feature/preprocess family 和冻结输入 manifest；算法差异必须形成新的 model-set version，不能静默配对。

只有 model set 的 L1 31/31、L2 131/131 全部满足
`fit_valid AND convergence_valid AND likelihood_valid AND covariance_valid AND semantic_evidence_valid AND coverage_complete AND causal_replay_passed`，
并通过 parser、三态、有限参数与全部 identity/hash 校验，该 set 才为 `READY`。任一独立状态为 failed、blocked、
insufficient 或未决时 candidate 返回 typed reason，family model set 不得为 `READY`；任一层或任一已批准 family 缺失时返回
`hmm_risk_state_model_set_incomplete`，不提供 L2-only、autocycle-only 或部分 family 完成声明。
L1/L2 均使用 `state_origin=direct_hmm`；不存在 `derived_l1_*` state origin。

### 4.3.1 Direct L1 stock-fact-first observation（C-007-A）

用户于 2026-07-23 明确批准 C-007-A。`hmm_risk_l1_stock_fact_observation_v1` 是两个 family 唯一允许的
direct L1 observation contract；禁止调用旧 `SectorHMMTrainer` 的 2-state/4 维路径，禁止平均 L2 feature、
percentile、z-score、hidden state 或 posterior，也禁止在缺失时使用 0、neutral、上一日或另一数据源填补。

#### A. PIT identity 与 canonical 31/131 sector set

artifact-preparation request 必须显式给出 immutable `universe_key`、`rule_version`、训练/验证窗口和 source
watermark，禁止默认 live/latest key。对每个交易日 `t`，eligible symbol 必须同时满足：

1. `market.stock_universe_pit_state` 对应 key 为 `ready`、`dirty=false`、rule/version 和 coverage 覆盖 `t`；
2. `market.stock_universe_pit_spans.eligible_start <= t` 且 `eligible_end IS NULL OR eligible_end >= t`；
3. `market.sw_index_member.in_date <= t` 且 `out_date IS NULL OR out_date >= t`；
4. mapping row 的 symbol、L1、L2、日期均非空，并且 symbol 只解析到一个 canonical `(L1,L2)` identity。

历史 `sw_index_member.l1_code` 允许保存申万 `industry_code` 或 canonical `index_code` 表示；resolver 的唯一行业身份权威为
`market.sw_index_member` 自身的完整成员闭包：每个 L2 code 必须同时且只指向一个 `801xxx.SI` canonical L1，同一非 canonical L1 alias
跨全部共享 L2 必须只指向一个 canonical L1，canonical L1 与 L2 的权威名称必须非空且无冲突；numeric alias 行允许不重复保存 L1 名称，
但不得以其空名称覆盖 canonical 名称。最终 catalog 必须严格闭合为 L1=31、L2=131。
`market.sw_index_classify` 不再是该路径的前置 authority，也不得在其为空时把合法 member facts 误判为 catalog 缺失。同一 symbol/date 的多条源行
只有在规范化后 L1/L2 完全相同才可合并为一个 identity，同时所有源行、有效期和 source-row hash 都必须进入
mapping manifest；规范化后仍指向不同 L1/L2、member identity 缺失、名称冲突或一对多时显式失败。禁止 `DISTINCT ON`、排序首行、
字符串前缀或当前 mapping 回填历史日。最终 canonical sector set 必须严格为 L1 31 个、L2 131 个，并冻结
按 date/symbol/L1/L2/source interval 排序的 mapping hash。

#### B. canonical stock facts 与单位

每个 eligible symbol/date 只读取以下既有事实；路径不构成 identity，表、列、row count、min/max date、内容 hash
和单位版本构成 `hmm_risk_l1_stock_fact_source_v1`：

| fact | authoritative source | canonical unit / rule |
|---|---|---|
| OHLC、volume、amount | `market.kline_daily_raw` | price=`*_li / 1000` 元；volume=`volume_hand * 100` 股；amount=`amount_li / 1000` 元 |
| total/float market value | `market.daily_basic` | `total_mv/circ_mv * 10000` 元；权重只使用前一完整交易日 `circ_mv` |
| small/medium/large/extra-large/net flow | `market.moneyflow_ts` | 所有 `*_amount * 10000` 元 |
| exact limit price | `market.stk_limit` | `pre_close/up_limit/down_limit` 为元；`limit_up=1` iff canonical close `>= up_limit-1e-4` |
| benchmark | `market.index_daily`, `000300.SH` | `pct_chg / 100` |
| calendar | `market.trading_calendar` | `is_trading=true` 的升序交易日；不得用自然日 |

价格、金额、成交量、市值、flow 或 limit row 的 NULL、非有限、负值、重复不一致、单位不符均记录 symbol/date/field
并进入 missing evidence；不得 `COALESCE 0`。停牌日以 `market.suspend_d` 和无 kline 事实证明为合法 non-observed
symbol，不进入当日 observed denominator；缺 `suspend_d` 证据的无 kline 行仍是缺失。一个 symbol/date 的重复事实
只有全字段相同才可折叠，否则该 L1/date 失败。

#### C. stock-fact-first 原始聚合

令 `S(g,t)` 为 L1 `g` 在 `t` 的 PIT eligible、canonical mapping 且当日 observed 的股票集合；
`w(i,t)=circ_mv(i,prev(t)) / sum(circ_mv(j,prev(t)))`，其中 `prev(t)` 是 calendar 中 `t` 的前一交易日。
当日市值不参与当日权重；权重只使用前一完整交易日市值，且不使用未来数据。每个 L1/date 先计算以下原始量：

- `l1_return(t) = sum_i w(i,t) * (close(i,t)/close(i,prev_observed_i)-1)`；`prev_observed_i` 必须 `< t`，
  不得跨越该 symbol 的 listing/PIT entry 之前取值；
- `l1_volume(t) = sum_i volume_shares(i,t)`，`l1_amount(t) = sum_i amount_cny(i,t)`，
  `l1_total_mv(t) = sum_i total_mv_cny(i,t)`；
- `l1_range_ratio(t) = sum_i w(i,t) * ((high(i,t)-low(i,t))/close(i,t))`；
- `l1_true_range_ratio(t) = sum_i w(i,t) * max(high-low,abs(high-prev_close),abs(low-prev_close))/prev_close`；
- 每个 moneyflow tier 和 `net_mf_amount` 均按 canonical CNY 直接求和；禁止平均股票 ratio；
- `limit_up_ratio(t) = count(limit_up=1) / count(S(g,t))`；
- breadth/dispersion 使用 `S(g,t)` 的股票 close-to-close 1/5/10 交易日 return 直接计算，分别为正收益占比、
  `STDDEV_SAMP` 和 median/mean；历史 return 只使用该 symbol 当时已发布的 price facts。

coverage 同时按 observed symbol count 与前一交易日 float-market-value weight 计算。两者均须 `>= 0.90`；
低于阈值使该 L1/date 无 observation，高于阈值也必须把缺失 symbols、fields、count ratio、weight ratio 写入 manifest。
任一时点无法解析 denominator/previous weight 时，该 L1/date 无 observation，并须在 manifest 记录具体缺失证据；
任何 L1 最终在训练窗口少于 120 个完整 observation，或在验证窗口少于 30 个完整 observation，则整个 family
制备失败；不得只跳过该 L1 后把 model set 标为 READY。

#### D. 7 维 base observation

`rolling_window=3`，且每个滚动量必须具有连续三个有效交易日，不能以短窗口或 0 补齐：

1. `daily_return = l1_return(t)`；
2. `excess_return_Nd = mean_{k=t-2..t}(l1_return(k)-csi300_return(k))`；
3. `volume_ratio = l1_volume(t) / sum_all_eligible_observed_stock_volume(t)`；分母缺失或非正时失败；
4. `limit_up_ratio` 使用 C 节 exact limit facts；
5. `volatility_Nd = population_std_{k=t-2..t}(l1_return(k))`，与批准 L2 family 的 `numpy.std(ddof=0)` 一致；
6. `net_mf_ratio = sum(net_mf_amount_cny) / l1_amount(t)`；
7. `elg_net_mf_ratio = sum(buy_elg_amount_cny-sell_elg_amount_cny) / l1_amount(t)`。

分母为零、滚动窗口不完整或任一结果非有限时该 L1/date 失败，不使用旧实现中的 0 fallback。

#### E. autocycle 追加的 13 维 sector-factor observation

先由 C 节原始量形成 L1 panel，再按现有 `all_core` family 的同名公式重新计算；不能对 L2 已变换 feature
做加权平均：

- `sector_turnover = l1_amount / l1_total_mv * 100`；
- `sf_turnover_pctile_250d_neg` / `120d_neg`：各 L1 自身 trailing 250/120 交易日 percentile rank 的负值，
  `min_periods=120/60`；
- `sf_turnover_ma5_ma20_neg = -(mean_5(sector_turnover)/mean_20(sector_turnover)-1)`，
  `min_periods=3/10`；
- `mf_net_ratio=sum(net_mf_amount_cny)/l1_amount`；
  `sf_mf_net_ratio_std_5d_neg=-sample_std_5(mf_net_ratio)`，`min_periods=5, ddof=1`；
- `small_net_ratio=sum(buy_sm_amount_cny-sell_sm_amount_cny)/l1_amount`；
  `sf_small_net_ratio_5d=mean_5(small_net_ratio)`，`min_periods=3`；
- `sf_intraday_range_5d_neg=-mean_5(l1_range_ratio)`，`min_periods=3`；
- `atr14=mean_14(l1_true_range_ratio)`，`min_periods=10`；
  `sf_atr14_pctile_250d_neg` 为各 L1 自身 trailing 250 日 `atr14` percentile rank 的负值，`min_periods=120`；
- `sf_range_vs_market_10d=mean_10(l1_range_ratio / median_31_l1(l1_range_ratio))`，`min_periods=5`；
- `sf_vol_vs_market_20d=sample_std_20(l1_return) / median_31_l1(sample_std_20(l1_return))`，
  numerator/denominator `min_periods=10, ddof=1`；
- `sf_breadth_1d`、`sf_breadth_5d` 为 C 节 stock breadth；
- `sf_excess_breadth_5d=sf_breadth_5d-mean_31_l1(sf_breadth_5d)`；
- `sf_dispersion_5d_neg=-STDDEV_SAMP(stock_return_5d)`。

`median_31_l1/mean_31_l1` 要求该日 31 个 L1 均通过 C 节 coverage；否则该日所有相关横截面 feature 无效，
不能从剩余行业重算市场基准。rolling percentile/rank 使用稳定 date/code 排序；tie 使用 pandas average-rank/pct
语义并固定 `hmm_risk_l1_sector_factor_formula_v1`。所有 13 维和 7 维 base feature 按批准的 20 项顺序写入
feature-definition manifest/hash。

#### F. preprocess、训练、semantic label 与 READY

- legacy/covfix family 使用 `identity` preprocess；autocycle family 使用 train-only global 1%/99% winsor 后 z-score；
  L1 自行拟合并冻结 center/scale，不复用 L2 数值参数，也不读取 validation 拟合 preprocess；
- 两个 family 保留已批准的 train `2022-01-01..2024-06-30` 与单一 semantic validation
  `2024-07-01..2025-03-31`，并保留各自批准的 feature/preprocess family；不得未经确认把 validation 拆成 calibration/holdout，
  也不得用新的 split 改写 semantic mapping 或 READY 业务语义；
- 3-state diagonal GaussianHMM 的 initialization、restart、likelihood、feature-scale covariance、train occupancy、
  family-global train-only selection 与 hard semantic thresholds 必须先按 4.3.2 D 补齐精确合同并取得用户确认；现有
  `random_seed=42` 单路径不能 READY，未确认的 proposed 值也不能进入实现；
  transition 先执行 `alpha=0.1` Dirichlet smoothing，再把每个 self-transition 下限约束为 `0.3` 并重归一化，
  算法参数与结果进入 artifact/hash；
- L1 semantic label 仅使用选中模型在已批准 validation 窗口上的 causal hard assignment 与 5/10/20D future excess
  utility，权重 `0.35/0.35/0.30`。validation 第一日从 fitted `startprob_` 重新开始 causal filtering，不继承 train 尾日
  posterior；任何缺态、tie 或非有限值仍 fail closed，禁止换 seed、soft authority、mean 第一列或 state index fallback。
  最小 occupancy、month/run coverage 与 utility separation 阈值尚未确认，不得由实现自行选择；
- 每个 L1 entry 保存 constituent source rows/hash、daily coverage、training rows、preprocess、startprob/transmat/
  means/covars/state labels。manifest 另保存 stock-fact、calendar、universe、mapping、formula 和 feature hashes；
- causal replay 必须证明任一 `t` posterior 只依赖 `<=t` observation。只有 L1 31/31、L2 131/131 和全部 hash/
  parser/replay 检查通过才写 `status=READY`；制备过程不写 candidate lifecycle，不注册 scheduler，不调用 daily worker。

### 4.3.2 C-008 model-preparation evidence 与诊断修订

用户于 2026-07-23 批准 C-008-A 固定种子诊断，并于同日批准 C-008-D1/C-008-B1。C-008-A 使用 producer commit
`2585fa9a06b7a2ce40280518a0b5543cb20028d8`、dataset manifest hash
`fca2069459ec730f13aa622ef4dd1631f98c43fc98e2ce0d9c6548815ade8366`、mapping manifest hash
`9cdddd98db3cacd9949ac5b7ba007c16eb66de46375e848eea676b0168b58159`，对两个 family、31 个 L1 sector、
seeds 42..49 完成 496 次拟合。canonical report SHA-256 为
`f5def29034679480fa55f2845bf9e1836cc2609b551920d5b4fad87841be9bd7`；报告明确记录
`selection_performed=false`、`ready_artifact_write_performed=false`，模型输出文件数为 0。

C-008-A 证明：legacy/covfix 在该固定 seed grid 中没有 31/31 seed，`801780.SI` 八个 seed 均缺至少一个 hard
validation state；autocycle 的部分 seed 达到 hard-label 31/31，但两个 family 的全部 seed 都至少有一个 sector
出现 negative likelihood delta。该证据否定“只替换全局 seed 即恢复完整两-family READY”的方案，但不证明所有整数
seed 永久失败，也不授权扩大 seed 搜索、按 sector 选 seed、排除 legacy 或交付 autocycle-only。

#### A. 独立 model-preparation 状态

每个 family/seed/sector 必须分别记录 `fit_status`、`monitor_status`、`likelihood_status`、`covariance_status`、
`semantic_assignment_status`、`semantic_evidence_status` 与 `model_entry_status`；family 汇总另记录
`family_model_set_status`。`monitor_converged=true` 不推导 likelihood 可接受；hard state 非零不推导 semantic evidence
充分；semantic labelable 不推导 covariance/convergence 有效；fit 完成也不推导 entry 或 model set `READY`。

现有源码仍保持 C-007-A hard validation-state semantic authority 和 `random_seed=42` 的历史路径，且继续被 C-008-A
阻塞。C-008-B3-DESIGN 定义的目标合同获批后，仅允许在后续独立实现 PR 中替换该路径；本设计更新本身不选择 seed、
不训练模型、不写 artifact。B2 posterior-weighted semantic authority 仍未批准。

#### B. C-008-B1 diagnostic-only contract

C-008-B1 只扩展只读诊断证据，不改变 hard-state 业务语义、不选择 seed、不写 model/READY artifact、不写数据库，
也不产生 runtime 副作用。它必须复用同一冻结 dataset/mapping、窗口、两个 family 和 seeds 42..49，并记录：

- train/validation hard occupancy；每个 state 的 filtered-posterior mass、normalized mass ratio、
  `ESS=(sum(w)^2)/sum(w^2)`、posterior-weighted future utility、weighted variance/standard error、时间分段稳定性；
- hard/soft utility 对照、entropy、top1-top2 margin、state-pair separation、posterior non-finite 与 sum-to-one error；
- monitor reason、iterations、tolerance、完整 likelihood history，以及 negative delta 的绝对值、相对值、位置和 terminal 标记；
- raw covariance shape/min/max、non-finite/non-positive 数量、lower/upper-bound anomaly 数量、逐 state/feature compact mask/hash，
  clip 后范围、failure stage 和结构化诊断 reason；
- producer commit、Python/hmmlearn/NumPy 版本、algorithm version、dataset/mapping/formula hashes；
- 顶层与 family 级 `selection_performed=false`、`ready_artifact_write_performed=false`。

B1 不定义或应用 posterior mass、ESS、utility separation、negative likelihood delta、covariance anomaly 的正式通过阈值；
这些指标只用于决定后续是否提交 B2 或 B3 设计。B1 报告完成不使 F-011、任何 entry 或 family model set 变为 READY。

C-008-B1 已在 producer commit `3b37fe2b2d5d45a2d9e5f888273fd52f74db31cb` 完成第二次全量运行；
canonical report SHA-256 为 `4728e75e8c059d38688bcd969d19379d71b2ad5e9cd5e07ff69138c184462722`，
dataset/mapping hashes 与 C-008-A 一致，runtime evidence 为 CPython 3.13.5、NumPy 2.3.3、hmmlearn 0.3.3。
报告再次明确 `selection_performed=false`、`formal_acceptance_thresholds_applied=false`、
`hard_semantic_authority_changed=false`、`ready_artifact_write_performed=false`，模型输出文件数为 0。

B1 证明 `801780.SI` 在 legacy family 的每个 seed 中，train hard occupancy 与 validation hard occupancy 均至少缺一态；
每个 seed 至少有一个缺失态的 validation normalized posterior mass 不超过 `0.0013361`。即使个别另一缺失态达到
`0.032652..0.040817`，同一 fit 仍存在接近零的第三态；缺失态 posterior-weighted utility 在三个 validation time
segment 中反复变号，standard error 常与 utility 或 state-pair separation 同量级。该证据不支持直接批准 B2，
也不支持把问题解释为仅 validation hard-argmax 丢失信息。

数值证据同时确认：237 个 legacy 成功 fit 中 183 个有一次 terminal negative likelihood delta；237/237 均执行
covariance clip，包含 3560 个 lower-bound 与 77 个 upper-bound cell。11 个失败 fit 均在
`raw_covariance_validation` 发现 21 个 non-finite covariance cell；其 monitor history 合计含 3025 个 non-finite entry。
因此现有 F-011-B/F-011-C/F-011-D 继续 blocked。B1 证据构成 C-008-B3-DESIGN 的输入，但不构成 B3 实现或 READY 证据。

#### C. C-008-B2 明确未批准

C-008-B2 posterior-weighted semantic authority 为 `NOT_APPROVED`。soft posterior 只保留为诊断 evidence；不得用于
semantic mapping、补足 hard 缺态、选择 restart 或覆盖 hard utility。禁止给缺失状态填 neutral、按 hidden-state index
指定标签，或从 semantic validation/future utility 反向选择 seed。

#### D. C-008-B3-DESIGN：审核修订后的批准边界与待决合同

用户于 2026-07-23 批准 C-008-B3-DESIGN 的方向：保留 hard semantic authority，使用 train-only、family-global 的
deterministic restart selection，同时要求两个 family 完整且禁止 per-sector seed 拼接、validation-driven seed picking、
neutral/index/soft fallback。本批准不自动批准随后写入文档的具体数值、算法默认值、额外 validation split 或 holdout gate。
本节在正式审核后把已批准设计合同与尚未授权的源码执行分开；D3-D7精确决策现已闭合，但本次文档任务不得实施B3、
执行fit/selection、写model或READY artifact。

##### D1. 已批准且不得漂移的方向

- legacy/covfix 与 autocycle 两个 family 均为必要交付；任一 family 不完整时，F-011-D 保持 blocked；
- restart selection 只能读取 train 数据与预先批准的 train-only 数值/结构指标，不能读取 validation observation、future
  utility、semantic labelability 或任何 READY 结果；
- 一个 family 只能选择一个 family-global restart identity 并用于全部 31 个 L1 sector；禁止 per-sector seed 拼接；
- 所有预先声明的 restart 必须完整运行并保留候选 receipt；不能遇到第一个可标注结果就停止，也不能失败后临时扩 grid；
- hard assignment 是 semantic mapping authority；B1 soft mass/ESS 仅为诊断，不得补态、覆盖 hard utility 或参与 selection；
- 不排除 legacy、不交付 autocycle-only，不改变 candidate lifecycle，不注册 scheduler，不运行 daily worker。

##### D2. 冻结数据窗口与 causal validation 语义

- fit/train 保持已批准的 `2022-01-01..2024-06-30`；只允许 train 窗口拟合 preprocess、初始化、HMM 参数和选择 restart；
- semantic validation 保持已批准的单一窗口 `2024-07-01..2025-03-31`。不得未经用户确认拆成 calibration/holdout，
  也不得用新增 holdout 改写 mapping 或 READY 验收语义；
- 选中模型后才在 validation 窗口生成 causal filtered posterior。validation 第一日明确以 fitted `startprob_` 作为 filter
  prior，不携带 train 最后一日 posterior；这与现有批准 hard-validation 行为一致；
- 5/10/20D future excess utility 只用于选中模型的 offline hard semantic mapping，不进入 observation、fit、restart
  selection 或当日 posterior；窗口、calendar、行数、source cutoff 与 content hash 全部进入 manifest；
- 如未来确需 selection split 或 final holdout，必须先提交其业务语义、样本边界、filter prior、阈值和 READY 关系供用户确认，
  不能由实现自行增加。

##### D3. initialization 与 restart：D3-01-A/D3-02-B/D3-03-A 已批准

C-008-B3-STRUCTURAL-A 已批准以下结构，不得在实现中改回库隐式默认或 validation-driven search：

- D3-01-A：两个 family 各自完整运行 family-global restarts `42..49`；每个 restart 必须完成 31/31 sector，禁止
  early stop、失败后扩大 seed grid 或按 sector 拼接 seed；
- D3-02-B：显式 KMeans `n_clusters=3`、`init="k-means++"`、`n_init=1`、`random_state=restart_seed`、
  `max_iter=300`、`tol=1e-4`、`algorithm="lloyd"`、`copy_x=true`；空 cluster 或成员少于 2 必须以最具体
  initialization reason fail closed；
- 手工初始化方向：KMeans center 作为 means，cluster population variance（`ddof=0`）作为 raw diagonal covariance，
  `startprob=[1/3,1/3,1/3]`，train hard transition count 经 `alpha=0.1` smoothing 与 self-transition floor `0.3`
  后形成 pre-fit transmat；fit 后禁止 parameter projection；
- legacy `identity` 与 autocycle train-only global 1%/99% winsor + z-score preprocess 保持已批准。

D3-03-A 于 2026-07-25 获用户明确批准，精确合同如下；该次 D3 决策本身不授权实现或 selection，也不能替代任何独立 D4/D5/D6
合同。历史 D4-01-A/D4-03-B 已被 2026-08-09 批准的 MAP-A/PERSISTENT-A 取代；active D4 不得从 D3-03-A
的 `tol` 或历史 monitor 行为隐式推导：

- algorithm version 固定为 `hmm_risk_c008_b3_d3_03_a_v1`；任何公式、参数或初始化顺序变化必须使用新版本，不得原地漂移；
- 对每个 family/sector，在批准的 family preprocess 后，仅用冻结 train observation 计算
  `R_sj = var(X_train_sector[:, j], ddof=0)`；shape 必须与 feature count 一致，每个值必须 finite 且严格大于 0，否则以
  `hmm_risk_model_initialization_failed` fail closed；`R_sj` 的 float64 identity/hash 必须进入 candidate identity；
- 固定 `ν=1.0`。令 KMeans state `k` 的成员数为 `n_k`，cluster population variance 为 `S_kj`，则手工初始化 covariance 为
  `C_init[k,j] = (n_k*S_kj + ν*R_sj)/(n_k+ν)`；不得执行 initialization clip/bounds projection；
- GaussianHMM 全参数固定为：`n_components=3`、`covariance_type="diag"`、`min_covar=0.0`、
  `startprob_prior=1.0`、`transmat_prior=1.0`、`means_prior=0.0`、`means_weight=0.0`、
  `covars_prior[k,j]=ν*R_sj`、`covars_weight=ν+1=2.0`、`algorithm="viterbi"`、
  `random_state=restart_seed`、`n_iter=300`、`tol=0.01`、`params="stmc"`、`init_params=""`、
  `implementation="log"`、`verbose=false`；不得由库默认值补全；
- `min_covar=0.0` 与 `init_params=""` 表示不允许库重新引入 absolute covariance initializer/floor；raw post-fit
  covariance 的有效性和可接受性完全由独立 D4-02-A 判断；
- `tol=0.01` 仅保留为 GaussianHMM 构造参数和历史 identity，不再作为正式 EM 停止 authority。正式训练按
  `C-008-B3-D4-01-MAP-A` 的 covariance-prior 一致 MAP objective 与 D4-02-A 联合停止；raw observed likelihood
  完整持久化为诊断，不单独决定停止或 acceptance；
- fit 后禁止 covariance/transmat 或其他参数 projection/clip；startprob、transmat、means、raw covariance、`R_sj`、prior、
  KMeans labels/counts、全部显式参数、依赖/线程环境与 algorithm version 必须进入 canonical receipt/hash。

##### D4. 独立数值验收合同：D4-01-MAP-A/D4-02-A/D4-03-PERSISTENT-A 已批准

fit、MAP convergence、raw likelihood diagnostic、covariance、train occupancy 必须是独立状态；历史
`monitor_converged=true` 不推导 MAP、covariance 或 train occupancy 可接受。D4-01-MAP-A 定义正式 convergence authority，
D4-02-A 独立定义 covariance validity/acceptance 并与 MAP 数值收敛构成联合停止条件，D4-03-PERSISTENT-A 定义
train-only hard occupancy 的 common gate 与互斥 recurrent/persistent 双路径；任一状态通过都不能覆盖其他状态失败。

C-008-B1 明确记录 `formal_acceptance_thresholds_applied=false`。随后完成的 DIAG-02 与 DIAG-03 补齐了 date-level
month/run/transition、raw covariance 与 sector-local reference evidence，但同样明确记录没有应用正式验收阈值；敏感性
grid 只用于展示候选边界影响，不能反向成为 gate。D4-03-B 是 2026-07-25 后续用户决策形成的新合同；不得把历史
DIAG-02/DIAG-04 records 改写为已执行正式 D4-03 acceptance。

DIAG-02（schema `hmm_risk_c008_b3_diag02_repeated_report_v1`，canonical SHA-256
`bd09380c74cce480489dcc6fee8a4ee739841c4a486a21a6a8deb894180ad5b2`）在同一固定单线程数值环境完成两个
fresh-process pass，每次 2 family × 8 seed × 31 sector = 496 fits；两次 canonical payload hash 均为
`899dcbb53dbaf041d05eaf1abe7b9f02f002039adedb9118dd537ae3b9706d30`。它证明但不正式验收：

- legacy 248/248 与 autocycle 248/248 均完成 fit；negative terminal delta 分别为 232/248 与 12/248，最小 relative
  delta 分别为 `-0.0012103691929454693` 与 `-1.416998811183232e-05`；该次诊断执行时 D4-01 tolerance 尚未批准，
  因而这些历史 records 不构成 D4-01-A 正式验收；
- legacy 的 `801780.SI` 在每个 seed 的 train 与 validation 都各有一个 singleton hard state，最小 train occupancy
  `0.0016638935108153079`；autocycle train 最小 hard count/occupancy 为 `36/0.061224489795918366`，但 validation 的
  seeds 44/46/47 在 `801970.SI` 各有 singleton；该次诊断执行时 D4-03/D6-01 阈值尚未批准；
- `selection_performed=false`、D4/D5-01/D6 acceptance 均未执行，不能把诊断性 score 排序称为 seed selection。

D4-02-DIAG-03（schema `hmm_risk_c008_b3_d4_02_diag03_sector_local_covariance_reference_v1`，producer
`e3aca20a83bff8fe4cd46a62184599a5206ffa05`，canonical SHA-256
`22ee3536b4dc6590c27fa6c2989bc830d3d5d336e71b193fd17801d7c62a7e43`）复用 DIAG-02 raw covariance 与冻结
dataset/mapping，只重聚合 31-sector × feature 的 sector-local train variance；没有 HMM fit/refit、seed expansion、selection、
model/READY write、数据库写入或 runtime action。它证明：

- legacy post-fit covariance/reference ratio 的 min/median/max 为
  `0.02457406582703769/1.175176670271593/486.83698774872465`，且 initialization raw variance 有 8 个 non-positive
  cell；autocycle 的 min/median/max 为
  `1.3965864305307218e-05/0.7651283051261104/13.086099936705574`，无 non-positive raw cell；
- 统一候选 `[1e-4,200]` + zero anomaly 会在 legacy 影响 24 records、3 sectors、48 upper cells，在 autocycle 影响
  38 records、6 sectors、49 lower cells；因此该候选被证据否定，不能批准为两个 family 的共同正式 bound；
- DIAG-03 不批准更宽的 envelope、family-specific threshold、非零 anomaly budget 或 clip-after-fit；在 DIAG-04 和
  2026-07-25 用户决策前，D4-02 保持 `PROPOSED_PENDING_USER_APPROVAL`。后续 DIAG-04/D4-02-A 必须且已经解释
  reference domain、lower/upper 业务含义、tolerance、zero budget 与 family preprocess 差异，没有直接用 observed
  min/max 拟合 envelope。

D3-03/D4-02-DIAG-04 使用 merge commit `a023cdb4e0368e040014f60e50d5ccb63d1b9617` 中的显式诊断实现，producer
commit `94abea6cecb320dafbc2525d9dc39bfd549b30cd`，复用同一冻结 dataset/mapping、两个 family、seeds `42..49`
和固定单线程环境，完成两个 fresh-process pass（合计 992 fits）。两次 canonical payload SHA-256 均为
`3abb384eb578dce1c39407ba55f9576f97db6f81ad54e07a8266fecf8eb19aac`，最终 repeated report canonical SHA-256 为
`2c9136d5e1c89f66c180226848f4e761b91b864edb412d5c8ebcea8f17c74c9b`；dataset/mapping manifest hash 重算一致。
它证明但在运行时没有应用正式 gate：

- legacy/autocycle 各自 248/248 fits 完成，raw post-fit covariance non-finite/non-positive cell 均为 0；
- scale-aware initialization 将 legacy 的 8 个 raw cluster zero-variance cell 显式收缩为正值；最终 raw covariance/
  `R_sj` ratio 分别为 legacy `0.002118218143529322/0.6193348632951627/17.870741762196126` 与 autocycle
  `0.0026685173135904475/0.7582900138397837/10.258349311788736`（min/median/max），不再依赖 absolute envelope；
- 严格零 tolerance dynamic lower reference 会记录 legacy 34 cells、autocycle 31 cells，全部为 lower-side；最大相对
  shortfall 分别为 `0.0018593298282513254` 与 `0.002352934077693627`，upper-side 为 0；
- raw covariance 相对当前 smoothed-posterior M-step expected covariance 的最大残差分别为 legacy
  `0.008985775242184002` 与 autocycle `0.013839448478190432`；
- DIAG-04 使用 0.5% dynamic-bound tolerance 与 2% M-step residual 做 sensitivity readback 时，两个 family 的全部
  496 records/cells 均无超界；该 readback 没有执行 family eligibility 或 selection；
- legacy train/validation hard state 最小 count 提升到 `17/6`；autocycle train 最小 count 为 `49`，但 validation 的
  `801970.SI` 仍有 4 个 singleton records。D4-03-B 后续 sensitivity 重聚合显示两个 family 的 train records 均为
  248/248、8/8 seed 满足候选边界，但该历史 artifact 的 `formal_acceptance_thresholds_applied=false` 保持不变；D6-01
  继续独立 blocked，D3-03/D4-02/D4-03 设计批准不能推导 validation semantic evidence 或 READY。

DIAG-04 的 likelihood 只读重聚合进一步确认：两个 family 各 248/248 monitor 均报告 converged，history 全部 finite，
iterations 均严格小于 300；legacy/autocycle 分别有 8/9 个 negative-delta records，全部且仅发生在 terminal delta，最大
negative relative magnitude 分别为 `1.273601785993481e-07` 与 `1.759309368731182e-05`，non-terminal negative delta 为 0。
严格 zero-negative 方案会使 legacy 的 8 个预声明 restart 全部失去 eligibility；DIAG-02 观察上界 `0.00125` 对 scale-aware
DIAG-04 又过宽。上述事实用于形成 D4-01-A 决策，但不把 DIAG-04 historical records 反写为已应用正式 gate。

D4-01-A 于 2026-07-25 获用户明确批准；以下条目保留为历史合同证据，并于 2026-08-09 被
`C-008-B3-D4-01-MAP-A` 明确取代，不再是 active convergence authority：

0. threshold/algorithm version 固定为 `hmm_risk_c008_b3_d4_01_a_v1`；任一 tolerance、比较符、denominator、状态或 reason
   mapping 变化必须使用新版本，禁止根据后续 fit 自动扩大 threshold；
1. **monitor 与 history 完整性**：`monitor_converged` 必须为 true；`history_length == iterations`、`2 <= iterations < 300`；
   history、previous/current likelihood、absolute/relative delta 必须全部 finite。未收敛、达到最大迭代、history 长度不一致或
   non-finite 分别以最具体 reason fail closed；
2. **delta 定义**：对 `t=1..iterations-1`，定义 `delta_t=L_t-L_(t-1)`，
   `relative_delta_t=delta_t/max(1.0,abs(L_(t-1)))`。denominator 必须按该公式计算，不得按 family/sector 改写；
3. **non-terminal decrease**：任何 `t < iterations-1` 的 `delta_t < 0` 均以
   `hmm_risk_model_likelihood_nonterminal_decrease` failure；不设 silent numeric epsilon，也不得降级为 warning；
4. **terminal non-negative delta**：terminal `delta_final >= 0` 时必须严格满足 `delta_final < 0.01`；恰好等于或大于
   `0.01` 均以 `hmm_risk_model_likelihood_tolerance_failed` failure。该比较与 GaussianHMM fit 的 `tol=0.01` 数值一致，
   但仍由独立 likelihood status 判定；满足时 `likelihood_status=accepted`；
5. **terminal negative delta**：固定 `tau_terminal_relative=2e-5`。若 `delta_final < 0` 且
   `relative_delta_final >= -2e-5`，model entry 的 likelihood status 为 `accepted_with_warning`，并持久化
   `hmm_risk_model_likelihood_terminal_decrease_warning`；边界 `-2e-5` 包含在 warning 范围内。若
   `relative_delta_final < -2e-5`，以 `hmm_risk_model_likelihood_tolerance_failed` failure；terminal warning 不是普通
   `accepted`，不得从 receipt、family aggregate、API 或报告中静默删除。terminal negative 路径不再附加
   `abs(delta_final) < 0.01` 条件；absolute delta 因总 log-likelihood scale 而只作持久诊断，正式通过边界以已批准的
   relative formula 为准；
6. **状态闭包与 READY 映射**：`monitor_status` 只允许 `accepted/failed/insufficient_evidence`；required monitor/history
   receipt 不存在或不可验证时为 `insufficient_evidence/convergence_valid=false`，证据存在但第 1 项任一条件失败时为
   `failed/false`，仅当第 1 项全部成立时 `monitor_status=accepted/convergence_valid=true`。`likelihood_status` 只允许
   `accepted/accepted_with_warning/failed/insufficient_evidence`；required history/receipt 不存在或不可验证时使用
   `insufficient_evidence`，不得伪造 failure evaluation。状态优先级固定为
   `failed > insufficient_evidence > accepted_with_warning > accepted`；`likelihood_valid=true` 当且仅当
   `likelihood_status in {accepted,accepted_with_warning}`。任一 likelihood failure 必须使
   `likelihood_status=failed/likelihood_valid=false`；warning 可继续接受独立 D4-02/D4-03 判断，但不能覆盖任何
   fit/covariance/occupancy/selection/semantic failure，也不能单独推导 `model_entry_status` 或 `READY`。最终状态由 reason arrays
   确定：failure 非空为 `failed`；否则 blocking 非空为 `insufficient_evidence`；否则 warning 非空为
   `accepted_with_warning`；三者均空才为 `accepted`；
7. **receipt、reason 聚合与确定性优先级**：保存完整 history 或其 immutable content-addressed payload、每个 delta 的
   index/terminal/previous/current/absolute/relative、monitor reason、iterations/maximum_iterations、fit tolerance、
   D4-01 version、`monitor_status/convergence_valid/likelihood_status/likelihood_valid`。每个 entry 必须同时保存
   `failure_reason_codes[]`、`blocking_reason_codes[]`、`warning_reason_codes[]` 与 `primary_reason_code`；不得只保留最后一个原因。
   evidence-missing reason 进入 blocking 数组，不得伪装为已执行失败验收。适用 failure reason 按固定顺序
   聚合：`hmm_risk_model_monitor_not_converged`、`hmm_risk_model_max_iterations_reached`、
   `hmm_risk_model_monitor_history_invalid`、`hmm_risk_model_likelihood_non_finite`、
   `hmm_risk_model_likelihood_nonterminal_decrease`、`hmm_risk_model_likelihood_tolerance_failed`；warning 在独立数组按
   history index 升序保存；blocking reason 按 contract field 顺序保存。
   `primary_reason_code` 为第一个 failure；无 failure 时为第一个 blocking；两者均无时为第一个 warning；三者均无时为 null。
   只要原始 history 可安全遍历，不得因发现第一个 failure 就跳过其余 evidence。family receipt 必须聚合
   warning/blocking/failure 的 sector、seed、count、code 和 evidence hash；
8. **禁止事后放宽**：未来 fit 超出 `-2e-5` 时保持 blocked，不得自动扩大 tolerance、换 seed、按 family 临时分阈值或把
   absolute delta 省略。任何合同调整必须基于新证据形成新 version，并重新取得用户批准。上述任一 failure 均设置
   `likelihood_status=failed`；不得用 monitor、covariance 或其他状态覆盖。

##### D4-01-MAP-A. covariance-prior 一致 MAP convergence authority（active）

用户于 2026-08-09 明确批准 `C-008-B3-D4-01-MAP-A`，active algorithm/version 固定为
`hmm_risk_c008_b3_d4_01_map_a_v1`：

1. **同一参数状态的 objective**：对第 `t` 次 E-step 所对应的 raw parameters，保存 raw observed log likelihood
   `L_t`。令 raw diagonal covariance 为 `c_kj`，D3-03-A 固定 `covars_weight=ν+1=2.0`、
   `covars_prior[k,j]=ν*R_sj`、`ν=1.0`，MAP objective 严格定义为
   `J_t = L_t - 0.5*Σ_kj[(covars_weight-1)*log(c_kj) + covars_prior[k,j]/c_kj]`。
   objective 只能使用该次 E-step 的同一组 raw parameters；不得混用 M-step 前后参数、public-expanded covariance、clip 或 projection。
2. **数值完整性**：`L_t`、`c_kj`、prior term 与 `J_t` 必须全部 finite，`c_kj` 必须严格为正且 shape 精确。
   任一失败以最具体的 `hmm_risk_model_map_objective_non_finite`、`hmm_risk_model_covariance_invalid` 或 shape reason
   fail closed；不得以 raw likelihood finite 掩盖 MAP invalid。
3. **MAP 数值包络**：对 `t>=1`，定义 `ΔJ_t=J_t-J_(t-1)`，
   `T_t=max(1e-8,sqrt(eps_float64)*max(1,abs(J_(t-1))))`。若 `ΔJ_t < -T_t`，以
   `hmm_risk_model_map_objective_decrease` fail closed；`-T_t <= ΔJ_t < 0` 作为显式 numeric-envelope warning
   持久化，不得静默删除。
4. **联合停止**：仅当 `abs(ΔJ_t)<=T_t` 且同一参数状态的 D4-02-A `covariance_valid=true` 时停止。
   MAP 已数值收敛但 D4-02-A 未通过时必须继续执行下一 M-step；D4-02-A 通过但 MAP 未收敛也不得停止。
   最多执行 300 次 E-step；未形成联合停止时以 `hmm_risk_model_map_joint_convergence_unavailable` fail closed。
5. **EM 执行边界**：保留已批准的手工初始化与 GaussianHMM 全参数，但正式循环显式调用 pinned hmmlearn 0.3.3
   的 `_do_estep/_do_mstep`；不得调用 `GaussianHMM.fit()` 的 raw-likelihood monitor 作为停止 authority，不得在停止后
   再执行 M-step，也不得 post-fit projection。
6. **raw likelihood 诊断**：完整保存每次 `L_t`、delta、relative delta、terminal/non-terminal identity、history hash。
   raw likelihood non-finite 仍 fail closed；raw negative delta 记录
   `hmm_risk_model_raw_likelihood_decrease_diagnostic` warning，但不覆盖 MAP authority，也不独立拒绝通过 MAP+D4-02-A
   联合停止的 candidate。
7. **D5 score 不漂移**：`D5-01-B` 的 `L_final` 固定为联合停止参数状态上的 raw observed log likelihood，source identity 为
   `map_joint_stop_raw_observed_log_likelihood`；不得改用 `J_t`、history maximum、rounded value、validation 或 future utility。
8. **receipt 与状态**：逐 entry 保存完整 MAP/raw histories、每步 objective components、numeric envelope、D4-02-A status/hash、
   joint-stop iteration、全部 failure/blocking/warning arrays 与 primary reason。`monitor_status/convergence_valid` 表示 MAP
   联合收敛状态；`likelihood_status/likelihood_valid` 表示 raw diagnostic 完整性，不再表示 raw 单调性 gate。
   MAP、D4-02、D4-03、D5、D6 与 READY 仍分别持久化，禁止互相推导成功。D5 candidate 与 READY readback 必须使用与
   writer 相同的 D4 authority 从 durable evidence 重算 receipt、状态与 reason；canonical hash 只证明 bytes 自洽，不能替代
   MAP 公式、covariance acceptance 或 train-structure comparison 的语义回读。逐层重新计算 hash 的冲突 evidence 仍须 fail closed。

形成该决策的只读证据包括：`p6_seed46_801741_map_fixedpoint_diag02.json` canonical
`758d5f3f9abc1a1f663ef8ce68956b47d34891c1227f878d52a6a54cd3269a1f`，其中 `801741.SI/seed46`
首次 MAP+D4-02-A 联合停止位于 iteration 46；以及 131-entry seed46 诊断 canonical
`44d07c8548da0607d33264cc18bdac693d716c3801f19b7d991b8f98c59e487d`。两者均为 diagnostic-only，
没有执行 selection/D6 或写 model/READY；本次批准的是上述精确合同，不是把历史诊断反写为正式 acceptance。

##### D4-L2-AUDIT-01. 既有 L2 provenance 只读审核（已完成）

C-008-B3-D4-L2-AUDIT-01 于 2026-07-25 在 main `5ef3d6e14f47176275763ec0d9fb77191b39e5f4` 完成，只读取
Git、13 个 candidate/snapshot 的 catalog 与模型目录、两份精确 L2 artifact、相邻 metadata/diagnostics/training-result 和
登记的 producer source；未重训、未修改 artifact、未选择 seed、未应用 D4 gate、未写 model/READY/数据库/runtime。结论如下：

1. 13 个 `DIRECT_STATE_PRODUCER` 的 snapshot/model path 13/13 存在，实际严格收敛为两个内容 identity：
   - legacy/covfix 9 个 candidate 均为 SHA-256
     `1b2179f3267c441c99fcdf7b514272991007f28e196e8b835b2f00c67644bf63`，131 个 L2 entry；
   - autocycle 4 个 candidate 均为 SHA-256
     `a0f2df5b801b20e4a725adaa7df82d01de1a8c5207c84c409a382da9b0d453ad`，131 个 L2 entry。
2. legacy/covfix 的 exact bytes 与 base snapshot `bbec3863-fb67-445f-938e-66f092d18696` 相同；登记的
   `training_result.json` 明确记录该 QE snapshot 未重新训练 HMM。全部 131 entry 只保存 final parameters/labels 与
   `covariance_fixed=true`，合计 `covariance_anomaly_count=1764`；没有 monitor、完整 likelihood history、raw covariance、
   train observation/reference、posterior/M-step receipt、producer/numeric environment 或 D4 contract version。
3. autocycle metadata 以 SHA-256
   `b5d2609d883b7c5fdfb9b423dea0c8ddefbc89fa5ce93052ed58e884e18b957c` 绑定 `model_diagnostics.json`。该文件只对每项保存
   `converged/em_iterations/final em_logprob`，没有完整 history 或 D4-01 delta receipt；全部 131 entry 均为
   `covariance_fixed=true`，合计 `covariance_anomaly_count=416`，没有 D4-02 所需 raw/reference/posterior/M-step receipt。
   catalog 登记的原始 diagnostic source path 已不存在，不能从原 workspace 安全补回不可变证据。
4. 可引用的 producer 路径在 `GaussianHMM.fit()` 后调用 covariance 修正和 transition projection，再序列化模型；因此
   diagnostics 中的 final logprob 不对应最终持久化参数，持久化 covariance 也不是 D4-02-A 要求的 raw fitted covariance。
5. `model_train_snapshots/model_train_jobs` 的 `completed` 只证明登记任务终结；metrics/config 未保存上述 receipt。13 个
   candidate 的 coefficient artifact manifest 也不是 model training receipt，不得用于推导 D4 通过。

AUDIT-01 的正式状态闭包为：

- 两个 family 的 262 个 L2 entry 均为
  `likelihood_status=insufficient_evidence/likelihood_valid=false`，blocking reason 为
  `hmm_risk_model_likelihood_evidence_missing`；`converged=true` 或 final logprob 存在不能替代完整 history。
- 两份 exact L2 artifact 均有明确 post-fit covariance 修正事实，违反 D4-02-A 第 7 项，故
  `covariance_status=failed/covariance_valid=false`，primary failure 为
  `hmm_risk_model_covariance_acceptance_failed`；同时保留
  `hmm_risk_model_covariance_evidence_missing`，表示 raw/reference/posterior/M-step receipt 不可回读。failure 优先于
  blocking，但不得丢弃 secondary blocker。
- 两个 family 均保持 blocked；不得 grandfather、补 metadata、复制 L1 evidence、把 `completed` 当 numeric acceptance、
  只交付其中一个 family 或写任何 READY artifact。

##### D4-L2-RETRAIN-A. 受控 L2 重训精确合同（设计已批准，正式执行因 BUG-868 合入前阻塞）

用户于 2026-07-25 批准受控 L2 重训方案 A 的设计方向。algorithm/receipt version 固定为
`hmm_risk_c008_b3_l2_retrain_a_v1`；本节只闭合未来实现合同，不授权当前文档任务执行 fit、安装依赖、选择 seed、生成
candidate/model/READY、更新 snapshot/catalog、写数据库或激活 runtime。

1. **冻结输入与 direct L2 边界**：BUG-868-A 重新冻结后的正式训练只允许使用 dataset manifest
   `c07177ddd01b324106755e47ee2cfe61a7f2916e08ccf9e888d3abf1115ebd7f`、mapping manifest
   `9cdddd98db3cacd9949ac5b7ba007c16eb66de46375e848eea676b0168b58159`、direct L2 stock-fact manifest
   `d4a5cc86f3230a7bbd5704b81e63fa16cf4dc5a074f461f28112d3c9582d1730`、共同水位 `2025-04-30`、train
   `2022-01-01..2024-06-30` 和唯一 validation `2024-07-01..2025-03-31`。L2 sector set 必须严格等于 frozen
   mapping 的 canonical 131 个集合并保存 ordered set/hash；缺 sector、额外 sector、row/hash 漂移均 fail closed。
   L2 observation 直接从 canonical L2 facts 构造，禁止从 L1、L2 posterior、semantic state 或旧 coefficient 反推。
   历史 dataset manifest `fca2069459ec730f13aa622ef4dd1631f98c43fc98e2ce0d9c6548815ade8366` 只保留为
   C-008-A/B1/DIAG historical evidence identity；它使用 previous-available `daily_basic`，不满足已审核的
   `previous_basic_date == previous_trade_date` 精确语义，因此不得用于正式 B3 request、fit、selection 或 READY。
2. **两个 family 不缩减**：legacy/covfix 固定 7 个批准 feature、identity preprocess；autocycle/all-core 固定 20 个
   批准 feature、train-only `winsor_zscore_1_99_train_global_v1`。preprocess 只在 train 拟合并进入 immutable receipt；
   任何一个 family 缺失都不能形成完整 model set。既有两份 L2 SHA 只作为 historical source identity，不作为初始化、
   fallback 或 acceptance source。
3. **完整 restart 与复现规模**：严格复用 D3-01-A 的 seeds `42..49`；每个 family/seed 必须完整运行 131/131 L2，
   不 early stop、不临时扩 grid、不按 sector 拼 seed。单个 fresh process 为 `2*8*131=2096` fits；D5-02-B 要求两个
   fresh process 时总计 `4192` fits，必须固定单线程数值环境并以 canonical parameter/receipt hash bitwise equality 验证。
   任一 process 或任一预声明 fit 缺失时 restart schedule 为 incomplete，不得进入 selection。
4. **fit/init 与独立 D4 authority**：每个 L2 fit 严格执行 D3-02-B KMeans identity 和 D3-03-A 的完整显式
   GaussianHMM/sector-local `R_sj`/`ν=1.0` initialization/prior；禁止 pre/post-fit clip/projection，禁止用旧 L2 参数作
    fallback。fit 后执行 D4-01-MAP-A 与 D4-02-A 联合停止，再执行 D4-03-PERSISTENT-A train occupancy 合同；MAP、
    raw likelihood diagnostic、covariance、train occupancy 状态独立持久化，任一失败不能被其他状态覆盖。
5. **selection/semantic 不前置**：D3/D4 fit 阶段必须记录
   `validation_accessed=false/future_utility_accessed=false`。只有同一 restart 的 131/131 L2 全部通过 D3/D4，才可成为
   L2 family candidate；本设计不执行 selection。D5-01-B 固定每个family分别选择L1/L2 level-global seed，L2只使用131-entry
   train-only min/median/mean lex receipt，不与L1 31-entry vector合并，也不得由L1 score推导L2 seed。
   selected identity冻结后才允许在唯一validation上执行D6-01-B hard semantic mapping；D6失败不得回到D5换seed。B2 soft authority、
   hidden-state index、neutral/fixed fallback 和 per-sector stitching 全部禁止。
6. **逐 entry immutable receipt**：至少保存 family/version、seed/schedule index、L2 code、ordered observation dates、
   training row count、observation/preprocess/reference hashes、KMeans identity/cluster evidence、完整 GaussianHMM parameter
    identity、dependency/numeric/thread environment、完整 MAP/raw likelihood histories、prior components、逐 iteration
    envelope/joint-stop/D4-02 evidence、
   `R_sj/ν/M_k/W_kj/C_expected/C_raw/L/U/E`、mask/count/hash、D4 statuses/reasons/warnings、raw parameter bytes/hash，及
   `validation_accessed/future_utility_accessed/artifact_write_performed`。non-finite 不能序列化为 null 后继续成功。
7. **family/repeat aggregate receipt**：聚合 131 sector 的 fit/monitor/likelihood/covariance/occupancy 状态、failure/blocking/
   warning code、evidence hash 和 candidate completeness；两个 fresh process 必须分别保存 receipt hash、model-parameter hash
   与 equality result。不得只保存 selected 或成功项，也不得在第一个 failure 后停止收集可安全遍历的其余 evidence。
8. **新 identity、禁止覆盖历史**：未来重训输出必须使用新的 content-addressed L2 model/artifact identity，不能覆盖、改写、
   删除或冒充上述两个历史 SHA。旧 candidate/snapshot/系数和研究结果保持原样；只有新的 L2 与 direct L1 在同一
   state-model-set manifest 中满足全部 D3-D6、L1 31/31、L2 131/131、parser/hash/causal replay 后才可能写 `READY`。
9. **失败与停止语义**：输入/sector/preprocess/fit/monitor/likelihood/covariance/occupancy/repeat/selection/semantic 任一
   failed、blocked、insufficient 或 pending 均禁止 READY；使用最具体现有 reason code。无 eligible candidate 时 family
   保持 blocked，不扩大 seed、不切换 family、不改变 threshold、不回退旧 L2 model，也不触发人工运行时审批。
10. **授权边界**：D4-01-MAP-A、D4-02-A、D4-03-PERSISTENT-A、D5-01-B与D6-01-B均已获用户精确确认。
    B3/L2 retrain源码和 Conda `AIstock` 依赖安装已分别完成；
    用户随后曾授权实际 5184 fits、selection 与完整验收，但执行在第一个 fit 前被 BUG-868 fail-closed 阻断。BUG-868 修复合入、
    formal grid恢复、model/READY write、PR merge与 runtime 激活继续分别执行和报告；先前执行授权不得绕过新的 frozen identity、
    clean producer 与合入边界。

##### BUG-868-A. 正式 frozen identity 与不可变 preflight 合同

1. **业务语义不回退**：保留现行 `previous_basic_date == previous_trade_date`，禁止为匹配旧 hash 回退到
   previous-available `daily_basic`、填补缺失日或改变 PIT/coverage 语义。当前 source/mapping 未变；重聚合后的 L1 aggregate
   为 `33221` rows、invalid L1 sector-date 为 `2491`，direct L2 aggregate 为 `145805` rows、invalid L2 sector-date 为 `4067`。
2. **唯一正式 identity**：正式 request 必须同时携带本节批准的 `c07177…` dataset、`9cdddd…` mapping 与 `d4a5cc…`
   direct-L2 hash；实现以 `B3_APPROVED_FROZEN_IDENTITIES` 对 preflight 的 live recomputation 和每个 formal child request 分别
   精确比较。三者任一漂移均在第一个 fit 前 fail closed，且 preflight 不得写 `candidate_ready`；禁止由 operator、child process
   或 selection 路径静默替换。历史 C-008 diagnostic loader 可回读旧 identity，但该兼容边界不得进入 formal preflight/child。
3. **preflight 输出**：`hmm_risk_b3_formal_preflight_v1` 只从同一 read-only PIT source 计算当前三个 identity，并以 canonical
   JSON 原子写入 immutable request candidate 与 preflight receipt；保存 source-template producer、当前 producer、数据库非秘密
   identity、L1/L2 aggregate/panel/invalid counts、candidate hash 和 receipt hash。preflight 与 formal child 都必须先验证 producer
   worktree clean；禁止用 dirty source 搭配 `rev-parse HEAD` 冒充 committed producer identity。目标路径存在但字节不同必须
   collision failure。
4. **无模型副作用**：preflight 必须固定记录 `fit_performed=false`、`selection_performed=false`、
   `formal_acceptance_thresholds_applied=false`、`hard_semantic_authority_changed=false`、model/READY/database/runtime write/action
   全部为 false；不得把 `candidate_ready` 解释为 model candidate、D4 acceptance、selection 或 READY。
5. **正式 runner 仍独立复核**：preflight 产出的 request 不是绕过校验的 receipt。两个 fresh process 必须各自从 source 重算
   dataset/mapping/direct-L2 hash，与 request 精确相等后才允许第一个 fit；两个 process 的 identity 和完整 payload 仍按 D5-02-B
   bitwise contract 验证。preflight 与 runner 之间的任何 source/code 漂移均 fail closed并重新进入 BUG 流程。
6. **执行边界**：BUG-868-A 合入前禁止恢复正式 grid；本 BUG 的源码、设计和定向测试完成不授权 5184 fits、selection、D6、
   model/READY、数据库或 runtime。正式 grid 仅在本 BUG 合入且用户另行确认后恢复。

##### BUG-877. frozen train stock-fact 源数据缺口的受控修复

1. **只读完整性基线**：在冻结 universe、train `2022-01-01..2024-06-30` 的 601 个交易日上，生产只读 plan 稳定得到
   `daily_basic=466`、`moneyflow_ts=1093`，candidate-key SHA-256 为
   `a69df9ec676031541824c314f154403a41fe596c410c933ebfba55aef0f696f3`，v2 full-plan SHA-256 为
   `9583b1a46a87b655c566662a77e9da8139c7507ee069367808da48e0dab0fe75`。同一批量 SQL 与直接 exact-key SQL 重算值一致，
   `db_writes=false`。2,627,746 个 canonical observed price row 的必需价格字段 NULL 数为 0，eligible/non-suspended 当日
   kline 缺失为 0，`stk_limit/up_limit` exact-key 缺失为 0，四张 source 表 conflicting duplicate group 为 0。
   466 个 daily-basic key 全部满足：Tushare exact provider row 不存在、`market.suspend_d` exact row 存在、当日 kline 不存在；
   即 466/466 均为个股停牌日，而不是本地漏采、H5 缺失或可回填的 provider 事实。现合同要求这些停牌日必须存在 exact
   `circ_mv`，因此失败对象是 circ-mv causal as-of 语义，不是生产数据落库完整性。
2. **moneyflow 缺口分类**：1,093 个 exact-key 候选分布为 `302132.SZ=591`、`689009.SH=497`、`603326.SH=2`，以及
   `603595.SH/603081.SH/002951.SZ` 各 1。`302132.SZ` 的 591 个目标日与数据库中 `300114.SZ` 的 591 个旧代码 moneyflow
   row 一一对应，required field NULL 为 0，source-row SHA-256 为
   `c43da698b9c53dc753dd81f7f05cb9b35cd183bae8564c5b7a527ec0b18c655d`；相同日期的新代码 price 与 daily-basic 也均为
   591/591。深交所公告确认公司法人主体未改变、旧代码权利义务平移至新代码，Tushare `namechange` 显示新代码从
   `2025-02-17` 启用。故这 591 行是跨源证券代码 identity 未归一，不得伪报为供应端事实缺失，也不得在 raw source 表中
   静默复制或改写代码。其余 502 行在 Tushare 按交易日全市场 `moneyflow` authority 中也不存在，普通 incremental/init 重跑
   无法恢复，禁止补零、前值填充、neutral 或 synthetic row。
3. **唯一候选 authority**：`hmm_risk_b3_stock_fact_gap_repair_plan_v2` 只能从与正式 reader 相同的 frozen universe、calendar、
   current-day PIT span、SW L1/L2 canonical mapping 与当日 observed kline 生成候选。当前已批准 daily-basic 合同同时直接检查当日
   `(t,ts_code).total_mv` 与精确前一交易日 `(prev(t),ts_code).circ_mv`，禁止使用“当前已有行的前一条记录”间接代替 exact
   calendar identity。moneyflow candidate 是 `(t,ts_code)` row 不存在。已有 row 但 required field 为 NULL 必须使用独立 typed
   failure，禁止把它混成 insert-only candidate 或覆盖更新。plan 必须把 canonical `selected_datasets` 写入 plan hash；默认同时审计
   `daily_basic/moneyflow_ts`，允许显式生成 `daily_basic`-only exact plan 以隔离供应端不可恢复的 moneyflow，但不得静默缩小范围。
   当前 production `daily_basic`-only plan 为 466/0，candidate-key SHA-256
   `4c1a755a14e40bfd6d734381e5b7757e4bd7809100ffc605118932a5be0f0e29`，plan SHA-256
   `5b48d1fa46f3a5a5f01c7a0b3e1778b24471007d738575b2a29d349577d83709`，`db_writes=false`。
4. **provider 与最小写入**：provider 必须为现有 Tushare `daily_basic`/`moneyflow` authority，moneyflow 必须按交易日读取
   全市场后筛选 plan exact key；按 symbol 查询旧历史返回空集，不构成缺失证明。plan 中每个 key 必须恰有一条同 identity provider
   row；缺 key、额外 key、重复 key、required numeric NULL/non-finite，或 daily-basic `close/total_mv/circ_mv<=0` 均在写入前
   fail closed。apply 只能执行 `INSERT ... ON CONFLICT DO NOTHING`，不得 `DO UPDATE`、不得覆盖任何现有事实，不得自行执行
   `300114.SZ -> 302132.SZ` alias rewrite。不得写 PIT/mapping/kline/sector_data/HMM artifact。
5. **并发、readback、幂等与 rollback**：plan 固化完整 ordered candidate keys、candidate hash 与 plan hash。apply 在事务 advisory
   lock 后从数据库重算候选，必须与 plan 完全相等；任一漂移、并发 conflict、provider key 不完整或 partial insert 使整个事务
   rollback。commit 前逐 key 回读并与 provider canonical row/hash 完全一致。相同 plan 再执行时，只有全部 durable row 与 receipt
   完全相同才返回 `already_applied/db_writes=false`。rollback 只允许 `status=applied/db_writes=true` receipt，且当前 durable row
   必须与 receipt 逐字段完全一致；不得删除 pre-existing row、不得范围 DELETE、不得把 candidate 消失伪装成成功。
6. **DEV/production 边界**：当前 DEV frozen source 不覆盖全部 601 个交易日，正式 DEV preflight 已 fail closed，未执行 apply。
   现有 DEV 数据库已完成事务内合成 exact-row smoke：insert/readback/exact-delete 均为 1，最终 transaction rollback，持久行数为 0；
   该 smoke 只证明 SQL 写入、逐字段回读和 rollback 机制，不替代 frozen-source preflight。
   preflight/readback 是只读；apply/rollback 必须显式给出 env-file、target 和固定 confirmation token。当前 466+1093 候选均没有
   同 identity authoritative provider row，故 `production_dml_gate=noop_no_authoritative_candidate`，不得执行 repair DML。证券代码 alias
   合同、Tushare 不可用的 502 行处置、HMM 5184 fits、D5/D6、model/READY、服务重启与 runtime activation 均分别 pending，必须另获
   用户明确授权。Local Data MCP 生成的 `ldmp_f0741df963256b8e495142eb`/`ldmp_eba647da9ec4a9202422867c` 是 generic
   incremental plan，未绑定上述 exact keys，不得作为 BUG-877 apply authority。
7. **C-009-A（用户于 2026-07-27 批准，源码实现中）**：对当日可交易股票 `t` 的 causal float-market-value denominator，改为该股票在
   `prev_market_trade_date(t)` 当日或之前的最新 authoritative `daily_basic.circ_mv`，并在 manifest/row evidence 中保存
   `circ_mv_source_date` 与 market-trading-day staleness；source date 必须 `<t`，不得使用当日或未来 row。不存在任何 causal row 时
   继续 fail closed。该语义与 price history 使用前一实际观测日一致；批准范围只允许 reader/source evidence 与 601 日只读预检，
   不授权重跑 5184 fits、selection、D6 或生成 model/READY。
8. **C-009-B（用户于 2026-07-27 批准，源码实现中）：按数据源和生效日期解析证券身份**。禁止把 `300114.SZ` 的历史事实复制、更新或伪装为
   `302132.SZ` 的 raw source row。正式 request 必须新增 immutable
   `hmm_risk_security_source_identity_manifest_v1` 及其 canonical SHA-256；每个 mapping row 至少包含
   `security_identity_id/canonical_ts_code/source_dataset/source_ts_code/effective_start/effective_end/authority_ref/authority_hash/row_hash`。
   manifest 顶层固定 `default_resolution=canonical_same_code`，它是 data join 之前执行、由 manifest hash 约束的显式 identity rule：
   只有不存在命中生效区间的 explicit alias 时才解析为同代码，且不得先查询同代码事实再因缺失改试 alias；命中 explicit alias 时
   source key 必须直接是 alias。default 与 explicit row 的 resolution kind、authority/hash 均须进入 row evidence。
   resolver 的输入严格为 `(canonical_ts_code, trade_date, source_dataset)`，输出必须恰好一条 source identity；零条返回
   `hmm_risk_stock_fact_source_identity_unresolved`，多条、有效期重叠、authority/hash 不完整或同一 identity 下事实冲突返回
   `hmm_risk_stock_fact_source_identity_ambiguous` 并 fail closed。不得按名称相似、代码前缀、当前代码、首行、最近行或“先同代码、
   缺失再试 alias”猜测。mapping 只改变事实连接键，不进入任何 feature、label、selection score 或 semantic utility；row evidence
   必须同时保存 canonical code、实际 source code、stable identity、source dataset、有效期和 mapping hash。当前 591 个历史日仅在
   `market.moneyflow_ts` authority 下解析为 `300114.SZ`；price/daily-basic 已有事实不得被该 dataset-specific mapping 改写。
   该合同是通用 resolver/manifest，不允许把单个 hard-coded `if 302132` 当成完成实现；首版 manifest 只能纳入具有权威主体连续性、
   明确新旧代码与生效区间证据的 row。它不会新增生产表，`production_ddl_gate=noop`。
9. **C-009-C（用户于 2026-07-27 批准，源码实现中）：provider absence 使用显式 NA，不改变既有 coverage authority**。Tushare trade-date authority
   不存在的 502 个 moneyflow key 在 stock-fact row 中保持 required moneyflow fields=`None/NA`，同时写入
   `fact_status=provider_absence`、dataset、canonical/source identity、trade date、missing fields 与 provider-audit receipt hash；
   本地 source join 为空本身不能证明 provider absence；只有 immutable `hmm_risk_provider_absence_manifest_v1` 中与
   Tushare trade-date full-market audit receipt 哈希绑定的 exact key 才能标记为 provider absence，其他缺失必须以
   `hmm_risk_stock_fact_provider_absence_unverified` fail closed。manifest/request/dataset 必须绑定相同 canonical SHA-256。
   禁止删除 expected symbol、填 0、前值、均值、neutral、行业代理、其他 family 输出或 synthetic row。股票日 NA 不直接进入
   GaussianHMM；`aggregate_l1_day` 仅以所有 required stock facts 均 finite 的 observed row 形成 approved numerator，expected
   denominator 仍是当日 PIT eligible、非停牌股票全集，并继续执行 C-007-A 已批准的
   `count_coverage >= 0.90 AND weight_coverage >= 0.90`。本合同不把阈值改成 95%，不增加新的 staleness、股票排除或人工审批门禁。
   coverage 通过时最终 L1/L2 feature 必须全部 finite，且 manifest 仍保留所有 NA key 和 coverage；coverage 不通过时仅该
   sector/date 为 typed invalid observation，后续 120/30 row、四个 family/level train coverage preflight 继续 fail closed。
   moneyflow 是当日流量，禁止跨日填充；`circ_mv` 是 causal stock variable，仅按 C-009-A 处理，二者不得共享填充策略。
10. **C-009-D（用户于 2026-07-27 批准的实施顺序）**：
    1. 先以只读脚本对 frozen universe 全量枚举 `(canonical_ts_code,trade_date,source_dataset)`，生成 identity candidate、未解析项、
       重叠项和 authority evidence；不得只验证 `302132.SZ` 正例，也不得从缺口结果反推 alias。
    2. 在 BUG-877 中扩展 allowed write scope 后，实现 `security_identity` manifest parser/resolver 及 source-specific exact join；
       request、dataset/mapping manifest 和 child 必须绑定相同 identity-manifest hash。
    3. 在 `PostgresStockFactReader` 中先完成 source identity resolution，再对 `circ_mv` 使用 C-009-A causal as-of；禁止修改
       `market.daily_basic`、`market.moneyflow_ts`、PIT universe、SW mapping 或 H5。
    4. 在 stock-fact/aggregate evidence 中显式传播 provider NA、source date/staleness、canonical/source identity 和 typed reason；
       final HMM matrix 仍拒绝任何 NaN/non-finite。
    5. 使用同一 frozen 601-day train source 执行只读 preflight，必须分别证明：591 个 code-identity apparent gap 被解析、466 个
       suspension-day circ-mv exact-calendar gap 被 causal as-of 闭合、502 个 provider absence 仍显式存在、四个 family/level 的逐
       sector row/date/coverage receipt 完整。任一 identity/hash 漂移或 coverage 不足均不得生成 formal request。
    6. 只有上述设计获得用户批准、源码独立审核通过、source PR 另获合入确认且新的 dataset/mapping/source-identity hashes 冻结后，
       才可另行申请恢复 5184 fits；本设计不授权 fit、D5、D6、model/READY、数据库写入、依赖变更或 runtime action。
11. **验证与反例矩阵**：direct tests 必须覆盖 source-specific effective-date 边界、零/多/重叠 alias、same-code/alias 双事实冲突、
    `302132.SZ -> 300114.SZ` moneyflow 正例、其他 dataset 不被误改、连续停牌及 eligible-start 边界、无 causal circ-mv、provider NA
    进入 missing evidence、0.90 count/weight 边界、coverage 通过后的 finite aggregate、coverage 失败后的 typed invalid、manifest/hash
    回读与 frozen preflight 不写数据库。禁止以 Qlib 能保存 NA 证明 GaussianHMM 可接收 NaN，也禁止以单个代码正例、单一 family、
    单一 sector 或 plan-only artifact 代替完整实现验收。

##### C-009 implementation/read-only preflight evidence（2026-07-27）

1. `security_identity` resolver、provider-absence authority、causal `circ_mv` as-of、row/aggregate evidence 与 C-009 CLI
   已在 BUG-877 独立分支实现。生产读采用 `calendar_month_split_fact_stream_v1`：价格/PIT 映射使用 server-side cursor，
   `daily_basic/moneyflow_ts/stk_limit` 使用 date-bounded exact-key maps，causal `circ_mv` 只从 authoritative
   `daily_basic` 状态推进；该拆分只改变查询执行计划，不改变 source identity、coverage、feature 或 semantic authority。
2. 601 日只读 preflight 已完整执行，receipt 为
   `tmp/validation/hmm_risk/bug877_c009_stock_fact_preflight.json`，canonical SHA-256=
   `b45d71d318e6793728dc8d570bd84be7d39f67e9d72937848b346fe4c37077d2`，内部 receipt SHA-256=
   `5b55fe57d14c8fe9c50ae4f468a09496c4255387b62ad8c0e37448ed635a1c58`。source statistics 精确为：
   moneyflow alias=591、provider absence=502、causal stale circ-mv=348、最大 staleness=30 个市场交易日；
   `fit/selection/D5/D6/model/READY/database/runtime` 均未执行。
3. preflight 正确 fail closed 为 `blocked/hmm_risk_model_train_observation_coverage_insufficient`，不是 source 实现成功后伪造
   READY：legacy L1=31/31 合格；legacy L2=130/131 合格，`801881.SI` 仅 96 行；autocycle L1=0/31、
   autocycle L2=0/131 合格。直接原因是 provider-absence NA 继续服从已批准 0.90 count/weight coverage，invalid sector/date
   又与 autocycle 的 120/250-day rolling 及 exact complete cross-section 合同组合；不得通过填 0、前值、行业代理、删除
   `689009.SH`、放宽 coverage、降低 rolling min-periods 或允许 incomplete cross-section 来伪造 120 行。
4. 因此 C-009-A/B/C 的 source implementation 已闭合，C-009-D 的 601 日执行已闭合但验收结果为 blocked；F-011 保持
   blocked。下一步必须先形成独立 train-coverage 根因/合同决策，不得在本 BUG 内自行迁移业务逻辑，也不得恢复 5184 fits。

##### C-010-DIAG-01. train-only feature-domain contributor eligibility 与 mask 候选诊断

1. **批准边界**：用户于 2026-07-27 批准先实现并执行 601 日只读诊断。该诊断只回答“provider-absence 是否应停止贡献特定
   train feature domain”以及“受影响 direct sector 是否存在可审计 feature mask 候选”；不批准正式训练政策、HMM fit、D5、
   D6、model/READY、数据库写入、依赖变更或 runtime action。
2. **证券与场景边界**：证券继续保留在 PIT 股票池、选股、回测/实盘候选和后续 runtime prediction eligibility 中；不得把
   `689009.SH` 或任何其他证券从业务 universe 删除。eligibility 仅依据冻结 train `2022-01-01..2024-06-30` 的 audited
   provider-absence 与同窗口 expected observation opportunity 冻结，并继续复用已批准 `0.90` availability，而非新增门禁。
3. **feature-domain 分离**：价格、成交额、停牌、涨跌停、收益、breadth 与 PIT sector identity 继续使用完整可用证券事实；仅
   moneyflow contributor domain 可排除 train availability 低于 `0.90` 的证券。不得填 0、前值、行业代理、neutral、L1 fallback
   或把 NA 送入 GaussianHMM；排除项、机会数、absence 数、availability、source/hash 必须进入 receipt。
4. **横截面诊断**：正式 exact-complete cross-section 入口保持不变。只读诊断可复用既有 `0.90` coverage 计算可区分
   “一个已审计 provider absence 放大全市场缺失”与真实 price-domain coverage failure；不得把诊断结果静默激活为正式
   cross-section policy。
5. **mask 候选**：每个 `family × direct L1/L2 sector` 先审计完整 feature set。只有与 excluded contributor 精确关联且完整
   feature set 不足 `120` 行、同时所有 mandatory non-moneyflow features 至少 `120` 行时，才可形成删除精确 moneyflow feature
   group 的 deterministic candidate。candidate 必须保存有序 mask、excluded feature、row count、status 与 canonical hash；
   不允许因 candidate 存在推导 family eligibility、selection、semantic mapping 或 READY。
6. **停止条件**：diagnostic 报告无论 candidate 是否完整都只写 evidence 并正常结束；任一 denominator、identity、sector set、
   price-domain coverage 或 mask 证据不完整时 fail closed。只有 601 日结果、正式设计复审和后续用户明确批准后，才可另行设计
   formal policy 与恢复 5184 fits；本次不得选择 seed 或访问 validation/future utility。
7. **首次 601 日执行证据（已被正式审核否定）**：clean producer `385e8309109dddab8e629768c8dc30934a179cd3` 完成
   `hmm_risk_c010_observation_eligibility_diagnostic_v1`。报告 canonical SHA-256 为
   `ded02740714ba55de6aeabf4b71d5f98282dfb4f6fb0dc11fc80e026ccad251f`，内部 body receipt SHA-256 为
   `959ddd0603549c869ed683b7419fbcf3494580f221a9a02cae0d91ab0d3efb70`。train-only eligibility 仅排除
   `689009.SH` 的 moneyflow contribution，精确影响 L1 `801880.SI` 与 L2 `801881.SI`；PIT/selection/runtime prediction
   eligibility 均未改变。该 report 后续被代码审核发现的 schema/denominator/opportunity-receipt 缺陷否定，不得继续作为可批准
   formal policy 的 evidence；其 hash 仅保留为 historical failed-review identity。
8. **首次候选结果（须重跑确认）**：四个 `family × level` 均形成完整 diagnostic candidate，且不需要删除任何 family feature：autocycle L1/L2
   最低完整 train row 分别为 `152/236`，legacy L1/L2 为 `313/422`，全部高于既有 `120`。诊断仍显式保留
   `999` 个 L1 与 `1351` 个 L2 price-domain invalid sector/date evidence；它们没有被填值或吞掉，而是由 diagnostic-only
   `0.90` cross-section evidence 与后续 rolling 共同决定最终行数。
9. **状态边界**：baseline formal coverage 仍为 invalid；`fit/selection/D6/model/READY/database/runtime` 全部为 `false`，且
   `formal_policy_activated=false`。首次结果不能证明 formal policy，也不能在修复后免除 601 日重跑。
10. **正式审核修复合同**：诊断 aggregate 必须使用独立 `FeatureDomainDailyAggregate`，不得向正式 `L1DailyAggregate.__dict__`、
    aggregate manifest/hash 或默认 feature definition 注入字段；只有 diagnostic 入口可声明 coverage-aware contract。诊断内
    `net_mf_ratio/elg_net_mf_ratio/sf_mf_net_ratio_std_5d_neg/sf_small_net_ratio_5d` 必须统一使用
    `moneyflow_contributor_amount`，而正式 C-007-A `/l1_amount` 公式保持不变。expected opportunity 必须保存逐 symbol 精确日期集合 hash，
    provider-absence 日期必须是其子集；eligibility 只能有一个 canonical receipt authority。上述修复通过直接边界测试后，必须以新
    clean producer 重跑 601 日诊断并生成新 hash，旧 `ded02740…251f` 不得复用。
11. **审核修复后 601 日证据**：clean producer `6f447908959161c8c4e87289476a99505d8ff537` 完成重跑，新报告
    `bug886_c010_observation_eligibility_reviewfix.json` canonical SHA-256 为
    `ac218d78935d2adbf3940c0b97cce2d9db4a3e382e00f6beb86e245c7006b3ae`，内部 body receipt 为
    `27f96a86034b1398490079a7813db17ce7ef376f3a1d38fe7e09b06e71c9ec5b`，eligibility receipt 为
    `9f38e61ee2c6b93795a14640adeca4f17ba22e7e2021272fbe23f37238565e3f`。5 个 absence symbol 均保存 exact opportunity
    date-set hash；仅 `689009.SH` availability=`0.1730449251` 被排除 moneyflow contribution。四项 candidate 再次完整且无需删
    feature，最低 train rows 仍为 autocycle L1/L2=`152/236`、legacy L1/L2=`313/422`。baseline formal coverage 仍 invalid；
    `fit/selection/D6/model/READY/database/runtime/formal_policy` 全部未执行。首次向旧路径写入因 collision fail-closed，旧报告未覆盖；
    随后使用独立路径完成，不删除任何历史 artifact。
12. **rebase 后权威证据**：PR #2796 source branch rebase 到最新 main 后，BUG-886 同步 rebase；代码内容不变但 commit identity 被
    改写，因此 `ac218d78…6b3ae` 仅保留为 pre-rebase verified evidence，不作为当前 source ancestry 的最终 receipt。新 clean producer
    `60d9ce16a5abe2ce92c3c1f664165c13ba414211` 以独立路径完成第三次 601 日只读诊断，报告 canonical SHA-256 为
    `2b1f4accfb4e8a3d85ed4ca8ce2f9bf0547552c48938eb85a6166c8a3f007260`，内部 body receipt 为
    `ac17beb27ee9688c3b8808b12e0ceae26fe4789020408091d3576c13f6fa12ae`，eligibility receipt 保持
    `9f38e61ee2c6b93795a14640adeca4f17ba22e7e2021272fbe23f37238565e3f`。5/5 opportunity hashes、排除 symbol、四项
    candidate 与最低行数均和审核修复后报告一致；所有 fit/write/action flags 继续为 `false`。

##### C-010-FORMAL-A. 正式 feature-domain contributor 与逐 feature cross-section 政策（历史 v1 已执行，A5 amendment pending）

本节把已完成的 C-010 只读诊断收敛为正式训练输入合同。其 algorithm/policy version 固定为
`hmm_risk_c010_feature_domain_policy_v1`；用户已于 2026-07-28 明确批准 C-010-A1/A2/A3/A4，当前状态为
`SOURCE_IMPLEMENTED_TARGETED_VALIDATION_PASSED_FORMAL_PREFLIGHT_PENDING`。本节记录已获批准并在当前独立源码 PR 中完成的实现；
它不等于 formal preflight 已通过，也不授权 HMM fit、D5、D6、model/READY、数据库写入或 runtime action。为避免一次批准掩盖不同业务语义，正式合同拆成
四个稳定决策项：C-010-A1=full-universe train-frozen contributor ledger；C-010-A2=price/moneyflow 双层 coverage 与同源
moneyflow denominator；C-010-A3=逐 feature cross-section；C-010-A4=公式/政策 identity、formal preflight 与激活边界。

1. **全量 contributor ledger 与 train-only authority**：对冻结 train `2022-01-01..2024-06-30` 中每个具有至少一个 expected
   opportunity 的 canonical symbol 建立一条 ledger entry，而不是只为出现 provider absence 的 symbol 建 entry。令
   `O_s` 为该 symbol 同时满足 PIT eligible、authoritative price row 存在、SW member 在当日有效且位于冻结 train 窗口的精确交易日集合；
   令 `A_s` 为 provider-absence authority 中同 symbol 的日期集合。必须满足 `A_s ⊆ O_s`，`O_s` 非空且两者均以有序日期集合
   SHA-256 固化。`availability_s=(|O_s|-|A_s|)/|O_s|`，正式 eligibility 使用等价整数比较
   `10*(|O_s|-|A_s|) >= 9*|O_s|`，且仅当该条件成立时
   `moneyflow_contributor_eligible=true`。无 absence 的 symbol 必须显式记录 `A_s=∅/availability=1.0`，不得以“未出现在缺失清单”
   代替 full-universe receipt；零 opportunity、重复日期、越界 absence、identity/hash 不一致均 fail closed。
2. **冻结与应用范围**：eligibility 只能从 train 数据和已审计 provider-absence authority 计算一次，不能读取 validation、future
   utility、D5/D6 或 READY。选定 policy manifest 后，同一 ledger 必须原样用于该 model identity 的 train、validation、causal replay
   与 runtime feature construction，避免 train/serving drift；runtime 不得按新到数据临时重算或改变 contributor eligibility。仅在
   validation/runtime 首次出现、train ledger 没有 entry 的证券继续进入业务 universe 与 price domain，但其 moneyflow contributor
   状态固定为 `train_eligibility_unavailable` 并排除于 moneyflow expected set，留下 symbol/date/policy evidence；禁止默认 eligible、
   临时估算 availability 或因此删除该证券。
3. **业务 universe 不变**：任何 excluded contributor 仍完整保留在 PIT 股票池、价格/收益/成交额/市值/breadth contributor、回测、
   选股、实盘候选、validation 和 runtime prediction eligibility 中。`689009.SH` 只能因正式公式计算结果在 moneyflow contributor
   domain 中被排除，禁止 hard-code symbol、删除证券、缩减 sector、改变 ST/退市语义或把 contributor exclusion 解释为模型/研究方向淘汰。
4. **price domain 第一层 coverage**：每个 `direct sector × trade_date` 的 expected price contributors 仍为当日非停牌且 PIT/SW
   identity 有效的完整证券集合；`prev_circ_mv_cny` 必须按 BUG-892 的 causal history contract 有效。只对 price mandatory fields
   finite 的 row 聚合，并继续同时要求 count coverage 与 `prev_circ_mv_cny` weight coverage 均 `>=0.90`。不满足时仅该
   sector/date 形成 typed invalid price observation；不得因 moneyflow eligibility 缩小 price denominator。price expected set 的每个
   contributor 必须有同一 identity/date receipt 下 finite 且严格正的 `prev_circ_mv_cny`；按 canonical symbol 顺序以 `math.fsum`
   计算的 `price_expected_weight` 必须 finite 且严格大于 0。任一 expected weight 缺失/非有限/非正或 sum 非有限/非正时，状态固定为
   `weight_denominator_invalid` 并使用 price-domain 专属 reason；contributor identity 重复或 row/hash 不一致使用
   `contributor_receipt_mismatch`。两类失败均禁止从 expected set 删除问题 row 后继续计算 coverage。
5. **moneyflow domain 第二层 coverage**：每个 `direct sector × trade_date` 的 expected moneyflow contributors 等于第一层 expected
   contributors 中 `moneyflow_contributor_eligible=true` 的集合。五个且仅有五个 moneyflow mandatory fields 固定为
   `buy_sm_amount_cny`、`sell_sm_amount_cny`、`buy_elg_amount_cny`、`sell_elg_amount_cny`、`net_mf_amount_cny`；`amount_cny` 与
   causal `prev_circ_mv_cny` 是额外的 domain completeness 字段，不得把 medium/large tier 或其他未批准字段加入该集合。
   complete moneyflow contributors 必须满足：五个 moneyflow 字段均 finite（signed amount 允许为负）；`amount_cny` finite 且非负；
   `prev_circ_mv_cny` finite 且严格正。不得用非负约束错误拒绝 net/small/elg signed flow，也不得接受负成交额或非正权重。
   complete rows 在该缩小后的 expected set 上分别满足 count coverage 与
   `prev_circ_mv_cny` weight coverage 均 `>=0.90`；count coverage 使用 `10*complete_count >= 9*expected_count`，weight coverage
   沿用既有 finite float64 `complete_weight/expected_weight >=0.90` 且不增加 epsilon/tolerance。moneyflow coverage 必须复用第 4 项
   已验证的同一 `prev_circ_mv_cny` row identity；expected/complete weight 均按 canonical symbol 顺序以 `math.fsum` 聚合，
   `moneyflow_expected_weight` 必须 finite 且严格大于 0，`complete_weight` 必须 finite、
   非负且不大于 expected weight。任一 weight 缺失/非有限/非正 denominator 时状态固定为 `weight_denominator_invalid`；
   `complete_weight > expected_weight`、identity 重复或 row/hash 不一致使用 `contributor_receipt_mismatch`。不得从 expected set
   再删除 row 或把异常压缩成普通 coverage insufficient。expected set 为空为
   `structurally_unavailable`，coverage 不足为 `coverage_insufficient`；这些状态均使该 sector/date 的 moneyflow features 保持
   NA/invalid，不得填 0、前值、均值、行业代理或 neutral。
6. **moneyflow numerator/denominator 同源**：`net_mf_ratio`、`elg_net_mf_ratio`、`sf_mf_net_ratio_std_5d_neg` 与
   `sf_small_net_ratio_5d` 的 numerator 只汇总 complete moneyflow contributors；denominator 固定为同一 contributor set 的
   `moneyflow_contributor_amount=sum(amount_cny)`。禁止继续以包含 excluded/missing contributor 的全 price-domain `l1_amount`
   作为上述四项 denominator，也禁止 numerator/denominator 使用不同 identity/date/coverage receipt。denominator 必须 finite 且严格
   大于 0，否则该 sector/date 的 moneyflow domain 以 `denominator_invalid` fail closed。
7. **逐 feature cross-section policy**：cross-section validity 不再由“任一 sector 任一 feature 缺失”全局扩散。L1 与 direct L2
   使用同一算子合同但各自只读取本 level 的 direct stock-fact aggregate；禁止从 L1 推导 L2 或在两个 level 间借用 valid set/reference。
   当前 7/20 维合同中
   必须逐项显式处理的 price-domain cross-section features 固定为 `volume_ratio`、`sf_range_vs_market_10d`、
   `sf_vol_vs_market_20d` 与 `sf_excess_breadth_5d`；当前没有 moneyflow-domain cross-section feature，四个 moneyflow feature
   只按第 5/6 项的 sector-local domain 计算。令 level `q` 的 canonical expected sector set 为 `E_q`，L1/L2 分别严格为 31/131；
   每个 feature `f` 按下列精确 pre-cross-section input 建立当日 ordered valid-sector set `V(f,t) ⊆ E_q`，不得用 `l1_return` 或其他
   surrogate 的 completeness 代替该 feature 自身的 finite input：
   - `volume_ratio`：input 为当日 direct-sector `sector_volume`，必须 finite 且非负；reference 为
     `sum_{h in V(volume_ratio,t)} sector_volume(h,t)`；
   - `sf_range_vs_market_10d`：daily input 为 finite 且非负的 `sector_range_ratio`；daily reference 为
     `median_{h in V(range,t)} sector_range_ratio(h,t)`，先形成 daily relative ratio，再按 sector 执行已批准
     `rolling(10,min_periods=5).mean()`；
   - `sf_vol_vs_market_20d`：input 为各 sector 已按 `rolling(20,min_periods=10).std(ddof=1)` 形成的 finite 且非负
     `sector_vol20`；reference 为
     `median_{h in V(vol20,t)} sector_vol20(h,t)`；
   - `sf_excess_breadth_5d`：input 为 finite 且位于闭区间 `[0,1]` 的 sector-local `sf_breadth_5d`；reference 为
     `mean_{h in V(breadth,t)} sf_breadth_5d(h,t)`。
   L1 denominator 固定 31、L2 固定 131，`feature_cross_section_coverage=valid_sector_count/expected_sector_count` 必须 `>=0.90`
   才允许对该 feature/date 计算横截面值，等价最小 valid count 分别为 L1=`28`、L2=`118`。
   不足时仅该 feature/date 的全部 cross-section output 为 typed invalid；达到阈值时只对 `V(f,t)` 计算，并让缺失 sector 保持 NA。
   `volume_ratio` 的 sum、range/volatility 的 median 与 breadth 的 mean reference 必须 finite；前三者还必须严格大于 0。reference
   缺失/非有限/非正时该 feature/date 使用独立 `reference_invalid` 状态，不得改成 coverage insufficient、回退全 31/131、填 epsilon
   或借用另一 feature 的 set。任何输出非有限时使用 `output_non_finite` fail closed。对
   `sf_range_vs_market_10d`，daily coverage/reference mask 必须在 rolling 前形成且在 rolling 后再次应用：即使 prior days 已满足
   `min_periods=5`，当前 `feature/date` coverage 不足或当前 sector 不在 `V(f,t)` 时，最终当前值仍必须为 NA，禁止 rolling resurrect。
   price-domain feature 不得被 moneyflow 缺失影响；moneyflow-domain feature 不得借用 price-domain completeness。每个
   `feature/date/level` 必须保存 expected/valid/missing sector set、ordered hashes、coverage、exact operator/reference value、
   pre/post-rolling mask hash 与 source domain。该 `0.90` 复用既有 coverage authority，不引入 95% 或其他新阈值。
   contributor availability、sector/date count、sector/date weight 与 feature cross-section 四个数值虽都为 `0.90`，manifest 必须分别命名
   `contributor_min_availability`、`domain_min_count_coverage`、`domain_min_weight_coverage` 与
   `feature_cross_section_min_coverage`，不得用一个含糊字段替代四种语义。
8. **feature set 不变、公式 identity 显式升级**：legacy 7 维与 autocycle 20 维 feature 名称、顺序和 rolling min-periods 保持已批准
   合同；C-010-DIAG-01 已证明四个 `family × level` 无需删除 feature，因此正式 policy 的 feature mask 必须与批准 feature list 完全相等。
   但第 6 项的 moneyflow denominator 与第 7 项的 cross-section validity 明确改变四个 moneyflow feature/横截面派生 feature 的计算语义，
   不能伪装成旧 `hmm_risk_l1_sector_factor_formula_v1`。A2/A3 已获批准，正式 formula version 必须升级为
   `hmm_risk_l1_sector_factor_formula_v2_c010` 并逐 feature 保存 old/new formula diff；旧 v1 artifact/diagnostic 保持历史只读，禁止
   原地改写。`moneyflow_domain_excluded_candidate` 只保留历史诊断证据，不得在正式入口自动删 feature；任何未声明 formula/feature
   identity 漂移均 fail closed。A2/A3 已获批准，因此未来 formal implementation 必须使用 v2，并只在第 6 项四个 moneyflow
   features 与第 7 项四个 price cross-section features 上覆盖 v1，其余 C-007-A 公式逐项保持字节级定义不变；当前 main 仍未实现
   v2，formal grid 因实现与 preflight 未完成继续 blocked。
9. **immutable manifest 与 request binding**：正式 policy manifest 至少包含 policy/version、train window、`0.90` authority、full-universe
   contributor ledger 与 receipt、excluded symbol set、逐 sector/date 的 price/moneyflow coverage receipts、逐 feature cross-section
   receipts、feature order/hash，以及 dataset、mapping、security-identity、provider-absence、causal-circ-mv、calendar 和 producer commit
   identities。formal request、两个 fresh process、child、candidate、selection receipt 与 READY manifest 必须绑定相同 policy SHA-256；
   任一缺失或漂移禁止第一个 fit。
10. **601 日 formal preflight**：源码实现后必须从 clean main、冻结 request 和只读 DB 重新生成 601 trading-day evidence；不能把
    C-010-DIAG-01/02 的 diagnostic hash 复制为 formal receipt。preflight 必须证明 full-universe ledger 完整、两层 coverage 与逐 feature
    cross-section receipts 可回读、四个 `legacy_covfix/autocycle_all_core × L1/L2` sector set 分别为 31/31/131/131，且每个 sector
    完整 train rows `>=120`。任一条件失败时 status=`blocked`、formal request candidate 为空、CLI 非零退出，fit/selection/D6/
    model/READY/database/runtime flags 全为 false。
11. **稳定错误分类**：实现必须至少区分 `hmm_risk_c010_expected_opportunity_missing`、
    `hmm_risk_c010_provider_absence_outside_opportunity`、`hmm_risk_c010_contributor_receipt_mismatch`、
    `hmm_risk_c010_price_domain_weight_denominator_invalid`、`hmm_risk_c010_price_domain_coverage_insufficient`、
    `hmm_risk_c010_moneyflow_domain_weight_denominator_invalid`、`hmm_risk_c010_moneyflow_domain_structurally_unavailable`、
    `hmm_risk_c010_moneyflow_domain_coverage_insufficient`、`hmm_risk_c010_feature_cross_section_coverage_insufficient`、
    `hmm_risk_c010_feature_cross_section_reference_invalid`、`hmm_risk_c010_feature_cross_section_output_non_finite`、
    `hmm_risk_c010_feature_cross_section_mask_mismatch`、`hmm_risk_c010_moneyflow_denominator_invalid`、
    `hmm_risk_c010_train_eligibility_unavailable`、
    `hmm_risk_c010_feature_identity_drift` 与 `hmm_risk_c010_policy_identity_mismatch`。不得压缩成 generic missing、静默跳过、
    fallback success 或自动放宽阈值。
12. **验证矩阵**：直接测试必须覆盖 full-universe 无 absence entry、恰好/略低于 `0.90` contributor 边界、absence 非 opportunity 子集、
    price 与 moneyflow domain 隔离、exact five-field moneyflow dependency、moneyflow expected set 为空、两层 count/weight 边界、
    expected weight 缺失/非有限/非正/越界、numerator/denominator 同一 contributor set、amount denominator 为 0/non-finite、
    train 后新证券、L1/L2 逐 feature cross-section 恰好/略低于 `0.90`、四项 exact operator/reference、reference 为 0/non-finite、
    缺失 sector 保持 NA、range rolling 不得 resurrect 当前 invalid date、pre/post mask hash 不一致 fail closed、
    7/20 维 feature set 不变且 formula v1/v2 identity 不混用、validation/future utility 不可见、
    policy/receipt/hash 漂移、两次 fresh-process preflight identity 一致及全部副作用 flags=false。
13. **证据边界与推荐**：当前权威 C-010-DIAG-01 canonical report `2b1f4acc…7260` 证明仅 `689009.SH`
    availability=`0.1730449251`、四个 candidate 均完整且无需删 feature；BUG-892 receipt `7c36f228…fdd1ca` 已证明 PIT-entry causal
    denominator `1073/1073/0`。C-010-DIAG-02 的 pre-fix full-universe 运行因 BUG-892 fail closed，不能冒充 post-fix formal evidence。
    用户已明确批准上述方案；源码已在当前独立 PR 中实现并通过定向验证，但新的 601 日 formal preflight 尚未完成，因此 F-011/F-011-A 继续 blocked，不恢复 5184 fits。

14. **源码实现与审计证据**：当前源码 PR 已实现并验证 full-universe `O_s/A_s` ledger、train 内 ledger 缺项 fail-closed、train 后新证券
    `train_eligibility_unavailable`、price/moneyflow 双层 exact count+weight coverage、同源 moneyflow numerator/denominator、L1/L2
    逐 feature cross-section、rolling post-mask、逐 sector/date 与逐 feature/date canonical receipts，以及 formula/policy 在 formal request、
    双 fresh-process、D4/D5/D6、selected artifact 与 READY manifest 的同一 SHA-256 lineage。READY 同时保存可回读的 full-universe ledger，
    不依赖 diagnostic 文件推断 runtime eligibility。正式入口仍先重算 policy/coverage receipt 并在任何 fit 前比较 frozen identity；旧 C-008/C-009
    diagnostic request loader 保持可读，不被 C-010 formal identity 误阻断。该实现未读取 validation/future utility 用于 train selection，未执行
    HMM fit、D5、D6、model/READY、数据库写入或 runtime action。当前 latest-main rebase 后本模块 required plan=`140 passed`、branch
    coverage=`75.30%`，`l0` blocking=`0`，F2 validator=`PASS`（3 items/8 rows/warnings=0）；下一业务证据只能来自 clean-main 601 日 formal preflight。

13. **PR #2837 正式代码审核补修（2026-07-29）**：policy manifest 不得只保存 receipt 顶层 hash。正式
    `hmm_risk_c010_feature_domain_policy_v1` 必须内嵌可独立回读的 eligibility receipt、逐 L1/L2 sector/date 的
    price/moneyflow domain receipt、逐 L1/L2 feature/date 的 cross-section receipt，以及 L1/L2 feature definition；同时保存各自
    canonical SHA-256。domain receipt identity 必须严格覆盖 frozen calendar 与 canonical L1=`31`、L2=`131` 的完整 Cartesian
    product，每个 identity 恰好一条 accepted 或 typed-invalid receipt；完全无 source group 的 identity 必须显式写入
    `hmm_risk_c010_expected_opportunity_missing`，不得从 receipt set 静默消失。cross-section receipt 必须严格覆盖四个批准 feature 与
    frozen calendar 的完整 Cartesian product，不得接受只有 index/hash 的语义空 entry。preflight、formal request、两个 child、
    D4/D5/D6 与 READY writer 必须复用同一个严格 validator；validator 同时校验 exact field set、ledger entry 完整字段、日期/sector/
    feature 唯一性、status/reason、四个 `0.90` authority、formula/feature order、source identities 与所有 nested canonical hash。
    自洽重算 hash 不能使缺字段、缺 identity、重复 identity、未知状态或部分 receipt set 通过。READY 必须保存完整 validated policy
    manifest；任一残缺必须在首个 fit 或 artifact write 之前 fail closed。这是批准合同的完整性修复，不增加人工审批或运行时门禁。

##### C-010-A5. provider-absence 审计域与 direct-sector contributor opportunity 域分离（用户已批准设计）

本节由 BUG-944 / Issue #3017 登记，用户于 2026-08-02 批准详细设计；当前源码已实现并通过601日只读preflight，状态为
`RESOLVED_USER_APPROVED_SOURCE_MERGED_601_DAY_PREFLIGHT_VERIFIED`。它修复 C-010-A1 中 `A_s ⊆ O_s` 的域定义缺口；
本次源码与preflight仍不授权 HMM fit、D5/D6、model/READY、数据库写入或 runtime action。

1. **根因与冻结事实**：D1-B controlled-refit 使用已批准的 C-010 formal request，在 `fresh_process_1`、首个 HMM fit 前以
   `hmm_risk_c010_expected_opportunity_missing` 拒绝 `002951.SZ`。full-market provider audit 的 exact absence key 为
   `002951.SZ@2023-05-22`；该证券当日有 authoritative price row，且位于冻结 PIT eligible span，但 Tushare
   `index_member_all(ts_code='002951.SZ')` 与本地 `market.sw_index_member` 均只有 `2025-08-11` 起生效的成员事实，冻结 train
   日期没有 L1/L2 申万身份。因此这不是 moneyflow/provider、price、PIT、证券代码 alias 或本地同步缺失，而是该 key 位于
   full-market provider audit domain、却不位于 direct-sector contributor opportunity domain。禁止补造历史申万成员、用当前行业
   回填、删除 provider-absence row 或取消 SW join 来伪造 opportunity。
2. **两个不可混同的权威域**：`P_all` 是 immutable full-market provider-absence manifest 中落入冻结 train window 的全部 exact
   `canonical symbol × trade_date` keys；`O_sector` 是同一 train window 内同时满足 PIT eligible、authoritative price row 存在、
   且当日具备唯一有效 direct L1 与 direct L2 申万 identity 的 contributor opportunity keys。正式 ledger 的 availability 只对
   `O_sector` 定义；`P_all` 不得被反向当作“每个 key 都必须属于 O_sector”的证明。两域的 source identity、window、ordered key set
   与 canonical SHA-256 必须独立保存。
3. **确定性分区**：对每个 `P_all` key 逐谓词审计 `pit_eligible`、`price_authority_present`、`sw_l1_identity_valid`、
   `sw_l2_identity_valid`，形成互斥且完备的 `P_in=P_all ∩ O_sector` 与 `P_out=P_all-O_sector`。必须满足
   `P_in ∩ P_out=∅`、`P_in ∪ P_out=P_all`、逐 key 唯一、entry count 与 ordered key hash 闭合；任一谓词缺证据、重复、hash 漂移、
   未知状态或分区不完备均以 `hmm_risk_c010_provider_absence_domain_partition_invalid` fail closed。
   四个 predicate 不建立新数据 authority，必须逐项复用并绑定当前 frozen request 的既有权威：
   - `pit_eligible`：使用 request 已冻结的 `universe_key/universe_rule_version`、已通过 source validation 的 ready/non-dirty PIT state，
     以及覆盖该日的唯一 `stock_universe_pit_spans` row；保存 state/span identity 与 hash。不得改用其他 pool、current/latest 或只看 span
     而绕过 state/rule identity；
   - `price_authority_present`：使用 C-009/C-010 stock-fact reader 同一 canonical security/date 对应的
     `market.kline_daily_raw` exact row identity。该 predicate 只表示 authoritative row 是否存在；价格字段 numeric completeness 继续由
     price-domain contract 独立判断。重复且非全字段等价、identity/date 不一致为 `invalid`，不得归类为普通 unavailable；
   - `sw_l1_identity_valid/sw_l2_identity_valid`：必须复用 §4.3.1 A 与 mapping manifest 的同一 member-closure resolver：active
     `sw_index_member` interval 经共享 L2 的唯一 canonical L1 owner 与无冲突名称闭包规范化后形成唯一 canonical `(L1,L2)` pair。无 active source row或
     member catalog 不完整为 `unavailable`；重叠/冲突/alias 一对多/名称冲突/非等价多行为空间 identity `invalid` 并使整个 partition fail closed，禁止把
     invalid 降级为合法 `P_out`；
   - 所有 predicate 在 join 前先应用 C-009-B 已批准的 source-specific effective-dated security identity resolver，并在 entry 中保存
     canonical/source/stable identity 与 resolver authority hash；禁止用 provider raw code、证券名称或当前代码直接关联历史事实。
4. **域外证据而非静默忽略**：每个 `P_out` entry 至少保存 canonical/source security identity、trade date、provider row hash、
   PIT predicate receipt、price predicate receipt、L1/L2 SW predicate receipt、有序 `failed_predicates`、primary reason code、policy
   version 与 entry hash。一个 key 可同时具有多个失败 predicate，全部必须保存；primary reason 只用于稳定分类，固定优先级为
   `pit_ineligible > price_unavailable > sw_l1_unavailable > sw_l2_unavailable`，不得因选择 primary reason 丢弃其余失败事实。
   `002951.SZ@2023-05-22` 的确定性 primary reason 为 `hmm_risk_c010_sw_identity_unavailable_for_opportunity`。未来若 predicate 表明
   PIT 不 eligible 或 price authority 不存在，分别使用 `hmm_risk_c010_pit_ineligible_for_opportunity`、
   `hmm_risk_c010_price_unavailable_for_opportunity`；后者不得覆盖 price-domain 自身的 missing/coverage failure，必须引用对应 typed
   price evidence。predicate 集不完整、primary reason 与优先级不一致或无法分类时不得使用 generic out-of-domain 成功状态。
5. **availability 与 ledger 公式**：对每个至少具有一个 `O_sector` key 的 canonical symbol，令 `O_s` 为其全部 sector opportunity
   dates，`A_s` 仅为 `P_in` 中同 symbol 的 dates；继续要求 `A_s ⊆ O_s`，并原样执行已批准的
   `10*(|O_s|-|A_s|) >= 9*|O_s|`。只有 `P_out`、没有任何 `O_sector` key 的 symbol 不生成 contributor eligibility entry，也不进入
   availability numerator/denominator；其完整 `P_out` evidence 仍必须进入 policy manifest。禁止把 `P_out` 计为 available、填 0、
   neutral、行业代理，或借此把同 symbol 在冻结 train 内较晚日期具有有效 SW identity 的 `O_sector` keys 排除。若整个 train 均无
   `O_sector` entry、只在 validation/runtime 首次获得 SW identity，继续沿用 A1 已批准的 `train_eligibility_unavailable`，不临时重算
   availability，也不删除证券。
6. **业务不变量**：C-010-A5 只界定 direct L1/L2 moneyflow contributor opportunity。证券仍保留在 PIT 股票池、价格/收益事实、
   回测、选股、实盘候选及 runtime prediction eligibility；不得因缺少历史 SW identity 删除证券，也不得为其生成虚构 sector
   observation。C-009 source identity、C-010-A2/A3 formula/coverage、7/20 feature order、两个 family、train/validation 窗口、
   hard semantic authority、D3-D6 与两-family READY 合取均保持不变。
7. **policy 与 artifact lineage**：domain partition contract 候选版本固定为
   `hmm_risk_c010_provider_absence_domain_partition_v1`；正式 policy schema 候选升级为
   `hmm_risk_c010_feature_domain_policy_v2`，eligibility/expected-opportunity schema 分别升级为
   `hmm_risk_c010_train_observation_eligibility_v2` 与 `hmm_risk_c010_expected_opportunity_dates_v2`。v1 request/manifest 不得仅因
   nested hash 自洽而被解释为具有 A5 evidence。
   - partition receipt 顶层 required fields 固定为：schema/contract/policy version、train start/end、provider-absence manifest identity、
     security-resolver identity、PIT state/span authority identity、price source identity、SW mapping/classify identity、`P_all/P_in/P_out`
     entry counts、三个 ordered key hashes、entry list、`partition_complete`、`diagnostic_only/formal_policy_activated` 与 receipt hash；
   - 每个 partition entry required fields 固定为：canonical/source/stable security identity、trade date、provider row hash、四个 predicate
     的 `available/unavailable/invalid` status、各 predicate authority receipt/hash、有序 `failed_predicates`、`in_domain/out_of_domain`
     partition、primary reason、entry hash。unknown field、unknown enum、字段缺失、hash 自洽但 authority 不闭合均拒绝；
   - cardinality 固定为每个 `P_all` exact key 恰好一条 entry，并满足顶层 counts、entry set、三个 key hash、disjoint/union invariants；
     writer 与 readback validator 必须调用同一 strict validation authority；
   - eligibility v2 在 v1 每-symbol `O_s/A_s` evidence 基础上新增 partition receipt/hash 与 `P_in` date-set hash；expected-opportunity v2
     保存逐 symbol `O_s` dates/hash及其 frozen predicate-authority identities；policy v2 内嵌并哈希上述完整 receipts，不接受 count-only、
     hash-only 或外部 diagnostic pointer；
   - v1 artifact、historical formal report 与 READY reader 继续按原 schema 只读回放，不迁移、不覆盖、不伪装成 v2；A5 合入后新的
     formal preflight/request/child/selected/READY writer 只允许 v2。任何以 v1 request 恢复新 fit 的尝试必须在首个 fit 前以
     `hmm_risk_c010_policy_identity_mismatch` fail closed。
   formal policy manifest、request candidate、两个 fresh-process child、D4/D5/D6 receipt、selected artifact 与任何 future READY
   manifest 必须保存同一 `P_all/P_in/P_out` partition receipt 及 canonical SHA-256；D1-B 只能在新 formal preflight 重算并验证该
   identity 后恢复。
8. **验证合同**：direct fix-point 必须覆盖：全部 absence 均 in-domain；合法 SW-domain-out key；PIT-domain-out；price-domain-out 且
   price failure 未被掩盖；同 symbol 同时具有 in/out keys；predicate 缺失/冲突；重复/漏分区/hash 漂移；out-only symbol 不进入
   denominator；同一 frozen train 内后续具备 SW identity 的 dates 正常进入 ledger；v1 历史只读/v2 新写、writer/readback 同权威；
   证券/PIT/runtime prediction flags 均不变。随后执行冻结 601 日 read-only formal preflight：必须完整分区冻结 manifest 的全部
   `P_all` keys并证明分区/hash/cardinality闭合，且结果必须包含已知 `002951.SZ@2023-05-22` SW-domain-out evidence；其他
   `P_out` 数量只能由完整审计结果确定，禁止在执行前硬编码为1或因出现额外合法域外 key 失败。在其通过前不恢复 D1-B fit。
9. **状态边界**：本节不新增 coverage threshold、人工逐股确认、运行时 acknowledge、研究方向淘汰或发布门禁。当前 D1-B
   report 保持 `diagnostic_failed`、fits=`0`、selection/model/READY/database/runtime actions=`false`。精确合同已在BUG-944任务分支实现并
   生成新的 formal policy identity；代码PR合入和后续fit仍按既有边界分别报告，不得把源码/preflight完成扩张为fit、selection、数据库或runtime授权。

##### BUG-892. PIT entry 与 causal `circ_mv` denominator 的时间域冲突

1. **根因事实**：C-010-DIAG-02 的 fail-closed evidence 包含 1,073 个精确 `symbol/date`、1,072 只证券和 455 个交易日；
   全部发生在 `trade_date == stock_universe_pit_spans.eligible_start`，其中 `ipo_365d=955`、`st_restore=118`。
   这些证券全部具有当日价格、当日正数 `circ_mv` 和 `<t` 的正数 authoritative `daily_basic.circ_mv`；955 项可使用 exact
   previous-market-day row，118 项使用 C-009-A 已批准的最新 causal row，staleness 为 1 个市场交易日。因此该缺口不是 DB/H5、
   Tushare provider、证券代码 identity 或 moneyflow 事实缺失，而是 reader 把当前 PIT span 的 `eligible_start` 错当成
   `circ_mv` 历史事实可见性的下界。
2. **批准的修复边界**：用户于 2026-07-28 授权登记 BUG-892 并按上述根因执行修复。PIT span 继续唯一决定证券是否进入当日
   eligible universe；`circ_mv` lookup 则使用版本 `hmm_risk_causal_circ_mv_source_window_v1`，在同一 immutable source/security
   identity 下先选择满足
   `request.source_start <= source_date <= prev_market_trade_date(t) < t` 的最新 authoritative row；该最新 row 的 `circ_mv`
   必须 finite 且严格为正，否则显式 fail closed，不得跳过它后回退到更早 row。
   `source_date` 可以早于当前 PIT span 的 `eligible_start`，但禁止早于 immutable request source window；不得使用当日/未来 row、
   0、均值、行业代理、neutral、当前值 fallback 或删除当日新进入证券。
3. **语义隔离**：该修复只把 PIT membership 时间域与 causal float-market-value 权重事实时间域分离。C-007-A 的
   `previous_close`/return 仍不得跨 listing/PIT entry，ST/退市股票的 PIT 选择语义、hard semantic authority、两个 family、
   D3/D4/D5/D6 及 model/READY 条件均不改变。首个 eligible day 的 return 缺失继续由既有 count/weight coverage 显式衡量，
   不得把 denominator 修复解释为该股票的 feature row 已完整。
4. **审计证据**：每个 stock-fact row 持久化 `circ_mv_source_date`、trading-day staleness、`circ_mv_history_start`、
   `circ_mv_pit_eligible_start`、`circ_mv_crossed_pit_entry_boundary` 和 lookback contract version；dataset manifest 保存 crossing
   count、ordered key hash、source-window identity 与 contract version。crossing=true 但日期顺序、staleness 或 contract identity
   不完整时以 `hmm_risk_stock_fact_circ_mv_pit_boundary_evidence_invalid` fail closed，不压缩为 generic missing。
5. **验证边界**：直接测试必须覆盖 PIT-entry 前一日正数事实可用、source window 之前的事实仍被拒绝、alias/no-alias SQL 使用同一
   history boundary、crossing receipt/hash 可回读。随后仅执行 601 日 no-fit denominator/preflight：1,073 项必须全部得到 causal
   weight，P/F/O、invalid sector/date 与 row-count evidence 重新计算；在该 evidence 完整前仍禁止 5184 fits、selection、D6、
   model/READY。该重跑不是新增人工门禁，也不授权 DDL、DML、依赖安装或 runtime action。
6. **正式审核修正**：direct SQL 与 missing-price direct 路径不得以 `circ_mv` 数值是否有效决定是否保留 source date、staleness 或
   PIT-boundary crossing。时间因果性与数值有效性必须独立：最新 causal row 即使为 NULL、非数值、非有限或非正，也必须保留其
   source identity，并以稳定 `circ_mv_fact_status`/`circ_mv_reason_code` 使 denominator fail closed；不得把“最新 row 数值无效”
   伪装成“source row 不存在”。crossing authority 必须由日期关系推导，调用方布尔值与推导结果任一方向不一致均以
   `hmm_risk_stock_fact_circ_mv_pit_boundary_evidence_invalid` 拒绝。crossing receipt/hash 同时绑定 fact status 与 reason code。
7. **C-009 窗口修正**：`source_start/source_end` 是本次 stock-fact observation 输出窗口；`circ_mv_history_start` 是从原始冻结 request
   继承的 causal denominator 历史下界，两者不得再次压缩成同一个字段。C-009 将 observation window 收窄到 601 日 train window 时，
   必须保持原 request 的 `circ_mv_history_start=2020-07-30`，否则 2022 年首个交易日所需的 2021-12-31 causal row 会再次被错误排除。
   L1/L2 preflight source statistics 必须同时比较 crossing total、available、invalid 和 ordered key hash；只有 total=available=1,073 且
   invalid=0 才证明本 BUG denominator 根因闭合。该历史窗口只服务 `<t` 权重，不扩展 observation、PIT membership 或 return history。

##### BUG-870. 正式 train coverage preflight 与 child failure receipt

1. **失败事实**：clean producer `1ad5ff6209d723c41537d51b1d2d750a95a2e371` 的 identity preflight 通过后，
   fresh_process_1 在 `_direct_train_series_for_family` 阶段返回
   `801010.SI train-only observation coverage is insufficient: 10`；该错误发生在首个 HMM fit 前。父进程原始失败只返回
   `stderr_sha256=fae31c52178516b376a69ac89e1b155fc5a3c673132348b12779cc29e9d0e719`，没有 durable typed receipt。
2. **四项 coverage authority**：preflight 必须在写 formal request 前，对 `legacy_covfix/autocycle_all_core × L1/L2`
   四个 family/level 组合分别按批准 feature 顺序、train `2022-01-01..2024-06-30`、direct sector set 与逐行 `dropna`
   语义审计。每个 direct sector 继续使用 C-007-A 已批准的最低 `120` 行；这不是新增阈值或人工审批。
3. **完整 evidence**：每个组合保存 expected/actual sector count、canonical sector set、逐 sector row count与date hash、min/max row、
   insufficient sector code全集、typed reason和canonical receipt。审计不得读取validation、future utility、semantic labelability，
   不得执行fit、selection或model/READY write。
4. **blocked 输出**：任一组合不满足时，preflight status=`blocked`，持久化完整 coverage receipt，
   `request_candidate/request_candidate_sha256=null`，CLI返回非零；禁止生成可供formal child使用的request。不得early stop于首个sector，
   不得删特征、填值、放宽31/31或131/131、降低120行、回退previous-available PIT或改用旧`fca206…`。
5. **request 与 child 绑定**：仅 `candidate_ready` request 才能写入
   `train_coverage_contract_version=hmm_risk_b3_train_coverage_preflight_set_v1` 与完整 coverage receipt SHA-256；每个 formal child
   必须在首个 fit 前重新计算四项 coverage，并同时验证 status 与 receipt hash。磁盘上即使残留旧 request candidate，也不得因文件存在、
   manifest identity 相同或历史 preflight 曾通过而复用；缺少 identity、coverage 已失效或 receipt 不一致均 fail-closed。
6. **child failure durability**：若通过preflight后的future child仍异常退出，parent必须原子写
   `hmm_risk_b3_child_failure_receipt_v1`，至少保存process identity、returncode、typed error、stdout/stderr bytes+hash及所有副作用false
   标志；parent错误必须引用该receipt path，不能只返回不可诊断的stderr hash。
7. **BUG-870 执行当时状态（历史证据）**：formal grid=`BLOCKED_TRAIN_INPUT_COVERAGE`，实际HMM fits=`0`；fresh_process_2、D5、D6、model/READY均未执行。
   BUG-870 source commit `0660262ac47dcdd685682dcc2ab46732dc71b926f` 的真实 preflight receipt canonical SHA-256 为
   `10d503b49af4e394a960596a592b58e014e399a22e5fe4907e466a0286084b9d`：legacy L1 31/31有效；legacy L2有1个sector不足，
   最低96行；autocycle L1 31个sector全部不足，范围2至10行；autocycle L2 131个sector全部不足，范围0至5行。
   request candidate未生成，fit/selection/model/READY/database/runtime flags全为false。本BUG只修复fail-closed位置和evidence
   completeness，不擅自选择数据修复或模型语义变更方向。

D4-02-A 于 2026-07-25 获用户明确批准，正式 covariance acceptance 合同如下：

0. threshold/algorithm version 固定为 `hmm_risk_c008_b3_d4_02_a_v1`；比较符、tolerance、denominator 或 reason
   mapping 的任何变化必须使用新版本；
1. **raw validity**：raw diagonal covariance shape 必须严格为 `(3, feature_count)`，每个 cell 必须 finite 且严格大于 0；
   shape/non-finite/non-positive 分别保留结构化 evidence，并以 `hmm_risk_model_covariance_invalid` fail closed。
2. **train-only posterior authority for covariance audit**：仅使用最终 fitted model 对完整 train sequence 计算的
   forward-backward smoothed posterior `γ(t,k)` 来形成 covariance audit；它不得参与 hard semantic mapping、D5 selection
   或 validation evidence。令 `M_k=Σ_t γ(t,k)`、train rows 为 `N`；`M_k` 必须 finite 且严格大于 0。
3. **M-step expected covariance**：令 `μ_k` 为 fitted mean，
   `W_kj = Σ_t γ(t,k)*(X_tj-μ_kj)^2/M_k`，则
   `C_expected[k,j]=(ν*R_sj+M_k*W_kj)/(ν+M_k)`；所有 numerator、denominator、expected value 必须 finite，
   denominator 必须严格大于 0。
4. **dynamic reference bounds**：
   `L_kj=ν*R_sj/(ν+M_k)`，`U_kj=(ν+N)*R_sj/(ν+M_k)`；固定 `τ_bound=0.005`，比较为闭区间：
   `(1-τ_bound)*L_kj <= C_raw[k,j] <= (1+τ_bound)*U_kj`。
5. **zero anomaly budget**：应用上述显式 tolerance 后，total/per-state/per-feature anomaly budget 全部为 0；任一超界以
   `hmm_risk_model_covariance_bounds_failed` 与 `hmm_risk_model_covariance_anomaly_budget_exceeded` 的最具体适用 reason
   fail closed。0.5% 是已批准的 dimensionless optimizer-consistency tolerance，不是允许静默异常或事后按 extrema 拟合的 budget。
6. **M-step consistency**：逐 cell 定义
   `E_kj=abs(C_raw[k,j]-C_expected[k,j])/max(abs(C_expected[k,j]), float64_tiny)`，其中
   `float64_tiny=np.finfo(np.float64).tiny`；固定 `τ_mstep=0.02`，要求
   `max(E_kj) <= τ_mstep`，否则以 `hmm_risk_model_covariance_acceptance_failed` fail closed。
7. **禁止修正后通过**：正式 posterior 与后续 semantic evidence 只能使用满足本合同的 raw fitted covariance；禁止
   post-fit clip/projection、禁止用 `C_expected` 覆盖 raw parameter、禁止按 family/sector 临时扩大 tolerance。
8. **receipt**：保存 `R_sj/ν/M_k/W_kj/C_expected/C_raw/L/U/E`、比较结果、逐 state/feature count、compact mask/hash、
   threshold version、producer/dependency/numeric-environment identity；non-finite 值不得被 JSON `null` 后继续判定成功。
9. **状态闭包**：`covariance_status` 只允许 `accepted/failed/insufficient_evidence`；required train/reference/posterior receipt
   不存在或不可验证时为 `insufficient_evidence/covariance_valid=false`，证据存在但第 1..7 项任一失败时为 `failed/false`，
   仅当全部通过时为 `accepted/true`。不得以 final covariance 参数存在、clip 后有限或其他状态通过推导
   `covariance_valid=true`。

D4-01-MAP-A 与 D4-02-A 共同形成正式停止条件，但仍分别保存 MAP convergence 与 covariance acceptance；它们都不推导
train occupancy、selection、semantic evidence、family completeness 或 READY。DIAG-04 本身仍为
`VERIFIED_DIAGNOSTIC_ONLY_NO_SELECTION_NO_ARTIFACT`，不得反写为已执行正式验收。

D4-03-B 于 2026-07-25 获用户明确批准；以下条目保留为历史合同证据，并于 2026-08-09 被
`C-008-B3-D4-03-PERSISTENT-A` 明确取代。除 run-path 判定外，其 common gate 原样保留：

0. threshold/algorithm version 固定为 `hmm_risk_c008_b3_d4_03_b_v1`；本合同对 direct L1 与未来受控 direct L2、两个
   family 和全部预声明 restart 使用同一阈值。禁止按 level/family/sector/seed 或 observed extrema 临时改写；
1. **train-only causal posterior authority**：仅使用最终 fitted raw parameters，从 fitted `startprob_` 开始，对 immutable
   train observation sequence 计算 causal filtered posterior `p(z_t|x_<=t)`；不得读取 validation、future utility、semantic
   label 或 D5 selection 结果。hard state 为每行 posterior `argmax`，hidden-state index 不具有 semantic 含义；
2. **posterior numeric validity**：posterior shape 必须严格为 `(N_train,3)` 且 `N_train>0`；所有 cell 必须 finite 且非负，
   每行 sum 的最大绝对误差必须 `<=1e-12`。每行 top1-top2 margin 必须严格 `>1e-12`；小于或恰好等于边界均为 tie failure，
   不得按 state index、stable sort 或默认值继续；
3. **hard count 与 occupancy**：对每个 hidden state `s`，令 `C_s=count(argmax_t=s)`，要求
   `C_s >= max(5,ceil(0.01*N_train))`，且 `C_s/N_train >=0.01`。两个条件分别记录 comparison evidence；即使 count 条件因
   ceil 已隐含 ratio，也不得删除 normalized occupancy readback 或把两个 reason 压缩为一个；
4. **calendar-month coverage**：每 state 被 hard assigned 的 observation date 必须覆盖至少 `3` 个不同 `YYYY-MM` calendar
   month；比较为 `month_count>=3`。month 只由 immutable ordered train dates 计算，不从 row index、validation 或当前日历回填；
5. **run 与 transition coverage**：在 immutable train observation row 顺序上，每 state 的 contiguous run count 必须
   `>=3`；incoming 和 outgoing state-change transition count 必须分别 `>=2`，仅统计相邻 observation rows 之间
   `state_a!=state_b` 的变化，自转移不计入。每 state 的 maximum single-run share
   `max_run_length/C_s` 必须 `<=0.8`；denominator 非正或 run 不可计算时 fail closed；
6. **date-gap 边界**：run 表示模型实际输入 observation sequence 的连续 row，不冒充自然日或无缺口交易日连续。ordered
   train date list/hash、canonical calendar coverage、missing/invalid date count/hash 必须进入 input/occupancy receipt；缺口仍由
   C-007-A input coverage 独立验收，不能因 row-run 通过而被隐藏，也不得把缺失日虚构成 state transition；
7. **diagnostic-only soft evidence**：posterior mass、normalized mass、ESS、entropy 和 posterior-weighted utility 可记录为
   diagnostic，但不参与 D4-03 pass/fail，不改变 hard authority，也不得用于补足 hard count/month/run/transition；
8. **状态闭包**：`train_occupancy_status` 只允许 `accepted/failed/insufficient_evidence`。必要 train dates/posterior/hard
   sequence receipt 缺失或 hash 不可验证时为 `insufficient_evidence/train_occupancy_valid=false`；证据存在但第 1..6 项任一
   失败时为 `failed/false`；全部通过时才为 `accepted/true`。D4-03 accepted 不推导 D5/D6、family completeness 或 READY；
9. **reason 与确定性优先级**：保留全部适用 failure/blocking，不得 first-failure 丢失可安全遍历证据。primary reason 顺序为
   evidence missing、date sequence invalid、posterior invalid、normalization、tie、state count、occupancy、month、run、transition、run concentration；
   新增/保留的稳定 code 见第 12 节，family receipt 按 level/family/seed/sector 聚合 count、code 与 evidence hash；
10. **receipt**：逐 entry 保存 contract version/hash、level/family/seed/sector、`N_train`、ordered date/hash、posterior/hard
    assignment identity、逐 state count/ratio/months/runs/incoming/outgoing/max-run-share、所有 threshold/comparison result、
    failure/blocking arrays、primary reason、status/valid mapping、producer/dependency/numeric environment，并固定
    `validation_accessed=false/future_utility_accessed=false`。

##### D4-03-PERSISTENT-A. recurrent/persistent 互斥双路径（active）

用户于 2026-08-09 明确批准 `C-008-B3-D4-03-PERSISTENT-A`，active algorithm/version 固定为
`hmm_risk_c008_b3_d4_03_persistent_a_v1`：

1. **common gate 不变**：继续逐 state 要求 posterior finite/nonnegative、row-sum max error `<=1e-12`、top1-top2 margin
   严格 `>1e-12`、hard count `>=max(5,ceil(0.01*N_train))`、normalized occupancy `>=0.01`、calendar month
   coverage `>=3`、incoming/outgoing state-change transitions 各 `>=2`。任一 common gate 失败时不得进入路径补偿。
2. **recurrent path**：`contiguous_run_count>=3` 且 `maximum_single_run_share<=0.8`。边界 `0.8` 归 recurrent；
   该路径不要求 persistent 的 10%/6-month/30-count 条件。
3. **persistent path**：仅在 `maximum_single_run_share>0.8` 时适用，并同时要求
   `hard_count>=max(30,ceil(0.10*N_train))`、normalized occupancy `>=0.10`、calendar month coverage `>=6`、
   contiguous run count `>=2`、incoming/outgoing state-change transitions 各 `>=2`。
4. **互斥与失败语义**：两个路径按 `<=0.8` 与 `>0.8` 严格互斥；不得同时通过、不得按更宽路径择优。
   common gate 通过但对应路径不通过时使用 `hmm_risk_model_train_regime_path_unsatisfied`，并保留逐条件 comparison。
   persistent 不是 singleton/单 run fallback；count、ratio、month、run、transition 任一不足均 fail closed。
5. **authority 与 receipt**：hard argmax 继续是唯一 train structure authority；soft posterior mass/ESS 只诊断。
   每 state 保存 `evidence_path=recurrent|persistent|none`、两路径 eligibility/result、所有 common/path thresholds、
   comparisons、reason arrays 与 hash。D5/READY readback 必须从这些 normalized evidence 重算 common gate、互斥路径、
   transition/count/month/run 一致性和最终 status/reason，禁止只信任持久化的 `train_occupancy_valid`。validation/future utility
   保持不可见；D4-03 通过不推导 D5/D6/READY。

形成该决策的只读证据为 131-entry seed46 诊断 canonical
`44d07c8548da0607d33264cc18bdac693d716c3801f19b7d991b8f98c59e487d`：392 个 state 走 recurrent，
仅 `801141.SI/state0` 以 count 71、occupancy 15.01%、6 months、2 runs、transitions 2/2、max-run-share 98.59%
走 persistent；131/131 entries 在 proposed contract 下通过。该 evidence 没有执行 selection/D6/model/READY，不能反写正式结果。

D4-03-B 的只读 sensitivity 不是正式 acceptance：DIAG-02 下 legacy 238/248 records 通过但 0/8 seed 达到 31/31，失败集中于
`801780.SI` 和 seeds 42/43 的 `801130.SI`；autocycle 248/248、8/8。DIAG-04 下两个 family 均为 248/248、8/8；2% 候选也
全部通过，而 5% 候选使 legacy 仅 236/248 且 0/8 完整 seed。选择 1% anti-singleton 合同的语义是阻止 singleton/单月/单 run
被当作完整训练结构，同时不在 D4 阶段用未经批准的 semantic significance 淘汰 rare regime。历史 artifact 继续标记
`formal_acceptance_thresholds_applied=false`；未来实现和受控 L2 重训必须从 immutable receipt 重新执行
D4-03-PERSISTENT-A，不得沿用历史 D4-03-B 判定。

##### D5. train-only family/level selection：D5-01-B/D5-02-B 已批准

D5-01-B 于 2026-07-25 获用户明确批准，正式版本固定为 `hmm_risk_c008_b3_d5_01_b_v1`。每个 family 分别选择一个
L1 level-global restart 与一个 L2 level-global restart，最终两个 family 共形成 4 个 selected level identities。两个 level
允许选择不同 seed，但每个 level 内全部 31/131 sector 必须共享同一 seed；这不是 per-sector stitching。禁止把 L1 score
推导为 L2 seed、把 L2 score 推导为 L1 seed，或在两个 family 之间选择/淘汰研究方向。

0. **预声明 schedule 完整性**：严格运行 D3-01-A seeds `42..49` 的全部 entry，L1 每个 family/seed 必须 31/31，L2
   必须 131/131；任一预声明 fit、receipt 或 fresh-process repeat 缺失时 schedule incomplete。禁止 early stop、遇到首个
   eligible candidate 即停止、失败后扩 grid 或按 observed score 追加 seed；
1. **level candidate eligibility**：同一 family/level/restart 的全部 entry 必须通过 D3 initialization/fit、D4-01 likelihood、
   D4-02 covariance 与 D4-03 train occupancy，且 D5-02-B 两个 fresh process 的 model/receipt hashes bitwise一致。
   `likelihood_status=accepted_with_warning` 仅在 `likelihood_valid=true` 时保持eligible，warning及完整delta evidence不得删除；
   任一entry failed/insufficient/pending或hash mismatch使整个level restart ineligible，禁止局部sector拼接；
2. **严格train-only输入**：selection输入只允许final train likelihood、training row count、family feature count、canonical sector
   set/order、D3/D4 statuses/reasons/hashes与schedule index。必须固定
   `validation_accessed=false`、`future_utility_accessed=false`、`semantic_labelability_accessed=false`、
   `d6_status_accessed=false`；任何字段为true、缺失或不可验证时selection fail closed；
3. **逐sector score**：对restart `r`、sector `i`，定义
   `q_r_i=L_final_r_i/(N_train_i*d_family)`，其中`L_final`是D4-01 monitor history的实际最后一个finite value，`N_train_i>0`，
   `d_family`严格等于批准family feature count。禁止使用history maximum、terminal warning前的likelihood、validation likelihood、
   rounded display value或另一level/family的normalizer；任一score non-finite即该candidate ineligible；
4. **canonical aggregation**：只对canonical sector code升序的完整score vector计算tuple
   `(minimum,median,mean)`。L1 vector长度严格31，L2严格131；median取排序后精确中间项，不插值；mean固定为
   `math.fsum(scores)/sector_count`，禁止依赖无序map、`np.mean`实现差异、跨level合并162项或按sector数量给L2隐式更高权重；
5. **tolerance-aware lexicographic maximize**：依次最大化minimum、median、mean。每一维先在当前candidate pool取精确
   `best=max(value)`，再保留满足
   `best-value <= 1e-12+1e-12*max(abs(best),abs(value))`的candidate；使用full float64，不预先round。三维过滤后仍有多个
   candidate时，选择预声明schedule index最小者；不得按seed数值、文件顺序、validation表现或D6结果另行tie-break；
6. **selection后不可变**：全部预声明restart及两个fresh-process receipt完成后才执行一次selection。selected identity冻结后
   禁止refit、参数projection、score重算、换seed或用另一个repeat的参数替换；未选candidate及其D3/D4 failure/warning/score
   evidence全部保留；
7. **D5-02复现闭包**：D5-02-B继续要求同一固定数值环境、单线程、两个fresh process的candidate payload/model/receipt
   bitwise canonical hash一致；D5-01 selection inputs、逐维pool过滤过程、selected result与selection receipt hash也必须在两次
   process间bitwise一致。numeric allclose只允许diagnosis，不可替代hash equality；该证据不外推跨host/BLAS/依赖升级；
8. **status与family闭包**：`level_selection_status`只允许`accepted/failed/insufficient_evidence`。schedule/score/repeat receipt
   缺失或hash不可验证为`insufficient_evidence/level_selection_valid=false`；完整证据存在但无eligible candidate、score非法或
   repeat mismatch为`failed/false`；仅selected identity与receipt全部闭合时为`accepted/true`。同一family的L1/L2两个level
   都accepted后`family_selection_status`才可accepted；D5 accepted不推导D6、family model-set completeness或READY。保留
   全部适用blocking/failure，primary reason依次为selection evidence missing、restart schedule incomplete、level incomplete、
   repeat mismatch、score non-finite、selection unavailable、selection contract unsatisfied；不得以first-failure丢失可安全遍历证据；
9. **D6严格后置且禁止reselection**：D5selected identity冻结后才执行D6-01-B。任一selected entry的D6 assignment/evidence
   failed或insufficient时，对应level/family保持blocked；禁止返回D5选择validation更好的seed、重新拟合、扩大schedule、切换family
   或回退historical model；
10. **immutable receipt**：逐family/level保存contract version/hash、canonical sector set/hash、完整schedule、每个restart的
    eligibility与全部entry D3/D4 status/reason/hash、逐sector`L_final/N/d/q`、orderedscore vector/hash、minimum/median/mean、
    每维exact best/tolerance/survivor pool、schedule tie-break、selected seed/model/parameter hash、未选原因、两个fresh-process
    equality、四个access flags、failure/blocking arrays、primary reason、status/valid mapping、producer/dependency/numeric environment；
11. **historical sensitivity不是selection**：DIAG-04的`family_candidate_eligibility_evaluated=false`、`selection_performed=false`、
    `d5_01_selection_score_approved=false`保持不变。mean-only sensitivity为legacy seed44、autocycle seed47；本合同
    min/median/mean lex sensitivity为legacy seed42、autocycle seed46。这些值只证明公式具有区分性，不写selected identity，
    不读取D6，且没有L2受控重训score；未来L1/L2必须在正式D3/D4 receipt上重新执行D5-01-B。

##### D6. hard semantic validation：D6-01-B 已批准

D6-01-B 于 2026-07-25 获用户明确批准，正式版本固定为 `hmm_risk_c008_b3_d6_01_b_v1`。本合同对 direct L1 与未来
受控 direct L2、两个 family 的 selected level-global entry 使用相同阈值；不得按 level/family/sector/seed、D5 排名或
observed extrema 临时改写。DIAG-04 只提供阈值敏感性，`d6_exact_contract_approved=false` 与
`formal_acceptance_thresholds_applied=false` 保持历史事实，不能反写为正式 D6 acceptance。

0. **执行时序与 selection 隔离**：只有 D5-01-B 按 train-only 合同选定 restart 后，才对该 selected restart 执行 D6。
   validation observation、future utility、hard labelability、D6 pass/fail 或 READY 状态不得成为 D5 输入。D6 失败后该
   level/family 保持 blocked，禁止返回 D5 换 seed、重新拟合、扩大 schedule、切换 family 或拼接 per-sector seed；
1. **冻结窗口与 causal prior**：validation 严格使用 `2024-07-01..2025-03-31` 的 immutable ordered 182 日 calendar
   ledger；首日从 selected fitted `startprob_` 开始 causal filtering，不继承 train 最后一日 posterior。ordered dates、calendar、
   source cutoff、逐日 observation/utility availability、row count 与 content hash 必须与 frozen InputManifest 一致。完整 ledger
   不得缩短、删日或压缩；缺失数据按 D6-NA-A 的 typed availability、transition-only 与 evidence-mask 合同处理；ledger identity
   缺失或漂移仍为 `insufficient_evidence`；
2. **future utility authority**：逐 calendar day 固定
   `y_t=0.35*excess_return_5d+0.35*excess_return_10d+0.30*excess_return_20d`。utility available 日的三个 component 与 `y_t` 必须 finite，
   calendar、单位和 benchmark identity 沿用 C-007-A 已批准语义，不选择新的 benchmark；source cutoff/common watermark 固定
   `2025-04-30`。ordered component vectors/hash、combined utility vector/hash 和 formula version
   全部进入 receipt；D6-NA-A 允许 component unavailable，但禁止部分公式、插补或默认值，该日不进入 semantic evidence。
   future utility 仅用于 D5 完成后的 offline semantic mapping，不进入 observation、fit、posterior 或 selection；
3. **posterior 与 hard assignment**：selected raw fitted parameters 在完整 calendar ledger 上生成 causal posterior，shape 必须严格为
   `(182,3)`；所有 cell finite 且非负，每行 sum 的最大绝对误差 `<=1e-12`。对 `E` 中的 semantic evidence row，top1-top2 margin
   必须严格 `>1e-12`，小于或等于边界均 fail closed；authoritative semantic hard state 为逐 `E` row `argmax`，不得以 state index、
   stable-sort、soft mass 或默认值打破 tie。`T\E` 的 posterior/argmax 只作 causal diagnostic，不进入 semantic acceptance；
4. **anti-singleton evidence**：令 `E={t | observation_available_t AND utility_available_t}`、`N_evidence=|E|`；对每个 hidden
   state `s`，令 `C_s=count(argmax_t=s, t in E)`，必须同时满足
   `C_s>=max(5,ceil(0.02*N_evidence))` 与 `C_s/N_evidence>=0.02`；并继续满足既有 validation 最小源数据行数
   `N_evidence>=30`。该 30 行约束是现有 source contract，不是新增 missing-ratio gate。
   每 state calendar-month coverage `>=2`、contiguous run count `>=2`、incoming/outgoing state-change transition count
   分别 `>=2`、maximum single-run share `<=0.9`。run 只在 calendar 相邻且两日均属于 `E` 时连续；任何非 evidence 日都打断
   run。transition 只在 calendar 相邻且两日均属于 `E` 时计数；自转移不计入 incoming/outgoing，禁止跨缺失日压缩时间或虚构
   transition；
5. **hard utility 与 mapping**：每 state 的 hard utility count 必须等于 `C_s`，mean 与 ddof=1 sample variance 必须 finite。
   按 mean 从低到高映射 `fading/neutral/trending`，hidden-state index 不具有 semantic 含义。对相邻 ordered utilities
   `U_a<U_b`，要求
   `U_b-U_a > max(1e-12,32*eps64*max(1,abs(U_a),abs(U_b)))`，其中 `eps64=np.finfo(np.float64).eps`；恰等于边界也失败。
   standard error、95% separation、posterior mass/ESS/entropy 和 posterior-weighted utility 只作 diagnostic，不参与 pass/fail；
6. **状态闭包**：`semantic_assignment_status` 与 `semantic_evidence_status` 分别只允许
   `accepted/failed/insufficient_evidence`。`semantic_assignment_status` 只由完整 calendar ledger、availability masks、182-row posterior
   与 `E` 上的 hard-assignment identity/numeric contract 决定；utility 数值和 D6 coverage/utility thresholds 不得进入 assignment
   status。receipt 缺失或 hash 不可验证为 `insufficient_evidence/semantic_assignment_valid=false`，证据存在但第 1/3 项失败为
   `failed/false`，全部通过才为 `accepted/true`。availability=false 本身不是 assignment failure，只决定 `E`；mask/ledger receipt
   invalid 才是 blocker/failure。`semantic_evidence_status` 以 assignment accepted 为前置，只由 `E` coverage 与 future-utility
   identity/evidence 决定：receipt 缺失或 hash 不可验证为
   `insufficient_evidence/semantic_evidence_valid=false`，第 2/4/5 项任一失败为 `failed/false`，全部通过才为 `accepted/true`。
   utility 缺失不得把有效 hard assignment 伪写为 failed，但两者未全部 accepted 前禁止写 semantic mapping。D6 accepted 不推导
   model entry、level/family completeness 或 READY；
7. **reason 与优先级**：保留全部适用 failure/blocking，不能 first-failure 丢失可安全遍历证据。primary reason 依次为
   calendar ledger/availability receipt invalid、evidence missing、date sequence invalid、posterior invalid、normalization、posterior tie、hard state missing、state count、
   occupancy、month、run、transition、run concentration、utility non-finite、utility variance non-finite、utility tie/gap。
   使用第 12 节最具体 stable reason code，family/level receipt 聚合全部 code、count 与 evidence hash；
8. **immutable receipt**：逐 entry 保存 contract version/hash、level/family/selected seed/sector、selected model parameter hash、
   完整 validation calendar ledger、observation/utility availability masks 与 canonical hashes、逐日 transition/emission/evidence mode、
   posterior/hard assignment identity、future utility component/combined identity、逐 state
   count/ratio/month/run/incoming/outgoing/max-run-share/utility mean/variance、相邻 gap/tolerance、diagnostic SE、semantic mapping、
   comparison results、failure/blocking arrays、primary reason、status/valid mapping、producer/dependency/numeric environment；
9. **历史敏感性不是选择**：DIAG-04 上，最低存在性方案会把两个 family 的 496/496 records 全部视为可继续，不能阻止
   singleton；D6-01-B sensitivity 为 legacy `244/248` records、4/8 complete seeds，autocycle `242/248`、2/8 complete seeds；
   加 95% significance 后 legacy 为 `0/248`、autocycle 为 `5/248` 且两个 family 都无完整 seed。上述 seed 集合不得写入
   selection receipt 或用于 D5；D6-01-B 选择 anti-singleton 与 numeric strict ordering，不新增 significance research gate。
   DIAG-04 没有受控重训 L2 的 D6 evidence；未来 L2 必须在 D5 selected identity 冻结后逐 131 entry 正式执行本合同，不能
    复制 L1 sensitivity 或既有 L2 final parameters。

##### D6-NA-A. immutable calendar、transition-only 与 evidence mask（已批准，待实现）

`C-008-B3-D6-NA-A` 于 2026-08-10 获用户批准，正式版本固定为
`hmm_risk_c008_b3_d6_na_a_v1`。本 amendment 只修订 D6-01-B 对冻结 validation 日历中 observation/utility
缺失的表达和 causal 计算；D5-01-B、D6 hard argmax authority、semantic mapping、既有 count/month/run/transition/utility gate、
两个 family 完整性与 READY 合取均不改变。

1. **完整 calendar authority**：`T=(t_1,...,t_182)` 必须与 frozen InputManifest 的交易日、顺序、cutoff 和 hash bitwise/canonical
   一致。任何 entry 都不得删除缺失日、只保存可用行或把不相邻日期压缩成连续序列。`calendar_position` 是 run/transition 的唯一
   时间邻接权威；
2. **typed availability**：`observation_available_t=true` 当且仅当该 selected entry 的 family-level full feature contract（legacy 7D；
   autocycle 20D，包括 D1 fixed projection 前的 inactive coordinate）全部存在、finite，且 observation source/status receipt 可验证；
   任一 full-contract feature unavailable 时整条 emission observation unavailable，禁止按 effective 19D 重算 availability、partial-dimension
   emission、均值/前值/零值插补。`utility_available_t=true` 当且仅当 5D/10D/20D future excess return、benchmark identity 和 combined
   utility 全部存在、finite且receipt可验证；禁止只用部分 horizon 重算权重或补默认 utility；
3. **causal transition-only**：令 `prior_1=startprob_`，`prior_t=posterior_(t-1) @ transmat_`。若
   `observation_available_t=true`，使用 BUG-1008 的稳定 log-space emission update 得到 posterior；若为 false，严格令
   `posterior_t=prior_t`。transition-only 日不调用 emission、不生成 synthetic feature vector；首日 unavailable 时 posterior 等于
   fitted `startprob_`。完整 182×3 posterior 仍必须 finite、nonnegative 且 row-sum error `<=1e-12`；
4. **semantic evidence mask**：仅 `E={t | observation_available_t AND utility_available_t}` 进入 hard assignment、state utility、
   month/run/transition 与 D6 acceptance。observation available 但 utility unavailable 的 posterior 仍更新并影响下一日 causal prior，
   但该日不得进入 semantic utility evidence。observation unavailable 日即使 utility available也不得进入 `E`；transition-only posterior
   只能作为 causal diagnostic，不能补 hard state evidence；
5. **existing gates only**：D6-01-B 的 count/ratio/month/run/transition/run-concentration/utility/gap 与既有 `N_evidence>=30`
   source contract在 `E` 上原样执行。不得新增 minimum availability ratio、maximum missing-day ratio、人工确认、按 sector 特批或
   provider 白名单。`N_evidence<30`（包括0）先以 `hmm_risk_semantic_validation_evidence_rows_insufficient` fail closed，禁止
   在零分母上计算 ratio。证据不足时使用现有 deterministic gate fail closed，不能因“少量缺失”自动 pass；
6. **calendar-aware structure**：run 和 transition 仅在 `calendar_position` 相邻且两日都属于 `E` 时连续/计数；任何不属于 `E` 的
   calendar day 均打断 run，禁止跨 gap 构造 state-change transition。month coverage 使用 evidence day 的真实 calendar month；
7. **receipt/readback authority**：逐日 ledger 至少保存 date、position、observation_available、utility_available、mode
   (`emission_update/transition_only`)、observation/utility typed unavailable reasons、source identities/hashes 和 evidence inclusion。
   parent/entry receipt 保存两个 masks、ledger、posterior、evidence dates、hard assignment、utility 与 comparison 的 canonical hashes；
   writer 和 durable readback 必须由同一 validator 重算，mask/ledger/hash/row count 任一漂移均 fail closed；
8. **禁止推导**：D6-NA-A 不批准 B2、soft semantic authority、neutral/index fallback、日期插值、feature/utility imputation、seed
   reselection、refit、per-sector stitching、单 family delivery 或 READY。它也不证明 6 个当前阻断 entry 将通过；只能让完整冻结日历上
   的真实 available evidence 进入既有 D6-01-B 验收。

9. **D6 calendar carrier 与 manifest v2**：正式输入载体固定为 `hmm_risk_d6_validation_calendar_series_v1`，替代现有 D6 路径把
   validation observation、utility 与 dates 存为同长度 finite dense arrays 的语义；train arrays/receipts保持不变。carrier 必须包含：
   `calendar_dates[182]`、`calendar_positions=[0..181]`、full `feature_names`、`observation_available_mask[182]`、
   `observation_available_positions`、与 positions 对齐的 finite `observation_values_f64[|O|,d_full]`、逐 component
   `component_available_mask/positions/values_f64`、`utility_available_mask/positions/combined_values_f64`、逐日 missing feature/component
   names 与 stable source reason/receipt hashes。numeric compact payload 不允许 `NaN/Infinity/null` placeholder；canonical JSON 使用
   `allow_nan=false`，每组 values 必须与其 positions 等长且 positions 严格递增、唯一并与 mask 完全等价。InputManifest 升级为
   `hmm_risk_d6_frozen_input_manifest_v2`，保存 carrier schema/version、calendar/full-feature/positions/masks/value payload/source receipts
   的独立 canonical hashes及 aggregate hash。`L1TrainingSeries.validate()`（L1/L2共用carrier）继续验证train finite/120-row合同，
   validation部分改为验证本carrier结构与hash，不再要求dense validation/utility arrays同长finite。v1只保留历史读档，不得作为
   新D6-NA-A execution的active input；
10. **source constructor 不得 dropna 压缩**：`build_l1_training_series()` 及 direct L2 同源构造必须先按 benchmark calendar 建满182行，
   再分别计算 observation/component availability；不得对 observation+utility 交集调用 `.dropna()` 后把剩余行当 validation calendar。
   `N_evidence>=30` 只在 D6 evidence evaluator 执行，source constructor 除 schema/hash/domain invalid 外不得因单 entry 的 NA 数量提前
   删除日期、sector或返回成功空集；
11. **preprocess / D1 projection / filter 顺序**：`O={t|observation_available_t}` 由 raw full family feature payload判定。只抽取 `O`
   的 compact full-dimensional finite matrix执行已冻结 preprocess；autocycle mixed-dimension entry必须继续验证 raw inactive coordinate
   exact-zero、full-20 preprocess replay与既有projection receipt，然后才投影到active 19D。对 `T\O` 不调用 preprocess/projection/emission。
   calendar filter按position逐日运行：每个position先取得prior，`O`日使用对应compact processed row执行log-space emission，非`O`日
   transition-only。禁止把compact matrix直接传给旧dense filter后再按日期扩回182行；
12. **三个集合与两个状态的唯一语义**：`T`是182日ledger，`O`是observation available集合，`U`是utility available集合，
   `E=O∩U`是唯一semantic evidence集合。T上仅在top1-top2 margin严格`>1e-12`的位置保存
   `diagnostic_hard_assignment_positions/values`，margin不满足的位置进入`diagnostic_tie_positions`，禁止即使在diagnostic中用state index
   静默打破tie；`T\E`只作diagnostic。authoritative `semantic_hard_assignment_E`、top1 margin、state utility与全部D6 structure gates只使用`E`。assignment status验证
   T/availability/posterior和E上的hard numeric identity；evidence status验证`N_evidence`、state structure与utility。U缺失不会生成
   assignment failure code；它只从E排除该position并可能使evidence gates失败；
13. **D6 composite version 与 selected artifact readback**：新 execution 的 semantic receipt 顶层
   `contract_version=hmm_risk_c008_b3_d6_01_b_na_a_v1`，并同时保存
   `base_contract_version=hmm_risk_c008_b3_d6_01_b_v1` 与
   `availability_contract_version=hmm_risk_c008_b3_d6_na_a_v1`。普通 selected-level schema升级为
   `hmm_risk_b3_selected_level_artifact_v2_d6_na`，mixed-dimension schema升级为
   `hmm_risk_b3_level_model_set_v3_projection_d6_na`。共同parser/READY validator必须重算三个version、calendar carrier/manifest、
   assignment/evidence receipt与selected-level aggregate hash；active D6-NA-A writer不得接受旧selected schema或仅有D6-01-B v1的
   semantic receipt。旧schema只允许历史只读，不得grandfather为新READY；
14. **零-refit replay lineage**：replay envelope固定为 `hmm_risk_b3_d6_zero_refit_replay_v1`，至少绑定原P6 parent report canonical
   hash、两个fresh child receipt hashes、D5 selection receipt hash、family/level/seed43、131个selected model payload hashes及aggregate、
   train dataset/mapping/calendar/L2-stock-fact/C-010 policy/projection identities、semantic source四个manifest hashes、新D6 manifest v2
   aggregate、training producer commit与replay producer commit。执行结果必须写`fit_performed=false`、`fit_count=0`、
   `selection_reexecuted=false`、`selected_seed_unchanged=true`、`model_parameter_hashes_unchanged=true`；任一 lineage 不闭合在首次
   semantic evaluation前fail closed，禁止重建model、重跑D5或用当前路径/文件名猜测历史authority；
15. **daily reason 与 aggregate status 映射**：`hmm_risk_semantic_validation_observation_unavailable`、
   `hmm_risk_semantic_validation_utility_unavailable`只进入逐日`availability_events`与计数，不进入failure/blocking/warning arrays，
   不能单独使entry failed。manifest/mask/positions/value/source hash不一致使用
   `hmm_risk_semantic_validation_availability_receipt_mismatch`并使assignment failed；缺失不可验证的ledger/manifest使用
   `hmm_risk_semantic_validation_calendar_ledger_invalid`并使assignment insufficient；`N_evidence<30`使用
   `hmm_risk_semantic_validation_evidence_rows_insufficient`并使evidence failed。其后仍按第7项既有priority聚合，所有适用code保留，
   daily availability event不得冒充primary failure。

##### P2-2 / C-008-B3-TRAIN-STABILITY-DIAG-01：train-only 跨阶段稳定性诊断（已完成）

本决策项状态固定为 `VERIFIED_DIAGNOSTIC_COMPLETE_NO_COMPLETE_SEED_TRANSITION_DWELL_DESIGN_REQUIRED`。它只回答现有 D5 为什么会选择一个 full-train
D4 合格、但在 D6 validation 发生 hard-state 结构坍缩的 level-global seed；它不批准新的 D5 gate、transition/dwell prior、
模型参数、threshold、refit、selection 或 READY。

执行权威为producer `7d57d57e36d09fbf8a7f80fc92ef7ed508dad190`和repo-external compact report
`F:/Dev/AIstock_artifacts/hmm_risk/train_stability_diag01_20260812_7d57d57e/train_stability_diag01.json`。按仓库
canonical JSON规则重算的对象SHA-256为`9c449e040ca28fb99138f358c6d8bd400284a061a2167e3dfdc744c0b8c9c5b1`，内部receipt
SHA-256为`0c3d1de236888a6ff2d3f3c112c0fa93a8bbece99ad764a2c6cf50805a3073d9`；byte SHA-256
`67cc147366ed6bbf6fdcde65d7a1027d7283b3cd4b803cd4ffcde76795521083`不替代canonical identity。执行完成
1048/1048 model profiles和131/131 source comparisons，两个互斥窗口均各182 rows，8个seed的双窗口stable sector数依次为
`108/108/97/103/109/105/104/106`，完整seed数为0。231个失败window中early/late分别为160/71；reason occurrence为
month 95、occupancy 82、run concentration 193、run coverage 146、state count 105、transition coverage 178。selected seed43
的11个D6 blocker中，6项为train structure instability、5项为validation-only collapse。诊断明确保持
`fit_performed=false/refit_count=0/selection_performed=false/d6_executed=false/formal_d5_gate_applied=false/model_write=false/ready_write=false/database_write=false/runtime_action=false`。

由于8个seed均不完整，`D5-STABILITY-ELIGIBILITY-A`不能单独解除F-011 blocker；直接启用只会令D5 candidate set为空。
该结果满足第6项“无完整seed”分支，只支持进入`TRANSITION-DWELL-B`或feature-contract设计，不能自动选择具体机制。

1. **P2-1 证据边界**：权威输入为 BUG-1029 zero-refit report
   `p6_d6_zero_refit_bug1029_e30aabbc.json`，canonical SHA-256
   `dcf4c69ec7ba817d8d19f8cca27f6a855f25b2e7d147a5b754549d431d8c26a1`。该报告证明
   `autocycle_all_core:L2/seed=43` 的 131/131 assignment accepted、120/131 evidence accepted；11 个失败的
   calendar/observation/utility/evidence 均为 182/182、availability event 均为 0、posterior margin 均严格高于 `1e-12`。
   对这 11 项按 receipt 中 hard sequence 独立重算 count/occupancy/month/run/incoming/outgoing/max-run-share，0 个字段差异；
   同一模型 entry 的 full-train D4-03 receipt 均 accepted。故当前没有程序或数据/NA BUG 可登记，失败归类为真实模型跨阶段
   hard-state 结构能力不足；该结论不等于已选择修复机制。
2. **冻结身份与可回放性边界**：诊断复用现有 P6 冻结权威，不创建或复制训练输入。必须绑定
   training authority receipt `012f5f93b0d47a8a6e084486fcb47869c7f9b489a7e038fdf764e8c6a3d7d650`、
   两个 fresh-process receipt hashes
   `8488d2e4c83fc016304ed29b5d06a1d37d0b02aea2df37e6418a4f88f5e5c40a` /
   `672e3aed63cc3e7e0cf1d938af5174391dfa54c9932ac70be085005d70424fcc`、D5 receipt
   `8ec3967bb775329bcd277c440a8cfc11f1b15888777e677c4612820d34085cbc`、ordered selected-model aggregate
   `f226650b4a85f5722bdae96b4e8dc09d0a07c8e9dce3983685a1687f38c7bb27`，以及 dataset/mapping/calendar/direct-L2/policy
   identities `75bd5d22…ca8c6` / `acb38f30…82ab9` / `af4a60cd…65b3` / `6a0aa51b…4144` /
   `7ca5ef41…595d3`。family、level、canonical 131-sector set、seeds 42..49、feature/preprocess/projection、模型参数与数值环境
   任一不一致均在第一个 posterior 前 fail closed；禁止猜测 latest artifact。现有 P6 fresh-process/checkpoint 只持久化模型、D4 receipt、
   `observation_manifest_hash`、`train_input_manifest_sha256` 与 `projected_matrix_sha256`，没有持久化 train ordered dates 或 matrix；
   因此本诊断不得声称纯 artifact replay，也不得从模型参数反推输入。
3. **精确 profile 范围与只读重建**：读取 `autocycle_all_core:L2` 的既有 `8 seeds × 131 sectors = 1048` 个冻结 model profile。
   train input 按原 formal request 的 source/security/provider/PIT/calendar/formula/policy identities，通过现有 direct-L2 只读 constructor
   在内存中逐 sector 重建一次，不写数据库或 input artifact。重建后必须逐 sector 先验证 ordered dates/content、
   `observation_manifest_hash`、`train_input_manifest_sha256`、preprocess replay、`projected_matrix_sha256`/shape 与 frozen entry 完全一致；
   任一 identity/hash 不一致时整个诊断返回 `insufficient_evidence/source_drift`、0 profile evaluated，禁止用当前重建值继续、补写历史
   input 或请求数据库修复。每个 seed 复用同一已闭合 sector matrix，不重复重建 8 次。不调用 HMM fit、KMeans、D5、D6、
   validation constructor、future utility 或 semantic mapping。诊断代码不得按当前 11-sector
   failure list缩减计算；该列表只允许在全量 train-only 计算完成后生成方便阅读的子集摘要，不能成为选择输入。
4. **两个互斥 train-only 窗口**：每个 sector 的 frozen train ordered complete observations 记为
   `(x_0,...,x_{N-1})`；现有 131 entries 均有 `N>=420`。固定取最接近 train cutoff 的 364 个 observation：
   `W_early=[N-364,N-182)`、`W_late=[N-182,N)`，各 182 行且互不重叠。窗口按原 observation date 保持真实交易日位置；
   缺失的市场交易日不压缩为相邻，run/transition 必须由 frozen calendar position 判断。每个窗口首行均从该 frozen model 的
   fitted `startprob_` 开始 causal filtering，不继承前一窗口 posterior；随后只使用该窗口已冻结 observation 与 transmat/emission。
   不得以 validation 182 日、future utility 或 D6 status校准窗口。
5. **只读结构指标**：对每个 window 保存 posterior finite/nonnegative、row-sum max error、top1-top2 minimum margin、hard state
   count/occupancy/calendar-month/run/incoming/outgoing/max-run-share及其 canonical hash。为形成可解释的 sensitivity，只以现有
   D6-01-B 的结构子集做诊断比较：每 state `count>=max(5,ceil(0.02*182))`、occupancy `>=0.02`、month/run `>=2`、
   incoming/outgoing `>=2`、max-run-share `<=0.9`、row-sum `<=1e-12`、margin `>1e-12`。不计算 hard utility、mapping 或
   validation acceptance；这些比较结果标记为 `diagnostic_only=true/formal_d5_gate_applied=false`，不得反写 D4/D5/D6 receipt。
6. **结果分类而非自动决策**：逐 profile 只允许 `train_window_structurally_observed` 或带全部 typed reason 的
   `train_window_structurally_unobserved`。逐 selected D6 blocker再分类为：两个 train window 都 observed 但 validation failed =
   `validation_only_structure_collapse`；任一 train window unobserved = `train_structure_instability_observed`。family/level 汇总记录
   每个 seed 是否 131/131 在两个窗口均 observed，但不得选择 seed：存在至少一个完整 seed只支持提交
   `D5-STABILITY-ELIGIBILITY-A`设计；不存在完整 seed只支持提交`TRANSITION-DWELL-B`或feature-contract设计。两种后续合同都必须
   给出精确公式、false accept/reject、fit成本与用户批准，诊断不得自动启用其中任何一种。
7. **最小输出**：允许的唯一新产物是 repo-external compact report
   `hmm_risk_c008_b3_train_stability_diag01_v1`。它只保存第 2 项 identities、逐 sector 重建 hash 与 frozen hash 的 comparison、
   1048 profile 的两窗口日期边界/hash和结构指标、
   per-seed aggregate、11 blocker子集摘要、完整 reason counts与 canonical report SHA；禁止嵌入 observation/posterior/hard arrays、
   model parameters、training matrices或复制现有大 JSON。writer/readback必须重算canonical identity；collision、duplicate key、
   非有限数值或 profile 缺失 fail closed。报告必须显式写
   `fit_performed=false`、`selection_performed=false`、`d6_executed=false`、`formal_acceptance_thresholds_applied=false`、
   `model_write=false`、`ready_write=false`、`database_write=false`、`runtime_action=false`。
8. **实施与验证边界**：若用户批准本诊断，源码只允许在现有 HMM offline preparation/acceptance 边界增加一个专用入口和直接测试；
   changed files 必须经 ownership 路由到 `hmm.risk`，只运行 changed-file lint/compile、窗口切分/causal reset/calendar-gap/metric/hash/
   no-fit-no-selection 正反例、本模块 required plan、scope check 与 `git diff --check`。不运行 2096/5184 fits，不运行其他模块测试，
   不建立通用诊断平台、调度器或新的历史输入物化。
9. **DESIGN-COMPLIANCE-001 预审**：禁止简化交付=`PASS`，因为该诊断不能代替11项D6闭合、两family READY或产品纵切；
   禁止静默错误=`PASS`，因为input未持久化事实与source drift均显式fail closed，所有profile/reason完整聚合；禁止业务逻辑迁移=`PASS`，
   因为hard semantic authority、D3-D6、seeds 42..49、两family完整性均不变，validation/future utility不可见；禁止未经确认的门禁和审批
   =`PASS`，因为D6结构阈值只作diagnostic sensitivity，`formal_d5_gate_applied=false`，D5 stability与transition/dwell均留在Decision
   Index待用户决定。预审结论仅为`PASS_PROPOSED_DIAGNOSTIC_DESIGN_NOT_APPROVED_NOT_IMPLEMENTED`，不得推导源码或执行授权。

##### P2-2A / C-008-B3-TRANSITION-DWELL-B：transition MAP regularization受控实验（已执行，无完整候选）

用户于2026-08-12明确确认本节完整精确合同；源码、BUG-1068 receipt闭合修复和首次受控实验现均已完成，状态为
`VERIFIED_DIAGNOSTIC_COMPLETE_NO_COMPLETE_CANDIDATE_NO_SELECTION_NO_READY`，直接映射F-011-B/C。它不是新增平台、通用训练框架、
HSMM迁移或新的产品目标；唯一目的，是验证一个最小训练内生机制能否让同一level-global restart在既有D4/D5/D6合同下形成
更稳定的三状态结构。本次证据未满足第7项唯一可推进条件，因而不激活正式model identity、selection、D6或READY。

1. **证据与排除项**：TRAIN-STABILITY-DIAG-01已证明8个seed都没有131/131双窗口稳定候选。失败同时包含低count/occupancy、
   run/transition不足与run concentration过高；失败window的经验self-transition四分位数为约`0.717/0.830/0.909`，且存在
   absorbing hard sequence。故禁止把“提高self-transition floor”当作唯一修复，也禁止降低D4-03/D6阈值、删除state/sector、
   扩大seed、per-sector stitching、validation-driven selection、soft semantic authority、posterior后处理或fit后transmat projection。
   现有D3 pre-fit `alpha=0.1/self-floor=0.3`继续只作初始化；本候选不把它改写为最终约束。
   候选对照固定为：A=`D5-STABILITY-ELIGIBILITY-A`，因0个完整seed而not selected；B=本train-only transition MAP prior，
   是唯一推荐且仍待批准；C=显式duration/HSMM，需要改变模型族、posterior/parser/hash与全部四level训练合同，当前证据不足且不进入本阶段；
   D=放宽D4/D6或删除state/sector，改变已批准验收语义，禁止采用。B的推荐不表示数值已被证据“证明最佳”。
2. **最小机制与精确公式**：候选algorithm version为`hmm_risk_c008_b3_transition_dwell_b_v1`，其适用scope固定为
   `autocycle_all_core:L2`的全部131个sector；同一level内不得按sector启停，其他family/level继续使用现有D3-03-A
   `transmat_prior=1.0`。这属于待批准的level-local model identity，不是per-sector stitching；D7最终manifest必须显式保存四个level
   各自的algorithm profile/hash。对该scope仅把
   `GaussianHMM.transmat_prior`从标量`1.0`替换为3×3、逐sector且由train-only KMeans hard sequence预注册的Dirichlet MAP prior；
   covariance/means/startprob prior及D3-03-A其余参数不变。令KMeans transition count为`C_ij`，row count
   `M_i=sum_j C_ij`，`q_ij=(C_ij+0.1)/(M_i+0.3)`；令
   `p_ii=clip(q_ii,0.50,0.90)`，对`j!=i`定义
   `p_ij=(1-p_ii)*(C_ij+0.1)/sum_(k!=i)(C_ik+0.1)`。固定concentration `tau=8.0`，传入hmmlearn的
   `transmat_prior[i,j]=1.0+tau*p_ij`。每行`p`必须finite、strict-positive且sum与1的绝对误差`<=1e-12`；shape、
   denominator、clip前后值或prior任一无效时以`hmm_risk_model_transition_prior_invalid` fail closed。这里的0.50/0.90只限定
   prior中心，对应几何dwell中心约2至10个observation rows；它不是fitted transmat或hard run的acceptance bound。
   `tau=8.0`表示每个source-state row总计8个transition pseudo-observations：对常见state相对完整train transition count影响有限，
   对稀疏state提供可见但不支配数据的收缩。选择该值的目的只是构造一次可证伪的最小treatment，不是从当前样本拟合出的最优超参；
   因此运行时不得搜索或自动改写tau，实验失败后任何新值都必须形成新version与用户决策。
3. **MAP objective与联合停止**：active D4-01-MAP-A的convergence authority必须把transition prior项纳入同一参数状态：
   `J_t^B = J_t^A + sum_ij((transmat_prior[i,j]-1)*log(a_ij))`，其中raw fitted`a_ij`必须finite且strict-positive、每行和误差
   `<=1e-12`。不得遗漏normalization-independent之外的参数相关项，不得使用hard path likelihood替代E-step raw observed likelihood。
   原MAP numeric envelope、300-iteration上限以及同参数状态D4-02-A联合停止保持不变；transition term非有限或raw transmat无效分别
   以`hmm_risk_model_transition_map_objective_non_finite`、`hmm_risk_model_transition_matrix_invalid` fail closed。
   D5的`L_final`仍是joint-stop raw observed log likelihood，不改用MAP objective。
4. **dwell的定义与权限**：`expected_dwell_i=1/(1-a_ii)`只作train-only解释性receipt；`a_ii>=1`、denominator非正或
   非有限必须作为transition matrix invalid失败。正式结构验收仍完全由已批准D4-03-PERSISTENT-A判定，validation semantic evidence
   仍完全由D6-01-B/D6-NA-A判定；不得新增expected-dwell pass/fail阈值，也不得用prior中心、soft mass或expected dwell补足hard evidence。
5. **固定范围、control与成本**：若获批，第一次受控实验只运行当前真实blocker level
   `autocycle_all_core:L2`，使用同一冻结dataset/mapping/policy、canonical 131-sector set、seeds42..49和固定单线程数值环境；
   每个fresh process为`131*8=1048` fits，两个fresh processes合计`2096` fits。treatment使用本候选prior；control不重跑，
   只引用现有P6 frozen model/receipt/hash与TRAIN-STABILITY-DIAG-01，禁止把不同source authority当control。只要冻结authority发生drift，
   整个实验在0 fits处`insufficient_evidence/source_drift`。不运行legacy或L1，因为此次没有共享feature/emission/covariance合同变化；
   treatment不能外推为其他level/family accepted。
   level-local scope使此次算法变化不会隐式失效其他三个level的既有model identity；若未来要把同一prior扩展到其他level/family，
   必须基于对应blocker另行更新设计、重新批准并只重训实际受影响scope，不能从本实验推导全局启用。
6. **执行次序与no-selection边界**：两个fresh processes必须完整完成全部2096 fits，不early stop。逐entry执行D3、扩展后的D4-01-MAP、
   D4-02-A与D4-03-PERSISTENT-A，保存full-train及与DIAG-01相同的early/late train-only结构重放，但
   `formal_d5_stability_gate_applied=false`。本首次实验不得执行D5 selection、D6、semantic mapping或model/READY write；
   即使某个seed达到131/131也只形成后续正式训练/验收决策输入，不能命名为selected或READY。
7. **成功、失败与不确定结果**：唯一可推进结果是：至少一个预声明seed在两个fresh processes中均达到131/131 D3/D4 accepted，
   且同一seed的early/late 131/131 train windows均通过第6项诊断比较，model/receipt hashes bitwise一致；此时只允许提交正式
   `TRANSITION-DWELL-B`模型合同启用与D5/D6执行授权。0个完整seed时状态为
   `diagnostic_complete_no_complete_candidate`并继续blocked；process/hash不一致或输入证据不足时为`insufficient_evidence`。
   不允许根据失败结果自动扩大tau/self-center/grid、换特征、进入HSMM或放宽阈值。
   false-accept风险是prior可能令train hard structure达到完整性但validation仍坍缩；因此首次实验不执行D6，后续正式D6仍独立
   fail closed且失败不得换seed。false-reject风险是真实regime的转移中心可能在`[0.50,0.90]`之外；由于该区间只约束有限强度
   `tau=8.0`的prior中心而非fitted transmat硬边界，风险低于直接projection，但超范围结果仍必须保持blocked，不能运行中调参。
8. **receipt与最小物化**：只允许repo-external compact report保存contract/producer/source identities、prior formula/version、逐seed/sector
   D3/D4状态与reason/hash、transition prior/raw transmat/expected-dwell摘要、early/late结构摘要、两process equality和side-effect flags；
   model parameters与完整history复用content-addressed child evidence，不嵌入report，不复制observation/posterior/matrix或历史大JSON。
   collision、duplicate key、非有限数值、profile/sector缺失、writer/readback authority不一致均fail closed。
9. **测试与changed-file路由**：未来实现限于既有`hmm.risk` offline training/acceptance和专用CLI/直接测试。fix-point必须覆盖
   prior公式、0.50/0.90闭边界、row normalization、invalid count/denominator、MAP transition项、raw transmat zero/non-finite/row-sum、
   D5 score不漂移、D4-03/D6 authority不变、双process mismatch、0/1/multiple complete candidate、no-D5/no-D6/no-write及compact writer/readback。
   changed files按ownership只运行`hmm_risk_backend` required plan、scope、静态检查和`git diff --check`；没有明确共享契约变化时不运行其他模块。
10. **执行结果与后续决策顺序**：用户确认本候选的0.50/0.90、`tau=8.0`、MAP公式、2096-fit范围与第7项判定后，源码、审核、BUG-1068修复及实验已依次完成。
    实验使用treatment producer `29417ceb…f8996fe`与冻结source `2ae9df85…be7fa`；两fresh processes各1048 fits，合计2096/2096 terminal entries，
    entry/model/profile payload hashes bitwise一致。完整候选seed为0，状态=`diagnostic_complete_no_complete_candidate`；parent body canonical
    `b6312171…582db`、完整对象canonical `e5f355fc…d4b54`。本结果只证明实现、数值复现和receipt lifecycle闭合，并证伪当前固定transition prior
    treatment足以解除131-sector完整性blocker；未执行D5/D6/selection，未写model/READY/DB/runtime。下一步只允许脚本化聚合两个既有child
    artifact中的逐seed/sector D3、D4和early/late结构失败分布，再提交新的精确模型决策；不得自动扩大tau/self-center/grid、换seed/feature、
    validation reselect、进入HSMM或放宽阈值，也不得把诊断完成改写为F-011验收完成。
11. **DESIGN-COMPLIANCE-001预审**：禁止简化交付=`PASS`，因为两个family/四level/READY目标未删减且单level实验不冒充交付；
    禁止静默错误=`PASS`，因为输入、prior、MAP、transmat、D4、repeat和writer均typed fail closed；禁止业务逻辑迁移=`PASS`，
    因为hard semantic authority、D4/D5/D6、seeds42..49、禁止stitching与两family完整性不变；禁止未经确认门禁/审批=`PASS`，
    因为新增数值均由用户明确批准，expected dwell只诊断且不新增人工运行时审批。结论为
    `PASS_EXACT_DESIGN_USER_APPROVED_NOT_IMPLEMENTED`。

##### D7. model identity、依赖与 READY

最终 B3 algorithm identity 至少必须包含：D2 窗口与 causal prior、经批准的 restart schedule、KMeans/HMM 全参数、preprocess、
likelihood/covariance/occupancy/semantic 阈值版本、selection formula/tie-break、全部候选摘要、selected family identity、
validation mapping/receipt、D6 base/amendment/composite versions、calendar carrier/manifest/masks hashes、运行库/数值环境和全部 input hashes。

只有两个 family 各自的 L1 31/31 与受控重训 L2 131/131 entry 都满足独立的
fit/convergence/likelihood/covariance/occupancy/selection/semantic evidence、coverage、parser/hash/causal replay，才允许构建
`READY` model set。D5-01-B固定每个family分别保存L1与L2 level-global selected identity；最终state-model-set identity必须
包含四个selected level receipt/hash及其family配对关系。任一level不得替代、重写或推导另一level的selection/acceptance，
也不得把既有L2 final parameters推导为数值验收通过。
D7-01-A 已批准并已在仓库 `requirements.txt` 精确声明 `hmmlearn==0.3.3`。用户另行授权在 Conda `AIstock` 环境执行
no-deps 安装，以保持既有 NumPy `2.4.0` 不变；import/version 与单线程 environment smoke 已通过。该依赖状态不推导
formal grid、runtime activation 或服务重启已完成。上述分离不是新增人工审批，而是 source、environment 与 runtime 的准确状态。

### 4.3.3 C-008-B3-FORMAL-BLOCKER-DIAG-01：正式制备 blocker 定向诊断设计

本决策项于 2026-07-30 获用户明确批准，并于 2026-07-31 按合同执行完成；当前状态为
`VERIFIED_DIAGNOSTIC_COMPLETE_NO_SELECTION_NO_READY`。它定义在一次完整正式制备已经
fail-closed 后，如何用受控、可复现、结果中立的定向诊断区分模型几何/初始化/数值优化问题与已批准 D4/D6 合同的真实
不满足。本次 docs 修订只回填既有执行证据，不重新运行诊断脚本或 HMM refit、不选择 seed、不修改任何已批准阈值，也不生成 model/READY。

#### A. 冻结权威与正式执行事实

诊断请求必须精确绑定下列不可变事实；任一 hash、producer、schema 或状态不一致均 fail closed，禁止自动改用“最新”报告：

- producer commit：`e2c01bae156281d551b084156fec4a09ed5a84ee`；
- formal receipt schema：`hmm_risk_b3_repeated_preparation_receipt_v1`；
- formal report canonical SHA-256：`e7992f87fb555eb26d6c2ef1ad9d45863954edd83fbfcc39f5ae01765cf3939f`；
- internal receipt SHA-256：`684b20471f54f17ada374b824b8d0703a770dcf9be9699cf9d15c46598f80362`；
- train dataset/mapping/calendar/direct-L2 SHA-256：
  `6afa5d35b350d3c58704e1da6308d3fff7f4e0fa06a9fe3050464026471665f3` /
  `2bc1c87a328758dc690e712ea2395972d0eb28f27412e0fc24633e8b04853560` /
  `af4a60cd23a079c015b3b1bca097de42c2da9948992a188e74a6640595b2f445` /
  `1a7f50f6d6782bfe36ff3638f8e0ddf06fbdb83328cb5cf126f5f1bdc66ef320`；
- semantic dataset/mapping/calendar/direct-L2 SHA-256：
  `5aa3778be68f081065c31e648c5781da68119e95b3ed1d9585769fb91de613dc` /
  `b80d1ee0c7628176c85053eeedd8a648c6384cdf7981a2bd54a22ea87e0fc864` /
  `f26f1a74aa80e42eddb0fa4de1f978dd605279f2340032b738704e4bccdeec08` /
  `a454985b3bab2692a09e5ca27b2d2552f4543baa81415f5672567f78f39f6a84`；
- formula/policy：`hmm_risk_l1_sector_factor_formula_v2_c010` /
  `ae8eda5bba1992965bcc8e17be6db1c6d9019d87417d7632d2b33a7728c220d9`；
- formal outcome：两次 fresh-process 共 `5184/5184` fits，`selection_performed=true`、
  `selection_used_validation=false`、`selection_used_future_utility=false`、
  `selection_followed_by_refit=false`、`ready_artifact_write_performed=false`、
  `database_write_performed=false`、`runtime_action_performed=false`、顶层 `status=blocked`。

正式 receipt 的 blocker 是本诊断 target authority，不能由手写 sector 清单或观察后的人工挑选替代：

| family/level | formal result | blocker closure |
|---|---|---|
| `autocycle_all_core:L1` | D5 无 eligible candidate | 9 个 rejected seed/sector identity；8 个 seed 的 `801030.SI` 均有 D4-03 run/transition/concentration failure，其中 seed 45 另有 covariance failure；seed 49 的 `801950.SI` 有 covariance failure |
| `autocycle_all_core:L2` | D5 无 eligible candidate | 74 个 rejected seed/sector identity、30 个 unique sector；stage events 为 train occupancy 14、covariance 40、likelihood 17、initialization 9 |
| `legacy_covfix:L1` | D5 选中 seed 43 | 31/31 D4 eligible；D6 仅 `801980.SI` failed，state 0 为 count 6、occupancy `0.032967...`、2 months、1 run、incoming/outgoing 1/1、max-run-share 1.0 |
| `legacy_covfix:L2` | D5 无 eligible candidate | 67 个 rejected seed/sector identity、14 个 unique sector；stage events 为 train occupancy 24、initialization 2、covariance 32、likelihood 12 |

#### B. 完整 target-set 与精确计算预算

定向诊断不得只选“最明显”的一个 sector，也不得重新运行 5184-fit 全 grid。target set 必须由 formal receipt 机械派生：

1. **D4 rejected set**：遍历 `autocycle_all_core:L1`、`autocycle_all_core:L2`、`legacy_covfix:L2`
   的全部 8 个 candidate `rejection_summary`，保留每个 `(family,level,seed,sector)` identity 和全部 failed stages/reasons。
   数量必须严格为 `9+74+67=150`；去重后仍为 150。一个 identity 同时失败多个 stage 时只 refit 一次，但所有安全可计算
   stage evidence 均须保留；初始化失败后不可伪造后续 stage evidence。
2. **D4 matched controls**：对上述三个 affected family/level 的每个 seed，各选 1 个 control，共 `3*8=24`。
   control 只能是 formal receipt canonical sector order 中不在该 candidate `rejection_summary` 的最小 sector code；选择过程只读取
   train-side D4 status，不读取 validation、future utility、semantic mapping、D5 score 或 READY 状态。若任一 seed 无合法 control，
   target derivation 失败，不减少 control 数量继续。
3. **targeted refit budget**：`150 rejected + 24 controls = 174` 个唯一 pair；每个 pair 在两个 fresh Python process
   各运行一次，精确预算为 `174*2=348 fits`。不得 early stop、扩大 seed grid、添加 sector 或把 control 当候选重新参与 selection。
4. **D6 replay set**：只回读 formal selected artifact `legacy_covfix:L1/seed=43`，包含 failed `801980.SI` 以及 canonical
   L1 sector order 上循环向前/向后最近的两个 `semantic_evidence_status=accepted` control。该规则在当前 frozen receipt 中得到
   `801970.SI` 与 `801010.SI`。D6 replay 使用已持久化 selected model，不 refit、不重跑 D5；精确为 3 个 replay entry、0 fit。
5. target manifest 必须保存 derivation algorithm version、formal report hash、完整 ordered rejected/control/D6 set、逐项 source
   receipt hash、数量与 canonical SHA-256。任何数量、重复 identity、source hash 或预期集合不一致均在 fit 前失败。

因此本诊断的上限是 348 个 targeted fit 加 3 个 selected-model D6 replay，而不是完整 grid。该缩减只减少与已知 blocker
无关的重复计算，不减少任一 formal rejected identity、affected family/level/seed 或失败 stage，不能被描述为模型交付子集。

#### C. 执行隔离与不可变业务语义

- 使用 formal run 相同 Conda `AIstock` 依赖、Python/NumPy/SciPy/scikit-learn/hmmlearn/threadpool identity 和五个线程变量为 1；
  两个 fresh process 的 target manifest、逐 entry inputs、parameters、likelihood history 与 evidence canonical hash 必须 bitwise equal。
- D4 targeted refit 只读取 frozen train observations；必须固定
  `validation_accessed=false/future_utility_accessed=false/semantic_labelability_accessed=false/selection_performed=false`。
- D6 replay 只能在 formal selected `legacy_covfix:L1/seed=43` 上读取唯一 frozen validation 和批准的 hard future utility；
  必须固定 `selection_reexecuted=false/refit_performed=false/soft_evidence_used_for_acceptance=false`。
- 保持两个 family、L1/L2 direct model、seeds 42..49、D3-03-A、D4-01-A、D4-02-A、D4-03-B、D5-01-B、
  D6-01-B 和 hard argmax semantic authority不变。不得排除 sector、拼接 per-sector seed、增加 restart、切换 B2、补 neutral、
  按 hidden-state index 映射、改变 validation 窗口或在 D6 失败后返回 D5 换 seed。
- 本诊断只计算“距已批准边界的证据”，不应用新阈值、不调整现有 pass/fail、不产出 eligible candidate、selected identity、
  model set 或 READY。`approved_threshold_distance_computed=true` 不得被解释为重新执行正式 acceptance。

#### D. 逐 stage 必须持久化的根因证据

1. **initialization/fit**：保存 KMeans labels/counts/centers/hash、每 cluster/feature 的 raw variance、`R_sj`、`ν=1.0`
   initialization/prior、startprob/transmat、完整显式 HMM profile、failure stage 和最具体 reason。空 cluster、少于两样本、
   non-finite center/variance/parameter 分开记录，禁止合并为 generic fit failed。
2. **likelihood**：保存完整 finite history/hash、iteration/max/tol、每个 comparable delta 的 index、terminal 标识、previous/current、
   absolute/relative delta。对 terminal negative 保存与批准边界 `-2e-5` 的 signed distance
   `relative_delta-(-2e-5)`；non-terminal negative 始终保留 failure，不以 signed distance 形成容忍区。
3. **covariance**：逐 cell 保存 `C_raw/R_sj/M_k/W_kj/C_expected/L/U/E_kj`。下界 signed slack 为
   `C_raw-(1-0.005)*L`，上界 signed slack 为 `(1+0.005)*U-C_raw`，M-step slack 为 `0.02-E_kj`；
   同时保存最小 slack、越界方向、state/feature mask/hash和距离分位数。posterior只使用 raw fitted covariance，禁止 clip/projection。
4. **train hard structure**：保存 causal posterior/hard sequence/date hash；逐 state 保存 count、occupancy、month、run、incoming、
   outgoing、max-run-share 与相对 D4-03-B 各闭/开边界的 signed distance。必须同时保存完整 run-length sequence 与 transition matrix，
   不能只重复 reason code；soft mass/ESS/entropy只作并列 diagnostic，不补足 hard evidence。
5. **D6 hard semantic**：对 3 个 replay entry 保存 validation posterior/hard sequence/date、run-length/transition、逐 state
   count/ratio/month/run/incoming/outgoing/max-run-share、hard utility count/mean/ddof=1 variance、adjacent numeric gap 与各 approved
   D6-01-B 边界的 signed distance。`801980.SI` state 0 的 run/transition/concentration failures 必须与 formal receipt hash 对齐；
   两个 control 仅用于解释相同 selected model family/seed 下的结构差异，不参与 selection 或阈值拟合。
6. **matched comparison**：按 family/level/seed/stage 对 rejected 与 deterministic control 计算同字段差异，保存 raw observation、
   fitted parameter和evidence hash。输出只能分类为 `initialization_failure`、`likelihood_failure`、`covariance_failure`、
   `hard_structure_failure`、`multi_stage_failure` 或 `insufficient_evidence`；不得自动给出“放宽阈值”“淘汰 family/sector”或
   “改用成功 seed”的执行决定。

#### E. Artifact、失败语义与完成标准

唯一输出 schema 候选为 `hmm_risk_c008_b3_formal_blocker_diag01_v1`，repo 外 append-only 写入。顶层至少包含：

- producer/source commit、formal authority hashes、numeric environment、target derivation version/hash；
- expected/observed fit 与 replay counts、两个 fresh-process payload hashes、bitwise equality；
- 174 个 targeted pair 的逐 process evidence、3 个 D6 replay、stage/reason/sector/seed 聚合；
- signed-distance formulas/version、raw evidence hashes、matched-control mapping、missing-evidence map；
- `status=diagnostic_complete|blocked|failed`，以及
  `selection_performed=false`、`acceptance_decision_reexecuted=false`、
  `formal_thresholds_changed=false`、`hard_semantic_authority_changed=false`、
  `model_write_performed=false`、`ready_artifact_write_performed=false`、
  `database_write_performed=false`、`runtime_action_performed=false`。

新增或复用的诊断 reason 必须稳定并分层：authority/hash mismatch、target-set incomplete/duplicate、control unavailable、
fresh-process mismatch、initialization evidence incomplete、likelihood evidence incomplete、covariance evidence incomplete、
hard-sequence evidence incomplete、D6 replay mismatch、non-finite diagnostic、artifact collision。不得吞掉底层 D3/D4/D6 reason。

只有同时满足以下条件才可把本诊断本身标为 `diagnostic_complete`：冻结 authority 全部匹配；150 rejected、24 controls、
348 fits、3 D6 replay 全部闭合；两个 fresh-process canonical payload bitwise equal；所有安全可计算的 stage 字段完整；
formal blocker 可从 artifact 独立重建；所有 no-selection/no-write flags 为 false。`diagnostic_complete` 仍不表示任何 candidate、family、
model set 或 Phase 2 READY，也不授权下一步模型/阈值变更。诊断结果完成后，若证据支持模型合同变更，必须以新的精确 decision
条目回填本设计并由用户确认；若现合同正确拒绝，则保持 blocked，不把“无可用 candidate”伪造成工程成功。

#### F. 正式诊断执行证据（2026-07-31）

`C-008-B3-FORMAL-BLOCKER-DIAG-01` 已按上述冻结合同完成，状态从“implementation ready”更新为
`VERIFIED_DIAGNOSTIC_COMPLETE_NO_SELECTION_NO_READY`。权威 evidence 为：

- artifact：`F:/Dev/AIstock_artifacts/hmm_risk/b3_blocker_diag01_20260731_ac3687c2/blocker_diagnostic.json`；
- schema：`hmm_risk_c008_b3_formal_blocker_diag01_v1`；producer：
  `ac3687c2e56d000a1fae6d196a8334e46060b07b`；
- byte SHA-256：`087fe24b1f85e84738b60256e57481d4b3899d60328db800247152eaefe95ed0`；
  canonical JSON SHA-256：`10287e845f07bf3d9c15a68e5d09ad14e54613348824ac2af568f0244a1cffe8`；
- formal report、target manifest 与 numeric environment SHA-256 分别为
  `e7992f87fb555eb26d6c2ef1ad9d45863954edd83fbfcc39f5ae01765cf3939f`、
  `ead996ed7ade9346bbc29bd2c64f5228e91086450bef5b475e79068919c7d06b`、
  `869e2d015c4ff18dbf04f0aa4ab0ba6c633519e389bbc62b8a152146fd68a6fc`；
- 174 pair/process、两个 fresh process、`348/348` fits；两次 payload SHA-256 均为
  `cd8345634b823bcc8cfb38e5e89ac745c15f26f16c50a751193e2552ce9d0a02`，两次 pass receipt SHA-256 均为
  `87d511a888c9c0ead8d08d3116975b3a54b7f3ab822a417988e9fd671d6cb8b0`，
  `canonical_payload_bitwise_equal=true`；
- 163 fit completed、11 initialization fit failed；三个 D6 replay 完整且不 refit、不 reselect；
- `selection_performed=false`、`selection_reexecuted=false`、`acceptance_decision_reexecuted=false`、
  `formal_thresholds_changed=false`、`hard_semantic_authority_changed=false`、model/READY/database/runtime write/action flags 全为 false。

150 个 formal rejected pair 的唯一主分类为：covariance 65、hard structure 43、likelihood 21、initialization 11、
multi-stage 10。允许重叠的 stage 事件为 covariance 74、train occupancy 46、likelihood 29、initialization 11；这证明
formal blocker 不是单一 seed、单一 sector、单一 family 或单一数值阶段造成。逐 family/level 分类为：

| family/level | classification evidence |
|---|---|
| `autocycle_all_core:L1` | covariance 1、hard structure 7、multi-stage 1 |
| `autocycle_all_core:L2` | covariance 35、hard structure 12、initialization 9、likelihood 12、multi-stage 6 |
| `legacy_covfix:L2` | covariance 29、hard structure 24、initialization 2、likelihood 9、multi-stage 3 |

artifact 中150个 comparison 均有 `missing_evidence_entry_count>0`，但该字段是 rejected/control 两个变长 numeric tree
路径的 symmetric difference，包括 likelihood history 与 run-length 数组长度差异；不得解释为150个 pair 都缺少正式证据。
直接 `missing_evidence` 非空的仅为11个 initialization failure；它们未产生 fitted model、likelihood、covariance或train occupancy，
这是执行阶段的自然不可用证据，不能用 null、默认值或 control evidence 补齐。

D6 replay 对 `legacy_covfix:L1/seed=43` 证明：`801980.SI` 的 hard assignment、posterior normalization与三态 hard utility
均有效，失败只发生在 state 0 的 temporal structure evidence——count slack `+1`、month slack `0`，但 incoming/outgoing、run
slack 均为 `-1`，单一 run 长度为6、run-concentration slack为 `-0.1`。相邻 controls `801970.SI` 与 `801010.SI`
均 accepted。因此不得把该 blocker 改写为 hard-state missing、utility tie、posterior normalization、全局 D6 缺陷、source-data
absence 或 seed nondeterminism。

#### G. 根因结论与后续精确设计边界

诊断证据足以排除“只换一个全局 seed”“只修一个 sector”“只处理 covariance”“放宽 D6 即可”“数据缺失导致全部失败”
等单因果方案，但不足以批准新的模型/optimizer/prior/transition/occupancy/covariance/semantic threshold。后续必须把以下五个
remediation decision 独立提交详细设计和用户确认，不能在实现中临时选择数值：

1. initialization remediation：仅针对11个 initialization failure，明确 cluster degeneracy 的处理算法与失败语义；
2. likelihood remediation：仅基于29个 likelihood stage事件，明确 optimizer/iteration/convergence合同，禁止按 observed envelope
   自动放宽 D4-01-A；
3. covariance remediation：针对74个 covariance stage事件，明确 emission covariance parameterization/regularization 与 D4-02-A
   的关系，禁止 post-fit clip/projection；
4. train-structure remediation：针对46个 D4-03 stage事件，明确是否调整模型表示/transition/dwell结构；不得通过删除低频state、
   降低已批准 evidence threshold 或 per-sector seed stitching伪造 candidate；
5. D6 temporal-evidence remediation：只处理 selected `801980.SI` 的 run/transition/concentration根因；不得改变 hard semantic authority、
   使用 validation reselect、加入 neutral/index/soft fallback，或以两个 control 的通过结果覆盖 failed entry。

上述五类remediation中，常量维子问题已选择D1-B设计方向；其源码随后已实现，controlled-refit也已另行获授权但在0 fits由
C-010-A5上游数据域合同阻断，其余initialization、likelihood、
covariance、train-structure与D6 temporal机制继续为`PROPOSED_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY`。F-011-B/C/D保持
blocked，不运行新的完整grid、不产生candidate/selection/model/READY。获批后的验证也必须先执行受控诊断验证模型机制，正式grid
仍沿用既有执行授权边界；不得把本诊断的348 fits复用为正式acceptance。

#### H. C-008-B3-REMEDIATION-DIAG-02：模型修订前证据闭合设计

状态：`RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_DIAGNOSTIC_EXECUTED_NO_MODEL_DECISION`。该项仍不批准任何模型修订；
它已在不运行 HMM 的前提下补齐会直接影响方案选择的证据缺口，避免同时修改 initialization、EM、covariance 与 transition
后无法识别真实因果。

2026-07-31 的正式执行使用 producer `b2456424b859f1635635129aa6a826a677f4fdec`，repo 外 append-only artifact 为
`F:/Dev/AIstock_artifacts/hmm_risk/b3_remediation_diag02_20260731_b2456424/remediation_diagnostic.json`，canonical SHA-256 为
`48157a4255e9d19b814b26b90b18ec38769e28fd0a18e58403edb83fc660bb58`。独立 readback 确认 receipt 有效，并闭合：

- 324/324 唯一 train profiles：`autocycle_all_core:L1/L2=31/131`、`legacy_covfix:L1/L2=31/131`；
- train-only projection 为 174 entries，其中 `fit_completed=163`、`fit_failed=11`，role 为 `rejected=150`、`control=24`；
- 11/11 initialization source identity保持为8个persistent zero-variance与3个singleton cluster，不重跑KMeans；
- 163个completed entry的likelihood/covariance cross-matrix、raw-pair correlations和train-structure evidence完整；
- `hmm_refit/selection/validation/formal_acceptance/threshold_change/model/READY/database_write/runtime_action`均为false。

结果证明唯一zero-variance profile为`autocycle_all_core:L2/801207.SI/sf_dispersion_5d_neg`，raw与preprocessed variance均为0，
其余130个同family/level feature profile均为positive；因此这不是winsor/z-score制造的常量，但该诊断不把描述性provenance
外推为公式缺陷或模型处置批准。163个completed entry中46个存在至少一个D4-03 train-structure signed-distance failure；
`autocycle_all_core:L1/801030.SI`、`legacy_covfix:L2/801019.SI`、`801207.SI`、`801782.SI`在其各自8个seed上持续失败。
相关性分析有6个`total_anomaly_cell_count`常量向量组，均显式为`insufficient_evidence`并保留raw pair；不生成0 correlation、
p-value、因果结论或新threshold。

**1. initialization failure 分层事实。** 11个 failure 不是同一根因：

- `autocycle_all_core:L2/801207.SI` 在 seeds 42..49 的8次 failure均由第20个 feature
  `sf_dispersion_5d_neg` 的 sector-local train variance精确为0触发；其余19维finite且positive。该失败与seed无关，
  不能用扩大seed grid解决；
- `autocycle_all_core:L2/801129.SI/seed=43` 的 KMeans counts 为 `1/284/188`；
  `legacy_covfix:L2/801113.SI/seed=42` 为 `300/298/1`；
  `legacy_covfix:L2/801769.SI/seed=49` 为 `308/290/1`。这3次是seed-specific singleton cluster，当前D3-02-B正确fail closed；
  在尚有其他restart可用时，不得为了挽救这3个candidate私自降低cluster成员门槛。

**2. no-fit full-profile variance provenance。** 对formal冻结输入中的四个family/level domain机械派生
`2 * (31 L1 + 131 L2) = 324` 个唯一 train profile；每个profile只计算一次，不按8个seed重复。逐feature保存：

- approved feature name/order、raw float64 observation hash、row count、unique finite value count；
- `min/max/mean/var_ddof0`、zero/non-positive/non-finite分类；
- ordered train dates必须严格递增、`max_date<=approved train_end`，且profile的dataset/mapping/calendar/direct-level/input hashes
  必须与formal train authority一致；不得读取semantic manifest或validation rows补足profile；
- 对zero variance保存全部值是否exact zero、公式component hash、preprocess前后variance与source/provenance receipt；
- 按family/level/feature聚合positive variance的count、min和固定分位数
  `q=[0,0.01,0.05,0.25,0.50,0.75,0.95,0.99,1]`，但不据此生成floor或acceptance threshold；
- 324/324 profile、7/20维shape和formal train manifest identity任一不闭合即diagnostic failed，不静默跳过profile/feature。

数值算法必须唯一：输入先验证为C-order little-endian float64且全部finite；`-0.0`在value identity中规范化为`+0.0`，
但raw observation hash仍保留原始float64 bytes；unique count按规范化后的float64 bit pattern计算。mean使用`math.fsum(x)/N`，
variance使用`math.fsum((x-mean)^2)/N`。positive variance按数值升序排列；对分位点`q`，令
`h=(n-1)q`、`i=floor(h)`、`j=ceil(h)`，结果为`x_i+(h-i)*(x_j-x_i)`，`n=1`时返回唯一值。
`n=0`时该family/level/feature distribution固定为`insufficient_evidence`、quantiles固定为null并保留count=0；它不覆盖逐profile
zero-variance evidence，也不得生成floor。
不得由NumPy/SciPy默认quantile method、普通`sum`或set/hash随机顺序补全算法。

该证据用于区分：公式/样本域内真实常量、preprocess制造常量、source/provenance缺陷。若发现source或公式实现不符合已批准
C-010 identity，必须单独登记BUG；若确认为真实常量，再由后续用户决策在“保持fail-closed”“显式inactive-dimension model identity”
与“正reference prior”之间选择。后两项都会改变D3/model identity，不能在diagnostic脚本中默认采用。

**3. likelihood 与 covariance fixed-point association。** 只使用本次artifact中163个fit-completed entry的train-only
allowlisted projection，不refit。163条必须显式分层为139个`role=rejected`和24个`role=control`，不能把targeted sample
描述为formal universe的随机或代表性样本。逐entry重聚合：

- terminal/non-terminal delta、relative delta、iteration与D4-01-A signed distance；
- D4-02-A `M-step relative residual`、dynamic-bound slack与anomaly mask；
- cross matrix的row固定为likelihood
  `accepted|accepted_with_warning|failed|insufficient_evidence`，column固定为covariance
  `accepted|failed_bounds_only|failed_mstep_only|failed_bounds_and_mstep|invalid|insufficient_evidence`；每格保存count和ordered
  entry identity/hash，不得使用“2×N”或实现自行合并状态；
- correlation的`x`固定为每个entry最后一个comparable delta的`relative`；`y`分别固定为
  `mstep_max_abs_relative_residual`、`covariance_min_lower_slack`、`covariance_min_upper_slack`和total anomaly cell count，
  四组统计不得互相替代；
- 分组固定为overall、每个`family/level`、每个`role`及每个`family/level/role`；每组保存role composition、ordered raw
  `(entry_identity,x,y)` list/hash。有效pair少于3或任一向量为常量时，单组显式`insufficient_evidence`；任一raw pair
  含non-finite则该group和顶层均以`hmm_risk_remediation_diag_numeric_non_finite`失败，不得降级为insufficient、伪造0
  correlation或使其他组静默消失；
- Pearson固定为centered-product numerator除以两个centered-square sum平方根；Spearman先按数值升序、ties取1-based average rank，
  再对rank执行同一Pearson公式。所有accumulation使用`math.fsum`，不计算p-value、显著性或自动decision threshold。

当前29个likelihood failure全部是terminal negative且低于已批准`-2e-5`边界，没有non-terminal negative；这只支持检查
“hmmlearn终止步与final fixed-point residual是否存在描述性关联”，不支持因果结论、放宽D4-01-A或自动选择EM/covariance方向。
任何observed association都只能形成后续controlled-refit设计的输入；monotonic EM/step-damping或covariance
parameterization/prior必须另立精确decision、保留current-profile control并取得用户确认。任何候选都必须使用新algorithm identity，
禁止把negative step从history中删除或把post-fit参数投影成通过。

**4. train-structure evidence重聚合。** 对163个completed entry回读D4-03 signed distance与完整hard sequence，按state和
family/level聚合count/month/run/transition/run-share失败的交集、最小slack和affected sector/seed persistence；不重算正式
acceptance、不改变1%/month/run/transition阈值。该结果只决定是否需要提出新的train-only model structure候选，不允许直接降低
D4-03-B、删除低频state或将soft posterior转为authority。

**5. D6边界。** DIAG-02不新增validation访问。现有3-entry replay已足以证明`801980.SI`是selected-model的sector-specific
temporal evidence failure；不得对其他seed/sector批量执行D6来挑选会通过validation的模型。只有未来经批准的train-only模型机制
完成并经D5冻结新的selected identity后，才按现有D6-01-B执行唯一正式validation。

**6. train-only projection。** blocker artifact的完整bytes/canonical hash只用于authority验证；计算输入必须重新构造并hash
allowlisted projection：`schema_version/diagnostic_contract/diagnostic_producer_commit/formal_authority/numeric_environment/`
`numeric_environment_sha256/targeted_evidence`。projection不得包含`d6_replay`、semantic receipt、validation observation、future
utility或semantic mapping；每个targeted entry只允许identity、role、train input hash、formal failed stages、training receipt、
signed distances、hard train sequence和no-access/no-write flags。发现非allowlisted payload、entry自身
`validation_accessed!=false`或`future_utility_accessed!=false`时，整个diagnostic以
`hmm_risk_remediation_diag_train_projection_invalid`失败。读取完整artifact bytes以核验authority不等同于消费validation；
`validation_accessed=false`只允许在上述projection hash与field-access audit同时成立时写入。

**7. artifact、错误合同与完成语义。** 唯一候选schema为 `hmm_risk_c008_b3_remediation_diag02_v1`，repo外append-only写入。
顶层必须包含：producer/source commit、formal/blocker canonical hashes、train projection hash、numeric environment hash、324-profile
manifest/hash、variance evidence、11-entry initialization failure source evidence/hash、163-entry role-stratified association/structure
evidence、section statuses、reason arrays、canonical receipt hash和全部side-effect flags。11-entry evidence必须保持8个persistent
zero-variance与3个singleton cluster的exact identity/counts并引用原diagnostic entry/source receipt hash，不得重新运行KMeans或用324-profile
variance替代singleton证据。顶层`status`只允许`diagnostic_complete|failed`；各统计group允许
`complete|insufficient_evidence|failed`，但不得用group insufficient伪造数值或把必需raw evidence缺失降级为insufficient。

稳定reason codes至少包括：

- `hmm_risk_remediation_diag_authority_mismatch`；
- `hmm_risk_remediation_diag_train_projection_invalid`；
- `hmm_risk_remediation_diag_profile_manifest_incomplete`；
- `hmm_risk_remediation_diag_profile_temporal_boundary_invalid`；
- `hmm_risk_remediation_diag_variance_evidence_invalid`；
- `hmm_risk_remediation_diag_initialization_source_mismatch`；
- `hmm_risk_remediation_diag_statistic_insufficient`（仅group status，不使raw evidence缺失通过）；
- `hmm_risk_remediation_diag_statistic_evidence_invalid`；
- `hmm_risk_remediation_diag_numeric_non_finite`；
- `hmm_risk_remediation_diag_artifact_collision`；
- `hmm_risk_remediation_diag_artifact_write_failed`；
- `hmm_risk_remediation_diag_readback_mismatch`。

写入前目标已存在时只允许canonical-identical readback，否则collision fail；先写同目录唯一temporary file、flush/fsync，
再以同卷atomic hard-link且不得覆盖既有目标的方式发布，最后执行bytes与canonical readback；并发发布目标已存在时仍只允许
identical payload，否则collision fail。失败不得把partial temporary解析为complete目标。顶层固定
`hmm_refit_performed=false/selection_performed=false/validation_accessed=false/formal_acceptance_reexecuted=false/`
`threshold_changed=false/model_write_performed=false/ready_artifact_write_performed=false/database_write_performed=false/`
`runtime_action_performed=false`。只有324/324 profile、11/11 initialization source evidence、139 rejected+24 control
completed-entry evidence、全部必需cross-matrix/raw pair/structure evidence、projection boundary和canonical readback闭合时才可
`diagnostic_complete`；该状态仍不使任何remediation
implementation ready。

**8. 计算预算。** 精确预算为324个train-profile单次scan、163个既有entry重聚合、0 KMeans、0 HMM fit、0 D5/D6。
实现必须逐profile streaming，内存上限为一个最大profile的`rows*feature_count` float64 matrix加固定聚合器，不得把324个matrix
同时常驻内存或复制为第二套dataset。输入只读、artifact repo外append-only；数据库写入、依赖安装、服务控制与runtime action均为0。

本no-fit诊断已经完成。下一步唯一推荐为先形成`C-008-B3-REMEDIATION-D1`精确设计决策：只处理
`autocycle_all_core:L2/801207.SI/sf_dispersion_5d_neg`真实sector-local常量维的模型identity语义，在“保持fail-closed”与
“显式inactive-dimension identity”之间给出完整公式、artifact/hash、runtime parser兼容、false accept/reject和controlled-refit
验收；不得在该决策中同时修改EM、likelihood tolerance、covariance acceptance、transition/dwell或D4-03/D6 threshold。
正reference prior仍属于另一个模型机制，不得与inactive dimension捆绑实施。用户现已选择D1-B设计方向；该选择不是新增模型gate、
自动淘汰feature或人工runtime审批，也不授权源码或fit；在后续独立授权前不实施B3、不运行新fit、不选择seed、不生成model/READY。

#### I. C-008-B3-REMEDIATION-D1：sector-local 常量维模型 identity 精确设计

状态（P1源码审核当时的历史边界）：`D1_B_P1_SOURCE_IMPLEMENTED_CONTROLLED_REFIT_NOT_EXECUTED`。用户已选择D1-B并明确不采用D1-A；P1最小源码已实现，
但不授权受控refit、D5/D6或model/READY。权威输入证据仍仅为 REMEDIATION-DIAG-02 producer
`b2456424b859f1635635129aa6a826a677f4fdec`、canonical `48157a4255e9d19b814b26b90b18ec38769e28fd0a18e58403edb83fc660bb58`。

**1. 决策对象与非目标。** 当前唯一允许进入D1决策的identity固定为：

```text
family=autocycle_all_core
level=L2
sector_code=801207.SI
feature_index=19
feature_name=sf_dispersion_5d_neg
approved_family_feature_count=20
raw_variance_ddof0=0
preprocessed_variance_ddof0=0
all_raw_values_exact_zero=true
unique_finite_value_count=1
```

D1不把“任意低方差”“任意常量”“任意初始化失败”扩大为同一合同。非零常量、近零但非零variance、source/provenance不闭合、
non-finite、preprocess单独制造常量或新增profile命中时均保持fail closed，并要求新的evidence/design版本。D1不处理3个singleton
KMeans failure，不修改KMeans cluster count、EM、likelihood tolerance、covariance bound/prior、transition/dwell、D4-03、D5或D6；
不删除global 20-feature业务合同，不把该sector从family中排除，也不扩大seed grid。

**2. 精确选项。**

- `C-008-B3-REMEDIATION-D1-A`：`NOT_SELECTED`。该方案保持现有fail-closed；优点是model identity、D5 score与runtime parser完全不变；代价是
  `autocycle_all_core:L2/801207.SI`在当前冻结数据和8个restart上持续无法进入fit，family仍blocked。该方案没有伪成功，
  但无法验证常量维显式投影是否能保留其余19维信息。
- `C-008-B3-REMEDIATION-D1-B`：`RESOLVED_USER_SELECTED_DESIGN_DIRECTION`。采用显式inactive-dimension model identity；当前选择
  不授权源码、后述controlled-refit或formal grid。该方案保持完整20-feature observation与
  preprocess contract，在model likelihood之前使用显式、hashed projection选择19个active dimensions；不加noise/floor，
  不伪造inactive covariance，不把hidden-state或hard semantic语义改为soft authority。

D1-B的主要false-accept风险是：19维模型可能在train numeric contract上通过，但被排除的feature在未来重新出现信息量；因此model
identity必须永久记录inactive维，runtime不得动态激活。主要false-reject风险是：严格exact-zero allowlist只修复当前已证明identity，
不会自动处理未来其他真实常量维。该保守边界用于保持因果可归因，不是研究方向淘汰gate。

**3. D1-B projection 算法。** 算法版本固定为`hmm_risk_c008_b3_d1_inactive_dimension_v1`：

1. 从冻结train input构造完整20维C-order little-endian float64矩阵`X_raw`，验证shape、feature order、dates、dataset/mapping/calendar、
   direct-L2与C-010 formula/provenance identity；任何non-finite或hash漂移失败。
2. 使用既有批准的`autocycle_all_core` full-20 preprocess identity计算`X_pre20`；不得先删除第20维再重新估计winsor/center/scale。
3. exact-zero资格只由index 19的raw向量决定：raw必须finite、variance_ddof0精确为0、归一化unique bit-pattern count为1，且所有raw值
   精确等于`+0.0|-0.0`；profile/formula/provenance hashes仍与批准receipt闭合。`X_pre20`必须与批准的full-20 preprocess公式精确重算结果
   bitwise一致，但inactive列在winsor/center/scale后允许为确定性的非零常量；receipt必须同时保存expected/observed preprocessed vector hash并相等。
   该列随后由固定mask删除且永不参与likelihood，不能因为preprocess改变其数值而动态激活。`-0.0`只在raw value identity归一为`+0.0`，
   raw bytes/hash仍保留原值。
4. `inactive_feature_indices=[19]`；`active_feature_indices=[0..18]`。`active_feature_mask`固定为长度20的JSON boolean list，
   list index等于approved feature index，0..18为true、19为false；不得使用实现相关bit-endian整数。令`P`为按该固定顺序抽取column的20→19 projection，
   `X_active=X_pre20[:,active_feature_indices]`。禁止PCA、feature reorder、imputation、noise、epsilon variance、dynamic mask或per-seed mask。
5. KMeans/HMM只接收`X_active`，参数shape按19维验证；startprob/transmat与D3-02-B初始化语义不变。fit后仍禁止projection/clip。
6. 没有inactive feature的control profile通过同一代码路径，固定`active_feature_indices=[0..19]`且`P=identity20`；其既有model
   parameter bytes、likelihood history与D3/D4 evidence必须与冻结control receipt bitwise一致，排除通用路径回归。

projection receipt至少包含full/active/inactive feature names与indices、20位mask、mask canonical hash、projection algorithm/version、
full preprocess identity/hash、raw/preprocessed inactive vector hashes、exact-zero evidence、source profile receipt、projected matrix shape/hash、
dataset/mapping/calendar/formula/provenance hashes和`dynamic_activation=false`。任一字段缺失或不一致不得默认为identity projection。

**4. Artifact 与 parser/runtime 契约。** D1-B未来model entry schema使用
`hmm_risk_b3_inactive_dimension_model_entry_v1`，并同时保存：

- global `feature_names`仍为批准的20维顺序；`feature_count=20`；
- `likelihood_feature_count=19`、active/inactive lists、projection receipt/hash；
- 3×19 means/covariance与19维KMeans center/initial covariance；不得保存伪造的第20维mean/covariance；
- full preprocess参数及其20维identity；parser先执行full preprocess再project；
- algorithm/model identity必须覆盖D1版本、projection hash、active dimension count、producer/dependency/numeric-environment identity；
- inference输入仍必须提供完整20维finite observation。inactive feature在该model lifetime内永不参与likelihood；即使未来值变为非零，
  也不得动态激活或改shape。该值及`inactive_feature_observed_non_zero`只进入可审计diagnostic receipt，不成为未批准的runtime gate；
  下一次训练是否重新激活只能由新model identity和当次train evidence决定。

parser遇到unknown projection version、mask/hash漂移、20维输入缺失、active参数shape不是3×19、full preprocess identity不一致或
inactive model缺projection receipt时，使用`hmm_risk_model_inactive_dimension_contract_invalid`失败；不得fallback为20维、补0参数、
复制相邻feature或回退上一model。API/UI未来只在真实model进入上游F-013后展示full/active count、inactive feature与原因，不在本D1
controlled-refit阶段增加runtime endpoint或UI。

**4.1 mixed-dimension level/family model-set。** D1-B不得只修改单entry parser而让level writer继续假设统一20维。未来正式
artifact版本固定新增`hmm_risk_b3_level_model_set_v2_projection`，保持每个sector独立model entry，不允许把131个sector堆叠为统一
`[sector,state,feature]` tensor。对于`autocycle_all_core:L2`：

- global family contract仍保存完整20维`feature_names`和`feature_count=20`；
- 131个sector entry按`sector_code`升序持久化，当前唯一允许`likelihood_feature_count=19`的entry是`801207.SI`，其余130个必须为20；
  `likelihood_feature_count_histogram={"19":1,"20":130}`。任一额外19维、其他维数或缺失sector均fail closed；
- 每个entry独立保存`model_entry_sha256/projection_sha256/likelihood_feature_count/means_shape/covariance_shape`。level identity的输入固定为
  长度131的canonical list：

```json
[
  {
    "sector_code": "<ascending canonical code>",
    "model_entry_sha256": "<64 hex>",
    "projection_sha256": "<64 hex>",
    "likelihood_feature_count": 19
  }
]
```

  list使用`canonical_json_bytes`直接hash，不含path、timestamp、run id或dict iteration order；identity20 entry同样必须有显式identity
  projection receipt，禁止缺字段时默认20维；
- selected-level validator必须先验证global20 feature/preprocess identity，再逐entry验证projection与3×effective-dimension参数shape；
  不得将19维entrypad为20维、生成inactive mean/covariance或把该entry排除在131/131 completeness之外；
- family manifest继续要求同一level使用一个selected global seed。level/family/READY hash必须覆盖上述ordered entry list、dimension
  histogram和D5 selection receipt；D1不改变两个family、L1 31/31、L2 131/131或两family READY合取；
- `models_from_repeat()`、selected-level artifact validator、four-layer READY writer与runtime parser必须使用同一projection validator；
  writer/parser任一侧仍强制全20或对mixed shape采用fallback时，整个D1实现不得合入。

在D5 comparability未批准前，上述v2 schema只属于设计合同和controlled-refit candidate envelope，不得写正式selected-level或READY
artifact。现有`hmm_risk_b3_*_v1` artifact保持immutable，禁止原地升级或补projection metadata。

**4.2 runtime inactive-observation receipt。** `inactive_feature_observed_non_zero`不得只存在于临时日志。未来每次真实generation/replay
都在content-addressed `InputManifest`内保存按`family/level/sector_code/feature_index`升序的
`inactive_dimension_observation_receipts`，entry schema固定为`hmm_risk_inactive_dimension_observation_receipt_v1`，至少包含：

```text
model_set_id, model_entry_sha256, projection_sha256,
family, level, sector_code, trade_date, as_of_date,
input_manifest_source_sha256, feature_index, feature_name,
raw_value_float64_sha256, preprocessed_value_float64_sha256,
raw_value_is_finite, preprocessed_value_is_finite,
inactive_feature_observed_non_zero, receipt_sha256
```

dedupe key固定为`(model_set_id,model_entry_sha256,projection_sha256,sector_code,trade_date,input_manifest_source_sha256,feature_index)`；
同key不同receipt hash为`hmm_risk_inactive_dimension_observation_receipt_conflict`，不得覆盖。full InputManifest hash覆盖ordered receipts，
`daily_generation_run.input_hash`继续引用该manifest；revision/replay必须回读同一receipt set并验证hash。finite=false仍按既有observation
invalid语义fail closed；finite但非零只记录true，不动态激活feature、不改变posterior、不新增runtime人工gate。F-013未来只读展示该receipt，
本D1 controlled-refit不写DB、不创建API/UI，也不伪造runtime receipt。

**5. D4 与 hard semantic边界。** controlled-refit仍计算现有D3/D4 train evidence，但不应用新的正式acceptance：

- D4-01 history/tolerance、D4-02-A dynamic bounds/M-step residual与D4-03-B hard structure公式保持原值；
- covariance/reference/anomaly只对19个active dimensions计算，inactive维不计入“accepted”、不生成covariance，也不得用exact-zero
  evidence覆盖active维D4 failure；
- full20 observation/preprocess/projection evidence是独立`projection_status`，不推导fit/convergence/covariance/occupancy accepted；
- hard hidden-state assignment、future utility、fading/neutral/trending mapping均不在controlled-refit输入中，B2继续NOT_APPROVED。

未来formal grid采用D1-B时，`801207.SI`的D5 sector score分母使用用户已批准的
`LL_final/(N*d_effective)`：该entry的`d_effective=19`，其余130个entry为20。该决定是明确业务公式，不是从REFIT-03样本推导出的
普遍统计可比性结论，也不增加经验性阈值。`C-008-B3-REMEDIATION-D1-D5-COMPAT-01-A`已完成源码实现和本模块审核；
controlled-refit历史证据仍固定`selection_performed=false`，不得把历史局部fit写成implementation ready或正式D5结果。

**6. Controlled refit 精确合同。** 仅当用户选择D1-B并单独批准该执行后，允许运行
`hmm_risk_c008_b3_d1_controlled_refit_v1`：

- 冻结dataset/mapping/C-010 policy、train window、seeds 42..49、Conda AIstock dependency与单线程数值环境不变；
- 每个fresh process运行16 fits：treatment=`autocycle_all_core:L2/801207.SI` 8 seeds，control=
  `autocycle_all_core:L2/801011.SI` 8 seeds；两个fresh processes合计32 fits；不early stop、不扩大grid；
- treatment profile receipt固定为`36cc1afd004796ce3458ab7090010abd07ddd94807d2701318e39d6d80f84e3d`，其按seed排序的
  8-entry source identity set hash固定为`d75e40d3cd82cf232d9e7633bd982eb4189e7fc625d43c2f91f7d010cb7530fb`；control profile
  receipt固定为`9e372d3bde299533fbbf28dee81f1cfc9bb614677f34f78fa49fd82230864929`，对应8-entry set hash固定为
  `905d97c7987896e854c905a831be33f2732b6d35dd56bedf82571caec2fa2d06`；两者preprocess identity均为
  `cd7d759178449c7ec9bda7d1fbad0969a55cc1756361a0d70936f226909ab976`，feature definition identity均为
  `0445f91a5587dddb85e93fa5d08897ba967d41f10819e65eeb13a0353fac9aca`；
- 上述两个8-entry set hash的canonical envelope均固定为长度8的JSON list，按整数`seed`升序，每个object只允许且必须包含
  `seed`、`diagnostic_entry_sha256`、`source_entry_receipt_sha256`三个字段；使用`canonical_json_bytes(list)`直接SHA-256。
  不得加入role/status/path/timestamp，不得只hash unordered set或省略原source receipt；
- treatment使用固定19维projection；control使用identity20。control必须与blocker diagnostic中同identity的8个冻结model/training
  payload按allowlisted canonical bytes逐项bitwise一致；wrapper新增projection字段单独hash，不得污染旧payload比较；
- 两fresh-process的treatment与control canonical payload必须分别bitwise一致；allclose只用于mismatch diagnosis，不代替hash equality；
- treatment逐seed保存initialization、monitor/history、D4-01/D4-02/D4-03 descriptive states、parameter/projection hashes与完整raw evidence；
  不执行D5/D6，不读取validation/future utility，不生成model/READY；
- 即使8/8 treatment完成fit或通过既有D4，也只形成`diagnostic_complete`，不证明131/131、family candidate或READY；任一fit失败
  仍完整保留其stage/reason，禁止换seed、加floor或扩大范围。

顶层`status=diagnostic_complete`只表示32个预声明attempt均已形成durable terminal evidence，不等于mechanism结论。报告必须另含
`mechanism_assessment`，其状态机固定为：

- `inconclusive`：authority/profile/set-envelope/numeric environment不闭合、identity20 control与冻结payload不一致、两fresh-process
  hash不一致，或32 attempts任一缺失。出现该状态不得解释treatment，也不得进入D5设计；
- `constant_dimension_mechanism_rejected`：前述完整性均通过，但任一treatment仍以exact-zero eligibility、projection、preprocess或
  inactive-dimension parameter-shape reason失败。不得用其他seed成功覆盖该失败；
- `constant_dimension_effect_supported`：完整性均通过，8/8 treatment全部越过exact-zero/preprocess/projection/parameter-shape阶段，
  且不再出现persistent-zero-variance reason。后续singleton、monitor、likelihood、covariance或train-structure失败仍逐项保留，
  不影响“常量维机制效果”这一窄结论，也不得被该结论覆盖；
- `d5_compatibility_evidence_ready`是与上述状态分离的boolean，只有8/8 treatment均`fit_completed`、final train likelihood finite、
  D3/D4 descriptive receipts完整且两个repeat一致时才为true。false时D5 comparability继续blocked；true也只允许编写D5 compatibility
  决策，不执行selection。

`mechanism_assessment_reason_codes`必须列出所有未满足条件；不得使用first-error summary吞掉其他attempt。任何状态都固定
`selection_performed=false/model_write_performed=false/ready_artifact_write_performed=false`。

计算预算固定为32 fits；不运行legacy、L1、其他L2 sector或5184-fit grid。输出repo外append-only artifact，schema为
`hmm_risk_c008_b3_d1_controlled_refit_report_v1`，使用不可覆盖发布、canonical readback和完整side-effect flags。

**6.1 REFIT-01 实际执行证据与不可继续边界。** REFIT-01 已在 C-010-A5 与 BUG-962 合入后按授权重试，但最终仍在
`fresh_process_1`、首个 HMM fit 前 fail closed。权威 failure report 为
`F:/Dev/AIstock_artifacts/hmm_risk/d1_controlled_refit_20260803_f226922b_bug962/d1_controlled_refit.json`，schema 为
`hmm_risk_c008_b3_d1_controlled_refit_report_v3`，producer commit 为
`f226922b066c37699e780dc5917200c1fa28f9e2`。readback 必须保留以下事实：

- `status=diagnostic_failed`、`attempt_count=0`、`completed_process_count=0`、`mechanism_assessment=inconclusive`；
- failure reason 为 `hmm_risk_model_inactive_dimension_authority_mismatch`，精确差异字段为
  `train_observation_sha256`；
- current-A5 authority 为 report `e7f7edc9fbe7f1cdb5ec739e1390fffec69a9ede6c8d719c9dda1a21df71773d`、
  partition `03d785347b35185fe9f9c771e0a4e69cd0deb8def31a0cb205d3ca7a86b8ead6`、mapping
  `6ed16f4e8473d851be7e359aac431c241bb98f0ba18dd3e7b537ca519f7fd696`；历史 formal/blocker/remediation
  hashes 仍按原值只读保留；
- `selection_performed=false`、`d5_compatibility_evidence_ready=false`、model/READY/database/runtime write/action 均为false。

该结果不是 provider coverage 缺失，也不是 C-010-A5 receipt-envelope 解析错误。BUG-962 已证明 A5 业务 payload 能按批准规则迁移；
最终 blocker 是 current-A5 重新构造的真实 train observation 与历史 frozen train observation 不同。因此 REFIT-01 的
“current-A5 输入 + 历史 control model/training payload bitwise equality”成功条件已被真实证据否定。不得通过排除
`train_observation_sha256`、复制历史数组、回退 C-010 v1、覆盖历史 artifact 或把 mismatch 降级为 warning 来继续。
REFIT-01 固定为 `VERIFIED_ATTEMPTED_INCONCLUSIVE_TRAIN_CORE_DRIFT_ZERO_FITS`，只保留历史审计价值，不再作为可执行合同。

**6.2 REFIT-02-A：current-A5 同基准三角色受控实验提案。** 决策ID为
`C-008-B3-REMEDIATION-D1-B-REFIT-02-A`，algorithm version 固定为
`hmm_risk_c008_b3_d1_refit_02_a_v1`，状态为
`RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_LOCAL_REVIEWED_NOT_EXECUTED`。用户批准覆盖本节三角色、固定预算、v4 receipt和no-access/no-write边界；源码已按该合同实现并完成正式审核。该批准不包含真实32-fit执行、D5/D6、model/READY、数据库、runtime、commit、PR或merge。

1. **单一 current-A5 authority。** treatment、matched negative control 与 harness control 必须由同一次 current-A5 请求、同一
   dataset/mapping/calendar、同一 train window `2022-01-01..2024-06-30`、同一 C-010-A5 policy/report/partition 和同一
   observation producer 构造。每个role保存完整 train input manifest、raw observation hash、preprocess hash、日期/row count、
   feature order和source identity。historical formal/blocker/remediation artifact 只作为 immutable provenance reference；不得再要求
   current model/training payload与历史payload相等，也不得用历史数组参与当前fit。运行child前必须先对current-A5的801207 full20
   profile重算exact-zero资格；index 19若不再满足raw exact-zero，或full-20 preprocessed matrix不能由批准preprocess精确重算，或其余19维不满足finite与positive variance，则顶层固定
   `status=not_applicable`、`mechanism_assessment=constant_dimension_mechanism_not_applicable_current_profile_changed`、attempts/fits=0，
   不得继续跑旧D1假设，也不得把not-applicable写成effect supported/rejected。
   `current_a5_experiment_authority_sha256`必须对固定envelope直接执行`canonical_sha256`；envelope只允许且必须包含
   `schema_version/c010_a5_report_sha256/c010_a5_partition_sha256/c010_a5_mapping_sha256/dataset_manifest_sha256/`
   `calendar_sha256/train_start/train_end/family/level/feature_domain_policy_sha256/role_inputs`。`role_inputs`按role name排序，每项固定
   `sector_code/row_count/min_date/max_date/train_observation_sha256/full20_preprocess_sha256/observation_manifest_sha256/`
   `pit_constituent_manifest_sha256/feature_names_sha256/train_input_manifest_sha256`；treatment与matched negative必须引用同一个801207 role-input hash。
2. **三角色与唯一用途。** 每个fresh process固定运行以下角色，不得删减或互相替代：
   - `treatment_19d`：`autocycle_all_core:L2/801207.SI`，先完成批准的full20 preprocess，再使用固定20→19 projection，seeds 42..49；
   - `matched_identity20_negative`：同一`801207.SI`、同一full20 raw/preprocessed observations和同一seeds，禁止projection，保留
     identity20。它必须以原始exact-zero initialization blocker终止，raw attempt仍为`failed`且`fit_performed=false`；顶层只能另记
     `negative_control_blocker_reproduced=true`，不得把预期失败改写为fit成功；
   - `harness_identity20_positive`：`autocycle_all_core:L2/801011.SI`、identity20、同一current-A5 authority和seeds，用于证明共享
     train-only runner、receipt和数值环境没有因REFIT-02改造而回归。它不充当801207的数值效果对照，也不参与D5。
3. **同sector因果闭合。** `treatment_19d`与`matched_identity20_negative`的full20 raw matrix hash、full20 preprocess hash、row/date
   identity、source manifest和除projection role外的D3参数必须逐项相等；唯一允许差异是projection receipt、active dimension count和
   由此产生的19D model parameters。任一其他差异使整个实验`inconclusive`。这条same-sector配对是D1机制归因权威；不得用801011
   跨sector score差替代。
4. **历史漂移receipt。** 对801207和801011分别保存历史v1 train input hashes、current-A5 train input hashes、有差异的exact paths和
   `historical_reference_status=drift_observed|equal`。该receipt只解释为什么REFIT-01不可继续，不形成migration acceptance、
   model acceptance或fallback。若current hash意外等于historical，仍按事实记录，不得改变三角色合同。
5. **精确预算。** 只有current-A5 exact-zero preflight通过后，每个fresh process才有24个terminal attempts：8个treatment真实HMM fit、8个same-sector negative control pre-fit
   terminal evidence、8个harness真实HMM fit；两个fresh processes合计48个attempts和32个真实HMM fits。不得把16个pre-fit negative
   attempts计为fit，不early stop、不扩大seed grid、不运行legacy/L1/其他L2 sector或5184/2096正式grid。preflight为not-applicable时
   固定预算为0 attempts/0 fits，不得为了消耗预算改选sector或feature。
6. **重复性。** 两个fresh Python process必须在已批准的固定单线程Conda `AIstock`数值环境运行。每个role、seed的input receipt、
   terminal attempt receipt、treatment/harness model parameter bytes、likelihood history与D3/D4 descriptive payload必须分别bitwise
   canonical相等；allclose仅用于mismatch诊断。negative control两次必须以同一底层initialization reason在fit前终止。
7. **状态机。** 顶层`status`只允许`not_applicable|diagnostic_complete|diagnostic_failed`。`diagnostic_complete`只表示48/48 attempts均有durable terminal evidence且两process完整回读，不表示
   48个fit成功、D4 accepted、D5 ready或模型可交付。`mechanism_assessment`固定为：
   - `constant_dimension_mechanism_not_applicable_current_profile_changed`：current-A5 authority与profile readback完整，但801207/index19
     不再满足REFIT-02 exact-zero资格；固定0 attempts/0 fits且D5 readiness=false；
   - `inconclusive`：任一authority、same-sector配对、attempt set、repeat、harness或negative-control blocker证据不闭合；
   - `constant_dimension_mechanism_rejected`：完整性闭合，但任一treatment仍在exact-zero eligibility、projection、preprocess、
     parameter shape或相同persistent-zero-variance stage失败；
   - `constant_dimension_effect_supported`：matched negative control 8/8按原blocker fail closed，harness 8/8越过runner/fit完整性，且
     treatment 8/8越过exact-zero/preprocess/projection/parameter-shape并完成fit。后续monitor/likelihood/covariance/train-structure
     failure仍逐项保留，不被窄机制结论覆盖。
8. **D4/D5/D6边界。** D3/D4只形成当前批准公式的descriptive receipts；active 19维任一D4 failure保持failure。只有treatment 8/8
   fit completed、D3/D4 evidence完整、两process bitwise相等且harness/negative controls闭合时，才可令
   `d5_compatibility_evidence_ready=true`；该boolean只允许进入`D1-D5-COMPAT-01`设计，不执行D5。validation/future utility、D6、
   selection、semantic mapping、model/READY均不得访问或写入。
9. **schema与错误语义。** 新写schema使用`hmm_risk_c008_b3_d1_controlled_refit_report_v4`；v1/v2/v3保持只读回放，不原地迁移。
   在既有typed reasons之外至少新增：
   `hmm_risk_model_inactive_dimension_current_authority_mismatch`、
   `hmm_risk_model_inactive_dimension_current_profile_not_applicable`、
   `hmm_risk_model_inactive_dimension_matched_input_mismatch`、
   `hmm_risk_model_inactive_dimension_negative_control_not_reproduced`、
   `hmm_risk_model_inactive_dimension_harness_control_failed`、
   `hmm_risk_model_inactive_dimension_historical_reference_invalid`。
   aggregate必须保留全部role/seed reasons；不得first-error吞错、把negative raw failure标成passed或把历史drift伪装成兼容。
10. **副作用与后续。** artifact继续repo外append-only、不可覆盖发布并canonical readback；固定
    `selection_performed=false/formal_model_set_acceptance_performed=false/hard_semantic_authority_changed=false/`
    `model_write_performed=false/ready_artifact_write_performed=false/database_write_performed=false/runtime_action_performed=false`。
    REFIT-02-A即使effect-supported也不实现mixed-dimension writer/parser/runtime，不改变D5分母，不生成candidate/READY。

**6.2.1 BUG-977 / REFIT-02-B current-A5 matched-fit 因果修订。** REFIT-02-A 的真实双 fresh-process 报告
`F:/Dev/AIstock_artifacts/hmm_risk/d1_refit02_20260804_6928949c_postbug975_v5_retry1/d1_controlled_refit.json`
已经完成 `48/48` attempts、`32/32` 旧合同真实 fits 和 bitwise repeat，canonical SHA-256 为
`a63347a2ac157d7422c7acd7dabead20e7c9cb05471c78424f812ce447cc7ab4`；19D treatment 与 20D positive
harness 均完成拟合，但 current-A5 matched 20D control 在初始化成功后被旧 fit-budget 合同强制停止，因而固定得到
`diagnostic_failed/inconclusive`。该结果不是 HMM failure，也不能支持或否定 inactive-dimension mechanism；历史 blocker 只能继续作为
immutable provenance reference，不能再充当 current-A5 matched control 的预期终态。

用户已批准按 BUG-977 修订为以下精确合同；本次批准覆盖源码、直接测试、文档与后续独立受控诊断，不授权 D5/D6、selection、
model/READY、数据库、runtime 或服务控制：

1. `matched_identity20_negative` 稳定 role id 为历史兼容保留，但 current writer 语义改为 matched current-A5 20D control；它与
   `treatment_19d` 共享同一 801207 full20 raw/preprocessed input、seeds `42..49`、D3 参数和数值环境，禁止 projection，并必须调用与
   treatment/harness 相同的 `fit_b3_preprocessed_train_only`。不得在 initialization success 后伪造 pre-fit failure，也不得用历史数组参与 fit。
2. 每个 fresh process 仍固定 24 terminal attempts，但真实 fit budget 改为最多 24：8 treatment + 8 matched control + 8 harness；两个
   fresh processes 合计 48 attempts、最多 48 fits。初始化前失败允许 under-budget，但必须保留 typed terminal evidence；不 early stop、
   不扩大 seed grid、不运行 legacy/L1/其他 L2 sector 或正式 grid。
3. 三类因果终态严格分离：matched 20D 8/8 在 current contract 完成 fit 时，结论为
   `constant_dimension_mechanism_rejected`，且 `d5_compatibility_evidence_ready=false`；matched 20D 8/8 仍以同一 initialization blocker
   fail closed、19D 8/8 完成 fit 且 harness 完整时，才允许 `constant_dimension_effect_supported`；seed 间混合、非 initialization blocker、
   harness/repeat/authority/matched-input 不闭合均为 `inconclusive`。历史 reference 的旧 failure 不参与 current outcome 判定。
4. current schema 固定为 attempt v5、process v6、report v6、fit-budget
   `hmm_risk_c008_b3_d1_refit02_fit_budget_v2`；投影与 HMM 数值算法未改变，algorithm version 保持
   `hmm_risk_c008_b3_d1_refit_02_a_v1`。report v4/v5、process v4/v5、attempt v3/v4 必须继续 immutable readback，禁止原地迁移。
5. current matched attempt 的 `negative_control_blocker_reproduced` 固定为 `null`；authoritative outcome 使用
   `matched_control_fit_completed|matched_control_failed` 及实际 fit/D3/D4 receipt。新增稳定 reason 至少覆盖
   `hmm_risk_model_inactive_dimension_matched_control_fit_completed` 与
   `hmm_risk_model_inactive_dimension_matched_control_inconclusive`，不得把 matched fit success 标为错误或幂等 negative success。
6. `d5_compatibility_evidence_ready` 只有在 mechanism=`constant_dimension_effect_supported`、19D D3/D4 evidence 完整、双 process bitwise
   相等且所有控制闭合时才可为 true；mechanism rejected/inconclusive 时必须 false。validation/future utility、D6、selection、semantic
   mapping、model/READY 均不得访问或写入；全部 no-access/no-write flags 继续逐 attempt/process/report 持久化。

**6.2.2 C-008-B3-D1-REFIT-03-COVARIANCE-DIAG-01 精确只读诊断合同（已批准、实现并执行；结果见 6.2.3）。** REFIT-02-B 已使用
producer `9aba27526c59ec2e16ff29d31c96373a79a449be` 在同一 current-A5 authority、seeds `42..49` 与固定单线程 Conda
`AIstock` 环境完成两次 fresh-process。报告
`F:/Dev/AIstock_artifacts/hmm_risk/d1_refit02b_20260804_9aba2752_v6/d1_controlled_refit.json` 的 canonical
SHA-256 为 `3a4de927b863969aa16d68a89f03ad2beb94afe9a4595522ad606d354b9536cf`，文件字节 SHA-256 为
`5f09bd593a70a2a642622b4e29aaebb68fe5a7611285731716678489b61165c6`。它闭合 `48/48` attempts、`48/48` 真实
fit 与双进程 bitwise equality；19D treatment、20D positive harness 均 `16/16 fit_completed`且descriptive covariance accepted，matched current-A5 20D control
则在两进程的八个 seed 中均以 `hmm_risk_model_covariance_invalid` 终止于 covariance stage。该失败不是原批准的
initialization blocker，也不是 matched fit completed，所以 REFIT-02-B 正确给出 `diagnostic_failed/inconclusive`、
`d5_compatibility_evidence_ready=false`。当前 v6 receipt 仅有 stage/reason，没有 raw covariance cell、shape 与 bit-pattern
证据，不能判断失败是否严格局限于 exact-zero inactive coordinate，亦不能据此改变 D4-02 或 D1-D5 业务合同。

用户于 2026-08-05 批准按以下已合入详细设计实施源码与定向测试，随后又独立授权真实48-fit只读诊断。当前状态为
`USER_APPROVED_SOURCE_MERGED_DIAGNOSTIC_EXECUTED_COMPLETE_INCONCLUSIVE`；该执行不包含 D5/D6、selection、model/READY、
数据库、runtime、依赖或服务控制：

1. **唯一目的与可证伪问题。** 诊断只回答 raw fitted covariance 在什么 exact coordinate、以何种数值类别失效，以及
   19D treatment、matched 20D control、20D positive harness 的差异是否与 inactive coordinate 一致。它不重新定义
   `covariance_valid`、不放宽 D4-02-A、不把诊断 pattern 写成模型 acceptance，也不决定 mixed-dimension D5 score 可比性。
2. **冻结输入与完整三角对照。** 继续绑定 REFIT-02-B 的 formal `e7992f87…39f`、blocker `10287e84…cffe8`、remediation
   `48157a42…bb58`、C-010-A5 `e7f7edc9…773d`、current-A5 role/input/preprocess/numeric-environment identities；保持
   `801207.SI` treatment/matched pair、既有 positive harness、seeds `42..49`、两个 fresh processes 与 fit-budget v2。
   每进程仍完整运行 8 treatment + 8 matched + 8 harness，共 `24 attempts/最多24 fits`；总计 `48 attempts/最多48 fits`。
   不得只跑失败的 matched role、复用 REFIT-02-B final parameters、early stop、扩 seed、换 sector、运行其他 L1/L2 或正式 grid。
3. **raw covariance 捕获位置与唯一权威。** 只在 GaussianHMM `fit()` 返回后、任何 raw shape/finite/positive/bounds 检查或异常转换前，
   复制 `GaussianHMM._covars_` 的内部 diagonal buffer；它是当前正式训练路径实际送入 `_b3_diag04_covariance_evidence()` 的唯一
   post-fit raw authority，期望 shape 严格为 `(3, feature_count)`。不得改读可能展开 diagonal matrix 的公开 `covars_`，不得在两者之间
   fallback，也不得把公开表示的 shape/hash 与内部 buffer 混为同一 evidence。capture 必须发生在 `np.asarray(..., dtype=float64)` 与现有
   covariance validator 之前；即使转换或 validator 随后失败，仍须尽可能保存原 buffer 的原始 dtype、shape、strides、memory framing
   与逐 cell bit pattern。权威 buffer 的 expected type/dtype/layout 固定为二维 `numpy.ndarray`、IEEE-754 float64、C-contiguous；若实际
   type、dtype、rank 或 layout 不符，只保存能够安全读取的 type/dtype/shape/strides/nbytes 与 logical-cell evidence，并以最具体 typed
   reason fail closed。非 ndarray 不读取任意对象内存，unsupported/object dtype 不生成伪 raw bytes，任何不适用的 frame/hash 字段必须为
   `null` 并附稳定 `evidence_unavailable_reason`；禁止先转换成 float64 后伪造原始 cell evidence。initialization covariance 与 post-fit
   raw covariance 分开记录；禁止为诊断执行 clip、floor、projection、填值、
   absolute-value、`nan_to_num`、重拟合或参数回写。正式 posterior、D4 acceptance 与现有 failure 行为保持不变。
4. **精确、可序列化的 cell evidence。** 新增 `hmm_risk_c008_b3_d1_covariance_evidence_v1`，至少保存：raw authority 固定值
   `gaussian_hmm_internal_diag_covars_v1`、expected/actual
   shape、dtype、endianness、C-order、state/feature count、feature order/hash、inactive mask/hash、sector-local `R_sj` 与批准的
   D4-02-A threshold/formula identity；每个 cell 保存 `state_index/feature_index/feature_name/is_inactive_coordinate`、IEEE-754
   64-bit semantic pattern hex（先按dtype byte order归一为同一uint64位序，不依赖host端序）、分类
   `finite_positive|positive_zero|negative_zero|finite_negative|nan|positive_infinity|negative_infinity`，
   finite 值另存 `float.hex()`。JSON 数值字段禁止 NaN/Infinity；非有限值只以枚举和 bit pattern 表示。`raw_covariance_payload_sha256`
   的 framing 唯一固定为：`uint64_be(header_length) || canonical_json_bytes(header) || cell_bits`；header 只含 schema、raw authority、
   actual dtype string、shape、strides、feature-order hash 与 inactive-mask hash，`cell_bits` 按 C logical order 依次写入每个 cell 的
   IEEE-754 semantic uint64 big-endian 8 bytes。只有 type/dtype/rank/C-contiguous 全部满足本节 authority 时才生成该 hash；其他情况
   `raw_covariance_payload_sha256=null`，另以 canonical failure-evidence receipt hash保护实际可读 metadata/cells。每态/每feature分类计数
   及mask/hash必须能从cell evidence重建。若仍为二维float64 C-contiguous但expected shape不匹配，保存实际
   dtype/shape/nbytes/frame hash与可枚举cell；每次`model.fit()`返回且shape正确的标准 evidence 严格为 `3 * feature_count` cells，因而完整三角色、
   8 seeds、2 processes 的最大 raw-cell 数为 `3 * (19 + 20 + 20) * 8 * 2 = 2832`。无法安全取得内部 raw buffer时诊断
   fail closed，不改读公开 `covars_`、不伪造空matrix。
5. **D4 derived evidence 的可计算性。** `R_sj`、`ν` 与 D4-02-A threshold/formula identity 来自 pre-fit authority，始终保存；
   `M_k/W_kj/C_expected/L/U/E` 只有 raw shape/finite/strict-positive 已通过、现有 full-sequence `score_samples()` 与 posterior mass audit
   实际完成后才允许计算。状态映射固定且不得由实现选择：raw shape/non-finite/non-positive 失败时
   `d4_derived_evidence_status=not_computable_raw_covariance_invalid`、正式 `covariance_status=failed`；`score_samples()` 抛错或 required
   posterior receipt 无法形成时 `not_computable_posterior_audit_unavailable`、正式 `covariance_status=insufficient_evidence`；posterior 已返回
   但 shape/non-finite/negative/row-sum/state-mass 违反 D4-02-A 时 `not_computable_posterior_audit_invalid`、正式
   `covariance_status=failed`。三种情况的 derived fields 全部为 `null` 并保存最具体 reason；不得代入默认 mass、初始化 covariance、
   clipped covariance 或其他 role 的 posterior 伪造 bounds。明确的 not-computable receipt 不阻止 raw-cell diagnostic completeness，
   但 `covariance_valid` 始终为 false，绝不构成 D4 acceptance。
6. **完整 partial-stage evidence。** covariance-stage failure 不能丢弃此前已完成的 initialization、monitor 与 likelihood 事实。
   future implementation 必须让 `B3TrainingStageError`（或同等单一路径）携带 immutable
   `hmm_risk_b3_training_stage_evidence_v1`：`fit_invoked/fit_returned/completed_stages`、initialization receipt、monitor history/receipt、
   likelihood receipt、raw covariance evidence 与 stage-specific cause evidence；attempt v6 即使 `core=None` 也从该 envelope 持久化，
   不得把这些字段重写为 `null`。post-fit covariance failure 缺任一已完成上游 stage receipt 时，诊断固定
   `evidence_incomplete/inconclusive`；不得因 exception 已分类便宣称 evidence complete。
7. **描述性 comparison，不新增 gate或state语义。** 对同一 seed 的三角色生成 pair receipt，机械比较 raw input、preprocess、projection、
   initialization、monitor/likelihood 与 covariance evidence。`state_index` 仅是单个 attempt 内的数值索引，不具有跨 role 或 semantic
   identity；pair comparison 只比较 feature-level invalid-coordinate set、逐 role aggregate 与完整性，禁止跨 role 按 state index 对齐、
   排序或调用 D6 semantic label。若未来确需 state-wise 因果比较，必须另有用户批准的 alignment contract，本诊断不得自行建立。
   允许的诊断标签仅为
   `inactive_coordinate_pattern_consistent|active_coordinate_failure_present|cross_role_failure_present|mixed_seed_pattern|evidence_incomplete`。
   `inactive_coordinate_pattern_consistent` 只表示：matched 20D 的 invalid cells 全部位于唯一inactive feature，19D treatment删除的正是
   该feature，positive harness相同20D coordinate非inactive且通过现有fit path；它仍不得推导
   `constant_dimension_effect_supported`、D4 accepted、D5 readiness、seed selection、feature deletion或阈值变化。
8. **完成与失败语义。** `diagnostic_complete` 要求两进程各24 terminal attempts、全部预定fit调用闭合、每个fit均有完整raw
   covariance evidence或明确的pre-covariance typed failure；对post-fit covariance failure还要求第6项完整stage envelope；三角色pair
   集合完整、authority/hash与canonical payload bitwise相等。
   任一raw capture缺失、writer不能表示非有限值、role/seed不完整、pair drift、进程不一致、bit-pattern/hash冲突或当前source漂移均为
   `diagnostic_failed/inconclusive`。HMM fit失败可与diagnostic完整并存；不得把model failure改写为diagnostic failure，也不得反向伪造fit success。
9. **schema与历史兼容。** current attempt/process/report分别使用 v6/v7/v7；fit-budget保持
   `hmm_risk_c008_b3_d1_refit02_fit_budget_v2`，HMM algorithm version保持不变。report v4/v5/v6、process v4/v5/v6、attempt
   v3/v4/v5继续immutable readback；禁止原地迁移或把旧null covariance补写成新evidence。新artifact写入显式repo-external、
   append-only路径，canonical writer/readback碰撞必须失败。
10. **稳定reason codes。** 至少覆盖
   `hmm_risk_model_covariance_raw_type_invalid`、`hmm_risk_model_covariance_raw_dtype_invalid`、
   `hmm_risk_model_covariance_raw_layout_invalid`、
   `hmm_risk_model_covariance_raw_shape_invalid`、`hmm_risk_model_covariance_raw_non_finite`、
   `hmm_risk_model_covariance_raw_non_positive`、`hmm_risk_model_covariance_raw_bounds_failed`、
   `hmm_risk_model_covariance_derived_evidence_not_computable`、`hmm_risk_model_training_stage_evidence_incomplete`、
   `hmm_risk_model_covariance_evidence_incomplete`、`hmm_risk_model_covariance_bitpattern_conflict`、
   `hmm_risk_model_inactive_dimension_covariance_pattern_mixed`与既有最具体D1/D3/D4 reason。aggregate保留全部role/seed reasons，
   primary reason不得吞掉raw类别或坐标证据。
11. **严格副作用边界。** attempt/process/report继续逐层固定
   `validation_accessed=false/future_utility_accessed=false/semantic_labelability_accessed=false/d6_status_accessed=false/`
   `selection_performed=false/formal_model_set_acceptance_performed=false/hard_semantic_authority_changed=false/`
   `model_write_performed=false/ready_artifact_write_performed=false/database_write_performed=false/runtime_action_performed=false`。
   只允许生产repo-external diagnostic JSON；不得写candidate/model/READY、数据库、配置或runtime。
12. **执行后决策边界。** 即使完整证据显示inactive-coordinate-only pattern，下一步也只能提交D1机制与D1-D5兼容性的精确设计选项供
    用户确认；不得直接实现mixed-dimension writer或重训2096/5184 grid。若证据包含active coordinate、cross-role、mixed seed或
    insufficient evidence，则继续blocked并针对最具体stage另立诊断，不得通过调阈值、删feature或选择成功seed收敛。

本次源码实现的直接测试覆盖：所有IEEE-754分类（含`+0.0/-0.0`与不同NaN payload）、内部`_covars_`与公开`covars_`不得混用、
非ndarray/非float64/错误shape/非C-contiguous strides的fail-closed evidence、跨byte-order semantic bit-pattern一致性、精确header-length/
header/cell-bits framing与hash正反例、finite `float.hex()` round-trip、禁止JSON NaN；raw-invalid、posterior-audit-unavailable与
posterior-audit-invalid三类必须分别断言`failed/insufficient_evidence/failed`、derived fields为null且不得伪造bounds；covariance exception
仍保留initialization/monitor/likelihood stage evidence、跨role state-index比较被拒绝、
inactive/active coordinate正反例、三角色pair drift、两fresh-process bitwise equality、48-fit上限；历史attempt v3/v4/v5与新attempt v6、
历史process/report v4/v5/v6与新process/report v7必须分别覆盖parser/writer/readback/collision，另覆盖evidence v1、all-reasons aggregate
与全部no-access/no-write flags。该测试只属于
`hmm_risk` primary module；没有shared contract变化，不增加其他模块测试。

2026-08-05 二次正式代码审核又补齐两项同一批准合同内的 fail-closed authority：第一，v6 attempt readback 必须从逐 cell
bit-pattern 重建 raw capture、C-order 坐标/strides/nbytes、classification aggregate、payload hash、D4 formula/acceptance 与 partial-stage
lineage；即使逐层重算 covariance/stage/attempt hash，任一内部关系漂移仍必须拒绝。第二，
`inactive_coordinate_pattern_consistent` 除 matched 20D 仅 inactive coordinate 失败外，还必须要求同 seed 的 19D treatment 与 20D positive
harness 均通过既有 likelihood/covariance/train-occupancy 路径；pre-covariance 或 D4 失败只能形成
`cross_role_failure_present`，不得伪造 inactive-only pattern。这些是 writer/readback 与既有三角对照语义的确定性修复，不新增 D4/D5/D6
门禁、人工审批或模型接受条件。第三次对抗性审核进一步固定 raw-authority/derived-status 双向关系：type、dtype、shape、layout、
non-finite 或 non-positive 任一 raw failure 都可作为显式 `not_computable_raw_covariance_invalid/failed` 证据持久化并通过 readback；反之，
存在任一 raw failure 时不得声称 D4 derived status 已 `computed`。这只修复 fail-closed evidence 的可读回与防伪关系，不改变验收语义。

**6.2.3 C-008-B3-D1-REFIT-03-RESULT-AUDIT-01 执行结果、D1边界与D5兼容决策包。** 2026-08-06 使用
producer `b474170fd58a466959e595ce7d245bae7da88ab8`、冻结 bundle
`F:/Dev/AIstock_artifacts/hmm_risk/d1_refit03_current_a5_20260806_b474170f_v9/d1-controlled-refit.frozen-input.json`
和同目录 report `d1_controlled_refit.json` 完成两次 fresh-process 受控诊断。bundle canonical SHA-256 为
`7ac88ef92d717c25734fbff0fc9322c2d3393da4d3f4f38d86287b9bf21146e2`；report canonical SHA-256 为
`7e8a17556aaf610de4bd7b2449cae5f224d032ce8acafdcbcb9a594bbf6276b9`，文件字节 SHA-256 为
`b93b7fc91c251b4908e019b4208ac8f1a1af0feb78a379da401fa2fc36ae0f5a`，receipt SHA-256 为
`51917ff56bfe4661f679dfd4aff89a8374fd4d8b852dd7487cdbcea61291ce52`。两进程 comparable payload SHA-256 均为
`53574f62608f6860e6ee59b7b31aafc7772a16d7637fe4a119e0128eda50888f`，`canonical_payload_bitwise_equal=true`；
固定环境为 CPython 3.12.12、NumPy 2.4.0、SciPy 1.16.3、scikit-learn 1.8.0、hmmlearn 0.3.3、
threadpoolctl 3.6.0 且有效线程池为1。该相等性只证明同一host和固定环境内的重复性，不外推跨host/BLAS版本。

执行事实与严格语义如下：

1. 两个进程各完整运行24个attempt/fit，总计`48/48`，未early stop、未扩seed、未换sector；
   `treatment_19d`与`harness_identity20_positive`分别`16/16`通过既有fit/likelihood/covariance/train结构路径。
2. `matched_identity20_negative`为`16/16` covariance failure。每进程的seeds 45/47/48只在feature index `[19]`
   出现non-positive raw covariance，标签为`inactive_coordinate_pattern_consistent`；seeds 42/43/44/46/49的invalid set为
   `[0..19]`并含non-finite，seeds 43/46另含non-positive，标签为
   `active_coordinate_failure_present`与`cross_role_failure_present`。
3. aggregate必须保持`diagnostic_complete`与model failure可并存；最终
   `covariance_pattern_assessment=mixed_seed_pattern`、`mechanism_assessment=inconclusive`、
   `d5_compatibility_evidence_ready=false`。19D treatment完整通过证明D1-B projection在该冻结输入上可拟合，
   但active-coordinate failures否定“全部matched失败严格由唯一inactive coordinate解释”；不得写成
   `constant_dimension_effect_supported`或“常量维已是唯一根因”。
4. report明确`d3_d4_descriptive_contracts_applied=true`，但
   `formal_model_set_acceptance_performed=false`。validation、future utility、semantic labelability、D6、selection、
   model/READY、数据库与runtime访问/写入均为false；因此REFIT-03不改变D3/D4正式验收状态，也不构成D5输入。
5. `C-008-B3-D1-POST-REFIT03-A=RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_LOCAL_REVIEWED`：用户已把D1-B固化为
   **level-local engineering robustness contract**。证据边界严格限定为：在本次冻结输入、已批准参数、seeds 42..49与固定单线程
   数值环境下，保留exact inactive coordinate的matched 20D为16/16 covariance failure，而删除该coordinate的19D为16/16
   `fit_completed`且descriptive `covariance_status=accepted`；不得外推为GaussianHMM普遍不能处理常量维，
   也不得再宣称D1-B已证明唯一统计因果机制。该批准只激活本节固定projection工程合同，不改变历史诊断的`inconclusive`结论。

`C-008-B3-REMEDIATION-D1-D5-COMPAT-01`的精确选项如下；用户已批准方案A，其他方案仅保留决策审计记录：

- **A（已批准并完成源码实现，`RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_LOCAL_REVIEWED_NOT_EXECUTED`）— effective-dimension train-only score。** 继承已批准D5-01-B的
  `L_final/(N*d)`归一化结构，并将该`d`从level-wide 20精确化为entry effective dimension `d_i`。
  只有exact proposed identity `autocycle_all_core:L2/801207.SI`使用`d_i=19`，
  其他entry使用`d_i=20`。同一level仍须131/131 sectors全部通过D3/D4才eligible，D5继续在全部131个score上按
  `(minimum, median, math.fsum mean)` tolerance-aware lexicographic maximize；禁止排除sector、per-sector seed stitching、
  validation/future utility输入或selection后refit。artifact/receipt必须持久化每entry的active feature list/mask/hash、`d_i`、
  denominator与score hash。风险是19D/20D的per-dimension likelihood尺度仍可能不同；方案A一旦由用户批准，`d_i`就是明确的
  业务公式，不再设置经验性“可比性阈值”或额外研究门禁。实现PR只执行确定性直接测试：19D/20D正反例逐项重算
  `L_final/(N_i*d_i)`；active feature list/mask/hash与`d_i`不一致必须fail closed；writer/parser/readback/canonical hash必须一致；
  131-entry aggregate不得漏sector；测试固定`validation_accessed=false/future_utility_accessed=false/selection_performed=false`。
  方案A批准后不再使用`hmm_risk_model_inactive_dimension_d5_comparability_unresolved`阻断；dimension identity/hash/`d_i`冲突使用
  `hmm_risk_model_inactive_dimension_contract_invalid`，score非有限使用`hmm_risk_model_selection_score_non_finite`，131-entry缺失使用
  `hmm_risk_model_selection_level_incomplete`，两fresh-process receipt不一致使用`hmm_risk_model_selection_repeat_mismatch`；不得压缩为generic failure。
  这些测试只证明实现忠实于获批公式，不宣称从一组样本统计证明跨维度普遍可比，也不阻断后续2096-fit正式制备之外的研究方向。
- **B（不推荐）— 固定20维分母。** 19D entry仍除以20，保持历史名义分母但系统性惩罚19D，不能证明跨sector可比；
  只有用户明确选择后才可采用。
- **C（`PROPOSED_NOT_RECOMMENDED_PENDING_USER_DECISION`）— 从D5 aggregate排除801207。** 即使保留其D3/D4 eligibility，也会让D5看不到
  weakest sector，形成未经批准的子集验收；不满足完整131-sector合同。
- **D（`PROPOSED_NOT_RECOMMENDED_PENDING_USER_DECISION`）— 全level改用within-sector rank/z-score。** 虽消除直接维度尺度，但会改变全部131个
  sector的D5算法和历史决策语义，属于没有当前证据支持的广泛模型迁移；本次不替用户淘汰该方向。

执行顺序因此修正为：`C-008-B3-D1-POST-REFIT03-A`与
`C-008-B3-REMEDIATION-D1-D5-COMPAT-01-A`已获用户批准；mixed-dimension artifact/parser、train-only score receipt及上述
确定性直接测试已完成源码实现与本模块审核，但尚未运行正式grid。下一步另行授权后运行受影响的`autocycle_all_core:L2` 2096 fits并按获批D5/D6合同fail closed。当前没有证据再次运行
相同48-fit诊断或扩大seed grid；后者会引入post-hoc
selection风险。D1-B未修改共享KMeans/EM/covariance合同，所以没有证据重跑完整5184 grid。任一步失败均保持两family blocked、READY=0。

**6.3 REFIT-02-A/B实现与验收切片。** 用户已批准并完成 REFIT-02-A 独立源码任务；BUG-977 在同一受控 runner 上修订 matched-fit
因果合同，不建设通用实验框架、scheduler、API/UI 或 runtime。直接测试至少覆盖 48-attempt/48-fit 上限、under-budget 初始化失败、
matched fit success 导致 mechanism rejected、matched initialization blocker + treatment fit 导致 effect supported、D5 readiness 关系、
双 fresh-process bitwise equality、v4/v5 历史 readback、v6 writer/readback、aggregate all-reasons 及全部 no-access/no-write flags。

原 REFIT-02-A 实现修改仅限现有D1 runner、制备入口及其直接测试：增加三角色
current-A5 snapshot authority、same-sector matched receipt、v4 process/report和历史reference drift receipt；不得建设通用实验框架、
scheduler、API/UI或runtime。直接测试至少覆盖：同sector raw/preprocess相等、唯一projection差异、negative raw failure不伪装成功、
harness失败导致inconclusive、48 attempts/32 fits预算、两个fresh-process bitwise equality、v1-v3 readback、v4 writer/readback、
aggregate all-reasons以及全部no-access/no-write flags。源码审核通过后仍需用户单独授权真实32-fit执行。

**7. 稳定失败语义。** 至少包括：

- `hmm_risk_model_inactive_dimension_authority_mismatch`；
- `hmm_risk_model_inactive_dimension_not_exact_zero`；
- `hmm_risk_model_inactive_dimension_preprocess_mismatch`；
- `hmm_risk_model_inactive_dimension_projection_invalid`；
- `hmm_risk_model_inactive_dimension_contract_invalid`；
- `hmm_risk_model_inactive_dimension_parameter_shape_invalid`；
- `hmm_risk_model_inactive_dimension_control_drift`；
- `hmm_risk_model_inactive_dimension_repeat_mismatch`；
- `hmm_risk_model_inactive_dimension_attempt_set_incomplete`；
- `hmm_risk_inactive_dimension_observation_receipt_conflict`；
- `hmm_risk_model_inactive_dimension_d5_comparability_unresolved`（批准前历史artifact只读reason；新实现不再产生）；
- 既有最具体的initialization/monitor/likelihood/covariance/train-structure reason codes。

不得把exact-zero写成普通missing data，不得把projection失败压缩成generic fit failure，也不得把control drift或repeat mismatch降级为warning。

**8. 验证计划与可合入边界。** 未来D1-B源码PR至少验证：

- exact allowlist、raw exact-zero、preprocessed approved-transform exact replay、preprocessed deterministic nonzero constant、`-0.0`、raw nonzero constant、near-zero、non-finite、source/hash drift正反例；
- full20 preprocess先于projection；active order/mask/hash固定；no-inactive control为identity20；
- KMeans/HMM只接收19维，model参数shape为3×19，inactive维无伪参数；
- parser/replay对unknown version、mask/shape/preprocess drift fail closed；runtime不动态激活inactive维；
- 131-entry mixed-dimension level manifest、dimension histogram、ordered entry-list hash、writer/parser same-authority与禁止padding；
- runtime inactive-observation receipt的dedupe/hash/replay、同key不同hash conflict与InputManifest/daily-generation lineage；
- control payload bitwise相等、双fresh-process bitwise相等；32-fit budget与no-early-stop；
- `diagnostic_complete`与`mechanism_assessment`独立；inconclusive/rejected/effect-supported及
  `d5_compatibility_evidence_ready`全部边界与aggregate reason正反例；
- D4各状态独立，0 D5/D6/validation/future utility/model/READY/DB/runtime write；
- changed-file ownership、`hmm_risk_backend` required plan、F2 validator、scope、Ruff/compile与`git diff --check`。

D1-B P1与P5 mixed-dimension源码均已实现；源码状态本身不授权formal grid/selection/model/READY。历史controlled-refit及后续
REFIT-03证据保持各自原始状态；正式2096-fit执行仍须单独授权。该限制是训练副作用与交付状态的准确边界，不是新增runtime人工审批。

**9. Evidence-first执行优先级与反过度工程边界。** 完整设计合同不删减，但实现按最短可证伪路径逐步推进；每个slice必须完整实现其
声明范围、保留typed failure与durable evidence，且不得把局部机制、受控训练或单level结果宣称为family/Phase 2完成。

| 优先级 | 类型 | 任务 | 完成语义与禁止事项 |
|---|---|---|---|
| P0 | 历史合同收敛 | REFIT-01固定为`VERIFIED_ATTEMPTED_INCONCLUSIVE_TRAIN_CORE_DRIFT_ZERO_FITS`，历史payload保持只读，不再作为current-A5成功条件 | 本文完成；不忽略train observation drift，不增加模型完成度 |
| P1 | current-A5实验设计 | 定义REFIT-02-A三角色、same-sector因果配对、48 attempts/32 fits、v4 schema与状态机 | 用户已批准且设计审核完成；不自动授权真实fit |
| P2 | 最小源码修订 | 扩展现有D1 runner/入口/直接测试，支持current-A5 snapshot、matched negative、harness与历史drift receipt | BUG-977 source/PR/close-sync已合入并清理；不建设通用实验平台、scheduler、API/UI、runtime或mixed-dimension READY writer |
| P3 | 真实受控训练 | REFIT-02-B已在两fresh processes完成48 attempts/48真实fits | 19D treatment与20D harness通过、matched 20D全部covariance failure；mechanism保持inconclusive，固定0 D5/D6/validation/model/READY |
| P3A | covariance evidence执行 | `C-008-B3-D1-REFIT-03-COVARIANCE-DIAG-01`完成raw cell/bit-pattern/三角色pair双fresh-process `48/48` fits | canonical `7e8a1755…76b9`、payload `53574f62…088f` bitwise equal；结论为`mixed_seed_pattern/inconclusive`、D5 readiness=false，不得写成D4/D5 gate |
| P4 | 模型合同决策 | `C-008-B3-D1-POST-REFIT03-A`与`C-008-B3-REMEDIATION-D1-D5-COMPAT-01-A` | `RESOLVED_USER_APPROVED`；采用level-local D1-B与effective-dimension公式，禁止排除sector或validation-driven选择 |
| P5 | 必要模型制备管线 | 完整mixed-dimension level entry、writer/parser same-authority、manifest/hash与train-only D5 score receipt，并执行公式/identity/131-entry完整性的确定性直接测试 | `SOURCE_IMPLEMENTED_LOCAL_REVIEWED`；直接测试只验证获批公式，不建立经验性可比阈值；未运行2096 fits、D5/D6或生成model/READY |
| P6 | 正式真实训练 | P5实现验证通过且依赖仍严格level-local后，重训受影响的`autocycle_all_core:L2`：131 sectors × 8 seeds × 2 fresh processes = 2096 fits | 按获批D5/D6合同fail closed；只有共享KMeans/EM/covariance或公共算法合同变化时才有证据重跑完整5184，不得扩大seed |
| P7 | 正式模型验收 | 对新`autocycle_all_core:L2`执行D5 train-only selection与唯一selected identity的D6 hard validation | D6失败不得返回D5换seed；不得用单level通过推导family READY |
| P8 | 其余模型阻塞修复 | 分别处理autocycle L1 `801030.SI`、legacy L2 `801019.SI/801207.SI/801782.SI`及独立likelihood/covariance机制 | 一次只改一个可归因机制；禁止把多类根因塞入“大重构”或放宽统一阈值 |
| P9 | 完整模型交付 | 四个family/level全部通过，闭合L1 31/31、L2 131/131、parser/hash/causal replay与两family READY | 任一层blocked时READY=0；不得交付autocycle-only、partial family或selected-only artifact |
| P10 | 产品功能 | 在真实稳定model-set之上依次实现generator/job/repository，再实现真实API/UI，最后评估scheduler/runtime | 不先建设训练平台、通用插件框架、额外DDL、mock UI、静态矩阵或runtime人工审批 |

历史P1最小源码已完成且不是“过渡工程”；当前P2只做REFIT-02-A runner最小修订，P3才执行真实训练。P5/P10必须等证据或上游artifact，提前实施会形成
无真实模型支撑的基础设施。按阶段推进不删除D1 §4/§4.1/§4.2的完整最终合同，只改变何时实现；对应阶段未到时继续保留为
mandatory deferred contract，而不是optional或简化项。

### 4.3.4 C-011 产品对齐的建模范式与分层验收（方向已批准，精确合同待闭合）

2026-08-14 用户基于父蓝图v2.25批准本节方向。该批准改变的是后续产品验收单位与modeling-paradigm决策边界，不反写
C-008/B3历史artifact、selection、D6或READY；旧B3的两个family、四level、D3-D6与blocked状态继续按原version解释。
新实现只能在本节精确合同经后续F2审核和用户确认后开始。

1. **产品验收单位**：主要产品单位为`trade_date × canonical L1/L2 cross-section`。模型输出必须使用t-1/PIT事实，证明其在未见
   日期上区分相对走强、走弱与风险；逐sector是否在一个固定窗口访问全部三态不再作为全局产品成功的充分或必要条件。
   D6类结构/utility证据继续决定该sector的semantic availability，不能删除或改写为warning success。
2. **四层验收**：
   - `numeric_model_status`：fit、MAP/raw likelihood、covariance、transition、posterior与因果identity；复用适用的D3/D4
     fail-closed原则，但新estimator必须用新version定义等价数值合同；
   - `sector_semantic_availability_status`：逐sector为`available/insufficient_structure/insufficient_evidence/failed`，保存
     映射authority与typed reason；未见state不得由index、neutral、soft mass或另一sector补标签；
   - `cross_section_product_status`：只由预注册walk-forward与最终untouched holdout上的产品指标决定；likelihood、occupancy、
     fit count或bitwise receipt不能替代；
   - `coverage_status`：保存canonical denominator、available/unavailable sector、L1/L2/行业/规模/流动性分布与偏差。缺失分母、
     代表性证据或exact contract时为`insufficient_evidence`，不得默认available。
3. **三个顶层状态**：
   - `FULL_READY`：选定canonical model version覆盖全部批准L1/L2范围，四层合同全部通过；
   - `COVERAGE_AVAILABLE`：精确coverage/代表性合同与产品指标均通过，只有available sector输出状态，其余逐项返回typed reason；
   - `NOT_AVAILABLE`：产品指标、coverage代表性、数值安全或必要证据不满足。不能因time-box到期或spike失败自动改成
     `COVERAGE_AVAILABLE`。后者不是FULL_READY别名，API/UI必须同时展示状态、分子、分母、偏差和不可用清单。
4. **restart selection边界**：允许在精确设计中提出per-sector restart，但schedule、train-only eligibility/score/tie-break必须在读取
   validation前冻结；validation/future utility/D6/holdout access固定false。D6或holdout失败后不得换seed、refit、扩大grid或回退旧模型。
   这与事后per-sector stitching不同，但在版本化公式、receipt与反例测试批准前仍非active D5合同。
5. **family角色**：legacy与autocycle是两种历史regime定义和研究候选，不再因历史存在自动构成新产品共同READY合取。精确协议必须
   用相同冻结数据、同一product metric与untouched holdout决定`canonical/secondary_research/rejected_for_product`角色；不得由实现、
   单一likelihood或validation局部表现淘汰family。若二者互补，组合公式、identity和增量贡献必须形成新model version并另行批准。
6. **首要结构候选**：优先评估`market_regime + sector_relative_strength`，产品至少区分market regime、sector relative score/state、
   risk alert与availability。market estimator可为HMM或jump estimator；sector relative部分必须低维、可解释、因果，并保存相对market
   benchmark与feature identity。该方向会改变历史`state_origin=direct_hmm`，因此只能用新origin/version，不能静默复用旧parser/hash。
7. **K与semantic边界**：现有child artifact只有K=3 fits，只能产生
   `K3_STRUCTURE_COLLAPSE_SUGGESTS_K2_HYPOTHESIS`，不能选择K=2。若P2-2聚合支持该假设，单一spike必须包含预注册K=2对照fit和相同
   holdout；K可变时UI仍需稳定产品语义，不能把不同hidden-state index直接当作trending/neutral/fading。
8. **walk-forward隔离**：时间顺序固定为train/selection folds、semantic calibration、最终untouched holdout；scaler、feature选择、
   restart/model/family选择只能读对应训练/选择窗口。final holdout只执行一次冻结模型评估，不能回流选择。重叠future-return horizon必须
   使用精确purge/embargo合同；精确fold、日期、horizon和hash待后续设计批准。
9. **产品指标候选**：至少覆盖日度横截面Rank IC、trending-fading或top-bottom前向超额收益spread、风险预警对已实现回撤事件的
   precision/recall、样本量、分阶段稳定性、coverage及其偏差。指标的signal编码、5/10/20日horizon、benchmark、事件标签、经济/统计阈值、
   多重试验处理与聚合公式均为`PENDING_EXACT_CONTRACT`；本节不把任何候选数值激活为新门禁。
10. **单一spike决策树**：首先零refit、流式聚合现有两个TRANSITION-DWELL-B child artifact，输出D3/D4/early/late主导失败、跨窗口
    持续sector与coverage偏差。transition/run/dwell主导时可选jump estimator；covariance/feature主导时选3-5个train-only可解释特征与
    t-emission；固定少数sector跨seed/窗口失败时选简单shared-prior/empirical-Bayes。市场regime+相对强弱优先作为部署结构。
    每轮只能批准一个spike，禁止A/B/C并行、自动进入HSMM/HDP-HMM或构建通用模型平台。
11. **spike范围与收敛**：必须覆盖全部已知11个D6失败sector、legacy缺态例`801780.SI`和预注册匹配成功对照；若评估横截面产品指标，
    必须使用完整canonical daily cross-section而非只用代表性sector宣称产品成功。spike失败后停止该方向并提交NOT_AVAILABLE或下一次用户决策，
    不自动开启新诊断，也不无条件承诺coverage成功。
12. **历史与完成度**：B3 P6/D5/D6、TRAIN-STABILITY和TRANSITION-DWELL-B继续是append-only决策证据，不能按新合同追认。
    C-011方向批准、文档完成、聚合或spike均不增加F-011/F-013产品验收计数；当前canonical模型、FULL_READY、COVERAGE_AVAILABLE、
    generator、API/UI、database/runtime write均为0/未执行。

#### 4.3.4.1 P2-2 既有 child evidence 单遍聚合合同

P2-2 不建立通用 evidence 平台，也不重新执行 HMM。唯一输入为已完成且 parent receipt 精确绑定的
`TRANSITION-DWELL-B` parent 与两个 fresh-process child；实现必须逐 child 单遍读取，核验 parent report、child canonical hash、
entry payload hash 与 profile payload hash，只抽取 `aggregate_receipt.l2_domain_receipts`、`level_repeat.entries` 和 `profiles`，并跳过
`models` 大 payload。禁止整 child 复制、整对象 JSON load、重新 materialize observation、refit、D5/D6、seed/family selection 或
model/READY 写入。

聚合 schema 固定为 `hmm_risk_phase2_p2_2_evidence_aggregation_v1`，至少包含：

1. 每个 `seed × sector` 的 D3/D4 status、typed rejected stage/reason、early/late structure status/reason、主导失败类别和跨窗口持续失败标志；
2. 主导 stage 顺序仅用于诊断归类：`fit → likelihood → covariance → train_occupancy → model_entry`；不存在 D3/D4 rejection 时，再按
   `posterior → transition/run/dwell → occupancy/coverage → other_structure` 归类，不改变任何正式 acceptance；
3. 仅当 D3/D4 accepted 且同一 K=3 entry 的 early/late 均 structural-unobserved 时，允许标记
   `K3_STRUCTURE_COLLAPSE_SUGGESTS_K2_HYPOTHESIS`。该标记必须同时写入 `k2_fit_performed=false`、`k_selected=false`；不得据此选择 K=2；
4. coverage denominator 固定为 source child 的 131 个 canonical L2 sector。size/liquidity 仅使用冻结 train receipts 的逐 sector
   `median(price_expected_weight)` 与 `median(moneyflow_contributor_amount)` 做诊断 quintile；不得访问 validation/future utility；
5. child 不携带 L2→L1 parent/industry authority，因此 P2-2 对 L1/industry bias 必须写
   `insufficient_evidence/hmm_risk_p2_2_child_artifact_l1_mapping_unavailable`，不得按代码前缀猜测、访问数据库补写或隐藏该缺口。P2-3
   exact coverage contract 必须另行绑定 canonical hierarchy mapping 后才能形成 FULL_READY/COVERAGE_AVAILABLE 代表性结论；
6. 两 fresh process 的 compact records 必须 canonical 相同；coverage 必须按 `(sector_code,trade_date)` 唯一键闭合，并证明 131 个 sector 共享同一冻结日期集合，输出保存日期起止与 canonical date-set hash；输出使用并发首次写入也不得覆盖既有结果的 collision-safe append-only write。任何 hash、denominator、array closure、
   finite proxy 或 process identity 不一致均 fail closed，不产生 `diagnostic_complete`；
7. 输出必须显式保留 `refit/selection/family_selection/D5/D6/model/READY/database/runtime=false` 和
   `formal_product_thresholds_applied=false`。聚合完成只允许进入 P2-3 精确设计，不增加 F-011/F-013 完成计数。

2026-08-14 首次正式执行结果：parent=`transition_dwell_b_postbug1068_20260813_29417ceb/transition_dwell_b.json`，两 child
canonical/payload closure一致；聚合输出 schema=`hmm_risk_phase2_p2_2_evidence_aggregation_v1`、receipt SHA-256=
`8aed2bc4c22037120fe6757e75fed8dc7407d8dcd12b4275f6a08e0c29194698`、`1048/1048` seed×sector records。主导类别为
`accepted=879`、`transition_run_dwell=149`、`occupancy_coverage=17`、`train_occupancy=3`；10个sector至少一个seed跨early/late
持续失败，其中`801155.SI`为8/8、`801038.SI`为7/8、`801141.SI`为6/8。9个D3/D4 accepted sector只形成
`K3_STRUCTURE_COLLAPSE_SUGGESTS_K2_HYPOTHESIS`，未执行K=2 fit或K选择。size/liquidity quintile诊断未显示失败只集中在最低
quintile。冻结domain分母为每sector相同的601日集合（`2022-01-04..2024-06-28`，date-set SHA-256=`b48fb5e911295d1c16920178b6ea48285c5890455aeaa31ad03ef7e11841f715`），`78362`个valid与`369`个typed invalid receipts按唯一`(sector_code,trade_date)`闭合`131×601=78731`；仅
`801114.SI`与`801952.SI`同时出现在10个persistent sector和32个存在invalid date的sector中，不能把主导结构失败归因于domain缺口。
L1 parent/industry mapping仍为typed insufficient。所有refit/selection/family selection/D5/D6/model/READY/DB/runtime
flags均为false。该证据把P2-3的唯一spike建议收敛为`market_regime + sector_relative_strength`部署结构，并优先评估jump estimator；
精确estimator、K、fold、metric、coverage与family/restart合同仍为`PENDING_USER_APPROVAL`，本次不得实施。

#### 4.3.4.2 P2-3 市场 regime + sector relative jump 单一 spike 精确候选合同

本节是用户已批准的 `C-011-P2-3-A` 完整精确合同。D1～D6 v1源码已经合入；首次正式spike在market `144/144` fits后因
fold-2零正例而按原D3合同停止。`C-011-P2-3-D3-MARKET-ZERO-EVENT-A`现已获用户批准，并以contract/request/report/market-fold
schema v2实施；旧v1 request/report只保留历史证据，不得被新执行grandfather或静默升级。本修订不执行fit、selection、
product acceptance、model、`FULL_READY`、`COVERAGE_AVAILABLE`、数据库或runtime。P2-2 已证明主导失败为
`transition_run_dwell=149/1048`，而冻结 domain 以唯一 `(sector_code,trade_date)` 闭合 131×601；因此本候选只评估
jump estimator，不并行 t-emission、shared-prior、per-sector HMM restart 或第二个模型范式。

##### D1. 模型结构与唯一 identity

候选 identity 为 `hmm_risk_market_relative_jump_spike_v1`，只包含三个直接组件：

1. 一个 L2 横截面市场向量上的 `K_market=2` jump model，输出 `risk_on/risk_off`；
2. 一个 pooled L1 sector-relative `K_sector=3` jump model；
3. 一个 pooled L2 sector-relative `K_sector=3` jump model。

sector-relative centers 在同一 level 内由所有 `sector×date` 共同拟合，但每个 sector 保持独立因果状态路径和 jump penalty；
禁止 per-sector centers、per-sector seed stitching、L1/L2 center 复用或按 validation/holdout 为失败 sector 换模型。该结构是一个
spike，不是三个并行范式。历史 legacy/autocycle 继续保留研究事实；本 spike 只使用
`autocycle_all_core` 已批准 observation 中的显式特征子集，legacy 不在本 spike 重拟合，也不因此被永久淘汰或改写历史角色。
新输出必须使用 `state_origin=market_relative_jump_v1`，禁止复用 `direct_hmm` parser、hash、READY 或旧 semantic receipt。
`K_sector=3` 是为了直接产出已批准的 `fading/neutral/trending` 产品语义，不是由P2-2的K2描述性假设推导出的统计结论；
若 pooled K=3 无法通过本合同，必须报告本spike失败，不能在同一执行中改K、合并状态或把K2结果冒充三态产品。

##### D2. 输入、特征、预处理与语义

全部输入继续来自 C-007/C-009/C-010 已批准的 PIT、t-1、31/131 canonical sector 和 formula-v2 observation authority；
不新增表、不访问未来 observation、不删除证券/sector、不填 0/前值/neutral，也不重新物化完整历史输入。

市场向量只取 131 个 L2 的以下五项现有特征在每个交易日的横截面 median：

`daily_return`、`volatility_Nd`、`net_mf_ratio`、`sf_breadth_5d`、`sf_dispersion_5d_neg`。

L1/L2 sector-relative 向量分别使用：

`excess_return_Nd`、`net_mf_ratio`、`elg_net_mf_ratio`、`sf_excess_breadth_5d`、
`sf_turnover_pctile_120d_neg`。

每个 fold 只在其 train 部分为每个 component/level 分别拟合并冻结 **level-global** preprocess：对同一 component/level 的全部 train
`sector×date` 行，每个 feature 只形成一组 1%/99% winsor threshold 与一组 mean/std；禁止逐 sector scaler，
否则会消除产品要比较的横截面强弱。std 非有限或 `<=1e-12` 时该 component fail closed。relative component
在 level-global winsor/z-score 后，再减去同 level/同日可用 sector 的横截面 median；market component 使用
L2-global transformed raw median，不读取 relative residual。市场 median 至少要求 L2 `118/131`，relative median
至少要求 L1 `28/31`、L2 `118/131`；不足时该 level/date 为 typed unavailable，不从剩余少量 sector 偷换
denominator。所有 threshold/mean/std、train rows、level、feature order 与 hash 必须进入 preprocess receipt。

preprocess数值合同固定如下：输入先按`trade_date,sector_code` canonical排序并转为little-endian float64；一行只有在五项feature与
source eligibility全部有效、有限时才是valid row，任一feature缺失/非有限使整行typed unavailable，禁止逐feature补值。
每项winsor阈值使用valid train值升序后的NumPy `quantile(method="linear")`；clip后mean使用`math.fsum(x)/N`，population
variance使用`math.fsum((x-mean)^2)/N`、std为其平方根（ddof=0）。daily cross-section median使用升序中位数，偶数项取中间两值
算术平均。上述算法、dtype、row count、unavailable identities、quantile method与实现版本都进入receipt；禁止依赖库默认
quantile/std行为。

market semantic score 固定为标准化 centroid 的
`daily_return - volatility_Nd`；较高 state=`risk_on`、较低 state=`risk_off`。sector semantic 只按 centroid 的
`excess_return_Nd` 严格升序映射 `fading < neutral < trending`。相邻 score 差 `<=1e-8`、non-finite 或 center 缺失均
fail closed；hidden-state index 不具有业务语义，禁止 index/neutral fallback。jump model 输出 hard causal state；本 spike 不声明
HMM posterior，也不伪造概率。置信度候选仅允许使用“次优 causal path cost - 最优 causal path cost”的非负 margin，margin
非有限时该点 unavailable。
semantic mapping 只由development train centroid按上述score冻结，不另建calibration split，也不读取fold validation或holdout来
重排label；fold/holdout只评估冻结mapping。任何mapping失败均不得通过换seed、换λ或按hidden-state index修复。

##### D3. Jump objective、初始化、选择与因果推断

market objective：

`J_m = Σ_t ||x_t-μ[s_t]||² + λ_m Σ_t 1[s_t != s_(t-1)]`。

level-local sector objective：

`J_l = Σ_g Σ_t ||r[g,t]-ν[z[g,t]]||² + λ_l Σ_g Σ_t 1[z[g,t] != z[g,t-1]]`。

gap 前后不得计算 transition；gap 后 arrival costs 全部重置为 0。参数 step 使用当前 assignment 的有限均值；任一 state
无样本即 candidate failed。path step 必须使用 `O(T*K²)` dynamic programming 求该参数下的全局最小路径。交替过程最多
200 次；`J_new > J_prev + 1e-10*max(1,abs(J_prev))` 为 numeric failure；下降量不超过同一 envelope 且 path 未变化才为
converged。所有 objective、iteration、center、path/count/run/jump 和 failure reason 进入 compact receipt。

KMeans初始化完整参数固定为`n_clusters=K, init="k-means++", n_init=1, random_state=restart_seed, max_iter=300,
tol=1e-4, algorithm="lloyd", copy_x=True`，输入保持上述canonical row order。DP在每个独立segment首日将K个arrival cost全部置0；
fold validation与未来P2-4 holdout首日也必须从全0 cost开始，不携带train terminal path/cost。每一步cost完全相同时按较小predecessor
state index、再按较小current state index确定性取值；该index只用于数值tie-break，不产生semantic。达到200次仍未满足converged
条件时以`hmm_risk_jump_max_iterations_reached`失败，禁止把last iterate当成功。

该 objective、交替参数/path优化和 `O(T*K²)` DP 来源于 Bemporad、Breschi、Piga、Boyd 的
*Fitting jump models*（Automatica 96, 2018, DOI `10.1016/j.automatica.2018.06.022`）；本文固定上述单一
Gaussian-centroid 实例，不引入论文框架的任意模型扩展。

每个 component 的 `λ` grid 固定为 `[0.25,0.5,1.0,2.0,4.0,8.0]`；每个 λ 完整运行 KMeans++
`n_init=1/random_state=42..49` 八个 global restart，不 early stop、不扩 grid。相同 λ 内只按 train normalized objective
`J/(valid_row_count*feature_count)` 最小选择 restart；tie tolerance 为
`1e-12+1e-12*max(abs(a),abs(b))`，完全 tie 取较早 schedule seed。禁止 validation/future utility/coverage/READY 参与
restart 选择，也禁止 per-sector restart。

开发区间固定为 `2022-01-04..2025-03-31` 的 783 个交易日，采用 anchored expanding walk-forward：

- base train：`2022-01-04..2023-09-01`（405日）；
- fold-1：`2023-09-04..2024-03-14`（126日）；
- fold-2：`2024-03-15..2024-09-18`（126日）；
- fold-3：`2024-09-19..2025-03-31`（126日）。

精确train/validation配对为：fold-1 train=`2022-01-04..2023-09-01`；fold-2 train=`2022-01-04..2024-03-14`
（531日）；fold-3 train=`2022-01-04..2024-09-18`（657日）。任何自然日推算、少日、重复日或跨fold行均fail closed。

每个 fold 只用此前日期拟合 preprocess/centers/restart，validation 使用固定参数和递归 arrival cost 做 causal inference，
不得 smoothing。λ 只按 D4 的三个 fold 产品指标选择；selection 后在完整 783 日开发区间重拟合一次，仍完整运行八个 restart并
只按 train objective 选最终 restart。untouched holdout `2025-04-01..2026-03-31` 共242个交易日，在 model/preprocess/
λ/restart/semantic hash 冻结前禁止读取。当前只读水位检查只证明数据源声明水位足以覆盖 holdout及20日 outcome，不构成输入
验收。执行前 source preflight 必须冻结实际 calendar、benchmark、feature formula/version、PIT hierarchy、coverage ledger、
development/holdout date set 与各自 canonical hash；任一逐日 coverage 或 identity 不闭合均 fail closed。候选五项不读取
`limit_up_ratio`，但不得由此推导其他数据源完整。

开发fold的future label不得越过各自fold end：horizon=h的metric只使用满足`t+h<=fold_end`的decision date；因此fold-3
不得读取2025-04-01之后的任何return/outcome。每个horizon的excluded-tail日期、eligible decision-date set与hash必须写入
fold receipt。P2-4可为holdout decision dates读取其后最多20个交易日的outcome tail，但必须把holdout state date-set与
outcome-only tail date-set、最大日期和source hash分开冻结；tail只计算label，不进入observation、fit、selection或state。

三个 component 的 λ 必须分别选择，禁止把三个各六档 grid 组合成 `6^3` 联合搜索，也禁止用一个 component 的成功替代另一个：

- market λ采用`C-011-P2-3-D3-MARKET-ZERO-EVENT-A`：三个fold的完整confusion matrix与100% outcome-eligible date coverage均为必要条件。对`P_f=TP_f+FN_f>0`的event-bearing fold，F1与precision-lift必须有限；对`P_f=0`且`N_f=FP_f+TN_f>0`的zero-event fold，recall/F1继续为typed unavailable且不得填0，同时必须保存有限`FPR_f=FP_f/N_f`与specificity。三个fold中至少两个必须为event-bearing；少于两个、任一fold负例分母非正、任一count/date/hash不闭合或任一所需指标非有限时，该λ不具备selection资格；
- 对具备资格的market λ，先在三个fold的confusion counts上计算并最大化pooled micro-F1，再最大化pooled precision-lift；若存在zero-event fold，再最小化其`max(FPR_f)`；最后选择较小λ。pooled precision/recall/F1/base-rate的分母必须严格为正，差`<=1e-4`按既有component tie处理。zero-event fold没有被删除、重切、借值或当作event-bearing fold；其FP/TN完整进入pooled分母和FPR负对照；
- L1 relative λ：先最大化三个 fold 的 L1 10D mean Rank IC median，再最大化 L1 10D `trending-fading` spread median，
  最后选择较小 λ；
- L2 relative λ：先最大化三个 fold 的 L2 10D mean Rank IC median，再最大化 L2 10D `trending-fading` spread median，
  最后选择较小 λ。

market metric必须覆盖该fold全部10D outcome-eligible dates；market零正例只适用上文显式negative-control合同，不构成缺失或可补值的
primary metric。L1/L2 10D Rank IC与spread必须各自覆盖该fold至少80%的10D outcome-eligible dates。除market零正例的已批准分支外，
任一fold的对应primary metric、numerator或denominator不可用或覆盖不足时，该component/λ不具备selection资格；不能用另一个fold或
secondary metric补齐。每个 component 差 `<=1e-4` 视为 tie。三项 selection receipt 独立冻结后，只组合、hash并生成待 P2-4 复现的唯一候选；
P2-3 不读取或执行 untouched holdout。holdout 或任一 holdout-derived hash 不得进入 selection；P2-4 holdout失败后不得换
λ、seed、feature、K 或 family。

##### D4. 产品指标、风险标签与正式阈值候选

未来 excess return 沿用批准的 CSI300 benchmark 和 5/10/20D causal decision-date boundary：
`R[g,t,h]=product_{u=t+1..t+h}(1+r[g,u])-1`，`R_excess[g,t,h]=R[g,t,h]-R[CSI300,t,h]`。
future window 不含 t 日；未来值只用于离线评估，不进入 t 日 observation/state。state signal 编码固定为
`trending=+1, neutral=0, fading=-1`，并以 average-rank 处理 tie。

1. 日度横截面 Rank IC：逐日对 state signal 与 sector future excess return 计算 Spearman；主指标为 L2 10D。两侧均按
   canonical sector code对齐后使用average-rank，再对rank计算Pearson correlation；任一侧样本少于5、方差非有限/非正、
   identity集合不相等或结果非有限时该日metric unavailable，禁止返回0。
2. spread：逐日 `mean(future_excess | trending)-mean(future_excess | fading)`；任一侧少于 5 个 sector 当日记为
   metric unavailable，不缩小阈值或改用 top/bottom 代替。
3. market λ selection label：`market_risk_event[t]=1` 当且仅当
   `min_{h=1..10}(product_{u=t+1..t+h}(1+r[CSI300,u])-1)<=-0.05`；prediction 为当日 market regime=`risk_off`。
   它只用于 market component 的开发 fold selection，不得替代
   sector holdout risk 指标。
4. sector final risk label：`risk_event[g,t]=1` 当且仅当 `min_{h=1..10}(R_excess[g,t,h])<=-0.05`；
   `warning[g,t]=1` 当且仅当 t 日 sector state=`fading` 且（前一可比较交易日不是`fading`，或 market regime=`risk_off`）。
   sector路径首个可用日没有前态，不因“进入fading”单独触发。precision/recall/F1按level在全部canonical available
   `sector×date`上做micro aggregation，并同时保存逐sector与逐quarter分解；L1/L2均使用同一阈值独立验收，不能用一层替代另一层。
   所有TP/FP/FN/TN、base-rate numerator/denominator必须持久化。market 与 sector label 的样本、分母、event hash 和metric receipt
   必须分开，禁止相互代用。
5. overlapping 5/10/20D 指标的均值 t-stat 使用 Newey-West，lag 分别为4/9/19。对长度 N 的有序 metric series，
   `gamma_k=(1/N)*sum_{t=k+1..N}((x_t-x_bar)*(x_{t-k}-x_bar))`，
   `variance_mean=(gamma_0+2*sum_{k=1..L}((1-k/(L+1))*gamma_k))/N`，
   `t_stat=x_bar/sqrt(variance_mean)`。N 必须 `>L+1`；输入、gamma、variance 或 t-stat 任一非有限，或
   `variance_mean<=0` 时该 metric typed unavailable，不补零、不换普通 t-stat。
6. risk metrics固定为`precision=TP/(TP+FP)`、`recall=TP/(TP+FN)`、
   `F1=2*precision*recall/(precision+recall)`、`base_rate=(TP+FN)/(TP+FP+FN+TN)`和
   `precision_lift=precision-base_rate`。sector产品验收仍沿用原分母合同。仅对market development λ selection，若单fold
   `TP+FN=0`，该fold的recall/F1必须保持`null/unavailable`，并改以`FPR=FP/(FP+TN)`和
   `specificity=TN/(FP+TN)`记录negative-control evidence；不得把unavailable写成0。market pooled micro指标只在三个fold
   counts全部闭合且总`TP+FN>0`、总`TP+FP>0`时计算。任一适用分母非正或结果非有限时对应fold/λ typed unavailable；不从其他
   fold/level借值。

以下是 P2-4 首次读取 untouched holdout 时适用的产品通过候选；P2-3 只冻结公式、阈值和输入 identity，不计算这些结果：

- L1与L2的10D Rank IC、spread各自必须在至少80%的10D outcome-eligible holdout dates上可计算；每个calendar quarter的
  L2 10D Rank IC也必须覆盖该quarter至少80%的outcome-eligible dates，否则对应主指标/quarter typed unavailable；
- L2 10D mean Rank IC `>=0.02` 且 Newey-West t-stat `>=1.96`；
- L2 10D trending-fading spread `>=0.005` 且 Newey-West t-stat `>=1.96`；
- L1 10D mean Rank IC `>0` 且 L1 10D spread `>0`；
- L1与L2各自的risk precision都至少比本level holdout risk-event base rate高`0.10`，recall都`>=0.25`；
- 四个 calendar quarter 中至少三个 L2 10D mean Rank IC `>0`，且任一 quarter 不得 `<-0.02`。

5D/20D、market-regime-conditioned metrics、confidence margin 与其它 family 只作预注册 secondary diagnostics，不得在主指标
失败后替换主指标或触发 reselection。上述数值已作为P2-4模型/产品精确合同获用户批准；它们是确定性离线验收条件，不是运行时人工审批。

##### D5. Coverage 与三状态

每个 holdout date 的 denominator 始终为 canonical L1=31、L2=131；每个 unavailable 项保留 level、sector、date、stage 和
typed reason。L2→L1 hierarchy mapping 以及 size/liquidity quintile assignment 必须只用完整783日development train facts
冻结并写入manifest/hash。size统计量固定为每个sector development期有限`price_expected_weight`的median，liquidity统计量固定为
有限`moneyflow_contributor_amount`的median；任一统计量覆盖少于该sector development有效日的80%时，不得把该sector静默放入
任一quintile，代表性合同直接fail closed。每个统计量按数值升序、再按canonical sector code稳定排序，并以
`floor(rank*5/sector_count)`（zero-based rank，最大值截为4）分配五组；holdout期间不得重算、重分组或按可用样本改变组别。
任一mapping、统计量、边界或组成员hash缺失时代表性合同fail closed。状态严格互斥：

coverage对全部242个holdout state dates计算，不因某日没有完整10/20日future outcome而删除该日；D4 metric eligibility与D5 state
availability使用两个明确分母并分别持久化，禁止用较短metric denominator冒充coverage。

- `FULL_READY`：D1～D4全部通过，且242个holdout date均为L1 `31/31`、L2 `131/131`；
- `COVERAGE_AVAILABLE`：D1～D4全部通过；至少90%的holdout dates达到L1 `>=28/31`与L2 `>=118/131`；每个
  canonical sector至少80%的holdout dates可用；size与liquidity每个quintile的sector-date availability均`>=80%`；
  每个L1 parent至少90%的holdout dates有一个可用L2 child；其余项显式typed unavailable；
- `NOT_AVAILABLE`：产品指标、数值/语义安全或任一coverage/代表性条件不满足。

`COVERAGE_AVAILABLE`不得写成READY、不得补neutral、不得删除分母，也不得因 spike time-box 自动获得。FULL_READY 与
COVERAGE_AVAILABLE 的 metrics 使用同一已冻结模型和同一 canonical denominator；禁止分别选择模型。

##### D6. 单一 spike 实施、成本与停止条件

实现只允许新增 `backend/services/hmm_risk/market_relative_jump_spike.py`、
`backend/tests/hmm_risk/test_market_relative_jump_spike.py`与薄CLI
`scripts/hmm_risk/run_market_relative_jump_spike.py`；复用现有 observation reader，
不得新建通用 estimator/evidence/training platform、数据库表、scheduler 或历史输入副本。算法只使用仓库现有
NumPy/SciPy/scikit-learn，不新增依赖。单进程 spike 计划为三个 component × 六个 λ × 八个 restart × 三个 fold，加三个
final component × 八 restart，共 `456` 个 pooled jump fits；它不是456×sector。spike 只写 compact model-candidate/metric/failure
receipt，不写生产 model set、READY、DB或runtime。

稳定failure reason至少区分：`hmm_risk_jump_input_identity_mismatch`、`hmm_risk_jump_fold_boundary_invalid`、
`hmm_risk_jump_preprocess_invalid`、`hmm_risk_jump_objective_non_finite`、`hmm_risk_jump_objective_increased`、
`hmm_risk_jump_state_empty`、`hmm_risk_jump_semantic_tie`、`hmm_risk_jump_selection_metric_unavailable`、
`hmm_risk_jump_selection_unavailable`、`hmm_risk_jump_holdout_access_forbidden`、`hmm_risk_jump_candidate_collision`与
`hmm_risk_jump_candidate_readback_mismatch`。P2-4另记录`hmm_risk_jump_product_metric_unavailable`、
`hmm_risk_jump_coverage_contract_failed`和`hmm_risk_jump_representativeness_failed`。这些是确定性错误分类，不是新增人工审批；
unknown exception必须保存exception type/stage后fail closed，不得压成`incomplete`或伪造成功。

`C-011-P2-3-D3-MARKET-ZERO-EVENT-A`实施后，P2-3唯一durable request/report schema分别固定为
`hmm_risk_market_relative_jump_spike_request_v2`与`hmm_risk_market_relative_jump_spike_report_v2`，algorithm version为
`hmm_risk_market_relative_jump_v2`；旧v1 request必须拒绝，旧v1 report只作历史证据。顶层至少包含：contract/model/algorithm
version、producer commit与数值环境、dataset/mapping/calendar/benchmark/formula/hierarchy hashes、development与forbidden-holdout
date-set hashes、planned/completed fit counts、三个component receipt/hash、candidate status、failure stage/reason、canonical hash以及
`holdout_accessed=false,selection_performed=<actual>,selection_scope=development_only,
product_acceptance_performed=false,model_write=false,ready_write=false,database_write=false,runtime_action=false`。
失败发生在selection前时`selection_performed=false`，只有三个component均完成批准的development restart/λ selection才为true；
不得预填true或与P2-4 acceptance混用。另以`candidate_receipt_write=<actual>`区分允许的compact receipt写入与禁止的production
model write。`holdout_accessed`只统计holdout feature/return/outcome/metric读取；预注册的窗口端点与calendar-only date-set identity可以
写入manifest，但不得据此访问该窗口的业务数据。

每个component receipt至少包含：component/level/K/feature order、valid row count/identity hash、typed unavailable item清单、preprocess完整参数与hash、每fold
train/validation/outcome-eligible hashes、全部λ×restart objective/status/failure摘要、selected λ与restart receipt、final centers、
semantic mapping、train path/count/run/jump摘要、final arrival-cost policy和parameter hash。大数组只保存shape/dtype/canonical content hash
与不可替代的final参数，不复制完整历史输入或每条path。success与failure都必须collision-safe首次写入、canonical回读并验证hash；
同路径既有不同内容必须`hmm_risk_jump_candidate_collision`，禁止覆盖。
market fold metrics schema固定为`hmm_risk_jump_market_fold_metrics_v2`，并额外保存fold role、event/negative/predicted-positive counts、
TP/FP/FN/TN、market event/prediction hashes、FPR/specificity及适用时的precision/recall/F1/lift；market λ receipt保存三个fold event hashes、
event-bearing/zero-event fold counts、pooled confusion counts、pooled micro-F1/lift和max zero-event FPR。

CLI必须显式接收绝对、repo-external的`--output`，不得提供`latest`、默认目录或覆盖开关。正常/业务失败写指定report；在指定report
无法安全落盘、发生collision或final readback失败时，只允许首次写入同目录不同identity的`<stem>.failure.json`，且仍不得覆盖既有
内容。JSON与hash统一复用`state_model_set.canonical_json_bytes/canonical_sha256`（UTF-8、sorted keys、compact separators、
`allow_nan=false`）；array hash使用显式dtype/shape framing和little-endian C-order bytes，禁止`default=str`或float舍入。

任一 source/hash/causal split/numeric/semantic闭合失败即 typed fail closed，状态为 `NOT_AVAILABLE_FOR_PROMOTION`；不得自动
启动第二范式、换阈值、扩大 grid、安装依赖或承诺 coverage fallback。只有三个 component 都完成三fold选择、完整783日final refit、
train-only semantic mapping、candidate hash/readback且 zero-side-effect，才形成
`P2_3_SPIKE_ACCEPTED_PENDING_P2_4_HOLDOUT_ACCEPTANCE`。P2-3 不读取 holdout、不判断 FULL_READY/COVERAGE_AVAILABLE。
P2-4 的双fresh-process reproducibility、首次 holdout evaluation、D4/D5 product acceptance、canonical model writer和最终状态写入
仍是独立实现及合入边界；P2-4失败不得返回P2-3重选。

##### D1～D6 取舍、成本与唯一建议

| 决策 | 主要价值 | 主要false-accept风险 | 主要false-reject风险 | 受控边界 |
|---|---|---|---|---|
| D1 pooled market+relative jump | 直接服务市场环境、L1/L2相对轮动，不再逐sector拟合三套centers | shared centers可能掩盖sector异质性 | 单一center geometry可能拒绝真实小众regime | 保留全canonical denominator、逐sector独立causal path、失败即停止 |
| D2五项可解释特征+level-global scaler | 避免黑箱PCA和逐sector scaler抹平横截面强弱 | 省略特征可能留下未建模混杂 | winsor可能压缩真实极端预警 | threshold/scaler仅train拟合，全部identity/hash可回读 |
| D3固定λ/grid/folds+零正例负对照 | 成本固定、避免无限搜索和holdout调参，并让无正例fold以FP/TN进入选择而不伪造F1 | pooled指标仍可能被不同fold事件率影响 | 至少两个event-bearing fold要求可能在低频市场拒绝全部λ | 不重切/删除fold、不改风险标签、不扩grid、不并行范式、P2-4失败不reselect |
| D4产品指标 | 验收直接对应轮动区分和风险提示 | 多指标同时观察可能放大偶然成功 | 242日holdout可能拒绝低频但真实预警 | primary/secondary预注册、全部分母和不可用原因持久化 |
| D5双层availability | 少量provider-absence不阻断全部产品且不隐藏覆盖 | 90%/80%边界内仍可能有结构偏差 | 稀有/新sector可能使coverage失败 | development冻结分组、holdout不重分组、FULL_READY严格全覆盖 |
| D6单进程spike | 最小代码面和456 fits，尽快验证产品假设 | 单进程成功不能证明可复现promotion | 环境噪声可能造成一次性失败 | P2-3不写model/READY；P2-4另做双fresh-process和holdout |

用户已整体批准`C-011-P2-3-A/D1～D6`，后续只实施这一条spike；不得拆成能绕过失败的子集实现。批准只使精确设计生效，
不把尚未发生的源码、456 fits、selection或P2-4 holdout验收写成完成。

实现PR的直接fix-point仅为`backend/tests/hmm_risk/test_market_relative_jump_spike.py`及薄CLI测试，必须覆盖：

- valid-row全五项finite闭合、linear quantile、`math.fsum` mean/std、偶数median、禁止逐sector scaler与NA补值；
- KMeans完整参数/seed identity、small-array DP对brute-force oracle、gap/segment reset、cost tie-break、empty state、objective increase与
  max-iteration failure；
- 三个anchored folds的精确日期/行数、fold-3 outcome不越过development、P2-3 holdout access fail-fast且网络/DB reader未被调用；
- restart只读train objective、三个component分别选择λ、market event-bearing/zero-event fold分类、至少两个event-bearing fold、pooled micro-F1/precision-lift/max-zero-event-FPR手算正反例、其他未批准fold metric unavailable仍candidate ineligible、无`6^3`联合搜索；
- market/sector semantic排序与tie、Rank IC/spread/Newey-West/risk公式的手算正反例、L1/L2独立risk gate；
- FULL_READY/COVERAGE_AVAILABLE/NOT_AVAILABLE边界、development冻结hierarchy/quintile、metric与coverage双分母；
- CLI显式repo-external output、无默认/latest/overwrite、report/failure sibling首次写入；success/failure schema、canonical
  hash/readback/collision、异常typed reason与所有zero-side-effect flags；
- 计划fit严格`456`，不乘sector、不启动第二范式、不调用旧HMM READY writer、repository、scheduler或runtime。

#### 4.3.4.3 P2-3A 正式失败与 P2-3B direct cross-sectional predictor 精确候选

##### A. P2-3A 不可用事实与停止边界

P2-3A zero-event v2正式执行绑定producer `c1c6c313c7b684afafc5b7266967d9ac9ee110d3`、request canonical
`5dd09e2f9ec081fdf1e4f1e9f658a60f77503088de4ba067f3881d05613383db`与failure report canonical
`034fdf3c7a2354bad62bdea0a55b675f2552c42a65d1ccacbc454561e75f12ec`。它完成market `152` fits和L1 relative
`144` fits，总计`296/456`；market进入full-development的lambda为`4.0`，八个seed normalized objective完全相同并按既有tie-break
对应seed42。L1六个lambda均因三fold的10D spread覆盖少于`93/116`而无selection资格，L2剩余160 fits未启动。

诊断只重放failure receipt已经记录的18个L1 `(lambda,fold,selected_seed)`，不扩大grid或重新选择；全部centers/path/selected-fit
hash精确闭合。Rank IC在18个fold receipt中均为`116/116`可计算，证明失败不是日期、provider或future-return大面积缺失；正式min-5
spread覆盖在lambda0.25的三fold仅`72/71/58`日，缺口主要来自trending组少于5个sector。18/18正式mean spread为负，Rank IC仅
3/18为正；即使诊断性把每侧最低sector数降为1，主流lambda也没有稳定正向三fold结果。因此：

1. P2-3A状态固定为`NOT_AVAILABLE_FOR_PROMOTION`，不得重跑同一456-fit grid或进入P2-4；
2. 不得降低80%/5-sector门禁、反转centroid label、删除fold/level、扩大seed/lambda或只交付market component；
3. P2-3A源码与artifact继续只作append-only失败证据，不作为P2-3B model/candidate/READY输入；
4. 本结果不是数据或执行器BUG，也不构成第二模型的执行结果。以下D1～D6精确合同随后已由用户批准、实施并正式执行；其独立结果见本节H与§23.24，不得用P2-3A artifact替代P2-3B证据。

##### B. C-011-P2-3B-D1：唯一模型identity与业务输出（USER_APPROVED_SOURCE_IMPLEMENTED_FORMAL_EXECUTED）

唯一候选identity建议为`hmm_risk_market_relative_ridge_candidate_v1`，只包含：

1. market component继续使用P2-3A已批准的K=2 jump算法、market五特征、六lambda、seeds42..49、三foldzero-event v2
   selection合同；新候选必须重新执行并写入自己的market receipt，禁止复制P2-3A局部结果；
2. L1和L2分别使用一个level-local、pooled、确定性的scikit-learn `Ridge`模型，直接预测10D future relative excess return；
3. sector主输出为连续`rotation_score`和由当日横截面投影得到的`forecast_state=trending|neutral|fading`；market输出仍为
   `risk_on|risk_off`。forecast state表示预测的未来相对走强/走弱，不是hidden state、当前强弱或HMM posterior；
4. market regime仅作为并列上下文和既有warning组合条件，本首个候选不把market state或interaction加入Ridge输入，避免在同一spike
   同时评估两套sector公式。若未来要加入interaction，必须是新的model identity和用户决策；
5. 新origin固定为`market_relative_ridge_v1`，禁止复用`market_relative_jump_v1`、`direct_hmm`的parser、hash、semantic receipt或READY。

##### C. C-011-P2-3B-D2：输入、target与因果边界（USER_APPROVED_FORMAL_EXECUTED_INPUT_CLOSED）

L1/L2继续使用P2-3A已经批准的五项sector-relative特征、canonical universe、PIT/t-1边界、level-global 1%/99% winsor和
population z-score；不新增数据集、特征、依赖、插补或逐sector scaler。每个fold的preprocess只拟合train并冻结到validation。

训练target候选固定为：

`Y[g,t,10] = R_excess[g,t,10] - median_h(R_excess[h,t,10])`

其中`R_excess`沿用D4的CSI300 10D future excess return，daily median仅在该level同日outcome完整的canonical sector上计算；最低分母
L1=`28/31`、L2=`118/131`，不足时整日target typed unavailable。训练样本必须同时具备t日五项finite observation和不越过该train
segment end的完整t+1..t+10 outcome；每个train segment最后10个交易日purge，不得借用validation或holdout outcome。target、row、date、
sector、excluded-tail、benchmark与cross-section denominator均进入hash/receipt。validation target只用于该fold已批准的开发selection指标；
untouched holdout在P2-3B仍不得读取。

##### D. C-011-P2-3B-D3：Ridge参数与开发选择（USER_APPROVED_FORMAL_EXECUTED_DEVELOPMENT_EFFECT_NON_POSITIVE）

每个level的候选参数建议固定为：`fit_intercept=true, solver="svd", positive=false, copy_X=true, tol=1e-4,
max_iter=null, random_state=null`，输入与系数均为little-endian float64。alpha grid建议为
`[0.01,0.1,1.0,10.0,100.0]`，不使用自动CV、随机搜索、per-sector模型或额外seed。

每个alpha完整运行三个既有anchored development folds；每fold仅以此前train rows拟合并预测validation。每个level的alpha eligibility要求
10D daily Rank IC和D4定义的forecast-state spread均在每fold至少80%的outcome-eligible dates可计算且finite。eligible alpha先最大化三fold
mean Rank IC的median，再最大化三foldmean spread的median；每级差`<=1e-4`视为tie，最终取较大alpha以预注册方式偏向更强收缩。
所有alpha完成后才选择，不early stop；selection不得读取holdout、risk warning结果、coverage READY或另一level结果。

建议增加开发可行性停止条件：L1和L2各自selected alpha的三foldmedian Rank IC与median spread必须都严格`>0`，否则该level和整个
P2-3B候选为`NOT_AVAILABLE_FOR_PROMOTION`，不得靠P2-4 holdout、另一level或market component挽救。选择后各level在完整783日development
区间仅以可获得完整10D target的样本重拟合一次；不再调参。

##### E. C-011-P2-3B-D4：rotation score、三态投影与产品指标（USER_APPROVED_FORMAL_EXECUTED_L1_METRICS_COMPLETE）

`rotation_score[g,t]`为冻结Ridge对t日五项输入的有限预测值，不解释为概率。每个level/date只在全部五项输入finite且daily denominator
达到L1 28、L2 118时形成canonical score cross-section。令`N`为该日可用sector数、`q=max(5,ceil(0.20*N))`；先按
`(rotation_score,canonical_sector_code)`建立稳定审计顺序，初始最低q个位置为fading、最高q个位置为trending、其余为neutral。
sector code只用于可复现排序，不解决数值tie。把score差`<=1e-12`视为同一tie group；若一个tie group同时跨越`q/q+1`或
`N-q/N-q+1`位置边界，则该tie group全部改为neutral，禁止按sector code拆成不同语义。若调整后任一extreme少于5个sector，
该日state projection以`hmm_risk_rotation_state_boundary_tie_insufficient` typed unavailable。无boundary tie时必须严格得到q个fading、
q个trending和`N-2q`个neutral；禁止强拆、补label、改q或缩小分母。

开发selection和P2-4继续使用D4既有10D Rank IC、trending-fading spread、sector risk label、Newey-West、risk precision/recall及阈值；
不因模型更换降低产品门禁。warning仍仅在sector=`fading`且“前一可比较日不是fading或market=`risk_off`”时触发，路径首日规则不变。
5D/20D、market-conditioned结果和raw coefficient只作secondary diagnostics，不得替代10D主指标或触发reselection。

##### F. C-011-P2-3B-D5：候选、holdout与三状态闭包（USER_APPROVED_NOT_ENTERED_NO_CANDIDATE）

只有market、L1、L2三component均完成全部development folds、selection、完整development refit、参数/metric/hash readback，并且L1/L2
通过D3开发正向停止条件时，才允许写compact状态
`P2_3B_CANDIDATE_FROZEN_PENDING_P2_4_HOLDOUT_ACCEPTANCE`。该状态不是model/READY/COVERAGE_AVAILABLE。

P2-4才执行两个fresh process的bitwise参数/score/state receipt一致性和一次untouched holdout。D4/D5既有holdout产品阈值、canonical
denominator、代表性与`FULL_READY|COVERAGE_AVAILABLE|NOT_AVAILABLE`互斥状态保持不变；P2-4失败后不得返回P2-3B换alpha、target、feature、
state boundary或market lambda。P2-3A与P2-3B不能投票、ensemble或fallback。

##### G. C-011-P2-3B-D6：最小实施、成本与停止条件（USER_APPROVED_FORMAL_EXECUTED_FAIL_CLOSED_167_OF_184）

实施范围建议新增`backend/services/hmm_risk/market_relative_ridge_candidate.py`、对应直接测试和薄CLI；允许仅为稳定导出既有market纯计算
authority而对`market_relative_jump_spike.py`及其直接测试做最小、无语义变化的公开helper调整。复用现有只读input loader与canonical hash，
不复制market算法，不抽象通用estimator/training/evidence平台，不新建表、scheduler或历史输入副本。scikit-learn已是现有依赖，
不安装新包。

计划成本固定为market `6 lambda × 8 seeds × 3 folds + 8 final = 152` fits，L1/L2各
`5 alpha × 3 folds + 1 final = 16` fits，总计`184` fits；它不乘sector，不执行双fresh-process或holdout。成功只写一个repo-external、
collision-safe compact candidate receipt，保存market centers、Ridge alpha/coefficient/intercept、preprocess、target、fold metrics、state projection、
完整identity与canonical hash；失败写typed sibling receipt。不得写production model、READY、DB或runtime。

至少区分`hmm_risk_rotation_input_identity_mismatch`、`hmm_risk_rotation_target_unavailable`、
`hmm_risk_rotation_fit_failed`、`hmm_risk_rotation_score_non_finite`、`hmm_risk_rotation_metric_unavailable`、
`hmm_risk_rotation_selection_unavailable`、`hmm_risk_rotation_development_effect_non_positive`、
`hmm_risk_rotation_state_boundary_tie_insufficient`、`hmm_risk_rotation_holdout_access_forbidden`、candidate collision/readback mismatch与
unknown exception。任一失败都保持NOT_AVAILABLE；不自动打开t-emission、shared-prior、deep model、交互项或第三个spike。

##### H. 设计状态与唯一建议

P2-3B已按用户批准的D1～D6完成正式开发执行。唯一producer为
`24e4ae79780e5bacdf34a3affb63d1db46f6d8a4`，request canonical为
`f3d9014ba6c1aa59eceda41b148ab97e37bed5f0c05a471128b8dc0f26c471b1`，正式failure receipt canonical为
`d3298654ed9f2080f4623c2c50721ebf9951d2034d42cfdfe225f36e4ee0fc45`。market完成`152/152` fits并选择
`lambda=4.0/seed=42`；L1完成五个alpha的全部`15`个fold fits，每个fold的Rank IC和spread覆盖均为`116/116`，五个alpha
均eligible，按批准的Rank IC→spread→较大alpha顺序选择`alpha=100.0`。

alpha100三fold结果为：fold-1 Rank IC=`-0.007807285873192439`、spread=`0.0009441908663057883`；fold-2 Rank IC=
`0.0867491657397108`、spread=`0.005458352160960382`；fold-3 Rank IC=`-0.057080784204671865`、spread=
`-0.006434638075431702`。三foldmedian Rank IC=`-0.007807285873192439`、median spread=`0.0009441908663057883`，
因此在`167/184` fits以`hmm_risk_rotation_development_effect_non_positive`正确fail closed；L1 final refit与L2均未执行。

491个嵌套receipt hash、request、attempt、alpha列表和逐日metric重聚合全部闭合；target unavailable、state projection unavailable均为0。
正式审核结论为`PASS_EXECUTION_INTEGRITY_FAIL_MODEL_ACCEPTANCE_NOT_AVAILABLE_FOR_PROMOTION`：失败来自跨fold预测关系不稳定，
不是输入数据、指标方向、优化器或执行器BUG。当前统一状态为`VERIFIED_FORMAL_EXECUTION_NOT_AVAILABLE_FOR_PROMOTION`；未访问holdout，
未形成candidate、model、FULL_READY或COVERAGE_AVAILABLE，也未写DB或触发runtime。不得进入P2-4、用market局部结果或spread局部为正冒充
产品候选，也不得自动开启第三模型；下一步只能由用户批准新的唯一模型合同，或明确停止Phase 2模型方向。

#### 4.3.4.4 P2-3C market-conditioned Ridge 精确合同（USER_APPROVED_EXACT_CONTRACT_IMPLEMENTATION_NOT_AUTHORIZED）

本节固定一个直接针对P2-3B失败形态的可证伪合同。P2-3B alpha100的fold-2与fold-3系数cosine为
`0.9854561478407049`，但Rank IC由`0.0867491657397108`反转为`-0.057080784204671865`，spread由
`0.005458352160960382`反转为`-0.006434638075431702`。这支持“特征斜率可能随market regime变化”的单一假设，
不证明该假设成立，也不允许把P2-3A/P2-3B development结果当作P2-3C成功证据。

##### A. C-011-P2-3C-D1：唯一identity、范围与非目标（USER_APPROVED_EXACT_CONTRACT_NOT_IMPLEMENTED）

候选identity固定为`hmm_risk_market_conditioned_ridge_candidate_v1`：

1. market仍是K=2 jump component，产品语义仍为`risk_on|risk_off`；sector仍输出连续`rotation_score`与
   `trending|neutral|fading`横截面forecast state；
2. L1/L2都必须使用同一新合同并共同形成候选，禁止market-only、L1-only、L2-only、per-sector模型或P2-3B fallback；
3. 不新增原始数据、target、sector universe、PCA、树模型、神经网络、ensemble、独立market intercept、通用estimator平台或scheduler；
4. P2-3A/P2-3B artifact只作为选择该假设的历史证据，不复制其market path、preprocess、coefficient、selection或candidate状态；
5. 若该候选在development失败，Phase 2模型方向停止，不自动开启P2-3D或通过阈值/方向/level变更继续搜索。

##### B. C-011-P2-3C-D2：market condition 的train-only因果合同（USER_APPROVED_EXACT_CONTRACT_NOT_IMPLEMENTED）

market参数不再搜索，固定为此前两次正式执行均选中的`jump_penalty=4.0`与tie-break `seed=42`。固定参数是新candidate的预注册输入，
不是复制旧fit：

1. 每个development fold只用该fold market train segment拟合既有五项market preprocess与K=2 centers；
2. 使用既有`causal_states()`、`arrival_cost_policy=zero_at_each_segment_start_no_train_carry`分别生成train与validation market state；
   禁止optimal/backtracked path、validation fit、train末状态carry或未来observation；
3. centers按既有market semantic mapping生成`risk_on|risk_off`；映射、centers、train/validation state rows、date set、state counts、
   transitions与canonical hash写入fold receipt；transition count只作诊断，不形成新门禁；
4. 固定lambda4的三个fold仍必须逐项通过P2-3A/P2-3B既有market metric validity、event-bearing/zero-event negative-control和聚合
   development acceptance；这里取消的是重复参数搜索，不是market产品验收；
5. train与validation均必须同时出现两个market state，否则交互假设在该fold不可检验并以
   `hmm_risk_market_conditioning_regime_unavailable` fail closed；不补state、不换lambda/seed、不借用另一fold；
6. 只有L1与L2 development均通过后，才在完整development window用相同固定参数执行一次market final fit；holdout仍不得读取。

##### C. C-011-P2-3C-D3：十维交互特征与Ridge参数（USER_APPROVED_EXACT_CONTRACT_NOT_IMPLEMENTED）

每个level继续使用P2-3B已经批准的五项sector-relative输入、PIT/t-1边界、fold-train-only level-global 1%/99% winsor与population z-score。
令标准化后的五维向量为`x[g,t]`，令`m[t]=+1`表示`risk_on`、`m[t]=-1`表示`risk_off`；新输入严格为：

`z[g,t] = [x_1,...,x_5,m*x_1,...,m*x_5]`

不加入单独`m`列，因为同日market state对整个sector横截面为常数且target已按同日level median中心化。该参数化使risk-on斜率为
`beta+gamma`、risk-off斜率为`beta-gamma`，共享一个intercept；它不改变score方向或产品标签。十维feature name/order、little-endian
float64 matrix、market-state identity、interaction rows与hash必须持久化；任何shape、非有限值、日期或state identity不一致均typed失败。

Ridge参数与P2-3B保持一致：`fit_intercept=true, solver="svd", positive=false, copy_X=true, tol=1e-4,
max_iter=null, random_state=null`，alpha grid仍为`[0.01,0.1,1.0,10.0,100.0]`。不得增加CV、第二正则参数、per-regime
独立模型或基于validation选择是否使用interaction。

##### D. C-011-P2-3C-D4：target、selection与产品验收（USER_APPROVED_EXACT_CONTRACT_NOT_IMPLEMENTED）

10D daily-centered future relative excess target、末10日purge、L1=`28/31`与L2=`118/131` denominator、三段anchored development folds、
top/bottom 20% state projection、boundary tie、每侧至少5、80% metric coverage及产品指标公式全部继承P2-3B，不作放宽或方向反转。

每个level的五个alpha必须全部完成三个fold后才选择；仍先最大化三foldmean Rank IC的median，再最大化mean spread的median，
每级差`<=1e-4`为tie并取较大alpha。selected alpha的median Rank IC与median spread必须对L1、L2分别严格`>0`；任一level失败，
整个P2-3C为`NOT_AVAILABLE_FOR_PROMOTION`。按risk_on/risk_off拆分的Rank IC、spread、日期数和coefficient只作诊断，不成为新的
selection输入或验收门禁，避免用较小regime子样本再次调参。

##### E. C-011-P2-3C-D5：candidate、holdout与多次开发边界（USER_APPROVED_EXACT_CONTRACT_NOT_IMPLEMENTED）

只有三个market fold、L1/L2全部alpha folds、两level development acceptance及三个final refit都闭合，才允许写
`P2_3C_CANDIDATE_FROZEN_PENDING_P2_4_HOLDOUT_ACCEPTANCE` compact receipt。它必须记录这是第三个development模型identity、
前两个NOT_AVAILABLE receipt hash、固定market参数来源、全部interaction identity和`holdout_accessed=false`；但P2-3A/P2-3B payload
不是模型输入。

该状态不是model、FULL_READY或COVERAGE_AVAILABLE。P2-4继续只允许一个冻结P2-3C candidate访问untouched holdout一次；P2-4失败后
不得回到development改变market sign、interaction、alpha、target、state boundary或level。由于前两个候选未访问holdout，本文不新增
holdout多重比较阈值；candidate attempt index只作透明审计，不形成运行时人工审批。

##### F. C-011-P2-3C-D6：最小源码、fit成本、failure与停止（USER_APPROVED_EXACT_CONTRACT_NOT_IMPLEMENTED）

后续另行取得源码实施授权时，只允许在已经登记为离线非运行时源的
`backend/services/hmm_risk/market_relative_ridge_candidate.py`、`scripts/hmm_risk/run_market_relative_ridge_candidate.py`与
`backend/tests/hmm_risk/test_market_relative_ridge_candidate.py`中增加P2-3C独立identity入口、薄CLI mode和直接测试；仅在无法复用既有公开
market helper时最小修改同样已登记的`market_relative_jump_spike.py`及其直接测试。不得新增平行模块/CLI/test文件、修改workflow/catalog、
复制reader、建立通用training/evidence平台或新增依赖。最终changed-file runtime contract必须为`runtime_impact=none`、`target_ids=[]`、
`backend_restart_required=false`；若分类不满足则fail closed并停止本HMM任务，不得在同一PR自行修改流程。

完整计划为：三个fold各一个固定参数market fit=`3`，L1五alpha×三fold=`15`，L2同为`15`；
两level development都通过后才执行market/L1/L2各一个final fit=`3`，总计最多`36` fits。L1失败时在`18/36`停止；L2失败时在
`33/36`停止；market development acceptance失败时在`3/36`停止。不得为了凑满计划执行无意义final fit。

除复用P2-3B的input/target/fit/score/metric/selection/development/collision/readback错误外，至少新增：
`hmm_risk_market_conditioning_identity_mismatch`、`hmm_risk_market_conditioning_regime_unavailable`、
`hmm_risk_market_conditioning_interaction_non_finite`。任一失败写repo-external、collision-safe typed sibling receipt，并保持
candidate/model/READY/database/runtime write为false。不得安装依赖、执行DDL/DML、启动进程或访问holdout。

直接测试至少覆盖：固定lambda4/seed42且不存在隐藏grid；fold-train-only preprocess/center与causal state无carry；既有market acceptance
失败在3/36停止；任一train/validation market state缺失typed失败；十维`[x,m*x]`数值、顺序、hash与risk-on/risk-off斜率公式；禁止独立m列；
interaction identity/non-finite失败；继承target/purge/denominator/state boundary；五alpha全量完成后selection；regime-split指标不参与selection；
L1/L2失败分别在18/36与33/36停止；成功严格36/36并只写compact candidate；collision/readback/unknown failure保留已完成证据；holdout、
model/READY/DB/runtime零副作用；changed-file runtime contract为none。

##### G. 当前设计状态与用户决策边界

P2-3C D1～D6统一状态为`USER_APPROVED_EXACT_CONTRACT_IMPLEMENTATION_NOT_AUTHORIZED`。选择该候选的理由是它以最小十维线性变化直接检验
P2-3B的跨时期斜率假设，同时保留已批准数据、target、产品指标、state语义与holdout。主要false-accept风险是market state恰好解释当前
development folds但无法泛化；主要false-reject风险是硬K=2 state不能表达连续market环境。两个风险只能由预注册development与最终
untouched holdout检验，不能通过并行模型或阈值调整消除。

用户已批准`C-011-P2-3C-D1～D6`精确合同，但未授权源码或实验。在后续取得独立实施授权前不得创建源码实现、运行任何fit、选择alpha、写candidate或进入P2-4。

#### 4.3.4.5 P2-4 untouched holdout 产品验收与状态闭合精确合同（USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED_HOLDOUT_NOT_AUTHORIZED）

本节只展开父蓝图v2.29 Gate 2 P2-4，不改变P2-3C estimator、参数、target、state语义或既有D4/D5数值。它把已经冻结的唯一
development candidate转换为一次、可复核、无reselection的样本外产品裁决。用户已批准D1～D6精确合同并授权最小源码实施；该批准
不授权正式holdout业务数据读取/执行、model/READY写入或任何PR合入。

##### A. C-011-P2-4-D1：唯一candidate authority与一次逻辑holdout边界（USER_APPROVED_EXACT_CONTRACT）

唯一输入固定为：

- candidate=`p2_3c_market_conditioned_ridge_candidate_8ca1b98d.json`，canonical
  `792d4f6ac6b313961eaf5017a0a3ea4a3ebf96ab8364f4ff8518c182a68d17e3`；
- request canonical=`4807125d24a9c01596f923122079c6d70dd48ff39522d3755c6ab0ad09ec6336`，candidate producer=
  `8ca1b98db922489f91814b5d51aae1ab9c59fbd0`，attempt index=`3`；
- planned/completed fits=`36/36`，market=`lambda4/seed42`，L1/L2 selected alpha均为`100.0`，6/6 component receipt hash闭合；
- candidate必须原样声明`holdout_accessed=false`、`product_acceptance_performed=false`、`model_write=false`、`ready_write=false`、
  `database_write=false`、`runtime_action=false`。

P2-4 identity固定为`hmm_risk_p2_4_market_conditioned_ridge_holdout_v1`。holdout state window严格为`2025-04-01..2026-03-31`
的canonical 242个交易日；5/10/20D outcome tail只按同一canonical calendar取holdout decision date之后最多20个交易日，单独保存
state date-set、outcome-only tail date-set、最大日期和source hash。tail不得进入observation、preprocess、fit、selection或state。
candidate中的development source identity仍严格截止`2025-03-31`，不得被改写成已覆盖holdout。P2-4 request必须新增独立
`holdout_source`：沿用同一universe key、universe rule version、security identity manifest、provider absence manifest、PIT hierarchy、
benchmark与feature formula version，但分别冻结holdout feature/state window和outcome-only tail的实际row/date/hash；任一规则或manifest与
candidate不一致即preflight失败。它是同一数据合同的后续时间切片，不是新dataset、重新selection或development source覆盖。
精确同源字段固定为P2-3C request中的`universe_key=shsz_st_pit_qe_dataset_qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2`、
`universe_rule_version=st_pub_next_trade_restore_active_l_v1`、`benchmark_ts_code=000300.SH`、security/provider manifest path+SHA、
`source_start=2022-01-01`与`circ_mv_history_start=2020-07-30`；`source_end`必须等于冻结的outcome tail末日，holdout feature formula SHA必须
等于candidate已持久化的development feature formula SHA。request还必须逐一保存acceptance、acceptance failure、model、READY和两个child及其
failure sibling的八个绝对repo-external输出identity；CLI实际路径必须与request逐项相等，禁止仅靠命令行换路径。
parent在任何holdout业务读取前先闭合candidate、calendar、benchmark、security identity、provider absence、PIT hierarchy、feature formula、
development/holdout/tail date-set与输出identity；preflight失败时`holdout_accessed=false`且不消费逻辑验收。两个child属于同一个
`holdout_evaluation_id=canonical_sha256(contract_version,candidate_report_sha256,holdout_state_date_set_sha256)`，不是两次模型试验。
一旦任一child读取holdout feature/return/outcome，父receipt必须如实写`holdout_accessed=true`；此后任何业务失败都终止该candidate，
不得换alpha、market参数、feature、target、state boundary、level或另建输出路径重试。

##### B. C-011-P2-4-D2：零refit双fresh-process复现与因果推理（USER_APPROVED_EXACT_CONTRACT）

P2-4计划fit数严格为`0`。parent启动两个全新Python process；每个child只读取同一candidate与同一冻结source，不读取另一个child的
中间结果。child必须：

1. 直接加载candidate中的market preprocess、centers、semantic mapping与固定参数；以
   `arrival_cost_policy=zero_at_each_segment_start_no_train_carry`在holdout state window做causal recursion，禁止smoothing、backtracking、
   development terminal state/cost carry或重新fit market；
2. 直接加载L1/L2 development-frozen preprocess、intercept、base/interaction coefficients、risk-on/risk-off slopes与alpha100；只对holdout
   同源五项sector-relative输入应用冻结winsor/z-score，并按`[x,m*x]`生成score；禁止重算quantile/mean/std、重新fit Ridge或读取future outcome
   生成state；
3. 以既有top/bottom 20%且每侧至少5个sector的日度横截面投影生成`trending|neutral|fading`；boundary tie、不可用日期与typed reason沿用
   P2-3C，不补neutral、不缩分母；
4. 保存market state、L1/L2 score/state、transition、warning、metric、coverage、representativeness的shape/dtype/hash和不可替代紧凑摘要。

两个child的model parameter、preprocess、market state、sector score/state、warning/event、D4 metric、D5 coverage与最终state payload canonical
hash必须bitwise相同；数值allclose只允许定位差异，不能替代hash equality。Python/NumPy/SciPy/scikit-learn版本、float64 little-endian
C-order、线程环境和有效threadpool count必须进入child receipt；不一致、非单线程或任一hash漂移均以
`hmm_risk_p2_4_fresh_process_reproducibility_failed` fail closed。该复现是单一已冻结candidate的执行完整性，不是新的模型选择门禁。
parent不得只验证“两份child彼此相等”：每份child的schema/contract/algorithm/producer、candidate SHA、holdout evaluation/source/date-set SHA、
`fit_count=0`、`selection_performed=false`与DB/runtime/model/READY零副作用都必须逐项闭合到parent request；两份相同但偏离parent authority的
self-hashed payload仍必须在任何writer前失败。child readback与candidate/request一样拒绝duplicate JSON key和NaN/Infinity。

##### C. C-011-P2-4-D3：holdout产品指标与通过阈值（USER_APPROVED_EXACT_CONTRACT，沿用既有D4值）

future excess return、average-rank Spearman、spread、sector risk label、warning、Newey-West与risk metric公式严格沿用§4.3.4.2 D4。
正式primary acceptance必须同时满足：

1. L1/L2的10D Rank IC与spread各覆盖至少80%的10D outcome-eligible holdout dates；每个calendar quarter的L2 10D Rank IC覆盖该quarter
   至少80%的outcome-eligible dates；
2. L2 10D mean Rank IC `>=0.02`且Newey-West t-stat `>=1.96`；
3. L2 10D trending-fading spread `>=0.005`且Newey-West t-stat `>=1.96`；
4. L1 10D mean Rank IC `>0`且L1 10D spread `>0`；
5. L1/L2各自risk precision至少比本level risk-event base rate高`0.10`，且recall均`>=0.25`；
6. 四个calendar quarter中至少三个L2 10D mean Rank IC `>0`，且任一quarter不得`<-0.02`。

5D/20D、market-regime-conditioned metric、confidence margin与历史family结果只作secondary diagnostics；不得补足primary、触发reselection或
改变状态。任一identity、分母、coverage、non-finite、Newey-West variance或primary metric不可用均使用typed reason并判定
`NOT_AVAILABLE`，不得写0、借用另一level/quarter或降低阈值。
逐日Rank IC/spread在计算前必须证明state、score与对应horizon outcome的sector identity集合完全相等；禁止先取交集再把缺失sector静默移出
横截面。10D sector risk path identity必须与10D outcome-eligible state identity闭合，缺失时risk metric typed unavailable而不是缩小micro分母。

##### D. C-011-P2-4-D4：coverage、representativeness与互斥三状态（USER_APPROVED_EXACT_CONTRACT，沿用既有D5）

holdout每日canonical denominator固定为L1=`31`、L2=`131`；D4 metric denominator与D5全部242个state-date denominator分开持久化。
L2→L1 hierarchy、size/liquidity统计量与quintile只用完整development facts冻结，公式、80%统计量coverage、排序/tie和group hash严格沿用
§4.3.4.2 D5；holdout期间禁止重算、重分组或按available rows改变成员。

- `FULL_READY`：D1～D3全部通过，且242个holdout date均为L1 `31/31`、L2 `131/131`；
- `COVERAGE_AVAILABLE`：D1～D3全部通过；至少90%的holdout dates达到L1 `>=28/31`和L2 `>=118/131`；每个canonical sector至少80%的
  holdout dates可用；size与liquidity每个quintile的sector-date availability均`>=80%`；每个L1 parent至少90%的holdout dates有一个
  可用L2 child；所有其余项保留typed unavailable；
- `NOT_AVAILABLE`：任一D1～D3、coverage或representativeness条件失败。

三状态严格互斥。`COVERAGE_AVAILABLE`不是READY，不得改名、补neutral、删除分母或隐藏不可用sector；`NOT_AVAILABLE`不得写model或READY。
本状态机是父蓝图v2.25后C-011唯一canonical product authority，不宣称或追认历史B3 legacy/autocycle“两family READY”；历史
F-011-D two-family合同及其blocked证据保持原样，只作旧范式审计，不再作为P2-4必须同时满足的第二套active门禁。

##### E. C-011-P2-4-D5：最小artifact、writer、readback与副作用（USER_APPROVED_EXACT_CONTRACT）

schemas固定为：request=`hmm_risk_p2_4_holdout_request_v1`、child=`hmm_risk_p2_4_holdout_child_v1`、parent acceptance=
`hmm_risk_p2_4_holdout_acceptance_v1`、canonical model=`hmm_risk_market_conditioned_ridge_model_v1`、FULL_READY marker=
`hmm_risk_market_conditioned_ridge_ready_v1`。所有路径必须绝对、repo-external、请求显式给出且collision-safe；禁止default/latest、覆盖开关或
扫描任意artifact root选candidate。

parent先验证两个child bitwise一致并形成immutable acceptance core。为避免model/READY与最终acceptance互相引用形成循环，唯一写入顺序固定为：
先写含`acceptance_core_sha256`且声明`activation_requires_matching_final_acceptance=true`的model；FULL_READY再写同样绑定core与model SHA的READY；
最后一次性写包含实际model/READY SHA的final acceptance。随后必须从磁盘重新读取全部适用artifact并验证acceptance core、candidate、availability、
model SHA和READY SHA的双向闭合；任一final acceptance缺失或bundle不闭合时，先前model/READY均不可消费，并写如实记录已发生side effect的typed
failure sibling。`NOT_AVAILABLE`不经过model/READY步骤，只写final acceptance/failure。

- `FULL_READY`：写一个canonical model与一个READY marker；
- `COVERAGE_AVAILABLE`：写一个带明确availability state/coverage manifest的canonical model，`ready_write=false`且不写READY marker；
- `NOT_AVAILABLE`：只写compact acceptance/failure receipt，`model_write=false,ready_write=false`。

FULL_READY marker只表示该离线canonical model通过P2-4并可被P2-5同一模型identity消费；它不表示P2-5单日预测、P2-6 API/UI、P2-7日任务、
production deployment、runtime activation或服务重启已经完成或获得授权。

model只包含线上/离线预测不可替代的market preprocess/centers/mapping、L1/L2 preprocess/coefficients/state projection、source/formula/hierarchy
identity与coverage manifest；不复制完整训练/holdout输入、逐行score或全部历史path。acceptance保存D3/D4完整分子分母、季度/sector/quintile摘要、
child hashes和最终state。writer必须首次写入、canonical回读并验证hash；collision/readback失败只允许首次写同目录不同identity的typed failure
receipt，不得覆盖已有内容。整个P2-4保持`database_write=false,runtime_action=false`；DDL/DML、依赖安装、服务重启、runtime activation与
P2-5均不属于本合同。

##### F. C-011-P2-4-D6：最小源码、直接测试、failure与停止（USER_APPROVED_EXACT_CONTRACT）

最小实现候选只新增：

- `backend/services/hmm_risk/market_relative_ridge_holdout.py`；
- `scripts/hmm_risk/run_market_relative_ridge_holdout.py`；
- `backend/tests/hmm_risk/test_market_relative_ridge_holdout.py`。

它复用既有P2-3C reader、canonical serializer、metric/state helper与final component schema；不得修改workflow/catalog、建立通用evaluation/model
registry/scheduler、增加DB表或依赖。changed-file runtime contract必须为`runtime_impact=none,target_ids=[],backend_restart_required=false`，
否则fail closed并登记独立流程BUG。

稳定reason code至少区分：candidate/request/source/holdout identity mismatch、holdout preflight failed、causal state failed、score/state non-finite、
metric unavailable、coverage contract failed、representativeness failed、fresh-process mismatch、output collision、readback mismatch和unknown execution
failure；unknown必须保留exception type/stage，不得压为`incomplete`。直接测试至少覆盖：

- exact candidate/hash/flags与一次逻辑evaluation identity；preflight失败时holdout未访问；
- request逐项绑定冻结source policy、feature formula与八个输出identity，CLI换路径在holdout读取前失败；
- 0 fit、0 reselection、冻结preprocess/parameter与causal zero-start；禁止development carry、smoothing和future state input；
- 两fresh process bitwise一致、单份child到parent authority闭合及各类identity/hash/线程漂移fail closed；
- D3每个阈值边界、逐日identity集合相等、coverage/quarter/Newey-West/risk分母、typed unavailable与secondary不替代primary；
- D4三状态互斥、canonical denominator、quintile/parent代表性、COVERAGE_AVAILABLE不写READY；
- FULL_READY/COVERAGE_AVAILABLE/NOT_AVAILABLE三条writer、final bundle双向闭合、collision、readback、unknown failure与zero DB/runtime副作用；
  failure receipt必须分开记录`holdout_accessed`与`product_acceptance_performed`，不得用“已读取”冒充“已完成产品验收”。

用户已批准上述精确合同并授权创建上述源码；实现PR仍须按changed files路由只运行hmm.risk直接计划。该授权不包括运行child、读取正式holdout、
写model/READY或合入PR；PR合入与正式holdout执行分别授权。P2-4失败不得回流P2-3或自动开启新模型方向。

##### G. C-011-P2-4-D1～D6 取舍、成本与停止结论（USER_APPROVED_EXACT_CONTRACT）

| 合同 | 主要价值 | false-accept风险 | false-reject风险 | 成本与控制 |
|---|---|---|---|---|
| D1唯一candidate/一次逻辑holdout | 防止重复试验和holdout调参 | 若允许另路径重跑会产生选择偏差，故禁止 | holdout读取后的执行缺陷也会终止有效candidate | preflight在业务读取前完成；访问后失败如实NOT_AVAILABLE，不自动重试 |
| D2双fresh-process/0 fit | 证明冻结参数可独立复现 | 单process偶然成功被误认稳定 | benign BLAS/thread差异也可能被bitwise规则拒绝 | 同host固定环境与单线程；两个child各一次source scan，不增加fit |
| D3既有产品阈值 | 直接裁决轮动区分和风险提示 | 多指标偶然同时通过 | 低频风险事件或242日窗口可能拒绝真实弱信号 | 不调阈值、不增加多重比较；失败即NOT_AVAILABLE |
| D4 coverage/代表性 | 不让局部可用样本冒充全产品 | 90%/80%边界内仍可能存在结构偏差 | 稀有sector/provider absence可能导致拒绝 | 保留31/131分母、sector/quintile/parent分解和typed unavailable |
| D5最小writer | 只保存P2-5直接需要的模型与状态 | coverage model被误读为READY | writer/readback故障阻断已通过验收的candidate | COVERAGE_AVAILABLE明确ready=false；首次写入、hash回读、无DB/runtime |

唯一建议仍是D1～D6整体批准或整体不实施，禁止只批准较宽松的某个指标/coverage分支。若正式P2-4状态为`NOT_AVAILABLE`，Gate 2模型方向
按父蓝图停止并向用户报告；不得自动开启P2-3D、修改阈值或建设新平台。若为`FULL_READY`或`COVERAGE_AVAILABLE`，下一步只进入P2-5
最小单日离线预测纵切，仍不直接进入API/UI、日任务或runtime。

#### 4.3.4.6 C-012：P2-4 后 capability-aligned product bundle（BLUEPRINT_DIRECTION_USER_AUTHORIZED_EXACT_F2_CONTRACT_PENDING）

本节同步父蓝图v2.31的产品架构方向，只定义下一份精确F2合同必须闭合的边界；它不修改已经终结的C-011-P2-4结果，不批准新模型、
阈值、fit、selection、holdout、model/READY、API/UI、DB或runtime。

##### A. C-012-D1：canonical bundle与能力边界

canonical authority必须是一个versioned product bundle，而不是强制单一estimator承担全部任务。bundle最少包含共享PIT/security/hierarchy/source
identity、market-regime component、rotation component、risk-alert component和availability manifest。能力轴固定为
`rotation_L1|rotation_L2|risk_L1|risk_L2`；每个component分别保存model/algorithm/source identity与因果输入边界，不得借用另一能力的
指标、状态或fallback形成成功。允许不同能力使用不同但预注册且独立验收的estimator；是否共享参数、特征或market component必须在后续精确
合同中明示，禁止实现自行组合。

##### B. C-012-D2：能力验收、coverage与顶层状态

四层验收继续保留，但按能力分别闭合：数值安全 → semantic/output validity → 样本外产品效果 → coverage/representativeness。
rotation只以预注册横截面Rank IC、spread、时间稳定性和coverage裁决；risk只以预注册事件定义、precision lift、recall、误报/漏报、
时间稳定性和coverage裁决。顶层状态为：

- `FULL_READY`：四个批准能力全部通过；
- `CAPABILITY_AVAILABLE`：至少一个明确命名能力通过，所有未通过能力显式`NOT_AVAILABLE`；
- `NOT_AVAILABLE`：没有达到下一精确合同定义的最低产品能力。

每个能力另记录`FULL_COVERAGE|COVERAGE_AVAILABLE|INSUFFICIENT_COVERAGE`。coverage只说明预测可用范围，不证明产品效果；
`CAPABILITY_AVAILABLE`不等于FULL_READY，也不增加F-011/F-013完整完成计数。未来精确阈值必须由新的F2决策批准，当前旧P2-4结果不按新状态追认。

##### C. C-012-D3：risk identity、abstention与失败语义

risk acceptance必须在完整canonical sector-date/event denominator上分别保存：prediction∩outcome、prediction-only、outcome-only和both-unavailable
identity/count/hash。recall分母不得删除真实risk event；precision只消费真实warning prediction，同时报告prediction coverage与abstention rate。
identity漂移、非有限值或分母不足继续typed fail closed，禁止静默inner join、补negative/neutral或把abstention计为正确预测。旧P2-4 L2
`actual_identity_count=31627`、`expected_identity_count=31615`继续按旧合同判metric unavailable；本修订不追认其risk能力。

##### D. C-012-D4：development、walk-forward与新holdout治理

`2025-04-01..2026-03-31`已被C-011-P2-4正式读取，只能作为历史样本外证据。任何新component必须预先冻结development/walk-forward folds、
选择指标、参数空间、停止条件和一个新的untouched验收边界；不得使用旧holdout结果调参后再次把该窗口称为untouched。新最终窗口的日期、最小
样本/事件数、purge/embargo和多重试验处理属于下一精确F2合同，本文不自行发明阈值。若尚无足够新数据，状态保持pending/NOT_AVAILABLE，
不能用历史窗口伪造最终验收。

##### E. C-012-D5：G2-A首个真实产品oracle

至少一个能力正式达到`CAPABILITY_AVAILABLE`后，G2-A必须继续消费同一bundle生成一个完整历史交易日的真实结果：market regime、31个L1板块的
rotation score/state、所有能力状态、coverage/abstention、validation basis、forward confirmation status和中文可读reason。日期只能由同一冻结HR1
request与input-complete canonical calendar机械派生并写入identity，不得读取运行时latest或人工挑选表现日期。
同一闭环必须把该结果写入最小必要repository，通过既有`overview`/`heatmap`真实read API读取，并在`/hmm-risk`呈现可访问的L1轮动热力图、
缺失cell、完整分母与失败原因；验收使用真实backend和无mock浏览器流程。`rotation_L2|risk_L1|risk_L2`必须显式返回typed `NOT_AVAILABLE`，
不得隐藏模块、输出空成功、复用rotation state伪造risk warning或把该纵切报告为FULL_READY、完整F-013或Phase 2完成。若rotation_L1未达到
`CAPABILITY_AVAILABLE`，闭环以`NOT_AVAILABLE`结束且不生成静态/伪造产品。该oracle不构成runtime activation，也不授权日任务。

##### F. C-012-D6：单一下一候选与反过度工程停止条件

下一精确合同一次只能选择一个直接解除`rotation_*`或`risk_*` blocker的component假设，必须列出最小替代方案、fit成本、false-accept、
false-reject、成功/失败停止条件和不做的业务影响。禁止四能力并行搜索、通用model/evidence/training平台、历史artifact迁移、重复完整输入物化、
无界诊断或承诺指定estimator必然FULL_READY。候选失败即把相应能力保留`NOT_AVAILABLE`并返回用户决策；不得自动打开下一模型方向。

当前连续状态为：`P2_4_FORMAL_NOT_AVAILABLE -> C012_BLUEPRINT_DIRECTION_APPROVED -> C012_RL1_EXACT_F2_CONTRACT_USER_APPROVED -> SOURCE_IMPLEMENTED_VERIFIED_PENDING_FORMAL_EXECUTION`。
实现、实验与产品纵切均未授权，严格产品进度仍为`11/17=64.71%`。

#### 4.3.4.7 C-012-RL1：rotation_L1 单一component精确合同（USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTED_VERIFIED_PENDING_FORMAL_EXECUTION）

本节只把父蓝图v2.31已经批准的capability方向收敛为一个可审计候选，不追认旧P2-4成功，也不授权源码、fit、holdout读取、model/bundle、
API/UI、数据库或runtime。选择L1 rotation的唯一依据是旧P2-4中该能力的directional gate通过，而risk_L1、rotation_L2与risk_L2均未通过
原完整合同；旧结果因此只用于选择下一研究能力和development协议，不能成为新candidate或最终验收结果。

##### A. C-012-RL1-D1：component identity、estimator、输入与非目标（USER_APPROVED_EXACT_CONTRACT）

唯一component identity固定为`hmm_risk_rotation_l1_market_conditioned_ridge_v1`：

contract version固定为`C-012-RL1-D1-D6`，algorithm version固定为`hmm_risk_rotation_l1_market_conditioned_ridge_v1`；任何后续公式、
feature、fold、threshold或window变化必须使用新version并重新批准，禁止原地漂移。

1. 只交付`rotation_L1`能力候选；`rotation_L2|risk_L1|risk_L2`保持typed `NOT_AVAILABLE`。该状态是父蓝图批准的能力分解，不是
   L1-only FULL_READY、Phase 2完成或对未实现能力的删除；
2. market component固定K=2 causal jump model、`jump_penalty=4.0`、`seed=42`、既有五项market feature与
   `arrival_cost_policy=zero_at_each_segment_start_no_train_carry`；每个fold和最终development必须重新fit，禁止复制旧centers/path；
3. L1 estimator固定Ridge：`alpha=100.0,fit_intercept=true,solver="svd",positive=false,copy_X=true,tol=1e-4,max_iter=null,
   random_state=null`。不再搜索alpha，不执行per-sector fit，不增加第二候选；
4. 输入严格复用五项L1 relative feature及五项`market_sign × feature`交互，顺序、PIT/t-1、level-global train-only 1%/99% winsor、
   population z-score、formula/security/provider-absence/hierarchy identity均与P2-3C一致；不新增PCA、树、t-emission、deep、ensemble或新数据；
5. target固定为同日L1横截面中心化的10D future relative excess return；score越高仍表示未来相对走强。state仍按当日available L1 score
   top/bottom 20%映射`trending|fading`，其余`neutral`，每侧至少5个sector且boundary tie不得按code强拆；
6. 该component不产生risk warning/severity，不将`fading`直接冒充risk event，也不改变market `risk_on|risk_off`为sector风险标签。

##### B. C-012-RL1-D2：development与五fold walk-forward合同（USER_APPROVED_EXACT_CONTRACT）

development observation window固定为`2022-01-04..2026-03-31`。原`2025-04-01..2026-03-31`已消费holdout降为development证据，必须以
新component identity重新生成fold-4/fold-5预测，不得复制旧P2-4 score/state。anchored expanding folds固定为：

| fold | train observation | validation state | target purge |
|---|---|---|---|
| fold-1 | `2022-01-04..2023-09-01` | `2023-09-04..2024-03-14` | train与validation各自最后10个canonical open days只生成state、不生成10D target/metric |
| fold-2 | `2022-01-04..2024-03-14` | `2024-03-15..2024-09-18` | 同上 |
| fold-3 | `2022-01-04..2024-09-18` | `2024-09-19..2025-03-31` | 同上 |
| fold-4 | `2022-01-04..2025-03-31` | `2025-04-01..2025-09-30` | 同上 |
| fold-5 | `2022-01-04..2025-09-30` | `2025-10-01..2026-03-31` | 同上 |

每个fold的canonical calendar/date-set/count/hash在request preflight冻结；`purge=10 canonical open days,embargo=0`。train Ridge只使用future horizon
完整落在该train segment内的target rows；validation metric只使用future horizon完整落在该validation segment内的state-date，禁止跨segment借tail。
state只消费当日及以前observation，10D target只用于离线metric。validation不得fit preprocess/market/Ridge，不carry train末market state或arrival cost。
五fold全部完成后才验收，不early stop选择fold，也不读取新holdout。Rank IC、top-bottom spread和Newey-West variance/t-stat严格复用§4.3.4.5
C-011-P2-4-D3的average-rank Spearman、等权组收益及重叠10D horizon公式，不建立第二套metric实现。

development acceptance同时要求：

- 每fold Rank IC与spread available dates各达到该fold outcome-eligible dates的`>=80%`；daily denominator至少`28/31`；
- 至少`4/5`个相同fold同时满足mean Rank IC `>0`且mean trending-fading spread `>0`；
- 五fold mean Rank IC的median `>=0.02`，五fold mean spread的median `>=0.003`；
- 将五fold不重叠validation daily rows按日期拼接后，Rank IC和spread的Newey-West t-stat均`>=1.645`，lag固定为`9`；
- metric、variance、t-stat、state、score与identity全部finite/完整；任何不可用、重复日期、非有限或阈值失败均
  `NOT_AVAILABLE_FOR_PROMOTION`，不得删除fold、换alpha、改feature或只取正向时期。

全部通过后才在`2022-01-04..2026-03-31`完整development上各执行一次market与L1 final fit，写
`ROTATION_L1_CANDIDATE_FROZEN_PENDING_NEW_HOLDOUT`。该candidate不是component model、CAPABILITY_AVAILABLE或READY。

##### C. C-012-RL1-D3：全新untouched holdout与产品阈值（USER_APPROVED_EXACT_CONTRACT）

唯一新holdout state window固定为`2026-04-01..2026-09-30`；10D outcome-only tail延伸到`2026-09-30`之后第10个canonical open day。
在state window和完整tail均可由同一PIT/source合同冻结前，holdout必须保持不可读。preflight冻结准确trading-day count、state/outcome date-set、
source/security/provider-absence/formula hash；日期或source身份不闭合时在首次业务读取前失败。旧P2-3C/P2-4 source identity截止到
`2026-06-30`，不能覆盖本窗口；新holdout必须使用一个显式versioned source revision，分别证明development只截止`2026-03-31`、state覆盖
`2026-04-01..2026-09-30`且outcome tail完整。缺失revision、tail或hash时保持`holdout_accessed=false`并fail closed，不回退旧dataset、当前DB max date
或部分Q2数据。

candidate参数、preprocess、market centers/mapping与Ridge coefficient在读取holdout前冻结；holdout为一次逻辑evaluation、0 fit、0 selection、
0 threshold change。primary 10D acceptance同时要求：

- Rank IC与spread各覆盖`>=80%` outcome-eligible dates；2026-Q2、2026-Q3各自覆盖`>=80%`；
- mean Rank IC `>=0.02`且Newey-West t-stat `>=1.645`（lag=`9`）；
- mean trending-fading spread `>=0.003`且Newey-West t-stat `>=1.645`（lag=`9`）；
- 2026-Q2与2026-Q3各自mean Rank IC `>0`且mean spread `>0`；
- score/state/outcome sector identity逐日完全相等；禁止先取交集、借用另一level、使用future input、补neutral或删除不可用日期。

5D/20D、market-regime split、confidence margin和旧P2-4结果只作secondary diagnostics，不能补足primary或触发reselection。若tail尚未完整、
有效日期不足或任一primary条件失败，状态保持`ROTATION_L1_NOT_AVAILABLE`；不得缩短窗口、延长窗口直到通过或打开第二candidate。

阈值依据与取舍固定为：

| threshold | 选择依据 | 主要false-accept | 主要false-reject |
|---|---|---|---|
| Rank IC `>=0.02` | 沿用父合同对可用横截面预测的最小效果量，不以“仅大于0”接受微弱结果 | 多次相关比较偶然为正 | 真实但弱于0.02的轮动信息被拒绝 |
| spread `>=0.003` | 10D L1极端组至少30bps的研究效果量；L1仅31个板块，低于旧L2 50bps但不是按旧结果取等值 | 不代表可交易成本后收益 | 分散轮动时期可能不足30bps |
| NW t-stat `>=1.645` | 单一、预注册、方向为正的5% one-sided检验；lag 9匹配10D重叠horizon | 非正态/结构突变可能低估不确定性 | 六个月样本对阶段性信号功效不足 |
| 4/5 folds与Q2/Q3均正 | 容忍一个历史regime失败，但禁止多数时期或任一新季度方向反转 | fold相关性降低独立证据量 | 单一异常季度会拒绝长期有效信号 |

##### D. C-012-RL1-D4：coverage、abstention与bundle状态（USER_APPROVED_EXACT_CONTRACT）

holdout每日canonical denominator固定为L1=`31`，所有availability以31×state-date完整分母报告：

- `FULL_COVERAGE`：全部state dates均`31/31`可用；
- `COVERAGE_AVAILABLE`：至少90%的state dates达到`>=28/31`，且每个canonical L1 sector至少80%的state dates可用；
- `INSUFFICIENT_COVERAGE`：任一上述条件失败。

每个可用state还必须通过既有C-010 contributor ledger、同源moneyflow coverage、逐feature cross-section及provider-absence语义；这些是输入有效性，
不得被31个sector code表面齐全替代。本文不新增或放宽C-010阈值，只要求新source revision逐项回读同一合同。

只有D1～D3产品指标通过且coverage为`FULL_COVERAGE|COVERAGE_AVAILABLE`时，能力状态才为`rotation_L1=AVAILABLE`，顶层bundle为
`CAPABILITY_AVAILABLE`。bundle必须同时声明`rotation_L2|risk_L1|risk_L2=NOT_AVAILABLE`及reason，`ready=false`且不写FULL_READY marker。
prediction-only、outcome-only、both-unavailable identity/count/hash和abstention rate全部保留；coverage不足不能被产品指标补足，产品指标失败也不能由
full coverage补足。

##### E. C-012-RL1-D5：最小artifact、writer与readback（USER_APPROVED_EXACT_CONTRACT）

schema identity固定为：

- development request=`hmm_risk_rotation_l1_candidate_request_v1`；
- development report=`hmm_risk_rotation_l1_candidate_report_v1`；
- holdout request=`hmm_risk_rotation_l1_holdout_request_v1`；
- holdout child=`hmm_risk_rotation_l1_holdout_child_v1`；
- holdout acceptance=`hmm_risk_rotation_l1_holdout_acceptance_v1`；
- component model=`hmm_risk_rotation_l1_component_model_v1`；
- capability bundle=`hmm_risk_capability_bundle_v1`。

所有输出必须repo-external、显式绝对路径、append-only、collision-safe并使用canonical JSON拒绝duplicate key与NaN。

development只写compact candidate或typed failure；不复制完整输入、逐行训练矩阵或旧artifact。holdout通过后固定写入顺序为component model →
bundle(`CAPABILITY_AVAILABLE`,`ready=false`) → final acceptance。parent在任何model写入前先形成不含component/bundle最终SHA的immutable
`acceptance_core_sha256`；component与bundle都绑定该core，final acceptance再绑定实际component/bundle SHA，避免循环引用。随后从磁盘回读并验证
candidate/core/component/bundle/final acceptance双向hash闭合。
holdout失败只写acceptance/failure，`model_write=false,bundle_write=false,ready_write=false`。writer/readback失败必须记录已发生side effect并使全部未闭合
artifact不可消费；全流程`database_write=false,runtime_action=false`。

##### F. C-012-RL1-D6：成本、复现、最小实施与停止条件（USER_APPROVED_EXACT_CONTRACT）

一个development process严格执行五fold×(market fit 1 + L1 Ridge fit 1) + full-development final fit 2=`12` fits；两个fresh Python process
独立执行同一request，总计划=`24` fits。两个process的fold metric、final parameter、preprocess、calendar/source identity与candidate payload canonical hash
必须bitwise一致；allclose只用于诊断。正式holdout仍为两个fresh process、每个0 fit，state/score/metric/coverage payload必须bitwise一致。
所有development/holdout receipt必须显式写`selection_performed=false`、`parameter_search_performed=false`；固定alpha/lambda/seed不构成运行时选择。

后续实现若获授权，只允许修改既有：

- `backend/services/hmm_risk/market_relative_ridge_candidate.py`；
- `backend/services/hmm_risk/market_relative_ridge_holdout.py`；
- `scripts/hmm_risk/run_market_relative_ridge_candidate.py`；
- `scripts/hmm_risk/run_market_relative_ridge_holdout.py`；
- 两个对应直接测试文件及本设计。

不得新增estimator registry、第二训练入口、数据库schema、依赖、scheduler或通用evidence平台。直接测试必须覆盖固定参数/feature/target、五fold边界与
purge、24-fit完整性、4/5与median/NW阈值边界、fold-4/5不复制旧score、holdout读取前preflight、新window/tail、Q2/Q3阈值、31分母、
coverage/abstention、三项未实现能力、writer/readback/collision、双fresh-process和DB/runtime零副作用。

稳定reason code至少包括：

- `hmm_risk_rotation_l1_input_identity_mismatch`；
- `hmm_risk_rotation_l1_development_fold_invalid`；
- `hmm_risk_rotation_l1_development_effect_unavailable`；
- `hmm_risk_rotation_l1_new_holdout_not_ready`；
- `hmm_risk_rotation_l1_holdout_access_forbidden`；
- `hmm_risk_rotation_l1_metric_unavailable`；
- `hmm_risk_rotation_l1_coverage_insufficient`；
- `hmm_risk_rotation_l1_fresh_process_mismatch`；
- `hmm_risk_rotation_l1_output_collision`；
- `hmm_risk_rotation_l1_readback_mismatch`；
- `hmm_risk_rotation_l1_unexpected_error`。

unknown failure必须保存exception type与stage，不得压缩为generic incomplete。未来changed-file contract必须为
`runtime_impact=none,runtime_files=[],target_ids=[],backend_restart_required=false`；分类为unknown/runtime或引入未列文件时停止并按真实范围重新设计，
不得在同一HMM实现中修改workflow/catalog来降级影响。

主要false-accept风险是基于已观察旧P2-4结果选择L1路径且五fold共享市场时期；通过新的未来holdout、固定单候选与一次读取控制。主要
false-reject风险是六个月窗口和双季度正向要求可能拒绝真实但阶段性弱的信号；失败必须如实NOT_AVAILABLE，不通过延长/换模型修复。
总训练成本24 fits、holdout 0 fit，远低于历史大矩阵且直接服务F-011/P2-5。D1～D6须整体批准或整体不实施；任一失败后停止该component并返回用户，
不得自动进入rotation_L2或risk模型。

当前状态：`C-012-RL1-D1～D6 = RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTED_VERIFIED_PENDING_FORMAL_24_FIT_AND_NEW_HOLDOUT`。

#### 4.3.4.8 C-012-RL1-HR1：历史因果回放正式能力验收修订（FORMAL_EXECUTED_ROTATION_L1_NOT_AVAILABLE）

本节是§4.3.4.7的前瞻性修订，不追改D1～D6的历史批准和源码实施事实。用户于2026-08-24明确要求长周期验证通过历史回放实现，禁止以等待
2026-09-30阻断产品研发。HR1仅改变“何种已批准证据足以形成advisory-only capability”及后续forward confirmation状态；estimator、feature、target、
fold、threshold、coverage、24-fit预算和失败停止语义保持不变。HR1不是把已消费P2-4结果重新命名，也不是降低阈值、增加第二candidate或把历史回放冒充untouched。

##### A. C-012-RL1-HR1-D1：模型、输入与选择冻结（USER_APPROVED_EXACT_CONTRACT）

1. component algorithm仍为`hmm_risk_rotation_l1_market_conditioned_ridge_v1`；market K=2、`jump_penalty=4.0`、`seed=42`，Ridge
   `alpha=100.0`及其全部参数、十维L1输入、10D centered target、score方向与top/bottom 20%三状态映射全部沿用§4.3.4.7 D1；
2. HR1 contract version固定为`C-012-RL1-HR1-D1-D6`。只有验收basis、artifact closure和forward状态变化；算法版本不变；
3. 禁止alpha/seed/lambda/feature/window search、per-sector fit、第二candidate、ensemble、旧holdout reuse及看到回放结果后的合同修改；
4. `rotation_L2|risk_L1|risk_L2`继续typed `NOT_AVAILABLE`，rotation_L1回放通过不得推导FULL_READY、risk warning或交易决策能力。

##### B. C-012-RL1-HR1-D2：五fold逐日历史因果回放（USER_APPROVED_EXACT_CONTRACT）

§4.3.4.7 D2的五个anchored expanding folds整体升级为正式`HISTORICAL_CAUSAL_WALK_FORWARD`验收，边界、purge、metric公式和阈值不变：

- 每fold只用train segment拟合market、preprocess和Ridge；validation从独立arrival cost开始，逐日只消费当日及以前t-1/PIT observation；
- 10D future outcome只在预测日之后用于离线metric，train/validation末10个canonical open days不跨segment借tail；
- 五fold必须全部执行，不early stop；两个fresh process各12 fits、合计24 fits；selection和parameter search均为false；
- 每foldRank IC与spread metric-date coverage各`>=80%`且daily denominator至少`28/31`；至少相同`4/5` folds两者均`>0`；
- 五foldmean Rank IC median `>=0.02`、mean spread median `>=0.003`；拼接不重叠OOF daily rows后两者Newey-West t-stat均`>=1.645`、lag=`9`；
- fold metric、final parameter、preprocess、calendar/source identity、availability/coverage payload与candidate canonical hash须跨两进程bitwise一致。

任何一项失败均为`ROTATION_L1_NOT_AVAILABLE`，不得通过删除fold、重跑直到通过、换参数、改阈值或组合局部成功修复。该结论是模型/产品结果，不自动登记程序BUG；
只有输入、实现、writer或复现合同被违反时才进入单独BUG流程。

##### C. C-012-RL1-HR1-D3：回放coverage、abstention与产品验收（USER_APPROVED_EXACT_CONTRACT）

正式回放以五fold不重叠validation state dates的并集为完整分母，并复用§4.3.4.7 D4，不新增数值门禁：

- 每日canonical L1 denominator固定31；`FULL_COVERAGE`要求所有state dates为31/31；
- `COVERAGE_AVAILABLE`要求至少90%的state dates达到`>=28/31`且每个canonical L1 sector至少80%的state dates可用；其余为`INSUFFICIENT_COVERAGE`；
- 每个可用state仍须通过C-010 contributor ledger、同源moneyflow coverage、逐feature cross-section、security identity及provider-absence合同；
- prediction-only、outcome-only、both-unavailable identity/count/hash与abstention rate须按fold和聚合总表完整记录；不得先取交集、补neutral或隐藏不可用sector；
- 只有D2全部产品指标通过且coverage为`FULL_COVERAGE|COVERAGE_AVAILABLE`时，rotation_L1才可AVAILABLE。coverage不能补足产品指标，产品指标也不能补足coverage。

##### D. C-012-RL1-HR1-D4：能力、basis与bundle状态（USER_APPROVED_EXACT_CONTRACT）

回放通过时顶层状态固定为：

```json
{
  "status": "CAPABILITY_AVAILABLE",
  "validation_basis": "HISTORICAL_CAUSAL_WALK_FORWARD",
  "forward_confirmation_status": "PENDING",
  "daily_prediction_status": "RESEARCH_ONLY_PENDING_FORWARD",
  "historical_analysis_available": true,
  "ready": false,
  "rotation_L1": "AVAILABLE",
  "rotation_L2": "NOT_AVAILABLE",
  "risk_L1": "NOT_AVAILABLE",
  "risk_L2": "NOT_AVAILABLE"
}
```

该bundle允许P2-5/P2-6生成advisory-only单日预测、历史分析及API/UI展示，不授权交易、Paper、Selection、strategy package或runtime activation。所有展示必须同时显示
`validation_basis`和`forward_confirmation_status`，不得以“已验证”省略历史回放限定。回放失败时不写component/bundle；只写typed acceptance/failure，顶层保持
`NOT_AVAILABLE`。严格产品进度只有在真实bundle生成并被P2-5消费后才按父蓝图验收项更新，不以源码、fit数量或receipt数量增加。

##### E. C-012-RL1-HR1-D5：最小artifact与writer/readback（USER_APPROVED_EXACT_CONTRACT）

为避免旧development/holdout envelope与新验收authority混用，HR1新增且仅新增以下versioned schema：

- replay request=`hmm_risk_rotation_l1_replay_request_v1`；
- replay child=`hmm_risk_rotation_l1_replay_child_v1`；
- replay acceptance=`hmm_risk_rotation_l1_replay_acceptance_v1`；
- component model=`hmm_risk_rotation_l1_component_model_v2`；
- capability bundle=`hmm_risk_capability_bundle_v2`。

replay request冻结既有development source、calendar/folds、算法参数、C-010 identity及全部输出authority；replay child只封装现有12-fit process的fold/final/coverage payload，
不得改变其计算公式。父CLI仍复用现有`--candidate-mode c012-rl1`入口，通过request中的HR1 contract/version选择新closure，不新增第二训练入口。不得复制完整输入、逐行训练矩阵、
旧P2-4 artifact或新建通用registry。
replay request的`outputs`对象必须精确包含且只包含`acceptance_core_path|acceptance_path|component_model_path|capability_bundle_path|child_dir|failure_path`六个绝对repo-external路径；
六者必须互异并处于request声明的artifact root内。CLI在HR1模式增加`--acceptance-core-output|--model-output|--bundle-output`，现有`--output|--child-dir`继续使用；所有CLI实参必须与request逐项相等，
failure path只由request派生且不得与成功输出重合。任一路径缺失、额外、自哈希改写、越界或CLI漂移均在fit前fail closed。
父CLI以`--prepare-request --source-authority <path>`执行唯一正式request制备：只读取source authority中的`source`对象，由程序固定development end、计算calendar/fold、dataset/mapping/database、C-010 identity和全部输出authority后append-only写request；禁止人工拼装request或新增第二训练入口。
当前正式制备允许`source-authority`为显式旧版`source`对象，或仓库跟踪的`pit_v2_source_freeze_receipt_v2`；后者必须逐项匹配canonical v2 profile路径及文件SHA、`aistock_equity_pit_canonical_v2`与`shsz_a_252td_st_delist_asof_v2`常量，并由当前tracked security/provider manifest计算canonical hash。禁止按数据库“最新一行”自动选universe，source preflight失败必须在0 fit回执中保留安全、可操作的底层reason message。
正式写入顺序为replay acceptance core → component v2 → bundle v2 → final replay acceptance；core不含循环依赖的最终SHA，component/bundle绑定core，final再绑定实际component/bundle SHA。
所有输出repo-external、绝对路径、append-only、collision-safe、canonical JSON duplicate-key/NaN拒绝，并从磁盘双向回读闭合。任何writer/readback失败保留已发生side effect，
未闭合artifact不可消费；`database_write=false,runtime_action=false,ready_write=false`。

##### F. C-012-RL1-HR1-D6：非阻塞forward confirmation与执行停止（USER_APPROVED_EXACT_CONTRACT）

未来窗口`2026-04-01..2026-09-30`及其后第10个canonical open-day outcome tail保留为同一冻结component的一次0-fit、0-selection forward confirmation，
但不再作为P2-5/P2-6前置条件：

- tail未完整时保持`PENDING`且holdout不可读，不得用当前DB max date、部分Q2/Q3或旧source填充；
- 通过原D3产品与coverage合同后写append-only confirmation receipt：`forward_confirmation_status=PASSED,daily_prediction_status=ADVISORY_AVAILABLE`，不重写模型参数；
- 任一primary或coverage条件失败时写append-only confirmation receipt：`forward_confirmation_status=FAILED,daily_prediction_status=DISABLED_FORWARD_FAILED`，停止新的日常预测输出并保留
  `historical_analysis_available=true`、原replay bundle、失败reason及完整分母；不得重fit、reselect、改阈值、延长窗口直到通过或打开第二candidate；
- 初始replay bundle固定`forward_confirmation_status=PENDING,daily_prediction_status=RESEARCH_ONLY_PENDING_FORWARD,historical_analysis_available=true`；P2-5/P2-6可消费，
  P2-7若在forward完成前运行，只能发布带相同标记的advisory research输出；
- `PENDING`期间只允许advisory-only research输出且必须可见标注，不能进入交易决策链；`FAILED`不能被历史回放结果覆盖成成功。

HR1源码只允许修改§4.3.4.7 D6已登记的两个service、两个CLI、两个直接测试文件及本设计；runtime contract必须继续为
`runtime_impact=none,runtime_files=[],target_ids=[],backend_restart_required=false`。正式24-fit回放可在源码合入后由独立实验worktree后台启动；启动回执只验证commit、request、
process和输出authority，后续进度由用户手工触发只读检查，不新增monitor service。任一设计、实现、复现或回放失败时按typed状态停止，不自动进入其他模型或能力。

主要false-accept风险是rotation_L1方向及固定alpha曾受旧P2-4研究结果影响，因此五fold不是从未被研究过程观察的untouched证据；HR1必须永久披露
`validation_basis=HISTORICAL_CAUSAL_WALK_FORWARD`，不允许省略为generic validated，并由未来单次forward confirmation检验泛化。主要false-reject风险是
固定4/5、median、NW及coverage合同可能拒绝阶段性有效能力；失败仍保持NOT_AVAILABLE，不通过放宽数值、换fold或增加candidate处理。该风险取舍是用户为避免等待自然日期而批准的
advisory-only产品路径，不改变F-012研究隔离。

当前状态：`C-012-RL1-HR1-D1～D6 = FORMAL_EXECUTED_STOPPED_AT_10_OF_24_ROTATION_L1_NOT_AVAILABLE`。停止原因是既有development经济验收失败；
不是源码、输入、writer或运行时故障。旧request不得重跑，详细事实见§4.3.4.9与§23.40。

#### 4.3.4.9 C-012-RL1-RW1：fixed rolling Ridge 与历史结构资格修订（USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED）

本节只处理HR1正式结果暴露的两个直接blocker：expanding Ridge对时变轮动关系响应不足，以及historical fold中尚未具备结构输入的sector被纳入可用性分母。
HR1冻结request、输入、artifact与失败结论保持不可改写；本节不是对旧结果调阈值。用户已在下述D1～D6精确授权RW1源码与测试，但仍未授权正式fit、selection、model/bundle/READY、数据库、runtime或合入。

正式证据边界固定为：producer commit=`5c1a90a7f664bf9729bb30eb0289f729725779bf`，parent report SHA-256=
`d302afe34ca7d4091971c5cd6db9a72b30a702329de19b72e87f8e3976233e44`，child failure SHA-256=
`60b56d6ee59eac2c795e5bad94776c6efed2e975cc406360ce03a879c48e590c`。HR1只完成fresh process 1的10个fold fit；
`selection_performed=false,holdout_accessed=false,model_write=false,bundle_write=false,ready_write=false,database_write=false,runtime_action=false`。

##### A. C-012-RL1-RW1-D1：唯一模型变化与identity（USER_APPROVED_EXACT_CONTRACT）

1. 新contract version拟固定为`C-012-RL1-RW1-D1-D6`，algorithm version拟固定为
   `hmm_risk_rotation_l1_market_conditioned_rolling_ridge_v1`；不得覆盖或复用HR1 identity；
2. market component完全不变：K=2、`jump_penalty=4.0`、`seed=42`、五项market feature和每fold expanding train均沿用HR1；
3. L1 Ridge参数、十维输入、10D centered target、score方向及daily top/bottom 20%三态投影全部不变；只把Ridge/preprocess train scope
   从anchored expanding改为预注册固定长度rolling window；
4. 唯一批准值为`rolling_window_open_days=252`。选择依据是：一个完整交易年、显著短于fold-3前的多年度expanding样本、覆盖现有120日最长feature
   lookback，并在31个L1横截面上保留约`31×252`个feature rows；该值不是从RW1结果选择，禁止window grid、自动比较126/252/378或失败后改值；
5. 用户已精确批准`252`及本节D1～D6，并授权源码与测试；正式24-fit、model/bundle/READY、数据库、runtime和合入仍不在本授权内。

##### B. C-012-RL1-RW1-D2：rolling train、purge与因果边界（USER_APPROVED_EXACT_CONTRACT）

对既有五fold，validation窗口、market train窗口、calendar、target horizon、purge=`10`、embargo=`0`全部不变。若D1的`W=252`获批：

1. 对fold `f`，`ridge_train_dates_f`严格等于该fold train segment末尾连续`W`个canonical open days；日期集合、count、start/end和SHA-256在读取
   validation outcome前冻结；不足`W`日则typed失败，不缩窗；
2. 为计算`ridge_train_dates_f`首日的最长120日rolling base feature，source read可向前读取最多120个canonical open days作为feature warmup；warmup rows及其
   outcome不得进入preprocess、Ridge fit、metric或rolling count，warmup date-set/count/hash必须单独冻结；
3. preprocess只在`ridge_train_dates_f`的可用L1 feature rows上拟合；Ridge target rows还必须满足10D future horizon完整落在该fold train segment内。
   因此最后10个train dates可参与train-only feature scale，但不得提供跨segment target；
4. validation仍从独立market arrival cost开始，逐日只读取t-1/PIT observation；不得carry train末market state、读取future outcome、对validation refit preprocess/Ridge，
   或用validation结果选择window；
5. full-development final Ridge拟使用截止`2026-03-31`的最后`W`个canonical open days；final market仍使用完整development expanding train。

##### C. C-012-RL1-RW1-D3：pre-frozen historical sector eligibility（USER_APPROVED_EXACT_CONTRACT）

canonical L1 catalog始终保留31个sector，不删除`801230.SI`或任何历史短样本sector。每fold在读取validation outcome和计算产品metric前冻结：

```text
E_f = {s in canonical_L1_31 |
       本fold首个canonical validation trading date按t-1/PIT规则可获得s的五项base feature，
       五项值均finite，且C-010/C-013 input status均为available}
```

1. 对首个canonical validation trading date `t_f`，资格feature cutoff严格等于calendar中前一canonical open day `t_f-1`；实现必须分别冻结并保存`authority_date=t_f`与`eligibility_feature_cutoff_date=t_f-1`，只能读取cutoff当日及此前的PIT输入。calendar边界若为非交易日，以request冻结的首个实际交易日及其前一open day为准；不得读取`t_f`当日feature，更不得读取future return、validation Rank IC/spread、state分布、coverage结果和任何D6结果；
2. `E_f`一经冻结，在整个fold内不因中途数据出现而加入新sector，也不因后续缺行删除sector；ineligible项使用
   `hmm_risk_rotation_l1_historical_structure_ineligible_at_fold_start`并保存code、input reason、authority identity和date；
3. output同时保存canonical 31、`E_f`、`canonical_minus_eligible`及各自count/hash。validation feature的preprocess与relative cross-section、10D target的centered cross-section都必须先按canonical 31及既有公式构造，再只投影`E_f` identity；禁止因资格缩分母重算feature median或target median。ineligible不生成neutral、默认score、前值或当前行业映射；
4. 此规则仅适用于historical causal replay。真实forward prediction的canonical denominator仍固定31，不得以过去某fold的eligibility排除当前sector；
5. 现有HR1 artifact显示`801230.SI`在fold-1为`0/126`、fold-2为`37/126`、fold-3～5完整；该事实支持设计修订，但不预先决定RW1是否通过。

##### D. C-012-RL1-RW1-D4：经济验收保持不变（USER_APPROVED_EXACT_CONTRACT）

RW1不改变任何产品效果门槛：五fold必须完整执行；至少相同`4/5` fold的Rank IC与spread均`>0`；五foldmedian Rank IC `>=0.02`、median spread
`>=0.003`；拼接OOF两项Newey-West t-stat均`>=1.645`、lag=`9`。metric公式、average-rank Spearman、极端组等权spread、state投影、finite与日期唯一性
均沿用HR1。失败不得删除fold、改NW lag、放宽阈值或选择局部正向时期。

##### E. C-012-RL1-RW1-D5：historical coverage与完整分母（USER_APPROVED_EXACT_CONTRACT）

1. 每fold产品metric只在`E_f`中计算，但每个receipt必须同时展示31个canonical sector和ineligible清单；不得将`|E_f|`冒充canonical universe size；
2. outcome-eligible日期的daily available count必须`>=max(28,ceil(0.90×|E_f|))`，且至少90%的outcome-eligible日期达到该值；若`|E_f|<28`则该fold直接
   `INSUFFICIENT_COVERAGE`，不得降低daily minimum；
3. `E_f`内每个sector的state/score availability必须达到该foldstate dates的`>=80%`；ineligible sector单独计入
   `structurally_ineligible_sector_count`，不进入per-eligible-sector availability分母；
4. `FULL_COVERAGE`只允许全部fold均`|E_f|=31`且31个sector在全部state dates可用；任何fold存在structural ineligible sector时，即使`E_f`内部100%可用，
   顶层coverage最高只能是`COVERAGE_AVAILABLE`。产品层必须展示每fold及聚合的`historical_structural_eligibility_ratio=|E_f|/31`，
   `COVERAGE_AVAILABLE`不能掩盖ineligible行业；
5. coverage不能补足D4经济失败，D4经济通过也不能补足coverage失败。forward继续按31个canonical sector执行既有coverage合同。

##### F. C-012-RL1-RW1-D6：成本、复现、artifact与停止（USER_APPROVED_EXACT_CONTRACT）

RW1仍为两个fresh Python process、每process五fold×(market fit 1 + L1 Ridge fit 1)+final market/Ridge各1=`12` fits，总计`24` fits；不因rolling变化新增grid或fit。
两process必须对rolling dates、eligibility、fold metrics、preprocess、parameters、source/calendar identity和最终payload达到bitwise canonical hash一致。

只允许扩展既有candidate service/CLI/direct tests及本设计，不新增依赖、DB schema、registry、scheduler、通用训练/evidence平台或第二writer。既有pre-HR1 holdout reader的contract/algorithm/schema/threshold/payload-key identity必须保持本地immutable，禁止从可演进RW1常量派生并重解释历史candidate。reason code至少新增：

- `hmm_risk_rotation_l1_rolling_window_incomplete`；
- `hmm_risk_rotation_l1_historical_structure_ineligible_at_fold_start`；
- `hmm_risk_rotation_l1_historical_eligibility_mismatch`。

RW1通过D1～D5后才允许写compact candidate，并继续G2-A的真实单日prediction/API/UI纵切；任何经济、coverage、writer/readback或fresh-process失败均为
`ROTATION_L1_NOT_AVAILABLE`，不写model/bundle/READY，不进入holdout，不打开第二candidate、window grid、阈值调整或新诊断阶段。

当前状态：`C-012-RL1-RW1-D1～D6 = USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED_NO_FORMAL_FIT`。

### 4.4 InputManifest

`hmm_risk_input_manifest_v1` 至少包含：

- request identity：candidate、trade_date policy、rule/generator/observation versions；
- candidate/coefficient artifact identity、state-model-set identity 与 L1/L2 model SHA-256；
- `train_end`、requested/resolved `trade_date`、`as_of_date`；
- 每个 dataset 的 max completed date、row count、content hash 和 missing evidence；
- PIT L1/L2 mapping rows 的 canonical hash；
- sector coverage、model coverage、observation coverage；
- source code/git commit 和 canonical JSON `input_hash`。

所有 dict key、sector、date 和 symbol 按规范顺序序列化。manifest/hash 不匹配时拒绝 persistence。

## 5. Watermark 与 PIT 数据契约

### 5.1 共同完成水位

默认 policy 为 `latest_common_completed`；也允许显式历史交易日。共同水位取以下数据集中最小完成日：

- `market.trading_calendar` 的最新 completed open day；
- `market.sector_data` 所需 sector aggregate 字段的最新完整日；
- `market.index_daily` 中 CSI300 benchmark 的最新完整日；
- `market.sw_daily` 的市场总量 observation 最新完整日；
- C-007-A L1 所需 `market.kline_daily_raw/daily_basic/moneyflow_ts/stk_limit/suspend_d` 的最新共同完整日；
- 请求指定的 `market.stock_universe_pit_state/spans` 为 ready、非 dirty 且覆盖目标日；
- `market.sw_index_member` 在目标日可解析的 PIT L1/L2 mapping；
- candidate coefficient coverage end 和 model `train_end` 上限。

显式日期不得超过共同水位，不得使用自然日、`date.today()`、上一份成功结果或单一表 max date回退。

### 5.2 Sector aggregate canonicalization

先按请求冻结的全局股票池 PIT 取得目标日 eligible 股票，再将 `market.sector_data` 按
`trade_date/ts_code` 动态关联 5.3 的历史行业 mapping。对同一 L2/date 的重复 eligible stock rows，
先比较所有 observation 字段；只有值完全一致时
才能折叠为一条 sector observation。任一字段不一致、非有限、单位不符或缺失时，该 sector/date 失败并
记录 row identities；禁止 `DISTINCT ON` 静默挑一行。

本节只定义 direct L2 canonical row；direct L1 不平均这里的 `sw2_*` row 或 L2 feature，而严格执行 4.3.1
的 stock-fact-first 原始量聚合与 7/20 维重算。两层各自保留 source/hash，不得互相 fallback。

### 5.3 PIT mapping snapshot

mapping 使用 `in_date <= as_of_date AND (out_date IS NULL OR out_date >= as_of_date)`；按 4.3.1 A 节通过
`sw_index_member` 完整闭包将历史 industry/index code 表示规范化后，冻结 source 与 canonical
`symbol/l1_code/l1_name/l2_code/l2_name/in_date/out_date` 的排序 hash。只有规范化后 identity 完全相同的多条
source row 才可保留全部 source evidence 后折叠；非等价多重 active mapping、L2 对应多个 canonical L1、
缺 code/name/member owner 或空 mapping 均显式失败。历史日不得读取当前成员关系。
mapping 只从 `sw_index_member` 动态解析，不要求或读取 `sector_data.l1_code/l2_code/mapping_in_date`。

## 6. 唯一 State Generator 契约

### 6.1 Direct state-evidence adapters

- generator 只调用已经在 `hmm_risk_candidate_state_evidence_matrix_v1` 中核验为 `SUPPORTED` 的
  candidate-specific adapter；不得从旧脚本抽取或迁移业务逻辑来填补新域合同。
- adapter 必须返回 `candidate_id/manifest_hash/parser_contract/sector_level/sector_code/trade_date`、
  semantic `trending/neutral/fading` posterior、semantic mapping evidence、source artifact SHA-256、
  observation version 和 adapter version。
- 对需要 forward-filter 的 source contract，只能 causal 使用 `<= as_of_date` observation；禁止 smoothing、
  future observation 或从旧成功结果回退。其它 source contract 必须按其经确认的直接证据合同计算，不能套用 legacy 公式。
- posterior 每项必须有限、非负、和为 1；`hmm_state=argmax(posterior)`。semantic label 缺失/重复、
  mapping 无权威证据或不可判定 tie 均使该 sector 失败，不得按系数大小、数组位置或显示名猜测。
- `state_confidence=max(posterior)`；`confidence_definition_version` 与 `state_origin` 由 adapter contract 固定。
  完整 posterior 与 mapping evidence 写入持久化 evidence，raw JSON 不直接展示给 UI。

### 6.2 Direct L1/L2 inference

- generator 从同一 `state_model_set_id` 选择请求层级的 direct model；不得跨层读取 posterior。
- L1 observation 必须使用 model-set manifest 固化的 C-007-A formula/universe/mapping contract，并按请求日重新解析、
  hash 当日 PIT L2 constituent set；这里聚合的是 canonical stock facts，不是 L2 feature、隐状态或 posterior。
- L2 observation 使用模型 entry 对应的 canonical L2 行情和 feature contract。
- 两层分别执行 causal forward-filter 并分别保存 posterior/confidence；缺任一 sector 使 run=`partial_failed`，
  缺整个层级使 run=`failed/hmm_risk_state_model_set_incomplete`。
- UI 显示 `direct_hmm`、model SHA 和 model-set version；禁止 derived 标签、另一层复制或 neutral 填充。

### 6.3 Transition 与 severity

transition 只比较同 candidate、sector_level、sector_code、generator/rule version 的前一完整交易日 current revision：

- 无前态：`initial`，severity `NONE`；
- `trending -> fading`：`HIGH`；
- `fading -> fading`：`MEDIUM`；
- `fading -> trending`：`OPPORTUNITY`；
- 其它：`NONE`。

severity 是 `hmm_risk_alert_rule_v1` 的解释标签，不改变 state，不触发交易动作。
`fading -> neutral` 属于 `NONE`，但必须把已有 `fading_risk` event 解析为 closed，
`resolution_reason=fading_exit_to_neutral`；`fading -> trending` 同样关闭该 event，
`resolution_reason=fading_exit_to_trending`，并产生 `OPPORTUNITY` alert。解析 event 不新增 severity。

### 6.4 失败与 partial 语义

- 输入级失败（candidate、model、watermark、mapping、全局 observation）使 run=`failed`，不写 state/alert/event。
- sector/level adapter 级失败允许其它已核验 sector 写入，但 run=`partial_failed`；缺 sector/level 不写 neutral placeholder。
- 受 source 缺失或 C-001/C-002 未决影响的层级不生成；UI 标为 degraded 并显示缺失 sector/level/reason。
- 只有 evidence matrix 声明的全部预期 candidate coverage、sector/level、alerts/events persistence 完成时，run 才可 `succeeded`。
- Offline model preparation 的 fit、monitor、likelihood、covariance、semantic assignment/evidence 与 family completeness
  使用 4.3.2 的独立状态；任一失败不得压缩成 generic success。C-008-B1 只生成 diagnostic receipt，不进入 daily run
  `succeeded/partial_failed`，也不改变 candidate lifecycle。未来 B3 实现任一 restart、family selection 或 semantic
  validation 失败均保持 model preparation blocked，不得转写为 daily run partial success。

## 7. Revision、Dedupe 与迟到数据重算

### 7.1 Keys

- `dedupe_key`：candidate + trade_date + sector_level + sector_code + generator_version + rule_version。
- `input_hash`：完整 InputManifest hash + sector observation hash + model hash + mapping hash。
- 相同 dedupe_key/input_hash 重放返回已有 current row，不新增 revision。
- 相同 dedupe_key、不同 input_hash 创建 `revision=max+1` 并设置 `supersedes_*`。
- IDs 由 kind + canonical identity hash 生成，不使用随机值冒充幂等。

### 7.2 事务、并发与失败收敛

- Transaction A（enqueue）：创建 queued run 或执行 idempotency compare；不同 request hash 返回 409。
- Transaction B（claim）：CAS `queued -> running`，递增并返回 `fencing_token`，写 owner、lease、heartbeat 和 started time。
- Compute（无写事务）：在已冻结、只读的 InputManifest 上完成 adapter 计算；任何 input/hash 漂移立即失败。
- Transaction C（successful persistence）：按排序后的 dedupe key 获取 transaction-scoped advisory lock；一次完成
  revision 分配、state、alert、event、counters 与 `succeeded/partial_failed` terminal status。唯一约束冲突必须
  重读并比较 input_hash；不同 payload 不能被当作相同成功。
- Transaction D（failure receipt）：Transaction C 或 compute 失败后，使用独立事务和 owner/fencing CAS 写
  `failed/error_code/error_message/error_context/completed_at`。若 D 也失败，worker 必须 fatal log、非零退出；
  API 以 lease expiry 推导 `stale` effective status，不得返回成功或旧结果冒充本次结果。

### 7.3 Late-data cascade

迟到 market/mapping/model content 导致某日 input_hash 改变时，从最早变化交易日开始，按交易日顺序
重算至该 candidate 已持久化的最新日期。transition、alert 和 event lifecycle 均随 state revision 追加新 revision；
不原地 update 历史。只有 output 与 current revision 完全相同时才不新增行。失败中断时 Transaction C
整体回滚，再由独立 Transaction D 持久化 failed evidence；不得声称同一已回滚事务保留了失败回执。

## 8. Database Contracts / Schema

### 8.1 `hmm_risk.daily_generation_run`

| column | exact contract |
|---|---|
| `run_id` | `TEXT PRIMARY KEY`，deterministic run identity |
| `idempotency_key` | `TEXT NOT NULL UNIQUE` |
| `request_hash` / `request_payload` | `CHAR(64) NOT NULL` / `JSONB NOT NULL` |
| `status` | `TEXT NOT NULL DEFAULT 'queued' CHECK` in `queued/running/succeeded/partial_failed/failed/cancel_requested/cancelled` |
| `candidate_id` | `TEXT NOT NULL REFERENCES hmm_evolution.candidate(candidate_id) ON DELETE RESTRICT` |
| `candidate_manifest_hash` | `CHAR(64) NOT NULL` |
| `state_model_set_id` / `state_model_set_hash` | `TEXT NOT NULL` / `CHAR(64) NOT NULL` |
| `l1_model_sha256` / `l2_model_sha256` | `CHAR(64) NOT NULL`；两层 direct model identity |
| `trade_date_policy` | `TEXT NOT NULL CHECK` in `explicit/latest_common_completed` |
| `requested_trade_date` | `DATE NULL`；policy=`explicit` 时 `NOT NULL`，否则必须 `NULL` |
| `resolved_trade_date` / `as_of_date` | `DATE NULL`；进入 successful terminal 前必须均非空且相等 |
| `generator_version` / `rule_version` | `TEXT NOT NULL` |
| `input_manifest` / `input_hash` | `JSONB NULL` / `CHAR(64) NULL`；resolution 后成对存在 |
| `owner_id` | `TEXT NULL` |
| `fencing_token` / `row_version` | `BIGINT NOT NULL DEFAULT 0/1 CHECK >= 0/>=1` |
| `lease_expires_at` / `heartbeat_at` | `TIMESTAMPTZ NULL`；running/cancel_requested 时与 owner 成组存在 |
| `max_runtime_seconds` | `INTEGER NOT NULL CHECK BETWEEN 60 AND 7200`；保存服务启动时已校验的显式配置值 |
| `expected_count` / `succeeded_count` / `failed_count` | `INTEGER NOT NULL DEFAULT 0 CHECK >= 0` |
| `l1_expected_count/l1_succeeded_count/l2_expected_count/l2_succeeded_count` | `INTEGER NOT NULL DEFAULT 0 CHECK >= 0` |
| `missing_evidence` | `JSONB NOT NULL DEFAULT '[]'::jsonb`，顶层必须 array |
| `error_code/error_message/error_context` | `TEXT NULL/TEXT NULL/JSONB NULL`；failed/cancelled 时 error_code 非空 |
| `cancel_requested_at/cancel_requested_by` | `TIMESTAMPTZ NULL/TEXT NULL`，成对出现 |
| `queued_at/started_at/completed_at/created_at/updated_at` | `TIMESTAMPTZ`；除 started/completed 可空外均 `NOT NULL`，DB UTC `now()` |

table CHECK 还要求 counters 不超过 expected；`succeeded` 必须 `failed_count=0 AND succeeded_count=expected_count`；
`partial_failed` 必须 succeeded/failed 均大于 0；所有 terminal status 必须有 `completed_at` 且 lease/owner 清空。
相同 idempotency key + 相同 request hash 返回同一 run，不同 hash 返回 409。

### 8.2 `hmm_risk.sector_state_timeline`

| column | exact contract |
|---|---|
| `state_id` | `TEXT PRIMARY KEY`，由 dedupe identity + revision hash 生成 |
| `run_id` | `TEXT NOT NULL REFERENCES hmm_risk.daily_generation_run(run_id) ON DELETE RESTRICT` |
| `candidate_id/candidate_manifest_hash/snapshot_id/config_id` | `TEXT NOT NULL/CHAR(64) NOT NULL/TEXT NOT NULL/TEXT NOT NULL` |
| `trade_date/as_of_date` | `DATE NOT NULL` 且必须相等 |
| `sector_level/sector_code/sector_name` | `TEXT NOT NULL`；level CHECK in `L1/L2`，code/name trim 后非空 |
| `hmm_state` | `TEXT NOT NULL CHECK` in `trending/neutral/fading` |
| `state_probabilities` | `JSONB NOT NULL`；model validation 要求仅三 semantic keys、有限、非负、和为 1 |
| `state_confidence` | `DOUBLE PRECISION NULL CHECK BETWEEN 0 AND 1` |
| `state_origin/confidence_definition_version` | `TEXT NOT NULL/TEXT NOT NULL`；值必须来自已核验 adapter contract，不设猜测 fallback |
| `parser_contract/adapter_version/observation_version` | `TEXT NOT NULL` |
| `state_model_set_id/model_artifact_sha256/input_hash/result_hash` | `TEXT NOT NULL/CHAR(64) NOT NULL/CHAR(64) NOT NULL/CHAR(64) NOT NULL` |
| `mapping_snapshot_hash` | `CHAR(64) NOT NULL` |
| `generator_version/rule_version` | `TEXT NOT NULL` |
| `transition_from/transition_kind` | `TEXT NULL/TEXT NOT NULL`；前者三值或 NULL，后者为 stable enum |
| `severity` | `TEXT NOT NULL CHECK` in `NONE/HIGH/MEDIUM/OPPORTUNITY` |
| `dedupe_key` | `CHAR(64) NOT NULL` |
| `revision` | `INTEGER NOT NULL CHECK > 0` |
| `supersedes_state_id` | `TEXT NULL REFERENCES hmm_risk.sector_state_timeline(state_id) ON DELETE RESTRICT` |
| `evidence` | `JSONB NOT NULL`，含 semantic mapping source/hash，禁止 UI 原样透传 |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` |

UNIQUE 为 `(dedupe_key,revision)` 与 `(dedupe_key,input_hash)`；FK candidate、snapshot/config 的一致性由 resolver
冻结证据和 repository precondition 双重校验。C-002-A 不允许任何 `derived_l1_*` state origin。

### 8.3 `hmm_risk.daily_alert`

| column | exact contract |
|---|---|
| `alert_id` | `TEXT PRIMARY KEY` |
| `run_id/state_id` | `TEXT NOT NULL`，分别 FK run/state，`ON DELETE RESTRICT` |
| `candidate_id/trade_date/sector_level/sector_code` | `TEXT/DATE/TEXT/TEXT NOT NULL`，与 state identity 完全一致 |
| `severity` | `TEXT NOT NULL CHECK` in `HIGH/MEDIUM/OPPORTUNITY`；`NONE` 不写 alert |
| `transition_from/transition_to` | `TEXT NOT NULL CHECK` 三值 |
| `rule_version/generator_version/explanation_version` | `TEXT NOT NULL` |
| `explanation` | `JSONB NOT NULL`，同时含稳定 message key 与结构化证据 |
| `input_hash/result_hash/dedupe_key` | `CHAR(64) NOT NULL` |
| `revision` | `INTEGER NOT NULL CHECK > 0` |
| `supersedes_alert_id` | `TEXT NULL REFERENCES hmm_risk.daily_alert(alert_id) ON DELETE RESTRICT` |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` |

UNIQUE 为 `(dedupe_key,revision)` 与 `(dedupe_key,input_hash)`；state_id 唯一，确保一条有 severity 的 state revision
最多一条 alert revision。

### 8.4 `hmm_risk.risk_event`

| column | exact contract |
|---|---|
| `event_revision_id` | `TEXT PRIMARY KEY` |
| `event_id/dedupe_key` | `TEXT NOT NULL/CHAR(64) NOT NULL`；event_id 跨 revision 稳定 |
| `candidate_id/sector_level/sector_code/event_type/rule_version` | `TEXT NOT NULL`；event_type v1 仅 `fading_risk` |
| `status` | `TEXT NOT NULL CHECK` in `open/resolved` |
| `revision` | `INTEGER NOT NULL CHECK > 0` |
| `first_alert_id/latest_alert_id` | `TEXT NOT NULL REFERENCES hmm_risk.daily_alert(alert_id) ON DELETE RESTRICT` |
| `opened_trade_date/last_trade_date/resolved_trade_date` | `DATE NOT NULL/DATE NOT NULL/DATE NULL` |
| `resolution_reason` | `TEXT NULL CHECK` in `fading_exit_to_neutral/fading_exit_to_trending` when non-null |
| `supersedes_event_revision_id` | `TEXT NULL REFERENCES hmm_risk.risk_event(event_revision_id) ON DELETE RESTRICT` |
| `result_hash/evidence` | `CHAR(64) NOT NULL/JSONB NOT NULL` |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` |

UNIQUE `(event_id,revision)` 与 `(event_id,result_hash)`。HIGH 打开、MEDIUM 延续；`fading -> neutral` 以 NONE state
revision 关闭但 latest_alert_id 保持最后一条 fading alert；`fading -> trending` 以 OPPORTUNITY alert 关闭。
resolved 必须有 resolved date/reason，open 必须二者均为空；同一 current chain 的 identity 必须一致。

### 8.5 `hmm_risk.retrospective_report`

| column | exact contract |
|---|---|
| `report_id` | `TEXT PRIMARY KEY` |
| `candidate_id/candidate_manifest_hash/model_artifact_sha256` | `TEXT NOT NULL/CHAR(64) NOT NULL/CHAR(64) NOT NULL` |
| `start_trade_date/end_trade_date/sector_level` | `DATE NOT NULL/DATE NOT NULL/TEXT NOT NULL CHECK L1/L2` |
| `report_spec/report_spec_hash` | `JSONB NOT NULL/CHAR(64) NOT NULL` |
| `source_manifest/source_hash` | `JSONB NOT NULL/CHAR(64) NOT NULL` |
| `status` | `TEXT NOT NULL CHECK` in `succeeded/failed`；不允许 partial 冒充可解释报告 |
| `metrics/evidence/result_hash` | `JSONB NULL/JSONB NOT NULL/CHAR(64) NULL`；succeeded 时 metrics/result_hash 非空 |
| `sample_count` | `INTEGER NOT NULL DEFAULT 0 CHECK >= 0` |
| `error_code/error_message/error_context` | `TEXT NULL/TEXT NULL/JSONB NULL`；failed 时 error_code 非空 |
| `created_at/completed_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()/TIMESTAMPTZ NOT NULL` |

UNIQUE `(candidate_id,start_trade_date,end_trade_date,sector_level,report_spec_hash,source_hash)`；同 spec/input 幂等。
报告只读 market forward returns，不写交易表；只接受 §10 已批准的 exact spec。

### 8.6 Views、COMMENT 与 exact verify

exact index 清单为：`idx_hmm_risk_run_claim(status,queued_at,run_id) WHERE status='queued'`、
`idx_hmm_risk_run_lease(lease_expires_at,run_id) WHERE status IN ('running','cancel_requested')`、
`idx_hmm_risk_state_lookup(candidate_id,sector_level,sector_code,trade_date DESC,revision DESC)`、
`idx_hmm_risk_state_run(run_id)`、`idx_hmm_risk_alert_lookup(candidate_id,trade_date DESC,sector_level,severity)`、
`idx_hmm_risk_event_lookup(candidate_id,status,sector_level,sector_code,last_trade_date DESC)`、
`idx_hmm_risk_report_lookup(candidate_id,end_trade_date DESC,sector_level)`。

建立 `sector_state_current`、`daily_alert_current`、`risk_event_current` views，以各 dedupe identity 的最大 revision 为 current；
view 必须显式列名，不使用 `SELECT *`。schema COMMENT 固定 `hmm_risk_schema_v1`，由 bootstrap exact verifier 读取。
所有 schema/table/column、constraint、index、view 必须有 COMMENT。`init_hmm_risk_schema.py` 采用单事务
bootstrap + exact columns/types/defaults/nullability/constraints/indexes/views/comments/version verify；业务服务不隐式执行 DDL。

## 9. Job、Worker 与 API Contracts

### 9.1 Preview 与 run

两个 POST 共用 exact request body：

| field | contract |
|---|---|
| `candidate_id` | required non-empty string；不得为 `latest`、display name 或排名 |
| `trade_date_policy` | required enum `explicit/latest_common_completed` |
| `requested_trade_date` | policy=`explicit` 时 required ISO date，否则必须省略/null |
| `generator_version` | required exact `hmm_risk_state_generator_v1` |
| `rule_version` | required exact `hmm_risk_alert_rule_v1` |

未知字段返回 422。`POST /api/v1/hmm-risk/jobs/daily/preview` 零写入；200 `data` 精确包含
`candidate_identity`、`resolved_trade_date`、`as_of_date`、`level_compatibility`、`expected_sector_counts`、
`missing_evidence[]`、`input_manifest_hash`、`runnable`。`runnable=false` 仍是已成功完成的 preview 计算，
但必须带稳定 reason；preview 不是批准门禁，run 不要求先 preview。

`POST /api/v1/hmm-risk/jobs/daily/run` 另要求 header `Idempotency-Key`（1..128 个可打印非空字符）；
只执行 Transaction A 并返回 202 `data={run_id,status:'queued',request_hash,queued_at}`。不得同步长算、
不得调用训练。相同 key/body 返回同一 202；相同 key/不同 body 返回 409 `hmm_risk_idempotency_conflict`。

### 9.2 Manual-first worker

`scripts/hmm_risk/run_daily_worker.py --once|--drain --max-jobs N --owner-id ...` 显式消费 queue。
首次版本没有 `--serve` scheduler 注册；claim 使用 lease/fencing，陈旧 owner 不能提交。每次 claim 前先把
lease 已过期的 running job 终态化为 `failed/hmm_risk_worker_lease_expired`，不自动重认领同一 run；调用方以
新 idempotency key 显式重跑。CLI 另提供 `--reap-expired` 只执行 reaper；它是故障收敛动作，不启动 scheduler。

`HMM_RISK_JOB_MAX_RUNTIME_SECONDS`、`HMM_RISK_JOB_LEASE_SECONDS`、`HMM_RISK_JOB_HEARTBEAT_SECONDS`
是 worker/API 启动必需显式配置：max runtime 为 60..7200，lease 为 30..300，heartbeat 为正整数且
`heartbeat * 3 < lease < max_runtime`；max runtime 写入每个 run。缺失或非法时启动/preview/run 均 fail loud，
不采用隐藏默认值。
到达 max runtime 时请求取消并由 Transaction D 写 `failed/hmm_risk_job_timeout`。受控中断先写
`cancel_requested`，在当前 adapter 安全点结束并写 `cancelled/hmm_risk_job_cancelled`；进程崩溃由显式 reaper
或下次 pre-claim reaper 收敛。

`GET /jobs/{run_id}` 在 DB status=`running/cancel_requested` 且 `lease_expires_at <= db_now()` 时，必须返回
`effective_status='stale'` 与 `hmm_risk_worker_lease_expired`，即使 reaper 尚未写终态；不能无限显示 running。
`POST /api/v1/hmm-risk/jobs/{run_id}/cancel` 仅对 queued/running 有效：queued 直接 CAS cancelled，running 写 cancel_requested；
terminal 重放幂等返回原状态。cancel 是技术控制，不是运行前审批。worker 只调用 service，不复制 generator。
首次 production run 必须另获运行授权；之后是否登记只读日任务属于独立发布步骤。

### 9.3 Read APIs

父设计端点保持不变，并固定 query/响应语义：所有 list query 必须显式提供 `candidate_id`；date range 为 ISO date，
`start <= end` 且最多 366 个交易日；`sector_level` 仅 L1/L2；pagination 使用显式 `limit` 1..500 与 opaque cursor，
不接受任意 sort expression。

- `overview`：current watermark、candidate/model identity、latest run、coverage、state distribution、alerts、staleness，以及下述
  `model_set_acceptance_summary`；不得把 model-set warning 压缩为普通 READY。
- `heatmap`：显式 candidate、level/date range；返回 cells、missing cells、run status 和 current revisions。
- `alerts`：按 date/level/candidate 查询 current alerts；无 alert 是真实空，不等于数据缺失。
- `timeline`：sector + candidate + level，返回 state revisions/current marker。
- `events/{event_id}`：完整 event revision chain 和可读 evidence。
- job status：`GET /jobs/{run_id}` 返回 persisted/effective status、lease/heartbeat、counts、missing evidence、
  stable error 与 timestamps；另有上述 cancel endpoint。
- report：增加 `GET /reports` 与 `GET /reports/{report_id}`；生成使用受控 job/CLI，不在普通 GET 中写入。

`overview.data.model_set_acceptance_summary` 精确包含：`state_model_set_id`、按 L1/L2 分开的
`d4_contract_versions={L1:{d4_01,d4_02},L2:{d4_01,d4_02}}`、`l1_expected_entry_count`、`l2_expected_entry_count`、
按 L1/L2 分开的 `likelihood_status_counts`、
`covariance_status_counts`、去重排序的 `model_set_blocking_reason_codes`、`model_set_blocking_evidence_hash`、
`likelihood_warning_count`、去重排序的 `likelihood_warning_reason_codes` 与 `likelihood_warning_evidence_hash`。
每层 status counts 必须覆盖 expected entry count，`accepted_with_warning` 不得计入普通 `accepted`。每个 D4 version 在该层
存在可验证 receipt 时必须是非空 exact version；仅当该层对应 status 全部为 `insufficient_evidence` 且带 evidence-missing reason
时允许为 null，任何 `READY` model set 的四个 version 值都必须非空。warning count 必须与 content-addressed entry receipts
重聚合一致；即使 count=0，也必须返回 canonical empty warning set 的非空 hash，不得以 null/缺字段代替。blocking reason/hash
也必须从 entry receipts 重聚合；无 blocker 时返回 canonical empty blocking set 的非空 hash。字段缺失、count 不闭合或
evidence hash 不一致时以最具体的
`hmm_risk_model_artifact_missing`/`hmm_risk_model_artifact_hash_drift`/`hmm_risk_state_model_set_incomplete` 失败，不得填 0、
空 warning 或继续返回可运行 READY。

retrospective report 的 source manifest 与 confirmed output 必须冻结同一 `model_set_acceptance_summary` 及其 canonical hash，
并在报告 evidence 中列出 warning/blocking family/level/sector/count/code；报告生成不得重新计算或改写 model acceptance。若模型
warning/blocking readback 缺失或漂移，report job 写 failed receipt，不写成功 metrics。普通 API summary 与 report 必须引用同一
immutable warning/blocking evidence identity，禁止一端显示、另一端静默省略。

成功响应 exact envelope 为 `{status:'success',data,trace_id}`；失败 exact envelope 为
`{status:'error',error:{reason_code,message,context},trace_id}`，error 时不得出现成功 `data`。
HTTP mapping：validation 422、not found 404、identity/hash/idempotency conflict 409、dependency unavailable 503、
unexpected 500。DB/schema/input error 不返回空数组成功；未知异常不把 stack/secret 放入 context。

### 9.4 Retrospective report generation CLI

报告仅由 `scripts/hmm_risk/generate_retrospective_report.py` 显式执行，required args 为 `--candidate-id`、
`--start-trade-date`、`--end-trade-date`、`--sector-level L1|L2`、`--report-spec-file`；可选 `--preview`
严格零写入。spec file 必须是 canonical JSON，且精确匹配 §10 的 `hmm_risk_retrospective_v1`。
版本不支持或任一字段缺失时，命令以非零退出并输出 compact JSON error
`hmm_risk_report_spec_unsupported`；不得采用默认 horizon/quantile。成功写入单个幂等 report row 并回读
`report_id/report_spec_hash/source_hash/result_hash/sample_count`；失败只写 failed report receipt，不写 metrics 假成功。

## 10. Retrospective Report Contracts

C-003-A 已批准 `hmm_risk_retrospective_v1`：

| field | exact approved value |
|---|---|
| `forward_horizons` | `[5,10,20]` trading days |
| L2 return | canonical L2 close：`close(T+h)/close(T)-1` |
| L1 daily return | 当日所有 PIT constituent L2 均有 canonical return 且 amount>0 时，`Σ amount_l2,d * return_l2,d / Σ amount_l2,d`；缺任一 constituent 则该 L1/date 缺失 |
| L1 horizon return | `Π(d=T+1..T+h)(1+l1_daily_return_d)-1` |
| benchmark | CSI300 同期 close return |
| excess return | `sector_forward_return - csi300_forward_return` |
| continuous metrics | HIGH/MEDIUM/NONE 分组的 count、mean、median、q20；OPPORTUNITY 独立分组，不混入 risk confusion matrix |
| primary binary horizon | 5 trading days |
| adverse oracle | 同 trade_date、同 sector_level 的有效 5D excess return `<= q20` |
| quantile | NumPy `quantile(...,0.20,method='linear')`，version `cross_sectional_excess_q20_linear_v1` |
| alert positive | severity in `HIGH/MEDIUM`；`NONE` 为 negative；OPPORTUNITY excluded |
| confusion metrics | hit/false_positive/miss/true_negative 及 precision/recall，全部显示 numerator/denominator |
| minimum coverage | L1 至少 28/31；L2 至少 118/131；不足则 report=`failed/hmm_risk_report_coverage_insufficient` |

return source 必须与 state timeline 使用同一 trade calendar、PIT sector identity 和共同完成水位；T+h 超过报告水位、
close/amount 非有限、L1 constituent 不全、CSI300 缺失均成为明确 missing evidence。达到 minimum coverage 时，
缺失项仍从指标分母排除并单列 count/reason；低于阈值不输出 succeeded metrics。golden fixture 至少覆盖：
q20 边界相等计 adverse、OPPORTUNITY 排除、L1 constituent 缺失、跨 horizon 尾部缺失、coverage 27/28 与 117/118 边界。

报告必须显示完整 spec/hash、state-model-set、coverage、missingness、每项 denominator 与 source hash；任何指标都只作解释，
不得产生 pass/fail、candidate lifecycle、Selection/QE/Paper/QMT 或交易副作用。迟到价格数据产生新 source hash，
旧 report append-only 保留。

## 11. UI Contracts

### 11.1 Navigation 与 route activation

- 完成真实 API/UI 验收前不注册 `/hmm-risk` 导航，也不改变 `/hmm`。
- 验收通过且 runtime 单独激活后，`/hmm` 重定向 `/hmm-risk`；导航展示“板块风险/演进实验室”。
- Phase 3 未完成时不渲染“滚动训练”占位、disabled tab 或静态页。

### 11.2 页面

- 默认最近 7 个完整交易日，L1/L2 切换；候选 identity 与 level compatibility 显式。
- cell 填充色仅表达 trending/neutral/fading；severity 用边框/角标/文本。
- confidence 为 null 时显示“未提供”，不补 0；L1/L2 均显示 direct model SHA 与 state-model-set version。
- 点击 cell 更新页面内固定详情，不用 drawer；详情含 identity、水位、state/probability/confidence、transition、
  severity、revision、source completeness 和可读 explanation。
- 今日预警、状态分布、event 和 retrospective report 都使用真实 API。
- loading/empty/degraded/failed/stale/terminal 全状态可区分；partial run 显示缺 sector，不涂 neutral。
- chart renderer 失败显示 `hmm_risk_chart_renderer_unavailable` 和结构化表；表是可访问证据，不冒充 chart 成功。
- 固定文案：“仅供研究分析，不构成交易决策”。不显示 raw manifest/error JSON。

## 12. Error Contracts / 稳定 reason codes

- `hmm_risk_candidate_not_found`
- `hmm_risk_candidate_lifecycle_ineligible`
- `hmm_risk_candidate_manifest_drift`
- `hmm_risk_model_artifact_missing`
- `hmm_risk_model_artifact_hash_drift`
- `hmm_risk_model_contract_unsupported`
- `hmm_risk_model_fit_failed`
- `hmm_risk_model_initialization_failed`
- `hmm_risk_model_monitor_not_converged`
- `hmm_risk_model_monitor_history_invalid`
- `hmm_risk_model_max_iterations_reached`
- `hmm_risk_model_likelihood_non_finite`
- `hmm_risk_model_likelihood_evidence_missing`
- `hmm_risk_model_likelihood_nonterminal_decrease`
- `hmm_risk_model_likelihood_terminal_decrease_warning`
- `hmm_risk_model_likelihood_tolerance_failed`
- `hmm_risk_model_map_objective_non_finite`
- `hmm_risk_model_map_objective_decrease`
- `hmm_risk_model_map_numeric_envelope_warning`
- `hmm_risk_model_map_joint_convergence_unavailable`
- `hmm_risk_model_raw_likelihood_decrease_diagnostic`
- `hmm_risk_model_covariance_invalid`
- `hmm_risk_model_covariance_evidence_missing`
- `hmm_risk_model_covariance_acceptance_failed`
- `hmm_risk_model_covariance_bounds_failed`
- `hmm_risk_model_covariance_anomaly_budget_exceeded`
- `hmm_risk_model_train_occupancy_evidence_missing`
- `hmm_risk_model_train_date_sequence_invalid`
- `hmm_risk_model_posterior_invalid`
- `hmm_risk_model_posterior_normalization_failed`
- `hmm_risk_model_train_occupancy_insufficient`
- `hmm_risk_model_train_state_count_insufficient`
- `hmm_risk_model_train_month_coverage_insufficient`
- `hmm_risk_model_train_run_coverage_insufficient`
- `hmm_risk_model_train_transition_coverage_insufficient`
- `hmm_risk_model_train_run_concentration_exceeded`
- `hmm_risk_model_train_regime_path_unsatisfied`
- `hmm_risk_model_posterior_tie`
- `hmm_risk_model_restart_family_incomplete`
- `hmm_risk_model_restart_schedule_incomplete`
- `hmm_risk_semantic_hard_state_missing`
- `hmm_risk_semantic_evidence_insufficient`
- `hmm_risk_semantic_validation_evidence_missing`
- `hmm_risk_semantic_validation_calendar_ledger_invalid`
- `hmm_risk_semantic_validation_availability_receipt_mismatch`
- `hmm_risk_semantic_validation_observation_unavailable`
- `hmm_risk_semantic_validation_utility_unavailable`
- `hmm_risk_semantic_validation_evidence_rows_insufficient`
- `hmm_risk_semantic_validation_date_sequence_invalid`
- `hmm_risk_semantic_validation_posterior_invalid`
- `hmm_risk_semantic_validation_posterior_normalization_failed`
- `hmm_risk_semantic_validation_posterior_tie`
- `hmm_risk_semantic_validation_state_count_insufficient`
- `hmm_risk_semantic_validation_occupancy_insufficient`
- `hmm_risk_semantic_validation_month_coverage_insufficient`
- `hmm_risk_semantic_validation_run_coverage_insufficient`
- `hmm_risk_semantic_validation_transition_coverage_insufficient`
- `hmm_risk_semantic_validation_run_concentration_exceeded`
- `hmm_risk_semantic_validation_utility_variance_non_finite`
- `hmm_risk_semantic_validation_utility_gap_insufficient`
- `hmm_risk_semantic_utility_non_finite`
- `hmm_risk_semantic_utility_tie`
- `hmm_risk_model_selection_evidence_missing`
- `hmm_risk_model_selection_score_non_finite`
- `hmm_risk_model_selection_level_incomplete`
- `hmm_risk_model_selection_repeat_mismatch`
- `hmm_risk_model_selection_contract_unsatisfied`
- `hmm_risk_model_selection_unavailable`
- `hmm_risk_state_model_set_family_incomplete`
- `hmm_risk_training_cutoff_missing_or_future`
- `hmm_risk_common_watermark_unavailable`
- `hmm_risk_sector_rows_inconsistent`
- `hmm_risk_mapping_missing_or_ambiguous`
- `hmm_risk_observation_invalid`
- `hmm_risk_sector_inference_failed`
- `hmm_risk_probability_tie`
- `hmm_risk_revision_conflict`
- `hmm_risk_stale_fencing_token`
- `hmm_risk_worker_lease_expired`
- `hmm_risk_job_timeout`
- `hmm_risk_job_cancelled`
- `hmm_risk_idempotency_conflict`
- `hmm_risk_candidate_not_state_capable`
- `hmm_risk_state_model_set_incomplete`
- `hmm_risk_report_spec_unsupported`
- `hmm_risk_report_coverage_insufficient`
- `hmm_risk_schema_drift`
- `hmm_risk_chart_renderer_unavailable`
- `hmm_risk_c010_expected_opportunity_missing`
- `hmm_risk_c010_contributor_receipt_mismatch`
- `hmm_risk_c010_policy_identity_mismatch`
- `hmm_risk_c010_provider_absence_domain_partition_invalid`
- `hmm_risk_c010_pit_ineligible_for_opportunity`
- `hmm_risk_c010_price_unavailable_for_opportunity`
- `hmm_risk_c010_sw_identity_unavailable_for_opportunity`

C-008-B3 的 D4-01-MAP-A/D4-02-A/D4-03-PERSISTENT-A/D5-01-B/D6-01-B/D6-NA-A reason code 已进入批准的设计合同，但在新源码实现前不代表历史 diagnostic record 已执行
正式验收。未来实现必须使用最具体 reason code；MAP numeric-envelope 与 raw-likelihood decrease warnings 都必须显式持久化，
不是 failure，也不得被压缩为普通 success。历史 diagnostic score不得提前触发正式 selection；
initialization、likelihood、covariance、occupancy、calendar/availability、validation evidence 或 family selection 失败不得压缩为 generic incomplete。
`observation_unavailable` 与 `utility_unavailable` 是逐日 typed evidence，不等同于 entry acceptance failure；entry 是否 accepted 仍只由
D6-01-B 在 D6-NA-A evidence mask 上的既有 gates 决定。ledger/availability receipt/hash 漂移则必须 fail closed，不能降级为普通缺失。

未知异常使用稳定 internal reason + trace id，详细堆栈只进入服务日志；不得转成 neutral、空成功或旧日 current。

## 13. Legacy Boundary / 旧 gate 冻结边界

Decision C-004 已按用户指令确定为 `NO_MIGRATION`：本 Phase 2 不修改、包装、迁移、退役或替换
`scripts/precompute_hmm_risk_gate.py`、`hmm_risk_gate_v1` artifact、Selection/QE loader/provider 或既有消费者接线。
新 `hmm_risk.*` 域独立实现，不要求旧脚本调用新 generator，也不让新域调用旧 gate 形成隐藏耦合。

未来若要共享 parser、切换 consumer、删除 producer 或改变 protect/block/fallback 行为，属于明确共享契约和业务逻辑迁移，
必须另立 feature/BUG、单独详细设计、逐项验收，并在对应 PR 合入前取得用户明确确认。当前文档不预先批准该工作。

## 14. Advisory-only Isolation

- runtime DB role 的 write allowlist 仅 `hmm_risk.*`；market、hmm_evolution、model registry、Selection、Paper、
  QMT 表均为 read-only。
- integration test 在 run 前后比较 Selection/Paper/QMT/strategy/config/snapshot 表摘要与 row counts。
- service 不 import `RiskDecision`、Selection provider、QE apply gate、QMT client 或 order service。
- API response 不包含 `can_buy/should_trade/order_action/position_delta`。
- worker 不创建 HMM training/evolution batch，不修改 candidate lifecycle。

## 15. Implementation Plan / 实施方案与分片

以下Slice 0/0A/0B保留为已完成历史，不再接受新增诊断或基础设施任务。当前执行顺序由父蓝图v2.25 Gate 2和§4.3.4控制：先聚合既有证据并闭合产品验收精确合同，只做一个spike，再以同一通过holdout的canonical输出完成generator/repository/API/UI纵向切片。除该最短路径所需的修改外，不得新增通用evidence框架、重复完整输入artifact、通用训练平台或调度器。

### Slice 0：catalog 与 schema contract

- 修改三份 validation catalog，登记 `hmm.risk` ownership/module/test plans。
- 新增 `backend/db/init_hmm_risk_schema.py` 和 exact schema tests。
- 新增受控 state-model-set artifact preparation：为两个已批准 L2 family 生成配对 direct L1 artifact，
  输出 `hmm_risk_state_model_set_v1` manifest；daily worker 不参与训练。
- C-008-B3-D4-L2-AUDIT-01 已证明历史 L2 artifact 的 D4-01 evidence 不足且 D4-02 post-fit 修正不合格；未来 Slice 0
  必须按 `hmm_risk_c008_b3_l2_retrain_a_v1` 生成新的受控 L2 identity，再与 direct L1 配对。不得覆盖历史 snapshot、
  只补 metadata、跳过 L2 或把旧 final parameters 视为已通过 numeric acceptance。
- C-008-A/B1 已证明当前 direct L1 preparation 合同不能完成 legacy 31/31；Slice 0 的 schema/direct preparation
  implementation 可继续保留。C-008-B3-DESIGN 的 D3-D7 精确合同已经批准，完整 model-set preparation 状态为
  `BLOCKED_C008_B3_IMPLEMENTATION_NOT_AUTHORIZED`；设计批准或 diagnostic completion 均不构成 Slice 0
  model artifact 完成。
- 仅 DEV DDL 验证；production DDL 独立 pending。

### Slice 0A：formal blocker targeted diagnostic（已执行、diagnostic-only）

- `C-008-B3-FORMAL-BLOCKER-DIAG-01` 已由 producer `ac3687c2…` 完成 target derivation、348-fit 两 fresh-process runner、
  3-entry D6 replay、compact canonical artifact和parser/hash验证；canonical SHA-256=`10287e84…cffe8`，状态为
  `VERIFIED_DIAGNOSTIC_COMPLETE_NO_SELECTION_NO_READY`。本 docs PR 只回填已存在证据，不重新执行诊断。
- target derivation 必须由 formal report canonical hash 和 candidate rejection summaries 机械生成；150 rejected pair、24 control、
  3 replay 任一不闭合即在 fit 前或 artifact 完成前 fail closed。
- 诊断未修改 D3-D6、family/sector/feature、seed schedule、hard semantic authority 或任何生产数据；结果只进入后续五类
  remediation 精确设计决策，不直接进入 selection、model/READY、generator/job 或 runtime。

### Slice 0B：D1 covariance-stage exact evidence（已执行、机制仍inconclusive）

- REFIT-02-B 的48/48真实fit与双fresh-process证据已经形成，但matched 20D仅保留typed covariance failure，缺少raw cell/shape/
  bit-pattern坐标证据，机制仍为inconclusive。
- `C-008-B3-D1-REFIT-03-COVARIANCE-DIAG-01`只扩展现有D1 runner的diagnostic receipt与writer/parser；保持三角色、8 seeds、
  两fresh processes和48/48 fits，不建设通用实验框架、scheduler、API/UI、数据库或runtime。
- 当前Slice状态为`SOURCE_MERGED_DIAGNOSTIC_EXECUTED_COMPLETE_INCONCLUSIVE_MIXED_SEED_PATTERN`。report canonical
  `7e8a1755…76b9`与两process payload `53574f62…088f`已闭合；该状态不构成D5/D6、selection、model/READY、数据库或runtime授权。

### G2-A：输入权威到首个真实L1轮动产品纵切

- 在同一Feature范围连续完成P2B adapter、601日0-fit预检、同范围程序缺陷修复、经用户授权的HR1 24-fit、唯一能力判定和最小canonical bundle；这些是内部动作，不是独立交付阶段。
- 只有`rotation_L1=AVAILABLE`时，继续使用同一bundle和一个由冻结request机械派生的input-complete历史交易日生成market regime与31个L1 score/state；其余三项能力显式`NOT_AVAILABLE`。
- 只新增该纵切必需的input adapter、state generator、最小repository、既有`overview`/`heatmap` read API、真实`/hmm-risk` L1热力图与typed前端adapter；不提前实现alerts/events/report、通用job或Phase 3调度器。
- UI必须显示完整31-sector分母、missing cells、coverage/abstention、validation basis、forward confirmation status及稳定reason code；使用真实backend和无mock浏览器验收。模型bundle、backend-only、static UI或空成功均不构成G2-A完成。
- `/hmm-risk`在该纵切验收后先通过DEV/安全端口直接访问以供真实功能验证；production/runtime activation仍须独立授权，`/hmm`默认重定向仍等待F-011～F-013完整验收，不能用G2-A局部能力伪造最终首页完成。

### G2-B：首个产品到扩展历史分析与预警纵切

- 复用G2-A同一计算入口、bundle和repository identity，扩展最近7个及已批准更长历史窗口、transition/severity、预警时序、横截面Rank IC/spread、命中/误报/漏报、稳定性、固定详情和retrospective report；不创建第二套状态计算路径。
- 后续`rotation_L2|risk_L1|risk_L2`仅在各自正式验收通过后接入；未通过能力继续typed `NOT_AVAILABLE`，不得用rotation state伪造risk warning或隐藏失败模块。
- API/UI扩展、产品指标和无mock浏览器验收在同一Feature范围闭合；manual job/worker、lease/fencing、revision与late-data仍不得提前建设。

### G2-C：真实产品到受控日任务与最终route闭合

- 在G2-B已验收产品identity上补共同水位、幂等日任务、revision/dedupe、late-data、受控runner和失败恢复，不建设Phase 3 scheduler。
- 完成跨层集成验收并确认F-011～F-013完整后，才将`/hmm`默认入口切换到`/hmm-risk`。

三个闭环都必须产生各自声明的真实业务结果；不得用schema-only、adapter-only、bundle-only、backend-only、static UI或历史receipt数量宣称闭环或Phase 2完成。

## 16. Verification Plan / 验证方案

### 16.1 Changed-file routing

每个 PR 严格执行：changed files → `file_ownership.yaml` → `module_registry.yaml` → `test_plans.yaml` →
primary module required plan。未映射文件先修 catalog。`impact_modules`、风险级别或默认矩阵不能触发额外模块测试。

### 16.2 Direct backend evidence

- `python -m pytest backend/tests/hmm_risk/test_schema.py -q`
- `python -m pytest backend/tests/hmm_risk/test_input_resolver.py -q`
- `python -m pytest backend/tests/hmm_risk/test_state_model_set.py -q`
- `python -m pytest backend/tests/hmm_risk/test_state_generator.py -q`
- `python -m pytest backend/tests/hmm_risk/test_alert_state_machine.py -q`
- `python -m pytest backend/tests/hmm_risk/test_revision_and_late_data.py -q`
- `python -m pytest backend/tests/hmm_risk/test_job_worker.py -q`
- `python -m pytest backend/tests/hmm_risk/test_api.py -q`
- `python -m pytest backend/tests/hmm_risk/test_retrospective_report.py -q`
- `python -m pytest backend/tests/hmm_risk/test_isolation.py -q`
- C-008-B1 fix-point 必须覆盖：hard/soft evidence 计算、filtered posterior causal 边界、mass/ESS/utility 数值、
  convergence 与 negative delta 独立记录、raw/clip covariance audit、fit failure stage、immutable report collision、
  `selection_performed=false`、`ready_artifact_write_performed=false` 和零 model output。
- C-008-B1 full receipt 必须固定 2 family × 31 sector × 8 seed=496 条记录，并回读 canonical report hash、
  dataset/mapping hashes、环境/算法版本与完整字段；它不执行正式阈值判定。
- D3-03-A/D4-02-A fix-point 必须覆盖：sector-local `R_sj` shape/finite/strict-positive；raw cluster zero variance 经
  `ν=1.0` 公式收缩但不 clip；完整 GaussianHMM 显式参数/identity；raw covariance shape/non-finite/non-positive；
  `L/U` 的 0.5% 闭区间边界内、恰好等于边界和越界；total/per-state/per-feature zero anomaly budget；M-step residual
  恰好等于 2% 和超过 2%；任一 denominator 非正或中间值 non-finite；禁止 post-fit clip/projection；smoothed posterior
  只用于 train covariance audit且不得进入 hard semantic/selection；`accepted/failed/insufficient_evidence` 到
  `covariance_valid` 的精确映射；receipt parser/hash 回读。
- D4-01-MAP-A fix-point 必须覆盖：MAP objective 精确 prior 公式与同参数状态；raw likelihood/MAP/prior/covariance
  non-finite；`ΔJ<-T`、`ΔJ=-T`、`abs(ΔJ)=T` 与略越界；MAP 数值收敛但 D4-02-A failed 时继续 M-step；
  D4-02-A accepted 但 MAP 未收敛时继续；joint stop 后不再 M-step；300 iterations 无 joint stop fail closed；
  raw likelihood negative delta 完整保存为 diagnostic warning但不替代 MAP authority；D5 score 使用 joint-stop raw `L_final`
  而不是 MAP J/history maximum；receipt/hash、typed reason、状态映射及无 validation/selection/model/READY 写入。
- D4-03-PERSISTENT-A fix-point 必须覆盖：train posterior shape 与 `N_train=0`；cell non-finite/negative；row-sum error 恰好
  `1e-12` 和略高于边界；top1-top2 margin 恰好 `1e-12` 和略高于边界；
  common gate 的 `max(5,ceil(0.01*N_train))`、1%、3 months、transitions 2/2；recurrent 的 runs 2/3 与
  max-run-share 恰好 `0.8`；persistent 的 share 略高于 0.8、count `max(30,ceil(10%*N))`、10%、6 months、2 runs、
  transitions 2/2 的等于/略内/略外边界；两路径互斥；2-run low-share、singleton、单 run persistent 均失败；
  ordered date/hash 缺失、重复、逆序和 canonical date gap evidence；
  observation-row run 不得冒充自然日连续；全部 reason 聚合、确定性 primary priority 与
  `accepted/failed/insufficient_evidence -> train_occupancy_valid` 映射；soft mass/ESS 不得补 hard evidence；
  validation/future utility 不可见；D4-03 accepted 不得覆盖 D4-01/D4-02 failure 或推导 D5/D6/READY。DIAG-02 与 DIAG-04
  sensitivity 的 legacy 0/8 -> 8/8 区分性必须保留，但历史 diagnostic 不得反写为正式 acceptance。
- D5-01-B fix-point 必须覆盖：L1 31/31与L2 131/131的level candidate完整性；每个family分别选择L1/L2 level-global
  restart且禁止per-sector stitching；D4-01 accepted-with-warning仍eligible并保留warning；`L_final/(N*d)`使用实际terminal
  likelihood且拒绝history maximum/rounded/validation值；canonical code order、odd-count median、`math.fsum` mean；
  min/median/mean逐维best与tolerance的等于/略内/略外边界；三维tie按schedule index；score missing/non-finite、无eligible
  candidate与repeat hash mismatch；四个access flags必须false；两个fresh process的inputs/filter pools/selected receipt bitwise
  equal；selection后不refit；L1/L2可不同seed但各level单一seed；D6失败不得reselection；historical DIAG mean/lex sensitivity
  只证明公式区分性，不得写selected identity。
- D6-01-B/D6-NA-A fix-point 必须覆盖：仅在 D5 selected identity 冻结后执行，validation/future utility 不得回流 selection；
  exact 182-day calendar ledger/date/source/hash 与首日 fitted `startprob_` causal prior；首日/中间/末日 observation unavailable 的
  transition-only update；feature 部分缺失时禁止 partial-dimension emission；observation available + utility unavailable 时 posterior 更新但
  evidence excluded；utility available + observation unavailable 时同样 excluded；posterior shape `(182,3)`、non-finite/negative、row-sum
  `1e-12` 闭边界和 evidence-row top1-top2 margin `1e-12` 开边界；future 5/10/20D component/combined utility identity；
  `max(5,ceil(0.02*N_evidence))` 的 count 与 2% ratio 分离、`N_evidence=29/30` 边界；month/run 1/2、incoming/outgoing 1/2、max-run-share 0.9
  闭边界；hard utility count/mean/ddof=1 variance；numeric gap 的等于/略高/略低边界；SE/95% separation/soft mass/ESS 只作
  diagnostic；缺失 calendar day 必须打断 run/transition且不能压缩日期；availability mask/ledger/hash 的 writer/readback mismatch
  必须 fail closed；不得新增 missing-ratio gate；assignment/evidence status 与最具体 reason 的确定性聚合；D6 failure 不得返回 D5 换 seed、refit、换 family 或
  拼接 sector；D6 accepted 不得推导 family/READY。DIAG-04 sensitivity 只能证明区分性，历史 flags 不得反写为正式 acceptance。
  还必须覆盖：v1 dense `.dropna()` source constructor 反例与v2满182日carrier正例；mask/positions/compact values 长度、顺序、重复、
  NaN/null和hash反例；full-20 raw availability与effective-19 projection顺序；`T/O/U/E` 四集合、T\E diagnostic tie不得按index赋值及diagnostic hard assignment
  不进入acceptance；base/amendment/composite三个version与两种selected artifact v2/v3 schema回读；旧D6-only receipt不得通过active
  READY；零-refit envelope的parent/child/D5/model-hash/semantic-source/producer lineage逐字段篡改反例；daily unavailable event不得进入
  failure/blocking arrays，而invalid ledger与`N_evidence=29/30`必须分别映射到assignment/evidence状态。
- L2 numeric provenance fix-point 已由 C-008-B3-D4-L2-AUDIT-01 完成：13/13 candidate snapshot 收敛为两份 exact SHA；
  两者均缺 immutable D4-01 history，且 262/262 entry 明确记录 post-fit covariance 修正。测试必须固定
  `likelihood_status=insufficient_evidence`、`covariance_status=failed`、primary/secondary reason 的优先级与 9+4 identity
  聚合；不得因 snapshot/job=`completed`、final logprob 或 metadata hash 存在而转为 accepted。
- L2 retrain contract fix-point 在未来实现获授权后必须覆盖：冻结 dataset/mapping/watermark/window 与 canonical 131 set/hash；
  两 family × seeds 42..49 × 131 sectors 的 `2096 fits/process` 完整性；两个 fresh process 的 `4192 fits` 与 bitwise
  receipt/model hash；D3-02-B/D3-03-A、D4-01-MAP-A/D4-02-A/D4-03-PERSISTENT-A 的逐 entry 状态和完整 receipt；D5 前 validation/future utility 不可见；
  D5-01-B按L2 131-entry train-only vector选择level-global identity，冻结后按D6-01-B生成semantic receipt；不覆盖旧SHA；
  任一family/level不完整时不得model/READY。该fix-point当前只进入设计，
  本 docs PR 不执行 fit 或生成 artifact。
- API/report fix-point 必须证明 `overview.model_set_acceptance_summary` 的 L1/L2 counts 闭合、warning 不计入普通 accepted、
  L1/L2 D4 version 的 non-null/null 边界、canonical empty warning hash、warning count/code/evidence hash 与 entry receipts 一致；
  blocking reason/hash 与 entry receipts 一致；字段缺失/hash drift 显式失败；retrospective report 与 overview 引用同一
  immutable warning/blocking evidence，API/report 任一端不得静默删除 warning 或 blocker。
- C-008-B3 实现 fix-point 在后续源码实现获得明确授权后启用；全部D3-D7设计合同已经闭合。实现必须覆盖：批准的完整restart schedule且不
  early-stop；KMeans/HMM 全参数 identity；likelihood/covariance/occupancy/validation evidence 的批准阈值边界；仅 31/31
  L1或131/131 L2 level candidate可参与selection；level-global seed且禁止per-sector拼接；selection score/tie-break；selection 对
  validation/future utility 不可见；selection 后不 refit；validation 从 fitted `startprob_` 重启 causal filtering；任一
  family blocked 时不得写 READY。未确认的阈值不得先写测试再反向成为业务合同。
- C-008-B3 artifact contract smoke 必须回读批准schedule的全部candidate摘要、四个selected family/level identities及配对关系、未选reason、
  完整算法/依赖/数值环境版本、validation mapping/receipt，并按批准的数值可复现性合同验证 selection receipt/hash。
- C-010-FORMAL-A 已获用户批准；源码实现 fix-point 必须覆盖：full-universe contributor ledger 而非 absence-only entries；
  `O_s/A_s` 有序日期集合及 `A_s ⊆ O_s`；availability 恰好/略低于 `0.90`；train-only 冻结后在 validation/replay/runtime
  原样应用且不改变业务 universe；price 与 moneyflow 两层 count/weight coverage及各自 weight denominator invalid；exact five-field
  moneyflow dependency；moneyflow numerator/denominator 同一 contributor set；L1 31/L2 131 的逐 feature cross-section `0.90`
  闭边界、四项 exact operator/reference、reference invalid、domain 隔离、缺失 sector NA、range rolling 不得 resurrect 当前 invalid date；
  pre/post mask hash 一致；7/20 维 feature order/hash 不变；
  policy/request/child/candidate/READY receipt 绑定；601 日四个 family/level `>=120` 行；两个 fresh-process preflight identity
  一致；任一 failure 时 request candidate 为空且 fit/selection/D6/model/READY/database/runtime flags 全为 false。该 fix-point 当前
  已完成正式设计复审但尚未进入源码，不得以测试名称或 diagnostic artifact 反向激活正式 policy。
- C-008-B3-FORMAL-BLOCKER-DIAG-01 已执行证据与未来 regression fix-point 必须覆盖：formal authority hash 全集；150 rejected pair 与 24 deterministic
  control 的无重复机械派生；精确 348-fit budget；selected legacy L1 seed 43 的 `801980/801970/801010` 三 entry replay；D4 refit
  完全不可见 validation/future utility；D6 replay 不 refit、不 reselect；逐 stage raw evidence 与 signed-distance 公式；两个
  fresh-process payload bitwise equality；artifact canonical hash/readback/collision；所有 selection/acceptance-change/model/READY/
  DB/runtime flags 为 false；同时固定 comparison `missing_evidence_entry_count` 与直接 `missing_evidence` 的不同语义，禁止把
  变长序列的路径差异误报为证据缺失。未来修改该 runner 时只运行本模块直接 plan，不自动运行完整 5184 grid。
- REFIT-03 covariance diagnostic未来fix-point必须覆盖：REFIT-02-B四份source authority与current-A5 role identity；三角色×8 seeds×
  两fresh processes的48-attempt/48-fit上限；fit返回后、任何validator/exception转换前捕获raw covariance；expected/actual shape、
  dtype/endianness/C-order、IEEE-754 bit pattern、finite `float.hex()`、non-finite枚举、inactive mask与framed matrix hash的重建；
  `+0.0/-0.0`、negative、NaN payload、正负Infinity、shape mismatch、buffer不可读和JSON `allow_nan=false`；initialization/post-fit
  evidence分离；三角色pair与inactive-coordinate-only/cross-coordinate/mixed-seed正反例；两process bitwise equality；v4-v6历史
  immutable readback与v7 writer collision；model failure和diagnostic completion独立；all-reasons aggregate；validation/future utility/
  D6/selection/formal acceptance/model/READY/database/runtime flags全为false。不得追加其他模块测试或执行formal grid。

旧 gate frozen 且不在 changed files 中，因此不运行 legacy/QE/Selection 模块测试。只有未来 PR 真实修改共享 artifact
contract 时，才能基于明确依赖边追加对应 contract smoke，并在验证证据中写明原因。

### 16.3 Direct UI evidence

- `npm run type-check`
- `npm run lint -- --file <changed hmm-risk files>`（若脚本支持 changed-file 参数）
- `npx playwright test frontend/tests/hmm-risk/hmm-risk.spec.ts`
- 真实安全端口 acceptance：L1/L2、7 日、cell detail、alerts、partial/failed/stale、renderer error、report。

### 16.4 Minimal gates

- changed-file compile/lint；
- direct contract/fix-point；
- `hmm_risk_backend` 或 `hmm_risk_ui` required plan；
- scope check；
- `git diff --check`；
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md --tier F2`。

广泛跨模块回归仅在明确 shared contract 改变时交给 CI/Validation Center/nightly，且记录具体原因。

## 17. Decision Index / 用户决策索引

| decision | exact question | status | implementation consequence |
|---|---|---|---|
| C-001 | pooled/coefficient-only 等不同 candidate 如何取得可证明的 `trending/neutral/fading` semantic state，哪些 candidate 经逐项确认不属于 sector-state producer | `RESOLVED_USER_APPROVED_C001_A` | 13 个 direct state producer 映射到两个 L2 model identity；4 个 coefficient-only candidate 明确非 state producer且显式报错 |
| C-002 | L1/L2 各自的 direct source；如需跨层 aggregation，其成分、PIT 权重、缺失、confidence 与版本公式 | `RESOLVED_USER_APPROVED_C002_A` | 使用 versioned state-model-set 的独立 direct L1/L2 HMM；禁止 posterior 跨层 aggregation |
| C-003 | retrospective adverse-outcome oracle 的 horizon、return、threshold/quantile、universe、缺失与 denominator | `RESOLVED_USER_APPROVED_C003_A` | 5/10/20 连续 return evidence；5D excess q20 次级 oracle；90% minimum coverage；OPPORTUNITY 单列 |
| C-004 | 是否迁移、包装或退役 legacy gate | `RESOLVED_NO_MIGRATION` | Phase 2 冻结旧 producer/consumer，不运行其测试 |
| C-005 | PR 是否可以自动合入 | `RESOLVED_PER_PR_USER_CONFIRMATION` | branch/commit/push/PR/CI 可继续；每个 PR 在 merge 前停下并取得用户明确确认 |
| C-006 | `sector_data` 是否需要持久化行业 PIT identity，股票 eligibility 是否另建规则 | `RESOLVED_USER_APPROVED_C006_A` | `sector_data` 保持 22 字段事实表；先复用全局股票池 PIT，再动态关联 `sw_index_member`，mapping snapshot/hash 写入 `hmm_risk` evidence；不执行 sector identity 生产 DDL/DML |
| C-007 | 两个 direct L1 family 如何从 PIT L2 constituent 构造全部 7/20 维 observation，并处理历史 code 表示、单位、权重、缺失和 causal rolling | `RESOLVED_USER_APPROVED_C007_A` | 使用 `hmm_risk_l1_stock_fact_observation_v1`：股票事实先聚合、L1 feature 重新计算、canonical 31/131、双 coverage evidence 和 fail-loud；禁止聚合 L2 feature/posterior或调用旧 4 维路径 |
| C-008-A | 固定 seed 42 失败后，是否先用同一冻结输入对 seeds 42..49 做不选 seed、不写 artifact 的完整事实诊断 | `VERIFIED_DIAGNOSTIC_ONLY_NO_SELECTION_NO_ARTIFACT` | 496 次拟合报告完成；legacy 无 31/31 seed，autocycle 局部成功不构成两-family READY；canonical report hash 固化于 4.3.2 |
| C-008-D1 | 是否按 C-008-A 新证据修订详细设计并阻塞 F-011 的实现就绪结论 | `RESOLVED_USER_APPROVED_C008_D1` | 回填证据、拆分独立状态与 READY 合取；不改变 runtime；当前文档 PR 合入已于本次另获用户明确授权 |
| C-008-B1 | 是否补充 soft posterior、covariance 与 convergence 的只读诊断证据 | `VERIFIED_DIAGNOSTIC_ONLY_NO_SELECTION_NO_ARTIFACT` | canonical report `4728e75e8c059d38688bcd969d19379d71b2ad5e9cd5e07ff69138c184462722`；同一冻结输入与 seeds 42..49；未定义正式阈值、未改变 hard semantic authority、未选择 seed、未写 READY artifact |
| C-008-B2 | 是否采用 posterior-weighted semantic authority | `NOT_APPROVED` | B1 不支持直接采用；soft evidence 仅诊断，不参与 mapping、selection 或缺态补足 |
| C-008-B3-DESIGN | 是否保留 hard semantic authority并重构 initialization/restart/occupancy/selection | `DIRECTION_APPROVED_EXACT_CONTRACT_PENDING_USER_CONFIRMATION` | 已批准 hard authority、train-only family-global selection、两 family完整和禁止 validation-driven/per-sector selection；后续已批准结构项与仍待决数值见以下独立 decision rows |
| C-008-B3-STRUCTURAL-A | 是否采用显式 KMeans/manual HMM initialization、完整 restart 与禁止 post-fit projection 的结构方向 | `RESOLVED_USER_APPROVED_STRUCTURAL_CONTRACT` | D3-01-A/D3-02-B/D3-03-A 均已生效；绝对 `[1e-3,10]` DIAG-02 profile 未获批准且已由 scale-aware contract 替代 |
| C-008-B3-D3-01 | restart schedule、数量和完整运行规则 | `RESOLVED_USER_APPROVED_D3_01_A` | 两个 family 各自完整运行 seeds 42..49 × 31 sectors；不 early stop、不扩 grid、不按 sector 拼接 |
| C-008-B3-D3-02 | KMeans initialization identity | `RESOLVED_USER_APPROVED_D3_02_B` | `k-means++/n_init=1/restart_seed/max_iter=300/tol=1e-4/lloyd/copy_x=true`；空或少于 2 成员 cluster fail closed |
| C-008-B3-D3-03 | GaussianHMM 全参数和 sector-local initialization covariance 精确数值 | `RESOLVED_USER_APPROVED_D3_03_A` | sector-local `R_sj`、`ν=1.0` shrinkage initialization/prior、`covars_weight=2.0`、`min_covar=0.0`、完整显式 GaussianHMM profile 与禁止 pre/post-fit clip/projection 已批准；active convergence authority 为 D4-01-MAP-A |
| C-008-B3-DIAG-02 | 是否在固定数值环境按批准结构运行两次完整只读结构诊断 | `VERIFIED_DIAGNOSTIC_ONLY_NO_SELECTION_NO_ARTIFACT` | 992 fits、两次 canonical payload hash 相同；补齐 likelihood、covariance、month/run/transition/occupancy evidence，未执行正式 D4/D5-01/D6 |
| C-008-B3-D4-01 | historical raw-likelihood monitor acceptance | `SUPERSEDED_BY_D4_01_MAP_A` | `hmm_risk_c008_b3_d4_01_a_v1` 保留为历史 receipt/readback identity；不得用于新 fit 的停止或 candidate acceptance |
| C-008-B3-D4-L2-AUDIT-01 | 既有 L2 131/131 是否具备可按 D4-01-A/D4-02-A 回读的 immutable training/numeric receipt | `VERIFIED_FAIL_CLOSED_LIKELIHOOD_INSUFFICIENT_COVARIANCE_FAILED` | 13/13 candidate snapshot 收敛为 legacy 9 + autocycle 4 两份 exact SHA；两者均缺完整 D4-01 history，262/262 entry 均有 post-fit covariance 修正。likelihood 保持 insufficient、covariance 为 failed；禁止 grandfather、补 metadata、复制 L1 evidence 或 READY |
| C-008-B3-D4-L2-RETRAIN-DESIGN-A | 是否在不覆盖历史 artifact 的前提下，以冻结输入和已批准 D3/D4 合同受控重训两 family 的 131/131 direct L2 | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_FORMAL_EXECUTED_BLOCKED` | 使用 `hmm_risk_c008_b3_l2_retrain_a_v1`、当前冻结 dataset/mapping/direct-L2、seeds 42..49、两 fresh process；正式 5184-fit grid 已包含两个 family 的 direct L2 4192 fits，未覆盖历史 artifact。两个 L2 family 均因 D3/D4 rejection 无 eligible D5 candidate，model/READY 和 runtime 未执行 |
| BUG-868-A | 正式 B3 是否保留 exact previous-trading-date 语义并重新冻结与当前 source 一致的 L1/L2 identities | `RESOLVED_USER_APPROVED_REFREEZE_PREFLIGHT_MERGED` | 保留 `previous_basic_date == previous_trade_date`；正式 identity 固定为 dataset `c07177…`、mapping `9cdddd…`、direct L2 `d4a5cc…`。source PR #2748 merge `44bc9e8a…`、close-sync PR #2752 merge `1ad5ff62…`；旧 `fca206…` 仅为历史诊断 identity |
| BUG-877 | frozen train source 的 exact daily-basic 与 moneyflow symbol-key 缺口如何受控修复 | `SOURCE_IMPLEMENTED_PREFLIGHT_BLOCKED_TRAIN_COVERAGE` | C-009 source 实现与 601 日只读 preflight 已完成；591 alias、502 provider absence、348 causal stale circ-mv 均有 hash evidence；preflight 因既有 train coverage 合同 blocked，禁止恢复训练 |
| C-009-A | 停牌导致前一市场交易日无 daily-basic 时如何取得 causal circ_mv | `IMPLEMENTED_VERIFIED_READONLY` | 使用 `<t` 且 `<=prev_market_trade_date(t)` 的最新 authoritative circ_mv，并持久化 source date/staleness；无 causal row fail closed；601 日发现 348 个 stale key，最大 30 个市场交易日 |
| C-009-B | 新旧证券代码如何在不改写 raw source 的前提下连接同一稳定证券身份 | `IMPLEMENTED_VERIFIED_READONLY` | immutable source-specific resolver 解析 591 个 moneyflow key；canonical/source/authority/hash evidence 完整，raw source 未改写 |
| C-009-C | Tushare authority 不存在的 stock/date moneyflow 如何使用 NA | `IMPLEMENTED_VERIFIED_READONLY` | provider-audit manifest 精确绑定 502 个 NA key；继续执行 0.90 count/weight coverage，未填 0/前值/代理，因 coverage 不足正确 blocked |
| C-009-D | 三类缺口按什么顺序实现和恢复正式训练 | `PREFLIGHT_EXECUTED_BLOCKED_TRAIN_COVERAGE` | 601 日 source-only preflight 已执行且无 DB/runtime write；legacy L1 31/31、legacy L2 130/131、autocycle L1/L2 0 complete；不得训练、selection、D6 或 READY |
| BUG-886 | provider absence 如何避免放大为无关 feature/sector 的全局 train coverage failure | `SOURCE_IMPLEMENTED_TARGETED_VALIDATION_PASSED_FORMAL_PREFLIGHT_PENDING` | 正式/诊断 schema、统一 denominator、full-universe ledger、双层 coverage、formula v2、逐 feature cross-section 与 policy lineage 已实现；历史 601 日 diagnostic report `2b1f4acc…7260` 仍只作诊断证据，新的 post-fix formal preflight 与训练继续 blocked |
| C-010-DIAG-01 | 是否先执行 601 日 feature-domain eligibility/mask 只读诊断 | `VERIFIED_AFTER_REVIEW_FIX_NO_FIT_NO_SELECTION_NO_ARTIFACT` | 当前 source ancestry 的 clean producer report canonical `2b1f4acc…7260`；5 个 exact opportunity hashes 完整，仅排除 `689009.SH` moneyflow contribution，四项 candidate valid 且无需删 feature；旧 `ded02740…251f` 仅为 failed-review identity，`ac218d78…6b3ae` 为 pre-rebase verified evidence |
| C-010-FORMAL-A | 是否采用 full-universe train-frozen contributor ledger、price/moneyflow 双层 coverage、同源 moneyflow denominator 与逐 feature `0.90` cross-section 的正式政策 | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_FORMAL_PREFLIGHT_AND_GRID_EXECUTED` | 用户于 2026-07-28 明确批准；version=`hmm_risk_c010_feature_domain_policy_v1`。clean-main formal preflight 与正式 5184 fits 已绑定 policy SHA `ae8eda5b…220d9` 完成；该历史执行仍有效，但后续D1-B refit暴露A5 domain-partition gap，新的policy/refit在A5闭合前blocked。仍未生成READY |
| C-010-A1 | 是否以 full-universe `O_s/A_s` ledger 冻结 train-derived moneyflow contributor eligibility | `RESOLVED_USER_APPROVED_C010_A1` | 每个 train opportunity symbol 显式入账，`A_s ⊆ O_s`，availability `>=0.90` eligible；train 后新证券保留业务/price eligibility但 moneyflow=`train_eligibility_unavailable`，不得默认 eligible或删除证券 |
| C-010-A2 | 是否采用 price/moneyflow 两层 count+weight coverage 与同一 moneyflow contributor set 的 numerator/denominator | `RESOLVED_USER_APPROVED_C010_A2` | price denominator 不受 moneyflow exclusion影响；moneyflow expected set仅含 A1 eligible contributor，exact five fields固定，count/weight均`>=0.90`；price/moneyflow weight denominator invalid 独立 fail closed；四项 moneyflow ratio使用严格正的`moneyflow_contributor_amount`，不填值或借用`l1_amount` |
| C-010-A3 | 是否把 exact-complete 全局传播改为按 feature domain 独立的 `0.90` cross-section | `RESOLVED_USER_APPROVED_C010_A3` | 每个 feature/date/level保存 expected/valid/missing sector set/hash及exact operator/reference；达到0.90只对valid set计算、缺失sector保持NA，不足仅该feature/date invalid；L1/L2最小valid count为28/118；reference invalid、output non-finite、pre/post mask mismatch独立fail closed，rolling不得复活当前invalid date |
| C-010-A4 | 是否升级 formula/policy identity并以601日formal preflight绑定后续B3执行 | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_FORMAL_EXECUTION_VERIFIED` | feature set/order/rolling不变；formula=`hmm_risk_l1_sector_factor_formula_v2_c010`。601日 formal preflight、request、两个 child、D4/D5/D6 receipt 已使用同一 policy identity；fit 前 coverage 条件通过。READY 因下游模型验收 blocked，不能倒推 A4 失败或重新启用 v1 |
| C-010-A5 | 是否把 full-market provider-absence audit domain 与要求有效 direct L1/L2 SW identity 的 contributor opportunity domain 显式分区 | `RESOLVED_USER_APPROVED_SOURCE_MERGED_601_DAY_PREFLIGHT_VERIFIED` | 逐key `P_all/P_in/P_out`完备分区、typed predicate authority、v2 writer/readback与policy lineage已合入；601日只读preflight canonical `e7f7edc9…773d`、partition canonical `03d78534…ead6`，结果`502/501/1`并验证`002951.SZ@2023-05-22`为SW-domain-out。不得伪造行业、删除absence/证券、改变PIT/runtime prediction、阈值、feature、hard semantic或D3-D6 |
| BUG-892 | PIT entry day 的 causal `circ_mv` 为什么形成结构性 denominator failure | `SOURCE_REVIEW_FIX_VERIFIED_DENOMINATOR_COMPLETE_DOWNSTREAM_TRAIN_COVERAGE_BLOCKED` | producer `77265dd6...` 的 601 日 no-fit receipt `7c36f228...fdd1ca`：crossing total/available/invalid=`1073/1073/0`，history start=`2020-07-30`，ordered key hash=`0b89a9d5...53c8f19`；PIT-entry denominator 根因闭合。整体 preflight 仅因既有 train observation coverage blocked（legacy L2 `801881.SI=102<120`、autocycle L2 coverage）而保持 blocked；未执行 fit/selection/D6/model/READY/DB/runtime action，且不改变 PIT、return 或 hard semantic 语义 |
| BUG-870 | formal preflight 是否在grid前闭合四个family/level的train coverage并持久化child typed failure | `SOURCE_FIX_MERGED_CURRENT_FORMAL_PREFLIGHT_PASSED` | 完整 coverage preflight 与 typed child failure 已合入；当前 clean-main request 通过该 fail-closed gate并运行正式 grid。历史首-fit前 blocker仍保留为旧 receipt，不再代表当前执行状态 |
| C-008-B3-D4-02-DIAG-03 | 是否仅重聚合 sector-local covariance reference 与候选 bounds sensitivity | `VERIFIED_DIAGNOSTIC_ONLY_NO_REFIT_NO_SELECTION_NO_ARTIFACT` | canonical report `22ee3536b4dc6590c27fa6c2989bc830d3d5d336e71b193fd17801d7c62a7e43`；统一 `[1e-4,200]` 被证据否定，未批准替代 bound |
| C-008-B3-D3-03/D4-02-DIAG-04 | 是否用 scale-aware initialization/prior 在固定环境执行两次完整 refit 诊断 | `VERIFIED_DIAGNOSTIC_ONLY_NO_SELECTION_NO_ARTIFACT` | producer `94abea6c...`；992 fits；payload hash `3abb384e...19aac` bitwise equal；report canonical `2c9136d5...74c9b`；无正式 acceptance、selection、model/READY/DB/runtime write |
| C-008-B3-D4-01-MAP-A | covariance-prior 一致 MAP objective、数值包络与 D4-02-A 联合停止 | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_LOCAL_VERIFIED_PENDING_PR_MERGE` | `hmm_risk_c008_b3_d4_01_map_a_v1`；`J=L-0.5*Σ[(w-1)log(c)+prior/c]`；`T=max(1e-8,sqrt(eps64)*max(1,abs(J_prev)))`；MAP 非有限/超包络下降 fail closed；仅 MAP 数值收敛与同参数状态 D4-02-A 同时通过才停止；raw likelihood 完整诊断，D5 仍取 joint-stop raw L；D5/READY 对 fully-rehashed D4 receipt 使用 writer authority 重算并保留具体失败；79 项直接 fix-point、365 项 module plan 均已通过 |
| C-008-B3-D4-02 | covariance reference/bounds/floor/anomaly budget | `RESOLVED_USER_APPROVED_D4_02_A` | dynamic `L/U`、`τ_bound=0.005` 闭区间、tolerance 后 total/per-state/per-feature zero anomaly、M-step residual `<=0.02`、raw-only posterior 与禁止 clip/projection 已批准 |
| C-008-B3-D1-REFIT-03-COVARIANCE-DIAG-01 | REFIT-02-B matched 20D covariance failure是否严格局限于exact-zero inactive coordinate，如何保存raw shape/cell/bit-pattern与三角色pair证据 | `VERIFIED_DIAGNOSTIC_COMPLETE_INCONCLUSIVE_MIXED_SEED_PATTERN` | producer `b474170f…`、48/48 fits、report canonical `7e8a1755…76b9`、两process payload `53574f62…088f` bitwise equal；treatment/harness 16/16 `fit_completed`且descriptive covariance accepted，matched 16/16 covariance failed，seeds 45/47/48仅feature 19失效而其余seed全active set失效。formal acceptance=false、D5 readiness=false；未执行selection/D6/model/READY/DB/runtime |
| C-008-B3-D1-REFIT-03-RESULT-AUDIT-01 | REFIT-03执行事实、D1证据边界、D5兼容选项与后续最小实现顺序是否完整回填 | `VERIFIED_RESULT_AUDIT_COMPLETE_DECISIONS_LATER_RESOLVED` | canonical `7e8a1755…76b9`与mixed-seed/inconclusive结论已回填；当次审核保持pending，后续用户已批准`POST-REFIT03-A`和`REMEDIATION-D1-D5-COMPAT-01-A`。未把descriptive accepted改写为formal acceptance，未新增经验性score阈值或自动淘汰方案 |
| C-008-B3-D4-03-PERSISTENT-A | train hard common gate 与 recurrent/persistent 互斥双路径 | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_LOCAL_VERIFIED_PENDING_PR_MERGE` | `hmm_risk_c008_b3_d4_03_persistent_a_v1`；common gate 保留 count1%/occupancy1%/month3/transitions2/posterior numeric；recurrent=`runs>=3 && share<=0.8`；persistent=`share>0.8 && count>=max(30,ceil(10%N)) && occupancy>=10% && months>=6 && runs>=2 && transitions>=2`；hard authority不变，历史诊断不反写 acceptance；exact boundary与anti-singleton正反例已通过 |
| C-008-B3-D5-01 | train-only family/level-global identity、score/aggregation/tie-break | `RESOLVED_USER_APPROVED_D5_01_B` | `hmm_risk_c008_b3_d5_01_b_v1`：每family分别选择L1 31/31与L2 131/131 level-global seed；`L_final/(N*d)`；min/median/`math.fsum` mean lex maximize；relative+absolute tolerance逐维过滤，最终按schedule index；validation/D6不可见、D6失败不得reselection；historical DIAG不写selection |
| C-008-B3-D5-02 | 固定数值环境内的可复现性 | `RESOLVED_USER_APPROVED_D5_02_B_FIXED_ENVIRONMENT` | 两个 fresh process canonical hash 必须 bitwise equal；不外推跨 host/BLAS/依赖版本 |
| C-008-B3-D6-01 | hard semantic validation count/month/run/utility gap | `RESOLVED_USER_APPROVED_D6_01_B` | `hmm_risk_c008_b3_d6_01_b_v1`：selected restart 后的 hard authority；每 state count `>=max(5,ceil(2%*N))`、occupancy `>=2%`、month/run `>=2`、incoming/outgoing `>=2`、max-run-share `<=0.9`、posterior row-sum `<=1e-12`、margin严格 `>1e-12`；hard utility mean/variance finite、numeric adjacent gap；95%/soft evidence只诊断，失败不得换 seed |
| C-008-B3-D6-NA-A | 冻结 validation calendar 中 observation/utility NA 如何保持 causal posterior 与 hard semantic evidence | `RESOLVED_USER_APPROVED_SOURCE_PR_3258_REVIEW_FIXES_VERIFIED_PENDING_MERGE_NO_REPLAY` | `hmm_risk_c008_b3_d6_na_a_v1`：保留182日完整ledger；feature-NA日transition-only且不插补，utility-NA日保留posterior但不进入evidence；existing D6 gates仅在`E=observation_available AND utility_available`执行，gap打断run/transition；保留既有30行source contract且不新增missing-ratio gate；carrier/manifest v2、full20→19顺序、T/O/U/E、composite selected schema、zero-refit lineage与reason/status映射已实现并通过直接测试；历史 C-008 入口显式保持 dense diagnostic v1，zero-refit 与全部 hidden child identity 在 dispatch 前互斥；本状态不表示已执行replay、D6 accepted或READY |
| C-008-B3-TRAIN-STABILITY-DIAG-01 | 11个完整输入D6结构失败应先修D5 train-only eligibility还是修改transition/dwell模型机制 | `VERIFIED_DIAGNOSTIC_COMPLETE_NO_COMPLETE_SEED_TRANSITION_DWELL_DESIGN_REQUIRED` | producer `7d57d57e…d190`；1048/1048 profiles、131/131 source comparisons、8个seed完整候选均为0；canonical `9c449e04…c5b1`。selected seed43的11项中6项train instability、5项validation-only collapse；0 fit/refit/selection/D6/model/READY/DB/runtime。证据只支持后续精确设计，不自动批准机制 |
| C-008-B3-D5-STABILITY-ELIGIBILITY-A | 是否把两段train-only hard结构稳定性加入D5 eligibility | `NOT_SELECTED_INSUFFICIENT_COMPLETE_CANDIDATE_SET` | DIAG-01下8个seed均不是131/131双窗口稳定；单独增加该gate会清空D5 candidate set，不能解除F-011 blocker。不得用validation/D6重新选择 |
| C-008-B3-TRANSITION-DWELL-B | 是否以train-only transition MAP prior改善跨窗口state structure | `VERIFIED_DIAGNOSTIC_COMPLETE_NO_COMPLETE_CANDIDATE_NO_SELECTION_NO_READY` | `hmm_risk_c008_b3_transition_dwell_b_v1`源码与BUG-1068 receipt闭合修复已合入；treatment `29417ceb…f8996fe`、source `2ae9df85…be7fa`完成2096/2096 fits，两process entry/model/profile hashes bitwise一致，但完整候选seed为0。parent body canonical `b6312171…582db`、完整对象canonical `e5f355fc…d4b54`；未执行D5/D6/selection，未写model/READY/DB/runtime。结果保持F-011 blocked，不授权自动调参或扩大scope |
| C-011-A-PRODUCT-ACCEPTANCE-UNIT | Phase 2是否继续以逐sector三态结构全局合取作为产品验收单位 | `RESOLVED_USER_APPROVED_P2_3_A_EXACT_METRICS` | 产品主验收为日期×canonical L1/L2横截面的样本外轮动/风险效果；§4.3.4.2 D4已固定Rank IC/spread/risk/Newey-West/availability公式与阈值，尚未执行 |
| C-011-B-READY-AND-COVERAGE | FULL_READY、COVERAGE_AVAILABLE与NOT_AVAILABLE如何分离 | `RESOLVED_USER_APPROVED_P2_3_A_EXACT_COVERAGE` | D5已固定canonical denominator、代表性分组与三状态阈值；COVERAGE_AVAILABLE不得冒充FULL_READY或由time-box自动获得，尚未执行 |
| C-011-C-MODEL-FAMILY-AND-RESTART | family角色与per-sector restart是否继续由历史B3固定 | `RESOLVED_USER_APPROVED_P2_3_A_POOLED_GLOBAL_RESTART` | P2-3只使用autocycle显式五项子集和每component全局restart；不采用per-sector stitching，legacy保留历史研究角色；selection与holdout隔离见D1～D3 |
| C-011-D-SINGLE-PARADIGM-SPIKE | 下一轮是否并行建设多个模型范式 | `P2_3_C_SINGLE_DIRECTION_FORMAL_CANDIDATE_FROZEN` | P2-3A/P2-3B均为NOT_AVAILABLE；唯一P2-3C已完成36/36并冻结candidate。不得重跑、组合、降门禁、并行其他范式或在P2-4失败后搜索新模型 |
| C-011-P2-3-D1 | 是否采用market K=2 + level-local pooled sector-relative K=3的单一jump结构 | `EXECUTED_NOT_AVAILABLE_FOR_PROMOTION` | P2-3A v2执行296/456 fits后在L1 selection fail closed；market局部成功不构成候选，L2未运行，禁止子集交付 |
| C-011-P2-3-D2 | train-only preprocess、centroid semantic score与hard causal margin合同 | `EXECUTED_PRODUCT_DIRECTION_FAILED` | 18个selected-fit hash闭合；centroid mapping在正式10D指标上18/18 spread为负，不得反转旧label或降门禁冒充修复 |
| C-011-P2-3-D3 | λ/grid/folds/restart与development selection | `EXECUTED_TYPED_SELECTION_UNAVAILABLE` | failure report canonical `034fdf3c…12ec`；market进入lambda4 full-development，L1六个lambda均无三foldspread coverage，L2未执行；holdout未访问 |
| C-011-P2-3-D3-MARKET-ZERO-EVENT-A | market零正例fold的pooled/negative-control合同 | `RESOLVED_USER_APPROVED_SOURCE_MERGED_EXECUTED_MARKET_COMPONENT_ACCEPTED` | PR #3515已合入；v2正式执行证明market合同可形成development选择，但不能代替L1/L2完整候选 |
| C-011-P2-3-D4 | Rank IC/spread/risk/Newey-West与holdout产品阈值 | `ACTIVE_PRODUCT_AUTHORITY_P2_3_A_FAILED` | 公式和阈值未被失败否定；它们正确暴露P2-3A负向预测结果。P2-3B不得降低阈值，只能改变经用户批准的新模型语义 |
| C-011-P2-3-D5 | FULL_READY/COVERAGE_AVAILABLE/NOT_AVAILABLE精确合同 | `ACTIVE_P2_3_A_NOT_AVAILABLE` | P2-3A没有完整candidate，故不能进入P2-4或获得coverage状态；canonical model/READY仍为0 |
| C-011-P2-3-D6 | 456 pooled-fit与promotion停止边界 | `EXECUTED_FAIL_CLOSED_296_OF_456` | 在L1无candidate后正确停止剩余160个L2 fits；未写model/READY/DB/runtime，未自动启动第二范式 |
| C-011-P2-3B-D1 | market jump + L1/L2 pooled Ridge、rotation score与forecast-state新identity | `VERIFIED_SOURCE_IMPLEMENTED_FORMAL_EXECUTED` | 新origin、market/L1/L2 component边界与forecast-state语义已按正式request执行；market完成，L1开发验收失败后按合同未进入L1 final/L2 |
| C-011-P2-3B-D2 | 五项relative输入与daily-centered 10D future excess target | `VERIFIED_FORMAL_EXECUTED_INPUT_AND_TARGET_CLOSED` | PIT/t-1/preprocess、末10日purge、L1 denominator与target hashes闭合；15个L1 fold的target/state unavailable均为0，未读取holdout |
| C-011-P2-3B-D3 | Ridge参数、alpha grid与三folddevelopment selection | `VERIFIED_EXECUTED_DEVELOPMENT_EFFECT_NON_POSITIVE` | 五alpha均eligible并选择alpha100；三foldmedian Rank IC=-0.007807285873192439、median spread=0.0009441908663057883，双正向条件未满足 |
| C-011-P2-3B-D4 | rotation score到trending/neutral/fading的daily cross-section投影 | `VERIFIED_L1_DEVELOPMENT_METRICS_COMPLETE` | 三foldRank IC/spread覆盖均116/116，逐日metric重聚合与receipt一致；产品方向跨fold不稳定，不得靠局部正值冒充通过 |
| C-011-P2-3B-D5 | compact candidate、P2-4 holdout和三状态闭包 | `VERIFIED_NOT_ENTERED_NO_DEVELOPMENT_CANDIDATE` | 开发停止条件失败，故未形成compact candidate、未进入P2-4、未访问holdout，model/READY/coverage均为0 |
| C-011-P2-3B-D6 | 单模块184-fit最小spike与zero-side-effect | `VERIFIED_FAIL_CLOSED_167_OF_184` | market152+L1 fold15=`167/184`后typed停止；L1 final与L2未运行，DB/runtime/model/READY flags均为false |
| C-011-P2-3B-POSTRUN-AUDIT-01 | P2-3B正式执行是否完整、可复核且足以形成产品候选 | `VERIFIED_EXECUTION_INTEGRITY_MODEL_NOT_AVAILABLE` | producer `24e4ae79…d8a4`；failure canonical `d3298654…fc45`；491个嵌套hash和指标重聚合闭合。结论为执行完整、模型验收失败，不登记BUG、不进入P2-4 |
| C-011-P2-3C-D1 | 是否以market-conditioned Ridge作为唯一第三个development候选 | `VERIFIED_FORMAL_EXECUTED_CANDIDATE_FROZEN` | 唯一identity保留market K=2、L1/L2共同交付；producer `8ca1b98d…`完成36/36并冻结candidate，不并行模型、不复制旧artifact |
| C-011-P2-3C-D2 | market regime如何进入sector predictor且保持train-only causal | `VERIFIED_FORMAL_EXECUTED` | 固定lambda4/seed42逐fold train-only拟合与causal zero-start闭合；BUG-1122修复合法sector-date子集后正式执行通过 |
| C-011-P2-3C-D3 | market条件特征与estimator参数 | `VERIFIED_FORMAL_EXECUTED` | 十维`[x,m*x]`、alpha100 L1/L2 final fit与interaction identity闭合，无独立m列或新依赖 |
| C-011-P2-3C-D4 | target、selection与产品验收是否变化 | `VERIFIED_DEVELOPMENT_ACCEPTANCE_PASSED` | L1/L2 development median Rank IC与spread均严格为正；regime-split仅诊断，holdout未访问 |
| C-011-P2-3C-D5 | candidate、holdout和第三次development审计边界 | `VERIFIED_CANDIDATE_FROZEN_PENDING_P2_4` | report canonical `792d4f6a…17e3`，attempt index 3、前两NOT_AVAILABLE hashes与holdout=false闭合；不是model/READY |
| C-011-P2-3C-D6 | 最小实施、36-fit预算与停止条件 | `VERIFIED_FORMAL_36_OF_36_ZERO_RUNTIME_SIDE_EFFECT` | 36/36 fits、6/6 component hash闭合，DB/runtime/model/READY均false |
| C-011-P2-4-D1 | 唯一candidate与一次逻辑holdout identity | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED` | 固定candidate `792d4f6a…17e3`、242 state dates与独立outcome tail；preflight前不读业务holdout，访问后不得换路径重试或reselect |
| C-011-P2-4-D2 | 双fresh-process如何复现而不refit | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED` | 0 fit；两个child加载同一冻结参数并独立产生causal state/score/metric/coverage，所有payload hash必须bitwise一致 |
| C-011-P2-4-D3 | untouched holdout产品指标和阈值 | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED` | 沿用D4：L2 Rank IC/spread与NW t-stat、L1正向、risk lift/recall、季度稳定性和80% metric coverage全部同时验收 |
| C-011-P2-4-D4 | coverage/representativeness与三状态 | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED` | 沿用D5 canonical denominator及quintile/parent代表性；FULL_READY、COVERAGE_AVAILABLE、NOT_AVAILABLE严格互斥 |
| C-011-P2-4-D5 | canonical model/READY最小writer | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED` | FULL_READY写model+READY；COVERAGE_AVAILABLE只写带coverage manifest的model且ready=false；NOT_AVAILABLE只写acceptance/failure |
| C-011-P2-4-D6 | 最小源码、测试与停止条件 | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED` | 只新增holdout service/薄CLI/直接测试，0 fit、无新依赖/DB/runtime；失败不得回流P2-3或开启新模型 |
| C-012-D1 | canonical authority是否拆为bundle与四个能力 | `RESOLVED_USER_APPROVED_BLUEPRINT_DIRECTION_EXACT_F2_PENDING` | 一个versioned bundle；rotation L1/L2与risk L1/L2独立component/identity/验收，不强制一个estimator承担全部任务 |
| C-012-D2 | 能力、coverage与顶层状态如何分离 | `RESOLVED_USER_APPROVED_BLUEPRINT_DIRECTION_EXACT_F2_PENDING` | FULL_READY/CAPABILITY_AVAILABLE/NOT_AVAILABLE与能力coverage双轴；局部能力不得冒充完整交付，旧P2-4结果不追认 |
| C-012-D3 | risk identity mismatch与abstention如何验收 | `RESOLVED_USER_APPROVED_BLUEPRINT_DIRECTION_EXACT_F2_PENDING` | 四类identity完整报告；recall保留事件分母，precision与abstention分开；禁止inner-join缩分母或补negative/neutral |
| C-012-D4 | 已消费holdout与下一样本外协议 | `RESOLVED_USER_APPROVED_BLUEPRINT_DIRECTION_EXACT_F2_PENDING` | 旧窗口只作历史证据；新component须预注册walk-forward与新untouched窗口，精确日期/阈值/事件数仍待F2批准 |
| C-012-D5 | CAPABILITY_AVAILABLE是否允许进入P2-5 | `RESOLVED_USER_APPROVED_BLUEPRINT_DIRECTION_EXACT_F2_PENDING` | 至少一个正式能力可用后允许真实单日离线oracle；未通过能力必须typed不可用，不构成FULL_READY或runtime授权 |
| C-012-D6 | 下一模型任务与停止条件 | `RESOLVED_USER_APPROVED_BLUEPRINT_DIRECTION_EXACT_F2_PENDING` | 一次一个直接能力假设；禁止四能力并行、无界诊断、通用平台或保证指定模型成功 |
| C-012-RL1-D1 | rotation_L1唯一component identity、estimator与输入 | `SOURCE_IMPLEMENTED_SUPERSEDED_BY_HR1_FORMAL_RESULT` | 原market K2/Ridge alpha100/feature/target identity保留为HR1历史权威；RW1如获批准必须使用新identity |
| C-012-RL1-D2 | development与walk-forward | `FORMAL_EXECUTED_VIA_HR1_NOT_AVAILABLE` | 原五fold在HR1执行；4/5与median IC通过，但median spread和两项OOF NW t失败 |
| C-012-RL1-D3 | 新untouched holdout与产品阈值 | `NOT_ENTERED_HR1_STOPPED_BEFORE_HOLDOUT` | development失败后holdout保持未读；不得复用或把未读状态冒充通过 |
| C-012-RL1-D4 | L1 coverage、abstention与bundle状态 | `FORMAL_EXECUTED_INSUFFICIENT_NO_BUNDLE` | historical coverage缺口显式保留；rotation_L1/CAPABILITY_AVAILABLE均未形成 |
| C-012-RL1-D5 | candidate/component/bundle最小writer | `FAILURE_RECEIPT_ONLY_NO_MODEL_OR_BUNDLE` | append-only failure闭合；model/bundle/READY/DB/runtime均未写 |
| C-012-RL1-D6 | 24-fit复现、最小源码与停止 | `FORMAL_STOPPED_10_OF_24_PER_CONTRACT` | fresh process 1完成10个fold fit后经济验收失败；第二进程/final/holdout未运行，旧request不得重跑 |
| C-012-RL1-HR1-D1 | 模型、输入与选择冻结 | `FORMAL_EXECUTED_IDENTITY_CLOSED` | producer `5c1a90a7…9f`；固定estimator/feature/target/alpha/lambda/seed，无selection、search或第二candidate |
| C-012-RL1-HR1-D2 | 五fold历史因果回放 | `FORMAL_EXECUTED_DEVELOPMENT_EFFECT_UNAVAILABLE` | fresh process 1完成10个fold fit后验收停止；median Rank IC通过，median spread及两项OOF NW t失败；parent `d302afe3…3e44` |
| C-012-RL1-HR1-D3 | 回放coverage与abstention | `FORMAL_EXECUTED_COVERAGE_INSUFFICIENT_PERSISTED` | `801230.SI` fold-1 `0/126`、fold-2 `37/126`；经济验收先失败，但coverage缺口仍在fold receipt中显式保留 |
| C-012-RL1-HR1-D4 | capability、basis与forward状态 | `ROTATION_L1_NOT_AVAILABLE` | 未形成CAPABILITY_AVAILABLE；forward、daily prediction与historical product纵切均未开启；其余三能力仍NOT_AVAILABLE |
| C-012-RL1-HR1-D5 | replay acceptance/component v2/bundle v2 | `FAILURE_RECEIPT_ONLY_NO_MODEL_OR_BUNDLE` | child failure `60b56d6e…590c`；失败不写component/bundle/READY，DB/runtime flags为false |
| C-012-RL1-HR1-D6 | 非阻塞forward confirmation与停止 | `FORMAL_STOPPED_10_OF_24_NO_HOLDOUT` | 第二fresh process、final fit和holdout均未执行；旧request不得重跑或通过改阈值修复 |
| C-012-RL1-RW1-D1 | rolling模型identity与唯一变化 | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED` | Ridge固定252 open days；market expanding、alpha/feature/target/seed不变；禁止window grid |
| C-012-RL1-RW1-D2 | rolling train、purge与因果边界 | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED` | 每fold末尾固定252日train、最多120日warmup；preprocess与target边界显式，validation/market窗口不变 |
| C-012-RL1-RW1-D3 | historical pre-frozen sector eligibility | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED` | validation outcome前按首个canonical validation trading date的PIT/t-1输入冻结E_f；31 canonical与typed ineligible同时保留，forward仍为31分母 |
| C-012-RL1-RW1-D4 | 经济验收 | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_NO_THRESHOLD_CHANGE` | 4/5、median IC .02、spread .003、OOF NW t 1.645/lag9全部保持HR1值 |
| C-012-RL1-RW1-D5 | historical coverage与完整分母 | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED` | daily `max(28,ceil(.90×|E_f|))`、90% dates、per-eligible sector 80%；ineligible显式报告，不补neutral |
| C-012-RL1-RW1-D6 | 24-fit复现与停止 | `RESOLVED_USER_APPROVED_EXACT_CONTRACT_SOURCE_IMPLEMENTATION_AUTHORIZED_NO_FORMAL_FIT` | 两fresh process总24 fits，不增加grid；失败终止rotation_L1方向，不写model/READY或打开第二candidate |
| C-013-PIT-ID-D1 | 行业成员如何形成order-invariant、可回放的版本化唯一身份 | `RESOLVED_SOURCE_IMPLEMENTED_VERIFIED_SHARED_CORE` | PR #3795已实现taxonomy version、成员有效区间、source/receipt hash和typed unavailable；BUG-1193完成bounded writer/readback |
| C-013-PIT-ID-D2 | 股票行业分类PIT与申万行业指数成员PIT如何分离，计入/更新/公告/指数切换日期如何解释 | `RESOLVED_SOURCE_IMPLEMENTED_VERIFIED_SHARED_CORE` | classification/index双authority、独立known-from及7/30、8/2、12/13边界已进入candidate/schema；HMM adapter仍须按同一合同消费 |
| C-013-PIT-ID-D3 | 同start多identity与顺序成员变更如何解析 | `RESOLVED_SOURCE_IMPLEMENTED_VERIFIED_SHARED_CORE` | 严格顺序半开区间与同边界typed unavailable已由共享resolver实现并验证 |
| C-013-PIT-ID-D4 | 行业identity unavailable是否删除股票或阻断全局流程 | `RESOLVED_HMM_MAPPING_IMPLEMENTED_601D_PREFLIGHT_VERIFIED` | HMM已把typed unavailable映射至既有contributor/coverage，禁止删除证券或补neutral；601日完整分母闭合 |
| C-013-PIT-ID-D5 | HMM、sector data、QE/Qlib、Selection/Paper与Advisory如何迁移 | `PARTIAL_SHARED_CORE_COMPLETE_MODULE_MIGRATIONS_PENDING` | shared resolver已合入；HMM迁移纳入G2-A，其他业务消费者由各owner独立迁移且不阻断HMM能力闭环 |
| C-013-PIT-ID-D6 | 数据权威、resolver、消费者迁移与HMM回放的顺序 | `RESOLVED_G2_A_DATA_INPUT_VERIFIED_HR1_EXECUTED_RW1_EXACT_PENDING` | P1/P2A、P2B和601日预检已完成；HR1已执行并以NOT_AVAILABLE终止；当前只等待RW1精确合同批准 |
| C-013-G2A-DATA-A | historical/forward classification basis及31行taxonomy→published L1 projection | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_601D_PREFLIGHT_VERIFIED` | historical stable backcast显式non-as-known、forward as-published；projection/preflight hash闭合，0 fit/selection/model/READY |
| C-008-B3-D7-01 | B3 runtime dependency identity | `RESOLVED_USER_APPROVED_D7_01_A` | 未来实现声明 `hmmlearn==0.3.3`；本 docs-only PR 不安装依赖，未来 production dependency gate 独立 pending |
| C-008-B3-FORMAL-EXEC-01 | 已批准 B3 合同在当前冻结输入上是否形成两-family READY | `VERIFIED_FORMAL_EXECUTION_BLOCKED_NO_READY` | producer `e2c01bae…` 完成 5184/5184 fits；formal canonical `e7992f87…39f`。D5 只选出 `legacy_covfix:L1/seed=43`，该 level 又在 D6 因 `801980.SI` failed；其余三个 family/level 无 eligible candidate。两 family blocked，selection未读validation/future utility，selection后未refit，model/READY/DB/runtime write均为false |
| C-008-B3-FORMAL-BLOCKER-DIAG-01 | 是否按 formal rejection summaries 对全部 blocker pair 与 deterministic controls 执行两 fresh-process 定向根因诊断 | `VERIFIED_DIAGNOSTIC_COMPLETE_NO_SELECTION_NO_READY` | producer `ac3687c2…`；artifact canonical `10287e84…cffe8`；150 rejected+24 controls、348/348 fits、3-entry D6 no-refit replay闭合，两次payload hash bitwise相同；不选择seed、不改阈值/authority、不写model/READY/DB/runtime |
| C-008-B3-REMEDIATION-DESIGN | blocker diagnostic后 initialization/likelihood/covariance/train structure/D6 temporal evidence 的模型修订合同 | `PROPOSED_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY` | 诊断证明多阶段、多family/level根因；尚不足以批准具体模型、optimizer、prior、transition/dwell或threshold。五类合同必须分别给出精确公式、成本、false accept/reject与验收证据后由用户确认 |
| C-008-B3-REMEDIATION-DIAG-02 | 是否在模型修订前执行324-profile variance provenance与163-entry likelihood/covariance/structure no-fit重聚合 | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_DIAGNOSTIC_EXECUTED_NO_MODEL_DECISION` | producer `b2456424…fdec`；canonical `48157a42…bb58`；324/324 profiles、163 completed entries、11 initialization sources闭合；唯一zero-variance profile为autocycle L2 `801207.SI/sf_dispersion_5d_neg`且preprocess前后均为0；46个completed entry有train-structure failure、4个sector identity跨8 seed持续失败；6个常量相关向量组显式insufficient。未运行HMM、未访问validation、未执行selection/acceptance、未写model/READY/DB/runtime |
| C-008-B3-REMEDIATION-D1 | `801207.SI/sf_dispersion_5d_neg`真实sector-local常量维应保持fail-closed还是采用显式inactive-dimension model identity | `RESOLVED_USER_SELECTED_D1_B_P1_SOURCE_IMPLEMENTED_REFIT_01_INCONCLUSIVE` | 用户选择B、A不采用。P1实现full20 preprocess、固定active indices、identity20 control及受控report；REFIT-01在current-A5 train observation drift处0 fits fail closed，未形成机制结论 |
| C-008-B3-REMEDIATION-D1-A | 保持现有constant-dimension fail-closed | `NOT_SELECTED` | 保留为决策审计记录；不得与D1-B并行实施或作为运行时fallback |
| C-008-B3-REMEDIATION-D1-B | 显式20→19 inactive-dimension model identity | `RESOLVED_USER_SELECTED_P1_SOURCE_MERGED_REFIT_03_DIAGNOSTIC_COMPLETE_INCONCLUSIVE` | `b3_d1_inactive_dimension.py`已实现exact-zero authority与固定projection；REFIT-03证明19D treatment在16/16 fits通过，但mixed seed raw pattern不证明唯一因果机制。是否固化为level-local engineering robustness由`POST-REFIT03-A`另行决定，mixed-dimension artifact/parser/runtime仍未实施 |
| C-008-B3-REMEDIATION-D1-B-REFIT-01 | 历史frozen control payload与current-A5输入能否同时闭合 | `VERIFIED_ATTEMPTED_INCONCLUSIVE_TRAIN_CORE_DRIFT_ZERO_FITS` | BUG-962 final report：current-A5 `train_observation_sha256`与历史train core不同；0 attempts/0 fits，mechanism inconclusive，禁止忽略hash、回退v1或覆盖历史artifact |
| C-008-B3-REMEDIATION-D1-B-REFIT-02-A | 是否以同一current-A5 authority运行801207 19D treatment、801207 identity20 matched negative和801011 identity20 harness | `RESOLVED_USER_APPROVED_SOURCE_MERGED_EXECUTED_INCONCLUSIVE` | REFIT-02-A/B已完成双process受控执行并暴露matched covariance failure；后续REFIT-03以current authority冻结bundle重跑48/48 fits并形成exact mixed-seed evidence。D5/D6/validation/model/READY/DB/runtime仍未执行 |
| C-008-B3-D1-POST-REFIT03-A | REFIT-03 mixed seed结果后，D1-B能否作为level-local engineering robustness而不宣称唯一统计因果机制 | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_LOCAL_REVIEWED` | 已限定为当前冻结输入/参数/seeds/固定环境下的level-local engineering robustness；不外推GaussianHMM普遍机制，不改写历史`inconclusive`结论 |
| C-008-B3-REMEDIATION-D1-D5-COMPAT-01 | inactive dimension后D5 `LL/(N*d_i)`分母与跨sector score如何保持批准语义 | `RESOLVED_USER_APPROVED_A_SOURCE_IMPLEMENTED_LOCAL_REVIEWED_NOT_EXECUTED` | 仅801207使用`d_i=19`，其他130 entry为20，131/131 eligibility与min/median/mean lexicographic保持；mixed schema、projection receipt、parser/readback、READY writer与公式/identity/完整性直接测试已实现。不设置经验性可比阈值；未运行2096 fits、D5/D6或生成model/READY |

C-001-A/C-002-A/C-003-A 已于 2026-07-22 获用户明确批准并回填本文；它们不是运行时人工审批。
C-006-A 已于 2026-07-23 获用户明确批准并回填本文；它不新增运行时审批或第二套股票池。
C-007-A 已于 2026-07-23 获用户明确批准并回填本文；它是 offline artifact-preparation 的固定算法版本，
不是运行时人工确认或可调门禁。
C-008-D1/C-008-B1/C-008-B3-DESIGN 方向已于 2026-07-23 获用户明确批准；后续又批准
C-008-B3-STRUCTURAL-A、DIAG-02、D3-01-A、D3-02-B、固定环境 D5-02-B、D7-01-A 与只读 D4-02-DIAG-03；
2026-07-25 又批准 D3-03-A、D4-01-A、D4-02-A、D4-03-B、D5-01-B、D6-01-B、C-008-B3-D4-L2-AUDIT-01 结论与受控 L2 重训设计方案 A。
2026-08-09 用户进一步批准 D4-01-MAP-A 与 D4-03-PERSISTENT-A，分别取代历史 D4-01-A 与 D4-03-B 的 active authority；
seeds 42..49、D4-02-A、D5/D6、hard semantic authority、两 family 完整性与禁止 per-sector stitching 保持不变。
2026-08-10 用户批准 C-008-B3-D6-NA-A：完整182日calendar ledger、feature-NA transition-only、utility-NA evidence exclusion、
calendar-aware run/transition与无新增missing-ratio gate；后续又授权独立源码实施。当前源码已在feature worktree实现并通过直接审核，
但尚未合入或执行D6 zero-refit replay，不能报告D6 accepted、model/READY或runtime已生效。
上述设计批准本身不包含B3/L2 retrain源码实现、实际fit、seed selection、model/READY artifact或runtime/database写入；
随后用户另行授权完成Slice 0源码与正式5184-fit执行。该执行已按批准D5/D6 fail closed并保持model/READY、database与runtime零写入，
不能倒推历史设计批准曾包含执行授权。
DIAG-02/03/04 的 `formal_acceptance_thresholds_applied=false` 仍是硬边界，
不得把 diagnostic completion 改写为正式 candidate acceptance。
C-008-B3-REMEDIATION-D1-B设计方向已于2026-07-31获用户选择；P1、REFIT-02/03源码均已合入。REFIT-03真实执行已完成
48/48 fits并形成`mixed_seed_pattern/inconclusive`；它未触及D5正式兼容语义、selection、D6、model/READY或runtime/database动作。
后续仍必须按§4.3.1 I.9的evidence-first顺序单独报告，不得把diagnostic execution扩张为D1唯一因果结论或D5实现授权。
C-005 是用户明确要求的交付控制，适用于今后每个 PR。

## 18. Design Acceptance Index / 设计验收索引

- F-011 parent：`G2_A_HR1_FORMAL_NOT_AVAILABLE_RW1_EXACT_CONTRACT_SOURCE_IMPLEMENTED_VERIFIED_PENDING_PR_AND_FORMAL_24_FIT`；历史C-009、BUG-892 与
  C-010-FORMAL-A 已合入并完成 clean-main 601 日 formal preflight。两 fresh-process 共 5184 fits 和 D5/D6 已执行；formal canonical
  `e7992f87…39f` 为 blocked，未生成 model/READY。targeted blocker diagnostic 已按批准合同完成，canonical
  `10287e84…cffe8`；no-fit remediation diagnostic也已完成，canonical `48157a42…bb58`。两项都只完成根因证据闭合且未重跑
  完整 grid，G2-A/G2-B/G2-C真实产品闭环仍未开始。C-010-A5已合入；D1 REFIT-01又在0 fits暴露current-A5 train observation与历史payload真实漂移。
  REFIT-02-B已按v6 matched-fit合同完成48/48真实fit并通过双进程bitwise验证；matched 20D在16/16 attempts均于covariance stage失败，
  treatment/harness均16/16 `fit_completed`且descriptive covariance accepted，机制仍inconclusive。REFIT-03 covariance exact-evidence已完成48/48 fits并形成
  `mixed_seed_pattern`。其后最新P6完成2096/2096 fits并选择`autocycle_all_core:L2/seed=43`；BUG-1008零refit重放闭合131-entry
  posterior与carrier readback后，最新零refit重放为assignment 131/131 accepted、evidence 120/131 accepted与11/131 failed；D6-NA-A与BUG-1029均已合入并close-sync。TRAIN-STABILITY-DIAG-01和`TRANSITION-DWELL-B`均未形成完整候选seed。P2-3A jump v2已正式执行并以`NOT_AVAILABLE_FOR_PROMOTION`停止；formal canonical=`034fdf3c…12ec`、296/456 fits、无holdout/model/READY。P2-3B亦完成批准合同内的正式执行：formal canonical=`d3298654…fc45`、167/184 fits；market完成、L1开发fold完整，但selected alpha100的median Rank IC为负，故在L1 final/L2前正确停止。491个嵌套hash和指标重聚合闭合，canonical model、FULL_READY与COVERAGE_AVAILABLE仍为0。
- F-011-A 数据/PIT/observation：`C013_G2_A_DATA_INPUT_IMPLEMENTED_601D_PREFLIGHT_VERIFIED`；C-007-A 单位、7/20 维公式、
  hard semantic authority、120/30 行合同与既有 `0.90` coverage authority 均保留。已批准 policy 只把 full-universe train-frozen
  contributor ledger、price/moneyflow 双层 coverage、同源 moneyflow denominator 与逐 feature cross-section 形式化；它不删除证券或
  feature，不改变 validation/runtime prediction universe。A2/A3 已批准，四个 moneyflow/横截面派生 feature 的公式 identity 必须从
  v1 显式升级为 `hmm_risk_l1_sector_factor_formula_v2_c010`，不得被描述为“公式不变”。设计、源码、formal preflight 与 formal child input
  identity 已完成历史验证。BUG-944 曾证明 full-market provider audit domain 与 direct-sector opportunity domain 未正式分离；
  C-010-A5源码与此前601日只读formal preflight已合入；BUG-1184证明`sw_index_member is_new=Y`不能单独充当历史成员区间后，
  C-013 P1/P2A已由PR #3795合入classification/index双candidate与共享resolver，BUG-1193 PR #3805补齐bounded writer/readback并完成runtime verify。`C-013-G2A-DATA-A`已实现historical stable backcast/forward as-published双边界、31行code projection和601日完整分母预检；当前数据输入缺口已闭合。HR1已正式执行并以NOT_AVAILABLE终止；RW1 D1～D6已精确批准，当前实施源码与测试。
- F-011-B numeric/sector semantic：`P2_4_FORMAL_EXECUTED_NUMERIC_VALID_PRODUCT_NOT_AVAILABLE`；历史P6/transition evidence不变。P2-3C保持批准的market-sign交互、target、score方向和state projection；正式holdout局部指标只作C-012能力分解输入，不追认candidate成功。
- F-011-C product/family/selection：`HR1_FORMAL_NOT_AVAILABLE_RW1_EXACT_SOURCE_IMPLEMENTED_VERIFIED_PENDING_PR_AND_FORMAL_24_FIT`；旧candidate、参数和holdout已终结，禁止reselection、阈值调整或复用holdout。RW1只允许一个252日fixed rolling候选；正式24-fit另行授权。
- F-011-D readiness/coverage：`FULL_READY_ZERO_CAPABILITY_AVAILABLE_ZERO_HR1_NOT_AVAILABLE`；历史READY artifact数为0，HR1只生成failure receipt。RW1 historical eligibility必须pre-frozen并与canonical 31并列展示；禁止删sector、补neutral或用源码事实推导产品成功。
- F-011-E state generator：`G2_A_CONDITIONAL_IMPLEMENTATION_PENDING`；rotation_L1正式通过后必须在同一G2-A纵切实施真实单日生成，能力失败则不生成伪结果。共同水位、job、revision/dedupe与late-data属于G2-C，不再作为F-011模型验收前置。
- F-012：advisory-only 写入与依赖隔离，不产生 Selection/Paper/QMT/QE/交易副作用。
- F-013：G2-A先完成真实单日rotation_L1 read API/UI与失败状态；G2-B再完成多日历史、风险预警、详情与retrospective report。

## 19. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | `backend/services/hmm_risk/{industry_pit_adapter,market_relative_ridge_candidate,market_relative_ridge_holdout,market_relative_jump_spike}.py`；父蓝图v2.36；§4.3.4.4～§4.3.4.9；§23.35～§23.40 | `artifact:F:/Dev/AIstock_artifacts/hmm_phase2_g2a_hr1_24fit_postbug1257_20260830_5c1a90a7/replay_acceptance.failure.json` parent `d302afe3…3e44`、child failure `60b56d6e…590c`；direct RW1/legacy-reader `63 passed`；`hmm_risk_backend=643 passed,coverage=76.93%`；F2 `PASS` | APPROVED_BY_USER_RW1_EXACT_CONTRACT_SOURCE_IMPLEMENTED_VERIFIED_PENDING_PR_AND_FORMAL_24_FIT | RW1 D1～D6及252日已批准并完成源码/测试/三轮审核；正式24-fit、model/bundle/API/UI仍为0；不能重跑HR1或在bundle处宣称完成 |
| F-011-A data/PIT/observation | `backend/services/industry_pit/**`; `backend/services/hmm_risk/{industry_pit_adapter,security_identity,provider_absence,observation_eligibility,stock_fact_repository,stock_fact_observation}.py`; C-007-A/C-009/C-010/C-013 contracts | `backend/tests/hmm_risk/test_industry_pit_adapter.py`；601日preflight canonical `e5f204d4…6059`；BUG-1193 record | APPROVED_BY_USER_G2_A_DATA_INPUT_IMPLEMENTED_601D_PREFLIGHT_VERIFIED | historical/forward basis、31行projection、typed unavailable与完整分母已闭合；24-fit/能力/产品仍未执行 |
| F-011-B fit/convergence/covariance/occupancy | `backend/services/hmm_risk/{b3_training,b3_acceptance,b3_transition_dwell,b3_remediation_diagnostic,b3_d1_inactive_dimension,state_model_set}.py`; `scripts/hmm_risk/prepare_state_model_set.py` | `backend/tests/hmm_risk/{test_b3_transition_dwell,test_prepare_state_model_set_b3}.py`；historical formal/DIAG receipts；P6 `2096/2096` fits；TRAIN-STABILITY无完整seed；TRANSITION-DWELL-B双fresh-process `2096/2096` fits bitwise一致、完整对象canonical `e5f355fc…d4b54` | APPROVED_BY_USER_TRANSITION_DWELL_EXECUTED_NO_COMPLETE_CANDIDATE_BLOCKED | 原P6曾形成D5候选，但后续两个train-only结构实验均未形成完整候选seed；不得外推到其他level/family，不得自动改tau/self-center/阈值/seed/grid或执行selection、D6、model/READY |
| F-011-C semantic/selection | `backend/services/hmm_risk/{b3_acceptance,b3_training}.py`; `scripts/hmm_risk/prepare_state_model_set.py` | `backend/tests/hmm_risk/{test_b3_acceptance,test_b3_training,test_prepare_state_model_set_b3}.py`；D5 selected seed43；BUG-1029 zero-refit assignment 131/131、evidence 120/131 accepted | APPROVED_BY_USER_D5_SELECTED_D6_120_OF_131_ACCEPTED_11_FAILED | selection train-only且无refit/reselection；11个失败保持typed evidence，D6-NA-A不改变hard authority，B2不采用 |
| C-008-B3-D6-NA-A calendar/availability amendment | `backend/services/hmm_risk/{stock_fact_observation,state_model_set,b3_acceptance,b3_training,b3_mixed_dimension}.py`; `scripts/hmm_risk/prepare_state_model_set.py` | `backend/tests/hmm_risk/{test_state_model_set,test_stock_fact_observation,test_b3_acceptance,test_b3_training,test_prepare_state_model_set_b3}.py`; PR #3258；BUG-1029/PR #3311 zero-refit report | APPROVED_BY_USER_SOURCE_MERGED_ZERO_REFIT_EXECUTED | carrier/manifest v2、T/O/U/E、composite selected schema、shared writer/readback与zero-refit lineage已实现并执行；11个真实D6 failure不被补值、删日或降级，不写model/READY |
| F-011-D historical B3 two-family READY | `backend/services/hmm_risk/b3_training.py::write_b3_ready_model_set` | `artifact:F:/Dev/AIstock_artifacts/hmm_risk/b3_formal_20260729_e2c01bae_bug912/b3_formal_preparation.json` top-level blocked/no-write receipt；`backend/tests/hmm_risk/test_b3_training.py` | APPROVED_BY_USER_SOURCE_IMPLEMENTED_BLOCKED_FORMAL_ACCEPTANCE | 历史B3 READY artifact数为0且四个family/level完整性未成立；该事实不变。父蓝图v2.25后的C-011 P2-4为独立唯一canonical product authority，不以旧B3 two-family合取作为第二套active gate |
| F-011-E state generator | `backend/services/hmm_risk/state_generator.py` | `backend/tests/hmm_risk/test_state_generator.py` | APPROVED_BY_USER_G2_A_CONDITIONAL_IMPLEMENTATION_PENDING | 仅在rotation_L1达到CAPABILITY_AVAILABLE后于同一G2-A纵切实施真实单日生成；若能力NOT_AVAILABLE则不生成伪预测。共同水位、job、revision/dedupe与late-data仍移至G2-C |
| C-011 product-aligned modeling and acceptance | 父蓝图v2.30；本设计§4.3.4.2～§4.3.4.5；`market_relative_ridge_{candidate,holdout}.py` | `artifact:F:/Dev/AIstock_artifacts/hmm_phase2_gate2_p2_4_20260823_15e041f_postbug1153/p2_4_holdout_acceptance.json` canonical `16004b24…7c87`；直接测试与双fresh-process正式结果 | APPROVED_BY_USER_VERIFIED_P2_4_NOT_AVAILABLE_MODEL_WRITE_FALSE | C-011精确candidate合同已终结，不得重跑或调参；不是canonical product bundle |
| C-012 capability-aligned product bundle | 父蓝图v2.36；本设计§4.3.4.6～§4.3.4.9；§23.35～§23.40 | `artifact:F:/Dev/AIstock_artifacts/hmm_phase2_g2a_hr1_24fit_postbug1257_20260830_5c1a90a7/replay_acceptance.failure.json`；RW1 direct `63 passed`、module `643 passed`；601日input preflight canonical `e5f204d4…6059` | APPROVED_BY_USER_RW1_EXACT_CONTRACT_SOURCE_IMPLEMENTED_VERIFIED_PENDING_PR_AND_FORMAL_24_FIT | HR1在10/24 fits后按合同停止，未写bundle；RW1 exact源码与测试已验证，正式24-fit与真实产品仍为0 |
| C-013 versioned industry PIT identity | §23.35 D1～D6与DATA-A；`backend/services/industry_pit/**`; `backend/services/hmm_risk/industry_pit_adapter.py` | shared resolver tests；`backend/tests/hmm_risk/test_industry_pit_adapter.py`；601日preflight canonical `e5f204d4…6059` | APPROVED_BY_USER_G2_A_DATA_INPUT_IMPLEMENTED_601D_PREFLIGHT_VERIFIED | P1/P2A/P2B与601日预检完成；其他consumer迁移由对应owner处理；24-fit另行授权 |
| C-011-P2-3A jump spike | §4.3.4.2 D1～D6；`backend/services/hmm_risk/market_relative_jump_spike.py` | `backend/tests/hmm_risk/test_market_relative_jump_spike.py`；`artifact:F:/Dev/AIstock_artifacts/hmm_phase2_gate2_p2_3_v2_20260816/p2_3_jump_spike_report_c1c6c313.failure.json` | VERIFIED_NOT_AVAILABLE_FOR_PROMOTION | 无 |
| C-011-P2-3B direct predictor exact contract | §4.3.4.3 D1～D6；`backend/services/hmm_risk/market_relative_ridge_candidate.py`；薄CLI | `artifact:F:/Dev/AIstock_artifacts/hmm_phase2_gate2_p2_3b_20260817/p2_3b_ridge_candidate_report_24e4ae79_formal.failure.json`；`backend/tests/hmm_risk/test_market_relative_ridge_candidate.py`；167/184 fits；491个嵌套hash闭合 | APPROVED_BY_USER_VERIFIED_NOT_AVAILABLE_FOR_PROMOTION | D1～D6已正式执行；L1 selected alpha100的median Rank IC非正，按批准停止条件fail closed。未执行L1 final、L2、P2-4、model/READY |
| C-011-P2-3C market-conditioned Ridge exact contract | §4.3.4.4 D1～D6；父蓝图v2.29 §11.6/§11.7 | candidate `artifact:F:/Dev/AIstock_artifacts/hmm_phase2_gate2_p2_3c_20260817/p2_3c_market_conditioned_ridge_candidate_8ca1b98d.json` canonical `792d4f6a…17e3`；`backend/tests/hmm_risk/test_market_relative_ridge_candidate.py` | APPROVED_BY_USER_SOURCE_MERGED_FORMAL_36_OF_36_CANDIDATE_FROZEN | market lambda4/seed42、L1/L2 alpha100、interaction与selection闭合；holdout/model/READY/DB/runtime均false |
| C-011-P2-4 untouched holdout exact contract | §4.3.4.5 D1～D6；父蓝图v2.30 §11.7/§11.8；`market_relative_ridge_holdout.py`、薄CLI与直接测试 | `artifact:F:/Dev/AIstock_artifacts/hmm_phase2_gate2_p2_4_20260823_15e041f_postbug1153/p2_4_holdout_acceptance.json` canonical `16004b24…7c87`；两个fresh-process payload bitwise一致；`fit_count=0` | APPROVED_BY_USER_VERIFIED_FORMAL_NOT_AVAILABLE_TERMINAL | L1 directional局部通过不补足risk；L2产品合同未闭合；model/READY均未写，已消费holdout不可复用 |
| F-012 | `backend/services/hmm_risk/**`; DB role/write-scope guard; `backend/routers/hmm_risk.py` | `backend/tests/hmm_risk/test_isolation.py` | DESIGN_READY_USER_APPROVED | 无 |
| F-013 | `backend/routers/hmm_risk.py`; `backend/services/hmm_risk/report_service.py`; `frontend/src/app/hmm-risk/**`; `frontend/src/components/hmm-risk/**`; `frontend/src/lib/hmm-risk/api.ts` | `backend/tests/hmm_risk/test_api.py`; `backend/tests/hmm_risk/test_retrospective_report.py`; `playwright test frontend/tests/hmm-risk/hmm-risk.spec.ts` | APPROVED_BY_USER_G2_A_L1_PRODUCT_AND_G2_B_EXPANSION_PENDING | G2-A能力通过后必须实现真实单日L1 read API/heatmap/浏览器纵切；G2-B扩展多日历史、预警、详情和后续能力。未通过能力、完整分母和abstention始终可见；`/hmm`默认切换仍等待完整验收 |

`DESIGN_READY_USER_APPROVED` 表示实现级业务合同已获批准并可进入对应 implementation；不表示源码、DDL、UI、runtime、
生产任务或后续实现 PR 合入已完成。

## 20. Risks / Failure Modes

| 风险 | 控制 |
|---|---|
| 旧模型结构被误判 | 显式 parser contract + content SHA；不按数量/路径猜测 |
| candidate 与 model 漂移 | manifest/snapshot/config/model hash 全冻结；任一不一致 fail loud |
| future leakage | train_end <= as_of；causal filter；所有 dataset watermark 固化 |
| sector duplicate 行不一致 | 全字段 equality 检查；不使用 DISTINCT ON 静默挑选 |
| L1/L2 来源被猜测 | C-002-A 要求同一 state-model-set 中独立 direct L1/L2 model；禁止 posterior aggregation |
| L1 observation 用旧 4 维子集或 L2 feature 平均冒充 | C-007-A 固定 stock-fact-first 7/20 维逐字段重算、PIT canonical mapping、单位和 coverage；区分性测试证明旧路径无法通过 |
| seed sensitivity 或 validation-driven seed picking | D3-01-A固定预声明schedule全量运行；D5-01-B固定每family分别选择L1/L2 level-global identity，使用完整31/131 train-only min/median/mean lex receipt。selection不接收validation/future utility/D6，禁止per-sector拼接或semantic失败后换seed |
| raw likelihood monitor 在 covariance prior 下提前停止 | D4-01-MAP-A 使用与 D3-03-A prior 完全一致的 MAP objective；同参数状态计算 J 与 D4-02-A；仅联合通过停止，禁止退回 `GaussianHMM.fit()` raw monitor |
| MAP 数值包络掩盖真实下降 | `ΔJ<-T` fail closed，包络内负值保留 warning；完整 J/L/prior/delta/envelope 与 hash 持久化，不得自动扩大 envelope |
| raw likelihood decrease 被静默删除或误作 authority | raw L 全历史只作诊断并保留 typed warning；non-finite 仍失败；D5 只取 joint-stop raw L，不取 J 或 history maximum |
| transition修订只提高self-transition并加剧吸收态 | TRANSITION-DWELL-B不修改fit后参数；prior中心从train-only KMeans transition派生，self center双向clip而非单向floor，fitted hard structure继续由不变D4-03 fail closed |
| transition prior改变训练目标但MAP receipt遗漏该项 | 候选`J_t^B`显式加入`sum((prior-1)*log(a_ij))`；raw transmat strict-positive/row-sum与objective component分别回读，不允许沿用旧MAP hash冒充新合同 |
| no-selection实验被当作正式模型改进 | 第一次2096-fit结果只产生compact diagnostic；D5/D6/model/READY flags固定false，任何正式启用、selection与validation均需后续独立决定 |
| 既有 L2 final parameters 被误当成 D4-01/D4-02 numeric receipt | AUDIT-01 已固定旧 L2 likelihood insufficient、covariance failed；禁止 grandfather、复制 L1 evidence、静默跳过或未经授权执行重训，新的两-family 131/131 未闭合前不得 READY |
| covariance clip 掩盖系统性 anomaly | D3-03-A/D4-02-A 禁止 initialization/post-fit clip 与 projection；正式 posterior 只使用通过 raw validity、0.5% dynamic-bound 闭区间、zero anomaly budget 和 2% M-step residual 的 raw covariance；全部 mask/hash 留存 |
| matched 20D covariance failure被压缩成generic stage后误判inactive机制 | REFIT-02-B保持inconclusive；REFIT-03提案要求在任何validator/exception转换前保存raw shape、IEEE bit pattern、cell坐标、inactive mask与三角色pair。诊断pattern不得推导D4/D5、删feature或放宽阈值；证据不完整继续fail closed |
| persistent regime 被旧 max-run-share gate 错拒或被放宽成 singleton fallback | train 侧按 `hmm_risk_c008_b3_d4_03_persistent_a_v1` 先执行不变 common gate，再走互斥 recurrent/persistent 路径；persistent 必须满足10%/30 count/6 months/2 runs/transitions，singleton或单 run不能通过；selected validation仍按D6-01-B独立验收 |
| 未经确认拆分 validation 或增加 holdout | 保持批准的 `2024-07-01..2025-03-31` 单一 validation 与 fitted `startprob_` prior；任何 split/holdout 先明确业务语义并获确认 |
| 删除NA日或压缩日期后把真实gap伪装成相邻状态 | D6-NA-A固定完整182日calendar ledger；run/transition仅在相邻calendar position且两日均为evidence时连续/计数，任何非evidence日都打断结构证据 |
| feature NA 被插补或只用剩余维度生成 emission | 任一approved feature unavailable则整日observation unavailable，只执行transition-only；禁止零值/均值/前值/partial-dimension/synthetic vector，逐日typed reason与mask/hash必须可回读 |
| utility NA 被默认值掩盖或反向阻断 causal posterior | posterior只由observation决定并保持全calendar causal propagation；utility unavailable仅排除该日semantic evidence，禁止部分horizon重算、默认utility或soft authority补足 |
| “少量缺失”被转化为新的经验门禁或自动成功 | 不新增minimum availability ratio/maximum missing ratio/人工确认；只在evidence set上执行已批准D6-01-B与既有30行source contract，证据不足继续fail closed |
| 库默认值或浮点环境导致不可复现 | D3-03-A 固定 KMeans/HMM 全参数与 sector-local prior；D5-02-B 固定依赖、BLAS/线程和 canonical serialization；不得仅凭 seed 或跨 host 外推 deterministic hash |
| 诊断数值被写成正式 gate | B1/DIAG-02/03/04及MAP/persistent diagnostics的`formal_acceptance_thresholds_applied=false`、`selection_performed=false`是硬边界；D3-03-A/D4-01-MAP-A/D4-02-A/D4-03-PERSISTENT-A/D5-01-B/D6-01-B才是active合同，不得把historical score排序改写为selection或acceptance |
| formal blocker 诊断被 cherry-pick 成单 sector 结论 | DIAG-01 target set 从 formal rejection summaries 机械派生全部150个 rejected seed/sector pair，并为每个 affected family/level/seed选24个train-only canonical control；数量/hash不闭合时fit前失败 |
| signed-distance evidence 被反向用于放宽阈值或 validation 选 seed | DIAG-01 只计算批准边界距离且固定 `acceptance_decision_reexecuted=false`；D4 refit看不到validation/future utility，D6只replay既有selected seed且禁止reselection；任何模型/阈值变更另立精确decision并取得用户确认 |
| 变长history/run tree的路径差异被误报为全部证据缺失 | 区分comparison的`missing_evidence_entry_count`与entry直接`missing_evidence`；前者只说明rejected/control numeric leaf路径不对称，只有后者可声明stage evidence不可用，禁止用null/default/control补齐 |
| persistent zero variance 被当成普通seed failure | DIAG-01已固定`autocycle_all_core:L2/801207.SI/sf_dispersion_5d_neg`跨8个seed为exact-zero reference variance；DIAG-02先查公式/source/preprocess provenance，禁止扩大seed、静默加floor或删除feature |
| 同时修改initialization、EM、covariance和transition导致因果不可归因 | DIAG-02先做no-fit full-profile与fixed-point association重聚合，且明确association不构成因果；后续每次只提交最小model mechanism候选，保留current control和完整raw receipt，不以多改动后的局部成功宣称根因闭合 |
| 批量replay D6形成validation-driven model选择 | DIAG-02禁止新增validation访问；现有3-entry只解释已冻结selected identity。未来仅在train-only D5冻结新identity后执行唯一D6，失败不得reselection |
| autocycle-only 冒充两-family 完成 | F-011-D 要求所有已批准 family 完整；legacy 缺失时保持 blocked |
| 历史 mapping 的 industry/index code 双表示被随机选行 | classify 唯一规范化；等价 source rows 全量留 hash，非等价多映射 fail loud；禁止 `DISTINCT ON` |
| 当前 canonical 股票代码回填历史事实导致 source join 缺失 | C-009-B 使用 source-dataset-specific、effective-dated stable identity manifest；保存 canonical/source code 与 authority hash；零/多/重叠/冲突 fail closed，禁止名称猜测、单股 hard-code 或 raw row 复制 |
| 停牌日没有 exact daily-basic 被误判为数据丢失 | C-009-A 只允许 `<t` 且 `<=prev_market_trade_date(t)` 的最新 authoritative circ-mv；保存 source date/staleness；无 causal row fail closed，不伪造停牌日 provider row |
| Qlib/股票层 NA 被直接送入 GaussianHMM 或被填零 | C-009-C 在 stock-fact 层保存 provider_absence NA 与完整 missing evidence；只对 finite complete rows 聚合并继续执行既有 0.90 count/weight coverage；最终 HMM matrix 拒绝 NaN/non-finite，禁止 0/前值/均值/neutral |
| absence-only 清单冒充 full-universe contributor policy | C-010-FORMAL-A 要求每个具有 expected opportunity 的 canonical symbol 都进入 ledger；无 absence 显式记录 availability=1.0，零 opportunity、重复/越界日期和 ledger/hash 不完整均 fail closed |
| full-market provider absence 被误要求必须具有 direct-sector opportunity | C-010-A5 候选合同把 `P_all` 按 PIT/price/SW L1/L2 predicate 完备分成 `P_in/P_out`；availability 只使用 `P_in ⊆ O_sector`。域外 key 必须保留 typed evidence/hash，禁止补造 SW identity、删除 absence、把域外计为 available或掩盖独立 price failure |
| train contributor exclusion 在 validation/runtime 临时重算造成 train-serving drift | eligibility 只由冻结 train authority 计算一次并进入 policy hash；validation、causal replay 与 runtime feature construction 原样应用同一 ledger，证券仍保留在全部业务 universe 与 price domain |
| 一个 sector/feature 缺失放大为全市场全部 feature 缺失 | 每个 feature 按自身 price/moneyflow domain 建 ordered valid-sector set，并以既有 0.90 authority 独立判定；不足只使该 feature/date typed invalid，禁止跨 domain 借 completeness、填值或全局吞错 |
| partial day 冒充完整 | run terminal `partial_failed`；UI degraded 并列 missing sectors |
| late data 覆盖历史 | append-only revision + supersedes + forward cascade |
| 并发生成重复 revision | advisory lock + unique keys + input-hash compare |
| 旧 gate 业务语义漂移 | Decision C-004 冻结 producer/consumer；未来迁移另立设计与逐 PR 确认 |
| UI 用旧日/neutral 填空 | stale/failed/empty 分离；missing cell 明示 |
| report 变成研究门禁 | 指标只解释，无 pass/fail 或 lifecycle 副作用 |
| scheduler 意外启用 | v1 worker 只有 once/drain；无 startup/scheduler 注册 |
| worker 崩溃永久 running | GET effective stale + 显式 reaper + pre-claim reaper + max runtime |
| persistence 回滚后伪称有失败回执 | Transaction C rollback 后以独立 Transaction D CAS；二次失败 fatal/nonzero |

## 21. Rollout / Rollback

### 21.1 Rollout

1. C-001-A/C-002-A/C-003-A 已获批准；本设计 PR #2616 已获本次用户明确合入授权；C-006-A/C-007-A 后续修订仍按 C-005 逐 PR 等待用户合入确认。
2. BUG-832 退休 `sector_data` 持久化 identity 与 repair DML；生产 `sector_data` 已符合 fact-only 目标，无需 identity DDL/DML。
3. Slice 0 在现有 DEV DB 验证 L1/L2 artifact preparation、schema bootstrap、exact verify 和 rollback；production DDL 保持 pending。
4. G2-A在DEV使用真实只读market input与经授权的24-fit闭合能力后，完成真实单历史交易日prediction、最小repository/read API和安全端口L1热力图验收；只写DEV `hmm_risk.*`，不得用fixture或mock替代最终业务验收。
5. G2-B扩展多日历史、预警、指标、详情与后续已验收能力；G2-C才加入人工job/bounded worker、revision/late-data并在完整验收后切换`/hmm`。
6. 每个后续源码 PR 均在 merge 前停止等待用户确认；源码合入后，production DDL 仍须独立目标授权、migration 和 readback。
7. production 首次 manual worker/API activation 再独立授权；不自动启动 scheduler。
8. BUG-877/C-009 若获批准，先以 docs-only revision 固化 A/B/C/D，再在扩展后的 BUG scope 中提交 HMM-local
   source-identity manifest/resolver、circ-mv as-of 与 provider-absence NA evidence；合入前只运行 frozen source 只读 preflight，
   不执行 DML。新的 dataset/mapping/source-identity hash 必须使用新 content identity，不能覆盖 `c07177…/9cdddd…/d4a5cc…`。
9. C-010-A1/A2/A3/A4 源码、601 日 formal preflight 与正式 5184 fits 均已执行；input/policy gate 已闭合，模型验收结果为
   blocked。该结果不回滚 C-010 policy，也不授权删除证券/feature、改变公式或重跑完整 grid。
10. `C-008-B3-FORMAL-BLOCKER-DIAG-01` 已按用户确认完成精确348-fit targeted runner与3-entry D6 replay。
    下一步只基于 canonical evidence 分别形成 initialization/likelihood/covariance/train structure/D6 temporal remediation精确设计；
    设计确认、implementation、PR merge、正式grid与production/runtime action继续分别报告，不增加运行时人工审批。

### 21.2 Rollback

- DDL transaction 失败自动回滚；成功后不 DROP 历史表，使用 forward-fix。
- runtime rollback 停止 manual worker/禁用 route activation，不删除 state/alert/event/report/history。
- UI rollback 恢复 `/hmm-evolution` 默认入口，不伪造风险页成功。
- generator/rule 新版本以新 identity 运行；旧 revision/report 保留，不原地重写。
- legacy v1 producer/consumer 完全冻结；Phase 2 rollback 不触碰其文件、artifact 或 runtime 接线。
- C-009 不写 raw source 或 schema，因此 rollback 仅回退未激活的 reader/manifest source revision，并把对应 formal request 标记
  superseded/blocked；已生成的只读 preflight、missing evidence、identity manifest 和 hash 保持 append-only，不删除、不改写，旧
  exact-calendar reader 只能恢复为 fail-closed diagnosis，不能恢复 5184-fit execution。
- C-010-FORMAL-A 不修改 raw source、业务 universe 或 schema。未来未激活实现的 rollback 只撤销 formal policy source revision并使其
  request candidate `superseded/blocked`；full-universe ledger、coverage/cross-section receipts 与 diagnostic artifacts append-only 保留。
  已绑定该 policy hash 的 candidate 不得切换回 diagnostic-only、absence-only 或 exact-complete fallback 后继续训练。

## 22. Production Gates

- 本设计 PR：`production_ddl_gate=noop`。
- 本设计 PR：`production_frontend_dependency_gate=noop`。
- 本设计 PR：`production_backend_dependency_gate=noop`。
- 本设计 PR：`production_runtime_activation_gate=noop`。
- BUG-877/C-009 当前：`production_dml_gate=noop_no_authoritative_candidate`；identity/as-of/NA 均在读取与 manifest 层完成，
  `production_ddl_gate=noop`、frontend/backend dependency gate=`noop`、runtime activation=`noop`。
- C-010-FORMAL-A 已完成 formal preflight/grid；本次 docs revision 不改变其 source、依赖或 runtime，DDL/DML、frontend/backend
  dependency 与 runtime activation 均为 `noop`。
- C-008-B3-FORMAL-BLOCKER-DIAG-01 已在先前独立执行中完成348 fits与D6 replay；本次仅回填设计证据：
  `production_ddl_gate=noop`、`production_dml_gate=noop`、`production_frontend_dependency_gate=noop`、
  `production_backend_dependency_gate=noop`、`runtime_activation=noop`，不重新执行诊断。
- REFIT-03 covariance diagnostic本次仅为F2设计修订与审核，未修改依赖、源码、数据库或runtime：
  `production_ddl_gate=noop`、`production_dml_gate=noop`、`production_frontend_dependency_gate=noop`、
  `production_backend_dependency_gate=noop`、`runtime_activation=noop`；未来源码实现与真实48-fit执行仍需分别授权。
- `sector_data` identity DDL/DML：`noop`，生产表保持 fact-only，行业 mapping 动态解析。
- 未来 schema implementation：DEV `applied_and_verified` 后，production DDL 仍为 `pending`，需要目标明确授权。
- C-008-B3 controlled execution dependency：仓库已声明 `hmmlearn==0.3.3`；用户已授权并在 Conda `AIstock` 环境完成
  no-deps 安装与 import/version smoke。该环境固定为 CPython `3.12.12`、NumPy `2.4.0`（按用户要求禁止降级或改写）、
  SciPy `1.16.3`、scikit-learn `1.8.0`、hmmlearn `0.3.3`、threadpoolctl `3.6.0`，正式执行时五个线程环境与实际
  threadpool count 必须为 1。历史 DIAG 的 CPython `3.13.5`/NumPy `2.3.3` 只保留为 historical evidence，不冒充本次
  formal environment。依赖安装不推导 grid、runtime activation 或服务重启已获授权。
- 未来源码合入不等于 API/UI/worker 激活；首次 production manual worker run 单独授权。
- Phase 2 scheduler：未批准、未实现、未启用。

## 23. DESIGN-COMPLIANCE-001 预审

- no_simplified_delivery：五张持久表/current views、全 candidate evidence matrix、direct L1/L2、唯一 generator、job/revision、API、真实 UI 与 confirmed report 均为完成边界；AUDIT-01 已固定旧 L2 likelihood insufficient/covariance failed，受控重训必须同时覆盖两个family的131/131；D5-01-B必须保留四个selected family/level identities及全部未选candidate，D6-01-B必须逐selected L1/L2 entry执行，不以DIAG sensitivity、final parameters、补metadata、L1 evidence、旧L2 fallback、单family、子集、默认或静态页代替。
  D6-NA-A必须保留完整182日ledger与全部selected entry；不得删除缺失日/sector、压缩日期、仅验收可用子集或把transition-only posterior冒充semantic evidence。
  BUG-877 不以“只修 302132”、只处理停牌、只保留可用股票或单一 dataset 冒充完成；C-009 必须同时覆盖 causal circ-mv、
  通用 source-specific identity resolver、provider-absence NA、四个 family/level coverage 与 frozen preflight。C-010-FORMAL-A 不以 5 个
  absence symbols、单一 `689009.SH`、单一 L1/L2 或 diagnostic mask candidate 冒充正式政策；必须交付 full-universe ledger、两层
  coverage、逐 feature cross-section、四个 family/level preflight 与完整 policy binding，且不删除任何批准 feature。
- no_silent_error：candidate/model/watermark/mapping/sector/L1/persistence/renderer 全部有 reason code；partial 不标 success；
  C-008-B3 将 initialization/fit/monitor/likelihood/covariance/occupancy/selection/semantic validation/family 状态分别持久化，
  D4-01-MAP-A 必须保留完整 MAP/raw histories、prior components、numeric envelope、D4-02 joint-stop evidence 与 typed reasons；
   raw negative likelihood 不得静默删除或反向成为停止 authority；D4-03-PERSISTENT-A 必须逐 state 持久化 common gate、互斥路径、
   path identity 与全部 comparison；D5-01-B把schedule/eligibility/score/repeat/pool/selected与未选reason分别留证，missing/non-finite/hash mismatch
   不得退回固定seed或任意candidate；D6-01-B 把 validation evidence missing/date/posterior/count/occupancy/month/run/transition/run concentration/
   utility variance/gap 分别持久化，失败后不得换 seed；任一失败不得压缩或静默推导 READY。
   D6-NA-A进一步把calendar ledger、observation/utility availability、transition-only mode、evidence inclusion及其hash分别持久化；
   禁止用插补、partial feature、默认utility、跨gap transition或writer/readback不一致伪造成功。
  C-009 的 unresolved/ambiguous identity、provider absence、circ-mv source missing、coverage insufficient 与 final non-finite 分别
  持久化；不得把 missing row、空 alias、被排除股票或 plan-only completion 写成 success。C-010-FORMAL-A 将 opportunity、ledger、
  price/moneyflow coverage、cross-section、feature identity 与 policy hash failure 分别持久化；不得把 excluded contributor 删除出
  universe、把 structurally unavailable 写成 available，或用另一 domain 的 completeness 覆盖 failure。
- no_business_semantic_drift：预警 severity 保持父设计；C-001-A capability、C-002-A direct model set、C-003-A oracle、
  C-006-A fact/universe/mapping 分层与 C-007-A stock-fact-first observation 均有用户明确批准；C-008-B3 保持 hard semantic
   authority、原单一 validation 和 fitted `startprob_` prior，B2 明确不采用；L2 provenance audit 未改 model/mapping，
  受控重训设计A固定frozen input、两个family、direct L2和新content identity；D5-01-B的level-global粒度保持direct L1/L2
  独立，不按sector拼接，也不在family之间淘汰方向；不迁移或覆盖旧candidate/snapshot；
  删除未经确认的 calibration/holdout split 与阈值。C-009 只改变事实解析与缺失表达，不改变两个 family、7/20 维公式、
  hard semantic authority、train/validation 窗口、D3-D6、0.90 count/weight coverage 或 120/30 row contract；identity mapping 不进入特征。
  C-010-FORMAL-A 保留证券、sector、feature set、PIT、回测、选股、实盘和 prediction universe；A2/A3 的 moneyflow
  denominator 与 feature-local cross-section 业务公式变化已获用户批准，因此必须使用新 formula version，不能静默继承 v1
   identity。train-derived ledger 在 validation/replay/runtime 原样应用，禁止 validation-driven 重算。
   D6-NA-A只改变已批准validation中NA的因果表达：feature-NA日`posterior=prior`、utility-NA日不进入hard evidence；hard argmax、
   0.35/0.35/0.30 utility、D5 selected identity、窗口、两个family与READY条件均不改变。
- no_unrequested_gate_or_approval：D4-01-MAP-A、D4-02-A、D4-03-PERSISTENT-A、D5-01-B与D6-01-B是用户明确批准的确定性模型合同，不是运行时人工审批；未获确认的
  split/holdout不进入active contract；未来确认的
  确定性模型合同不是运行时人工审批。preview 不是批准步骤，普通 read 无确认；只保留规范要求的 production DDL/dependency/
  runtime 独立授权和用户要求的逐 PR 合入确认。L2 provenance audit 与受控重训的确定性合同不是新增人工审批、发布门禁或
  研究方向淘汰；D3-D7设计合同虽已闭合但尚未由实现执行，保持blocked是未授权实现和完整READY合取的准确状态。C-009 不新增
  95% coverage、最大 staleness、人工 alias 确认或数据源准入门禁；C-009-A/B/C/D 已于 2026-07-27 获用户明确批准，
  当前 source implementation/preflight 状态不构成新增人工审批或恢复模型训练的授权。C-010-FORMAL-A 的 `0.90`、120 行与
   full-universe/双层/逐 feature receipts 是用户已批准且已执行的确定性合同，不是运行时人工审批。DIAG-01 的用户确认只用于
   批准新的诊断范围与计算预算，不得被转化为每次训练、每只股票或每个 artifact 的新人工门禁。
  D6-NA-A由用户明确批准且不新增missing-ratio、provider whitelist、sector特批或人工确认；既有`N_evidence>=30`继续是确定性
  source contract，不得被重命名为新审批或在实现中临时提高。

### 23.1 C-010-FORMAL-A 正式设计审核结论

- no simplified/subset/POC：`PASS_USER_APPROVED_DESIGN_AFTER_REVIEW_FIX`。A1 覆盖 full-universe ledger 与 train 后新证券，A2 覆盖 price/moneyflow
  两层 count+weight 和同源 denominator，A3 固定四个 cross-section feature 及 L1=28/L2=118 边界，A4 绑定 formula/policy/request/
  child/candidate/READY identity；不以 `689009.SH` hard-code、absence-only 清单、单层 coverage 或 feature deletion 代替完整方案。
- no silent error/fail-open：`PASS_USER_APPROVED_DESIGN_AFTER_REVIEW_FIX`。零 opportunity、越界 absence、unseen train eligibility、
  structurally unavailable、price/moneyflow weight denominator invalid、coverage insufficient、moneyflow amount denominator invalid、
  cross-section coverage/reference/output/mask invalid、formula/policy/hash drift 均有独立状态和 reason code；
  未定义任何填 0、前值、代理、neutral、默认 eligible 或 old-v1 fallback。
- no business semantic drift：`PASS_USER_APPROVED_DESIGN_AFTER_REVIEW_FIX_WITH_EXPLICIT_FORMULA_CHANGE`。证券、PIT、sector、feature set、hard semantic、
  train/validation 窗口和 D3-D6 均保持；A2/A3 对四个 moneyflow denominator 与四个 price cross-section features 的计算语义变化已
  明确标为已批准的 `hmm_risk_l1_sector_factor_formula_v2_c010`；四项 operator/reference、L1/L2 authority 与 rolling 后 mask 已精确固定，
  未伪装成 v1 不变或在源码中提前激活。
- no unauthorized gate/approval：`PASS_USER_APPROVED_DESIGN_AFTER_REVIEW_FIX`。数值只复用既有 `0.90` 与 120 行 authority，并分别命名四种 coverage 语义；
  没有 95%、staleness cap、人工逐股确认、运行时 acknowledge、方向淘汰或每次训练审批。当前用户决定只用于批准明确的 formula/policy
  amendment；批准后 implementation、preflight、PR merge、5184 fits 与 runtime 仍按既有边界分别执行和报告。

综合结论：`PASS_USER_APPROVED_DESIGN_IMPLEMENTATION_READY`。C-010-A1/A2/A3/A4 的完整正式合同已获用户批准，且 formula v2、失败语义、
exact fields、mask/rolling 与状态一致性缺口均已修复；这只表示设计可进入独立源码实现，不表示源码、601 日 formal preflight、
5184 fits、D5/D6 或 model/READY 已完成。

### 23.1.1 C-010-A5 / BUG-944 修订后正式审核

- **禁止简化/子集/POC**：`PASS_USER_APPROVED_DESIGN_AFTER_REVIEW_FIX`。设计覆盖 full-market `P_all`、direct L1/L2 `O_sector`、逐 key
  predicate、`P_in/P_out` 完备分区、同 symbol 混合 in/out、formal lineage 与601日回读；未用单股 hard-code、删除一条 manifest row、
  去除 SW join、“允许 missing symbol”或预设 `P_out=1` 代替完整合同。601日执行必须分区全部 frozen keys，已知 key 只作为包含性断言。
- **禁止静默错误/fail-open**：`PASS_USER_APPROVED_DESIGN_AFTER_REVIEW_FIX`。四个 predicate 已分别绑定 frozen PIT state/span、stock-fact
  price identity、canonical SW resolver/classify 与 C-009-B security resolver；unavailable 与 invalid 分离。域外 key 保存全部 predicate
  evidence/failed set，分区缺失、重复、冲突、unknown、hash drift、price-domain failure 与 policy identity drift 均 fail closed。
  `P_out` 不会被计为 available、丢弃或用 default/neutral/行业代理补足。
- **禁止业务逻辑迁移**：`PASS_USER_APPROVED_DESIGN_AFTER_REVIEW_FIX`。设计不改变 PIT/selection/runtime prediction universe、C-009 source identity、
  C-010-A2/A3 formula、feature set/order、两个 family、train/validation、hard semantic authority、D3-D6 或 READY 条件；缺少历史 SW
  identity 的证券只是不产生该日 direct-sector contribution，不被从其他场景删除，也不获得虚构 sector。
- **禁止未经确认的门禁和审批**：`PASS_USER_APPROVED_DESIGN_AFTER_REVIEW_FIX`。没有新增 threshold、固定域外数量、人工逐股审批、runtime
  acknowledge、发布门禁或研究方向淘汰；新增内容是确定性 domain/predicate 数据合同。其状态保持
  `RESOLVED_USER_APPROVED_DESIGN_IMPLEMENTATION_READY`；批准的是确定性设计，不是 runtime 人工 gate 或本次执行授权。
- **schema/readback 完整性**：`PASS_USER_APPROVED_DESIGN_AFTER_REVIEW_FIX`。partition/eligibility/expected-opportunity/policy v2 已固定 required
  fields、枚举、cardinality、nested hashes、writer/readback same-authority与v1历史只读/v2新写边界；缺字段或只提供顶层hash不能通过。
- **证据充分性**：`PASS_SOURCE_IMPLEMENTED_601_DAY_PREFLIGHT_VERIFIED_PENDING_CODE_PR`。生产只读 DB 与 Tushare provider 对 `002951.SZ` 的成员事实一致；
  新preflight已完整审计502个 `P_all` keys并得出 `P_in=501/P_out=1`，没有用symbol-level early stop或预设数量替代逐key分区。

综合结论：`PASS_SOURCE_IMPLEMENTED_601_DAY_PREFLIGHT_VERIFIED_PENDING_CODE_PR`。数量预设、predicate authority、v2 schema、writer/readback、
lineage与顶层状态已在rebase后commit `a41d55e0…ce73`闭合；preflight report canonical为`e7f7edc9…773d`。本次仍未恢复D1-B fit，
未生成新的model/READY，也未改变数据库或runtime；源码合入仍是独立状态。

### 23.1.2 C-010-A5 / BUG-944 第二轮源码审核修订

- **diagnostic/formal 模式隔离**：eligibility builder 必须保留 partition receipt 的
  `formal_policy_activated`，不得把 diagnostic-only receipt 强制提升为 formal，也不得把 formal receipt 降级为 diagnostic。
  `evidence()` 只能回读同一模式；模式漂移以 `hmm_risk_c010_contributor_receipt_mismatch` fail closed。该修订恢复既有
  C-010 diagnostic 路径，不改变 formal v2 的政策、阈值或业务语义。
- **predicate same-authority 回读**：reader 不再只信任可重算的顶层 hash。PIT、price、SW L1/L2 均从 typed candidate
  evidence 重建 `available/unavailable/invalid`，并与顶层 status、candidate count/code、resolver receipt 和 source identity
  逐项一致；攻击者即使同时重算 nested/top-level hash，也不能把 `unavailable/P_out` 改写为 `available/P_in`。
- **集合代数闭合**：对 frozen `P_all` 和 expected direct-sector opportunity set `O_sector`，writer/readback 同时强制
  `P_in = P_all ∩ O_sector` 与 `P_out = P_all - O_sector`。因此每个 `P_in` key 必须属于 `O_sector`，每个 `P_out` key
  必须不属于 `O_sector`；partition 完备、互斥和 cardinality 继续独立校验。
- **运行态有效性证据**：BUG-944 改动命中 backend runtime。共享只读 `/api/v1/runtime-identity` 在进程导入时冻结 clean
  checkout 的 exact Git SHA；新增 `/api/v1/runtime-contracts/hmm-risk-c010-a5` 回读 v2 合同常量及同一 merge SHA。
  identity 不可取得、tracked checkout dirty 或已加载合同常量漂移均返回非成功状态，不伪造 ready。
- **重启所有权和 aftercare**：`backend-main` 的启动、停止、重启完全归用户。operator runbook 只描述用户重启后的
  `post-restart-verify`、health/identity/business smoke 和 merge-SHA readback；Skill、CI、merge、close-sync 或本文均不产生
  进程控制授权。未发生 DDL/DML、HMM fit、selection、D6、model/READY 或 runtime action。
- **第二轮 DESIGN-COMPLIANCE-001**：`PASS_AFTER_REVIEW_FIX_PENDING_PR_UPDATE`。没有 single-symbol hard-code、简化子集、
  neutral/default fallback、异常吞噬、hash-only trust、业务政策迁移或新增人工审批；新增 runtime readback 是现有
  `BUG-RESTART-EFFECTIVE-001` 的确定性证据，不是新的研究门禁。

第二轮审核修订完成后，C-010-A5 的源码状态为
`SOURCE_FIXED_REVIEW_PASSED_PENDING_PR_UPDATE_AND_USER_MERGE_AUTHORIZATION`。运行态仍是旧进程/未验证状态；只有代码合入、
用户完成后端重启并由 fresh-process readback 证明 exact merge SHA 后，才可将 `post_restart_effective_gate` 独立更新为通过。

### 23.2 C-008-B3-FORMAL-BLOCKER-DIAG-01 正式设计审核结论

- **禁止简化/子集/POC**：`PASS_USER_APPROVED_DESIGN_COMPLETE`。设计覆盖 formal receipt 中全部150个 D4 rejected
  seed/sector pair、三个 affected family/level、8 seeds、24个确定性 controls 和唯一 selected L1 的 failed D6 entry；没有只诊断
  `801030.SI`、`801980.SI` 或单一 L2 stage，也没有把348-fit诊断声明为两-family模型交付。
- **禁止静默错误**：`PASS_USER_APPROVED_DESIGN_COMPLETE`。authority/target/control/repeat/stage/evidence/hash/collision
  均有 fail-closed 状态；初始化失败不会伪造后续stage，multi-stage failure不会压缩为单一generic reason，non-finite和缺失字段
  不会被null/空数组写成 diagnostic complete。
- **禁止业务逻辑迁移**：`PASS_USER_APPROVED_DESIGN_COMPLETE`。两个 family、direct L1/L2、seeds 42..49、D3-D6、hard
  semantic authority、单一 validation与D5 selected identity均保持。D4 refit不可见validation/future utility；D6 replay不refit、不
  reselect；未加入B2、neutral、index fallback、per-sector stitching、family/sector排除或阈值放宽。
- **禁止未经确认的门禁和审批**：`PASS_USER_APPROVED_DESIGN_COMPLETE`。signed distance是观察字段，不是新gate；设计未增加
  manual acknowledge、逐sector审批、研究方向淘汰或runtime gate。用户批准的是一次确定性诊断合同，不会被转化为永久运行时人工审批。
- **可区分性与结果价值**：`PASS`。formal report只提供compact rejection summary，无法判定失败距阈值的量级或与accepted
  control的几何差异；本设计要求raw likelihood/covariance/hard-sequence和signed-distance evidence，能区分“已批准合同正确拒绝”与
  “初始化/数值/结构需要另行设计修订”，且不预设任何方向必然通过。
- **验证与预算**：`PASS`。150 rejected + 24 controls = 174 unique pair，两个 fresh process精确348 fits；D6为3-entry/0-fit
  replay。完整target覆盖和两次bitwise equality均为diagnostic completeness，不是模型acceptance或新人工门禁。

该执行前正式审核结论为 `PASS_USER_APPROVED_DESIGN_IMPLEMENTATION_READY`：未发现简化交付、静默错误、业务语义
漂移或未经确认的门禁。此处保留为历史设计审核记录；随后执行事实和当前结论以 23.3 为准，不再把任务描述为 not started。

### 23.3 C-008-B3-FORMAL-BLOCKER-DIAG-01 执行证据回填审核

- **禁止简化/子集/POC**：`PASS`。执行覆盖合同规定的150 rejected、24 deterministic controls、两个 fresh process和3个
  D6 replay；未用单一 sector、单一 stage、单一 family或已通过control代替完整 blocker closure。
- **禁止静默错误**：`PASS`。11个 initialization failure 保留为无 fitted downstream evidence；其余multi-stage reason与raw
  evidence未被压缩。文档明确区分变长numeric tree路径差异与真实 `missing_evidence`，不把150个comparison伪报为证据缺失。
- **禁止业务逻辑迁移**：`PASS`。执行没有 selection/reselection、acceptance重跑、threshold变更、hard authority变更、model/READY、
  DB或runtime动作；D6失败保持temporal evidence failure，不改写为missing state或utility failure。
- **禁止未经确认的门禁和审批**：`PASS`。诊断signed distance、matched comparison和分类只形成证据，不成为新gate；本次诊断
  执行审核时五类remediation均保持`PROPOSED_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY`，之后D1-B才由用户选择；没有自动
  淘汰family/sector或新增人工运行时审批。
- **根因结论可区分性**：`PASS`。证据支持“多阶段、多family/level blocker”，并明确不足以批准具体模型机制或阈值；设计没有
  从observed distribution反推新acceptance，也没有把diagnostic completion写成implementation/READY。

综合审核结论：`PASS_DIAGNOSTIC_EVIDENCE_ACCURATELY_INTEGRATED_REMEDIATION_NOT_APPROVED`。本结论只批准设计文档对既有
diagnostic evidence的准确回填，不批准任何remediation实现、模型训练、selection、D6、model/READY或生产动作。

### 23.4 C-008-B3-REMEDIATION-DIAG-02 正式设计审核

- **第二次审核修复**：`RESOLVED`。已补齐train-only allowlisted projection/hash，明确完整artifact authority hash不等同于消费其中
  validation payload；`d6_replay`、semantic/utility/validation字段不得进入DIAG-02计算输入。
- **第二次审核修复**：`RESOLVED`。已把模糊的`2×N`改为固定likelihood/covariance status matrix，固定四组correlation scalar、
  overall/family/level/role分层、Pearson/Spearman/tie/quantile/float64算法和insufficient语义。
- **第二次审核修复**：`RESOLVED`。描述性association不再推导因果或自动选择monotonic EM/covariance方向；任何model mechanism
  必须进入新的controlled-refit design decision，保留current-profile control。
- **第二次审核修复**：`RESOLVED`。schema已补齐section status、stable reason codes、collision、temporary write、fsync、atomic rename与
  canonical readback；group insufficient不能覆盖raw evidence缺失。
- **禁止简化/子集/POC**：`PASS_REPAIRED_PROPOSED_DESIGN_COMPLETE`。覆盖formal四个family/level的324个唯一train profile、
  139 rejected+24 control completed entry以及11个initialization failure；没有只处理`801207.SI`或单一covariance reason。
- **禁止静默错误**：`PASS_REPAIRED_PROPOSED_DESIGN_COMPLETE`。zero/non-positive/non-finite、formula/source/preprocess provenance、
  projection越界、统计不足、profile/hash缺失、collision/readback均有typed fail-closed语义；不会用0 correlation、variance floor、
  validation payload或control evidence伪造完成。
- **禁止业务逻辑迁移**：`PASS_REPAIRED_PROPOSED_DESIGN_COMPLETE`。no-fit诊断不改变7/20维feature、D3-D6、hard authority、seed
  schedule、family/sector、selection或validation；inactive dimension、positive prior和model structure都只列为后续用户决策。
- **禁止未经确认的门禁和审批**：`PASS_REPAIRED_PROPOSED_DESIGN_COMPLETE`。分位数、association和cross-matrix只作描述性
  evidence，不成为acceptance或因果决策；DIAG-02保持pending，未新增runtime人工确认或research方向淘汰流程。
- **因果可区分性**：`PASS_REPAIRED`。先区分persistent zero variance与seed-specific singleton，再按role/family/level观察
  terminal likelihood和M-step residual association，最后重聚合hard-structure persistence；不从targeted sample外推formal universe。

综合审核结论：`PASS_REPAIRED_PROPOSED_DIAGNOSTIC_DESIGN_PENDING_USER_APPROVAL`。第二次审核的五项阻塞缺口均已修复；
该结论表示设计完整且可供用户决策，不表示DIAG-02已获批准或已执行，更不授权任何HMM refit、remediation实现、正式grid、
selection、D6、model/READY、数据库或runtime动作。

执行后正式审核结论：`PASS_EXECUTED_DIAGNOSTIC_CONTRACT_COMPLETE_MODEL_REMEDIATION_NOT_APPROVED`。

- **禁止简化/子集/POC**：`PASS`。源码和artifact覆盖四个family/level的324个唯一profile、全部163个既有completed entry与
  11个initialization failure source；没有只处理zero-variance sector、单一family或单一failure stage。
- **禁止静默错误**：`PASS`。non-finite、temporal/profile/projection/authority/receipt、statistic insufficient、collision与readback
  均有fail-closed语义；6个常量相关向量组显式insufficient，没有伪造0 correlation。并发publisher不得被覆盖。
- **禁止业务逻辑迁移**：`PASS`。执行未读取validation/future utility，未重跑acceptance、未选择seed、未改变hard semantic authority，
  未删除feature、降低D4/D6 threshold或生成model/READY。
- **禁止未经确认的门禁和审批**：`PASS`。variance quantile、correlation和structure persistence仅是描述性证据；未形成新acceptance gate、
  runtime人工确认或研究方向淘汰。该执行审核时`C-008-B3-REMEDIATION-D1`仍为proposed；当前D1-B已由用户选择，但源码与fit仍需独立授权。
- **交付状态分离**：源码producer已提交、repo外artifact已生成并通过canonical readback；design回填不等于PR已合入，PR合入不等于
  dependency/DDL/DML/runtime已激活。本执行的这些生产gate与runtime动作均为noop。

### 23.5 C-008-B3-REMEDIATION-D1 正式设计审核

- **头部与父蓝图漂移**：`RESOLVED`。详细设计已更新修订日期、D1-B已选择但source未开始状态与父蓝图v2.15；同PR同步父蓝图的
  DIAG-02执行事实、D1-B选择、evidence-first P2计划、F-011证据和版本历史。
- **mixed-dimension aggregate缺口**：`RESOLVED`。已定义131-entry独立model entries、唯一19维allowlist、dimension histogram、
  ordered list hash、level/family/READY identity、writer/parser same-authority与禁止padding；D5未闭合前仍禁止写正式artifact。
- **controlled-refit结论缺口**：`RESOLVED`。`diagnostic_complete`已与inconclusive/rejected/effect-supported分离，并单列
  `d5_compatibility_evidence_ready`，禁止用局部fit或first-error伪造mechanism结论。
- **canonical set hash缺口**：`RESOLVED`。两个8-entry set固定seed排序、三字段object和canonical JSON list envelope，可独立重算。
- **runtime receipt缺口**：`RESOLVED`。inactive observation已绑定versioned receipt、dedupe key、InputManifest/daily-run hash lineage、
  conflict/replay语义；finite非零只记录，不动态激活或私增gate。
- **完整性审核**：`PASS_D1_B_USER_SELECTED_SOURCE_NOT_STARTED`。A/B选项、exact allowlist、20→19 projection、full preprocess先行、
  artifact/parser/runtime、D4独立状态、32-fit treatment/control、reason codes与验证矩阵均已定义；用户已选择B，但没有把“drop一列”
  或设计选择冒充完整实现。
- **D5兼容缺口**：`BLOCKED_EXPLICITLY_NOT_SILENT`。审核确认inactive dimension会使`d_family=20`与`d_effective=19`产生真实score
  comparability决策；设计已拆出`D1-D5-COMPAT-01`，controlled-refit固定0 selection。未由实现默认选择19或20，也未把局部fit
  改写为full-grid ready。
- **禁止简化/子集/POC**：`PASS`。D1有意只隔离一个模型机制，但同时覆盖treatment全部8 seeds、同family/level全部8 control seeds、
  双fresh-process identity与未来parser/runtime完整契约；它不是autocycle-only交付或family READY声明。
- **禁止静默错误**：`PASS`。近零、非零常量、non-finite、source/profile/preprocess/mask/shape/hash drift、control drift和repeat mismatch
  均fail closed；没有noise/floor、伪inactive covariance、identity fallback或dynamic activation。
- **禁止业务逻辑迁移**：`PASS`。global 20-feature observation/preprocess contract、两个family、31/131完整性、hard semantic authority、
  D3/D4 thresholds与D6保持不变；B2仍NOT_APPROVED。runtime对inactive维的“不参与likelihood”由model identity显式持久化，不是隐藏fallback。
- **禁止未经确认的门禁和审批**：`PASS`。A/B方向已由用户选择B；controlled-refit和D5 comparability仍保持pending。未增加runtime
  人工确认、feature淘汰审批或研究准入gate。exact allowlist是本次证据范围，不是动态业务门禁。
- **因果可区分性（历史结论）**：`SUPERSEDED_BY_REFIT_01_EVIDENCE`。原设计要求identity20 control与旧payload bitwise一致；BUG-962后的
  current-A5执行证明train observation已真实变化，因此该历史要求不能继续作为current实验成功条件。替代因果合同见23.8：同一801207
  current-A5 input的19D treatment与identity20 matched negative只允许projection差异，801011只作runner harness。

综合审核结论：`PASS_D1_B_USER_SELECTED_SOURCE_NOT_STARTED_CONTROLLED_REFIT_PENDING_D5_COMPAT_BLOCKED`。用户已选择D1-B、
D1-A不采用；源码、32-fit执行和D5 score语义仍未获授权，因此不得由本次文档修订实施源码、运行fit或进入formal grid。

### 23.6 D1-B evidence-first优先级修订正式审核

- **禁止简化/子集/POC**：`PASS`。历史P1与当前P2/P3分别是完整的最小模型机制、runner修订和真实受控训练，不是Phase 2交付声明；P5～P10的
  mixed-dimension artifact、正式训练、两family READY和产品功能合同均保留为mandatory deferred contract，没有从蓝图删除。
- **禁止静默错误**：`PASS`。P1必须实现全部authority/hash/shape/control/repeat/aggregate typed failure；P2任一attempt均形成durable
  terminal evidence。blocked、inconclusive或failed不得转成warning success，D5 19/20不得由实现默认。
- **禁止业务逻辑迁移**：`PASS`。两family、L1/L2完整性、hard semantic authority、D3/D4/D5/D6公式和validation边界均不变；
  evidence-first只调整实施时间，不改变业务验收语义。
- **禁止未经确认的门禁和审批**：`PASS`。没有新增runtime人工确认、research淘汰或发布门禁；源码、32-fit、D5兼容、正式训练和
  PR merge继续作为彼此独立的既有授权/状态边界准确报告。
- **反过度工程**：`PASS`。当前不实现通用动态维度框架、训练平台、调度器、额外DDL、API/UI或伪造READY；P5/P10仅在真实
  上游证据和consumer存在后实施。仅受影响level的2096-fit重训由明确level-local依赖支持，公共算法变化时才允许升级为5184全量。

综合审核结论：`PASS_D1_B_EVIDENCE_FIRST_PRIORITY_COMPLETE_NO_SIMPLIFIED_DELIVERY`。本结论只批准文档中的任务排序，未授权源码、fit、
selection、model/READY、数据库、依赖或runtime动作。

### 23.7 D1-B P1源码正式审核

- **实现范围**：`PASS`。`b3_training.py`仅抽取artifact-neutral的共享train-only HMM数值核心，formal entry/model组装保持v1；
  `b3_d1_inactive_dimension.py`只实现P1 authority/projection/control/process/report/writer；`prepare_state_model_set.py`仅增加从既有三份冻结
  authority启动两个fresh process的窄入口，不实现P4 mixed-dimension selected-level、READY或通用调度框架。
- **禁止简化/子集/POC**：`PASS`。exact-zero正反例、full20 preprocess顺序、19/20 shape、八seed双role、双process、mechanism状态、
  D5 readiness、immutable readback均有直接测试；源码完成不被声明为32-fit、family candidate或READY完成。
- **禁止静默错误**：`PASS`。authority、preprocess payload、source set、manifest、non-finite、near-zero、`+0/-0` bit pattern、control
  frozen hash、numeric environment、attempt completeness、repeat mismatch和artifact collision均fail closed并保留typed reason。
- **禁止业务逻辑迁移**：`PASS`。formal B3旧路径继续生成同一v1 entry/model hash；D1 treatment只在full20 preprocess后固定投影，
  control保持identity20；没有D5/D6/validation/soft authority、动态激活、noise/floor或post-fit projection。
- **禁止未经确认的门禁和审批**：`PASS`。没有新增runtime人工确认或研究准入；源码只使P2可执行，32 fits、D5兼容、正式grid和
  PR merge继续独立报告。
- **审核修复**：`RESOLVED`。所有control frozen hash在首fit前预校验；attempt持久化numeric environment、期望/实际control hash；
  treatment/control train-input manifest与批准profile逐项绑定；父进程拒绝即使canonical hash自洽但携带selection/model/READY/DB/runtime
  side effect的child payload；report保留全部downstream D4 typed failure，不以机制支持吞掉失败；writer在落盘前校验内部receipt。
  首轮正式审核后又固定三份批准输入artifact canonical hash、clean producer commit及child source authority，限制report为repo外显式artifact root，
  并在第二child或parent finalize失败时保留已完成process与typed failure receipt，不伪造未知fit数量。第二轮审核进一步要求
  `fresh_process_1/2`精确identity、process/report/failure writer-readback同一重建authority，并拒绝仅重算顶层hash的伪成功报告。
  report明确`d3_d4_descriptive_contracts_applied=true`与`formal_model_set_acceptance_performed=false`，不使用含糊成功语义。

历史源码审核结论：`PASS_D1_B_P1_SOURCE_COMPLETE_CONTROLLED_REFIT_NOT_EXECUTED`。本结论不表示32 fits、selection、D6、model/READY、
database或runtime已执行。

### 23.8 REFIT-01执行收敛与REFIT-02-A正式设计审核

- **实时证据与设计状态一致性**：`PASS`。BUG-962/PR #3089、Issue #3075、final failure report及current-A5 hashes均已回填；
  REFIT-01准确记录为0 attempts/0 fits、inconclusive，没有沿用“C-010-A5尚未合入”或“32 fits尚未启动”的过期状态。
- **根因分层**：`PASS`。设计区分已修复的v1/v2 authority-envelope矛盾与仍然真实存在的train observation业务变化；历史payload
  readback继续有效，但不再被要求与current-A5 model payload相等。没有通过排除`train_observation_sha256`伪造兼容。
- **禁止简化/子集/POC**：`PASS_PROPOSED_DESIGN_COMPLETE`。REFIT-02-A同时保留801207 19D treatment、同sector identity20
  matched negative、801011 identity20 harness、8 seeds和两个fresh processes；48/48 terminal attempts与32个真实fits均为完整合同，
  没有只跑成功seed、删掉负对照或把harness冒充same-sector效果对照。
- **禁止静默错误或伪成功**：`PASS_PROPOSED_DESIGN_COMPLETE`。negative control底层attempt必须保持`failed/fit_performed=false`，
  顶层只单列blocker是否复现；authority、配对、repeat、harness、attempt set与历史reference均有typed failure。`diagnostic_complete`
  与mechanism、D4和D5 readiness继续分离；current-A5不再exact-zero时使用独立`not_applicable`和0-fit语义，不改选sector或伪造机制结论。
- **禁止业务逻辑迁移**：`PASS_PROPOSED_DESIGN_COMPLETE`。current-A5是三个role唯一训练authority；global 20-feature observation与
  full20 preprocess保持不变，只有treatment按已选择D1-B投影到19维。D3/D4公式、hard semantic authority、validation窗口、D5/D6、
  两family/31/131完整性均未改变。
- **禁止未经确认的门禁和审批**：`PASS_PROPOSED_DESIGN_COMPLETE`。REFIT-02-A明确为
  `PROPOSED_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY`；本修订没有把三角色、schema或结论条件写成已生效代码gate，未增加
  runtime人工确认、研究方向淘汰或发布审批。源码、真实32-fit、D5决定与每个PR merge继续独立授权。
- **因果可区分性**：`PASS`。同一801207 current-A5 full20 input的19D treatment与identity20 negative只允许projection差异；801011
  仅验证共享runner。该结构能区分“projection消除已证明常量维blocker”与“runner/current-A5整体异常”，不会从跨sector分数声称因果。
- **反过度工程**：`PASS`。下一源码切片只扩展现有D1 runner/入口/直接测试，不建设通用实验平台、动态维插件、scheduler、API/UI、
  runtime或READY writer；32真实fit预算与REFIT-01相同。

综合审核结论：`PASS_REFIT_02_A_PROPOSED_DESIGN_COMPLETE_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY`。这表示提案已达到可供用户
决策的完整设计质量，不表示REFIT-02-A已获批准、已实现或已执行；在用户明确批准前保持0新fit、0 D5/D6、0 model/READY及0生产动作。

### 23.9 REFIT-02-A 用户批准后的源码正式审核

- **审核范围**：仅审核 `b3_d1_inactive_dimension.py`、`prepare_state_model_set.py` 与两份 HMM 直接测试；未执行真实32-fit、D5/D6、model/READY、数据库或runtime动作。
- **禁止简化/子集/POC**：`PASS`。每个fresh process固定三角色×seeds 42..49共24个terminal attempts；两process固定48 attempts、32个planned true HMM fits。treatment、matched negative与harness均不可删除、替换或early stop。
- **禁止静默错误或伪成功**：`PASS`。current-A5 authority、历史drift、same-sector full20输入、projection、attempt、process及v4 report均canonical回读；expected negative仍保持raw `fit_failed/fit_performed=false`，顶层只记录blocker reproduced。authority drift、negative未复现、harness失败与重复不一致均保留typed failure，不会改写为READY或幂等成功。
- **禁止业务逻辑迁移**：`PASS`。只有treatment执行固定20→19 projection；matched negative与harness保持identity20。validation、future utility、semantic labelability、D6与selection均不可访问，hard semantic authority、D5分母、两个family和31/131完整性未改变。
- **禁止未经确认的门禁和审批**：`PASS`。已激活的仅是用户明确批准的REFIT-02-A确定性诊断合同；真实32-fit、D5/D6、mixed-dimension artifact/parser、model/READY、数据库、runtime及每个PR merge仍为独立授权边界，没有新增人工runtime审批。
- **fail-closed与可区分性**：`PASS`。current 801207不再exact-zero时固定`not_applicable`、0 attempts/0 fits；当前profile适用时才允许child。`diagnostic_complete`、`mechanism_assessment`与`d5_compatibility_evidence_ready`保持独立；源码完成不推导实验完成或model acceptance。
- **审核发现与修复**：初版聚合器会把已调用HMM fit、但随后在likelihood/covariance/train-structure阶段失败的treatment错误归为`inconclusive`。现已修复为：只要8/8 treatment均越过D1 projection/parameter-shape机制并真实调用fit，机制可记录`constant_dimension_effect_supported`；原D4 reason完整保留且`d5_compatibility_evidence_ready=false`。新增正反例防止D4失败被吞掉或反向伪造D5 readiness。
- **验证证据**：changed files全部映射到`hmm.risk`，classifier=`targeted_ci_required`、`unmapped_code_files=[]`；直接REFIT-02测试、CLI父子authority测试、v4 immutable writer/readback及`hmm_risk_backend` required plan均通过。未增加跨模块验证。

综合源码审核结论：`PASS_REFIT_02_A_SOURCE_IMPLEMENTED_LOCAL_REVIEWED_NOT_EXECUTED`。该结论只表示当前独立worktree源码达到可提交条件；不表示已commit、PR、merge、执行32-fit、选择seed或生成model/READY。

### 23.10 BUG-977 / REFIT-02-B matched-fit 合同正式审核

- **根因闭合**：`PASS`。REFIT-02-A v5真实报告证明matched current-A5 20D初始化已成功，却被fit-budget v1停止；因此旧合同无法观测
  20D outcome，`inconclusive`是合同缺口而不是HMM结论。REFIT-02-B让20D control调用同一train-only fit，历史blocker仅作reference。
- **禁止简化/子集/POC**：`PASS`。三角色、8 seeds、两个fresh processes、48 terminal attempts全部保留；预算只从32扩为匹配三角色的
  最多48 fits，不删harness、不换sector、不按成功seed early stop，也不执行正式grid。
- **禁止静默错误或伪成功**：`PASS`。matched fit success明确为`matched_control_fit_completed`并导致mechanism rejected、D5 readiness=false；
  initialization blocker、混合seed outcome、其他stage failure与repeat/harness/authority异常均保留typed evidence，不改写成negative success。
- **禁止业务逻辑迁移**：`PASS`。HMM参数、current-A5输入、D3/D4公式、19D projection、hard semantic authority与D5/D6均未改变；变更仅修复
  causal control的执行边界。algorithm version保持v1，schema/fit-budget独立升至v6/v2。
- **禁止未经确认的门禁和审批**：`PASS`。新增状态是确定性报告语义，不增加人工runtime gate、研究淘汰或发布审批；D5/D6、selection、
  model/READY、数据库、runtime和merge仍各自独立。
- **历史兼容与副作用**：`PASS`。report v4/v5、process v4/v5、attempt v3/v4继续writer-authority readback；current v6使用append-only输出。
  validation/future utility/semantic/D6访问及selection/model/READY/database/runtime写入标志继续固定false。
- **直接验证**：REFIT-02 fix-point/current+legacy writer-readback、48-fit上限、under-budget、mechanism supported/rejected/inconclusive和D5 readiness
  正反例均进入`backend/tests/hmm_risk/test_b3_d1_inactive_dimension.py`；不增加跨模块验证。

审核结论：`PASS_REFIT_02_B_SOURCE_IMPLEMENTED_MODULE_VALIDATED_NOT_EXECUTED`。直接REFIT-02矩阵、历史v5 artifact readback、
`hmm_risk_backend` required plan、ownership/catalog及F2 design validator均通过。该结论不表示新48-fit诊断、D5/D6、selection、model/READY、
数据库或runtime已执行。该结论是REFIT-02-B执行前的历史source-review状态；执行后的当前事实与下一设计边界由23.11和第24节覆盖，
不得继续把`NOT_EXECUTED`当作当前状态。

### 23.11 REFIT-02-B执行证据与REFIT-03 covariance诊断设计正式审核

- **证据基线**：`PASS`。REFIT-02-B v6报告由producer `9aba2752…`生成，canonical=`3a4de927…36cf`，闭合两fresh
  processes、48/48 attempts、48/48 fit invocations与bitwise equality；19D treatment和20D positive harness均16/16 `fit_completed`且descriptive covariance accepted，
  matched current-A5 20D在16/16 attempts均以`hmm_risk_model_covariance_invalid`终止。报告明确为
  `diagnostic_failed/inconclusive`、D5 readiness=false，未把fit failure或进程完成伪装成机制成功。
- **禁止简化/子集/POC**：`PASS`。REFIT-03提案保留同一三角色、8 seeds、两个fresh processes与48-attempt/最多48-fit上限；
  不只挑失败role、不复用旧parameter、不early stop、不换sector、不扩grid。新evidence覆盖每个raw cell并保留pair/repeat全集。
- **禁止静默错误或伪成功**：`PASS`。raw shape、non-finite、positive/negative zero、negative、Infinity与buffer不可读均有可序列化typed
  evidence；JSON禁止NaN，缺cell/hash/pair/repeat时diagnostic fail closed。raw-invalid时D4 derived bounds明确为not-computable/null，
  禁止默认mass或其他role posterior伪造；covariance exception仍必须携带此前已完成的initialization/monitor/likelihood stage evidence。
  model fit failure与diagnostic completeness独立，aggregate不first-error吞错。
- **禁止业务逻辑迁移**：`PASS`。提案不修改current-A5输入、HMM参数、D3/D4阈值、19D projection、hard semantic、D5/D6或
  family completeness；`state_index`严格为attempt-local，跨role只比较feature-level invalid-coordinate set，禁止隐含state alignment或
  D6 semantic映射。`inactive_coordinate_pattern_consistent`仅是描述性标签，不推导机制批准、feature删除、D4 acceptance或D5 readiness。
- **禁止未经确认的门禁和审批**：`PASS`。新增内容是repo-external只读evidence schema与确定性failure语义，不增加人工runtime gate、
  研究淘汰、发布审批或服务确认。Decision Index明确保持`PROPOSED_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY`。
- **精确性与可复现性**：`PASS`。post-fit raw authority已固定为当前正式训练实际使用的`GaussianHMM._covars_`二维diagonal
  buffer，禁止与公开展开`covars_`混用或fallback；非ndarray、非float64、错误rank/layout均只记录安全metadata/evidence后fail closed，
  不读取任意对象内存或伪造raw bytes。framing已固定为`uint64_be(header_length)+canonical header+uint64_be cell bits`，IEEE-754
  semantic bit pattern、finite `float.hex()`、dtype/endianness/order/hash和inactive mask允许exact readback；initialization/post-fit分离，
  历史attempt v3-v5、process/report v4-v6 immutable，新attempt v6、process/report v7 append-only。两fresh-process payload不一致即失败。
- **再次审核缺口闭合**：`PASS`。第二轮独立审核发现并已修订五项：D4 `L/U`在raw-invalid时不可计算、stage exception丢失上游
  evidence、matrix hash framing未定、跨role state-index语义未封闭、attempt/process/report版本测试混写。修订后全部以明确公式、typed
  not-computable、stage envelope、feature-level comparison与逐schema测试矩阵闭合；没有通过默认值、放宽阈值或新增审批回避问题。
- **测试与路由**：`PASS`。未来changed files只属于`hmm.risk`，直接fix-point覆盖cell分类、framing/hash、derived-not-computable、
  partial-stage envelope、feature-level pair/repeat、attempt v3-v6与process/report v4-v7逐版本readback/collision、evidence v1和全部
  no-access/no-write flags；无shared contract变化，不增加其他模块测试。本docs-only修订只运行F2 validator、文档
  contract/scope与`git diff --check`。
- **副作用与完成边界**：`PASS`。本次不实施源码、不重跑HMM、不选择seed、不执行D5/D6、不生成model/READY、不写数据库、不控制
  runtime，也不创建新的业务门禁。文档提交、PR与merge仍是独立状态。

审核结论：`PASS_DESIGN_CONTRACT_COMPLETE_PROPOSED_NOT_APPROVED_NOT_IMPLEMENTATION_READY`。该结论只表示REFIT-03诊断设计已经
达到可供用户决策的精确程度；不表示用户已批准、源码可直接实施、48-fit可执行或D1/D5机制已闭合。

### 23.12 REFIT-03真实结果与D5兼容决策包正式审核（批准前历史快照）

- **证据权威与可复现性**：`PASS`。report canonical `7e8a1755…76b9`、bundle canonical `7ac88ef9…46e2`、
  两进程 comparable payload `53574f62…088f` 已按仓库 canonical JSON 规则回读；48/48 attempts/fits与三角色×8 seeds×2
  processes完整，未用文件字节hash替代canonical hash。证据只外推到同一host/固定数值环境。
- **禁止简化/子集/POC**：`PASS`。审核同时保留19D treatment、matched 20D negative与20D positive harness的全部seed；
  未以treatment局部成功替代two-family、131-sector、D5/D6或READY验收。推荐D5-A仍要求131/131 eligibility，明确拒绝排除801207。
- **禁止静默错误或伪成功**：`PASS`。matched role的16次covariance failure、三次inactive-only pattern与五次active/cross-role
  pattern均逐项保留；`diagnostic_complete`与model failure独立，mechanism明确为`inconclusive`，D5 readiness为false。
  未把fit调用返回、进程完成或bitwise equality伪装成formal model acceptance。
- **禁止业务逻辑迁移**：`PASS`。D1-B的level-local engineering robustness仅为待用户决定的解释边界；未改HMM参数、D4阈值、
  hard semantic authority、train/validation窗口、family/sector完整性或D5选择规则。D5-A只是精确候选，未由文档变成active实现。
- **禁止未经确认的门禁和审批**：`PASS`。新增项是确定性设计决策与train-only公式实现证据，不增加runtime人工审批、经验性score阈值、
  研究方向淘汰、发布门禁或服务控制。`C-008-B3-D1-POST-REFIT03-A`与
  `C-008-B3-REMEDIATION-D1-D5-COMPAT-01`均保持`PROPOSED_PENDING_USER_DECISION`；C/D仅为不推荐选项，不标记用户已淘汰。
- **最小且完整的后续边界**：`PASS`。先决策，再实现mixed-dimension artifact/parser和score receipt，并以公式/identity/131-entry
  完整性的确定性直接测试验证实现；随后执行受影响L2 2096 fits及获批D5/D6。没有共享KMeans/EM/covariance变化，故不授权完整5184重跑；不扩大seed。
- **正式复审缺口修订**：`PASS`。统一唯一ID为`C-008-B3-REMEDIATION-D1-D5-COMPAT-01`，并将
  `C-008-B3-D1-REFIT-03-RESULT-AUDIT-01`加入Decision Index；GaussianHMM结论限定到冻结输入/参数/seeds/固定环境，
  所有current status的`accepted`均限定为descriptive covariance status且明确formal acceptance=false；C/D改为
  `PROPOSED_NOT_RECOMMENDED_PENDING_USER_DECISION`，不再提前声明用户已淘汰；删除未定义的经验性可比性验收，改为
  公式、dimension identity、131-entry完整性和canonical readback的确定性直接测试。修订未改变A/B/C/D推荐关系或自动批准任何方案。
- **状态与副作用**：`PASS`。本docs-only修订没有训练、selection、D6、model/READY、DDL/DML、依赖、客户端或runtime动作；
  merge仍须用户单独确认。严格进度保持`11/17=64.71%`，诊断完成不增加产品验收计数。

审核结论：`PASS_RESULT_AUDIT_COMPLETE_PROPOSED_DECISIONS_NOT_APPROVED_NOT_IMPLEMENTATION_READY`。该结论只证明REFIT-03结果被
完整、非简化、非静默地纳入设计；不批准D1工程语义、D5-A、mixed-dimension实现、2096-fit训练或模型交付。

### 23.13 D1-D5 mixed-dimension源码正式审核

- **批准边界**：`PASS`。用户已批准`C-008-B3-D1-POST-REFIT03-A`和
  `C-008-B3-REMEDIATION-D1-D5-COMPAT-01-A`；本切片只实现mixed-dimension model/level schema、projection receipt、
  repeat/parser、selected-level/READY readback与D5 effective-dimension score。未运行2096 fits、未执行真实D5/D6、未写model/READY、
  未访问数据库或控制runtime。
- **禁止简化/子集/POC**：`PASS`。`autocycle_all_core:L2`仍要求131/131 entries和单一family-global seed；
  `801207.SI`固定19维，其余130 entries显式identity20，histogram固定`{"19":1,"20":130}`。没有padding、伪inactive参数、
  sector排除、per-sector seed stitching或single-family完成声明。
- **禁止静默错误**：`PASS`。unknown/missing projection、preprocess/source/hash/mask/count/shape drift、额外19维entry、非有限score、
  repeat mismatch与level缺失分别fail closed；entry与model projection identity必须一致。projection receipt持久化raw/preprocessed
  variance、zero/sign count、归一化bit-pattern count、固定treatment source-profile receipt及current formal profile identity；projected row count
  必须与D5 `training_rows`相等。旧v1 repeat/model/selected artifact路径保持不变，不把缺失projection默认为identity20。
- **禁止业务逻辑迁移**：`PASS`。full20 observation与full20 preprocess先执行，再按固定indices投影；D5仍使用train-only
  `(minimum, median, math.fsum mean)` lexicographic selection，只把批准公式分母明确为entry `d_i`。validation、future utility、
  hard semantic authority、D4阈值、两family和31/131完整性均未改变。
- **禁止未经确认的门禁和审批**：`PASS`。没有经验性19D/20D可比阈值、runtime人工确认、research淘汰或发布审批；
  `hmm_risk_model_inactive_dimension_d5_comparability_unresolved`仅保留为历史artifact reason，新路径不再产生。
- **writer/parser同权威**：`PASS`。model hash覆盖dimension contract与projection hash；level identity覆盖按sector升序的
  model/projection/dimension list及histogram；READY layer hash继续覆盖D5 receipt与dimension identity。训练入口、repeat回读、D6输入投影、
  selected-level validator、READY writer及落盘selected-level/four-layer model-set readback parser共用同一projection validator；readback拒绝
  非canonical bytes、path escape、manifest/layer/artifact/hash/source-profile/score drift，不fallback到previous或partial artifact。
- **确定性直接验证**：`PASS`。正反例覆盖19D target、20D identity、full preprocess后固定projection、model/repeat roundtrip、
  rehashed mask/count/source-profile/exact-zero drift、非零inactive拒绝、projected row-count drift、effective-dimension公式、per-entry
  denominator/score hash、131-entry完整性、落盘READY roundtrip与旧v1回归；直接矩阵`56 passed`，
  `hmm_risk_backend=320 passed`、总覆盖率`75.70%`，F2 validator为`PASS`且warnings=0。该证据不替代后续正式2096-fit执行结果。
- **第二轮审核修复**：`RESOLVED`。补齐五项阻塞：exact-zero/source-profile durable evidence、projection rows与D5分母闭合、
  per-entry D5 denominator/score receipt hash、独立落盘selected-level/four-layer readback parser，以及本节审核状态与真实实现一致性。
  修复未改变D1 active indices、D5公式、D4/D6阈值、family completeness或runtime边界。

审核结论：`PASS_D1_D5_COMPAT_SOURCE_IMPLEMENTED_LOCAL_REVIEWED_FORMAL_GRID_NOT_EXECUTED`。该结论只表示源码达到可提交条件；
不表示已commit、PR、merge、运行2096 fits、选择seed、执行D6或生成model/READY。

### 23.14 BUG-995：P6 exact level-local formal executor 正式审核

PR #3197 正式代码审核发现三个阻塞问题（C-001/C-002/C-003），本 PR 已逐项修复并通过正式复审；下述审核结论只代表源码达到可提交条件，不代表已合入、已执行 2096 fits、D5/D6 或生成 model/READY。

- **根因与修复范围**：`PASS`。批准的 P6 只允许 `autocycle_all_core:L2` 的 `131 sectors × 8 seeds × 2 fresh processes = 2096 fits`，但历史 `--b3-preparation-output` 只能执行两 family、L1/L2 的完整 5184-fit grid。BUG-995 新增独立 `--b3-p6-autocycle-l2-output` 与隐藏 child 入口；旧 full-grid 入口保持不变，P6 与其他 output/child/diagnostic authority 参数互斥，禁止通过组合参数意外扩大训练范围。
- **C-001 true L2-only construction**：`PASS`（修复+反例）。新增最小且正式的 `_direct_l2_train_series_for_family`（child train-only）与 `_direct_l2_series_for_family`（D6 semantic/validation）构造路径，二者只读取 `inputs["l2_panel"]` 与 L2 constituents，绝不访问 L1 panel/constituents；family-wide `_direct_train_series_for_family`/`_direct_series_for_family` 保留给历史 full-grid，其 L2 分支复用同一 L2-only helper（构造行为一致）。P6 child 与 D6 只调用 L2-only helper。反例测试：`test_p6_single_pass_runs_only_autocycle_l2_exact_grid`（`_direct_train_series_for_family` 被设为 pytest.fail 仍完成 1048-entry grid）、`test_p6_l2_only_train_constructor_never_touches_l1`、`test_p6_l2_only_validation_constructor_never_touches_l1`（build 函数断言 panel 恒为 L2 panel、`direct_sector_level=L2`、`expected_sector_count=131`），`_p6_parent_setup` 中 `_direct_series_for_family` 被设为 pytest.fail 而 D5/D6 仍按契约运行。证明 P6 只构建 `autocycle_all_core:L2`，L1 构造路径即使被设为抛错也不会被 P6 访问，仍严格得到 `131 sectors × 8 seeds × 2 fresh processes`。
- **C-002 parent-authoritative child closure**：`PASS`（修复+正反例）。D5 前 parent 使用自身重载的正式 train authority 派生 `_b3_p6_closure_from_inputs`：exact autocycle_all_core family、exact `ALL_CORE_FEATURES` 顺序、approved preprocess family（identity 或 winsor_zscore_1_99_train_global_v1）、canonical 131-sector L2 集合/哈希与 authority keys；`_validate_b3_p6_child_payload(..., expected=closure)` 在两个 child repeat 上逐项匹配，任何 feature/preprocess/sector/hash/schedule/authority drift 都在 `select_level_restart()` 前 fail closed，绝不以 child 自报 sector set 作为唯一 expected authority，绝不放宽 D3/D4/D5/D6 阈值，绝不用 validation/future utility/semantic labelability/D6 状态选择 seed。反例测试：`test_p6_child_validator_rejects_self_hashed_feature_names_drift`、`..._preprocess_family_drift`、`..._replaced_l2_sector`、`..._missing_l2_sector`、`test_p6_parent_fails_closed_before_d5_on_child_feature_drift`、`test_p6_parent_fails_closed_on_two_process_sector_mismatch`（均断言 `select_level_restart` 未被调用）；正例：`test_p6_parent_runs_two_exact_children_and_blocks_without_d5_candidate` 与 `test_p6_parent_persists_only_accepted_selected_level_and_never_ready` 证明正确 authority 可进入原 D5，D5 accepted 后只执行一次 D6。
- **C-003 whole-finalization durable failure**：`PASS`（修复+正反例）。parent finalization（execution + report write + report readback + equality/hash 校验）整体纳入 try/except；任何 finalization 失败都在独立 sibling 路径 `<output-stem>.parent.failure.json` 写入 durable parent failure receipt，绝不尝试用 failure 覆盖已存在的 immutable success/report 路径，绝不因 readback 失败或 collision 伪造 READY。failure receipt 保留 verified child process count、child receipt paths/hashes、terminal_entry_count、`fit_grid_completed`、selection/D6 已知或 unknown 状态（已知为 True/False，未知为 `unknown_due_parent_failure`/None）、selected-level write 状态、`phase2_ready=false`、`ready_artifact_write_performed=false`、database/runtime flags=false、typed error/failure_stage。反例测试（CLI 级，两 child 完成后）：`test_p6_cli_report_write_failure_writes_durable_parent_failure`（report write 失败，`failure_stage=report_write`，verified_process_count=2/terminal=2096/fit_grid_completed=true）、`test_p6_cli_report_readback_mismatch_writes_durable_parent_failure`（readback mismatch，`failure_stage=report_readback`，旧 report 未被篡改）、`test_p6_cli_report_collision_preserves_existing_report`（已存在不同 report 的 collision，旧 report 未被覆盖）、`test_p6_cli_execution_failure_writes_parent_failure_with_unknown_states`（execution 失败保持 unknown 状态，不伪造为 false/success）。正例：`test_p6_cli_routes_only_to_level_local_executor` 证明 CLI 只路由到 P6 level-local executor。
- **训练与选择顺序**：`PASS`（未改变）。两个 fresh child 必须按固定单线程环境顺序完成，各自产生 `131 × 8 = 1048` 个 terminal entry；两份 request、producer、dataset、mapping、calendar、L2 stock-fact、feature-domain policy 与 sector/schedule identity 全部与 parent authority 闭合后，父进程才执行该 level 的 train-only D5。D5 未接受时不得读取 semantic validation；D5 identity 冻结后才允许读取批准的 validation 输入并执行 D6，D6 失败不得返回 D5 换 seed，也不得扩大 seed grid、按 sector 拼 seed或改用另一 family。
- **持久化与失败语义**：`PASS`（未改变）。每个 child receipt、child failure 与 parent report 都是 canonical JSON、append-only 路径、collision/readback fail closed。第二进程失败必须保留第一进程 receipt；typed failure 不得被 stderr 文本、进程完成或 fit count 吞并。
- **artifact 与 READY 边界**：`PASS`（未改变）。只有 D5 与 D6 均通过时才允许写该 `autocycle_all_core:L2` 的 immutable selected-level artifact，并立即用共同 parser 回读；该写入仍不表示 family model set 或 Phase 2 READY。P6 永远写明 `family_model_set_status=blocked`、`phase2_ready=false`、`ready_manifest_path=null`、`ready_artifact_write_performed=false`，不写数据库、不控制 runtime。
- **禁止简化与业务漂移**：`PASS`（未改变）。实现不得减少 131 sectors、跳过任何 restart、early stop、复用单进程结果、使用 validation/future utility 做 D5、用 D6 换 seed、排除失败 sector、执行 autocycle-only 的 Phase 2 完成声明或回退到历史 full-grid。hard semantic authority、D3/D4/D5/D6 数值合同、train/validation 窗口、D1 projection 与 effective-dimension 分母、seeds 42..49 与两 family 最终完整性均未改变；D6 失败不得返回 D5 换 seed；READY 必须四层完整。
- **验证边界**：`PASS`（未改变）。direct fix-point 覆盖 exact single-pass、双 child 与单线程环境、D5 前禁止 semantic access、D5 后单次 D6、selected-level canonical write/readback、child/parent failure receipt、scope/hash drift、mode isolation、timeout recovery，以及 CLI 不调用历史 full-grid executor。required plan 仍只路由 `hmm.risk`；不扩展其他模块测试。
- **当前状态**：`SOURCE_REVIEW_FIXED_THREE_FINDINGS_CLOSED_NOT_EXECUTED_PENDING_PR`。本节记录源码合同与审核结果，不代表 BUG-995 已合入，亦不代表 2096 fits、D5、D6、selected-level artifact、model/READY、DDL/DML、依赖或 runtime 动作已经发生。

审核结论：`PASS_BUG_995_REVIEW_FIXED_THREE_FINDINGS_CLOSED_NOT_EXECUTED_PENDING_PR`。

### 23.15 BUG-999：raw exact-zero 与 full preprocess replay 合同修复

首次 P6 执行使用 producer `d1b4c35f194fbec143ec5c23f62046acc862ecc8`、当前 C-010 policy
`b1e72d95cd2c105d8e5561005cb9250b853d7598276801ae9cb8689da8e6c871` 和固定单线程 Conda `AIstock`
环境，完成两个 fresh process 的 `1048+1048=2096` terminal entries。两次 child authority 闭合后 D5 正常执行，
但 8 个 seed 均缺失 `801207.SI` model，最终 `hmm_risk_model_selection_unavailable`；D6 未运行，selected-level、
model、READY、database 与 runtime 写入均为 0。正式 report canonical SHA-256 为
`4f53347b4aa0bf957c3c4f7b4d073e267007b9c1502e6afc0bcb46e6c8555dd2`。

- **根因**：`build_projection_receipt()` 正确地先计算批准的 global full-20 preprocess，再执行固定 20→19 projection，
  但旧 projection v1 同时要求 raw inactive vector 与 preprocessed inactive vector exact-zero。本次真实 feature 19 的
  `winsor_high=-0.00268461666241596`、`center=-0.041761032442194194`、`scale=0.0231404684253839`，因此 raw zero
  会被批准公式确定性映射为非零；旧条件与“full preprocess 先于 fixed projection”自相矛盾，并在任何 HMM fit 前拒绝
  `801207.SI` 的 seeds 42..49。该失败不是 D4 模型验收结论，也不能通过排除 sector 或放宽 D5 修复。
- **修复合同**：projection receipt/algorithm 升级为 v2。raw inactive vector 继续必须 exact-zero，禁止 epsilon、近零、
  imputation 或动态 mask；调用方提供的完整 preprocessed matrix 必须与 `_apply_preprocess(raw, approved_params)` bitwise
  相同。preprocessed inactive vector允许是该公式产生的确定性非零值，并保存 expected/observed vector SHA-256、统计摘要与
  `preprocessed_matches_approved_transform=true`；固定 mask 随后删除该列，KMeans/HMM 仍只接收19维。
- **真实输入 fix-point**：在同一正式 request/data/policy authority 上仅重建输入、不执行 HMM，`801207.SI` train rows=`473`，
  raw inactive zero count=`473`且all-zero；批准公式重算后的inactive列min=max=`1.6886613987862675`、all-zero=false，
  expected/observed transform匹配，固定projection shape=`[473,19]`，projection SHA-256=
  `c280a0fba3d6a4eced3a749a81f36d8e2b96842f53ba3115f153151bccbd9f60`。该检查的fit/selection/database-write均为false，
  只证明原阻断点已被精确修复，不证明D4/D5/D6或READY通过。
- **fail-closed 边界**：raw inactive nonzero/non-finite、preprocessed payload 篡改、preprocess identity/hash 漂移、mask/shape/hash
  漂移仍使用 typed `hmm_risk_model_inactive_dimension_contract_invalid` 失败。其他130个 sector继续 identity20；
  `likelihood_feature_count_histogram={"19":1,"20":130}`、D3/D4/D5/D6、seeds 42..49、两个 fresh processes、
  hard semantic authority 与两-family READY 完整性均不改变。
- **执行边界**：BUG-999 源码、测试与本节设计修订合入前不得重跑正式 P6；合入后使用新的 clean-main producer、同一
  frozen request/data authority 和新 artifact 路径重新执行完整2096 fits，不复用本次失败 child model payload，不覆盖历史报告。
  D5/D6 仍按结果 fail closed，不预设修复后必然产生 selected-level artifact。
- **DESIGN-COMPLIANCE-001**：不删除 sector、不减少 seed/fits、不使用旧 model 或20维 fallback，满足禁止简化；preprocess
  expected/observed 精确回放与typed failure满足禁止静默错误；full preprocess、fixed projection、effective-dimension D5 与hard D6
  语义不变，满足禁止业务逻辑迁移；没有新增人工门禁、运行时审批或研究方向淘汰。

当前状态：`BUG999_SOURCE_FIXED_LOCAL_TESTED_PENDING_FORMAL_REVIEW_PR_MERGE_P6_RERUN`。

### 23.16 C-008-B3-D6-NA-A 详细设计正式审核

- **第二轮审核发现与修复**：第二轮独立审核曾以`FAIL`指出5个P1：NA carrier/manifest未定义、full20/effective19
  preprocess/projection顺序不明确、D6 amendment与selected artifact/READY版本未闭合、assignment/evidence集合语义冲突、zero-refit
  lineage不足。修订后分别由D6-NA-A第9-15项及§16.2 exact fix-points闭合，不把问题降级为实现自行决定。
- **批准与范围闭包**：`PASS`。本节只把用户批准的完整182日calendar ledger、feature-NA transition-only、utility-NA evidence
  exclusion与calendar-aware structure写入D6-01-B，并补足其实现级schema/lineage；没有实施BUG-1008/D6-NA-A源码、重跑HMM、
  重选seed、写model/READY或触发数据库/runtime。
- **carrier/source authority**：`PASS`。`hmm_risk_d6_validation_calendar_series_v1`与
  `hmm_risk_d6_frozen_input_manifest_v2`定义full calendar、compact finite payload、positions/masks/value/source hashes；v1仅历史读取，
  source constructor禁止`.dropna()`压缩日期，NaN/Infinity/null不得进入numeric payload。
- **preprocess/projection/causal order**：`PASS`。availability固定由raw family-level full 7D/20D判定；仅O行执行冻结preprocess，
  D1 entry继续验证raw inactive exact-zero与full20 replay后才project 19D；T\O只transition，不把compact rows传给旧dense filter。
- **assignment/evidence status**：`PASS`。T/O/U/E唯一化；posterior覆盖T，T\E argmax只诊断，E hard assignment是semantic authority；
  assignment status不读取utility数值或D6 thresholds，evidence status独立执行既有30行/structure/utility gates。
- **version/readback/lineage**：`PASS`。composite D6 version同时绑定base+amendment；普通/mixed selected artifact均升级schema并由共同
  parser/READY validator重算，旧D6-only receipt不能grandfather。zero-refit envelope绑定原P6 parent/children、D5 selection、131 model
  hashes、train/semantic identities及双producer commits，并显式记录0 fit/no reselection/no parameter change。
- **no simplified/subset/POC**：`PASS`。完整ledger、131个selected L2 entry与D5 seed43 identity均保留；缺失日不会被删除，
  transition-only日不会冒充hard semantic evidence，6个当前阻断entry也不会被静默排除。
- **no silent error/fail-open**：`PASS`。observation/utility availability、mode、reason、mask、ledger/posterior/evidence hash和
  writer/readback authority均为正式receipt字段；partial feature、部分utility公式、imputation、date compression、跨gap transition、
  旧schema grandfather和lineage猜测全部禁止。
- **no business semantic drift**：`PASS`。hard argmax仍是唯一semantic authority，0.35/0.35/0.30 utility、单一validation窗口、
  fitted startprob causal prior、D5-01-B、D6-01-B thresholds、两family完整性与READY合取均不变；B2/neutral/index fallback不采用。
- **no unauthorized gate/approval**：`PASS`。没有新增missing-ratio、provider whitelist、sector exception、人工确认或runtime门禁；
  只在`E`上执行既有D6 gates与既有`N_evidence>=30` source contract。证据不足保持deterministic fail closed，不等同于新增审批。
- **第三轮公式与边界审核**：`PASS`。首日/中间/末日NA、observation-only/utility-only NA、posterior finite/normalization、calendar gap
  run/transition断开、29/30 evidence rows、mask/positions/value/hash、full20→19 projection、旧schema、lineage drift与D6 failure不得
  reselection均已有明确正反例；实现不得自行选择阈值或兼容fallback。

审核结论：`PASS_AFTER_SECOND_REVIEW_FIX_DESIGN_APPROVED_SOURCE_IMPLEMENTED_LOCAL_REVIEWED_PENDING_PR_NOT_READY`。
源码合入与zero-refit replay仍是独立状态；在源码PR另行取得合入确认并完成真实replay前，不得把本设计或本地实现报告为运行中模型合同已生效。

### 23.17 C-008-B3-D6-NA-A 源码正式审核

- **审核结论**：`PASS_AFTER_REVIEW_FIXES_PR_3258_PENDING_MERGE`。实现覆盖log-space因果后验、182日calendar carrier、manifest v2、source constructor、
  transition-only、T/O/U/E、gap-aware D6、composite selected schema、READY active-schema readback与zero-refit CLI lineage；没有执行HMM fit、
  D5 selection、D6 replay或model/READY写入。
- **no simplified/subset/POC**：`PASS`。正式路径保留完整calendar与全部selected sector；旧dense v1不能进入active writer/READY，
  任何单entry/单family结果仍不能推导Phase 2完成。
- **no silent error/fail-open**：`PASS`。compact numeric payload拒绝NaN/Infinity/null；mask/positions/value/source receipt/ledger/manifest
  任一漂移由writer/evaluator/readback共同validator拒绝；无evidence日不伪造hard assignment或utility。
- **no business semantic drift**：`PASS`。hard argmax、0.35/0.35/0.30 utility、D5 seed43 identity、D6-01-B阈值、D1 full20→19顺序、
  两family完整性与READY合取不变；B2、neutral/index fallback、插补、日期压缩、reselection均未引入。
- **no unauthorized gate/approval**：`PASS`。只复用N_evidence>=30和现有D6 gates；未增加missing-ratio、provider whitelist、sector例外、
  人工确认或runtime门禁。
- **直接证据**：Ruff/format、py_compile及五个直接测试文件已通过；覆盖首/中/末transition-only、T\E diagnostic tie、29/30 evidence、
  gap断开run/transition、manifest/receipt drift、raw20 exact-zero后preprocess/projection、旧schema拒绝和zero-refit no-fit/no-D5 flags。
- **第二轮源码审核修复**：初审发现历史 C-008-A/B1/DIAG-02/DIAG-04 仍按 dense posterior/utility 行对齐合同消费数据，若误接 D6 carrier
  会在拟合后发生 utility 行数错位；现以 `build_legacy_dense_diagnostic_series()` 显式隔离历史诊断，正式 D6 继续唯一使用完整 calendar
  constructor。另发现 zero-refit 可与隐藏 child flag 组合并被较早 child dispatch 覆盖；现于任何 source/fit/dispatch 前拒绝全部 B3 hidden
  child 与 child identity。四个入口路由、全部 child identity 与 CLI dispatch 反例均已有直接测试，`hmm_risk_backend` 387 项通过、coverage
  76.21%。复审未发现简化交付、静默错误、业务语义迁移或未经确认的门禁。

### 23.18 TRAIN-STABILITY-DIAG-01结果与TRANSITION-DWELL-B正式设计审核

审核范围严格为P2-2诊断事实回填、P2-2A精确候选、Design Acceptance Index、验收矩阵、父蓝图状态和优先级；没有审核或修改
尚未实现的源码。第一轮发现并修复以下阻断设计问题：

1. **把诊断性D6结构阈值升级为D5 gate**：按active D4-03-PERSISTENT-A重算两个182-row窗口后，8个seed仍全部无完整候选，
   因此明确将`D5-STABILITY-ELIGIBILITY-A`标为not selected，禁止清空candidate set后伪称稳定性修复。
2. **单向sticky prior可能加剧吸收态**：失败window同时有低占用与高run concentration，不能只提高self-transition floor。
   修订为以KMeans train transition为中心、self center双向clip至`[0.50,0.90]`的Dirichlet MAP prior；expected dwell只解释，
   fitted structure仍由不变D4-03验收。
3. **prior与MAP convergence authority不闭合**：若只改`transmat_prior`却不把参数相关transition prior项加入active MAP objective，
   停止authority与训练目标会不一致。修订后精确定义`J_t^B`、raw transmat有效性、typed reason与D5 raw likelihood不漂移。
4. **实验范围与正式启用混淆**：修订后首次2096-fit treatment明确为no-D5/no-D6/no-model/no-READY受控实验；实验成功只允许提交
   正式启用决策，不能直接选择seed或交付模型。
5. **过度工程风险**：明确排除HSMM、通用训练/证据平台、新scheduler、重复输入物化及其他level/family默认重训；既有control
   只读复用，不复制历史大JSON。

第二轮逐项审核结论：

- **公式完整性：PASS**。transition center、off-diagonal normalization、Dirichlet prior、MAP objective、row/denominator/finite边界、
  D5 score source均有精确定义；新增数值已由用户于2026-08-12明确批准，但尚未进入源码或运行态。
- **因果与选择隔离：PASS**。prior只读train KMeans counts；validation/future utility/D6不可见；不扩大seed、不per-sector stitching。
- **状态与错误语义：PASS**。source drift在0 fits处insufficient；prior/MAP/transmat/repeat/writer分别typed fail closed；0候选不伪造成功。
- **范围与成本：PASS**。直接blocker为`autocycle_all_core:L2`，故首次实验只运行131×8×2=2096 fits；没有共享feature/emission/
  covariance合同变化，不触发其他level/family测试或训练。
- **产品目标一致性：PASS**。该候选只解除F-011模型验收blocker，READY后仍按父蓝图立即进入真实预测/预警纵切；不增加产品完成度。
- **DESIGN-COMPLIANCE-001：PASS**。无简化/partial完成声明；无静默错误；hard semantic、D4/D5/D6与两family目标不迁移；
  无未确认门禁或人工审批。

用户于2026-08-12确认全部精确数值、公式、scope与结果判定后，正式审核结论更新为
`PASS_EXACT_DESIGN_USER_APPROVED_MERGE_READY_NOT_IMPLEMENTED`。这里的merge-ready只表示文档内部完整、合同已获批准且可提交；
不表示源码已实现、2096 fits已授权、D5/D6已执行或模型/READY可写。

## 24. 当前完成状态与下一步

本文件已闭合 C-001-A/C-002-A/C-003-A/C-006-A/C-007-A/C-008-D1/C-008-B1、C-008-B3-STRUCTURAL-A、
D3-01-A、D3-02-B、D3-03-A、D4-01-MAP-A、D4-02-A、D4-03-PERSISTENT-A、D5-01-B、D6-01-B、固定环境 D5-02-B、D7-01-A、
C-008-B3-D4-L2-AUDIT-01 与受控 L2 重训设计 A，并登记 DIAG-02/03/04 historical evidence。C-008-B2 继续为
`NOT_APPROVED`。C-009、BUG-892、C-010-FORMAL-A v1 与 C-010-A5 source/preflight均已合入；provider-audit/contributor-opportunity
domain partition已不再是上游blocker。C-008-B3-REMEDIATION-DIAG-02 已按批准的no-fit合同完成并通过执行后正式审核。
BUG-1184 新增的历史成员版本/区间冲突是独立上游blocker，不否定上述price/moneyflow/domain成果，但会阻止当前HR1 request进入fit。

正式 B3 当前状态为：

- producer=`e2c01bae156281d551b084156fec4a09ed5a84ee`；formal canonical=`e7992f87…39f`；
- 两次 fresh-process 总 fits=`5184/5184`，bitwise receipt identity 已闭合；
- D5 已执行，validation/future utility 未用于 selection，selection 后未 refit；
- `legacy_covfix:L1` 选中 seed 43，但 `801980.SI` 在 D6 hard structure evidence 失败；
- `autocycle_all_core:L1/L2` 与 `legacy_covfix:L2` 无 eligible D5 candidate；formal rejection summaries 为
  9/74/67 seed-sector pairs；
- 两 family 均 `blocked`，READY artifact数为0，`model_write/ready_write/database_write/runtime_action=false`。

上述B3结果是历史事实，当前F-011 parent已由后续C-013/BUG-1193闭环、HR1正式结果和父蓝图v2.36更新为
`G2_A_HR1_FORMAL_NOT_AVAILABLE_RW1_EXACT_CONTRACT_PENDING`。旧B3、P2-3A与P2-3B失败只作历史决策证据；
P2-3C/P2-4 terminal结果及C-012-RL1源码事实保持不变。HR1已在10/24 fits后因经济验收失败停止；后续只按§24.1精确批准并实施唯一RW1，
通过后继续真实单日prediction/API/UI，不再复用本历史章节生成任务。F-011-D仍为`FULL_READY_ZERO_CAPABILITY_AVAILABLE_ZERO_HR1_NOT_AVAILABLE`，
F-012保持`DESIGN_READY_USER_APPROVED`，F-013保持`APPROVED_BY_USER_G2_A_L1_PRODUCT_AND_G2_B_EXPANSION_PENDING`。

历史`C-008-B3-FORMAL-BLOCKER-DIAG-01`、`C-008-B3-REMEDIATION-DIAG-02`与REFIT-01/02/03证据保持append-only只读，用于解释既有合同来源；它们已经完成，不再产生后续任务、重复fit或新的产品验收计数。

### 23.19 C-011 产品验收方向修订正式审核

审核范围为父蓝图v2.25、§4.3.4、Decision Index、Design Acceptance Index/Matrix、§24优先级和历史提案状态；不审核尚未设计的模型源码。三轮审核结论如下：

1. **无简化/partial伪完成：PASS**。COVERAGE_AVAILABLE不是READY别名，必须有canonical denominator、产品指标、代表性与typed unavailable；单family、局部sector、单日脚本或spike不得声明FULL_READY/Phase 2完成。
2. **无静默错误/fail-open：PASS**。D3/D4数值失败、D6 semantic不足、coverage偏差和NOT_AVAILABLE分别保留；禁止neutral/index、删sector、隐藏分母、自动fallback或用旧artifact冒充新model version。
3. **无业务逻辑偷换：PASS_USER_APPROVED_SCOPE_CHANGE**。用户明确批准产品主验收改为日期×横截面与三状态；PIT、t-1因果、advisory-only、错误可见性保持不变。市场regime+relative结构必须使用新origin/version，不能静默复用`direct_hmm` parser/hash。
4. **无未经确认门禁/审批：PASS**。本文不设Rank IC、spread、precision/recall、coverage、fold或K的数值阈值；这些精确合同后续由用户确认，但不是runtime人工审批。合入、依赖、DDL/DML和进程控制仍按既有独立权限。
5. **选择与holdout隔离：PASS_DIRECTION_ONLY**。per-sector restart只作为待精确设计的train-only候选；final holdout不可参与模型/family/seed选择，失败不得reselection。
6. **反过度工程：PASS**。只允许一次零refit紧凑聚合与一个spike；不并行A/B/C，不复制大artifact，不建通用模型/evidence/调度平台。K=3 collapse只形成K=2假设，无K=2 fit不得裁决。

正式结论：`PASS_DIRECTION_USER_APPROVED_EXACT_PRODUCT_AND_SPIKE_CONTRACT_PENDING_NOT_IMPLEMENTATION_READY`。F2 validator覆盖3个稳定design items与10个matrix rows；本结论允许文档进入main，不授权fit、selection、model/FULL_READY/COVERAGE_AVAILABLE、database或runtime。

### 23.20 C-011-P2-3-A D1～D6 精确设计正式审核

审核对象为§4.3.4.2、Decision Index六项、Acceptance Matrix与父蓝图v2.25 Gate 2。第一轮发现并修复pooled preprocess
authority不清、三个component共用λ selection、market/sector risk label混用、Newey-West与代表性分组公式不完整、matrix缺少可执行
证据五项缺口；第二轮发现并修复P2-3提前读取P2-4 untouched holdout、fold-3 future label越界、风险聚合层级与metric coverage
分母不明确；第三轮补齐用户批准状态。PR创建后的第四轮独立复审又修复preprocess数值算法/NA row资格、KMeans完整参数、
DP/validation arrival-cost初始化与tie-break、max-iteration失败、compact report schema和直接测试矩阵；最终逐项结果如下：

1. **父蓝图/阶段一致性：PASS_EXACT_DESIGN**。P2-2主导失败只选择一个jump spike；P2-3只使用development folds、完成456 fits与
   唯一候选冻结，不读取holdout。P2-4才首次执行双fresh-process、holdout与D4/D5产品验收，失败不得返回P2-3重选。
2. **无简化/子集伪完成：PASS_EXACT_DESIGN**。market、L1、L2三个component必须全部完成；canonical L1=31、L2=131分母不删除。
   `COVERAGE_AVAILABLE`须通过同一产品指标与代表性合同，不是FULL_READY、spike成功或time-box的别名。
3. **无静默错误/fail-open：PASS_EXACT_DESIGN**。input/fold/preprocess/objective/state/semantic/selection/collision/readback均有稳定
   typed reason；non-finite、空state、metric denominator不足、mapping/quintile证据缺失均fail closed，不补neutral或默认值。
4. **无业务逻辑迁移：PASS_USER_APPROVED_SCOPE_CHANGE**。PIT/t-1/canonical denominator/advisory-only保持；新jump输出使用独立
   `market_relative_jump_v1` identity，禁止复用direct-HMM parser/READY。用户已明确批准autocycle五项、market K2与relative K3，
   legacy历史角色不被删除。
5. **无未经确认门禁/审批：PASS_USER_APPROVED_EXACT_CONTRACT**。用户已明确批准λ、seed、fold、risk label、
   Rank IC/spread/risk/coverage阈值；它们是确定性离线模型合同，不增加runtime人工审批。merge、依赖、DDL/DML和进程控制保持独立授权。
6. **因果与选择隔离：PASS_EXACT_DESIGN**。preprocess/center/restart仅train拟合；development future labels不得越过fold end；semantic
   mapping只由train centroid冻结；holdout identity在candidate冻结前不可读，P2-4失败后不得换λ/seed/feature/K/family。
7. **产品目标与反过度工程：PASS_EXACT_DESIGN**。主指标直接是横截面Rank IC、trending-fading spread与风险precision/recall；只新增
   一个模型模块、直接测试和薄CLI，零新增依赖/表/scheduler/通用平台/完整历史副本。456是pooled fits，不乘sector。
8. **验证：PASS**。`python scripts/aistock_feature_workflow.py validate --design
   docs/architecture/hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md --tier F2`返回
   `PASS / design_items=3 / matrix_rows=11 / warnings=0`；`git diff --check`无blocking error。

用户已整体批准`C-011-P2-3-A/D1～D6`。正式结论更新为
`PASS_EXACT_DESIGN_USER_APPROVED_DOCUMENT_PR_READY_NOT_IMPLEMENTED`；本次只授权文档提交与PR，源码和456-fit spike在文档合入后
继续。本审核不表示fit、selection、holdout读取、model/READY、DB或runtime已经执行。

### 23.21 C-011-P2-3-D3-MARKET-ZERO-EVENT-A 正式设计审核

审核对象为首次formal spike的BUG-1100完整failure receipt、§4.3.4.2 D3/D4、Decision Index和Acceptance Matrix。已确认
market三个fold各有116个10D outcome-eligible dates，风险正例数为`6/0/13`；六个λ均完成market三fold×八restart=`144/144`
fits，且全部只因fold-2的`TP+FN=0`而在原F1分母合同下失去selection资格。lambda/fold evidence canonical hash已由BUG-1100
持久化；selection、holdout、L1/L2、final refit、candidate/model/READY/DB/runtime均未发生。

1. **验收对象和业务标签：PASS_USER_APPROVED**。保留原`-5%` CSI300 market-risk label、三个anchored fold及全部日期；不按已观察结果重切fold、改阈值或删除零事件区间。
2. **无简化/partial：PASS**。三个fold仍全部进入资格与pooled分母，至少两个event-bearing fold是必要条件；zero-event fold不是被跳过的子集，也不允许另一个component代替market。
3. **无静默错误：PASS**。zero-event recall/F1明确保持`null/unavailable`，不定义为0；TP/FP/FN/TN、event/negative count、FPR、specificity、fold分类和canonical hash均持久化。任一分母、count、coverage或finite闭合失败仍typed fail closed。
4. **无业务逻辑漂移：PASS_USER_APPROVED_CONTRACT_CHANGE**。唯一变化是market development λ selection：以三个fold合并confusion counts计算pooled micro-F1与pooled precision-lift，并以zero-event max FPR作第三排序。L1/L2 selection、P2-4产品阈值、hard semantic authority、seed/grid/K/feature和holdout隔离均不变。
5. **无未经确认门禁/审批：PASS**。至少两个event-bearing fold与排序公式已由用户明确批准，是确定性离线模型合同，不增加runtime人工审批、发布确认或研究方向淘汰流程。
6. **版本与历史隔离：PASS**。contract/request/report/market-fold schema升级为v2；旧v1 failure receipt保留历史事实但不得直接产生v2 candidate或READY。该审核当时要求源码合入后重新执行完整456-fit spike；后续执行已在296/456处按合同fail closed，事实与新决策见§23.22，禁止把未运行的L2补写为完成。
7. **反过度工程：PASS**。只修改现有jump spike模块、直接测试和本文；不新增平台、表、scheduler、依赖、完整历史副本或第二模型范式。

历史正式结论：`PASS_EXACT_AMENDMENT_USER_APPROVED_IMPLEMENTATION_READY_NO_MODEL_ACCEPTANCE`。该结论已由后续P2-3A正式失败事实收敛；当前不得再据此启动P2-3A重跑，最新边界见§23.22。

### 23.22 P2-3A 正式失败与 P2-3B 精确候选设计审核

审核对象为父蓝图v2.26、§4.3.4.3、Decision Index、Design Acceptance Index/Matrix与§24优先级。第一轮审核发现daily state
boundary文字会在无tie时也少分一个extreme，以及实施范围未允许为market纯函数复用做最小公开helper调整；两项已修正。首次F2 validator
又拒绝了自造acceptance status和无actionable evidence的matrix行；已改为仓库支持的`APPROVED_BY_USER_DIRECTION_ONLY`/`VERIFIED`
状态，并绑定正式failure artifact、直接测试与本审核receipt。逐项结论如下：

1. **产品目标：PASS_DIRECTION**。P2-3B直接预测10D future relative excess并输出rotation score/forecast state，验收仍是横截面
   Rank IC、spread、risk precision/recall与coverage；不再为通过hidden K=3 occupancy而开发。
2. **禁止简化/子集：PASS**。market、L1、L2必须共同形成一个candidate；P2-3A market局部成功不能复用为产品，P2-3B任何level
   失败均为整体NOT_AVAILABLE。P2-4和P2-5不能在candidate前启动。
3. **禁止静默错误：PASS**。target/score/metric/boundary tie/selection/collision/readback/unknown exception均有typed failure；不补NA、
   不强拆tie、不填neutral、不反转旧label、不以另一level或holdout挽救。
4. **禁止业务逻辑迁移：PASS_EXACT_CONTRACT_USER_APPROVED**。用户已批准“market regime + direct predictor”方向及daily-centered target、
   Ridge参数、alpha grid、20%投影和development正向停止条件；forecast state明确是未来相对走强/走弱预测，不复用hidden-state语义。
5. **禁止未经确认门禁/审批：PASS**。`median Rank IC>0`、`median spread>0`、alpha tie与state boundary是用户批准的确定性离线模型合同，
   不是runtime人工审批；源码、184 fits、P2-4、DDL/DML、依赖和进程控制仍是与本次文档合入分离的状态。
6. **因果与holdout隔离：PASS**。train target不越过segment end，末10日purge；preprocess/Ridge只读train，validation只做development
   alpha selection，P2-3B不读holdout。P2-4失败不得回流调alpha/target/state boundary。
7. **反过度工程：PASS**。唯一建议是一个5-feature pooled Ridge候选；不加interaction、PCA、deep model、t-emission/shared-prior并行、
   通用训练/evidence平台、表或scheduler。计划184 fits且不乘sector。
8. **结果真实性：PASS_IMPLEMENTATION_READY_NOT_IMPLEMENTED**。P2-3A保持NOT_AVAILABLE，P2-3B精确设计已批准但源码未实施，
   canonical model/FULL_READY/COVERAGE_AVAILABLE仍为0；文档完成与F2 PASS不增加`11/17`完成度。

正式审核结论：`PASS_P2_3B_EXACT_DESIGN_USER_APPROVED_IMPLEMENTATION_READY_NOT_IMPLEMENTED`。下一步是按§4.3.4.3 D1～D6实施
最小源码与184-fit spike；本次文档合入不执行源码、fit、holdout、model/READY/DB/runtime。

### 23.23 P2-3B 最小源码正式审核

审核范围严格为`market_relative_ridge_candidate.py`、薄CLI、既有jump模块的market-only公开复用入口、两份直接测试和本设计状态回填；
未执行184-fit、P2-4 holdout、model/READY、数据库或运行时操作。第一轮审核修复了四项真实性缺口：失败路径补齐已完成component
receipt；candidate-level selection与partial component selection分开记录；Ridge输入/系数显式固定little-endian float64并对shape异常typed
fail closed；finalization/runtime-version receipt失败不再丢失主失败证据。修复后复审结论如下：

1. **D1 identity与组件闭合：PASS**。新origin固定`market_relative_ridge_v1`；market只通过单一公开helper复用原K2 jump authority，L1/L2
   为level-local pooled Ridge；market/L1/L2任一失败均不能形成candidate。
2. **D2因果target：PASS**。五项relative输入沿用train-only level-global preprocess；10D future excess按level/date median中心化；每个train
   segment末10日purge；L1 28/L2 118 denominator、benchmark、row/date/sector与tail均进入canonical receipt。缺值只产生typed unavailable，
   不插补、不借用validation/holdout outcome。
3. **D3 selection与停止：PASS**。Ridge全参数、五alpha、三fold、80% metric coverage、Rank IC→spread→larger-alpha tie-break与双指标严格
   正向停止条件逐项实现；所有alpha完成后才选择，不early stop、不per-sector fit、不读取holdout或另一level结果。
4. **D4产品投影：PASS**。连续rotation score使用横截面Rank IC；top/bottom20%按批准q公式投影；`1e-12` boundary tie group不按sector
   code强拆，极端组少于5时整日typed unavailable；spread只消费真实forecast state与target。
5. **D5/D6状态和成本：PASS**。计划严格为market152+L1 16+L2 16=`184`，不乘sector；成功仅形成
   `P2_3B_CANDIDATE_FROZEN_PENDING_P2_4_HOLDOUT_ACCEPTANCE` compact receipt，失败写collision-safe sibling；model/READY/DB/runtime flags
   全为false。184-fit尚未执行，因此不能报告candidate成功。
6. **禁止简化/子集：PASS**。没有market-only、单level、POC、placeholder、mock-only或partial candidate成功路径。
7. **禁止静默错误：PASS**。input/target/fit/score/metric/selection/development/tie/holdout/collision/readback/unknown异常均保留typed stage、
   reason和已完成attempt/component evidence；没有默认neutral、反转label、NA填值或异常吞噬。
8. **禁止业务逻辑迁移：PASS**。源码逐项实现已批准D1～D6，未改变五特征、target、alpha、阈值、状态语义、P2-4边界或P2-3A失败事实。
9. **禁止未经确认门禁/审批：PASS**。只实现用户批准的确定性离线合同；未新增runtime人工审批、研究方向淘汰、依赖、DDL/DML或进程控制。
10. **验证：PASS**。直接矩阵`43 passed`；`hmm_risk_backend`=`494 passed`、branch coverage总计`76.92%`；Ridge新模块覆盖率`80%`；
    ruff、compile、F2、L0、ownership/scope与diff check由本实现PR最终HEAD绑定。

正式结论：`PASS_P2_3B_SOURCE_IMPLEMENTED_VALIDATED_PENDING_184_FIT_NOT_EXECUTED`。源码达到提交/PR技术条件；该结论不授权或伪造
184-fit结果、P2-4、model/READY、DB/runtime，也不增加严格产品完成度`11/17`。
这是2026-08-16源码PR时点的历史结论；正式执行后的现行状态由§23.24覆盖，不得继续将`PENDING_184_FIT`作为当前状态。

### 23.24 P2-3B 正式执行与 POSTRUN-AUDIT-01

审核范围为正式failure receipt、其491个嵌套receipt、批准的D1～D6、producer源码方向、逐日metric重聚合和零副作用边界；不重新训练、
不读取holdout、不改变模型合同。结论如下：

1. **正式身份与输入：PASS**。producer=`24e4ae79780e5bacdf34a3affb63d1db46f6d8a4`，request canonical=
   `f3d9014ba6c1aa59eceda41b148ab97e37bed5f0c05a471128b8dc0f26c471b1`，failure report canonical=
   `d3298654ed9f2080f4623c2c50721ebf9951d2034d42cfdfe225f36e4ee0fc45`，byte SHA-256=
   `8af5ab46eb04340acda4a3fbc4f95cd808631a743c439e0bf0c285d84f846762`。最初DEV前缀下0-fit preflight失败只作非正式启动证据，
   不参与模型结论；正式request的数据集、mapping、PIT和日期身份闭合。
2. **执行完整性：PASS**。market完成152 fits并选择`lambda=4.0/seed=42`；L1完成五alpha×三fold共15 fits，五alpha均eligible，
   按批准顺序选择`alpha=100.0`。总计`167/184` fits；停止后没有执行L1 final refit或L2。所有167项均为`fit_completed`，
   491个嵌套receipt hash、request、attempt、alpha列表、daily metrics和aggregate均重算一致。
3. **数据与覆盖：PASS**。15个L1 fold的Rank IC与spread覆盖均为`116/116`，高于批准最低`93`；target unavailable=0，
   state projection unavailable=0。结果不能归因为日期缺口、provider absence、边界tie或coverage不足。
4. **产品方向与选择：PASS_CONTRACT_FAIL_EFFECT**。alpha100的三foldRank IC分别为
   `-0.007807285873192439`、`0.0867491657397108`、`-0.057080784204671865`，spread分别为
   `0.0009441908663057883`、`0.005458352160960382`、`-0.006434638075431702`；median Rank IC为负、median spread为正。
   target方向、Rank IC、trending-fading spread、alpha选择与双正向停止顺序均与批准合同一致，故失败是跨时期预测关系不稳定，
   不是未来方向反转、优化器异常或执行器BUG。
5. **零副作用与停止：PASS_FAIL_CLOSED**。receipt状态=`NOT_AVAILABLE_FOR_PROMOTION`，reason=
   `hmm_risk_rotation_development_effect_non_positive`；未形成candidate，未访问P2-4 holdout，未写model、FULL_READY、
   COVERAGE_AVAILABLE、DB或runtime。不得用market局部完成、spread局部为正或`167/184`进度伪造产品成功。
6. **DESIGN-COMPLIANCE-001：PASS**。没有简化/子集交付、异常吞噬、业务语义迁移或未经确认的新门禁；也没有自动打开第三模型、
   降阈值、跳过L1/L2或把failure receipt当作READY。严格产品进度保持`11/17=64.71%`。

正式结论：`PASS_EXECUTION_INTEGRITY_FAIL_MODEL_ACCEPTANCE_NOT_AVAILABLE_FOR_PROMOTION`。P2-4保持未进入；后续只允许用户先批准新的
唯一模型合同，或明确停止Phase 2模型方向。

### 23.25 P2-3C 精确合同正式设计审核与用户批准

审核对象为父蓝图v2.28、§4.3.4.4 D1～D6、Decision Index、Design Acceptance Index/Matrix和§24优先级；不审核或授权尚不存在的
P2-3C源码、fit或candidate。第一轮发现并修复两项设计缺口：删除“validation至少一次market transition”这一非必要新门禁；补回固定
lambda4后仍必须执行的既有market fold/product acceptance，避免把“取消重复搜索”误写成“取消market验收”。第二轮又修复新平行
module/CLI/test路径会触发未知runtime分类的问题，把实施scope收敛到已登记offline的既有入口，并补齐market 3/36停止与直接测试矩阵。
修订后结论如下：

1. **目标对齐：PASS_USER_APPROVED_EXACT_CONTRACT**。唯一新增假设是market regime条件斜率，直接对应P2-3B fold-2/fold-3系数近似稳定而产品效果反转；
   输出仍是market regime、sector rotation score和forecast state，不把结构fit当产品成功。
2. **因果与selection：PASS_USER_APPROVED_CAUSAL_CONTRACT**。每fold market只在train拟合，validation只运行causal recursion；sector preprocess和Ridge同样
   train-only。固定lambda4/seed42后保留既有market acceptance；alpha仍只由development folds选择，holdout不可读取。
3. **禁止简化/子集：PASS**。market、L1、L2和三个final fit必须共同闭合；36是去除已闭合重复market搜索后的精确成本，不复制旧path、
   不删除level，也不把18/36或33/36写成candidate。
4. **禁止静默错误：PASS**。两态缺失、identity、interaction非有限、target/metric/selection、collision/readback和unknown异常均typed
   fail closed；regime-split指标只作诊断，不能静默触发reselection。
5. **禁止业务逻辑迁移：PASS_USER_APPROVED_SCOPE_EXPLICIT**。target、五项sector输入、Ridge参数、alpha、产品指标、state projection、
   双正向development gate、P2-4 holdout和三状态语义均不变；唯一变化是显式十维`[x,m*x]`新model identity。
6. **禁止未经确认门禁/审批：PASS_USER_APPROVED_EXACT_CONTRACT**。用户已明确批准D1～D6；两态存在、既有market acceptance和失败后
   停止只在未来获授权的P2-3C实现中生效，不增加runtime人工审批，也不推导本次已有实施授权。
7. **反过度工程：PASS**。只有一个线性候选、一个alpha轴、最多36 fits；不并行PCA/树/deep/ensemble，不建通用训练/evidence平台，
   不新增平行模块/CLI/test、依赖、数据集、DB、scheduler或workflow/catalog修改。P2-3C development失败后停止模型方向的条款已随
   D1～D6由用户明确批准。
8. **结果真实性：PASS_NOT_IMPLEMENTED**。P2-3C源码、tests、fit、selection、candidate、holdout、model/READY均为0；文档提案和F2
   validation不增加严格产品完成度`11/17=64.71%`。
9. **实现验证与runtime边界：PASS_DESIGN_COMPLETE**。直接测试矩阵覆盖3/18/33/36 fit停止、causal state、interaction identity、
   selection隔离、receipt readback和零副作用；实现changed files必须得到`runtime_impact=none`，否则停止并交由独立流程缺陷处理，
   本HMM PR不得自行扩展workflow scope。

正式结论：`PASS_EXACT_DESIGN_USER_APPROVED_IMPLEMENTATION_NOT_AUTHORIZED`。模型合同已获批准并达到文档PR条件；该结论不构成源码实现、
36-fit实验、P2-4、model/READY或运行时授权。

### 23.26 P2-3C 正式执行回填与 P2-4 精确合同提案审核

审核对象为父蓝图v2.29、P2-3C immutable candidate、§4.3.4.5 D1～D6、Decision Index、Design Acceptance Matrix与§24优先级；
本轮没有读取holdout、实现源码或写model/READY。第一轮F2 validator修复了unsupported status、不可验证evidence和未批准gap表达；第二轮
人工审核又修复了development/holdout source identity混用、C-011与历史B3 two-family authority冲突，以及一次性holdout/bitwise复现缺少
false-accept、false-reject和成本说明。修订后结论如下：

1. **上游候选完整性：PASS**。producer=`8ca1b98d…fbd0`，request canonical=`4807125d…6336`，candidate canonical=
   `792d4f6a…17e3`；36/36 fits、6/6 component receipt hash与report readback闭合。L1/L2均选择alpha100，development median Rank IC
   与spread均严格正，且holdout/product/model/READY/DB/runtime flags全部false。
2. **目标对齐：PASS_USER_APPROVED_EXACT_CONTRACT**。P2-4只裁决板块横截面轮动、风险与coverage，不新增结构普查、estimator或研究方向；通过后直接供P2-5
   单日离线预测消费，失败则NOT_AVAILABLE且停止模型方向。
3. **因果与选择隔离：PASS_USER_APPROVED_EXACT_CONTRACT**。candidate参数、preprocess、market centers、semantic mapping与alpha全部冻结；P2-4为0 fit、
   0 selection。holdout outcome只计算离线label/metric，不进入observation/state；两个fresh process属于同一logical evaluation。
4. **禁止简化/子集：PASS_USER_APPROVED_EXACT_CONTRACT**。market、L1、L2、D3产品指标、D4 coverage与representativeness必须共同闭合；FULL_READY、
   COVERAGE_AVAILABLE和NOT_AVAILABLE互斥，coverage状态不得冒充READY。
5. **禁止静默错误：PASS_USER_APPROVED_EXACT_CONTRACT**。preflight、candidate/source/date、causal state、score、metric、coverage、representativeness、fresh-process、
   collision/readback和unknown failure均有独立typed边界；non-finite、分母不足或hash漂移不得补0或降级成功。
6. **禁止业务逻辑迁移：PASS_USER_APPROVED_EXACT_CONTRACT**。P2-3C模型、target、alpha、market参数、state projection与既有D4/D5数值保持不变；5D/20D、
   regime split与历史family只作secondary diagnostics，不参与acceptance或reselection。
7. **禁止未经确认门禁/审批：PASS_USER_APPROVED_EXACT_CONTRACT**。用户已明确批准D1～D6并授权源码实施；它们是确定性离线模型/产品
   合同，不是runtime人工审批。正式holdout读取/执行、model/READY写入与PR合入仍分别授权。
8. **反过度工程：PASS_USER_APPROVED_EXACT_CONTRACT**。最小实现只含一个holdout service、薄CLI和直接测试，fit数为0；不建registry、scheduler、通用平台、
   DB schema或新增依赖，不复制完整历史输入/score/path。

正式结论：`PASS_P2_3C_CANDIDATE_FROZEN_P2_4_EXACT_CONTRACT_USER_APPROVED_SOURCE_IMPLEMENTATION_AUTHORIZED_HOLDOUT_NOT_AUTHORIZED`。

用户已选择B“显式inactive-dimension identity”，A“保持fail-closed”不采用；P1最小必要模型机制源码已实现并通过正式审核，
但不提升F-011完成计数。C-010-A5已合入并完成601日preflight；BUG-962又修复current-A5 authority与历史readback的程序矛盾。
REFIT-01最终仍因current-A5 `train_observation_sha256`与历史frozen train core真实不同而在0 fits处fail closed，状态为
`VERIFIED_ATTEMPTED_INCONCLUSIVE_TRAIN_CORE_DRIFT_ZERO_FITS`。该结果不否定D1-B机制，也不允许把历史payload当current数据。
REFIT-02-A已在双 fresh-process 完成48 attempts/32 fits并暴露旧matched pre-fit缺口；BUG-977随后完成源码修订、PR #3134 merge
`aa3293ae…`、close-sync PR #3135 merge `9aba2752…`与清理。REFIT-02-B已在producer `9aba2752…`完成双fresh-process
48 attempts/48 fits，canonical=`3a4de927…36cf`、payload bitwise equal；19D treatment与positive harness均16/16 `fit_completed`且descriptive covariance accepted，matched current-A5
20D在seeds42..49的两次运行中均于covariance stage失败。由于该失败不是initialization blocker且matched fit未完成，真实结论继续为
`diagnostic_failed/inconclusive`，D5 readiness=false。历史v6只保存typed stage/reason，未保存raw covariance exact cell/shape/bit-pattern；
`C-008-B3-D1-REFIT-03-COVARIANCE-DIAG-01`已完成源码合入、冻结bundle与两fresh-process真实`48/48` fits。producer为
`b474170fd58a466959e595ce7d245bae7da88ab8`，report canonical=`7e8a1755…76b9`，两进程payload=`53574f62…088f` bitwise equal；
treatment/harness均16/16 `fit_completed`且descriptive covariance accepted，matched 16/16 covariance failed，raw pattern为三次inactive-only与五次active/cross-role，故
`diagnostic_complete/mixed_seed_pattern/mechanism_assessment=inconclusive`、D5 readiness=false。该结果不批准feature删除、阈值变化、
formal D4 acceptance、D5 selection或READY。

`C-008-B3-D1-POST-REFIT03-A`与`C-008-B3-REMEDIATION-D1-D5-COMPAT-01-A`已获批准，且P5正式
mixed-dimension artifact/parser、dimension receipt与公式/identity/131-entry完整性的确定性直接测试已完成源码实现和本模块审核。
最新 P6 已在 producer `0ab6dec3` 完成`autocycle_all_core:L2`双 fresh-process `2096/2096` fits；D5按train-only
合同选定seed43。历史BUG-1008修复后的首次零refit replay曾有6个calendar availability blocker；该结果已被后续D6-NA-A与BUG-1029 replay取代，只保留为历史诊断来源，不是当前任务状态。

`C-008-B3-D6-NA-A`已由PR #3258合入；BUG-1029又修复carrier-v2严格empty legacy sentinel与冻结producer zero-refit readback。最新零refit D6 report SHA-256=`dcf4c69ec7ba817d8d19f8cca27f6a855f25b2e7d147a5b754549d431d8c26a1`，`fits=0`、`refit_count=0`、`selection_reexecuted=false`、seed43与模型hash不变；assignment 131/131 accepted，evidence 120/131 accepted、11/131 failed。11个失败为：`801038.SI`、`801127.SI`、`801204.SI`、`801223.SI`、`801231.SI`、`801711.SI`、`801723.SI`、`801733.SI`、`801738.SI`、`801743.SI`、`801971.SI`。

`C-008-B3-TRAIN-STABILITY-DIAG-01`已在producer `7d57d57e…d190`完成：1048/1048 frozen model profiles、131/131
source comparisons、0 refit/selection/D6/model/READY/DB/runtime，canonical=`9c449e04…c5b1`。8个seed的双窗口stable sector数
为`108/108/97/103/109/105/104/106`，完整seed为0；因此D5 stability gate单独使用会清空candidate set。`TRANSITION-DWELL-B`
精确合同、源码、BUG-1068 receipt lifecycle修复及受控实验均已完成：treatment producer `29417ceb…f8996fe`、冻结source
`2ae9df85…be7fa`，双fresh-process完成2096/2096 fits，entry/model/profile payload hashes bitwise一致；完整候选seed仍为0，
状态=`diagnostic_complete_no_complete_candidate`，parent body canonical=`b6312171…582db`、完整对象canonical=`e5f355fc…d4b54`。
未执行selection/D5/D6，未生成model/READY且无DB/runtime动作。

### 23.27 P2-4 正式结果、蓝图可实现性与 C-012 修订审核

1. **实时状态：PASS_CORRECTED**。P2-4已正式执行且状态为`NOT_AVAILABLE`；acceptance canonical=`16004b24…7c87`，0 fit/0 selection、
   双fresh-process一致，model/READY/DB/runtime均false。顶层、Feature Card、Decision Index、Acceptance Matrix与§24优先级已同步，不再保留
   “holdout未授权/未执行”的active状态。
2. **不可追认边界：PASS**。L1 directional通过、L2 Rank IC显著与coverage局部通过只作能力分解证据，不把旧结果追认为
   CAPABILITY_AVAILABLE；L1 risk失败、L2 spread/季度coverage/risk identity失败均保留。旧holdout已消费，禁止调参后重用。
3. **可实现性：PASS_NO_IMPOSSIBLE_PROMISE**。最终板块轮动预测、风险预警、历史分析与API/UI仍可实现；蓝图不再承诺P2-3C或任何单一
   estimator必然同时通过四项能力，也不把FULL_READY写成保证结果。合法终态包括NOT_AVAILABLE。
4. **架构闭环：PASS_DIRECTION_COMPLETE**。canonical bundle与四能力、能力coverage/abstention、顶层三状态、risk四类identity、新holdout治理、
   P2-5最小oracle和单候选停止条件均已进入§4.3.4.6；运行职责从F-011模型前置移至P2-7。
5. **禁止简化交付：PASS**。CAPABILITY_AVAILABLE必须点名能力并显示所有未通过项，不等于FULL_READY或Phase 2完成；没有rotation-only、
   L1-only、backend-only或空UI被宣称完整交付。
6. **禁止静默错误：PASS**。不缩risk分母、不inner-join隐藏identity、不补neutral/negative、不隐藏abstention或未通过能力；旧结果保持fail closed。
7. **禁止业务逻辑迁移：PASS_USER_AUTHORIZED_BLUEPRINT_REVISION**。用户明确要求按可达目标修订蓝图；PIT、因果、advisory-only、真实产品指标
   与失败可见性不变。精确component/model/阈值尚未批准，不由本文自行选择。
8. **禁止未经确认门禁/审批：PASS**。本修订拆除错误的四能力总耦合，不降低旧阈值，也不新增runtime人工审批；下一精确F2合同是既有
   FEATURE-WORKFLOW-001设计阶段，不是新发布门禁。
9. **反过度工程：PASS**。下一任务一次只处理一个明确能力假设；禁止通用平台、历史artifact迁移、重复完整物化与无界诊断。

正式结论：`PASS_P2_4_TERMINAL_RESULT_BLUEPRINT_CAPABILITY_REALIGNED_EXACT_F2_CONTRACT_PENDING_NO_IMPLEMENTATION_AUTHORIZED`。

### 23.28 C-012-RL1 D1～D6 精确合同正式审核

审核仅覆盖§4.3.4.7、Decision Index、Acceptance Matrix与§24优先级；没有读取新holdout、运行fit、选择参数、写candidate/component/bundle、
修改源码、数据库或runtime。第一轮发现并修复fold label purge/segment边界、新source revision、writer hash cycle和阈值依据四项缺口；第二轮补齐
稳定schema/contract/algorithm identity、typed reason code与runtime classification。最终审核如下：

1. **目标与范围：PASS_EXACT_CONTRACT_COMPLETE**。唯一目标为rotation_L1，不把risk或L2伪造成已实现，也不把单能力候选命名为FULL_READY；其他三能力
   继续typed NOT_AVAILABLE。该分解来自父蓝图v2.31已批准方向，D1～D6精确值已获用户批准，源码和实验仍未授权。
2. **模型与因果：PASS**。market K2 lambda4/seed42、Ridge alpha100、十维交互、10D centered target与score/state方向全部固定；五fold与final fit
   都从本fold/full-development重新拟合，不复制旧centers/score，不使用validation/holdout fit或future state input。
3. **development验收：PASS_EXACT**。五fold日期、purge=10、embargo=0、80% coverage、4/5双正向、median IC/spread、拼接OOF NW t-stat与lag均给出
   精确值；所有fold完整执行，无alpha/seed/grid选择或early stop挑正向时期。
4. **新holdout：PASS_EXACT_BUT_FUTURE_DATA_PENDING**。窗口固定2026-04-01..2026-09-30及10-open-day outcome tail；旧source截止2026-06-30
   明确不合格。完整tail/source identity冻结前禁止业务读取；读取后失败不得缩窗、延窗、reselection或打开第二candidate。
5. **阈值与取舍：PASS_EXPLICIT**。IC .02、spread .003、one-sided NW 1.645、双季度正向均记录统计/经济依据和false-accept/false-reject；这些是
   已批准产品合同，不从旧holdout追认成功，也不静默降低旧P2-4结果。
6. **coverage与状态：PASS**。31×date完整分母、sector最低coverage、C-010 input validity、abstention与四类identity均保留；只有产品与coverage同时
   通过才形成rotation_L1 AVAILABLE和顶层CAPABILITY_AVAILABLE，ready=false且不写FULL_READY marker。
7. **artifact与失败：PASS**。stable schema、acceptance core、component/bundle/final双向闭合、collision/readback和unknown failure均typed；失败不写
   model/bundle/READY，不以partial artifact冒充成功。
8. **成本与反过度工程：PASS**。两个fresh process合计24 fits、holdout 0 fit；只复用两个既有service/CLI/test边界，无新依赖、DB、registry、
   scheduler、通用平台或历史artifact迁移。失败即停止该能力，不自动进入下一方向。
9. **DESIGN-COMPLIANCE-001：PASS**。无简化交付、静默错误、业务逻辑迁移或未经确认的门禁/审批；精确合同已获用户批准但源码与实验仍保持
   `IMPLEMENTATION_NOT_AUTHORIZED`，该状态是权限边界而非新增runtime人工审批。

用户于2026-08-23批准`C-012-RL1-D1～D6`精确合同；该批准只把上述确定性公式、阈值、窗口、状态和停止条件变为实现权威，不授权源码、24 fits、
新holdout读取、model/bundle、API/UI、数据库或runtime，也不改变本审核关于完整性和反过度工程的结论。

正式结论：`PASS_C012_ROTATION_L1_EXACT_DESIGN_USER_APPROVED_IMPLEMENTATION_NOT_AUTHORIZED`。

### 23.29 C-012-RL1 源码实施正式审核

用户随后独立授权D1～D6源码实施，但未授权24 fits、新holdout读取、component/bundle生成、数据库或runtime动作。本次实施严格复用既有
Ridge candidate与holdout两个service/CLI/test边界，没有新增依赖、registry、scheduler、数据库schema、通用平台或历史artifact迁移。

1. **D1/D2 candidate：PASS**。固定K2/lambda4/seed42、Ridge alpha100、十维交互、10D centered target、五个anchored folds、purge、
   4/5双正向、median与拼接OOF NW合同均为显式常量和typed failure；每fresh process只有12 fits，双进程closure严格闭合24 fits且不执行selection。
2. **D3 holdout：PASS_SOURCE_ONLY**。源码固定`2026-04-01..2026-09-30`及其后10个canonical open-day tail；development与holdout source revision
   必须不同，candidate/source/date/formula/security/provider/C-010稳定policy逐项闭合。当前未读取该窗口，故没有产品验收结论。
3. **D4产品与coverage：PASS_SOURCE_ONLY**。Rank IC、spread、NW、Q2/Q3及31×date分母均直接实现；prediction-only、outcome-only、
   both-unavailable与abstention保留count/hash。metric和coverage互不补足，未通过时保持`NOT_AVAILABLE`。
4. **D5 writer/readback：PASS**。development和holdout request均冻结全部输出路径；parent/child在数据读取前验证路径权威。candidate、component、
   bundle、acceptance与failure使用canonical JSON、append-only collision和磁盘回读；业务schema、capability、side effect与双向hash同时校验，不能用
   自哈希额外字段或伪造`CAPABILITY_AVAILABLE`绕过。
5. **D6复现与停止：PASS_SOURCE_ONLY**。两个development child必须bitwise payload一致；两个holdout child为0 fit/0 selection且payload一致；
   `rotation_L2|risk_L1|risk_L2`始终typed `NOT_AVAILABLE`，任何失败都不打开第二candidate或其他能力。
6. **DESIGN-COMPLIANCE-001：REVIEWED**。实现覆盖已批准D1～D6全范围；异常、缺失与不可用状态全部typed且fail closed；不复用旧holdout，
   不改变业务语义，不增加人工审批或runtime门禁。源码完成不推导fit成功、产品成功、CAPABILITY_AVAILABLE或READY。

正式结论：`PASS_C012_ROTATION_L1_SOURCE_IMPLEMENTED_VERIFIED_PENDING_FORMAL_24_FIT_AND_NEW_HOLDOUT`。

### 23.30 C-012-RL1-HR1 历史因果回放修订正式审核

审核对象为父蓝图v2.32、§4.3.4.8 HR1 D1～D6、Decision Index、Design Acceptance Matrix与§24唯一优先级。用户授权边界是：长期验证必须通过历史回放实现，
文档和源码经多轮审核、CI全绿后允许直接合入；正式24-fit回放可在最终merge commit上后台启动并由用户手工触发后续检查。

1. **目标对齐：PASS**。HR1直接消除“等待未来日期阻断板块轮动预测”的不可接受路径；通过同一五fold逐日回放形成可被P2-5消费的真实能力，不把receipt、平台或历史归档作为交付目标。
2. **模型与阈值：PASS_NO_DRIFT**。算法、输入、target、alpha、lambda、seed、fold、purge、4/5、median、NW和coverage数值全部保持§4.3.4.7批准值；没有放宽门禁或新增候选。
3. **因果与selection隔离：PASS**。每fold只用过去数据拟合，validation逐日t-1/PIT，future outcome只作事后metric；selection/search均为false。历史数据曾参与研究的事实通过`validation_basis`明确披露，不伪造untouched。
4. **能力状态：PASS**。CAPABILITY_AVAILABLE、coverage、validation basis、forward confirmation与ready五个维度分离；其余三能力显式NOT_AVAILABLE，rotation_L1不能冒充FULL_READY或risk。
5. **forward confirmation：PASS_NON_BLOCKING**。PENDING不阻断advisory-only P2-5/P2-6；未来FAILED停止新日常预测但保留历史分析，禁止重fit/reselect/延窗。这是确定性状态合同，不是新增人工审批。
6. **artifact边界：PASS_MINIMAL**。只增加replay request、replay child、replay acceptance、component v2和bundle v2五个必要schema，复用既有service/CLI/test；不复制完整输入、不迁移旧artifact、不建registry/DB/scheduler/evidence平台。
7. **错误与复现：PASS_FAIL_CLOSED**。24 fits与双fresh-process bitwise closure不变；指标、coverage、writer、readback或复现失败均typed停止，模型失败不伪装程序BUG，程序缺陷不伪装模型失败。
8. **DESIGN-COMPLIANCE-001：PASS**。批准范围全部保留，错误与不可用状态完整外显，模型及产品语义没有漂移，没有增加设计之外的门禁或审批；历史回放basis和forward pending必须在产品层可见。

正式结论：`PASS_C012_RL1_HR1_EXACT_CONTRACT_READY_FOR_SOURCE_IMPLEMENTATION`。

### 24.1 当前唯一任务优先级（父蓝图v2.36 Gate 2）

P2-1～P2-4、历史B3、C-011与旧诊断继续作为只读事实索引，不再是待执行阶段。当前任务列表只保留三个端到端业务闭环：

1. **G2-A / P0 输入权威到首个真实L1轮动产品（当前唯一任务）**：C-013输入闭包和601日预检已经完成；HR1 expanding-window正式执行已在10/24 fits后因经济验收失败而终止为`ROTATION_L1_NOT_AVAILABLE`。用户已精确批准§4.3.4.9 RW1 D1～D6：只实施一个252日fixed rolling-Ridge与pre-frozen historical eligibility候选，market、feature、target、alpha、seed、fold、经济阈值和24-fit fresh-process合同不变。当前完成源码与测试并停在待合入；正式24-fit另行授权。RW1若`rotation_L1=AVAILABLE`，同一闭环立即继续真实单历史交易日prediction、最小repository、`overview`/`heatmap` read API、真实`/hmm-risk` L1热力图和无mock浏览器验收；失败则终止rotation_L1模型方向。不得重跑HR1、自动搜索window、降低阈值、删除fold或打开第二candidate。
2. **G2-B / P1 首个产品到扩展分析与预警（blocked by G2-A）**：在G2-A同一真实纵切上扩展最近7个及已批准更长历史、transition/severity、预警时序、横截面与命中/误报/漏报指标、稳定性、固定详情和后续已验收的L2/risk能力。历史分析、API/UI扩展和浏览器验收不得独立成为阶段；未通过能力继续typed `NOT_AVAILABLE`，不得以rotation state伪造risk warning或隐藏失败。
3. **G2-C / P2 真实产品到受控日任务（blocked by G2-B）**：在同一已验收产品identity上完成共同水位、幂等日任务、revision/dedupe、late-data、受控runner、失败恢复及跨层集成验收。不得提前建设通用调度器，Phase 3调度仍不属于本阶段。

**同一闭环内的执行规则**：小型设计补充、测试、程序BUG、审核修复、状态回填和直接性能修复必须随当前任务收敛，不得独立设计为阶段。只有模型合同变化、生产DDL/DML/依赖、无法由一个owner安全修改的模块边界、或超出当前allowed scope的独立缺陷才允许拆分；拆分前必须说明不拆分为何无法完成F-011/F-012/F-013。代码merge、实验、用户重启、runtime verify是独立授权状态，但不是产品阶段，也不增加完成度。

停止项：复用已消费holdout、把历史回放冒充untouched、重跑旧HR1 request、window/alpha/seed grid、删除失败fold、四能力并行搜索、保证指定estimator成功、新的通用evidence/训练/调度平台、重复完整输入物化、历史artifact迁移/清理、为同一闭环反复建立小文档/小PR/小阶段、与F-011/F-012/F-013无直接关系的基础设施，以及用diagnostic/receipt数量增加完成度。当前DDL/DML、依赖、数据库、model/READY与客户端同步均为`noop`；HMM Phase 2新runtime仍未激活；严格进度保持`11/17=64.71%`。

### 24.2 BUG-982：REFIT-03 冻结输入可回放合同（历史已闭合）

REFIT-03 首次真实执行暴露了一个独立的可复现性缺陷：C-010-A5 artifact 冻结了
`mapping_manifest_sha256`，但没有保存可回放的映射行或 D1 角色训练矩阵；runner 的父进程与两个 fresh child 又分别读取
可变 PostgreSQL PIT 表。`sw_index_member` / `sw_index_classify` 没有可用于恢复旧版本的 mutation timestamp，因而历史修订后只能
检测 hash drift，不能重建已批准输入。该失败在 0 fits 处 fail closed；不得把当前数据库、DEV 数据库、旧模型参数或 role-local hash
静默替代为已批准 C-010-A5 权威。

BUG-982 的修复合同如下：

- REFIT-03 在任何 child 启动前，由父进程生成一个 repo-external、append-only 的
  `hmm_risk_c008_b3_d1_frozen_input_bundle_v1`；只有实时读取仍同时通过既有 C-010-A5 report、mapping、policy、role manifest 与
  historical-reference 检查时才允许首次写入。
- bundle 必须保存 treatment 与 harness 的 exact little-endian float64 C-order bytes、shape、byte hash、dates、PIT constituents、
  observation/constituent/train-input manifests、preprocess、lineage migration receipt、current authority 与 historical reference hash；
  canonical writer、readback 和 collision 必须 fail closed。
- 两个 fresh child 只能读取同一个 bundle，不得重新访问数据库。已有 bundle 的 replay 必须显式提供 canonical SHA-256；缺失 hash、
  payload/hash/manifest/authority 任一不一致、重复 JSON key、路径越出 explicit artifact root 均在首个 fit 前失败。
- bundle 是已批准输入的持久化载体，不是新的数据权威、threshold、selection 或 model artifact。它不得改变 hard semantic authority，
  不得执行 D5/D6，不得生成 model/READY，也不得执行数据库或 runtime 动作。
- 本次历史 C-010-A5 mapping 已发生不可逆 source drift，且旧行未被任何现存 artifact 保存。因此 BUG-982 源码修复只能防止未来重复
  丢失并允许已存在 bundle 的确定性 replay；它不能伪造旧 bundle。继续 48-fit REFIT-03 的唯一合法前置是：提供真实旧冻结输入，或由
  用户另行批准一个基于当前只读 preflight 的新 authority revision。该决定不由实现自动作出。
- 用户已选择后一条路径。当前 `main@c6b6300b79deba04a3fd76cd9caab34689c8559d` 已按同一601日、同一
  security/provider-absence/train-coverage 合同完成新的只读 C-010-A5 preflight：canonical report
  `cd70e597519b583c848928aca41d73e9acfecf0c83340a8d35ec6c0a90d3fee5`、partition
  `9282e05ba235449c315e11e9ed324e52c8c73dc3a25547ae4157544aaab12d93`、mapping
  `acb38f303e5b9c7447fcae8e65ea23fe58615da67da762f51f03caa862682ab9`，结果仍为 `P_all/P_in/P_out=502/501/1`；
  fit、selection、D6、model/READY、数据库和runtime flags均为false。该revision表示“从当前PIT事实启动新的可复现实验权威”，不是
  对历史缺口的修复、回填或覆盖。
- 新 authority 使用 versioned v2 envelope 及 v8 process/report readback；旧 `e7f7edc9…773d` / `03d78534…ead6` /
  `6ed16f4e…d696` 与既有 v1/v7 artifact 继续按原authority只读解析。writer不得把新mapping写入旧schema，也不得把旧artifact迁移或重签。

当前状态：`BUG_982_FIXED_CLOSE_SYNCED_CURRENT_AUTHORITY_BUNDLE_CREATED_REFIT_03_EXECUTED`。原 REFIT-03 三份0-fit failure receipt继续
immutable；producer `b474170f…` 已使用current authority创建append-only bundle并完成真实48-fit诊断。该执行没有回填或改写历史PIT数据，
也没有selection、D6、model/READY、数据库或runtime动作；bundle与report继续作为repo-external只读证据。

### 23.31 C-012-RL1-HR1 源码正式审核

审核结论：`PASS_SOURCE_IMPLEMENTED_VERIFIED_PENDING_FORMAL_REPLAY`。本结论只覆盖源码与直接验证，不代表24-fit回放已经通过。

1. **无模型/阈值漂移**：market K2、jump penalty 4、seed 42、Ridge alpha 100、十维输入、五fold、product/coverage阈值与24-fit预算均未改变；selection/search/holdout access仍为false。
2. **request与输出authority闭合**：唯一CLI新增`--prepare-request`，从read-only source authority生成request；request冻结dataset/mapping/database/C-010 identity、artifact root及六个输出路径，任一漂移在数据库读取或fit前失败。
3. **coverage分母完整**：每fold以31×全部validation state dates为分母，保留daily/sector coverage、prediction-only、outcome-only、both-unavailable及abstention count/hash；metric与coverage分别验收，互不补足。
4. **artifact双向闭合**：acceptance core、component v2、bundle v2、final acceptance按固定顺序append-only写入并从磁盘回读；四层schema均限制精确字段集，额外自哈希字段、capability/forward/ready漂移和SHA不一致均fail closed。
5. **部分写入不静默**：finalization失败会在typed failure receipt中如实记录已写core/model/bundle；未形成final acceptance的partial artifact不可消费。
6. **旧holdout不可重解释**：pre-HR1 holdout reader显式固定旧contract/report/component/payload authority；HR1新schema不会回写或重新解释已终结P2-4 artifact。
7. **DESIGN-COMPLIANCE-001**：没有subset/POC、neutral/fallback、第二candidate、历史artifact迁移、通用registry、数据库写入、runtime动作或新增人工门禁。rotation_L1通过仍只形成`CAPABILITY_AVAILABLE`且`ready=false`，其余三能力保持`NOT_AVAILABLE`。

### 23.32 BUG-1167：HR1 当前 PIT 与行业身份权威兼容性审核

HR1 首次正式 request preflight 在 0 fits 处正确停止，但暴露两个源码缺陷：CLI 只接受已过时的 C-010 source envelope，且 typed failure receipt
没有保存安全的底层错误消息。改为显式验证 tracked canonical PIT v2 source receipt 后，第二次 0-fit smoke 又证明 DEV
`market.sw_index_classify` 为空；当时的源码验证仅证明 `market.sw_index_member` 能闭合 31 个 canonical L1、131 个 L2 catalog，
不能证明每个 symbol/date 的历史成员区间完整。BUG-1184 的真实只读回放已推翻“当前 member rows 等同完整历史 PIT 区间”这一过宽结论；
后续以 §23.35 的版本化成员身份合同为准。

1. **权威没有降级或 fallback**：HMM mapping 的明确权威改为 `sw_index_member` member closure；不是“classify 为空才换表”，也不自动读取 latest。
   canonical L1 必须匹配 `801xxx.SI`，每个 L2 必须恰有一个 canonical L1 owner，历史 industry alias 跨共享 L2 必须唯一指向同一 owner。
2. **完整性继续 fail closed**：L1/L2 必须严格 31/131，canonical code/name 非空且无冲突；alias 一对多、L2 多 owner、名称冲突、空 catalog 或 active mapping
   多 identity 均失败。不得使用字符串截断、首行、`DISTINCT ON`、neutral 或当前成员关系回填历史日。
3. **所有正式读取路径一致**：mapping manifest、L1/L2 stock fact、missing-price 与 separated/alias-aware 路径复用同一 member-derived SQL closure，
   不允许 request preflight 与实际训练读取使用不同 identity authority。
4. **实验边界不变**：BUG 修复不修改 HR1 estimator、特征、fold、阈值、seed、24-fit、selection 或 capability 语义；不执行数据库写入、模型/READY
   写入或 runtime 动作。修复后的 request preflight 必须先闭合当前 PIT receipt 与 member catalog，才允许用户已授权的正式 24-fit replay 启动。
5. **审核结论**：`PASS_BUG_1167_MEMBER_CLASSIFICATION_AUTHORITY_FAIL_CLOSED_SOURCE_VALIDATED`。DEV只读核验得到218条 distinct member catalog rows、
   31个canonical L1、131个canonical L2、190个canonical/alias lookup entries，并成功读取首条2022-01-04 PIT mapping。该结论只说明源码合同正确，不能推导
   replay、模型或产品能力成功。

### 23.33 BUG-1169：canonical PIT v2 provider-absence 权威闭合

HR1 在生产只读历史源与 `aistock_equity_pit_canonical_v2` 上执行 request preflight 时，于 0 fits 处对
`002366.SZ/2021-09-23` 正确返回 `hmm_risk_stock_fact_provider_absence_unverified`。根因不是本地价格、PIT 身份或
moneyflow 写入缺失，而是 active `hmm_risk_provider_absence_manifest_v1` 仍冻结在旧的 502 个 exact key，未覆盖
canonical PIT v2 当前完整候选集。

1. **精确权威闭合**：生产只读候选集为 563 个 exact key；旧权威 502 个，新增 61 个，分布于 40 个交易日。新增 key
   已逐日按 Tushare `moneyflow` full-market authority 核验为 61/61 provider absent、0 present；manifest v2 保存旧 receipt
   hash、supplement key hash、计数与 `db_writes=false`，每行继续绑定同一新 receipt hash 和 row hash。
2. **fail-closed 语义不变**：只有 manifest 中 `(canonical_ts_code, source_dataset, source_ts_code, trade_date)` 完全匹配的
   key 才可形成 provider-absence NA evidence；未知 key、identity 漂移、receipt/hash/count/order 漂移仍失败。不得填 0、前值、
   neutral、行业代理或 synthetic row。
3. **业务与实验合同不变**：本修复只更新 active 输入权威，不修改 HR1 estimator、特征、fold、阈值、seed、24-fit、selection、
   capability 或 READY 语义；不执行 DDL/DML、依赖安装、runtime activation 或服务启停。
4. **完成边界**：manifest/parser/定向测试通过只证明 source preflight 可继续；不推导 24-fit replay、模型验收或板块轮动产品能力成功。

### 23.34 BUG-1170：C-010 contributor ledger 与 stock-fact 行业权威统一

BUG-1169 修复后的 HR1 生产只读 request preflight 已越过全部 563 个 provider-absence exact key，但在 0 fits 处对
`002505.SZ/2022-01-04` 返回 `hmm_risk_c010_contributor_receipt_mismatch`。该日证券仍在 canonical PIT universe 与
SW member authority 中但没有 raw price，因此由 missing-price 路径进入完整 stock-fact denominator；C-010 所谓 full-universe
expected-opportunity ledger 却从 `market.kline_daily_raw` 起表，并另行 join `market.sw_index_classify`，同时漏掉无价格机会和偏离
BUG-1167 后统一的 `market.sw_index_member` canonical 31-L1/131-L2 owner closure。

1. **真正 full-universe ledger**：C-010 opportunity query 必须从 trading calendar × canonical PIT spans 构造全部预期
   symbol/date，不得依赖 raw price 存在；missing-price 行必须能在 ledger 中闭合，而不是被误判为未知 contributor。
2. **单一行业权威**：opportunity query 与 provider-absence domain partition 必须复用 `stock_fact_repository` 的
   member-classification CTE，按 L2 的唯一 canonical L1 owner 解析 `l1_code/l2_code`；不得继续读取
   `sw_index_classify`、字符串截断、首行或 latest mapping。
3. **完整性不放宽**：canonical L1=31、L2=131、L2 owner 唯一、成员区间覆盖和单日 mapping 唯一仍 fail closed；修复只消除
   authority 分叉，不允许缺 contributor、重复 mapping 或非法 identity 被视为可用。
4. **实验合同不变**：不修改 HR1 estimator、特征、fold、阈值、seed、24-fit、selection、capability 或 READY 语义；不执行
   DDL/DML、依赖安装、runtime activation 或服务启停。修复后必须重新执行完整 request preparation，且只有 0-fit request
   成功生成后才可启动正式 24-fit replay。

### 23.35 BUG-1184：申万成员版本边界、唯一行业身份与跨消费者迁移详细设计

#### 23.35.1 结论、事实边界与非目标

BUG-1184 的直接失败为 `300741.SZ/2020-07-30` 同时解析到 `801030.SI/801034.SI` 与
`801120.SI/801124.SI`，HR1 在 0/24 fits 处以 `hmm_risk_rotation_l1_input_identity_mismatch` 正确
fail closed。只读核验同时确认：canonical universe、交易日与价格行均唯一，冲突来自行业成员关系，不是停牌、证券代码变更、
价格缺失、HMM estimator 或验收阈值。

当前 `market.sw_index_member` 共 5,587 行，全部为 `is_new='Y'`、`out_date=NULL`；Tushare
`index_member_all(ts_code=..., is_new='Y')` 对冲突证券返回相同多身份。它是当前成员集合，不足以单独证明历史有效区间。
在 HR1 的 `2020-07-30..2026-03-31` 读取范围内共有 23 只证券、23,326 个 symbol-day 多身份，约占
5,999,301 个 canonical opportunity symbol-day 的 0.389%；其中 19 只能按不同 `in_date` 形成顺序候选，另有
`300741.SZ`、`300858.SZ`、`603020.SH`、`605077.SH` 四只在同一源生效日仍有不同身份，不能用排序解决。

新增本地申万2021资料包只作为版本化数据candidate与设计证据，未经P1 writer/readback闭合不得直接成为运行时数据源：

- `SwClassCode_2021.xls` SHA-256=`923492f4bcf3c7056904385a0769e4dda561904a29ecd9243f942680cef68c81`，
  仅含511行申万2021行业代码目录，不含股票或日期；
- `StockClassifyUse_stock.xls` SHA-256=`15979d9cf8a3b83ccc8dadc967de52f35e667b4f4da5e4e4e3dd5a8bb1f17402`，
  含11,803行、5,161只股票的`股票代码/计入日期/行业代码/更新日期`历史；3,811只股票有多条顺序记录，
  但exact duplicate、同股票同计入日期多identity和同股票/行业/计入日期重复均为0。因此“重复股票”表示历史分类变化，不是可直接删除的脏行；
- `最新个股申万行业分类(完整版-截至7月末).xlsx`
  SHA-256=`b242ab04e0f68357cf90772e3f15367644d3e74c08a767eb9c5edcf21467fcbb`，已在2021年7月末快照中把四只股票归入
  `220315/基础化工-化学制品-食品及饲料添加剂`；
- `SwClassStd2021.pdf` SHA-256=`18fb07fafda072dad39e274371660706e21678045ae8204931958db9906faa1a`，
  明确申万2021分类于2021-07-30正式推出、2021-07-31在指数网站公告，配套行业指数调整时间另行确定。

`StockClassifyUse_stock.xls` 中四只股票的新分类`计入日期`均为2021-07-30；`300741/300858/603020`的更新日期为
2021-07-31，`605077`为2022-08-21。后者在2021年7月末快照中已经存在，直接证明`更新日期`不是首次可知日期或成员生效日。
全表另有5,029行在2015-10-27统一更新但计入日期横跨1984-05-09至2015-10-26，且5行更新时间早于计入日；因此
`更新日期`只能作为`source_last_updated_at` lineage，严禁用作`valid_from`、`valid_to`或`known_from`。

公开资料继续只作独立交叉证据：Tushare `index_member_all` 的字段与 `is_new` 语义见
<https://tushare.pro/document/2?doc_id=335>；新浪相关资料页对
[`300741.SZ`](https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpXiangGuan/stockid/sz300741.phtml)、
[`300858.SZ`](https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpXiangGuan/stockid/sz300858.phtml) 与
[`603020.SH`](https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpXiangGuan/stockid/sh603020.phtml)
明确显示旧申万行业指数成员关系在2021-12-13结束、随后进入基础化工指数；研究资料亦明确自2021-12-13起行业指数采用申万2021版。
`605077.SH` 的公开资料也同时证明旧申万“食品综合”和申万 2021“基础化工/化学制品/食品及饲料添加剂”两种口径，
参见[新浪旧申万板块](https://money.finance.sina.com.cn/corp/go.php/vCI_CorpOtherInfo/stockid/605077/menu_num/0.phtml)及
[公开发行材料中的申万2021分类](https://disc.static.szse.cn/disc/disk03/finalpage/2022-09-22/3c4e2c60-d5b5-4c36-b1e3-79ed711b89bb.PDF)；
本地分类历史足以确定其分类记录于2021-07-30变更，但仍不得把该日期冒充申万行业指数成员切换日，也禁止把四只证券写成源码例外。

本设计不修改 HMM 模型、特征、fold、threshold、seed、hard semantic authority、24-fit 预算、selection、capability 或 READY
语义；不回写旧实验、CAS、模型或历史 artifact；不执行 DDL/DML、依赖安装、部署、runtime activation 或进程控制。

#### 23.35.2 唯一权威对象与解析合同（C-013-PIT-ID-D1～D3）

1. **双 authority 必须分离**：`stock_industry_classification_pit` 表示股票被归入行业分类的历史；
   `sw_industry_index_membership_pit` 表示股票进入/退出申万行业指数的历史。两者分别持久化
   `authority_type/schema/version/source/receipt/hash`，不得以相同的`in_date/out_date`字段混存，也不得因L1/L2名称相同而互相替代。
2. **分类 PIT 的日期合同**：`StockClassifyUse_stock.xls.计入日期`是候选`classification_valid_from`；同一canonical symbol按
   严格递增计入日期排序，上一identity的`classification_valid_to_exclusive=next_distinct_valid_from`。实际可用于股票事实的区间必须再与
   `stock_universe_pit_spans`相交，因此上市前已完成分类不会制造可交易股票。`更新日期`只写`source_last_updated_at`，不参与区间或因果选择。
3. **知识时间必须独立**：`classification_valid_from`不等于`known_from`。申万2021批量分类于2021-07-30推出、2021-07-31公开；
   在没有2021-07-30盘中可用时间证据时，日频causal reader只能从公告后的首个交易日2021-08-02消费新版分类。
   对后续单股历史修订，当前汇总文件的`更新日期`不能证明首次公开时间；缺少append-only source snapshot/publication receipt时返回
   `classification_knowledge_time_unverified`，或显式选择非as-known的revised-history研究basis，禁止冒充causal PIT。
4. **行业指数 PIT 的日期合同**：published index constituent reader使用申万行业指数的真实进入/退出日期。2021版行业指数体系的
   切换边界为2021-12-13；该日期不回写股票分类`classification_valid_from`。任何同时消费分类聚合与published sector/index序列的路径
   必须保存`authority_alignment_status`；2021-08-02..2021-12-10两种体系未对齐时不得静默join、前填或共用一个identity hash。
5. **顺序区间只处理有序事实**：同一 canonical symbol、同一 authority/version 内，若多个不同 identity 具有严格递增且权威的
   valid-from，可构造半开区间；相同 identity 的精确重复行可合并并保留全部 source hashes。相同 start 指向不同 identity、
   来源版本未知、切换日期无权威证据或区间重叠时，返回 `industry_identity_ambiguous`，不得以 `MIN/MAX`、`DISTINCT ON`、
   `LIMIT 1`、输入顺序、字典覆盖或字符串前缀裁决。
6. **研究basis必须预先冻结**：每次dataset/request明确携带`industry_authority_type`、`industry_classification_basis`、
   `taxonomy_contract_id`与`knowledge_time_policy`。用户已批准`C-013-G2A-DATA-A`：HR1历史回放的active basis固定为
   `stable_taxonomy_backcast`、`non_as_known_taxonomy=true`；它只允许把冻结as-published candidate中
   `classification_knowledge_time_unverified`且恰好一个完整、hash-valid conflict candidate的区间确定性派生为resolved。零个、多个、
   hash漂移或其他unavailable reason继续typed unavailable。forward active basis固定为`as_published_pit`、
   `non_as_known_taxonomy=false`，不得复用backcast结果或声称as-known causal。双边界及active mode进入
   `hmm_risk_industry_pit_research_basis_v1` canonical hash，不同basis不得混写同一request/artifact/hash。
7. **当前 HMM 使用固定31个L1 denominator且不得伪造成员关系**：taxonomy→published L1 code projection固定为31行
   `hmm_risk_industry_l1_code_projection_v1`。连接键只能是两份权威同时携带的数值`industry_code`：
   `SwClassCode_2021.xls`提供31个SW2021 taxonomy L1 code，`market.sw_index_classify`的Tushare `index_classify/SW2021/L1`
   snapshot在同一行提供`industry_code→index_code`。中文名称只作相等readback断言，禁止按名称、前缀、行序或当前成员事实推断映射。
   projection必须持久化version、source ids/hashes、每行hash及整体canonical hash；31个taxonomy code或31个canonical `801xxx.SI`
   任一不闭合、重复或名称readback不一致均fail closed。该L1产品路径不伪称已闭合131个L2 published code。
8. **resolver 输出是共享数据合同而非 HMM 私有格式**：输入严格为 canonical symbol、trade date、authority type、taxonomy contract 与 authority
   receipt；输出严格为 `resolved` 或 `unavailable`。`resolved` 必须携带唯一 L1/L2/L3、有效半开区间、taxonomy/source/row/receipt
   hashes、known-from与alignment状态；`unavailable` 必须携带稳定 reason、全部冲突候选 identity/hash 且不含伪造行业。
   HMM 只通过 adapter 消费该合同。

#### 23.35.3 不确定成员的业务语义（C-013-PIT-ID-D4）

- 证券的价格、alpha、股票池和可交易资格与行业身份分离。行业身份不确定不删除证券，也不伪造停牌、退市、ST 或价格缺失。
- 依赖行业身份的 contributor、sector aggregation、行业黑名单、sector overlay 与 HMM observation 对该 symbol/date 写入
  `industry_identity_unavailable`；不得补 neutral、前值、当前行业、任意行业或系数 `1.0`。
- sector aggregation 显式从对应行业 numerator/denominator 排除该 contributor，并保留 unavailable count/ratio；是否仍可形成
  level/date output 只由既有批准 coverage 合同判断。单个 ambiguous symbol 不再自动终结全局流程，但 coverage 不足仍 fail closed。
- 必须精确区分 `exact_duplicate_collapsed`、`sequential_interval_resolved`、`taxonomy_version_unavailable`、
  `membership_boundary_unavailable`、`same_boundary_identity_conflict` 与 `catalog_identity_invalid`，不能压缩成 generic missing。

#### 23.35.4 消费者迁移边界（C-013-PIT-ID-D5）

| consumer | 当前风险 | 必须采用的目标语义 | owner |
|---|---|---|---|
| HMM `stock_fact_repository` / HR1 request | 多 identity 正确 fail closed，但会全局阻断 | direct stock-fact aggregation只消费`stock_industry_classification_pit`；typed unavailable进入既有coverage；若再读取published sector序列必须独立验证authority alignment | 本 HMM 窗口 |
| `sector_data_builder` | 当前四只同边界冲突使 preflight blocked | 股票contributor使用classification PIT；published申万指数事实使用index-membership PIT；两种identity/日期/hash分开，不写生产表 | 数据准备窗口 |
| QE data service / Qlib exporter | `merge_asof` 或 `LATERAL ... LIMIT 1` 可静默选行 | 移除顺序依赖；按实际字段声明classification或index authority；candidate dataset重建前显式unavailable/unaligned | QE/数据导出窗口 |
| Selection / Paper | provider 与字典覆盖可静默选择 | industry blacklist使用classification PIT；要求身份时显式排除候选并报告原因；不改变价格/选股 universe | 选股/模拟盘窗口 |
| Advisory | 部分路径已 fail closed，部分路径需同权威 | 股票行业特征使用classification PIT；行业指数特征使用index PIT；不得把unavailable或unaligned当“无行业限制” | 荐股业务窗口 |

执行与券商下单、分钟执行、普通 OHLC/volume-only QE、无行业特征的 alpha 训练不在本 BUG 源码范围内；只有实际读取
行业/sector identity 的路径需要迁移。既有不可变实验不重写，只对决策仍有效且确实消费行业字段的 dataset/实验给出重建或重跑清单。

#### 23.35.5 业务闭环、负责人和停止条件（C-013-PIT-ID-D6）

| business closure | 内容 | 负责人 | 完成条件 / 停止条件 |
|---|---|---|---|
| C-013 shared authority foundation（已完成事实，不再是待执行阶段） | 原P0/P1/P2A：双authority设计、classification/index candidates、完整分母、order-invariant resolver、typed reason、bounded repo-external writer/readback | 数据准备窗口 | PR #3795、PR #3805、backend-main post-restart verify与PR #3810已闭合；classification/index candidate为11,631/8行 |
| G2-A HMM输入到首个真实L1轮动产品（当前唯一HMM闭环） | 接入shared resolver、映射typed unavailable、执行完整601日0-fit预检、同范围程序修复与审核；预检通过后按用户授权执行既有24-fit HR1，能力通过后继续生成真实单历史交易日prediction、最小repository/read API和L1热力图并完成无mock浏览器验收 | 本HMM窗口 | adapter、完整分母预检、24-fit验收、最小bundle、真实prediction/API/UI必须共同闭合；程序缺陷同任务修复，模型/coverage失败返回NOT_AVAILABLE且不生成伪产品，合同变化才停下请求裁决 |
| Cross-consumer authority migration（各owner并行责任，不属于HMM阶段） | `sector_data_builder`、QE/Qlib、Selection/Paper/Advisory按自身真实行业读取路径消费shared contract | 各数据/业务owner | 各模块独立业务验收；不阻断G2-A，不由HMM窗口跨模块修改，也不得把shared core完成冒充消费者已迁移 |

共享authority foundation只证明数据合同可用，不增加F-011/F-013完成度。G2-A内部不得再拆“adapter/预检/训练/bundle/API/UI”阶段；代码merge和24-fit动作因授权边界分别报告，
但仍属于一个输入到首个真实产品闭环。任何owner不得为了“让回放继续”扩大write scope、降低HMM产品门禁、更改股票池、制造默认行业，或用数据基础、fit、bundle、backend-only与静态页面冒充用户功能。

#### 23.35.6 定向验证与验收矩阵

1. resolver 输入顺序完全置换时输出/错误 reason/hash不变；精确重复只折叠、不丢 source lineage。
2. 四只股票的classification regression必须得到旧食品→新基础化工的两个半开区间，新区间候选起点为2021-07-30；
   `605077.SH`的2022-08-21更新日期不得推迟或重写该分类起点。另覆盖一个无行业冲突对照和全部23只已知冲突股。
3. 新版classification causal regression必须证明：无2021-07-30盘中发布证据时，2021-07-30仍使用旧classification，
   2021-08-02才允许使用新版；published index membership则保持到2021-12-10旧版、2021-12-13切换新版。
4. 对任意 trade date，resolver 不读取 `>trade_date` 才可获知的 membership mutation；若选择 backcast/revised-history basis，artifact 必须显式
   标识 `non_as_known_taxonomy=true`，且不得用于声称 as-known causal 验收。
5. `更新日期`仅可进入lineage；使用它计算valid-from、valid-to或known-from的实现必须由负例测试拒绝。classification/index两个resolver的
   schema、receipt与canonical hash不得相同，跨authority join必须显式输出aligned/unaligned/unavailable。
6. 数据 candidate 与 601 日 preflight 都必须以各自冻结的完整 universe/window 为分母，报告 total opportunity、resolved、unavailable、
   按 reason/日期/sector 分布、coverage 与 canonical hash；23只冲突股只作强制回归集合，不能替代全量验收；
   `resolved + unavailable = denominator`，禁止 inner join 缩分母。
7. 对 `qe_data_service`、Qlib exporter、Selection/Paper 与 Advisory 现存首行/覆盖路径建立独立 BUG/Feature 后再改；本 BUG 的文档通过
   不得冒充这些模块已修复。
8. 历史 24-fit artifact 不迁移、不重签、不覆盖；新的 request 使用新的 authority revision。只有 0-fit preflight 与原 HR1
   输入合同同时通过后，才可由用户另行授权训练。

#### 23.35.7 决策与状态

| decision | status | consequence |
|---|---|---|
| C-013-PIT-ID-D1 versioned resolver / no arbitrary tie-break | `RESOLVED_SOURCE_IMPLEMENTED_VERIFIED_SHARED_CORE` | PR #3795与BUG-1193已实现并验证versioned resolver、bounded writer/readback与typed失败 |
| C-013-PIT-ID-D2 classification/index dual authority | `RESOLVED_SOURCE_IMPLEMENTED_VERIFIED_SHARED_CORE` | classification/index双authority、更新日期lineage-only、known-from独立及7/30、8/2、12/13边界已进入共享candidate合同 |
| C-013-PIT-ID-D3 interval and same-boundary semantics | `RESOLVED_SOURCE_IMPLEMENTED_VERIFIED_SHARED_CORE` | 严格顺序半开区间与同边界多identity unavailable已进入共享resolver |
| C-013-PIT-ID-D4 price/universe 与 industry unavailable 分离 | `RESOLVED_HMM_MAPPING_IMPLEMENTED_601D_PREFLIGHT_VERIFIED` | HMM adapter已保留证券资格与industry unavailable分离；stable backcast仅恢复唯一冻结candidate，剩余typed unavailable进入既有coverage |
| C-013-PIT-ID-D5 cross-consumer migration | `PARTIAL_SHARED_CORE_COMPLETE_MODULE_MIGRATIONS_PENDING` | HMM adapter纳入G2-A；其他业务owner独立迁移且不阻断HMM产品闭环 |
| C-013-PIT-ID-D6 phased execution | `RESOLVED_G2_A_DATA_INPUT_VERIFIED_HR1_EXECUTED_RW1_EXACT_PENDING` | 数据authority/shared resolver/P2B/601日预检已闭合；HR1已执行并以NOT_AVAILABLE终止；下一动作是RW1精确合同批准 |
| C-013-G2A-DATA-A historical/forward basis与31行L1 projection | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_601D_PREFLIGHT_VERIFIED` | historical=`stable_taxonomy_backcast/non_as_known=true`，forward=`as_published_pit`；31行code-to-code projection与601日完整分母已闭合，0 fit/selection/model/READY |

当前顶层状态已由§4.3.4.9/§23.40更新为`G2_A_HR1_FORMAL_NOT_AVAILABLE_RW1_DIRECTION_APPROVED_EXACT_CONTRACT_PENDING`。HMM HR1完成
fresh process 1的10个fold fit后正确停止，`rotation_L1=NOT_AVAILABLE`且model/READY为0；数据层PR #3795、资源修复PR #3805及runtime close-sync PR #3810已完成。
production DDL/DML、依赖和HMM runtime均无变化。

#### 23.35.8 P3A 数据准备接口包 v1（BUG-1201）

- P1/P2A 权威源码保持为 BUG-1189 / PR #3795 与 BUG-1193 / PR #3805；P3A 只消费其不可变
  `candidate_bundle_manifest.json`、双 authority receipt、冻结 universe state receipt 和 denominator digest，不复制 resolver。
- P3A 输出固定为 repo-external 新目录下的 `assignments.jsonl`、`sector_facts.jsonl`、`candidate_report.json` 和
  `candidate_manifest.json`。assignment 对每个 frozen symbol/date 保留 `resolved/unaligned/unavailable` 与 typed reason；
  sector fact 仅在 classification/index identity aligned、published fact 可用且至少一个 moneyflow contributor 可用时形成。
- assignment 与 sector fact 分离；sector fact identity 使用 L2 authority projection hash，不从同一 L2 下任意股票的完整 L3
  identity 选取代表。相同 L2 键若映射到冲突 identity，必须 fail closed。
- writer 按交易日有界消费，先在临时目录完成 schema/hash/readback，再原子发布；失败不得留下正式候选目录。CLI 在只读数据库事务中
  校验 live frozen denominator 与双 receipt 完全一致，非 dry-run 还要求 producer commit/tree 在构建前后稳定且工作树 clean。
- `resolved + unaligned + unavailable = expected denominator`，sample/full 必须显式区分。candidate 构建不包含生产 DML、激活、
  HMM adapter、QE/Qlib exporter 或 Selection/Paper/Advisory 迁移；这些消费者仍由各 owner 独立 BUG/PR 验收。
- 最终 HEAD 全分母 dry-run：`5,999,301/5,999,301` assignments 闭合，`4,103 resolved + 260 unaligned +
  5,994,938 unavailable`，生成逻辑识别 1,040 条 sector facts；opportunity digest 为
  `b9fd10d9bb23bc658836620e1a1d64d7e8760d42645cc8e7b98e396012910c052`。该运行数据库只读、写入 0、artifact 写入 false、
  production activation false，Python 工作集抽样约 0.284 GiB。
- 2021-12-13 极小真实 writer/readback 样本仅包含四只请求股票中当日冻结 PIT 有效的 3 个机会，输出 3 assignments、1 sector fact，
  candidate hash `350af8901eb8e801da844ff8f4a74c35d62aa71b60bb3ca733d8a063de5e909a`，写入独立 X 盘新目录且未覆盖既有数据。
- 当前源码状态：BUG-1201 / PR #3829 已由 merge commit `dab281a5` 合入并完成 backend-main post-restart verify。大量 typed
  unavailable 来自上游 classification/index evidence coverage，P3A 未缩分母或伪造行业；该状态仍不表示 full candidate 已签核、
  月度 dataset release 已消费 P3A 或生产数据已激活。

#### 23.35.9 P3A → monthly dataset_release source binding v1（BUG-1218）

- 仅 canonical `qe_hmm_full_v2` 启用该合同；legacy `qe_hmm_full_v1` 保持既有 `market.sector_data` 读取语义。v2 source freeze
  禁止再读取 legacy `market.sector_data`，也不再要求其 refresh-audit/active-writer 证据。
- P3A artifact 必须写入既有 allowlisted `candidate_root` 下的确定性 repo-external 路径：
  `.sector_data_authority/<profile>/<cutoff>/full`；极小 sample 使用
  `.sector_data_authority/<profile>/<cutoff>/sample-<sorted-symbol-set-digest>`。路径、目录和四个文件均禁止 symlink/reparse，
  缺失、越界、scope 不符或存在任意 hash/readback 漂移时 fail closed。
- source adapter 必须重新执行 P3A 四文件 readback，并以冻结 PIT spans × 官方交易日重算完整 opportunity count/digest；必须满足
  `candidate.expected_opportunities == recomputed denominator` 且 opportunity digest 完全一致。不得 inner join 缩分母，也不得以
  `sector_fact_rows` 代替 assignment denominator。
- 只有 `status=resolved`、`alignment_state=aligned` 且引用有效 `sector_fact_row_hash` 的 assignment 才生成稀疏 `sector_data`
  source row；`unaligned/unavailable` 不生成默认行业、neutral、前填或当前成员关系，其 typed reason/count 原样保存在 P3A source
  receipt。因行业不可用产生的稀疏行仍由既有 static-factor coverage 合同处理，不改变 PIT 股票池、价格、停牌、ST 或退市事实。
- `l2_code_id` 延续既有 sorted SW L2 published-index code map，以 aligned fact 的 `index_l2_code` 为映射键；classification taxonomy
  `l2_code` 仍单独保留并验证 assignment/fact identity，不得把 `220xxx` taxonomy code 与 `801xxx.SI` index code 混入同一映射空间。
- adapter 按日期 byte-offset 建立 O(trading-days) 小索引，每个日期只在内存保留当日 sector facts；禁止把全量 assignments/facts
  累积到 DataFrame/list。source partition query version 与 synthetic source schema 均绑定 `candidate_hash`，因此 P3A identity 必须进入
  source content root、component source partition identity、release fingerprint、same-cutoff probe 和 re-attestation；移动或修改
  candidate 不能静默复用旧 identity。
- 本 BUG 只实现 candidate-only source binding、CAS receipt/readback 与共享数据层测试；不执行 P3A/full dataset 导出，不修改或激活
  既有 2026-07-31 数据集，不执行 DDL/DML、生产 pointer、后端/worker 进程控制，也不声称任何新数据 candidate 已发布。

#### 23.35.10 C-013-G2A-DATA-A historical/forward 双basis与L1 projection（2026-08-28）

1. **批准事实**：用户批准历史HR1使用`stable_taxonomy_backcast`，明确`non_as_known_taxonomy=true`且只用于research replay；真实
   forward继续使用`as_published_pit`。该批准不修改estimator、feature、fold、threshold、seed、24-fit预算、selection、capability、
   hard semantic authority或READY，也不授权24-fit、model/READY、DDL/DML、runtime或进程控制。
2. **派生边界**：HMM adapter先完整readback原immutable bundle，再以原classification candidate/receipt/row hashes派生新的
   non-as-known receipt和candidate hash；仅`classification_knowledge_time_unverified`且唯一conflict candidate的行可恢复identity。
   same-boundary conflict、authority unavailable、catalog invalid、零/多candidate或hash不一致均保留typed unavailable。adapter继续调用共享
   `IndustryPitResolver`，不复制first-row/latest/fallback resolver。
3. **31行projection权威**：taxonomy source=`local:SwClassCode_2021.xls`，SHA-256=
   `923492f4bcf3c7056904385a0769e4dda561904a29ecd9243f942680cef68c81`；published-index snapshot=
   `market.sw_index_classify:index_classify:SW2021:L1`，31行canonical SHA-256=
   `876599b554147d1c52b9534872c76c0f05802a5f450fbd383399030ed25d866f`。两侧按数值`industry_code`一一闭合，projection canonical=
   `1a16aec38f49a925e9cd271a05b4b80c5e0af24c9114f9e41d1aa66f504dcc64`；名称只验证同码名称一致，不参与映射。
4. **双basis identity**：research-basis canonical=
   `fc55cd0de171b5552bc33195762ed279560b567a3b45910b4e15979aacd9876c`。mapping/stock-fact/request必须同时保存source classification
   receipt、active classification receipt、active basis、non-as-known flag、derived candidate hash、projection hash和原candidate bundle hash；
   historical与forward request identity不同，禁止跨basis复用component/bundle。
5. **601日零拟合结果**：冻结窗口`2022-01-01..2024-06-30`恰好601个交易日、完整PIT分母`2,666,162`；
   `resolved=2,652,602`、`unavailable=13,560`、coverage=`0.994914037482`，剩余全部为
   `classification:classification_authority_unavailable`。`resolved+unavailable=denominator`，preflight canonical=
   `e5f204d4a31c4e23e17096c8b5d4a39e7268af916a4c384417d37f5065426059`；`fit_count=0`、selection/D5/D6=false、
   model/READY/database/runtime write=false。
6. **后续状态更新**：P2B adapter与601日输入预检在G2-A内部闭合且不增加`11/17`产品完成度；HR1随后正式执行并以NOT_AVAILABLE终止。
   当前下一动作以§4.3.4.9/§24.1为准：精确批准唯一RW1；不得以99.49% input coverage宣称CAPABILITY_AVAILABLE，也不得重跑HR1、先做第二candidate、调阈值或API/UI静态壳。

### 23.36 BUG-1184 详细设计正式审核

审核范围仅为BUG-1184事实边界、C-013-PIT-ID-D1～D6、总Decision/Acceptance Index、§24优先级及跨窗口职责；不审核或授权尚未
实现的resolver、数据candidate、业务消费者迁移或HMM replay。

1. **第一轮——状态一致性**：发现§24仍把24-fit列为直接下一步，且C-013未进入总Decision/Acceptance Index。已把F-011/F-011-A/C、
   C-012与§24统一为输入权威冲突事实，并明确24-fit必须排在数据authority、resolver和601日0-fit预检之后。
2. **第二轮——所有权与完整分母**：发现shared resolver被错误归给HMM窗口，且P1只要求23只冲突股闭合。已拆为数据准备窗口拥有的
   P2A shared core与本HMM窗口拥有的P2B adapter；P1/P3验收改为冻结universe/window全分母，23只仅为强制回归集。
3. **第三轮——F2验收语义**：发现自定义blocked状态与非可执行证据不符合矩阵schema。已在不伪造用户批准的前提下改为
   `VERIFIED_*`事实状态，绑定BUG JSON和真实failure artifact；待决业务合同继续只在Decision Index中保持
   `PROPOSED_PENDING_*`，没有把它们升级为active gate。
4. **第四轮——本地申万资料与双authority复审**：发现原设计把2021-07-30分类计入与2021-12-13指数切换误当成竞争日期，且可能把
   `更新日期`误用为knowledge time。已核验代码表、11,803行股票分类历史、7月末快照和官方说明的exact SHA；拆分
   `stock_industry_classification_pit`与`sw_industry_index_membership_pit`，增加独立`known_from`、首个causal交易日及alignment状态。
   `更新日期`仅保留lineage；四只股票通过一般化顺序区间合同闭合，不新增symbol hard-code。
5. **第五轮——source fact assertions**：以只读解析对四个冻结文件执行断言，确认catalog=`511×4`、history=`11,803`、
   canonical numeric stock=`5,161`、exact/same-stock-date duplicates=`0/0`；四只新identity均为`220315@2021-07-30`，7月末快照一致，
   `605077`的`source_last_updated_at=2022-08-21`且全表存在5条update早于valid-from。所有断言通过，进一步否定“更新日期=调整生效日”。
6. **禁止简化交付：PASS**。设计覆盖完整universe/window、23只已知冲突、全部五类直接消费者、双authority和resolved/unavailable完整分母；
   不以四只硬编码、仅HMM局部修复、latest映射或删除证券代替完整机制。
7. **禁止静默错误：PASS**。同边界多identity、未知taxonomy、缺少成员边界、knowledge time未验证、authority未对齐和catalog非法分别typed fail closed；禁止首行、排序、
   `LIMIT 1`、字典覆盖、neutral、前值、当前行业或系数1.0 fallback。
8. **禁止业务逻辑迁移：PASS_FOR_PROPOSED_DESIGN**。股票池、价格/alpha资格、HMM模型/seed/fold/threshold/hard semantic和
   既有coverage合同均不变；classification/index双authority及“行业unavailable与证券资格分离”仍明确等待用户批准，未进入源码。
9. **禁止未经确认的门禁/审批：PASS**。新增项是确定性数据合同与typed状态，不是运行时人工审批；D1～D6均保持proposed，
   production DDL/DML、依赖、部署、runtime、进程控制、训练和merge仍是独立授权。
10. **可验证性：PASS**。F2 validator为`PASS / 16 matrix rows / warnings=0`；changed-files仅为BUG metadata与本权威设计，
   `validation_module_registry_l0=8 passed / 14 of 14 mapped`，L0 guardrail为0 finding，`git diff --check`通过。

审核结论：`PASS_DOCUMENT_MERGE_READY_USER_DECISIONS_REQUIRED_NO_IMPLEMENTATION_AUTHORITY`。这里的merge-ready只表示文档内部一致、
事实与职责可审计；不表示C-013-D1～D6已获用户批准、数据已准备、跨模块源码已修复、24-fit可启动或产品能力可用。

### 23.37 Gate 2交付粒度收敛与C-013实施状态正式审核

> 本节记录父蓝图v2.33的历史审核结论；其中“G2-A止于能力bundle、G2-B才开始真实产品”的编排已由父蓝图v2.34和§23.38取代，不再作为当前任务权威。

本轮审核对象为父蓝图v2.33、本文顶层状态、§0.3、Decision/Acceptance Index、§23.35及§24.1。目标是消除“小功能即阶段”的流程碎片，
同时不合并必须独立授权的merge、实验、生产和进程动作，也不把数据基础设施完成冒充模型或产品完成。

1. **实时状态修正：PASS**。C-013 P1/P2A已由PR #3795 / merge `e66d8680…`合入；BUG-1193 bounded writer由
   PR #3805 / merge `592aeb1a…`修复，backend-main重启后identity/business smoke通过并由PR #3810 close-sync。分类与指数candidate
   分别为11,631/8行。旧`NO_SOURCE_IMPLEMENTATION`、`BUG_1184_BLOCKED`与pending-user-decision状态已从当前顶层、Decision Index和矩阵移除；
   历史§23.36仍只描述当时设计审核，不作为当前状态。
2. **交付粒度：PASS_THREE_BUSINESS_CLOSURES**。Gate 2只保留G2-A输入到能力、G2-B能力到真实产品、G2-C产品到受控日任务。
   P2B/601日预检/24-fit不再是三个阶段；P2-5/P2-6不再拆成后端、API、UI或历史分析阶段。现有编号仅作合同索引。
3. **授权边界：PASS_SEPARATE_ACTIONS_NOT_STAGES**。代码merge、实验、生产DDL/DML/依赖、runtime activation和进程控制仍须按规范分别授权和报告；
   这些状态不产生新的feature阶段，也不得互相推导完成。G2-A可在预检通过后停在等待24-fit授权，不因此创建新的设计任务。
4. **失败与缺陷收敛：PASS_FAIL_CLOSED**。同合同程序缺陷必须留在当前业务包修复、定向回归并重新审核；模型/coverage验收失败按批准合同形成
   `NOT_AVAILABLE`并停止，不自动扩seed、调阈值、换candidate或建立诊断阶段。只有合同变化、生产动作、owner冲突或真实scope越界才允许拆分。
5. **禁止简化交付：PASS**。G2-A必须同时闭合adapter、完整601日分母、24-fit能力判定和最小bundle；G2-B必须是预测、分析、API/UI和真实浏览器验收的
   完整纵切；不得以adapter源码、预检receipt、fit完成、backend-only或静态页面冒充闭环完成。
6. **禁止静默错误：PASS**。双authority、typed unavailable、coverage/abstention、未通过能力与失败原因均保留；减少任务数量不删除任何验证条件，
   不引入neutral、当前行业、前值、任意tie-break或产品fallback。
7. **禁止业务逻辑迁移：PASS**。C-012 estimator、feature、fold、seed、阈值、hard semantic authority、advisory-only、两类PIT authority及
   CAPABILITY_AVAILABLE/FULL_READY/NOT_AVAILABLE语义均不变；本轮只修改交付编排与实时状态。
8. **禁止未经确认的门禁/审批：PASS**。没有新增研究淘汰、人工审批或runtime门禁；“不拆小阶段”是任务编排约束，不是模型验收条件。
   模型合同变化仍必须由用户确认，PR合入和进程控制仍为独立授权。
9. **进度真实性：PASS**。C-013及BUG-1193完成解除输入基础blocker，但不增加17项产品验收计数；24-fit、capability bundle、P2-5/P2-6产品结果均为0，
   严格进度保持`11/17=64.71%`。

审核结论：`PASS_GATE2_THREE_BUSINESS_CLOSURES_DESIGN_MERGE_READY_NO_SOURCE_OR_EXPERIMENT_AUTHORITY`。下一任务只能是G2-A，且应在一个稳定任务范围内
连续完成HMM adapter、601日预检、同范围修复与正式审核；预检通过后再请求24-fit动作授权，不再为这些内部步骤建立独立阶段。

### 23.38 Gate 2首个真实功能闭环正式审核（父蓝图v2.34）

本轮审核对象为父蓝图v2.34、本文顶层状态、§0.2～§0.3、C-012-D5、G2-A/G2-B/G2-C实施纵切、Read API、Rollout、
Design Acceptance Matrix、§23.35和§24.1。审核目标是消除“首个闭环仍只产出bundle”的目标偏差，同时不放宽模型验收、伪造风险能力或增加未经确认的门禁。

1. **第一轮——用户结果与闭环终点：PASS_AFTER_REVISION**。发现v2.33 G2-A只要求adapter/preflight/24-fit/bundle，真实prediction/API/UI全部后置，仍可能在数周开发后没有可验证功能。已将G2-A终点改为真实单历史交易日rotation_L1 prediction、最小repository/read API、L1热力图和无mock浏览器验收；bundle-only、backend-only、static UI均明确不算完成。
2. **第二轮——能力范围与非简化边界：PASS**。G2-A只交付已批准且可独立使用的`rotation_L1`能力，但必须覆盖完整31个L1分母、真实score/state、missing cells、coverage/abstention、validation basis和forward status；其余三能力显式`NOT_AVAILABLE`。这不是FULL_READY、完整F-013或Phase 2完成，也不删除L2/risk目标。
3. **第三轮——失败语义：PASS_FAIL_CLOSED**。24-fit、能力或coverage失败时G2-A以`NOT_AVAILABLE`终止，不生成空成功、静态页面、默认neutral、当前行业、前值或risk fallback；程序缺陷在同范围修复后重验，模型合同变化才请求用户裁决。
4. **第四轮——产品身份与因果边界：PASS**。单日prediction只能由冻结HR1 request与input-complete canonical calendar机械派生，绑定同一bundle/authority identity；不得读取运行时latest、人工挑选表现日期、复用已消费holdout调参或从未来数据生成state。
5. **第五轮——API/UI真实性：PASS**。G2-A复用既有`overview`/`heatmap` read API语义，真实backend和无mock浏览器流程验收；API失败、缺失cell、renderer错误与不可用能力均有typed可见状态。`/hmm-risk`可直接用于纵切验证，但`/hmm`默认切换仍等待F-011～F-013完整验收。
6. **第六轮——后续范围与反过度工程：PASS**。G2-B只扩展多日历史、transition/severity、预警、产品指标、详情和后续已验收能力；G2-C才加入共同水位、job、revision/dedupe、late-data和runner。没有新增通用训练/evidence/调度平台、历史artifact迁移或重复输入物化。
7. **禁止简化交付：PASS**。首个能力切片虽为rotation_L1，但它是完整端到端真实功能；没有用POC、placeholder、mock-only、局部分母、bundle或静态页面代替声明结果，未实现能力也没有被删除或冒充成功。
8. **禁止静默错误：PASS**。身份、coverage、abstention、missing cell、能力不可用、API/renderer失败和完整分母均显式；任何必需readback/hash/identity不闭合均fail closed。
9. **禁止业务逻辑迁移：PASS**。C-012 estimator、feature、fold、seed、阈值、hard semantic authority、advisory-only、双PIT authority和CAPABILITY_AVAILABLE/FULL_READY/NOT_AVAILABLE语义全部不变；本轮只改变首个用户功能的交付编排。
10. **禁止未经确认的门禁/审批：PASS**。没有新增模型阈值、研究淘汰、人工审批或runtime门禁；PR合入、24-fit、production DDL/DML、依赖、runtime activation和进程控制仍按既有边界分别授权。
11. **进度真实性：PASS**。本轮仅修订设计；24-fit、bundle、prediction、API/UI与浏览器业务验收仍为0，严格进度保持`11/17=64.71%`，不得因文档通过而提升产品完成度。

审核结论：`PASS_G2_A_FIRST_REAL_ROTATION_L1_PRODUCT_DESIGN_MERGE_READY_NO_SOURCE_EXPERIMENT_OR_RUNTIME_AUTHORITY`。下一任务仍为单一G2-A Feature范围，但其终止条件已从“能力bundle生成”提升为“真实单日rotation_L1产品纵切完成”；若能力正式`NOT_AVAILABLE`，则按批准失败语义终止而不是伪造产品。

### 23.39 C-013-G2A-DATA-A 正式设计与源码审核

审核范围仅为historical/forward basis、31行L1 projection、HMM adapter、stock-fact消费、601日零拟合预检和HR1 request identity；不审核或授权24-fit、selection、capability、model/READY、API/UI、DDL/DML、runtime或进程控制。

1. **权威与映射：PASS**。`.xls`只提供31个taxonomy L1 code；published index snapshot在同一行直接提供
   `industry_code→index_code`。实现严格按数值code连接，名称只作readback，不使用名称、前缀、输入顺序、latest/member fallback。
2. **historical/forward因果边界：PASS**。historical只恢复`knowledge_time_unverified + exactly-one + hash-valid`冻结candidate并在每个projection、mapping、preflight和stock-fact row暴露`stable_taxonomy_backcast/non_as_known=true`；forward绑定同一版本合同但active basis为`as_published_pit`。两者identity/hash不同，禁止互换。
3. **typed unavailable与完整分母：PASS**。其他unavailable reason、零/多candidate、identity/provenance漂移继续fail closed；601日
   `resolved+unavailable=2,666,162`，没有inner join缩分母、删除证券、neutral、当前行业或前值填充。
4. **第一轮源码审核修复：PASS_AFTER_FIX**。发现basis/projection version只校验非空、未绑定basis仍可resolve、同一adapter可重绑不同authority、derived candidate hash受输入行序影响。已改为精确常量、显式未绑定失败、不同hash禁止重绑、row hash排序，并增加conflict identity/source/lineage闭合测试。
5. **禁止简化交付：PASS**。adapter覆盖完整冻结bundle和601日分母；31行projection必须双侧一一闭合。没有用四只股票、局部日期、单source名称映射或静态fixture冒充正式输入。
6. **禁止静默错误：PASS**。schema、version、source ids/hashes、row/overall hash、basis、active mode、candidate/receipt/lineage、31-code closure及readback任一不符均typed失败；remaining 13,560机会显式不可用。
7. **禁止业务逻辑迁移：PASS**。C-012 estimator/feature/fold/seed/threshold、24-fit、selection、hard semantic、capability/READY和advisory-only均未改变；classification决定contributor，index membership只保留独立diagnostic/alignment。
8. **禁止未经确认的门禁/审批：PASS**。只激活用户批准的`C-013-G2A-DATA-A`确定性合同，没有新增coverage阈值、模型淘汰、runtime人工审批或发布门禁；24-fit与PR merge仍单独授权。
9. **反过度工程：PASS**。只增加G2-A直接需要的adapter、版本化小crosswalk、source request绑定和既有路径测试；没有新DB、registry、scheduler、通用evidence平台或历史artifact迁移。严格产品进度保持`11/17=64.71%`。
10. **第二轮源码复审修复：PASS_AFTER_FIX**。复审发现四个可形成局部自洽但不完整权威闭包的路径：空`source_ids/source_hashes`会因空集subset语义通过historical conflict provenance；先绑定projection再切换historical basis会留下旧classification receipt的constituent；未绑定31行projection仍可返回`closure.passed=true`的preflight；仅有L1 crosswalk的adapter公共reader仍允许请求direct L2。实现已改为conflict provenance非空且逐项有效、basis切换同步刷新全部constituent receipt、preflight强制projection已绑定、adapter路径显式拒绝L2，并以6个新增RED→GREEN case覆盖。上述修复不改变historical/forward basis、股票池、C-012模型、24-fit、阈值、selection或产品能力语义，也不增加人工门禁。

审核结论：`PASS_C013_G2A_DATA_A_SOURCE_AND_DESIGN_REVIEW_READY_PENDING_PR_NO_FIT_MODEL_OR_RUNTIME`。下一动作是在最终HEAD完成模块门禁和PR；PR合入后才请求24-fit授权。

### 23.40 HR1正式结果、根因边界与RW1方向正式审核

审核对象为父蓝图v2.36、§4.3.4.7～§4.3.4.9、Decision Index、Design Acceptance Matrix与§24.1。审核只读取已完成HR1 artifact并修订设计，
没有执行fit、selection、holdout、writer、数据库或runtime动作。

1. **正式结果完整性：PASS_FAIL_CLOSED**。HR1在fresh process 1完成五foldmarket/Ridge共10 fits后进入development acceptance；既有4/5 fold与median Rank IC通过，
   median spread和两项OOF NW t失败。按合同停止第二进程/final/holdout是正确行为，不是“14 fits缺失”或可重试程序BUG。
2. **根因结论边界：PASS_NO_OVERCLAIM**。fold-3双指标为负、相邻系数方向稳定、排除fold-3后spread NW仍失败，共同支持time non-stationarity与弱经济spread；
   这些证据不证明所有rolling window必然通过，也不允许把252从诊断推导为已批准最优参数。
3. **唯一候选与反过度工程：PASS**。RW1只改变Ridge时间域与historical eligibility；market、feature、target、alpha、seed、fold、metric和阈值不变，
   没有window grid、第二模型、通用平台、artifact迁移或重复输入物化。RW1失败即终止该模型方向。
4. **coverage语义：PASS_AFTER_REVISION**。第一轮审核发现若只把801230从分母删除，会形成事后缩分母与业务覆盖漂移；第二轮又发现“eligible全覆盖”若仍命名
   `FULL_COVERAGE`会冒充canonical 31全覆盖。修订后eligibility必须在validation outcome前由首日PIT/t-1结构输入冻结，同时保留canonical 31、typed ineligible和eligibility ratio；
   任一fold的`|E_f|<31`时顶层最高只能`COVERAGE_AVAILABLE`，forward仍用31分母，禁止neutral/前值/当前行业fallback。
5. **门禁与批准边界：PASS_USER_APPROVED_EXACT**。D4经济门槛没有降低；D5公式只对pre-frozen eligible集合定义historical coverage，并保留最低28个sector。
   用户已精确批准`W=252`、eligibility公式及D1～D6并授权源码与测试；正式24-fit、合入、model/READY、DB和runtime仍未授权。
6. **DESIGN-COMPLIANCE-001——禁止简化交付：PASS**。RW1若通过仍须继续真实prediction/API/UI；candidate或receipt不能冒充产品。完整31 universe与未验收能力均保留。
7. **DESIGN-COMPLIANCE-001——禁止静默错误：PASS**。rolling不足、结构ineligible、eligibility漂移、coverage与经济失败均有typed状态；任何失败不写model/READY。
8. **DESIGN-COMPLIANCE-001——禁止业务逻辑迁移：PASS_USER_APPROVED_EXACT**。用户已批准252日rolling与historical eligibility；其余模型、target、经济阈值与产品语义保持不变。
9. **DESIGN-COMPLIANCE-001——禁止未经确认的门禁/审批：PASS**。没有新增runtime人工审批；PR merge、正式实验、DDL/DML、依赖、runtime和进程控制仍是独立授权。
10. **进度真实性：PASS**。文档修订不增加产品完成度；rotation_L1、canonical bundle、API/UI仍不可用，严格进度保持`11/17=64.71%`。
11. **第一轮源码审核修复：PASS_AFTER_FIX**。发现先按`E_f`缩小validation panel再构造10D target会重算横截面中位数，形成target业务语义迁移；已改为先按canonical 31及原公式构造target，再只投影eligible identity，并以明确`target_formula_recomputed=false` receipt与直接正反例闭合。
12. **第二轮源码审核修复：PASS_AFTER_FIX**。发现eligibility实现直接读取首个validation日feature而非其前一canonical open day，且旧holdout reader的payload/algorithm identity仍从live candidate常量派生。已分别冻结`authority_date`与`eligibility_feature_cutoff_date`、验证source/C-010/C-013 authority hash，并将旧holdout的contract/algorithm/schema/threshold/payload keys固化为immutable本地常量；self-hashed authority drift与跨合同重解释均有失败测试。
13. **第三轮源码审核修复：PASS_AFTER_FIX**。发现validation feature若先过滤`E_f`，既有`relative=True`路径会按缩小集合重算横截面median，仍会迁移十维feature定义。已改为canonical 31完成preprocess/relative feature构造后只投影eligible sequence；资格receipt同时固定feature/target denominator contract，直接测试证明投影值逐字节保持且不重新center。
14. **最终源码验证：PASS_PENDING_PR_NO_FORMAL_FIT**。RW1与immutable legacy-reader直接测试`63 passed`；`hmm_risk_backend=643 passed`、branch coverage总计`76.93%>=70%`；module ownership `4/4 mapped,unmapped=0,ambiguous=0`；module registry `8 passed,14/14 mapped`；L0 `blocking=0`；F2 `PASS,design_items=3,matrix_rows=16,warnings=0`；runtime classifier=`none/runtime_files=[]/target_ids=[]`。上述测试未运行正式24-fit、selection、holdout、model/READY、DB或runtime。

审核结论：`PASS_HR1_FORMAL_RESULT_AND_RW1_EXACT_D1_D6_USER_APPROVED_SOURCE_IMPLEMENTATION_AUTHORIZED_NO_FORMAL_EXPERIMENT`。
