# HMM Evolution Phase 2 风险监控与预警 F2 实现级详细设计

- 文档类型：F2 从属实现级详细设计 / Feature Card
- 日期：2026-07-22
- 状态：`C008_B3_DIRECTION_APPROVED_DESIGN_AUDIT_REPAIRED_DETAILS_PENDING_USER_CONFIRMATION`
- 父级权威：`docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md` v2.11
- 上游权威：`docs/architecture/hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md` v2.8
- Feature tier：F2
- Design Acceptance Index：F-011、F-012、F-013
- 当前边界：C-001-A/C-002-A/C-003-A 已于 2026-07-22 获用户明确批准；C-006-A/C-007-A/C-008-D1/C-008-B1 已于 2026-07-23 获用户明确批准；C-008-B3-DESIGN 仅批准保留 hard semantic authority、train-only family-global restart selection、两 family 完整交付和禁止 validation-driven seed picking 的方向。算法全参数、数值阈值、restart schedule、selection score/tie-break 与额外数据分区尚未获用户确认，C-008-B3 尚未实现；任何后续 PR 合入仍须用户逐 PR 明确确认

本文只细化总体蓝图已批准的 Phase 2。它不建立第二套产品方向，不修改 Selection、Advisory、
Paper v2、MiniQMT、StrategyPackage、QE 或现有 `hmm_risk_gate_v1` 消费者的业务语义。
Phase 2 的输出是研究分析事实，不是交易门禁、可买性、调仓或模型晋级结论。

## 0. Feature Card / 功能卡

### 0.1 用户结果

用户可以在 `/hmm-risk` 查看指定 HMM candidate 在最新共同完成交易日的申万 L1/L2 状态、
最近 7 个完整交易日热力图、今日预警、固定详情、事件生命周期和版本化回测报告。所有页面事实来自
`hmm_risk.*` 的真实 API；数据缺失、输入漂移、部分失败和 renderer 错误均显式展示。

### 0.2 成功边界

- F-011：唯一 versioned sector-state generator、共同水位、revision/dedupe、预警状态机和迟到数据重算完整；其 direct L1 model-set preparation 当前为 `BLOCKED_C008_B3_EXACT_CONTRACT_DECISIONS`，不得以单 family、部分 sector、未经批准的阈值或 validation-picked seed 冒充完成。
- F-012：所有生成、查询和报告均为 advisory-only，只写 `hmm_risk.*`，不产生任何交易副作用。
- F-013：真实 API/UI 完成 L1/L2、7 日热力图、今日预警、固定详情、状态分布、事件与回测证据。

### 0.3 交付顺序

先完成 catalog/schema/纯计算与 repository，再完成受控 job/API，最后完成真实 UI。任何实现 PR 只在其
Design Acceptance Matrix 行全部有源码和结果证据后才能标为完成。不得以后端-only、静态页面冒充完成；
不得以 mock-only、旧 artifact 换皮或部分 sector 成功冒充 Phase 2 完成。

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

历史 `sw_index_member.l1_code/l2_code` 允许保存 `industry_code` 或 `index_code` 表示；resolver 必须使用
`market.sw_index_classify` 同 level 的唯一行，将二者规范化为 canonical `index_code`。同一 symbol/date 的多条源行
只有在规范化后 L1/L2 完全相同才可合并为一个 identity，同时所有源行、有效期和 source-row hash 都必须进入
mapping manifest；规范化后仍指向不同 L1/L2、classify 缺失或一对多时显式失败。禁止 `DISTINCT ON`、排序首行、
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
本节在正式审核后把已批准方向与待用户确认的实现合同分开；在 D3-D6 的精确决策完成前不得实施 B3、选择 seed、写模型或
READY artifact。

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

##### D3. initialization 与 restart 的精确合同仍待确认

以下项目必须形成一组完整、版本化且经用户确认的值后才能实现；库默认值不构成合同：

- restart schedule、restart 数量、seed 粒度与完整运行规则；C-008-A/B1 的 `42..49` 只是诊断 grid，不自动成为 B3 schedule；
- KMeans 的 `init/n_clusters/n_init/random_state/max_iter/tol/algorithm/copy_x`、空 cluster 语义，以及 center/covariance
  initialization 公式；
- GaussianHMM 的 `n_components/covariance_type/min_covar/startprob_prior/transmat_prior/means_prior/means_weight/
  covars_prior/covars_weight/algorithm/random_state/n_iter/tol/params/init_params/implementation` 全参数；
- `startprob_`、`transmat_` 初值与 transition smoothing/constraint 的先后顺序、公式和版本；
- legacy `identity` 与 autocycle train-only global 1%/99% winsor + z-score preprocess 保持已批准，但 family 内拟合粒度、
  序列化精度和 hash 输入必须明确。

##### D4. 独立数值验收合同仍待确认

fit、monitor、likelihood、covariance、train occupancy 必须是独立状态；`monitor_converged=true` 不推导 likelihood 或
covariance 可接受。下列具体公式/阈值尚未获批，不能把当前文档旧值写成 active gate：

- likelihood decrease 的绝对/相对 tolerance、terminal 与 non-terminal 差异和 failure/warning 语义；
- feature-scale variance 的统计域、degrees of freedom、floor/ceiling 公式、raw invalid 规则、总量/per-state/per-feature
  anomaly budget；
- train hard count、normalized occupancy、contiguous run/transition/dwell coverage 与 posterior normalization/tie tolerance；
- 任一阈值的版本、边界比较符、non-finite/zero denominator 语义和 typed reason code。

C-008-B1 明确记录 `formal_acceptance_thresholds_applied=false`，且没有 date-level calendar-month coverage、连续 run 序列或
feature-scale variance，不能据此验证上述旧阈值。旧值只能作为被审核驳回的草案，不能作为实现或 READY 证据。

##### D5. train-only family selection 的待决精确合同

- 只有同一 family 的 31/31 sector 均通过最终 D3/D4 合同的 restart 才能成为 family candidate；局部 sector 不能拼接；
- selection score、normalization、sector 聚合、数值精度与 deterministic tie-break 仍待用户确认；validation/future utility
  始终不可见，selection 完成后不得 refit；
- reproducibility receipt 必须记录全部候选、失败阶段/reason、parameter hash、selected identity 与
  `validation_accessed=false/future_utility_accessed=false`；
- bitwise/canonical hash 不能仅由 seed 推导。实现合同还必须固定 Python/NumPy/scikit-learn/hmmlearn 版本、BLAS/线程
  环境、浮点 canonical serialization 与允许的重复运行数值容差；这些决策完成前不得声称 deterministic hash 已闭合。

##### D6. hard semantic validation 的待决证据充分性

- 选中 family-global restart 后，才在 D2 单一 validation 窗口执行 causal hard argmax；soft mass/ESS 只作诊断；
- hard utility 沿用已批准的 `0.35*5D + 0.35*10D + 0.30*20D` future excess return；三个 utility 必须有限且
  `fading < neutral < trending` 严格排序；
- validation hard count、calendar-month/run coverage、utility separation/tie tolerance 的精确阈值尚未确认。仅出现 1 个
  hard sample 不自动构成证据充分，但实现也不得自行选择最小值；
- 缺态、non-finite 或 utility tie 继续 fail closed；证据不足不得返回 selection 换 seed，也不得使用 state index、neutral
  fallback、soft-weighted utility 或另一个 family 的 mapping 修复；
- hard-state missing、state-count insufficient、month/run coverage insufficient、utility non-finite/tie/gap insufficient
  必须分别保留 typed reason，不能压缩为 generic semantic failure。

##### D7. model identity、依赖与 READY

最终 B3 algorithm identity 至少必须包含：D2 窗口与 causal prior、经批准的 restart schedule、KMeans/HMM 全参数、preprocess、
likelihood/covariance/occupancy/semantic 阈值版本、selection formula/tie-break、全部候选摘要、selected family identity、
validation mapping/receipt、运行库/数值环境和全部 input hashes。

只有两个 family 各自 31/31 entry 同时满足独立的 fit/convergence/likelihood/covariance/occupancy/selection/semantic
evidence 合同，且原 C-002-A 的 L2 131/131、parser/hash/causal replay 全部通过，才允许构建 `READY` model set。
当前 `hmmlearn` 0.3.3 仅存在于执行环境但未在 `requirements.txt` 声明；当前文档 PR 的 backend dependency gate 为
`noop`，未来 B3 实现的 `production_backend_dependency_gate` 必须保持 `pending`，直至版本声明随实现合入、安装获得独立
授权并完成 import/version smoke。上述状态不是新增人工审批，而是现有依赖 gate 的准确记录。

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
`sw_index_classify` 将历史 industry/index code 表示规范化后，冻结 source 与 canonical
`symbol/l1_code/l1_name/l2_code/l2_name/in_date/out_date` 的排序 hash。只有规范化后 identity 完全相同的多条
source row 才可保留全部 source evidence 后折叠；非等价多重 active mapping、L2 对应多个 canonical L1、
缺 code/classify 或空 mapping 均显式失败。历史日不得读取当前成员关系。
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

- `overview`：current watermark、candidate/model identity、latest run、coverage、state distribution、alerts、staleness。
- `heatmap`：显式 candidate、level/date range；返回 cells、missing cells、run status 和 current revisions。
- `alerts`：按 date/level/candidate 查询 current alerts；无 alert 是真实空，不等于数据缺失。
- `timeline`：sector + candidate + level，返回 state revisions/current marker。
- `events/{event_id}`：完整 event revision chain 和可读 evidence。
- job status：`GET /jobs/{run_id}` 返回 persisted/effective status、lease/heartbeat、counts、missing evidence、
  stable error 与 timestamps；另有上述 cancel endpoint。
- report：增加 `GET /reports` 与 `GET /reports/{report_id}`；生成使用受控 job/CLI，不在普通 GET 中写入。

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
- `hmm_risk_model_likelihood_decrease`
- `hmm_risk_model_likelihood_tolerance_failed`
- `hmm_risk_model_covariance_invalid`
- `hmm_risk_model_covariance_acceptance_failed`
- `hmm_risk_model_covariance_bounds_failed`
- `hmm_risk_model_covariance_anomaly_budget_exceeded`
- `hmm_risk_model_train_occupancy_insufficient`
- `hmm_risk_model_train_state_count_insufficient`
- `hmm_risk_model_train_run_coverage_insufficient`
- `hmm_risk_model_posterior_tie`
- `hmm_risk_model_restart_family_incomplete`
- `hmm_risk_model_restart_schedule_incomplete`
- `hmm_risk_semantic_hard_state_missing`
- `hmm_risk_semantic_evidence_insufficient`
- `hmm_risk_semantic_validation_state_count_insufficient`
- `hmm_risk_semantic_validation_month_coverage_insufficient`
- `hmm_risk_semantic_validation_run_coverage_insufficient`
- `hmm_risk_semantic_validation_utility_gap_insufficient`
- `hmm_risk_semantic_utility_non_finite`
- `hmm_risk_semantic_utility_tie`
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

C-008-B3 相关新 reason code 当前为保留合同名称，不代表其阈值已生效。未来实现必须在对应精确阈值获确认后使用最具体
reason code；不得把 initialization、likelihood、covariance、occupancy、validation evidence 或 family selection 失败
压缩为 generic incomplete，也不得在未批准阈值上提前触发 gate。

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

### Slice 0：catalog 与 schema contract

- 修改三份 validation catalog，登记 `hmm.risk` ownership/module/test plans。
- 新增 `backend/db/init_hmm_risk_schema.py` 和 exact schema tests。
- 新增受控 state-model-set artifact preparation：为两个已批准 L2 family 生成配对 direct L1 artifact，
  输出 `hmm_risk_state_model_set_v1` manifest；daily worker 不参与训练。
- C-008-A/B1 已证明当前 direct L1 preparation 合同不能完成 legacy 31/31；Slice 0 的 schema/direct preparation
  implementation 可继续保留。C-008-B3-DESIGN 仅批准 hard-authority/train-only family-global restart 方向，精确算法、
  阈值、selection 与 evidence contract 尚待用户确认，完整 model-set preparation 状态为
  `BLOCKED_C008_B3_EXACT_CONTRACT_DECISIONS`；诊断或设计方向批准均不构成 Slice 0 model artifact 完成。
- 仅 DEV DDL 验证；production DDL 独立 pending。

### Slice 1：identity、input、generator、repository

- 新增 `models.py`、`input_resolver.py`、`market_repository.py`、`observation.py`、`state_generator.py`、
  `alert_state_machine.py`、`repository.py`。
- 完成 C-001-A candidate capability resolver、C-002-A model-set adapters、deterministic hashes、direct L1/L2、revision、late-data cascade 和 isolation tests。
- 不修改或迁移 legacy script；旧 gate 不属于本模块 changed files。

### Slice 2：durable job、worker、API、report

- 新增 job service、manual worker CLI、router、report service。
- API 只登记 `hmm_risk.*` queue；worker 显式启动。
- 完成 API envelope、idempotency、lease/fencing、partial/failed 和 report tests。

### Slice 3：真实 UI 与 route activation

- 新增 `/hmm-risk` 页面、typed adapter、heatmap、fixed detail、alerts/events/report panels。
- 修改 HMM navigation；只有真实 API/UI 验收后才切 `/hmm` 默认 route。
- 安全端口运行真实 API/UI Playwright；不得 mock route 冒充最终验收。

每个 slice 都是完整、可验证的 feature slice；不得用 schema-only、backend-only 或 static UI 宣称整个 Phase 2 完成。

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
- C-008-B3 实现 fix-point 仅在 D3-D6 精确合同获用户确认后启用；届时必须覆盖：批准的完整 restart schedule 且不
  early-stop；KMeans/HMM 全参数 identity；likelihood/covariance/occupancy/validation evidence 的批准阈值边界；仅 31/31
  family candidate 可参与 selection；family-global seed 且禁止 per-sector 拼接；selection score/tie-break；selection 对
  validation/future utility 不可见；selection 后不 refit；validation 从 fitted `startprob_` 重启 causal filtering；任一
  family blocked 时不得写 READY。未确认的阈值不得先写测试再反向成为业务合同。
- C-008-B3 artifact contract smoke 必须回读批准 schedule 的全部 candidate 摘要、selected family identity、未选 reason、
  完整算法/依赖/数值环境版本、validation mapping/receipt，并按批准的数值可复现性合同验证 selection receipt/hash。

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
| C-008-B3-DESIGN | 是否保留 hard semantic authority并重构 initialization/restart/occupancy/selection | `DIRECTION_APPROVED_EXACT_CONTRACT_PENDING_USER_CONFIRMATION` | 已批准 hard authority、train-only family-global selection、两 family 完整和禁止 validation-driven/per-sector selection；算法全参数、restart schedule、数值阈值、selection score/tie-break 与任何额外 split/holdout 均未批准，4.3.2 D 已按审核修订 |

C-001-A/C-002-A/C-003-A 已于 2026-07-22 获用户明确批准并回填本文；它们不是运行时人工审批。
C-006-A 已于 2026-07-23 获用户明确批准并回填本文；它不新增运行时审批或第二套股票池。
C-007-A 已于 2026-07-23 获用户明确批准并回填本文；它是 offline artifact-preparation 的固定算法版本，
不是运行时人工确认或可调门禁。
C-008-D1/C-008-B1/C-008-B3-DESIGN 方向已于 2026-07-23 获用户明确批准；D1 是设计证据修订，B1 是已完成的只读
diagnostic-only 合同，B3-DESIGN 只批准 4.3.2 D1 的方向边界。D3-D6 的精确合同、B2、B3 源码实现、seed selection
执行、READY artifact 与 runtime/database 写入均未获授权。本次用户另行明确授权当前文档修复 PR 合入，不扩展上述模型授权。
C-005 是用户明确要求的交付控制，适用于今后每个 PR。

## 18. Design Acceptance Index / 设计验收索引

- F-011 parent：`BLOCKED_C008_B3_EXACT_CONTRACT_DECISIONS`；C-008 方向已批准，但精确算法/阈值/selection 合同、源码、真实 selection 与两-family READY 证据均未完成。
- F-011-A 数据/PIT/observation：`DESIGN_READY_USER_APPROVED`；C-007-A 数据、单位、PIT mapping 与 7/20 维公式未被 C-008-A 推翻。
- F-011-B fit/convergence/covariance：`BLOCKED_C008_B3_EXACT_NUMERIC_CONTRACT`；D3-D5 的全参数、阈值、score/tie-break 与 reproducibility contract 待用户确认。
- F-011-C semantic evidence/selection：`BLOCKED_C008_B3_SEMANTIC_EVIDENCE_CONTRACT`；hard authority 与单一 validation 语义保持，count/month/run/utility separation 阈值待用户确认，B2 不采用。
- F-011-D 两-family READY：`BLOCKED_DEPENDENCY`；当前 READY artifact 数为 0，legacy 无 seeds 42..49 的 31/31 结果。
- F-011-E generator/job/revision：`PENDING_IMPLEMENTATION`；不得由未完成的 model-set preparation 推导为 verified。
- F-012：advisory-only 写入与依赖隔离，不产生 Selection/Paper/QMT/QE/交易副作用。
- F-013：真实 read API、风险 UI、失败状态、可访问证据与 retrospective report。

## 19. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | `backend/db/init_hmm_risk_schema.py`; `backend/services/hmm_risk/{input_resolver,state_model_set,market_repository,observation,state_generator,alert_state_machine,repository,job_service,worker}.py`; `scripts/hmm_risk/run_daily_worker.py` | artifact: `F:/Dev/AIstock_worktrees/BUG-836-hmm-risk-fixed-seed-l1-preparation-cannot-label-20260722/tmp/validation/hmm_risk/c008_b1_soft_evidence.json`; `backend/tests/hmm_risk/test_state_model_set.py` | APPROVED_BY_USER_DIRECTION_ONLY_BLOCKED_EXACT_CONTRACT | 用户明确批准 B3 方向，不批准未验证的完整算法/阈值/selection 细节；实现、真实 selection 与两-family READY 均未完成 |
| F-011-A data/PIT/observation | `backend/services/hmm_risk/{market_repository,observation}.py`; C-007-A formulas | `backend/tests/hmm_risk/test_state_model_set.py`; artifact: `F:/Dev/AIstock_worktrees/BUG-836-hmm-risk-fixed-seed-l1-preparation-cannot-label-20260722/tmp/validation/hmm_risk/c008_seed_diagnostic.json` | DESIGN_READY_USER_APPROVED | 无 |
| F-011-B fit/convergence/covariance | `backend/services/hmm_risk/state_model_set.py`; `scripts/hmm_risk/prepare_state_model_set.py` | `backend/tests/hmm_risk/test_state_model_set.py`; artifact: `F:/Dev/AIstock_worktrees/BUG-836-hmm-risk-fixed-seed-l1-preparation-cannot-label-20260722/tmp/validation/hmm_risk/c008_b1_soft_evidence.json` | APPROVED_BY_USER_DIRECTION_ONLY_BLOCKED_EXACT_NUMERIC_CONTRACT | 用户明确批准 B3 train-only family-global 方向；B1 未应用正式阈值且缺 date-level/run/feature-scale evidence，具体参数/阈值/selection 细节未批准 |
| F-011-C semantic/selection | `backend/services/hmm_risk/state_model_set.py` preparation boundary | `backend/tests/hmm_risk/test_state_model_set.py`; artifact: `F:/Dev/AIstock_worktrees/BUG-836-hmm-risk-fixed-seed-l1-preparation-cannot-label-20260722/tmp/validation/hmm_risk/c008_b1_soft_evidence.json` | APPROVED_BY_USER_DIRECTION_ONLY_BLOCKED_SEMANTIC_EVIDENCE_CONTRACT | 用户明确批准 hard authority和原单一 validation 方向；不得新增 calibration/holdout split，count/month/run/utility gap 细节未批准；B2 不采用 |
| F-011-D two-family READY | content-addressed L1/L2 model-set artifact | `backend/tests/hmm_risk/test_state_model_set.py`; artifact: `F:/Dev/AIstock_worktrees/BUG-836-hmm-risk-fixed-seed-l1-preparation-cannot-label-20260722/tmp/validation/hmm_risk/c008_b1_soft_evidence.json` | APPROVED_BY_USER_BLOCKED_DEPENDENCY | READY artifact 数为 0；legacy seeds 42..49 无 31/31，B1 不改变该状态 |
| F-011-E generator/job/revision | `backend/services/hmm_risk/{state_generator,job_service,repository}.py` | `backend/tests/hmm_risk/test_state_generator.py`; `backend/tests/hmm_risk/test_revision_and_late_data.py` | APPROVED_BY_USER_PENDING_IMPLEMENTATION | 用户明确批准 C-008-D1：上游 READY model set 尚未形成，不推导 generator/job 已验证 |
| F-012 | `backend/services/hmm_risk/**`; DB role/write-scope guard; `backend/routers/hmm_risk.py` | `backend/tests/hmm_risk/test_isolation.py` | DESIGN_READY_USER_APPROVED | 无 |
| F-013 | `backend/routers/hmm_risk.py`; `backend/services/hmm_risk/report_service.py`; `frontend/src/app/hmm-risk/**`; `frontend/src/components/hmm-risk/**`; `frontend/src/lib/hmm-risk/api.ts` | `backend/tests/hmm_risk/test_api.py`; `backend/tests/hmm_risk/test_retrospective_report.py`; `playwright test frontend/tests/hmm-risk/hmm-risk.spec.ts` | USER_APPROVED_PENDING_UPSTREAM_MODEL_SET | 用户明确批准 C-008-D1：API/UI 合同未被否定，但真实验收依赖可证明的 READY model set |

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
| seed sensitivity 或 validation-driven seed picking | C-008-B3 方向要求预声明 schedule 全量运行、31/31 family-global train-only selection；selection 不接收 validation/future utility，禁止 per-sector 拼接或 semantic 失败后换 seed；schedule 与 score 待确认 |
| monitor converged 掩盖 likelihood decrease | monitor 与 absolute/relative delta 独立记录；正式 tolerance 与 failure/warning 语义待用户确认，未确认前保持 blocked |
| covariance clip 掩盖系统性 anomaly | raw/bounded covariance、mask/hash 与 per-state/per-feature evidence 必须保留；feature-scale floor/ceiling 与 anomaly budget 待用户确认 |
| hard occupancy 极低但仍 labelable | train 与单一 validation 分别记录 hard count、contiguous run/month coverage 和 utility separation；精确阈值待用户确认，不得以 1 个样本自动通过 |
| 未经确认拆分 validation 或增加 holdout | 保持批准的 `2024-07-01..2025-03-31` 单一 validation 与 fitted `startprob_` prior；任何 split/holdout 先明确业务语义并获确认 |
| 库默认值或浮点环境导致不可复现 | KMeans/HMM 全参数、依赖版本、BLAS/线程、序列化精度和重复运行容差进入待决合同；不得仅凭 seed 声称 deterministic hash |
| 诊断数值被写成正式 gate | B1 的 `formal_acceptance_thresholds_applied=false` 是硬边界；没有证据与用户确认的数值只作 proposed，不进入实现或 READY |
| autocycle-only 冒充两-family 完成 | F-011-D 要求所有已批准 family 完整；legacy 缺失时保持 blocked |
| 历史 mapping 的 industry/index code 双表示被随机选行 | classify 唯一规范化；等价 source rows 全量留 hash，非等价多映射 fail loud；禁止 `DISTINCT ON` |
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
4. Slice 1/2 在 DEV 运行 fixture、真实只读 market input、人工 job 和 bounded worker；只写 DEV `hmm_risk.*`。
5. Slice 3 在安全端口完成真实 API/UI acceptance；未通过前不切 `/hmm`。
6. 每个后续源码 PR 均在 merge 前停止等待用户确认；源码合入后，production DDL 仍须独立目标授权、migration 和 readback。
7. production 首次 manual worker/API activation 再独立授权；不自动启动 scheduler。

### 21.2 Rollback

- DDL transaction 失败自动回滚；成功后不 DROP 历史表，使用 forward-fix。
- runtime rollback 停止 manual worker/禁用 route activation，不删除 state/alert/event/report/history。
- UI rollback 恢复 `/hmm-evolution` 默认入口，不伪造风险页成功。
- generator/rule 新版本以新 identity 运行；旧 revision/report 保留，不原地重写。
- legacy v1 producer/consumer 完全冻结；Phase 2 rollback 不触碰其文件、artifact 或 runtime 接线。

## 22. Production Gates

- 本设计 PR：`production_ddl_gate=noop`。
- 本设计 PR：`production_frontend_dependency_gate=noop`。
- 本设计 PR：`production_backend_dependency_gate=noop`。
- 本设计 PR：`production_runtime_activation_gate=noop`。
- `sector_data` identity DDL/DML：`noop`，生产表保持 fact-only，行业 mapping 动态解析。
- 未来 schema implementation：DEV `applied_and_verified` 后，production DDL 仍为 `pending`，需要目标明确授权。
- 未来 C-008-B3 implementation：当前环境存在 `hmmlearn==0.3.3`，但仓库 `requirements.txt` 未声明该依赖；实现 PR 必须
  先提交并验证明确版本。合入后、运行时激活前 `production_backend_dependency_gate=pending`，直至获得独立安装授权并完成
  import/version smoke；不得把当前文档 PR 的 `noop` 误报为未来实现也无需依赖处理。
- 未来源码合入不等于 API/UI/worker 激活；首次 production manual worker run 单独授权。
- Phase 2 scheduler：未批准、未实现、未启用。

## 23. DESIGN-COMPLIANCE-001 预审

- no_simplified_delivery：五张持久表/current views、全 candidate evidence matrix、direct L1/L2、唯一 generator、job/revision、API、真实 UI 与 confirmed report 均为完成边界；未决项不以子集、默认或静态页代替。
- no_silent_error：candidate/model/watermark/mapping/sector/L1/persistence/renderer 全部有 reason code；partial 不标 success；
  C-008-B3 将 initialization/fit/monitor/likelihood/covariance/occupancy/selection/semantic validation/family 状态分别持久化，
  并为 count/month/run/gap 等失败预留最具体 reason；任一失败不得压缩或静默推导 READY。
- no_business_semantic_drift：预警 severity 保持父设计；C-001-A capability、C-002-A direct model set、C-003-A oracle、
  C-006-A fact/universe/mapping 分层与 C-007-A stock-fact-first observation 均有用户明确批准；C-008-B3 保持 hard semantic
  authority、原单一 validation 和 fitted `startprob_` prior，B2 明确不采用；删除未经确认的 calibration/holdout split 与阈值。
- no_unrequested_gate_or_approval：未获确认的 B3 数值、occupancy、selection、split/holdout 不进入 active contract；未来确认的
  确定性模型合同不是运行时人工审批。preview 不是批准步骤，普通 read 无确认；只保留规范要求的 production DDL/dependency/
  runtime 独立授权和用户要求的逐 PR 合入确认。

## 24. 当前完成状态与下一步

本文件已回填 C-001-A/C-002-A/C-003-A/C-006-A/C-007-A/C-008-D1/C-008-B1 用户批准，并完成 C-008-B3-DESIGN
正式审核修复。C-008-A/C-008-B1 均为 `VERIFIED_DIAGNOSTIC_ONLY_NO_SELECTION_NO_ARTIFACT`；C-008-B2 为
`NOT_APPROVED`；C-008-B3-DESIGN 为 `DIRECTION_APPROVED_EXACT_CONTRACT_PENDING_USER_CONFIRMATION`。F-011 parent 当前为
`BLOCKED_C008_B3_EXACT_CONTRACT_DECISIONS`，F-012 保持 `DESIGN_READY_USER_APPROVED`，F-013 为
`PENDING_UPSTREAM_MODEL_SET`。本文不使任何 model set READY，也不授权 B3 实现。

生产 `sector_data` 不执行 identity DDL/DML；当前设计修订未安装依赖、未启停服务、未运行 job、未写数据库，也未激活
Phase 2 runtime。下一步先对 4.3.2 D3-D6 的精确算法、阈值、selection 与可复现性合同作用户决策；决策未完成前不得
启动 B3 源码实现。后续实现及其 PR 合入均不由本设计提交自动授权。
