# HMM Evolution Phase 2 风险监控与预警 F2 实现级详细设计

- 文档类型：F2 从属实现级详细设计 / Feature Card
- 日期：2026-07-22
- 修订日期：2026-07-31
- 状态：`B3_FORMAL_EXECUTED_BLOCKED_REMEDIATION_DIAG02_EXECUTED_D1_PROPOSED_NOT_IMPLEMENTATION_READY`
- 父级权威：`docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md` v2.14
- 上游权威：`docs/architecture/hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md` v2.8
- Feature tier：F2
- Design Acceptance Index：F-011、F-012、F-013
- 当前边界：C-001-A/C-002-A/C-003-A/C-006-A/C-007-A/C-008-D1/C-008-B1、B3 structural 与 D3-D7 精确合同均已按历史用户决定固化；Slice 0 B3/L2 retrain、C-009 与 C-010-FORMAL-A 源码均已合入。producer `e2c01bae156281d551b084156fec4a09ed5a84ee` 已在冻结 dataset/mapping、两个 family、seeds 42..49 和固定单线程 Conda `AIstock` 数值环境完成两次 fresh-process 正式制备，共 `5184/5184` fits；D5 已按 train-only 合同执行，未读取 validation/future utility，selection 后未 refit。正式 receipt canonical SHA-256=`e7992f87fb555eb26d6c2ef1ad9d45863954edd83fbfcc39f5ae01765cf3939f`，最终 `status=blocked`：只有 `legacy_covfix:L1` 选中 seed 43，且在 D6 因 `801980.SI` 失败；其余三个 family/level 均无 D5 eligible candidate。两 family均 blocked，`ready_artifact_write_performed=false`、model/READY 数量为0、数据库与 runtime均未变化。随后 `C-008-B3-FORMAL-BLOCKER-DIAG-01` 完成348/348 targeted fits与3-entry D6 no-refit replay；`C-008-B3-REMEDIATION-DIAG-02` 又完成324/324 train profiles、163/163 completed entries与11/11 initialization sources的no-fit闭合，producer `b2456424b859f1635635129aa6a826a677f4fdec`、canonical SHA-256=`48157a4255e9d19b814b26b90b18ec38769e28fd0a18e58403edb83fc660bb58`。当前D1-A/B、32-fit controlled-refit与D5 19/20维comparability均为proposed；不批准新的模型、优化器、threshold或acceptance，不得运行新fit、selection、D6、model/READY或生产动作。

本文只细化总体蓝图已批准的 Phase 2。它不建立第二套产品方向，不修改 Selection、Advisory、
Paper v2、MiniQMT、StrategyPackage、QE 或现有 `hmm_risk_gate_v1` 消费者的业务语义。
Phase 2 的输出是研究分析事实，不是交易门禁、可买性、调仓或模型晋级结论。

## 0. Feature Card / 功能卡

### 0.1 用户结果

用户可以在 `/hmm-risk` 查看指定 HMM candidate 在最新共同完成交易日的申万 L1/L2 状态、
最近 7 个完整交易日热力图、今日预警、固定详情、事件生命周期和版本化回测报告。所有页面事实来自
`hmm_risk.*` 的真实 API；数据缺失、输入漂移、部分失败和 renderer 错误均显式展示。

### 0.2 成功边界

- F-011：唯一 versioned sector-state generator、共同水位、revision/dedupe、预警状态机和迟到数据重算完整；其 direct L1/L2 model-set preparation 源码已实现并通过本模块 required plan。实际双 fresh-process 训练已完成 5184/5184 fits，D5/D6 已按批准合同执行但 fail closed，当前为 `FORMAL_EXECUTED_BLOCKED_NO_READY`。不得以 fit 完成、单个 `legacy_covfix:L1` selected artifact、单 family、部分 sector、historical diagnostic score、放宽阈值或 validation-picked seed 冒充完成。
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
合同。D4-03-B 随后由用户独立批准；D4-01-A 由以下独立合同批准，不得从 D3-03-A 的 `tol` 隐式推导：

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
- `tol=0.01` 是 GaussianHMM fit 的显式停止参数，不推导 D4-01 likelihood acceptance。monitor、absolute/relative delta
  与 terminal/non-terminal 语义由已批准的 D4-01-A 独立判断；
- fit 后禁止 covariance/transmat 或其他参数 projection/clip；startprob、transmat、means、raw covariance、`R_sj`、prior、
  KMeans labels/counts、全部显式参数、依赖/线程环境与 algorithm version 必须进入 canonical receipt/hash。

##### D4. 独立数值验收合同：D4-01-A/D4-02-A/D4-03-B 已批准

fit、monitor、likelihood、covariance、train occupancy 必须是独立状态；`monitor_converged=true` 不推导 likelihood、
covariance 或 train occupancy 可接受。D4-01-A 独立定义 convergence/likelihood，D4-02-A 独立定义 covariance，
D4-03-B 独立定义 train-only hard occupancy/run/transition evidence；任一状态通过都不能覆盖其他状态失败。

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

D4-01-A 于 2026-07-25 获用户明确批准，正式 convergence/likelihood acceptance 合同如下：

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
   fallback。fit 后分别执行 D4-01-A likelihood、D4-02-A raw covariance 与 D4-03-B train occupancy 合同；monitor、
   likelihood、covariance、train occupancy 状态独立持久化，任一失败不能被其他状态覆盖。
5. **selection/semantic 不前置**：D3/D4 fit 阶段必须记录
   `validation_accessed=false/future_utility_accessed=false`。只有同一 restart 的 131/131 L2 全部通过 D3/D4，才可成为
   L2 family candidate；本设计不执行 selection。D5-01-B 固定每个family分别选择L1/L2 level-global seed，L2只使用131-entry
   train-only min/median/mean lex receipt，不与L1 31-entry vector合并，也不得由L1 score推导L2 seed。
   selected identity冻结后才允许在唯一validation上执行D6-01-B hard semantic mapping；D6失败不得回到D5换seed。B2 soft authority、
   hidden-state index、neutral/fixed fallback 和 per-sector stitching 全部禁止。
6. **逐 entry immutable receipt**：至少保存 family/version、seed/schedule index、L2 code、ordered observation dates、
   training row count、observation/preprocess/reference hashes、KMeans identity/cluster evidence、完整 GaussianHMM parameter
   identity、dependency/numeric/thread environment、完整 monitor history/iteration/convergence、逐 delta D4-01 evidence、
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
10. **授权边界**：D4-03-B、D5-01-B与D6-01-B均已获用户精确确认。B3/L2 retrain源码和 Conda `AIstock` 依赖安装已分别完成；
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

##### C-010-FORMAL-A. 正式 feature-domain contributor 与逐 feature cross-section 政策（源码已实现，formal preflight pending）

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

D4-01-A 与 D4-02-A 分别解决 convergence/likelihood 和 covariance validity/acceptance；它们都不推导 train occupancy、
selection、semantic evidence、family completeness 或 READY。DIAG-04 本身仍为
`VERIFIED_DIAGNOSTIC_ONLY_NO_SELECTION_NO_ARTIFACT`，不得反写为已执行正式验收。

D4-03-B 于 2026-07-25 获用户明确批准，正式 train occupancy acceptance 合同如下：

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

D4-03-B 的只读 sensitivity 不是正式 acceptance：DIAG-02 下 legacy 238/248 records 通过但 0/8 seed 达到 31/31，失败集中于
`801780.SI` 和 seeds 42/43 的 `801130.SI`；autocycle 248/248、8/8。DIAG-04 下两个 family 均为 248/248、8/8；2% 候选也
全部通过，而 5% 候选使 legacy 仅 236/248 且 0/8 完整 seed。选择 1% anti-singleton 合同的语义是阻止 singleton/单月/单 run
被当作完整训练结构，同时不在 D4 阶段用未经批准的 semantic significance 淘汰 rare regime。历史 artifact 继续标记
`formal_acceptance_thresholds_applied=false`；未来实现和受控 L2 重训必须从 immutable receipt 重新执行 D4-03-B。

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
1. **冻结窗口与 causal prior**：validation 严格使用 `2024-07-01..2025-03-31` 的 immutable ordered 182 个 observation
   rows；首日从 selected fitted `startprob_` 开始 causal filtering，不继承 train 最后一日 posterior。ordered dates、calendar、
   source cutoff、缺失/无效日、row count 与 content hash 必须与 frozen InputManifest 一致；缺失或漂移时
   `insufficient_evidence`，不得缩短窗口或按剩余行继续；
2. **future utility authority**：逐 validation row 固定
   `y_t=0.35*excess_return_5d+0.35*excess_return_10d+0.30*excess_return_20d`。三个 component 与 `y_t` 必须 finite，
   calendar、单位和 benchmark identity 沿用 C-007-A 已批准语义，不选择新的 benchmark；source cutoff/common watermark 固定
   `2025-04-30`。ordered component vectors/hash、combined utility vector/hash 和 formula version
   全部进入 receipt。future utility 仅用于 D5 完成后的 offline semantic mapping，不进入 observation、fit、posterior 或 selection；
3. **posterior 与 hard assignment**：selected raw fitted parameters 生成的 causal posterior shape 必须严格为
   `(N_validation,3)`；所有 cell finite 且非负，每行 sum 的最大绝对误差 `<=1e-12`。每行 top1-top2 margin 必须严格
   `>1e-12`，小于或等于边界均 fail closed；hard state 为逐行 `argmax`，不得以 state index、stable-sort、soft mass 或默认值
   打破 tie；
4. **anti-singleton evidence**：对每个 hidden state `s`，令 `C_s=count(argmax_t=s)`，必须同时满足
   `C_s>=max(5,ceil(0.02*N_validation))` 与 `C_s/N_validation>=0.02`；当前 `N_validation=182` 时 count threshold 为 `5`。
   每 state calendar-month coverage `>=2`、contiguous run count `>=2`、incoming/outgoing state-change transition count
   分别 `>=2`、maximum single-run share `<=0.9`。run/transition 按 immutable observation row sequence 计算；自转移不计入
   incoming/outgoing，date gap 仍由 input coverage 独立展示，不能虚构 transition；
5. **hard utility 与 mapping**：每 state 的 hard utility count 必须等于 `C_s`，mean 与 ddof=1 sample variance 必须 finite。
   按 mean 从低到高映射 `fading/neutral/trending`，hidden-state index 不具有 semantic 含义。对相邻 ordered utilities
   `U_a<U_b`，要求
   `U_b-U_a > max(1e-12,32*eps64*max(1,abs(U_a),abs(U_b)))`，其中 `eps64=np.finfo(np.float64).eps`；恰等于边界也失败。
   standard error、95% separation、posterior mass/ESS/entropy 和 posterior-weighted utility 只作 diagnostic，不参与 pass/fail；
6. **状态闭包**：`semantic_assignment_status` 与 `semantic_evidence_status` 分别只允许
   `accepted/failed/insufficient_evidence`。`semantic_assignment_status` 只由 validation dates、posterior 与 hard-assignment
   identity/numeric contract 决定：receipt 缺失或 hash 不可验证为 `insufficient_evidence/semantic_assignment_valid=false`，
   证据存在但第 1/3 项失败为 `failed/false`，全部通过才为 `accepted/true`。`semantic_evidence_status` 以 assignment accepted
   为前置，只由 coverage 与 future-utility identity/evidence 决定：receipt 缺失或 hash 不可验证为
   `insufficient_evidence/semantic_evidence_valid=false`，第 2/4/5 项任一失败为 `failed/false`，全部通过才为 `accepted/true`。
   utility 缺失不得把有效 hard assignment 伪写为 failed，但两者未全部 accepted 前禁止写 semantic mapping。D6 accepted 不推导
   model entry、level/family completeness 或 READY；
7. **reason 与优先级**：保留全部适用 failure/blocking，不能 first-failure 丢失可安全遍历证据。primary reason 依次为
   evidence missing、date sequence invalid、posterior invalid、normalization、posterior tie、hard state missing、state count、
   occupancy、month、run、transition、run concentration、utility non-finite、utility variance non-finite、utility tie/gap。
   使用第 12 节最具体 stable reason code，family/level receipt 聚合全部 code、count 与 evidence hash；
8. **immutable receipt**：逐 entry 保存 contract version/hash、level/family/selected seed/sector、selected model parameter hash、
   validation dates/calendar/source/hash、posterior/hard assignment identity、future utility component/combined identity、逐 state
   count/ratio/month/run/incoming/outgoing/max-run-share/utility mean/variance、相邻 gap/tolerance、diagnostic SE、semantic mapping、
   comparison results、failure/blocking arrays、primary reason、status/valid mapping、producer/dependency/numeric environment；
9. **历史敏感性不是选择**：DIAG-04 上，最低存在性方案会把两个 family 的 496/496 records 全部视为可继续，不能阻止
   singleton；D6-01-B sensitivity 为 legacy `244/248` records、4/8 complete seeds，autocycle `242/248`、2/8 complete seeds；
   加 95% significance 后 legacy 为 `0/248`、autocycle 为 `5/248` 且两个 family 都无完整 seed。上述 seed 集合不得写入
   selection receipt 或用于 D5；D6-01-B 选择 anti-singleton 与 numeric strict ordering，不新增 significance research gate。
   DIAG-04 没有受控重训 L2 的 D6 evidence；未来 L2 必须在 D5 selected identity 冻结后逐 131 entry 正式执行本合同，不能
   复制 L1 sensitivity 或既有 L2 final parameters。

##### D7. model identity、依赖与 READY

最终 B3 algorithm identity 至少必须包含：D2 窗口与 causal prior、经批准的 restart schedule、KMeans/HMM 全参数、preprocess、
likelihood/covariance/occupancy/semantic 阈值版本、selection formula/tie-break、全部候选摘要、selected family identity、
validation mapping/receipt、运行库/数值环境和全部 input hashes。

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

上述 decision 当前统一为 `PROPOSED_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY`。在任何精确合同获批前，F-011-B/C/D
保持 blocked，不运行新的完整 grid、不产生 candidate/selection/model/READY。获批后的验证也必须先执行受控诊断验证模型机制，
正式 grid仍沿用既有执行授权边界；不得把本诊断的348 fits复用为正式 acceptance。

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
正reference prior仍属于另一个模型机制，不得与inactive dimension捆绑实施。该推荐是待用户确认的设计决策，不是新增模型gate、
自动淘汰feature或人工runtime审批；在获批前不实施B3、不运行新fit、不选择seed、不生成model/READY。

#### I. C-008-B3-REMEDIATION-D1：sector-local 常量维模型 identity 精确设计

状态：`PROPOSED_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY`。本节只形成精确设计选项与受控 refit 合同；不表示用户已选择
方案、不修改源码、不运行 HMM、不执行 D5/D6、不生成 model/READY。权威证据仅为 REMEDIATION-DIAG-02 producer
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

- `C-008-B3-REMEDIATION-D1-A`：保持现有fail-closed。优点是model identity、D5 score与runtime parser完全不变；代价是
  `autocycle_all_core:L2/801207.SI`在当前冻结数据和8个restart上持续无法进入fit，family仍blocked。该方案没有伪成功，
  但无法验证常量维显式投影是否能保留其余19维信息。
- `C-008-B3-REMEDIATION-D1-B`：采用显式inactive-dimension model identity。推荐为
  `RECOMMENDED_PENDING_USER_APPROVAL`，但只授权后述controlled-refit，不授权formal grid。该方案保持完整20-feature observation与
  preprocess contract，在model likelihood之前使用显式、hashed projection选择19个active dimensions；不加noise/floor，
  不伪造inactive covariance，不把hidden-state或hard semantic语义改为soft authority。

D1-B的主要false-accept风险是：19维模型可能在train numeric contract上通过，但被排除的feature在未来重新出现信息量；因此model
identity必须永久记录inactive维，runtime不得动态激活。主要false-reject风险是：严格exact-zero allowlist只修复当前已证明identity，
不会自动处理未来其他真实常量维。该保守边界用于保持因果可归因，不是研究方向淘汰gate。

**3. D1-B projection 算法。** 算法版本固定为`hmm_risk_c008_b3_d1_inactive_dimension_v1`：

1. 从冻结train input构造完整20维C-order little-endian float64矩阵`X_raw`，验证shape、feature order、dates、dataset/mapping/calendar、
   direct-L2与C-010 formula/provenance identity；任何non-finite或hash漂移失败。
2. 使用既有批准的`autocycle_all_core` full-20 preprocess identity计算`X_pre20`；不得先删除第20维再重新估计winsor/center/scale。
3. exact-zero资格要求index 19的raw与preprocessed向量均finite、variance_ddof0精确为0、unique bit-pattern count为1、所有raw值
   精确等于`+0.0|-0.0`，且profile/formula/provenance hashes与DIAG-02 exact receipt一致。`-0.0`只在value identity归一为`+0.0`，
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

未来formal grid若采用D1-B，`801207.SI`的D5 sector score分母存在必须单独确认的comparability问题：既有批准公式为
`LL_final/(N*d_family)`且autocycle family的`d_family=20`；改为`d_effective=19`会改变D5合同，继续使用20又可能使19维likelihood与
其他20维sector不可直接比较。因此登记`C-008-B3-REMEDIATION-D1-D5-COMPAT-01=PROPOSED_PENDING_EVIDENCE_AND_USER_DECISION`。
controlled-refit固定`selection_performed=false`，不得通过本节私自选择19或20、不得执行D5，也不得因该gap把局部fit写成
implementation ready。

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
- `hmm_risk_model_inactive_dimension_d5_comparability_unresolved`；
- 既有最具体的initialization/monitor/likelihood/covariance/train-structure reason codes。

不得把exact-zero写成普通missing data，不得把projection失败压缩成generic fit failure，也不得把control drift或repeat mismatch降级为warning。

**8. 验证计划与可合入边界。** 未来D1-B源码PR至少验证：

- exact allowlist、raw/preprocessed zero、`-0.0`、nonzero constant、near-zero、non-finite、source/hash drift正反例；
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

D1详细设计只有在用户明确选择A或B并批准相应精确合同后才能从proposed转为design-ready。选择B也只使controlled-refit
implementation-ready；D5 comparability未闭合前仍禁止formal grid/selection/model/READY。该限制是既有D5业务语义的准确边界，
不是新增runtime人工审批。

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
- `hmm_risk_model_posterior_tie`
- `hmm_risk_model_restart_family_incomplete`
- `hmm_risk_model_restart_schedule_incomplete`
- `hmm_risk_semantic_hard_state_missing`
- `hmm_risk_semantic_evidence_insufficient`
- `hmm_risk_semantic_validation_evidence_missing`
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

C-008-B3 的 D4-01-A/D4-02-A/D4-03-B/D5-01-B/D6-01-B reason code 已进入批准的设计合同，但在 B3 源码实现前不代表历史 diagnostic record 已执行
正式验收。未来实现必须使用最具体 reason code；`hmm_risk_model_likelihood_terminal_decrease_warning` 是可继续后续独立
验收的显式 warning 状态，不是 failure，也不得被压缩为普通 success。历史 diagnostic score不得提前触发正式 selection；
initialization、likelihood、covariance、occupancy、validation evidence 或 family selection 失败不得压缩为 generic incomplete。

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
- D3-03-A/D4-02-A fix-point 必须覆盖：sector-local `R_sj` shape/finite/strict-positive；raw cluster zero variance 经
  `ν=1.0` 公式收缩但不 clip；完整 GaussianHMM 显式参数/identity；raw covariance shape/non-finite/non-positive；
  `L/U` 的 0.5% 闭区间边界内、恰好等于边界和越界；total/per-state/per-feature zero anomaly budget；M-step residual
  恰好等于 2% 和超过 2%；任一 denominator 非正或中间值 non-finite；禁止 post-fit clip/projection；smoothed posterior
  只用于 train covariance audit且不得进入 hard semantic/selection；`accepted/failed/insufficient_evidence` 到
  `covariance_valid` 的精确映射；receipt parser/hash 回读。
- D4-01-A fix-point 必须覆盖：monitor false；`iterations=300`；history 长度小于 2、与 iterations 不一致或包含 non-finite；
  任意幅度的 non-terminal negative delta；terminal positive delta 的 `nextafter(0.01,0)`、`0.01` 与大于 `0.01`；
  terminal negative relative delta 的 `-2e-5` 闭边界和略低于边界；previous likelihood 为 0/接近 0 时
  `max(1.0,abs(previous))` denominator；`accepted/accepted_with_warning/failed/insufficient_evidence` 到
  `likelihood_valid` 的精确映射；多 failure + blocking + warning 的数组聚合、确定性顺序、状态优先级与 primary reason；
  warning receipt/aggregate 不得丢失；threshold version/hash 回读；D4-01 warning 不得覆盖 D4-02/D4-03 或后续
  selection/semantic failure。
- D4-03-B fix-point 必须覆盖：train posterior shape 与 `N_train=0`；cell non-finite/negative；row-sum error 恰好
  `1e-12` 和略高于边界；top1-top2 margin 恰好 `1e-12` 和略高于边界；
  `max(5,ceil(0.01*N_train))` 在整数/非整数 1% 边界的 count 与 ratio 分离；month count 2/3；run count 2/3；incoming/
  outgoing 1/2；maximum run share 恰好 0.8 和略高于 0.8；ordered date/hash 缺失、重复、逆序和 canonical date gap evidence；
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
- D6-01-B fix-point 必须覆盖：仅在 D5 selected identity 冻结后执行，validation/future utility 不得回流 selection；
  exact 182-row date/calendar/source/hash 与首日 fitted `startprob_` causal prior；posterior shape、non-finite/negative、row-sum
  `1e-12` 闭边界和 top1-top2 margin `1e-12` 开边界；future 5/10/20D component/combined utility identity；
  `max(5,ceil(0.02*N_validation))` 的 count 与 2% ratio 分离；month/run 1/2、incoming/outgoing 1/2、max-run-share 0.9
  闭边界；hard utility count/mean/ddof=1 variance；numeric gap 的等于/略高/略低边界；SE/95% separation/soft mass/ESS 只作
  diagnostic；assignment/evidence status 与最具体 reason 的确定性聚合；D6 failure 不得返回 D5 换 seed、refit、换 family 或
  拼接 sector；D6 accepted 不得推导 family/READY。DIAG-04 sensitivity 只能证明区分性，历史 flags 不得反写为正式 acceptance。
- L2 numeric provenance fix-point 已由 C-008-B3-D4-L2-AUDIT-01 完成：13/13 candidate snapshot 收敛为两份 exact SHA；
  两者均缺 immutable D4-01 history，且 262/262 entry 明确记录 post-fit covariance 修正。测试必须固定
  `likelihood_status=insufficient_evidence`、`covariance_status=failed`、primary/secondary reason 的优先级与 9+4 identity
  聚合；不得因 snapshot/job=`completed`、final logprob 或 metadata hash 存在而转为 accepted。
- L2 retrain contract fix-point 在未来实现获授权后必须覆盖：冻结 dataset/mapping/watermark/window 与 canonical 131 set/hash；
  两 family × seeds 42..49 × 131 sectors 的 `2096 fits/process` 完整性；两个 fresh process 的 `4192 fits` 与 bitwise
  receipt/model hash；D3-02-B/D3-03-A、D4-01-A/D4-02-A/D4-03-B 的逐 entry 状态和完整 receipt；D5 前 validation/future utility 不可见；
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
| C-008-B3-D3-03 | GaussianHMM 全参数和 sector-local initialization covariance 精确数值 | `RESOLVED_USER_APPROVED_D3_03_A` | sector-local `R_sj`、`ν=1.0` shrinkage initialization/prior、`covars_weight=2.0`、`min_covar=0.0`、完整显式 GaussianHMM profile 与禁止 pre/post-fit clip/projection 已批准；likelihood acceptance 由独立批准的 D4-01-A 判断 |
| C-008-B3-DIAG-02 | 是否在固定数值环境按批准结构运行两次完整只读结构诊断 | `VERIFIED_DIAGNOSTIC_ONLY_NO_SELECTION_NO_ARTIFACT` | 992 fits、两次 canonical payload hash 相同；补齐 likelihood、covariance、month/run/transition/occupancy evidence，未执行正式 D4/D5-01/D6 |
| C-008-B3-D4-01 | convergence/likelihood exact tolerance 与 warning/failure 语义 | `RESOLVED_USER_APPROVED_D4_01_A` | `hmm_risk_c008_b3_d4_01_a_v1`：monitor/history 独立完整性；non-terminal negative fail；terminal positive `<0.01`；terminal negative relative `>=-2e-5` 为持久化 warning、低于边界 fail；不得自动放宽或把 warning 静默成普通 success |
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
| C-010-FORMAL-A | 是否采用 full-universe train-frozen contributor ledger、price/moneyflow 双层 coverage、同源 moneyflow denominator 与逐 feature `0.90` cross-section 的正式政策 | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_FORMAL_PREFLIGHT_AND_GRID_EXECUTED` | 用户于 2026-07-28 明确批准；version=`hmm_risk_c010_feature_domain_policy_v1`。clean-main formal preflight 已通过且正式 5184 fits 已绑定 policy SHA `ae8eda5b…220d9` 完成；当前 blocker 位于 D3/D4/D6 模型验收，不是 C-010 coverage/policy pending。仍未生成 READY |
| C-010-A1 | 是否以 full-universe `O_s/A_s` ledger 冻结 train-derived moneyflow contributor eligibility | `RESOLVED_USER_APPROVED_C010_A1` | 每个 train opportunity symbol 显式入账，`A_s ⊆ O_s`，availability `>=0.90` eligible；train 后新证券保留业务/price eligibility但 moneyflow=`train_eligibility_unavailable`，不得默认 eligible或删除证券 |
| C-010-A2 | 是否采用 price/moneyflow 两层 count+weight coverage 与同一 moneyflow contributor set 的 numerator/denominator | `RESOLVED_USER_APPROVED_C010_A2` | price denominator 不受 moneyflow exclusion影响；moneyflow expected set仅含 A1 eligible contributor，exact five fields固定，count/weight均`>=0.90`；price/moneyflow weight denominator invalid 独立 fail closed；四项 moneyflow ratio使用严格正的`moneyflow_contributor_amount`，不填值或借用`l1_amount` |
| C-010-A3 | 是否把 exact-complete 全局传播改为按 feature domain 独立的 `0.90` cross-section | `RESOLVED_USER_APPROVED_C010_A3` | 每个 feature/date/level保存 expected/valid/missing sector set/hash及exact operator/reference；达到0.90只对valid set计算、缺失sector保持NA，不足仅该feature/date invalid；L1/L2最小valid count为28/118；reference invalid、output non-finite、pre/post mask mismatch独立fail closed，rolling不得复活当前invalid date |
| C-010-A4 | 是否升级 formula/policy identity并以601日formal preflight绑定后续B3执行 | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_FORMAL_EXECUTION_VERIFIED` | feature set/order/rolling不变；formula=`hmm_risk_l1_sector_factor_formula_v2_c010`。601日 formal preflight、request、两个 child、D4/D5/D6 receipt 已使用同一 policy identity；fit 前 coverage 条件通过。READY 因下游模型验收 blocked，不能倒推 A4 失败或重新启用 v1 |
| BUG-892 | PIT entry day 的 causal `circ_mv` 为什么形成结构性 denominator failure | `SOURCE_REVIEW_FIX_VERIFIED_DENOMINATOR_COMPLETE_DOWNSTREAM_TRAIN_COVERAGE_BLOCKED` | producer `77265dd6...` 的 601 日 no-fit receipt `7c36f228...fdd1ca`：crossing total/available/invalid=`1073/1073/0`，history start=`2020-07-30`，ordered key hash=`0b89a9d5...53c8f19`；PIT-entry denominator 根因闭合。整体 preflight 仅因既有 train observation coverage blocked（legacy L2 `801881.SI=102<120`、autocycle L2 coverage）而保持 blocked；未执行 fit/selection/D6/model/READY/DB/runtime action，且不改变 PIT、return 或 hard semantic 语义 |
| BUG-870 | formal preflight 是否在grid前闭合四个family/level的train coverage并持久化child typed failure | `SOURCE_FIX_MERGED_CURRENT_FORMAL_PREFLIGHT_PASSED` | 完整 coverage preflight 与 typed child failure 已合入；当前 clean-main request 通过该 fail-closed gate并运行正式 grid。历史首-fit前 blocker仍保留为旧 receipt，不再代表当前执行状态 |
| C-008-B3-D4-02-DIAG-03 | 是否仅重聚合 sector-local covariance reference 与候选 bounds sensitivity | `VERIFIED_DIAGNOSTIC_ONLY_NO_REFIT_NO_SELECTION_NO_ARTIFACT` | canonical report `22ee3536b4dc6590c27fa6c2989bc830d3d5d336e71b193fd17801d7c62a7e43`；统一 `[1e-4,200]` 被证据否定，未批准替代 bound |
| C-008-B3-D3-03/D4-02-DIAG-04 | 是否用 scale-aware initialization/prior 在固定环境执行两次完整 refit 诊断 | `VERIFIED_DIAGNOSTIC_ONLY_NO_SELECTION_NO_ARTIFACT` | producer `94abea6c...`；992 fits；payload hash `3abb384e...19aac` bitwise equal；report canonical `2c9136d5...74c9b`；无正式 acceptance、selection、model/READY/DB/runtime write |
| C-008-B3-D4-02 | covariance reference/bounds/floor/anomaly budget | `RESOLVED_USER_APPROVED_D4_02_A` | dynamic `L/U`、`τ_bound=0.005` 闭区间、tolerance 后 total/per-state/per-feature zero anomaly、M-step residual `<=0.02`、raw-only posterior 与禁止 clip/projection 已批准 |
| C-008-B3-D4-03 | train hard occupancy/month/run/transition acceptance | `RESOLVED_USER_APPROVED_D4_03_B` | `hmm_risk_c008_b3_d4_03_b_v1`：causal train hard authority；每 state count `>=max(5,ceil(1%*N))`、occupancy `>=1%`、month/run `>=3`、incoming/outgoing `>=2`、max-run-share `<=0.8`、row-sum error `<=1e-12`、margin严格 `>1e-12`；historical DIAG 不反写正式 acceptance |
| C-008-B3-D5-01 | train-only family/level-global identity、score/aggregation/tie-break | `RESOLVED_USER_APPROVED_D5_01_B` | `hmm_risk_c008_b3_d5_01_b_v1`：每family分别选择L1 31/31与L2 131/131 level-global seed；`L_final/(N*d)`；min/median/`math.fsum` mean lex maximize；relative+absolute tolerance逐维过滤，最终按schedule index；validation/D6不可见、D6失败不得reselection；historical DIAG不写selection |
| C-008-B3-D5-02 | 固定数值环境内的可复现性 | `RESOLVED_USER_APPROVED_D5_02_B_FIXED_ENVIRONMENT` | 两个 fresh process canonical hash 必须 bitwise equal；不外推跨 host/BLAS/依赖版本 |
| C-008-B3-D6-01 | hard semantic validation count/month/run/utility gap | `RESOLVED_USER_APPROVED_D6_01_B` | `hmm_risk_c008_b3_d6_01_b_v1`：selected restart 后的 hard authority；每 state count `>=max(5,ceil(2%*N))`、occupancy `>=2%`、month/run `>=2`、incoming/outgoing `>=2`、max-run-share `<=0.9`、posterior row-sum `<=1e-12`、margin严格 `>1e-12`；hard utility mean/variance finite、numeric adjacent gap；95%/soft evidence只诊断，失败不得换 seed |
| C-008-B3-D7-01 | B3 runtime dependency identity | `RESOLVED_USER_APPROVED_D7_01_A` | 未来实现声明 `hmmlearn==0.3.3`；本 docs-only PR 不安装依赖，未来 production dependency gate 独立 pending |
| C-008-B3-FORMAL-EXEC-01 | 已批准 B3 合同在当前冻结输入上是否形成两-family READY | `VERIFIED_FORMAL_EXECUTION_BLOCKED_NO_READY` | producer `e2c01bae…` 完成 5184/5184 fits；formal canonical `e7992f87…39f`。D5 只选出 `legacy_covfix:L1/seed=43`，该 level 又在 D6 因 `801980.SI` failed；其余三个 family/level 无 eligible candidate。两 family blocked，selection未读validation/future utility，selection后未refit，model/READY/DB/runtime write均为false |
| C-008-B3-FORMAL-BLOCKER-DIAG-01 | 是否按 formal rejection summaries 对全部 blocker pair 与 deterministic controls 执行两 fresh-process 定向根因诊断 | `VERIFIED_DIAGNOSTIC_COMPLETE_NO_SELECTION_NO_READY` | producer `ac3687c2…`；artifact canonical `10287e84…cffe8`；150 rejected+24 controls、348/348 fits、3-entry D6 no-refit replay闭合，两次payload hash bitwise相同；不选择seed、不改阈值/authority、不写model/READY/DB/runtime |
| C-008-B3-REMEDIATION-DESIGN | blocker diagnostic后 initialization/likelihood/covariance/train structure/D6 temporal evidence 的模型修订合同 | `PROPOSED_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY` | 诊断证明多阶段、多family/level根因；尚不足以批准具体模型、optimizer、prior、transition/dwell或threshold。五类合同必须分别给出精确公式、成本、false accept/reject与验收证据后由用户确认 |
| C-008-B3-REMEDIATION-DIAG-02 | 是否在模型修订前执行324-profile variance provenance与163-entry likelihood/covariance/structure no-fit重聚合 | `RESOLVED_USER_APPROVED_SOURCE_IMPLEMENTED_DIAGNOSTIC_EXECUTED_NO_MODEL_DECISION` | producer `b2456424…fdec`；canonical `48157a42…bb58`；324/324 profiles、163 completed entries、11 initialization sources闭合；唯一zero-variance profile为autocycle L2 `801207.SI/sf_dispersion_5d_neg`且preprocess前后均为0；46个completed entry有train-structure failure、4个sector identity跨8 seed持续失败；6个常量相关向量组显式insufficient。未运行HMM、未访问validation、未执行selection/acceptance、未写model/READY/DB/runtime |
| C-008-B3-REMEDIATION-D1 | `801207.SI/sf_dispersion_5d_neg`真实sector-local常量维应保持fail-closed还是采用显式inactive-dimension model identity | `PROPOSED_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY` | A保持fail-closed；B显式20→19 inactive-dimension identity，为唯一推荐但仍待用户批准。B保持full20 observation/preprocess，likelihood只用固定active indices；mixed-dimension level/family/READY identity、runtime InputManifest receipt与writer/parser same-authority已精确定义；不静默drop、padding、加floor/噪声或动态激活 |
| C-008-B3-REMEDIATION-D1-B-REFIT-01 | 若选择B，是否按固定treatment/control执行双fresh-process controlled refit | `PROPOSED_PENDING_USER_APPROVAL_NOT_EXECUTED` | 每process固定801207 treatment 8 seeds + 801011 identity20 control 8 seeds，两process共32 fits；control旧payload与两次repeat均须bitwise一致；`diagnostic_complete`与inconclusive/rejected/effect-supported分离，D5 evidence readiness单列；0 D5/D6/validation/model/READY/DB/runtime |
| C-008-B3-REMEDIATION-D1-D5-COMPAT-01 | inactive dimension后D5 `LL/(N*d_family)`分母与跨sector score如何保持批准语义 | `PROPOSED_PENDING_EVIDENCE_AND_USER_DECISION` | 19会改变已批准D5，20可能产生跨sector偏置；D1 controlled refit不得选择其一或执行D5。该项闭合前禁止formal grid/selection/model/READY |

C-001-A/C-002-A/C-003-A 已于 2026-07-22 获用户明确批准并回填本文；它们不是运行时人工审批。
C-006-A 已于 2026-07-23 获用户明确批准并回填本文；它不新增运行时审批或第二套股票池。
C-007-A 已于 2026-07-23 获用户明确批准并回填本文；它是 offline artifact-preparation 的固定算法版本，
不是运行时人工确认或可调门禁。
C-008-D1/C-008-B1/C-008-B3-DESIGN 方向已于 2026-07-23 获用户明确批准；后续又批准
C-008-B3-STRUCTURAL-A、DIAG-02、D3-01-A、D3-02-B、固定环境 D5-02-B、D7-01-A 与只读 D4-02-DIAG-03；
2026-07-25 又批准 D3-03-A、D4-01-A、D4-02-A、D4-03-B、D5-01-B、D6-01-B、C-008-B3-D4-L2-AUDIT-01 结论与受控 L2 重训设计方案 A。
上述设计批准本身不包含B3/L2 retrain源码实现、实际fit、seed selection、model/READY artifact或runtime/database写入；
随后用户已单独授权并完成本 Slice 0 源码开发，但实际fit及其后续动作仍未执行。
DIAG-02/03/04 的 `formal_acceptance_thresholds_applied=false` 仍是硬边界，
不得把 diagnostic completion 改写为正式 candidate acceptance。
C-005 是用户明确要求的交付控制，适用于今后每个 PR。

## 18. Design Acceptance Index / 设计验收索引

- F-011 parent：`APPROVED_BY_USER_SOURCE_IMPLEMENTED_FORMAL_EXECUTED_DIAGNOSED_BLOCKED_MODEL_ACCEPTANCE`；C-009、BUG-892 与
  C-010-FORMAL-A 已合入并完成 clean-main 601 日 formal preflight。两 fresh-process 共 5184 fits 和 D5/D6 已执行；formal canonical
  `e7992f87…39f` 为 blocked，未生成 model/READY。targeted blocker diagnostic 已按批准合同完成，canonical
  `10287e84…cffe8`；no-fit remediation diagnostic也已完成，canonical `48157a42…bb58`。两项都只完成根因证据闭合且未重跑
  完整 grid，Slice 1-3仍未开始。
- F-011-A 数据/PIT/observation：`C010_FORMAL_A_PREFLIGHT_AND_FORMAL_INPUT_VERIFIED`；C-007-A 单位、PIT sector mapping、7/20 维公式、
  hard semantic authority、120/30 行合同与既有 `0.90` coverage authority 均保留。已批准 policy 只把 full-universe train-frozen
  contributor ledger、price/moneyflow 双层 coverage、同源 moneyflow denominator 与逐 feature cross-section 形式化；它不删除证券或
  feature，不改变 validation/runtime prediction universe。A2/A3 已批准，四个 moneyflow/横截面派生 feature 的公式 identity 必须从
  v1 显式升级为 `hmm_risk_l1_sector_factor_formula_v2_c010`，不得被描述为“公式不变”。设计、源码、formal preflight 与 formal child input
  identity 已验证；当前 blocker 不在 data/PIT/policy gate。
- F-011-B fit/convergence/covariance/occupancy：`SOURCE_IMPLEMENTED_FORMAL_EXECUTED_DIAGNOSED_BLOCKED_D3_D4`；完整 grid、双 fresh-process hash、targeted diagnostic与no-fit remediation DIAG-02均已执行。
  `autocycle_all_core:L1/L2` 与 `legacy_covfix:L2` 因 initialization/likelihood/covariance/train-occupancy rejection 无 eligible candidate；
  `legacy_covfix:L1` 8 个 seed 均 D4 eligible。历史 DIAG 不反写正式 D4 acceptance。
- F-011-C semantic evidence/selection：`SOURCE_IMPLEMENTED_FORMAL_EXECUTED_DIAGNOSED_BLOCKED_D5_D6`；D5 train-only selection 已执行且未读取
  validation/future utility；仅选出 `legacy_covfix:L1/seed=43`。D6 在 `801980.SI` fail closed且没有 reselection，其余三个 level 无 D5 candidate。
- F-011-D 两-family READY：`BLOCKED_FORMAL_ACCEPTANCE`；READY artifact 数为0；四个 family/level 未全部 accepted，源码正确禁止 write。
- F-011-E generator/job/revision：`PENDING_IMPLEMENTATION`；不得由未完成的 model-set preparation 推导为 verified。
- F-012：advisory-only 写入与依赖隔离，不产生 Selection/Paper/QMT/QE/交易副作用。
- F-013：真实 read API、风险 UI、失败状态、可访问证据与 retrospective report。

## 19. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | `backend/db/init_hmm_risk_schema.py`; `backend/services/hmm_risk/{state_model_set,b3_acceptance,b3_training,b3_remediation_diagnostic,observation_eligibility,stock_fact_observation,stock_fact_repository}.py`; `scripts/hmm_risk/prepare_state_model_set.py` | formal canonical `e7992f87…39f`；blocker diagnostic canonical `10287e84…cffe8`；remediation DIAG-02 canonical `48157a42…bb58`；`backend/tests/hmm_risk/{test_b3_training,test_b3_remediation_diagnostic}.py` | APPROVED_BY_USER_SOURCE_IMPLEMENTED_FORMAL_EXECUTED_DIAGNOSED_BLOCKED_MODEL_ACCEPTANCE | 两 family均blocked、READY=0；targeted与no-fit diagnostics已完成但未批准任何remediation。下一步只允许先形成D1常量维精确设计决策，不自动改模型/阈值或重跑完整grid |
| F-011-A data/PIT/observation | `backend/services/hmm_risk/{security_identity,provider_absence,observation_eligibility,stock_fact_repository,stock_fact_observation}.py`; C-007-A/C-009/C-010 contracts | `F:/Dev/AIstock_artifacts/hmm_risk/b3_formal_20260729_e2c01bae_bug912/c010_formal_preflight.json`；`backend/tests/hmm_risk/test_stock_fact_observation.py` | APPROVED_BY_USER_C010_FORMAL_INPUT_VERIFIED | 601日 formal input与policy identity已通过并进入两个 child；当前blocker属于模型验收，不得回退到删证券/feature或填充缺失值 |
| F-011-B fit/convergence/covariance/occupancy | `backend/services/hmm_risk/{b3_training,b3_acceptance,b3_remediation_diagnostic,state_model_set}.py`; `scripts/hmm_risk/prepare_state_model_set.py` | formal rejection summaries；blocker diagnostic canonical `10287e84…cffe8`；remediation DIAG-02 canonical `48157a42…bb58`；`backend/tests/hmm_risk/{test_b3_acceptance,test_b3_remediation_diagnostic}.py` | APPROVED_BY_USER_SOURCE_IMPLEMENTED_FORMAL_EXECUTED_DIAGNOSED_BLOCKED_D3_D4 | covariance 74、train structure 46、likelihood 29、initialization 11个stage事件；8个persistent zero-variance与3个singleton init failure已区分。DIAG-02进一步闭合唯一zero profile、46个structure failed entry和4个跨8-seed persistent sector identity；D1及其他remediation合同未获批准，不能改阈值、模型或执行新grid |
| F-011-C semantic/selection | `backend/services/hmm_risk/{b3_acceptance,b3_training}.py`; `scripts/hmm_risk/prepare_state_model_set.py` | formal D5/D6 receipts；blocker diagnostic 3-entry replay canonical `10287e84…cffe8`；`backend/tests/hmm_risk/test_b3_training.py` | APPROVED_BY_USER_SOURCE_IMPLEMENTED_FORMAL_EXECUTED_DIAGNOSED_BLOCKED_D5_D6 | selection train-only且无refit；D6失败未reselection；`801980.SI` assignment/utility有效但temporal evidence失败；B2不采用，remediation待用户批准 |
| F-011-D two-family READY | `backend/services/hmm_risk/b3_training.py::write_b3_ready_model_set` | `F:/Dev/AIstock_artifacts/hmm_risk/b3_formal_20260729_e2c01bae_bug912/b3_formal_preparation.json` top-level blocked/no-write receipt；`backend/tests/hmm_risk/test_b3_training.py` | APPROVED_BY_USER_SOURCE_IMPLEMENTED_BLOCKED_FORMAL_ACCEPTANCE | READY artifact数为0；四个family/level完整性未成立，禁止partial/single-family write |
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
| seed sensitivity 或 validation-driven seed picking | D3-01-A固定预声明schedule全量运行；D5-01-B固定每family分别选择L1/L2 level-global identity，使用完整31/131 train-only min/median/mean lex receipt。selection不接收validation/future utility/D6，禁止per-sector拼接或semantic失败后换seed |
| monitor converged 掩盖 likelihood decrease | D4-01-A 独立校验 monitor/history；non-terminal negative 一律失败；terminal positive 必须 `<0.01`；terminal negative relative `>=-2e-5` 仅为显式持久化 warning，低于边界失败；不得自动放宽或静默删除 warning |
| `accepted_with_warning` 被当成 failure 或普通 success | 固定 `likelihood_valid` 映射和 `failed > insufficient_evidence > accepted_with_warning > accepted` 优先级；failure/warning 分数组聚合；overview/report 回读同一 warning evidence hash |
| 既有 L2 final parameters 被误当成 D4-01/D4-02 numeric receipt | AUDIT-01 已固定旧 L2 likelihood insufficient、covariance failed；禁止 grandfather、复制 L1 evidence、静默跳过或未经授权执行重训，新的两-family 131/131 未闭合前不得 READY |
| covariance clip 掩盖系统性 anomaly | D3-03-A/D4-02-A 禁止 initialization/post-fit clip 与 projection；正式 posterior 只使用通过 raw validity、0.5% dynamic-bound 闭区间、zero anomaly budget 和 2% M-step residual 的 raw covariance；全部 mask/hash 留存 |
| hard occupancy 极低但仍 labelable | train 侧按 `hmm_risk_c008_b3_d4_03_b_v1` 验收，selected validation 侧按 `hmm_risk_c008_b3_d6_01_b_v1` 验收 count/ratio/month/run/transition/max-run-share、posterior numeric validity 和 hard utility gap。任一侧都不得以 1 个样本自动通过，也不得互相推导 acceptance |
| 未经确认拆分 validation 或增加 holdout | 保持批准的 `2024-07-01..2025-03-31` 单一 validation 与 fitted `startprob_` prior；任何 split/holdout 先明确业务语义并获确认 |
| 库默认值或浮点环境导致不可复现 | D3-03-A 固定 KMeans/HMM 全参数与 sector-local prior；D5-02-B 固定依赖、BLAS/线程和 canonical serialization；不得仅凭 seed 或跨 host 外推 deterministic hash |
| 诊断数值被写成正式 gate | B1/DIAG-02/03/04的`formal_acceptance_thresholds_applied=false`、`selection_performed=false`是硬边界；D3-03-A/D4-01-A/D4-02-A/D4-03-B/D5-01-B/D6-01-B是后续实现合同，不得把historical score排序改写为正式selection或acceptance |
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
4. Slice 1/2 在 DEV 运行 fixture、真实只读 market input、人工 job 和 bounded worker；只写 DEV `hmm_risk.*`。
5. Slice 3 在安全端口完成真实 API/UI acceptance；未通过前不切 `/hmm`。
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
  BUG-877 不以“只修 302132”、只处理停牌、只保留可用股票或单一 dataset 冒充完成；C-009 必须同时覆盖 causal circ-mv、
  通用 source-specific identity resolver、provider-absence NA、四个 family/level coverage 与 frozen preflight。C-010-FORMAL-A 不以 5 个
  absence symbols、单一 `689009.SH`、单一 L1/L2 或 diagnostic mask candidate 冒充正式政策；必须交付 full-universe ledger、两层
  coverage、逐 feature cross-section、四个 family/level preflight 与完整 policy binding，且不删除任何批准 feature。
- no_silent_error：candidate/model/watermark/mapping/sector/L1/persistence/renderer 全部有 reason code；partial 不标 success；
  C-008-B3 将 initialization/fit/monitor/likelihood/covariance/occupancy/selection/semantic validation/family 状态分别持久化，
  D4-01-A 的 terminal negative acceptance 必须保留 `accepted_with_warning`、完整 delta evidence 与 family aggregate，不得静默
  变成普通 success；failure/warning 数组、primary reason、状态优先级、`likelihood_valid` 映射与 overview/report readback 均为
  exact contract；D5-01-B把schedule/eligibility/score/repeat/pool/selected与未选reason分别留证，missing/non-finite/hash mismatch
  不得退回固定seed或任意candidate；D6-01-B 把 validation evidence missing/date/posterior/count/occupancy/month/run/transition/run concentration/
  utility variance/gap 分别持久化，失败后不得换 seed；任一失败不得压缩或静默推导 READY。
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
- no_unrequested_gate_or_approval：D4-01-A、D4-02-A、D4-03-B、D5-01-B与D6-01-B是用户明确批准的确定性模型合同，不是运行时人工审批；未获确认的
  split/holdout不进入active contract；未来确认的
  确定性模型合同不是运行时人工审批。preview 不是批准步骤，普通 read 无确认；只保留规范要求的 production DDL/dependency/
  runtime 独立授权和用户要求的逐 PR 合入确认。L2 provenance audit 与受控重训的确定性合同不是新增人工审批、发布门禁或
  研究方向淘汰；D3-D7设计合同虽已闭合但尚未由实现执行，保持blocked是未授权实现和完整READY合取的准确状态。C-009 不新增
  95% coverage、最大 staleness、人工 alias 确认或数据源准入门禁；C-009-A/B/C/D 已于 2026-07-27 获用户明确批准，
  当前 source implementation/preflight 状态不构成新增人工审批或恢复模型训练的授权。C-010-FORMAL-A 的 `0.90`、120 行与
  full-universe/双层/逐 feature receipts 是用户已批准且已执行的确定性合同，不是运行时人工审批。DIAG-01 的用户确认只用于
  批准新的诊断范围与计算预算，不得被转化为每次训练、每只股票或每个 artifact 的新人工门禁。

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
- **禁止未经确认的门禁和审批**：`PASS`。诊断signed distance、matched comparison和分类只形成证据，不成为新gate；五类
  remediation均明确保持 `PROPOSED_PENDING_USER_APPROVAL_NOT_IMPLEMENTATION_READY`，没有自动淘汰family/sector或新增人工运行时审批。
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
  runtime人工确认或研究方向淘汰。`C-008-B3-REMEDIATION-D1`仍为proposed，必须取得用户精确决策后才能实现。
- **交付状态分离**：源码producer已提交、repo外artifact已生成并通过canonical readback；design回填不等于PR已合入，PR合入不等于
  dependency/DDL/DML/runtime已激活。本执行的这些生产gate与runtime动作均为noop。

### 23.5 C-008-B3-REMEDIATION-D1 正式设计审核

- **头部与父蓝图漂移**：`RESOLVED`。详细设计已更新修订日期、非implementation-ready状态与父蓝图v2.14；同PR同步父蓝图的
  DIAG-02执行事实、D1 proposed状态、P2计划、F-011证据和版本历史。
- **mixed-dimension aggregate缺口**：`RESOLVED`。已定义131-entry独立model entries、唯一19维allowlist、dimension histogram、
  ordered list hash、level/family/READY identity、writer/parser same-authority与禁止padding；D5未闭合前仍禁止写正式artifact。
- **controlled-refit结论缺口**：`RESOLVED`。`diagnostic_complete`已与inconclusive/rejected/effect-supported分离，并单列
  `d5_compatibility_evidence_ready`，禁止用局部fit或first-error伪造mechanism结论。
- **canonical set hash缺口**：`RESOLVED`。两个8-entry set固定seed排序、三字段object和canonical JSON list envelope，可独立重算。
- **runtime receipt缺口**：`RESOLVED`。inactive observation已绑定versioned receipt、dedupe key、InputManifest/daily-run hash lineage、
  conflict/replay语义；finite非零只记录，不动态激活或私增gate。
- **完整性审核**：`PASS_FOR_USER_DECISION_NOT_IMPLEMENTATION_READY`。A/B选项、exact allowlist、20→19 projection、full preprocess先行、
  artifact/parser/runtime、D4独立状态、32-fit treatment/control、reason codes与验证矩阵均已定义；没有把“drop一列”冒充完整实现。
- **D5兼容缺口**：`BLOCKED_EXPLICITLY_NOT_SILENT`。审核确认inactive dimension会使`d_family=20`与`d_effective=19`产生真实score
  comparability决策；设计已拆出`D1-D5-COMPAT-01`，controlled-refit固定0 selection。未由实现默认选择19或20，也未把局部fit
  改写为full-grid ready。
- **禁止简化/子集/POC**：`PASS`。D1有意只隔离一个模型机制，但同时覆盖treatment全部8 seeds、同family/level全部8 control seeds、
  双fresh-process identity与未来parser/runtime完整契约；它不是autocycle-only交付或family READY声明。
- **禁止静默错误**：`PASS`。近零、非零常量、non-finite、source/profile/preprocess/mask/shape/hash drift、control drift和repeat mismatch
  均fail closed；没有noise/floor、伪inactive covariance、identity fallback或dynamic activation。
- **禁止业务逻辑迁移**：`PASS`。global 20-feature observation/preprocess contract、两个family、31/131完整性、hard semantic authority、
  D3/D4 thresholds与D6保持不变；B2仍NOT_APPROVED。runtime对inactive维的“不参与likelihood”由model identity显式持久化，不是隐藏fallback。
- **禁止未经确认的门禁和审批**：`PASS`。A/B、controlled-refit和D5 comparability均保持proposed；未增加runtime人工确认、feature
  淘汰审批或研究准入gate。exact allowlist是本次证据范围，不是动态业务门禁。
- **因果可区分性**：`PASS`。identity20 control必须与旧payload bitwise一致；treatment只改变projection，禁止同时改EM/covariance/
  transition/threshold。即使treatment通过，也只证明该mechanism可进入下一项设计证据，不证明family candidate或READY。

综合审核结论：`PASS_PROPOSED_D1_B_CONTROLLED_REFIT_DESIGN_PENDING_USER_DECISION_D5_COMPAT_BLOCKED`。唯一推荐为D1-B，
但用户尚未批准A/B选择、32-fit执行或D5 score语义；因此不得实施源码、运行fit或进入formal grid。

## 24. 当前完成状态与下一步

本文件已闭合 C-001-A/C-002-A/C-003-A/C-006-A/C-007-A/C-008-D1/C-008-B1、C-008-B3-STRUCTURAL-A、
D3-01-A、D3-02-B、D3-03-A、D4-01-A、D4-02-A、D4-03-B、D5-01-B、D6-01-B、固定环境 D5-02-B、D7-01-A、
C-008-B3-D4-L2-AUDIT-01 与受控 L2 重训设计 A，并登记 DIAG-02/03/04 historical evidence。C-008-B2 继续为
`NOT_APPROVED`。C-009、BUG-892 和 C-010-FORMAL-A source/preflight 已完成并进入本次 formal execution；不再把 input coverage
描述为当前 blocker。C-008-B3-REMEDIATION-DIAG-02 已按批准的 no-fit 合同完成并通过执行后正式审核。

正式 B3 当前状态为：

- producer=`e2c01bae156281d551b084156fec4a09ed5a84ee`；formal canonical=`e7992f87…39f`；
- 两次 fresh-process 总 fits=`5184/5184`，bitwise receipt identity 已闭合；
- D5 已执行，validation/future utility 未用于 selection，selection 后未 refit；
- `legacy_covfix:L1` 选中 seed 43，但 `801980.SI` 在 D6 hard structure evidence 失败；
- `autocycle_all_core:L1/L2` 与 `legacy_covfix:L2` 无 eligible D5 candidate；formal rejection summaries 为
  9/74/67 seed-sector pairs；
- 两 family 均 `blocked`，READY artifact数为0，`model_write/ready_write/database_write/runtime_action=false`。

当前 F-011 parent 为 `APPROVED_BY_USER_SOURCE_IMPLEMENTED_FORMAL_EXECUTED_DIAGNOSED_BLOCKED_MODEL_ACCEPTANCE`；F-011-A 的
formal input/policy 已验证，F-011-B/C/D 分别 blocked 于 D3/D4、D5/D6 与两-family READY 合取。F-012 保持
`DESIGN_READY_USER_APPROVED`，F-013 保持 `PENDING_UPSTREAM_MODEL_SET`。

`C-008-B3-FORMAL-BLOCKER-DIAG-01` 已完成：producer=`ac3687c2…`、canonical=`10287e84…cffe8`、348/348 fits、
3-entry D6 replay、双 fresh-process bitwise equality均闭合；执行没有selection、threshold/authority变更、model/READY、数据库或runtime动作。

`C-008-B3-REMEDIATION-DIAG-02` 已完成：producer=`b2456424…fdec`、canonical=`48157a42…bb58`、324/324 profiles、
163/163 completed entries与11/11 initialization source均闭合；唯一zero-variance profile、46个train-structure failed entries、
4个跨8-seed persistent sector identity与6个statistic-insufficient groups均保留exact evidence。执行没有HMM refit、validation、
acceptance、selection、threshold/authority变更、model/READY、数据库或runtime动作。

下一步不是再次运行完整5184 grid，也不是直接修改阈值或模型。`C-008-B3-REMEDIATION-D1`精确设计与正式审核已完成，当前等待用户
在A“保持fail-closed”与B“显式inactive-dimension identity”之间决策；唯一推荐为B。若选择B，还需用户单独批准32-fit
controlled-refit；该执行固定0 D5/D6/validation/model/READY。D5的19/20维score comparability继续由
`D1-D5-COMPAT-01`显式blocked，不能由实现猜测。在A/B与controlled-refit取得批准前，不实施B3、不运行新fit、不选择seed、
不生成model/READY。当前revision的DDL/DML、依赖、runtime、数据库、model/READY与客户端同步均为`noop`，PR merge仍等待用户单独确认。
