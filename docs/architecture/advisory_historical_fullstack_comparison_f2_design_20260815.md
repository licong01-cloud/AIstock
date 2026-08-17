# Advisory Historical Full-stack Comparison F2 设计

> 日期：2026-08-15
> Feature tier：F2
> 任务级别：T3
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 适用范围：历史研究与模拟荐股验证；不连接下单、Paper持仓或QMT
> 状态：`DESIGN_ACCEPTED_BY_USER_EXECUTION_REQUEST_IMPLEMENTATION_IN_PROGRESS`

## 1. 背景与问题

已完成的 Historical Range 回放 `ahrr_b0f61e7801752c006da8fe18054372c5` 冻结了空 `runtime_config`。其 2,200 个候选在 alpha raw、HMM、risk 和 selection 四层的 rank/score 完全一致，`advisory_model_rank` 全空。该运行成功验证了 PIT 候选生产、名单生命周期和 outcome 统计，但没有验证平台 HMM、风险政策或模型 challenger 的增量。

本设计建立一条可恢复、可审计、禁止未来数据泄露的三臂对照，不修改现有正式荐股排序，不把 shadow 模型静默激活。

## 2. 范围

1. 冻结同一个多 Alpha 包、日期窗口、历史数据源、成本、名单退出规则和 outcome 口径。
2. 运行 A/B 两条正式 Historical Range 研究臂。
3. 从 A 的不可变 candidate artifact 构建独立 M5A Historical challenger artifact。
4. 产出逐层 rank、排除、名单状态、固定期限收益、episode/path、月份和市场阶段对比。
5. 长任务保存 request/run/artifact identity、heartbeat、失败原因和恢复入口。

## 3. 非目标

- 不激活或改写当前 Program/shadow binding。
- 不把 M5A/meta-label 输出写回 R2-B candidate artifact；该 artifact 合同明确禁止模型分数。
- 不创建新数据库表，不修改 Historical Range、Selection、Paper 或模拟盘既有表语义。
- 不训练新策略包、HMM 或荐股模型。
- 不将 M5A 强行串接在平台 HMM 调整分数之后；M5A 冻结 runtime semantics 要求原始 Alpha leg 组合分数。
- 不自行启动、停止或重启用户后端。
- P0-D meta-label 保留为独立附加研究项；在其历史 runtime consumer 完成前，不用训练/CPCV builder 冒充历史推理。

## 4. 冻结实验契约

### 4.1 公共输入

```text
package_id = pkg_ma_8ec5e389fa2c5e484a1ac7e9
manifest_sha256 = f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016
start_trade_date = 2026-05-15
end_trade_date = 2026-07-16
data_source = DB_HISTORICAL
entry/exit price basis = next_open_executable
target_count_business = 20
rank_enter_threshold = 20
rank_exit_threshold = 40
rank_exit_confirm_days = 2
daily_replacement_budget = 5
stop_loss_bps = 800
take_profit_bps = 1800
trailing_stop_bps = 700
time_stop_days = 20
```

所有臂使用同一个代码提交和相同日期计划。若某一臂因输入不完整而失败，不得用其他代码版本或缩短日期静默补跑。

### 4.2 A：原始 Selection 基线

```text
hmm.enabled = false
risk_policy.enabled = false
tradability.exclude_suspended = true
industry_blacklist = []
selection.top_k = 50
```

所有 NOT_APPLICABLE/COMPLETE receipt 必须明确持久化。A 是 B/C 的共同 raw evidence 父节点。

`top_k=50` 与策略包清单的多 Alpha 候选池契约保持一致；运行时不得擅自缩小。
对既有 44 日 raw Top50 与逐日 PIT `market.sw_index_member` 做预检后，Top50 中有
13 日、15 个候选缺少当时有效的行业归属（最早缺失排名为 35）。不允许使用
2026-07-01 以后才首次出现的成员关系回填 5—6 月，也不允许给缺失行业静默赋中性
系数。B 明确使用 `hmm.missing_sector_policy=exclude_candidate`：缺映射候选在 HMM
阶段剔除并写入 exclusion receipt；A 保留原始 Top50，使这一数据就绪约束成为可量化的
平台增强效果。

### 4.3 B：平台 HMM + 风险 + 可交易性

```text
hmm.enabled = true
hmm.model_snapshot_id = bbec3863-fb67-445f-938e-66f092d18696
hmm.signal_preset = preset_A
hmm.missing_sector_policy = exclude_candidate
risk_policy.enabled = true
risk_policy.providers = [st_pit]
risk_policy.hard_actions = [block_buy, force_exit]
risk_policy.score_overlay.enabled = false
tradability.exclude_suspended = true
selection.top_k = 50
```

HMM snapshot 于 2026-04-27 已存在，早于实验窗口。逐日 coefficient 使用该冻结模型和截至 decision date 的历史输入生成；每个交易日保存 model/coefficient/source hash、as-of、effective date 和 first-observed evidence。禁止 dynamic latest、缺日中性回退或使用5月19日以后训练的模型覆盖窗口早段。

风险政策只消费当日决策时可见的 ST PIT 事实。数据缺失时整日 typed failure，不把 B 降级成 A。

### 4.4 C：M5A Historical challenger

```text
parent_arm = A
bundle_id = 1757b24b854cf8b5bfee8874bd442491091ea979c86522fbeef15a02930f8ecb
bundle_schema = advisory_model_bundle_v2
bundle_status = EXPERIMENTAL_SHADOW
continuation_cutoff = 2026-03-10
candidate_group = A.selection_effective_rank <= 20
model_weight = 0.75
shortlist_count = 5
```

C 读取 A 的 candidate artifact、component lineage 和同日 DB 历史特征。模型特征只允许 decision date 及以前数据；target date 仅用于时钟身份。C 输出独立的 `advisory_historical_model_challenger_v1` artifact，包含父 candidate hash、bundle/feature/source identity、逐 symbol selection/model rank/score 和 typed coverage，不修改 A/B candidate facts。

M5A 当前 Program binding 指向旧 M1 bundle，不得为本实验改写 binding。C 显式加载上述研究 bundle并执行完整 hash/readback 校验。

## 5. 同容量比较

业务名单与模型 Top5 不能直接混为一组。报告同时提供：

1. `A20`：A 组现行20只名单政策；
2. `B20`：B 组相同20只名单政策；
3. `A5`：A selection rank 的 matched Top5；
4. `B5`：B selection rank 的 matched Top5；
5. `C5`：M5A model rank Top5。

A5/B5/C5统一使用目标5只、每日替换预算5、rank exit threshold 40、确认2天、止盈止损和20日time stop。C 的退出 rank 保留原始 Selection Top40 语义；模型只控制 entry priority，避免每日模型波动同时改变退出定义。

结果层必须区分“逐日信号集合”和“名单生命周期集合”。逐日 Top5/Top20 仅用于 rank overlap 与信号诊断，不得冒充名单绩效。A20/B20 从权威 list item 的 ENTER/HOLD 事实读回；A5/B5/C5 使用中立 `AdvisoryListTransitionEngine` 和各日 decision-cutoff mark 重放。C 对未持有候选按 model rank 排 entry priority，对已持有 symbol 的 review/exit 始终使用原 Selection rank。每组保存逐日 active symbols、动作、退出原因和 episode identity，固定期限与 episode/path 指标只从该生命周期证据派生。

## 6. 架构

```text
sealed package + date plan + source catalog
  -> A Historical Range run
       -> immutable raw candidate artifacts
       -> A20 list/outcomes
       -> A5 matched counterfactual
  -> B Historical Range run
       -> sealed per-day HMM bindings
       -> ST PIT risk + tradability
       -> B20 list/outcomes
       -> B5 matched counterfactual
  -> C historical challenger
       -> explicit M5A bundle loader
       -> A Top20 component evidence
       -> decision-cutoff feature source
       -> shared feature builder + M5A scorer
       -> independent challenger artifact
       -> C5 policy replay/outcomes
  -> one comparison receipt/report
```

现有 Historical Range candidate/outcome 表是权威输入。新增比较产物写 repo-external artifact root；不新增 DDL。可重复使用的入口位于 `scripts/`，一次性运行日志位于 `tmp/`。

## 7. Implementation Plan / 实施方案

计划源文件：

- `backend/services/advisory_model_first/model_bundle.py`：增加显式、只读、未激活 research bundle loader。
- `backend/services/advisory_model_first/model_inference.py`：拆出不依赖本地 LightGBM 的规范化特征矩阵与 booster 原始输出后处理，保持生产 ensemble/rank/explanation 语义唯一。
- `backend/services/advisory_historical_range/model_challenger.py`：从已封存候选生成独立模型 challenger artifact。
- `backend/services/advisory_historical_range/wsl_model_scorer.py`：Windows 冻结输入与 WSL LightGBM 原始推理之间的显式身份、路径和输出校验边界。
- `backend/services/advisory_historical_range/fullstack_comparison.py`：冻结合同、逐日信号诊断、复用中立引擎的同容量名单生命周期重放、指标和receipt。
- `backend/services/advisory_historical_range/api_models.py` 与 `runtime_factories.py`：把 domain 已有的目标 logical-id、计算修正原因和不可变 evidence ref 显式透传到 R5 outcome request；保持默认普通 refresh 完全兼容。
- `scripts/wsl/advisory_historical_model_predict.py`：Python 3.10 兼容的纯 LightGBM helper，只返回冻结模型的 raw score/contribution，不访问数据库、不重排。
- `scripts/advisory_historical_fullstack_comparison.py`：长任务CLI、冻结 A/B contract 读回、独立 challenger implementation hash、outcome 恢复代次、有界连接池、失败 logical-id 精确计算修正、状态、heartbeat与非零失败。
- `backend/tests/advisory_historical_range/test_model_challenger.py`、`test_wsl_model_scorer.py`、`test_r5_outcome_correction_api.py` 与 `backend/tests/scripts/test_advisory_historical_fullstack_comparison.py`：parent immutability、PIT/identity、matched lifecycle、WSL fail-closed、correction passthrough、连接复用、事务 reset、恢复代次和指标测试。
- 本设计文档和最终 `docs/analysis/` 结果报告。

若现有 HMM Historical Range 已完整支持B组，仅复用正式入口，不复制HMM算法。

## 8. 指标与判定

### 8.1 每层效果

- rank changed count/rate、Spearman、Top5/Top20 overlap；
- included/excluded count、停牌/ST/风险原因；
- raw/HMM/risk/selection/model stage receipt状态；
- 空增强检测：B若 HMM/risk 全部 NOT_APPLICABLE 则整组无效。

### 8.2 名单与收益

- ENTER/HOLD/EXIT/WATCH、active count、换手和退出原因；
- 1/3/5/10/20日 executable net absolute；基准恢复后同时给net excess；
- A20/B20 episode 使用已成熟的 executable net absolute；A5/B5/C5 反事实 episode 使用同日 decision-mark gross，并明确禁止伪造未执行的成本现金流；两类均报告胜率、持有期、MFE/MAE和最大连续亏损；
- 月份、上涨/下跌/震荡阶段和7月专项切片；
- 相对A matched baseline的均值、胜率和置信区间。

无法获得benchmark时，absolute与excess明确分开，禁止用absolute冒充alpha。

## 9. 无未来数据泄露

1. candidate source catalog覆盖每个decision date和所有读取分区。
2. HMM snapshot训练时间早于窗口；coefficient输入最大日期不晚于decision date。
3. M5A bundle continuation cutoff早于窗口；实时特征查询不晚于decision date。
4. target/exit价格、收益和标签只在候选/名单冻结后进入outcome阶段。
5. 每个artifact保存父hash、代码hash、数据revision和日期时钟；不满足则非零失败。

## 10. 长任务与恢复

- 状态文件保存 `phase/arm/request_id/batch_id/run_id/last_trade_date/artifact_hash/error`。
- 每一阶段exact retry必须返回相同identity；不同输入拒绝覆盖。
- A/B 完成后只从不可变 contract artifact 恢复实验身份；C 另存 challenger implementation hash，C 已有成功日后代码指纹变化必须拒绝混跑。
- outcome 父操作终态失败时递增恢复代次并创建新 idempotency key；父操作仍为 `QUEUED/RUNNING` 时拒绝并发恢复，禁止复用失败父操作造成 heartbeat/CAS 竞争。
- outcome 细粒度读取使用 task-local `ThreadedConnectionPool` 复用有界连接；每次归还前成功 commit、异常 rollback 并 `reset()`，失效连接 discard，禁止每个 calculation 新建 TCP 连接。
- outcome 已提交版本不得删除或覆盖。若执行中发现已修复的确定性计算缺陷，必须等当前父操作终态，发布包含旧/新 producer hash、BUG/PR 和目标 logical-id 集合的不可变 REQUEST evidence，再以 `CALCULATION_CORRECTION` 只追加目标 outcome 新版本；普通 refresh 不得隐式升级成全量 correction。
- 长步骤输出heartbeat和阶段耗时；失败后按原冻结identity恢复，不新建相似任务掩盖失败。
- 用户后端重启不应破坏数据库任务；任务恢复从持久状态和API/DB readback继续。

## 11. 失败语义

首批typed reason：

```text
ADVISORY_COMPARISON_CONTRACT_MISMATCH
ADVISORY_COMPARISON_HMM_EVIDENCE_INCOMPLETE
ADVISORY_COMPARISON_RISK_EVIDENCE_INCOMPLETE
ADVISORY_COMPARISON_PARENT_CANDIDATE_MISMATCH
ADVISORY_COMPARISON_MODEL_BUNDLE_INVALID
ADVISORY_COMPARISON_FEATURE_CUTOFF_VIOLATION
ADVISORY_COMPARISON_MODEL_FEATURE_INCOMPLETE
ADVISORY_COMPARISON_MODEL_INFERENCE_ENVIRONMENT_UNAVAILABLE
ADVISORY_COMPARISON_MODEL_INFERENCE_FAILED
ADVISORY_COMPARISON_MODEL_INFERENCE_OUTPUT_INVALID
ADVISORY_COMPARISON_OUTCOME_INCOMPLETE
ADVISORY_COMPARISON_OUTCOME_IN_PROGRESS
ADVISORY_COMPARISON_EMPTY_ENHANCEMENT
ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT
```

任何异常不得转换成零收益、空成功、A组结果或默认排名。

## 12. Verification Plan / 验证方案

### L0/L1

- bundle路径、manifest/file hash、明确bundle ID；
- parent candidate/component lineage/hash闭合；
- decision/target clock、feature cutoff和HMM as-of；
- deterministic rank/tie-break、Top5匹配和list transition；
- 逐日 rank 集合不得替代 active list；A20/B20 权威 list readback 与 A5/B5/C5 matched lifecycle 均需动作/episode/active-set 断言；
- 缺特征、缺HMM日、风险源不完整、bundle冲突全部fail closed；
- exact retry和状态恢复。
- R5 API/application factory 对目标 logical-id、correction reason/evidence 的成对校验、排序校验和完整透传；缺 evidence 或非目标 outcome 必须 fail closed。

### L2

- 使用真实多Alpha候选、真实HMM snapshot、真实DB历史行情和真实M5A bundle；
- A/B各44个交易日完整完成；C对A的44日逐日推理；
- outcome refresh及A20/B20/A5/B5/C5完整统计。

### L3/L4/L5

- 本功能无新UI，L3/L4为`noop`；Historical Range refresh API 仅补齐 domain 已有的版本化 correction 字段，不改变普通 refresh 默认语义。
- 跨模块长回归交由CI/Validation Center，交付紧凑receipt。

## 13. Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-001 | A/B/C公共输入和唯一变量被冻结并可hash |
| F-002 | B使用窗口开始前可用的明确HMM snapshot和逐日PIT coefficient |
| F-003 | B风险/可交易性缺失时失败，不降级成A |
| F-004 | C显式加载M5A research bundle，不修改激活binding |
| F-005 | C只读A候选并发布独立artifact，不修改R2-B候选 |
| F-006 | C特征只使用decision cutoff，target/outcome不进入模型输入 |
| F-007 | A20/B20和A5/B5/C5均有同口径名单及收益统计 |
| F-008 | 每层排名变化、排除和空增强状态可见 |
| F-009 | 长任务有heartbeat、持久状态、resume、exact retry和不可变计算修正链 |
| F-010 | 缺输入/冲突/未来信息全部typed failure，无silent fallback |
| F-011 | 三组真实运行完成并产出可复核结果报告 |
| F-012 | DESIGN-COMPLIANCE-001四项逐项有直接证据 |

## 14. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/advisory_historical_range/fullstack_comparison.py`; `scripts/advisory_historical_fullstack_comparison.py` | artifact:`F:/Dev/AIstock_model_artifacts/advisory_fullstack_comparison_configfix_20260817/comparison_result_v6.json`; `backend/tests/scripts/test_advisory_historical_fullstack_comparison.py` | verified | - |
| F-002 | `backend/services/selection_center/hmm_runtime.py`; `scripts/precompute_hmm_coefficients.py` | artifact:`F:/Dev/AIstock_model_artifacts/advisory_fullstack_comparison/hmm/coefficients_preset_A_2026-05-15_2026-07-16_pit_v3.json`; `backend/tests/advisory_historical_range/test_r2b_hmm_binding.py` | verified | - |
| F-003 | existing risk provider + comparison validator | `backend/tests/advisory_historical_range/test_model_challenger.py`; artifact:`docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md` | verified | - |
| F-004 | `backend/services/advisory_model_first/model_bundle.py`; `backend/services/advisory_historical_range/wsl_model_scorer.py` | `backend/tests/advisory_historical_range/test_wsl_model_scorer.py`; `backend/tests/advisory_model_first/test_quality_bundle.py` | verified | - |
| F-005 | `backend/services/advisory_historical_range/model_challenger.py`; independent comparison artifact store | `backend/tests/advisory_historical_range/test_model_challenger.py`; artifact:`F:/Dev/AIstock_model_artifacts/advisory_fullstack_comparison_configfix_20260817/challenger_state_v6.json` | verified | - |
| F-006 | shared feature builder + host matrix/WSL raw scorer | `backend/tests/advisory_historical_range/test_wsl_model_scorer.py`; artifact:`F:/Dev/AIstock_model_artifacts/advisory_fullstack_comparison_configfix_20260817/comparison_result_v6.json` | verified | - |
| F-007 | comparison service/script | `backend/tests/advisory_historical_range/test_model_challenger.py`; artifact:`docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md` | verified | - |
| F-008 | comparison report + daily stage effects | `backend/tests/scripts/test_advisory_historical_fullstack_comparison.py`; artifact:`docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md` | verified | - |
| F-009 | comparison CLI/status + R5 correction passthrough + outcome parent heartbeat | `backend/tests/advisory_historical_range/test_r5_outcome_correction_api.py`; `backend/tests/advisory_historical_range/test_r5_background_lifecycle.py`; PR #3553; merge commit `c1b8030ab071ee2b9bbad0df9eb97fa1e2b18011` | verified | - |
| F-010 | all new boundaries | `backend/tests/advisory_historical_range/test_model_challenger.py`; `backend/tests/advisory_historical_range/test_wsl_model_scorer.py`; `backend/tests/scripts/test_advisory_historical_fullstack_comparison.py` | verified | - |
| F-011 | runtime artifacts/report | artifact:`F:/Dev/AIstock_model_artifacts/advisory_fullstack_comparison_configfix_20260817/comparison_result_v6.json`; artifact:`docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md` | verified | - |
| F-012 | section 18 DESIGN-COMPLIANCE-001 item-by-item review | `backend/tests/advisory_historical_range/test_model_challenger.py`; artifact:`docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md` | verified | - |

## 15. Production Gates

| 项目 | 状态 | 说明 |
|---|---|---|
| production DDL | `noop` | 不新增或修改schema |
| production DML | `authorized_task_runtime_only` | 用户已要求启动三组历史研究；只经现有Historical Range正式repository/API写研究表 |
| runtime asset write | `authorized_exact_artifacts_only` | 只写命名的HMM/comparison artifact root，保存hash和readback |
| backend dependency | `noop` | 复用现有LightGBM/Pandas/Psycopg2 |
| frontend dependency | `noop` | 无前端范围 |
| service restart | `noop_user_owned` | 不控制用户后端 |
| runtime activation | `noop` | 不激活模型或策略 |
| package approval | `noop` | 不改变策略包状态 |

## 16. Rollout / Rollback / 发布与回滚

- 发布：只提交设计、研究执行器、测试与结果报告；不激活模型、不改变策略包、不修改生产 schema。
- 运行：A/B 只经现有 Historical Range API 建立可恢复研究任务，C 只在命名的 repo-external artifact root 写入独立研究产物。
- 回滚：停止提交新的研究请求，删除本任务命名 artifact root 前须另获明确授权；数据库中的已完成研究记录保持审计可追溯，不做逆向 DML。
- 失败恢复：按冻结 contract hash、batch/run identity 和 artifact hash 继续 exact retry；输入不同则创建新 identity，禁止覆盖。

## 17. Risks / 风险

- B 的 HMM 输入可能覆盖不全：按日 readback，任一交易日缺失即失败关闭。
- C 的模型特征可能因历史源缺失而无法评分：记录明确 symbol/date/reason，禁止回退到 A 排名。
- Top20 与 Top5 容量不同会混淆结论：分别报告 A20/B20 与匹配容量 A5/B5/C5。
- 7 月单边下跌会影响绝对胜率：同时报告全窗、月度和截面相对指标，不将单一胜率解释为排序质量。

## 18. DESIGN-COMPLIANCE-001 终审

1. 禁止简化交付：`verified`。A/B/C 均完成 44/44 日；结果 v2 同时保留净绝对、毛收益、净超额缺口、月度、CI、名单与 Episode 证据，未用逐日 Top5 替代名单生命周期。直接证据为 artifact:`F:/Dev/AIstock_model_artifacts/advisory_fullstack_comparison_configfix_20260817/comparison_result_v6.json` 与 artifact:`docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md`。
2. 禁止静默错误：`verified`。C 的候选完整性、WSL 请求/模型字节身份、缺特征与 artifact 冲突均失败关闭；31 个失败 outcome 只以 `CALCULATION_CORRECTION` 追加新版本，最新失败数为 0。直接测试为 `backend/tests/advisory_historical_range/test_model_challenger.py`、`backend/tests/advisory_historical_range/test_wsl_model_scorer.py` 与 `backend/tests/advisory_historical_range/test_r5_outcome_correction_api.py`。
3. 禁止改变业务逻辑：`verified`。A/B raw candidate 逐日相同校验；C 只读 A Top20 并发布独立 artifact，active review/exit 继续使用 Selection rank；A/B 正式 candidate artifact 未写入模型字段。直接测试为 `backend/tests/advisory_historical_range/test_model_challenger.py`。
4. 禁止私增门禁：`verified`。production DDL/dependency/package approval 均为 `noop`，未添加角色审批或额外发布阻断。BUG-1115 已通过 PR #3553 合入 merge commit `c1b8030ab071ee2b9bbad0df9eb97fa1e2b18011`，因此 F-009 可以关闭；backend-main 重启及重启后读回仍是独立的运行时待用户执行边界，不把源码合入冒充为运行时已生效。
