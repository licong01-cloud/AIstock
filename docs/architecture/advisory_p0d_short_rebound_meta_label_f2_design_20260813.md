# Advisory P0-D SHORT_REBOUND Meta-label 真实训练详细设计

> 日期：2026-08-13
> Feature tier：F2
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.0
> 直接父产物：P0-C bundle `81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd`
> 当前阶段：`DESIGN_READY_FOR_IMPLEMENTATION`
> 适用范围：学术研究与模拟荐股观察，不构成实时投资建议，不连接实盘交易或下单执行

## 1. Background / 当前真实基线

P0-C 已基于目标多 Alpha 父包真实文件数据产出：

- 386 个 candidate decision days，405 个 Top40 rank context days。
- 7,720 个候选，7,716 个成熟 policy labels。
- take正例4,199、负例3,517，take rate 54.42%。
- 28/28 purged CPCV paths 均包含正负类且状态 `READY`。
- 持有期中位6日、最大20日；退出主要来自 rank drop、stop loss、time stop 和 trailing take profit。
- selection rank 1..5、6..10、11..15、16..20 的 take rate 均约54%，没有单调区分力；继续纯重排 selection rank 的价值低。
- P0-C PBO 为 `NOT_COMPUTABLE_NO_TRIAL_RESULTS`，因为尚无真实模型 family/trial。

P0-D 的唯一目标是训练首个真实 take/skip/confidence meta-label challenger，并以同一 policy portfolio 口径评价。它不再优化5日 NDCG，也不建设额外基础设施。

## 2. Scope / 交付范围

1. 冻结一个绑定 P0-C bundle、QE feature files、repository commit 和候选 family 矩阵的训练 request。
2. 对每个 CPCV path，分类词汇和模型只在该 path train dates 内拟合；HMM作为无监督市场状态，按每个基础时间块起点仅使用当时已发生的连续历史拟合并因果投影，绝不使用该块及之后行情。
3. 训练 `take_probability/skip_probability/confidence`，产生每日候选优先级和 Top5 challenger。
4. 用候选级分类指标和 Top5 shadow portfolio 指标分别评价所有6个 trial；主选择指标固定为 validation `mean_daily_net_excess_return_bps`。
5. 用完整 trial/block score 矩阵计算 PBO，保存每条 path 的全部结果，不只保存 winner。
6. 选出 validation winner 后，在全部成熟历史上重拟合 HMM/预处理/LightGBM，发布不可变 experimental meta-label bundle。
7. 提供 exact bundle loader/scorer，供 P0-A/P0-B dynamic descriptor 后续引用；本阶段不写 descriptor、不激活、不修改 baseline。

## 3. Non-goals / 明确禁止

- 不读取数据库、Paper、模拟盘、QMT、Historical Range 或前向 episode 作为训练输入。
- 不使用旧80日 test 选 family、seed、threshold、特征、HMM或政策。
- 不训练 reranker/LambdaRank，不用规则结果冒充模型概率。
- 不自动激活 bundle，不写 Program descriptor，不修改 P0-A observation、Selection rank 或生产 Top20 policy。
- 不做跨包共享、多任务/MMoE、adaptive conformal、LONG_TREND、仓位资金或自动交易。
- 不扩建缓存、模型注册中心、历史证据、通用编排或审批门禁。
- 不在 Windows 训练；正式训练只在 WSL `rdagent-gpu` Conda。

## 4. Contracts / 冻结身份

`FrozenAdvisoryMetaLabelTrainingRequestV1` 至少包含：

```text
request_id / request_sha256 / created_at
policy_dataset_bundle_root / id / manifest_file_sha256
program/package/binding/manifest/style identities
shadow_policy_sha256 / cost_policy_sha256 / split_policy_sha256
qlib_daily_root / factor_data_root / factor_data_cutoff / suspend_data_root
repository_root / repository_commit
output_root / resource_max_rss_bytes
feature_schema_version / feature_schema_hash
family_specs
seed_roster = [20260813, 20260817, 20260823]
primary_metric = mean_daily_net_excess_return_bps
tie_break = family_id, seed ascending
probability_threshold = 0.5 (只用于take/skip分类，不用于Top5选股)
```

request hash 排除 created_at/output_root/request id/hash，其余功能字段全部纳入。P0-C bundle 必须全文件 readback；request 中的 package/policy/split identity 必须与 bundle manifest 和 request 交叉一致。

## 5. Architecture / 数据与训练流

```text
exact P0-C policy dataset bundle
  -> candidate_rankings + policy labels + CPCV paths
exact QE H5/Parquet/Qlib Bin
  -> one common decision-cutoff non-HMM base feature Parquet
  -> per-block past-only fresh HMM fit + per-path train-only categorical vocabulary
  -> validation causal projection
  -> 2 families x 3 seeds LightGBM binary classifiers
  -> candidate take/skip metrics
  -> dual-rank Top5 policy replay
       entry_priority_rank = model probability order
       selection_exit_rank = original Top40 rank
  -> path/block policy metrics
  -> PBO
  -> winner selection by frozen primary metric/tie-break
  -> full-history refit
  -> immutable meta-label bundle
```

训练只读文件；不写数据库。中间矩阵以 Parquet 分 path 落盘并按阶段释放内存，峰值RSS不得超过8GB。

## 6. Feature Contract / 特征与泄漏边界

使用现有 `FEATURE_SCHEMA_PAYLOAD` 和 `SharedAdvisoryFeatureBuilder`，特征包括：

- selection score/rank和两腿原始/标准分数、rank gap、方向一致性、权重集中度。
- decision日及以前的收益、波动、流动性、成交、资金、估值、行业、市场广度和涨跌停距离。
- 当前 decision日的停牌/涨跌停可见状态。
- fresh sector HMM 的 causal posterior/state/duration。

禁止进入特征：entry/exit price、未来rank、holding days、exit reason、MFE/MAE、net return、take label、policy path。

### 6.1 HMM causal walk-forward

不能为每条 CPCV path 把非连续 train 日期直接拼起来拟合 HMM，因为 validation 之后的 train block 对较早 validation 属于未来。批准算法：

1. 以8个基础时间块为 walk-forward边界。
2. 对每个块 `B_i`，HMM仅使用严格早于 `B_i.start` 的连续历史行情拟合；它不读取任何label。此前已发生但在某条CPCV组合中属于另一validation block的行情仍是当时可见的无监督市场事实，可以进入较晚块HMM；当前块及未来行情禁止进入。
3. 使用固定 seed=42和现有 `fit_fresh_sector_hmm` 合同，causal forward filter只投影 `B_i`。
4. 前置历史不足120日的块标记 `HMM_UNAVAILABLE_FOR_BLOCK`，HMM四列保持NaN并由既有四个missing indicators表达；不能用全局HMM或后验回填。
5. HMM-enabled family可消费显式missing HMM状态，量化后续可用块的增量；每个path报告HMM可用/缺失块数。只有全部块均缺HMM时该family才`NOT_COMPUTABLE`，避免把合理冷启动变成无法通过的门禁。

为避免28 path重复训练相同 block HMM，按 `(request_sha256, block_id, train_date_hash, source_schema_hash)` 缓存到当前 run 的 repo-external Parquet/JSON；缓存是确定性文件中间结果，不是通用平台。

非HMM基础特征对所有path相同且每行只使用自身decision及过去窗口，因此每个request只构建一次 `base_features.parquet`；8个block HMM states按键追加形成 `walk_forward_features.parquet`。28 paths只依据P0-C path成员切片并在train内拟合词汇/模型，不重复读取61万行情行或重算全部技术指标。

## 7. Model Families / 固定小型实验

只比较两个预注册 family，每个3 seeds，共6 trials：

### FAMILY_CORE

- LightGBM binary objective。
- 使用除 HMM 四列外的现有 model features。
- 参数：`num_leaves=15, learning_rate=0.03, min_data_in_leaf=80, feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, lambda_l1=0.1, lambda_l2=1.0, num_threads=4`。

### FAMILY_CORE_HMM

- 同 FAMILY_CORE，加 `hmm_bull_posterior/hmm_state/hmm_state_duration/hmm_observation_completeness`。
- 其它参数完全相同，确保 HMM 增量可归因。

每个 path 最多600 rounds、60 rounds early stopping。class weight不自动平衡，因为真实标签近似平衡；禁止按 validation结果增加第三个family或改参数。

## 8. Path-local Preprocessing

- categorical `l2_code_id` vocabulary只从当前 path train 拟合；validation未知类别为missing，不扩词表。
- optional missing indicators沿用 feature schema。
- required feature缺失导致对应日期不可用并有coverage receipt，不能填0。
- 连续 `confidence_target` 的 winsor边界只从path train的1%/99%分位拟合；分类模型标签保持原始take 0/1。
- LightGBM原生处理NaN，不做全局 scaler或全数据插补。

## 9. Candidate and Portfolio Evaluation

### 9.1 Candidate metrics

每个 path/trial 保存：ROC-AUC、PR-AUC、Brier、log loss、accuracy、precision/recall、take/skip coverage、按selection rank bucket和regime分层表现。单类/空validation显式 `NOT_COMPUTABLE`。

### 9.2 Dual-rank portfolio adapter

meta-label输出只改变新入场优先级，不改变退出信号：

- `entry_priority_rank`：当日Top20按take_probability降序、selection rank、symbol确定性排序；前5供新入场。
- `selection_exit_rank`：始终来自P0-C当日原始Top40；持仓不在Top40时为41。
- adapter先用selection exit rank推进已有持仓，再按entry priority填空位；target count、daily replacement budget、现金、停牌/涨跌停、next-open时钟和成本仍使用shadow policy。
- validation block结束后禁止新入场，但继续用后续rank context/open结算该block已建立的episode直至退出；这些结算日收益计入该block policy score，避免块尾持仓被截断。
- 禁止把model rank用于rank_exit，否则会改变review policy。

每个 path/trial 同时报：mean daily net excess、cumulative net return、maximum drawdown、turnover、completed episode hit rate、active/cash coverage。

### 9.3 Baselines

同一validation dates和policy下比较：

- `selection_top5`。
- `M5A_reranker`：只在已有精确预测覆盖且未用于选择时作历史对照；不覆盖则typed unavailable。
- `HMM_top5`：只在合法path-local HMM覆盖时作对照。
- `random_top5`：request固定 seed，纯诊断。
- `candidate20_equal`：候选级收益对照，不冒充5槽portfolio。

## 10. Selection, PBO and Final Refit

1. 每个 trial 在28 paths上的主指标按8个基础块聚合。
2. 调用P0-C `calculate_policy_pbo()`，6 trials × 8 blocks必须完整；不可计算时保留原因但不自动停止训练。
3. winner按所有READY validation path的主指标均值最大选择；tie-break固定family_id、seed升序。
4. 同时报相对selection Top5的配对日差异、bootstrap CI和路径胜率；这些是研究结果，不是激活门禁。
5. final classifier 使用全部7,716成熟labels及逐块expanding causal HMM特征，不用一个最新HMM回填早期样本；另为未来runtime拟合截至最后candidate decision的`runtime_hmm_bundle`，只用于该日期之后推理。训练特征HMM identities与runtime HMM identity分别写入manifest；不得读取P0-C context-only未来label。
6. final模型输出概率必须有限且位于[0,1]；不做post-hoc Platt/conformal，本阶段校准状态为 `UNCALIBRATED`，Brier如实报告。

## 11. Bundle Contract / 不可变模型包

输出：`<output_root>/meta_label_bundles/<bundle_id>/`。

```text
manifest.json
training_request.json
policy_dataset_manifest.json
feature_schema.json
model.txt
fresh_hmm_models.json
fresh_hmm_unavailable.json
walk_forward_hmm_receipt.json
family_specs.json
cpcv_trial_metrics.parquet
cpcv_block_scores.parquet
pbo_receipt.json
winner_receipt.json
baseline_comparison.json
training_log.json
resource_report.json
```

bundle id由request hash及所有功能文件hash派生；运行耗时/创建时间不参与身份。完整目录原子发布，全文件hash/size/readback；相同id不同内容fail closed。

loader/scorer必须精确接收bundle root/id/manifest hash，不扫描latest。输出：

```text
take_probability
skip_probability = 1 - take_probability
advisory_model_confidence = abs(take_probability - 0.5) * 2
entry_priority_rank
selection_exit_rank
model_status = EXPERIMENTAL_MODEL
calibration_state = UNCALIBRATED
```

## 12. Components / 实现边界

新增：

```text
backend/services/advisory_model_first/meta_label_contracts.py
backend/services/advisory_model_first/meta_label_features.py
backend/services/advisory_model_first/meta_label_training.py
backend/services/advisory_model_first/meta_label_evaluation.py
backend/services/advisory_model_first/meta_label_bundle.py
backend/services/advisory_model_first/meta_label_pipeline.py
scripts/advisory_meta_label_prepare_request.py
scripts/wsl/advisory_meta_label_train.py
```

最小修改：

- `shadow_portfolio_policy.py` 增加显式dual-rank replay接口，不改变selection baseline现有函数。
- `policy_cpcv.py` 复用PBO，不改P0-C bundle。
- ownership catalog精确增加CLI与设计文件。

本阶段不改API/UI/P0-A/P0-B；runtime integration等P0-A/P0-B合入后，以独立小提交把meta-label loader接入dynamic descriptor。

## 13. API / UI / Database Contracts and Impact

- API/UI：none。
- DDL/DML：none。
- 后端重启：none。
- scheduler/descriptor/model activation：none。
- Paper/Simulation/QMT/Selection writes：none。

## 14. Verification Plan / 验证方案

### 14.1 Identity/boundary

- request和bundle canonical hash、exact P0-C readback、repo commit、file roots。
- Windows执行formal training fail closed；WSL环境/Conda可见。
- 禁止DB/runtime/Historical Range/Paper/Simulation imports。

### 14.2 Leakage

- 每个path train/validation identity与P0-C一致。
- HMM每块train_end严格早于validation block start，early block无历史时typed unavailable。
- categorical/winsor/early stopping均只读path train/validation；旧80日test完全不参与。
- 特征列不含任何label/path/future字段。

### 14.3 Training/evaluation

- 2 families × 3 seeds × 28 paths均有状态和指标。
- 相同request同一trial预测/metrics确定性一致。
- dual-rank adapter以selection rank退出、model rank入场的手工事件路径一致。
- PBO 6×8矩阵完整或明确不可计算原因。
- selection/M5A/HMM/random/candidate20基线状态真实。

### 14.4 Real WSL

- 真实P0-C bundle训练非空meta-label模型，概率有限。
- 记录总耗时、peak RSS、每path/family耗时、HMM缓存规模。
- immutable bundle全文件readback和精确重试。
- 结果无论优劣均保存；不以规则或selection结果替代模型。

## 15. Rollout / Rollback

### 15.1 Rollout

1. P0-D源码合入需用户确认；无运行时效果。
2. 正式WSL训练生成experimental bundle，不自动激活。
3. P0-A/P0-B合入、DDL、descriptor写入、用户重启和scheduler激活分别授权。
4. 只有dynamic descriptor显式绑定P0-D bundle后，P0-A才发布meta-label challenger observation；baseline不变。

### 15.2 Rollback

- 源码回滚不触碰数据库和P0-C产物。
- 未激活bundle保留真实实验结果，不自动删除。
- 训练语义缺陷注册BUG，旋转request/bundle，不原地改写。

## 16. Risks / 风险

| 风险 | 处理 |
|---|---|
| HMM fold泄漏 | block-start前历史拟合，禁止全局HMM |
| 28×6训练耗时 | HMM按8块缓存、4 threads、文件矩阵复用，小时级 |
| model rank误驱动退出 | dual-rank adapter，selection_exit_rank独立断言 |
| family搜索过拟合 | 固定2 family×3 seed，PBO完整报告 |
| 概率未校准 | 明确UNCALIBRATED，不伪造validated probability |
| 模型不提升 | 保存结果，保持selection baseline，不转向平台工程 |
| 旧test再次被调参 | 代码边界与receipt禁止进入selection inputs |
| 激活污染baseline | 本阶段无descriptor/runtime写入 |

## 17. Design Acceptance Index

| ID | requirement |
|---|---|
| F-601 | 冻结request精确绑定P0-C bundle/QE/repo/family/seed/metric identities |
| F-602 | path-local特征与HMM无未来拟合泄漏 |
| F-603 | 固定2 families×3 seeds真实LightGBM binary训练 |
| F-604 | take/skip/confidence概率输出与候选分类指标完整 |
| F-605 | dual-rank portfolio只用model rank入场、selection rank退出 |
| F-606 | 28 paths全部trial结果与8-block聚合完整 |
| F-607 | PBO和winner选择使用冻结主指标/tie-break |
| F-608 | selection/M5A/HMM/random/candidate20基线同policy可比 |
| F-609 | full-history refit不读取context-only future labels |
| F-610 | immutable meta-label bundle全文件readback和精确重试 |
| F-611 | exact loader/scorer不扫描latest且输出typed experimental状态 |
| F-612 | WSL-only、8GB内存、小时级资源报告 |
| F-613 | 零DB/API/UI/runtime/shared-module write与零自动激活 |
| F-614 | 无简化版、静默错误、业务漂移、审批或额外门禁 |

## 18. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-601 | `meta_label_contracts.py` | `backend/tests/advisory_model_first/test_meta_label_contracts.py` | pass | none |
| F-602 | `meta_label_features.py` | `backend/tests/advisory_model_first/test_meta_label_features.py` | pass | none |
| F-603 | `meta_label_training.py` | `backend/tests/advisory_model_first/test_meta_label_training.py` | pass | none |
| F-604 | `meta_label_training.py`, `meta_label_evaluation.py` | `backend/tests/advisory_model_first/test_meta_label_training.py` | pass | none |
| F-605 | `shadow_portfolio_policy.py` | `backend/tests/advisory_model_first/test_meta_label_portfolio.py` | pass | none |
| F-606 | `meta_label_pipeline.py` | `backend/tests/advisory_model_first/test_meta_label_pipeline.py` | pass | none |
| F-607 | `meta_label_evaluation.py` | `backend/tests/advisory_model_first/test_meta_label_evaluation.py` | pass | none |
| F-608 | `meta_label_pipeline.py` | `backend/tests/advisory_model_first/test_meta_label_evaluation.py` | pass | none |
| F-609 | `meta_label_pipeline.py` | `backend/tests/advisory_model_first/test_meta_label_features.py` | pass | none |
| F-610 | `meta_label_bundle.py` | `backend/tests/advisory_model_first/test_meta_label_bundle.py` | pass | none |
| F-611 | `meta_label_bundle.py` | `backend/tests/advisory_model_first/test_meta_label_bundle.py` | pass | none |
| F-612 | `meta_label_pipeline.py`, WSL CLI | `backend/tests/advisory_model_first/test_meta_label_pipeline.py`; artifact: `meta_label_bundle/manifest.json` | pass | none |
| F-613 | import/scope boundary | `backend/tests/advisory_model_first/test_meta_label_boundaries.py` | pass | none |
| F-614 | repeated design/source review | `python -m nox -s advisory_modeling_backend` | pass | none |

## 19. DESIGN-COMPLIANCE-001

1. **无简化版**：真实P0-C labels、真实path-local特征/HMM、真实LightGBM、全部trials、policy portfolio和bundle。
2. **无静默错误**：每个path/family/HMM/baseline均有typed状态；不补0、不回退规则、不跳过失败trial。首轮正式矩阵任一已声明trial异常均整体fail closed并修复，不能捕获后继续选择剩余trial。
3. **无业务偏移**：模型只改变新入场优先级；selection exit rank、shadow policy、baseline、其它模块均不变。
4. **无额外门禁审批**：PBO/CI/效果仅研究结果；无角色、人工审批、自动激活或收益门槛。

设计审核记录：

- Round 1：基于P0-C真实正负类/持有期/退出原因收敛为2 family×3 seed，删除开放式45-trial搜索。
- Round 2：引入dual-rank adapter，防止model entry rank静默改变rank-exit policy。
- Round 3：否决“每path非连续train直接拟合HMM”，改为8个时间块按块起点只用过去数据的causal walk-forward HMM。
- Round 4：禁止final refit用最新HMM参数回填早期训练样本；final classifier继续使用逐块walk-forward states，runtime latest-cutoff HMM仅服务未来推理且单独标识。
- Round 5：基础特征和8块HMM特征按request只构建一次并落Parquet，28 paths只切片训练；避免重复行情读取/特征计算而不引入通用缓存平台。
- Round 6：早期块不足120日时保留NaN+missing indicator，禁止未来回填但不阻断整个HMM family；避免所有path因共同包含冷启动块而不可计算。
- Source Round 1：CORE同时移除HMM原值与HMM missing indicators；validation block尾部持仓继续结算并计入block score，context-only日禁止新入场。
- Source Round 2：行情/rank context读取至P0-C source request data cutoff；factor H5 cutoff作为P0-D request显式身份独立验证，禁止用最后candidate target截断持仓结算或猜测factor版本日期。
- Source Round 3：文件读取起点早于HMM训练起点至少60个交易日，保证20日sector observation与60日个股特征warmup；warmup不进入labels或HMM fit日期。
- Source Round 4：传给fresh HMM的inference calendar从HMM history start开始，warmup行情只用于构造首日observations；避免warmup前空日期被误判为continuation gap。
- Source Round 5：不以宽泛catch把失败trial降级后继续选winner；已声明6×28矩阵任一异常整体fail closed，修复后重跑完整矩阵。

## 20. Implementation Plan / 实施方案

1. F2 validator与重复设计审核。
2. 实现request、path-local features/HMM、binary training、dual-rank evaluator、PBO/winner、bundle/CLI。
3. 定向测试后重复源码审核修复，直到DESIGN-COMPLIANCE-001逐项通过。
4. 完整`advisory_modeling_backend`、ownership/classifier/static gate。
5. 建立P0-D PR；合入等待用户确认。
6. 以最终源码SHA在WSL执行真实训练和精确重试，记录结果；不自动激活。

## 21. Production Gates / 生产影响与独立授权

本阶段没有业务门禁或审批：

| action | 状态 | 独立授权 |
|---|---|---|
| P0-D源码/PR合入 | pending user confirmation | 必须 |
| DEV/生产DDL/DML | none | 不适用 |
| backend restart | none | 不适用 |
| descriptor写入/模型激活 | out of scope | 后续单独授权 |
| 正式WSL训练 | 长任务实验步骤 | 只写repo-external model artifacts |

## 22. Completion Definition

- 14项验收矩阵均有实际实现与测试证据。
- 6 trials×28 paths、8-block PBO、winner和所有baselines状态可读。
- 真实WSL final bundle加载后对相同输入确定性输出take/skip/confidence。
- 训练在8GB内、小时级完成并精确重试。
- 不触碰数据库、服务、runtime、descriptor或其它模块。
- 模型不提升也如实交付实验结果，不回到平台工程。
