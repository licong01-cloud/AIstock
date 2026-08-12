# Advisory Model-First M5 模型质量迭代 F2 详细设计

> 日期：2026-08-11
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 当前阶段：`M5A_SOURCE_IMPLEMENTED_PENDING_POST_MERGE_TRAINING`
> 当前实施范围：`M5A_TOP5_QUALITY_ONLY`
> 训练环境：WSL Conda `rdagent-gpu`

## 1. Background / 当前真实问题

M1-M4 已完成真实训练和荐股运行时接入，但“模型可以输出”不等于“模型排序有效”。当前 M1 bundle
`9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629` 的冻结 80 日 test 结果为：

| 方法 | 5 日平均超额收益 | 绝对胜率 | 超额胜率 |
|---|---:|---:|---:|
| M1 model Top5 | -0.0002833 | 0.5025 | 0.4600 |
| 原始 selection rank Top5 | 0.0085591 | 0.5375 | 0.4825 |
| HMM Top5 | 0.0040167 | 0.5225 | 0.4850 |
| 随机 Top5 | 0.0055652 | 0.5275 | 0.4975 |
| Top20 等权 | 0.0067497 | 0.5378 | 0.4966 |

M1 `best_iteration=1`，说明当前单一 LambdaRank 配置和 validation NDCG 早停没有学到可稳定替代原始排序的经济关系。M5A 的第一目标不是增加平台能力，而是在同一批已有 QE 文件和冻结候选上，找到对 Top20→Top5 真正有增量的有限模型方案；如果没有观察到增量，必须如实报告，不得用 test 调参或把原始排序冒充模型结果。

M3 当前概率 AUC 多数接近 0.5 且仍为 `UNCALIBRATED`；M4 entry-gap q10-q90 test coverage 为 0.72795，低于名义 0.8。它们属于 M5B/M5C，排在 M5A 后，不得混入第一批代码扩大范围。

## 2. Scope / 当前范围

### 2.1 M5A 本阶段实现

1. 精确复用 M1 的 406 个 decision dates、8120 个 runtime-equivalent Top20 候选、103 个特征、标签、成本口径和冻结 test。
2. 在 train/validation 内比较有限的窗口、算法和随机种子。
3. 对每个模型家族使用多种子 rank ensemble，并显式比较与 selection prior 的融合权重。
4. 只用 validation 选择唯一 winner；winner 冻结后才读取 test 一次。
5. 生成完整 tournament receipt、test report 和新的实验性 reranker bundle；只有非零模型权重 winner 才产生可绑定模型 bundle。
6. 后续源码阶段接入现有 Advisory model-shadow exact binding；不修改荐股列表、规则荐股或 Review transition。

### 2.2 后继阶段

- M5B：M3 binary 概率 calibration 与收益/MFE/MAE quantile coverage 修正。
- M5C：M4 entry-gap quantile coverage 修正；entry executable 二分类继续保持未校准，直到同一目标包拥有足够权威负例。
- M5D：新 bundle exact binding、用户重启和真实单/原生多 Alpha readback。

M5B/M5C 在各自实施前补充独立 acceptance rows；本设计不允许在 M5A 代码中提前夹带实现。

## 3. Non-goals / 明确禁止

- 不读取生产数据库重建多年训练历史。
- 不启动 Historical Range、Phase 1R、capture/build/SEALED、CAS、source revision 或历史固化。
- 不处理旧 batch、旧 artifact root、orphan build、归档或 GC。
- 不增加自动训练、ModelOps、漂移平台、角色、审批或人工准入门禁。
- 不修改 Selection、StrategyPackage、Paper、模拟盘、QE 策略包资产或共享推理基础设施。
- 不使用分钟线；M5A、M5B、M5C 都是日线级模型。
- 不用其它实验、其它策略包或后来生成的新模型预测回填目标父包历史。
- 不把 test、当前实时行情或目标日未来行情用于 trial 选择、早停、融合权重或 feature selection。

## 4. 不变业务边界

1. 模型训练只在 WSL `rdagent-gpu` 执行；Windows 只生成请求、调用 WSL 和读取结果。
2. 基础行情训练输入继续使用现有 QE H5/Parquet/Qlib Bin；已有腿预测继续读取冻结 PKL。
3. 正式预测继续只读取数据库 decision cutoff 行情和当次 Program/策略包真实输入。
4. M5 模型是 `EXPERIMENTAL_SHADOW`，不下单、不影响模拟盘，也不替代人工买入决定。
5. 模型不可用或候选级推理失败时保持现有 typed unavailable；规则荐股、M2/M3/M4其它可用子信封继续运行。
6. 单 Alpha和原生多 Alpha按 package/manifest/style独立 exact binding；没有对应 bundle 时不跨包套用。

## 5. Architecture / 目标架构

```text
existing M1 frozen request + candidates/features/labels/split
  -> deterministic projection builder (只按冻结split分区，不计算test指标)
  -> Stage A train request + train/validation projection (不包含 test rows/path)
  -> 3 windows x 3 families x 5 seeds
  -> validation ensemble + selection-prior weight comparison
  -> immutable winner receipt
  -> Stage B evaluation request (winner receipt + frozen test projection identity)
  -> Stage B test evaluator
  -> tournament/test reports
  -> optional advisory_model_bundle_v2
  -> exact shadow binding
  -> existing Advisory model-shadow API/UI
```

投影builder只执行按既有split的确定性行分区和hash，不训练、不计算test标签统计、不输出test指标。Stage A和Stage B是两个独立CLI进程。Stage A request不包含test projection路径，读取器也拒绝`split=test`；Stage B只接受已冻结winner hash和单独生成的evaluation request，不能启动新trial或改写winner。该边界使“winner前训练流程不读test”成为代码和进程输入合同，而不是依赖开发者自觉。

M5A复用现有 FeatureBuilder、标签和Advisory推理入口，不复制第二套特征计算，不新增数据库、任务调度或模型注册平台。

### 5.1 Authority / 精确输入身份

M5A拆分两个request：

- `advisory_reranker_quality_train_request_v1`只提供train/validation projection；
- `advisory_reranker_quality_test_request_v1`只在winner receipt落盘后创建，绑定winner和test projection。

train request必须绑定：

- `parent_bundle_id=9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629`；
- M1 `training_request.json`、`feature_schema.json`、`label_policy.json`、`split.json` 的 SHA256；
- train/validation candidates/features/labels projection的路径身份、SHA256、行数和日期范围；
- `package_id`、`manifest_sha256`、`style_profile_id/hash`；
- 当前代码 commit、WSL Conda 环境名、LightGBM版本；
- 本设计冻结的 trial matrix、selection policy 和 test-once policy。

test request必须绑定parent split hash、winner receipt hash、test projection路径/SHA256/行数/日期和唯一evaluation id。winner receipt生成前拒绝创建test request。

两个request身份都不包含输出路径或创建时间。输入hash不一致时typed failure，不扫描latest，不自动寻找替代文件。该绑定只服务于本次模型训练和评价可复现性，不扩展成历史证据平台。

## 6. Contracts / Split与test-once

沿用 M1 已冻结日期成员，不改变原 test：

```text
pre_test_train_and_validation = existing train + existing validation
existing purge dates          = never fit, never score for selection
frozen_test                    = existing 80 decision dates
```

M5A trial 只使用既有 train 和 validation：

- window 只裁剪既有 train 的起点，不把 purge、validation 或 test 移入 train；
- validation 日期和标签只用于 early stop、family/weight选择和稳定性比较；
- test projection路径在winner identity冻结前不得进入trial worker或train request；
- winner 冻结后执行一次 test prediction/report；失败重试必须使用同一 winner identity，不得重新选择参数。

`test_once_receipt` 保存winner hash、test request hash、test input hash、开始/结束时间、退出状态和报告hash。Stage A仅接收剔除test行的train/validation投影；Stage B在读入winner receipt前拒绝打开test projection。它不是审批门禁，只防止代码静默把test变成调参集。

## 7. M5A trial matrix

### 7.1 窗口

只比较三个已有 train 尾部窗口：

```text
EXPANDING_ALL = 全部既有 modelable train dates
ROLLING_160   = 最近 160 个 modelable train dates
ROLLING_120   = 最近 120 个 modelable train dates
```

不足指定日期数时该 window typed unavailable，不缩短成未声明窗口。3/5年窗口不在本阶段范围。

### 7.2 算法家族

固定三个真实 LightGBM 家族，均使用同一103特征：

1. `LAMBDARANK_NDCG5`：现有 relevance 0..4 和 `label_gain=[0,1,3,7,15]`。
2. `RANK_XENDCG5`：同一 relevance，使用 LightGBM `rank_xendcg`，降低单一 LambdaRank 配置敏感性。
3. `REGRESSION_L1_UTILITY5`：以连续 `utility_5` 为目标，验证是否连续收益目标比离散 relevance 更适合当前小样本。

每个家族只有一组冻结树参数；M5A 不做大规模超参搜索。共同参数维持 `num_threads=4`、deterministic和受控内存；家族特有 objective/metric 进入 request。

### 7.3 种子与 ensemble

固定种子：`[20260808, 20260817, 20260829, 20260843, 20260871]`。种子只改变 bagging/feature sampling，不改变 split、标签或特征。

同一 `(window, family)` 的五个 booster 构成一个 family candidate。每个 decision group 内：

1. 每个 booster raw score 按 `score DESC, instrument ASC` 转成 `[0,1]` percentile rank；
2. 五个 percentile 等权平均为 `ensemble_score`；
3. 不按 test 表现选择单个种子。

任一 booster 缺失、空文件、行数/候选不一致时该 family candidate 显式失败，不用剩余种子构成简化 ensemble。

### 7.4 selection prior融合

原始 selection rank 转为：

```text
selection_prior = (group_size - selection_effective_rank) / max(group_size - 1, 1)
```

对每个 family candidate只比较：

```text
model_weight in [0.25, 0.50, 0.75, 1.00]
final_score = model_weight * ensemble_score + (1 - model_weight) * selection_prior
```

`model_weight=0` 只作为 `SELECTION_PRIOR_ONLY` 对照，不是可发布模型。融合权重必须写入 bundle；运行时按同一公式计算，不得训练时融合、在线时只用 booster。

## 8. Validation winner policy

每个 `(window, family, model_weight)` 在既有 validation 上按 decision date 选 Top5并计算：

- `mean_daily_top5_excess_return_5`；
- `median_daily_top5_excess_return_5`；
- absolute/excess hit rate；
- date-level NDCG@5；
- shortlist turnover；
- 五种子 raw ranking 的两两 Spearman均值和最差值；
- 相对 `selection_rank_top5` 的逐日 excess-return lift。

winner 排序固定为：

1. `mean_daily_top5_excess_return_5` 降序；
2. `median_daily_top5_excess_return_5` 降序；
3. `excess_hit_rate` 降序；
4. `shortlist_turnover` 升序；
5. `window_id/family_id/model_weight` 字典序。

同时计算 `SELECTION_PRIOR_ONLY`。若其 validation primary metric 排第一，则结果状态为 `NO_VALIDATION_MODEL_LIFT_OBSERVED`：仍生成完整 receipt和一次冻结 test 对照，但不发布新的模型 bundle、不改 active binding。该状态是诚实的实验结果，不是审批或额外业务门禁。

不得设置隐藏最低收益、p-value、胜率或校准阈值。bootstrap只提供区间，不决定代码是否“成功”。

## 9. Test report

winner 固定后，冻结 test 只报告：

- 与当前 M1、selection rank、HMM、随机和Top20等权相同口径的全部指标；
- 每个 decision date 的 Top5符号、分数、rank和逐日收益；
- 相对 selection rank 的逐日 lift均值、中位数和95% moving-block bootstrap区间；
- 五种子 ensemble稳定性；
- train/validation/test日期和行数；
- wall time、CPU threads、峰值RSS和artifact大小。

bootstrap固定 `replicates=1000`、seed由 request hash前8字节确定、block length=5个decision dates。它只量化不确定性，不作为发布或页面门禁。

test结果无论正、负或无差异都必须原样保存。禁止根据test结果新增trial、改权重、换seed、换窗口或重新定义label。

## 10. Bundle / runtime 合同

当 winner 的 `model_weight>0` 时发布 `advisory_model_bundle_v2`：

- 五个非空 booster文件和逐文件SHA256；
- `window_id/family_id/seeds/model_weight`；
- `ensemble_score_policy=PERCENTILE_RANK_MEAN_V1`；
- `selection_prior_policy=SELECTION_EFFECTIVE_RANK_PERCENTILE_V1`；
- parent M1 bundle/input/style/feature/label/split身份；
- tournament receipt和test report SHA256；
- `calibration_state=NOT_APPLICABLE_RANKING_SCORE`；
- 现有103特征schema和categorical vocabulary。

publish采用临时目录、完整读回后原子rename。五模型少一项即拒绝；不得发布单seed简化包。

runtime loader 显式支持 v1单booster和 v2五booster；v2按manifest公式计算最终分数。loader不得把v2加载失败降级成v1、selection rank或latest bundle。现有v1 binding继续有效，回滚只需恢复旧exact binding。

## 11. M5B/M5C 校准边界

### 11.1 M3 binary

M5B 对每个 horizon 的positive-excess和signal-survival head使用validation预测拟合Platt calibrator，test只评价ROC-AUC、Brier、logloss和10-bin ECE。calibrator不改变raw model或排序；API同时保留raw与calibrated身份。validation缺任一类别时该head保持 `UNCALIBRATED`，不输出常数概率。

### 11.2 M3 quantile

收益、MFE和MAE q10/q90使用validation-only conformalized quantile adjustment，目标名义coverage保持0.8。调整值按horizon/head独立冻结；test只读评价。

### 11.3 M4

entry-gap q10/q90使用同样的validation-only coverage adjustment。entry-executable全量只有4个权威负例，当前不拟合Platt/isotonic，也不因高raw probability隐藏候选；继续显示 `UNCALIBRATED`。

## 12. Errors / 错误可见性

新增reason codes至少包括：

- `ADVISORY_M5_INPUT_IDENTITY_MISMATCH`
- `ADVISORY_M5_WINDOW_NOT_AVAILABLE`
- `ADVISORY_M5_TRIAL_FAILED`
- `ADVISORY_M5_ENSEMBLE_INCOMPLETE`
- `ADVISORY_M5_TEST_ACCESSED_BEFORE_WINNER_FREEZE`
- `ADVISORY_M5_BUNDLE_INCOMPLETE`
- `ADVISORY_M5_RUNTIME_POLICY_MISMATCH`

训练CLI以非零退出并写结构化failure receipt；`rdagent-gpu` 当前 Python 3.10 是必须兼容的真实运行环境，不得使用仅 Python 3.11+ 可导入的标准库符号。后台日志只记录request/trial/winner/reason/耗时/RSS，不输出逐行特征或无价值循环日志。在线候选级错误进入现有model-shadow子信封；共同bundle/identity错误使M5 reranker unavailable，但不阻断规则荐股或M3/M4。

## 13. 计划代码范围

M5A允许修改：

- `backend/services/advisory_model_first/quality_contracts.py`
- `backend/services/advisory_model_first/quality_tournament.py`
- `backend/services/advisory_model_first/quality_bundle.py`
- `backend/services/advisory_model_first/quality_pipeline.py`
- `backend/services/advisory_model_first/model_bundle.py`
- `backend/services/advisory_model_first/model_inference.py`
- `backend/services/advisory_model_first/runtime_bundle.py`
- `frontend/src/lib/api/advisory.ts` 中父重排模型 calibration state 的 v2 类型扩展
- `scripts/advisory_model_quality_prepare_request.py`
- `scripts/advisory_model_quality_train_wsl.py`
- `scripts/wsl/advisory_model_quality_train.py`
- `tests/aistock_validation/catalog/file_ownership.yaml` 中 `advisory_model_first_training` 的三个精确 CLI 路径登记
- 对应 `backend/tests/advisory_model_first/test_quality_*.py`
- 现有 model inference/runtime bundle直接依赖测试。

禁止修改 Selection、StrategyPackage、Paper、模拟盘、QE、Historical Range和数据库migration。若实现发现确需越界，停止并修订设计，不得静默扩大scope。

## 14. Implementation Plan

1. 冻结确定性projection builder、train/test双request、trial matrix、test-once和reason-code合同。
2. 实现三窗口×三家族×五种子tournament，使用日期/候选分批和临时Parquet，峰值RSS低于8GB。
3. 实现validation family ensemble、selection-prior融合、winner冻结和test-once报告。
4. 实现v2五booster bundle原子发布、readback和tamper检测。
5. 实现runtime v1/v2显式分支和exact binding，不改变M3/M4或荐股列表。
6. 完成定向测试、F2 validator、ownership、lint和DESIGN-COMPLIANCE-001。
7. 合入源码后在WSL执行真实M5A训练；训练、bundle生成、binding、用户重启和deployed readback分别报告。
8. M5A真实结果完成后再开始M5B详细实现，不提前混入。

## 15. Verification Plan

- request：train/test双身份稳定、created/output不入hash、trial matrix不可增删、winner前train request无test路径。
- window：精确日期裁剪、purge/validation/test不进入train、窗口不足typed failure。
- family：三个真实LightGBM objective、五种子完整、模型非空、无mock/常数fallback。
- ensemble：组内percentile、instrument tie-break、五模型均值和selection prior公式golden vector。
- winner：validation-only排序、确定性tie-break、prior-only结果不发布伪模型。
- test-once：winner前禁止读取、同winner精确重试、test不反向改变trial。
- bundle：v2五模型完整性、member hash、atomic publish、tamper、path containment。
- runtime：v1保持不变、v2公式parity、exact package/style、错误隔离、M2候选顺序合同。
- WSL：真实环境、wall time、RSS<8GB、无Windows训练。
- boundary：受保护模块零diff、无DDL/DML、无角色/审批/二次准入。

只运行变更模块和真实依赖模块测试；广泛回归交由CI/Validation Center，不在本地重复全库测试。

## 16. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-360 | M5A精确复用M1候选、103特征、标签、split和冻结test，以确定性投影和train/test双request隔离，不重建历史数据 |
| F-361 | 三窗口、三模型家族和五种子矩阵完整执行，无subset或单seed简化 |
| F-362 | 五种子按组内percentile平均，selection prior融合公式训练/在线完全一致 |
| F-363 | winner只由validation决定，test在winner冻结前物理不可读且只评价一次 |
| F-364 | prior-only胜出时如实输出无模型增量，不发布或绑定伪模型 |
| F-365 | v2 bundle包含五个模型、完整hash、融合策略、tournament和test身份，原子发布/readback |
| F-366 | runtime显式支持v1/v2且exact package/style绑定；v2失败不降级或跨包套用 |
| F-367 | test报告与M1相同基线和经济指标，负结果完整保留，bootstrap不成为隐藏门禁 |
| F-368 | WSL执行器完整支持45-trial矩阵、Python 3.10、RSS/耗时记录和非零失败；合入前以真实五种子smoke验证可执行性，不新建缓存/证据平台；完整正式训练属于§14.7合入后运行任务 |
| F-369 | Selection、StrategyPackage、Paper、模拟盘、QE、Historical Range和数据库零写入 |
| F-370 | 无简化版、静默错误、业务语义漂移、角色审批、二次准入或未经确认门禁 |
| F-371 | M5B校准和M5C coverage调整不进入M5A首批代码，entry executable负例不足不伪造校准 |

## 17. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-360 | `backend/services/advisory_model_first/quality_contracts.py`, `backend/services/advisory_model_first/quality_pipeline.py`, existing M1 artifacts | `backend/tests/advisory_model_first/test_quality_contracts.py`, `backend/tests/advisory_model_first/test_quality_pipeline.py` | implemented_local_verified | none |
| F-361 | `backend/services/advisory_model_first/quality_tournament.py` frozen 3-window x 3-family x 5-seed matrix | `backend/tests/advisory_model_first/test_quality_tournament.py::test_tournament_executes_all_45_boosters_and_all_fusion_weights` | implemented_local_verified | none |
| F-362 | `backend/services/advisory_model_first/quality_tournament.py::apply_ensemble_scores`, `backend/services/advisory_model_first/model_inference.py::_score` v2 branch | `backend/tests/advisory_model_first/test_quality_scoring.py` | implemented_local_verified | none |
| F-363 | `backend/services/advisory_model_first/quality_contracts.py` dual request, `backend/services/advisory_model_first/quality_pipeline.py` Stage A/B and test-once receipt | `backend/tests/advisory_model_first/test_quality_contracts.py::test_test_request_cannot_be_created_before_winner_receipt_exists`, `backend/tests/advisory_model_first/test_quality_pipeline.py::test_stage_a_exact_retry_reuses_frozen_winner_without_retraining` | implemented_local_verified | none |
| F-364 | `backend/services/advisory_model_first/quality_contracts.py::QualityWinnerReceiptV1`, prior-only Stage B and publish refusal | `backend/tests/advisory_model_first/test_quality_contracts.py`, `backend/tests/advisory_model_first/test_quality_bundle.py` | implemented_local_verified | none |
| F-365 | `backend/services/advisory_model_first/quality_bundle.py`, `backend/services/advisory_model_first/model_bundle.py` explicit v2 validation/load | `backend/tests/advisory_model_first/test_quality_bundle.py` | implemented_local_verified | none |
| F-366 | `backend/services/advisory_model_first/model_bundle.py`, `backend/services/advisory_model_first/model_inference.py` explicit v1/v2 paths; `frontend/src/lib/api/advisory.ts` v2 calibration type | `backend/tests/advisory_model_first/test_quality_scoring.py`, `backend/tests/advisory_model_first/test_model_inference.py`, `backend/tests/advisory_model_first/test_review_regressions.py` | implemented_local_verified | none |
| F-367 | `backend/services/advisory_model_first/quality_pipeline.py` test report, current M1/selection/HMM/random/Top20 baselines and moving-block bootstrap | `backend/tests/advisory_model_first/test_quality_pipeline.py::test_moving_block_bootstrap_is_deterministic_for_frozen_seed`; command `python -m nox -s advisory_modeling_backend` | implemented_local_verified | none |
| F-368 | `scripts/advisory_model_quality_prepare_request.py`, `scripts/advisory_model_quality_train_wsl.py`, `scripts/wsl/advisory_model_quality_train.py` | `backend/tests/advisory_model_first/test_quality_tournament.py::test_tournament_executes_all_45_boosters_and_all_fusion_weights`；`rdagent-gpu` Python 3.10真实五种子单家族smoke：4957 train/validation rows、1599 test rows、80 test dates、5 non-empty boosters、五类baseline完整 | implemented_local_verified | none |
| F-369 | Advisory-only changed files; no protected module or DB diff | `backend/tests/advisory_model_first/test_quality_boundaries.py`; command `python scripts/aistock_module_ownership_scan.py --changed-only --fail-on-unmapped --fail-on-ambiguous` | verified | none |
| F-370 | DESIGN-COMPLIANCE-001 source review | command `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_model_first_m5_quality_iteration_f2_design_20260811.md --tier F2`; command `python -m nox -s advisory_modeling_backend` | implemented_local_verified | none |
| F-371 | M5A-only source; no M5B/M5C implementation/import | `backend/tests/advisory_model_first/test_quality_boundaries.py`; command `python scripts/aistock_module_ownership_scan.py --changed-only --fail-on-unmapped --fail-on-ambiguous` | verified | none |

## 18. Risks / 风险与处置

| 风险 | 直接处置 |
|---|---|
| 36个validation组合在50日上产生选择偏差 | 固定trial matrix、保存全部结果、报告block bootstrap区间和稳定性；不把区间变成门禁，不根据test追加trial |
| selection prior融合被误写成规则fallback | manifest保存非零`model_weight`和精确公式；prior-only只输出实验结果，不发布模型 |
| 五模型runtime增加耗时或内存 | booster体积、单次推理耗时和RSS进入真实WSL/runtime receipt；保持4线程和候选内批处理，不建常驻缓存 |
| v2 loader破坏现有v1 | v1/v2显式分支和完整v1回归测试；不原地改写现有bundle或binding |
| rank_xendcg在目标LightGBM不可用 | request冻结并验证目标版本支持；不替换objective或缩减矩阵，明确失败后修订设计 |
| validation winner在test转负 | 原样发布报告但不回调参数；是否激活新binding作为单独运行态动作报告，不伪造优化成功 |
| M5再次扩张为历史平台 | changed-file ownership和边界测试只允许§13文件；历史模块出现diff即判设计偏移 |

## 19. Production Gates / 生产影响（无新增业务门禁）

```text
production_ddl_gate = noop
production_dml_gate = noop
production_backend_dependency_gate = noop
production_frontend_dependency_gate = noop
backend_restart = user-owned; only after runtime source merge and binding activation
runtime_activation = exact M5 reranker binding, reported separately
```

上述字段只报告交付影响，不是应用运行时审批。训练无需启动、停止或重启后端；本设计不授权任何进程控制。

## 20. Rollback

- 训练失败：保留failure receipt，不发布不完整bundle。
- 无validation增量：保留完整结果，继续使用当前M1 exact binding。
- v2运行时失败：恢复旧v1 exact binding；M3/M4和规则荐股不变。
- 源码回滚与binding回滚分别执行；不删除历史bundle，不修改数据库。

## 21. DESIGN-COMPLIANCE-001 审核清单

1. **禁止简化版**：三窗口×三家族×五种子和五模型bundle必须完整，不能只交付单trial或mock。
2. **禁止静默错误**：输入、window、trial、ensemble、test访问、bundle和runtime错误均有typed reason和日志。
3. **禁止业务语义偏移**：仍是每个Program独立Top20→Top5影子重排，不修改候选来源、荐股列表或交易模块。
4. **禁止未经确认门禁/审批**：没有角色、审批、质量阻断或二次准入；prior-only是实验结果语义，bootstrap只报告区间。
