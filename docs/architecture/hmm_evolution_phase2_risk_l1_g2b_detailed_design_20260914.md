# HMM Evolution Phase 2 G2-B `risk_L1` 详细设计

> **版本**：v0.2
> **日期**：2026-09-14
> **设计层级**：F2
> **状态**：DESIGN_APPROVED_IMPLEMENTATION_READY
> **批准记录**：2026-09-14 用户批准 G2B-RL1-D1～D6，并授权提交、合入本设计及同步父蓝图/G2-C 相邻设计；源码、12-fit development、DDL/DML、tail、runtime activation 与进程控制仍是后续独立状态。
> **父蓝图**：`hmm_evolution_and_risk_management_system_design_20260716.md`
> **终极目标**：对 31 个申万一级板块提供因果可得、逐日可解释的未来相对下行风险提示；不把轮动弱势、HMM 隐状态或工程成功冒充风险预测有效性。

## 0. Background（背景）与当前事实

G2-A v1.6 已交付真实 L1 轮动 research capability；G2-C 已完成连续零 fit 验证，并于 2026-09-14 在既有 DEV 数据库 `aistock_dev` 对两个相邻日期完成受控写入、回读和同请求幂等重放：

- `2026-08-27/as-of 2026-08-26`：31 行、31 sectors、revision 1；
- `2026-08-28/as-of 2026-08-27`：31 行、31 sectors、revision 1；
- 同输入重放 `2026-08-27` 后行数、revision 与 canonical row hash 不变；
- 三次执行合计 `model_fit_count=0`、`tail_accessed=false`、`target_columns_read=false`、`runtime_action_performed=false`。

当前 `rotation_L1` 仍为 forward 未确认的研究预测，`risk_L1|rotation_L2|risk_L2` 均为 typed `NOT_AVAILABLE`。父蓝图要求日更新稳定后只选择一个完整扩展能力；本设计选择 `risk_L1`，因为风险提示是终极目标的第二条主轴，而 L2 只是扩大呈现粒度。

旧 P2-4 风险逻辑把“进入 fading 且发生状态变化或 market risk-off”当成 warning。在其已消费窗口中，L1 结果为：

- event base rate=`0.09864036256998134`；
- precision=`0.15029469548133595`；
- precision lift=`0.051654332911354614`；
- recall=`0.20675675675675675`。

该历史结果只说明“轮动态派生 warning”没有达到既有风险门槛，不作为本候选的训练或验收结果，也不重跑、不改写。新候选直接以风险事件为训练目标，避免再次把 descriptive state 当预测器。

## 1. Scope（范围）与非目标

### 1.1 本设计包含

1. 一个且仅一个 `risk_L1` 候选：固定浅层 LightGBM binary classifier；
2. 31 个 SW2021 L1 sector、10 个交易日未来相对不利路径标签；
3. 五折因果 rolling development OOF、双 fresh-process 和一个冻结 full-development 模型；
4. `risk_score`、`high|watch|normal|unavailable`、解释、coverage、效果与 forward 状态；
5. 一个最小 repository table、两个 read API，以及在现有 `/hmm-risk` 页面增加同级风险视图；
6. 与 G2-C 相同的显式 candidate、`trade_date=t/as_of_date=t-1`、零 fit 单日推理边界。

### 1.2 明确不包含

- 不实施 `rotation_L2` 或 `risk_L2`；
- 不复用 fading、market regime 或 hidden-state index 作为默认风险答案；
- 不新增 HMM/jump fit、seed grid、模型搜索、第二候选或结果后调参；
- 不建设 scheduler、queue、alert event lifecycle、feature store、evidence 平台或训练平台；
- 不接入 Selection、Paper、QE、QMT、订单或调仓；
- 不读取 sealed tail，不执行 DDL/DML、runtime activation、依赖安装或进程控制；
- 不重新导出 direct-v2 数据，不修改数据生产或 canonical PIT authority。

## 2. Architecture（架构）

```text
显式 direct-v2 v3 + C-013 PIT + SW L1/CSI300 close
                         │
          既有 t-1 feature 构造 + 10D risk label
                         │
       固定五折/504日 rolling + purge=10（development）
                         │
       唯一浅层 binary GBDT，双 fresh-process，共12 fits
                         │
      OOF risk score/level + 效果与coverage独立验收
                         │
        冻结 full-development model（单日推理0 fit）
                         │
      最小 risk repository → 两个 read API → 现有页面风险视图
```

`risk_L1` 是独立 component，但复用 G2-A/G2-C 已验证的数据 reader、PIT/停牌/provider-absence 语义、日期边界、hash 工具、revision/readback 方式和页面骨架。不得复制一套通用平台，也不得把 rotation table 的字段改名后冒充风险产品。

## 3. Approved Contracts（D1～D6）

### G2B-RL1-D1：风险事件、时间边界与呈现语义

推荐唯一标签为 10 个交易日最大相对不利路径：

```text
sector_cum(t,k)    = product_{i=1..k}(1 + sector_return[t+i]) - 1
benchmark_cum(t,k) = product_{i=1..k}(1 + CSI300_return[t+i]) - 1
relative_adverse_excursion_10d(t)
                    = min_{k=1..10}(sector_cum(t,k) - benchmark_cum(t,k))
risk_event_10d(t)  = relative_adverse_excursion_10d(t) <= -0.05
```

- decision date 为 `t`，所有 feature 只读至 canonical `t-1`；标签只读 `t+1..t+10`，不得进入单日推理；
- benchmark 固定同 release `000300.SH`；sector close 固定同 release 31 个 SW2021 L1 index；
- 任一所需日缺失、非有限、重复或非正 close 时该 label typed unavailable，不缩短 horizon、不前填、不补 0；
- 产品输出的 `risk_score` 是相对排序分数，不是校准违约概率；UI 必须直说；
- `risk_level` 固定为 `high|watch|normal|unavailable`，不复用 `trending|neutral|fading`。

选择该标签的理由是它直接回答“未来十日内是否出现显著跑输市场的下行路径”，并延续旧风险合同的 `-5%/10D` 业务量级。替代的“10D 终点收益低于阈值”会遗漏中途明显下挫后修复的风险；HMM 状态缺失或 fading 则不等于未来损失。

**状态：APPROVED_BY_USER_20260914。**

### G2B-RL1-D2：唯一模型、feature 与结构安全

唯一候选为 `hmm_risk_risk_l1_g2b_v1`，使用现有 LightGBM 4.6.0，不新增依赖。输入固定为以下九列，保持既有业务公式：

```text
relative_momentum_5d
relative_momentum_10d
relative_momentum_20d
relative_momentum_60d
relative_downside_volatility_20d
relative_max_drawdown_20d
pit_breadth_above_ma20
moneyflow_intensity_20d
moneyflow_intensity_delta_5d
```

每个 decision date 对可用的 canonical L1 sector 逐列使用 average-rank 映射至 `[-0.5,0.5]`。`relative_downside_volatility_20d` 与 `relative_max_drawdown_20d` 为每行必需；其余七列至少六列有限，总计至少八列有限。LightGBM 原生 missing 只处理这一个已批准缺一列边界，不允许插值、补 0、补中位数或新增 missing indicator。

固定模型 profile：

```text
LGBMClassifier(
  boosting_type="gbdt", objective="binary", n_estimators=240,
  learning_rate=0.03, max_depth=3, num_leaves=7,
  min_child_samples=310, min_child_weight=0.001,
  min_split_gain=0.0, max_bin=63, min_data_in_bin=3,
  subsample=1.0, subsample_freq=0, colsample_bytree=1.0,
  reg_alpha=1.0, reg_lambda=10.0, path_smooth=0.0,
  class_weight="balanced", random_state=42, deterministic=true,
  force_col_wise=true, n_jobs=1, feature_pre_filter=false,
  use_missing=true, zero_as_missing=false, extra_trees=false,
  max_delta_step=0.0, subsample_for_bin=200000,
  importance_type="split", verbosity=-1
)
```

- 不使用 early stopping、validation metric 选树、grid、第二 seed 或阈值搜索；
- 每个训练集必须同时存在正负类；建议正式 class sufficiency 为每类至少 `max(100, ceil(0.02*N_train_rows))`，不足即 `hmm_risk_risk_l1_train_class_insufficient`；`class_weight="balanced"`解析出的两个实际权重与训练类别计数必须进入 model/fit receipt 与 hash；
- 每叶 distinct decision dates 硬底线为 10，全部实际叶中少于 20 日的比例须 `<=1%`，沿用已验证的 GBDT 日期结构保护；
- 训练结束必须验证 240 棵树、全部参数有限、`predict_proba[:,1]` 有限且位于 `[0,1]`；概率仅命名为 `risk_score`；
- `pred_contrib` 在 logit 域重构 raw score，UI解释也必须标注为 log-odds contribution；重构失败是模型/批次完整性失败，不把单条坏结果降级后继续写其余 sector。

该方案比 LogisticRegression 多保留已知的非线性交互能力，但完全复用现有依赖与浅树安全边界；比新 HMM/jump 风险模型少一套状态、semantic mapping 和额外 fit。

**状态：APPROVED_BY_USER_20260914。**

### G2B-RL1-D3：development、复现与 fit 预算

- 复用 G2-A 已消费的五个 validation 窗口：
  `2023-09-04..2024-03-14`、`2024-03-15..2024-09-18`、
  `2024-09-19..2025-03-31`、`2025-04-01..2025-09-30`、
  `2025-10-01..2026-03-31`；
- 每 fold 使用 validation 前、purge 10 个 canonical open days 后的最后 504 个 train decision dates；`embargo=0`；
- feature lookback 可早于 train start，但不能成为训练样本；训练 label 的 `t+10` 必须处于该 fold 的 train 可见边界；
- 五 fold OOF 只由对应 fold model 生成，禁止 in-sample fitted prediction；
- full-development model 使用最后 504 个合法且 outcome 成熟的 train dates，冻结后供 label-free 单日推理；
- 每个 fresh process 执行 5 fold fits + 1 full-development fit，两个进程总计 12 fits；任何超出 12 的运行视为未批准搜索；
- 两进程读取同一 immutable input bundle，model text、OOF rows、risk score/level、metric payload 和 canonical hash 必须 bitwise 相同；数值 allclose 只用于定位；
- 不运行 horizon battery，10D 由 D1 业务事件定义直接冻结；不拟合 market context。

任一 source/calendar/fold/label/feature/fit/leaf/reproducibility 失败均停止，不读取 tail，不生成 component model 或产品成功状态。

**状态：APPROVED_BY_USER_20260914。**

### G2B-RL1-D4：风险投影、产品效果与能力状态

对每个 metric-valid date，在可用 sector 内对 `risk_score` 做 average rank：

```text
risk_percentile = (average_rank - 1) / (available_count - 1)
high   : risk_percentile >= 0.80
watch  : 0.60 <= risk_percentile < 0.80
normal : risk_percentile < 0.60
predicted_warning = (risk_level == "high")
```

相同 score 必须得到相同 average rank；不得用 sector code 打破业务 tie。边界 tie 可使某日 high/watch 数量偏离 7，但不得删除或随意升降 tied sector。少于 28/31 raw score available 时不得计算 percentile/level/warning；该日仍保留 31 行，但全部标记 `availability=unavailable`，使用 batch-derived `hmm_risk_risk_l1_daily_coverage_insufficient`，并在 evidence 中保留各行更上游的原始 missing reason，不能只从指标分母中静默删除该日。

Primary development risk metrics 使用 `high` 作为 predicted positive：

```text
base_rate        = actual_event_count / evaluable_count
precision        = TP / (TP + FP)
precision_lift   = precision - base_rate
recall           = TP / (TP + FN)
```

推荐沿用既有业务门槛：`precision_lift >= 0.10` 且 `recall >= 0.25`。不增加 AUROC、PR-AUC、季度全正或统计显著性 AND 门；这些只进入诊断。coverage 独立要求：每个 metric date 至少 28/31 available、全部 development metric-valid date ratio `>=90%`、每 sector availability ratio `>=90%`。

状态必须分层：

1. `risk_l1_research_surface_status=AVAILABLE_EXPERIMENTAL`：因果 OOF、identity、coverage、复现、writer/API/UI 工程闭合；不要求效果门通过；
2. `risk_l1_capability_status=RESEARCH_RISK_WARNING_AVAILABLE_FORWARD_UNCONFIRMED`：上述工程条件与两个 primary effect 门同时通过；
3. `ADVISORY_RISK_WARNING_AVAILABLE`：只允许未来另行授权的 sealed tail 实际 forward 合同通过后设置；
4. 其他情况为 typed `NOT_AVAILABLE`，但不能回滚已经诚实闭合的 experimental surface。

旧 P2-4 的 `0.05165/0.20676` 仅作基准说明，不能降低门槛、调模型或充当本候选结果。development 不通过时不读 tail、不自动开第二候选。tail 的 exact dates、功效与 passed/inconclusive/failed 需在未来读取前单独批准；本设计不预消费 tail。

最小 typed reason 集固定覆盖：`hmm_risk_risk_l1_target_unavailable`、`hmm_risk_risk_l1_feature_contract_invalid`、`hmm_risk_risk_l1_train_class_insufficient`、`hmm_risk_risk_l1_fit_failed`、`hmm_risk_risk_l1_leaf_date_coverage_insufficient`、`hmm_risk_risk_l1_score_invalid`、`hmm_risk_risk_l1_daily_coverage_insufficient`、`hmm_risk_risk_l1_metric_unavailable`、`hmm_risk_risk_l1_reproducibility_mismatch`。writer/readback 继续使用 component-specific conflict/readback reason；不得把这些状态压缩成 generic incomplete。

**状态：APPROVED_BY_USER_20260914。**

### G2B-RL1-D5：最小持久化、API/UI 与单日推理

建议新增一张表 `hmm_risk.risk_l1_prediction`，不修改 rotation 行语义，也不建设通用 alert/event schema。最小字段：

```text
prediction_id, product_bundle_id, trade_date, as_of_date,
sector_level='L1', sector_code, sector_name,
risk_score, risk_percentile, risk_level, predicted_warning,
feature_contributions, availability, reason_code,
risk_l1_research_surface_status, risk_l1_capability_status,
forward_power_status, forward_confirmation, advisory_status,
validation_basis, development_precision_lift, development_recall,
model_hash, input_hash, mapping_snapshot_hash, tail_accessed,
revision, supersedes_prediction_id, created_at
```

约束与现有 rotation repository 一致：31-row batch、model/date/sector/revision 唯一、相同 identity/payload 幂等、不同 payload 不覆盖、current revision 显式读取、canonical readback hash。availability 为 unavailable 时 score/percentile/level/warning 必须为 null 且 reason 非空；available 时四者完整、有限且 reason 为空。

只新增两个 read endpoint：

- `GET /api/v1/hmm-risk/risk-l1/overview`
- `GET /api/v1/hmm-risk/risk-l1?trade_date=...&model_hash=...`

前端在现有 `/hmm-risk` 增加同级 `L1 风险`视图，展示 31-sector risk score/level、high warning、具体 unavailable reason、as-of、model/input/mapping identity、development precision lift/recall、forward 状态和“非校准概率/仅研究”声明。不得新建空页面、mock 或静态矩阵。

单日执行复用 G2-C 的显式 candidate/authority 参数和共同水位：`trade_date=t`、`as_of=t-1`、冻结 full-development risk model，`model_fit_count=0`、`target_columns_read=false`、`tail_accessed=false`。缺输入、模型、31 分母或 readback 失败均 typed fail closed，不复用上一日 warning。

DDL、DEV/生产 DML、runtime receipt 和用户重启分别授权；源码/测试通过不能代报运行态可见。

**状态：APPROVED_BY_USER_20260914。**

### G2B-RL1-D6：实施顺序、验证与停止条件

批准后在一个完整 Feature 任务中实施，不拆成模型、repository、API、UI 四个微阶段：

1. 扩展现有 immutable builder，只增加 D1 标签与 D2 九列所需最小内容；不重新导出原始数据；
2. 实现唯一 12-fit development executor、双进程 closure、typed failure 与 compact acceptance；
3. 同一任务实现 model writer、单日期零 fit executor、最小 table migration、repository/API/UI；
4. 最多三轮代码审核与修复，完成所属 backend/frontend/F2 门禁；
5. 固定 merge commit 后才可另行授权正式 development 12 fits；
6. 只有 development 结构/coverage/效果状态真实闭合后，才按分层状态写入 DEV 并做无 mock 浏览器验证；生产与 runtime 仍需独立授权。

直接测试至少覆盖：

- 10D 路径标签端点、阈值、缺日、非有限和禁止 future leakage；
- 九列 identity/顺序、风险必需列、8/9 availability 与禁止填补；
- 单类/少类训练拒绝、固定 LightGBM profile、leaf date coverage、概率范围；
- five-fold rolling/purge、OOF 非 in-sample、12-fit 上限、双进程 bitwise closure；
- risk percentile tie、high/watch/normal、31 分母与 coverage；
- precision lift/recall、research/capability/forward 分层，失败不读 tail；
- model/hash/contribution reconstruction、单日零 fit/零 target；
- writer 幂等/conflict/revision/readback、API 404/409/500、UI loading/error/empty/stale/unavailable；
- rotation API/UI 与其他业务模块无回归、无交易副作用。

正式门禁：changed files → ownership → module registry → test plans、精确 pytest、Ruff、py_compile、frontend lint/typecheck/定向 Playwright、`hmm_risk_backend`、`validation_module_registry_l0`、`l0`、F2 validator 和 `git diff --check`。

停止条件：

- 设计未批准：不得实现；
- 源码/数据/fit/复现失败：typed 停止，不生成模型或产品成功；
- research 工程通过、效果未过：可形成显式 experimental surface，但 `risk_l1_capability_status=NOT_AVAILABLE`、tail 禁读；
- development 效果通过：只形成未 forward 确认的 research risk capability；tail/生产/runtime 不自动执行；
- 本唯一候选失败后返回用户，不自动调参、放宽门槛、换模型或开启 L2。

**状态：APPROVED_BY_USER_20260914。**

## 4. Design Acceptance Index

| design_item | decision | 推荐方案 | 状态 |
|---|---|---|---|
| F-001 | G2B-RL1-D1 | 10D 最大相对不利路径 `<=-5%`，t-1 feature / t+1..t+10 label | APPROVED_BY_USER_20260914 |
| F-002 | G2B-RL1-D2 | 九列、固定浅层 binary LightGBM、balanced class、风险两列必需 | APPROVED_BY_USER_20260914 |
| F-003 | G2B-RL1-D3 | 既有五折/504日/purge10，双 fresh-process，总 12 fits，无 market fit | APPROVED_BY_USER_20260914 |
| F-004 | G2B-RL1-D4 | 日内 percentile 风险层级；precision lift 0.10 + recall 0.25；研究/能力/forward 分层 | APPROVED_BY_USER_20260914 |
| F-005 | G2B-RL1-D5 | 一张风险表、两个 read API、现有页面同级视图、单日零 fit | APPROVED_BY_USER_20260914 |
| F-006 | G2B-RL1-D6 | 一个完整 Feature 任务、最多三轮审核、失败不自动开新方向 | APPROVED_BY_USER_20260914 |

## 5. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | proposed new HMM-owned risk target helper | planned `backend/tests/hmm_risk/test_risk_l1_prediction.py` | APPROVED_BY_USER_20260914_PENDING_IMPLEMENTATION | 无 |
| F-002 | reuse `rotation_l1_gbdt.py` primitives without changing rotation behavior | planned `backend/tests/hmm_risk/test_risk_l1_model.py` | APPROVED_BY_USER_20260914_PENDING_IMPLEMENTATION | 无 |
| F-003 | proposed risk executor/CLI | planned `backend/tests/hmm_risk/test_run_risk_l1_g2b.py` | APPROVED_BY_USER_20260914_PENDING_IMPLEMENTATION | 无 |
| F-004 | proposed risk metric/state helpers | artifact: `F:/Dev/AIstock_artifacts/hmm_phase2_gate2_p2_4_20260823_15e041f_postbug1153/p2_4_holdout_acceptance.json` only as non-authoritative baseline; planned `backend/tests/hmm_risk/test_risk_l1_model.py` | APPROVED_BY_USER_20260914_PENDING_IMPLEMENTATION | 无 |
| F-005 | proposed table/repository/router/current page extension | planned `backend/tests/hmm_risk/test_risk_l1_prediction.py`; planned `frontend/tests/hmm-risk/risk-l1.spec.ts` | APPROVED_BY_USER_20260914_PENDING_IMPLEMENTATION | 无 |
| F-006 | this document | validation-receipt: F2 validator and `git diff --check` | APPROVED_BY_USER_20260914_PENDING_IMPLEMENTATION | 无 |

## 6. Implementation Plan（实施方案）

实施只允许按 D6 的单一业务闭环进行：在 HMM-owned source 中复用 reader/feature/hash/repository 模式，新增一个 risk model/executor、一张表、两个只读 endpoint 和现有页面风险视图。不得拆出通用 trainer、scheduler 或第二 writer。源码范围与生产门禁在实际 changed files 确定后重新分类；本文不能预先把 backend runtime 影响降级为 none。

## 7. Verification Plan（验证方案）

验证必须同时覆盖 D6 所列 target/model/fold/metric/product 测试、双 fresh-process 12-fit 正式 development、DEV writer/readback、真实 API/UI 和无交易副作用。源码通过、正式 fit、DEV/生产数据库、runtime 与用户重启分别报告，不能互相代替。未运行的测试不得写成通过。

## 8. Risks / Failure Modes（风险与失败模式）

| 风险 | 防护与真实代价 |
|---|---|
| 旧 fading warning 被复用成答案 | D1/D2 直接预测未来风险事件；旧结果只作非权威基线 |
| 类别不平衡造成恒定预测 | balanced class + train class sufficiency；不足 typed 停止，不调阈值 |
| 风险两列缺失后模型仍给分 | 两列 mandatory；无补值或上一日 fallback |
| percentile tie 产生伪精度 | average rank 保留 tie；不按 sector code 强拆 |
| experimental surface 被宣传为 advisory | D4 三层状态与页面声明；只有未来 forward 通过才能升级 |
| 一次失败诱发模型搜索 | 12-fit 上限与单候选停止条件；失败返回用户 |
| 风险产品演变为通用告警平台 | 首版只含逐日 risk prediction；无 event lifecycle/queue/scheduler |

## 9. Rollout / Rollback（发布与回滚）

- 文档批准不启动实施；源码合入不自动执行 fit、DDL、DML 或 runtime activation。
- 正式 development 只在固定 merge commit 的独立 validation worktree 运行；失败不读取 tail。
- DEV 通过后再分别授权生产 DDL、model/prediction DML、runtime receipt 和用户重启。
- 回滚恢复上一份 HMM runtime receipt，并停止新 risk 写入；不删除历史 rotation/risk 行，不修改冻结模型或 candidate。

## 10. Production Gates（生产门禁）

| gate | 当前状态 |
|---|---|
| production_ddl_gate | pending future authorization；本文不执行 |
| production_dml_gate | pending future authorization；本文不执行 |
| production_backend_dependency_gate | noop；复用已安装 LightGBM 4.6.0 |
| production_frontend_dependency_gate | noop |
| runtime_activation_gate | pending future authorization |
| backend_restart_owner | user |

## 11. DESIGN-COMPLIANCE-001 设计审核

1. **禁止简化交付**：一个任务闭合 model→OOF→repository→API→真实 UI→单日推理；不把 fit、receipt、mock 或 fading 规则冒充风险能力。
2. **禁止静默错误**：缺失、标签、类别、score、tie、coverage、identity、writer/readback 均有 typed 失败；无补值、默认风险或上一日 fallback。
3. **禁止业务逻辑迁移**：风险 component 独立于 rotation，不修改 G2-A/G2-C、QE、Selection、Paper 或数据生产；输出保持 advisory-only。
4. **禁止未经确认门禁/审批**：D1～D6 已由用户逐项批准；没有把批准扩大为 DDL/DML、tail、runtime activation 或进程控制，也未新增运行时人工审批。

## 12. 当前停止位置

- 详细设计 D1～D6 已获用户批准并达到 implementation-ready；源码尚未修改，正式 risk fits=`0/12`；
- tail 未读取，model/product/READY 未生成；
- DDL/DML、依赖、runtime、进程均为 noop；
- 下一步按 D6 开始一个完整源码实施任务；文档合入、源码实现、正式 fit、数据库与 runtime 仍分别报告，不互相代替。
