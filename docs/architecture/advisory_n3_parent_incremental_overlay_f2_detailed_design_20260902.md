# AIstock Advisory N3 父包增量 Overlay F2 详细设计 v1.0

> 日期：2026-09-02  
> 状态：`IMPLEMENTED_REVIEW_PASS_FORMAL_PENDING`  
> tier：`F2`  
> objective contract：`ALPHA_RANKING`  
> study type：`EXPLORATORY_SCREEN`  
> decision use：`NAVIGATION_ONLY`  
> production gates：restart/DDL/DML/runtime activation 均为 `noop`

## 1. Background / 当前事实与问题定义

N3 首批 QE 上游 Alpha MVE 已从 clean merge commit `dc3ace36...` 完成正式运行。immutable bundle `09137f0c...` 覆盖 386 个开发决策日、1,709,387 条 `CURRENT_IC_PARENT` H20 成本后 outcome，完成 `24/24/24/0`。24 个单信号的 family-wise Top5 相对父包 lift 下界均不大于 0，因此原 frontier 的 `selected=0` 不得修改、重选或放宽。

原实验同时留下一个不同且尚未检验的经济假设：5 个完整窗口信号和 1 个下行 regime 信号具有正的 family-wise RankIC 下界，且与父包同日分数 Spearman 绝对值均低于 0.23。它们不能独立替换父包，但可能以小权重补充父包信息。

本设计建立新 hypothesis lineage `ADVISORY-N3-PARENT-INCREMENTAL-OVERLAY-V1`，只检验“固定弱信号作为父包小权重增量项”这一假设。由于候选信号是从同一开发窗口的探索结果中选出，本轮结果无论正负都只可导航；不得作为 confirmation、activation、StrategyPackage 或运行时证据。

## 2. 目标

1. 只读消费已验证的 N3 正式 bundle，不重新访问数据库、Qlib、Tushare、网络、因子源或 sealed holdout。
2. 冻结 6 个候选信号与每项 4 个非零小权重，形成恰好 24 个新 trial。
3. 在同一 canonical PIT 成员内把父包与候选信号转换为同日 rank，再计算父包增量 overlay；缺失或非激活 regime 精确回退父包。
4. 逐 trial 输出配对 RankIC delta、Top5 H20 成本后 lift、Top5 干预支持度、churn、coverage、95% block CI、24-trial family-wise interval 和 DSR 诊断。
5. 按一次性 frontier 选择 0 或 1 个探索 candidate；不回到原 24-signal frontier，不调方向，不扩权重网格，不挑子窗口。
6. 发布 immutable bundle 和一条 append-only registry record，并把当前路线写为 confirmation design 或信息集扩展。

## 3. 非目标

- 不训练新 ranker、Entry、Exit、价格区间、仓位或交易执行模型。
- 不修改原 N3 `selected=0`、原 proposal summary、原 registry record 或原 route receipt。
- 不把分数系数解释为资金权重；不形成现金、仓位、下单或动态组合输入。
- 不写因子库、StrategyPackage、Selection、Program binding、模型 descriptor、数据库或生产运行时。
- 不进行历史证据固化、归档、旧 root 清理、通用组合平台、自动 alpha agent 或 H0 性能工程。
- 不读取 sealed holdout，也不把已消费开发窗口声称为新 OOS。

## 4. Architecture / 输入架构与数据流

```text
verified N3 immutable bundle (selected=0)
                    |
       exact manifest/source validation
                    |
  parent score + fixed 6 candidate score columns
                    |
 canonical same-date rank + 4 frozen weights
                    |
 24 overlay trials -> paired daily metrics/frontier
                    |
 immutable bundle + one registry row + route page
```

该架构只增加一个有界研究 evaluator 和 immutable delivery；不增加 scheduler、API、worker、数据库、缓存服务或通用组合平台。

## 5. Contracts / 输入与身份合同

唯一市场结果输入是 N3 正式 bundle 的 `score_panel.parquet`。请求必须同时绑定：

- bundle 目录名 `09137f0c...`、`manifest.json` SHA256/size；
- 父 request SHA256 `28ac7e99...`、父 receipt、父 registry record 与父 frontier SHA256；
- `score_panel.parquet` SHA256/size/row count/schema；
- dataset identity、policy identity、信号窗口 `2024-07-04..2026-02-02`；
- 当前 clean repository commit、registry path、route path 和 output root。

加载时复用原 N3 bundle 的完整 member/manifest/relational identity 校验。父 bundle 必须满足：

- `study_type=EXPLORATORY_SCREEN`、`decision_use=NAVIGATION_ONLY`；
- `selected_trial_count=0`、`selected_proposal_id=null`；
- `sealed_holdout_accessed=false`、`deployable=false`、`runtime_eligible=false`；
- 24 个原 proposal 已全部生成和评价；
- score panel 只包含冻结的父 `score`、outcome 和 24 个 proposal score。

任何成员 mutation、路径漂移、身份不一致、重复 PIT key、缺列或多余候选均 typed fail closed。

## 6. 固定候选与 24-trial roster

候选由原正式 proposal summary 的预定义导航规则固定：`familywise_rank_ic_lower > 0` 且 `abs(parent_score_spearman_mean) < 0.8`。exact roster 为：

1. `N3_PRICE_VOLUME_BEHAVIOR_02`
2. `N3_SECTOR_RELATIVE_04`
3. `N3_CROWDING_DISPERSION_01`
4. `N3_CROWDING_DISPERSION_03`
5. `N3_CROWDING_DISPERSION_04`
6. `N3_REGIME_CONDITIONED_02`

每个候选只允许 `0.05/0.10/0.15/0.20` 四档非零系数，总计 24 trial。trial id 由 `candidate_id + weight_bps` 确定；候选顺序、方向、权重和 trial 数均进入 request hash。禁止零权重计 trial、负权重、结果后反向、插值或新增档位。

## 7. Overlay 算法与回退语义

对每个 `decision_as_of_trade_date`：

1. 仅使用该日 score panel 中的 canonical PIT 行；不读取 label 计算 rank。
2. 父包有限 `score` 以 `rank(method="average", pct=True)` 转为 `parent_rank`。
3. 候选有限分数在同一日以相同方法转为 `candidate_rank`。
4. 候选当日有限值不足 2 个或横截面无变异时，该日被标记 `INACTIVE_OR_DEGENERATE`，全部行精确使用 `parent_rank`。
5. 候选活跃但单行缺失时，该行精确使用 `parent_rank`；不得以 0、均值、前值或未来值填充。
6. 其余行计算 `overlay_rank=(1-w)*parent_rank+w*candidate_rank`。
7. 父 score 非有限的行保持非有限；overlay 不得创造父包原本不可排名的股票。

父 rank 的 Top5 必须与原始父 score 的 Top5 在每个可评价日完全一致；否则以 `ADVISORY_N3_PARENT_OVERLAY_BASELINE_PARITY_FAILED` 停止。overlay 有限掩码必须与父 rank 完全一致，确保不因缺失删除股票、删除交易日或制造 coverage/cash 变化。

## 8. 评价与干预支持度

所有 label 只在 overlay score 冻结后按原 `(decision_date,instrument)` 行使用。每个 trial 每日记录：

- parent/overlay RankIC 与 paired RankIC delta；
- parent/overlay Top5 成本后净超额与 paired lift；
- overlay Top5 与 parent Top5 的替换数量、是否真实干预；
- overlay/parent Top5 churn；
- candidate finite fraction、active/fallback 行数与 fallback reason；
- overlay 与 parent rank Spearman。

确认一个 trial 具有最低实际干预支持，必须同时满足：

- 可评价日不少于 382；
- Top5 发生变化的交易日不少于 20；
- 干预日占全部可评价日不少于 5%；
- 干预至少分布在 2 个自然季度。

上述阈值在运行前进入 request，结果后不得修改。稀疏干预的均值与区间仍按交易日 moving block 计算，不把每日观测当独立样本。

## 9. 统计合同与一次性 frontier

主统计序列是每日 paired `rank_ic_delta` 和 `top5_lift_bps`，包括精确回退日的零增量；不得只挑干预日或下行日作为主结果。固定参数：20 交易日 moving block、2,000 次 bootstrap、seed `20260902`。

每个 trial 同时报告普通 95% CI 与 Bonferroni `alpha=0.05/24` family-wise interval。Top5 lift 另报告 24-trial-aware DSR 诊断，但 DSR 不替代配对经济门槛。

candidate eligibility 必须全部满足：

1. baseline Top5 exact parity 和 overlay coverage parity；
2. 可评价日与三项干预支持门槛；
3. family-wise RankIC delta 下界 `>0`；
4. family-wise Top5 成本后 lift 下界 `>0`；
5. 无 PIT、schema、identity、degenerate 或非有限统计失败。

若多个 trial eligible，只按以下冻结顺序选择一次：

`familywise_top5_lift_lower DESC, familywise_rank_ic_delta_lower DESC, weight ASC, trial_id ASC`。

未选 trial 与完整 frontier 一并冻结。若 selected=0，下一任务固定为 `N3_ALPHA_INFORMATION_SET_EXPANSION_MVE`；若 selected=1，下一任务固定为 `N3_PARENT_OVERLAY_CONFIRMATION_DESIGN`。confirmation 失败后不得回本 frontier 重选。

## 10. Artifact、registry 与 route

bundle 固定成员：

- `request.json`
- `overlay_roster.json`
- `overlay_score_panel.parquet`
- `daily_metrics.parquet`
- `overlay_summary.json`
- `frontier_receipt.json`
- `source_identity_receipt.json`
- `resource_report.json`
- `overlay_receipt.json`
- `registry_record.json`
- `manifest.json`

manifest 绑定每个成员的 SHA256、size 和 parquet row count。partial/mutated bundle 无 manifest 或 inspect 失败。exact retry 必须复用同一 bundle，registry duplicate no-op，route 内容 hash 不变。

registry 只追加一条：

- experiment：`ADVISORY-N3-PARENT-INCREMENTAL-OVERLAY-V1`
- stage：`N3_PARENT_INCREMENTAL_OVERLAY_EXPLORATORY_SCREEN`
- study：`EXPLORATORY_SCREEN`
- objective：`ALPHA_RANKING`
- decision use：`NAVIGATION_ONLY`
- trial count：`24/24/24/0|1`
- parent lineage 显式包含 `ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1`
- consumed window 仍为 `P0C_DEVELOPMENT_CONSUMED_20240704_20260202`

route 是单页状态，不是模型激活：selected=0 时进入信息集扩展；selected=1 时只进入独立 confirmation 设计。

## 11. 资源、错误与安全边界

- concurrency=1；RSS 上限 16 GiB；临时输出上限 16 GiB；wall time 仅遥测，不设停止门禁。
- 一次读取父 score panel，只 materialize 24 个 float32 overlay 列；禁止复制 24 份完整父表。
- 无 DB/network/Qlib/Tushare 访问；无后端进程控制；无 DDL/DML；无模型训练或 GPU 依赖。
- 失败保留未发布隐藏临时目录用于本次诊断，但不写 manifest、registry 或 route。

typed reason codes：

- `ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID`
- `ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH`
- `ADVISORY_N3_PARENT_OVERLAY_BASELINE_PARITY_FAILED`
- `ADVISORY_N3_PARENT_OVERLAY_PIT_LEAKAGE`
- `ADVISORY_N3_PARENT_OVERLAY_COVERAGE_FAILED`
- `ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID`
- `ADVISORY_N3_PARENT_OVERLAY_RESOURCE_LIMIT_EXCEEDED`

## 12. Implementation plan / 文件范围与实现顺序

允许新增/修改：

- `backend/services/advisory_model_first/parent_incremental_overlay_contracts.py`
- `backend/services/advisory_model_first/parent_incremental_overlay_pipeline.py`
- `scripts/advisory_parent_incremental_overlay_run.py`
- `backend/tests/advisory_model_first/test_parent_incremental_overlay_contracts.py`
- `backend/tests/advisory_model_first/test_parent_incremental_overlay_pipeline.py`
- `backend/tests/advisory_model_first/test_parent_incremental_overlay_delivery.py`
- `scripts/ci_change_classifier.py`
- `backend/tests/scripts/test_ci_change_classifier.py`
- 本详细设计及主蓝图的精确状态更新

实现顺序：合同与 roster → source/baseline parity → rank overlay → daily/statistical frontier → immutable delivery/CLI → 多轮审核。正式 24-trial 只能从合入后的 clean SHA 冻结 request 并运行；`prepare` 必须验证 `HEAD == origin/main` 且 worktree clean，本地/PR 阶段只使用 synthetic fixture 和只读 inspect。

## 13. Verification plan / 验证方案

- exact 6×4 roster、非零权重、request/receipt/hash/extra-field 拒绝；
- 父 bundle mutation、selected 非零、sealed/deployable 漂移和 schema 漂移 fail closed；
- 父 raw score 与 parent rank Top5 每日 exact parity；
- candidate 缺失、整日常数和非激活 regime 精确回退；停牌/正常缺失不删行、不删日；
- outcome poison 不改变 overlay score；候选未来行和非成员极值不改变较早 PIT score；
- paired RankIC delta、Top5 lift、干预日/季度、churn 和 block/family-wise interval 边界；
- one-shot 0/1 selection、candidate reselection 禁止、exact retry；
- bundle partial/mutation、registry append/no-op、route update/no-op、CLI typed JSON；
- targeted advisory modeling、CI classifier、ownership/L0、ruff/compile/diff、F2 validator 与 DESIGN-COMPLIANCE-001。

## 14. Risks and controls / 风险与控制

| risk | control |
|---|---|
| 同窗候选选择被误当独立证据 | 全部产物固定 `EXPLORATORY_SCREEN/NAVIGATION_ONLY`，独立 confirmation 使用新窗口 |
| 小权重 trial 退化为父包恒等 | 预注册干预日、比例和季度门槛，同时完整报告零干预 trial |
| regime 非激活值被当有效横截面 | 当日少于两个有限值或无横截面变异时整日精确回退父包 |
| 缺失处理改变股票池 | 逐行回退父 rank，overlay 有限掩码必须与父包完全相同 |
| outcome 进入 score 构造 | rank/overlay 在 label 使用前完成，future/outcome poison 单测验证 |
| 24 权重搜索产生选择偏差 | 固定预算、Bonferroni family-wise interval、DSR、一次选点且不重选 |
| 研究系数被误作资金仓位 | contract/manifest/receipt 均固定 runtime、factor、position 为 noop |
| 大表内存膨胀 | 单次读取、24 列 float32 wide panel、16 GiB RSS/temp typed gate |

## 15. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-848 | 新 lineage 只读绑定 N3 正式 selected=0 bundle 与已消费开发窗口，不修改原 frontier |
| F-849 | exact 6 候选×4 非零小权重形成 24 trial，方向和权重结果后不可修改 |
| F-850 | canonical same-date rank overlay，候选缺失/非激活 regime 精确回退父包 |
| F-851 | 父 raw/rank Top5 exact parity，overlay coverage 与父包 exact parity，不删股票或交易日 |
| F-852 | paired RankIC delta、Top5 成本后 lift、churn、coverage 与干预支持度完整报告 |
| F-853 | 20日 block、95%与24-trial family-wise interval、DSR 和全日零增量回退语义 |
| F-854 | 预注册最低干预日/比例/季度与双 family-wise 正下界的一次性 frontier |
| F-855 | immutable bundle、单条 registry、route、exact retry 与 mutation fail closed |
| F-856 | 无 sealed/DB/network/Qlib/因子库/StrategyPackage/Selection/runtime/仓位/restart/DDL 写入 |

## 16. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-848 | `parent_incremental_overlay_pipeline.prepare_parent_overlay_request`; `_validate_parent_navigation_source`; `HEAD==origin/main` gate | `backend/tests/advisory_model_first/test_parent_incremental_overlay_delivery.py`; real parent bundle readback `09137f0c...=VALID` | PASS | none |
| F-849 | `build_default_overlay_trials`; `FrozenParentIncrementalOverlayRequestV1` | `backend/tests/advisory_model_first/test_parent_incremental_overlay_contracts.py` | PASS | none |
| F-850 | `build_overlay_scores` | `backend/tests/advisory_model_first/test_parent_incremental_overlay_pipeline.py` | PASS | none |
| F-851 | `_validate_daily_parent_top5_parity`; `build_overlay_scores` | `backend/tests/advisory_model_first/test_parent_incremental_overlay_pipeline.py` | PASS | none |
| F-852 | `evaluate_overlay_trials`; `_evaluate_one_overlay_daily` | `backend/tests/advisory_model_first/test_parent_incremental_overlay_pipeline.py` | PASS | none |
| F-853 | frozen N3 block/DSR statistical helpers consumed by `_summarize_overlay_trial` | `backend/tests/advisory_model_first/test_parent_incremental_overlay_pipeline.py` | PASS | none |
| F-854 | one-shot frontier in `evaluate_overlay_trials`; frozen request support thresholds | `backend/tests/advisory_model_first/test_parent_incremental_overlay_pipeline.py` | PASS | none |
| F-855 | `_publish_bundle`; `_read_overlay_bundle`; `_deliver_bundle`; CLI | `backend/tests/advisory_model_first/test_parent_incremental_overlay_delivery.py` | PASS | none |
| F-856 | literal request/receipt/manifest gates | `backend/tests/advisory_model_first/test_parent_incremental_overlay_contracts.py`; `backend/tests/advisory_model_first/test_parent_incremental_overlay_pipeline.py`; `backend/tests/advisory_model_first/test_parent_incremental_overlay_delivery.py`; `backend/tests/scripts/test_ci_change_classifier.py` | PASS | none |

## 17. DESIGN-COMPLIANCE-001

1. 不把同窗 overlay 探索、正 RankIC 或 point lift 冒充独立确认、可交易 alpha 或荐股功能完成。
2. 不以 0/均值/前值/未来值填充候选，不反向符号，不删停牌/缺失股票或失败交易日；所有错误 typed fail closed。
3. 不改变父 H20 outcome、成本、PIT universe、候选流、Selection、Entry/Exit、价格范围或仓位业务语义。
4. 不新增 restart、DDL、审批、数据库、因子库、模型平台或运行时 activation 门禁。

## 18. Rollout、rollback 与生产门禁

- 代码合入与正式实验是分离状态。正式 request 只能由 clean merged SHA 创建。
- `production_ddl_gate=noop`
- `backend_restart=noop`
- `runtime_activation=noop`
- `factor_catalog_write=noop`
- rollback 只通过普通 PR revert 源码；append-only 正式 artifact/registry 不覆盖、不删除，也不冒充生产状态。
