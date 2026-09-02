# AIstock Advisory N2 Exit Learnability Audit F2 详细设计 v1.0

> 日期：2026-09-02
> 状态：`IMPLEMENTED_LOCAL_FULL_VERIFIED_PR_READY`
> tier：F2
> objective contract：`RISK_MANAGED_ADVISORY`
> study type：`LEARNABILITY_AUDIT`
> production gates：restart/DDL/DML/runtime activation 均为 noop

## 1. 背景与当前事实

1. N2 Entry/Exit formal bundle `5c5946a7adfb1e41c5287d5781f240fa290d690c63be0679edb5b00960556f2c` 已在 development window 完成并通过 immutable inspect；sealed holdout 未读，结果不可部署。
2. Exit oracle 对 1930 个 baseline episode 生成 17970 个 review decision label；1928 个 episode 可评价，1349 个 episode 存在正 hindsight intervention，覆盖 252/304 个动作日。perfect Exit ceiling 均值为 `386.60 bps/episode`，daily moving-block 95% 区间为 `[256.03,409.89]`。
3. 上述结果只证明动作空间高，不证明在 review clock 当时可见信息下能够预测“下一可交易开盘退出相对继续 baseline”的增量价值。直接把 oracle、liability 或 holding prediction 当成 Exit 模型会违反蓝图的 oracle/learnability 分离。
4. N1 已冻结 8 个时间 block、28 条 READY CPCV path、每个 observation 7 个 OOF prediction 的 cross-fitting 语义。本任务复用该 split，不搜索 fold、模型族、超参、阈值或特征子集。
5. 本任务是 N3 唯一路由前缺失的最后一个最小诊断。它不是 Exit candidate/confirmation，不与 QE alpha MVE 并行形成第二条模型主线。

## 2. Scope / 目标

交付一个单模型、固定信息集、严格 cross-fitted 的 Exit learnability audit：

1. 从 clean merged source 冻结 request，绑定 N1 request/bundle、policy dataset、N2 action request/bundle、Qlib daily/suspend、CPCV、policy hashes、repository commit 和 output root。
2. 用真实 baseline episode 与 N2 Exit incremental labels 构造每个 review decision 的 T-visible feature matrix；每个 episode 的所有 review rows 只能属于同一 train 或 validation side。
3. 固定训练一个 `SKLEARN_RIDGE_V1` trial：`alpha=100`、`solver=svd`、train-fold median imputer、standard scaler、categorical one-hot、28 paths、7 OOF predictions/row。
4. 固定 policy：每个 episode 按 review date 升序，在 OOF predicted exit advantage `>5 bps` 的第一个 review 执行 `EXIT_NEXT_OPEN`；否则完全继续 baseline。禁止调阈值、选择最佳 review 或事后回选。
5. 报告 OOF row/episode/daily 增量、coverage、干预支持、MDE、moving-block CI、MDD proxy、tail、两 regime 分布和 typed evidence state。
6. 发布 immutable bundle并向 trial registry 原子追加一条 model-trial record；route 继续保持 N2，直到聚合任务同时读取 N1/N2-A/N2-B/Entry/Exit/QE preparation 后选择唯一 N3 主线。

## 3. Non-goals

- 不训练第二个模型、分类器、LightGBM、Transformer、survival head 或 ensemble。
- 不改变 Ridge alpha/solver、feature roster、阈值、CPCV、成本、baseline policy 或 block bootstrap 参数。
- 不读取 sealed holdout，不生成 confirmation/activation/prospective evidence。
- 不 final-refit、不保存 deployable model、不接 API、Selection、Advisory runtime、Paper/QMT 或订单。
- 不输出 `REDUCE` 数值仓位，不改变固定五槽或动态资金授权边界。
- 不读取 liability/holding label 作为特征，不使用 baseline 最终退出日期/原因、未来 high/low/close、oracle-best review 或未来 action state。
- 不修改因子库、StrategyPackage、数据库或线上 descriptor。

## 4. Architecture

```text
N1 request/bundle + policy dataset/CPCV
                 +
N2 action request/bundle (Exit labels + exact baseline parity)
                 +
clean source + Qlib daily/suspend identity
                 |
                 v
       frozen Exit learnability request
                 |
       T-visible feature reconstruction
                 |
       28-path episode-isolated Ridge OOF
                 |
       first predicted advantage > 5 bps
                 |
       policy lift + support + MDE/CI
                 |
 immutable bundle + one trial registry record
                 |
 route remains N2; no model/runtime activation
```

新增范围：

- `backend/services/advisory_model_first/exit_learnability_contracts.py`
- `backend/services/advisory_model_first/exit_learnability_pipeline.py`
- `scripts/advisory_exit_learnability_audit.py`
- `backend/tests/advisory_model_first/test_exit_learnability_contracts.py`
- `backend/tests/advisory_model_first/test_exit_learnability_pipeline.py`
- `backend/tests/advisory_model_first/test_exit_learnability_delivery.py`
- 既有 CI classifier/file ownership 的精确 CLI 映射

不新建 scheduler、cache、DB schema、UI、审批或通用 ModelOps。

规模与复杂度边界：冻结输入最多 1930 个 Top5 episode、约 1.8 万个 review rows、28 个 CPCV paths；episode metadata join 必须 `many_to_one`，OOF 聚合必须以唯一 `label_id` 校验 7 倍 multiplicity。行情只读取这些 episode 涉及的 symbol 和冻结日期范围。特征重建为 `O(review_rows × max_lookback_20)`，cross-fit 为固定 28 次单模型拟合，不允许按结果扩展 trial；真实预检需回报行数、阶段耗时和 RSS。

## 5. Contracts / Frozen request and source identity

request 必须绑定并逐项 hash/size/semantic 回读：

- N1 frozen request和formal manifest；
- N1 policy dataset manifest、`candidate_episode_labels.parquet`、`cpcv_paths.json`、baseline/shadow/cost policy；
- N2 action frozen request、formal manifest、`exit_labels.parquet`、`exit_decisions.parquet`、`exit_episode_best.parquet`、source receipt；
- N2 oracle receipt id/hash，要求 `deployable=false`、`sealed_holdout_accessed=false`；
- Qlib daily root、suspend root、dataset identity、policy hashes、feature schema identity；
- repository root/clean commit、output root、RSS `8 GiB`、wall time telemetry only；
- `planned/generated/evaluated/selected trial count = 1/1/1/0` 只在成功 bundle 中成立；request 自身 planned=1，其余为0；
- `objective_contract=RISK_MANAGED_ADVISORY`、`study_type=LEARNABILITY_AUDIT`、sealed=false、deployable=false。

source drift、unknown field、dirty repo、sealed path、N1/N2 policy hash不一致、Exit baseline parity非`EXACT`、CPCV非28 READY或7 OOF、重复 key、额外 feature/model/threshold 一律 fail closed。

## 6. Decision clock and feature schema

### 6.1 Observation identity

唯一 row key：`(episode_id, review_decision_date)`。每行同时保留：

- `entry_decision_date`：原 Selection 决策日，也是 CPCV block 归属日；
- `entry_trade_date`：真实下一可交易开盘 entry；
- `review_decision_date`：T 日收盘后作 Exit 决策；
- `target_action_date/effective_action_date`：T+1 下一可交易开盘，仅用于 label/action receipt，不进入 feature；
- `instrument`、原 `selection_rank/selection_score`、policy hashes。

训练/验证切分只按 `entry_decision_date` 映射 N1 CPCV。一个 episode 的所有 review rows 必须全部落在同一 side；按 review date 分割属于 leakage，硬失败。

### 6.2 Frozen T-visible features

模型特征只包含下列 22 项，顺序进入 schema hash：

1. `selection_rank`
2. `selection_score`
3. `holding_trading_days_elapsed`
4. `holding_fraction_of_time_stop`
5. `unrealized_close_return_bps`
6. `relative_return_since_entry_bps`
7. `return_1d_bps`
8. `return_3d_bps`
9. `return_5d_bps`
10. `return_10d_bps`
11. `realized_vol_5d_bps`
12. `realized_vol_10d_bps`
13. `realized_vol_20d_bps`
14. `drawdown_from_peak_since_entry_bps`
15. `runup_from_entry_peak_bps`
16. `distance_to_stop_bps`
17. `distance_to_take_profit_bps`
18. `distance_to_trailing_stop_bps`
19. `intraday_range_bps`
20. `close_location_in_day`
21. `volume_ratio_5d_to_20d`
22. `market_regime` (`UP_OR_FLAT/DOWN`)

精确定义：

- 所有 trailing 窗口包含 review date且最多读到 T close；`return_Nd=close_T/close_{T-N}-1`；
- `holding_trading_days_elapsed` 为从 `entry_trade_date` 到 T（首尾均含）的可见交易日数量，`holding_fraction_of_time_stop=elapsed/20`；
- realized vol 为截至 T、按 `close_t/close_{t-1}-1` 计算的 N 个日收益样本标准差（`ddof=1`）；不足 N 个收益保留 typed missing，由 train-fold median imputer处理；
- entry-to-review path只从 `entry_trade_date` 到 T；peak 固定为该区间可见 `close` 最大值，runup/drawdown均基于该 peak close，不得读取 T+1；
- `distance_to_stop = unrealized_return_bps + 800`；
- `distance_to_take_profit = 1800 - runup_from_entry_peak_bps`；
- `distance_to_trailing_stop = drawdown_from_peak_since_entry_bps + 700`；
- benchmark relative return使用同一 entry trade date open 到 T close；
- volume ratio为截至T的5日均量/20日均量，safe divide保留missing；
- `intraday_range_bps=(high_T-low_T)/close_T*10000`；`close_location_in_day=(close_T-low_T)/(high_T-low_T)`，当日区间为零时保留 typed missing；
- regime严格复用 `CSI300_TRAILING20_CLOSE_RETURN_SIGN_AT_T_V1`；
- 特征构造后运行 future-poison不变性测试：修改 T+1及以后 OHLCV/benchmark 不得改变任何 T feature/hash。

`incremental_net_value_bps`、baseline最终收益/退出日/退出原因、oracle action、未来执行状态和任何 label字段不得出现在 feature roster。

## 7. Fixed model and cross-fitting

单一 model contract：

- estimator：`SKLEARN_RIDGE_V1`
- target：N2 `incremental_net_value_bps`
- alpha：`100.0`
- solver：`svd`
- fit_intercept：true
- numeric：train-fold median imputer + standard scaler
- categorical：train-fold most-frequent + dense one-hot，unknown ignore
- path：N1 exact 28 READY CPCV
- OOF aggregation：同 row 7 次 validation prediction算术平均
- random model seed：not applicable；bootstrap seed `20260902`

每条 path：

1. 用 entry decision date选择 train/validation episodes；
2. train和validation episode id交集必须为空；
3. preprocessing只fit train；
4. fit Ridge并对validation所有review rows预测；
5. 每row必须最终恰好7个prediction；不足或多出均失败。

该 audit计1个model trial，不把28个fold计为28个trial，也不做超参搜索。任一代码/data identity修复才允许 exact retry；结果不佳不得改阈值或feature后沿用同experiment identity。

## 8. Fixed Exit policy and OPE

对每个episode按`review_decision_date, target_action_date, label_id`稳定排序：

- 第一个 `predicted_exit_advantage_bps > 5.0` 且label status=`AVAILABLE` 的row执行`EXIT_NEXT_OPEN`；
- 找不到则`HOLD_TO_BASELINE`；
- 禁止在一个episode多次退出，禁止选择预测最大或真实最大review；
- realized policy lift直接取被选择row的冻结 `incremental_net_value_bps`；未干预为0；
- 两臂必须来自同一N2 shadow simulator/cost policy，禁止另算成交价或成本；
- daily portfolio lift按原`entry_decision_date`对五个episode等权平均；不形成动态资金权重。

报告：

- row prediction Pearson/Spearman与方向命中率，仅作诊断；
- intervention episode/day/regime/block支持；
- mean/median episode lift、positive/negative fraction、5% tail；
- daily mean、moving-block 95% CI、MDE、cumulative lift与MDD proxy；
- oracle capture ratio = learnable mean lift / frozen oracle mean lift，仅作诊断；
- baseline parity、coverage和unavailable reason counts。

## 9. Evidence rule

沿用N2 support contract：minimum intervention count=20、minimum intervention day fraction=0.25、required regimes=`UP_OR_FLAT/DOWN`、minimum 5 days/regime、block length=20、minimum effective blocks=2。

其中支持度的“day”固定为 Exit 动作时钟：分母是所有可评价 review action days，分子是至少发生一次 OOF 退出干预的 action days；regime 与 block 同样按 action day 计算。收益推断仍按原始 `entry_decision_date` 聚合五个等权 episode，二者不得混用。

经济阈值固定 `5 bps/五槽entry-day`。evidence state：

- `HIGH`：support充分、daily CI lower `>5`，且MDE `<=max(point lift,5)`；
- `LOW`：support充分、daily CI upper `<=5`，且MDE满足同一功效规则；
- 其他为`INCONCLUSIVE`。

`HIGH/LOW`可作为N3 `DIRECTION_GATE`，仍不能 activation；`INCONCLUSIVE`只能`NAVIGATION_ONLY`。oracle ceiling不与learnability lift相加，也不计入model trial统计。

## 10. Immutable bundle, registry and route

bundle文件固定：

- `request.json`
- `source_identity_receipt.json`
- `feature_schema.json`
- `features.parquet`
- `oof_predictions.parquet`
- `episode_policy.parquet`
- `daily_policy.parquet`
- `learnability_receipt.json`
- `resource_report.json`
- `registry_record.json`
- `manifest.json`

manifest绑定每个文件SHA256/size/row count、request/receipt identity、model trial count、objective/study/decision use、sealed=false、deployable=false。publish后完整readback成功才允许registry append；失败时不留下可误用的部分bundle。

registry record：

- experiment id：`ADVISORY-N2-EXIT-LEARNABILITY-V1`
- attempt id：request id
- stage：`N2_EXIT_LEARNABILITY_AUDIT`
- parent lineage：N1 Tier1 + N2 Exit oracle bundle
- planned/generated/evaluated/selected：`1/1/1/0`
- consumed window：P0C development decision start 至 outcome cutoff（label 的未来实现区间也计入消费）
- study：`LEARNABILITY_AUDIT`
- decision use/result class按§9预注册规则派生

delivery后生成current route，但必须仍为`N2_ENTRY_EXIT_QE_PREPARATION`。N3 route只能由后续独立聚合任务读取完整N1/N2 evidence后生成。

## 11. Error contract

- `ADVISORY_EXIT_LEARNABILITY_REQUEST_INVALID`
- `ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH`
- `ADVISORY_EXIT_LEARNABILITY_FEATURE_LEAKAGE`
- `ADVISORY_EXIT_LEARNABILITY_CROSSFIT_INVALID`
- `ADVISORY_EXIT_LEARNABILITY_POLICY_INVALID`
- `ADVISORY_EXIT_LEARNABILITY_COVERAGE_INSUFFICIENT`
- `ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID`
- `ADVISORY_EXIT_LEARNABILITY_RESOURCE_LIMIT_EXCEEDED`

正常停牌、涨跌停或缺失行情保留typed missing/unavailable；不得删除episode或 broad exception 后继续成功。schema、hash、PIT、parity、fold、OOF multiplicity、非有限prediction、重复row、额外模型/feature/threshold均fail closed。

## 12. Implementation plan

1. 实现 frozen request、feature/model/policy/evidence contracts和semantic identity。
2. 实现source验证与T-visible feature builder，含future-poison和episode split guard。
3. 实现固定28-path Ridge OOF、first-crossing policy、支持度/MDE/CI。
4. 实现immutable bundle、exact retry、registry/route delivery和薄CLI。
5. 完成多轮正确性/泄漏/统计/资源/交付审核与修复。
6. 合入后从clean main冻结正式request并在WSL运行；源码合入和实验结果分别报告。

## 13. Risks and controls

| 风险 | 控制 |
|---|---|
| 同episode review rows跨fold | split只按entry decision date；train/validation episode集合必须不交叉 |
| T+1行情泄漏 | feature builder所有slice截止T；future-poison逐row/hash测试 |
| oracle best review泄漏 | policy只取首个OOF prediction超过固定5bps，不读oracle-best字段 |
| 多次尝试Ridge/threshold | request只允许一个model/一个threshold；修改产生新lineage且本frontier消费 |
| 高相关review rows夸大样本 | episode分组cross-fit；推断以entry-day moving block为主，row相关仅诊断 |
| liability机械信号冒充Exit | feature roster不含liability/holding预测label；elapsed day只是T-visible状态 |
| 日线停牌/缺失被删 | typed missing/unavailable并报告coverage，不阻断合法episode整体 |
| oracle高被误报可部署 | manifest/receipt固定deployable=false、无final model/refit/runtime binding |
| 平台工程膨胀 | 单request/pipeline/CLI；复用N1/N2/Qlib/registry，不建新平台 |

## 14. Verification plan

- request/hash/unknown field/dirty repo/sealed/source drift fail closed；
- 22项feature exact roster和schema hash；
- future-poison、T cutoff、entry/review/action clocks；
- episode fold isolation、28 READY、7 OOF、train-only preprocessing；
- Ridge alpha/solver/threshold不可override；
- first-crossing而非max prediction/true label；每episode最多一次action；
- support/evidence HIGH/LOW/INCONCLUSIVE边界与欠功效降级；
- bundle mutation、partial publish、registry exact retry与route保持N2；
- direct tests、相关N1/N2 compatibility、完整`advisory_modeling_backend`、ruff/format/compile/diff、L0、F2 validator；
- formal WSL run完成后inspect、resource、registry tail和sealed readback。

## 15. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-828 | Exit oracle与learnability严格分离；本任务只运行一个固定cross-fitted model trial |
| F-829 | request精确绑定N1/N2/Qlib/CPCV/policy/source identity，sealed=false |
| F-830 | 22项T-visible feature roster与future-poison不变性，无label/future字段 |
| F-831 | episode按entry decision date隔离，28 READY paths、7 OOF/row |
| F-832 | Ridge alpha=100/solver=svd和train-fold preprocessing冻结，不搜索 |
| F-833 | 每episode首个predicted advantage>5bps动作；不选max、不多次退出 |
| F-834 | OPE复用同一shadow simulator/cost label，固定五槽等权，无动态仓位 |
| F-835 | 支持度、block CI、MDE与HIGH/LOW/INCONCLUSIVE证据规则冻结 |
| F-836 | immutable bundle、exact retry、1条registry record、route保持N2 |
| F-837 | 无final refit/model、API、DB、runtime、Selection、Paper/QMT、订单或restart |

## 16. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-828 | `backend/services/advisory_model_first/exit_learnability_contracts.py`; `backend/services/advisory_model_first/exit_learnability_pipeline.py` | `backend/tests/advisory_model_first/test_exit_learnability_pipeline.py`；真实 N2 只读预检 1 fixed trial | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-829 | `prepare_exit_learnability_request`; `_load_verified_sources` | `backend/tests/advisory_model_first/test_exit_learnability_contracts.py`; `backend/tests/advisory_model_first/test_exit_learnability_delivery.py` | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-830 | `build_exit_feature_matrix`; `_stock_features_at` | `backend/tests/advisory_model_first/test_exit_learnability_pipeline.py::test_feature_builder_is_invariant_to_post_review_market_poison`；真实预检 17970 rows/1928 episodes | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-831 | `run_exit_crossfit` | `backend/tests/advisory_model_first/test_exit_learnability_pipeline.py::test_crossfit_uses_all_28_paths_and_seven_predictions_per_row`；真实预检每行7 OOF | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-832 | `ExitLearnabilityModelSpecV1`; fixed sklearn pipeline | `backend/tests/advisory_model_first/test_exit_learnability_contracts.py`; `backend/tests/advisory_model_first/test_exit_learnability_pipeline.py` | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-833 | `evaluate_exit_policy` first crossing | `backend/tests/advisory_model_first/test_exit_learnability_pipeline.py::test_policy_uses_first_threshold_crossing_not_max_prediction` | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-834 | paired N2 incremental label evaluator | `backend/tests/advisory_model_first/test_exit_learnability_pipeline.py`；真实预检 384 complete five-slot days | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-835 | `_infer_daily_lift`; `ExitLearnabilitySupportV1`; receipt validator | `backend/tests/advisory_model_first/test_exit_learnability_contracts.py`；真实预检 support sufficient/result inconclusive | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-836 | `_publish_bundle/_read_bundle/_deliver_bundle`; CLI | `backend/tests/advisory_model_first/test_exit_learnability_delivery.py`; `backend/tests/scripts/test_ci_change_classifier.py` | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-837 | request/receipt literal false gates；无 runtime adapter | `backend/tests/advisory_model_first/test_exit_learnability_delivery.py`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_n2_exit_learnability_audit_f2_detailed_design_20260902.md --tier F2` | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |

## 17. DESIGN-COMPLIANCE-001

1. 不把oracle ceiling、单模型exploration、部分coverage或源码存在冒充完整Exit能力。
2. 无silent fallback；source、feature、fold、OOF、policy、bundle、registry和resource错误均typed fail closed。
3. 业务语义保持“下一可交易开盘退出 vs 继续冻结baseline policy”的增量价值，不改成本、成交价、Selection或资金语义。
4. 不新增人工审批、RBAC、后端重启、数据库、动态仓位或N3之外的发布门禁。

## 18. Production gates and rollback

- `production_ddl_gate=noop`
- `production_dml_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- backend restart：noop
- runtime/Selection/StrategyPackage/Factor：无变化
- rollback：回退离线pipeline源码；已发布且登记的不可变诊断bundle不得删除、改写或冒充activation。
