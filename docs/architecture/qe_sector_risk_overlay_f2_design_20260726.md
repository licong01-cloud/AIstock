# QE Sector-Risk Overlay F2 设计

> 文档状态：`implementation_merged_runtime_active_artifact_built_bug881_retry_selection_fix_in_review`
> Feature tier：`F2`
> 父蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v5.12
> 范围：仅限 QE 研究；不接入 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage 或生产交易。

## 1. Background / 背景

R11A 证明固定延长 `hold_thresh` 不能稳定改善收益或回撤；R12P 又证明跨标签、跨任务四腿 equal 组合在当前 OOS trial 中明显高于 LGBM baseline。下一研究问题不是继续调固定持有天数或重复 prediction average，而是让板块趋势恶化状态显式参与新买入、持仓降险、退出与重入。

现有 HMM 风险 gate 只保护持仓并过滤部分新买入，现有 `QEEventRiskPolicy` 主要支持 symbol 级阻买和全量强制退出。二者都不能表达板块风险强度、目标暴露乘数、有界减仓、持有期覆盖和风险消退后的可解释重入，因此不能冒充本功能。

本设计在现有 QE 数据快照、`ScoreWeightedTopkStrategyV2`、配置组合器、multi-alpha pred-backtest、F-014 评价和 Archive/Prediction Store 上增量实现，不建设第二套回测平台。

## 2. Scope / 范围

### 2.1 目标

1. 从显式 QE 数据快照生成不可变、可复核的日度板块风险制品。
2. 在相同 prediction、股票池、OOS、费用和执行配置下支持四个实验臂：
   - `none`：无 overlay 控制；
   - `entry_gate`：只限制风险板块的新买入；
   - `bounded_de_risk`：限制新买入并按状态降低现有持仓目标暴露；
   - `exit_reentry`：高风险有界降险、临界风险退出、风险消退后按确认期允许重入。
3. 保持 suspend、涨跌停、可交易性、手数、持仓和成本语义与现有 QE 策略一致。
4. 输出风险状态、策略动作和后续评价可消费的确定性证据，不从日志文本猜测动作。
5. 复用 R8–R12 已归档 prediction，不重新训练主模型。

### 2.2 Non-Goals / 非目标与边界

- 不修改或读取非 QE 运行状态、缓存、表或服务。
- 不把 overlay 结果自动启用到荐股、模拟盘、Paper 或实盘。
- 不使用未来回撤、未来收益或回测结果生成运行时风险状态。
- 不把 HMM artifact 改名后当作 sector-risk artifact。
- 不在本切片训练 GAT/HIST/HMM/TRA 或新的个股预测模型。
- 不引入审批、准入、淘汰或研究停止开关。
- 不允许数据缺失时使用全零风险、最近一天状态、当前行业快照或其他静默回退。

## 3. Design Acceptance Index / 设计验收索引

| ID | 验收项 |
|---|---|
| F-023 | 风险制品只读取显式 QE 快照，冻结文件 hash、数据日期、公式版本和 `signal_date → effective_trade_date` 映射；运行制品不含未来 outcome。 |
| F-024 | 风险分数完整实现相对强弱拐点、价格宽度恶化、资金流背离、领导集中和波动/拥挤五个组件；缺字段、重复键、非 PIT 行业码和低覆盖必须显式失败；局部源数据或滚动分量不完整时保留原始缺失并写入 `INCOMPLETE`，不得扩大为全制品失败。 |
| F-025 | 同一策略 adapter 完整支持 `none/entry_gate/bounded_de_risk/exit_reentry` 四臂，状态到目标暴露映射和重入确认期可冻结、可归档。 |
| F-026 | overlay 强制动作与常规 TopK 动作分离；仅配置允许时覆盖 `hold_thresh`，部分卖出按当前权威持仓、价格、factor 和交易单位计算。 |
| F-027 | 配置组合器只对受支持的 ScoreWeighted V2 策略接线，复制并校验 manifest/parquet/helper；未知模式、文件 hash 不符或 class 不匹配时拒绝执行。 |
| F-028 | multi-alpha pred-backtest 可复用专用 runtime template 和同一风险制品；显式 `prediction_task_selection` 可冻结 baseline/LOO child 选择，四臂仅改变 overlay policy，不改变 prediction、OOS、费用或执行算法。 |
| F-029 | 结构化记录逐日/逐 symbol 的风险状态、entry block、目标乘数、减仓/退出/重入动作及原因，供 F-014 和实验分析读取。 |
| F-030 | 评价层报告 1/3/5/10 日预警提前量、避免回撤、false early-exit、post-exit MFE、重入延迟、趋势捕获、换手和成本；缺失指标局部标记，不取消其他结果。 |
| F-031 | 运行保持 QE-only、CPU pred-backtest、远端最多 4 并行；不调用 `nvidia-smi`、NVML 或 GPU 资源轮询，不影响后端重启接管。 |

## 4. Architecture / 架构

```text
QE snapshot (daily_pv.h5 + sector_data.h5)
        |
        v
QESectorRiskArtifactBuilder
  - PIT l2_code_id
  - five causal components
  - cross-sectional ranks + state/hysteresis
  - signal_date shifted to effective_trade_date
        |
        +--> manifest.json (identity/hash/formula/coverage)
        +--> sector_risk_overlay.parquet (stock-date runtime rows)
        |
        v
ConfigComposer / multi-alpha runtime template
        |
        v
QESectorRiskOverlayScoreWeightedTopkStrategyV2
  - entry filter
  - target exposure multiplier
  - partial/full sell orders
  - re-entry confirmation
  - structured action ledger
        |
        v
Qlib report/positions/trades + overlay action artifact
        |
        v
F-014 / overlay evaluator
```

### 4.1 复用与新增边界

- 复用 `scripts/score_weighted_strategy_v2.py` 的 TopK、动态 `n_drop`、价格、factor、手数和现金语义。
- 在基类增加默认 no-op hook；默认策略行为必须逐订单保持不变。
- 新增专用 helper/wrapper，不把 overlay 语义塞入 HMM gate 或事件风险策略。
- 复用 `BacktestBaseDataMemoryCache` 的显式文件根、日期切片和文件摘要，只加载 `daily_pv.h5` 与 `sector_data.h5`。
- 复用现有 config composer helper-copy 和 custom params 过滤机制。
- multi-alpha 继续使用既有 durable child/attempt/event、Prediction Store 和 pred-backtest，不新增表。

## 5. Contracts / 契约

### 5.1 风险制品请求

```text
QESectorRiskBuildRequest
  factor_data_dir               explicit path
  dataset_identity              non-empty immutable identity
  start_date/end_date           inclusive OOS-compatible dates
  formula_version               qe_sector_risk_v1
  source_files                  daily_pv.h5, sector_data.h5 only
  effective_shift_trading_days  1
```

`factor_data_dir` 必须存在且两个源文件均存在；manifest 记录两个文件的 SHA-256、字节数、mtime、行列数和日期范围。路径不能逃逸显式根目录。

### 5.2 风险组件

所有 rolling/rank 只使用 `signal_date` 当日及以前数据，状态在下一个交易日生效：

1. `rs_turn_risk`：申万 L2 20 日相对收益截面 rank 的 5 日下降幅度。
2. `breadth_deterioration`：板块成员站上个股 MA20 的比例相对 5 日前下降幅度。
3. `flow_divergence_risk`：`sw2_mf_net_amt / max(abs(sw2_amount), eps)` 的 5 日均值走弱，并与仍为正的板块 20 日收益形成背离。
4. `leadership_concentration`：板块内个股 20 日收益 top20% 均值与中位数的差，配合 breadth 下降描述少数龙头集中。
5. `vol_crowding_risk`：板块日收益 10 日波动/60 日波动与 5 日/20 日成交额比的组合。

各组件先做当日板块截面 percentile rank，再等权合成 `risk_score`。组件原值和 rank 均写入 artifact，禁止只保存最终分数。

### 5.3 风险状态

```text
NORMAL    risk_score < 0.60
CAUTION   0.60 <= risk_score < 0.80
HIGH      0.80 <= risk_score < 0.90
CRITICAL  risk_score >= 0.90
INCOMPLETE one or more component values are unavailable
```

阈值是首个 trial 的冻结参数，不是研究准入条件。artifact 同时记录阈值；后续敏感性实验另立 artifact identity。
`INCOMPLETE` 不是风险等级，也不是研究准入或淘汰状态。它保留缺失分量及其 manifest 统计，四臂均以目标暴露 `1.00`、允许新买入的原策略语义处理；不得填零、前向填充或继承最近风险状态。

### 5.4 Runtime parquet

唯一键：`effective_trade_date + instrument`。

必需列：

```text
signal_date, effective_trade_date, instrument, l2_code_id,
risk_score, risk_state,
rs_turn_risk, breadth_deterioration, flow_divergence_risk,
leadership_concentration, vol_crowding_risk
```

未知 `l2_code_id=-1` 明确写入 `UNMAPPED`，策略不得把它当作 `NORMAL`。已映射但任一五分量缺失时写入 `INCOMPLETE`，并在 manifest 固化股票日数、板块日数和分量缺失计数。首轮实验对 `UNMAPPED/INCOMPLETE` 均保持原策略行为并单独统计覆盖，不借此淘汰样本或研究方向。

### 5.5 策略参数

```text
sector_risk_overlay_enabled: bool
sector_risk_overlay_mode: none | entry_gate | bounded_de_risk | exit_reentry
sector_risk_overlay_manifest_file: str
sector_risk_overlay_data_file: str
sector_risk_overlay_strict: true
sector_risk_overlay_override_hold_thresh: bool
sector_risk_overlay_reentry_confirm_days: int
sector_risk_overlay_state_multipliers: mapping
sector_risk_overlay_action_log: str
```

冻结首轮映射：

| mode | NORMAL | CAUTION | HIGH | CRITICAL | UNMAPPED / INCOMPLETE | 新买入 |
|---|---:|---:|---:|---:|---:|---|
| none | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 不过滤 |
| entry_gate | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | HIGH/CRITICAL 阻止 |
| bounded_de_risk | 1.00 | 0.75 | 0.50 | 0.25 | 1.00 | HIGH/CRITICAL 阻止 |
| exit_reentry | 1.00 | 0.75 | 0.50 | 0.00 | 1.00 | HIGH/CRITICAL 阻止；降至 NORMAL/CAUTION 连续 3 日后允许重入；缺失证据不阻止原策略动作 |

### 5.6 Action ledger

每个交易日原子写 JSONL；重复 `(trade_date, instrument, action_type, policy_hash)` 拒绝。事件至少包含风险状态、base/target weight、持仓量、卖出量、是否覆盖 hold threshold、订单是否生成和明确原因。策略不把日志作为自身状态真相；状态来自 artifact 与当前持仓。

## 6. Implementation Plan / 实施方案

1. 新增纯计算 `sector_risk_overlay.py`：schema、公式、builder、manifest/hash 和 action/evaluation contracts。
2. 新增 CLI `scripts/build_qe_sector_risk_overlay.py`，只从显式 QE snapshot 生成候选制品。
3. 在 `ScoreWeightedTopkStrategyV2` 增加默认 no-op target-weight/rebalance hook，并用现有测试证明默认订单不变。
4. 新增 `qe_sector_risk_overlay_strategy.py` wrapper，实现 entry filter、目标暴露、部分/全部退出、重入与 action ledger。
5. 扩展 config composer 的支持矩阵、参数白名单、helper copy 与 artifact hash 校验。
6. 扩展 multi-alpha runtime-template identity，使 overlay manifest/data/helper hash 进入 child input/artifact evidence；不新增 DB schema。
7. 新增 evaluator，将风险事件与 Qlib report/position/trade、F-014 episode 对齐，输出指标族状态和数值。
8. 生成 R13A 实验卡：按 `none/entry_gate/bounded_de_risk/exit_reentry` 创建 4 个 policy run；每个 run 冻结 `include_baseline=true/include_loo=false`，只生成 R12P equal 与 LGBM baseline 两个 child，共 8 个 CPU pred-backtest child，远端最多 4 并行。

## 7. Verification Plan / 验证方案

- builder：未来数据截断、T+1 生效、跨节点确定性、五组件公式、重复键、字段/覆盖/hash 失败。
- strategy：四臂订单 oracle；默认 no-op parity；entry block；25%/50%/75% 暴露；critical 全退；hold override；停牌/涨跌停；手数/factor；重入确认。
- composer：受支持 class 正向接线；未知 class/mode、缺文件、hash 漂移、非法参数均拒绝。
- multi-alpha：相同 prediction identity、仅 policy hash 不同；显式关闭 LOO 后每个 policy run 必须恰好产生 baseline/equal 两个 child，旧请求未提供选择契约时仍保持自动 LOO；child/attempt/archive/restart 不被改变。
- evaluator：1/3/5/10 日 lead、false early-exit、post-exit MFE、re-entry delay、capture/cost 的确定性 fixture。
- 最小本地门：Ruff、py_compile、定向 pytest、`git diff --check` 和 F2 validator。
- 广泛 QE API/UI/远端 E2E 交由 CI/Validation Center；真实 R13A 只在代码合入、用户重启后启动。

## 8. Risks / Failure Modes / 风险与失败模式

1. **未来函数**：任何未 shift 的风险行立即失败；manifest 固化 shift 和日历 hash。
2. **重复行业行**：`sector_data.h5` 在个股层重复板块指数值；builder 必须按 `date+l2_code_id` 验证一致后去重，不得任取一行掩盖冲突。
3. **部分卖出重复执行**：目标是绝对目标权重，不按昨日持仓比例反复乘法，避免指数式减仓。
4. **hold threshold 冲突**：overlay 只在显式配置下覆盖，action ledger 记录每次覆盖。
5. **不可交易退出**：不伪造成交；保留目标动作和实际订单/成交差异，后续由 F-014 解释损失。
6. **制品漂移**：manifest/parquet/helper 任一 hash 不符即失败，不在节点现场重建或回退 HMM。
7. **跨模块影响**：所有模块、路径、任务和制品前缀均为 QE；禁止导入非 QE runtime service。
8. **资源波动**：本实验只做 CPU pred-backtest，远端最多 4 并行；不启用 GPU 监控。

## 9. Rollout / Rollback / 发布与回滚

- 源代码合入、后端重启、风险制品生成和 R13A 启动分别报告。
- PR #2754、BUG-871 已合入，用户已完成后端重启，正式不可变风险制品已经生成。BUG-872 补齐精确 child 选择前不提交会自动膨胀 LOO 的错误 R13A 请求。
- BUG-872 激活后先提交一个 `include_baseline=true/include_loo=false` 的两-child wiring canary，再按四个 policy run 提交 8 个正式 child；canary 只验证 wiring，不形成 Alpha 结论。
- 回滚停止创建新 overlay child，保留已生成 artifact、action ledger、run/child/attempt 和结果；代码按 PR revert，不删除研究记录。
- 默认策略 hook 为 no-op，关闭 overlay 后必须恢复原有订单语义。

## 10. Production Gates / 生产门禁

```text
production_ddl_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
runtime_restart = active_from_pr_2754
qe_artifact_build = formal_built_oos_20240701_20260629_v1
r13a_experiment = retry_canary_cancelled_bug881_selection_fix_in_review
non_qe_impact = prohibited
```

## 11. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-014 | `sector_risk_overlay_evaluation.py` 复用 F-014 holding episode 的 exit/MFE/MAE/capture 字段并输出局部证据状态 | `backend/tests/quantevolver/test_sector_risk_overlay_evaluation.py` 的 episode/action alignment tests | implemented_verified | none |
| F-023 | `sector_risk_overlay.py`、`build_qe_sector_risk_overlay.py` 的 immutable identity/hash/T+1 实现 | `backend/tests/quantevolver/test_sector_risk_overlay.py` 的 identity/hash/T+1/truncation tests | implemented_verified | none |
| F-024 | `sector_risk_overlay.py` 的 five-component、schema、PIT coverage，以及 incomplete-component 显式保留和中性策略语义 | `backend/tests/quantevolver/test_sector_risk_overlay.py`、`backend/tests/quantevolver/test_qe_sector_risk_overlay_runtime.py` 与真实 2026-06-30 snapshot candidate build | implemented_verified | none |
| F-025 | `qe_sector_risk_overlay.py`、`qe_sector_risk_overlay_strategy.py` 的 four-arm policy 与 order 实现 | `backend/tests/quantevolver/test_qe_sector_risk_overlay_runtime.py` 与 `backend/tests/unified_engine/test_qe_sector_risk_overlay_strategy.py` | implemented_verified | none |
| F-026 | `ScoreWeightedTopkStrategyV2` no-op hooks 与 overlay partial/full rebalance | `backend/tests/unified_engine/test_score_weighted_capacity_registration.py` 与 `backend/tests/unified_engine/test_qe_sector_risk_overlay_strategy.py` | implemented_verified | none |
| F-027 | `ConfigComposer._prepare_sector_risk_overlay_runtime`、helper packaging 与 wrapper routing | `backend/tests/quantevolver/test_sector_risk_overlay_config.py` | implemented_verified | none |
| F-028 | 复用 unified runtime bundle；`prediction_task_selection` 精确冻结 baseline/LOO child；四臂仅通过各 policy run 的 pred-backtest `strategy_kwargs` 改 policy | `backend/tests/multi_alpha/test_sector_risk_overlay_pred_backtest.py`、`backend/tests/multi_alpha/test_durable_plan.py`、`backend/tests/test_multi_alpha_combine_backtest.py` | implemented_verified | none |
| F-029 | JSONL action ledger + `qe_sector_risk_overlay_artifacts.py` Recorder 三制品持久化 | `backend/tests/quantevolver/test_sector_risk_overlay_artifacts.py` 与 `backend/tests/unified_engine/test_qe_sector_risk_overlay_strategy.py` | implemented_verified | none |
| F-030 | `evaluate_sector_risk_overlay` 的 lead/avoided drawdown/false exit/post-exit MFE/reentry/capture/cost 指标族 | `backend/tests/quantevolver/test_sector_risk_overlay_evaluation.py` | implemented_verified | none |
| F-031 | QE-only service/scripts、CPU pred-backtest、无 GPU telemetry | `tests/aistock_validation/test_qe_sector_risk_overlay_isolation.py` 与 `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_sector_risk_overlay_f2_design_20260726.md --tier F2` | implemented_verified | none |
| F-032 | 超过 small-file 契约的 portable runtime Parquet 经 CAS 上传、冻结 binding，并在 qrun 前校验 size/SHA256 后链接；legacy/durable 语义一致 | `backend/tests/test_multi_alpha_remote_dispatch.py`、`backend/tests/multi_alpha/test_durable_execution_adapter.py` 的 oversized runtime CAS、无静默排除、binding evidence tests | implemented_verified | none |

本地验收记录（2026-07-26）：新增与受影响测试合计 `207 passed, 38 skipped`；builder/evaluator branch coverage 合计 `88%`，runtime/Recorder helper branch coverage 合计 `95%`；动态加载的策略 wrapper 以 entry、four-arm mapping、partial/full exit、hold override、factor/lot 和默认 no-op business oracle 补证。定向 lint、`py_compile`、`git diff --check` 通过。未生成正式风险制品、未启动 R13A、未启停服务、未修改数据库。

BUG-871 真实数据补证（2026-07-26）：当前固定 snapshot 映射覆盖率 `99.9922%`，正式 OOS 制品包含 `2,198,910` 个股票日和 `63,273` 个板块日；其中 `138,393` 个股票日（`6.2937%`）、`4,495` 个板块日为 `INCOMPLETE`。缺失分量计数为 `rs_turn_risk=1,085`、`flow_divergence_risk=1,455`、`vol_crowding_risk=4,103`，宽度和领导集中分量无缺失。制品保留这些证据并完成 hash 固化，没有填补或伪造源数据；R13A 尚未启动。

BUG-872 child 规划补证（2026-07-26）：现有 scheme 结果身份为 `(run_id, weighting_scheme)`，因此单 run 内复制四个同名 `equal` child 会破坏持久化与 Archive 语义。R13A 改为四个 policy run，每个 run 使用同一 roster、`weighting_schemes=[equal]`、`include_baseline=true`、`include_loo=false`，由持久化 request snapshot、request hash 和 child input manifest 共同冻结选择。该结构无需 DDL，仍得到严格的 `4 × 2 = 8` 个 child；不是裁剪实验臂，也不改变任何 Alpha 结论。

BUG-878 远端运行时制品传输补证（2026-07-27）：BUG-872 两-child wiring canary 已准确规划 baseline 与 equal scheme，随后两个 child 均在 qrun 前因 `qe_sector_risk_overlay.parquet.b64=25,257,152` 超过 `qe_file_sync` 10MB 小文件契约而失败；失败 run/child/attempt 保留，未形成 Alpha 结论，正式 R13A 尚未提交。修复不提高小文件上限、不压缩或裁剪制品，也不手工复制远端文件：所有 Base64 后会超限的 portable runtime Parquet 统一经 `WorkspaceArtifactSyncClient` 内容寻址 CAS 上传，按 filename/SHA256/size/CAS root 冻结远端 binding；qrun 前校验 CAS 文件大小和 SHA256 后建立 workspace 链接。小文件继续走原 `qe_file_sync`，节点绑定的 QE 数据链接继续排除，legacy 与 durable 两条远端执行路径采用同一语义。代码合入和用户重启后只重试两-child wiring canary；canary 成功后再提交四个 policy run 的 8 个正式 child。

BUG-881 retry 选择身份补证（2026-07-27）：BUG-878 合入并重启后，原 canary 的 exact-snapshot retry 正确记录源 run lineage，但 `DurableCombineSubmissionService._replace_run_async` 在强制异步时遗漏 `prediction_task_selection`，导致后继 run 从目标 2 个 child 膨胀为 baseline、equal scheme 和 4 个 LOO，共 6 个 child。该 retry run 已提交 durable cancel 并保留全部 append-only 证据，未用于 Alpha 判断，正式 R13A 仍未提交。修复不再手工逐字段重建 `CombineBacktestRequest`：通用 request replace 与 run-async override 均使用 dataclass `replace`，新增字段自动守恒；回归覆盖 exact snapshot 恢复、异步覆盖、持久化 `_combine_request_v1` 和 deterministic child planning，明确断言后继仅包含 `baseline:leg_a` 与 `scheme:equal` 两个 child。合入和用户重启后再次从原失败 canary 执行 exact-snapshot retry。

BUG-883 运行资产协议加固（2026-07-27）：BUG-878 的真实 Parquet CAS
路径保持不变，但不再依靠“顶层文件 + parquet 后缀”推断运行资产。每次远端
提交先生成并持久化 `multi_alpha_remote_runtime_file_manifest_v1`，逐文件冻结
安全相对路径、SHA256、size、text/binary 类型和 `small_text / small_binary /
cas / empty_file` 传输语义。顶层契约内小文件继续使用 `qe_file_sync`；嵌套
运行模块及超过 Base64 10MB 契约的任意非空运行资产使用内容寻址 CAS，qrun
前逐项校验大小与 SHA256、建立必要父目录并链接；空文件按 manifest 显式创建。
manifest 与本地字节不一致、CAS binding 缺失或路径不安全均显式失败，不静默
遗漏。`__pycache__`、`.pyc`、`.pyo` 不进入传输清单或 runtime-template
identity；真实 Python 源文件变化仍会改变身份。该修复只作用于 QE multi-alpha
远端运行资产，不改变 overlay 计算、策略参数、研究方向或其他模块运行时。

BUG-884 小文本字节保真补证（2026-07-27）：BUG-881/883 合入并重启后的 exact-snapshot retry `macb_453ca2d0c5b21b40_20240701_20260629_20260727T054318127030Z_55fbc49e` 已准确规划 `baseline:lgbm_g14_fp_h60` 与 `scheme:equal` 两个 child，证明 retry 字段守恒生效；嵌套模块、因子文件和 `qe_sector_risk_overlay.parquet` 也已通过 CAS 绑定、远端 size/SHA 校验，证明运行资产枚举与 CAS 路径生效。但 `small_text` 打包使用 `Path.read_text()`，其 universal-newline 行为把 Windows workspace 中的 CRLF 原始字节转换成 LF，再交给 `qe_file_sync`；manifest 仍冻结转换前的原始 SHA256/size，导致两个 child 均在 qrun 前的严格字节校验处终止。该 run 保留为基础架构失败证据，不作 Alpha 判断。修复必须以 `read_bytes().decode("utf-8")` 保留 JSON 文本通道中的 CRLF 字符，使远端重新编码后的字节与 manifest 完全相同；不关闭校验、不跳过文件、不改变 CAS 分层。所有远端 missing/size/SHA256 不一致必须输出 `QE_RUNTIME_FILE_VERIFY_FAILED` 及 path、expected、observed 后显式终止。合入和用户重启后只重试同一原始 2-child canary，成功后再提交正式 R13A。

## 12. DESIGN-COMPLIANCE-001

- [x] 设计覆盖运行制品、策略、配置、组合回测、评价和隔离，不交付缺臂实现。
- [x] 所有缺失、漂移、冲突和不可交易状态显式表达，不静默回退。
- [x] 运行时不使用未来 outcome；评价 outcome 与运行 artifact 分离。
- [x] 不新增科研准入、淘汰、审批或停止机制。
- [x] QE-only 是唯一硬边界，非 QE 模块不读、不写、不调用。
- [x] DDL、依赖、合入、重启、制品生成和实验启动分别报告。
