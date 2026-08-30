# AIstock Advisory N1 Tier-1 Oracle 与 Learnability Audit F2 详细设计 v1.0

> 日期：2026-08-31  
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`  
> 交付等级：F2  
> 当前状态：`IMPLEMENTED_LOCAL_FORMAL_RUN_PENDING`  
> 业务模块：Advisory / Selection Center  
> 目标合同：`ALPHA_RANKING`  
> 生产影响：无 API/UI、无数据库 DDL/DML、无 descriptor、无 Selection 写入、无运行时激活、无需后端重启

## 1. Background / 当前事实

N0 已从合入源码生成正式控制面收据，且不是预合入候选证据：

| 项目 | 正式事实 |
|---|---|
| N0 receipt | `F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830/n0_completion_receipt.json` |
| N0 status / next | `COMPLETE` / `N1_TIER1_ORACLE_LEARNABILITY` |
| N0 receipt SHA256 | `9b460c294472daddd02375f0e68752dd741ab43617b9d0af1b6213c0b3ee9a9e` |
| registry SHA256 | `d33661d3743f800aa74093e95a6e970ebe8843f325372a3f35adf44f27255960` |
| parent capability | `FROZEN_MODEL_CAN_INFER` |
| development window | `P0C_DEVELOPMENT_V1`，`2024-07-04..2026-03-10`，dataset `81e2c9ba...` |
| sealed holdout | `ADVISORY_SEALED_HOLDOUT_2026Q4_V1`，`2026-08-31..2026-11-30`，`SEALED_UNCONSUMED` |
| backend / DDL / runtime | `noop` / `noop` / `noop` |

P0-C bundle `81e2c9ba...` 已包含 386 个候选决策日、405 个排名上下文日、Top40 排名、Top20 policy episode 标签及 28 条 READY CPCV path。它没有 Top50、没有全市场 winner 标签，也没有可重放的全市场 PIT membership。因此 N1 不得把“存在行情/预测”替代为股票池规则，也不得用当前成分股列表回填历史。

当前 canonical PIT v2 只读预检为：

- universe：`aistock_equity_pit_canonical_v2`；
- rule：`shsz_a_252td_st_delist_asof_v2`；
- scope：`canonical_all_listed`；
- 状态：`ready` 且 `dirty=false`；
- 覆盖：`2018-08-01..2026-07-31`，覆盖 N1 开发窗口；
- 规则包含上市满 252 个交易日、按日 ST 与退市 PIT；停牌不改变 membership，仅改变当日可交易状态。

N1 的问题不是“再找一个 P0 模型”，而是分别回答：

1. 全市场赢家是否已经进入父包 Top20/40/50；
2. 若赢家进入 Top20，完美 Top5 排序能增加多少成本后收益；
3. 在冻结 feature schema v2 和一个简单模型下，该增量是否可 cross-fitted 学习；
4. 理论空间与可学习空间落在哪个 typed 象限，后续 N2/N3 应走哪类诊断，而不是在 N1 内选最终模型。

## 2. Scope

N1 交付以下能力：

1. `AdvisoryOracleMiniContractV1`：冻结 PIT、候选、标签、成本、容量、benchmark、执行价、统计和 sealed 边界。
2. canonical PIT v2 的一次只读 MVCC 快照冻结；该步骤只读现有状态/跨度并写文件 artifact，不重建、不激活、不写数据库。
3. 仅开发窗口的 Top20/40/50 winner recall、rank bucket、Top20 perfect Top5 与原始 Top5 对照。
4. `AdvisoryLearnabilityAuditV1`：冻结一个 Ridge family、一个超参集合、feature schema v2 和 28-path CPCV cross-fitting。
5. 干预支持度、moving-block/cluster 区间、MDE 与 typed 四象限收据。
6. ORACLE_DIAGNOSTIC 与 LEARNABILITY_AUDIT 两条 registry 记录的一次原子追加、最小路线页更新和 exact retry。
7. 一个批处理 CLI；真实运行只在 WSL `rdagent-gpu` 中读取文件行情/因子，峰值 RSS 小于 8GB。

## 3. Non-goals

- 不读取 sealed holdout，不生成 confirmation 或 activation evidence。
- 不搜索模型家族、loss、alpha、阈值、feature subset、超参或随机种子。
- 不训练候选生产模型，不保存可部署模型，不修改 P0-D 至 P0-L 结论。
- 不执行 N2 Entry Guard、Exit-label、QE alpha 生成/评价或 N3 主线选择。
- 不新建研究 UI、API、数据库表、审批、调度器、缓存平台、ModelOps 或证据仓库。
- 不调用 Tushare，不重建 PIT，不用 live DB 行情生成标签。
- 不把停牌、涨跌停或普通数据缺失当作删除股票的理由；保留 typed row 与覆盖统计。
- 不形成动态资金仓位、组合权重、交易指令或 Selection 写入。
- 不因负结果、欠功效或不确定结果放宽预注册合同。

## 4. Architecture

```text
N0 formal receipt + research-window contract
  -> authorize ORACLE_DIAGNOSTIC(development only)
  -> authorize LEARNABILITY_AUDIT(development only)
  -> sealed holdout denied before any market-data loader

canonical PIT v2 state + spans (one repeatable-read read-only transaction)
  -> FrozenPitSnapshot (252 sessions / ST / delist PIT)
  -> snapshot SHA + spans SHA bound into N1 request

P0-C exact request + Prediction Store descriptors
  + PIT-filtered parent predictions
  -> exact Top50 parent ranking

PIT-eligible full universe + Qlib daily + suspend sidecar + CSI300
  -> fixed H20 open-to-open outcome rows
  -> full-universe winners + Top20/40/50 recall
  -> baseline Top5 / perfect Top5 / rank buckets

Top20 feature schema v2
  + N1 H20 label-information intervals
  + P0-C frozen split policy (8 groups / 2 validation / 20-day embargo)
  -> rebuild exactly 28 READY CPCV paths for N1 labels
  -> one fixed Ridge pipeline
  -> averaged cross-fitted OOF score
  -> learnability Top5 vs frozen baseline Top5

oracle receipt + learnability receipt
  -> typed quadrant + intervention/MDE receipt
  -> immutable bundle
  -> atomic two-row registry append
  -> derived current_route.md
```

流水线不扫描 `latest`，所有输入路径、hash、日期与 identity 均来自冻结 request。

## 5. Contracts

### 5.1 `AdvisoryOracleMiniContractV1`

请求必须绑定：

```text
N0 completion receipt ref/hash
research window contract ref/hash
P0-C bundle id + manifest file hash + request hash
program/binding/package/manifest/runtime-semantics identity
baseline/shadow/cost/split policy hash
Prediction Store descriptors and representative run ids
canonical PIT snapshot file SHA + spans SHA + rule/parameter/source identity
market calendar and suspend sidecar identity
Qlib/factor/suspend roots
repository commit and WSL repository root
decision range / data cutoff
label, cost, capacity, inference and resource policies
```

请求创建后 hash 不得变化；同 request hash 的运行只允许 exact retry。

### 5.2 PIT membership 与 tradability

PIT snapshot 冻结规则：

1. 使用一个 `REPEATABLE READ READ ONLY` 事务读取 canonical state 与窗口跨度。
2. state 必须为 `ready`、`dirty=false`，rule 必须为 `shsz_a_252td_st_delist_asof_v2`，且覆盖整个开发窗口。
3. snapshot 截断为 `2024-07-04..2026-03-10`，用既有 `freeze_pit_snapshot()` 规范化、检查重叠并计算 hash。
4. snapshot 在结果计算前用 create-only 写入；已存在且内容相同为 exact no-op，不同则冲突。
5. 父包两腿 prediction 在横截面标准化和排序之前按决策日 PIT membership 过滤。

membership 与 tradability 分开：

- 新股、ST 与确认退市按 PIT span 决定是否属于股票池；
- 停牌不删除 membership；entry/exit 当日由 suspend sidecar 决定是否可执行；
- 一字涨停禁止 entry，一字跌停或停牌禁止当日 exit 并进入有界顺延；
- 非权威缺失保留 typed `DATA_UNAVAILABLE_*`，不得静默删除或令全任务失败；
- 每日 outcome coverage 进入收据，低覆盖日标记不可判定，不伪造收益。

### 5.3 决策时钟与固定 winner label

N1 排名标签固定为 `ADVISORY_TIER1_H20_OPEN_TO_OPEN_V1`：

```text
T close       : decision；模型/排名只能看到 <= T
T+1 open      : entry；必须 PIT eligible 且可执行
T+21 open     : planned exit，即 entry 后完整持有 20 个交易时段
T+21..T+26    : exit 停牌/一字跌停时最多顺延 5 个交易日
```

执行与收益：

- entry/exit 均使用实际可执行 open；benchmark 使用同日 CSI300 open；
- `gross_excess_bps = stock_open_to_open_bps - benchmark_open_to_open_bps`；
- `round_trip_cost_bps = buy_cost_bps + sell_cost_bps = 6.90`；
- `capacity_haircut_bps = 5.00`，仅是冻结研究折损，不含资金权重语义；
- `economic_net_excess_bps = gross_excess_bps - 6.90 - 5.00`；
- 未能 entry 的候选在固定槽位 arm 中按现金 `0 bps` 计，不从槽位分母删除；
- 非权威缺失/右删失不伪造为现金，相关日期按显式 coverage 规则决定是否可评价。

每个决策日的 full-universe winner 是全部可执行且 outcome 完整股票中 `economic_net_excess_bps` 最高的 5 只。该未来标签仅用于 oracle/训练 target，永远不可作为 T 日 feature。

### 5.4 Candidate 与 Oracle

父包排序固定复用 P0-C 两腿、terminal weights、z-score、instrument tie-break；唯一区别是：

- 在 z-score 前应用 canonical PIT membership；
- 深度从 40 扩为 50；
- 不改变父模型、权重或 runtime semantics。

Oracle 至少输出：

1. 每日及聚合 Top20/40/50 winner recall；
2. PIT universe、双腿共同 prediction、可执行 outcome 的分层覆盖；
3. rank bucket `1-5 / 6-10 / 11-20 / 21-40 / 41-50` 的均值、中位数、胜率和 coverage；
4. 原始 rank 1-5 固定五槽收益；
5. Top20 内按未来 `economic_net_excess_bps` 完美选择的固定五槽收益；
6. perfect-minus-baseline 的 moving-block 区间、MDE 和 typed 理论空间。

Oracle 是 `CLAIRVOYANT_ACTION_CEILING`，`deployable=false`，不得输出模型文件。

### 5.5 Learnability frozen baseline

Learnability 只允许一个冻结 pipeline：

| 项目 | 冻结值 |
|---|---|
| feature schema | `advisory_feature_schema_v2_suspension_aware` 全部 `MODEL_FEATURE_COLUMNS` |
| categorical | 仅训练折内词表；unknown ignore |
| numeric missing | 仅训练折 median imputer |
| numeric scaling | 仅训练折 StandardScaler |
| categorical encoding | OneHotEncoder，dense，handle_unknown=ignore |
| estimator | `sklearn.linear_model.Ridge` |
| alpha / solver | `100.0` / `svd` |
| fit_intercept | `true` |
| model/seed count | 1 个确定性 family；无 seed roster |
| target | 同一 H20 `economic_net_excess_bps`；不可 entry 为 0，未知 outcome 不进入 target fit |
| split | 复用 P0-C 冻结的 8 group / 2 validation group / 20-day embargo 参数；必须按 N1 H20 实际 label-information interval 重新 purge 并生成 28 READY paths，不得直接复用旧 episode 的 train-date 列表 |
| OOF merge | 每个 date-symbol 的所有 validation-path 预测取算术平均；预期每行 7 个 OOF 预测 |

每个 validation path 的 imputer、scaler、category vocabulary 和 Ridge 只 fit outer-train。N1 标签的信息区间从实际 entry 延伸到实际/顺延 exit；不可 entry 的信息区间止于 entry。路径生成必须先剔除与 validation 信息区间重叠的训练行，再应用 20 日 embargo。任何从 validation 或未来日期 fit 的统计量、或直接复用旧标签路径，均为泄漏并 fail closed。

Learnability arm 每日按 OOF score 选择 Top5，与原始 rank 1-5 比较；不选择 threshold，不根据结果换模型或换 feature。

### 5.6 统计、MDE 与干预支持度

所有日级区间使用同一个冻结 moving-block bootstrap：

```text
confidence = 95%
block_length = 20 trading days
bootstrap_repetitions = 2000
random_seed = 20260831
two_sided_alpha = 0.05
power = 0.80
```

MDE 固定按 block-bootstrap 标准误计算：

```text
MDE = (z_0.975 + z_0.80) * bootstrap_standard_error
```

合同级最小经济收益为 `5.00 bps/五槽日`。由于 arm 收益已扣除 round-trip cost 与 capacity haircut，判定不再重复扣费：

- `HIGH`：95% 下界严格大于 5 bps；
- `LOW`：95% 上界不大于 5 bps；
- `INCONCLUSIVE`：区间跨越 5 bps。

oracle 与 learnability 若要作为 `DIRECTION_GATE`，各自还必须满足：

- 实际 Top5 set 改变至少 60 个交易日；
- 干预日至少占可评价日 25%；
- 以 T 日可见 CSI300 trailing-20 close return 划分 `UP_OR_FLAT` / `DOWN`，每个出现的 regime 至少 20 个干预日；
- 每个 date-symbol 恰有预期 OOF 预测计数；
- MDE 不高于实际 point lift 与 5 bps 中较大者。

任一 arm 不足时该 arm 的结果仍完整产出，但标记 `EXPLORATORY_INSUFFICIENT_SUPPORT` 或 `EXPLORATORY_UNDERPOWERED`，只能导航，不能关闭方向或支持激活。

### 5.7 Typed 四象限

`theoretical_state` 来自 perfect Top5 lift，`learnability_state` 来自 OOF Top5 lift：

```text
THEORETICAL_LOW__LEARNABILITY_LOW
THEORETICAL_HIGH__LEARNABILITY_LOW
THEORETICAL_HIGH__LEARNABILITY_HIGH
THEORETICAL_LOW__LEARNABILITY_HIGH_ANOMALY
INCONCLUSIVE__<point-estimate quadrant>
```

- 理论低/学习低：排名层降级，检查上游召回、股票池或动作空间。
- 理论高/学习低：只证明当前信息集 + 冻结简单模型不足；先扩信息集，后另立模型 lineage。
- 理论高/学习高：允许 N3 后创建一项 confirmation candidate，不可直接激活。
- 理论低/学习异常高：先排查泄漏、标签、基线和评价错误。
- 欠功效/支持不足：保留 point quadrant，但 `direction_ready=false`。

N1 不根据四象限直接启动 N3；按父蓝图先进入 N2 的 Entry/Exit 独立诊断。

### 5.8 Registry 与 decision use

完成 bundle 后一次 `append_batch()` 两条记录：

| experiment | study_type | objective | 默认 decision_use |
|---|---|---|---|
| `ADVISORY-N1-TIER1-ORACLE` | `ORACLE_DIAGNOSTIC` | `ALPHA_RANKING` | powered 则 `DIRECTION_GATE`，否则 `NAVIGATION_ONLY` |
| `ADVISORY-N1-TIER1-LEARNABILITY` | `LEARNABILITY_AUDIT` | `ALPHA_RANKING` | powered/support 足够则 `DIRECTION_GATE`，否则 `NAVIGATION_ONLY` |

两条记录都只登记一个预注册 study trial；28 path 是 cross-fitting 结构，不伪装成 28 次模型 trial。两条记录绑定同一 request、dataset、policy、PIT snapshot 与 consumed development window，均禁止 `ACTIVATION_EVIDENCE`。

## 6. Sealed holdout / leakage hard boundary

`run` 的执行顺序必须为：

1. 解析 request；
2. 加载并验证 N0 window contract；
3. 分别授权 ORACLE_DIAGNOSTIC 与 LEARNABILITY_AUDIT；
4. 确认 `sealed_holdout_accessed=false` 且 canonical consume receipt 不存在；
5. 才允许调用 Prediction Store、Qlib、factor 或 suspend loader。

poison tests 必须证明：

- request 日期与 sealed 任意相交时，在 loader 调用前拒绝；
- future rows 注入 feature source 不改变 T 日 features/OOF；
- PIT snapshot、Prediction Store、P0-C、calendar/suspend、policy hash 任一漂移均 fail closed；
- oracle future label 列无法进入模型 feature matrix。

## 7. Batch pipeline and artifacts

正式根：

```text
F:/Dev/AIstock_model_artifacts/advisory_n1_tier1_oracle_learnability_20260831/
  frozen_pit_snapshot.json
  n1_request.json
  tier1_bundles/<bundle_id>/
    request.json
    oracle_daily.parquet
    oracle_recall_daily.parquet
    outcome_coverage.parquet
    learnability_oof.parquet
    oracle_receipt.json
    learnability_receipt.json
    quadrant_receipt.json
    registry_records.json
    resource_report.json
    manifest.json
```

发布顺序：临时 sibling 目录 -> 全文件 hash/readback -> 原子 rename。若同 request 已有完整 bundle，返回 `EXISTING_BUNDLE`；内容不同则 typed conflict。若 bundle 已发布但 registry/route 尚未完成，exact retry 只恢复 append/route，不重算市场结果。

## 8. CLI

唯一入口：`scripts/advisory_n1_tier1_oracle_learnability.py`。

```text
freeze-pit-snapshot  # Windows，显式 --env-file；仅只读数据库
prepare-request      # Windows/WSL，全部路径显式
run                  # WSL rdagent-gpu；批量计算
inspect              # 只读 readback
```

CLI 规则：

- 不从 cwd 或 `latest` 猜输入；
- stdout 只输出一条 JSON summary，stage progress 写 stderr；
- failure 返回非零和 typed reason；
- `run` 必须验证 WSL、`CONDA_DEFAULT_ENV=rdagent-gpu` 与 exact repository commit；
- 峰值 RSS 超 8GB 立即停止并保留 typed failure receipt，不降采样冒充完成。

## 9. Error contract

| reason_code | 语义 |
|---|---|
| `ADVISORY_N1_REQUEST_INVALID` | request schema/hash/预注册值无效 |
| `ADVISORY_N1_N0_IDENTITY_MISMATCH` | N0 receipt/window/registry identity 漂移 |
| `ADVISORY_N1_PIT_STATE_NOT_READY` | canonical PIT 状态、规则或覆盖不可用 |
| `ADVISORY_N1_PIT_SNAPSHOT_CONFLICT` | PIT snapshot 已存在但内容不同或 hash 漂移 |
| `ADVISORY_N1_SOURCE_IDENTITY_MISMATCH` | P0-C、prediction、calendar、suspend、factor 或 policy identity 漂移 |
| `ADVISORY_N1_SEALED_HOLDOUT_ACCESS_DENIED` | 任何诊断请求触及 sealed holdout |
| `ADVISORY_N1_LABEL_CLOCK_INVALID` | T/T+1/T+21 与 calendar 不闭合 |
| `ADVISORY_N1_OUTCOME_COVERAGE_INSUFFICIENT` | 无足够可评价日期，不能形成 typed N1 结果 |
| `ADVISORY_N1_CROSSFIT_INVALID` | train/validation、embargo、OOF count 或未来毒化失败 |
| `ADVISORY_N1_BUNDLE_CONFLICT` | 同 request 的 immutable bundle 内容冲突 |
| `ADVISORY_N1_MEMORY_LIMIT_EXCEEDED` | 峰值 RSS 超 8GB |
| `ADVISORY_MODEL_TRAINING_REQUIRES_WSL` | `run` 不在 WSL rdagent-gpu |

## 10. Implementation Plan / Implementation scope

允许修改：

```text
backend/services/advisory_model_first/tier1_oracle_contracts.py
backend/services/advisory_model_first/tier1_oracle_pipeline.py
backend/services/advisory_model_first/research_control.py
backend/tests/advisory_model_first/test_oracle_mini_contract.py
backend/tests/advisory_model_first/test_oracle_learnability_audit.py
backend/tests/advisory_model_first/test_tier1_oracle_pipeline.py
backend/tests/advisory_model_first/test_research_control_cli.py
scripts/advisory_n1_tier1_oracle_learnability.py
tests/aistock_validation/catalog/file_ownership.yaml
docs/architecture/advisory_n1_tier1_oracle_learnability_f2_detailed_design_20260831.md
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
```

任何新增范围先更新本节，再编辑源码。

## 11. Verification plan

### 11.1 Contract and PIT

- request canonical hash、非法枚举/数值/日期、identity drift、exact retry。
- canonical PIT state poison、规则/coverage poison、snapshot overlapping span poison。
- 上市不足 252 session 不入 membership；ST/退市按日生效。
- 停牌 row 保留 membership；entry/exit typed，不静默删除。
- 只读事务测试证明无 DDL/DML SQL。

### 11.2 Label and oracle

- T close 不读 T+1；T+1 entry、T+21 planned exit、最多 5 日顺延。
- 一字涨停、停牌、一字跌停、权威停牌缺 bar、非权威缺失、右删失。
- benchmark 同实际 entry/exit 日期；6.9 bps cost + 5 bps capacity 只扣一次。
- Top20/40/50 recall、rank bucket、固定五槽 cash、perfect Top5 对照。
- winner/outcome future 列不进入 feature matrix。

### 11.3 Learnability and inference

- 按 N1 label-information interval 重新 purge 后仍为 exact 28 READY path、20-day embargo、每行 7 个 OOF 预测；旧 P0-C episode 的 train-date 列表不得直接复用。
- imputer/scaler/vocabulary 仅训练折 fit；validation/future poison 不改变旧预测。
- 只有 Ridge(alpha=100, solver=svd)，无 family/seed/threshold search。
- moving-block bootstrap 固定种子可复现；MDE、干预次数/比例/regime 分类正确。
- 四象限、欠功效和支持不足的 decision_use 正确。

### 11.4 Delivery

- immutable bundle、文件 hash/readback、同 request exact retry、冲突拒绝。
- registry 两条原子 append；中断恢复不重复市场计算；route 显示 N1 complete 与 N2 next。
- sealed consume receipt 仍不存在。
- F2 validator、定向 pytest、Ruff、compile、ownership/guardrail、`git diff --check`。
- 正式 WSL 运行的 resource report 峰值 RSS < 8GB。

## 12. Design Acceptance Index

| ID | Requirement |
|---|---|
| F-191 | 只使用 N0 声明的 development window；sealed 在 loader 前拒绝 |
| F-192 | canonical PIT v2 以单次只读 MVCC 快照冻结；上市 252 session、ST、退市 membership 正确 |
| F-193 | 停牌/涨跌停/缺失保留 typed row，不以行情存在性替代 membership |
| F-194 | 父包两腿在 PIT 过滤后按原 terminal weights 重建 exact Top50 |
| F-195 | H20 open-to-open 标签、benchmark、成本、容量与 fixed-slot cash 语义冻结 |
| F-196 | 全市场 winner 的 Top20/40/50 recall 与 coverage 完整 |
| F-197 | Top20 perfect Top5、baseline Top5 与 rank bucket 完整 |
| F-198 | Learnability 只使用 feature schema v2 + 一个冻结 Ridge family |
| F-199 | exact 28-path CPCV、train-only preprocessing、平均 OOF 与未来毒化闭合 |
| F-200 | 干预支持度、moving-block CI、MDE 与 regime 覆盖预注册并可复现 |
| F-201 | typed 四象限区分理论空间、可学习空间与欠功效/支持不足 |
| F-202 | oracle/learnability 分别登记且不污染 DSR/PBO 模型 trial 计数 |
| F-203 | immutable bundle、exact retry、registry 原子追加和 route 恢复闭合 |
| F-204 | 无 API/UI/DDL/DML/runtime/descriptor/Selection/动态仓位影响 |
| F-205 | WSL rdagent-gpu 真实开发窗口运行且 peak RSS < 8GB |

## 13. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-191 | `backend/services/advisory_model_first/tier1_oracle_contracts.py`; `tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_oracle_mini_contract.py` | LOCAL_VERIFIED | none |
| F-192 | `backend/services/advisory_model_first/tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_oracle_mini_contract.py`; PIT snapshot `5c3a5247...` | LOCAL_VERIFIED | none |
| F-193 | `backend/services/advisory_model_first/tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_tier1_oracle_pipeline.py` | LOCAL_VERIFIED | none |
| F-194 | `backend/services/advisory_model_first/tier1_oracle_pipeline.py`; `policy_rank_source.py` | `backend/tests/advisory_model_first/test_tier1_oracle_pipeline.py` | LOCAL_VERIFIED | none |
| F-195 | `backend/services/advisory_model_first/tier1_oracle_contracts.py`; `tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_tier1_oracle_pipeline.py` | LOCAL_VERIFIED | none |
| F-196 | `backend/services/advisory_model_first/tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_tier1_oracle_pipeline.py` | LOCAL_VERIFIED | none |
| F-197 | `backend/services/advisory_model_first/tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_tier1_oracle_pipeline.py` | LOCAL_VERIFIED | none |
| F-198 | `backend/services/advisory_model_first/tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_oracle_learnability_audit.py` | LOCAL_VERIFIED | none |
| F-199 | `backend/services/advisory_model_first/tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_oracle_learnability_audit.py` | LOCAL_VERIFIED | none |
| F-200 | `backend/services/advisory_model_first/tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_oracle_learnability_audit.py` | LOCAL_VERIFIED | none |
| F-201 | `backend/services/advisory_model_first/tier1_oracle_contracts.py`; `tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_oracle_learnability_audit.py` | LOCAL_VERIFIED | none |
| F-202 | `backend/services/advisory_model_first/research_control.py`; `tier1_oracle_pipeline.py` | `backend/tests/advisory_model_first/test_research_control_cli.py` | LOCAL_VERIFIED | none |
| F-203 | `backend/services/advisory_model_first/tier1_oracle_pipeline.py`; `research_control.py` | `backend/tests/advisory_model_first/test_tier1_oracle_pipeline.py` | LOCAL_VERIFIED | none |
| F-204 | no production surface | artifact: `tests/aistock_validation/catalog/file_ownership.yaml` | LOCAL_VERIFIED | none |
| F-205 | `scripts/advisory_n1_tier1_oracle_learnability.py` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n1_tier1_oracle_learnability_20260831` | DESIGN_READY | none |

## 14. Risks / failure modes

| 风险 | 影响 | 处理 |
|---|---|---|
| hindsight oracle 被误当可部署模型 | 方向误判 | oracle receipt 固定 `deployable=false`，与 learnability 分开登记 |
| sealed holdout 被诊断读取 | 独立证据污染 | window authorization 在所有 loader 之前；future/holdout poison test |
| PIT membership 被行情可用性替代 | 新股、ST、退市和停牌语义错误 | canonical v2 快照决定 membership；suspend 只决定 tradability |
| 同一开发窗持续调参 | 多重检验与过拟合 | 一个 Ridge family、一个超参集合、一个 feature set；变更必须新 lineage |
| 稀疏干预制造高方差正结果 | 假增量 | 预注册次数、比例、regime 与 block inference；不足仅 exploratory |
| 全市场批量加载超内存 | 任务中断 | 分阶段释放 prediction/market frame，resource receipt，8GB fail-closed |
| registry 成功而 bundle 不完整 | 路线状态漂移 | bundle 先 immutable publish，随后两条记录原子 append；exact retry 补 route |
| 治理膨胀 | 延误模型演进 | 只复用 JSONL/route，不增加 UI、DB、scheduler 或通用平台 |

## 15. Rollout / rollback

- rollout 只创建 task artifact、向既有 JSONL registry 追加两行并更新派生 route。
- 未合入源码不得形成正式 N1 方向证据；候选运行仅供实现验证。
- 源码回滚只需 revert PR；无数据库、runtime、descriptor 或模型激活需要回滚。
- 正式 artifact 不删除；若发现实现错误，以新 lineage/attempt 追加纠正，不改写旧 registry 行。

## 16. Production gates

```text
backend_restart = noop
production_ddl_gate = noop
production_dml_gate = noop
dependency_install = noop
runtime_activation = noop
client_install = noop
```

## 17. DESIGN-COMPLIANCE-001

1. 禁止简化交付：缺 full-universe recall、perfect Top5、fixed cross-fit、四象限或真实运行任一项，不得称 N1 complete。
2. 禁止静默错误：停牌/缺失、identity drift、欠功效和 sealed 访问均使用 typed 状态，不伪造成功或 0 收益。
3. 禁止改变业务逻辑：不改父包模型/权重、P0 结论、Selection、成本或用户授权边界。
4. 禁止私增门禁：只执行本设计的 PIT、sealed、identity、统计、资源和发布约束；负结果是合法完成，不增设人工审批。
