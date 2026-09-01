# AIstock Advisory N2 Entry / Exit Formal Audit F2 详细设计 v1.0

- 日期：2026-09-02
- Feature tier：F2
- 当前状态：`IMPLEMENTED_LOCAL_TARGETED_VERIFIED_FORMAL_RUN_PENDING`
- 业务归属：Selection Center / Advisory / Model-first Research
- 目标合同：`RISK_MANAGED_ADVISORY`
- 研究类型：`ORACLE_DIAGNOSTIC`
- 证据用途：`NAVIGATION_ONLY`
- 前置源码：PR #4170 / merge commit `65f837882c8cd400ed78f884e70470c6002edde3`

## 1. Background / 已验证事实

1. N2 Entry/Exit 源码核心已实现增量价值、干预支持、Entry Guard、Entry paired label、Exit oracle、证据分级与无仓位边界；源码存在不等于动作空间有效或可学。
2. N1 formal bundle `74827d03...` 绑定 386 个 development 决策日、Top40 ranking、canonical PIT、baseline/shadow/cost policy、Qlib daily 与 suspend identity；sealed holdout 未读。
3. M4 price-range bundle `1a939f05...` 含 80 个 test 决策日、1600 行冻结 `entry_gap_q10/q50/q90` 与实际 executable gap；binary executable head 仍为不可学习的 `UNCALIBRATED`，本任务不消费该概率作为选择器。
4. 2026-09-02 真实可行性 spike 只读上述两份冻结数据和同一 Qlib 日线：
   - M4×N1 exact key 共同窗口为 `2025-11-07..2026-02-02`，60 个决策日、1200 行；
   - 1199 行 baseline episode 已成熟；
   - Qlib `target open / decision close - 1` 与 M4 `entry_gap_return` 在 1200/1200 行逐值完全相等，max error 0，行情缺失 0。
5. N2-B v2 StrategyPackage 审计独立运行。本任务不读取其经济结果、不按其结果调整 Entry/Exit arm，也不选择 N3 主线。

## 2. Goal and scope

交付一个最小且完整的 formal N2 action diagnostic pipeline：

1. 冻结 request，精确绑定 N0/N1、M4、policy dataset、Qlib/suspend、repository commit、registry 与所有 arm policy hash。
2. Entry 在固定 60 日/1200 行共同窗口比较无保护、固定 3%、固定 5%、M4 q90 动态 guard；每个 guard 同时报告固定槽位现金与显式 rank6..20 补位。
3. Entry 报告 perfect skip oracle（原 Top5 现金）与 perfect skip+replacement oracle（仍按 Selection rank 依次补位），不使用未来收益重排。
4. Exit 在完整 N1 development window 内调用真实 baseline policy simulator，生成“下一可交易开盘退出 vs 继续 baseline”的增量标签；每个 episode 只选择最大正增量时点形成 perfect-exit ceiling。
5. Entry/Exit 分别产生预注册干预支持 receipt、typed coverage、净收益/MDD/tail/cash/延迟指标和不可部署 oracle summary。
6. 发布 immutable bundle、exact retry、资源与 source identity receipt；向 trial registry 原子追加 Entry/Exit 两条 0-trial navigation record。
7. route 继续保持 N2，直到 N2-B 与本 formal audit 均完成后由后续 N3 分流任务统一判定。

## 3. Non-goals

- 不训练、cross-fit、调参或选择 Entry/Exit 模型；learnability 是后续独立步骤。
- 不按结果调整 3%/5%、q90、yellow boundary、cash/replacement、support threshold 或窗口。
- 不使用 M4 executable probability；不修复或重新校准其极端不平衡标签。
- 不把 replacement 按未来收益排序；只按冻结 Selection rank 顺序扫描。
- 不给 `REDUCE` 赋予 0.5 仓位；formal shadow 将其明确解释为“提示但 entry action 不变”。
- 不输出动态权重、数量、订单、Selection 写入、API、Paper/QMT/runtime binding。
- 不读取 sealed holdout，不生成 prospective receipt，不支持 activation。
- 不建设通用回测平台、OPE 平台、scheduler、缓存、UI、审批或归档系统。
- 不执行后端重启、DDL/DML、依赖安装或生产激活。

## 4. Architecture

```text
explicit N1 request/bundle + explicit M4 request/bundle + clean source commit
                                  |
                source identity + development access guard
                                  |
               exact M4/N1 key overlap = 60d / 1200 rows
                         /                         \
       Qlib decision close + target open      N1 Top40 + full market
                 exact gap parity                  |
                         |                  real baseline simulator
        four candidate-level guard arms             |
                         |                  Exit incremental labels
             cash / explicit replacement             |
                         |                  best positive exit / episode
             daily equal-slot outcomes                |
                         \                         /
                 support + metrics + typed coverage
                                  |
       immutable bundle + two 0-trial registry records + unchanged N2 route
```

代码边界：

- `entry_exit_formal_contracts.py`：request、support spec、receipt 与 manifest identity；
- `entry_exit_formal_pipeline.py`：source verification、Entry/Exit formal computation、bundle、registry、exact retry；
- `scripts/advisory_entry_exit_formal_audit.py`：`prepare/run/inspect` 薄 CLI；
- 复用 `entry_guard_decision.py`、`incremental_value_labels.py`、`exit_label_oracle.py`、N1 loader 与 research registry；
- 不修改 Selection、PriceGuard、Advisory API 或数据库层。

## 5. Contracts / Frozen request

request 必须冻结：

- N1 request、formal bundle manifest、policy dataset manifest 的 URI/SHA256/size/semantic ID；
- M4 training request、formal bundle manifest、test predictions 的 URI/SHA256/size/bundle ID；
- N0 completion/window、registry 与 route path，从 N1 身份继承并逐项验证；
- Entry 共同窗口 `2025-11-07..2026-02-02`、60 日、1200 exact keys、1199 matured rows；
- Exit 窗口 `2024-07-04..2026-02-02`，outcome cutoff `2026-03-10`；
- `NO_GUARD`、`FIXED_GAP_3`、`FIXED_GAP_5`、`FROZEN_DYNAMIC_Q90` policy hash；
- dynamic 语义：`max_gap_bps=max(0, entry_gap_q90*10000)`；`max_buy_price=decision_close*(1+max_gap_bps/10000)`；不使用 test outcome 拟合；
- Entry cash/replacement、REDUCE=`ENTER_UNCHANGED_ADVICE_ONLY`、target slots=5、replacement depth=20；
- Entry/Exit support spec：minimum intervention count、day fraction、required regimes、days/regime、block length、minimum effective blocks；
- bootstrap/summary policy、repository clean commit、output root、RSS limit 8 GiB；
- `objective_contract=RISK_MANAGED_ADVISORY`、`study_type=ORACLE_DIAGNOSTIC`、`decision_use=NAVIGATION_ONLY`、`sealed_holdout_accessed=false`、trial counts all 0。

request identity 是除自身 id/hash 外 canonical JSON。unknown field、source drift、窗口/row/key 漂移、额外 arm、policy hash 漂移、非 clean commit 或 sealed path 一律拒绝。

## 6. Entry formal semantics

### 6.1 Input parity

1. M4 prediction 与 N1 baseline episode 按 `(decision, target, instrument)` one-to-one inner join；结果必须精确为 60 日×20 行。
2. Qlib 对所有共同 symbols 一次区间读取；每行读取 decision close、target open、limit up/down 与 factor。
3. `target_open/decision_close-1` 必须与 M4 `entry_gap_return` 逐行 `atol=1e-10` 一致；任何缺失或偏差 fail closed。
4. M4 q10/q50/q90、Selection rank、binding/schema hash 来自冻结 artifact；actual gap/open 只进入 T+1 observation。

### 6.2 Arms

固定 arm：

- `NO_GUARD_BASELINE`；
- `FIXED_3_CASH`、`FIXED_3_REPLACE`；
- `FIXED_5_CASH`、`FIXED_5_REPLACE`；
- `DYNAMIC_Q90_CASH`、`DYNAMIC_Q90_REPLACE`；
- `PERFECT_SKIP_CASH_ORACLE`、`PERFECT_SKIP_REPLACE_ORACLE`。

每个规则 arm 对 20 个候选分别调用真实 Entry Guard：

- cash arm 只观察原 Top5；`SKIP/WAITING` 对应 0 bps 空槽；`ACCEPT/REDUCE` 使用同一 baseline episode net return；
- replacement arm 按 rank1..20 扫描，跳过 `SKIP/WAITING`，直到填满 5 个固定等权槽位；不得按预测或未来收益重排；
- `REDUCE` 只记录提示，shadow action 为 entry unchanged，不产生权重；
- baseline 未成熟/不可用保持 typed unknown，不填 0；当日经济指标仅在已知槽位满足冻结 coverage 规则时可用。

perfect skip oracle 只知道候选 baseline net value 的正负：原 Top5 负值留现金；replacement 仍按 rank 顺序用第一个正值候选补位。它是 future ceiling，不可部署。

### 6.3 Entry metrics

每 arm 输出：daily equal-slot net bps、累计净收益、MDD、5% tail、正收益日、cash slot fraction、skip/reduce/waiting/replacement 数、可评价日、相对 no-guard paired lift，以及干预支持。胜率只作辅助。

## 7. Exit formal semantics

1. 从 N1 source 读取 Top40 rankings、完整 Qlib market、benchmark、suspend、calendar、shadow policy 与 cost policy。
2. 公共 `build_exit_label_oracle` 必须内部调用 baseline simulator；其 Top5 baseline label 与冻结 policy dataset 在 episode key、status、entry/exit date、price、net bps 上 exact/strict tolerance parity。
3. 每个 held episode 的全部 review labels 保留；停牌、跌停、market missing、baseline unavailable 与 right censoring 分别统计。
4. perfect-exit ceiling 每个 episode 最多干预一次：选择最大正 `incremental_net_value_bps`；若最大值不正则保持 HOLD。
5. 汇总 episode count、positive intervention count、mean/median/CI lift、避免亏损、过早退出负 advantage 分布、defer days、holding days、MDD proxy/tail 与跨 regime coverage。
6. liability/holding 列不参与 preferred action；oracle summary 标记 `FUTURE_INFORMATION_CEILING/NOT_DEPLOYABLE`。

## 8. Intervention support

- Entry 每个规则 policy 独立统计实际 `SKIP`；Exit 只统计每 episode 最佳正增量退出。
- Entry 与 Exit 均预注册：minimum intervention count=20、minimum intervention day fraction=0.25、required regimes=`UP_OR_FLAT/DOWN`、minimum days per regime=5、block length=20、minimum effective blocks=2；名称严格复用 N1 冻结的 trailing-20 benchmark regime 语义。
- 60 日 Entry 很可能欠功效；不足只标记 `EXPLORATORY_ONLY`，不能因 point lift 正而升级。
- regime 使用 N1 冻结 benchmark trailing-20 close-return sign semantics，不按结果改分区。

## 9. Bundle and registry

immutable bundle 至少包含：

- `request.json`、`source_identity_receipt.json`、`resource_report.json`；
- `entry_decisions.parquet`、`entry_labels.parquet`、`entry_daily.parquet`、`entry_summary.json`、`entry_support.json`；
- `exit_labels.parquet`、`exit_decisions.parquet`、`exit_episode_best.parquet`、`exit_summary.json`、`exit_support.json`；
- `audit_receipt.json`、`registry_records.json`、`manifest.json`。

bundle id 由 request/receipt semantic identity 计算；所有文件保存 SHA256/size/row count。exact retry 必须返回同一 bundle id。

registry 原子追加两条：

- `ADVISORY-N2-ENTRY-GUARD-ORACLE`；
- `ADVISORY-N2-EXIT-LABEL-ORACLE`。

两条均为 0 trial、`EXPLORATORY/NAVIGATION_ONLY`，消费 development window，不消费 sealed holdout。任何一条失败不得只追加另一条。

## 10. Error contract

至少包含：

- `ADVISORY_N2_ACTION_REQUEST_INVALID`
- `ADVISORY_N2_ACTION_SOURCE_IDENTITY_MISMATCH`
- `ADVISORY_N2_ENTRY_KEY_OVERLAP_INVALID`
- `ADVISORY_N2_ENTRY_GAP_PARITY_FAILED`
- `ADVISORY_N2_ENTRY_COVERAGE_INSUFFICIENT`
- `ADVISORY_N2_EXIT_BASELINE_PARITY_FAILED`
- `ADVISORY_N2_ACTION_RESOURCE_LIMIT_EXCEEDED`
- `ADVISORY_N2_ACTION_BUNDLE_INVALID`
- `ADVISORY_N2_ACTION_SEALED_ACCESS_DENIED`

身份、PIT、parity、hash、窗口、schema、registry 错误 fail closed；正常停牌/涨跌停/缺失使用 typed rows。禁止 broad exception 后继续成功。

## 11. Implementation plan

1. 实现 request/support/receipt contracts 与 immutable identity。
2. 实现 prepare/source verification 和 exact 60日 overlap freeze。
3. 实现 Entry candidate decisions、cash/replacement/oracle arms 与 daily metrics。
4. 实现 Exit source load、baseline parity、per-episode best oracle 与 metrics。
5. 实现 bundle/inspect/exact retry、双 registry 原子追加和薄 CLI。
6. 运行直接测试、真实数据 smoke、多轮审核修复、F2 validator、完整 Advisory nox、PR/CI/合入。
7. 合入后从 clean main 冻结 formal request 并运行；源码与实验状态分开报告。

## 12. Verification plan

- request hash、source file hash、unknown field、repo dirty、sealed access、额外 arm均拒绝；
- 真实 fixture 固定 60日/1200 行与 exact gap parity；
- Entry 规则边界、REDUCE unchanged、cash、rank-only replacement、oracle 不重排；
- baseline unknown 不填 0；不足 coverage 日不冒充可用；
- Exit baseline simulator 被调用并与冻结 baseline parity；episode 只取一次最佳正干预；
- suspend/limit-down/defer/censoring/liability poison；
- support 不足保持 exploratory；
- bundle mutation、registry partial append 与 exact retry；
- direct tests、ruff/format/compile/diff、`advisory_modeling_backend`、L0、F2 validator。

## 13. Design Acceptance Index

| ID | Requirement |
|---|---|
| F-801 | request 精确绑定 N1/M4/policy/Qlib/repository/registry，拒绝 sealed 与 identity drift |
| F-802 | Entry 共同窗口固定 60日/1200 行，Qlib gap 与 M4 label 逐值 parity |
| F-803 | 固定3%/5%/q90 guard 的 cash 与显式 rank-only replacement 独立报告 |
| F-804 | Entry perfect skip oracle 不使用未来收益重排，REDUCE 无仓位语义 |
| F-805 | Exit 强制复用 baseline simulator并与冻结 baseline parity |
| F-806 | Exit 每 episode 最多选择一次最佳正增量；停牌/跌停/删失 typed |
| F-807 | Entry/Exit support 阈值预注册；不足只可 exploratory |
| F-808 | immutable bundle、exact retry 与两条 0-trial registry record 原子交付 |
| F-809 | 无 API/DB/runtime/仓位/订单/Selection 写入，formal 结果不冒充 activation |

## 14. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-801 | `backend/services/advisory_model_first/entry_exit_formal_contracts.py`; `backend/services/advisory_model_first/entry_exit_formal_pipeline.py` | `backend/tests/advisory_model_first/test_entry_exit_formal_contracts.py` | IMPLEMENTED_LOCAL_TARGETED_VERIFIED | none |
| F-802 | `entry_exit_formal_pipeline.py::_entry_overlap/_run_entry_audit` | `backend/tests/advisory_model_first/test_entry_exit_formal_pipeline.py`; real spike: 60d/1200 exact | IMPLEMENTED_LOCAL_TARGETED_VERIFIED | none |
| F-803 | `entry_exit_formal_pipeline.py::_build_entry_daily` | `backend/tests/advisory_model_first/test_entry_exit_formal_pipeline.py` | IMPLEMENTED_LOCAL_TARGETED_VERIFIED | none |
| F-804 | `entry_exit_formal_pipeline.py::_build_entry_daily` oracle arms | `backend/tests/advisory_model_first/test_entry_exit_formal_pipeline.py` | IMPLEMENTED_LOCAL_TARGETED_VERIFIED | none |
| F-805 | `entry_exit_formal_pipeline.py::_run_exit_audit/_verify_exit_baseline_parity` | `backend/tests/advisory_model_first/test_entry_exit_formal_pipeline.py` | IMPLEMENTED_LOCAL_TARGETED_VERIFIED | none |
| F-806 | `entry_exit_formal_pipeline.py::_exit_episode_best/_exit_summary` | `backend/tests/advisory_model_first/test_entry_exit_formal_pipeline.py` | IMPLEMENTED_LOCAL_TARGETED_VERIFIED | none |
| F-807 | `ActionSupportSpecV1`; Entry/Exit `build_intervention_support_from_labels` | `backend/tests/advisory_model_first/test_incremental_value_labels.py`; `backend/tests/advisory_model_first/test_entry_exit_formal_pipeline.py` | IMPLEMENTED_LOCAL_TARGETED_VERIFIED | none |
| F-808 | `_publish_bundle/_read_bundle/_deliver_bundle`; `scripts/advisory_entry_exit_formal_audit.py` | `backend/tests/advisory_model_first/test_entry_exit_formal_delivery.py` | IMPLEMENTED_LOCAL_TARGETED_VERIFIED | none |
| F-809 | strict offline schemas and delivery gates | `backend/tests/advisory_model_first/test_entry_exit_formal_delivery.py`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_n2_entry_exit_formal_audit_f2_detailed_design_20260902.md --tier F2` | IMPLEMENTED_LOCAL_TARGETED_VERIFIED | none |

## 15. Risks and controls

| Risk | Control |
|---|---|
| 60日样本欠功效 | support/MDE边界保持 exploratory，不用 point lift 激活 |
| oracle hindsight 被误当可学 | future ceiling/not deployable 固定，learnability另立实验 |
| replacement 偷换排名 | 只按冻结 Selection rank 扫描，未来收益只决定 skip/keep |
| M4/N1 价格基础漂移 | 1200 行逐值 gap parity hard gate |
| Exit baseline 重实现 | 强制调用现有 simulator并与冻结 episode parity |
| 双 registry 部分提交 | bundle 完整验证后 append_batch 原子追加两条 |
| 平台化膨胀 | 只实现单一 N2 request/pipeline/CLI，不建设 scheduler/cache/UI |

## 16. Production gates and rollback

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- backend restart：不需要；仅离线研究 CLI/module。
- DB DDL/DML：无；正式运行只读冻结文件/Qlib/suspend，不访问业务写库。
- rollback：回退新增离线 formal pipeline；已发布 immutable negative/diagnostic bundle 与 registry 记录不得删除或改写。

## 17. DESIGN-COMPLIANCE-001

1. 不用规则、mock、subset 或 spike 冒充模型/正式实验成功；规则 arm、oracle 与真实结果分别标注。
2. 无静默 fallback；正常缺失 typed 保留，身份/parity/证据错误 fail closed。
3. 业务计算位于 service pipeline，CLI 只编排，测试/文档不实现业务逻辑。
4. 不新增人工审批、动态仓位、下单、生产运行时或未授权门禁。
