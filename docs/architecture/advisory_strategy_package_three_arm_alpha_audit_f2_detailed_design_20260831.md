# AIstock Advisory StrategyPackage 三臂 Alpha 信号审计 F2 详细设计 v1.0

- 日期：2026-08-31
- Feature tier：F2
- 当前状态：`LOCAL_IMPLEMENTED_REVIEWED_COMMITTED_COMPUTE_PENDING`
- 业务归属：Selection Center / Advisory / StrategyPackage / QE Research
- 目标合同：`ALPHA_RANKING`
- 研究类型：`ORACLE_DIAGNOSTIC`
- 证据用途：`NAVIGATION_ONLY`
- 前置依赖：PR #4014（N1 Tier-1 oracle 与固定 learnability audit）

## 1. Background / 当前事实

1. N1 正式开发窗口为 386 个决策日：`2024-07-04..2026-02-02`，行情与 H20 outcome 截止 `2026-03-10`；382 日可形成完整 Top20 oracle 与 learnability 日度结果。
2. 当前 Selection 父包 `pkg_ma_8ec5e389fa2c5e484a1ac7e9` 由两个固定信号腿组成：
   - `a1_plus3_LSTM_h20`：代表 run `qear_run_fc5d506390b8f70651a790e6`；
   - `new_FUNDGROWTH_h20`：代表 run `qe_20260622_035058_ec76_L5`；
   - 终态权重分别为 `0.6966591521` 与 `0.3033408479`。
3. N1 证明父包 Top20 对全市场未来 Top5 赢家的总体召回率仅 `0.8808%`；下跌 regime 为 `0.255%`，上涨或平稳 regime 为 `1.244%`。父包候选召回不足是明确瓶颈，但现有证据没有回答：
   - 哪一条腿实际贡献 Alpha；
   - 组合是否稳定优于最佳单腿；
   - 基本面腿是否稀释量价腿；
   - 不同取样区间与 regime 是否改变相对结论。
4. StrategyPackage 注册表中的 Top25 与 Top50 父包引用同一组合预测；它们是候选深度/投资组合政策差异，不是两个独立 Alpha，不得计作两个模型或信号试验。
5. 本任务使用已经消费的 development window 做导航诊断，不读取 sealed holdout、不训练模型、不改变 Selection、StrategyPackage 或 Advisory 运行时。

## 2. Scope

本任务完整交付以下能力：

1. 固定三臂共同窗口审计：
   - `LSTM_ONLY`：`a1_plus3_LSTM_h20=1.0`；
   - `FUNDGROWTH_ONLY`：`new_FUNDGROWTH_h20=1.0`；
   - `IC_WEIGHTED_PARENT`：精确使用父包终态权重。
2. 从 N1 冻结 request、Prediction Store descriptor、canonical PIT snapshot、H20 outcome policy、cost policy 和 benchmark 构建单一不可变 request。
3. 在三臂共同预测交集上计算全市场 signal 指标，并分别保留每臂自身覆盖率，不用静默 inner join 隐藏缺失。
4. 对每臂重建 exact Top50，计算 Top20/40/50 winner recall、Top5 成本后超额、rank bucket、Top20 perfect Top5 headroom 与随机召回倍数。
5. 计算组合相对每个单腿的同日配对增量、腿间相关性、Top5/Top20 重叠和 churn。
6. 报告全窗口、预冻结市场 regime 与自然季度描述性分段；正式推断使用 20 日 moving-block bootstrap。
7. 生成不可变 bundle、exact retry、资源回执以及一条不计模型 trial 的 JSONL registry 记录。

## 3. Non-goals

- 不训练、微调、重训或选择任何模型、seed、特征、权重或阈值。
- 不把 Top25/Top50、季度或 regime 当作新增 Alpha arm。
- 不读取或消费 sealed holdout，不形成 confirmation 或 activation 证据。
- 不修改当前父包权重、Selection 排名、Advisory 推荐、Paper、模拟盘、QMT 或前端/API。
- 不为退役或缺完整预测的其他 StrategyPackage 伪造预测；它们进入后续三态 spike。
- 不用原生历史 Sharpe、年化收益或不同 label/window 的指标替代共同窗口结果。
- 不建立新的研究 UI、DB 表、scheduler、审批或通用实验平台。

## 4. Architecture

```text
N0 development window contract
             |
N1 frozen request + N1 formal bundle
             |
Prediction Store exact descriptors
       |             |
   LSTM score    FUND score
       \             /
        common PIT intersection
          |       |       |
       LSTM     FUND    frozen IC blend
          |       |       |
       exact Top50 per arm
          |       |       |
canonical full-universe H20 outcome panel
          |       |       |
signal metrics / recall / Top5 / oracle / pairwise deltas
                         |
immutable audit bundle + navigation-only registry row
```

代码边界：

- `alpha_signal_audit_contracts.py`：request、arm、metric、receipt 与 manifest schema；
- `alpha_signal_audit_pipeline.py`：source verification、共同 outcome panel、三臂 ranking、统计与发布；
- `tier1_oracle_pipeline.py`：只暴露可复用的 full-universe H20 outcome helper，不改变 N1 语义；
- CLI：只接受显式 request/bundle 路径，不扫描 latest、不读取 `.env` 猜路径。

## 5. Contracts

### 5.1 `AdvisoryThreeArmAlphaAuditRequestV1`

request 固定包含：

- N0 completion/window、N1 request 与 N1 formal bundle 的 role + URI + SHA256 + size；
- N1 `program_id`、binding、package、manifest、policy、cost、PIT、calendar、suspend 与 dataset identity；
- 两条 representative seed run id 及 Prediction Store descriptor；
- 三个且仅三个 `AlphaAuditArmV1`；
- `decision_date_start=2024-07-04`、`decision_date_end=2026-02-02`、`data_cutoff=2026-03-10`；
- 固定统计参数：20 日 block、2000 次 bootstrap、seed `20260831`；
- repository compute commit、显式 output root 与 8GB RSS 上限；request 冻结和正式 compute 均要求 task worktree clean，禁止用旧 HEAD 掩盖未提交源码。

request 使用 canonical JSON hash 形成 `request_id`。未知字段、额外 arm、非正权重、权重和不为 1、source descriptor 漂移或 N1 identity 不一致一律拒绝。

### 5.2 固定 arm 身份

| arm_id | weight vector | 语义 |
|---|---|---|
| `LSTM_ONLY` | `a1_plus3_LSTM_h20=1.0` | 单量价腿 |
| `FUNDGROWTH_ONLY` | `new_FUNDGROWTH_h20=1.0` | 单基本面腿 |
| `IC_WEIGHTED_PARENT` | `0.6966591521 / 0.3033408479` | 当前父包 exact blend |

- 所有 arm 使用相同 `build_policy_rankings` 标准化、score desc + instrument asc tie-break 和 PIT 过滤。
- `IC_WEIGHTED_PARENT` 排名必须与 N1 formal `candidate_rankings_top50.parquet` 逐行一致；不一致即 source/semantic drift。
- Top25/Top50 只作为同一 arm 的 depth 指标，不产生新 `arm_id`。

### 5.3 共同覆盖与缺失

每个决策日同时记录：

- PIT member count；
- 每腿自身 prediction count；
- 两腿共同 prediction count；
- outcome known/matured/cash/unknown count；
- 三臂 exact Top50 是否完成。

Signal 横向比较只使用两腿共同预测且 canonical PIT 有效的 instrument-date；每腿自身覆盖率另行报告。任何缺腿、缺日期、重复 score、非有限 score 或 Top50 不足都形成 typed failure/coverage row，不得用 0、均值或另一腿替代。

### 5.4 单一 full-universe outcome panel

三臂共用同一个 N1 outcome helper：

- 决策日 T，只使用 T 及以前预测和 membership；
- T+1 open 入场，入场 session 计 holding day 1，T+20 planned open 退出；
- 停牌/一字跌停最多顺延 5 个交易日；
- 一字涨停、停牌、上市资格变化与行情缺失保留 typed status；
- benchmark 使用实际 entry/effective exit 日期；
- 成本为 buy `0.95bps` + sell `5.95bps`，另加 `5bps` capacity haircut；
- `MATURED` 使用 economic net excess；合法未成交使用 fixed-slot cash 0；未知 outcome 保持 null。

Outcome 只作为标签与审计结果，绝不进入 score 计算或排序。

### 5.5 Signal 指标

每个 arm、每日和聚合层至少报告：

1. `matured_pearson_ic`：score 与 `economic_net_excess_bps` 的 Pearson IC；
2. `matured_rank_ic`：score 与 `economic_net_excess_bps` 的 Spearman RankIC；
3. `policy_rank_ic`：score 与 `slot_return_bps` 在所有 outcome-known 共同样本上的 Spearman；
4. daily mean/median/std、ICIR=`mean/std`、positive-day fraction、20 日 block CI；
5. common-universe 五分位/十分位收益与 top-minus-bottom spread；
6. Top20/40/50 全市场赢家召回、随机期望召回与 observed/random lift；
7. 每臂 Top5 fixed-slot 成本后超额均值、累计 episode mean 序列、胜率与覆盖率；
8. 每臂 Top20 perfect Top5 headroom，明确标为 hindsight oracle。

随机期望召回以当日三臂共同可选 prediction universe 为抽样支持，并乘以“全市场赢家进入共同可选 universe”的覆盖比例；不得直接用 `depth / PIT member count` 掩盖 prediction coverage 瓶颈。

### 5.6 Pairwise 与组合边际价值

固定比较：

- `IC_WEIGHTED_PARENT - LSTM_ONLY`；
- `IC_WEIGHTED_PARENT - FUNDGROWTH_ONLY`；
- `LSTM_ONLY - FUNDGROWTH_ONLY`。

每对报告：

- 同日 Top5 net excess delta 的 point estimate 与 block CI；
- daily RankIC delta 与 block CI；
- score Spearman/Pearson；
- Top5/Top20 Jaccard、rank correlation 与日间 churn；
- 组合相对 best leg 的增量只按预注册的 `LSTM_ONLY`、`FUNDGROWTH_ONLY` 两腿计算，不从其他包或季度事后选择 best。

### 5.7 区间与 regime

- 主结论：完整共同开发窗口；
- regime：沿用 N1 `CSI300_TRAILING20_CLOSE_RETURN_SIGN_AT_T_V1`，只读取 T 可见信息；
- 季度：仅描述性 sensitivity，不形成方向关闭或激活证据；
- H20 日度样本重叠，所有正式区间使用固定 20 日 moving-block bootstrap；
- 不对看起来最好的季度重新选权重、阈值或 arm。

### 5.8 Registry 与结论边界

审计写入一条：

```text
study_type=ORACLE_DIAGNOSTIC
objective_contract=ALPHA_RANKING
planned/generated/evaluated/selected_trial_count=0
result_class=EXPLORATORY
decision_use=NAVIGATION_ONLY
```

三臂是预冻结角色分解，不是三个模型 trial。结果可以确定后续其他 StrategyPackage spike 顺序和 QE 上游准备优先级，但不得激活、关闭全局方向或消费 sealed holdout。

## 6. Sealed holdout / leakage hard boundary

1. request 必须引用 N0 development window contract；access authorization 在任何 Prediction Store、Qlib、factor 或 outcome loader 之前执行。
2. 只允许 `P0C_DEVELOPMENT_V1`；sealed window id、consume receipt 或越界日期立即失败。
3. 输入特征仅为父包在 T 已生成的冻结 score；H20 outcome 只在排序完成后 join。
4. bundle manifest 固定 `sealed_holdout_accessed=false`、`runtime_eligible=false`、`activated=false`。

## 7. Batch pipeline and artifacts

执行顺序：

1. validate request 与 repository identity；
2. authorize development window；
3. readback N1 request/bundle/PIT/prediction descriptors；
4. 加载两腿 score，执行 PIT 过滤、重复/非有限值检查；
5. 构建一次 full-universe H20 outcome panel；
6. 按三组冻结权重形成三份 exact Top50；
7. 验证父包 arm 与 N1 formal ranking parity；
8. 计算 signal、recall、Top5、oracle、pairwise、regime/quarter 指标；
9. 生成 receipt/manifest/resource report；
10. 临时目录 fsync 后原子发布 content-addressed bundle；
11. bundle readback 完整后原子 append registry，并刷新现有单页路线的 registry hash；`next_task` 必须仍为 `N2_ENTRY_EXIT_QE_PREPARATION`，不得把 N2-A 派生成新主线；exact retry 返回既有 bundle。

`audit_receipt` 分别绑定 `request_sha256`、`source_identity_sha256` 与全部科学结果 JSON/Parquet 文件描述符形成的 `result_files_sha256`；request/source 文件本身以及 `environment.json`、`resource_report.json` 由 manifest 单独绑定，不重复进入科学结果 semantic id，避免 created_at、墙钟或 RSS 波动改变同一科学结果的身份。

Bundle 至少包含：

```text
request.json
source_identity_receipt.json
coverage_daily.parquet
full_universe_signal_outcomes.parquet
arm_rankings_top50.parquet
arm_recall_daily.parquet
arm_top5_daily.parquet
arm_oracle_daily.parquet
signal_metrics_daily.parquet
arm_summary.json
pairwise_summary.json
regime_quarter_summary.parquet
audit_receipt.json
registry_record.json
resource_report.json
manifest.json
```

### 7.1 Complexity / capacity

- 决策日严格固定为 386；full-universe outcome 每日行数不超过冻结 PIT member count，预计总量约 200 万 instrument-date。
- 两腿 prediction 只按唯一键 `(trade_date, instrument)` 做 `one_to_one` inner join；outcome 只按唯一键 `(decision_as_of_trade_date, instrument)` join，不允许 many-to-many 或笛卡尔扩张。
- 行情、benchmark 与 suspend 以整个冻结区间批量读取；H20 outcome 只执行一次 386 日有界循环，不按 arm 重算，也不按股票发出 DB/文件请求。
- 三臂完整市场比较使用向量化 normalize/groupby；结果级 ranking 仅 `3 × 386 × 50=57,900` 行。完成 outcome 后主动释放原始行情，正式运行以阶段回执记录耗时与 peak RSS，并在 8GB 上限 fail closed。
- bootstrap 只消费按日聚合后的不超过 386 个值，固定 20 日 block、2000 次重复；季度/regime 不触发重新训练、重新选 arm 或重复全市场 outcome。

## 8. CLI

`scripts/advisory_strategy_package_alpha_audit.py`：

- `prepare --n1-request ... --n1-bundle ... --output-root ...`
- `run --request ...`
- `inspect --bundle ...`

`run` 只在 WSL `rdagent-gpu` 执行。`prepare` 与 `inspect` 可在 Windows 运行。CLI 不扫描 latest，不重训、不启动/停止服务、不触发 runtime activation。

## 9. Error contract

| reason_code | 含义 |
|---|---|
| `ADVISORY_ALPHA_AUDIT_REQUEST_INVALID` | request/arm/hash/date/权重非法 |
| `ADVISORY_ALPHA_AUDIT_WINDOW_FORBIDDEN` | 非 development window 或疑似 sealed 访问 |
| `ADVISORY_ALPHA_AUDIT_SOURCE_IDENTITY_MISMATCH` | N0/N1/PIT/prediction/policy identity 漂移 |
| `ADVISORY_ALPHA_AUDIT_ARM_CONTRACT_INVALID` | arm 数量、名称、腿或权重不是冻结三臂 |
| `ADVISORY_ALPHA_AUDIT_PREDICTION_INVALID` | 缺日期、重复、非有限 score 或共同覆盖不可审计 |
| `ADVISORY_ALPHA_AUDIT_PARENT_PARITY_FAILED` | 父包 arm 与 N1 formal ranking 不一致 |
| `ADVISORY_ALPHA_AUDIT_OUTCOME_INVALID` | H20 时钟、benchmark、PIT 或 typed outcome 不一致 |
| `ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT` | 同 request 的不可变 bundle 内容冲突 |
| `ADVISORY_ALPHA_AUDIT_MEMORY_LIMIT_EXCEEDED` | peak RSS 超过 8GB |
| `ADVISORY_MODEL_TRAINING_REQUIRES_WSL` | 正式 run 不在 WSL rdagent-gpu |

## 10. Implementation Plan / Implementation scope

允许修改：

```text
backend/services/advisory_model_first/alpha_signal_audit_contracts.py
backend/services/advisory_model_first/alpha_signal_audit_pipeline.py
backend/services/advisory_model_first/tier1_oracle_pipeline.py
backend/tests/advisory_model_first/test_alpha_signal_audit_contracts.py
backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py
scripts/advisory_strategy_package_alpha_audit.py
scripts/ci_change_classifier.py
backend/tests/scripts/test_ci_change_classifier.py
tests/aistock_validation/catalog/file_ownership.yaml
docs/architecture/advisory_strategy_package_three_arm_alpha_audit_f2_detailed_design_20260831.md
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
```

任何新增范围先更新本节，再编辑源码。

实施阶段：

1. 合同、source identity 与三臂拒绝测试；
2. full-universe outcome helper 与 N1 回归；
3. signal/policy/recall/oracle/pairwise 指标；
4. immutable bundle、exact retry、registry append；
5. CLI、CI classifier 与 ownership；
6. 单元/集成测试、多轮审核修复；
7. 绑定已提交 compute commit 后执行 WSL 正式诊断。

2026-08-31 checkpoint：阶段1-6已在task worktree实现；定向与模块门禁合计覆盖600项通过、16项跳过，L0无P0/P1阻断，F2 validator 15/15、ownership 11/11。阶段7尚未执行，因为正式request与compute都拒绝dirty worktree；提交/推送未获本轮授权，故没有三臂科学结果、registry新行或route刷新，不得把本地实现状态写成正式诊断完成。

## 11. Verification Plan

### 11.1 Contract / source / leakage

- request canonical hash、三臂 exact identity、额外 arm、改权重、descriptor drift、future-date poison；
- window authorization 必须早于任何 loader；sealed path/date/receipt poison 必须失败；
- Top25/Top50 不产生两个 arm，trial count 固定为 0。

### 11.2 PIT / outcome / parity

- canonical PIT membership、上市 252 session、ST/退市、停牌与行情缺失不静默删除；
- T/T+1/T+20、顺延、benchmark、成本与 fixed-slot cash 与 N1 完全相同；
- `IC_WEIGHTED_PARENT` 的 386×50 ranking 与 N1 formal artifact 逐行一致；
- full outcome panel 只构建一次并被三臂共享。

### 11.3 Metrics / inference

- Pearson/Spearman、MATURED 与 policy-known 样本边界、bucket、random recall lift；
- Top5/Top20 overlap、churn、组合相对两腿的配对 delta；
- regime 在 T 计算，季度仅描述；20 日 block bootstrap 固定 seed 可复现；
- 未来 outcome poison 不改变历史 score/ranking。

### 11.4 Delivery

- immutable bundle、文件 hash/readback、exact retry、冲突拒绝；
- registry 只追加一条 0-trial navigation record，失败不提前追加；
- request 与正式 compute 均拒绝 dirty worktree；registry 追加后路线页只刷新身份且主路线不变；
- peak RSS < 8GB，manifest runtime/activation/holdout flags 全为 false；
- F2 validator、定向 pytest、Ruff、compile、ownership、classifier、`git diff --check`。

## 12. Design Acceptance Index

| ID | Requirement |
|---|---|
| F-206 | 固定 LSTM/FUND/父包三臂；Top25/Top50 不冒充独立 Alpha |
| F-207 | N0/N1/PIT/prediction/policy identity 全量冻结并可读回 |
| F-208 | 三臂横向指标使用共同 PIT prediction intersection，同时报告各腿自身覆盖 |
| F-209 | 三臂共享一次构建的 full-universe H20 outcome，语义与 N1 完全一致 |
| F-210 | 完整报告 matured IC/RankIC、policy RankIC、ICIR、bucket 与 block CI |
| F-211 | 每臂报告 Top20/40/50 winner recall、随机 lift、Top5 与 oracle headroom |
| F-212 | 固定三组 pairwise delta、相关性、重叠与 churn，不事后选 arm |
| F-213 | 完整窗口为主结论；T 可见 regime 与季度 sensitivity 前置冻结 |
| F-214 | development-only、sealed loader 前拒绝、无训练/调参/激活 |
| F-215 | 父包 arm 与 N1 formal ranking 逐行 parity |
| F-216 | immutable bundle、content address、atomic publish、exact retry 与 readback |
| F-217 | registry 记录为 0-trial ORACLE_DIAGNOSTIC / NAVIGATION_ONLY |
| F-218 | CLI 显式路径、WSL rdagent-gpu 正式运行、peak RSS < 8GB |
| F-219 | 无 API/UI/DDL/DML/restart/runtime/Selection/StrategyPackage 状态影响 |
| F-220 | 源码依赖 #4014 单独保持；本任务不扩大或修改其 PR |

## 13. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-206 | `alpha_signal_audit_contracts.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_contracts.py` | LOCAL_IMPLEMENTED_REVIEW_PASS | none |
| F-207 | `alpha_signal_audit_pipeline.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py`；真实 N1 prepare smoke | LOCAL_REVIEW_PASS | none |
| F-208 | `alpha_signal_audit_pipeline.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py` common/own coverage | LOCAL_REVIEW_PASS | none |
| F-209 | `tier1_oracle_pipeline.py`; `alpha_signal_audit_pipeline.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py` N1 outcome回归 | LOCAL_REVIEW_PASS | none |
| F-210 | `alpha_signal_audit_pipeline.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py` IC/bucket/block tests | LOCAL_REVIEW_PASS | none |
| F-211 | `alpha_signal_audit_pipeline.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py` recall/Top5/oracle tests | LOCAL_REVIEW_PASS | none |
| F-212 | `alpha_signal_audit_pipeline.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py` pairwise/overlap/churn tests | LOCAL_REVIEW_PASS | none |
| F-213 | `alpha_signal_audit_contracts.py`; `alpha_signal_audit_pipeline.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py` regime/quarter/future poison | LOCAL_REVIEW_PASS | none |
| F-214 | `alpha_signal_audit_pipeline.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py` authorization/dirty/flags | LOCAL_REVIEW_PASS | none |
| F-215 | `alpha_signal_audit_pipeline.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py` parity正负测试 | LOCAL_REVIEW_PASS | none |
| F-216 | `alpha_signal_audit_pipeline.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py` immutable/exact retry | LOCAL_REVIEW_PASS | none |
| F-217 | `alpha_signal_audit_pipeline.py`; existing `research_control.py` | `backend/tests/advisory_model_first/test_research_trial_registry.py`；`backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py` | LOCAL_REVIEW_PASS | none |
| F-218 | `scripts/advisory_strategy_package_alpha_audit.py` | `backend/tests/advisory_model_first/test_alpha_signal_audit_pipeline.py`；artifact: WSL frozen spike console receipts | LOCAL_REVIEW_PASS | none |
| F-219 | no production surface | artifact: manifest false flags；`tmp/validation/guardrails/verify_skill_l0_paths.json` | VERIFIED_NOOP | none |
| F-220 | task-scoped stacked branch | artifact: merge-base=`1acecacb...`（PR #4014 head） | DEPENDENCY_READY | none |

## 14. Risks / failure modes

| 风险 | 影响 | 处理 |
|---|---|---|
| 不同包原生回测口径混入 | 虚假横向排名 | 只比较同一 window/PIT/outcome/cost；原生指标仅作 inventory |
| 合并包与单腿覆盖不同 | 组合被错误奖励或惩罚 | 主比较固定共同 intersection，另报 own coverage |
| Top25/Top50 重复计数 | 多重检验与伪多样性 | 相同 prediction identity 只算一个 arm |
| outcome join 泄漏排序 | 未来数据泄漏 | ranking 完成后才 join outcome；poison test |
| 停牌/缺失被删除 | Alpha 指标虚高 | typed status + coverage，policy 指标包含合法 cash |
| 最好季度被当作主结论 | 取样偏差 | 全窗口主结论，季度只描述，固定 regime/block |
| 高相关两腿被误当正交 | 组合价值夸大 | score/result correlation + paired marginal delta |
| 复用私有 N1 逻辑漂移 | outcome 口径分叉 | 暴露单一公共 helper，并保留 N1 回归 parity |
| #4014 未合入 | 新任务无法独立落主线 | 保持 stacked dependency；不修改 #4014，合入状态单独报告 |
| 诊断重新平台化 | 延误模型演进 | 仅两个模块、一个 CLI、JSON/Parquet bundle；无 UI/DB/scheduler |

## 15. Rollout / rollback

- rollout 只新增离线 research source、文档和 task artifact；不触碰运行时。
- 源码未合入前，结果最多是开发窗口 navigation evidence；不能声称生产交付。
- #4014 先于本任务合入；本任务后续在最新 main 上重放/重基，科学 request identity 不通过静默改写保持。
- rollback 为 revert source PR；不可变 artifact 与 registry 历史保留，以新 lineage 纠错，不覆盖旧结果。

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

1. 禁止简化交付：缺任一固定 arm、full-universe signal 指标、共同 coverage、pairwise delta、regime/block 统计或真实 bundle，不得称审计完成。
2. 禁止静默错误：缺腿、缺日期、重复、非有限 score、unknown outcome、identity drift 与统计欠覆盖均 typed 失败或显式状态，不返回伪零值。
3. 禁止改变业务逻辑：不改父包、权重、Selection、成本、PIT、持有期或其他运行时语义。
4. 禁止私增门禁：审计结果只导航，不新增人工审批、激活阈值或方向关闭规则；负结果与不确定结果均为合法输出。
