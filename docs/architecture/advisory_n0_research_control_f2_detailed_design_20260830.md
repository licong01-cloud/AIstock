# AIstock Advisory N0 最小研究控制面 F2 详细设计 v1.1

> 日期：2026-08-30
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.10
> 交付等级：F2
> 当前状态：`SOURCE_READY_CANDIDATE_VERIFIED_MERGE_PENDING`
> 业务模块：Advisory / Selection Center
> 生产影响：无数据库、无 API/UI、无 descriptor、无模型训练、无运行时激活、无后端重启

## 1. Background / 已核实事实

P0-D 至 P0-L 已在相同 P0-C 数据、Top20 候选、feature schema v2 和 CORE/CORE_HMM 家族上完成九轮自适应研究，且没有可激活 winner。父蓝图已冻结该研究族，并把 N0 定义为 N1 oracle/learnability 前的唯一前置任务。

截至本设计冻结时，目标父包事实为：

| 项目 | 已核实值 |
|---|---|
| package | `pkg_ma_8ec5e389fa2c5e484a1ac7e9` |
| manifest | `f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016` |
| Program / binding | `advp_3126dd77f9774d94850f37ad012f640f` / `advb_f860140caa314665ad60ac089ed84b3f` |
| runtime semantics | `advisory_multi_alpha_representative_terminal_top25_to20_v1` / `83fc0475...` |
| representative legs | `a1_plus3_LSTM_h20`、`new_FUNDGROWTH_h20` |
| common historical prediction cutoff | `2026-03-10` |
| runtime assets | 两腿均存在 package-owned model weight、factor entry、factor order 和 manifest |
| post-cutoff evidence | 冻结 H0 v6 在 `2026-05-15..2026-07-16` 产生 44 个非空父候选日；该窗口已消费，只作能力证据 |
| P0-C policy dataset | `81e2c9ba...`，386 decision days、28 READY CPCV paths |
| current production model | P0-D exact experimental shadow；不替换 Selection |

N0 只把已有事实变成最小、可机器校验的研究控制合同。它不产生模型、alpha、IC、收益判断或新的方向证据。

## 2. Scope

N0 交付四项能力：

1. `AdvisoryResearchTrialRegistryV1`：只追加 JSONL，登记实验身份、研究族、唯一变量、数据/policy 身份、trial 数、消费窗口、结果分类、decision use 和具体 evidence reference。
2. `AdvisoryResearchRouteV1`：只从 registry、父包 spike receipt 和 research-window contract 生成单页当前路线；不手工维护第二份状态。
3. `AdvisoryParentPredictionExtensionReceiptV1`：对目标父包执行只读三态判定。
4. `AdvisoryResearchWindowContractV1`：冻结 development/consumed windows 与未来 sealed holdout，并为后续命令提供 fail-closed 访问检查。

首次正式 N0 输出根固定为显式参数，计划使用：

```text
F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830/
  trial_registry.jsonl
  current_route.md
  parent_prediction_extension_receipt.json
  research_window_contract.json
  n0_completion_receipt.json
```

## 3. Non-goals

- 不训练、重训、加载或搜索任何 Advisory/QE 模型。
- 不生成、评价或筛选 alpha，不读取 IC/收益后调整搜索预算。
- 不执行 N1 oracle、learnability 或 winner 召回计算。
- 不读取 sealed holdout 内容，不把未来窗口加入 feature selection。
- 不新增数据库表、API、页面、审批、角色、调度器、ModelOps 或通用研究平台。
- 不修改 Selection、StrategyPackage、Paper、Simulation、QMT、descriptor 或生产 Program。
- 不复制既有 artifact，只保存 URI、SHA256、size 和最小身份。
- 不重算 P0-D 至 P0-L、M5、历史回放或 H0 的经济结果。

## 4. Architecture

```text
existing frozen manifests / reports / receipts
  -> verified evidence references
  -> append-only AdvisoryResearchTrialRegistryV1
  -> derived one-page AdvisoryResearchRouteV1

Prediction Store descriptors + exact package runtime assets
  + one frozen post-cutoff parent-candidate evidence
  -> ParentPredictionExtensionSpike
  -> FROZEN_MODEL_CAN_INFER
     | HISTORICAL_PREDICTION_ONLY
     | RETRAIN_NEW_LINEAGE_REQUIRED

development/consumed window declarations
  + future sealed holdout declaration
  -> AdvisoryResearchWindowContractV1
  -> authorize_access(study_type, dataset_identity, date_range, decision_use)
  -> allow or typed fail-closed rejection
```

所有输出均是离线 task artifact。N0 CLI 只消费显式路径，不扫描“latest”，不读取数据库，不启动服务。

## 5. Contracts

### 5.1 Registry record

每个 JSONL 行是一个不可变 `AdvisoryResearchTrialRecordV1`：

```text
registry_entry_id
experiment_id / attempt_id / research_stage
study_type
hypothesis_family_id / parent_lineage / unique_variable
objective_contract
dataset_identity / schema_identity / policy_identity
planned/generated/evaluated/selected trial counts
consumed_windows[]
result_class
decision_use
evidence_refs[] = artifact_uri + sha256 + size_bytes
recorded_at / record_sha256
```

固定枚举：

```text
study_type = ORACLE_DIAGNOSTIC | LEARNABILITY_AUDIT | EXPLORATORY_SCREEN
           | CANDIDATE_MODEL | CONFIRMATION | ACTIVATION
objective_contract = ALPHA_RANKING | RISK_MANAGED_ADVISORY
decision_use = NAVIGATION_ONLY | DIRECTION_GATE | ACTIVATION_EVIDENCE
result_class = EXPLORATORY | NEGATIVE | INCOMPLETE_NEGATIVE | FAMILY_FROZEN
             | CONTROL_READY | CONFIRMED | REJECTED | ACTIVATED
```

约束：

- `selected <= evaluated <= generated <= planned`。
- oracle/learnability 不得标记 `ACTIVATION_EVIDENCE`。
- ACTIVATION 必须引用 `ACTIVATION_EVIDENCE`，其它 study 不得伪造激活。
- 相同 `registry_entry_id` + 相同 hash 为幂等 no-op；相同 id 不同内容 typed conflict。
- 同一 `experiment_id` 的 family、objective、dataset/schema/policy identity 不得漂移。
- registry 中任意一行损坏、重复冲突或 hash 不一致时，全量读取 fail closed。

### 5.2 Append semantics

- 同一进程内使用独占锁完成 read-validate-append。
- 一批 record 全部验证后一次追加；任一失败时零追加。
- 写入使用 UTF-8 canonical JSON、单行、尾换行和 fsync。
- 不提供 update/delete/rewrite/compact API。
- route 读取损坏 registry 时失败，不使用最后一条成功行静默继续。

### 5.3 P0-family backfill

版本化 seed spec 只列相对 artifact 路径和已冻结研究事实。bootstrap 时对每个 evidence 文件重新计算 SHA256/size 后生成 registry record。

首批必须覆盖：

- P0-D 至 P0-L 九个模型实验；
- 已消费 M1/M5 80 日 test；
- P0-D/P0-E 24 决策日历史回放；
- H0 v6 44 日窗口；
- N0 parent spike + window contract 的 `CONTROL_READY` 记录。

所有旧记录固定为 `NAVIGATION_ONLY`；它们不能成为 N1 激活证据。

### 5.4 Parent prediction extension

三态判定固定为：

| 状态 | 必要证据 |
|---|---|
| `FROZEN_MODEL_CAN_INFER` | exact package/manifest/runtime semantics 一致；两腿 runtime model/factor assets 齐全且 model SHA 匹配；代表 seed Prediction Store descriptor 齐全；存在共同历史 cutoff 之后、同 package/manifest/runtime semantics 的非空父候选 evidence |
| `HISTORICAL_PREDICTION_ONLY` | 历史 prediction descriptor 有效，但 runtime assets 或 post-cutoff executable evidence 不完整；不外推最后一日 prediction |
| `RETRAIN_NEW_LINEAGE_REQUIRED` | 只有显式 typed receipt 证明冻结模型/schema 与目标窗口不兼容并声明必须形成新模型 identity 时成立；不得根据文件缺失猜测重训 |

spike 不重新执行推理；它验证现有冻结 post-cutoff inference evidence。receipt 同时报告：common prediction cutoff、target extension range、每腿资产 hash/count/bytes、evidence decision date、既有 observed duration 和扫描耗时。

### 5.5 Research windows and sealed holdout

首次 contract 冻结：

| window | 日期 | 状态/用途 |
|---|---|---|
| `P0C_DEVELOPMENT_V1` | `2024-07-04..2026-03-10` | 已消费 development/CPCV |
| `M1_M5_FROZEN_TEST_V1` | `2025-11-07..2026-03-10` | 已消费 80 日 test；不得重新称 OOS |
| `HISTORICAL_REPLAY_V1` | `2026-05-15..2026-07-16` | 已消费 44 日 context / 24 matured decisions |
| `ADVISORY_SEALED_HOLDOUT_2026Q4_V1` | `2026-08-31..2026-11-30` | prospective-only、未打开；只允许主线完全冻结后一次 confirmation |

sealed holdout identity 绑定 package、manifest、runtime semantics、baseline/shadow/cost policy hash、日期、source policy、artifact root和唯一`sealed_holdout_consumption_receipt.json` URI。日期范围可包含非交易日边界，但实际 eligible calendar 必须由后续 confirmation request 固定，不能结果后缩放窗口；换一个receipt路径不能绕过一次消费。

访问矩阵：

| study_type | development/consumed | sealed holdout |
|---|---:|---:|
| ORACLE_DIAGNOSTIC | allow | deny |
| LEARNABILITY_AUDIT | allow | deny |
| EXPLORATORY_SCREEN | allow | deny |
| CANDIDATE_MODEL | allow | deny |
| CONFIRMATION | deny consumed selection windows；allow exact sealed identity once | allow exact identity once |
| ACTIVATION | 只读已确认 prospective evidence | 不直接消费未确认 holdout |

confirmation 首次授权后必须在contract绑定的canonical URI写 consume receipt；并发请求只能一个成功，第二次访问、同 frontier 回选、替换receipt路径或不同 objective/policy identity 均拒绝。N0 本身只生成未消费 contract，不读取 holdout。

## 6. CLI

唯一入口：`scripts/advisory_n0_research_control.py`。

```text
bootstrap-registry
parent-spike
freeze-windows
check-window-access
generate-route
complete-n0
```

每个命令：

- 参数路径显式；禁止 cwd/latest 隐式发现。
- stdout 只输出单个 JSON summary。
- 失败返回非零退出码和 typed reason code。
- 输出 JSON 使用 atomic replace；registry 例外地使用 locked append-only。
- 不捕获异常后返回成功，不产生空 route/receipt。

## 7. Error contract

| reason_code | 语义 |
|---|---|
| `ADVISORY_RESEARCH_REGISTRY_INVALID` | JSONL、schema 或 hash 损坏 |
| `ADVISORY_RESEARCH_REGISTRY_CONFLICT` | append-only id/identity 冲突 |
| `ADVISORY_RESEARCH_EVIDENCE_MISSING` | evidence 文件不存在或 hash/size 无法读取 |
| `ADVISORY_PARENT_PREDICTION_IDENTITY_MISMATCH` | package/manifest/semantics/leg identity 漂移 |
| `ADVISORY_PARENT_RUNTIME_ASSET_INVALID` | runtime manifest/model/factor 资产不完整或 model hash 不符 |
| `ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID` | post-cutoff evidence 不足或日期/身份不符 |
| `ADVISORY_RESEARCH_WINDOW_CONFLICT` | development/holdout identity 或日期重叠冲突 |
| `ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED` | oracle/learnability/exploration/candidate 访问 sealed holdout |
| `ADVISORY_SEALED_HOLDOUT_ALREADY_CONSUMED` | confirmation 重复消费 |
| `ADVISORY_RESEARCH_ROUTE_INCONSISTENT` | registry 导出互斥状态或缺少 N0 完成证据 |

## 8. Implementation Plan / Implementation scope

实施顺序固定为：先实现纯契约与严格 JSONL registry；再实现父包三态 spike 和窗口访问守卫；随后实现派生 route/CLI；最后只读运行正式 N0、回写真实 receipt 与父蓝图状态。任何阶段不得因后续命令需要而扩大到模型训练、数据库、服务进程或运行时激活。

允许修改：

```text
backend/services/advisory_model_first/research_control_contracts.py
backend/services/advisory_model_first/research_control.py
backend/services/advisory_model_first/research_control_seed_v1.json
backend/tests/advisory_model_first/test_research_control_contracts.py
backend/tests/advisory_model_first/test_research_trial_registry.py
backend/tests/advisory_model_first/test_parent_prediction_extension.py
backend/tests/advisory_model_first/test_research_window_guard.py
backend/tests/advisory_model_first/test_research_control_cli.py
scripts/advisory_n0_research_control.py
tests/aistock_validation/catalog/file_ownership.yaml
docs/architecture/advisory_n0_research_control_f2_detailed_design_20260830.md
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
```

新增范围必须先回写本设计再修改。

## 9. Verification Plan

### 9.1 Contracts and registry

- 全枚举、count 单调、identity/hash、evidence ref 和跨 event identity 验证。
- exact duplicate no-op、same-id conflict、batch all-or-nothing、损坏末行 fail closed。
- 并发 append 不丢行、不产生交错 JSON。
- NAVIGATION_ONLY 不能被 activation 引用。

### 9.2 Parent spike

- 两腿完整 + post-cutoff evidence -> `FROZEN_MODEL_CAN_INFER`。
- prediction 存在但删除 runtime asset -> `HISTORICAL_PREDICTION_ONLY`。
- 仅显式 retrain-required receipt -> `RETRAIN_NEW_LINEAGE_REQUIRED`。
- model/factor hash、package、manifest、runtime semantics、日期或候选数毒化均 typed fail/降级，不返回伪 READY。
- 正式 target receipt 对真实 Prediction Store、runtime assets 和 H0 evidence 运行并 exact retry 同 hash。

### 9.3 Window guard

- oracle、learnability、exploration 和 candidate 对 sealed identity/date overlap 全部拒绝。
- 非 holdout development 请求允许。
- confirmation 只有 exact dataset/objective/policy identity 允许一次；第二次拒绝。
- 日期包含关系、边界相交、identity 漂移和 malformed receipt 均 fail closed。

### 9.4 Route and delivery

- route 仅由 registry + 两份 N0 receipt 派生；手改 route 不反写权威。
- P0 family 显示 frozen、N0 complete、N1 next、无 active main/aux model line。
- route 不显示任何新收益、winner 或 activation。
- F2 validator、定向 pytest、Ruff、compile、ownership/guardrail、`git diff --check` 通过。

## 10. Implementation Facts / 当前实现事实

截至 2026-08-30，本设计范围内的契约、只追加 registry、父包三态 spike、window guard、派生 route 和 CLI 已完成源码实现；第二轮审核补齐完整P0-C/policy-set identity、延伸区间约束、canonical-path并发consume-once、attempt-stage唯一性和navigation/activation证据隔离后，50 个定向测试与 Ruff 通过。真实资产候选贯通使用独立预合入根：

```text
F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_candidate_premerge_v3_20260830/
```

候选贯通只读核验了 P0-D 至 P0-L、M1/M5、P0-D/P0-E replay、H0、目标 Prediction Store 与 package-owned runtime assets；没有读取 sealed holdout，没有训练或重算经济结果。候选结果为：

| 项目 | 候选事实 |
|---|---|
| registry | 13 条历史记录 + 1 条 N0 control；P0-C绑定完整bundle/policy-set identity；文件 SHA256 `3642ca01...` |
| parent spike | `FROZEN_MODEL_CAN_INFER`；semantic SHA256 `760edac9...` |
| window contract | semantic SHA256 `4f493e7d...`；future holdout仍为`SEALED_UNCONSUMED`；consume-once receipt URI绑定候选root |
| route | P0 family frozen、N0 candidate complete、active main/aux均为NONE、next=N1 |
| candidate completion | receipt SHA256 `6dee6744...`；连续 exact retry 返回同 identity |

这些是预合入候选证据，不冒充合入后的正式 N0 receipt。正式根仍按§2固定，必须由最终 merge commit 的源码在合入后生成；N1只能引用 merge commit与正式 receipt，不能把本候选根当作激活或方向证据。

## 11. Design Acceptance Index

| ID | 要求 |
|---|---|
| F-001 | N0 仅实现 registry、route、parent spike 和 window guard，不生成模型/alpha/经济证据 |
| F-002 | registry 为只追加 JSONL，不提供 update/delete/rewrite/compact |
| F-003 | record 的 study/objective/decision/result 枚举和 trial count 单调性 fail closed |
| F-004 | record hash、entry id 和同 experiment identity 不可漂移 |
| F-005 | batch append 全量预验证、独占锁、单次追加和 fsync |
| F-006 | exact duplicate 幂等；same-id different-content 冲突 |
| F-007 | 损坏/截断 JSONL 读取失败，不忽略尾行 |
| F-008 | evidence 只保存 URI/hash/size，bootstrap 验证真实文件且不复制 artifact |
| F-009 | 首批 backfill 覆盖 P0-D..P0-L、80 日 test、24 日 replay 和 44 日 H0 |
| F-010 | 旧研究记录全部 NAVIGATION_ONLY，不能成为 activation evidence |
| F-011 | route 只从 registry/spike/window receipt 派生且不形成第二权威 |
| F-012 | route 固定显示 P0 family frozen、N0 complete、N1 next，无互斥 active line |
| F-013 | parent spike 精确绑定 target package/manifest/runtime semantics/representative legs |
| F-014 | parent spike 验证 Prediction Store common cutoff 和每腿 runtime model/factor assets |
| F-015 | `FROZEN_MODEL_CAN_INFER` 必须有 post-cutoff 非空父候选执行证据 |
| F-016 | `HISTORICAL_PREDICTION_ONLY` 不外推预测；`RETRAIN_NEW_LINEAGE_REQUIRED` 只接受显式 typed receipt |
| F-017 | parent receipt 报告资源、日期、文件/hash/count 并支持 exact retry |
| F-018 | window contract 冻结三个 consumed windows 和独立 future sealed holdout |
| F-019 | oracle/learnability/exploration/candidate 对 sealed holdout fail closed |
| F-020 | confirmation 仅 exact identity 一次消费；重复/回选拒绝 |
| F-021 | CLI 只用显式路径、typed failure、atomic JSON 和单 JSON summary |
| F-022 | 不修改 DB/API/UI/runtime/descriptor/Selection/StrategyPackage/Paper/Simulation/QMT |
| F-023 | 正式 N0 receipt 使用真实目标资产，输出根位于 repo 外 artifact store |
| F-024 | 父蓝图回写 N0 真实实现/receipt 状态，但不提前标记 N1 完成 |

## 12. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/advisory_model_first/research_control.py`; explicit N0 command surface | `backend/tests/advisory_model_first/test_research_control_contracts.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-002 | `AdvisoryResearchTrialRegistryV1` | `backend/tests/advisory_model_first/test_research_trial_registry.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-003 | `research_control_contracts.py` typed enums/count rules | `backend/tests/advisory_model_first/test_research_control_contracts.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-004 | record functional payload/hash/id and experiment invariants | `backend/tests/advisory_model_first/test_research_control_contracts.py`; `backend/tests/advisory_model_first/test_research_trial_registry.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-005 | cross-platform locked batch append + fsync | `backend/tests/advisory_model_first/test_research_trial_registry.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-006 | exact duplicate no-op / drift conflict | `backend/tests/advisory_model_first/test_research_trial_registry.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-007 | strict UTF-8/newline/schema JSONL reader | `backend/tests/advisory_model_first/test_research_trial_registry.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-008 | root-contained evidence resolver + SHA256/size | `backend/tests/advisory_model_first/test_research_trial_registry.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-009 | `backend/services/advisory_model_first/research_control_seed_v1.json` | artifact: candidate `trial_registry.jsonl` 14 lines | IMPLEMENTED_CANDIDATE_VERIFIED | none |
| F-010 | study/decision-use activation isolation | `backend/tests/advisory_model_first/test_research_control_contracts.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-011 | `generate_current_route` | `backend/tests/advisory_model_first/test_research_control_cli.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-012 | frozen-P0/N0/active-line route assertions | artifact: candidate `current_route.md` SHA256 `5667a708...` | IMPLEMENTED_CANDIDATE_VERIFIED | none |
| F-013 | exact target package/manifest/runtime semantics/legs | `backend/tests/advisory_model_first/test_parent_prediction_extension.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-014 | `ExactPredictionSource` descriptors + package-owned runtime scanner | `backend/tests/advisory_model_first/test_parent_prediction_extension.py` | IMPLEMENTED_REAL_ASSET_CANDIDATE_VERIFIED | none |
| F-015 | post-cutoff candidate/state identity and non-empty proof | `backend/tests/advisory_model_first/test_parent_prediction_extension.py` | IMPLEMENTED_REAL_ASSET_CANDIDATE_VERIFIED | none |
| F-016 | three-state classifier + explicit retrain receipt | `backend/tests/advisory_model_first/test_parent_prediction_extension.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-017 | immutable parent receipt/exact retry/resource refs | artifact: candidate parent semantic SHA256 `760edac9...` | IMPLEMENTED_CANDIDATE_VERIFIED | none |
| F-018 | fixed consumed/sealed window contract + canonical consumption URI | `backend/tests/advisory_model_first/test_research_window_guard.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-019 | exact declared-window access + sealed deny matrix | `backend/tests/advisory_model_first/test_research_window_guard.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-020 | exact confirmation identity + canonical-path/concurrent consume-once rejection | `backend/tests/advisory_model_first/test_research_window_guard.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-021 | `scripts/advisory_n0_research_control.py` six subcommands | `backend/tests/advisory_model_first/test_research_control_cli.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-022 | ownership catalog + no DB/API/UI/runtime imports | `backend/tests/advisory_model_first/test_research_control_cli.py`; validation-receipt: changed-file ownership gate | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-023 | canonical post-merge formal output root | artifact: premerge candidate `n0_completion_receipt.json` SHA256 `6dee6744...` | IMPLEMENTED_CANDIDATE_VERIFIED | approved_by_user: canonical formal rollout is a separate post-merge state and is not claimed by the premerge candidate receipt |
| F-024 | parent blueprint §1/§9/§11/§16 | validation-receipt: F2 parent/child validator receipts | BLUEPRINT_UPDATED_VERIFIED | none |

## 13. Risks / failure modes

- **治理膨胀**：N0 只保留文件级契约与 CLI，不新增 UI、数据库、审批或通用平台。
- **复杂度误判或无界增长**：初始bootstrap严格固定13行，append/strict-read为registry行数与字节数线性复杂度且无行情join；route是固定17行投影。N0不加载候选矩阵、市场表或模型，不引入高维join/排序。
- **hindsight oracle 被误当可学习空间**：N0 不产生 oracle 结果，registry 已把 `ORACLE_DIAGNOSTIC` 与激活证据隔离。
- **sealed holdout 污染**：窗口守卫按 study、identity、日期和 consume receipt fail closed；N0 正式运行不得读取 holdout 内容。
- **route 状态漂移**：route 永远由 registry 和 receipt 重建，手工文件不是权威。
- **父包能力误判**：`FROZEN_MODEL_CAN_INFER` 必须同时满足 runtime assets 与共同 cutoff 后非空父候选证据；缺件只能降级或显式失败。

## 14. Rollout / rollback

Rollout：设计与源码合入后，离线执行正式 N0 CLI，生成不可变 receipt；不需要后端重启。N1 只能引用该 merge commit 和 receipt hash。

Rollback：停止使用未合入源码或指定 output root；registry/receipt 保留只读，不删除、不重写。回滚不影响生产模型、Program 或 Selection。

## 15. Production gates

```text
production_ddl_gate = noop
production_dml_gate = noop
production_backend_dependency_gate = noop
production_frontend_dependency_gate = noop
runtime_activation = noop
backend_restart = noop
```

## 16. DESIGN-COMPLIANCE-001

1. **禁止简化交付：通过。** registry/route/spike/window/CLI均为真实实现；真实目标资产候选贯通和exact retry成功，不用静态Markdown、mock或空receipt冒充交付。
2. **禁止静默错误：通过。** JSONL截断、identity/hash漂移、post-cutoff毒化、非法decision use、sealed访问/并发重复消费和CLI参数错误均返回typed nonzero failure；不存在默认READY或吞错分支。
3. **禁止改变业务逻辑：通过。** changed scope仅新增离线研究控制文件、测试、设计和ownership映射；未修改Selection、StrategyPackage、Advisory runtime、descriptor、DB/API/UI、模型或经济合同，sealed holdout内容未读。
4. **禁止私增门禁审批：通过。** 未新增角色、审批、数据库、服务或通用平台；post-merge正式receipt只是本设计既有rollout状态边界，不构成额外人工门禁，且N0全程无需后端重启/DDL/DML。
