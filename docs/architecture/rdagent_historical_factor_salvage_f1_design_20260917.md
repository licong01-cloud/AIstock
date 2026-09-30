# RD-Agent 历史因子抢救 F1 详细设计

> Feature tier：`F1`。本功能在现有因子研究模块内增加一个只读、离线的历史候选清点与去重入口；不建立新平台、调度器、数据库表或自动晋升链。

## 1. Background / 背景与目标

RD-Agent 的历史因子分散在本机 WSL、node1 和 AIstock 中央导出目录。既有同步只覆盖已导出的最终因子，可能遗漏运行失败、未成为 SOTA 或未同步的历史候选。本任务先完成 dry-run：统一清点来源、跨节点去重、与正式因子目录只读比对，并输出可复核的待评价清单。只有用户确认清单后，才可在独立任务中执行正式评价或入库。

## 2. 范围与边界

### 2.1 输入

- 本机 WSL：`/mnt/f/Dev/RD-Agent-main/log`，pickle 仅由 `rdagent-gpu` Python 3.10 在 WSL 内解析。
- node1：`/home/lc999/rdagent_state/log`，pickle 仅由 node1 的 `rdagent-gpu` Python 3.10 在节点内解析。
- 中央导出：`F:/Dev/AIstock/rdagent_assets/rdagent_tasks`，只读取已导出的文本因子和 manifest。
- 正式因子库：只读查询目录名称、代码和来源身份；不得写入目录、指标或相关性表。

节点清单通过显式 JSON 配置传入。控制端只接收节点本地提取后的 JSONL，不复制 pickle，不在 Windows 反序列化 pickle。节点不可达时以 typed unavailable 记录，不将缺失节点静默视为零候选。

### 2.2 输出

输出仅写入显式指定的 repo-external artifact root：

- `source_inventory.jsonl`：每个来源对象及 provenance；
- `candidate_groups.jsonl`：确定性去重组；
- `dry_run_summary.json`：分母闭合、来源状态与去重统计；
- `review_candidates.jsonl`：尚未在正式目录出现、可进入后续技术验证的候选；
- `unavailable.jsonl`：不可读取、语法异常或身份冲突等 typed reason。

产物采用新目录写入；目标目录非空时 loud fail，不覆盖既有报告。

## 3. Non-goals / 非目标

- 不运行完整因子指标、相关性或 QE 实验；本 F1 的首个交付点是 dry-run 清单。
- 不保存正式因子，不写 `aistock_factor_catalog`、`aistock_factor_metrics` 或 `qe_factor_correlations`。
- 不删除、移动或修改任何 RD-Agent 日志、中央导出资产或历史任务。
- 不修改 RD-Agent、QE、数仓、评级引擎、后端 API 或 UI。
- 不启动、停止或重启 WSL、node1、backend、worker 或 scheduler。
- 不新增审批门禁；清单确认只是本任务与正式交付之间的授权边界。

## 4. 统一身份与去重合同

每条来源记录至少包含：`source_node`、`task_id`、`loop_id`、`workspace_key`、`factor_name`、`code_text`、`code_sha256`、`ast_sha256`、`source_kind`、`source_path`。

去重分为三层，均保持完整 provenance：

1. `exact_code`：UTF-8 换行与尾随空白规范化后的代码 hash 相同；
2. `ast_equivalent`：Python AST 去除位置属性后的 canonical dump 相同；
3. `name_conflict`：规范化名称相同但代码/AST 不同，不得覆盖或任意选一，全部进入人工复核。

AST 解析失败不执行源码，记录 `python_ast_invalid`。不同文件名但相同 AST 可以合并为一组；同名不同 AST 必须保留为多个 candidate identity。算法不依赖输入顺序。

中央导出中的 `factor_order.json` 只用于标注当时动态因子是否被消费，不把非 current 因子判为无价值。模型源码、配置脚本和不含 `calculate_*` 入口的 Python 文件不进入因子分母。

## 5. 正式目录只读比对

比对次序：正式目录 exact code hash、AST hash、规范化名称。结果类型：

- `catalog_exact_match`：代码或 AST 与正式目录一致；
- `catalog_name_conflict`：名称一致但实现不同；
- `catalog_absent`：未发现对应正式因子；
- `catalog_unavailable`：无法获得只读目录快照。

名称命中不能代替实现比对；目录不可读时不得把所有候选判为 absent。目录快照可由只读 SQL 生成，也可用事先导出的 JSON 输入；本任务不创建数据库连接的第二套配置或 writer。

## 6. 分母闭合与失败语义

对每个发现的 factor source object，最终必须且只能落入：`grouped` 或 `unavailable`。摘要满足：

`source_objects = grouped_members + unavailable`

对去重后的组满足：

`unique_groups = catalog_exact_match + catalog_name_conflict + catalog_absent + catalog_unavailable`

已知 typed reasons 至少包括：`source_node_unreachable`、`source_root_missing`、`node_extractor_failed`、`python_ast_invalid`、`factor_entry_missing`、`source_record_invalid`、`catalog_snapshot_unavailable`。不得用空列表替代错误。

## 7. Implementation Plan / 实施方案

复用 `scripts/factor_research.py`，增加独立的 `salvage` 子命令，避免新增第二个因子研究 CLI：

```text
python scripts/factor_research.py salvage \
  --input <spec.json> \
  --artifact-root <new-empty-directory> \
  --format summary
```

`salvage` 不要求 `--env-file/--target`；目录快照由输入文件显式提供，确保命令本身没有生产数据库写入能力。节点提取器只执行路径枚举和既有 pickle 对象的字段读取；不 import 或运行候选代码。

为控制资源：逐任务提取、逐行写 JSONL、单任务完成后释放对象；不把所有 analyzer 大报告留在内存，不做全矩阵相关性，不运行模型训练。

## 8. 后续评价与停止点

dry-run 后必须停止并向用户报告清单。后续只有经用户确认的候选才进入既有 factor-research 流程：

1. 技术验证只检查 AST、入口、字段依赖、输出形状和有限值，不据小样本判定价值；
2. 技术有效且非 exact duplicate 的候选，在 active canonical PIT 与当前数据集上做完整历史评价；
3. 评价覆盖 2018-08 至当前 cutoff，并优先单列 2024 至今、最近年度和最近月份；
4. 计算正式独立指标和与存量库的相关性后，再给出抢救/拒绝建议；
5. 正式保存、官方指标写入和晋升仍需单独授权。

## 9. Design Acceptance Index

| item | requirement |
| --- | --- |
| F-001 | 同时支持 WSL、node1 与中央导出三个来源，节点 pickle 只在源节点 `rdagent-gpu` 中解析。 |
| F-002 | 每条候选保留稳定 provenance；提取失败 typed unavailable，不静默缩小来源分母。 |
| F-003 | exact code、AST equivalent、name conflict 三层确定性去重与输入顺序无关。 |
| F-004 | 只把带 `calculate_*` 因子入口的源码纳入分母，不执行候选代码。 |
| F-005 | 与正式目录按 code/AST/name 只读比对；name conflict 不覆盖，catalog 不可用不伪装 absent。 |
| F-006 | 两级 denominator closure 可机器验证。 |
| F-007 | 仅写显式 repo-external 新目录；目标非空 loud fail，不改源日志、中央资产或因子库。 |
| F-008 | 逐任务、流式处理；不保留 analyzer 全报告，不执行训练或全矩阵计算。 |
| F-009 | dry-run 输出 review 清单后停止；正式评价和入库不在本次自动执行范围。 |
| F-010 | 不新增 DDL、依赖、runtime、服务控制、生产 DML 或业务门禁。 |

## 10. Verification Plan / 验证方案

- 纯函数测试：换行规范化、AST canonicalization、严格单调输入顺序置换、exact/AST/name-conflict 分组。
- 解析测试：有效 factor、模型 Python、语法错误、缺少入口、恶意顶层代码（确认仅 AST 解析、不执行）。
- closure 测试：来源对象与组级分母等式；节点失败仍闭合。
- 写入测试：新空目录成功，非空目录失败，源文件 mtime/hash 不变。
- catalog 测试：exact、name conflict、absent、snapshot unavailable 四种结果。
- live dry-run：三个来源只读清点，输出紧凑摘要；不运行正式指标。
- 静态验证：changed Python `ruff`/`py_compile`、`git diff --check`、`validation_module_registry_l0`、`l0`、F1 validator。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | `backend/services/factor_research/rdagent_salvage.py` | `backend/tests/factor_research/test_rdagent_salvage.py`; artifact: `dry_run_summary.json.source_status` | PASS | none |
| F-002 | 同上 | `backend/tests/factor_research/test_rdagent_salvage.py`; artifact: `unavailable.jsonl` | PASS | none |
| F-003 | 同上 | `backend/tests/factor_research/test_rdagent_salvage.py::test_grouping_is_order_invariant_and_preserves_name_conflict` | PASS | none |
| F-004 | 同上 | `backend/tests/factor_research/test_rdagent_salvage.py::test_inspection_never_executes_candidate_code` | PASS | none |
| F-005 | 同上 | `backend/tests/factor_research/test_rdagent_salvage.py::test_catalog_classification_separates_exact_conflict_absent_and_unavailable` | PASS | none |
| F-006 | 同上 | `backend/tests/factor_research/test_rdagent_salvage.py::test_run_salvage_closes_denominators_and_refuses_overwrite` | PASS | none |
| F-007 | `scripts/factor_research.py` | `backend/tests/factor_research/test_rdagent_salvage.py`; artifact: `X:/AIstock_factor_research/rdagent_salvage/20260917-dry-run-1/artifacts-v7` | PASS | none |
| F-008 | service/CLI | artifact: fresh-process dry-runs `artifacts-v6`/`artifacts-v7` 五文件 SHA256 逐一相同；代码审查确认 line-streamed node output 和 timeout/cancel 生命周期 | PASS | none |
| F-009 | CLI summary | artifact: `review_candidates.jsonl`; `factor_library_writes=0` | PASS | none |
| F-010 | final changed-file/runtime audit | `python -m nox -s validation_module_registry_l0`; `python -m nox -s l0` | PASS | none |
| F-011 | `prepare_salvage_evaluation` | `backend/tests/factor_research/test_rdagent_salvage.py`; artifact: `X:/AIstock_factor_research/rdagent_salvage/20260917-evaluation-prep-v2/evaluation_prep_summary.json` | PASS | none |
| F-012 | `evaluation_name`; adapter identity readback | `backend/tests/factor_research/test_rdagent_salvage.py::test_prepare_salvage_evaluation_materializes_audited_candidate` | PASS | none |
| F-013 | `audit_legacy_factor_code` | `backend/tests/factor_research/test_rdagent_salvage.py::test_legacy_audit_accepts_read_only_contract_and_rejects_side_effects` | PASS | none |
| F-014 | `LEGACY_ADAPTER_TEMPLATE` | `backend/tests/factor_research/test_rdagent_salvage.py::test_rendered_legacy_adapter_respects_scope_and_does_not_mutate_input` | PASS | none |
| F-015 | `run_salvage_precheck` | `backend/tests/factor_research/test_rdagent_salvage.py::test_salvage_precheck_executes_bounded_scope_without_value_conclusion`; artifact: `X:/AIstock_factor_research/rdagent_salvage/20260917-evaluation-precheck-v1/precheck_summary.json` | PASS | none |
| F-016 | existing `compute_candidate_metrics` and `CorrelationEngine.compute_selected_daily_submatrix` | `python -m nox -s factor_research_backend` | PASS | none |
| F-017 | `candidate_ids` selection and multi-root identity checks | `backend/tests/factor_research/test_rdagent_salvage.py::test_salvage_precheck_rejects_unknown_candidate_selection` | PASS | none |
| F-018 | `run_salvage_metric_evaluation` | `backend/tests/factor_research/test_rdagent_salvage.py::test_salvage_metric_evaluation_reuses_context_and_closes_denominator` | PASS | none |
| F-019 | `run_salvage_reference_correlation` | `backend/tests/factor_research/test_rdagent_salvage.py::test_salvage_reference_correlation_is_bounded_and_closes_pairs` | PASS | none |
| F-020 | AST-suffixed `evaluation_name`; catalog status retained in metric result | `backend/tests/factor_research/test_rdagent_salvage.py` | PASS | none |
| F-021 | existing candidate-pair path in `compute_correlation_views` applied only to resulting shortlist | `backend/tests/factor_research/test_full_evaluation.py::test_full_evaluation_computes_only_requested_candidate_pairs` | PASS | none |

## 12. Production Gates

- `production_ddl_gate=noop`
- `production_dml_gate=noop`
- `dependency_install=noop`
- `factor_catalog_write_count=0`
- `official_metric_write_count=0`
- `runtime_impact=none`
- `process_control=false`

## 12.1 Risks / Failure Modes / 风险

- pickle 是 RD-Agent 自身可信运行产物，但只能在生成节点匹配的 `rdagent-gpu` 环境中读取；节点缺依赖或对象版本不匹配时输出 `node_extractor_failed`，不得复制到 Windows 强行解析。
- AST 相同只能证明源码结构相同，不证明市场价值；AST 不同也不证明信息独立。去重结果仅用于避免完全重复评价，不能代替正式指标与相关性。
- 同名不同实现可能是修复版或退化版，必须保留冲突 provenance；不得按时间、节点或输入顺序任意覆盖。
- 中央导出的历史文本可能包含乱码注释，但只要 Python AST 和计算入口有效即可进入技术候选；乱码本身不作为价值结论。
- 节点当前为零因子仍须在 `source_status` 中出现，避免把“已检查为空”与“根本未检查”混淆。

## 13. DESIGN-COMPLIANCE-001 预审

| 检查项 | 设计结论 |
| --- | --- |
| 禁止简化版、子集或 POC 冒充完整交付 | 三来源、分母闭合与 typed unavailable 均纳入 F-001～F-006；单节点成功不能冒充全量清点完成。 |
| 禁止静默错误或伪成功 | 节点/目录/AST 失败显式记录；catalog 不可读不判 absent。 |
| 禁止未经确认改变业务语义 | 仅生成研究候选清单，不改变目录、评级、QE 准入或研究价值结论。 |
| 禁止私增门禁或审批 | 没有生产门禁；停止点是授权边界，不改变任何运行时准入。 |

## 14. 用户确认后的评价准备增补（2026-09-17）

用户确认 dry-run 清单后，允许继续执行第 8 节的技术验证与完整评价准备。本增补不把原 dry-run 追溯改写为已完成评价，也不扩大到正式因子入库。

### 14.1 遗留脚本适配

历史候选均为零参数 `calculate_*()` 脚本，固定读取同目录数据文件并写 `result.h5`，不能直接满足当前 runner 的显式参数合同。评价准备复用 `factor_research.py`，增加两个离线子命令：

- `salvage-prepare`：从已完成清单读取候选代码，静态审计后在 repo 外生成稳定适配脚本；不执行候选。
- `salvage-precheck`：在显式股票与连续日期范围内顺序执行已审计适配脚本，只检查字段、公式执行、输出 schema、有限值与范围，不计算价值指标。

适配器不改候选计算区。它把已有数据文件按原文件名只读呈现给旧脚本，并在读取器层施加调用者声明的日期/股票范围；候选仍只允许写本次目录的 `result.h5`，随后规范为当前 runner 所需的单列 `MultiIndex(datetime,instrument)` 结果。评价名由规范化历史名称与 AST identity 共同生成，不用同名覆盖。

静态审计只接受：单一零参数 `calculate_*` 入口、`pandas/numpy/os/scipy` 既有计算依赖、已知 factor H5/parquet/schema 输入、唯一 `result.h5` 输出以及纯 `__main__` 调用。网络、数据库、进程、动态执行、任意文件读写、未知输入路径或额外输出一律记 typed unavailable，不运行。该审计是执行安全与可复用性检查，不是 alpha 门禁。

### 14.2 执行和结论边界

- 技术预检可以使用少量历史 PIT 股票和连续窗口，但只能给 `available/unavailable` 技术结论。
- 技术有效候选的价值结论仍必须复用现有 `full_evaluation`：全目标 PIT 股票池、2018-08 至 active profile cutoff、2024 至今优先窗口、正式独立指标及候选×存量相关。
- 预检输出与候选值只写新建 repo 外目录；不得写官方指标、相关性、目录或 `is_available`。
- 不重导数据集，不修改 active profile，不复制第二套指标公式，不执行 QE 实验。

### 14.3 增补验收

| item | requirement |
| --- | --- |
| F-011 | 683 个 review candidate 必须逐一进入 prepared 或 typed unavailable，分母闭合。 |
| F-012 | 原候选代码 AST/code identity 在适配前复核；适配名稳定、唯一且符合 runner 命名合同。 |
| F-013 | 静态审计发现顶层执行、动态执行、未知输入与额外写入；不得执行未通过代码。 |
| F-014 | 适配器仅按显式日期/股票读取，保持候选计算区不变；输入文件 mtime/内容不被修改。 |
| F-015 | 技术预检逐候选隔离、超时和失败后继续，结果分母闭合；不把失败或小样本结果解释为价值。 |
| F-016 | 正式评价复用既有 runner/指标/相关实现；本增补不新增指标引擎、生产表、门禁或调度器。 |

## 15. 完整 PIT 评价执行增补（2026-09-17）

本节承接用户对 683 个待审核候选执行完整 PIT 历史评价的授权。它不改变第 1～14 节已经完成的来源清点、确定性去重和技术预检事实，也不把研究候选自动升级为正式因子。

### 15.1 分片生成与逐候选指标

完整值生成继续复用 `salvage-precheck` 的已审计适配器，通过互斥 `candidate_ids` 分片并行执行。正式评价范围绑定执行时 active profile 的版本化 Qlib/因子数据目录、canonical PIT 股票池以及 `2018-08-01..2026-08-31` 连续窗口；不得回退到旧 profile，也不得重新导出数据集。

`salvage-evaluate` 读取一个或多个互斥分片的结果，加载一次既有 `prepare_shared_context`，然后逐候选调用既有 `compute_single_factor_metrics`/`compute_candidate_metrics`。任何时刻只保留当前候选值和共享评价上下文，不构造 683 列候选大面板。输出覆盖全期、2024 至今、2024/2025/2026 年度、近 6/3/1 个月、2024 年以来逐月以及 h1/h5/h10/h20；NaN/Inf 继续沿用现有显式缺失语义，不补零、不删除股票来美化结果。

同名不同代码以 `evaluation_name = normalized_name + AST identity` 分别评价；没有任何“同名取最新”或输入顺序覆盖。每个候选必须落入 `evaluated` 或带阶段的 typed unavailable，满足：

`requested_candidates = evaluated_candidates + unavailable_candidates`

### 15.2 有界相关性与短名单复核

`salvage-correlate` 只计算“历史候选 × 可比正式因子库”指定子矩阵，复用 `CorrelationEngine.compute_selected_daily_submatrix`，按候选块和正式因子块加载，不计算已经存在的正式因子间矩阵。结果只保留每个候选、每个声明窗口的分母、不可用原因和绝对值最高的正式因子匹配；不保存无决策价值的百万行成功日志。全部候选仍满足：

`requested_pairs = available_pairs + unavailable_pairs`

候选之间的相关性不在 683 个候选上先做全矩阵。先用完整独立指标与候选×正式库结果形成待晋级短名单，再对短名单运行候选×候选相关性；这不是省略去重，而是把昂贵比较限定到仍可能进入正式库的对象。同名不同代码候选在短名单决策前不得因名称合并。

### 15.3 并发、恢复和停止条件

- 并发只来自调用者显式传入、彼此互斥的 candidate 分片；没有后台调度器、资源门禁或自动扩缩容。
- 单分片失败不覆盖已完成的 repo-external 产物；后续只对缺失 candidate identity 建立新目录重跑，不重算成功分片。
- 操作系统 OOM、磁盘写失败或输入 profile 改变时停止该分片并报告；不通过改股票池、缩日期或关闭 PIT 语义继续。
- 评价产物仅用于形成 `建议入库 / 替换或版本候选 / 继续研究 / 拒绝或技术不可用` 清单。正式保存、官方指标/相关性写库和晋升仍需独立授权。

### 15.4 完整评价验收

| item | requirement |
| --- | --- |
| F-017 | 分片 candidate identity 互斥且并集闭合；完整值绑定同一 active profile、canonical PIT 股票池和完整日期范围。 |
| F-018 | `salvage-evaluate` 共享上下文只加载一次、逐候选计算既有正式指标；全期、2024+、年度、近期、逐月和四个期限均由现有引擎给出。 |
| F-019 | `salvage-correlate` 采用有界 candidate/reference block，只算候选×正式库；每个窗口相关性分母闭合，正式库内部 pair 数为 0。 |
| F-020 | 同名不同代码逐一进入完整评价；最终建议不得只依据小样本预检、因子名、旧 RD-Agent 超额收益或单一 IC。 |
| F-021 | 短名单执行候选×候选相关复核后才可给出入库建议；无正式因子库、生产指标、相关性或 `is_available` 写入。 |
