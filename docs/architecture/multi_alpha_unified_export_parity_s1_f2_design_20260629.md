# Multi-Alpha Unified StrategyPackage Export Parity S1 F2 Design

## Background

PR #1701 §3.8 将 StrategyPackage 的统一抽象核实为既有事实：单 Alpha 和多 Alpha 共用 `StrategyPackageManifest`、同一张 `strategy_pkg.package` 表、同一套后续消费流程；差异只在 `alpha_mode` 和信号层组合逻辑。当前缺口不是包抽象，而是导出体验不对称：单 Alpha 可通过 `from-qe-experiment` / `from-qe-evolution-loop` 一步导出，多 Alpha combine-backtest 仍要求操作者先为每条腿手工建 single_alpha component 包，再把 `component_package_ids` 传给 `from-multi-alpha-combine-run`。

第 0 步 ground-truth 已完成（只读，不写 DB）：

- 现有 `SourceType` 只有 `qe_experiment`、`qe_evolution_loop`、`candidate_strategy_package`。
- 现有 `multi_alpha_promotion._build_manifest()` 把多 Alpha parent 包记录为 `SourceType.CANDIDATE_STRATEGY_PACKAGE`，`source_id` 是 `multi_alpha_combine:<run_id>:<scheme>:topk<topk>:<suffix>`。
- 当前生产库 `strategy_pkg.package` 中 `alpha_mode='multi_alpha'` 为 0 条；`strategy_pkg.multi_alpha_combine_backtest_run` 为 28 条，因此 source_type 迁移风险低，但仍需要 DDL 兼容处理。
- 目标 roster `7738e811293948eb` 最新成功 run 为 `macb_7738e811293948eb_20250601_20260310_20260627T191255096216Z`；两条腿分别为 `a1_plus3_LSTM_h20`（33 个 `qear_run_*` seed）和 `new_FUNDGROWTH_h20`（5 个 `qe_<task>_L<idx>` seed）。
- 该 run 的 seed 解析前提成立：33/33 个 `qear_run_*` 可经 `qe_archive.run` 解析；5/5 个 `qe_<task>_L<idx>` 可经 `qe_evolution_loops` + `qe_archive.run` 解析；无 unresolved seed。

## Scope

本 S1 只做多 Alpha 导出 parity：

- S1-1：`from-multi-alpha-combine-run` 在缺省 `component_package_ids` 时自动复用或物化每条腿的 component single_alpha 包。
- S1-2：新增 `SourceType.MULTI_ALPHA_COMBINE_RUN = "multi_alpha_combine_run"`，并让多 Alpha parent 包 `manifest.source.source_type` 与 `source_id` 如实指向 combine run。
- S1-3：combine-backtest 详情页增加“导出为策略包”入口，默认 `ic_weighted` + 当前 run 的 TopK 语义，调用同一个后端端点。
- S1-4：把 UI 文案从“JSON 导出 / MCP-only promotion”调整为“导出策略包”的同一概念；导出后进入 `/paper-v2/packages` 的同一包列表和后续 Selection / Advisory / Paper v2 流程。

## Non-goals / 边界

- 不改 `StrategyPackageManifest` schema，不新增 manifest 顶层字段。
- 不改 F-008：Paper v2 / Selection Center / Advisory 仍只消费单个 `package_id`。
- 不改单 Alpha `from-qe-experiment` / `from-qe-evolution-loop` 的 API 契约和现有导出路径。
- 不启动或重启 backend `8001`、frontend `3000`、TDX `19080`。
- 不在本任务内打通 Paper v2 LocalSim admission；该工作属于并行的批 1 admission 任务。
- 不执行生产 DDL/DML；本 PR 只提交 migration，生产应用由用户或合入后流程控制。

## Architecture

### 1. Promotion flow

`MultiAlphaPackagePromotionService.promote_from_combine_run()` 的函数签名调整为：

```python
def promote_from_combine_run(
    *,
    combine_backtest_run_id: str,
    weighting_scheme: str,
    topk: int,
    confirmation: str,
    component_package_ids: Mapping[str, str] | None = None,
    weight_policy: Mapping[str, Any],
    scheme_result_id: str | None = None,
    secondary_topk: Sequence[int] | None = None,
    package_name: str | None = None,
    promotion_gate: Mapping[str, Any] | None = None,
) -> MultiAlphaPackagePromotionResult:
    ...
```

执行顺序：

1. 读取 combine run、roster、scheme_result、terminal weights，并保留现有成功状态、指标、prediction ref、weight policy 校验。
2. 若 `component_package_ids` 非空：走原显式路径，要求 leg_id 集合与 roster 精确一致，并校验 child single_alpha / manifest sha / seed 覆盖。
3. 若 `component_package_ids` 为空：对每个 roster leg 调用 `_materialize_or_reuse_component_package()`。
4. 生成 parent multi_alpha manifest，并保存 parent 与 component edges。

### 2. 自动 component 复用 / 物化

新增内部函数签名：

```python
def _materialize_or_reuse_component_package(
    self,
    *,
    leg_id: str,
    seed_run_ids: tuple[str, ...],
    terminal_weight: float,
    run_id: str,
) -> _LegEvidence:
    ...

def _resolve_leg_seed_sources(
    self,
    *,
    leg_id: str,
    seed_run_ids: tuple[str, ...],
    run_id: str,
) -> list[SeedProvenance]:
    ...

def _build_auto_component_manifest(
    self,
    *,
    leg_id: str,
    seed_run_ids: tuple[str, ...],
    seed_sources: Sequence[SeedProvenance],
    run_id: str,
) -> StrategyPackageManifest:
    ...
```

复用判定：

- 扫描已有 `alpha_mode=single_alpha` 且未 `RETIRED` 的包。
- `_collect_seed_refs(child)` 必须覆盖该 leg 的全部 `seed_run_ids`。
- `manifest_sha256` 必须为合法 sha256，且存储值等于当前 manifest 计算值；否则 fail-loud，`reason_code=multi_alpha_child_package_not_frozen`。
- 命中多个等价包时选择创建时间最新的合法包；不按名字或 UI 文案猜测。

自动物化：

- 每个 seed 用 `MultiAlphaProvenanceResolver.resolve_seed()` 解析。
- `qear_run_*` 走 `qe_archive.run`；`qe_<task>_L<idx>` 走 `qe_evolution_loops` + `qe_archive.run`。
- 若 provenance 提供 `source_task_id + source_loop_id`，通过现有 `QEExperimentSourceResolver.build_from_evolution_loop()` 获取 base single_alpha manifest；`source_loop_id` 为 `qe_<task>_LoopN` 时转换为 resolver 需要的 `LoopN`。
- 若只能提供 `source_experiment_id`，通过 `QEExperimentSourceResolver.build_from_experiment()` 获取 base manifest。
- 自动 component 使用 base manifest 的 factor/model/metrics/runtime evidence 作为单腿基础，再附加 `source_evidence.multi_alpha_component`：`combine_backtest_run_id`、`leg_id`、完整 `seed_run_ids`、每个 seed 的解析坐标、`component_materialization="auto_from_combine_roster"`。
- 自动 component 的 `alpha_mode` 仍是 `single_alpha`，`alpha_combination_policy` 仍是 `identity`，不会引入新的 manifest 结构。
- 自动 component 的 `source.source_type` 使用 `multi_alpha_combine_run`，`source.source_id=combine run_id`，`source.loop_id="component:<leg_id>:<seed_digest>"` 作为同一 run 下多条腿的兼容唯一维度；真正 QE 来源在 `source_evidence.multi_alpha_component.seed_provenance` 中审计。
- 自动 component `package_id` 由 `run_id + leg_id + seed_run_ids + provenance coordinates` 的 canonical JSON 计算，重复导出同一 run + roster 不生成重复 component 包。

### 3. Parent lineage

Parent multi_alpha manifest：

- `source.source_type = SourceType.MULTI_ALPHA_COMBINE_RUN`
- `source.source_id = run_id`
- `source.loop_id = f"{scheme_result_id}:topk{topk}"`，仅用于现有唯一索引兼容，不改变 source_id 的审计语义。
- `source.run_id = run_id`
- `source_evidence.multi_alpha.combine_backtest_run_id = run_id` 保持现有审计字段。
- `combined_prediction_ref` 解析优先使用 scheme_result / weights_json 内的显式 prediction-store manifest；若存量 combine run 只有 `pred_persisted=false` 或缺少显式 ref，则只读查找 combine workspace 文件 `<root>/<run_id>/combined_<scheme>/combined_prediction.pkl`，root 来自注入参数、`AISTOCK_MULTI_ALPHA_BACKTEST_ROOT`、相对 `rdagent_assets/multi_alpha_combine_backtests` 和 repo-root 同名目录。命中后计算 sha256 并记录 `combined_prediction_ref_source=combine_backtest_local_workspace`；缺失、空文件、不可读或路径逃逸均 fail-loud，不写 parent 半包。
- `backtest_config_json` 兼容旧 combine 记录：`stock_pool` / `execution_algo` 缺失但存在 `strategy` 时，`strategy` 可作为二者的冻结审计值；若 `strategy` 也缺失，仍以 `multi_alpha_manifest_incomplete` 拒绝。

### 4. Fail-loud reason codes

新增或沿用 reason_code：

| reason_code | 触发条件 |
|---|---|
| `multi_alpha_combine_run_missing` | combine run 不存在 |
| `multi_alpha_scheme_not_succeeded` | scheme_result 缺失、失败、skipped、指标非有限 |
| `multi_alpha_roster_mismatch` | roster leg/seed 与显式请求不一致，或 roster 缺 seed |
| `multi_alpha_child_package_missing` | 显式 child package 不存在 |
| `multi_alpha_child_package_not_frozen` | child package sha 缺失、格式非法、或 manifest sha 漂移 |
| `multi_alpha_seed_unresolved` | seed 无法解析为 QE archive run 或 QE evolution loop |
| `multi_alpha_seed_source_incomplete` | seed 已解析但缺少构建 component 所需 QE source 坐标 |
| `multi_alpha_component_auto_materialize_failed` | 解析坐标存在，但调用既有 QE resolver 构建 single_alpha manifest 失败 |
| `multi_alpha_prediction_ref_missing` | 显式 combine prediction ref 缺失 / sha 无效，且本地 combine workspace 文件不存在、为空或不是文件 |
| `multi_alpha_prediction_ref_unreadable` | 本地 combine workspace prediction 文件存在但无法读取或 hash |
| `multi_alpha_prediction_ref_path_escape` | 本地 prediction 文件候选路径逃逸配置的 workspace root |
| `multi_alpha_manifest_incomplete` | topk、weight_policy、backtest_config、component evidence 等缺关键字段 |
| `multi_alpha_metrics_below_gate` | 显式 promotion gate 不满足 |

任何错误都在写 parent / component edge 之前抛出；自动 component manifest 保存发生在 parent 保存之前，因此 parent 不会半包落库。

## Contracts / API DB UI MCP

### API

`POST /api/v1/strategy-packages/from-multi-alpha-combine-run` 保持路径不变。

请求兼容：

```json
{
  "combine_backtest_run_id": "macb_...",
  "weighting_scheme": "ic_weighted",
  "scheme_result_id": "scheme_icw_1",
  "topk": 50,
  "secondary_topk": [25],
  "package_name": "MA2_...",
  "component_package_ids": {},
  "weight_policy": {"mode": "frozen_backtest_terminal_weights"},
  "confirmation": "MULTI_ALPHA_PACKAGE_PROMOTE"
}
```

- `component_package_ids` 缺省或 `{}`：自动复用 / 物化 component。
- `component_package_ids` 非空：保留旧路径，精确校验 leg_id。
- 响应维持现有 `ok/package_id/alpha_mode/manifest_sha256/source_run_id/package/components` 结构，并新增 `auto_component_materialization` 摘要字段（仅响应层，非 manifest schema）。

### DB / migration

需要更新 `strategy_pkg.package.source_type` CHECK 约束，允许：

- `qe_experiment`
- `qe_evolution_loop`
- `candidate_strategy_package`
- `multi_alpha_combine_run`

提交 migration：`backend/migrations/strategy_pkg_multi_alpha_combine_source_type_20260629.sql`；更新 schema initializer：`backend/db/init_trading_core_v2_schema.py` 和 `backend/migrations/trading_core_v2_schema.sql` 中的约束文本。存量 multi_alpha 包当前为 0 条；若未来有旧 `candidate_strategy_package` source 的 multi_alpha 包，代码仍能读取，因为 enum 保留旧值。

### UI

`frontend/src/app/quantevolver/multi-alpha/combine-backtest/[taskKey]/page.tsx`：

- 将原“导出”改名为“导出 JSON”。
- 新增“导出为策略包”按钮。
- 默认 scheme 使用当前 `selectedScheme`，按钮仅在 run 状态为 `succeeded/completed` 且 scheme 为 `ic_weighted` 时可用；disabled 文案说明原因。
- 请求体不传 `component_package_ids`，让后端自动物化。
- `topk` 优先取 active loop / task backtest config 的 `topk`，缺失时 fail in UI，不用默认 TopK 兜底。
- `weight_policy.mode` 固定传 `frozen_backtest_terminal_weights`，其余参数使用现有 promotion 默认显示值。
- 成功后显示 `package_id` 和跳转 `/paper-v2/packages?package_id=<id>` 的链接；失败显示后端 reason_code/context。

### MCP / 文案

本任务不新增 MCP tool；API 语义作为 MCP / operator 文案基础：

- QE 单 Alpha source：`qe_experiment` / `qe_evolution_loop`。
- 多 Alpha source：`multi_alpha_combine_run`。
- 两者统一称为“导出策略包”，导出后进入同一 StrategyPackage 列表。

## Design Acceptance Index

- F-001：`component_package_ids` 从必填改为可选，缺省时自动复用或物化每条 leg 的 single_alpha component 包。
- F-002：自动 component 复用必须校验 seed 覆盖和 manifest sha；sha 缺失或漂移必须 fail-loud。
- F-003：自动 component 物化必须解析 `qear_run_*` 与 `qe_<task>_L<idx>` 两种 seed，并通过既有 QE resolver 构建 base single_alpha manifest。
- F-004：新增 `SourceType.MULTI_ALPHA_COMBINE_RUN` 与 DB CHECK 兼容；parent source 必须记录 `source_type=multi_alpha_combine_run`、`source_id=run_id`。
- F-005：旧显式 `component_package_ids` 路径必须继续兼容并保留原校验。
- F-006：所有自动建包 / 解析失败必须带具体 reason_code + context，不能跳 seed、不能降级、不能默认 equal/component。
- F-011：prediction ref 解析优先显式 prediction-store manifest；存量 run 缺显式 ref 时允许只读使用 combine workspace `combined_prediction.pkl` 并冻结 uri/sha/ref_source，缺失/不可读/路径逃逸必须 fail-loud。
- F-007：combine detail UI 必须新增“导出为策略包”按钮并调用真实后端 API；原 JSON 下载语义必须区分。
- F-008：单 Alpha 导出路径和 F-008 单 `package_id` downstream 契约零回归。
- F-009：同 run + 同 roster 重复导出必须幂等，不重复创建 component 包，parent 包 source lineage 稳定。
- F-010：验证证据必须覆盖自动路径、显式旧路径、负路径、SourceType lineage、prediction ref fallback、legacy `strategy` config 兼容、UI 静态/类型检查和单 Alpha regression。

## Implementation Plan

1. 设计落地：提交本文档并运行 `python scripts/aistock_feature_workflow.py validate --design docs/architecture/multi_alpha_unified_export_parity_s1_f2_design_20260629.md --tier F2`。
2. 后端模型与 migration：新增 enum 值，更新 CHECK 约束文件与 schema initializer。
3. 后端 promotion：注入 provenance/source resolver；实现 auto component reuse/materialize；保留 explicit path；响应增加 auto summary。
4. 后端测试：扩展 `backend/tests/strategy_package/test_multi_alpha_promotion.py` 覆盖自动创建、复用、source lineage、seed unresolved、显式旧路径。
5. 前端：在 combine detail 页新增真实导出按钮、错误展示、跳转链接；改原按钮为“导出 JSON”。
6. 验证与 PR：运行目标 pytest、py_compile、TypeScript check、feature workflow validate、git diff check，并记录 validation history。

## Verification Plan

目标命令：

```powershell
python scripts/aistock_feature_workflow.py validate --design docs/architecture/multi_alpha_unified_export_parity_s1_f2_design_20260629.md --tier F2
python -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_qe_source_resolver.py -q -p no:cacheprovider
python -m py_compile backend/services/strategy_package/models.py backend/services/strategy_package/multi_alpha_promotion.py backend/routers/strategy_packages.py
cd frontend; npx tsc --noEmit --pretty false
cd ..; git diff --check
```

业务验收映射：

- 自动路径：in-memory combine run 不传 `component_package_ids`，自动建 2 个 child + parent。
- 幂等：重复调用后 child count 不增加，parent package_id / manifest_sha256 稳定。
- lineage：parent manifest source_type/source_id 正确。
- 负路径：缺 seed / seed unresolved / child sha drift / prediction ref missing 返回 reason_code，parent 不落半包。
- 存量 combine 兼容：缺显式 `combined_prediction_ref` 时可从本地 combine workspace 文件冻结 `file://` uri + sha；`backtest_config_json.strategy` 可补齐旧记录缺失的 `stock_pool` / `execution_algo`。
- 显式旧路径：传 `component_package_ids` 的现有测试继续通过。
- UI：TypeScript check 覆盖按钮 props / API payload；如需真实 E2E，仅使用 3011/8011，不碰生产端口。
- 单 Alpha：现有 QE resolver tests 保持通过；本任务不修改单 Alpha route 行为。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `multi_alpha_promotion.promote_from_combine_run`; `_materialize_or_reuse_component_package` | 自动路径 pytest：不传 `component_package_ids` 自动创建 2 个 child + parent | ready | - |
| F-002 | `_find_reusable_component_package`; `_collect_seed_refs`; manifest hash 校验 | pytest：复用已有 child；sha drift 返回 `multi_alpha_child_package_not_frozen` | ready | - |
| F-003 | `_resolve_leg_seed_sources`; `_build_auto_component_manifest`; `MultiAlphaProvenanceResolver` | ground-truth 33/33 + 5/5 seed 解析；pytest 覆盖两种 seed 格式 resolver stub | ready | - |
| F-004 | `models.SourceType`; migration check constraint; parent `_build_manifest` | pytest 断言 parent `source_type/source_id`；schema grep / py_compile | ready | - |
| F-005 | explicit branch in `promote_from_combine_run`; `_load_leg_evidence` | 现有 router/service explicit tests 继续通过 | ready | - |
| F-006 | `_fail`; `_fail_manifest_incomplete`; new reason_code branches | pytest negative paths assert reason_code/context | ready | - |
| F-011 | `_extract_prediction_ref`; `_workspace_prediction_ref`; `_build_strategy_snapshot` | pytest covers local workspace `combined_prediction.pkl` fallback, missing fallback reason context, and legacy `strategy` config | ready | - |
| F-007 | `frontend/src/app/quantevolver/multi-alpha/combine-backtest/[taskKey]/page.tsx` | `npx tsc --noEmit --pretty false`; UI payload code review | ready | - |
| F-008 | no changes to single Alpha service/router; F-008 downstream untouched | `test_qe_source_resolver.py` + targeted StrategyPackage tests | ready | - |
| F-009 | stable component/package id helpers; repository save idempotency | pytest repeated export count/package hash assertions | ready | - |
| F-010 | validation history + PR body self-review matrix | feature workflow validate + command evidence in PR | ready | - |

## Rollout / Rollback

Rollout:

1. 合并代码和 migration 后，先应用 CHECK 约束 migration。
2. UI combine detail 页即可对 succeeded + `ic_weighted` run 一步导出 StrategyPackage。
3. 导出的 parent 与自动 component 均写入同一 `strategy_pkg.package` 表；后续 Selection / Advisory / Paper v2 流程按既有列表发现。
4. 对存量显式 component 操作者不做破坏，仍可传 `component_package_ids`。

Rollback:

1. 前端可隐藏“导出为策略包”按钮，后端显式旧路径仍可用。
2. 若后端需要回滚，恢复 `promote_from_combine_run` 要求显式 `component_package_ids`；已创建的 package 仍保持 frozen manifest 可审计。
3. DB 回滚需先确认不存在 `source_type='multi_alpha_combine_run'` 的 package；否则不得收窄 CHECK 约束。
4. 不删除已生成 component/parent 包，除非走现有 StrategyPackage delete dependencies 安全检查。

## Risks / Failure Modes

- SourceType CHECK 未应用：后端创建新 source_type 会被 DB 拒绝；PR 必须标注 `production_ddl_gate=pending` 直到 migration 应用。
- seed provenance 不完整：自动 component 失败并返回 `multi_alpha_seed_source_incomplete`，不会默认挑第一条可用 seed。
- child sha 漂移：自动复用或显式传入均拒绝，避免 parent 锁到不可信 child。
- 同一 run 多 scheme/topk：parent `source.loop_id` 使用 `scheme_result_id:topk` 作为唯一维度；`source_id` 仍为 run_id。
- UI TopK 缺失：按钮失败显示“缺少 TopK”，不使用硬编码默认。
- 存量 combine prediction 只有本地文件：后端只在显式 ref 缺失时读取并 hash `combined_prediction.pkl`；文件缺失或不可读时返回 `multi_alpha_prediction_ref_missing` / `multi_alpha_prediction_ref_unreadable`，不会假造 prediction-store ref。

## Production Gates

- `production_ddl_gate=pending`：本任务提交 DB CHECK migration，但当前会话不执行生产 DDL。
- `production_frontend_dependency_gate=noop`：不新增前端依赖。
- `production_backend_dependency_gate=noop`：不新增后端依赖。
- Production runtime：不启动/重启 backend 8001、frontend 3000、TDX 19080。
- Production DB：除只读 ground-truth 外不写入生产 DB，不执行 DDL/DML。
