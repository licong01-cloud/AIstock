# Multi-Alpha Parent Self-Contained Runtime And Parent-Only Promotion F2 Design

日期：2026-07-02
分支：`docs/multi-alpha-parent-self-contained-design-20260702`
任务层级：T3 / F2，涉及 StrategyPackage promotion、frozen runtime asset、Selection Artifact、Paper dry-run admission。
当前阶段：设计先行；本文档通过 Tier2 审核前不实现运行时代码、不启服务、不写生产 DB、不执行子包退役 DML。

## Background

用户已确认的战略事实是：进入 StrategyPackage 体系的多 Alpha 资产只能是组合后的父包，不得再出现 UI 上像独立策略包、但实际不可选股的单腿 component 子包。当前 `promote_from_combine_run()` 在缺省路径会自动创建两个 `single_alpha` component 子包；这些子包资产已被父包完整覆盖，却仍污染包列表和心智模型。

现状代码证据：

- `backend/services/strategy_package/multi_alpha_live.py:339` 的 `MultiAlphaLivePredictionProvider.generate_artifacts()` 目前对每条腿调用 `_validate_child()` 与 `_run_seed_live_inference(child_record=child)`。
- `backend/services/strategy_package/multi_alpha_live.py:572` 的 `_validate_child()` 要求 `child_package_id` 存在，并校验 `child_manifest_sha256`。
- `backend/services/strategy_package/multi_alpha_live.py:606` 的 `_run_seed_live_inference()` 以 `child_record.current_manifest()`、`child_record.package_id`、`seed_run_id` 调 `load_source_for_strategy_package()`，因此运行时硬依赖子包记录。
- `backend/services/strategy_package/live_inference.py:704` 在 manifest 已有 frozen runtime assets 时短路到 `_source_from_package_assets()`，且该短路发生在使用 `run_id` 前；这解释了 frozen child 下多个 seed 实际读取同一冻结模型。
- `backend/services/strategy_package/live_inference.py:368` 的 `_single_model_asset_for_runtime()` 当前要求 runtime manifest 只有一个 model asset；父包多 model 需要先按腿切片，不能把整包直接喂给现有单模型路径。
- `backend/services/strategy_package/live_inference.py:809` 的 `_source_from_package_assets()` 已经能从 package asset store 物化 factor/model/model_code/Alpha158，但 cache namespace 当前仅由 `package_id + manifest_sha` 组成；父包按腿切片后必须增加 leg 维度，避免两条腿互相覆盖 workspace。
- `backend/services/strategy_package/multi_alpha_promotion.py:233` 到 `:254` 在缺省 `component_package_ids` 路径会 `_prepare_component_package()`，再 `_save_component_plans()`。
- `backend/services/strategy_package/multi_alpha_promotion.py:314` 的 `_prepare_component_package()` 会复用或新建 child，并在 `:353` 冻结 child assets。
- `backend/services/strategy_package/multi_alpha_promotion.py:770` 的 `_build_manifest()` 当前从 `leg.child.manifest` 合并父包 `factor_set` 和 `model_asset`，并在 `source_evidence.multi_alpha.legs` 写入 `child_package_id`。
- `backend/services/strategy_package/multi_alpha_promotion.py:1427` 的 `_component_from_child()` 已经把每腿 `factor_artifact_refs` 和 `model_id` 写入 `AlphaComponent`，但 `lineage.model_artifact_ref` 仍是 `child_package:<id>`。
- `backend/services/strategy_package/models.py:64`、`:72`、`:114`、`:159`、`:277` 证明 manifest 已有 `AlphaLineage.factor_artifact_refs`、`AlphaComponent.model_id`、`factor_set`、`model_asset`、`runtime_assets` 字段，可表达父包自包含映射。
- `backend/services/strategy_package/frozen_runtime_self_check.py:161` 的 `assert_manifest_self_contained()` 已有 frozen asset origin 和 strict feature count 检查，需扩展为同一入口下的 multi-alpha per-leg 检查，而不是另开宽松路径。
- `backend/services/strategy_package/multi_alpha_paper_dry_run.py:157` 已通过 `StrategyPackageSelectionArtifactService.generate_from_live_inference()` 走真实 live inference 链路；provider 改成父包自包含后 dry-run validator 必须同步消费新路径。

业务结论：父包现有 manifest 已携带足够的每腿映射，运行时依赖 child package 是实现路径选择，不是资产缺口。新设计以父包 `model_id`、`factor_artifact_refs`、`runtime_assets.alpha158` 和父包 package asset ledger 为唯一运行时 authority；`child_package:` legacy ref 只能作为审计历史被忽略，不得回退读取。

## Scope

本设计覆盖两块：

1. 多 Alpha 父包运行时自包含：
   - `MultiAlphaLivePredictionProvider` 每腿从父包自有 `model_asset`、`factor_set`、`runtime_assets.alpha158` 按腿切片物化 workspace。
   - 不再调用 `_validate_child()`，不再读取 `child_package_id`，不再以 child manifest 作为 runtime source。
   - 保持组合语义、权重、归一化、coverage gate、selection artifact 主体行为与现状一致。
   - `MultiAlphaPaperDryRunValidator` 继续走真实 selection artifact / runtime / target / rebalance preview 链路，但底层 live inference 来源改为父包自包含。
2. Promotion 不再创建或复用单腿子包：
   - `promote_from_combine_run()` 对 multi-alpha promotion 只生成一个父包 StrategyPackage。
   - 停止 `_prepare_component_package()`、`_save_component_plans()` 和 reusable child 分支；显式 `component_package_ids` 在新契约下 fail-closed。
   - 父包 freeze 继续嵌入所有 per-leg model/factor/model_code/Alpha158，并写入自包含 lineage。
   - 父包 freeze 后走多 Alpha self-check；冻不全、不匹配、不能出 combined signal 均 fail-closed，不入库。

## Non-Goals

- 不改 single-alpha package 生成、freeze、selection、paper 路径。
- 不执行生产 DDL/DML，不退役已存在子包，不硬删任何 `pkg_mac_*` 资产。
- 不启/重启 backend、frontend、TDX 或 WSL 服务；实现阶段的 parity 只允许运行 CLI/pytest/debug tool，不启动生产服务。
- 不改变 combine-backtest schema，不新增 DB 表，不放宽 `strategy_pkg.package` manifest hash 或 asset ledger 校验。
- 不把 `child_package`、QE 源、prediction workspace 作为 runtime fallback；测试 oracle 可以显式读取 legacy child 作对照，但不得进入生产 provider。
- 不触碰已合并的固化 #1792 与因子库保护 #1799 的语义；若实现文件有交叉，必须只做兼容调用，不放宽保护。

## Architecture

### 1. Parent self-contained leg slicing

运行时新增一个内部概念：`ParentLegRuntimeSlice`。它不是新 DB schema，而是从父包 frozen manifest 和 package asset ledger 派生的只读运行时视图。

每腿解析顺序：

1. `leg_id` authority：
   - 以 `manifest.alpha_components[].alpha_id` 为主表。
   - `source_evidence.multi_alpha.legs[].leg_id` 只用于 seed metadata、terminal evidence 和 legacy audit，必须与 component id 集合一致；不一致 fail-closed。
2. model authority：
   - `component.model_id` 必填。
   - 在父包 `manifest.model_asset` 列表中按 `model_id` 精确命中一个 `ModelAsset`。
   - 0 个命中、多个命中、缺 `asset_ref/sha256`、model_code required 但缺 code asset 均 fail-closed。
3. factor authority：
   - `component.lineage.factor_artifact_refs` 必填，作为每腿因子子集 authority。
   - 每个 ref 必须在父包 `manifest.factor_set` 中精确命中一个 factor。匹配键按实现固定为：先 `factor_name`，再 `factor_id`，如果任一键出现多义或 ref 同时命中多个 asset，fail-closed。
   - `component.factor_ids` 只做一致性校验和 UI 展示辅助，不作为 silent fallback。
4. Alpha158 authority：
   - 父包 `manifest.runtime_assets.alpha158.enabled=true` 时必须有 `asset_ref/sha256/aliases`。
   - 若父包缺 schema，不能去 child 或 QE 源补；直接 fail-closed。
5. package asset ledger authority：
   - 每个 slice 读取父包 `package_id` 下的 `model_weight`、`factor_code`、`model_code`、`factor_schema` asset refs。
   - `asset_ref` sha 校验沿用 `QEExperimentRuntimeAssetResolver._read_package_asset_bytes()` 的现有逻辑。

推荐实现方式：

```python
@dataclass(frozen=True)
class ParentLegRuntimeSlice:
    parent_package_id: str
    parent_manifest_sha256: str
    leg_id: str
    component: AlphaComponent
    model_asset: ModelAsset
    factor_set: tuple[FactorAsset, ...]
    runtime_assets: RuntimeAssetManifest
    seed_run_ids: tuple[str, ...]
    terminal_weight: float | None
```

在 `QEExperimentRuntimeAssetResolver` 中新增显式 leg 方法，而不是把父包伪装成 child package：

```python
def load_source_for_strategy_package_leg(
    *,
    manifest: StrategyPackageManifest,
    package_id: str,
    leg_id: str,
    model_asset: ModelAsset,
    factor_set: Sequence[FactorAsset],
    runtime_assets: RuntimeAssetManifest | None,
) -> QEExperimentRuntimeSource: ...
```

该方法复用 `_source_from_package_assets()` 的物化逻辑，但需要扩展 `_source_from_package_assets()` 支持 `model_asset_override`、`factor_set_override`、`runtime_assets_override`、`cache_namespace`。不要新增从 child/QE 源读取的 fallback 分支。

### 2. Cache and workspace isolation

父包两条腿共享 `package_id` 和 `manifest_sha256`，现有 cache key 会冲突。实现必须把 `leg_id` 纳入 source 和 prepared workspace namespace：

- source cache：`_package_asset_sources/<parent_package_id>/<manifest_sha_prefix>/leg_<safe_leg_id>/`
- prepared workspace：`<cache_root>/<parent_package_id>/<manifest_sha_prefix>__leg_<safe_leg_id>/`

不要为了 cache 隔离伪造 package id 存入 artifact 或 DB。`package_id` authority 仍是父包，cache namespace 只是本地临时路径。

### 3. Runtime generation flow

`MultiAlphaLivePredictionProvider.generate_artifacts()` 改成：

1. 读取父包 record 和 manifest，确认 `alpha_mode=MULTI_ALPHA`、`manifest_sha256` 存在。
2. 解析全部 `ParentLegRuntimeSlice`，并校验 component 集合、weight 集合、source evidence leg 集合一致。
3. 每个 trade date：
   - 每条腿调用父包 leg slice resolver 物化 workspace。
   - 每条腿只运行一次 frozen representative model。
   - 为保持当前 frozen child 行为 parity，把单次推理结果按 `seed_run_ids` 复制成 seed frame map，再调用现有 `_ensemble_seed_frames()`；这样 score/topK 与当前 child-based frozen path 一致，同时 `seed_count` 仍等于 manifest seed 数。
   - 执行 `_normalize_leg_frame()`、`_align_component_frames()`、coverage gate、`weight_service.weights_for_apply_date()`、`_combine_aligned()` 和 `_artifact_rows()`。
4. selection artifact metadata 改为 parent asset authority：
   - 保留 `component_score_artifact_id`、`component_score_artifact_sha256`、`seed_run_ids`、`seed_count`、`ensemble_method`、`candidate_count`。
   - 新增或替换为 `runtime_source="parent_package_asset"`、`runtime_package_id=<parent>`、`model_id`、`model_asset_ref`、`factor_count`、`alpha158_schema_sha256`。
   - 不填充 runtime authoritative `child_package_id` / `child_manifest_sha256`。如兼容旧 JSON consumer 必须保留 key，则值只能为 `null`，并附 `legacy_child_ref_ignored=true`；不得读取或展示为依赖。

### 4. Seed ensemble parity

当前 frozen child path 因 `load_source_for_strategy_package()` 在 `manifest_has_frozen_runtime_assets()` 时短路，所有 seed 实际加载同一个冻结 child model。父包每腿已嵌入同 sha 的代表模型，因此新路径应保持以下语义：

- `seed_run_ids` 保留为审计和 artifact metadata。
- runtime 只执行每腿一个 frozen representative model。
- ensemble 输入通过复制 frame 保持 `seed_count` 与现状一致；复制行为必须在 metadata 中声明 `seed_runtime_mode="frozen_representative_model_replayed_for_legacy_seed_metadata"`。
- 不允许因 seed 解析失败而回退 QE 源；如果父包缺 seed metadata，只影响 artifact schema 时也必须 fail-closed，而不是默默把 seed_count 置 1。

### 5. Dry-run validator sync

`MultiAlphaPaperDryRunValidator.run()` 不需要绕开 selection pipeline。它继续调用：

- `StrategyPackageSelectionArtifactService.generate_from_live_inference()`
- `StrategyPackageRuntime.build_signal_snapshot()`
- `TargetPositionEngine.build_targets()`
- `RebalanceEngine.build_order_intents()`

provider 改成父包自包含后，dry-run 的 source origin 必须变为 parent package asset。dry-run evidence 需要增加：

- `runtime_source="parent_package_asset"`
- 每腿 `model_id/model_asset_sha256/factor_count/alpha158_schema_sha256`
- `legacy_child_ref_ignored=true`，仅当 manifest 中存在 `child_package:` legacy ref 时记录。

dry-run admission 失败仍不得写 admission row。

### 6. Promotion parent-only freeze

`promote_from_combine_run()` 新契约：

- `component_package_ids` 非空时立即拒绝：`reason_code=multi_alpha_promotion_component_package_ids_unsupported`。
- 不调用 `_prepare_component_package()`、`_find_reusable_component_package()`、`_save_component_plans()`。
- 对 roster 每腿解析 seed provenance，构造 in-memory `LegAssetPlan`，但不保存 child manifest。
- 使用现有 QE resolver / asset freezer 读取每腿代表模型、factor set、model_code、Alpha158 schema；这些结果只进入父包 manifest 和父包 asset ledger。
- 父包保存后 `component_package_id=None`；不写 `StrategyPackageComponentRecord` child edges。UI 和 API 的 component 展示从 `manifest.alpha_components` 派生。

推荐内部结构：

```python
@dataclass(frozen=True)
class LegAssetPlan:
    leg_id: str
    seed_run_ids: tuple[str, ...]
    terminal_weight: float
    component: AlphaComponent
    factor_set: tuple[FactorAsset, ...]
    model_asset: ModelAsset
    runtime_assets: RuntimeAssetManifest
    seed_provenance: tuple[SeedProvenance, ...]
```

父包 build 规则：

- `factor_set = union(leg.factor_set)`，按 `factor_id` 稳定排序；同名不同 sha 或同 id 不同 sha fail-closed。
- `model_asset = [leg.model_asset...]`，按 `model_id` 稳定排序；同 `model_id` 不同 sha fail-closed。
- `runtime_assets.alpha158` 必须从各腿得到相同 schema sha / aliases；不一致 fail-closed。
- `alpha_components[].lineage.factor_artifact_refs` 写每腿 factor refs。
- `alpha_components[].model_id` 写该腿 model id。
- `alpha_components[].lineage.model_artifact_ref` 改为 `parent_package_asset:model_id:<model_id>`，不再写 `child_package:`。
- `source_evidence.multi_alpha.legs[]` 保留 `leg_id/seed_run_ids/ensemble_method/terminal_weight/model_id/factor_artifact_refs`，不再写 `child_package_id` 或 `child_manifest_sha256`。
- `alpha_combination_policy.weights` 继续来自 terminal weights，例如 `{a1: 0.6967, FUND: 0.3033}`。

父包 freeze 后必须生成父包 package asset ledger，依赖 blob store sha 去重；取消子包不会丢失存储复用。

### 7. Self-check contract

不新增宽松的 multi-alpha 自检旁路。扩展 `FrozenRuntimeSelfCheckService.assert_manifest_self_contained()`：

- single-alpha：继续走现有单模型 self-check。
- multi-alpha parent：同一入口 dispatch 到 per-leg self-check。

多 Alpha self-check 最低要求：

1. 每腿都能用父包 asset store materialize leg workspace，`source.model_params_origin="package_asset"`，`source_workspace_type="strategy_package_asset_store"`。
2. 每腿模型可反序列化；feature count 等于 `len(dynamic_factors) + len(alpha158_aliases)`，沿用现有 strict feature count，不放宽。
3. 每腿 factor subset 与 component `factor_artifact_refs` 完全一致。
4. 能对一个明确 self-check trade date 生成非空 leg score；trade date 来源按顺序：promotion request 显式传入、combine run 最新可用 prediction date、`run.oos_end`。三者均不可用则 fail-closed。
5. combined signal 非空，topK 前后 deterministic replay 一致。

Promotion 保存父包前必须先 freeze assets，再跑 self-check。self-check 任一失败不得保存 half package、不得保存 component edge、不得写 child package。

### 8. Legacy parent compatibility

现存父包 `pkg_ma_0c796d57d216ebbd1daf0412` 的 `alpha_components[].lineage.model_artifact_ref` 仍可能是 `child_package:pkg_mac_...`。新运行时兼容策略：

- authority 优先级固定为：`component.model_id` + 父包 `model_asset` + `factor_artifact_refs` + 父包 `runtime_assets`。
- `lineage.model_artifact_ref` 若以 `child_package:` 开头，只记录为 `legacy_child_ref_ignored=true`，不得调用 `package_repository.get(child_id)`。
- 只要父包自有 assets 完整，现存父包无需重建即可运行。
- 如果父包自有 assets 缺 model/factor/schema，直接 fail-closed；不得因为 legacy child ref 存在而读取 child 弥补。

### 9. Existing child package cleanup plan

本 PR 不执行子包退役 DML。实现合并并经 Tier2 parity 通过后，单独走只读确认和状态机退役：

1. 只读查询确认 `pkg_mac_6e48c4963846f7bf4f16a5f9`、`pkg_mac_a889a92ef523d91a1c103dc1` 不再被运行时调用。
2. 查询是否仍被 active Paper portfolio、Selection run、advisory binding、runtime release、component edge 引用。
3. 若仅存在历史 audit ref，走 StrategyPackage 状态机标记 `RETIRED`，不硬删 package、manifest、asset ledger、blob。
4. 退役操作另开 Tier2 DML 任务，记录 before/after DB evidence；不得混入本设计或实现 PR。

## Contracts

### Runtime contract

- 输入：parent `package_id`、trade date、runtime config、frozen parent manifest。
- 输出：同现有 selection artifact row 语义；component score 来自 parent package asset。
- 禁止：child package repository read、QE source fallback、默认因子/模型补齐。
- 必须：失败 response / exception context 带 `reason_code`、`package_id`、`leg_id`、`model_id` 或缺失 asset ref。

### Promotion API contract

`POST /api/v1/strategy-packages/from-multi-alpha-combine-run` 路径不变，但 multi-alpha 请求不再接受 component package：

```json
{
  "combine_backtest_run_id": "macb_...",
  "weighting_scheme": "ic_weighted",
  "scheme_result_id": "scheme_...",
  "topk": 50,
  "secondary_topk": [25],
  "weight_policy": {"mode": "frozen_backtest_terminal_weights"},
  "confirmation": "MULTI_ALPHA_PACKAGE_PROMOTE"
}
```

若请求包含非空 `component_package_ids`：

```json
{
  "detail": {
    "context": {
      "reason_code": "multi_alpha_promotion_component_package_ids_unsupported"
    }
  }
}
```

### DB contract

- 不新增 DB 表，不新增 DDL。
- `strategy_pkg.package` 只新增一个 multi-alpha parent package row。
- 不新增 `single_alpha` component package row。
- 不新增 child package edge；父包 component 展示以 manifest 为 authority。
- 已存在 child rows 不在本 PR 修改。

### Manifest contract

新父包 manifest 必须满足：

- `alpha_mode="multi_alpha"`
- `alpha_components[].model_id` 全部可映射父包 `model_asset[]`
- `alpha_components[].lineage.factor_artifact_refs` 全部可映射父包 `factor_set[]`
- `alpha_components[].lineage.model_artifact_ref="parent_package_asset:model_id:<model_id>"`
- `source_evidence.multi_alpha.legs[]` 不含 runtime authoritative child refs
- `runtime_assets.alpha158` 完整且与每腿 feature order self-check 匹配

### Paper dry-run contract

- `paper-runtime-dry-run` 仍只 admitir `broker_backend=local_sim`。
- admission key 不变：`(package_id, manifest_sha256, broker_backend, runtime_variant)`。
- dry-run evidence 必须证明 `origin=package_asset` 且每腿 `runtime_source=parent_package_asset`。

## Fail-Closed Reason Codes

运行时：

| reason_code | 触发条件 |
|---|---|
| `multi_alpha_parent_leg_mapping_missing` | component、weight、source_evidence leg 集合不一致或缺 leg |
| `multi_alpha_parent_leg_seed_metadata_missing` | 父包缺每腿 seed metadata，无法保持 artifact schema / parity |
| `multi_alpha_parent_leg_model_id_missing` | component 缺 `model_id` |
| `multi_alpha_parent_leg_model_asset_missing` | 父包 `model_asset` 找不到该 `model_id` |
| `multi_alpha_parent_leg_model_asset_ambiguous` | 同一 `model_id` 多个 model asset 或 sha 冲突 |
| `multi_alpha_parent_leg_factor_refs_missing` | component 缺 `lineage.factor_artifact_refs` |
| `multi_alpha_parent_leg_factor_asset_missing` | 父包 `factor_set` 找不到某 ref |
| `multi_alpha_parent_leg_factor_asset_ambiguous` | factor ref 多义或同名不同 sha |
| `multi_alpha_parent_alpha158_schema_missing` | 父包 Alpha158 schema 缺 asset_ref/sha/aliases |
| `multi_alpha_parent_alpha158_schema_mismatch` | schema payload 与 manifest aliases 不一致 |
| `multi_alpha_parent_leg_runtime_assets_incomplete` | model/factor/model_code/schema asset ref 或 sha 缺失 |
| `multi_alpha_parent_leg_feature_count_mismatch` | per-leg feature count 与模型期望不一致 |
| `multi_alpha_parent_leg_inference_empty` | 单腿 parent asset inference 无有效 score |
| `multi_alpha_parent_combined_signal_empty` | 对齐/组合后无 combined score |

Promotion：

| reason_code | 触发条件 |
|---|---|
| `multi_alpha_promotion_component_package_ids_unsupported` | 新 multi-alpha promotion 请求仍传 child package ids |
| `multi_alpha_promotion_leg_source_unresolved` | 某腿 seed provenance 无法解析到可冻结 QE source |
| `multi_alpha_promotion_parent_model_id_collision` | 多腿 model_id 冲突且 sha 不同 |
| `multi_alpha_promotion_parent_factor_ref_collision` | factor id/name 冲突且 sha 不同 |
| `multi_alpha_promotion_parent_runtime_assets_missing` | 无法从腿 source 得到一致 Alpha158 runtime assets |
| `multi_alpha_promotion_parent_asset_freeze_incomplete` | 父包 asset ledger 缺 model/factor/model_code/schema |
| `multi_alpha_promotion_parent_self_check_failed` | 多 Alpha self-check 任一腿失败 |
| `multi_alpha_promotion_parent_combined_signal_failed` | self-check combined signal 为空或 replay 不一致 |

审计信息：

- `multi_alpha_parent_legacy_child_ref_ignored` 只能作为 metadata/warning，不作为 fallback 成功条件。

## Design Acceptance Index

| 设计项 | 标题 |
|---|---|
| F-001 | Runtime 每腿只从父包 `model_id -> model_asset`、`factor_artifact_refs -> factor_set`、`runtime_assets.alpha158` 解析，不读 child package |
| F-002 | Runtime cache/workspace 增加 leg namespace，防止同父包多腿互相覆盖 |
| F-003 | Frozen seed ensemble parity：每腿运行一个代表模型并复制 seed frame 保持现有 child-based score/topK parity |
| F-004 | Dry-run validator 继续走真实 selection/runtime/target/rebalance 链路，但 evidence 指向 parent package asset |
| F-005 | Promotion 停止 auto-create/reuse child package；非空 `component_package_ids` fail-closed |
| F-006 | 新父包 manifest lineage 改为 `parent_package_asset:model_id:<model_id>`，source_evidence 不再写 runtime child refs |
| F-007 | 父包 freeze 嵌入每腿 model/factor/model_code/Alpha158，blob store sha dedup 保持存储复用 |
| F-008 | 同一 `FrozenRuntimeSelfCheckService.assert_manifest_self_contained()` 支持 multi-alpha per-leg strict self-check，不放宽 feature 校验 |
| F-009 | Legacy 父包兼容：忽略 `child_package:` ref，优先 parent self-contained assets，无需重建现存父包 |
| F-010 | 已存在 `pkg_mac_*` 子包仅列后续 retire 步骤，本 PR 不 DML/DDL |
| F-011 | 实现阶段必须提供真实 WSL parity oracle：现有 child-based vs parent-self-contained combined score/topK 逐值一致 |
| F-012 | 单 alpha 路径零回归，子包清理和 single-alpha package 逻辑不混入本 PR |

## Implementation Plan

阶段 0：设计落地

1. 提交本文档。
2. 运行 `rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/multi_alpha_parent_self_contained_runtime_design_20260702.md --tier F2`。
3. Tier2 审核通过后才能进入实现。

阶段 1：runtime parent leg slice

1. 在 `multi_alpha_live.py` 新增 parent leg slice 解析 helper，替换 `_validate_child()` 调用点。
2. 在 `live_inference.py` 扩展 package asset source materialization 支持 leg override 和 cache namespace。
3. 修改 component metadata，去掉 runtime child authority，增加 parent asset metadata。
4. 保留现有 `_normalize_leg_frame()`、`_align_component_frames()`、coverage gate、weight service、`_combine_aligned()`。

阶段 2：self-check 和 dry-run

1. 扩展 `FrozenRuntimeSelfCheckService.assert_manifest_self_contained()`，single-alpha 逻辑不变，multi-alpha dispatch 到 per-leg strict self-check。
2. 更新 `MultiAlphaPaperDryRunValidator` evidence 和 tests，确保 dry-run 走 parent asset source。
3. 增加 fail-closed reason_code 测试。

阶段 3：promotion parent-only

1. 修改 `promote_from_combine_run()`：拒绝 `component_package_ids`，不建不复用 child。
2. 抽取 `LegAssetPlan`，从 combine roster seed provenance 构造每腿 in-memory assets。
3. 父包 manifest 写 parent self-contained lineage、factor/model/runtime assets。
4. 父包 freeze + multi-alpha self-check 成功后保存唯一 parent package；不保存 component child edge。

阶段 4：legacy compatibility 和 cleanup readiness

1. 现存 `pkg_ma_0c796d57d216ebbd1daf0412` 用 parent assets 跑通；legacy `child_package:` ref 仅记录 ignored。
2. 增加只读 cleanup checklist 文档或 PR body 小节，不执行 retire DML。

## Verification Plan

设计阶段已执行或应执行：

```powershell
rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/multi_alpha_parent_self_contained_runtime_design_20260702.md --tier F2
rtk git diff --check
```

实现阶段最低验证：

```powershell
rtk python -m py_compile backend/services/strategy_package/multi_alpha_live.py backend/services/strategy_package/live_inference.py backend/services/strategy_package/frozen_runtime_self_check.py backend/services/strategy_package/multi_alpha_paper_dry_run.py backend/services/strategy_package/multi_alpha_promotion.py
rtk python -m pytest -q backend/tests/strategy_package/test_multi_alpha_live_selection.py backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py -p no:cacheprovider
rtk python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
rtk git diff --check
```

三项杀手测试：

1. 父包自包含出 combined signal：
   - fixture parent manifest 含 2 leg、2 model_asset、factor union、Alpha158 schema。
   - fake repository 对 child package id 调用直接 fail；测试必须证明 provider 未调用 child get。
   - artifact evidence 断言 `model_params_origin=package_asset`、`runtime_source=parent_package_asset`、无 authoritative child dependency。
2. 真实 WSL parity：
   - 目标 parent：`pkg_ma_0c796d57d216ebbd1daf0412`。
   - 只读选择一个两条腿都有有效 score 的 `trade_date`。
   - test-only/debug oracle 显式跑 legacy child-based path；production provider 跑 parent self-contained path。
   - 逐值比较：`instrument`、combined score、rank、topK symbols、每腿 normalized score、weights。
   - 要求完全一致；若出现浮点差异，只允许记录 deterministic tolerance，例如 `abs(diff) <= 1e-12`，并解释来源为 DataFrame 排序/浮点累加，不允许业务差异。
3. Promotion parent-only：
   - 不传 `component_package_ids` 时只新增一个 `alpha_mode=multi_alpha` 父包。
   - 不新增 `alpha_mode=single_alpha` component package，不写 child component edge。
   - 父包 self-check 冻不全时 fail-closed，repository 无 half package。
   - 非空 `component_package_ids` 返回 `multi_alpha_promotion_component_package_ids_unsupported`。

真实 parity debug 命令建议实现为：

```powershell
rtk python debug_tools/strategy_package/multi_alpha_parent_self_contained/compare_parent_vs_legacy_child.py --package-id pkg_ma_0c796d57d216ebbd1daf0412 --trade-date <YYYY-MM-DD> --backend wsl --read-only
```

该 debug tool 只允许放在 `debug_tools/`，必须 fail-fast，输出 score diff sha、topK diff 和 reason_code；不得被生产 provider import。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/multi_alpha_live.py`; `backend/services/strategy_package/live_inference.py` | parent leg slice unit tests; child repository access sentinel | ready | - |
| F-002 | `live_inference.py` cache namespace; `prepare_workspace` namespace support | two-leg same parent package cache isolation test | ready | - |
| F-003 | `multi_alpha_live.py` seed frame replay metadata | parity unit test and real WSL oracle compare | ready | - |
| F-004 | `multi_alpha_paper_dry_run.py`; selection artifact service | dry-run evidence asserts parent package asset origin | ready | - |
| F-005 | `multi_alpha_promotion.py` request handling | promotion tests assert no child creation and explicit child ids rejected | ready | - |
| F-006 | `multi_alpha_promotion.py` manifest builder | manifest snapshot test asserts parent lineage and no runtime child refs | ready | - |
| F-007 | `package_asset_freeze.py`; parent freeze path | asset ledger tests for per-leg model/factor/model_code/Alpha158 | ready | - |
| F-008 | `frozen_runtime_self_check.py` | strict per-leg feature count tests and combined signal smoke | ready | - |
| F-009 | `multi_alpha_live.py` legacy compatibility branch | existing `pkg_ma_0c...` oracle test ignores `child_package:` ref | ready | - |
| F-010 | PR body / follow-up cleanup checklist | read-only reference query plan; no DML in this PR | ready | - |
| F-011 | `debug_tools/strategy_package/...compare_parent_vs_legacy_child.py`; pytest marker or manual Tier2 run | WSL parity evidence attached to PR | ready | - |
| F-012 | single-alpha targeted tests | existing single-alpha StrategyPackage tests unchanged | ready | - |

## Rollout / Rollback

Rollout：

1. 先合入设计文档，Tier2 审核通过后进入实现。
2. 实现 PR 合入前必须提交 parity evidence、promotion parent-only evidence、dry-run parent asset evidence。
3. 合入后不需要 DDL；生产生效需要用户按既有流程重启 backend，Codex 不主动重启。
4. 已存在 parent 包可直接使用新 runtime；已存在 child 包暂不退役。

Rollback：

1. 如果 runtime parent path 出现问题，回滚实现 PR 即恢复旧 child-based runtime；不得在新代码中保留自动 fallback。
2. 已用新 promotion 创建的 parent-only 包保持可审计；若回滚后旧 runtime 不能消费 parent-only 包，应由 eligibility gate fail-closed，而不是伪造 child。
3. 子包 retire 若另案执行，必须有独立 rollback 方案；本 PR 不做 retire，因此无 DML rollback。

## Risks / Failure Modes

| 风险 | 影响 | 缓解 |
|---|---|---|
| factor ref 匹配歧义 | 错腿特征进入模型，产生错误 selection | factor_name/factor_id 精确索引，任何多义 fail-closed |
| cache namespace 冲突 | 第二条腿覆盖第一条腿 workspace | source cache 和 prepared workspace 都加入 safe `leg_id` |
| seed ensemble 行为误解 | parent path 跑一次看似少跑 seed | 记录 frozen representative model replay 语义，并用 child oracle 逐值验证 |
| legacy `child_package:` 被误用 | 子包依赖残留 | runtime 禁止 child repository get；仅 metadata 标记 ignored |
| promotion 自检只测单腿不测组合 | 父包可保存但不能出 combined signal | multi-alpha self-check 包含 per-leg strict check 和 combined signal smoke |
| explicit `component_package_ids` 老客户端继续传 | 再次产生子包心智污染 | 新契约 fail-closed，前端/文档同步移除该字段 |
| 现存 child 包被提前删除 | 审计链断裂 | 本 PR 不退役；后续只走状态机 retire 非硬删 |

## Production Gates

- `production_ddl_gate=noop`：本文档和后续实现设计均不新增 DB DDL；子包退役 DML 不在本 PR。
- `production_frontend_dependency_gate=noop`：本设计不新增前端依赖。
- `production_backend_dependency_gate=noop`：本设计不新增 Python 依赖。
- `production_runtime_gate=pending_user_activation`：实现合入后是否重启 backend 由用户单独授权。
- 本阶段未启动服务、未写生产 DB、未执行 WSL parity、未退役子包。
