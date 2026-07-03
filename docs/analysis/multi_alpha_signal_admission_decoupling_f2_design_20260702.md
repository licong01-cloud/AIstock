# 多Alpha 信号准入与订单生成解耦 F2 设计

任务层级：T3 / F2
模块：StrategyPackage / Selection Artifact / Paper Trading v2 / MiniQMT SIM
阶段：Tier2 已通过，授权实现；实现必须补齐“信号证据廉价持久化，热路径禁止重跑 self-check”
设计日期：2026-07-02
约束：不启动或重启服务；不写生产 DB；无 DDL/DML；不修改 MiniQMT 执行层门；不污染 root main。

## Background / 背景

### 用户定调

本设计采纳用户在战略 session 中给出的分层判定：

- 信号层（StrategyPackage）职责：每日选股 topK、目标权重、隐含淘汰，形成目标组合状态。目标组合状态由信号、topK 与权重策略确定性推出。
- 执行层（Paper / MiniQMT）职责：目标组合 -> 订单差额 -> 执行算法 -> 下单与成交回报。
- 现状越界：当前 `multi_alpha_runtime_not_validated_until_dry_run` 把多 Alpha 父包准入绑定到 `MultiAlphaPaperDryRunValidator`，而该 validator 会经过 `TargetPositionEngine` 与 `RebalanceEngine` 生成订单 preview。这等于用“能否生成订单”卡“信号包能否使用”，混合了信号层和执行层。
- 已证信号有效：用户提供的新完整父包 `pkg_ma_8ec5e389fa2c5e484a1ac7e9` 已经建包 self-check 通过并成功生成 25 只选股结果。信号层准入应验“自包含 + 可产出有效 selection artifact”，而不是订单 dry-run。

### 当前代码证据

- `backend/services/strategy_package/asset_eligibility.py` 定义 `MULTI_ALPHA_PAPER_ADMISSION_BLOCKER = "multi_alpha_runtime_not_validated_until_dry_run"`，并在 `_multi_alpha_runtime_blockers()` 中读取 `source_evidence.multi_alpha.paper_admission.blocking` 与 `MultiAlphaPaperAdmissionRepository.get_eligible()`。
- `backend/services/strategy_package/multi_alpha_promotion.py` 在 promotion 时通过 `_paper_admission()` 写入 `{"eligible": False, "blocking": ["multi_alpha_runtime_not_validated_until_dry_run"]}`，导致新父包出生即带阻断提示。
- `backend/services/strategy_package/multi_alpha_paper_dry_run.py` 的 `MultiAlphaPaperDryRunValidator` 当前会生成 selection artifact，然后构造 signal snapshot、target positions 与 order intents preview；这条链路适合诊断执行预览，不适合作为信号包准入硬门。
- `backend/services/strategy_package/frozen_runtime_self_check.py` 已提供 `FrozenRuntimeSelfCheckService.assert_manifest_self_contained()`，并在 multi-alpha 路径做 parent package asset 与 combined signal smoke 检查。
- `backend/services/strategy_package/selection_artifact.py` 与 `backend/services/strategy_package/multi_alpha_live.py` 已能为 multi-alpha 父包生成 authoritative selection artifact，row 中包含 `symbol`、`score`、`rank`、`target_weight` 与 component scores。
- `backend/services/paper_trading_v2/service.py` 的 `create_portfolio()` 先调用 `asset_eligibility_service.require_eligible()`，随后才进入 broker/data source、broker compatibility、validated execution policy 等执行层门。`create_minqmt_sim_auto_run()` 还保留 `_assert_minqmt_account_accepts_group_slot()` 账户/分仓/slot 约束。

### 为什么必须改

之前 LocalSim 放宽只把 `multi_alpha_runtime_not_validated_until_dry_run` 在 `broker_backend=local_sim` 上解释为 warning，但 `minqmt_sim` 仍被相同信号包卡在订单 dry-run 缺失上。新的架构判定要求：

1. 信号准入不区分 LocalSim / MiniQMT，因为 selection artifact 是 broker-agnostic 的目标组合信号。
2. MiniQMT 的风险必须留在执行层门，不应由信号层 dry-run admission 承担。
3. `multi_alpha_runtime_not_validated_until_dry_run` 作为“订单 dry-run admission 缺失”的 hard blocker 应从信号准入路径删除或降级为 legacy diagnostic。

## Scope / 范围

本设计覆盖以下变更，待 Tier2 通过后实施：

1. 重定义 multi-alpha asset eligibility：准入判据改为“非 retired + manifest identity/hash 硬门通过 + frozen self-check 自包含 + 可产出有效 selection artifact / deterministic combined signal”。
2. 删除 `asset_eligibility` 对 `multi_alpha_runtime_not_validated_until_dry_run` 的订单 dry-run admission 查询依赖，LocalSim 与 MiniQMT SIM 使用同一信号层判据。
3. promotion 不再写入出生即阻断的 `source_evidence.multi_alpha.paper_admission.blocking=[multi_alpha_runtime_not_validated_until_dry_run]`，并在建包 self-check 通过后把信号准入证据持久化到 manifest。
4. 订单生成与订单 preview 移交执行层；`TargetPositionEngine`、`RebalanceEngine`、order intents preview 不再出现在信号准入路径。
5. `paper-runtime-dry-run` endpoint 保留为可选诊断 / 留证工具，但不再是 signal admission 的硬前置。
6. 保留 MiniQMT 执行层门：broker account、account group、strategy slot、validated execution policy、broker/data_source compatibility、MiniQMT 行情/执行边界。
7. 增加 fail-closed reason_code 与测试计划，确保 self-check fail、selection 空、非确定性、未知 blocker 都 loud fail。

## Non-goals / 边界

1. 不弱化 frozen self-check；`assert_manifest_self_contained()` 仍是建包硬门，且真实信号失败必须 fail-closed。
2. eligibility 是 package list / selectable / create 的热路径；热路径禁止重跑完整 `assert_manifest_self_contained()`，禁止物化 workspace、探模型或调用 WSL。
3. 不伪造 admission；不向 `strategy_pkg.multi_alpha_paper_admission` 插入假行，不把 optional dry-run 结果当信号准入权威。
4. 不改 single-alpha 路径；`alpha_mode != multi_alpha` 的 asset eligibility、selection 与 Paper create 语义保持零回归。
5. 不触碰 MiniQMT 执行层门；不修改 `_assert_minqmt_account_accepts_group_slot()`、validated execution policy、broker/data_source compatibility 的安全语义。
6. 不改生产 DB schema；本设计不引入 DDL，也不要求生产 DML/backfill。
7. 不启动、不重启 backend/frontend/TDX/MiniQMT；运行时激活仍由用户拥有。
8. 不更改固化 #1792、因子库保护 #1799、父包自包含 #1819、LocalSim 放宽 #1814 的已合入保护语义；如果实现触及同文件，必须以新信号准入覆盖旧订单 dry-run blocker，而不是回滚既有保护。
9. 不删除 `paper-runtime-dry-run` 代码；是否未来重命名为 diagnostic endpoint 另行设计。

## Architecture / 架构

### 1. 分层后的准入模型

新的 multi-alpha 准入拆成两层：

| 层 | 责任 | 是否属于本次信号准入 |
|---|---|---|
| Asset / manifest hard gates | 非 `RETIRED`、manifest sha 稳定、alpha core shape、protected asset self-check | 是 |
| Signal admission | parent package asset 自包含、combined signal deterministic、selection artifact 可非空产出 | 是 |
| Portfolio construction | selection artifact -> target positions -> order deltas | 否，执行层 |
| Broker execution | execution policy、broker/data_source、MiniQMT account/group/slot、adapter 下单 | 否，执行层 |

核心原则：`asset_eligibility` 只判断这个 frozen multi-alpha package 是否是一个可运行的信号包；它不判断今天是否能生成订单、不判断当前账户是否允许下单、不判断执行算法是否会接受所有订单。

### 2. 新 signal admission 判据

对 `alpha_mode=multi_alpha` 的父包，`StrategyPackageAssetEligibilityService.summarize(..., broker_backend=*)` 应执行同一套 broker-agnostic 信号判据：

1. **生命周期硬门**：`package_status != RETIRED`。
2. **manifest identity/hash 硬门**：当前记录 `manifest_sha256` 与 canonical manifest payload 一致；manifest 必须 frozen。
3. **父包自包含硬门**：满足以下任一可审计证据：
   - 当前 promotion/build 路径已在保存前执行 `FrozenRuntimeSelfCheckService.assert_manifest_self_contained(frozen_assets.manifest)`，且 self-check result 已持久化为 `source_evidence.multi_alpha.signal_admission`；或
   - 存量 legacy 包已有同一 `(package_id, manifest_sha256)` 的 successful selection artifact，可作为“曾成功生成 selection”的廉价持久化证据。
4. **有效 selection 信号硬门**：满足以下任一条件：
   - self-check result 包含 `combined_signal_smoke.schema_version=multi_alpha_parent_combined_signal_smoke_v1`、`leg_count > 0`、`deterministic_replay=True`，证明组合信号可确定性产出；或
   - artifact repository 中存在同一 `(package_id, manifest_sha256)` 的 authoritative selection artifact，`status=SUCCEEDED`、`score_count >= 1`、rows 非空且包含 `symbol/rank/score/target_weight`，metadata 标明 `target_weight_policy` 与 topK。
5. **旧订单 dry-run blocker 归一化**：如果 manifest 仍含 legacy `source_evidence.multi_alpha.paper_admission.blocking=[multi_alpha_runtime_not_validated_until_dry_run]`，该 reason 不再进入 hard blockers；若上述信号判据通过，最多生成 `WARN` / diagnostic context，说明旧订单 dry-run blocker 已被 signal admission superseded。
6. **未知 blocker 不放宽**：任何非 legacy 的 `paper_admission.blocking` reason、manifest hash mismatch、self-check fail、selection 空或非确定性均 hard fail。

### 2.4 热路径信号证据持久化契约（必办）

`asset_eligibility` 是 package list、selectable packages 与 Paper/MiniQMT create 的热路径，不能在每次 eligibility 里重跑完整 frozen self-check。完整 `assert_manifest_self_contained()` 会物化/读取 runtime assets、探测模型，某些路径可能调用 WSL；这些只能发生在建包 build-time hard gate。

新 promotion 保存父包时必须把 build-time self-check 结果压缩为廉价读取的 manifest 证据：

```json
{
  "source_evidence": {
    "multi_alpha": {
      "signal_admission": {
        "schema_version": "multi_alpha_signal_admission_v1",
        "self_check_passed": true,
        "self_check_origin": "package_asset",
        "self_check_manifest_sha256": "<manifest sha before evidence write>",
        "combined_signal_smoke": {
          "schema_version": "multi_alpha_parent_combined_signal_smoke_v1",
          "leg_count": 2,
          "deterministic_replay": true
        },
        "deterministic": true,
        "leg_count": 2,
        "provider_version": "multi_alpha_package_promotion_v1",
        "paper_runtime_dry_run_required": false,
        "persisted_for_hot_path": true
      }
    }
  }
}
```

落地规则：

- promotion 顺序为：构建 manifest -> freeze assets -> build-time self-check hard gate -> 写入 `signal_admission` -> 重新 `freeze_manifest()` -> `save_manifest_with_assets()`；证据字段进入新父包 canonical hash，存量父包不原地改写。
- `signal_admission` 只写在新建 manifest 中；不得对已合父包做生产 DML/backfill，也不得修改既有 manifest JSON/sha。新增字段没有 Pydantic 默认值，不会让 #1792 的旧包 hash 因模型默认注入产生 drift。
- eligibility 优先廉价读取 `source_evidence.multi_alpha.signal_admission`，并仅做 JSON schema、`self_check_passed`、`self_check_origin`、`combined_signal_smoke.schema_version`、`leg_count>0`、`deterministic=true` 检查。
- 若无 `signal_admission`，eligibility 可读取已有 selection artifact 作为已持久化证据；读取失败、artifact 空或缺 deterministic digest 必须 fail-closed。
- 若无 manifest evidence 且无 selection artifact，才允许 legacy structural smoke 兜底；兜底只检查 `source_evidence.authority`、`legs`、component ids、weights 结构，不能物化 workspace、不能探模型、不能跑 WSL、不能调用订单引擎，并在 warning context 中记录 `cost_class=cheap_structural_no_workspace_no_model_probe_no_wsl`。
- 无证据且结构性 smoke 不满足时 fail-closed，reason_code 必须是 `multi_alpha_signal_admission_not_validated` 或更具体 signal reason，不能 silent eligible。

### 3. `multi_alpha_runtime_not_validated_until_dry_run` 替换方案

实现时建议保留旧常量作为 legacy compatibility，但改名或注释为：

```python
LEGACY_MULTI_ALPHA_PAPER_DRY_RUN_BLOCKER = "multi_alpha_runtime_not_validated_until_dry_run"
MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED = "multi_alpha_signal_admission_not_validated"
```

替换规则：

- `asset_eligibility._multi_alpha_runtime_blockers()` 改名为 `_multi_alpha_signal_admission_checks()` 或等价名称。
- 不再调用 `MultiAlphaPaperAdmissionRepository.get_eligible()` 作为 signal admission 条件。
- 遇到 legacy blocker：
  - 若 signal criteria pass：生成 `WARN`，`reason_code=multi_alpha_legacy_paper_dry_run_blocker_superseded`，`eligible=True`。
  - 若 signal criteria fail：返回新的具体 signal failure reason，不再返回订单 dry-run reason。
- 对新 promotion 产物：不再写 legacy blocker。
- 对存量父包：无需生产 DML 修 manifest；eligibility 运行时解释旧 blocker 为 legacy diagnostic，保证 root main 合入后可让 `pkg_ma_8ec5e389fa2c5e484a1ac7e9` 这类自包含且已能选股的父包在 `local_sim` 与 `minqmt_sim` 信号层均 eligible。

### 4. Promotion 不再 born-blocked

`backend/services/strategy_package/multi_alpha_promotion.py` 的变更方向：

1. `_paper_admission()` 不再返回 hard blocker；推荐删除 manifest 中的 `source_evidence.multi_alpha.paper_admission`，或改为非阻断 diagnostic 信息：

```json
{
  "paper_runtime_diagnostics": {
    "schema_version": "multi_alpha_paper_runtime_diagnostics_v1",
    "dry_run_endpoint": "paper-runtime-dry-run",
    "required_for_signal_admission": false,
    "status": "optional"
  }
}
```

2. `MultiAlphaPackagePromotionResult.paper_admission` 为兼容 API 响应可继续存在，但必须表达为非权威、非阻断：

```json
{
  "required_for_signal_admission": false,
  "blocking": [],
  "diagnostic_only": true
}
```

3. `AssetCheck` message 从 “Paper runtime remains blocked until dry-run” 改为 “signal admission is governed by frozen self-check and selection artifact readiness; paper runtime dry-run is optional diagnostic evidence”。
4. promotion 必须把 build-time self-check result 转成 `source_evidence.multi_alpha.signal_admission` 并重新 freeze manifest；eligibility 后续只读该证据，不重跑 self-check。
5. 父包保存流程仍必须先 freeze assets，再跑 self-check，再保存；self-check 失败不得保存 half package。
6. 不持久化假 admission row；optional dry-run 结果若继续写旧 admission table，只能作为诊断证据，不能被 eligibility 当作硬门。

### 5. Selection artifact 是目标组合信号边界

信号层输出以 selection artifact 表达目标组合状态：

- `scores_json` rows：`symbol`、`score`、`rank`、`target_weight`、`component_scores`。
- `metadata`：`target_weight_policy`、`topk`、`combined_score_artifact_sha256`、`component_score_artifact_sha256`、`weight_artifact_sha256`。
- 隐含淘汰：执行层将今日 selection artifact 的目标组合与当前持仓比较；不在信号准入时提前生成 sell/buy order preview。

因此，准入只要求 “能确定性生成非空 selection 信号”，不要求 “能生成至少一条订单”。若当前持仓已经等于目标组合，执行层生成零订单也可能是正确业务状态，不能反向证明信号包不可用。

### 6. 订单生成归执行层

以下逻辑从 signal admission 路径中剥离：

- `StrategyPackageRuntime.build_signal_snapshot()` 可以由 execution / runtime 使用，但 eligibility 不应通过它生成订单 preview。
- `TargetPositionEngine.build_targets()` 属于 target portfolio construction。
- `RebalanceEngine.build_order_intents()` 属于 order delta generation。
- `order_intent_count > 0` 不再是多 Alpha 包 eligible 的条件。
- 执行失败应在 Paper/MiniQMT runtime 现场暴露、隔离和记录；例如 BUG-574 类执行层问题不应回堵 signal admission。

### 7. `paper-runtime-dry-run` 端点去留

本设计建议 **保留但降级**：

- 保留 `POST /strategy-packages/{package_id}/paper-runtime-dry-run` 作为可选诊断 / operator evidence / regression harness。
- 文案改为 “可选订单链路诊断”，不再叫 “准入清门”。
- 若它继续写 `strategy_pkg.multi_alpha_paper_admission`，表名语义暂时作为 legacy，不参与 eligibility 决策；实现报告必须明确 “旧 admission row 不是信号准入权威”。
- MiniQMT 仍可返回 `multi_alpha_paper_dry_run_unsupported_broker_backend`，但该错误只表示 dry-run endpoint 不支持 MiniQMT 订单 preview，不表示 multi-alpha 信号包不 eligible。
- 未来如要重命名为 `paper-runtime-diagnostics` 或迁移表结构，应单独设计，不在本次无 DDL 任务内完成。

### 8. MiniQMT 执行层门保留

Signal admission 对 `broker_backend=local_sim` 与 `broker_backend=minqmt_sim` 输出同一结果，但 MiniQMT 组合创建/运行仍必须经过执行层门：

- `create_minqmt_sim_auto_run()` 要求 `broker_account_id` 非空。
- `_assert_minqmt_account_accepts_group_slot()` 继续检查 active binding、account_group_id、strategy_slot_id，避免账户/slot 冲突。
- `assert_broker_market_source_match("minqmt_sim", MinuteDataSource.MINIQMT_REALTIME)` 继续阻断错误数据源。
- `_resolve_validated_execution_policy()` 继续要求 validated execution policy；不因 signal eligible 自动生成或放宽 execution policy。
- broker compatibility、MiniQMT 行情与 adapter 侧 fail-fast 不变。

换言之，`pkg_ma_8ec5e389...` 在 `minqmt_sim` 上通过的是“信号层准入”，不是“可以绕过账户/slot/执行策略直接下单”。

## Contracts / 契约

### Backend eligibility contract

| 场景 | 新结果 | 说明 |
|---|---|---|
| multi-alpha, self-check pass, selection signal pass, legacy dry-run blocker only | `eligible=True`, optional warning | LocalSim/MiniQMT 同判据 |
| multi-alpha, no legacy blocker, self-check pass, selection signal pass | `eligible=True` | 新 promotion 默认状态 |
| multi-alpha, self-check fail | `eligible=False`, hard blocker | 不写假成功 |
| multi-alpha, selection artifact empty / smoke empty | `eligible=False`, hard blocker | fail-closed |
| multi-alpha, selection replay non-deterministic | `eligible=False`, hard blocker | fail-closed |
| multi-alpha, unknown `paper_admission.blocking` reason | `eligible=False`, hard blocker | 不放宽未知策略 |
| single-alpha | 保持现状 | 不读取 multi-alpha signal admission |
| retired / manifest hash mismatch | `eligible=False`, hard blocker | 既有硬门保留 |

### Reason codes

| reason_code | severity | 语义 |
|---|---:|---|
| `multi_alpha_signal_admission_passed` | pass | 父包通过信号准入 |
| `multi_alpha_signal_self_check_passed` | pass | frozen self-check / package_asset origin 通过 |
| `multi_alpha_selection_artifact_available` | pass | 存在可用 authoritative selection artifact 或 combined smoke |
| `multi_alpha_legacy_paper_dry_run_blocker_superseded` | warning | 旧订单 dry-run blocker 被信号准入替代 |
| `multi_alpha_signal_admission_not_validated` | hard | 缺少足够 self-check / selection 证据 |
| `multi_alpha_signal_self_check_failed` | hard | self-check 报错或 origin 非 package_asset |
| `multi_alpha_signal_selection_artifact_empty` | hard | selection artifact / combined smoke 为空 |
| `multi_alpha_signal_selection_artifact_nondeterministic` | hard | replay 或 deterministic check 不一致 |
| `multi_alpha_signal_selection_artifact_unavailable` | hard | 无法取得 artifact 且无法轻量复验 |
| `multi_alpha_signal_unknown_manifest_blocker` | hard | 存在未知 blocking reason |
| `multi_alpha_signal_evidence_missing` | warning | 存量包缺少新持久化 signal evidence，使用廉价结构性 smoke 兜底 |

实现时可复用现有更具体的 self-check reason，例如 `strategy_package_frozen_self_check_origin_not_package_asset`、`multi_alpha_promotion_parent_self_check_failed`、`multi_alpha_promotion_parent_combined_signal_failed`，但不得吞掉 context。

### API / UI contract

- package summary、selectable packages、Paper create preflight 均以新的 `asset_eligibility` 为准。
- 若 API 返回 legacy dry-run warning，UI 应展示为“订单 dry-run 可选诊断”，不显示为阻断。
- 对 MiniQMT：UI 可以显示“信号层已 eligible；执行层仍需 broker account / slot / execution policy / data source 通过”。
- `paper-runtime-dry-run` 返回值保持兼容，但 UI 文案必须去除“必须先 dry-run 才能准入”的表达。

### DB contract

- 无 DDL。
- 无生产 DML/backfill。
- 不要求更新存量 manifest JSON。
- 新父包的 `source_evidence.multi_alpha.signal_admission` 是 manifest 内构建期证据；它只随新包创建写入，不对旧包做 DML 修补。
- `strategy_pkg.multi_alpha_paper_admission` 若继续存在，只作为 legacy optional diagnostic evidence，不作为 signal admission source of truth。

## Design Acceptance Index / 设计验收索引

| ID | 设计项 |
|---|---|
| F-001 | `asset_eligibility` 将 multi-alpha 准入改为 signal admission：self-contained + valid selection signal |
| F-002 | 删除对 `multi_alpha_runtime_not_validated_until_dry_run` 订单 dry-run admission 查询的硬依赖 |
| F-003 | LocalSim 与 MiniQMT SIM 使用同一信号层判据；MiniQMT 执行层门另行保留 |
| F-004 | Promotion 不再写 born-blocked `paper_admission.blocking=[multi_alpha_runtime_not_validated_until_dry_run]` |
| F-005 | 订单生成、target/rebalance/order preview 不在信号准入路径执行 |
| F-006 | `paper-runtime-dry-run` 降级为可选诊断 / 留证工具，不再是准入硬前置 |
| F-007 | Self-check 不弱化：build-time self-check 仍 hard gate，失败不保存父包或不放行 |
| F-008 | Selection artifact 失败、空结果或非确定性必须 fail-closed 并返回具体 reason_code/context |
| F-009 | Single-alpha 路径零回归 |
| F-010 | 测试覆盖 killer cases：已自包含且能选股的父包 local_sim/minqmt_sim 均 eligible，无 dry-run 依赖 |
| F-011 | 保留已合入 #1792/#1799/#1819/#1814 语义，不回滚保护 |
| F-012 | 无 DDL/DML、无服务启动/重启，生产激活仍用户拥有 |
| F-013 | 信号有效判据必须走廉价持久化证据；eligibility 热路径禁止重跑完整 self-check、探模型、物化 workspace 或调用 WSL |

## Implementation Plan / 实施方案

### Phase 0：实现前保护与现状锁定

1. 新增或调整测试先描述新语义，确保旧 `multi_alpha_runtime_not_validated_until_dry_run` 不再作为 hard blocker。
2. 在测试中注入一个会抛错的 admission_reader，断言 `asset_eligibility` 不再查询 paper admission 来决定 signal eligibility。
3. 对 `TargetPositionEngine` / `RebalanceEngine` 做静态或 monkeypatch guard，确保 eligibility 不调用订单链路。
4. 对 `FrozenRuntimeSelfCheckService.assert_manifest_self_contained`、workspace asset resolver、WSL/model probe 做 monkeypatch guard，证明 eligibility 热路径只读持久化证据。

### Phase 1：重写 asset eligibility signal admission

1. 将 `_multi_alpha_runtime_blockers()` 改为 `_multi_alpha_signal_admission_checks()`。
2. legacy paper dry-run blocker 只做兼容归一化，不再查询 `MultiAlphaPaperAdmissionRepository`。
3. 引入只读 signal evidence evaluator：
   - 优先读取 `source_evidence.multi_alpha.signal_admission`；
   - 其次可选读取最近 successful selection artifact；
   - 最后才允许 bounded structural smoke fallback，只做 JSON/manifest 结构检查，不得调用完整 self-check / workspace / model probe / WSL。
4. Failures 使用新 reason_code；未知 blocker、manifest/hash/self-check 失败保持 hard。
5. 更新 `repository.py` summary SQL / summary helper，不再把 legacy dry-run blocker 计入 summary hard blockers 或 admission lookup warning。

### Phase 2：promotion 去 born-blocked

1. 修改 `_paper_admission()` 或调用点，停止写入 hard `blocking`。
2. API response 保留兼容字段时标记 `diagnostic_only=true` 与 `required_for_signal_admission=false`。
3. self-check result 通过 `_signal_admission_evidence()` 持久化到 manifest `source_evidence.multi_alpha.signal_admission`，字段走 canonical manifest hash；旧包不改 JSON/sha。
4. `AssetCheck` message 更新为 signal admission 语义。
5. 父包 freeze + self-check + signal evidence + save 顺序不放宽；self-check failure 继续无 half package。

### Phase 3：dry-run 降级与文案

1. 保留 `MultiAlphaPaperDryRunValidator` 与 endpoint 代码。
2. 文档、UI、API helper 文案改为 optional diagnostic，不再描述为准入硬前置。
3. 若 dry-run 失败，仍展示真实 reason_code；不得伪造成 admission success。
4. 不修改 MiniQMT unsupported dry-run behavior，但不得让该 behavior 影响 signal eligibility。

### Phase 4：测试与静态验证

1. 后端 targeted tests 覆盖 asset_eligibility、promotion、repository summary、Paper create_portfolio、MiniQMT execution gates。
2. `python scripts/aistock_feature_workflow.py validate --design <本文件> --tier F2` 必须通过。
3. `git diff --check` 必须通过。
4. 若实现修改 Python，按范围运行 compileall 与 targeted pytest；本设计阶段不运行服务。

## Verification Plan / 验证方案

### Killer tests

1. **完整父包 signal eligible**：构造或读取与 `pkg_ma_8ec5e389fa2c5e484a1ac7e9` 等价的 multi-alpha parent，具备 frozen self-check pass 与 non-empty selection artifact；断言：
   - `summarize(record, broker_backend="local_sim").eligible is True`；
   - `summarize(record, broker_backend="minqmt_sim").eligible is True`；
   - admission_reader 未被调用；
   - blockers 不含 `multi_alpha_runtime_not_validated_until_dry_run`。
2. **self-check fail**：模拟 `assert_manifest_self_contained()` 抛 `multi_alpha_promotion_parent_self_check_failed` 或 origin 非 package_asset；断言 fail-closed，reason_code 原样透传或包装为 `multi_alpha_signal_self_check_failed`。
3. **selection empty**：selection smoke / artifact `score_count=0` 或 rows 为空；断言 `eligible=False`，reason_code=`multi_alpha_signal_selection_artifact_empty`。
4. **selection non-deterministic**：两次 smoke / artifact digest 不一致；断言 `eligible=False`，reason_code=`multi_alpha_signal_selection_artifact_nondeterministic`。
5. **unknown blocker**：manifest 中有 `blocking=["unsupported_multi_alpha_policy"]`；LocalSim 与 MiniQMT 都 hard fail，不被新逻辑吞掉。
6. **single-alpha zero regression**：现有 single-alpha create_portfolio、selection artifact、asset eligibility tests 不变。
7. **MiniQMT execution gate preserved**：signal eligible 的 multi-alpha 父包进入 `create_minqmt_sim_auto_run()` 时，如果缺 `broker_account_id`、account/group/slot 冲突、data_source 错配、validated execution policy 缺失，仍由执行层 typed error 阻断。
8. **no order dry-run dependency**：在 asset eligibility 单测中 monkeypatch `TargetPositionEngine` / `RebalanceEngine` / `MultiAlphaPaperDryRunValidator` 为 raise，signal eligibility 仍可通过。
9. **promotion born eligible**：新 promotion response 不含 hard paper_admission blocker；manifest source_evidence 不写 legacy blocker；package status 仍是 `ASSET_VALIDATED` 而不是自动 `PAPER_ENABLED`。
10. **summary path 对齐**：package list / selectable packages summary 不因 legacy dry-run blocker 过滤掉 signal eligible multi-alpha 父包。
11. **hot path no self-check**：asset eligibility 中 monkeypatch full self-check、workspace materialization、model probe、WSL probe 与订单引擎为 raise；有持久化 signal evidence 的父包仍 eligible。
12. **manifest hash no drift**：新增字段只在新 promotion manifest 中显式写入；存量 manifest canonical hash 不因 Pydantic 默认注入 drift，至少覆盖 15 包 integrity scan 或等价 regression fixture。

### Suggested commands after implementation

```powershell
rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/multi_alpha_signal_admission_decoupling_f2_design_20260702.md --tier F2
rtk python -m compileall backend/services/strategy_package backend/services/paper_trading_v2 backend/routers/strategy_packages.py
rtk python -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py backend/tests/strategy_package/test_repository_service.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider
rtk git diff --check
```

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/asset_eligibility.py::_multi_alpha_signal_admission_checks` | Killer tests 1-4 validate self-contained + non-empty deterministic selection signal | ready | 无 |
| F-002 | `asset_eligibility.py`; remove `MultiAlphaPaperAdmissionRepository.get_eligible` from signal path | admission_reader raising test proves no dry-run admission dependency | ready | 无 |
| F-003 | `asset_eligibility.py`; `paper_trading_v2/service.py` | local_sim and minqmt_sim eligibility both pass signal layer; MiniQMT execution gate tests still fail when account/slot/policy invalid | ready | 无 |
| F-004 | `backend/services/strategy_package/multi_alpha_promotion.py::_paper_admission` and source_evidence build | promotion test asserts no legacy hard blocker in new parent manifest/response | ready | 无 |
| F-005 | `asset_eligibility.py` imports/calls; no target/rebalance in signal evaluator | monkeypatch/static guard prevents `TargetPositionEngine` / `RebalanceEngine` / dry-run validator use during eligibility | ready | 无 |
| F-006 | `backend/routers/strategy_packages.py`; `multi_alpha_paper_dry_run.py`; frontend copy if touched | endpoint still works as optional diagnostic; failing dry-run no longer blocks asset eligibility | ready | 无 |
| F-007 | `frozen_runtime_self_check.py`; promotion save flow | self-check failure test proves no half package and no eligibility pass | ready | 无 |
| F-008 | signal evaluator reason_code map | empty/non-deterministic selection tests assert fail-closed with context | ready | 无 |
| F-009 | existing single-alpha tests | single-alpha create/selection/eligibility tests unchanged and pass | ready | 无 |
| F-010 | targeted fixture or read-only integration fixture for `pkg_ma_8ec5e389...` semantics | self-contained + 25-selection equivalent parent is eligible for both local_sim and minqmt_sim without dry-run | ready | 无 |
| F-011 | touched files overlapping #1792/#1799/#1819/#1814 | regression tests for self-check, factor protection, parent package assets, LocalSim warning compatibility | ready | 无 |
| F-012 | no migrations/dependency files; no runtime commands | `git diff --check`; final report declares production gates noop and service_restart not performed | ready | 无 |
| F-013 | `multi_alpha_promotion.py::_with_signal_admission_evidence`; `asset_eligibility.py::_checks_from_persisted_signal_evidence` | tests assert persisted evidence is cheap-readable and eligibility does not call full self-check/workspace/model/WSL/order engines; hash drift fixture remains stable | ready | 无 |

## Rollout / Rollback / 发布回滚

### Rollout

1. 合入实现后，存量 multi-alpha 父包无需 DML 修改；legacy dry-run blocker 在 runtime eligibility 中被 signal admission supersede。
2. 新 promotion 父包不再 born-blocked，但仍只是 `ASSET_VALIDATED`；是否创建 Paper portfolio 仍由用户/API 操作触发。
3. Optional dry-run 端点继续可用于留证，不参与准入 hard gate。
4. MiniQMT 执行层仍需要独立账号/slot/policy/数据源验证；signal eligible 不等于 live activation。

### Rollback

1. 代码回滚即可恢复旧订单 dry-run hard gate；无 DB schema 需要回滚。
2. 如果 optional dry-run 端点出现问题，可停止调用该 endpoint；signal eligibility 仍由 self-check + selection signal 保护。
3. 若发现 signal evaluator 放宽过度，应先把 new signal reason fail-closed，再单独评估是否恢复 legacy blocker；不得通过写假 admission 补洞。

## Risks / Failure Modes / 风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| 把 legacy blocker 全局删除导致未知失败被吞 | 假 eligible | 只 supersede 精确 reason `multi_alpha_runtime_not_validated_until_dry_run`，其它 blocker hard fail |
| self-check 结果未持久化导致 eligibility 难判 | 存量包无法安全放行或列表变慢 | 新 promotion 持久化 `signal_admission`；存量包优先读 selection artifact，最后只做廉价 structural smoke，不能热路径重跑 full self-check |
| selection artifact 空但 self-check smoke 通过 | 信号包无法实际选股 | 有 successful selection artifact 时检查 `score_count>=1`；无 artifact 时 combined smoke 必须 non-empty deterministic，且实现可要求一次 selection smoke |
| order preview 从准入移除后执行层问题暴露更晚 | create/run 时失败 | 明确执行层 fail-fast；MiniQMT account/slot/policy/data source gates 保留并测试 |
| dry-run endpoint 名称继续含 admission 语义 | 操作员误解 | UI/API/docs 文案改为 optional diagnostic；结果不再作为 hard admission |
| MiniQMT 被误认为“信号 eligible 即可下单” | 绕过执行安全 | 报告和 UI 明确拆分信号层 eligible 与执行层 gates；测试执行层 blockers |
| 单 alpha 被误改 | 生产回归 | `alpha_mode != multi_alpha` 早返回；single-alpha regression tests |

## Production Gates / 生产门禁

- `production_ddl_gate=noop`：本设计及后续实现不新增/修改 DB schema，不执行 DDL。
- `production_dml_gate=noop`：不要求生产 backfill，不修改存量 manifest JSON 或 admission rows。
- `production_frontend_dependency_gate=noop`：本设计不修改 frontend dependency；若后续仅改 copy/API helper也不改 dependency。
- `production_backend_dependency_gate=noop`：本设计不修改 Python dependency。
- `service_restart=not_performed`：不启动、不停止、不重启 backend/frontend/TDX/MiniQMT；合入后的运行时激活由用户确认。
- `protected_asset_mutation=none`：不修改 frozen manifest/model/selection artifact 文件；实现只改代码语义和测试。
