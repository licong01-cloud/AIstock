# StrategyPackage manifest_sha256 完整性修复 F2 设计

- 文档类型: F2 生产关键数据完整性修复设计
- 日期: 2026-06-30
- 分支: `fix/strategy-package-manifest-sha-integrity-20260630`
- Worktree: `F:\Dev\AIstock_worktrees\strategy-package-manifest-sha-integrity-20260630`
- 同步基线: `origin/main` = `02840dc7`
- 生产写入状态: 本设计与本 PR 不执行生产 DML；生产 hash repair 仅提供 runbook，等待用户单独授权窗口。

## 1. Background / 背景与第 0 步取证定性

### 1.1 问题

生产 `strategy_pkg.package` 共 15 个包，后端同一路径 `compute_manifest_sha256(record.current_manifest())` 复算后只有 5 个匹配，10 个 `manifest_sha256` 与当前 canonical manifest 不一致。该不一致会让多 Alpha S1 自动复用 single-alpha component 包时触发 child sha 校验失败，例如 `pkg_b4ce634c24bd470fac2c7b581a4e106f` 的 stored hash 为 `19b02fa4...`，当前 computed hash 为 `117e7f2b...`。

本任务必须先区分两类成因：

- A 类: `manifest_json` 未被改脏，只有 hash 因 schema/canonicalization 演进而陈旧；可使用现有 `repair_manifest_hash()` 回填 hash。
- B 类: `manifest_json` 已被改脏或无法证明未改脏；禁止自动重算 hash，以免把脏 JSON 固化为合法。

### 1.2 第 0 步取证方法

本轮只读生产 DB 和 git 历史，未执行生产 DDL/DML。证据文件位于 ignored debug 目录，PR/验证记录仅引用摘要，不提交生产敏感 payload：

- `debug_tools/strategy_package/20260630_manifest_sha_integrity/prod_manifest_sha_integrity_snapshot.json`
- `debug_tools/strategy_package/20260630_manifest_sha_integrity/prod_manifest_sha_integrity_table.json`
- `debug_tools/strategy_package/20260630_manifest_sha_integrity/prod_manifest_hash_repair_dry_run.json`

取证逻辑：

1. 读生产 `.env` 的 `TDX_DB_*` 连接生产 DB，只读扫描 `strategy_pkg.package` / `strategy_pkg.package_status_event` / runtime refs。
2. 对每个包计算三类 hash：stored、computed、raw persisted。raw persisted 对 DB 中 `manifest_json` 原样复制，仅 neutralize `manifest_sha256/package_status` 后 hash，不注入 Pydantic default。
3. 对比 `package_status_event` 时间线：`package_created` 事件 context 中的 `manifest_sha256` 与 stored/embedded 一致。
4. 查 git 历史与写入路径：`10150104 fix(strategy-package): enforce alpha-core manifest boundary` 给 `StrategyPackageManifest` 增加默认 `source_evidence={}` 与 `backtest_context={}`；grep 当前 StrategyPackage 代码未发现 `UPDATE strategy_pkg.package SET manifest_json` 路径，正式创建/改 manifest 路径均经 `freeze_manifest()`。

### 1.3 生产现状摘要

- 总包数: 15。
- clean: 5。
- mismatch: 10。
- mismatch status 分布: `BACKTEST_APPROVED=5`、`SELECTION_ENABLED=4`、`PAPER_ENABLED=1`。
- mismatch alpha_mode: 全部 `single_alpha`。
- mismatch source_type 分布: `qe_experiment=3`、`qe_evolution_loop=5`、`candidate_strategy_package=2`。
- 所有 10 个 mismatch 均 `stored == embedded_manifest_sha256 == raw_current_excluding_status_sha256`，且 `manifest_json` 缺失当前模型默认键 `source_evidence/backtest_context`；没有 B 类脏 JSON 证据。

### 1.4 每个坏包 A/B 定性

| package_id | status | source_type | stored short | computed short | 关键证据 | class | 处置 |
|---|---|---|---|---|---|---|---|
| `pkg_b4ce634c24bd470fac2c7b581a4e106f` | BACKTEST_APPROVED | qe_evolution_loop | `19b02fa41435` | `117e7f2b057a` | stored=embedded=raw persisted；缺 `source_evidence/backtest_context`；event `package_created` 记录 stored | A_schema_evolution_stale_hash | 可 repair hash |
| `pkg_95523262439644e49ae52f9b5087165d` | BACKTEST_APPROVED | qe_evolution_loop | `75b15de12708` | `38b7f45f6c6b` | stored=embedded=raw persisted；缺默认键；event 无 JSON 改写证据 | A_schema_evolution_stale_hash | 可 repair hash |
| `pkg_cfa3c5b4068d4db1ad06db352bfece93` | SELECTION_ENABLED | qe_evolution_loop | `b31877525700` | `82c3a97f16dd` | stored=embedded=raw persisted；`enable_selection` 只改 status，hash 忽略 status | A_schema_evolution_stale_hash | 可 repair hash |
| `pkg_2563063e544f4d1fa601e740d019f8c7` | BACKTEST_APPROVED | qe_evolution_loop | `f0ad585c2fb9` | `f03d3a46b7e3` | stored=embedded=raw persisted；缺默认键；event 无 JSON 改写证据 | A_schema_evolution_stale_hash | 可 repair hash |
| `pkg_b2faccade8d549af9621c51d285bdc06` | BACKTEST_APPROVED | candidate_strategy_package | `47d3c100def0` | `18df83186fb9` | stored=embedded=raw persisted；缺默认键；event 无 JSON 改写证据 | A_schema_evolution_stale_hash | 可 repair hash |
| `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27` | PAPER_ENABLED | candidate_strategy_package | `be65b8f24c83` | `c9e010ef9a86` | stored=embedded=raw persisted；synthetic evidence event 不改 manifest_json | A_schema_evolution_stale_hash | 可 repair hash |
| `pkg_1de32357724a4c5b874f2abd90f22da5` | BACKTEST_APPROVED | qe_evolution_loop | `ad337eced2f4` | `1a0b25390e69` | stored=embedded=raw persisted；缺默认键；event 无 JSON 改写证据 | A_schema_evolution_stale_hash | 可 repair hash |
| `pkg_99142cb1440c40a7824e83902f4e7da9` | SELECTION_ENABLED | qe_experiment | `38874a0282cc` | `4a92be98b2ec` | stored=embedded=raw persisted；`enable_selection` event 只改 status | A_schema_evolution_stale_hash | 可 repair hash |
| `pkg_006a42323f7c4e81a468fdaad2cb16a3` | SELECTION_ENABLED | qe_experiment | `f0512fae9ea5` | `18b0a2ea02a7` | stored=embedded=raw persisted；`enable_selection` event 只改 status | A_schema_evolution_stale_hash | 可 repair hash |
| `pkg_b668f8a633c44b72a5d557a2cb8970e3` | SELECTION_ENABLED | qe_experiment | `3e9cd8e0a63c` | `5f043c3fd72b` | stored=embedded=raw persisted；147 paper portfolio events 不改 manifest_json | A_schema_evolution_stale_hash | 可 repair hash |

### 1.5 根因结论

根因不是当前代码有一条“改 `manifest_json` 但不改 hash”的现行写入路径，而是历史 package 在 `10150104` 之前按旧 manifest schema 冻结；当前 Pydantic model canonicalization 自动注入 `source_evidence={}` 与 `backtest_context={}`，导致后端当前 `compute_manifest_sha256(record.current_manifest())` 与旧 hash 不同。由于生产坏包的 stored hash 仍匹配 DB 中 raw persisted `manifest_json`，本轮定性为 A 类 schema evolution stale hash。
## 2. Scope / 范围

本 F2 修复覆盖：

- 增加 raw persisted `manifest_json` hash 计算与 A/B drift classifier。
- `validate_manifest_integrity()` 输出 repair plan，并显式区分 A 类可修复与 B 类隔离。
- `repair_manifest_hash()` 只允许 A 类，且继续要求 operator 同时确认 stored/computed hash。
- 修复 `StrategyPackageRecord.current_manifest()`，让 API/业务返回的 manifest 内嵌 `manifest_sha256` 与 DB 列一致。
- 增加正式 repair helper 脚本，默认只读 dry-run；生产 apply 需要双重确认，dev/scratch apply 只能指向显式 dev DB。
- 在 scratch/dev DB 复现 hash drift、repair、校验和幂等。

## 3. Non-goals / 非目标与边界

- 不启动/重启 backend、frontend、TDX、scheduler 或生产服务。
- 不执行生产 DDL；本任务无 schema/migration 变更。
- 不在本 PR 执行生产 DML；生产 repair 需要用户后续单独授权。
- 不自动修复 B 类或未知类 drift；B 类只能报告并交由用户决策恢复原 JSON 或重建包。
- 不修改 `backend/services/strategy_package/multi_alpha_promotion.py`、`models.py` 的 `SourceType` enum、combine-backtest 前端。
- 不改变 PaperPortfolio 单 `package_id` 契约。

## 4. Architecture / 架构

### 4.1 修复策略

```mermaid
flowchart TD
    A[Scan strategy_pkg.package] --> B[Validate manifest_json with current model]
    B -->|valid| C[computed = compute_manifest_sha256(record.current_manifest())]
    B -->|invalid| Q[Class B invalid or unknown: quarantine_manual_review]
    C -->|stored == computed| OK[clean]
    C -->|stored != computed| D[raw_hash = hash raw manifest_json with status/hash neutralized]
    D --> E{stored == raw_hash and embedded == stored?}
    E -->|yes| R[Class A schema evolution stale hash]
    E -->|no| Q2[Class B dirty or unknown]
    R --> F[repair_manifest_hash allowed only with exact stored/computed confirmation]
    Q --> X[block automatic repair]
    Q2 --> X
```

### 4.2 A/B classifier

- A 类条件必须同时满足：
  - `stored_sha256 != computed_sha256`。
  - `stored_sha256 == compute_manifest_json_sha256(raw manifest_json)`。
  - `manifest_json.manifest_sha256 == stored_sha256`。
  - 当前模型能 validate `manifest_json`，并能产生 computed hash。
- B 类条件：任一 A 类条件不满足，或 `manifest_json` 不是 mapping/当前模型无法 validate。
- B 类输出 `repair_allowed=false`、`recommended_action=quarantine_manual_review`，不会调用 UPDATE。

### 4.3 写入边界

- 正式 StrategyPackage manifest 写入继续只允许 `save_manifest()`，该方法先 `freeze_manifest()` 再 `INSERT strategy_pkg.package`。
- `transition_status()` 只改 `package_status`；`compute_manifest_sha256()` 已 neutralize `package_status`，生命周期变化不应改变 hash。
- `repair_manifest_hash()` 只改 `strategy_pkg.package.manifest_sha256` 和插入 `package_status_event(reason='manifest_hash_repaired')`；不修改 `manifest_json`。

## 5. Contracts / API、DB、脚本契约

### 5.1 后端契约

- `compute_manifest_json_sha256(manifest_json)`: 对 raw persisted JSON 计算 hash，不注入模型默认值。
- `classify_manifest_hash_drift(...)`: 返回 `classification`、`repair_allowed`、`reason`、raw/embedded/hash 对比、缺失默认键。
- `validate_manifest_integrity(limit=...)`: read-only report；每个 drift row 包含 `repair_plan`。
- `repair_manifest_hash(package_id, confirm_stored_sha256, confirm_computed_sha256)`: 只有 `A_schema_evolution_stale_hash` 可写；确认值不匹配或分类非 A 时抛 `InvalidStateTransitionError`，context 带 package_id/hash/repair_plan。

### 5.2 脚本契约

`scripts/strategy_package_manifest_hash_repair.py`:

- 默认 dry-run，只读扫描，不写 DB。
- `--target-db prod` 使用 `TDX_DB_*`；apply 必须同时满足：`--apply --confirm-production-dml` 和 `STRATEGY_PACKAGE_MANIFEST_HASH_REPAIR_APPLY=I_UNDERSTAND_PRODUCTION_DML`。
- `--target-db dev` 使用 `TDX_DB_DEV_*`；apply 必须 `--confirm-scratch-dml`，且 host 必须是 `127.0.0.1/localhost`，dbname 必须包含 `dev/scratch/test`。
- 如果报告中存在 B 类/blocked drift，apply 整体失败，不做部分静默修复。

### 5.3 DB 契约

- 本任务无 DDL，不新增表/列/约束。
- A 类生产 repair 只更新 `strategy_pkg.package.manifest_sha256`，并插入 `strategy_pkg.package_status_event` 审计行。
- rollback 是 DML 级别：按 repair event context 中 `rollback_restore.restore_value` 把 hash 恢复到旧值，并插入人工 rollback 审计；该操作也必须单独授权。
## 6. Design Acceptance Index

- F-001: 第 0 步必须完成生产只读 DB、事件审计、git 历史、写入路径 grep，并逐包给出 A/B 定性。
- F-002: 必须实现 raw persisted JSON hash，不把 Pydantic 默认值注入 raw hash。
- F-003: 必须实现 A/B classifier；B/invalid/unknown drift 不允许自动 repair。
- F-004: `validate_manifest_integrity()` 必须输出 classification 与 repair plan，且不写 DB。
- F-005: `repair_manifest_hash()` 必须只允许 A 类，并要求 exact stored/computed confirmation。
- F-006: repair audit event 必须记录 operator、old/new hash、classification、rollback_restore。
- F-007: `current_manifest()` 必须返回与 DB 列一致的 `manifest_sha256`，避免 repair 后 API 继续暴露旧 embedded hash。
- F-008: helper 脚本必须默认 dry-run；生产 apply 必须双重确认；dev/scratch apply 必须限制到显式非生产 DB。
- F-009: scratch/dev DB 必须复现 drift -> repair -> 全量校验 -> 幂等。
- F-010: 生产 runbook 必须明确 A 类自动修、B 类拦截、rollback 与待用户授权。
- F-011: 不执行生产 DDL/DML，不启动/重启服务，不触碰 S1 禁区。
- F-012: 回归测试覆盖 classifier、幂等 repair、B 类阻断、router 错误映射、脚本 gate。

## 7. Implementation Plan / 实施方案

1. 设计文档落 `docs/architecture/strategy_package_manifest_sha_integrity_repair_design_20260630.md`，执行 F2 validate。
2. 在 `manifest.py` 增加 `compute_manifest_json_sha256()` 与 `classify_manifest_hash_drift()`。
3. 在 `repository.py`：
   - `current_manifest()` overlay DB `manifest_sha256`。
   - `_manifest_drift_repair_plan()` 接入 classifier。
   - `validate_manifest_integrity()` 输出 classification/repair plan。
   - `repair_manifest_hash()` 非 A 类直接抛错，A 类仍需 exact confirmation。
4. 增加 `scripts/strategy_package_manifest_hash_repair.py`。
5. 增加/更新单测覆盖 L1 场景。
6. 在 `aistock_dev` scratch/dev DB 执行复现、repair、校验和幂等，并记录验证历史。
7. PR body 填第 0 步结论、验证证据、生产 repair runbook、生产门禁和 Tier2 终审请求。

## 8. Verification Plan / 验证计划

- L0:
  - `rtk python -m compileall -q backend/services/strategy_package backend/routers scripts`
  - `rtk git diff --check`
  - `rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_manifest_sha_integrity_repair_design_20260630.md --tier F2`
- L1:
  - targeted pytest for repository/router/script hash repair tests。
  - changed module tests under `backend/tests/strategy_package`。
- L2 scratch/dev DB:
  - `--target-db dev` dry-run before。
  - scratch prefix reproduce drift。
  - `--target-db dev --apply --confirm-scratch-dml --package-id-prefix <scratch_prefix>`。
  - rerun apply for idempotency。
- Production read-only:
  - `--target-db prod` dry-run: expect 15 scanned, 10 A-class repairable, 0 blocked。
- Business oracle:
  - 10 production mismatches are A-class and repairable by policy。
  - B-class fixture blocks repair and returns explicit reason/context。
  - After scratch repair, integrity drift for scratch prefix is 0 and second apply repairs 0 rows。

### 8.1 Verification Results / Current Verification Results

Implemented verification results:

- F2 workflow: `rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_manifest_sha_integrity_repair_design_20260630.md --tier F2` -> `PASS, design_items=12, matrix_rows=12`.
- Targeted L1: `rtk python -m pytest backend/tests/strategy_package/test_repository_service.py::test_validate_manifest_integrity_classifies_safe_schema_evolution_drift backend/tests/strategy_package/test_repository_service.py::test_validate_manifest_integrity_blocks_dirty_manifest_json_repair backend/tests/strategy_package/test_repository_service.py::test_validate_manifest_integrity_blocks_invalid_manifest_json_repair backend/tests/strategy_package/test_repository_service.py::test_repair_manifest_hash_fixes_a_class_drift backend/tests/strategy_package/test_repository_service.py::test_repair_manifest_hash_requires_explicit_confirmation backend/tests/strategy_package/test_manifest_integrity_router.py backend/tests/scripts/test_strategy_package_manifest_hash_repair.py -q` -> `19 passed`.
- Production read-only dry-run: `rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db prod --limit 500 --output debug_tools/strategy_package/20260630_manifest_sha_integrity/prod_manifest_hash_repair_dry_run.json` -> `total_scanned=15, clean_count=5, drifted_count=10, filtered_drifted_count=10, repairable_count=10, blocked_count=0`.
- Dev DB pre-check: `rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db dev --limit 500 --output debug_tools/strategy_package/20260630_manifest_sha_integrity/dev_manifest_hash_repair_dry_run_before.json` -> `target_db=dev, total_scanned=4, repairable_count=4, blocked_count=0`; this is only a non-production baseline.
- Scratch B-class guard: after inserting `scratch_manifest_sha_20260630_a_repairable` and `scratch_manifest_sha_20260630_b_blocked`, `--target-db dev --package-id-prefix scratch_manifest_sha_20260630_` dry-run -> `repairable_count=1, blocked_count=1`; apply is blocked by `non-repairable drift exists`, proving B-class is never auto-repaired.
- Scratch production-10 replay: after inserting 10 `scratch_manifest_sha_20260630_prod10_*` A-class drift rows, dry-run -> `filtered_drifted_count=10, repairable_count=10, blocked_count=0`; apply -> `repaired_count=10, after_filtered_drifted_count=0`; second apply -> `repaired_count=0, after_filtered_drifted_count=0`.

## 9. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §1.2-§1.5; `debug_tools/strategy_package/20260630_manifest_sha_integrity/*`; git commits `10150104`/`2d7c4d38`; grep of StrategyPackage write paths | 生产只读 snapshot: 15 total/5 clean/10 mismatch；§1.4 逐包 A/B 表；dry-run 10 repairable/0 blocked | pass | none |
| F-002 | `backend/services/strategy_package/manifest.py` `compute_manifest_json_sha256` | `test_validate_manifest_integrity_classifies_safe_schema_evolution_drift`; script dry-run evidence raw persisted hash matches stored | pass | none |
| F-003 | `backend/services/strategy_package/manifest.py` `classify_manifest_hash_drift`; `backend/services/strategy_package/repository.py` safe classification gate | `test_validate_manifest_integrity_blocks_dirty_manifest_json_repair`; `test_validate_manifest_integrity_blocks_invalid_manifest_json_repair`; `test_manifest_hash_apply_refuses_b_class` | pass | none |
| F-004 | `backend/services/strategy_package/repository.py` `validate_manifest_integrity` | router/report test and production dry-run report include `repair_plan.classification` | pass | none |
| F-005 | `backend/services/strategy_package/repository.py` `repair_manifest_hash`; `SAFE_MANIFEST_REPAIR_CLASSIFICATION` | `test_repair_manifest_hash_fixes_a_class_drift`; `test_repair_manifest_hash_requires_explicit_confirmation`; B-class apply refusal test | pass | none |
| F-006 | `backend/services/strategy_package/repository.py` audit event context | `test_repair_manifest_hash_fixes_a_class_drift` asserts operator old/new hash classification rollback_restore | pass | none |
| F-007 | `backend/services/strategy_package/repository.py` `StrategyPackageRecord.current_manifest` | `test_repair_manifest_hash_endpoint_passes_confirmation_fields` asserts response package manifest hash equals DB hash | pass | none |
| F-008 | `scripts/strategy_package_manifest_hash_repair.py` | `test_manifest_hash_main_apply_requires_double_confirmation`; `test_dev_target_config_must_look_like_scratch`; production dry-run ran without write | pass | none |
| F-009 | `scripts/strategy_package_manifest_hash_repair.py`; scratch/dev evidence files | Dev DB pre-check done；scratch reproduce/apply/verify/idempotent results recorded in validation history | pass | none |
| F-010 | §11 runbook; script apply gates | Production runbook documents dry-run, apply, post-check, rollback；production DML not executed in this PR | pass | none |
| F-011 | Diff scope; §12 gates | No migration/db init/frontend/S1 territory changes；no service start/restart；`production_ddl_gate=noop`；production DML left for later authorization | pass | none |
| F-012 | `backend/tests/strategy_package/test_repository_service.py`; `backend/tests/strategy_package/test_manifest_integrity_router.py`; `backend/tests/scripts/test_strategy_package_manifest_hash_repair.py` | Targeted L1 currently `19 passed`；final module/static gates recorded in validation history | pass | none |

## 10. Rollout / Rollback

### 10.1 Code rollout

1. 合入 PR 后不自动触发生产 repair；代码变更只增强校验、分类和显式 repair 能力。
2. 后端重启/运行时激活由用户决定；本任务不启动/重启服务。
3. 生产 repair 必须在用户授权窗口执行 §11 runbook。

### 10.2 Rollback

- Code rollback: git revert 本 PR 可移除 classifier/script/current_manifest overlay。
- Data rollback: 如生产 repair 已另行授权执行，按对应 `package_status_event(reason='manifest_hash_repaired')` 的 `context.rollback_restore.restore_value` 恢复每个 package 的旧 hash，并插入人工 rollback audit event。rollback 同样属于生产 DML，必须单独授权。

## 11. Production Repair Runbook / 待授权生产回填手册

执行前提：用户明确授权生产 DML，且确认本 PR 已合入并在目标 checkout 中。

1. 只读预检：

```powershell
rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db prod --limit 500 --output tests/aistock_validation/history/strategy_package/<ts>_prod_manifest_hash_repair_dry_run.json
```

期望：`total_scanned=15`、`repairable_count=10`、`blocked_count=0`。若 `blocked_count>0`，停止，不执行 apply。

2. 执行生产 DML apply：

```powershell
$env:STRATEGY_PACKAGE_MANIFEST_HASH_REPAIR_APPLY='I_UNDERSTAND_PRODUCTION_DML'
rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db prod --limit 500 --apply --confirm-production-dml --operator <operator> --output tests/aistock_validation/history/strategy_package/<ts>_prod_manifest_hash_repair_apply.json
```

3. 后置校验：

```powershell
rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db prod --limit 500 --output tests/aistock_validation/history/strategy_package/<ts>_prod_manifest_hash_repair_after.json
```

期望：`drifted_count=0`、`blocked_count=0`。若发现 B 类，停止并交用户决定恢复原 JSON 或重建包。

4. 多 Alpha S1 oracle：重新执行 child package manifest check，应不再出现 `stored manifest_sha256 does not match stored manifest`。

## 12. Risks / Failure Modes

- 风险: 误把脏 JSON 修成合法。缓解: A 类条件要求 stored 同时等于 raw persisted hash 与 embedded hash；否则 B 类阻断。
- 风险: 生产 apply 被误触发。缓解: 默认 dry-run；生产 apply 需要 flag + env token；dev apply 也需要 scratch confirmation 且 DB 名称/host guard。
- 风险: repair 后 API manifest 内仍带旧 embedded hash。缓解: `current_manifest()` overlay DB `manifest_sha256`，router 测试覆盖。
- 风险: 现有生产仍有 10 个 mismatch，在生产 DML 前 S1 复用仍会失败。缓解: PR 交付 runbook，明确生产 repair 需要后续授权。

## 13. Production Gates / 生产门禁

- `production_ddl_gate=noop`: 无 migration、schema、COMMENT、constraint 变更。
- `production_frontend_dependency_gate=noop`: 不改 frontend，不安装依赖。
- `production_backend_dependency_gate=noop`: 不改依赖清单，不安装依赖。
- `production_dml_gate=pending_user_authorization`: 本 PR 不执行生产 DML；hash repair 回填等待用户单独授权。
- Runtime gate: 不启动/重启生产 backend/frontend/TDX/scheduler。
