# StrategyPackage manifest 哈希漂移只读 RCA（2026-07-01）

## 0. 只读边界

- 调查 worktree：`F:\Dev\AIstock_worktrees\strategy-package-manifest-sha-drift-readonly-20260701`
- 调查时间：2026-07-01，Asia/Shanghai。
- 本报告只做取证分析：未改代码、未启停服务、未写生产 DB、未重建/回滚 binding 或 package、未发/撤券商订单、未跑 operator 命令。
- DB 查询均为只读事务：`conn.set_session(readonly=True, autocommit=False)`，查询后 `rollback()`。
- production gates：`production_ddl_gate=noop`，`production_frontend_dependency_gate=noop`，`production_backend_dependency_gate=noop`。

## 1. 结论摘要

1. 2026-07-01 01:59:20-01:59:23 的批量 `manifest_sha256` 变化来源明确：`strategy_package_asset_backfill_20260701` 生产 DML，即 StrategyPackage runtime asset backfill/freeze，而不是 BUG-565，也不是 `manifest-sha-integrity` 哈希修复。
2. 定论：这不是“仅哈希口径变化”。manifest 内容实质变化：`factor_set` 与 `model_asset` 中新增了非空 `asset_ref`、`sha256`、`source_uri`、`size_bytes` 等冻结 runtime asset 指针；`factor_id` 和 `model_id` 未变，但 manifest 作为冻结运行资产声明已经改变，因此 hash 改变符合当前 canonical hash 逻辑。
3. 完整性守卫工作正常：scheduler 在 pre-run 阶段加载当前 package manifest 后，与 runtime release/binding 冻结哈希比对；不一致即 fail-closed，持久化 `PRE_RUN_FAILED` 证据，`broker_called=false`、`submitted_intents=0`，未触达 broker。
4. 归因：01:59 操作是预期的资产自包含治理/回填操作，但缺少同步的 runtime release / simulation binding / portfolio 重新冻结步骤，导致旧 binding 被新 package manifest 孤儿化。
5. 对 A/B MiniQMT 路线：A event_loop 与 B compiler 都走同一个 pre-run manifest identity guard；在 binding/release 与 current package manifest 对齐前，MiniQMT 模拟盘无法进入真实提交阶段。

## 2. 01:59 批量重写来源

### 2.1 Git / PR 证据

- 直接记录本次生产 DML 的 PR：#1782 `docs(strategy-package): record production asset backfill apply`，merge commit `074ffd0dae74b47fd6ec2ff2e63a5c7366a4928c`，merged at `2026-07-01 02:20:06+08`。
- PR #1782 body 记录：`production_dml_gate=applied_and_verified`，`applied=13`、`asset_count=605`、`unrecoverable_count=0`，并列出 L2 package `pkg_a2f53f3f2f3e4095a910b939464c35e6`。
- 执行 worktree：`F:\Dev\AIstock_worktrees\strategy-package-prod-asset-backfill-20260701`，branch `ops/strategy-package-prod-asset-backfill-20260701`，commit `b9e64457`。
- 实现来源：
  - PR #1771 `feat(strategy-package): backfill frozen package runtime assets`，merge commit `b7d0c56f4e3a385839606117cef1fbed8775cc1a`，新增 `scripts/strategy_package_asset_backfill.py`、`backend/services/strategy_package/package_asset_backfill.py`、`backend/services/strategy_package/repository.py`。
  - PR #1773 `feat(strategy-package): dual-source asset backfill with QE-node recovery`，merge commit `b3f39b4f3d1106fd286cbaa460327caff809231e`，补双源恢复能力。
  - PR #1761 `feat(strategy-package): freeze package model and factor assets`，merge commit `bc6d1a75e1b58968a0d3d2c3aec3ee49f0077b61`，引入资产冻结基础能力。
- 排除项：PR #1756 `fix(strategy-package): repair manifest sha integrity + root-cause sync` 是 manifest hash repair。代码锚点 `backend/services/strategy_package/repository.py:2117` 明确只修 hash column，`backend/services/strategy_package/repository.py:2225` 仅 `SET manifest_sha256 = %s, updated_at = NOW()`；它不修改 `manifest_json`，且 01:59 DB 事件不是 `manifest_hash_repaired`。

### 2.2 代码锚点

- `scripts/strategy_package_asset_backfill.py:1`：脚本是 StrategyPackage runtime asset backfill helper；默认 dry-run，apply 需要显式 gate。
- `scripts/strategy_package_asset_backfill.py:258`：`--apply --target-db prod` 需要 `--confirm-production-dml` 以及 `STRATEGY_PACKAGE_ASSET_BACKFILL_APPLY=I_UNDERSTAND_PRODUCTION_DML`。
- `backend/services/strategy_package/package_asset_backfill.py:417`：对 desired manifest 重新 `freeze_manifest`。
- `backend/services/strategy_package/package_asset_backfill.py:435`：`asset_freezer.freeze_manifest_assets(desired)` 生成带 runtime asset refs 的 frozen manifest。
- `backend/services/strategy_package/package_asset_backfill.py:458`：计划项记录 `old_manifest_sha256` 与 `new_manifest_sha256`。
- `backend/services/strategy_package/repository.py:554`：生产 DML `UPDATE strategy_pkg.package SET manifest_json = %s, manifest_sha256 = %s, updated_at = NOW()`。
- `backend/services/strategy_package/repository.py:581`：写 `strategy_pkg.package_status_event`，reason=`strategy_package_asset_backfill_freeze`，context 内带 `old_manifest_sha256`、`new_manifest_sha256`、`asset_count` 和 `rollback_restore`。

### 2.3 DB 事件证据

只读查询生产 DB `aistock`（server `172.17.0.3:5432`，observed_at `2026-07-01 10:34:05+08`）显示：

- 2026-07-01 01:50-02:05 之间有 13 条 `strategy_package_asset_backfill_freeze` 事件，时间范围 `2026-07-01 01:59:20.536013+08` 到 `2026-07-01 01:59:23.858890+08`。
- 这 13 个 package 与 PR #1782 apply 记录一致：
  - `pkg_006a42323f7c4e81a468fdaad2cb16a3`
  - `pkg_09750b4944ca434db03efd399ccf2144`
  - `pkg_1de32357724a4c5b874f2abd90f22da5`
  - `pkg_2563063e544f4d1fa601e740d019f8c7`
  - `pkg_2a9fccb83da840c9a27a2d7a4118af9a`
  - `pkg_378eb9c91e104c64935404e257e932ee`
  - `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27`
  - `pkg_99142cb1440c40a7824e83902f4e7da9`
  - `pkg_a2f53f3f2f3e4095a910b939464c35e6`
  - `pkg_b2faccade8d549af9621c51d285bdc06`
  - `pkg_b668f8a633c44b72a5d557a2cb8970e3`
  - `pkg_c4703dfc2fdf4e548cf8dd3027ef228b`
  - `pkg_cfa3c5b4068d4db1ad06db352bfece93`

注：本次读取时，`strategy_pkg.package` 共有 15 行；13 行在 01:59 被 asset backfill/freeze 更新，`pkg_b4ce...` 在 01:52 retired，`pkg_955...` 当前为 10:32 retired（不属于 01:59 batch）。

## 3. 内容变了还是仅哈希口径变了

### 3.1 哈希计算锚点

- `backend/services/strategy_package/manifest.py:14` `_canonical_payload` 基于 `manifest.model_dump(mode="json")`，将 `manifest_sha256` 与 `package_status` 置空。
- `backend/services/strategy_package/manifest.py:23` `compute_manifest_sha256` 对 canonical payload 做 `json.dumps(... sort_keys=True, separators=(",", ":"))` 后 SHA256。
- `backend/services/strategy_package/manifest.py:133` `freeze_manifest` 调用 `compute_manifest_sha256` 并写回 digest。
- `backend/services/strategy_package/manifest.py:138` `_drop_empty_asset_fields` 只剔除空的 asset 默认字段；`backend/services/strategy_package/manifest.py:156` 仅当 `asset_ref`、`sha256`、`size_bytes`、`source_uri` 为 `None`/空值时剔除。非空资产指针会参与 hash。

### 3.2 L2 package 对比

目标 package：`pkg_a2f53f3f2f3e4095a910b939464c35e6`。

- DB event：`event_id=402`，`created_at=2026-07-01 01:59:22.914381+08`，reason=`strategy_package_asset_backfill_freeze`。
- old sha：`b3fa7f6eed5cf929c79ad1726ade31eb80a9ad54f45bfad764c6ef52a9fe0dfe`。
- new/current sha：`77402e38e2cb215b213c7bd9e243bd2a74cdc855acb180cdbc5196b6916ef207`。
- `factor_ids_equal=true`，factor count `23 -> 23`。
- old factor assets：`with_asset_ref=0`、`with_sha256=0`、`with_source_uri=0`。
- new factor assets：`with_asset_ref=23`、`with_sha256=23`、`with_source_uri=23`。
- old model：`model_id=__seed_LSTM_10D_hs64_d02__`，`asset_ref/sha256/source_uri/size_bytes=null`。
- new model：同一 `model_id=__seed_LSTM_10D_hs64_d02__`，但新增 `asset_ref=aistock-package-asset://...336ac4c2...`，`sha256=336ac4c2ac0e7aa9a3679fca7a86e1e0c4995585b53e996e8ac6daf08f062a1b`，`source_uri=qe-workspace://node/wsl2-5080/tasks/qe_20260601_172505_fe17/loops/Loop2/mlruns/artifacts/params.pkl`，`size_bytes=680261`。

### 3.3 L16 package 对比

目标 package：`pkg_378eb9c91e104c64935404e257e932ee`。

- DB event：`event_id=399`，`created_at=2026-07-01 01:59:22.084344+08`。
- old sha：`8f6d8b0235459a0b657a3c0bb3a00e9a63707578e0bd7de978add42855d31ebf`。
- new/current sha：`2aae3560563bd669e5f1951c40ae939744f82a67be5b7479f239b9f910270300`。
- `factor_ids_equal=true`，factor count `57 -> 57`。
- old factor assets：`with_asset_ref=0`、`with_sha256=0`、`with_source_uri=0`。
- new factor assets：`with_asset_ref=57`、`with_sha256=57`、`with_source_uri=57`。
- model `model_id` 未变，新增 package asset pointer、model weight sha 与 source URI。

### 3.4 其它包抽样

`pkg_b668f8a633c44b72a5d557a2cb8970e3`：

- 06-30 曾发生一次 `manifest_hash_repaired`：`3e9cd8e0... -> 5f043c3f...`，operator=`strategic_session_tier2`，这属于哈希修复历史。
- 07-01 01:59 又发生 `strategy_package_asset_backfill_freeze`：`5f043c3f... -> a4a21c01...`，新增 50 个 factor asset refs 与 1 个 model weight asset ref。

### 3.5 定论

这次 01:59 不是序列化/字段集口径漂移导致的“hash-only”变化，而是 manifest JSON 中资产指针字段从空值变为非空 package-owned asset references。alpha 因子身份和模型身份未变，但 runtime artifact authority 从外部 QE/source path 升级为 package-owned frozen asset，属于 manifest 内容实质变化，hash 改变是预期结果。

## 4. 影响面统计

### 4.1 Package 层

- 当前 `strategy_pkg.package`：15 行。
- 01:59 asset backfill/freeze 更新：13 行。
- L2 当前 package sha：`77402e38...`；旧 binding/release sha：`b3fa7f6e...`。
- L16 当前 package sha：`2aae3560...`；旧 binding/release sha：`8f6d8b02...`。

### 4.2 Simulation release binding 层

只读统计 `paper_v2.simulation_release_binding` 对 current `strategy_pkg.package.manifest_sha256`：

- total bindings：82。
- mismatched bindings：82。
- 按 `effective_to IS NULL OR effective_to >= 2026-07-01` 判定 active-on-or-after-0701：4。
- active mismatched：4/4。
- 其中 MiniQMT active mismatched：2/2。
- LocalSim active mismatched：2/2。
- 这 4 条 active binding 的 `approval_state` 均为 `SIM_VALIDATING`，不是 `APPROVED`。

Active mismatch 明细：

| binding_id | backend | slot/account | package_id | binding/release sha | current package sha |
|---|---|---|---|---|---|
| `simbind_8de8ab6f86b09093` | `minqmt_sim` | `codex_final_ms_l2_20260603` / `ag_minqmt_62266303_sim` | `pkg_a2f53f3f...` | `b3fa7f6e...` | `77402e38...` |
| `simbind_ce7a6848f546b43a` | `minqmt_sim` | `codex_final_ms_l16_20260603` / `ag_minqmt_62266303_sim` | `pkg_378eb9c9...` | `8f6d8b02...` | `2aae3560...` |
| `simbind_ad760218884114b5` | `local_sim` | historical LocalSim L2 | `pkg_a2f53f3f...` | `b3fa7f6e...` | `77402e38...` |
| `simbind_cbb7f43f22c515b9` | `local_sim` | historical LocalSim L16 | `pkg_378eb9c9...` | `8f6d8b02...` | `2aae3560...` |

### 4.3 Portfolio 层

只读统计 `paper_v2.portfolio` 对 current package sha：

- total portfolios：359。
- mismatched portfolios：359。
- `auto_run_enabled=true` portfolios：2。
- autorun mismatched：2/2。

注意：许多 historical/E2E portfolios 本来就是历史冻结快照；“359/359 mismatched”表示它们相对 current package manifest 已全变成旧 sha，并不等同于全部需要运营处置。但对于仍会被 scheduler/auto-run 使用的 active binding/portfolio，必须重新冻结或重建。

### 4.4 07-01 MiniQMT run 证据

- L2 run `simrun_9268ce2302f16d62`：
  - `trade_date=2026-07-01`，`broker_backend=minqmt_sim`，`binding_id=simbind_8de8ab6f86b09093`。
  - DB run column `manifest_sha256=b3fa7f6e...`，即 runtime release/binding 旧 sha。
  - `pre_run_failure.context.manifest_sha256=77402e38...`，即当前 package sha。
  - `binding_manifest_sha256=b3fa7f6e...`，`release_manifest_sha256=b3fa7f6e...`。
  - payload：`broker_called=false`，`submitted_intents=0`。
- L16 run `simrun_18eef66256c91e1d`：
  - DB run column `manifest_sha256=8f6d8b02...`。
  - `pre_run_failure.context.manifest_sha256=2aae3560...`。
  - `binding_manifest_sha256=8f6d8b02...`，`release_manifest_sha256=8f6d8b02...`。
  - payload：`broker_called=false`，`submitted_intents=0`。

说明：run 行本身继承 runtime release/binding frozen sha；pre-run failure context 里的 `manifest_sha256` 是当前 package manifest sha。报错本质是 current package vs frozen binding/release 不一致。

## 5. 完整性守卫定位

### 5.1 校验点

- `backend/services/simulation_runtime/scheduler.py:1392` `_load_strategy_package_manifest(...)` 按 `binding.package_id` 加载当前 StrategyPackage manifest。
- `backend/services/simulation_runtime/scheduler.py:1430` 调用 `_validate_manifest_identity(...)`。
- `backend/services/simulation_runtime/scheduler.py:1446` 校验 `manifest.package_id` 与 `binding.package_id`、`runtime_release.package_id` 一致。
- `backend/services/simulation_runtime/scheduler.py:1455` 校验 `manifest.manifest_sha256 == binding.manifest_sha256 == runtime_release.manifest_sha256`。
- `backend/services/simulation_runtime/scheduler.py:1456` 不一致时抛 `DataUnavailableError("LocalSim manifest hash does not match runtime release binding")`，context 包含 current manifest sha、release sha、binding sha。

注：报错文本写的是 `LocalSim manifest hash...`，但该 helper 被 MiniQMT simulation context 共用；MiniQMT L2/L16 也是这个 guard 拦截。

### 5.2 fail-closed 持久化

- `backend/services/simulation_runtime/scheduler.py:2223` scheduler 捕获 `DataUnavailableError` / `RuntimeConfigInvalidError`。
- `backend/services/simulation_runtime/scheduler.py:2260` `_persist_pre_run_binding_failure(...)` 持久化 pre-run failure run。
- `backend/services/simulation_runtime/scheduler.py:2323` 更新 run status 为 `FAILED_RETRYABLE`，payload `last_stage=PRE_RUN_FAILED`、`broker_called=false`、`submitted_intents=0`、`failed_intents=0`。
- `backend/services/simulation_runtime/scheduler.py:2377` `_pre_run_failure_diagnostic(...)` 写 durable diagnostic，`next_action` 明确 no broker order was submitted before this failure。

结论：完整性守卫是 loud + fail-closed；问题不在守卫失效，而在 01:59 package manifest 更新后，runtime release / simulation binding / portfolio 冻结快照没有同步更新或重建。

## 6. 归因判定

二选一判定：这是“预期治理操作 + 缺 binding 同步流程”，不是“误操作/哈希口径回归”。

证据：

1. 01:59 全部 DB 事件均为 `strategy_package_asset_backfill_freeze`，且 event context 记录 `operator=codex_strategy_package_asset_backfill_20260701`、old/new sha、asset_count、rollback_restore。
2. PR #1782 明确记录该生产 DML 已获授权、目标是让 StrategyPackage 资产自包含，`production_dml_gate=applied_and_verified`。
3. manifest 内容确实新增 package-owned asset references；不是仅 `manifest_sha256` column 改写。
4. 当前 hash 函数会剔除空 asset 默认字段，但会包含非空 asset refs/sha/source_uri；因此新 hash 是设计内结果。
5. 旧 simulation binding/runtime release 是不可变冻结对象，仍指向 backfill 前 sha；它们在 package manifest 被原地改写后必然与 current package sha 不一致。

风险性质：资产治理 DML 改变了既有 package identity 的 manifest hash，但没有提供自动的下游 release/binding/portfolio refreeze/rebind 步骤。对于“runtime 以 package_id 读取 current manifest，再与 binding/release frozen sha 比对”的路径，这会产生全量旧 binding orphan。

## 7. 对 A 路线 / 模拟盘上线影响

- MiniQMT A event_loop 与 B compiler 都在进入 broker submit 前经过 scheduler pre-run manifest identity guard。
- 只要 active binding/release 仍冻结旧 sha，而 current `strategy_pkg.package.manifest_sha256` 已是新 sha，A/B 都会在 pre-run 阶段失败，不会触发 broker。
- 因此 BUG-565 修复后的 submit-window / cross-day terminalization 并不是当前 07-01 PRE_RUN_FAILED 的原因；当前阻塞点在 StrategyPackage manifest/binding 生命周期一致性。
- 对 LocalSim：本次任务不分析 WSL inference failure；但只读统计显示 active LocalSim binding 也已相对 current package sha mismatch。若其路径进入同一 identity guard，也会受影响。

## 8. 建议处置方向（不实施）

优先建议：按“预期治理操作”处理，不回滚 manifest，不改 hash 口径；由战略 session 授权执行受控的 release/binding/portfolio 重新冻结或重建。

建议顺序：

1. 对仍需运行的 package/slot，基于当前 package manifest sha 创建新的 immutable `strategy_runtime_release`。
2. 基于新 release 重建或重冻结 `paper_v2.simulation_release_binding`，至少覆盖 2026-07-01 active MiniQMT L2/L16：
   - L2：`simbind_8de8ab6f86b09093` 所在 slot `codex_final_ms_l2_20260603`，目标 sha `77402e38...`。
   - L16：`simbind_ce7a6848f546b43a` 所在 slot `codex_final_ms_l16_20260603`，目标 sha `2aae3560...`。
3. 对 `paper_v2.portfolio` 中仍 active/auto-run 的 rows 进行同样的受控 refreeze/rebind；历史 E2E/retired rows 可不作为运营阻塞，但应在影响面报告中分层。
4. 在未来的 `strategy_package_asset_backfill` 生产 DML runbook 中增加 preflight impact report：列出将被 orphan 的 active simulation bindings、runtime releases、paper portfolios，并要求同一变更窗口完成 downstream sync 或显式 no-go。
5. 不建议修 hash 计算口径：当前 canonical hash 正确反映非空 asset refs 进入 manifest identity。
6. 不建议默认回滚 manifest：回滚会撤销 package-owned runtime asset freeze 的治理成果；只有当战略 session 判定 01:59 asset backfill 本身未授权或资产指针错误时，才应使用 event context 中的 `rollback_restore` 走单独授权 DML。

## 9. 未做事项

- 未登记 BUG。
- 未重建/回滚任何 package、binding、runtime release、portfolio。
- 未写 DB、未应用 DDL、未启停服务、未触发 scheduler/operator、未发/撤券商订单。
