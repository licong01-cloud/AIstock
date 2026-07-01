# BUG-567 StrategyPackage binding refreeze dry-run handoff（2026-07-01）

## 0. 边界

- worktree：`F:\Dev\AIstock_worktrees\BUG-567-active-simulation-bindings-orphaned-after-packag-20260701`
- BUG：`BUG-567`；GitHub Issue：`#1786`
- 本轮只做 develop + dry-run：未执行 `--apply`，未写生产 DB，未启停服务，未发/撤券商订单，未跑 operator 命令。
- production gates：`production_ddl_gate=noop`，`production_frontend_dependency_gate=noop`，`production_backend_dependency_gate=noop`。
- 根因证据已随 PR 落地：`docs/handoff/strategy_package_manifest_sha_drift_readonly_rootcause_2026-07-01.md`。

## 1. 根因引用

前序 RCA 定论：PR #1782 的生产 asset backfill DML 在 `2026-07-01 01:59` 将 13 个 package manifest 从外部资产引用升级为 package-owned frozen assets，manifest 内容实质变化、hash 合法变化；但下游 `strategy_runtime_release` / `simulation_release_binding` / active auto-run `paper_v2.portfolio` 未同步重新冻结，导致 scheduler pre-run manifest identity guard fail-closed。MiniQMT A/B 与 LocalSim 共用该一致性约束，当前 active SIM binding 对齐前无法通过 pre-run。

## 2. 本 PR 新增脚本

- 脚本：`scripts/strategy_package_binding_refreeze.py`
- 默认模式：dry-run；支持显式 `--dry-run`，只有 `--apply` 才进入 DML 分支。
- 生产 apply gate：必须同时满足 `--apply --target-db prod --confirm-production-dml` 和环境变量 `STRATEGY_PACKAGE_BINDING_REFREEZE_APPLY=I_UNDERSTAND_PRODUCTION_DML`。
- 默认目标筛选：`approval_state=SIM_VALIDATING` 且 `effective_to IS NULL OR effective_to >= --active-on`，并比较 binding/release frozen sha 与 current package sha；支持 `--binding-id` 与 `--backend` 限定。
- 安全门：package manifest 自洽校验、LIVE binding 拒绝、binding/release sha 已不一致则 loud raise、已对齐则 skip、不修改 package manifest。
- 审计/事务 apply 路径（本轮未执行）：创建新 immutable release/binding，active/auto-run portfolio 同 batch CAS 更新，写 `strategy_pkg.package_status_event`，reason=`strategy_package_binding_refreeze_after_asset_backfill`。

代码锚点：

- apply gate：`scripts/strategy_package_binding_refreeze.py:45`、`scripts/strategy_package_binding_refreeze.py:947`
- package 自洽校验：`scripts/strategy_package_binding_refreeze.py:274`
- 同日窗口保护：`scripts/strategy_package_binding_refreeze.py:402`
- dry-run 计划生成：`scripts/strategy_package_binding_refreeze.py:514`
- active auto-run portfolio 过滤：`scripts/strategy_package_binding_refreeze.py:706`
- apply 事务入口：`scripts/strategy_package_binding_refreeze.py:782`

## 3. MiniQMT dry-run 结果

命令（只读）：

```powershell
rtk python scripts/strategy_package_binding_refreeze.py --dry-run --env-file F:\Dev\AIstock\.env --target-db prod --active-on 2026-07-01 --backend minqmt_sim --binding-id simbind_8de8ab6f86b09093 --binding-id simbind_ce7a6848f546b43a --output tmp/issue_workflow/BUG-567/miniqmt_binding_refreeze_dry_run.json
```

结果：`status=passed`，`db_writes_executed=false`，`planned_refreeze=2`，`skipped=0`，`portfolio_updates=1`。

| binding_id | backend | slot | package | sha | planned release | planned binding | portfolio updates |
|---|---|---|---|---|---|---|---|
| `simbind_8de8ab6f86b09093` | `minqmt_sim` | `codex_final_ms_l2_20260603` | `pkg_a2f53f3f2f3e4095a910b939464c35e6` | `b3fa7f6e..` -> `77402e38..` | `srr_b6881c2fa6d0c1d6` | `simbind_06efa40c99da8bc9` | 无 |
| `simbind_ce7a6848f546b43a` | `minqmt_sim` | `codex_final_ms_l16_20260603` | `pkg_378eb9c91e104c64935404e257e932ee` | `8f6d8b02..` -> `2aae3560..` | `srr_d5128027d4768885` | `simbind_dcabd41bdbac1b1c` | `paper_1d9b1f03700f4810aef8351124c8ab6c` |

说明：当前 2 条 MiniQMT source binding 的 `effective_from=2026-07-01` 且 `effective_to=2026-07-01`；为避免把同日 source binding 改成非法窗口，dry-run 计划采用 `future_replacement_preserves_same_day_source_window`，新 release/binding 生效窗口为 `2026-07-02~2026-07-02`，旧 binding 保持到 `2026-07-01`。

## 4. 全 4 条 active mismatch dry-run

命令（只读）：

```powershell
rtk python scripts/strategy_package_binding_refreeze.py --dry-run --env-file F:\Dev\AIstock\.env --target-db prod --active-on 2026-07-01 --binding-id simbind_8de8ab6f86b09093 --binding-id simbind_ce7a6848f546b43a --binding-id simbind_ad760218884114b5 --binding-id simbind_cbb7f43f22c515b9 --output tmp/issue_workflow/BUG-567/all4_binding_refreeze_dry_run.json
```

结果：`status=passed`，`db_writes_executed=false`，`planned_refreeze=4`，`skipped=0`，`portfolio_updates=1`。默认筛选 dry-run（不传 binding_id）也得到相同计数 `planned_refreeze=4`。

| binding_id | backend | slot/strategy | sha | planned effective window | portfolio updates |
|---|---|---|---|---|---|
| `simbind_8de8ab6f86b09093` | `minqmt_sim` | `codex_final_ms_l2_20260603` | `b3fa7f6e..` -> `77402e38..` | `2026-07-02~2026-07-02` | 无 |
| `simbind_ce7a6848f546b43a` | `minqmt_sim` | `codex_final_ms_l16_20260603` | `8f6d8b02..` -> `2aae3560..` | `2026-07-02~2026-07-02` | `paper_1d9b1f03700f4810aef8351124c8ab6c` |
| `simbind_ad760218884114b5` | `local_sim` | `paper_e225bf8a68244c54b4cc25506dadad81` | `b3fa7f6e..` -> `77402e38..` | `2026-07-02~2026-07-02` | 无 |
| `simbind_cbb7f43f22c515b9` | `local_sim` | `paper_b26d2312d986441f8497f7484c05f0ec` | `8f6d8b02..` -> `2aae3560..` | `2026-07-02~2026-07-02` | 无 |

active auto-run portfolio 分层结果：只有 `paper_1d9b1f03700f4810aef8351124c8ab6c`（MiniQMT L16，status=`FAILED`，auto_run_enabled=`true`）与目标包、旧 sha、同 backend 匹配，被列入 MiniQMT L16 refreeze 计划；历史/E2E/retired portfolio 未纳入。

## 5. 测试证据

- `rtk python -m compileall scripts/strategy_package_binding_refreeze.py backend/tests/scripts/test_strategy_package_binding_refreeze.py`：passed
- `rtk python -m pytest backend/tests/scripts/test_strategy_package_binding_refreeze.py -q`：`7 passed`
- `rtk python -m ruff check scripts/strategy_package_binding_refreeze.py backend/tests/scripts/test_strategy_package_binding_refreeze.py`：passed
- `rtk python -m nox -s validation_module_registry_l0`：passed
- `rtk python -m nox -s l0`：passed
- `rtk python -m nox -s paper_v2_backend`：passed，`800 passed, 1 skipped, 2 xfailed`
- `rtk git diff --check`：passed

覆盖点：mismatch binding 生成新 release/binding dry-run、already matched skip、package 不自洽 loud raise、LIVE binding 拒绝、历史 retired/effective_to past 不被默认选入、同日 binding 使用次日 replacement window、apply gate 双确认。dry-run 分支测试中将 repository `save_strategy_runtime_release` / `save_simulation_release_binding` 替换为 fail，确认 dry-run 不调用保存路径。

## 6. Apply 前需战略 session 二次确认

建议战略 session 审核项：

1. 目标是否只允许 MiniQMT 2 条，还是一次处理全 4 条 active mismatch。
2. 是否接受同日 source binding（`2026-07-01~2026-07-01`）的 replacement window 从 `2026-07-02` 开始；若要求 retroactive 修复 07-01 run，需要单独授权并明确窗口语义。
3. 是否处理 status=`FAILED` 但 `auto_run_enabled=true` 的 MiniQMT L16 portfolio refreeze；脚本只处理 auto-run 且同 backend 的 portfolio，不触碰历史/E2E/retired。
4. Apply 必须单独授权；本 PR 不执行 apply，不合并即不改变生产数据。
