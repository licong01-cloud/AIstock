# Codex DISPATCH — 9:30 Prod Cutover End-to-End Wrapper (Task 7)

**From**: Strategy session
**To**: Codex App
**Sent**: 2026-05-11 ~21:30
**Type**: Type B (coordinated, operator ergonomics)
**Branch**: codex/qe-governance-integration-20260509
**Priority**: MEDIUM — 用户便利, 非阻断

## 上下文

R6 prod readiness 完全 GREEN. 用户今晚到明早 9:30 之间需手动执行多步:
1. DR snapshot (pg_dump)
2. evidence backfill --apply × 2 (strategy_package + protected_asset_ledger)
3. 6 governance migrations apply
4. paper-v2 daemon enable
5. cold-start sanity (`--mode=prod`)
6. 9:30 GO/NO-GO 决策

每步独立, 手动执行容易出错或漏步。可写一个 **end-to-end wrapper** 让用户一键串联跑, 失败立停 + rollback 提示。

## 任务

写 **`scripts/r6_prod_cutover_e2e_wrapper.py`** on codex/qe-governance branch.

### 设计

**单一 invocation**, 默认 `--mode=dry-run` (全部 step 跑 dry-run, 无 prod 接触), `--mode=prod` 需 explicit + 多重 guard。

**Step 1 — Preflight gates**:
- 验证 paper-v2 baseline v2 doc 存在 (`docs/baseline/stage6_baseline_post_r5_v2_20260511.md`)
- 验证 codex branch baseline doc 存在 (paper-v2 Task A deliver, `docs/baseline/stage6_branch_baseline_codex_qe_c2352a9_20260511.md`)
- 验证 2 verify docs 存在 + verdict READY (strategy_package + protected_asset_ledger)
- 验证 cold-start sanity verify doc 存在 + verdict READY (paper-v2 Task B deliver)
- 验证 R6 merge 已完成 (main HEAD 含 c2352a9 ancestry)
- 任一 fail → exit + 列 missing prereq

**Step 2 — DR snapshot orchestration**:
- Call `pg_dump prod -f <snapshot_path>` (no schema-only, 含 data)
- 验证 snapshot size > threshold (e.g., > 100MB)
- 验证 snapshot row counts (sanity: SELECT count(*) FROM key_tables)
- 输出 DR ref JSON: `{path, timestamp, size_bytes, row_counts_per_table}`
- 这个 DR ref 后续给 prod executor 用

**Step 3 — Apply 2 prod executors**:
- 调 `scripts/strategy_package_governance_evidence_backfill_prod_executor.py --apply` 含全部 5-guard + DR ref + plan preview confirmation
- 等待 exit + 验证 JSON output verdict=passed
- 调 `scripts/protected_asset_ledger_backfill_prod_executor.py --apply` 同上
- 任一 fail → exit + rollback instruction

**Step 4 — Apply 6 migrations**:
- 按 runbook §7.1 顺序逐个 apply (per-file txn)
- 每个 migration 后跑 schema validation query
- 任一 fail → exit + rollback to DR snapshot instruction

**Step 5 — Start prod backend + daemon**:
- backend 8001 启动 (subprocess + health check)
- daemon enable (paper-v2 daemon process spawn or API call)
- 验证 daemon process 真在跑

**Step 6 — Cold-start sanity**:
- 调 `scripts/paper_v2_coldstart_sanity.py --mode=prod` 含 5-guard
- 等 verdict JSON GO/NO-GO
- NO-GO → exit + 列 failed_checks
- GO → 输出 final report

**Step 7 — 9:30 readiness verdict**:
- 全 step PASS → exit 0, JSON `{ready_for_trading: true, daemon_pid, backend_pid, timestamp}`
- 任一 fail → exit non-zero, JSON 含 step_failed + remedial_action

### 5-Guard

`--mode=prod` 需:
- exact token `RUN_R6_PROD_CUTOVER_E2E`
- env `AISTOCK_R6_PROD_CUTOVER_E2E_ENABLED=true`
- mutex env (单实例)
- non-cutover-hours OK check (e.g., 22:00-09:00 推荐, 但不强制)
- operator confirmation (typed 全部 prereq doc paths + DR ref + final intent)

### 测试

`backend/tests/scripts/test_r6_prod_cutover_e2e_wrapper.py`:
- 各 step preflight 单测 (mocked file system / subprocess)
- 失败立停验证 (DR snapshot fail / executor fail / migration fail / sanity NO-GO)
- 5-guard reject paths
- final JSON schema
- ≥ 25 tests passed, 0 P1 guardrail

### Update runbook

Append `docs/operations/r6_prod_apply_runbook_20260511.md` §11 (End-to-End Wrapper Usage):
- CLI 示例 + 各 step 输出例子
- 与 §7-§10 手动 step 等价说明

### Do NOT

- ❌ 不要执行 prod cutover (这是 user 实盘前手动 invoke)
- ❌ 不要 INSERT dev DB
- ❌ 不要触 prod backend / prod DB
- ❌ 不要 commit credentials / token literal in test
- ❌ 不要 merge codex branch

### Deliver

drawer:
- commit hash
- files added
- test count
- runbook §11 update
- E2E wrapper readiness: confirmed / blocked

### SLA

**≤ 1.5h** (~23:00 deliver)

## 引用

- main HEAD: `f498246`
- codex branch HEAD: `c2352a9` (含 2 prod executors + cold-start sanity + 6 migrations + runbook §1-§10)
- 实盘目标: 明早 9:30 A股开市
- paper-v2 Task A (branch baseline) + Task B (verify sanity) 并行进行

## 注意

如 paper-v2 Task A/B 发现 R6 BLOCKER, 本任务的 E2E wrapper 可能需相应调整 (e.g., 修复后再跑)。**deliver 即可, 不阻断 R6 merge**。
