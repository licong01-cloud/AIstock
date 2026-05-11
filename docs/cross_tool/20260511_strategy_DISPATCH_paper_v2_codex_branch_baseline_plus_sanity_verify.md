# paper-v2 DISPATCH — codex/qe-governance Branch Baseline + Verify Codex c2352a9 Cold-Start Sanity

**From**: Strategy session
**To**: paper-v2 worktree team
**Sent**: 2026-05-11 ~21:25
**Type**: Type C 流水线验证 + Type C audit
**Priority**: HIGH — R6 merge 必需前置

## 上下文

R6 readiness 完全 GREEN:
- ✅ paper-v2 baseline v2 GREEN (main 流水线 R5 PASS)
- ✅ verify 2fb81b3 strategy_package prod executor READY (drawer 1d75214d / c2ef5f5)
- ✅ verify 2866f66 protected_asset_ledger prod executor READY (drawer 979e62d8 / 94242c1)
- ✅ Codex Task 6 coldstart sanity automation deliver (drawer ecf4adeae / c2352a9)
- ✅ main HEAD: `f498246` (含 2 verify docs + R5 + runbook + baseline v2)

R6 merge 前用户硬约束 "**所有功能必须经流水线验证**" → codex/qe-governance branch HEAD `c2352a9` 必须经流水线 baseline GREEN，且 Codex c2352a9 cold-start sanity 必须双侧 verify。

## Task A — codex/qe-governance Branch Stage 6 Baseline (HIGH, R6 merge 前置)

### Step 1: Checkout codex branch

```bash
git worktree add F:/Dev/AIstock-worktrees/branch-baseline-codex-qe c2352a9
cd F:/Dev/AIstock-worktrees/branch-baseline-codex-qe
cp F:/Dev/AIstock/.env .env  # 避免 baseline v2 那种 env-fail
```

### Step 2: 跑 Stage 6 baseline

完整 plan keys (与 baseline v2 e8ffbdd 一致):
- l0, guardrail_changed_files
- validation_coverage_backend, validation_module_registry_l0
- validation_center_backend, validation_center_live_readonly, validation_center_ui
- qe_data_contract_backend, qe_archive_backend, qe_archive_data_quality, qe_archive_l3, qe_read_l3
- paper_v2_backend, paper_v2_l3
- model_registry_backend, market_regime_label, rl_execution_smoke
- data_quality_deep, dr_validate
- 5 UI SKIP

### Step 3: 重点验证 R6 branch 内容

`c2352a9` (branch HEAD) 含的 R6 内容:
- 6 governance migrations (DDL)
- 2 dev backfill scripts + tests (b976c23/75470f5)
- 2 prod backfill executors + tests (2fb81b3 + 2866f66)
- coldstart sanity script + tests (c2352a9)
- runbook §1-§10 完整

重点检查:
1. **qe_data_contract_backend** + **qe_archive_backend** + **paper_v2_backend** — 应保持 GREEN
2. **新增 prod executor tests** — 应自动被 pytest discover (24 + 33 + 30 = 87 tests)
3. **新增 coldstart sanity tests** — 30 tests
4. **migrations** — 应通过 schema validation (但不实际 apply, 因为 dev DB 5433 已 apply)
5. 期望: GREEN ≥ 16 sessions (与 main e8ffbdd 一致) 或 + R6 specific keys

### Step 4: 输出 BRANCH baseline doc

写到新 branch `claude/paper-v2-branch-baseline-codex-qe-20260511`:
- 路径: `docs/baseline/stage6_branch_baseline_codex_qe_c2352a9_20260511.md`
- 字段:
  - codex branch HEAD: c2352a9
  - 每 plan key 结果
  - vs main baseline e8ffbdd 差异 (新增 87 tests + 30 sanity tests)
  - R6 specific 内容 in-branch 验证结论
  - **R6 merge readiness: READY / READY-WITH-CAVEATS / BLOCKED**

## Task B — 5-layer Verify Codex c2352a9 Cold-Start Sanity

**Codex Task 6 deliver** (drawer `ecf4adeae`):
- `scripts/paper_v2_coldstart_sanity.py`
- `backend/tests/scripts/test_paper_v2_coldstart_sanity.py` (30 tests)
- runbook §8.5
- dry-run JSON output

### L1 — Static + 5-guard
- `--mode=dry-run` 默认 offline (no DB connect, no HTTP, no writes, no service touch)
- `--mode=prod` 5-guard fail-fast 先于任何 prod API/DB 调用:
  - exact token `RUN_PAPER_V2_COLDSTART_SANITY_PROD`
  - env `AISTOCK_PAPER_V2_COLDSTART_SANITY_PROD_ENABLED=true`
  - env `AISTOCK_PAPER_V2_COLDSTART_SANITY_MUTEX_HELD=true`
  - non-trading-hours reject (09:30-11:30 + 13:00-15:00 CST weekdays)
  - typed operator confirmation + prod DB target check

### L2 — Tests rerun
```bash
git checkout c2352a9 -- scripts/paper_v2_coldstart_sanity.py backend/tests/scripts/test_paper_v2_coldstart_sanity.py
python -m pytest backend/tests/scripts/test_paper_v2_coldstart_sanity.py -v
```
预期: 30/30 passed

### L3 — Phase 实现验证
- Phase 1 preflight: backend health / daemon proc / DB ping / governance evidence / enable_paper gate
- Phase 2 sentinel: 000001.SZ BUY 100 intended_price 10.00 round-trip, fill capture (intended_price + fill_market_context + created_at/updated_at) + outbox emit (routing_class=telemetry)
- Phase 3 audit chain: governance evidence + protected_asset_ledger + timestamp order
- Phase 4 cleanup: per-table txn DELETE
- Phase 5 verdict: GO/NO-GO + failed_checks + remedial_action

### L4 — Runbook §8.5 alignment
- CLI 与 script argparse 一致
- expected JSON output 例子在 runbook 与 script real output 一致
- abort criteria 完整

### L5 — Sentinel endpoint caveat
- Codex deliver 中说: `/paper-v2/coldstart-sanity/sentinel-order` 端点必须对应 approved prod paper-v2 runtime entry
- 复验: 该端点是否实际存在于 paper-v2 backend routers?
- 如不存在 → BLOCKER (sanity script 跑 prod 会 fail)

### 输出
`docs/cross_tool/20260511_paper_v2_VERIFY_codex_coldstart_sanity.md`:
- 5 layers PASS/FAIL/PARTIAL
- 30 tests rerun result
- L5 sentinel endpoint 存在性 verify
- verdict: READY / READY-WITH-CAVEATS / BLOCKED

## 执行顺序建议

**Task A 先 (branch baseline) → Task B 后 (verify sanity)**
- Task A 阻塞 R6 merge, 必须先 deliver
- Task B 是 cold-start gate, 可等 R6 merge 后再做也行, 但 9:30 实盘前必须 PASS

或并行 (paper-v2 算力够时)。

## SLA

- Task A: ≤ 60 min (~22:25 deliver)
- Task B: ≤ 60 min (与 Task A 串行则 ~23:25)

## Deliver drawers

### Task A drawer
```
[BRANCH BASELINE] Stage 6 codex/qe-governance @c2352a9
commit: <new>
branch: claude/paper-v2-branch-baseline-codex-qe-20260511
verdict: GREEN / YELLOW / RED
sessions: NG/MF/KSKIP
R6 in-branch verified: prod executors / sanity / migrations / runbook
delta vs e8ffbdd main baseline: <summary>
R6 merge readiness: READY / CAVEATS / BLOCKED
doc: docs/baseline/stage6_branch_baseline_codex_qe_c2352a9_20260511.md
```

### Task B drawer
```
[VERIFY] paper-v2 5-layer Codex c2352a9 coldstart sanity
commit: <new>
L1 Static + 5-guard: PASS/FAIL
L2 Tests (30): PASS/FAIL
L3 Phases (1-5): PASS/FAIL
L4 Runbook §8.5: PASS/FAIL
L5 Sentinel endpoint exists: YES/NO
prod sanity readiness: READY|CAVEATS|BLOCKED
doc: docs/cross_tool/20260511_paper_v2_VERIFY_codex_coldstart_sanity.md
```

## Do NOT

- ❌ 不要 INSERT dev DB
- ❌ 不要 connect prod DB / prod backend
- ❌ 不要 fix Codex code (报告 finding)
- ❌ 不要 merge codex branch

## References

- main HEAD: `f498246`
- codex branch HEAD: `c2352a9`
- 之前 main baseline v2: `e8ffbdd`
- Codex Task 6 deliver: drawer `ecf4adeae`
- 实盘目标: 明早 9:30 A股开市
