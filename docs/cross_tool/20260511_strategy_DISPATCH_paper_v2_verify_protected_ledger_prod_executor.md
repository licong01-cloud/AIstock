# paper-v2 DISPATCH — 5-Layer Verify Codex 2866f66 protected_asset_ledger Prod Executor

**From**: Strategy session
**To**: paper-v2 worktree team
**Sent**: 2026-05-11 ~20:25
**Type**: Type C audit (dual-party verify Codex prod executor)
**Prereq**: c2ef5f5 verify 2fb81b3 PASS, READY-GO

## 上下文

Codex Task 5 deliver (drawer `b113a7a2`, commit `2866f66`) on codex/qe-governance-integration-20260509:

- `scripts/protected_asset_ledger_backfill_prod_executor.py`
- `backend/tests/scripts/test_protected_asset_ledger_backfill_prod_executor.py` (33 tests)
- `docs/operations/r6_prod_apply_runbook_20260511.md` §7.3
- 2 dry-run JSON outputs in `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/`
- 57 paired prod executor tests + 86 broader passed
- guardrail 0 P1 blocking, 7 P2 complexity findings on bounded helper loops

paper-v2 baseline v2 GREEN (e8ffbdd), verify 2fb81b3 strategy_package executor PASS (c2ef5f5)。protected_asset_ledger executor 必须经双侧 verify 才能授权 prod `--apply`。

## 任务

5-layer audit, **沿用 2fb81b3 verify 同款 pattern** (你刚 deliver c2ef5f5, 流程熟悉):

### L1 — Static safety invariants

读 `scripts/protected_asset_ledger_backfill_prod_executor.py`:
- 8-guard fail-fast 链是否完整 (与 strategy_package executor 镜像):
  - flag (`--apply` explicit)
  - token literal `APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD` (Codex 自决具体值, 验证 source)
  - env `AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_PROD_APPLY_ENABLED=true`
  - mutex env `AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_MUTEX_HELD=true`
  - target_db=prod + port=5432 + dbname=aistock
  - DR snapshot ref validation
  - plan preview sha256 confirmation
  - operator typed confirmation (token + DB label + plan sha + DR ref + package_ids)
- 任一 guard 失败 → exit non-zero, **不开 DB connection**
- dry-run 真 offline (no psycopg2.connect / engine.connect)
- audit row emit + per-package txn

### L2 — Tests rerun

```bash
git fetch origin codex/qe-governance-integration-20260509
git checkout 2866f66 -- scripts/protected_asset_ledger_backfill_prod_executor.py backend/tests/scripts/test_protected_asset_ledger_backfill_prod_executor.py
python -m pytest backend/tests/scripts/test_protected_asset_ledger_backfill_prod_executor.py -v
```

预期: 33/33 passed
- 覆盖度: dry-run / token reject / 2 envs / mutex / target_db dev rejected / DR snapshot stale / plan sha mismatch / operator confirmation incomplete / per-package fail rollback / audit emit / JSON schema

### L3 — Dry-run JSON 验证

读 `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/protected_asset_ledger_*.json`:
- status=passed, db_connection_opened=false, db_writes_executed=false, ddl=false
- planned rows 与 dev backfill (b976c23) 一致 (4 packages, 4 protected ledger rows)
- plan_hash deterministic (重跑同输入 → 同 hash)

### L4 — Semantic + runbook §7.3 alignment

读 `docs/operations/r6_prod_apply_runbook_20260511.md` §7.3:
- CLI 与 executor argparse 字面值一致
- env vars 名字 + token literal 字面匹配
- operator confirmation 步骤完整 (5 字段: token + DB label + plan sha + DR ref + package_ids)
- DR snapshot ref 格式与 §7.2 strategy_package 一致

### L5 — P2 ALGO-COMPLEXITY 复验

Codex 标 P2 "bounded helper loops":
- 7 P2 findings 是否真 bounded
- 无 unbounded scan / cursor
- 4 package_ids 硬编码或 config-bound
- 如发现 unbounded → 升级 BLOCKER

## 输出

`docs/cross_tool/20260511_paper_v2_VERIFY_codex_protected_ledger_prod_executor.md`:
- 5 layers each PASS/FAIL/PARTIAL
- 33 tests rerun result
- P2 ALGO-COMPLEXITY 复验结论
- verdict: READY / READY-WITH-CAVEATS / BLOCKED
- 任何 findings 列 file:line

## Deliver drawer

```
[VERIFY] paper-v2 5-layer verify of Codex 2866f66 protected_asset_ledger prod executor
commit: <new>
branch: claude/paper-v2-baseline-post-r5-20260511 (or new)
L1 Static: PASS/FAIL
L2 Tests (33): PASS/FAIL
L3 JSONs: PASS/FAIL
L4 Semantic+runbook §7.3: PASS/FAIL
L5 ALGO-COMPLEXITY: bounded confirmed / not bounded
prod apply readiness: READY|CAVEATS|BLOCKED
doc: docs/cross_tool/20260511_paper_v2_VERIFY_codex_protected_ledger_prod_executor.md
```

## SLA

**≤ 60 min** (~21:25 deliver)

实盘目标驱动: 9:30 A股开市。如 PASS → 下一步 codex/qe-governance branch 流水线 baseline (R6 merge 前置)。

## Do NOT

- ❌ 不要 INSERT dev DB
- ❌ 不要 connect prod DB
- ❌ 不要 fix Codex 代码 (报告 finding 即可)
- ❌ 不要 merge codex branch

## References

- prev verify c2ef5f5 (2fb81b3 strategy_package): main `3435f21`
- Codex Task 5 deliver: drawer `b113a7a2`, commit 2866f66
- runbook on main: `30879c2` + Codex 最新 §7.3 在 codex branch
- main HEAD: `3435f21`
- 实盘目标: 明早 9:30 A股开市
