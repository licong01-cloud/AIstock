# Codex Verify - Synthetic Evidence Rollback

Date: 2026-05-12
Branch: `codex/qe-cleanup-and-pr005-prep-20260512`
Worktree: `F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512`
Task: Task 15, five-layer static verification of `scripts/r6_cutover_synthetic_evidence_rollback.py`

## Verdict

BLOCKED.

The rollback script is syntactically valid and its apply path wraps the main delete/update block in a transaction, but it is not production-ready for the requested Task 10 substitute gate. It lacks the hardened production guard chain, its dry-run opens the production-like DB target, its implemented row semantics do not match the stated cleanup contract, and it has insufficient audit/reporting and concurrency protection for a production synthetic-evidence rollback.

Do not execute `scripts/r6_cutover_synthetic_evidence_rollback.py` from Codex or an automated window. The next safe step is to harden it or replace it with a DB-operator-reviewed row-id rollback package before any production apply.

## Scope and Constraints

- I did not run the rollback script in dry-run or apply mode.
- I did not connect to prod DB, dev DB, backend, frontend, paper daemon, live broker, or any service port.
- I did not touch `main`, Claude worktrees, services, daemons, or broker/runtime state.
- I wrote only this report file in the worktree. An unrelated untracked Task 14-looking file was observed and left untouched: `docs/handoff/archive_branches_register_20260512.md`.

## Source Position

- Current worktree `HEAD=7eaee9f8d6b90b3bdcd33d9375562899da4246ee`.
- `origin/main=da648066473be2546151bff58b8c2f3febcf2de9`.
- Current branch and `origin/main` are diverged: `git rev-list --left-right --count HEAD...origin/main` returned `2 5`.
- The target script is absent from current `HEAD`, but present in `origin/main`.
- Audited script object: `origin/main:scripts/r6_cutover_synthetic_evidence_rollback.py`, blob `ef40bbda06456677082cb8bba2743280f2ec1f97`.
- The blob is introduced by commit `e293a8c chore(cutover): 2026-05-12 R6 prod cutover artifacts + synthetic evidence + rollback`.

## Commands Run

All commands below were static/read-only or test-only and did not connect to DB/services.

```powershell
git status --short --branch
git rev-parse --abbrev-ref HEAD
git rev-parse HEAD
git rev-parse origin/main
git ls-tree -r HEAD --name-only | rg "scripts/r6_cutover_synthetic_evidence_rollback.py"
git ls-tree -r origin/main --name-only | rg "scripts/r6_cutover_synthetic_evidence_rollback.py|docs/handoff/r6_prod_cutover_20260512_state.md|scripts/r6_cutover_synthetic_evidence_pkg_5a5c.py"
git rev-parse origin/main:scripts/r6_cutover_synthetic_evidence_rollback.py
git show origin/main:scripts/r6_cutover_synthetic_evidence_rollback.py | python -c "import ast,sys; src=sys.stdin.read(); ast.parse(src); print('AST_PARSE_OK lines=%d chars=%d' % (src.count(chr(10))+1, len(src)))"
git grep -n "r6_cutover_synthetic_evidence_rollback\|synthetic_pre_real_etl\|synthetic_evidence_9:30_sanity\|strategy_session_9:30\|pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27" origin/main -- .
git grep -n "r6_cutover_synthetic_evidence_rollback\|synthetic_evidence_rollback\|synthetic_pre_real_etl" origin/main -- backend/tests tests
$env:PYTHONDONTWRITEBYTECODE='1'; python -m pytest backend/tests/scripts/test_r6_prod_cutover_e2e_wrapper.py -q -p no:cacheprovider
```

Key outputs:

- `AST_PARSE_OK lines=116 chars=4959`.
- Direct rollback-script test search returned `NO_DIRECT_TEST_MATCHES`.
- Hardened R6 cutover wrapper regression, used only as production-guard comparator: `31 passed in 0.64s`.

## Layer 1 - Static Guard Chain

Verdict: BLOCKED.

Evidence:

- Script has only `apply_mode = "--apply" in sys.argv` as the execution mode gate (`origin/main:scripts/r6_cutover_synthetic_evidence_rollback.py:31-32`).
- It opens a DB connection unconditionally immediately after parsing `--apply` (`:33-36`).
- Connection target and credentials are hard-coded as `host="127.0.0.1", port=5432, dbname="aistock", user="postgres", password="lc78080808"` (`:33-36`).
- No exact confirmation token, environment enable flag, mutex guard, cutover-window acknowledgement, DR snapshot reference, operator confirmation, plan-preview hash, output path, or target triple-check exists in this script.
- The runbook/hardened wrapper pattern in this branch requires exact token/env/mutex/operator-confirmation/DR checks before production touch (`scripts/r6_prod_cutover_e2e_wrapper.py:197-214`, `:228-235`, `:577-584`), and those wrapper guards have targeted tests (`backend/tests/scripts/test_r6_prod_cutover_e2e_wrapper.py`, 31 passed).
- `docs/operations/task11_real_evidence_backfill_sop_20260512.md` on `origin/main` already identifies the same guard gap: hard-coded credentials, no confirm token/env/mutex/DR/operator confirmation, and no structured JSON report (`:30-40`).

Risk:

- A single typo-free `--apply` is sufficient to open the production-like target and execute deletes. This is far below the guard parity used for other R6 production executors.

Required before PASS:

- Replace raw `sys.argv` parsing with `argparse`.
- Require exact `--confirm-apply` token, explicit `--target-db prod`, DB args from CLI/env, password-env only, mutex env, prod-apply env, operator confirmation text, verified DR snapshot path/ref, and expected package/row-count confirmation.
- Fail closed before DB connect if any guard is missing.

## Layer 2 - Dry-Run and Apply-Mode Correctness

Verdict: BLOCKED.

Evidence:

- Dry-run is not a no-touch preview. The script connects to DB and executes read queries before it reaches the dry-run return (`origin/main:scripts/r6_cutover_synthetic_evidence_rollback.py:33-69`).
- Dry-run returns without an explicit `conn.rollback()` or `conn.close()` (`:67-69`, close only appears at `:111` after apply success).
- The apply mutation block is transaction-wrapped and rolls back on exceptions inside the `try` block (`:71-109`), which is a partial positive.
- Several pre-apply DB operations happen before the `try` block (`:43-60`). Exceptions there do not hit the rollback/close path.
- `current_status = cur.fetchone()[0]` assumes the package exists (`:59-60`). If the package row is absent or the query returns no row, the script raises outside the guarded block.

Risk:

- Operators cannot safely use dry-run as an offline or no-DB static preview.
- Dry-run can leave an open transaction/connection until interpreter teardown.
- Error paths before the apply `try` lack structured failure, rollback, and close handling.

Required before PASS:

- Add a true offline plan mode that reads a provided row-set preview artifact and never opens DB.
- If DB preview is retained, require explicit `--db-preview` or equivalent, open read-only, close in `finally`, and emit JSON.
- Wrap all DB work in `try/finally`; rollback read transactions where applicable.

## Layer 3 - Package Filter and Cutover Artifact Semantics

Verdict: BLOCKED.

Evidence:

- Handoff docs state the synthetic row set is 2 `package_asset`, 1 `package_runtime_variant`, 3 `package_validation_run`, 1 `package_status_event`, and 1 package status update for `pkg_5a5c` (`origin/main:docs/handoff/r6_prod_cutover_20260512_state.md:35-45`).
- Handoff docs say soft rollback removes "the 7 synthetic rows + reverts pkg_5a5c to BACKTEST_APPROVED" (`origin/main:docs/handoff/r6_prod_cutover_20260512_state.md:55-62`).
- Script counts `package_status_event` rows with `reason='synthetic_evidence_9:30_sanity'` (`origin/main:scripts/r6_cutover_synthetic_evidence_rollback.py:55-57`) but never deletes those rows in apply mode (`:72-104`).
- Script inserts a new `synthetic_evidence_rollback` status event when reverting status (`:95-100`), so status-event rows increase rather than the documented 7 synthetic rows being removed.
- Script header claims detection includes `evidence_json` caveats and ID patterns (`:7-9`), but the implementation does not use `evidence_json`, `synth_20260512_*`, `var_synth_*`, or `vr_synth_*` filters. It uses only `created_by='strategy_session_9:30'` for validation/runtime rows and `metadata->>'caveat'` for assets (`:43-57`, `:72-82`).
- Script header lists `promotion_review` as a rollback target (`:11-13`), but no `promotion_review` count or delete exists in the implementation.
- The target script is not in the current cleanup branch `HEAD`; it must be inspected from `origin/main`. That means the cleanup branch cannot test or reference the working-tree path without first synchronizing code, which I did not do per instruction.

Risk:

- The implemented row set does not match the stated rollback contract.
- A status-event audit trail may be desirable to keep, but the docs and script disagree. This must be explicitly decided before production execution.
- The broad `created_by` and metadata filters depend on tags staying globally unique for the package and do not bind row IDs or expected hashes.

Required before PASS:

- Decide whether synthetic `package_status_event` rows should be immutable audit rows or deleted cleanup rows, then align docs, script, and operator checklist.
- Add row-id preview and exact row-id apply.
- Validate expected counts before and after apply, and block on any mismatch.
- Either implement `promotion_review` handling or remove it from the rollback contract.

## Layer 4 - Audit and Rollback Safety

Verdict: BLOCKED.

Evidence:

- The script prints text only. It does not emit structured JSON, report file, row IDs, before/after status, row-count assertions, query hashes, DR snapshot reference, operator identity, or confirmation hash.
- The package status revert checks only the latest status-event reason (`origin/main:scripts/r6_cutover_synthetic_evidence_rollback.py:86-90`) and then unconditionally updates the package by package_id (`:91-94`).
- There is no `SELECT ... FOR UPDATE`, no compare-and-set `WHERE package_status='PAPER_ENABLED'`, and no guard against a concurrent real transition between the latest-event check and the status update.
- There is no check that real evidence backfill is absent/present in the expected order. The script docstring says it runs after real evidence/backfill (`:3-5`), while the handoff doc says it should run before applying real backfill (`origin/main:docs/handoff/r6_prod_cutover_20260512_state.md:9-13`), and the Task 11 SOP says it is an apply gate before production real backfill (`origin/main:docs/operations/task11_real_evidence_backfill_sop_20260512.md:130-159`).
- `docs/operations/task11_real_evidence_backfill_sop_20260512.md` already says to stop before production writes until Task 10 verify is READY, user/strategy authorizes, DB operator confirms DR snapshot, real inputs are checksummed, no manual gaps remain, and rollback dry-run/apply path is reviewed for production guard parity (`:19-29`).

Risk:

- A concurrent or already-applied real transition could be overwritten back to `BACKTEST_APPROVED`.
- Lack of row-id audit makes rollback review hard after the fact.
- Contradictory operation order increases chance of running rollback after real backfill or leaving synthetic evidence mixed with real evidence.

Required before PASS:

- Use an explicit transaction with row locks on `strategy_pkg.package` and selected status/evidence rows.
- Add compare-and-set status revert, for example status must still be `PAPER_ENABLED` and latest status event must still be the synthetic event at lock time.
- Emit a JSON artifact with target, DR snapshot ref, operator confirmation hash, exact row IDs, before/after counts, and final status.
- Make operation order unambiguous: rollback synthetic rows before real backfill, then run hardened real-evidence executor, then re-enable paper through the normal governed path.

## Layer 5 - Operational Recommendation

Verdict: BLOCKED / NO-GO for execution.

Recommendation:

1. Do not run `scripts/r6_cutover_synthetic_evidence_rollback.py` in dry-run or apply mode from Codex.
2. Treat `docs/operations/task11_real_evidence_backfill_sop_20260512.md:171-173` as controlling guidance: do not execute production rollback or real backfill yet.
3. Convert this rollback into a hardened executor with the same guard parity as `scripts/r6_prod_cutover_e2e_wrapper.py` and the production backfill executors.
4. Require a DB-operator row-id preview against the current production state, with DR snapshot reference and typed approval, before any delete/update.
5. Block real evidence backfill until synthetic rollback produces a reviewed JSON artifact proving expected row IDs/counts, package final status, and no synthetic caveat rows remain.

Minimum acceptance checklist for a future PASS:

- Target script exists in the active worktree branch.
- Static syntax parse passes.
- Offline dry-run opens no DB connection.
- DB preview mode is separately guarded, read-only, and emits JSON.
- Apply mode fails closed before DB connect without token/env/mutex/DR/operator confirmation.
- Apply mode uses exact row IDs and expected counts.
- Package status revert is locked and compare-and-set guarded.
- Docs agree on status-event retention/deletion and operation order.
- Direct unit tests cover default no-touch dry-run, guard failures before DB connect, row-set mismatch, rollback-on-error, status race/CAS failure, and JSON report schema.

## Final Status

BLOCKED. Static verification found production guard, dry-run, semantic, and audit-safety gaps. No DB/service/prod execution was performed.
