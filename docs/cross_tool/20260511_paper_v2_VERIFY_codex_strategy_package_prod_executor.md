# [VERIFY] Claude paper-v2 5-layer verify of Codex strategy_package PROD executor (2fb81b3)

> **from**: paper-v2 team Lead (cross-test teammate)
> **to**: Codex (cross-tool review v3 protocol §6 dual-party verify) + strategy session
> **date**: 2026-05-11
> **target**: codex/qe-governance-integration-20260509@2fb81b3 "feat(qe): add production evidence backfill executor"
> **reviewer branch**: claude/paper-v2-baseline-post-r5-20260511 (HEAD e8ffbdd)
> **SLA**: ≤60 min (9:30 9-am tomorrow 实盘 apply gate)

## §0 Verdict

**READY**

Per-layer:
- L1 Static (5-guard real + strict): **PASS**
- L2 Tests (24 expected): **PASS (24/24)**
- L3 Dry-run JSON: **PASS** (deterministic, db_connection_opened=false)
- L4 Semantic + runbook §7.2 (5 fields): **PASS (5/5)**
- L5 ALGO-COMPLEXITY bounded: **PASS** (4-package bound enforced by plan_preview schema validation, all SQL parameterized + bounded by package_id / natural_key / LIMIT 1)

R6 prod apply readiness: **GO** (with operator following runbook §7.2 verbatim; do not deviate flag set).

Caveat (non-blocking): the 4 production package_ids (pkg_006a / pkg_1de3 / pkg_9914 / pkg_b668) are NOT hardcoded as Python constants in the executor — they are bound by the approved plan_preview JSON the operator supplies at apply time. This is **by design** and acceptable for prod safety because (a) plan_preview is a release-commander-reviewed artifact stored in SECURE_EVIDENCE_DIR; (b) executor enforces `package_count == EXPECTED_PACKAGE_COUNT == 4` (executor.py:33,248,294); (c) operator_confirmation must enumerate every package_id literally (executor.py:364–365); (d) bundle alignment is strict (executor.py:270–287). Recommend: operator verify plan_preview package_ids match the canonical 4 prod IDs in the cutover log step 7.2.2 (already required by runbook line 411).

## §1 Scope + commit anchors

- Codex worktree: `F:\Dev\AIstock_worktrees\qe-governance-integration-20260509\` (read-only)
- Codex HEAD verified: `2fb81b3 feat(qe): add production evidence backfill executor`
- Files added by 2fb81b3 (7 files, +2802):
  - `scripts/strategy_package_governance_evidence_backfill_prod_executor.py` (+782)
  - `backend/tests/scripts/test_strategy_package_governance_evidence_backfill_prod_executor.py` (+542, 24 tests)
  - `docs/operations/r6_prod_apply_runbook_20260511.md` (modified, +35/−8)
  - `docs/cross_tool/20260511_codex_to_claude_INFO_r6_prod_apply_runbook_handoff.md` (+40)
  - `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/governance_backfill_bundle_fixture.json` (+409)
  - `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/strategy_package_governance_evidence_backfill_plan_dry_run.json` (+866)
  - `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/strategy_package_governance_evidence_backfill_prod_executor_default_dry_run.json` (+136)
- Reviewer branch HEAD: `e8ffbdd` (baseline-post-r5).

## §2 Layer 1 — Static review

Read end-to-end (`scripts/strategy_package_governance_evidence_backfill_prod_executor.py`, 782 lines).

### §2.1 5-guard chain (actually a 7-guard chain — Codex spec mentions 7, user prompt said "5"; reconciled to 7 below)

Reconciliation: user prompt's "5-guard" loose-counts; source has 7 distinct guards in `_require_apply_guards()` (executor.py:368–376) PLUS DR snapshot validation + operator confirmation + plan-required check in `main()` (executor.py:751–760). All run **fail-fast BEFORE any psycopg2.connect()** (connect only happens inside `run_apply()` at executor.py:620).

Per-guard cite (BEFORE connect, in order):
1. **--apply flag** — main.py:747 `if not args.apply: report = run_preview(...)` — dry-run path NEVER calls _require_apply_guards.
2. **Exact confirm-apply token** — executor.py:370 `_require(args.confirm_apply == CONFIRM_APPLY, ...)` where `CONFIRM_APPLY = "APPLY_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD"` (executor.py:30).
3. **Env apply enabled** — executor.py:371 `_require(_env_truthy(ENV_APPLY_ENABLED), ...)` (`AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD_APPLY_ENABLED=true`).
4. **Env mutex held** — executor.py:372 `_require(_env_truthy(ENV_MUTEX_HELD), ...)` (`AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_MUTEX_HELD=true`).
5. **Target DB triple-check (prod + port 5432 + non-dev dbname + explicit host)** — executor.py:373–376 (4 sub-checks bundled).
6. **Plan preview required** — main.py:751 `_require(plan_payload is not None, ...)`.
7. **DR snapshot verified** — main.py:752 `_validate_dr_snapshot(args)` calls executor.py:316–332 which loads JSON, asserts status ∈ {verified,passed,completed}, and computes snapshot_ref.
8. **Operator confirmation scope** — main.py:753–760 `_require_operator_confirmation_scope(...)` enforces token, target label, plan_sha, dr_snapshot_ref, and every package_id literally present in confirmation text (executor.py:350–365).

Each guard is independent (uses `_require()` raise-on-false; no try/except swallows them). Ordering verified: all 8 fail-fast BEFORE `_connect(target)` at executor.py:620. **PASS**.

### §2.2 Dry-run truly offline

- `main()` (executor.py:737–778): if `not args.apply`, calls `run_preview(...)` (executor.py:577) which does NOT call `_connect()`.
- `_connect()` only invoked from `run_apply()` (executor.py:620). Verified via grep: `_connect(` appears only at line 200 (def) and line 620 (call inside run_apply).
- Plan & bundle loaded from filesystem via `_load_json()` (executor.py:140–152) — no DB.
- L3 reproduces shipped dry-run JSON byte-identically on SHA fields → confirms determinism (see §4).

**PASS**.

### §2.3 Audit emit unconditional

- Apply path (`run_apply`, executor.py:609–682) appends to `audit_rows` BOTH on success (executor.py:627 `audit_rows.append(_apply_package(...))`) and on exception (executor.py:633–648 `audit_rows.append({..., "status": "rolled_back", "error": failure_error, ...})`).
- Try/except wraps every per-package call; rollback called via `getattr(conn, "rollback", None)` before logging.
- Final report.audit_rows + report.transactions populated unconditionally (executor.py:670–679).
- Failure_payload also emits a JSON record even on top-level exception (executor.py:773–778).

**PASS**.

### §2.4 Per-package transaction

- `_apply_package` (executor.py:490–520): opens `with conn.cursor() as cur:`, performs SELECT FOR UPDATE + INSERT loop + COUNT, then `conn.commit()` at executor.py:505.
- On exception inside the per-package loop (executor.py:628), `conn.rollback()` is invoked before moving to next package — but the code `break`s out of the loop after rollback (executor.py:648), so only the failed package rolls back; previously-committed packages stay committed (correct atomicity per package).
- Connection close in `finally` block (executor.py:649–652).

**PASS**.

### §2.5 SQL safety / no silent fallback

- All SQL uses parameterized `%s` placeholders (no f-string-injected values). The f-string is used only for whitelisted table names and column identifiers, each routed through `_safe_identifier()` (executor.py:407–409) regex `^[a-zA-Z_][a-zA-Z0-9_]*$`.
- `ALLOWED_TABLES` whitelist (executor.py:35–40) enforced at row-normalize time (executor.py:223).
- `DANGEROUS_SQL_VERBS` blacklist applied to any operator-reviewed SQL package (executor.py:58–69, 379–396).
- No bare `except: pass`. The two `except Exception` instances both re-raise as `GovernanceEvidenceBackfillProdExecutorError` with chained context (executor.py:150, 173, 207) — only executor.py:172 has `# pragma: no cover - fallback for tests` which returns a JSON string when psycopg2.Json unavailable; this is a test-only adapter, not silent failure.

**PASS**.

## §3 Layer 2 — Unit tests (24 expected)

```
============================= test session starts =============================
platform win32 -- Python 3.12.12, pytest-9.0.3, pluggy-1.6.0
collecting ... collected 24 items
...
============================= 24 passed in 0.22s ==============================
```

All 24 tests pass: 0 failed. Test names confirm coverage of every guard (apply_requires_exact_confirmation_token, apply_requires_mutex_guard, apply_requires_enabled_environment_flag, apply_requires_prod_target_triple_check, apply_refuses_unverified_dr_snapshot, apply_refuses_blocked_plan_preview, apply_refuses_unapproved_operator_confirmation, apply_requires_operator_confirmation_scope_before_connect, apply_uses_one_transaction_per_package, apply_rolls_back_only_failed_package_transaction, prod_executor_does_not_import_or_delegate_to_dev_locked_apply_scripts, sql_package_validation_rejects_destructive_or_ddl_sql ×3, etc.).

**PASS**.

## §4 Layer 3 — Dry-run JSON

Re-ran shipped fixtures via `python scripts/strategy_package_governance_evidence_backfill_prod_executor.py --evidence-bundle <fixture> --plan-preview <plan_dry_run>`. Compared rerun output vs shipped `..._default_dry_run.json`:

| Field | Shipped | Rerun | Match |
|---|---|---|---|
| status | passed | passed | ✓ |
| db_connection_opened | False | False | ✓ |
| db_writes_executed | False | False | ✓ |
| plan_preview_sha256 | af06bf93… | af06bf93… | ✓ (deterministic) |
| evidence_bundle_sha256 | 3c768ac7… | 3c768ac7… | ✓ (deterministic) |
| package_count | 4 | 4 | ✓ |
| package_ids | [pkg_1..pkg_4] | [pkg_1..pkg_4] | ✓ |
| ddl | False | False | ✓ |
| production_services_touched | False | False | ✓ |
| mode | dry_run | dry_run | ✓ |
| target_db | prod | prod | ✓ |

Fixture uses synthetic pkg_1..pkg_4 (not real prod IDs). This is correct — fixtures must not embed prod package_ids. Real prod IDs will live in `<SECURE_EVIDENCE_DIR>/r6_evidence_backfill_plan.json` supplied at apply time.

**PASS**.

## §5 Layer 4 — Semantic + runbook §7.2

Cross-check between `scripts/strategy_package_governance_evidence_backfill_prod_executor.py` and `docs/operations/r6_prod_apply_runbook_20260511.md` §7.2 (lines 406–451):

| Field | Source | Runbook §7.2 | Match |
|---|---|---|---|
| Script path | `scripts/strategy_package_governance_evidence_backfill_prod_executor.py` | line 412, 423 | ✓ |
| --apply flag | argparse `--apply` (executor.py:687) | line 424 | ✓ |
| --confirm-apply token literal | `APPLY_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD` (executor.py:30) | line 425 (single-quoted) | ✓ |
| Env apply enabled | `AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD_APPLY_ENABLED` (executor.py:31) | line 420 | ✓ |
| Env mutex held | `AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_MUTEX_HELD` (executor.py:32) | line 421 | ✓ |
| --target-db | choices=('dev','prod'), default='prod' (executor.py:695) | line 431 ('prod') | ✓ |
| --db-port | int, default=5432 (executor.py:697) | line 433 ('5432') | ✓ |
| --db-password-env | default='AISTOCK_PROD_DB_PASSWORD' (executor.py:701) | line 436 | ✓ |
| Operator confirmation scope | token + DB label/name + plan_sha + dr_snapshot_ref + every package_id (executor.py:359–365) | line 450 | ✓ |

All 9 fields match (5 explicitly requested + 4 implicit). Runbook is a faithful operator recipe of the source CLI. **PASS**.

## §6 Layer 5 — ALGO-COMPLEXITY (KEY CHECK)

### §6.1 4 package_ids: hardcoded / config-bound / DB-queried?

**Plan-input-bound** (not hardcoded, not DB-queried unbounded). The 4 production package_ids live in `<SECURE_EVIDENCE_DIR>/r6_evidence_backfill_plan.json`, supplied at apply time via `--plan-preview`. The executor enforces:

- `EXPECTED_PACKAGE_COUNT = 4` constant (executor.py:33).
- Plan-preview validation requires `package_count == 4` (executor.py:294).
- Bundle must have `== 4` packages, all distinct (executor.py:248, 256).
- Bundle ↔ plan_preview package_id set must match exactly (executor.py:287).
- Operator confirmation must enumerate every package_id literally in the typed confirmation text (executor.py:364–365). Operator cannot accidentally apply to 5 or 3 packages.
- Each package's `package_status` must be in `ALLOWED_PACKAGE_STATUSES = {BACKTEST_APPROVED, SELECTION_ENABLED, PAPER_ENABLED}` (executor.py:34, 258).

This is **stronger** than hardcoded constants because the actual package_ids are reviewed by the release commander out-of-band and cryptographically pinned by `plan_preview_sha256` which the operator must type in the confirmation. Trade-off: a compromised plan_preview file could redirect to different package_ids — but that file lives in SECURE_EVIDENCE_DIR and is gated by the operator confirmation pipeline. Acceptable.

**No DB query produces the package_id list** — they always come from offline-reviewed plan_preview JSON.

### §6.2 Unbounded SQL scan check

Every SQL statement in the executor:

| Line | SQL | Boundedness |
|---|---|---|
| 439 | `SELECT {select_cols} FROM {row.table} WHERE {natural_key_where} LIMIT 1` | LIMIT 1 + WHERE on natural_key |
| 461 | `INSERT INTO {row.table} ({names}) VALUES ({placeholders})` | single row insert |
| 466–473 | `SELECT package_id, manifest_sha256, package_status FROM strategy_pkg.package WHERE package_id = %s FOR UPDATE` | WHERE = single id |
| 502 | `SELECT COUNT(*) … WHERE package_id = %s AND manifest_sha256 = %s` | WHERE on (id, manifest) — bounded |

**No UNBOUNDED SELECT/UPDATE/DELETE found.** All table names are whitelisted (`ALLOWED_TABLES`, executor.py:35–40). All identifiers regex-validated (executor.py:407–409). All values parameterized.

### §6.3 Unbounded loop check

`run_apply()` main loop: `for package in packages` (executor.py:625) where `packages` has been validated to have exactly 4 entries. Per-package work:
- `_lock_and_check_package`: 1 SELECT FOR UPDATE
- `_insert_row` per row: 1 SELECT + (conditional) 1 INSERT
- 1 COUNT(*) verification

Total DB ops: 4 packages × (1 SELECT_FOR_UPDATE + N_rows × (1 SELECT + 1 INSERT) + 1 COUNT). With ~6 rows per package per shipped fixture: ~4 × (1 + 12 + 1) = ~56 DB ops total. **Bounded and small.**

No `fetch_all_packages()` or analogous fan-out. No nested loops. No recursion.

### §6.4 Verdict + severity

**L5 PASS.** The "bounded to 4 packages" claim is **literally true** in the source. Production safety risk from unbounded scan = **NONE**.

## §7 Boundary confirmations

- codex_code_modified=**false** (read-only inspection of Codex worktree)
- prod_db_touched=**false** (only ran tests in-process with mock cursors + dry-run with `db-host=prod-db.invalid` which never resolves; dry-run path never calls `_connect()`)
- dev_db_writes=**false** (no INSERT executed; pytest uses fakes, dry-run is offline)
- prod_8001_touched=**false**
- --apply never invoked=**true** (only `--help` and default dry-run)
- codex_branch_merged=**false**
- baseline-post-r5 worktree: only this doc written; no source code modified
- `frontend/tsconfig.tsbuildinfo` NOT staged
