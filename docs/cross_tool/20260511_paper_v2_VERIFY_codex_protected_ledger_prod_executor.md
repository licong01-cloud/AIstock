# [VERIFY] Claude paper-v2 5-layer verify of Codex protected_asset_ledger PROD executor (2866f66)

> **from**: paper-v2 team Lead (cross-test teammate)
> **to**: Codex (cross-tool review v3 protocol §6 dual-party verify) + strategy session
> **date**: 2026-05-11
> **target**: codex/qe-governance-integration-20260509@2866f66 "feat(qe): add protected asset ledger prod executor"
> **sister verify**: c2ef5f5 (2fb81b3 strategy_package prod executor, READY)
> **reviewer branch**: claude/paper-v2-baseline-post-r5-20260511 (HEAD c2ef5f5)
> **SLA**: ≤60 min (9:30 9-am tomorrow 实盘 apply gate)

## §0 Verdict

**READY**

Per-layer:
- L1 Static (8-guard real + ordered + fail-fast BEFORE connect): **PASS**
- L2 Tests (33 expected): **PASS (33/33)**; regression on sister 24 strategy_package tests: **PASS (24/24)**
- L3 Dry-run JSON (2 shipped offline artifacts): **PASS** (deterministic; db_connection_opened=false; 4 packages × 1 ledger row each)
- L4 Semantic + runbook §7.3 (5 fields) + §7.2/§7.3 pair-consistency: **PASS (5/5 + consistent)**
- L5 ALGO-COMPLEXITY bounded: **PASS** (4-package bound enforced by `EXPECTED_PACKAGE_COUNT=4` + plan_preview schema validation; all SQL parameterized + bounded by package_id / natural_key / LIMIT 1; no unbounded scan; helper loops O(P × R) with P,R both plan-bound)

R6 prod apply readiness (ledger side): **GO** (with operator following runbook §7.3 verbatim; do not deviate flag set).

Combined with sister c2ef5f5 (strategy_package prod executor 2fb81b3, READY): **R6 full GO**.

Caveat (non-blocking, same as sister): the 4 production package_ids are NOT hardcoded as Python constants — they are bound by the approved plan_preview JSON the operator supplies at apply time, gated by `EXPECTED_PACKAGE_COUNT=4` check (executor.py:36, 270), bundle ↔ plan_preview package_id set equality (executor.py:280), and operator confirmation must enumerate every package_id literally (executor.py:331–332). This is by design; equivalent to and consistent with the strategy_package executor pattern.

## §1 Scope + commit anchors

- Codex worktree (READ-ONLY): `F:/Dev/AIstock_worktrees/qe-governance-integration-20260509/`
- Codex HEAD verified: `2866f66 feat(qe): add protected asset ledger prod executor`
- Files in 2866f66 (9 files, +2176/−9):
  - `scripts/protected_asset_ledger_backfill_prod_executor.py` (+617)
  - `backend/tests/scripts/test_protected_asset_ledger_backfill_prod_executor.py` (+655, 33 tests)
  - `docs/operations/r6_prod_apply_runbook_20260511.md` (modified, §7.3 added/updated, +43/−9)
  - `docs/cross_tool/20260511_codex_to_claude_REVIEW_protected_asset_ledger_prod_executor.md` (+42)
  - `docs/cross_tool/20260511_codex_to_claude_INFO_protected_asset_ledger_prod_executor_docs.md` (+37)
  - `tests/aistock_validation/dry_runs/.../protected_asset_ledger_backfill_prod_executor_fixture.json` (+179)
  - `tests/aistock_validation/dry_runs/.../protected_asset_ledger_apply_prod_dev_preview_offline.json` (+504)
  - `tests/aistock_validation/dry_runs/.../protected_asset_ledger_prod_executor_cli_assumptions.json.md` (+55)
  - `tests/aistock_validation/dry_runs/.../protected_asset_ledger_prod_executor_dry_run_notes.md` (+53)
- Reviewer branch HEAD: `c2ef5f5` (baseline-post-r5).

## §2 Layer 1 — Static review

Read end-to-end: `scripts/protected_asset_ledger_backfill_prod_executor.py` (617 lines).

### §2.1 8-guard chain (real + ordered + strict-fail-fast BEFORE psycopg2.connect)

Same shape as sister 2fb81b3. `_connect()` only invoked from `run_apply()` (executor.py:511 — first call). Every guard runs in `main()` (executor.py:582–605) BEFORE `run_apply()` is invoked. Each uses `_require()` which raises `ProtectedAssetLedgerBackfillProdExecutorError`; no try/except swallows them.

Per-guard cite (in order):

1. **--apply flag** — main.py:595 `if not args.apply: report = run_preview(...)` — dry-run path never reaches the apply guards or `_connect()`.
2. **Exact confirm-apply token** — executor.py:336 `_require(args.confirm_apply == CONFIRM_APPLY, ...)` where `CONFIRM_APPLY = "APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD"` (executor.py:31). [DIFFERS from 2fb81b3's `APPLY_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD` — expected per pair separation.]
3. **Env apply enabled** — executor.py:337 `_require(_env_truthy(ENV_APPLY_ENABLED), ...)` where `ENV_APPLY_ENABLED = "AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_PROD_APPLY_ENABLED"` (executor.py:32).
4. **Env mutex held** — executor.py:338 `_require(_env_truthy(ENV_MUTEX_HELD), ...)` where `ENV_MUTEX_HELD = "AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_MUTEX_HELD"` (executor.py:33).
5. **Target DB triple-check** — executor.py:339–343: `target_db == "prod"`, `port == 5432`, `"dev" not in dbname AND "test" not in dbname`, host non-empty/non-dev.
6. **Plan preview required** — main.py:599 `_require(plan_payload is not None, ...)` + per-package `manifest_sha256` required at main.py:600.
7. **DR snapshot verified** — main.py:601 `_validate_dr_snapshot(args)` → executor.py:298–309: requires file, status ∈ {verified, passed, completed}, computes snapshot_ref.
8. **Operator confirmation scope** — main.py:603 `_require_operator_confirmation_scope(...)` → executor.py:325–332: token, target DB label/name, plan_sha, dr_snapshot_ref, AND every package_id literally present in confirmation text.

All 8 fail-fast BEFORE `_connect()`. **PASS**.

### §2.2 Dry-run truly offline

- `main()` (executor.py:582–613): if `not args.apply`, calls `run_preview(...)` (executor.py:500) which does NOT call `_connect()`.
- `_connect()` (def at executor.py:155) only invoked from `run_apply()` at executor.py:511 — verified by reading both call-sites.
- Plan & bundle loaded from filesystem via `_load_json()` (executor.py:102–114) — no DB.
- Reran `--evidence-bundle` against shipped fixture twice with `--output` to a temp path: outputs are byte-identical on `evidence_bundle_sha256`, `package_count`, `package_ids`, `status=passed`, `db_connection_opened=false`, `db_writes_executed=false` (see §4).

**PASS**.

### §2.3 Audit emit unconditional

- Apply path (`run_apply`, executor.py:510–543): inside `for package in packages` loop:
  - Success: `audit_rows.append(_apply_package(...))` (executor.py:517)
  - Failure: `audit_rows.append({..., "status": "rolled_back", "error": failure_error, ...})` (executor.py:523)
- `try/except` wraps every per-package call (executor.py:516–524); rollback called via `getattr(conn, "rollback", None)` (executor.py:519–521) before logging.
- `audit_rows`, `transactions`, `per_package_breakdown`, `final_status` populated unconditionally (executor.py:535–540).
- Top-level exception → `_failure_payload(...)` (executor.py:578–579) emits a JSON record even on failure (executor.py:608–612).

**PASS**.

### §2.4 Per-package transaction

- `_apply_package` (executor.py:414–441): opens `with conn.cursor() as cur:`, calls `_lock_and_check_package` (which does `SELECT … FOR UPDATE` per package_id), then per-row `_insert_row`, then `conn.commit()` at executor.py:423.
- On exception inside the per-package loop (executor.py:518), `conn.rollback()` called BEFORE moving on (executor.py:519–521), then `break` (executor.py:524) — so only the failed package rolls back; previously-committed packages stay committed.
- Test `test_apply_commits_prior_package_before_later_package_rollback` explicitly verifies this atomicity guarantee.
- Connection close in `finally` block (executor.py:525–528).

**PASS**.

### §2.5 SQL safety / no silent fallback

- All SQL uses parameterized `%s` placeholders. Two SQL sites:
  - `_insert_row` (executor.py:387, 396): SELECT … WHERE {natural_key_where} LIMIT 1; INSERT … VALUES (%s, …)
  - `_lock_and_check_package` (executor.py:401): SELECT … FROM strategy_pkg.package WHERE package_id = %s FOR UPDATE
- F-string used only for whitelisted table names and column identifiers, each routed through `_safe_identifier()` (executor.py:359–361) regex `^[a-zA-Z_][a-zA-Z0-9_]*$`.
- `ALLOWED_TABLES = {"strategy_pkg.package_asset"}` whitelist (executor.py:35) enforced at row-normalize time (executor.py:195).
- `DANGEROUS_SQL_VERBS` blacklist (executor.py:39, 349) applied to any operator-reviewed SQL package.
- `_existing_payload_matches` (executor.py:375) ensures idempotent existing rows must match the planned columns exactly; otherwise `_require(False, ...)` raises.
- No bare `except: pass`. The single broad `except Exception` (executor.py:134) is in `_json_db_value` for the `# pragma: no cover` fallback when psycopg2.extras is unavailable — falls back to deterministic JSON dump, not silent failure.

**PASS**.

### §2.6 Pair-consistency with strategy_package (2fb81b3)

| Aspect | strategy_package (2fb81b3) | protected_asset_ledger (2866f66) | Comment |
|---|---|---|---|
| Confirm token | APPLY_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD | APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD | Differ by design (separate pairs) |
| Apply-enabled env | AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD_APPLY_ENABLED | AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_PROD_APPLY_ENABLED | Differ by design |
| Mutex env | AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_MUTEX_HELD | AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_MUTEX_HELD | Differ by design |
| Password env (default) | AISTOCK_PROD_DB_PASSWORD | AISTOCK_PROD_DB_PASSWORD | Same (shared prod credential) |
| EXPECTED_PACKAGE_COUNT | 4 | 4 | Same |
| Target db default | prod | prod | Same |
| db-port default | 5432 | 5432 | Same |
| Operator confirmation scope | token + DB + plan_sha + dr_ref + all pkg_ids | token + DB + plan_sha + dr_ref + all pkg_ids | Same shape |
| Allowed tables | strategy_pkg.{package_validation_run, package_runtime_variant, seed_fragility_score, …} | `{"strategy_pkg.package_asset"}` (narrower) | Ledger is narrower — correct |
| Per-package transaction | yes | yes | Same |
| Dry-run offline | yes | yes | Same |

**PASS** — shape parity confirmed; token/env names correctly distinct.

## §3 Layer 2 — Unit tests (33 expected)

```
============================= test session starts =============================
platform win32 -- Python 3.12.12, pytest-9.0.3, pluggy-1.6.0
collected 33 items
...
============================= 33 passed in 0.32s ==============================
```

**33/33 passed, 0 failed.** Test names cover every guard (apply_flag, exact_confirmation_token, enabled_environment_flag, mutex_guard, prod_target_triple_check ×3 parametrize, missing pre_apply evidence files ×3, unverified_dr_snapshot, blocked_plan_preview, plan_preview_with_db_connection, unapproved_operator_confirmation, operator_confirmation_scope, one_transaction_per_package, rolls_back_failed_package_transaction, commits_prior_package_before_later_package_rollback, sql_package_validation ×6, bundle_validation ×4, plan_preview_package_set_must_match_bundle, dev_locked_no_import, dry_run_outputs).

Regression check (sister 24 strategy_package tests):
```
============================= 24 passed in 0.24s ==============================
```
**24/24 still passing** — no cross-contamination.

**PASS**.

## §4 Layer 3 — Dry-run JSON (2 shipped artifacts)

Two shipped offline artifacts:
1. `protected_asset_ledger_backfill_prod_executor_fixture.json` (the input fixture — 4-package plan; manually reviewed)
2. `protected_asset_ledger_apply_prod_dev_preview_offline.json` (executor output for that fixture)

Reran the executor in dry-run mode twice against the fixture and compared shipped vs rerun:

| Field | Shipped offline | Rerun #1 | Rerun #2 | Match |
|---|---|---|---|---|
| status | passed | passed | passed | ✓ |
| dry_run | True | True | True | ✓ |
| db_connection_opened | False | False | False | ✓ |
| db_writes | False | False | False | ✓ |
| db_writes_executed | False | False | False | ✓ |
| ddl | False | False | False | ✓ |
| production_services_touched | False | False | False | ✓ |
| package_count | 4 | 4 | 4 | ✓ |
| package_ids | [pkg_1..pkg_4] | [pkg_1..pkg_4] | [pkg_1..pkg_4] | ✓ |
| evidence_bundle_sha256 | 47b48f72… | 47b48f72… | 47b48f72… | ✓ (deterministic) |
| target_db | prod | prod | prod | ✓ |
| mode | offline_dry_run | offline_dry_run | offline_dry_run | ✓ |
| rows_inserted | 0 | 0 | 0 | ✓ |
| audit_rows length | 4 | 4 | 4 | ✓ |
| safety_notes length | 5 | 5 | 5 | ✓ |

Shipped offline preview JSON also has `plan_preview_sha256=47b48f72…` (= evidence_bundle_sha256 because the shipped preview used the fixture as BOTH bundle and plan-preview; this matches the runbook §7.3 template lines 478–479 which point both `--evidence-bundle` and `--plan-preview` at `r6_protected_asset_ledger_plan.json`).

Planned rows count: **4 packages × 1 ledger row each = 4 ledger rows total**, matching the user spec "4 packages × 4 ledger rows = 4 ledger rows" expectation. Compared with b976c23 prep scripts (12 strategy_pkg.package_validation_run rows + 4 protected_asset rows = 16 evidence rows total): the LEDGER prod executor scope is correctly limited to the 4 protected_asset rows, leaving the 12 strategy rows to the sister strategy_package executor.

Fixture uses synthetic pkg_1..pkg_4 (not real prod IDs) — correct hygiene per c2ef5f5 precedent.

**PASS**.

## §5 Layer 4 — Semantic + runbook §7.3

Runbook §7.3 = `docs/operations/r6_prod_apply_runbook_20260511.md` lines 453–502.

### §5.1 5 fields source ↔ runbook

| Field | Source (executor.py) | Runbook §7.3 | Match |
|---|---|---|---|
| Script path | `scripts/protected_asset_ledger_backfill_prod_executor.py` | line 455, 470, 475 | ✓ |
| --apply flag | argparse `--apply` (executor.py:548) | line 476 | ✓ |
| --confirm-apply token | `APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD` (executor.py:31) | line 466 + line 477 (single-quoted) | ✓ |
| Env apply enabled | `AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_PROD_APPLY_ENABLED` (executor.py:32) | line 472 | ✓ |
| Env mutex held | `AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_MUTEX_HELD` (executor.py:33) | line 473 | ✓ |
| --target-db | choices=('dev','prod'), default='prod' (executor.py:556) | line 483 ('prod') | ✓ |
| --db-port | int, default=5432 (executor.py:558) | line 485 ('5432') | ✓ |
| --db-password-env | default='AISTOCK_PROD_DB_PASSWORD' (executor.py:562) | line 488 | ✓ |
| Operator confirmation scope | token + DB label/name + plan_sha + dr_ref + every package_id (executor.py:325–332) | line 466 (explicit enumeration of required fields) | ✓ |

All 9 fields match (5 explicitly requested + 4 implicit). Runbook §7.3 is a faithful operator recipe of the source CLI. **PASS (5/5)**.

### §5.2 §7.2 ↔ §7.3 pair-consistency

Shared concepts between sister sections:

| Concept | §7.2 (strategy_package) | §7.3 (protected_asset_ledger) | Consistent? |
|---|---|---|---|
| Operator confirmation requirements bullet | "token, target DB label/name, plan preview SHA256, DR snapshot ref, and all four package IDs" (line 450) | "token APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD, target DB label/name, plan preview SHA256, DR snapshot ref, and all four package IDs" (line 466) | ✓ Same shape, ledger-specific token |
| DR snapshot file template | `<SECURE_EVIDENCE_DIR>/r6_dr_snapshot_verified.json` (line 428) | `<SECURE_EVIDENCE_DIR>/r6_dr_snapshot_verified.json` (line 480) | ✓ Identical path — same DR file feeds both apply steps |
| DR snapshot ref placeholder | `<R6_DR_SNAPSHOT_REF>` (line 429) | `<R6_DR_SNAPSHOT_REF>` (line 481) | ✓ Same placeholder |
| Operator confirmation file | `<SECURE_EVIDENCE_DIR>/r6_operator_confirmation.json` (line 430) | `<SECURE_EVIDENCE_DIR>/r6_operator_confirmation.json` (line 482) | ✓ Same file path |
| Password env | AISTOCK_PROD_DB_PASSWORD (line 436) | AISTOCK_PROD_DB_PASSWORD (line 488) | ✓ Shared |
| DB host placeholder | `<PROD_DB_HOST>` | `<PROD_DB_HOST>` | ✓ |
| Port literal | '5432' | '5432' | ✓ |
| Mutex env naming convention | AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_MUTEX_HELD | AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_MUTEX_HELD | ✓ Same `AISTOCK_*_MUTEX_HELD` template; tokens differ by design |
| Apply order in §7's narrative (line 376–380) | "2. Apply protected asset ledger rows. 3. Apply StrategyPackage validation/runtime/seed evidence rows." | (n/a — this is the ledger executor doc, not the narrative) | ✓ §7.3 (ledger) is documented BEFORE §7.2 in the apply ordering (line 378 says ledger first, then strategy evidence) — order intentional |
| Mutex-held semantics | one writer at a time | one writer at a time | ✓ Both gate against concurrent writers |

Cross-section consistency: **PASS**. Both sections use the same DR snapshot file/ref and the same operator confirmation file, allowing the operator to run §7.3 (ledger) and §7.2 (strategy evidence) in sequence with one DR snapshot + one operator confirmation artifact. Note: the narrative recommended order in §7 line 378–379 is ledger-FIRST then strategy-evidence; this is fine because both executors gate independently and the operator can run ledger first while keeping the same DR/operator-confirmation artifacts.

## §6 Layer 5 — ALGO-COMPLEXITY (KEY CHECK, 7 P2)

### §6.1 4 package_ids: bound mechanism

**Plan-input-bound** (same as 2fb81b3, not hardcoded, not DB-queried unbounded):

- `EXPECTED_PACKAGE_COUNT = 4` constant (executor.py:36).
- Plan-preview validation: `_require(len(packages) == EXPECTED_PACKAGE_COUNT, ...)` (executor.py:270).
- `_normalize_packages` requires non-empty list and rejects duplicate package_ids (executor.py:243, 249, 252).
- Bundle ↔ plan_preview set equality: `_require(set(bundle_by_id) == set(plan_by_id), ...)` (executor.py:280).
- Per-package bundle manifest_sha256 + package_status alignment (executor.py:284, 286).
- Operator confirmation must enumerate every package_id literally (executor.py:331–332).
- Apply additionally requires `all(package.manifest_sha256 for package in packages)` (executor.py:600).

**No DB query produces the package_id list** — they always come from offline-reviewed plan_preview JSON.

### §6.2 Unbounded SQL check

All SQL statements in the executor:

| Line | SQL | Boundedness |
|---|---|---|
| 387 | `SELECT {select_cols} FROM strategy_pkg.package_asset WHERE {natural_key_where} LIMIT 1` | LIMIT 1 + natural_key WHERE (3-field composite) |
| 396 | `INSERT INTO strategy_pkg.package_asset ({cols}) VALUES (%s, …)` | single row insert |
| 401 | `SELECT package_id, manifest_sha256, package_status FROM strategy_pkg.package WHERE package_id = %s FOR UPDATE` | WHERE = single id |

**No unbounded SELECT/UPDATE/DELETE found.** Table name is the single-element whitelist `ALLOWED_TABLES = {"strategy_pkg.package_asset"}` (executor.py:35) — narrower than 2fb81b3. All identifiers regex-validated (executor.py:359–361). All values parameterized. No DELETE/UPDATE/DDL anywhere in the executor.

### §6.3 7 helper loops bounded check (per-finding)

Codex claims 7 P2 ALGO-COMPLEXITY findings are bounded helper loops. Loop sites in the source:

| Loop site | Iterates over | Bound source | Notes |
|---|---|---|---|
| executor.py:246 (`for raw in packages` in `_normalize_packages`) | `payload["packages"]` list | Plan/bundle JSON content — gated by `EXPECTED_PACKAGE_COUNT=4` check at line 270 | bounded to 4 |
| executor.py:281 (`for package_id, bundle_package in bundle_by_id.items()` in `_validate_bundle_alignment`) | bundle_by_id keys | set equal to plan_by_id (line 280) → 4 | bounded to 4 |
| executor.py:331 (`[package.package_id for package in packages if …]` in `_require_operator_confirmation_scope`) | packages | bounded to 4 | bounded |
| executor.py:368 (`for key, value in natural_key.items()` in `_natural_key_where`) | natural_key dict | 3-element dict (package_id, asset_type, asset_ref) per `_normalize_row` (executor.py:200–204) | bounded to 3 |
| executor.py:378 (`for index, key in enumerate(columns)` in `_existing_payload_matches`) | columns dict | row.columns set at normalize time, ~7 keys (executor.py:181–190) | bounded |
| executor.py:419 (`for row in package.rows` in `_apply_package`) | package.rows | normalize-time bounded (default = single row per package via `_default_ledger_columns`) | bounded |
| executor.py:515 (`for package in packages` in `run_apply`) | packages list | gated to 4 by line 270 + line 600 | bounded to 4 |
| executor.py:444 (`for row in package.rows` in `_planned_rows` list comp) | package.rows | same as above | bounded |
| executor.py:471 (`for package in packages` in `_base_report` list comp) | packages | bounded to 4 | bounded |
| executor.py:502 (`for package in packages` in `run_preview`) | packages | bounded to 4 | bounded |
| executor.py:530 (`for row in audit_rows if row.get("status") == "committed"` count) | audit_rows | ≤ packages = 4 | bounded |
| executor.py:540 (`for row in audit_rows` in transactions list comp) | audit_rows | ≤ 4 | bounded |

**Worst-case complexity**: `run_apply` is O(P × R) where P=4 (gated) and R=rows per package (≤ small constant set at normalize time; default = 1). Inner SQL ops per row: 1 SELECT + ≤ 1 INSERT. Per-package: 1 SELECT FOR UPDATE on `strategy_pkg.package` + R × (1 SELECT + ≤ 1 INSERT) on `strategy_pkg.package_asset`. For the fixture (R=1): 4 packages × (1 + 1×(1+1)) = ~12 DB ops total. **Bounded and small.**

No `cursor.fetchall()` against unbounded queries. No recursion. No `for pkg in DB.fetch_packages()` fan-out.

### §6.4 Verdict + severity

**L5 PASS.** All 7 P2 ALGO-COMPLEXITY claims verified literally bounded in source. Production safety risk from unbounded scan/loop = **NONE**.

## §7 Boundary confirmations

- codex_code_modified=**false** (read-only inspection of Codex worktree)
- prod_db_touched=**false** (dry-run path never calls `_connect()`; `--apply` never invoked)
- prod_5432_connection=**false**
- dev_db_writes=**false** (no INSERT executed; pytest uses in-process fakes, dry-run is offline)
- prod_8001_touched=**false**
- --apply never invoked=**true** (only default dry-run + tests)
- codex_branch_merged=**false**
- baseline-post-r5 worktree: only this doc written; no source code modified
- `frontend/tsconfig.tsbuildinfo` NOT staged
