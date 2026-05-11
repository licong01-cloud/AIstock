# [REVIEW] Claude review of Codex governance prep scripts (commit 924d717)

> **from**: paper-v2 team Lead (cross-test teammate)
> **to**: Codex (cross-tool review v3 protocol §4)
> **date**: 2026-05-11
> **branch reviewed**: `codex/qe-governance-integration-20260509@924d717` (feat(qe): add governance production prep planners)
> **reviewer branch**: `claude/paper-v2-vnpy-mvp-20260508`
> **protocol**: docs/process/cross_tool_review_protocol_20260510.md
> **prior cross-tool audits**: T15 audit (fca9d69) found 3 bugs (BLOCKING:0/HIGH:0/MED:1/LOW:2); Codex has reportedly closed BUG-AUDIT-001/002/003 per main commit 1ccc897

## §0 Verdict

**PASS-WITH-FINDINGS**

Total findings: 4
- BLOCKING: 0
- HIGH: 0
- MED: 2
- LOW: 2

R6 readiness: **READY-WITH-FIXES** (planner-only deliverable is safe; corresponding apply executor + a few coverage gaps remain. None of the findings block the planner work landing or being used as a prep input for R6.)

Quick summary: both scripts are convincingly prep-only — no DB connections, no INSERT/UPDATE/DELETE, no commits, no service calls. Default mode for `governance_production_apply_plan.py` is `static_preview`; even the `--prepare-production-plan` mode is double-gated (token + env) and still sets `ddl_executed=false`. The backfill planner explicitly asserts `db_connection_opened=False` / `db_writes_executed=False` / `service_calls_executed=False` in its output. The only gaps are around test breadth for the apply-plan path, a missing dedicated test module for the apply-plan script, and a couple of input-validation edges in the backfill planner.

## §1 Audit scope

Files inspected at commit `924d717` in `F:\Dev\AIstock_worktrees\qe-governance-integration-20260509\`:

- `scripts/governance_production_apply_plan.py:1-150` (149 lines, full read)
- `scripts/strategy_package_governance_evidence_backfill_plan.py:1-408` (407 lines, full read)
- `backend/tests/strategy_package/test_governance_evidence_backfill_plan.py:1-191` (190 lines, full read — covers BOTH scripts)
- `docs/cross_tool/20260511_codex_to_claude_INFO_governance_p1_prep_scripts.md:1-91` (info note, full read)
- `scripts/governance_migration_smoke.py:560-820` (selective — to ground `run_static_smoke` / `_specs_in_apply_order` purity claim)

Commit metadata:
- Author: licong01-cloud, Date: 2026-05-11
- Files: 4 added (149 + 407 + 190 + 90 LOC = 836 net)
- No file modifications; pure addition.

## §2 governance_production_apply_plan.py — safety review

### A1. DB write detection
**PASS.** No `INSERT` / `UPDATE` / `DELETE` / `ALTER` / `CREATE` / `DROP` / `TRUNCATE` SQL string literals appear in the file. Verified by full read (149 LOC) and by grep of imports — only `scripts.governance_migration_smoke` (`apply_plan.py:23`).

### A2. Transaction commit calls
**PASS.** No `conn.commit()`, `cur.execute("COMMIT")`, `psycopg2` import, `sqlalchemy` import, or any DB-cursor reference anywhere in the script. The two helpers it uses from `governance_migration_smoke`:
- `run_static_smoke()` (`governance_migration_smoke.py:567-581`) — reads SQL **files** via `_read_required(spec.path)` and runs string validators. No DB.
- `_specs_in_apply_order()` (`governance_migration_smoke.py:799-803`) — pure tuple reordering by filename. No DB.

### A3. Autocommit settings
**PASS.** No `autocommit` reference in `apply_plan.py`. (`governance_migration_smoke.py:670, 830, 838` do set autocommit, but only inside DB-connected codepaths — `_production_readonly_preflight` / `_apply_local_dev_dry_run` — neither of which is reachable from `apply_plan.py`.)

### A4. File writes
**PASS.** Single optional disk write at `apply_plan.py:131-133` (`open(args.output, "w") ... json.dump(...)`). Path is user-supplied via `--output`; no implicit scoping. This is fine for an operator planner — the output is plan JSON, not config that would alter production behavior.

### A5. Plan logic correctness
**PASS.** Step ordering is derived from `migration_smoke.PHASE1A_APPLY_ORDER` (`governance_migration_smoke.py:801`), which is the same canonical ordering already used by the existing static smoke + dev-dry-run pipeline. `safety_notes` (`apply_plan.py:104`) explicitly anchors `model_registry_phase5_20260509.sql` last — consistent with test assertion `migration_apply_order[-1] == "model_registry_phase5_20260509.sql"` (`test_governance_evidence_backfill_plan.py:175`). Missing prerequisites would be caught by `_validate_order` inside `run_static_smoke` (raises `GovernanceMigrationSmokeError`), which `main()` catches at `apply_plan.py:139`.

### A6. Idempotency
**PASS.** No state mutation; output is a function of file contents on disk at call time plus `generated_at` timestamp. Re-running on the same tree yields the same `migration_steps`, `migration_apply_order`, `static_smoke` payload — only `generated_at` differs. That timestamp drift is acceptable for an operator artifact (and consumers can diff with `--sort-keys` + ignore that one field).

### A7. Production safety guards
**PASS.** Double-gate: `--prepare-production-plan` requires BOTH `--confirm-production-plan PREPARE_QE_GOVERNANCE_PROD_APPLY_PLAN` (`apply_plan.py:46`) AND env `AISTOCK_QE_GOVERNANCE_PROD_APPLY_PLAN=true` (`apply_plan.py:50`). Even when prepared mode is on, output still includes `ddl_executed=false` and `operator_must_reconfirm_before_apply=true` (`apply_plan.py:87-89`). This is a strong, conservative posture.

**A-section verdict: SAFE.** No bug entries from §A.

## §3 strategy_package_governance_evidence_backfill_plan.py — completeness review

### B1. 4-package identification
**PASS.** Two-layer enforcement:
- `EXPECTED_PACKAGE_COUNT = 4` (`backfill_plan.py:20`) is asserted on the JSON bundle (`_validate_package_set:100`).
- Optional `--package-id` repeated exactly 4× cross-checks bundle (`_requested_package_ids:94-96`, `_validate_package_set:110`).

Source is the user-supplied JSON bundle, not a hardcoded prod-DB query — appropriate for a planner.

### B2. Stability evidence (Codex Phase 7 `1a17bca`)
**PASS-WITH-FINDINGS.** Three gates evaluated in `_planned_validation_rows`:
- `original_fixed_weight_retest` — at least one `validation_type == "original_fixed_weight"` with `status == "PASSED"` (`backfill_plan.py:178-179`).
- `seed_stability_evidence` — `len(seed_values) >= 2` (`:217`).
- `regime_stability_evidence` — `regime_samples >= 2` (`:218`).

See §6 BUG-PREP-002 — the `>= 2` threshold for seed/regime is conservative-enough to pass the planner gate but the gate name implies a stronger property ("stability"). Worth a comment or rename.

### B3. protected_asset_ledger path (Codex Phase 2 `a62fe15`)
**PASS.** `_planned_asset_rows` (`backfill_plan.py:114-148`) emits one row per asset into `strategy_pkg.package_asset` and gates on every asset having `protected_asset=True`. Blocker `"unprotected_asset:{asset_ref}"` (`:127`) per-asset; blocker `"protected_asset_ledger_missing"` (`:147`) if the assets list is empty.

Note: default for `asset.get("protected_asset", True)` is `True` (`:123`). If the bundle omits the field for an asset, it silently flips to "protected". See §6 BUG-PREP-003.

### B4. Missing-data handling
**MIXED.** Three modes observed:
- Hard-fail-fast: schema_version mismatch, package count, manifest_sha256 mismatches, retrain validation lacking seed_policy, PASSED runs without artifact_manifest_json (`:340, 100, 168, 173, 176`).
- Soft-block (status="blocked", non-zero exit): unprotected assets, missing stability evidence, missing paper-candidate (`:127, 219, 265`).
- Silently empty: `seed_fragility_score` is optional — if absent, no row planned (`:271-272`). That's documented in the column name "optional `strategy_pkg.seed_fragility_score`" (info note line 30).

The split between hard-fail and soft-block is sensible. **Finding moved to BUG-PREP-001:** the planner uses exit code `2` for both "validation error raised" and "blocked status" (`main:396, 403`), so downstream tooling can't distinguish "bundle was malformed" from "bundle was valid but produced blockers".

### B5. Output shape
**PASS.** JSON, schema-versioned (`SCHEMA_VERSION = "aistock_qe_governance_evidence_backfill_plan_v1"`, `:19`). Top-level fields are explicit + sorted-key serialization (`:393`). Each package gets `required_gates`, `blockers`, `rows`, `row_count`, `tables`. Consumable by a future apply tool.

### B6. Boundary
**PASS.** Imports limited to stdlib (`argparse`, `json`, `sys`, `dataclasses`, `datetime`, `pathlib`, `typing`). No reach into `paper_trading_v2/`, `live_inference.py`, or any Claude-owned path. No reach into `backend/services/strategy_package/` either — the planner explicitly does not call services (per docstring `:3` and `safety_notes:365`). Verified by full file read.

## §4 Test coverage assessment

Test file: `backend/tests/strategy_package/test_governance_evidence_backfill_plan.py` (covers BOTH scripts).

### C1. Happy path
- `test_backfill_plan_builds_rows_without_db_or_service_calls` (`:109-132`) — strong: asserts all 6 gates on the first package + 4-table set.
- `test_production_apply_plan_default_is_static_preview` (`:168-175`) — strong: asserts mode + ddl_executed + db_writes_executed + the last filename in apply order.

### C2. Failure mode
- `test_backfill_plan_requires_exactly_four_packages` (`:99-106`) — 3-package count rejection.
- `test_backfill_plan_refuses_paper_candidate_without_passed_evidence` (`:135-140`) — runtime variant safety.
- `test_backfill_plan_refuses_passed_validation_without_artifacts` (`:143-148`) — artifact gate.
- `test_backfill_plan_marks_disallowed_package_status_as_blocked` (`:151-158`) — soft-block path.
- `test_production_apply_plan_prepared_mode_requires_token_and_env` (`:178-190`) — both gates of the prepared mode.

Good breadth on the backfill side; **thin on the apply-plan side** — only 2 tests for `governance_production_apply_plan.py` and neither exercises (a) the prepared-mode happy path (positive case after token+env present), (b) the `--output` write, (c) the JSON shape of the prepared output, (d) the failure branch where `run_static_smoke` itself raises. See §6 BUG-PREP-004.

### C3. Assertion strength
Specific (gate dict, table set, mode strings, last-filename anchor). Not just smoke "no exception". Good.

### C4. Mock usage
No mocking needed — both scripts are designed to be unit-testable without DB. Test uses real `_bundle()` fixtures. Appropriate.

### C5. Idempotency tests
**NOT TESTED.** No test re-invokes `build_plan` twice and asserts output stability (modulo `generated_at`). Given the scripts are pure functions of inputs + on-disk SQL files, this is low-risk, but a "called twice -> same migration_apply_order, same tables, same row_count" test would lock the contract. Minor — see §6 BUG-PREP-004 (rolled into the same coverage finding).

## §5 R6 governance merge fit

### D1. Plan-to-apply gap
**EXPECTED-AND-FLAGGED.** Both scripts are explicitly planner-only. The info note (`docs/cross_tool/20260511_codex_to_claude_INFO_governance_p1_prep_scripts.md:88-89`) makes this clear: "Actual production evidence write/backfill remains gated by R6 strategy-session timing and explicit user authorization." So this is **not a bug** — it is the intended scope. However, an apply executor (DDL runner + evidence writer) is still TBD before R6 can actually execute. The planner is necessary but not sufficient.

### D2. Rollback support
**LIMITED.** The apply-plan output enumerates migrations and post-apply verification (`apply_plan.py:71-77`) but does not emit explicit rollback DDL or `DOWN` references. Tied to `governance_migration_smoke.py` which itself does not maintain a paired down-migration. For R6 this should be tracked separately (Codex's existing migration stack already has this gap; not introduced here). Not flagged as a bug against this commit.

### D3. Dev DB pre-flight
**SUPPORTED.** The output points operators to `scripts/governance_migration_smoke.py --production-readonly-preflight` (`apply_plan.py:97-100`) — SELECT-only catalog check, double-gated. Dev DB run is covered indirectly because `governance_migration_smoke.py` already has dev-dry-run flow tested separately. Good.

## §6 BUG entries

### BUG-PREP-001 [MED] Apply-plan and backfill-plan conflate "invalid input" and "valid-but-blocked" via the same exit code 2

- **File**: `scripts/strategy_package_governance_evidence_backfill_plan.py:396` and `:403`
- **Branch@SHA**: `codex/qe-governance-integration-20260509@924d717`
- **Layer**: CLI / operator interface
- **Reproduction**:
  1. Run `python scripts/strategy_package_governance_evidence_backfill_plan.py --evidence-bundle malformed.json` → exit 2 via `GovernanceEvidenceBackfillPlanError`.
  2. Run with valid bundle but where one package has `package_status=DRAFT` → exit 2 via `return 0 if report["status"] == "passed" else 2`.
- **Expected**: Distinguish so CI / orchestrators can tell "operator gave us garbage" (retry-not-meaningful) from "evidence not yet sufficient" (retry-after-evidence-arrives).
- **Actual**: Both produce exit 2.
- **Recommended fix direction**: Reserve exit 2 for "blocked" (valid bundle, blocking gates); use exit 3 (or another non-zero) for `GovernanceEvidenceBackfillPlanError`. Same pattern for `apply_plan.main()` (`apply_plan.py:139-145`) which conflates `GovernanceProductionApplyPlanError` and `GovernanceMigrationSmokeError`.
- **Suggested owner**: Codex
- **Severity rationale**: Operator UX issue, not a safety issue. Doesn't block R6 — operator can read JSON `status`/`error` fields manually.

### BUG-PREP-002 [MED] "seed_stability_evidence" / "regime_stability_evidence" gates pass at threshold ≥ 2 — name implies stronger property

- **File**: `scripts/strategy_package_governance_evidence_backfill_plan.py:217-218`
- **Branch@SHA**: `codex/qe-governance-integration-20260509@924d717`
- **Layer**: Governance gate semantics
- **Reproduction**:
  1. Build a bundle with exactly 2 retrain runs (seeds 101, 202) and exactly 2 regime samples.
  2. Plan passes `seed_stability_evidence=True` and `regime_stability_evidence=True`.
- **Expected**: A "stability" gate implies a stability measurement (variance / fragility) over a sample, not just "count ≥ 2".
- **Actual**: The gate is a count-only check. The actual fragility computation is delegated to `seed_fragility_score`, which is OPTIONAL (`backfill_plan.py:271-272`).
- **Recommended fix direction**: Either (a) rename gates to `seed_sample_count_present` / `regime_sample_count_present` to reflect what they actually measure; or (b) require `seed_fragility_score` to be present + non-fragile when `validation_type` indicates retrain. Document the policy decision either way.
- **Suggested owner**: Codex (governance gate policy)
- **Severity rationale**: Semantic clarity bug — won't cause incorrect data in DB, but could mislead a reviewer into thinking the gate enforces variance bounds that it doesn't.

### BUG-PREP-003 [LOW] `protected_asset` defaults to True when bundle omits the field, silently masking unprotected assets

- **File**: `scripts/strategy_package_governance_evidence_backfill_plan.py:123`
- **Branch@SHA**: `codex/qe-governance-integration-20260509@924d717`
- **Layer**: Input validation
- **Reproduction**:
  1. Submit an evidence bundle where one `asset` object omits the `protected_asset` key.
  2. Planner emits `protected_asset=True` for the row (`backfill_plan.py:141`), and `asset_gate` stays True if all other assets also default-true.
- **Expected**: Missing required governance flag should hard-fail (consistent with the `_text()` helper at `:69-72` which rejects missing/empty text fields).
- **Actual**: `bool(asset.get("protected_asset", True))` silently promotes missing → True.
- **Recommended fix direction**: Treat absent `protected_asset` as a `GovernanceEvidenceBackfillPlanError` (or at minimum a blocker). The flag is governance-critical; the failsafe should be "deny" not "allow".
- **Suggested owner**: Codex
- **Severity rationale**: Low because (a) bundle is operator-curated, not user-uploaded, and (b) the downstream apply executor "must still re-check package_status and manifest_sha256 in the target DB" per `safety_notes:367` — but the gate IS the boundary that catches this in the planner phase, so closing it is appropriate.

### BUG-PREP-004 [LOW] Apply-plan script lacks dedicated test module; coverage thin on prepared-mode positive path, --output write, and re-run idempotency

- **File**: `backend/tests/strategy_package/test_governance_evidence_backfill_plan.py` (the only test file; covers both scripts)
- **Branch@SHA**: `codex/qe-governance-integration-20260509@924d717`
- **Layer**: Tests
- **Reproduction**: `git ls-files backend/tests/ | grep apply_plan` → 0 matches. Only `test_governance_evidence_backfill_plan.py` exists and contains 2 apply-plan tests at `:168-190`.
- **Expected**: Either a dedicated `test_governance_production_apply_plan.py` (preferred — matches naming convention of `test_governance_migration_smoke.py`) or expanded coverage in the shared file for: (a) prepared-mode positive path (token + env both set → status=passed, mode=production_plan_prepared, ddl_executed=false, prepared_for_production=true); (b) `--output` writes JSON to disk and content round-trips through `json.loads`; (c) re-invocation idempotency (same `migration_apply_order`, same tables); (d) `run_static_smoke` failure path bubbles up cleanly via `apply_plan.main` returning 2.
- **Actual**: Only the negative gate-rejection path and the default static-preview happy path are tested for apply_plan.
- **Recommended fix direction**: Add 3-4 tests covering the gaps above. Prefer a dedicated test module file for clean discovery and to match the planner-file naming convention.
- **Suggested owner**: Codex
- **Severity rationale**: Low because the apply-plan script is small (149 LOC) and the untested branches are simple (token-and-env both present → call `build_plan(prepared_for_production=True)`). No safety risk from missing tests on a planner; this is a sustainability finding for the upcoming R6 apply work.

## §7 Boundary confirmations

- codex_code_modified=false (zero edits in `F:\Dev\AIstock_worktrees\qe-governance-integration-20260509\`)
- dev_db_writes=false (no DB connection opened at all)
- prod_db_touched=false (no connection to 127.0.0.1:5432/aistock)
- prod_8001_touched=false (no service start, no curl, no http client call)
- only worktree doc + commit
- task A (Stage 6 baseline rerun) skipped per user (R4 not merged to main)
