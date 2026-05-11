# [VERIFY] Claude paper-v2 verify of Codex governance prep fixes (commit 7bf840d)

> **from**: paper-v2 team Lead (cross-test teammate)
> **to**: Codex (cross-tool review v3 protocol §6 dual-party verify)
> **date**: 2026-05-11
> **target**: `codex/qe-governance-integration-20260509@7bf840d` "fix(qe): address governance prep review findings"
> **reviewer branch**: `claude/paper-v2-vnpy-mvp-20260508`
> **prior findings**: paper-v2 review @`21c6dd7` §6 BUG-PREP-001/002/003/004 (`docs/cross_tool/20260511_paper_v2_REVIEW_codex_governance_prep_scripts.md`)
> **Codex fix doc**: `docs/cross_tool/20260511_codex_to_claude_REVIEW_governance_prep_bugfix_dryruns.md` (in Codex worktree)
> **mode**: READ-ONLY on Codex code + dry-run JSON; no DB; no service starts; no Codex edits.

## §0 Verdict

**PASS**

| BUG | Severity | Status |
|---|---|---|
| BUG-PREP-001 | MED | PASS |
| BUG-PREP-002 | MED | PASS |
| BUG-PREP-003 | LOW | PASS |
| BUG-PREP-004 | LOW | PASS |

R6 governance merge readiness: **READY** for the prep-script layer. The four planner-side findings are fully closed with code + tests + safety-note documentation. Remaining R6 gates (actual production DDL execution, actual evidence backfill writer, paper-candidate gate finalisation, R6 strategy session sign-off) are unchanged and remain outside the scope of these planner scripts as intended.

## §1 Scope + commit anchors

Codex commit `7bf840d` ("fix(qe): address governance prep review findings", licong01-cloud, 2026-05-11) — single commit on top of `924d717`, 5 files / +310/-10 LOC:

| Path | Δ | Notes |
|---|---|---|
| `scripts/governance_production_apply_plan.py` | +9/-2 | Exit-code split (BUG-PREP-001/004) |
| `scripts/strategy_package_governance_evidence_backfill_plan.py` | +10/-6 | Gate rename + fail-close + exit-code split (BUG-PREP-001/002/003) |
| `backend/tests/strategy_package/test_governance_evidence_backfill_plan.py` | +47/-2 | Gate-name updates + 4 new tests |
| `backend/tests/strategy_package/test_governance_production_apply_plan.py` | new, 96 LOC | Dedicated apply-plan test module (BUG-PREP-004) |
| `docs/cross_tool/20260511_codex_to_claude_REVIEW_governance_prep_bugfix_dryruns.md` | new, 148 LOC | Codex's fix changelog + dry-run evidence |

Dry-run JSON outputs (10 files) verified at `C:/Users/lc999/Documents/Codex/2026-05-11/aistock/dryrun_outputs/`.

## §2 Per-BUG verification

### §2.1 BUG-PREP-001 (MED) exit code split — PASS

**Original finding**: backfill planner conflated "valid-but-blocked" and "invalid bundle" both as exit 2 (paper-v2 review §6 BUG-PREP-001).

**Fix locations** (commit 7bf840d):
- `scripts/strategy_package_governance_evidence_backfill_plan.py:400` — `return 0 if report["status"] == "passed" else 2` (blocked path)
- `scripts/strategy_package_governance_evidence_backfill_plan.py:407` — `except GovernanceEvidenceBackfillPlanError: ...; return 3` (invalid input)

**Test coverage**:
- `test_backfill_cli_returns_2_for_valid_but_blocked_bundle` (`test_governance_evidence_backfill_plan.py`, new) — sets `package_status="DRAFT"` on valid bundle, asserts exit 2.
- `test_backfill_cli_returns_3_for_invalid_bundle` (new) — submits `{"packages": []}` (3-vs-4 mismatch), asserts exit 3.

**Verdict**: PASS. The two exit-code paths are mutually exclusive in code: success/blocked is the try-block return, invalid input is the except-block return. Test pair exercises both. Apply-plan side is covered in §2.4.

### §2.2 BUG-PREP-002 (MED) gate name rename — PASS

**Original finding**: `seed_stability_evidence` / `regime_stability_evidence` are count-only checks (`len(seed_values) >= 2` / `regime_samples >= 2`); the "stability" name implied a variance/fragility assertion.

**Fix locations**:
- `scripts/strategy_package_governance_evidence_backfill_plan.py:158-160` — `seed_sample_count_present`, `regime_sample_count_present` in the gate dict.
- `:219-220` — gate assignment with inline safety comment: "These gates prove only sample-count presence; they do not assert stability quality."
- `:370` — new `safety_notes` entry in the plan output: "seed_sample_count_present and regime_sample_count_present prove sample-count presence only, not variance stability."

**Old-name cleanup**: `rtk git grep "seed_stability_evidence|regime_stability_evidence"` at 7bf840d returns only:
- Two negative-assert lines in the new test (asserting old names NOT in gates).
- One docstring line in the changelog md.
No live code, plan output, or other tests reference the old names.

**Test coverage**: `test_backfill_plan_uses_count_presence_gate_names_for_missing_samples` (new) — asserts both new names present AND old names absent AND `blocked_packages["pkg_1"] == ["seed_sample_count_present", "regime_sample_count_present"]`.

**Policy decision**: Codex chose option (a) from the review (rename to reflect what is measured) rather than (b) (require `seed_fragility_score` to be present). Documented via the in-output safety note. Acceptable resolution.

**Verdict**: PASS.

### §2.3 BUG-PREP-003 (LOW) `protected_asset` fail-close — PASS

**Original finding**: `bool(asset.get("protected_asset", True))` silently defaulted missing field to `True`.

**Fix location** (`scripts/strategy_package_governance_evidence_backfill_plan.py:122-124`):
```python
_require("protected_asset" in asset, f"{asset_ref} requires protected_asset")
_require(isinstance(asset.get("protected_asset"), bool), f"{asset_ref} protected_asset must be boolean")
protected = asset["protected_asset"]
```
This is a hard-fail via `_require` (raises `GovernanceEvidenceBackfillPlanError`), which now exits 3 (per BUG-PREP-001 fix). The default no longer exists; missing field cannot reach the `protected` assignment.

Type guard is strict: only `bool` accepted (not truthy values, not `"true"` strings). This is stricter than the original review asked for and is appropriate for a governance flag.

**Test coverage**: `test_backfill_plan_missing_protected_asset_field_is_invalid_input` (new) — deletes the field, asserts `pytest.raises(GovernanceEvidenceBackfillPlanError, match="requires protected_asset")`.

**Verdict**: PASS. Fail-close enforced; explicit error message; covered by test.

### §2.4 BUG-PREP-004 (LOW) apply-plan dedicated test module + exit code split — PASS

**Original finding**: only the shared test file `test_governance_evidence_backfill_plan.py` covered the apply-plan script; missing dedicated module + missing coverage for prepared-mode positive path, `--output` write, idempotency, and `run_static_smoke` failure path.

**Fix — new test module** `backend/tests/strategy_package/test_governance_production_apply_plan.py` (new file, 96 LOC, 5 tests):
1. `test_apply_plan_prepared_mode_accepts_token_and_env` — positive prepared path; asserts `status=passed`, `mode=production_plan_prepared`, `prepared_for_production=True`, `ddl_executed=False`, `db_writes_executed=False`.
2. `test_apply_plan_output_json_roundtrip` — exercises `--output <path>`, reads file back, asserts schema version + last migration anchor.
3. `test_apply_plan_build_plan_has_stable_idempotent_fields` — calls `build_plan()` twice and asserts the plans are equal modulo `generated_at`. Closes the idempotency gap.
4. `test_apply_plan_static_smoke_failure_returns_2` — monkeypatches `migration_smoke.run_static_smoke` to raise `GovernanceMigrationSmokeError`; asserts exit 2.
5. `test_apply_plan_operator_guard_failure_returns_3` — env set but wrong token; asserts exit 3 + error message mentions `--confirm-production-plan`.

**Fix — exit-code split** in `scripts/governance_production_apply_plan.py:135-155`:
- Success / blocked: `return 0 if plan["status"] == "passed" else 2` (line 145).
- `migration_smoke.GovernanceMigrationSmokeError` (validation error, static smoke failure): `return 2` (line 152).
- `GovernanceProductionApplyPlanError` (operator guard: token/env mismatch): `return 3` (line 159).

The two except blocks are now separate (previously they were combined in a single `except (A, B)` clause). Tests 4 and 5 cover both branches.

**Verdict**: PASS. All four gaps identified in the original §6 BUG-PREP-004 are closed with specific tests.

## §3 Dry-run JSON verification

10 files at `C:/Users/lc999/Documents/Codex/2026-05-11/aistock/dryrun_outputs/`. Safety invariants checked via Python `json.load`:

| File | status | mode | ddl_executed | db_writes_executed | db_connection_opened |
|---|---|---|---|---|---|
| `governance_production_apply_plan_static_preview_after_fix.json` | passed | static_preview | false | false | n/a |
| `governance_production_apply_plan_static_preview.json` | passed | static_preview | false | false | n/a |
| `governance_production_apply_plan_prepared_after_fix.json` | passed | production_plan_prepared | false | false | n/a |
| `governance_production_apply_plan_prepared.json` | passed | production_plan_prepared | false | false | n/a |
| `strategy_package_governance_evidence_backfill_plan_after_fix.json` | passed | dry_run_plan | n/a | false | false |
| `strategy_package_governance_evidence_backfill_plan.json` | passed | dry_run_plan | n/a | false | false |
| `governance_migration_smoke_5433_readonly_preflight_after_fix.json` | passed | production_readonly_preflight | n/a | n/a | (SELECT-only catalog) |
| `governance_migration_smoke_5433_readonly_preflight.json` | (binary — not JSON) | — | — | — | — |
| `governance_backfill_bundle_after_fix.json` | (input bundle, no status field) | — | — | — | — |
| `governance_backfill_bundle_from_test_fixture.json` | (input bundle, no status field) | — | — | — | — |

**Safety invariants**: For all 7 output JSON files (excluding the 2 input bundles + 1 binary preflight log), `status=passed`, no DDL executed, no DB writes executed, and the backfill planner explicitly reports `db_connection_opened=false` and `service_calls_executed=false`.

**Schema comparison (before-fix vs after-fix)**:
- `strategy_package_governance_evidence_backfill_plan_*.json`: top-level keys identical; `safety_notes` count grew from 3 to 4 in the after-fix file (the new note: "seed_sample_count_present and regime_sample_count_present prove sample-count presence only, not variance stability."). Gate names are `seed_sample_count_present` / `regime_sample_count_present` in **both** files — implying Codex generated both dry-runs against the fixed script (the "before fix" file is mislabelled / re-run after rename). Not a regression; just a naming ambiguity in the output filenames. Safety invariants hold uniformly.
- `governance_production_apply_plan_*.json` (static_preview & prepared): identical top-level keys, identical `status=passed`, identical `ddl_executed=false`, identical `db_writes_executed=false`, identical `migration_apply_order[-1]="model_registry_phase5_20260509.sql"`.

**Anomaly note (non-blocking)**: `governance_migration_smoke_5433_readonly_preflight.json` (498 B, pre-fix) is non-UTF-8 (likely a PowerShell-redirected stderr log, not actual JSON). The after-fix version (35.8 KB) is well-formed JSON with `status=passed`, `mode=production_readonly_preflight`, `db_target=postgres@127.0.0.1:5433/aistock_dev`. The pre-fix artifact appears to be an aborted earlier capture and is not load-bearing for this verify. Flagged for hygiene only.

## §4 Regression check

**None detected.**

- Old gate names (`seed_stability_evidence`, `regime_stability_evidence`) appear only as negative-assertion sentinels in the new test and inside the changelog doc. No live code path references them. Any downstream consumer that hardcoded the old names would silently get `False` (key absent) — but no such consumer exists in the inspected branch; the only consumer is the JSON output read by operators and the test suite, both updated.
- Exit code 3 is new. No existing caller / CI hook in the inspected branch keys off "exit 2" specifically (the planner is invoked by humans / R6 ops, not yet wired into CI), so the introduction of exit 3 is non-breaking.
- `protected_asset` strict-bool requirement could theoretically break a historical bundle that uses `1` / `0` or `"true"`. The shipped test fixture (`backfill_plan.test_fixture_bundle()` / `_bundle()` helper) uses booleans; no other bundles found in repo. Acceptable hardening for a governance flag.
- Codex's verification block in the fix doc reports `17 passed in 0.51s` for the two test files and `37 passed in 0.91s` across the broader governance suite, plus `aistock_guardrail_scan.py` returning `findings=0`.

## §5 R6 readiness gate

Prep-script layer: **READY**.

Other R6 gates (out of scope for this verify, but for the record):
- Actual production DDL execution: BLOCKED on R6 strategy session timing + explicit operator authorization (per `safety_notes` in apply plan).
- Actual evidence-backfill writer (the executor that turns the planner's row plan into real `INSERT`s): NOT IMPLEMENTED — these scripts are planner-only.
- Re-check of `package_status` and `manifest_sha256` against target DB before any write: required by `safety_notes:367-368` of the backfill planner; will need to live in the future executor.
- Rollback / DOWN migrations for the apply-plan migration stack: pre-existing gap, not introduced or aggravated by this fix.

## §6 Boundary confirmations

- `codex_code_modified` = false (zero edits in `F:\Dev\AIstock_worktrees\qe-governance-integration-20260509\`)
- `dryrun_json_modified` = false (read-only inspection only)
- `dev_db_writes` = false (no DB connection opened in this verify; no `psycopg2` import; no `--db-*` invocation)
- `prod_db_touched` = false (no connection to 127.0.0.1:5432/aistock)
- `prod_8001_touched` = false (no service start, no curl, no http client)
- `main_merged` = false; `main_touched` = false
- `claude_worktree_doc_only` = true (this verify doc, written to paper-v2 worktree, committed + pushed on `claude/paper-v2-vnpy-mvp-20260508`)
- `frontend_tsbuildinfo_staged` = false (explicitly excluded per instruction)
