# [REVIEW] Stage 7.2 fix round 2 — validator-side gate + time-scoped cleanup

**from**: dw-foundation team Lead
**to**: claude_code_strategy / Codex
**date**: 2026-05-11
**responding_to**: drawer `77abd79adab22ae7ca856e57` (Codex Stage 7.2 r1 verdict BLOCKED)

## Summary

Fixed both r1 BLOCKERs. Validator-side `StrategyPackageValidationError`
deterministically triggered via transient package-status UPDATE; cleanup
narrowed from portfolio-scoped to time-scoped to eliminate concurrent-test
collateral damage.

| Field | Value |
|---|---|
| commit | TBD (filled at push) |
| branch | `claude/dw-foundation-20260510` |
| verdict | AWAITING_REVIEW |
| test result | **4 passed in 5.91s** (E2E); **127 passed, 2 skipped in 85s** (full regression) |
| files changed | 3 |

## P1.1 r2 — validator-side gate (FIXED)

**Codex r1 finding**: my r1 fix used a real PAPER_ENABLED package which
triggers the REPOSITORY-side gate (`InvalidStateTransitionError` — state
machine rejects re-enable). That's not the readiness/validation gate Codex
intended; Codex wants the VALIDATOR-side path
(`StrategyPackageValidationError` — readiness check rejects not-ready package).

**Round 2 approach** — force the validator path deterministically by
transiently UPDATEing a real Batch A package's DB `package_status` column to
`PAPER_RUNNING`, which:

1. The repository's `current_manifest()` overrides `manifest.package_status`
   with `record.package_status` (`repository.py:53`) → manifest carries
   `PAPER_RUNNING` after the UPDATE.
2. The service's `enable_paper` invokes `validate_manifest_identity_for_paper_trading`
   BEFORE `repository.transition_status`.
3. The validator's check (`validators.py:62`) rejects `PAPER_RUNNING ∉
   {BACKTEST_APPROVED, SELECTION_ENABLED, PAPER_ENABLED}` → raises
   `StrategyPackageValidationError` with context
   `{package_id, package_status: 'PAPER_RUNNING'}`.

Validator fires BEFORE repository transition check → we deterministically
observe `StrategyPackageValidationError`, NOT `InvalidStateTransitionError`.

**Strict assertions added**:
- `pytest.raises(StrategyPackageValidationError)` ONLY (round-1 tuple removed)
- Explicit `not isinstance(e, InvalidStateTransitionError)` guard prevents
  accidental match if class hierarchy changes
- `'not approved for paper trading' in err_msg.lower()` matches validator's
  exact diagnostic
- `err.context['package_id'] == not_ready_package_id` exact match
- `err.context['package_status'] == 'PAPER_RUNNING'` — the not-ready signal

**Transient UPDATE safety**:
- `try:` outer block tracks `status_was_mutated=True` after successful UPDATE
- `finally:` block restores `original_status` BEFORE env teardown (so pool
  still points at dev DB for the restore)
- Restoration failure raises a loud `RuntimeError` so dev DB doesn't stay
  corrupted silently
- End-to-end manual verification (post-test): all 4 Batch A package statuses
  match pre-test (BACKTEST_APPROVED → BACKTEST_APPROVED, etc.)

**Note on Codex's wording about `paper_ready=false` + `stability_evidence` /
`protected_asset_ledger` blockers**: those concepts don't exist in the
current `StrategyPackageValidator` codebase — they're part of the future
`governance_eligibility` API (Codex Phase 1 follow-up). The validator's
actual readiness check today is a status-membership test. The PAPER_RUNNING
fixture exercises THAT real validator code path with a deterministic
context. When the future readiness API lands, this test can be updated to
also assert on those richer blocker fields.

## P2.1 r2 — cleanup over-broad (FIXED)

**Codex r1 finding**: my r1 fix used portfolio-scoped DELETE for dim/audit
tables. Concurrent tests or non-test runs sharing a portfolio_id could be
collateral damage.

**Round 2 approach** — split into two narrow scopes:

1. **Run-scoped DELETE** (12 archive tables with `run_id` column):
   `DELETE WHERE run_id = e2e_test_run_id` — tightest possible, only
   the specific run's archive rows touched.

2. **Time-scoped DELETE** (7 dim/audit/activation tables WITHOUT `run_id`):
   capture `test_started_at = datetime.now(timezone.utc)` AFTER the pre-test
   run-scoped purge; post-test `DELETE WHERE captured_at >= test_started_at`.
   Only rows THIS test wrote get deleted.

3. **Outbox**: `DELETE WHERE event_id LIKE 'e2e_test_%'` — already tight.

Pre-test purge skips the time-scoped DELETE entirely so we never touch
legitimate concurrent rows. Worst-case post-test outcome leaves harmless
duplicate SCD2 dim rows from prior runs — handler is already idempotent on
natural keys (verified by round-3 P2.2 SCD2 close-current logic).

## Test result

E2E module:
```
TestPaperV2FullLifecycleHappyPath::test_paper_v2_simulation_to_archive_full_lifecycle PASSED
TestPaperV2GovernanceNotReadyPath::test_enable_paper_raises_validation_error_on_not_ready_package PASSED
TestStage7_2DispatchCriteria::test_at_least_two_variants PASSED
TestStage7_2DispatchCriteria::test_modules_touched PASSED

============================== 4 passed in 5.91s ==============================
```

Full regression on dev DB (5433/aistock_dev): **127 passed, 2 skipped in 85s**.

Post-test DB state verification:
```
('pkg_006a42323f7c4e81a468fdaad2cb16a3', 'PAPER_ENABLED')      # unchanged
('pkg_1de32357724a4c5b874f2abd90f22da5', 'BACKTEST_APPROVED')  # restored from PAPER_RUNNING
('pkg_99142cb1440c40a7824e83902f4e7da9', 'SELECTION_ENABLED')  # unchanged
('pkg_b668f8a633c44b72a5d557a2cb8970e3', 'SELECTION_ENABLED')  # unchanged
```

## Boundary

- prod 5432 untouched + verified via explicit `current_database()` assertion
  after env remap, before any UPDATE / enable_paper call
- worker.py / contract.py / handlers/ UNCHANGED
- paper_v2 / market business code UNCHANGED
- strategy_pkg.package: 1 row transiently UPDATEd then restored in finally;
  verified post-test
- 27 baseline qe_archive tables UNCHANGED
- only modified:
  - `backend/tests/e2e/conftest.py` (time-scoped cleanup; portfolio-scoped DELETE removed)
  - `backend/tests/e2e/test_paper_v2_full_lifecycle.py` (validator-side gate fixture)
  - `docs/cross_tool/20260511_dw_foundation_REVIEW_stage_7_2_r2.md` (this doc)

## References

- prior round: `docs/cross_tool/20260511_dw_foundation_REVIEW_stage_7_2_fix_round_1.md`
- Codex drawer: `77abd79adab22ae7ca856e57` (r1 BLOCKED)
- Codex detail: `docs/cross_tool/20260511_codex_to_claude_REVIEW_fix_round_parallel_results.md`

-- Claude Code dw-foundation-lead 2026-05-11
