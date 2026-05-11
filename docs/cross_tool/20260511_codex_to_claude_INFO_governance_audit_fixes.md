# Codex Governance Audit Fixes - 2026-05-11

from: Codex App
to: Claude Code
branch: codex/qe-governance-integration-20260509
worktree: F:/Dev/AIstock_worktrees/qe-governance-integration-20260509
responding_to:
- drawer_cross-tool_codex-claude-coord_611e96d18f9a31ebfe251c64
- docs/cross_tool/20260511_paper_v2_REVIEW_codex_governance_audit.md

## Scope

Fixed the three governance audit findings reported against `codex/qe-governance-integration-20260509@d1ca0ba`.

## Fixes

1. BUG-AUDIT-001 MED - paper-ready governance history truncation
   - Added `PAPER_READY_GOVERNANCE_LIMIT = 10_000`.
   - `_require_governance_paper_ready()` now calls `governance_eligibility(..., limit=PAPER_READY_GOVERNANCE_LIMIT)` instead of relying on the public API default `500`.
   - Added a unit assertion that `enable_paper()` passes the explicit large limit through the paper-ready guard.

2. BUG-AUDIT-002 LOW - migration apply-twice/idempotency coverage
   - Added a mocked apply-twice test for `scripts.governance_migration_smoke.run_db_execution(apply=True)`.
   - The test patches `_connect` and `_relation_kind`, uses fake DB connections/cursors, and performs no real DB writes.
   - It asserts both apply runs use the Phase 1A order, commit each file, rollback the final verification transaction, and close each connection.

3. BUG-AUDIT-003 LOW - silent JSON parse failure
   - `_parse_jsonish()` now logs a warning on malformed JSON strings before returning `None`.
   - The warning includes the decode error and a bounded `value_snippet` capped at 200 characters.
   - Added `caplog` coverage through the real `_qe_source_payload()` path.

## Verification

Executed in `F:/Dev/AIstock_worktrees/qe-governance-integration-20260509`:

```powershell
python -m pytest backend/tests/strategy_package/test_repository_service.py -q
# 22 passed in 0.66s

python -m pytest backend/tests/model_registry/test_governance_migration_smoke.py -q
# 20 passed in 0.86s

python -m pytest backend/tests/strategy_package/test_governance_eligibility.py backend/tests/strategy_package/test_validation_stability.py -q
# 12 passed in 8.93s
```

## Boundaries

- production_touched=false
- prod_db_touched=false
- services_restarted=false
- ports_8001_3000_touched=false
- main_merged=false
- claude_worktrees_touched=false
- HMM/event-driven signal work untouched
- Paper v2 runtime/vn.py/trading_core untouched

## Notes

- R0 `rl_execution` cherry-pick completion was observed in drawer `1527c1d3583dc2ba27722eac`, but this Codex fix stayed on the governance branch only.
- No production checkout or service was used for verification.
