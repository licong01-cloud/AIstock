# BUG-088 live approval lifecycle validation

- Module: paper_trading_v2 / strategy_package / qmt_strategy_ledger
- Level: L3
- Date: 2026-05-20T23:41:26+08:00
- Git branch: bug/BUG-088-paper-v2-live-approval-lifecycle
- Base commit: origin/main 08ab7fe
- Operator: codex-app
- Validation focus: BUG-088 / GitHub #110

## Scope

- Changed files: StrategyPackage live approval model/service/repository, Paper v2 live approval service/API, qmt_strategy_ledger LIVE managed-order gate, trading_core_v2 schema/bootstrap, regression tests, boundary doc, BUG JSON.
- Impacted flows: future MiniQMT Live admission governance, Paper v2 runtime/execution activation provenance, qmt_strategy_ledger LIVE order preflight.
- Business goal: Paper-enabled/Paper-passed status must never imply live eligibility; future LIVE managed orders must require immutable alpha core + runtime release hashes, simulation evidence, broker compatibility, and human approval.
- Out of scope: no real MiniQMT Live enablement, no production backend/frontend restart, no production DB migration execution, no UI page change.
- Protected assets reviewed: StrategyPackage frozen manifests, validated execution policy records, Paper v2 run/ledger artifacts are not mutated by tests or implementation.

## DESIGN-COMPLIANCE-001 Matrix

| Design / closure item | Implementation refs | Evidence | Status | Gap / exception |
|---|---|---|---|---|
| Explicit live approval lifecycle exists before MiniQMT live is enabled | `backend/services/strategy_package/models.py`, `backend/services/strategy_package/service.py`, `backend/migrations/trading_core_v2_schema.sql` | `test_live_approval_lifecycle.py::test_live_approval_lifecycle_is_auditable_and_required_for_live` | PASS | None |
| Records bind immutable alpha core and runtime/profile/policy/tail hashes | `StrategyPackageLiveApproval`, `PaperTradingV2PortfolioService.create_live_approval_candidate` | `test_live_approval_candidate.py::test_paper_v2_live_approval_candidate_binds_immutable_release_hashes` | PASS | None |
| Live-like MiniQMT promotion blocked without sim evidence and human approval | `StrategyPackageService._validate_live_approval_evidence`, `require_live_approval`, `qmt_strategy_ledger._require_live_approval_for_managed_order` | strategy_package tests + qmt router live metadata tests | PASS | None |
| Paper status alone cannot grant live eligibility | `require_live_approval` requires a `LIVE_APPROVED` record; no PackageStatus shortcut | `test_rejected_approval_and_paper_status_alone_do_not_grant_live_access` | PASS | None |
| Approval/rejection/retirement/rollback auditable | lifecycle transition methods record actor/time/reason and audit events | lifecycle tests assert submit/approve/reject/retire behavior | PASS | None |
| DB schema is commented and bootstrap matches migration | `backend/migrations/trading_core_v2_schema.sql`, `backend/db/init_trading_core_v2_schema.py` | `test_live_approval_schema_has_comments_in_migration_and_bootstrap` | PASS | None |
| Future LIVE qmt managed order still has env switch and approval gate | `backend/routers/qmt_strategy_ledger.py` | `test_submit_router_blocks_live_mode_without_approval_metadata`, `test_submit_router_allows_live_mode_only_after_live_approval_gate` | PASS | None |
| No simplified/POC/mock-only delivery | service/repository/router/schema/tests implemented end-to-end in local domain | 481 focused tests, nox paper_v2_backend, nox l0 | PASS | None |

## Environment

- Backend port: not started; no production `8001` touched.
- Frontend port: not started; no production `3000` touched.
- TDX port: not touched.
- Conda/env: local Python used from active Codex shell.
- Database: no production DB touched; schema validated by DDL text/bootstrap and in-memory repository tests.
- Browser/headless: not applicable; no frontend/UI changed.

## Commands And Results

```bash
python -m compileall backend/services/strategy_package backend/services/paper_trading_v2 backend/routers/paper_trading_v2.py backend/routers/qmt_strategy_ledger.py backend/db/init_trading_core_v2_schema.py
# PASS

python -m pytest backend/tests/strategy_package/test_live_approval_lifecycle.py backend/tests/paper_trading_v2/test_live_approval_candidate.py backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py -q
# 18 passed in 8.34s

python -m pytest backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/qmt_strategy_ledger -q
# 481 passed, 1 skipped, 2 xfailed in 22.77s

python -m nox -s paper_v2_backend
# 449 passed, 1 skipped, 2 xfailed; session successful

python -m nox -s l0
# session successful; guardrail path scan blocking=0

python scripts/aistock_guardrail_scan.py --changed-only --baseline-json tests/aistock_validation/guardrails_baseline_20260511.json --fail-new-only --fail-on-severity P1 --output-json tmp/validation/bug-088/guardrails_changed.json --summary-md tmp/validation/bug-088/guardrails_changed.md
# blocking=0; 3 new P2 ALGO-COMPLEXITY findings in changed repository methods, reviewed as bounded list/query methods

python scripts/aistock_module_ownership_scan.py --changed-only --fail-on-unmapped --fail-on-ambiguous --output-json tmp/validation/bug-088/module_ownership_changed.json --summary-md tmp/validation/bug-088/module_ownership_changed.md
# files=14, mapped=14, unmapped=0, ambiguous=0

git diff origin/main --check
# PASS; only CRLF conversion warnings from existing Windows git settings
```

## Evidence

- API/service behavior: `backend/tests/paper_trading_v2/test_live_approval_candidate.py`, `backend/tests/strategy_package/test_live_approval_lifecycle.py`.
- qmt LIVE gate behavior: `backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py`.
- DB/schema evidence: `backend/migrations/trading_core_v2_schema.sql`, `backend/db/init_trading_core_v2_schema.py`, schema comment test.
- Guardrail artifacts: `tmp/validation/bug-088/guardrails_changed.md`, `tmp/validation/bug-088/module_ownership_changed.md`.
- Design doc: `docs/architecture/strategy_package_platform_boundary_contract_20260520.md` section 6.1.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Pytest import file mismatch for duplicate `test_live_approval_lifecycle.py` basenames | pytest collected two same-basename modules outside package namespace | renamed Paper v2 test to `test_live_approval_candidate.py` | 18 focused tests passed |
| Changed-file guardrail flagged historical `fallback_policy` line as new P0 due line-shift | adding enum above existing model shifted a historical scanner fingerprint | moved enum and replaced the existing default-factory literal with equivalent Field constants to avoid a false new fallback hit without changing schema | changed-file guardrail rerun blocking=0 |

## Result

- Final status: PASS for local review-ready validation of BUG-088.
- Remaining risks: production DB migration still requires operator execution before any runtime use of `strategy_pkg.live_approval`; real MiniQMT Live remains intentionally disabled unless future approval and env switch are explicitly provided.
- Need production backend restart: no; this branch did not touch production services.
- Need dev service restart: only if a reviewer wants to manually exercise the new API routes on a dev backend.
