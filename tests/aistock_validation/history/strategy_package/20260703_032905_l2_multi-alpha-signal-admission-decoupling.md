# multi alpha signal admission decoupling

- Module: strategy_package
- Level: L2 / T3 / F2
- Date: 2026-07-03T03:29:05+08:00
- Updated: 2026-07-03T04:10:00+08:00
- Git base: f21b8cf7 chore(issue): close-sync BUG-582 after merge (#1845)
- Branch: feature/multi-alpha-signal-admission-impl-20260703
- Worktree: F:\Dev\AIstock_worktrees\multi-alpha-signal-admission-impl-20260703
- Operator: lc999 / Codex

## Scope

- Changed files: `backend/services/strategy_package/asset_eligibility.py`, `backend/services/strategy_package/multi_alpha_promotion.py`, `backend/services/strategy_package/repository.py`, `backend/services/strategy_package/multi_alpha_paper_admission.py`, `backend/services/strategy_package/multi_alpha_paper_dry_run.py`, `backend/routers/strategy_packages.py`, `frontend/src/app/paper-v2/packages/page.tsx`, targeted tests, and this evidence file.
- Impacted flows: StrategyPackage multi-alpha asset eligibility, multi-alpha promotion, package summary/selectable summary, optional paper-runtime-dry-run diagnostics, Paper v2 LocalSim/MiniQMT SIM create_portfolio gate interactions.
- Business goal: decouple multi-alpha signal admission from order preview generation; admission is frozen self-check provenance plus non-empty deterministic selection evidence, while order deltas stay in execution layer.
- Out of scope: MiniQMT execution-layer gates, single-alpha behavior, production DB backfill, DDL/DML, service restart, existing frozen manifest mutation.
- Protected assets reviewed: no existing frozen manifest/model/selection artifact mutated; new `signal_admission` evidence is only written for newly promoted parent packages.

## Environment

- Backend port: not started; production 8001 not touched.
- Frontend port: not started; production 3000 not touched.
- TDX port: not started; production 19080 not touched.
- Conda/env: existing local Python via `rtk python`.
- Database: no production DB reads or writes; tests use in-memory repositories.
- Browser/headless: not used.

## Design Acceptance Summary

| Item | Result | Evidence |
|---|---|---|
| F-001 signal-layer eligibility | PASS | `asset_eligibility.py::_multi_alpha_signal_admission_checks`; 95 targeted tests |
| F-002 remove dry-run admission dependency | PASS | raising `admission_reader`; no `get_eligible` call |
| F-003 local_sim/minqmt_sim same signal criterion | PASS | local and minqmt summaries eligible in targeted tests |
| F-004 promotion not born-blocked | PASS | `paper_admission.blocking=[]`, manifest has `signal_admission`, no legacy `paper_admission` |
| F-005 order engine out of eligibility | PASS | monkeypatch guards for `TargetPositionEngine`, `RebalanceEngine`, dry-run validator |
| F-006 dry-run optional diagnostic | PASS | endpoint response marks `diagnostic_only=true`, `required_for_signal_admission=false` |
| F-007 build self-check hard gate preserved | PASS | self-check failure test leaves no half package |
| F-008 fail-closed reason codes | PASS | self-check fail, empty, nondeterministic, unknown blocker tests |
| F-009 single-alpha zero regression | PASS | single-alpha create test in targeted suite |
| F-010 pkg_ma_8ec5e389-equivalent parent | PASS | equivalent fixture eligible for local_sim and minqmt_sim without admission row |
| F-011 #1792/#1799/#1819/#1814 compatibility | PASS | manifest hash no-drift and existing affected tests pass |
| F-012 no production gates triggered | PASS | no migrations/dependencies; no services/DB touched |
| F-013 cheap persisted signal evidence | PASS | persisted manifest evidence read; no workspace/model/WSL/self-check/order hot-path calls |

## Killer Test Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Complete multi-alpha parent | local_sim and minqmt_sim eligible without dry-run admission | `test_asset_eligibility_uses_persisted_signal_evidence_for_local_and_minqmt_without_dry_run` | PASS |
| Hot path no expensive work | no full self-check/workspace/model/WSL/order engine calls | `test_signal_eligibility_hot_path_reads_persisted_evidence_without_artifact_or_order_engines` | PASS |
| Self-check fail | fail-closed specific reason, no half package | `test_parent_self_check_failure_fails_without_half_package`; persisted evidence failure tests | PASS |
| Selection empty / invalid / nondeterministic | fail-closed specific reason | empty leg_count, invalid metadata, invalid sha256 digest tests | PASS |
| Unknown blocker | hard fail for local_sim and minqmt_sim | `test_unknown_multi_alpha_paper_admission_blocker_still_blocks_localsim` | PASS |
| MiniQMT execution gate | missing account/group/slot/policy still blocks | targeted strategy + paper_trading_v2 MiniQMT tests | PASS |
| Promotion born eligible | new parent not born-blocked | `test_promote_target_two_leg_run_freezes_deterministic_multi_alpha_package` | PASS |
| Manifest hash no drift | 15-package scan clean | `test_validate_manifest_integrity_clean_for_15_packages_with_multi_alpha_signal_evidence` | PASS |

## Commands

```powershell
rtk python F:\Dev\AIstock\scripts\aistock_issue_workflow.py doctor
# workflow_gate=ready; restart_recommended=false; codegraph_status=ok

rtk python -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py backend/tests/strategy_package/test_repository_service.py -q -p no:cacheprovider
# 95 passed in 4.32s

rtk python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_auto_run.py -q -p no:cacheprovider
# 69 passed in 3.78s

rtk python -m compileall backend/services/strategy_package backend/services/paper_trading_v2 backend/routers/strategy_packages.py
# PASS

rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/multi_alpha_signal_admission_decoupling_f2_design_20260702.md --tier F2
# Feature workflow validation: PASS; tier=F2 design_items=13 matrix_rows=13 warnings=0

rtk powershell -NoProfile -Command "git diff --check"
# PASS; only Git LF->CRLF warnings

rtk python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
# Guardrail scan completed: mode=changed_only, files=12, findings=0, blocking=0

rtk npm run lint -- --file src/app/paper-v2/packages/page.tsx
# BLOCKED: 'next' is not recognized because this worktree has no frontend/node_modules

rtk node F:/Dev/AIstock/frontend/node_modules/next/dist/bin/next lint --file src/app/paper-v2/packages/page.tsx
# BLOCKED: ERR_MODULE_NOT_FOUND: Cannot find package 'next' imported from this worktree's frontend/next.config.mjs
```

## Evidence

- API calls: not run; no service started.
- DB checks: no production DB touched; tests use in-memory repositories.
- Logs: command stdout captured in Codex session.
- Playwright/screenshots: not run; UI change is copy-only and lint was blocked by missing worktree dependencies.
- Business output summary: multi-alpha parent signal eligibility no longer depends on order dry-run; dry-run endpoint remains optional diagnostics; MiniQMT execution-layer guards remain fail-closed.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Frontend lint blocked | isolated worktree lacks `frontend/node_modules`; root `next` binary cannot resolve worktree-local `next.config.mjs` package imports | no dependency install performed under no-production-dependency-change scope | documented as residual validation gap |
| Selection artifact digest accepted 64 non-hex chars during review | eligibility only checked length | changed to `_is_sha256()` and added invalid digest fail-closed test | 95 targeted tests pass |
| Default eligibility service instantiated selection artifact repository even when manifest evidence exists | default reader was created eagerly | changed to lazy default sentinel so persisted manifest evidence path does not construct repository | hot-path tests and compile pass |

## Result

- Final status: local implementation gate passed; ready for Tier2 implementation review, not committed/pushed/PR-created in this session.
- Remaining risks: frontend changed-file lint is blocked by missing worktree dependencies; branch is behind `origin/main` by workflow-only commit `fe1f1e1d` and should be refreshed before PR if required.
- Need production backend restart: no; `service_restart=not_performed`.
- Need dev service restart: no.
- `production_ddl_gate=noop`.
- `production_dml_gate=noop`.
- `production_frontend_dependency_gate=noop`.
- `production_backend_dependency_gate=noop`.
- `protected_asset_mutation=none`.
