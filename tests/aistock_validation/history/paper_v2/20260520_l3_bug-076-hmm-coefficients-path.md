# BUG-076 Paper v2 HMM coefficients_path L3 Validation

- Date: 2026-05-20
- Branch: `bug/BUG-076-paper-v2-hmm-coefficients-path`
- Worktree: `F:\Dev\AIstock_worktrees\bug-076-paper-v2-hmm-coefficients-path`
- GitHub issue: #103
- Module: `paper_v2`
- Level: L3 targeted regression plus L0/changed-file guardrails
- Production impact: no production backend `8001` restart; no frontend `3000` restart; no production DB migration; no HMM snapshot/model/StrategyPackage manifest/protected artifact writes.

## Scope

BUG-076 requires Paper v2 Selection, portfolio creation, run-console day/replay/live/switch actions, and runtime-profile persistence to use the same explicit HMM coefficient artifact contract. When HMM is enabled, UI actions must carry `runtime_profile.hmm.coefficients_path` for a covering artifact and block before backend submission when coverage is missing. HMM remains platform runtime state and is not written into StrategyPackage manifest/state.

## Implementation Summary

- Added shared frontend helper `frontend/src/lib/paper-v2/hmm-runtime.ts` for HMM coefficient artifact coverage selection.
- Reused that helper from `frontend/src/app/paper-v2/selection/page.tsx` so Selection keeps its explicit `coefficients_path` behavior through the common contract.
- Updated `frontend/src/app/paper-v2/portfolios/page.tsx` so portfolio runtime profile creation and initial session runtime config include `runtime_profile.hmm.coefficients_path`, and creation is blocked when no selected artifact covers the session date/range.
- Updated `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx` so readiness, run-day, replay, live session creation, mode switch, and runtime profile/version save all build date-aware runtime configs with an explicit covering `coefficients_path`.
- Added mocked Playwright regression coverage in `frontend/tests/paper-v2/paper-v2-hmm-runtime-coefficients.spec.ts`.

## Commands And Results

| Command | Result |
|---|---|
| `cd frontend; npx tsc --noEmit` | PASS |
| `cd frontend; npx playwright test tests/paper-v2/paper-v2-hmm-runtime-coefficients.spec.ts --config=playwright.paper-v2.config.ts` | PASS: `3 passed` |
| `python -m nox -s paper_v2_backend` | PASS: `443 passed, 1 skipped, 2 xfailed` |
| `python -m nox -s l0` | PASS; baseline/P2 guardrail findings only, no blocking P0/P1 delta |
| `cd frontend; npm run build` | PASS; existing unrelated hook warnings remain outside changed BUG-076 files |
| `git diff --check` | PASS |
| `python -m nox -s guardrail_changed_files` | PASS; staged files mapped to modules; no blocking P0/P1 findings |

## Additional UI Attempt Notes

- Attempted full `python -m nox -s paper_v2_ui -- 8012 3012` on an isolated dev backend. The first attempt failed because the worktree had no `.env`, causing DB auth `fe_sendauth: no password supplied`.
- Retried a dev backend after read-only loading `F:\Dev\AIstock\.env` and disabling schedulers plus `MINIQMT_ENABLED=false`; the suite progressed through BUG-076 mocked tests and real-backend StrategyPackage checks, then failed in the existing real-flow Selection Center test because the selected package was blocked by Selection Center health (`STRATEGY_PACKAGE_VALIDATION_ERROR: strategy package is blocked by Selection Center health preflight`). This is data/package-state dependent and not a BUG-076 code regression.
- During the isolated dev-backend retry, no production `8001`/`3000` service was restarted. The dev backend touched the local application DB through normal UI validation requests; no schema migration, HMM snapshot/model artifact, or StrategyPackage manifest mutation was performed by this BUG-076 fix.

## Guardrail Notes

- `guardrail_changed_files` reported only P2 `UI-RAWJSON-001` findings on pre-existing `JsonPanel` usage in Paper v2 pages and shifted/new lines in touched UI blocks. The gate is configured to block new P1+ findings; no P0/P1 blocking issue was found.
- `l0` reported existing/baseline findings such as `TRADING-FALLBACK-001` and unrelated validation UI raw JSON warnings; no BUG-076 blocking delta.

## DESIGN-COMPLIANCE-001 Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| Selection keeps explicit HMM coefficient artifact contract | `frontend/src/app/paper-v2/selection/page.tsx`; `frontend/src/lib/paper-v2/hmm-runtime.ts` | `npx tsc --noEmit`; mocked Playwright suite passed | PASS | Selection behavior was refactored to the shared helper only; no StrategyPackage state write added. |
| Portfolio creation/runtime profile carries `runtime_profile.hmm.coefficients_path` when HMM is enabled and coverage exists | `frontend/src/app/paper-v2/portfolios/page.tsx` | Mocked Playwright `Portfolio creation persists and starts sessions with explicit HMM coefficients_path`; `npm run build` | PASS | None. |
| Portfolio creation blocks before backend submission when HMM coverage is missing | `frontend/src/app/paper-v2/portfolios/page.tsx`; shared helper | Mocked Playwright verifies coverage UI and payload path for covered case; TypeScript/build validate code path | PASS | The explicit missing-coverage block is implemented in UI; covered by run-console negative test for same shared helper semantics. |
| Run console readiness and run-day use date-aware explicit `coefficients_path` | `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx` | Mocked Playwright `Run console sends explicit HMM coefficients_path for day...` | PASS | None. |
| Run console replay validates coefficient coverage across the full requested replay range | `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx`; `artifactCoversDateRange` | Mocked Playwright negative replay range test blocks `2026-05-18~2026-05-21`; positive test covers `2026-05-18~2026-05-20` | PASS | Coverage uses `covered_trade_dates` when present and falls back to artifact bounds only when known dates are absent. |
| Run console live session and switch session use explicit `coefficients_path` | `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx` | Mocked Playwright verifies create live and switch-mode payload captures | PASS | None. |
| Runtime profile save/version uses explicit `coefficients_path` | `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx` | Mocked Playwright verifies runtime-profile payload captures | PASS | None. |
| HMM remains platform runtime config and must not be bound into StrategyPackage manifest/state | Only frontend Paper v2 runtime pages and tests changed; no backend `strategy_package` service/manifest code changed | `git diff --cached --name-only`; no StrategyPackage manifest files edited; `paper_v2_backend` passed | PASS | None. |
| No simplified/subset/POC/mock-only completion claim | All issue-required UI paths implemented; targeted mocked Playwright plus backend regression, TypeScript, production build, L0, and changed-file guardrails run | This validation record plus command results above | PASS | Full real-backend UI suite remains partly data-state blocked outside BUG-076; not claimed as BUG-076 completion evidence. |

## Residual Risks

- The full real-backend Paper v2 UI suite is not a clean BUG-076 proof in the current environment because it depends on live local DB/package health state. The targeted mocked regression covers BUG-076 payload semantics deterministically.
- Existing Paper v2 `JsonPanel` raw JSON UI debt remains P2 and should be handled by a separate UI simplification issue, not mixed into BUG-076.
- Production runtime activation still requires merge, sync, and user-managed backend/frontend restart. This branch does not activate itself on production `8001`.
