# [PROGRESS] Stage 7.1 — Frontend Pipeline Pages (2/4 delivered)

**from**: claude_code (frontend-pipeline-pages worktree)
**to**: claude_code_strategy
**date**: 2026-05-11
**branch**: `claude/frontend-pipeline-pages-20260511`
**worktree**: `F:/Dev/AIstock_worktrees/frontend-pipeline-pages-20260511`

## Verdict

Delivered **2 of 4** sub-pages in depth this session per user direction. The remaining 2 are scoped and ready for the next session or a teammate.

## Delivered

### 7.1.a qe-archive UI (complete)

The qe-archive page already shipped on origin/main (28.6 KB `page.tsx`, fully featured: candidate list, dry-run, write, worker, quality lookup). This session augmented it:

- Added `frontend/tests/qe-archive/qe-archive-flows.spec.ts` with **5 new discrete Playwright tests** covering scenarios the existing monolithic test left out:
  1. empty state (no candidates / outbox / jobs)
  2. status-filter forwards `status=` to backfill-candidates API
  3. `include_archived` toggle forwards `include_archived=true`
  4. health API 500 surfaces the ErrorPanel without crashing
  5. refresh button triggers a new health fetch (call-count spy)
- `noxfile.py qe_archive_ui` default ports updated `8011/3011 → 8012/3012` (per dispatch constraint).
- All 4 catalog yamls already register `/qe-archive` (no change required).

### 7.1.d strategy-package-governance UI (complete)

New page + tests + API client, registered in all 4 catalogs:

- `frontend/src/app/strategy-package-governance/{page.tsx,layout.tsx}`
- `frontend/src/lib/strategy-package-governance/api.ts` (typed client, `governanceApi.{listPackages, eligibility, enablePaper}`)
- 5 evidence visualizations (manifest_identity / original_fixed_weight_retest / validation_stability / protected_asset_ledger / runtime_variant_paper_candidate), each rendered as a status-toned tile with reason text
- `paper_ready` summary + block reason
- `enable_paper` two-step confirm action (reuses `ConfirmAction`, confirm token `ENABLE_PAPER_CONFIRM`); button disabled unless `paper_ready=true`
- `frontend/tests/strategy-package-governance/governance.spec.ts` with **5 Playwright tests**:
  1. list renders + first ready package auto-selected
  2. status filter narrows visible rows
  3. blocked package surfaces missing evidence + disables enable button
  4. enable_paper confirm flow posts to API (call-count spy + success message)
  5. packages API 500 surfaces ErrorPanel without crashing
- Sidebar entry added (`/strategy-package-governance` under Paper Trading v2 group) to satisfy `test_default_ui_target_catalog_matches_frontend_nav_groups`.
- `nox_strategy_package_governance_ui` added to `ALLOWED_COMMAND_KEYS` allowlist in `backend/services/validation/plan_catalog.py`.

**Backend caveat**: `GET /strategy-packages/{id}/governance-eligibility` does NOT exist on origin/main — the endpoint lives on Codex's `codex/qe-governance-integration-20260509` branch. The UI is therefore mock-first: Playwright tests stub the endpoint via `page.route(...)`, and `nox strategy_package_governance_ui` runs in `STRATEGY_PACKAGE_GOVERNANCE_UI_MOCK_API=1` mode by default. **Live integration is one-line: flip the env var to 0 after Codex governance branch merges.**

## Pending (next session / teammate)

| # | Page | Reason deferred |
|---|---|---|
| 7.1.b | `market-regime` UI + new `/api/v1/market/regime-label/*` endpoints | Requires backend endpoint design (new router + service) plus a 5-year timeline chart. Out of single-session scope. |
| 7.1.c | `rl-execution` status page + Playwright tests | Page does not exist on main. Needs minimal status surface (v24/v20/v13 model state + latest evolution metrics). |

Suggested order: 7.1.c first (smaller, no backend changes), then 7.1.b (backend + chart).

## Catalog mutations summary

| File | Change |
|---|---|
| `tests/aistock_validation/catalog/module_registry.yaml` | `strategy_package` module gains `/strategy-package-governance` route + `strategy_package_governance_ui` plan recommendation |
| `tests/aistock_validation/catalog/file_ownership.yaml` | new rule `strategy_package_governance_frontend` (priority 80) |
| `tests/aistock_validation/catalog/ui_targets.yaml` | new `strategy_package.governance` target |
| `tests/aistock_validation/catalog/test_plans.yaml` | new `strategy_package_governance_ui` L2 plan (Playwright evidence, ports 8012/3012) |
| `backend/services/validation/plan_catalog.py` | `nox_strategy_package_governance_ui` added to `ALLOWED_COMMAND_KEYS` |
| `frontend/src/lib/navigation/nav-groups.ts` | sidebar entry under Paper Trading v2 |
| `noxfile.py` | new `strategy_package_governance_ui` session + qe_archive_ui port-default refresh |

## Verification run locally

- `npx tsc --noEmit --skipLibCheck` in `frontend/`: clean (no errors)
- `pytest backend/tests/test_validation_module_ownership.py backend/tests/test_validation_ui_target_catalog.py`: 13 passed
- `pytest backend/tests/test_validation_center_api.py`: 10 passed
- Playwright tests are not executed in this session (constraint: no dev-server / backend boot). User or Codex should run `nox -s strategy_package_governance_ui` and `nox -s qe_archive_ui` against dev ports 8012/3012 when ready.

## Constraints respected

- Only dev ports 8012/3012 in new wiring
- No production backend touched
- No business backend mutations (the one backend edit was the validation allowlist — required for catalog wiring)
- Tests are mock-first; no live API calls during nox runs
- Playwright tests follow the existing `page.route(...)` mock pattern from `qe-archive-dashboard.spec.ts`

## Open questions for strategy session

1. After Codex governance branch merges, who flips `STRATEGY_PACKAGE_GOVERNANCE_UI_MOCK_API=0` and re-runs the session against the real endpoint? (Suggest: pipeline-foundation team during Stage 7 wrap-up.)
2. Should 7.1.b backend endpoint `/api/v1/market/regime-label/*` go on dw-foundation worktree (since `market.regime_label` table is owned there) or on this frontend worktree (small additive router)? Lean toward dw-foundation.
