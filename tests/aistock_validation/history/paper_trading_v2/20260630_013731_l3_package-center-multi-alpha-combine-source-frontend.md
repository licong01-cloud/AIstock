# Package center multi-alpha combine source frontend validation

## Scope

- Worktree: `F:\Dev\AIstock_worktrees\paper-v2-multi-alpha-combine-source-20260630`
- Branch: `feature/paper-v2-multi-alpha-combine-source`
- Change type: frontend-only package-center creation flow.
- Backend/services: no start, no restart, no backend code change.

## Design Compliance Matrix

| item | implementation refs | evidence | status | gap/exception |
|---|---|---|---|---|
| F-001 source type and selector | `frontend/src/lib/paper-v2/types.ts`, `frontend/src/app/paper-v2/packages/page.tsx` | `aistock_feature_workflow validate` PASS; lint/tsc/build PASS | verified | - |
| F-002 combine-only selector | `frontend/src/lib/paper-v2/api.ts`, `frontend/src/app/paper-v2/packages/page.tsx` | read-only GET `/multi-alpha/combine-backtest/runs?status=succeeded&limit=20` returned roster_hash `7738e811293948eb` with 2 legs `a1_plus3_LSTM_h20` and `new_FUNDGROWTH_h20`; lint/tsc/build PASS | verified | - |
| F-003 strict create payload | `frontend/src/app/paper-v2/packages/page.tsx` | code review confirms `ic_weighted`, `topk`, `weight_policy.mode=frozen_backtest_terminal_weights`, `confirmation=MULTI_ALPHA_PACKAGE_PROMOTE`, no `component_package_ids`; tsc/build PASS; validation POST preserved backend 400 without silent fallback | implementation verified | full create chain blocked by backend/runtime stored manifest hash mismatch |
| F-004 fail-fast/error visibility | `frontend/src/lib/paper-v2/api.ts`, `frontend/src/app/paper-v2/packages/page.tsx` | QE-only buttons disabled for combine source; `parseError` preserves `detail.reason_code`; no catch-to-success path | verified | - |
| F-005 post-create guidance | `frontend/src/app/paper-v2/packages/page.tsx` | success preview includes `paper_admission` and explicit `paper-runtime-dry-run(local_sim)` next step | verified | - |

## Commands And Results

- `rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/paper_v2_package_center_multi_alpha_source_f0_20260630.md --tier F0` -> PASS.
- `rtk git diff --check` -> PASS.
- `cd frontend && rtk npm run lint` -> PASS with pre-existing `react-hooks/exhaustive-deps` warnings in unrelated files.
- `cd frontend && rtk npx tsc --noEmit` -> PASS, `TypeScript: No errors found`.
- `cd frontend && rtk npm run build` -> PASS, `/paper-v2/packages` built successfully.
- Read-only API: `GET http://127.0.0.1:8001/api/v1/multi-alpha/combine-backtest/runs?status=succeeded&limit=20` -> returned succeeded run `macb_7738e811293948eb_20250601_20260310_20260627T191255096216Z`, roster_hash `7738e811293948eb`, two legs `a1_plus3_LSTM_h20` and `new_FUNDGROWTH_h20`, topk 25.
- Read-only API: `GET http://127.0.0.1:8001/api/v1/multi-alpha/combine-backtest/runs/macb_7738e811293948eb_20250601_20260310_20260627T191255096216Z` -> PASS, returned run detail with `scheme_results` including `ic_weighted`.
- Validation write API: `POST http://127.0.0.1:8001/api/v1/strategy-packages/from-multi-alpha-combine-run` with `combine_backtest_run_id=macb_7738e811293948eb_20250601_20260310_20260627T191255096216Z`, `weighting_scheme=ic_weighted`, `topk=25`, `weight_policy.mode=frozen_backtest_terminal_weights`, `confirmation=MULTI_ALPHA_PACKAGE_PROMOTE` -> HTTP 400, surfaced backend `STRATEGY_PACKAGE_VALIDATION_ERROR` instead of fallback success.

## Backend Runtime Blocker

The valid create POST reached the existing backend but failed before the end-to-end chain could continue:

```json
{
  "detail": {
    "error_code": "STRATEGY_PACKAGE_VALIDATION_ERROR",
    "message": "stored manifest_sha256 does not match stored manifest",
    "context": {
      "package_id": "pkg_b4ce634c24bd470fac2c7b581a4e106f",
      "stored_sha256": "19b02fa414351d6d96f9d721f676bd080072526aa48a26819e58f1e5182ee50e",
      "computed_sha256": "117e7f2b057a60fa24b338d4002be891217dbe88218716cf3537c9bd01340fed"
    }
  }
}
```

- Follow-up read of `/strategy-packages/pkg_b4ce634c24bd470fac2c7b581a4e106f` returned the same 400.
- `/strategy-packages?limit=500` did not show a selectable usable package for that id.
- No backend fix was attempted because this task is frontend-only and the user explicitly said not to change backend.

## Not Executed

- Did not start or restart frontend/backend/TDX services.
- Did not complete `paper-runtime-dry-run(local_sim)`, selectable-packages, advisory dropdown, or create_portfolio because the create step was blocked by the backend/runtime stored manifest hash mismatch above.
- No screenshot captured because no dev frontend service was started per task constraint.

## Production Gates

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- Production runtime touched: no starts/restarts; existing `8001` was accessed for read-only combine GET/detail checks and one validation POST that failed with backend 400.
