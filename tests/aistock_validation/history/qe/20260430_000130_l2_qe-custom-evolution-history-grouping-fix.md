# QE custom evolution history grouping fix

- Module: qe
- Level: L2
- Date: 2026-04-30T00:01:30
- Base Git commit before fix: 532b938
- Operator: lc999

## Scope

- Changed files:
  - backend/services/quantevolver/config_composer.py
  - backend/services/quantevolver/qe_evolution_service.py
  - backend/routers/quantevolver.py
  - frontend/src/app/quantevolver/experiments/page.tsx
  - backend/tests/unified_engine/test_qe_custom_evo_status.py
- Impacted flows:
  - QE experiment history list API.
  - QE custom_evo / strategy_evo loop persistence parent linkage.
  - QE experiment history frontend grouping.
- Business goal:
  - Single QE experiments, standard auto-evolution loops, and custom evolution tasks appear in the same history page with loops grouped under the correct parent card.
- Out of scope:
  - Starting/stopping QE jobs, modifying historical experiment artifacts, or changing Paper Trading v2.
- Protected assets reviewed:
  - No StrategyPackage manifest, QE workspace artifact, model weight, HMM snapshot, or validated execution policy was modified.

## Environment

- Backend port: not restarted; direct service/API logic validation only.
- Frontend port: not restarted; TypeScript static validation only.
- TDX port: not used.
- Conda/env: local Python environment via `python`; frontend via local `npx`.
- Database: local PostgreSQL/TimescaleDB `aistock`; read-only SELECT checks with existing local password env.
- Browser/headless: not run; changed frontend fetch query only.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `rg` scan on changed files; matches were existing env/default/fallback references, no new secret literal or protected path | Passed |
| Backend tests | QE custom-evolution grouping regression passes | `python -m pytest backend/tests/unified_engine/test_qe_custom_evo_status.py -q` -> 7 passed | Passed |
| API flow | History API view returns custom parent plus all loops grouped under base | Direct `ConfigComposer().list_experiments(limit=5, include_children=True)` returned `custom_parent_present=True`, `custom_child_count=10`, loops `[1..10]` | Passed |
| UI static | History page query requests grouped history mode and type-checks | `npx tsc --noEmit --pretty false --incremental false` -> exit 0 | Passed |
| Asset safety | No protected asset modified silently | `git status` reviewed; only code/test/run-record files touched for this fix | Passed |

## Commands

```powershell
python -m pytest backend/tests/unified_engine/test_qe_custom_evo_status.py -q
python -m py_compile backend/services/quantevolver/config_composer.py backend/services/quantevolver/qe_evolution_service.py backend/routers/quantevolver.py
cd frontend && npx tsc --noEmit --pretty false --incremental false

# DB password value intentionally omitted from this record.
$env:TDX_DB_PASSWORD='<local secret>'
@'
from backend.services.quantevolver.config_composer import ConfigComposer
res = ConfigComposer().list_experiments(limit=5, offset=0, include_children=True)
items = res["items"]
children = [e for e in items if e.get("parent_experiment_id") == "qe_20260429_015755_c4ba_base"]
print({
    "total": res["total"],
    "item_count": len(items),
    "custom_parent_present": any(e["experiment_id"] == "qe_20260429_015755_c4ba_base" for e in items),
    "custom_child_count": len(children),
    "child_loop_indices": [e.get("loop_index") for e in children],
})
'@ | python -
```

## Evidence

- API calls: direct service call equivalent to `/api/v1/quantevolver/experiments?limit=5&offset=0&include_children=true`.
- DB checks: read-only SELECT through `ConfigComposer`; latest custom task `qe_20260429_015755_c4ba` now appears as parent `qe_20260429_015755_c4ba_base` with 10 child loops in the response.
- Log files: not applicable; no service restart or job execution.
- Playwright report/trace: not run; UI code path changed only by adding `include_children=true`.
- Screenshots: not captured.
- Business output summary: custom evolution loops are no longer orphaned by `parent_experiment_id == task_id`; the history page receives a normalized parent id matching the base experiment card.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Custom evolution loops absent from QE history grouping | custom_evo/strategy_evo loop rows used `parent_experiment_id=task_id`, but the parent card is `*_base`; frontend only groups by experiment id | Added grouped history API mode that normalizes old rows to `base_experiment_id`, changed future loop persistence to write `base_experiment_id`, and made the history page request grouped mode | Unit tests passed; direct grouped API returned 10 loops under `qe_20260429_015755_c4ba_base` |

## Result

- Final status: Passed targeted L2 validation.
- Remaining risks: Browser E2E was not run because this fix is a one-line fetch-mode UI change plus backend response grouping; existing running services still need reload to pick up code.
- Need production backend restart: yes, to load the new `include_children` API behavior; production port was not restarted during validation.
- Need dev service restart: frontend dev server may hot-reload; backend process needs restart/reload.
