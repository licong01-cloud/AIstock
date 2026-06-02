# Research Assistant Phase 7 Live Smoke Template

- phase: Phase 7
- plan_key: `ra_phase7_full_accept`
- smoke_type: user-started read-only Playwright smoke
- automatic_g1_gate: excluded
- production_8001_touched: false
- production_3000_touched: false
- production_19080_touched: false
- production_ddl_gate: required_pending_user_approval

## Preconditions

- User starts a development backend on `8011` or `8012`.
- User starts a development frontend on `3011` or `3012`.
- Do not start, stop, restart, or call production `8001`, `3000`, or `19080`.
- Do not apply production DDL.

## Command

```powershell
cd frontend
$env:RA_PHASE7_LIVE_SMOKE = "1"
$env:FRONTEND_PORT = "3011"
$env:BACKEND_PORT = "8011"
$env:FRONTEND_BASE_URL = "http://127.0.0.1:3011"
$env:NEXT_PUBLIC_API_BASE = "http://127.0.0.1:8011/api/v1"
$env:NEXT_PUBLIC_TDX_BACKEND_BASE = "http://127.0.0.1:8011"
npx playwright test tests/research-assistant/phase7-live-smoke.spec.ts --project chromium --grep "600584"
```

## Expected Evidence

- Input `600584 是否值得买入`.
- The UI shows either evidence cards with `source`, `provenance`, and `as_of`, or honest blocker cards explaining missing evidence or required approval.
- The UI contains no `TODO`, `placeholder`, `XX`, `X%`, fake `mock` business result, or generated default `as_of`.
- The smoke does not trigger confirmed actions, QE runs, GitHub sync, production DDL, or production DB writes.

## Result

- status: `not_run_manual_smoke`
- reason: Manual smoke is intentionally outside automatic G1-central. Record the user-started run output, screenshot or trace path, and production gate confirmation here when executed.
