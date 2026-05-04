# AIstock Guardrail Baseline Scan

- Generated at: 2026-05-04T12:14:01.359723+00:00
- Mode: `paths`
- Files scanned: 20
- Total findings: 6

## Summary By Baseline Status

| Status | Count |
|---|---:|
| `baseline` | 1 |
| `new` | 5 |

## Summary By Severity

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 1 |
| P2 | 5 |
| P3 | 0 |

## Summary By Rule

| Rule | Count |
|---|---:|
| `ALGO-COMPLEXITY-001` | 2 |
| `SCRIPT-LOCATION-001` | 1 |
| `UI-RAWJSON-001` | 3 |

## Interpretation

This is a read-only baseline report. It does not mean all historical findings must be fixed immediately.
New or changed P0/P1 findings should be blocked after the changed-files gate is enabled.
Historical findings should be triaged by module and burned down with regression tests.

## First 6 Findings

| Severity | Status | Rule | File | Line | Remediation |
|---|---|---|---|---:|---|
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/validation/execution_runner.py` | 245 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/validation/execution_runner.py` | 639 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `UI-RAWJSON-001` | `frontend/src/app/validation-center/page.tsx` | 749 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `new` | `UI-RAWJSON-001` | `frontend/src/lib/validation/api.ts` | 417 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `new` | `UI-RAWJSON-001` | `frontend/src/lib/validation/api.ts` | 521 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P1 | `baseline` | `SCRIPT-LOCATION-001` | `noxfile.py` | 1 | Put one-off diagnostics under debug_tools/<module>/<date_or_issue>; reusable business scripts need parameters and tests under scripts. |
