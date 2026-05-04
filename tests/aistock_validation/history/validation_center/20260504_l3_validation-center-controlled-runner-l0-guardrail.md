# AIstock Guardrail Baseline Scan

- Generated at: 2026-05-04T11:31:53.934814+00:00
- Mode: `paths`
- Files scanned: 19
- Total findings: 4

## Summary By Baseline Status

| Status | Count |
|---|---:|
| `baseline` | 1 |
| `new` | 3 |

## Summary By Severity

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 1 |
| P2 | 3 |
| P3 | 0 |

## Summary By Rule

| Rule | Count |
|---|---:|
| `SCRIPT-LOCATION-001` | 1 |
| `UI-RAWJSON-001` | 3 |

## Interpretation

This is a read-only baseline report. It does not mean all historical findings must be fixed immediately.
New or changed P0/P1 findings should be blocked after the changed-files gate is enabled.
Historical findings should be triaged by module and burned down with regression tests.

## First 4 Findings

| Severity | Status | Rule | File | Line | Remediation |
|---|---|---|---|---:|---|
| P2 | `new` | `UI-RAWJSON-001` | `frontend/src/app/validation-center/page.tsx` | 679 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `new` | `UI-RAWJSON-001` | `frontend/src/lib/validation/api.ts` | 384 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `new` | `UI-RAWJSON-001` | `frontend/src/lib/validation/api.ts` | 482 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P1 | `baseline` | `SCRIPT-LOCATION-001` | `noxfile.py` | 1 | Put one-off diagnostics under debug_tools/<module>/<date_or_issue>; reusable business scripts need parameters and tests under scripts. |
