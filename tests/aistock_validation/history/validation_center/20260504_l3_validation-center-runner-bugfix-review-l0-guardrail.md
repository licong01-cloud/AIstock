# AIstock Guardrail Baseline Scan

- Generated at: 2026-05-04T13:05:03.222163+00:00
- Mode: `paths`
- Files scanned: 21
- Total findings: 2

## Summary By Baseline Status

| Status | Count |
|---|---:|
| `new` | 2 |

## Summary By Severity

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 0 |
| P2 | 2 |
| P3 | 0 |

## Summary By Rule

| Rule | Count |
|---|---:|
| `ALGO-COMPLEXITY-001` | 2 |

## Interpretation

This is a read-only baseline report. It does not mean all historical findings must be fixed immediately.
New or changed P0/P1 findings should be blocked after the changed-files gate is enabled.
Historical findings should be triaged by module and burned down with regression tests.

## First 2 Findings

| Severity | Status | Rule | File | Line | Remediation |
|---|---|---|---|---:|---|
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/validation/execution_runner.py` | 254 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/validation/execution_runner.py` | 665 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
