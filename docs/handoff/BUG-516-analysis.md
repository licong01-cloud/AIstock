# BUG-516 Analysis - Nightly Silent-Degradation Suppression

## Root Cause

The nightly silent-degradation audit had no auditable suppression mechanism. Once a candidate was manually confirmed as a false positive, the audit still emitted it on later runs. This was especially costly for LLM-shaped findings because `finding_id` is derived from `module`, `title`, `reference_refs`, and `code_refs`; LLM-provided titles and refs can drift, so a finding-id-only allowlist would be fragile.

The concrete trigger was `SDA-2d2969408339` for `miniqmt_execution_runtime`, which Tier2 reviewed on 2026-06-24 as a false positive. The relevant MiniQMT paths are per-order fail-closed-and-loud with explicit error details, not silent degradation.

## Implemented Design

- Added optional top-level `suppressions` config in `configs/validation/silent_degradation_audit.yaml`.
- Matching is structured and requires module equality plus either exact `finding_id` or `code_refs_any` path-prefix overlap with finding `code_refs`; line numbers are ignored.
- `title_contains` is an optional case-insensitive guard applied as an AND condition.
- `expires_at` is honored, so expired suppressions stop applying and the finding can reappear.
- Invalid suppressions that omit both `finding_id` and `code_refs_any` raise `SilentDegradationAuditError` loudly.
- `build_audit()` applies suppressions after deterministic/LLM findings are aggregated and before `workflow_gate` is computed.
- Suppressed items are moved into `suppressed_findings` with `suppressed_by` metadata and do not count toward `workflow_gate`.

## Why This Is Not Silent Dropping

Suppressed findings remain in both the internal payload and the public artifact under `suppressed_findings`. The public markdown output also reports `suppressed_count` and lists the suppressed findings with suppression reason and expiry. This keeps every dismissal visible, reviewable, and reversible.

## First Suppression

The first configured suppression covers `SDA-2d2969408339` for `miniqmt_execution_runtime`, guarded by both finding id and MiniQMT runtime/client/gateway code refs, with `title_contains: fail-closed`, Tier2 reason, and expiry on 2026-09-24.

## Validation Plan

- `python -m pytest backend/tests/scripts/test_nightly_silent_degradation_audit.py -q`
- `ruff` on changed Python files
- `python -m nox -s l0`
- `python -m nox -s validation_module_registry_l0`
- `git diff --check`

## Production Gates

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
