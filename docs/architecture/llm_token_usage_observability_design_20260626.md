# LLM Token Usage Observability Design

Version: 2026-06-26 v1
Tier: F1 standard pipeline capability
Owner: validation_llm_pipeline

## Background

AIstock nightly and CI workflows already use DeepSeek / GitHub Models in several warning-only steps, including test-plan advice, nightly scheduler advice, discovery hypotheses, adaptive scheduler, design-drift audit, and silent-degradation audit. Individual artifacts often contain `llm_invocation_evidence.usage_summary`, but there is no single run-level token usage summary that shows where token spend happened, whether usage was missing, and whether the spend produced useful candidates or plan changes.

The immediate goal is observability only. This feature must not add token limits, hard caps, CI failure gates, extra LLM calls, production DB writes, or service restarts.

## Scope

In scope:

- Add a run-level LLM usage summary artifact for nightly / code-intelligence output directories.
- Aggregate existing `llm_invocation_evidence.usage_summary` fields from existing artifacts.
- Render a concise Markdown summary for humans.
- Add compact CLI output that reports totals without dumping full JSON.
- Wire the summary into `.github/workflows/nightly.yml` after existing LLM artifacts are generated.
- Add regression tests proving aggregation, missing-usage reporting, and no limit enforcement.

## Non-Goals

- No token limit, budget enforcement, warning threshold, or CI/nightly blocking gate.
- No new LLM invocation and no prompt content capture.
- No production backend/frontend restart, no production DB/DDL, and no runtime service change.
- No Research Assistant usage persistence in this first phase; that can be a later feature after nightly/CI artifact usage is stable.
- No UI dashboard in this first phase.

## Design Acceptance Index

- F-001: Produce `llm-usage-summary.json` and optional `llm-usage-summary.md` from a nightly artifact directory using only existing artifacts.
- F-002: Normalize per-step records with `provider`, `model`, `invoked`, `fallback_used`, `fallback_reason`, `prompt_units`, `completion_units`, `total_units`, `usage_available`, and `usage_missing_reason`.
- F-003: Aggregate totals by run, provider/model, and step without enforcing limits or changing workflow gates.
- F-004: Include value context from existing artifacts: advice consumed, plan changed, selected plans, high-value candidate count, and issue payload count when available.
- F-005: Keep output compact and human-readable; successful commands must not dump large JSON to stdout.
- F-006: Wire nightly summary generation into `.github/workflows/nightly.yml` after existing LLM/code-intelligence artifacts, without adding any LLM calls.
- F-007: Preserve production safety: no DDL, no production DB writes, no service restart, no production port interaction.

## Implementation Plan

1. Add usage aggregation helpers to `scripts/code_intelligence_adapter.py` because it already owns code-intelligence nightly summary artifacts.
2. Reuse existing JSON artifacts under `tmp/validation/code-intelligence/<run_id>/`:
   - `llm-test-plan-advice.json`
   - `llm-nightly-scheduler-advice.json`
   - `llm-hypotheses.json`
   - `llm-nightly-adaptive-scheduler.json`
   - `design-drift-audit.json`
   - `silent-degradation-audit.json`
   - `llm-prompt-evaluation.json`
   - `llm-guarded-rollout-gate.json`
3. Add `build_llm_usage_summary()` and `render_llm_usage_summary_markdown()`.
4. Add CLI command `llm-usage-summary` with compact default stdout and explicit full JSON via existing output behavior.
5. Add regression tests in `backend/tests/scripts/test_code_intelligence_adapter.py`.
6. Add a nightly workflow step that writes:
   - `tmp/validation/code-intelligence/${RUN_ID}/llm-usage-summary.json`
   - `tmp/validation/code-intelligence/${RUN_ID}/llm-usage-summary.md`
7. Append the Markdown summary to the final nightly summary when present.

## Verification Plan

- `python -m pytest backend/tests/scripts/test_code_intelligence_adapter.py -q -p no:cacheprovider`
- `python scripts/code_intelligence_adapter.py llm-usage-summary --artifact-dir <tmp-dir> --output <json> --output-md <md>` against test fixtures or temporary artifacts.
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/llm_token_usage_observability_design_20260626.md --tier F1`
- `git diff --check`
- Targeted workflow text checks proving nightly calls `llm-usage-summary` and does not add `--invoke-llm` in that step.

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/code_intelligence_adapter.py `build_llm_usage_summary`, `cmd_llm_usage_summary`; .github/workflows/nightly.yml | `python -m pytest backend/tests/scripts/test_code_intelligence_adapter.py -q -p no:cacheprovider` -> 61 passed; CLI smoke wrote json/md | verified | none |
| F-002 | scripts/code_intelligence_adapter.py usage record normalization | pytest covers usage present, invoked missing usage, not invoked | verified | none |
| F-003 | scripts/code_intelligence_adapter.py totals and provider/model summary | pytest asserts total prompt/completion/total units and `limit_enforced=false` | verified | none |
| F-004 | scripts/code_intelligence_adapter.py value context extraction | pytest asserts advice/candidate fields in summary | verified | none |
| F-005 | scripts/code_intelligence_adapter.py compact CLI output and Markdown renderer | pytest capsys asserts compact stdout excludes raw records; CLI smoke prints one compact line | verified | none |
| F-006 | .github/workflows/nightly.yml usage summary step and final summary append | workflow text assertion confirms no `--invoke-llm` in usage step | verified | none |
| F-007 | design + implementation avoids DB/runtime/service changes | diff review; production gates are noop | verified | none |

## Risks

- Some providers or fallback paths may not return usage. The summary records `usage_available=false` and a reason instead of inventing numbers.
- Existing artifacts may be missing in failed nightly runs. Missing artifacts are warnings in the summary, not blockers.
- The summary must not be interpreted as a budget gate. `limit_enforced` remains false and no threshold fields are introduced in this phase.

## Production Gates

- `production_ddl_gate`: `noop`
- `production_frontend_dependency_gate`: `noop`
- `production_backend_dependency_gate`: `noop`

No production runtime, DB, DDL, dependency, or service restart is required.

