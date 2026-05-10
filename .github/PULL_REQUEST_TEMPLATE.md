# PR Description

## Summary

<!-- 1-3 sentence description of what this PR does and why. -->

## Bug registry linkage

<!--
Required if this PR fixes or partially addresses an issue tracked under
tests/aistock_validation/bugs/. Use the canonical BUG-NNN id.
Multiple BUGs allowed; use comma-separated.
-->

- Closes: BUG-NNN
- Partially addresses: BUG-NNN

## Affected modules

<!--
Pick from tests/aistock_validation/catalog/module_registry.yaml.
Multiple allowed. Examples:
  qe.archive, qe.archive_handlers, paper_v2, strategy_package,
  market.regime_label, model_registry, rl_execution, validation.center
-->

- module_id:
- module_id:

## Required validation plans

<!--
Pick the plans that MUST be green to merge. Reviewer will require evidence
either inline or via Validation Center / Actions artifacts.

L0 plans (always required for any code change):
  - l0
  - validation_module_registry_l0  (if catalog yaml changed)
  - guardrail_changed_files

L2 backend plans (pick those touching changed modules):
  - paper_v2_backend, qe_archive_backend, qe_data_contract_backend,
    validation_center_backend, model_registry_backend,
    market_regime_label, rl_execution_smoke

L3 plans (for cross-cutting / UI / live changes):
  - paper_v2_l3, qe_archive_l3, qe_read_l3, validation_center_real_port_ui

Use the `aistock-validation` MCP tool `start_validation_execution(plan_key=...)`
during local review or rely on the Actions matrix to run them on PR push.
-->

- plan_key:
- plan_key:

## DDL / schema changes

- [ ] No DDL changes
- [ ] DDL changes: list `backend/db/init_*.sql` or `backend/migrations/*.sql`
      below
- [ ] dev DB apply evidence (Validation Center run id or drawer link):

## Production safety

- [ ] `production_8001_touched=false`
- [ ] `production_db_writes=false`
- [ ] No production credentials, secrets, or hostnames in this diff
- [ ] If a release-affecting change: explicitly call out in description and
      link a release plan / drawer

## Reviewer routing

- [ ] claude_code (Claude Code Lead session)
- [ ] codex_app
- [ ] both (cross-tool review for high-risk changes)

## Test plan

<!--
Markdown checklist of what reviewers should verify. Include:
- Local nox commands run (and results)
- Validation Center plan keys triggered + run ids
- Manual smoke steps if any
-->

- [ ] `python -m nox -s l0` passes
- [ ] Affected L2 sessions pass
- [ ] (if UI) tsc --noEmit + Playwright spec passes
- [ ] (if DDL) dev DB apply succeeds + schema diff reviewed

## Worktree note

<!-- If this branch lives under F:/Dev/AIstock_worktrees/, name it here so
     reviewers know which working copy was used. Optional. -->

worktree:
