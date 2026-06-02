# QE read-only workspace access regression

- Module: qe
- Level: L3
- Date: 2026-06-02T21:51:37+08:00
- Git commit before task commit: f8a85fe8
- Operator: codex
- Issue: BUG-217 / GitHub #588

## Scope

- Changed files: none in QE runtime; QE L3 is required by BUG-217 validation selector because shared execution adapter touches cross-module execution semantics.
- Impacted flows: QE read-only backend/read-path regression only.
- Business goal: ensure Phase 2 MiniQMT adapter changes do not regress QE read-path contracts.
- Out of scope: QE UI E2E on 8011/3011, production backend restart, QE archive worker, data writes.
- Protected assets reviewed: no QE/RD-Agent artifacts or worker workspace files modified.

## Environment

- Backend port: 8011 not started for final evidence; an initial full `qe_read_l3` attempt failed because no dev backend was listening on 8011.
- Frontend port: 3011 not started for final evidence.
- TDX port: skipped by QE read-only UI service check; no production service touched.
- Conda/env: local Python/nox in this worktree.
- Database: backend tests use local configured test/read fixtures; no DDL or migration.
- Browser/headless: not used in final evidence.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| QE read guardrails | No blocking high-severity guardrail finding in QE read paths | `scan_quality_guardrails.py ... --fail-on HIGH`; 20 medium raw-json findings only | PASS |
| QE backend read tests | Evolution/experiment/log terminal read-path tests pass | `qe_read_backend`: 14 passed | PASS |
| QE UI E2E | Full UI E2E requires dev backend on 8011 and is not a Phase 2 MiniQMT adapter behavior | Initial full run stopped at service check: 8011 connection refused | NOT USED AS PASS EVIDENCE |
| Asset safety | No QE artifacts or protected workspace files modified | `git status`; changed files remain within BUG-217 scope | PASS |

## Commands

```powershell
$env:QE_READ_L3_SKIP_UI='1'; python -m nox -s qe_read_l3
```

## Evidence

- `qe_read_l3` guardrail section: successful; medium raw-json UI findings are non-blocking for `--fail-on HIGH`.
- `qe_read_backend`: 14 passed in 10.54s.
- Initial full `qe_read_l3` attempted UI and failed because no dev backend was listening on 8011; no production backend was started or restarted.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Full `qe_read_l3` attempted `qe_read_ui` and failed service check | no local backend on 8011; QE UI is not part of BUG-217 backend MiniQMT adapter behavior | reran required read-path validation with `QE_READ_L3_SKIP_UI=1` | `qe_read_l3` + `qe_read_backend` passed |

## Result

- Final status: PASS for BUG-217 QE read-only backend/guardrail validation with explicit skip-UI.
- Remaining risks: QE UI E2E not executed in final evidence; should be run in a dedicated UI validation window before any final full-platform release gate.
- Need production backend restart: no
- Need dev service restart: no
