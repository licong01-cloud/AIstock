# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-04-29T01:02:26
- Git commit: 43e9c3d
- Operator: lc999

## Scope

- Changed files:
  - `noxfile.py`
  - `scripts/aistock_validate.py`
  - `.pre-commit-config.yaml`
  - `.semgrep/aistock/guardrails.yml`
  - `.codex/skills/verify-aistock-feature/SKILL.md`
  - `.codex/skills/verify-aistock-feature/scripts/new_test_run.py`
  - `.codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py`
  - `tests/aistock_validation/**`
  - `requirements-dev.txt`
- Impacted flows:
  - Local validation entry points for Paper v2 + Selection Center first-stage rollout.
- Business goal:
  - Establish a repeatable local L0/L3 validation path before deeper Paper v2 UI/API business validation is expanded.
- Out of scope:
  - No backend service restart, no production port 8001 access, no DB migration, no strategy/model/HMM asset changes.
- Protected assets reviewed:
  - No protected asset files or DB assets were modified by this infrastructure-only change.

## Environment

- Backend port:
  - Not started.
- Frontend port:
  - Not started.
- TDX port:
  - Not used.
- Conda/env:
  - `AIstock`; installed local dev tools `nox` and `pytest-html` into the conda environment.
- Database:
  - Not used by this validation run.
- Browser/headless:
  - Not used in this run.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `conda run -n AIstock python -m nox -s l0` | PASS; 13 medium review findings, 0 high blocking findings |
| Backend tests | Paper v2 + Selection Center backend tests pass | `conda run -n AIstock python -m nox -s paper_v2_backend` | PASS; 103 passed |
| API flow | API, DB, and logs agree | Not executed in this infrastructure bootstrap | Deferred |
| UI E2E | User-visible flow works with no console/page/request errors | Not executed in this infrastructure bootstrap | Deferred |
| Asset safety | No protected asset modified silently | Git status reviewed; only validation infra/docs changed for this task | PASS |

## Commands

```bash
conda run -n AIstock python -m pip install nox pytest-html
conda run -n AIstock python -m nox -s l0
conda run -n AIstock python -m nox -s paper_v2_backend
conda run -n AIstock python -m nox -s paper_v2_l3
conda run -n AIstock python -m compileall noxfile.py scripts/aistock_validate.py .codex/skills/verify-aistock-feature/scripts
```

## Evidence

- API calls:
  - Not executed.
- DB checks:
  - Not executed.
- Log files:
  - Nox command output in terminal; no backend service logs.
- Playwright report/trace:
  - Not generated.
- Screenshots:
  - Not generated.
- Business output summary:
  - First-stage local runner now executes L0 and backend Paper v2 + Selection Center tests together.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| L0 initially scanned unrelated dirty QE files and failed | `--changed-only` included unrelated existing workspace changes | Updated `noxfile.py` L0 to scan the Paper v2/Selection first-stage paths by default | Rerun `conda run -n AIstock python -m nox -s l0` passed |

## Result

- Final status: PASS for first-stage infrastructure bootstrap L0 + backend suite.
- Remaining risks:
  - API flow and UI E2E are intentionally deferred to the next implementation step; this run only proves the local runner skeleton and backend suite.
  - Medium guardrail findings remain as review items in existing Paper v2 test files and strategy package tests.
- Need production backend restart: no
- Need dev service restart: no
