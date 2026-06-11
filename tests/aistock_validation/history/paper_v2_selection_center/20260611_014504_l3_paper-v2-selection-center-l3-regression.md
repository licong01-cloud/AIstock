# Paper v2 Selection Center L3 regression - BUG-312

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-11T01:45:04+08:00
- Branch: bug/BUG-312-paper-v2-hmm-400-20260611
- Git commit at attempted L3 start: 95dc9def
- Operator: lc999 / Codex

## Scope

- Changed files: HMM daily coefficient planning, Selection Center HMM runtime reason metadata, HMM regression tests, BUG-312 registry, and validation history.
- Business goal: selecting HMM in Paper v2 must auto-compute and cache missing daily HMM coefficients instead of failing with HTTP 400 when a built-in metadata-only preset is selected.
- Out of scope: production service restart, production DDL, frontend dependency changes, and manual live trading operation.
- Protected assets reviewed: no DDL, no dependency files, no production service restart.

## Environment

- Backend port: validation-owned test backend only when invoked by `paper_v2_l3`; production `8001` was not restarted.
- Frontend port: validation-owned test frontend only when invoked by `paper_v2_l3`; production `3000` was not restarted.
- TDX port: production `19080` was not restarted.
- Database: local AIstock DB/model data used by Paper v2 validation; full UI L3 can write selection-run rows and HMM cache artifacts.
- Browser/headless: attempted through `paper_v2_l3` before the final UI reason-visibility fix.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Targeted HMM auto-compute | Metadata-only built-in `preset_A` can create a daily coefficient plan for selection-time cache generation | `pytest backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_hmm_runtime.py backend/tests/selection_center/test_runtime_selection.py` -> 74 passed | PASS |
| Real snapshot preview | Reported snapshot/preset resolves dated coefficients without an existing artifact | `preview_daily_coefficients(...)` -> `trending=1.0`, `neutral=1.0`, `fading=0.955`, `existing_artifact=false` | PASS |
| Backend Paper v2 regression | Paper v2 backend selection/runtime tests remain green | `python -m nox -s paper_v2_backend` -> 613 passed, 1 skipped, 2 xfailed | PASS |
| Registry/L0 guardrails | No blocking registry or L0 guardrail regression | `python -m nox -s l0 validation_module_registry_l0` | PASS |
| Full UI L3 attempt | UI should expose HMM-adjusted selection output | Initial `paper_v2_l3` attempt failed before final reason fix because the result table did not include `hmm` in the visible reason | FIXED BY UNIT REGRESSION, FULL UI NOT RERUN |

## Commands

```bash
python -m pytest -q backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_hmm_runtime.py backend/tests/selection_center/test_runtime_selection.py -p no:cacheprovider
python -m nox -s l0 validation_module_registry_l0
python -m nox -s paper_v2_backend
python -m ruff check backend/services/hmm_training_service.py backend/services/selection_center/hmm_runtime.py backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_hmm_runtime.py
python -m compileall -q backend/services/hmm_training_service.py backend/services/selection_center/hmm_runtime.py backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_hmm_runtime.py
git diff --check
```

## Evidence

- Targeted regression: 74 passed.
- Real snapshot preview: `snapshot_id=ecee7c40-6764-49ad-bc0f-1c6c6b390504`, `preset_A`, `as_of=2026-06-10`, `effective=2026-06-11` resolved `{"trending":1.0,"neutral":1.0,"fading":0.955}` and planned `coefficients_preset_A_2026-06-11_2026-06-11.json`.
- Static/lint: Ruff, compileall, and `git diff --check` passed.
- Backend/regression gates: `paper_v2_backend` passed; `l0 validation_module_registry_l0` passed.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `ARTIFACT_GENERATION_FAILED` / `HMM signal_preset has no coefficients: preset_A` | The daily coefficient plan path treated precomputed metadata-only built-in `preset_A` as invalid coefficients, so cache auto-generation stopped before materializing the trade-date artifact | Added narrow selection-time opt-in for built-in metadata-only presets and parsed range hints into state coefficients | Targeted HMM tests and real snapshot preview passed |
| UI L3 result table did not contain `hmm` | HMM adjusted candidate kept the original reason `live_qe_model_inference_score`, so visible UI assertions could not see the HMM adjustment even though component metadata existed | Append `|hmm_adjusted` to non-HMM reasons and preserve source reason under the HMM component | `test_adjust_candidates_marks_existing_reason_as_hmm_adjusted` passed as part of targeted regression |

## Result

- Final status: backend, targeted, registry, static, and real snapshot preview evidence passed.
- Full UI L3 was not rerun after the final reason fix because that path can write local AIstock DB rows and generate real HMM cache artifacts; the UI-visible reason behavior is covered by a focused unit regression.
- Need production backend restart: no
- Need production frontend restart: no
- production_ddl_gate: noop
- production_backend_dependency_gate: noop
- production_frontend_dependency_gate: noop
