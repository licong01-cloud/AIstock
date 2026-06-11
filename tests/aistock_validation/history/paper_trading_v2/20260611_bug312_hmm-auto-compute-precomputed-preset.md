# BUG-312 Paper v2 HMM auto-compute validation

- Module: paper_v2 / selection_center / HMM
- Date: 2026-06-11
- Branch: bug/BUG-312-paper-v2-hmm-400-20260611
- GitHub Issue: https://github.com/licong01-cloud/AIstock/issues/928

## Problem

Paper v2 selection returned `ARTIFACT_GENERATION_FAILED` HTTP 400 when HMM was enabled for snapshot `ecee7c40-6764-49ad-bc0f-1c6c6b390504`, `trade_date=2026-06-11`, `as_of_date=2026-06-10`.

The reported model config stores `signal_presets.preset_A` as precomputed metadata:

- `precomputed_only=true`
- `runtime_generation_supported=false`
- coefficient metadata range `[0.955, 1.0]`

The selection-time daily artifact path correctly detected that no artifact covered the trade date, but coefficient plan creation failed before it could materialize the cache because `preset_A` metadata was rejected as having no coefficients.

## Fix

- Keep normal HMM preset parsing fail-fast for metadata-only presets.
- Add a narrow opt-in for daily coefficient generation so built-in `preset_A` / `preset_B` can derive coefficients from precomputed metadata.
- For metadata range hints, derive `{trending: high, neutral: 1.0, fading: low}`; the reported config resolves to `{trending: 1.0, neutral: 1.0, fading: 0.955}`.
- Mark HMM-adjusted selection candidates with `|hmm_adjusted` while preserving the source reason in `component_scores["hmm"]["source_reason"]`.

## Verification

| Gate | Command / Evidence | Result |
|---|---|---|
| Targeted regression | `python -m pytest -q backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_hmm_runtime.py backend/tests/selection_center/test_runtime_selection.py -p no:cacheprovider` | PASS, 74 passed |
| Real snapshot preview | `preview_daily_coefficients(snapshot_id=ecee7c40-6764-49ad-bc0f-1c6c6b390504, preset_A, as_of=2026-06-10, effective=2026-06-11)` | PASS, `preset_coeffs={"trending":1.0,"neutral":1.0,"fading":0.955}`, `existing_artifact=false`, output `coefficients_preset_A_2026-06-11_2026-06-11.json` |
| Static lint | `python -m ruff check backend/services/hmm_training_service.py backend/services/selection_center/hmm_runtime.py backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_hmm_runtime.py` | PASS |
| Compile | `python -m compileall -q backend/services/hmm_training_service.py backend/services/selection_center/hmm_runtime.py backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_hmm_runtime.py` | PASS |
| Diff whitespace | `git diff --check` | PASS |
| Registry/L0 gate | `python -m nox -s l0 validation_module_registry_l0` | PASS |
| Paper v2 backend | `python -m nox -s paper_v2_backend` | PASS, 613 passed, 1 skipped, 2 xfailed |

## L3 boundary

A full `paper_v2_l3` run was attempted before the final reason-visibility fix. It proved the backend HMM auto-generation path could proceed, but the UI substage failed because the visible selection result reason did not contain `hmm`. The final patch adds `|hmm_adjusted` and covers that behavior with `test_adjust_candidates_marks_existing_reason_as_hmm_adjusted`.

The full UI L3 path may write real selection-run rows and generate HMM cache artifacts against the local AIstock DB/model directory, so it was not rerun without separate production-data write approval. Backend, targeted, static, and registry gates passed.

## Production Gates

- production_ddl_gate: noop
- production_backend_dependency_gate: noop
- production_frontend_dependency_gate: noop
- Production services `8001`, `3000`, and `19080` were not restarted.
- No DDL was applied.
