# BUG-193 HMM metadata-only preset auto-generation regression

- Module: paper_trading_v2 / selection_center / HMM
- Level: L2
- Date: 2026-06-01T22:28:08+08:00
- Branch: bug/BUG-193-paper-v2-hmm-auto-generation-fails-when-signal-p-20260601
- Git base: origin/main a923da3e
- Operator: lc999

## Scope

- Changed files:
  - backend/services/hmm_training_service.py
  - backend/tests/test_hmm_daily_coefficients.py
  - backend/tests/selection_center/test_runtime_selection.py
  - tests/aistock_validation/bugs/20260601_BUG-193-paper-v2-hmm-auto-generation-fails-when-signal-preset-has-no-coefficient.json
- Business goal: Paper v2 HMM auto-generation must not fail for built-in preset_A when the HMM config stores only operator-facing metadata for that built-in preset.
- Out of scope: production DB writes, service restarts, model weight or HMM snapshot mutation.
- Protected assets reviewed: no StrategyPackage frozen manifest, model weight, HMM snapshot, QE/RD-Agent artifact, or Paper ledger files changed.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| HMM preset parser | Built-in metadata-only preset_A resolves to default coefficients; custom metadata-only presets still fail fast | `backend/tests/test_hmm_daily_coefficients.py` | PASS |
| Selection/Paper runtime | HMM coefficient auto-generation path accepts metadata-only built-in preset and produces a real coefficient artifact path | `backend/tests/selection_center/test_runtime_selection.py::test_strategy_package_runtime_auto_generation_accepts_metadata_only_builtin_preset` | PASS |
| No silent fallback | Only built-in preset_A/B metadata-only configs use known defaults; custom metadata-only preset remains a hard error | unit tests above | PASS |
| Guardrails | Changed-file lint and whitespace checks pass | ruff, git diff --check | PASS |

## Commands

```powershell
python -m pytest -q backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_runtime_selection.py -p no:cacheprovider
python -m ruff check backend/services/hmm_training_service.py backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_runtime_selection.py
git diff --check
```

## Evidence

- `python -m pytest -q backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_runtime_selection.py -p no:cacheprovider` -> `61 passed`
- `python -m ruff check backend/services/hmm_training_service.py backend/tests/test_hmm_daily_coefficients.py backend/tests/selection_center/test_runtime_selection.py` -> `All checks passed!`
- `git diff --check` -> passed

## DESIGN-COMPLIANCE-001 Review

| Requirement | Implementation refs | Validation evidence | Status |
|---|---|---|---|
| Fix observed `HMM signal_preset has no coefficients: preset_A` for built-in metadata-only runtime preset | `backend/services/hmm_training_service.py` | HMM daily coefficient tests | PASS |
| Do not mask invalid custom presets | `backend/services/hmm_training_service.py` | custom metadata-only preset fail-fast test | PASS |
| Preserve Paper v2/Selection runtime auto-generation path | `backend/tests/selection_center/test_runtime_selection.py` | auto-generation integration-style unit test | PASS |
| No protected asset mutation | git diff name review | changed files are service/test/BUG evidence only | PASS |

## Result

- Final status: targeted L2 regression passed.
- Remaining risks: full `paper_v2_backend`, `paper_v2_l3`, `qe_read_l3`, and `validation_center_backend` still need to run before merge readiness.
- Need production backend restart: yes, after merge and user-controlled restart only.
- Production gates: `production_ddl_gate=noop`, `production_frontend_dependency_gate=noop`, `production_backend_dependency_gate=noop`.
