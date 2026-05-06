# HMM Loop10-Centered Attribution Screen - L2

## Scope

- Task analyzed: `qe_20260504_184036_3a3c`.
- Baseline: `LOOP10_BASE__penalty_only_f096` / `6ea64754-003d-48d8-ad9e-d0e7857716c8`.
- Goal: run script-level attribution before any further remote QE, and screen Loop10-centered virtual candidates.

## Commands

```powershell
python scripts/diagnostics/hmm_qe_candidate_attribution.py qe_20260504_184036_3a3c --registry .codex_tmp/hmm_registry_updates/hmm_utility_mapping_registry_result_20260504_183459.json --output-dir .codex_tmp/hmm_qe_utility_candidate_attribution --api-base http://127.0.0.1:8001/api/v1 --topk 50
python scripts/diagnostics/hmm_loop10_centered_attribution.py --task-id qe_20260504_184036_3a3c --task-detail-json .codex_tmp/qe_20260504_184036_3a3c_detail_final.json --registry .codex_tmp/hmm_registry_updates/hmm_utility_mapping_registry_result_20260504_183459.json --topk 50
python scripts/diagnostics/hmm_loop10_virtual_candidate_screen.py --task-id qe_20260504_184036_3a3c --registry .codex_tmp/hmm_registry_updates/hmm_utility_mapping_registry_result_20260504_183459.json --topk 50
python -m py_compile scripts/diagnostics/hmm_loop10_centered_attribution.py scripts/diagnostics/hmm_loop10_virtual_candidate_screen.py
```

## Evidence

- Loop10-centered report: `.codex_tmp/hmm_loop10_centered_attribution/qe_20260504_184036_3a3c/loop10_centered_attribution_report.md`.
- Virtual candidate report: `.codex_tmp/hmm_loop10_virtual_candidate_screen/qe_20260504_184036_3a3c/virtual_candidate_screen_report.md`.
- Virtual coefficient files: `.codex_tmp/hmm_loop10_virtual_candidate_screen/qe_20260504_184036_3a3c/candidate_coefficients/`.

## Business Outcome

- Current utility/aggressive QE candidates were all negative vs Loop10 in completed QE daily deltas.
- Pairwise TopK attribution vs Loop10 also showed utility/aggressive replacements have negative holdout DB 10d contribution.
- The only virtual candidate with positive full/train/holdout DB 10d attribution was `VIRT_L10_FPB_VALZ_BOTTOM20_PENALTY_0p98`.
- No remote QE task was submitted in this validation pass; this preserves the user's requirement to avoid multi-hour QE on weak candidates.

## Residual Risk

- Script-level TopK attribution is a filter, not a substitute for full QE with minute execution and tail-substitute behavior.
- DB forward returns are read from `market.kline_daily_raw`; a pandas DBAPI warning was emitted but data reads succeeded.
- Virtual coefficients are unregistered runtime artifacts and cannot be selected in QE until explicitly registered.

## Additional Grid Screen

- Expanded `hmm_loop10_virtual_candidate_screen.py` to evaluate 34 Loop10-centered virtual candidates.
- Grid included FPB_VALZ and VOLCOMP bottom 10/15/20/25 percent penalty overlays, penalties 0.99/0.985/0.98, VOLCOMP risk-only penalties, and mild blend/confirm variants.
- Re-ran py_compile after the grid expansion.
- The strongest robust script-level candidate was `VIRT_L10_FPB_VALZ_BOTTOMP20_PENALTY_0p98`; DB 10d attribution was positive in full/train/holdout.
