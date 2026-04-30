# HMM DB vs Dynamic 1Y Script Comparison - 2026-04-29

## Scope

- Module: HMM offline validation.
- Level: L2 script/business-result validation.
- Guardrails: no DB writes, no QE experiment, no backend/frontend runtime code changes.

## Commands

```powershell
wsl -d Ubuntu -- bash -lc 'source ~/miniconda3/etc/profile.d/conda.sh 2>/dev/null || true; conda activate rdagent-gpu 2>/dev/null || true; cd /mnt/f/Dev/AIstock && python -m py_compile scripts/hmm_db_vs_dynamic_1y_compare.py'
```

```powershell
wsl -d Ubuntu -- bash -lc 'source ~/miniconda3/etc/profile.d/conda.sh 2>/dev/null || true; conda activate rdagent-gpu 2>/dev/null || true; cd /mnt/f/Dev/AIstock && export TDX_DB_PASSWORD=*** && python scripts/hmm_db_vs_dynamic_1y_compare.py --output-root /mnt/f/Dev/AIstock/.codex_tmp/hmm_db_vs_dynamic_1y_20260429 --docs-report /mnt/f/Dev/AIstock/docs/analysis/hmm_db_vs_dynamic_1y_comparison_report_20260429.md'
```

## Data Window

- qlib daily validation: 2025-03-11 ~ 2026-03-03.
- TopK: 50.
- Rebalance: 5 trading days.
- Raw score: trailing 5D/10D/20D rank blend.

## Results

| Rank | Version | Source | PIT | Total | Sharpe | MaxDD |
|---:|---|---|---|---:|---:|---:|
| 1 | `OFFLINE_DYNAMIC::p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075` | offline_dynamic | Y | -0.81% | 0.142 | -30.91% |
| 2 | `OFFLINE_DYNAMIC::p8_pup_w20_50_clip_0p9800_1p0150_conf_0p10` | offline_dynamic | Y | -0.95% | 0.138 | -30.91% |
| 3 | `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore::preset_A` | db | N | -8.74% | -0.182 | -27.27% |
| 4 | `HMM_HORIZON_V2_w5w10w20_oos6m__n3_diag_ms75_no_limitup::preset_A` | db | N | -11.20% | -0.239 | -32.26% |
| 5 | `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_B` | db | N | -12.89% | -0.440 | -29.44% |
| 8 | `NO_HMM_BASELINE` | baseline | Y | -21.00% | -0.628 | -37.34% |

## Business Oracles

- Existing full-window DB artifacts are diagnostic-only for this 1Y window because train/validation periods overlap the test start.
- Offline dynamic candidates are PIT-compatible for this 1Y window.
- No-HMM baseline is included in the same script methodology.
- Capital utilization proxy is 100% and buy-unfilled proxy is 0% by construction; this is not a minute execution simulation.

## Evidence

- Report: `docs/analysis/hmm_db_vs_dynamic_1y_comparison_report_20260429.md`.
- JSON: `.codex_tmp/hmm_db_vs_dynamic_1y_20260429/run_summary.json`.
- Summary CSV: `.codex_tmp/hmm_db_vs_dynamic_1y_20260429/summary.csv`.
- Monthly CSV: `.codex_tmp/hmm_db_vs_dynamic_1y_20260429/monthly.csv`.
- Contributions CSV: `.codex_tmp/hmm_db_vs_dynamic_1y_20260429/contributions.csv`.

## Asset Safety

- DB HMM config count after validation: 4.
- DB HMM snapshot count after validation: 4.
- No new DB HMM config/snapshot/job inserted.
- No production backend/frontend port was restarted.

## Recommendation

Treat `p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075` as the pending DB-registration candidate, but do not insert it until explicit user confirmation.
