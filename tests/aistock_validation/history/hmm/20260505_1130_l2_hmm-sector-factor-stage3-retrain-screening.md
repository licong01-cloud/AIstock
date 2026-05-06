# HMM Sector-Factor Stage3 Retrain Screening - 2026-05-05

## Scope

- Module: HMM / QE diagnostic research.
- Level: L2 script-level business diagnostic plus L0 guardrails.
- Goal: continue HMM-focused R&D by training sector-factor candidates inside `GaussianHMM.fit`, not by applying post-hoc sector-factor overlays.
- Production impact: no backend restart, no QE submission, no HMM registry/snapshot write.

## Changed Code

- `scripts/hmm_sector_factor_stage3_screen_20260505.py`
  - Added stage3 candidate wrapper with flow+breadth+tier, light turnover, dynamic flow sentiment, momentum, volatility, and preprocessing variants.
  - Extends the existing diagnostic engine at runtime with train-window-only `zscore` and `robust_zscore`; the original base diagnostic file is not modified.

## Commands

```powershell
python -m py_compile scripts/hmm_sector_factor_retrain_diagnostic_20260504.py scripts/hmm_sector_factor_stage3_screen_20260505.py
rg -n "except Exception|INSERT|UPDATE|DELETE|requests\.|8000|8001|quantevolver|model_train_|register|submit_qe|POST|http://" scripts/hmm_sector_factor_stage3_screen_20260505.py -S
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- scripts/hmm_sector_factor_stage3_screen_20260505.py tests/aistock_validation/history/hmm/20260505_1130_l2_hmm-sector-factor-stage3-retrain-screening.md
```

```powershell
wsl bash -lc "cd /mnt/f/Dev/AIstock && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -u scripts/hmm_sector_factor_stage3_screen_20260505.py --output-dir .codex_tmp/hmm_sector_factor_stage3_smoke_20260505 --candidates baseline_legacy7_winsor_zscore stage3_flow_breadth_tier_robust --max-sectors 3 --n-iter 10"
```

```powershell
wsl bash -lc "cd /mnt/f/Dev/AIstock && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -u scripts/hmm_sector_factor_stage3_screen_20260505.py --output-dir .codex_tmp/hmm_sector_factor_stage3_smoke2_20260505 --candidates stage3_flow_breadth_tier_robust stage3_flow_breadth_tier_zscore --max-sectors 3 --n-iter 5"
```

```powershell
wsl bash -lc "cd /mnt/f/Dev/AIstock && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -u scripts/hmm_sector_factor_stage3_screen_20260505.py --output-dir .codex_tmp/hmm_sector_factor_stage3_diag3_20260505 --n-states 3 --covariance-type diag --n-iter 300"
```

```powershell
wsl bash -lc "cd /mnt/f/Dev/AIstock && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -u scripts/hmm_sector_factor_stage3_screen_20260505.py --output-dir .codex_tmp/hmm_sector_factor_stage3_diag2_20260505 --n-states 2 --covariance-type diag --n-iter 300"
```

```powershell
wsl bash -lc "cd /mnt/f/Dev/AIstock && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -u scripts/hmm_sector_factor_stage3_screen_20260505.py --output-dir .codex_tmp/hmm_sector_factor_stage3_best_final_20260505 --candidates stage3_flow_breadth_tier_robust --n-states 2 --covariance-type diag --n-iter 300"
```

```powershell
@'
import json
import requests
rows = requests.get(
    "http://127.0.0.1:8001/api/v1/hmm-training/configs",
    params={"model_type": "sector_hmm"},
    timeout=30,
).json()
print(json.dumps({
    "count": len(rows),
    "rows": [{"config_id": r.get("config_id"), "display_name": r.get("display_name"), "model_type": r.get("model_type")} for r in rows],
}, ensure_ascii=False, indent=2))
'@ | python -
```

## Result Summary

Top combined stage2 + stage3 script-level candidates by holdout weighted RankIC:

| Rank | Candidate | Run | Score | Preprocess | States | Weighted RankIC | 5D | 10D | 20D | 10D Spread | 10D Hit |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `stage3_flow_breadth_tier_robust` | stage3 diag2 | `trend_fade` | `robust_zscore` | 2 | 0.027554 | 0.017641 | 0.025278 | 0.041776 | 0.002765 | 0.513960 |
| 2 | `flow_plus_breadth` | stage2 aggregate | `utility_raw` | `winsor_zscore` | 3 | 0.026085 | 0.021669 | 0.029110 | 0.027708 | 0.000838 | 0.509207 |
| 3 | `stage3_flow_breadth_turnover_light` | stage3 diag3 | `utility_raw` | `winsor_zscore` | 3 | 0.025714 | 0.021745 | 0.029173 | 0.026311 | 0.001730 | 0.506818 |
| 4 | `flow_plus_breadth` | stage2 aggregate | `utility_raw` | `winsor_zscore` | 2 | 0.025687 | 0.023609 | 0.027576 | 0.025909 | 0.000027 | 0.504135 |
| 5 | `flow_core` | stage2 aggregate | `utility_raw` | `winsor_zscore` | 2 | 0.025519 | 0.022062 | 0.028262 | 0.026352 | 0.000848 | 0.509059 |

Stage3 conclusions:

- `stage3_flow_breadth_tier_robust` is the new script-level leader, beating the previous stage2 best by +0.001469 weighted RankIC.
- The improvement is mostly from 20D sector-rotation RankIC, so it may favor slower rotation regimes and should be treated as a QE candidate, not a proven QE winner.
- `stage3_flow_breadth_turnover_light` is the best 3-state stage3 candidate and is close to the stage2 best; it may be a secondary QE candidate if only 3-state models are desired.
- Momentum-heavy and volatility-compression-heavy candidates remain weak in holdout and should not be promoted.

## Safety And Selector Validation

- `py_compile`: passed.
- Targeted L0 nox on changed files: passed with 0 findings.
- Mutation scan on the new wrapper: no broad exception, DB writes, HTTP calls, QE submission, or HMM registration paths found.
- HMM selector validation on production backend `8001`: still exactly 2 visible `sector_hmm` configs:
  - `ce4952c1-4b0d-46a7-81f2-ae1d4a249555` / `HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504`
  - `b99c907b-873a-4173-a4ee-5eab266f8c49` / `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore`
- Protected HMM model assets were not modified.

## Artifacts

- `.codex_tmp/hmm_sector_factor_stage3_smoke_20260505/`
- `.codex_tmp/hmm_sector_factor_stage3_smoke2_20260505/`
- `.codex_tmp/hmm_sector_factor_stage3_best_final_20260505/summary.csv`
- `.codex_tmp/hmm_sector_factor_stage3_diag2_20260505/summary.csv`
- `.codex_tmp/hmm_sector_factor_stage3_diag3_20260505/summary.csv`
- `.codex_tmp/hmm_sector_factor_stage3_aggregate_20260505/summary_all_runs.csv`
- `.codex_tmp/hmm_sector_factor_stage3_aggregate_20260505/report.md`

## Residual Risk

- These are sector-level diagnostic metrics, not stock-level QE PnL.
- The best candidate uses `trend_fade` scoring rather than the previous `utility_raw` mapping; if promoted, the registration/precompute path must preserve that scoring behavior instead of silently mapping it to a different preset.
- No new candidate was added to the QE selector in this stage.
