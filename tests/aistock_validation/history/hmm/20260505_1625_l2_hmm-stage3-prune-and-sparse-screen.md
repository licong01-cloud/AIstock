# HMM Stage3 Prune And Sparse Penalty Screen - L2

## Scope

- Module: HMM / QE selector and script-level diagnostics.
- Goal:
  - Hide the four Stage3 HMM versions that underperformed in `qe_20260505_123035_bf80`.
  - Keep only Loop10 and the old Loop2 baseline visible in QE HMM selectors.
  - Screen sparse penalty-only mappings from already retrained Stage3 HMM score panels before any new remote QE run.
- Production impact:
  - No backend restart.
  - No QE task submission.
  - No physical HMM asset deletion.

## Selector Prune

- Archived underperforming Stage3 configs by changing `model_train_configs.model_type` from `sector_hmm` to `sector_hmm_disabled_stage3_underperform_20260505`.
- Archived configs:
  - `79e62bce-c41c-440b-b578-96762cc9c055` / `HMM_STAGE3_FBT_ROBUST_N2_TF_PEN_0p96__qe20260505`
  - `8e572f6d-d5d0-4eaf-8294-b2d97af0cad7` / `HMM_STAGE3_FBT_ROBUST_N2_TF_SYM_0p96_1p04__qe20260505`
  - `a6007665-b542-4b41-8015-52a3ea8243cd` / `HMM_STAGE3_FBT_ROBUST_N2_TF_AGG_0p95_1p08__qe20260505`
  - `c0850f0b-ea2d-487d-9aff-ca1d338c6612` / `HMM_STAGE3_TURNOVER_LIGHT_N3_UTIL_PEN_0p96__qe20260505`
- Retained visible configs:
  - `ce4952c1-4b0d-46a7-81f2-ae1d4a249555` / `HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504`
  - `b99c907b-873a-4173-a4ee-5eab266f8c49` / `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore`

## New Diagnostic Script

- Added `scripts/hmm_stage3_sparse_penalty_screen_20260505.py`.
- The script is read-only:
  - It loads retrained Stage3 HMM score panels.
  - It converts HMM scores into sparse penalty-only sector coefficient maps on top of Loop10.
  - It replays TopK enter/drop attribution versus Loop10.
  - It writes only `.codex_tmp` diagnostic outputs.
- It does not register models, write protected HMM assets, or submit QE tasks.

## Commands

```powershell
python -m py_compile scripts/hmm_stage3_sparse_penalty_screen_20260505.py
rg -n "requests\.|POST|UPDATE|DELETE|INSERT|submit|quantevolver|model_train_|backend/data/hmm_models|http://|except Exception" scripts/hmm_stage3_sparse_penalty_screen_20260505.py -S
```

```powershell
python -u scripts/hmm_stage3_sparse_penalty_screen_20260505.py `
  --output-dir .codex_tmp/hmm_stage3_sparse_penalty_smoke_20260505 `
  --max-candidates 2 `
  --bottom-pcts 0.10 `
  --penalties 0.99 0.985 `
  --confirm-modes stage3_only `
  --task-id qe_20260505_123035_bf80
```

```powershell
python -u scripts/hmm_stage3_sparse_penalty_screen_20260505.py `
  --output-dir .codex_tmp/hmm_stage3_sparse_penalty_screen_20260505 `
  --task-id qe_20260505_123035_bf80 `
  --bottom-pcts 0.05 0.10 0.15 0.20 `
  --penalties 0.995 0.99 0.985 0.98 0.96 `
  --confirm-modes stage3_only
```

```powershell
python -c "import requests,json; rows=requests.get('http://127.0.0.1:8001/api/v1/hmm-training/configs', params={'model_type':'sector_hmm'}, timeout=30).json(); print(json.dumps({'count':len(rows),'rows':[{'config_id':r.get('config_id'),'display_name':r.get('display_name'),'snapshot_count':r.get('snapshot_count')} for r in rows]}, ensure_ascii=False, indent=2))"
```

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- scripts/hmm_stage3_sparse_penalty_screen_20260505.py tests/aistock_validation/history/hmm/20260505_1625_l2_hmm-stage3-prune-and-sparse-screen.md
```

## Selector Validation

`/api/v1/hmm-training/configs?model_type=sector_hmm` returned exactly two configs:

| config_id | display_name | snapshot_count |
| --- | --- | ---: |
| `ce4952c1-4b0d-46a7-81f2-ae1d4a249555` | `HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504` | 1 |
| `b99c907b-873a-4173-a4ee-5eab266f8c49` | `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore` | 1 |

## Sparse Screen Result

- Tested 100 sparse penalty-only candidates.
- All candidates use already retrained Stage3 HMM score panels; this stage did not perform a new HMM fit.
- The most promising candidates by holdout TopK replacement metrics are:

| candidate | changed_days | avg_entered/day | net_db_ret_5d | net_db_ret_10d | net_db_ret_20d | changed positive label ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `SPARSE_turnover_light_n3_util_low_B15_PEN_0p99_stage3_only` | 32 | 0.138075 | 0.006199 | 0.021204 | 0.020643 | 0.687500 |
| `SPARSE_flow_breadth_n2_util_low_B20_PEN_0p995_stage3_only` | 30 | 0.129707 | 0.004278 | 0.019884 | 0.023098 | 0.700000 |
| `SPARSE_turnover_light_n3_util_low_B20_PEN_0p98_stage3_only` | 91 | 0.439331 | 0.000573 | 0.015239 | 0.011513 | 0.604396 |

## Interpretation

- Sparse mapping materially reduces the Stage3 failure mode:
  - Continuous Stage3 QE candidates changed too many sector/date coefficients.
  - Sparse mappings change far fewer TopK names while preserving positive 10D/20D replacement quality.
- The top two candidates have good replacement quality but only 30-32 changed days, so they are promising but still sample-limited.
- The third candidate has broader coverage, but 5D edge is weak. It is better as a stress candidate than as the first QE candidate.
- No candidate is promoted to QE yet; remote QE should only run after selecting at most two candidates plus Loop10/Loop2/No-HMM controls.

## Evidence

- Prune backup: `.codex_tmp/hmm_stage3_prune_20260505_next/before_sector_hmm_configs.json`
- Prune result: `.codex_tmp/hmm_stage3_prune_20260505_next/prune_result.json`
- Sparse screen report: `.codex_tmp/hmm_stage3_sparse_penalty_screen_20260505/qe_20260505_123035_bf80/sparse_penalty_screen_report.md`
- Sparse ranked summary: `.codex_tmp/hmm_stage3_sparse_penalty_screen_20260505/qe_20260505_123035_bf80/sparse_topk_holdout_ranked.csv`
- Candidate coefficients: `.codex_tmp/hmm_stage3_sparse_penalty_screen_20260505/qe_20260505_123035_bf80/candidate_coefficients/`
- API validation snapshot: `.codex_tmp/hmm_stage3_prune_20260505_next/api_validation_sector_hmm.json`

## Guardrails

- `py_compile`: passed.
- Read-only mutation scan: no HTTP, QE submission, model registry write, or HMM asset write path in the diagnostic script.
- `nox -s l0`: passed. Two P2 complexity findings remain as review-only notes for the bounded diagnostic grid; no P1/P0 blocking findings.

## Residual Risk

- TopK attribution is a screen, not a full QE backtest.
- Current sparse candidates use previous Stage3 retrained score panels and new sparse mappings; they are not newly retrained HMM models.
- Do not add all 100 candidates to QE. If proceeding, register only the top one or two sparse candidates and keep Loop10, Loop2, and No-HMM as controls.
