# HMM Dynamic Candidates DB Registration - 2026-04-29

## Scope

- Module: HMM DB assets.
- Level: L2 DB/file/result validation.
- User decision: add both dynamic candidates to DB; keep only one previous DB version as baseline.

## Commands

```powershell
wsl -d Ubuntu -- bash -lc 'source ~/miniconda3/etc/profile.d/conda.sh 2>/dev/null || true; conda activate rdagent-gpu 2>/dev/null || true; cd /mnt/f/Dev/AIstock && python -m py_compile scripts/register_dynamic_hmm_candidates.py && export TDX_DB_PASSWORD=*** && python scripts/register_dynamic_hmm_candidates.py'
```

```powershell
wsl -d Ubuntu -- bash -lc 'source ~/miniconda3/etc/profile.d/conda.sh 2>/dev/null || true; conda activate rdagent-gpu 2>/dev/null || true; cd /mnt/f/Dev/AIstock && export TDX_DB_PASSWORD=*** && python scripts/hmm_db_vs_dynamic_1y_compare.py --output-root /mnt/f/Dev/AIstock/.codex_tmp/hmm_db_after_registration_1y_20260429 --docs-report /mnt/f/Dev/AIstock/docs/analysis/hmm_db_after_registration_1y_report_20260429.md'
```

## DB Result

```text
+------+---------------------------------------------------------+--------------------------------------+--------------------------------------+
| Role | Display Name                                            | Config ID                            | Snapshot ID                          |
+------+---------------------------------------------------------+--------------------------------------+--------------------------------------+
| Base | HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore     | b99c907b-873a-4173-a4ee-5eab266f8c49 | bbec3863-fb67-445f-938e-66f092d18696 |
| NEW1 | HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag        | 442fd70a-47b5-41ca-b4f5-96f52b81742e | ecd2bc1f-5b1b-4057-8815-c5590ab26804 |
| NEW2 | HMM_DYNAMIC_PUP_w20_50_conf_0p10_PIT1Y__n3_diag         | f3fe9433-ea86-4a16-a44b-989e1398c1b2 | daddcd16-a618-4d5b-8919-dd61fd4e5eca |
+------+---------------------------------------------------------+--------------------------------------+--------------------------------------+
```

## Deleted Old Versions

```text
+------+---------------------------------------------------------+--------------------------------------+
| No.  | Deleted Version                                         | Config ID                            |
+------+---------------------------------------------------------+--------------------------------------+
| 1    | HMM_BASELINE_ORIGINAL_w3_raw_unfixed__n3_diag_rw3_nozscore | 564b407f-1541-4b18-a087-2a45cfbca9d9 |
| 2    | HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore         | c095ab83-48f4-453d-9eb9-c1987b6bd7fe |
| 3    | HMM_HORIZON_V2_w5w10w20_oos6m__n3_diag_ms75_no_limitup  | f1da5529-0109-495f-a2b8-a2033cc31ee8 |
+------+---------------------------------------------------------+--------------------------------------+
```

## Validation

- DB has exactly 3 `sector_hmm` configs after registration.
- Each new model path exists under `backend/data/hmm_models`.
- Each new coefficient artifact is named `coefficients_preset_A_2025-03-11_2026-03-03.json`.
- Each new coefficient artifact contains `preset_key=preset_A`, full date coverage, and `stock_sector_map`.
- Post-registration script comparison confirmed DB NEW1 and DB NEW2 match the prior offline dynamic results.

## Evidence

- Registration report: `docs/analysis/hmm_dynamic_db_registration_report_20260429.md`.
- Post-registration script report: `docs/analysis/hmm_db_after_registration_1y_report_20260429.md`.
- Post-registration summary CSV: `.codex_tmp/hmm_db_after_registration_1y_20260429/summary.csv`.

## Residual Risk

- The dynamic DB artifacts currently cover `2025-03-11 ~ 2026-03-03`; QE experiments with other windows need matching dynamic coefficient artifacts before submission.
