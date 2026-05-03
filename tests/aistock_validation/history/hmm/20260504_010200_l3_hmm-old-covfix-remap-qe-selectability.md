# HMM old-covfix remap QE selectability - 2026-05-04

## Scope

- Module: HMM registry / QE HMM selection / precomputed coefficient artifacts.
- Level: L3 DB + API + QE config-composer smoke.
- Goal: register 5 old-covfix coefficient remap candidates so QE experiments can select them directly, without retraining or overwriting existing HMM assets.
- Protected asset policy: old covfix baseline assets were read-only sources; new UUID model directories were created under `backend/data/hmm_models/*/2026-05-04/`.

## Registered Candidates

```text
Display Name                                           Config ID                             Snapshot ID
-----------------------------------------------------  ------------------------------------  ------------------------------------
HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504 ce4952c1-4b0d-46a7-81f2-ae1d4a249555  6ea64754-003d-48d8-ad9e-d0e7857716c8
HMM_TEST_old_covfix_boost_only_p105__qe20260504        82a40d27-0e96-48a1-882a-4d182a58b931  377a8447-ee26-44a8-8ead-7338f525e0f2
HMM_TEST_old_covfix_penalty094_boost103__qe20260504    22d53160-7195-4e69-86ec-76c19c615a69  5a8ce90e-50bb-4fbd-8cd8-e3b95c9dffa0
HMM_TEST_old_covfix_penalty095_boost104__qe20260504    ea0db9d3-69bf-489e-aa55-c74b6340e68d  afa6acd9-f766-4394-970e-451d1a39bb06
HMM_TEST_old_covfix_penalty095_boost106__qe20260504    518ddf2d-e4a0-4bf0-8572-7cea429e27d5  8ddb5d29-8097-4aef-b110-f2f94f54ca4b
```

## Commands

```powershell
$env:TDX_DB_PASSWORD='***'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/register_hmm_remap_qe_candidates_20260504.py --dry-run
```

```powershell
$env:TDX_DB_PASSWORD='***'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/register_hmm_remap_qe_candidates_20260504.py
```

```powershell
# DB + HMMTrainingService + ConfigComposer validation
$env:TDX_DB_PASSWORD='***'
@'
# Inline validation loaded the 5 rows, checked files/coverage/maps/coeff values,
# called HMMTrainingService.list_configs/get_snapshot, and called
# ConfigComposer._resolve_hmm_coefficients_json for each snapshot.
'@ | C:/Users/lc999/miniconda3/envs/AIstock/python.exe -
```

```powershell
Invoke-RestMethod -Uri 'http://127.0.0.1:8001/api/v1/hmm-training/configs?model_type=sector_hmm' -TimeoutSec 5
Invoke-RestMethod -Uri 'http://127.0.0.1:8001/api/v1/hmm-training/configs/<config_id>/snapshots' -TimeoutSec 5
```

```powershell
$env:TDX_DB_PASSWORD='***'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/unified_engine/test_qe_config_truth.py -k hmm -q
```

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile scripts/register_hmm_remap_qe_candidates_20260504.py
rg -n "lc78080808|TDX_DB_PASSWORD='lc|password=.*lc|postgresql://" scripts/register_hmm_remap_qe_candidates_20260504.py docs/analysis/hmm_training_current_status_20260503.md docs/analysis/hmm_sector_factor_stacking_next_step_20260504.md tests/aistock_validation/history/hmm/20260504_010200_l3_hmm-old-covfix-remap-qe-selectability.md
```

## Results

- Registration dry-run passed and rolled back, then real registration completed.
- DB query found 9 `sector_hmm` selectable configs after registration: old covfix, 3 previous 2026-05-02 candidates, and 5 new 2026-05-04 remap candidates.
- All 5 new rows have `model_type='sector_hmm'`, one `completed` snapshot, `sector_count=131`, and one completed job row.
- All 5 new model paths exist and all coefficient artifacts exist with filename `coefficients_preset_A_2024-07-01_2026-04-27.json`.
- Coefficient artifact coverage: 442 trade dates, includes `2024-07-01` and `2026-04-27`, 131 sectors on first/last date, `stock_sector_map=5847`.
- `HMMTrainingService.list_configs('sector_hmm')` returned the 5 new display names; `get_snapshot(snapshot_id)` returned the expected model paths.
- `ConfigComposer._resolve_hmm_coefficients_json` resolved each new snapshot from its local artifact without fallback.
- API on existing production port `8001` returned all 9 sector_hmm configs and all 5 new snapshots; the backend was not restarted.
- Targeted pytest passed: `3 passed, 30 deselected in 12.20s`.
- Registration script `py_compile` passed; secret scan returned no local DB password literals in committed files.

## Coefficient Sanity

```text
Candidate                                             Unique Coefficients     Counts
----------------------------------------------------  ----------------------  ----------------------------------------
penalty_only_f096_b000                                0.96, 1.00              0.96=23867, 1.00=34035
boost_only_p105                                       1.00, 1.05              1.00=55450, 1.05=2452
penalty094_boost103                                   0.94, 1.00, 1.03        0.94=23867, 1.00=31583, 1.03=2452
penalty095_boost104                                   0.95, 1.00, 1.04        0.95=23867, 1.00=31583, 1.04=2452
penalty095_boost106                                   0.95, 1.00, 1.06        0.95=23867, 1.00=31583, 1.06=2452
```

## Evidence

- Registration script: `scripts/register_hmm_remap_qe_candidates_20260504.py`.
- Registration result: `.codex_tmp/hmm_registry_updates/hmm_remap_registry_result_20260504_005555.json`.
- DB backup before registration: `.codex_tmp/hmm_registry_updates/hmm_remap_registry_before_20260504_005555.json`.
- Status doc updated: `docs/analysis/hmm_training_current_status_20260503.md`.

## Residual Risk

- Selectability is verified, but QE performance is not yet proven for the 5 remap candidates; each candidate still needs a full QE shadow-loop comparison against no-HMM and old covfix.
- The model artifacts live under ignored `backend/data/hmm_models`; DB registry plus local asset backup are required to reproduce the selectable state on another machine.
- Sector-factor stacking remains a hypothesis and should be tested only after the best old-covfix/remap candidate is selected by QE results.
