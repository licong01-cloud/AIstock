# HMM Dynamic Candidates DB Registration Report

Created: 2026-04-29

## Scope

- User request: register both dynamic HMM candidates into DB and keep only one existing DB HMM version as baseline.
- No QE experiment was started.
- No backend/frontend runtime code was changed.

## Final DB HMM Set

```text
+------+---------------------------------------------------------+--------------------------------------+--------------------+
| Role | Display Name                                            | Config ID                            | Preset             |
+------+---------------------------------------------------------+--------------------------------------+--------------------+
| Base | HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore     | b99c907b-873a-4173-a4ee-5eab266f8c49 | preset_A,preset_B  |
| NEW1 | HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag        | 442fd70a-47b5-41ca-b4f5-96f52b81742e | preset_A           |
| NEW2 | HMM_DYNAMIC_PUP_w20_50_conf_0p10_PIT1Y__n3_diag         | f3fe9433-ea86-4a16-a44b-989e1398c1b2 | preset_A           |
+------+---------------------------------------------------------+--------------------------------------+--------------------+
```

## Added Snapshots

```text
+------+--------------------------------------+--------------------------------------+-------------------------------+
| Role | Snapshot ID                          | Train / Validation                   | Coefficients                  |
+------+--------------------------------------+--------------------------------------+-------------------------------+
| NEW1 | ecd2bc1f-5b1b-4057-8815-c5590ab26804 | 2021-01-04~2024-11-29 / 2024-12-02~2025-03-10 | 2025-03-11~2026-03-03 |
| NEW2 | daddcd16-a618-4d5b-8919-dd61fd4e5eca | 2021-01-04~2024-11-29 / 2024-12-02~2025-03-10 | 2025-03-11~2026-03-03 |
+------+--------------------------------------+--------------------------------------+-------------------------------+
```

## Deleted Old DB Versions

```text
+------+---------------------------------------------------------+--------------------------------------+
| No.  | Deleted Version                                         | Deleted Config ID                    |
+------+---------------------------------------------------------+--------------------------------------+
| 1    | HMM_BASELINE_ORIGINAL_w3_raw_unfixed__n3_diag_rw3_nozscore | 564b407f-1541-4b18-a087-2a45cfbca9d9 |
| 2    | HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore         | c095ab83-48f4-453d-9eb9-c1987b6bd7fe |
| 3    | HMM_HORIZON_V2_w5w10w20_oos6m__n3_diag_ms75_no_limitup  | f1da5529-0109-495f-a2b8-a2033cc31ee8 |
+------+---------------------------------------------------------+--------------------------------------+
```

The corresponding model asset directories under `backend/data/hmm_models/` were also removed.

## Post-registration 1Y Script Check

```text
+------+---------------------------------------------------------+---------+---------+----------+----------+
| Rank | Version                                                 | Source  | Total   | Sharpe   | MaxDD    |
+------+---------------------------------------------------------+---------+---------+----------+----------+
| 1    | HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag        | DB      | -0.81%  | 0.142    | -30.91%  |
| 2    | HMM_DYNAMIC_PUP_w20_50_conf_0p10_PIT1Y__n3_diag         | DB      | -0.95%  | 0.138    | -30.91%  |
| 3    | HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore:B   | DB      | -12.89% | -0.440   | -29.44%  |
| 4    | HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore:A   | DB      | -18.51% | -0.682   | -32.58%  |
| 5    | NO_HMM_BASELINE                                         | raw     | -21.00% | -0.628   | -37.34%  |
+------+---------------------------------------------------------+---------+---------+----------+----------+
```

The newly registered DB artifacts reproduce the offline dynamic results exactly.

## Important Usage Note

The two dynamic candidates are registered with `preset_A` as the runtime preset name so existing QE/Paper HMM runtime can locate:

`coefficients_preset_A_2025-03-11_2026-03-03.json`

If a future QE experiment uses a different backtest date window, a matching coefficient artifact must be generated first. The current artifact is ready for the 2025-03-11 to 2026-03-03 validation window.

## Evidence

- Registration script: `scripts/register_dynamic_hmm_candidates.py`
- Post-registration check: `.codex_tmp/hmm_db_after_registration_1y_20260429/summary.csv`
- Post-registration report: `docs/analysis/hmm_db_after_registration_1y_report_20260429.md`
- NEW1 model: `backend/data/hmm_models/442fd70a-47b5-41ca-b4f5-96f52b81742e/2026-04-29/models.json`
- NEW1 coefficients: `backend/data/hmm_models/442fd70a-47b5-41ca-b4f5-96f52b81742e/2026-04-29/coefficients_preset_A_2025-03-11_2026-03-03.json`
- NEW2 model: `backend/data/hmm_models/f3fe9433-ea86-4a16-a44b-989e1398c1b2/2026-04-29/models.json`
- NEW2 coefficients: `backend/data/hmm_models/f3fe9433-ea86-4a16-a44b-989e1398c1b2/2026-04-29/coefficients_preset_A_2025-03-11_2026-03-03.json`
