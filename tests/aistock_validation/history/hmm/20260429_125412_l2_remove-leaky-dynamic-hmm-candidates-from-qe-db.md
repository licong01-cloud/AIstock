# Remove Leaky Dynamic HMM Candidates From QE DB

## Scope

- Remove `HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag` and `HMM_DYNAMIC_PUP_w20_50_conf_0p10_PIT1Y__n3_diag` from QE-selectable HMM database state.
- Remove their model asset directories from `backend/data/hmm_models`.
- Keep the existing baseline HMM only.

## Reason

NEW1/NEW2 were trained/validated with dates later than the QE default backtest start (`2024-07-01`). Keeping them selectable in DB could cause QE backtests to use future-leaked model parameters, making the backtest conclusion invalid.

## Deleted DB Records

```text
config_id: 442fd70a-47b5-41ca-b4f5-96f52b81742e
snapshot_id: ecd2bc1f-5b1b-4057-8815-c5590ab26804

def config_id: f3fe9433-ea86-4a16-a44b-989e1398c1b2
snapshot_id: daddcd16-a618-4d5b-8919-dd61fd4e5eca
```

## Deleted Asset Directories

```text
backend/data/hmm_models/442fd70a-47b5-41ca-b4f5-96f52b81742e
backend/data/hmm_models/f3fe9433-ea86-4a16-a44b-989e1398c1b2
```

## Verification

```text
DB sector_hmm count: 1
Remaining display_name: HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore
Remaining asset dir: backend/data/hmm_models/b99c907b-873a-4173-a4ee-5eab266f8c49
py_compile: passed
```

## Residual Risk

- The remaining baseline HMM is only the current retained baseline; strict no-leak status for any future QE default-window conclusion must be confirmed separately or replaced by newly trained strict HMM versions.
- Dynamic HMM runtime support code remains because strict dynamic HMM versions will need it; no leaky NEW1/NEW2 DB records remain selectable.
