# Strict Default Dynamic HMM Training Registration

## Scope

- Train two strict no-leak dynamic PUP HMM versions for QE default window.
- Register them as completed `sector_hmm` DB configs/snapshots.
- Do not start a QE experiment.

## Strict Split

```text
Train:      2021-01-04 ~ 2024-02-29
Validation: 2024-03-01 ~ 2024-05-30
20D label end: 2024-06-28
QE test:    2024-07-01 ~ 2026-03-10
Backtest:   2024-07-01 ~ 2026-03-03
```

## Registered

```text
S1 config_id=8ef81e6b-263d-4acd-93ff-4a20526b2d13 snapshot_id=c1c81aa0-aae2-4942-881c-4baafbd2f160
S2 config_id=5a3183b6-39bc-45dd-8b3d-d2027c476e62 snapshot_id=d11dc38e-84f0-4e5c-80e7-42cb5d978d40
```

## Verification

```text
DB sector_hmm count: 3
Old PIT1Y dynamic version count: 0
S1/S2 coefficient coverage: 404 days, 2024-07-01~2026-03-03
S1/S2 stock_sector_map: 5621 stocks, as_of=2024-07-01
S1/S2 DB config flags: precomputed_only=true, runtime_generation_supported=false
ConfigComposer default split hit: passed
py_compile: passed
Strict non-default HMM window fail-fast guard: passed
pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_multi_alpha_command_generation.py -q: 74 passed
```

## Evidence

- Report: `docs/analysis/hmm_strict_default_training_registration_report_20260429.md`
- Output root: `.codex_tmp/hmm_dynamic_strict_default_20260429`
