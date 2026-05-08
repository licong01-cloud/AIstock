# QE Backtest-Only Recorder Isolation Validation - 2026-05-08

- Task/branch: Agent A / codex/qe-backtest-recorder-isolation-20260508
- Commit: uncommitted, awaiting integration validation
- Modified files: scripts/qrun_limit.py; scripts/qrun_limit_minute.py; backend/services/quantevolver/qe_evolution_service.py; backend/services/quantevolver/qe_workspace_client.py; backend/tests/unified_engine/test_qe_config_truth.py; backend/tests/unified_engine/test_qrun_recorder_isolation.py
- Production 8001 touched: no
- Protected assets touched: no StrategyPackage frozen manifests, model weights, HMM snapshots, QE/RD-Agent artifacts, Paper ledger, or validated policies modified
- DB writes: none

## Source/Target mlruns Summary

- Backtest-only source params are loaded from `source_model` / `QE_BACKTEST_SOURCE_PARAMS_DIR`, not from target `LoopX/mlruns`.
- If RD-Agent extracts packaged source `mlruns` into `LoopX/mlruns`, the runner relocates it to `LoopX/source_model/mlruns` before qlib initializes.
- Target `LoopX/mlruns` is recreated as a loop-local non-symlink directory and becomes `MLFLOW_TRACKING_URI`.
- The isolation gate rejects target symlinks, source/target realpath equality, and target-under-source nesting.
- Pickle loads used by pred-backtest and backtest-only params are size-bounded before unpickle to satisfy `MEMORY-DATAFRAME-001`; defaults are 2 GiB and can only be raised with explicit byte-limit env vars.
- Retry AUTO/FULL fallback cannot catch and hide a failed `qe_recorder_isolation.json` check for original backtest-only loops.
- A final target `mlruns` symlink/reparse/realpath check runs before `qlib.init` and again before `R.start`.

## Test Commands And Results

```powershell
python -m py_compile scripts/qrun_limit.py scripts/qrun_limit_minute.py backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/qe_workspace_client.py
# result: passed

pytest backend/tests/unified_engine/test_qrun_recorder_isolation.py backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider -k "backtest_only or recorder or retry_requires_isolation or retry_accepts_isolation"
# result after PM review fixes: 12 passed, 45 deselected in 7.13s

pytest backend/tests -q -p no:cacheprovider -k "backtest_only or recorder or qrun"
# result after PM review fixes: 18 passed, 877 deselected in 9.20s

git diff --check
# result: passed; only CRLF normalization warnings for existing Git attributes/platform behavior

python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1 --fail-new-only --baseline-json tmp/validation/guardrails/baseline_20260504.json
# result: passed; P1 blocking=0, P2 complexity review findings only
```

## Residual Risk

- Same-node legacy RD-Agent still contains symlink code when AIstock sends non-cross-node `model_source`; this branch avoids it by always packaging source params for backtest-only paths it owns. External RD-Agent should still be updated separately to reject symlink target `mlruns` at API level.
