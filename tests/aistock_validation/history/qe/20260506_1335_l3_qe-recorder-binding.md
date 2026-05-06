# QE Recorder Binding Validation - 2026-05-06

## Scope

- Fixed QE result extraction so new QE loops bind `read_exp_res.py` to the recorder created by the current runner.
- Preserved legacy latest-recorder fallback when strict mode is not enabled, so old experiments remain readable.
- No Qlib source code, data warehouse module, Paper/Selection module, model asset, HMM snapshot, or QE historical workspace was modified.

## Changed Files

- `scripts/qrun_limit_minute.py`
- `scripts/qrun_limit.py`
- `backend/services/quantevolver/templates/read_exp_res.py`
- `backend/services/quantevolver/config_composer.py`
- `backend/services/quantevolver/multi_alpha_engine.py`
- `backend/services/quantevolver/multi_alpha_result_collector.py`
- `backend/tests/unified_engine/test_qe_config_truth.py`
- `docs/operations/qe_recorder_binding_contract_20260506.md`

## Business Risks Covered

- Parallel loops no longer silently extract the latest recorder from a shared `mlruns` tree when a current-loop recorder id is available.
- New generated QE commands fail fast with `QE_REQUIRE_RECORDER_ID=1` if no recorder binding exists.
- Old experiments with no binding file can still be read in legacy mode without setting strict mode.

## Local Validation

```powershell
python -m py_compile scripts/qrun_limit.py scripts/qrun_limit_minute.py backend/services/quantevolver/templates/read_exp_res.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/multi_alpha_engine.py backend/services/quantevolver/multi_alpha_result_collector.py backend/tests/unified_engine/test_qe_config_truth.py
```

Result: passed.

```powershell
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py -q
```

Result: `47 passed`.

```powershell
python -m pytest backend/tests/unified_engine/test_multi_alpha_command_generation.py -q -k "not update_group_records and not collect_distributed_graceful_without_label"
```

Result: `59 passed, 2 deselected`.

```powershell
python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH backend/services/quantevolver/config_composer.py backend/services/quantevolver/templates/read_exp_res.py backend/services/quantevolver/multi_alpha_engine.py backend/services/quantevolver/multi_alpha_result_collector.py scripts/qrun_limit.py scripts/qrun_limit_minute.py docs/operations/qe_recorder_binding_contract_20260506.md backend/tests/unified_engine/test_qe_config_truth.py
```

Result: `0 finding(s)`.

```powershell
git diff --check
```

Result: exit 0. Git emitted only the existing line-ending warning for `backend/tests/unified_engine/test_qe_config_truth.py`.

### Full Related Test Command Note

```powershell
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_multi_alpha_command_generation.py -q
```

Result: `104 passed, 2 failed`. The two failures are DB-connection environment failures in `_update_group_records` tests (`psycopg2.OperationalError: no password supplied`) and are unrelated to the recorder-binding changes. The same multi-alpha suite passes when those DB-dependent tests are deselected.

## Remote Validation

Remote host: `lc999@192.168.50.215`.

### 1. Known mismatch workspace, strict bound recorder

Source workspace: `/home/lc999/projects/RD-Agent-main/qe_workspace/qe_20260506_004257_b34a/Loop1`.

Validation copied the new `read_exp_res.py` into a temp directory, symlinked the old `mlruns`, and wrote `qe_current_recorder.json` pointing to the recorder that Loop1 actually started:

- target recorder: `210f2ea26ad24d809ad20855fd09f07d`
- previous latest-recorder extraction in old log selected a different recorder.

Command pattern:

```bash
QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py
```

Observed output:

```text
Bound recorder selected: recorder_id=210f2ea26ad24d809ad20855fd09f07d experiment=workflow source=.../qe_current_recorder.json
Latest recorder: {'class': 'Recorder', 'id': '210f2ea26ad24d809ad20855fd09f07d', ...}
BOUND_SELECTED=210f2ea26ad24d809ad20855fd09f07d
```

Result: passed. Strict binding selected the intended recorder, not the latest recorder.

### 2. Old experiment compatibility without strict mode

Same temp setup, but no `qe_current_recorder.json` and no `QE_REQUIRE_RECORDER_ID`.

Observed output:

```text
Warning: no bound recorder id found; using legacy latest-recorder fallback for old experiments
Latest recorder: {'class': 'Recorder', 'id': '6f12249c116a4b02875a2bfd48e3d672', ...}
```

`qlib_res.csv` was generated.

Result: passed. Old-style result extraction remains readable when strict mode is not enabled.

### 3. Strict mode without binding must fail

Same temp setup, no binding file, with `QE_REQUIRE_RECORDER_ID=1`.

Observed output:

```text
STRICT_MISSING_EXIT=1
ERROR: QE_REQUIRE_RECORDER_ID=1 but no QE_RECORDER_ID, qe_current_recorder.json, or qe_recorder_id.txt was found. Refusing legacy latest-recorder fallback.
```

Result: passed. No silent fallback in strict mode.

### 4. Runner binding helper validation on remote

Copied modified `qrun_limit_minute.py` and `qrun_limit.py` to remote `/tmp`, imported them in the `rdagent-gpu` conda environment, and called `_write_qe_current_recorder()` with a fake recorder.

Observed output:

```text
qrun_limit_minute.py:OK:backtest_only:abc123
qrun_limit.py:OK:full:abc123
```

Result: passed. Both runner helpers write `qe_current_recorder.json` with the expected recorder id, experiment name, mode, runner, and metadata.

## Evidence Paths

- Remote strict/legacy validation temp dir: `/home/lc999/tmp/qe_recorder_binding_validation_20260506_132954`
- Remote runner-helper validation temp dir: `/home/lc999/tmp/qe_qrun_binding_helper_20260506_133036`
- Local operation docs: `docs/operations/qe_recorder_binding_contract_20260506.md`

## Production Port Impact

- Did not restart or modify production backend port `8001`.
- Remote validation used temp files under `/tmp` and `/home/lc999/tmp`; no old QE loop workspace was modified.

## Residual Risks

- Historical experiments that already used legacy latest-recorder fallback may still contain incorrect persisted metrics. They need mismatch scanning and/or re-extraction with explicit recorder id to be certified.
- New correctness depends on all QE runners using the updated `qrun_limit*.py` and generated commands with `QE_REQUIRE_RECORDER_ID=1`.
- Data warehouse and Paper/Selection modules still need to consume recorder trust metadata; this change only documents the contract and updates QE, per scope restriction.
