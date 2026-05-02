# QE Generation/Retry/Clone Worker-Path Remediation - L2

Date: 2026-05-02
Scope: QE experiment generation/regeneration, retry, custom_evo rerun/append/clone, cross-node backtest-only model reuse, HMM coefficient handling, stock-pool loop payload delivery, and local QE program/experiment roots.

## Business Goal

Windows-side AIstock/FastAPI must treat WSL/RD-Agent/QE worker filesystems as remote Linux nodes. Generation, retry, clone, and cross-node submission flows may create AIstock-owned local artifacts and send payloads through QE/node APIs, but must not read, scan, copy, mutate, or delete worker workspace directories directly.

## Changed Behavior Verified

- Removed `QE_WORKSPACE_WIN` usage from QE config generation code paths; generated commands may still contain node-side Linux paths, but Windows does not dereference them.
- Benchmark/helper/template files are packaged from AIstock-local code/artifact roots and delivered through API/payload flows.
- `RDAGENT_FACTOR_TEMPLATE_WIN` no longer defaults to a worker workspace; Linux/WSL values are rejected before dependency reads.
- `QE_EXPERIMENTS_ROOT`, `QE_PROGRAMS_WIN`, and `FACTOR_CACHE_ROOT_WIN` are guarded AIstock-owned roots; legacy worker values are ignored or rejected before local file operations.
- `read_exp_res.py` is available as a bundled AIstock template so generation no longer depends on an RD-Agent-main `qe_programs` checkout.
- HMM-enabled generation/retry/clone uses precomputed local artifacts or node-sourced loop artifacts; missing coefficients fail fast instead of invoking WSL.
- Cross-node backtest-only model reuse remains API-based through `download_mlruns_params` and loop payload injection.

## Production Safety

- Did not restart production FastAPI/backend/API on port `8001`.
- Did not restart QE/RD-Agent node services on port `9000` or compute-node APIs.
- Did not create, retry, delete, or mutate real running QE experiments.
- Validation used unit/integration tests and static scans only; no production task assets were modified.

## Commands And Results

```powershell
python -m py_compile backend/services/strategy_package/workspace_policy.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/templates/read_exp_res.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_multi_alpha_command_generation.py
# passed

python -m pytest backend/tests/unified_engine/test_qe_config_truth.py -q
# 29 passed in 16.45s

python -m pytest backend/tests/unified_engine/test_multi_alpha_command_generation.py -q
# 59 passed in 15.49s

python -m pytest backend/tests/unified_engine/test_backtest_executor.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q
# 26 passed in 9.33s

python -m pytest backend/tests/unified_engine/test_qe_evolution_read_paths.py backend/tests/unified_engine/test_qe_experiment_read_paths.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_stop_task.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_backtest_executor.py backend/tests/unified_engine/test_multi_alpha_command_generation.py backend/tests/strategy_package/test_model_asset_resolver.py -q
# 171 passed in 16.85s

Select-String -Path backend/services/quantevolver/config_composer.py -Pattern 'QE_WORKSPACE_WIN','QE_WORKSPACE_WIN.parent','subprocess.run','["wsl"','WSL HMM','model_path 自动转换' -SimpleMatch
# no matches

python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH backend/services/quantevolver/config_composer.py backend/services/strategy_package/workspace_policy.py backend/services/quantevolver/templates/read_exp_res.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_multi_alpha_command_generation.py tests/aistock_validation/modules/qe.md
# Guardrail scan completed with 0 finding(s).

git diff --check -- backend/services/quantevolver/config_composer.py backend/services/strategy_package/workspace_policy.py backend/services/quantevolver/templates/read_exp_res.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_multi_alpha_command_generation.py tests/aistock_validation/modules/qe.md
# passed; CRLF warnings only
```

## Bugs Found During Validation

- Import-time failure: current `.env` still contains legacy `QE_PROGRAMS_WIN` pointing at an RD-Agent-main `qe_programs` directory. Strict root validation initially made `config_composer` fail during test collection. Fixed by ignoring invalid legacy env roots with a warning and using guarded AIstock defaults plus the bundled `read_exp_res.py` template.
- Guardrail noise: test fixtures contained hardcoded workstation Linux-mount literals. Replaced them with constructed worker-path fixtures or computed expected paths; guardrail scan now reports zero findings.

## Business Oracles

- QE creation/retry/clone code paths fail fast on missing HMM coefficients and forbidden worker paths; they do not fake successful metrics or silently use neutral HMM coefficients.
- Stock-pool delivery for execution remains observable through loop payload files and node-side install commands, not through Windows SSH/WSL directory writes.
- Local cleanup/read policy still refuses worker workspaces while allowing AIstock-owned local artifact caches.

## Residual Risk

- Running production services will continue using the old loaded Python code until the user later schedules a safe restart/reload. No restart was performed in this validation.
- This validation intentionally did not run a real QE create/retry/clone task because active QE experiments are running and the user prohibited production service disruption.
