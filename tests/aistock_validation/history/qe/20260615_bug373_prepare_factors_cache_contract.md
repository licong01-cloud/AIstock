# BUG-373 QE prepare_factors official cache-hit contract 验证

日期：2026-06-15
模块：quantevolver / QE factor cache

## 目标

QE 回测生成的 `prepare_factors.py` 必须与官方离线因子缓存契约保持一致，避免独立指标、相关性、QE 回测三条链路各自维护 cache-hit 判断。BUG-373 将 QE `prepare_factors.py` 的命中判断收敛为 `official_factor_cache_hit_validation_v1` 结构化契约。

## 已落地行为

- 生成脚本新增 `_validate_official_cache_hit_contract()`，输出 `schema_version=official_factor_cache_hit_validation_v1`。
- miss reason 分类包含：`missing_from_cache`、`missing_meta`、`as_of_date_mismatch`、`window_not_covered`、`universe_mismatch`、`index_policy_mismatch`、`hash_mismatch`、`schema_invalid`。
- `_try_cache_hit()` 只在 `official_cache_hit=True` 时读取 single parquet；否则日志输出 contract gate 和分类原因。
- `FactorValueLoader.validate_official_cache_window_hit()` 同步支持 `expected_code_hashes`，用于 QE 子窗口命中验证时识别 hash mismatch。
- 对 `factor_values_realtime` 仍 fail-fast，QE 回测不得使用该目录。

## 本地验证

- `python -m pytest -q backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py backend/tests/quantevolver/test_official_runtime_validation.py backend/tests/test_factor_cache_wsl_env.py backend/tests/unified_engine/test_multi_alpha_command_generation.py` -> `89 passed`
- `python -m py_compile backend/services/quantevolver/config_composer.py backend/services/quantevolver/factor_value_loader.py backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py backend/tests/quantevolver/test_official_runtime_validation.py` -> passed
- `python -m ruff check backend/services/quantevolver/config_composer.py backend/services/quantevolver/factor_value_loader.py backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py backend/tests/quantevolver/test_official_runtime_validation.py` -> passed
- `git diff --check` -> passed

## 生产影响

- `production_ddl_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- 未重启生产 backend/frontend/TDX，未写生产 DB DDL。
