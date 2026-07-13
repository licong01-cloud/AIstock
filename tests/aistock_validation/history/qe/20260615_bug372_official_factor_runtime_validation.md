# BUG-372 官方离线因子 WSL 运行时验收门禁

日期：2026-06-15
模块：quantevolver / official factor cache

## 目标

BUG-371 已经把官方离线因子计算主链路切换为 WSL、code_text、回测底层数据内存缓存和 single cache。BUG-372 补齐可重复验收证据结构：WSL runner 执行后，结果必须自带 `runtime_validation`，用于判断 2 因子 smoke、16 因子 batch、全量 enabled factors、相关性全量和 QE 子窗口缓存命中是否满足设计 G10/G12。

## 已落地门禁

- `OfficialFactorBatchComputeService.compute()` 返回 `runtime_validation.schema_version=official_factor_runtime_validation_v1`。
- 因子计算验收覆盖：WSL 已进入、禁止 realtime cache、code_text source、请求/成功/失败/跳过计数一致、expected_factor_count、metrics context、metrics write、batch release、single cache release、universe metadata、失败分类。
- `FactorValueLoader.validate_official_cache_window_hit()` 用于 QE 子窗口命中检查，校验 single parquet、`_meta.json`、as_of_date、日期窗口、universe、index policy，并分类返回 cache miss 原因。
- 相关性计算返回 `runtime_validation.schema_version=official_factor_correlation_runtime_validation_v1`，校验 official cache、计数一致、excluded 分类、成功因子数、cache integrity 可见和 universe metadata。

## 推荐 WSL 实跑验收顺序

1. 2 因子 smoke：payload 设置 `factor_names` 为 2 个 enabled 因子，`validation_mode=smoke_2`。
2. 16 因子 batch：payload 设置 16 个 enabled 因子，`validation_mode=batch_16`。
3. 全量 enabled factors：payload 不传 `factor_names`，设置 `validation_mode=full_enabled` 与 `expected_factor_count` 为当前未禁用/可离线计算因子数。
4. 相关性全量：使用同一份 `rdagent_assets/factor_values/single` 触发 `run_correlation_compute_wsl.py`，检查 `runtime_validation.gate_status` 和 excluded 分类。
5. QE 子窗口：在回测配置读取 official cache 前调用 `FactorValueLoader.validate_official_cache_window_hit()`，子窗口必须被 full cache 覆盖。

## 本地单元验证

- `python -m pytest -q backend/tests/quantevolver/test_official_runtime_validation.py backend/tests/quantevolver/test_official_factor_batch_compute.py backend/tests/test_correlation_compute_independence.py` -> `16 passed`

## 生产影响

- `production_ddl_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- 未重启生产 backend/frontend/TDX，未写生产 DB DDL。
