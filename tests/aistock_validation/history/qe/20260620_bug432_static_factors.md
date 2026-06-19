# BUG-432 static_factors 官方离线因子计算修复验证

## 范围

- BUG: `BUG-432` / GitHub Issue `#1328`
- 模块: `quantevolver`
- 目标: 官方全量因子独立指标计算只使用 WSL + 回测离线 `bin/h5/parquet` 基础数据缓存，不回退 DB snapshot 或 `factor_values_realtime`；修复 12 个依赖 `static_factors.parquet` 存在性检查的因子运行失败。

## 修复点

- `OfflineCodeTextFactorExecutor` 在每个因子的临时执行目录中创建已加载基础数据文件名的轻量 marker，仅用于兼容历史 code_text 中的 `os.path.exists(...)` / `os.path.isfile(...)` 检查。
- `OfflineCodeTextFactorExecutor` 通过受控 `os` proxy 兼容 `import os`、`import os.path`、`from os.path import isfile` 等存在性判断。
- `pd.read_hdf` / `pd.read_parquet` 仍继续重定向到 `BacktestBaseDataMemoryCache.get(...)`，实际因子基础数据读取仍来自一次性内存缓存。

## 验证命令与结果

- `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/quantevolver/offline_code_text_factor_executor.py backend/tests/quantevolver/test_official_factor_batch_compute.py` -> passed
- `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/quantevolver/test_official_factor_batch_compute.py -q -p no:cacheprovider` -> `15 passed, 1 skipped`
- `C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` -> `findings=0, blocking=0`
- `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m ruff check ...` -> not executed in Windows AIstock env because `ruff` module is not installed; final nox gates cover repository standard checks.
- `$env:QE_READ_L3_SKIP_UI='1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_read_l3` -> passed (`qe_read_l3` success, `qe_read_backend` 14 passed; UI intentionally skipped for this backend executor BUG).
- WSL smoke command:

```powershell
wsl.exe -e bash -lc "set -o pipefail; cd /mnt/f/Dev/AIstock_worktrees/BUG-432-static-factors-12-20260620; set -a; source <(tr -d '\r' < /mnt/f/Dev/AIstock/.env); set +a; source /home/lc999/miniconda3/etc/profile.d/conda.sh; conda activate rdagent-gpu; mkdir -p tmp/issue_workflow/BUG-432; python -u backend/scripts/run_official_factor_full_compute_wsl.py tmp/issue_workflow/BUG-432/failed12_payload.json 2>&1 | tee tmp/issue_workflow/BUG-432/failed12_smoke.log"
```

Result: `success=true`, `runtime_validation.gate_status=passed`, `success_count=12`, `fail_count=0`, `failed_factors=[]`.

## WSL 业务验证要点

- 运行环境: WSL `rdagent-gpu`。
- 数据窗口: `2018-08-01` ~ `2026-04-30`。
- 基础数据来源: `/mnt/f/dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data`。
- `base_cache.base_data_cache_policy`: `load_once_readonly`。
- `static_factors.parquet`: 已在 `base_cache_loaded` 中加载，`read_count=1`。
- 12 个历史失败因子全部完成 factor value + 独立指标计算，未出现 `static_factors.parquet not found`。
- runtime validation checks 中 `official_cache_only=true`、`code_text_source=true`、`resource_gate_ok=true`、`window_declared=true`。
- 验证日志: `tmp/issue_workflow/BUG-432/failed12_smoke.log`。

## 生产影响

- 未重启 backend/frontend/TDX。
- 无 DB DDL。
- WSL smoke 会按官方独立指标流程写入 12 个因子的指标结果和本 worktree 下的临时 factor cache，用于 BUG 修复验证。
