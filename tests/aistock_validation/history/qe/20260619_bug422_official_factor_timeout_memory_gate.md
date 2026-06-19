# BUG-422 官方全量因子计算 timeout 与内存门禁验证记录

日期：2026-06-19

## 背景

- 全量官方因子独立指标计算在 WSL batch `b0010` 后长时间无新事件，`timeout_per_factor=1800` 未生效。
- 现场证据显示 WSL 进程 `VmSwap` 约 3.9GB，违反官方离线因子计算设计中的 timeout、RSS/swap hard gate 和结构化失败分类要求。

## 修复范围

- `backend/services/quantevolver/official_factor_batch_compute_service.py`
  - WSL/Linux 下每个因子使用 fork 子进程执行，父进程按 `timeout_per_factor` 强制终止超时因子。
  - 增加 `factor_started`、`factor_timeout`、`resource_gate_warning`、`resource_gate_failed` 事件。
  - 增加 RSS/PSS/USS、swap growth、available memory 门禁；hard gate 触发时剩余因子结构化标记为 `memory_gate_failed`。
  - runtime validation 增加 timeout/resource gate 检查、阈值和失败摘要。
- `backend/tests/quantevolver/test_official_factor_batch_compute.py`
  - 覆盖基础数据只读缓存、result.h5 捕获、线程 fallback、WSL fork timeout、resource gate 分类。
- `backend/tests/quantevolver/test_official_runtime_validation.py`
  - 覆盖 timeout/resource gate runtime validation 字段和失败分类。

## 验证命令

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/quantevolver/official_factor_batch_compute_service.py backend/tests/quantevolver/test_official_factor_batch_compute.py backend/tests/quantevolver/test_official_runtime_validation.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/quantevolver/test_official_factor_batch_compute.py backend/tests/quantevolver/test_official_runtime_validation.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/Scripts/ruff.exe check backend/services/quantevolver/official_factor_batch_compute_service.py backend/tests/quantevolver/test_official_factor_batch_compute.py backend/tests/quantevolver/test_official_runtime_validation.py
git diff --check
```

## WSL timeout smoke

```powershell
wsl.exe -e bash -lc "set -eo pipefail; cd /mnt/f/Dev/AIstock_worktrees/BUG-422-wsl-20260619; source /home/lc999/miniconda3/etc/profile.d/conda.sh; conda activate rdagent-gpu; python <inline timeout smoke>"
```

结果摘要：

```json
{"wall": 1.095, "error_type": "factor_timeout", "elapsed_sec": 1.079, "events": ["factor_started", "factor_timeout"]}
```

## 当前结论

- BUG-422 修复后，WSL/Linux 官方因子 batch 不再因单个因子长时间卡死；超时因子会被强制终止并分类为 `factor_timeout`。
- 内存/swap hard gate 会在 batch 前、batch 中和 batch 后检查；触发后任务停止继续拉起新因子，剩余因子分类为 `memory_gate_failed`。
- 本修复不重启生产后端、前端或 TDX，不包含 DB DDL。
