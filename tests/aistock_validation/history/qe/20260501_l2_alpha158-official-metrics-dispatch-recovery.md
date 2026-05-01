# QE Alpha158 official metrics dispatch recovery validation

- Date: 2026-05-01
- Module: QuantEvolver factor official evaluation / dispatch
- Level: L2 backend workflow regression
- Changed files: `backend/services/dispatch_service.py`, `backend/services/quantevolver/factor_official_evaluation_service.py`

## Business Risk

The factor library UI reported `指标获取失败：节点不可达，连续 3 次同步失败` while the WSL official-evaluation runner kept computing and writing metrics. This produced a false failed result for Alpha158 independent metrics and could make users rerun or distrust completed factor metrics.

## Root Cause

`DispatchService.sync_running_tasks()` used the same 3-failure progress-sync threshold for all task types. Long custom jobs such as `official_evaluation` can be CPU/IO-bound for more than one progress timeout while still writing logs/results, so the local dispatch row was marked `failed` before the remote job completed. `FactorOfficialEvaluationService` then trusted the local failed status even when remote results or DB metrics later showed success.

## Fix

- Custom dispatch tasks now use a higher configurable sync-failure threshold: `AISTOCK_CUSTOM_TASK_SYNC_FAIL_THRESHOLD`, default `10`.
- Official-evaluation compute now keeps waiting after sync-unreachable failures while metric rows are progressing.
- Official-evaluation compute now treats complete DB coverage or a successful remote latest result as authoritative and recovers the local dispatch task to `success`.
- Official-evaluation responses include `success_count`, `fail_count`, `total_metrics_inserted`, and `total_metrics_skipped` for UI display.
- The already affected dispatch task `e0f5ee30-a499-44f2-ab42-753c78781dd3` was corrected from `failed` to `success` after remote result verification.

## Validation Commands

- `python -m py_compile backend/services/dispatch_service.py backend/services/quantevolver/factor_official_evaluation_service.py backend/services/quantevolver/factor_transformation_service.py`
- `git diff --check -- backend/services/dispatch_service.py backend/services/quantevolver/factor_official_evaluation_service.py backend/services/quantevolver/factor_transformation_service.py`
- Direct DB/service check: Alpha158 `qe_eval_v2` metrics coverage for snapshot `2026-04-10`.
- Direct dispatch check: task `e0f5ee30-a499-44f2-ab42-753c78781dd3` status and custom sync threshold.

## Results

- Static compile passed.
- Diff check passed; Git reported only repository line-ending warnings.
- Alpha158 coverage: `complete=True`, `complete_factor_count=20`, `expected=20`, `metric_rows=100`, `missing=[]`.
- Corrected dispatch task status: `success`, `error_message=None`, `remote_task_id=232`.
- Threshold check: default dispatch threshold remains `3`; custom task threshold is `10`.

## Residual Risk

- Full UI rerun was not executed to avoid starting another 20-factor full metrics job after all 20 factors already completed.
- Running backend processes need restart before the new recovery logic is active for future UI-triggered jobs.
