# 旧实盘演练与 QE 实验选股代码退休验证记录

日期：2026-05-19  
模块：development_guardrails / legacy_cleanup  
级别：L3  
状态：passed（代码清理分支首轮验证通过；尚未提交、推送或合并）

## 范围

本次验证覆盖独立 worktree 中的旧模块删除：

- 删除旧 `frontend/src/app/paper-trading/` 页面组和 `/api/v1/paper-trading/*` 后端实现。
- 删除旧 `frontend/src/app/quantevolver/selection/` 页面和 `qe_selection_service.py`。
- 移除 `backend/main.py` 中旧 paper-trading router/scheduler 注册。
- 移除 `backend/routers/quantevolver.py` 中旧 `POST /experiments/{experiment_id}/selection`。
- 移除导航和 validation catalog 中旧 UI/API 目标。

## 隔离与生产边界

- Worktree：`F:\Dev\AIstock_worktrees\legacy-paper-qe-selection-cleanup-20260519`
- Branch：`codex/legacy-paper-qe-selection-cleanup-20260519`
- Base：`733be353ccf8a2653b1f74da8c65df7a0e04ef61`
- `F:\Dev\AIstock` 根工作区仅做只读 status 检查，未在根工作区执行删除。
- `production_8001_touched=false`
- `production_3000_touched=false`
- `db_write=false`
- `db_schema_drop=false`

## 备份证据

- Archive branch：`archive/legacy-paper-qe-selection-before-cleanup-20260519`
- Tag：`legacy-paper-qe-selection-before-cleanup-20260519`
- Bundle：`F:\Dev\AIstock_backups\legacy_cleanup_20260519\aistock-legacy-paper-qe-selection-before-cleanup.bundle`
- File snapshot：`F:\Dev\AIstock_backups\legacy_cleanup_20260519\tracked_files`
- Manifest：`F:\Dev\AIstock_backups\legacy_cleanup_20260519\MANIFEST.md`
- `git bundle verify`：passed

## 保护路径审计

以下新主路径未进入本次 diff，且路径存在：

```text
frontend/src/app/paper-v2/
frontend/src/lib/paper-v2/
backend/routers/paper_trading_v2.py
backend/services/paper_trading_v2/
backend/routers/selection_center.py
backend/services/selection_center/
backend/routers/strategy_packages.py
backend/services/strategy_package/
backend/inference_engine.py
backend/data_service/qe_data_service.py
backend/services/quantevolver/
```

## 命令与结果

```powershell
rg active legacy reference scan
```

结果：passed。未发现旧 `/paper-trading`、`/quantevolver/selection`、`paper_trading_scheduler`、`DISABLE_PAPER_TRADING_SCHEDULER`、`qe_selection_service`、`ExperimentSelectionRequest`、`trigger_experiment_selection` 等主动代码引用。

```powershell
python -m compileall -q backend/main.py backend/routers/quantevolver.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py
```

结果：passed。

```powershell
python -m pytest -q backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py -p no:cacheprovider
```

结果：`14 passed in 16.60s`。

```powershell
python -m pytest -q backend/tests/strategy_package backend/tests/selection_center backend/tests/paper_trading_v2 -p no:cacheprovider
```

结果：`430 passed, 1 skipped, 2 xfailed in 25.93s`。

```powershell
python -c "import pathlib, yaml; ... yaml.safe_load(...)"
```

结果：`module_registry.yaml` 与 `ui_targets.yaml` 均通过 YAML 解析。

```powershell
git diff --check
```

结果：passed。

```powershell
cd frontend
npm ci
npx tsc --noEmit --pretty false
npm run build
```

结果：tsc passed；Next.js production build passed。

## 观察到的非阻塞事项

- `npm run build` 仍输出既有 React Hook lint warnings，本次删除未新增对应页面或组件警告。
- `npm ci` 输出现有依赖审计问题 `10 vulnerabilities`；本次没有修改前端依赖文件。
- 验证过程中生成了忽略文件/目录，例如 `frontend/node_modules`、前端构建输出、Python `__pycache__`；这些不纳入 Git diff。

## 当前结论

首轮验证支持继续进行人工 diff 审核、提交和 PR 准备。尚不得合并到 `main` 或触发生产重启；旧 `paper_trading` 数据库 schema 也不得在本轮代码清理中处理。
