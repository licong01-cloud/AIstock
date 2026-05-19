# 旧实盘演练与 QE 实验选股退役执行方案

生成日期：2026-05-19  
状态：执行中；已完成独立 worktree、备份、旧代码删除和首轮验证，尚未提交/推送/合并 main  
适用范围：AIstock `main` 未来清理旧“实盘演练”模块和旧 QE“实验选股”模块  
生产边界：本方案落地阶段不触碰生产 `8001` / `3000`，不写数据库，不删除任何代码  

## 1. 目标

本方案的目标是让未来生产 `main` 分支不再包含已经被 Paper Trading v2、StrategyPackage、Selection Center 替代的旧模块代码，同时保证旧代码仍然可以在本地读取、审计和恢复。

核心目标：

1. 旧代码从未来生产 `main` 中删除。
2. 旧代码在本地有多层备份，可读、可 checkout、可从 bundle 恢复。
3. 清理过程不影响 Paper Trading v2、Selection Center、StrategyPackage、QE 主路径。
4. 清理过程不重启生产 `8001` / `3000`。
5. 第一轮不删除生产数据库中的旧 `paper_trading` schema。

## 2. 清理范围

### 2.1 旧“实盘演练”模块

计划从未来 `main` 删除：

```text
frontend/src/app/paper-trading/
backend/routers/paper_trading.py
backend/services/paper_trading/
backend/db/init_paper_trading_schema.py
scripts/alter_portfolio_status.py
scripts/alter_daily_snapshot_add_metrics.py
```

同时从以下位置移除旧入口：

```text
frontend/src/lib/navigation/nav-groups.ts
backend/main.py
tests/aistock_validation/catalog/module_registry.yaml
tests/aistock_validation/catalog/ui_targets.yaml
```

旧模块当前包含的典型功能：

- `/paper-trading/selection`：旧实盘选股
- `/paper-trading/training`：旧模型训练
- `/paper-trading/config`：旧模拟盘配置
- `/paper-trading/monitor`：旧模拟盘监控
- `/paper-trading/reports`：旧报表分析
- `/api/v1/paper-trading/*`：旧后端 API
- `paper_trading_scheduler`：旧模拟盘调度器

### 2.2 旧 QE“实验选股”模块

计划从未来 `main` 删除：

```text
frontend/src/app/quantevolver/selection/
backend/services/quantevolver/qe_selection_service.py
```

同时从以下位置移除旧入口：

```text
frontend/src/lib/navigation/nav-groups.ts
backend/routers/quantevolver.py
tests/aistock_validation/catalog/module_registry.yaml
tests/aistock_validation/catalog/ui_targets.yaml
```

旧模块当前包含的典型功能：

- `/quantevolver/selection`：旧 QE 实验选股页面
- `POST /api/v1/quantevolver/experiments/{experiment_id}/selection`
- `backend/services/quantevolver/qe_selection_service.py`

## 3. 明确保留范围

以下路径不能在本次清理中删除：

```text
frontend/src/app/paper-v2/
frontend/src/lib/paper-v2/
frontend/src/components/paper-v2/
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

保留原因：

- `paper_trading_v2` 是当前模拟盘主路径。
- `selection_center` 是当前统一选股主路径。
- `strategy_package` 是 QE source 到 Selection/Paper 的权威桥接。
- `backend/inference_engine.py` 的 `workspace_path` 推理模式仍被 StrategyPackage live inference 使用。
- `backend/data_service/qe_data_service.py` 仍被 QE 主路径、因子流水线、Qlib exporter、StrategyPackage live inference 使用。
- `backend/services/quantevolver/` 中除 `qe_selection_service.py` 之外仍包含大量 QE 主路径能力。

## 4. 新权威路径

旧“实盘演练”和旧“QE实验选股”删除后，未来产品路径统一为：

```text
QE 单次实验 / QE evolution loop
  -> StrategyPackage
  -> authoritative selection artifact
  -> Selection Center
  -> Paper Trading v2
```

对应现有路径：

```text
/paper-v2/packages
/paper-v2/selection
/paper-v2/portfolios
POST /api/v1/strategy-packages/{package_id}/selection-artifacts/generate
POST /api/v1/selection-center/runs
POST /api/v1/selection-center/runs/{run_id}/create-paper-portfolio
```

权威选股 artifact 规则：

- 必须通过 live/latest-data QE model inference 生成。
- `metadata.source_type` 应为 `live_qe_model_inference_v1`。
- `metadata.authority_scope` 应为 `authoritative_selection`。
- QE 回测 `pred.pkl` 只能作为 diagnostic，不得作为 Selection Center / Paper v2 权威选股输入。

## 5. 执行总览

执行分为三大阶段：

1. **备份阶段**：创建 archive branch、tag、bundle、文件级快照和 manifest。
2. **清理阶段**：在独立 worktree 中删除旧代码、旧入口和旧验证目标。
3. **验证阶段**：用 grep、compile、pytest、frontend build、临时端口 smoke 证明新主路径未受影响。

可选后续阶段：

4. **替代增强阶段**：如仍需要“一键从 QE source 选股”，新增 Selection Center 原子入口。
5. **数据库归档阶段**：单独审批是否 dump / drop 旧 `paper_trading` schema。

## 6. Phase 0：现场冻结与只读检查

执行位置：

```powershell
cd F:\Dev\AIstock
```

检查命令：

```powershell
git status --short --branch
git branch --show-current
git log --oneline -5
git rev-parse HEAD
git rev-parse origin/main
```

判断规则：

- 如果当前根目录有未跟踪或未提交文件，不在根目录执行清理。
- 清理必须从 `origin/main` 创建独立 worktree。
- 根目录 `F:\Dev\AIstock` 视为生产运行和同步基线，不作为本次清理开发目录。

建议输出：

```text
F:\Dev\AIstock_backups\legacy_cleanup_20260519\PRECHECK.txt
```

记录内容：

```text
source_repo=F:\Dev\AIstock
base_commit=<git rev-parse origin/main>
root_status=<git status --short --branch>
production_8001_touched=false
production_3000_touched=false
db_write=false
db_schema_drop=false
```

## 7. Phase 1：创建独立清理 worktree

命令：

```powershell
cd F:\Dev\AIstock

git fetch origin

$branch = "codex/legacy-paper-qe-selection-cleanup-20260519"
$worktree = "F:\Dev\AIstock_worktrees\legacy-paper-qe-selection-cleanup-20260519"

git worktree add -b $branch $worktree origin/main

cd $worktree
git status --short --branch
git log --oneline -5
```

验收：

- 分支为 `codex/legacy-paper-qe-selection-cleanup-20260519`。
- worktree 初始干净。
- base commit 来自 `origin/main`。

## 8. Phase 2：三重本地备份

### 8.1 Archive branch 和 tag

目的：以后可以直接 checkout 旧代码。

命令：

```powershell
cd F:\Dev\AIstock_worktrees\legacy-paper-qe-selection-cleanup-20260519

$base = git rev-parse HEAD

git branch archive/legacy-paper-qe-selection-before-cleanup-20260519 $base
git tag legacy-paper-qe-selection-before-cleanup-20260519 $base
```

恢复命令：

```powershell
git worktree add F:\Dev\AIstock_worktrees\restore-legacy-paper-qe-selection archive/legacy-paper-qe-selection-before-cleanup-20260519
```

### 8.2 Git bundle

目的：即使本地 branch/tag 被误删，仍可从 bundle 恢复。

命令：

```powershell
$backupRoot = "F:\Dev\AIstock_backups\legacy_cleanup_20260519"
New-Item -ItemType Directory -Force $backupRoot | Out-Null

git bundle create "$backupRoot\aistock-legacy-paper-qe-selection-before-cleanup.bundle" --branches --tags
git bundle verify "$backupRoot\aistock-legacy-paper-qe-selection-before-cleanup.bundle"
```

恢复命令：

```powershell
git clone "F:\Dev\AIstock_backups\legacy_cleanup_20260519\aistock-legacy-paper-qe-selection-before-cleanup.bundle" F:\Dev\AIstock_restore_legacy_bundle
```

### 8.3 文件级快照

目的：不 checkout Git 也能直接阅读旧代码。

备份目录：

```text
F:\Dev\AIstock_backups\legacy_cleanup_20260519\tracked_files\
```

命令：

```powershell
$repo = "F:\Dev\AIstock_worktrees\legacy-paper-qe-selection-cleanup-20260519"
$backup = "F:\Dev\AIstock_backups\legacy_cleanup_20260519\tracked_files"

New-Item -ItemType Directory -Force $backup | Out-Null

$paths = @(
  "frontend/src/app/paper-trading",
  "frontend/src/app/quantevolver/selection",
  "backend/routers/paper_trading.py",
  "backend/services/paper_trading",
  "backend/services/quantevolver/qe_selection_service.py",
  "backend/db/init_paper_trading_schema.py",
  "scripts/alter_portfolio_status.py",
  "scripts/alter_daily_snapshot_add_metrics.py"
)

foreach ($p in $paths) {
  $src = Join-Path $repo $p
  if (Test-Path $src) {
    $dst = Join-Path $backup $p
    New-Item -ItemType Directory -Force (Split-Path $dst -Parent) | Out-Null
    Copy-Item -LiteralPath $src -Destination $dst -Recurse -Force
  }
}
```

### 8.4 备份 manifest

创建：

```text
F:\Dev\AIstock_backups\legacy_cleanup_20260519\MANIFEST.md
```

建议内容：

```md
# AIstock Legacy Paper/QE Selection Backup

Date: 2026-05-19
Source repo: F:\Dev\AIstock
Cleanup worktree: F:\Dev\AIstock_worktrees\legacy-paper-qe-selection-cleanup-20260519
Base commit: <base_commit>

## Backup Layers

1. Local branch:
   - archive/legacy-paper-qe-selection-before-cleanup-20260519

2. Local tag:
   - legacy-paper-qe-selection-before-cleanup-20260519

3. Git bundle:
   - F:\Dev\AIstock_backups\legacy_cleanup_20260519\aistock-legacy-paper-qe-selection-before-cleanup.bundle

4. File snapshot:
   - F:\Dev\AIstock_backups\legacy_cleanup_20260519\tracked_files

## Removed From Future Main

- Legacy Paper Trading frontend /paper-trading/*
- Legacy Paper Trading backend /api/v1/paper-trading/*
- Legacy Paper Trading scheduler and services
- Legacy QE experiment selection page /quantevolver/selection
- Legacy QE experiment selection backend endpoint
- Legacy backend/services/quantevolver/qe_selection_service.py

## Canonical Replacement

QE source -> StrategyPackage -> authoritative selection artifact -> Selection Center -> Paper v2

## Restore Commands

git worktree add F:\Dev\AIstock_worktrees\restore-legacy-paper-qe-selection archive/legacy-paper-qe-selection-before-cleanup-20260519

or

git clone F:\Dev\AIstock_backups\legacy_cleanup_20260519\aistock-legacy-paper-qe-selection-before-cleanup.bundle F:\Dev\AIstock_restore_legacy_bundle

## Production Safety

production_8001_touched=false
production_3000_touched=false
db_write=false
db_schema_drop=false
```

## 9. Phase 3：删除旧“实盘演练”前端

删除：

```text
frontend/src/app/paper-trading/
```

修改：

```text
frontend/src/lib/navigation/nav-groups.ts
```

删除整个旧导航组：

```text
title: "📊 实盘演练"
items:
  /paper-trading/selection
  /paper-trading/training
  /paper-trading/config
  /paper-trading/monitor
  /paper-trading/reports
```

不得删除：

```text
frontend/src/app/paper-v2/
frontend/src/lib/paper-v2/
frontend/src/components/paper-v2/
```

验证：

```powershell
rg -n "/paper-trading|实盘演练|实盘选股|模拟盘配置|模拟盘监控" frontend/src -g "!*.tsbuildinfo"
```

期望：

- `frontend/src` 中不再有旧 `/paper-trading` route 或导航。
- 历史文档命中不阻断，但生产 frontend 源码不应命中。

## 10. Phase 4：删除旧“实盘演练”后端

删除：

```text
backend/routers/paper_trading.py
backend/services/paper_trading/
backend/db/init_paper_trading_schema.py
scripts/alter_portfolio_status.py
scripts/alter_daily_snapshot_add_metrics.py
```

修改：

```text
backend/main.py
```

删除旧 router import：

```python
from .routers import paper_trading
```

删除旧 scheduler 启动逻辑：

```python
disable_pt = ...
from .services.paper_trading.scheduler import paper_trading_scheduler
paper_trading_scheduler.start()
```

删除旧 scheduler shutdown 逻辑：

```python
from .services.paper_trading.scheduler import paper_trading_scheduler
paper_trading_scheduler.shutdown(wait=False)
```

删除旧 router include：

```python
app.include_router(paper_trading.router, prefix="/api/v1")
```

不得删除：

```text
backend/routers/paper_trading_v2.py
backend/services/paper_trading_v2/
backend/db/init_trading_core_v2_schema.py
backend/migrations/trading_core_v2_schema.sql
```

验证：

```powershell
rg -n --pcre2 "backend\.services\.paper_trading(?!_v2)|routers import paper_trading|paper_trading_scheduler|/api/v1/paper-trading(?!-v2)|prefix=\"/paper-trading\"" backend frontend/src tests scripts -g "!*.tsbuildinfo"
```

期望：

- 生产代码不再引用 `backend.services.paper_trading`。
- `backend/main.py` 不再启动旧 scheduler。
- `backend/main.py` 不再挂载 `/api/v1/paper-trading`。
- `paper_trading_v2` 命中不算问题。

## 11. Phase 5：删除旧 QE“实验选股”前端

删除：

```text
frontend/src/app/quantevolver/selection/
```

修改：

```text
frontend/src/lib/navigation/nav-groups.ts
```

删除旧导航项：

```text
{ href: "/quantevolver/selection", label: "🚀 实验选股" }
```

不得删除：

```text
frontend/src/app/quantevolver/experiments/
frontend/src/app/quantevolver/evolution/
frontend/src/app/quantevolver/templates/
frontend/src/app/paper-v2/packages/
frontend/src/app/paper-v2/selection/
```

验证：

```powershell
rg -n "/quantevolver/selection|实验选股|QE实验选股" frontend/src -g "!*.tsbuildinfo"
```

期望：

- `frontend/src` 中不再存在旧实验选股 route 或导航。

## 12. Phase 6：删除旧 QE“实验选股”后端

修改：

```text
backend/routers/quantevolver.py
```

删除：

```python
class ExperimentSelectionRequest(BaseModel):
    ...
```

删除：

```python
@router.post("/experiments/{experiment_id}/selection")
def trigger_experiment_selection(...):
    ...
```

删除：

```text
backend/services/quantevolver/qe_selection_service.py
```

不得删除：

```text
backend/inference_engine.py
backend/services/strategy_package/live_inference.py
backend/services/strategy_package/selection_artifact.py
backend/services/strategy_package/qe_source_resolver.py
backend/services/strategy_package/service.py
backend/services/selection_center/
backend/data_service/qe_data_service.py
```

验证：

```powershell
rg -n "qe_selection_service|experiments/\\{experiment_id\\}/selection|/quantevolver/experiments/.*/selection|QE实验选股|实验选股" backend frontend/src tests -g "!*.tsbuildinfo"
```

期望：

- 生产 backend/frontend/tests 不再引用旧 QE 选股服务和旧接口。
- 文档命中可以后续标注为历史说明，不阻断代码清理。

## 13. Phase 7：修正 validation catalog 和测试

修改：

```text
tests/aistock_validation/catalog/module_registry.yaml
tests/aistock_validation/catalog/ui_targets.yaml
```

处理规则：

1. 删除或归档 `paper_trading_legacy` 模块。
2. 从 `selection_center` 的 `ui_routes` 中移除 `/paper-trading/selection`。
3. 从 `qe.single_experiment` 的 `ui_routes` 中移除 `/quantevolver/selection`。
4. 删除旧 UI target：

```text
paper_trading.selection
paper_trading.training
paper_trading.config
paper_trading.monitor
paper_trading.reports
qe.selection
```

需要处理的测试：

```text
backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py
```

该测试当前可能引用旧模块：

```python
from backend.services.paper_trading import db_selection_service as db_selection
from backend.services.paper_trading import training_service as paper_training
```

处理方案：

- 如果测试只覆盖旧模块 worker workspace policy，删除对应旧用例。
- 如果仍需覆盖 worker workspace policy，将用例迁移到 `backend/services/strategy_package/live_inference.py`、`backend/services/strategy_package/workspace_policy.py`、`backend/services/strategy_package/qe_source_resolver.py` 的新路径。

验证：

```powershell
rg -n "paper_trading_legacy|/paper-trading/|/quantevolver/selection|backend\\.services\\.paper_trading" tests backend/tests frontend/tests -g "!*.tsbuildinfo"
```

期望：

- 测试和 validation catalog 不再把旧模块视为当前产品目标。

## 14. Phase 8：更新项目说明文档

建议在本文件之外，同步更新：

```text
docs/codex_project_memory.md
```

建议追加内容：

```md
## Legacy Paper Trading / QE Experiment Selection Retirement - 2026-05-19

- Legacy `/paper-trading/*` and `/api/v1/paper-trading/*` were retired from main after local branch/tag/bundle/file backups.
- Legacy `/quantevolver/selection` and `qe_selection_service.py` were retired.
- Canonical path is now QE source -> StrategyPackage -> authoritative selection artifact -> Selection Center -> Paper v2.
- Do not reintroduce legacy paper_trading services or QE experiment direct-selection endpoints; add new one-click QE source selection under Selection Center if needed.
```

本条仅在实际清理完成后追加，不在方案落地时提前写成已完成状态。

## 15. Phase 9：验证矩阵

### 15.1 静态清理验证

```powershell
rg -n --pcre2 "backend\.services\.paper_trading(?!_v2)|routers import paper_trading|paper_trading_scheduler|/api/v1/paper-trading(?!-v2)|prefix=\"/paper-trading\"" backend frontend/src tests scripts -g "!*.tsbuildinfo"

rg -n "qe_selection_service|experiments/\\{experiment_id\\}/selection|/quantevolver/selection|QE实验选股|实验选股" backend frontend/src tests -g "!*.tsbuildinfo"

rg -n "paper_trading_legacy|paper_trading.selection|paper_trading.training|paper_trading.config|paper_trading.monitor|paper_trading.reports|qe.selection" tests/aistock_validation/catalog
```

期望：

- 第一条无生产代码命中。
- 第二条无生产代码命中。
- 第三条无 catalog 命中。

### 15.2 Python 编译验证

```powershell
python -m compileall backend/main.py backend/routers backend/services/strategy_package backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver
```

### 15.3 后端测试

优先运行：

```powershell
pytest backend/tests/strategy_package backend/tests/selection_center backend/tests/paper_trading_v2 -q -p no:cacheprovider
pytest backend/tests/unified_engine -q -p no:cacheprovider
pytest backend/tests/test_aistock_legacy_inventory.py backend/tests/test_aistock_guardrail_scan.py -q -p no:cacheprovider
```

如 `backend/tests/unified_engine` 过重，可先运行被修改测试，再运行 guardrail。

### 15.4 前端构建

```powershell
cd frontend
npm run build
```

### 15.5 新主路径 API smoke

不使用生产 `8001`，使用临时端口，例如 `8013`：

```powershell
cd F:\Dev\AIstock_worktrees\legacy-paper-qe-selection-cleanup-20260519
$env:DISABLE_PAPER_TRADING_V2_SCHEDULER = "1"
uvicorn backend.main:app --host 127.0.0.1 --port 8013
```

另一个终端：

```powershell
Invoke-RestMethod "http://127.0.0.1:8013/api/v1/strategy-packages/qe-sources?limit=5"
Invoke-RestMethod "http://127.0.0.1:8013/api/v1/selection-center/selectable-packages?limit=5"
```

确认旧 API 不存在：

```powershell
try {
  Invoke-RestMethod "http://127.0.0.1:8013/api/v1/paper-trading/portfolios"
} catch {
  $_.Exception.Response.StatusCode.value__
}

try {
  Invoke-RestMethod "http://127.0.0.1:8013/api/v1/quantevolver/experiments/dummy/selection" -Method Post -ContentType "application/json" -Body "{}"
} catch {
  $_.Exception.Response.StatusCode.value__
}
```

期望：

- 新 StrategyPackage / Selection Center API 可用。
- 旧 `/api/v1/paper-trading/*` 不再返回旧业务结果。
- 旧 QE experiment selection API 不再返回旧业务结果。

### 15.6 关闭临时服务

如启动了 `8013`，验证后关闭并确认端口释放：

```powershell
netstat -ano | findstr :8013
```

## 16. Phase 10：提交和 PR

提交前检查：

```powershell
git status --short
git diff --check
git diff --stat
```

建议 commit 拆分：

```text
docs(ops): document legacy paper and qe selection retirement
refactor(legacy): remove old paper trading and qe experiment selection paths
```

不要提交：

```text
F:\Dev\AIstock_backups\...
Git bundle
DB dump
.codex_tmp
临时日志
frontend build 输出
```

PR 描述模板：

```md
## Summary

Retires legacy Paper Trading and QE experiment direct-selection paths from main.

Removed:
- frontend /paper-trading/*
- backend /api/v1/paper-trading/*
- legacy paper_trading scheduler/services
- frontend /quantevolver/selection
- backend /quantevolver/experiments/{experiment_id}/selection
- qe_selection_service.py

Kept:
- Paper Trading v2
- Selection Center
- StrategyPackage
- StrategyPackage authoritative selection artifact generation
- inference_engine workspace_path mode
- qe_data_service.py

## Backup

Local backup only:
- branch: archive/legacy-paper-qe-selection-before-cleanup-20260519
- tag: legacy-paper-qe-selection-before-cleanup-20260519
- bundle: F:\Dev\AIstock_backups\legacy_cleanup_20260519\...
- file snapshot: F:\Dev\AIstock_backups\legacy_cleanup_20260519\tracked_files

## Validation

- rg old path checks: pass
- python compileall: pass
- backend pytest: pass
- frontend npm run build: pass
- temp API smoke on 8013: pass

## Production

- production_8001_touched=false
- production_3000_touched=false
- db_write=false
- db_schema_drop=false
```

## 17. Phase 11：可选替代增强

如果仍需要“从 QE 实验/Loop 一键选股”的体验，不恢复旧 `/quantevolver/selection`，而是在 Selection Center 新增原子入口。

建议新入口：

```text
POST /api/v1/selection-center/runs/from-qe-source
```

建议请求：

```json
{
  "source_kind": "qe_experiment",
  "experiment_id": "qe_20260506_182113",
  "qe_task_id": null,
  "qe_loop_id": null,
  "trade_date": "2026-04-30",
  "data_source": "DB_HISTORICAL",
  "runtime_config": {
    "top_k": 50,
    "exclude_suspended": true
  },
  "resolve_runtime_assets": true,
  "generate_selection_artifact": true,
  "reuse_existing_package": true
}
```

内部流程：

```text
1. 校验 QE source 存在且 completed
2. 创建或复用 StrategyPackage
3. 校验 package health
4. 生成或复用 authoritative selection artifact
5. 调用 SelectionCenterService.run_single_package
6. 返回 selection run、package_id、artifact_id、health/preflight 信息
```

前端入口建议放在：

```text
frontend/src/app/paper-v2/packages/page.tsx
```

或：

```text
frontend/src/app/paper-v2/selection/page.tsx
```

不得恢复：

```text
frontend/src/app/quantevolver/selection/
```

## 18. Phase 12：数据库归档，单独审批

第一轮代码清理不 drop DB。后续如果确认旧数据无生产价值，再单独做 DB 归档。

只读统计：

```sql
SELECT COUNT(*) FROM paper_trading.portfolio_config;
SELECT COUNT(*) FROM paper_trading.trade_signals;
SELECT COUNT(*) FROM paper_trading.daily_snapshot;
SELECT COUNT(*) FROM paper_trading.positions;
SELECT COUNT(*) FROM paper_trading.trades;
SELECT COUNT(*) FROM paper_trading.training_jobs;
```

dump 建议：

```powershell
pg_dump --schema=paper_trading --schema-only ...
pg_dump --schema=paper_trading --data-only ...
```

只有在明确批准后，才允许执行：

```sql
DROP SCHEMA paper_trading CASCADE;
```

该步骤必须作为独立 DB 变更，不得混入代码清理 PR。

## 19. 最终验收标准

清理完成后，未来生产 `main` 必须满足：

- `main` 不包含旧 `/paper-trading/*` 前端页面。
- `main` 不挂载旧 `/api/v1/paper-trading/*` 后端 API。
- `main` 不启动旧 `paper_trading_scheduler`。
- `main` 不包含 `backend/services/paper_trading/*`。
- `main` 不包含旧 `/quantevolver/selection` 页面。
- `main` 不包含旧 `backend/services/quantevolver/qe_selection_service.py`。
- `main` 不包含旧 `POST /api/v1/quantevolver/experiments/{experiment_id}/selection`。
- 新 `Paper v2 / StrategyPackage / Selection Center` 仍通过构建和测试。
- 本地可通过 branch/tag/bundle/file snapshot 恢复或阅读旧代码。
- 生产 `8001/3000` 未在清理过程中被触碰。
- 生产 DB 未在清理过程中被写入或 drop。

## 20. 回滚方案

如果清理后发现需要恢复旧代码，有三种方式。

### 20.1 从 archive branch 创建恢复 worktree

```powershell
git worktree add F:\Dev\AIstock_worktrees\restore-legacy-paper-qe-selection archive/legacy-paper-qe-selection-before-cleanup-20260519
```

### 20.2 从 bundle 恢复完整仓库

```powershell
git clone F:\Dev\AIstock_backups\legacy_cleanup_20260519\aistock-legacy-paper-qe-selection-before-cleanup.bundle F:\Dev\AIstock_restore_legacy_bundle
```

### 20.3 从文件快照拷贝单个文件

```powershell
Copy-Item -Recurse -Force `
  F:\Dev\AIstock_backups\legacy_cleanup_20260519\tracked_files\backend\services\paper_trading `
  F:\Dev\AIstock_worktrees\restore-target\backend\services\paper_trading
```

注意：

- 回滚到生产 `main` 必须重新走 PR 和验证。
- 不允许直接把旧代码复制回生产根目录并重启服务。
- 如果旧 DB schema 已在未来单独 drop，代码恢复不等于数据恢复。

## 21. 当前执行状态（2026-05-19）

本节记录本次实际执行进度，覆盖“开始执行”后的已完成事项。执行仍停留在独立 worktree，本分支尚未提交、尚未推送、尚未合并到 `main`。

### 21.1 隔离边界

- 清理 worktree：`F:\Dev\AIstock_worktrees\legacy-paper-qe-selection-cleanup-20260519`
- 清理分支：`codex/legacy-paper-qe-selection-cleanup-20260519`
- 基线 commit：`733be353ccf8a2653b1f74da8c65df7a0e04ef61`
- 根工作区 `F:\Dev\AIstock` 保持原有任务状态，仅做只读 status 检查；未在根工作区执行本次删除。
- 生产 `8001` / `3000` 未启动、未停止、未重启；生产数据库未写入、未 dump、未 drop。

### 21.2 已完成备份

- 本地归档分支：`archive/legacy-paper-qe-selection-before-cleanup-20260519`
- 本地 tag：`legacy-paper-qe-selection-before-cleanup-20260519`
- Git bundle：`F:\Dev\AIstock_backups\legacy_cleanup_20260519\aistock-legacy-paper-qe-selection-before-cleanup.bundle`
- 文件级快照：`F:\Dev\AIstock_backups\legacy_cleanup_20260519\tracked_files`
- 备份清单：`F:\Dev\AIstock_backups\legacy_cleanup_20260519\MANIFEST.md`
- 备份校验：archive branch、tag 均指向 `733be353ccf8a2653b1f74da8c65df7a0e04ef61`；bundle `git bundle verify` 通过。

### 21.3 已删除旧模块代码

旧“实盘演练”删除范围：

```text
frontend/src/app/paper-trading/
backend/routers/paper_trading.py
backend/services/paper_trading/
backend/db/init_paper_trading_schema.py
scripts/alter_portfolio_status.py
scripts/alter_daily_snapshot_add_metrics.py
```

旧 QE“实验选股”删除范围：

```text
frontend/src/app/quantevolver/selection/
backend/services/quantevolver/qe_selection_service.py
```

同步修改入口与目录：

```text
backend/main.py
backend/routers/quantevolver.py
frontend/src/lib/navigation/nav-groups.ts
tests/aistock_validation/catalog/module_registry.yaml
tests/aistock_validation/catalog/ui_targets.yaml
backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py
```

### 21.4 保护范围复核

以下新主路径文件/目录未进入本次 diff，且路径存在：

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

### 21.5 首轮验证结果

已通过：

```text
rg active legacy reference scan：通过，未发现旧 /paper-trading、/quantevolver/selection、paper_trading_scheduler、qe_selection_service 等主动引用。
python -m compileall：通过。
python -m pytest -q backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py -p no:cacheprovider：14 passed。
python -m pytest -q backend/tests/strategy_package backend/tests/selection_center backend/tests/paper_trading_v2 -p no:cacheprovider：430 passed, 1 skipped, 2 xfailed。
python YAML safe_load：module_registry.yaml 和 ui_targets.yaml 均通过。
git diff --check：通过。
frontend npx tsc --noEmit --pretty false：通过。
frontend npm run build：通过。
```

注意事项：

- `npm run build` 仍显示仓库已有 React Hook lint warnings；本次删除未新增对应页面/组件警告。
- `npm ci` 报告现有依赖审计问题 `10 vulnerabilities`；本次没有修改 `package.json` 或 `package-lock.json`。
- 前端构建未再生成 `/paper-trading/*` 或 `/quantevolver/selection` 路由；保留 `/paper-v2/*`、`/quantevolver/*` 主路径。

### 21.6 当前未完成事项

- 尚未提交 commit。
- 尚未 push 分支。
- 尚未创建 PR。
- 尚未合并到 `main`。
- 尚未做生产服务重启或生产数据库处理。
- 旧 `paper_trading` schema 的数据库归档仍需作为独立审批事项，不纳入本轮代码清理。

## 22. 后续执行计划

1. 继续人工审阅当前 diff，重点复核 `backend/main.py`、`backend/routers/quantevolver.py`、`frontend/src/lib/navigation/nav-groups.ts`、validation catalog 与删除清单是否完全符合退休范围。
2. 如用户批准提交，则只 stage 本次清理相关文件和验证记录，不纳入根工作区或其他 worktree 的任何文件。
3. 提交后可按需推送 `codex/legacy-paper-qe-selection-cleanup-20260519` 并创建 PR；合并 `main` 前再次确认 `origin/main` 是否已变化，如变化则在独立 worktree 内 rebase/merge 审核，不切换根目录分支。
4. 合并前重复验证：旧引用扫描、compileall、目标 pytest、StrategyPackage/Selection Center/Paper v2 回归、YAML 校验、frontend tsc/build、`git diff --check`。
5. 合并后是否重启生产服务由用户单独决定；代码同步和生产运行时激活仍然分离。
6. 数据库归档或 drop 旧 `paper_trading` schema 必须另起独立方案、备份和审批，不跟本次代码清理混合。
