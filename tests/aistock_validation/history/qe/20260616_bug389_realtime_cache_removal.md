# BUG-389 旧 factor_values_realtime 兼容路径清理验证记录

- 日期：2026-06-16
- 工作树：`F:\Dev\AIstock_worktrees\BUG-389-factor-values-realtime-20260616`
- 分支：`bug/BUG-389-factor-values-realtime-20260616`
- GitHub Issue：`https://github.com/licong01-cloud/AIstock/issues/1161`
- 生产端口：只读检查 `127.0.0.1:8001`，未重启后端，未写生产 DB，未执行 DDL。

## 修复范围

- 删除 `backend/routers/quantevolver_evolution.py` 中旧 `/factor-values*` 路由、legacy realtime cache env 开关和 `factor_values_realtime` 兼容入口，不再保留 410 业务兼容代码。
- 删除 `backend/services/quantevolver/data_snapshot_manager.py`，并删除 `RealtimeFactorDataLoader` 的 snapshot 注入状态与 `set_snapshot/clear_snapshot` 路径。
- 将 `FactorValuePipeline` 收敛为官方 offline cache metadata helper，只读取 `rdagent_assets/factor_values` 的 `single` parquet 和 `_meta.json`，不再计算实时缓存或创建快照。
- 将相关性计算、官方独立指标、QE prepare-factors cache contract、batch compute runtime validation 统一到官方 `rdagent_assets/factor_values`，非官方 cache root fail-fast。
- UI 文案移除旧实时缓存业务入口，因子库按钮和相关性页面只描述 official offline cache。
- 历史迁移脚本和 workspace protected asset 列表不再包含 `rdagent_assets/factor_values_realtime`。

## DESIGN-COMPLIANCE-001 验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| 删除旧 `/factor-values*` API，不保留 410 兼容入口 | `backend/routers/quantevolver_evolution.py` | 源码扫描无 `/factor-values`；live openapi 仍暴露旧路由，判定为生产运行时代码未加载本 worktree 清理版本 | 代码通过，运行时待部署 | 需要后续用本分支代码重启/合入后再做 live openapi 复验 |
| 删除 `factor_values_realtime` 业务路径 | `backend/routers/quantevolver_evolution.py`, `backend/routers/quantevolver.py`, `backend/services/quantevolver/*`, `scripts/*`, `frontend/src/app/quantevolver/*` | `rg` scoped scan 无 `factor_values_realtime`/legacy env/`DataSnapshotManager` 命中 | 通过 | 历史 BUG JSON 和 baseline 不在业务源码扫描范围 |
| `FactorValuePipeline` 只读官方 cache metadata | `backend/services/quantevolver/factor_value_pipeline.py` | `python -m py_compile`, `ruff`, targeted pytest | 通过 | 无 |
| 删除 DataSnapshotManager 和 snapshot 注入 | `backend/services/quantevolver/data_snapshot_manager.py`, `backend/data_service/realtime_factor_data_loader.py` | 文件删除；`rg data_snapshot_manager|set_snapshot|clear_snapshot` scoped scan 无命中 | 通过 | 无 |
| 相关性计算只使用官方 offline cache | `backend/services/quantevolver/correlation_compute_service.py`, `backend/routers/quantevolver_evolution.py` | `backend/tests/test_correlation_compute_independence.py`; live status 显示 `latest_computation.num_factors=574`、`db_correlation_count=164429` | 通过 | live 8001 仍为旧 openapi，但相关性只读状态已使用官方 cache |
| 独立指标和 QE 回测 cache contract 统一官方 cache | `backend/routers/quantevolver.py`, `backend/services/quantevolver/config_composer.py`, `backend/services/quantevolver/factor_value_loader.py`, `backend/services/quantevolver/factor_official_evaluation_service.py` | `test_qe_prepare_factors_cache_contract.py`, `test_official_evaluation_cache_source.py`, `test_official_runtime_validation.py` | 通过 | 无 |
| UI 不再暴露旧实时缓存业务入口 | `frontend/src/app/quantevolver/components/FactorList.tsx`, `frontend/src/app/quantevolver/factor-correlation/components/ComputePanel.tsx` | targeted `npx tsc` 通过；源码扫描无旧关键词 | 通过 | 未启动/重启前端，未做浏览器 E2E |
| 禁止 legacy resume/backfill 字段误入新 full-compute API | `backend/routers/quantevolver.py`, `backend/tests/quantevolver/test_official_factor_cache_dispatch_route.py` | `FactorCacheComputeRequest` `extra=forbid`; targeted pytest 40 passed | 通过 | 无 |

## 验证命令

```powershell
python -m py_compile <changed Python files>
python -m ruff check <changed Python files>
git diff --check
rg -n 'factor_values_realtime|/factor-values|factor-values|DataSnapshotManager|data_snapshot_manager|AISTOCK_ENABLE_LEGACY_REALTIME_FACTOR_CACHE|LEGACY_REALTIME_FACTOR_CACHE' backend/routers backend/services backend/data_service frontend/src/app/quantevolver scripts -S --glob '!**/__pycache__/**'
python -m pytest backend/tests/test_factor_cache_wsl_env.py backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py backend/tests/test_correlation_compute_independence.py backend/tests/quantevolver/test_official_evaluation_cache_source.py backend/tests/quantevolver/test_official_runtime_validation.py backend/tests/quantevolver/test_official_factor_cache_dispatch_route.py -q
cd frontend; npm ci --ignore-scripts --no-audit --fund=false
npx tsc --noEmit --pretty false --allowJs false --jsx preserve --module esnext --target ES2020 --lib dom,dom.iterable,esnext --skipLibCheck --esModuleInterop --moduleResolution node --resolveJsonModule --isolatedModules --strict src/types/react-plotly-js.d.ts src/app/quantevolver/factor-correlation/components/ComputePanel.tsx src/app/quantevolver/factor-correlation/page.tsx src/app/quantevolver/components/FactorList.tsx
QE_READ_L3_SKIP_UI=1 python -m nox -s l0 validation_module_registry_l0 qe_read_l3
```

## 验证结果

- `python -m py_compile <changed Python files>`：通过。
- `python -m ruff check <changed Python files>`：通过。
- `git diff --check`：通过。
- scoped `rg` 源码扫描：无旧 `factor_values_realtime`、`/factor-values`、`DataSnapshotManager`、legacy env 命中。
- targeted pytest：`40 passed in 5.50s`。
- frontend targeted TypeScript：通过；在 task worktree 执行过 `npm ci --ignore-scripts --no-audit --fund=false` 安装依赖。
- nox gates：`l0`、`validation_module_registry_l0`、`qe_read_l3`、`qe_read_backend` 全部 successful；`qe_read_backend` 为 `14 passed`。
- nox guardrail 输出仍包含既有 baseline/new P2/RAW_JSON_UI 提示，但本次会话 exit code 为 0，未形成 blocking。

## Live 只读 API 复验

- `GET http://127.0.0.1:8001/api/v1/quantevolver/evolution/correlations/overview`：200；返回 `official_cache_window.cache_root=F:\Dev\AIstock\rdagent_assets\factor_values`，`enabled.total=575`，`enabled.correlation_cached=575`，`enabled.correlation_computed=574`。
- `GET http://127.0.0.1:8001/api/v1/quantevolver/evolution/correlations/status`：200；`db_correlation_count=164429`，`uncorrelated_factor_count=1`，`latest_computation.num_factors=574`。
- `GET http://127.0.0.1:8001/openapi.json`：仍列出 8 个旧 `/api/v1/quantevolver/evolution/factor-values*` 路径，说明当前生产 8001 进程仍加载旧代码，不是本 worktree 清理后的代码。
- `GET http://127.0.0.1:8001/api/v1/quantevolver/evolution/factor-values/status`：410，进一步说明运行时仍是 BUG-386/旧隔离版本，而非 BUG-389 删除版本。

## 结论和风险

- 代码层面的清理已完成并通过静态、单测、前端类型检查和 nox gates。
- 当前 live 8001 已经能读到官方相关性结果，相关性数据不是 105 个旧结果：数据库相关性对数为 `164429`，最新计算覆盖 `574` 个因子，剩余 `1` 个因子未相关。
- 但 live 8001 的 OpenAPI 仍暴露旧 `/factor-values*` 路由，证明后端重启未加载本 task worktree 的最新清理代码；需要合入/部署后再次重启并复验 openapi 中不再存在这些路径。
- 本次未写生产 DB、未执行 DDL、未重启后端/前端。
