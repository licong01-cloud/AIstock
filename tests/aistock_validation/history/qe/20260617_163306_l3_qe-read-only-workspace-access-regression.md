# BUG-401 官方因子缓存旧链路清理验证记录

## 背景

BUG-401 清理官方因子缓存旧链路隐式数据源：官方独立指标、因子相关性、QE 回测读取必须显式使用 `rdagent_assets/factor_values` 的 `single` cache；不得因 `FactorValueLoader` 默认 `auto`、`FactorAnalyst` 旧 ad-hoc 相关性 fallback、legacy backfill、相关性详情/LLM 分析或因子代码改造功能误入实时/快照/非官方改造链路。

## DESIGN-COMPLIANCE-001 验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `FactorValueLoader` 不再允许裸调用，所有读取必须显式声明数据源 | `backend/services/quantevolver/factor_value_loader.py`, `backend/tests/quantevolver/test_legacy_factor_paths_removed.py` | `FactorValueLoader()` fail-fast 回归；源码扫描只剩测试中的裸调用断言 | pass | 无 |
| 官方分析辅助和 ad-hoc 因子相关性只使用 official `source="single"` cache，且不写旧 DB、不做分类估算兜底 | `backend/services/quantevolver/factor_analyst.py`, `backend/routers/quantevolver_evolution.py`, `backend/tests/quantevolver/test_legacy_factor_paths_removed.py` | pytest 覆盖 `_official_factor_value_loader()`、无 `_upsert_correlation`、无 `_estimate_by_category`、`save_hdf5=False`、router `_get_loader` 默认 `single` | pass | 无 |
| 相关性详情和相关性 LLM 分析不得读取非官方 live/simulation 改造代码 | `backend/routers/quantevolver_evolution.py`, `backend/services/quantevolver/correlation_llm_agent.py`, `backend/services/quantevolver/correlation_compute_service.py` | pytest 断言只查询 `code_text, asset_path`，不再读取 `realtime_code_text/qe_code_path`；持久化错误文案不再要求 `transformation_status=SUCCESS` 或改造文件 | pass | 无 |
| legacy `scripts/backfill_factor_cache.py` 不再作为手工入口执行重算 | `scripts/backfill_factor_cache.py`, `backend/tests/quantevolver/test_legacy_factor_paths_removed.py` | `backfill_factor_cache.main()` 直接提示 retired；保留 importable helper 以维持历史 cache contract tests | pass | 无 |
| `RealtimeFactorDataLoader` 仅保留非官方 live/simulation 因子代码改造旁路，不能被官方指标/相关性/QE cache 使用 | `backend/services/quantevolver/factor_code_transformer.py`, `backend/services/quantevolver/factor_transformation_service.py`, `frontend/src/app/quantevolver/factor-transformation/*` | `FactorCodeTransformer` 必须传 `NON_OFFICIAL_LIVE_TRANSFORMATION_CONTEXT`；UI 文案显示“非官方 live 版本；不参与官方因子缓存、相关性或 QE 回测”；Playwright 访问页面确认文案 | pass | 无 |
| 业务源码不再暴露旧 realtime cache 目录、DataSnapshotManager、旧 `/factor-values` API、隐式 auto 读取路径或误导性“实时代码/实时数据接口”口径 | `backend/**`, `frontend/src/**`, `scripts/**`, `tests/**` | `rg` 静态扫描无业务旧链路命中；剩余命中仅为 `FactorValueLoader` 内部 legacy diagnostics auto 支持、测试断言、issue workflow `open_auto_filed` 或 paper trading 无关 `live_data_source` | pass | 无 |
| 生产安全门禁 | 本次无 DDL、无依赖变更、未重启生产 backend/frontend/TDX，未写生产 DB | nox gates、targeted tests、UI read-only smoke | pass | UI smoke 读取了现有生产后端 8001 的只读接口；未重启生产服务 |

## 验证命令与结果

```powershell
python -m py_compile backend/routers/quantevolver.py backend/routers/quantevolver_evolution.py backend/services/quantevolver/factor_value_loader.py backend/services/quantevolver/factor_analyst.py backend/services/quantevolver/factor_code_transformer.py backend/services/quantevolver/factor_transformation_service.py backend/services/quantevolver/correlation_llm_agent.py backend/services/quantevolver/correlation_compute_service.py scripts/backfill_factor_cache.py backend/tests/quantevolver/test_legacy_factor_paths_removed.py
# PASS

python -m ruff check backend/routers/quantevolver.py backend/routers/quantevolver_evolution.py backend/services/quantevolver/factor_value_loader.py backend/services/quantevolver/factor_analyst.py backend/services/quantevolver/factor_code_transformer.py backend/services/quantevolver/factor_transformation_service.py backend/services/quantevolver/correlation_llm_agent.py backend/services/quantevolver/correlation_compute_service.py scripts/backfill_factor_cache.py backend/tests/quantevolver/test_legacy_factor_paths_removed.py
# PASS: All checks passed

python -m pytest backend/tests/quantevolver/test_legacy_factor_paths_removed.py backend/tests/quantevolver/test_official_evaluation_cache_source.py backend/tests/quantevolver/test_official_runtime_validation.py backend/tests/test_factor_st_pit_metrics_cache.py backend/tests/test_factor_cache_wsl_env.py tests/test_backfill_factor_cache_task_dir.py backend/tests/test_correlation_compute_independence.py backend/tests/quantevolver/test_bug_013_014_factor_eligibility_correlation.py -q -p no:cacheprovider
# PASS: 55 passed, 1 warning

npm exec eslint -- src/app/quantevolver/factor-transformation/page.tsx src/app/quantevolver/factor-transformation/components/CodeModal.tsx src/app/quantevolver/factor-transformation/components/FactorTable.tsx src/app/quantevolver/factor-transformation/components/types.ts
# PASS

cd frontend; .\node_modules\.bin\tsc.cmd --noEmit --incremental false --pretty false --project .\tsconfig.bug401.tmp.json
# PASS with temporary project including changed factor-transformation files; temp config removed

$env:NO_PROXY='127.0.0.1,localhost,::1'; $env:no_proxy='127.0.0.1,localhost,::1'; $env:QE_READ_L3_SKIP_UI='1'; python -m nox -s l0 validation_module_registry_l0 qe_read_l3
# PASS: l0, validation_module_registry_l0, qe_read_l3, qe_read_backend; qe_read_backend 14 passed

rg -n 'FactorValueLoader\(\)|source\s*[:=][^\n]*auto|factor_values_realtime|DataSnapshotManager|/factor-values|AISTOCK_ENABLE_LEGACY_REALTIME_FACTOR_CACHE|实时代码|实时加载器|实时数据接口|实时获取数据|get_factor_realtime_code|_save_realtime_code|realtime_code_text, asset_path, qe_code_path|for path_key in \["qe_code_path", "asset_path"\]|transformation_status=SUCCESS 且 qe_code_path' backend frontend/src scripts tests .github noxfile.py -g '!frontend/node_modules/**' -g '!tests/aistock_validation/history/**' -g '!tests/aistock_validation/bugs/**' -g '!docs/**' -g '!tmp/**' -g '!**/__pycache__/**' -g '!*.pyc'
# PASS: no business legacy-cache hits; remaining hits are FactorValueLoader internal legacy diagnostics auto support, test assertions, issue workflow open_auto_filed, or unrelated paper_trading live_data_source text

git diff --check
# PASS
```

## UI 只读验证

- 临时启动本分支 Next dev server：`127.0.0.1:3011`，`NEXT_PUBLIC_API_BASE=http://127.0.0.1:8001/api/v1`。
- Playwright 打开 `/quantevolver/factor-transformation` 返回 HTTP 200。
- 页面显示：`将 RDAgent 生成的因子代码转换为仅供模拟盘/选股/荐股使用的非官方 live 版本；不参与官方因子缓存、相关性或 QE 回测`。
- 页面统计读取现有后端只读接口成功：`778` 因子总数、`778` 非官方改造代码；console 无错误。
- 截图：`tmp/bug401_factor_transformation_prod8001.png`（本地 ignored 产物，不提交）。

## 已知环境说明

- 使用临时 dev backend `8011` 做 UI smoke 时，接口因当前 shell 未配置 `TDX_DB_PASSWORD` 返回 DB auth 500：`fe_sendauth: no password supplied`。这不是本次代码逻辑失败；生产/现有后端 `8001` 只读接口正常。
- 全量 `frontend` TypeScript 仍被既有无关文件阻断：`frontend/tests/research-assistant/phase5-mcp-gateway-ui.spec.ts(226,11): Type 'null' is not assignable to type 'string'`。本次变更文件的临时 tsc 项目已通过。
- nox 生成的同名 `.json` metadata 为 ignored 运行产物，不提交。

## 生产门禁

- `production_ddl_gate=noop`：无 DDL。
- `production_backend_dependency_gate=noop`：无后端依赖新增。
- `production_frontend_dependency_gate=noop`：无前端依赖新增；`npm ci` 仅恢复 worktree-local `node_modules`。
- 未重启生产 backend `8001`、frontend `3000`、TDX `19080`；未写生产 DB；临时 dev frontend/backend 已停止。
