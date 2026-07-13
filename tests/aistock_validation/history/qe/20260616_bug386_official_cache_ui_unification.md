# BUG-386 官方因子缓存 UI/API 统一验收记录

## 背景

用户要求因子独立指标、因子相关性、QE 回测缓存统一使用官方离线因子值缓存 `rdagent_assets/factor_values`，不得再把 `factor_values_realtime` 当作业务缓存入口。UI 需要继续展示独立指标、相关性、QE 回测缓存时间段，支持手工配置，默认使用全量回测数据集窗口 `2018-08-01 ~ 2026-04-30`。

## DESIGN-COMPLIANCE-001 验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| 因子库 UI 展示官方共用缓存和默认全量时间段 | `frontend/src/app/quantevolver/components/FactorList.tsx` | `npm run build`、定向 `npx tsc` 通过 | pass | 无 |
| 因子库 UI 支持手工配置缓存开始/结束日期并提交到后端 | `frontend/src/app/quantevolver/components/FactorList.tsx`、`backend/routers/quantevolver.py` | 定向 `npx tsc`、`py_compile` 通过 | pass | 无 |
| 移除独立的旧回测缓存生成/旧 snapshot 管理业务入口 | `frontend/src/app/quantevolver/components/FactorList.tsx` | 代码扫描和前端 build 通过 | pass | 历史说明文本保留，用于提示旧入口已禁用 |
| 因子相关性 UI 只使用官方缓存窗口，不再传旧 snapshot/data_date/as_of_date 分支 | `frontend/src/app/quantevolver/factor-correlation/page.tsx`、`frontend/src/app/quantevolver/factor-correlation/components/ComputePanel.tsx` | 定向 `npx tsc`、前端 build 通过 | pass | 无 |
| 相关性后端默认使用官方缓存 `rdagent_assets/factor_values` | `backend/services/quantevolver/correlation_compute_service.py`、`backend/routers/quantevolver_evolution.py` | `py_compile` 通过，代码扫描确认相关性路径无 realtime fallback | pass | 无 |
| 官方独立指标批量/全流程计算透传 start_date/end_date | `backend/services/manual_factor_service.py`、`backend/routers/quantevolver.py`、`frontend/src/app/quantevolver/components/ManualFactorDialog.tsx`、`frontend/src/app/quantevolver/components/FullPipelineDialog.tsx` | `py_compile`、定向 `npx tsc` 通过 | pass | 无 |
| QE 回测缓存候选只指向官方共用缓存，不再读取 `factor_values_realtime` | `backend/routers/quantevolver.py` | `py_compile` 通过，代码扫描确认 realtime 仅作为排除/legacy 注释 | pass | 无 |
| 旧 `/factor-values*` 实盘缓存/快照 API 默认隔离 | `backend/routers/quantevolver_evolution.py` | `py_compile` 通过；默认未设置 `AISTOCK_ENABLE_LEGACY_REALTIME_FACTOR_CACHE` 时返回 HTTP 410 | pass | 无 |
| `FactorValuePipeline` 禁止无参静默落到旧 realtime 目录 | `backend/services/quantevolver/factor_value_pipeline.py` | `py_compile` 通过；构造函数无 `output_dir` 时 fail-fast | pass | 无 |

## 验证证据

- `python -m py_compile backend/routers/quantevolver_evolution.py backend/routers/quantevolver.py backend/services/quantevolver/factor_value_pipeline.py backend/services/manual_factor_service.py`：通过。
- `cd frontend; npx tsc --noEmit --pretty false --allowJs false --jsx preserve --module esnext --target ES2020 --lib dom,dom.iterable,esnext --skipLibCheck --esModuleInterop --moduleResolution node --resolveJsonModule --isolatedModules --strict src/types/react-plotly-js.d.ts src/app/quantevolver/factor-correlation/components/ComputePanel.tsx src/app/quantevolver/factor-correlation/page.tsx src/app/quantevolver/components/FactorList.tsx src/app/quantevolver/components/ManualFactorDialog.tsx src/app/quantevolver/components/FullPipelineDialog.tsx`：通过。
- `cd frontend; npm run build`：通过，仅保留既有 React hook warning。
- `python -m nox -s l0 validation_module_registry_l0 qe_read_l3`：首次运行 `l0`、`validation_module_registry_l0` 和 `qe_read_l3` backend/read gates 通过；`qe_read_ui` 在服务预检阶段阻断，现象为 `127.0.0.1:8011` socket 未有临时后端服务但 urllib 经 `HTTP_PROXY=http://127.0.0.1:7896` 返回 HTTP 502，且当时环境未设置 `NO_PROXY`。该失败不来自本次代码变更。
- `QE_READ_L3_SKIP_UI=1 python -m nox -s l0 validation_module_registry_l0 qe_read_l3`：通过，覆盖 `l0`、`validation_module_registry_l0`、`qe_read_l3`、`qe_read_backend`。
- `python -m nox -s qe_read_ui`（临时启动非生产后端 `127.0.0.1:8011`，设置 `NO_PROXY=127.0.0.1,localhost`）：服务预检通过；随后被既有无关全量前端 TypeScript 错误阻断：`tests/research-assistant/phase5-mcp-gateway-ui.spec.ts(226,11): Type 'null' is not assignable to type 'string'`。
- `cd frontend; npx tsc --noEmit --pretty false`：失败于既有无关测试文件 `tests/research-assistant/phase5-mcp-gateway-ui.spec.ts(226,11): Type 'null' is not assignable to type 'string'`，不属于 BUG-386 变更范围。

## 生产门禁

- `production_ddl_gate=noop`：无 DDL。
- `production_backend_dependency_gate=noop`：无后端依赖新增。
- `production_frontend_dependency_gate=noop`：无前端依赖新增；`npm ci` 仅在 worktree 安装现有依赖用于验证。
- 未重启生产后端、未重启生产前端、未写生产 DB、未触碰生产数据。



