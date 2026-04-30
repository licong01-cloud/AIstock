# Multi-Alpha QE 三阶段修复与执行计划

Last updated: 2026-04-26

## 1. 目标与当前结论

目标是在不修改 Qlib 源代码的前提下，修复 AIstock Multi-Alpha QE 实验执行链路，并保证现有单节点 QE 单次实验、QE 演进实验不被回归破坏。

结论：当前最合理的执行顺序不是先做两节点分布式，而是先完成 WSL 单节点 Multi-Alpha 闭环。原因是 Qlib 默认把训练和回测绑定在一次 `qrun` 执行中，Multi-Alpha 又需要先分别训练多个 group，再由 meta model 合成预测，最后统一回测。如果直接叠加两节点调度，会把核心 Qlib 拆分问题、远端文件传输、状态回收、UI 调度问题混在一起，无法定位阻断点。

因此按 3 个阶段推进：

1. Phase 1：所有训练和统一回测都在 WSL 单节点执行，证明 Multi-Alpha 主链路可闭环。
2. Phase 2：继续保持 WSL 单节点，但强化结果生命周期、错误显式化、UI 展示闭环。
3. Phase 3：在 Phase 1/2 验证通过后，再启用两节点分布式，WSL 承担 GPU 类型训练，远端 CPU 节点承担 CPU 模型训练。

## 2. 不可违反约束

- 禁止修改 Qlib 源代码或 Python 环境中的 `site-packages/qlib` 文件。
- 禁止改变 `python qrun_limit_minute.py conf.yaml` 的默认语义：仍然是完整训练 + 回测。
- 禁止改变 `--train-only` 和 `--backtest-only` 的既有语义。
- 禁止静默降级。如果当前阶段不支持 `distributed`，必须显式报错，而不是自动切回 `serial`。
- 禁止业务逻辑兜底隐藏错误。缺预测、缺组合回测结果、缺指标、缺 artifact 都必须 fail fast。
- 禁止简化版或占位实现。阶段内需要的能力必须完整实现；无法完整实现时阻断该阶段。
- 每个阶段必须通过对应验证后才能进入下一阶段。失败后必须定位并修复，再重新验证。
- UI 与后端必须同步适配；不允许后端完成但 UI 无法提交、无法查看状态或无法展示结果。

## 3. 已确认阻断性问题

| 区域 | 阻断问题 | 影响 | 关键文件 |
| --- | --- | --- | --- |
| Qlib 执行拆分 | Multi-Alpha 需要先 group train-only，再统一 pred-backtest；原 runner 没有 `--pred-backtest` | meta 合成后的预测无法进入统一回测 | `scripts/qrun_limit_minute.py`, `backend/services/quantevolver/multi_alpha_engine.py` |
| 统一回测文件包 | root unified backtest 不能复用 train-only group 文件包，否则会缺策略依赖或 root 因子准备逻辑 | unified backtest 运行时缺文件 | `backend/services/quantevolver/multi_alpha_engine.py` |
| root 因子环境 | `combined_prediction.pkl` 回测前，root dataset 仍需要先执行 `prepare_factors.py` 生成 `combined_factors_df.parquet` 和 `.factor_env` | Qlib dataset 初始化失败 | `backend/services/quantevolver/multi_alpha_engine.py` |
| 分布式模式过早暴露 | UI 默认 `distributed`，后端分布式链路尚未完成 | 用户提交会进入未验证路径 | `frontend/src/app/quantevolver/components/MultiAlphaGroupEditor.tsx`, `backend/routers/quantevolver.py` |
| 二进制 artifact 传输 | 分布式 collector 存在 pickle/parquet 以文本形式提交的风险 | 远端/本地 artifact 可能损坏 | `backend/services/quantevolver/multi_alpha_result_collector.py` |
| CPU/GPU 策略 | CPU group 目前可能 round-robin 到 WSL，不满足“WSL GPU、远端 CPU”目标 | Phase 3 调度策略不正确 | `backend/services/quantevolver/multi_alpha_resource_planner.py` |

## 4. Phase 1 - WSL 单节点 Multi-Alpha 闭环

### 4.1 阶段目标

在 WSL 节点 `wsl2-5080` 上完成完整链路：

`group train-only -> group pred.pkl -> meta combine -> combined_prediction.pkl -> pred-backtest -> qlib_results_enhanced.json -> UI result endpoint`

Phase 1 不启用分布式训练；任何 `execution_mode=distributed` 请求必须显式拒绝。

### 4.2 已实施内容

Backend：

- `scripts/qrun_limit_minute.py`
  - 新增互斥模式 `--pred-backtest PRED_PKL`。
  - 在 pred-backtest 模式中读取已有预测，不再执行 `SignalRecord` 生成预测。
  - 校验预测文件存在、pickle 内容为 `DataFrame`/`Series`、index 为 MultiIndex。
  - 初始化原 `conf.yaml` dataset，从 test segment 获取 label，并写入 `pred.pkl`、`label.pkl`。
  - 要求 `PortAnaRecord` 必须存在；缺失时显式失败。
  - 保持默认完整训练回测、`--train-only`、`--backtest-only` 既有语义不变。
- `backend/routers/quantevolver.py`
  - 新增分布式功能开关检查。
  - 在 auto-select、config-generate、experiment run 入口阻断未启用的 `distributed`。
  - 阻断信息要求用户使用 `execution_mode='serial'`，或者等 Phase 3 完成后显式开启功能开关。
- `backend/services/quantevolver/multi_alpha_engine.py`
  - root unified backtest 改为通过 `compose_experiment_in_memory(... train_only=False)` 生成权威完整文件包。
  - 不再从 train-only group 复制 root 回测依赖。
  - 生成 `meta_model_runner.py` 时修复 Windows 路径反斜杠转义语法错误。
  - 在 unified pred-backtest 前执行 root `prepare_factors.py`，校验 `combined_factors_df.parquet`，读取 `.factor_env` 并传入回测环境。
  - unified backtest 调用 `qrun_limit_minute.py conf.yaml --pred-backtest combined_prediction.pkl`。
- `backend/services/quantevolver/executors/backtest.py`
  - `BACKTEST_ONLY` 没有 `ctx.model_source` 时显式失败，避免隐式猜测 workspace。
- `backend/services/quantevolver/qe_evolution_service.py`
  - QE 演进 retry/backtest-only 调用传入明确 `model_source`，避免上述 fail-fast 破坏既有演进路径。
- `backend/tests/conftest.py`
  - 为当前 Python/pytest 行为补充事件循环 fixture，保持既有 `asyncio.get_event_loop().run_until_complete(...)` 测试可运行。

Frontend：

- `frontend/src/app/quantevolver/components/MultiAlphaGroupEditor.tsx`
  - auto-select 默认 `execution_mode` 从 `distributed` 改为 `serial`。
  - `NEXT_PUBLIC_MULTI_ALPHA_DISTRIBUTED_ENABLED !== "1"` 时禁用分布式选项。
  - 如果用户仍尝试提交 disabled distributed，前端显示明确错误。
- `frontend/src/app/quantevolver/compose/page.tsx`
  - config generate 前阻断 disabled distributed，避免 UI 进入后端未支持路径。
- `frontend/src/app/quantevolver/components/MultiAlphaProgress.tsx`
  - 文案不再暗示当前必须是分布式调度。
- `frontend/src/app/quantevolver/multi-alpha/evolve-wizard/page.tsx`
  - 修复 `useSearchParams()` 缺少 `Suspense` 导致的 Next build 阻断。

### 4.3 Phase 1 验证结果

| 类型 | 命令或检查 | 结果 |
| --- | --- | --- |
| Python 静态编译 | `python -m py_compile scripts/qrun_limit_minute.py backend/routers/quantevolver.py backend/services/quantevolver/multi_alpha_engine.py backend/services/quantevolver/executors/backtest.py backend/services/quantevolver/qe_evolution_service.py backend/tests/conftest.py` | PASS |
| 目标后端测试 | `python -m pytest backend/tests/unified_engine/test_multi_alpha_command_generation.py backend/tests/unified_engine/test_backtest_executor.py backend/tests/unified_engine/test_ab_comparison.py` | PASS，66 passed |
| unified engine 全量测试 | `python -m pytest backend/tests/unified_engine` | PASS，111 passed |
| 前端生产构建 | `cd frontend && npm run build` | PASS |
| TypeScript 类型检查 | `cd frontend && npx tsc --noEmit` | PASS |
| 前端 lint | `cd frontend && npm run lint` | BLOCKED：当前仓库没有已提交 ESLint 配置，`next lint` 进入交互式初始化提示并以 code 1 退出；这不是本次 Multi-Alpha 代码新增错误，但不能静默忽略。若要求 lint 作为硬 gate，需要先建立全仓 lint baseline 并处理历史 lint debt。 |
| 后端 distributed 阻断 | `POST /api/v1/quantevolver/multi-alpha/auto-select?...&execution_mode=distributed` | PASS，HTTP 400，错误信息显式说明 Phase 1 使用 `serial` |
| 后端 serial auto-select | `POST /api/v1/quantevolver/multi-alpha/auto-select?...&execution_mode=serial` | PASS，返回 `execution_mode: serial` |
| WSL-only Multi-Alpha smoke | `POST /api/v1/quantevolver/experiments/malpha_phase1_smoke_20260426_081239/run?node_id=wsl2-5080` | PASS，最终 `status: completed` |
| 状态接口 | `GET /api/v1/quantevolver/experiments/malpha_phase1_smoke_20260426_081239/run-status` | PASS，返回 `alpha_mode: multi` 和 enhanced metrics |
| 结果接口 | `GET /api/v1/quantevolver/experiments/malpha_phase1_smoke_20260426_081239/multi-alpha/results` | PASS，返回 2 个 group、meta weights、assigned node |
| artifact 检查 | `F:/Dev/RD-Agent-main/qe_workspace/malpha_phase1_smoke_20260426_081239/Loop1` | PASS，存在 `combined_prediction.pkl`、`multi_alpha_results.json`、`qlib_results_enhanced.json`、`qlib_results_llm.json`、`status.txt` |
| Qlib 源码修改检查 | `git diff --name-only -- . | rg -i "(^|/)(qlib|site-packages)(/|$)|site-packages"` | PASS，未发现 Qlib/source/site-packages 修改 |

### 4.4 Phase 1 调试记录

| 问题 | 根因 | 修复 |
| --- | --- | --- |
| `--pred-backtest` 不存在 | runner 只支持 full/train-only/backtest-only | 新增真实 pred-backtest 模式，并保留原模式语义 |
| root unified backtest 缺依赖 | root 文件包复用了 train-only group bundle | root 通过 full backtest compose 重新生成权威 bundle |
| `meta_model_runner.py` 语法错误 | 生成脚本中反斜杠转义错误 | 生成代码改为正确的 `replace("\\", "/")` 语义 |
| `combined_factors_df.parquet` 缺失 | root pred-backtest 前没有运行 root `prepare_factors.py` | meta runner 在统一回测前准备 root factor env |
| `ic_weighted` 在某 group 非正 IC 时失败 | 这是 no-silent-fallback 下的正确 fail-fast，不应自动改权重 | smoke 改用 `equal` 验证执行链路；`ic_weighted` 后续应通过 UI/后端明确提示数据/模型质量问题 |
| PowerShell artifact 检查命令语法错误 | 手工验证命令里 `foreach` 直接接管道写法错误 | 修正命令后重新检查；非业务代码问题 |

### 4.5 Phase 1 状态

- Multi-Alpha WSL 单节点功能链路：已实现并通过 smoke。
- 后端单元/集成目标测试：已通过。
- 前端 build 与 TypeScript：已通过。
- UI/后端 distributed gating：已验证。
- 未修改 Qlib 源代码：已检查。
- `npm run lint`：因仓库缺 ESLint baseline 被阻断，已显式记录；在未建立 lint baseline 前，不应把该结果误判为 Multi-Alpha 修复失败，也不应静默标记为通过。

当前建议：Phase 1 可以进入人工 review；是否进入 Phase 2 取决于团队对 lint baseline 的处理决策。如果要求“所有原计划命令零失败”作为硬门禁，则应先单独修复前端 ESLint baseline，再进入 Phase 2。

## 5. Phase 2 - 单节点硬化与 UI 结果生命周期

### 5.1 进入条件与门禁策略

- Phase 1 WSL-only smoke、后端测试、前端 build/tsc、接口验证已通过。
- 用户已确认采用“阶段模式”继续：本修复链路的前端硬门禁为 `npm run build` + `npx tsc --noEmit` + UI/API 行为验证。
- `npm run lint` 仍作为独立前端质量任务，不静默标记通过。原因是仓库当前缺少已提交 ESLint baseline，直接运行 `next lint` 会进入交互式初始化。

### 5.2 阶段目标

保持 WSL-only 执行，但让结果生命周期足够可靠，避免：

- 后端已标记 completed，但 UI 无法区分 group training、result collection、artifact validation。
- 缺 `multi_alpha_results.json`、group `pred.pkl`、`combined_prediction.pkl`、`qlib_results_enhanced.json` 时被展示为空成功。
- 前端只显示空表格或泛化失败，不显示具体 artifact 错误。

### 5.3 已实施内容

Backend：

- `backend/services/quantevolver/multi_alpha_result_collector.py`
  - 新增 `MultiAlphaArtifactError`。
  - 单节点结果收集前执行 `_validate_single_node_artifacts(...)`。
  - 显式校验 `combined_prediction.pkl`、`multi_alpha_results.json`、`qlib_results_enhanced.json`、每个 retrain group 的 `pred.pkl`。
  - reuse group 必须有 `prediction_path`，否则失败。
  - artifact 缺失或格式错误时抛出明确异常，不进入 DB completed 回写。
  - `collect_and_persist(...)` 返回 `result_metrics`，run-status 可以直接把完整 Multi-Alpha 指标返回给 UI。
- `backend/routers/quantevolver.py`
  - 新增 `_load_multi_alpha_status_payload(...)`，为 UI 输出 `stage`、`artifact_status`、group counts、group rows。
  - 新增 `_mark_multi_alpha_artifact_failure(...)`，artifact 校验失败时将实验置为 `failed`，并把 `multi_alpha_lifecycle` 写入 `result_metrics`，不伪造指标。
  - `/experiments/{id}/run-status` 对 Multi-Alpha 返回 `multi_alpha_stage`、`artifact_status` 和 `multi_alpha` 明细。
  - loop completed 后先进入 `artifact_validation`，collector 成功后才进入 `completed/ready`。
  - collector 失败时进入 `failed_artifact/failed`，错误信息通过 `error` 和 `multi_alpha.artifact_errors` 返回。
  - `/experiments/{id}/multi-alpha/results` 返回 `ready`、`stage`、`artifact_status`、`artifact_errors`、`experiment_status`。
  - 如果实验是 `completed` 但缺 meta weights 或 `multi_alpha_detail`，接口返回 HTTP 409，而不是空成功。

Frontend：

- `frontend/src/app/quantevolver/components/MultiAlphaProgress.tsx`
  - 同时读取 `run-status` 和 `multi-alpha/results`。
  - 展示 group training、result collection、artifact validation、failed artifact 等阶段。
  - 展示 artifact 错误，不再只按 group 数量显示进度。
- `frontend/src/app/quantevolver/components/MultiAlphaResults.tsx`
  - 支持 `ready/stage/artifact_status/artifact_errors`。
  - 未 ready 或 artifact 异常时显示明确提示，不展示空成功表格。
  - ready 后才展示 Meta-Model 权重分布。
- `frontend/src/app/quantevolver/components/useExperimentSSE.ts`
  - run-status 返回 failed 时，把 `error` 或 `multi_alpha.artifact_errors` 写入日志，而不是只显示泛化失败。

### 5.4 Phase 2 验证结果

| 类型 | 命令或检查 | 结果 |
| --- | --- | --- |
| Python 静态编译 | `python -m py_compile backend/services/quantevolver/multi_alpha_result_collector.py backend/routers/quantevolver.py backend/tests/unified_engine/test_multi_alpha_command_generation.py` | PASS |
| Multi-Alpha 目标测试 | `python -m pytest backend/tests/unified_engine/test_multi_alpha_command_generation.py` | PASS，47 passed |
| unified engine 全量测试 | `python -m pytest backend/tests/unified_engine` | PASS，115 passed |
| TypeScript 类型检查 | `cd frontend && npx tsc --noEmit` | PASS |
| 前端生产构建 | `cd frontend && npm run build` | PASS；为避免污染正在运行的 dev server，先停止 3000 dev，清理 `.next`，构建通过后重新启动 dev server |
| 页面 smoke | `GET http://127.0.0.1:3000/quantevolver/compose` 与 `GET http://127.0.0.1:3000/quantevolver/multi-alpha/evolve-wizard` | PASS，均返回 200 |
| completed 结果接口 | `GET /api/v1/quantevolver/experiments/malpha_phase1_smoke_20260426_081239/multi-alpha/results` | PASS，返回 `ready: true`、`stage: completed`、`artifact_status: ready` |
| completed run-status | `GET /api/v1/quantevolver/experiments/malpha_phase1_smoke_20260426_081239/run-status` | PASS，返回 `multi_alpha_stage: completed`、`artifact_status: ready` 和 group counts |
| artifact missing API smoke | 插入临时 synthetic completed multi-alpha 实验但不写 meta weights/detail，再请求 results endpoint | PASS，返回 HTTP 409 和明确 artifact-ready 错误；临时 DB 记录已清理 |
| Phase 2 后 fresh 小数据全流程 smoke | 通过当前代码后端生成并执行 `malpha_phase2_fullflow_smoke_20260426_091140`，配置为 2 个 group、`execution_mode=serial`、`meta_model.method=equal`、2024-01 至 2024-05 小窗口 | PASS，约 170s 完成；`run-status` 返回 `status=completed`、`multi_alpha_stage=completed`、`artifact_status=ready`、`completed_groups=2`、`running_groups=0`；`multi-alpha/results` 返回 `ready=true`、2 个 group、权重 `money_flow=0.5`/`price_volume=0.5` |
| fresh fullflow artifact 检查 | `F:/Dev/RD-Agent-main/qe_workspace/malpha_phase2_fullflow_smoke_20260426_091140/Loop1` | PASS，存在 `combined_prediction.pkl`、`multi_alpha_results.json`、`qlib_results_enhanced.json`、`qlib_results_llm.json`、`status.txt` |
| 8001 UI 后端对接 smoke | 重启 8001 FastAPI 使 UI 使用当前代码后，请求 `malpha_phase2_fullflow_smoke_20260426_091140` 的 `run-status` 与 `multi-alpha/results` | PASS，8001 返回 `completed/ready`，group counts 为 completed=2/running=0；前端 `.env.local` 指向 `http://127.0.0.1:8001/api/v1` |
| 前端页面 smoke | `GET http://127.0.0.1:3000/quantevolver/compose` 与 `GET http://127.0.0.1:3000/quantevolver/multi-alpha/evolve-wizard` | PASS，均返回 200 |
| Qlib 源码修改检查 | `git diff --name-only -- . | rg -i "(^|/)(qlib|site-packages)(/|$)|site-packages"` | PASS，未发现 Qlib/source/site-packages 修改 |
| 旧版执行引擎清理 | 代码检索 `engineMode/setEngineMode/startRun(..., engineMode)`、`engine_mode="legacy"`、Multi-Alpha command fallback | PASS，QE compose/experiments UI 不再暴露旧版引擎选项；`useExperimentSSE` 固定提交 `engine_mode=unified`；后端 `/experiments/{id}/run?engine_mode=legacy` 返回 HTTP 400；Multi-Alpha group command 缺 `wsl_command_core` 时 fail-fast，不再 fallback 到旧 `wsl_command` |
| QE 演进残留旧路径清理 | 检索 `qe_evolution_service.py` 内 `compose_experiment_in_memory(` 与 `create_and_run_loop(`，并新增静态 guard 测试 | PASS，标准自动演进 `submit_next_loop` 与策略演进 `submit_strategy_evo_loop` 均改为 `ExperimentConfig + BacktestExecutor`；跨节点 backtest-only 模型参数同步失败改为 fail-fast；服务层不再直接手工提交旧执行调用 |
| 统一执行层回归测试 | `python -m py_compile ...`；`python -m pytest backend/tests/unified_engine` | PASS，Python 编译通过；统一执行层测试 121 passed |
| 旧版执行引擎移除后 fullflow smoke | 通过当前 8001 后端生成并执行 `malpha_unified_only_smoke_20260426_095311`，配置为 2 个 group、`execution_mode=serial`、`meta_model.method=equal`、2024-01 至 2024-05 小窗口 | PASS，`config/generate` 校验 retrain group 全部存在 `wsl_command_core`；约 185s 完成；`run-status` 返回 `completed/ready`、`completed_groups=2`、`running_groups=0`；`multi-alpha/results` 返回 `ready=true`、2 个 group |
| 前端 lint | `cd frontend && npm run lint` | 仍为独立任务：仓库缺 ESLint baseline，会进入交互式初始化，不作为本阶段硬 gate |

### 5.5 Phase 2 状态

- Phase 2 后端 artifact-ready 检查已实现。
- Phase 2 run-status lifecycle 已实现。
- Phase 2 前端进度与结果展示已适配。
- 已补跑 Phase 2 fresh 小数据 Multi-Alpha 全流程 smoke，并修复一次终态 `run-status` 响应内 group counts 与刚完成收集后的 DB 状态不同步的问题：collector 成功后重新加载 lifecycle/group snapshot，再返回给 UI。
- 已执行统一执行层收口：旧版单次实验执行分支、自定义演进旧分支、标准自动演进手工提交路径、策略演进手工 backtest-only 提交路径均不再可达；UI 不再选择旧版；后端显式拒绝 `engine_mode=legacy`；Multi-Alpha 不再允许缺 `wsl_command_core` 时退回旧命令。
- 阶段模式下的验证门禁已通过。
- 未修改 Qlib 源代码。

当前建议：Phase 2 可以进入人工 review；进入 Phase 3 前需要再次确认两节点环境、CPU/GPU 节点能力与 binary-safe artifact 传输方案。

### 5.6 统一分析层补强 - 2026-04-26

本轮继续分析确认：

- Multi-Alpha 父实验的组合回测结果已通过 `result_metrics.enhanced_metrics` 保存完整 `qlib_results_enhanced.json`，因此 `/api/v1/quantevolver/experiments/{experiment_id}/enhanced-metrics` 可以生成与 QE 单 alpha 实验一致的组合级 IC 曲线、收益曲线、交易诊断、预测诊断、股票明细、因子重要性等分析数据。
- 缺口在于 Multi-Alpha 自身的组级训练/预测诊断未聚合到父实验：组合回测的 `training_diagnostics` 通常为空，因为 root loop 是 `--pred-backtest`，真实训练发生在 `group_*` train-only 子目录。
- 已补齐 compact `multi_alpha_analysis` 契约，避免把完整 group artifact 大对象直接塞进 UI，但保留业务诊断所需数据：
  - `combined_vs_groups`：组合 IC、最佳单组 IC、加权组 IC。
  - `portfolio_diagnostics`：含/不含成本年化收益、成本拖累、最大回撤、换手、Top30 稳定性。
  - `diversification`：权重 HHI、有效组数、主导组、平均/最大组间相关、高相关 pair。
  - `group_diagnostics`：每组 IC/ICIR/Sharpe、权重贡献、训练 loss 摘要、过拟合比、预测 rank turnover、Top30 稳定性、Top 因子重要性。
  - `data_availability`：明确标记组合/组级 enhanced、训练、预测、交易诊断是否存在；缺失项不静默推断。
  - `optimization_guidance`：基于组合回测、组级训练和预测数据生成可追踪证据的优化建议。
- 单节点 collector 现在要求每个 retrain group 产出 `group_{name}/qlib_results_enhanced.json`，缺失则 artifact 校验失败，不把实验标记为成功。
- `/enhanced-metrics` 已透传 `multi_alpha_detail` 和 `multi_alpha_analysis`，单 alpha 实验不受影响；没有这些字段时仍保持原返回结构。
- Multi-Alpha diagnostics API 已返回新增字段，前端诊断页已展示统一分析覆盖率、组合回测诊断、单组训练/预测/因子诊断、以及带证据和演进向导链接的优化指导。

### 5.7 实验 `848c3d74-68a` 阻断修复记录 - 2026-04-26

用户侧完整实验 `848c3d74-68a` 在 `Loop1/meta_model_runner.py` 的 meta 合成阶段失败：

```text
RuntimeError: Group cross_dataset produced NaN IC on 2026-03-09 00:00:00
```

定位结论：

- `cross_dataset` 与 `price_volume` 的 `pred.pkl` 均正常，无预测全空问题。
- `label.pkl` 在 `2026-03-09` 和 `2026-03-10` 两天全为 NaN；这是 future-return label 在测试窗口末尾可能出现的预期数据边界，不是模型训练失败。
- 旧的 meta runner 对 daily Spearman IC 逐日计算时没有先剔除无效 label 行，遇到全 NaN label 日直接得到 NaN IC 并中断。
- 不能把 NaN IC 填 0、不能静默忽略，也不能修改 Qlib 源码；必须显式记录剔除原因，并在有效天数覆盖不足时继续 fail-fast。

已实施修复：

- `backend/services/quantevolver/multi_alpha_engine.py`
  - 生成的 `meta_model_runner.py` 新增 `compute_daily_ic_series(...)`。
  - 每日 IC 计算前对 prediction/label 对齐后 `dropna()`。
  - 对有效样本不足、预测常量、label 常量/全空、Spearman NaN 的日期只做“显式剔除”，并写入 `IC_QUALITY`。
  - 若有效 IC 天数低于 `min(5, total_days * 50%)`，仍然抛出 `RuntimeError`，不产出伪成功。
  - `multi_alpha_results.json` 新增 `ic_quality`，记录 `total_days`、`valid_days`、`skipped_days`、前 20 个 skipped 样本。
- `backend/services/quantevolver/multi_alpha_result_collector.py`
  - 持久化 `ic_quality` 到 `result_metrics.multi_alpha_detail.ic_quality` 与 `multi_alpha_analysis.ic_quality`。
  - 修正组合增强指标读取：单节点 collector 直接读取权威 `qlib_results_enhanced.json`，不再把 `/metrics` 的扁平 summary 当作完整 enhanced payload，确保 Multi-Alpha 父实验具备与单 alpha 一致的收益曲线、交易诊断、预测诊断、股票明细和因子重要性。
- `backend/services/quantevolver/multi_alpha_diagnostics.py`
  - diagnostics 返回 `ic_quality`，供 UI 展示每日 IC 质量覆盖。
- `frontend/src/app/quantevolver/multi-alpha/diagnostics/[expId]/page.tsx`
  - 诊断页新增 IC 质量诊断卡片，展示各上下文的 total/valid/skipped 天数，并明确说明只有 NaN/常量日会被剔除，有效覆盖不足仍失败。

实验级验证结果：

- 已将失败 workspace 的 `meta_model_runner.py` 用同一模板修复后重跑。
- `meta_model_runner.py` 成功通过 meta 合成、生成 `combined_prediction.pkl`、执行统一 `--pred-backtest` 并产出组合 `qlib_results_enhanced.json`。
- `multi_alpha_results.json` 中：
  - `combined_ic = 0.062252`
  - `cross_dataset` 权重 `0.5150073741903002`
  - `price_volume` 权重 `0.48499262580969965`
  - `combined` IC 质量为 `valid_days=407`、`skipped_days=2`
  - skipped 日期为 `2026-03-09`、`2026-03-10`，原因均为 `insufficient_valid_samples_after_dropna`，`raw_samples=4700`、`valid_samples=0`
- 组合增强指标存在完整 `summary`、`return_curves`、`trade_diagnostics`、`prediction_diagnostics`、`all_stocks`、`factor_analysis`、`absolute_returns`。
- collector 已重新持久化实验，DB 状态：
  - `qe_experiments.status = completed`
  - `ic = 0.04829517543378902`
  - `rank_ic = 0.062252318899676494`
  - `annualized_return = 0.058245202471834394`
  - `max_drawdown = -0.1921189407651959`
  - 两个 group 状态均为 `completed`
- API 验证：
  - `/experiments/848c3d74-68a/run-status` 返回 `completed`、`multi_alpha_stage=completed`、`artifact_status=ready`
  - `/experiments/848c3d74-68a/multi-alpha/results` 返回 `ready=true`、2 个 group
  - `/multi-alpha/848c3d74-68a/diagnostics` 返回 `ic_quality`
  - `/experiments/848c3d74-68a/enhanced-metrics` 返回 `multi_alpha_detail`、`multi_alpha_analysis`、收益曲线与交易诊断
- workspace 中保留原始 `error.log`，同时写入 `recovery_status.json` 并将 `status.txt` 更新为 `completed`，避免 UI/log viewer 继续把已修复重跑成功的 loop 显示为失败；原始失败原因未删除。

## 6. Phase 3 - 两节点分布式 Multi-Alpha

### 6.1 进入条件

- Phase 1 和 Phase 2 全部门禁通过。
- distributed feature flag 仍默认关闭。
- 已明确远端 CPU 节点的 node id、能力描述、文件上传/下载协议。

### 6.2 阶段目标

启用两节点训练：

- WSL 负责所有 GPU 类型 group。
- 远端 CPU 节点负责所有 CPU 模型 group。
- unified backtest 在 WSL 或显式 primary node 上执行。

### 6.3 实施范围

Backend：

- 以 `AISTOCK_MULTI_ALPHA_DISTRIBUTED_ENABLED=1` 之类显式开关启用 distributed。
- 替换 CPU round-robin，改为明确 CPU-node selection。
- 保留 GPU group 固定到 WSL 的策略。
- 修复 pickle/parquet 二进制传输，使用 `.b64` 明确后缀或真实 binary upload API。
- 使用单一权威 unified backtest config source。
- group loop 完成后逐个校验 artifact，不允许 node completed 直接等于 group success。
- 记录 group-level failure，避免全局状态被节点级成功掩盖。

Frontend：

- 只有 backend capability/feature flag 表示可用时才重新启用 distributed。
- 提交前展示 node health/capability。
- 展示每个 group 的 node assignment。
- 展示 unified backtest primary node。

### 6.4 验证门禁

Backend：

- planner 测试：GPU group -> WSL，CPU group -> 远端 CPU 节点。
- collector 测试：pickle/parquet 通过 `.b64` 或 binary-safe payload 传输。
- 缺 group prediction 必须阻断 unified backtest。
- node completed 但 artifact missing 不得标记 experiment successful。

Frontend：

- `cd frontend && npm run build`
- `cd frontend && npx tsc --noEmit`
- 若已建立 lint baseline，则同时执行 `cd frontend && npm run lint`。

Manual smoke：

- 两节点 smoke：一个 GPU group，一个 CPU group。
- 确认 group-to-node assignment 符合策略。
- 确认最终 combined result 可在 UI 查询。
- 关闭 feature flag 后，UI 和后端都必须显式拒绝 distributed。

## 7. 当前状态表

| Phase | 状态 | 说明 |
| --- | --- | --- |
| Phase 1 | 功能验证通过，等待 lint baseline 决策/人工 review | WSL-only Multi-Alpha smoke、后端测试、前端 build/tsc、API 结果均通过；`npm run lint` 被仓库缺 ESLint config 阻断 |
| Phase 2 | 阶段模式验证通过，等待人工 review | 已实现 artifact-ready 校验、run-status lifecycle、前端 not-ready/error 展示；lint baseline 仍为独立任务 |
| Phase 3 | 未开始 | 必须等 Phase 2 人工确认后再启用 distributed |

## 7.1 状态自动同步与增强指标修复 - 2026-04-26

问题复盘：

- `qe_20260426_142629` 的 DB 状态已是 `completed`，但增强指标页面报“增强指标不可用”。根因不是实验未生成结果，而是 `qlib_results_enhanced.json` 生成在 task root：`F:/Dev/RD-Agent-main/qe_workspace/qe_20260426_142629/qlib_results_enhanced.json`；DB 只保存了 summary metrics，且 RDAgent 9000 不运行时，AIstock 原增强指标接口无法从本地 artifact 回退读取。
- `848c3d74-68a` 的 Multi-Alpha 实验已恢复为 `completed/ready`，但 `run.log` 保留了旧的 NaN IC 失败栈；日志末尾有 `[RECOVERY] ... rerun succeeded ... DB status persisted completed`。UI 若只看旧日志片段，会误判为仍失败。
- 更深层原因是状态同步依赖前端/SSE 触发；如果页面打开但 SSE 没有终态事件、后端重启、RDAgent callback 配置为 base URL 或 9000 停止，DB 和 UI 就可能不同步。

已实施修复：

- 新增 `backend/services/quantevolver/qe_experiment_status_scanner.py`，FastAPI 启动后默认每 30 秒扫描 `qe_experiments.status='running'`，对单 alpha 和 Multi-Alpha 统一调用 run-status 同步逻辑，保证无前端页面时也能收敛到 completed/failed/interrupted。
- 新增 `backend/services/quantevolver/callback_urls.py`，把 `infra.compute_nodes.callback_url` 的 base URL 展开为真实 webhook endpoint；WSL 默认使用 `http://127.0.0.1:8001/api/v1/quantevolver/webhook/loop-completed`，避免旧配置的 8000/base URL 导致 callback 无效。
- 单次 QE 新增 `/api/v1/quantevolver/webhook/loop-completed`，RDAgent callback 只触发同步，不直接信任 callback 标记 completed。
- QE 演进任务 callback URL 也改为完整路径 `/api/v1/quantevolver/evolution/webhook/loop-completed`。
- `get_experiment_enhanced_metrics` 增加本地 artifact fallback：优先 DB enhanced metrics，其次读取 `workspace_path`/`QE_WORKSPACE_WIN` 下的 `qlib_results_enhanced.json`，最后才请求 RDAgent。
- `stream_experiment_logs` 在 RDAgent log stream 不可用时读取本地 `run.log`，并在流末尾追加 `[System] AIstock authoritative final status: ...`，避免旧失败栈掩盖恢复后的权威终态。
- 前端 `useExperimentSSE` 在日志连接期间额外每 10 秒主动调用 `run-status`，不再只依赖 SSE error 才触发终态同步。
- 前端实验历史页加载 running 实验后自动批量触发 `run-status`，打开历史页即可修复卡住的 running 状态。

验证结果：

- `python -m py_compile backend/main.py backend/routers/quantevolver.py backend/services/quantevolver/callback_urls.py backend/services/quantevolver/qe_experiment_status_scanner.py backend/services/quantevolver/qe_evolution_service.py`：PASS。
- `python -m pytest backend/tests/unified_engine -q`：PASS，127 passed。
- `cd frontend && npx tsc --noEmit`：PASS。
- 重启 8001 后验证 `qe_20260426_142629/enhanced-metrics`：HTTP 200，返回 672,816 bytes，包含 407 个 IC 点、404 个收益点、1,452 条股票明细。
- 验证 `848c3d74-68a/run-status`：`status=completed`、`multi_alpha_stage=completed`、`artifact_status=ready`、2 个 group 均 completed。
- 验证 `848c3d74-68a/logs`：流末尾包含 `[RECOVERY] ... rerun succeeded ...` 和 `[System] AIstock authoritative final status: completed`。

## 8. 新窗口恢复步骤

1. 进入 `F:\Dev\AIstock`。
2. 读取 `AGENTS.md`。
3. 读取 `docs/codex_project_memory.md`。
4. 读取本文档 `docs/architecture/multi_alpha_qe_repair_plan.md`。
5. 执行 `git status --short`，注意不要回滚无关脏文件。
6. 从第一个未完成 phase 继续。
7. 若继续 Phase 3，先确认 Phase 2 人工 review 已通过，并重新确认两节点能力、binary-safe artifact 传输方案与 distributed feature flag。
8. 每个阶段失败后先修复失败原因，再重新跑验证；不要无改动重复运行失败命令。

## 9. 本次 Phase 1/2 相关改动文件

- `docs/architecture/multi_alpha_qe_repair_plan.md`
- `scripts/qrun_limit_minute.py`
- `backend/routers/quantevolver.py`
- `backend/services/quantevolver/multi_alpha_engine.py`
- `backend/services/quantevolver/multi_alpha_diagnostics.py`
- `backend/services/quantevolver/multi_alpha_result_collector.py`
- `backend/services/quantevolver/executors/backtest.py`
- `backend/services/quantevolver/qe_evolution_service.py`
- `backend/tests/conftest.py`
- `backend/tests/unified_engine/test_multi_alpha_command_generation.py`
- `frontend/src/app/quantevolver/components/MultiAlphaGroupEditor.tsx`
- `frontend/src/app/quantevolver/components/MultiAlphaProgress.tsx`
- `frontend/src/app/quantevolver/components/MultiAlphaResults.tsx`
- `frontend/src/app/quantevolver/components/useExperimentSSE.ts`
- `frontend/src/app/quantevolver/compose/page.tsx`
- `frontend/src/app/quantevolver/multi-alpha/diagnostics/[expId]/page.tsx`
- `frontend/src/app/quantevolver/multi-alpha/evolve-wizard/page.tsx`
