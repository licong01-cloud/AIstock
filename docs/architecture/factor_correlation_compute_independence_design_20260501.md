# 因子相关性计算独立化设计

日期：2026-05-01

## 背景

因子相关性计算通过 `infra.dispatch_tasks` 提交到 WSL / RD-Agent 节点执行，远端入口为：

```text
backend/scripts/run_correlation_compute_wsl.py
```

历史实现为了复用相关性计算函数，让 WSL runner 导入 `backend.routers.quantevolver_evolution`。该 router 同时承载 QE 演进 API，顶部会导入 `qe_evolution_service`。当 QE 演进服务新增导出符号后，runner 内部为轻量导入而伪造的 `qe_evolution_service` stub 不再完整，导致相关性计算在模块导入阶段失败，尚未进入缓存检查或 Spearman/EWMA 矩阵计算。

## 问题链路

```text
run_correlation_compute_wsl.py
  -> backend.routers.quantevolver_evolution
      -> backend.services.quantevolver.qe_evolution_service
          -> QE evolution symbols
```

这个链路把「因子相关性计算」和「QE 演进 API/router 顶层导入」耦合在一起，风险包括：

- QE router 顶层 import 变化会破坏相关性 runner。
- runner import 失败时无法进入 `main()`，远端结果缺少结构化 error。
- 前端只能显示泛化错误 `dispatch task failed: failed`。
- 相关性计算的业务边界不清晰，不利于后续独立指标、缓存、相关性任务稳定运行。

## 目标架构

第一阶段不新增网络服务和端口，只新增独立 Python service 模块：

```text
run_correlation_compute_wsl.py
  -> backend.services.quantevolver.correlation_compute_service
      -> FactorValuePipeline / FactorValueLoader / CorrelationEngine
      -> qe_factor_correlations / qe_correlation_metadata
```

`quantevolver_evolution.py` 保留 API、dispatch、页面查询职责；本地相关性计算权威实现迁移到 service。WSL runner 不再导入 FastAPI router，也不再伪造 QE service stub。

## 代码边界

| 模块 | 职责 |
|---|---|
| `backend/services/quantevolver/correlation_compute_service.py` | 相关性计算本地权威实现、缓存完整性检查、矩阵计算、DB 写入、结构化进度事件 |
| `backend/scripts/run_correlation_compute_wsl.py` | WSL custom task runner，只解析 payload、绑定事件输出、调用独立 service |
| `backend/routers/quantevolver_evolution.py` | API、dispatch 提交、状态/日志/矩阵查询；本地计算函数名委托到独立 service |
| `backend/services/quantevolver/correlation_engine.py` | Spearman + EWMA 矩阵算法，不依赖 QE router |
| `backend/services/quantevolver/factor_value_pipeline.py` | 独立指标 single parquet 缓存与 `_meta.json` 权威性校验 |

## 非目标

- 不新增独立 server、端口、常驻 worker。
- 不修改 QE 演进业务逻辑。
- 不修改因子、模型、策略包、RD-Agent/QE workspace 等资产文件。
- 不改变 Spearman + EWMA 相关性算法。
- 不把缺失缓存、跨快照、DB 写入异常做静默兜底。

## 错误处理要求

- runner 的 service import 放在 `main()` 的 `try` 内，确保 import 失败也能输出结构化 JSON result。
- 相关性 service 仍沿用 fail-fast：
  - `_meta.json` 权威性自检失败立即停止；
  - 请求 `as_of_date` 与缓存 `as_of_date` 不一致立即停止；
  - DB 清空、写入、缓存读取异常立即返回失败；
  - HDF5 / single parquet 损坏不静默跳过。
- dispatch 层仍会优先读取 `latest_result.error`，若远端 runner 返回结构化失败，UI 可以看到真实错误。

## 验证方案

| 层级 | 验证内容 | 预期 |
|---|---|---|
| 静态边界 | runner 不包含 `backend.routers.quantevolver_evolution` 或 `qe_evolution_service` | 相关性 runner 与 QE router 解耦 |
| 静态边界 | service 不包含 `qe_evolution_service`、`AutoEvolutionScheduler`、`APIRouter` | service 不依赖 QE 演进或 FastAPI router |
| runner 启动 | `python backend/scripts/run_correlation_compute_wsl.py` 无参数执行 | 返回结构化 usage JSON，不出现 ImportError |
| service 单元 | monkeypatch DB/缓存/矩阵引擎执行两因子最小成功路径 | 返回 success，record_count=1，且不触碰真实 DB/cache |
| 远端联调 | 通过 UI 或 API 提交相关性 dispatch | 远端任务不再在 import 阶段失败，进入 Phase0/缓存/矩阵流程 |

## 后续建议

后续可以继续把 router 内历史遗留的相关性本地计算副本删除，所有相关性计算只保留 service 一份权威实现。删除前需要确认没有外部代码直接引用 router 私有函数。
