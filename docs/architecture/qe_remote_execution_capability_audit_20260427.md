# QE 远端执行能力检查与整改方案（2026-04-27）

> 目的：记录当前多周期策略、QE 单次实验、自定义演进任务、V25 日内执行策略对远端机运行的支持状态，方便后续窗口中断后继续整改。
> 范围：仅基于 AIstock 代码与当前节点配置进行分析；本文档不修改 Qlib 源码，不代表已完成整改。

## 1. 总体结论

当前不能认为 QE 全链路已经完整支持远端机运行。

已经具备的基础能力：

- `infra.compute_nodes` 已配置 WSL 节点与远端节点。
- QE 后端统一执行层已经可以接收 `node_id` 并按节点生成 workspace 路径。
- 自定义演进任务 UI 与后端已经有执行节点选择与远端提交路径。
- 多周期训练标签 `label_horizon=1/3/5/10` 已接入统一配置层。

主要阻断：

- QE 单次实验前端没有执行节点选择，也不会把 `node_id` 传给 `/experiments/{id}/run`。
- V25 日内执行策略尚未接入 QE/Qlib `NestedExecutor`，当前在 QE 中选择 V25 存在静默落回默认 TailTWAP 的风险。
- 远端 callback URL 当前不可达，远端任务完成后不能可靠主动通知 AIstock。
- filtered_pool/行业黑名单文件同步在部分路径中不是 fail-fast，自定义演进 loop 级股票池同步也不完整。
- Multi-Alpha 分布式仍处于 Phase 3 禁用状态，当前 planner 即使开启也不满足“WSL 跑 GPU、远端跑 CPU”的目标。

## 2. 支持状态矩阵

| 模块 | 当前支持远端机程度 | 结论 |
| --- | --- | --- |
| 多周期策略 / `label_horizon=1/3/5/10` | 部分支持 | 配置层支持；是否能远端运行取决于入口是否传 `node_id` |
| QE 单次实验 | 后端支持，UI 不支持 | 可通过 API 手动传 `node_id`；UI 一键回测无法选择远端节点 |
| 自定义演进任务 | 基本支持，但不够硬 | UI/后端支持远端节点；callback、股票池同步、执行算法校验仍有缺口 |
| 标准自动演进 / 策略演进 | 部分支持 | 后端/UI 有 `node_id` 路径；标准演进明确不允许 V25 |
| V25 日内执行策略 | 不支持 QE 权威回测 | 有算法注册与模型路径配置，但没有 QE/Qlib inner strategy 包装与脚本打包 |
| Multi-Alpha 分布式负载均衡 | 不支持 | 功能开关禁用；当前 planner 不满足 GPU/CPU 分流目标 |

## 3. 当前节点配置与风险

上次检查到的节点配置：

```text
rdagent-node1
- api_base_url: http://192.168.50.215:9000
- status: busy
- gpu_model: RTX 2060
- workspace_base: /home/lc999/projects/RD-Agent-main/qe_workspace
- factor_data_dir: /home/lc999/data/factor_data
- qlib_data_path: /home/lc999/data/qlib_bin
- qlib_minute_path: /home/lc999/data/qlib_minute_bin
- qlib_rdagent_root: /home/lc999/projects/RD-Agent-main
- callback_url: http://192.168.50.14:8000

wsl2-5080
- api_base_url: http://127.0.0.1:9000
- status: busy
- gpu_model: RTX 5080
- workspace_base: /mnt/f/Dev/RD-Agent-main/qe_workspace
- factor_data_dir: /mnt/f/dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data
- qlib_data_path: /home/lc999/data/qlib_bin
- qlib_minute_path: /home/lc999/data/qlib_minute_bin
- qlib_rdagent_root: /mnt/f/Dev/RD-Agent-main
- callback_url: http://192.168.50.14:8000
```

健康检查记录：

- `http://127.0.0.1:9000/health` 返回 200。
- `http://192.168.50.215:9000/health` 返回 200。
- `http://192.168.50.14:8000` 上次检查无监听。
- `http://192.168.50.14:8001` 上次检查也不可从 LAN 地址访问；FastAPI 仅监听 `127.0.0.1:8001`。

风险：

- 远端节点无法主动调用 AIstock callback，只能依赖 scanner 主动轮询。
- 演进任务状态更新可能延迟，或在 scanner 失败时长期停留 running。
- 后续整改前必须重新实测端口、节点健康、callback 可达性。

## 4. 代码证据

### 4.1 QE 单次实验

后端支持 `node_id`：

- `backend/routers/quantevolver.py:5390`：`run_experiment(experiment_id, engine_mode="unified", node_id=None)`。
- `backend/routers/quantevolver.py:5698`：`_run_experiment_unified(experiment_id, node_id=None)`。
- `backend/routers/quantevolver.py:5729`：创建 `ExecutionContext(node_id=node_id, callback_url=...)`。
- `backend/routers/quantevolver.py:5737`：有 `node_id` 时使用 `QEWorkspaceClient.for_node(node_id)`。
- `backend/routers/quantevolver.py:5749`：提交后把 `execution_node_id` 写入 `custom_params`。

统一执行层向配置生成透传 `node_id`：

- `backend/services/quantevolver/executors/backtest.py:68`：调用 `compose_experiment_in_memory(...)`。
- `backend/services/quantevolver/executors/backtest.py:79`：传入 `node_id=ctx.node_id`。

远端 workspace 路径读取：

- `backend/services/quantevolver/config_composer.py:150`：`_fetch_workspace_config(node_id)`。
- `backend/services/quantevolver/config_composer.py:171`：`_get_node_paths(node_id)`。
- 缺 `workspace_base/factor_data_dir/qlib_data_path/qlib_minute_path/qlib_rdagent_root` 会 fail-fast。

前端缺口：

- `frontend/src/app/quantevolver/components/useExperimentSSE.ts:270`：固定请求 `/run?engine_mode=unified`，没有 `node_id`。
- `frontend/src/app/quantevolver/compose/page.tsx` 和 `frontend/src/app/quantevolver/experiments/page.tsx` 调用 `sse.startRun(...)`，目前没有传节点参数。

结论：后端可远端，UI 不可远端。

### 4.2 自定义演进任务

UI 支持选择远端节点：

- `frontend/src/app/quantevolver/evolution/page.tsx:906`：创建自定义演进任务。
- `frontend/src/app/quantevolver/evolution/page.tsx:914`：提交 `node_id: customEvoNodeId || undefined`。
- `frontend/src/app/quantevolver/evolution/page.tsx:3123`：自定义演进执行节点选择框。

后端支持远端节点：

- `backend/routers/quantevolver_evolution.py:862`：`CustomEvolutionCreateRequest` 含 `node_id`。
- `backend/routers/quantevolver_evolution.py:943`：校验节点存在且不离线。
- `backend/services/quantevolver/qe_evolution_service.py:3678`：创建任务时持久化 `node_id`。
- `backend/services/quantevolver/qe_evolution_service.py:35`：`_get_workspace_client_for_task()` 根据 task 的 `node_id` 返回对应节点 client。
- `backend/services/quantevolver/qe_evolution_service.py:3822`：自定义 loop 使用统一执行层提交。
- `backend/services/quantevolver/qe_evolution_service.py:3830`：`ExecutionContext(node_id=task.get("node_id"))`。

缺口：

- callback URL 对远端不可达。
- loop 级 `stock_pool` 同步没有完整 fail-fast 保障。
- 自定义演进的执行算法选项来自全量算法目录，可能包含 QE 尚未支持的 V25。

### 4.3 标准自动演进与策略演进

标准自动演进：

- `frontend/src/app/quantevolver/evolution/page.tsx:960`：提交 `node_id: newTask.node_id || undefined`。
- `backend/routers/quantevolver_evolution.py:145`：`EvolutionTaskCreateRequest` 含 `node_id`。
- `backend/routers/quantevolver_evolution.py:256`：校验节点。

策略演进：

- `backend/routers/quantevolver_evolution.py:756`：`StrategyEvolutionForkRequest` 含 `node_id`。
- `backend/services/quantevolver/qe_evolution_service.py:3260` 附近：策略演进使用统一执行层与 `node_id`。
- 跨节点 backtest-only 有 mlruns 同步逻辑，失败应 fail-fast。

V25 限制：

- `backend/routers/quantevolver_evolution.py:293`：标准演进只允许 `TWAP/VWAP/CLOSE_PRICE/V24_PLAN`，不允许 `V25_TWO_STAGE`。

### 4.4 多周期策略 / Label Horizon

统一配置层支持：

- `backend/services/quantevolver/experiment_config.py:13`：允许 `1/3/5/10`。
- `backend/services/quantevolver/experiment_config.py:17`：`normalize_label_horizon(...)` 非法值 fail-fast。
- `backend/services/quantevolver/experiment_config.py:185`：非 1d 时写入 `custom_params.label_horizon`。
- `backend/services/quantevolver/config_composer.py:1706`：生成 Qlib label 前校验 horizon。
- `backend/services/quantevolver/config_composer.py:1708`：label 公式按 horizon 生成。

UI 支持：

- `frontend/src/app/quantevolver/compose/page.tsx:383`：加载历史实验 `label_horizon`。
- `frontend/src/app/quantevolver/compose/page.tsx:553`：非 1d 写入 `label_horizon`。
- `frontend/src/app/quantevolver/evolution/page.tsx:2954`：自定义演进 loop 支持 `1/3/5/10d`。

注意：

- `label_horizon` 是训练标签期限。
- `hold_thresh` 是策略最短持仓/卖出门槛。
- 两者相关但不是同一字段，不能后台静默替用户修改。

### 4.5 V25 日内执行策略

算法目录中存在 V25：

- DB 中 `execution_algorithm_catalog` 有 `V25_TWO_STAGE`，且上次检查 `is_enabled=true`。
- `backend/execution_algos/v25_two_stage_algo.py:23`：注册 `V25TwoStageAlgo`。
- `rl_execution/executor/v25_two_stage_executor.py`：存在 V25 executor。

但 QE/Qlib 回测未接入：

- `backend/services/quantevolver/config_composer.py:1845`：只判断 `CLOSE_PRICE`。
- `backend/services/quantevolver/config_composer.py:1858`：只特别处理 `V24_PLAN`。
- `backend/services/quantevolver/config_composer.py:1873`：其他执行算法全部走默认 `TailTWAPWithLimitStrategy`。
- `backend/services/quantevolver/config_composer.py:697`：分钟线脚本只打包 `qrun_limit_minute.py`、`tail_twap_strategy.py`、`tail_twap_v24_strategy.py`。
- 仓库当前没有 `scripts/tail_twap_v25_strategy.py` 或等价 Qlib inner strategy。

结论：

- V25 目前只能说存在独立执行算法实现，不能说 QE 单次实验/演进任务已经能权威回测 V25。
- 当前若 UI 允许在 QE 中选择 `V25_TWO_STAGE`，应先阻断或隐藏，避免静默落回默认 TailTWAP。

### 4.6 Multi-Alpha 分布式

当前分布式被禁用：

- `backend/routers/quantevolver.py:85`：`AISTOCK_MULTI_ALPHA_DISTRIBUTED_ENABLED` 功能开关。
- `backend/routers/quantevolver.py:103`：未启用时拒绝 `execution_mode='distributed'`。
- `frontend/src/app/quantevolver/components/MultiAlphaGroupEditor.tsx:846`：分布式选项前端禁用。
- `frontend/src/app/quantevolver/compose/page.tsx:605`：生成配置前阻断未启用的 distributed。

planner 缺口：

- `backend/services/quantevolver/multi_alpha_resource_planner.py:91`：`_plan_distributed(...)`。
- `backend/services/quantevolver/multi_alpha_resource_planner.py:111`：GPU 组分配给最大 VRAM 节点。
- `backend/services/quantevolver/multi_alpha_resource_planner.py:119`：CPU 组 round-robin 到所有 active nodes。

这不满足目标：

- 目标是 WSL 承担所有 GPU 类型训练。
- 远端机承担所有 CPU 模型训练。
- 当前逻辑会把 CPU 组分配到 WSL 或 GPU 节点，不能直接开启。

## 5. 必须整改的问题清单

### P0：先阻断静默错误

目标：不允许 UI 显示可用但后端实际落回默认策略。

任务：

- 在 QE 配置生成和 run 入口校验 `execution_algo`。
- 明确允许 QE/Qlib 当前真实支持的算法：建议先只允许 `TWAP`、`VWAP`、`CLOSE_PRICE`、`V24_PLAN` 或空默认。
- 对 `V25_TWO_STAGE`、`AC_OPTIMAL`、`POV`、`SBB_EMA` 等未接入 Qlib inner strategy 的算法返回明确 400。
- 前端 QE compose/evolution 算法下拉仅展示 QE 支持算法，或对未支持算法显示“暂不可用于 QE 回测”并禁用。
- 添加测试覆盖：选择 `V25_TWO_STAGE` 生成 QE 配置必须失败，不能生成 TailTWAP。

验收：

- 选择 V25 做 QE 单次实验时得到明确错误。
- 选择 V25 做自定义演进时得到明确错误。
- 默认/`TWAP`/`VWAP`/`CLOSE_PRICE`/`V24_PLAN` 既有实验不受影响。

### P1：补齐 QE 单次实验 UI 远端运行

目标：QE 单次实验从 UI 可选择远端节点并正确提交。

任务：

- 在 QE compose 的执行步骤或运行按钮附近增加执行节点选择。
- 在实验历史页重试/一键执行时允许选择节点，或继承实验 `custom_params.execution_node_id`。
- 修改 `useExperimentSSE.startRun(...)` 支持可选 `nodeId`。
- 前端请求 `/experiments/{id}/run?engine_mode=unified&node_id=...`。
- 后端已能持久化 `execution_node_id`，但需要确认 UI 展示实际执行节点。

验收：

- UI 创建单次 QE 实验，选择 `rdagent-node1`，提交后 DB `custom_params.execution_node_id=rdagent-node1`。
- `/run-status` 能从远端节点查询状态。
- `/logs` 能从远端节点拉取日志。
- 本地默认不选节点时，单节点 WSL 既有行为不变。

### P2：修复远端 callback 与状态收敛

目标：远端任务完成后能主动通知 AIstock；scanner 只作为兜底。

任务：

- 让 FastAPI 监听可被远端访问的地址，例如 `0.0.0.0:8001`，或提供反向代理。
- 更新 `infra.compute_nodes.callback_url` 为远端可达的 AIstock base URL，例如 `http://<AIstock-LAN-IP>:8001`。
- 或设置环境变量 `AISTOCK_QE_CALLBACK_BASE_URL` / `AISTOCK_BACKEND_CALLBACK_BASE_URL`。
- 保留 scanner：`QE_EXPERIMENT_SCAN_INTERVAL_SEC` 与 `QE_EVOLUTION_SCAN_INTERVAL_SEC` 继续作为兜底。
- 增加 callback 连通性检查接口或启动时告警。

验收：

- 从远端机能访问 `http://<AIstock-LAN-IP>:8001/api/v1/...`。
- 远端 QE 单次实验完成后，不依赖手动打开 UI，也能自动更新 completed/failed。
- callback 失败时 scanner 仍能在约定时间内收敛状态。

### P3：远端股票池与行业黑名单同步 fail-fast

目标：远端节点运行时，实验使用的股票池文件必须真实存在于目标节点。

任务：

- 统一处理单次 QE、标准演进、自定义演进、Multi-Alpha 的 `stock_pool`。
- 只要 `stock_pool` 指向 `filtered_pool_*.txt`，提交远端前必须同步到目标节点。
- 同步失败必须返回错误，不能仅 warning。
- 自定义演进每个 loop 的 `stock_pool` 都要检查。
- 记录同步目标路径与文件 checksum，便于追溯行业黑名单配置。

验收：

- 远端节点缺少 filtered_pool 时，提交前自动同步。
- 同步失败时任务不进入 running。
- 实验详情能显示实际使用的行业黑名单/股票池配置。

### P4：V25 QE 接入（单独阶段）

目标：V25 在 QE 中成为真正的 Qlib minute backtest inner strategy，而不是独立算法目录里的孤立实现。

任务：

- 新增 Qlib `NestedExecutor` 可用的 V25 inner strategy，例如 `scripts/tail_twap_v25_strategy.py`。
- 明确 V25 需要的市场上下文：全日 minute OHLCV、prev_close、limit 信息、模型路径、device。
- 在 `ConfigComposer` 中增加 `V25_TWO_STAGE` 分支。
- 打包 V25 strategy 脚本、依赖脚本、模型路径配置。
- 先在 WSL 节点做小数据量 V25 QE 回测。
- 再验证远端节点是否有 Torch、模型文件、CUDA/CPU device 配置。

验收：

- 选择 `V25_TWO_STAGE` 时 conf.yaml 明确生成 V25 inner strategy。
- 如果模型文件不存在、Torch 不可用、device 不可用，必须 fail-fast。
- V25 结果与默认 TailTWAP 结果可区分，并有日志证明实际执行了 V25。

### P5：Multi-Alpha 分布式负载均衡

目标：实现设计目标：WSL 承担所有 GPU 训练，远端机承担所有 CPU 模型训练。

任务：

- 修改 `multi_alpha_resource_planner.py`：
  - GPU group 固定分配到 `wsl2-5080` 或显式 GPU 主节点。
  - CPU group 固定分配到远端 CPU 节点池，不 round-robin 到 WSL。
  - 节点能力不足时 fail-fast。
- 明确 CPU/GPU 模型分类来源：模型 catalog、group config 或手工标记。
- 完成 binary-safe artifact 传输与结果收集。
- 先 2 组小数据 smoke，再扩大到真实多 alpha 实验。

验收：

- 生成 plan 时能看到 GPU groups -> WSL，CPU groups -> remote。
- 分布式实验每个 group 的 `assigned_node_id` 与预期一致。
- root meta backtest 能收集远端 pred/model artifacts。
- UI 能展示每个 group 的节点、状态、artifact 校验结果。

## 6. 后续整改推荐顺序

建议按以下顺序推进，不能跳阶段：

1. P0：阻断未支持执行算法的静默 fallback。
2. P1：补齐 QE 单次实验 UI 远端节点选择。
3. P2：修复 callback 可达性与状态自动更新。
4. P3：远端股票池/行业黑名单同步 fail-fast。
5. P4：单独实现 V25 QE 接入。
6. P5：最后实现 Multi-Alpha 两节点分布式负载均衡。

原因：

- P0 是安全底线，防止产生假回测结果。
- P1/P2/P3 是所有远端任务的基础设施。
- V25 涉及 Qlib inner strategy、Torch、模型文件与分钟数据，必须单独验证。
- Multi-Alpha 分布式依赖远端任务、artifact 传输、状态收敛全部稳定后才能做。

## 7. 回归测试门禁

每个阶段完成后必须至少执行：

```powershell
python -m py_compile backend/routers/quantevolver.py backend/routers/quantevolver_evolution.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/qe_evolution_service.py
python -m pytest backend/tests/unified_engine -q
cd frontend
npx tsc --noEmit
npm run build
```

远端相关阶段必须额外验证：

```powershell
# 节点健康
python -X utf8 - <<'PY'
import asyncio, httpx
nodes=[('wsl2-5080','http://127.0.0.1:9000'),('rdagent-node1','http://192.168.50.215:9000')]
async def main():
    async with httpx.AsyncClient(timeout=5.0, proxy=None) as c:
        for nid, base in nodes:
            try:
                r=await c.get(base + '/health')
                print(nid, r.status_code, r.text[:200])
            except Exception as e:
                print(nid, type(e).__name__, e)
asyncio.run(main())
PY
```

需要人工或脚本验证：

- UI 能选择远端节点并提交。
- DB 写入 `execution_node_id` 或 evolution task `node_id`。
- `/run-status` 能自动更新 terminal 状态。
- `/logs` 能读取目标节点日志。
- 远端回测 artifact 存在且 enhanced metrics 可展示。
- 失败时 UI 显示明确错误，不展示空成功。

## 8. 禁止事项

后续整改必须遵守：

- 禁止修改 Qlib 源码或 site-packages。
- 禁止把错误静默降级为默认执行策略。
- 禁止 V25 未接入 QE 时在 UI 中显示为可用于 QE 权威回测。
- 禁止 callback 失败时伪造 completed。
- 禁止股票池同步失败后继续提交远端任务。
- 禁止为了让测试通过而简化业务逻辑或跳过真实 artifact 校验。
- 禁止破坏 WSL 单节点 QE 单次实验和演进实验既有行为。

## 9. 下一步可执行任务

建议下一窗口从 P0 开始：

1. 定义 QE 支持的 execution algorithm 白名单。
2. 后端 config generate/run/custom evolution 统一校验 execution algorithm。
3. 前端 QE compose/evolution 下拉按 QE 支持状态过滤或禁用。
4. 添加回归测试：`V25_TWO_STAGE` 在 QE 中必须明确失败。
5. 验证默认 TailTWAP、`CLOSE_PRICE`、`V24_PLAN` 仍可生成配置。
