# QE 单 Alpha 自定义 Loop 分节点执行设计方案（2026-04-28）

> 状态：已按本方案完成第一版核心实现；需在真实远端节点上执行端到端运行验收。  
> 目标：在不引入静默兜底、不改变业务语义的前提下，让 QE 单 Alpha 的单次实验与自定义演进任务具备可审计、可回收结果、可手工分配节点的分布式执行能力。  
> 重点：每个 custom loop 可手动指定执行节点；默认继承 Loop 1 节点；busy 节点允许提交；offline/API 不可达必须拒绝；每节点并行度默认 1、最大 4；重跑固定使用上次节点。

---

## 1. 范围与非目标

### 1.1 本方案覆盖

1. QE 单 Alpha 单次实验（一键运行 / 历史重跑）的远端运行硬化。
2. QE 自定义演进任务（custom_evo）按 loop 手动选择节点。
3. custom_evo 并行执行时，按节点独立限流：每节点默认并行度 1，最大 4。
4. custom_evo 多 loop 结果仍统一回写 AIstock，用于任务内横向对比、SOTA、详情页、增强指标、资产同步。
5. retry / rerun 行为固定使用历史记录中的执行节点，不开放节点修改入口。
6. no-silent-fallback 审计：远端 API、节点状态、workspace、callback、metrics、cross-node model_source 均必须显式成功或显式失败。

### 1.2 本方案不覆盖

1. Multi-Alpha 分布式 planner 的重新设计；本文只把 Multi-Alpha 已有按组分发模式作为参考。
2. V25_TWO_STAGE 接入 Qlib inner strategy；如果 QE 当前仍不能权威回测 V25，应继续 fail-fast，而不是落回 TailTWAP。
3. 自动资源调度 / 自动选择最佳节点；用户已明确远端 GPU 较弱，需要按模型和算力人工配置。
4. 修改 Qlib 源码、site-packages、历史实验资产或已训练模型产物。

---

## 2. 已确认需求

1. **Loop 节点选择**：每个 custom loop 可以手动指定节点；默认设置为继承第一个 loop 使用的节点。
2. **节点可用性规则**：`busy` 节点允许提交；`offline` 节点和 API 不可达节点必须在提交前拒绝。
3. **节点并行度规则**：每个节点都可以设置并行度；默认 `1`；最大 `4`；自定义任务表单最下方允许为每个被选中的节点配置专门并行度。
4. **重跑规则**：重跑必须使用上次的节点，不允许修改节点。
5. **结果回收规则**：每个 loop 的结果数据必须准确回到 AIstock，支持多个 loop 之间横向对比。
6. **错误处理规则**：严禁为了“走通流程”增加静默兜底；错误必须暴露为明确失败、可追踪上下文和可复现日志。

---

## 3. 当前代码能力与差距

### 3.1 单次 QE 实验：已有“单目标节点”能力，但远端链路仍不完整

已有能力：

- `backend/routers/quantevolver.py:5523` 的 `/experiments/{experiment_id}/run` 已接收 `node_id`。
- `backend/routers/quantevolver.py:5830` `_run_experiment_unified(...)` 进入统一执行层。
- `backend/routers/quantevolver.py:5861` 构建 `ExecutionContext(node_id=node_id, callback_url=...)`。
- `backend/routers/quantevolver.py:5869` 有 `node_id` 时使用 `QEWorkspaceClient.for_node(node_id)`。
- `backend/services/quantevolver/executors/backtest.py:67` 在 `ctx.node_id` 存在时同步 stock pool 到目标节点。
- `backend/services/quantevolver/executors/backtest.py:76` 调用 `compose_experiment_in_memory(...)`。
- `backend/services/quantevolver/executors/backtest.py:87` 将 `node_id=ctx.node_id` 透传给配置层。
- `backend/services/quantevolver/config_composer.py:172` `_fetch_workspace_config(node_id)` 支持按节点读取路径。
- `backend/services/quantevolver/config_composer.py:193` `_get_node_paths(node_id)` 对缺失节点路径 fail-fast。
- `frontend/src/app/quantevolver/compose/page.tsx:224` 已有 `executionNodeId` 状态。
- `frontend/src/app/quantevolver/compose/page.tsx:327` 已加载 `/dispatch/nodes`。
- `frontend/src/app/quantevolver/compose/page.tsx:1845` 已有运行节点选择。
- `frontend/src/app/quantevolver/compose/page.tsx:1856` 调用 `sse.startRun(experiment_id, executionNodeId || undefined)`。
- `frontend/src/app/quantevolver/components/useExperimentSSE.ts:270` `startRun` 支持可选 `nodeId`，并在 `:280` 写入 query param。
- `frontend/src/app/quantevolver/experiments/page.tsx:62` 历史页读取 `custom_params.execution_node_id`。
- `frontend/src/app/quantevolver/experiments/page.tsx:265` 重跑时把继承节点传给 `sse.startRun`，符合“重跑固定上次节点”的方向。

主要差距：

1. **远端提交前缺少统一 preflight**：当前单次 QE run 只按 `node_id` 构造 client，未统一校验 `offline`、API 不可达、workspace config 不可读；需要与 custom loop 共用同一套规则。
2. **有效节点没有始终持久化**：`backend/routers/quantevolver.py:5883` 只有 `node_id` 非空才写 `execution_node_id`；新逻辑应始终写入 resolved effective node，避免空值和历史遗留值歧义。
3. **远端 workspace 目录存在隐藏 bug**：AIstock 生成的 `wsl_command` 会 `cd {workspace_base}/{experiment_name}`（`backend/services/quantevolver/config_composer.py:1161`、`:3266`），但 RD-Agent API 会把提交文件写入 `{WORKSPACE_BASE}/{task_id}/Loop1` 并以 `cwd=loop_dir` 启动子进程（`F:/Dev/RD-Agent-main/rdagent/app/api_endpoints/qe_evolution_api.py:86`、`:116`、`:197`）。单次实验当前 `task_id == experiment_name` 且 `experiment_name` 未包含 `Loop1`，命令会进入 task 根目录而不是 `Loop1`，可能导致找不到文件或误读旧文件。
4. **结果/增强指标仍有默认本地路径**：`/sync-results`、enhanced metrics、terminal log tail 等路径仍需统一使用记录节点，不能在远端任务失败时落到本地默认 client 或本地 artifact。
5. **callback URL 存在本地兜底风险**：`backend/services/quantevolver/callback_urls.py:49` 在远端 callback 缺失或为 localhost 时可落到 `127.0.0.1:8001`；远端节点无法访问该地址时不应假装可回调。

### 3.2 custom_evo：当前只有任务级节点，不支持 loop 级节点

已有能力：

- `backend/routers/quantevolver_evolution.py:857` `CustomEvoLoopConfig` 定义每个 loop 的因子/模型/策略等配置。
- `backend/routers/quantevolver_evolution.py:884` `CustomEvolutionCreateRequest` 定义 custom_evo 创建请求。
- `backend/routers/quantevolver_evolution.py:889` 当前只有顶层 `node_id`，含义是整个任务的执行节点。
- `frontend/src/app/quantevolver/evolution/page.tsx:236` 当前有 `customEvoNodeId`。
- `frontend/src/app/quantevolver/evolution/page.tsx:1037` 创建 custom_evo 时只提交顶层 `node_id`。
- `backend/services/quantevolver/qe_evolution_service.py:3999` 创建 task 时只把 `node_id` 写到 `qe_evolution_tasks.node_id`。
- `backend/services/quantevolver/qe_evolution_service.py:4169` custom loop 提交时通过 `_get_workspace_client_for_task(task_id)` 取任务级 client。
- `backend/services/quantevolver/qe_evolution_service.py:4175` `ExecutionContext.node_id=task.get("node_id")`，因此同一 custom_evo 任务的所有 loop 只能跑在同一节点。

主要差距：

1. **请求模型缺少 loop 级 `node_id`**：每个 `CustomEvoLoopConfig` 不能表达“Loop 2 跑远端、Loop 3 跑 WSL”。
2. **数据库 loop 节点字段未被使用**：迁移 `backend/db/migrations/create_dispatch_tables.py:122` 已给 `qe_evolution_loops` 加 `node_id`，但 custom loop insert/update（如 `qe_evolution_service.py:4102`）未写入。
3. **调度器只按全局并行度限流**：`backend/services/quantevolver/qe_evolution_service.py:4373` 使用一个 `asyncio.Semaphore(parallelism)`，不能表达“WSL 并行 2，远端并行 1”。
4. **状态/日志/指标/资产仍按任务级节点取 client**：scanner（`:1976`）、task detail（`:2312`）、日志（`:2925`）、资产同步（`:3017`）、stop（`:2470`）、retry（`:2711`、`:2729`）均主要依赖 `_get_workspace_client_for_task(task_id)`。
5. **增强指标路由使用全局 RD-Agent base**：`backend/routers/quantevolver_evolution.py:32` 的 `RDAGENT_QE_BASE` 默认为 `http://127.0.0.1:9000`，`/tasks/{task_id}/loops/{loop_id}/enhanced-metrics` 在 `:1132` 直接拼该 base，不适合 per-loop 多节点。
6. **cross-node backtest-only 只在 strategy_evo 有同步**：`qe_evolution_service.py:3536` 附近已有 strategy_evo 跨节点 `mlruns` 同步，但 custom_evo backtest-only 当前只传 `model_source`，没有按 source loop 节点打包/传输模型参数。

### 3.3 RD-Agent 侧需要同步配合的风险

- RD-Agent `qe_evolution_api.py:116` 将 `experiment_files` 写入 `loop_dir`。
- RD-Agent `qe_evolution_api.py:169` 如果 AIstock 传入 `wsl_command`，直接使用该命令。
- RD-Agent `qe_evolution_api.py:197` 虽然 `cwd=loop_dir`，但 shell 命令内部的 `cd ...` 会覆盖 `cwd`。
- RD-Agent `qe_evolution_api.py:149` 在 `cross_node=True` 且缺少 `mlruns_params.tar.gz` 时只写 WARN 后继续，这会制造“看起来提交成功但实际模型不可用”的静默错误；应改为失败。

---

## 4. 设计原则与强约束

1. **记录节点即权威节点**：新任务、新 loop、重跑都必须有 resolved effective node；后续 status/log/metrics/assets/kill/retry 均使用该节点，不再重新推断。
2. **不允许静默 fallback**：目标节点不可达、workspace 不可读、metrics 不存在、mlruns 缺失、callback 不可达，都必须以明确错误返回或标记失败，不能改跑本地、不能换节点、不能假装 completed。
3. **busy 不是不可用**：`busy` 仅表示节点繁忙，允许排队/提交；只有 `offline`、节点不存在、`api_base_url` 缺失、API 不可达、`/config` 不可读才拒绝。
4. **分布式只发生在 custom loop 维度**：每个 loop 单独选择节点；同一个 loop 的训练、回测、状态、结果、资产必须来自同一节点。
5. **并行度按节点限流**：同一任务内，按 resolved node 分组，每组一个 semaphore；默认 1，最大 4。
6. **重跑不可换节点**：重跑从 `qe_evolution_loops.node_id` 或单次 QE 的 `custom_params.execution_node_id` 读取节点并锁定，不接受 UI 或 API 传入的新节点。
7. **结果横向对比仍以 AIstock DB 为准**：所有 loop 完成后必须回写 `qe_evolution_loops.metrics_json`、`config_json`、`status`、SOTA registry 和本地详情缓存；前端对比不直接依赖远端 workspace。
8. **兼容旧任务但不制造新空值**：旧任务缺少 loop node 时可以按 task node/local 做只读兼容；新建任务必须写 loop node。

---

## 5. 数据模型与请求模型设计

### 5.1 请求模型

修改 `backend/routers/quantevolver_evolution.py`：

```python
class CustomEvoLoopConfig(BaseModel):
    ...
    node_id: Optional[str] = Field(
        None,
        description="执行节点 ID；Loop1 为空时使用任务默认节点，Loop2+ 为空时继承 Loop1 resolved node",
    )

class CustomEvolutionCreateRequest(BaseModel):
    ...
    node_id: Optional[str] = Field(None, description="Loop1/default 执行节点 ID；兼容旧前端")
    node_parallelism: Optional[Dict[str, int]] = Field(
        None,
        description="每节点并行度，key=node_id，value=1..4；未提供默认1",
    )
```

说明：

- 顶层 `node_id` 不再表示“所有 loop 强制同一节点”，而是兼容旧 API，作为 Loop 1 的默认节点。
- Loop 1 的 effective node 解析顺序：`loops[0].node_id` -> `request.node_id` -> 系统默认节点（建议解析为 `AISTOCK_DEFAULT_GPU_NODE_ID` 或 `wsl2-5080`，并显式写入）。
- Loop 2+ 的 effective node 解析顺序：`loop.node_id` -> `Loop1 effective node`。
- 新建任务时，所有 loop 的 resolved node 都必须写回 loop 配置和 DB，不允许保留 `None`。

### 5.2 数据库存储

优先复用现有字段，避免不必要 schema 扩张：

1. `qe_evolution_tasks.node_id`
   - 作为 custom_evo 的 `default_node_id` / `loop1_node_id` 兼容字段。
   - 新建 custom_evo 时写 Loop 1 effective node。
2. `qe_evolution_tasks.strategy_evo_config`
   - 继续保存 `{"loops": [...], "engine_mode": "unified"}`。
   - 新增保存：

```json
{
  "loops": [
    {"loop_index": 1, "node_id": "wsl2-5080", "...": "..."},
    {"loop_index": 2, "node_id": "rdagent-node1", "...": "..."}
  ],
  "engine_mode": "unified",
  "node_parallelism": {
    "wsl2-5080": 1,
    "rdagent-node1": 1
  },
  "node_resolution_policy": "loop1_inherit_v1"
}
```

3. `qe_evolution_loops.node_id`
   - 已由 `backend/db/migrations/create_dispatch_tables.py:122` 添加。
   - custom loop 创建/提交时必须写入该 loop 的 effective node。
   - 所有后续操作优先使用该字段。
4. `qe_evolution_loops.config_json`
   - 写入 `execution_node_id`、`node_id`、`node_parallelism_snapshot`、`rdagent_task_id`、`rdagent_loop_id`，便于详情页和审计。
5. 单次 QE `qe_experiments.custom_params.execution_node_id`
   - `_run_experiment_unified` 必须始终写入 resolved node。
   - 历史重跑读取该字段并锁定。

建议补充 schema/索引：

```sql
ALTER TABLE qe_evolution_loops ADD COLUMN IF NOT EXISTS node_id TEXT;
CREATE INDEX IF NOT EXISTS idx_qe_evolution_loops_task_node
ON qe_evolution_loops(task_id, node_id);
```

同时更新 `backend/init_catalog_db.py` 的 base schema / migration block，避免新库缺少 `qe_evolution_loops.node_id`。

---

## 6. 节点解析与 preflight 设计

### 6.1 核心 helper

建议在 `backend/services/quantevolver/qe_evolution_service.py` 或独立模块 `backend/services/quantevolver/node_execution.py` 增加：

```python
def resolve_default_qe_node_id() -> str:
    """返回显式默认节点，例如 env AISTOCK_DEFAULT_GPU_NODE_ID 或 wsl2-5080。"""

async def preflight_qe_node(node_id: str) -> dict:
    """校验 compute_nodes + RD-Agent API /config；busy 允许，offline/API不可达拒绝。"""

def resolve_custom_loop_nodes(
    loops_config: list[dict],
    request_node_id: str | None,
) -> tuple[list[dict], str, set[str]]:
    """写入每个 loop 的 effective node_id，并返回 Loop1 node 与节点集合。"""

def normalize_node_parallelism(
    selected_node_ids: set[str],
    raw: dict[str, int] | None,
) -> dict[str, int]:
    """只允许 selected nodes；默认1；范围1..4；未知节点和越界值直接报错。"""
```

### 6.2 preflight 规则

每个 selected node 必须满足：

1. `infra.compute_nodes` 中存在该 `node_id`。
2. `status != 'offline'`；`busy`、`active`、`idle`、`unknown` 的处理建议：
   - `busy`：允许提交。
   - `active/idle`：允许提交。
   - `unknown`：如果 API reachable 且 `/config` 成功，可以允许，但返回 warning；如果 API 不可达则拒绝。
3. `api_base_url` 非空。
4. `QEWorkspaceClient.for_node(node_id).get_workspace_config()` 成功返回，并包含必要路径：`workspace_base`、`factor_data_dir`、`qlib_data_path`、`qlib_minute_path`、`qlib_rdagent_root`。
5. 如果需要 callback 主动回调，则 callback base 必须远端可达；若不可证明可达，仍可依赖 scanner，但不能把 `127.0.0.1:8001` 传给远端节点。更严格方案：远端节点 selected 时 callback_url 必须是非 localhost 的 `AISTOCK_QE_CALLBACK_BASE_URL` 或 `compute_nodes.callback_url`。

API 不可达示例错误：

```json
{
  "error_code": "QE_NODE_API_UNREACHABLE",
  "message": "节点 rdagent-node1 API 不可达，拒绝提交 custom_evo",
  "context": {
    "node_id": "rdagent-node1",
    "api_base_url": "http://192.168.50.215:9000",
    "phase": "preflight_get_workspace_config"
  }
}
```

### 6.3 创建 custom_evo 时的流程

1. 前端提交 loops + loop node + node_parallelism。
2. 后端解析每个 loop effective node。
3. 后端校验并行度 map。
4. 后端对 selected nodes 全量 preflight。
5. 后端执行 stock_pool sync preflight：每个 loop 的 `stock_pool` 必须同步到该 loop target node；失败则拒绝整个 task 创建，不进入 running。
6. 后端持久化 task、loops config、node_parallelism。
7. 后端启动 `submit_custom_evo_all_loops(task_id)`。

说明：为避免“创建成功但部分节点不可达”导致排错困难，建议在创建阶段对所有 selected nodes 做 preflight；后续真正提交前也再做一次轻量 preflight，防止排队期间节点变 offline。

---

## 7. custom_evo 调度与并行设计

### 7.1 执行模式调整

当前 `execution_mode` 支持 `serial` / `parallel_N`。新逻辑建议：

- `serial`：仍按 loop_index 顺序执行；每个 loop 使用自己的 node。
- `parallel` / `parallel_N`：保留旧 UI/API 兼容，但实际限流由 `node_parallelism` 控制。
- `parallel_N` 的 N 不再允许 6/8 这种绕过节点能力的全局并发；如果保留 N，只作为全局上限且不得突破每节点并行度。推荐后续 UI 改为“并行执行 + 每节点并行度”。

### 7.2 调度算法

伪代码：

```python
async def submit_custom_evo_all_loops(task_id: str, force_full_train: bool = False):
    task = load_task(task_id)
    config = parse_strategy_evo_config(task)
    loops = load_or_build_loops_to_run(task, config)

    # 每个 loop 都必须有 node_id；旧任务兼容时只读 task.node_id/default，并补写前先记录 warning。
    loops = resolve_or_load_loop_nodes(task, loops)
    node_parallelism = normalize_node_parallelism(
        {loop.node_id for loop in loops},
        config.get("node_parallelism"),
    )

    # 所有节点轻量 preflight；busy 允许，offline/API 不可达拒绝对应 loop。
    await preflight_all_selected_nodes(loops)

    semaphores = {
        node_id: asyncio.Semaphore(limit)
        for node_id, limit in node_parallelism.items()
    }

    async def run_one(loop_config):
        node_id = loop_config["node_id"]
        async with semaphores[node_id]:
            if task_stopped(task_id):
                mark_loop_cancelled(loop_id)
                return
            await submit_custom_evo_loop(task_id, loop_index, expected_node_id=node_id)
            await wait_loop_terminal_on_node(task_id, loop_id, node_id)
            await process_completed_loop(task_id, loop_id, expected_node_id=node_id)

    await asyncio.gather(*(run_one(loop) for loop in loops_to_run), return_exceptions=True)
```

### 7.3 单 loop 提交改造点

`_submit_custom_evo_loop_unified(...)` 必须：

1. 从 `loop_config["node_id"]` 或 `qe_evolution_loops.node_id` 读取 `effective_node_id`。
2. `INSERT INTO qe_evolution_loops (...)` 时写入 `node_id`。
3. `ON CONFLICT` 时不得覆盖已完成 loop 的 node；如果是 retry，必须读取旧 node 并校验入参一致。
4. `config_record` 写入 `execution_node_id`。
5. `client = _get_workspace_client_for_loop(task_id, loop_id)`，不再用 `_get_workspace_client_for_task`。
6. `ExecutionContext.node_id=effective_node_id`。
7. `callback_url=_get_callback_url_for_loop(task_id, loop_index, effective_node_id)`。
8. backtest-only 的 `model_source` 如跨节点，必须先同步 `mlruns_params.tar.gz.b64`，否则失败。

---

## 8. 结果、日志、状态、资产的 loop-node-aware 改造

### 8.1 新增读取 helper

```python
def get_loop_execution_node(task_id: str, loop_id_or_index: str | int) -> str:
    """优先 qe_evolution_loops.node_id；旧任务兼容 qe_evolution_tasks.node_id；仍为空则返回显式默认节点并记录兼容警告。"""

def get_workspace_client_for_loop(task_id: str, loop_id_or_index: str | int) -> QEWorkspaceClient:
    node_id = get_loop_execution_node(task_id, loop_id_or_index)
    return self._get_workspace_client_for_node_id(node_id)
```

### 8.2 必须改造的调用点

1. `scan_running_loops`：`qe_evolution_service.py:1976` 改为按 loop node 查 `get_loop_status`。
2. `get_task_detail`：`qe_evolution_service.py:2312` 改为每个 running loop 使用自己的 node；API 异常展示为 node/API error，不把 API 不可达静默映射成业务 failed metrics。
3. `process_completed_loop`：`qe_evolution_service.py:1510` 改为从 loop node 拉 metrics/enhanced metrics。
4. `process_strategy_evo_completed_loop`：`qe_evolution_service.py:3787` 改为从 loop node 拉 metrics/enhanced metrics；保持 strategy_evo 兼容。
5. `stream_task_logs`：`qe_evolution_service.py:2925` 当前按 task client 拉整任务日志；custom_evo 多节点时建议提供 per-loop log stream，任务级日志聚合时显式标注 `[node_id][LoopN]`。
6. `sync_loop_assets`：`qe_evolution_service.py:3017` 按 loop node 下载资产。
7. `stop_task`：`qe_evolution_service.py:2470` 按每个 non-terminal loop 的 node 调 `kill_loop`；不能只 kill task node。
8. `retry_loop`：`qe_evolution_service.py:2711`、`:2729` 固定读取原 loop node；不接受新 node。
9. `quantevolver_evolution.py:1132` enhanced metrics 路由不再拼 `RDAGENT_QE_BASE`，改为 scheduler/helper 按 loop node 转发并缓存。

### 8.3 结果回写约束

每个 loop 的完成处理必须写：

- `qe_evolution_loops.status`：`completed` / `failed` / `cancelled`。
- `qe_evolution_loops.metrics_json`：完整 metrics + enhanced summary + node trace。
- `qe_evolution_loops.config_json.execution_node_id`。
- `qe_evolution_loops.agent_analysis`：错误时写结构化错误，不覆盖已有有效分析。
- `qe_sota_registry`：只基于 AIstock 已回收 metrics 判断，不直接读远端 workspace。

建议在 metrics_json 中加入：

```json
{
  "execution_trace": {
    "node_id": "rdagent-node1",
    "rdagent_task_id": "qe_20260428_120000_abcd",
    "rdagent_loop_id": "Loop2",
    "metrics_source": "qe_workspace_api",
    "metrics_fetched_at": "2026-04-28T..."
  }
}
```

---

## 9. 单次 QE 实验远端流程硬化

### 9.1 修复 workspace 目录不一致

问题：单次实验通过 `BacktestExecutor` 提交到 RD-Agent 时，RD-Agent 文件落在 `{task_id}/Loop1`，但 AIstock 生成的命令 `cd {workspace_base}/{experiment_name}`。如果 `experiment_name == task_id`，命令进入 task 根目录，无法稳定读取 `conf.yaml`、`qrun_limit_minute.py`、`read_exp_res.py`。

推荐方案：

1. 保持 DB 中 `qe_task_id = experiment_name`，`qe_loop_id = Loop1`。
2. 单次 QE 提交时，传给 `ConfigComposer.compose_experiment_in_memory` 的执行工作目录应为 `{experiment_name}/Loop1`。
3. 可通过以下方式之一实现：
   - 给 `ExecutionContext` 增加 `workspace_experiment_name` / `wsl_workdir_name`，custom_evo 传 `task_id/LoopN`，single QE 传 `experiment_name/Loop1`。
   - 或在 `_run_experiment_unified` 构造 `ctx.experiment_name=f"{experiment_name}/Loop1"`，同时保留单独字段用于 DB 展示名称。
4. 增加测试证明 `wsl_command` 中 `cd` 路径等于 RD-Agent loop_dir。

禁止方案：

- 不允许 RD-Agent 在找不到 `conf.yaml` 时回退到 task 根目录或历史目录。
- 不允许 AIstock 在 `/sync-results` 找不到远端结果时读取本地同名旧 artifact。

### 9.2 单次 QE 节点 preflight

`_run_experiment_unified(experiment_id, node_id)` 应先解析 effective node：

1. 如果 API 入参有 `node_id`：使用它。
2. 如果是历史重跑且已有 `custom_params.execution_node_id`：必须使用历史节点；如果入参不同，返回 400。
3. 如果新实验没有节点：使用显式默认节点并持久化。

然后执行同 custom_evo 的 `preflight_qe_node(effective_node_id)`。

### 9.3 单次 QE 后续接口节点化

必须改造：

1. `/experiments/{experiment_id}/sync-results`：使用 `custom_params.execution_node_id` 的 client。
2. `/experiments/{experiment_id}/run-status`：当前 `quantevolver.py:6124` 已有 node-aware 逻辑，应保持并增加 API unreachable 明确错误。
3. `/experiments/{experiment_id}/logs`：当前 `quantevolver.py:6253` 已有 node-aware live stream，但 fallback 本地 run.log 只能用于明确本地节点或 terminal artifact，远端 API 失败不得伪装为本地成功。
4. `/experiments/{experiment_id}/logs/tail`：当前主要读本地 tail，应按 execution node 增加远端 tail 或明确“不支持远端 tail”。
5. `/experiments/{experiment_id}/enhanced-metrics`：优先按 execution node 读取远端 / 或按已同步本地 artifact；不得使用全局 local RD-Agent base。
6. workspace cleanup：按 recorded node 清理；不能仅清理本地。

---

## 10. cross-node backtest-only 模型复用

### 10.1 custom_evo 必须补齐 strategy_evo 已有能力

当前 strategy_evo 在 `qe_evolution_service.py:3536` 附近已有跨节点 `mlruns` 同步逻辑；custom_evo 在 `qe_evolution_service.py:4178` 进入 backtest-only 时只传：

```python
model_source={
    "source_task_id": cfg.model_source_task_id,
    "source_loop": f"Loop{cfg.model_source_loop_index}",
}
```

新逻辑必须：

1. 读取 source loop 的 `node_id`。
2. 比较 `source_node_id` 与 `target_node_id`。
3. 如果同节点：继续使用 RD-Agent 侧 symlink。
4. 如果跨节点：从 source node 调 `download_mlruns_params(source_task_id, source_loop)`。
5. 将 tar.gz 以 `.b64` 写入 `ctx.extra_experiment_files["mlruns_params.tar.gz.b64"]`。
6. `ctx.model_source["cross_node"] = True`。
7. source tar 缺失、无 params.pkl、下载失败时，loop 直接 failed，不提交目标节点。

### 10.2 RD-Agent 必须 fail-fast

修改 `F:/Dev/RD-Agent-main/rdagent/app/api_endpoints/qe_evolution_api.py`：

- `cross_node=True` 且 `mlruns_params.tar.gz` 不存在时，不再仅 `_append_log(... WARN ...)`。
- 应写 `status.txt=failed`，记录错误，抛出异常或返回明确失败。

---

## 11. 前端交互设计

### 11.1 custom_evo loop 卡片

每个 loop 卡片新增“执行节点”选择器：

- Loop 1：默认显示当前任务默认节点（建议 `wsl2-5080` 或现有 `customEvoNodeId`），可手动选择。
- Loop 2+：默认值为“继承 Loop 1（当前：xxx）”；用户可改为具体节点。
- 节点选项显示：`display_name/node_id/status/gpu_model/gpu_vram`。
- `offline` 节点禁用并显示“离线，不可提交”。
- `busy` 节点可选并显示“繁忙，可提交但可能排队”。
- API 不可达需要后端最终拒绝；前端如有健康状态可提前提示，但不能只靠前端判断。

### 11.2 表单底部每节点并行度面板

在 custom task 表单最下方、创建按钮上方新增“节点并行度”面板：

- 根据 resolved nodes 自动生成行。
- 每行：节点名、状态、GPU 信息、选择的 loop 列表、并行度 select/input。
- 并行度范围：`1..4`，默认 `1`。
- 如果用户选择 `4`，显示提示：“请确认该节点显存/CPU 可承载 4 个 QE loop 并行”。
- 未出现在 loop selection 中的节点不显示，也不允许提交 parallelism key。

示例：

| 节点 | Loops | 状态 | 并行度 |
| --- | --- | --- | --- |
| wsl2-5080 | Loop1, Loop3 | busy | 2 |
| rdagent-node1 | Loop2 | busy | 1 |

### 11.3 创建 payload

```json
{
  "task_name": "custom distributed smoke",
  "target_desc": "...",
  "execution_mode": "parallel",
  "node_id": "wsl2-5080",
  "node_parallelism": {
    "wsl2-5080": 2,
    "rdagent-node1": 1
  },
  "loops": [
    {"loop_index": 1, "node_id": "wsl2-5080", "...": "..."},
    {"loop_index": 2, "node_id": "rdagent-node1", "...": "..."},
    {"loop_index": 3, "node_id": null, "...": "..."}
  ],
  "engine_mode": "unified"
}
```

后端解析后，Loop3 的 `node_id` 必须变成 Loop1 的 `wsl2-5080` 并持久化。

### 11.4 重跑 UI

- 单次 QE 历史重跑：显示“锁定上次节点：xxx”，不显示可编辑 selector。
- custom_evo loop retry：按钮旁显示“重跑节点：xxx”；不允许传新 node。
- 如果历史 node offline/API 不可达，重跑按钮点击后返回明确错误：“上次节点 xxx 当前不可达，不能换节点重跑”。

---

## 12. API 行为设计

### 12.1 创建 custom_evo

`POST /api/v1/quantevolver/evolution/custom-tasks`

新增成功返回：

```json
{
  "status": "success",
  "task_id": "qe_20260428_120000_abcd",
  "total_loops": 3,
  "execution_mode": "parallel",
  "node_assignments": {
    "Loop1": "wsl2-5080",
    "Loop2": "rdagent-node1",
    "Loop3": "wsl2-5080"
  },
  "node_parallelism": {
    "wsl2-5080": 2,
    "rdagent-node1": 1
  }
}
```

失败返回应尽量结构化。现有接口可先用 `HTTPException(status_code=400, detail={...})`，前端统一展示 `detail.message` 和 `detail.context`。

### 12.2 获取 task detail

`GET /api/v1/quantevolver/evolution/tasks/{task_id}` 返回每个 loop：

```json
{
  "loop_id": "qe_xxx_Loop2",
  "loop_index": 2,
  "status": "completed",
  "node_id": "rdagent-node1",
  "node_status_snapshot": "busy",
  "metrics_json": {...},
  "config_json": {...}
}
```

### 12.3 enhanced metrics

`GET /api/v1/quantevolver/evolution/tasks/{task_id}/loops/{loop_id}/enhanced-metrics`

- 先查 AIstock cache。
- cache 缺失时按 loop node 调 `QEWorkspaceClient.get_enhanced_metrics(...)`。
- 成功后缓存到 `qe_evolution_loops.metrics_json`。
- node API 不可达返回明确 502/503，不落到 `RDAGENT_QE_BASE`。

### 12.4 retry loop

`POST /api/v1/quantevolver/evolution/tasks/{task_id}/loops/{loop_index}/retry`

- 不接受 `node_id`。
- 读取 `qe_evolution_loops.node_id`。
- 如果缺失：旧任务兼容读取 `qe_evolution_tasks.node_id`，并在 retry 前补写 loop node；仍缺失则 fail-fast。
- 如果前端或调用方传入 node，应返回 400：`QE_RETRY_NODE_LOCKED`。

---

## 13. 实施阶段计划

### Phase 0：文档与测试基线

要做：

1. 固化本设计文档。
2. 增加最小单元测试 scaffold，覆盖 node resolution、parallelism normalization、preflight 行为。
3. 不改业务行为。

验证：

```powershell
python -m py_compile backend/services/quantevolver/qe_evolution_service.py backend/routers/quantevolver.py backend/routers/quantevolver_evolution.py
```

### Phase 1：单次 QE 远端运行硬化

要做：

1. 抽出 `resolve_default_qe_node_id`、`preflight_qe_node`。
2. `_run_experiment_unified` 使用 effective node 并始终持久化 `custom_params.execution_node_id`。
3. 修复 single QE `wsl_command` 的 loop workspace 目录，确保 `cd` 到 `{task_id}/Loop1`。
4. `/sync-results`、enhanced metrics、logs/tail、cleanup 全部按 `execution_node_id` 使用 client。
5. 历史重跑若传入 node 与 stored node 不一致，返回 400。

验证：

- 单测断言 single QE `wsl_command` 包含 `/Loop1`。
- 单测断言 `sync-results` 使用 stored node。
- 手工 smoke：远端 busy 节点可提交；offline/API 不可达拒绝。

### Phase 2：custom_evo 请求模型与持久化

要做：

1. `CustomEvoLoopConfig` 增加 `node_id`。
2. `CustomEvolutionCreateRequest` 增加 `node_parallelism`。
3. 创建时解析每个 loop effective node，Loop2+ 默认继承 Loop1。
4. 写入 `strategy_evo_config.loops[*].node_id` 和 `strategy_evo_config.node_parallelism`。
5. `qe_evolution_tasks.node_id` 写 Loop1 effective node。
6. `qe_evolution_loops.node_id` 在 loop 创建/提交时写入。
7. 更新 `backend/init_catalog_db.py`，确保新库有 loop node 字段。

验证：

- Loop1 指定 WSL，Loop2 空，Loop3 指定远端；DB 中 Loop2 node == Loop1。
- `node_parallelism` 空时默认每个 selected node 为 1。
- parallelism 0/5/未知 node key 返回 400。

### Phase 3：custom_evo per-node scheduler

要做：

1. `submit_custom_evo_all_loops` 改为按 node 分组 semaphore。
2. 保留 serial 顺序执行，但每 loop 用自己的 node。
3. parallel 模式下每节点 semaphore 独立限流。
4. 提交前再次轻量 preflight；offline/API 不可达时该 loop 失败并写结构化错误，不切换节点。
5. 保留任务取消/暂停检查。

验证：

- 构造 4 个 loops：WSL 两个并行度 2、远端两个并行度 1；测试同时运行计数符合限制。
- busy 节点不被拒绝。
- offline/API unreachable 拒绝。

### Phase 4：状态、日志、指标、资产、停止、重试节点化

要做：

1. 增加 `_get_workspace_client_for_loop`。
2. 替换 scanner、task detail、process completed、enhanced metrics、sync assets、stop task、retry loop 的 task-level client。
3. 任务详情返回每 loop node 信息。
4. 日志聚合标注 node/loop；必要时新增 per-loop log endpoint。
5. API 不可达不再映射成业务指标失败。

验证：

- 两节点 loop 同时 running 时，scanner 分别调用对应 client。
- Stop task 对每个 non-terminal loop 调用其 node 的 kill。
- Enhanced metrics 不再使用 `RDAGENT_QE_BASE` 全局 local base。

### Phase 5：前端 custom_evo UI

要做：

1. 前端 `CustomEvoLoopConfig` type 增加 `node_id`。
2. 每个 custom loop card 增加节点选择器。
3. Loop2+ 默认“继承 Loop1”。
4. 表单底部新增每节点并行度面板，默认 1，范围 1..4。
5. 创建 payload 提交 `node_parallelism` 和 loop-level node。
6. task detail / loop card 展示 execution node。
7. retry / rerun UI 展示锁定节点，不允许编辑。

验证：

```powershell
cd frontend
npx tsc --noEmit
npm run build
```

### Phase 6：cross-node model_source 与 RD-Agent fail-fast

要做：

1. custom_evo backtest-only 增加 source loop node 查询。
2. 跨节点下载 source `mlruns_params.tar.gz` 并以 `.b64` 传给 target node。
3. target RD-Agent `cross_node=True` 缺 tar 时 fail-fast。
4. source 模型缺失时不提交 target loop。

验证：

- 同节点 backtest-only 仍 symlink。
- 跨节点 backtest-only 带 tar，target 解压成功。
- 跨节点缺 tar 失败，不继续 qrun。

### Phase 7：端到端验收

要做：

1. 单次 QE 远端完整跑通：submit -> logs/status -> completion -> metrics/enhanced metrics -> sync assets。
2. custom_evo 三 loop：Loop1 WSL、Loop2 remote、Loop3 inherit Loop1；并行度 WSL=2、remote=1。
3. 横向对比页面显示三个 loop 的 metrics，node trace 正确。
4. 重跑 Loop2 时固定 remote；remote offline/API 不可达时拒绝，不允许改 WSL。

建议命令：

```powershell
python -m py_compile backend/routers/quantevolver.py backend/routers/quantevolver_evolution.py backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/executors/backtest.py
python -m pytest backend/tests/unified_engine -q
cd frontend
npx tsc --noEmit
npm run build
```

手工 smoke 注意：不要重启或占用生产 `8001`；如需验证后端，使用临时端口（例如 `8011/8012`）。

---

## 14. 测试清单

### 14.1 必须新增/更新的单测

1. `test_custom_loop_node_resolution_loop1_inherit`
   - Loop1 指定 node A，Loop2 空，Loop3 指定 node B。
   - 断言 resolved nodes 为 A/A/B。
2. `test_custom_node_parallelism_default_and_max`
   - 空 map 默认 1；4 允许；5 拒绝；未知 node key 拒绝。
3. `test_preflight_allows_busy_rejects_offline`
   - busy + API reachable -> pass。
   - offline -> 400。
4. `test_preflight_rejects_api_unreachable`
   - status busy 但 `/config` 失败 -> reject。
5. `test_custom_loop_insert_writes_node_id`
   - `_submit_custom_evo_loop_unified` insert/update 写 `qe_evolution_loops.node_id`。
6. `test_custom_parallel_scheduler_per_node_limit`
   - mock submit/wait，断言每 node 并发不超过配置。
7. `test_get_workspace_client_for_loop_uses_loop_node`
   - scanner/status/metrics 使用 loop node，不使用 task node。
8. `test_retry_loop_locks_previous_node`
   - retry 读取原 node；入参 node 不被接受。
9. `test_single_qe_remote_workdir_is_loop_dir`
   - single QE `wsl_command` cd 到 `{workspace_base}/{task_id}/Loop1`。
10. `test_single_qe_sync_results_uses_execution_node`
    - `custom_params.execution_node_id` 为 remote 时，`QEWorkspaceClient.for_node(remote)` 被调用。
11. `test_cross_node_custom_backtest_only_requires_mlruns_tar`
    - source/target 不同且下载失败时 loop failed，不提交。
12. `test_rdagent_cross_node_missing_tar_fails`
    - RD-Agent 侧 `cross_node=True` 缺 tar 时失败。

### 14.2 grep 防回归

```powershell
rg -n "RDAGENT_QE_BASE" backend/routers/quantevolver_evolution.py
rg -n "_get_workspace_client_for_task\(" backend/services/quantevolver/qe_evolution_service.py
rg -n "execution_node_id" backend/routers/quantevolver.py
rg -n "mlruns_params.tar.gz not found|\[WARN\] Cross-node" F:/Dev/RD-Agent-main/rdagent/app/api_endpoints/qe_evolution_api.py
```

目标：

- enhanced metrics 不再直接依赖 `RDAGENT_QE_BASE`。
- custom_evo loop 相关路径不再错误使用 `_get_workspace_client_for_task`。
- single QE 始终写 `execution_node_id`。
- RD-Agent cross-node 缺 tar 不再只是 WARN。

---

## 15. 禁止事项

1. 禁止目标节点失败后自动改跑本地或其他节点。
2. 禁止 API 不可达时把 loop 标记为业务 `failed` 但不记录 node/API context。
3. 禁止 metrics 缺失时用旧缓存/空 metrics 伪造成 completed。
4. 禁止 callback 不可达时传 `127.0.0.1:8001` 给远端节点。
5. 禁止重跑时允许用户改节点。
6. 禁止 per-node 并行度超过 4。
7. 禁止 custom_evo backtest-only 跨节点缺模型参数时继续 qrun。
8. 禁止修改历史实验资产、训练产物、DB 资产来掩盖流程 bug。
9. 禁止为了通过测试而跳过真实 `conf.yaml`、`read_exp_res.py`、metrics artifact 校验。

---

## 16. 建议落地顺序

建议严格按以下顺序执行，避免把新分布式能力建立在不可靠的单次远端链路上：

1. **先修单次 QE 远端硬化**：尤其是 workspace 目录不一致、effective node 持久化、sync/enhanced/log 节点化。
2. **再加 custom_evo loop node 数据模型**：请求模型、解析、持久化、preflight。
3. **再改调度器 per-node semaphore**：先保证提交到正确节点，再放开并行。
4. **再改结果/状态/日志/资产/stop/retry**：确保所有后续读取都回到正确节点。
5. **最后改前端 UI 与 cross-node backtest-only**：UI 只暴露后端已经严格校验的能力。

---

## 17. 待讨论问题

1. 默认节点是否固定为 `wsl2-5080`，还是读取 `AISTOCK_DEFAULT_GPU_NODE_ID` 并要求该节点必须存在于 `infra.compute_nodes`？建议后者，默认值为 `wsl2-5080`。
2. 远端 callback 是否强制要求可达？建议远端节点提交时必须传非 localhost callback；scanner 可作为兜底，但不能把 localhost 传给远端。
3. `execution_mode=parallel_N` 是否继续在 UI 暴露 N=2/4/6/8？建议收敛为“并行执行”开关 + 每节点并行度，避免全局 N 与 per-node N 冲突。
4. 旧 custom_evo 任务缺少 `qe_evolution_loops.node_id` 时，是否允许一次性 backfill？建议只对历史只读/重跑做兼容，重跑前补写 resolved node，并在日志中标记 legacy compatibility。
