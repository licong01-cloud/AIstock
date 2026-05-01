# QE 自定义演进 Loop 重新运行、继续演进与克隆创建实施方案（2026-05-01）

> 状态：已按本方案完成第一版实现，并通过不真实运行 QE 的静态/路由级 dry-run 验证。
> 范围：只覆盖 `custom_evo` 自定义演进任务的 Loop 重新运行、失败重试边界说明、完成任务继续演进、克隆已有任务创建新自定义演进任务、前端展示和后端调度/数据清理。
> 用户已确认：重新运行支持所有 Loop 状态；旧结果完全删除且不保留备份；重新运行/继续演进允许修改节点和执行模式；继续演进允许一次新增多个 Loop；任务内已有失败 Loop 时允许继续但前端必须提示；继续演进默认复制最后一个 Loop 配置，不判断其是否失败。补充确认：任务列表需要支持克隆一个已有自定义演进任务的所有 Loop 和配置，打开与新建自定义演进任务完全一致的创建窗口，允许修改每个 Loop 的全部配置、删除部分 Loop、新增 Loop，并自定义选择执行节点。

---

## 1. 目标与业务语义

### 1.1 要解决的问题

1. **配置错误修正**：自定义演进任务创建后，如果某个 Loop 的因子、模型、策略、训练周期、HMM、执行节点等配置错误，需要在原任务内用同一个 Loop 序号重新运行，覆盖旧结果，保持横向对比不产生重复行。
2. **结果基础上的继续探索**：自定义演进任务完成或终止后，需要在同一个任务下追加新 Loop，用最后一个 Loop 的配置作为默认起点，再修改方向继续实验，便于统一看演进轨迹和 Loop 详情。
3. **区分 retry 与 rerun**：失败重试仍是“用原配置、选择训练/回测模式”；重新运行是“展示完整配置、允许修改所有配置、删除旧结果后重新执行”。
4. **克隆已有实验新建任务**：在任务列表中从一个已有 `custom_evo` 任务克隆全部 Loop 和配置，打开新建自定义演进任务窗口预填内容；用户可以删除部分 Loop、新增 Loop、修改任意 Loop 配置和执行节点，再创建一个全新的自定义演进任务。

### 1.2 三种操作的边界

```text
Action              Trigger Scope        Editable Config         Old Result Handling        Main Use Case
retry               failed/cancelled     retry_mode only         keep row, rerun same cfg    execution/backtest transient failure
rerun               any loop status      full loop + node/mode   delete old then recreate    wrong loop config or completed-loop replacement
continue evolution  terminal custom task new loops full config   append only                 add new directions in same comparison task
clone create        any custom task       all loops full config   create new task only        copy an existing task as an editable new experiment
```

说明：

- `retry` 保留现有含义，不改成配置编辑入口。
- `rerun` 使用同一个 `loop_index` 和 `loop_id`，但旧 DB 结果、SOTA 记录、因子/模型记录、实验记录、workspace 产物都必须先删除。
- `continue evolution` 只追加新 Loop，不自动重跑历史失败 Loop；如果历史失败 Loop 仍存在，任务最终状态仍可能是 `failed`，前端必须提示。
- `clone create` 不修改源任务，不复制源任务的指标、SOTA、实验结果或 workspace 产物；它只把源任务配置预填到新建窗口，最终仍走创建新 `custom_evo` 任务流程。

---

## 2. Phase 0 代码与文档发现

### 2.1 已读项目约束

- `AGENTS.md`：要求架构/后端/前端/QE 相关工作前先读 `docs/codex_project_memory.md`。
- `docs/codex_project_memory.md`：确认 QE、前端、后端、测试和表格输出偏好；用户要求后续表格列宽对齐。
- `C:/Users/lc999/.codex/skills/make-plan/SKILL.md`：要求先做文档发现，方案内列出可用 API、引用来源、验证和反模式。

### 2.2 当前可用代码入口

```text
Area                          Source                                                                          Current Finding
router retry                  backend/routers/quantevolver_evolution.py:864-900                               retry API only accepts failed/cancelled loops
router custom request          backend/routers/quantevolver_evolution.py:1169-1205                             CustomEvoLoopConfig already contains most editable loop fields
router custom create           backend/routers/quantevolver_evolution.py:1207-1348                             create API validates loop config, node preflight, stock pool sync
service resume                 backend/services/quantevolver/qe_evolution_service.py:2186-2245                 resume only resets failed/cancelled loops to pending
service retry                  backend/services/quantevolver/qe_evolution_service.py:2737-2905                 retry reuses existing config and locks to stored node
service create custom          backend/services/quantevolver/qe_evolution_service.py:4134-4258                 create_custom_evo_task persists strategy_evo_config and starts all loops
service submit one             backend/services/quantevolver/qe_evolution_service.py:4260-4415                 submit_custom_evo_loop reads config from strategy_evo_config.loops
service submit all             backend/services/quantevolver/qe_evolution_service.py:4505-4775                 submit all skips completed only; failed loops would run again
service final status           backend/services/quantevolver/qe_evolution_service.py:107-121                   custom_evo completed only when every configured loop completed
service detail                 backend/services/quantevolver/qe_evolution_service.py:2403-2498                 task detail returns task row plus ordered loops and live status sync
DB cascade                     backend/init_catalog_db.py:604-620,622-630,647-704                             loop delete cascades SOTA/factor/model records
workspace client               backend/services/quantevolver/qe_workspace_client.py:129-137,286-297            has kill_loop and task cleanup; no loop-level cleanup API yet
frontend custom state          frontend/src/app/quantevolver/evolution/page.tsx:179-325                       custom_evo form state already has full loop config and node resolution
frontend custom submit         frontend/src/app/quantevolver/evolution/page.tsx:1045-1095                     create payload already sends loop config, node, execution mode
frontend retry                 frontend/src/app/quantevolver/evolution/page.tsx:1585-1626                     retry prompt only sends retry_mode
frontend topology              frontend/src/app/quantevolver/evolution/components/TopologyPanel.tsx:142-149    retry button only appears for failed/cancelled loops
frontend task list             frontend/src/app/quantevolver/evolution/page.tsx:1774-1810                       task list already distinguishes custom_evo tasks and can host clone action
```

### 2.3 关键发现

1. 当前创建自定义演进的配置字段已经比较完整，可以作为 `rerun` 和 `continue evolution` 的请求模型基础。
2. 当前 `retry` 后端强制只允许 `failed/cancelled`，并且 `retry_loop` 只复用旧 `config_json`，不适合作为“完整配置重新运行”的入口。
3. 当前 `submit_custom_evo_all_loops` 会跳过 `completed`，但会运行 `failed/cancelled/pending`；因此继续演进不能直接调用它，否则会把历史失败 Loop 一起重跑。
4. 当前 DB 的 `qe_evolution_loops` 删除会级联清理 `qe_sota_registry`、`qe_loop_factor_records`、`qe_loop_model_records`，但 `qe_experiments` 和 `qe_factor_experiment_metrics` 需要显式删除。
5. 当前 `QEWorkspaceClient` 只有任务级 workspace 删除，没有 loop 级 workspace 删除；若不补 loop 级 cleanup，同一 `task_id/LoopN` 重新运行可能混入旧产物。
6. 克隆创建不需要复制结果资产，主要依赖 `strategy_evo_config.loops`、`strategy_evo_execution_mode`、`node_id` 和 `node_parallelism` 作为预填来源；创建提交仍可复用现有 `/custom-tasks` API。

---

## 3. 用户确认后的决策固化

```text
Decision ID   Topic                       Final Decision
D1            rerun status scope           support every loop status, including completed/running/pending/failed/cancelled
D2            old result retention         delete completely, no backup table and no visible duplicate copy
D3            node and execution mode      rerun and continue both allow editing node and execution mode
D4            append loop count            allow multiple new loops, same editing style as new custom_evo task
D5            failed loops in task         allow continue, but show warning before submit
D6            default copy source          copy the highest loop_index config directly, regardless of success/failure
D7            clone existing task          clone all loop configs into a new editable custom_evo create form
```

实现解释：

- “完全删除不保留”只针对 DB 业务结果、可视化数据、资产目录和 workspace 产物；服务运行日志仍可能有普通日志行，但不做 UI/API 审计备份。
- “支持所有状态 Loop 重新运行”要求对 `running/processing/pending` 先终止或清理旧执行，再删除旧结果，避免同一 Loop 同时存在两个后台执行。
- “允许修改执行模式”在单 Loop `rerun` 中主要更新任务级 `strategy_evo_execution_mode` 和 `node_parallelism`，单个 Loop 本身没有并发差异；该模式会影响后续继续演进或多 Loop 追加批次。
- “克隆已有任务”只复制配置，不复制结果；克隆后的新任务拥有新的 `task_id`、新的 Loop 结果和新的 workspace。源任务可以是 completed、failed、cancelled、running 或 mixed 状态，因为克隆不读取源任务的运行中结果。

---

## 4. 后端 API 设计

### 4.1 获取可编辑配置

新增：

```http
GET /api/v1/quantevolver/evolution/tasks/{task_id}/custom-evo-config
```

用途：

- 给前端 rerun/continue 弹窗提供权威配置来源。
- 优先读取 `qe_evolution_tasks.strategy_evo_config.loops`，因为它保留了原始 `factor_keys`、`disable_alpha158`、`backtest_only`、`node_id` 等创建配置。
- 如果某些老任务缺少 `strategy_evo_config.loops`，再从 `qe_evolution_loops.config_json` 做只读兼容转换；转换不完整时返回明确错误。

返回示例：

```json
{
  "status": "success",
  "task_id": "qe_20260430_010121_d55f",
  "task_type": "custom_evo",
  "execution_mode": "parallel_2",
  "node_parallelism": {"wsl2-5080": 1},
  "loops": [
    {"loop_index": 1, "label": "...", "factor_keys": ["x||custom"], "model_id": "..."}
  ]
}
```

### 4.2 重新运行单个 Loop

新增：

```http
POST /api/v1/quantevolver/evolution/tasks/{task_id}/loops/{loop_index}/rerun
```

请求体：

```json
{
  "loop": {"label": "new note", "factor_keys": ["factor||source"], "model_id": "..."},
  "execution_mode": "serial",
  "node_id": "wsl2-5080",
  "node_parallelism": {"wsl2-5080": 1},
  "engine_mode": "unified",
  "confirm_delete_old_result": true
}
```

规则：

1. 仅允许 `task_type == custom_evo`。
2. 允许目标 Loop 处于任何状态。
3. 如果目标 Loop 当前 `running/processing/pending`，必须先调用原节点 `kill_loop` 或确认远端不存在；无法停止时返回 409/500，不删除 DB。
4. 删除旧 DB 结果与旧 workspace 成功后，替换 `strategy_evo_config.loops[loop_index]` 的配置。
5. 使用同一个 `loop_index`、同一个 `loop_id = {task_id}_Loop{loop_index}` 重新创建并提交。
6. 重新运行期间将 task 置为 `running`；完成后按全任务所有 Loop 状态重新计算最终状态。
7. 若用户修改节点，则新结果使用新节点；旧 workspace cleanup 必须按旧节点执行，新提交按新节点执行。

### 4.3 继续演进追加 Loop

新增：

```http
POST /api/v1/quantevolver/evolution/tasks/{task_id}/custom-loops/append
```

请求体：

```json
{
  "loops": [
    {"label": "Loop 6", "factor_keys": ["factor||source"], "model_id": "..."},
    {"label": "Loop 7", "factor_keys": ["factor2||source"], "model_id": "..."}
  ],
  "execution_mode": "parallel_2",
  "node_id": "wsl2-5080",
  "node_parallelism": {"wsl2-5080": 1, "remote-gpu": 1},
  "engine_mode": "unified",
  "ack_failed_loop_warning": true
}
```

规则：

1. 仅允许 `task_type == custom_evo`。
2. 默认前端复制最高 `loop_index` 的配置，不检查其是否成功或失败。
3. 后端按当前最大 `loop_index` 之后连续分配新序号，例如原来最大为 5，则新增为 6、7。
4. 更新 `strategy_evo_config.loops`、`max_loops`、`strategy_evo_execution_mode`、`node_parallelism`。
5. 只调度新增的 Loop 索引，不能自动运行历史失败 Loop。
6. 如果任务内已有失败/取消 Loop，前端必须提示；后端如果收到 `ack_failed_loop_warning != true` 可返回 400，避免用户无感提交。


### 4.4 克隆已有自定义演进任务为新任务

前端入口：任务列表中的 `克隆` 按钮，显示条件建议为 `task_type === "custom_evo"`，不限制源任务状态。

后端 API 复用策略：

1. 配置读取复用 `GET /api/v1/quantevolver/evolution/tasks/{task_id}/custom-evo-config`。
2. 最终创建仍复用 `POST /api/v1/quantevolver/evolution/custom-tasks`，因为克隆后的任务本质是一个新的 custom_evo 创建请求。
3. 可在 `CustomEvolutionCreateRequest` 中新增可选字段 `clone_from_task_id`，只用于新任务配置溯源，不参与结果复制。

克隆预填规则：

```text
Source Field                         Clone Form Behavior
source task_name                     default to "克隆 - {source task_name}"
source target_desc                   copy into target_desc and append source task id note
strategy_evo_config.loops            copy every loop config, preserving order
strategy_evo_execution_mode          prefill execution mode selector
strategy_evo_config.node_parallelism prefill per-node parallelism panel
loop node_id                         prefill each loop node, user can change per loop
loop label                           prefill loop description, user can edit
loop factor/model/strategy/HMM       prefill exactly, user can edit
source metrics/results/SOTA/assets   do not copy
```

重要规则：

1. 克隆窗口必须与新建自定义演进任务窗口完全一致，只是初始状态来自源任务。
2. 用户可以删除任意非唯一 Loop；至少保留 1 个 Loop。
3. 用户可以新增 Loop；新增 Loop 仍按当前新建逻辑默认继承 Loop 1 或当前选择的模板配置。
4. 用户可以修改每个 Loop 的所有配置，包括因子、Alpha158、模型、策略、训练周期、HMM、股票池、执行算法、执行节点和执行模式。
5. 克隆默认不复制源任务的结果、指标、SOTA、experiment_id、workspace、日志或资产。
6. 如果源 Loop 是 `backtest_only`，默认保留原 `model_source_task_id/model_source_loop_index`，并在 UI 提示“该克隆 Loop 将复用源任务模型；如需重新训练请关闭 backtest-only”。
7. 如果用户修改因子列表、Alpha158 口径或 label_horizon，沿用现有保护逻辑自动关闭不兼容的 backtest-only。

---

## 5. 后端服务层设计

### 5.1 新增核心 helper

建议在 `backend/services/quantevolver/qe_evolution_service.py` 或新模块 `backend/services/quantevolver/custom_evo_mutation.py` 增加：

```python
async def get_custom_evo_editable_config(task_id: str) -> dict: ...

def build_custom_evo_clone_seed(task_id: str) -> dict: ...

def validate_custom_evo_loop_payload(loop: dict, index: int) -> dict: ...

def normalize_custom_evo_mutation_nodes(loops: list[dict], node_id: str | None, node_parallelism: dict | None) -> tuple: ...

async def delete_custom_evo_loop_result(task_id: str, loop_index: int, *, kill_if_running: bool) -> dict: ...

async def rerun_custom_evo_loop(task_id: str, loop_index: int, loop_config: dict, execution_mode: str, node_parallelism: dict) -> dict: ...

async def append_custom_evo_loops(task_id: str, loops_config: list[dict], execution_mode: str, node_parallelism: dict) -> dict: ...

async def submit_custom_evo_selected_loops(task_id: str, loop_indexes: list[int], execution_mode: str | None = None) -> dict: ...

def recompute_custom_evo_task_status(task_id: str) -> str: ...

def recompute_task_sota(task_id: str) -> None: ...
```

复用要求：

- `validate_custom_evo_loop_payload` 必须复用当前 `/custom-tasks` 的校验逻辑：因子非空、模型非空、HMM 必须有 snapshot、backtest-only 必须检查模型来源、label_horizon、因子列表和 alpha158 口径一致。
- `normalize_custom_evo_mutation_nodes` 必须复用 `resolve_custom_loop_nodes`、`normalize_node_parallelism`、`preflight_qe_nodes`，保证节点解析与新建任务一致。
- `submit_custom_evo_selected_loops` 必须和 `submit_custom_evo_all_loops` 共享单 Loop 提交流程，但只调度传入的 loop indexes。

### 5.2 删除旧结果的精确范围

重新运行前必须删除以下数据：

```text
Object                         Cleanup Method                                  Reason
qe_evolution_loops             DELETE loop row                                 cascades SOTA/factor/model records
qe_sota_registry               cascade by loop FK                              remove old SOTA state
qe_loop_factor_records          cascade by loop FK                              remove old factor attribution
qe_loop_model_records           cascade by loop FK                              remove old model diagnostics
qe_factor_experiment_metrics    explicit DELETE by old experiment_id           experiment metrics are not under loop FK
qe_experiments                  explicit DELETE old loop experiment             avoid old experiment duplicate
local QE workspace              remove task/LoopN scoped dirs only              avoid stale local artifacts
local SOTA assets               remove task/LoopN scoped assets                 avoid stale deployable assets
remote workspace                delete task/LoopN on old node                   avoid stale qlib/mlruns/metrics files
```

注意：

- 不能删除整个任务 workspace，否则同任务其他 Loop 的结果可能被破坏。
- 因为当前 `QEWorkspaceClient` 只有 `cleanup_task_workspace(task_id)`，需要补充 `cleanup_loop_workspace(task_id, loop_id)`。
- 如果远端 loop-level cleanup API 暂时不可用，`rerun` 必须 fail-fast，不允许直接覆盖写入旧目录。

### 5.3 目标 Loop 各状态处理

```text
Loop Status     Pre Action Before Delete                     Allowed Result
completed       delete DB/artifacts/workspace                 submit new run
failed          delete DB/artifacts/workspace                 submit new run
cancelled       delete DB/artifacts/workspace                 submit new run
pending         cancel local pending row, cleanup workspace   submit new run
running         kill old remote loop, wait/verify stopped     submit new run
processing      same as running                              submit new run
unknown/null    require row/config exists, cleanup if any     submit new run
```

关键约束：

- `running/processing` 的旧远端执行如果无法停止，必须拒绝重新运行，避免同一个 `task_id/LoopN` 双执行。
- 删除 DB 前先记录旧 `experiment_id`、旧 `node_id`、旧 `loop_id` 用于 cleanup；但不持久化备份。
- 删除完成后才替换 `strategy_evo_config.loops[loop_index]` 并提交新运行。

### 5.4 只调度指定 Loop，避免误跑历史失败 Loop

当前 `submit_custom_evo_all_loops` 的行为是“跳过 completed，运行其他所有状态”，这不适合 append/rerun。

新函数 `submit_custom_evo_selected_loops` 的规则：

1. 只读取 `loop_indexes` 对应配置。
2. 不扫描、不重置、不运行其他失败/取消 Loop。
3. 支持 `serial` 与 `parallel_N`，并复用 per-node semaphore。
4. 完成后调用统一的 `recompute_custom_evo_task_status(task_id)`。
5. 对 append 的新增 Loop，按新索引运行。
6. 对 rerun 的替换 Loop，只运行目标索引。

### 5.5 任务状态与 SOTA 重算

重新运行和继续演进都会改变任务统计，需要统一重算：

1. `max_loops`：等于 `strategy_evo_config.loops` 当前长度。
2. `current_loop`：建议设为当前已存在 loop row 的最大 `loop_index`，避免 UI 进度为旧值；更准确的统计应由前端按 status counts 展示。
3. `status`：运行中为 `running`；所有配置 Loop 均 terminal 后，继续使用 `derive_custom_evo_final_status`，即只有全部 completed 才是 completed，否则 failed。
4. `is_sota` 与 `qe_sota_registry`：删除旧 Loop 后必须重算一次，避免旧 SOTA 指向被删除的结果；新 Loop 完成后再重算。
5. `metrics_json`、`config_json`、`agent_analysis`：重新运行后只保留新结果。

---

## 6. 前端交互设计

### 6.1 左侧演进拓扑

修改 `TopologyPanel`：

1. 对 `custom_evo` 每个 Loop 都显示“重新运行”按钮，不受状态限制。
2. 对 `failed/cancelled` 仍显示“重试”按钮，保持原 retry 行为。
3. 按钮文案区分：
   - `重试`：只选择训练/回测模式。
   - `重新运行`：编辑完整配置并覆盖旧结果。
4. 点击“重新运行”后打开 custom_evo 配置弹窗，默认加载该 Loop 的配置。

建议按钮显示规则：

```text
Task Type    Loop Status             Retry Button       Rerun Button
custom_evo   failed/cancelled         show               show
custom_evo   completed/running/etc    hide               show
auto/strategy failed/cancelled         show if supported  hide
```

### 6.2 继续演进按钮

在任务列表项或任务详情顶部增加“继续演进”按钮：

- 显示条件：`task_type === "custom_evo"` 且任务不是正在运行；建议对 `completed/failed/cancelled` 都显示，符合“包含失败 Loop 也允许继续”的要求。
- 如果任务内存在 `failed/cancelled` Loop，弹窗顶部显示提示：
  - “当前任务存在失败/取消 Loop，继续演进不会自动重跑这些 Loop；如果不修复它们，任务最终状态可能仍为 failed。”
- 默认配置：直接复制最高 `loop_index` 的配置，不判断该 Loop 的状态。
- 用户可以像新建自定义演进任务一样新增多个 Loop、修改所有字段、节点和执行模式。


### 6.3 任务列表克隆按钮

在任务列表每个 `custom_evo` 任务上增加 `克隆` 按钮：

1. 点击后调用 `GET /tasks/{task_id}/custom-evo-config` 读取源任务完整配置。
2. 将 `customEvoFormMode` 设置为 `clone`，打开与新建自定义演进任务完全一致的弹窗。
3. 预填全部 Loop、执行模式、默认节点和每节点并行度。
4. 任务名默认 `克隆 - {source task_name}`，目标说明默认复制源说明并追加源任务 ID，用户可修改。
5. 弹窗中允许删除部分 Loop、新增 Loop、修改每个 Loop 的全部配置和节点。
6. 点击确认后调用现有 `POST /custom-tasks` 创建新任务，不修改源任务。
7. 创建成功后刷新任务列表，并选中新创建的任务。

克隆按钮不应出现在自动演进和策略演进任务上；如果以后要支持这些任务，需要单独设计转换规则，不能把非 custom_evo 强行塞进 custom_evo 表单。

### 6.4 复用/抽取自定义演进编辑器

当前自定义演进表单集中在 `frontend/src/app/quantevolver/evolution/page.tsx`，建议抽取或最小复用为四种模式：

```ts
type CustomEvoFormMode = "create" | "rerun" | "append" | "clone";
```

状态建议：

```ts
const [customEvoFormMode, setCustomEvoFormMode] = useState<CustomEvoFormMode>("create");
const [customEvoTargetTaskId, setCustomEvoTargetTaskId] = useState<string>("");
const [customEvoTargetLoopIndex, setCustomEvoTargetLoopIndex] = useState<number | null>(null);
```

提交分支：

- `create`：继续调用 `POST /custom-tasks`。
- `clone`：预填来源任务配置后仍调用 `POST /custom-tasks` 创建新任务，可附带 `clone_from_task_id` 溯源。
- `rerun`：调用 `POST /tasks/{task_id}/loops/{loop_index}/rerun`。
- `append`：调用 `POST /tasks/{task_id}/custom-loops/append`。

### 6.5 弹窗内容要求

重新运行弹窗必须展示并允许修改：

- Loop 说明/label。
- 因子列表、是否禁用 Alpha158。
- 模型、模型参数或模型来源。
- 策略、策略参数、执行算法、执行算法参数。
- 训练周期：`label_horizon`，以及 `data_split`。
- HMM 是否启用、HMM 版本/snapshot、preset。
- 股票池、停牌过滤、尾盘未成交处理等运行参数。
- 执行节点、执行模式、每节点并行度。

前端需要保留当前已有的变更保护：

- backtest-only 模式下如果因子或 Alpha158 口径改变，自动关闭 backtest-only，避免复用不兼容模型。
- 后端仍必须重复校验，不能只靠前端。

### 6.6 提交后刷新

提交成功后：

1. 立即调用 `fetchTasks()`。
2. 立即调用 `fetchTaskDetail(activeTaskId)`。
3. 保持选中刚提交的 Loop：
   - rerun：选中原 `loop_index`。
   - append：选中第一个新增 `loop_index`。
4. 后续沿用现有轮询/SSE 机制刷新拓扑、轨迹、Loop 详情卡片。

---

## 7. 数据一致性与并发控制

### 7.1 任务级 mutation lock

新增 rerun/append 时建议使用 PostgreSQL advisory lock，避免同时点击多个修改操作导致 `strategy_evo_config` 丢更新。

策略：

- 在配置变更事务内使用 `pg_try_advisory_xact_lock(hashtext(task_id))`。
- 拿不到锁时返回 409：任务正在处理其他配置变更，请稍后再试。
- 不在整个训练期间持有 DB 锁；只锁“删除旧结果/更新配置/写 pending/running 行”这段短事务。

### 7.2 正在运行 Loop 的重新运行

因用户要求支持所有状态，目标 Loop 若正在运行：

1. 后端先读取旧 `node_id`。
2. 调用旧节点 `kill_loop(task_id, LoopN)`。
3. 查询状态确认不再运行，或 kill API 返回明确成功。
4. 执行 loop-level workspace cleanup。
5. 删除旧 DB 行和旧实验。
6. 写入新配置并提交。

如果旧节点不可达：

- 默认返回错误，不删除 DB，不提交新 Loop。
- 原因：无法确认旧进程是否仍在写同一个 workspace，直接覆盖会造成数据混淆。

### 7.3 历史失败 Loop 与继续演进

继续演进允许任务中存在失败 Loop，但不改变失败 Loop：

- 新增 Loop 成功后，如果旧失败 Loop 仍在，最终 `derive_custom_evo_final_status` 仍会返回 `failed`。
- 前端提示用户：如果希望任务最终变为 completed，需要单独 rerun 或 retry 失败 Loop。
- 演进轨迹表应显示每个 Loop 的状态、说明、模型、HMM、训练周期、CAGR、回撤等，便于比较新增 Loop 的价值。

---

## 8. 需要修改的文件清单

预计修改：

```text
File                                                                                  Change
backend/routers/quantevolver_evolution.py                                             add config/rerun/append request models/endpoints and clone source metadata
backend/services/quantevolver/qe_evolution_service.py                                 add rerun/append services, selected-loop scheduler, cleanup/status/SOTA recompute
backend/services/quantevolver/qe_workspace_client.py                                  add cleanup_loop_workspace client method
frontend/src/app/quantevolver/evolution/page.tsx                                      add form modes, clone/continue buttons, rerun/append/create submit branches
frontend/src/app/quantevolver/evolution/components/TopologyPanel.tsx                  add rerun action for every custom_evo loop
frontend/src/app/quantevolver/evolution/components/LoopDetailPanel.tsx                optional: expose rerun/continue action near loop detail if needed
docs/architecture/qe_custom_evo_rerun_continue_plan_20260501.md                       this implementation plan
```

可能需要同步修改但取决于 RD-Agent 当前 API 能力：

```text
External File                                                                         Change
F:/Dev/RD-Agent-main/rdagent/app/api_endpoints/qe_evolution_api.py                    add DELETE /tasks/{task_id}/loops/{loop_id} if absent
```

说明：如果不新增 RD-Agent loop-level cleanup API，则重新运行同一个 Loop 时无法保证 workspace 完全删除，应阻断 rerun，不建议用任务级 cleanup 替代。

---

## 9. 分阶段实施计划

### Phase 1：后端配置读取与校验复用

要做：

1. 新增 `GET /custom-evo-config`。
2. 抽出 custom_evo loop payload 校验函数，复用 create API 的校验逻辑。
3. 抽出 custom_evo 节点解析和 preflight 逻辑，保证 create/rerun/append 一致。
4. 增加后端单测覆盖：可编辑配置来源、backtest-only 校验、HMM 校验、节点校验。

验证：

```powershell
python -m py_compile backend/routers/quantevolver_evolution.py backend/services/quantevolver/qe_evolution_service.py
pytest backend/tests -q -k "custom_evo and config"
```

反模式：

- 不要从 `config_json.factor_list` 反推 `factor_keys` 作为新任务唯一来源，除非 `strategy_evo_config` 缺失且明确提示兼容降级。
- 不要只在前端校验 backtest-only 兼容性。

### Phase 2：Loop 级 cleanup 能力

要做：

1. `QEWorkspaceClient` 新增 `cleanup_loop_workspace(task_id, loop_id)`。
2. 如果 RD-Agent 缺 endpoint，新增 `DELETE /tasks/{task_id}/loops/{loop_id}`。
3. 本地 cleanup 增加 task/LoopN 级目录删除，不删除整个 task。
4. 实现 `delete_custom_evo_loop_result`，先 kill/cleanup，再删 DB。

验证：

```powershell
python -m py_compile backend/services/quantevolver/qe_workspace_client.py backend/services/quantevolver/qe_evolution_service.py
```

反模式：

- 不要调用 `cleanup_task_workspace(task_id)` 来清理单个 Loop。
- 不要在远端 cleanup 失败时继续提交同一个 Loop。

### Phase 3：重新运行 rerun API

要做：

1. 新增 `CustomEvoLoopRerunRequest` 和 `/loops/{loop_index}/rerun`。
2. 实现所有状态 Loop 的 pre-action：running 先 kill，terminal 直接 cleanup。
3. 删除旧 Loop 结果，替换 `strategy_evo_config.loops[loop_index]`。
4. 更新 `strategy_evo_execution_mode`、`node_id`、`node_parallelism`。
5. 调用 `submit_custom_evo_selected_loops(task_id, [loop_index])`。
6. 删除后和完成后都调用 SOTA 重算。

验证：

- completed Loop rerun 后，DB 中同一个 `loop_id` 只有一条新记录。
- old `qe_experiments` 被删除，新完成后生成新的 experiment_id。
- old SOTA/factor/model records 不残留。
- running Loop rerun 时，kill 失败必须阻断。

### Phase 4：继续演进 append API

要做：

1. 新增 `CustomEvoAppendRequest` 和 `/custom-loops/append`。
2. 后端分配连续新 `loop_index`。
3. 更新 `strategy_evo_config.loops`、`max_loops`、执行模式和并行度。
4. 只调度新增 Loop，不运行旧 failed/cancelled Loop。
5. 失败 Loop 存在且请求未确认 warning 时返回 400。

验证：

- 原任务 5 个 Loop，append 2 个后 `max_loops=7`，新增 index 为 6/7。
- 如果原 Loop2 failed，append 后只运行 6/7，不运行 2。
- append 完成后轨迹表和 Loop 详情能看到新增数据。

### Phase 5：前端 rerun/continue UI

要做：

1. `TopologyPanel` 对 custom_evo 所有 Loop 增加“重新运行”。
2. 保留 failed/cancelled 的“重试”。
3. 任务卡片或详情顶部增加“继续演进”。
4. 任务列表的 custom_evo 任务增加“克隆”按钮，点击后预填全部 Loop 和配置。
5. 抽取/复用 custom_evo 表单，支持 `create/clone/rerun/append` 四种提交模式。
6. rerun 默认加载目标 Loop 配置；append 默认复制最高 loop_index 配置；clone 默认复制源任务全部 Loop。
7. 有失败 Loop 时 append 弹窗显示 warning，并带 `ack_failed_loop_warning=true`。
8. clone 弹窗允许删除部分 Loop、新增 Loop、修改每个 Loop 的全部配置和节点。
9. 提交后刷新任务、拓扑、轨迹、Loop 详情。

验证：

```powershell
cd frontend
npx tsc --noEmit
```

反模式：

- 不要让“重新运行”只在 failed Loop 上显示。
- 不要把“重试”和“重新运行”的按钮文案混用。
- 不要在 continue 默认复制最后成功 Loop；用户已明确“不判断是否失败”。
- 不要让 clone 复制源任务结果、SOTA、experiment_id、workspace 或资产；clone 只复制配置。
- 不要把非 custom_evo 任务直接克隆成 custom_evo，除非以后单独设计转换规则。

### Phase 6：端到端验证

建议准备一个小型 custom_evo 测试任务：

1. 完成 Loop rerun：选一个 completed Loop，修改说明和因子，提交 rerun，确认旧结果消失、新结果出现。
2. 失败 Loop rerun：选一个 failed Loop，修改配置，确认能重新运行。
3. running Loop rerun：用可控长运行 Loop 验证 kill/cleanup 逻辑。
4. continue append：在已有任务末尾新增 2 个 Loop，确认历史失败 Loop 不被自动重跑。
5. UI 刷新：确认拓扑、轨迹表、Loop 详情卡片都展示新/覆盖后的数据。

最终验证命令：

```powershell
python -m py_compile backend/routers/quantevolver_evolution.py backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/qe_workspace_client.py
cd frontend
npx tsc --noEmit
```

---

## 10. 风险与处理方案

### 10.1 Workspace 旧产物混淆

风险：同一个 `task_id/LoopN` 重新运行，如果不先删除远端 Loop 目录，可能读到旧 `metrics`、旧 `mlruns`、旧 HMM 系数或旧回测结果。

处理：必须实现 loop-level cleanup；cleanup 失败就阻断 rerun。

### 10.2 追加新 Loop 时误跑旧失败 Loop

风险：直接调用 `submit_custom_evo_all_loops` 会运行所有非 completed Loop。

处理：新增 `submit_custom_evo_selected_loops`，append 只传新增索引，rerun 只传目标索引。

### 10.3 SOTA 状态残留

风险：旧 SOTA Loop 被 rerun 删除后，`is_sota` 或 registry 仍指向旧结果。

处理：删除旧 Loop 后立刻清空并重算 task 内 SOTA；新 Loop 完成后再重算。

### 10.4 任务最终状态可能仍 failed

风险：继续演进允许历史失败 Loop 存在，因此新增 Loop 全部成功后任务仍可能因为旧失败 Loop 而是 `failed`。

处理：前端 warning 明确说明；状态计算保持严格，不因为新增成功而掩盖旧失败。

### 10.5 并发点击导致配置丢失

风险：用户同时点 rerun 和 continue，可能覆盖 `strategy_evo_config`。

处理：配置变更事务使用 advisory lock；拿不到锁返回 409。

---

## 11. 验收标准

1. 自定义演进任意状态 Loop 都能打开“重新运行”配置弹窗。
2. 重新运行可以修改说明、因子、模型、策略、训练周期、HMM、节点、执行模式等完整配置。
3. 重新运行提交后，旧结果完全删除；同一个 Loop 在轨迹和详情中只显示新结果。
4. 继续演进可以在同一个任务中追加多个 Loop，默认复制最高 loop_index 配置。
5. 继续演进不会自动重跑历史失败 Loop；如果存在失败 Loop，前端必须提示。
6. 任务列表中的 custom_evo 任务可以点击“克隆”，打开预填全部 Loop 和配置的新建窗口。
7. 克隆窗口允许删除部分 Loop、新增 Loop、修改每个 Loop 的全部配置和执行节点。
8. 克隆创建的新任务拥有新的 `task_id` 和独立结果，不复制源任务指标、SOTA、experiment_id、workspace 或资产。
9. 新增/重跑/克隆创建 Loop 完成后，任务列表、左侧拓扑、演进轨迹表、Loop 详情卡片自动刷新。
10. 数据库中不存在同一个 `task_id + loop_index` 的重复可视化结果，也不存在旧 experiment 指标残留。
11. 如果远端旧 Loop 仍在运行或 workspace cleanup 失败，后端必须拒绝 rerun，不能冒险覆盖。

---

## 12. 本次实现落地记录

### 12.1 已实现范围

```text
Area                          Implemented Capability
backend router                custom_evo editable config / rerun / append APIs; create API accepts clone provenance
backend service               editable config extraction; loop result deletion; rerun mutation; append mutation; selected-loop scheduler; task status recompute
workspace client              loop-level cleanup client; 404 is fail-fast and does not fall back to task cleanup
RD-Agent workspace API        DELETE /tasks/{task_id}/loops/{loop_id}; missing loop dir returns explicit existed=false
frontend task list            custom_evo continue and clone buttons
frontend topology             every custom_evo loop exposes full-config rerun; failed/cancelled loops keep retry
frontend editor               one custom_evo editor supports create / clone / rerun / append modes
tests                         route-level dry-run tests verify rerun/append scheduling without real QE execution
```

### 12.2 不真实运行 QE 的验证口径

本次验证不启动真实 QE 训练/回测，不提交 RD-Agent 真实 Loop，只验证：

1. Python 代码可编译。
2. 前端 TypeScript 可编译。
3. rerun API 必须显式确认删除旧结果。
4. rerun API 只把目标 `loop_index` 放入后台 selected-loop 调度。
5. append API 只把后端返回的新增 `loop_index` 放入后台 selected-loop 调度。
6. workspace cleanup 使用 loop 级删除；远端 API 缺失或返回 404 时 fail-fast，不静默使用 task 级删除。

### 12.3 已执行验证命令

```powershell
python -m py_compile backend/routers/quantevolver_evolution.py backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/qe_workspace_client.py F:/Dev/RD-Agent-main/rdagent/app/api_endpoints/qe_evolution_api.py
python -m pytest backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q
cd frontend
npm exec tsc -- --noEmit
```

### 12.4 明确不做的事

1. 不在实现或验证中启动真实 QE 训练/回测。
2. 不把 rerun cleanup 降级为任务级 workspace 删除，避免误删同任务其他 Loop。
3. 不在 cleanup 失败时继续提交同一个 Loop，避免旧产物混入新结果。
4. 不自动重跑 append 前已失败/取消的历史 Loop。
5. 不复制 clone 源任务的结果、SOTA、experiment_id、workspace 或资产。

---

## 13. 后续可选增强

1. 在 Loop 详情页增加“复制为新增 Loop”快捷入口，本质调用 continue append 表单。
2. 增加批量 rerun 多个 Loop 的能力，当前先做单 Loop rerun，降低误删风险。
3. 增加任务级状态统计卡：completed/failed/running/pending 数量，比 `current_loop/max_loops` 更适合 custom_evo。
4. 增加每次 rerun/append 的前端操作确认摘要，但不持久化旧结果备份，避免与用户确认的“完全删除不保留”冲突。
