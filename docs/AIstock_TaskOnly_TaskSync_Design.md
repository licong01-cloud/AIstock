# AIstock Task-Only（Log 驱动）同步与选股设计方案（不依赖 Registry/Loop）

## 0. 目标与硬约束

本方案是 AIstock 与 RD-Agent 对接的**唯一约束方案（Single Source of Truth）**，用于：

- 以 **Task（log 目录名）** 为唯一主键执行同步与选股。
- **不使用** RD-Agent `registry.sqlite` 的任何数据（包括 task_runs/loops/workspaces 等表）。
- **不使用**任何 loop 概念、loop_id、asset_bundle_id 作为权威索引。
- 数据权威来源为：
  - RD-Agent 本地 `log/<task_id>/`（task_id = 目录名，例如 `2026-01-15_07-06-15-039369`）
  - RD-Agent 本地 `log/<task_id>/__session__/*`（session 快照，含 trace 与 sub_workspace_list.file_dict）
  - 仅在“按文档精确定位”可得的情况下，访问 `git_ignore_folder/RD-Agent_workspace/<workspace_id>/` 做**单点校验/补充**（禁止全盘遍历）。

## 1. 需求说明（本期一期范围）

### 1.1 业务目标

- **以 Task 为单位完成初始化同步与选股**：
  - 同步：在 AIstock 本地落盘 `rdagent_assets/rdagent_tasks/<task_id>/` 资产与 `manifest.json`，并落库 catalog。
  - 选股：AIstock UI 选择 `task_id` 后，直接使用该 task 的推理资产执行推理并返回 TopK。

### 1.2 核心业务规则（必须严格满足）

- **Task 唯一主键**：`task_id == log 目录名`。
- **选股模型权重的唯一来源（关键）**：
  - 每个因子的演进 loop 都使用**统一模型**进行回测。
  - AIstock 的 Task 选股必须使用：**“最后进入 SOTA 的因子所在 loop 的模型权重”** 作为该 Task 的选股模型。
  - **模型演进（model loop）与 SOTA 无关**，只使用 alpha 因子；因此：
    - 任何 model loop 的数据不作为选股依据。
    - “SOTA 模型列表”不作为选股依据。

- **SOTA 因子 => 必须可选股（严格模式）**：
  - 只要该 Task 存在 SOTA 因子（即 trace.hist 中存在因子实验且 `feedback.decision=True`），一期就必须具备可执行的 Task 选股能力。
  - 若同步阶段无法提取到可执行的因子实现（`factor_entry.py`）或无法提取到对应统一模型权重（`model.pkl`），应视为**同步逻辑缺陷/BUG**，需要从脚本逻辑与资产定位链路上修复。
  - **不允许**回退为“仅使用 alpha 因子执行选股”。

- **混合任务（因子/模型交替演进）的处理（一 期）**：
  - 一期只考虑使用 **SOTA 因子** 来确定选股资产（因子实现 + 统一模型权重）。
  - 任务中出现的“模型演进链路/模型 SOTA”与本期 Task 选股无关，不参与选股资产决策。

### 1.3 约束与边界

- **不得依赖**：
  - RD-Agent `registry.sqlite`
  - loop_id / task_run_id / asset_bundle_id 作为权威索引
  - AIstock 现有 loop 选股链路的 sqlite/loop catalog 等能力
- **允许新增/调整**：
  - RD-Agent Results API：允许按本方案需要扩展接口（例如返回 session 解析结果/权重定位结果），并可按需重启。
- **必须保留但不改动**（本期不做任何更新）：
  - AIstock 已有的 loop 级别同步/选股代码与页面
  - 其保留目的：未来即便某个 task 没有 SOTA 因子，也可能存在你认为有价值的 loop 回测产物，届时仍可通过 loop 级能力获取该 loop 的因子/模型数据。

## 2. 现状（已完成能力盘点）

### 2.1 AIstock 已有能力

- **[已存在]** Task 资产目录：`AIstock/rdagent_assets/rdagent_tasks/<task_id>/...`。
- **[已存在]** Task 同步脚本：`scripts/init_rdagent_task_assets.py`（可全量/指定 task，同步后写入 `aistock_task_catalog`）。
- **[已存在]** Task 同步服务：`backend/services/rdagent_task_sync_service.py::sync_task`：
  - 能从本地 log 的 session 快照中选择 “trace.hist 最长” 的 session 文件。
  - 能基于 session 的 trace 寻找 SOTA 因子与其后续模型实验（当前实现仍需按本方案调整，见差距）。
- **[已存在]** Task 选股路由：`POST /rdagent/tasks/{task_id}/selection`（当前实现复用 loop 选股，需要改造为 task-only）。

### 2.2 AIstock 已有 loop 级能力（本期保持不改动）

- loop catalog、loop 选股、以及相关的 registry/sqlite 依赖链路。

### 2.3 已完成工作（可复用的产物与代码）

以下工作已完成，可直接作为一期开发的基础（不代表已经满足“一期可选股”验收）：

- **[清理与取证工具链]**
  - `scripts/_outputs/scan_rdagent_task_result_status.json`
    - 已对 RD-Agent log 目录下选定的 52 个 task 做了 session hist 长度/decision 情况/部分 workspace 结果存在性检查。
    - 已识别 A 类任务：`hist_len=0` 的 13 个 task。
  - `scripts/_outputs/scan_rdagent_hist0_evidence.json`
    - 已对 13 个 A 类任务做取证：均无 `decision=True`、无 workspace 引用、无回测产物证据。
  - `scripts/cleanup_failed_rdagent_tasks.py`
    - 已支持 `--mode hist0_evidence --apply`：可按取证报告直接清理这些 A 类任务 log 与可关联 workspace。

- **[初始化同步入口]**
  - `scripts/init_rdagent_task_assets.py`
    - 已支持全量/指定 task 的初始化同步（幂等/force/limit/offset/max-tasks）。
    - 已支持写入 PG 表 `aistock_task_catalog`（首次运行会自动建表）。

- **[Task 同步服务（已有雏形）]**
  - `backend/services/rdagent_task_sync_service.py::sync_task`
    - 已具备：从 `log/<task_id>/__session__` 选择“trace.hist 最长”的 session 快照。
    - 已具备：尝试定位“最后进入 SOTA 的因子实验”与“其后续模型实验”（但当前实现仍存在缺口，见 2.4）。
    - 已具备：允许 Results API 不可用时继续生成本地 manifest（当前表现为 `sync_status=partial`）。

### 2.4 已知限制/阻塞点（导致之前无法落地的根因）

以下是**已被实际运行验证的阻塞点**（对应你提到的“持续开发很多天但一直无法落地”）：

- **[阻塞点1：Task 资产不完整，无法满足“一期可选股”]**
  - 现象：同步后 `rdagent_tasks/<task_id>/` 下经常缺失 `model.pkl`；或 `factor_entry.py` 为 stub（`compute()` 直接抛错）。
  - 直接后果：Task 选股无法执行（即使 UI/路由存在也会在推理阶段失败）。
  - 现象证据（历史样本）：
    - `rdagent_assets/rdagent_tasks/2025-12-19_08-42-46-183506/manifest.json`
      - `model_exp_index=null`、`primary_assets.model_weight_relpath=null`，且 `factor_entry.py` 为 stub。

- **[阻塞点2：权重定位未严格按 v2，且缺少“失败即诊断”的机制]**
  - 现象：当未找到 model_exp 或未能从 file_dict 提取权重时，同步仍可能以 `partial` 结束，但未将其作为“一期必须修复的失败”。
  - 直接后果：后续全量同步得到大量“看似完成但不可选股”的 task，无法收敛。

- **[阻塞点3：Task 选股路由仍复用 loop 选股链路]**
  - 现象：`POST /rdagent/tasks/{task_id}/selection` 目前会尝试从 manifest 推导 `task_run_id/loop_id` 并走 loop 推理链路。
  - 与本期约束冲突：一期 Task 选股必须独立，不得依赖 loop/sqlite/registry。

### 2.5 一期验收标准（开发前必须明确）

为避免“开发很多天仍无法落地”，一期以**可执行与可验证**为第一原则，验收标准如下：

- **[同步验收]** 对任意一个存在 SOTA 因子（`decision=True` 的因子实验）的 task：
  - 必须在 `rdagent_tasks/<task_id>/` 落盘：
    - `manifest.json`（可解析，schema_version=1）
    - `factor_entry.py`（可执行，不得为抛错 stub）
    - `model.pkl`（可被加载，来源为 v2 定位的后继模型实验 file_dict）
  - 若任一项缺失：同步必须返回失败，并给出可定位的 diagnostics（缺哪个 file_dict key、anchor index、model_exp index、session 文件名等）。

- **[选股验收]**
  - 对上述已成功同步的 task：调用 Task-only 选股 API 必须可返回 TopK，且运行期间不访问 registry.sqlite/loop。

- **[范围验收]**
  - 既有 loop 同步/loop 选股/loop 页面：本期不得改动其行为（允许仅做隔离与新增，不做逻辑更新）。

## 3. 现状与目标差距（必须整改点）

### 3.1 Task 选股链路当前仍复用 loop 选股（不符合 task-only）

- 当前 `/rdagent/tasks/{task_id}/selection` 会尝试从 manifest 推导 `task_run_id/loop_id` 并调用 loop 推理链路。
- 该设计与本方案的“**不使用 loop 概念**”矛盾，必须改造为**直接读取 task_dir 的推理资产**执行推理。

### 3.2 模型权重定位逻辑必须严格遵循《模型权重文件定位方案_v2》

- 当前同步实现存在两类风险：
  - 误把 model loop（alpha-only）当作选股模型来源。
  - 或在 Results API 不可用时缺失权重落盘，导致无法选股。
- 本期必须将“权重定位”收敛为：
  - **仅从 session 的 sub_workspace_list.file_dict 精确提取**（不遍历 workspace、不猜测）。
  - 以“最后进入 SOTA 的因子实验”作为 anchor，提取该因子实验对应 loop 的统一模型权重。

### 3.3 因子入口（factor_entry.py）落盘规则

本期的目标是：**只要存在 SOTA 因子，就必须让 Task 选股可执行**，因此 `factor_entry.py` 必须可运行。

落盘规则：

1) 必须能从 session `sub_workspace_list.file_dict` 中提取到可执行的因子入口代码（例如 `factor_entry.py` / `factor.py` / 约定的入口文件）。
2) 同步阶段将其原样复制为 `rdagent_tasks/<task_id>/factor_entry.py`。
3) 若仅能得到“入口20”的特征名列表但缺少可执行实现：
   - 该情况按业务规则视为同步逻辑缺陷（应修复定位链路，而不是回退到 alpha 因子选股）。
   - 同步结果应标记为失败并输出明确诊断信息（例如缺少哪个 file_dict key、anchor 指向的实验对象信息等），用于快速修复。

### 4.1 数据契约：Task 推理资产与 manifest schema（v1）

目录：`AIstock/rdagent_assets/rdagent_tasks/<task_id>/`

最小落盘文件：

```text
rdagent_tasks/<task_id>/
  manifest.json
  factor_entry.py
  model.pkl
  config.yaml (可选)
  diagnostics.json (可选)
```

manifest.json（v1）必须包含：

- `schema_version: 1`
- `task_id`
- `log_dir`
- `session_anchor`（用于审计与复现）
  - `source_session_dir_id`
  - `chosen_session_file`
  - `hist_len`
- `sota_factor_anchor`（选股唯一依据）
  - `last_sota_factor_index`
  - `last_sota_factor_workspace_path`（仅作审计，不作为推理读取路径）
  - `resolved_model_weight_key`（来自 file_dict 的 key）
- `primary_assets`
  - `factor_entry_relpath: "factor_entry.py"`
  - `model_weight_relpath: "model.pkl"`
  - `config_relpath`（可选）
- `selection_eligibility`
  - `is_ready: bool`（是否满足选股资产完整性）
  - `reasons: []`（不可用原因列表）

### 4.2 权重定位与落盘（严格按 v2 文档）

权重定位的唯一流程（摘要）：

1) 从 `log/<task_id>/__session__/` 选择 “trace.hist 最长” 的 session 快照。
2) 遍历 `trace.hist`，找到“最后一个 feedback.decision==True 的 **因子实验**”（SOTA 因子 anchor）。
3) 以该因子实验所在 loop 为依据，从对应 experiment 的 `sub_workspace_list.file_dict` 中提取模型权重：
   - 优先 key 命中 `model.pkl`
   - 其次 key 命中 `params.pkl`（兼容 mlruns/artifacts/params.pkl 等）
4) 将 bytes 写入 `rdagent_tasks/<task_id>/model.pkl`，并记录 `resolved_model_weight_key`。

重要声明：

- **禁止**使用 model loop（alpha-only）的任何权重或其 SOTA 列表。
- **禁止**遍历 workspace 根目录进行猜测。

### 4.3 因子入口（factor_entry.py）落盘规则

本期的目标是：**只要存在 SOTA 因子，就必须让 Task 选股可执行**，因此 `factor_entry.py` 必须可运行。

落盘规则：

1) 必须能从 session `sub_workspace_list.file_dict` 中提取到可执行的因子入口代码（例如 `factor_entry.py` / `factor.py` / 约定的入口文件）。
2) 同步阶段将其原样复制为 `rdagent_tasks/<task_id>/factor_entry.py`。
3) 若仅能得到“入口20”的特征名列表但缺少可执行实现：
   - 该情况按业务规则视为同步逻辑缺陷（应修复定位链路，而不是回退到 alpha 因子选股）。
   - 同步结果应标记为失败并输出明确诊断信息（例如缺少哪个 file_dict key、anchor 指向的实验对象信息等），用于快速修复。

### 4.4 AIstock 推理引擎：新增 Task-only 推理入口

新增独立入口（不复用 loop 选股链路）：

- `POST /rdagent/tasks/{task_id}/selection`：
  - 读取 `rdagent_tasks/<task_id>/manifest.json`，校验 `selection_eligibility.is_ready`。
  - 读取 `factor_entry.py` 与 `model.pkl`。
  - 以 `trade_date/cutoff_date` 获取实时/历史数据窗口。
  - 计算特征并加载模型进行推理。
  - 写入 `trading.rdagent_signal`（strategy_id 可用 `uuid5(task_id)`），返回 TopK。

注意：该入口必须保证：

- 运行期间不读取 registry.sqlite。
- 不依赖 loop_id/task_run_id。

### 4.5 UI（一期）

复用既有 task 列表与 task 选股页面（不创建重复页面入口）：

- 在既有页面中新增/增强展示：task_id、同步状态、是否可选股、同步时间、不可用原因（不可用时）。
- 在既有页面中新增/增强操作：
  - “同步/刷新该 Task”（触发本期 task-only 同步）
  - “启用/禁用用于选股”（仅对 `is_ready==true` 允许启用）
  - “一键选股”（trade_date/cutoff_date/top_k）

## 5. 初始化同步（全量幂等）

### 5.1 同步入口

- 脚本：`python -m scripts.init_rdagent_task_assets`
- 服务：`POST /rdagent/tasks/sync`

### 5.2 幂等策略

- 若 DB `sync_status in {success, partial}` 且 manifest sha1 未变化，则跳过。
- 提供 `--force` 强制重算（用于规则升级后重建资产）。

### 5.3 同步产物完整性校验

- `manifest.json` 必须存在且可解析。
- `model.pkl` 必须存在。
- `factor_entry.py` 必须存在且为可执行来源（非抛错 stub）。

## 6. Results API（本期必须实现）

说明：Task-only 的权威来源仍然是本地 log/session；但从产品与工程演进角度，AIstock 侧需要将 Results API 作为“统一的数据获取接口”，用于：

- 初始化同步（一次性批量拉取 task 元信息与资产锚点/诊断信息）
- 未来增量同步（按 task_id 或时间范围拉取变更）
- 跨机/远端同步（按 key 获取 file_dict bytes）

因此本期 Results API **必须补齐**，并且接口实现不得依赖 registry.sqlite、loop 概念或 SQLite。

本期必须新增接口（不依赖 registry.sqlite）：

- `GET /tasks/{task_id}/session_anchor`：返回 chosen_session_file、hist_len 等。
- `GET /tasks/{task_id}/sota_factor_anchor`：返回 last_sota_factor_index 与用于定位权重的 file_dict keys（不返回大文件 bytes）。
- `GET /tasks/{task_id}/asset_bytes?key=...`：按 key 返回 file_dict 中的 bytes（用于远端同步场景）。

同时建议新增（本期允许直接实现，便于 AIstock 初始化/增量）：

- `GET /tasks`：按时间范围/分页返回 task_id 列表与基础摘要（task_id 作为 log 目录名）。
- `GET /tasks/{task_id}/sync_summary`：返回用于 UI 展示与同步诊断的摘要（是否有 SOTA 因子、是否存在后继模型实验、可用的 file_dict keys、错误原因等）。

## 7. 保留 loop 级能力的策略（一期不改动）

- **[保留旧能力]** 既有 loop 级同步/选股功能保持不改动（一期只新增 task-only 能力）。
- **[前端复用]** AIstock 侧已经存在 task 列表与 task 选股相关页面与交互：本期必须复用与增强既有页面与接口，不创建“重复入口/重复页面”。
- **[Results API 主接口]** Results API 不是可选项：本次开发必须补齐 Results API（不依赖 registry.sqlite/loop/sqlite），并将其作为 AIstock 侧“初始化同步 + 未来增量同步”的主要数据获取接口。

## 8. 实施步骤（一期）

1) 文档对齐：固化本设计方案与验收标准。
2) 同步侧：
   - 将 `sync_task` 的“权重定位”严格改为以 SOTA 因子 anchor 的统一模型权重为准（按 v2 文档）。
   - 生成 `selection_eligibility` 并确保 UI 可判定是否可选股。
3) 推理侧：新增 Task-only 推理入口与服务实现。
4) UI：新增 Task 选股页面与交互。
5) 验证：
   - 先选一个历史上“多次 SOTA 因子”的 task 做同步与选股验证。
   - 通过后执行全量初始化同步。

## 9. 二期规划（本阶段不实施，但需预留）

二期目标（仅描述，不做详细设计）：

- **实验成果展示**：对所有 task 实现基于 log 目录内数据分析的实验成果展示（可追溯到 session/trace 证据链）。
- **双模式资产选择**：
  - Task 级：选择并同步 task 的 SOTA 数据（展示/对比/可追溯），并允许基于“Task SOTA 因子 + 相关模型能力”做更复杂的组合选股。
  - Loop 级：按你判定有价值的 loop，获取该 loop 的因子/模型/回测产物（复用既有 loop 功能）。
- **混合任务增强**：
  - 对“因子/模型交替演进”的混合任务，二期可考虑引入该任务内的 SOTA 模型做组合（与一期的“仅用 SOTA 因子”形成递进）。
- **模型演进 task 增强**：
  - 二期将补充“从专门的模型演进 task 中获取因子与 SOTA 模型结合进行选股”的能力。

## 附：本文件与《模型权重文件定位方案_v2》的关系

- 本文件定义“AIstock 的业务约束与落地形态（同步/推理/UI）”。
- 《模型权重文件定位方案_v2》定义“如何从 session 精确定位权重文件”的确定性算法。
- 实现时必须以 v2 的算法为准，且本文件中的任何“权重定位描述”都不能与 v2 冲突。
