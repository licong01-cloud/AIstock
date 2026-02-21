# QE 任务自动演进系统（Auto-Evolution）架构与设计方案 v3.0

## 1. 核心架构升级：纯 API 通信与物理隔离

为了适应未来 **RDAgent 与 AIstock 部署在不同物理机**的场景，必须彻底废除“AIstock 直接读写 WSL/RDAgent 目录文件”的设计。

### 1.1 纯 API 交互原则
- **禁止直接文件操作**：AIstock 后端绝对不使用 `open()`, `shutil`, `os.walk` 等直接访问 RDAgent 的实验路径。
- **RDAgent 作为微服务**：RDAgent 侧必须启动并暴露一组专用的 RESTful API（例如 `/api/v1/qe_workspace/...`），提供以下功能：
  1. 接收配置并执行 `qlib` 回测任务（异步）。
  2. 获取特定任务/LOOP 的日志流 (Log Streaming)。
  3. 读取特定 LOOP 的 `qlib_res.csv` 或 JSON 指标。
  4. 打包并下载指定 LOOP 的模型资产（权重 `.pkl`、特征序列等文件流下载）。
  5. 级联清理/删除指定的实验工作区。
- **通过这种设计，AIstock 变成了纯粹的“大脑控制中心”，网络隔离彻底完成。**

### 1.2 模块化、代码复用与独立路由设计
为了防止现有的 API 文件过大，影响已有功能，演进模块在 AIstock 侧必须在架构上完全独立，但**在底层核心逻辑上完全复用**：
- **独立路由与前端**：新建 `backend/routers/quantevolver_evolution.py`，专门负责 `/api/v1/quantevolver/evolution/...` 下的所有自动演进接口。前端也会提供完全独立于当前 QE 的新页面，绝不影响现有 QE 单次实验功能。
- **底层代码复用**：虽然业务流独立，但每个 LOOP 的本质依然是单次实验。因此，**组装配置文件的逻辑、触发回测的代码、特别是实验结束后的多维度结果统计与图表分析脚本，都将被完全复用**。
- **独立服务**：新建 `backend/services/quantevolver/qe_evolution_service.py` 承载状态机与 Agent 调度逻辑。
- **独立通信客户端**：新建 `backend/services/quantevolver/qe_rdagent_api_client.py`，专门封装与物理隔离的 RDAgent 机器通信的网络请求（基于 `httpx` 或 `aiohttp`）。

---

## 2. 演进 Agent 角色与工作流（增强版）

基于 RDAgent 演进架构并在 QE 层面优化，设计 4 个 Agent：

1. **实验诊断分析师 (Experiment Analyst Agent)**
   - 职责：解析回测指标，诊断当前组合的瓶颈。
   - **输入强化**：完全复用现有 QE 单次实验跑完后生成的“多维度结果总结分析报告”（包括收益、回撤、分层图、相关性等多维数据）。这些既有成熟的统计分析结果将直接作为本 Agent 的核心输入（Context），极大提升其诊断准确性。
2. **SOTA 评估官 (SOTA Evaluator Agent)**【独立 Agent】
   - 职责：专门负责将本轮 LOOP 结果与全局历史 SOTA 数据库对比，独立输出判定结论（是/否 SOTA，以及超越的具体维度理由）。
3. **演进策略研究员 (Evolution Researcher Agent)**
   - 职责：核心大脑，根据诊断和目标，决策下一轮是调因子、调参数还是换模型。
4. **配置审查与构建员 (Config Reviewer & Builder Agent)**
   - 职责：防止幻觉，审查研究员输出的草案，生成标准的可以直接发给 RDAgent 执行的 JSON/YAML 配置。

---

## 3. 详细数据库表结构设计 (PostgreSQL)

由于要实现完备的状态追溯和 SOTA 维护，需在 AIstock 的 DB 中新增以下表结构：

### 3.1 `qe_evolution_tasks` (演进任务主表)
记录任务的宏观设定。
- `task_id` (VARCHAR PK): 演进任务唯一ID (如 `Evo_20260221_01`)
- `task_name` (VARCHAR): 任务名称
- `target_desc` (TEXT): 演进目标描述 (供 Agent 参考)
- `max_loops` (INT): 设定的最大演进轮数
- `current_loop` (INT): 当前执行到的轮数
- `status` (VARCHAR): 任务状态 (running, paused, completed, failed)
- `base_experiment_id` (VARCHAR): 初始起点的基础实验ID
- `created_at` / `updated_at` (TIMESTAMP)

### 3.2 `qe_evolution_loops` (演进轮次详细记录表)
记录每一次具体尝试的详细数据。
- `loop_id` (VARCHAR PK): 唯一轮次ID (如 `Evo_..._Loop_3`)
- `task_id` (VARCHAR FK): 关联主任务
- `loop_index` (INT): 第几轮 (0, 1, 2...)
- `action_type` (VARCHAR): 本轮动作类型 (factor_adjust, param_tune, model_switch)
- `config_json` (JSONB): 本轮发送给 RDAgent 执行的具体配置 (因子列表、模型及超参)
- `metrics_json` (JSONB): RDAgent 返回的回测指标
- `agent_analysis` (JSONB): 诊断分析师和研究员输出的思考过程和诊断报告
- `is_sota` (BOOLEAN): 本轮是否被 SOTA 评估官标记为 SOTA
- `status` (VARCHAR): 本轮执行状态 (pending, running, analyzing, completed, failed)
- `created_at` / `updated_at`

### 3.3 `qe_sota_registry` (全局 SOTA 组合榜单表)
供用户浏览和提取历史最高表现组合。
- `sota_id` (SERIAL PK): 自增ID
- `loop_id` (VARCHAR FK): 来源的具体演进轮次
- `evaluation_reason` (TEXT): SOTA 评估官给出的入选理由
- `model_assets_synced` (BOOLEAN): 模型权重和特征文件是否已被同步拉取到 AIstock 本地盘
- `local_asset_path` (VARCHAR): 在 AIstock 机器上的物理存放路径 (如已同步)
- `created_at`

---

## 4. 实时调度与日志流架构 (纯 API 方案)

1. **AIstock 侧后台调度 (Async Task)**
   - 使用 `asyncio` 和 `qe_evolution_service.py` 维护流转引擎。不需要人工干预，一个 LOOP 的 Agent 推理结束后，自动组装配置发起网络请求给 RDAgent 启动下一轮。
2. **基于 API 的实时日志流 (SSE)**
   - RDAgent 端暴露 `GET /api/v1/qe_workspace/{loop_id}/log_stream` (支持 Server-Sent Events)。
   - AIstock 端的独立接口 `/api/v1/quantevolver/evolution/tasks/{task_id}/logs` 作为代理，将 RDAgent 的日志流转发给前端浏览器。
   - 前端通过黑色大屏 Terminal 组件实时打印 QLib 训练日志和 Agent 思考输出，提供极其硬核透明的监控体验。

---

## 5. 模型资产的一键同步与实盘选股

- **按需同步机制**：并非所有试错 LOOP 都拉取大文件。只有当某个 LOOP 被标为 SOTA，或者用户在前端手动点击“一键同步至 AIstock”时，触发同步动作。
- **纯 API 下载**：AIstock 发起请求 `GET /api/v1/qe_workspace/{loop_id}/assets/download` 到 RDAgent 机器。
- **解压落盘**：RDAgent 将 `models/*.pkl` 和 `features_order.txt` 打包为 ZIP 流返回。AIstock 接收并在本地 `f:/Dev/AIstock/rdagent_assets/qe_sota_assets/...` 解压。
- **实盘接轨**：AIstock 将同步好的路径更新到 `qe_sota_registry`，实盘选股模块可直接读取这些 `.pkl` 进行每日增量预测。

---

## 6. 二期工程规划 (Phase 2): 知识库 (RAG) 与模型拓展

为保证第一期的快速落地且稳健，我们将两个极其高级但复杂的需求规划为 **二期实施目标**。

### 6.1 阶段二：引入长期演进知识库 (Knowledge Base / RAG - 文件存储方案)
- **RDAgent 知识库参考**：RDAgent 能够通过不断积累历史成功或失败的“经验”（如：哪些因子容易共线性，哪种市场适合哪种树模型深度），沉淀为经验文件。
- **QE 的 Agent 赋能 (File-based)**：在二期，我们将**不使用复杂的向量数据库**，而是参考 RDAgent 的原生做法，使用基于文件（如 JSON/YAML 经验日志库）的方式进行管理。每次 LOOP 的 `agent_analysis` (特别是失败的教训和超参敏感度) 都会被序列化到持久化经验文件目录中。
- **效果提升**：未来的 **演进策略研究员 (Researcher)** 在决策前，会先读取这些经验文件。比如：“发现记录显示，因子A与因子B组合会导致模型过拟合”，从而主动避开陷阱，使 Agent 具备真正的长期记忆和自我进化能力。

### 6.2 阶段二：拓展模型选择范围至所有 QLib 模型及新架构
- **当前限制**：第一阶段由于求稳，`Evolution Planner Agent` 只能在 QE 当前配置好的 `[LightGBM, XGBoost, CatBoost, 简单线性]` 等几个成熟模型中选择或调参。
- **二期拓展**：随着“配置构建员”能力的提升，我们可以允许它从更广泛的范围内挑选模型（包括 QLib 社区支持的所有深度学习模型如 Transformer、RNN 等）。甚至允许 Agent 自动调整模型结构层的配置参数。这需要大幅增强 RDAgent 侧接收任意未知模型配置并动态编译加载的能力。

---

## 7. AIstock 侧 UI 界面设计 (演进控制台)

前端采用深浅模式适配的现代仪表盘风格：

1. **左侧：演进指令舱 (Command Center)**
   - 顶部显示演进任务的全局目标与进度呼吸灯。
   - 下方为巨大宽阔的 **WebSocket 终端窗口 (Terminal)**，源源不断输出 RDAgent 执行流和 AIstock 的 Agent 思考流。
2. **中间：演进血脉拓扑树 (Evolution Tree)**
   - 类似 Git 的节点提交图。每一个小圈代表一个 LOOP。
   - 颜色区分状态，带动画的金色星星标注为 SOTA 节点。点击节点可切换右侧详情。
3. **右侧：LOOP 深度看板 (Loop Inspector)**
   - **智能差异对比 (Config Diff)**：高亮展示本轮配置对比上一轮究竟修改了哪些因子或超参。
   - **Agent 结案陈词**：优雅渲染诊断分析师和 SOTA 评估官的文字报告。
   - **雷达图与一键实盘按钮**：多维指标展示。下方提供醒目的 CTA 按钮：“获取实体资产 -> 将此组合部署至 AIstock 准备实盘”。
4. **独立的 全局 SOTA 殿堂 (Hall of Fame)**
   - 在主导航单独提供入口。列出所有演进任务中跑出的历史最强策略，支持直接导入实盘或作为下一次全新演进任务的基础起点（`base_experiment_id`）。

---

## 【总结与决策建议】

### 对您补充需求的响应与建议：
1. **纯 API 物理隔离**：完全采用方案，AIstock 不碰 WSL 物理文件。这要求我们在启动前端开发前，先定义好 RDAgent 侧必须暴露的约 5 个核心执行与下载 API。
2. **模块独立化**：同意。为了代码整洁，我们将在 AIstock 创建独立的路由文件和 Service 文件，不污染现有的单次实验逻辑。
3. **独立的 SOTA Agent**：已在架构中独立拆分（第 2.2 节）。
4. **二期 RAG 与模型拓展**：非常高瞻远瞩的需求，我们已在第 6 节中完成了远景规划。这有效避免了第一阶段开发周期失控，同时为系统的智能上限留足了空间。

**下一步决策点**：
本 v3.0 方案已经是一份极具实操性的架构蓝图。如果您认可目前的数据表设计和“独立路由/纯 API 交互”模式，我们可以开始实际行动。
第一步应该是：**创建 AIstock 数据库的演进表 (`qe_evolution_tasks` 等)**，然后建立新的路由骨架代码。您是否同意进入代码编写阶段？
