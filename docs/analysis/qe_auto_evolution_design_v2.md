# QE 任务自动演进系统（Auto-Evolution）架构与设计方案 v2.0

## 1. 架构核心选型（AIstock 控制器方案）

根据深入讨论，明确采用 **方案 B**：**在 AIstock 侧实现整个任务的调度与状态管理，WSL 环境（RDAgent）仅作为实验计算的执行端。**

### 1.1 核心优势
- **全局状态掌控**：AIstock 数据库全盘掌握所有 LOOP 的进度、配置、日志和分析结果。
- **动态大模型调度**：直接复用 AIstock 侧强大的 `llm_client.py` 机制，为不同的演进环节分配不同的模型。
- **无缝衔接实盘**：AIstock 后端可以直接控制并拉取 WSL 中产生的模型权重文件和特征文件，一键接入实盘。
- **极致的 UI 体验**：通过 AIstock 的前端界面，可以提供类似 RDAgent 命令行的实时滚动日志，以及直观的演进拓扑图。

---

## 2. 借鉴 RDAgent 的 QE Agent 角色设计

参考 RDAgent 在因子/模型演进中的多智能体架构（Researcher, Coder, Executor, Summarizer 等），结合 QE “无需从零写代码，只需重组与调参”的特点，我们设计 **4 个独立的 Agent** 组成闭环流转流水线：

### 2.1 实验诊断分析师 (Experiment Analyst Agent)
- **类似 RDAgent 中的 Feedback / Reviewer**
- **触发时机**：每个 LOOP 的 WSL 物理回测执行完毕后。
- **输入**：本轮 `qlib_res.csv` 的各项指标（IC, IR, 收益率, 换手率等），以及当前的因子/模型配置。
- **输出**：深度的结构化诊断报告。例如指出“换手率过高导致费后收益崩塌”、“多空不对称”，并明确列出当前组合与用户设定的“终极演进目标”之间的具体差距。
- **自动入库**：该报告会作为该 LOOP 的核心分析资产，在回测一结束就立刻入库 AIstock 数据库。

### 2.2 SOTA 评估官 (SOTA Evaluator Agent)
- **独立评估角色（类似 RDAgent 中的 Summarizer/Leader）**
- **触发时机**：在诊断分析师完成报告后。
- **输入**：本轮各项指标、历史最佳（SOTA）组合的指标、用户的演进目标。
- **职责**：独立判断当前 LOOP 是否在“演进目标”的维度上超越了历史最佳。因为评估标准往往不是单一维度的（例如：收益率下降了 2%，但最大回撤减少了 10%，在稳健型目标下应判定为新 SOTA）。
- **输出**：`is_sota: bool` 以及评定理由。如果是 SOTA，触发 AIstock 状态机将该 LOOP 记录写入 `qe_sota_registry` 全局榜单。

### 2.3 演进策略研究员 (Evolution Researcher Agent)
- **类似 RDAgent 中的 Proposer**
- **触发时机**：SOTA 评估完成后。
- **输入**：演进目标、本轮诊断报告、当前是否是 SOTA、全局可用因子库列表。
- **职责**：作为大脑，思考下一步应该怎么走。决定在【①调整因子组合、②调整模型超参数、③更换模型】中选择唯一的方向，并给出具体的调整策略草案。
- **策略示例**：*“由于当前模型对高频资金流因子的捕获已达瓶颈，建议方向①：删除低效的 3 个基本面因子，从库中新增 2 个量价相关性因子。”*

### 2.4 配置审查与构建员 (Config Reviewer & Builder Agent)
- **类似 RDAgent 中的 Coder**
- **触发时机**：研究员给出策略草案后。
- **职责**：对草案进行“幻觉过滤”和“格式化构建”。它必须检查草案中提到的因子是否真的存在于 AIstock 因子库中，超参数范围是否合法。确认无误后，输出严格的、可以直接下发给 WSL 执行的下一轮（LOOP n+1） `config.yaml` 或 JSON 格式数据。

---

## 3. 基于 RDAgent API 的实时监控与自动化调度架构

为了解决“自动流转”和“实时日志可见”的需求，我们需要在 AIstock 后端设计一个强大的异步调度器引擎。

### 3.1 异步调度引擎 (AutoEvolutionScheduler)
1. **状态机驱动**：在 AIstock 后端 (`backend/services/quantevolver/qe_evolution_service.py`) 运行一个基于 `asyncio` 的后台任务队列。每个演进任务拥有严格的状态流转：`Init -> Exec_WSL -> Sync_Result -> Agent_Analysis -> Agent_SOTA -> Agent_Propose -> Exec_WSL(Loop+1)`。
2. **免人工干预**：状态流转全自动进行。当一个步骤完成（例如 RDAgent 执行完毕，或者 Agent 返回结果），事件循环立刻唤醒下一步，直到达到 `max_loops` 或达成演进目标才停止。

### 3.2 实时监控与终端日志推流 (Real-time Log Streaming)
为了在 AIstock 前端实现类似 RDAgent 命令行的实时黑框日志效果：
- **方案**：AIstock 后端通过 Server-Sent Events (SSE) 或 WebSocket 与前端通信。
- **底层实现**：由于 AIstock 和 WSL 运行在同一台物理机上，AIstock 后端在触发 RDAgent 的执行 API 后，可以通过 Windows 的挂载路径（例如 `\\wsl.localhost\Ubuntu\...\RDAGENT_ROOT\qe_experiments\Task_001\LOOP_1\run.log`）实时读取 WSL 的日志文件。
- **双向透明**：使用类似 `tail -f` 的文件系统监控实时将新增的模型训练日志推送给前端。同时，Agent 的每一次思考、诊断报告、下一轮计划的生成过程也会以流式（Streaming）形式推送到前端的监控大屏上。

---

## 4. LOOP 数据的自动同步入库与 SOTA 管理

### 4.1 每次 LOOP 结束后的自动即时入库
不需要等待整个演进任务结束。每一轮（LOOP）的 WSL 计算一跑完，AIstock 调度器会立刻：
1. **读取指标**：跨系统读取 WSL 中该 LOOP 的 `qlib_res.csv` 和回测图表数据。
2. **入库记录**：在 `qe_experiments` (将单个 LOOP 视作一个实验记录) 中保存本轮指标，以及本轮的具体配置（使用了哪些因子、模型名、参数）。
3. **Agent 结论同步**：诊断分析师和 SOTA 评估官生成的 JSON 分析结论，立刻保存到该 LOOP 的数据库 `agent_analysis_json` 字段中。

### 4.2 SOTA 全局历史数据维护
当 SOTA 评估官 (SOTA Evaluator Agent) 判定某一个 LOOP 取得了突破：
1. AIstock 会将其插入到全局的 `qe_sota_registry` 表中。
2. 这个表不仅记录得分和配置，还持久化保存该 SOTA 组合当时所对应的 WSL 工作目录路径，作为最高优的历史资产。

---

## 5. 模型资产的一键同步与实盘无缝接轨

- 并非所有 LOOP 都值得占用 Windows 侧的宝贵硬盘空间（很多试错模型只需保留在 WSL 中即可，甚至后续可以定时清理掉无用的模型权重）。
- **逻辑设计**：在 AIstock 前端 UI 上，对于任意一个已完成的 LOOP，或者是全局 SOTA 列表中的记录，用户可以点击 **【一键同步资产至 AIstock】** 按钮。
- **执行动作**：
  1. 后端访问该 LOOP 在 WSL 的专属 `models/` 目录。
  2. 将训练好的模型权重文件（如 `.pkl`, `.bin`），以及对应的 `features_order.txt`（特征值序列入口文件）物理拷贝至 AIstock 的实盘数据目录：`f:/Dev/AIstock/rdagent_assets/qe_experiments/{TaskID}/{LoopID}/`。
  3. 将该 LOOP 使用的准确因子组合列表、策略参数作为一条可用配置同步注册到 AIstock 的策略/模型运行配置中。
  4. 随后，用户可直接在 AIstock 的实盘选股模块中选取这个已同步的模型（和它的特征顺序）发起实盘预测。

---

## 6. UI 界面设计：高效、美观、直观的演进控制台

AIstock 侧的前端界面 (`/quantevolver/evolution`) 需要重新设计以容纳这套复杂的自动演进流。

### 6.1 页面布局设计
采用典型的 **左右两栏 / 仪表盘布局**，以深色模式（适合展现代码和图表）或极简冷色调为主：

#### 📍 左侧区：全局任务监控与实时终端 (Command Center)
- **任务概览面板**：显示当前演进任务名称、目标描述、已执行 LOOP 数 / 最大 LOOP 数、当前所处阶段（例如：`Agent 诊断中...`，通过闪烁呼吸灯或进度条提示）。
- **实时日志终端 (Terminal)**：类似 VSCode 终端的黑色背景、等宽绿色或白色字体。通过 WebSocket 实时滚动打印 WSL 侧的运行日志（模型加载、训练 progress），以及 Agent 思考时输出的流式中间过程。给用户“硬核、透明、实时”的掌控感。

#### 📍 右侧区：演进历史与 LOOP 深度详情 (Evolution Board)
- **演进拓扑图 (Timeline / Flow)**：横向或纵向的时间线展示所有的 LOOP。
  - **状态颜色**：灰（排队）、蓝（进行中）、绿（成功）、红（失败）、**带闪耀动画的金/紫边框标星（SOTA）**。
  - **交互**：点击任意一个已完成或正在执行的 LOOP 节点，下方内容区立刻刷新。
- **配置对比面板**：显示当前选中 LOOP 的具体模型超参、因子列表。若与上一个 LOOP 相比，直观地高亮显示（Diff 绿色加号、红色减号）增加/删除的因子，或修改的参数。
- **回测表现雷达**：直观对比本 LOOP 与初始 LOOP、SOTA 的多维指标（IC, 收益, 回撤, IR 等）。
- **Agent 分析报告卡片**：使用优雅的 Markdown 渲染“实验分析师”的文字诊断，以及“演进研究员”做出的决策理由。
- **一键同步实盘按钮**：巨大的行动号召按钮（CTA）。若尚未同步，显示“一键同步此模型及特征资产至 AIstock 实盘”；若已同步，则显示“已同步”并可点击跳转到实盘选股准备页。

### 6.2 独立 SOTA 排行榜面板 (SOTA Leaderboard)
除了单个演进任务的控制台，系统提供一个单独的全局 SOTA 榜单页面。汇总 AIstock 系统有史以来所有自动演进任务中诞生的最佳模型组合（包含完整的因子配置、模型配置、使用的策略）。用户可以按市场、策略风格筛选，直接“提取并同步”SOTA 配置到自己的交易流水线中。

---

## 7. 后续实施阶段建议

本 v2.0 方案已经完整覆盖了调度架构、Agent设计、同步落盘与 UI 全景。
如确认设计思路无误，我们将进入代码落地阶段，步骤如下：

1. **阶段 1 (数据库与调度基建)**：建立相关的 PostgreSQL 数据库表 (`qe_evolution_tasks`, `qe_evolution_loops`, `qe_sota_registry`)，并在 AIstock 后端编写 `AutoEvolutionScheduler` 异步状态机。
2. **阶段 2 (WSL 交互与实时监控推流)**：实现 WebSocket/SSE 日志推送服务，确保 AIstock 能够精确拉起 RDAgent 任务并实时监听其磁盘运行日志和返回状态。
3. **阶段 3 (Agent 链路接入)**：利用现有的 `llm_client.py` 串联起 4 个角色的 Agent Prompt，实现从“提取指标进行文本诊断”到“生成下一阶段合理 JSON 配置草案”的逻辑闭环。
4. **阶段 4 (前端工程与资产同步)**：开发 React 前端演进控制台页面，以及点击按钮即可执行的文件复制（模型权重、特征顺序文件）落盘接口和实盘接入逻辑。
