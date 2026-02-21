# QE 任务自动演进系统（Auto-Evolution）架构与设计方案 v1.0

## 1. 核心需求与背景分析

### 1.1 业务目标
基于现有 QuantEvolver (QE) 的因子库和模型库，实现一个**自动化、智能化的策略演进闭环**。
用户设定初始组合（人工或LLM智能生成）和演进目标（如：提升ICIR，降低最大回撤），系统自动在给定的 LOOP 数量内进行迭代：
- **每一轮（LOOP）**只做单一方向的调整：① 因子组合微调（叠加alpha因子、增加、删除或替换）；② 模型超参数调整；③ 更换底层预测模型。
- **目标驱动**：每一轮的调整依据都是上一轮的详细回测结果和预设的终极目标。
- **SOTA 追踪**：全局维护 State-of-the-Art (SOTA) 历史组合列表，记录每次突破历史最佳表现的完整配置与结果、工作目录，支持一键提取。
- **实盘接轨**：所有 LOOP 和 SOTA 数据必须能同步至 AIstock 侧，直接用于实盘选股。

### 1.2 前提限制与基础设施
- **运行环境**：所有的 QLib 因子计算、模型训练和回测任务**必须且只能**运行在 RDAgent 侧的 WSL 环境中。
- **存储效率**：为防止多轮演进产生海量数据导致磁盘爆炸，各 LOOP 的工作区必须利用 WSL 的文件链接（软链接/硬链接）机制复用底层数据集，只独立保存本轮的日志、模型权重和特定特征配置。
- **无需新写因子**：本演进方案不包含在 LOOP 中从零开发编写新因子的代码，仅针对现有高质量因子库做组合优化与模型适配。

---

## 2. 演进工作流与 Agent 角色设计

为了实现科学、严谨的自动演进，建议设计以下 **4 个专属 Agent 角色** 协同工作：

### 2.1 核心 Agent 角色规划

1. **实验诊断分析师 (Experiment Analyst Agent)**
   - **职责**：在每个 LOOP 结束后，深度解析回测结果（IC, IR, 收益率, 换手率, 各分层收益曲线等详细数据）。
   - **输入**：本轮 QLib 详细结果、本轮使用的因子/模型配置、设定的演进目标。
   - **输出**：结构化的诊断报告。指出当前瓶颈（例如：头部收益衰减快 -> 可能是因子拥挤；多空收益不对称 -> 模型可能对空头信号过拟合），并明确距离目标的差距。

2. **SOTA 评估官 (SOTA Evaluator Agent)**
   - **职责**：对比本轮结果与全局历史 SOTA 记录。
   - **逻辑**：不单纯依赖绝对数值，LLM 可以综合判断（例如：“收益略有下降但最大回撤大幅改善，在当前目标下应记为另一种风格的 SOTA”）。若确实优秀，打上 SOTA 标记。

3. **演进决策者 (Evolution Planner Agent)**
   - **职责**：根据“诊断报告”和“演进目标”，推荐下一轮（LOOP n+1）的**唯一调整方向**（因子 / 超参 / 换模型）以及**具体动作**。
   - **输出**：下一轮的详细配置草案。
     - *方向1（因子）*：以现有因子库为基础，Drop 表现差的因子，Add 互补的 Alpha 因子。
     - *方向2（超参）*：保持因子不变，调整 LightGBM/XGBoost 的 `learning_rate`、`max_depth` 等。
     - *方向3（模型）*：若当前模型已达瓶颈，建议更换为特定类型的其他模型（如从线性切到树模型，或切到 DNN）。

4. **配置审查员 (Configuration Reviewer Agent)**
   - **职责**：对“演进决策者”给出的配置草案进行常识与合法性合理性分析审查。
   - **目标**：防止 LLM 幻觉产生不存在的因子名、越界的超参数、或者完全不合理的搭配（如试图让线性模型处理纯非线性因子组合）。
   - **输出**：通过（Approved）或打回重审（Rejected + 修改建议）。

### 2.2 自动演进的单轮生命周期 (LOOP)
`初始化实验 (LOOP 1)` -> `WSL 执行计算` -> `诊断分析师分析` -> `SOTA 评估` -> `演进决策者推荐 (LOOP 2)` -> `审查员审核` -> `WSL 执行 (LOOP 2)` ... 循环至设定的 `max_loops`。

---

## 3. 架构选型分析：控制程序运行在 AIstock 还是 RDAgent？

这是整个系统最关键的架构决策：控制自动演进（发派任务、调用Agent、控制Loop流转）的程序代码应该运行在哪里？

### 方案 A：控制器运行在 RDAgent 侧 (WSL 环境内)
把 Agent 调用、状态机和循环控制逻辑写在 RDAgent 的 Python 环境中。AIstock 触发任务后，RDAgent 自己闭环运行 10 个 LOOP。

*   **优势**：
    - 离执行环境最近，QLib 回测结束后可以直接在同一个 Python 进程中立刻启动 Agent 分析和下一轮执行，无需跨系统网络通信。
*   **劣势**：
    - **UI 交互极差**：Windows 侧的 AIstock 很难实时获取演进过程中的详细思考日志和状态，用户体验像个“黑盒”。
    - **LLM 配置重复建设**：AIstock 刚完成了完善的数据库 LLM 配置，如果在 RDAgent 侧跑 Agent，又需要解决 WSL 环境如何安全读取和动态切换 API Key 的问题。
    - **SOTA 维护困难**：全局 SOTA 应该持久化在 AIstock 数据库中，如果 RDAgent 控制循环，还需要繁琐地回调 AIstock 写入数据。

### 方案 B：控制器运行在 AIstock 侧 (Windows 端) - **【强烈推荐方案】**
把演进状态机、Agent 调用逻辑、SOTA 数据维护全部放在 AIstock 后端。AIstock 作为“大脑中枢”，RDAgent (WSL) 仅仅作为“任务执行引擎”。

*   **优势**：
    - **完美的 UI 掌控力**：AIstock 数据库主导，前端可以实时渲染“演进树”，用户能看到每个 LOOP 中 Agent 的诊断和思考过程。
    - **无缝衔接现有 LLM 架构**：直接利用刚重构的 `llm_client.py`，为不同环节分配不同的模型（如决策用 GPT-4o，审查用 Claude）。
    - **数据同源与持久化**：每一次 LOOP 的配置、执行结果、Agent 报告直接入库 PostgreSQL。天然支持演进任务的中断恢复和事后深度分析。
    - **同步自然完成**：AIstock 控制每次 WSL 执行结束后，直接触发前述设计的“实验数据同步”逻辑，实盘直接可用。
*   **劣势**：
    - AIstock 和 RDAgent 之间需要建立健壮的 API 通信机制（发派任务 -> 轮询/回调状态 -> 拉取结果）。但目前 AIstock 已有 `qmt_client` 和 `rdagent_http_sync_service` 的经验，技术上完全可控。

**结论**：强烈建议采用 **方案 B**。将复杂的业务逻辑（Agent 调度、SOTA 判断、循环控制）留在 AIstock，而高负载的计算（QLib）严格限制在 RDAgent WSL。

---

## 4. 底层文件目录结构与软链接机制设计

为了确保不额外占用空间，每个 LOOP 必须复用底层数据。

### 4.1 演进任务目录树设计 (RDAgent WSL 侧)
假设初始实验名为 `Exp_Alpha_001`：

```text
/RDAGENT_ROOT/qe_experiments/Exp_Alpha_001/
├── meta.json                      # 记录演进目标、最大LOOP数等元数据
├── global_data_links/             # 【核心】该演进任务共用的数据软链接池
│   ├── daily_pv.h5 -> /path/to/source/daily_pv.h5
│   └── static_factors.parquet -> /path/to/source/...
├── LOOP1/                         # 第1轮（初始）实验
│   ├── config.yaml                # 具体的模型和因子配置
│   ├── data_link -> ../global_data_links  # 链接到全局池
│   ├── qlib_res.csv               # 结果
│   └── models/                    # 模型权重文件 (本轮特有)
├── LOOP2/                         # 第2轮实验
│   ├── config.yaml                # 突变后的配置
│   ├── data_link -> ../global_data_links
│   ├── qlib_res.csv
│   └── models/
└── LOOP3/ ...
```

### 4.2 运行与同步机制
1. **执行隔离**：每个 LOOP 生成独立的 QLib 执行脚本，其工作目录 (`workspace`) 设为对应的 `LOOPX/`。脚本内读取数据时，使用相对路径 `data_link/`，这样既保证了数据的绝对一致，又没有复制任何大文件。
2. **AIstock 侧镜像目录**：当某个 LOOP 完成并被同步，或者被标记为 SOTA 时，AIstock 侧创建 `/rdagent_assets/qe_experiments/Exp_Alpha_001/LOOPX/`，将 WSL 中的 `models/` 权重文件和特征顺序文件拉取保存，以便实盘模块随时调用。

---

## 5. 数据库结构建议 (AIstock 侧扩展)

为了实现全局 SOTA 管理和 LOOP 追溯，建议在 AIstock 中增加/修改表结构：

1. **`qe_evolution_tasks` (演进任务主表)**
   - 记录：任务ID、初始实验ID、演进目标描述（Target）、最大循环数、当前进行到的 LOOP、任务整体状态。
2. **`qe_experiments` (实验记录表/LOOP表)**
   - 修改：增加 `evolution_task_id` 和 `loop_index` 字段。这意味着**每一次 LOOP 本质上就是一个标准的 Experiment**，它完美兼容现有的详情展示和运行机制。
   - 增加字段：`evolution_direction` (因子/超参/模型)、`agent_analysis_json` (保存当前 LOOP 诊断报告和决策)。
3. **`qe_sota_registry` (全局 SOTA 注册表)**
   - 记录：SOTA_ID、来源的 Experiment_ID、综合得分、对应的模型权重路径、确立为 SOTA 时的评语。提供 UI 列表展示。

## 6. 总结与后续步骤选择

**方案核心总结：**
- **4 Agent 协同**：诊断 -> 评估 -> 决策 -> 审查，单向数据流。
- **AIstock 主控**：所有智能逻辑和状态管理留在 Windows 侧，WSL 纯执行。
- **软链接隔离**：通过 `global_data_links` 共享数据，避免磁盘膨胀，同时隔离每个 LOOP 的权重产出。

请您审阅以上设计方案。特别是：
1. **是否认同将控制器放在 AIstock 侧？**
2. **4个 Agent 角色的划分是否符合您的期望？SOTA 评估是否需要独立 Agent 还是用硬规则？**

确认方向后，我们将进入下个阶段，输出具体的 API 接口和数据表 SQL 定义，并开始落地代码。
