# Research Assistant MCP/Skill 执行闭环补充设计

> 日期: 2026-05-25
> 状态: Implementation design, pending code development；2026-05-26 补充 P0 类人对话治理前置整改；2026-05-26 追加自动模式切换架构作为最高优先级
> 适用范围: Research Assistant 下一阶段研发；先完成自动模式切换与类人主对话治理，再补齐 MCP/Skill 真实执行闭环
> 设计来源:
> - `docs/architecture/aistock_research_agent_console_design_20260520.md`
> - `docs/architecture/research_assistant_prompt_context_runtime_governance_design_20260524.md`
> - `docs/architecture/research_assistant_context_compression_design_20260524.md`
> - `tests/aistock_validation/history/research_assistant/20260525_l3_prompt_context_runtime_governance_validation.md`
> - 参考理念：Anthropic workflow/agent 分离、LangGraph checkpoint/human-in-the-loop、AutoGen human input mode、OpenHands agent loop、Aider/Continue 模式分离、Letta/MemGPT stateful memory、ReAct/Reflexion/Generative Agents、Microsoft Human-AI Interaction Guidelines；仅吸收设计原则，不引入替换性框架。

## -1. P-1 最高优先级：自动模式切换的人类助手架构

本节必须放在所有后续研发方案之前执行。它不是替换现有 Research Assistant 架构，而是在现有 Prompt Pack、Runtime Config、MCP catalog、Capability Registry、Approval、Trace、Context Pack 和 Workbench 之上增加一层 `Dialogue Mode Router + Mode State Machine`。后续所有 P0 类人对话治理、MCP/Skill 执行闭环、QE workflow、Issue workflow、Memory/Context 压缩与 UI 方案均必须与本节保持一致；如有冲突，必须先更新后续章节。

### -1.1 参考理念与取舍

| 参考对象 | 可借鉴理念 | AIstock 采用方式 | 不采用内容 |
| --- | --- | --- | --- |
| Anthropic workflow/agent 分离 | workflow 适合确定流程，agent 适合动态判断 | QE、Issue、Memory、MCP 执行作为 workflow；能力问答、概念解释、状态询问作为 direct dialogue | 不把所有对话都包装成 workflow |
| LangGraph checkpoint / human-in-the-loop | 状态可恢复，人工审批是状态节点 | 任务模式记录 `mode_state`、`approval_state`、`checkpoint_ref`；执行前从 task/action proposal 恢复 | 不引入 LangGraph 替换现有 FastAPI/DB 状态机 |
| AutoGen human input mode | 人类介入等级可配置 | 用 runtime config 定义 read-only、draft-only、write_nonprod、high_cost、production_sensitive 的确认/审批要求 | 不让模型自行决定高风险执行 |
| OpenHands agent loop | observe -> think -> act -> observe，但过程可追踪 | 内部保留 trace/event loop；主回答只给自然结果，过程进审计视图 | 不在主聊天展示 Thought/Action/Observation |
| Aider / Continue 模式分离 | Ask/Plan/Agent/Act 分离 | 自动路由到 `dialogue`、`analysis`、`planning`、`preflight`、`execution`、`audit`、`recovery` | 不要求用户手动切换模式作为默认入口 |
| Letta/MemGPT stateful memory | 记忆是 agent 状态，不是回答模板 | Context Pack 和压缩摘要只作为内部上下文；用户主动展开时才展示 | 不在普通回答中暴露 `Context Pack: 0 memories` |
| ReAct / Reflexion | 工具使用和复盘提升任务质量 | 用于内部工具编排、失败恢复、验证后复盘 | 不外显推理链，不把反思当成每轮回答 |
| Generative Agents / HAI Guidelines | 行为要符合人类预期、少打扰、可恢复 | 默认直接回答、最少澄清、主动但克制、错误可恢复 | 不做电影化角色扮演或夸张人格化 |

### -1.2 总体目标

1. **自动模式切换**：用户不需要手工选择模式；系统根据本轮消息、上下文、风险、任务状态和显式用户覆盖指令自动选择模式。
2. **默认对话优先**：普通问题、能力询问、概念解释、状态询问、方案讨论默认进入 `dialogue` 或 `analysis`，不启动任务 workflow。
3. **显式任务才规划**：只有用户明确要求创建、设计、诊断、修复、提交、校验、物化、运行、同步等任务时，才进入 `planning`、`preflight` 或 `execution`。
4. **风险门禁代码化**：高风险动作由状态机、approval policy、preflight 和 capability side-effect level 控制；不能只依赖 prompt。
5. **主回答类人化**：主聊天气泡只显示自然语言结论、必要澄清和简短结果；计划、Trace、Context Pack、raw JSON、工具事件默认进侧栏、Workbench 或 Audit 页面。
6. **现有架构增量整合**：保留现有数据库、Prompt Pack、runtime config、MCP/Skill、Trace、Approval、Context Pack 和 UI 页面，不引入替换性外部 agent 框架。

### -1.3 模式定义

| mode | 典型触发 | 主回答风格 | 工具权限 | 审计展示 |
| --- | --- | --- | --- | --- |
| `dialogue` | 问候、能力询问、概念解释、普通追问 | 直接、简洁、自然；不展示计划字段 | 默认不调用工具 | 不展示 |
| `analysis` | 只读原因分析、方案比较、状态说明、bug 可能原因 | 先给结论和依据，再给建议 | 只读工具可选，不能写 | 默认折叠 |
| `planning` | 明确要求制定方案、设计草案、整理执行步骤 | 给方案、参数、缺口和验收点；不执行 | 只读/草案能力 | 可显示简要计划 |
| `preflight` | 用户要求后续执行且参数基本齐全 | 展示检查结果、阻塞项、是否可执行 | 仅 preflight/validate 工具 | 展示必要检查 |
| `execution` | 用户明确确认执行且审批满足 | 汇报进度、结果和失败恢复 | 已批准执行工具 | Trace 默认折叠 |
| `audit` | 用户要求展开证据、Trace、验证矩阵、上下文 | 结构化证据和引用 | 只读审计工具 | 展示 |
| `recovery` | 上下文超限、工具失败、恢复任务、中断续跑 | 尽量无感；必要时简短说明恢复状态 | 压缩/恢复/只读检查 | 默认折叠 |

### -1.4 自动模式路由

模式路由必须是代码可测的决策层，输出结构化 `mode_decision`，至少包含：

```json
{
  "mode": "dialogue|analysis|planning|preflight|execution|audit|recovery",
  "intent_type": "capability_inquiry|concept_explanation|status_query|bug_diagnosis_request|issue_intake_request|experiment_draft_request|experiment_validation_request|experiment_execution_request|ambiguous_request|general_chat|audit_request|recovery_request",
  "confidence": 0.0,
  "mode_reason": "short machine readable reason",
  "requires_tool": false,
  "allowed_tool_side_effect": "none|read_only|draft_only|preflight|approved_execution",
  "requires_user_confirmation": false,
  "requires_approval": false,
  "visible_audit_default": false
}
```

路由规则：

1. 用户问“能否、是否可以、有什么能力、是什么、为什么、目前状态、是否已完成”时，默认 `dialogue` 或 `analysis`。
2. 用户说“帮我做、设计、创建、诊断、修复、提交、校验、物化、运行、同步”且对象明确时，才进入任务模式。
3. `QE`、`实验`、`回测`、`bug`、`Issue`、`MCP` 仅作为候选能力召回信号，不得单独改变模式到 workflow。
4. 用户显式说“只做分析、不改代码、不执行、不提交”时，强制不超过 `analysis`。
5. 用户显式说“确认执行、开始执行、按方案执行”时，也必须检查当前是否存在可执行 proposal、preflight、approval 和确认文本；不能直接执行。
6. 低置信度时问一个最小澄清问题；不得生成多项计划、不得追问股票池/时间窗等领域参数，除非已经是明确任务模式。

### -1.5 模式状态机

```text
user message
  -> mode router
  -> mode state update
  -> prompt node selection by mode + intent + risk
  -> context assembly by mode
  -> LLM response
  -> optional cards/trace/proposal
  -> persisted mode_decision and checkpoint
```

允许流转：

```text
dialogue
  -> dialogue        # 普通追问、能力询问、概念解释
  -> analysis        # 原因分析、只读诊断
  -> planning        # 明确要求方案/草案
  -> audit           # 要求证据/Trace

analysis
  -> dialogue        # 回到普通问答
  -> planning        # 用户要求形成方案
  -> preflight       # 用户要求检查是否可执行
  -> audit           # 用户要求展开证据

planning
  -> dialogue        # 用户取消任务或改问普通问题
  -> analysis        # 继续只读分析
  -> preflight       # 参数齐全且用户要求执行前检查
  -> audit           # 查看计划依据

preflight
  -> planning        # 检查失败或需补参数
  -> execution       # 检查通过且用户确认/审批满足
  -> audit           # 查看检查证据

execution
  -> recovery        # 工具失败、上下文超限、中断恢复
  -> audit           # 查看执行证据
  -> dialogue        # 任务结束后回到对话

recovery
  -> analysis        # 恢复后解释原因
  -> planning        # 需要修正方案
  -> execution       # 幂等恢复后继续已批准执行
```

禁止流转：

- `dialogue` 直接进入 `execution`。
- `capability_inquiry` 进入 `preflight` 或 `execution`。
- `concept_explanation` 生成 Action Proposal。
- 未经 preflight/approval 的 `planning` 进入高风险 MCP run。
- `recovery` 静默继续新的高风险动作。

### -1.6 Prompt Pack 选取原则

Prompt Pack 必须按模式隔离，不再让 direct-answer 场景加载 planning/workflow 节点：

| 模式 | 允许节点 | 禁止节点 |
| --- | --- | --- |
| `dialogue` | `root.identity`、`mode.dialogue`、必要的轻量领域说明 | `intent.planning`、`workflow.*`、`tool_guard.*`、任务审计模板 |
| `analysis` | `root.identity`、`mode.analysis`、只读领域节点 | `workflow.*` execution guard、Action Proposal 模板 |
| `planning` | `root.identity`、`mode.planning`、相关 domain/workflow draft 节点 | execute 节点、run guard，除非用户明确进入后续阶段 |
| `preflight` | `root.identity`、`mode.preflight`、`tool_guard.*` | 普通能力营销话术 |
| `execution` | `root.identity`、`mode.execution`、risk/approval/result renderer | 未批准工具说明 |
| `audit` | `root.identity`、`mode.audit`、trace/context renderer | 新任务 proposal |
| `recovery` | `root.identity`、`mode.recovery`、context compaction renderer | 新执行动作 |

`root.identity` 只能定义身份、总边界和主风格；不得包含“任务工作流必须说明目标、对象、约束、缺失信息、风险级别和下一步”这类会污染普通对话的模板。此类字段只允许出现在 `mode.planning`、`mode.preflight`、`mode.audit`，且仅在对应模式加载。

### -1.7 Context Pack 与上下文压缩展示原则

1. `dialogue`、`analysis` 默认不得向用户展示 `Context Pack`、`0 approved memories`、`0 temp memories`、prompt bundle、token budget 或压缩摘要。
2. Context Pack 作为内部上下文注入时，必须使用系统/开发者不可外显说明，明确“不要在回答中提及本上下文包或记忆数量，除非用户要求审计”。
3. 空 Context Pack 不应注入用户可见消息；避免诱导模型回答“当前上下文 0 条”。
4. 自动压缩应尽量无感执行；只有压缩失败、需要用户补充原始信息或用户主动要求展开时，才进入 `recovery` 或 `audit` 可见说明。
5. 压缩摘要、key facts、source ids 和 context assembly trace 必须保留在审计层，不能丢失用户确认、审批、风险边界、未完成任务和否定约束。

### -1.8 Runtime Config 新增项

所有模式、阈值、禁用词、展示策略和确认策略必须配置化，不得硬编码：

```yaml
dialogue_modes:
  default_mode: dialogue
  modes:
    dialogue:
      prompt_nodes: [root.identity, mode.dialogue]
      allow_tools: false
      allow_action_proposals: false
      expose_context_pack: false
      expose_audit_fields: false
      max_clarification_questions: 0
      forbidden_main_reply_phrases:
        - "目标："
        - "对象："
        - "约束："
        - "缺失信息："
        - "风险级别："
        - "下一步："
        - "Context Pack"
        - "可审计计划"
    analysis:
      prompt_nodes: [root.identity, mode.analysis]
      allow_tools: read_only
      allow_action_proposals: false
      expose_context_pack: false
      max_clarification_questions: 1
    planning:
      prompt_nodes: [root.identity, mode.planning]
      allow_tools: read_only
      allow_action_proposals: draft_only
      expose_context_pack: false
      max_clarification_questions: 3
    preflight:
      prompt_nodes: [root.identity, mode.preflight]
      allow_tools: preflight
      approval_required: true
    execution:
      prompt_nodes: [root.identity, mode.execution]
      allow_tools: approved_execution
      approval_required: true
    audit:
      prompt_nodes: [root.identity, mode.audit]
      allow_tools: read_only
      expose_audit_fields: true
    recovery:
      prompt_nodes: [root.identity, mode.recovery]
      allow_tools: read_only
      expose_audit_fields: false

mode_router:
  confidence_thresholds:
    direct_answer_min: 0.55
    task_request_min: 0.72
    execution_request_min: 0.86
  fallback:
    low_confidence_mode: dialogue
    ask_clarification_mode: analysis
    max_questions: 1
  user_overrides:
    analysis_only_patterns: ["只做分析", "不改代码", "不要执行", "只读分析"]
    execute_patterns: ["开始执行", "按方案执行", "确认执行"]
    audit_patterns: ["展开证据", "列出 Trace", "验证矩阵", "审计记录"]
```

### -1.9 主回答污染防护

`dialogue` 和 `analysis` 模式的主回答必须通过污染防护测试。以下内容默认禁止出现在主气泡中，除非用户明确要求展开审计或进入任务计划：

- `目标：`
- `对象：`
- `约束：`
- `缺失信息：`
- `风险级别：`
- `下一步：`
- `Context Pack`
- `0 条已审计记忆`
- `可审计计划`
- `我将生成计划`
- `请选择角色或模式`
- `确认问题：`
- raw JSON、trace id、task id、prompt bundle id

前端允许在侧栏显示状态，但默认应极简；普通对话不显示计划卡，或计划卡为空且折叠。

### -1.10 数据与 API 增量

建议在不破坏现有表的前提下增量记录：

- `research_agent_tasks.input_json.dialogue_mode`
- `research_agent_tasks.input_json.mode_decision`
- `assistant_conversation_messages.content_json.dialogue_mode`
- `assistant_conversation_messages.content_json.intent_type`
- `assistant_conversation_messages.content_json.mode_decision`
- `assistant_trace_events.payload_json.mode_decision`
- `assistant_action_proposals.mode_required` 或等价字段

如后续新增正式字段，必须走 DDL gate，并为表/字段添加 PostgreSQL comment。

### -1.11 P-1 验收矩阵

| 编号 | 验收项 | 标准 |
| --- | --- | --- |
| M0A | 自动模式路由 | 每轮返回 `mode_decision`，包含 mode、intent、confidence、tool/approval 边界 |
| M0B | 能力询问保持 dialogue | “你能做什么”“能否生成 QE 实验和诊断 bug”直接回答，不进入 planning/preflight/execution |
| M0C | 多轮模式切换 | “你能做什么” -> “通用能力” 仍保持 direct answer，不输出计划模板 |
| M0D | 关键词不触发模式升级 | 含 QE/bug/Issue/MCP 的能力、概念、状态问题不得生成 Action Proposal |
| M0E | 显式任务进入 planning | “帮我设计一个 QE 实验，先不运行” 进入 planning，只生成草案，不执行 |
| M0F | 确认执行必须经 preflight/approval | “确认执行” 没有有效 proposal/preflight/approval 时不能执行 |
| M0G | Context Pack 不外显 | direct-answer 主回复不得出现 Context Pack、记忆条数、token budget |
| M0H | 主回答污染防护 | dialogue/analysis 不出现目标/对象/约束/风险级别/下一步等计划字段 |
| M0I | 审计可展开 | 用户要求 Trace/验证矩阵时进入 audit，能展示证据且不创建新任务动作 |
| M0J | 配置化 | 模式、阈值、禁用词、工具权限、展示策略全部来自 runtime config/Prompt Pack/UI copy/DB activation |

### -1.12 与后续章节的关系

- 原 `P0 类人对话治理` 升级为本模式架构下的第一个落地切片，继续保留 E0A-E0H，但必须补充 M0A-M0J。
- 原 `Capability Registry`、`Action Proposal`、`Execution Gateway` 和 `QE workflow` 只允许从 `planning/preflight/execution` 模式进入。
- 原 `Context Compression` 设计并入 `recovery` 与内部 context layer；压缩不得打断用户连续使用，且不得丢失关键事实。
- 原 `Prompt Pack Runtime Governance` 继续作为配置与激活机制；但 prompt 选取必须由 mode router 控制，不再只按 intent 选取。
- 后续实现报告必须先证明 M0A-M0J，再证明 E0A-E0H 和 E1-E18。

## 0. P0 前置整改：类人对话治理与默认示例清理

本节是 `Phase -1` 自动模式切换通过后的第一落地切片，必须排在 Capability Registry、Action Proposal、MCP/Skill Execution Gateway 和 QE workflow 之前。后续任何方案、实现、测试或 UI 文案如果与 `Phase -1` 或本节冲突，均以 `Phase -1` 和本节为准并必须更新。

### 0.1 整改目标

1. **直接回答用户真实问题**：能力询问、概念解释、状态查询应先给结论；不得输出解释用户意图分类或声明“不执行动作”的元话术。
2. **先理解意图，再选择流程**：模型或意图层必须区分 `capability_inquiry`、`concept_explanation`、`status_query`、`bug_diagnosis_request`、`issue_intake_request`、`experiment_draft_request`、`experiment_validation_request`、`experiment_execution_request`、`ambiguous_request`、`general_chat`。
3. **关键词只用于候选召回，不能启动 workflow**：`QE`、`实验`、`回测`、`bug`、`issue`、`MCP` 等词只能帮助检索能力目录或上下文，不能单独触发草案、确认卡、preflight、materialize、run 或 issue 创建流程。
4. **彻底清理固定 QE loop 示例污染**：active prompt、runtime config、backend card、frontend 示例/placeholder、测试夹具和文档中的执行型示例不得把固定 loop 数当作默认任务；只有用户明确提出时才保留该参数。
5. **Bug 诊断是一等能力**：用户问“能否诊断 bug”或请求诊断时，应回答诊断能力、所需证据和可用入口；不得被 QE 草案流程覆盖。
6. **主回答保持简洁**：普通对话只显示必要结论和最少澄清；计划卡、确认卡、上下文健康、trace、候选能力和过程细节默认折叠到 side panel/debug drawer，不能拼接进主气泡。
7. **用户可见话术全部治理化**：能力说明、澄清问题、计划卡标题、确认文案、欢迎语、placeholder、示例问题和 workflow renderer 模板必须来自 Prompt Pack、runtime config、capability catalog、UI copy config 或 DB activation；不得硬编码在 Python/TSX 中。
8. **JARVIS-like 是交互风格，不是角色扮演**：目标是冷静、简洁、上下文感知、主动但克制、风险时一句话提醒、需要授权时清楚等待；不得加入电影化口癖、夸张自述或无关过程。

### 0.2 已确认的清理目标

下一阶段实现必须优先扫描并移除以下 active/runtime 可见默认示例，替换为参数化、非默认化或按用户输入生成的内容：

| 位置 | 当前问题 | 整改要求 |
| --- | --- | --- |
| `configs/research_assistant/runtime_context.yaml` | 示例能力文案包含固定 QE loop 数 | 改为通用 QE 实验草案示例，loop 数仅来自用户输入 |
| `backend/services/research_assistant/service.py` | `_build_human_cards()` 等路径含固定 QE loop 草案和确认问题 | 删除固定 loop 数；按 intent 和 capability schema 动态生成 |
| `prompt_packs/research_assistant/main/nodes/domain.qe_experiment.md` | domain 示例把特定 loop 任务作为触发样例 | 改为能力说明与条件化模板，不默认启动流程 |
| `frontend/src/app/research-assistant/chat/page.tsx` | chat 示例和 placeholder 暗示固定 QE loop 任务 | 改为能力问答、诊断、草案等中性示例 |
| `frontend/src/app/research-assistant/workbench/page.tsx` | workbench mock/dry-run 标题含固定 QE loop draft | 改为 generic draft 或从 proposal.title 读取 |

### 0.3 与执行闭环的关系

执行闭环仍然是下一阶段目标，但入口必须从“用户明确提出任务请求”开始，而不是从关键词匹配开始：

```text
User Message
  -> Intent Understanding
  -> Direct Answer or Clarification
  -> Capability Candidate Recall
  -> Action Proposal only for explicit task requests
  -> Confirmation / Preflight / Execute
```

因此，`Conversation -> Planner -> Action Proposal -> Confirmation -> Preflight -> MCP/Skill -> Trace -> Human Report` 只适用于 `experiment_draft_request`、`experiment_validation_request`、`experiment_execution_request`、`issue_intake_request` 等明确任务请求；不适用于能力询问、概念解释、状态查询和普通对话。

### 0.4 P0 验收用例

| 编号 | 验收项 | 标准 |
| --- | --- | --- |
| E0A | 能力询问直答 | 用户问“能否生成 QE 实验和诊断 bug”时，回答能力范围和所需输入；不输出意图分类元话术，不生成计划卡 |
| E0B | Bug 诊断直答 | 用户问 bug 诊断能力时必须覆盖诊断流程、可用证据和边界，不被 QE 草案覆盖 |
| E0C | 关键词不触发 workflow | 含 `QE`、`实验`、`bug`、`issue` 的概念/能力/状态问题不得创建 Action Proposal、preflight 或确认卡 |
| E0D | 默认 loop 清零 | active prompt、runtime config、backend、frontend 用户可见文案和测试不再包含固定 QE loop 执行型任务 |
| E0E | 显式任务才规划 | 只有用户明确要求“帮我设计/创建/运行/提交”时，才进入草案、确认或执行链路 |
| E0F | 主回答瘦身 | 主气泡不拼接过程卡；计划、trace、候选能力、上下文健康默认折叠或在 side panel 展示 |
| E0G | 用户可见文案外置 | 能力说明、确认问题、计划卡标题、placeholder、示例问题不得硬编码在 `.py` 或 `.tsx` 业务逻辑中 |
| E0H | 风格一致性 | 回答保持类人助手风格：少说过程、多给结论；风险和授权点简短明确 |

## 1. 目的与边界

本设计不是新的研发方向，而是在 `Phase -1` 自动模式切换和 P0 类人对话治理通过后，补齐既有 Research Assistant 设计中的执行闭环部分。

原设计已经定义 Phase 1 必须实现的主链路:

```text
Conversation -> Planner -> Action Proposal -> Confirmation -> Preflight -> MCP/Skill -> Trace -> Human Report
```

当前 PR `#198` 已完成 Prompt Pack、Runtime Config、上下文预算、自动压缩、Reactive compact、key facts、MCP/Skill 基础目录、preflight、approval、trace 和 dry-run 基础能力；但尚未实现真实 MCP/Skill 执行和端到端任务闭环。

在 `Phase -1` 与 P0 整改验收通过后，本设计补齐以下内容:

1. Capability Registry 与 MCP/Skill 能力同步。
2. Action Proposal、Confirmation、Approval 与 plan digest 绑定。
3. MCP/Skill Execution Gateway 的契约、状态机、幂等、超时、错误和 trace。
4. 首个强制验收 Workflow Pack: `qe.create_experiment`。
5. UI 执行态、审计回放和验证矩阵。

## 2. 非目标

以下内容不纳入本阶段，避免偏离既有研发方向:

1. 不引入 Temporal、LangGraph、Dify、Flowise 等外部工作流引擎作为 Phase 1 前置依赖。
2. 不绕过 AIstock 原生 Memory Ledger、Task Ledger、Approval、Trace 和 MCP catalog。
3. 不允许助手调用任意未登记、未批准、未审计的 MCP server 或 tool。
4. 不把 dry-run 结果伪装为真实执行。
5. 不自动执行生产敏感操作，不触碰生产 backend `8001`、frontend `3000` 或生产 DB。
6. 不把次模型、Verifier 或外部 agent 作为高风险 MCP 的直接执行者。
7. 不关闭 BUG/GitHub Issue，不执行 main 合入，除非后续实现任务有明确用户授权。

## 3. 当前能力基线

| 能力 | 当前状态 | 证据 |
| --- | --- | --- |
| Prompt Pack 文件化 | 已具备 | `prompt_packs/research_assistant/main/**` |
| Runtime Config 激活 | 已具备 | `configs/research_assistant/runtime_context.yaml` |
| 上下文预算与压缩 | 已具备 | `ResearchAssistantService._maybe_compact_prior_messages()` |
| Reactive compact + retry | 已具备 | `test_reactive_context_overflow_compacts_and_retries_without_user_interruption` |
| 高风险上下文超限 fail-fast | 已具备 | `test_high_risk_reactive_overflow_fail_fast_after_configured_retries` |
| MCP server/tool seed catalog | 部分具备 | `DEFAULT_MCP_SERVERS` / `DEFAULT_MCP_TOOLS` |
| MCP preflight | 已具备 | `/api/v1/research-assistant/mcp/preflight` |
| Approval request | 已具备 | `assistant_approval_requests` |
| Workbench dry-run | 已具备 | `/api/v1/research-assistant/workbench/dry-run-execute` returns `executed=false` |
| 自动模式切换 | 待实现 | 本设计 P-1 新增要求；当前仅有 intent-gated Prompt Tree 的第一版切片 |
| 真实 MCP/Skill execute | 未具备 | 需要本设计实现 |
| 全量能力同步 | 未具备 | 需要本设计实现 |
| QE 创建实验端到端 | 未具备 | 需要本设计实现 |

## 4. 设计原则

1. **Mode first**: 每轮先由可测试的 Mode Router 决定 `dialogue`、`analysis`、`planning`、`preflight`、`execution`、`audit` 或 `recovery`，再选择 prompt、context、tool 和 UI 展示密度。
2. **Dialogue first**: 先回答用户真实问题；能力问答、概念解释和状态查询不进入执行链路。
3. **Intent before workflow**: 先分类意图，再召回 capability；关键词不得单独触发 workflow。
4. **Capability first**: 助手只能从已同步、已批准、可审计的 Capability Registry 中选择能力。
5. **MCP/API first**: 所有业务动作通过 MCP/API/Skill gateway，不通过 UI 点击或隐式脚本。
6. **Proposal before execution**: 任何有副作用动作必须先生成 Action Proposal。
7. **Confirmation before preflight/execute**: 未确认不得进入有副作用 preflight 或 execute。
8. **Preflight before execute**: preflight 失败不得执行。
9. **Approval for high risk**: 高风险和 production-sensitive 必须绑定 approval、plan digest、config/version 和确认文本。
10. **Trace everything**: plan、preflight、approval、execute、result、failure 都必须可回放。
11. **Fail fast, no silent fallback**: 失败必须结构化返回，不得把失败伪装成成功。
12. **Runtime configurable**: retry、timeout、page size、catalog sync limit、risk policy、用户可见 copy 和 UI 展示开关等可调项进入 runtime config、Prompt Pack、UI copy config 或 DB activation。
13. **Original facts preserved**: 用户确认、参数、审批、执行结果和错误是结构化事实源，不依赖自然语言摘要。

## 5. 数据模型补充

现有表 `assistant_mcp_servers`、`assistant_mcp_tools`、`assistant_mcp_tool_events`、`assistant_approval_requests`、`assistant_trace_events` 和 `agent_task_events` 可支撑第一版执行闭环，但需要补充 Capability Registry 与 Action Proposal 概念。

### 5.1 `assistant_capabilities`

建议新增或等价实现以下 registry，用于让 Planner 知道“能做什么”。

```sql
assistant_capabilities
  capability_id TEXT PRIMARY KEY
  capability_key TEXT UNIQUE NOT NULL
  capability_type TEXT NOT NULL -- mcp_tool / skill / workflow_pack / composite
  title TEXT NOT NULL
  natural_language_triggers JSONB NOT NULL DEFAULT '[]'
  description_for_llm TEXT NOT NULL DEFAULT ''
  risk_level TEXT NOT NULL DEFAULT 'medium'
  side_effect_level TEXT NOT NULL DEFAULT 'read_only'
  required_confirmations JSONB NOT NULL DEFAULT '[]'
  preferred_model_role TEXT
  input_slots JSONB NOT NULL DEFAULT '{}'
  output_cards JSONB NOT NULL DEFAULT '[]'
  mcp_tool_refs JSONB NOT NULL DEFAULT '[]'
  skill_refs JSONB NOT NULL DEFAULT '[]'
  workflow_pack_ref TEXT
  status TEXT NOT NULL DEFAULT 'approved'
  source_ref TEXT
  checksum TEXT NOT NULL
  last_synced_at TIMESTAMPTZ
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

状态建议:

| status | 含义 |
| --- | --- |
| `draft` | 同步到但未批准，不能用于执行 |
| `approved` | 可供 Planner 选择 |
| `disabled` | 不再选择，但保留历史 |
| `deprecated` | 可回放旧 trace，禁止新任务 |
| `blocked` | 存在风险或依赖缺失，禁止选择 |

### 5.2 `assistant_action_proposals`

Action Proposal 是执行前的不可变计划快照。后续实现也可以先存在 `research_agent_tasks.triage_json` 或 `assistant_approval_requests.approval_context_json` 中，但最终建议独立表。

```sql
assistant_action_proposals
  action_proposal_id TEXT PRIMARY KEY
  task_id TEXT NOT NULL
  conversation_id TEXT
  capability_key TEXT NOT NULL
  proposal_type TEXT NOT NULL -- workflow_step / mcp_tool / skill
  title TEXT NOT NULL
  summary TEXT NOT NULL
  risk_level TEXT NOT NULL
  side_effect_level TEXT NOT NULL
  input_json JSONB NOT NULL DEFAULT '{}'
  expected_result_json JSONB NOT NULL DEFAULT '{}'
  plan_digest TEXT NOT NULL
  prompt_bundle_signature TEXT
  runtime_config_activation_id TEXT
  context_pack_id TEXT
  status TEXT NOT NULL DEFAULT 'proposed'
  approval_id TEXT
  idempotency_key TEXT NOT NULL
  expires_at TIMESTAMPTZ
  created_by TEXT NOT NULL DEFAULT 'assistant'
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

状态机:

```text
proposed -> confirmed -> preflight_passed -> approval_required -> approved -> executing -> succeeded
         -> rejected
         -> expired
         -> preflight_failed
         -> failed
         -> cancelled
```

### 5.3 `assistant_mcp_tool_events` 扩展建议

现有表已包含 `tool_event_id`、`task_id`、`server_key`、`tool_name`、`event_type`、`status`、`idempotency_key`、request/response/error JSON 和时间字段。执行闭环实现时建议扩展或写入 payload:

- `action_proposal_id`
- `approval_id`
- `plan_digest`
- `transport`
- `timeout_ms`
- `attempt_index`
- `duration_ms`
- `result_card_json`
- `artifact_refs`

如果不立刻改 DDL，第一版可以写入 `request_json` / `response_json` / `error_json`，但验证记录必须证明字段可回放。

## 6. Capability 与 MCP/Skill 同步

### 6.1 同步来源

允许来源:

1. AIstock 已注册 MCP server catalog。
2. 当前 repo 内 MCP module manifest 或 server tool-list API。
3. 本地 Codex Skill 目录和 AIstock skill registry。
4. 已批准 workflow pack 配置。

禁止来源:

1. 用户自由输入的任意 URL 或 tool name。
2. 未批准的 MCP server。
3. 未经过 checksum/version 记录的本地脚本。
4. 生产敏感工具的隐式 fallback。

### 6.2 同步流程

```text
sync request
  -> enumerate approved MCP servers
  -> call tool-list/manifest endpoint with timeout
  -> normalize schemas and metadata
  -> classify risk and side effect
  -> compute checksum
  -> dry-run diff
  -> apply only after explicit admin action or approved seed job
  -> write assistant_mcp_tools and assistant_capabilities
  -> write sync trace/report
```

### 6.3 风险分类

| side_effect_level | 示例 | 默认门禁 |
| --- | --- | --- |
| `read_only` | list/query/status/report | preflight 可自动执行，仍需 trace |
| `draft_only` | create candidate, build draft, dry-run validate | 用户确认后执行 |
| `write_nonprod` | create template, validation run, non-prod DB write | confirmation + preflight + approval |
| `high_cost_compute` | QE run, large validation, RDAgent job | approval + cost guard + progress trace |
| `production_sensitive` | production DB, live trading, restart, broker write | 本阶段禁止自动执行，必须用户单独授权 |

### 6.4 同步验证

- 新增 MCP tool 后必须有 input/output schema。
- 每个 capability 必须有 risk level、side effect、checksum、中文说明和可读触发条件。
- 每个写操作必须有 required confirmation 或 approval policy。
- 任何 disabled/blocked tool 不得进入 LLM 可选 catalog。
- 同步失败必须产生可读 error report，不得静默吞掉。

## 7. Planner 与 Action Proposal

### 7.1 Planner 输入

Planner 必须读取:

1. 当前用户消息。
2. Active Prompt Bundle。
3. Context Pack 与 key facts。
4. Capability Registry 中按 intent 召回的 top-k capability。
5. 当前 task/conversation 状态。
6. Runtime config、risk policy 和用户确认状态。

### 7.2 Planner 输出

Planner 不直接执行工具，只输出结构化 Action Proposal:

```json
{
  "capability_key": "qe.create_experiment_draft",
  "proposal_type": "workflow_pack",
  "title": "创建 QE 实验草案",
  "risk_level": "medium",
  "side_effect_level": "draft_only",
  "input_json": {
    "intent_type": "experiment_draft_request",
    "loop_count": "unspecified_until_user_provides",
    "stock_pool": "pending_if_required",
    "backtest_window": "pending_if_required",
    "materialize": false,
    "run": false
  },
  "required_confirmations": [
    "CONFIRM_QE_DRAFT_PARAMETERS"
  ],
  "next_step": "ask_only_missing_required_inputs"
}
```

### 7.3 Plan digest

`plan_digest` 必须覆盖:

- capability key/version/checksum
- input JSON canonical hash
- selected MCP tool refs
- prompt bundle signature
- runtime config activation id
- risk level and side effect
- approval requirements

任何参数、capability version、runtime config 或 prompt bundle 改变，都必须导致旧 approval 失效。

## 8. Confirmation 与 Approval

### 8.1 Confirmation 层级

| 层级 | 适用 | 机制 |
| --- | --- | --- |
| C0 | read-only 查询 | 可由 chat 明确同意或 UI 按钮触发 |
| C1 | draft-only | 用户确认一次 |
| C2 | write_nonprod | confirmation + preflight + approval request |
| C3 | high_cost_compute | approval + cost/range guard + progress acknowledgement |
| C4 | production_sensitive | 本阶段不自动执行；需要用户单独授权生产操作 |

### 8.2 Approval 失效条件

审批必须在以下情况下失效:

1. input JSON 变化。
2. plan digest 变化。
3. prompt bundle signature 变化。
4. runtime config activation 变化。
5. tool checksum/version 变化。
6. preflight result 过期。
7. task risk level 升级。

### 8.3 禁止行为

- 禁止把普通自然语言“好的”当作高风险执行确认。
- 禁止未展示 diff/preflight 就执行 materialize/run。
- 禁止 secondary_worker 或 verifier_critic 直接创建 approval 或执行 MCP。
- 禁止 approval 被复用于不同 proposal。

## 9. MCP/Skill Execution Gateway

### 9.1 统一执行契约

建议新增 API:

```text
POST /api/v1/research-assistant/actions/propose
POST /api/v1/research-assistant/actions/{action_proposal_id}/confirm
POST /api/v1/research-assistant/actions/{action_proposal_id}/preflight
POST /api/v1/research-assistant/actions/{action_proposal_id}/approve
POST /api/v1/research-assistant/actions/{action_proposal_id}/execute
GET  /api/v1/research-assistant/actions/{action_proposal_id}
GET  /api/v1/research-assistant/actions/{action_proposal_id}/events
```

第一版也可以复用现有 workbench endpoints，但必须保证语义分离:

- `dry-run-execute`: 只生成预演结果，永远 `executed=false`。
- `execute`: 真实调用 gateway，必须写 MCP event 和 trace。

### 9.2 执行前检查

`execute` 必须逐项检查:

1. proposal exists and status is executable。
2. capability/tool status is approved。
3. plan digest matches latest proposal。
4. preflight passed and not expired。
5. required approval exists and status approved。
6. idempotency key not already succeeded with different payload。
7. runtime risk policy allows execution。
8. target server health is ready。
9. timeout and retry policy resolved from runtime config。

任何检查失败必须返回 structured error，并写入 `agent_task_events` 与 `assistant_trace_events`。

### 9.3 Transport adapter

MCP transport 建议第一阶段支持:

| transport | 用途 | 说明 |
| --- | --- | --- |
| `loopback_http` | AIstock backend 本地 API/MCP facade | 默认开发模式 |
| `stdio_mcp` | 标准 MCP server process | 仅限 approved manifest |
| `python_module` | repo 内已批准 MCP module | 需要严格 allowlist |
| `external_http` | 远程 MCP/API | Phase 1 不默认启用，需显式配置 |

Transport adapter 必须统一返回:

```json
{
  "status": "succeeded|failed|partial|cancelled|timeout",
  "result_json": {},
  "result_cards": [],
  "artifact_refs": [],
  "error_json": {},
  "duration_ms": 0,
  "retry_count": 0
}
```

### 9.4 Timeout / Retry / Cancel

所有参数进入 runtime config 或 DB activation:

```yaml
execution:
  default_timeout_seconds: 60
  high_cost_timeout_seconds: 600
  max_retries: 1
  retryable_error_codes: [timeout, transient_network, rate_limited]
  non_retryable_error_codes: [approval_missing, schema_invalid, risk_policy_blocked]
  cancel_check_interval_seconds: 2
```

禁止无边界等待和后台静默继续。

## 10. Workflow Pack: `qe.create_experiment`

`qe.create_experiment` 是 Phase 1 修复版强制验收场景，但必须在 P0 类人对话治理通过后实施。实现顺序必须与原设计一致：先确认用户明确要求创建、校验、物化或运行实验，再进入 workflow。

### 10.1 Workflow steps

| step | 能力 | 类型 | 风险 | 门禁 |
| --- | --- | --- | --- | --- |
| 1 | 理解实验目标和边界 | planner | medium | 无副作用 |
| 2 | 读取 QE MCP/Skill 目录 | capability lookup | read_only | trace |
| 3 | 加载相关 Memory/规则 | context pack | read_only | trace |
| 4 | 询问缺失的窗口、股票池、模型资源和用户明确需要的迭代数量 | chat confirmation | medium | 用户确认；不得默认固定 loop 数 |
| 5 | 生成实验草案 | workflow_pack | draft_only | C1 |
| 6 | 创建/校验 template | mcp_tool | write_nonprod | C2 approval |
| 7 | 展示 validate diff/summary | renderer | read_only | trace |
| 8 | materialize | mcp_tool | high_cost_compute/write_nonprod | C3 approval |
| 9 | run experiment | mcp_tool | high_cost_compute | C3 approval + cost guard |
| 10 | 查询状态并汇报 | mcp_tool/skill | read_only | trace |

### 10.2 强制 guard

- 能力询问、概念解释、状态查询不得进入本 workflow。
- 不得因 QE、实验、回测、bug 等关键词自动进入本 workflow。
- 未确认不得 materialize。
- 未二次确认不得 run。
- 股票池、回测窗口、成本、节点健康、模板 diff 必须展示。
- preflight 失败不得执行下一步。
- 用户修改配置后旧 approval 必须失效。
- 失败时必须给出人类可读原因、下一步和审计链接。
- 不允许 raw JSON 作为主对话结果。

### 10.3 QE capability 映射

| capability_key | 目标 MCP/Skill | 说明 |
| --- | --- | --- |
| `qe.create_experiment_draft` | workflow pack | 生成草案，不执行 |
| `qe.validate_template` | QE MCP validate tool | 校验模板 |
| `qe.materialize_template` | QE MCP materialize tool | 创建 pending/正式实验对象 |
| `qe.run_experiment` | QE MCP run/submit tool | 启动 loop/run |
| `qe.analyze_result` | QE diagnostics skill | 结果分析 |

实际工具名必须由 capability sync 从 MCP manifest 获取，不能在 prompt 或 planner 中硬编码假定名称。

## 11. UI 执行态

### 11.1 Chat 主入口

Chat 主入口必须先按模式决定展示密度。`dialogue` / `analysis` 主气泡只展示自然语言结论、必要依据和最多一个澄清问题；以下卡片只在任务模式或侧栏/折叠区呈现:

- Intent summary card
- Missing slots card
- Action proposal card
- Risk/approval card
- Preflight result card
- Execute progress card
- Result / failure card

普通对话不得把目标、对象、约束、风险级别、下一步、Context Pack 或计划卡拼接到主气泡；任务模式也应优先给用户可读摘要，raw JSON 只能进入调试抽屉。

### 11.2 Workbench

Workbench 从 dry-run 升级为执行控制台，至少包含:

1. capability/tool selector。
2. input form from JSON schema。
3. diff preview。
4. preflight result。
5. approval request/status。
6. execute button with disabled reason。
7. event timeline。
8. result cards and artifact refs。
9. failure reason and retry/cancel guidance。

### 11.3 进度更新

第一版可使用 polling，后续支持 SSE/WebSocket。无论 UI 方式如何，事实源必须是 `agent_task_events`、`assistant_mcp_tool_events` 和 `assistant_trace_events`。

## 12. 多模型边界

本阶段保留原设计中的主模型/次模型/Verifier 边界:

| 角色 | 允许 | 禁止 |
| --- | --- | --- |
| `primary_orchestrator` | 生成计划、风险判断、最终回复、发起 proposal | 绕过 approval 直接执行高风险 MCP |
| `secondary_worker` | 草案、摘要、日志分析、低风险结构化结果 | 直接执行 MCP、写 approved memory、直接答复用户最终结论 |
| `verifier_critic` | 复核草案和执行前风险 | 执行 MCP、替代用户确认 |
| `router_model` | 候选 prompt/tool/model 推荐 | 决定高风险执行 |
| `long_context_reader` | 长日志/文档证据摘要 | 无证据改写事实源 |

所有 WorkerResult 必须 schema 校验，通过 trace 关联 prompt bundle signature、model profile 和 context pack。

## 13. Runtime Config 补充项

新增可调参数必须进入 runtime config 或 DB activation，不得写死在 Python 中:

```yaml
capability_sync:
  enabled: true
  max_servers_per_run: 20
  max_tools_per_server: 500
  timeout_seconds: 30
  require_checksum: true

planner:
  candidate_capability_top_k: 12
  require_action_proposal_for_side_effects: true

execution:
  default_timeout_seconds: 60
  high_cost_timeout_seconds: 600
  max_retries: 1
  retryable_error_codes: [timeout, transient_network, rate_limited]
  non_retryable_error_codes: [approval_missing, schema_invalid, risk_policy_blocked]

approval_policy:
  approval_ttl_minutes: 60
  expire_on_input_change: true
  expire_on_tool_checksum_change: true
  expire_on_runtime_config_change: true
  production_sensitive_auto_execute: false

ui_execution:
  event_poll_interval_seconds: 2
  show_raw_json_debug_drawer: true
  raw_json_main_view: false
```

## 14. 验证矩阵

| 编号 | 验收项 | 标准 |
| --- | --- | --- |
| M0A-M0J | P-1 自动模式切换与主回答隔离 | 先通过本设计第 -1.11 节全部验收；否则不得进入 P0、Capability Registry 或执行闭环开发验收 |
| E0A-E0H | P0 类人对话治理 | 依赖 M0A-M0J 通过；再通过本设计第 0.4 节全部验收，否则不得进入执行闭环开发验收 |
| E1 | Capability sync | 从 approved MCP/Skill 来源同步 catalog；disabled/blocked 不进入可选列表 |
| E2 | Capability schema | 每个 capability 有 schema、risk、side_effect、checksum、中文说明 |
| E3 | Planner proposal | 有副作用任务只生成 Action Proposal，不直接 execute |
| E4 | Plan digest | 参数/工具/runtime/prompt 变化导致旧 approval 失效 |
| E5 | Preflight gate | preflight 失败不得 execute，写事件和 trace |
| E6 | Approval gate | 高风险无 approval 不得 execute |
| E7 | Dry-run boundary | dry-run 永远 `executed=false`，不能写真实业务结果 |
| E8 | Execute gateway | 成功 execute 写 `assistant_mcp_tool_events`、`agent_task_events`、`assistant_trace_events` |
| E9 | Timeout/retry | 超时、可重试、不可重试错误均按 runtime config 处理 |
| E10 | Human-readable result | 主 UI 不以 raw JSON 作为业务结果 |
| E11 | QE workflow draft | 可生成 QE 实验草案，不 materialize |
| E12 | QE validate | 用户确认后 validate，展示 diff/summary |
| E13 | QE materialize gate | 未二次确认不得 materialize |
| E14 | QE run gate | 未 run confirmation 和 cost guard 不得 run |
| E15 | Failure recovery | 失败返回人类可读原因、下一步和审计链接 |
| E16 | Multi-model boundary | secondary/verifier 不直接执行高风险 MCP |
| E17 | Production boundary | 不触碰生产 `8001/3000` 和生产 DB，除非用户单独授权 |
| E18 | Design compliance | 实现报告逐条映射本矩阵到代码、测试、API/UI/trace 证据 |

## 15. 实施阶段

### Phase -1: 自动模式切换与主回答隔离

- 实现 Mode Router，持久化 `mode_decision`，覆盖 `dialogue`、`analysis`、`planning`、`preflight`、`execution`、`audit`、`recovery`。
- 按模式隔离 Prompt Pack：`dialogue` / `analysis` 不加载 planning、workflow、tool_guard 等任务模板；`planning` / `preflight` / `audit` 才加载对应节点。
- 按模式隔离 Context Pack：direct-answer 场景不外显 Context Pack、记忆条数、token budget 或压缩摘要；Context Pack 只作为内部上下文和审计证据。
- 增加主回答污染防护，覆盖目标、对象、约束、风险级别、下一步、Context Pack、可审计计划、确认问题、raw JSON 等不应出现在普通对话主气泡的内容。
- 验证 M0A-M0J，未通过前不得启动 Phase 0 或 Phase A-F 的完成验收。

### Phase 0: P0 类人对话治理与默认示例清理

- 在 Mode Router 基础上实现 intent understanding，明确区分能力问答、概念解释、状态查询、bug 诊断、issue intake、实验草案、实验校验和实验执行。
- 清理 active prompt、runtime config、backend card、frontend 示例和测试中的固定 QE loop 执行型内容；loop 数只能来自用户输入、任务参数或 capability schema。
- 将用户可见能力说明、澄清问题、计划卡标题、placeholder 和示例问题迁移到 Prompt Pack、runtime config、capability catalog、UI copy config 或 DB activation。
- 修改 chat 主气泡渲染，默认不拼接 plan、clarification、proposal、context health；这些内容进入折叠面板或 side panel。
- 验证 E0A-E0H，且必须依赖 M0A-M0J 通过；未通过前不得启动 Phase A-F 的完成验收。

### Phase A: Capability Registry 与 sync dry-run

- 新增 `assistant_capabilities` 或等价 registry。
- 新增 capability sync service 和 dry-run diff。
- 从现有 `assistant_mcp_servers`、MCP manifest、Skill registry 同步 metadata。
- 验证 E1、E2。

### Phase B: Action Proposal 与 approval digest

- 新增 proposal create/confirm/preflight API。
- 绑定 plan digest、input hash、tool checksum、runtime config activation、prompt bundle signature。
- 审批失效逻辑落地。
- 验证 E3、E4、E5、E6。

### Phase C: Execution Gateway

- 新增 execute API。
- 实现 loopback_http / approved local adapter 第一版。
- 统一 timeout/retry/error/result contract。
- 写入 task events、MCP tool events、trace。
- 验证 E7、E8、E9、E15、E17。

### Phase D: QE create experiment workflow

- 定义 `qe.create_experiment` workflow pack。
- 连接 QE template draft/validate/materialize/run/status tools。
- 完成多次确认和 guard。
- 验证 E11-E14。

### Phase E: UI execution closure

- Chat action cards。
- Workbench execute controls。
- Approval/result/failure timeline。
- 验证 E10、E15、E18。

### Phase F: Multi-model delegation boundary

- WorkerResult schema。
- verifier_critic 复核。
- 主模型最终合并和确认。
- 验证 E16。

## 16. 与当前 PR #198 的关系

PR `#198` 提供本设计的前置能力:

1. Prompt Pack 和 runtime config governance。
2. Context Pack、context compression、key facts 和 trace。
3. MCP/Skill seed catalog、preflight、approval、dry-run。
4. A1-A10 的上下文治理验收证据。

本设计应作为后续独立开发包实施。实现时必须从最新 `origin/main` 创建独立 worktree/branch，不得复用当前 PR 的功能分支，也不得把其他模块 issue 混入。

## 17. 生产门禁

设计文档本身不需要生产 DDL。

后续代码实现若新增/修改 DB 表或字段:

1. 必须有 PostgreSQL comment。
2. 合入 main 后、生产 runtime activation 前必须执行 committed DDL。
3. 最终报告必须明确 `production_ddl_gate`。
4. 未验证生产 DDL 时必须标记 `production_ddl_pending`，不能宣称 production-ready。

## 18. 结论

下一阶段方向与既有研发设计一致，不是新的替换路线，也不引入替换性外部 agent/workflow 框架。本次调整只是把多个工具与研究中的可借鉴理念，整合到 AIstock 现有 Research Assistant 架构中，形成更接近人类助手习惯的自动模式切换、主回答隔离和可审计任务执行链路。

后续优先级必须调整为：

1. 先完成 `Phase -1` 自动模式切换与主回答隔离。
2. 再完成 `Phase 0` 类人对话治理、固定示例清理和用户可见 copy 外置。
3. 最后进入 `Phase A-F`，补齐 Phase 1 修复版中尚未完成的 MCP/Skill 真实执行闭环。

开发前应以本设计的 M0A-M0J、E0A-E0H 与 E1-E18 验证矩阵作为准入和验收标准，顺序不能倒置。M0A-M0J 未通过时，不得验收 P0 类人对话治理；E0A-E0H 未通过时，不得继续宣称或验收 MCP/Skill 执行闭环；完成 QE 创建实验端到端 workflow 前，Research Assistant 不应宣称具备完整任务执行能力或可调用所有 MCP。
