# Research Assistant MCP/Skill 执行闭环补充设计

> 日期: 2026-05-25
> 状态: Implementation design, pending code development
> 适用范围: Research Assistant Phase 1 修复版中尚未完成的 MCP/Skill 真实执行闭环
> 设计来源:
> - `docs/architecture/aistock_research_agent_console_design_20260520.md`
> - `docs/architecture/research_assistant_prompt_context_runtime_governance_design_20260524.md`
> - `tests/aistock_validation/history/research_assistant/20260525_l3_prompt_context_runtime_governance_validation.md`

## 1. 目的与边界

本设计不是新的研发方向，而是补齐既有 Research Assistant 设计中的执行闭环部分。

原设计已经定义 Phase 1 必须实现的主链路:

```text
Conversation -> Planner -> Action Proposal -> Confirmation -> Preflight -> MCP/Skill -> Trace -> Human Report
```

当前 PR `#198` 已完成 Prompt Pack、Runtime Config、上下文预算、自动压缩、Reactive compact、key facts、MCP/Skill 基础目录、preflight、approval、trace 和 dry-run 基础能力；但尚未实现真实 MCP/Skill 执行和端到端任务闭环。

本设计补齐以下内容:

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
| 真实 MCP/Skill execute | 未具备 | 需要本设计实现 |
| 全量能力同步 | 未具备 | 需要本设计实现 |
| QE 创建实验端到端 | 未具备 | 需要本设计实现 |

## 4. 设计原则

1. **Capability first**: 助手只能从已同步、已批准、可审计的 Capability Registry 中选择能力。
2. **MCP/API first**: 所有业务动作通过 MCP/API/Skill gateway，不通过 UI 点击或隐式脚本。
3. **Proposal before execution**: 任何有副作用动作必须先生成 Action Proposal。
4. **Confirmation before preflight/execute**: 未确认不得进入有副作用 preflight 或 execute。
5. **Preflight before execute**: preflight 失败不得执行。
6. **Approval for high risk**: 高风险和 production-sensitive 必须绑定 approval、plan digest、config/version 和确认文本。
7. **Trace everything**: plan、preflight、approval、execute、result、failure 都必须可回放。
8. **Fail fast, no silent fallback**: 失败必须结构化返回，不得把失败伪装成成功。
9. **Runtime configurable**: retry、timeout、page size、catalog sync limit、risk policy 等可调参数进入 runtime config 或 DB activation。
10. **Original facts preserved**: 用户确认、参数、审批、执行结果和错误是结构化事实源，不依赖自然语言摘要。

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
  "capability_key": "qe.create_experiment",
  "proposal_type": "workflow_pack",
  "title": "创建 QE 10 loop 实验草案",
  "risk_level": "high",
  "side_effect_level": "high_cost_compute",
  "input_json": {
    "loop_count": 10,
    "stock_pool": "pending_confirmation",
    "backtest_window": "pending_confirmation",
    "materialize": false,
    "run": false
  },
  "required_confirmations": [
    "CONFIRM_QE_DRAFT",
    "CONFIRM_QE_MATERIALIZE",
    "CONFIRM_QE_RUN"
  ],
  "next_step": "ask_confirmation"
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

`qe.create_experiment` 是 Phase 1 修复版强制验收场景。实现顺序必须与原设计一致。

### 10.1 Workflow steps

| step | 能力 | 类型 | 风险 | 门禁 |
| --- | --- | --- | --- | --- |
| 1 | 理解实验目标和边界 | planner | medium | 无副作用 |
| 2 | 读取 QE MCP/Skill 目录 | capability lookup | read_only | trace |
| 3 | 加载相关 Memory/规则 | context pack | read_only | trace |
| 4 | 询问 loop、窗口、股票池、模型资源 | chat confirmation | medium | 用户确认 |
| 5 | 生成实验草案 | workflow_pack | draft_only | C1 |
| 6 | 创建/校验 template | mcp_tool | write_nonprod | C2 approval |
| 7 | 展示 validate diff/summary | renderer | read_only | trace |
| 8 | materialize | mcp_tool | high_cost_compute/write_nonprod | C3 approval |
| 9 | run experiment | mcp_tool | high_cost_compute | C3 approval + cost guard |
| 10 | 查询状态并汇报 | mcp_tool/skill | read_only | trace |

### 10.2 强制 guard

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

Chat 返回必须以人类可读卡片呈现:

- Intent summary card
- Missing slots card
- Action proposal card
- Risk/approval card
- Preflight result card
- Execute progress card
- Result / failure card

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

下一阶段方向与既有研发设计一致，但必须表述为“Phase 1 修复版中尚未完成的 MCP/Skill 真实执行闭环”，而不是新路线。

开发前应以本设计的 E1-E18 验证矩阵作为准入和验收标准。完成 QE 创建实验端到端 workflow 前，Research Assistant 不应宣称具备完整任务执行能力或可调用所有 MCP。
