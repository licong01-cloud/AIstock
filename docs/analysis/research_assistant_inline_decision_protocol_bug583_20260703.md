# Research Assistant Inline Decision Protocol (BUG-583)

## Feature Card

本设计用于 BUG-583：退役 Research Assistant 对话外层的旧式无状态预路由硬门、逐字确认令牌和默认右侧 JSON/卡片展示，把“澄清 / 审批 / 展示”的决策面收敛到带历史的 agent loop 与统一 `decision_request`。目标体验对齐 Claude Code：agent 在同一段带历史对话内决定何时调用工具、何时追问、何时请求用户确认；用户用自然语言或内联选项继续，后端在下一轮解析决策并保留审批审计。

## Scope

- 后端 `backend/services/research_assistant/**`：
  - 预路由只保留为 `route_candidates` 种子提示，不再硬返回 `requires_clarification`。
  - `chat/turn` 返回顶层 `decision_request`，用于澄清和审批。
  - 下一轮按 `decision_id`、选项 id 或自然语言解析审批；不再要求 `confirmation_text` 与内部确认令牌逐字相等。
  - side-effect 工具在用户明确肯定前保持 `executed=False`；审批记录仍写入 `approvals` 与 task event。
  - 默认 `cards` 过滤用户不可见的旧卡片；`developer_diagnostics=true` 才返回调试卡片。
- 前端 `frontend/src/app/research-assistant/**`、`frontend/src/components/research-assistant/**`、`frontend/src/lib/research-assistant/**`：
  - 新增内联 `decision_request` 选择条，支持按钮、方向键上下选择、Enter 确认和自由文本。
  - 默认隐藏 Phase7 evidence / blocker / action proposal / diagnostic tables；开发者调试开关默认关闭。
- 测试 `backend/tests/research_assistant/**`：
  - 覆盖多轮指代、自然语言/选项审批、否定不执行、默认无卡片、显式工具失败不软化为 insufficient。

## Non-goals

- 不启动、停止或重启 `8001`、`3000`、`19080`。
- 不连接生产 DB，不执行 DDL 或迁移。
- 不删除底层审批、preflight、action proposal、trace/event 审计能力。
- 不放宽 BUG-568 遗留红线：`confirmed_action` 和 `plan_or_preflight` 写动作仍必须人在环。
- 不把 `production_sensitive` 审批降级为普通自然语言放行；此类审批必须通过内联 `approve` 选项放行。
- 不把 developer diagnostics 作为普通用户主视图。

## Contracts

### `decision_request`

`chat/turn` 顶层返回：

```json
{
  "decision_request": {
    "decision_id": "dec_<kind>_<stable-id>",
    "kind": "clarify",
    "prompt_text": "自然语言问题或确认说明",
    "options": [
      {"id": "approve", "label": "确认执行", "description": "用户明确批准后才执行 pending_action"},
      {"id": "reject", "label": "不执行", "description": "保持 executed=false 并记录拒绝或取消"}
    ],
    "allow_free_text": true,
    "approve_requires_option": false,
    "pending_action": {
      "server_key": "aistock-qe",
      "tool_name": "qe_template_create",
      "tool_args": {},
      "risk_level": "high"
    }
  }
}
```

约束：

- `kind="clarify"`：`pending_action` 为空；`options` 可为空；下一轮用户文本作为澄清事实进入带历史 agent loop。
- `kind="approve_action"`：必须包含 `pending_action`，并与当前 conversation 内的 pending approval/action proposal 可追溯关联。
- `decision_id` 稳定绑定 conversation，不依赖前端右侧卡片，也不暴露逐字确认令牌。
- `prompt_text` 是给用户看的自然语言，不包含 raw JSON、内部 token 或 DB 字段转储。
- `approve_requires_option=true` 表示 `production_sensitive` 审批只能由 `decision_option_id="approve"` 放行；自由文本肯定不执行并重发同一 `decision_request`。

### 消解决策

后端在下一轮按以下优先级解析：

1. `decision_id + decision_option_id`：`approve` / `execute` / `yes` 视为批准；`reject` / `cancel` / `no` 视为不执行。
2. `decision_id + message`：自然语言肯定如 `确认`、`执行`、`同意`、`好`、`第一个` 视为批准；否定如 `取消`、`拒绝`、`不要`、`不执行` 视为不执行；其它文本不执行并说明无法确认。
3. `production_sensitive` / L2 审批例外：批准只接受 `decision_option_id="approve"`；自由文本肯定或旧 `confirmation_text` 只返回 `approval_confirmation_l2_requires_inline_option` 并保持 pending。拒绝是安全动作，仍接受自由文本。
4. 无 `decision_id` 但当前 conversation 最近一轮只有一个 pending `approve_action`：清晰肯定/否定可消解；否则交给 agent loop 当普通消息处理。若该 pending approval 为 L2，则清晰肯定也只会触发内联选项引导，不执行。
5. 旧字段 `confirm_approval_id` / `confirmation_text` 仅作为兼容输入；chat 路径不得要求 `confirmation_text` 精确匹配内部令牌。

审批消解结果：

- 批准：更新 `approvals.status=approved`、写 `approval_text` 和 task event，再执行 pending action。
- 拒绝：更新 `approvals.status=rejected`、写 `approval_text` 和 task event；`executed=False`。
- 模糊：不更新为 approved，不执行；返回明确 `reason_code`，保留 pending 状态供用户继续。
- L2 自由文本肯定：不更新为 approved，不执行；返回 `approval_confirmation_l2_requires_inline_option`、自然语言引导，并重发同一 `decision_request`。

### 审批审计保留

- 继续使用现有 `approvals`、`action_proposals`、`task_events`、`trace_events`。
- `required_confirmation_text` 可以保留为内部兼容 / 历史字段，但默认响应与前端不展示、不要求用户逐字输入。
- 每次 approval decision 必须记录 `decision_id`、`confirmation_source`、用户自然语言或 option id、`action_proposal_id`。
- `production_sensitive` 审批放行必须记录 `decision_option_id="approve"`；自由文本拒绝仍记录为 rejection 审计。
- 工具失败必须显式暴露 `reason_code`、`server_key/tool_name`、阶段和错误消息，不得软化成 `insufficient`。

### 前端内联交互

- `decision_request` 渲染在 composer 上方，不进入右侧卡片栏。
- 有 options 时显示 Claude Code 式内联选项列表：
  - ArrowUp / ArrowDown 切换选项。
  - Enter 发送当前选项。
  - 用户仍可在 composer 中自由文本回复。
- API payload：
  - 自由文本：`{ message, conversation_id, decision_id, developer_diagnostics }`
  - 选项：`{ message: option.label, conversation_id, decision_id, decision_option_id, developer_diagnostics }`
- 当 `approve_requires_option=true` 时，前端应提示生产敏感审批必须选择内联确认选项；用户输入自由文本肯定时，后端会重发同一 `decision_request`。
- 默认 `developer_diagnostics=false`；只有开关打开时才展示旧 evidence/blocker/action proposal/diagnostic 辅助面板。

## Design Acceptance Index

- F-001: 预路由退役硬决策职责；`requires_clarification` 不再提前覆盖带历史 agent 回答，只能变成候选种子或内部提示。
- F-002: `decision_request` 成为澄清和审批的统一用户交互契约，顶层随 `chat/turn` 返回。
- F-003: `approve_action` 下一轮支持自然语言和 option id 通过，否定/模糊不执行；禁止逐字令牌匹配作为 chat 审批门。
- F-004: BUG-568 红线保留：side-effect 工具在用户明确肯定前 `executed=False`，审批审计记录仍写入。
- F-004a: `production_sensitive` / L2 审批批准必须通过内联 `approve` 选项；自由文本肯定只重发 `decision_request` 并保持 pending。
- F-005: 默认主对话和默认 `cards` 不暴露 action proposals、clarification cards、evidence/blocker cards 或 raw diagnostic tables；开发者调试开关才暴露。
- F-006: 多轮 conversation 历史继续进入 agent loop，指代性 follow-up 不被无状态预路由反问覆盖。
- F-007: no-silent-error 强化：单工具失败返回具体 `reason_code`、工具名、阶段和错误，不 404、不吞错、不伪装为 insufficient。

## Implementation Plan

- 后端模型：
  - `ChatTurnRequest` 增加 `decision_id`、`decision_option_id`、`developer_diagnostics`。
  - 响应增加顶层 `decision_request`；保存在 assistant message `content_json` 供下一轮消解。
- 后端流程：
  - `_semantic_or_legacy_route_decision` 将 semantic clarification 转换为 `clarification_seed` 和 `route_candidates`。
  - `_compose_assistant_reply` 移除 `requires_clarification` early return。
  - `_maybe_handle_chat_approval_confirmation` 改为 decision resolver：自然语言/选项批准或拒绝；旧 token 字段仅兼容。
  - `_first_pending_decision_request` 从 approval/preflight 内部状态构造 `approve_action`。
  - `_public_chat_cards(cards, developer_diagnostics=False)` 默认过滤旧用户卡片。
- 前端：
  - API type 增加 `AssistantDecisionRequest`。
  - Chat adapter 在 pending decision 存在时透传 `decision_id`。
  - 新增 inline selector；默认关闭 developer diagnostics side panels。

## Verification Plan

- `rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/research_assistant_inline_decision_protocol_bug583_20260703.md --tier F0`
- Targeted pytest:
  - `rtk python -m pytest backend/tests/research_assistant/test_service.py -k "bug_583 or chat_turn_natural_language or no_silent" -q`
  - 覆盖多轮指代、approval gate、自然语言/选项、否定不执行、默认 cards 过滤、工具失败 explicit。
- Workflow required gates as feasible:
  - `rtk python -m nox -s l0`
  - `rtk python -m nox -s research_assistant_backend`
  - `rtk python -m nox -s research_assistant_mcp_contract`
  - `rtk python -m nox -s validation_module_registry_l0`
- `rtk git diff --check`
- `rtk python scripts/aistock_issue_workflow.py finish --bug-id BUG-583 --plan-only`

## Production Gates

- `production_ddl_gate=noop`：不新增 DB schema，不执行 DDL。
- `production_backend_dependency_gate=noop`：不改 Python 依赖。
- `production_frontend_dependency_gate=noop`：不改前端依赖。
- `production_runtime_activation=not_touched`：不启停服务，不触碰生产端口。

## Anti-regression Checklist

- 写动作未确认前没有真实执行；`mcp_execution_result.executed=false`。
- LLM 文本中自行说“确认/执行”不能自审批。
- L1 用户自然语言肯定可以审批；用户否定或模糊文本不执行。
- `production_sensitive` / L2 用户自由文本肯定不审批、不执行；必须选择内联 `确认执行` 选项。
- `decision_request.pending_action` 不泄露内部确认令牌。
- 默认响应不展示旧 action proposals / clarification / evidence / blocker / diagnostic cards。
- 开发者调试开启后仍可追溯 route、preflight、approval、evidence、blocker、trace。
- 单工具失败时用户能看到 `reason_code` 与具体工具；聊天接口不变成 404/白屏。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/research_assistant/service.py::_semantic_or_legacy_route_decision; backend/services/research_assistant/service.py::_compose_assistant_reply | targeted pytest for no hard clarification and feature workflow validation | ready | n/a |
| F-002 | backend/services/research_assistant/models.py::ChatTurnRequest; backend/services/research_assistant/service.py::_first_pending_decision_request; frontend/src/lib/research-assistant/api.ts | targeted pytest for approve_action decision_request; frontend type check when available | ready | n/a |
| F-003 | backend/services/research_assistant/service.py::_maybe_handle_chat_approval_confirmation; backend/services/research_assistant/service.py::_resolve_chat_decision_action | targeted pytest for L1 natural language, option id, rejection and no token requirement | ready | n/a |
| F-004 | backend/services/research_assistant/service.py::_consume_and_execute_chat_approval; backend/services/research_assistant/service.py::_reject_chat_approval_decision | targeted pytest aligned with BUG-568 gate | ready | n/a |
| F-004a | backend/services/research_assistant/service.py::_approval_requires_inline_option_select; backend/services/research_assistant/service.py::_block_chat_approval_requires_inline_option; frontend/src/lib/research-assistant/api.ts | test_chat_turn_l2_freetext_affirmation_requires_inline_option_not_execute; test_chat_turn_l2_option_select_approval_executes; test_chat_turn_l2_freetext_rejection_still_rejects_without_execution | ready | n/a |
| F-005 | backend/services/research_assistant/service.py::_public_chat_cards; frontend/src/app/research-assistant/chat/page.tsx | targeted pytest for default card filtering; frontend developer diagnostics toggle | ready | n/a |
| F-006 | backend/services/research_assistant/service.py::_fetch_prior_chat_messages; backend/services/research_assistant/service.py::_chat_messages_for_llm | targeted pytest for same conversation referent follow-up | ready | n/a |
| F-007 | backend/services/research_assistant/service.py::_mcp_tool_failure_result; backend/services/research_assistant/service.py::_render_tool_error_reply | existing BUG-404-style test plus BUG-583 regression | ready | n/a |
