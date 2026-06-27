# Research Assistant LLM Token Usage Feedback And Chart Audit Design

Version: 2026-06-27 v1 draft
Tier: F1 standard single-module feature
Owner: research_assistant
Status: design_ready_user_defaults_confirmed_20260627

## Background

Research Assistant 已在 2026-06-26 的 `assistant_llm_usage_events` ledger 中落库每次 LLM provider call 的 token 与 cost，并提供基础只读 API：

- `GET /api/v1/research-assistant/llm-usage/events`
- `GET /api/v1/research-assistant/llm-usage/summary`

当前体验仍有两个缺口：

1. 对话页只展示回答与业务卡片；用户在一次对话完成后看不到本轮 token/cost 消耗反馈。
2. 审计页只有 Trace 区域的汇总 chip 与明细表，缺少可按日期范围、模型、小时/天粒度观察 token/cost 的图表型报表。

本设计在既有 ledger 之上增加 UI 与只读聚合 API，不改变 LLM 路由、prompt、审批、grounding、MCP 执行或 cost 计算口径。

## Documentation Discovery

已核对的现有事实源与可复用模式：

- `docs/architecture/ra_llm_token_cost_accounting_design_20260626.md`：定义 `assistant_llm_usage_events` 为 token/cost 权威事实源，`assistant_trace_events.cost_json` 仅为汇总缓存。
- `backend/routers/research_assistant.py`：已有 `list_llm_usage_events` 与 `llm_usage_summary` 只读端点，支持 `trace_id/task_id/conversation_id/model/provider/date_from/date_to` 过滤。
- `backend/services/research_assistant/repository.py`：已有 `list_llm_usage_events()` 与 `summarize_llm_usage_events()`，DB 路径可继续扩展聚合查询；in-memory repository 可用于离线测试。
- `backend/services/research_assistant/service.py`：`chat_turn()` 已在 trace 创建后写 `assistant_llm_usage_events` 并把 `trace.cost_json.usage_summary` 作为本轮汇总缓存；失败会写 `llm_usage_accounting_failed`，不是 silent fallback。
- `frontend/src/lib/research-assistant/api.ts`：已有 `AssistantLlmUsageEvent`、`AssistantLlmUsageSummary`、`llmUsageEvents()`、`llmUsageSummary()` 类型与 client。
- `frontend/src/app/research-assistant/chat/page.tsx`：chat adapter 在 `researchAssistantApi.chatTurn()` 返回后持有 `AssistantChatTurnResult`，可从 `result.trace.cost_json.usage_summary` 或按 `trace_id` 拉取 ledger summary。
- `frontend/src/app/research-assistant/audit/TraceSection.tsx`：已有审计 tab 与 token/cost 总览 chip，可抽出新的 LLM usage chart section。
- `frontend/package.json`：已存在 `react-plotly.js` 与 `plotly.js-basic-dist`，因此图表实现不得新增前端依赖；也可用轻量 SVG，但优先复用现有 Plotly 依赖。

## Scope

In scope:

- 对话页每次 `chat/turn` 完成后显示本轮 LLM 消耗反馈：调用数、输入 tokens、输出 tokens、总 tokens、cost、usage/cost 状态。
- 本轮反馈不混入助手回答正文；以 bubble 下方或对话侧栏中的专用 usage chip/card 展示，并能展开查看 ledger refs 与估算/不可用 reason。
- 新增专用审计 tab，例如 `/research-assistant/audit?tab=llm-usage`，用于 LLM token/cost 图表报表。
- 审计图表支持日期范围、粒度 `hour/day`、模型/provider 过滤；默认从 `assistant_llm_usage_events.completed_at` 聚合。
- 审计页面用图表和 KPI card 呈现，不用表格呈现 LLM usage 明细或排名。
- 后端新增只读聚合 API，返回 chart-ready buckets 和 model/provider series；保留现有 `/llm-usage/events` 与 `/summary` 兼容。
- 所有 estimated/unavailable/failed 状态必须显式显示 reason，不得把 cost/token 缺失渲染为 0 或空白。
- 前端图表无数据时显示明确 empty/degraded state，不静默空图。

## Non-Goals

- 不新增 token/cost ledger 表，不修改 `assistant_llm_usage_events` DDL。
- 不做预算限制、额度告警、扣费阻断、provider throttle 或自动换模型。
- 不保存 prompt、messages 全文或 API key。
- 不改变 LLM cost 计算方法；继续消费 2026-06-26 已落地的 LiteLLM/provider accounting 结果。
- 不把图表实现成表格；如需排查单行明细，仍从现有 Trace/detail drawer 或 API 调试入口读取。
- 不启动/重启生产 `8001/3000/19080`，不连接生产 DB。

## Contracts

### Backend API

新增只读端点：

```http
GET /api/v1/research-assistant/llm-usage/report
```

Query parameters:

- `date_from?: string` ISO datetime/date；默认由前端传入。
- `date_to?: string` ISO datetime/date；默认由前端传入。
- `granularity: hour | day`，默认 `day`。
- `model?: string`，可重复或逗号分隔；实现可先支持单值，再在验收前扩展多值。
- `provider?: string`，可重复或逗号分隔；实现可先支持单值，再在验收前扩展多值。
- `conversation_id?: string`，用于从对话入口钻取当前会话消耗。
- `timezone?: string`，默认见待确认项；返回中必须回显。
- `limit_models?: number`，默认 8，用于图表 series 上限，剩余模型合并为 `other`。

Response shape:

```json
{
  "schema_version": "aistock_research_assistant_llm_usage_report_v1",
  "source_of_truth": "assistant_llm_usage_events",
  "filters": {
    "date_from": "...",
    "date_to": "...",
    "granularity": "hour",
    "timezone": "Asia/Shanghai",
    "model": null,
    "provider": null
  },
  "summary": {
    "call_count": 0,
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0,
    "total_cost_usd": null,
    "usage_status": "recorded|estimated|unavailable|mixed",
    "cost_status": "recorded|estimated|unavailable|failed|mixed",
    "estimated_usage_event_count": 0,
    "unavailable_usage_event_count": 0,
    "unavailable_cost_event_count": 0,
    "failed_cost_event_count": 0
  },
  "time_series": [
    {
      "bucket_start": "2026-06-27T10:00:00+08:00",
      "bucket_end": "2026-06-27T11:00:00+08:00",
      "model": "deepseek-chat",
      "provider": "deepseek",
      "call_count": 3,
      "prompt_tokens": 12000,
      "completion_tokens": 4200,
      "total_tokens": 16200,
      "total_cost_usd": "0.012345",
      "usage_status": "recorded",
      "cost_status": "recorded"
    }
  ],
  "model_breakdown": [
    {
      "model": "deepseek-chat",
      "provider": "deepseek",
      "call_count": 3,
      "total_tokens": 16200,
      "total_cost_usd": "0.012345",
      "usage_status": "recorded",
      "cost_status": "recorded"
    }
  ],
  "status_breakdown": {
    "usage": { "recorded": 3, "estimated": 0, "unavailable": 0, "failed": 0 },
    "cost": { "recorded": 3, "estimated": 0, "unavailable": 0, "failed": 0 }
  },
  "prompt_text_retained": false,
  "degraded": false,
  "reason_code": null
}
```

Failure/degraded contract:

- schema missing: return mapped config/schema error；UI must show `assistant_llm_usage_events_missing` or existing schema error message.
- invalid `granularity`: HTTP 422 or explicit validation error.
- unsupported timezone: explicit `invalid_timezone`，不得 silent UTC fallback unless user confirms UTC fallback policy.
- aggregation error: no partial fake chart；return loud API error and UI error panel.

### Repository aggregation

DB implementation should use SQL aggregation from `assistant_llm_usage_events`:

- `date_trunc('hour'|'day', completed_at AT TIME ZONE <timezone>)` for buckets.
- group by `bucket_start, provider, model` for chart series.
- aggregate numeric tokens with `SUM(COALESCE(..., 0))` but status counts separately distinguish real zero from unavailable.
- `total_cost_usd` aggregates only non-null costs; if any cost unavailable/failed, summary `cost_status` becomes `mixed` or the most severe explicit status.

In-memory repository must implement equivalent bucket aggregation for offline tests.

### Frontend API types

Extend `frontend/src/lib/research-assistant/api.ts`:

- `AssistantLlmUsageReport`
- `AssistantLlmUsageTimeBucket`
- `AssistantLlmUsageModelBreakdown`
- `llmUsageReport(params)` client method

Existing `llmUsageEvents()` and `llmUsageSummary()` remain unchanged.

## UI Design

### Chat Turn Usage Feedback

Placement:

- Each completed assistant turn shows a compact `TurnUsagePanel` in the right-side conversation rail tied to the latest assistant `message_id`. The panel is the canonical chat location for per-turn usage; do not duplicate it under the bubble by default.
- The chip is not part of the model answer text；copy/paste of the answer should not include usage metadata by default.
- The chip displays:
  - `本轮: <call_count> 次 LLM 调用`
  - `输入 <prompt_tokens> / 输出 <completion_tokens> / 总 <total_tokens>`
  - `成本 <total_cost_usd>` or `成本不可用: <cost_reason_code>`
  - `usage=<status> / cost=<status>`
- Expandable details show `trace_id`、`usage_event_refs`、`provider/model`、estimated/unavailable reason；仍不显示 prompt 全文。

Data source order:

1. Prefer `result.trace.cost_json.usage_summary` and `usage_event_refs` returned by `chat/turn` for immediate post-turn display.
2. If the trace summary is pending or missing while `trace_id` exists, call `llmUsageSummary({ trace_id })` once after turn completion.
3. If both fail, show explicit degraded chip: `本轮消耗统计暂不可用` + reason；do not show zeros.

State model:

- `ResearchAssistantChatPage` keeps `turnUsageHistory` in React state and renders the latest/current turn usage in the right rail.
- Append one entry per successful `chatTurn` result using `assistant_message.message_id` or fallback `trace_id` as stable key.
- New conversation clears only visible local history；persisted ledger remains queryable from audit report.

### Dedicated Audit Chart Page

Add an audit tab:

- tab key: `llm-usage`
- label: `LLM 消耗`
- URL: `/research-assistant/audit?tab=llm-usage`

The tab must be chart-first and no table:

1. Filter bar:
   - date range picker/input (`date_from`, `date_to`)
   - preset range chips: `最近 7 天` default and `最近 30 天` optional
   - granularity toggle: `按小时` / `按天`
   - model selector
   - provider selector
   - refresh button
2. KPI cards:
   - total tokens
   - prompt/completion split
   - total cost USD
   - call count
   - estimated/unavailable/failed counts
3. Charts:
   - stacked bar or stacked area: tokens over time by model
   - line chart: cost over time
   - horizontal bar chart: top models by total tokens
   - donut or stacked bar: usage/cost status distribution
4. Empty/degraded state:
   - no rows: show `所选范围暂无 LLM ledger 记录`
   - schema/API error: show explicit error with reason and source endpoint
   - unavailable cost: chart tokens still render；cost chart shows unavailable annotation rather than zero line

Chart implementation:

- Reuse existing frontend dependencies `react-plotly.js` and `plotly.js-basic-dist` with `dynamic(..., { ssr: false })`.
- No new frontend dependency.
- Encapsulate RA-specific charts in `frontend/src/app/research-assistant/audit/LlmUsageSection.tsx` or `frontend/src/components/research-assistant/LlmUsageCharts.tsx`.
- Use RA CSS tokens from `research-assistant.css`；do not import Paper v2 CSS。

## Implementation Plan

1. Backend report API
   - Add service method `llm_usage_report(...)` in `backend/services/research_assistant/service.py`.
   - Add repository method `report_llm_usage_events(...)` in `backend/services/research_assistant/repository.py` for DB and in-memory repositories.
   - Add router endpoint `GET /llm-usage/report` in `backend/routers/research_assistant.py`.
   - Preserve no prompt text and explicit status/reason semantics.

2. Chat page usage feedback
   - Extend frontend types to read `trace.cost_json.usage_summary` safely.
   - Add `TurnUsageChip` / `TurnUsagePanel` in `frontend/src/app/research-assistant/chat/page.tsx` or a small component under `frontend/src/components/research-assistant/`.
   - Show one feedback entry after each completed turn；never insert token text into the answer body。
   - Add degraded UI when usage summary missing or API lookup fails。

3. Audit chart tab
   - Add `llm-usage` to `AUDIT_TABS` and `renderAuditSection()` in `frontend/src/app/research-assistant/audit/page.tsx`。
   - Add `LlmUsageSection.tsx` that calls `researchAssistantApi.llmUsageReport()`。
   - Implement chart-only visualizations and KPI cards；do not use `PaperTable` for this tab。
   - Keep existing Trace tab available for trace events。

4. Styling and copy
   - Add RA-native chart/card/filter CSS classes to `research-assistant.css`。
   - Use Chinese operator-facing labels。
   - Make estimated/unavailable status visually distinct。

5. Tests
   - Backend unit/API tests for report aggregation by hour/day, model/provider filters, status counts, no prompt text。
   - Frontend type/lint/build tests。
   - Playwright or component-level test asserting the LLM usage audit tab renders charts, not tables。
   - Chat page test asserting usage chip appears after a mocked `chatTurn` result and does not alter answer text。

## Verification Plan

Required validation before implementation PR:

- `rtk proxy python scripts/aistock_feature_workflow.py validate --design docs/architecture/ra_llm_usage_feedback_and_chart_audit_design_20260627.md --tier F1`
- targeted backend pytest for new report aggregation and API。
- targeted frontend/component or Playwright coverage for chat usage chip and chart-only audit tab。
- `rtk proxy python -m nox -s l0`
- `rtk proxy python -m nox -s research_assistant_backend`
- `rtk proxy python -m nox -s research_assistant_mcp_contract`
- `rtk proxy python -m nox -s ra_phase7_full_accept`
- From `frontend/`: `rtk proxy npm exec tsc --noEmit --incremental false`, `rtk proxy npm run lint`, `rtk proxy npm run build`
- `rtk proxy git diff --check`
- `rtk proxy ruff check <changed python files>`

## Design Acceptance Index

- F-001: Backend exposes chart-ready read-only LLM usage report API with date range, `hour/day` granularity, model/provider filters, summary, time series, model breakdown, and status breakdown.
- F-002: Report aggregation consumes only `assistant_llm_usage_events`; `assistant_trace_events.cost_json` remains cache only and is not report authority.
- F-003: Chat UI displays post-turn token/cost feedback for each completed assistant turn without inserting usage metadata into answer text.
- F-004: Chat feedback degrades loudly when usage is unavailable, showing specific reason instead of zero/blank values.
- F-005: Audit page provides a dedicated `LLM 消耗` tab with date range and granularity controls.
- F-006: Audit `LLM 消耗` tab uses charts and KPI cards only；no usage table or ranking table is used in this tab。
- F-007: Charts include token over time by model, cost over time, top models by token consumption, and usage/cost status distribution.
- F-008: UI clearly labels estimated/unavailable/failed usage or cost；cost unavailable does not render as `$0`。
- F-009: Implementation adds no DDL, no new dependency, no prompt/message persistence, and does not affect routing/prompt/approval/grounding/MCP execution.

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/routers/research_assistant.py`; `backend/services/research_assistant/service.py`; `backend/services/research_assistant/repository.py` | API tests for hour/day buckets and filters | ready | none |
| F-002 | repository report query against `assistant_llm_usage_events` | test with polluted trace `cost_json` still reports ledger values | ready | none |
| F-003 | `frontend/src/app/research-assistant/chat/page.tsx` or `TurnUsageChip` component | mocked chat turn renders usage chip after answer | ready | none |
| F-004 | chat usage degraded state | mocked missing/failed summary shows reason and no zero fallback | ready | none |
| F-005 | `frontend/src/app/research-assistant/audit/page.tsx`; `LlmUsageSection.tsx` | route/tab smoke for `/audit?tab=llm-usage` | ready | none |
| F-006 | `LlmUsageSection.tsx`; chart components | frontend test asserts no `table`/`PaperTable` in LLM usage tab | ready | none |
| F-007 | chart components using existing Plotly dependency | screenshot or DOM test for token/cost/model/status charts | ready | none |
| F-008 | shared formatter/status badge helpers | tests for unavailable/estimated labels | ready | none |
| F-009 | design + diff review | production gates report and grep for new DDL/dependency/prompt persistence | ready | none |

## Risks And Guards

- Chart query load may grow with ledger size. Guard: require date range；default range bounded；DB aggregation uses indexed `completed_at` and model/provider filters。
- Cost unavailable may be misunderstood as zero. Guard: formatter returns `成本不可用` with reason, and cost charts annotate unavailable segments。
- Model names can be high cardinality. Guard: `limit_models` groups long tail into `other` and exposes exact filter controls。
- Frontend chart library can increase bundle size. Guard: dynamic import Plotly with SSR disabled and only in audit LLM tab。
- Chat UI may mismatch assistant-ui internal message IDs. Guard: key usage feedback by backend `assistant_message.message_id` when available；fallback to `trace_id`；keep metadata outside answer text。
- Timezone grouping can be confusing. Guard: API response echoes timezone and bucket boundaries；UI displays timezone in filter summary。

## Production Gates

- `production_ddl_gate=noop` because this feature reuses already-applied `assistant_llm_usage_events` and adds no schema change。
- `production_frontend_dependency_gate=noop` because existing Plotly dependencies are reused and no package changes are planned。
- `production_backend_dependency_gate=noop` because no backend dependency changes are planned。
- Runtime activation: after code merge, running backend/frontend need user-owned restart/redeploy to expose new API/UI. Codex must not start or restart `8001/3000/19080`。

## User Confirmed Defaults

用户已确认以下默认方案，后续实现必须按此落地：

1. 默认报表范围：最近 7 天；UI 必须提供 `最近 30 天` 作为快捷可选项。
2. 默认时间桶：日期范围小于等于 48 小时时默认 `hour`，否则默认 `day`。
3. 默认 timezone：`Asia/Shanghai`，并在 UI 明示。
4. 对话页展示位置：右侧“本轮消耗”面板；不默认贴在每条 bubble 下方。
5. 审计图表分组：默认按 `model` 分组，`provider` 作为过滤器和 tooltip。
