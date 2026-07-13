import { expect, test } from "@playwright/test";

const chatTurnResponse = {
  conversation: { title: "QE 实验草案" },
  assistant_message: {
    content_text: "可以。QE 实验方面我能生成草案、校验模板、做 preflight 并在确认后调用已登记 MCP；Bug 诊断方面我能分析报错、日志、Trace、实验记录和配置差异。",
    content_json: {},
  },
  mode_decision: {
    mode: "dialogue",
    intent_type: "capability_inquiry",
    confidence: 0.9,
    requires_tool: false,
    allowed_tool_side_effect: "none",
    requires_user_confirmation: false,
    requires_approval: false,
    visible_audit_default: false,
  },
  task: { title: "QE 能力询问", status: "planned" },
  task_events: [
    { event_type: "chat_received", message: "已接收用户对话并进入意图理解。" },
    { event_type: "llm_done", message: "主模型已返回。" },
  ],
  cards: {
    dialogue_mode: "dialogue",
    mode_decision: { mode: "dialogue", intent_type: "capability_inquiry" },
    action_proposals: [],
    ui_display: {
      show_plan_card: false,
      show_clarification_card: false,
      show_context_health_badge: false,
      details_default_collapsed: true,
    },
    status_rail: [
      { label: "接收问题", status: "done" },
      { label: "理解意图", status: "done" },
      { label: "回答", status: "done" },
      { label: "等待任务指令", status: "idle" },
      { label: "MCP 预检查", status: "locked" },
      { label: "执行", status: "locked" },
      { label: "写入记忆", status: "locked" },
    ],
    capability_summary: {
      mcp: "可按明确任务调用 Research Assistant、QE、Validation、GitHub 同步等已登记 MCP 能力。",
      skill: "可按任务加载 QE 诊断、因子分析、Issue 处理等本地技能。",
      model: "DeepSeek primary",
    },
    safety: {
      no_materialize_before_confirmation: true,
      no_run_before_confirmation: true,
      no_raw_json_in_main_chat: true,
    },
    runtime_code: {
      schema_version: "aistock_research_assistant_runtime_code_visibility_v1",
      status: "current",
      runtime_loaded_at: "2026-05-29T06:00:00Z",
      runtime_loaded_git_commit_short: "1d917189",
      current_repo_git_commit_short: "1d917189",
      origin_main_git_commit_short: "1d917189",
      loaded_source_matches_disk: true,
      loaded_commit_matches_repo: true,
      repo_matches_origin_main: true,
      restart_required_to_activate_main: false,
      operator_message: "Running Research Assistant code matches local/origin main.",
    },
  },
};


const llmUsageReportResponse = {
  schema_version: "aistock_research_assistant_llm_usage_report_v1",
  source_of_truth: "assistant_llm_usage_events",
  filters: { date_from: "2026-06-21T00:00:00+08:00", date_to: "2026-06-27T23:59:59+08:00", granularity: "day", timezone: "Asia/Shanghai" },
  summary: {
    call_count: 3,
    prompt_tokens: 1200,
    completion_tokens: 420,
    total_tokens: 1620,
    total_cost_usd: "0.0123000000",
    usage_status: "recorded",
    cost_status: "mixed",
    estimated_usage_event_count: 0,
    unavailable_usage_event_count: 0,
    unavailable_cost_event_count: 1,
    failed_cost_event_count: 0,
  },
  time_series: [
    { bucket_start: "2026-06-27T09:00:00+08:00", bucket_end: "2026-06-27T10:00:00+08:00", model: "deepseek-chat", provider: "deepseek", call_count: 2, prompt_tokens: 900, completion_tokens: 300, total_tokens: 1200, total_cost_usd: "0.0100000000", usage_status: "recorded", cost_status: "recorded", usage_status_counts: { recorded: 2 }, cost_status_counts: { recorded: 2 } },
    { bucket_start: "2026-06-27T10:00:00+08:00", bucket_end: "2026-06-27T11:00:00+08:00", model: "deepseek-reasoner", provider: "deepseek", call_count: 1, prompt_tokens: 300, completion_tokens: 120, total_tokens: 420, total_cost_usd: null, usage_status: "recorded", cost_status: "unavailable", usage_status_counts: { recorded: 1 }, cost_status_counts: { unavailable: 1 } },
  ],
  model_breakdown: [
    { model: "deepseek-chat", provider: "deepseek", call_count: 2, prompt_tokens: 900, completion_tokens: 300, total_tokens: 1200, total_cost_usd: "0.0100000000", usage_status: "recorded", cost_status: "recorded" },
    { model: "deepseek-reasoner", provider: "deepseek", call_count: 1, prompt_tokens: 300, completion_tokens: 120, total_tokens: 420, total_cost_usd: null, usage_status: "recorded", cost_status: "unavailable" },
  ],
  status_breakdown: { usage: { recorded: 3, estimated: 0, unavailable: 0, failed: 0 }, cost: { recorded: 2, estimated: 0, unavailable: 1, failed: 0 } },
  prompt_text_retained: false,
  degraded: false,
  reason_code: null,
};

function page<T>(items: T[]) {
  return { items, total: items.length, page: 1, page_size: 100, has_more: false };
}

const catalogNotReadyDetail = {
  code: "research_assistant_catalog_not_ready",
  message: "研究助理目录尚未初始化完整，请先初始化 Prompt Tree、MCP、Skill 与模型路由目录。",
  operator_action: "POST /api/v1/research-assistant/catalogs/seed",
  readiness: {
    ready: false,
    status: "catalog_not_ready",
    missing_catalogs: ["prompt_nodes", "mcp_tools"],
    checks: [
      { catalog: "prompt_nodes", label: "Prompt Tree", expected_min: 8, present: 0, ready: false },
      { catalog: "mcp_tools", label: "MCP Tool Catalog", expected_min: 6, present: 0, ready: false },
    ],
  },
};

test("Research Assistant chat shows readable catalog setup state instead of backend error JSON", async ({ page: browserPage }) => {
  const writeMethods: string[] = [];
  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const method = route.request().method();
    if (method !== "GET") writeMethods.push(`${method} ${path}`);

    if (path.endsWith("/chat/turn")) {
      return route.fulfill({
        status: 409,
        contentType: "application/json",
        body: JSON.stringify({ detail: catalogNotReadyDetail }),
      });
    }
    if (path.endsWith("/catalogs/seed")) {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ status: "success", data: { seeded: { prompt_nodes: 8, mcp_tools: 6 } } }),
      });
    }
    if (path.endsWith("/catalogs/readiness")) {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ status: "success", data: { ...catalogNotReadyDetail.readiness, ready: true, status: "ready", missing_catalogs: [], checks: [] } }),
      });
    }
    return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data: page([]) }) });
  });

  await browserPage.goto("/research-assistant");
  await browserPage.getByPlaceholder(/直接提问或描述任务/).fill("你能生成 QE 实验和诊断 bug 吗？");
  await browserPage.getByRole("button", { name: "发送" }).click();

  await expect(browserPage.getByText("助理目录尚未初始化完整").first()).toBeVisible();
  await expect(browserPage.getByTestId("ra-chat-catalog-setup")).toContainText("Prompt Tree：当前 0 / 至少 8");
  await expect(browserPage.getByTestId("ra-chat-catalog-setup")).toContainText("MCP Tool Catalog：当前 0 / 至少 6");
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("research_assistant_catalog_not_ready");
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("{");

  await browserPage.getByRole("button", { name: "初始化助理目录" }).click();
  await expect(browserPage.getByText("目录初始化完成。请重新发送你的研究或实验目标。")).toBeVisible();
  expect(writeMethods).toEqual(["POST /api/v1/research-assistant/chat/turn", "POST /api/v1/research-assistant/catalogs/seed"]);
});

test("Research Assistant main entry is a Codex-like LLM chat with readable cards", async ({ page: browserPage }) => {
  const writeMethods: string[] = [];
  const consoleErrors: string[] = [];
  const pageErrors: string[] = [];

  browserPage.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  browserPage.on("pageerror", (error) => pageErrors.push(error.message));

  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const method = route.request().method();
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (method !== "GET") writeMethods.push(`${method} ${path}`);

    if (path.includes("/api/ingestion/alerts/")) return respond(path.endsWith("/unack-count") ? { count: 0 } : []);
    if (path.endsWith("/chat/turn")) return respond(chatTurnResponse);
    return respond(page([]));
  });
  await browserPage.route("**/api/ingestion/alerts/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const body = path.endsWith("/unack-count") ? { count: 0 } : { alerts: [] };
    return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
  });

  await browserPage.goto("/research-assistant");
  await expect(browserPage.getByRole("heading", { name: "像研究搭档一样对话，由 MCP 安全执行" })).toBeVisible();
  await expect(browserPage.getByText("助理正在做什么")).toBeVisible();
  await expect(browserPage.getByText("计划、确认、Trace 和 payload 留在可折叠详情或审计页面")).toBeVisible();

  await browserPage.getByPlaceholder(/直接提问或描述任务/).fill("你能生成 QE 实验和诊断 bug 吗？");
  await browserPage.getByRole("button", { name: "发送" }).click();

  await expect(browserPage.getByText("QE 实验方面我能生成草案").first()).toBeVisible();
  await expect(browserPage.getByTestId("ra-runtime-code-card")).toBeVisible();
  await expect(browserPage.getByTestId("ra-runtime-code-card")).toContainText("运行时代码可见性");
  await expect(browserPage.getByTestId("ra-runtime-code-card")).toContainText("运行中 commit：1d917189");
  await expect(browserPage.getByTestId("ra-runtime-code-card")).toContainText("本地 main：1d917189");
  await expect(browserPage.getByTestId("ra-chat-plan-card")).toHaveCount(0);
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("固定 PIT 股票池");
  await expect(browserPage.getByText("回答").first()).toBeVisible();
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("不会执行 QE materialize/run");
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("生成 QE 实验草案");

  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("payload_json");
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("trace_id");
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("task_id");
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("{");

  expect(writeMethods).toEqual(["POST /api/v1/research-assistant/chat/turn"]);
  expect(pageErrors).toEqual([]);
  expect(consoleErrors.filter((message) => !message.includes("Failed to load resource"))).toEqual([]);
});

test("Research Assistant chat renders tool choice markup as readable MCP route cards", async ({ page: browserPage }) => {
  const response = {
    ...chatTurnResponse,
    assistant_message: {
      content_text: "<assistant_tool_choice>{\"tool\":\"mcp_github_issue_sync_bug\"}</assistant_tool_choice>",
      content_json: {},
    },
    cards: {
      ...chatTurnResponse.cards,
      ui_display: { show_plan_card: false, show_clarification_card: false, show_context_health_badge: false, details_default_collapsed: true },
      mcp_route_decision: {
        domain: "validation_issue",
        server_key: "aistock-validation",
        tool_name: "mcp_github_issue_sync_bug",
        reason: "Matched Validation Center issue sync.",
        side_effect: "confirmed_action",
        summary_first: true,
        preflight_required: true,
        confirmation_required: true,
      },
    },
  };

  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/chat/turn")) return respond(response);
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant");
  await browserPage.getByPlaceholder(/直接提问或描述任务/).fill("同步 BUG-120 GitHub issue 状态");
  await browserPage.getByRole("button", { name: "发送" }).click();

  await expect(browserPage.getByTestId("ra-chat-main")).not.toContainText("<assistant_tool_choice>");
  await expect(browserPage.getByTestId("ra-chat-main")).toContainText("validation issue");
  await expect(browserPage.getByTestId("ra-chat-main")).not.toContainText("route decision");
  await expect(browserPage.getByTestId("ra-chat-main")).not.toContainText("aistock-validation/mcp_github_issue_sync_bug");
  await expect(browserPage.getByTestId("ra-mcp-route-card")).toContainText("需要确认和审批后才可执行");
  await expect(browserPage.getByTestId("ra-mcp-route-card")).not.toContainText("MCP route decision");
  await expect(browserPage.getByTestId("ra-mcp-route-card")).not.toContainText("aistock-validation/mcp_github_issue_sync_bug");
});

test("Research Assistant chat renders auto-executed MCP summary result cards", async ({ page: browserPage }) => {
  const response = {
    ...chatTurnResponse,
    assistant_message: {
      content_text: "已通过只读工具完成 MCP summary-first 查询；我只展示概要，不展开原始行、矩阵、日志或模型权重。",
      content_json: {},
    },
    cards: {
      ...chatTurnResponse.cards,
      ui_display: { show_plan_card: false, show_clarification_card: false, show_context_health_badge: false, details_default_collapsed: true },
      mcp_route_decision: {
        domain: "factor_library",
        server_key: "aistock-factor-library",
        tool_name: "factor_library_list",
        reason: "Matched factor library list request.",
        side_effect: "read_only",
        summary_first: true,
        preflight_required: false,
        confirmation_required: false,
      },
      mcp_execution_result: {
        auto_executed: true,
        status: "succeeded",
        executed: true,
        route: "aistock-factor-library/factor_library_list",
        server_key: "aistock-factor-library",
        tool_name: "factor_library_list",
        summary_first: true,
        response_summary: { returned_count: 2, total_count: 47 },
      },
      mcp_summary_result: {
        summary_first: true,
        response_mode: "summary",
        returned_count: 2,
        total_count: 47,
        items: [
          { factor_name: "alpha_momentum_20d", category: "momentum", status: "ready" },
          { factor_name: "alpha_value_quality", category: "quality", status: "ready" },
        ],
        omitted_sections: ["raw_payload", "matrix", "factor_value_rows"],
        artifact_refs: ["mcp://summary/factor_library/list"],
        detail_tool: "factor_library_get_detail",
        next_step: "Use the referenced detail tool when one factor needs full metadata.",
      },
      mcp_result_cards: [
        {
          title: "aistock-factor-library/factor_library_list",
          summary: "Prepared a summary-first MCP result envelope for factor_library; heavy sections are omitted or referenced.",
          route: "aistock-factor-library/factor_library_list",
          summary_first: true,
          next_step: "Use the referenced detail tool when one factor needs full metadata.",
        },
      ],
      mcp_tool_event: {
        status: "succeeded",
        server_key: "aistock-factor-library",
        tool_name: "factor_library_list",
        transport: "research_assistant_catalog_summary_adapter",
        artifact_refs: ["mcp://summary/factor_library/list"],
      },
    },
  };

  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/chat/turn")) return respond(response);
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant");
  await browserPage.getByPlaceholder(/直接提问或描述任务/).fill("查看因子库概要");
  await browserPage.getByRole("button", { name: "发送" }).click();

  const main = browserPage.getByTestId("ra-chat-main");
  await expect(browserPage.getByTestId("ra-mcp-summary-card")).toBeVisible();
  await expect(browserPage.getByTestId("ra-mcp-summary-card")).toContainText("已完成只读业务查询");
  await expect(browserPage.getByTestId("ra-mcp-summary-card")).not.toContainText("aistock-factor-library/factor_library_list");
  await expect(browserPage.getByTestId("ra-mcp-summary-card")).not.toContainText("summary-first");
  await expect(browserPage.getByTestId("ra-mcp-summary-card")).toContainText("返回 2 / 总计 47");
  await expect(browserPage.getByTestId("ra-mcp-summary-card")).toContainText("alpha_momentum_20d");
  await expect(browserPage.getByTestId("ra-mcp-summary-card")).not.toContainText("原始 payload / 矩阵 / 因子明细行");
  await expect(browserPage.getByTestId("ra-mcp-summary-card")).not.toContainText("factor_library_get_detail");
  await expect(main).not.toContainText("raw_payload");
  await expect(main).not.toContainText("factor_value_rows");
  await expect(main).not.toContainText("research_assistant_catalog_summary_adapter");
  await expect(main).not.toContainText('"items"');
  await expect(main).not.toContainText("{");
});



test("Research Assistant blocker diagnostics are collapsed behind developer details", async ({ page: browserPage }) => {
  const response = {
    ...chatTurnResponse,
    assistant_message: {
      content_text: "Local data repair requires explicit confirmation; diagnostic detail is rendered as a direct log block.",
      content_json: {},
    },
    cards: {
      ...chatTurnResponse.cards,
      ui_display: { show_plan_card: false, show_clarification_card: false, show_context_health_badge: false, details_default_collapsed: true },
      action_proposals: [
        { title: "read-only health overview", approval_required: false, status: "read_only" },
        { title: "repair plan", approval_required: false, status: "plan_only" },
        {
          action_proposal_id: "proposal-blocker-3",
          title: "local_data_apply_repair_confirmed",
          approval_required: true,
          status: "approval_required",
          reason: "local_data_apply_repair_confirmed",
          next_step: "请在对话内审批卡片查看预检结果，并输入精确确认令牌后再执行。",
          provenance: { source: "action_proposals" },
        },
      ],
    },
  };

  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/chat/turn")) return respond(response);
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant");
  await browserPage.locator(".ra-chat-input").fill("check local data sync status");
  await browserPage.locator(".ra-chat-send").click();

  const blockerLog = browserPage.getByTestId("ra-blocker-log");
  await expect(blockerLog).toBeVisible();
  await expect(blockerLog).toContainText("approval_required");
  await expect(blockerLog).toContainText("local_data_apply_repair_confirmed");
  await expect(blockerLog).toContainText("请在对话内审批卡片查看预检结果，并输入精确确认令牌后再执行。");
  await expect(blockerLog).toContainText("action_proposals");
  await expect(blockerLog.locator("pre")).toBeHidden();
  await expect(blockerLog.locator(".ra-json-preview")).toHaveCount(0);
  await blockerLog.locator("summary").first().click();
  await expect(blockerLog.locator(".ra-json-summary")).not.toHaveCount(0);
  await expect(blockerLog).toContainText("状态");
  await expect(blockerLog).toContainText("原因");
  await expect(blockerLog).toContainText("下一步");
  await expect(blockerLog).toContainText("来源");
  await expect(blockerLog.locator("pre")).toBeHidden();
  await blockerLog.getByText("查看原始数据/开发者").click();
  await expect(blockerLog.locator("pre")).toBeVisible();
  await expect(blockerLog.locator("pre")).toContainText("proposal-blocker-3");
  await expect(browserPage.getByTestId("ra-blocker-card").locator("details.ra-detail-drawer")).toHaveCount(1);
});

test("Research Assistant evidence diagnostics are collapsed behind developer details", async ({ page: browserPage }) => {
  const response = {
    ...chatTurnResponse,
    assistant_message: {
      content_text: "Evidence card keeps provenance readable and raw diagnostic payload collapsed.",
      content_json: {},
    },
    cards: {
      ...chatTurnResponse.cards,
      ui_display: { show_plan_card: false, show_clarification_card: false, show_context_health_badge: false, details_default_collapsed: true },
      evidence_cards: [
        {
          card_id: "evidence-card-1",
          title: "stock evidence",
          summary: "Quote evidence captured from stock MCP.",
          status: "supported",
          evidence_refs: [
            {
              source: "stock_analysis.latest_quote",
              source_ref: "stock_analysis:000688",
              as_of: "",
              provenance: {
                source: "stock_analysis",
                server_key: "aistock-stock-analysis",
                trace_id: "trace-secret-raw-json",
              },
            },
          ],
        },
      ],
    },
  };

  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/chat/turn")) return respond(response);
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant");
  await browserPage.locator(".ra-chat-input").fill("show stock evidence");
  await browserPage.locator(".ra-chat-send").click();

  const evidenceCard = browserPage.getByTestId("ra-evidence-card");
  await expect(evidenceCard).toBeVisible();
  const evidenceGrid = evidenceCard.locator(".ra-evidence-ref-grid");
  await expect(evidenceGrid).toContainText("stock_analysis.latest_quote");
  await expect(evidenceGrid).toContainText("stock_analysis");
  await expect(evidenceGrid).toContainText("-");
  await expect(evidenceGrid).not.toContainText("trace-secret-raw-json");
  await expect(evidenceGrid).not.toContainText("{");

  const evidenceLog = browserPage.getByTestId("ra-evidence-log");
  await expect(evidenceLog).toBeVisible();
  await expect(evidenceLog.locator("summary").first()).toContainText("Developer details / Diagnostic log");
  await expect(evidenceLog.locator("pre")).toBeHidden();
  await evidenceLog.locator("summary").first().click();
  await expect(evidenceLog).toContainText("来源");
  await expect(evidenceLog).toContainText("服务");
  await expect(evidenceLog).toContainText("Trace ID");
  await expect(evidenceLog.locator("pre")).toBeHidden();
  await evidenceLog.getByText("查看原始数据/开发者").click();
  await expect(evidenceLog.locator("pre")).toBeVisible();
  await expect(evidenceLog.locator("pre")).toContainText("trace-secret-raw-json");
  await expect(evidenceCard.locator("details.ra-detail-drawer")).toHaveCount(1);
});

test("Research Assistant MCP tools page treats ready servers as ready", async ({ page: browserPage }) => {
  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/mcp/servers")) {
      return respond(page([
        { server_id: "srv_factor_library", server_key: "aistock-factor-library", title: "Factor Library MCP", display_title: "因子库", display_name_zh: "因子库", business_aliases_zh: ["因子目录", "因子列表"], summary_zh: "查询因子概要，详情按需展开。", status: "ready", health_json: { domain: "factor_library", display_name_zh: "因子库", business_aliases_zh: ["因子目录", "因子列表"] } },
        { server_id: "srv_factor_metrics", server_key: "aistock-factor-metrics", title: "Factor Metrics MCP", display_title: "因子独立指标", display_name_zh: "因子独立指标", business_aliases_zh: ["IC", "RankIC", "稳定性"], status: "ready", health_json: { domain: "factor_metrics" } },
        { server_id: "srv_factor_corr", server_key: "aistock-factor-correlation", title: "Factor Correlation MCP", display_title: "因子相关性", display_name_zh: "因子相关性", business_aliases_zh: ["相关性矩阵", "替换建议"], status: "ready", health_json: { domain: "factor_correlation" } },
        { server_id: "srv_model_registry", server_key: "aistock-model-registry", title: "Model Registry MCP", display_title: "模型库", display_name_zh: "模型库", business_aliases_zh: ["模型版本", "模型试验"], status: "ready", health_json: { domain: "model_registry", display_name_zh: "模型库", business_aliases_zh: ["模型版本", "模型试验"] } },
        { server_id: "srv_strategy_governance", server_key: "aistock-strategy-governance", title: "Strategy Governance MCP", display_title: "策略库", display_name_zh: "策略库", business_aliases_zh: ["策略包", "策略治理"], status: "ready", health_json: { domain: "strategy_governance", display_name_zh: "策略库", business_aliases_zh: ["策略包", "策略治理"] } },
        { server_id: "srv_execution_policy", server_key: "aistock-execution-policy", title: "Execution Policy MCP", display_title: "执行策略库", display_name_zh: "执行策略库", business_aliases_zh: ["minute algo", "TWAP", "VWAP"], status: "ready", health_json: { domain: "execution_policy" } },
      ]));
    }
    if (path.endsWith("/mcp/tools")) {
      expect(url.searchParams.get("limit")).toBe("50");
      expect(url.searchParams.get("include_schema")).toBe("false");
      return respond(page([
        { tool_id: "tool_factor_library_list", server_key: "aistock-factor-library", tool_name: "factor_library_list", title: "factor library list", risk_level: "low", requires_approval: false, status: "enabled", detail_available: true, detail_fields: ["input_schema_json", "preflight_schema_json"] },
        { tool_id: "tool_factor_metrics_plan", server_key: "aistock-factor-metrics", tool_name: "factor_metrics_plan", title: "factor metrics plan", risk_level: "plan_only", requires_approval: false, status: "enabled" },
        { tool_id: "tool_factor_corr_plan", server_key: "aistock-factor-correlation", tool_name: "factor_corr_plan", title: "factor corr plan", risk_level: "plan_only", requires_approval: false, status: "enabled" },
        { tool_id: "tool_model_registry_list", server_key: "aistock-model-registry", tool_name: "model_registry_list", title: "model registry list", risk_level: "low", requires_approval: false, status: "enabled" },
        { tool_id: "tool_strategy_governance_list", server_key: "aistock-strategy-governance", tool_name: "strategy_governance_list_packages", title: "strategy package list", risk_level: "low", requires_approval: false, status: "enabled" },
        { tool_id: "tool_execution_policy_list", server_key: "aistock-execution-policy", tool_name: "execution_policy_list_algos", title: "execution algo list", risk_level: "low", requires_approval: false, status: "enabled" },
      ]));
    }
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant/mcp-tools");
  for (const label of ["因子库", "因子独立指标", "因子相关性", "模型库", "策略库", "执行策略库"]) {
    await expect(browserPage.getByText(label).first()).toBeVisible();
  }
  await expect(browserPage.getByText("模型版本 / 模型试验").first()).toBeVisible();
  await expect(browserPage.getByText("summary-first").first()).toBeVisible();
  await expect(browserPage.getByText("include_schema=false").first()).toBeVisible();
  await expect(browserPage.getByText("ready").first()).toBeVisible();
  await expect(browserPage.locator("body")).not.toContainText("鎴");
  await expect(browserPage.locator("body")).not.toContainText("锛");
});
test("Research Assistant retired workbench redirects to the single chat entry", async ({ page: browserPage }) => {
  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data: page([]) }) });
  });

  await browserPage.goto("/research-assistant/workbench");
  await expect(browserPage).toHaveURL(/\/research-assistant\/chat$/);
  await expect(browserPage.locator(".ra-chat-input")).toBeVisible();

  const nav = browserPage.getByRole("navigation", { name: "研究助理功能导航" });
  await expect(nav.getByRole("link", { name: "对话" })).toHaveAttribute("href", "/research-assistant/chat");
  await expect(nav.getByRole("link", { name: /对话/ })).toHaveCount(1);
  await expect(nav.locator('a[href="/research-assistant"]')).toHaveCount(0);
  await expect(nav.locator('a[href="/research-assistant/workbench"]')).toHaveCount(0);
  await expect(nav.getByRole("link", { name: "工作台" })).toHaveCount(0);
  await expect(nav.locator('a[href="/research-assistant/audit"]')).toHaveCount(1);
  await expect(nav.locator('a[href="/research-assistant/tasks"]')).toHaveCount(0);
  await expect(nav.locator('a[href="/research-assistant/trace"]')).toHaveCount(0);
  await expect(nav.locator('a[href="/research-assistant/agent-runs"]')).toHaveCount(0);
  await expect(nav.locator('a[href="/research-assistant/external-agents"]')).toHaveCount(0);
  await expect(browserPage.locator("body")).not.toContainText("本地数据 MCP 工作台");
});

test("Research Assistant audit legacy routes redirect to the consolidated audit tabs", async ({ page: browserPage }) => {
  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data: page([]) }) });
  });

  const cases = [
    ["/research-assistant/tasks", "tasks", "Task Ledger"],
    ["/research-assistant/trace", "trace", "Trace Events"],
    ["/research-assistant/agent-runs", "agent-runs", "Agent 运行审计"],
    ["/research-assistant/external-agents", "external-agents", "External Agent Connector"],
  ] as const;

  for (const [legacyPath, tab, expectedText] of cases) {
    await browserPage.goto(legacyPath);
    await expect(browserPage).toHaveURL(new RegExp(`/research-assistant/audit\\?tab=${tab}$`));
    await expect(browserPage.getByRole("heading", { name: "研究助理审计" })).toBeVisible();
    await expect(browserPage.getByText(expectedText).first()).toBeVisible();
  }
});


test("Research Assistant chat shows per-turn LLM usage in the right rail only", async ({ page: browserPage }) => {
  const response = {
    ...chatTurnResponse,
    assistant_message: { ...chatTurnResponse.assistant_message, message_id: "msg_usage" },
    trace: {
      trace_id: "trace_usage",
      cost_json: {
        source_of_truth: "assistant_llm_usage_events",
        prompt_text_retained: false,
        usage_event_refs: ["assistant_llm_usage_events:llmu_usage"],
        usage_summary: {
          call_count: 2,
          prompt_tokens: 1234,
          completion_tokens: 456,
          total_tokens: 1690,
          total_cost_usd: "0.0123000000",
          usage_status: "recorded",
          cost_status: "recorded",
        },
      },
    },
  };
  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/chat/turn")) return respond(response);
    if (path.endsWith("/llm-usage/summary")) return respond({ summary: response.trace.cost_json.usage_summary });
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant");
  await browserPage.locator(".ra-chat-input").fill("usage check");
  await browserPage.locator(".ra-chat-send").click();

  await expect(browserPage.getByTestId("ra-turn-usage-panel")).toContainText("本轮消耗");
  await expect(browserPage.getByTestId("ra-turn-usage-panel")).toContainText("1,690");
  await expect(browserPage.getByTestId("ra-turn-usage-panel")).toContainText("$0.0123");
  await expect(browserPage.locator(".ra-chat-bubble").filter({ hasText: "1,690" })).toHaveCount(0);
  await expect(browserPage.locator(".ra-chat-bubble").filter({ hasText: "usage=recorded" })).toHaveCount(0);
});

test("Research Assistant audit LLM usage tab renders charts and KPI cards without tables", async ({ page: browserPage }) => {
  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/llm-usage/report")) return respond(llmUsageReportResponse);
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant/audit?tab=llm-usage");

  await expect(browserPage.getByTestId("ra-llm-usage-section")).toBeVisible();
  await expect(browserPage.getByText("LLM 消耗报表")).toBeVisible();
  await expect(browserPage.getByRole("button", { name: "最近 7 天" })).toBeVisible();
  await expect(browserPage.getByRole("button", { name: "最近 30 天" })).toBeVisible();
  await expect(browserPage.getByTestId("ra-llm-usage-kpis")).toContainText("1,620");
  await expect(browserPage.getByTestId("ra-llm-token-chart")).toBeVisible();
  await expect(browserPage.getByTestId("ra-llm-cost-chart")).toContainText("部分成本不可用");
  await expect(browserPage.getByTestId("ra-llm-top-model-chart")).toBeVisible();
  await expect(browserPage.getByTestId("ra-llm-status-chart")).toBeVisible();
  await expect(browserPage.getByTestId("ra-llm-usage-section").locator("table")).toHaveCount(0);
});

test("Research Assistant graph page renders React Flow read-only graph with local layout controls", async ({ page: browserPage }) => {
  const graphSummary = {
    namespace: "aistock",
    entity_count: 3,
    relation_count: 2,
    evolution_path_count: 1,
    entities: [
      { entity_id: "entity_qe", entity_type: "module", entity_key: "qe", title: "QE 实验", summary: "量化实验与回测", source_refs: ["docs/qe.md"], confidence: 0.98, approval_status: "approved" },
      { entity_id: "entity_paper", entity_type: "module", entity_key: "paper_v2", title: "Paper v2", summary: "模拟盘与策略包", source_refs: ["docs/paper.md"], confidence: 0.95, approval_status: "approved" },
      { entity_id: "entity_factor", entity_type: "factor", entity_key: "alpha158", title: "Alpha158 因子", summary: "因子集合", source_refs: ["docs/factor.md"], confidence: 0.9, approval_status: "draft" },
    ],
    relations: [
      { relation_id: "rel_qe_paper", source_entity_id: "entity_qe", target_entity_id: "entity_paper", relation_type: "promotes_to", evidence_refs: ["workflow:qe-to-paper"], confidence: 0.88, approval_status: "approved" },
      { relation_id: "rel_missing", source_entity_id: "entity_qe", target_entity_id: "entity_missing", relation_type: "missing_target", evidence_refs: ["workflow:missing"], confidence: 0.5, approval_status: "draft" },
    ],
    evolution_paths: [{ path_id: "path_qe_paper", objective: "QE 到 Paper v2" }],
  };

  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/graph/summary")) return respond(graphSummary);
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant/graph");

  await expect(browserPage.getByTestId("ra-graph-flow-section")).toBeVisible();
  await expect(browserPage.getByTestId("ra-graph-flow").locator(".react-flow")).toBeVisible();
  await expect(browserPage.locator(".react-flow__minimap")).toBeVisible();
  await expect(browserPage.locator(".react-flow__controls")).toBeVisible();
  await expect(browserPage.getByRole("button", { name: "重置布局" })).toBeVisible();
  await expect(browserPage.getByText("QE 实验").first()).toBeVisible();
  await expect(browserPage.getByText("Paper v2").first()).toBeVisible();
  await expect(browserPage.getByTestId("ra-graph-degraded-relations")).toContainText("graph_relation_endpoint_missing");
  await expect(browserPage.getByTestId("ra-graph-degraded-relations")).toContainText("未静默绘制成假边");

  await browserPage.getByTestId("ra-graph-node").filter({ hasText: "QE 实验" }).first().click();
  await expect(browserPage.getByTestId("ra-graph-inspector")).toContainText("实体：QE 实验");
  await expect(browserPage.getByTestId("ra-graph-inspector")).toContainText("图谱可审计详情");

  await browserPage.evaluate(() => window.localStorage.setItem("aistock.ra.graph.layout.aistock.v1", JSON.stringify({ entity_qe: { x: 17, y: 23 } })));
  await browserPage.reload();
  await expect(browserPage.getByTestId("ra-graph-flow-section")).toContainText("本地布局已恢复");
  await browserPage.getByRole("button", { name: "重置布局" }).click();
  await expect(browserPage.getByTestId("ra-graph-flow-section")).toContainText("自动布局");
});
