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
  },
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
  await expect(browserPage.getByTestId("ra-chat-main")).toContainText("aistock-validation/mcp_github_issue_sync_bug");
  await expect(browserPage.getByTestId("ra-mcp-route-card")).toContainText("需要确认和审批后才可执行");
});

test("Research Assistant MCP tools page treats ready servers as ready", async ({ page: browserPage }) => {
  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/mcp/servers")) {
      return respond(page([
        { server_id: "srv_factor_library", server_key: "aistock-factor-library", title: "Factor Library MCP", display_title: "因子库", display_name_zh: "因子库", business_aliases_zh: ["因子目录", "因子列表"], status: "ready", health_json: { domain: "factor_library", display_name_zh: "因子库", business_aliases_zh: ["因子目录", "因子列表"] } },
        { server_id: "srv_model_registry", server_key: "aistock-model-registry", title: "Model Registry MCP", display_title: "模型库", display_name_zh: "模型库", business_aliases_zh: ["模型版本", "模型试验"], status: "ready", health_json: { domain: "model_registry", display_name_zh: "模型库", business_aliases_zh: ["模型版本", "模型试验"] } },
        { server_id: "srv_strategy_governance", server_key: "aistock-strategy-governance", title: "Strategy Governance MCP", display_title: "策略库", display_name_zh: "策略库", business_aliases_zh: ["策略包", "策略治理"], status: "ready", health_json: { domain: "strategy_governance", display_name_zh: "策略库", business_aliases_zh: ["策略包", "策略治理"] } },
      ]));
    }
    if (path.endsWith("/mcp/tools")) {
      expect(url.searchParams.get("limit")).toBe("50");
      expect(url.searchParams.get("include_schema")).toBe("false");
      return respond(page([
        { tool_id: "tool_factor_library_list", server_key: "aistock-factor-library", tool_name: "factor_library_list", title: "factor library list", risk_level: "low", requires_approval: false, status: "enabled", detail_available: true, detail_fields: ["input_schema_json", "preflight_schema_json"] },
      ]));
    }
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant/mcp-tools");
  await expect(browserPage.getByText("因子库").first()).toBeVisible();
  await expect(browserPage.getByText("模型库").first()).toBeVisible();
  await expect(browserPage.getByText("策略库").first()).toBeVisible();
  await expect(browserPage.getByText("模型版本 / 模型试验")).toBeVisible();
  await expect(browserPage.getByText("已就绪").first()).toBeVisible();
  await expect(browserPage.getByText("未就绪")).toHaveCount(0);
});

test("Research Assistant admin page separates audit tools from the chat entry", async ({ page: browserPage }) => {
  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/mcp/tools")) {
      return respond(page([
        {
          tool_id: "mcp_tool_research_assistant_issue",
          server_key: "research-assistant",
          tool_name: "assistant_create_issue_candidate",
          title: "创建候选 Issue",
          risk_level: "high",
          requires_approval: true,
          status: "enabled",
          input_schema_json: { type: "object" },
          preflight_schema_json: { checks: ["dedupe_key"] },
          required_confirmations: ["APPROVE_RESEARCH_ASSISTANT_ACTION"],
        },
      ]));
    }
    if (path.endsWith("/tasks")) return respond(page([{ task_id: "rat_demo_1", title: "QE 实验规划", status: "running" }]));
    if (path.endsWith("/mcp/preflight")) return respond({ passed: false, approval_required: true, missing_confirmations: ["APPROVE_RESEARCH_ASSISTANT_ACTION"], trace_event: { event_type: "approval_required" }, deep_links: ["/research-assistant/approvals"] });
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant/admin");
  await expect(browserPage.getByRole("heading", { name: "旧版表格与 JSON 详情保留在这里" })).toBeVisible();
  await expect(browserPage.getByText("后台管理区面向开发、审计和问题排查")).toBeVisible();

  await browserPage.goto("/research-assistant/workbench");
  await expect(browserPage.getByRole("heading", { name: "Action Proposal 执行控制台" })).toBeVisible();
  await expect(browserPage.getByText("输入 JSON")).toBeVisible();
  await browserPage.getByRole("button", { name: "执行 preflight" }).click();
  await browserPage.getByText("dry-run / preflight debug payload").click();
  await expect(browserPage.getByText("Missing Confirmations").first()).toBeVisible();
});


test("Research Assistant workbench surfaces local data MCP tools as readable cards", async ({ page: browserPage }) => {
  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (path.endsWith("/capabilities")) return respond(page([]));
    if (path.endsWith("/actions")) return respond(page([]));
    if (path.endsWith("/tasks")) return respond(page([{ task_id: "rat_local_data_1", title: "本地数据检查", status: "running" }]));
    if (path.endsWith("/mcp/tools")) {
      return respond(page([
        {
          tool_id: "mcp_tool_local_data_health",
          server_key: "aistock-local-data",
          tool_name: "local_data_health_overview",
          title: "数据健康总览",
          risk_level: "read_only",
          requires_approval: false,
          status: "enabled",
          input_schema_json: { type: "object" },
          preflight_schema_json: { checks: ["dataset", "alerts", "jobs"] },
          required_confirmations: [],
        },
        {
          tool_id: "mcp_tool_local_data_plan_repair",
          server_key: "aistock-local-data",
          tool_name: "local_data_plan_repair",
          title: "生成本地数据修复计划",
          risk_level: "plan_only",
          requires_approval: false,
          status: "enabled",
          input_schema_json: { type: "object" },
          preflight_schema_json: { checks: ["overview", "gaps", "targets"] },
          required_confirmations: [],
        },
      ]));
    }
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant/workbench");
  await expect(browserPage.getByRole("heading", { name: "本地数据 MCP 工作台" })).toBeVisible();
  await expect(browserPage.getByTestId("ra-local-data-workbench-card")).toContainText("local_data_management");
  await expect(browserPage.getByText("本地数据检查").first()).toBeVisible();
  await expect(browserPage.getByText("本地数据工具目录")).toBeVisible();
  await expect(browserPage.getByTestId("ra-local-data-tool-cards")).toContainText("数据健康总览");
  await expect(browserPage.getByTestId("ra-local-data-tool-cards")).toContainText("生成本地数据修复计划");
});
