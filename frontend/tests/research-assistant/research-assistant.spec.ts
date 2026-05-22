import { expect, test } from "@playwright/test";

const chatTurnResponse = {
  conversation: { title: "QE 10 loop 实验" },
  assistant_message: {
    content_text: "我理解你要创建 QE 10 loop 回测实验。本轮我会先确认固定 PIT 股票池、生成计划卡，并等待你确认，不执行物化或运行。",
    content_json: {},
  },
  task: { title: "QE 10 loop 实验", status: "planned" },
  task_events: [
    { event_type: "chat_received", message: "已接收用户对话需求，进入理解与计划阶段。" },
    { event_type: "llm_done", message: "主模型已返回，计划卡和确认卡已生成。" },
  ],
  cards: {
    plan_card: {
      title: "本轮计划",
      steps: [
        "复述 QE 实验目标、收益评估方向和本轮不执行的边界。",
        "从 QE MCP 目录中选择模板创建、验证、预检查相关能力，并确认固定 PIT 股票池要求。",
        "生成 10 个 loop 的草稿结构、候选因子来源、时间窗和成本约束。",
      ],
    },
    clarification_card: {
      title: "需要你确认",
      questions: [
        "本次 QE 回测应使用哪个固定 PIT 股票池或默认回测股票池？",
        "确认前是否继续保持只生成草稿，不调用 materialize/run？",
      ],
    },
    action_proposals: [
      { title: "生成 QE 10 loop 实验草稿", risk: "medium", approval_required: false, status: "draft_only" },
      { title: "QE template validate + MCP preflight", risk: "high", approval_required: true, status: "waiting_confirmation" },
    ],
    status_rail: [
      { label: "接收需求", status: "done" },
      { label: "选择提示词", status: "done" },
      { label: "构建上下文", status: "done" },
      { label: "等待确认", status: "current" },
      { label: "MCP 预检查", status: "locked" },
      { label: "执行", status: "locked" },
      { label: "写入记忆", status: "locked" },
    ],
    capability_summary: {
      mcp: "已识别 Research Assistant、QE、Validation 等 MCP 能力候选。",
      skill: "已纳入本地 Skill Catalog，后续可按任务加载 QE 诊断、因子分析等能力。",
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

    if (path.endsWith("/chat/turn")) return respond(chatTurnResponse);
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant");
  await expect(browserPage.getByRole("heading", { name: "像 Codex 一样对话，由 MCP 安全执行" })).toBeVisible();
  await expect(browserPage.getByText("助理正在做什么")).toBeVisible();
  await expect(browserPage.getByText("主界面不显示 JSON")).toBeVisible();

  await browserPage.getByPlaceholder(/直接描述你的研究目标/).fill("帮我创建一个 QE 10 loop 实验，先不要执行。");
  await browserPage.getByRole("button", { name: "发送" }).click();

  await expect(browserPage.getByText("我理解你要创建 QE 10 loop 回测实验").first()).toBeVisible();
  await expect(browserPage.getByTestId("ra-chat-plan-card")).toContainText("本轮计划");
  await expect(browserPage.getByTestId("ra-chat-confirm-card")).toContainText("固定 PIT 股票池");
  await expect(browserPage.getByText("等待确认").first()).toBeVisible();
  await expect(browserPage.getByText("不会执行 QE materialize/run").first()).toBeVisible();
  await expect(browserPage.getByText("生成 QE 10 loop 实验草稿").first()).toBeVisible();

  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("payload_json");
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("trace_id");
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("task_id");
  await expect(browserPage.locator("[data-testid='ra-chat-main']")).not.toContainText("{");

  expect(writeMethods).toEqual(["POST /api/v1/research-assistant/chat/turn"]);
  expect(pageErrors).toEqual([]);
  expect(consoleErrors.filter((message) => !message.includes("Failed to load resource"))).toEqual([]);
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
    if (path.endsWith("/tasks")) return respond(page([{ task_id: "rat_demo_1", title: "QE 10 loop 实验规划", status: "running" }]));
    if (path.endsWith("/mcp/preflight")) return respond({ passed: false, approval_required: true, missing_confirmations: ["APPROVE_RESEARCH_ASSISTANT_ACTION"], trace_event: { event_type: "approval_required" }, deep_links: ["/research-assistant/approvals"] });
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant/admin");
  await expect(browserPage.getByRole("heading", { name: "旧版表格与 JSON 详情保留在这里" })).toBeVisible();
  await expect(browserPage.getByText("后台管理区面向开发、审计和问题排查")).toBeVisible();

  await browserPage.getByRole("link", { name: /MCP 执行工作台/ }).click();
  await expect(browserPage.getByRole("heading", { name: "MCP 执行工作台" })).toBeVisible();
  await expect(browserPage.getByText("配置草稿 JSON")).toBeVisible();
  await browserPage.getByRole("button", { name: "执行 preflight" }).click();
  await expect(browserPage.getByText("Missing Confirmations").first()).toBeVisible();
});
