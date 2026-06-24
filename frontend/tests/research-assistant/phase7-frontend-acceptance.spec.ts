import { expect, test } from "@playwright/test";

function envelope(data: unknown, status = 200) {
  return { status: status >= 400 ? "error" : "success", data };
}

function pageOf<T>(items: T[]) {
  return { items, total: items.length, page: 1, page_size: 100, has_more: false };
}

const evidenceRef = {
  source: "mcp://stock/fundamental/600584",
  source_ref: "stock.fundamental.600584",
  as_of: "2026-06-02",
  provenance: { server_key: "aistock-stock-research", tool_name: "stock_fundamental_readonly" },
  confidence: 0.72,
};

const evidenceCards = [
  {
    card_id: "ev_600584_fundamental",
    title: "600584 stock evidence",
    summary: "Readonly evidence is available, but the assistant keeps investment action gated.",
    status: "supported",
    evidence_refs: [evidenceRef],
  },
  {
    card_id: "ev_missing_as_of",
    title: "Incomplete market evidence",
    summary: "The backend returned a source without as_of, so the card remains insufficient.",
    status: "supported",
    evidence_refs: [{ source: "mcp://stock/price/600584", provenance: { tool_name: "stock_price_readonly" } }],
  },
];

const blockerCards = [
  {
    blocker_id: "blk_high_risk_investment",
    status: "approval_required",
    reason: "Buy or sell judgement is high risk and requires user confirmation.",
    next_step: "Show evidence, keep the action read-only, and wait for explicit approval.",
    provenance: { policy: "investment_high_risk_gate" },
    as_of: "2026-06-02",
  },
];

const taskRows = [
  { task_id: "task_phase7", title: "600584 research request", status: "running", risk_level: "medium", updated_at: "2026-06-02T10:00:00Z" },
];

const taskDetail = {
  task: taskRows[0],
  events: [
    { event_id: "evt_phase7_1", task_id: "task_phase7", event_type: "planned", severity: "info", message: "orchestrator created read-only plan", payload_json: { source: "route" }, created_at: "2026-06-02T10:00:00Z" },
  ],
};

const agentRuns = [
  {
    agent_run_id: "agent_orchestrator_phase7",
    parent_task_id: "task_phase7",
    agent_key: "orchestrator",
    role: "orchestrator",
    status: "succeeded",
    model_profile_id: "deepseek_primary",
    trace_id: "trace_orchestrator_phase7",
    input_json: { objective: "600584 是否值得买入", context_pack_id: "ctx_phase7" },
    result_json: { summary: "Orchestrator reduced worker evidence.", reduce_summary: "Evidence visible; action remains gated.", evidence_refs: [evidenceRef] },
  },
  {
    agent_run_id: "agent_fundamental_worker_phase7",
    parent_task_id: "task_phase7",
    agent_key: "fundamental_worker",
    role: "worker",
    status: "succeeded",
    model_profile_id: "cheap_worker",
    trace_id: "trace_fundamental_worker_phase7",
    input_json: { context_pack_id: "ctx_phase7", allowed_tools: ["stock_fundamental_readonly"] },
    result_json: { summary: "Worker returned supported evidence.", evidence_cards: [evidenceCards[0]] },
  },
  {
    agent_run_id: "agent_risk_worker_phase7",
    parent_task_id: "task_phase7",
    agent_key: "risk_worker",
    role: "worker",
    status: "approval_required",
    model_profile_id: "cheap_worker",
    trace_id: "trace_risk_worker_phase7",
    input_json: { context_pack_id: "ctx_phase7", allowed_tools: ["risk_policy_readonly"] },
    result_json: { summary: "Worker blocked high risk action.", blocker_cards: blockerCards },
  },
];

const traceEvents = [
  { trace_id: "trace_orchestrator_phase7", task_id: "task_phase7", event_type: "reduce", component: "orchestrator", status: "succeeded", payload_json: { context_pack_id: "ctx_phase7" }, duration_ms: 33, created_at: "2026-06-02T10:00:00Z" },
  { trace_id: "trace_fundamental_worker_phase7", task_id: "task_phase7", event_type: "tool_observation", component: "worker", status: "succeeded", payload_json: { source_ref: "stock.fundamental.600584" }, duration_ms: 22, created_at: "2026-06-02T10:00:01Z" },
  { trace_id: "trace_risk_worker_phase7", task_id: "task_phase7", event_type: "approval_gate", component: "worker", status: "approval_required", payload_json: { policy: "investment_high_risk_gate" }, duration_ms: 11, created_at: "2026-06-02T10:00:02Z" },
];

const memories = [
  {
    memory_id: "mem_project_phase7",
    namespace: "aistock",
    scope: "project",
    memory_type: "topic",
    tree_path: "project/research_assistant/phase7/evidence_cards",
    parent_key: "project/research_assistant/phase7",
    title: "Evidence card contract",
    content_text: "Evidence cards require source, provenance, and as_of.",
    approval_status: "approved",
    source_ref: "blueprint#phase7",
    evidence_refs: [evidenceRef],
    updated_at: "2026-06-02T10:00:00Z",
  },
  {
    memory_id: "mem_personal_phase7",
    namespace: "personal",
    scope: "personal",
    memory_type: "preference",
    tree_path: "personal/operator/preferences/summary_only",
    parent_key: "personal/operator/preferences",
    title: "Summary only main bubble",
    content_text: "Keep worker process in Workbench and Trace.",
    approval_status: "approved",
    source_ref: "user#phase7",
    evidence_refs: [evidenceRef],
    updated_at: "2026-06-02T10:00:00Z",
  },
];

const contextPacks = [
  {
    context_pack_id: "ctx_phase7",
    task_id: "task_phase7",
    pack_summary: "Phase7 context pack consumed tree branches.",
    token_budget: 16000,
    checksum: "phase7checksum",
    pack_json: {
      matched_branches: ["project/research_assistant/phase7/evidence_cards", "personal/operator/preferences/summary_only"],
      route_reason: "600584 query matched stock evidence and summary-only preference branches.",
      omitted_relevant_refs: [],
      graph_relation_refs: ["graph://research-assistant/phase7"],
      evidence_refs: [evidenceRef],
    },
  },
];

const chatTurnResponse = {
  conversation: { conversation_id: "conv_phase7", title: "600584 research" },
  assistant_message: {
    message_id: "msg_phase7_assistant",
    role: "assistant",
    content_text: "Orchestrator summary: 600584 has readonly evidence, but any buy or sell action remains blocked until approval.",
    content_json: {
      worker_results: [{ hidden: true }],
      payload_json: { hidden: true },
    },
  },
  task: taskRows[0],
  cards: {
    orchestrator_summary: "Orchestrator summary: 600584 has readonly evidence, but any buy or sell action remains blocked until approval.",
    evidence_cards: evidenceCards,
    blocker_cards: blockerCards,
    worker_results: [{ trace_id: "trace_fundamental_worker_phase7", payload_json: { hidden: true } }],
    action_proposals: [],
  },
  context_pack: { evidence_refs: [evidenceRef] },
  mode_decision: { mode: "analysis", intent_type: "stock_research" },
};

test.beforeEach(async ({ page }) => {
  const consoleErrors: string[] = [];
  const pageErrors: string[] = [];
  const forbiddenRequests: string[] = [];

  page.on("console", (message) => {
    if (message.type() === "error" && !message.text().includes("Failed to load resource")) consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("request", (request) => {
    const url = request.url();
    const path = new URL(url).pathname;
    const isMockedAlertRequest = path.startsWith("/api/ingestion/alerts/");
    if (!isMockedAlertRequest && (url.includes(":8001") || url.includes(":3000") || url.includes(":19080"))) forbiddenRequests.push(url);
  });

  await page.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify(envelope(data, status)) });

    if (path.endsWith("/chat/turn")) return respond(chatTurnResponse);
    if (path.endsWith("/memories")) return respond(pageOf(memories));
    if (path.endsWith("/context-packs")) return respond(pageOf(contextPacks));
    if (path.endsWith("/agent-runs")) return respond(pageOf(agentRuns));
    if (path.endsWith("/trace-events")) return respond(pageOf(traceEvents));
    if (path.endsWith("/overview")) return respond({ trace_status: { succeeded: 2, approval_required: 1 } });
    if (path.endsWith("/tasks")) return respond(pageOf(taskRows));
    if (path.endsWith("/tasks/task_phase7")) return respond(taskDetail);
    if (path.endsWith("/actions")) return respond(pageOf([]));
    if (path.endsWith("/capabilities")) return respond(pageOf([]));
    if (path.endsWith("/mcp/tools")) return respond(pageOf([]));
    return respond(pageOf([]));
  });
  await page.route("**/api/ingestion/alerts/**", async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ alerts: [], count: 0 }) }));

  (page as typeof page & { _phase7Errors?: () => { consoleErrors: string[]; pageErrors: string[]; forbiddenRequests: string[] } })._phase7Errors = () => ({ consoleErrors, pageErrors, forbiddenRequests });
});

test.afterEach(async ({ page }) => {
  const errors = (page as typeof page & { _phase7Errors?: () => { consoleErrors: string[]; pageErrors: string[]; forbiddenRequests: string[] } })._phase7Errors?.();
  expect(errors?.consoleErrors || []).toEqual([]);
  expect(errors?.pageErrors || []).toEqual([]);
  expect(errors?.forbiddenRequests || []).toEqual([]);
});

async function assertNoForbiddenUiText(pageText: string) {
  expect(pageText).not.toContain("TODO");
  expect(pageText).not.toContain("placeholder");
  expect(pageText).not.toContain("XX");
  expect(pageText).not.toContain("X%");
  expect(pageText).not.toContain("约X");
  expect(pageText).not.toContain("mock success");
}

test("Phase 7 chat shows evidence and blockers while main bubble stays summary-only", async ({ page }) => {
  await page.goto("/research-assistant");
  await page.locator(".ra-chat-input").fill("600584 是否值得买入");
  await page.locator(".ra-chat-send").click();

  await expect(page.getByText("Orchestrator summary: 600584").first()).toBeVisible();
  await expect(page.getByTestId("ra-phase7-evidence-panel")).toBeVisible();
  await expect(page.getByTestId("ra-evidence-card").first()).toContainText("mcp://stock/fundamental/600584");
  await expect(page.getByTestId("ra-evidence-card").first()).toContainText("2026-06-02");
  await expect(page.getByTestId("ra-evidence-card").first()).toContainText("server_key");
  await expect(page.getByTestId("ra-evidence-gap")).toContainText("as_of");
  await expect(page.getByTestId("ra-blocker-card")).toContainText("approval_required");
  await expect(page.getByTestId("ra-blocker-card")).toContainText("investment_high_risk_gate");

  const bubbleText = await page.locator(".ra-chat-bubble").last().textContent();
  expect(bubbleText || "").not.toContain("worker_results");
  expect(bubbleText || "").not.toContain("payload_json");
  expect(bubbleText || "").not.toContain("trace_id");
  expect(bubbleText || "").not.toContain("{");
  await assertNoForbiddenUiText(await page.locator("body").innerText());
});

test("Phase 7 memory tree renders project and personal branches with context pack route evidence", async ({ page }) => {
  await page.goto("/research-assistant/memory");

  await expect(page.getByTestId("ra-memory-tree-view")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Project tree" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Personal tree" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Evidence card contract" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Summary only main bubble" })).toBeVisible();
  await expect(page.getByTestId("ra-context-pack-routes")).toContainText("matched_branches");
  await expect(page.getByTestId("ra-context-pack-routes")).toContainText("route_reason");
  await expect(page.getByTestId("ra-context-pack-routes")).toContainText("project/research_assistant/phase7/evidence_cards");
  await assertNoForbiddenUiText(await page.locator("body").innerText());
});

test("Phase 7 Agent Teams view shows orchestrator, workers, reduce, evidence, and approval blockers", async ({ page }) => {
  await page.goto("/research-assistant/audit?tab=tasks");

  await expect(page.getByTestId("ra-agent-teams-view")).toBeVisible();
  await expect(page.getByTestId("ra-agent-teams-view")).toContainText("2 workers");
  await expect(page.getByTestId("ra-agent-teams-view")).toContainText("orchestrator");
  await expect(page.getByTestId("ra-agent-teams-view")).toContainText("fundamental_worker:succeeded");
  await expect(page.getByTestId("ra-agent-teams-view")).toContainText("risk_worker:approval_required");
  await expect(page.getByTestId("ra-agent-teams-view")).toContainText("Evidence visible; action remains gated.");
  await expect(page.getByTestId("ra-agent-teams-view")).toContainText("approval_required");
  await expect(page.getByTestId("ra-agent-teams-view")).toContainText("Open worker trace");
  await assertNoForbiddenUiText(await page.locator("body").innerText());
});

test("Phase 7 Trace page carries worker process instead of hiding failures", async ({ page }) => {
  await page.goto("/research-assistant/audit?tab=trace");

  await expect(page.getByTestId("ra-agent-teams-view")).toBeVisible();
  await expect(page.getByTestId("ra-agent-teams-view")).toContainText("risk_worker:approval_required");
  await expect(page.getByText("approval_gate").first()).toBeVisible();
  await expect(page.getByText("trace_risk_worker_phase7").first()).toBeVisible();
  await assertNoForbiddenUiText(await page.locator("body").innerText());
});
