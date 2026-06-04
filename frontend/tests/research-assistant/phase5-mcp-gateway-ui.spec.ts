import { expect, test } from "@playwright/test";

function envelope(data: unknown, status = 200) {
  return { status: status >= 400 ? "error" : "success", data };
}

function pageOf<T>(items: T[], extra: Record<string, unknown> = {}) {
  return { items, total: items.length, page: 1, page_size: 100, has_more: false, ...extra };
}

const servers = [
  {
    server_id: "srv_gateway_lite",
    server_key: "aistock-gateway-lite",
    title: "Gateway Lite",
    display_title: "Gateway Lite",
    status: "ready",
    health_json: { domain: "catalog", summary_zh: "Catalog, health and preflight tools" },
  },
  {
    server_id: "srv_qe",
    server_key: "aistock-qe",
    title: "QE Gateway",
    display_title: "QE Gateway",
    status: "ready",
    health_json: { domain: "qe_archive" },
  },
  {
    server_id: "srv_external",
    server_key: "aistock-external-research",
    title: "External Research",
    display_title: "External Research",
    status: "ready",
    health_json: { domain: "external_research" },
  },
  {
    server_id: "srv_validation",
    server_key: "aistock-validation",
    title: "Validation Center",
    display_title: "Validation Center",
    status: "ready",
    health_json: { domain: "validation" },
  },
  {
    server_id: "srv_local_data",
    server_key: "aistock-local-data",
    title: "Local Data",
    display_title: "Local Data",
    status: "ready",
    health_json: { domain: "local_data" },
  },
];

const tools = [
  {
    tool_id: "mcp_tool_aistock_qe_qe_archive_query_run_leaderboard",
    server_key: "aistock-qe",
    tool_name: "qe_archive_query_run_leaderboard",
    title: "QE archive leaderboard",
    description: "Read archived run leaderboard rows.",
    risk_level: "low",
    manifest_risk_level: "read_only",
    assistant_usable: "direct_or_catalog",
    side_effect_level: "read_only",
    requires_approval: false,
    status: "enabled",
    module: "qe_archive",
    profile: "qe",
    profile_tags: ["qe"],
    backend_endpoint: "GET /api/v1/qe-archive/analytics/run-leaderboard",
    catalog_source: "gateway_manifest_derived_catalog",
    detail_available: true,
    detail_fields: ["preflight_schema_json"],
  },
  {
    tool_id: "mcp_tool_external_search_web",
    server_key: "aistock-external-research",
    tool_name: "external_research_search_web",
    title: "External web search",
    description: "L2.5 read-only evidence-first retrieval.",
    risk_level: "low",
    manifest_risk_level: "read_only",
    assistant_usable: "direct_or_catalog",
    side_effect_level: "read_only",
    requires_approval: false,
    status: "enabled",
    module: "external_research",
    profile: "external_research",
    profile_tags: ["external_research"],
    backend_endpoint: "POST /api/v1/external-research/search-web",
    catalog_source: "gateway_manifest_derived_catalog",
  },
  {
    tool_id: "mcp_tool_external_save_evidence",
    server_key: "aistock-external-research",
    tool_name: "external_research_save_evidence",
    title: "Save external evidence draft",
    description: "Draft write remains preflight required.",
    risk_level: "high",
    manifest_risk_level: "external_network",
    assistant_usable: "preflight_required",
    side_effect_level: "draft_only",
    requires_approval: true,
    status: "enabled",
    module: "external_research",
    profile: "external_research",
    profile_tags: ["external_research"],
    backend_endpoint: "POST /api/v1/external-research/evidence",
    catalog_source: "gateway_manifest_derived_catalog",
  },
  {
    tool_id: "mcp_tool_validation_start_validation_execution",
    server_key: "aistock-validation",
    tool_name: "start_validation_execution",
    title: "Start validation execution",
    description: "Long-running validation runner preflight.",
    risk_level: "high",
    manifest_risk_level: "long_running",
    assistant_usable: "preflight_required",
    side_effect_level: "high_cost_compute",
    requires_approval: true,
    status: "enabled",
    module: "validation",
    profile: "validation",
    profile_tags: ["validation"],
    backend_endpoint: "POST /api/v1/validation/executions",
    catalog_source: "gateway_manifest_derived_catalog",
  },
  {
    tool_id: "mcp_tool_local_data_list_sync_targets",
    server_key: "aistock-local-data",
    tool_name: "local_data_list_sync_targets",
    title: "List local-data sync targets",
    description: "Read configured sync targets.",
    risk_level: "low",
    manifest_risk_level: "read_only",
    assistant_usable: "direct_or_catalog",
    side_effect_level: "read_only",
    requires_approval: false,
    status: "enabled",
    module: "local_data",
    profile: "data",
    profile_tags: ["data"],
    backend_endpoint: "GET /api/v1/local-data/targets",
    catalog_source: "gateway_manifest_derived_catalog",
  },
];

let toolEvents = [
  {
    tool_event_id: "mcptev_existing_readonly",
    task_id: "task_phase5",
    server_key: "aistock-qe",
    tool_name: "qe_archive_query_run_leaderboard",
    event_type: "execute_read_only",
    status: "succeeded",
    request_json: { limit: 1 },
    response_json: { profile: "qe", approval_required: false, evidence_refs: ["manifest:qe_archive_query_run_leaderboard"] },
    result_card_json: { title: "QE leaderboard", summary: "2 rows returned" },
    artifact_refs: ["mcp://qe/archive/leaderboard"],
  },
];

const preflightResponse = {
  server_key: "aistock-external-research",
  canonical_server_key: "aistock-external-research",
  requested_server_key: "aistock-external-research",
  tool_name: "external_research_save_evidence",
  module: "external_research",
  profile: "external_research",
  risk_level: "high",
  manifest_risk_level: "external_network",
  assistant_usable: "preflight_required",
  side_effect_level: "draft_only",
  requires_approval: true,
  passed: false,
  approval_required: true,
  missing_confirmations: ["APPROVE_EXTERNAL_EVIDENCE_DRAFT"],
  preflight_checks: ["evidence_refs", "draft_only"],
  failed_checks: [],
  catalog_source: "gateway_manifest_derived_catalog",
  evidence_refs: ["manifest:external_research_save_evidence", "profile:external_research"],
  tool_event_id: "mcptev_phase5_preflight",
};

test.beforeEach(async ({ page }) => {
  const consoleErrors: string[] = [];
  const pageErrors: string[] = [];
  const forbiddenRequests: string[] = [];
  toolEvents = toolEvents.slice(0, 1);

  page.on("console", (message) => {
    if (message.type() === "error" && !message.text().includes("Failed to load resource")) consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("request", (request) => {
    const url = request.url();
    if (url.includes(":8001") || url.includes(":3000") || url.includes(":19080")) forbiddenRequests.push(url);
  });

  await page.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify(envelope(data, status)) });

    if (path.endsWith("/mcp/servers")) return respond(pageOf(servers));
    if (path.endsWith("/mcp/tools")) {
      expect(url.searchParams.get("include_schema")).toBe("false");
      return respond(pageOf(tools, {
        source: "gateway_manifest_derived_catalog",
        catalog_source: "gateway_manifest_derived_catalog",
        manifest_tool_count: 209,
        server_count: 9,
        risk_distribution: { read_only: 3, external_network: 1, long_running: 1 },
        profile_distribution: { qe: 1, external_research: 2, validation: 1, data: 1 },
        backend_health: { status: "not_checked", reason: "mocked route test does not call live backend" },
        recent_smoke: { status: "not_run", reason: "mocked route test" },
      }));
    }
    if (path.endsWith("/mcp/preflight")) {
      const body = route.request().postDataJSON();
      expect(body.tool_name).toBe("external_research_save_evidence");
      toolEvents = [
        {
          tool_event_id: "mcptev_phase5_preflight",
          task_id: null,
          server_key: "aistock-external-research",
          tool_name: "external_research_save_evidence",
          event_type: "preflight",
          status: "approval_required",
          request_json: body.payload_json,
          response_json: preflightResponse,
          result_card_json: { title: "Preflight card", summary: "Approval pending" },
          artifact_refs: [],
        },
        ...toolEvents,
      ];
      return respond(preflightResponse);
    }
    if (path.endsWith("/mcp/tool-events")) return respond(pageOf(toolEvents));
    return respond(pageOf([]));
  });
  await page.route("**/api/ingestion/alerts/**", async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ alerts: [], count: 0 }) }));

  (page as typeof page & { _phase5Errors?: () => { consoleErrors: string[]; pageErrors: string[]; forbiddenRequests: string[] } })._phase5Errors = () => ({ consoleErrors, pageErrors, forbiddenRequests });
});

test.afterEach(async ({ page }) => {
  const errors = (page as typeof page & { _phase5Errors?: () => { consoleErrors: string[]; pageErrors: string[]; forbiddenRequests: string[] } })._phase5Errors?.();
  expect(errors?.consoleErrors || []).toEqual([]);
  expect(errors?.pageErrors || []).toEqual([]);
  expect(errors?.forbiddenRequests || []).toEqual([]);
});

test("Phase 5 MCP tools page shows unified catalog health, filters, profile recommendation, preflight and audit evidence", async ({ page }) => {
  await page.goto("/research-assistant/mcp-tools");

  await expect(page.getByTestId("ra-mcp-catalog-source")).toContainText("gateway_manifest_derived_catalog");
  await expect(page.getByTestId("ra-mcp-manifest-count")).toContainText("209");
  await expect(page.getByTestId("ra-mcp-backend-health")).toContainText("not_checked");
  await expect(page.getByTestId("ra-mcp-recent-smoke")).toContainText("not_run");
  await expect(page.getByTestId("ra-mcp-risk-distribution")).toContainText("external_network");
  await expect(page.getByTestId("ra-mcp-profile-distribution")).toContainText("external_research");

  await page.getByPlaceholder("leaderboard, external, validation...").fill("external");
  await expect(page.getByTestId("ra-mcp-tool-row-external_research_search_web")).toBeVisible();
  await expect(page.getByTestId("ra-mcp-tool-row-external_research_save_evidence")).toBeVisible();
  await expect(page.getByTestId("ra-mcp-tool-row-qe_archive_query_run_leaderboard")).toHaveCount(0);

  await page.getByTestId("ra-mcp-tool-row-external_research_save_evidence").getByRole("button", { name: "Inspect" }).click();
  await expect(page.getByTestId("ra-mcp-selected-tool")).toContainText("external_research_save_evidence");
  await expect(page.getByTestId("ra-mcp-profile-recommendation")).toContainText("external_research");
  await expect(page.getByTestId("ra-mcp-approval-state")).toContainText("preflight_required");

  await page.getByTestId("ra-mcp-run-preflight").click();
  await expect(page.getByTestId("ra-mcp-preflight-result")).toContainText("approval_required");
  await expect(page.getByTestId("ra-mcp-preflight-result")).toContainText("APPROVE_EXTERNAL_EVIDENCE_DRAFT");
  await expect(page.getByTestId("ra-mcp-evidence-refs")).toContainText("manifest:external_research_save_evidence");
  await expect(page.getByTestId("ra-mcp-audit-panel")).toContainText("mcptev_phase5_preflight");
  await expect(page.getByTestId("ra-mcp-audit-panel")).toContainText("external_research_save_evidence");

  const bodyText = await page.locator("body").innerText();
  expect(bodyText).not.toContain("placeholder");
  expect(bodyText).not.toContain("mock success");
  expect(bodyText).not.toContain("raw_payload");
});
