import fs from "fs";
import path from "path";

import { expect, test } from "@playwright/test";

const labels = {
  title: "\u81ea\u52a8\u5316\u6d4b\u8bd5\u6d41\u6c34\u7ebf\u4e2d\u5fc3",
  gitWorkspace: "Git \u5de5\u4f5c\u533a\u72b6\u6001",
  moduleQuality: "\u6a21\u5757\u8d28\u91cf\u4f18\u5148\u7ea7",
  needsValidation: "\u9700\u8981\u9a8c\u8bc1\u6a21\u5757",
  commitActivityUpper: "\u8fd1\u671f COMMIT",
  commitActivityTitle: "\u8fd1\u671f Commit",
  fileOwnershipAggregation: "\u6309\u6587\u4ef6\u5f52\u5c5e\u81ea\u52a8\u805a\u5408",
  navigationSource: "UI Target Route Coverage",
  navigationSharedSource: "Catalog: tests/aistock_validation/catalog/ui_targets.yaml",
  routeDetail: "UI Target Detail",
  viewCoverage: "View UI target coverage",
  pipelineCenter: "Validation Center",
  validationCenterRoute: "/validation-center",
};

const backendPort = process.env.BACKEND_PORT || "8012";
const frontendPort = process.env.FRONTEND_PORT || "3012";
const apiBase =
  process.env.VALIDATION_CENTER_API_BASE ||
  process.env.NEXT_PUBLIC_API_BASE ||
  `http://127.0.0.1:${backendPort}/api/v1`;
const frontendBase = process.env.FRONTEND_BASE_URL || `http://127.0.0.1:${frontendPort}`;
const outputPath =
  process.env.VALIDATION_CENTER_UI_SMOKE_OUTPUT ||
  path.resolve(process.cwd(), "..", "tmp", "validation", "validation_center", "ui_real_port_smoke.json");

type SmokeSummary = {
  schema_version: string;
  status: "running" | "passed" | "failed";
  generated_at: string;
  frontend_url: string;
  api_base: string;
  assertions: Record<string, boolean>;
  validation_responses: string[];
  bad_responses: string[];
  request_failures: string[];
  page_errors: string[];
  console_errors: string[];
  write_methods_sent: string[];
  production_8001_touched: boolean;
};

function isForbiddenApiBase(base: string): boolean {
  try {
    const parsed = new URL(base);
    return parsed.port === "8001" || !["127.0.0.1", "localhost", "::1"].includes(parsed.hostname);
  } catch {
    return true;
  }
}

function writeSummary(summary: SmokeSummary): void {
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
}

async function waitForCollectedResponse(
  responses: string[],
  pathFragment: string,
  timeoutMs = 30_000,
): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (responses.some((line) => line.startsWith("200 ") && line.includes(pathFragment))) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

test("Validation Center Git and module quality panels work against real dev ports", async ({ page }) => {
  if (isForbiddenApiBase(apiBase)) {
    throw new Error("Refusing to run Validation Center real-port UI smoke against non-dev API base.");
  }

  const summary: SmokeSummary = {
    schema_version: "aistock_validation_center_real_port_ui_smoke_v1",
    status: "running",
    generated_at: new Date().toISOString(),
    frontend_url: `${frontendBase}/validation-center`,
    api_base: apiBase,
    assertions: {},
    validation_responses: [],
    bad_responses: [],
    request_failures: [],
    page_errors: [],
    console_errors: [],
    write_methods_sent: [],
    production_8001_touched: false,
  };

  page.on("pageerror", (error) => summary.page_errors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") {
      summary.console_errors.push(message.text());
    }
  });
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/validation/") && request.method() !== "GET") {
      summary.write_methods_sent.push(`${request.method()} ${request.url()}`);
    }
  });
  page.on("requestfailed", (request) => {
    if (!request.url().includes("/_next/webpack-hmr")) {
      summary.request_failures.push(
        `${request.method()} ${request.url()} ${request.failure()?.errorText || ""}`.trim(),
      );
    }
  });
  page.on("response", (response) => {
    if (response.url().includes("/api/v1/validation/")) {
      const line = `${response.status()} ${response.url()}`;
      summary.validation_responses.push(line);
      if (response.status() >= 400) {
        summary.bad_responses.push(line);
      }
    }
  });

  await page.route("**/api/ingestion/**", async (route) => {
    const pathName = new URL(route.request().url()).pathname;
    const data = pathName.endsWith("/unack-count") ? { count: 0 } : { alerts: [], items: [], total: 0 };
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(data),
    });
  });

  try {
    await page.goto(`${frontendBase}/validation-center`, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await expect(page.getByText("Validation Center / Read Only")).toBeVisible({ timeout: 30_000 });
    const phaseTabs = page.locator(".pv2-phase-tab");
    await expect(phaseTabs).toHaveCount(10, { timeout: 30_000 });

    await phaseTabs.nth(7).click();
    await expect(page.getByText("read_only_allowlist").first()).toBeVisible({ timeout: 30_000 });

    await phaseTabs.nth(5).click();
    await expect(page.getByText("validation.center").first()).toBeVisible({ timeout: 30_000 });

    await phaseTabs.nth(4).click();
    await expect(page.getByRole("heading", { name: labels.navigationSource })).toBeVisible({ timeout: 30_000 });
    const validationRouteRow = page.locator("tr", { hasText: labels.pipelineCenter }).filter({ hasText: labels.validationCenterRoute }).first();
    await expect(validationRouteRow).toBeVisible({ timeout: 30_000 });
    await validationRouteRow.getByRole("button", { name: labels.viewCoverage }).click();
    await expect(page.getByRole("heading", { name: labels.routeDetail })).toBeVisible({ timeout: 30_000 });

    await waitForCollectedResponse(summary.validation_responses, "/git/commit-activity");
    await waitForCollectedResponse(summary.validation_responses, "/modules/quality-summary");
    await waitForCollectedResponse(summary.validation_responses, "/modules/detail-summary");
    await waitForCollectedResponse(summary.validation_responses, "/ui-targets");
    await waitForCollectedResponse(summary.validation_responses, "/cards/summary");

    const body = await page.locator("body").innerText();
    summary.assertions = {
      has_title: await page.getByText("Validation Center / Read Only").isVisible(),
      has_phase_tabs: await phaseTabs.count() === 10,
      has_git_workspace_panel_response: summary.validation_responses.some(
        (line) => line.startsWith("200 ") && line.includes("/git/workspace-status"),
      ),
      has_module_quality_panel_response: summary.validation_responses.some(
        (line) => line.startsWith("200 ") && line.includes("/modules/quality-summary"),
      ),
      has_module_detail_panel_response: summary.validation_responses.some(
        (line) => line.startsWith("200 ") && line.includes("/modules/detail-summary"),
      ),
      has_navigation_source_panel: body.includes(labels.navigationSource),
      has_route_detail_panel: body.includes(labels.routeDetail),
      has_validation_center_route: body.includes(labels.validationCenterRoute),
      has_cards_endpoint_response: summary.validation_responses.some(
        (line) => line.startsWith("200 ") && line.includes("/cards/summary"),
      ),
      has_commit_activity_endpoint_response: summary.validation_responses.some(
        (line) => line.startsWith("200 ") && line.includes("/git/commit-activity"),
      ),
      has_ui_target_endpoint_response: summary.validation_responses.some(
        (line) => line.startsWith("200 ") && line.includes("/ui-targets"),
      ),
    };

    const failedAssertions = Object.entries(summary.assertions)
      .filter(([, passed]) => !passed)
      .map(([name]) => name);
    if (
      failedAssertions.length === 0 &&
      summary.bad_responses.length === 0 &&
      summary.request_failures.length === 0 &&
      summary.page_errors.length === 0 &&
      summary.console_errors.length === 0 &&
      summary.write_methods_sent.length === 0
    ) {
      summary.status = "passed";
    } else {
      summary.status = "failed";
    }

    expect(summary, JSON.stringify({ failed_assertions: failedAssertions, summary }, null, 2)).toMatchObject({
      status: "passed",
    });
  } finally {
    if (summary.status === "running") {
      summary.status = "failed";
    }
    summary.generated_at = new Date().toISOString();
    writeSummary(summary);
  }
});
