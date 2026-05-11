import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

const PACKAGE_FIXTURES = [
  {
    package_id: "pkg_demo_ready",
    package_name: "Demo Ready Package",
    status: "active",
    paper_status: "disabled",
    selection_status: "enabled",
    source_system: "qe",
    source_id: "qe_task_demo_ready",
    created_at: "2026-05-01T10:00:00",
    updated_at: "2026-05-08T12:00:00",
  },
  {
    package_id: "pkg_demo_blocked",
    package_name: "Demo Blocked Package",
    status: "active",
    paper_status: "disabled",
    selection_status: "draft",
    source_system: "qe",
    source_id: "qe_task_demo_blocked",
    created_at: "2026-05-02T11:00:00",
    updated_at: "2026-05-08T13:00:00",
  },
  {
    package_id: "pkg_demo_retired",
    package_name: "Retired Package",
    status: "retired",
    paper_status: "disabled",
    source_system: "qe",
    source_id: "qe_task_demo_retired",
    created_at: "2026-04-01T09:00:00",
    updated_at: "2026-04-15T10:00:00",
  },
];

const READY_ELIGIBILITY = {
  package_id: "pkg_demo_ready",
  paper_ready: true,
  paper_ready_block_reason: null,
  evaluated_at: "2026-05-09T08:00:00",
  manifest_identity: { status: "pass", reason: null },
  original_fixed_weight_retest: { status: "pass", reason: null },
  validation_stability: { status: "pass", reason: null },
  protected_asset_ledger: { status: "pass", reason: null },
  runtime_variant_paper_candidate: { status: "pass", reason: null },
};

const BLOCKED_ELIGIBILITY = {
  package_id: "pkg_demo_blocked",
  paper_ready: false,
  paper_ready_block_reason: "validation_stability=INSUFFICIENT_EVIDENCE",
  evaluated_at: "2026-05-09T08:05:00",
  manifest_identity: { status: "pass", reason: null },
  original_fixed_weight_retest: { status: "pass", reason: null },
  validation_stability: { status: "missing", reason: "INSUFFICIENT_EVIDENCE" },
  protected_asset_ledger: { status: "fail", reason: "ledger_missing" },
  runtime_variant_paper_candidate: { status: "pending", reason: null },
};

type MockOptions = {
  packages?: typeof PACKAGE_FIXTURES;
  packagesStatus?: number;
  eligibilityByPackage?: Record<string, unknown>;
  eligibilityStatus?: number;
  enableResult?: { ok: boolean; package?: unknown };
  enableStatus?: number;
  enableSpy?: { count: number };
};

async function mockApi(page: Page, options: MockOptions = {}) {
  await page.route("**/api/v1/strategy-packages**", async (route) => {
    const url = route.request().url();
    const method = route.request().method();
    const respond = (data: unknown, status = 200) =>
      route.fulfill({ status, contentType: "application/json", body: JSON.stringify(data) });

    if (method === "POST" && url.includes("/enable-paper")) {
      if (options.enableSpy) options.enableSpy.count += 1;
      if (options.enableStatus && options.enableStatus >= 400) {
        return respond({ detail: "enable_paper_blocked" }, options.enableStatus);
      }
      return respond(options.enableResult ?? { ok: true });
    }

    if (url.includes("/governance-eligibility")) {
      if (options.eligibilityStatus && options.eligibilityStatus >= 400) {
        return respond({ detail: "eligibility_unavailable" }, options.eligibilityStatus);
      }
      const packageId = decodeURIComponent(url.split("/strategy-packages/")[1]?.split("/")[0] ?? "");
      const data = options.eligibilityByPackage?.[packageId];
      if (!data) return respond({ detail: "eligibility_not_found" }, 404);
      return respond(data);
    }

    // List packages
    if (options.packagesStatus && options.packagesStatus >= 400) {
      return respond({ detail: "packages_unavailable" }, options.packagesStatus);
    }
    return respond(options.packages ?? PACKAGE_FIXTURES);
  });
}

test("governance page lists strategy packages and selects first ready package by default", async ({ page }) => {
  await mockApi(page, {
    eligibilityByPackage: {
      pkg_demo_ready: READY_ELIGIBILITY,
      pkg_demo_blocked: BLOCKED_ELIGIBILITY,
    },
  });
  await page.goto("/strategy-package-governance");
  await expect(page.getByText("策略包治理")).toBeVisible();
  await expect(page.getByText("pkg_demo_ready")).toBeVisible();
  await expect(page.getByText("Demo Ready Package")).toBeVisible();
  await expect(page.getByTestId("paper-ready-summary")).toContainText("READY");
});

test("status filter narrows the visible package list", async ({ page }) => {
  await mockApi(page, {
    eligibilityByPackage: {
      pkg_demo_ready: READY_ELIGIBILITY,
      pkg_demo_blocked: BLOCKED_ELIGIBILITY,
    },
  });
  await page.goto("/strategy-package-governance");
  await expect(page.getByText("pkg_demo_retired")).toBeVisible();
  await page.getByTestId("status-filter").selectOption("retired");
  await expect(page.getByText("pkg_demo_retired")).toBeVisible();
  await expect(page.getByText("pkg_demo_ready")).not.toBeVisible();
  await page.getByTestId("status-filter").selectOption("all");
  await expect(page.getByText("pkg_demo_ready")).toBeVisible();
});

test("blocked package shows missing evidence and disables enable_paper", async ({ page }) => {
  await mockApi(page, {
    eligibilityByPackage: {
      pkg_demo_ready: READY_ELIGIBILITY,
      pkg_demo_blocked: BLOCKED_ELIGIBILITY,
    },
  });
  await page.goto("/strategy-package-governance");
  await page.getByTestId("select-pkg_demo_blocked").click();
  await expect(page.getByTestId("paper-ready-summary")).toContainText("NOT_READY");
  await expect(page.getByTestId("block-reason")).toContainText("validation_stability=INSUFFICIENT_EVIDENCE");
  await expect(page.getByTestId("evidence-validation_stability-reason")).toContainText("INSUFFICIENT_EVIDENCE");
  await expect(page.getByTestId("evidence-protected_asset_ledger-reason")).toContainText("ledger_missing");
  // Enable button is rendered but disabled because paper_ready=false
  const enableButton = page.getByRole("button", { name: /启用 Paper/ });
  await expect(enableButton).toBeDisabled();
});

test("enable_paper requires the two-step confirm and posts to the API", async ({ page }) => {
  const spy = { count: 0 };
  await mockApi(page, {
    eligibilityByPackage: { pkg_demo_ready: READY_ELIGIBILITY },
    packages: [PACKAGE_FIXTURES[0]],
    enableSpy: spy,
  });
  await page.goto("/strategy-package-governance");
  await expect(page.getByTestId("paper-ready-summary")).toContainText("READY");
  // Open the confirm box via the action button
  await page.getByTestId("enable-paper-action").click();
  // Type confirmation token
  await page.getByTestId("enable-paper-action-input").fill("ENABLE_PAPER_CONFIRM");
  await page.getByTestId("enable-paper-action-confirm").click();
  await expect.poll(() => spy.count).toBe(1);
  await expect(page.getByTestId("enable-message")).toContainText("Paper 已启用");
});

test("packages API failure surfaces the error panel without crashing", async ({ page }) => {
  const pageErrors: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  await mockApi(page, { packagesStatus: 500 });
  await page.goto("/strategy-package-governance");
  await expect(page.getByText("治理 API 调用失败")).toBeVisible();
  await expect(page.getByText("策略包治理")).toBeVisible();
  expect(pageErrors).toEqual([]);
});
