import { expect, test } from "@playwright/test";

async function mockResearchAssistantApi(page: import("@playwright/test").Page) {
  await page.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace(/^\/api\/v1/, "");
    let data: unknown = {};

    if (path === "/research-assistant/health") {
      data = {
        status: "ok",
        repository: { source: "playwright" },
        runtime_boundaries: { mode: "test" },
      };
    } else if (path === "/research-assistant/mcp/servers") {
      data = {
        items: [{ server_key: "aistock-qe", title: "QE Gateway", status: "ready" }],
        total: 1,
        page: 1,
        page_size: 50,
        has_more: false,
      };
    } else if (path === "/research-assistant/mcp/tools") {
      data = {
        items: [{
          tool_id: "tool-1",
          server_key: "aistock-qe",
          tool_name: "qe_archive_query_run_leaderboard",
          title: "QE Leaderboard",
          description: "Read QE leaderboard",
          risk_level: "read_only",
          manifest_risk_level: "read_only",
          profile: "research_assistant",
        }],
        total: 1,
        page: 1,
        page_size: 50,
        has_more: false,
        server_count: 1,
        risk_distribution: { read_only: 1 },
        profile_distribution: { research_assistant: 1 },
      };
    } else if (path === "/research-assistant/mcp/tool-events") {
      data = { items: [], total: 0, page: 1, page_size: 20, has_more: false };
    } else if (path === "/research-assistant/skills") {
      data = {
        items: [{ skill_key: "qe.diagnostics", title: "QE 诊断", status: "approved", risk_level: "low" }],
        total: 1,
        page: 1,
        page_size: 50,
        has_more: false,
      };
    } else if (path === "/research-assistant/skills/usage-events") {
      data = { items: [], total: 0, page: 1, page_size: 50, has_more: false };
    } else if (path === "/research-assistant/models/profiles") {
      data = {
        items: [{ model_profile_id: "profile-main", provider: "deepseek", model_name: "deepseek-chat", role: "primary", status: "ready" }],
        total: 1,
        page: 1,
        page_size: 50,
        has_more: false,
      };
    } else if (path === "/research-assistant/models/routing-policies") {
      data = {
        items: [{ policy_id: "policy-low", role: "cheap_worker", risk_level: "low", status: "ready" }],
        total: 1,
        page: 1,
        page_size: 50,
        has_more: false,
      };
    }

    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({ status: "ok", data }),
    });
  });
}

test.describe("Research Assistant settings consolidation", () => {
  test.beforeEach(async ({ page }) => {
    await mockResearchAssistantApi(page);
  });

  test("renders four settings tabs and keeps top nav compact", async ({ page }) => {
    await page.goto("/research-assistant/settings");

    const topNav = page.locator('header nav[aria-label="研究助理功能导航"]');
    await expect(topNav.getByRole("link", { name: "设置" })).toBeVisible();
    await expect(topNav.getByRole("link", { name: "MCP 工具" })).toHaveCount(0);
    await expect(topNav.getByRole("link", { name: "Skills" })).toHaveCount(0);
    await expect(topNav.getByRole("link", { name: "模型路由" })).toHaveCount(0);

    const settingsTabs = page.locator('nav[aria-label="研究助理设置标签"]');
    await expect(settingsTabs.getByRole("link", { name: "MCP目录" })).toBeVisible();
    await expect(settingsTabs.getByRole("link", { name: "技能" })).toBeVisible();
    await expect(settingsTabs.getByRole("link", { name: "模型路由" })).toBeVisible();
    await expect(settingsTabs.getByRole("link", { name: "健康" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Research Assistant consumes the unified MCP manifest catalog" })).toBeVisible();
  });

  test("deep-links each consolidated section", async ({ page }) => {
    await page.goto("/research-assistant/settings?tab=skills");
    await expect(page.getByRole("heading", { name: "本地 Skill Catalog" })).toBeVisible();

    await page.goto("/research-assistant/settings?tab=models");
    await expect(page.getByRole("heading", { name: "模型配置与路由" })).toBeVisible();

    await page.goto("/research-assistant/settings?tab=health");
    await expect(page.getByRole("heading", { name: "研究助理设置" }).first()).toBeVisible();
    await expect(page.getByText("API Base")).toBeVisible();
  });

  test("legacy config routes redirect to settings tabs instead of 404", async ({ page }) => {
    await page.goto("/research-assistant/mcp-tools");
    await expect(page).toHaveURL(/\/research-assistant\/settings\?tab=mcp$/);
    await expect(page.getByRole("heading", { name: "Research Assistant consumes the unified MCP manifest catalog" })).toBeVisible();

    await page.goto("/research-assistant/skills");
    await expect(page).toHaveURL(/\/research-assistant\/settings\?tab=skills$/);
    await expect(page.getByRole("heading", { name: "本地 Skill Catalog" })).toBeVisible();

    await page.goto("/research-assistant/models");
    await expect(page).toHaveURL(/\/research-assistant\/settings\?tab=models$/);
    await expect(page.getByRole("heading", { name: "模型配置与路由" })).toBeVisible();
  });
});
