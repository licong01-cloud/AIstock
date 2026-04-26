import { defineConfig, devices } from "@playwright/test";

const frontendPort = Number(process.env.PAPER_V2_FRONTEND_PORT || 3011);
const apiBase = process.env.PAPER_V2_API_BASE || "http://127.0.0.1:8011/api/v1";

export default defineConfig({
  testDir: "./tests",
  timeout: 90_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,
  workers: 1,
  reporter: [["line"]],
  outputDir: "./test-results/paper-v2",
  use: {
    baseURL: `http://127.0.0.1:${frontendPort}`,
    headless: true,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "off",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: `npm run dev -- -p ${frontendPort}`,
    url: `http://127.0.0.1:${frontendPort}/paper-v2`,
    reuseExistingServer: true,
    timeout: 120_000,
    env: {
      NEXT_PUBLIC_API_BASE: "/api/v1",
      PAPER_V2_API_PROXY_TARGET: apiBase,
    },
  },
});
