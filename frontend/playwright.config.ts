import { defineConfig, devices } from "@playwright/test";

const backendPort = process.env.BACKEND_PORT || "8012";
const frontendPort =
  process.env.FRONTEND_PORT || process.env.PAPER_V2_FRONTEND_PORT || "3012";
const apiBase =
  process.env.API_BASE ||
  process.env.NEXT_PUBLIC_API_BASE ||
  process.env.PAPER_V2_API_BASE ||
  `http://127.0.0.1:${backendPort}/api/v1`;

export default defineConfig({
  testDir: ".",
  testMatch: ["e2e/**/*.spec.ts", "tests/**/*.spec.ts"],
  timeout: 30 * 60 * 1000,
  expect: { timeout: 30_000 },
  fullyParallel: false,
  workers: 1,
  reporter: [["list"], ["html", { outputFolder: "../tmp/playwright-report", open: "never" }]],
  outputDir: "../tmp/playwright-results",
  use: {
    baseURL: process.env.FRONTEND_BASE_URL || `http://127.0.0.1:${frontendPort}`,
    headless: true,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: `npm run dev -- -p ${frontendPort}`,
    url: process.env.FRONTEND_BASE_URL || `http://127.0.0.1:${frontendPort}/quantevolver/compose`,
    reuseExistingServer: true,
    timeout: 120_000,
    env: {
      NEXT_PUBLIC_API_BASE: apiBase,
      PAPER_V2_API_BASE: apiBase,
      PAPER_V2_API_PROXY_TARGET: apiBase,
    },
  },
});
