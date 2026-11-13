import { defineConfig } from "@playwright/test";
import path from "node:path";
const isolated = process.platform === "win32"
  ? require("./advisory-mocked.config").default
  : require("../playwright.config").default;

// Reuse the approved X-only, prebuilt-dependency, runner-owned frontend.
// No production backend, dependency installation, training or business writes.
export default defineConfig({
  ...isolated,
  testDir: path.resolve(__dirname, "local-data"),
  testMatch: "daily-integrity-calendar.spec.ts",
  webServer: { ...isolated.webServer, url: "http://127.0.0.1:3012/local-data" },
});
