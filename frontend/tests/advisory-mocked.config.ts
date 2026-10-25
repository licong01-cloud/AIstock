import { defineConfig, devices } from "@playwright/test";
import { copyFileSync, cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, realpathSync, symlinkSync } from "node:fs";
import path from "node:path";

const frontend = path.resolve(__dirname, "..");

// Fail before any browser/frontend startup, never use OS/C-drive fallback.
const temp = realpathSync(process.env.TEMP || "X:/AIstock-CI/missing-temp");
if (path.parse(temp).root.toUpperCase() !== "X:\\") {
  throw new Error("Advisory mocked UI requires preconfigured writable X-only TEMP");
}
const run = mkdtempSync(path.join(temp, "advisory-mocked-"));
const app = path.join(run, "frontend");
mkdirSync(app);
// Next and source must resolve on X to avoid cross-drive Webpack entry errors.
// Copy existing Next bytes (no installation); other prebuilt dependencies are
// read-only junctions. Never copy the complete multi-GB node_modules tree.
if (!process.argv.includes("--list")) {
  cpSync(path.join(frontend, "src"), path.join(app, "src"), { recursive: true });
  mkdirSync(path.join(app, "node_modules"));
  for (const entry of readdirSync(path.join(frontend, "node_modules"), { withFileTypes: true })) {
    const source = path.join(frontend, "node_modules", entry.name);
    const target = path.join(app, "node_modules", entry.name);
    if (entry.name === "next") cpSync(source, target, { recursive: true });
    else if (entry.isDirectory() || entry.isSymbolicLink()) symlinkSync(source, target, "junction");
  }
  if (existsSync(path.join(frontend, "public"))) symlinkSync(path.join(frontend, "public"), path.join(app, "public"), "junction");
  for (const name of ["package.json", "tsconfig.json", "next.config.mjs", "next-env.d.ts",
    "postcss.config.js", "postcss.config.mjs", "tailwind.config.ts", "tailwind.config.js"]) {
    const source = path.join(frontend, name);
    if (existsSync(source)) copyFileSync(source, path.join(app, name));
  }
}
const env = {
  TEMP: run, TMP: run, TMPDIR: run,
  NEXT_DIST_DIR: ".next", NEXT_DEV_PORT: "3012", FRONTEND_PORT: "3012",
  NEXT_PUBLIC_API_BASE: "http://127.0.0.1:8012/api/v1",
  NEXT_PUBLIC_TDX_BACKEND_BASE: "http://127.0.0.1:8012",
  PAPER_V2_API_PROXY_TARGET: "http://127.0.0.1:8012/api/v1",
  NEXT_TELEMETRY_DISABLED: "1",
};
// Playwright browser profiles inherit the runner's X-only storage too.
Object.assign(process.env, { TEMP: run, TMP: run, TMPDIR: run });

export default defineConfig({
  testDir: path.join(frontend, "tests/paper-v2"),
  testMatch: "paper-v2-advisory-ui.spec.ts",
  timeout: 60_000,
  expect: { timeout: 20_000 },
  workers: 1, fullyParallel: false,
  reporter: [["list"], ["json", { outputFile: path.join(run, "results.json") }]],
  outputDir: path.join(run, "test-results"),
  use: { baseURL: "http://127.0.0.1:3012", headless: true, trace: "retain-on-failure" },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: `node ${JSON.stringify(path.join(app, "node_modules/next/dist/bin/next"))} dev --hostname 127.0.0.1 -p 3012`,
    cwd: app, env, url: "http://127.0.0.1:3012/paper-v2/advisory",
    reuseExistingServer: false, timeout: 120_000,
  },
});
