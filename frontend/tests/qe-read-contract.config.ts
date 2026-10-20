import { defineConfig, devices } from "@playwright/test";
import { copyFileSync, cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, realpathSync, symlinkSync } from "node:fs";
import path from "node:path";

const frontend = path.resolve(__dirname, "..");
// Fail before startup; never fall back to the system/C-drive temporary directory.
const temp = realpathSync(process.env.TEMP || "X:/AIstock-CI/missing-temp");
if (path.parse(temp).root.toUpperCase() !== "X:\\") {
  throw new Error("QE read contracts require preconfigured writable X-only TEMP");
}
const run = mkdtempSync(path.join(temp, "qe-read-contract-"));
Object.assign(process.env, { TEMP: run, TMP: run, TMPDIR: run });
const targets = process.argv.filter(arg => arg.endsWith(".spec.ts"));
const needsFrontend = !targets.length || targets.some(arg => arg.endsWith("qe-evolution-read-only.spec.ts"));
const app = path.join(run, "frontend");
// Source-only and about:blank harness tests need no frontend server at all.
if (needsFrontend && !process.argv.includes("--list")) {
  mkdirSync(app);
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
    if (existsSync(path.join(frontend, name))) copyFileSync(path.join(frontend, name), path.join(app, name));
  }
}
const env = {
  TEMP: run, TMP: run, TMPDIR: run,
  NEXT_DIST_DIR: ".next", NEXT_DEV_PORT: "3012", FRONTEND_PORT: "3012",
  NEXT_PUBLIC_API_BASE: "http://127.0.0.1:8012/api/v1",
  NEXT_PUBLIC_TDX_BACKEND_BASE: "http://127.0.0.1:8012",
  PAPER_V2_API_PROXY_TARGET: "http://127.0.0.1:8012/api/v1", NEXT_TELEMETRY_DISABLED: "1",
};

export default defineConfig({
  testDir: path.join(frontend, "tests/qe"),
  // The mocked test has an unchanged 65-second no-poll observation.
  timeout: 150_000, expect: { timeout: 20_000 }, workers: 1, fullyParallel: false,
  reporter: [["list"], ["json", { outputFile: path.join(run, "results.json") }]],
  outputDir: path.join(run, "test-results"),
  use: { baseURL: "http://127.0.0.1:3012", headless: true, trace: "retain-on-failure" },
  projects: [
    { name: "source-and-harness", testMatch: ["qe-evolution-best-loop-ui.spec.ts", "qe-read-performance.spec.ts"],
      use: { ...devices["Desktop Chrome"] } },
    // Positive selection: the real-backend task-detail test remains delegated to qe_read_ui.
    { name: "terminal-mocked", testMatch: "qe-evolution-read-only.spec.ts",
      grep: /QE dashboard stops automatic polling when task list has no active task$/,
      use: { ...devices["Desktop Chrome"] } },
  ],
  webServer: needsFrontend ? {
    command: `node ${JSON.stringify(path.join(app, "node_modules/next/dist/bin/next"))} dev --hostname 127.0.0.1 -p 3012`,
    cwd: app, env, url: "http://127.0.0.1:3012/quantevolver/evolution",
    reuseExistingServer: false, timeout: 120_000,
  } : undefined,
});
