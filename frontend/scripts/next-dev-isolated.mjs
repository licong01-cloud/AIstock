import { spawn } from "node:child_process";
import { join } from "node:path";

function parsePort(args) {
  const envPort =
    process.env.NEXT_DEV_PORT ||
    process.env.FRONTEND_PORT ||
    process.env.PAPER_V2_FRONTEND_PORT ||
    process.env.PORT;

  if (envPort) return envPort;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if ((arg === "-p" || arg === "--port") && args[i + 1]) return args[i + 1];
    if (arg.startsWith("--port=")) return arg.slice("--port=".length);
    if (arg.startsWith("-p=")) return arg.slice("-p=".length);
  }

  return "3000";
}

const args = process.argv.slice(2);
const port = parsePort(args);
const normalizedPort = /^\d+$/.test(port) ? port : "3000";
const distDir = process.env.NEXT_DIST_DIR || `.next-dev-${normalizedPort}`;

const env = {
  ...process.env,
  NEXT_DEV_PORT: normalizedPort,
  NEXT_DIST_DIR: distDir,
};

const nextCli = join(process.cwd(), "node_modules", "next", "dist", "bin", "next");
const child = spawn(process.execPath, [nextCli, "dev", ...args], {
  env,
  stdio: "inherit",
  shell: false,
});

child.on("exit", (code) => {
  process.exit(code ?? 1);
});

child.on("error", (error) => {
  console.error(error);
  process.exit(1);
});
