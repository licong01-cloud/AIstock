import { spawn } from "node:child_process";
import { join } from "node:path";

const env = {
  ...process.env,
  // Avoid intermittent Windows build-worker races that leave generated server
  // chunks temporarily unavailable during page-data collection.
  NEXT_PRIVATE_BUILD_WORKER: process.env.NEXT_PRIVATE_BUILD_WORKER || "0",
};

const nextCli = join(
  process.cwd(),
  "node_modules",
  "next",
  "dist",
  "bin",
  "next",
);

const child = spawn(process.execPath, [nextCli, "build"], {
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
