import { PHASE_DEVELOPMENT_SERVER } from "next/constants.js";

/** @type {import('next').NextConfig} */
const paperV2ApiProxyTarget = (process.env.PAPER_V2_API_PROXY_TARGET || "").replace(/\/$/, "");

function parseDevPort(argv) {
  const envPort =
    process.env.NEXT_DEV_PORT ||
    process.env.FRONTEND_PORT ||
    process.env.PAPER_V2_FRONTEND_PORT ||
    process.env.PORT;

  if (envPort) return envPort;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if ((arg === "-p" || arg === "--port") && argv[i + 1]) return argv[i + 1];
    if (arg.startsWith("--port=")) return arg.slice("--port=".length);
    if (arg.startsWith("-p=")) return arg.slice("-p=".length);
  }

  return "3000";
}

function resolveDevDistDir() {
  if (process.env.NEXT_DIST_DIR) return process.env.NEXT_DIST_DIR;
  const port = parseDevPort(process.argv);
  const normalizedPort = /^\d+$/.test(port) ? port : "3000";
  return `.next-dev-${normalizedPort}`;
}

const sharedConfig = {
  reactStrictMode: true,
  async rewrites() {
    if (!paperV2ApiProxyTarget) return [];
    return [
      {
        source: "/api/v1/:path*",
        destination: `${paperV2ApiProxyTarget}/:path*`,
      },
    ];
  },
};

export default function nextConfig(phase) {
  const isDevServer = phase === PHASE_DEVELOPMENT_SERVER;
  return {
    ...sharedConfig,
    ...(isDevServer ? { distDir: resolveDevDistDir() } : {}),
  };
}
