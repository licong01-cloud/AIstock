import { PHASE_DEVELOPMENT_SERVER } from "next/constants.js";

/** @type {import('next').NextConfig} */
const paperV2ApiProxyTarget = (process.env.PAPER_V2_API_PROXY_TARGET || "").replace(/\/$/, "");

function parseArgPort(argv) {
  const ports = [];
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if ((arg === "-p" || arg === "--port") && argv[i + 1]) ports.push(argv[++i]);
    else if (arg.startsWith("--port=")) ports.push(arg.slice("--port=".length));
    else if (arg.startsWith("-p=")) ports.push(arg.slice("-p=".length));
  }
  if (ports.includes("3100") && new Set(ports).size > 1) {
    throw new Error("3100 dev port conflicts with another CLI port argument.");
  }
  return ports[0];
}

function parseDevPort(argv) {
  const envPort =
    process.env.NEXT_DEV_PORT ||
    process.env.FRONTEND_PORT ||
    process.env.PAPER_V2_FRONTEND_PORT ||
    process.env.PORT;

  if (envPort) return envPort;

  return parseArgPort(argv) || "3000";
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
  const port = isDevServer ? parseDevPort(process.argv) : undefined;
  const argPort = isDevServer ? parseArgPort(process.argv) : undefined;
  const remoteDev = isDevServer && port === "3100";
  if (isDevServer && (remoteDev || argPort === "3100") && argPort && argPort !== port) {
    throw new Error("3100 dev port conflicts with the configured development port.");
  }

  let remoteConfig = {};
  if (remoteDev) {
    if (!argPort && process.env.PORT !== "3100") {
      throw new Error("3100 requires an explicit CLI port or PORT=3100.");
    }
    if (resolveDevDistDir() !== ".next-dev-3100") {
      throw new Error("3100 must use its own .next-dev-3100 output directory.");
    }
    let target;
    try {
      target = new URL(paperV2ApiProxyTarget);
    } catch {
      throw new Error("3100 requires PAPER_V2_API_PROXY_TARGET as an absolute HTTP(S) URL ending in /api/v1.");
    }
    if (
      !["http:", "https:"].includes(target.protocol) ||
      target.username || target.password || target.search || target.hash ||
      target.pathname !== "/api/v1"
    ) {
      throw new Error("3100 proxy target must be HTTP(S), end in /api/v1, and contain no credentials, query or fragment.");
    }
    remoteConfig = {
      // Resolve after shell path conversion; never inline a Windows path or an
      // inherited absolute browser backend address into the remote frontend.
      env: {
        NEXT_PUBLIC_API_BASE: "/api/v1",
        NEXT_PUBLIC_TDX_BACKEND_BASE: "/__backend",
      },
      async rewrites() {
        return [
          { source: "/api/v1/:path*", destination: `${target.origin}/api/v1/:path*` },
          { source: "/__backend/api/:path*", destination: `${target.origin}/api/:path*` },
        ];
      },
    };
  }
  return {
    ...sharedConfig,
    ...(isDevServer ? { distDir: resolveDevDistDir() } : {}),
    ...remoteConfig,
  };
}
