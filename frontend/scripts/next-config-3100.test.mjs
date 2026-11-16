import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import test from "node:test";

const require = createRequire(import.meta.url);
const { PHASE_DEVELOPMENT_SERVER, PHASE_PRODUCTION_BUILD } = require("next/constants");
const { defaultConfig } = require("next/dist/server/config-shared");
const { getDefineEnv } = require("next/dist/build/webpack/plugins/define-env-plugin");
const { getPathMatch } = require("next/dist/shared/lib/router/utils/path-match");
const { prepareDestination } = require("next/dist/shared/lib/router/utils/prepare-destination");
const yaml = require("js-yaml");
const keys = [
  "NEXT_DEV_PORT", "FRONTEND_PORT", "PAPER_V2_FRONTEND_PORT", "PORT",
  "NEXT_DIST_DIR", "NEXT_PUBLIC_API_BASE", "NEXT_PUBLIC_TDX_BACKEND_BASE",
  "PAPER_V2_API_PROXY_TARGET",
];
let sequence = 0;

async function scenario(check, { args = ["--port", "3100"], env = {}, phase = PHASE_DEVELOPMENT_SERVER } = {}) {
  const saved = new Map(keys.map(key => [key, process.env[key]]));
  const argv = process.argv;
  try {
    for (const key of keys) delete process.env[key];
    process.env.PAPER_V2_API_PROXY_TARGET = "http://127.0.0.1:8001/api/v1";
    Object.assign(process.env, env);
    process.argv = [process.execPath, "next", "dev", ...args];
    const { default: factory } = await import(`../next.config.mjs?scenario=${++sequence}`);
    await check(() => factory(phase));
  } finally {
    process.argv = argv;
    for (const [key, value] of saved) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test("3100 uses same-origin API bases despite a shell-converted path and inherited absolute root", async () => {
  await scenario(factory => {
    const config = factory();
    assert.deepEqual(config.env, {
      NEXT_PUBLIC_API_BASE: "/api/v1", NEXT_PUBLIC_TDX_BACKEND_BASE: "/__backend",
    });
    assert.equal(config.distDir, ".next-dev-3100");
    assert.equal(process.env.NEXT_PUBLIC_API_BASE, "C:/Program Files/Git/api/v1");
    assert.equal(process.env.NEXT_PUBLIC_TDX_BACKEND_BASE, "http://127.0.0.1:8001");
    const defines = getDefineEnv({
      config: { ...defaultConfig, ...config },
      dev: true, distDir: config.distDir, hasRewrites: true,
      isClient: true, isEdgeServer: false, isNodeServer: false,
      isNodeOrEdgeCompilation: false, isTurbopack: false,
    });
    assert.equal(defines["process.env.NEXT_PUBLIC_API_BASE"], JSON.stringify("/api/v1"));
    assert.equal(defines["process.env.NEXT_PUBLIC_TDX_BACKEND_BASE"], JSON.stringify("/__backend"));
  }, { env: {
    NEXT_PUBLIC_API_BASE: "C:/Program Files/Git/api/v1",
    NEXT_PUBLIC_TDX_BACKEND_BASE: "http://127.0.0.1:8001",
  } });
});

test("real Next rewrite matching preserves the backend paths and query parameters", async () => {
  await scenario(async factory => {
    const rules = await factory().rewrites();
    for (const [path, expected] of [
      ["/api/v1/health", "/api/v1/health"],
      ["/api/v1/quantevolver/evolution/tasks/example/logs", "/api/v1/quantevolver/evolution/tasks/example/logs"],
      ["/__backend/api/ingestion/alerts/unack-count", "/api/ingestion/alerts/unack-count"],
      ["/__backend/api/ingestion/alerts/active?limit=10", "/api/ingestion/alerts/active"],
      ["/__backend/api/v1/qlib/snapshots", "/api/v1/qlib/snapshots"],
      ["/__backend/api/v1/local-data/integrity/calendar", "/api/v1/local-data/integrity/calendar"],
      ["/__backend/api/data-stats", "/api/data-stats"],
    ]) {
      const request = new URL(path, "https://remote.example");
      const query = Object.fromEntries(request.searchParams);
      const rule = rules.find(item => getPathMatch(item.source)(request.pathname));
      assert.ok(rule, path);
      const params = getPathMatch(rule.source)(request.pathname);
      const { parsedDestination } = prepareDestination({
        destination: rule.destination, params, query, appendParamsToQuery: true,
      });
      assert.equal(parsedDestination.protocol, "http:");
      assert.equal(parsedDestination.hostname, "127.0.0.1");
      assert.equal(parsedDestination.port, "8001");
      assert.equal(parsedDestination.pathname, expected);
      if (query.limit) assert.equal(parsedDestination.query.limit, query.limit);
    }
    for (const path of ["/__backend/openapi.json", "/__backend/other", "/analysis", "/api/ingestion/alerts/active"]) {
      assert.equal(rules.some(rule => Boolean(getPathMatch(rule.source)(path))), false, path);
    }
  });
});

for (const port of ["3000", "3011", "3012"]) {
  test(`${port} retains its original configuration and configured proxy target`, async () => {
    await scenario(async factory => {
      const config = factory();
      assert.equal(config.distDir, `.next-dev-${port}`);
      assert.equal(config.env, undefined);
      assert.equal(config.reactStrictMode, true);
      assert.deepEqual(await config.rewrites(), [
        { source: "/api/v1/:path*", destination: "http://127.0.0.1:8001/api/v1/:path*" },
      ]);
    }, { args: ["-p", port] });
  });
}

test("3000 with no API proxy remains unchanged", async () => {
  await scenario(async factory => {
    const config = factory();
    assert.equal(config.env, undefined);
    assert.deepEqual(await config.rewrites(), []);
  }, { args: [], env: { PAPER_V2_API_PROXY_TARGET: "" } });
});

test("production build never opts into 3100 development behavior", async () => {
  await scenario(async factory => {
    const config = factory();
    assert.equal(config.distDir, undefined);
    assert.equal(config.env, undefined);
    assert.deepEqual(await config.rewrites(), []);
  }, { phase: PHASE_PRODUCTION_BUILD, env: { PAPER_V2_API_PROXY_TARGET: "" } });
});

for (const args of [["-p", "3100"], ["--port=3100"], ["-p=3100"]]) {
  test(`3100 accepts the existing port argument form ${args.join(" ")}`, async () => {
    await scenario(factory => assert.equal(factory().env.NEXT_PUBLIC_API_BASE, "/api/v1"), { args });
  });
}

test("3100 accepts an explicit matching port and output directory", async () => {
  await scenario(factory => assert.equal(factory().distDir, ".next-dev-3100"), {
    env: { NEXT_DEV_PORT: "3100", NEXT_DIST_DIR: ".next-dev-3100" },
  });
});

test("3100 accepts the native Next PORT setting without CLI arguments", async () => {
  await scenario(factory => assert.equal(factory().env.NEXT_PUBLIC_API_BASE, "/api/v1"), {
    args: [], env: { PORT: "3100" },
  });
});

test("a build-directory hint alone cannot activate remote config on Next's default 3000 listener", async () => {
  await scenario(factory => assert.throws(factory, /explicit CLI port or PORT=3100/), {
    args: [], env: { NEXT_DEV_PORT: "3100" },
  });
});

test("3100 accepts a trailing slash in the backend target", async () => {
  await scenario(async factory => {
    assert.equal((await factory().rewrites())[0].destination, "http://127.0.0.1:8001/api/v1/:path*");
  }, { env: { PAPER_V2_API_PROXY_TARGET: "http://127.0.0.1:8001/api/v1/" } });
});

for (const target of ["", "/api/v1", "C:/Program Files/Git/api/v1", "ftp://127.0.0.1/api/v1",
  "http://127.0.0.1:8001", "http://127.0.0.1:8001/api/v1?x=1",
  "http://127.0.0.1:8001/api/v1#x", "http://user:example@127.0.0.1:8001/api/v1"]) {
  test(`3100 rejects invalid backend target case ${sequence++}`, async () => {
    await scenario(factory => assert.throws(factory, /3100.*(?:requires|proxy target)/), {
      env: { PAPER_V2_API_PROXY_TARGET: target },
    });
  });
}

for (const dir of [".next-dev-3000", ".next-dev-3011", ".next-dev-3012"]) {
  test(`3100 refuses to write another frontend output directory ${dir}`, async () => {
    await scenario(factory => assert.throws(factory, /own .next-dev-3100/), { env: { NEXT_DIST_DIR: dir } });
  });
}

for (const [port, args] of [["3000", ["-p", "3100"]], ["3100", ["-p", "3000"]]]) {
  test(`3100 rejects conflicting environment port ${port} and CLI ${args.join(" ")}`, async () => {
    await scenario(factory => assert.throws(factory, /dev port conflicts/), { args, env: { NEXT_DEV_PORT: port } });
  });
}

for (const args of [["-p", "3100", "--port=3000"], ["--port=3000", "-p", "3100"]]) {
  test(`3100 rejects ambiguous duplicate CLI ports ${args.join(" ")}`, async () => {
    await scenario(factory => assert.throws(factory, /dev port conflicts/), { args });
  });
}

test("3100 uses the configured HTTPS backend origin without a browser cross-origin request", async () => {
  await scenario(async factory => {
    const config = factory();
    assert.equal(config.env.NEXT_PUBLIC_API_BASE, "/api/v1");
    assert.deepEqual(await config.rewrites(), [
      { source: "/api/v1/:path*", destination: "https://backend.example:9443/api/v1/:path*" },
      { source: "/__backend/api/:path*", destination: "https://backend.example:9443/api/:path*" },
    ]);
  }, { env: { PAPER_V2_API_PROXY_TARGET: "https://backend.example:9443/api/v1" } });
});

test("the exact source and direct test are owned and the CI frontend session executes the test", async () => {
  const ownership = yaml.load(await readFile(new URL("../../tests/aistock_validation/catalog/file_ownership.yaml", import.meta.url), "utf8"));
  for (const path of ["frontend/next.config.mjs", "frontend/scripts/next-config-3100.test.mjs"]) {
    const rule = ownership.rules.find(item => item.rule_id === "frontend_common");
    assert.ok(rule.include.includes(path), path);
    assert.equal(rule.primary_module, "frontend_common");
  }
  const nox = await readFile(new URL("../../noxfile.py", import.meta.url), "utf8");
  const session = nox.slice(nox.indexOf("def frontend_type_lint(")).split(/\n@nox\.session/)[0];
  assert.match(session, /session\.run\("node", "--test", "scripts\/next-config-3100\.test\.mjs", external=True\)/);
});
