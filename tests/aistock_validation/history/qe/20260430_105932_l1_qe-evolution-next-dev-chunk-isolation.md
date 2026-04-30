# QE evolution Next dev chunk isolation

- Module: qe
- Level: L1
- Date: 2026-04-30T10:59:32
- Git commit: 4525a5a
- Operator: lc999

## Scope

- Changed files: `frontend/next.config.mjs`, `frontend/scripts/next-dev-isolated.mjs`, `frontend/package.json`, `frontend/playwright.config.ts`, `frontend/playwright.paper-v2.config.ts`, `frontend/tsconfig.json`, `.gitignore`
- Impacted flows: Next.js dev server for `/quantevolver/evolution`, especially when multiple dev ports such as 3000/3011/3012 are active.
- Business goal: prevent intermittent Next dev webpack chunk/runtime corruption that surfaces on the QE evolution trajectory page as `Cannot read properties of undefined (reading 'call')`, route 404s, or missing error components.
- Out of scope: backend QE scheduler/data changes, production backend restart, full browser E2E after the operator requested Codex not start frontend services.
- Protected assets reviewed: no StrategyPackage, QE/RD-Agent workspace, model weight, HMM snapshot, execution policy, selection artifact, or paper ledger path changed.

## Environment

- Backend port: not restarted; existing API endpoints were probed earlier on 8001/8011/8012 before service-start restriction.
- Frontend port: 3000 was briefly affected by Next auto-restart after config edit; Codex stopped the session and left port 3000 free for operator restart.
- TDX port: not used.
- Conda/env: Windows PowerShell, Node.js v22.20.0, frontend npm environment.
- Database: not used.
- Browser/headless: not rerun after service-start restriction; earlier Playwright probes reproduced intermittent 404/missing component behavior.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Config import smoke | Next config loads in ESM mode and resolves isolated dev dist dir | `node -e "import('./next.config.mjs').then(...)"` -> `.next-dev-3000` | PASS |
| TypeScript | Frontend typecheck passes without emitting application code | `npm exec tsc -- --noEmit` | PASS |
| L0 guardrails | No high-risk path/secret/fallback/asset finding in touched files | `scan_quality_guardrails.py ... --fail-on HIGH` -> 0 findings | PASS |
| Dev service safety | Codex is not leaving a frontend service running after user requested manual restart | `Get-NetTCPConnection -LocalPort 3000 -State Listen` -> no listener | PASS |
| UI E2E | User-visible QE evolution trajectory works with no console/page/request errors | Deferred because operator requested Codex not start frontend services | DEFERRED |

## Commands

```bash
node -e "import('./next.config.mjs').then(async m=>{const cfg=m.default('phase-development-server'); console.log(cfg.distDir);}).catch(e=>{console.error(e); process.exit(1)})"
npm exec tsc -- --noEmit
python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py .gitignore frontend/next.config.mjs frontend/package.json frontend/playwright.config.ts frontend/playwright.paper-v2.config.ts frontend/scripts/next-dev-isolated.mjs frontend/tsconfig.json --fail-on HIGH
Get-NetTCPConnection -LocalPort 3000 -State Listen -ErrorAction SilentlyContinue
```

## Evidence

- API calls: none after the operator requested manual frontend restart.
- DB checks: not applicable.
- Log files: user-provided frontend log showed alternating `GET /quantevolver/evolution 404`, `HEAD ... 200`, `GET ... 200`, then Next config reload crash due bad `next/constants` import.
- Playwright report/trace: not generated in final rerun because frontend service start was deferred to operator.
- Screenshots: none.
- Business output summary: dev builds are now isolated by port (`.next-dev-3000`, `.next-dev-3011`, `.next-dev-3012`) to prevent one dev server from serving another server's stale/missing webpack chunks.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `ERR_MODULE_NOT_FOUND: next/constants` after config auto-restart | ESM config imported `next/constants` without `.js` extension under Node.js v22 ESM resolution | Changed import to `next/constants.js` | Config import smoke passed |
| `Cannot read properties of undefined (reading 'call')` / intermittent route 404 | Multiple Next dev servers shared one `.next` cache/chunk directory, causing stale route/chunk manifests during dev rebuilds | Added isolated dev `distDir` per port and wired Playwright env to use matching cache dirs | Static config/type/guardrail checks passed; manual UI restart required for browser confirmation |

## Result

- Final status: code-side fix implemented; static validation passed; final browser confirmation pending operator restart.
- Remaining risks: full UI E2E for `/quantevolver/evolution` trajectory was not rerun because Codex was instructed not to start frontend services.
- Need production backend restart: no
- Need dev service restart: yes, frontend dev server only; operator will restart manually.
