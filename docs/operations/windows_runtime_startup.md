# Windows one-click startup

Run `F:\Dev\AIstock\start_all_ai_stock.bat` after closing the actual old service
windows. Closing only the parent PowerShell does not guarantee child services stop.
The launcher never kills an existing process, reloads Python, installs dependencies,
changes firewall rules, or invokes a database migration.

## Behaviour

- Conda activation and the absolute AIstock Python are checked before dispatch.
- All three services inherit the original invoking console, in Windows Terminal
  or legacy PowerShell alike. No new tab/window is opened and no manual window ID
  is required. `WT_SESSION`, `WT_WINDOW_ID`, foreground focus and MRU are not used
  for routing: switching to Codex cannot redirect startup there. The absolute
  Python supervisors run without additional shell profiles. Live output has
  `[tdx]`, `[backend]` or `[frontend]` prefixes; separate full logs are preserved.
- The supervisor and Python service stdout/stderr explicitly use UTF-8, including
  redirected logs; a GBK parent locale must not crash Unicode startup messages.
- Ports 19080, 8001 and 3000 are checked with their process ancestry. An unknown
  listener blocks startup. DEV TDX ports 19081/19082 are not managed.
- An old startup/shutdown process or monthly worker is allowed up to 120 seconds
  to settle. A second launcher is rejected while the OS startup lock is held.
- Each service has up to 120 seconds for readiness, checked concurrently rather
  than in three serial waits. The original console shows prerequisite checks,
  dispatch and five-second readiness progress, including per-service failures.
  Backend health and its frozen
  source identity must match the clean checkout. TDX health requires metadata-ready
  and production identity; a newly started TDX must match its verified build receipt,
  not an unrelated repository HEAD. Frontend stays on port 3000, not a fallback port.
- All absent services are dispatched in the original console before any service
  readiness wait. Backend startup does not wait for TDX; each service initializes
  independently. A shared process/port snapshot precedes dispatch instead of
  repeated inventories between services. Services with old processes still settling
  are handled after the other services have been dispatched. Service-specific
  prerequisites (such as frontend dependencies) are checked in that service's
  child; a missing prerequisite must not suppress the other services' dispatch.
- Closed-port polling uses a cheap loopback connection, not
  a new PowerShell/CIM inventory on every poll. Before accepting readiness, the
  listener's PID/ancestry and service health/identity must still be verified.
- Startup validates the cached TDX build and runs the fixed EXE directly. A valid
  cache never invokes the compiler. Only first use or a genuine tracked TDX build
  input/configuration change triggers one offline build using existing dependencies
  in the TDX child, not the shared parent; backend/frontend do not wait for it.
  It never uses `go run` or installs dependencies. The verified actual build revision
  is recorded in `tdx-executable.json`, not relabeled with an unrelated current HEAD.
  Build failure never falls back to a stale EXE. An explicit build-only preparation
  remains available and does not start TDX:

  ```powershell
  & C:\Users\lc999\miniconda3\envs\AIstock\python.exe F:\Dev\AIstock\scripts\start_tdx_go_backend.py --database-target production --env-file F:\Dev\AIstock\.env --build-only
  ```

  For an invalid/incomplete receipt, use that command with `--rebuild` only after
  closing the actual TDX service. Corrupt/incomplete receipts are not rebuilt
  automatically. Build publication uses the existing build lock and checked
  candidate EXE; a running EXE is never overwritten in place.
- Existing healthy instances are not relaunched. A different backend revision is
  reported as old and is not silently accepted as an updated backend.
- Failures are displayed immediately and preserved in logs under
  `F:\Dev\AIstock_runtime\startup\<start-id>`. Successful startup writes a compact
  `summary.json`, including per-service errors on failure. A service failure does
  not suppress dispatch/readiness of the others. There is no automatic kill,
  retry loop or rollback of other services.
- The launcher keeps the original console occupied while its own children are
  running, including after a readiness failure, so PowerShell does not print a
  command prompt into live service logs. The final exit code is nonzero on startup
  or child failure. Existing healthy processes are not adopted or controlled.

## Read-only check and runtime verification

`start_all_ai_stock.bat --check` inspects prerequisites, process ownership and ports;
it does not build, create logs/locks, or start services. It is not a health or business
acceptance receipt. A task worktree without existing frontend dependencies fails
explicitly; never install dependencies merely to make a read-only check pass.

After source merge, the user owns the next normal startup. For `backend-main`,
verify `/api/v1/health`, the frozen `/api/v1/runtime-identity` merge identity and
`/openapi.json` using the existing `openapi_document` semantic contract. This
startup-only smoke proves that the real application route schema is served; it
does not claim any QE, Advisory or trading outcome. TDX has a separate target and
runbook in `docs/operations/tdx_go_runtime_runbook.md`.

Network, database, dependency and OS failures cannot be made impossible by a
launcher. A timeout is an actionable failure, not a promise that all child processes
have exited. Inspect the named console/log; do not repeatedly launch copies.
