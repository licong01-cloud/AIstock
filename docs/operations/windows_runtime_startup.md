# Windows one-click startup

Run `F:\Dev\AIstock\start_all_ai_stock.bat` after closing the actual old service
windows. Closing only the parent PowerShell does not guarantee child services stop.
The launcher never kills an existing process, reloads Python, installs dependencies,
changes firewall rules, or invokes a database migration.

## Behaviour

- Run the BAT entry from the intended, active Windows Terminal window. One
  immediate `wt.exe -w 0` request creates three separate tabs: `AIstock TDX`,
  `AIstock Backend`, and `AIstock Frontend`. Tab dispatch precedes Conda activation
  and process inventory; those checks run independently inside each service tab.
  No permanent window ID or manual configuration is needed. `-w 0` means the
  most recently active Terminal window, not a guaranteed caller-window binding:
  keep the intended window active during the immediate request. The launcher
  does not reroute tabs after slow setup. No shared-console fallback is attempted
  if Terminal is unavailable or dispatch fails.
- All tabs use the absolute AIstock Python for orchestration; only the backend
  activates AIstock. TDX runs its fixed Go EXE and frontend runs Node, so neither
  needs Conda activation. Read-only `--check` also skips activation. The backend
  checks the Conda entry and activates with its normal hooks/DLL configuration.
  Each activation gets a UUID-named, atomically created private directory under
  `AIstock_runtime/startup/conda/`; TEMP, TMP and TMPDIR all point there only during
  activation, then are restored even on failure. This isolates Conda's
  RANDOM-based temporary filenames from other CMD processes. Only the empty owned
  directory is removed; nonempty failure artifacts are retained with a warning.
  CMD `/k` preserves the tab and error output after exit. Supervisors and their
  service processes remain attached to their own tabs, with no extra shell profile.
  Live output has service prefixes; separate full logs are preserved.
- The supervisor and Python service stdout/stderr explicitly use UTF-8, including
  redirected logs; a GBK parent locale must not crash Unicode startup messages.
- Ports 19080, 8001 and 3000 are checked with their process ancestry. An unknown
  listener blocks startup. DEV TDX ports 19081/19082 are not managed.
- An old startup/shutdown process or monthly worker is allowed up to 120 seconds
  to settle. Per-service OS locks reject duplicate starts without making TDX,
  backend and frontend wait for each other's initialization.
- Each service has up to 120 seconds for readiness, checked concurrently rather
  than in three serial waits. Each service tab shows prerequisite checks,
  dispatch and five-second readiness progress, including per-service failures.
  Backend health and its frozen
  source identity must match the clean checkout. TDX health requires metadata-ready
  and production identity; a newly started TDX must match its verified build receipt,
  not an unrelated repository HEAD. Frontend stays on port 3000, not a fallback port.
- All three tabs are requested together before any service readiness wait.
  Backend startup does not wait for TDX; each service initializes independently.
  Each tab takes one process/port snapshot and checks only its assigned service;
  another service's slow shutdown or unknown listener cannot suppress its dispatch.
  Service-specific
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
- Each service supervisor keeps its own tab occupied while its child is running,
  including after a readiness failure. The original invoking tab returns after
  tab dispatch, not after application readiness. A successful dispatch is not a
  health receipt: check all three tabs/logs for actual startup results. Each service
  tab reports nonzero startup/child failures. Existing healthy processes are not
  adopted or controlled. Close the actual old service tabs manually before restart;
  closing only the original invoking tab does not stop services in the other tabs.

## Read-only check and runtime verification

### Start only the backend in a new tab

Run `.\start_backend.bat` in the Windows Terminal window where the new
`AIstock Backend` tab should appear. Close the old backend tab manually first.
The entry requests one tab in the most recently active Terminal window (`-w 0`);
it does not configure a permanent window ID or guarantee routing if another
Terminal window becomes active during dispatch. It does not alter the all-service
launcher, start TDX/frontend, stop any process or use reload. It activates AIstock
and uses its absolute UTF-8 Python. An occupied/unavailable 8001 port fails visibly
without stopping its owner. The tab remains open after backend exit. Dispatch
success only means the tab was requested, not that the API is healthy.

`start_backend.bat --check` checks the existing interpreter/Conda entry only;
it does not open a tab, activate Conda, bind a port or start a service.

`start_backend.bat --check-env` and `start_all_ai_stock.bat --check-env` perform
real isolated Conda activation and verify interpreter/environment prefix agreement,
without opening tabs, binding ports or starting applications. These are diagnostic
commands, not additional mandatory startup gates. The backend-only entry uses the
same private activation-temp policy as the all-service backend tab.

### All-service check and runtime verification

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
