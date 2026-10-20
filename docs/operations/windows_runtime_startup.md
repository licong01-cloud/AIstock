# Windows one-click startup

Run `F:\Dev\AIstock\start_all_ai_stock.bat` after closing the actual old service
windows. Closing only the parent PowerShell does not guarantee child services stop.
The launcher never kills an existing process, reloads Python, installs dependencies,
changes firewall rules, or invokes a database migration.

## Behaviour

- Conda activation and the absolute AIstock Python are checked before dispatch.
- When invoked from PowerShell inside Windows Terminal (`WT_SESSION` is set and
  `wt.exe` is available), services open as named tabs using `wt -w 0 new-tab`.
  `0` selects the most recently used Terminal window: keep the invoking window
  active while dispatching. An explicit `WT_WINDOW_ID` overrides that selection.
  A legacy PowerShell console has no tabs and uses separate PowerShell windows.
  Both hosts execute the same absolute Python supervisor, without shell profiles.
- The supervisor and Python service stdout/stderr explicitly use UTF-8, including
  redirected logs; a GBK parent locale must not crash Unicode startup messages.
- Ports 19080, 8001 and 3000 are checked with their process ancestry. An unknown
  listener blocks startup. DEV TDX ports 19081/19082 are not managed.
- An old startup/shutdown process or monthly worker is allowed up to 120 seconds
  to settle. A second launcher is rejected while the OS startup lock is held.
- Each service has up to 120 seconds for readiness. Backend health and its frozen
  source identity must match the clean checkout. TDX health requires metadata-ready
  and production identity; a newly started TDX must match its verified build receipt,
  not an unrelated repository HEAD. Frontend stays on port 3000, not a fallback port.
- Frontend preparation overlaps TDX build/readiness; the backend still starts
  after TDX is ready. Closed-port polling uses a cheap loopback connection, not
  a new PowerShell/CIM inventory on every poll. Before accepting readiness, the
  listener's PID/ancestry and service health/identity must still be verified.
- The fixed TDX executable is built/reused by `scripts/start_tdx_go_backend.py`.
  Its build/reuse checks are separate from service readiness.
  The first build or changed TDX inputs can take longer; unchanged inputs reuse
  the fixed EXE. `tdx-build.log` reports `build_action=built` or `reused`.
- Existing healthy instances are not relaunched. A different backend revision is
  reported as old and is not silently accepted as an updated backend.
- Failures return nonzero and preserve each service console plus logs under
  `F:\Dev\AIstock_runtime\startup\<start-id>`. Successful startup writes a compact
  `summary.json`. There is no automatic kill, retry loop or rollback of other services.

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
