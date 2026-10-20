# Windows one-click startup

Run `F:\Dev\AIstock\start_all_ai_stock.bat` after closing the actual old service
windows. Closing only the parent PowerShell does not guarantee child services stop.
The launcher never kills an existing process, reloads Python, installs dependencies,
changes firewall rules, or invokes a database migration.

## Behaviour

- Conda activation and the absolute AIstock Python are checked before dispatch.
- All three services use the same ordinary Windows console implementation; no
  Windows Terminal-specific command chain or environment differences remain.
- Ports 19080, 8001 and 3000 are checked with their process ancestry. An unknown
  listener blocks startup. DEV TDX ports 19081/19082 are not managed.
- An old startup/shutdown process or monthly worker is allowed up to 120 seconds
  to settle. A second launcher is rejected while the OS startup lock is held.
- Each service has up to 120 seconds for readiness. Backend health and its frozen
  source identity must match the clean checkout. TDX health requires metadata-ready
  and production identity; a newly started TDX must match its verified build receipt,
  not an unrelated repository HEAD. Frontend stays on port 3000, not a fallback port.
- The fixed TDX executable is built/reused by `scripts/start_tdx_go_backend.py`.
  Its build/reuse checks are separate from service readiness.
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
