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

### 单独启动或重启后端（每次新标签页）

```powershell
# 只启动：已有后端时返回失败，不隐式重启
.\start_backend.bat

# 明确重启：停止本项目旧后端及其派生进程，再在新标签页启动
.\start_backend.bat --restart
```

两条命令均请求一个新的 `AIstock Backend` 标签页（Windows Terminal `-w 0 new-tab`）。
`-w 0` 选择最近活动的 Terminal 窗口，不能保证是调用者所在的窗口。
调用窗口等待本次唯一请求的结果，最多 180 秒；只有新后端进程仍存活、8001 的监听者
确实是该进程、health 就绪且冻结的 runtime identity 与启动前干净源码 HEAD 一致，
才打印 `Backend READY` 并返回 0。标签页创建成功本身不算启动成功。

`--restart` 只允许终止工作目录、绝对解释器路径、命令参数均匹配本项目的后端、
后端监督器及固定 `--supervised` 月度 worker；终止前复核进程创建时间，避免 PID
重用。8001 被其他项目或未知进程占用时直接报错，不按端口盲杀。旧树未完全退出或
端口无法独占绑定时不启动第二份。重启使用强制进程树终止，会中断旧后端正在处理的请求。
该显式操作不会操作前端、TDX 或独立 QE 实验。

新后端先以挂起状态创建，再加入不继承句柄的 Windows Job，绑定成功后才执行。
关闭这个新服务标签页、监督器异常退出或启动验收失败时，Windows 会终止其受管
后端及全部派生进程；关闭原始调用窗口不等于关闭服务标签页。此保证只覆盖这个
专用入口创建的进程树，不接管其他入口已有的服务。

缺少环境、Conda 激活失败、锁冲突、应用提前退出、身份不符和超时都返回非零；
不循环重试或用旧进程冒充重启成功。超时或调用被中断会取消本次请求；新标签页
在停止/启动前及等待就绪时核对取消标记，失败清理受管进程树。当前请求的日志和
原子结果回执位于 `F:\Dev\AIstock_runtime\startup\backend-<nonce>\`，后端输出为
`backend.log`。服务标签页在退出后保留错误输出，便于诊断。

`start_backend.bat --check` 仅检查现有 Python/Conda 路径，不开标签、不占端口或启动服务。
`start_backend.bat --check-env` 和 `start_all_ai_stock.bat --check-env` 使用各自独立的
临时目录做真实 Conda 激活，并核对 Python 与 `CONDA_PREFIX`，不启动服务。

上述 READY 证明启动和身份，不代表 QE、交易或模块业务验收通过。正式交付仍保留
用户重启和对应 `post-restart-verify` 业务探针要求。全服务入口保持原来的只启动、
不自动终止已有服务的策略。

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
