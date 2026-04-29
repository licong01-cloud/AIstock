# Paper v2 Live + Dynamic HMM Daily Coefficient Validation Follow-up

## Scope

- Prepared Paper Trading v2 live-market validation for trading hours on development ports only.
- Revalidated `CATCHUP_THEN_LIVE`: replay one latest completed DB historical trading day, switch to current-day `TDX_REALTIME`, process observed minute bars, then settle in live waiting state when no new bar is available.
- Fixed dynamic PUP HMM daily coefficient generation for snapshots that store `state_validation_stats` rather than legacy `state_labels`.
- Fixed Paper v2 model/HMM UI display so ordinary operators see readable status/error summaries instead of raw stderr, traceback, local file paths, or raw structured payloads.

## Environment

- Date/time: 2026-04-29 trading hours.
- Backend: `127.0.0.1:8012` development FastAPI service; production `8001` was not restarted.
- Frontend: `127.0.0.1:3012` existing development Next.js service.
- TDX: `127.0.0.1:19080` realtime minute endpoint.
- StrategyPackage used by live validation: `pkg_b668f8a633c44b72a5d557a2cb8970e3` / `qe_20260416_002701`.
- Execution policy: `execpol_8e96a3ec3d4d414f9581c66fbf405830` / `V25_TWO_STAGE`.

## Commands And Results

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m pytest backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_day_runner.py backend/tests/selection_center/test_runtime_selection.py -q -p no:cacheprovider
# 42 passed

C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m pytest backend/tests/test_hmm_daily_coefficients.py -q -p no:cacheprovider
# 8 passed

C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0
# passed; 0 HIGH, 13 pre-existing MEDIUM findings

C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s paper_v2_backend
# 107 passed

cd frontend
npm exec tsc -- --noEmit
# passed

$env:BACKEND_PORT='8012'; $env:FRONTEND_PORT='3012'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s paper_v2_ui
# 12 passed

$env:BACKEND_PORT='8012'; $env:FRONTEND_PORT='3012'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s paper_v2_live -- --require-live-bars
# passed
```

## Live Validation Output

- Mode: `catchup_then_live`.
- Replay start: `2026-04-28`.
- Live date: `2026-04-29`.
- Latest/processed bar: `2026-04-29T11:19:00+08:00`.
- Final status: `LIVE_WAITING_FOR_BAR`.
- Portfolio: `paper_dc072bb3f06f49fead6f676445bcbdd9`.
- Session: `psess_27cb3a0732464f418c4ee4e3856f7f76`.
- Run count: 2.
- Order count: 21.
- Fill count: 74.
- Error count: 0.

## Business Oracles Checked

- Replay did not use daily fallback, default prices, fake orders, or empty success.
- Live mode used `TDX_REALTIME`; historical replay used `DB_HISTORICAL`.
- Current-day selection uses previous completed trading day as daily-factor cutoff and keeps the current live trade date as target date.
- V25 live execution can start incrementally with observed live bars and does not require 240 bars at market open.
- Missing data/model/runtime errors remain fail-fast; generated HMM coefficients are explicit runtime artifacts with hash/audit.
- UI E2E covered strategy packages, single-package selection, multi-package aggregate selection, watchlist import, HMM/blacklist/top-k controls, portfolio creation, run console readiness, replay reset/reject, live controls, ledger/performance views, and negative structured errors.

## Asset Safety

- No StrategyPackage manifest, model weight, validated execution policy, QE/RD-Agent source asset, or production ledger was modified by code changes.
- HMM daily coefficient validation generated a new runtime coefficient artifact for the selected HMM snapshot as part of the explicit UI/API flow; this artifact is not source code and is not committed.
- Production backend `8001` was not restarted.

## Additional Dynamic HMM Script Check

After adding explicit fail-fast requirements for dynamic HMM config keys, the WSL coefficient script was run against the dynamic PUP snapshot with a temporary `.codex_tmp` output file. Result: `dynamic_coefficients=true`, `sector_count=131`, generated date `2026-03-04`.
