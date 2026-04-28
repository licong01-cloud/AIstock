# QE blacklist stock pool sync timeout fix

- Module: qe
- Level: L2
- Date: 2026-04-29
- Git commit: pre-commit 21464ca
- Operator: Codex

## Scope

- Changed files: `scripts/generate_stock_pool.py`, `frontend/src/app/quantevolver/components/SectorBlacklistPanel.tsx`, `backend/tests/test_generate_stock_pool_paths.py`
- Impacted flows: QE custom evolution industry blacklist -> stock-pool generation -> WSL Qlib instruments sync; QuantEvolver blacklist panel request scheduling.
- Business goal: selecting an industry blacklist must not fail with a 30s WSL `/mnt/f` copy timeout, and repeated UI edits must not start overlapping stock-pool generation requests.
- Out of scope: changing real Qlib bin files, running full QE evolution, modifying strategy/model artifacts, restarting production backend on port 8001.
- Protected assets reviewed: no StrategyPackage manifests, model weights, HMM snapshots, QE/RD-Agent artifacts, validated policies, or real Qlib stock-pool files were modified by validation.

## Environment

- Backend port: not started; production 8001 untouched.
- Frontend port: not started; `next build` only.
- TDX port: not used.
- Conda/env: local `python`, local `npm`; WSL distro `Ubuntu` for isolated `/tmp` smoke.
- Database: not used by smoke; unit tests mock WSL subprocess and path handling.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Backend unit regression | stock-pool sync streams bytes to WSL stdin, avoids `/mnt` and `cp`, uses unique temp file, actionable timeout error | `pytest backend/tests/test_generate_stock_pool_paths.py backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider` -> 20 passed | PASS |
| WSL smoke | generated pool bytes are written to a WSL instruments directory and checksum verified without Windows-mount copy | `sync_pool_to_qlib(..., /tmp/aistock_stock_pool_sync_test/instruments)` returned `/tmp/.../filtered_pool_codex_sync_test.txt` | PASS |
| Frontend build | blacklist panel request-serialization changes pass Next.js compile, lint, and type validation | `npm --prefix frontend run build` -> compiled and generated 65 pages | PASS |
| Guardrail | relevant diffs have no whitespace errors | `git diff --check -- ...` -> no errors, only CRLF conversion warnings | PASS |
| Asset safety | no real Qlib bin/instrument asset or strategy/model asset is changed | smoke used `/tmp`; temp files cleaned | PASS |

## Commands

```bash
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; pytest backend/tests/test_generate_stock_pool_paths.py backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
$env:PYTHONDONTWRITEBYTECODE='1'; python -m py_compile scripts/generate_stock_pool.py
npm --prefix frontend run build
# WSL smoke used scripts.generate_stock_pool.sync_pool_to_qlib with /tmp/aistock_stock_pool_sync_test/instruments.
git diff --check -- scripts/generate_stock_pool.py backend/tests/test_generate_stock_pool_paths.py frontend/src/app/quantevolver/components/SectorBlacklistPanel.tsx
```

## Evidence

- API calls: not run; no backend service was started or restarted.
- DB checks: not run; the failure was isolated to WSL file sync and UI request scheduling.
- Log files: subprocess evidence from tests and WSL smoke.
- Playwright report/trace: not applicable.
- Screenshots: not applicable.
- Business output summary: WSL `/mnt/f` `cp` is no longer used for stock-pool sync; the generated Windows file is streamed through stdin, verified by sha256 inside WSL before and after atomic move, and the UI serializes blacklist-triggered generation.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| HTTP 500 `subprocess.TimeoutExpired` during blacklist selection | stock-pool generator copied from Windows mount `/mnt/f/...` to WSL Qlib with fixed 30s timeout | stream generated file bytes through WSL stdin, unique temp file, 120s configurable timeout, sha256 verification | Backend tests + WSL smoke PASS |
| Repeated blacklist edits can overlap generation | frontend auto-generated on every blacklist state change and initial load | wait for initial blacklist load; serialize in-flight generation and collapse pending requests | `npm --prefix frontend run build` PASS |
| New WSL checksum script initially failed in smoke | WSL `bash -lc` command arguments require escaped `$` for variables in this Windows invocation path | avoid command substitution and escape `$actual_sha` / `$final_sha` in the WSL script | WSL smoke PASS |

## Result

- Final status: PASS for targeted L2 validation.
- Remaining risks: full QE evolution was not rerun because it would create real experiment artifacts; production backend must be restarted by the user/operator before the running 8001 service uses the committed fix.
- Need production backend restart: yes, user-managed only; Codex did not touch port 8001.
- Need dev service restart: no persistent dev service was started.
