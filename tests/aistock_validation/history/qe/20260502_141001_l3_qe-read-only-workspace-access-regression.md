# QE read-only workspace access regression

- Module: qe
- Level: L3
- Date: 2026-05-02T14:10:01+08:00
- Base Git commit: 41ca660
- Operator: lc999 / Codex

## Scope

- Changed files: QE experiment read/log/analysis backend path, QE feedback service, QE experiment detail UI, QE SSE copy, QE read-only backend/UI tests, nox QE read sessions, QE validation matrix, architecture validation plan.
- Impacted flows: existing evolution task detail read, single experiment enhanced metrics read, terminal experiment log tail, experiment analysis/evolution context read, experiment detail page metric/chart/log display.
- Business goal: QE read-only pages display accurate DB/node API data without Windows FastAPI directly reading WSL/RD-Agent worker workspace files.
- Out of scope: experiment creation, dispatch, run, retry, rerun, resume, fork, append, delete, cleanup, scheduler behavior beyond read-only no-active-task polling tests.
- Protected assets reviewed: no edits under mlruns, qe_workspace, rdagent_assets runtime artifacts, model weights, HMM snapshots, StrategyPackage frozen manifests, selection artifacts, or paper ledgers.

## Environment

- Backend port: 8012 (temporary dev FastAPI, schedulers/scanners disabled; production 8001 not restarted)
- Frontend port: 3011 (Playwright-managed Next.js dev server)
- TDX port: skipped for this QE read-only L3
- Conda/env: C:/Users/lc999/miniconda3/envs/AIstock/python.exe
- Database: local AIstock PostgreSQL/TimescaleDB via backend .env
- Browser/headless: Playwright Chromium headless
- QE read task: qe_20260414_173338_d1c5
- QE read experiment: qe_20260501_011054_c90a_L1

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No HIGH hardcoded path/secret/silent fallback findings in read-path validation scope | `scan_quality_guardrails.py ... --fail-on HIGH` completed; 19 MEDIUM raw JSON review findings only | PASS |
| Backend tests | QE read-path unit/integration regressions pass | `backend/tests/unified_engine/test_qe_evolution_read_paths.py`, `test_qe_experiment_read_paths.py`, `test_qe_experiment_log_terminal.py`: 11 passed | PASS |
| API flow | API, DB cached metrics, node log API, and task detail agree | API probe summary below: IC/RankIC match, 437 IC points, 869 all-stock rows, node log source `qe_workspace_api` | PASS |
| UI E2E | User-visible QE read-only flow works; no page/console/request failures; data visible, not just HTTP 200 | Playwright QE read-only suite: 4 passed | PASS |
| Asset safety | No protected runtime artifacts or experiment execution paths modified | staged-file audit excludes runtime artifacts and creation/dispatch/retry/delete code; delete/cleanup residual `QE_WORKSPACE_WIN` remains out of scope | PASS |

## Commands

```powershell
# Restart temporary dev backend only; production 8001 was not touched.
$env:DISABLE_INGESTION_SCHEDULER='1'
$env:DISABLE_STRATEGY_SCHEDULER='1'
$env:DISABLE_PAPER_TRADING_SCHEDULER='1'
$env:ENABLE_PAPER_TRADING_V2_SCHEDULER='0'
$env:DISABLE_NODE_HEALTH_SCHEDULER='1'
$env:DISABLE_HMM_SCHEDULER='1'
$env:DISABLE_EVOLUTION_SCANNER='1'
$env:DISABLE_QE_EXPERIMENT_SCANNER='1'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -c "import os, uvicorn; from dotenv import load_dotenv; load_dotenv(r'F:/Dev/AIstock/.env', override=True); os.environ['PYTHON_DOTENV_DISABLED']='1'; [os.environ.__setitem__(k, v) for k, v in {'DISABLE_INGESTION_SCHEDULER':'1','DISABLE_STRATEGY_SCHEDULER':'1','DISABLE_PAPER_TRADING_SCHEDULER':'1','ENABLE_PAPER_TRADING_V2_SCHEDULER':'0','DISABLE_NODE_HEALTH_SCHEDULER':'1','DISABLE_HMM_SCHEDULER':'1','DISABLE_EVOLUTION_SCANNER':'1','DISABLE_QE_EXPERIMENT_SCANNER':'1','PYTHONIOENCODING':'utf-8'}.items()]; uvicorn.run('backend.main:app', host='127.0.0.1', port=8012)"

# Syntax gate.
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/routers/quantevolver.py backend/services/quantevolver/qe_feedback_service.py

# Full QE read-only L3.
$env:BACKEND_PORT='8012'
$env:FRONTEND_PORT='3011'
$env:QE_API_BASE='http://127.0.0.1:8012/api/v1'
$env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8012/api/v1'
$env:QE_READ_TASK_ID='qe_20260414_173338_d1c5'
$env:QE_READ_EXPERIMENT_ID='qe_20260501_011054_c90a_L1'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_read_l3

# API business sanity probe.
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -c "import requests; base='http://127.0.0.1:8012/api/v1'; exp='qe_20260501_011054_c90a_L1'; task='qe_20260414_173338_d1c5'; e=requests.get(f'{base}/quantevolver/experiments/{exp}',timeout=30).json()['experiment']; m=requests.get(f'{base}/quantevolver/experiments/{exp}/enhanced-metrics',timeout=30).json(); l=requests.get(f'{base}/quantevolver/experiments/{exp}/logs/tail?tail=3',timeout=30).json()['data']; t=requests.get(f'{base}/quantevolver/evolution/tasks/{task}',timeout=30).json()['data']; print({'experiment_id':e['experiment_id'],'experiment_status':e['status'],'qe_task_id':e['qe_task_id'],'qe_loop_id':e['qe_loop_id'],'result_IC':e['result_metrics'].get('IC'),'result_Rank_IC':e['result_metrics'].get('Rank_IC'),'enhanced_ic':m['summary'].get('ic'),'enhanced_rank_ic':m['summary'].get('rank_ic'),'enhanced_annualized_return':m['summary'].get('annualized_return'),'dates_count':len(m.get('dates') or []),'ic_series_count':len(m.get('ic_series') or []),'all_stocks_count':len(m.get('all_stocks') or []),'log_source':l.get('log_source'),'log_node':l.get('node_id'),'log_lines':len(l.get('logs') or []),'task_status':t.get('status'),'task_current_loop':t.get('current_loop'),'task_max_loops':t.get('max_loops'),'task_loops_count':len(t.get('loops') or [])})"
```

## Evidence

- API calls:
  - `/api/v1/quantevolver/experiments/qe_20260501_011054_c90a_L1` returned completed experiment with `qe_task_id=qe_20260501_011054_c90a`, `qe_loop_id=Loop1`, `IC=0.06935669161926977`, `Rank_IC=0.08770889995742083`.
  - `/api/v1/quantevolver/experiments/qe_20260501_011054_c90a_L1/enhanced-metrics` returned `summary.ic=0.06935669161926977`, `summary.rank_ic=0.08770889995742083`, `summary.annualized_return=0.39348126349725115`, `dates_count=437`, `ic_series_count=437`, `all_stocks_count=869`.
  - `/api/v1/quantevolver/experiments/qe_20260501_011054_c90a_L1/logs/tail?tail=3` returned `log_source=qe_workspace_api`, `node_id=wsl2-5080`, `log_lines=3`.
  - `/api/v1/quantevolver/evolution/tasks/qe_20260414_173338_d1c5` returned `status=completed`, `current_loop=2`, `max_loops=2`, `loops_count=2`.
- UI observations:
  - Evolution task page loads terminal task details and loop data.
  - No-active-task dashboard state does not keep automatic once-per-second polling; manual refresh / click-driven refresh is validated.
  - Experiment detail page shows metric cards, stock data, and `IC 诊断` chart from `/experiments/{id}/enhanced-metrics` only.
  - Experiment detail page does not issue guessed `/evolution/tasks/{experimentId}/loops/{experimentId}_Loop1/enhanced-metrics` fallback request.
  - Terminal log UI says QE node log tail and does not expose local workspace path wording.
- Playwright report/trace: no retained trace for passing run; stdout recorded 4/4 passed.
- Backend log evidence: temporary backend 8012 served API probes and Playwright requests; no unexpected 5xx during final L3.
- Business output summary: UI-visible IC/RankIC/annualized-return series and stock counts are sourced from DB cache or QE node API, not Windows local workspace reads.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Initial L3 guardrail failed on 4 HIGH `SILENT_EMPTY_SUCCESS` matches | Guardrail regex flagged tuple-return file read helpers and JSON parse helper defaults in `backend/routers/quantevolver.py` | Narrowed file-read catches to file/encoding errors and made JSON parse helpers explicit with warnings; no business behavior change | Final `qe_read_l3` guardrail completed with no HIGH findings |
| Earlier UI E2E expected canvas chart | QE chart is Plotly/SVG, not canvas | UI test asserts `IC 诊断` chart section and metric data instead of canvas implementation detail | Final Playwright 4 passed |

## Result

- Final status: PASS
- Remaining risks:
  - Existing MEDIUM raw-JSON review findings remain in evolution UI/test files; not blocking this L3 because `--fail-on HIGH` and outside current read-path redline.
  - Existing `QE_WORKSPACE_WIN` usage in delete/cleanup and other non-read execution/asset flows remains out of scope by user instruction and must be addressed in later approved phases.
  - StrategyPackage/selection artifact readers that directly touch worker paths remain later phases, not part of this QE experiment read/log/analysis fix.
- Need production backend restart: yes, after merge/deploy, for production 8001 to pick up the backend changes; production 8001 was not restarted during validation.
- Need dev service restart: dev backend 8012 was restarted and is still a temporary validation service unless manually stopped.
