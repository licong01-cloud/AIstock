# Guardrail Delta Triage - 2026-05-13

Scope: triage only for BUG-038 / GitHub #19. No business-code remediation is included in this report.

## Scan Inputs
- Current scan artifact: `tmp/guardrail_delta_triage_20260513/guardrail_delta_current_vs_20260511.json` (gitignored, generated from current `origin/main`)
- Baseline: `tests/aistock_validation/guardrails_baseline_20260511.json`
- Command: `python scripts/aistock_guardrail_scan.py --baseline --baseline-json tests/aistock_validation/guardrails_baseline_20260511.json --fail-new-only --fail-on-severity NONE --output-json tmp/guardrail_delta_triage_20260513/guardrail_delta_current_vs_20260511.json --summary-md tmp/guardrail_delta_triage_20260513/guardrail_delta_current_vs_20260511.md`

## Summary
- New fingerprints: 195 ({'P0': 36, 'P2': 151, 'P1': 8})
- New P0/P1 fingerprints triaged here: 44
- Classification counts: {'codex_child_candidate': 32, 'defer_other_window': 9, 'child_candidate_needs_owner_confirm': 1, 'false_positive_or_owned_elsewhere': 1, 'false_positive_guardrail': 1}
- Owner counts: {'codex-app': 33, 'paper-runtime-window': 8, 'qe-archive/dw-owner': 1, 'qe-factor-window': 2}

## Validation
- JSON load: `json_ok 39` for `tests/aistock_validation/bugs/*.json`
- Targeted GitHub sync/MCP tests: `43 passed in 1.79s`
- Python compile: `python -m py_compile scripts/bug_github_sync.py scripts/aistock_mcp_server.py backend/tests/scripts/test_bug_github_sync.py backend/tests/scripts/test_aistock_mcp_github_issue_tools.py`
- Whitespace check: `git diff --check`
- Guardrail scan rerun: `files=1683`, `findings=1874`, `baseline=1679`, `new=195`, `blocking=0`

## Decision
- Do not import these 44 P0/P1 static findings as 44 GitHub issues.
- Keep BUG-038 / #19 open as the aggregate triage issue.
- Split only confirmed, owner-scoped work into child BUGs after focused review.
- Do not touch Paper simulation, strategy runtime, or QE experiment paths from this window.

## Recommended Child Work
- `GUARDRAIL-P0-BACKEND-LIFECYCLE-EXCEPTION-HANDLING` (codex-app): 28 findings. Create child BUG only after checking startup/shutdown/db-pool behavior and writing a focused verification plan.
- `GUARDRAIL-P1-R6-CUTOVER-DB-CREDS` (codex-app): 3 findings. Create child BUG or patch to load DB credentials from env/.env without touching production DB.
- `GUARDRAIL-P1-REGIME-LABEL-TEST-LOCATION` (codex-app): 1 findings. Move to backend/tests or debug_tools after checking no external runner depends on this path.
- `GUARDRAIL-QEARCHIVE-SYNTHESIZE-DECIMAL-PARSE` (qe-archive/dw-owner): 1 findings. Confirm archive owner before splitting or fixing; likely narrow exception handling around Decimal parse.

## Classification by Group
### backend lifecycle/db pool exception handling
- Count: 28; Owner: codex-app; Classification: codex_child_candidate
- `P0` `ERR-FALLBACK-001` `backend/db/pg_pool.py:186` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/db/pg_pool.py:52` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/db/pg_pool.py:56` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/db/pg_pool.py:228` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/db/pg_pool.py:316` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/db/pg_pool.py:324` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/db/pg_pool.py:365` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:88` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:93` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:127` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:142` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:144` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:166` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:187` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:218` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:237` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:247` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:251` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:389` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:393` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:398` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:403` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:408` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:413` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:418` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:423` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:429` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/main.py:433` - Broad exception handlers must not return fake success or defaults

### Paper v2 POC/runtime smoke scripts
- Count: 7; Owner: paper-runtime-window; Classification: defer_other_window
- `P0` `ERR-FALLBACK-001` `backend/services/paper_trading_v2/poc/step3_vnpy_smoke.py:147` - Broad exception handlers must not return fake success or defaults
- `P1` `CONFIG-HARDCODE-001` `backend/services/paper_trading_v2/poc/step3_vnpy_smoke.py:58` - Runtime code must not hardcode workstation paths or secrets
- `P1` `CONFIG-HARDCODE-001` `backend/services/paper_trading_v2/poc/step3b_vendored_pythonpath_probe.py:59` - Runtime code must not hardcode workstation paths or secrets
- `P1` `CONFIG-HARDCODE-001` `backend/services/paper_trading_v2/poc/step3b_vendored_pythonpath_probe.py:90` - Runtime code must not hardcode workstation paths or secrets
- `P1` `CONFIG-HARDCODE-001` `backend/services/paper_trading_v2/poc/step3b_vendored_pythonpath_probe.py:212` - Runtime code must not hardcode workstation paths or secrets
- `P0` `ERR-FALLBACK-001` `backend/services/paper_trading_v2/poc/step4_intraday_revalidate.py:211` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/services/paper_trading_v2/poc/step4_intraday_revalidate.py:251` - Broad exception handlers must not return fake success or defaults

### R6 cutover scripts hardcoded DB credentials
- Count: 3; Owner: codex-app; Classification: codex_child_candidate
- `P1` `CONFIG-HARDCODE-001` `scripts/r6_cutover_apply_r5_migrations.py:25` - Runtime code must not hardcode workstation paths or secrets
- `P1` `CONFIG-HARDCODE-001` `scripts/r6_cutover_synthetic_evidence_pkg_5a5c.py:28` - Runtime code must not hardcode workstation paths or secrets
- `P1` `CONFIG-HARDCODE-001` `scripts/r6_cutover_synthetic_evidence_rollback.py:35` - Runtime code must not hardcode workstation paths or secrets

### factor/QE official evaluation rollback cleanup
- Count: 2; Owner: qe-factor-window; Classification: defer_other_window
- `P0` `ERR-FALLBACK-001` `backend/services/quantevolver/factor_official_evaluation_service.py:1326` - Broad exception handlers must not return fake success or defaults
- `P0` `ERR-FALLBACK-001` `backend/services/quantevolver/factor_official_evaluation_service.py:1333` - Broad exception handlers must not return fake success or defaults

### qe_archive numeric synthesis exception specificity
- Count: 1; Owner: qe-archive/dw-owner; Classification: child_candidate_needs_owner_confirm
- `P0` `ERR-FALLBACK-001` `backend/services/qe_archive/handlers/_synthesize.py:42` - Broad exception handlers must not return fake success or defaults

### docstring mentions cache fallback; strategy runtime owned elsewhere
- Count: 1; Owner: paper-runtime-window; Classification: false_positive_or_owned_elsewhere
- `P0` `TRADING-FALLBACK-001` `backend/services/strategy_package/live_inference.py:1084` - Trading, backtest, and HMM paths must not silently downgrade business logic

### fail-fast error text mentions default local registry
- Count: 1; Owner: codex-app; Classification: false_positive_guardrail
- `P0` `TRADING-FALLBACK-001` `scripts/aistock_mcp_server.py:808` - Trading, backtest, and HMM paths must not silently downgrade business logic

### script/test placement hygiene
- Count: 1; Owner: codex-app; Classification: codex_child_candidate
- `P1` `SCRIPT-LOCATION-001` `scripts/test_regime_label.py:1` - One-off test and diagnostic scripts belong under debug_tools

## Full P0/P1 Triage Table
| Classification | Owner | Severity | Rule | File | Line |
| --- | --- | --- | --- | --- | ---: |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 186 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 52 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 56 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 228 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 316 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 324 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 365 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 88 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 93 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 127 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 142 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 144 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 166 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 187 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 218 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 237 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 247 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 251 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 389 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 393 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 398 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 403 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 408 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 413 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 418 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 423 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 429 |
| `codex_child_candidate` | `codex-app` | `P0` | `ERR-FALLBACK-001` | `backend/main.py` | 433 |
| `defer_other_window` | `paper-runtime-window` | `P0` | `ERR-FALLBACK-001` | `backend/services/paper_trading_v2/poc/step3_vnpy_smoke.py` | 147 |
| `defer_other_window` | `paper-runtime-window` | `P1` | `CONFIG-HARDCODE-001` | `backend/services/paper_trading_v2/poc/step3_vnpy_smoke.py` | 58 |
| `defer_other_window` | `paper-runtime-window` | `P1` | `CONFIG-HARDCODE-001` | `backend/services/paper_trading_v2/poc/step3b_vendored_pythonpath_probe.py` | 59 |
| `defer_other_window` | `paper-runtime-window` | `P1` | `CONFIG-HARDCODE-001` | `backend/services/paper_trading_v2/poc/step3b_vendored_pythonpath_probe.py` | 90 |
| `defer_other_window` | `paper-runtime-window` | `P1` | `CONFIG-HARDCODE-001` | `backend/services/paper_trading_v2/poc/step3b_vendored_pythonpath_probe.py` | 212 |
| `defer_other_window` | `paper-runtime-window` | `P0` | `ERR-FALLBACK-001` | `backend/services/paper_trading_v2/poc/step4_intraday_revalidate.py` | 211 |
| `defer_other_window` | `paper-runtime-window` | `P0` | `ERR-FALLBACK-001` | `backend/services/paper_trading_v2/poc/step4_intraday_revalidate.py` | 251 |
| `child_candidate_needs_owner_confirm` | `qe-archive/dw-owner` | `P0` | `ERR-FALLBACK-001` | `backend/services/qe_archive/handlers/_synthesize.py` | 42 |
| `defer_other_window` | `qe-factor-window` | `P0` | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_official_evaluation_service.py` | 1326 |
| `defer_other_window` | `qe-factor-window` | `P0` | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_official_evaluation_service.py` | 1333 |
| `false_positive_or_owned_elsewhere` | `paper-runtime-window` | `P0` | `TRADING-FALLBACK-001` | `backend/services/strategy_package/live_inference.py` | 1084 |
| `false_positive_guardrail` | `codex-app` | `P0` | `TRADING-FALLBACK-001` | `scripts/aistock_mcp_server.py` | 808 |
| `codex_child_candidate` | `codex-app` | `P1` | `CONFIG-HARDCODE-001` | `scripts/r6_cutover_apply_r5_migrations.py` | 25 |
| `codex_child_candidate` | `codex-app` | `P1` | `CONFIG-HARDCODE-001` | `scripts/r6_cutover_synthetic_evidence_pkg_5a5c.py` | 28 |
| `codex_child_candidate` | `codex-app` | `P1` | `CONFIG-HARDCODE-001` | `scripts/r6_cutover_synthetic_evidence_rollback.py` | 35 |
| `codex_child_candidate` | `codex-app` | `P1` | `SCRIPT-LOCATION-001` | `scripts/test_regime_label.py` | 1 |

## Follow-up Rules
- A child issue needs a reproduction/verification command and an allowed write scope.
- False positives should be handled by guardrail pattern refinement or targeted waiver, not runtime code changes.
- Other-window items should be assigned through cross-tool coordination before code edits.
- Each child PR must update the relevant BUG JSON and sync GitHub labels/status.
