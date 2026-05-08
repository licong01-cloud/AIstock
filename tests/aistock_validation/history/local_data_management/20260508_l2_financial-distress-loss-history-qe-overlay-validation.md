# L2 Validation - Financial Distress Loss-History QE Overlay Research - 2026-05-08

## Scope

```text
Worktree      : F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508
Branch        : codex/financial-distress-rerank-20260508
Changed scope : backend/services/event_signal, backend/tests/event_signal, docs/analysis
Runtime scope : offline QE overlay research only
Production 8001 touched: no
Writes DB     : no
QE runtime hook: no
Paper/Selection/QMT hook: no
```

## Business Goal

Validate whether rolling financial loss history can be used as a structured financial-distress score-down candidate before any runtime integration.

False-success risks checked:

- The rule could be too broad and improve one loop only by chance.
- The rule could duplicate the existing loss-to-market-cap >= 50% severe-loss rule.
- The offline script could accidentally reference runtime consumers.
- The report could be generated without test coverage for new rule routing.

## Commands

```powershell
python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_qe_overlay_research.py
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
rg -n "loss_history|loss_reports_ge_4|financial_distress_loss_history" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

```bash
wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock_worktrees/financial-distress-rerank-20260508 && export PYTHONIOENCODING=utf-8 PYTHONDONTWRITEBYTECODE=1 TDX_DB_HOST=127.0.0.1 TDX_DB_PORT=5432 TDX_DB_NAME=aistock TDX_DB_USER=postgres TDX_DB_PASSWORD=<redacted> && python -m backend.services.event_signal.financial_distress_qe_overlay_research --loop-spec-json .codex_tmp/event_signal/financial_distress_qe_loops.json --output-dir reports/event_signal/financial_distress_loss_history_qe_overlay --date-from 2024-07-01 --date-to 2026-04-27 --simulator-mode score_down --score-down-rank-penalty-pct 0.20 --simulator-mode score_down_severity --score-down-severity-profile balanced --score-down-top-k 50 --score-down-ranking-date-mode previous --loss-history-only --no-overlay-csv --price-return-csv /mnt/f/Dev/AIstock_worktrees/event-signal-policy-20260507/reports/event_signal/qe_overlay_validation/candidate_price_returns_loop1_20240701_20260427.csv"
```

## Results

```text
+------------------------------------------------------------+--------+
| check                                                      | result |
+------------------------------------------------------------+--------+
| py_compile                                                 | pass   |
| targeted pytest backend/tests/event_signal                 | 21 pass|
| event signal pytest suite                                  | 151 pass|
| runtime isolation rg scan                                  | pass; no matches |
| git diff --check                                           | pass; LF/CRLF warnings only |
| WSL 10-loop offline QE overlay                             | pass; 240 validations |
+------------------------------------------------------------+--------+
```

Generated evidence:

```text
reports/event_signal/financial_distress_loss_history_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_173351.json
reports/event_signal/financial_distress_loss_history_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_173351.md
docs/analysis/event_signal_financial_distress_loss_history_qe_overlay_result_20260508.md
```

## Business Findings

```text
+-------------------------------------------+------------------+-------------------------------------------------------------+
| rule                                      | decision         | reason                                                      |
+-------------------------------------------+------------------+-------------------------------------------------------------+
| loss_reports_ge_4                         | REJECT_RUNTIME   | Too broad; 60/120td return impact is not stable.            |
| loss_reports_ge_4_mv_lt_10bn              | RESEARCH_FEATURE | Useful small-cap feature, but still too broad standalone.   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | RESEARCH_FEATURE | Best incremental 242td row, but short horizons are weak.    |
| forecast_loss_reports_ge_4_mv_lt_10bn     | RESEARCH_FEATURE | Cross-source explanation feature, not standalone runtime.   |
+-------------------------------------------+------------------+-------------------------------------------------------------+
```

## Boundary Verification

```text
No writes to DB.
No backend API or scheduler changes.
No QE runtime, Paper Trading v2, Selection Center, or QMT references added.
No production backend/frontend restart.
Reports under reports/ remain ignored generated artifacts.
```

## Residual Risks

- Offline overlay uses existing QE artifacts and a historical price-return CSV; it is suitable for research direction, not production readiness.
- Loss-history rules are broad and need further combination with market-cap, relative-loss, industry, and consecutive annual-loss features.
- The next stage should remain script-only and compare every new signal against the existing `loss_to_market_cap_ge_50pct_mv_lt_10bn` benchmark.
