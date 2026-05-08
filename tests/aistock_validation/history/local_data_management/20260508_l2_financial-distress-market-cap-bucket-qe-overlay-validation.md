# L2 Validation - Financial Distress Market-Cap Bucket QE Overlay Research - 2026-05-08

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

Add market-cap bucket visibility for every financial-distress rule/mode so the research does not overgeneralize small-cap findings to all A-share market-cap buckets.

False-success risks checked:

- A signal appears useful only because all Top50 effects are concentrated in <10bn CNY stocks.
- Medium/large-cap signals are hidden by all-market averages.
- New research summary accidentally touches runtime consumers.
- Generated markdown exists but JSON lacks full bucket rows.

## Commands

```powershell
python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_qe_overlay_research.py
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
rg -n "market_cap_bucket_summary|MARKET_CAP_BUCKET_ORDER|normalize_market_cap_bucket_counter" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

```bash
wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock_worktrees/financial-distress-rerank-20260508 && export PYTHONIOENCODING=utf-8 PYTHONDONTWRITEBYTECODE=1 TDX_DB_HOST=127.0.0.1 TDX_DB_PORT=5432 TDX_DB_NAME=aistock TDX_DB_USER=postgres TDX_DB_PASSWORD=<redacted> && python -m backend.services.event_signal.financial_distress_qe_overlay_research --loop-spec-json .codex_tmp/event_signal/financial_distress_qe_loops.json --output-dir reports/event_signal/financial_distress_market_cap_bucket_qe_overlay --date-from 2024-07-01 --date-to 2026-04-27 --simulator-mode score_down --score-down-rank-penalty-pct 0.20 --simulator-mode score_down_severity --score-down-severity-profile balanced --score-down-top-k 50 --score-down-ranking-date-mode previous --include-size-bucket-rules --include-loss-history-rules --no-overlay-csv --price-return-csv /mnt/f/Dev/AIstock_worktrees/event-signal-policy-20260507/reports/event_signal/qe_overlay_validation/candidate_price_returns_loop1_20240701_20260427.csv"
```

## Results

```text
+------------------------------------------------------------+------------------------------+
| check                                                      | result                       |
+------------------------------------------------------------+------------------------------+
| py_compile                                                 | pass                         |
| targeted financial-distress pytest                         | 23 passed                    |
| event signal pytest suite                                  | 153 passed                   |
| runtime isolation rg scan                                  | pass; no matches             |
| git diff --check                                           | pass; LF/CRLF warnings only  |
| WSL 10-loop offline QE overlay                             | pass; 840 validations        |
| market_cap_bucket_summary rows                             | 504 rows                     |
+------------------------------------------------------------+------------------------------+
```

Generated evidence:

```text
reports/event_signal/financial_distress_market_cap_bucket_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_190841.json
reports/event_signal/financial_distress_market_cap_bucket_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_190841.md
docs/analysis/event_signal_financial_distress_market_cap_bucket_qe_overlay_result_20260508.md
```

## Business Findings

```text
+-------------------------------------+--------------------------------------------------------------+
| finding                             | implication                                                  |
+-------------------------------------+--------------------------------------------------------------+
| Current strongest effect is <10bn   | Keep small-cap severe-loss benchmark, but do not hard-code it|
| 10-30bn has sparse Top50 interaction| Needs dedicated medium-cap event-family research             |
| >=100bn has almost no interaction   | Raw loss/mv and rolling loss are insufficient for large caps |
| all buckets now appear in JSON      | Future rules can be reviewed without small-cap-only bias     |
+-------------------------------------+--------------------------------------------------------------+
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

- Market-cap bucket summary is exposure and Top50-hit diagnostics; it does not rerun a separate return counterfactual for each bucket yet.
- Composite market-cap rows can be counted into multiple buckets when multiple active events share one stock/date; this is intentional for coverage but should not be interpreted as mutually exclusive row share.
- Medium/large-cap effectiveness still needs new event families beyond raw financial loss and rolling-loss rules.
