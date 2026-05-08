# L2 Financial Distress QE Overlay Validation

Date: 2026-05-08 Asia/Shanghai
Worktree: `F:\Dev\AIstock_worktrees\financial-distress-qe-overlay-20260508`
Branch: `codex/financial-distress-qe-overlay-20260508`
Scope: event_signal research only; no QE / Selection / Paper / QMT / simulated / live trading runtime change.
Production backend impact: none; port `8001` was not restarted or touched.

## Implemented Scope

- Added read-only script `backend/services/event_signal/financial_distress_qe_overlay_research.py`.
- Added tests `backend/tests/event_signal/test_financial_distress_qe_overlay_research.py`.
- Evaluated first-batch financial distress candidate rules on `qe_20260507_132049_d4e7` / `Loop1`.
- Wrote curated result doc `docs/analysis/event_signal_financial_distress_qe_overlay_result_20260508.md`.
- Generated ignored runtime reports under `reports/event_signal/financial_distress_qe_overlay/`.

## Commands And Results

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_qe_overlay_research.py
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
# 5 passed in 0.71s

python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
# 135 passed in 2.54s
```

```bash
wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock_worktrees/financial-distress-qe-overlay-20260508 && export PYTHONIOENCODING=utf-8 PYTHONDONTWRITEBYTECODE=1 TDX_DB_HOST=127.0.0.1 TDX_DB_PORT=5432 TDX_DB_NAME=aistock TDX_DB_USER=postgres TDX_DB_PASSWORD='***' && python -m backend.services.event_signal.financial_distress_qe_overlay_research --experiment-id qe_20260507_132049_d4e7 --loop-id Loop1 --loop-path /mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop1 --date-from 2024-07-01 --date-to 2026-04-27 --price-return-csv /mnt/f/Dev/AIstock_worktrees/event-signal-policy-20260507/reports/event_signal/qe_overlay_validation/candidate_price_returns_loop1_20240701_20260427.csv --output-dir reports/event_signal/financial_distress_qe_overlay"
# report_id: financial_distress_qe_overlay_qe_20260507_132049_d4e7_Loop1_20240701_20260508_094938
# validations: 30
```

## Key Result

```text
+-----------+--------------------------------------+----------------+--------------+--------------+------------+
| active_td | rule_key                             | mode           | blocked_buys | return_delta | cagr_delta |
+-----------+--------------------------------------+----------------+--------------+--------------+------------+
| 60        | loss_to_market_cap_ge_50pct          | next_candidate | 6            | 3.34%        | 1.13%      |
| 60        | forecast_loss_to_market_cap_ge_50pct | next_candidate | 6            | 3.34%        | 1.13%      |
| 60        | loss_20_50pct_and_loss_reports_ge_4  | cash           | 10           | 0.48%        | 0.16%      |
| 120       | forecast_loss_and_loss_reports_ge_4  | cash           | 134          | -14.40%      | -4.94%     |
| 242       | forecast_loss_and_loss_reports_ge_4  | next_candidate | 163          | -25.67%      | -8.89%     |
+-----------+--------------------------------------+----------------+--------------+--------------+------------+
```

Interpretation:

- `loss_to_market_cap_ge_50pct` is the best candidate for the next multi-loop research round.
- The broad `forecast_loss_and_loss_reports_ge_4` rule is too wide for overlay use on Loop1.
- Financial distress rules remain research-only. They are not hard buy bans or forced sells.
- Active lifetime must be configurable; Loop1 favors 60 trading days over 120 / 242 trading days.
