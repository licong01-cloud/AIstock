# L2 Financial Distress Multi-Loop QE Overlay Validation

Date: 2026-05-08 Asia/Shanghai
Worktree: `F:\Dev\AIstock_worktrees\financial-distress-multiloop-20260508`
Branch: `codex/financial-distress-multiloop-20260508`
Scope: event_signal research script only; no QE / Selection / Paper / QMT / simulated / live trading runtime change.
Production backend impact: none; port `8001` was not restarted or touched.

## Implemented Scope

- Extended `backend/services/event_signal/financial_distress_qe_overlay_research.py` with multi-loop batch mode.
- Added loop-spec parsing, common-date resolution, shared financial-row preload, optional shared price-return preload, multi-loop stability summary, and exposure summary.
- Expanded tests in `backend/tests/event_signal/test_financial_distress_qe_overlay_research.py`.
- Ran WSL `rdagent-gpu` multi-loop validation on 10 existing QE loop artifacts.
- Wrote curated result doc `docs/analysis/event_signal_financial_distress_multiloop_qe_overlay_result_20260508.md`.

## Commands And Results

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_qe_overlay_research.py
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
# 8 passed in 0.79s
```

```bash
wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock_worktrees/financial-distress-multiloop-20260508 && export PYTHONIOENCODING=utf-8 PYTHONDONTWRITEBYTECODE=1 TDX_DB_HOST=127.0.0.1 TDX_DB_PORT=5432 TDX_DB_NAME=aistock TDX_DB_USER=postgres TDX_DB_PASSWORD='***' && python -m backend.services.event_signal.financial_distress_qe_overlay_research --date-from 2024-07-01 --date-to 2026-04-27 --price-return-csv /mnt/f/Dev/AIstock_worktrees/event-signal-policy-20260507/reports/event_signal/qe_overlay_validation/candidate_price_returns_loop1_20240701_20260427.csv --output-dir reports/event_signal/financial_distress_qe_multiloop --no-overlay-csv --loop-spec qe_20260507_132049_d4e7,Loop1,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop1 --loop-spec qe_20260507_132049_d4e7,Loop2,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop2 --loop-spec qe_20260506_182113,Loop1,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260506_182113/Loop1 --loop-spec qe_20260505_153534_388f,Loop1,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260505_153534_388f/Loop1 --loop-spec qe_20260505_153534_388f,Loop2,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260505_153534_388f/Loop2 --loop-spec qe_20260505_122348_690d,Loop1,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260505_122348_690d/Loop1 --loop-spec qe_20260505_122348_690d,Loop2,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260505_122348_690d/Loop2 --loop-spec qe_20260501_011054_c90a,Loop19,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260501_011054_c90a/Loop19 --loop-spec qe_20260501_011054_c90a,Loop24,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260501_011054_c90a/Loop24 --loop-spec qe_20260501_011054_c90a,Loop26,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260501_011054_c90a/Loop26"
# loops: 10
# validations: 300
# output_json: reports/event_signal/financial_distress_qe_multiloop/financial_distress_qe_multiloop_20240701_20260508_101734.json
```

## Business Result

```text
+-----------+--------------------------------------+----------------+-----------+---------+-----------+-----------+
| active_td | rule_key                             | mode           | pos/loops | blocked | avg_ret_d | med_ret_d |
+-----------+--------------------------------------+----------------+-----------+---------+-----------+-----------+
| 242       | loss_to_market_cap_ge_50pct          | next_candidate | 9/10      | 250     | 12.46%    | 12.32%    |
| 120       | loss_to_market_cap_ge_50pct          | next_candidate | 7/10      | 138     | 4.67%     | 3.47%     |
| 60        | loss_to_market_cap_ge_50pct          | next_candidate | 7/10      | 78      | 0.86%     | 1.01%     |
| 60        | loss_to_market_cap_ge_50pct          | cash           | 3/10      | 78      | -1.01%    | -1.04%    |
| 120       | loss_to_market_cap_ge_50pct          | cash           | 2/10      | 138     | -2.25%    | -2.31%    |
| 242       | loss_to_market_cap_ge_50pct          | cash           | 1/10      | 250     | -3.52%    | -3.72%    |
| 60        | forecast_loss_and_loss_reports_ge_4  | cash           | 0/10      | 1048    | -7.59%    | -8.11%    |
| 120       | forecast_loss_and_loss_reports_ge_4  | cash           | 0/10      | 1918    | -18.92%   | -20.30%   |
| 242       | forecast_loss_and_loss_reports_ge_4  | cash           | 0/10      | 2305    | -21.18%   | -21.18%   |
+-----------+--------------------------------------+----------------+-----------+---------+-----------+-----------+
```

Interpretation:

- Cash-only no-buy is not validated for financial distress rules.
- Next-candidate replacement is promising in offline approximation, but must not be treated as production QE behavior.
- `loss_to_market_cap_ge_50pct` remains the cleanest candidate for the next size-bucket and score-down research round.
- Broad rolling-loss rules are harmful in cash mode and should not become hard rules.
