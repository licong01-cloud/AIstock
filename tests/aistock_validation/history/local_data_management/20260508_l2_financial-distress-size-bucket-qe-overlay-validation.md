# L2 Financial Distress Size-Bucket QE Overlay Validation

Date: 2026-05-08 Asia/Shanghai
Worktree: `F:\Dev\AIstock_worktrees\financial-distress-sizebucket-20260508`
Branch: `codex/financial-distress-sizebucket-20260508`
Scope: event_signal research script only; no QE / Selection / Paper / QMT / simulated / live trading runtime change.
Production backend impact: none; port `8001` was not restarted or touched.

## Implemented Scope

- Added market-cap bucket rules for `loss_to_market_cap_ge_50pct`.
- Added CLI switches:
  - `--include-size-bucket-rules`
  - `--size-bucket-only`
- Kept all new logic in `backend/services/event_signal/financial_distress_qe_overlay_research.py`.
- Expanded tests in `backend/tests/event_signal/test_financial_distress_qe_overlay_research.py`.
- Ran WSL `rdagent-gpu` size-bucket multi-loop validation on 10 existing QE loop artifacts.
- Wrote result doc `docs/analysis/event_signal_financial_distress_size_bucket_qe_overlay_result_20260508.md`.

## Commands And Results

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_qe_overlay_research.py
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
# 10 passed in 0.80s
```

```bash
wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock_worktrees/financial-distress-sizebucket-20260508 && export PYTHONIOENCODING=utf-8 PYTHONDONTWRITEBYTECODE=1 TDX_DB_HOST=127.0.0.1 TDX_DB_PORT=5432 TDX_DB_NAME=aistock TDX_DB_USER=postgres TDX_DB_PASSWORD='***' && python -m backend.services.event_signal.financial_distress_qe_overlay_research --date-from 2024-07-01 --date-to 2026-04-27 --price-return-csv /mnt/f/Dev/AIstock_worktrees/event-signal-policy-20260507/reports/event_signal/qe_overlay_validation/candidate_price_returns_loop1_20240701_20260427.csv --output-dir reports/event_signal/financial_distress_sizebucket_qe_overlay --no-overlay-csv --size-bucket-only --loop-spec qe_20260507_132049_d4e7,Loop1,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop1 --loop-spec qe_20260507_132049_d4e7,Loop2,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop2 --loop-spec qe_20260506_182113,Loop1,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260506_182113/Loop1 --loop-spec qe_20260505_153534_388f,Loop1,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260505_153534_388f/Loop1 --loop-spec qe_20260505_153534_388f,Loop2,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260505_153534_388f/Loop2 --loop-spec qe_20260505_122348_690d,Loop1,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260505_122348_690d/Loop1 --loop-spec qe_20260505_122348_690d,Loop2,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260505_122348_690d/Loop2 --loop-spec qe_20260501_011054_c90a,Loop19,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260501_011054_c90a/Loop19 --loop-spec qe_20260501_011054_c90a,Loop24,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260501_011054_c90a/Loop24 --loop-spec qe_20260501_011054_c90a,Loop26,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260501_011054_c90a/Loop26"
# loops: 10
# validations: 300
# output_json: reports/event_signal/financial_distress_sizebucket_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_103233.json
```

## Business Result

```text
+-----------+----------------------------------------+----------------+-----------+---------+-----------+-----------+
| active_td | rule_key                               | mode           | pos/loops | blocked | avg_ret_d | med_ret_d |
+-----------+----------------------------------------+----------------+-----------+---------+-----------+-----------+
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | next_candidate | 9/10      | 247     | 12.36%    | 11.84%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_5bn  | next_candidate | 8/10      | 189     | 8.32%     | 10.42%    |
| 242       | loss_to_market_cap_ge_50pct_mv_5_10bn  | next_candidate | 9/10      | 58      | 5.24%     | 4.27%     |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | next_candidate | 7/10      | 137     | 4.66%     | 3.38%     |
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | next_candidate | 7/10      | 77      | 0.84%     | 0.92%     |
| 242       | loss_to_market_cap_ge_50pct_mv_ge_10bn | next_candidate | 5/10      | 3       | 0.09%     | 0.00%     |
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | cash           | 3/10      | 77      | -1.04%    | -1.04%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | cash           | 2/10      | 137     | -2.28%    | -2.31%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | cash           | 1/10      | 247     | -3.57%    | -3.72%    |
+-----------+----------------------------------------+----------------+-----------+---------+-----------+-----------+
```

Interpretation:

- The `loss_to_market_cap_ge_50pct` signal is effectively a small-cap distress overlay.
- `mv_lt_10bn` captures almost all useful exposure; `mv_ge_10bn` has too few hits to be a separate rule.
- Cash-only no-buy remains rejected.
- Next research should simulate score-down or candidate re-ranking for `mv_lt_10bn` only.
