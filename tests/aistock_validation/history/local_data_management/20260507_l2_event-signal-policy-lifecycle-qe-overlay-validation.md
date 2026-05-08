# L2 Event Signal Policy Lifecycle and QE Overlay Validation - 2026-05-07

## Scope

- Branch: `codex/event-signal-policy-20260507`.
- Implemented event-signal policy lifecycle schema, ST force-exit policy rules, state-span expansion, daily overlay generation, QE Loop1 offline cash-counterfactual validation, and validation-result persistence.
- Target QE baseline: `qe_20260507_132049_d4e7` / `Loop1`.
- Current phase remains research-only: no QE runtime, Selection Center, Paper v2, QMT, or live trading consumer was modified.
- Production port `8001` was not restarted.

## Commands And Results

- `python -m backend.services.event_signal.policy_lifecycle --ensure-schema --write --start-date 2024-07-01 --end-date 2024-07-31 --source-start-date 2018-08-01 --time-mode backtest --limit 200` -> wrote smoke profile/state/overlay rows.
- `python -m backend.services.event_signal.policy_lifecycle --write --start-date 2024-07-01 --end-date 2026-04-27 --source-start-date 2018-08-01 --time-mode backtest` -> generated full Loop1 ST policy overlay.
- Exported overlay CSV from `market.event_signal_daily_overlay` to `reports/event_signal/qe_overlay_validation/st_policy_overlay_loop1_20240701_20260427.csv`.
- WSL `rdagent-gpu`: `python -m backend.services.event_signal.qe_loop_overlay_validation ...` -> generated JSON and Markdown validation report.
- Persisted validation payload into `market.event_signal_validation_result`.
- `python -m backend.services.event_signal.qe_loop_overlay_validation --prepare-price-return-csv-only ...` -> exported next-candidate return input with `2,249,823` rows, `5,117` symbols, `442` return dates.
- WSL `rdagent-gpu`: `python -m backend.services.event_signal.qe_loop_overlay_validation ... --simulator-mode next_candidate --price-return-csv reports/event_signal/qe_overlay_validation/candidate_price_returns_loop1_20240701_20260427.csv` -> generated next-candidate JSON and Markdown validation report.
- Persisted next-candidate validation payload into `market.event_signal_validation_result`.
- `python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q` -> `114 passed in 2.06s`.
- `python -m py_compile ...` for changed Python files -> passed.
- `git diff --check` -> passed.

## DB Overlay Summary

- Profile: `event_signal_policy_st_force_exit_v1_20260507`.
- Source signal rows: `11839`.
- State spans: `712`.
- Daily overlay rows: `268827`.
- Unique overlay symbols: `712`.
- Force-exit overlay rows: `268827`.
- Block-buy overlay rows: `268827`.

## QE Loop1 Cash Counterfactual Summary

- Baseline final account: `290,649,636.16`.
- Overlay final account: `275,851,858.45`.
- Final account delta: `-14,797,777.71`.
- Baseline CAGR: `79.59%`.
- Overlay CAGR: `74.51%`.
- CAGR delta: `-5.08pp`.
- Baseline max drawdown: `-17.42%`.
- Overlay max drawdown: `-16.36%`.
- Max drawdown improvement: `+1.06pp`.
- Blocked buy events: `139`.
- Force-exit events: `1873`.
- Unique buy-hit symbols: `83`.
- Unique force-exit symbols: `89`.
- Blocked symbols original PnL sum: `+9,858,139.20`.
- Blocked symbol PnL counts: positive `58`, negative `30`, zero `0`.

## QE Loop1 Next-Candidate Counterfactual Summary

- Baseline final account: `290,649,636.16`.
- Overlay final account: `270,623,364.56`.
- Final account delta: `-20,026,271.60`.
- Baseline CAGR: `79.59%`.
- Overlay CAGR: `72.69%`.
- CAGR delta: `-6.90pp`.
- Baseline max drawdown: `-17.42%`.
- Overlay max drawdown: `-17.73%`.
- Max drawdown delta: `-0.32pp`.
- Candidate score dates/symbols: `443` / `5117`.
- Price-return dates/symbols: `442` / `5117`.
- Replacement open events: `1155`.
- Replacement no-candidate events: `0`.
- Replacement reselect events: `1008`.
- Replacement missing-return days: `1`.
- Replacement PnL sum: `-2,918,871.89`.

## Artifacts

- JSON: `reports/event_signal/qe_overlay_validation/event_signal_validation_qe_20260507_132049_d4e7_Loop1_event_signal_policy_st_force_exit_v1_20260507_event_signal_overlay_cash_counterfactual_v1_20260507_2024-07-01_2026-04-27.json`.
- Markdown: `reports/event_signal/qe_overlay_validation/event_signal_validation_qe_20260507_132049_d4e7_Loop1_event_signal_policy_st_force_exit_v1_20260507_event_signal_overlay_cash_counterfactual_v1_20260507_2024-07-01_2026-04-27.md`.
- Next-candidate JSON: `reports/event_signal/qe_overlay_validation/event_signal_validation_qe_20260507_132049_d4e7_Loop1_event_signal_policy_st_force_exit_v1_20260507_event_signal_overlay_next_candidate_v1_20260507_2024-07-01_2026-04-27.json`.
- Next-candidate Markdown: `reports/event_signal/qe_overlay_validation/event_signal_validation_qe_20260507_132049_d4e7_Loop1_event_signal_policy_st_force_exit_v1_20260507_event_signal_overlay_next_candidate_v1_20260507_2024-07-01_2026-04-27.md`.
- Overlay CSV: `reports/event_signal/qe_overlay_validation/st_policy_overlay_loop1_20240701_20260427.csv`.
- Candidate return CSV: `reports/event_signal/qe_overlay_validation/candidate_price_returns_loop1_20240701_20260427.csv`.
- DB validation key: `event_signal_validation:qe_20260507_132049_d4e7:Loop1:event_signal_policy_st_force_exit_v1_20260507:event_signal_overlay_cash_counterfactual_v1_20260507:2024-07-01:2026-04-27`.
- DB next-candidate validation key: `event_signal_validation:qe_20260507_132049_d4e7:Loop1:event_signal_policy_st_force_exit_v1_20260507:event_signal_overlay_next_candidate_v1_20260507:2024-07-01:2026-04-27`.

## Interpretation

- Lifecycle-aware ST policy improves max drawdown by about `1.06pp`, but still reduces final account by about `14.80M` in a pure cash counterfactual.
- The reason is not purely false positives: hit symbols are mixed, with `58` positive and `30` negative original-PnL names. A strategy that uses replacement candidates instead of holding cash may reduce the return drag.
- The first next-candidate replacement simulation did not reduce drag in this Loop1 approximation: final-account delta worsened to about `-20.03M`, max drawdown was slightly worse than baseline, and replacement PnL was about `-2.92M`.
- This result supports keeping ST hard-risk policy in `REVIEW` rather than directly promoting it into a trading consumer.
- The next validation step should diagnose whether the replacement rule is too naive, then test one signal family at a time before any actual QE/Paper integration.
