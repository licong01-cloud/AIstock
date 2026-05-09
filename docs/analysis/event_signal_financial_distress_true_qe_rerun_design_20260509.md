# Phase 17 True QE Rerun / Dry-Run Harness Design - 2026-05-09

Research-only design and prototype for validating the current financial-distress score-down candidate with Qlib's real portfolio backtest path, while staying outside production QE runtime. This phase adds a standalone prediction materializer under `backend/services/event_signal` and does not modify QE, Selection Center, Paper Trading, QMT, live trading, DB schema, or DB data.

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | 17                                                                  |
| candidate        | indicator_large_decline_mv_10_30bn                                  |
| preferred profile| 60td + score_down_context_rank_decay_balanced_top50_previous        |
| implementation   | research-only pred.pkl materializer + tests                         |
| QE runtime impact| none                                                                |
| DB impact        | none                                                                |
+------------------+---------------------------------------------------------------------+
```

## Key Code Path Finding

```text
+-------------------------------+------------------------------------------------------+---------------------------------------------+
| path                          | finding                                              | phase-17 implication                        |
+-------------------------------+------------------------------------------------------+---------------------------------------------+
| scripts/qrun_limit_minute.py  | --pred-backtest loads external pred.pkl then runs    | true portfolio rerun can use copied preds   |
|                               | SigAnaRecord + PortAnaRecord                         | without changing QE runtime                 |
| qrun_limit_minute.py          | strategy trades date T from prediction date T-1      | previous-date materialization is required   |
| config_composer.py            | generated conf.yaml already contains strategy,       | copied loop conf.yaml can be reused         |
|                               | executor, costs, universe, exchange settings         | for comparable rerun                        |
| event_signal overlay scripts  | existing context profile computes trade-date penalty | reusable for prediction score demotion      |
+-------------------------------+------------------------------------------------------+---------------------------------------------+
```

## Recommended Harness

```text
+------+---------------------------------------------+------------------------------+---------------------------------------------+
| step | action                                      | artifact                     | guardrail                                   |
+------+---------------------------------------------+------------------------------+---------------------------------------------+
| 1    | copy one QE loop workspace to temp research  | copied Loop*/conf.yaml       | never mutate original QE artifact           |
| 2    | build 60td overlay CSV for selected rule     | overlay.csv                  | research-only event_signal script           |
| 3    | materialize score-down into copied pred.pkl  | adjusted_pred.pkl            | preserve original pred.pkl separately       |
| 4    | run qrun_limit_minute.py --pred-backtest     | new MLflow recorder          | run in copied workspace only                |
| 5    | compare original vs rerun report/positions   | comparison JSON/MD           | require rank trace and metric deltas        |
| 6    | aggregate across the same 22 loops           | phase-18/19 research report  | no runtime promotion unless median improves |
+------+---------------------------------------------+------------------------------+---------------------------------------------+
```

## Materializer Contract

The added module is `backend/services/event_signal/financial_distress_pred_materializer.py`.

```text
+---------------------+---------------------------------------------------------------------+
| contract item       | behavior                                                            |
+---------------------+---------------------------------------------------------------------+
| input               | existing QE pred.pkl plus an overlay CSV                            |
| output              | adjusted_pred.pkl, trace.csv, meta.json, optional report.md         |
| ranking mode        | maps trade date T penalty to prediction rank date T-1 by default    |
| profile             | reuses existing context profiles, default rank_decay_balanced       |
| score rewrite       | keeps each date's original score distribution but reassigns scores  |
|                     | so score sorting follows the penalized rank order                   |
| runtime coupling    | none; consumer command is qrun_limit_minute.py --pred-backtest      |
| intended use        | research-only copied loop rerun                                     |
+---------------------+---------------------------------------------------------------------+
```

## Audit Trace Fields

```text
+--------------------------+--------------------------------------------------------------+
| field                    | purpose                                                      |
+--------------------------+--------------------------------------------------------------+
| trade_date               | event-signal effective trading date                          |
| rank_date                | prediction row date actually rewritten for Qlib              |
| ts_code                  | stock code                                                   |
| rank_penalty_pct         | context-aware rank-demotion percentage                       |
| context_profile          | score-down profile key                                       |
| source_signal_ids        | source event_signal ids from overlay when available          |
| event_types              | source event types from overlay when available               |
| original_rank            | rank before materialization                                  |
| adjusted_rank            | rank after score-down order                                  |
| adjusted_sort_rank       | raw sort key after adding penalty ranks                      |
| original_score           | original pred.pkl score                                      |
| materialized_score       | rewritten pred.pkl score                                     |
| dropped_from_topk        | whether original TopK candidate leaves TopK after demotion   |
+--------------------------+--------------------------------------------------------------+
```

## Fidelity Assessment

```text
+----------------------------+------------------+--------------------------------------------------------------+
| question                   | assessment       | explanation                                                  |
+----------------------------+------------------+--------------------------------------------------------------+
| Does it use real Qlib PnL? | yes              | PortAnaRecord reruns strategy/executor/cost/position path    |
| Does it retrain model?     | no               | pred.pkl is intentionally reused to isolate event overlay     |
| Is TopK rank behavior true?| high fidelity    | strategies sort by pred score, and score order is rewritten   |
| Is score-weighted sizing   | approximate      | score values are reassigned, not a native rank overlay hook   |
| Does it test hard bans?    | no               | current financial distress policy is non-hard score-down only |
| Is it deployable runtime?  | no               | this is a research harness, not a Selection/Paper/QE hook     |
+----------------------------+------------------+--------------------------------------------------------------+
```

## Why This Is Better Than The Previous Overlay Approximation

```text
+----------------------------+-----------------------------+---------------------------------------------+
| dimension                  | previous artifact overlay   | phase-17 pred-backtest harness              |
+----------------------------+-----------------------------+---------------------------------------------+
| portfolio path             | edits daily return stream    | reruns Qlib PortAnaRecord                   |
| turnover logic             | approximated by positions    | uses actual strategy turnover rules         |
| tradability/cost handling  | partly approximated          | uses Qlib exchange/executor config          |
| replacement selection      | approximated from pred.pkl   | produced by strategy after adjusted scores  |
| traceability               | hit_stats only               | rank-date + symbol-level materialized trace |
| runtime safety             | isolated                     | isolated                                    |
+----------------------------+-----------------------------+---------------------------------------------+
```

## Current Decision

```text
+-----------------------------------+-------------------------+--------------------------------------------------------------+
| item                              | decision                | reason                                                       |
+-----------------------------------+-------------------------+--------------------------------------------------------------+
| research-only materializer         | IMPLEMENTED             | enables copied-loop --pred-backtest without QE runtime edits |
| immediate QE runtime integration   | REJECT                  | Phase 16 robustness still too weak                           |
| DB policy persistence              | DEFER                   | wait for true rerun breadth and positive median              |
| next empirical step                | RUN_SMALL_TRUE_RERUN    | one copied loop first, then 22-loop batch if stable          |
| LLM/PDF preprocessing              | DEFER                   | structured financial signals still need stronger proof       |
+-----------------------------------+-------------------------+--------------------------------------------------------------+
```

## Example Command Shape

The exact paths must point to a copied loop workspace and generated overlay files.

```powershell
python -m backend.services.event_signal.financial_distress_pred_materializer `
  --prediction-pkl <copied-loop>/mlruns/<exp>/<run>/artifacts/pred.pkl `
  --overlay-csv <reports>/financial_distress_overlay.csv `
  --output-pkl <copied-loop>/event_signal_pred_backtest/adjusted_pred.pkl `
  --trace-csv <copied-loop>/event_signal_pred_backtest/trace.csv `
  --meta-json <copied-loop>/event_signal_pred_backtest/meta.json `
  --report-md <copied-loop>/event_signal_pred_backtest/report.md `
  --context-profile rank_decay_balanced `
  --top-k 50 `
  --ranking-date-mode previous
```

Then inside the copied loop workspace under the Qlib-capable environment:

```bash
python qrun_limit_minute.py conf.yaml --pred-backtest event_signal_pred_backtest/adjusted_pred.pkl
QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py
```

## Promotion Gate For The Next Empirical Phase

```text
+----------------------------+------------------------------+--------------------------------------------------------------+
| gate                       | required                     | reason                                                       |
+----------------------------+------------------------------+--------------------------------------------------------------+
| copied-loop smoke          | one loop completes           | proves harness can run real PortAnaRecord                    |
| metric parity              | baseline rerun close to old  | prevents workspace/config drift from being mistaken as alpha |
| true rerun average delta   | positive                     | must beat zero across loops                                  |
| true rerun median delta    | positive                     | fixes Phase 16 median-zero weakness                          |
| worst-loop tail            | no worse than Phase 16       | prevents hidden drawdown degradation                         |
| action density             | enough changed trades        | avoids decisions from one or two replacements                |
+----------------------------+------------------------------+--------------------------------------------------------------+
```

## Residual Risks

- The materializer is faithful to rank-order changes, but not a native strategy hook; score-weighted strategies may change sizing because scores are reassigned.
- A copied-loop baseline rerun is required before interpreting event-overlay deltas; otherwise differences may come from environment or Qlib data drift.
- This phase does not yet execute the full 22-loop true rerun batch; it creates the safe path and tested artifact generator.
- No conclusion from this phase permits hard buy ban, forced sell, Selection Center integration, Paper v2 integration, or DB policy persistence.
