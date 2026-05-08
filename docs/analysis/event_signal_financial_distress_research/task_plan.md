# Event Signal Financial Distress Research Task Plan

## Purpose

Maintain a restart-safe research trail for AIstock event-signal financial-distress studies. This is a research-only branch/worktree plan and must not be treated as runtime approval.

## Workspace Boundary

```text
Dedicated worktree : F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508
Branch             : codex/financial-distress-rerank-20260508
Remote branch      : origin/codex/financial-distress-rerank-20260508
Do not merge main  : true, unless the user explicitly asks
Production 8001    : do not restart or touch
Runtime code scope : do not modify QE/Paper/Selection/QMT runtime without explicit user confirmation
Research scope     : backend/services/event_signal, backend/tests/event_signal, docs/analysis, validation records
Generated reports  : reports/ is ignored; commit only curated docs and validation records
```

## Completed Phases

```text
+-------+------------------------------------------+----------+-------------------------------------------------------------+
| phase | topic                                    | status   | commit/docs                                                 |
+-------+------------------------------------------+----------+-------------------------------------------------------------+
| 1     | single-loop financial distress overlay   | complete | event_signal_financial_distress_qe_overlay_result_20260508  |
| 2     | multi-loop overlay validation            | complete | event_signal_financial_distress_multiloop_qe_overlay_result |
| 3     | market-cap size bucket split             | complete | event_signal_financial_distress_size_bucket_qe_overlay      |
| 4     | fixed score-down rerank overlay          | complete | d87e308 / score_down_qe_overlay_result                      |
| 5     | severity score-down overlay              | complete | 46fe636 / severity_qe_overlay_result                        |
| 6     | market-cap and industry exposure audit   | complete | 9c61508 / exposure_qe_overlay_result                        |
| 7     | rolling loss-history rules               | complete | bf67daa / loss_history_qe_overlay_result                    |
| 8     | market-cap bucket report for every signal| complete | market_cap_bucket_qe_overlay_result                         |
+-------+------------------------------------------+----------+-------------------------------------------------------------+
```

## Current Research Baseline

```text
+----------------------------------------+----------------------+------------------------------------------------+
| baseline                               | current conclusion   | use                                            |
+----------------------------------------+----------------------+------------------------------------------------+
| loss_to_market_cap_ge_50pct_mv_lt_10bn | strongest so far     | benchmark for every new financial-risk signal |
| fixed rank20 score-down                | simple stable mode   | baseline score-down simulation                |
| severity balanced score-down           | explainable variant  | compare with fixed rank20                     |
| loss-history-only                      | too broad standalone | feature candidate, not runtime rule           |
+----------------------------------------+----------------------+------------------------------------------------+
```

## Next Phases

```text
+-------+----------------------------------------------+------------+--------------------------------------------------------------+
| phase | next research                                 | status     | validation requirement                                       |
+-------+----------------------------------------------+------------+--------------------------------------------------------------+
| 9     | medium/large-cap event families               | pending    | test impairment, audit opinion, regulatory, debt, miss cases |
| 10    | loss history + industry + size interactions   | pending    | must beat or explain baseline without overfitting            |
| 11    | forecast/express/report mismatch research     | pending    | detect expectation miss, not raw growth/loss only            |
| 12    | LLM/PDF preprocessing design                  | deferred   | start only after structured signals show value               |
+-------+----------------------------------------------+------------+--------------------------------------------------------------+
```

## Operating Rules

- Commit after each meaningful research phase and push to `origin/codex/financial-distress-rerank-20260508`.
- Do not merge to `main` during research unless explicitly requested.
- Every committed phase needs both a curated analysis doc and a validation record.
- Every new signal must be validated on the same 10 QE loops before being considered for a signal table or runtime hook.
- Every report must compare against `loss_to_market_cap_ge_50pct_mv_lt_10bn` and show market-cap exposure.
- Financial distress signals remain research-only: no hard buy ban, no forced sell, no live/paper runtime connection.

## Resume Checklist

```text
1. cd F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508
2. git status --short --branch
3. git log --oneline -8
4. read docs/analysis/event_signal_financial_distress_research/task_plan.md
5. read docs/analysis/event_signal_financial_distress_research/findings.md
6. read docs/analysis/event_signal_financial_distress_research/progress.md
7. continue from the first pending phase
```
