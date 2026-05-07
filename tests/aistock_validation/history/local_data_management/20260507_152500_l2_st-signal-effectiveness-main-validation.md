# L2 ST Signal Effectiveness Validation After Main Merge - 2026-05-07

## Scope

- Merged `origin/codex/event-signal-st-llm-design-20260506` into local `main`.
- Backup branch created before merge: `backup/main-before-event-signal-merge-20260507_151859`.
- Merge commit message: `merge: event signal ST and PDF smoke`.
- Validated ST-first event signal effectiveness on `main` using historical local DB data from `2018-08-01` onward.
- Production port `8001` was not restarted.

## Commands And Results

- `git merge --no-ff origin/codex/event-signal-st-llm-design-20260506 -m "merge: event signal ST and PDF smoke"` -> completed with `ort`, no conflicts.
- `python -m pytest backend/tests/announcements/test_title_classifier.py backend/tests/event_signal -q` -> `108 passed in 1.44s`.
- `python -m compileall -q backend/services/event_signal backend/services/announcements/title_classifier.py backend/tests/event_signal backend/tests/announcements/test_title_classifier.py` -> passed.
- `python -m backend.services.event_signal.st_event_study --start-date 2018-08-01 --output-dir reports/event_signal/st_first` -> completed.

## Event Study Output

- Report id: `st_first_event_study_20180801_all_20260507_152007`.
- JSON: `reports/event_signal/st_first/st_first_event_study_20180801_all_20260507_152007.json`.
- CSV: `reports/event_signal/st_first/st_first_event_study_20180801_all_20260507_152007_details.csv`.
- Markdown: `reports/event_signal/st_first/st_first_event_study_20180801_all_20260507_152007.md`.
- Rule version: `unified_event_signal_rules_st_first_v1_20260506`.
- Signal rows: `12048`.
- Deduped events: `11048`.
- Detail rows: `55240`.
- Price window: `2018-07-31` to `2026-05-11`.

## Event Counts

| event_type | events |
| --- | ---: |
| stock_delisting_confirmed | 1648 |
| stock_delisting_risk_warning | 3732 |
| stock_st_added_or_continued | 902 |
| stock_st_imposed | 4085 |
| stock_st_removal_applied | 681 |

## Combined Hard-risk Metrics

Hard-risk group excludes `stock_st_removal_applied` and includes confirmed delisting, delisting risk warning, ST imposed, and ST added/continued.

| window | rows | valid | mean raw | median raw | mean abnormal | median abnormal | negative rate | down-limit | suspended | missing price |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| T-1 | 10367 | 4997 | -0.531% | -0.198% | -0.559% | -0.366% | 51.131% | 4.813% | 14.479% | 51.490% |
| T0 | 10367 | 4802 | -0.752% | -0.324% | -0.763% | -0.457% | 52.561% | 8.045% | 19.533% | 50.169% |
| T+1 | 10367 | 5059 | -0.386% | -0.053% | -0.420% | -0.250% | 50.010% | 7.485% | 15.549% | 49.079% |
| T+2 | 10367 | 5173 | -0.235% | 0.000% | -0.272% | -0.198% | 49.952% | 5.797% | 13.620% | 49.590% |
| T0_T2 | 10367 | 4594 | -0.735% | -0.404% | -0.798% | -0.511% | 53.309% | 12.781% | 23.556% | 52.204% |

## Key Event-type Metrics

| event_type | window | mean raw | median raw | mean abnormal | down-limit | suspended | missing price |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| stock_st_imposed | T0 | -1.334% | -0.573% | -1.354% | 13.439% | 22.154% | 34.786% |
| stock_st_imposed | T0_T2 | -1.562% | -0.825% | -1.639% | 20.881% | 25.581% | 38.311% |
| stock_st_added_or_continued | T0 | -0.127% | -0.309% | -0.259% | 10.200% | 19.290% | 26.275% |
| stock_st_added_or_continued | T0_T2 | 0.012% | -0.701% | 0.011% | 17.960% | 20.399% | 29.047% |
| stock_delisting_risk_warning | T0 | -0.277% | 0.000% | -0.258% | 4.957% | 10.316% | 61.468% |
| stock_delisting_risk_warning | T0_T2 | 0.051% | 0.000% | -0.042% | 7.958% | 16.720% | 62.138% |
| stock_delisting_confirmed | T0_T2 | 0.134% | 0.000% | 0.184% | 0.789% | 35.740% | 76.820% |
| stock_st_removal_applied | T0_T2 | 1.964% | 1.081% | 1.822% | 9.838% | 8.076% | 20.999% |

## Effectiveness Judgment

- `stock_st_imposed` is clearly effective as a risk-avoidance signal: T0 mean abnormal return is about `-1.354%`, T0_T2 mean abnormal return is about `-1.639%`, and T0_T2 down-limit rate reaches `20.881%`.
- `stock_st_added_or_continued` is directionally useful: median T0_T2 raw return is `-0.701%`, while down-limit/suspended rates are high, supporting risk review or block-buy policy.
- `stock_delisting_risk_warning` is still useful for risk avoidance even when average T0_T2 raw return is near zero, because missing-price, suspension, and down-limit rates are high; pure return metrics understate tradability and tail risk.
- `stock_delisting_confirmed` has very high missing-price/suspension rates. It should remain a direct no-buy/block signal; event-study price returns are not the right primary metric once trading is halted or delisting is near-confirmed.
- `stock_st_removal_applied` behaves differently from hard-risk events and shows positive T0_T2 returns. It should not be used as a hard block; keep it as review/positive-candidate only and require stricter confirmation before any alpha use.

## Decision

- Keep the first-stage policy: ST imposed, continued/added ST, delisting warning, and confirmed delisting can be generated from titles without downloading PDFs.
- Use these hard-risk ST signals as independent risk controls outside existing alpha factors.
- `stock_st_imposed` can be the first production-quality prohibition candidate after additional PIT/data-readiness checks.
- Do not yet connect this signal to Paper v2 or live trading in this phase.
