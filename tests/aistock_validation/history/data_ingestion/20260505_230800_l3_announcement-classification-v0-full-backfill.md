# Announcement Classification v0 Full Backfill

- Date: 2026-05-05
- Module: local data management / announcements
- Level: L3
- Scope: metadata-only title classification and first-stage risk signal persistence
- Production port impact: no production backend restart; DB schema/data changes only

## Implemented Scope

- Created `market.ann_event_taxonomy`, `market.ann_rule_set`, `market.ann_event_classification`, and `market.ann_risk_signal`.
- All new tables and columns have PostgreSQL comments.
- Added deterministic `AnnouncementTitleClassifier` under backend services so backtest and live can share the same `rule_version`.
- Reworked `scripts/classify_announcement_titles_v0.py` to seed rule metadata, classify `market.anns`, persist one classification per announcement/rule version, and generate P0/P1/P2 risk signals.
- No PDF download and no LLM call were used.

## Data Backfill Result

- Rule version: `aistock_announcement_title_rules_v0_20260505`
- `market.anns` rows: `5,131,337`
- `market.ann_event_classification` rows for this version: `5,131,337`
- Classification coverage: `100%`
- `market.ann_risk_signal` rows for this version: `1,414,628`
- Taxonomy rows: `28`

Risk-level counts:

| risk_level | rows |
| --- | ---: |
| P4_NEUTRAL | 3,311,376 |
| P2_REVIEW | 1,299,530 |
| P3_POSITIVE_CANDIDATE | 405,333 |
| P1_HIGH | 89,648 |
| P0_BLOCK | 25,450 |

Risk signal counts:

| risk_level | action | rows |
| --- | --- | ---: |
| P2_REVIEW | warn_review | 1,299,530 |
| P1_HIGH | warn_high | 89,648 |
| P0_BLOCK | block_buy | 25,450 |

Source time quality:

| source_time_quality | rows |
| --- | ---: |
| EXACT | 4,894,061 |
| MISSING | 201,023 |
| MIDNIGHT_DEFAULT | 36,253 |

## Validation Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/announcements/title_classifier.py backend/db/init_announcement_event_schema.py scripts/classify_announcement_titles_v0.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/announcements/test_title_classifier.py backend/tests/test_announcement_event_schema.py backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s local_data_management_audit
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m backend.db.init_announcement_event_schema
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/classify_announcement_titles_v0.py --start-date 2018-08-01 --end-date 2026-05-05 --batch-size 20000 --persist --missing-only --json-out reports/anns/announcement_title_classification_v0_full_20260505.json --md-out docs/analysis/announcement_title_classification_v0_full_20260505.md
git diff --check -- <current announcement files>
```

## Validation Result

- Classifier/schema/scheduler pytest: `10 passed`.
- `local_data_management_audit`: passed, including dataset audit schema/comment smoke.
- DB comment check for announcement tables: `0` uncommented columns for all four new tables.
- Full backfill processed missing rows in `867.612s`; combined with smoke/perf rows, final DB coverage is `5,131,337 / 5,131,337`.
- Final DB summary written to `docs/analysis/announcement_title_classification_v0_db_summary_20260505.md`.

## Residual Risks

- v0 title rules are intentionally conservative but still contain false positives, especially broad financing/guarantee/shareholder-change categories.
- P2 rows are review candidates and should not become hard trading blocks until later PDF/LLM or event-study validation.
- P3 positive candidates are persisted in classification only; positive alpha remains disabled.
- The classifier is not yet wired into the hourly `anns_metadata` schedule; future incremental classification should process new `market.anns` rows after each metadata sync.
