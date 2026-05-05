# AIstock Announcement Event Risk Signal Top-Level Design

Date: 2026-05-04

## 1. Purpose

Build an announcement event framework that first improves risk warning and risk avoidance, then gradually supports research-only alpha candidates after event-study validation or model training.

The first implementation step is to complete historical announcement metadata from 2018-08-01 to the latest available trading date, without downloading all PDF files. The metadata history will support:

- title-based announcement classification;
- risk signal generation for backtests and live/paper trading;
- event-study analysis;
- future supervised model training and LLM-assisted document extraction.

## 2. Current Data Source Decision

`market.anns` already stores announcement metadata and local file metadata. The current Tushare `anns_d` interface is documented as returning announcement metadata plus original-document URL, with a single-call limit of 2000 rows and a separate permission requirement.

On 2026-05-04, the existing local ingestion script `scripts/ingest_tushare_anns_init.py` was tested for `2025-12-12`; the Tushare response was "no anns_d permission". This is a permission issue, not a frequency-limit issue. Therefore the current backfill attempt uses cninfo as a fallback source while preserving the same target table.

Source priority for production design:

1. Tushare `anns_d` when permission is available.
2. cninfo announcement query fallback when Tushare is unavailable.
3. Both sources must normalize into the same canonical announcement schema and write source/audit metadata.

## 3. Historical Sync Scope

Target range:

- Start date: 2018-08-01
- Latest trading date on current local calendar: 2026-04-30

Sync should be natural-day based because announcements can be disclosed on non-trading days. Trading calendar is used only for `effective_trade_date`.

First stage does not download PDF files. It only stores metadata:

- `ann_date`
- `ts_code`
- `name`
- `title`
- `url`
- `rec_time` if available
- source identifier and source URL when available in future schema
- local `available_at`
- calculated `effective_trade_date`

## 4. Backtest/Live Consistency Principle

There must be one canonical engine:

```text
AnnouncementSignalEngine
```

The same engine must run in:

- historical backfill/classification;
- event-study backtesting;
- Selection Center filtering;
- Paper Trading v2 readiness and daily runs;
- future live warning.

Avoid separate SQL rules for backtest and separate Python/frontend keyword rules for live mode.

Required invariant:

```text
same input + same rule_version/model_version/prompt_version => same event/risk output
```

## 5. Time Semantics

Announcement signals are event-time data, not ordinary daily factors. The following fields are mandatory for leakage control:

| Field | Meaning |
| --- | --- |
| `ann_date` | Announcement date from source |
| `rec_time` | Source disclosure/receive time, if available |
| `available_at` | Time AIstock locally obtained the record |
| `effective_trade_date` | First trade date when the signal can be used |

Recommended conservative rules:

- if time quality is `EXACT` and the announcement is available before the strategy decision time, signal can become effective on the same trading day;
- if time quality is `EXACT` but disclosed after market close, signal becomes effective on the next trading day;
- if `rec_time` is exactly `00:00:00`, treat it as `DATE_ONLY_OR_MIDNIGHT` by default and use next trading day unless another trusted source verifies that it was a real midnight disclosure;
- if `rec_time` is missing, signal becomes effective on the next trading day;
- if disclosed intraday but no minute-level event backtest is available, use next trading day in historical backtests.

Recommended `source_time_quality` values:

| Value | Meaning | Default effective-date rule |
| --- | --- | --- |
| `EXACT` | Non-midnight source timestamp that can be trusted as disclosure/receive time | Same day if before decision time, otherwise next trading day |
| `DATE_ONLY_OR_MIDNIGHT` | Source timestamp is `00:00:00` or date-only; it may be a true midnight disclosure but is not trusted by default | Next trading day unless cross-source verified |
| `MISSING` | No usable source time | Next trading day |
| `ESTIMATED` | Time inferred by AIstock rules or cross-source reconciliation | Use the estimate only with explicit quality flag |

## 6. Signal Position in Trading Stack

Announcement signals should be independent from existing alpha signals.

Recommended stack:

```text
Base alpha / StrategyPackage score
  -> Announcement Event Risk Overlay
  -> Tradability filters: suspend, limit, liquidity
  -> Portfolio constraints
  -> Orders
```

Announcement risk is primarily a gating/overlay layer:

- `BLOCK_BUY`: hard forbid new buys even if alpha is strong;
- `REDUCE_WEIGHT`: cap or reduce exposure;
- `WATCH`: alert only;
- positive event candidates are research-only until validated.

Announcement events can later be transformed into daily state features for research, but production risk actions should remain separate from alpha scoring.

## 7. Multi-Stage Filtering

### L0. Metadata Quality Layer

- source normalization;
- deduplication;
- stock market tagging;
- title cleanup;
- URL normalization;
- source count and sync audit.

### L1. Title Hard-Risk Rules

Title is sufficient to produce a P0/P1 signal. PDF/LLM is not required before action.

Examples:

- delisting risk warning;
- ST/*ST/other risk warning;
- termination of listing;
- administrative penalty decision;
- investigation notice;
- public condemnation / disciplinary sanction;
- debt overdue/default;
- bankruptcy/restructuring/liquidation;
- disclaimer/adverse/qualified audit opinion;
- non-standard audit opinion;
- major internal-control deficiency;
- fund occupation;
- illegal guarantee;
- criminal detention/illegal act.

### L2. Title Soft-Risk Candidates

Title identifies candidate risk but final severity needs document extraction or LLM.

Examples:

- litigation/arbitration;
- judicial freeze;
- pledge/re-pledge;
- reduction/passive reduction;
- guarantee/financial assistance;
- related-party transaction;
- performance forecast/revision;
- impairment;
- accounting correction;
- inquiry/concern letter and reply;
- M&A/restructuring/planning;
- asset sale/acquisition;
- financing/bonds/convertible bonds.

### L3. Neutral/Routine Drop or Low-Priority Archive

No PDF/LLM in the first stage.

Examples:

- routine shareholder meeting notice/resolution;
- routine board/supervisory board resolution;
- articles/system amendments;
- independent director annual report;
- legal opinions for routine meetings;
- dividend implementation;
- incentive participant lists;
- social responsibility/ESG reports.

### L4. Research-Only Positive Event Candidates

Do not add alpha in stage 1.

Examples:

- winning bid / pre-winning bid;
- major contract;
- production start/completion/expansion;
- buyback/increase holding;
- performance increase;
- subsidy;
- clinical trial / consistency evaluation;
- operating data such as throughput.

## 8. Warning Levels

| Level | Meaning | First-stage action |
| --- | --- | --- |
| P0 | Hard block | Forbid new buy; strong alert for existing positions |
| P1 | High risk | Forbid new buy or require manual confirmation; strong alert |
| P2 | Medium risk | Warning, weight cap/reduction candidate |
| P3 | Watch | Record event; no automatic trading impact |
| P4 | Neutral | Archive/drop; no alert |

Stage 1 enables risk warning and risk avoidance only. Positive gain is disabled until event-study or model validation.

## 9. LLM Usage Boundary

LLM is not a first-line signal generator and must not directly make buy/sell decisions.

LLM is used only for selected candidate events where title and rules cannot determine final severity:

- extract amount and ratios;
- identify subject: listed company, subsidiary, controlling shareholder, actual controller, director/supervisor/senior manager;
- classify event status: new, progress, resolved, clarification;
- extract evidence sentences;
- determine whether the issue may affect going concern or trigger ST/delisting conditions.

Expected structured output:

```json
{
  "event_status": "NEW",
  "risk_subject": "LISTED_COMPANY",
  "amount_cny": 320000000,
  "ratio_to_net_assets": 0.18,
  "is_resolved": false,
  "impact": "may affect going concern",
  "evidence_sentence": "..."
}
```

Trading action is still computed by deterministic rules over structured outputs.

## 10. Recommended Tables

Initial stage:

```text
market.ann_sync_audit
market.ann_event_taxonomy
market.ann_rule_set
market.ann_event_classification
market.ann_risk_signal
market.ann_event_feature_daily
```

Later stages:

```text
market.ann_text_extract
market.ann_llm_extract
market.ann_event_study_result
market.ann_live_alert
```

Any new table/column must include PostgreSQL comments per AIstock database schema standards.

## 11. Research and Institution Practices to Borrow

- Chinese stock announcement event classification: use fine-grained event types rather than generic sentiment; the referenced paper builds 54 event types from Chinese announcement news and explicitly warns that announcements should not be considered without other information such as industry and macro news.
- Event-study methodology: evaluate each event type with T+1/T+3/T+5/T+10/T+20 abnormal returns, drawdown, limit-down probability, suspension probability, and liquidity.
- Financial textual dictionaries: use finance-specific vocabulary; generic sentiment dictionaries misclassify financial text.
- MD&A financial-crisis early warning: text readability/similarity can add value to financial indicators, but model validation is required.
- SEC 8-K practice: treat material corporate events as a current-report taxonomy, not as free-form sentiment.
- Institutional news analytics practice: track event taxonomy, relevance, novelty, repetition, and ongoing/resolved status.
- ESG controversy practice: severe events can remain active over a long period; risk is a state, not only a one-day label.

## 12. Stage Plan

### Stage 1. Complete Metadata

- Backfill 2018-08-01 to latest available trading date.
- No full PDF download.
- Write source audit and coverage audit.
- Confirm row counts and missing dates.

### Stage 2. Title Rule Taxonomy v0

- Mine local `market.anns.title` distribution.
- Use the 54-type Chinese announcement paper as seed, not as final taxonomy.
- Add AIstock-specific risk types: ST/delisting, non-standard audit, debt default, fund occupation, illegal guarantee, internal-control deficiency.
- Generate persistent classification results with `rule_version`.

### Stage 3. Risk Signal v0

- P0/P1 produce `BLOCK_BUY`.
- P2 produce warning or weight-cap candidate.
- P3 watch only.
- P4 no action.

### Stage 4. Event Study and Portfolio Backtest

- Validate each event type.
- Risk avoidance first.
- Positive alpha remains disabled until statistically and economically validated.

### Stage 5. Selective PDF/LLM Extraction

- Only parse/LLM L2 candidate events and selected P1/P2 categories.
- Extract amounts, subjects, state, and evidence.
- Refine risk level and reduce false positives.

### Stage 6. Alpha Research

- Convert validated event types to daily state features.
- Export for research/model training.
- Enable positive gain only after out-of-sample validation.

## 13. Open Decisions

- Whether to formally keep B-share announcement metadata while excluding them from A-share trading signals.
- Whether to subscribe to Tushare `anns_d` separate permission or keep cninfo fallback as primary.
- Exact P0/P1 trading action for existing positions: alert only, reduce, or forced exit.
- Whether intraday announcement signals should be supported in minute-level backtests or conservatively delayed to next trading day.
- LLM provider/cost limit and whether local OCR/LLM is required.
