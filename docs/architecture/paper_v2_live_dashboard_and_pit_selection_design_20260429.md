# Paper v2 Live Dashboard and PIT Selection Design

Date: 2026-04-29

## 1. Scope

This design closes two Paper Trading v2 / Selection Center gaps:

1. Each running Paper v2 portfolio/session needs a Chinese, operator-friendly
   detail page showing the complete persisted chain from daily signal to target,
   order intent, minute execution, fills, NAV, positions, errors, and data
   freshness.
2. Selection Center needs point-in-time historical selection: choosing target
   trade date `D` must use data available no later than the previous trading day
   and produce stocks intended for trading on `D`.

The change is framework/UI/API only. It must not mutate StrategyPackage
manifests, model weights, HMM snapshots, QE/RD-Agent workspaces, validated
execution policies, or other persisted strategy assets.

## 2. Live Dashboard Contract

Route:

```text
/paper-v2/portfolios/{portfolioId}/live-dashboard
```

API:

```text
GET /api/v1/paper-v2/portfolios/{portfolio_id}/live-dashboard?trade_date=YYYY-MM-DD
GET /api/v1/paper-v2/portfolios/{portfolio_id}/minute-execution?trade_date=YYYY-MM-DD&symbol=&limit=500
GET /api/v1/paper-v2/portfolios/{portfolio_id}/intraday-snapshots?trade_date=YYYY-MM-DD&limit=500
```

The aggregate API is read-only. It must not tick sessions, start schedulers,
create orders, generate signals, or modify any portfolio state.

The response includes:

- portfolio and package identity;
- active session and any conflicting active sessions;
- current run and session days;
- scheduler status;
- data freshness: latest available bar, last processed bar, lag minutes;
- daily signal artifact metadata and Top candidates;
- target positions and order intents reconstructed from persisted run events;
- minute execution timeline from persisted order events, fills, and execution
  state;
- intraday NAV snapshots;
- positions, orders, fills, errors, run events, warnings.

Missing sub-data is represented as `status=MISSING` plus a human-readable
`missing_reason`. The API must not return empty arrays as fake success for a
missing signal, missing run, or missing execution history.

## 3. UI Rules

The live dashboard is an operator page:

- All labels and explanations are Chinese.
- Raw JSON is not shown on the page.
- `NO_FILL` reasons are translated to business explanations, for example:
  - `round_lot_zero`: 本分钟计划量不足 A 股最小交易单位，不能成交。
  - `limit_up_buy_blocked`: 涨停，买入受限。
  - `limit_down_sell_blocked`: 跌停，卖出受限。
  - `intraday_halt_or_no_bar`: 该分钟无有效行情，可能停牌或缺分钟线。
- The running monitor defaults to this dashboard when a user clicks a running
  portfolio.
- The page refreshes read-only data periodically and never triggers trading
  actions.

## 4. Intraday Snapshot Rule

Live session ticks should persist an intraday snapshot whenever a new completed
minute bar is processed, even if that minute produces no fill. If current
positions exist, valuation must use real observed/live prices. Missing prices
must fail fast; old prices or default prices must not be reused silently.

## 5. PIT Selection Semantics

Target date:

```text
target_trade_date = D
```

Authoritative historical selection mode:

```text
pit_mode = PREVIOUS_TRADING_DAY_CLOSE
cutoff_date = previous trading day before D
score_trade_date = cutoff_date
reference_price_trade_date = cutoff_date
```

Example:

```text
User selects target trade date 2026-03-10.
The system validates 2026-03-10 as a trading day.
The system resolves cutoff_date to 2026-03-09, or the nearest earlier trading day.
Model/factor inference may only use data <= cutoff_date.
The result remains a selection run for trade_date 2026-03-10.
```

Backend runtime config stores the resolved context:

```json
{
  "selection_artifact_config": {
    "auto_generate": true,
    "inference_backend": "wsl",
    "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
    "cutoff_date": "2026-03-09"
  },
  "point_in_time_context": {
    "trade_date": "2026-03-10",
    "cutoff_date": "2026-03-09",
    "score_trade_date": "2026-03-09",
    "reference_price_trade_date": "2026-03-09"
  }
}
```

## 6. PIT Fail-Fast Rules

Selection Center must fail if:

- target trade date is missing from the trading calendar;
- target trade date is not a trading day;
- no previous trading day exists in the lookup window;
- an explicit cutoff date is on or after the target trade date;
- live inference cannot enforce the cutoff date;
- generated score artifact is missing, non-authoritative, empty, or failed;
- enabled suspension filtering lacks `suspend_d` readiness for the target trade
  date;
- enabled HMM lacks coefficients for the target trade date;
- the runtime produces no candidates without an explicit `valid_no_candidate`
  state and reason.

No daily fallback, default price, default feature, or backtest prediction input
may be used as an authoritative live/PIT selection signal.

## 7. Verification

Required validation levels:

- backend unit tests for PIT cutoff resolution and artifact generation;
- backend unit tests for the live dashboard aggregation and minute reason labels;
- TypeScript compile check for the new UI route and API client;
- Paper v2 backend regression suite;
- Paper v2 Playwright UI suite on development ports only.

Development validation must use ports such as backend `8011/8012` and frontend
`3011/3012`; production backend `8001` must not be restarted by development
validation.

## 12. Implementation Validation Update - 2026-04-29

The implementation added one operational guard that is required for local validation and future multi-backend operation:

- Session tick processing must hold both the existing in-process lock and a PostgreSQL advisory lock keyed by `session_id` before it advances replay/live state. The DB lock is mandatory because production 8001, dev 8011/8012, and future scheduler processes share the same Paper v2 tables; an in-memory lock alone cannot prevent duplicate replay attempts from another process.
- A scheduler or backend process that cannot obtain the advisory lock must fail fast with a lock error and must not start replay, mutate the ledger, or create a duplicate run. This is not a silent fallback; it is a concurrency safety boundary.
- UI validation uses development ports only, but the local production scheduler may still be running against the same DB. Therefore E2E flows use isolated portfolios, short V25 replay cases, and the DB-backed tick lock to keep validation independent from production observation sessions.
- PIT selection validation must resolve `cutoff_date` through the backend calendar before running selection. The UI and tests must wait for long-running live StrategyPackage inference to settle before inspecting result tables; placeholder rows are not valid evidence.
- HMM validation must accept only two outcomes: a successful HMM-adjusted result with trace metadata, or an explicit fail-fast backend/UI error when the selected snapshot lacks stock-sector mapping or coefficient coverage. Neutral coefficients or silent HMM disablement are forbidden.
