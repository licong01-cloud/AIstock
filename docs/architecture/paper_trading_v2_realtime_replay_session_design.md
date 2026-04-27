# Paper Trading v2 Real-Time Replay Session Detailed Design

Status: detailed design, not implemented in this document  
Date: 2026-04-27  
Scope: StrategyPackage -> Paper Trading v2 historical minute replay, real-time minute simulation, and catch-up-to-live sessions  
Out of scope: QMT, Shadow Trading, live brokerage, daily-bar fallback, backend/data_service semantic changes, QE business-logic changes

## 1. Purpose

This document designs the next Paper Trading v2 execution layer needed to support three authoritative modes:

1. `REPLAY_ONLY`: run a historical time range using completed historical minute bars.
2. `LIVE_ONLY`: run the current/future trading session incrementally as new minute bars arrive.
3. `CATCHUP_THEN_LIVE`: replay historical completed dates first, catch up to the latest available current-day minute, then automatically continue in live minute mode.

The design is intentionally strict:

- UI-configured features must map to real backend fields, APIs, persistence, and execution code.
- Unsupported or incomplete features must fail with structured errors.
- Waiting for a future minute bar is an explicit running state, not success and not fallback.
- No source fallback, algorithm fallback, daily fallback, default price, default cash, default position, empty-result success, or silent business-logic change is allowed.

This document does not change any business logic. It defines the implementation plan and acceptance criteria for future code work.

## 2. Source-Backed Current-State Findings

The design is based on the following local project sources:

- `docs/codex_project_memory.md`: Paper v2 already has StrategyPackage persistence, Selection Center, portfolio/run/order/fill/cash/position/snapshot persistence, runtime profiles, validated execution policies, replay reset, UI, and fail-fast rules.
- `docs/adr/0001-ai-stock-trading-core-direction.md`: StrategyPackage is the only new Selection Center and Paper v2 entry; current scope excludes QMT, Shadow, and live brokerage; minute execution is mandatory.
- `docs/architecture/trading_core_v2.md`: Trading Core v2 must fail fast, must not fake success, and must keep OMS/Ledger as the authoritative cash/position path.
- `docs/architecture/paper_trading_v2_top_level_design.md`: Paper v2 main flow is StrategyPackageRuntime -> TargetPosition -> OrderIntent -> RiskEngine -> OMS -> MinuteExecutionEngine -> Fill -> Ledger.
- `docs/contracts/strategy_package_manifest_v1.md`: `minute_execution_policy` is required; daily matching is disabled; missing minute data or unsupported execution behavior must fail.
- `docs/architecture/paper_trading_v2_runtime_profile_execution_policy_design.md`: HMM, industry blacklist, and execution policy selection are runtime choices outside the frozen manifest; Paper v2 must only use backtest-validated execution policies.
- `backend/services/paper_trading_v2/day_runner.py`: current `PaperTradingDayRunner.run_day()` is a strict single-day closed-run path. It creates one run per portfolio/date, loads all required inputs, executes orders with `MinuteExecutionEngine.execute_order()`, persists ledger artifacts, and marks the run succeeded or failed.
- `backend/services/paper_trading_v2/replay.py`: current historical replay loops over `run_day()` and currently requires `DB_HISTORICAL`.
- `backend/services/paper_trading_v2/market_data.py`: current provider supports explicit `TDX_REALTIME` and `DB_HISTORICAL`, rejects unsupported sources, requires pre-close/limit data, and does not silently fallback between sources.
- `backend/services/trading_core/minute_execution.py`: current engine executes over a supplied list of minute bars, has no daily fallback, and fails when no fills are produced.
- `backend/services/trading_core/execution_algo_capabilities.py`: current capability metadata contains one `min_required_bars` value and runtime asset requirements; this must be split for historical and live semantics.
- `backend/execution_algos/v25_two_stage_algo.py`: current V25 adapter path directly imports Torch and currently rejects fewer than 240 bars. That behavior is valid for full-day replay only unless a real-time adapter contract is added.
- `backend/routers/paper_trading_v2.py`: current API exposes portfolios, run-day, readiness, replay, execution policy activation, ledger views, run events, and errors.

## 3. Key Correction: V25 240 Bars

The 240-bar requirement must not be interpreted as a real-time pre-open startup requirement.

Correct interpretation:

- In historical replay, a full trading-day execution algorithm can require the full expected minute set, for example 240 one-minute bars.
- In real-time trading, the system must process minute bars incrementally as they arrive.
- If an algorithm needs a full-day future bar array to decide the first live minute, that algorithm is not real-time safe and must fail with `ALGO_REALTIME_UNSUPPORTED`; it must not downgrade to TWAP, close price, daily execution, or any other fallback.
- For V25, `240` should be modeled as `historical_min_required_bars` or `plan_horizon`, not as `live_min_start_bars`.

Required V25 real-time contract:

```text
V25 realtime-safe adapter
  input: bars observed so far, current bar, static day context, persisted algo state
  output: current-minute step fill recommendation or updated execution plan state
  forbidden: reading future minute prices/volume that are not yet available in live mode
```

If the current V25 artifact cannot satisfy this contract, Paper v2 must reject live sessions for V25 with a structured error. It may still run historical replay if the full-day replay contract is satisfied.

## 4. Non-Negotiable Invariants

### 4.1 No Silent Fallback

Forbidden:

- `TDX_REALTIME` silently falling back to `DB_HISTORICAL`.
- `DB_HISTORICAL` silently falling back to TDX.
- V25 silently falling back to TWAP, VWAP, close-price, or daily execution.
- Missing price silently using pre-close, zero, last close, or arbitrary defaults.
- Missing signal artifact silently using QE backtest `pred.pkl`.
- Missing HMM/industry/suspend/limit/calendar data silently treating rows as tradable.
- API returning `ok=true` when the requested business action did not actually run.

### 4.2 Explicit Waiting Is Allowed

Real-time systems spend most of their time waiting. Waiting is allowed only when it is modeled explicitly:

```text
session_status = LIVE_WAITING_FOR_BAR
last_processed_bar_time = 2026-04-27T10:14:00+08:00
next_expected_bar_time = 2026-04-27T10:15:00+08:00
reason = "next minute bar has not completed yet"
```

This state is neither success nor failure. If the bar should already be available and remains unavailable beyond the configured grace window, the session must fail with a data-late or data-missing error.

### 4.3 UI Options Must Be Backed By Backend Reality

The UI must not display selectable options unless the backend supports them through:

- a typed API request field;
- server-side validation;
- persisted run/session context;
- service implementation;
- error propagation and UI display.

If an option exists only in a mock, local React state, or display label, it must be removed or disabled with an explicit unsupported reason.

### 4.4 Existing Business Logic Must Not Be Mutated By Fallback

This design does not permit changing strategy, selection, execution, cash, position, or performance semantics just to make a flow appear successful. Future implementation must add explicit session orchestration around existing strict components rather than weakening them.

## 5. Domain Concepts

### 5.1 Portfolio

`paper_v2.portfolio` remains the account container and freezes strategy package invariants:

- `package_id`
- `manifest_sha256`
- `initial_cash`
- `start_date`
- default `data_source`
- fee/risk/execution policy snapshots

Portfolio is not a run scheduler by itself.

### 5.2 Session

A `PaperTradingSession` is the long-running command that performs replay, live execution, or catch-up-to-live.

Examples:

- Replay the last 10 trading days and stop.
- Start live mode at today's next available minute.
- Reset portfolio runs, replay from a historical date, then switch to live when caught up.

A session must be durable. It must survive backend restart and resume from persisted cursor/state without duplicating fills.

### 5.3 Run

`paper_v2.run` remains the day-level authoritative ledger grouping. The current closed-day `run_day()` model is valid for completed historical days.

For live sessions, a run may be open intraday. It must not be marked `SUCCEEDED` until the day is finalized after market close and all required snapshots are persisted.

### 5.4 OrderExecutionState

`OrderExecutionState` is a new persistent object storing the incremental execution state for each order:

- current order status and filled quantity;
- remaining quantity;
- last processed bar time;
- execution algorithm state JSON;
- algorithm plan hash or plan payload when required;
- idempotency keys for generated fills/events.

Without this object, the system cannot safely resume a real-time session or process one minute at a time.

### 5.5 Minute Cursor

A minute cursor records how far a session has processed:

```text
session_id
portfolio_id
trade_date
symbol or "*"
last_seen_bar_time
last_processed_bar_time
latest_available_bar_time
cursor_source
updated_at
```

The cursor prevents duplicate execution when `tick` is called repeatedly.

## 6. Run Modes

### 6.1 `REPLAY_ONLY`

Purpose: deterministic historical Paper v2 replay.

Rules:

- Uses only an explicitly selected historical data source.
- Requires completed trading days.
- Requires full-day data completeness according to the execution policy.
- May reuse the current closed-day `PaperTradingDayRunner` for fully completed dates.
- Stops after `end_date`.
- Returns success only after all requested trading days have successful persisted runs.

Recommended allowed sources:

- `DB_HISTORICAL`
- optional future `TDX_HISTORICAL`, only after a real backend provider is implemented and exposed explicitly

### 6.2 `LIVE_ONLY`

Purpose: current/future real-time Paper v2 operation.

Rules:

- Uses only a real-time source, currently `TDX_REALTIME`.
- Creates or resumes the current trade date's open run.
- Builds signal/targets/order intents at the configured daily cutoff.
- Processes only completed minute bars available at tick time.
- Does not require 240 bars at market open.
- Emits `LIVE_WAITING_FOR_BAR` when the next bar is not yet complete.
- Fails if the selected execution algorithm is not real-time safe.

### 6.3 `CATCHUP_THEN_LIVE`

Purpose: start from a historical point, replay to the latest available minute, then continue live without manual intervention.

Rules:

1. Replay completed historical trading days with historical source.
2. If current date is a trading day, process available current-day bars incrementally up to the latest completed minute.
3. Switch phase to live waiting/running after the session cursor catches up to the current latest available minute.
4. Continue through the real-time source for later ticks.
5. If current date is not a trading day or market is closed, enter `LIVE_WAITING_NEXT_TRADING_DAY` after replaying the latest completed trading day.

No source is implied. The request must explicitly choose:

```json
{
  "historical_data_source": "DB_HISTORICAL",
  "live_data_source": "TDX_REALTIME"
}
```

## 7. Data Source Contract

Current code has:

```text
MinuteDataSource.DB_HISTORICAL
MinuteDataSource.TDX_REALTIME
```

The session design needs source roles rather than one ambiguous source field:

```text
historical_data_source: DB_HISTORICAL | TDX_HISTORICAL
live_data_source: TDX_REALTIME
```

Implementation rule:

- If `TDX_HISTORICAL` is not implemented, backend validation must reject it.
- UI must not expose `TDX_HISTORICAL` as selectable until backend supports it.
- `TDX_REALTIME` must not be used as a fake historical source unless its provider explicitly supports historical date-range loading and the enum/API says so.
- `DB_HISTORICAL` must not be used as a fake live source.

## 8. Minute Feed Interface

The current `load_symbol_input()` API is sufficient for one-shot day execution, but real-time sessions need a feed interface:

```python
class PaperV2MinuteFeed:
    def load_completed_day(symbol, trade_date, source, expected_bars) -> MinuteExecutionMarketInput:
        ...

    def load_observed_intraday(symbol, trade_date, source, until_time) -> MinuteExecutionMarketInput:
        ...

    def load_new_bars(symbol, trade_date, source, after_time, until_time) -> list[MinuteBar]:
        ...

    def latest_available_bar_time(symbols, trade_date, source, as_of_time) -> datetime | None:
        ...
```

Required behavior:

- `load_completed_day` fails if the full historical day is incomplete.
- `load_observed_intraday` returns only observed bars and never fabricates future bars.
- `load_new_bars` returns only bars later than the cursor and up to the latest completed minute.
- `latest_available_bar_time` returns `None` only when no bar is expected yet or the source has no data; the caller decides whether this is waiting or failure based on market time and grace rules.

## 9. Execution Algorithm Capability Split

The current single `min_required_bars` capability is insufficient. It must be split:

```python
class ExecutionAlgoCapability:
    algo_code: str
    historical_min_required_bars: int
    historical_requires_full_day: bool
    live_supported: bool
    live_min_start_bars: int
    live_step_mode: "streaming_step" | "persisted_plan" | "close_bar_only"
    plan_horizon_bars: int | None
    runtime_asset_keys: tuple[str, ...]
```

Suggested initial capability table:

| Algo | historical requirement | live requirement | live status |
| --- | --- | --- | --- |
| `CLOSE_PRICE` | completed close bar | close bar only | supported if it was backtest-validated |
| `TWAP` | complete historical interval | first executable bar and schedule config | supported if backtest-validated |
| `VWAP` | complete historical interval or streaming estimator | first executable bar and streaming volume model | supported only if adapter proves no future volume lookahead |
| `POV` | completed interval volume | first executable bar and current bar volume | supported if adapter uses observed volume only |
| `V24_PLAN` | existing adapter requirement | adapter-specific | live only if explicitly declared real-time safe |
| `V25_TWO_STAGE` | 240 bars or full historical day | 1 observed executable bar if real-time adapter exists | unsupported in live until adapter proves no future-bar use |

If an algorithm is configured in a validated execution policy but does not support the selected run mode, the session must fail with `ALGO_MODE_UNSUPPORTED`.

## 10. V25 Real-Time Contract

V25 must be integrated under the same capability system as every other QE minute execution strategy.

### 10.1 Historical Replay

Historical V25 replay may require:

- exactly the expected full-day minute array;
- model assets reachable from the backend environment;
- Torch/CUDA availability if the adapter imports Torch;
- strict market context fields such as previous close, limit up/down, and day features.

Failure examples:

- fewer than required historical bars -> `MINUTE_BAR_INCOMPLETE`
- missing model file -> `ALGO_ASSET_MISSING`
- Torch unavailable -> `ALGO_DEPENDENCY_MISSING`

### 10.2 Real-Time Execution

Real-time V25 must not read future bars. There are two acceptable designs:

1. Streaming-step V25:
   - Each tick calls V25 with observed bars and persisted state.
   - V25 returns a current-minute participation decision.
2. Persisted-plan V25:
   - V25 creates a plan using only information available at plan creation time.
   - The plan and plan hash are persisted.
   - Later ticks consume the persisted plan without re-reading future bars.

Forbidden:

- generating the live plan from full-day future prices or volumes;
- using DB historical bars for the current day after the user selected live TDX;
- falling back to TWAP when V25 cannot run.

### 10.3 V25 240-Bar Meaning

For V25:

```text
historical_min_required_bars = 240
plan_horizon_bars = 240
live_min_start_bars = 1 only if realtime adapter is implemented
```

If real-time adapter is not implemented:

```text
live_supported = false
error_code = ALGO_REALTIME_UNSUPPORTED
```

## 11. Session Lifecycle

### 11.1 Status

```text
CREATED
PREFLIGHTING
REPLAYING
CATCHING_UP
SWITCHING_TO_LIVE
LIVE_RUNNING
LIVE_WAITING_FOR_BAR
LIVE_WAITING_NEXT_TRADING_DAY
PAUSED
STOPPING
STOPPED
SUCCEEDED
FAILED
```

Rules:

- `SUCCEEDED` is valid only for bounded sessions such as `REPLAY_ONLY`.
- Long-running live sessions normally stay in a live/waiting status.
- `FAILED` must include `error_code`, `message`, and context.
- `PAUSED` must stop processing new ticks but must not delete state.

### 11.2 Phase

`phase` is more specific than status:

```text
historical_replay
current_day_catchup
live_intraday
day_finalization
waiting_next_day
```

### 11.3 Day Finalization

After market close and after all expected bars are available:

1. Verify every open order has a modeled final state.
2. Persist cash ledger, positions, daily snapshot, and performance inputs.
3. Mark day run `SUCCEEDED` only if all required artifacts exist.
4. Enter `LIVE_WAITING_NEXT_TRADING_DAY` for long-running sessions.

## 12. Persistence Design

### 12.1 `paper_v2.trade_session`

```text
session_id text primary key
portfolio_id text not null references paper_v2.portfolio(portfolio_id)
mode text not null
status text not null
phase text not null
start_date date not null
end_date date null
historical_data_source text null
live_data_source text null
runtime_config_json jsonb not null
validated_execution_policy_json jsonb not null
created_by text null
created_at timestamptz not null
updated_at timestamptz not null
started_at timestamptz null
completed_at timestamptz null
last_error_json jsonb null
```

### 12.2 `paper_v2.session_day`

```text
session_day_id text primary key
session_id text not null references paper_v2.trade_session(session_id)
portfolio_id text not null
trade_date date not null
run_id text null references paper_v2.run(run_id)
status text not null
phase text not null
data_source text not null
expected_bar_count int null
latest_available_bar_time timestamptz null
last_processed_bar_time timestamptz null
created_at timestamptz not null
updated_at timestamptz not null
unique(session_id, trade_date)
```

### 12.3 `paper_v2.order_execution_state`

```text
execution_state_id text primary key
session_id text not null references paper_v2.trade_session(session_id)
run_id text not null references paper_v2.run(run_id)
order_id text not null
symbol text not null
trade_date date not null
algo_code text not null
algo_state_json jsonb not null
plan_json jsonb null
plan_sha256 text null
last_processed_bar_time timestamptz null
filled_quantity int not null
remaining_quantity int not null
status text not null
created_at timestamptz not null
updated_at timestamptz not null
unique(order_id)
```

### 12.4 `paper_v2.intraday_snapshots`

```text
snapshot_id text primary key
session_id text not null
run_id text not null
portfolio_id text not null
trade_date date not null
snapshot_time timestamptz not null
cash numeric not null
market_value numeric not null
nav numeric not null
positions_json jsonb not null
source text not null
created_at timestamptz not null
unique(run_id, snapshot_time)
```

### 12.5 `paper_v2.session_events`

```text
event_id bigserial primary key
session_id text not null
run_id text null
event_type text not null
message text not null
context jsonb not null default '{}'
created_at timestamptz not null default now()
```

### 12.6 `paper_v2.run` Extensions

Future implementation may add:

```text
session_id text null
run_kind text not null default 'closed_day'
opened_at timestamptz null
finalized_at timestamptz null
latest_processed_bar_time timestamptz null
```

If modifying existing tables is riskier than adding side tables, `session_day` may store the mapping instead. The acceptance requirement is traceability and idempotent resume, not a specific physical layout.

## 13. Service Design

### 13.1 `PaperTradingSessionService`

Responsibilities:

- create sessions;
- validate mode/source/date combinations;
- freeze runtime profile and active validated execution policy for the session;
- reject unsupported UI options;
- expose session detail/progress;
- pause/resume/stop sessions.

Create flow:

```text
validate portfolio READY or allowed live state
validate mode
validate historical/live source compatibility
validate execution policy supports mode
validate runtime profile
preflight data readiness for first required date
persist trade_session
persist SESSION_CREATED event
```

### 13.2 `PaperTradingSessionRunner.tick(session_id, as_of_time)`

Responsibilities:

- acquire a session lock;
- load current session state and cursor;
- decide the next phase;
- process a bounded unit of work;
- persist state and events;
- return progress.

`tick` must be idempotent. Repeated calls with no new bars must not create duplicate orders, fills, cash ledger rows, or snapshots.

### 13.3 `PaperTradingLiveMinuteExecutor`

Responsibilities:

- open or resume the current day's run;
- generate signal/targets/order intents once per trade date according to cutoff rules;
- create OMS orders once per intent;
- create `order_execution_state`;
- process new minute bars incrementally;
- persist fills/events/cash/position updates;
- update intraday snapshots.

### 13.4 `PaperTradingDayFinalizer`

Responsibilities:

- detect market close and completed data availability;
- finalize open live day;
- persist daily snapshot;
- mark day run succeeded or failed;
- move session to next day waiting state.

### 13.5 Existing `PaperTradingDayRunner`

The current `PaperTradingDayRunner` remains useful for completed historical days. It must not be used to fake real-time execution by running a partial current day and marking the day successful.

## 14. Incremental Execution Contract

Future `MinuteExecutionEngine` must add an incremental API rather than weakening the current one-shot API:

```python
def execute_order_incremental(
    *,
    order: Order,
    execution_state: OrderExecutionState,
    new_bars: list[MinuteBar],
    algo_code: str,
    algo_config: dict[str, Any],
    market_context: dict[str, Any],
) -> tuple[Order, OrderExecutionState, list[Fill], list[OrderEvent]]:
    ...
```

Required semantics:

- `new_bars` must be strictly after `execution_state.last_processed_bar_time`.
- Algorithm state must be updated and persisted after each processed bar.
- Fill idempotency must be guaranteed by `order_id + bar_time + fill sequence`.
- If an algorithm returns no fill for a bar, that can be a valid no-fill event only if the algorithm explicitly models it; it must not mark the whole order/day successful prematurely.
- If an order remains partially filled at close, the outcome must follow the backtest-validated policy. If no validated policy exists for tail handling, fail.

## 15. Signal And Selection Cutoff Rules

Real-time Paper v2 must avoid lookahead bias.

### 15.1 Daily Signal Generation

For a normal trading day:

- selection artifacts should be generated before open using data available by the configured cutoff;
- if the signal is based on T-1 close/fundamentals, use latest completed data readiness date;
- if same-day pre-open suspend data is known, tradability filtering may remove confirmed suspended stocks and backfill from lower-ranked candidates;
- if data readiness is missing, fail before creating orders.

### 15.2 Intraday Execution

Once targets/order intents are created for a trade date:

- live minute execution must not recompute selection on every bar unless a future backtest-validated intraday-rebalance strategy explicitly says so;
- known post-signal suspension/limit conditions must be handled by risk/execution state, not hidden by changing the signal history;
- unfilled or rejected orders must be traceable with reasons.

### 15.3 Suspended Candidates

If confirmed suspension data is available at signal time:

- exclude suspended candidates from final tradable candidates;
- backfill from lower-ranked candidates;
- persist excluded rows and reasons.

If suspension is announced after signal generation:

- do not rewrite the historical signal silently;
- execution/risk must reject or leave unfilled with explicit reason;
- optional future replacement/backfill must be a backtest-validated rule, not a paper-only behavior.

## 16. Readiness Gates

### 16.1 Session Creation Readiness

Required:

- portfolio exists and is in an allowed lifecycle state;
- frozen package manifest hash matches portfolio;
- package is Paper-enabled;
- runtime profile parses with no unknown keys;
- execution policy is backtest-validated and enabled for Paper v2;
- selected algorithm supports requested run mode;
- source combination is explicit and supported.

### 16.2 Historical Day Readiness

Required:

- trading calendar says the date is a trading day;
- `suspend_d` refresh audit success when suspend filtering/status is required;
- `stk_limit` refresh audit success when limit/pre-close is required;
- historical minute bars are complete for every traded symbol;
- required model/runtime assets exist;
- signal artifact is authoritative live/latest-data scope, not diagnostic backtest scope.

### 16.3 Live Day Readiness

Required before creating orders:

- trading calendar available;
- pre-close and limit data available;
- suspend data availability satisfies the runtime profile;
- selection artifact can be generated with data available at cutoff;
- algorithm supports live mode;
- TDX real-time source is reachable if live source is `TDX_REALTIME`.

Required per tick:

- new bars are from the selected source;
- bars are strictly increasing;
- bar times are not in the future;
- missing bars are either explicit waiting or explicit failure after grace period.

## 17. Error Model

All errors must include:

```json
{
  "error_code": "STRING",
  "message": "human readable message",
  "context": {
    "session_id": "...",
    "portfolio_id": "...",
    "trade_date": "YYYY-MM-DD"
  }
}
```

Suggested new error codes:

| Code | Meaning |
| --- | --- |
| `SESSION_CONFIG_INVALID` | invalid mode/source/date/runtime profile combination |
| `SESSION_SOURCE_UNSUPPORTED` | selected source is not implemented for that role |
| `SESSION_ALREADY_RUNNING` | conflicting active session exists for the same portfolio |
| `SESSION_LOCK_TIMEOUT` | runner could not acquire processing lock |
| `ALGO_MODE_UNSUPPORTED` | execution algorithm does not support replay/live mode |
| `ALGO_REALTIME_UNSUPPORTED` | algorithm is historical-only under current adapter |
| `ALGO_STATE_MISSING` | incremental execution state is required but missing |
| `ALGO_DEPENDENCY_MISSING` | runtime dependency such as Torch is missing |
| `ALGO_ASSET_MISSING` | model/runtime asset path is missing or unreadable |
| `MINUTE_BAR_WAITING` | no new bar yet; status, not failure |
| `MINUTE_BAR_LATE` | expected live bar did not arrive within grace window |
| `MINUTE_BAR_INCOMPLETE` | historical day does not have required bars |
| `SIGNAL_CUTOFF_UNREADY` | selection cannot run because required source data is not ready |
| `DAY_FINALIZATION_FAILED` | close-of-day snapshot/finalization incomplete |

`MINUTE_BAR_WAITING` should normally be represented as session status and event, not HTTP error, unless the API call explicitly requests strict immediate execution.

## 18. API Design

All new APIs live under `/api/v1/paper-v2` and must return structured fail-fast errors through the existing `TradingCoreError` mapping pattern.

### 18.1 Create Session

```text
POST /api/v1/paper-v2/portfolios/{portfolio_id}/sessions
```

Request:

```json
{
  "mode": "CATCHUP_THEN_LIVE",
  "start_date": "2026-04-15",
  "end_date": null,
  "historical_data_source": "DB_HISTORICAL",
  "live_data_source": "TDX_REALTIME",
  "runtime_config": {
    "runtime_profile": {
      "tradability": {"exclude_suspended": true},
      "industry_blacklist": [],
      "hmm": {"enabled": false},
      "selection": {"top_k": 20}
    }
  },
  "rerun_policy": "reject_existing",
  "confirm_reset": false,
  "confirm_text": null,
  "created_by": "ui"
}
```

Validation:

- `REPLAY_ONLY` requires `historical_data_source`.
- `LIVE_ONLY` requires `live_data_source`.
- `CATCHUP_THEN_LIVE` requires both.
- `reset_portfolio` requires explicit confirmation.
- unsupported sources fail.
- unsupported algorithm/mode combinations fail.

### 18.2 List Sessions

```text
GET /api/v1/paper-v2/portfolios/{portfolio_id}/sessions
```

### 18.3 Get Session Detail

```text
GET /api/v1/paper-v2/sessions/{session_id}
```

### 18.4 Get Progress

```text
GET /api/v1/paper-v2/sessions/{session_id}/progress
```

Response includes:

```json
{
  "session_id": "...",
  "status": "LIVE_WAITING_FOR_BAR",
  "phase": "live_intraday",
  "current_trade_date": "2026-04-27",
  "last_processed_bar_time": "2026-04-27T10:14:00+08:00",
  "next_expected_bar_time": "2026-04-27T10:15:00+08:00",
  "latest_available_bar_time": "2026-04-27T10:14:00+08:00",
  "orders": [],
  "fills_today": [],
  "last_error": null
}
```

### 18.5 Manual Tick

```text
POST /api/v1/paper-v2/sessions/{session_id}/tick
```

Purpose:

- allows UI/background validation without waiting for scheduler;
- idempotently processes one bounded unit of work.

### 18.6 Pause, Resume, Stop

```text
POST /api/v1/paper-v2/sessions/{session_id}/pause
POST /api/v1/paper-v2/sessions/{session_id}/resume
POST /api/v1/paper-v2/sessions/{session_id}/stop
```

Rules:

- pause/resume must not delete state;
- stop must mark session stopped and leave historical artifacts intact;
- reset/delete remains a separate explicit audited replay action.

## 19. Scheduler Design

The UI must not be required to stay open for live sessions.

Scheduler responsibilities:

- find active sessions in live/waiting/catchup statuses;
- call `tick(session_id)` on a safe interval;
- avoid port 8001 restart assumptions;
- persist scheduler errors as session events/errors;
- use locks so concurrent UI tick and scheduler tick do not duplicate work.

Suggested intervals:

- during trading hours: every 15-30 seconds or just after expected minute completion;
- lunch break: slower interval;
- outside trading hours: next-day waiting interval;
- catch-up mode: fast loop with bounded batch size until live cursor is reached.

The scheduler interval is not a trading algorithm parameter and must not change business semantics.

## 20. UI Contract

The Paper v2 UI must be able to complete the real flow with real backend APIs.

### 20.1 Required UI Controls

- strategy package selector;
- portfolio selector/current running portfolios;
- initial cash at portfolio creation;
- runtime profile:
  - top K;
  - suspend filtering;
  - industry blacklist;
  - HMM enabled flag, model snapshot, signal preset;
- validated execution policy selector;
- mode selector:
  - historical replay only;
  - live only;
  - catch up then live;
- historical start/end date;
- historical source;
- live source;
- reset confirmation only when reset is selected;
- start session, pause, resume, stop, manual tick;
- progress and error panel.

### 20.2 UI Must Not Do

- submit `algo_code` directly as a paper-only override;
- show an algorithm not returned by backend as paper-enabled and mode-compatible;
- show HMM snapshot choices that backend cannot validate;
- hide `error_code`, message, or context;
- mark a live session completed just because the latest tick had no new bar;
- allow catch-up-to-live without explicit historical and live data sources.

### 20.3 Display Requirements

The session page should display:

- current status/phase;
- selected data sources;
- selected validated execution policy and hash;
- model freshness warning;
- HMM snapshot/preset;
- industry blacklist;
- current trade date;
- latest available minute and last processed minute;
- orders/fills/positions/cash/NAV;
- unfilled/rejected reasons;
- session events and persisted errors.

## 21. Implementation Phases

### Phase 0: Documentation And Baseline

Deliverables:

- this document;
- no backend/frontend business logic changes.

Verification:

- `git diff --check` for the new document.

### Phase 1: Session Persistence And Models

Implement:

- session models;
- schema/migration for session tables;
- repository methods;
- tests for idempotent create/update and status transitions.

Anti-pattern guards:

- do not delete old runs without explicit reset;
- do not reuse portfolio status as session status;
- do not mark live session success at creation.

### Phase 2: Source Role And Minute Feed

Implement:

- source-role validation;
- feed methods for completed day, observed intraday, and new bars;
- live waiting vs late/missing bar classification.

Anti-pattern guards:

- no DB/TDX silent fallback;
- no future bar fabrication;
- no `TDX_HISTORICAL` UI exposure before backend support.

### Phase 3: Execution Capability Split

Implement:

- historical/live capability fields;
- mode compatibility validation;
- V25 historical/live distinction;
- tests for unsupported live algorithms.

Anti-pattern guards:

- no generic default that treats every algorithm as live-safe;
- no V25-to-TWAP fallback;
- no 240-bar live-open gate unless algorithm explicitly requires it and is declared live-unsupported.

### Phase 4: Incremental Live Execution

Implement:

- order execution state;
- incremental minute execution API;
- idempotent fill/event persistence;
- intraday snapshots;
- day finalization.

Anti-pattern guards:

- no partial current day marked `SUCCEEDED`;
- no duplicate fills on repeated ticks;
- no silently ignored algorithm state errors.

### Phase 5: Session API And Scheduler

Implement:

- session create/list/detail/progress/tick/pause/resume/stop APIs;
- scheduler loop;
- structured error mapping.

Anti-pattern guards:

- no background exception swallowed as success;
- no UI-only state not persisted in session tables;
- no hard dependency on production port restart for validation.

### Phase 6: UI Wiring

Implement:

- session creation and progress UI;
- real backend option loading;
- error display;
- manual tick and scheduler status display.

Anti-pattern guards:

- no fake options;
- no hidden fallback;
- no success toast for failed backend action.

## 22. Test Matrix

### 22.1 Backend Unit Tests

- session mode/source validation;
- session lifecycle transitions;
- reset confirmation required;
- source fallback rejection;
- algorithm mode compatibility;
- V25 historical accepted only when assets/dependencies/bars exist;
- V25 live rejected when real-time adapter is not declared live-safe;
- waiting for bar does not create failure or success;
- late bar creates structured failure.

### 22.2 Repository Tests

- session create/list/detail;
- session day cursor updates;
- order execution state save/load;
- idempotent fill insert;
- intraday snapshot uniqueness;
- session events/errors persistence.

### 22.3 Integration Tests

- `REPLAY_ONLY` uses closed-day runner and persists final snapshots.
- `LIVE_ONLY` processes one new bar per tick and persists execution state.
- repeated tick with no new bars creates no duplicate fills.
- `CATCHUP_THEN_LIVE` replays historical days, catches current-day cursor, and enters live waiting.
- missing pre-close/limit/suspend/calendar fails before order execution.
- missing minute bars in historical replay fails.
- unsupported live algorithm fails before order creation.

### 22.4 API Tests

- create session success/failure;
- progress response includes cursor and current phase;
- tick is idempotent;
- pause/resume/stop transitions;
- structured errors are passed through.

### 22.5 UI/E2E Tests

Use non-production ports only, for example backend `8012` and frontend `3012`.

Required browser flow:

1. create/select a StrategyPackage-backed portfolio;
2. choose runtime profile, HMM/industry options, and validated execution policy from real backend data;
3. start `REPLAY_ONLY`;
4. start `CATCHUP_THEN_LIVE`;
5. verify progress status, events, orders/fills, NAV, and errors;
6. verify unsupported options display backend errors and do not show fake success.

## 23. Acceptance Criteria

1. UI can create `REPLAY_ONLY`, `LIVE_ONLY`, and `CATCHUP_THEN_LIVE` sessions through real backend APIs.
2. `CATCHUP_THEN_LIVE` replays completed historical dates and then switches to live minute processing without manual restart.
3. Real-time sessions process minute bars incrementally and persist per-order execution state.
4. V25 live mode does not require 240 pre-existing bars; if V25 real-time adapter is not available, backend fails with `ALGO_REALTIME_UNSUPPORTED`.
5. V25 historical replay enforces full-day/minimum-bar completeness.
6. No source fallback, algorithm fallback, daily fallback, default price/cash/position, or fake success exists in the authoritative path.
7. Backend errors include `error_code`, message, and context, and the UI displays them.
8. Scheduler can continue live sessions without the frontend open.
9. Restart/repeated tick does not duplicate orders, fills, cash ledger rows, or snapshots.
10. Full backend and UI validation can run on temporary ports without restarting production backend port `8001`.

## 24. Explicit Non-Goals

- Do not implement QMT.
- Do not implement Shadow Trading.
- Do not implement live brokerage or real order placement.
- Do not change `backend/data_service` semantics.
- Do not add daily-bar paper trading fallback.
- Do not make multi-package aggregate selection directly tradeable in Paper v2 until a frozen SelectionBundle or combined StrategyPackage contract exists.
- Do not expose execution options in Paper v2 unless they exist in the backtest-validated execution policy contract.

## 25. Implementation Readiness Checklist

Before code implementation starts, confirm:

- backend will add session orchestration instead of weakening `PaperTradingDayRunner`;
- current UI options are audited so unsupported options are disabled or removed;
- V25 capability is represented as historical-safe and live-safe separately;
- TDX historical usage, if desired, is added as an explicit provider/source, not an implicit fallback;
- tests include negative paths and idempotency, not only happy paths;
- temporary backend/frontend ports are used for validation.

