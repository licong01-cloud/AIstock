# Unified Event Signal Time Semantics Validation

Date: 2026-05-06 20:52 Asia/Shanghai
Branch: codex/unified-event-signal-backfill-20260506
Scope: validate the standalone event time semantics module and financial event adapter integration. No trading consumers were modified or activated.

## Business Goal

- Backtest mode must remain leakage-safe: use source publish time when exact, otherwise use conservative date-only next trading day.
- Paper/live mode must use real AIstock local observation time (`first_seen_at`, then `observed_at` fallback) instead of treating Tushare-returned rows as unavailable solely because `ann_date` may be later than the fetch day.
- Current phase only generates event facts/signals; it does not connect `event_signal` to QE, Selection Center, Paper v2, QMT, or live trading.

## Changed Scope

- `backend/services/event_signal/time_semantics.py`
- `backend/services/event_signal/financial_event_adapter.py`
- `backend/tests/event_signal/test_time_semantics.py`
- `backend/tests/event_signal/test_financial_event_adapter.py`
- `docs/architecture/unified_event_signal_architecture_20260506.md`

## Verification Commands

```powershell
python -m py_compile backend/services/event_signal/time_semantics.py backend/services/event_signal/financial_event_adapter.py
```

Result: passed.

```powershell
pytest backend/tests/event_signal/test_time_semantics.py backend/tests/event_signal/test_financial_event_adapter.py backend/tests/event_signal/test_announcement_adapter.py -q -p no:cacheprovider
```

Result: 25 passed in 0.46s.

```powershell
pytest backend/tests/event_signal -q -p no:cacheprovider
```

Result: 45 passed in 0.73s.

```powershell
git diff --check
```

Result: passed. Git emitted only existing line-ending normalization warnings for modified tracked files.

## Guardrails

```powershell
rg -n "event_signal|time_semantics" backend/services/quantevolver backend/services/paper_trading backend/services/paper_trading_v2 backend/services/selection_center backend/routers/qmt.py backend/infra/qmt_client.py
```

Result: no matches (`rg` exit 1), confirming no new event-signal consumer references in QE, Selection Center, Paper v2, Paper Trading, QMT, or live adapter paths.

```powershell
Select-String -Path <changed code/test files> -Pattern AKIA,SECRET_KEY,TUSHARE_TOKEN=,password= -SimpleMatch
```

Result: no forbidden token matches.

```powershell
rg -n "reset --hard|checkout --|git clean|TODO|FIXME|pass  #|silent fallback|fake success|default cash|default price" <changed files>
```

Result: no guardrail pattern matches.

## Business Cases Covered

- Backtest date-only Tushare financial sources ignore local `first_seen_at` and use next trading day after `ann_date`.
- Backtest exact publish time before 09:25 uses same trading day.
- Backtest exact publish time after 09:25 uses next trading day.
- Backtest `00:00:00` publish timestamp defaults to next trading day with `MIDNIGHT_DEFAULT`.
- Paper/live can use a prior-day local observation of a Tushare row whose `ann_date` is the next trading day.
- Paper/live uses same trading day when local `first_seen_at` is before 09:25.
- Paper/live without observation time falls back to date-only conservative handling.
- Observed mode prefers `observed_at` for audit and remains separate from backtest truth.

## Residual Risks

- No DB write smoke was run in this validation; the change is pure time semantics plus existing adapter tuple generation tests.
- No frontend/API validation was required because no endpoint or UI path changed.
- The module is not yet integrated into announcement classification; existing announcement rows already carry time semantics from the announcement classifier.
- Production backend port 8001 was not restarted or touched.
