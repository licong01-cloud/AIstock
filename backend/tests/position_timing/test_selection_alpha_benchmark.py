"""Direct contracts for the PT-NEXT-028 selection-alpha benchmark."""
from __future__ import annotations

from decimal import Decimal

import numpy as np
import pandas as pd

from backend.services.position_timing import r8_proxy_screen as screens
from backend.services.position_timing import selection_alpha_benchmark as benchmark
from backend.services.position_timing.pattern_close_cash_replay import Account


def _bars(*, periods: int = 270, entry_blocked: bool = False) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    close = np.linspace(10.0, 20.0, periods)
    up = np.full(periods, 100.0)
    if entry_blocked:
        up[2] = close[2]
    return pd.DataFrame(
        {
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1_000_000.0,
            "factor": 1.0,
            "up_limit": up,
            "down_limit": 1.0,
            "is_suspended": False,
            "pit_active": True,
        },
        index=dates,
    )


def _proxies(dates: pd.DatetimeIndex) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = pd.DataFrame(
        {
            "db_total_mv": 600_000.0,
            "db_turnover_rate_f": 1.0,
            "db_pe_ttm": 20.0,
            "db_pb": 2.0,
        },
        index=dates,
    )
    bak = pd.DataFrame(
        {column: 10.0 for column in screens.BAK_COLUMNS},
        index=dates,
    )
    return daily, bak


def test_monthly_schedule_uses_first_global_session_and_leaves_entry_session():
    calendar = pd.DatetimeIndex(
        ["2024-01-02", "2024-01-31", "2024-02-02", "2024-02-29", "2024-03-01"]
    )
    assert benchmark._monthly_decision_ordinals(calendar) == (0, 2)


def test_replay_uses_t_minus_one_screen_then_t_plus_one_close_and_fixed_horizons():
    bars = _bars()
    daily, bak = _proxies(bars.index)
    # The decision is ordinal 1.  A failure introduced on T itself must only
    # affect the following session because the screen contract shifts once.
    daily.loc[bars.index[1], "db_total_mv"] = 1.0

    symbol, events, decisions = benchmark._replay_symbol_task(
        "600001.SH", bars, daily, bak, (1,)
    )

    assert symbol == "600001.SH"
    assert decisions.iloc[0][f"{screens.U2}_state"] == "PASS"
    assert set(events.horizon) == set(benchmark.HORIZONS)
    assert events.entry_ordinal.nunique() == 1
    assert int(events.entry_ordinal.iloc[0]) == 2
    primary = events.loc[events.horizon.eq(benchmark.PRIMARY_HORIZON)].iloc[0]
    assert primary.maturity_status == "EVALUATED"
    assert primary.entry_status == "FILLED"
    assert primary.net_return > 0
    assert primary.estimated_exit_fee_cny > 0


def test_blocked_t_plus_one_buy_stays_cash_without_retry_or_survivor_drop():
    bars = _bars(entry_blocked=True)
    daily, bak = _proxies(bars.index)

    _, events, _ = benchmark._replay_symbol_task("600001.SH", bars, daily, bak, (1,))

    assert events.entry_status.eq("DIRECTIONAL_LIMIT_BLOCKED").all()
    assert not events.entry_filled.any()
    assert events.net_return.fillna(999).eq(0).all()
    assert events.terminal_sellability.eq("NOT_APPLICABLE_CASH").all()
    assert events.max_drawdown.eq(0).all()


def test_missing_proxy_symbol_becomes_unknown_instead_of_dropping_population():
    bars = _bars(periods=30)
    empty_index = pd.MultiIndex.from_arrays(
        [pd.DatetimeIndex([]), pd.Index([], dtype=str)],
        names=["datetime", "instrument"],
    )
    source = pd.DataFrame(
        index=empty_index,
        columns=list(screens.DAILY_COLUMNS),
        dtype=float,
    )
    daily = benchmark._symbol_frame(source, "600001.SH", "daily_basic")
    assert daily.empty
    bak = pd.DataFrame(index=bars.index, columns=screens.BAK_COLUMNS, dtype=float)

    _, events, decisions = benchmark._replay_symbol_task(
        "600001.SH", bars, daily, bak, (1,)
    )

    assert decisions.iloc[0][f"{screens.U0}_state"] == "UNKNOWN"
    assert events.empty


def test_terminal_mark_deducts_sell_cost_without_claiming_a_fill():
    account = Account(cash=Decimal("100"), units=Decimal("10"))
    wealth, cost = benchmark._terminal_net_mtm(
        account, {"close": 10.0, "factor": 1.0}
    )
    assert wealth is not None and cost is not None
    assert wealth == float(Decimal("200") - Decimal(str(cost)))
    assert cost > 0


def test_sellability_is_directional_and_has_frozen_defer_limit():
    records = _bars(periods=10).to_dict("records")
    records[2]["close"] = records[2]["down_limit"]
    records[3]["close"] = records[3]["down_limit"]
    status, deferred, last = benchmark._sellability(records, nominal=2)
    assert (status, deferred, last) == ("SELLABLE", 2, "EXECUTABLE")

    for ordinal in range(2, 2 + benchmark.TERMINAL_SELLABILITY_DEFER + 1):
        records[ordinal]["close"] = records[ordinal]["down_limit"]
    status, deferred, last = benchmark._sellability(records, nominal=2)
    assert status == "UNAVAILABLE_WITHIN_DEFER"
    assert deferred is None
    assert last == "DIRECTIONAL_LIMIT_BLOCKED"


def test_cohort_requires_every_selected_event_and_retains_cash_returns():
    decisions = pd.DataFrame(
        [
            {"symbol": "A", "decision_date": "2024-01-02", **{f"{s}_state": "PASS" for s in screens.SCREEN_IDS}},
            {"symbol": "B", "decision_date": "2024-01-02", **{f"{s}_state": "PASS" for s in screens.SCREEN_IDS}},
        ]
    )
    base = {
        "decision_date": "2024-01-02",
        "horizon": benchmark.PRIMARY_HORIZON,
        "maturity_status": "EVALUATED",
        "entry_filled": True,
        "net_return": 0.10,
        "max_drawdown": -0.05,
        **{f"{s}_state": "PASS" for s in screens.SCREEN_IDS},
    }
    incomplete = pd.DataFrame([{"symbol": "A", **base}])
    result = benchmark._cohort_monthly(incomplete, decisions)
    primary = result.loc[result.horizon.eq(benchmark.PRIMARY_HORIZON)]
    assert primary.status.eq("EVENT_MATERIALIZATION_MISSING").all()

    complete = pd.DataFrame(
        [
            {"symbol": "A", **base},
            {"symbol": "B", **{**base, "entry_filled": False, "net_return": 0.0, "max_drawdown": 0.0}},
        ]
    )
    result = benchmark._cohort_monthly(complete, decisions)
    primary = result.loc[result.horizon.eq(benchmark.PRIMARY_HORIZON)]
    assert primary.status.eq("COMPLETE").all()
    assert primary.mean_net_return.eq(0.05).all()
    assert primary.cash_no_entry_count.eq(1).all()


def test_formal_bootstrap_uses_zero_economic_threshold_and_three_way_family():
    positive = benchmark._bootstrap_monthly(np.full(36, 0.01))
    negative = benchmark._bootstrap_monthly(np.full(36, -0.01))
    inconclusive = benchmark._bootstrap_monthly(np.r_[np.full(18, -0.01), np.full(18, 0.01)])

    assert positive["evidence_state"] == "SUPPORTED"
    assert negative["evidence_state"] == "NEGATIVE"
    assert inconclusive["evidence_state"] == "INCONCLUSIVE"
    assert positive["family_size"] == 3
    assert positive["economic_threshold_bps"] == 0.0
    assert positive["power_status"] == "NOT_COMPUTABLE"


def test_frozen_block_bootstrap_refuses_fewer_than_two_complete_blocks():
    result = benchmark._bootstrap_monthly(
        np.full(benchmark.BOOTSTRAP_BLOCK_MONTHS, 0.01)
    )

    assert result["status"] == "UNAVAILABLE"
    assert result["evidence_state"] == "INCONCLUSIVE"
    assert result["power_status"] == "UNDERPOWERED"
    assert result["observed_months"] == 12
    assert result["minimum_observed_months"] == 24
    assert not any("interval" in key for key in result)


def test_concat_keeps_canonical_columns_when_one_symbol_has_all_na_values():
    left = pd.DataFrame({"symbol": ["A"], "terminal": [None], "return": [0.1]})
    right = pd.DataFrame({"symbol": ["B"], "terminal": [12], "return": [0.2]})

    result = benchmark._concat([left, right])

    assert list(result.columns) == ["symbol", "terminal", "return"]
    assert pd.isna(result.loc[0, "terminal"])
    assert result.loc[1, "terminal"] == 12


def test_contract_remains_offline_exploratory_and_selects_nothing_for_live():
    assert benchmark.CONTRACT["selected_trial_count"] == 0
    assert benchmark.CONTRACT["strict_financial_pit_claimed"] is False
    for field in (
        "database_read",
        "database_write",
        "network_accessed",
        "live_market_read",
        "runtime_action_performed",
        "service_process_control_performed",
        "other_module_write",
    ):
        assert benchmark.CONTRACT[field] is False
