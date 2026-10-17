from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_sources import (
    EconomicDailySnapshot,
    EconomicEntryReadonlyDailySource,
    project_economic_price_inputs,
    _suspension_states,
    _records,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


@pytest.fixture
def data():
    days = pd.bdate_range("2025-01-02", periods=4)
    daily = pd.DataFrame(
        {
            "trade_date": days,
            "instrument": "000001.SZ",
            "raw_open_cny": [10.0, 10.0, 9.0, 11.0],
            "raw_close_cny": [10.0, 10.0, 9.0, 11.0],
            "adj_factor": 2.0,
        }
    )
    snapshot = EconomicDailySnapshot(
        daily,
        pd.DataFrame(columns=["trade_date", "instrument"]),
        pd.DataFrame(
            columns=[
                "instrument",
                "end_date",
                "ann_date",
                "div_proc",
                "stk_div",
                "stk_bo_rate",
                "stk_co_rate",
                "cash_div",
                "cash_div_tax",
                "imp_ann_date",
                "ex_date",
            ]
        ),
        days,
        "a" * 64,
        {"native_capture": False},
    )
    candidates = pd.DataFrame(
        [{"decision_as_of_trade_date": days[0], "target_trade_date": days[1], "instrument": "000001.SZ"}]
    )
    episodes = pd.DataFrame(
        [
            {
                "label_status": "MATURED",
                "entry_trade_date": days[1],
                "effective_exit_date": days[3],
                "instrument": "000001.SZ",
                "entry_price": 20.0,
                "exit_price": 22.0,
            }
        ]
    )
    return snapshot, candidates, episodes


def test_all_endpoint_coordinate_parity_and_unknown_factor_are_explicit(data):
    snapshot, candidates, episodes = data
    prices, refs, _, receipt = project_economic_price_inputs(
        snapshot=snapshot, candidates=candidates, episodes=episodes
    )
    assert prices["policy_price_per_raw_cny"].tolist() == [2.0] * 4
    assert refs.iloc[0]["target_reference_raw_cny"] == 10
    assert receipt["verified_endpoint_count"] == 2 and receipt["native_capture"] is False
    snapshot.daily.loc[2, "adj_factor"] = float("nan")
    prices, _, _, _ = project_economic_price_inputs(snapshot=snapshot, candidates=candidates, episodes=episodes)
    assert pd.isna(prices.iloc[2]["policy_price_per_raw_cny"])
    snapshot.daily.loc[3, "raw_open_cny"] = 11.1
    with pytest.raises(AdvisoryModelFirstError) as caught:
        project_economic_price_inputs(snapshot=snapshot, candidates=candidates, episodes=episodes)
    assert caught.value.reason_code == "ADVISORY_ECONOMIC_PRICE_PARITY"


def test_authoritative_suspension_without_quote_is_preserved(data):
    snapshot, candidates, episodes = data
    snapshot.daily.drop(index=2, inplace=True)
    snapshot.suspend_rows.loc[0] = [snapshot.calendar[2], "000001.SZ"]
    prices, _, _, _ = project_economic_price_inputs(snapshot=snapshot, candidates=candidates, episodes=episodes)
    row = prices.loc[prices["trade_date"].eq(snapshot.calendar[2])].iloc[0]
    assert row["suspended"] and pd.isna(row["raw_open_cny"])


@pytest.mark.parametrize("periods", [4, 24])
def test_db_consumer_is_one_readonly_snapshot_with_bounded_queries(periods):
    days = pd.bdate_range("2025-01-02", periods=periods)
    daily = pd.DataFrame(
        {
            "trade_date": days,
            "instrument": "000001.SZ",
            "open_li": 10000,
            "high_li": 11000,
            "low_li": 9000,
            "close_li": 10000,
            "adj_factor": 2.0,
            "pre_close": 10.0,
            "up_limit": 11.0,
            "down_limit": 9.0,
        }
    )
    frames = [part for _, part in daily.groupby(daily["trade_date"].dt.to_period("M"))] + [
        pd.DataFrame(columns=["trade_date", "instrument", "suspend_type", "suspend_timing"]),
        pd.DataFrame({"cal_date": days}),
        pd.DataFrame(
            columns=[
                "instrument",
                "end_date",
                "ann_date",
                "div_proc",
                "stk_div",
                "stk_bo_rate",
                "stk_co_rate",
                "cash_div",
                "cash_div_tax",
                "imp_ann_date",
                "ex_date",
            ]
        ),
    ]
    calls, sessions = [], []

    class Cursor:
        index = -1

        def execute(self, sql, args):
            calls.append((sql, args))
            if sql.lstrip().upper().startswith("SELECT"):
                self.index += 1
                self.description = [SimpleNamespace(name=name) for name in frames[self.index].columns]

        def fetchall(self):
            return list(frames[self.index].itertuples(index=False, name=None))

        def close(self):
            calls.append(("CLOSE", None))

    class Connection:
        def cursor(self):
            return Cursor()

        def set_session(self, **kwargs):
            sessions.append(kwargs)

        def rollback(self):
            calls.append(("ROLLBACK", None))

    @contextmanager
    def connection():
        yield Connection()

    source = EconomicEntryReadonlyDailySource(connection_context_factory=connection)
    result = source.load(symbols=("000001.SZ",), start_date=days[0].date(), end_date=days[-1].date(), maximum_rows=30)
    assert sessions == [dict(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)]
    assert len([sql for sql, _ in calls if sql.lstrip().upper().startswith("SELECT")]) == len(frames)
    daily_calls = [(sql, args) for sql, args in calls if "FROM market.kline_daily_raw" in sql]
    for _, args in daily_calls:
        assert args[0:2] == args[2:4] == args[5:7]  # All joined partition clocks are bounded.
        assert args[0].month == args[1].month
    assert calls[0] == ("SET LOCAL statement_timeout = %s", (30000,))
    assert any(sql == "ROLLBACK" for sql, _ in calls)
    assert result.daily["raw_open_cny"].tolist() == [10.0] * periods  # Divide by 1000 only once.
    assert result.source_receipt["database_written"] is False


def test_source_query_failure_is_typed_and_rolls_back_without_exposing_sql():
    closed = []

    class Cursor:
        def execute(self, sql, parameters):
            if sql.startswith("SELECT"):
                raise TimeoutError("must not expose SQL or credentials")

        def close(self):
            closed.append("cursor")

    class Connection:
        def cursor(self):
            return Cursor()

        def set_session(self, **kwargs):
            pass

        def rollback(self):
            closed.append("rollback")

    @contextmanager
    def connection():
        yield Connection()

    with pytest.raises(AdvisoryModelFirstError) as caught:
        EconomicEntryReadonlyDailySource(connection_context_factory=connection).load(
            symbols=["000001.SZ"],
            start_date=pd.Timestamp("2025-01-02").date(),
            end_date=pd.Timestamp("2025-01-03").date(),
            maximum_rows=10,
        )
    assert caught.value.reason_code == "ADVISORY_ECONOMIC_SOURCE_QUERY_FAILED"
    assert caught.value.context["phase"] == "daily:2025-01-02:2025-01-03"
    assert "must not expose" not in str(caught.value)
    assert closed == ["rollback", "cursor"]


def test_full_day_intraday_and_ambiguous_suspensions_are_not_conflated():
    rows = pd.DataFrame([
        [pd.Timestamp("2025-01-02"), "000001.SZ", "S", None],
        [pd.Timestamp("2025-01-03"), "000001.SZ", "R", None],
        [pd.Timestamp("2025-01-06"), "000001.SZ", "S", "09:30-10:00"],
        [pd.Timestamp("2025-01-07"), "000001.SZ", "S", None],
        [pd.Timestamp("2025-01-07"), "000001.SZ", "R", None],
    ], columns=["trade_date", "instrument", "suspend_type", "suspend_timing"])
    result = _suspension_states(rows)
    assert len(result) == 3
    assert result["suspended"].tolist() == [True, False, False]
    assert result["tradability_unknown"].tolist() == [False, True, True]
    assert len(rows) == 5  # Raw event evidence is retained, not resolved by arbitrary precedence.


def test_source_hash_serialization_handles_database_dates_decimals_and_nulls():
    frame = pd.DataFrame({"date": [date(2025, 1, 2), date(2025, 1, 3)],
                          "decimal": [Decimal("1.234567890123456789"), None], "mark": [10., float("nan")]})
    assert _records(frame) == [{"date": "2025-01-02", "decimal": "1.234567890123456789", "mark": 10.},
                              {"date": "2025-01-03", "decimal": None, "mark": None}]
