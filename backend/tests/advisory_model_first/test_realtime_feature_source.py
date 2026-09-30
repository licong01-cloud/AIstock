from __future__ import annotations

from contextlib import nullcontext
from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.canonical_equity_pit import CANONICAL_PIT_UNIVERSE_KEY
from backend.services.advisory_model_first import realtime_feature_source
from backend.services.advisory_model_first.realtime_feature_source import (
    PostgresAdvisoryReviewSource,
    PostgresRealtimeFeatureSource,
    _market_frame,
)


class _PriceContextCursor:
    _DEFAULT = object()

    def __init__(self, *, audit=("success", "ok"), adjustment_audit=_DEFAULT) -> None:
        self.audit = audit
        self.adjustment_audit = audit if adjustment_audit is self._DEFAULT else adjustment_audit
        self.sql = ""
        self.params = None
        self.calls = []

    def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.params = params
        self.calls.append((self.sql, params))

    def fetchone(self):
        if "stock_universe_pit_state" in self.sql:
            return ("ready", False, date(2018, 8, 1), date(2026, 7, 20))
        if "dataset_date_refresh_audit" in self.sql:
            return self.adjustment_audit if "dataset = 'adj_factor'" in self.sql else self.audit
        raise AssertionError(self.sql)

    def fetchall(self):
        if "FROM market.kline_daily_raw price" in self.sql:
            return [
                ("000001.SZ", 10000, date(2020, 1, 1), 99, 1.0),
                ("000002.SZ", 20000, date(2020, 1, 1), 99, 1.0),
            ]
        if "stock_universe_pit_events" in self.sql:
            return [("000002.SZ", "st_negative", date(2026, 7, 21), date(2026, 7, 20), None, None)]
        if "FROM market.dividend" in self.sql:
            return [
                (
                    "000001.SZ",
                    date(2025, 12, 31),
                    date(2026, 7, 1),
                    "实施",
                    0.1,
                    0.04,
                    0.06,
                    0.16,
                    0.2,
                    date(2026, 7, 15),
                )
            ]
        raise AssertionError(self.sql)


def test_price_context_reads_decision_raw_price_and_visible_target_actions_only() -> None:
    cursor = _PriceContextCursor()
    contexts, unavailable = PostgresRealtimeFeatureSource._price_range_contexts(
        cursor,
        symbols=("000001.SZ", "000002.SZ"),
        decision_as_of_trade_date=date(2026, 7, 20),
        target_trade_date=date(2026, 7, 21),
    )
    assert unavailable == ()
    assert contexts["000001.SZ"].decision_raw_close == 10.0
    assert np.isclose(contexts["000001.SZ"].target_raw_price_multiplier, 1.0 / 1.1223)
    assert contexts["000001.SZ"].corporate_action_source.endswith("provider_projection_v2")
    assert contexts["000002.SZ"].target_is_st is True
    price_query = next(call for call in cursor.calls if "FROM market.kline_daily_raw price" in call[0])
    assert price_query[1][2] == date(2026, 7, 20)
    dividend_query = next(call for call in cursor.calls if "FROM market.dividend" in call[0])
    assert dividend_query[1] == (date(2026, 7, 21), ["000001.SZ", "000002.SZ"])


def test_price_context_uses_explicit_canonical_runtime_universe() -> None:
    cursor = _PriceContextCursor()

    PostgresRealtimeFeatureSource._price_range_contexts(
        cursor,
        symbols=("000001.SZ", "000002.SZ"),
        decision_as_of_trade_date=date(2026, 7, 20),
        target_trade_date=date(2026, 7, 21),
        pit_universe_key=CANONICAL_PIT_UNIVERSE_KEY,
    )

    state_query = next(call for call in cursor.calls if "stock_universe_pit_state" in call[0])
    event_query = next(call for call in cursor.calls if "stock_universe_pit_events" in call[0])
    assert state_query[1] == (CANONICAL_PIT_UNIVERSE_KEY,)
    assert event_query[1][0] == CANONICAL_PIT_UNIVERSE_KEY


def test_missing_dividend_refresh_is_candidate_visible_not_multiplier_one() -> None:
    cursor = _PriceContextCursor(audit=None)
    contexts, unavailable = PostgresRealtimeFeatureSource._price_range_contexts(
        cursor,
        symbols=("000001.SZ", "000002.SZ"),
        decision_as_of_trade_date=date(2026, 7, 20),
        target_trade_date=date(2026, 7, 21),
    )
    assert contexts == {}
    assert {item["reason_code"] for item in unavailable} == {
        "ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE"
    }
    assert all("FROM market.dividend" not in sql for sql, _ in cursor.calls)


def test_missing_adjustment_refresh_blocks_only_visible_corporate_action() -> None:
    cursor = _PriceContextCursor(adjustment_audit=None)
    contexts, unavailable = PostgresRealtimeFeatureSource._price_range_contexts(
        cursor,
        symbols=("000001.SZ", "000002.SZ"),
        decision_as_of_trade_date=date(2026, 7, 20),
        target_trade_date=date(2026, 7, 21),
    )
    assert set(contexts) == {"000002.SZ"}
    assert unavailable[0]["symbol"] == "000001.SZ"
    assert unavailable[0]["reason_code"] == "ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE"


@pytest.mark.parametrize(
    "symbol,raw_close,cash_dividend,decision_factor,target_factor",
    [
        ("600016.SH", 4.33, 0.136, 35.5097, 36.6962),
        ("601766.SH", 7.54, 0.21, 1.4036, 1.4438),
        ("688667.SH", 65.23, 0.09923, 1.0130, 1.0146),
    ],
)
def test_provider_projection_matches_frozen_corporate_action_rows(
    symbol, raw_close, cash_dividend, decision_factor, target_factor
) -> None:
    action = (
        symbol,
        date(2025, 12, 31),
        date(2025, 8, 1),
        "实施",
        0.0,
        0.0,
        0.0,
        cash_dividend,
        cash_dividend,
        date(2025, 8, 1),
    )
    multiplier, source = realtime_feature_source._target_raw_price_multiplier(
        symbol=symbol,
        decision_raw_close=raw_close,
        decision_adjustment_factor=decision_factor,
        rows=(action,),
        decision_as_of_trade_date=date(2025, 9, 15),
    )
    assert multiplier == pytest.approx(decision_factor / target_factor, abs=1e-12)
    assert source.endswith("provider_projection_v2")


def test_provider_projection_fails_closed_when_decision_factor_is_missing() -> None:
    action = (
        "600016.SH",
        date(2025, 12, 31),
        date(2025, 8, 1),
        "实施",
        0.0,
        0.0,
        0.0,
        0.136,
        0.136,
        date(2025, 8, 1),
    )
    with pytest.raises(realtime_feature_source.AdvisoryModelFirstError) as exc_info:
        realtime_feature_source._target_raw_price_multiplier(
            symbol="600016.SH",
            decision_raw_close=4.33,
            decision_adjustment_factor=None,
            rows=(action,),
            decision_as_of_trade_date=date(2025, 9, 15),
        )
    assert exc_info.value.reason_code == "ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE"


def test_realtime_market_frame_matches_qlib_daily_units_and_true_limit_flags() -> None:
    raw = pd.DataFrame(
        {
            "trade_date": [pd.Timestamp("2026-07-20"), pd.Timestamp("2026-07-21")],
            "ts_code": ["000001.SZ", "000001.SZ"],
            "open_li": [10000, 11000],
            "high_li": [10500, 11000],
            "low_li": [9500, 11000],
            "close_li": [10000, 11000],
            "volume_hand": [100, 120],
            "amount_li": [1_000_000, 1_200_000],
            "adj_factor": [1.0, 2.0],
            "base_adj_factor": [2.0, 2.0],
            "pre_close": [9.0, 10.0],
            "up_limit": [11.0, 11.0],
            "down_limit": [9.0, 9.0],
        }
    )
    result = _market_frame(raw, context="test")
    first = result.loc[(pd.Timestamp("2026-07-20"), "000001.SZ")]
    second = result.loc[(pd.Timestamp("2026-07-21"), "000001.SZ")]
    assert first["factor"] == 0.5
    assert first["close"] == 5.0
    assert first["volume"] == 20_000.0
    assert first["amount"] == 1000.0
    assert first["limit_up"] == 0.0
    assert second["factor"] == 1.0
    assert second["close"] == 11.0
    assert second["limit_up"] == 1.0
    assert np.isfinite(result[["open", "high", "low", "close", "volume", "amount"]]).all().all()


def test_market_breadth_query_uses_authoritative_pit_universe(monkeypatch) -> None:
    captured_sql: list[str] = []

    def fake_read_frame(_cursor, sql: str, _parameters) -> pd.DataFrame:
        captured_sql.append(sql)
        return pd.DataFrame(
            {
                "trade_date": [date(2026, 7, 14), date(2026, 7, 15)],
                "ts_code": ["000001.SZ", "000001.SZ"],
                "open_li": [10000, 10100],
                "high_li": [10200, 10300],
                "low_li": [9900, 10000],
                "close_li": [10000, 10200],
                "adj_factor": [1.0, 1.0],
                "up_limit": [11.0, 11.0],
                "down_limit": [9.0, 9.0],
            }
        )

    monkeypatch.setattr(realtime_feature_source, "_read_frame", fake_read_frame)

    result = PostgresRealtimeFeatureSource._market_daily(
        object(),
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 15),
    )

    assert len(result) == 2
    assert len(captured_sql) == 1
    assert "JOIN market.sector_data AS eligible" in captured_sql[0]
    assert "eligible.trade_date = price.trade_date" in captured_sql[0]


def test_review_source_reads_exact_identity_in_a_read_only_transaction() -> None:
    class _Cursor:
        def __init__(self) -> None:
            self.query = ""
            self.parameters = ()
            self.closed = False

        def execute(self, query: str, parameters: tuple[str]) -> None:
            self.query = query
            self.parameters = parameters

        @staticmethod
        def fetchone():
            return (
                "review-1",
                "program-1",
                "binding-1",
                date(2026, 7, 21),
                "selection-1",
                ["selection-1"],
            )

        def close(self) -> None:
            self.closed = True

    class _Connection:
        def __init__(self) -> None:
            self.readonly = False
            self.rollback_count = 0
            self.cursor_instance = _Cursor()

        def cursor(self) -> _Cursor:
            return self.cursor_instance

        def set_session(self, *, isolation_level: str, readonly: bool, autocommit: bool) -> None:
            assert isolation_level == "REPEATABLE READ"
            assert autocommit is False
            self.readonly = readonly

        def rollback(self) -> None:
            self.rollback_count += 1

    connection = _Connection()
    source = PostgresAdvisoryReviewSource(
        connection_context_factory=lambda: nullcontext(connection),
    )

    identity = source.get("review-1")

    assert identity.review_run_id == "review-1"
    assert identity.selection_run_id == "selection-1"
    assert identity.selection_run_ids == ("selection-1",)
    assert connection.readonly is True
    assert connection.rollback_count == 1
    assert connection.cursor_instance.parameters == ("review-1",)
    assert "FROM app.advisory_review_run" in connection.cursor_instance.query
    assert connection.cursor_instance.closed is True
