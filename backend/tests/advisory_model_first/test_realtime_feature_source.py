from __future__ import annotations

from contextlib import nullcontext
from datetime import date

import numpy as np
import pandas as pd

from backend.services.advisory_model_first import realtime_feature_source
from backend.services.advisory_model_first.realtime_feature_source import (
    PostgresRealtimeFeatureSource,
    PostgresAdvisoryReviewSource,
    _market_frame,
)


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
