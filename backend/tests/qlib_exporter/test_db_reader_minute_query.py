from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from backend.qlib_exporter import db_reader as module


class _ConnectionContext:
    def __init__(self) -> None:
        self.connection = object()

    def __enter__(self) -> object:
        return self.connection

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None


def test_load_minute_uses_per_code_equality_for_compressed_chunks(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_read_sql(sql: str, connection: object, params: dict[str, object]) -> pd.DataFrame:
        del connection
        calls.append((sql, dict(params)))
        code = str(params["ts_code"])
        minute = 2 if code == "000002.SZ" else 1
        return pd.DataFrame(
            {
                "trade_time": [datetime(2026, 7, 31, 9, 30 + minute)],
                "ts_code": [code],
                "open_li": [100_000],
                "high_li": [101_000],
                "low_li": [99_000],
                "close_li": [100_500],
                "volume_hand": [10],
                "amount_li": [100_000_000],
            }
        )

    monkeypatch.setattr(module, "get_conn", _ConnectionContext)
    monkeypatch.setattr(module.pd, "read_sql", fake_read_sql)

    result = module.DBReader().load_minute(
        ["000002.SZ", "000001.SZ", "000001.SZ"],
        date(2026, 7, 31),
        date(2026, 7, 31),
    )

    assert [call[1]["ts_code"] for call in calls] == ["000002.SZ", "000001.SZ"]
    assert all("ts_code = %(ts_code)s" in call[0] for call in calls)
    assert all("ANY(" not in call[0] for call in calls)
    assert all("trade_time::date" not in call[0] for call in calls)
    assert all(call[1]["start_ts"] == datetime(2026, 7, 31) for call in calls)
    assert all(call[1]["end_next_ts"] == datetime(2026, 8, 1) for call in calls)
    assert list(result.index.get_level_values("instrument")) == ["000001.SZ", "000002.SZ"]
    assert not result.index.has_duplicates


def test_load_minute_empty_codes_do_not_open_connection(monkeypatch) -> None:
    monkeypatch.setattr(
        module,
        "get_conn",
        lambda: (_ for _ in ()).throw(AssertionError("connection must not open")),
    )

    assert module.DBReader().load_minute([], date(2026, 7, 31), date(2026, 7, 31)).empty


def test_load_qlib_minute_uses_same_per_code_sargable_source_path(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_read_sql(sql: str, connection: object, params: dict[str, object]) -> pd.DataFrame:
        del connection
        calls.append((sql, dict(params)))
        code = str(params["ts_code"])
        if "FROM market.stk_limit" in sql:
            return pd.DataFrame(
                {
                    "ts_code": [code],
                    "trade_date": [date(2026, 7, 31)],
                    "up_limit": [11.0],
                    "down_limit": [9.0],
                }
            )
        return pd.DataFrame(
            {
                "trade_time": [datetime(2026, 7, 31, 9, 31)],
                "ts_code": [code],
                "open_li": [100_000],
                "high_li": [101_000],
                "low_li": [99_000],
                "close_li": [100_500],
                "volume_hand": [10],
                "amount_li": [100_000_000],
            }
        )

    class FakeAdjFactorProvider:
        def __init__(self, *, use_tushare_fallback: bool) -> None:
            assert use_tushare_fallback is False

        def get_adj_factor(self, codes, start, end) -> pd.DataFrame:
            del start, end
            return pd.DataFrame(
                {
                    "ts_code": list(codes),
                    "trade_date": [pd.Timestamp("2026-07-31")] * len(codes),
                    "adj_factor": [1.0] * len(codes),
                }
            )

        def calculate_qfq_factor(self, frame: pd.DataFrame) -> pd.DataFrame:
            result = frame.copy()
            result["qfq_factor"] = 1.0
            return result

    monkeypatch.setattr(module, "get_conn", _ConnectionContext)
    monkeypatch.setattr(module.pd, "read_sql", fake_read_sql)
    monkeypatch.setattr(module, "AdjFactorProvider", FakeAdjFactorProvider)

    result = module.DBReader().load_qlib_minute_data(
        ["000002.SZ", "000001.SZ", "000001.SZ"],
        date(2026, 7, 31),
        date(2026, 7, 31),
        use_tushare_adj=False,
    )

    minute_calls = [call for call in calls if f"FROM {module.MINUTE_RAW_TABLE}" in call[0]]
    limit_calls = [call for call in calls if "FROM market.stk_limit" in call[0]]
    assert [call[1]["ts_code"] for call in minute_calls] == ["000002.SZ", "000001.SZ"]
    assert [call[1]["ts_code"] for call in limit_calls] == ["000002.SZ", "000001.SZ"]
    assert all("k.ts_code = %(ts_code)s" in call[0] for call in minute_calls)
    assert all("ANY(" not in call[0] for call in minute_calls)
    assert all("trade_time::date" not in call[0] for call in minute_calls)
    assert all("k.trade_time >= %(start_ts)s" in call[0] for call in minute_calls)
    assert all("k.trade_time < %(end_next_ts)s" in call[0] for call in minute_calls)
    assert list(result.index.get_level_values("instrument")) == ["000001.SZ", "000002.SZ"]
    assert not result.index.has_duplicates
