from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from backend.qlib_exporter.db_reader import DBReader
from backend.qlib_exporter.field_map import build_field_map_rows_for_snapshot
from backend.services.industry_code_map import build_sw_l2_code_map


SW2_COLS = [
    "sw2_open",
    "sw2_high",
    "sw2_low",
    "sw2_close",
    "sw2_pct_change",
    "sw2_vol",
    "sw2_amount",
    "sw2_pe",
    "sw2_pb",
    "sw2_total_mv",
    "sw2_mf_buy_sm_amt",
    "sw2_mf_sell_sm_amt",
    "sw2_mf_buy_md_amt",
    "sw2_mf_sell_md_amt",
    "sw2_mf_buy_lg_amt",
    "sw2_mf_sell_lg_amt",
    "sw2_mf_buy_elg_amt",
    "sw2_mf_sell_elg_amt",
    "sw2_mf_net_amt",
    "sw2_mf_buy_elg_vol",
    "sw2_mf_sell_elg_vol",
    "sw2_mf_net_vol",
]


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn
        self.description = []
        self._rows = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, sql: str, *_args: object) -> None:
        self._conn.sql.append(sql)
        if "FROM market.sw_index_classify" in sql:
            self.description = [("index_code",)]
            self._rows = [(code,) for code in self._conn.classify_codes]
        else:
            self.description = [(col,) for col in ["trade_date", "ts_code", "l2_code", *SW2_COLS]]
            self._rows = self._conn.sector_rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class _FakeConn:
    def __init__(self, *, sector_rows: list[tuple[object, ...]], classify_codes: list[str]) -> None:
        self.sector_rows = sector_rows
        self.classify_codes = classify_codes
        self.sql: list[str] = []

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)


def _sector_row(trade_date: date, ts_code: str, l2_code: str | None, offset: float) -> tuple[object, ...]:
    values = [offset + idx + 0.25 for idx in range(len(SW2_COLS))]
    return (trade_date, ts_code, l2_code, *values)


def test_build_sw_l2_code_map_is_order_stable() -> None:
    first = build_sw_l2_code_map(["801020.SI", "801010.SI", "801030.SI"])
    second = build_sw_l2_code_map(["801030.SI", "801020.SI", "801010.SI", "801020.SI"])

    assert first == second
    assert first == {"801010.SI": 0, "801020.SI": 1, "801030.SI": 2}


def test_load_sector_data_panel_adds_pit_l2_code_id_and_preserves_sw2_values(monkeypatch) -> None:
    rows = [
        _sector_row(date(2024, 1, 2), "000004.SZ", "801010.SI", 10.0),
        _sector_row(date(2024, 1, 3), "000004.SZ", "801020.SI", 20.0),
        _sector_row(date(2024, 1, 3), "000005.SZ", None, 30.0),
    ]
    conn = _FakeConn(sector_rows=rows, classify_codes=["801020.SI", "801010.SI", "801030.SI"])
    monkeypatch.setattr("backend.qlib_exporter.db_reader.get_conn", lambda: conn)

    df = DBReader().load_sector_data_panel(
        start=date(2024, 1, 2),
        end=date(2024, 1, 3),
        ts_codes=["000004.SZ", "000005.SZ"],
    )

    sector_sql = conn.sql[0]
    assert "LEFT JOIN LATERAL" in sector_sql
    assert "m.in_date <= sd.trade_date" in sector_sql
    assert "(m.out_date IS NULL OR m.out_date >= sd.trade_date)" in sector_sql
    assert "ORDER BY m.in_date DESC NULLS LAST, m.out_date DESC NULLS LAST" in sector_sql

    assert len(df) == len(rows)
    assert list(df.columns) == [*SW2_COLS, "l2_code_id"]
    assert df["l2_code_id"].tolist() == [0, 1, -1]
    assert str(df["l2_code_id"].dtype) == "int16"
    assert all(str(df[col].dtype) == "float32" for col in SW2_COLS)

    expected_first_sw2 = np.asarray(rows[0][3:], dtype=np.float32)
    np.testing.assert_array_equal(df.iloc[0][SW2_COLS].to_numpy(dtype=np.float32), expected_first_sw2)


def test_load_sector_data_panel_warns_when_l2_code_coverage_is_low(monkeypatch, caplog) -> None:
    rows = [
        _sector_row(date(2024, 1, 2), f"00000{i}.SZ", None if i else "801010.SI", float(i))
        for i in range(10)
    ]
    conn = _FakeConn(sector_rows=rows, classify_codes=["801010.SI"])
    monkeypatch.setattr("backend.qlib_exporter.db_reader.get_conn", lambda: conn)

    with caplog.at_level(logging.WARNING, logger="backend.qlib_exporter.db_reader"):
        DBReader().load_sector_data_panel(start=date(2024, 1, 2), end=date(2024, 1, 2))

    assert "reason_code=sector_data_l2_code_id_low_coverage" in caplog.text
    assert "missing_count=9" in caplog.text
    assert "total_count=10" in caplog.text


def test_static_bundle_float32_cast_keeps_l2_code_id_integer_semantics() -> None:
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), "000001.SZ"), (pd.Timestamp("2024-01-02"), "000002.SZ")],
        names=["datetime", "instrument"],
    )
    original = pd.DataFrame(
        {
            "sw2_close": np.asarray([123.5, 456.25], dtype=np.float32),
            "sw2_pct_change": np.asarray([1.25, -0.5], dtype=np.float32),
            "l2_code_id": np.asarray([130, -1], dtype=np.int16),
        },
        index=index,
    )

    bundled = original.astype(np.float32)

    np.testing.assert_array_equal(bundled["sw2_close"].to_numpy(), original["sw2_close"].to_numpy())
    np.testing.assert_array_equal(bundled["sw2_pct_change"].to_numpy(), original["sw2_pct_change"].to_numpy())
    assert bundled["l2_code_id"].tolist() == [130.0, -1.0]
    assert all(float(value).is_integer() for value in bundled["l2_code_id"])


def test_sector_field_map_includes_l2_code_id(monkeypatch) -> None:
    monkeypatch.setattr("backend.qlib_exporter.field_map._fetch_pg_column_comments", lambda *_args: {})

    rows = build_field_map_rows_for_snapshot(
        daily_basic_columns=None,
        moneyflow_columns=None,
        sector_data_columns=["sw2_close", "l2_code_id"],
        sector_data_dtypes={"sw2_close": "float32", "l2_code_id": "int16"},
    )

    by_name = {row.name: row for row in rows}
    assert by_name["l2_code_id"].dtype_hint == "int16"
    assert by_name["l2_code_id"].source_table == "sector_data"
    assert "market.sw_index_classify" in by_name["l2_code_id"].comment
