from __future__ import annotations

import logging
from contextlib import contextmanager

import pandas as pd

from backend.data_service import qe_data_service as qe


def _fake_conn_cm():
    @contextmanager
    def _cm():
        yield object()

    return _cm()


def _sector_frame(rows: list[tuple[str, str, str | None]]) -> pd.DataFrame:
    """rows = [(trade_date, ts_code, l2_code)]; fill 22 sw2_* numeric cols."""
    data = {"trade_date": [], "ts_code": [], "l2_code": []}
    for col in qe.SECTOR_DATA_COLUMNS:
        data[col] = []
    for i, (td, ts, l2) in enumerate(rows):
        data["trade_date"].append(td)
        data["ts_code"].append(ts)
        data["l2_code"].append(l2)
        for j, col in enumerate(qe.SECTOR_DATA_COLUMNS):
            data[col].append(float(i * 100 + j) + 0.25)
    return pd.DataFrame(data)


def _patch_boundary(monkeypatch, frame, code_map, captured):
    monkeypatch.setattr(
        qe, "_normalize_and_validate_instruments", lambda instruments, **kw: [str(x) for x in instruments]
    )
    monkeypatch.setattr(qe, "get_conn", lambda: _fake_conn_cm())
    monkeypatch.setattr(qe, "load_sw_l2_code_map", lambda conn: dict(code_map))
    monkeypatch.setattr(qe._CACHE, "get", lambda *a, **k: None)
    monkeypatch.setattr(qe._CACHE, "set", lambda *a, **k: None)

    def _fake_read_sql(sql, conn, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return frame.copy()

    monkeypatch.setattr(qe.pd, "read_sql", _fake_read_sql)


def test_load_sector_data_appends_pit_l2_code_id_and_preserves_sw2(monkeypatch):
    frame = _sector_frame(
        [
            ("2024-01-02", "000004.SZ", "801010.SI"),
            ("2024-01-03", "000004.SZ", "801020.SI"),  # industry migration
            ("2024-01-03", "000005.SZ", None),          # unmatched -> -1
        ]
    )
    code_map = {"801010.SI": 0, "801020.SI": 1, "801030.SI": 2}
    captured: dict = {}
    _patch_boundary(monkeypatch, frame, code_map, captured)

    df = qe.load_sector_data(["000004.SZ", "000005.SZ"], "2024-01-02", "2024-01-03")

    # PIT lateral join present in SQL
    assert "LEFT JOIN LATERAL" in captured["sql"]
    assert "m.in_date <= sd.trade_date" in captured["sql"]
    assert "(m.out_date IS NULL OR m.out_date >= sd.trade_date)" in captured["sql"]

    # column set: 22 sw2_* + l2_code_id
    assert list(df.columns) == [*qe.SECTOR_DATA_COLUMNS, "l2_code_id"]
    assert str(df["l2_code_id"].dtype) == "int16"
    assert all(str(df[c].dtype) == "float32" for c in qe.SECTOR_DATA_COLUMNS)

    # migration: same instrument, different code across dates; unmatched -> -1
    got = {
        (dt.date().isoformat(), inst): int(code)
        for (dt, inst), code in df["l2_code_id"].items()
    }
    assert got[("2024-01-02", "000004.SZ")] == 0
    assert got[("2024-01-03", "000004.SZ")] == 1
    assert got[("2024-01-03", "000005.SZ")] == -1


def test_load_sector_data_warns_on_low_l2_coverage(monkeypatch, caplog):
    rows = [("2024-01-02", f"00000{i}.SZ", None if i else "801010.SI") for i in range(10)]
    frame = _sector_frame(rows)
    captured: dict = {}
    _patch_boundary(monkeypatch, frame, {"801010.SI": 0}, captured)

    with caplog.at_level(logging.WARNING, logger="aistock.qe_data_service"):
        qe.load_sector_data([r[1] for r in rows], "2024-01-02", "2024-01-02")

    assert "reason_code=sector_data_l2_code_id_low_coverage" in caplog.text
    assert "missing_count=9" in caplog.text
    assert "total_count=10" in caplog.text
