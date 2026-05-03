from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.qlib_exporter import authoritative_bin_exporter as authoritative
from backend.qlib_exporter.db_reader import DBReader


class _DummyConn:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


def test_authoritative_stock_export_exchanges_are_sh_sz_only() -> None:
    assert authoritative.normalize_stock_export_exchanges(None) == ["sh", "sz"]
    assert authoritative.normalize_stock_export_exchanges(["sz", "sh", "sh"]) == ["sh", "sz"]

    with pytest.raises(ValueError, match="BJ/BSE"):
        authoritative.normalize_stock_export_exchanges(["sh", "bj"])

    with pytest.raises(ValueError, match="unsupported exchange"):
        authoritative.normalize_stock_export_exchanges(["hk"])


def test_authoritative_stock_universe_sql_enforces_qe_export_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(sql: str, conn: object, params: dict[str, object]) -> pd.DataFrame:
        captured["sql"] = sql
        captured["params"] = params
        return pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]})

    monkeypatch.setattr(authoritative, "get_conn", lambda: _DummyConn())
    monkeypatch.setattr(authoritative.pd, "read_sql", fake_read_sql)

    codes = authoritative.resolve_stock_universe(
        authoritative.StockUniverseConfig(
            start=date(2024, 1, 1),
            end=date(2026, 4, 28),
            exchanges=None,
            exclude_st=True,
        )
    )

    assert codes == ["000001.SZ", "600000.SH"]
    assert captured["params"] == {
        "exchanges": ["SSE", "SZSE"],
        "end": date(2026, 4, 28),
    }
    sql = str(captured["sql"])
    assert "s.exchange = ANY(%(exchanges)s)" in sql
    assert "s.list_status = 'L'" in sql
    assert "s.list_date <= %(end)s" in sql
    assert "min_listed_days" not in sql
    assert "market.stock_st" in sql
    assert "BSE" not in sql


def test_authoritative_stock_universe_requires_st_and_listing_filters() -> None:
    with pytest.raises(ValueError, match="exclude all stocks"):
        authoritative.resolve_stock_universe(
            authoritative.StockUniverseConfig(
                start=date(2024, 1, 1),
                end=date(2026, 4, 28),
                exclude_st=False,
            )
        )

    with pytest.raises(ValueError, match="exclude delisted"):
        authoritative.resolve_stock_universe(
            authoritative.StockUniverseConfig(
                start=date(2024, 1, 1),
                end=date(2026, 4, 28),
                exclude_delisted_or_paused=False,
            )
        )


def test_authoritative_explicit_bj_codes_fail_fast() -> None:
    with pytest.raises(ValueError, match="BJ/BSE"):
        authoritative.resolve_stock_universe(
            authoritative.StockUniverseConfig(
                start=date(2024, 1, 1),
                end=date(2026, 4, 28),
                ts_codes=["430047.BJ"],
            )
        )


def test_db_reader_base_universe_defaults_to_sh_sz_and_rejects_bj(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(sql: str, conn: object) -> pd.DataFrame:
        captured["sql"] = sql
        return pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]})

    import backend.qlib_exporter.db_reader as db_reader_module

    monkeypatch.setattr(db_reader_module, "get_conn", lambda: _DummyConn())
    monkeypatch.setattr(db_reader_module.pd, "read_sql", fake_read_sql)

    reader = DBReader()
    codes = reader.get_base_ts_codes(start=date(2024, 1, 1), end=date(2026, 4, 28))

    assert codes == ["000001.SZ", "600000.SH"]
    sql = str(captured["sql"])
    assert "ts_code LIKE '%.SH'" in sql
    assert "ts_code LIKE '%.SZ'" in sql
    assert "%.BJ" not in sql

    with pytest.raises(ValueError, match="BJ/BSE"):
        reader.get_base_ts_codes(start=date(2024, 1, 1), end=date(2026, 4, 28), exchanges=["bj"])


def test_rewrite_stock_all_txt_applies_ipo_filter_without_deleting_bins(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    instruments = tmp_path / "instruments"
    features = tmp_path / "features" / "001312.sz"
    instruments.mkdir(parents=True)
    features.mkdir(parents=True)
    (features / "close.1min.bin").write_bytes(b"kept")
    (instruments / "all.txt").write_text(
        "\n".join(
            [
                "000001.SZ\t2024-01-02 09:30:00\t2026-04-28 15:00:00",
                "001312.SZ\t2026-04-21 09:30:00\t2026-04-28 15:00:00",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_read_sql(sql: str, conn: object, params: dict[str, object]) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "001312.SZ"],
                "list_date": [date(1991, 4, 3), date(2026, 4, 21)],
            }
        )

    monkeypatch.setattr(authoritative, "get_conn", lambda: _DummyConn())
    monkeypatch.setattr(authoritative.pd, "read_sql", fake_read_sql)

    summary = authoritative.rewrite_stock_all_txt_for_ipo_filter(bin_dir=tmp_path)

    assert summary["input_rows"] == 2
    assert summary["output_rows"] == 1
    assert summary["skipped_ipo_rows"] == 1
    assert (features / "close.1min.bin").exists()
    assert (instruments / "all.txt").read_text(encoding="utf-8") == (
        "000001.SZ\t2024-01-02 09:30:00\t2026-04-28 15:00:00\n"
    )
    assert (instruments / "all_ipo_filter_summary.json").exists()
