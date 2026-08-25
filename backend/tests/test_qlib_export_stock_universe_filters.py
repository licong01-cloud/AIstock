from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.qlib_exporter import authoritative_bin_exporter as authoritative
from scripts import qlib_authoritative_bin_export as authoritative_cli
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


def test_rewrite_minute_all_txt_uses_physical_feature_bounds_instead_of_stale_metadata(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    instruments = tmp_path / "instruments"
    calendars = tmp_path / "calendars"
    features = tmp_path / "features" / "000001.sz"
    instruments.mkdir(parents=True)
    calendars.mkdir(parents=True)
    features.mkdir(parents=True)
    (instruments / "all.txt").write_text(
        "000001.SZ\t2026-01-05 09:31:00\t2026-04-28 15:00:00\n",
        encoding="utf-8",
    )
    (calendars / "1min.txt").write_text(
        "\n".join(
            [
                "2026-01-05 09:31:00",
                "2026-04-28 15:00:00",
                "2026-06-30 09:31:00",
                "2026-06-30 15:00:00",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    np.asarray([0, 10, 11, 12, 13], dtype="<f4").tofile(features / "close.1min.bin")

    monkeypatch.setattr(
        authoritative,
        "_load_pit_spans_for_all_txt",
        lambda **_kwargs: pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "eligible_start": [date(2026, 1, 5)],
                "eligible_end": [date(2026, 6, 30)],
                "entry_reason": ["eligible"],
                "exit_reason": ["generation_end"],
            }
        ),
    )

    summary = authoritative.rewrite_stock_all_txt_from_pit_spans(
        bin_dir=tmp_path,
        start=date(2026, 1, 5),
        end=date(2026, 6, 30),
        feature_frequency="1min",
    )

    assert (instruments / "all.txt").read_text(encoding="utf-8") == (
        "000001.SZ\t2026-01-05 09:31:00\t2026-06-30 15:00:00\n"
    )
    assert summary["range_authority"] == "physical_qlib_feature_bounds_v1"
    assert summary["physical_range_summary"] == {
        "range_authority": "physical_qlib_feature_bounds_v1",
        "frequency": "1min",
        "field": "close",
        "calendar_rows": 4,
        "feature_files": 1,
        "feature_instruments": 1,
        "feature_start_min": "2026-01-05",
        "feature_end_max": "2026-06-30",
    }


def test_physical_feature_range_rejects_bounds_past_calendar(tmp_path) -> None:
    instruments = tmp_path / "instruments"
    calendars = tmp_path / "calendars"
    features = tmp_path / "features" / "000001.sz"
    instruments.mkdir(parents=True)
    calendars.mkdir(parents=True)
    features.mkdir(parents=True)
    (instruments / "all.txt").write_text(
        "000001.SZ\t2026-01-05 09:31:00\t2026-01-05 15:00:00\n",
        encoding="utf-8",
    )
    (calendars / "1min.txt").write_text(
        "2026-01-05 09:31:00\n2026-01-05 15:00:00\n",
        encoding="utf-8",
    )
    np.asarray([1, 10, 11, 12], dtype="<f4").tofile(features / "close.1min.bin")

    with pytest.raises(RuntimeError, match="feature bounds exceed calendar"):
        authoritative.rewrite_stock_all_txt_from_pit_spans(
            bin_dir=tmp_path,
            start=date(2026, 1, 5),
            end=date(2026, 1, 5),
            feature_frequency="1min",
        )


def test_physical_feature_range_rejects_uncontrolled_frequency(tmp_path) -> None:
    instruments = tmp_path / "instruments"
    instruments.mkdir(parents=True)
    (instruments / "all.txt").write_text(
        "000001.SZ\t2026-01-05 09:31:00\t2026-01-05 15:00:00\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unsupported physical feature frequency"):
        authoritative.rewrite_stock_all_txt_from_pit_spans(
            bin_dir=tmp_path,
            start=date(2026, 1, 5),
            end=date(2026, 1, 5),
            feature_frequency="../day",
        )


def test_minute_cli_requests_physical_feature_authority_for_pit_rewrite(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, object]] = []

    class _FakePitService:
        def ensure_st_pit_universe(self, **_kwargs):
            return {"status": "ready"}

    monkeypatch.setattr(authoritative_cli, "StockUniversePitService", _FakePitService)
    monkeypatch.setattr(authoritative_cli, "run_wsl_dump", lambda **_kwargs: {"ok": True, "returncode": 0})
    monkeypatch.setattr(
        authoritative_cli,
        "rewrite_stock_all_txt_from_pit_spans",
        lambda **kwargs: calls.append(kwargs) or {"mode": "pit_universe_spans"},
    )
    monkeypatch.setattr(authoritative_cli, "write_bin_meta", lambda **_kwargs: None)

    exit_code = authoritative_cli.main(
        [
            "--dataset",
            "stock_minute",
            "--stage",
            "dump",
            "--snapshot-id",
            "minute-candidate",
            "--start",
            "2026-01-05",
            "--end",
            "2026-06-30",
            "--stock-universe-mode",
            "pit_spans",
            "--csv-root",
            str(tmp_path / "csv"),
            "--bin-root",
            str(tmp_path / "bin"),
            "--reports-dir",
            str(tmp_path / "reports"),
        ]
    )

    assert exit_code == 0
    assert len(calls) == 1
    assert calls[0]["feature_frequency"] == "1min"
