from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.qlib_exporter import authoritative_bin_exporter as authoritative
from backend.qlib_exporter import router as qlib_router
from scripts import qlib_authoritative_bin_export as authoritative_cli
from backend.qlib_exporter.db_reader import DBReader


class _DummyConn:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


def _daily_csv_frame(code: str, dates: list[date]) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "date": [value.isoformat() for value in dates],
            "symbol": [code] * len(dates),
        }
    )
    for column in authoritative.DAILY_REQUIRED_COLUMNS[2:]:
        frame[column] = 1.0
    return frame.loc[:, authoritative.DAILY_REQUIRED_COLUMNS]


def test_daily_csv_resume_reuses_only_complete_atomic_physical_range(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    code = "302132.SZ"
    dates = [date(2026, 8, 28), date(2026, 8, 31)]
    raw = pd.DataFrame({"trade_date": dates, "ts_code": [code, code]})
    csv_dir = tmp_path / "snapshot" / "stock_daily"
    csv_dir.mkdir(parents=True)
    _daily_csv_frame(code, dates).to_csv(csv_dir / f"{code}.csv", index=False)
    monkeypatch.setattr(authoritative, "resolve_stock_universe", lambda _config: [code])
    monkeypatch.setattr(authoritative, "_load_daily_raw", lambda *_args, **_kwargs: raw)

    def must_not_rebuild(*_args, **_kwargs):
        raise AssertionError("complete atomic daily CSV must be reused")

    monkeypatch.setattr(authoritative, "_build_daily_expected_frame", must_not_rebuild)

    summary = authoritative.export_stock_daily_csv(
        snapshot_id="snapshot",
        start=dates[0],
        end=dates[-1],
        csv_root=tmp_path,
        resume_csv=True,
    )

    assert summary.resumed_csv_files == 1
    assert summary.csv_files == 1
    assert summary.csv_rows == 2
    assert summary.stocks_written == 1


def test_daily_csv_resume_rebuilds_stale_or_incomplete_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    code = "302132.SZ"
    dates = [date(2026, 8, 28), date(2026, 8, 31)]
    raw = pd.DataFrame({"trade_date": dates, "ts_code": [code, code]})
    csv_dir = tmp_path / "snapshot" / "stock_daily"
    csv_dir.mkdir(parents=True)
    _daily_csv_frame(code, dates[:1]).to_csv(csv_dir / f"{code}.csv", index=False)
    monkeypatch.setattr(authoritative, "resolve_stock_universe", lambda _config: [code])
    monkeypatch.setattr(authoritative, "_load_daily_raw", lambda *_args, **_kwargs: raw)
    monkeypatch.setattr(
        authoritative,
        "_build_daily_expected_frame",
        lambda *_args, **_kwargs: _daily_csv_frame(code, dates),
    )

    summary = authoritative.export_stock_daily_csv(
        snapshot_id="snapshot",
        start=dates[0],
        end=dates[-1],
        csv_root=tmp_path,
        resume_csv=True,
    )

    assert summary.resumed_csv_files == 0
    assert summary.csv_files == 1
    assert summary.csv_rows == 2
    assert pd.read_csv(csv_dir / f"{code}.csv")["date"].tolist() == [
        "2026-08-28",
        "2026-08-31",
    ]


def test_daily_csv_rejects_overwrite_and_resume_together(tmp_path) -> None:
    with pytest.raises(ValueError, match="cannot both be true"):
        authoritative.export_stock_daily_csv(
            snapshot_id="snapshot",
            start=date(2026, 8, 1),
            end=date(2026, 8, 31),
            csv_root=tmp_path,
            overwrite_csv=True,
            resume_csv=True,
        )


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


def test_missing_stk_limit_rows_use_versioned_board_and_st_rules() -> None:
    required = pd.DataFrame(
        {
            "ts_code": ["000622.SZ", "000001.SZ"],
            "trade_date": [date(2024, 7, 23), date(2024, 7, 23)],
        }
    )
    history = pd.DataFrame(
        {
            "ts_code": ["000622.SZ", "000001.SZ"],
            "trade_date": [date(2024, 7, 22), date(2024, 7, 22)],
            "daily_close": [10.0, 10.0],
        }
    )
    factors = pd.DataFrame(
        {
            "ts_code": ["000622.SZ", "000622.SZ", "000001.SZ", "000001.SZ"],
            "trade_date": [date(2024, 7, 22), date(2024, 7, 23)] * 2,
            "adj_factor": [1.0, 1.0, 1.0, 1.0],
        }
    )
    st_periods = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "start_date": [date(2024, 7, 1)],
            "end_date": [date(2024, 7, 31)],
        }
    )

    completed, count = authoritative._complete_missing_limits_with_rules(
        limits=pd.DataFrame(
            columns=["ts_code", "trade_date", "prev_close", "up_limit_price", "down_limit_price"]
        ),
        daily_history=history,
        adj_factors=factors,
        st_periods=st_periods,
        required_keys=required,
    )

    values = completed.set_index("ts_code")
    assert count == 2
    assert values.loc["000622.SZ", ["prev_close", "up_limit_price", "down_limit_price"]].tolist() == [
        10.0,
        11.0,
        9.0,
    ]
    assert values.loc["000001.SZ", ["prev_close", "up_limit_price", "down_limit_price"]].tolist() == [
        10.0,
        10.5,
        9.5,
    ]


def test_missing_stk_limit_for_302_prefix_uses_chinext_rule() -> None:
    required = pd.DataFrame(
        {"ts_code": ["302132.SZ"], "trade_date": [date(2026, 5, 8)]}
    )
    history = pd.DataFrame(
        {
            "ts_code": ["302132.SZ"],
            "trade_date": [date(2026, 5, 7)],
            "daily_close": [10.0],
        }
    )
    factors = pd.DataFrame(
        {
            "ts_code": ["302132.SZ", "302132.SZ"],
            "trade_date": [date(2026, 5, 7), date(2026, 5, 8)],
            "adj_factor": [1.0, 1.0],
        }
    )

    completed, count = authoritative._complete_missing_limits_with_rules(
        limits=pd.DataFrame(
            columns=["ts_code", "trade_date", "prev_close", "up_limit_price", "down_limit_price"]
        ),
        daily_history=history,
        adj_factors=factors,
        st_periods=pd.DataFrame(columns=["ts_code", "start_date", "end_date"]),
        required_keys=required,
    )

    assert count == 1
    assert completed.iloc[0][["prev_close", "up_limit_price", "down_limit_price"]].tolist() == [
        10.0,
        12.0,
        8.0,
    ]


def test_partial_stk_limit_preserves_existing_values_and_fills_only_missing_fields() -> None:
    limits = pd.DataFrame(
        {
            "ts_code": ["000622.SZ"],
            "trade_date": [date(2024, 7, 23)],
            "prev_close": [10.0],
            "up_limit_price": [12.0],
            "down_limit_price": [None],
        }
    )
    history = pd.DataFrame(
        {"ts_code": ["000622.SZ"], "trade_date": [date(2024, 7, 22)], "daily_close": [10.0]}
    )
    factors = pd.DataFrame(
        {
            "ts_code": ["000622.SZ", "000622.SZ"],
            "trade_date": [date(2024, 7, 22), date(2024, 7, 23)],
            "adj_factor": [1.0, 1.0],
        }
    )

    completed, count = authoritative._complete_missing_limits_with_rules(
        limits=limits,
        daily_history=history,
        adj_factors=factors,
        st_periods=pd.DataFrame(columns=["ts_code", "start_date", "end_date"]),
        required_keys=limits[["ts_code", "trade_date"]],
    )

    assert count == 1
    assert completed.iloc[0]["up_limit_price"] == 12.0
    assert completed.iloc[0]["down_limit_price"] == 9.0


def test_missing_daily_close_uses_last_intraday_close_as_rule_reference() -> None:
    minute = pd.DataFrame(
        {
            "ts_code": ["000622.SZ", "000622.SZ", "000622.SZ"],
            "trade_time": [
                "2024-07-22 09:31:00",
                "2024-07-22 15:00:00",
                "2024-07-23 09:31:00",
            ],
            "close_li": [1600, 1630, 1640],
        }
    )
    history = authoritative._augment_daily_history_from_price_rows(
        pd.DataFrame(columns=["ts_code", "trade_date", "daily_close"]),
        minute,
    )
    factors = pd.DataFrame(
        {
            "ts_code": ["000622.SZ", "000622.SZ"],
            "trade_date": [date(2024, 7, 22), date(2024, 7, 23)],
            "adj_factor": [2.525, 2.525],
        }
    )

    completed, count = authoritative._complete_missing_limits_with_rules(
        limits=pd.DataFrame(
            {
                "ts_code": ["000622.SZ"],
                "trade_date": [date(2024, 7, 23)],
                "prev_close": [None],
                "up_limit_price": [1.79],
                "down_limit_price": [1.47],
            }
        ),
        daily_history=history,
        adj_factors=factors,
        st_periods=pd.DataFrame(columns=["ts_code", "start_date", "end_date"]),
        required_keys=pd.DataFrame(
            {"ts_code": ["000622.SZ"], "trade_date": [date(2024, 7, 23)]}
        ),
    )

    assert count == 1
    assert completed.iloc[0]["prev_close"] == 1.63


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
        allowed_bin_root=tmp_path.parent,
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
            allowed_bin_root=tmp_path.parent,
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
            allowed_bin_root=tmp_path.parent,
        )


def test_minute_cli_requests_physical_feature_authority_for_pit_rewrite(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, object]] = []

    class _FakePitService:
        def get_status_readonly(self, **_kwargs):
            return {
                "status": "ready",
                "dirty": False,
                "rule_version": authoritative_cli.DEFAULT_ST_PIT_RULE_VERSION,
                "scope": "sh_sz_a_ex_bj_st_excluded_pit",
                "start_date": date(2018, 8, 1),
                "end_date": date(2026, 8, 31),
            }

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
    assert calls[0]["allowed_bin_root"] == tmp_path / "bin"


def test_canonical_pit_export_preflight_is_readonly_and_never_rebuilds() -> None:
    calls: list[str] = []

    class _FakePitService:
        def get_status_readonly(self, *, universe_key: str):
            calls.append(universe_key)
            return {
                "status": "ready",
                "dirty": False,
                "rule_version": authoritative_cli.CANONICAL_PIT_RULE_VERSION,
                "scope": authoritative_cli.CANONICAL_PIT_SCOPE,
                "start_date": date(2018, 8, 1),
                "end_date": date(2026, 8, 31),
            }

        def ensure_st_pit_universe(self, **_kwargs):
            raise AssertionError("direct export must not rebuild PIT")

        def ensure_canonical_pit_universe(self, **_kwargs):
            raise AssertionError("direct export must not rebuild PIT")

    receipt = authoritative_cli.require_readonly_pit_coverage(
        _FakePitService(),
        universe_key=authoritative_cli.CANONICAL_PIT_UNIVERSE_KEY,
        start=date(2024, 1, 2),
        end=date(2026, 8, 31),
    )

    assert calls == [authoritative_cli.CANONICAL_PIT_UNIVERSE_KEY]
    assert receipt["read_only"] is True
    assert receipt["source_scan"] is False
    assert receipt["database_writes"] == 0
    assert receipt["rule_version"] == authoritative_cli.CANONICAL_PIT_RULE_VERSION


def test_canonical_pit_export_preflight_rejects_incomplete_coverage_without_write() -> None:
    class _FakePitService:
        def get_status_readonly(self, **_kwargs):
            return {
                "status": "ready",
                "dirty": False,
                "rule_version": authoritative_cli.CANONICAL_PIT_RULE_VERSION,
                "scope": authoritative_cli.CANONICAL_PIT_SCOPE,
                "start_date": date(2018, 8, 1),
                "end_date": date(2026, 7, 31),
            }

    with pytest.raises(RuntimeError, match="not ready"):
        authoritative_cli.require_readonly_pit_coverage(
            _FakePitService(),
            universe_key=authoritative_cli.CANONICAL_PIT_UNIVERSE_KEY,
            start=date(2024, 1, 2),
            end=date(2026, 8, 31),
        )


def test_backend_minute_finalize_passes_physical_range_root(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    class _DumpResult:
        ok = True
        stdout = "dumped"

    monkeypatch.setattr(
        qlib_router,
        "rewrite_stock_all_txt_from_pit_spans",
        lambda **kwargs: calls.append(kwargs) or {"mode": "pit_universe_spans"},
    )

    ok, _stdout, error, summary = qlib_router._finalize_stock_dump_result(
        _DumpResult(),
        tmp_path / "candidate",
        stock_universe_mode="pit_spans",
        universe_key=authoritative.DEFAULT_PIT_UNIVERSE_KEY,
        start=date(2026, 1, 5),
        end=date(2026, 6, 30),
        feature_frequency="1min",
        allowed_bin_root=tmp_path,
    )

    assert ok is True
    assert error is None
    assert summary == {"mode": "pit_universe_spans"}
    assert calls[0]["feature_frequency"] == "1min"
    assert calls[0]["allowed_bin_root"] == tmp_path
