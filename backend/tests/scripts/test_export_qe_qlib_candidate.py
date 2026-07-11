from __future__ import annotations

import importlib.util
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = ROOT / "scripts" / "export_qe_qlib_candidate.py"
SPEC = importlib.util.spec_from_file_location("export_qe_qlib_candidate", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_load_daily_data_chunks_unique_codes_not_listing_dates(monkeypatch) -> None:
    start = date(2018, 8, 1)
    end = date(2026, 6, 30)
    pool = pd.DataFrame(
        {
            "ts_code": [f"{code:06d}.SZ" for code in range(1000)] + ["000000.SZ"],
            "list_date": [start + timedelta(days=offset) for offset in range(1000)] + [start],
        }
    )
    calls: list[tuple[list[str], date, date, bool, dict[str, date]]] = []

    class FakeReader:
        def load_qlib_daily_data(
            self,
            codes,
            call_start,
            call_end,
            use_tushare_adj,
            instrument_start_dates,
        ):
            batch = list(codes)
            calls.append(
                (batch, call_start, call_end, use_tushare_adj, instrument_start_dates)
            )
            index = pd.MultiIndex.from_tuples(
                [(pd.Timestamp(call_start), batch[0])],
                names=["datetime", "instrument"],
            )
            return pd.DataFrame({"$close": [1.0]}, index=index)

    monkeypatch.setattr(MODULE, "DBReader", FakeReader)

    result = MODULE.load_daily_data(pool, start, end, batch_size=400)

    assert len(calls) == 3
    assert [len(call[0]) for call in calls] == [400, 400, 200]
    assert all(call[1:4] == (start, end, True) for call in calls)
    assert all(set(call[0]) == set(call[4]) for call in calls)
    assert calls[0][4]["000000.SZ"] == start
    assert calls[-1][4]["000999.SZ"] == start + timedelta(days=999)
    assert sum(len(call[0]) for call in calls) == 1000
    assert not result.empty


def test_read_static_schema_columns_accepts_explicit_source(tmp_path) -> None:
    source = tmp_path / "schema.parquet"
    pd.DataFrame(
        {"beta": [1.0], "l2_code_id": [1], "alpha": [2.0]}
    ).to_parquet(source)

    assert MODULE.read_static_schema_columns(source) == [
        "beta",
        "l2_code_id",
        "alpha",
    ]


def test_read_static_schema_columns_rejects_stale_source(tmp_path) -> None:
    source = tmp_path / "stale.parquet"
    pd.DataFrame({"beta": [1.0]}).to_parquet(source)

    with pytest.raises(ValueError, match="lacks l2_code_id"):
        MODULE.read_static_schema_columns(source)


def test_align_static_schema_preserves_l2_int16_and_unknown_code() -> None:
    frame = pd.DataFrame(
        {"alpha": [1, 2], "l2_code_id": [3, None]},
        index=pd.Index(["a", "b"]),
    )

    result = MODULE.align_static_schema(
        frame,
        ["alpha", "l2_code_id", "missing_factor"],
    )

    assert str(result["alpha"].dtype) == "float32"
    assert str(result["missing_factor"].dtype) == "float32"
    assert str(result["l2_code_id"].dtype) == "int16"
    assert result["l2_code_id"].tolist() == [3, -1]
