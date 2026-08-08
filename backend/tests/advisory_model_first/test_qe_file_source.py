from __future__ import annotations

from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.qe_file_source import load_static_factors, load_suspend_rows


def test_static_factor_reader_uses_date_symbol_and_column_projection(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2024-07-04", "2024-07-04", "2024-07-05"]),
            "instrument": ["000001.SZ", "000002.SZ", "000001.SZ"],
            "db_pe_ttm": [1.0, 2.0, 3.0],
            "unused": [4.0, 5.0, 6.0],
        }
    ).set_index(["datetime", "instrument"])
    frame.to_parquet(tmp_path / "static_factors.parquet")
    result = load_static_factors(
        tmp_path,
        columns=["db_pe_ttm"],
        start="2024-07-04",
        end="2024-07-04",
        instruments=["000001.SZ"],
    )
    assert result.index.tolist() == [(pd.Timestamp("2024-07-04"), "000001.SZ")]
    assert result.columns.tolist() == ["db_pe_ttm"]


def test_suspend_reader_keeps_only_exact_suspensions(tmp_path: Path) -> None:
    pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2024-07-04", "2024-07-04", "2024-07-05"]),
            "ts_code": ["000001.SZ", "000002.SZ", "000001.SZ"],
            "suspend_type": ["S", "R", "S"],
        }
    ).to_parquet(tmp_path / "suspend_d.parquet")
    result = load_suspend_rows(
        tmp_path,
        start="2024-07-04",
        end="2024-07-04",
        instruments=["000001.SZ", "000002.SZ"],
    )
    assert result[["trade_date", "instrument"]].to_dict("records") == [
        {"trade_date": pd.Timestamp("2024-07-04"), "instrument": "000001.SZ"}
    ]
