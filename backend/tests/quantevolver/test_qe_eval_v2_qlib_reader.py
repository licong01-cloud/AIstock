from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.quantevolver.qe_eval_v2_qlib_reader import (
    _load_all_close_prices,
    _read_instruments,
    clear_close_cache,
)


def _write_close(path: Path, start_index: int, values: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    np.asarray([float(start_index), *values], dtype="<f").tofile(path)


def test_reader_preserves_all_spans_and_filters_dates_by_their_union(tmp_path: Path) -> None:
    (tmp_path / "calendars").mkdir()
    (tmp_path / "instruments").mkdir()
    dates = pd.date_range("2026-01-01", periods=8, freq="D")
    (tmp_path / "calendars" / "day.txt").write_text(
        "\n".join(str(day.date()) for day in dates), encoding="utf-8"
    )
    (tmp_path / "instruments" / "all.txt").write_text(
        "000001.SZ\t2026-01-02\t2026-01-03\n"
        "000001.SZ\t2026-01-06\t2026-01-07\n",
        encoding="utf-8",
    )
    _write_close(
        tmp_path / "features" / "000001.sz" / "close.day.bin",
        0,
        list(range(1, 9)),
    )

    clear_close_cache()
    spans = _read_instruments(tmp_path)
    result = _load_all_close_prices(tmp_path)

    assert spans == {
        "000001.SZ": [
            ("2026-01-02", "2026-01-03"),
            ("2026-01-06", "2026-01-07"),
        ]
    }
    assert result.index.get_level_values("datetime").tolist() == [
        pd.Timestamp("2026-01-02"),
        pd.Timestamp("2026-01-03"),
        pd.Timestamp("2026-01-06"),
        pd.Timestamp("2026-01-07"),
    ]
    assert result["close"].tolist() == [2.0, 3.0, 6.0, 7.0]
    clear_close_cache()


def test_missing_close_bin_is_reported_without_dropping_other_symbols(
    tmp_path: Path, caplog,
) -> None:
    (tmp_path / "calendars").mkdir()
    (tmp_path / "instruments").mkdir()
    (tmp_path / "calendars" / "day.txt").write_text(
        "2026-01-01\n2026-01-02\n", encoding="utf-8"
    )
    (tmp_path / "instruments" / "all.txt").write_text(
        "000001.SZ\t2026-01-01\t2026-01-02\n"
        "000002.SZ\t2026-01-01\t2026-01-02\n",
        encoding="utf-8",
    )
    _write_close(
        tmp_path / "features" / "000001.sz" / "close.day.bin", 0, [1.0, 2.0]
    )

    clear_close_cache()
    with caplog.at_level(logging.WARNING):
        result = _load_all_close_prices(tmp_path)

    assert set(result.index.get_level_values("instrument")) == {"000001.SZ"}
    assert "without close.day.bin: 1" in caplog.text
    assert "000002.SZ" in caplog.text
    clear_close_cache()
