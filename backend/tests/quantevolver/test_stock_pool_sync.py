from __future__ import annotations

from datetime import date

import pytest

from backend.services.quantevolver.stock_pool_sync import read_stock_pool_snapshot


def test_read_stock_pool_snapshot_preserves_pit_intervals(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("STOCK_POOL_OUTPUT_DIR", str(tmp_path))
    pool = tmp_path / "filtered_pool_fixture.txt"
    pool.write_text(
        "600000.SH\t2025-01-02\t2025-02-28\n"
        "600000.SH\t2025-03-03\t2025-12-31\n"
        "000001.SZ\t2025-01-02\t2025-12-31\n",
        encoding="utf-8",
    )

    snapshot = read_stock_pool_snapshot("filtered_pool_fixture")

    assert snapshot.filename == "filtered_pool_fixture.txt"
    assert snapshot.instrument_name == "filtered_pool_fixture"
    assert len(snapshot.sha256) == 64
    assert snapshot.intervals[0].ts_code == "600000.SH"
    assert snapshot.intervals[0].eligible_start == date(2025, 1, 2)
    assert snapshot.intervals[1].eligible_start == date(2025, 3, 3)


def test_read_stock_pool_snapshot_rejects_overlapping_intervals(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("STOCK_POOL_OUTPUT_DIR", str(tmp_path))
    pool = tmp_path / "filtered_pool_fixture.txt"
    pool.write_text(
        "600000.SH\t2025-01-02\t2025-03-31\n"
        "600000.SH\t2025-03-31\t2025-12-31\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="overlap or are unsorted"):
        read_stock_pool_snapshot("filtered_pool_fixture")
