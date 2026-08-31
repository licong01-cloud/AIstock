from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.services.dataset_release.streaming_artifacts import (
    ArtifactChunkTooLarge,
    ArtifactSchemaDrift,
    LegacyFixedH5BoundedReadUnsupported,
    build_date_chunks,
    finalize_h5_from_parquet_chunks,
    finalize_parquet_chunks,
    iter_hdf_frames,
    iter_parquet_frames,
    write_frame_parquet_atomic,
)


def _frame(start: str, periods: int, *, column: str = "value") -> pd.DataFrame:
    index = pd.MultiIndex.from_product(
        [pd.date_range(start, periods=periods, freq="D"), ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    return pd.DataFrame({column: pd.Series(range(periods), dtype="float32").to_numpy()}, index=index)


def test_date_chunks_are_contiguous_and_cover_scope() -> None:
    chunks = build_date_chunks(date(2025, 11, 15), date(2026, 7, 31), months=3)
    assert [(item.start, item.end) for item in chunks] == [
        (date(2025, 11, 15), date(2026, 2, 14)),
        (date(2026, 2, 15), date(2026, 5, 14)),
        (date(2026, 5, 15), date(2026, 7, 31)),
    ]
    assert [item.ordinal for item in chunks] == [0, 1, 2]


def test_parquet_row_groups_finalize_to_bounded_hdf_table(tmp_path) -> None:
    first = _frame("2026-07-01", 3)
    second = _frame("2026-07-04", 3)
    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"
    first_receipt = write_frame_parquet_atomic(first, first_path, row_group_size=2)
    second_receipt = write_frame_parquet_atomic(second, second_path, row_group_size=2)
    assert first_receipt["max_row_group_rows"] <= 2
    assert second_receipt["max_row_group_rows"] <= 2

    h5_path = tmp_path / "factor.h5"
    h5_receipt = finalize_h5_from_parquet_chunks([first_path, second_path], h5_path, max_rows_in_memory=2)
    streamed = list(iter_hdf_frames(h5_path, chunksize=2))
    restored = pd.concat(streamed).sort_index()

    pd.testing.assert_frame_equal(restored, pd.concat([first, second]))
    assert h5_receipt["rows"] == 6
    assert h5_receipt["max_rows_in_memory"] <= 2
    assert all(len(frame) <= 2 for frame in streamed)

    aggregate = tmp_path / "factor.parquet"
    aggregate_receipt = finalize_parquet_chunks([first_path, second_path], aggregate, max_rows_in_memory=2)
    assert aggregate_receipt["rows"] == 6
    assert aggregate_receipt["max_rows_in_memory"] <= 2
    restored_parquet = pd.concat(iter_parquet_frames([aggregate], max_rows=2))
    pd.testing.assert_frame_equal(restored_parquet, pd.concat([first, second]))


def test_legacy_fixed_h5_is_typed_block_not_full_read(tmp_path) -> None:
    path = tmp_path / "legacy_fixed.h5"
    _frame("2026-07-01", 3).to_hdf(path, key="data", format="fixed")

    with pytest.raises(
        LegacyFixedH5BoundedReadUnsupported,
        match="cannot be bounded-read safely",
    ):
        list(iter_hdf_frames(path, chunksize=2))


def test_hdf_reader_rejects_physical_index_order_drift(tmp_path) -> None:
    path = tmp_path / "unordered.h5"
    _frame("2026-07-01", 3).iloc[::-1].to_hdf(path, key="data", format="table")

    with pytest.raises(ArtifactSchemaDrift, match="physically monotonically increasing"):
        list(iter_hdf_frames(path, chunksize=2))


def test_row_group_bound_is_checked_before_materialization(tmp_path) -> None:
    path = tmp_path / "oversized.parquet"
    _frame("2026-07-01", 3).to_parquet(path, row_group_size=3)

    with pytest.raises(ArtifactChunkTooLarge, match="before read"):
        list(iter_parquet_frames([path], max_rows=2))


def test_hdf_finalizer_rejects_schema_drift(tmp_path) -> None:
    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"
    write_frame_parquet_atomic(_frame("2026-07-01", 2), first_path, row_group_size=2)
    write_frame_parquet_atomic(
        _frame("2026-07-03", 2, column="other"),
        second_path,
        row_group_size=2,
    )

    with pytest.raises(ArtifactSchemaDrift, match="ordered columns"):
        finalize_h5_from_parquet_chunks(
            [first_path, second_path],
            tmp_path / "drift.h5",
            max_rows_in_memory=2,
        )
