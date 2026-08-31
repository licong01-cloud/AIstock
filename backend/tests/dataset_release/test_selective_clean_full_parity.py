from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.dataset_release.contracts import Component, ComponentAction
from backend.services.dataset_release.dependency_graph import (
    DatasetDependencyGraph,
    RevisionEvent,
    RevisionKind,
)
from backend.services.dataset_release.factor_materializer import merge_factor_partition_by_instrument
from backend.services.dataset_release.streaming_artifacts import (
    iter_parquet_frames,
    sha256_file,
    write_frame_parquet_atomic,
)


def test_selective_factor_merge_matches_clean_full_index_dtype_nan_and_values(
    tmp_path: Path,
) -> None:
    index = pd.MultiIndex.from_product(
        [
            pd.to_datetime(["2026-07-30", "2026-07-31"]),
            ["000001.SZ", "000002.SZ"],
        ],
        names=["datetime", "instrument"],
    )
    baseline = pd.DataFrame(
        {
            "open": np.asarray([1.0, 2.0, np.nan, 4.0], dtype=np.float32),
            "rank": np.asarray([0.25, 0.75, np.nan, 0.50], dtype=np.float32),
        },
        index=index,
    )
    replacement = baseline.xs("000001.SZ", level="instrument", drop_level=False).copy()
    replacement.loc[:, "open"] = np.asarray([11.0, 13.0], dtype=np.float32)
    replacement.loc[:, "rank"] = np.asarray([0.80, np.nan], dtype=np.float32)
    clean_full = baseline.copy()
    clean_full.loc[replacement.index, replacement.columns] = replacement

    baseline_path = tmp_path / "baseline.parquet"
    replacement_path = tmp_path / "replacement.parquet"
    target_path = tmp_path / "selective.parquet"
    write_frame_parquet_atomic(baseline, baseline_path, row_group_size=2)
    write_frame_parquet_atomic(replacement, replacement_path, row_group_size=1)
    baseline_digest = sha256_file(baseline_path)

    chunk, receipt = merge_factor_partition_by_instrument(
        baseline_path=baseline_path,
        replacement_path=replacement_path,
        target_path=target_path,
        dataset="daily_pv",
        partition_key="2026-07",
        affected_instruments=("000001.SZ",),
        row_group_rows=2,
        max_rows=8,
    )
    observed = pd.concat(list(iter_parquet_frames([target_path], max_rows=2))).sort_index()

    pd.testing.assert_frame_equal(observed, clean_full.sort_index(), check_dtype=True)
    assert np.array_equal(observed.isna().to_numpy(), clean_full.sort_index().isna().to_numpy())
    assert sha256_file(baseline_path) == baseline_digest
    assert chunk.rows == len(clean_full)
    assert receipt["replacement_rows"] == len(replacement)
    assert receipt["whole_market_history_frames_retained"] == 0


def test_pit_revision_requires_cross_component_rebuild_not_all_txt_only() -> None:
    graph = DatasetDependencyGraph(
        dataset_start=date(2018, 8, 1),
        cutoff=date(2026, 7, 31),
        trading_dates=(date(2026, 7, 30), date(2026, 7, 31)),
    )
    invalidations = graph.propagate(
        RevisionEvent(
            kind=RevisionKind.PIT_SPAN,
            dataset="market.stock_universe_pit_spans",
            instruments=("600462.SH",),
            start=date(2025, 7, 18),
            end=date(2025, 7, 21),
        )
    )

    assert {item.component for item in invalidations} == {
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    }
    assert all(item.action is ComponentAction.SELECTIVE_REBUILD for item in invalidations)
    assert all(item.instruments == ("600462.SH",) for item in invalidations)
