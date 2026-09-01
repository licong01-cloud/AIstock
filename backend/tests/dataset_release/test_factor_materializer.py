from __future__ import annotations

from pathlib import Path
import os

import numpy as np
import pandas as pd
import pytest

from backend.services.dataset_release.factor_materializer import (
    FACTOR_H5_DATASETS,
    FACTOR_H5_DTYPES,
    FACTOR_H5_SCHEMAS,
    STATIC_DATASET,
    FactorBundleMaterializer,
    FactorCheckpointConflict,
    FactorMaterializationSpec,
    SealedFactorChunk,
    merge_factor_partition_by_instrument,
)
from backend.services.dataset_release.streaming_artifacts import (
    iter_parquet_frames,
    sha256_file,
    write_frame_parquet_atomic,
)
from backend.services.dataset_release.static_schema import STATIC_ORDERED_COLUMNS


def _index(day: str) -> pd.MultiIndex:
    return pd.MultiIndex.from_product(
        [[pd.Timestamp(day)], ["000001.SZ", "000002.SZ"]],
        names=["datetime", "instrument"],
    )


def _static_columns() -> tuple[str, ...]:
    return STATIC_ORDERED_COLUMNS


def _build_spec(source: Path, staging: Path) -> FactorMaterializationSpec:
    chunks: list[SealedFactorChunk] = []
    static_columns = _static_columns()
    for dataset in (*FACTOR_H5_DATASETS, STATIC_DATASET):
        for ordinal, day in enumerate(("2026-07-30", "2026-07-31")):
            index = _index(day)
            if dataset == STATIC_DATASET:
                frame = pd.DataFrame(index=index)
                for name in static_columns:
                    frame[name] = (
                        np.asarray([1, -1], dtype=np.int16)
                        if name == "l2_code_id"
                        else np.asarray([ordinal + 1, ordinal + 2], dtype=np.float32)
                    )
            else:
                frame = pd.DataFrame(index=index)
                for position, column in enumerate(FACTOR_H5_SCHEMAS[dataset]):
                    dtype = FACTOR_H5_DTYPES[dataset][column]
                    frame[column] = (
                        np.asarray([1, -1], dtype=np.int16)
                        if dtype == "int16"
                        else np.asarray(
                            [ordinal + position + 1, ordinal + position + 2],
                            dtype=np.float32,
                        )
                    )
            relative = Path(dataset) / f"2026-07-{ordinal}.parquet"
            path = source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            write_frame_parquet_atomic(frame, path, row_group_size=1)
            chunks.append(
                SealedFactorChunk(
                    dataset=dataset,
                    partition_key=f"2026-07-{ordinal}",
                    relative_path=relative.as_posix(),
                    sha256=sha256_file(path),
                    rows=len(frame),
                    ordered_columns=tuple(str(value) for value in frame.columns),
                )
            )
    return FactorMaterializationSpec(
        source_root=source,
        staging_root=staging,
        chunks=tuple(chunks),
        static_ordered_columns=static_columns,
        row_group_rows=2,
    )


def test_factor_bundle_streams_partitions_and_resumes_by_hash(tmp_path: Path) -> None:
    source = tmp_path / "sealed"
    staging = tmp_path / "candidate-staging"
    source.mkdir()
    staging.mkdir()
    spec = _build_spec(source, staging)
    checkpoints: list[int] = []

    first = FactorBundleMaterializer().materialize(spec, checkpoint=lambda: checkpoints.append(1))
    second = FactorBundleMaterializer().materialize(spec)

    assert first.receipt["status"] == "PASS"
    assert first.receipt["memory_contract"] == {
        "mode": "partitioned_parquet_to_new_aggregate_v1",
        "max_rows_in_memory": 2,
        "whole_panel_frames_retained": 0,
    }
    assert first.receipt["outputs"] == second.receipt["outputs"]
    assert all(
        "path" not in value and not Path(str(value["artifact_relative_path"])).is_absolute()
        for value in first.receipt["outputs"].values()
    )
    assert len(checkpoints) == len(spec.chunks) + len(FACTOR_H5_DATASETS) + 1
    assert pd.read_hdf(staging / "factor_bundle" / "daily_pv.h5", key="data").shape == (4, 7)
    static = pd.read_parquet(staging / "factor_bundle" / "static_factors.parquet")
    assert static.shape == (4, 121)
    assert str(static["l2_code_id"].dtype) == "int16"


def test_factor_candidate_chunks_do_not_alias_mutable_source_and_refs_survive_rename(
    tmp_path: Path,
) -> None:
    source = tmp_path / "sealed"
    staging = tmp_path / "candidate-staging"
    source.mkdir()
    staging.mkdir()
    spec = _build_spec(source, staging)
    receipt = FactorBundleMaterializer().materialize(spec).receipt
    planned = spec.chunks[0]
    source_path = source / planned.relative_path
    candidate_path = staging / "factor_bundle" / "partitions" / planned.dataset / f"{planned.partition_key}.parquet"
    candidate_before = candidate_path.read_bytes()

    assert not os.path.samefile(source_path, candidate_path)
    with source_path.open("ab") as handle:
        handle.write(b"source-cache-corruption")
    assert candidate_path.read_bytes() == candidate_before

    published = tmp_path / "published-candidate"
    os.rename(staging, published)
    for value in receipt["outputs"].values():
        assert (published / value["artifact_relative_path"]).is_file()


def test_factor_resume_never_overwrites_conflicting_output(tmp_path: Path) -> None:
    source = tmp_path / "sealed"
    staging = tmp_path / "candidate-staging"
    source.mkdir()
    staging.mkdir()
    spec = _build_spec(source, staging)
    materializer = FactorBundleMaterializer()
    materializer.materialize(spec)

    target = staging / "factor_bundle" / "daily_pv.h5"
    original = target.read_bytes()
    with target.open("ab") as handle:
        handle.write(b"conflict")

    with pytest.raises(FactorCheckpointConflict):
        materializer.materialize(spec)
    assert target.read_bytes() == original + b"conflict"


def test_factor_plan_requires_all_eight_artifact_authorities(tmp_path: Path) -> None:
    source = tmp_path / "sealed"
    staging = tmp_path / "candidate-staging"
    source.mkdir()
    staging.mkdir()
    complete = _build_spec(source, staging)

    with pytest.raises(Exception, match="omits required datasets"):
        FactorMaterializationSpec(
            source_root=source,
            staging_root=staging,
            chunks=tuple(item for item in complete.chunks if item.dataset != STATIC_DATASET),
            static_ordered_columns=complete.static_ordered_columns,
            row_group_rows=2,
        )


def test_selective_factor_partition_merge_replaces_only_affected_code(
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
            "open": np.asarray([1, 2, 3, 4], dtype=np.float32),
            "close": np.asarray([11, 12, 13, 14], dtype=np.float32),
        },
        index=index,
    )
    replacement = baseline.loc[baseline.index.get_level_values("instrument") == "000001.SZ"].copy()
    replacement.loc[:, "open"] = np.asarray([101, 103], dtype=np.float32)
    baseline_path = tmp_path / "baseline.parquet"
    replacement_path = tmp_path / "replacement.parquet"
    target = tmp_path / "merged.parquet"
    write_frame_parquet_atomic(baseline, baseline_path, row_group_size=2)
    write_frame_parquet_atomic(replacement, replacement_path, row_group_size=1)

    chunk, receipt = merge_factor_partition_by_instrument(
        baseline_path=baseline_path,
        replacement_path=replacement_path,
        target_path=target,
        dataset="daily_pv",
        partition_key="2026-07",
        affected_instruments=("000001.SZ",),
        row_group_rows=2,
        max_rows=10,
    )

    merged = pd.concat(list(iter_parquet_frames([target], max_rows=2))).sort_index()
    pd.testing.assert_frame_equal(
        merged.loc[merged.index.get_level_values("instrument") == "000002.SZ"],
        baseline.loc[baseline.index.get_level_values("instrument") == "000002.SZ"],
    )
    assert merged.loc[(pd.Timestamp("2026-07-30"), "000001.SZ"), "open"] == 101
    assert chunk.rows == 4
    assert receipt["replacement_rows"] == 2
    assert receipt["unaffected_rows"] == 2
    assert receipt["whole_market_history_frames_retained"] == 0
