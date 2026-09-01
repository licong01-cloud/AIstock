from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backend.services.dataset_release.errors import SourceManifestError, SourceSnapshotDrift
from backend.services.dataset_release.source_manifest import (
    ColumnKind,
    ColumnSpec,
    PartitionSpec,
    SourceManifest,
    VerifiedPartitionStream,
    summarize_partition,
)


@pytest.fixture
def partition_spec() -> PartitionSpec:
    return PartitionSpec(
        dataset="market.kline_daily_raw",
        partition_key="2026-07",
        query_version="daily_partition_query_v1",
        columns=(
            ColumnSpec("ts_code", ColumnKind.STRING, required=True),
            ColumnSpec("trade_date", ColumnKind.DATE, required=True),
            ColumnSpec("close", ColumnKind.DECIMAL, required=True),
        ),
        primary_keys=("ts_code", "trade_date"),
    )


def _rows(close: str = "10.25") -> list[dict[str, object]]:
    return [
        {"ts_code": "000001.SZ", "trade_date": date(2026, 7, 30), "close": close},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 7, 31), "close": "10.50"},
    ]


class OneShotRows:
    def __init__(self, rows):
        self.rows = rows
        self.iterations = 0

    def __iter__(self):
        self.iterations += 1
        if self.iterations > 1:
            raise AssertionError("row source was queried more than once")
        yield from self.rows


def test_verified_partition_stream_hashes_and_consumes_same_single_row_stream(
    partition_spec: PartitionSpec,
) -> None:
    planned = summarize_partition(
        partition_spec,
        _rows(),
        ingestion_audit_identity="planner-snapshot-1",
        snapshot_tokens=["xmin:100"],
    )
    source = OneShotRows(_rows())
    consumed = []
    actual = VerifiedPartitionStream(planned, partition_spec).consume(
        source,
        consumed.append,
        ingestion_audit_identity="materializer-snapshot-2",
        snapshot_tokens=["xmin:101"],
    )

    assert source.iterations == 1
    assert actual.content_digest == planned.content_digest
    assert consumed[0]["close"] == Decimal("10.25")
    assert actual.ingestion_audit_identity != planned.ingestion_audit_identity


def test_same_row_count_historical_value_change_changes_root_and_blocks_publish(
    partition_spec: PartitionSpec,
) -> None:
    planned = summarize_partition(
        partition_spec,
        _rows("10.25"),
        ingestion_audit_identity="plan",
    )
    consumed = []
    with pytest.raises(SourceSnapshotDrift) as captured:
        VerifiedPartitionStream(planned, partition_spec).consume(
            _rows("10.26"),
            consumed.append,
            ingestion_audit_identity="actual",
        )
    assert len(consumed) == planned.row_count
    assert captured.value.code == "BLOCKED_SOURCE_SNAPSHOT_DRIFT"
    assert captured.value.context["planned_rows"] == captured.value.context["actual_rows"]
    assert captured.value.context["planned"] != captured.value.context["actual"]


def test_content_and_provenance_roots_are_separate(partition_spec: PartitionSpec) -> None:
    first = summarize_partition(
        partition_spec,
        _rows(),
        ingestion_audit_identity="ingestion-a",
        snapshot_tokens=["job-a"],
    )
    second = summarize_partition(
        partition_spec,
        _rows(),
        ingestion_audit_identity="ingestion-b",
        snapshot_tokens=["job-b"],
    )
    first_manifest = SourceManifest((first,))
    second_manifest = SourceManifest((second,))
    assert first_manifest.source_content_root == second_manifest.source_content_root
    assert first_manifest.source_provenance_root != second_manifest.source_provenance_root


def test_partition_rejects_duplicate_or_unsorted_primary_keys(
    partition_spec: PartitionSpec,
) -> None:
    duplicate = [_rows()[0], _rows()[0]]
    with pytest.raises(SourceManifestError, match="duplicate"):
        summarize_partition(partition_spec, duplicate, ingestion_audit_identity="x")
    with pytest.raises(SourceManifestError, match="order drift"):
        summarize_partition(
            partition_spec,
            list(reversed(_rows())),
            ingestion_audit_identity="x",
        )
