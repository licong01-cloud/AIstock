from __future__ import annotations

import json
import tracemalloc

import pytest

from backend.services.dataset_release.cas_store import (
    CASCorruptionError,
    CASStore,
    canonical_json_bytes,
)
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.sealed_source_reader import (
    CASSealedPartitionReader,
)
from backend.services.dataset_release.source_authority import (
    SOURCE_PARTITION_ROWS_SCHEMA,
)
from backend.services.dataset_release.errors import SourceSnapshotDrift
from backend.services.dataset_release.source_manifest import (
    CanonicalPartitionHasher,
    ColumnKind,
    ColumnSpec,
    PartitionSpec,
)
from backend.services.dataset_release.source_rows_codec import (
    StreamingCompressionStats,
    iter_gzip_level1,
)


def _sealed_partition(tmp_path):
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    spec = PartitionSpec(
        dataset="kline_daily_raw",
        partition_key="2026-07-01_2026-07-31",
        query_version="fixture-query-v1",
        columns=(
            ColumnSpec("row_key", ColumnKind.STRING, True),
            ColumnSpec("row_payload", ColumnKind.STRING, True),
        ),
        primary_keys=("row_key",),
    )
    ingestion = "fixture-ingestion-v1"
    hasher = CanonicalPartitionHasher(spec, ingestion_audit_identity=ingestion, snapshot_tokens=())
    rows = [
        {
            "row_key": json.dumps(["000001.SZ", "20260730"]),
            "row_payload": json.dumps(
                {"ts_code": "000001.SZ", "trade_date": "2026-07-30", "close": 10.0},
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
        {
            "row_key": json.dumps(["000001.SZ", "20260731"]),
            "row_payload": json.dumps(
                {"ts_code": "000001.SZ", "trade_date": "2026-07-31", "close": 10.5},
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    ]
    normalized = [hasher.update(value) for value in rows]
    summary = hasher.finish()
    header = {
        "schema_version": SOURCE_PARTITION_ROWS_SCHEMA,
        "partition_identity": spec.identity,
        "query_version": spec.query_version,
        "schema_digest": spec.schema_digest,
        "columns": [item.as_dict() for item in spec.columns],
        "primary_keys": list(spec.primary_keys),
        "source_table_identity": "market.kline_daily_raw",
        "source_table_schema_digest": "a" * 64,
    }
    uncompressed = [
        canonical_json_bytes(header) + b"\n",
        *(canonical_json_bytes(value) + b"\n" for value in normalized),
    ]
    compression = StreamingCompressionStats()
    reference = cas.put_stream(iter_gzip_level1(uncompressed, compression))
    partition = {
        "dataset": spec.dataset,
        "partition_key": spec.partition_key,
        "query_version": spec.query_version,
        "schema_digest": spec.schema_digest,
        "columns": [item.as_dict() for item in spec.columns],
        "primary_keys": list(spec.primary_keys),
        "timezone_name": spec.timezone_name,
        "null_marker": spec.null_marker,
        "float_non_finite_policy": spec.float_non_finite_policy,
        "source_table_identity": header["source_table_identity"],
        "source_table_schema_digest": header["source_table_schema_digest"],
        "ingestion_audit_identity": ingestion,
        "row_count": summary.row_count,
        "required_null_count": summary.required_null_count,
        "duplicate_count": summary.duplicate_count,
        "content_digest": summary.content_digest,
        "merkle_root": summary.merkle_root,
        "rows_ref": reference.as_dict(),
        **compression.as_descriptor_fields(),
    }
    return cas, partition


def test_reader_streams_exact_cas_rows_without_get_bytes(tmp_path, monkeypatch) -> None:
    cas, partition = _sealed_partition(tmp_path)

    def reject_get_bytes(*_args, **_kwargs):
        raise AssertionError("data-bearing CAS must not use get_bytes")

    monkeypatch.setattr(cas, "get_bytes", reject_get_bytes)
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)

    frame = reader.read_frame("kline_daily_raw", partition["partition_key"])

    assert frame.to_dict("records") == [
        {"ts_code": "000001.SZ", "trade_date": "2026-07-30", "close": 10.0},
        {"ts_code": "000001.SZ", "trade_date": "2026-07-31", "close": 10.5},
    ]


def test_reader_validates_cas_in_the_same_pass_without_preverify(tmp_path, monkeypatch) -> None:
    cas, partition = _sealed_partition(tmp_path)
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)

    def reject_verify(*_args, **_kwargs):
        raise AssertionError("sealed reader must not pre-read CAS through verify")

    monkeypatch.setattr(cas, "verify", reject_verify)
    rows = list(reader.iter_rows("kline_daily_raw", partition["partition_key"]))

    assert len(rows) == 2


def test_partial_consumer_context_drains_and_fails_closed_on_digest_drift(tmp_path) -> None:
    cas, partition = _sealed_partition(tmp_path)
    partition["content_digest"] = "f" * 64
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)

    with pytest.raises(SourceSnapshotDrift, match="digest differs"):
        with reader.iter_rows("kline_daily_raw", partition["partition_key"]) as rows:
            assert next(rows)["trade_date"] == "2026-07-30"


def test_partial_consumer_explicit_verify_completes_verification(tmp_path) -> None:
    cas, partition = _sealed_partition(tmp_path)
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)
    rows = reader.iter_rows("kline_daily_raw", partition["partition_key"])

    assert next(rows)["trade_date"] == "2026-07-30"
    assert rows.verified is False
    rows.verify()

    assert rows.verified is True


def test_partial_consumer_close_aborts_without_claiming_verification(tmp_path) -> None:
    cas, partition = _sealed_partition(tmp_path)
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)
    rows = reader.iter_rows("kline_daily_raw", partition["partition_key"])

    assert next(rows)["trade_date"] == "2026-07-30"
    rows.close()

    assert rows.verified is False
    assert rows.closed is True
    with pytest.raises(SourceSnapshotDrift, match="did not complete"):
        rows.verify()


def test_consumer_exception_aborts_stream_without_masking_or_verifying(tmp_path) -> None:
    cas, partition = _sealed_partition(tmp_path)
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)
    rows = reader.iter_rows("kline_daily_raw", partition["partition_key"])

    with pytest.raises(RuntimeError, match="consumer failed"):
        with rows:
            assert next(rows)["trade_date"] == "2026-07-30"
            raise RuntimeError("consumer failed")

    assert rows.closed is True
    assert rows.verified is False


def test_reader_fails_before_materializing_partition_above_bound(tmp_path) -> None:
    cas, partition = _sealed_partition(tmp_path)
    partition["row_count"] = 11
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)

    with pytest.raises(Exception, match="exceeds row bound"):
        reader.read_frame("kline_daily_raw", partition["partition_key"])


def test_reader_rejects_compressed_cas_tamper(tmp_path) -> None:
    cas, partition = _sealed_partition(tmp_path)
    path = cas.root / partition["rows_ref"]["relative_path"]
    with path.open("r+b") as handle:
        handle.seek(max(10, path.stat().st_size // 2))
        original = handle.read(1)
        handle.seek(-1, 1)
        handle.write(bytes([original[0] ^ 0x01]))

    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)
    with pytest.raises(CASCorruptionError):
        list(reader.iter_rows("kline_daily_raw", partition["partition_key"]))


def test_reader_recomputes_semantic_digest_after_streaming_decompression(tmp_path) -> None:
    cas, partition = _sealed_partition(tmp_path)
    partition["content_digest"] = "f" * 64
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)

    with pytest.raises(SourceSnapshotDrift, match="digest differs"):
        list(reader.iter_rows("kline_daily_raw", partition["partition_key"]))


def test_high_repetition_partition_compresses_and_reader_peak_is_bounded(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "large-control")
    cas = CASStore(store.root)
    row_count = 40_000
    spec = PartitionSpec(
        dataset="high_repetition",
        partition_key="2026-07",
        query_version="fixture-query-v1",
        columns=(
            ColumnSpec("row_key", ColumnKind.STRING, True),
            ColumnSpec("row_payload", ColumnKind.STRING, True),
        ),
        primary_keys=("row_key",),
    )
    ingestion = "fixture-ingestion-v1"
    hasher = CanonicalPartitionHasher(spec, ingestion_audit_identity=ingestion, snapshot_tokens=())
    header = {
        "schema_version": SOURCE_PARTITION_ROWS_SCHEMA,
        "partition_identity": spec.identity,
        "query_version": spec.query_version,
        "schema_digest": spec.schema_digest,
        "columns": [item.as_dict() for item in spec.columns],
        "primary_keys": list(spec.primary_keys),
        "source_table_identity": "fixture.high_repetition",
        "source_table_schema_digest": "a" * 64,
    }

    def canonical_rows():
        yield canonical_json_bytes(header) + b"\n"
        for ordinal in range(row_count):
            normalized = hasher.update(
                {
                    "row_key": f"{ordinal:08d}",
                    "row_payload": json.dumps(
                        {"trade_date": "2026-07-31", "value": "x" * 256},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                }
            )
            yield canonical_json_bytes(normalized) + b"\n"

    compression = StreamingCompressionStats()
    reference = cas.put_stream(iter_gzip_level1(canonical_rows(), compression))
    summary = hasher.finish()
    partition = {
        "dataset": spec.dataset,
        "partition_key": spec.partition_key,
        "query_version": spec.query_version,
        "schema_digest": spec.schema_digest,
        "columns": [item.as_dict() for item in spec.columns],
        "primary_keys": list(spec.primary_keys),
        "timezone_name": spec.timezone_name,
        "null_marker": spec.null_marker,
        "float_non_finite_policy": spec.float_non_finite_policy,
        "source_table_identity": header["source_table_identity"],
        "source_table_schema_digest": header["source_table_schema_digest"],
        "ingestion_audit_identity": ingestion,
        "row_count": summary.row_count,
        "required_null_count": summary.required_null_count,
        "duplicate_count": summary.duplicate_count,
        "content_digest": summary.content_digest,
        "merkle_root": summary.merkle_root,
        "rows_ref": reference.as_dict(),
        **compression.as_descriptor_fields(),
    }
    assert reference.size < compression.uncompressed_bytes // 10

    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=row_count)
    tracemalloc.start()
    observed = sum(
        1
        for _ in reader.iter_rows(
            spec.dataset,
            spec.partition_key,
            decode_row_payload=False,
        )
    )
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert observed == row_count
    assert peak < 16 * 1024 * 1024
