from __future__ import annotations

from types import SimpleNamespace

from backend.services.dataset_release.artifact_ready_build_source import (
    ArtifactReadyBuildSource,
    _minute_bucket,
)
from backend.services.dataset_release.artifact_ready_source import ARTIFACT_READY_DAILY_COVERAGE_SCHEMA
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.contracts import Component


class _Reader:
    def __init__(self) -> None:
        self.partition_keys: list[str] = []

    def iter_rows(self, dataset, partition_key, *, decode_row_payload):
        assert dataset == "kline_minute_raw"
        assert decode_row_payload is True
        self.partition_keys.append(partition_key)
        return iter(
            (
                {
                    "ts_code": "000001.SZ",
                    "trade_time": "2026-07-31 09:31:00",
                    "freq": "1m",
                },
            )
        )


def test_single_code_minute_selection_opens_only_its_stable_hash_bucket() -> None:
    code = "000001.SZ"
    bucket_count = 1024
    selected = _minute_bucket(code, bucket_count)
    source = ArtifactReadyBuildSource.__new__(ArtifactReadyBuildSource)
    source.profile = SimpleNamespace(minute_code_bucket_count=bucket_count)
    source.component_manifests = {
        Component.MINUTE_BIN: {
            "partitions": [
                {
                    "identity": f"kline_minute_raw:2026-07-01_2026-07-31_bucket-{bucket:04d}",
                    "dataset": "kline_minute_raw",
                    "partition_key": f"2026-07-01_2026-07-31_bucket-{bucket:04d}",
                    "role": "sealed_database_source",
                }
                for bucket in range(bucket_count)
            ]
        }
    }
    source._reader = _Reader()
    source._raw_descriptor = lambda entry: entry

    partitions = source.ordered_partitions(
        Component.MINUTE_BIN,
        "kline_minute_raw",
        effective=False,
        instruments=(code,),
    )
    rows = [row for partition in partitions for row in partition.rows]

    assert len(partitions) == 1
    assert partitions[0].identity.endswith(f"bucket-{selected:04d}")
    assert source._reader.partition_keys == [f"2026-07-01_2026-07-31_bucket-{selected:04d}"]
    assert [row["ts_code"] for row in rows] == [code]


def test_effective_daily_rows_stream_database_then_missing_only_overlay(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    overlay = {
        "ts_code": "600001.SH",
        "trade_date": "2026-07-31",
        "open_li": 10000,
        "high_li": 11000,
        "low_li": 9000,
        "close_li": 10500,
        "volume_hand": 100,
        "amount_li": 100000,
    }
    reference = cas.put_json(
        {
            "schema_version": ARTIFACT_READY_DAILY_COVERAGE_SCHEMA,
            "overlay_rows": [overlay],
        }
    )
    source = ArtifactReadyBuildSource.__new__(ArtifactReadyBuildSource)
    source.cas = cas
    source.component_manifests = {
        Component.DAILY_BIN: {
            "partitions": [
                {
                    "dataset": "daily_coverage",
                    "partition_key": "2026-07-01_2026-07-31",
                    "rows_ref": reference.as_dict(),
                }
            ]
        }
    }
    database = [{**overlay, "ts_code": "000001.SZ"}]

    rows = list(
        source._effective_daily_rows(
            Component.DAILY_BIN,
            {"partition_key": "2026-07-01_2026-07-31"},
            database,
        )
    )
    assert [row["ts_code"] for row in rows] == ["000001.SZ", "600001.SH"]
