from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.artifact_ready_build_source import (
    ArtifactReadyBuildSource,
    ArtifactReadyBuildSourceError,
    _minute_bucket,
)
from backend.services.dataset_release.a_share_limit_rule import PRICE_LIMIT_RULE_VERSION
from backend.services.dataset_release.artifact_ready_source import (
    ARTIFACT_READY_DAILY_COVERAGE_SCHEMA,
    ARTIFACT_READY_LIMIT_COVERAGE_SCHEMA,
)
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.contracts import Component
from backend.services.dataset_release.pit import freeze_pit_snapshot


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


def test_daily_and_minute_share_one_verified_effective_limit_overlay(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control-limit")
    cas = CASStore(store.root)
    day = date(2024, 7, 23)
    pit = freeze_pit_snapshot(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": day,
                "eligible_end": day,
                "entry_reason": None,
                "exit_reason": None,
            },
            {
                "ts_code": "600001.SH",
                "eligible_start": day,
                "eligible_end": day,
                "entry_reason": None,
                "exit_reason": None,
            },
        ],
        universe_key="shsz_st_pit_active_v1",
        rule_version="st_pub_next_trade_restore_active_l_v1",
        scope_start=day,
        cutoff=day,
        state_identity="pit-fixture",
        source_fingerprint_sha256="a" * 64,
        parameter_hash="b" * 64,
    )
    overlay = {
        "ts_code": "600001.SH",
        "trade_date": day.isoformat(),
        "pre_close": "10.00",
        "up_limit": "11.00",
        "down_limit": "9.00",
    }
    effective_root = "d" * 64
    partition_key = f"{day.isoformat()}_{day.isoformat()}"
    zero_safety = {
        "database_writes": 0,
        "provider_database_writes": 0,
        "candidate_writes": 0,
        "production_writes": 0,
        "production_deletes": 0,
        "production_pointer_changes": 0,
        "service_process_controls": 0,
    }
    reference = cas.put_json(
        {
            "schema_version": ARTIFACT_READY_LIMIT_COVERAGE_SCHEMA,
            "raw_partition_identity": f"stk_limit:{partition_key}",
            "partition_key": partition_key,
            "rule_version": PRICE_LIMIT_RULE_VERSION,
            "pit_snapshot_digest": pit.spans_sha256,
            "database_override_rows": 0,
            "unresolved_keys": 0,
            "effective_content_root": effective_root,
            "overlay_rows": [overlay],
            "safety": zero_safety,
        }
    )
    entry = {
        "dataset": "stk_limit_rule_coverage",
        "partition_key": partition_key,
        "content_digest": effective_root,
        "rows_ref": reference.as_dict(),
    }
    source = ArtifactReadyBuildSource.__new__(ArtifactReadyBuildSource)
    source.cas = cas
    source.pit_snapshot = pit
    source.profile = SimpleNamespace(pit_authority_status="ACTIVE_CANONICAL")
    source.component_manifests = {
        Component.DAILY_BIN: {"partitions": [entry]},
        Component.MINUTE_BIN: {"partitions": [entry]},
    }
    database = [
        {
            "ts_code": "000001.SZ",
            "trade_date": day.isoformat(),
            "pre_close": 10.0,
            "up_limit": 11.0,
            "down_limit": 9.0,
        }
    ]
    descriptor = {"partition_key": partition_key}

    daily = list(source._effective_limit_rows(Component.DAILY_BIN, descriptor, database))
    minute = list(source._effective_limit_rows(Component.MINUTE_BIN, descriptor, database))

    assert daily == minute
    assert [row["ts_code"] for row in daily] == ["000001.SZ", "600001.SH"]

    receipt = cas.get_json(reference)
    receipt["pit_snapshot_digest"] = "e" * 64
    tampered = cas.put_json(receipt)
    source.component_manifests[Component.DAILY_BIN]["partitions"][0] = {
        **entry,
        "rows_ref": tampered.as_dict(),
    }
    with pytest.raises(ArtifactReadyBuildSourceError, match="receipt is invalid"):
        list(source._effective_limit_rows(Component.DAILY_BIN, descriptor, database))
