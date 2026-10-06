from datetime import date
from types import SimpleNamespace
from contextlib import contextmanager
import json

import pytest

from backend.services.dataset_release.monthly_component_preparation import ComponentPreparationError
from backend.services.dataset_release.monthly_preparation_source import freeze_preparation_source
from backend.services.dataset_release.source_authority import MONTHLY_SECTOR_SOURCE_POLICY


@pytest.mark.parametrize("failure", [None, "schema", "writer", "control", "null_value", "deferred_tail", "cancel"])
def test_private_freeze_uses_production_row_sealer_and_bracket(monkeypatch, tmp_path, failure):
    from backend.services.dataset_release import monthly_preparation_source as preparation
    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.source_authority import (
        MonthlySourceAuthority,
        PRODUCTION_QUERY_SPECS,
        SourceTableSchema,
        SourceSnapshotDriftBlocked,
    )
    from backend.services.dataset_release.errors import SourceManifestError
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseCancelled

    ControlStore.initialize(tmp_path)
    cas = CASStore(tmp_path)
    cutoff = date(2026, 9, 30)
    queries = {
        key: PRODUCTION_QUERY_SPECS[key]
        for key in (
            "sw_index_classify",
            "sw_index_member",
            "kline_daily_raw",
            "margin_detail",
        )
    }
    monkeypatch.setattr(preparation, "PRODUCTION_QUERY_SPECS", queries)
    schemas = {key: SourceTableSchema(query.table_identity, query.required_columns) for key, query in queries.items()}
    streamed = []
    payloads = {
        "sw_index_classify": {"index_code": "801011.SI", "level": "L2", "src": "SW2021", "is_pub": "0"},
        "sw_index_member": {"ts_code": "000001.SZ", "in_date": "2026-09-01", "l2_code": "801011.SI", "out_date": None},
        "kline_daily_raw": {
            "ts_code": "000001.SZ",
            "trade_date": "2026-09-30",
            **{key: 1000 for key in queries["kline_daily_raw"].value_columns},
        },
        "margin_detail": {
            "ts_code": "000001.SZ",
            "trade_date": "2026-09-29",
            **{key: 1000 for key in queries["margin_detail"].value_columns},
        },
    }
    if failure == "null_value":
        payloads["kline_daily_raw"]["close_li"] = None

    class Session:
        snapshot_tokens = ("postgres:1-ABC-1",)

        def describe(self, key):
            if failure == "schema" and key == "kline_daily_raw":
                return SourceTableSchema("wrong.table", ("wrong",))
            return schemas[key]

        def stream(self, key, params, *, fetch_rows):
            streamed.append(key)
            assert fetch_rows == 1000
            if key == "margin_detail":
                assert failure == "deferred_tail"
                assert params["end"] == date(2026, 9, 29)
            query = queries[key]
            payload = payloads[key]
            for index in range(2001 if failure == "cancel" and key == "kline_daily_raw" else 1):
                current = {**payload, "ts_code": f"{index + 1:06d}.SZ"} if failure == "cancel" and key == "kline_daily_raw" else payload
                yield {
                    "row_key": json.dumps([current[name] for name in query.key_columns]),
                    "row_payload": json.dumps(current),
                }

    @contextmanager
    def sessions(_policy):
        yield Session()

    profile = SimpleNamespace(
        profile="qe_hmm_full_v2",
        start_date=date(2026, 9, 1),
        resource_policy=SimpleNamespace(validation_read_chunk_rows=1000),
        pressure_ladder={"date_chunk_months": (1,), "minute_batch": (100,)},
    )
    authority = MonthlySourceAuthority(
        profile, cas, session_factory=sessions, sector_source_policy=MONTHLY_SECTOR_SOURCE_POLICY
    )
    audit = SimpleNamespace(
        partition_digest=lambda *args: "b" * 64, as_receipt=lambda **kwargs: {"schema_version": "fixture_audit"}
    )
    pit = SimpleNamespace(spans_sha256="c" * 64, canonical_bytes=lambda: b'{"fixture_pit":true}')
    control = SimpleNamespace(
        schemas=schemas,
        audit=audit,
        pit_snapshot=pit,
        pit_partitions=(),
        snapshot_tokens=("postgres:1-ABC-1",),
        consistency_digest="a" * 64,
        writer_ledger_digest="d" * 64,
    )
    brackets = []

    def capture(**kwargs):
        brackets.append(kwargs)
        return (
            SimpleNamespace(**{**control.__dict__, "consistency_digest": "e" * 64})
            if failure == "control" and len(brackets) == 2
            else control
        )

    monkeypatch.setattr(authority, "_capture_control_snapshot", capture)
    monkeypatch.setattr(
        authority,
        "_partition_requests",
        lambda query, *_args, **_kwargs: [("2026-09-01_2026-09-30", {"start": profile.start_date, "end": cutoff})],
    )
    monkeypatch.setattr(
        authority, "_freeze_writer_ledger", lambda *_args, **_kwargs: ("wrong" if failure == "writer" else "d" * 64, {})
    )
    observations = []

    def checkpoint():
        if failure == "cancel" and observations and observations[-1]["rows_validated"] >= 1000:
            raise MonthlyReleaseCancelled("cancel raw chunk")

    expected_error = (MonthlyReleaseCancelled if failure == "cancel" else
                      SourceManifestError if failure == "null_value" else SourceSnapshotDriftBlocked)
    if failure and failure != "deferred_tail":
        with pytest.raises(expected_error):
            preparation.freeze_preparation_source(
                authority, operation_id=f"dmr_{'1' * 32}", cutoff=cutoff, blocking_datasets=("margin_detail",),
                checkpoint=checkpoint, progress=lambda value: observations.append(dict(value)),
            )
        if failure == "cancel":
            assert observations[-1]["rows_validated"] == 1002  # Two classification rows plus one raw chunk.
            assert observations[-1]["partitions_sealed"] == 2  # Daily partition never sealed.
        return
    result = preparation.freeze_preparation_source(
        authority,
        operation_id=f"dmr_{'1' * 32}",
        cutoff=cutoff,
        blocking_datasets=("margin_detail",),
        deferred_cutoff_datasets=("margin_detail",) if failure == "deferred_tail" else (),
        progress=lambda value: observations.append(dict(value)),
    )
    manifest = cas.get_json(result.source_manifest_ref)
    assert manifest["schema_version"] == preparation.PREPARATION_SOURCE_SCHEMA
    assert manifest["publication_allowed"] is False
    assert manifest["consistent_input_set_complete"] is False
    assert manifest["omitted_datasets"] == ["margin_detail"]
    assert streamed == ["sw_index_classify", "sw_index_member", "kline_daily_raw"] + (
        ["margin_detail"] if failure == "deferred_tail" else []
    )
    assert len(brackets) == 2
    assert len(result.partitions) == (4 if failure == "deferred_tail" else 3)
    assert observations[-1]["rows_validated"] == len(result.partitions)
    assert observations[-1]["rows_sealed"] == len(result.partitions)
    assert observations[-1]["partitions_sealed"] == len(result.partitions)
    assert observations[-1]["phase"] == "PRIVATE_SOURCE_BRACKET"
    daily_partition = next(item for item in result.partitions if item.spec.dataset == "kline_daily_raw")
    assert [item["month"] for item in daily_partition.monthly_content_leaves] == ["2026-09"]
    assert (
        sum(item["row_count"] for item in daily_partition.monthly_content_leaves) == daily_partition.summary.row_count
    )
    if failure == "deferred_tail":
        assert manifest["deferred_cutoff_datasets"] == ["margin_detail"]
        assert result.partitions[-1].spec.partition_key == "2026-09-01_2026-09-29"


@pytest.mark.parametrize("blockers", [("unknown",), ("stock_universe_pit",), ("trading_calendar",), ()])
def test_private_freeze_rejects_unavailable_control_before_any_payload(blockers):
    authority = SimpleNamespace(
        uses_p3a_sector_source=False,
        _sector_source_policy=MONTHLY_SECTOR_SOURCE_POLICY,
        _mvcc_reuse_capability=False,
    )
    with pytest.raises(ComponentPreparationError):
        freeze_preparation_source(
            authority,
            operation_id=f"dmr_{'1' * 32}",
            cutoff=date(2026, 9, 30),
            blocking_datasets=blockers,
        )
