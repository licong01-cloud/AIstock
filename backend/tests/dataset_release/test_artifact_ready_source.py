from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

import pandas as pd
import pytest

from backend.services.dataset_release import artifact_ready_source as artifact_ready_module
from backend.services.dataset_release.artifact_ready_source import (
    ARTIFACT_READY_CONTRACT_SCHEMA,
    ARTIFACT_READY_RECHECK_SCHEMA,
    ArtifactReadyCoverageIncomplete,
    ArtifactReadySourceBuilder,
    ArtifactReadySourceError,
    load_artifact_ready_contract,
    load_artifact_ready_recheck_expectations,
)
from backend.services.dataset_release.artifact_ready_build_source import (
    ArtifactReadyBuildSource,
)
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.build_processor import (
    BuildSourceRevised,
    _validate_fresh_probe,
)
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.component_artifact_manifest import (
    load_component_artifact_manifest,
    normalize_current_source_partition,
    seal_component_artifact_manifest,
)
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.contracts import Component
from backend.services.dataset_release.minute_overlay import canonical_session_times
from backend.services.dataset_release.mixed_planner import (
    MixedPlannerContext,
    build_mixed_action_plan,
    load_artifact_ready_planning_authority,
    pit_span_digest_by_code,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.source_rows_codec import (
    SOURCE_ROWS_CODEC,
    SOURCE_ROWS_CODEC_IDENTITY,
    SOURCE_ROWS_CODEC_LEVEL,
    SOURCE_ROWS_CODEC_VERSION,
    SOURCE_ROWS_FORMAT,
    compression_ratio_text,
)


DAY = date(2026, 7, 31)
CODE = "000001.SZ"


def test_recheck_expectation_loader_uses_metadata_only_and_binds_codec(
    tmp_path,
    monkeypatch,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)

    def descriptor(dataset: str, scope: str, payload: bytes):
        reference = cas.put_bytes(payload)
        return {
            "recheck_partition_scope": scope,
            "dataset": dataset,
            "partition_key": "fixture",
            "query_version": "fixture-v1",
            "schema_digest": _digest(f"schema:{dataset}"),
            "content_digest": _digest(f"content:{dataset}"),
            "merkle_root": _digest(f"merkle:{dataset}"),
            "ingestion_audit_identity": _digest(f"audit:{dataset}"),
            "row_count": 1,
            "rows_ref": reference.as_dict(),
            "rows_format": SOURCE_ROWS_FORMAT,
            "rows_codec": SOURCE_ROWS_CODEC,
            "rows_codec_version": SOURCE_ROWS_CODEC_VERSION,
            "rows_codec_level": SOURCE_ROWS_CODEC_LEVEL,
            "rows_codec_identity": SOURCE_ROWS_CODEC_IDENTITY,
            "rows_uncompressed_bytes": len(payload),
            "rows_compressed_bytes": len(payload),
            "rows_compression_ratio": compression_ratio_text(len(payload), len(payload)),
            "source_table_schema_digest": _digest(f"table:{dataset}"),
            "columns": [{"name": "value", "kind": "integer", "required": True}],
            "primary_keys": ["value"],
            "source_table_columns": ["value"],
            "source_table_types": ["integer"],
        }

    source = descriptor("source_fixture", "source", b"source")
    pit = descriptor("pit_fixture", "pit", b"pit")
    source_root = artifact_ready_module.merkle_root_from_named_digests(
        "dataset_release_source_content_root_v1",
        (("source_fixture:fixture", source["content_digest"]),),
    )
    loaded = SimpleNamespace(
        payload={
            "source_content_root": source_root,
            "source_recheck_partition_expectations": [source, pit],
        }
    )
    sealed_paths = {cas.root / item["rows_ref"]["relative_path"] for item in (source, pit)}
    original_open = Path.open

    def guarded_open(path, *args, **kwargs):
        if Path(path) in sealed_paths:
            raise AssertionError("expectation loading must not read row payloads")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    expectations = load_artifact_ready_recheck_expectations(cas, loaded)
    assert len(expectations) == 2

    loaded.payload["source_recheck_partition_expectations"][0]["rows_codec_identity"] = "drifted-codec"
    with pytest.raises(
        artifact_ready_module.ArtifactReadySourceRevised,
        match="incomplete",
    ):
        load_artifact_ready_recheck_expectations(cas, loaded)


class _TrackingFrame:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.to_dict_called = False

    @property
    def columns(self):
        return self.frame.columns

    def __len__(self) -> int:
        return len(self.frame)

    def memory_usage(self, *, index: bool, deep: bool):
        return self.frame.memory_usage(index=index, deep=deep)

    def __getitem__(self, key: str):
        return self.frame[key]

    def to_dict(self, *, orient: str):
        self.to_dict_called = True
        return self.frame.to_dict(orient=orient)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _minute_rows(*, count: int = 240, open_li: int = 10_000):
    return [
        {
            "ts_code": CODE,
            "trade_time": value.isoformat(sep=" "),
            "freq": "1m",
            "open_li": open_li,
            "high_li": 11_000,
            "low_li": 9_000,
            "close_li": 10_500,
            "volume_hand": 100,
            "amount_li": 100_000,
        }
        for value in canonical_session_times(DAY)[:count]
    ]


def _tdx_rows(*, count: int = 240):
    return [
        {
            "Code": CODE[:6],
            "TradeTime": value.isoformat(sep=" "),
            "Open": 10_000,
            "High": 11_000,
            "Low": 9_000,
            "Close": 10_500,
            "Volume": 100,
            "Amount": 100_000,
        }
        for value in canonical_session_times(DAY)[:count]
    ]


def _tushare_rows(*, count: int = 240):
    return [
        {
            "ts_code": CODE,
            "trade_time": value.isoformat(sep=" "),
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
            "vol": 10_000,
            "amount": 100,
        }
        for value in canonical_session_times(DAY)[:count]
    ]


def _tushare_rows_with_auction():
    return [
        {
            "ts_code": CODE,
            "trade_time": f"{DAY.isoformat()} 09:30:00",
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
            "vol": 10_000,
            "amount": 100,
        },
        *_tushare_rows(),
    ]


def _index_row(code: str, *, close: float = 10.0, trade_date: date = DAY) -> dict[str, Any]:
    return {
        "ts_code": code,
        "trade_date": trade_date.isoformat(),
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "pre_close": close - 0.5,
        "pct_chg": 5.0,
        "vol": 123.0,
        "amount": 456.0,
    }


@dataclass
class _View:
    values: dict[str, list[Mapping[str, Any]]]
    rows: dict[str, list[Mapping[str, Any]]]

    def descriptors(self, dataset: str) -> Sequence[Mapping[str, Any]]:
        return tuple(self.values.get(dataset, ()))

    def iter_partition_rows(self, descriptor: Mapping[str, Any]):
        return iter(self.rows[f"{descriptor['dataset']}:{descriptor['partition_key']}"])


def _fixture(
    dataset_profile,
    tmp_path,
    *,
    minute_rows: Sequence[Mapping[str, Any]],
    suspended: bool = False,
    missing_index_code: str | None = None,
):
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    dummy_ref = cas.put_json({"fixture": "sealed-rows"})
    datasets = {
        "trading_calendar",
        "kline_daily_raw",
        "adj_factor",
        "stk_limit",
        "suspend_d",
        "kline_minute_raw",
        "daily_basic",
        "moneyflow_ts",
        "bak_basic",
        "cyq_perf",
        "sector_data",
        "margin_detail",
        "stock_basic",
        "sw_index_classify",
        "sw_index_member",
        "index_daily",
    }
    minute_bucket = (
        int(hashlib.sha256(CODE.encode("utf-8")).hexdigest()[:16], 16) % dataset_profile.minute_code_bucket_count
    )
    partition_keys = {
        "kline_daily_raw": f"{DAY.isoformat()}_{DAY.isoformat()}",
        "kline_minute_raw": (f"{DAY.isoformat()}_{DAY.isoformat()}_bucket-{minute_bucket:04d}"),
        "index_daily": f"{DAY.isoformat()}_{DAY.isoformat()}",
        "adj_factor": f"{DAY.isoformat()}_{DAY.isoformat()}",
    }
    values: dict[str, list[Mapping[str, Any]]] = {}
    rows: dict[str, list[Mapping[str, Any]]] = {}
    for dataset in datasets:
        partition_key = partition_keys.get(dataset, "fixture")
        identity = f"{dataset}:{partition_key}"
        if dataset == "trading_calendar":
            source_rows = [{"cal_date": DAY.isoformat(), "is_trading": True}]
        elif dataset == "kline_minute_raw":
            source_rows = [dict(item) for item in minute_rows]
        elif dataset == "kline_daily_raw":
            source_rows = [{"ts_code": CODE, "trade_date": DAY.isoformat()}]
        elif dataset == "suspend_d" and suspended:
            source_rows = [
                {
                    "ts_code": CODE,
                    "trade_date": DAY.isoformat(),
                    "suspend_type": "S",
                    "suspend_timing": None,
                }
            ]
        elif dataset == "index_daily":
            source_rows = [
                _index_row(item.daily_code)
                for item in DOMESTIC_INDEX_DEFINITIONS
                if item.daily_code != missing_index_code
            ]
        elif dataset == "adj_factor":
            source_rows = [
                {
                    "ts_code": CODE,
                    "trade_date": DAY.isoformat(),
                    "adj_factor": 1.25,
                }
            ]
        else:
            source_rows = []
        descriptor = {
            "dataset": dataset,
            "partition_key": partition_key,
            "row_count": len(source_rows),
            "content_digest": _digest(f"content:{identity}"),
            "schema_digest": _digest(f"schema:{identity}"),
            "rows_ref": dummy_ref.as_dict(),
        }
        values[dataset] = [descriptor]
        rows[identity] = source_rows
    pit = freeze_pit_snapshot(
        [
            {
                "ts_code": CODE,
                "eligible_start": DAY,
                "eligible_end": DAY,
                "entry_reason": "fixture",
                "exit_reason": None,
            }
        ],
        universe_key=dataset_profile.universe_key,
        rule_version=dataset_profile.universe_rule_version,
        scope_start=DAY,
        cutoff=DAY,
        state_identity=_digest("state"),
        source_fingerprint_sha256=_digest("pit-source"),
        parameter_hash=_digest("pit-params"),
        state_start=DAY,
        state_end=DAY,
    )
    snapshot = SimpleNamespace(
        official_cutoff=DAY,
        pit_snapshot=pit,
        source_content_root=_digest("raw-source"),
        pit_snapshot_digest=pit.spans_sha256,
        partitions=(),
    )
    return cas, _View(values, rows), snapshot


def _minute_coverage(cas: CASStore, bundle) -> Mapping[str, Any]:
    contract = cas.get_json(bundle.artifact_ready_contract_ref)
    minute_manifest = cas.get_json(contract["component_manifests"]["minute_bin"])
    entry = next(item for item in minute_manifest["partitions"] if item["dataset"] == "minute_coverage")
    return cas.get_json(entry["rows_ref"])


def _component_baseline_from_artifact_ready(
    cas: CASStore,
    planning,
    snapshot,
):
    pit_digests = pit_span_digest_by_code(snapshot.pit_snapshot)
    components: dict[str, Mapping[str, Any]] = {}
    for component in Component:
        authority = planning.components[component]
        source = [dict(item) for item in authority.partitions]
        source_ids = [str(item["identity"]) for item in source]
        output = "artifact.h5"
        non_index = component is not Component.DOMESTIC_INDEX_CONTEXT
        adj = authority.adj_series
        components[component.value] = {
            "status": "COMPLETE",
            "component": component.value,
            "component_root_relative_path": component.value,
            "source_partitions": source,
            "artifact_partitions": [
                {
                    "partition_key": "all",
                    "source_partition_identities": source_ids,
                    "dependency_edges": [f"artifact_ready->{component.value}"],
                    "instruments": ([CODE] if non_index else ["000985.CSI"]),
                    "start": DAY.isoformat(),
                    "end": DAY.isoformat(),
                    "files": [
                        {
                            "relative_path": output,
                            "size_bytes": 16,
                            "sha256": _digest(f"output:{component.value}"),
                            "instrument": None,
                        }
                    ],
                }
            ],
            "append_rules": [
                {
                    "rule_id": "effective-tail",
                    "datasets": sorted({str(item["dataset"]) for item in source}),
                    "replace_existing_targets": [output],
                    "create_new_targets": [],
                    "create_target_templates": [],
                    "writer_targets_by_instrument": {},
                    "writer_target_policy": "explicit_by_instrument_v1",
                    "dependency_edges": [f"effective_tail->{component.value}"],
                }
            ],
            "pit_mutation_rule": {
                "rule_id": "pit-change",
                "datasets": ["stock_universe_pit_spans"],
                "replace_existing_targets": [],
                "create_new_targets": [],
                "create_target_templates": ["new/{instrument}.bin"],
                "writer_targets_by_instrument": {CODE: [output]},
                "writer_target_policy": "explicit_by_instrument_v1",
                "dependency_edges": [f"pit_span->{component.value}"],
            },
            "pit_instruments": [CODE],
            "pit_span_digest_by_code": pit_digests,
            "adj_series": (
                {
                    "complete": True,
                    "qfq_denominator_by_code": dict(adj.qfq_denominator_by_code),
                    "ordered_adj_digest_by_code": dict(adj.ordered_adj_digest_by_code),
                    "adj_row_count_by_code": dict(adj.adj_row_count_by_code),
                    "monthly_ordered_adj_by_code": {},
                    "writer_targets_by_code": {},
                    "shared_writer_targets": [output],
                    "writer_target_policy": "artifact_file_instrument_index_v1",
                }
                if adj is not None
                else None
            ),
        }
    reference = seal_component_artifact_manifest(
        cas,
        {
            "profile": "qe_hmm_full_v1",
            "scope": "full",
            "cutoff": DAY.isoformat(),
            "candidate_identity": _digest("candidate"),
            "artifact_root": _digest("artifact"),
            "semantic_profile_digest": _digest("semantic"),
            "producer_fingerprint": _digest("producer"),
            "artifact_fingerprint": _digest("artifact-contract"),
            "validation_fingerprint": _digest("validation"),
            "source_content_root": snapshot.source_content_root,
            "artifact_ready_content_root": planning.effective_content_root,
            "pit_snapshot_digest": snapshot.pit_snapshot_digest,
            "components": components,
        },
    )
    return load_component_artifact_manifest(cas, reference)


def test_tushare_minute_row_window_bound_precedes_record_materialization(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "minute-provider-control")
    builder = ArtifactReadySourceBuilder(dataset_profile, CASStore(store.root))
    frame = _TrackingFrame(
        pd.DataFrame(
            {
                "ts_code": [CODE] * 242,
                "trade_time": [f"{DAY.isoformat()} 09:31:00"] * 242,
                "open": [10.0] * 242,
                "close": [10.0] * 242,
                "high": [10.0] * 242,
                "low": [10.0] * 242,
                "vol": [100.0] * 242,
                "amount": [100.0] * 242,
            }
        )
    )
    builder._tushare._provider = SimpleNamespace(stk_mins=lambda **_kwargs: frame)

    with pytest.raises(artifact_ready_module.MinuteProviderTerminal):
        builder._fetch_tushare_minute_rows(CODE, DAY)

    assert frame.to_dict_called is False


def test_tushare_index_column_bound_precedes_record_materialization(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "index-provider-control")
    builder = ArtifactReadySourceBuilder(dataset_profile, CASStore(store.root))
    row = _index_row(DOMESTIC_INDEX_DEFINITIONS[0].daily_code)
    row["unexpected"] = "field"
    frame = _TrackingFrame(pd.DataFrame([row]))
    builder._tushare._provider = SimpleNamespace(index_daily=lambda **_kwargs: frame)

    with pytest.raises(artifact_ready_module.IndexProviderUnavailable):
        builder._fetch_tushare_index_rows(
            DOMESTIC_INDEX_DEFINITIONS[0],
            DAY,
            DAY,
        )

    assert frame.to_dict_called is False


def test_tushare_adj_factor_row_bound_precedes_record_materialization(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "adj-provider-control")
    builder = ArtifactReadySourceBuilder(dataset_profile, CASStore(store.root))
    frame = _TrackingFrame(
        pd.DataFrame(
            {
                "ts_code": [CODE] * 20_001,
                "trade_date": [DAY.isoformat()] * 20_001,
                "adj_factor": [1.0] * 20_001,
            }
        )
    )
    builder._tushare._provider = SimpleNamespace(adj_factor=lambda **_kwargs: frame)

    with pytest.raises(artifact_ready_module.ArtifactReadyProviderTerminal):
        builder._fetch_tushare_adj_factor_rows(DAY)

    assert frame.to_dict_called is False


def test_tushare_receipt_byte_bound_is_stream_counted_without_large_json_blob(
    monkeypatch,
) -> None:
    frame = _TrackingFrame(
        pd.DataFrame(
            [
                {
                    "ts_code": "X" * 256,
                    "trade_date": DAY.isoformat(),
                    "adj_factor": 1.0,
                }
            ]
        )
    )
    monkeypatch.setattr(
        artifact_ready_module,
        "MAX_TUSHARE_PROVIDER_RECEIPT_BYTES",
        128,
    )

    with pytest.raises(
        artifact_ready_module.ArtifactReadyProviderTerminal,
        match="receipt exceeds bound",
    ):
        artifact_ready_module._bounded_tushare_records(
            frame,
            dataset="adj_factor",
            expected_columns=("ts_code", "trade_date", "adj_factor"),
            date_column="trade_date",
            start=DAY,
            end=DAY,
            max_rows=1,
        )

    assert frame.to_dict_called is True


def test_complete_database_never_calls_provider_and_recheck_is_contract_only(
    dataset_profile,
    tmp_path,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(),
    )
    calls: list[str] = []
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: calls.append("tdx"),
        fetch_tushare_minute_rows=lambda *_args: calls.append("tushare-minute"),
        fetch_tushare_index_rows=lambda *_args: calls.append("tushare-index"),
    )

    bundle = builder.build(snapshot, source_view=view)
    assert calls == []
    assert builder.peak_provider_calls == 0
    assert bundle.provider_receipt_refs == ()
    coverage = _minute_coverage(cas, bundle)
    assert coverage["days"][0]["status"] == "DATABASE_COMPLETE"
    contract = cas.get_json(bundle.artifact_ready_contract_ref)
    assert contract["schema_version"] == ARTIFACT_READY_CONTRACT_SCHEMA
    assert set(contract["component_manifests"]) == {
        "daily_bin",
        "minute_bin",
        "factor_h5_static",
        "domestic_index_context",
    }
    loaded = load_artifact_ready_contract(
        cas,
        dataset_profile,
        bundle.artifact_ready_contract_ref,
        expected_source_content_root=snapshot.source_content_root,
        expected_pit_snapshot_digest=snapshot.pit_snapshot_digest,
    )
    assert loaded.artifact_ready_effective_content_root == bundle.artifact_ready_content_root
    exact = builder.verify_current_exact(
        loaded,
        fresh_snapshot=snapshot,
        observed_at=datetime.now(UTC),
        execution_id="execution-exact",
        run_id="run-exact",
        attempt_id="attempt-exact",
        attempt_fence=1,
    )
    exact_receipt = cas.get_json(exact.source_probe_ref)
    assert exact_receipt["raw_source_changed"] is False
    assert exact_receipt["source_recheck_scan_policy"] == "exact_partition_hash_only_v1"
    assert exact_receipt["fresh_artifact_ready_contract_ref"] == (bundle.artifact_ready_contract_ref.as_dict())
    with pytest.raises(artifact_ready_module.ArtifactReadySourceRevised) as revised:
        builder.verify_current_exact(
            loaded,
            fresh_snapshot=SimpleNamespace(
                **{
                    **vars(snapshot),
                    "source_content_root": _digest("revised-source"),
                }
            ),
            observed_at=datetime.now(UTC),
            execution_id="execution-exact",
            run_id="run-exact",
            attempt_id="attempt-revised",
            attempt_fence=2,
        )
    assert revised.value.code == "BLOCKED_SOURCE_REVISED"
    planning = load_artifact_ready_planning_authority(
        cas,
        dataset_profile,
        SimpleNamespace(
            **vars(snapshot),
            artifact_ready_contract_ref=bundle.artifact_ready_contract_ref,
            artifact_ready_content_root=bundle.artifact_ready_content_root,
            artifact_ready_provenance_root=loaded.artifact_ready_provenance_root,
        ),
    )
    assert planning.effective_content_root == bundle.artifact_ready_content_root
    assert set(planning.components) == set(Component)
    assert set(planning.components[Component.DAILY_BIN].adj_series.qfq_denominator_by_code) == {CODE}
    factor_manifest = cas.get_json(contract["component_manifests"]["factor_h5_static"])
    assert factor_manifest["details"]["overlay_summary"] == contract["factor_overlay_summary"]
    for raw_ref in contract["component_manifests"].values():
        manifest = cas.get_json(raw_ref)
        assert [normalize_current_source_partition(item) for item in manifest["effective_partitions"]]
    build_source = ArtifactReadyBuildSource(
        cas=cas,
        profile=dataset_profile,
        cutoff=DAY,
        pit_snapshot=snapshot.pit_snapshot,
        source_content_root=snapshot.source_content_root,
        source_partitions=tuple(descriptor for descriptors in view.values.values() for descriptor in descriptors),
        artifact_ready_contract_ref=bundle.artifact_ready_contract_ref,
    )
    minute_datasets = {item["dataset"] for item in build_source.source_partition_evidence(Component.MINUTE_BIN)}
    assert "minute_coverage" in minute_datasets
    assert "kline_minute_raw" not in minute_datasets
    index_datasets = {
        item["dataset"] for item in build_source.source_partition_evidence(Component.DOMESTIC_INDEX_CONTEXT)
    }
    assert index_datasets == {"trading_calendar", "index_daily_merged"}
    recheck = builder.verify_current(
        bundle,
        fresh_snapshot=SimpleNamespace(
            **{
                **vars(snapshot),
                "source_content_root": _digest("raw-source-changed-placement"),
            }
        ),
        observed_at=datetime.now(UTC),
        execution_id="execution-1",
        run_id="run-1",
        attempt_id="attempt-1",
        attempt_fence=1,
        source_view=view,
    )
    receipt = cas.get_json(recheck.source_probe_ref)
    assert receipt["schema_version"] == ARTIFACT_READY_RECHECK_SCHEMA
    assert receipt["raw_source_changed"] is True
    validated_probe = _validate_fresh_probe(
        cas,
        recheck.source_probe_ref,
        logical_request_key="logical-unused-for-artifact-recheck",
        source_content_root=bundle.artifact_ready_content_root,
        pit_snapshot_digest=snapshot.pit_snapshot_digest,
        run_id="run-1",
        attempt_id="attempt-1",
        attempt_fence=1,
        execution_id="execution-1",
        now=datetime.now(UTC),
    )
    assert validated_probe["source_probe_key"] == recheck.source_probe_key
    with pytest.raises(BuildSourceRevised, match="recheck differs"):
        _validate_fresh_probe(
            cas,
            recheck.source_probe_ref,
            logical_request_key="logical-unused-for-artifact-recheck",
            source_content_root=bundle.artifact_ready_content_root,
            pit_snapshot_digest=snapshot.pit_snapshot_digest,
            run_id="another-run",
            attempt_id="attempt-1",
            attempt_fence=1,
            execution_id="execution-1",
            now=datetime.now(UTC),
        )
    with pytest.raises(ArtifactReadySourceError, match="PIT identity differs"):
        builder.verify_current(
            bundle,
            fresh_snapshot=SimpleNamespace(**{**vars(snapshot), "pit_snapshot_digest": _digest("changed-pit")}),
            observed_at=datetime.now(UTC),
            execution_id="execution-1",
            run_id="run-1",
            attempt_id="attempt-2",
            attempt_fence=2,
            source_view=view,
        )


def test_real_artifact_ready_evidence_drives_reuse_and_pit_selective_whole_h5(
    dataset_profile,
    tmp_path,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(),
    )
    bundle = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: (),
        fetch_tushare_minute_rows=lambda *_args: (),
        fetch_tushare_index_rows=lambda *_args: (),
    ).build(snapshot, source_view=view)
    loaded = load_artifact_ready_contract(
        cas,
        dataset_profile,
        bundle.artifact_ready_contract_ref,
        expected_source_content_root=snapshot.source_content_root,
        expected_pit_snapshot_digest=snapshot.pit_snapshot_digest,
    )
    frozen = SimpleNamespace(
        **vars(snapshot),
        artifact_ready_contract_ref=bundle.artifact_ready_contract_ref,
        artifact_ready_content_root=bundle.artifact_ready_content_root,
        artifact_ready_provenance_root=loaded.artifact_ready_provenance_root,
    )
    current = load_artifact_ready_planning_authority(cas, dataset_profile, frozen)
    baseline = _component_baseline_from_artifact_ready(cas, current, snapshot)
    pit_by_code = pit_span_digest_by_code(snapshot.pit_snapshot)
    base_context = MixedPlannerContext(
        source_release_id="fixture-release",
        source_release_digest=_digest("release"),
        source_attestation_key=_digest("attestation"),
        dataset_start=DAY,
        cutoff=DAY,
        current_pit_snapshot_digest=snapshot.pit_snapshot_digest,
        current_pit_instruments=(CODE,),
        current_pit_span_digest_by_code=pit_by_code,
    )

    reuse = build_mixed_action_plan(
        baseline=baseline,
        current=current.components,
        context=base_context,
        compatible=True,
    )
    assert {item.action.value for item in reuse.actions} == {"REUSE"}

    revised_context = MixedPlannerContext(
        source_release_id=base_context.source_release_id,
        source_release_digest=base_context.source_release_digest,
        source_attestation_key=base_context.source_attestation_key,
        dataset_start=DAY,
        cutoff=DAY,
        current_pit_snapshot_digest=_digest("revised-pit-root"),
        current_pit_instruments=(CODE,),
        current_pit_span_digest_by_code={CODE: _digest("revised-code-span")},
    )
    selective = build_mixed_action_plan(
        baseline=baseline,
        current=current.components,
        context=revised_context,
        compatible=True,
    )
    by_component = {item.component: item for item in selective.actions}
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        action = by_component[component]
        assert action.action.value == "SELECTIVE_REBUILD"
        assert action.frozen_reuse is not None
        # These fixtures model monolithic H5/component outputs.  Selective is
        # a bounded dependency decision, but the safe writer target is the
        # entire file; it never claims an in-file hardlink or row-level COW.
        assert action.frozen_reuse.replace_existing_targets == ("artifact.h5",)
    assert by_component[Component.DOMESTIC_INDEX_CONTEXT].action.value == "REUSE"


def test_missing_minute_is_filled_by_tdx_without_tushare(
    dataset_profile,
    tmp_path,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(count=239),
    )
    calls: list[str] = []
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: calls.append("tdx") or _tdx_rows(),
        fetch_tushare_minute_rows=lambda *_args: (_ for _ in ()).throw(
            AssertionError("Tushare must not run after valid TDX")
        ),
        fetch_tushare_index_rows=lambda *_args: (),
    )

    bundle = builder.build(snapshot, source_view=view)

    assert calls == ["tdx"]
    assert builder.peak_provider_calls == 1
    assert len(bundle.provider_receipt_refs) == 1
    day = _minute_coverage(cas, bundle)["days"][0]
    assert day["status"] == "PROVIDER_FILLED"
    assert day["provider"] == "tdx" and day["final_rows"] == 240


def test_failed_tdx_falls_back_to_tushare_without_leaking_error_text(
    dataset_profile,
    tmp_path,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(count=239),
    )
    calls: list[str] = []

    def failed_tdx(*_args):
        calls.append("tdx")
        raise RuntimeError("SUPERSECRET-TDX-FAILURE")

    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=failed_tdx,
        fetch_tushare_minute_rows=lambda *_args: calls.append("tushare") or _tushare_rows(),
        fetch_tushare_index_rows=lambda *_args: (),
    )

    bundle = builder.build(snapshot, source_view=view)

    assert calls == ["tdx", "tushare"]
    day = _minute_coverage(cas, bundle)["days"][0]
    assert day["provider"] == "tushare"
    assert day["attempts"][0]["message_sha256"] == hashlib.sha256(b"RuntimeError\0SUPERSECRET-TDX-FAILURE").hexdigest()
    assert "SUPERSECRET" not in str(cas.get_json(bundle.artifact_ready_contract_ref))


def test_tushare_official_241_rows_drop_only_0930_auction_bar(
    dataset_profile,
    tmp_path,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(count=239),
    )
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: (_ for _ in ()).throw(RuntimeError("tdx down")),
        fetch_tushare_minute_rows=lambda *_args: _tushare_rows_with_auction(),
        fetch_tushare_index_rows=lambda *_args: (),
    )

    bundle = builder.build(snapshot, source_view=view)

    day = _minute_coverage(cas, bundle)["days"][0]
    assert day["provider"] == "tushare" and day["final_rows"] == 240
    provider = cas.get_json(bundle.provider_receipt_refs[0])
    tushare_call = next(item for item in provider["calls"] if item["provider"] == "tushare")
    assert len(tushare_call["rows"]) == 240
    assert all("09:30:00" not in str(row["trade_time"]) for row in tushare_call["rows"])


@pytest.mark.parametrize("same_value", [True, False])
def test_recheck_allows_db_to_absorb_identical_overlay_but_blocks_conflict(
    dataset_profile,
    tmp_path,
    same_value: bool,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(count=239),
    )
    calls: list[str] = []
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: calls.append("tdx") or _tdx_rows(),
        fetch_tushare_minute_rows=lambda *_args: (_ for _ in ()).throw(AssertionError("Tushare is not expected")),
        fetch_tushare_index_rows=lambda *_args: (),
    )
    bundle = builder.build(snapshot, source_view=view)
    initial_loaded = load_artifact_ready_contract(
        cas,
        dataset_profile,
        bundle.artifact_ready_contract_ref,
        expected_source_content_root=snapshot.source_content_root,
        expected_pit_snapshot_digest=snapshot.pit_snapshot_digest,
    )
    initial_planning = load_artifact_ready_planning_authority(
        cas,
        dataset_profile,
        SimpleNamespace(
            **vars(snapshot),
            artifact_ready_contract_ref=bundle.artifact_ready_contract_ref,
            artifact_ready_content_root=bundle.artifact_ready_content_root,
            artifact_ready_provenance_root=(initial_loaded.artifact_ready_provenance_root),
        ),
    )
    minute_descriptor = view.values["kline_minute_raw"][0]
    identity = f"kline_minute_raw:{minute_descriptor['partition_key']}"
    absorbed = dict(_minute_rows()[-1])
    if not same_value:
        absorbed["open_li"] = 9_999
    view.rows[identity].append(absorbed)
    minute_descriptor["row_count"] = 240
    minute_descriptor["content_digest"] = _digest(f"fresh-minute-{'same' if same_value else 'conflict'}")
    fresh_snapshot = SimpleNamespace(**{**vars(snapshot), "source_content_root": _digest("fresh-raw-source")})

    if same_value:
        result = builder.verify_current(
            bundle,
            fresh_snapshot=fresh_snapshot,
            observed_at=datetime.now(UTC),
            execution_id="execution-absorb",
            run_id="run-absorb",
            attempt_id="attempt-absorb",
            attempt_fence=3,
            source_view=view,
        )
        recheck = cas.get_json(result.source_probe_ref)
        assert recheck["raw_source_changed"] is True
        fresh_contract = cas.get_json(recheck["fresh_artifact_ready_contract_ref"])
        fresh_planning = load_artifact_ready_planning_authority(
            cas,
            dataset_profile,
            SimpleNamespace(
                **vars(fresh_snapshot),
                artifact_ready_contract_ref=recheck["fresh_artifact_ready_contract_ref"],
                artifact_ready_content_root=result.artifact_ready_content_root,
                artifact_ready_provenance_root=fresh_contract["artifact_ready_provenance_root"],
            ),
        )
        baseline = _component_baseline_from_artifact_ready(cas, initial_planning, snapshot)
        plan = build_mixed_action_plan(
            baseline=baseline,
            current=fresh_planning.components,
            context=MixedPlannerContext(
                source_release_id="fixture-release",
                source_release_digest=_digest("release"),
                source_attestation_key=_digest("attestation"),
                dataset_start=DAY,
                cutoff=DAY,
                current_pit_snapshot_digest=snapshot.pit_snapshot_digest,
                current_pit_instruments=(CODE,),
                current_pit_span_digest_by_code=pit_span_digest_by_code(snapshot.pit_snapshot),
            ),
            compatible=True,
        )
        assert {item.action.value for item in plan.actions} == {"REUSE"}
    else:
        with pytest.raises(
            ArtifactReadySourceError,
            match="effective DB plus immutable overlay roots differ",
        ):
            builder.verify_current(
                bundle,
                fresh_snapshot=fresh_snapshot,
                observed_at=datetime.now(UTC),
                execution_id="execution-conflict",
                run_id="run-conflict",
                attempt_id="attempt-conflict",
                attempt_fence=4,
                source_view=view,
            )
    assert calls == ["tdx"]


def test_fixed_loopback_tdx_kline_envelope_and_rfc3339_time(
    dataset_profile,
    tmp_path,
    monkeypatch,
) -> None:
    cas, _view, _snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(),
    )
    captured: dict[str, Any] = {}
    payload = {
        "code": 0,
        "message": "success",
        "data": {
            "count": 1,
            "list": [
                {
                    "Time": "2026-07-31T09:31:00+08:00",
                    "Open": 10_000,
                    "High": 11_000,
                    "Low": 9_000,
                    "Close": 10_500,
                    "Volume": 100,
                    "Amount": 100_000,
                }
            ],
            "meta": {"source": "tdx", "type": "minute1"},
        },
    }

    class _Response:
        is_redirect = False
        is_permanent_redirect = False

        @staticmethod
        def raise_for_status() -> None:
            return None

        @staticmethod
        def iter_content(*, chunk_size: int):
            assert chunk_size == 64 * 1024
            yield json.dumps(payload).encode("utf-8")

    class _Session:
        trust_env = True

        def get(self, url, **kwargs):
            captured.update(url=url, kwargs=kwargs, trust_env=self.trust_env)
            return _Response()

        def close(self) -> None:
            captured["closed"] = True

    import requests

    monkeypatch.setattr(requests, "Session", _Session)
    builder = ArtifactReadySourceBuilder(dataset_profile, cas)

    rows = builder._fetch_tdx_rows(CODE, DAY, DAY)

    assert len(rows) == 1 and rows[0]["Time"].endswith("+08:00")
    assert captured["url"] == "http://127.0.0.1:19080/api/kline-all/tdx"
    assert captured["kwargs"]["params"] == {
        "code": "000001",
        "type": "minute1",
        "limit": 240,
    }
    assert captured["trust_env"] is False and captured["closed"] is True


def test_qfq_denominator_includes_higher_factor_during_pit_excluded_interval(
    dataset_profile,
    tmp_path,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(),
    )
    first = DAY - timedelta(days=2)
    excluded = DAY - timedelta(days=1)
    pit = freeze_pit_snapshot(
        [
            {
                "ts_code": CODE,
                "eligible_start": first,
                "eligible_end": first,
                "entry_reason": "fixture-first-span",
                "exit_reason": "fixture-excluded",
            },
            {
                "ts_code": CODE,
                "eligible_start": DAY,
                "eligible_end": DAY,
                "entry_reason": "fixture-reentry",
                "exit_reason": None,
            },
        ],
        universe_key=dataset_profile.universe_key,
        rule_version=dataset_profile.universe_rule_version,
        scope_start=first,
        cutoff=DAY,
        state_identity=_digest("pit-interruption-state"),
        source_fingerprint_sha256=_digest("pit-interruption-source"),
        parameter_hash=_digest("pit-interruption-params"),
        state_start=first,
        state_end=DAY,
    )
    snapshot.pit_snapshot = pit
    snapshot.pit_snapshot_digest = pit.spans_sha256
    snapshot.source_content_root = _digest("pit-interruption-raw-source")
    for dataset in ("kline_daily_raw", "adj_factor"):
        descriptor = view.values[dataset][0]
        old_identity = f"{dataset}:{descriptor['partition_key']}"
        descriptor["partition_key"] = f"{first.isoformat()}_{DAY.isoformat()}"
        descriptor["row_count"] = 3
        descriptor["content_digest"] = _digest(f"{dataset}:pit-interruption")
        new_identity = f"{dataset}:{descriptor['partition_key']}"
        view.rows.pop(old_identity)
        if dataset == "kline_daily_raw":
            view.rows[new_identity] = [
                {"ts_code": CODE, "trade_date": day.isoformat()} for day in (first, excluded, DAY)
            ]
        else:
            view.rows[new_identity] = [
                {
                    "ts_code": CODE,
                    "trade_date": day.isoformat(),
                    "adj_factor": factor,
                }
                for day, factor in ((first, 1.0), (excluded, 5.0), (DAY, 2.0))
            ]
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: (),
        fetch_tushare_minute_rows=lambda *_args: (),
        fetch_tushare_index_rows=lambda *_args: (),
    )

    bundle = builder.build(snapshot, source_view=view)

    contract = cas.get_json(bundle.artifact_ready_contract_ref)
    qfq = cas.get_json(contract["qfq_denominator_authority_ref"])
    assert qfq["values"] == [
        {
            "ts_code": CODE,
            "denominator": 5.0,
            "adj_row_count": 3,
            "ordered_adj_series_sha256": qfq["values"][0]["ordered_adj_series_sha256"],
        }
    ]


def test_minute_bucket_assignment_is_linear_in_instruments_not_partitions(
    dataset_profile,
    tmp_path,
    monkeypatch,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(),
    )
    codes = tuple(f"{number:06d}.SZ" for number in range(1, 21))
    pit = freeze_pit_snapshot(
        [
            {
                "ts_code": code,
                "eligible_start": DAY,
                "eligible_end": DAY,
                "entry_reason": "linear-bucket-fixture",
                "exit_reason": None,
            }
            for code in codes
        ],
        universe_key=dataset_profile.universe_key,
        rule_version=dataset_profile.universe_rule_version,
        scope_start=DAY,
        cutoff=DAY,
        state_identity=_digest("linear-bucket-state"),
        source_fingerprint_sha256=_digest("linear-bucket-source"),
        parameter_hash=_digest("linear-bucket-params"),
        state_start=DAY,
        state_end=DAY,
    )
    snapshot.pit_snapshot = pit
    snapshot.pit_snapshot_digest = pit.spans_sha256
    snapshot.source_content_root = _digest("linear-bucket-raw")
    for dataset, rows in (
        (
            "kline_daily_raw",
            [{"ts_code": code, "trade_date": DAY.isoformat()} for code in codes],
        ),
        (
            "adj_factor",
            [
                {
                    "ts_code": code,
                    "trade_date": DAY.isoformat(),
                    "adj_factor": 1.0,
                }
                for code in codes
            ],
        ),
    ):
        descriptor = view.values[dataset][0]
        identity = f"{dataset}:{descriptor['partition_key']}"
        descriptor["row_count"] = len(rows)
        descriptor["content_digest"] = _digest(f"linear:{dataset}")
        view.rows[identity] = rows
    original_minute = view.values["kline_minute_raw"][0]
    view.rows.pop(f"kline_minute_raw:{original_minute['partition_key']}")
    buckets: dict[int, list[str]] = {}
    for code in codes:
        bucket = artifact_ready_module._minute_bucket(code, dataset_profile.minute_code_bucket_count)
        buckets.setdefault(bucket, []).append(code)
    view.values["kline_minute_raw"] = []
    for bucket, bucket_codes in sorted(buckets.items()):
        partition_key = f"{DAY.isoformat()}_{DAY.isoformat()}_bucket-{bucket:04d}"
        rows = [{**row, "ts_code": code} for code in bucket_codes for row in _minute_rows()]
        view.values["kline_minute_raw"].append(
            {
                **original_minute,
                "partition_key": partition_key,
                "row_count": len(rows),
                "content_digest": _digest(f"linear:minute:{bucket}"),
            }
        )
        view.rows[f"kline_minute_raw:{partition_key}"] = rows
    calls = 0
    original_bucket = artifact_ready_module._minute_bucket

    def counted(code: str, bucket_count: int) -> int:
        nonlocal calls
        calls += 1
        return original_bucket(code, bucket_count)

    monkeypatch.setattr(artifact_ready_module, "_minute_bucket", counted)
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: (),
        fetch_tushare_minute_rows=lambda *_args: (),
        fetch_tushare_index_rows=lambda *_args: (),
    )

    builder.build(snapshot, source_view=view)

    assert calls == len(codes)


def test_minute_eligibility_index_matches_reference_pit_filter_and_order() -> None:
    first = date(2026, 1, 1)
    trading_dates = tuple(
        first + timedelta(days=ordinal) for ordinal in range(24) if (first + timedelta(days=ordinal)).weekday() < 5
    )
    cutoff = trading_dates[-1]
    codes = ("000001.SZ", "000002.SZ", "600001.SH")
    snapshot = freeze_pit_snapshot(
        [
            {
                "ts_code": codes[0],
                "eligible_start": first,
                "eligible_end": first + timedelta(days=5),
                "entry_reason": "first",
                "exit_reason": "gap",
            },
            {
                "ts_code": codes[0],
                "eligible_start": first + timedelta(days=9),
                "eligible_end": cutoff,
                "entry_reason": "reentry",
                "exit_reason": None,
            },
            {
                "ts_code": codes[1],
                "eligible_start": first + timedelta(days=3),
                "eligible_end": first + timedelta(days=14),
                "entry_reason": "middle",
                "exit_reason": "done",
            },
            {
                "ts_code": codes[2],
                "eligible_start": first + timedelta(days=6),
                "eligible_end": cutoff,
                "entry_reason": "late",
                "exit_reason": None,
            },
        ],
        universe_key="fixture",
        rule_version="fixture-v1",
        scope_start=first,
        cutoff=cutoff,
        state_identity=_digest("eligibility-state"),
        source_fingerprint_sha256=_digest("eligibility-source"),
        parameter_hash=_digest("eligibility-params"),
        state_start=first,
        state_end=cutoff,
    )
    minute_start = first + timedelta(days=2)
    bucket_count = 4
    index = artifact_ready_module._MinuteEligibilityIndex.build(
        trading_dates=trading_dates,
        spans=snapshot.spans,
        minute_start_date=minute_start,
        bucket_count=bucket_count,
    )
    spans_by_code = {code: tuple(span for span in snapshot.spans if span.ts_code == code) for code in codes}
    windows = (
        (first - timedelta(days=10), first + timedelta(days=1)),
        (first, first + timedelta(days=10)),
        (first + timedelta(days=4), first + timedelta(days=18)),
        (first + timedelta(days=20), cutoff),
    )

    for bucket in range(bucket_count):
        bucket_codes = tuple(
            code for code in sorted(codes) if artifact_ready_module._minute_bucket(code, bucket_count) == bucket
        )
        for start, end in windows:
            reference = tuple(
                (code, day)
                for code in bucket_codes
                for day in trading_dates
                if max(start, minute_start) <= day <= end
                and any(span.eligible_start <= day <= span.eligible_end for span in spans_by_code[code])
            )
            assert tuple(index.iter_expected(bucket=bucket, start=start, end=end)) == reference


def test_minute_eligibility_index_scans_calendar_once_and_bisects_per_query(
    monkeypatch,
) -> None:
    first = date(2000, 1, 1)
    values = tuple(first + timedelta(days=ordinal) for ordinal in range(10_000))

    class _CountingDates:
        yielded = 0

        def __iter__(self):
            for value in values:
                self.yielded += 1
                yield value

    dates = _CountingDates()
    codes = tuple(f"{ordinal:06d}.SZ" for ordinal in range(1, 65))
    spans = tuple(
        SimpleNamespace(
            ts_code=code,
            eligible_start=values[-1],
            eligible_end=values[-1],
        )
        for code in codes
    )
    index = artifact_ready_module._MinuteEligibilityIndex.build(
        trading_dates=dates,
        spans=spans,
        minute_start_date=first,
        bucket_count=1,
    )
    assert dates.yielded == len(values)
    bisect_calls = 0
    original_left = artifact_ready_module.bisect_left
    original_right = artifact_ready_module.bisect_right

    def counted_left(*args, **kwargs):
        nonlocal bisect_calls
        bisect_calls += 1
        return original_left(*args, **kwargs)

    def counted_right(*args, **kwargs):
        nonlocal bisect_calls
        bisect_calls += 1
        return original_right(*args, **kwargs)

    monkeypatch.setattr(artifact_ready_module, "bisect_left", counted_left)
    monkeypatch.setattr(artifact_ready_module, "bisect_right", counted_right)
    descriptor_queries = 300
    for ordinal in range(descriptor_queries):
        day = values[ordinal]
        assert tuple(index.iter_expected(bucket=0, start=day, end=day)) == ()

    assert dates.yielded == len(values)
    assert bisect_calls == descriptor_queries * (2 + len(codes))


def test_provider_overlap_conflict_is_terminal_and_seals_safe_failure(
    dataset_profile,
    tmp_path,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(count=1, open_li=9_999),
    )
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: _tdx_rows(),
        fetch_tushare_minute_rows=lambda *_args: _tushare_rows(),
        fetch_tushare_index_rows=lambda *_args: (),
    )

    with pytest.raises(ArtifactReadySourceError) as caught:
        builder.build(snapshot, source_view=view)

    assert caught.value.code == "BLOCKED_MINUTE_PROVIDER_CONFLICT"
    failure = cas.get_json(caught.value.context["failure_receipt_ref"])
    assert failure["exception_type"] == "MinuteSourceConflict"
    assert "message" not in failure and len(failure["message_sha256"]) == 64


class _RateLimited(RuntimeError):
    code = "40203"


@pytest.mark.parametrize("tushare_rows", ["rate-limit", "still-missing"])
def test_tushare_terminal_or_still_missing_never_claims_complete(
    dataset_profile,
    tmp_path,
    tushare_rows: str,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(count=239),
    )

    def fallback(*_args):
        if tushare_rows == "rate-limit":
            raise _RateLimited("provider 40203 token=SHOULD_NOT_PERSIST")
        return _tushare_rows(count=239)

    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: (_ for _ in ()).throw(RuntimeError("tdx down")),
        fetch_tushare_minute_rows=fallback,
        fetch_tushare_index_rows=lambda *_args: (),
    )

    with pytest.raises(ArtifactReadySourceError) as caught:
        builder.build(snapshot, source_view=view)

    if tushare_rows == "rate-limit":
        assert caught.value.code == "BLOCKED_PROVIDER_TERMINAL_40203"
    else:
        assert caught.value.code == "BLOCKED_ARTIFACT_READY_COVERAGE_INCOMPLETE"
    failure = cas.get_json(caught.value.context["failure_receipt_ref"])
    assert "SHOULD_NOT_PERSIST" not in str(failure)


def test_full_day_suspension_explains_zero_minute_rows_without_provider(
    dataset_profile,
    tmp_path,
) -> None:
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=(),
        suspended=True,
    )
    calls: list[str] = []
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: calls.append("tdx"),
        fetch_tushare_minute_rows=lambda *_args: calls.append("tushare"),
        fetch_tushare_index_rows=lambda *_args: calls.append("index"),
    )

    bundle = builder.build(snapshot, source_view=view)

    assert calls == []
    day = _minute_coverage(cas, bundle)["days"][0]
    assert day["status"] == "SUSPENDED_FULL_DAY" and day["final_rows"] == 0


def test_index_provider_runs_only_for_missing_code_and_overlay_is_immutable(
    dataset_profile,
    tmp_path,
) -> None:
    missing = DOMESTIC_INDEX_DEFINITIONS[0].daily_code
    cas, view, snapshot = _fixture(
        dataset_profile,
        tmp_path,
        minute_rows=_minute_rows(),
        missing_index_code=missing,
    )
    calls: list[str] = []

    def index_rows(definition, start, end):
        calls.append(definition.daily_code)
        assert start == end == DAY
        return [_index_row(definition.daily_code)]

    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tdx_rows=lambda *_args: (),
        fetch_tushare_minute_rows=lambda *_args: (),
        fetch_tushare_index_rows=index_rows,
    )

    bundle = builder.build(snapshot, source_view=view)

    assert calls == [missing]
    assert len(bundle.provider_receipt_refs) == 1
    contract = cas.get_json(bundle.artifact_ready_contract_ref)
    index_manifest = cas.get_json(contract["component_manifests"]["domestic_index_context"])
    merged = next(item for item in index_manifest["partitions"] if item["dataset"] == "index_daily_merged")
    receipt = cas.get_json(merged["rows_ref"])
    assert len(receipt["rows"]) == len(DOMESTIC_INDEX_DEFINITIONS)
    assert receipt["details"][missing]["provider_fill_rows"] == 1


def _star_base_point_index_view(*, include_required_row: bool) -> tuple[_View, tuple[date, date]]:
    base_point = date(2019, 12, 31)
    required_day = date(2020, 1, 2)
    partition_key = "2019-11-01_2020-01-31"
    rows: list[Mapping[str, Any]] = []
    for definition in DOMESTIC_INDEX_DEFINITIONS:
        rows.append(_index_row(definition.daily_code, trade_date=base_point))
        if definition.daily_code != "000688.SH" or include_required_row:
            rows.append(_index_row(definition.daily_code, trade_date=required_day))
    descriptor = {
        "dataset": "index_daily",
        "partition_key": partition_key,
        "row_count": len(rows),
        "content_digest": _digest("star-base-point-index-content"),
        "schema_digest": _digest("star-base-point-index-schema"),
    }
    return (
        _View(
            {"index_daily": [descriptor]},
            {f"index_daily:{partition_key}": rows},
        ),
        (base_point, required_day),
    )


def test_index_partition_ignores_star_base_point_before_required_from(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "star-base-point-control")
    cas = CASStore(store.root)
    view, trading_dates = _star_base_point_index_view(include_required_row=True)
    provider_calls: list[str] = []
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tushare_index_rows=lambda definition, *_args: provider_calls.append(definition.daily_code),
    )

    entries, provider_refs, _derived_refs = builder._index_entries(
        view,
        cutoff=trading_dates[-1],
        trading_dates=trading_dates,
        checkpoint=lambda: None,
    )

    assert provider_calls == []
    assert provider_refs == ()
    receipt = cas.get_json(entries[0]["rows_ref"])
    star_dates = [row["trade_date"] for row in receipt["rows"] if row["ts_code"] == "000688.SH"]
    assert star_dates == [trading_dates[-1].isoformat()]
    assert receipt["details"]["000688.SH"]["expected_rows"] == 1
    assert len(receipt["rows"]) == (len(DOMESTIC_INDEX_DEFINITIONS) - 1) * 2 + 1


def test_index_partition_still_requires_star_rows_on_and_after_required_from(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "star-required-row-control")
    cas = CASStore(store.root)
    view, trading_dates = _star_base_point_index_view(include_required_row=False)
    provider_calls: list[tuple[str, date, date]] = []

    def no_provider_rows(definition, start, end):
        provider_calls.append((definition.daily_code, start, end))
        return ()

    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tushare_index_rows=no_provider_rows,
    )

    with pytest.raises(ArtifactReadySourceError) as failure:
        builder._index_entries(
            view,
            cutoff=trading_dates[-1],
            trading_dates=trading_dates,
            checkpoint=lambda: None,
        )

    assert failure.value.code == ArtifactReadyCoverageIncomplete.code
    assert provider_calls == [("000688.SH", trading_dates[-1], trading_dates[-1])]


def test_index_partition_rejects_post_required_noncalendar_database_key(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "index-rogue-calendar-control")
    cas = CASStore(store.root)
    view, trading_dates = _star_base_point_index_view(include_required_row=True)
    partition_key = "2019-11-01_2020-01-31"
    view.rows[f"index_daily:{partition_key}"].append(_index_row("000001.SH", trade_date=date(2020, 1, 1)))
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tushare_index_rows=lambda *_args: (),
    )

    with pytest.raises(
        ArtifactReadyCoverageIncomplete,
        match="unexpected calendar key: 000001.SH:2020-01-01",
    ):
        builder._index_entries(
            view,
            cutoff=trading_dates[-1],
            trading_dates=trading_dates,
            checkpoint=lambda: None,
        )


def test_index_partition_rejects_post_required_key_beyond_effective_cutoff(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "index-beyond-cutoff-control")
    cas = CASStore(store.root)
    view, trading_dates = _star_base_point_index_view(include_required_row=True)
    partition_key = "2019-11-01_2020-01-31"
    view.rows[f"index_daily:{partition_key}"] = [
        row
        for row in view.rows[f"index_daily:{partition_key}"]
        if row["trade_date"] != trading_dates[-1].isoformat() or row["ts_code"] == "000688.SH"
    ]
    builder = ArtifactReadySourceBuilder(
        dataset_profile,
        cas,
        fetch_tushare_index_rows=lambda *_args: (),
    )

    with pytest.raises(
        ArtifactReadyCoverageIncomplete,
        match="unexpected calendar key: 000688.SH:2020-01-02",
    ):
        builder._index_entries(
            view,
            cutoff=trading_dates[0],
            trading_dates=trading_dates[:1],
            checkpoint=lambda: None,
        )
