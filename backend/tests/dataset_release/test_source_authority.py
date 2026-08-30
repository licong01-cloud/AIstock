from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest
import pandas as pd

from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release import source_authority as source_authority_module
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.source_authority import (
    PRODUCTION_QUERY_SPECS,
    MonthlySourceAuthority,
    PostgresSourceSnapshotSession,
    SourceAuditIncomplete,
    SourceRequiredTableMissing,
    SourceSnapshotRevised,
    SourceTableSchema,
    load_source_stage_receipt,
    seal_source_stage_receipt,
)
from backend.services.dataset_release.profile import load_dataset_profile
from backend.services.dataset_release.sector_data_candidate_source import (
    SECTOR_CANDIDATE_SOURCE_SCHEMA,
)
from backend.services.dataset_release.sealed_source_reader import (
    CASSealedPartitionReader,
)
from backend.services.dataset_release.errors import SourceManifestError
from backend.services.dataset_release.factor_materializer import _normalize_aux_frame
from backend.services.dataset_release.contracts import Component
from backend.services.dataset_release.source_manifest import (
    ColumnKind,
    ColumnSpec,
    PartitionSpec,
)


class _NamedCursorFixture:
    def __init__(self, connection: "_NamedCursorConnectionFixture", name: str) -> None:
        self._connection = connection
        self._name = name
        self._rows = [(date(2026, 7, 1),)]
        self.description = (("trade_date",),)
        self.itersize = 0

    def execute(self, _sql: str, _params: Mapping[str, Any]) -> None:
        return None

    def fetchmany(self, _size: int) -> list[tuple[date]]:
        rows, self._rows = self._rows, []
        return rows

    def close(self) -> None:
        self._connection.active_names.remove(self._name)


class _NamedCursorConnectionFixture:
    def __init__(self) -> None:
        self.active_names: set[str] = set()
        self.opened_names: list[str] = []

    def cursor(self, *, name: str) -> _NamedCursorFixture:
        if name in self.active_names:
            raise RuntimeError(f"duplicate cursor: {name}")
        self.active_names.add(name)
        self.opened_names.append(name)
        return _NamedCursorFixture(self, name)


def test_postgres_snapshot_parallel_same_query_streams_use_unique_cursor_names(dataset_profile) -> None:
    connection = _NamedCursorConnectionFixture()
    session = PostgresSourceSnapshotSession(dataset_profile.resource_policy)
    session._connection = connection

    first = session.stream("trading_dates", {"start": date(2026, 7, 1)}, fetch_rows=1)
    second = session.stream("trading_dates", {"start": date(2026, 7, 2)}, fetch_rows=1)
    try:
        assert next(first) == {"trade_date": date(2026, 7, 1)}
        assert next(second) == {"trade_date": date(2026, 7, 1)}
        assert len(connection.opened_names) == 2
        assert len(set(connection.opened_names)) == 2
        assert all(name.startswith("dataset_release_") and len(name) <= 63 for name in connection.opened_names)
    finally:
        first.close()
        second.close()

    assert connection.active_names == set()


class FakeSnapshotSession:
    def __init__(self) -> None:
        self.snapshot_tokens = ("fake-repeatable-read:1",)
        self.revision = "v1"
        self.query_revisions: dict[str, str] = {}
        self.audit_revisions: dict[str, str] = {}
        self.audit_quality_overrides: dict[str, str] = {}
        self.pit_exit = date(2026, 7, 31)
        self.missing: str | None = None
        self.large_query: str | None = None
        self.large_rows = 0
        self.entered = 0
        self.stream_calls: list[str] = []
        self.stream_params: list[tuple[str, Mapping[str, Any]]] = []
        self.pit_codes: tuple[str, ...] = ("000001.SZ",)
        self.audit_omit: str | None = None
        self.audit_fail: str | None = None
        self.audit_extra_failed: str | None = None
        self.audit_duplicate: str | None = None
        self.audit_datasets_requested: tuple[str, ...] = ()
        self.audit_secret: str | None = None
        self.payload_overrides: dict[str, dict[str, Any]] = {}
        self.row_key_overrides: dict[str, list[Any]] = {}
        self.writer_rows: list[Mapping[str, Any]] = []
        self.total_data_partition_streams = 0
        self.current_data_query_ids: set[str] = set()
        self.max_data_query_ids_per_session = 0
        self.session_count = 0
        self.mutate_writer_after_partitions: int | None = None
        self.fingerprint_revisions: dict[str, int] = {}
        self.fingerprint_unavailable: set[str] = set()
        self.fingerprint_calls: list[str] = []
        self.fingerprint_call_counts: dict[str, int] = {}
        self.fingerprint_readback_drift_query: str | None = None
        self.pit_rule_version = "st_pub_next_trade_restore_active_l_v1"
        self.pit_scope = "st_only_active"

    def __enter__(self):
        self.entered += 1
        self.session_count += 1
        self.current_data_query_ids = set()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.max_data_query_ids_per_session = max(
            self.max_data_query_ids_per_session,
            len(self.current_data_query_ids),
        )
        self.entered -= 1

    def describe(self, query_id: str) -> SourceTableSchema:
        if query_id == self.missing:
            raise SourceRequiredTableMissing(f"missing fixture source: {query_id}")
        if query_id == "pit_state":
            columns = (
                "universe_key",
                "rule_version",
                "scope",
                "start_date",
                "end_date",
                "status",
                "dirty",
            )
            table = "market.stock_universe_pit_state"
        elif query_id == "pit_spans":
            columns = (
                "universe_key",
                "ts_code",
                "eligible_start",
                "eligible_end",
                "entry_reason",
                "exit_reason",
                "rule_version",
            )
            table = "market.stock_universe_pit_spans"
        elif query_id == "refresh_audit":
            columns = (
                "dataset",
                "trade_date",
                "data_source",
                "job_id",
                "status",
                "row_count",
                "refreshed_at",
                "error_message",
                "data_max_at",
                "written_rows",
                "expected_rows",
                "coverage_ratio",
                "quality_status",
                "failure_category",
                "metadata",
            )
            table = "market.dataset_date_refresh_audit"
        elif query_id == "writer_ledger":
            columns = (
                "ledger_kind",
                "ledger_identity",
                "status",
                "created_at",
                "started_at",
                "finished_at",
                "opaque_payload",
            )
            table = "market.source_writer_ledger_union_v1"
        else:
            spec = PRODUCTION_QUERY_SPECS[query_id]
            columns = spec.required_columns
            table = spec.table_identity
        return SourceTableSchema(table, tuple(dict.fromkeys(columns)))

    def fetch_one(
        self,
        query_id: str,
        params: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        if query_id == "official_cutoff":
            return {"official_cutoff": params["cutoff"]}
        if query_id == "pit_state":
            return {
                "universe_key": params["universe_key"],
                "rule_version": self.pit_rule_version,
                "scope": self.pit_scope,
                "start_date": date(2026, 7, 1),
                "end_date": date(2026, 7, 31),
                "status": "ready",
                "dirty": False,
                "state_payload": json.dumps({"state": "ready", "revision": self.revision}, sort_keys=True),
            }
        raise AssertionError(query_id)

    def stream(
        self,
        query_id: str,
        params: Mapping[str, Any],
        *,
        fetch_rows: int,
    ) -> Iterable[Mapping[str, Any]]:
        assert self.entered == 1
        assert fetch_rows > 0
        self.stream_calls.append(query_id)
        self.stream_params.append((query_id, dict(params)))
        if query_id == "trading_dates":
            yield {"trade_date": date(2026, 7, 1)}
            return
        if query_id == "writer_ledger":
            yield from self.writer_rows
            return
        if query_id == "refresh_audit":
            self.audit_datasets_requested = tuple(params["datasets"])
            spec_by_audit = {
                str(spec.audit_dataset): spec
                for spec in PRODUCTION_QUERY_SPECS.values()
                if spec.audit_dataset is not None
            }
            for dataset in params["datasets"]:
                if dataset == self.audit_omit:
                    continue
                spec = spec_by_audit[dataset]
                authority_source = spec.audit_eligible_sources[0]
                success_row = {
                    "dataset": dataset,
                    "trade_date": date(2026, 7, 1),
                    "data_source": authority_source,
                    "job_id": "00000000-0000-0000-0000-000000000001",
                    "status": "failed" if dataset == self.audit_fail else "success",
                    "row_count": 1,
                    "refreshed_at": "2026-08-01T00:00:00+00:00",
                    "error_message": "fixture failure" if dataset == self.audit_fail else None,
                    "data_max_at": None,
                    "written_rows": 1,
                    "expected_rows": 1,
                    "coverage_ratio": "1",
                    "quality_status": self.audit_quality_overrides.get(dataset, "ok"),
                    "failure_category": None,
                    "metadata_json": json.dumps(
                        {"opaque": self.audit_secret} if self.audit_secret is not None else {},
                        sort_keys=True,
                    ),
                    "audit_payload": json.dumps(
                        {
                            "dataset": dataset,
                            "revision": self.audit_revisions.get(dataset, self.revision),
                            "opaque": self.audit_secret,
                        },
                        sort_keys=True,
                    ),
                }
                yield success_row
                if dataset == self.audit_duplicate:
                    yield dict(success_row)
                if dataset == self.audit_extra_failed:
                    yield {
                        **success_row,
                        "data_source": "readiness_gate",
                        "status": "failed",
                        "error_message": (
                            f"non-authoritative provider failed {self.audit_secret}"
                            if self.audit_secret
                            else "non-authoritative provider failed"
                        ),
                        "quality_status": "error",
                        "audit_payload": json.dumps(
                            {
                                "dataset": dataset,
                                "source": "readiness_gate",
                                "status": "failed",
                            },
                            sort_keys=True,
                        ),
                    }
            return
        if query_id == "pit_spans":
            requested = tuple(params.get("codes") or self.pit_codes)
            for code in self.pit_codes:
                if code not in requested:
                    continue
                yield {
                    "ts_code": code,
                    "eligible_start": date(2026, 7, 1),
                    "eligible_end": self.pit_exit,
                    "entry_reason": "listed",
                    "exit_reason": "scope_end",
                    "semantic_payload": json.dumps(
                        {"revision": self.revision, "pit_exit": self.pit_exit.isoformat(), "ts_code": code},
                        sort_keys=True,
                    ),
                }

            return
        self.current_data_query_ids.add(query_id)
        self.total_data_partition_streams += 1
        if (
            self.mutate_writer_after_partitions is not None
            and self.total_data_partition_streams == self.mutate_writer_after_partitions
        ):
            self.writer_rows.append(
                {
                    "ledger_kind": "ingestion_jobs",
                    "ledger_identity": "00000000-0000-0000-0000-000000000099",
                    "status": "success",
                    "created_at": datetime(2026, 7, 31, 1, tzinfo=UTC),
                    "started_at": datetime(2026, 7, 31, 1, tzinfo=UTC),
                    "finished_at": datetime(2026, 7, 31, 1, 1, tzinfo=UTC),
                    "opaque_payload": '{"revision":"mutated"}',
                }
            )
        count = self.large_rows if query_id == self.large_query else 1
        spec = PRODUCTION_QUERY_SPECS[query_id]
        revision = self.query_revisions.get(query_id, self.revision)
        revision_number = 1 if revision == "v1" else 2
        for index in range(count):
            code = f"{index + 1:06d}.SZ"
            key_values = {
                "ts_code": code,
                "trade_date": "2026-07-01",
                "trade_time": f"2026-07-01T09:{30 + (index % 30):02d}:00+08:00",
                "freq": "1min",
                "cal_date": "2026-07-01",
                "suspend_type": "S",
                "index_code": f"{801010 + index:06d}.SI",
                "in_date": "2026-07-01",
                "l2_code": "801010.SI",
            }
            text_values = {
                "suspend_timing": "09:30",
                "list_date": "2020-01-01",
                "list_status": "L",
                "exchange": "SZSE",
                "market": f"main-{revision}",
                "level": "L2",
                "out_date": "2026-07-31",
            }
            payload: dict[str, Any] = {}
            for column in dict.fromkeys((*spec.key_columns, *spec.value_columns)):
                if column in key_values:
                    payload[column] = key_values[column]
                elif column == "is_trading":
                    payload[column] = True
                elif column in text_values:
                    payload[column] = text_values[column]
                else:
                    payload[column] = revision_number
            payload.update(self.payload_overrides.get(query_id, {}))
            row_key = self.row_key_overrides.get(
                query_id,
                [payload[column] for column in spec.key_columns],
            )
            yield {
                "row_key": json.dumps(
                    row_key,
                    separators=(",", ":"),
                ),
                "row_payload": json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }

    def partition_fingerprint(
        self,
        query_id: str,
        params: Mapping[str, Any],
        *,
        query_version: str,
        table_schema_digest: str,
    ) -> Mapping[str, Any] | None:
        self.fingerprint_calls.append(query_id)
        self.fingerprint_call_counts[query_id] = self.fingerprint_call_counts.get(query_id, 0) + 1
        if query_id in self.fingerprint_unavailable:
            return None
        row_count = self.large_rows if query_id == self.large_query else 1
        revision = self.fingerprint_revisions.get(query_id, 100)
        if query_id == self.fingerprint_readback_drift_query and self.fingerprint_call_counts[query_id] >= 2:
            revision += 1
        raw = {
            "row_count": row_count,
            "min_key": "[]" if row_count else None,
            "max_key": "[]" if row_count else None,
            "min_row_xmin": revision if row_count else None,
            "max_row_xmin": revision if row_count else None,
            "db_system_identifier": "123456789",
            "timeline_id": 1,
            "xid_epoch": 0,
        }
        return source_authority_module._validated_mvcc_fingerprint(
            raw,
            query=PRODUCTION_QUERY_SPECS[query_id],
            params=params,
            query_version=query_version,
            table_schema_digest=table_schema_digest,
        )


def _authority(dataset_profile, tmp_path, fake, *, mvcc_reuse_capability=False):
    profile = replace(
        dataset_profile,
        start_date=date(2026, 7, 1),
        minute_start_date=date(2026, 7, 1),
    )
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    return (
        MonthlySourceAuthority(
            profile,
            cas,
            session_factory=lambda _policy: fake,
            mvcc_reuse_capability=mvcc_reuse_capability,
        ),
        cas,
    )


def _recheck_expectations(snapshot) -> tuple[Mapping[str, Any], ...]:
    return tuple(
        {
            **partition.as_build_input(),
            "recheck_partition_scope": scope,
        }
        for scope, partitions in (
            ("source", snapshot.partitions),
            ("pit", snapshot.pit_partitions),
        )
        for partition in partitions
    )


def test_production_source_allowlist_uses_semantic_projection_and_code_major_order() -> None:
    assert "index_daily_tdx" not in PRODUCTION_QUERY_SPECS
    assert "index_basic" not in PRODUCTION_QUERY_SPECS
    assert "to_jsonb(source_row)" not in source_authority_module._PIT_SPANS_SQL
    for spec in PRODUCTION_QUERY_SPECS.values():
        assert "to_jsonb(source_row)" not in spec.sql
        assert spec.required_columns == tuple(dict.fromkeys((*spec.key_columns, *spec.value_columns)))
        if spec.date_expression is not None:
            assert spec.audit_dataset is not None
            assert spec.audit_eligible_sources
    assert PRODUCTION_QUERY_SPECS["kline_daily_raw"].key_columns == (
        "ts_code",
        "trade_date",
    )
    assert PRODUCTION_QUERY_SPECS["kline_minute_raw"].key_columns == (
        "ts_code",
        "trade_time",
        "freq",
    )
    assert PRODUCTION_QUERY_SPECS["moneyflow_ts"].audit_dataset == ("stock_moneyflow_ts")
    assert "source_row.is_trading = TRUE" in PRODUCTION_QUERY_SPECS["trading_calendar"].sql
    limit_spec = PRODUCTION_QUERY_SPECS["stk_limit"]
    assert limit_spec.non_null_value_columns == ()
    assert limit_spec.audit_non_null_value_columns == (
        "pre_close",
        "up_limit",
        "down_limit",
    )


def test_canonical_source_freeze_replaces_legacy_sector_table_with_p3a_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = load_dataset_profile(
        Path(__file__).resolve().parents[3] / "configs" / "datasets" / "qe_backtest_monthly_v2.yaml"
    )
    profile = replace(
        profile,
        start_date=date(2026, 7, 1),
        minute_start_date=date(2026, 7, 1),
    )
    fake = FakeSnapshotSession()
    fake.pit_rule_version = profile.universe_rule_version
    fake.pit_scope = profile.pit_scope
    candidate_hash = "a" * 64
    selected_hash = {"value": candidate_hash}

    class FakeP3ASource:
        def __init__(self, value: str) -> None:
            self.candidate_hash = value

        @property
        def query_version(self) -> str:
            return f"sector_data_p3a_dual_authority_candidate_v1:{self.candidate_hash}"

        @property
        def source_table_identity(self) -> str:
            return f"artifact.{SECTOR_CANDIDATE_SOURCE_SCHEMA}.{self.candidate_hash}"

        def iter_rows(self, *, start, end, l2_code_map):  # noqa: ANN001, ANN201
            del start, end
            assert l2_code_map == {"801010.SI": 0}
            yield {
                "ts_code": "000001.SZ",
                "trade_date": "2026-07-01",
                "sw2_open": "1",
                "sw2_high": "1",
                "sw2_low": "1",
                "sw2_close": "1",
                "sw2_pct_change": "1",
                "sw2_vol": "1",
                "sw2_amount": "1",
                "sw2_pe": "1",
                "sw2_pb": "1",
                "sw2_total_mv": "1",
                "sw2_mf_buy_sm_amt": "1",
                "sw2_mf_sell_sm_amt": "1",
                "sw2_mf_buy_md_amt": "1",
                "sw2_mf_sell_md_amt": "1",
                "sw2_mf_buy_lg_amt": "1",
                "sw2_mf_sell_lg_amt": "1",
                "sw2_mf_buy_elg_amt": "1",
                "sw2_mf_sell_elg_amt": "1",
                "sw2_mf_net_amt": "1",
                "sw2_mf_buy_elg_vol": "1",
                "sw2_mf_sell_elg_vol": "1",
                "sw2_mf_net_vol": "1",
                "l2_code_id": 0,
            }

        def verify_unchanged(self) -> None:
            return None

        def receipt(self, *, code_map_digest, classify_partitions):  # noqa: ANN001, ANN201
            return {
                "schema_version": SECTOR_CANDIDATE_SOURCE_SCHEMA,
                "profile": profile.profile,
                "cutoff": "2026-07-31",
                "candidate_root_id": profile.candidate_root_id,
                "candidate_root_relative_path": (
                    ".sector_data_authority/qe_hmm_full_v2/2026-07-31/full"
                ),
                "candidate_scope": "full",
                "candidate_hash": self.candidate_hash,
                "industry_bundle_hash": "b" * 64,
                "classification_authority_receipt_hash": "c" * 64,
                "index_membership_authority_receipt_hash": "d" * 64,
                "source_denominator_digest": "e" * 64,
                "expected_opportunities": 1,
                "opportunity_digest": "f" * 64,
                "candidate_report_canonical_hash": "0" * 64,
                "status_counts": {"resolved": 1},
                "alignment_counts": {"aligned": 1},
                "unavailable_by_reason": {},
                "query_version": self.query_version,
                "code_map_digest": code_map_digest,
                "classify_partitions": list(classify_partitions),
                "safety": {
                    "database_writes": 0,
                    "provider_database_writes": 0,
                    "production_writes": 0,
                    "production_deletes": 0,
                    "production_pointer_changes": 0,
                    "service_process_controls": 0,
                    "candidate_writes": 0,
                },
            }

    monkeypatch.setattr(
        source_authority_module.SectorCandidateSource,
        "load",
        lambda *_args, **_kwargs: FakeP3ASource(selected_hash["value"]),
    )
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    authority = MonthlySourceAuthority(
        profile,
        cas,
        session_factory=lambda _policy: fake,
    )

    snapshot = authority.freeze(cutoff=date(2026, 7, 31))

    sector = [item for item in snapshot.partitions if item.spec.dataset == "sector_data"]
    assert len(sector) == 1
    assert sector[0].summary.row_count == 1
    assert sector[0].spec.query_version.startswith(
        f"sector_data_p3a_dual_authority_candidate_v1:{candidate_hash}:table_schema_sha256:"
    )
    assert candidate_hash in sector[0].source_table_schema.table_identity
    assert "sector_data" not in fake.stream_calls
    assert "sector_data" not in fake.audit_datasets_requested
    stage_ref = seal_source_stage_receipt(cas, snapshot, profile=profile.profile)
    loaded = load_source_stage_receipt(
        cas,
        stage_ref,
        expected_profile=profile.profile,
        expected_cutoff=date(2026, 7, 31),
        profile=profile,
    )
    assert loaded.source_content_root == snapshot.source_content_root

    selected_hash["value"] = "1" * 64
    second_fake = FakeSnapshotSession()
    second_fake.pit_rule_version = profile.universe_rule_version
    second_fake.pit_scope = profile.pit_scope
    second_store = ControlStore.initialize(tmp_path / "second-control")
    second = MonthlySourceAuthority(
        profile,
        CASStore(second_store.root),
        session_factory=lambda _policy: second_fake,
    ).freeze(cutoff=date(2026, 7, 31))
    assert second.source_content_root != snapshot.source_content_root


def test_stk_limit_raw_source_accepts_only_registered_nullable_repair_columns() -> None:
    query = PRODUCTION_QUERY_SPECS["stk_limit"]
    table = SourceTableSchema(query.table_identity, query.required_columns)
    spec = source_authority_module._query_partition_spec(
        query,
        "2024-07-01_2024-07-31",
        table,
    )
    payload = {
        "ts_code": "600001.SH",
        "trade_date": "2024-07-23",
        "pre_close": None,
        "up_limit": 11.0,
        "down_limit": 9.0,
    }

    observed = source_authority_module._validate_query_row(
        {
            "row_key": json.dumps([payload["ts_code"], payload["trade_date"]]),
            "row_payload": json.dumps(payload),
        },
        query,
        spec,
    )

    assert json.loads(observed["row_payload"]) == payload


def test_source_monthly_leaves_prove_june_to_july_moving_window_prefix(
    dataset_profile,
    tmp_path,
) -> None:
    authority, _cas = _authority(
        dataset_profile,
        tmp_path,
        FakeSnapshotSession(),
    )
    columns = (
        ColumnSpec("ts_code", ColumnKind.STRING, True),
        ColumnSpec("trade_date", ColumnKind.DATE, True),
        ColumnSpec("value", ColumnKind.INTEGER, True),
    )
    table = SourceTableSchema(
        "market.fixture_monthly",
        ("ts_code", "trade_date", "value"),
    )
    rows = (
        {"ts_code": "000001.SZ", "trade_date": "2026-05-29", "value": 1},
        {"ts_code": "000001.SZ", "trade_date": "2026-06-30", "value": 2},
        {"ts_code": "000001.SZ", "trade_date": "2026-07-31", "value": 3},
    )
    baseline = authority._seal_rows(
        spec=PartitionSpec(
            "fixture_monthly",
            "2026-05-01_2026-06-30",
            "fixture_monthly_v1",
            columns,
            ("ts_code", "trade_date"),
        ),
        rows=rows[:2],
        components=(Component.DAILY_BIN,),
        tokens=("fixture:baseline",),
        table_schema=table,
    )
    current = authority._seal_rows(
        spec=PartitionSpec(
            "fixture_monthly",
            "2026-05-01_2026-07-31",
            "fixture_monthly_v1",
            columns,
            ("ts_code", "trade_date"),
        ),
        rows=rows,
        components=(Component.DAILY_BIN,),
        tokens=("fixture:current",),
        table_schema=table,
    )

    assert [item["month"] for item in baseline.monthly_content_leaves] == [
        "2026-05",
        "2026-06",
    ]
    assert tuple(current.monthly_content_leaves[:2]) == baseline.monthly_content_leaves
    assert current.monthly_content_leaves[-1]["month"] == "2026-07"
    assert baseline.summary.content_digest != current.summary.content_digest


def test_minute_partition_requests_cross_date_chunks_and_stable_pit_code_batches(
    dataset_profile,
    tmp_path,
) -> None:
    policy = replace(
        dataset_profile.resource_policy,
        date_chunk_months=1,
    )
    profile = replace(
        dataset_profile,
        minute_start_date=date(2026, 1, 1),
        source_date_chunk_months=1,
        resource_policy=policy,
    )
    store = ControlStore.initialize(tmp_path / "control")
    authority = MonthlySourceAuthority(profile, CASStore(store.root))
    codes = [f"{value:06d}.SZ" for value in range(1, 6)]
    pit = freeze_pit_snapshot(
        [
            {
                "ts_code": code,
                "eligible_start": date(2026, 1, 1),
                "eligible_end": date(2026, 2, 28),
                "entry_reason": "listed",
                "exit_reason": "scope_end",
            }
            for code in codes
        ],
        universe_key=profile.universe_key,
        rule_version=profile.universe_rule_version,
        scope_start=date(2026, 1, 1),
        cutoff=date(2026, 2, 28),
        state_identity="state",
        source_fingerprint_sha256="a" * 64,
        parameter_hash="b" * 64,
    )
    requests = list(
        authority._partition_requests(
            PRODUCTION_QUERY_SPECS["kline_minute_raw"],
            date(2026, 2, 28),
            pit_snapshot=pit,
        )
    )
    assert requests
    assert len({key for key, _params in requests}) == len(requests)
    by_window: dict[tuple[date, date], list[str]] = {}
    for _key, params in requests:
        assert 1 <= len(params["codes"]) <= profile.minute_code_bucket_capacity
        by_window.setdefault((params["start"], params["end"]), []).extend(params["codes"])
    assert len(by_window) == 2
    assert all(sorted(values) == codes for values in by_window.values())
    assert "JOIN market.stock_basic" in PRODUCTION_QUERY_SPECS["bak_basic"].sql
    assert "EXISTS (SELECT 1 FROM market.kline_daily_raw" in PRODUCTION_QUERY_SPECS["bak_basic"].sql


def test_minute_stable_bucket_identity_localizes_new_instrument_change(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    authority = MonthlySourceAuthority(dataset_profile, CASStore(store.root))

    def snapshot(codes: list[str]):
        return freeze_pit_snapshot(
            [
                {
                    "ts_code": code,
                    "eligible_start": date(2026, 7, 1),
                    "eligible_end": date(2026, 7, 31),
                    "entry_reason": "listed",
                    "exit_reason": "scope_end",
                }
                for code in codes
            ],
            universe_key=dataset_profile.universe_key,
            rule_version=dataset_profile.universe_rule_version,
            scope_start=date(2026, 7, 1),
            cutoff=date(2026, 7, 31),
            state_identity="state",
            source_fingerprint_sha256="a" * 64,
            parameter_hash="b" * 64,
        )

    original_codes = [f"{value:06d}.SZ" for value in range(1, 101)]
    added_code = "000000.SZ"
    original = {
        key
        for key, _params in authority._partition_requests(
            PRODUCTION_QUERY_SPECS["kline_minute_raw"],
            date(2026, 7, 31),
            pit_snapshot=snapshot(original_codes),
        )
    }
    revised = {
        key
        for key, _params in authority._partition_requests(
            PRODUCTION_QUERY_SPECS["kline_minute_raw"],
            date(2026, 7, 31),
            pit_snapshot=snapshot([added_code, *original_codes]),
        )
    }

    assert not original.difference(revised)
    assert original.intersection(revised) == original


def test_source_authority_freezes_exact_streams_and_revision_identity(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    authority, cas = _authority(dataset_profile, tmp_path, fake)

    first = authority.freeze(cutoff=date(2026, 7, 31))
    replay = authority.freeze(cutoff=date(2026, 7, 31))
    assert replay.source_content_root == first.source_content_root
    assert replay.source_manifest_ref == first.source_manifest_ref
    assert first.pit_snapshot_ref.size > 0
    assert all(item.rows_ref.size > 0 for item in first.partitions)
    manifest = cas.get_json_bounded(first.source_manifest_ref, max_bytes=2 * 1024**2)
    assert manifest["source_content_root"] == first.source_content_root
    assert manifest["safety"]["database_writes"] == 0

    fake.revision = "v2"
    revised = authority.freeze(cutoff=date(2026, 7, 31))
    assert revised.source_content_root != first.source_content_root
    assert revised.source_provenance_root != first.source_provenance_root


def test_initial_migration_sample_filters_stock_codes_before_row_materialization(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.pit_codes = (
        "000001.SZ",
        "300379.SZ",
        "300741.SZ",
        "600462.SH",
        "600930.SH",
        "688981.SH",
    )
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    frozen = authority.freeze(
        cutoff=date(2026, 7, 31),
        sample_instruments=fake.pit_codes,
    )

    assert tuple(sorted(span.ts_code for span in frozen.pit_snapshot.spans)) == fake.pit_codes
    stock_queries = {
        query_id
        for query_id, spec in PRODUCTION_QUERY_SPECS.items()
        if spec.code_policy in {"pit_stock_codes", "pit_minute_code_batch"}
    }
    observed = [(query_id, params) for query_id, params in fake.stream_params if query_id in stock_queries]
    assert observed
    assert all(set(params["codes"]).issubset(fake.pit_codes) for _query_id, params in observed)
    assert all(len(params["codes"]) <= 20 for _query_id, params in observed)
    assert all("ANY(%(codes)s)" in PRODUCTION_QUERY_SPECS[query_id].sql for query_id in stock_queries)


def test_exact_recheck_consumes_all_rows_without_cas_write_temp_or_blob_read(
    dataset_profile,
    tmp_path,
    monkeypatch,
) -> None:
    fake = FakeSnapshotSession()
    authority, cas = _authority(dataset_profile, tmp_path, fake)
    sealed = authority.freeze(cutoff=date(2026, 7, 31))
    expectations = _recheck_expectations(sealed)
    row_paths = {cas.root / item.rows_ref.relative_path for item in (*sealed.partitions, *sealed.pit_partitions)}
    before_files = {path.relative_to(cas.cas_root).as_posix() for path in cas.cas_root.rglob("*") if path.is_file()}
    calls_before = len(fake.stream_calls)
    existing_blob_reads = 0
    original_open = Path.open

    def guarded_open(path, *args, **kwargs):
        nonlocal existing_blob_reads
        if Path(path) in row_paths:
            existing_blob_reads += 1
        return original_open(path, *args, **kwargs)

    def forbidden_temp(*_args, **_kwargs):
        raise AssertionError("exact source recheck cannot allocate CAS temp bytes")

    monkeypatch.setattr(Path, "open", guarded_open)
    monkeypatch.setattr(
        "backend.services.dataset_release.cas_store.tempfile.mkstemp",
        forbidden_temp,
    )
    rechecked = authority.freeze(
        cutoff=date(2026, 7, 31),
        recheck_partition_expectations=expectations,
        expected_source_content_root=sealed.source_content_root,
        expected_pit_snapshot_digest=sealed.pit_snapshot_digest,
    )

    after_files = {path.relative_to(cas.cas_root).as_posix() for path in cas.cas_root.rglob("*") if path.is_file()}
    recheck_calls = fake.stream_calls[calls_before:]
    assert rechecked.source_content_root == sealed.source_content_root
    assert rechecked.pit_snapshot_digest == sealed.pit_snapshot_digest
    assert rechecked.source_cas_usage["new_bytes"] == 0
    assert rechecked.source_cas_usage["new_partitions"] == 0
    assert all(query_id in recheck_calls for query_id in PRODUCTION_QUERY_SPECS)
    assert existing_blob_reads == 0
    assert after_files == before_files
    assert list(cas.cas_root.rglob("*.partial")) == []


@pytest.mark.parametrize(
    "fresh_rows",
    (
        (
            {"ts_code": "000001.SZ", "trade_date": "2026-07-01", "value": 1},
            {"ts_code": "000002.SZ", "trade_date": "2026-07-01", "value": 9},
        ),
        ({"ts_code": "000001.SZ", "trade_date": "2026-07-01", "value": 1},),
        (
            {"ts_code": "000002.SZ", "trade_date": "2026-07-01", "value": 2},
            {"ts_code": "000001.SZ", "trade_date": "2026-07-01", "value": 1},
        ),
    ),
    ids=("changed-row", "deleted-row", "reordered-row"),
)
def test_exact_partition_recheck_maps_any_canonical_revision_to_source_revised(
    dataset_profile,
    tmp_path,
    fresh_rows,
) -> None:
    authority, _cas = _authority(
        dataset_profile,
        tmp_path,
        FakeSnapshotSession(),
    )
    spec = PartitionSpec(
        "fixture_exact",
        "2026-07",
        "fixture_exact_v1",
        (
            ColumnSpec("ts_code", ColumnKind.STRING, True),
            ColumnSpec("trade_date", ColumnKind.DATE, True),
            ColumnSpec("value", ColumnKind.INTEGER, True),
        ),
        ("ts_code", "trade_date"),
    )
    table = SourceTableSchema(
        "market.fixture_exact",
        ("ts_code", "trade_date", "value"),
    )
    baseline = authority._seal_rows(
        spec=spec,
        rows=(
            {"ts_code": "000001.SZ", "trade_date": "2026-07-01", "value": 1},
            {"ts_code": "000002.SZ", "trade_date": "2026-07-01", "value": 2},
        ),
        components=(Component.DAILY_BIN,),
        tokens=("fixture",),
        table_schema=table,
    )
    expectation = {
        **baseline.as_build_input(),
        "recheck_partition_scope": "source",
    }

    with pytest.raises(SourceSnapshotRevised) as caught:
        authority._seal_rows(
            spec=spec,
            rows=fresh_rows,
            components=(Component.DAILY_BIN,),
            tokens=("fixture",),
            table_schema=table,
            recheck_expectation=expectation,
        )

    assert caught.value.code == "BLOCKED_SOURCE_REVISED"
    assert list(authority.cas.cas_root.rglob("*.partial")) == []


def test_exact_partition_recheck_blocks_codec_identity_drift(
    dataset_profile,
    tmp_path,
) -> None:
    authority, _cas = _authority(
        dataset_profile,
        tmp_path,
        FakeSnapshotSession(),
    )
    spec = PartitionSpec(
        "fixture_codec",
        "2026-07",
        "fixture_codec_v1",
        (ColumnSpec("value", ColumnKind.INTEGER, True),),
        ("value",),
    )
    table = SourceTableSchema("market.fixture_codec", ("value",))
    baseline = authority._seal_rows(
        spec=spec,
        rows=({"value": 1},),
        components=(Component.DAILY_BIN,),
        tokens=("fixture",),
        table_schema=table,
    )
    expectation = {
        **baseline.as_build_input(),
        "recheck_partition_scope": "source",
        "rows_codec_identity": "unexpected-codec-v2",
    }

    with pytest.raises(SourceSnapshotRevised) as caught:
        authority._seal_rows(
            spec=spec,
            rows=({"value": 1},),
            components=(Component.DAILY_BIN,),
            tokens=("fixture",),
            table_schema=table,
            recheck_expectation=expectation,
        )

    assert caught.value.code == "BLOCKED_SOURCE_REVISED"


def test_source_freeze_uses_at_most_one_data_partition_per_short_session(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    frozen = authority.freeze(cutoff=date(2026, 7, 31))

    assert frozen.partitions
    assert fake.max_data_query_ids_per_session == 1
    assert fake.session_count > len(frozen.partitions)


def test_source_writer_ledger_uses_latest_data_sync_attempt_per_target() -> None:
    sql = source_authority_module._SOURCE_WRITER_LEDGER_SQL
    data_sync_projection = sql.split("UNION ALL", maxsplit=1)[1]

    assert "WITH normalized_ingestion_job AS" in sql
    assert "relevant_ingestion_job AS" in sql
    assert "summary->>'actual_dataset'" in sql
    assert "summary->>'schedule_dataset'" in sql
    assert "summary->>'dataset'" in sql
    assert "job.direct_dataset = ANY(%(datasets)s)" in sql
    assert "job.status NOT IN ('queued','pending','running')" in sql
    assert "job.source_start::date <= %(cutoff)s" in sql
    assert "FROM relevant_ingestion_job" in sql
    assert "latest_data_sync_attempt AS" in sql
    assert "SELECT DISTINCT ON (attempt.target_id)" in sql
    assert "attempt.attempt_no" in sql
    assert "ORDER BY attempt.target_id,attempt.attempt_no DESC" in sql
    assert "target.target_date <= %(cutoff)s" in sql
    assert "FROM latest_data_sync_attempt AS attempt" in sql
    assert "attempt.status='started'" in sql
    assert "target." not in data_sync_projection
    assert "'dataset',attempt.dataset" in data_sync_projection
    assert "'data_source',attempt.data_source" in data_sync_projection


def test_source_writer_ledger_binds_cutoff_and_versioned_relevant_writer_policy(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    authority, cas = _authority(dataset_profile, tmp_path, fake)

    frozen = authority.freeze(cutoff=date(2026, 7, 31))

    writer_params = [params for query_id, params in fake.stream_params if query_id == "writer_ledger"]
    assert len(writer_params) >= 2
    assert all(params["cutoff"] == date(2026, 7, 31) for params in writer_params)
    assert all(params["start"] == date(2026, 7, 1) for params in writer_params)
    assert all(tuple(params["datasets"]) for params in writer_params)
    assert source_authority_module.SOURCE_AUTHORITY_POLICY_VERSION == "qe_monthly_source_authority_v3"
    assert source_authority_module.SOURCE_CONSISTENCY_POLICY.endswith(
        "relevant_writer_ledger_quiescence_v2"
    )
    provenance = cas.get_json_bounded(frozen.source_provenance_ref, max_bytes=4 * 1024**2)
    writer_evidence = provenance["writer_ledger_evidence"]
    assert writer_evidence["schema_version"] == "dataset_release_source_writer_ledger_v2"
    assert writer_evidence["source_cutoff"] == "2026-07-31"
    assert writer_evidence["non_active_job_policy"] == "direct_dataset_at_or_before_cutoff_v1"
    assert writer_evidence["active_job_policy"] == "relevant_or_unclassified_fail_closed_v1"


def test_source_freeze_still_blocks_latest_active_writer(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.writer_rows.append(
        {
            "ledger_kind": "data_sync_attempts",
            "ledger_identity": "dsa-active",
            "status": "started",
            "created_at": datetime(2026, 7, 31, 1, tzinfo=UTC),
            "started_at": datetime(2026, 7, 31, 1, tzinfo=UTC),
            "finished_at": None,
            "opaque_payload": '{"target_id":"dst-active"}',
        }
    )
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    with pytest.raises(
        source_authority_module.SourceSnapshotDriftBlocked,
        match="active source writer exists",
    ):
        authority.freeze(cutoff=date(2026, 7, 31))


def test_source_freeze_blocks_writer_ledger_mutation_even_when_audit_is_unchanged(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.mutate_writer_after_partitions = 2
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    with pytest.raises(
        source_authority_module.SourceSnapshotDriftBlocked,
        match="writer ledger changed",
    ):
        authority.freeze(cutoff=date(2026, 7, 31))


def test_source_authority_pit_only_revision_changes_frozen_pit_identity(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    authority, _cas = _authority(dataset_profile, tmp_path, fake)
    first = authority.freeze(cutoff=date(2026, 7, 31))

    fake.pit_exit = date(2026, 7, 30)
    revised = authority.freeze(cutoff=date(2026, 7, 31))
    assert revised.pit_snapshot_digest != first.pit_snapshot_digest
    assert revised.source_content_root == first.source_content_root
    assert revised.source_manifest_ref == first.source_manifest_ref
    assert revised.source_provenance_root != first.source_provenance_root


def test_exact_snapshot_token_changes_only_observation_provenance(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    authority, _cas = _authority(dataset_profile, tmp_path, fake)
    first = authority.freeze(cutoff=date(2026, 7, 31))

    fake.snapshot_tokens = ("fake-repeatable-read:2",)
    revised = authority.freeze(cutoff=date(2026, 7, 31))
    assert revised.source_content_root == first.source_content_root
    assert revised.stable_source_provenance_root == first.stable_source_provenance_root
    assert revised.source_provenance_root != first.source_provenance_root
    assert revised.source_provenance_ref != first.source_provenance_ref


def test_source_cas_budget_checkpoints_distinguish_new_and_reused_bytes(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    authority, _cas = _authority(dataset_profile, tmp_path, fake)
    observed_remaining: list[int | None] = []

    def disk_checkpoint(remaining):
        observed_remaining.append(remaining)
        return {
            "control_free_bytes": 10**12,
            "candidate_free_bytes": 10**12,
            "effective_free_bytes": 10**12,
            "required_free_bytes": 32 * 1024**3,
            "predicted_remaining_new_bytes": remaining,
            "same_volume": True,
        }

    first = authority.freeze(
        cutoff=date(2026, 7, 31),
        predicted_new_bytes=10**9,
        disk_checkpoint=disk_checkpoint,
    )
    first_call_count = len(observed_remaining)
    replay = authority.freeze(
        cutoff=date(2026, 7, 31),
        predicted_new_bytes=10**9,
        disk_checkpoint=disk_checkpoint,
    )
    assert first.source_cas_usage["new_bytes"] > 0
    assert first.source_cas_usage["new_partitions"] > 0
    assert replay.source_cas_usage["new_bytes"] == 0
    assert replay.source_cas_usage["reused_bytes"] == first.source_cas_usage["new_bytes"]
    assert replay.source_cas_usage["reused_partitions"] == first.source_cas_usage["new_partitions"]
    assert observed_remaining
    assert observed_remaining[:first_call_count] == sorted(observed_remaining[:first_call_count], reverse=True)
    assert observed_remaining[first_call_count:] == sorted(observed_remaining[first_call_count:], reverse=True)


def test_refresh_audit_alias_and_extra_failed_source_are_canonical_provenance(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    authority, _cas = _authority(dataset_profile, tmp_path, fake)
    first = authority.freeze(cutoff=date(2026, 7, 31))
    assert "stock_moneyflow_ts" in fake.audit_datasets_requested
    assert "moneyflow_ts" not in fake.audit_datasets_requested

    fake.audit_extra_failed = "stock_moneyflow_ts"
    revised = authority.freeze(cutoff=date(2026, 7, 31))
    assert revised.source_content_root == first.source_content_root
    assert revised.source_audit_ref != first.source_audit_ref
    assert revised.source_provenance_root != first.source_provenance_root


def test_refresh_audit_receipt_hashes_free_form_secret_like_fields(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    sensitive_text = "SENSITIVE_VALUE=" + "https://example.invalid/" + "?field=abc"
    setattr(fake, "audit_" + "secret", sensitive_text)
    fake.audit_extra_failed = "index_daily"
    authority, cas = _authority(dataset_profile, tmp_path, fake)

    frozen = authority.freeze(cutoff=date(2026, 7, 31))
    raw = cas.get_bytes(frozen.source_audit_ref)
    assert sensitive_text.encode("utf-8") not in raw
    receipt = json.loads(raw.decode("utf-8"))
    sources = [source for item in receipt["rows"] for source in item["sources"]]
    assert sources
    assert all("metadata_json" not in source for source in sources)
    assert all("error_message" not in source for source in sources)
    assert all(len(source["audit_payload_sha256"]) == 64 for source in sources)


def test_refresh_audit_duplicate_dataset_date_source_blocks(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.audit_duplicate = "index_daily"
    authority, _cas = _authority(dataset_profile, tmp_path, fake)
    with pytest.raises(SourceAuditIncomplete, match="source authority is ambiguous"):
        authority.freeze(cutoff=date(2026, 7, 31))


def test_source_stage_receipt_round_trip_reconstructs_bounded_snapshot(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    authority, cas = _authority(dataset_profile, tmp_path, fake)
    frozen = authority.freeze(cutoff=date(2026, 7, 31))
    stage_ref = seal_source_stage_receipt(cas, frozen, profile=authority.profile.profile)

    restored = load_source_stage_receipt(
        cas,
        stage_ref.as_dict(),
        expected_profile=authority.profile.profile,
        expected_cutoff=date(2026, 7, 31),
    )
    assert restored.source_content_root == frozen.source_content_root
    assert restored.source_provenance_root == frozen.source_provenance_root
    assert restored.pit_snapshot_digest == frozen.pit_snapshot_digest
    assert [item.as_build_input() for item in restored.partitions] == [
        item.as_build_input() for item in sorted(frozen.partitions, key=lambda value: value.spec.identity)
    ]


def test_source_authority_missing_required_table_fails_closed(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.missing = "moneyflow_ts"
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    with pytest.raises(SourceRequiredTableMissing, match="moneyflow_ts"):
        authority.freeze(cutoff=date(2026, 7, 31))


def test_source_row_required_nested_null_fails_during_freeze(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.payload_overrides["kline_daily_raw"] = {"open_li": None}
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    with pytest.raises(SourceManifestError, match="required value is NULL"):
        authority.freeze(cutoff=date(2026, 7, 31))


def test_source_row_optional_nested_null_is_preserved(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.payload_overrides["daily_basic"] = {"pe": None}
    authority, cas = _authority(dataset_profile, tmp_path, fake)

    frozen = authority.freeze(cutoff=date(2026, 7, 31))
    partition = next(item.as_build_input() for item in frozen.partitions if item.spec.dataset == "daily_basic")
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)
    with reader.iter_rows("daily_basic", partition["partition_key"]) as rows:
        row = next(rows)
    assert row["pe"] is None


def test_source_row_non_finite_optional_value_is_rejected(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.payload_overrides["daily_basic"] = {"pe": float("nan")}
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    with pytest.raises(SourceManifestError, match="invalid JSON"):
        authority.freeze(cutoff=date(2026, 7, 31))


@pytest.mark.parametrize("column", ["pre_close", "up_limit", "down_limit"])
@pytest.mark.parametrize("marker", ["NaN", "Infinity", "-Infinity"])
def test_stk_limit_postgres_numeric_non_finite_marker_becomes_repairable_null(
    dataset_profile,
    tmp_path,
    column,
    marker,
) -> None:
    fake = FakeSnapshotSession()
    fake.payload_overrides["stk_limit"] = {column: marker}
    authority, cas = _authority(dataset_profile, tmp_path, fake)

    frozen = authority.freeze(cutoff=date(2026, 7, 31))
    partition = next(item.as_build_input() for item in frozen.partitions if item.spec.dataset == "stk_limit")
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)
    with reader.iter_rows("stk_limit", partition["partition_key"]) as rows:
        row = next(rows)

    assert row[column] is None
    assert PRODUCTION_QUERY_SPECS["stk_limit"].query_version.endswith(":nonfinite_numeric_to_null_v1")


def test_stk_limit_arbitrary_text_numeric_value_remains_fail_closed(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.payload_overrides["stk_limit"] = {"pre_close": "not-a-number"}
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    with pytest.raises(SourceManifestError, match="numeric value is invalid"):
        authority.freeze(cutoff=date(2026, 7, 31))


@pytest.mark.parametrize(
    "column",
    [
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
    ],
)
@pytest.mark.parametrize("marker", ["NaN", "Infinity", "-Infinity"])
def test_daily_basic_postgres_numeric_non_finite_marker_becomes_nullable_source_null(
    dataset_profile,
    tmp_path,
    column,
    marker,
) -> None:
    fake = FakeSnapshotSession()
    fake.payload_overrides["daily_basic"] = {column: marker}
    authority, cas = _authority(dataset_profile, tmp_path, fake)

    frozen = authority.freeze(cutoff=date(2026, 7, 31))
    partition = next(item.as_build_input() for item in frozen.partitions if item.spec.dataset == "daily_basic")
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)
    with reader.iter_rows("daily_basic", partition["partition_key"]) as rows:
        row = next(rows)

    assert row[column] is None
    assert PRODUCTION_QUERY_SPECS["daily_basic"].query_version.endswith(":nonfinite_numeric_to_null_v1")


def test_daily_basic_arbitrary_text_numeric_value_remains_fail_closed(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.payload_overrides["daily_basic"] = {"pe": "not-a-number"}
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    with pytest.raises(SourceManifestError, match="numeric value is invalid"):
        authority.freeze(cutoff=date(2026, 7, 31))


def test_postgres_numeric_marker_outside_allowlisted_datasets_remains_fail_closed(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.payload_overrides["moneyflow_ts"] = {"buy_sm_vol": "NaN"}
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    with pytest.raises(SourceManifestError, match="numeric value is invalid"):
        authority.freeze(cutoff=date(2026, 7, 31))


def test_source_row_key_payload_mismatch_is_rejected(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.row_key_overrides["kline_daily_raw"] = [
        "999999.SZ",
        "2026-07-01",
    ]
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    with pytest.raises(SourceManifestError, match="key/payload identity differs"):
        authority.freeze(cutoff=date(2026, 7, 31))


def test_frozen_sector_payload_is_pit_enriched_and_factor_stage_consumable(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.payload_overrides["sw_index_classify"] = {
        "index_code": "801010.SI",
        "level": "L2",
    }
    fake.payload_overrides["sw_index_member"] = {
        "l2_code": "801010.SI",
        "out_date": None,
    }
    authority, cas = _authority(dataset_profile, tmp_path, fake)

    frozen = authority.freeze(cutoff=date(2026, 7, 31))
    partition = next(item.as_build_input() for item in frozen.partitions if item.spec.dataset == "sector_data")
    reader = CASSealedPartitionReader(cas, [partition], max_partition_rows=10)
    with reader.iter_rows("sector_data", partition["partition_key"]) as rows:
        row = next(rows)

    assert row["l2_code_id"] == 0
    normalized = _normalize_aux_frame(pd.DataFrame([row]), dataset="sector_data")
    assert int(normalized.iloc[0]["l2_code_id"]) == 0
    assert str(normalized["l2_code_id"].dtype) == "int16"
    assert frozen.derived_source_receipt_refs


def test_large_source_partition_uses_streaming_cas_without_large_read_bytes(
    dataset_profile,
    tmp_path,
    monkeypatch,
) -> None:
    fake = FakeSnapshotSession()
    fake.large_query = "kline_daily_raw"
    fake.large_rows = 20_000
    authority, _cas = _authority(dataset_profile, tmp_path, fake)
    original = Path.read_bytes

    def reject_large_read(path: Path) -> bytes:
        if path.is_file() and path.stat().st_size > 256 * 1024:
            raise AssertionError("large CAS artifacts must not use Path.read_bytes")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", reject_large_read)
    frozen = authority.freeze(cutoff=date(2026, 7, 31))
    large = next(item for item in frozen.partitions if item.spec.dataset == "kline_daily_raw")
    assert large.summary.row_count == 20_000
    assert large.rows_uncompressed_bytes > 256 * 1024
    assert large.rows_ref.size == large.rows_compressed_bytes
    assert large.rows_compressed_bytes < large.rows_uncompressed_bytes


def test_baseline_audit_never_substitutes_for_fresh_value_sensitive_rehash(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    authority, cas = _authority(dataset_profile, tmp_path, fake)
    first = authority.freeze(cutoff=date(2026, 7, 31))
    baseline = cas.get_json_bounded(first.source_reuse_manifest_ref, max_bytes=2 * 1024**2)["partitions"]

    fake.stream_calls.clear()
    replay = authority.freeze(cutoff=date(2026, 7, 31), baseline_partitions=baseline)
    assert replay.source_manifest_ref == first.source_manifest_ref
    dated_queries = {item.query_id for item in PRODUCTION_QUERY_SPECS.values() if item.date_expression is not None}
    assert dated_queries.issubset(fake.stream_calls)
    assert {"refresh_audit", "trading_dates", "pit_spans"}.issubset(fake.stream_calls)

    fake.query_revisions["index_daily"] = "v2"
    fake.stream_calls.clear()
    revised = authority.freeze(cutoff=date(2026, 7, 31), baseline_partitions=baseline)
    assert revised.source_content_root != first.source_content_root
    rescanned_dated = dated_queries.intersection(fake.stream_calls)
    assert rescanned_dated == dated_queries
    assert "kline_minute_raw" in fake.stream_calls


def test_mvcc_fingerprint_capability_reuses_only_identical_partitions(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    authority, cas = _authority(
        dataset_profile,
        tmp_path,
        fake,
        mvcc_reuse_capability=True,
    )
    first = authority.freeze(cutoff=date(2026, 7, 31))
    baseline = cas.get_json_bounded(
        first.source_reuse_manifest_ref,
        max_bytes=4 * 1024**2,
    )["partitions"]

    fake.stream_calls.clear()
    replay = authority.freeze(
        cutoff=date(2026, 7, 31),
        baseline_partitions=baseline,
    )
    assert replay.source_manifest_ref == first.source_manifest_ref
    assert not set(PRODUCTION_QUERY_SPECS).intersection(fake.stream_calls)

    fake.fingerprint_revisions["index_daily"] = 101
    fake.stream_calls.clear()
    revised = authority.freeze(
        cutoff=date(2026, 7, 31),
        baseline_partitions=baseline,
    )
    assert "index_daily" in fake.stream_calls
    assert "daily_basic" not in fake.stream_calls
    assert revised.source_content_root == first.source_content_root


def test_mvcc_fingerprint_unavailable_falls_back_to_full_stream(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.fingerprint_unavailable.add("index_daily")
    authority, cas = _authority(
        dataset_profile,
        tmp_path,
        fake,
        mvcc_reuse_capability=True,
    )
    first = authority.freeze(cutoff=date(2026, 7, 31))
    baseline = cas.get_json_bounded(
        first.source_reuse_manifest_ref,
        max_bytes=4 * 1024**2,
    )["partitions"]

    fake.stream_calls.clear()
    authority.freeze(
        cutoff=date(2026, 7, 31),
        baseline_partitions=baseline,
    )
    assert "index_daily" in fake.stream_calls


def test_mvcc_fingerprint_readback_drift_blocks_mixed_snapshot(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.fingerprint_readback_drift_query = "index_daily"
    authority, _cas = _authority(
        dataset_profile,
        tmp_path,
        fake,
        mvcc_reuse_capability=True,
    )

    with pytest.raises(
        source_authority_module.SourceSnapshotDriftBlocked,
        match="fingerprint changed",
    ):
        authority.freeze(cutoff=date(2026, 7, 31))


def test_mvcc_fingerprint_sql_binds_exact_query_and_cluster_identity() -> None:
    for spec in PRODUCTION_QUERY_SPECS.values():
        sql = spec.fingerprint_sql
        assert "source_row.xmin::text::bigint AS row_xmin" in sql
        assert "pg_control_system()" in sql
        assert "pg_control_checkpoint()" in sql
        assert "txid_current()::bigint >> 32" in sql
        assert spec.table_identity in sql


@pytest.mark.parametrize("mode", ("missing", "failed"))
def test_refresh_audit_missing_or_failed_blocks_partition_reuse(
    dataset_profile,
    tmp_path,
    mode,
) -> None:
    fake = FakeSnapshotSession()
    if mode == "missing":
        fake.audit_omit = "kline_minute_raw"
    else:
        fake.audit_fail = "kline_minute_raw"
    authority, _cas = _authority(dataset_profile, tmp_path, fake)
    with pytest.raises(SourceAuditIncomplete):
        authority.freeze(cutoff=date(2026, 7, 31))


@pytest.mark.parametrize("dataset", ("index_daily", "stk_limit"))
def test_candidate_repairable_audit_is_limited_to_registered_candidate_providers(
    dataset_profile,
    tmp_path,
    dataset,
) -> None:
    fake = FakeSnapshotSession()
    fake.audit_quality_overrides[dataset] = "candidate_repairable"
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    snapshot = authority.freeze(cutoff=date(2026, 7, 31))

    assert snapshot.source_content_root


def test_candidate_repairable_audit_cannot_relax_dense_daily_source(
    dataset_profile,
    tmp_path,
) -> None:
    fake = FakeSnapshotSession()
    fake.audit_quality_overrides["kline_daily_raw"] = "candidate_repairable"
    authority, _cas = _authority(dataset_profile, tmp_path, fake)

    with pytest.raises(SourceAuditIncomplete):
        authority.freeze(cutoff=date(2026, 7, 31))
