from __future__ import annotations

from datetime import date
import json
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.contracts import Component
from backend.services.dataset_release.errors import SourceManifestError
from backend.services.dataset_release.profile import ResourcePolicy
from backend.services.dataset_release.source_authority import (
    PRODUCTION_QUERY_SPECS,
    PostgresSourceSnapshotSession,
    SourceRequiredDatasetEmpty,
    SourceAuditIncomplete,
    SourceQuerySpec,
    _validate_core_index_membership_authority,
    imported_source_session_factory,
)


@pytest.mark.parametrize("query_id", ["kline_daily_raw", "kline_minute_raw", "daily_basic"])
def test_raw_source_sealer_keeps_row_identity_and_emits_exact_month_leaves(tmp_path, query_id):
    import gzip
    from dataclasses import replace
    from backend.services.dataset_release.canonical import canonical_json_bytes
    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.source_authority import MonthlySourceAuthority, SourceTableSchema
    from backend.services.dataset_release.source_manifest import CanonicalPartitionHasher

    ControlStore.initialize(tmp_path)
    cas = CASStore(tmp_path)
    query = PRODUCTION_QUERY_SPECS[query_id]
    payloads = []
    for month in (7, 8, 9):
        payload = {"ts_code": "000001.SZ"}
        payload["trade_time" if query_id == "kline_minute_raw" else "trade_date"] = (
            f"2026-{month:02d}-01 09:31:00" if query_id == "kline_minute_raw" else f"2026-{month:02d}-01"
        )
        if query_id == "kline_minute_raw":
            payload["freq"] = "1min"
        payload.update({field: 1000 for field in query.value_columns})
        if query_id in {'kline_daily_raw', 'kline_minute_raw'}:
            payload.update(volume_shares_source='tushare_daily' if query_id == 'kline_daily_raw' else 'tushare_stk_mins',
                           volume_shares_sha256='a' * 64)
        payloads.append(payload)
    expected_rows = [
        {
            "row_key": canonical_json_bytes([p[field] for field in query.key_columns]).decode(),
            "row_payload": canonical_json_bytes(p).decode(),
        }
        for p in payloads
    ]

    class Session:
        def stream(self, *_args, **_kwargs):
            return iter(
                {"row_key": json.dumps([p[field] for field in query.key_columns]), "row_payload": json.dumps(p)}
                for p in payloads
            )

    profile = SimpleNamespace(
        resource_policy=ResourcePolicy(), pressure_ladder={"date_chunk_months": (3,), "minute_batch": (20,)}
    )
    authority = MonthlySourceAuthority(profile, cas, sector_source_policy="classification_published_snapshot_v1")
    partition = authority._seal_query_partition(
        Session(),
        query=query,
        partition_key="2026-07-01_2026-09-30",
        params={"start": date(2026, 7, 1), "end": date(2026, 9, 30)},
        tokens=(),
        table_schema=SourceTableSchema(query.table_identity, query.required_columns),
    )
    baseline_hasher = CanonicalPartitionHasher(
        partition.spec, ingestion_audit_identity=partition.summary.ingestion_audit_identity
    )
    for row in expected_rows:
        baseline_hasher.update(row)
    assert partition.summary == baseline_hasher.finish()
    path = tmp_path / partition.rows_ref.relative_path
    encoded = gzip.decompress(path.read_bytes()).splitlines()[1:]
    assert encoded == [canonical_json_bytes(row) for row in expected_rows]
    assert [leaf["month"] for leaf in partition.monthly_content_leaves] == ["2026-07", "2026-08", "2026-09"]
    for leaf, row in zip(partition.monthly_content_leaves, expected_rows):
        month_hasher = CanonicalPartitionHasher(
            replace(partition.spec, partition_key=leaf["month"]),
            ingestion_audit_identity=partition.summary.ingestion_audit_identity,
        )
        month_hasher.update(row)
        expected = month_hasher.finish()
        assert (leaf["row_count"], leaf["merkle_root"], leaf["content_digest"]) == (
            1,
            expected.merkle_root,
            expected.content_digest,
        )


@pytest.mark.parametrize("mismatch", [False, True])
def test_validated_text_keys_avoid_reencoding_without_weakening_identity(monkeypatch, mismatch):
    from backend.services.dataset_release import source_authority as source
    from backend.services.dataset_release.source_manifest import PartitionSpec, ColumnSpec, ColumnKind

    query = PRODUCTION_QUERY_SPECS["kline_minute_raw"]
    spec = PartitionSpec(
        query.query_id,
        "test",
        query.query_version,
        (ColumnSpec("row_key", ColumnKind.STRING, True), ColumnSpec("row_payload", ColumnKind.STRING, True)),
        ("row_key",),
    )
    payload = {
        "ts_code": "000001.SZ",
        "trade_time": "2026-09-30 09:31:00",
        "freq": "1min",
        **{field: 1000 for field in query.value_columns},
        'volume_shares_source': 'tushare_stk_mins',
        'volume_shares_sha256': 'a' * 64,
    }
    keys = [payload[field] for field in query.key_columns]
    if mismatch:
        keys[0] = "000002.SZ"
    calls = []
    original = source.canonical_json_bytes

    def counted(value):
        calls.append(value)
        return original(value)

    monkeypatch.setattr(source, "canonical_json_bytes", counted)
    row = {"row_key": json.dumps(keys), "row_payload": json.dumps(payload)}
    if mismatch:
        with pytest.raises(SourceManifestError, match="key/payload identity differs"):
            source._validate_query_row(row, query, spec)
    else:
        result = source._validate_query_row(row, query, spec)
        assert result == {"row_key": original(keys).decode(), "row_payload": original(payload).decode()}
    assert len(calls) == (0 if mismatch else 2)


@pytest.mark.parametrize("key,value", [(1, 1), (1, 1.0)])
def test_numeric_key_identity_still_distinguishes_integer_and_float(key, value):
    from dataclasses import replace
    from backend.services.dataset_release.source_authority import (
        _query_partition_spec,
        _validate_query_row,
        SourceTableSchema,
    )

    query = replace(PRODUCTION_QUERY_SPECS["kline_daily_raw"], key_columns=("close_li",))
    spec = _query_partition_spec(query, "numeric-key", SourceTableSchema(query.table_identity, query.required_columns))
    payload = {field: 1000 for field in query.value_columns}
    payload.update(volume_shares_source='tushare_daily', volume_shares_sha256='a' * 64)
    payload["close_li"] = value
    raw = {"row_key": json.dumps([key]), "row_payload": json.dumps(payload)}
    if isinstance(value, float):
        with pytest.raises(SourceManifestError, match="key/payload identity differs"):
            _validate_query_row(raw, query, spec)
    else:
        assert json.loads(_validate_query_row(raw, query, spec)["row_key"]) == [key]


@pytest.mark.parametrize(
    "fault", ["null_key", "numeric_text_key", "missing_field", "extra_field", "nonfinite", "null_required"]
)
def test_optimized_raw_validator_retains_fail_closed_payload_contract(fault):
    from backend.services.dataset_release.source_authority import (
        _query_partition_spec,
        _validate_query_row,
        SourceTableSchema,
    )

    query = PRODUCTION_QUERY_SPECS["kline_daily_raw"]
    spec = _query_partition_spec(query, "fail-closed", SourceTableSchema(query.table_identity, query.required_columns))
    payload = {"ts_code": "000001.SZ", "trade_date": "2026-09-30", **{field: 1000 for field in query.value_columns}}
    payload.update(volume_shares_source='tushare_daily', volume_shares_sha256='a' * 64)
    if fault == "null_key":
        payload["ts_code"] = None
    elif fault == "numeric_text_key":
        payload["ts_code"] = 1
    elif fault == "missing_field":
        del payload["close_li"]
    elif fault == "extra_field":
        payload["unexpected"] = 1
    elif fault == "nonfinite":
        payload["close_li"] = float("nan")
    else:
        payload["close_li"] = None
    raw = {"row_key": json.dumps([payload[field] for field in query.key_columns]), "row_payload": json.dumps(payload)}
    with pytest.raises(SourceManifestError):
        _validate_query_row(raw, query, spec)


def test_monthly_sector_source_policy_is_explicit_and_preserves_legacy_p3a() -> None:
    from backend.services.dataset_release.source_authority import MonthlySourceAuthority

    profile = SimpleNamespace(profile="qe_hmm_full_v2")
    legacy = MonthlySourceAuthority(profile, None)
    monthly = MonthlySourceAuthority(profile, None, sector_source_policy="classification_published_snapshot_v1")
    assert legacy.uses_p3a_sector_source is True
    assert monthly.uses_p3a_sector_source is False
    assert "sector_data" not in {q.query_id for q in legacy._database_query_specs()}
    assert "sector_data" in {q.query_id for q in monthly._database_query_specs()}
    assert monthly.profile is profile
    with pytest.raises(ValueError, match="sector source policy"):
        MonthlySourceAuthority(profile, None, sector_source_policy="fallback_if_missing")


def test_native_sector_receipt_keeps_exact_classification_and_partition_identity() -> None:
    from backend.services.dataset_release.source_authority import (
        _validate_monthly_sector_publication_receipt,
    )
    from backend.services.dataset_release.sector_enrichment import FrozenSectorEnricher

    classify = [{"identity": "sw_index_classify:timeless", "content_digest": "a" * 64}]
    member = [{"identity": "sw_index_member:timeless", "content_digest": "b" * 64}]
    enricher = FrozenSectorEnricher.build(
        [{"index_code": f"801{value:03d}.SI", "level": "L2"} for value in range(131)], []
    )
    receipt = {
        **enricher.receipt(classify_partitions=classify, member_partitions=member),
        "schema_version": "dataset_release_monthly_sector_publication_v1",
        "publication_policy": "classification_published_snapshot_v1",
        "profile": "qe_hmm_full_v2",
        "cutoff": "2026-09-30",
    }
    arguments = dict(
        expected_profile="qe_hmm_full_v2",
        expected_cutoff=date(2026, 9, 30),
        classify_partitions=classify,
        member_partitions=member,
    )
    _validate_monthly_sector_publication_receipt(receipt, **arguments)
    for field, wrong in (
        ("cutoff", "2026-08-31"),
        ("code_count", 130),
        ("publication_policy", "silent_fallback"),
        ("member_partitions", []),
    ):
        with pytest.raises(SourceAuditIncomplete, match="sector publication"):
            _validate_monthly_sector_publication_receipt({**receipt, field: wrong}, **arguments)

    for field, wrong in (
        ("code_map_digest", "bad"),
        ("membership_digest", "bad"),
        ("mapping_policy", "model_private_ids"),
        ("profile", "other"),
        ("unexpected", "extra"),
    ):
        with pytest.raises(SourceAuditIncomplete, match="sector publication"):
            _validate_monthly_sector_publication_receipt({**receipt, field: wrong}, **arguments)


def test_production_source_allowlist_preserves_exact_daily_and_minute_ordering() -> None:
    daily = PRODUCTION_QUERY_SPECS["kline_daily_raw"]
    minute = PRODUCTION_QUERY_SPECS["kline_minute_raw"]

    assert daily.key_columns == ("ts_code", "trade_date")
    assert daily.sql.endswith("ORDER BY row_key, row_payload")
    assert minute.key_columns == ("ts_code", "trade_time", "freq")
    assert minute.date_range_policy == "timestamp_day_half_open"
    assert "source_row.trade_time < (%(end)s::date + interval '1 day')" in minute.sql
    assert minute.sql.endswith("ORDER BY source_row.ts_code,source_row.trade_time,source_row.freq")
    assert "to_jsonb(source_row)" not in daily.sql + minute.sql


@pytest.mark.parametrize("query_id", ["margin_detail", "daily_basic", "kline_daily_raw", "kline_minute_raw"])
def test_margin_publication_scope_is_independent_of_pit_stock_queries(dataset_profile, query_id):
    from backend.services.dataset_release.monthly_source_progress import MonthlyObservedSourceAuthority

    authority = MonthlyObservedSourceAuthority(
        dataset_profile, None, progress=lambda _: None, month_start=date(2026, 9, 1),
    )
    pit = SimpleNamespace(spans=[SimpleNamespace(ts_code="000001.SZ")])
    query = PRODUCTION_QUERY_SPECS[query_id]
    requests = list(authority._partition_requests(query, date(2026, 9, 30), pit_snapshot=pit))
    assert requests and all(params["start"] == date(2026, 9, 1) for _, params in requests)
    if query_id == "margin_detail":
        assert query.code_column is query.code_policy is None
        assert "%(codes)s" not in query.sql
        assert "provider_publication_scope_v1" in query.query_version
        assert all("codes" not in params for _, params in requests)
    else:
        assert "%(codes)s" in query.sql
        assert all(params["codes"] == ["000001.SZ"] for _, params in requests)
    assert [span.ts_code for span in pit.spans] == ["000001.SZ"]


def test_margin_publication_sql_in_existing_readonly_dev():
    import os

    if os.getenv("AISTOCK_MONTHLY_MARGIN_DEV_READBACK") != "1":
        pytest.skip("explicit existing DEV read-only validation only")
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from backend.services.dataset_release.monthly_frozen_source_audit import GateCounter, audit_margin_publication

    target = {key: os.environ[f"TDX_DB_DEV_{env}"] for key, env in (
        ("host", "HOST"), ("port", "PORT"), ("dbname", "NAME"), ("user", "USER"), ("password", "PASSWORD"),
    )}
    assert target["port"] == "5433" and "dev" in target["dbname"].lower()
    query = PRODUCTION_QUERY_SPECS["margin_detail"]
    day = date(2026, 9, 1)
    provider = [dict(ts_code=code, trade_date=day.isoformat(), **dict.fromkeys(query.value_columns, 1.0))
                for code in ("000001.SZ", "000002.SZ", "510050.SH")]
    columns = "ts_code text, trade_date date," + ",".join(f"{name} numeric" for name in query.value_columns)
    sql = (f"WITH provider_rows AS (SELECT * FROM jsonb_to_recordset(%(provider_rows)s::jsonb) AS fact({columns})) "
           + query.sql.replace(query.table_identity, "provider_rows"))
    with psycopg2.connect(**target, application_name="BUG-1818-readonly-DEV") as connection:
        connection.set_session(readonly=True, autocommit=False)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SHOW transaction_read_only")
            assert cursor.fetchone()["transaction_read_only"] == "on"
            cursor.execute(sql, dict(provider_rows=json.dumps(provider), codes=["000001.SZ"], start=day, end=day))
            actual = [json.loads(row["row_payload"]) for row in cursor.fetchall()]
    assert {row["ts_code"] for row in actual} == {row["ts_code"] for row in provider}
    receipt = {"eligible_sources": {"margin_detail": ["tushare"]},
               "eligible_quality_statuses": {"margin_detail": ["ok"]},
               "rows": [{"dataset": "margin_detail", "trade_date": day.isoformat(), "sources": [{
                   "data_source": "tushare", "status": "success", "error_present": False,
                   "quality_status": "ok", "expected_rows": len(provider),
               }]}]}
    gate = GateCounter("financial_moneyflow")
    audit_margin_publication(day=day, rows=actual, receipt=receipt, gate=gate)
    assert gate.status == "PASS"


def test_dated_source_query_cannot_bypass_refresh_audit_contract() -> None:
    with pytest.raises(ValueError, match="refresh-audit dataset"):
        SourceQuerySpec(
            query_id="fixture_daily",
            schema_name="market",
            table_name="fixture_daily",
            components=(Component.DAILY_BIN,),
            key_columns=("ts_code", "trade_date"),
            value_columns=("close",),
            derived_value_columns=(),
            required_columns=("ts_code", "trade_date", "close"),
            non_null_value_columns=("close",),
            date_expression="source_row.trade_date",
            start_policy="daily",
            query_version="fixture_v1",
        )


def test_imported_postgres_snapshot_identity_is_strict() -> None:
    with pytest.raises(ValueError, match="snapshot identity"):
        PostgresSourceSnapshotSession(
            ResourcePolicy(),
            imported_snapshot_id="x'; SELECT pg_sleep(1); --",
        )

    with pytest.raises(ValueError, match="snapshot identity"):
        imported_source_session_factory("not-a-snapshot")


def test_imported_source_factory_binds_every_session_to_same_snapshot() -> None:
    connections = []

    def connection_factory():
        value = object()
        connections.append(value)
        return value

    factory = imported_source_session_factory(
        "00000003-0000001B-1",
        connection_factory=connection_factory,
    )

    first = factory(ResourcePolicy())
    second = factory(ResourcePolicy())

    assert first is not second
    assert first._imported_snapshot_id == second._imported_snapshot_id == "00000003-0000001B-1"


def _core_index_rows() -> list[dict[str, object]]:
    identities = (
        ("csi300", "000300.SH", "CSI", "2018-08-01"),
        ("csi500", "000905.SH", "CSI", "2018-08-01"),
        ("csi1000", "000852.SH", "CSI", "2018-08-01"),
        ("star50", "000688.SH", "SSE", "2020-07-22"),
        ("star100", "000698.SH", "SSE", "2023-08-07"),
    )
    return [
        {
            "pool_id": pool_id,
            "index_code": index_code,
            "ts_code": f"{index + 1:06d}.SZ",
            "effective_from": effective_from,
            "effective_to_exclusive": None,
            "source_provider": provider,
            "source_reference": f"official:{pool_id}",
            "updated_at": "2026-09-30T18:00:00+08:00",
        }
        for index, (pool_id, index_code, provider, effective_from) in enumerate(identities)
    ]


def test_core_index_membership_source_query_is_exact_five_pool_overlap_authority() -> None:
    query = PRODUCTION_QUERY_SPECS["index_membership_pit"]

    assert query.table_identity == "market.core_index_membership_pit"
    assert query.start_policy == "window_overlap"
    assert query.key_columns == ("pool_id", "ts_code", "effective_from")
    assert "source_row.effective_from <= %(end)s" in query.sql
    assert "source_row.effective_to_exclusive > %(start)s" in query.sql
    assert all(pool_id in query.sql for pool_id in ("csi300", "csi500", "csi1000", "star50", "star100"))


@pytest.mark.parametrize("pressure_rung", [0, 2])
@pytest.mark.parametrize("duplicate", [False, True])
def test_overlap_source_seals_long_interval_once_and_rejects_real_duplicate(tmp_path, pressure_rung, duplicate):
    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.source_authority import MonthlySourceAuthority, SourceTableSchema

    ControlStore.initialize(tmp_path)
    query = PRODUCTION_QUERY_SPECS["index_membership_pit"]
    payload = {
        "pool_id": "csi1000", "index_code": "000852.SH", "ts_code": "000006.SZ",
        "effective_from": "2020-06-15", "effective_to_exclusive": "2024-06-17",
        "source_provider": "CSI", "source_reference": "CSI:announcement:11429",
        "updated_at": "2026-09-05T05:22:35.798704+08:00",
    }
    params = {"start": date(2018, 8, 1), "end": date(2026, 9, 30)}
    calls = []

    class Session:
        def stream(self, query_id, values, *, fetch_rows):
            calls.append((query_id, values, fetch_rows))
            if date.fromisoformat(payload["effective_from"]) <= values["end"] and (
                date.fromisoformat(payload["effective_to_exclusive"]) > values["start"]
            ):
                row = {"row_key": json.dumps([payload[key] for key in query.key_columns]),
                       "row_payload": json.dumps(payload)}
                yield row
                if duplicate:
                    yield row

    profile = SimpleNamespace(
        resource_policy=ResourcePolicy(),
        pressure_ladder={"date_chunk_months": (3, 2, 1), "minute_batch": (20, 10, 5)},
    )
    authority = MonthlySourceAuthority(profile, CASStore(tmp_path), sector_source_policy="classification_published_snapshot_v1")

    def seal():
        return authority._seal_query_partition(
            Session(), query=query, partition_key="2018-08-01_2026-09-30", params=params,
            tokens=(), table_schema=SourceTableSchema(query.table_identity, query.required_columns),
            pressure_rung=pressure_rung, read_chunk_rows=7,
        )

    if duplicate:
        with pytest.raises(SourceManifestError, match="duplicate partition primary key"):
            seal()
    else:
        partition = seal()
        assert partition.summary.row_count == 1
        assert partition.summary.duplicate_count == 0
        assert calls == [(query.query_id, params, 7)]


def test_point_date_source_still_splits_date_and_code_batches_under_pressure():
    from backend.services.dataset_release.source_authority import MonthlySourceAuthority

    authority = object.__new__(MonthlySourceAuthority)
    authority.profile = SimpleNamespace(pressure_ladder={"date_chunk_months": (3, 1), "minute_batch": (20, 2)})
    params = {"start": date(2026, 7, 1), "end": date(2026, 9, 30), "codes": ["000001.SZ", "000002.SZ", "000003.SZ"]}
    chunks = authority._execution_query_params(PRODUCTION_QUERY_SPECS["kline_minute_raw"], params, pressure_rung=1)
    assert [(item["start"], item["end"]) for item in chunks] == [
        (date(2026, 7, 1), date(2026, 7, 31)), (date(2026, 7, 1), date(2026, 7, 31)),
        (date(2026, 8, 1), date(2026, 8, 31)), (date(2026, 8, 1), date(2026, 8, 31)),
        (date(2026, 9, 1), date(2026, 9, 30)), (date(2026, 9, 1), date(2026, 9, 30)),
    ]
    assert [item["codes"] for item in chunks] == [params["codes"][:2], params["codes"][2:]] * 3


def test_core_index_membership_authority_closes_exact_pool_catalog_and_intervals() -> None:
    receipt = _validate_core_index_membership_authority(
        _core_index_rows(),
        start=date(2018, 8, 1),
        cutoff=date(2026, 9, 30),
    )

    assert receipt["schema_version"] == "dataset_release_core_index_membership_source_receipt_v1"
    assert receipt["pool_count"] == 5
    assert receipt["row_count"] == 5
    assert receipt["duplicate_count"] == receipt["overlap_count"] == 0
    assert receipt["database_write_performed"] is False


def test_core_index_membership_authority_rejects_overlap_and_missing_pool() -> None:
    rows = _core_index_rows()
    overlapping = dict(rows[0])
    overlapping["effective_from"] = "2020-01-01"
    rows[0]["effective_to_exclusive"] = None
    rows.append(overlapping)
    with pytest.raises(SourceManifestError, match="overlap"):
        _validate_core_index_membership_authority(
            rows,
            start=date(2018, 8, 1),
            cutoff=date(2026, 9, 30),
        )

    with pytest.raises(SourceRequiredDatasetEmpty, match="pool is empty"):
        _validate_core_index_membership_authority(
            _core_index_rows()[:-1],
            start=date(2018, 8, 1),
            cutoff=date(2026, 9, 30),
        )


def test_core_index_stage_receipt_closes_source_pins_and_pool_counts() -> None:
    from copy import deepcopy
    from backend.services.dataset_release.cas_store import CASRef
    from backend.services.dataset_release.source_authority import _validate_core_index_stage_receipt

    partition = SimpleNamespace(
        spec=SimpleNamespace(dataset="index_membership_pit", identity="index_membership_pit:window"),
        summary=SimpleNamespace(content_digest="a" * 64, row_count=5),
        rows_ref=CASRef("b" * 64, 10, "cas/sha256/bb/" + "b" * 64),
    )
    receipt = {
        **_validate_core_index_membership_authority(
            _core_index_rows(), start=date(2018, 8, 1), cutoff=date(2026, 9, 30)
        ),
        "source_partitions": [
            {
                "identity": partition.spec.identity,
                "content_digest": partition.summary.content_digest,
                "row_count": 5,
                "rows_ref": partition.rows_ref.as_dict(),
            }
        ],
    }
    arguments = dict(partitions=[partition], scope_start=date(2018, 8, 1), expected_cutoff=date(2026, 9, 30))
    _validate_core_index_stage_receipt(receipt, **arguments)
    for field, wrong in (
        ("source_partitions", []),
        ("row_count", 6),
        ("pool_count", True),
        ("authority_digest", "bad"),
        ("database_write_performed", True),
        ("overlap_count", 1),
        ("unknown_pool_count", 1),
        ("window", {"start": "2018-08-01", "cutoff": "2026-08-31"}),
    ):
        with pytest.raises(SourceAuditIncomplete, match="core-index"):
            _validate_core_index_stage_receipt({**receipt, field: wrong}, **arguments)
    broken = deepcopy(receipt)
    broken["coverage"]["csi300"]["source_provider"] = "untrusted"
    with pytest.raises(SourceAuditIncomplete, match="core-index"):
        _validate_core_index_stage_receipt(broken, **arguments)
    broken = deepcopy(receipt)
    broken["coverage"]["csi300"]["first_effective_from"] = "2018-08-02"
    with pytest.raises(SourceAuditIncomplete, match="core-index"):
        _validate_core_index_stage_receipt(broken, **arguments)
