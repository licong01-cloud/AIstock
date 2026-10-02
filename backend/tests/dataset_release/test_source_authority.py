from __future__ import annotations

from datetime import date
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
