from datetime import date
from dataclasses import replace
from types import SimpleNamespace

import pytest

from backend.services.dataset_release import monthly_preparation_artifacts as preparation
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.contracts import Component
from backend.services.dataset_release.monthly_component_preparation import ComponentPreparationError
from backend.services.dataset_release.monthly_preparation_source import PreparationSourceSnapshot
from backend.services.dataset_release.monthly_unified import SOURCE_GATES


def fixture(tmp_path):
    ControlStore.initialize(tmp_path)
    cas = CASStore(tmp_path)
    ref = cas.put_json({"private_source": True})
    cutoff = date(2026, 9, 30)
    snapshot = PreparationSourceSnapshot(
        f"dmr_{'1' * 32}",
        cutoff,
        SimpleNamespace(cutoff=cutoff, spans_sha256="a" * 64),
        ref,
        ref,
        ref,
        tuple(
            SimpleNamespace(spec=SimpleNamespace(dataset=name))
            for name in ("trading_calendar", "index_daily", "index_membership_pit")
        ),
        (),
        ("postgres:1-ABC-1",),
        ("margin_detail",),
        "a" * 64,
        SimpleNamespace(source_content_root="b" * 64),
    )
    audit = {
        "schema_version": "aistock_monthly_preparation_source_audit_v1",
        "operation_id": snapshot.operation_id,
        "cutoff": cutoff.isoformat(),
        "audit_start": "2026-09-01",
        "source_manifest_ref": ref.as_dict(),
        "gates": [
            {
                "gate_id": gate,
                "snapshot_group_id": "postgres:1-ABC-1",
                "expected_count": 1,
                "observed_count": 0 if gate == "financial_moneyflow" else 1,
                "explained_missing_count": 0,
                "unexplained_missing_count": 1 if gate == "financial_moneyflow" else 0,
                "duplicate_count": 0,
                "invalid_value_count": 0,
            }
            for gate in SOURCE_GATES
        ],
    }
    return cas, snapshot, audit


@pytest.mark.parametrize(
    "change",
    ["factor", "blocked_pit", "tail_audit", "cross_operation", "missing_source", "bad_counts", "duplicate_gate"],
)
def test_normalization_rejects_incomplete_or_drifted_private_domain(tmp_path, change):
    cas, snapshot, audit = fixture(tmp_path)
    selected = (Component.DOMESTIC_INDEX_CONTEXT,)
    builder = SimpleNamespace(cas=cas, profile=SimpleNamespace(start_date=date(2026, 9, 1)))
    if change == "factor":
        selected = (Component.FACTOR_H5_STATIC,)
    elif change == "blocked_pit":
        gate = next(g for g in audit["gates"] if g["gate_id"] == "pit_stock_pools")
        gate.update(observed_count=0, unexplained_missing_count=1)
    elif change == "tail_audit":
        audit["audit_start"] = "2026-09-02"
    elif change == "cross_operation":
        audit["operation_id"] = f"dmr_{'2' * 32}"
    elif change == "missing_source":
        snapshot = replace(snapshot, partitions=snapshot.partitions[:1])
    elif change == "bad_counts":
        audit["gates"][0]["observed_count"] = 0
    else:
        audit["gates"].append(dict(audit["gates"][0]))
    with pytest.raises(ComponentPreparationError):
        preparation.normalize_preparation_artifacts(builder, snapshot=snapshot, audit=audit, components=selected)


def test_index_normalization_does_not_read_price_or_factor_domain(monkeypatch, tmp_path):
    cas, snapshot, audit = fixture(tmp_path)
    calls = []
    monkeypatch.setattr(preparation, "_SealedSnapshotView", lambda *_: object())

    class Builder:
        profile = SimpleNamespace(profile="qe_hmm_full_v2", start_date=date(2026, 9, 1))

        def _trading_dates(self, *_):
            return (snapshot.official_cutoff,)

        def _index_entries(self, *_args, **_kwargs):
            calls.append("index_quotes")
            return (), (), ()

        def _raw_entries(self, _view, component):
            assert component is Component.DOMESTIC_INDEX_CONTEXT
            return ()

        def _seal_component_manifest(self, component, **_kwargs):
            calls.append(component.value)
            return cas.put_json({"component": component.value})

    builder = Builder()
    builder.cas = cas
    result = preparation.normalize_preparation_artifacts(
        builder,
        snapshot=snapshot,
        audit=audit,
        components=(Component.DOMESTIC_INDEX_CONTEXT,),
    )
    payload = cas.get_json(result.reference)
    assert calls == ["index_quotes", Component.DOMESTIC_INDEX_CONTEXT.value]
    assert payload["schema_version"] == preparation.PREPARATION_ARTIFACT_SCHEMA
    assert payload["publication_allowed"] is False
    assert payload["consistent_input_set_complete"] is False
    assert result.qfq_authority_ref is None
    assert set(result.component_manifests) == {Component.DOMESTIC_INDEX_CONTEXT}


def test_private_index_reader_reuses_formal_cas_rows_and_rejects_full_ready(tmp_path):
    """Actual production sealer and component hashes, not a caller PASS flag."""
    from backend.services.dataset_release.artifact_ready_build_source import (
        ArtifactReadyPreparationBuildSource,
        ArtifactReadyBuildSourceError,
    )
    from backend.services.dataset_release.artifact_ready_source import (
        ArtifactReadySourceBuilder,
        _SealedSnapshotView,
        ARTIFACT_READY_INDEX_CHUNK_SCHEMA,
        load_artifact_ready_contract,
        ArtifactReadySourceError,
    )
    from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields
    from backend.services.dataset_release.source_authority import (
        MonthlySourceAuthority,
        PRODUCTION_QUERY_SPECS,
        SourceTableSchema,
    )
    from backend.services.dataset_release.source_manifest import SourceManifest
    import hashlib

    ControlStore.initialize(tmp_path)
    cas = CASStore(tmp_path)
    cutoff = date(2026, 9, 30)
    profile = SimpleNamespace(
        profile="qe_hmm_full_v2",
        start_date=cutoff,
        pit_authority_status="ACTIVE_CANONICAL",
        resource_policy=SimpleNamespace(validation_read_chunk_rows=1000),
        pressure_ladder={"date_chunk_months": (1,), "minute_batch": (100,)},
    )
    authority = MonthlySourceAuthority(profile, cas)
    raw_rows = {
        "trading_calendar": {"cal_date": cutoff.isoformat(), "is_trading": True},
        "index_daily": {
            "ts_code": "000300.SH",
            "trade_date": cutoff.isoformat(),
            **{name: 1 for name in PRODUCTION_QUERY_SPECS["index_daily"].value_columns},
        },
    }

    class Session:
        def stream(self, key, _params, *, fetch_rows):
            query = PRODUCTION_QUERY_SPECS[key]
            row = raw_rows[key]
            yield {
                "row_key": canonical_json_bytes([row[name] for name in query.key_columns]).decode(),
                "row_payload": canonical_json_bytes(row).decode(),
            }

    partitions = tuple(
        authority._seal_query_partition(
            Session(),
            query=PRODUCTION_QUERY_SPECS[key],
            partition_key="2026-09-30_2026-09-30",
            params={"start": cutoff, "end": cutoff},
            tokens=("postgres:1-ABC-1",),
            table_schema=SourceTableSchema(
                PRODUCTION_QUERY_SPECS[key].table_identity, PRODUCTION_QUERY_SPECS[key].required_columns
            ),
        )
        for key in raw_rows
    )
    pit_bytes = b'{"fixture_pit":true}'
    pit_ref = cas.put_bytes(pit_bytes)
    source_ref = cas.put_json({"schema_version": "private_fixture_source"})
    snapshot = PreparationSourceSnapshot(
        f"dmr_{'1' * 32}",
        cutoff,
        SimpleNamespace(
            cutoff=cutoff, spans_sha256=hashlib.sha256(pit_bytes).hexdigest(), canonical_bytes=lambda: pit_bytes
        ),
        pit_ref,
        source_ref,
        source_ref,
        partitions,
        (),
        ("postgres:1-ABC-1",),
        ("margin_detail",),
        "a" * 64,
        SourceManifest(tuple(part.summary for part in partitions)),
    )
    builder = ArtifactReadySourceBuilder(profile, cas)
    chunk = cas.put_json({"schema_version": ARTIFACT_READY_INDEX_CHUNK_SCHEMA, "rows": [raw_rows["index_daily"]]})
    raw_entries = builder._raw_entries(_SealedSnapshotView(cas, snapshot), Component.DOMESTIC_INDEX_CONTEXT)
    derived = {
        "identity": "index_daily_merged:2026-09-30_2026-09-30",
        "dataset": "index_daily_merged",
        "partition_key": "2026-09-30_2026-09-30",
        "role": "derived_fixture_index",
        "rows_ref": chunk.as_dict(),
        "content_digest": chunk.sha256,
        "schema_digest": "a" * 64,
        "row_count": 1,
    }
    component_ref = builder._seal_component_manifest(
        Component.DOMESTIC_INDEX_CONTEXT,
        source_content_root=snapshot.source_content_root,
        partitions=(*raw_entries, derived),
        details={},
    )
    body = {
        "schema_version": preparation.PREPARATION_ARTIFACT_SCHEMA,
        "operation_id": snapshot.operation_id,
        "profile": profile.profile,
        "cutoff": cutoff.isoformat(),
        "preparation_source_manifest_ref": source_ref.as_dict(),
        "source_content_root": snapshot.source_content_root,
        "pit_snapshot_ref": pit_ref.as_dict(),
        "pit_snapshot_digest": snapshot.pit_snapshot_digest,
        "component_manifests": {Component.DOMESTIC_INDEX_CONTEXT.value: component_ref.as_dict()},
        "qfq_denominator_authority_ref": None,
        "qfq_source_summary": {},
        "provider_receipt_refs": [],
        "derived_source_receipt_refs": [chunk.as_dict()],
        "consistent_input_set_complete": False,
        "publication_allowed": False,
        "database_write_performed": False,
    }
    ref = cas.put_json({**body, "canonical_digest": digest_named_fields(preparation.PREPARATION_ARTIFACT_SCHEMA, body)})
    source = ArtifactReadyPreparationBuildSource(cas=cas, profile=profile, snapshot=snapshot, reference=ref)
    assert source.trading_days() == (cutoff,)
    assert tuple(source.index_rows()) == (raw_rows["index_daily"],)
    with pytest.raises(ArtifactReadyBuildSourceError, match="not a full"):
        _ = source.artifact_ready_content_root
    with pytest.raises(ArtifactReadySourceError):
        load_artifact_ready_contract(
            cas,
            profile,
            ref,
            expected_source_content_root=snapshot.source_content_root,
            expected_pit_snapshot_digest=snapshot.pit_snapshot_digest,
        )
    forged = {**body, "publication_allowed": True}
    forged_ref = cas.put_json(
        {**forged, "canonical_digest": digest_named_fields(preparation.PREPARATION_ARTIFACT_SCHEMA, forged)}
    )
    with pytest.raises(ArtifactReadyBuildSourceError, match="graph identity"):
        ArtifactReadyPreparationBuildSource(cas=cas, profile=profile, snapshot=snapshot, reference=forged_ref)
