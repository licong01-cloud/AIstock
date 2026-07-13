"""Full Batch D Parquet/CAS/manifest contract tests."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import os

import pytest

from backend.services.advisory_phase1.dataset_build import (
    BuildCheckpoint,
    DatasetBuild,
    FixtureDatasetBuildRequest,
    InMemoryDatasetBuildRepository,
)
from backend.services.advisory_phase1.dataset_store import (
    LocalContentAddressedStore,
    LocalContentAddressedStoreError,
    REASON_CAS_CONTENT_CONFLICT,
)
from backend.services.advisory_phase1.outcome_engine import CalculationEvidenceBundle
from backend.services.advisory_phase1.snapshot_writer import (
    BATCH_D_BUILDER_VERSION,
    BATCH_D_WRITER_VERSION,
    SNAPSHOT_ARROW_SCHEMAS_V1,
    DatasetCapabilityManifest,
    DatasetCapabilityRow,
    DatasetCasPromoter,
    DatasetSnapshotMaterializer,
    DatasetSnapshotPipeline,
    DeterministicParquetWriter,
    FullParquetVerifier,
    LogicalDatasetRow,
    SnapshotWriterError,
    _logical_parquet_path,
    _logical_sort_key,
    assemble_sealed_snapshot,
    build_dataset_manifest,
    build_promotion_receipt,
    snapshot_files_from_published,
)


UTC_TS = datetime(2026, 7, 3, tzinfo=timezone.utc)
TRADE_DATE = date(2026, 7, 1)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _identity() -> dict[str, str]:
    return {
        "backend": "LOCAL_FILESYSTEM_V1",
        "durability_mode": LocalContentAddressedStore.expected_durability_mode(),
        "atomic_publish_mode": "HARDLINK_CREATE_IF_ABSENT_V1",
        "writer_compatibility": BATCH_D_WRITER_VERSION,
    }


def _default_value(name: str, arrow_type: str, nullable: bool):  # type: ignore[no-untyped-def]
    if nullable:
        return None
    if arrow_type == "utf8":
        return _hash(name) if "hash" in name or "sha256" in name else f"{name}-v"
    if arrow_type == "date32":
        return TRADE_DATE
    if arrow_type == "timestamp_us_utc":
        return UTC_TS
    if arrow_type == "decimal38_12":
        return Decimal("1.000000000000")
    if arrow_type in {"int32", "int64"}:
        return 1
    if arrow_type == "bool":
        return False
    if arrow_type == "list_utf8":
        return []
    if arrow_type == "canonical_json":
        return {"field": name}
    raise AssertionError(arrow_type)


def _values(role: str, **overrides):  # type: ignore[no-untyped-def]
    result = {
        field.name: _default_value(field.name, field.arrow_type, field.nullable)
        for field in SNAPSHOT_ARROW_SCHEMAS_V1[role]
    }
    result.update(overrides)
    return result


def _row(role: str, partition: dict[str, str], **overrides) -> LogicalDatasetRow:  # type: ignore[no-untyped-def]
    values = _values(role, **overrides)
    return LogicalDatasetRow(
        logical_role=role,
        partition_key=partition,
        sort_key=_logical_sort_key(role, values),
        values=values,
    )


def _fixture_rows() -> dict[str, tuple[LogicalDatasetRow, ...]]:
    month = {"year": "2026", "month": "07"}
    outcome_partition = {"horizon": "5", **month}
    signal_id = "signal-1"
    observation_id = "observation-1"
    observation_hash = _hash("observation-content")
    stage_id = "stage-1"
    candidate_label_id = "label-candidate-1"
    universe_label_id = "label-universe-1"
    candidate_label_hash = _hash("label-candidate-content")
    universe_label_hash = _hash("label-universe-content")
    candidate_key = _hash("candidate-key")
    universe_key = _hash("universe-key")
    evidence_store = _hash("evidence-store")
    candidate_bundle = CalculationEvidenceBundle(evidence_payload={"owner": "candidate", "version": 1})
    universe_bundle = CalculationEvidenceBundle(evidence_payload={"owner": "universe", "version": 1})
    candidate_evidence = str(candidate_bundle.evidence_hash)
    universe_evidence = str(universe_bundle.evidence_hash)
    common_outcome = {
        "decision_as_of_trade_date": TRADE_DATE,
        "horizon_trading_days": 5,
        "projection": "RETURN_NET_ABSOLUTE",
        "projection_schema_version": "advisory_phase1_outcome_calculation_v1",
        "maturity_status": "MATURED",
        "outcome_event_status": "NONE",
        "entry_status": "EXECUTABLE",
        "computed_at": UTC_TS,
        "scheduled_maturity_ts": UTC_TS,
    }
    candidate_outcome = _values(
        "outcome_labels",
        **common_outcome,
        label_version_id=candidate_label_id,
        label_content_hash=candidate_label_hash,
        label_key_hash=candidate_key,
        label_revision_no=1,
        owner_type="CANDIDATE",
        owner_key="candidate-owner",
        canonical_signal_id=signal_id,
        observation_version_id=observation_id,
        candidate_stage_evidence_id=stage_id,
        symbol="000001.SZ",
        universe_layer=None,
        calculation_evidence_sha256=candidate_evidence,
        calculation_evidence_size_bytes=len(candidate_bundle.canonical_bytes()),
        calculation_evidence_store_backend_hash=evidence_store,
    )
    universe_outcome = _values(
        "outcome_labels",
        **common_outcome,
        label_version_id=universe_label_id,
        label_content_hash=universe_label_hash,
        label_key_hash=universe_key,
        label_revision_no=1,
        owner_type="UNIVERSE",
        owner_key="universe-owner",
        canonical_signal_id=signal_id,
        observation_version_id=None,
        candidate_stage_evidence_id=None,
        symbol="000002.SZ",
        universe_layer="FULL_MARKET",
        calculation_evidence_sha256=universe_evidence,
        calculation_evidence_size_bytes=len(universe_bundle.canonical_bytes()),
        calculation_evidence_store_backend_hash=evidence_store,
    )
    return {
        "canonical_signals": (
            _row(
                "canonical_signals",
                month,
                canonical_signal_id=signal_id,
                signal_schema_version="advisory_canonical_signal_v1",
                decision_as_of_trade_date=TRADE_DATE,
                selection_as_of_trade_date=TRADE_DATE,
                target_trade_date=date(2026, 7, 2),
                decision_cutoff_ts=datetime(2026, 7, 1, 7, tzinfo=timezone.utc),
                alpha_mode="single_alpha",
            ),
        ),
        "observation_versions": (
            _row(
                "observation_versions",
                month,
                observation_version_id=observation_id,
                canonical_signal_id=signal_id,
                observation_schema_version="advisory_signal_observation_version_v1",
                observation_revision_no=1,
                observation_content_hash=observation_hash,
                observation_status="COMPLETE",
                valid_no_candidate=False,
            ),
        ),
        "selected_observations": (
            _row(
                "selected_observations",
                {},
                selected_mapping_id="observation-map-1",
                selected_mapping_hash=_hash("observation-map-1"),
                canonical_signal_id=signal_id,
                terminal_observation_version_id=observation_id,
                terminal_observation_content_hash=observation_hash,
                terminal_revision_no=1,
            ),
        ),
        "lineage": (
            _row(
                "lineage",
                month,
                lineage_id="lineage-1",
                canonical_signal_id=signal_id,
                observation_version_id=observation_id,
                evidence_scope="RETROSPECTIVE_RESEARCH_ONLY",
                oos_interval_id="oos-1",
            ),
        ),
        "stage_summaries": (
            _row(
                "stage_summaries",
                month,
                stage_evidence_id=stage_id,
                observation_version_id=observation_id,
                stage="selection_effective",
                capability_status="FULL",
            ),
        ),
        "stage_candidates": (
            _row(
                "stage_candidates",
                month,
                stage_evidence_id=stage_id,
                symbol="000001.SZ",
                membership_status="INCLUDED",
                rank=1,
                component_capability="FULL",
            ),
        ),
        "outcome_labels": (
            _row("outcome_labels", outcome_partition, **candidate_outcome),
            _row("outcome_labels", outcome_partition, **universe_outcome),
        ),
        "selected_labels": (
            _row(
                "selected_labels",
                {"horizon": "5"},
                selector_request_hash=_hash("selector-request"),
                selection_policy="LATEST_ELIGIBLE_REVISION_V1",
                selection_policy_hash=_hash("selector-policy"),
                label_key_hash=candidate_key,
                requested_label_as_of_ts=UTC_TS,
                terminal_label_version_id=candidate_label_id,
                terminal_label_content_hash=candidate_label_hash,
                terminal_label_revision_no=1,
                terminal_maturity_status="MATURED",
                terminal_outcome_event_status="NONE",
                selection_status="SELECTED",
                selected_label_mapping_id="label-map-1",
                selected_label_mapping_hash=_hash("label-map-1"),
            ),
        ),
        "outcome_source_evidence": (
            _row(
                "outcome_source_evidence",
                {"owner_type": "CANDIDATE", "horizon": "5", **month},
                owner_type="CANDIDATE",
                label_version_id=candidate_label_id,
                label_key_hash=candidate_key,
                canonical_signal_id=signal_id,
                symbol="000001.SZ",
                horizon_trading_days=5,
                projection="RETURN_NET_ABSOLUTE",
                calculation_evidence_sha256=candidate_evidence,
                calculation_evidence_size_bytes=len(candidate_bundle.canonical_bytes()),
                calculation_evidence_store_backend_hash=evidence_store,
                calculation_evidence_json=candidate_bundle.model_dump(mode="json"),
            ),
            _row(
                "outcome_source_evidence",
                {"owner_type": "UNIVERSE", "horizon": "5", **month},
                owner_type="UNIVERSE",
                label_version_id=universe_label_id,
                label_key_hash=universe_key,
                canonical_signal_id=signal_id,
                symbol="000002.SZ",
                horizon_trading_days=5,
                projection="RETURN_NET_ABSOLUTE",
                calculation_evidence_sha256=universe_evidence,
                calculation_evidence_size_bytes=len(universe_bundle.canonical_bytes()),
                calculation_evidence_store_backend_hash=evidence_store,
                calculation_evidence_json=universe_bundle.model_dump(mode="json"),
            ),
        ),
        "universe_outcomes": (_row("universe_outcomes", outcome_partition, **universe_outcome),),
        "gaps": (),
        "source_revisions": (
            _row(
                "source_revisions",
                {},
                source_revision_set_id="snapshot-source-v1",
                source_revision_set_hash=_hash("source-revision"),
                query_registry_hash=_hash("queries"),
                requested_source_cutoff=datetime(2026, 7, 2, tzinfo=timezone.utc),
                label_as_of_ts=UTC_TS,
                research_only=True,
                member_count=1,
                schema_version="advisory_phase1_source_revision_set_v2",
                member_key="member-1",
                enforced_cutoff_predicate_hash=_hash("cutoff"),
            ),
        ),
    }


def test_outcome_optional_timing_fields_remain_nullable() -> None:
    fields = {field.name: field for field in SNAPSHOT_ARROW_SCHEMAS_V1["outcome_labels"]}

    assert fields["time_to_executable_hit_trading_days"].nullable
    assert fields["observed_holding_trading_days"].nullable


def _request() -> FixtureDatasetBuildRequest:
    return FixtureDatasetBuildRequest(
        phase0a_audit_id="audit-1",
        phase0a_audit_hash=_hash("audit"),
        phase1_handoff_bundle_hash=_hash("handoff-bundle"),
        handoff_readiness_hash=_hash("handoff-ready"),
        admission_scopes=({"identity_id": "scope-1", "identity_hash": _hash("scope-1")},),
        captures=(
            {
                "capture_batch_id": "capture-1", "capture_request_hash": _hash("capture-request-1"),
                "capture_receipt_hash": _hash("receipt-1"), "membership_hash": _hash("members-1"),
                "capture_purpose": "OBSERVATION_CAPTURE_V1", "handoff_readiness_hash": _hash("handoff-ready"),
                "admission_scope_id": "scope-1", "admission_scope_hash": _hash("scope-1"),
                "source_revision_set_id": "source-revision-1", "source_revision_set_hash": _hash("source-revision-1"),
                "date_start": TRADE_DATE, "date_end": date(2026, 7, 2),
            },
            {
                "capture_batch_id": "capture-2", "capture_request_hash": _hash("capture-request-2"),
                "capture_receipt_hash": _hash("receipt-2"), "membership_hash": _hash("members-2"),
                "capture_purpose": "LABEL_CAPTURE_V1", "handoff_readiness_hash": _hash("handoff-ready"),
                "admission_scope_id": "scope-1", "admission_scope_hash": _hash("scope-1"),
                "source_revision_set_id": "label-source-revision-1", "source_revision_set_hash": _hash("label-source-revision-1"),
                "date_start": TRADE_DATE, "date_end": date(2026, 7, 2),
            },
        ),
        date_start=TRADE_DATE,
        date_end=date(2026, 7, 2),
        selected_observation_mappings=({"identity_id": "observation-map-1", "identity_hash": _hash("observation-map-1")},),
        selected_label_mappings=({"identity_id": "label-map-1", "identity_hash": _hash("label-map-1")},),
        label_policy_bundle_id="label-policy-1",
        label_policy_bundle_hash=_hash("policy"),
        label_targets=({"horizon_trading_days": 5, "projection": "RETURN_NET_ABSOLUTE", "projection_schema_version": "advisory_phase1_outcome_calculation_v1"},),
        universe_policy_hash=_hash("universe"), benchmark_policy_hash=_hash("benchmark"), cost_policy_hash=_hash("cost"),
        calendar_hash=_hash("calendar"), symbol_normalization_policy_hash=_hash("symbol"),
        query_registry_version="queries-v1", query_registry_hash=_hash("queries"),
        snapshot_source_revision_set_id="snapshot-source-v1", snapshot_source_revision_set_hash=_hash("source-revision"),
        required_composite_capabilities=({"component": "canonical_signals", "capability": "FULL", "required": True},),
        builder_version=BATCH_D_BUILDER_VERSION, code_commit="abc123", writer_version=BATCH_D_WRITER_VERSION,
        snapshot_schema_version="snapshot-v1", schema_fingerprint=_hash("schema"),
        partition_policy_id="partition-v1", partition_policy_hash=_hash("partition"),
        policy_compatibility_hash=_hash("build-policy-compatibility"), compression_config={"codec": "zstd", "level": 3},
        requested_source_cutoff=date(2026, 7, 2), label_as_of_ts=UTC_TS,
    )


def _materialized_build() -> DatasetBuild:
    build = InMemoryDatasetBuildRepository(now_provider=lambda: UTC_TS).create_or_get(_request(), actor="test")
    payload = build.model_dump(mode="python")
    payload.update(
        checkpoint=BuildCheckpoint.MATERIALIZED,
        materialized_attempt_id="attempt-materialized",
        materialize_receipt_hash=_hash("materialize-receipt"),
        materialized_file_set_hash=_hash("attempt-file-set"),
        row_version=2,
    )
    return DatasetBuild.model_validate(payload)


def _capability_manifest() -> DatasetCapabilityManifest:
    return DatasetCapabilityManifest(
        rows=(
            DatasetCapabilityRow(component="canonical_signals", capability="FULL", status="FULL"),
            DatasetCapabilityRow(component="MODEL", capability="MODEL_TRAINING_READY", status="false"),
            DatasetCapabilityRow(component="RUNTIME", capability="RUNTIME_ADVISORY_READY", status="false"),
            DatasetCapabilityRow(component="TRADING", capability="TRADING_EXECUTION_READY", status="false"),
        )
    )


def _write_full_fixture(tmp_path):  # type: ignore[no-untyped-def]
    writer = DeterministicParquetWriter()
    rows = _fixture_rows()
    files = []
    for role in sorted(SNAPSHOT_ARROW_SCHEMAS_V1):
        files.append(writer.write_schema_descriptor(path=tmp_path / "schemas" / f"{role}.json", logical_role=role))
        role_rows = rows[role]
        partition = role_rows[0].partition_key if role_rows else {}
        files.append(
            writer.write_parquet(
                path=tmp_path / "data" / f"{role}.parquet",
                logical_path=_logical_parquet_path(role=role, partition_key=partition),
                logical_role=role,
                partition_key=partition,
                ordinal=0,
                rows=role_rows,
            )
        )
    return writer, tuple(files)


def test_writer_and_full_verifier_cover_every_role_and_are_byte_deterministic(tmp_path) -> None:  # type: ignore[no-untyped-def]
    writer, files = _write_full_fixture(tmp_path / "first")
    _, second = _write_full_fixture(tmp_path / "second")
    assert [PathLike.read_bytes(file.uri) for file in files] == [PathLike.read_bytes(file.uri) for file in second]

    receipt = FullParquetVerifier().verify_files(
        build=_materialized_build(),
        files=files,
        capability_manifest=_capability_manifest(),
    )
    assert receipt.receipt_hash
    assert len(receipt.files) == len(SNAPSHOT_ARROW_SCHEMAS_V1) * 2
    assert len(receipt.selected_observations) == 1
    assert len(receipt.selected_labels) == 1


class PathLike:
    @staticmethod
    def read_bytes(uri: str) -> bytes:
        from urllib.parse import unquote, urlparse
        from pathlib import Path

        raw = unquote(urlparse(uri).path)
        if os.name == "nt" and raw.startswith("/") and len(raw) > 2 and raw[2] == ":":
            raw = raw[1:]
        return Path(raw).read_bytes()


def test_full_verifier_rejects_missing_role_and_post_write_corruption(tmp_path) -> None:  # type: ignore[no-untyped-def]
    _, files = _write_full_fixture(tmp_path)
    with pytest.raises(SnapshotWriterError, match="cover every logical role"):
        FullParquetVerifier().verify_files(
            build=_materialized_build(),
            files=tuple(file for file in files if "gaps" not in file.logical_path),
            capability_manifest=_capability_manifest(),
        )
    target = next(file for file in files if file.logical_role == "canonical_signals")
    from pathlib import Path
    from urllib.parse import unquote, urlparse

    raw = unquote(urlparse(target.uri).path)
    if os.name == "nt" and raw.startswith("/") and len(raw) > 2 and raw[2] == ":":
        raw = raw[1:]
    Path(raw).write_bytes(b"corrupted")
    with pytest.raises(SnapshotWriterError, match="PARQUET_BYTES_CONFLICT"):
        FullParquetVerifier().verify_files(
            build=_materialized_build(), files=files, capability_manifest=_capability_manifest()
        )


def test_dataset_store_rejects_path_traversal_and_conflicting_blob(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = LocalContentAddressedStore(root=tmp_path / "store", repository_root=tmp_path / "repo", store_identity=_identity())
    with pytest.raises(LocalContentAddressedStoreError, match="safe path component"):
        store.cleanup_attempt_staging(build_id="..", attempt_id="blobs")
    payload = b"payload"
    stored = store.put_blob_bytes(payload)
    target = tmp_path / "store" / "blobs" / "sha256" / stored.sha256[:2] / stored.sha256
    target.write_bytes(b"conflict")
    with pytest.raises(LocalContentAddressedStoreError, match=REASON_CAS_CONTENT_CONFLICT):
        store.put_blob_bytes(payload)


def test_manifest_promotion_and_seal_share_one_content_contract(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = LocalContentAddressedStore(root=tmp_path / "store", repository_root=tmp_path / "repo", store_identity=_identity())
    _, files = _write_full_fixture(tmp_path / "store" / "staging" / "build-1" / "attempt-1")
    build = _materialized_build()
    verification = FullParquetVerifier().verify_files(
        build=build, files=files, capability_manifest=_capability_manifest()
    )
    verified_payload = build.model_dump(mode="python")
    verified_payload.update(
        checkpoint=BuildCheckpoint.VERIFIED,
        verified_attempt_id="verify-1",
        verify_receipt_hash=verification.receipt_hash,
        verified_file_set_hash=verification.file_set_hash,
        verification_contract_version=verification.verification_contract_version,
        row_version=3,
    )
    verified = DatasetBuild.model_validate(verified_payload)
    promoter = DatasetCasPromoter(store=store)
    snapshot_files = snapshot_files_from_published(promoter.publish_files(files))
    manifest = build_dataset_manifest(
        build=verified,
        verification=verification,
        files=snapshot_files,
        capability_manifest=_capability_manifest(),
        store_backend_hash=store.store_backend_hash,
    )
    promoter.publish_manifest(manifest)
    promotion = build_promotion_receipt(build=verified, verification=verification, manifest=manifest)
    promotion_object = promoter.publish_promotion_receipt(promotion)
    promoted_payload = verified.model_dump(mode="python")
    promoted_payload.update(
        checkpoint=BuildCheckpoint.PROMOTED,
        promoted_attempt_id="promote-1",
        promotion_receipt_hash=promotion.receipt_sha256,
        promoted_manifest_hash=manifest.manifest_sha256,
        current_attempt_id="seal-1",
        current_fencing_token=2,
        row_version=4,
    )
    promoted = DatasetBuild.model_validate(promoted_payload)
    snapshot = assemble_sealed_snapshot(
        build=promoted,
        seal_attempt_id="seal-1",
        verification=verification,
        manifest=manifest,
        promotion=promotion,
        promotion_object=promotion_object,
        label_maturity_event_summary=verification.relational_closure_summary,
    )
    assert snapshot.manifest_core_sha256 == manifest.core.manifest_core_sha256
    assert snapshot.snapshot_id == f"advsnap_{manifest.core.manifest_core_sha256[:24]}"


def test_promotion_document_must_exist_before_control_transition(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = LocalContentAddressedStore(root=tmp_path / "store", repository_root=tmp_path / "repo", store_identity=_identity())
    _, files = _write_full_fixture(tmp_path / "store" / "staging" / "build-1" / "attempt-1")
    build = _materialized_build()
    verification = FullParquetVerifier().verify_files(
        build=build, files=files, capability_manifest=_capability_manifest()
    )
    verified_payload = build.model_dump(mode="python")
    verified_payload.update(
        checkpoint=BuildCheckpoint.VERIFIED,
        verified_attempt_id="verify-1",
        verify_receipt_hash=verification.receipt_hash,
        verified_file_set_hash=verification.file_set_hash,
        verification_contract_version=verification.verification_contract_version,
        row_version=3,
    )
    verified = DatasetBuild.model_validate(verified_payload)
    snapshot_files = snapshot_files_from_published(DatasetCasPromoter(store=store).publish_files(files))
    manifest = build_dataset_manifest(
        build=verified,
        verification=verification,
        files=snapshot_files,
        capability_manifest=_capability_manifest(),
        store_backend_hash=store.store_backend_hash,
    )
    promotion = build_promotion_receipt(build=verified, verification=verification, manifest=manifest)
    with pytest.raises(LocalContentAddressedStoreError, match="document is missing"):
        store.verify_document_bytes(kind="promotion_receipts", payload=promotion.canonical_bytes())


def test_complete_pipeline_reaches_real_sealed_snapshot(tmp_path) -> None:  # type: ignore[no-untyped-def]
    class Source:
        @staticmethod
        def read(build):  # type: ignore[no-untyped-def]
            assert build.request.build_request_hash == _request().build_request_hash
            return {role: list(rows) for role, rows in _fixture_rows().items()}

    repository = InMemoryDatasetBuildRepository(now_provider=lambda: UTC_TS)
    build = repository.create_or_get(_request(), actor="test")
    store = LocalContentAddressedStore(
        root=tmp_path / "store",
        repository_root=tmp_path / "repo",
        store_identity=_identity(),
    )
    pipeline = DatasetSnapshotPipeline(
        repository=repository,
        materializer=DatasetSnapshotMaterializer(source_reader=Source(), writer=DeterministicParquetWriter()),
        store=store,
    )

    sealed = pipeline.run(build_id=build.build_id, actor="test")
    assert sealed.checkpoint is BuildCheckpoint.SEALED
    assert sealed.sealed_snapshot_id
    assert len(repository.snapshot_files(str(sealed.sealed_snapshot_id))) >= len(SNAPSHOT_ARROW_SCHEMAS_V1) * 2
    assert pipeline.run(build_id=build.build_id, actor="test") == sealed
