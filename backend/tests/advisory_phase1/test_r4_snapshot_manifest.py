from __future__ import annotations

import hashlib
import json

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.dataset_build import (
    DatasetBlobHeader,
    DatasetSnapshotFile,
    DatasetSnapshotLabel,
    DatasetSnapshotObservation,
)
from backend.services.advisory_phase1.snapshot_writer import (
    DatasetCapabilityManifest,
    DatasetCapabilityRow,
    DatasetManifest,
    DatasetManifestCore,
)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _members(selector_hash: str):  # type: ignore[no-untyped-def]
    blob = DatasetBlobHeader(
        store_backend_hash=_hash("store"),
        blob_sha256=_hash("blob"),
        size_bytes=10,
    )
    file = DatasetSnapshotFile(
        logical_path="canonical_signals/part-00000.parquet",
        logical_role="canonical_signals",
        partition_key_hash=_hash("partition"),
        ordinal=0,
        content_uri="file:///snapshot/canonical-signals.parquet",
        sha256=blob.blob_sha256,
        size_bytes=blob.size_bytes,
        row_count=1,
        schema_fingerprint=_hash("schema"),
        partition_content_hash=_hash("content"),
        blob=blob,
    )
    observation = DatasetSnapshotObservation(
        canonical_signal_id="signal-1",
        observation_version_id="observation-1",
        oos_interval_id="RETROSPECTIVE_RANGE_NO_FORMAL_OOS_V1",
        selector_policy_hash=selector_hash,
    )
    label = DatasetSnapshotLabel(
        label_key_hash=_hash("label-key"),
        label_version_id="label-1",
        canonical_signal_id=observation.canonical_signal_id,
        observation_version_id=observation.observation_version_id,
        candidate_stage_evidence_id="stage-1",
        symbol="000001.SZ",
        selector_policy_hash=selector_hash,
    )
    return file, observation, label


def _capability() -> DatasetCapabilityManifest:
    return DatasetCapabilityManifest(
        rows=(
            DatasetCapabilityRow(
                component="OBSERVATION",
                capability="RESEARCH_AUDIT",
                status="FULL",
            ),
            DatasetCapabilityRow(
                component="MODEL",
                capability="MODEL_TRAINING_READY",
                status="false",
            ),
            DatasetCapabilityRow(
                component="RUNTIME",
                capability="RUNTIME_ADVISORY_READY",
                status="false",
            ),
            DatasetCapabilityRow(
                component="TRADING",
                capability="TRADING_EXECUTION_READY",
                status="false",
            ),
        )
    )


def _common(selector_hash: str) -> dict[str, object]:
    file, observation, label = _members(selector_hash)
    return {
        "files": (file,),
        "selected_observations": (observation,),
        "selected_labels": (label,),
        "snapshot_source_revision_set_hash": _hash("source"),
        "capture_set_hash": _hash("capture"),
        "query_registry_hash": _hash("query"),
        "capability_manifest": _capability(),
        "schema_fingerprint": _hash("schema"),
        "builder_version": "builder-v1",
        "code_commit": "commit-r4",
        "writer_version": "writer-v1",
        "partition_policy_hash": _hash("partition-policy"),
        "policy_compatibility_hash": _hash("compatibility"),
    }


def test_formal_manifest_bytes_keep_the_pre_r4_shape() -> None:
    selector_hash = _hash("formal-selector")
    core = DatasetManifestCore(
        **_common(selector_hash),
        handoff_readiness_hash=_hash("handoff"),
        admission_scope_set_hash=_hash("admission"),
    )
    manifest = DatasetManifest(core=core, store_backend_hash=_hash("store"))
    canonical = manifest.canonical_bytes().decode("ascii")
    canonical_core = json.loads(canonical)["core"]

    assert "lineage_identity_type" not in canonical_core
    assert "selector_policy_hash" not in canonical_core
    assert "range_lineage_scope_set_hash" not in canonical_core


def test_retrospective_manifest_closes_selector_policy_and_exact_sets() -> None:
    selector_hash = _hash("retrospective-selector")
    payload = {
        **_common(selector_hash),
        "lineage_identity_type": "HISTORICAL_RANGE",
        "execution_origin": "HISTORICAL_RANGE_RESEARCH",
        "research_scope": "RETROSPECTIVE_RESEARCH_ONLY",
        "evidence_scope": "RETROSPECTIVE_RESEARCH_ONLY",
        "range_lineage_scope_set_hash": _hash("range-scopes"),
        "selector_policy_hash": selector_hash,
        "selected_range_day_outcome_set_hash": _hash("range-day-outcome-set"),
        "policy_lineage_type": "HISTORICAL_RANGE_OUTCOME_POLICY",
        "historical_range_policy_bundle_hash": _hash("range-policy"),
        "policy_component_set_hash": _hash("policy-components"),
        "selected_observation_mapping_set_hash": _hash("observation-mappings"),
        "selected_label_mapping_set_hash": _hash("label-mappings"),
        "source_revision_closure_hash": _hash("source-closure"),
        "maturity_coverage_hash": _hash("maturity-coverage"),
    }
    core = DatasetManifestCore(**payload)
    manifest = DatasetManifest(core=core, store_backend_hash=_hash("store"))

    assert core.manifest_core_sha256 == canonical_json_sha256(
        {
            "files": [item.model_dump(mode="json") for item in core.files],
            "observations": [item.model_dump(mode="json") for item in core.selected_observations],
            "labels": [item.model_dump(mode="json") for item in core.selected_labels],
            "source_revision_set_hash": core.snapshot_source_revision_set_hash,
            "capture_set_hash": core.capture_set_hash,
            "base_snapshot": None,
            "handoff_readiness_hash": None,
            "admission_scope_set_hash": None,
            "query_registry_hash": core.query_registry_hash,
            "capability_hash": core.capability_manifest.manifest_hash,
            "schema_fingerprint": core.schema_fingerprint,
            "builder_version": core.builder_version,
            "code_commit": core.code_commit,
            "writer_version": core.writer_version,
            "partition_policy_hash": core.partition_policy_hash,
            "policy_compatibility_hash": core.policy_compatibility_hash,
            "lineage_identity_type": core.lineage_identity_type,
            "execution_origin": core.execution_origin,
            "research_scope": core.research_scope,
            "evidence_scope": core.evidence_scope,
            "range_lineage_scope_set_hash": core.range_lineage_scope_set_hash,
            "selector_policy_hash": core.selector_policy_hash,
            "selected_range_day_outcome_set_hash": core.selected_range_day_outcome_set_hash,
            "policy_lineage_type": core.policy_lineage_type,
            "historical_range_policy_bundle_hash": core.historical_range_policy_bundle_hash,
            "policy_component_set_hash": core.policy_component_set_hash,
            "selected_observation_mapping_set_hash": core.selected_observation_mapping_set_hash,
            "selected_label_mapping_set_hash": core.selected_label_mapping_set_hash,
            "observation_count": 1,
            "label_count": 1,
            "source_revision_closure_hash": core.source_revision_closure_hash,
            "maturity_coverage_hash": core.maturity_coverage_hash,
        }
    )
    assert '"lineage_identity_type":"HISTORICAL_RANGE"' in manifest.canonical_bytes().decode("ascii")

    mixed = dict(payload)
    mixed["selected_labels"] = (
        payload["selected_labels"][0].model_copy(
            update={"selector_policy_hash": _hash("formal-selector")}
        ),
    )
    with pytest.raises(ValueError, match="retrospective manifest lineage identity"):
        DatasetManifestCore(**mixed)
