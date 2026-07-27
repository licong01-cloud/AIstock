from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib

import pytest

from backend.services.advisory_phase1.dataset_build import (
    BaseSnapshotIdentity,
    CompositeCapabilityRequirement,
    FrozenIdentity,
    LabelTargetIdentity,
    RetrospectiveCaptureSetMember,
    RetrospectiveDatasetBuildRequest,
    build_id_for,
    logical_build_key,
)
from backend.services.advisory_phase1.dataset_build_postgres import (
    PostgresDatasetBuildRepository,
)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _policy_ref(policy_hash: str) -> dict[str, object]:
    envelope_hash = _hash(f"policy-envelope:{policy_hash}")
    return {
        "schema_version": "advisory_historical_range_artifact_ref_v1",
        "artifact_kind": "REQUEST",
        "relative_path": f"requests/{envelope_hash}.json",
        "semantic_content_hash": envelope_hash,
        "payload_sha256": policy_hash,
        "file_sha256": _hash("policy-file"),
        "producer_contract_version": "advisory_phase1r_r4_outcome_policy_v1",
        "payload_schema_version": "advisory_historical_range_outcome_policy_bundle_v1",
    }


def _request() -> RetrospectiveDatasetBuildRequest:
    scope = FrozenIdentity(identity_id="range-scope", identity_hash=_hash("range-scope"))
    captures = tuple(
        RetrospectiveCaptureSetMember(
            capture_batch_id=batch_id,
            capture_request_hash=_hash(f"request-{purpose}"),
            capture_receipt_hash=_hash(f"receipt-{purpose}"),
            membership_hash=_hash(f"membership-{purpose}"),
            capture_purpose=purpose,
            range_lineage_scope_id=scope.identity_id,
            range_lineage_scope_hash=scope.identity_hash,
            source_revision_set_id="source-revision",
            source_revision_set_hash=_hash("source-revision"),
            date_start=date(2026, 7, 1),
            date_end=date(2026, 7, 1),
        )
        for batch_id, purpose in (
            ("capture-label", "LABEL_CAPTURE_V1"),
            ("capture-observation", "OBSERVATION_CAPTURE_V1"),
        )
    )
    policy_hash = _hash("range-policy")
    return RetrospectiveDatasetBuildRequest(
        range_lineage_scopes=(scope,),
        captures=captures,
        date_start=date(2026, 7, 1),
        date_end=date(2026, 7, 1),
        selected_observation_mappings=(
            FrozenIdentity(identity_id="observation-mapping", identity_hash=_hash("observation-mapping")),
        ),
        selected_label_mappings=(
            FrozenIdentity(identity_id="label-mapping", identity_hash=_hash("label-mapping")),
        ),
        label_policy_bundle_id="range-policy",
        label_policy_bundle_hash=policy_hash,
        historical_range_policy_bundle_ref=_policy_ref(policy_hash),
        label_targets=(
            LabelTargetIdentity(
                horizon_trading_days=5,
                projection="EXECUTABLE",
                projection_schema_version="advisory_phase1_outcome_projection_v1",
            ),
        ),
        universe_policy_hash=_hash("universe"),
        benchmark_policy_hash=_hash("benchmark"),
        cost_policy_hash=_hash("cost"),
        calendar_hash=_hash("calendar"),
        symbol_normalization_policy_hash=_hash("symbol"),
        query_registry_version="registry-v1",
        query_registry_hash=_hash("registry"),
        snapshot_source_revision_set_id="source-revision",
        snapshot_source_revision_set_hash=_hash("source-revision"),
        required_composite_capabilities=(
            CompositeCapabilityRequirement(component="labels", capability="RESEARCH_AUDIT"),
            CompositeCapabilityRequirement(component="observations", capability="INTERNAL_BOOTSTRAP"),
        ),
        builder_version="builder-v1",
        code_commit="commit-r4",
        writer_version="writer-v1",
        snapshot_schema_version="snapshot-retrospective-v1",
        schema_fingerprint=_hash("schema"),
        partition_policy_id="partition-v1",
        partition_policy_hash=_hash("partition"),
        policy_compatibility_hash=_hash("compatibility"),
        compression_config={"codec": "zstd"},
        requested_source_cutoff=date(2026, 7, 1),
        label_as_of_ts=datetime(2026, 7, 23, tzinfo=timezone.utc),
        selector_policy_hash=_hash("retrospective-selector"),
        selected_range_day_outcome_set_hash=_hash("range-day-outcome-set"),
        policy_component_set_hash=_hash("policy-component-set"),
    )


class _CaptureCursor:
    def __init__(self, request: RetrospectiveDatasetBuildRequest) -> None:
        self.request = request
        self.query = ""
        self.params: tuple[object, ...] = ()

    def execute(self, query: str, params: tuple[object, ...] = ()) -> None:
        self.query = query
        self.params = params

    def fetchone(self) -> dict[str, object] | None:
        if "FROM app.advisory_capture_batch" not in self.query:
            return None
        capture_id = str(self.params[0])
        member = next(item for item in self.request.captures if item.capture_batch_id == capture_id)
        common = {
            "capture_request_hash": member.capture_request_hash,
            "capture_status": "COMPLETE",
            "membership_hash": member.membership_hash,
            "capture_receipt_hash": member.capture_receipt_hash,
            "capture_purpose": member.capture_purpose,
            "lineage_identity_type": "HISTORICAL_RANGE",
            "range_lineage_scope_id": member.range_lineage_scope_id,
            "range_lineage_scope_hash": member.range_lineage_scope_hash,
            "execution_origin": "HISTORICAL_RANGE_RESEARCH",
            "research_scope": "RETROSPECTIVE_RESEARCH_ONLY",
            "evidence_scope": "RETROSPECTIVE_RESEARCH_ONLY",
            "selector_policy_hash": self.request.selector_policy_hash,
        }
        if member.capture_purpose == "OBSERVATION_CAPTURE_V1":
            common.update(
                request_payload_jsonb={
                    "plans": [
                        {
                            "decision_as_of_trade_date": "2026-07-01",
                            "signal_source_revision_set_id": member.source_revision_set_id,
                            "signal_source_revision_set_hash": member.source_revision_set_hash,
                            "range_scope": {
                                "range_lineage_scope_id": member.range_lineage_scope_id,
                                "range_lineage_scope_hash": member.range_lineage_scope_hash,
                            },
                            "selector_policy_hash": self.request.selector_policy_hash,
                        }
                    ]
                },
                historical_range_policy_bundle_ref=self.request.historical_range_policy_bundle_ref,
                historical_range_policy_bundle_hash=self.request.label_policy_bundle_hash,
            )
        else:
            common.update(
                request_payload_jsonb={
                    "planned_labels": [
                        {
                            "decision_as_of_trade_date": "2026-07-01",
                            "horizon_trading_days": 5,
                            "projection": "EXECUTABLE",
                        }
                    ],
                    "selected_observation_mappings": [
                        {
                            "selected_mapping_id": "observation-mapping",
                            "selected_mapping_hash": _hash("observation-mapping"),
                        }
                    ],
                    "label_source_revision_set_id": member.source_revision_set_id,
                    "label_source_revision_set_hash": member.source_revision_set_hash,
                    "label_policy_bundle_id": self.request.label_policy_bundle_id,
                    "label_policy_bundle_hash": self.request.label_policy_bundle_hash,
                    "label_as_of_ts": self.request.label_as_of_ts.isoformat(),
                },
                historical_range_policy_bundle_ref=self.request.historical_range_policy_bundle_ref,
                historical_range_policy_bundle_hash=self.request.label_policy_bundle_hash,
            )
        return common

    def fetchall(self) -> list[dict[str, str]]:
        capture_id = str(self.params[0])
        if capture_id == "capture-observation":
            return [
                {
                    "evidence_role": "OBSERVATION_VERSION",
                    "evidence_id": "observation-version",
                    "evidence_content_hash": _hash("observation-version"),
                }
            ]
        return [
            {
                "evidence_role": "SELECTED_LABEL_MAPPING",
                "evidence_id": "label-mapping",
                "evidence_content_hash": _hash("label-mapping"),
            }
        ]


class _BaseSnapshotCursor:
    def __init__(self, lineage_identity_type: str) -> None:
        self.lineage_identity_type = lineage_identity_type
        self.query = ""

    def execute(self, query: str, _params: tuple[object, ...] = ()) -> None:
        self.query = query

    def fetchone(self) -> dict[str, object] | None:
        if "FROM app.advisory_dataset_snapshot s" in self.query:
            return {
                "snapshot_id": "snapshot-base",
                "snapshot_content_hash": _hash("snapshot-content"),
                "manifest_sha256": _hash("snapshot-manifest"),
                "policy_compatibility_hash": _hash("compatibility"),
                "base_snapshot_id": None,
                "lineage_identity_type": self.lineage_identity_type,
                "range_lineage_scope_set_hash": _hash("range-scope-set"),
                "selector_policy_hash": _hash("retrospective-selector"),
            }
        return None


def _build_row(request: RetrospectiveDatasetBuildRequest) -> dict[str, object]:
    now = datetime(2026, 7, 23, tzinfo=timezone.utc)
    key = logical_build_key(request)
    return {
        "build_id": build_id_for(key, 1),
        "build_request_payload_jsonb": request.model_dump(mode="json"),
        "logical_build_key_sha256": key,
        "build_generation": 1,
        "predecessor_build_id": None,
        "lineage_identity_type": request.lineage_identity_type,
        "range_lineage_scope_set_hash": request.range_lineage_scope_set_hash,
        "selector_policy_hash": request.selector_policy_hash,
        "execution_origin": request.execution_origin,
        "research_scope": request.research_scope,
        "evidence_scope": request.evidence_scope,
        "historical_range_policy_bundle_ref": request.historical_range_policy_bundle_ref,
        "historical_range_policy_bundle_hash": request.label_policy_bundle_hash,
        "selected_range_day_outcome_set_hash": request.selected_range_day_outcome_set_hash,
        "policy_component_set_hash": request.policy_component_set_hash,
        "lifecycle_status": "ACTIVE",
        "checkpoint": "REQUESTED",
        "current_fencing_token": 1,
        "current_attempt_id": None,
        "row_version": 1,
        "materialized_attempt_id": None,
        "materialize_receipt_hash": None,
        "materialized_file_set_hash": None,
        "verified_attempt_id": None,
        "verify_receipt_hash": None,
        "verified_file_set_hash": None,
        "verification_contract_version": None,
        "promoted_attempt_id": None,
        "promotion_receipt_hash": None,
        "promoted_manifest_hash": None,
        "sealed_attempt_id": None,
        "seal_receipt_hash": None,
        "sealed_snapshot_id": None,
        "termination_receipt_hash": None,
        "terminal_reason_code": None,
        "created_at": now,
        "updated_at": now,
    }


def test_retrospective_capture_admission_and_readback_are_exact() -> None:
    request = _request()
    PostgresDatasetBuildRepository._require_capture_admission(
        _CaptureCursor(request), request
    )
    persisted = PostgresDatasetBuildRepository._build_from_row(_build_row(request))
    assert persisted.request == request


def test_retrospective_build_readback_rejects_mixed_selector_hash() -> None:
    request = _request()
    row = _build_row(request)
    row["selector_policy_hash"] = _hash("formal-selector")
    with pytest.raises(Exception, match="identity differs"):
        PostgresDatasetBuildRepository._build_from_row(row)


def test_retrospective_build_rejects_formal_base_snapshot() -> None:
    payload = _request().model_dump(mode="python")
    payload["build_request_hash"] = None
    payload["base_snapshot"] = BaseSnapshotIdentity(
        snapshot_id="snapshot-base",
        snapshot_content_hash=_hash("snapshot-content"),
        manifest_sha256=_hash("snapshot-manifest"),
        snapshot_source_revision_set_hash=_hash("source-revision"),
        capture_set_hash=_hash("base-captures"),
        policy_compatibility_hash=_hash("compatibility"),
    )
    request = RetrospectiveDatasetBuildRequest.model_validate(payload)
    with pytest.raises(Exception, match="lineage type differs"):
        PostgresDatasetBuildRepository._require_base_snapshot_admission(
            _BaseSnapshotCursor("PHASE0A"), request
        )
