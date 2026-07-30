from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.dataset_build import (
    BaseSnapshotIdentity,
    CompositeCapabilityRequirement,
    FrozenIdentity,
    LabelTargetIdentity,
    RetrospectiveCaptureSetMember,
    RetrospectiveDatasetBuildRequest,
    RetrospectiveSnapshotPolicyMember,
    RetrospectiveSnapshotPolicySet,
    SNAPSHOT_POLICY_SET_SCHEMA_VERSION,
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
    def __init__(
        self,
        request: RetrospectiveDatasetBuildRequest,
        policy_by_capture: dict[str, RetrospectiveSnapshotPolicyMember] | None = None,
    ) -> None:
        self.request = request
        self.policy_by_capture = policy_by_capture or {}
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
        policy = self.policy_by_capture.get(capture_id)
        policy_ref = (
            policy.policy_bundle_ref
            if policy is not None
            else self.request.historical_range_policy_bundle_ref
        )
        policy_hash = (
            policy.policy_bundle_hash
            if policy is not None
            else self.request.label_policy_bundle_hash
        )
        suffix = capture_id.rsplit("-", 1)[-1] if policy is not None else None
        observation_mapping_id = (
            f"observation-mapping-{suffix}" if suffix else "observation-mapping"
        )
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
                historical_range_policy_bundle_ref=policy_ref,
                historical_range_policy_bundle_hash=policy_hash,
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
                            "selected_mapping_id": observation_mapping_id,
                            "selected_mapping_hash": _hash(observation_mapping_id),
                        }
                    ],
                    "label_source_revision_set_id": member.source_revision_set_id,
                    "label_source_revision_set_hash": member.source_revision_set_hash,
                    "label_policy_bundle_id": (
                        policy.policy_bundle_id
                        if policy is not None
                        else self.request.label_policy_bundle_id
                    ),
                    "label_policy_bundle_hash": policy_hash,
                    "policy_component_set_hash": (
                        policy.policy_component_set_hash
                        if policy is not None
                        else self.request.policy_component_set_hash
                    ),
                    "label_as_of_ts": self.request.label_as_of_ts.isoformat(),
                },
                historical_range_policy_bundle_ref=policy_ref,
                historical_range_policy_bundle_hash=policy_hash,
            )
        return common

    def fetchall(self) -> list[dict[str, str]]:
        capture_id = str(self.params[0])
        member = next(
            item
            for item in self.request.captures
            if item.capture_batch_id == capture_id
        )
        if member.capture_purpose == "OBSERVATION_CAPTURE_V1":
            return [
                {
                    "evidence_role": "OBSERVATION_VERSION",
                    "evidence_id": "observation-version",
                    "evidence_content_hash": _hash("observation-version"),
                }
            ]
        suffix = capture_id.rsplit("-", 1)[-1] if capture_id in self.policy_by_capture else None
        label_mapping_id = f"label-mapping-{suffix}" if suffix else "label-mapping"
        return [
            {
                "evidence_role": "SELECTED_LABEL_MAPPING",
                "evidence_id": label_mapping_id,
                "evidence_content_hash": _hash(label_mapping_id),
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


def _snapshot_policy_member(name: str) -> RetrospectiveSnapshotPolicyMember:
    component_hashes = {
        role: _hash(f"{name}:{role}")
        for role in (
            "BARRIER",
            "BENCHMARK",
            "CALENDAR",
            "CASH_RETURN",
            "CORPORATE_ACTION",
            "COST",
            "EXECUTION",
            "MARKET_DATA",
            "TERMINAL",
        )
    }
    component_set_hash = canonical_json_sha256(
        [
            {"component_role": role, "component_hash": component_hashes[role]}
            for role in sorted(component_hashes)
        ]
    )
    policy_hash = _hash(f"policy:{name}")
    return RetrospectiveSnapshotPolicyMember(
        policy_bundle_id=f"policy-{name}",
        policy_bundle_hash=policy_hash,
        policy_bundle_ref=_policy_ref(policy_hash),
        policy_component_hashes=component_hashes,
        policy_component_set_hash=component_set_hash,
    )


def _composite_request() -> tuple[
    RetrospectiveDatasetBuildRequest,
    RetrospectiveSnapshotPolicySet,
    dict[str, RetrospectiveSnapshotPolicyMember],
]:
    policies = {
        "a": _snapshot_policy_member("a"),
        "b": _snapshot_policy_member("b"),
    }
    policy_set = RetrospectiveSnapshotPolicySet.from_members(policies.values())
    aggregate_hash = canonical_json_sha256(policy_set.canonical_payload())
    aggregate_ref = {
        **_policy_ref(aggregate_hash),
        "producer_contract_version": SNAPSHOT_POLICY_SET_SCHEMA_VERSION,
        "payload_schema_version": SNAPSHOT_POLICY_SET_SCHEMA_VERSION,
    }
    scopes = tuple(
        FrozenIdentity(
            identity_id=f"range-scope-{suffix}",
            identity_hash=_hash(f"range-scope-{suffix}"),
        )
        for suffix in policies
    )
    captures = tuple(
        sorted(
            (
                RetrospectiveCaptureSetMember(
                    capture_batch_id=f"capture-{kind}-{suffix}",
                    capture_request_hash=_hash(f"request-{kind}-{suffix}"),
                    capture_receipt_hash=_hash(f"receipt-{kind}-{suffix}"),
                    membership_hash=_hash(f"membership-{kind}-{suffix}"),
                    capture_purpose=purpose,
                    range_lineage_scope_id=f"range-scope-{suffix}",
                    range_lineage_scope_hash=_hash(f"range-scope-{suffix}"),
                    source_revision_set_id="source-revision",
                    source_revision_set_hash=_hash("source-revision"),
                    date_start=date(2026, 7, 1),
                    date_end=date(2026, 7, 1),
                )
                for suffix in policies
                for kind, purpose in (
                    ("label", "LABEL_CAPTURE_V1"),
                    ("observation", "OBSERVATION_CAPTURE_V1"),
                )
            ),
            key=lambda item: item.capture_batch_id,
        )
    )
    payload = _request().model_dump(mode="python")
    payload.update(
        range_lineage_scopes=scopes,
        range_lineage_scope_set_hash=None,
        captures=captures,
        capture_set_hash=None,
        selected_observation_mappings=tuple(
            FrozenIdentity(
                identity_id=f"observation-mapping-{suffix}",
                identity_hash=_hash(f"observation-mapping-{suffix}"),
            )
            for suffix in policies
        ),
        selected_observation_mapping_set_hash=None,
        selected_label_mappings=tuple(
            FrozenIdentity(
                identity_id=f"label-mapping-{suffix}",
                identity_hash=_hash(f"label-mapping-{suffix}"),
            )
            for suffix in policies
        ),
        selected_label_mapping_set_hash=None,
        label_policy_bundle_id=f"ahrpbs_{aggregate_hash[:20]}",
        label_policy_bundle_hash=aggregate_hash,
        historical_range_policy_bundle_ref=aggregate_ref,
        benchmark_policy_hash=policy_set.aggregate_component_hashes["BENCHMARK"],
        cost_policy_hash=policy_set.aggregate_component_hashes["COST"],
        policy_component_set_hash=policy_set.aggregate_component_set_hash,
        build_request_hash=None,
    )
    request = RetrospectiveDatasetBuildRequest.model_validate(payload)
    by_capture = {
        member.capture_batch_id: policies[member.capture_batch_id.rsplit("-", 1)[-1]]
        for member in captures
    }
    return request, policy_set, by_capture


def test_composite_snapshot_policy_admission_preserves_each_capture_policy() -> None:
    request, policy_set, by_capture = _composite_request()

    PostgresDatasetBuildRepository._require_capture_admission(
        _CaptureCursor(request, by_capture),
        request,
        lambda _ref: policy_set.canonical_payload(),
    )


def test_composite_snapshot_policy_admission_requires_exact_artifact_readback() -> None:
    request, _, by_capture = _composite_request()

    with pytest.raises(Exception, match="requires exact artifact readback"):
        PostgresDatasetBuildRepository._require_capture_admission(
            _CaptureCursor(request, by_capture),
            request,
        )


def test_composite_snapshot_policy_admission_requires_complete_member_coverage() -> None:
    request, policy_set, by_capture = _composite_request()
    policy_a = next(
        item for item in policy_set.members if item.policy_bundle_id == "policy-a"
    )
    incomplete = {capture_id: policy_a for capture_id in by_capture}

    with pytest.raises(Exception, match="do not exactly cover the snapshot set"):
        PostgresDatasetBuildRepository._require_capture_admission(
            _CaptureCursor(request, incomplete),
            request,
            lambda _ref: policy_set.canonical_payload(),
        )


def test_composite_snapshot_policy_admission_rejects_tampered_aggregate() -> None:
    request, policy_set, by_capture = _composite_request()
    payload = policy_set.canonical_payload()
    payload["aggregate_component_set_hash"] = _hash("tampered")

    with pytest.raises(Exception, match="artifact is invalid"):
        PostgresDatasetBuildRepository._require_capture_admission(
            _CaptureCursor(request, by_capture),
            request,
            lambda _ref: payload,
        )


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
