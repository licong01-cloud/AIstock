"""Shared R4 bridge fixtures retained after duplicate aggregate scenarios were removed."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeBridgeResultStatus,
    HistoricalRangeDatasetBridgeReceiptV1,
    HistoricalRangeDatasetBridgeRequestV1,
    HistoricalRangeLineageIdentity,
    HistoricalRangeOperationStatus,
    HistoricalRangeOutcomePolicyBundleV1,
    REASON_DATASET_BRIDGE_VALID_EMPTY,
)
from backend.services.advisory_phase1.capture_foundation import RetrospectiveObservationCapturePlan
from backend.services.advisory_phase1.observation_capture import (
    materialize_retrospective_observation_row_bundle,
    retrospective_observation_payload,
)
from backend.services.advisory_phase1.outcome_engine import OutcomeOwner, OwnerType
from backend.services.advisory_phase1.retrospective_contracts import (
    HistoricalRangeArtifactReference,
    HistoricalRangeCaptureScope,
    HistoricalRangeLineageProjection,
)
from backend.services.advisory_phase1.retrospective_selector import RETROSPECTIVE_SELECTOR_POLICY_HASH


def _ref(kind: HistoricalRangeArtifactKind, char: str) -> HistoricalRangeArtifactRefV1:
    digest = char * 64
    namespace = {
        HistoricalRangeArtifactKind.REQUEST: "requests",
        HistoricalRangeArtifactKind.FROZEN_PROGRAM: "frozen-programs",
        HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT: "candidate-artifacts",
        HistoricalRangeArtifactKind.OUTCOME: "outcomes",
        HistoricalRangeArtifactKind.SUMMARY: "summaries",
        HistoricalRangeArtifactKind.DAY_RECEIPT: "day-receipts",
    }[kind]
    return HistoricalRangeArtifactRefV1(
        artifact_kind=kind,
        relative_path=f"{namespace}/{digest}.json",
        producer_contract_version="test_v1",
        payload_schema_version="test_v1",
        semantic_content_hash=digest,
        payload_sha256=digest,
        file_sha256=digest,
    )


def _request(*, candidate_refs=(), outcome_refs=(), policy_ref=None, policy_components=None) -> HistoricalRangeDatasetBridgeRequestV1:
    policy_ref = policy_ref or _ref(HistoricalRangeArtifactKind.REQUEST, "a")
    components = policy_components or {
        role: character * 64
        for role, character in zip(
            ("BARRIER", "BENCHMARK", "CALENDAR", "CASH_RETURN", "CORPORATE_ACTION", "COST", "EXECUTION", "MARKET_DATA", "TERMINAL"),
            "abcdefabc",
            strict=True,
        )
    }
    return HistoricalRangeDatasetBridgeRequestV1(
        batch_id="batch-1",
        range_run_ids=("run-1",),
        successful_day_refs=(_ref(HistoricalRangeArtifactKind.DAY_RECEIPT, "d"),),
        candidate_refs=candidate_refs,
        outcome_refs=outcome_refs,
        requested_horizons=(5,),
        requested_maturity_statuses=("COMPLETE",),
        policy_bundle_refs=(policy_ref,),
        policy_component_hashes={policy_ref.payload_sha256: components},
        canonical_signal_dedup_policy_hash="1" * 64,
        retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
        dataset_schema_hash="2" * 64,
        builder_hash="3" * 64,
        writer_hash="4" * 64,
        partition_policy_hash="5" * 64,
        compression_config_hash="6" * 64,
        artifact_root_identity_hash="7" * 64,
        operation_idempotency_key="bridge-1",
        expected_batch_row_version=1,
    )


def test_bridge_request_rejects_duplicate_candidate_evidence() -> None:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "b")
    payload = _request().model_dump(mode="python", exclude={"request_hash"})
    payload["candidate_refs"] = (candidate_ref, candidate_ref)
    with pytest.raises(ValueError, match="duplicate-free|sorted"):
        HistoricalRangeDatasetBridgeRequestV1.model_validate(payload)


class _OperationRepository:
    def __init__(self, *, initial=None) -> None:
        self.operation = dict(initial) if initial is not None else None
        self.transitions, self.attempts, self.expired_attempts = [], [], []

    def get_or_create_operation(self, request):
        if self.operation is not None:
            return dict(self.operation), True
        self.operation = {
            "operation_id": request.operation_id,
            "batch_id": request.batch_id,
            "operation_type": request.operation_type.value,
            "operation_idempotency_key": request.operation_idempotency_key,
            "request_payload_sha256": request.request_payload_sha256,
            "expected_row_version": request.expected_row_version,
            "status": HistoricalRangeOperationStatus.QUEUED.value,
            "row_version": 1,
            "attempt_no": 0,
            "fencing_token": 0,
            "stable_keyset_cursor_json": None,
            "result_ref": None,
            "result_status": None,
        }
        return dict(self.operation), False

    def transition_operation(self, **kwargs):
        assert kwargs["expected_row_version"] == self.operation["row_version"]
        if kwargs.get("expired_attempt") is not None:
            self.expired_attempts.append(kwargs["expired_attempt"])
        if kwargs.get("attempt") is not None:
            self.attempts.append(kwargs["attempt"])
        self.transitions.append(dict(kwargs))
        self.operation.update(
            status=kwargs["target_status"].value,
            row_version=self.operation["row_version"] + 1,
            attempt_no=kwargs["attempt_no"],
            worker_id=kwargs.get("worker_id"),
            lease_token=kwargs.get("lease_token"),
            lease_expires_at=kwargs.get("lease_expires_at"),
            fencing_token=kwargs.get("fencing_token", self.operation.get("fencing_token")),
            stable_keyset_cursor_json=kwargs.get("stable_keyset_cursor_json", self.operation.get("stable_keyset_cursor_json")),
            result_ref=(kwargs["result_ref"].model_dump(mode="json") if kwargs.get("result_ref") is not None else self.operation.get("result_ref")),
            result_status=kwargs.get("result_status", self.operation.get("result_status")),
            started_at=self.operation.get("started_at") or kwargs.get("started_at"),
        )
        return dict(self.operation)


class _OperationBridge:
    def __init__(self, *, store: HistoricalRangeArtifactStore, effects=()) -> None:
        self.store, self.effects, self.calls = store, list(effects), 0

    def build(self, *, operation_id, request, resolved_request_hash, heartbeat):
        self.calls += 1
        heartbeat("INPUT_RESOLVED")
        if self.effects:
            effect = self.effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
        return self._publish(operation_id, request, resolved_request_hash, HistoricalRangeBridgeResultStatus.VALID_EMPTY, (REASON_DATASET_BRIDGE_VALID_EMPTY,))

    def publish_failed_receipt(self, *, operation_id, request, resolved_request_hash, reason_code, result_status=HistoricalRangeBridgeResultStatus.FAILED):
        return self._publish(operation_id, request, resolved_request_hash, result_status, (reason_code,))

    def _publish(self, operation_id, request, resolved_request_hash, status, reason_codes):
        bridge_ref = self.store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.DATASET_BRIDGE,
            producer_contract_version="test_bridge_v1",
            payload_schema_version="test_bridge_v1",
            resolved_request_hash=resolved_request_hash,
            payload={"operation_id": operation_id, "request_hash": request.request_hash, "result_status": status.value},
        ).ref
        receipt = HistoricalRangeDatasetBridgeReceiptV1(
            operation_id=operation_id,
            request_hash=str(request.request_hash),
            result_status=status,
            observation_count=0,
            label_count=0,
            canonical_signal_count=0,
            range_lineage_count=0,
            retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
            bridge_artifact_ref=bridge_ref,
            reason_codes=reason_codes,
        )
        receipt_ref = self.store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT,
            producer_contract_version="test_bridge_v1",
            payload_schema_version=receipt.schema_version,
            resolved_request_hash=resolved_request_hash,
            payload=receipt.model_dump(mode="json"),
            upstream_refs=(bridge_ref,),
        ).ref
        return receipt, receipt_ref


def _bridge_projection(*, lineage: HistoricalRangeLineageIdentity, policy_ref: HistoricalRangeArtifactRefV1, policy: HistoricalRangeOutcomePolicyBundleV1):
    phase1_lineage = HistoricalRangeLineageProjection.model_validate(lineage.model_dump(mode="json"))
    scope = HistoricalRangeCaptureScope(
        historical_range_request_ref=phase1_lineage.historical_range_request_ref,
        historical_range_frozen_program_ref=phase1_lineage.historical_range_frozen_program_ref,
        range_run_id=lineage.range_run_id,
        historical_range_policy_bundle_ref=HistoricalRangeArtifactReference.model_validate(policy_ref.model_dump(mode="json")),
        historical_range_policy_bundle_hash=str(policy.policy_bundle_hash),
        selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
        signal_source_revision_set_id="source-set-1",
        signal_source_revision_set_hash=lineage.signal_source_revision_set_hash,
        oos_interval_hash=lineage.oos_interval_hash,
    )
    signal_scope_hash = canonical_json_sha256({"signal": "candidate-1", "policy": policy.policy_bundle_hash})
    stages = {
        name: {"stage": name, "status": "COMPLETE", "input_count": 1, "output_count": 1, "excluded_count": 0, "reason_codes": ()}
        for name in ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")
    }
    fact = {
        "candidate_id": "candidate-1",
        "day_run_id": lineage.range_day_run_id,
        "symbol": "000001.SZ",
        "membership_status": "INCLUDED",
        "alpha_raw_rank": 1,
        "alpha_raw_score": "0.9",
        "hmm_adjusted_rank": 1,
        "hmm_adjusted_score": "0.9",
        "risk_policy_adjusted_rank": 1,
        "risk_policy_adjusted_score": "0.9",
        "selection_effective_rank": 1,
        "selection_effective_score": "0.9",
        "advisory_model_rank": None,
        "advisory_model_score": None,
        "component_lineage_json": {"component": "alpha-1"},
        "component_lineage_hash": canonical_json_sha256({"component": "alpha-1"}),
    }
    plan = RetrospectiveObservationCapturePlan(
        canonical_signal_id=f"acs_{signal_scope_hash[:20]}",
        symbol="000001.SZ",
        decision_as_of_trade_date=date(2026, 7, 3),
        selection_as_of_trade_date=date(2026, 7, 3),
        target_trade_date=date(2026, 7, 6),
        decision_cutoff_ts=datetime(2026, 7, 3, 7, tzinfo=UTC),
        alpha_mode="single_alpha",
        selection_runtime_semantics_hash="5" * 64,
        package_effective_config_hash="6" * 64,
        calendar_version=policy.calendar_version,
        calendar_hash=policy.calendar_hash,
        stable_signal_semantics_hash="7" * 64,
        canonical_signal_scope_hash=signal_scope_hash,
        lineage=phase1_lineage,
        range_scope=scope,
        signal_source_revision_set_id=scope.signal_source_revision_set_id,
        signal_source_revision_set_hash=scope.signal_source_revision_set_hash,
        range_signal_context_hash="8" * 64,
        evidence_bundle_hash="9" * 64,
        stage_payload_hash=canonical_json_sha256(stages),
        runtime_profile_version_id="runtime-1",
        runtime_profile_version_hash="a" * 64,
        hmm_snapshot_status="NOT_APPLICABLE",
        risk_policy_hash="b" * 64,
        universe_policy_hash="c" * 64,
        symbol_normalization_policy_hash="d" * 64,
        evidence_available_at=datetime(2026, 7, 3, 8, tzinfo=UTC),
        selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
    )
    rows = materialize_retrospective_observation_row_bundle(plan=plan, stage_payload=stages, candidate_fact=fact, created_by_capture_batch_id="test-capture")
    selection_stage = next(item for item in rows.stage_evidence_rows if item["stage"] == "selection_effective")
    owner = OutcomeOwner(
        owner_type=OwnerType.CANDIDATE,
        owner_key="candidate-1",
        canonical_signal_id=plan.canonical_signal_id,
        observation_version_id=rows.observation_version["observation_version_id"],
        candidate_stage_evidence_id=selection_stage["stage_evidence_id"],
        symbol="000001.SZ",
        decision_as_of_trade_date=date(2026, 7, 3),
    )
    payload = retrospective_observation_payload(
        plan=plan,
        candidate_fact=fact,
        stage_evidence_bundle_hash=canonical_json_sha256([item["content_hash"] for item in rows.stage_evidence_rows]),
    )
    return plan, stages, fact, owner, payload
