from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatchStatus,
    CaptureMembership,
    InMemoryCaptureBatchRepository,
    capture_request_purpose,
    capture_request_schema,
    parse_capture_batch_request_payload,
)
from backend.services.advisory_phase1.label_capture import (
    CandidateCoverageSummary,
    CaptureEvidenceMembershipReference,
    CapturePlanReference,
    LABEL_CAPTURE_BATCH_SCHEMA_VERSION,
    LABEL_CAPTURE_PURPOSE,
    LabelCaptureGap,
    LabelCaptureBatchRequestV2,
    LabelCaptureBinding,
    PlannedLabelDescriptor,
    SelectedObservationMappingReference,
    UniverseCoverageSummary,
    _plan_set_hash,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError


UTC = timezone.utc
NOW = datetime(2026, 7, 13, 9, 0, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64


def _descriptor() -> PlannedLabelDescriptor:
    return PlannedLabelDescriptor(
        canonical_signal_id="advsig-fixture",
        observation_version_id="osv-fixture",
        candidate_stage_evidence_id="advstage-fixture",
        symbol="000001.SZ",
        decision_as_of_trade_date=date(2026, 7, 3),
        horizon_trading_days=1,
        projection="RETURN_GROSS",
        label_key_hash=HASH_A,
    )


def _mapping() -> SelectedObservationMappingReference:
    return SelectedObservationMappingReference(
        selected_mapping_id="som-fixture",
        selected_mapping_hash=HASH_B,
        canonical_signal_id="advsig-fixture",
        terminal_observation_version_id="osv-fixture",
        terminal_observation_content_hash=HASH_C,
        terminal_revision_no=1,
    )


def _binding(*, batch_id: str) -> LabelCaptureBinding:
    return LabelCaptureBinding(
        capture_batch_id=batch_id,
        current_fencing_token=1,
        source_observation_capture_batch_id="observation-capture-fixture",
        source_capture_request_hash=HASH_A,
        source_capture_receipt_hash=HASH_B,
        source_capture_membership_count=1,
        source_capture_membership_hash=HASH_C,
        source_capture_plan_set_count=1,
        source_capture_plan_set_hash=HASH_D,
        source_trace_binding_hash=HASH_E,
        source_control_binding_event_hash=HASH_F,
        phase1_handoff_bundle_hash=HASH_A,
        handoff_readiness_hash=HASH_B,
        admission_scope_id="fixture-scope",
        admission_scope_hash=HASH_C,
        selected_observation_mapping_set_count=1,
        selected_observation_mapping_set_hash=canonical_json_sha256([_mapping().model_dump(mode="json")]),
        label_policy_bundle_id="lpb-fixture",
        label_policy_bundle_hash=HASH_D,
        label_source_revision_set_id="srs-fixture",
        label_source_revision_set_hash=HASH_E,
        label_as_of_ts=NOW,
    )


def _request(*, batch_id: str) -> LabelCaptureBatchRequestV2:
    descriptor = _descriptor()
    return LabelCaptureBatchRequestV2(
        capture_batch_id=batch_id,
        binding=_binding(batch_id=batch_id),
        source_observation_capture_batch_id="observation-capture-fixture",
        source_capture_receipt_hash=HASH_B,
        source_capture_membership_hash=HASH_C,
        source_capture_plan_set_count=1,
        source_capture_plan_set_hash=HASH_D,
        selected_observation_mappings=(_mapping(),),
        label_policy_bundle_id="lpb-fixture",
        label_policy_bundle_hash=HASH_D,
        label_source_revision_set_id="srs-fixture",
        label_source_revision_set_hash=HASH_E,
        label_as_of_ts=NOW,
        planned_labels=(descriptor,),
        planned_label_count=1,
        planned_label_hash=canonical_json_sha256([descriptor.model_dump(mode="json")]),
    )


def test_v2_request_is_explicitly_tagged_and_recovery_excludes_new_batch_identity() -> None:
    first = _request(batch_id="label-capture-1")
    recovery = _request(batch_id="label-capture-2")

    assert first.schema_version == LABEL_CAPTURE_BATCH_SCHEMA_VERSION
    assert first.capture_purpose == LABEL_CAPTURE_PURPOSE
    assert first.capture_request_hash == recovery.capture_request_hash
    assert first.binding.binding_hash != recovery.binding.binding_hash
    assert capture_request_schema(first) == LABEL_CAPTURE_BATCH_SCHEMA_VERSION
    assert capture_request_purpose(first) == LABEL_CAPTURE_PURPOSE


def test_v2_in_memory_state_machine_recovery_and_raw_parser_are_one_pass() -> None:
    clock = [NOW]
    repository = InMemoryCaptureBatchRepository(now_provider=lambda: clock[0])
    first = repository.create(_request(batch_id="label-capture-1"))
    running = repository.acquire(
        capture_batch_id=first.request.capture_batch_id,
        expected_row_version=first.row_version,
        lease_seconds=1,
    )
    clock[0] = NOW + timedelta(seconds=2)
    expired = repository.expire(
        capture_batch_id=running.request.capture_batch_id,
        expected_row_version=running.row_version,
        fencing_token=running.fencing_token,
    )
    recovered = repository.recover(
        request=_request(batch_id="label-capture-2"),
        predecessor_capture_batch_id=expired.request.capture_batch_id,
        expected_predecessor_row_version=expired.row_version,
        predecessor_fencing_token=expired.fencing_token,
    )

    assert expired.status is CaptureBatchStatus.EXPIRED
    assert recovered.capture_attempt_no == 2
    assert recovered.request.capture_request_hash == expired.request.capture_request_hash
    assert repository.create(recovered.request) == recovered
    parsed = parse_capture_batch_request_payload(recovered.request.model_dump(mode="json"))
    assert parsed == recovered.request
    malformed = recovered.request.model_dump(mode="json")
    malformed["capture_purpose"] = "OBSERVATION_CAPTURE_V1"
    with pytest.raises(ValueError, match="requires LABEL_CAPTURE_V1"):
        parse_capture_batch_request_payload(malformed)


def test_v2_capture_membership_remains_fenced_and_fail_closed() -> None:
    repository = InMemoryCaptureBatchRepository(now_provider=lambda: NOW)
    planned = repository.create(_request(batch_id="label-capture-1"))
    running = repository.acquire(
        capture_batch_id=planned.request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=60,
    )
    with pytest.raises(SourceLedgerError):
        repository.add_membership(
            capture_batch_id=running.request.capture_batch_id,
            expected_row_version=running.row_version,
            fencing_token=running.fencing_token + 1,
            membership=CaptureMembership(evidence_role="fixture", evidence_id="one", evidence_content_hash=HASH_F),
        )
    with_membership = repository.add_membership(
        capture_batch_id=running.request.capture_batch_id,
        expected_row_version=running.row_version,
        fencing_token=running.fencing_token,
        membership=CaptureMembership(evidence_role="fixture", evidence_id="one", evidence_content_hash=HASH_F),
    )
    complete = repository.complete(
        capture_batch_id=with_membership.request.capture_batch_id,
        expected_row_version=with_membership.row_version,
        fencing_token=with_membership.fencing_token,
    )
    assert complete.status is CaptureBatchStatus.COMPLETE


def test_v2_contract_rejects_nonresearch_scope_and_planned_set_drift() -> None:
    request = _request(batch_id="label-capture-1")
    nonresearch = request.model_dump(mode="python", exclude={"capture_request_hash"})
    nonresearch["execution_origin"] = "LIVE"
    with pytest.raises(ValueError, match="historical advisory research"):
        LabelCaptureBatchRequestV2.model_validate(nonresearch)

    planned_drift = request.model_dump(mode="python", exclude={"capture_request_hash"})
    planned_drift["planned_label_count"] = 0
    with pytest.raises(ValueError, match="planned label count"):
        LabelCaptureBatchRequestV2.model_validate(planned_drift)

    binding_drift = _binding(batch_id="label-capture-1").model_dump(mode="python")
    binding_drift["admission_scope_id"] = "other-scope"
    with pytest.raises(ValueError, match="binding_hash"):
        LabelCaptureBinding.model_validate(binding_drift)


def test_capture_contract_models_reject_duplicate_or_noncanonical_content() -> None:
    descriptor = _descriptor()
    with pytest.raises(ValueError, match="owner_type"):
        PlannedLabelDescriptor.model_validate({**descriptor.model_dump(mode="python"), "owner_type": "UNIVERSE"})
    with pytest.raises(ValueError, match="coverage counts"):
        CandidateCoverageSummary(
            observation_count=1,
            included_count=0,
            excluded_count=0,
            empty_observation_count=1,
            planned_label_count=0,
            maturity_counts={"MATURED": -1},
        )
    with pytest.raises(ValueError, match="sorted and unique"):
        LabelCaptureGap(
            planned_identity={"label_key_hash": HASH_A},
            reason_code="fixture",
            observed_at=NOW,
            evidence_hashes=(HASH_B, HASH_A),
        )
    membership = CaptureEvidenceMembershipReference(
        evidence_role="fixture",
        evidence_id="fixture-id",
        evidence_content_hash=HASH_A,
    )
    assert membership.content_key == ("fixture", "fixture-id")
    assert membership.canonical_identity()["evidence_content_hash"] == HASH_A

    request = _request(batch_id="label-capture-1")
    duplicate = _mapping().model_copy(
        update={"selected_mapping_id": "som-other", "selected_mapping_hash": HASH_C}
    )
    payload = request.model_dump(mode="python", exclude={"capture_request_hash"})
    payload["selected_observation_mappings"] = (payload["selected_observation_mappings"][0], duplicate)
    with pytest.raises(ValueError, match="duplicate canonical signals"):
        LabelCaptureBatchRequestV2.model_validate(payload)


def test_capture_contract_negative_identity_branches_are_fail_closed() -> None:
    with pytest.raises(ValueError, match="lowercase sha256"):
        CaptureEvidenceMembershipReference(
            evidence_role="fixture",
            evidence_id="fixture",
            evidence_content_hash="G" * 64,
        )
    with pytest.raises(ValueError, match="explicit timezone"):
        LabelCaptureGap(
            planned_identity={"label_key_hash": HASH_A},
            reason_code="fixture",
            observed_at=datetime(2026, 7, 13, 9, 0),
        )

    duplicate_plan = CapturePlanReference(
        selection_run_id="selection-run",
        package_id="package",
        manifest_sha256=HASH_A,
        plan_hash=HASH_B,
    )
    with pytest.raises(ValueError, match="duplicate identities"):
        _plan_set_hash((duplicate_plan, duplicate_plan))

    candidate_coverage = CandidateCoverageSummary(
        observation_count=1,
        included_count=1,
        excluded_count=0,
        empty_observation_count=0,
        planned_label_count=1,
    )
    with pytest.raises(ValueError, match="candidate coverage content_hash"):
        CandidateCoverageSummary.model_validate(
            {**candidate_coverage.model_dump(mode="python"), "content_hash": HASH_A}
        )
    with pytest.raises(ValueError, match="coverage counts"):
        UniverseCoverageSummary(
            frozen_constituent_count=1,
            planned_row_count=1,
            raw_row_count=0,
            denominator_count_by_projection={"RETURN_GROSS": -1},
        )
    universe_coverage = UniverseCoverageSummary(
        frozen_constituent_count=1,
        planned_row_count=1,
        raw_row_count=1,
    )
    with pytest.raises(ValueError, match="universe coverage content_hash"):
        UniverseCoverageSummary.model_validate(
            {**universe_coverage.model_dump(mode="python"), "content_hash": HASH_A}
        )
    gap = LabelCaptureGap(
        planned_identity={"label_key_hash": HASH_A},
        reason_code="fixture",
        observed_at=NOW,
    )
    with pytest.raises(ValueError, match="gap_hash"):
        LabelCaptureGap.model_validate({**gap.model_dump(mode="python"), "gap_hash": HASH_A})


@pytest.mark.parametrize(
    ("mutator", "message"),
    (
        (lambda payload: payload.update({"schema_version": "wrong"}), "schema or purpose"),
        (
            lambda payload: payload.update({"binding": _binding(batch_id="other-batch")}),
            "reference this batch",
        ),
        (
            lambda payload: payload.update({"source_capture_receipt_hash": HASH_A}),
            "does not match its binding",
        ),
        (
            lambda payload: payload.update(
                {
                    "selected_observation_mappings": (
                        SelectedObservationMappingReference.model_validate(
                            {
                                **payload["selected_observation_mappings"][0],
                                "selected_mapping_hash": HASH_A,
                            }
                        ),
                    )
                }
            ),
            "mapping set",
        ),
        (
            lambda payload: payload.update(
                {"planned_labels": (payload["planned_labels"][0], payload["planned_labels"][0])}
            ),
            "unique signal",
        ),
        (lambda payload: payload.update({"capture_request_hash": HASH_A}), "capture_request_hash"),
    ),
)
def test_v2_request_rejects_each_identity_boundary(mutator, message: str) -> None:
    payload = _request(batch_id="label-capture-negative").model_dump(
        mode="python",
        exclude={"capture_request_hash"},
    )
    mutator(payload)
    with pytest.raises(ValueError, match=message):
        LabelCaptureBatchRequestV2.model_validate(payload)
