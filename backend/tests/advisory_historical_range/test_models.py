from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from pydantic import ValidationError

from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactEnvelopeV1,
    HistoricalRangeArtifactKind,
    HistoricalRangeDayAttemptV1,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeListAction,
    HistoricalRangeListItemFactV1,
    HistoricalRangeResearchBatchRequestV1,
    HistoricalRangeSourceRevisionRefV1,
    ResearchProgramSpecV1,
    derive_day_run_id,
    derive_episode_id,
)
from backend.tests.advisory_historical_range.conftest import (
    artifact_ref,
    digest,
    frozen_program,
    research_spec,
    resolved_request,
)


def test_user_and_resolved_hashes_exclude_request_metadata_and_display_name() -> None:
    first = resolved_request()
    renamed = research_spec(name="renamed only")
    second = resolved_request(
        specs=(renamed,),
        client_key="another-client-key",
        request_id="another-request-id",
        requested_at=datetime(2026, 7, 20, 8, 0, tzinfo=UTC),
    )

    assert first.request.user_request_semantic_hash == second.request.user_request_semantic_hash
    assert first.request_payload_sha256 == second.request_payload_sha256
    assert first.batch_id == second.batch_id


def test_business_config_changes_research_identity_and_request_hash() -> None:
    first = resolved_request(specs=(research_spec(target_count=5),))
    second = resolved_request(specs=(research_spec(target_count=6),))

    assert first.frozen_programs[0].research_program_id != second.frozen_programs[0].research_program_id
    assert first.request.user_request_semantic_hash != second.request.user_request_semantic_hash
    assert first.request_payload_sha256 != second.request_payload_sha256


def test_client_cannot_supply_package_derived_fields_or_review_schedule() -> None:
    payload = research_spec().model_dump(mode="python")
    payload["alpha_mode"] = "multi_alpha"
    with pytest.raises(ValidationError):
        ResearchProgramSpecV1.model_validate(payload)

    request_payload = {
        "client_idempotency_key": "key",
        "program_specs": [research_spec().model_dump(mode="json")],
        "start_trade_date": "2026-06-01",
        "end_trade_date": "2026-06-03",
        "review_schedule": "daily",
    }
    with pytest.raises(ValidationError):
        HistoricalRangeResearchBatchRequestV1.model_validate(request_payload)


def test_date_plan_requires_frozen_ordered_range_and_completed_watermark() -> None:
    with pytest.raises(ValidationError):
        resolved_request(
            trade_dates=(date(2026, 6, 2), date(2026, 6, 1)),
        )


def test_artifact_envelope_hashes_payload_and_lineage_canonically() -> None:
    resolved = resolved_request()
    envelope = HistoricalRangeArtifactEnvelopeV1(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="phase1r_r1",
        payload_schema_version=resolved.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        source_revision_refs=(
            HistoricalRangeSourceRevisionRefV1(revision_id="revision-b", revision_hash=digest("revision-b")),
            HistoricalRangeSourceRevisionRefV1(revision_id="revision-a", revision_hash=digest("revision-a")),
        ),
        payload=resolved.model_dump(mode="json"),
    )

    assert tuple(item.revision_id for item in envelope.source_revision_refs) == ("revision-a", "revision-b")
    assert envelope.payload_sha256 == digest(resolved.model_dump(mode="json"))
    assert len(envelope.semantic_content_hash) == 64


def test_deterministic_day_and_episode_ids_do_not_depend_on_wall_clock() -> None:
    day_id = derive_day_run_id("range-1", date(2026, 6, 2), 2)
    assert day_id == derive_day_run_id("range-1", date(2026, 6, 2), 2)
    episode_id = derive_episode_id("range-1", "000001.SZ", date(2026, 6, 2), 1)
    assert episode_id == derive_episode_id("range-1", "000001.sz", date(2026, 6, 2), 1)


def test_watch_is_non_active_and_has_no_episode() -> None:
    item = HistoricalRangeListItemFactV1(
        list_item_id="item-1",
        list_version_id="list-1",
        symbol="000001.sz",
        action=HistoricalRangeListAction.WATCH,
        rank=8,
        score=0.3,
        rule_guidance_json={"source": "rule_default"},
        execution_status="NOT_DUE",
    )
    assert item.symbol == "000001.SZ"
    assert item.episode_id is None
    assert len(item.evidence_hash) == 64

    with pytest.raises(ValidationError):
        HistoricalRangeListItemFactV1(
            list_item_id="item-2",
            list_version_id="list-1",
            symbol="000002.SZ",
            action=HistoricalRangeListAction.WATCH,
            episode_id="episode-1",
            rule_guidance_json={"source": "rule_default"},
            execution_status="NOT_DUE",
        )


def test_request_rejects_naive_requested_at() -> None:
    spec = research_spec()
    with pytest.raises(ValidationError):
        HistoricalRangeResearchBatchRequestV1(
            client_idempotency_key="key",
            program_specs=(spec,),
            start_trade_date=date(2026, 6, 1),
            end_trade_date=date(2026, 6, 3),
            requested_at=datetime.now() + timedelta(seconds=1),
        )


def test_resolved_request_requires_one_warmup_contract_per_program() -> None:
    resolved = resolved_request()
    invalid_plan = resolved.date_plan.model_copy(update={"per_program_input_warmup_ranges": {"wrong-program": {}}})
    with pytest.raises(ValidationError):
        resolved.model_copy(update={"date_plan": invalid_plan}).model_validate(
            resolved.model_copy(update={"date_plan": invalid_plan}).model_dump()
        )


def test_fact_hash_is_derived_and_cannot_be_silently_overridden() -> None:
    item = HistoricalRangeListItemFactV1(
        list_item_id="item-derived",
        list_version_id="list-1",
        symbol="000001.SZ",
        action=HistoricalRangeListAction.WATCH,
        rule_guidance_json={"source": "rule_default"},
        execution_status="NOT_DUE",
    )
    assert len(item.evidence_hash) == 64
    with pytest.raises(ValidationError):
        HistoricalRangeListItemFactV1(
            list_item_id="item-tampered",
            list_version_id="list-1",
            symbol="000001.SZ",
            action=HistoricalRangeListAction.WATCH,
            rule_guidance_json={"source": "rule_default"},
            execution_status="NOT_DUE",
            evidence_hash=digest("not the item payload"),
        )


def test_non_running_day_attempt_requires_receipt_and_can_reference_partial_candidate() -> None:
    attempt = HistoricalRangeDayAttemptV1(
        attempt_id="attempt-1",
        day_run_id="day-1",
        attempt_no=1,
        worker_id="worker-1",
        lease_token="lease-1",
        fencing_token=1,
        status="WAITING_INPUT",
        input_hash=digest("attempt-input"),
        candidate_artifact_ref=artifact_ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "candidate-artifact"),
        attempt_receipt_ref=artifact_ref(HistoricalRangeArtifactKind.DAY_RECEIPT, "attempt-receipt"),
        started_at=datetime(2026, 7, 19, 1, 0, tzinfo=UTC),
        finished_at=datetime(2026, 7, 19, 1, 1, tzinfo=UTC),
    )
    assert attempt.status == "WAITING_INPUT"

    with pytest.raises(ValidationError):
        HistoricalRangeDayAttemptV1(
            attempt_id="attempt-2",
            day_run_id="day-1",
            attempt_no=2,
            worker_id="worker-1",
            lease_token="lease-2",
            fencing_token=2,
            status="RETRYABLE_FAILED",
            input_hash=digest("attempt-input-2"),
            started_at=datetime(2026, 7, 19, 1, 2, tzinfo=UTC),
            finished_at=datetime(2026, 7, 19, 1, 3, tzinfo=UTC),
        )


def test_frozen_projection_and_warmup_contracts_reject_untyped_or_mismatched_payloads() -> None:
    frozen = frozen_program(research_spec())
    payload = frozen.model_dump(mode="json")
    payload["admitted_package_projection"] = {"garbage": True}
    payload["admitted_package_projection_hash"] = digest(payload["admitted_package_projection"])
    with pytest.raises(ValidationError):
        HistoricalRangeFrozenProgramV1.model_validate(payload)

    resolved = resolved_request()
    plan_payload = resolved.date_plan.model_dump(mode="json")
    program_id = resolved.frozen_programs[0].research_program_id
    plan_payload["per_program_input_warmup_ranges"][program_id]["components"][0]["lookback_contract_hash"] = digest(
        "different-lookback"
    )
    plan_payload.pop("per_program_input_warmup_ranges_hash", None)
    plan_payload.pop("date_plan_hash", None)
    with pytest.raises(ValidationError):
        resolved.model_copy(
            update={"date_plan": resolved.date_plan.__class__.model_validate(plan_payload)}
        ).__class__.model_validate(
            resolved.model_copy(
                update={"date_plan": resolved.date_plan.__class__.model_validate(plan_payload)}
            ).model_dump(mode="json")
        )


def test_artifact_kind_identity_is_not_optional_for_candidate_artifacts() -> None:
    resolved = resolved_request()
    with pytest.raises(ValidationError):
        HistoricalRangeArtifactEnvelopeV1(
            artifact_kind=HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
            producer_contract_version="phase1r_r1",
            payload_schema_version="candidate_v1",
            resolved_request_hash=resolved.request_payload_sha256,
            payload={"candidates": []},
        )

    with pytest.raises(ValidationError):
        HistoricalRangeArtifactEnvelopeV1(
            artifact_kind=HistoricalRangeArtifactKind.REQUEST,
            producer_contract_version="phase1r_r1",
            payload_schema_version=resolved.schema_version,
            resolved_request_hash=resolved.request_payload_sha256,
            source_revision_refs=(
                HistoricalRangeSourceRevisionRefV1(revision_id="same", revision_hash=digest("first")),
                HistoricalRangeSourceRevisionRefV1(revision_id="same", revision_hash=digest("second")),
            ),
            payload=resolved.model_dump(mode="json"),
        )
