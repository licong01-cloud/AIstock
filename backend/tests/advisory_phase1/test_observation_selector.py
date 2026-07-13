"""Phase 1C-2 terminal-first fixture observation selector contracts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from backend.services.advisory_phase1.observation_selector import (
    REASON_OBSERVATION_CAPABILITY_UNAVAILABLE,
    REASON_OBSERVATION_CAPTURE_RECORD_INVALID,
    REASON_OBSERVATION_EXACT_VERSION_MISMATCH,
    REASON_OBSERVATION_MAPPING_CONFLICT,
    REASON_OBSERVATION_TERMINAL_CONFLICT,
    REASON_OBSERVATION_VERSION_CHAIN_INVALID,
    FixtureObservationVersion,
    FixtureObservationVersionSelector,
    InMemoryFixtureObservationRepository,
    InMemorySelectedObservationMappingRepository,
    ObservationSelectionPolicy,
    ObservationSelectionRequest,
    ObservationSelectionStatus,
    ObservationStatus,
    build_fixture_observation_version,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError


UTC = timezone.utc
AVAILABLE_AT = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64


def _version(
    *,
    revision_no: int = 1,
    predecessor: str | None = None,
    evidence_available_at: datetime = AVAILABLE_AT,
    observation_status: ObservationStatus = ObservationStatus.COMPLETE,
    capability: str = "CAPABILITY_FULL",
    source_hash: str = HASH_A,
):
    return build_fixture_observation_version(
        canonical_signal_id="acs-fixture",
        observation_revision_no=revision_no,
        supersedes_observation_version_id=predecessor,
        evidence_available_at=evidence_available_at,
        admission_scope_id="scope-fixture",
        admission_scope_hash=HASH_B,
        handoff_readiness_hash=HASH_C,
        signal_source_revision_set_id="srs-fixture",
        signal_source_revision_set_hash=source_hash,
        observation_status=observation_status,
        capability=capability,
        stage_content_hashes=(HASH_D, HASH_E),
    )


def _request(
    *,
    policy: ObservationSelectionPolicy = ObservationSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
    cutoff: datetime = AVAILABLE_AT,
    explicit_version_id: str | None = None,
    required_status: ObservationStatus = ObservationStatus.COMPLETE,
    capability: str = "CAPABILITY_FULL",
    source_hash: str = HASH_A,
) -> ObservationSelectionRequest:
    return ObservationSelectionRequest(
        selection_policy=policy,
        canonical_signal_id="acs-fixture",
        requested_source_cutoff=cutoff,
        required_observation_status=required_status,
        required_capability=capability,
        admission_scope_id="scope-fixture",
        admission_scope_hash=HASH_B,
        handoff_readiness_hash=HASH_C,
        signal_source_revision_set_hash=source_hash,
        explicit_observation_version_id=explicit_version_id,
    )


def test_exact_and_latest_policies_select_the_unique_as_of_terminal() -> None:
    first = _version()
    second = _version(
        revision_no=2,
        predecessor=first.observation_version_id,
        evidence_available_at=AVAILABLE_AT + timedelta(hours=1),
    )
    selector = FixtureObservationVersionSelector()

    exact = selector.select(
        request=_request(
            policy=ObservationSelectionPolicy.EXACT_REVISION_V1,
            cutoff=AVAILABLE_AT,
            explicit_version_id=first.observation_version_id,
        ),
        observation_versions=(first, second),
    )
    latest = selector.select(
        request=_request(cutoff=second.evidence_available_at),
        observation_versions=(first, second),
    )

    assert exact.selection_status is ObservationSelectionStatus.SELECTED
    assert exact.terminal_observation_version_id == first.observation_version_id
    assert latest.selection_status is ObservationSelectionStatus.SELECTED
    assert latest.terminal_observation_version_id == second.observation_version_id


def test_future_correction_does_not_pollute_latest_as_of_selection() -> None:
    first = _version()
    future = _version(
        revision_no=2,
        predecessor=first.observation_version_id,
        evidence_available_at=AVAILABLE_AT + timedelta(days=1),
    )
    result = FixtureObservationVersionSelector().select(
        request=_request(cutoff=AVAILABLE_AT + timedelta(minutes=1)),
        observation_versions=(first, future),
    )

    assert result.selection_status is ObservationSelectionStatus.SELECTED
    assert result.terminal_observation_version_id == first.observation_version_id


def test_terminal_capability_failure_is_unavailable_and_never_falls_back_to_old_complete() -> None:
    first = _version()
    partial = _version(
        revision_no=2,
        predecessor=first.observation_version_id,
        evidence_available_at=AVAILABLE_AT + timedelta(minutes=1),
        observation_status=ObservationStatus.PARTIAL,
    )
    result = FixtureObservationVersionSelector().select(
        request=_request(cutoff=partial.evidence_available_at),
        observation_versions=(first, partial),
    )

    assert result.selection_status is ObservationSelectionStatus.UNAVAILABLE
    assert result.terminal_observation_version_id == partial.observation_version_id
    assert result.reason_codes == (REASON_OBSERVATION_CAPABILITY_UNAVAILABLE,)


def test_exact_policy_rejects_old_revision_when_as_of_terminal_is_newer() -> None:
    first = _version()
    second = _version(
        revision_no=2,
        predecessor=first.observation_version_id,
        evidence_available_at=AVAILABLE_AT + timedelta(minutes=1),
    )
    result = FixtureObservationVersionSelector().select(
        request=_request(
            policy=ObservationSelectionPolicy.EXACT_REVISION_V1,
            cutoff=second.evidence_available_at,
            explicit_version_id=first.observation_version_id,
        ),
        observation_versions=(first, second),
    )

    assert result.selection_status is ObservationSelectionStatus.CONFLICT
    assert result.terminal_observation_version_id == second.observation_version_id
    assert result.reason_codes == (REASON_OBSERVATION_EXACT_VERSION_MISMATCH,)


def test_terminal_identity_mismatch_is_conflict_not_a_current_source_lookup() -> None:
    version = _version(source_hash=HASH_A)
    result = FixtureObservationVersionSelector().select(
        request=_request(source_hash=HASH_D),
        observation_versions=(version,),
    )

    assert result.selection_status is ObservationSelectionStatus.CONFLICT
    assert result.reason_codes == (REASON_OBSERVATION_TERMINAL_CONFLICT,)


def test_invalid_chain_and_duplicate_terminal_fail_closed() -> None:
    first = _version()
    malformed = _version(
        revision_no=2,
        predecessor=first.observation_version_id,
        evidence_available_at=AVAILABLE_AT + timedelta(minutes=1),
    ).model_copy(update={"supersedes_observation_version_id": "wrong-predecessor"})
    result = FixtureObservationVersionSelector().select(
        request=_request(cutoff=AVAILABLE_AT + timedelta(minutes=1)),
        observation_versions=(first, malformed),
    )
    assert result.selection_status is ObservationSelectionStatus.CONFLICT
    assert result.reason_codes == (REASON_OBSERVATION_VERSION_CHAIN_INVALID,)

    duplicate = _version().model_copy(update={"observation_content_hash": HASH_E, "observation_version_id": "osv-duplicate"})
    duplicate_result = FixtureObservationVersionSelector().select(
        request=_request(),
        observation_versions=(first, duplicate),
    )
    assert duplicate_result.selection_status is ObservationSelectionStatus.CONFLICT
    assert duplicate_result.reason_codes == (REASON_OBSERVATION_TERMINAL_CONFLICT,)


def test_local_repository_is_append_only_and_rejects_a_revision_fork() -> None:
    repository = InMemoryFixtureObservationRepository()
    first = _version()
    second = _version(
        revision_no=2,
        predecessor=first.observation_version_id,
        evidence_available_at=AVAILABLE_AT + timedelta(minutes=1),
    )
    assert repository.append(first) == first
    assert repository.append(first) == first
    assert repository.append(second) == second
    assert repository.for_signal("acs-fixture") == (first, second)

    fork = _version(
        revision_no=3,
        predecessor=first.observation_version_id,
        evidence_available_at=AVAILABLE_AT + timedelta(minutes=2),
    )
    with pytest.raises(SourceLedgerError, match=REASON_OBSERVATION_VERSION_CHAIN_INVALID):
        repository.append(fork)


def test_selected_mapping_repository_is_idempotent_and_rejects_identity_conflict() -> None:
    selector = FixtureObservationVersionSelector()
    original = selector.select(request=_request(), observation_versions=(_version(),))
    divergent = selector.select(
        request=_request(cutoff=AVAILABLE_AT - timedelta(minutes=1)),
        observation_versions=(_version(),),
    )
    repository = InMemorySelectedObservationMappingRepository()

    assert repository.save(original) is original
    assert repository.save(original) is original
    assert repository.get(str(original.selected_mapping_id)) is original

    conflicting = divergent.model_copy(update={"selected_mapping_id": original.selected_mapping_id})
    with pytest.raises(SourceLedgerError, match=REASON_OBSERVATION_MAPPING_CONFLICT):
        repository.save(conflicting)


def test_capture_record_adapter_reports_a_typed_error_for_malformed_payload() -> None:
    malformed = SimpleNamespace(
        canonical_signal_id="acs-fixture",
        observation_version_id="osv-invalid",
        observation_content_hash=HASH_A,
        observation_payload={"stages": []},
    )

    with pytest.raises(SourceLedgerError, match=REASON_OBSERVATION_CAPTURE_RECORD_INVALID):
        FixtureObservationVersion.from_capture_record(malformed)
