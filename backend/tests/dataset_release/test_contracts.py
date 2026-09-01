from __future__ import annotations

import uuid
from datetime import date

import pytest

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.contracts import (
    UNKNOWN_PIT_SNAPSHOT,
    UNKNOWN_PRODUCER_PROVENANCE,
    AttestationIdentity,
    AttemptIdentity,
    CandidateIdentity,
    EquivalenceMode,
    LogicalRequestIdentity,
    PitProvenanceState,
    ProducerProvenanceState,
    ReleaseIdentity,
    ResolvedIntentIdentity,
    RunIdentity,
    Scope,
    attestation_observation_key,
    canonical_request_hash,
    noop_operation_target,
    resume_operation_target,
)
from backend.services.dataset_release.errors import IdentityConflictError


def _digest(char: str) -> str:
    return char * 64


def test_canonical_request_and_logical_identity_are_stable() -> None:
    assert canonical_request_hash({"scope": "full", "cutoff": "2026-07-31"}) == (
        canonical_request_hash({"cutoff": "2026-07-31", "scope": "full"})
    )
    first = LogicalRequestIdentity(
        profile="qe_hmm_full_v1",
        resolved_cutoff=date(2026, 7, 31),
        scope=Scope.FULL,
        semantic_profile_digest=_digest("a"),
    )
    second = LogicalRequestIdentity(
        profile="qe_hmm_full_v1",
        resolved_cutoff=date(2026, 7, 31),
        scope=Scope.FULL,
        semantic_profile_digest=_digest("a"),
    )
    assert first.key == second.key


def test_pit_only_revision_changes_intent_release_and_path() -> None:
    logical = _digest("1")
    first_intent = ResolvedIntentIdentity(logical, _digest("2"), _digest("3"))
    second_intent = ResolvedIntentIdentity(logical, _digest("2"), _digest("4"))
    assert first_intent.key != second_intent.key

    first = ReleaseIdentity(
        first_intent.key,
        _digest("3"),
        Scope.FULL,
        _digest("5"),
        _digest("6"),
        date(2026, 7, 31),
        "qe_hmm_full_v1",
    )
    second = ReleaseIdentity(
        second_intent.key,
        _digest("4"),
        Scope.FULL,
        _digest("5"),
        _digest("6"),
        date(2026, 7, 31),
        "qe_hmm_full_v1",
    )
    assert first.digest != second.digest
    assert first.release_id != second.release_id
    assert first.release_id.endswith("-candidate")


def test_candidate_identity_binds_physical_registration_and_lineage() -> None:
    base = dict(
        registration_uuid=str(uuid.uuid4()),
        allowlisted_root_id="aistock-x-candidate-v1",
        volume_serial="X-VOLUME-1234",
        root_relative_path="2026/Release-Candidate",
        profile="qe_hmm_full_v1",
        scope=Scope.FULL,
        cutoff=date(2026, 7, 31),
        lineage_anchor=f"BUILD_RELEASE_DIGEST:{_digest('a')}",
        pit_provenance_state=PitProvenanceState.KNOWN,
        pit_provenance_digest_or_sentinel=_digest("b"),
        artifact_root=_digest("c"),
        producer_provenance_state=ProducerProvenanceState.KNOWN,
        producer_provenance_digest_or_sentinel=_digest("d"),
    )
    first = CandidateIdentity(**base)
    moved = CandidateIdentity(
        **{**base, "registration_uuid": str(uuid.uuid4()), "root_relative_path": "moved/release-candidate"}
    )
    assert first.key != moved.key


def test_unknown_provenance_requires_canonical_sentinels() -> None:
    identity = CandidateIdentity(
        registration_uuid=str(uuid.uuid4()),
        allowlisted_root_id="root",
        volume_serial="serial",
        root_relative_path="legacy/candidate",
        profile="qe_hmm_full_v1",
        scope=Scope.FULL,
        cutoff=date(2026, 7, 31),
        lineage_anchor=f"LEGACY_RECEIPT:r1:{_digest('a')}",
        pit_provenance_state=PitProvenanceState.UNKNOWN,
        pit_provenance_digest_or_sentinel=UNKNOWN_PIT_SNAPSHOT,
        artifact_root=_digest("b"),
        producer_provenance_state=ProducerProvenanceState.UNKNOWN,
        producer_provenance_digest_or_sentinel=UNKNOWN_PRODUCER_PROVENANCE,
    )
    assert len(identity.key) == 64
    with pytest.raises(IdentityConflictError, match="must use"):
        CandidateIdentity(
            **{
                **identity.__dict__,
                "producer_provenance_digest_or_sentinel": _digest("c"),
            }
        ).key


def test_noop_target_binds_candidate_probe_and_validation() -> None:
    first = noop_operation_target(_digest("1"), _digest("2"), _digest("3"), _digest("4"))
    second = noop_operation_target(_digest("5"), _digest("2"), _digest("3"), _digest("4"))
    assert first != second

    attestation = AttestationIdentity(
        candidate_identity=_digest("1"),
        producer_provenance_state=ProducerProvenanceState.KNOWN,
        producer_provenance_digest_or_sentinel=_digest("2"),
        artifact_root=_digest("3"),
        current_source_content_root=_digest("4"),
        pit_digest=_digest("5"),
        semantic_profile_digest=_digest("6"),
        validation_fingerprint=_digest("7"),
        equivalence_mode=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT,
        source_probe_key=_digest("8"),
    )
    assert attestation.target_key == digest_named_fields(
        "dataset_release_attestation_key_v1",
        {
            "candidate_identity": _digest("1"),
            "producer_provenance_state": ProducerProvenanceState.KNOWN,
            "producer_provenance_digest_or_sentinel": _digest("2"),
            "artifact_root": _digest("3"),
            "current_source_content_root": _digest("4"),
            "pit_digest": _digest("5"),
            "semantic_profile_digest": _digest("6"),
            "validation_fingerprint": _digest("7"),
            "equivalence_mode": EquivalenceMode.CURRENT_SOURCE_EQUIVALENT,
        },
    )
    assert attestation.key == digest_named_fields(
        "dataset_release_attestation_observation_v1",
        {
            "attestation_target_key": attestation.target_key,
            "source_probe_key": _digest("8"),
        },
    )
    assert attestation.key == attestation_observation_key(
        attestation.target_key,
        _digest("8"),
    )


def test_attestation_observation_renews_without_mutating_stable_target() -> None:
    first = AttestationIdentity(
        candidate_identity=_digest("1"),
        producer_provenance_state=ProducerProvenanceState.KNOWN,
        producer_provenance_digest_or_sentinel=_digest("2"),
        artifact_root=_digest("3"),
        current_source_content_root=_digest("4"),
        pit_digest=_digest("5"),
        semantic_profile_digest=_digest("6"),
        validation_fingerprint=_digest("7"),
        equivalence_mode=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT,
        source_probe_key=_digest("8"),
    )
    renewed = AttestationIdentity(**{**first.__dict__, "source_probe_key": _digest("9")})

    assert first.target_key == renewed.target_key
    assert first.key != renewed.key


def test_run_attempt_and_resume_identities_bind_monotonic_lineage() -> None:
    run = RunIdentity(_digest("1"), _digest("2"), _digest("3"), 0)
    resumed = RunIdentity(_digest("1"), _digest("2"), _digest("3"), 1)
    assert run.key != resumed.key
    assert AttemptIdentity(run.key, 1).key != AttemptIdentity(run.key, 2).key
    assert resume_operation_target(
        run.key,
        _digest("2"),
        _digest("4"),
        1,
    ) != resume_operation_target(
        run.key,
        _digest("2"),
        _digest("4"),
        2,
    )
