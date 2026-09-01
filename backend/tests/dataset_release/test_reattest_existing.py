from __future__ import annotations

import io
import uuid
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from backend.services.dataset_release.attestation import (
    AttestationRequest,
    AttestationService,
    CandidateReadOnlyError,
    LegacyValidationEvidence,
    ReadOnlyCandidateHandle,
    ReattestExecutionContext,
    decide_legacy_attestation,
)
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.contracts import (
    UNKNOWN_PRODUCER_PROVENANCE,
    AttestationIdentity,
    CandidateIdentity,
    EquivalenceMode,
    PitProvenanceState,
    ProducerProvenanceState,
    Scope,
)
from backend.services.dataset_release.control_store import ControlStore, volume_identity
from backend.services.dataset_release.lease import LeaseManager
from backend.services.dataset_release.publisher import artifact_tree_digest
from backend.services.dataset_release.state_machine import DatasetReleaseStateMachine, IntentSpec


def _digest(char: str) -> str:
    return char * 64


def _evidence(**changes) -> LegacyValidationEvidence:
    value = LegacyValidationEvidence(
        artifact_identity_complete=True,
        artifact_valid=True,
        artifact_root_matches_catalog=True,
        validation_passed=True,
        full_required_component_coverage=True,
        full_current_source_value_parity=False,
        original_pit_snapshot_digest=_digest("1"),
        current_pit_snapshot_digest=_digest("1"),
        original_source_content_root=_digest("2"),
        current_source_content_root=_digest("2"),
        original_producer_provenance_digest=_digest("3"),
        validation_details={"checks": "PASS"},
    )
    return replace(value, **changes)


def _reattest_execution(
    *,
    store: ControlStore,
    cas: CASStore,
    request: AttestationRequest,
    target_key: str,
    observation_key: str,
    suffix: str,
) -> ReattestExecutionContext:
    machine = DatasetReleaseStateMachine(store)
    plan_ref = cas.put_json(
        {
            "schema_version": "dataset_release_resolution_plan_v1",
            "operation_kind": "REATTEST",
            "resolved_intent_key": _digest("a"),
            "source_content_root": request.current_source_content_root,
            "source_provenance_root": _digest("b"),
            "pit_snapshot_digest": request.current_pit_snapshot_digest,
            "attestation_target_key": target_key,
            "attestation_observation_key": observation_key,
            "source_probe_key": request.source_probe_key,
            "source_probe_ref": request.source_probe_ref,
        }
    )
    run = machine.create_queued_run(
        intent=IntentSpec(
            logical_request_key=_digest("c"),
            resolved_intent_key=_digest("a"),
            source_content_root=request.current_source_content_root,
            source_provenance_root=_digest("b"),
            pit_snapshot_digest=request.current_pit_snapshot_digest,
        ),
        run_generation_digest=_digest(suffix),
        operation_kind="REATTEST",
        plan_ref=plan_ref.sha256,
    )
    claim = LeaseManager(store).claim_build(
        run_id=run["run_id"],
        release_id=f"legacy-reattest-{suffix}",
        owner_identity="reattest-worker",
        ttl_seconds=120,
        attempt_kind="REATTEST",
        now=request.observed_at,
    )
    assert claim.host is not None and claim.release is not None
    owned = store.get_run(run["run_id"])
    return ReattestExecutionContext(
        run_id=run["run_id"],
        attempt_id=claim.attempt_id,
        expected_row_version=owned["row_version"],
        attempt_fence=claim.attempt_fence,
        tokens=(claim.host, claim.release),
        finalized_at=request.observed_at + timedelta(seconds=1),
    )


@pytest.mark.parametrize(
    ("evidence", "outcome", "eligible"),
    [
        (_evidence(artifact_valid=False), EquivalenceMode.INVALID, False),
        (
            _evidence(
                artifact_identity_complete=False,
            ),
            EquivalenceMode.BLOCKED_LEGACY_PROVENANCE,
            False,
        ),
        (
            _evidence(
                original_pit_snapshot_digest=None,
                original_source_content_root=None,
                original_producer_provenance_digest=None,
                full_current_source_value_parity=True,
            ),
            EquivalenceMode.ARTIFACT_VALID_ONLY,
            False,
        ),
        (
            _evidence(
                original_source_content_root=None,
                original_producer_provenance_digest=None,
                full_current_source_value_parity=False,
            ),
            EquivalenceMode.ARTIFACT_VALID_ONLY,
            False,
        ),
        (
            _evidence(
                original_source_content_root=None,
                original_producer_provenance_digest=None,
                full_current_source_value_parity=True,
            ),
            EquivalenceMode.CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED,
            True,
        ),
        (
            _evidence(original_source_content_root=_digest("4")),
            EquivalenceMode.ARTIFACT_VALID_SOURCE_CHANGED,
            False,
        ),
        (
            _evidence(current_pit_snapshot_digest=_digest("4")),
            EquivalenceMode.ARTIFACT_VALID_SOURCE_CHANGED,
            False,
        ),
        (_evidence(), EquivalenceMode.CURRENT_SOURCE_EQUIVALENT, True),
    ],
)
def test_complete_legacy_truth_table(evidence, outcome, eligible) -> None:
    decision = decide_legacy_attestation(evidence)
    assert decision.outcome is outcome
    assert decision.eligible_for_noop_reuse is eligible


def test_legacy_reconstruction_requires_full_component_value_parity() -> None:
    incomplete = decide_legacy_attestation(
        _evidence(
            original_source_content_root=None,
            original_producer_provenance_digest=None,
            full_current_source_value_parity=True,
            full_required_component_coverage=False,
        )
    )
    assert incomplete.outcome is EquivalenceMode.ARTIFACT_VALID_ONLY


def test_readonly_candidate_handle_exposes_only_binary_read(tmp_path: Path) -> None:
    allowed = tmp_path / "candidates"
    candidate = allowed / "legacy-candidate"
    candidate.mkdir(parents=True)
    payload = candidate / "payload.bin"
    payload.write_bytes(b"immutable")
    with ReadOnlyCandidateHandle(candidate, allowlisted_roots=(allowed,)) as handle:
        assert handle.artifact_root == artifact_tree_digest(candidate)
        assert handle.file_count == 1
        assert handle.total_bytes == len(b"immutable")
        with handle.open_file("payload.bin") as stream:
            assert stream.read() == b"immutable"
            with pytest.raises(io.UnsupportedOperation):
                stream.write(b"forbidden")
        assert not hasattr(handle, "root")
    assert payload.read_bytes() == b"immutable"


def test_reattest_receipt_is_external_cas_and_candidate_remains_unchanged(tmp_path: Path) -> None:
    allowed = tmp_path / "candidates"
    candidate = allowed / "legacy-candidate"
    candidate.mkdir(parents=True)
    (candidate / "payload.bin").write_bytes(b"immutable payload")
    (candidate / "manifest.json").write_text('{"status":"legacy"}\n', encoding="utf-8")
    with ReadOnlyCandidateHandle(candidate, allowlisted_roots=(allowed,)) as handle:
        artifact_root = handle.artifact_root
    before = {
        path.relative_to(candidate).as_posix(): path.read_bytes() for path in candidate.rglob("*") if path.is_file()
    }

    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    probe_ref = cas.put_json({"probe": "fixture"})
    identity = CandidateIdentity(
        registration_uuid=str(uuid.uuid4()),
        allowlisted_root_id="fixture-root",
        volume_serial=volume_identity(candidate),
        root_relative_path="legacy-candidate",
        profile="qe_hmm_full_v1",
        scope=Scope.FULL,
        cutoff=date(2026, 7, 31),
        lineage_anchor=f"LEGACY_RECEIPT:legacy-r1:{_digest('a')}",
        pit_provenance_state=PitProvenanceState.KNOWN,
        pit_provenance_digest_or_sentinel=_digest("1"),
        artifact_root=artifact_root,
        producer_provenance_state=ProducerProvenanceState.UNKNOWN,
        producer_provenance_digest_or_sentinel=UNKNOWN_PRODUCER_PROVENANCE,
    )
    now = datetime.now(UTC)
    request = AttestationRequest(
        candidate_identity=identity,
        candidate_path=candidate,
        allowlisted_roots={"fixture-root": allowed},
        production_roots=(),
        current_source_content_root=_digest("2"),
        current_pit_snapshot_digest=_digest("1"),
        source_probe_key=_digest("4"),
        source_probe_ref=probe_ref.sha256,
        semantic_profile_digest=_digest("5"),
        validation_fingerprint=_digest("6"),
        observed_at=now,
        valid_until=now + timedelta(minutes=30),
    )

    validation_calls: list[str] = []

    def validator(handle, actual_root):
        validation_calls.append(actual_root)
        with handle.open_file("payload.bin") as stream:
            assert stream.read() == b"immutable payload"
        return _evidence(
            artifact_root_matches_catalog=actual_root == artifact_root,
            original_source_content_root=None,
            original_producer_provenance_digest=None,
            full_current_source_value_parity=True,
        )

    expected = AttestationIdentity(
        candidate_identity=identity.key,
        producer_provenance_state=ProducerProvenanceState.RECONSTRUCTED_SOURCE_ONLY,
        producer_provenance_digest_or_sentinel=UNKNOWN_PRODUCER_PROVENANCE,
        artifact_root=artifact_root,
        current_source_content_root=request.current_source_content_root,
        pit_digest=request.current_pit_snapshot_digest,
        semantic_profile_digest=request.semantic_profile_digest,
        validation_fingerprint=request.validation_fingerprint,
        equivalence_mode=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED,
        source_probe_key=request.source_probe_key,
    )
    execution = _reattest_execution(
        store=store,
        cas=cas,
        request=request,
        target_key=expected.target_key,
        observation_key=expected.key,
        suffix="8",
    )
    result = AttestationService(
        cas,
        DatasetReleaseStateMachine(store),
    ).reattest_existing(request, validator, execution=execution)

    assert result.outcome is EquivalenceMode.CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED
    assert result.eligible_for_noop_reuse is True
    assert result.run["state"] == "SUCCEEDED"
    assert result.run["outcome"] == "REATTESTED"
    assert store.get_attempt(execution.attempt_id)["state"] == "RELEASED_SUCCEEDED"
    assert (
        store._many(
            "SELECT * FROM leases WHERE attempt_id=?",
            (execution.attempt_id,),
        )
        == []
    )
    assert len(validation_calls) == 1
    receipt = cas.get_json(result.receipt_ref)
    assert receipt["safety"]["candidate_writes"] == 0
    assert receipt["safety"]["database_writes"] == 0
    assert receipt["safety"]["production_writes"] == 0
    after = {
        path.relative_to(candidate).as_posix(): path.read_bytes() for path in candidate.rglob("*") if path.is_file()
    }
    assert after == before
    assert not any("attestation" in name for name in after)
    with store.transaction(immediate=False) as connection:
        row = connection.execute(
            "SELECT * FROM attestations WHERE attestation_key=?",
            (result.attestation_key,),
        ).fetchone()
    assert row is not None and row["receipt_ref"] == result.receipt_ref.sha256

    replay = AttestationService(
        cas,
        DatasetReleaseStateMachine(store),
    ).reattest_existing(
        request,
        validator,
        execution=replace(
            execution,
            finalized_at=now + timedelta(hours=1),
        ),
    )
    assert replay.run == result.run
    assert replay.receipt_ref == result.receipt_ref
    assert len(validation_calls) == 1
    assert (
        len([event for event in store.list_events(run_id=execution.run_id) if event["type"] == "RUN_REATTESTED"]) == 1
    )

    renewed_probe_ref = cas.put_json({"probe": "renewed-fixture"})
    renewed_request = replace(
        request,
        source_probe_key=_digest("7"),
        source_probe_ref=renewed_probe_ref.sha256,
        observed_at=now + timedelta(minutes=31),
        valid_until=now + timedelta(minutes=61),
    )
    renewed_expected = AttestationIdentity(
        **{**expected.__dict__, "source_probe_key": renewed_request.source_probe_key}
    )
    renewed_execution = _reattest_execution(
        store=store,
        cas=cas,
        request=renewed_request,
        target_key=renewed_expected.target_key,
        observation_key=renewed_expected.key,
        suffix="9",
    )
    renewed = AttestationService(
        cas,
        DatasetReleaseStateMachine(store),
    ).reattest_existing(
        renewed_request,
        validator,
        execution=renewed_execution,
    )
    assert renewed.attestation_target_key == result.attestation_target_key
    assert renewed.attestation_key != result.attestation_key
    assert renewed.receipt_ref != result.receipt_ref
    assert len(validation_calls) == 2
    with store.transaction(immediate=False) as connection:
        assert connection.execute("SELECT COUNT(*) FROM attestations").fetchone()[0] == 2


def test_candidate_outside_allowlist_is_rejected(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    candidate = tmp_path / "outside-candidate"
    candidate.mkdir()
    with pytest.raises(CandidateReadOnlyError, match="outside allowlisted"):
        ReadOnlyCandidateHandle(candidate, allowlisted_roots=(allowed,))


def test_candidate_handle_rejects_candidate_containing_production_root(
    tmp_path: Path,
) -> None:
    allowed = tmp_path / "allowed"
    candidate = allowed / "candidate"
    production = candidate / "production"
    production.mkdir(parents=True)

    with pytest.raises(CandidateReadOnlyError, match="overlaps a production root"):
        ReadOnlyCandidateHandle(
            candidate,
            allowlisted_roots=(allowed,),
            production_roots=(production,),
        )


def test_reattest_rejects_validator_invented_missing_pit_provenance(
    tmp_path: Path,
) -> None:
    allowed = tmp_path / "candidates"
    candidate = allowed / "legacy-candidate"
    candidate.mkdir(parents=True)
    (candidate / "payload.bin").write_bytes(b"immutable")
    with ReadOnlyCandidateHandle(candidate, allowlisted_roots=(allowed,)) as handle:
        artifact_root = handle.artifact_root
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    probe_ref = cas.put_json({"probe": "fixture"})
    identity = CandidateIdentity(
        registration_uuid=str(uuid.uuid4()),
        allowlisted_root_id="fixture-root",
        volume_serial=volume_identity(candidate),
        root_relative_path="legacy-candidate",
        profile="qe_hmm_full_v1",
        scope=Scope.FULL,
        cutoff=date(2026, 7, 31),
        lineage_anchor=f"LEGACY_RECEIPT:legacy-r1:{_digest('a')}",
        pit_provenance_state=PitProvenanceState.UNKNOWN,
        pit_provenance_digest_or_sentinel="UNKNOWN_PIT_SNAPSHOT_V1",
        artifact_root=artifact_root,
        producer_provenance_state=ProducerProvenanceState.UNKNOWN,
        producer_provenance_digest_or_sentinel=UNKNOWN_PRODUCER_PROVENANCE,
    )
    now = datetime.now(UTC)
    request = AttestationRequest(
        candidate_identity=identity,
        candidate_path=candidate,
        allowlisted_roots={"fixture-root": allowed},
        production_roots=(),
        current_source_content_root=_digest("2"),
        current_pit_snapshot_digest=_digest("1"),
        source_probe_key=_digest("4"),
        source_probe_ref=probe_ref.sha256,
        semantic_profile_digest=_digest("5"),
        validation_fingerprint=_digest("6"),
        observed_at=now,
        valid_until=now + timedelta(minutes=30),
    )
    execution = _reattest_execution(
        store=store,
        cas=cas,
        request=request,
        target_key=_digest("8"),
        observation_key=_digest("9"),
        suffix="7",
    )

    with pytest.raises(CandidateReadOnlyError, match="cannot invent PIT"):
        AttestationService(
            cas,
            DatasetReleaseStateMachine(store),
        ).reattest_existing(
            request,
            lambda _handle, _root: _evidence(),
            execution=execution,
        )
    assert store.get_run(execution.run_id)["state"] == "REATTESTING"
    assert store.get_attempt(execution.attempt_id)["state"] == "RUNNING"


def test_reattest_binds_root_id_relative_path_and_volume(tmp_path: Path) -> None:
    allowed = tmp_path / "candidates"
    candidate = allowed / "actual"
    candidate.mkdir(parents=True)
    (candidate / "payload.bin").write_bytes(b"immutable")
    with ReadOnlyCandidateHandle(candidate, allowlisted_roots=(allowed,)) as handle:
        artifact_root = handle.artifact_root
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    now = datetime.now(UTC)
    identity = CandidateIdentity(
        registration_uuid=str(uuid.uuid4()),
        allowlisted_root_id="configured-root",
        volume_serial=volume_identity(candidate),
        root_relative_path="different",
        profile="qe_hmm_full_v1",
        scope=Scope.FULL,
        cutoff=date(2026, 7, 31),
        lineage_anchor=f"LEGACY_RECEIPT:legacy-r1:{_digest('a')}",
        pit_provenance_state=PitProvenanceState.KNOWN,
        pit_provenance_digest_or_sentinel=_digest("1"),
        artifact_root=artifact_root,
        producer_provenance_state=ProducerProvenanceState.KNOWN,
        producer_provenance_digest_or_sentinel=_digest("3"),
    )
    request = AttestationRequest(
        candidate_identity=identity,
        candidate_path=candidate,
        allowlisted_roots={"configured-root": allowed},
        production_roots=(),
        current_source_content_root=_digest("2"),
        current_pit_snapshot_digest=_digest("1"),
        source_probe_key=_digest("4"),
        source_probe_ref=cas.put_json({"probe": "fixture"}).sha256,
        semantic_profile_digest=_digest("5"),
        validation_fingerprint=_digest("6"),
        observed_at=now,
        valid_until=now + timedelta(minutes=30),
    )
    execution = _reattest_execution(
        store=store,
        cas=cas,
        request=request,
        target_key=_digest("8"),
        observation_key=_digest("9"),
        suffix="7",
    )

    with pytest.raises(CandidateReadOnlyError, match="root-relative identity"):
        AttestationService(cas, DatasetReleaseStateMachine(store)).reattest_existing(
            request,
            lambda _handle, _root: _evidence(),
            execution=execution,
        )
    assert store.get_run(execution.run_id)["state"] == "REATTESTING"
    assert store.get_attempt(execution.attempt_id)["state"] == "RUNNING"
