from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from backend.services.dataset_release.control_store import (
    ControlStore,
    StateConflict,
    build_candidate_registration_id,
    volume_identity,
)
from backend.services.dataset_release.contracts import (
    CandidateIdentity,
    PitProvenanceState,
    ProducerProvenanceState,
    Scope,
    attestation_observation_key,
)
from backend.services.dataset_release.lease import LeaseManager
from backend.services.dataset_release.publisher import (
    DatasetPublisher,
    PublishSpec,
    artifact_tree_digest,
)
from backend.services.dataset_release.state_machine import DatasetReleaseStateMachine, IntentSpec


class InjectedCrash(RuntimeError):
    pass


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _prepared_fixture(
    tmp_path: Path, suffix: str = "one"
) -> tuple[
    ControlStore,
    DatasetReleaseStateMachine,
    LeaseManager,
    Path,
    PublishSpec,
]:
    store = ControlStore.initialize(tmp_path / f"control-{suffix}")
    candidate_root = tmp_path / f"candidates-{suffix}"
    (candidate_root / ".staging").mkdir(parents=True)
    machine = DatasetReleaseStateMachine(store)
    manager = LeaseManager(store)
    run = machine.create_queued_run(
        intent=IntentSpec(
            logical_request_key=f"logical-{suffix}",
            resolved_intent_key=f"resolved-{suffix}",
            source_content_root=f"source-{suffix}",
            source_provenance_root=f"provenance-{suffix}",
            pit_snapshot_digest=f"pit-{suffix}",
        ),
        run_generation_digest=f"generation-{suffix}",
        operation_kind="BUILD",
        plan_ref=f"cas:plan-{suffix}",
    )
    release_id = f"20260731-qe-full-{suffix}-candidate"
    claim = manager.claim_build(
        run_id=run["run_id"],
        release_id=release_id,
        owner_identity=f"worker-{suffix}",
        ttl_seconds=120,
    )
    staging = candidate_root / ".staging" / claim.attempt_id / str(claim.attempt_fence)
    staging.mkdir(parents=True)
    (staging / "daily.bin").write_bytes(b"daily bytes")
    (staging / "metadata").mkdir()
    (staging / "metadata" / "manifest.json").write_text('{"schema_version":"fixture"}\n', encoding="utf-8")
    with store.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET staging_ref=? WHERE attempt_id=?",
            (str(staging), claim.attempt_id),
        )
    artifact_root = artifact_tree_digest(staging)
    release_digest = _digest(f"release-digest-{suffix}")
    registration_id = build_candidate_registration_id(release_digest)
    producer_digest = _digest(f"producer-{suffix}")
    pit_digest = _digest(f"pit-{suffix}")
    candidate_identity = CandidateIdentity(
        registration_uuid=registration_id,
        allowlisted_root_id="fixture-candidate-root",
        volume_serial=volume_identity(candidate_root),
        root_relative_path=release_id,
        profile="qe_hmm_full_v1",
        scope=Scope.FULL,
        cutoff=datetime(2026, 7, 31, tzinfo=UTC).date(),
        lineage_anchor=f"BUILD_RELEASE_DIGEST:{release_digest}",
        pit_provenance_state=PitProvenanceState.KNOWN,
        pit_provenance_digest_or_sentinel=pit_digest,
        artifact_root=artifact_root,
        producer_provenance_state=ProducerProvenanceState.KNOWN,
        producer_provenance_digest_or_sentinel=producer_digest,
    ).key
    now = datetime.now(UTC)
    attestation_target_key = _digest(f"attestation-target-{suffix}")
    source_probe_key = _digest(f"probe-key-{suffix}")
    attestation_key = attestation_observation_key(
        attestation_target_key,
        source_probe_key,
    )
    machine.register_attestation(
        attestation_id=f"attestation-{suffix}",
        attestation_key=attestation_key,
        attestation_target_key=attestation_target_key,
        subject_type="release",
        subject_digest=candidate_identity,
        candidate_identity=candidate_identity,
        producer_provenance_state="KNOWN",
        producer_provenance_digest_or_sentinel=producer_digest,
        candidate_artifact_root=artifact_root,
        current_source_content_root=f"source-{suffix}",
        source_probe_key=source_probe_key,
        source_probe_ref=f"cas:probe-{suffix}",
        pit_snapshot_digest=pit_digest,
        semantic_profile_digest="semantic-profile",
        validation_fingerprint="validator-v2",
        observed_at=now,
        valid_until=now + timedelta(hours=1),
        equivalence_mode="full",
        outcome="CURRENT_SOURCE_EQUIVALENT",
        receipt_ref=f"cas:attestation-{suffix}",
        committed=False,
    )
    tokens = (claim.host, claim.release)
    executing = store.get_run(run["run_id"])
    validating = machine.transition_owned_keep(
        run_id=run["run_id"],
        attempt_id=claim.attempt_id,
        expected_state="EXECUTING",
        expected_row_version=executing["row_version"],
        attempt_fence=claim.attempt_fence,
        tokens=tokens,
        next_state="VALIDATING",
    )
    machine.transition_owned_keep(
        run_id=run["run_id"],
        attempt_id=claim.attempt_id,
        expected_state="VALIDATING",
        expected_row_version=validating["row_version"],
        attempt_fence=claim.attempt_fence,
        tokens=tokens,
        next_state="PREPARING_PUBLISH",
    )
    spec = PublishSpec(
        run_id=run["run_id"],
        attempt_id=claim.attempt_id,
        attempt_fence=claim.attempt_fence,
        host_fence=claim.host.fence,
        release_fence=claim.release.fence,
        release_id=release_id,
        release_digest=release_digest,
        candidate_registration_id=registration_id,
        allowlisted_root_id="fixture-candidate-root",
        volume_serial=volume_identity(candidate_root),
        root_relative_path=release_id,
        lineage_anchor=f"BUILD_RELEASE_DIGEST:{release_digest}",
        candidate_identity=candidate_identity,
        producer_provenance_state="KNOWN",
        producer_provenance_digest_or_sentinel=producer_digest,
        pit_provenance_state="KNOWN",
        profile="qe_hmm_full_v1",
        scope="full",
        cutoff="2026-07-31",
        staging_path=staging,
        final_path=candidate_root / release_id,
        manifest_root=f"manifest-{suffix}",
        artifact_root=artifact_root,
        pit_snapshot_digest=pit_digest,
        build_receipt_ref=f"cas:build-receipt-{suffix}",
        attestation_key=attestation_key,
        attestation_ref=f"cas:attestation-{suffix}",
        source_probe_key=source_probe_key,
        source_probe_ref=f"cas:probe-{suffix}",
    )
    return store, machine, manager, candidate_root, spec


def test_two_phase_publish_commits_marker_catalog_run_and_lease_release(tmp_path) -> None:
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path)
    publisher = DatasetPublisher(store, candidate_root=candidate_root)

    prepared = publisher.prepare(spec)
    files = publisher.commit_files(spec.release_id)
    release = publisher.finalize(spec.release_id)

    assert prepared["state"] == "PREPARED"
    assert files["state"] == "FILES_COMMITTED"
    assert release["state"] == "COMMITTED"
    assert spec.final_path.is_dir() and not spec.staging_path.exists()
    assert (spec.final_path / ".dataset_release_committed.json").is_file()
    assert store.get_run(spec.run_id)["state"] == "SUCCEEDED"
    assert store.get_run(spec.run_id)["outcome"] == "CANDIDATE_VALIDATED"
    assert store.get_attempt(spec.attempt_id)["state"] == "RELEASED_SUCCEEDED"
    assert store.get_lease("host:heavy-dataset")["state"] == "FREE"
    assert store.get_lease(f"release:{spec.release_id}")["state"] == "FREE"
    assert publisher.discover(spec.release_id)["candidate_identity"] == spec.candidate_identity
    registration = store.latest_candidate_registration(profile=spec.profile, scope=spec.scope, state="RELEASED")
    assert registration["registration_id"] == spec.candidate_registration_id
    assert registration["candidate_identity"] == spec.candidate_identity
    assert registration["root_relative_path"] == spec.root_relative_path
    assert registration["last_attested_at"] is not None


def test_publish_finalize_registers_candidate_in_same_atomic_transaction(tmp_path, monkeypatch) -> None:
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path, "registration-atomic")
    publisher = DatasetPublisher(store, candidate_root=candidate_root)
    publisher.prepare(spec)
    publisher.commit_files(spec.release_id)
    original = store.register_candidate_in_transaction

    def injected_failure(*_args, **_kwargs):
        raise RuntimeError("injected registration failure")

    monkeypatch.setattr(store, "register_candidate_in_transaction", injected_failure)
    with pytest.raises(RuntimeError, match="injected registration failure"):
        publisher.finalize(spec.release_id)

    assert store.get_publish_record(spec.release_id)["state"] == "FILES_COMMITTED"
    assert store.get_release_for_run(spec.run_id) is None
    assert store.latest_candidate_registration(profile=spec.profile, scope=spec.scope) is None
    assert store.get_run(spec.run_id)["state"] == "PUBLISHING"
    assert store.get_attempt(spec.attempt_id)["state"] == "RUNNING"
    assert store.get_lease("host:heavy-dataset")["state"] == "ACTIVE"

    monkeypatch.setattr(store, "register_candidate_in_transaction", original)
    assert publisher.finalize(spec.release_id)["state"] == "COMMITTED"


@pytest.mark.parametrize("drift", ["observation_key", "source_probe_key"])
def test_publish_rejects_target_key_or_probe_drift_before_prepare(tmp_path, drift: str) -> None:
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path, f"drift-{drift}")
    if drift == "observation_key":
        invalid = replace(
            spec,
            attestation_key=_digest(f"wrong-attestation-observation-{drift}"),
        )
    else:
        invalid = replace(spec, source_probe_key=_digest("probe-key-from-another-observation"))

    with pytest.raises(StateConflict, match="attestation .*mismatch"):
        DatasetPublisher(store, candidate_root=candidate_root).prepare(invalid)

    assert store.get_publish_record(spec.release_id) is None
    assert store.get_run(spec.run_id)["state"] == "PREPARING_PUBLISH"


def test_prepare_is_idempotent_for_exact_same_immutable_identity(tmp_path) -> None:
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path, "prepare-replay")
    publisher = DatasetPublisher(store, candidate_root=candidate_root)

    first = publisher.prepare(spec)
    replay = publisher.prepare(spec)

    assert replay["publish_nonce"] == first["publish_nonce"]
    assert replay["state"] == "PREPARED"
    assert len(store._many("SELECT * FROM publish_records WHERE run_id=?", (spec.run_id,))) == 1


@pytest.mark.parametrize("crash_point", ["after_rename", "after_marker", "after_marker_temp"])
def test_publish_crash_windows_recover_same_run_nonce_without_overwrite(tmp_path, crash_point) -> None:
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path, crash_point)

    def inject(point: str) -> None:
        if point == crash_point:
            raise InjectedCrash(point)

    crashing = DatasetPublisher(store, candidate_root=candidate_root, fault_injector=inject)
    prepared = crashing.prepare(spec)
    with pytest.raises(InjectedCrash, match=crash_point):
        crashing.commit_files(spec.release_id)

    assert store.get_publish_record(spec.release_id)["state"] == "PREPARED"
    recovering = DatasetPublisher(store, candidate_root=candidate_root)
    files = recovering.commit_files(spec.release_id)
    release = recovering.finalize(spec.release_id)

    assert files["publish_nonce"] == prepared["publish_nonce"]
    assert release["state"] == "COMMITTED"
    assert artifact_tree_digest(spec.final_path) == spec.artifact_root
    assert recovering.discover(spec.release_id) is not None


def test_publish_orphan_finalizer_handoff_keeps_marker_publisher_identity(tmp_path) -> None:
    store, _machine, manager, candidate_root, spec = _prepared_fixture(tmp_path, "handoff")
    publisher = DatasetPublisher(store, candidate_root=candidate_root)
    prepared = publisher.prepare(spec)
    publisher.commit_files(spec.release_id)
    marker_before = (spec.final_path / ".dataset_release_committed.json").read_bytes()

    manager.mark_orphan_hold(
        run_id=spec.run_id,
        attempt_id=spec.attempt_id,
        tree_status="unknown",
    )
    successor = manager.handoff_publish_finalizer(
        run_id=spec.run_id,
        old_attempt_id=spec.attempt_id,
        new_owner_identity="recovery-worker",
        ttl_seconds=120,
        tree_quiescent=True,
    )
    release = publisher.finalize(spec.release_id)

    assert release["state"] == "COMMITTED"
    assert successor.attempt_id != spec.attempt_id
    assert store.get_attempt(spec.attempt_id)["state"] == "EXPIRED"
    assert store.get_attempt(successor.attempt_id)["state"] == "RELEASED_SUCCEEDED"
    assert (spec.final_path / ".dataset_release_committed.json").read_bytes() == marker_before
    record = store.get_publish_record(spec.release_id)
    assert record["published_by_attempt_id"] == prepared["attempt_id"]
    assert record["finalized_by_attempt_id"] == successor.attempt_id


def test_stale_orphan_attempt_cannot_touch_final_path_before_handoff(tmp_path) -> None:
    store, _machine, manager, candidate_root, spec = _prepared_fixture(tmp_path, "stale-publisher")
    publisher = DatasetPublisher(store, candidate_root=candidate_root)
    publisher.prepare(spec)
    manager.mark_orphan_hold(
        run_id=spec.run_id,
        attempt_id=spec.attempt_id,
        tree_status="unknown",
    )

    with pytest.raises(Exception, match="filesystem owner is not active"):
        publisher.commit_files(spec.release_id)

    assert not spec.final_path.exists()
    assert spec.staging_path.is_dir()


def test_final_path_conflict_terminalizes_attempt_and_releases_both_leases(tmp_path) -> None:
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path, "conflict")
    publisher = DatasetPublisher(store, candidate_root=candidate_root)
    publisher.prepare(spec)
    spec.final_path.mkdir()
    (spec.final_path / "wrong.bin").write_bytes(b"wrong")

    with pytest.raises(Exception, match="both staging and final path exist"):
        publisher.commit_files(spec.release_id)

    run = store.get_run(spec.run_id)
    assert run["state"] == "BLOCKED_PUBLISH_CONFLICT"
    assert run["active_attempt_id"] is None
    assert store.get_attempt(spec.attempt_id)["state"] == "FAILED_TERMINAL"
    assert store.get_lease("host:heavy-dataset")["state"] == "FREE"
    assert store.get_lease(f"release:{spec.release_id}")["state"] == "FREE"


@pytest.mark.parametrize("command_state", ["QUEUED", "PENDING", "CLAIMED"])
def test_active_cancel_wins_prepare_commit_race_and_final_path_is_untouched(tmp_path, command_state: str) -> None:
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path, f"cancel-{command_state.lower()}")
    command = store.enqueue_command(
        target_type="run",
        target_id=spec.run_id,
        command_type="CANCEL_REQUESTED",
        principal="operator",
        route="/runs/cancel-request",
        idempotency_key=f"cancel-{command_state.lower()}",
        request_hash=f"cancel-request-{command_state.lower()}",
        actor="operator",
    )
    if command_state != "QUEUED":
        with store.transaction() as connection:
            connection.execute(
                "UPDATE commands SET state=? WHERE command_id=?",
                (command_state, command["command_id"]),
            )

    with pytest.raises(StateConflict, match="cancel request won"):
        DatasetPublisher(store, candidate_root=candidate_root).prepare(spec)

    assert not spec.final_path.exists()
    assert store.get_publish_record(spec.release_id) is None
    assert store.get_run(spec.run_id)["state"] == "PREPARING_PUBLISH"
