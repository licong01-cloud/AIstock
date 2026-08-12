from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from backend.services.dataset_release.control_store import ControlStore, StateConflict
from backend.services.dataset_release.contracts import attestation_observation_key
from backend.services.dataset_release.lease import LeaseManager
from backend.services.dataset_release.state_machine import (
    AttestationObservationSpec,
    AttestationRenewalSpec,
    DatasetReleaseStateMachine,
    IntentSpec,
    NoOpFinalizeSpec,
    ReattestFinalizeSpec,
    ResolutionSnapshotSpec,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _submission(store: ControlStore, suffix: str = "1") -> dict:
    return store.submit(
        principal="operator",
        route="runs",
        idempotency_key=f"key-{suffix}",
        request_hash=f"request-{suffix}",
        logical_request_key="qe:2026-07-31:full",
        request_ref=f"cas:request-{suffix}",
    )


def _prepare_noop(
    store: ControlStore,
    *,
    suffix: str = "1",
    valid_for: timedelta = timedelta(minutes=30),
) -> tuple[DatasetReleaseStateMachine, NoOpFinalizeSpec]:
    machine = DatasetReleaseStateMachine(store)
    source_probe_key = _digest(f"probe-{suffix}")
    attestation_target_key = _digest("attestation-target")
    attestation_key = attestation_observation_key(
        attestation_target_key,
        source_probe_key,
    )
    submission = _submission(store, suffix)
    claim = LeaseManager(store).claim_resolution(
        submission_id=submission["submission_id"],
        owner_identity=f"worker-{suffix}",
        ttl_seconds=60,
    )
    now = datetime.now(UTC)
    machine.record_resolution_snapshot(
        submission_id=submission["submission_id"],
        resolution_attempt_id=claim.attempt_id,
        resolution_fence=claim.attempt_fence,
        source_content_root="source-root",
        source_provenance_root="provenance-root",
        pit_snapshot_digest="pit-root",
        source_probe_ordinal=1,
        source_probe_key=source_probe_key,
        source_probe_ref="cas:probe",
        source_probe_valid_until=now + valid_for,
    )
    if not store._many("SELECT 1 FROM attestations WHERE attestation_key=?", (attestation_key,)):
        machine.register_attestation(
            attestation_id=f"attestation-{suffix}",
            attestation_key=attestation_key,
            attestation_target_key=attestation_target_key,
            subject_type="candidate",
            subject_digest="candidate-identity",
            candidate_identity="candidate-identity",
            producer_provenance_state="KNOWN",
            producer_provenance_digest_or_sentinel="producer-root",
            candidate_artifact_root="artifact-root",
            current_source_content_root="source-root",
            source_probe_key=source_probe_key,
            source_probe_ref="cas:probe",
            pit_snapshot_digest="pit-root",
            semantic_profile_digest="semantic-profile",
            validation_fingerprint="validator-v2",
            observed_at=now,
            valid_until=now + valid_for,
            equivalence_mode="CURRENT_SOURCE_EQUIVALENT",
            outcome="CURRENT_SOURCE_EQUIVALENT",
            receipt_ref="cas:attestation",
        )
    spec = NoOpFinalizeSpec(
        submission_id=submission["submission_id"],
        resolution_attempt_id=claim.attempt_id,
        resolution_fence=claim.attempt_fence,
        intent=IntentSpec(
            logical_request_key="qe:2026-07-31:full",
            resolved_intent_key="resolved-source-pit",
            source_content_root="source-root",
            source_provenance_root="provenance-root",
            pit_snapshot_digest="pit-root",
        ),
        run_generation_digest="noop-generation",
        candidate_identity="candidate-identity",
        artifact_root="artifact-root",
        source_probe_ordinal=1,
        source_probe_key=source_probe_key,
        source_probe_ref="cas:probe",
        attestation_key=attestation_key,
        attestation_target_key=attestation_target_key,
        attestation_ref="cas:attestation",
        semantic_profile_digest="semantic-profile",
        validation_fingerprint="validator-v2",
        decision_schema="dataset_release_decision_v1",
        terminal_receipt_ref="cas:noop-receipt",
    )
    return machine, spec


def _prepare_reattest(
    store: ControlStore,
    *,
    suffix: str = "1",
    observed_at: datetime | None = None,
    valid_until: datetime | None = None,
    committed: bool = True,
    operation_kind: str = "REATTEST",
) -> tuple[DatasetReleaseStateMachine, ReattestFinalizeSpec, datetime]:
    machine = DatasetReleaseStateMachine(store)
    run = machine.create_queued_run(
        intent=IntentSpec(
            logical_request_key=f"logical-reattest-{suffix}",
            resolved_intent_key=f"resolved-reattest-{suffix}",
            source_content_root=f"source-reattest-{suffix}",
            source_provenance_root=f"provenance-reattest-{suffix}",
            pit_snapshot_digest=f"pit-reattest-{suffix}",
        ),
        run_generation_digest=f"generation-reattest-{suffix}",
        operation_kind=operation_kind,
        plan_ref=f"cas:plan-reattest-{suffix}",
    )
    claim = LeaseManager(store).claim_build(
        run_id=run["run_id"],
        release_id=f"release-reattest-{suffix}",
        owner_identity=f"worker-reattest-{suffix}",
        ttl_seconds=60,
        attempt_kind="REATTEST",
    )
    assert claim.host is not None and claim.release is not None
    now = datetime.now(UTC)
    attestation_target_key = _digest(f"attestation-target-{suffix}")
    source_probe_key = _digest(f"probe-reattest-{suffix}")
    observation = AttestationObservationSpec(
        attestation_id=None,
        attestation_key=attestation_observation_key(
            attestation_target_key,
            source_probe_key,
        ),
        attestation_target_key=attestation_target_key,
        subject_type="candidate",
        subject_digest=f"candidate-reattest-{suffix}",
        candidate_identity=f"candidate-reattest-{suffix}",
        producer_provenance_state="KNOWN",
        producer_provenance_digest_or_sentinel=f"producer-reattest-{suffix}",
        candidate_artifact_root=f"artifact-reattest-{suffix}",
        current_source_content_root=f"source-reattest-{suffix}",
        source_probe_key=source_probe_key,
        source_probe_ref=f"cas:probe-reattest-{suffix}",
        pit_snapshot_digest=f"pit-reattest-{suffix}",
        semantic_profile_digest=f"semantic-reattest-{suffix}",
        validation_fingerprint=f"validation-reattest-{suffix}",
        observed_at=observed_at or now,
        valid_until=valid_until or now + timedelta(minutes=30),
        equivalence_mode="CURRENT_SOURCE_EQUIVALENT",
        outcome="CURRENT_SOURCE_EQUIVALENT",
        receipt_ref=f"cas:attestation-reattest-{suffix}",
        committed=committed,
    )
    owned = store.get_run(run["run_id"])
    spec = ReattestFinalizeSpec(
        run_id=run["run_id"],
        attempt_id=claim.attempt_id,
        expected_row_version=owned["row_version"],
        attempt_fence=claim.attempt_fence,
        tokens=(claim.host, claim.release),
        observation=observation,
    )
    return machine, spec, now


def test_fresh_noop_creates_reachable_terminal_run_and_releases_resolution(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, spec = _prepare_noop(store)

    run = machine.finalize_noop(spec)

    assert run["state"] == "SUCCEEDED"
    assert run["outcome"] == "NO_OP_VERIFIED"
    assert run["operation_kind"] == "NO_OP"
    assert run["active_attempt_id"] is None
    submission = store.get_submission(spec.submission_id)
    assert submission is not None
    assert submission["state"] == "RESOLVED_NO_OP"
    assert submission["run_id"] == run["run_id"]
    assert submission["terminal_receipt_ref"] == "cas:noop-receipt"
    assert store.get_resolution_attempt(spec.resolution_attempt_id)["state"] == "RELEASED_SUCCEEDED"
    resolution_leases = store._many("SELECT * FROM leases WHERE attempt_id=?", (spec.resolution_attempt_id,))
    assert resolution_leases == []
    assert store._many("SELECT * FROM attempts WHERE run_id=?", (run["run_id"],)) == []
    assert store._many("SELECT * FROM publish_records WHERE run_id=?", (run["run_id"],)) == []
    assert [event["type"] for event in store.list_events(run_id=run["run_id"])] == [
        "NO_OP_VERIFIED",
        "RESOLVED_NO_OP",
    ]


def _renewed_noop_spec(
    store: ControlStore,
    machine: DatasetReleaseStateMachine,
    spec: NoOpFinalizeSpec,
    *,
    extend_prior_validity: bool = False,
) -> tuple[NoOpFinalizeSpec, datetime]:
    prior = store._many(
        "SELECT * FROM attestations WHERE attestation_key=?",
        (spec.attestation_key,),
    )[0]
    observed = datetime.now(UTC) + timedelta(seconds=1)
    source_valid_until = observed + timedelta(minutes=40)
    machine.record_resolution_snapshot(
        submission_id=spec.submission_id,
        resolution_attempt_id=spec.resolution_attempt_id,
        resolution_fence=spec.resolution_fence,
        source_content_root=spec.intent.source_content_root,
        source_provenance_root=spec.intent.source_provenance_root,
        pit_snapshot_digest=spec.intent.pit_snapshot_digest,
        source_probe_ordinal=2,
        source_probe_key=_digest("renewal-probe"),
        source_probe_ref="cas:renewal-probe",
        source_probe_valid_until=source_valid_until,
    )
    prior_valid_until = datetime.fromisoformat(str(prior["valid_until"]))
    renewal_valid_until = min(prior_valid_until, source_valid_until)
    if extend_prior_validity:
        renewal_valid_until = prior_valid_until + timedelta(minutes=1)
    observation_key = attestation_observation_key(
        str(prior["attestation_target_key"]),
        _digest("renewal-probe"),
    )
    observation = AttestationObservationSpec(
        attestation_id=f"attestation-renewed-{spec.submission_id}",
        attestation_key=observation_key,
        attestation_target_key=str(prior["attestation_target_key"]),
        subject_type="candidate",
        subject_digest=spec.candidate_identity,
        candidate_identity=spec.candidate_identity,
        producer_provenance_state=str(prior["producer_provenance_state"]),
        producer_provenance_digest_or_sentinel=str(prior["producer_provenance_digest_or_sentinel"]),
        candidate_artifact_root=spec.artifact_root,
        current_source_content_root=spec.intent.source_content_root,
        source_probe_key=_digest("renewal-probe"),
        source_probe_ref="cas:renewal-probe",
        pit_snapshot_digest=spec.intent.pit_snapshot_digest,
        semantic_profile_digest=spec.semantic_profile_digest,
        validation_fingerprint=spec.validation_fingerprint,
        observed_at=observed,
        valid_until=renewal_valid_until,
        equivalence_mode="CURRENT_SOURCE_EQUIVALENT",
        outcome="CURRENT_SOURCE_EQUIVALENT",
        receipt_ref="cas:renewal-attestation",
    )
    return (
        replace(
            spec,
            run_generation_digest="noop-renewal-generation",
            source_probe_ordinal=2,
            source_probe_key=observation.source_probe_key,
            source_probe_ref=observation.source_probe_ref,
            attestation_key=observation.attestation_key,
            attestation_ref=observation.receipt_ref,
            terminal_receipt_ref="cas:renewal-noop-receipt",
            attestation_renewal=AttestationRenewalSpec(
                prior_attestation_key=spec.attestation_key,
                prior_attestation_ref=spec.attestation_ref,
                observation=observation,
            ),
        ),
        observed,
    )


def test_noop_renewal_commits_fresh_observation_with_terminal_run_atomically(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, original = _prepare_noop(store, suffix="renewal")
    renewed, observed = _renewed_noop_spec(store, machine, original)

    run = machine.finalize_noop(renewed, now=observed + timedelta(seconds=1))

    assert run["state"] == "SUCCEEDED"
    assert store.get_submission(renewed.submission_id)["state"] == "RESOLVED_NO_OP"
    observations = store._many(
        "SELECT * FROM attestations ORDER BY observed_at,attestation_id",
        (),
    )
    assert len(observations) == 2
    assert observations[-1]["attestation_key"] == renewed.attestation_key
    assert observations[-1]["source_probe_key"] == renewed.source_probe_key
    assert datetime.fromisoformat(observations[-1]["valid_until"]) <= (
        datetime.fromisoformat(observations[0]["valid_until"])
    )
    assert store._many("SELECT * FROM attempts WHERE run_id=?", (run["run_id"],)) == []


def test_noop_renewal_cannot_extend_prior_artifact_validation_validity(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, original = _prepare_noop(store, suffix="renewal-cap")
    renewed, observed = _renewed_noop_spec(
        store,
        machine,
        original,
        extend_prior_validity=True,
    )

    with pytest.raises(StateConflict, match="extended artifact validation"):
        machine.finalize_noop(renewed, now=observed + timedelta(seconds=1))

    assert len(store._many("SELECT * FROM attestations", ())) == 1
    assert store.get_submission(renewed.submission_id)["state"] == "RESOLVING_SOURCE"
    assert store.get_resolution_attempt(renewed.resolution_attempt_id)["state"] == "RUNNING"


def test_noop_renewal_observation_rolls_back_with_terminal_finalize_failure(
    tmp_path,
    monkeypatch,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, original = _prepare_noop(store, suffix="renewal-rollback")
    renewed, observed = _renewed_noop_spec(store, machine, original)

    def fail_submission_link(*_args, **_kwargs):
        raise StateConflict("injected terminal link failure")

    monkeypatch.setattr(machine, "_resolve_submission_to_run", fail_submission_link)
    with pytest.raises(StateConflict, match="injected terminal link failure"):
        machine.finalize_noop(renewed, now=observed + timedelta(seconds=1))

    assert len(store._many("SELECT * FROM attestations", ())) == 1
    assert store._many("SELECT * FROM runs", ()) == []
    assert store.get_submission(renewed.submission_id)["state"] == "RESOLVING_SOURCE"
    assert store.get_resolution_attempt(renewed.resolution_attempt_id)["state"] == "RUNNING"


def test_source_probe_ordinal_strictly_advances_as_one_atomic_snapshot(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, spec = _prepare_noop(store)
    future = datetime.now(UTC) + timedelta(minutes=10)

    with pytest.raises(StateConflict, match="ordinal"):
        machine.record_resolution_snapshot(
            submission_id=spec.submission_id,
            resolution_attempt_id=spec.resolution_attempt_id,
            resolution_fence=spec.resolution_fence,
            source_content_root="source-same-ordinal",
            source_provenance_root="provenance-same-ordinal",
            pit_snapshot_digest="pit-same-ordinal",
            source_probe_ordinal=1,
            source_probe_key="probe-same-ordinal",
            source_probe_ref="cas:probe-same-ordinal",
            source_probe_valid_until=future,
        )

    machine.record_resolution_snapshot(
        submission_id=spec.submission_id,
        resolution_attempt_id=spec.resolution_attempt_id,
        resolution_fence=spec.resolution_fence,
        source_content_root="source-v2",
        source_provenance_root="provenance-v2",
        pit_snapshot_digest="pit-v2",
        source_probe_ordinal=2,
        source_probe_key="probe-v2",
        source_probe_ref="cas:probe-v2",
        source_probe_valid_until=future,
    )
    with pytest.raises(StateConflict, match="ordinal"):
        machine.record_resolution_snapshot(
            submission_id=spec.submission_id,
            resolution_attempt_id=spec.resolution_attempt_id,
            resolution_fence=spec.resolution_fence,
            source_content_root="source-regressed",
            source_provenance_root="provenance-regressed",
            pit_snapshot_digest="pit-regressed",
            source_probe_ordinal=1,
            source_probe_key="probe-regressed",
            source_probe_ref="cas:probe-regressed",
            source_probe_valid_until=future,
        )

    durable = store.get_resolution_attempt(spec.resolution_attempt_id)
    assert durable["source_probe_ordinal"] == 2
    assert durable["source_content_root"] == "source-v2"
    assert durable["source_probe_key"] == "probe-v2"


def test_attestation_target_keeps_distinct_immutable_observations(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine = DatasetReleaseStateMachine(store)
    now = datetime.now(UTC)
    target_key = _digest("stable-target")
    first_probe_key = _digest("probe-1")
    second_probe_key = _digest("probe-2")
    common = {
        "attestation_id": None,
        "attestation_target_key": target_key,
        "subject_type": "candidate",
        "subject_digest": "candidate",
        "candidate_identity": "candidate",
        "producer_provenance_state": "KNOWN",
        "producer_provenance_digest_or_sentinel": "producer",
        "candidate_artifact_root": "artifact",
        "current_source_content_root": "source",
        "pit_snapshot_digest": "pit",
        "semantic_profile_digest": "semantic",
        "validation_fingerprint": "validation",
        "equivalence_mode": "CURRENT_SOURCE_EQUIVALENT",
        "outcome": "CURRENT_SOURCE_EQUIVALENT",
        "committed": True,
    }
    first = machine.register_attestation(
        **common,
        attestation_key=attestation_observation_key(target_key, first_probe_key),
        source_probe_key=first_probe_key,
        source_probe_ref="cas:probe-1",
        observed_at=now,
        valid_until=now + timedelta(minutes=30),
        receipt_ref="cas:attestation-1",
    )
    second = machine.register_attestation(
        **common,
        attestation_key=attestation_observation_key(target_key, second_probe_key),
        source_probe_key=second_probe_key,
        source_probe_ref="cas:probe-2",
        observed_at=now + timedelta(minutes=1),
        valid_until=now + timedelta(minutes=31),
        receipt_ref="cas:attestation-2",
    )

    assert first != second
    assert (
        len(
            store._many(
                "SELECT * FROM attestations WHERE attestation_target_key=?",
                (target_key,),
            )
        )
        == 2
    )
    with pytest.raises(StateConflict, match="different immutable evidence"):
        machine.register_attestation(
            **common,
            attestation_key=attestation_observation_key(target_key, first_probe_key),
            source_probe_key=first_probe_key,
            source_probe_ref="cas:probe-1",
            observed_at=now,
            valid_until=now + timedelta(hours=1),
            receipt_ref="cas:attestation-1",
        )


def test_finalize_reattest_commits_observation_and_terminal_state_atomically(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, spec, now = _prepare_reattest(store)

    run = machine.finalize_reattest(spec, now=now + timedelta(seconds=1))

    assert run["state"] == "SUCCEEDED"
    assert run["outcome"] == "REATTESTED"
    assert run["terminal_receipt_ref"] == spec.observation.receipt_ref
    assert run["active_attempt_id"] is None
    assert store.get_attempt(spec.attempt_id)["state"] == "RELEASED_SUCCEEDED"
    assert store.get_lease("host:heavy-dataset")["state"] == "FREE"
    assert store.get_lease("release:release-reattest-1")["state"] == "FREE"
    observation = store._many(
        "SELECT * FROM attestations WHERE attestation_key=?",
        (spec.observation.attestation_key,),
    )
    assert len(observation) == 1 and observation[0]["committed"] == 1
    assert store.list_events(run_id=spec.run_id)[-1]["type"] == "RUN_REATTESTED"


def test_finalize_reattest_exact_terminal_replay_is_idempotent_after_ttl(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, spec, now = _prepare_reattest(store, suffix="replay")
    first = machine.finalize_reattest(spec, now=now + timedelta(seconds=1))

    replay = machine.finalize_reattest(spec, now=now + timedelta(hours=1))

    assert replay == first
    events = [event for event in store.list_events(run_id=spec.run_id) if event["type"] == "RUN_REATTESTED"]
    assert len(events) == 1
    assert len(store._many("SELECT * FROM attestations", ())) == 1


def test_finalize_reattest_terminal_replay_rejects_identity_drift(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, spec, now = _prepare_reattest(store, suffix="replay-drift")
    machine.finalize_reattest(spec, now=now + timedelta(seconds=1))

    with pytest.raises(StateConflict, match="terminal_receipt_ref"):
        machine.finalize_reattest(
            replace(
                spec,
                observation=replace(spec.observation, receipt_ref="cas:different-attestation"),
            ),
            now=now + timedelta(seconds=2),
        )


def test_finalize_reattest_rejects_build_operation_kind_without_mutation(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, spec, now = _prepare_reattest(
        store,
        suffix="wrong-operation",
        operation_kind="BUILD",
    )

    with pytest.raises(StateConflict, match="ownership changed"):
        machine.finalize_reattest(spec, now=now + timedelta(seconds=1))

    assert store._many("SELECT * FROM attestations", ()) == []
    assert store.get_run(spec.run_id)["state"] == "REATTESTING"
    assert store.get_attempt(spec.attempt_id)["state"] == "RUNNING"
    assert store.get_lease("host:heavy-dataset")["state"] == "ACTIVE"


@pytest.mark.parametrize("failure", ["stale_fence", "expired", "uncommitted"])
def test_finalize_reattest_failure_rolls_back_observation_state_and_leases(tmp_path, failure: str) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, spec, now = _prepare_reattest(store)
    observed = now + timedelta(seconds=1)
    if failure == "stale_fence":
        spec = replace(spec, attempt_fence=spec.attempt_fence + 1)
    elif failure == "expired":
        spec = replace(
            spec,
            observation=replace(
                spec.observation,
                observed_at=now - timedelta(minutes=2),
                valid_until=now - timedelta(minutes=1),
            ),
        )
    else:
        spec = replace(spec, observation=replace(spec.observation, committed=False))

    with pytest.raises(StateConflict):
        machine.finalize_reattest(spec, now=observed)

    assert store._many("SELECT * FROM attestations", ()) == []
    assert store.get_run(spec.run_id)["state"] == "REATTESTING"
    assert store.get_attempt(spec.attempt_id)["state"] == "RUNNING"
    assert store.get_lease("host:heavy-dataset")["state"] == "ACTIVE"
    assert store.get_lease("release:release-reattest-1")["state"] == "ACTIVE"


def test_finalize_reattest_rejects_attempt_from_another_run(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, spec, now = _prepare_reattest(store, suffix="owner")
    other = machine.create_queued_run(
        intent=IntentSpec("logical-other", "resolved-other", "source-other", "p-other", "pit-other"),
        run_generation_digest="generation-other",
        operation_kind="BUILD",
        plan_ref="cas:plan-other",
    )

    with pytest.raises(StateConflict, match="ownership changed"):
        machine.finalize_reattest(
            replace(spec, run_id=other["run_id"], expected_row_version=other["row_version"]),
            now=now + timedelta(seconds=1),
        )

    assert store._many("SELECT * FROM attestations", ()) == []
    assert store.get_run(spec.run_id)["state"] == "REATTESTING"
    assert store.get_lease("host:heavy-dataset")["state"] == "ACTIVE"


def test_equivalent_concurrent_generation_links_existing_terminal_noop(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, first_spec = _prepare_noop(store, suffix="first")
    first = machine.finalize_noop(first_spec)

    machine, second_spec = _prepare_noop(store, suffix="second")
    second = machine.finalize_noop(second_spec)

    assert second["run_id"] == first["run_id"]
    assert store.get_submission(second_spec.submission_id)["run_id"] == first["run_id"]
    assert len(store._many("SELECT * FROM runs", ())) == 1


def test_expired_probe_rolls_back_noop_terminal_transaction(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine, spec = _prepare_noop(store, valid_for=timedelta(milliseconds=1))
    future = datetime.now(UTC) + timedelta(seconds=1)

    with pytest.raises(StateConflict, match="expired"):
        machine.finalize_noop(spec, now=future)

    assert store._many("SELECT * FROM runs", ()) == []
    assert store.get_submission(spec.submission_id)["state"] == "RESOLVING_SOURCE"
    assert store.get_resolution_attempt(spec.resolution_attempt_id)["state"] == "RUNNING"


def test_invalid_run_transition_is_rejected_without_mutation(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine = DatasetReleaseStateMachine(store)
    run = machine.create_queued_run(
        intent=IntentSpec("logical", "resolved", "source", "provenance", "pit"),
        run_generation_digest="generation",
        operation_kind="BUILD",
        plan_ref="cas:plan",
    )

    with pytest.raises(StateConflict, match="invalid run transition"):
        machine.transition_unowned_run(
            run_id=run["run_id"],
            expected_state="QUEUED",
            expected_row_version=run["row_version"],
            next_state="SUCCEEDED",
            outcome="CANDIDATE_VALIDATED",
        )

    assert store.get_run(run["run_id"])["state"] == "QUEUED"


def test_existing_generation_atomically_links_and_releases_second_resolver(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine = DatasetReleaseStateMachine(store)
    intent = IntentSpec("logical", "resolved", "source", "provenance", "pit")
    existing = machine.create_queued_run(
        intent=intent,
        run_generation_digest="generation",
        operation_kind="BUILD",
        plan_ref="cas:plan",
    )
    submission = store.submit(
        principal="operator",
        route="runs",
        idempotency_key="second-resolver",
        request_hash="second-request",
        logical_request_key="logical",
        request_ref="cas:request",
    )
    claim = LeaseManager(store).claim_resolution(
        submission_id=submission["submission_id"],
        owner_identity="resolver",
        ttl_seconds=60,
    )
    valid_until = datetime.now(UTC) + timedelta(minutes=5)
    machine.record_resolution_snapshot(
        submission_id=submission["submission_id"],
        resolution_attempt_id=claim.attempt_id,
        resolution_fence=claim.attempt_fence,
        source_content_root="source",
        source_provenance_root="provenance",
        pit_snapshot_digest="pit",
        source_probe_ordinal=1,
        source_probe_key="probe-key",
        source_probe_ref="cas:probe",
        source_probe_valid_until=valid_until,
    )
    snapshot = ResolutionSnapshotSpec(
        source_content_root="source",
        source_provenance_root="provenance",
        pit_snapshot_digest="pit",
        source_probe_ordinal=1,
        source_probe_key="probe-key",
        source_probe_ref="cas:probe",
        source_probe_valid_until=valid_until,
    )

    linked = machine.create_queued_run(
        intent=intent,
        run_generation_digest="generation",
        operation_kind="BUILD",
        plan_ref="cas:plan",
        submission_id=submission["submission_id"],
        resolution_attempt_id=claim.attempt_id,
        resolution_fence=claim.attempt_fence,
        expected_resolution_snapshot=snapshot,
    )

    assert linked["run_id"] == existing["run_id"]
    resolved = store.get_submission(submission["submission_id"])
    assert resolved["state"] == "RESOLVED_TO_EXISTING"
    assert resolved["run_id"] == existing["run_id"]
    assert resolved["resolution_attempt_id"] is None
    assert store.get_resolution_attempt(claim.attempt_id)["state"] == "RELEASED_SUCCEEDED"
    assert store.get_lease(claim.resolution.resource_key)["state"] == "FREE"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_content_root", "other-source"),
        ("source_provenance_root", "other-provenance"),
        ("pit_snapshot_digest", "other-pit"),
        ("source_probe_ordinal", 2),
        ("source_probe_key", "other-probe-key"),
        ("source_probe_ref", "cas:other-probe"),
        ("source_probe_valid_until", datetime(2099, 1, 1, tzinfo=UTC)),
    ],
)
def test_queued_run_rejects_resolution_snapshot_drift_before_releasing_lease(
    tmp_path, field: str, value: object
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine = DatasetReleaseStateMachine(store)
    submission = _submission(store, "snapshot-drift")
    claim = LeaseManager(store).claim_resolution(
        submission_id=submission["submission_id"],
        owner_identity="resolver",
        ttl_seconds=60,
    )
    valid_until = datetime.now(UTC) + timedelta(minutes=5)
    machine.record_resolution_snapshot(
        submission_id=submission["submission_id"],
        resolution_attempt_id=claim.attempt_id,
        resolution_fence=claim.attempt_fence,
        source_content_root="source",
        source_provenance_root="provenance",
        pit_snapshot_digest="pit",
        source_probe_ordinal=1,
        source_probe_key="probe-key",
        source_probe_ref="cas:probe",
        source_probe_valid_until=valid_until,
    )
    expected = replace(
        ResolutionSnapshotSpec(
            source_content_root="source",
            source_provenance_root="provenance",
            pit_snapshot_digest="pit",
            source_probe_ordinal=1,
            source_probe_key="probe-key",
            source_probe_ref="cas:probe",
            source_probe_valid_until=valid_until,
        ),
        **{field: value},
    )

    with pytest.raises(StateConflict, match="immutable identity mismatch"):
        machine.create_queued_run(
            intent=IntentSpec("qe:2026-07-31:full", "resolved-drift", "source", "provenance", "pit"),
            run_generation_digest="generation-drift",
            operation_kind="BUILD",
            plan_ref="cas:plan",
            submission_id=submission["submission_id"],
            resolution_attempt_id=claim.attempt_id,
            resolution_fence=claim.attempt_fence,
            expected_resolution_snapshot=expected,
        )

    assert store.get_resolution_attempt(claim.attempt_id)["state"] == "RUNNING"
    assert store.get_lease(claim.resolution.resource_key)["state"] == "ACTIVE"
    assert store._many("SELECT * FROM runs", ()) == []


def test_queued_run_rejects_expired_resolution_snapshot_before_releasing_lease(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine = DatasetReleaseStateMachine(store)
    submission = _submission(store, "expired-snapshot")
    claim = LeaseManager(store).claim_resolution(
        submission_id=submission["submission_id"],
        owner_identity="resolver",
        ttl_seconds=60,
    )
    valid_until = datetime.now(UTC) - timedelta(seconds=1)
    expected = ResolutionSnapshotSpec(
        source_content_root="source",
        source_provenance_root="provenance",
        pit_snapshot_digest="pit",
        source_probe_ordinal=1,
        source_probe_key="probe-key",
        source_probe_ref="cas:probe",
        source_probe_valid_until=valid_until,
    )
    machine.record_resolution_snapshot(
        submission_id=submission["submission_id"],
        resolution_attempt_id=claim.attempt_id,
        resolution_fence=claim.attempt_fence,
        source_content_root=expected.source_content_root,
        source_provenance_root=expected.source_provenance_root,
        pit_snapshot_digest=expected.pit_snapshot_digest,
        source_probe_ordinal=expected.source_probe_ordinal,
        source_probe_key=expected.source_probe_key,
        source_probe_ref=expected.source_probe_ref,
        source_probe_valid_until=valid_until,
    )

    with pytest.raises(StateConflict, match="expired"):
        machine.create_queued_run(
            intent=IntentSpec("qe:2026-07-31:full", "resolved-expired", "source", "provenance", "pit"),
            run_generation_digest="generation-expired",
            operation_kind="BUILD",
            plan_ref="cas:plan",
            submission_id=submission["submission_id"],
            resolution_attempt_id=claim.attempt_id,
            resolution_fence=claim.attempt_fence,
            expected_resolution_snapshot=expected,
        )

    assert store.get_resolution_attempt(claim.attempt_id)["state"] == "RUNNING"
    assert store.get_lease(claim.resolution.resource_key)["state"] == "ACTIVE"
    assert store._many("SELECT * FROM runs", ()) == []


def test_queued_run_rejects_resolution_attempt_from_another_submission(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine = DatasetReleaseStateMachine(store)
    first = _submission(store, "cross-source")
    second = store.submit(
        principal="operator",
        route="runs",
        idempotency_key="key-cross-target",
        request_hash="request-cross-target",
        logical_request_key="qe:2026-07-31:other",
        request_ref="cas:request-cross-target",
    )
    first_claim = LeaseManager(store).claim_resolution(
        submission_id=first["submission_id"],
        owner_identity="resolver-first",
        ttl_seconds=60,
    )
    second_claim = LeaseManager(store).claim_resolution(
        submission_id=second["submission_id"],
        owner_identity="resolver-second",
        ttl_seconds=60,
    )
    valid_until = datetime.now(UTC) + timedelta(minutes=5)
    expected = ResolutionSnapshotSpec(
        source_content_root="source",
        source_provenance_root="provenance",
        pit_snapshot_digest="pit",
        source_probe_ordinal=1,
        source_probe_key="probe-key",
        source_probe_ref="cas:probe",
        source_probe_valid_until=valid_until,
    )
    machine.record_resolution_snapshot(
        submission_id=first["submission_id"],
        resolution_attempt_id=first_claim.attempt_id,
        resolution_fence=first_claim.attempt_fence,
        source_content_root=expected.source_content_root,
        source_provenance_root=expected.source_provenance_root,
        pit_snapshot_digest=expected.pit_snapshot_digest,
        source_probe_ordinal=expected.source_probe_ordinal,
        source_probe_key=expected.source_probe_key,
        source_probe_ref=expected.source_probe_ref,
        source_probe_valid_until=valid_until,
    )

    with pytest.raises(StateConflict, match="does not own submission"):
        machine.create_queued_run(
            intent=IntentSpec("qe:2026-07-31:other", "resolved-cross", "source", "provenance", "pit"),
            run_generation_digest="generation-cross",
            operation_kind="BUILD",
            plan_ref="cas:plan",
            submission_id=second["submission_id"],
            resolution_attempt_id=first_claim.attempt_id,
            resolution_fence=first_claim.attempt_fence,
            expected_resolution_snapshot=expected,
        )

    assert store.get_lease(first_claim.resolution.resource_key)["state"] == "ACTIVE"
    assert store.get_lease(second_claim.resolution.resource_key)["state"] == "ACTIVE"
    assert store._many("SELECT * FROM runs", ()) == []


def test_resume_lineage_allows_one_active_leaf_and_replays_same_generation(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    machine = DatasetReleaseStateMachine(store)
    original = machine.create_queued_run(
        intent=IntentSpec("logical", "resolved", "source", "provenance", "pit"),
        run_generation_digest="build-generation",
        operation_kind="BUILD",
        plan_ref="cas:plan",
    )
    terminal = machine.transition_unowned_run(
        run_id=original["run_id"],
        expected_state="QUEUED",
        expected_row_version=original["row_version"],
        next_state="CANCELLED",
        outcome="CANCELLED",
    )

    resume = machine.create_resume_run(
        resumes_run_id=terminal["run_id"],
        run_generation_digest="resume-generation-1",
        plan_ref="cas:resume-plan",
    )
    replay = machine.create_resume_run(
        resumes_run_id=terminal["run_id"],
        run_generation_digest="resume-generation-1",
        plan_ref="cas:resume-plan",
    )

    assert replay["run_id"] == resume["run_id"]
    with pytest.raises(StateConflict, match="latest lineage leaf|RESUME_LINEAGE_ACTIVE"):
        machine.create_resume_run(
            resumes_run_id=terminal["run_id"],
            run_generation_digest="resume-generation-2",
            plan_ref="cas:other-plan",
        )
