from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from backend.services.dataset_release.attestation import AttestationResult
from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.contracts import (
    Component,
    EquivalenceMode,
    LogicalRequestIdentity,
    ResolvedIntentIdentity,
    Scope,
    SubmissionIdentity,
    ValidationCompatibility,
    attestation_observation_key,
)
from backend.services.dataset_release.control_store import (
    ControlStore,
    SourceSnapshotCatalogSpec,
    StateConflict,
)
from backend.services.dataset_release.decision import ComponentDecisionInput, build_action_plan
from backend.services.dataset_release.errors import DecisionError, IdentityConflictError
from backend.services.dataset_release.lease import LeaseConflict
from backend.services.dataset_release.resolution import (
    BUILD_INPUTS_SCHEMA_VERSION,
    ResolutionService,
    SourceSnapshot,
)
from backend.services.dataset_release.source_rows_codec import (
    StreamingCompressionStats,
    iter_gzip_level1,
)


def _digest(char: str) -> str:
    return char * 64


def _logical() -> str:
    return LogicalRequestIdentity(
        "qe_hmm_full_v1",
        date(2026, 7, 31),
        Scope.FULL,
        _digest("a"),
    ).key


def _submit(service: ResolutionService, suffix: str) -> dict:
    return service.submit(
        identity=SubmissionIdentity(
            "operator",
            "POST:/api/v1/dataset-releases/runs",
            f"monthly-{suffix}",
        ),
        logical_request_key=_logical(),
        request_payload={
            "profile": "qe_hmm_full_v1",
            "cutoff": "2026-07-31",
            "scope": "full",
            "candidate_only": True,
        },
    )


def _probe(
    service,
    submission,
    claim,
    now,
    ordinal=1,
    *,
    source_provenance_root: str | None = None,
    snapshot_tokens: tuple[str, ...] = ("xmin:100", "provider:none"),
):
    return service.record_source_probe(
        submission_id=submission["submission_id"],
        claim=claim,
        candidate_identity=_digest("b"),
        artifact_root=_digest("c"),
        snapshot=SourceSnapshot(
            _digest("d"),
            source_provenance_root or _digest("e"),
            _digest("f"),
            snapshot_tokens,
        ),
        probe_policy_version="monthly_source_probe_v1",
        probe_ordinal=ordinal,
        observed_at=now,
        ttl=timedelta(minutes=30),
    )


def _build_inputs(cas: CASStore, probe) -> dict:
    source_manifest_ref = cas.put_json({"schema_version": "fixture-source-manifest-v1"})
    pit_snapshot_ref = cas.put_json({"schema_version": "fixture-pit-v1"})
    compression = StreamingCompressionStats()
    rows_ref = cas.put_stream(
        iter_gzip_level1(
            (b'{"row_key":"1","row_payload":"fixture"}\n',),
            compression,
        )
    )
    resolved_intent_key = ResolvedIntentIdentity(
        probe.logical_request_key,
        probe.snapshot.source_content_root,
        probe.snapshot.pit_snapshot_digest,
    ).key
    raw_source_content_root = _digest("0")
    effective_partitions = {
        component.value: [
            {
                "component": component.value,
                "dataset": f"fixture-{component.value}",
                "partition_key": "2026-07",
                "effective_content_sha256": _digest("6"),
            }
        ]
        for component in Component
    }
    component_refs = {}
    for component in Component:
        component_refs[component.value] = cas.put_json(
            {
                "component": component.value,
                "source_content_root": raw_source_content_root,
                "effective_partitions": effective_partitions[component.value],
            }
        ).as_dict()
    artifact_ready_contract_ref = cas.put_json(
        {
            "source_content_root": raw_source_content_root,
            "pit_snapshot_digest": probe.snapshot.pit_snapshot_digest,
            "artifact_ready_content_root": probe.snapshot.source_content_root,
            "artifact_ready_effective_content_root": probe.snapshot.source_content_root,
            "artifact_ready_provenance_root": probe.snapshot.source_provenance_root,
            "component_manifests": component_refs,
            "provider_receipt_refs": [],
            "derived_source_receipt_refs": [],
        }
    )
    return {
        "schema_version": BUILD_INPUTS_SCHEMA_VERSION,
        "profile": "qe_hmm_full_v1",
        "scope": "full",
        "cutoff": "2026-07-31",
        "logical_request_key": probe.logical_request_key,
        "resolved_intent_key": resolved_intent_key,
        "semantic_profile_digest": _digest("a"),
        "predicted_new_bytes": 128 * 1024**3,
        "source_manifest_ref": source_manifest_ref.as_dict(),
        "artifact_ready_contract_ref": artifact_ready_contract_ref.as_dict(),
        "artifact_ready_content_root": probe.snapshot.source_content_root,
        "artifact_ready_provenance_root": probe.snapshot.source_provenance_root,
        "provider_receipt_refs": [],
        "artifact_ready_derived_source_receipt_refs": [],
        "pit_snapshot_ref": pit_snapshot_ref.as_dict(),
        "source_snapshot": {
            "source_content_root": probe.snapshot.source_content_root,
            "raw_source_content_root": raw_source_content_root,
            "artifact_ready_content_root": probe.snapshot.source_content_root,
            "artifact_ready_provenance_root": probe.snapshot.source_provenance_root,
            "pit_snapshot_digest": probe.snapshot.pit_snapshot_digest,
        },
        "source_probe": {
            "subject_kind": probe.subject_kind.value,
            "subject_identity": probe.subject_identity,
            "candidate_identity": probe.candidate_identity,
            "artifact_root": probe.artifact_root,
        },
        "partitions": [
            {
                "component": Component.DAILY_BIN.value,
                "dataset": "fixture",
                "partition_key": "2026-07",
                "query_version": "fixture-v1",
                "schema_digest": _digest("7"),
                "columns": [
                    {"name": "row_key", "kind": "string", "required": True},
                    {"name": "row_payload", "kind": "string", "required": True},
                ],
                "primary_keys": ["row_key"],
                "row_count": 1,
                "content_digest": _digest("8"),
                "merkle_root": _digest("9"),
                "rows_ref": rows_ref.as_dict(),
                **compression.as_descriptor_fields(),
            }
        ],
        "artifact_ready_effective_partitions": effective_partitions,
        "baseline": {},
        "fingerprints": {},
        "safety": {},
    }


def _source_catalog(
    cas: CASStore,
    probe,
    *,
    nonce: str = "",
) -> SourceSnapshotCatalogSpec:
    refs = [
        cas.put_json(
            {
                "fixture": name,
                "nonce": nonce,
                **({"source_provenance_root": probe.snapshot.source_provenance_root} if name == "provenance" else {}),
            }
        ).sha256
        for name in ("content", "reuse", "audit", "provenance", "pit")
    ]
    fields = {
        "profile": "qe_hmm_full_v1",
        "scope": "full",
        "cutoff": date(2026, 7, 31),
        "source_content_root": probe.snapshot.source_content_root,
        "source_provenance_root": probe.snapshot.source_provenance_root,
        "stable_source_provenance_root": _digest("4"),
        "source_content_manifest_ref": refs[0],
        "source_reuse_manifest_ref": refs[1],
        "source_refresh_audit_ref": refs[2],
        "source_provenance_ref": refs[3],
        "pit_snapshot_digest": probe.snapshot.pit_snapshot_digest,
        "pit_snapshot_ref": refs[4],
    }
    return SourceSnapshotCatalogSpec(
        observation_id=digest_named_fields("dataset_release_source_snapshot_observation_v1", fields),
        **fields,
        observed_at=probe.observed_at,
    )


def test_resolution_lease_enforces_single_active_logical_request(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    service = ResolutionService(store, CASStore(store.root))
    first = _submit(service, "first")
    second = _submit(service, "second")
    service.claim(
        submission_id=first["submission_id"],
        owner_identity="worker-1",
        ttl_seconds=60,
    )
    with pytest.raises(LeaseConflict):
        service.claim(
            submission_id=second["submission_id"],
            owner_identity="worker-2",
            ttl_seconds=60,
        )
    assert store.get_submission(second["submission_id"])["state"] == "QUEUED_RESOLUTION"


def test_source_probe_ttl_and_ordinal_create_fresh_distinct_evidence(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = ResolutionService(store, cas)
    submission = _submit(service, "probe")
    now = datetime.now(UTC)
    claim = service.claim(
        submission_id=submission["submission_id"],
        owner_identity="worker",
        ttl_seconds=60,
        now=now,
    )
    first = _probe(service, submission, claim, now, ordinal=1)
    second = _probe(service, submission, claim, now + timedelta(seconds=1), ordinal=2)
    assert first.source_probe_key != second.source_probe_key
    assert first.receipt_digest != second.receipt_digest
    assert first.is_fresh(now=now + timedelta(minutes=1))
    assert not first.is_fresh(now=now + timedelta(hours=1))
    payload = cas.get_json(second.cas_ref)
    assert payload["source_content_root"] == _digest("d")
    assert payload["pit_snapshot_digest"] == _digest("f")
    assert payload["safety"]["database_writes"] == 0


@pytest.mark.parametrize("replayed_ordinal", (1, 2))
def test_source_probe_ordinal_cannot_repeat_or_move_backward(
    tmp_path,
    replayed_ordinal: int,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    service = ResolutionService(store, CASStore(store.root))
    submission = _submit(service, f"probe-ordinal-{replayed_ordinal}")
    now = datetime.now(UTC)
    claim = service.claim(
        submission_id=submission["submission_id"],
        owner_identity="worker",
        ttl_seconds=60,
        now=now,
    )
    accepted = _probe(service, submission, claim, now, ordinal=2)

    with pytest.raises(StateConflict, match="ordinal"):
        _probe(
            service,
            submission,
            claim,
            now + timedelta(seconds=1),
            ordinal=replayed_ordinal,
        )

    durable = store.get_resolution_attempt(claim.attempt_id)
    assert durable["source_probe_ordinal"] == 2
    assert durable["source_probe_key"] == accepted.source_probe_key
    assert durable["source_probe_ref"] == accepted.cas_ref.sha256


def test_source_snapshot_catalog_is_idempotent_drift_safe_and_tie_safe(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = ResolutionService(store, cas)
    submission = _submit(service, "source-catalog")
    now = datetime.now(UTC)
    claim = service.claim(
        submission_id=submission["submission_id"],
        owner_identity="worker",
        ttl_seconds=60,
        now=now,
    )
    probe = _probe(service, submission, claim, now)
    first = _source_catalog(cas, probe, nonce="first")
    tied = _source_catalog(cas, probe, nonce="tied")

    assert store.register_source_snapshot(first)["observation_id"] == first.observation_id
    assert store.register_source_snapshot(first)["observation_id"] == first.observation_id
    with pytest.raises(StateConflict, match="drifted immutable"):
        store.register_source_snapshot(replace(first, source_reuse_manifest_ref=tied.source_reuse_manifest_ref))
    store.register_source_snapshot(tied)
    with pytest.raises(StateConflict, match="ambiguous"):
        store.latest_source_snapshot(
            profile="qe_hmm_full_v1",
            scope="full",
            cutoff_on_or_before=date(2026, 7, 31),
        )


def _registered_attestation(service, cas, probe, now) -> AttestationResult:
    receipt = cas.put_json({"schema_version": "fixture-attestation"})
    attestation_target_key = _digest("5")
    attestation_key = attestation_observation_key(
        attestation_target_key,
        probe.source_probe_key,
    )
    attestation_id = service.state_machine.register_attestation(
        attestation_id=None,
        attestation_key=attestation_key,
        attestation_target_key=attestation_target_key,
        subject_type="candidate",
        subject_digest=probe.candidate_identity,
        candidate_identity=probe.candidate_identity,
        producer_provenance_state="KNOWN",
        producer_provenance_digest_or_sentinel=_digest("8"),
        candidate_artifact_root=probe.artifact_root,
        current_source_content_root=probe.snapshot.source_content_root,
        source_probe_key=probe.source_probe_key,
        source_probe_ref=probe.cas_ref.sha256,
        pit_snapshot_digest=probe.snapshot.pit_snapshot_digest,
        semantic_profile_digest=_digest("9"),
        validation_fingerprint=_digest("6"),
        observed_at=now,
        valid_until=now + timedelta(minutes=30),
        equivalence_mode=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT.value,
        outcome=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT.value,
        receipt_ref=receipt.sha256,
        committed=True,
    )
    return AttestationResult(
        attestation_id=attestation_id,
        attestation_key=attestation_key,
        attestation_target_key=attestation_target_key,
        candidate_identity=probe.candidate_identity,
        receipt_ref=receipt,
        artifact_root=probe.artifact_root,
        outcome=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT,
        eligible_for_noop_reuse=True,
        current_source_content_root=probe.snapshot.source_content_root,
        pit_snapshot_digest=probe.snapshot.pit_snapshot_digest,
        semantic_profile_digest=_digest("9"),
        validation_fingerprint=_digest("6"),
        valid_until=now + timedelta(minutes=30),
        run={},
    )


def test_fresh_noop_uses_atomic_state_machine_terminal_path(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = ResolutionService(store, cas)
    submission = _submit(service, "noop")
    now = datetime.now(UTC)
    claim = service.claim(
        submission_id=submission["submission_id"],
        owner_identity="worker",
        ttl_seconds=60,
        now=now,
    )
    probe = _probe(service, submission, claim, now)
    attestation = _registered_attestation(service, cas, probe, now)

    result = service.resolve_noop(
        submission_id=submission["submission_id"],
        claim=claim,
        probe=probe,
        attestation=attestation,
        producer_fingerprint=_digest("1"),
        artifact_fingerprint=_digest("2"),
        sample_policy="on_contract_change",
        source_snapshot_catalog=_source_catalog(cas, probe),
        now=now + timedelta(seconds=1),
    )

    assert result.run["state"] == "SUCCEEDED"
    assert result.run["outcome"] == "NO_OP_VERIFIED"
    durable_submission = store.get_submission(submission["submission_id"])
    assert durable_submission["state"] == "RESOLVED_NO_OP"
    assert durable_submission["run_id"] == result.run["run_id"]
    assert store.get_resolution_attempt(claim.attempt_id)["state"] == "RELEASED_SUCCEEDED"
    with store.transaction(immediate=False) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM attempts WHERE run_id=?",
                (result.run["run_id"],),
            ).fetchone()[0]
            == 0
        )
    receipt = cas.get_json(result.receipt_ref)
    assert receipt["outcome"] == "NO_OP_VERIFIED"
    assert "fence" not in receipt and "attempt_id" not in receipt
    assert all(value == 0 for value in receipt["safety"].values())


def test_expired_probe_cannot_create_noop_run(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = ResolutionService(store, cas)
    submission = _submit(service, "expired")
    now = datetime.now(UTC)
    claim = service.claim(
        submission_id=submission["submission_id"],
        owner_identity="worker",
        ttl_seconds=60,
        now=now,
    )
    probe = _probe(service, submission, claim, now)
    attestation = _registered_attestation(service, cas, probe, now)
    with pytest.raises(DecisionError, match="not fresh"):
        service.resolve_noop(
            submission_id=submission["submission_id"],
            claim=claim,
            probe=probe,
            attestation=attestation,
            producer_fingerprint=_digest("1"),
            artifact_fingerprint=_digest("2"),
            sample_policy="on_contract_change",
            source_snapshot_catalog=_source_catalog(cas, probe),
            now=now + timedelta(hours=1),
        )
    with store.transaction(immediate=False) as connection:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0


def test_expired_attestation_cannot_create_noop_run(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = ResolutionService(store, cas)
    submission = _submit(service, "expired-attestation")
    now = datetime.now(UTC)
    claim = service.claim(
        submission_id=submission["submission_id"],
        owner_identity="worker",
        ttl_seconds=60,
        now=now,
    )
    probe = _probe(service, submission, claim, now)
    attestation = replace(
        _registered_attestation(service, cas, probe, now),
        valid_until=now,
    )

    with pytest.raises(DecisionError, match="attestation is not fresh"):
        service.resolve_noop(
            submission_id=submission["submission_id"],
            claim=claim,
            probe=probe,
            attestation=attestation,
            producer_fingerprint=_digest("1"),
            artifact_fingerprint=_digest("2"),
            sample_policy="on_contract_change",
            source_snapshot_catalog=_source_catalog(cas, probe),
            now=now + timedelta(seconds=1),
        )
    with store.transaction(immediate=False) as connection:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0


def _full_build_plan(*, reverse: bool = False):
    components = list(Component)
    if reverse:
        components.reverse()
    return build_action_plan(
        [ComponentDecisionInput(component, "all", semantic_compatible=False) for component in components]
    )


def _reattest_plan():
    return build_action_plan(
        [
            ComponentDecisionInput(
                component,
                "all",
                source_equivalence_current=True,
                validation_compatibility=(ValidationCompatibility.VALIDATOR_STRENGTHENING_COMPATIBLE),
            )
            for component in Component
        ]
    )


def test_pure_reattest_resolution_binds_fresh_observation_generation(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = ResolutionService(store, cas)
    submission = _submit(service, "reattest")
    now = datetime.now(UTC)
    claim = service.claim(
        submission_id=submission["submission_id"],
        owner_identity="worker",
        ttl_seconds=60,
        now=now,
    )
    probe = _probe(service, submission, claim, now)
    target_key = _digest("5")
    observation_key = attestation_observation_key(target_key, probe.source_probe_key)

    result = service.resolve_action_plan(
        submission_id=submission["submission_id"],
        claim=claim,
        probe=probe,
        action_plan=_reattest_plan(),
        producer_fingerprint=_digest("1"),
        artifact_fingerprint=_digest("2"),
        validation_identity=observation_key,
        sample_policy="on_contract_change",
        attestation_target_key=target_key,
        source_snapshot_catalog=_source_catalog(cas, probe),
        now=now + timedelta(seconds=1),
    )

    assert result.run["operation_kind"] == "REATTEST"
    plan = cas.get_json(result.receipt_ref)
    assert plan["attestation_target_key"] == target_key
    assert plan["attestation_observation_key"] == observation_key
    assert plan["source_probe_key"] == probe.source_probe_key


@pytest.mark.parametrize("missing_target", (True, False))
def test_pure_reattest_resolution_requires_matching_target_and_observation(
    tmp_path,
    missing_target: bool,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = ResolutionService(store, cas)
    submission = _submit(service, f"reattest-invalid-{missing_target}")
    now = datetime.now(UTC)
    claim = service.claim(
        submission_id=submission["submission_id"],
        owner_identity="worker",
        ttl_seconds=60,
        now=now,
    )
    probe = _probe(service, submission, claim, now)
    target_key = None if missing_target else _digest("5")

    error = DecisionError if missing_target else IdentityConflictError
    with pytest.raises(error):
        service.resolve_action_plan(
            submission_id=submission["submission_id"],
            claim=claim,
            probe=probe,
            action_plan=_reattest_plan(),
            producer_fingerprint=_digest("1"),
            artifact_fingerprint=_digest("2"),
            validation_identity=_digest("6"),
            sample_policy="on_contract_change",
            attestation_target_key=target_key,
            source_snapshot_catalog=_source_catalog(cas, probe),
            now=now + timedelta(seconds=1),
        )
    with store.transaction(immediate=False) as connection:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0


def test_build_resolution_links_equivalent_second_submission_to_existing_run(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = ResolutionService(store, cas)
    now = datetime.now(UTC)

    first_submission = _submit(service, "build-first")
    first_claim = service.claim(
        submission_id=first_submission["submission_id"],
        owner_identity="worker-1",
        ttl_seconds=60,
        now=now,
    )
    first_probe = _probe(service, first_submission, first_claim, now)
    first = service.resolve_action_plan(
        submission_id=first_submission["submission_id"],
        claim=first_claim,
        probe=first_probe,
        action_plan=_full_build_plan(reverse=True),
        producer_fingerprint=_digest("1"),
        artifact_fingerprint=_digest("2"),
        validation_identity=_digest("3"),
        sample_policy="on_contract_change",
        build_inputs=_build_inputs(cas, first_probe),
        source_snapshot_catalog=_source_catalog(cas, first_probe),
        now=now + timedelta(seconds=1),
    )
    assert first.run["state"] == "QUEUED"

    second_submission = _submit(service, "build-second")
    second_claim = service.claim(
        submission_id=second_submission["submission_id"],
        owner_identity="worker-2",
        ttl_seconds=60,
        now=now + timedelta(seconds=2),
    )
    second_probe = _probe(
        service,
        second_submission,
        second_claim,
        now + timedelta(seconds=2),
        source_provenance_root=_digest("6"),
        snapshot_tokens=("xmin:101", "provider:none"),
    )
    second = service.resolve_action_plan(
        submission_id=second_submission["submission_id"],
        claim=second_claim,
        probe=second_probe,
        action_plan=_full_build_plan(),
        producer_fingerprint=_digest("1"),
        artifact_fingerprint=_digest("2"),
        validation_identity=_digest("3"),
        sample_policy="on_contract_change",
        build_inputs=_build_inputs(cas, second_probe),
        source_snapshot_catalog=_source_catalog(cas, second_probe),
        now=now + timedelta(seconds=3),
    )
    assert second.run["run_id"] == first.run["run_id"]
    assert store.get_submission(second_submission["submission_id"])["state"] == ("RESOLVED_TO_EXISTING")
    assert store.get_resolution_attempt(second_claim.attempt_id)["state"] == ("RELEASED_SUCCEEDED")
    with store.transaction(immediate=False) as connection:
        catalog_rows = connection.execute(
            "SELECT source_content_root,source_provenance_root FROM source_snapshot_catalog ORDER BY observed_at"
        ).fetchall()
    assert [row["source_content_root"] for row in catalog_rows] == [
        _digest("d"),
        _digest("d"),
    ]
    assert {row["source_provenance_root"] for row in catalog_rows} == {
        _digest("e"),
        _digest("6"),
    }


def test_build_resolution_rejects_probe_from_another_submission_atomically(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = ResolutionService(store, cas)
    now = datetime.now(UTC)

    first_submission = _submit(service, "probe-owner")
    first_claim = service.claim(
        submission_id=first_submission["submission_id"],
        owner_identity="worker-1",
        ttl_seconds=60,
        now=now,
    )
    first_probe = _probe(service, first_submission, first_claim, now)
    first_result = service.resolve_action_plan(
        submission_id=first_submission["submission_id"],
        claim=first_claim,
        probe=first_probe,
        action_plan=_full_build_plan(),
        producer_fingerprint=_digest("1"),
        artifact_fingerprint=_digest("2"),
        validation_identity=_digest("3"),
        sample_policy="on_contract_change",
        build_inputs=_build_inputs(cas, first_probe),
        source_snapshot_catalog=_source_catalog(cas, first_probe),
        now=now + timedelta(seconds=1),
    )

    second_submission = _submit(service, "different-probe-owner")
    second_claim = service.claim(
        submission_id=second_submission["submission_id"],
        owner_identity="worker-2",
        ttl_seconds=60,
        now=now + timedelta(seconds=2),
    )
    _probe(
        service,
        second_submission,
        second_claim,
        now + timedelta(seconds=2),
    )
    with pytest.raises(StateConflict, match="immutable identity mismatch"):
        service.resolve_action_plan(
            submission_id=second_submission["submission_id"],
            claim=second_claim,
            probe=first_probe,
            action_plan=_full_build_plan(),
            producer_fingerprint=_digest("1"),
            artifact_fingerprint=_digest("2"),
            validation_identity=_digest("3"),
            sample_policy="on_contract_change",
            build_inputs=_build_inputs(cas, first_probe),
            source_snapshot_catalog=_source_catalog(cas, first_probe, nonce="must-rollback"),
            now=now + timedelta(seconds=3),
        )

    assert store.get_submission(second_submission["submission_id"])["state"] == ("RESOLVING_SOURCE")
    assert store.get_resolution_attempt(second_claim.attempt_id)["state"] == "RUNNING"
    assert store.get_lease(second_claim.resolution.resource_key)["state"] == "ACTIVE"
    with store.transaction(immediate=False) as connection:
        runs = connection.execute("SELECT run_id FROM runs").fetchall()
        catalog_count = connection.execute("SELECT COUNT(*) FROM source_snapshot_catalog").fetchone()[0]
    assert [row["run_id"] for row in runs] == [first_result.run["run_id"]]
    assert catalog_count == 1
