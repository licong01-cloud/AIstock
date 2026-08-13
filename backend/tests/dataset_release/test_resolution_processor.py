from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.dataset_release.attestation import (
    ATTESTATION_SCHEMA_VERSION,
    AttestationResult,
)
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.contracts import (
    AttestationIdentity,
    EquivalenceMode,
    LogicalRequestIdentity,
    PitProvenanceState,
    ProducerProvenanceState,
    Scope,
    SubmissionIdentity,
)
from backend.services.dataset_release.control_store import (
    CandidateRegistrationSpec,
    ControlStore,
)
from backend.services.dataset_release.control_service import (
    DatasetReleaseControlService,
    DatasetReleaseProfileBinding,
)
from backend.services.dataset_release.profile import load_dataset_profile, load_initial_migration_plan
from backend.services.dataset_release.resolution import (
    ResolutionService,
    SourceSnapshot,
)
from backend.services.dataset_release.resolution_processor import (
    MonthlyResolutionProcessor,
    ResolutionProcessorError,
    ResolutionRequestInvalid,
    SupervisedResolutionSourceStage,
)
from backend.services.dataset_release.source_authority import (
    SOURCE_AUTHORITY_POLICY_VERSION,
    SOURCE_REUSE_MANIFEST_SCHEMA,
)
from backend.services.dataset_release.worker import WORKER_ERROR_RECEIPT_SCHEMA


ROOT = Path(__file__).resolve().parents[3]
V2_PROFILE_PATH = ROOT / "configs" / "datasets" / "qe_backtest_monthly_v2.yaml"
INITIAL_PLAN_PATH = ROOT / "configs" / "datasets" / "migrations" / "pit_v2_initial_20260731_v1.yaml"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def test_production_resolution_processor_uses_supervised_source_stage(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    processor = MonthlyResolutionProcessor(
        dataset_profile,
        store,
        CASStore(store.root),
    )
    assert isinstance(processor.source_stage, SupervisedResolutionSourceStage)


def test_resolution_resource_spec_restores_waiting_pressure_rung(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    submission = store.submit(
        principal="operator",
        route="runs",
        idempotency_key="pressure-rung",
        request_hash="request-pressure-rung",
        logical_request_key="logical-pressure-rung",
        request_ref="request-ref",
    )
    claim = ResolutionService(store, cas).claim(
        submission_id=submission["submission_id"],
        owner_identity="worker",
        ttl_seconds=60,
    )
    error_ref = cas.put_json(
        {
            "schema_version": WORKER_ERROR_RECEIPT_SCHEMA,
            "kind": "resolution",
            "target_id": submission["submission_id"],
            "disposition": "WAITING",
            "context": {"pressure_rung": 1, "data_scope_changed": False},
        }
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE resolution_attempts SET state='RELEASED_WAITING',error_ref=? WHERE resolution_attempt_id=?",
            (error_ref.sha256, claim.attempt_id),
        )
    processor = MonthlyResolutionProcessor(
        dataset_profile,
        store,
        cas,
        source_authority=object(),
    )
    processor._predicted_new_bytes = lambda _submission: 0
    assert processor.resource_spec(submission).pressure_rung == 1


def test_supervised_source_stage_uses_versioned_timeout_and_rung(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    stage = SupervisedResolutionSourceStage(dataset_profile, store, cas)
    observed: dict[str, Any] = {}

    class Context:
        claim = SimpleNamespace(attempt_id="attempt-1", attempt_fence=7)

        def run_supervised(self, command, **kwargs):
            observed["command"] = list(command)
            observed.update(kwargs)
            return SimpleNamespace(
                returncode=1,
                active_processes=0,
                runtime="windows",
            )

    with pytest.raises(ResolutionProcessorError, match="source stage failed"):
        stage.freeze(
            Context(),
            cutoff=date(2026, 7, 31),
            baseline_reuse_ref=None,
            baseline_partitions=(),
            predicted_new_bytes=1,
            pressure_rung=2,
            sample_instruments=("000001.SZ", "600462.SH"),
        )

    assert observed["timeout_seconds"] == float(dataset_profile.stage_timeouts_seconds["source_freeze"])
    command = observed["command"]
    assert command[command.index("--pressure-rung") + 1] == "2"
    assert command[command.index("--stage-timeout-seconds") + 1] == str(
        dataset_profile.stage_timeouts_seconds["source_freeze"]
    )
    assert [command[index + 1] for index, value in enumerate(command) if value == "--sample-instrument"] == [
        "000001.SZ",
        "600462.SH",
    ]


def test_resolution_reader_revalidates_fixed_initial_plan_and_sample_scope(tmp_path) -> None:
    profile = load_dataset_profile(V2_PROFILE_PATH)
    plan = load_initial_migration_plan(INITIAL_PLAN_PATH)
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = DatasetReleaseControlService(
        (
            DatasetReleaseProfileBinding(
                profile_id=profile.profile,
                semantic_profile_digest=profile.semantic_profile_digest,
                cutoff_policy=profile.cutoff_policy,
                store=store,
                cas=cas,
                cutoff_resolver=lambda _: date(2099, 12, 31),
                initial_migration_plans={plan.plan_id: plan},
            ),
        )
    )
    submitted = service.submit_initial_migration(
        profile_id=profile.profile,
        plan_id=plan.plan_id,
        scope="sample",
        candidate_only=True,
        principal="operator",
        idempotency_key="initial-plan-reader",
        route="cli:initial-migration",
        now=datetime(2027, 3, 15, tzinfo=UTC),
    )
    processor = MonthlyResolutionProcessor(profile, store, cas, source_authority=object())
    request = processor._read_request(store.get_submission(submitted["submission_id"]))

    assert request.is_initial_migration is True
    assert request.cutoff == date(2026, 7, 31)
    assert request.sample_instruments == plan.sample_instruments

    valid_outer = cas.get_json_bounded(
        store.get_submission(submitted["submission_id"])["request_ref"],
        max_bytes=2 * 1024**2,
    )
    tampered = {**valid_outer["request"], "plan_digest": "c" * 64, "logical_request_key": "d" * 64}
    rejected = ResolutionService(store, cas).submit(
        identity=SubmissionIdentity(
            principal="operator",
            route="cli:initial-migration",
            idempotency_key="initial-plan-reader-tampered",
        ),
        logical_request_key="d" * 64,
        request_payload=tampered,
    )
    with pytest.raises(ResolutionRequestInvalid, match="checked-in plan"):
        processor._read_request(store.get_submission(rejected["submission_id"]))

    tampered_logical = {**valid_outer["request"], "logical_request_key": "e" * 64}
    rejected_logical = ResolutionService(store, cas).submit(
        identity=SubmissionIdentity(
            principal="operator",
            route="cli:initial-migration",
            idempotency_key="initial-plan-reader-logical-tampered",
        ),
        logical_request_key="e" * 64,
        request_payload=tampered_logical,
    )
    with pytest.raises(ResolutionRequestInvalid, match="checked-in plan"):
        processor._read_request(store.get_submission(rejected_logical["submission_id"]))


class _FrozenFixtureStage:
    def __init__(self, frozen: Any) -> None:
        self.frozen = frozen
        self.calls = 0

    def freeze(self, context, **_kwargs):
        self.calls += 1
        context.checkpoint()
        return self.frozen


class _ResolutionContext:
    kind = "resolution"
    pressure_rung = 0

    def __init__(self, store: ControlStore, record: dict, claim) -> None:
        self.store = store
        self.record = record
        self.claim = claim
        self.target_id = str(record["submission_id"])
        self.checkpoints = 0

    def checkpoint(self) -> None:
        self.checkpoints += 1


def _monthly_request(dataset_profile) -> dict[str, Any]:
    cutoff = date(2026, 7, 31)
    logical = LogicalRequestIdentity(
        profile=dataset_profile.profile,
        resolved_cutoff=cutoff,
        scope=Scope.FULL,
        semantic_profile_digest=dataset_profile.semantic_profile_digest,
    ).key
    return {
        "schema_version": "dataset_release_monthly_request_v1",
        "profile": dataset_profile.profile,
        "cutoff_policy": "auto-previous-month",
        "cutoff_resolution_policy": ("previous_month_last_completed_trading_day"),
        "resolved_cutoff": cutoff.isoformat(),
        "scope": Scope.FULL.value,
        "candidate_only": True,
        "logical_request_key": logical,
        "semantic_profile_digest": dataset_profile.semantic_profile_digest,
        "resolution": "worker_required",
        "activation": "not_requested",
        "node1": "not_requested",
        "db_repair": "not_requested",
        "restart": "not_requested",
        "cleanup": "not_requested",
    }


def _submit_monthly(
    service: ResolutionService,
    payload: dict[str, Any],
    *,
    suffix: str,
) -> dict:
    return service.submit(
        identity=SubmissionIdentity(
            principal="operator",
            route="POST:/api/v1/dataset-releases/runs",
            idempotency_key=f"processor-renewal-{suffix}",
        ),
        logical_request_key=str(payload["logical_request_key"]),
        request_payload=payload,
    )


def test_processor_cross_submission_fresh_probe_renews_attestation_and_noops(
    dataset_profile,
    tmp_path,
) -> None:
    """A new probe gets a new observation; it never falls through to BUILD."""

    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = ResolutionService(store, cas)
    clock = [datetime(2026, 8, 3, 1, 0, tzinfo=UTC)]
    source_root = _digest("stable-source")
    provenance_root = _digest("observation-provenance")
    stable_provenance_root = _digest("stable-provenance")
    pit_root = _digest("stable-pit")
    artifact_root = _digest("immutable-candidate")
    producer_root = _digest("candidate-producer")
    safety = {
        "database_writes": 0,
        "production_writes": 0,
        "production_deletes": 0,
        "production_pointer_changes": 0,
        "service_process_controls": 0,
        "provider_database_writes": 0,
        "candidate_writes": 0,
    }
    source_manifest_ref = cas.put_json({"fixture": "source-manifest"})
    source_reuse_manifest_ref = cas.put_json(
        {
            "schema_version": SOURCE_REUSE_MANIFEST_SCHEMA,
            "profile": dataset_profile.profile,
            "cutoff": "2026-07-31",
            "source_content_root": source_root,
            "partitions": [],
            "safety": safety,
        }
    )
    source_audit_ref = cas.put_json({"fixture": "source-audit"})
    source_provenance_ref = cas.put_json(
        {
            "fixture": "source-provenance",
            "source_provenance_root": provenance_root,
        }
    )
    pit_ref = cas.put_json({"fixture": "pit"})
    frozen = SimpleNamespace(
        source_content_root=source_root,
        source_provenance_root=provenance_root,
        stable_source_provenance_root=stable_provenance_root,
        pit_snapshot_digest=pit_root,
        source_manifest_ref=source_manifest_ref,
        source_reuse_manifest_ref=source_reuse_manifest_ref,
        source_audit_ref=source_audit_ref,
        source_provenance_ref=source_provenance_ref,
        pit_snapshot_ref=pit_ref,
        snapshot_tokens=("fixture:stable",),
        partitions=(),
        artifact_ready_contract_ref=source_manifest_ref,
        artifact_ready_content_root=source_root,
        artifact_ready_provenance_root=provenance_root,
        provider_receipt_refs=(),
        artifact_ready_derived_source_receipt_refs=(),
    )
    stage = _FrozenFixtureStage(frozen)
    processor = MonthlyResolutionProcessor(
        dataset_profile,
        store,
        cas,
        source_stage=stage,
        now=lambda: clock[0],
    )
    candidate = store.register_candidate(
        CandidateRegistrationSpec(
            allowlisted_root_id="candidate-root",
            volume_serial="fixture-volume",
            root_relative_path="qe/2026-07-31/candidate",
            profile=dataset_profile.profile,
            scope=Scope.FULL.value,
            cutoff=date(2026, 7, 31),
            lineage_anchor=f"BUILD_RELEASE_DIGEST:{_digest('release')}",
            artifact_root=artifact_root,
            producer_provenance_state=ProducerProvenanceState.KNOWN.value,
            producer_provenance_digest_or_sentinel=producer_root,
            pit_provenance_state=PitProvenanceState.KNOWN.value,
            pit_provenance_digest_or_sentinel=pit_root,
            state="ATTESTED",
            last_attested_at=clock[0],
        )
    )
    payload = _monthly_request(dataset_profile)

    first = _submit_monthly(service, payload, suffix="first")
    first_claim = service.claim(
        submission_id=first["submission_id"],
        owner_identity="worker-first",
        ttl_seconds=60,
        now=clock[0],
    )
    first_probe = service.record_source_probe(
        submission_id=first["submission_id"],
        claim=first_claim,
        candidate_identity=str(candidate["candidate_identity"]),
        artifact_root=artifact_root,
        snapshot=SourceSnapshot(
            source_content_root=source_root,
            source_provenance_root=provenance_root,
            pit_snapshot_digest=pit_root,
            snapshot_tokens=frozen.snapshot_tokens,
        ),
        probe_policy_version=SOURCE_AUTHORITY_POLICY_VERSION,
        probe_ordinal=1,
        observed_at=clock[0],
        ttl=timedelta(hours=2),
    )
    identity = AttestationIdentity(
        candidate_identity=str(candidate["candidate_identity"]),
        producer_provenance_state=ProducerProvenanceState.KNOWN,
        producer_provenance_digest_or_sentinel=producer_root,
        artifact_root=artifact_root,
        current_source_content_root=source_root,
        pit_digest=pit_root,
        semantic_profile_digest=dataset_profile.semantic_profile_digest,
        validation_fingerprint=processor.validation_fingerprint,
        equivalence_mode=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT,
        source_probe_key=first_probe.source_probe_key,
    )
    old_valid_until = clock[0] + timedelta(minutes=90)
    old_receipt = cas.put_json(
        {
            "schema_version": ATTESTATION_SCHEMA_VERSION,
            "attestation_key": identity.key,
            "attestation_observation_key": identity.key,
            "attestation_target_key": identity.target_key,
            "candidate_identity": candidate["candidate_identity"],
            "candidate_artifact_root": artifact_root,
            "producer_provenance_state": ProducerProvenanceState.KNOWN.value,
            "producer_provenance_digest_or_sentinel": producer_root,
            "current_source_content_root": source_root,
            "source_probe_key": first_probe.source_probe_key,
            "source_probe_ref": first_probe.cas_ref.sha256,
            "pit_snapshot_digest": pit_root,
            "semantic_profile_digest": dataset_profile.semantic_profile_digest,
            "validation_fingerprint": processor.validation_fingerprint,
            "observed_at": clock[0].isoformat().replace("+00:00", "Z"),
            "valid_until": old_valid_until.isoformat().replace("+00:00", "Z"),
            "equivalence_mode": EquivalenceMode.CURRENT_SOURCE_EQUIVALENT.value,
            "outcome": EquivalenceMode.CURRENT_SOURCE_EQUIVALENT.value,
            "eligible_for_noop_reuse": True,
            "safety": {
                key: value
                for key, value in safety.items()
                if key not in {"provider_database_writes", "candidate_writes"}
            },
        }
    )
    old_attestation_id = service.state_machine.register_attestation(
        attestation_id=None,
        attestation_key=identity.key,
        attestation_target_key=identity.target_key,
        subject_type="candidate",
        subject_digest=str(candidate["candidate_identity"]),
        candidate_identity=str(candidate["candidate_identity"]),
        producer_provenance_state=ProducerProvenanceState.KNOWN.value,
        producer_provenance_digest_or_sentinel=producer_root,
        candidate_artifact_root=artifact_root,
        current_source_content_root=source_root,
        source_probe_key=first_probe.source_probe_key,
        source_probe_ref=first_probe.cas_ref.sha256,
        pit_snapshot_digest=pit_root,
        semantic_profile_digest=dataset_profile.semantic_profile_digest,
        validation_fingerprint=processor.validation_fingerprint,
        observed_at=clock[0],
        valid_until=old_valid_until,
        equivalence_mode=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT.value,
        outcome=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT.value,
        receipt_ref=old_receipt.sha256,
    )
    request = processor._read_request(store.get_submission(first["submission_id"]))
    first_result = service.resolve_noop(
        submission_id=first["submission_id"],
        claim=first_claim,
        probe=first_probe,
        attestation=AttestationResult(
            attestation_id=old_attestation_id,
            attestation_key=identity.key,
            attestation_target_key=identity.target_key,
            candidate_identity=str(candidate["candidate_identity"]),
            receipt_ref=old_receipt,
            artifact_root=artifact_root,
            outcome=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT,
            eligible_for_noop_reuse=True,
            current_source_content_root=source_root,
            pit_snapshot_digest=pit_root,
            semantic_profile_digest=dataset_profile.semantic_profile_digest,
            validation_fingerprint=processor.validation_fingerprint,
            valid_until=old_valid_until,
            run={},
        ),
        producer_fingerprint=processor.producer_fingerprint,
        artifact_fingerprint=processor.artifact_fingerprint,
        sample_policy="on_contract_change",
        source_snapshot_catalog=processor._source_snapshot_catalog_spec(
            request,
            frozen,
            first_probe,
        ),
        now=clock[0] + timedelta(seconds=1),
    )
    assert first_result.run["outcome"] == "NO_OP_VERIFIED"

    candidate_before = store.latest_candidate_registration(
        profile=dataset_profile.profile,
        scope=Scope.FULL.value,
    )
    clock[0] += timedelta(minutes=5)
    second = _submit_monthly(service, payload, suffix="second")
    second_claim = service.claim(
        submission_id=second["submission_id"],
        owner_identity="worker-second",
        ttl_seconds=60,
        now=clock[0],
    )
    context = _ResolutionContext(
        store,
        store.get_submission(second["submission_id"]),
        second_claim,
    )

    result = processor.process(context)

    assert result.disposition == "DURABLE_SUCCESS"
    second_durable = store.get_submission(second["submission_id"])
    assert second_durable["state"] == "RESOLVED_NO_OP"
    second_run = store.get_run(second_durable["run_id"])
    assert second_run["operation_kind"] == "NO_OP"
    assert second_run["outcome"] == "NO_OP_VERIFIED"
    assert stage.calls == 1
    assert context.checkpoints >= 4
    assert (
        store.latest_candidate_registration(
            profile=dataset_profile.profile,
            scope=Scope.FULL.value,
        )
        == candidate_before
    )
    observations = store._many(
        "SELECT * FROM attestations ORDER BY observed_at,attestation_id",
        (),
    )
    assert len(observations) == 2
    renewed = observations[-1]
    assert renewed["attestation_key"] != identity.key
    assert renewed["source_probe_key"] != first_probe.source_probe_key
    assert datetime.fromisoformat(renewed["valid_until"]) == old_valid_until
    renewal_receipt = cas.get_json(renewed["receipt_ref"])
    assert renewal_receipt["schema_version"] == "dataset_release_attestation_renewal_v1"
    assert renewal_receipt["renewed_from"]["attestation_key"] == identity.key
    assert renewal_receipt["renewed_from"]["validity_extended"] is False
    assert len(store._many("SELECT * FROM runs WHERE operation_kind='NO_OP'", ())) == 2
    assert store._many("SELECT * FROM attempts", ()) == []
    assert store._many("SELECT * FROM releases", ()) == []
    assert store._many("SELECT * FROM publish_records", ()) == []
