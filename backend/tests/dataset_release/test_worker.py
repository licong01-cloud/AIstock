from __future__ import annotations

import hashlib
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import backend.services.dataset_release.worker as worker_module
from backend.services.dataset_release.control_store import (
    ControlStore,
    build_candidate_registration_id,
    volume_identity,
)
from backend.services.dataset_release.contracts import (
    CandidateIdentity,
    OperationKind,
    PitProvenanceState,
    ProducerProvenanceState,
    Scope,
    attestation_observation_key,
)
from backend.services.dataset_release.lease import LeaseManager
from backend.services.dataset_release.profile import ResourcePolicy
from backend.services.dataset_release.resource_budget import GIB, HostMemorySnapshot
from backend.services.dataset_release.resource_gate import DiskSpaceSnapshot, ResourceGate
from backend.services.dataset_release.resource_supervisor import ResourceSupervisorError
from backend.services.dataset_release.state_machine import (
    AttestationObservationSpec,
    DatasetReleaseStateMachine,
    IntentSpec,
    ReattestFinalizeSpec,
    ResolutionSnapshotSpec,
)
from backend.services.dataset_release.worker import (
    SOURCE_RECHECK_EXECUTION_ID,
    CycleReport,
    DatasetReleaseWorker,
    ProcessorRegistry,
    ProcessorContractViolation,
    ProcessorResult,
    PublishRecoveryConflict,
    SupervisorRequest,
    WorkResourceSpec,
    WorkerError,
)
from backend.services.dataset_release.publisher import (
    DatasetPublisher,
    PublishSpec,
    artifact_tree_digest,
)
from backend.tests.dataset_release.test_publish_protocol import _prepared_fixture
from backend.services.dataset_release.worker_identity import (
    WorkerHeartbeatStore,
    WorkerIdentity,
)


class Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 8, 11, 9, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


class FakeSupervisor:
    supervised_calls = []

    def __init__(self, request: SupervisorRequest, entered: list[SupervisorRequest]) -> None:
        self.request = request
        self.entered = entered

    def __enter__(self):
        self.entered.append(self.request)
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def run_supervised(self, command, **kwargs):
        self.supervised_calls.append((tuple(command), dict(kwargs)))
        return SimpleNamespace(
            resource_gate_receipt={
                "final_status": "READY",
                "checkpoint_requested": False,
                "pressure_rung": int(kwargs["pressure_rung"]),
            }
        )


class StaticProcessor:
    def __init__(
        self,
        result: ProcessorResult,
        *,
        release_id: str | None = None,
        on_process=None,
    ) -> None:
        self.result = result
        self.release_id = release_id
        self.on_process = on_process
        self.calls = 0

    def resource_spec(self, _record):
        return WorkResourceSpec(
            ResourcePolicy(),
            hybrid_wsl=False,
            release_id=self.release_id,
        )

    def process(self, context):
        self.calls += 1
        if self.on_process is not None:
            self.on_process(context)
        return self.result


class FixturePublishRecovery:
    def __init__(self, publisher: DatasetPublisher, *, conflict: bool = False) -> None:
        self.publisher = publisher
        self.conflict = conflict
        self.calls = 0
        self.owned_leases = []

    def recover_and_finalize(self, *, run, claim):
        self.calls += 1
        self.owned_leases = self.publisher.store._many(
            "SELECT * FROM leases WHERE attempt_id=? ORDER BY resource_key",
            (claim.attempt_id,),
        )
        if self.conflict:
            raise PublishRecoveryConflict("prepared marker identity mismatch")
        record = self.publisher.store._many(
            "SELECT * FROM publish_records WHERE run_id=? LIMIT 1",
            (run["run_id"],),
        )[0]
        return self.publisher.recover_and_finalize(str(record["release_id"]))


def _identity(suffix: str, clock: Clock) -> WorkerIdentity:
    return WorkerIdentity.create(
        code_sha=suffix * 40,
        profile_digests={"qe_hmm_full_v1": suffix * 64},
        capabilities=("resolution", "build", "commands", "orphan-reconcile"),
        now=clock,
        host=f"host-{suffix}",
        pid=1000 + ord(suffix),
        process_create_time=clock().isoformat(),
        instance_id=f"dsw_{suffix * 32}",
    )


def _worker(
    store: ControlStore,
    *,
    clock: Clock,
    registry: ProcessorRegistry,
    identity_suffix: str = "a",
    liveness_probe=lambda _owner: "unknown",
    stop_event: threading.Event | None = None,
    sleeps: list[float] | None = None,
    heartbeat_interval_seconds: float = 15.0,
    max_attempts: int = 3,
    retry_backoff_seconds: float = 30.0,
):
    entered: list[SupervisorRequest] = []

    def record_sleep(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)
        clock.advance(seconds)

    worker = DatasetReleaseWorker(
        store=store,
        identity=_identity(identity_suffix, clock),
        registry=registry,
        supervisor_factory=lambda request: FakeSupervisor(request, entered),
        liveness_probe=liveness_probe,
        lease_ttl_seconds=30,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        retry_backoff_seconds=retry_backoff_seconds,
        max_attempts=max_attempts,
        now=clock,
        sleep=(record_sleep if sleeps is not None else lambda _seconds: None),
        stop_event=stop_event,
    )
    return worker, entered


def _submission(store: ControlStore, suffix: str, *, logical: str | None = None):
    return store.submit(
        principal="operator",
        route="runs",
        idempotency_key=f"submission-{suffix}",
        request_hash=f"request-{suffix}",
        logical_request_key=logical or f"logical-{suffix}",
        request_ref=f"request-ref-{suffix}",
    )


def _run(store: ControlStore, suffix: str):
    return DatasetReleaseStateMachine(store).create_queued_run(
        intent=IntentSpec(
            logical_request_key=f"logical-{suffix}",
            resolved_intent_key=f"resolved-{suffix}",
            source_content_root=f"source-{suffix}",
            source_provenance_root=f"provenance-{suffix}",
            pit_snapshot_digest=f"pit-{suffix}",
        ),
        run_generation_digest=f"generation-{suffix}",
        operation_kind=OperationKind.BUILD.value,
        plan_ref=f"plan-{suffix}",
    )


def _resource_gate(dataset_profile, *, available: int = 32 * GIB) -> ResourceGate:
    return ResourceGate(
        dataset_profile,
        host_probe=lambda: HostMemorySnapshot(
            observed_monotonic=1.0,
            available_bytes=available,
            commit_total_bytes=40 * GIB,
            commit_limit_bytes=80 * GIB,
            pagefile_used_bytes=2 * GIB,
            pagefile_limit_bytes=32 * GIB,
            page_reads_per_second=0.0,
            low_memory_signaled=False,
        ),
        disk_probe=lambda predicted: DiskSpaceSnapshot(
            control_free_bytes=128 * GIB,
            candidate_free_bytes=128 * GIB,
            effective_free_bytes=128 * GIB,
            required_free_bytes=32 * GIB,
            predicted_remaining_new_bytes=predicted,
            same_volume=True,
        ),
        sleep=lambda _seconds: None,
    )


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _commit_publish_for_worker_context(
    context,
    *,
    candidate_root: Path,
    release_id: str,
    suffix: str,
) -> PublishSpec:
    """Commit a tiny fixture candidate under the worker-owned attempt/fences."""

    store = context.store
    machine = DatasetReleaseStateMachine(store)
    candidate_root.mkdir(parents=True, exist_ok=True)
    staging = candidate_root / ".staging" / context.claim.attempt_id / str(context.claim.attempt_fence)
    staging.mkdir(parents=True)
    (staging / "daily.bin").write_bytes(b"fixture daily bytes")
    (staging / "metadata").mkdir()
    (staging / "metadata" / "manifest.json").write_text(
        '{"schema_version":"worker-post-commit-fixture"}\n',
        encoding="utf-8",
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET staging_ref=? WHERE attempt_id=?",
            (str(staging), context.claim.attempt_id),
        )

    artifact_root = artifact_tree_digest(staging)
    release_digest = _digest(f"release-{suffix}")
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
    source_probe_key = _digest(f"probe-{suffix}")
    attestation_key = attestation_observation_key(
        attestation_target_key,
        source_probe_key,
    )
    attestation_ref = f"cas:attestation-{suffix}"
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
        receipt_ref=attestation_ref,
        committed=False,
    )
    assert context.claim.host is not None and context.claim.release is not None
    tokens = (context.claim.host, context.claim.release)
    executing = store.get_run(context.target_id)
    validating = machine.transition_owned_keep(
        run_id=context.target_id,
        attempt_id=context.claim.attempt_id,
        expected_state="EXECUTING",
        expected_row_version=executing["row_version"],
        attempt_fence=context.claim.attempt_fence,
        tokens=tokens,
        next_state="VALIDATING",
    )
    machine.transition_owned_keep(
        run_id=context.target_id,
        attempt_id=context.claim.attempt_id,
        expected_state="VALIDATING",
        expected_row_version=validating["row_version"],
        attempt_fence=context.claim.attempt_fence,
        tokens=tokens,
        next_state="PREPARING_PUBLISH",
    )
    spec = PublishSpec(
        run_id=context.target_id,
        attempt_id=context.claim.attempt_id,
        attempt_fence=context.claim.attempt_fence,
        host_fence=context.claim.host.fence,
        release_fence=context.claim.release.fence,
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
        attestation_ref=attestation_ref,
        source_probe_key=source_probe_key,
        source_probe_ref=f"cas:probe-{suffix}",
    )
    publisher = DatasetPublisher(store, candidate_root=candidate_root)
    publisher.prepare(spec)
    publisher.commit_files(release_id)
    publisher.finalize(release_id)
    return spec


def test_resource_wait_happens_before_any_build_lease_or_supervisor(tmp_path, dataset_profile) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "resource-wait")
    entered = []
    processor = StaticProcessor(ProcessorResult.retryable("unused"), release_id="release-resource-wait")
    worker = DatasetReleaseWorker(
        store=store,
        identity=_identity("b", clock),
        registry=ProcessorRegistry(build=processor),
        supervisor_factory=lambda request: FakeSupervisor(request, entered),
        resource_gate_factory=lambda _resources, _stage: _resource_gate(dataset_profile, available=10 * GIB),
        now=clock,
        sleep=lambda _seconds: None,
    )

    report = worker.run_once()

    assert report.state == "WAITING_RESOURCE"
    assert entered == []
    assert store.get_run(run["run_id"])["active_attempt_id"] is None
    assert store._many("SELECT * FROM attempts WHERE run_id=?", (run["run_id"],)) == []
    assert store._many("SELECT * FROM leases WHERE attempt_id IS NOT NULL", ()) == []


def test_production_supervisor_without_resource_gate_blocks_before_source_claim(
    tmp_path,
) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    submission = _submission(store, "missing-resource-gate")
    processor = StaticProcessor(ProcessorResult.retryable("unused"))
    worker = DatasetReleaseWorker(
        store=store,
        identity=_identity("c", clock),
        registry=ProcessorRegistry(resolution=processor),
        now=clock,
        sleep=lambda _seconds: None,
    )

    report = worker.run_once()

    assert report.state == "BLOCKED_CONTRACT"
    assert report.detail == "BLOCKED_RESOURCE_ENFORCEMENT_UNAVAILABLE"
    assert store.get_submission(submission["submission_id"])["state"] == "BLOCKED_CONTRACT"
    assert (
        store._many(
            "SELECT * FROM resolution_attempts WHERE submission_id=?",
            (submission["submission_id"],),
        )
        == []
    )
    assert store._many("SELECT * FROM leases WHERE attempt_id IS NOT NULL", ()) == []


def test_source_resource_wait_uses_durable_first_wait_and_times_out(tmp_path, dataset_profile) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    submission = _submission(store, "resource-timeout")

    class WaitingSourceProcessor(StaticProcessor):
        def resource_spec(self, _record):
            return WorkResourceSpec(ResourcePolicy(wait_deadline_seconds=1), hybrid_wsl=False)

    processor = WaitingSourceProcessor(ProcessorResult.retryable("unused"))
    worker = DatasetReleaseWorker(
        store=store,
        identity=_identity("d", clock),
        registry=ProcessorRegistry(resolution=processor),
        supervisor_factory=lambda request: FakeSupervisor(request, []),
        resource_gate_factory=lambda _resources, _stage: _resource_gate(dataset_profile, available=10 * GIB),
        retry_backoff_seconds=1,
        now=clock,
        sleep=lambda _seconds: None,
    )

    first = worker.run_once()
    clock.advance(2)
    second = worker.run_once()

    durable = store.get_submission(submission["submission_id"])
    assert first.state == "WAITING_SOURCE"
    assert second.state == "BLOCKED_CONTRACT"
    assert second.detail == "BLOCKED_RESOURCE_TIMEOUT"
    assert durable["terminal_receipt_ref"] is not None
    receipt = worker.cas.get_json(durable["terminal_receipt_ref"])
    assert receipt["error_code"] == "BLOCKED_RESOURCE_TIMEOUT"
    assert receipt["context"]["resource_admission_class"] == "full"
    assert receipt["context"]["effective_host_start_commit_headroom_bytes"] == 16 * GIB


def test_context_exposes_only_supervised_launch_and_forwards_pressure_rung(tmp_path, dataset_profile) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "supervised-only")
    FakeSupervisor.supervised_calls.clear()

    def invoke(context) -> None:
        assert not hasattr(context, "launch_windows")
        assert not hasattr(context, "launch_wsl")
        context.run_supervised(["fixture-child"], execution_id="fixture-child", cwd=tmp_path)

    processor = StaticProcessor(
        ProcessorResult.retryable("FIXTURE_RETRY"),
        release_id="release-supervised-only",
        on_process=invoke,
    )
    worker, _entered = _worker(store, clock=clock, registry=ProcessorRegistry(build=processor))

    worker.run_once()

    assert len(FakeSupervisor.supervised_calls) == 1
    assert FakeSupervisor.supervised_calls[0][1]["pressure_rung"] == 0
    catalog = store.list_run_log_executions(run_id=run["run_id"])
    assert len(catalog) == 1
    assert catalog[0]["execution_id"] == "fixture-child"
    assert catalog[0]["relative_log_root"] == (
        f"attempt_runs/{catalog[0]['attempt_id']}-{catalog[0]['attempt_fence']}/fixture-child/logs"
    )


def test_typed_source_recheck_forwards_only_db_credentials_and_exact_script(
    tmp_path,
    dataset_profile,
) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    _run(store, "typed-source-recheck")
    FakeSupervisor.supervised_calls.clear()
    project_root = Path(__file__).resolve().parents[3]

    def command(context):
        result = (
            store.root
            / "attempt_runs"
            / f"{context.claim.attempt_id}-{context.claim.attempt_fence}"
            / SOURCE_RECHECK_EXECUTION_ID
            / "semantic_result.json"
        )
        return [
            sys.executable,
            str(project_root / "scripts" / "dataset_release_source_recheck.py"),
            "--profile",
            str(dataset_profile.path),
            "--control-root",
            str(store.root),
            "--cutoff",
            "2026-07-31",
            "--artifact-ready-contract-ref",
            "a" * 64,
            "--run-id",
            context.target_id,
            "--attempt-id",
            context.claim.attempt_id,
            "--attempt-fence",
            str(context.claim.attempt_fence),
            "--execution-id",
            SOURCE_RECHECK_EXECUTION_ID,
            "--result-path",
            str(result),
            "--stage-timeout-seconds",
            str(dataset_profile.stage_timeouts_seconds["source_freeze"]),
            "--pressure-rung",
            str(context.pressure_rung),
        ]

    def invoke(context) -> None:
        context.run_source_recheck_supervised(
            command(context),
            execution_id=SOURCE_RECHECK_EXECUTION_ID,
            cwd=project_root,
            timeout_seconds=float(dataset_profile.stage_timeouts_seconds["source_freeze"]),
        )

    processor = StaticProcessor(
        ProcessorResult.retryable("FIXTURE_RETRY"),
        release_id="release-typed-source-recheck",
        on_process=invoke,
    )
    worker, _entered = _worker(store, clock=clock, registry=ProcessorRegistry(build=processor))

    worker.run_once()

    assert len(FakeSupervisor.supervised_calls) == 1
    _child, kwargs = FakeSupervisor.supervised_calls[0]
    assert kwargs["environment_scope"] == "source"
    assert kwargs["credential_env_keys"] == (
        "TDX_DB_HOST",
        "TDX_DB_NAME",
        "TDX_DB_PASSWORD",
        "TDX_DB_PORT",
        "TDX_DB_USER",
    )
    assert "TUSHARE_TOKEN" not in kwargs["credential_env_keys"]
    assert "TDX_HTTP_PORT" not in kwargs["credential_env_keys"]


@pytest.mark.parametrize("mutation", ["executable", "extra-flag"])
def test_typed_source_recheck_rejects_command_authority_expansion(
    tmp_path,
    dataset_profile,
    mutation: str,
) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    _run(store, f"source-recheck-{mutation}")
    FakeSupervisor.supervised_calls.clear()
    project_root = Path(__file__).resolve().parents[3]

    def invoke(context) -> None:
        result = (
            store.root
            / "attempt_runs"
            / f"{context.claim.attempt_id}-{context.claim.attempt_fence}"
            / SOURCE_RECHECK_EXECUTION_ID
            / "semantic_result.json"
        )
        command = [
            sys.executable,
            str(project_root / "scripts" / "dataset_release_source_recheck.py"),
            "--profile",
            str(dataset_profile.path),
            "--control-root",
            str(store.root),
            "--cutoff",
            "2026-07-31",
            "--artifact-ready-contract-ref",
            "b" * 64,
            "--run-id",
            context.target_id,
            "--attempt-id",
            context.claim.attempt_id,
            "--attempt-fence",
            str(context.claim.attempt_fence),
            "--execution-id",
            SOURCE_RECHECK_EXECUTION_ID,
            "--result-path",
            str(result),
            "--stage-timeout-seconds",
            str(dataset_profile.stage_timeouts_seconds["source_freeze"]),
            "--pressure-rung",
            str(context.pressure_rung),
        ]
        if mutation == "executable":
            command[0] = str(project_root / "scripts" / "dataset_release_source_stage.py")
        else:
            command.extend(("--unexpected", "value"))
        with pytest.raises(ProcessorContractViolation):
            context.run_source_recheck_supervised(
                command,
                execution_id=SOURCE_RECHECK_EXECUTION_ID,
                cwd=project_root,
                timeout_seconds=float(dataset_profile.stage_timeouts_seconds["source_freeze"]),
            )

    processor = StaticProcessor(
        ProcessorResult.retryable("FIXTURE_RETRY"),
        release_id=f"release-source-recheck-{mutation}",
        on_process=invoke,
    )
    worker, _entered = _worker(store, clock=clock, registry=ProcessorRegistry(build=processor))

    worker.run_once()

    assert FakeSupervisor.supervised_calls == []


def test_build_credential_request_is_blocked_before_claim_or_supervisor(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "build-secret")
    entered = []

    class SecretRequestingBuild(StaticProcessor):
        def resource_spec(self, _record):
            return WorkResourceSpec(
                ResourcePolicy(),
                hybrid_wsl=False,
                release_id="release-build-secret",
                credential_env_allowlist=("TUSHARE_TOKEN",),
            )

    worker = DatasetReleaseWorker(
        store=store,
        identity=_identity("e", clock),
        registry=ProcessorRegistry(
            build=SecretRequestingBuild(
                ProcessorResult.retryable("must-not-run"),
                release_id="release-build-secret",
            )
        ),
        supervisor_factory=lambda request: FakeSupervisor(request, entered),
        now=clock,
        sleep=lambda _seconds: None,
    )

    report = worker.run_once()

    assert report.state == "BLOCKED_VERSION_MISMATCH"
    assert entered == []
    assert store.get_run(run["run_id"])["active_attempt_id"] is None
    assert store._many("SELECT * FROM attempts WHERE run_id=?", (run["run_id"],)) == []


def test_resolution_retryable_releases_fences_and_records_durable_retry(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    submission = _submission(store, "retry")
    claimed_leases = []

    def inspect_claim(context) -> None:
        claimed_leases.extend(
            store._many(
                "SELECT * FROM leases WHERE attempt_id=? ORDER BY resource_key",
                (context.claim.attempt_id,),
            )
        )

    processor = StaticProcessor(
        ProcessorResult.retryable("TRANSIENT_SOURCE", retry_after_seconds=60),
        on_process=inspect_claim,
    )
    worker, entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(resolution=processor),
    )

    report = worker.run_once()

    durable = store.get_submission(submission["submission_id"])
    attempts = store._many(
        "SELECT * FROM resolution_attempts WHERE submission_id=?",
        (submission["submission_id"],),
    )
    assert report.state == "FAILED_RETRYABLE"
    assert durable["state"] == "FAILED_RETRYABLE"
    assert durable["resolution_attempt_id"] is None
    assert durable["next_retry_at"] == "2026-08-11T09:01:00.000000+00:00"
    assert attempts[0]["state"] == "RELEASED_RETRYABLE"
    assert store.get_lease("host:heavy-dataset")["state"] == "FREE"
    assert len(claimed_leases) == 2
    identity_fields = {
        (
            lease["code_sha"],
            lease["capability_digest"],
            lease["requested_ram"],
            lease["db_connections"],
            lease["io_class"],
        )
        for lease in claimed_leases
    }
    assert identity_fields == {
        (
            "a" * 40,
            worker.identity.capability_digest,
            12 * 2**30,
            4,
            "dataset-release",
        )
    }
    events = store.list_events(submission_id=submission["submission_id"])
    assert events[-1]["type"] == "SUBMISSION_FAILED_RETRYABLE"
    assert entered and entered[0].attempt_id == attempts[0]["resolution_attempt_id"]


def test_resolution_retry_exhaustion_binds_latest_error_receipt(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    submission = _submission(store, "resolution-exhausted")
    processor = StaticProcessor(ProcessorResult.retryable("TRANSIENT_SOURCE", retry_after_seconds=0))
    worker, _entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(resolution=processor),
        max_attempts=1,
        retry_backoff_seconds=0,
    )

    assert worker.run_once().state == "FAILED_RETRYABLE"
    worker.run_once()

    durable = store.get_submission(submission["submission_id"])
    attempt = store._many(
        "SELECT * FROM resolution_attempts WHERE submission_id=?",
        (submission["submission_id"],),
    )[0]
    assert durable["state"] == "BLOCKED_RETRY_EXHAUSTED"
    assert durable["terminal_receipt_ref"] == attempt["error_ref"]
    receipt = worker.cas.get_json(durable["terminal_receipt_ref"])
    assert receipt["kind"] == "resolution"
    assert receipt["target_id"] == submission["submission_id"]
    assert receipt["disposition"] == "RETRYABLE"


def test_build_retry_exhaustion_binds_latest_error_receipt_and_missing_ref_fails_closed(
    tmp_path,
) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "build-exhausted")
    processor = StaticProcessor(
        ProcessorResult.retryable("TRANSIENT_BUILD", retry_after_seconds=0),
        release_id="release-build-exhausted",
    )
    worker, _entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(build=processor),
        max_attempts=1,
        retry_backoff_seconds=0,
    )

    assert worker.run_once().state == "FAILED_RETRYABLE"
    worker.run_once()
    durable = store.get_run(run["run_id"])
    attempt = store._many("SELECT * FROM attempts WHERE run_id=?", (run["run_id"],))[0]
    assert durable["state"] == "BLOCKED_RETRY_EXHAUSTED"
    assert durable["terminal_receipt_ref"] == attempt["error_ref"]
    receipt = worker.cas.get_json(durable["terminal_receipt_ref"])
    assert receipt["kind"] == "build"
    assert receipt["target_id"] == run["run_id"]

    broken = _run(store, "build-exhausted-missing")
    broken_processor = StaticProcessor(
        ProcessorResult.retryable("TRANSIENT_BUILD", retry_after_seconds=0),
        release_id="release-build-exhausted-missing",
    )
    broken_worker, _entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(build=broken_processor),
        identity_suffix="b",
        max_attempts=1,
        retry_backoff_seconds=0,
    )
    assert broken_worker.run_once().state == "FAILED_RETRYABLE"
    with store.transaction() as connection:
        connection.execute("UPDATE attempts SET error_ref=NULL WHERE run_id=?", (broken["run_id"],))
    with pytest.raises(WorkerError, match="lacks the latest"):
        broken_worker.run_once()
    assert store.get_run(broken["run_id"])["state"] == "FAILED_RETRYABLE"


def test_build_terminal_failure_is_blocked_and_never_fake_success(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "terminal")
    processor = StaticProcessor(
        ProcessorResult.blocked(
            "PERMANENT_CONTRACT",
            context={"partition": "2026-07", "api_token": "must-not-persist"},
        ),
        release_id="release-terminal",
    )
    worker, entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(build=processor),
    )

    report = worker.run_once()

    durable = store.get_run(run["run_id"])
    attempt = store._many("SELECT * FROM attempts WHERE run_id=?", (run["run_id"],))[0]
    assert report.state == "FAILED_TERMINAL"
    assert durable["state"] == "FAILED_TERMINAL"
    assert durable["outcome"] == "BLOCKED"
    assert durable["active_attempt_id"] is None
    assert attempt["state"] == "FAILED_TERMINAL"
    assert attempt["error_ref"] == durable["terminal_receipt_ref"]
    error_receipt = worker.cas.get_json(attempt["error_ref"])
    assert error_receipt["context"] == {
        "api_token": "<redacted>",
        "partition": "2026-07",
    }
    assert entered and store.get_lease("host:heavy-dataset")["state"] == "FREE"


def test_processor_cannot_claim_success_without_durable_publish_readback(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "false-success")
    processor = StaticProcessor(
        ProcessorResult.durable_success(),
        release_id="release-false-success",
    )
    worker, _entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(build=processor),
    )

    report = worker.run_once()

    assert report.state == "FAILED_TERMINAL"
    assert report.detail == "BLOCKED_DATASET_PROCESSOR_CONTRACT"
    assert store.get_run(run["run_id"])["outcome"] == "BLOCKED"


def test_resolution_post_terminal_exception_reads_back_exact_durable_success(
    tmp_path,
) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    submission = _submission(store, "resolution-post-commit")
    committed_run_ids: list[str] = []

    def commit_then_raise(context) -> None:
        machine = DatasetReleaseStateMachine(store)
        observed = datetime.now(UTC)
        snapshot = ResolutionSnapshotSpec(
            source_content_root="source-resolution-post-commit",
            source_provenance_root="provenance-resolution-post-commit",
            pit_snapshot_digest="pit-resolution-post-commit",
            source_probe_ordinal=1,
            source_probe_key=_digest("probe-resolution-post-commit"),
            source_probe_ref="cas:probe-resolution-post-commit",
            source_probe_valid_until=observed + timedelta(hours=1),
        )
        machine.record_resolution_snapshot(
            submission_id=context.target_id,
            resolution_attempt_id=context.claim.attempt_id,
            resolution_fence=context.claim.attempt_fence,
            source_content_root=snapshot.source_content_root,
            source_provenance_root=snapshot.source_provenance_root,
            pit_snapshot_digest=snapshot.pit_snapshot_digest,
            source_probe_ordinal=snapshot.source_probe_ordinal,
            source_probe_key=snapshot.source_probe_key,
            source_probe_ref=snapshot.source_probe_ref,
            source_probe_valid_until=snapshot.source_probe_valid_until,
        )
        run = machine.create_queued_run(
            intent=IntentSpec(
                logical_request_key=str(context.record["logical_request_key"]),
                resolved_intent_key="resolved-resolution-post-commit",
                source_content_root=snapshot.source_content_root,
                source_provenance_root=snapshot.source_provenance_root,
                pit_snapshot_digest=snapshot.pit_snapshot_digest,
            ),
            run_generation_digest="generation-resolution-post-commit",
            operation_kind=OperationKind.BUILD.value,
            plan_ref="cas:plan-resolution-post-commit",
            submission_id=context.target_id,
            resolution_attempt_id=context.claim.attempt_id,
            resolution_fence=context.claim.attempt_fence,
            expected_resolution_snapshot=snapshot,
        )
        committed_run_ids.append(str(run["run_id"]))
        raise RuntimeError("injected after resolution terminal commit")

    processor = StaticProcessor(
        ProcessorResult.blocked("SHOULD_BE_IGNORED"),
        on_process=commit_then_raise,
    )
    worker, _entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(resolution=processor),
    )

    report = worker.run_once()

    durable = store.get_submission(submission["submission_id"])
    attempt = store.get_resolution_attempt(
        store._many(
            "SELECT resolution_attempt_id FROM resolution_attempts WHERE submission_id=?",
            (submission["submission_id"],),
        )[0]["resolution_attempt_id"]
    )
    assert report.state == "RESOLVED_NEW_RUN" and report.detail is None
    assert durable["state"] == "RESOLVED_NEW_RUN"
    assert durable["run_id"] == committed_run_ids[0]
    assert durable["resolution_attempt_id"] is None
    assert attempt["state"] == "RELEASED_SUCCEEDED" and attempt["error_ref"] is None
    assert (
        store._many(
            "SELECT * FROM leases WHERE attempt_id=? AND state!='FREE'",
            (attempt["resolution_attempt_id"],),
        )
        == []
    )
    assert [
        row["type"]
        for row in store.list_events(submission_id=submission["submission_id"])
        if row["type"].startswith("SUBMISSION_FAILED")
    ] == []


def test_publish_post_terminal_exception_reads_back_exact_durable_success(tmp_path, monkeypatch) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "publish-post-commit")
    release_id = "20260731-qe-full-publish-post-commit-candidate"
    specs: list[PublishSpec] = []

    def commit_then_raise(context) -> None:
        specs.append(
            _commit_publish_for_worker_context(
                context,
                candidate_root=tmp_path / "candidates",
                release_id=release_id,
                suffix="publish-post-commit",
            )
        )
        raise RuntimeError("injected after publish terminal commit")

    processor = StaticProcessor(
        ProcessorResult.blocked("SHOULD_BE_IGNORED"),
        release_id=release_id,
        on_process=commit_then_raise,
    )
    worker, _entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(build=processor),
    )
    original_stop = worker_module._LeaseHeartbeatLoop.stop

    def stop_with_post_commit_lease_error(loop):
        original_stop(loop)
        return WorkerError("injected lease loss after terminal release")

    monkeypatch.setattr(
        worker_module._LeaseHeartbeatLoop,
        "stop",
        stop_with_post_commit_lease_error,
    )

    report = worker.run_once()

    spec = specs[0]
    durable = store.get_run(run["run_id"])
    attempt = store.get_attempt(spec.attempt_id)
    assert report.state == "SUCCEEDED" and report.detail is None
    assert durable["state"] == "SUCCEEDED"
    assert durable["terminal_receipt_ref"] == spec.build_receipt_ref
    assert attempt["state"] == "RELEASED_SUCCEEDED" and attempt["error_ref"] is None
    assert store.get_lease("host:heavy-dataset")["state"] == "FREE"
    assert store.get_lease(f"release:{release_id}")["state"] == "FREE"
    assert (
        len(
            [
                event
                for event in store.list_events(run_id=run["run_id"])
                if event["type"] == "CANDIDATE_VALIDATED" and event["attempt_id"] == spec.attempt_id
            ]
        )
        == 1
    )


def test_reattest_post_terminal_exception_reads_back_exact_durable_success(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    machine = DatasetReleaseStateMachine(store)
    run = machine.create_queued_run(
        intent=IntentSpec(
            logical_request_key="logical-reattest-post-commit",
            resolved_intent_key="resolved-reattest-post-commit",
            source_content_root="source-reattest-post-commit",
            source_provenance_root="provenance-reattest-post-commit",
            pit_snapshot_digest="pit-reattest-post-commit",
        ),
        run_generation_digest="generation-reattest-post-commit",
        operation_kind=OperationKind.REATTEST.value,
        plan_ref="cas:plan-reattest-post-commit",
    )
    release_id = "legacy-reattest-post-commit"
    committed_attempt_ids: list[str] = []

    def commit_then_raise(context) -> None:
        observed = datetime.now(UTC)
        attestation_target_key = _digest("reattest-target-post-commit")
        source_probe_key = _digest("reattest-probe-post-commit")
        observation = AttestationObservationSpec(
            attestation_id=None,
            attestation_key=attestation_observation_key(
                attestation_target_key,
                source_probe_key,
            ),
            attestation_target_key=attestation_target_key,
            subject_type="candidate",
            subject_digest="candidate-reattest-post-commit",
            candidate_identity="candidate-reattest-post-commit",
            producer_provenance_state="KNOWN",
            producer_provenance_digest_or_sentinel="producer-reattest-post-commit",
            candidate_artifact_root="artifact-reattest-post-commit",
            current_source_content_root="source-reattest-post-commit",
            source_probe_key=source_probe_key,
            source_probe_ref="cas:probe-reattest-post-commit",
            pit_snapshot_digest="pit-reattest-post-commit",
            semantic_profile_digest="semantic-reattest-post-commit",
            validation_fingerprint="validation-reattest-post-commit",
            observed_at=observed,
            valid_until=observed + timedelta(hours=1),
            equivalence_mode="CURRENT_SOURCE_EQUIVALENT",
            outcome="CURRENT_SOURCE_EQUIVALENT",
            receipt_ref="cas:attestation-reattest-post-commit",
            committed=True,
        )
        assert context.claim.host is not None and context.claim.release is not None
        owned = store.get_run(context.target_id)
        machine.finalize_reattest(
            ReattestFinalizeSpec(
                run_id=context.target_id,
                attempt_id=context.claim.attempt_id,
                expected_row_version=owned["row_version"],
                attempt_fence=context.claim.attempt_fence,
                tokens=(context.claim.host, context.claim.release),
                observation=observation,
            ),
            now=observed,
        )
        committed_attempt_ids.append(context.claim.attempt_id)
        raise RuntimeError("injected after reattest terminal commit")

    processor = StaticProcessor(
        ProcessorResult.blocked("SHOULD_BE_IGNORED"),
        release_id=release_id,
        on_process=commit_then_raise,
    )
    worker, _entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(build=processor),
    )

    report = worker.run_once()

    durable = store.get_run(run["run_id"])
    attempt = store.get_attempt(committed_attempt_ids[0])
    assert report.state == "SUCCEEDED" and report.detail is None
    assert durable["outcome"] == "REATTESTED"
    assert durable["terminal_receipt_ref"] == "cas:attestation-reattest-post-commit"
    assert attempt["state"] == "RELEASED_SUCCEEDED" and attempt["error_ref"] is None
    assert store.get_lease("host:heavy-dataset")["state"] == "FREE"
    assert store.get_lease(f"release:{release_id}")["state"] == "FREE"
    assert (
        len(
            [
                event
                for event in store.list_events(run_id=run["run_id"])
                if event["type"] == "RUN_REATTESTED" and event["attempt_id"] == committed_attempt_ids[0]
            ]
        )
        == 1
    )


def test_two_worker_instances_do_not_claim_same_logical_resolution(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    first = _submission(store, "first", logical="same-logical")
    second = _submission(store, "second", logical="same-logical")
    started = threading.Event()
    release = threading.Event()

    def block(_context) -> None:
        started.set()
        assert release.wait(timeout=5)

    first_processor = StaticProcessor(
        ProcessorResult.retryable("TRANSIENT", retry_after_seconds=60),
        on_process=block,
    )
    second_processor = StaticProcessor(ProcessorResult.retryable("SHOULD_NOT_RUN", retry_after_seconds=60))
    worker_one, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(resolution=first_processor),
        identity_suffix="a",
    )
    worker_two, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(resolution=second_processor),
        identity_suffix="b",
    )
    reports = []
    thread = threading.Thread(target=lambda: reports.append(worker_one.run_once()))
    thread.start()
    assert started.wait(timeout=5)

    raced = worker_two.run_once()
    release.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert raced.claimed is False and raced.state == "CLAIM_RACE"
    assert first_processor.calls == 1
    assert second_processor.calls == 0
    assert store.get_submission(first["submission_id"])["state"] == "FAILED_RETRYABLE"
    assert store.get_submission(second["submission_id"])["state"] == "QUEUED_RESOLUTION"


def test_cancel_and_resume_commands_are_atomic_and_fenced(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    cancel_run = _run(store, "cancel")
    resume_run = _run(store, "resume")
    with store.transaction() as connection:
        connection.execute(
            "UPDATE runs SET state='FAILED_RETRYABLE' WHERE run_id=?",
            (resume_run["run_id"],),
        )
    cancel = store.enqueue_command(
        target_type="run",
        target_id=cancel_run["run_id"],
        command_type="CANCEL_REQUESTED",
        principal="operator",
        route="cancel",
        idempotency_key="cancel-once",
        request_hash="cancel-hash",
        actor="operator",
    )
    resume = store.enqueue_command(
        target_type="run",
        target_id=resume_run["run_id"],
        command_type="RESUME_REQUESTED",
        principal="operator",
        route="resume",
        idempotency_key="resume-once",
        request_hash="resume-hash",
        actor="operator",
    )
    worker, _ = _worker(store, clock=clock, registry=ProcessorRegistry())

    first = worker.run_once()
    second = worker.run_once()

    assert {first.identity, second.identity} == {cancel["command_id"], resume["command_id"]}
    assert store.get_run(cancel_run["run_id"])["state"] == "CANCELLED"
    assert store.get_run(resume_run["run_id"])["state"] == "QUEUED"
    assert store.get_command(cancel["command_id"])["state"] == "APPLIED"
    assert store.get_command(resume["command_id"])["state"] == "APPLIED"
    command_leases = store._many(
        "SELECT * FROM leases WHERE resource_key LIKE 'command:%' ORDER BY resource_key",
        (),
    )
    assert len(command_leases) == 2
    assert all(item["state"] == "FREE" and item["fence_counter"] == 1 for item in command_leases)


def test_new_resume_invocation_is_not_suppressed_by_prior_rejected_command(
    tmp_path,
) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "resume-after-rejected")
    first = store.enqueue_command(
        target_type="run",
        target_id=run["run_id"],
        command_type="RESUME_REQUESTED",
        principal="operator",
        route="resume",
        idempotency_key="resume-too-early",
        request_hash="same-resume-request",
        actor="operator",
    )
    worker, _ = _worker(store, clock=clock, registry=ProcessorRegistry())
    rejected = worker.run_once()
    assert rejected.state == "REJECTED_INVALID_STATE"
    assert store.get_command(first["command_id"])["state"] == "REJECTED_INVALID_STATE"

    with store.transaction() as connection:
        connection.execute(
            "UPDATE runs SET state='FAILED_RETRYABLE' WHERE run_id=?",
            (run["run_id"],),
        )
    second = store.enqueue_command(
        target_type="run",
        target_id=run["run_id"],
        command_type="RESUME_REQUESTED",
        principal="operator",
        route="resume",
        idempotency_key="resume-after-failure",
        request_hash="same-resume-request",
        actor="operator",
    )
    assert second["command_id"] != first["command_id"]
    applied = worker.run_once()
    assert applied.identity == second["command_id"]
    assert applied.state == "APPLIED"
    assert store.get_run(run["run_id"])["state"] == "QUEUED"
    assert (
        len(
            store._many(
                "SELECT command_id FROM commands WHERE target_id=? AND type='RESUME_REQUESTED'",
                (run["run_id"],),
            )
        )
        == 2
    )


def test_expired_unknown_owner_is_held_and_never_reclaimed(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "orphan")
    past = clock() - timedelta(minutes=5)
    claim = LeaseManager(store).claim_build(
        run_id=run["run_id"],
        release_id="release-orphan",
        owner_identity="old-worker",
        ttl_seconds=1,
        now=past,
    )
    processor = StaticProcessor(ProcessorResult.blocked("SHOULD_NOT_RUN"), release_id="release-orphan")
    worker, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(build=processor),
        liveness_probe=lambda _owner: "unknown",
    )

    report = worker.run_once()

    durable = store.get_run(run["run_id"])
    assert report.kind == "orphan_build"
    assert report.state == "WAITING_ORPHAN_QUIESCENCE"
    assert durable["active_attempt_id"] == claim.attempt_id
    assert store.get_lease("host:heavy-dataset")["state"] == "ORPHAN_HOLD"
    assert processor.calls == 0


def test_expired_dead_owner_is_requeued_only_after_quiescence_proof(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "dead-orphan")
    past = clock() - timedelta(minutes=5)
    claim = LeaseManager(store).claim_build(
        run_id=run["run_id"],
        release_id="release-dead-orphan",
        owner_identity="old-worker",
        ttl_seconds=1,
        now=past,
    )
    worker, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(),
        liveness_probe=lambda _owner: "dead",
    )

    report = worker.run_once()

    assert report.state == "QUEUED"
    assert store.get_run(run["run_id"])["active_attempt_id"] is None
    assert store.get_attempt(claim.attempt_id)["state"] == "EXPIRED"
    assert store.get_lease("host:heavy-dataset")["state"] == "FREE"
    assert store.get_lease("release:release-dead-orphan")["state"] == "FREE"


def test_dead_publish_owner_is_fence_handed_off_and_parent_finalizes_without_free_window(
    tmp_path,
) -> None:
    clock = Clock()
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path, "worker-finalizer")
    publisher = DatasetPublisher(store, candidate_root=candidate_root)
    publisher.prepare(spec)
    with store.transaction() as connection:
        connection.execute(
            "UPDATE leases SET expires_at=? WHERE attempt_id=?",
            ((clock() - timedelta(seconds=1)).isoformat(), spec.attempt_id),
        )
    old_fences = {
        row["resource_key"]: row["fence_counter"]
        for row in store._many("SELECT * FROM leases WHERE attempt_id=?", (spec.attempt_id,))
    }
    recovery = FixturePublishRecovery(publisher)
    worker, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(publish_recovery=recovery),
        liveness_probe=lambda _owner: "dead",
    )

    report = worker.run_once()

    assert report.kind == "orphan_publish" and report.state == "SUCCEEDED"
    assert recovery.calls == 1
    assert len(recovery.owned_leases) == 2
    assert all(row["state"] == "ACTIVE" for row in recovery.owned_leases)
    assert all(row["fence_counter"] == old_fences[row["resource_key"]] + 1 for row in recovery.owned_leases)
    assert all(row["attempt_kind"] == "FINALIZER_RECOVERY" for row in recovery.owned_leases)
    assert all(row["worker_instance_id"] == worker.identity.instance_id for row in recovery.owned_leases)
    assert all(row["hybrid_wsl"] == 0 for row in recovery.owned_leases)
    assert store.get_run(spec.run_id)["state"] == "SUCCEEDED"
    attempts = store._many("SELECT * FROM attempts WHERE run_id=? ORDER BY ordinal", (spec.run_id,))
    assert [row["state"] for row in attempts] == ["EXPIRED", "RELEASED_SUCCEEDED"]
    assert store.get_lease("host:heavy-dataset")["state"] == "FREE"
    assert store.get_lease(f"release:{spec.release_id}")["state"] == "FREE"


def test_unknown_publish_owner_or_missing_provider_remains_held_without_handoff(
    tmp_path,
) -> None:
    clock = Clock()
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path, "worker-publish-hold")
    publisher = DatasetPublisher(store, candidate_root=candidate_root)
    publisher.prepare(spec)
    with store.transaction() as connection:
        connection.execute(
            "UPDATE leases SET expires_at=? WHERE attempt_id=?",
            ((clock() - timedelta(seconds=1)).isoformat(), spec.attempt_id),
        )
    recovery = FixturePublishRecovery(publisher)
    unknown_worker, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(publish_recovery=recovery),
        liveness_probe=lambda _owner: "unknown",
    )

    unknown = unknown_worker.run_once()

    assert unknown.state == "WAITING_PUBLISH_RECOVERY"
    assert unknown.detail == "unknown" and recovery.calls == 0
    held = store._many("SELECT * FROM leases WHERE attempt_id=? ORDER BY resource_key", (spec.attempt_id,))
    assert len(held) == 2 and all(row["state"] == "ORPHAN_HOLD" for row in held)

    missing_worker, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(),
        identity_suffix="b",
        liveness_probe=lambda _owner: "dead",
    )
    missing = missing_worker.run_once()
    assert missing.state == "WAITING_PUBLISH_RECOVERY"
    assert missing.detail == "BLOCKED_PUBLISH_RECOVERY_PROVIDER_UNAVAILABLE"
    assert all(
        row["attempt_id"] == spec.attempt_id and row["state"] == "ORPHAN_HOLD"
        for row in store._many("SELECT * FROM leases ORDER BY resource_key", ())
    )


def test_publish_recovery_identity_conflict_is_typed_block_after_atomic_handoff(
    tmp_path,
) -> None:
    clock = Clock()
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path, "worker-publish-conflict")
    publisher = DatasetPublisher(store, candidate_root=candidate_root)
    publisher.prepare(spec)
    with store.transaction() as connection:
        connection.execute(
            "UPDATE leases SET expires_at=? WHERE attempt_id=?",
            ((clock() - timedelta(seconds=1)).isoformat(), spec.attempt_id),
        )
    recovery = FixturePublishRecovery(publisher, conflict=True)
    worker, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(publish_recovery=recovery),
        liveness_probe=lambda _owner: "dead",
    )

    report = worker.run_once()

    assert report.state == "BLOCKED_PUBLISH_CONFLICT"
    assert report.detail == "PUBLISH_FINAL_PATH_CONFLICT"
    assert store.get_run(spec.run_id)["active_attempt_id"] is None
    assert store.get_run(spec.run_id)["outcome"] == "BLOCKED"
    assert store.get_publish_record(spec.release_id)["state"] == "CONFLICT"
    assert all(row["state"] == "FREE" for row in store._many("SELECT * FROM leases ORDER BY resource_key", ()))


def test_resource_supervisor_uncertainty_retains_attempt_and_leases(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "supervisor-failure")
    processor = StaticProcessor(
        ProcessorResult.blocked("SHOULD_NOT_RUN"),
        release_id="release-supervisor-failure",
    )
    worker, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(build=processor),
    )

    def fail_supervisor(_request):
        raise ResourceSupervisorError("injected enforcement failure")

    worker.supervisor_factory = fail_supervisor
    report = worker.run_once()

    durable = store.get_run(run["run_id"])
    assert report.state == "WAITING_ORPHAN_QUIESCENCE"
    assert report.detail.startswith("BLOCKED_RESOURCE_ENFORCEMENT_UNAVAILABLE")
    assert durable["active_attempt_id"] is not None
    assert store.get_lease("host:heavy-dataset")["state"] == "ORPHAN_HOLD"
    assert processor.calls == 0


def test_once_blocks_only_one_missing_processor_item_and_serve_is_bounded(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    first = _submission(store, "one")
    second = _submission(store, "two")
    worker, _ = _worker(store, clock=clock, registry=ProcessorRegistry())

    report = worker.run_once()

    states = {
        store.get_submission(first["submission_id"])["state"],
        store.get_submission(second["submission_id"])["state"],
    }
    assert report.claimed is True
    assert states == {"BLOCKED_CONTRACT", "QUEUED_RESOLUTION"}

    with store.transaction() as connection:
        connection.execute("UPDATE submissions SET state='CANCELLED' WHERE state='QUEUED_RESOLUTION'")
    sleeps: list[float] = []
    idle_worker, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(),
        identity_suffix="b",
        sleeps=sleeps,
    )
    reports = idle_worker.run_serve(max_polls=205)
    heartbeat = idle_worker.heartbeats.read(idle_worker.identity.instance_id)

    assert len(reports) == 200 and all(item.state == "IDLE" for item in reports)
    assert sleeps[:5] == [5.0, 10.0, 15.0, 15.0, 15.0]
    assert len(sleeps) == 205 and max(sleeps) == 15.0
    assert heartbeat["status"] == "STOPPED"
    assert heartbeat["counter"] >= 4


def test_claimed_heartbeat_is_never_throttled_by_idle_interval(tmp_path) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    worker, _entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(),
    )
    active = CycleReport(True, "build", "run-1", "RUNNING")

    worker._heartbeat(active)
    first = worker.heartbeats.read(worker.identity.instance_id)["counter"]
    worker._heartbeat(active)
    second = worker.heartbeats.read(worker.identity.instance_id)["counter"]
    worker.close()

    assert second == first + 1


def test_long_processor_refreshes_worker_health_independently_and_stops_thread(
    tmp_path,
) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    run = _run(store, "long-health")
    started = threading.Event()
    release = threading.Event()

    def block(_context) -> None:
        clock.advance(31)
        started.set()
        assert release.wait(timeout=2)

    processor = StaticProcessor(
        ProcessorResult.retryable("FIXTURE_RETRY", retry_after_seconds=0),
        release_id="release-long-health",
        on_process=block,
    )
    worker, _entered = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(build=processor),
        heartbeat_interval_seconds=0.01,
    )
    reports: list[CycleReport] = []
    thread = threading.Thread(target=lambda: reports.append(worker.run_once()))
    thread.start()
    assert started.wait(timeout=1)
    time.sleep(0.05)

    health = worker.heartbeats.read_latest(
        profile="qe_hmm_full_v1",
        config_digest="a" * 64,
        ttl_seconds=30,
        now=clock(),
    )
    payload = worker.heartbeats.read(worker.identity.instance_id)
    assert health.state == "healthy"
    assert health.worker_status == "BUILD_RUNNING"
    assert payload["claim_kind"] == "build"
    assert payload["claim_id"] == run["run_id"]

    release.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert reports[0].state == "FAILED_RETRYABLE"
    counter = worker.heartbeats.read(worker.identity.instance_id)["counter"]
    time.sleep(0.03)
    assert worker.heartbeats.read(worker.identity.instance_id)["counter"] == counter
    worker.close()


def test_worker_health_read_is_bounded_validated_and_has_no_init_side_effect(
    tmp_path,
) -> None:
    clock = Clock()
    store = ControlStore.initialize(tmp_path / "control")
    health_store = WorkerHeartbeatStore(store)
    before = sorted(path.relative_to(store.root).as_posix() for path in store.root.rglob("*"))

    unavailable = health_store.read_latest(
        profile="qe_hmm_full_v1",
        config_digest="a" * 64,
        ttl_seconds=10,
        now=clock(),
    )
    assert unavailable.state == "unavailable"
    after = sorted(path.relative_to(store.root).as_posix() for path in store.root.rglob("*"))
    assert before == after

    identity = _identity("a", clock)
    health_store.write(identity, status="IDLE", observed_at=clock())
    healthy = health_store.read_latest(
        profile="qe_hmm_full_v1",
        config_digest="a" * 64,
        ttl_seconds=10,
        now=clock(),
    )
    assert healthy.state == "healthy"
    assert healthy.instance_id == identity.instance_id

    clock.advance(11)
    stale = health_store.read_latest(
        profile="qe_hmm_full_v1",
        config_digest="a" * 64,
        ttl_seconds=10,
        now=clock(),
    )
    assert stale.state == "stale"

    health_store.write(
        identity,
        status="BLOCKED_PROCESSOR_UNAVAILABLE",
        observed_at=clock(),
    )
    blocked = health_store.read_latest(
        profile="qe_hmm_full_v1",
        config_digest="a" * 64,
        ttl_seconds=10,
        now=clock(),
    )
    assert blocked.state == "blocked"
