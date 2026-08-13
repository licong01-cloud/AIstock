"""Production candidate-build controller for the independent Worker.

The controller intentionally never loads a market-data panel.  Every
data-bearing operation is a separately supervised stage.  The parent reads
only bounded typed results, advances fenced catalog state, creates the pending
attestation, and invokes the parent-only atomic publisher.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import stat
import sys
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Protocol, Sequence

from .attestation import ATTESTATION_SCHEMA_VERSION
from .artifact_ready_source import ARTIFACT_READY_RECHECK_SCHEMA
from .canonical import digest_named_fields, ensure_sha256
from .candidate_consumer_smoke import (
    CandidateConsumerSmokeError,
    validate_candidate_consumer_smoke_receipt,
)
from .cas_store import CASRef, CASStore, CASStoreError
from .component_artifact_manifest import load_component_artifact_manifest
from .contracts import (
    AttestationIdentity,
    CandidateIdentity,
    EquivalenceMode,
    OperationKind,
    PitProvenanceState,
    ProducerProvenanceState,
    ReleaseIdentity,
    RunGenerationIdentity,
    Scope,
    SourceProbeIdentity,
    SourceProbeSubjectKind,
    build_operation_target,
    new_build_probe_subject,
)
from .control_store import (
    ControlStore,
    build_candidate_registration_id,
    volume_identity,
)
from .daily_minute_materializer import (
    QlibDumpToolchain,
    build_qlib_dump_command,
)
from .errors import DatasetReleaseError, IdentityConflictError
from .profile import DatasetProfile, load_initial_migration_plan
from .pit import (
    PitSnapshotError,
    frozen_pit_snapshot_from_mapping,
    require_canonical_frozen_snapshot,
)
from .publisher import DatasetPublisher, PublishSpec
from .resource_gate import RESOURCE_GATE_RECEIPT_SCHEMA
from .resolution import (
    BUILD_INPUTS_SCHEMA_VERSION,
    RESOLUTION_PLAN_SCHEMA_VERSION,
    SOURCE_PROBE_SCHEMA_VERSION,
)
from .resource_supervisor import WslSupervisedOptions
from .state_machine import DatasetReleaseStateMachine
from .stock_schema import QLIB_STOCK_FIELDS
from .worker import (
    WORKER_ERROR_RECEIPT_SCHEMA,
    ProcessorResult,
    WorkResourceSpec,
    WorkerAttemptContext,
    SOURCE_RECHECK_EXECUTION_ID,
)

SOURCE_RECHECK_RESULT_SCHEMA = "dataset_release_source_recheck_result_v1"


BUILD_STAGE_RESULT_SCHEMA = "dataset_release_build_stage_result_v1"
BUILD_RESOURCE_RECEIPT_SCHEMA = "dataset_release_build_resource_receipt_v1"
BUILD_RECEIPT_SCHEMA = "dataset_release_build_receipt_v1"
MAX_BUILD_PLAN_BYTES = 32 * 1024 * 1024
MAX_STAGE_RESULT_BYTES = 16 * 1024 * 1024
MAX_BATCH_MANIFEST_BYTES = 8 * 1024 * 1024
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_ZERO_SAFETY = {
    "database_writes": 0,
    "provider_database_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
}
_SOURCE_PROBE_ZERO_SAFETY = {
    "database_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
}


class BuildProcessorError(DatasetReleaseError):
    code = "BLOCKED_DATASET_BUILD_PROCESSOR_INVALID"


class BuildStageFailed(BuildProcessorError):
    code = "BLOCKED_DATASET_BUILD_STAGE_FAILED"


class BuildSourceRevised(BuildProcessorError):
    code = "BLOCKED_SOURCE_REVISED"


class BuildResourceEvidenceInvalid(BuildProcessorError):
    code = "BLOCKED_RESOURCE_ENFORCEMENT_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class BuildStageLayout:
    release_id: str
    release_digest: str
    staging_relative_path: str
    final_relative_path: str
    staging_path: Path
    final_path: Path


class BuildStageCommandFactory(Protocol):
    def windows_command(
        self,
        *,
        stage: str,
        run_id: str,
        attempt_id: str,
        attempt_fence: int,
        pressure_rung: int,
        stage_timeout_seconds: int,
        plan_ref: str,
        layout: BuildStageLayout,
        result_path: Path,
        prerequisite_refs: Mapping[str, str],
    ) -> Sequence[str]: ...


@dataclass(frozen=True, slots=True)
class DefaultBuildStageCommandFactory:
    project_root: Path
    control_root: Path
    candidate_root: Path
    profile_path: Path

    def windows_command(
        self,
        *,
        stage: str,
        run_id: str,
        attempt_id: str,
        attempt_fence: int,
        pressure_rung: int,
        stage_timeout_seconds: int,
        plan_ref: str,
        layout: BuildStageLayout,
        result_path: Path,
        prerequisite_refs: Mapping[str, str],
    ) -> Sequence[str]:
        if stage not in {"prepare", "finalize-bins", "validate"}:
            raise BuildProcessorError(f"unsupported build stage: {stage}")
        command = [
            sys.executable,
            str(self.project_root / "scripts" / "dataset_release_build_stage.py"),
            stage,
            "--control-root",
            str(self.control_root),
            "--candidate-root",
            str(self.candidate_root),
            "--profile",
            str(self.profile_path),
            "--plan-ref",
            plan_ref,
            "--run-id",
            run_id,
            "--attempt-id",
            attempt_id,
            "--attempt-fence",
            str(attempt_fence),
            "--pressure-rung",
            str(pressure_rung),
            "--stage-timeout-seconds",
            str(stage_timeout_seconds),
            "--release-id",
            layout.release_id,
            "--release-digest",
            layout.release_digest,
            "--staging-relative-path",
            layout.staging_relative_path,
            "--result-path",
            str(result_path),
        ]
        for name, reference in sorted(prerequisite_refs.items()):
            command.extend(("--prerequisite-ref", f"{name}={reference}"))
        return command


class ProductionBuildProcessor:
    """Fenced, supervised and candidate-only BUILD processor."""

    def __init__(
        self,
        *,
        profile: DatasetProfile,
        profile_path: Path,
        project_root: Path,
        store: ControlStore,
        cas: CASStore,
        candidate_root: Path,
        qlib_toolchain: QlibDumpToolchain,
        stage_commands: BuildStageCommandFactory | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.profile = profile
        self.profile_path = Path(profile_path).resolve(strict=True)
        self.project_root = Path(project_root).resolve(strict=True)
        self.store = store
        self.cas = cas
        self.candidate_root = _plain_existing_root(candidate_root)
        self.qlib_toolchain = qlib_toolchain
        self.qlib_toolchain_receipt = qlib_toolchain.verify_content()
        self.stage_commands = stage_commands or DefaultBuildStageCommandFactory(
            project_root=self.project_root,
            control_root=self.store.root,
            candidate_root=self.candidate_root,
            profile_path=self.profile_path,
        )
        self.publisher = DatasetPublisher(store, candidate_root=self.candidate_root)
        self.state_machine = DatasetReleaseStateMachine(store)
        self._now = now
        if (
            str(self.profile.candidate_root).replace("\\", "/").casefold()
            != str(self.candidate_root).replace("\\", "/").casefold()
        ):
            raise BuildProcessorError("candidate root differs from allowlisted profile")

    def resource_spec(self, run: Mapping[str, Any]) -> WorkResourceSpec:
        plan, build_inputs = self._plan(run)
        release = _release_identity(build_inputs)
        _validate_run_plan_identity(run, plan, build_inputs, release)
        _validate_release_pit_binding(self.profile, self.cas, build_inputs, release)
        return WorkResourceSpec(
            policy=self.profile.resource_policy,
            hybrid_wsl=True,
            release_id=release.release_id,
            # The attempt identity/fence does not exist until after resource
            # admission and claim.  ``process`` binds the exact isolated path
            # before the first child or candidate write.
            staging_ref=None,
            pressure_rung=self._resume_pressure_rung(str(run["run_id"])),
            predicted_new_bytes=int(build_inputs["predicted_new_bytes"]),
        )

    def _resume_pressure_rung(self, run_id: str) -> int:
        """Restore the monotonic durable pressure rung across released attempts."""

        rows = self.store._many(
            """
            SELECT ordinal,error_ref FROM attempts
            WHERE run_id=? AND state='RELEASED_WAITING' AND error_ref IS NOT NULL
            ORDER BY ordinal
            """,
            (run_id,),
        )
        event_rows = self.store._many(
            """
            SELECT event_id,payload_ref FROM events
            WHERE run_id=? AND type='RESOURCE_WAITING_RESOURCE'
              AND payload_ref IS NOT NULL
            ORDER BY event_id
            """,
            (run_id,),
        )
        sequences: list[list[int]] = [[], []]
        for sequence, collection in zip(sequences, (event_rows, rows)):
            for row in collection:
                sequence.append(self._pressure_rung_from_receipt(run_id, row))
        if any(later < earlier for sequence in sequences for earlier, later in zip(sequence, sequence[1:])):
            raise BuildResourceEvidenceInvalid("durable pressure rung moved backwards")
        latest = [sequence[-1] for sequence in sequences if sequence]
        return max(latest) if latest else 0

    def _pressure_rung_from_receipt(self, run_id: str, row: Mapping[str, Any]) -> int:
        reference = str(row.get("payload_ref") or row.get("error_ref") or "")
        receipt = self.cas.get_json_bounded(reference, max_bytes=1024 * 1024)
        if (
            not isinstance(receipt, Mapping)
            or receipt.get("schema_version") != WORKER_ERROR_RECEIPT_SCHEMA
            or receipt.get("target_id") != run_id
            or receipt.get("disposition") != "WAITING"
            or receipt.get("kind") not in {"build", "build_resource_admission"}
        ):
            raise BuildResourceEvidenceInvalid("durable pressure-rung receipt identity is invalid")
        context = receipt.get("context")
        if (
            not isinstance(context, Mapping)
            or context.get("data_scope_changed") is not False
            or type(context.get("pressure_rung")) is not int
        ):
            raise BuildResourceEvidenceInvalid("durable pressure-rung receipt context is invalid")
        rung = int(context["pressure_rung"])
        if not 0 <= rung < len(self.profile.pressure_ladder["h5_batch"]):
            raise BuildResourceEvidenceInvalid("durable pressure rung exceeds the profile ladder")
        return rung

    def process(self, context: WorkerAttemptContext) -> ProcessorResult:
        run = self._active_run(context)
        plan, build_inputs = self._plan(run)
        release = _release_identity(build_inputs)
        _validate_run_plan_identity(run, plan, build_inputs, release)
        _validate_release_pit_binding(self.profile, self.cas, build_inputs, release)
        layout = self._layout(release, context)
        self._bind_attempt_staging(context, layout)

        prepare, prepare_ref, prepare_child = self._run_windows_stage(
            context,
            stage="prepare",
            execution_id="build-prepare",
            plan_ref=str(run["plan_ref"]),
            layout=layout,
            prerequisite_refs={},
        )
        _validate_stage_result(
            prepare,
            stage="prepare",
            run=run,
            context=context,
            layout=layout,
            profile=self.profile,
        )
        context.checkpoint()
        dump_operations = _qlib_dump_operations(
            prepare,
            layout=layout,
            attempt_id=context.claim.attempt_id,
            attempt_fence=context.claim.attempt_fence,
            expected_max_codes_per_batch=_resource_rung_value(prepare, "minute_batch"),
        )
        dump_children: dict[str, object] = {}
        dump_child_refs: dict[str, CASRef] = {}
        for operation in dump_operations:
            operation_id = str(operation["operation_id"])
            child = self._run_qlib_dump(
                context,
                operation=operation,
                layout=layout,
                dump_workers=_resource_rung_value(prepare, "dump_workers"),
            )
            dump_children[operation_id] = child
            dump_child_refs[operation_id] = self.cas.put_json(_portable_child_receipt(child))

        finalized, finalized_ref, finalized_child = self._run_windows_stage(
            context,
            stage="finalize-bins",
            execution_id="build-finalize-bins",
            plan_ref=str(run["plan_ref"]),
            layout=layout,
            prerequisite_refs={
                "prepare": prepare_ref.sha256,
                **{
                    f"qlib_dump_{operation_id}": reference.sha256
                    for operation_id, reference in sorted(dump_child_refs.items())
                },
            },
        )
        _validate_stage_result(
            finalized,
            stage="finalize-bins",
            run=run,
            context=context,
            layout=layout,
            profile=self.profile,
        )
        consumer_smoke, consumer_smoke_ref, consumer_smoke_child = self._run_consumer_smoke(
            context,
            layout=layout,
            release=release,
            cutoff=date.fromisoformat(str(build_inputs["cutoff"])),
            stock_instrument=_consumer_smoke_instrument(prepare),
        )
        run = self._transition(context, "EXECUTING", "VALIDATING")

        validated, validated_ref, validation_child = self._run_windows_stage(
            context,
            stage="validate",
            execution_id="build-validate",
            plan_ref=str(run["plan_ref"]),
            layout=layout,
            prerequisite_refs={
                "prepare": prepare_ref.sha256,
                **{
                    f"qlib_dump_{operation_id}": reference.sha256
                    for operation_id, reference in sorted(dump_child_refs.items())
                },
                "finalize_bins": finalized_ref.sha256,
                "consumer_smoke": consumer_smoke_ref.sha256,
            },
        )
        _validate_stage_result(
            validated,
            stage="validate",
            run=run,
            context=context,
            layout=layout,
            profile=self.profile,
        )
        source_recheck, source_recheck_ref, source_recheck_child = self._run_source_recheck(
            context,
            run=run,
            build_inputs=build_inputs,
        )
        validated = {
            **validated,
            "source_probe_ref": source_recheck["source_probe_ref"],
        }
        identities = self._validate_final_evidence(
            validated,
            run=run,
            context=context,
            build_inputs=build_inputs,
            release=release,
            layout=layout,
        )
        attestation_ref = self._register_pending_attestation(
            context=context,
            run=run,
            build_inputs=build_inputs,
            release=release,
            layout=layout,
            evidence=validated,
            identities=identities,
        )
        build_receipt_ref = self._write_build_receipt(
            context=context,
            run=run,
            plan=plan,
            build_inputs=build_inputs,
            release=release,
            layout=layout,
            evidence=validated,
            identities=identities,
            attestation_ref=attestation_ref,
            stage_refs={
                "prepare": prepare_ref.sha256,
                **{
                    f"qlib_dump_{operation_id}": reference.sha256
                    for operation_id, reference in sorted(dump_child_refs.items())
                },
                "finalize_bins": finalized_ref.sha256,
                "consumer_smoke": consumer_smoke_ref.sha256,
                "validate": validated_ref.sha256,
                "source_recheck": source_recheck_ref.sha256,
            },
            child_receipts={
                "prepare": _portable_child_receipt(prepare_child),
                **{
                    f"qlib_dump_{operation_id}": _portable_child_receipt(child)
                    for operation_id, child in sorted(dump_children.items())
                },
                "finalize_bins": _portable_child_receipt(finalized_child),
                "consumer_smoke": _portable_child_receipt(consumer_smoke_child),
                "validate": _portable_child_receipt(validation_child),
                "source_recheck": _portable_child_receipt(source_recheck_child),
            },
        )
        run = self._transition(context, "VALIDATING", "PREPARING_PUBLISH")
        assert context.claim.host is not None and context.claim.release is not None
        spec = PublishSpec(
            run_id=str(run["run_id"]),
            attempt_id=context.claim.attempt_id,
            attempt_fence=context.claim.attempt_fence,
            host_fence=context.claim.host.fence,
            release_fence=context.claim.release.fence,
            release_id=release.release_id,
            release_digest=release.digest,
            candidate_registration_id=identities["candidate_registration_id"],
            allowlisted_root_id=self.profile.candidate_root_id,
            volume_serial=volume_identity(self.candidate_root),
            root_relative_path=layout.final_relative_path,
            lineage_anchor=f"BUILD_RELEASE_DIGEST:{release.digest}",
            candidate_identity=identities["candidate_identity"],
            producer_provenance_state=ProducerProvenanceState.KNOWN.value,
            producer_provenance_digest_or_sentinel=identities["producer_provenance_digest"],
            pit_provenance_state=PitProvenanceState.KNOWN.value,
            profile=self.profile.profile,
            scope=str(build_inputs["scope"]),
            cutoff=str(build_inputs["cutoff"]),
            staging_path=layout.staging_path,
            final_path=layout.final_path,
            manifest_root=ensure_sha256(str(validated["manifest_root"]), field="manifest_root"),
            artifact_root=ensure_sha256(str(validated["artifact_root"]), field="artifact_root"),
            pit_snapshot_digest=ensure_sha256(
                str(build_inputs["source_snapshot"]["pit_snapshot_digest"]),
                field="pit_snapshot_digest",
            ),
            build_receipt_ref=build_receipt_ref.sha256,
            attestation_key=identities["attestation_key"],
            attestation_ref=attestation_ref.sha256,
            source_probe_key=identities["source_probe_key"],
            source_probe_ref=identities["source_probe_ref"],
        )
        self.publisher.prepare(spec)
        self.publisher.commit_files(release.release_id)
        self.publisher.finalize(release.release_id)
        return ProcessorResult.durable_success()

    def _plan(self, run: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        if str(run.get("operation_kind")) != "BUILD":
            raise BuildProcessorError("ProductionBuildProcessor accepts BUILD runs only")
        plan = self.cas.get_json_bounded(str(run.get("plan_ref", "")), max_bytes=MAX_BUILD_PLAN_BYTES)
        if not isinstance(plan, Mapping) or plan.get("schema_version") != RESOLUTION_PLAN_SCHEMA_VERSION:
            raise BuildProcessorError("build resolution plan schema is invalid")
        if plan.get("operation_kind") != "BUILD":
            raise BuildProcessorError("build resolution plan operation differs")
        build_inputs = plan.get("build_inputs")
        if not isinstance(build_inputs, Mapping) or build_inputs.get("schema_version") != BUILD_INPUTS_SCHEMA_VERSION:
            raise BuildProcessorError("immutable build inputs are missing")
        if dict(build_inputs.get("safety") or {}).get("candidate_writes") != 0:
            raise BuildProcessorError("resolution build inputs contain prior candidate writes")
        _validate_initial_migration_build_inputs(self.profile, build_inputs)
        return plan, build_inputs

    def _active_run(self, context: WorkerAttemptContext) -> Mapping[str, Any]:
        run = self.store.get_run(context.target_id)
        if run is None or run.get("state") != "EXECUTING" or run.get("active_attempt_id") != context.claim.attempt_id:
            raise BuildProcessorError("build run is not actively fenced in EXECUTING")
        return run

    def _layout(self, release: ReleaseIdentity, context: WorkerAttemptContext) -> BuildStageLayout:
        staging_relative = f".staging/{context.claim.attempt_id}/{context.claim.attempt_fence}"
        final_relative = release.release_id
        return BuildStageLayout(
            release_id=release.release_id,
            release_digest=release.digest,
            staging_relative_path=staging_relative,
            final_relative_path=final_relative,
            staging_path=_contained_future_path(self.candidate_root, staging_relative),
            final_path=_contained_future_path(self.candidate_root, final_relative),
        )

    def _bind_attempt_staging(
        self,
        context: WorkerAttemptContext,
        layout: BuildStageLayout,
    ) -> None:
        """Fence one not-yet-created staging path to the active attempt.

        A failed attempt's bytes are deliberately left untouched for audit.
        Every successor gets a different attempt/fence path, so it can rebuild
        without adopting or overwriting an old private Qlib/output tree.
        """

        expected = str(layout.staging_path)
        stamp = _timestamp(self._aware_now())
        with self.store.transaction() as connection:
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=?",
                (context.claim.attempt_id,),
            ).fetchone()
            run = connection.execute(
                "SELECT * FROM runs WHERE run_id=?",
                (context.target_id,),
            ).fetchone()
            if (
                attempt is None
                or run is None
                or attempt["run_id"] != context.target_id
                or attempt["state"] != "RUNNING"
                or int(attempt["attempt_fence"]) != context.claim.attempt_fence
                or run["state"] != "EXECUTING"
                or run["active_attempt_id"] != context.claim.attempt_id
            ):
                raise BuildProcessorError("build staging cannot bind outside the active attempt fence")
            current = attempt["staging_ref"]
            if current is not None and _path_identity_text(str(current)) != _path_identity_text(expected):
                raise BuildProcessorError("build attempt already owns a different staging path")
            updated = connection.execute(
                """
                UPDATE attempts SET staging_ref=?,updated_at=?
                WHERE attempt_id=? AND run_id=? AND state='RUNNING'
                  AND attempt_fence=? AND (staging_ref IS NULL OR staging_ref=?)
                """,
                (
                    expected,
                    stamp,
                    context.claim.attempt_id,
                    context.target_id,
                    context.claim.attempt_fence,
                    expected,
                ),
            )
            if updated.rowcount != 1:
                raise BuildProcessorError("build staging bind lost its attempt fence")

    def _run_windows_stage(
        self,
        context: WorkerAttemptContext,
        *,
        stage: str,
        execution_id: str,
        plan_ref: str,
        layout: BuildStageLayout,
        prerequisite_refs: Mapping[str, str],
    ) -> tuple[Mapping[str, Any], CASRef, object]:
        execution_root = (
            self.store.root
            / "attempt_runs"
            / f"{context.claim.attempt_id}-{context.claim.attempt_fence}"
            / execution_id
        )
        result_path = execution_root / "semantic_result.json"
        command = self.stage_commands.windows_command(
            stage=stage,
            run_id=context.target_id,
            attempt_id=context.claim.attempt_id,
            attempt_fence=context.claim.attempt_fence,
            pressure_rung=context.pressure_rung,
            stage_timeout_seconds=self.profile.stage_timeouts_seconds["full_build"],
            plan_ref=plan_ref,
            layout=layout,
            result_path=result_path,
            prerequisite_refs=prerequisite_refs,
        )
        child = context.run_supervised(
            command,
            execution_id=execution_id,
            cwd=self.project_root,
            runtime="windows",
            timeout_seconds=float(self.profile.stage_timeouts_seconds["full_build"]),
            cooperative_grace_seconds=30.0,
        )
        portable = _portable_child_receipt(child)
        _validate_supervised_resource_receipt(
            portable,
            profile=self.profile,
            execution_id=execution_id,
            runtime="windows",
            pressure_rung=context.pressure_rung,
            timeout_seconds=self.profile.stage_timeouts_seconds["full_build"],
        )
        if int(portable.get("returncode", -1)) != 0 or int(portable.get("active_processes", -1)) != 0:
            raise BuildStageFailed(f"supervised Windows stage failed: {stage}")
        value = _read_json_bounded(result_path, max_bytes=MAX_STAGE_RESULT_BYTES)
        reference = self.cas.put_json(value)
        self.cas.verify(reference)
        return value, reference, child

    def _run_qlib_dump(
        self,
        context: WorkerAttemptContext,
        *,
        operation: Mapping[str, Any],
        layout: BuildStageLayout,
        dump_workers: int,
    ) -> object:
        operation_id = str(operation["operation_id"])
        dataset = str(operation["dataset"])
        execution_id = f"build-dump-{operation_id}"
        csv_root = _contained_future_path(layout.staging_path, str(operation["csv_relative_path"]))
        qlib_root = _contained_future_path(layout.staging_path, str(operation["qlib_relative_path"]))
        command = build_qlib_dump_command(
            dataset=dataset,
            csv_root=csv_root,
            working_root=qlib_root,
            dump_workers=dump_workers,
            toolchain=self.qlib_toolchain,
            mode=str(operation["mode"]),
        )
        execution_root = (
            self.store.root
            / "attempt_runs"
            / f"{context.claim.attempt_id}-{context.claim.attempt_fence}"
            / execution_id
        )
        child = context.run_supervised(
            command,
            execution_id=execution_id,
            cwd=self.project_root,
            runtime="wsl",
            timeout_seconds=float(self.profile.stage_timeouts_seconds["qlib_dump"]),
            cooperative_grace_seconds=30.0,
            wsl=WslSupervisedOptions(
                distro=self.qlib_toolchain.distro,
                guardian_python=self.qlib_toolchain.guardian_python,
                guardian_script_wsl=self.qlib_toolchain.guardian_script_wsl,
                heartbeat_path_wsl=_windows_to_wsl(context.supervised_heartbeat_path),
                runner_python_wsl=self.qlib_toolchain.runner_python_wsl,
                runner_script_wsl=self.qlib_toolchain.runner_script_wsl,
                task_cwd_wsl=_windows_to_wsl(self.project_root),
                execution_root_wsl=_windows_to_wsl(execution_root),
            ),
        )
        portable = _portable_child_receipt(child)
        _validate_supervised_resource_receipt(
            portable,
            profile=self.profile,
            execution_id=execution_id,
            runtime="wsl",
            pressure_rung=context.pressure_rung,
            timeout_seconds=self.profile.stage_timeouts_seconds["qlib_dump"],
        )
        if (
            int(portable.get("returncode", -1)) != 0
            or int(portable.get("active_processes", -1)) != 0
            or portable.get("runtime") != "wsl"
            or not isinstance(portable.get("wsl_readback"), Mapping)
        ):
            raise BuildStageFailed(f"supervised WSL dump failed: {dataset}")
        if int(portable.get("job_peak_commit_bytes", -1)) > int(self.profile.resource_policy.hybrid_job_commit_bytes):
            raise BuildResourceEvidenceInvalid("WSL dump exceeded Windows-side Job cap")
        return child

    def _run_source_recheck(
        self,
        context: WorkerAttemptContext,
        *,
        run: Mapping[str, Any],
        build_inputs: Mapping[str, Any],
    ) -> tuple[Mapping[str, Any], CASRef, object]:
        artifact_ref = build_inputs.get("artifact_ready_contract_ref")
        try:
            artifact_reference = CASRef.from_value(artifact_ref)
            artifact_reference = self.cas.verify(artifact_reference)
        except (CASStoreError, TypeError, ValueError) as exc:
            raise BuildSourceRevised("prepublish recheck lacks immutable artifact-ready contract") from exc
        execution_id = SOURCE_RECHECK_EXECUTION_ID
        execution_root = (
            self.store.root
            / "attempt_runs"
            / f"{context.claim.attempt_id}-{context.claim.attempt_fence}"
            / execution_id
        )
        result_path = execution_root / "semantic_result.json"
        timeout = self.profile.stage_timeouts_seconds["source_freeze"]
        script = self.project_root / "scripts" / "dataset_release_source_recheck.py"
        command = [
            sys.executable,
            str(script),
            "--profile",
            str(self.profile.path),
            "--control-root",
            str(self.store.root),
            "--cutoff",
            str(build_inputs["cutoff"]),
            "--artifact-ready-contract-ref",
            artifact_reference.sha256,
            "--run-id",
            str(run["run_id"]),
            "--attempt-id",
            context.claim.attempt_id,
            "--attempt-fence",
            str(context.claim.attempt_fence),
            "--execution-id",
            execution_id,
            "--result-path",
            str(result_path),
            "--stage-timeout-seconds",
            str(timeout),
            "--pressure-rung",
            str(context.pressure_rung),
        ]
        child = context.run_source_recheck_supervised(
            command,
            execution_id=execution_id,
            cwd=self.project_root,
            timeout_seconds=float(timeout),
            cooperative_grace_seconds=30.0,
        )
        portable = _portable_child_receipt(child)
        _validate_supervised_resource_receipt(
            portable,
            profile=self.profile,
            execution_id=execution_id,
            runtime="windows",
            pressure_rung=context.pressure_rung,
            timeout_seconds=timeout,
        )
        if int(portable.get("returncode", -1)) != 0 or int(portable.get("active_processes", -1)) != 0:
            raise BuildSourceRevised("supervised prepublish source recheck failed")
        value = _read_json_bounded(result_path, max_bytes=MAX_STAGE_RESULT_BYTES)
        expected = {
            "schema_version": SOURCE_RECHECK_RESULT_SCHEMA,
            "status": "PASS",
            "run_id": str(run["run_id"]),
            "attempt_id": context.claim.attempt_id,
            "attempt_fence": context.claim.attempt_fence,
            "execution_id": execution_id,
            "artifact_ready_contract_ref": artifact_reference.as_dict(),
            "artifact_ready_content_root": build_inputs.get("artifact_ready_content_root"),
            "stage_timeout_seconds": timeout,
            "safety": {
                **_ZERO_SAFETY,
                "provider_database_writes": 0,
                "candidate_writes": 0,
            },
        }
        if any(value.get(key) != item for key, item in expected.items()):
            raise BuildSourceRevised("prepublish source recheck result identity differs")
        source_probe_ref = self.cas.verify(value.get("source_probe_ref"))
        if source_probe_ref.as_dict() != value.get("source_probe_ref"):
            raise BuildSourceRevised("prepublish source recheck probe reference is non-canonical")
        reference = self.cas.put_json(value)
        return value, self.cas.verify(reference), child

    def _run_consumer_smoke(
        self,
        context: WorkerAttemptContext,
        *,
        layout: BuildStageLayout,
        release: ReleaseIdentity,
        cutoff: date,
        stock_instrument: str,
    ) -> tuple[Mapping[str, Any], CASRef, object]:
        """Run the actual Qlib public-reader smoke under the WSL supervisor."""

        execution_id = "build-consumer-smoke"
        execution_root = (
            self.store.root
            / "attempt_runs"
            / f"{context.claim.attempt_id}-{context.claim.attempt_fence}"
            / execution_id
        )
        result_path = execution_root / "semantic_result.json"
        script = _windows_to_wsl(self.project_root / "scripts" / "dataset_release_candidate_consumer_smoke.py")
        values = (
            "python",
            script,
            "--daily-provider-uri",
            _windows_to_wsl(layout.staging_path / "daily_bin" / "qlib"),
            "--minute-provider-uri",
            _windows_to_wsl(layout.staging_path / "minute_bin" / "qlib"),
            "--index-h5-path",
            _windows_to_wsl(layout.staging_path / "index_context" / "index_daily.h5"),
            "--cutoff",
            cutoff.isoformat(),
            "--stock-instrument",
            stock_instrument,
            "--profile",
            self.profile.profile,
            "--run-id",
            context.target_id,
            "--attempt-id",
            context.claim.attempt_id,
            "--attempt-fence",
            str(context.claim.attempt_fence),
            "--release-id",
            release.release_id,
            "--release-digest",
            release.digest,
            "--staging-relative-path",
            layout.staging_relative_path,
            "--execution-id",
            execution_id,
            "--max-h5-rows",
            str(self.profile.resource_policy.validation_read_chunk_rows),
            "--stage-timeout-seconds",
            str(self.profile.stage_timeouts_seconds["consumer"]),
            "--result-path",
            _windows_to_wsl(result_path),
            "--control-root",
            _windows_to_wsl(self.store.root),
            "--candidate-root",
            _windows_to_wsl(self.candidate_root),
        )
        command = [
            "bash",
            "-lc",
            " && ".join(
                (
                    f"source {shlex.quote(self.qlib_toolchain.conda_sh)}",
                    f"conda activate {shlex.quote(self.qlib_toolchain.conda_env)}",
                    " ".join(shlex.quote(value) for value in values),
                )
            ),
        ]
        child = context.run_supervised(
            command,
            execution_id=execution_id,
            cwd=self.project_root,
            runtime="wsl",
            timeout_seconds=float(self.profile.stage_timeouts_seconds["consumer"]),
            cooperative_grace_seconds=30.0,
            wsl=WslSupervisedOptions(
                distro=self.qlib_toolchain.distro,
                guardian_python=self.qlib_toolchain.guardian_python,
                guardian_script_wsl=self.qlib_toolchain.guardian_script_wsl,
                heartbeat_path_wsl=_windows_to_wsl(context.supervised_heartbeat_path),
                runner_python_wsl=self.qlib_toolchain.runner_python_wsl,
                runner_script_wsl=self.qlib_toolchain.runner_script_wsl,
                task_cwd_wsl=_windows_to_wsl(self.project_root),
                execution_root_wsl=_windows_to_wsl(execution_root),
            ),
        )
        portable = _portable_child_receipt(child)
        _validate_supervised_resource_receipt(
            portable,
            profile=self.profile,
            execution_id=execution_id,
            runtime="wsl",
            pressure_rung=context.pressure_rung,
            timeout_seconds=self.profile.stage_timeouts_seconds["consumer"],
        )
        if (
            int(portable.get("returncode", -1)) != 0
            or int(portable.get("active_processes", -1)) != 0
            or portable.get("runtime") != "wsl"
            or not isinstance(portable.get("wsl_readback"), Mapping)
        ):
            raise BuildStageFailed("supervised WSL consumer smoke failed")
        value = _read_json_bounded(result_path, max_bytes=MAX_STAGE_RESULT_BYTES)
        try:
            validated = validate_candidate_consumer_smoke_receipt(
                value,
                profile=self.profile.profile,
                cutoff=cutoff,
                expected_index_codes=self.profile.index_codes,
                expected_identity={
                    "run_id": context.target_id,
                    "attempt_id": context.claim.attempt_id,
                    "attempt_fence": context.claim.attempt_fence,
                    "release_id": release.release_id,
                    "release_digest": release.digest,
                    "staging_relative_path": layout.staging_relative_path,
                },
                expected_stage_timeout_seconds=self.profile.stage_timeouts_seconds["consumer"],
            )
        except (CandidateConsumerSmokeError, TypeError, ValueError) as exc:
            raise BuildStageFailed(f"consumer smoke receipt is invalid: {exc}") from exc
        reference = self.cas.put_json(validated)
        self.cas.verify(reference)
        return validated, reference, child

    def _transition(
        self,
        context: WorkerAttemptContext,
        expected_state: str,
        next_state: str,
    ) -> Mapping[str, Any]:
        context.checkpoint()
        run = self.store.get_run(context.target_id)
        if run is None:
            raise BuildProcessorError("build run disappeared before stage transition")
        return self.state_machine.transition_owned_keep(
            run_id=context.target_id,
            attempt_id=context.claim.attempt_id,
            expected_state=expected_state,
            expected_row_version=int(run["row_version"]),
            attempt_fence=context.claim.attempt_fence,
            tokens=context.tokens,
            next_state=next_state,
        )

    def _validate_final_evidence(
        self,
        value: Mapping[str, Any],
        *,
        run: Mapping[str, Any],
        context: WorkerAttemptContext,
        build_inputs: Mapping[str, Any],
        release: ReleaseIdentity,
        layout: BuildStageLayout,
    ) -> dict[str, Any]:
        if value.get("validation_status") != "PASS" or int(value.get("required_validation_failures", -1)) != 0:
            raise BuildStageFailed("candidate required validation did not pass")
        validation_ref = self.cas.verify(value.get("validation_ref"))
        manifest_ref = self.cas.verify(value.get("manifest_ref"))
        validation = self.cas.get_json_bounded(validation_ref, max_bytes=MAX_STAGE_RESULT_BYTES)
        manifest = self.cas.get_json_bounded(manifest_ref, max_bytes=MAX_STAGE_RESULT_BYTES)
        component_ref = self.cas.verify(value.get("component_artifact_manifest_ref"))
        component_manifest = load_component_artifact_manifest(self.cas, component_ref)
        artifact_root = ensure_sha256(str(value.get("artifact_root", "")), field="artifact_root")
        manifest_root = ensure_sha256(str(value.get("manifest_root", "")), field="manifest_root")
        if (
            not isinstance(validation, Mapping)
            or validation.get("status") != "PASS"
            or validation.get("validation_fingerprint") != build_inputs["fingerprints"]["validation_fingerprint"]
            or not isinstance(manifest, Mapping)
            or manifest.get("artifact_root") != artifact_root
            or manifest.get("manifest_root") != manifest_root
            or manifest.get("component_artifact_manifest_ref") != component_ref.as_dict()
            or component_manifest.artifact_root != artifact_root
            or component_manifest.manifest_root != manifest_root
            or component_manifest.profile != self.profile.profile
            or component_manifest.scope != str(build_inputs["scope"])
            or component_manifest.cutoff != date.fromisoformat(str(build_inputs["cutoff"]))
            or component_manifest.semantic_profile_digest != build_inputs["semantic_profile_digest"]
            or component_manifest.producer_fingerprint != build_inputs["fingerprints"]["producer_fingerprint"]
            or component_manifest.artifact_fingerprint != build_inputs["fingerprints"]["artifact_fingerprint"]
            or component_manifest.validation_fingerprint != build_inputs["fingerprints"]["validation_fingerprint"]
            or component_manifest.artifact_ready_content_root != build_inputs["artifact_ready_content_root"]
            or value.get("artifact_ready_content_root") != build_inputs["artifact_ready_content_root"]
        ):
            raise BuildStageFailed("validation/manifest CAS evidence differs")
        validation_evidence = validation.get("evidence")
        if not isinstance(validation_evidence, Mapping):
            raise BuildStageFailed("validation omitted bounded evidence")
        try:
            validate_candidate_consumer_smoke_receipt(
                validation_evidence.get("qe_hmm_consumer_smoke", {}),
                profile=self.profile.profile,
                cutoff=date.fromisoformat(str(build_inputs["cutoff"])),
                expected_index_codes=self.profile.index_codes,
                expected_identity={
                    "run_id": str(run["run_id"]),
                    "attempt_id": context.claim.attempt_id,
                    "attempt_fence": context.claim.attempt_fence,
                    "release_id": release.release_id,
                    "release_digest": release.digest,
                    "staging_relative_path": layout.staging_relative_path,
                },
                expected_stage_timeout_seconds=self.profile.stage_timeouts_seconds["consumer"],
            )
        except (CandidateConsumerSmokeError, TypeError, ValueError) as exc:
            raise BuildStageFailed(f"validation omitted actual Qlib consumer evidence: {exc}") from exc
        producer_digest = ensure_sha256(
            str(value.get("producer_provenance_digest", "")),
            field="producer_provenance_digest",
        )
        if producer_digest != build_inputs["fingerprints"]["producer_fingerprint"]:
            raise BuildStageFailed("producer provenance differs from frozen plan")
        probe = _validate_fresh_probe(
            self.cas,
            value.get("source_probe_ref"),
            logical_request_key=str(build_inputs["logical_request_key"]),
            source_content_root=str(build_inputs["source_snapshot"]["source_content_root"]),
            pit_snapshot_digest=str(build_inputs["source_snapshot"]["pit_snapshot_digest"]),
            run_id=str(run["run_id"]),
            attempt_id=context.claim.attempt_id,
            attempt_fence=context.claim.attempt_fence,
            execution_id=SOURCE_RECHECK_EXECUTION_ID,
            now=self._aware_now(),
        )
        registration_id = build_candidate_registration_id(release.digest)
        candidate_identity = CandidateIdentity(
            registration_uuid=registration_id,
            allowlisted_root_id=self.profile.candidate_root_id,
            volume_serial=volume_identity(self.candidate_root),
            root_relative_path=layout.final_relative_path,
            profile=self.profile.profile,
            scope=Scope(str(build_inputs["scope"])),
            cutoff=date.fromisoformat(str(build_inputs["cutoff"])),
            lineage_anchor=f"BUILD_RELEASE_DIGEST:{release.digest}",
            pit_provenance_state=PitProvenanceState.KNOWN,
            pit_provenance_digest_or_sentinel=str(build_inputs["source_snapshot"]["pit_snapshot_digest"]),
            artifact_root=artifact_root,
            producer_provenance_state=ProducerProvenanceState.KNOWN,
            producer_provenance_digest_or_sentinel=producer_digest,
        ).key
        attestation = AttestationIdentity(
            candidate_identity=candidate_identity,
            producer_provenance_state=ProducerProvenanceState.KNOWN,
            producer_provenance_digest_or_sentinel=producer_digest,
            artifact_root=artifact_root,
            current_source_content_root=str(build_inputs["source_snapshot"]["source_content_root"]),
            pit_digest=str(build_inputs["source_snapshot"]["pit_snapshot_digest"]),
            semantic_profile_digest=str(build_inputs["semantic_profile_digest"]),
            validation_fingerprint=str(build_inputs["fingerprints"]["validation_fingerprint"]),
            equivalence_mode=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT,
            source_probe_key=probe["source_probe_key"],
        )
        if component_manifest.candidate_identity != candidate_identity:
            raise BuildStageFailed("component manifest candidate identity differs")
        return {
            "candidate_registration_id": registration_id,
            "candidate_identity": candidate_identity,
            "producer_provenance_digest": producer_digest,
            "source_probe_key": probe["source_probe_key"],
            "source_probe_ref": probe["source_probe_ref"],
            "source_probe_observed_at": probe["observed_at"],
            "source_probe_valid_until": probe["valid_until"],
            "attestation_key": attestation.key,
            "attestation_target_key": attestation.target_key,
            "validation_ref": validation_ref.sha256,
            "manifest_ref": manifest_ref.sha256,
            "component_artifact_manifest_ref": component_ref.as_dict(),
        }

    def _register_pending_attestation(
        self,
        *,
        context: WorkerAttemptContext,
        run: Mapping[str, Any],
        build_inputs: Mapping[str, Any],
        release: ReleaseIdentity,
        layout: BuildStageLayout,
        evidence: Mapping[str, Any],
        identities: Mapping[str, Any],
    ) -> CASRef:
        observed = _parse_utc(identities["source_probe_observed_at"])
        valid_until = _parse_utc(identities["source_probe_valid_until"])
        receipt = {
            "schema_version": ATTESTATION_SCHEMA_VERSION,
            "attestation_key": identities["attestation_key"],
            "attestation_observation_key": identities["attestation_key"],
            "attestation_target_key": identities["attestation_target_key"],
            "run_id": str(run["run_id"]),
            "run_generation_digest": str(run["run_generation_digest"]),
            "resolved_intent_key": str(build_inputs["resolved_intent_key"]),
            "release_id": release.release_id,
            "release_digest": release.digest,
            "candidate_identity": identities["candidate_identity"],
            "candidate_path_identity": layout.final_relative_path,
            "candidate_artifact_root": str(evidence["artifact_root"]),
            "producer_provenance_state": ProducerProvenanceState.KNOWN.value,
            "producer_provenance_digest_or_sentinel": identities["producer_provenance_digest"],
            "current_source_content_root": str(build_inputs["source_snapshot"]["source_content_root"]),
            "source_probe_key": identities["source_probe_key"],
            "source_probe_ref": identities["source_probe_ref"],
            "pit_snapshot_digest": str(build_inputs["source_snapshot"]["pit_snapshot_digest"]),
            "semantic_profile_digest": str(build_inputs["semantic_profile_digest"]),
            "validation_fingerprint": str(build_inputs["fingerprints"]["validation_fingerprint"]),
            "validation_ref": identities["validation_ref"],
            "observed_at": _timestamp(observed),
            "valid_until": _timestamp(valid_until),
            "equivalence_mode": EquivalenceMode.CURRENT_SOURCE_EQUIVALENT.value,
            "outcome": EquivalenceMode.CURRENT_SOURCE_EQUIVALENT.value,
            "eligible_for_noop_reuse": True,
            "reason": "new candidate passed full frozen-source validation",
            "safety": dict(_ZERO_SAFETY),
        }
        if "initial_migration_plan" in build_inputs:
            receipt["initial_migration_plan"] = dict(build_inputs["initial_migration_plan"])
        reference = self.cas.put_json(receipt)
        self.cas.verify(reference)
        self.state_machine.register_attestation(
            attestation_id=None,
            attestation_key=identities["attestation_key"],
            attestation_target_key=identities["attestation_target_key"],
            subject_type="candidate",
            subject_digest=identities["candidate_identity"],
            candidate_identity=identities["candidate_identity"],
            producer_provenance_state=ProducerProvenanceState.KNOWN.value,
            producer_provenance_digest_or_sentinel=identities["producer_provenance_digest"],
            candidate_artifact_root=str(evidence["artifact_root"]),
            current_source_content_root=str(build_inputs["source_snapshot"]["source_content_root"]),
            source_probe_key=identities["source_probe_key"],
            source_probe_ref=identities["source_probe_ref"],
            pit_snapshot_digest=str(build_inputs["source_snapshot"]["pit_snapshot_digest"]),
            semantic_profile_digest=str(build_inputs["semantic_profile_digest"]),
            validation_fingerprint=str(build_inputs["fingerprints"]["validation_fingerprint"]),
            observed_at=observed,
            valid_until=valid_until,
            equivalence_mode=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT.value,
            outcome=EquivalenceMode.CURRENT_SOURCE_EQUIVALENT.value,
            receipt_ref=reference.sha256,
            committed=False,
        )
        return reference

    def _write_build_receipt(
        self,
        *,
        context: WorkerAttemptContext,
        run: Mapping[str, Any],
        plan: Mapping[str, Any],
        build_inputs: Mapping[str, Any],
        release: ReleaseIdentity,
        layout: BuildStageLayout,
        evidence: Mapping[str, Any],
        identities: Mapping[str, Any],
        attestation_ref: CASRef,
        stage_refs: Mapping[str, str],
        child_receipts: Mapping[str, Mapping[str, Any]],
    ) -> CASRef:
        receipt = {
            "schema_version": BUILD_RECEIPT_SCHEMA,
            "status": "PASS",
            "run_id": str(run["run_id"]),
            "attempt_id": context.claim.attempt_id,
            "attempt_fence": context.claim.attempt_fence,
            "release_id": release.release_id,
            "release_digest": release.digest,
            "candidate_registration_id": identities["candidate_registration_id"],
            "candidate_identity": identities["candidate_identity"],
            "candidate_root_id": self.profile.candidate_root_id,
            "candidate_root_relative_path": layout.final_relative_path,
            "profile": self.profile.profile,
            "scope": str(build_inputs["scope"]),
            "cutoff": str(build_inputs["cutoff"]),
            "resolved_intent_key": str(build_inputs["resolved_intent_key"]),
            "plan_ref": str(run["plan_ref"]),
            "action_plan_digest": str(plan["action_plan_digest"]),
            "source_manifest_ref": dict(build_inputs["source_manifest_ref"]),
            "source_content_root": str(build_inputs["source_snapshot"]["source_content_root"]),
            "pit_snapshot_ref": dict(build_inputs["pit_snapshot_ref"]),
            "pit_snapshot_digest": str(build_inputs["source_snapshot"]["pit_snapshot_digest"]),
            "producer_provenance_state": ProducerProvenanceState.KNOWN.value,
            "producer_provenance_digest": identities["producer_provenance_digest"],
            "artifact_root": str(evidence["artifact_root"]),
            "artifact_snapshot": dict(evidence["artifact_snapshot"]),
            "manifest_root": str(evidence["manifest_root"]),
            "manifest_ref": identities["manifest_ref"],
            "component_artifact_manifest_ref": dict(identities["component_artifact_manifest_ref"]),
            "artifact_ready_contract_ref": dict(build_inputs["artifact_ready_contract_ref"]),
            "artifact_ready_content_root": str(build_inputs["artifact_ready_content_root"]),
            "artifact_ready_provenance_root": str(build_inputs["artifact_ready_provenance_root"]),
            "validation_ref": identities["validation_ref"],
            "source_probe_key": identities["source_probe_key"],
            "source_probe_ref": identities["source_probe_ref"],
            "attestation_key": identities["attestation_key"],
            "attestation_ref": attestation_ref.sha256,
            "stage_refs": dict(stage_refs),
            "supervised_children": dict(child_receipts),
            "resource_policy_digest": self.profile.resource_policy_digest,
            "qlib_toolchain": dict(self.qlib_toolchain_receipt),
            "production_activation": "not_requested",
            "node1_distribution": "not_requested",
            "database_repair": "not_requested",
            "runtime_restart": "not_requested",
            "cleanup": "not_requested",
            "runtime_real_data_evidence": str(evidence.get("runtime_real_data_evidence", "not_run_not_authorized")),
            "safety": dict(_ZERO_SAFETY),
        }
        reference = self.cas.put_json(receipt)
        self.cas.verify(reference)
        return reference

    def _aware_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise BuildProcessorError("build processor clock is timezone-naive")
        return value.astimezone(UTC)


def _release_identity(build_inputs: Mapping[str, Any]) -> ReleaseIdentity:
    fingerprints = build_inputs.get("fingerprints")
    snapshot = build_inputs.get("source_snapshot")
    if not isinstance(fingerprints, Mapping) or not isinstance(snapshot, Mapping):
        raise BuildProcessorError("build fingerprints/source snapshot are missing")
    try:
        return ReleaseIdentity(
            resolved_intent_key=str(build_inputs["resolved_intent_key"]),
            frozen_pit_spans_digest=str(snapshot["pit_snapshot_digest"]),
            scope=Scope(str(build_inputs["scope"])),
            producer_fingerprint=str(fingerprints["producer_fingerprint"]),
            artifact_fingerprint=str(fingerprints["artifact_fingerprint"]),
            cutoff=date.fromisoformat(str(build_inputs["cutoff"])),
            profile=str(build_inputs["profile"]),
        )
    except (KeyError, ValueError, IdentityConflictError) as exc:
        raise BuildProcessorError("release identity cannot be derived from build plan") from exc


def _validate_initial_migration_build_inputs(
    profile: DatasetProfile,
    build_inputs: Mapping[str, Any],
) -> None:
    value = build_inputs.get("initial_migration_plan")
    if value is None:
        return
    required = {
        "plan_id",
        "plan_digest",
        "fixed_cutoff",
        "scope",
        "sample_instruments",
        "event_windows",
        "index_windows",
        "source_identity_policy",
    }
    if not isinstance(value, Mapping) or set(value) != required:
        raise BuildProcessorError("initial migration build identity schema differs")
    plan_id = str(value.get("plan_id") or "")
    if plan_id not in profile.initial_migration_plan_ids:
        raise BuildProcessorError("initial migration build plan is not profile-allowlisted")
    try:
        plan = load_initial_migration_plan(profile.path.parent / "migrations" / f"{plan_id}.yaml")
        plan_digest = ensure_sha256(str(value.get("plan_digest") or ""), field="plan_digest")
    except (DatasetReleaseError, OSError, ValueError) as exc:
        raise BuildProcessorError("initial migration build plan cannot be verified") from exc
    expected = {
        "plan_id": plan.plan_id,
        "plan_digest": plan.plan_digest,
        "fixed_cutoff": plan.cutoff.isoformat(),
        "scope": str(build_inputs.get("scope")),
        "sample_instruments": list(plan.sample_instruments),
        "event_windows": [dict(item) for item in plan.event_windows],
        "index_windows": [dict(item) for item in plan.index_windows],
        "source_identity_policy": plan.source_identity_policy,
    }
    scope = str(build_inputs.get("scope"))
    if (
        dict(value) != expected
        or plan_digest != plan.plan_digest
        or str(build_inputs.get("cutoff")) != plan.cutoff.isoformat()
        or not plan.allows_scope(scope)
    ):
        raise BuildProcessorError("initial migration build inputs differ from the checked-in plan")


def _validate_release_pit_binding(
    profile: DatasetProfile,
    cas: CASStore,
    build_inputs: Mapping[str, Any],
    release: ReleaseIdentity,
) -> None:
    if profile.pit_authority_status != "ACTIVE_CANONICAL":
        return
    reference = build_inputs.get("pit_snapshot_ref")
    source_snapshot = build_inputs.get("source_snapshot")
    if not isinstance(reference, Mapping) or not isinstance(source_snapshot, Mapping):
        raise BuildProcessorError("canonical release PIT reference is missing")
    try:
        payload = cas.get_json_bounded(str(reference["sha256"]), max_bytes=MAX_BUILD_PLAN_BYTES)
        if not isinstance(payload, Mapping):
            raise PitSnapshotError("canonical release PIT artifact is not a mapping")
        snapshot = frozen_pit_snapshot_from_mapping(payload)
        expected_digest = ensure_sha256(
            str(source_snapshot["pit_snapshot_digest"]),
            field="pit_snapshot_digest",
        )
        require_canonical_frozen_snapshot(
            snapshot,
            release_id=release.release_id,
            rolling_cutoff_spans_sha256=expected_digest,
            consumer="qe_training",
        )
    except (KeyError, CASStoreError, PitSnapshotError, ValueError) as exc:
        raise BuildProcessorError("canonical release PIT binding is invalid") from exc


def _consumer_smoke_instrument(prepare: Mapping[str, Any]) -> str:
    value = str(prepare.get("consumer_smoke_instrument", "")).strip().upper()
    if len(value) != 9 or value[6] != "." or not value[:6].isdigit() or value[7:] not in {"SH", "SZ"}:
        raise BuildStageFailed("prepare stage omitted a safe consumer smoke instrument")
    return value


def _qlib_dump_operations(
    prepare: Mapping[str, Any],
    *,
    layout: BuildStageLayout,
    attempt_id: str,
    attempt_fence: int,
    expected_max_codes_per_batch: int,
) -> tuple[Mapping[str, Any], ...]:
    """Validate the exact prepare-stage dump set; REUSE emits no operation."""

    raw = prepare.get("qlib_dump_operations")
    if not isinstance(raw, list) or len(raw) > 64:
        raise BuildStageFailed("prepare stage Qlib dump operation set is invalid")
    expected_fields = {
        "operation_id",
        "dataset",
        "mode",
        "component_action",
        "csv_relative_path",
        "qlib_relative_path",
        "writer_targets_digest",
        "batch_manifest_identity",
        "batch_manifest_sha256",
    }
    allowed_pairs = {
        ("FULL_REBUILD", "batched_full"),
        ("INCREMENTAL", "batched_patch"),
        ("SELECTIVE_REBUILD", "batched_patch"),
    }
    output: list[Mapping[str, Any]] = []
    identities: set[str] = set()
    for value in raw:
        if not isinstance(value, Mapping) or set(value) != expected_fields:
            raise BuildStageFailed("Qlib dump operation schema differs")
        operation_id = str(value.get("operation_id", ""))
        if (
            not operation_id
            or len(operation_id) > 64
            or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for character in operation_id)
            or operation_id in identities
        ):
            raise BuildStageFailed("Qlib dump operation identity differs")
        identities.add(operation_id)
        dataset = str(value.get("dataset", ""))
        mode = str(value.get("mode", ""))
        action = str(value.get("component_action", ""))
        if dataset not in {"daily_bin", "minute_bin"} or (action, mode) not in allowed_pairs:
            raise BuildStageFailed("Qlib dump operation action/mode differs")
        csv_relative = str(value["csv_relative_path"]).replace("\\", "/")
        qlib_relative = str(value["qlib_relative_path"]).replace("\\", "/")
        if not csv_relative.startswith(f"{dataset}/") or not qlib_relative.startswith(f"{dataset}/"):
            raise BuildStageFailed("Qlib dump path is outside its component")
        private_prefix = f"{dataset}/.writer-private/{operation_id}/"
        if (
            csv_relative != f"{private_prefix}csv"
            or qlib_relative != f"{private_prefix}qlib"
            or qlib_relative == f"{dataset}/qlib"
        ):
            raise BuildStageFailed("Qlib external writer is not isolated from the final/COW tree")
        csv_root = _contained_future_path(layout.staging_path, csv_relative)
        qlib_root = _contained_future_path(layout.staging_path, qlib_relative)
        if not csv_root.is_dir():
            raise BuildStageFailed("prepared Qlib CSV root is missing")
        _assert_plain(csv_root)
        expected_target_digest = digest_named_fields(
            "dataset_release_qlib_dump_writer_targets_v1",
            {"dataset": dataset, "mode": mode, "target": qlib_relative},
        )
        if value.get("writer_targets_digest") != expected_target_digest:
            raise BuildStageFailed("Qlib dump writer target digest differs")
        if mode in {"batched_full", "batched_patch"}:
            if not qlib_root.is_dir():
                raise BuildStageFailed("batched private Qlib authority root is missing")
            _assert_plain(qlib_root)
            _validate_batch_manifest_binding(
                csv_root,
                value,
                dataset=dataset,
                attempt_id=attempt_id,
                attempt_fence=attempt_fence,
                expected_max_codes_per_batch=expected_max_codes_per_batch,
            )
        elif qlib_root.exists():
            raise BuildStageFailed("isolated Qlib dump target already exists")
        elif (
            value.get("batch_manifest_identity") is not None
            or value.get("batch_manifest_sha256") is not None
            or (csv_root / "batch_manifest.json").exists()
        ):
            raise BuildStageFailed("full Qlib dump unexpectedly contains a batch manifest")
        output.append(dict(value))
    return tuple(output)


def _validate_batch_manifest_binding(
    csv_root: Path,
    operation: Mapping[str, Any],
    *,
    dataset: str,
    attempt_id: str,
    attempt_fence: int,
    expected_max_codes_per_batch: int,
) -> None:
    path = csv_root / "batch_manifest.json"
    try:
        value = _read_json_bounded(path, max_bytes=MAX_BATCH_MANIFEST_BYTES)
    except (BuildStageFailed, BuildProcessorError) as exc:
        raise BuildStageFailed("batched Qlib manifest is unavailable") from exc
    try:
        declared_identity = ensure_sha256(
            str(operation.get("batch_manifest_identity", "")),
            field="batch_manifest_identity",
        )
        declared_sha = ensure_sha256(
            str(operation.get("batch_manifest_sha256", "")),
            field="batch_manifest_sha256",
        )
    except (DatasetReleaseError, TypeError, ValueError) as exc:
        raise BuildStageFailed("batched Qlib manifest digest is invalid") from exc
    raw_sha = _sha256_file(path)
    body = {key: item for key, item in value.items() if key != "manifest_identity"}
    encoded = json.dumps(
        body,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    actual_identity = hashlib.sha256(encoded).hexdigest()
    checkpoint = value.get("resource_checkpoint_identity")
    expected_fields = {
        "schema_version",
        "manifest_identity",
        "dataset",
        "freq",
        "fields",
        "max_codes_per_batch",
        "per_batch_timeout_seconds",
        "resource_checkpoint_identity",
        "phases",
        "expected_total_code_writes",
        "expected_total_rows",
    }
    if (
        set(value) != expected_fields
        or value.get("schema_version") != "dataset_release_qlib_batched_dump_manifest_v1"
        or value.get("dataset") != dataset
        or value.get("freq") != ("day" if dataset == "daily_bin" else "1min")
        or tuple(value.get("fields") or ()) != tuple(QLIB_STOCK_FIELDS)
        or type(expected_max_codes_per_batch) is not int
        or not 0 < expected_max_codes_per_batch <= 20
        or type(value.get("max_codes_per_batch")) is not int
        or int(value["max_codes_per_batch"]) != expected_max_codes_per_batch
        or type(value.get("per_batch_timeout_seconds")) is not int
        or not 0 < int(value["per_batch_timeout_seconds"]) <= 1800
        or not isinstance(value.get("phases"), list)
        or not value["phases"]
        or type(value.get("expected_total_code_writes")) is not int
        or int(value["expected_total_code_writes"]) <= 0
        or type(value.get("expected_total_rows")) is not int
        or int(value["expected_total_rows"]) <= 0
        or value.get("manifest_identity") != actual_identity
        or declared_identity != actual_identity
        or declared_sha != raw_sha
        or not isinstance(checkpoint, Mapping)
        or set(checkpoint) != {"attempt_id", "fence", "execution_id"}
        or checkpoint.get("attempt_id") != attempt_id
        or checkpoint.get("fence") != attempt_fence
        or checkpoint.get("execution_id") != f"build-dump-{dataset.removesuffix('_bin')}"
    ):
        raise BuildStageFailed("batched Qlib manifest identity differs")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_run_plan_identity(
    run: Mapping[str, Any],
    plan: Mapping[str, Any],
    build_inputs: Mapping[str, Any],
    release: ReleaseIdentity,
) -> None:
    expected_target = build_operation_target(str(build_inputs["resolved_intent_key"]), str(plan["action_plan_digest"]))
    fingerprints = build_inputs["fingerprints"]
    expected_generation = RunGenerationIdentity(
        operation_kind=OperationKind.BUILD,
        decision_schema=str(fingerprints["decision_schema"]),
        producer_fingerprint=str(fingerprints["producer_fingerprint"]),
        artifact_fingerprint=str(fingerprints["artifact_fingerprint"]),
        validation_identity=str(fingerprints["validation_fingerprint"]),
        sample_policy=str(fingerprints["sample_policy"]),
        operation_target=expected_target,
    ).digest
    mismatches = {
        "plan_resolved_intent_key": (
            plan.get("resolved_intent_key"),
            build_inputs.get("resolved_intent_key"),
        ),
        "run_generation_digest": (
            run.get("run_generation_digest"),
            expected_generation,
        ),
        "release_profile": (release.profile, build_inputs.get("profile")),
    }
    failed = {
        key: {"actual": actual, "expected": expected}
        for key, (actual, expected) in mismatches.items()
        if actual != expected
    }
    if failed:
        raise BuildProcessorError("run/build plan identity differs", context=failed)


def _validate_stage_result(
    value: Mapping[str, Any],
    *,
    stage: str,
    run: Mapping[str, Any],
    context: WorkerAttemptContext,
    layout: BuildStageLayout,
    profile: DatasetProfile,
) -> None:
    expected = {
        "schema_version": BUILD_STAGE_RESULT_SCHEMA,
        "stage": stage,
        "status": "PASS",
        "run_id": str(run["run_id"]),
        "attempt_id": context.claim.attempt_id,
        "attempt_fence": context.claim.attempt_fence,
        "release_id": layout.release_id,
        "release_digest": layout.release_digest,
        "staging_relative_path": layout.staging_relative_path,
        "stage_timeout_seconds": profile.stage_timeouts_seconds["full_build"],
    }
    mismatch = {
        key: {"expected": expected_value, "actual": value.get(key)}
        for key, expected_value in expected.items()
        if value.get(key) != expected_value
    }
    if mismatch or dict(value.get("safety") or {}) != _ZERO_SAFETY:
        raise BuildStageFailed(
            f"supervised stage result identity/safety differs: {stage}",
            context=mismatch,
        )
    _validate_resource_receipt(
        value.get("resource_receipt"),
        profile=profile,
        stage=stage,
        expected_pressure_rung=context.pressure_rung,
    )


def _validate_resource_receipt(
    value: Any,
    *,
    profile: DatasetProfile,
    stage: str,
    expected_pressure_rung: int,
) -> None:
    if not isinstance(value, Mapping):
        raise BuildResourceEvidenceInvalid(f"resource receipt is missing: {stage}")
    if (
        value.get("schema_version") != BUILD_RESOURCE_RECEIPT_SCHEMA
        or value.get("policy_digest") != profile.resource_policy_digest
        or value.get("stage") != stage
        or value.get("admission_checked") is not True
        or value.get("all_chunks_checked") is not True
        or value.get("memory_control_semantics")
        != {
            "factor_h5": "bounded_date_slice_plus_row_group_rows_v1",
            "h5_batch": "reserved_profile_telemetry_not_consumed_v1",
            "minute_batch": "child_manifest_plus_parent_bound_v1",
        }
    ):
        raise BuildResourceEvidenceInvalid(f"resource receipt identity differs: {stage}")
    checkpoints = value.get("checkpoints")
    if not isinstance(checkpoints, list) or len(checkpoints) < 2:
        raise BuildResourceEvidenceInvalid(f"resource checkpoints are incomplete: {stage}")
    sequences = [item.get("sequence") for item in checkpoints if isinstance(item, Mapping)]
    if sequences != list(range(len(checkpoints))):
        raise BuildResourceEvidenceInvalid(f"resource checkpoint sequence differs: {stage}")
    chunk_ids: set[str] = set()
    previous_rung = -1
    for item in checkpoints:
        if not isinstance(item, Mapping):
            raise BuildResourceEvidenceInvalid(f"resource checkpoint is invalid: {stage}")
        kind = item.get("kind")
        if kind not in {"admission", "chunk", "final"} or item.get("decision") != "READY":
            raise BuildResourceEvidenceInvalid(f"resource checkpoint is not READY: {stage}")
        rung = int(item.get("pressure_rung", -1))
        if (
            rung < previous_rung
            or rung != expected_pressure_rung
            or not 0 <= rung < len(profile.pressure_ladder["h5_batch"])
        ):
            raise BuildResourceEvidenceInvalid(f"resource pressure rung is invalid: {stage}")
        previous_rung = rung
        if kind == "chunk":
            chunk_id = str(item.get("chunk_id", ""))
            if not chunk_id or chunk_id in chunk_ids:
                raise BuildResourceEvidenceInvalid(f"resource chunk identity differs: {stage}")
            chunk_ids.add(chunk_id)
        for field in ("host_available_bytes", "owned_private_commit_bytes"):
            if type(item.get(field)) is not int or int(item[field]) < 0:
                raise BuildResourceEvidenceInvalid(f"resource checkpoint telemetry is invalid: {stage}:{field}")
    if int(value.get("chunks_completed", -1)) != len(chunk_ids):
        raise BuildResourceEvidenceInvalid(f"resource chunk count differs: {stage}")
    peak = int(value.get("peak_owned_private_commit_bytes", -1))
    if not 0 <= peak <= profile.resource_policy.aggregate_private_commit_bytes:
        raise BuildResourceEvidenceInvalid(f"resource peak exceeds policy: {stage}")
    effective = value.get("effective_rung")
    if not isinstance(effective, Mapping):
        raise BuildResourceEvidenceInvalid(f"effective pressure rung is missing: {stage}")
    rung_index = int(effective.get("index", -1))
    if rung_index != previous_rung or rung_index != expected_pressure_rung:
        raise BuildResourceEvidenceInvalid(f"effective pressure rung differs: {stage}")
    expected_fields = {
        "h5_batch": profile.pressure_ladder["h5_batch"][min(rung_index, len(profile.pressure_ladder["h5_batch"]) - 1)],
        "minute_batch": profile.pressure_ladder["minute_batch"][
            min(rung_index, len(profile.pressure_ladder["minute_batch"]) - 1)
        ],
        "chunk_months": profile.pressure_ladder["date_chunk_months"][
            min(rung_index, len(profile.pressure_ladder["date_chunk_months"]) - 1)
        ],
        "row_group_rows": profile.pressure_ladder["row_group_rows"][
            min(rung_index, len(profile.pressure_ladder["row_group_rows"]) - 1)
        ],
        "dump_workers": profile.pressure_ladder["dump_workers"][
            min(rung_index, len(profile.pressure_ladder["dump_workers"]) - 1)
        ],
    }
    if any(int(effective.get(key, -1)) != int(expected) for key, expected in expected_fields.items()):
        raise BuildResourceEvidenceInvalid(f"effective pressure values differ: {stage}")


def _validate_supervised_resource_receipt(
    value: Mapping[str, Any],
    *,
    profile: DatasetProfile,
    execution_id: str,
    runtime: str,
    pressure_rung: int,
    timeout_seconds: int,
) -> None:
    gate = value.get("resource_gate_receipt")
    expected_settings = {
        "h5_batch": profile.pressure_ladder["h5_batch"][pressure_rung],
        "minute_batch": profile.pressure_ladder["minute_batch"][pressure_rung],
        "chunk_months": profile.pressure_ladder["date_chunk_months"][pressure_rung],
        "row_group_rows": profile.pressure_ladder["row_group_rows"][pressure_rung],
        "dump_workers": profile.pressure_ladder["dump_workers"][pressure_rung],
    }
    if (
        value.get("schema_version") != "dataset_supervised_execution_receipt_v1"
        or value.get("execution_id") != execution_id
        or value.get("runtime") != runtime
        or float(value.get("timeout_seconds", -1)) != float(timeout_seconds)
        or not isinstance(gate, Mapping)
        or gate.get("schema_version") != RESOURCE_GATE_RECEIPT_SCHEMA
        or int(gate.get("sample_count", 0)) < 2
        or gate.get("final_status") != "READY"
        or gate.get("checkpoint_requested") is not False
        or int(gate.get("pressure_rung", -1)) != pressure_rung
        or int(gate.get("next_pressure_rung", -1)) != pressure_rung
        or dict(gate.get("pressure_settings") or {}) != expected_settings
        or gate.get("wsl_required") is not (runtime == "wsl")
        or gate.get("data_scope_changed") is not False
        or int(gate.get("aggregate_owned_peak_commit_bytes", -1)) < 0
        or int(gate.get("aggregate_owned_peak_commit_bytes", -1))
        > profile.resource_policy.aggregate_private_commit_bytes
    ):
        raise BuildResourceEvidenceInvalid(f"authoritative supervised resource receipt differs: {execution_id}")


def _resource_rung_value(stage: Mapping[str, Any], field: str) -> int:
    try:
        value = int(stage["resource_receipt"]["effective_rung"][field])
    except (KeyError, TypeError, ValueError) as exc:
        raise BuildResourceEvidenceInvalid(f"resource rung omits {field}") from exc
    return value


def _validate_fresh_probe(
    cas: CASStore,
    reference_value: Any,
    *,
    logical_request_key: str,
    source_content_root: str,
    pit_snapshot_digest: str,
    run_id: str,
    attempt_id: str,
    attempt_fence: int,
    execution_id: str,
    now: datetime,
) -> dict[str, str]:
    reference = cas.verify(reference_value)
    value = cas.get_json_bounded(reference, max_bytes=MAX_STAGE_RESULT_BYTES)
    if not isinstance(value, Mapping):
        raise BuildSourceRevised("fresh source probe is invalid")
    if value.get("schema_version") == ARTIFACT_READY_RECHECK_SCHEMA:
        observed = _parse_utc(value.get("observed_at"))
        valid_until = _parse_utc(value.get("valid_until"))
        try:
            contract_ref = CASRef.from_value(value.get("artifact_ready_contract_ref"))
        except CASStoreError as exc:
            raise BuildSourceRevised("artifact-ready prepublish contract reference is invalid") from exc
        effective_roots = value.get("effective_component_roots")
        if (
            value.get("status") != "PASS"
            or value.get("artifact_ready_content_root") != source_content_root
            or value.get("pit_snapshot_digest") != pit_snapshot_digest
            or value.get("run_id") != run_id
            or value.get("attempt_id") != attempt_id
            or value.get("attempt_fence") != attempt_fence
            or value.get("execution_id") != execution_id
            or not isinstance(effective_roots, Mapping)
            or set(effective_roots)
            != {
                "daily_bin",
                "minute_bin",
                "factor_h5_static",
                "domestic_index_context",
            }
            or value.get("freshness_authority") != "fresh_db_readback_plus_immutable_provider_overlay_v1"
            or value.get("provider_recheck_policy") != "no_provider_refetch_v1"
            or value.get("safety")
            != {
                "database_writes": 0,
                "provider_database_writes": 0,
                "candidate_writes": 0,
                "production_writes": 0,
                "production_deletes": 0,
                "production_pointer_changes": 0,
                "service_process_controls": 0,
            }
            or not observed <= now < valid_until
        ):
            raise BuildSourceRevised("artifact-ready prepublish source recheck differs")
        expected_key = digest_named_fields(
            ARTIFACT_READY_RECHECK_SCHEMA,
            {
                "artifact_ready_contract_ref": contract_ref.sha256,
                "artifact_ready_content_root": source_content_root,
                "initial_source_content_root": value.get("initial_source_content_root"),
                "fresh_source_content_root": value.get("fresh_source_content_root"),
                "pit_snapshot_digest": pit_snapshot_digest,
                "effective_component_roots": dict(effective_roots),
                "execution_id": execution_id,
                "run_id": run_id,
                "attempt_id": attempt_id,
                "attempt_fence": attempt_fence,
                "observed_at": observed,
            },
        )
        if value.get("source_probe_key") != expected_key:
            raise BuildSourceRevised("artifact-ready prepublish source-probe identity differs")
        return {
            "source_probe_key": expected_key,
            "source_probe_ref": reference.sha256,
            "observed_at": value["observed_at"],
            "valid_until": value["valid_until"],
        }
    if value.get("schema_version") != SOURCE_PROBE_SCHEMA_VERSION:
        raise BuildSourceRevised("fresh source probe schema is invalid")
    subject = new_build_probe_subject(logical_request_key)
    expected = {
        "logical_request_key": logical_request_key,
        "source_content_root": source_content_root,
        "pit_snapshot_digest": pit_snapshot_digest,
        "subject_kind": SourceProbeSubjectKind.NEW_BUILD.value,
        "subject_identity": subject,
        "candidate_identity": None,
        "artifact_root": None,
    }
    if any(value.get(field) != expected_value for field, expected_value in expected.items()):
        raise BuildSourceRevised("source/PIT changed before publish")
    observed = _parse_utc(value.get("observed_at"))
    valid_until = _parse_utc(value.get("valid_until"))
    if not observed <= now < valid_until:
        raise BuildSourceRevised("fresh source probe expired before publish")
    tokens = value.get("snapshot_tokens")
    if not isinstance(tokens, list) or not tokens or len(tokens) != len(set(tokens)):
        raise BuildSourceRevised("fresh source probe snapshot tokens are invalid")
    semantic_body = {
        key: value[key]
        for key in (
            "schema_version",
            "probe_policy_version",
            "subject_kind",
            "subject_identity",
            "candidate_identity",
            "artifact_root",
            "logical_request_key",
            "source_content_root",
            "source_provenance_root",
            "pit_snapshot_digest",
            "snapshot_tokens",
            "probe_ordinal",
            "observed_at",
            "valid_until",
        )
    }
    receipt_digest = digest_named_fields(SOURCE_PROBE_SCHEMA_VERSION, semantic_body)
    if value.get("receipt_digest") != receipt_digest:
        raise BuildSourceRevised("fresh source probe receipt digest differs")
    source_probe_key = SourceProbeIdentity(
        logical_request_key=logical_request_key,
        candidate_identity=None,
        artifact_root=None,
        source_content_root=source_content_root,
        source_provenance_root=ensure_sha256(
            str(value.get("source_provenance_root", "")), field="source_provenance_root"
        ),
        pit_digest=pit_snapshot_digest,
        probe_policy_version=str(value.get("probe_policy_version", "")),
        probe_receipt_digest=receipt_digest,
        subject_kind=SourceProbeSubjectKind.NEW_BUILD,
        subject_identity=subject,
    ).key
    if value.get("source_probe_key") != source_probe_key:
        raise BuildSourceRevised("fresh source probe key differs")
    if dict(value.get("safety") or {}) != _SOURCE_PROBE_ZERO_SAFETY:
        raise BuildSourceRevised("fresh source probe safety receipt differs")
    return {
        "source_probe_key": source_probe_key,
        "source_probe_ref": reference.sha256,
        "observed_at": _timestamp(observed),
        "valid_until": _timestamp(valid_until),
    }


def _portable_child_receipt(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        payload = dict(value)
    elif is_dataclass(value):
        payload = asdict(value)
    elif hasattr(value, "as_dict"):
        payload = dict(value.as_dict())
    else:
        raise BuildStageFailed("supervised child receipt is not typed")
    segments = []
    for item in payload.get("log_segments") or []:
        if not isinstance(item, Mapping):
            raise BuildStageFailed("supervised log segment receipt is invalid")
        segments.append(
            {
                field: item[field]
                for field in (
                    "stream",
                    "generation",
                    "size_bytes",
                    "sha256",
                    "cas_ref",
                )
                if field in item
            }
        )
    payload["log_segments"] = segments
    payload.pop("result_path", None)
    payload.pop("log_root", None)
    return payload


def _read_json_bounded(path: Path, *, max_bytes: int) -> Mapping[str, Any]:
    resolved = Path(path).resolve(strict=True)
    _assert_plain(resolved)
    if not resolved.is_file() or resolved.stat().st_size > max_bytes:
        raise BuildStageFailed("stage semantic result is missing or oversized")
    with resolved.open("rb") as handle:
        raw = handle.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise BuildStageFailed("stage semantic result exceeds bounded read")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BuildStageFailed("stage semantic result is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise BuildStageFailed("stage semantic result is not an object")
    return value


def _plain_existing_root(path: Path) -> Path:
    resolved = Path(path).expanduser().resolve(strict=True)
    if not resolved.is_dir():
        raise BuildProcessorError("candidate root is not a directory")
    current = Path(resolved.anchor)
    if current.exists():
        _assert_plain(current)
    for part in resolved.parts[1:]:
        current = current / part
        _assert_plain(current)
    return resolved


def _contained_future_path(root: Path, relative: str) -> Path:
    normalized = str(relative).replace("\\", "/")
    pure = PurePosixPath(normalized)
    if (
        pure.is_absolute()
        or not pure.parts
        or any(part in {"", ".", ".."} for part in pure.parts)
        or any(character in normalized for character in ("*", "?", "[", "]", ":"))
    ):
        raise BuildProcessorError("candidate relative path is invalid")
    path = (root / Path(*pure.parts)).resolve(strict=False)
    if root not in path.parents:
        raise BuildProcessorError("candidate path escapes allowlisted root")
    return path


def _path_identity_text(value: str) -> str:
    return str(Path(value).resolve(strict=False)).replace("\\", "/").casefold()


def _assert_plain(path: Path) -> None:
    try:
        value = os.lstat(path)
    except OSError as exc:
        raise BuildProcessorError(f"build path is unavailable: {path}") from exc
    if stat.S_ISLNK(value.st_mode) or (int(getattr(value, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise BuildProcessorError(f"build path traverses symlink/reparse: {path}")


def _windows_to_wsl(path: Path) -> str:
    text = str(path.resolve(strict=False)).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    if not text.startswith("/"):
        raise BuildProcessorError("cannot map build path into WSL")
    return text


def _parse_utc(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise BuildProcessorError("build timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BuildProcessorError("build timestamp is timezone-naive")
    return parsed.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


__all__ = [
    "BUILD_RECEIPT_SCHEMA",
    "BUILD_RESOURCE_RECEIPT_SCHEMA",
    "BUILD_STAGE_RESULT_SCHEMA",
    "BuildProcessorError",
    "BuildResourceEvidenceInvalid",
    "BuildSourceRevised",
    "BuildStageCommandFactory",
    "BuildStageFailed",
    "BuildStageLayout",
    "DefaultBuildStageCommandFactory",
    "ProductionBuildProcessor",
]
