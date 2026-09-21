"""Drive the mature provider-free materializers for one monthly candidate."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from .build_stage import BuildStageInvocation, run_build_stage
from .canonical import digest_named_fields
from .cas_store import CASRef, CASStore
from .monthly_build_bridge import CompiledMonthlyBuild
from .monthly_build_executor import PhysicalBuildResult
from .monthly_worker import ProducerContext
from .profile import DatasetProfile


class MonthlyMatureBuildError(RuntimeError):
    """The mature provider-free BUILD stages returned inconsistent evidence."""


class MonthlyQlibWriter(Protocol):
    def execute(
        self,
        *,
        context: ProducerContext,
        staging_root: Path,
        operation: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


class MonthlyConsumerSmoke(Protocol):
    def execute(
        self,
        *,
        context: ProducerContext,
        staging_root: Path,
        prepare_result: Mapping[str, Any],
        release_digest: str,
    ) -> Mapping[str, Any]: ...


class MonthlyCandidateFinalizer(Protocol):
    def execute(
        self,
        *,
        context: ProducerContext,
        staging_root: Path,
        compiled: CompiledMonthlyBuild,
        validation_result: Mapping[str, Any],
        stage_refs: Mapping[str, CASRef],
    ) -> PhysicalBuildResult: ...


@dataclass(frozen=True, slots=True)
class MatureMonthlyPhysicalBuildRunner:
    """Run prepare, Qlib writers, finalize, smoke and validation in order."""

    profile: DatasetProfile
    cas: CASStore
    project_root: Path
    qlib_writer: MonthlyQlibWriter
    consumer_smoke: MonthlyConsumerSmoke
    finalizer: MonthlyCandidateFinalizer

    def __post_init__(self) -> None:
        project = self.project_root.resolve(strict=True)
        if self.project_root.is_symlink() or not project.is_dir():
            raise MonthlyMatureBuildError("monthly build project root is unavailable")
        configured_root = Path(self.profile.candidate_root).resolve(strict=True)
        if configured_root.is_symlink() or not configured_root.is_dir():
            raise MonthlyMatureBuildError("monthly build candidate root is unavailable")

    def execute(
        self,
        *,
        context: ProducerContext,
        staging_root: Path,
        compiled: CompiledMonthlyBuild,
    ) -> PhysicalBuildResult:
        release_id = str(context.plan.get("release_id") or "")
        if not release_id:
            raise MonthlyMatureBuildError("monthly release id is missing")
        if staging_root.parent.resolve(strict=True) != Path(
            self.profile.candidate_root
        ).resolve(strict=True):
            raise MonthlyMatureBuildError("monthly staging root differs from profile")
        release_digest = digest_named_fields(
            "aistock_monthly_physical_release_v1",
            {
                "release_id": release_id,
                "target_cutoff": context.plan.get("target_cutoff"),
                "source_bundle_sha256": compiled.source_bundle_sha256,
                "action_plan_digest": compiled.physical_plan.get("action_plan_digest"),
            },
        )
        common = {
            "run_id": context.operation_id,
            "attempt_id": f"{context.operation_id}-attempt-{context.attempt}",
            "attempt_fence": context.attempt,
            "pressure_rung": 0,
            "stage_timeout_seconds": self.profile.stage_timeouts_seconds["full_build"],
            "release_id": release_id,
            "release_digest": release_digest,
            "staging_relative_path": staging_root.name,
            "project_root": self.project_root.resolve(strict=True),
            "candidate_root": staging_root.parent.resolve(strict=True),
            "staging_root": staging_root,
            "profile": self.profile,
            "cas": self.cas,
            "plan": compiled.physical_plan,
        }

        prepare = run_build_stage(
            BuildStageInvocation(stage="prepare", prerequisites={}, **common)
        )
        prepare_ref = self._seal_stage(prepare, expected="prepare")
        operations = prepare.get("qlib_dump_operations")
        if not isinstance(operations, list) or any(
            not isinstance(item, Mapping) for item in operations
        ):
            raise MonthlyMatureBuildError("prepare Qlib operation set is invalid")
        dump_refs: dict[str, CASRef] = {}
        for raw in operations:
            operation = dict(raw)
            operation_id = str(operation.get("operation_id") or "")
            if not operation_id or operation_id in dump_refs:
                raise MonthlyMatureBuildError("Qlib operation identity is empty or duplicated")
            receipt = self.qlib_writer.execute(
                context=context,
                staging_root=staging_root,
                operation=operation,
            )
            dump_refs[operation_id] = self.cas.verify(self.cas.put_json(dict(receipt)))

        finalize_prerequisites = {
            "prepare": prepare_ref.sha256,
            **{
                f"qlib_dump_{name}": reference.sha256
                for name, reference in sorted(dump_refs.items())
            },
        }
        finalized = run_build_stage(
            BuildStageInvocation(
                stage="finalize-bins",
                prerequisites=finalize_prerequisites,
                **common,
            )
        )
        finalized_ref = self._seal_stage(finalized, expected="finalize-bins")
        smoke = self.consumer_smoke.execute(
            context=context,
            staging_root=staging_root,
            prepare_result=prepare,
            release_digest=release_digest,
        )
        smoke_ref = self.cas.verify(self.cas.put_json(dict(smoke)))
        validation_prerequisites = {
            **finalize_prerequisites,
            "finalize_bins": finalized_ref.sha256,
            "consumer_smoke": smoke_ref.sha256,
        }
        validated = run_build_stage(
            BuildStageInvocation(
                stage="validate",
                prerequisites=validation_prerequisites,
                **common,
            )
        )
        validated_ref = self._seal_stage(validated, expected="validate")
        return self.finalizer.execute(
            context=context,
            staging_root=staging_root,
            compiled=compiled,
            validation_result=validated,
            stage_refs={
                "prepare": prepare_ref,
                **{
                    f"qlib_dump_{name}": reference
                    for name, reference in sorted(dump_refs.items())
                },
                "finalize_bins": finalized_ref,
                "consumer_smoke": smoke_ref,
                "validate": validated_ref,
            },
        )

    def _seal_stage(self, value: Mapping[str, Any], *, expected: str) -> CASRef:
        if (
            value.get("schema_version") != "dataset_release_build_stage_result_v1"
            or value.get("stage") != expected
            or value.get("status") != "PASS"
        ):
            raise MonthlyMatureBuildError(f"mature build stage failed: {expected}")
        return self.cas.verify(self.cas.put_json(dict(value)))


__all__: Sequence[str] = (
    "MatureMonthlyPhysicalBuildRunner",
    "MonthlyCandidateFinalizer",
    "MonthlyConsumerSmoke",
    "MonthlyMatureBuildError",
    "MonthlyQlibWriter",
)
