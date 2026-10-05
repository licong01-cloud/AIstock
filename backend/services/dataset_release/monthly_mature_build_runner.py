"""Drive the mature provider-free materializers for one monthly candidate."""

from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import date
import hashlib
from pathlib import Path
import stat
import time
from typing import Any, Mapping, Protocol, Sequence

from .build_stage import BuildStageInvocation, run_build_stage
from .canonical import digest_named_fields, ensure_sha256, normalize_root_relative_path
from .cas_store import CASRef, CASStore
from .errors import CanonicalizationError
from .monthly_build_bridge import CompiledMonthlyBuild, load_monthly_predecessor_prefix
from .monthly_legacy_prefix import append_legacy_qlib_month, is_monthly_provider_instrument
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
    ) -> "MonthlyConsumerSmokeResult": ...


@dataclass(frozen=True, slots=True)
class MonthlyConsumerSmokeResult:
    semantic_receipt: Mapping[str, Any]
    resource_receipt: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.semantic_receipt or not self.resource_receipt:
            raise MonthlyMatureBuildError("consumer smoke evidence is incomplete")


@dataclass(frozen=True, slots=True)
class MonthlyBuildExecutionTools:
    """Attempt-bound data-plane capabilities used by one physical BUILD.

    The tools are intentionally created after ``ProducerContext`` exists.  A
    resource supervisor carries an attempt/fence identity and therefore must
    never be retained on a process-wide producer registry or reused by a
    resumed monthly operation.
    """

    qlib_writer: MonthlyQlibWriter
    consumer_smoke: MonthlyConsumerSmoke


class MonthlyBuildExecutionScopeFactory(Protocol):
    def __call__(self, context: ProducerContext) -> AbstractContextManager[MonthlyBuildExecutionTools]: ...


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
    finalizer: MonthlyCandidateFinalizer
    qlib_writer: MonthlyQlibWriter | None = None
    consumer_smoke: MonthlyConsumerSmoke | None = None
    execution_scope_factory: MonthlyBuildExecutionScopeFactory | None = None

    def __post_init__(self) -> None:
        project = self.project_root.resolve(strict=True)
        if self.project_root.is_symlink() or not project.is_dir():
            raise MonthlyMatureBuildError("monthly build project root is unavailable")
        configured_root = Path(self.profile.candidate_root).resolve(strict=True)
        if configured_root.is_symlink() or not configured_root.is_dir():
            raise MonthlyMatureBuildError("monthly build candidate root is unavailable")
        has_legacy_tools = self.qlib_writer is not None and self.consumer_smoke is not None
        if (self.qlib_writer is None) != (self.consumer_smoke is None):
            raise MonthlyMatureBuildError("monthly build tools must be supplied as one pair")
        if (self.execution_scope_factory is None) != has_legacy_tools:
            raise MonthlyMatureBuildError("monthly build requires exactly one attempt-scoped or legacy tool source")

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
        candidate_root = Path(self.profile.candidate_root).resolve(strict=True)
        if staging_root.parent.name != ".staging" or staging_root.parent.parent.resolve(strict=True) != candidate_root:
            raise MonthlyMatureBuildError("monthly staging root differs from profile")
        staging_relative_path = staging_root.relative_to(candidate_root).as_posix()
        release_digest = str(compiled.physical_plan.get("release_digest") or "")
        expected_release_digest = digest_named_fields(
            "aistock_monthly_physical_release_v1",
            {
                "release_id": release_id,
                "target_cutoff": context.plan.get("target_cutoff"),
                "source_bundle_sha256": compiled.source_bundle_sha256,
                "action_plan_digest": compiled.physical_plan.get("action_plan_digest"),
            },
        )
        if release_digest != expected_release_digest:
            raise MonthlyMatureBuildError("monthly physical release digest differs")
        physical_plan = dict(compiled.physical_plan)
        predecessor = context.plan.get("predecessor")
        if isinstance(predecessor, Mapping):
            # Only the durable controller plan supplies this lineage, never a
            # path from an API caller or a partial preparation SOURCE receipt.
            physical_plan["preparation_predecessor_profile_sha256"] = predecessor.get("profile_sha256")
        common = {
            "run_id": context.operation_id,
            "attempt_id": f"{context.operation_id}-attempt-{context.attempt}",
            "attempt_fence": context.attempt,
            "pressure_rung": 0,
            "stage_timeout_seconds": self.profile.stage_timeouts_seconds["full_build"],
            "release_id": release_id,
            "release_digest": release_digest,
            "staging_relative_path": staging_relative_path,
            "project_root": self.project_root.resolve(strict=True),
            "candidate_root": candidate_root,
            "staging_root": staging_root,
            "profile": self.profile,
            "cas": self.cas,
            "plan": physical_plan,
        }

        if self.execution_scope_factory is not None:
            scope = self.execution_scope_factory(context)
        else:
            if self.qlib_writer is None or self.consumer_smoke is None:  # pragma: no cover
                raise MonthlyMatureBuildError("monthly build tools are unavailable")
            scope = nullcontext(
                MonthlyBuildExecutionTools(
                    qlib_writer=self.qlib_writer,
                    consumer_smoke=self.consumer_smoke,
                )
            )
        with scope as tools:
            prepare = run_build_stage(BuildStageInvocation(stage="prepare", prerequisites={}, **common))
            prepare_ref = self._seal_stage(prepare, expected="prepare")
            operations = prepare.get("qlib_dump_operations")
            if not isinstance(operations, list) or any(not isinstance(item, Mapping) for item in operations):
                raise MonthlyMatureBuildError("prepare Qlib operation set is invalid")
            dump_refs: dict[str, CASRef] = {}
            for raw in operations:
                operation = dict(raw)
                operation_id = str(operation.get("operation_id") or "")
                if not operation_id or operation_id in dump_refs:
                    raise MonthlyMatureBuildError("Qlib operation identity is empty or duplicated")
                if operation.get("mode") == "inherited_month":
                    receipt = self._append_inherited_month(
                        context=context, staging_root=staging_root,
                        compiled=compiled, operation=operation,
                    )
                else:
                    receipt = tools.qlib_writer.execute(
                        context=context,
                        staging_root=staging_root,
                        operation=operation,
                    )
                dump_refs[operation_id] = self.cas.verify(self.cas.put_json(dict(receipt)))

            finalize_prerequisites = {
                "prepare": prepare_ref.sha256,
                **{f"qlib_dump_{name}": reference.sha256 for name, reference in sorted(dump_refs.items())},
            }
            finalized = run_build_stage(
                BuildStageInvocation(
                    stage="finalize-bins",
                    prerequisites=finalize_prerequisites,
                    **common,
                )
            )
            finalized_ref = self._seal_stage(finalized, expected="finalize-bins")
            smoke = tools.consumer_smoke.execute(
                context=context,
                staging_root=staging_root,
                prepare_result=prepare,
                release_digest=release_digest,
            )
        smoke_ref = self.cas.verify(self.cas.put_json(dict(smoke.semantic_receipt)))
        smoke_resource_ref = self.cas.verify(self.cas.put_json(dict(smoke.resource_receipt)))
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
                **{f"qlib_dump_{name}": reference for name, reference in sorted(dump_refs.items())},
                "finalize_bins": finalized_ref,
                "consumer_smoke": smoke_ref,
                "consumer_smoke_resource": smoke_resource_ref,
                "validate": validated_ref,
            },
        )

    def _append_inherited_month(
        self, *, context: ProducerContext, staging_root: Path,
        compiled: CompiledMonthlyBuild, operation: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Run the shared native writer on precisely sealed month inputs.

        No child/process-success receipt is fabricated for an in-process
        writer. The actual native receipt remains unpublished until the common
        finalize, consumer smoke and candidate validation complete.
        """
        if context.stage != "BUILD" or operation.get("mode") != "inherited_month" or set(operation) != {
            "operation_id", "dataset", "mode", "preparation_ref",
        }:
            raise MonthlyMatureBuildError("inherited Qlib operation contract differs")
        dataset = operation["dataset"]
        if dataset not in {"daily_bin", "minute_bin"} or operation["operation_id"] != dataset.removesuffix("_bin"):
            raise MonthlyMatureBuildError("inherited Qlib operation identity differs")
        if not isinstance(operation["preparation_ref"], Mapping) or set(operation["preparation_ref"]) != {"sha256", "size", "relative_path"}:
            raise MonthlyMatureBuildError("inherited Qlib preparation reference differs")
        prepared = self.cas.get_json_bounded(operation["preparation_ref"], max_bytes=16 * 1024 * 1024)
        expected_fields = {
            "schema_version", "dataset", "cutoff", "predecessor_manifest_sha256",
            "source_bundle_sha256", "csv_relative_path", "calendar_ref",
            "instruments_ref", "csv_refs", "qfq_basis_changes",
        }
        if (
            not isinstance(prepared, Mapping) or set(prepared) != expected_fields
            or prepared["schema_version"] != "aistock_monthly_legacy_qlib_operation_v1"
            or prepared["dataset"] != dataset
            or prepared["cutoff"] != context.plan.get("target_cutoff")
            or prepared["source_bundle_sha256"] != compiled.source_bundle_sha256
        ):
            raise MonthlyMatureBuildError("inherited Qlib prepared input binding differs")
        prefix = load_monthly_predecessor_prefix(context_plan=context.plan, profile=self.profile)
        if prepared["predecessor_manifest_sha256"] != prefix.manifest_sha256:
            raise MonthlyMatureBuildError("inherited Qlib predecessor differs")
        root = staging_root.resolve(strict=True)
        refs = prepared["csv_refs"]
        if not isinstance(refs, list) or not refs:
            raise MonthlyMatureBuildError("inherited Qlib month CSV population is empty")
        def relative_path(value: object) -> str:
            if not isinstance(value, str) or any(token in value for token in ("\\", ":", "\x00")):
                raise MonthlyMatureBuildError("inherited Qlib month path is not text")
            try:
                normalized = normalize_root_relative_path(value)
            except CanonicalizationError as exc:
                raise MonthlyMatureBuildError("inherited Qlib month path is invalid") from exc
            if normalized != value.casefold():
                raise MonthlyMatureBuildError("inherited Qlib month path is not normalized")
            # Preserve actual filename spelling on case-sensitive filesystems;
            # use casefold only for duplicate identity comparison.
            return value

        csv_relative = relative_path(prepared["csv_relative_path"])
        csv_root = root / csv_relative
        paths: set[str] = set()
        signatures: dict[Path, tuple[int, int, int, int]] = {}
        instruments: list[str] = []
        last_pulse = time.monotonic()

        def pulse() -> None:
            nonlocal last_pulse
            now = time.monotonic()
            if now - last_pulse >= 2:
                context.checkpoint()
                last_pulse = now

        def checked_file(reference: object) -> Path:
            if not isinstance(reference, Mapping) or set(reference) != {"id", "sha256", "size"}:
                raise MonthlyMatureBuildError("inherited Qlib month file reference differs")
            relative = relative_path(reference["id"])
            if relative.casefold() in paths:
                raise MonthlyMatureBuildError("inherited Qlib month file reference is duplicated")
            paths.add(relative.casefold())
            path = root
            for part in Path(relative).parts:
                path /= part
                metadata = path.lstat()
                if stat.S_ISLNK(metadata.st_mode) or int(getattr(metadata, "st_file_attributes", 0)) & 0x0400:
                    raise MonthlyMatureBuildError("inherited Qlib month path contains a link")
            before = path.stat()
            if not stat.S_ISREG(before.st_mode) or type(reference["size"]) is not int or before.st_size != reference["size"]:
                raise MonthlyMatureBuildError("inherited Qlib month file size differs")
            digest = hashlib.sha256()
            with path.open("rb") as reader:
                while block := reader.read(1024 * 1024):
                    pulse()
                    digest.update(block)
            after = path.stat()
            if (
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                or digest.hexdigest() != ensure_sha256(reference["sha256"], field="month file")
            ):
                raise MonthlyMatureBuildError("inherited Qlib month file bytes differ")
            signatures[path] = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            return path

        context.checkpoint()
        for reference in refs:
            path = checked_file(reference)
            if path.parent != csv_root or path.suffix != ".csv" or not is_monthly_provider_instrument(path.stem, dataset=dataset):
                raise MonthlyMatureBuildError("inherited Qlib CSV escaped its frozen root")
            instruments.append(path.stem)
        if {value.name.casefold() for value in csv_root.glob("*.csv")} != {f"{code}.csv".casefold() for code in instruments}:
            raise MonthlyMatureBuildError("inherited Qlib CSV population differs")
        calendar = checked_file(prepared["calendar_ref"])
        all_instruments = checked_file(prepared["instruments_ref"])
        bases = prepared["qfq_basis_changes"]
        if not isinstance(bases, Mapping):
            raise MonthlyMatureBuildError("inherited Qlib QFQ basis binding is invalid")
        context.progress({"phase": "BUILD_NATIVE_QLIB_APPEND", "dataset": dataset, "instrument_count": len(instruments)})
        def unchanged_inputs() -> None:
            for path, expected in signatures.items():
                actual = path.stat()
                if (actual.st_dev, actual.st_ino, actual.st_size, actual.st_mtime_ns) != expected:
                    raise MonthlyMatureBuildError("inherited Qlib month input changed during append")

        unchanged_inputs()
        receipt = append_legacy_qlib_month(
            prefix, root, dataset=dataset,
            target_cutoff=date.fromisoformat(prepared["cutoff"]),
            csv_root=csv_root, calendar_path=calendar,
            instruments_all_path=all_instruments, instruments=tuple(instruments),
            source_receipt_sha256=compiled.source_bundle_sha256,
            qfq_basis_changes=bases, checkpoint=context.checkpoint, progress=context.progress,
        )
        unchanged_inputs()
        context.progress({"phase": "BUILD_NATIVE_QLIB_APPEND_DONE", "dataset": dataset, "instrument_count": len(instruments)})
        return {**receipt, "preparation_ref": dict(operation["preparation_ref"])}

    def _seal_stage(self, value: Mapping[str, Any], *, expected: str) -> CASRef:
        if (
            value.get("schema_version") != "dataset_release_build_stage_result_v1"
            or value.get("stage") != expected
            or value.get("status") != "PASS"
        ):
            raise MonthlyMatureBuildError(f"mature build stage failed: {expected}")
        return self.cas.verify(self.cas.put_json(dict(value)))


__all__: Sequence[str] = (
    "MonthlyBuildExecutionScopeFactory",
    "MonthlyBuildExecutionTools",
    "MatureMonthlyPhysicalBuildRunner",
    "MonthlyCandidateFinalizer",
    "MonthlyConsumerSmoke",
    "MonthlyConsumerSmokeResult",
    "MonthlyMatureBuildError",
    "MonthlyQlibWriter",
)
