"""Provider-free execution boundary for the unified monthly BUILD stage.

This service owns the transition from a sealed monthly SOURCE receipt to one
create-exclusive candidate root.  The physical runner receives only the
verified build plan; it cannot receive a database session or reopen a provider.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import stat
from typing import Mapping, Protocol, Sequence

from .cas_store import CASStore
from .monthly_build_bridge import CompiledMonthlyBuild, compile_initial_monthly_build
from .monthly_official_adapters import (
    BuildExecution,
    StageWorkload,
    validate_build_execution,
)
from .monthly_unified import COMPONENTS
from .monthly_worker import ProducerContext
from .profile import DatasetProfile


class MonthlyBuildExecutorError(RuntimeError):
    """The sealed monthly build could not be executed without ambiguity."""


@dataclass(frozen=True, slots=True)
class PhysicalBuildResult:
    manifest_path: Path
    component_artifacts: tuple[Path, ...]
    input_artifacts: tuple[Path, ...] = ()
    workload: StageWorkload = StageWorkload()
    database_read_performed: bool = False
    database_write_performed: bool = False


class MonthlyPhysicalBuildRunner(Protocol):
    def execute(
        self,
        *,
        context: ProducerContext,
        staging_root: Path,
        compiled: CompiledMonthlyBuild,
    ) -> PhysicalBuildResult: ...


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(
        int(getattr(metadata, "st_file_attributes", 0)) & 0x0400
    )


def _absolute(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path.expanduser())))


def _require_plain_chain(path: Path, *, stop: Path) -> None:
    requested = _absolute(path)
    boundary = _absolute(stop)
    try:
        relative = requested.relative_to(boundary)
    except ValueError as exc:
        raise MonthlyBuildExecutorError("monthly candidate escaped its release root") from exc
    current = boundary
    if _is_link(current):
        raise MonthlyBuildExecutorError("monthly release root is linked")
    for part in relative.parts:
        current /= part
        if _is_link(current):
            raise MonthlyBuildExecutorError("monthly candidate path contains a link")


def _plain_file_under(root: Path, path: Path, *, label: str) -> Path:
    requested = _absolute(path)
    _require_plain_chain(requested, stop=root)
    try:
        resolved = requested.resolve(strict=True)
        resolved_root = root.resolve(strict=True)
    except OSError as exc:
        raise MonthlyBuildExecutorError(f"{label} is unavailable") from exc
    if not resolved.is_relative_to(resolved_root) or _is_link(requested) or not resolved.is_file():
        raise MonthlyBuildExecutorError(f"{label} must be a regular candidate file")
    return resolved


@dataclass(frozen=True, slots=True)
class SealedMonthlyBuildExecutor:
    """Execute one initial monthly build from the exact SOURCE receipt bytes."""

    profile: DatasetProfile
    cas: CASStore
    artifact_roots: tuple[Path, ...]
    controller_release_root: Path
    runner: MonthlyPhysicalBuildRunner

    def __post_init__(self) -> None:
        release_root = self.controller_release_root.resolve(strict=True)
        if _is_link(self.controller_release_root) or not release_root.is_dir():
            raise MonthlyBuildExecutorError("controller release root is unavailable or linked")
        roots = tuple(root.resolve(strict=True) for root in self.artifact_roots)
        if not roots or len(set(roots)) != len(roots):
            raise MonthlyBuildExecutorError("artifact roots are empty or duplicated")

    def execute(
        self,
        context: ProducerContext,
        *,
        component_actions: Mapping[str, str],
    ) -> BuildExecution:
        if context.stage != "BUILD":
            raise MonthlyBuildExecutorError("monthly build executor received another stage")
        if (
            re.fullmatch(r"dmr_[0-9a-f]{32}", context.operation_id) is None
            or type(context.attempt) is not int
            or context.attempt <= 0
        ):
            raise MonthlyBuildExecutorError("monthly build attempt identity is invalid")
        if set(component_actions) != set(COMPONENTS):
            raise MonthlyBuildExecutorError("monthly build component actions are incomplete")
        source_receipt = context.prior_receipts.get("SOURCE")
        source_scope = source_receipt.get("scope") if isinstance(source_receipt, Mapping) else None
        if (
            not isinstance(source_scope, Mapping)
            or source_scope.get("component_actions") != dict(component_actions)
        ):
            raise MonthlyBuildExecutorError("monthly build actions differ from sealed SOURCE")
        target_raw = context.plan.get("candidate_root")
        if not isinstance(target_raw, str) or not target_raw.strip():
            raise MonthlyBuildExecutorError("monthly candidate root is missing")
        target = _absolute(Path(target_raw))
        release_root = self.controller_release_root.resolve(strict=True)
        if target.parent.resolve(strict=True) != release_root:
            raise MonthlyBuildExecutorError("monthly candidate is not a direct release child")
        _require_plain_chain(target, stop=release_root)
        if target.exists():
            raise MonthlyBuildExecutorError("monthly candidate already exists")
        staging = release_root / (
            f".{target.name}.{context.operation_id}.attempt-{context.attempt}.building"
        )
        _require_plain_chain(staging, stop=release_root)
        if staging.exists():
            raise MonthlyBuildExecutorError("monthly candidate staging already exists")

        compiled = compile_initial_monthly_build(
            context_plan=context.plan,
            source_receipt=source_receipt,
            profile=self.profile,
            cas=self.cas,
            artifact_roots=self.artifact_roots,
        )
        result = self.runner.execute(
            context=context,
            staging_root=staging,
            compiled=compiled,
        )
        if result.database_read_performed or result.database_write_performed:
            raise MonthlyBuildExecutorError("physical BUILD must not access a database")
        try:
            actual_root = staging.resolve(strict=True)
        except OSError as exc:
            raise MonthlyBuildExecutorError("physical BUILD did not create its staging root") from exc
        if _is_link(staging) or not actual_root.is_dir():
            raise MonthlyBuildExecutorError("physical BUILD staging is linked or not a directory")
        manifest = _plain_file_under(actual_root, result.manifest_path, label="dataset manifest")
        components = tuple(
            _plain_file_under(actual_root, path, label="component artifact")
            for path in result.component_artifacts
        )
        if not components or len(set(components)) != len(components):
            raise MonthlyBuildExecutorError("physical BUILD component file set is empty or duplicated")
        extra_inputs = tuple(
            _plain_file_under(actual_root, path, label="physical build input")
            for path in result.input_artifacts
        )
        manifest_relative = manifest.relative_to(actual_root)
        component_relatives = tuple(path.relative_to(actual_root) for path in components)
        input_relatives = tuple(path.relative_to(actual_root) for path in extra_inputs)
        validate_build_execution(
            BuildExecution(
                manifest_path=manifest,
                component_artifacts=components,
                input_artifacts=extra_inputs,
                workload=result.workload,
            ),
            context=context,
        )
        try:
            staging.rename(target)
        except OSError as exc:
            raise MonthlyBuildExecutorError("monthly candidate atomic publish failed") from exc
        return BuildExecution(
            manifest_path=target / manifest_relative,
            component_artifacts=tuple(target / path for path in component_relatives),
            input_artifacts=(
                compiled.source_bundle_path,
                *(target / path for path in input_relatives),
            ),
            workload=result.workload,
        )


__all__: Sequence[str] = (
    "MonthlyBuildExecutorError",
    "MonthlyPhysicalBuildRunner",
    "PhysicalBuildResult",
    "SealedMonthlyBuildExecutor",
)
