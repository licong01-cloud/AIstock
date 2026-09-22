"""Production executor composition for the unified monthly dataset worker."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
from typing import Mapping, Sequence

from .canonical import canonical_json_bytes
from .cas_store import CASStore
from .control_store import ControlStore
from .index_sources import independent_postgres_connection_factory
from .monthly_build_executor import SealedMonthlyBuildExecutor
from .monthly_candidate_finalizer import UnifiedMonthlyCandidateFinalizer
from .monthly_hmm_derive import (
    FrozenHMMCoefficientAuthority,
    HMMCoefficientProduct,
    MonthlyHMMCoefficientExecutor,
    WSLPythonHMMCoefficientProcess,
)
from .monthly_local_validation import MonthlyCandidateLocalValidationExecutor
from .monthly_mature_build_runner import MatureMonthlyPhysicalBuildRunner
from .monthly_postgres_source import PostgresMonthlySourceAdapter
from .monthly_profile_candidate import SealedMonthlyProfileCandidateBuilder
from .monthly_registry import OfficialMonthlyProducerRegistry
from .monthly_runtime import MonthlyRuntimeConfigurationError, MonthlyRuntimeSettings
from .monthly_shared_components import FrozenMonthlySharedComponentBuilder
from .monthly_source_producer import AuditedMonthlySourceProducer
from .monthly_supervised_scope import ResourceSupervisedMonthlyBuildScopeFactory
from .monthly_worker_composition import (
    MonthlyWorkerExecutors,
    build_monthly_worker_registry,
)
from .monthly_worker_nodes import MonthlyNodeRuntimeSettings
from .profile import CANONICAL_PROFILE_ID, DatasetProfile, load_dataset_profile
from .resource_supervisor import ResourceSupervisor


MONTHLY_HMM_AUTHORITY_SCHEMA = "aistock_monthly_hmm_coefficient_authority_v1"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_AUTHORITY_FIELDS = {
    "schema_version",
    "authority_id",
    "model",
    "config",
    "producer_script_sha256",
    "products",
}
_FILE_FIELDS = {"path", "sha256", "size"}
_PRODUCT_FIELDS = {
    "asset_id",
    "preset_key",
    "preset_coefficients",
    "test_start",
    "backtest_lag_trade_days",
}


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(
        int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _plain_file(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise MonthlyRuntimeConfigurationError(f"{label} must be absolute")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if _is_link(current):
            raise MonthlyRuntimeConfigurationError(f"{label} path chain must not be linked")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise MonthlyRuntimeConfigurationError(f"{label} is unavailable") from exc
    if not resolved.is_file():
        raise MonthlyRuntimeConfigurationError(f"{label} must be a plain file")
    return resolved


def _authority_file(raw: object, *, label: str) -> tuple[Path, str]:
    if not isinstance(raw, Mapping) or set(raw) != _FILE_FIELDS:
        raise MonthlyRuntimeConfigurationError(f"{label} authority fields differ")
    path = _plain_file(Path(str(raw["path"] or "")), label=label)
    digest = str(raw["sha256"] or "")
    size = raw["size"]
    if (
        re.fullmatch(r"[0-9a-f]{64}", digest) is None
        or type(size) is not int
        or size <= 0
        or path.stat().st_size != size
        or _sha256(path) != digest
    ):
        raise MonthlyRuntimeConfigurationError(f"{label} authority bytes differ")
    return path, digest


def load_monthly_hmm_authority(
    path: Path,
    *,
    producer_script: Path,
) -> FrozenHMMCoefficientAuthority:
    authority_path = _plain_file(path, label="monthly HMM authority")
    raw = authority_path.read_bytes()
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyRuntimeConfigurationError("monthly HMM authority is invalid JSON") from exc
    if (
        not isinstance(value, Mapping)
        or set(value) != _AUTHORITY_FIELDS
        or value.get("schema_version") != MONTHLY_HMM_AUTHORITY_SCHEMA
        or raw != canonical_json_bytes(value) + b"\n"
    ):
        raise MonthlyRuntimeConfigurationError("monthly HMM authority contract differs")
    model_path, model_sha = _authority_file(value["model"], label="frozen HMM model")
    config_path, config_sha = _authority_file(value["config"], label="frozen HMM config")
    script = _plain_file(producer_script, label="HMM coefficient producer")
    script_sha = str(value["producer_script_sha256"] or "")
    if _sha256(script) != script_sha:
        raise MonthlyRuntimeConfigurationError("HMM coefficient producer hash differs")
    rows = value["products"]
    if not isinstance(rows, list) or not rows:
        raise MonthlyRuntimeConfigurationError("monthly HMM products are empty")
    products: list[HMMCoefficientProduct] = []
    for raw_product in rows:
        if not isinstance(raw_product, Mapping) or set(raw_product) != _PRODUCT_FIELDS:
            raise MonthlyRuntimeConfigurationError("monthly HMM product fields differ")
        coefficients = raw_product["preset_coefficients"]
        if not isinstance(coefficients, Mapping) or any(
            isinstance(item, bool) or not isinstance(item, (int, float))
            for item in coefficients.values()
        ):
            raise MonthlyRuntimeConfigurationError("monthly HMM coefficients are invalid")
        try:
            test_start = date.fromisoformat(str(raw_product["test_start"]))
            product = HMMCoefficientProduct(
                asset_id=str(raw_product["asset_id"]),
                preset_key=str(raw_product["preset_key"]),
                preset_coefficients={str(key): float(item) for key, item in coefficients.items()},
                test_start=test_start,
                backtest_lag_trade_days=raw_product["backtest_lag_trade_days"],
            )
        except (TypeError, ValueError) as exc:
            raise MonthlyRuntimeConfigurationError("monthly HMM product is invalid") from exc
        products.append(product)
    try:
        return FrozenHMMCoefficientAuthority(
            authority_id=str(value["authority_id"]),
            model_path=model_path,
            model_sha256=model_sha,
            config_path=config_path,
            config_sha256=config_sha,
            script_sha256=script_sha,
            products=tuple(products),
        )
    except ValueError as exc:
        raise MonthlyRuntimeConfigurationError(
            "monthly HMM authority identity is invalid"
        ) from exc


@dataclass(frozen=True, slots=True)
class MonthlyProductionSettings:
    project_root: Path
    profile_path: Path
    hmm_authority_path: Path
    sector_membership_start: date = date(2024, 7, 1)

    def __post_init__(self) -> None:
        project = self.project_root.resolve(strict=True)
        if _is_link(self.project_root) or not project.is_dir():
            raise MonthlyRuntimeConfigurationError("monthly project root is unavailable")
        _plain_file(self.profile_path, label="monthly dataset profile")
        authority = _plain_file(
            self.hmm_authority_path,
            label="monthly HMM authority",
        )
        if authority.is_relative_to(project):
            raise MonthlyRuntimeConfigurationError(
                "monthly HMM authority must be repository-external"
            )

    @classmethod
    def from_env(cls, *, project_root: Path) -> "MonthlyProductionSettings":
        authority = str(os.getenv("AISTOCK_MONTHLY_HMM_AUTHORITY_PATH") or "").strip()
        if not authority:
            raise MonthlyRuntimeConfigurationError(
                "monthly production environment is incomplete",
                context={"missing": ["AISTOCK_MONTHLY_HMM_AUTHORITY_PATH"]},
            )
        return cls(
            project_root=project_root.resolve(strict=True),
            profile_path=(
                project_root / "configs" / "datasets" / "qe_backtest_monthly_v2.yaml"
            ).resolve(strict=True),
            hmm_authority_path=Path(authority),
        )


def _same_windows_path(left: object, right: Path) -> bool:
    return str(left).replace("/", "\\").casefold() == str(right).replace(
        "/", "\\"
    ).casefold()


def _supervisor_factory(
    *,
    profile: DatasetProfile,
    artifact_root: Path,
):
    def build(context):  # type: ignore[no-untyped-def]
        return ResourceSupervisor(
            attempt_id=f"{context.operation_id}-build",
            fence=context.attempt,
            control_root=artifact_root,
            policy=profile.resource_policy,
            hybrid_wsl=True,
        )

    return build


def build_monthly_production_registry(
    *,
    runtime: MonthlyRuntimeSettings,
    nodes: MonthlyNodeRuntimeSettings,
    production: MonthlyProductionSettings,
) -> OfficialMonthlyProducerRegistry:
    """Build the complete official registry without opening data sources."""

    profile = load_dataset_profile(production.profile_path)
    if profile.profile != CANONICAL_PROFILE_ID:
        raise MonthlyRuntimeConfigurationError("monthly production profile is not canonical v2")
    if not _same_windows_path(profile.candidate_root, runtime.controller_release_root):
        raise MonthlyRuntimeConfigurationError(
            "monthly controller release root differs from canonical profile"
        )
    if not _same_windows_path(profile.control_root, runtime.artifact_root):
        raise MonthlyRuntimeConfigurationError(
            "monthly artifact root differs from canonical profile control root"
        )
    artifact = runtime.artifact_root.resolve(strict=True)
    cas = CASStore(artifact)
    source_catalog = ControlStore(artifact)
    source_adapter = PostgresMonthlySourceAdapter(
        profile=profile,
        cas=cas,
        artifact_root=artifact,
        source_catalog=source_catalog,
    )
    source = AuditedMonthlySourceProducer(
        producer_id="aistock.monthly.source.postgres_snapshot",
        producer_version="1",
        artifact_root=artifact,
        connection_factory=independent_postgres_connection_factory,
        adapter=source_adapter,
    )
    toolchain = profile.qlib_toolchain.build_verified(production.project_root)
    shared = FrozenMonthlySharedComponentBuilder(
        profile=profile,
        cas=cas,
        sector_membership_start=production.sector_membership_start,
    )
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=profile,
        cas=cas,
        project_root=production.project_root,
        finalizer=UnifiedMonthlyCandidateFinalizer(shared_components=shared),
        execution_scope_factory=ResourceSupervisedMonthlyBuildScopeFactory(
            profile=profile,
            project_root=production.project_root,
            toolchain=toolchain,
            supervisor_factory=_supervisor_factory(
                profile=profile,
                artifact_root=artifact,
            ),
        ),
    )
    build = SealedMonthlyBuildExecutor(
        profile=profile,
        cas=cas,
        artifact_roots=(artifact,),
        controller_release_root=runtime.controller_release_root,
        runner=runner,
    )
    script = production.project_root / "scripts" / "precompute_hmm_coefficients.py"
    authority = load_monthly_hmm_authority(
        production.hmm_authority_path,
        producer_script=script,
    )
    wsl = shutil.which("wsl.exe")
    if not wsl:
        raise MonthlyRuntimeConfigurationError("WSL executable is unavailable")
    derive = MonthlyHMMCoefficientExecutor(
        authority=authority,
        process=WSLPythonHMMCoefficientProcess(
            wsl_executable=Path(wsl).resolve(strict=True),
            distribution=nodes.wsl_distro,
            python_executable=nodes.wsl_python,
            script_path=script.resolve(strict=True),
            project_root=production.project_root,
        ),
        artifact_root=artifact,
    )
    return build_monthly_worker_registry(
        runtime=runtime,
        nodes=nodes,
        executors=MonthlyWorkerExecutors(
            source=source,
            build=build,
            derive=derive,
            local_validate=MonthlyCandidateLocalValidationExecutor(),
            profile_builder=SealedMonthlyProfileCandidateBuilder(),
        ),
    )


__all__: Sequence[str] = (
    "MONTHLY_HMM_AUTHORITY_SCHEMA",
    "MonthlyProductionSettings",
    "build_monthly_production_registry",
    "load_monthly_hmm_authority",
)
