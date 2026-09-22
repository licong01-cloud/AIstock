"""Seal one unified monthly candidate and its consumer dataset manifest.

All data-bearing materializers have completed before this boundary.  The
finalizer asks one code-owned shared-component builder for suspend, benchmark,
stock-pool and sector-context sidecars, writes a compact build-evidence
receipt, then snapshots every release file exactly once into the common
``qe_dataset_manifest_v1`` identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import os
from pathlib import Path
import stat
from typing import Any, Mapping, Protocol, Sequence

from .canonical import canonical_json_bytes
from .cas_store import CASRef
from .monthly_build_bridge import CompiledMonthlyBuild
from .monthly_build_executor import PhysicalBuildResult
from .monthly_incremental_baseline import (
    MONTHLY_INCREMENTAL_BASELINE_PATH,
    build_monthly_incremental_baseline,
)
from .factor_materializer import FACTOR_H5_DATASETS
from .monthly_official_adapters import StageWorkload
from .monthly_worker import ProducerContext


MONTHLY_BUILD_EVIDENCE_SCHEMA = "aistock_monthly_candidate_build_evidence_v1"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


class MonthlyCandidateFinalizerError(RuntimeError):
    """The unified candidate could not be sealed without ambiguity."""


@dataclass(frozen=True, slots=True)
class SharedReleaseComponents:
    """Consumer-neutral sidecars produced from the sealed SOURCE snapshot."""

    required_files: tuple[Path, ...]
    qlib_calendar_path: Path
    qlib_instruments_path: Path
    st_pit_manifest: Mapping[str, Any]
    source_contract: Mapping[str, Any]
    source_rows_read: int = 0
    computed_rows: int = 0

    def __post_init__(self) -> None:
        if not self.required_files or len(set(self.required_files)) != len(
            self.required_files
        ):
            raise MonthlyCandidateFinalizerError(
                "shared component file set is empty or duplicated"
            )
        if (
            type(self.source_rows_read) is not int
            or self.source_rows_read < 0
            or type(self.computed_rows) is not int
            or self.computed_rows < 0
        ):
            raise MonthlyCandidateFinalizerError(
                "shared component workload is invalid"
            )


class MonthlySharedComponentBuilder(Protocol):
    def execute(
        self,
        *,
        context: ProducerContext,
        staging_root: Path,
        compiled: CompiledMonthlyBuild,
        validation_result: Mapping[str, Any],
    ) -> SharedReleaseComponents: ...


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(
        int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _plain_file(root: Path, path: Path, *, label: str) -> Path:
    if not Path(path).is_absolute():
        raise MonthlyCandidateFinalizerError(f"{label} path must be absolute")
    requested = Path(path).absolute()
    try:
        relative = requested.relative_to(root)
    except ValueError as exc:
        raise MonthlyCandidateFinalizerError(f"{label} escapes candidate") from exc
    current = root
    if _is_link(current):
        raise MonthlyCandidateFinalizerError("candidate root is linked")
    for part in relative.parts:
        current /= part
        if _is_link(current):
            raise MonthlyCandidateFinalizerError(f"{label} traverses a link")
    try:
        resolved = requested.resolve(strict=True)
    except OSError as exc:
        raise MonthlyCandidateFinalizerError(f"{label} is unavailable") from exc
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise MonthlyCandidateFinalizerError(
            f"{label} must be a regular candidate file"
        )
    return resolved


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_exclusive(path: Path, value: Mapping[str, Any]) -> Path:
    payload = canonical_json_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise MonthlyCandidateFinalizerError(
            f"candidate finalizer target already exists: {path.name}"
        ) from exc
    return path


def _manifest_identity(value: Mapping[str, Any]) -> str:
    unsigned = dict(value)
    unsigned.pop("dataset_manifest_sha256", None)
    return hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()


def _component_name(relative: str) -> str:
    return relative.replace("/", "__")


def _st_pit_files(root: Path, value: Mapping[str, Any]) -> tuple[Path, ...]:
    required = {
        "schema_version",
        "snapshot_id",
        "cutoff_trade_date",
        "universe_key",
        "rule_version",
        "selection_universe",
        "index_membership_sidecars",
    }
    if not required.issubset(value):
        raise MonthlyCandidateFinalizerError("ST-PIT manifest fields are incomplete")
    sidecars = value.get("index_membership_sidecars")
    pools = {"stock_universe", "csi300", "csi500", "csi1000", "star50", "star100"}
    if not isinstance(sidecars, Mapping) or set(sidecars) != pools:
        raise MonthlyCandidateFinalizerError("ST-PIT pool set differs")

    def pinned(raw: object, *, label: str) -> Path:
        if not isinstance(raw, Mapping) or not {"path", "sha256", "size"}.issubset(raw):
            raise MonthlyCandidateFinalizerError(f"{label} pin is incomplete")
        path = _plain_file(root, root / Path(str(raw["path"])), label=label)
        if (
            path.stat().st_size != raw["size"]
            or _sha256(path) != raw["sha256"]
        ):
            raise MonthlyCandidateFinalizerError(f"{label} bytes differ")
        return path

    paths = tuple(
        pinned(sidecars[name], label=f"ST-PIT pool {name}") for name in sorted(pools)
    )
    selection = pinned(value.get("selection_universe"), label="ST-PIT selection universe")
    stock_universe = paths[sorted(pools).index("stock_universe")]
    if selection != stock_universe:
        raise MonthlyCandidateFinalizerError(
            "ST-PIT selection universe differs from stock_universe"
        )
    return paths


def _validate_consumer_layout(root: Path, required_files: Sequence[Path]) -> None:
    required = set(required_files)
    pairs = [
        (
            "daily_bin/qlib/calendars/day.txt",
            "components/daily_bin_candidate/calendars/day.txt",
        ),
        (
            "daily_bin/qlib/instruments/all.txt",
            "components/daily_bin_candidate/instruments/all.txt",
        ),
        (
            "minute_bin/qlib/calendars/1min.txt",
            "components/minute_bin_candidate/calendars/1min.txt",
        ),
        (
            "minute_bin/qlib/instruments/all.txt",
            "components/minute_bin_candidate/instruments/all.txt",
        ),
        (
            "index_context/index_daily.h5",
            "components/index_context/index_daily.h5",
        ),
        *[
            (
                f"factor_bundle/{dataset}.h5",
                f"components/factor_h5_static_candidate_v2/{dataset}.h5",
            )
            for dataset in FACTOR_H5_DATASETS
        ],
        (
            "factor_bundle/static_factors.parquet",
            "components/factor_h5_static_candidate_v2/static_factors.parquet",
        ),
    ]
    expected = {
        "components/daily_bin_candidate/instruments/stock_universe.txt",
        "components/daily_bin_candidate/instruments/benchmark.txt",
        "components/daily_bin_candidate/meta_export.json",
        "components/minute_bin_candidate/meta_export.json",
        "components/factor_h5_static_candidate_v2/meta.json",
        "components/index_context/meta.json",
        "reports/qe_index_pool_coverage_receipt.json",
        "reports/monthly_consumer_layout_receipt.json",
        *(target for _source, target in pairs),
    }
    resolved = {
        relative: _plain_file(root, root / relative, label="consumer release file")
        for relative in sorted(expected)
    }
    if not set(resolved.values()).issubset(required):
        raise MonthlyCandidateFinalizerError(
            "consumer release layout is absent from shared file inventory"
        )
    for source, target in pairs:
        source_path = _plain_file(root, root / source, label="internal release file")
        if not os.path.samefile(source_path, resolved[target]):
            raise MonthlyCandidateFinalizerError(
                "consumer release data is not a hardlink to the validated build"
            )


@dataclass(frozen=True, slots=True)
class UnifiedMonthlyCandidateFinalizer:
    shared_components: MonthlySharedComponentBuilder

    def execute(
        self,
        *,
        context: ProducerContext,
        staging_root: Path,
        compiled: CompiledMonthlyBuild,
        validation_result: Mapping[str, Any],
        stage_refs: Mapping[str, CASRef],
    ) -> PhysicalBuildResult:
        if context.stage != "BUILD":
            raise MonthlyCandidateFinalizerError(
                "candidate finalizer received another stage"
            )
        try:
            root = staging_root.resolve(strict=True)
        except OSError as exc:
            raise MonthlyCandidateFinalizerError(
                "candidate staging root is unavailable"
            ) from exc
        if _is_link(staging_root) or not root.is_dir():
            raise MonthlyCandidateFinalizerError(
                "candidate staging root is linked or invalid"
            )
        manifest_path = root / "qe_dataset_manifest.json"
        if manifest_path.exists():
            raise MonthlyCandidateFinalizerError(
                "candidate manifest already exists before finalization"
            )
        if (
            validation_result.get("validation_status") != "PASS"
            or validation_result.get("required_validation_failures") != 0
        ):
            raise MonthlyCandidateFinalizerError(
                "physical candidate validation did not pass"
            )
        required_stage_refs = {
            "prepare",
            "finalize_bins",
            "consumer_smoke",
            "consumer_smoke_resource",
            "validate",
        }
        if not required_stage_refs.issubset(stage_refs):
            raise MonthlyCandidateFinalizerError("candidate stage evidence is incomplete")
        sidecars = self.shared_components.execute(
            context=context,
            staging_root=root,
            compiled=compiled,
            validation_result=validation_result,
        )
        required_files = tuple(
            _plain_file(root, path, label="shared component")
            for path in sidecars.required_files
        )
        _validate_consumer_layout(root, required_files)
        calendar = _plain_file(
            root, sidecars.qlib_calendar_path, label="Qlib day calendar"
        )
        instruments = _plain_file(
            root, sidecars.qlib_instruments_path, label="Qlib provider catalog"
        )
        if calendar not in required_files or instruments not in required_files:
            raise MonthlyCandidateFinalizerError(
                "Qlib identities are absent from the shared release file set"
            )
        st_pit = dict(sidecars.st_pit_manifest)
        if (
            st_pit.get("schema_version") != "qe_st_pit_manifest_v1"
            or st_pit.get("cutoff_trade_date") != context.plan.get("target_cutoff")
            or not str(st_pit.get("snapshot_id") or "").strip()
        ):
            raise MonthlyCandidateFinalizerError("ST-PIT manifest identity differs")
        st_pit_files = _st_pit_files(root, st_pit)
        if not set(st_pit_files).issubset(required_files):
            raise MonthlyCandidateFinalizerError(
                "ST-PIT files are absent from the shared release file set"
            )
        source_contract = dict(sidecars.source_contract)
        if source_contract.get("no_fabrication") is not True:
            raise MonthlyCandidateFinalizerError(
                "monthly source contract must fail closed against fabrication"
            )
        baseline_authority = build_monthly_incremental_baseline(
            release_id=str(context.plan.get("release_id") or ""),
            release_digest=str(compiled.physical_plan.get("release_digest") or ""),
            profile=str(compiled.physical_plan.get("build_inputs", {}).get("profile") or ""),
            scope=str(compiled.physical_plan.get("build_inputs", {}).get("scope") or ""),
            cutoff=str(context.plan.get("target_cutoff") or ""),
            candidate_root_name=Path(str(context.plan.get("candidate_root") or "")).name,
            component_artifact_manifest_ref=validation_result.get(
                "component_artifact_manifest_ref"
            ),
            validation_ref=validation_result.get("validation_ref"),
            source_stage_receipt_ref=compiled.source_stage_receipt_ref.as_dict(),
            source_bundle_sha256=compiled.source_bundle_sha256,
        )
        baseline_path = _write_exclusive(
            root / MONTHLY_INCREMENTAL_BASELINE_PATH,
            baseline_authority,
        )
        evidence_path = _write_exclusive(
            root / "reports" / "monthly_candidate_build_evidence.json",
            {
                "schema_version": MONTHLY_BUILD_EVIDENCE_SCHEMA,
                "operation_id": context.operation_id,
                "attempt": context.attempt,
                "release_id": context.plan.get("release_id"),
                "target_cutoff": context.plan.get("target_cutoff"),
                "source_bundle_sha256": compiled.source_bundle_sha256,
                "source_stage_receipt_ref": compiled.source_stage_receipt_ref.as_dict(),
                "stage_refs": {
                    name: reference.as_dict()
                    for name, reference in sorted(stage_refs.items())
                },
                "validation_ref": validation_result.get("validation_ref"),
                "component_artifact_manifest_ref": validation_result.get(
                    "component_artifact_manifest_ref"
                ),
                "incremental_baseline_authority": {
                    "path": baseline_path.relative_to(root).as_posix(),
                    "sha256": _sha256(baseline_path),
                    "size": baseline_path.stat().st_size,
                    "baseline_authority_sha256": baseline_authority[
                        "baseline_authority_sha256"
                    ],
                },
                "database_read_performed": False,
                "database_write_performed": False,
                "runtime_action_performed": False,
            },
        )

        all_files = self._snapshot_release_files(root)
        if evidence_path.resolve(strict=True) not in all_files:
            raise MonthlyCandidateFinalizerError(
                "build evidence is absent from release inventory"
            )
        if not set(required_files).issubset(all_files):
            raise MonthlyCandidateFinalizerError(
                "shared component inventory is incomplete"
            )
        component_rows: dict[str, dict[str, Any]] = {}
        for path in sorted(all_files, key=lambda item: item.relative_to(root).as_posix()):
            relative = path.relative_to(root).as_posix()
            name = _component_name(relative)
            if name in component_rows:
                raise MonthlyCandidateFinalizerError(
                    "candidate component names are ambiguous"
                )
            component_rows[name] = {
                "path": relative,
                "sha256": _sha256(path),
                "size": path.stat().st_size,
            }
        deployment_content = hashlib.sha256(
            canonical_json_bytes(component_rows)
        ).hexdigest()
        release_id = str(context.plan.get("release_id") or "")
        revision = str(context.plan.get("revision") or "")
        cutoff = str(context.plan.get("target_cutoff") or "")
        try:
            date.fromisoformat(cutoff)
        except ValueError as exc:
            raise MonthlyCandidateFinalizerError(
                "candidate cutoff is invalid"
            ) from exc
        if not release_id or not revision:
            raise MonthlyCandidateFinalizerError(
                "candidate release identity is incomplete"
            )
        st_pit_sha = hashlib.sha256(canonical_json_bytes(st_pit)).hexdigest()
        manifest: dict[str, Any] = {
            "schema_version": "qe_dataset_manifest_v1",
            "release_id": release_id,
            "revision": revision,
            "cutoff_trade_date": cutoff,
            "deployment_content_sha256": deployment_content,
            "deployment_snapshot_id": f"{release_id}_{deployment_content[:16]}",
            "qlib_calendar_sha256": component_rows[
                _component_name(calendar.relative_to(root).as_posix())
            ]["sha256"],
            "qlib_instruments_sha256": component_rows[
                _component_name(instruments.relative_to(root).as_posix())
            ]["sha256"],
            "st_pit_snapshot_id": st_pit["snapshot_id"],
            "st_pit_manifest_sha256": st_pit_sha,
            "st_pit_manifest": st_pit,
            "source_contract": source_contract,
            "components": component_rows,
        }
        manifest["dataset_manifest_sha256"] = _manifest_identity(manifest)
        _write_exclusive(manifest_path, manifest)
        return PhysicalBuildResult(
            manifest_path=manifest_path,
            component_artifacts=tuple(all_files),
            input_artifacts=(),
            workload=StageWorkload(
                source_rows_read=sidecars.source_rows_read,
                computed_rows=sidecars.computed_rows,
                bytes_transferred=0,
            ),
            database_read_performed=False,
            database_write_performed=False,
        )

    @staticmethod
    def _snapshot_release_files(root: Path) -> tuple[Path, ...]:
        files: list[Path] = []
        for base, directories, names in os.walk(root):
            directories.sort()
            names.sort()
            base_path = Path(base)
            if _is_link(base_path):
                raise MonthlyCandidateFinalizerError(
                    "candidate directory inventory contains a link"
                )
            for directory in directories:
                if _is_link(base_path / directory):
                    raise MonthlyCandidateFinalizerError(
                        "candidate directory inventory contains a link"
                    )
            for name in names:
                path = _plain_file(root, base_path / name, label="candidate inventory")
                if path.name == "qe_dataset_manifest.json":
                    raise MonthlyCandidateFinalizerError(
                        "candidate manifest appeared during file snapshot"
                    )
                files.append(path)
        if not files or len(set(files)) != len(files):
            raise MonthlyCandidateFinalizerError(
                "candidate file inventory is empty or duplicated"
            )
        return tuple(files)


__all__: Sequence[str] = (
    "MONTHLY_BUILD_EVIDENCE_SCHEMA",
    "MonthlyCandidateFinalizerError",
    "MonthlySharedComponentBuilder",
    "SharedReleaseComponents",
    "UnifiedMonthlyCandidateFinalizer",
)
