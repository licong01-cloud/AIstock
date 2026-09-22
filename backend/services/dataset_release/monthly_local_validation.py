"""File-only LOCAL_VALIDATE executor for one sealed monthly candidate."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Any, Mapping, Sequence

from .canonical import canonical_json_bytes, digest_named_fields
from .monthly_candidate_finalizer import MONTHLY_BUILD_EVIDENCE_SCHEMA
from .monthly_consumer_layout import CONSUMER_LAYOUT_RECEIPT_SCHEMA
from .monthly_official_adapters import (
    DERIVED_ASSET_REGISTRY_SCHEMA,
    LocalValidationExecution,
    StageWorkload,
)
from .monthly_unified import REQUIRED_CONSUMERS
from .monthly_worker import ProducerContext
from .profile_contract import ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS
from .monthly_shared_components import CORE_INDEX_COVERAGE_SCHEMA


CONSUMER_CONTRACT_SCHEMA = "aistock_monthly_consumer_contract_v1"
SOURCE_READINESS_SCHEMA = "aistock_monthly_source_readiness_v1"
COMPONENT_VALIDATION_SCHEMA = "aistock_monthly_component_validation_v1"
RELEASE_LINEAGE_SCHEMA = "aistock_monthly_release_lineage_v1"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_POOLS = ("stock_universe", "csi300", "csi500", "csi1000", "star50", "star100")
_GROUP_PREFIXES = {
    "day": ("components/daily_bin_candidate/",),
    "minute": ("components/minute_bin_candidate/",),
    "factor": ("components/factor_h5_static_candidate_v2/",),
    "index": ("components/index_context/",),
    "suspend": ("components/suspend_d_daily_candidate_v2/",),
    "stock_pools": ("stock_pools/",),
    "sector_context": ("components/sector_context_candidate_v1/",),
}
_GROUP_EXACT = {
    "benchmark": ("components/daily_bin_candidate/instruments/benchmark.txt",),
    "coverage": ("reports/qe_index_pool_coverage_receipt.json",),
}


class MonthlyLocalValidationError(RuntimeError):
    """The sealed candidate cannot close file-only local validation."""


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


def _read_canonical(path: Path, *, label: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyLocalValidationError(f"{label} is unreadable") from exc
    if not isinstance(value, dict) or raw != canonical_json_bytes(value) + b"\n":
        raise MonthlyLocalValidationError(f"{label} is not canonical JSON")
    return value


def _plain_file(root: Path, relative: str, *, label: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise MonthlyLocalValidationError(f"{label} path is invalid")
    requested = root / path
    current = root
    for part in path.parts:
        current /= part
        if _is_link(current):
            raise MonthlyLocalValidationError(f"{label} traverses a link")
    try:
        resolved = requested.resolve(strict=True)
    except OSError as exc:
        raise MonthlyLocalValidationError(f"{label} is unavailable") from exc
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise MonthlyLocalValidationError(f"{label} is not a candidate file")
    return resolved


def _write_canonical(path: Path, value: Mapping[str, Any]) -> Path:
    payload = canonical_json_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        if _is_link(path) or not path.is_file() or path.read_bytes() != payload:
            raise MonthlyLocalValidationError(
                f"local validation target already differs: {path.name}"
            ) from exc
    return path


def _pin(root: Path, path: Path) -> dict[str, Any]:
    resolved = path.resolve(strict=True)
    return {
        "path": resolved.relative_to(root).as_posix(),
        "sha256": _sha256(resolved),
        "size": resolved.stat().st_size,
    }


def _content_summary(
    manifest_paths: Mapping[str, Mapping[str, Any]],
    *,
    prefix: str,
) -> dict[str, Any]:
    rows = {
        path: {
            "sha256": pin["sha256"],
            "size": pin["size"],
        }
        for path, pin in sorted(manifest_paths.items())
        if path.startswith(prefix)
    }
    if not rows:
        raise MonthlyLocalValidationError(f"consumer component is empty: {prefix}")
    return {
        "file_count": len(rows),
        "logical_bytes": sum(int(item["size"]) for item in rows.values()),
        "content_digest": digest_named_fields(
            "aistock_monthly_consumer_component_v1", rows
        ),
    }


def _manifest_identity(value: Mapping[str, Any]) -> str:
    unsigned = dict(value)
    unsigned.pop("dataset_manifest_sha256", None)
    return hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()


def _cas_ref(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "sha256",
        "size",
        "relative_path",
    }:
        raise MonthlyLocalValidationError(f"{label} is not a complete CAS reference")
    digest = value.get("sha256")
    size = value.get("size")
    relative = value.get("relative_path")
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or type(size) is not int
        or size < 0
        or not isinstance(relative, str)
        or not relative.startswith("cas/sha256/")
        or ".." in Path(relative).parts
    ):
        raise MonthlyLocalValidationError(f"{label} CAS reference is invalid")
    return dict(value)


def _manifest_inventory(
    root: Path,
    manifest: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], tuple[Path, ...]]:
    components = manifest.get("components")
    if not isinstance(components, Mapping) or not components:
        raise MonthlyLocalValidationError("dataset manifest components are empty")
    by_path: dict[str, dict[str, Any]] = {}
    files: list[Path] = []
    for name, raw in sorted(components.items()):
        if not isinstance(raw, Mapping) or set(raw) != {"path", "sha256", "size"}:
            raise MonthlyLocalValidationError(f"manifest component pin is invalid: {name}")
        relative = str(raw["path"])
        if str(name) != relative.replace("/", "__") or relative in by_path:
            raise MonthlyLocalValidationError("manifest component names are ambiguous")
        path = _plain_file(root, relative, label="manifest component")
        pin = dict(raw)
        if path.stat().st_size != pin["size"] or _sha256(path) != pin["sha256"]:
            raise MonthlyLocalValidationError(f"manifest component bytes differ: {relative}")
        by_path[relative] = pin
        files.append(path)
    if manifest.get("deployment_content_sha256") != hashlib.sha256(
        canonical_json_bytes(dict(components))
    ).hexdigest():
        raise MonthlyLocalValidationError("deployment content identity differs")
    return by_path, tuple(files)


def _validate_manifest_authorities(
    root: Path,
    manifest: Mapping[str, Any],
    manifest_paths: Mapping[str, Mapping[str, Any]],
) -> None:
    release_id = str(manifest.get("release_id") or "")
    deployment_content = str(manifest.get("deployment_content_sha256") or "")
    if manifest.get("deployment_snapshot_id") != f"{release_id}_{deployment_content[:16]}":
        raise MonthlyLocalValidationError("deployment snapshot identity differs")
    qlib_files = {
        "qlib_calendar_sha256": "components/daily_bin_candidate/calendars/day.txt",
        "qlib_instruments_sha256": "components/daily_bin_candidate/instruments/all.txt",
    }
    for field, relative in qlib_files.items():
        pin = manifest_paths.get(relative)
        if pin is None or manifest.get(field) != pin.get("sha256"):
            raise MonthlyLocalValidationError(f"{field} differs from consumer bytes")
    st_pit = manifest.get("st_pit_manifest")
    if (
        not isinstance(st_pit, Mapping)
        or st_pit.get("schema_version") != "qe_st_pit_manifest_v1"
        or st_pit.get("snapshot_id") != manifest.get("st_pit_snapshot_id")
        or st_pit.get("cutoff_trade_date") != manifest.get("cutoff_trade_date")
        or manifest.get("st_pit_manifest_sha256")
        != hashlib.sha256(canonical_json_bytes(dict(st_pit))).hexdigest()
    ):
        raise MonthlyLocalValidationError("ST-PIT manifest identity differs")
    sidecars = st_pit.get("index_membership_sidecars")
    selection = st_pit.get("selection_universe")
    if not isinstance(sidecars, Mapping) or set(sidecars) != set(_POOLS):
        raise MonthlyLocalValidationError("ST-PIT six-pool authority differs")
    for label, raw in (("selection_universe", selection), *sorted(sidecars.items())):
        if not isinstance(raw, Mapping) or not {"path", "sha256", "size"}.issubset(raw):
            raise MonthlyLocalValidationError(f"ST-PIT pin is incomplete: {label}")
        relative = str(raw["path"])
        manifest_pin = manifest_paths.get(relative)
        path = _plain_file(root, relative, label=f"ST-PIT {label}")
        if (
            manifest_pin is None
            or raw.get("sha256") != manifest_pin.get("sha256")
            or raw.get("size") != manifest_pin.get("size")
            or _sha256(path) != raw.get("sha256")
        ):
            raise MonthlyLocalValidationError(f"ST-PIT bytes differ: {label}")
    source_contract = manifest.get("source_contract")
    if (
        not isinstance(source_contract, Mapping)
        or source_contract.get("no_fabrication") is not True
        or source_contract.get("database_fallback") is not False
    ):
        raise MonthlyLocalValidationError("sealed source contract is incomplete")


def _validate_core_inventory(root: Path, pinned_paths: set[str]) -> None:
    allowed_unpinned = {
        "qe_dataset_manifest.json",
        "release_closure_receipt.json",
    }
    for base, directories, names in os.walk(root):
        directories.sort()
        names.sort()
        base_path = Path(base)
        if _is_link(base_path):
            raise MonthlyLocalValidationError("candidate inventory contains a linked directory")
        relative_dir = base_path.relative_to(root).as_posix()
        if relative_dir == "provenance" or relative_dir.startswith("provenance/"):
            directories[:] = []
            continue
        if relative_dir == "derived" or relative_dir.startswith("derived/"):
            directories[:] = []
            continue
        for directory in directories:
            if _is_link(base_path / directory):
                raise MonthlyLocalValidationError("candidate inventory contains a linked directory")
        for name in names:
            relative = (base_path / name).relative_to(root).as_posix()
            if relative not in pinned_paths and relative not in allowed_unpinned:
                raise MonthlyLocalValidationError(
                    f"candidate core file is not manifest-pinned: {relative}"
                )


def _derived_inventory(
    root: Path,
    *,
    dataset_manifest_sha256: str,
    derive_scope: Mapping[str, Any],
) -> tuple[Path, tuple[Path, ...], list[dict[str, Any]]]:
    registry_path = _plain_file(
        root,
        "derived/derived_asset_registry.json",
        label="derived asset registry",
    )
    registry = _read_canonical(registry_path, label="derived asset registry")
    rows = registry.get("assets")
    if (
        registry.get("schema_version") != DERIVED_ASSET_REGISTRY_SCHEMA
        or registry.get("source_dataset_manifest_sha256") != dataset_manifest_sha256
        or not isinstance(rows, list)
        or not rows
    ):
        raise MonthlyLocalValidationError("derived asset registry identity differs")
    scope_ref = derive_scope.get("derived_asset_registry_ref")
    if (
        not isinstance(scope_ref, Mapping)
        or scope_ref.get("sha256") != _sha256(registry_path)
        or scope_ref.get("size") != registry_path.stat().st_size
    ):
        raise MonthlyLocalValidationError("DERIVE registry receipt differs")
    assets: list[Path] = []
    pins: list[dict[str, Any]] = []
    observed: set[str] = set()
    for raw in rows:
        if not isinstance(raw, Mapping) or set(raw) != {
            "asset_id",
            "path",
            "sha256",
            "size",
            "schema_version",
        }:
            raise MonthlyLocalValidationError("derived asset row is invalid")
        asset_id = str(raw["asset_id"])
        relative = f"derived/{raw['path']}"
        if not asset_id or asset_id in observed:
            raise MonthlyLocalValidationError("derived asset ids are ambiguous")
        path = _plain_file(root, relative, label="derived asset")
        if path.stat().st_size != raw["size"] or _sha256(path) != raw["sha256"]:
            raise MonthlyLocalValidationError(f"derived asset bytes differ: {asset_id}")
        observed.add(asset_id)
        assets.append(path)
        pins.append(_pin(root, path))
    expected = {"derived/derived_asset_registry.json", *(pin["path"] for pin in pins)}
    actual: set[str] = set()
    for base, directories, names in os.walk(root / "derived"):
        base_path = Path(base)
        if _is_link(base_path):
            raise MonthlyLocalValidationError("derived directory contains a link")
        for directory in directories:
            if _is_link(base_path / directory):
                raise MonthlyLocalValidationError("derived directory contains a link")
        for name in names:
            path = base_path / name
            if _is_link(path) or not path.is_file():
                raise MonthlyLocalValidationError("derived directory contains a link")
            actual.add(path.relative_to(root).as_posix())
    if actual != expected:
        raise MonthlyLocalValidationError("derived directory contains unregistered files")
    return registry_path, tuple(assets), pins


def _component_groups(
    manifest_file_pin: Mapping[str, Any],
    manifest_paths: Mapping[str, Mapping[str, Any]],
    derived_pins: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {
        "manifest": [dict(manifest_file_pin)],
        "derived_assets": [dict(item) for item in derived_pins],
    }
    for name, prefixes in _GROUP_PREFIXES.items():
        groups[name] = [
            dict(pin)
            for path, pin in sorted(manifest_paths.items())
            if any(path.startswith(prefix) for prefix in prefixes)
        ]
    for name, exact in _GROUP_EXACT.items():
        groups[name] = [dict(manifest_paths[path]) for path in exact if path in manifest_paths]
    required = set().union(*(set(value) for value in ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS.values()))
    if set(groups) != required or any(not groups[name] for name in required):
        raise MonthlyLocalValidationError("consumer logical component coverage differs")
    return groups


@dataclass(frozen=True, slots=True)
class MonthlyCandidateLocalValidationExecutor:
    """Validate only sealed candidate bytes and emit candidate-local evidence."""

    def execute(
        self,
        context: ProducerContext,
        *,
        dataset_manifest_sha256: str,
    ) -> LocalValidationExecution:
        if context.stage != "LOCAL_VALIDATE":
            raise MonthlyLocalValidationError("local validator received another stage")
        candidate = Path(str(context.plan.get("candidate_root") or ""))
        if not candidate.is_absolute():
            raise MonthlyLocalValidationError("candidate root is not absolute")
        try:
            root = candidate.resolve(strict=True)
        except OSError as exc:
            raise MonthlyLocalValidationError("candidate root is unavailable") from exc
        if _is_link(candidate) or not root.is_dir():
            raise MonthlyLocalValidationError("candidate root is linked or invalid")

        manifest_path = _plain_file(root, "qe_dataset_manifest.json", label="dataset manifest")
        manifest = _read_canonical(manifest_path, label="dataset manifest")
        if (
            manifest.get("schema_version") != "qe_dataset_manifest_v1"
            or manifest.get("dataset_manifest_sha256") != dataset_manifest_sha256
            or _manifest_identity(manifest) != dataset_manifest_sha256
            or manifest.get("release_id") != context.plan.get("release_id")
            or manifest.get("cutoff_trade_date") != context.plan.get("target_cutoff")
        ):
            raise MonthlyLocalValidationError("dataset manifest identity differs")
        manifest_paths, component_files = _manifest_inventory(root, manifest)
        _validate_manifest_authorities(root, manifest, manifest_paths)
        _validate_core_inventory(root, set(manifest_paths))

        build_evidence_path = _plain_file(
            root,
            "reports/monthly_candidate_build_evidence.json",
            label="monthly build evidence",
        )
        build_evidence = _read_canonical(build_evidence_path, label="monthly build evidence")
        if (
            build_evidence.get("schema_version") != MONTHLY_BUILD_EVIDENCE_SCHEMA
            or build_evidence.get("operation_id") != context.operation_id
            or build_evidence.get("release_id") != context.plan.get("release_id")
            or build_evidence.get("target_cutoff") != context.plan.get("target_cutoff")
            or not isinstance(build_evidence.get("source_bundle_sha256"), str)
            or len(build_evidence["source_bundle_sha256"]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in build_evidence["source_bundle_sha256"]
            )
            or build_evidence.get("database_read_performed") is not False
            or build_evidence.get("database_write_performed") is not False
            or build_evidence.get("runtime_action_performed") is not False
        ):
            raise MonthlyLocalValidationError("monthly build evidence differs")
        validation_authority = {
            "validation_ref": _cas_ref(
                build_evidence.get("validation_ref"), label="validation_ref"
            ),
            "component_artifact_manifest_ref": _cas_ref(
                build_evidence.get("component_artifact_manifest_ref"),
                label="component_artifact_manifest_ref",
            ),
        }

        coverage_path = _plain_file(
            root,
            "reports/qe_index_pool_coverage_receipt.json",
            label="QE coverage receipt",
        )
        coverage = _read_canonical(coverage_path, label="QE coverage receipt")
        pools = coverage.get("pools")
        if (
            coverage.get("schema_version") != "qe_index_pool_coverage_receipt_v1"
            or coverage.get("release_id") != context.plan.get("release_id")
            or coverage.get("cutoff") != context.plan.get("target_cutoff")
            or not isinstance(pools, Mapping)
            or set(pools) != set(_POOLS)
        ):
            raise MonthlyLocalValidationError("QE coverage receipt identity differs")
        pool_gaps = {
            name: (
                len(value["gaps"])
                if isinstance(value, Mapping)
                and isinstance(value.get("gaps"), list)
                and value.get("available_end") == context.plan.get("target_cutoff")
                else -1
            )
            for name, value in pools.items()
        }
        if any(value != 0 for value in pool_gaps.values()):
            raise MonthlyLocalValidationError("six-pool coverage contains gaps")

        core_coverage_path = _plain_file(
            root,
            "reports/index_pool_coverage.json",
            label="physical six-pool coverage",
        )
        core_coverage = _read_canonical(
            core_coverage_path, label="physical six-pool coverage"
        )
        core_pools = core_coverage.get("pools")
        pool_paths = {
            name: (
                "stock_pools/stock_universe.txt"
                if name == "stock_universe"
                else f"stock_pools/index_pool__{name}.txt"
            )
            for name in _POOLS
        }
        if (
            core_coverage.get("schema_version") != CORE_INDEX_COVERAGE_SCHEMA
            or core_coverage.get("cutoff_trade_date") != context.plan.get("target_cutoff")
            or core_coverage.get("unexplained_gap_count") != 0
            or core_coverage.get("physical_validation_ref")
            != validation_authority["validation_ref"]
            or core_coverage.get("component_artifact_manifest_ref")
            != validation_authority["component_artifact_manifest_ref"]
            or not isinstance(core_pools, Mapping)
            or set(core_pools) != set(_POOLS)
            or any(
                not isinstance(value, Mapping)
                or value.get("day_gap_count") != 0
                or value.get("minute_gap_count") != 0
                or value.get("subset_of_frozen_pit") is not True
                or value.get("sidecar_sha256")
                != manifest_paths.get(pool_paths[name], {}).get("sha256")
                for name, value in core_pools.items()
            )
        ):
            raise MonthlyLocalValidationError("physical six-pool coverage differs")

        layout_path = _plain_file(
            root,
            "reports/monthly_consumer_layout_receipt.json",
            label="consumer layout receipt",
        )
        layout = _read_canonical(layout_path, label="consumer layout receipt")
        layout_body = dict(layout)
        layout_digest = layout_body.pop("consumer_layout_digest", None)
        expected_layout_components = {
            name: _content_summary(manifest_paths, prefix=prefix)
            for name, prefix in {
                "day": "components/daily_bin_candidate/",
                "minute": "components/minute_bin_candidate/",
                "factor": "components/factor_h5_static_candidate_v2/",
                "index": "components/index_context/",
            }.items()
        }
        coverage_pin = _pin(root, coverage_path)
        if (
            layout.get("schema_version") != CONSUMER_LAYOUT_RECEIPT_SCHEMA
            or layout.get("release_id") != context.plan.get("release_id")
            or layout.get("cutoff") != context.plan.get("target_cutoff")
            or layout.get("publication_mode") != "same_filesystem_hardlink_v1"
            or layout.get("components") != expected_layout_components
            or layout.get("source_freeze") is not True
            or layout.get("database_read") is not False
            or layout.get("database_write") is not False
            or layout.get("runtime_action") is not False
            or layout.get("validation_authority") != validation_authority
            or layout.get("coverage_receipt") != coverage_pin
            or layout_digest
            != digest_named_fields(CONSUMER_LAYOUT_RECEIPT_SCHEMA, layout_body)
        ):
            raise MonthlyLocalValidationError("consumer layout readiness differs")

        derive_receipt = context.prior_receipts.get("DERIVE")
        derive_scope = derive_receipt.get("scope") if isinstance(derive_receipt, Mapping) else None
        if not isinstance(derive_scope, Mapping):
            raise MonthlyLocalValidationError("DERIVE scope is unavailable")
        registry_path, derived_files, derived_pins = _derived_inventory(
            root,
            dataset_manifest_sha256=dataset_manifest_sha256,
            derive_scope=derive_scope,
        )

        manifest_file_pin = _pin(root, manifest_path)
        groups = _component_groups(manifest_file_pin, manifest_paths, derived_pins)
        provenance = root / "provenance"
        expected_names = {
            "source-readiness.json",
            "component-validation.json",
            "release-lineage.json",
            *(f"consumer-contract-{name}.json" for name in REQUIRED_CONSUMERS),
        }
        if provenance.exists():
            if _is_link(provenance) or not provenance.is_dir():
                raise MonthlyLocalValidationError("candidate provenance root is invalid")
            existing = {
                path.name for path in provenance.iterdir() if path.is_file()
            }
            if existing.difference(expected_names) or any(path.is_dir() for path in provenance.iterdir()):
                raise MonthlyLocalValidationError("candidate provenance contains unknown entries")

        contracts: list[Path] = []
        for consumer in REQUIRED_CONSUMERS:
            requirements = sorted(ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[consumer])
            contracts.append(
                _write_canonical(
                    provenance / f"consumer-contract-{consumer}.json",
                    {
                        "schema_version": CONSUMER_CONTRACT_SCHEMA,
                        "consumer_id": consumer,
                        "dataset_manifest_sha256": dataset_manifest_sha256,
                        "required_components": requirements,
                        "component_refs": {
                            name: groups[name] for name in requirements
                        },
                        "status": "READY_FOR_NODE_READBACK",
                    },
                )
            )

        source_readiness = _write_canonical(
            provenance / "source-readiness.json",
            {
                "schema_version": SOURCE_READINESS_SCHEMA,
                "dataset_manifest_sha256": dataset_manifest_sha256,
                "build_evidence_ref": _pin(root, build_evidence_path),
                "coverage_ref": _pin(root, coverage_path),
                "physical_coverage_ref": _pin(root, core_coverage_path),
                "consumer_layout_ref": _pin(root, layout_path),
                "source_freeze": True,
                "database_write": False,
                "unexplained_gap_count": 0,
                "status": "PASS",
            },
        )
        component_validation = _write_canonical(
            provenance / "component-validation.json",
            {
                "schema_version": COMPONENT_VALIDATION_SCHEMA,
                "dataset_manifest_sha256": dataset_manifest_sha256,
                "deployment_content_sha256": manifest["deployment_content_sha256"],
                "manifest_component_count": len(manifest_paths),
                "logical_components": sorted(groups),
                "derived_asset_count": len(derived_pins),
                "duplicate_path_count": 0,
                "unpinned_core_file_count": 0,
                "hash_mismatch_count": 0,
                "status": "PASS",
            },
        )
        predecessor = context.plan.get("predecessor")
        predecessor_ref = context.plan.get("predecessor_profile_ref")
        if not isinstance(predecessor, Mapping) or not isinstance(predecessor_ref, Mapping):
            raise MonthlyLocalValidationError("predecessor lineage is unavailable")
        lineage = _write_canonical(
            provenance / "release-lineage.json",
            {
                "schema_version": RELEASE_LINEAGE_SCHEMA,
                "dataset_manifest_sha256": dataset_manifest_sha256,
                "release_id": context.plan.get("release_id"),
                "revision": context.plan.get("revision"),
                "cutoff_trade_date": context.plan.get("target_cutoff"),
                "predecessor_dataset_manifest_sha256": predecessor.get(
                    "dataset_manifest_sha256"
                ),
                "predecessor_profile_ref": dict(predecessor_ref),
                "source_bundle_sha256": build_evidence.get("source_bundle_sha256"),
                "status": "SEALED_SUCCESSOR",
            },
        )
        inputs = (
            manifest_path,
            build_evidence_path,
            coverage_path,
            core_coverage_path,
            layout_path,
            registry_path,
            *derived_files,
        )
        return LocalValidationExecution(
            pool_gap_counts=pool_gaps,
            dataset_identity_complete=True,
            consumer_contracts=tuple(contracts),
            source_readiness=(source_readiness,),
            component_validations=(component_validation,),
            lineage_path=lineage,
            input_artifacts=inputs,
            workload=StageWorkload(
                source_rows_read=len(component_files) + len(derived_files),
                computed_rows=len(contracts) + 3,
                bytes_transferred=0,
            ),
        )


__all__: Sequence[str] = (
    "COMPONENT_VALIDATION_SCHEMA",
    "CONSUMER_CONTRACT_SCHEMA",
    "MonthlyCandidateLocalValidationExecutor",
    "MonthlyLocalValidationError",
    "RELEASE_LINEAGE_SCHEMA",
    "SOURCE_READINESS_SCHEMA",
)
