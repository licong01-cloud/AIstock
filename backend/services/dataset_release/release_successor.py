"""Create an immutable dataset successor by adding a frozen sector context.

The baseline release is never opened for writing.  A successor is assembled in
an exclusive sibling staging directory and becomes visible only after all
identity checks and metadata writes have succeeded.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
from pathlib import Path
import shutil
from typing import Any, Mapping

from .canonical import canonical_json_bytes, ensure_sha256
from .shared_sector_context import (
    SECTOR_CONTEXT_COMPONENT_ROOT,
    SECTOR_CONTEXT_PINS_SCHEMA,
    load_release_sw_l2_code_map,
)


SUCCESSOR_RECEIPT_SCHEMA = "aistock_dataset_release_successor_receipt_v1"
PROFILE_SCHEMA_V3 = "aistock_active_dataset_profile_v3"

_SECTOR_FILES = {
    "sector_code_map": "sector_code_map.json",
    "market_context": "market_context.parquet",
    "sector_membership_spans": "sector_membership_spans.parquet",
    "sector_context_receipt": "component_receipt.json",
}
_CONSUMER_REQUIREMENTS = {
    "qe": [
        "benchmark",
        "coverage",
        "day",
        "factor",
        "index",
        "manifest",
        "minute",
        "sector_context",
        "stock_pools",
        "suspend",
    ],
    "hmm": ["factor", "index", "manifest", "sector_context"],
    "selection": [
        "day",
        "factor",
        "index",
        "manifest",
        "minute",
        "stock_pools",
        "suspend",
    ],
    "advisory": [
        "day",
        "factor",
        "index",
        "manifest",
        "minute",
        "stock_pools",
        "suspend",
    ],
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, *, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{field} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    return value


def _write_json_exclusive(path: Path, value: Mapping[str, Any]) -> None:
    payload = canonical_json_bytes(value) + b"\n"
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()


def _manifest_identity(value: Mapping[str, Any]) -> str:
    identity = dict(value)
    identity.pop("dataset_manifest_sha256", None)
    return hashlib.sha256(canonical_json_bytes(identity)).hexdigest()


def _require_manifest_identity(
    manifest_path: Path,
    *,
    expected_identity: str,
    expected_file_sha256: str,
) -> dict[str, Any]:
    expected_identity = ensure_sha256(expected_identity, field="expected_manifest_identity")
    expected_file_sha256 = ensure_sha256(expected_file_sha256, field="expected_manifest_file_sha256")
    manifest = _read_json(manifest_path, field="baseline manifest")
    actual_identity = _manifest_identity(manifest)
    actual_file_sha256 = _sha256(manifest_path)
    if (
        manifest.get("dataset_manifest_sha256") != expected_identity
        or actual_identity != expected_identity
        or actual_file_sha256 != expected_file_sha256
    ):
        raise ValueError("baseline dataset manifest identity differs")
    return manifest


def _validate_manifest_components(root: Path, components: Mapping[str, Any]) -> None:
    for name, raw in components.items():
        if not isinstance(raw, Mapping):
            raise ValueError(f"manifest component is invalid: {name}")
        relative = str(raw.get("path") or "")
        expected = str(raw.get("sha256") or "")
        if not relative or Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise ValueError(f"manifest component path is invalid: {name}")
        path = root / relative
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"manifest component is missing or linked: {name}")
        if _sha256(path) != ensure_sha256(expected, field=f"components.{name}.sha256"):
            raise ValueError(f"manifest component hash differs: {name}")
        if int(raw.get("size", -1)) != path.stat().st_size:
            raise ValueError(f"manifest component size differs: {name}")


def _load_sector_receipt(component_root: Path) -> dict[str, Any]:
    if component_root.is_symlink() or not component_root.is_dir():
        raise ValueError("sector context source is missing or linked")
    receipt = _read_json(component_root / "component_receipt.json", field="sector receipt")
    if receipt.get("schema_version") != "aistock_sector_context_receipt_v1":
        raise ValueError("sector context receipt schema differs")
    for key, filename in _SECTOR_FILES.items():
        path = component_root / filename
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"sector context file is missing or linked: {filename}")
        if key == "sector_context_receipt":
            continue
        section = (
            receipt["sector_code_map"]
            if key == "sector_code_map"
            else receipt["market_context"]
            if key == "market_context"
            else receipt["membership"]
        )
        if section.get("path") != filename or section.get("sha256") != _sha256(path):
            raise ValueError(f"sector context receipt hash differs: {filename}")
    load_release_sw_l2_code_map(component_root / "sector_code_map.json")
    receipt["receipt_sha256"] = _sha256(component_root / "component_receipt.json")
    return receipt


def _sector_manifest_components(component_root: Path, receipt: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    root = SECTOR_CONTEXT_COMPONENT_ROOT
    return {
        "sector_code_map": {
            "path": f"{root}/sector_code_map.json",
            "sha256": _sha256(component_root / "sector_code_map.json"),
            "size": (component_root / "sector_code_map.json").stat().st_size,
            "schema_version": receipt["sector_code_map"]["schema_version"],
            "code_map_digest": receipt["sector_code_map"]["code_map_digest"],
        },
        "market_context": {
            "path": f"{root}/market_context.parquet",
            "sha256": _sha256(component_root / "market_context.parquet"),
            "size": (component_root / "market_context.parquet").stat().st_size,
            "schema_version": receipt["market_context"]["schema_version"],
            "definition": receipt["market_context"]["definition"],
            "start": receipt["market_context"]["start"],
            "end": receipt["market_context"]["end"],
            "row_count": receipt["market_context"]["row_count"],
        },
        "sector_membership_spans": {
            "path": f"{root}/sector_membership_spans.parquet",
            "sha256": _sha256(component_root / "sector_membership_spans.parquet"),
            "size": (component_root / "sector_membership_spans.parquet").stat().st_size,
            "schema_version": receipt["membership"]["schema_version"],
            "start": receipt["membership"]["start"],
            "end": receipt["membership"]["end"],
            "span_count": receipt["membership"]["span_count"],
            "symbol_count": receipt["membership"]["symbol_count"],
        },
        "sector_context_receipt": {
            "path": f"{root}/component_receipt.json",
            "sha256": _sha256(component_root / "component_receipt.json"),
            "size": (component_root / "component_receipt.json").stat().st_size,
            "schema_version": receipt["schema_version"],
        },
    }


def _sector_context_pins(receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SECTOR_CONTEXT_PINS_SCHEMA,
        "component_root": SECTOR_CONTEXT_COMPONENT_ROOT,
        "code_map_file": "sector_code_map.json",
        "code_map_sha256": receipt["sector_code_map"]["sha256"],
        "code_map_digest": receipt["sector_code_map"]["code_map_digest"],
        "market_context_file": "market_context.parquet",
        "market_context_sha256": receipt["market_context"]["sha256"],
        "market_volume_definition": receipt["market_context"]["definition"],
        "market_start": receipt["market_context"]["start"],
        "market_end": receipt["market_context"]["end"],
        "membership_file": "sector_membership_spans.parquet",
        "membership_sha256": receipt["membership"]["sha256"],
        "membership_start": receipt["membership"]["start"],
        "membership_end": receipt["membership"]["end"],
        "receipt_file": "component_receipt.json",
        "receipt_sha256": receipt["receipt_sha256"],
        "sector_data_sha256": receipt["sector_data"]["sha256"],
        "source_dataset_manifest_sha256": receipt["source_dataset_manifest_sha256"],
        "authority_id": receipt["sector_code_map"]["authority"]["authority_id"],
        "authority_sha256": receipt["sector_code_map"]["authority"]["authority_sha256"],
    }


def _copy_ignore(baseline_root: Path):
    def ignore(directory: str, names: list[str]) -> set[str]:
        if Path(directory) == baseline_root:
            return {name for name in names if name in {"qe_dataset_manifest.json", "direct_monthly_state.json"}}
        if Path(directory) == baseline_root / "components":
            sector_name = Path(SECTOR_CONTEXT_COMPONENT_ROOT).name
            return {sector_name} if sector_name in names else set()
        return set()

    return ignore


def build_successor(
    *,
    baseline_root: Path,
    successor_root: Path,
    sector_component_root: Path,
    revision: str,
    expected_baseline_manifest_identity: str,
    expected_baseline_manifest_file_sha256: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Build and atomically publish one immutable successor candidate."""

    baseline_root = baseline_root.resolve(strict=True)
    successor_root = successor_root.absolute()
    sector_component_root = sector_component_root.resolve(strict=True)
    if successor_root.exists():
        raise FileExistsError(f"successor candidate already exists: {successor_root}")
    staging = successor_root.with_name(f".{successor_root.name}.building")
    if staging.exists():
        raise FileExistsError(f"successor staging root already exists: {staging}")
    if baseline_root == successor_root or successor_root.is_relative_to(baseline_root):
        raise ValueError("successor root must be separate from baseline")
    manifest = _require_manifest_identity(
        baseline_root / "qe_dataset_manifest.json",
        expected_identity=expected_baseline_manifest_identity,
        expected_file_sha256=expected_baseline_manifest_file_sha256,
    )
    components = manifest.get("components")
    if not isinstance(components, dict):
        raise ValueError("baseline manifest components are invalid")
    _validate_manifest_components(baseline_root, components)
    receipt = _load_sector_receipt(sector_component_root)
    if receipt.get("source_dataset_manifest_sha256") != expected_baseline_manifest_identity:
        raise ValueError("sector context source manifest differs from baseline")
    sector_h5 = baseline_root / str(receipt["sector_data"]["path"])
    if _sha256(sector_h5) != receipt["sector_data"]["sha256"]:
        raise ValueError("sector context sector_data identity differs from baseline")
    timestamp = created_at or dt.datetime.now(dt.timezone.utc).isoformat()

    shutil.copytree(
        baseline_root,
        staging,
        copy_function=shutil.copy2,
        ignore=_copy_ignore(baseline_root),
    )
    target_component = staging / Path(SECTOR_CONTEXT_COMPONENT_ROOT)
    shutil.copytree(sector_component_root, target_component, copy_function=shutil.copy2)

    new_manifest = copy.deepcopy(manifest)
    new_manifest["revision"] = str(revision)
    new_manifest["components"].update(_sector_manifest_components(target_component, receipt))
    new_manifest["deployment_content_sha256"] = hashlib.sha256(
        canonical_json_bytes(new_manifest["components"])
    ).hexdigest()
    new_manifest["deployment_snapshot_id"] = (
        f"{new_manifest['release_id']}_{new_manifest['deployment_content_sha256'][:16]}"
    )
    new_manifest["dataset_manifest_sha256"] = _manifest_identity(new_manifest)
    manifest_path = staging / "qe_dataset_manifest.json"
    _write_json_exclusive(manifest_path, new_manifest)
    manifest_file_sha256 = _sha256(manifest_path)

    baseline_state = _read_json(baseline_root / "direct_monthly_state.json", field="baseline direct state")
    new_state = copy.deepcopy(baseline_state)
    new_state.update(
        {
            "baseline_root": str(baseline_root),
            "candidate_root": str(successor_root),
            "created_at": timestamp,
            "updated_at": timestamp,
            "revision": str(revision),
            "database_repair": {"ddl": False, "dml": False, "production_writes": 0},
            "production_activation": False,
            "production_pointer_changes": 0,
            "production_writes": 0,
            "runtime_action_performed": False,
            "manifest": {
                "path": "qe_dataset_manifest.json",
                "dataset_manifest_sha256": new_manifest["dataset_manifest_sha256"],
                "deployment_snapshot_id": new_manifest["deployment_snapshot_id"],
                "file_sha256": manifest_file_sha256,
            },
            "source_candidate": {
                "root": str(baseline_root),
                "dataset_manifest_sha256": expected_baseline_manifest_identity,
                "manifest_file_sha256": expected_baseline_manifest_file_sha256,
            },
        }
    )
    new_state.setdefault("components", {})["sector_context"] = {
        "action": "ADD_FROZEN_SHARED_SECTOR_CONTEXT",
        "status": "PASS",
        "receipt_sha256": receipt["receipt_sha256"],
    }
    _write_json_exclusive(staging / "direct_monthly_state.json", new_state)

    _validate_manifest_components(staging, new_manifest["components"])
    staging.rename(successor_root)
    return {
        "schema_version": SUCCESSOR_RECEIPT_SCHEMA,
        "baseline_root": str(baseline_root),
        "successor_root": str(successor_root),
        "revision": str(revision),
        "release_id": new_manifest["release_id"],
        "dataset_manifest_sha256": new_manifest["dataset_manifest_sha256"],
        "dataset_manifest_file_sha256": manifest_file_sha256,
        "deployment_content_sha256": new_manifest["deployment_content_sha256"],
        "deployment_snapshot_id": new_manifest["deployment_snapshot_id"],
        "sector_context_pins": _sector_context_pins(receipt),
        "database_read": False,
        "database_write": False,
        "runtime_action": False,
        "production_activation": False,
    }


def build_profile_v3(
    *,
    baseline_profile_path: Path,
    profile_output_path: Path,
    successor_receipt: Mapping[str, Any],
    generation: str,
    controller_candidate_root: str,
    node_candidate_roots: Mapping[str, str],
) -> dict[str, Any]:
    """Create a v3 profile candidate without activating it."""

    if profile_output_path.exists():
        raise FileExistsError(f"profile candidate already exists: {profile_output_path}")
    base = _read_json(baseline_profile_path, field="baseline active profile")
    profile = copy.deepcopy(base)
    profile["schema_version"] = PROFILE_SCHEMA_V3
    profile["generation"] = str(generation)
    profile["controller_paths"]["candidate_root"] = str(controller_candidate_root)
    profile["controller_paths"]["stock_pool_root"] = str(Path(controller_candidate_root) / "stock_pools")
    profile["node_bindings"] = {
        str(node): {"candidate_root": str(root)} for node, root in sorted(node_candidate_roots.items())
    }
    profile["components"]["sector_context_pins"] = dict(successor_receipt["sector_context_pins"])
    profile["components"]["dataset_manifest_sha256"] = successor_receipt["dataset_manifest_sha256"]
    profile["components"]["dataset_manifest_file_sha256"] = successor_receipt["dataset_manifest_file_sha256"]
    qe = copy.deepcopy(profile["consumers"]["qe"])
    qe["required_components"] = _CONSUMER_REQUIREMENTS["qe"]
    profile["consumers"] = {
        "qe": qe,
        "hmm": {"required_components": _CONSUMER_REQUIREMENTS["hmm"]},
        "selection": {"required_components": _CONSUMER_REQUIREMENTS["selection"]},
        "advisory": {"required_components": _CONSUMER_REQUIREMENTS["advisory"]},
    }
    profile_output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json_exclusive(profile_output_path, profile)
    return profile
