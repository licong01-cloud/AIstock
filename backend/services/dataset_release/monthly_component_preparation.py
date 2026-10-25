"""Unpublished component dependency and checkpoint contracts.

ELIGIBLE is not READY and PREPARED_UNPUBLISHED is not a SOURCE/BUILD receipt.
The worker must still seal a complete SOURCE and validate the final release.
This module does not read a provider, execute a writer, or activate a profile.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping, Sequence

from .artifact_ready_source import _COMPONENT_DATASETS
from .canonical import canonical_json_bytes, digest_named_fields, ensure_sha256, normalize_root_relative_path
from .contracts import Component
from .errors import CanonicalizationError
from .source_authority import PRODUCTION_QUERY_SPECS


PLAN_SCHEMA = "aistock_monthly_component_preparation_plan_v1"
RECEIPT_SCHEMA = "aistock_monthly_prepared_component_v1"
INPUT_SCHEMA = "aistock_monthly_component_preparation_input_v1"
_PHYSICAL = {
    "day": Component.DAILY_BIN,
    "minute": Component.MINUTE_BIN,
    "factor": Component.FACTOR_H5_STATIC,
    "index": Component.DOMESTIC_INDEX_CONTEXT,
}
_EXTRA = {
    "day": frozenset({"stock_basic"}),
    "minute": frozenset({"stock_basic"}),
    "factor": frozenset(),
    "index": frozenset(),
    "suspend": frozenset({"suspend_d", "stk_limit", "stock_basic"}),
    "benchmark": frozenset({"kline_daily_raw", "adj_factor", "index_daily"}),
    "stock_pools": frozenset({"index_membership_pit", "stock_basic"}),
    "sector_context": frozenset({"sector_data", "sw_index_classify", "sw_index_member", "stock_basic", "moneyflow_ts"}),
}
_COMMON = frozenset({"trading_calendar", "stock_universe_pit"})
_ALIASES = {
    "stock_moneyflow_ts": ("moneyflow_ts",),
    "moneyflow": ("moneyflow_ts",),
    "industry_classification": ("sector_data", "sw_index_classify", "sw_index_member"),
    "sw_daily": ("sector_data",),
}
_IDENTITY_FIELDS = {
    "component",
    "cutoff",
    "predecessor_profile_sha256",
    "effective_source_sha256",
    "pit_sha256",
    "qfq_sha256",
    "producer_sha256",
    "schema_sha256",
    "build_parameters_sha256",
    "validation_policy_sha256",
}
_RECEIPT_FIELDS = {
    "schema_version",
    "operation_id",
    "source_snapshot_id",
    "identity",
    "identity_digest",
    "status",
    "publication_allowed",
    "output_refs",
    "validation_refs",
    "canonical_digest",
}
_MAX_RECEIPT_BYTES = 16 * 1024 * 1024


class ComponentPreparationError(RuntimeError):
    """Preparation is unknown, incomplete, unsafe or no longer equivalent."""


def _operation_id(value: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"dmr_[0-9a-f]{32}", value) is None:
        raise ComponentPreparationError("preparation operation identity is invalid")
    return value


def component_dependencies() -> dict[str, tuple[str, ...]]:
    """Union formal raw-query and normalized physical-writer dependencies.

    Adding a producer dependency must never accidentally weaken preparation.
    Shared sidecars declare their own facts instead of depending on a complete
    factor aggregate, which includes unrelated margin data.
    """
    result = {}
    for name, extra in _EXTRA.items():
        datasets = set(extra) | set(_COMMON)
        physical = _PHYSICAL.get(name)
        if physical is not None:
            datasets.update(_COMPONENT_DATASETS[physical])
            datasets.update(query.query_id for query in PRODUCTION_QUERY_SPECS.values() if physical in query.components)
        result[name] = tuple(sorted(datasets))
    return result


def preparation_plan(*, operation_id: str, cutoff: date, blocking_datasets: Sequence[str]) -> dict[str, Any]:
    """Compute eligibility only; never infer fact coverage from refresh rows."""
    _operation_id(operation_id)
    if type(cutoff) is not date or isinstance(blocking_datasets, (str, bytes)):
        raise ComponentPreparationError("preparation cutoff/blocker contract is invalid")
    if any(not isinstance(value, str) or not value.strip() for value in blocking_datasets):
        raise ComponentPreparationError("preparation blocker identity is missing")
    dependencies = component_dependencies()
    known = set().union(*(set(values) for values in dependencies.values()))
    expanded = set()
    for raw in blocking_datasets:
        expanded.update(_ALIASES.get(raw, (raw,)))
    blockers = tuple(sorted(expanded))
    unknown = expanded - known
    rows = {}
    for name, required in sorted(dependencies.items()):
        affected = sorted(unknown | (expanded & set(required)))
        rows[name] = {
            "status": "DEFERRED" if affected else "ELIGIBLE",
            "blocking_datasets": affected,
            "required_datasets": list(required),
        }
    body = {
        "schema_version": PLAN_SCHEMA,
        "operation_id": operation_id,
        "cutoff": cutoff.isoformat(),
        "dependency_contract_digest": digest_named_fields("aistock_monthly_component_dependency_v1", dependencies),
        "blocking_datasets": list(blockers),
        "unknown_blocking_datasets": sorted(unknown),
        "components": rows,
        "eligible_component_count": sum(row["status"] == "ELIGIBLE" for row in rows.values()),
        "deferred_component_count": sum(row["status"] == "DEFERRED" for row in rows.values()),
        "prepared_component_count": 0,
        "publication_allowed": False,
        "consistent_input_set_complete": False,
    }
    return {**body, "canonical_digest": digest_named_fields(PLAN_SCHEMA, body)}


def _strict_sha(value: str, *, field: str) -> str:
    try:
        normalized = ensure_sha256(value, field=field)
    except CanonicalizationError as exc:
        raise ComponentPreparationError(f"{field} digest is invalid") from exc
    if value != normalized:
        raise ComponentPreparationError(f"{field} digest is non-canonical")
    return normalized


@dataclass(frozen=True, slots=True)
class ComponentInputIdentity:
    component: str
    cutoff: date
    predecessor_profile_sha256: str
    effective_source_sha256: str
    pit_sha256: str
    qfq_sha256: str | None
    producer_sha256: str
    schema_sha256: str
    build_parameters_sha256: str
    validation_policy_sha256: str

    def __post_init__(self) -> None:
        if self.component not in _EXTRA or type(self.cutoff) is not date:
            raise ComponentPreparationError("component input identity is invalid")
        for field in _IDENTITY_FIELDS - {"component", "cutoff", "qfq_sha256"}:
            _strict_sha(getattr(self, field), field=field)
        if self.qfq_sha256 is not None:
            _strict_sha(self.qfq_sha256, field="qfq_sha256")
        if self.component in {"day", "minute", "factor", "benchmark"} and self.qfq_sha256 is None:
            raise ComponentPreparationError("price component lacks QFQ input identity")

    def payload(self) -> dict[str, Any]:
        return {
            field: self.cutoff.isoformat() if field == "cutoff" else getattr(self, field)
            for field in sorted(_IDENTITY_FIELDS)
        }

    @property
    def digest(self) -> str:
        return digest_named_fields(INPUT_SCHEMA, self.payload())


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(int(getattr(metadata, "st_file_attributes", 0)) & 0x0400)


def _plain_path(path: Path, *, root: Path, directory: bool = False) -> Path:
    if not root.is_absolute() or not path.is_absolute():
        raise ComponentPreparationError("preparation path must be absolute")
    if ".." in root.parts or ".." in path.parts:
        raise ComponentPreparationError("preparation path contains parent traversal")
    root = Path(os.path.abspath(root))
    requested = Path(os.path.abspath(path))
    if not requested.is_relative_to(root):
        raise ComponentPreparationError("preparation path escaped its root")
    # Check before resolve so a link cannot be made invisible by normalization.
    current = Path(requested.anchor)
    for part in requested.parts[1:]:
        current /= part
        if _is_link(current):
            raise ComponentPreparationError("preparation path contains a link or junction")
    if not (requested.is_dir() if directory else requested.is_file()):
        raise ComponentPreparationError("preparation path is not a regular file/directory")
    if not directory and requested.stat().st_nlink != 1:
        raise ComponentPreparationError("preparation file is hardlinked instead of private")
    return requested


def _file_ref(root: Path, relative: str) -> dict[str, Any]:
    if not isinstance(relative, str) or ":" in relative or "\\" in relative:
        raise ComponentPreparationError("preparation relative file path is invalid")
    try:
        normalized = normalize_root_relative_path(relative)
    except CanonicalizationError as exc:
        raise ComponentPreparationError("preparation relative file path is invalid") from exc
    # Formal index CSV names contain uppercase exchange suffixes. Keep their
    # actual spelling for ext4 while still rejecting non-normalized syntax;
    # Windows case identity is used for duplicate detection, not file renaming.
    if normalized != relative.casefold():
        raise ComponentPreparationError("preparation relative file path is non-canonical")
    path = _plain_path(root / relative, root=root)
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ino, before.st_nlink) != (
        after.st_size,
        after.st_mtime_ns,
        after.st_ino,
        after.st_nlink,
    ) or after.st_nlink != 1:
        raise ComponentPreparationError("preparation file changed while hashing")
    return {"path": relative, "sha256": digest.hexdigest(), "size": after.st_size}


def _refs(root: Path, paths: Sequence[str]) -> list[dict[str, Any]]:
    if (
        isinstance(paths, (str, bytes))
        or not paths
        or any(not isinstance(path, str) for path in paths)
        or len({path.casefold() for path in paths}) != len(paths)
    ):
        raise ComponentPreparationError("preparation file set is empty or duplicated")
    if "prepared-component.json" in {path.casefold() for path in paths}:
        raise ComponentPreparationError("preparation receipt cannot hash itself")
    return [_file_ref(root, path) for path in sorted(paths)]


def seal_prepared_component(
    *,
    preparation_root: Path,
    component_root: Path,
    operation_id: str,
    source_snapshot_id: str,
    identity: ComponentInputIdentity,
    output_paths: Sequence[str],
    validation_paths: Sequence[str],
) -> dict[str, Any]:
    """Pin actual writer files and domain validation evidence, create-exclusive.

    The calling formal producer is responsible for domain validation. A caller
    cannot pass this receipt as a full SOURCE or candidate validation receipt.
    """
    _operation_id(operation_id)
    root = _plain_path(component_root, root=preparation_root, directory=True)
    if (
        not isinstance(source_snapshot_id, str)
        or not source_snapshot_id.startswith("postgres:")
        or not source_snapshot_id[9:]
    ):
        raise ComponentPreparationError("preparation snapshot identity is missing")
    outputs, validations = _refs(root, output_paths), _refs(root, validation_paths)
    if {path.casefold() for path in output_paths} & {path.casefold() for path in validation_paths}:
        raise ComponentPreparationError("preparation output/validation paths overlap")
    body = {
        "schema_version": RECEIPT_SCHEMA,
        "operation_id": operation_id,
        "source_snapshot_id": source_snapshot_id,
        "identity": identity.payload(),
        "identity_digest": identity.digest,
        "status": "PREPARED_UNPUBLISHED",
        "publication_allowed": False,
        "output_refs": outputs,
        "validation_refs": validations,
    }
    value = {**body, "canonical_digest": digest_named_fields(RECEIPT_SCHEMA, body)}
    raw = canonical_json_bytes(value) + b"\n"
    if len(raw) > _MAX_RECEIPT_BYTES:
        raise ComponentPreparationError("preparation receipt exceeds bounded control size")
    path = root / "prepared-component.json"
    if _is_link(path):
        raise ComponentPreparationError("preparation receipt is linked")
    with path.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    return {
        "path": path.relative_to(preparation_root).as_posix(),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size": len(raw),
    }


def validate_prepared_component(
    *,
    preparation_root: Path,
    component_root: Path,
    receipt_sha256: str,
    operation_id: str,
    expected_identity: ComponentInputIdentity,
) -> Mapping[str, Any]:
    """Verify an unpublished checkpoint against final verified input identity.

    This validates neither full SOURCE nor release readiness. Final SOURCE
    closure and component/domain validation remain mandatory in the caller.
    """
    _operation_id(operation_id)
    _strict_sha(receipt_sha256, field="receipt_sha256")
    root = _plain_path(component_root, root=preparation_root, directory=True)
    path = _plain_path(root / "prepared-component.json", root=root)
    if path.stat().st_size > _MAX_RECEIPT_BYTES:
        raise ComponentPreparationError("preparation receipt exceeds bounded control size")
    with path.open("rb") as handle:
        raw = handle.read(_MAX_RECEIPT_BYTES + 1)
    if len(raw) > _MAX_RECEIPT_BYTES or hashlib.sha256(raw).hexdigest() != receipt_sha256:
        raise ComponentPreparationError("preparation receipt bytes differ")
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise ComponentPreparationError("preparation receipt is invalid JSON") from exc
    if (
        not isinstance(value, dict)
        or set(value) != _RECEIPT_FIELDS
        or value["schema_version"] != RECEIPT_SCHEMA
        or value["status"] != "PREPARED_UNPUBLISHED"
        or value["publication_allowed"] is not False
        or raw != canonical_json_bytes(value) + b"\n"
    ):
        raise ComponentPreparationError("preparation receipt schema differs")
    unsigned = {key: item for key, item in value.items() if key != "canonical_digest"}
    if value["canonical_digest"] != digest_named_fields(RECEIPT_SCHEMA, unsigned):
        raise ComponentPreparationError("preparation receipt canonical digest differs")
    if value["operation_id"] != operation_id:
        raise ComponentPreparationError("preparation belongs to another operation")
    if value["identity"] != expected_identity.payload() or value["identity_digest"] != expected_identity.digest:
        raise ComponentPreparationError("preparation component input identity differs")
    snapshot = value["source_snapshot_id"]
    if not isinstance(snapshot, str) or not snapshot.startswith("postgres:") or not snapshot[9:]:
        raise ComponentPreparationError("preparation snapshot identity is missing")
    seen = set()
    for field in ("output_refs", "validation_refs"):
        rows = value[field]
        if not isinstance(rows, list) or not rows:
            raise ComponentPreparationError("preparation output/evidence set is missing")
        for ref in rows:
            if not isinstance(ref, dict) or set(ref) != {"path", "sha256", "size"} or not isinstance(ref["path"], str):
                raise ComponentPreparationError("preparation file ref is invalid")
            if ref["path"].casefold() in seen or ref["path"].casefold() == "prepared-component.json":
                raise ComponentPreparationError("preparation file ref is duplicated/self-referential")
            seen.add(ref["path"].casefold())
            if type(ref["size"]) is not int or ref["size"] < 0 or _file_ref(root, ref["path"]) != ref:
                raise ComponentPreparationError("preparation file bytes differ")
    return value
