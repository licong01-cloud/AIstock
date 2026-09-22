"""Immutable node deployment for one sealed monthly candidate.

The executor inventories one controller candidate once, then requires every
node transport to read back the exact same relative file set, byte count and
SHA-256.  Transports are code-owned capabilities; neither API requests nor
operation state may provide commands or change an existing target directory.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
from typing import Mapping, Protocol, Sequence

from .canonical import canonical_json_bytes, ensure_sha256
from .monthly_official_adapters import (
    DeployExecution,
    NodeDeployment,
    StageWorkload,
)
from .monthly_unified import REQUIRED_NODES
from .monthly_worker import ProducerContext


DEPLOYMENT_RECEIPT_SCHEMA = "aistock_monthly_node_deployment_receipt_v1"
DERIVED_ASSET_REGISTRY_SCHEMA = "aistock_dataset_derived_asset_registry_v1"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


class MonthlyImmutableDeployError(RuntimeError):
    """A target is mutable, unsafe, incomplete or byte-inconsistent."""


@dataclass(frozen=True, slots=True)
class ReleaseFile:
    relative_path: str
    source_path: Path
    sha256: str
    size: int


@dataclass(frozen=True, slots=True)
class NodeTransferReadback:
    node_id: str
    candidate_root: str
    files: tuple[tuple[str, str, int], ...]
    bytes_transferred: int


class MonthlyNodeReleaseTransport(Protocol):
    node_id: str

    def deploy(
        self,
        context: ProducerContext,
        *,
        candidate_root: str,
        files: tuple[ReleaseFile, ...],
    ) -> NodeTransferReadback: ...


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


def _write_exclusive(path: Path, value: Mapping[str, object]) -> None:
    raw = canonical_json_bytes(value) + b"\n"
    with path.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def _read_canonical(path: Path, *, label: str) -> Mapping[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyImmutableDeployError(f"{label} is unreadable") from exc
    if not isinstance(value, Mapping) or raw != canonical_json_bytes(value) + b"\n":
        raise MonthlyImmutableDeployError(f"{label} must be canonical JSON")
    return value


def _plain_candidate_root(context: ProducerContext) -> Path:
    candidate = Path(str(context.plan.get("candidate_root") or ""))
    if not candidate.is_absolute():
        raise MonthlyImmutableDeployError("controller candidate root must be absolute")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise MonthlyImmutableDeployError("controller candidate root is unavailable") from exc
    if _is_link(candidate) or not resolved.is_dir():
        raise MonthlyImmutableDeployError("controller candidate root must be a plain directory")
    return resolved


def _manifest_identity(value: Mapping[str, object]) -> str:
    unsigned = dict(value)
    claimed = str(unsigned.pop("dataset_manifest_sha256", ""))
    ensure_sha256(claimed, field="dataset_manifest_sha256")
    if hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest() != claimed:
        raise MonthlyImmutableDeployError("dataset manifest canonical identity differs")
    return claimed


def _inventory(
    context: ProducerContext,
    *,
    dataset_manifest_sha256: str,
) -> tuple[Path, tuple[ReleaseFile, ...]]:
    root = _plain_candidate_root(context)
    manifest_path = root / "qe_dataset_manifest.json"
    manifest = _read_canonical(manifest_path, label="dataset manifest")
    if _manifest_identity(manifest) != dataset_manifest_sha256:
        raise MonthlyImmutableDeployError("dataset manifest identity differs")
    components = manifest.get("components")
    if not isinstance(components, Mapping) or not components:
        raise MonthlyImmutableDeployError("dataset manifest component inventory is empty")

    files: dict[str, ReleaseFile] = {}
    for base, directories, names in os.walk(root):
        base_path = Path(base)
        if _is_link(base_path):
            raise MonthlyImmutableDeployError("candidate inventory contains a linked directory")
        directories.sort()
        names.sort()
        for directory in directories:
            if _is_link(base_path / directory):
                raise MonthlyImmutableDeployError("candidate inventory contains a linked directory")
        for name in names:
            path = base_path / name
            if _is_link(path) or not path.is_file():
                raise MonthlyImmutableDeployError("candidate inventory contains a non-regular file")
            relative = path.relative_to(root).as_posix()
            files[relative] = ReleaseFile(
                relative_path=relative,
                source_path=path.resolve(strict=True),
                sha256=_sha256(path),
                size=path.stat().st_size,
            )
    if "qe_dataset_manifest.json" not in files:
        raise MonthlyImmutableDeployError("candidate manifest is absent from release inventory")
    component_paths: set[str] = set()
    for name, raw in components.items():
        if not isinstance(raw, Mapping):
            raise MonthlyImmutableDeployError(f"manifest component is invalid: {name}")
        relative = str(raw.get("path") or "")
        if not relative or relative in component_paths:
            raise MonthlyImmutableDeployError("manifest component paths are ambiguous")
        component_paths.add(relative)
        observed = files.get(relative)
        if (
            observed is None
            or observed.sha256 != str(raw.get("sha256") or "")
            or observed.size != raw.get("size")
        ):
            raise MonthlyImmutableDeployError(f"manifest component bytes differ: {name}")

    derive_scope = context.prior_receipts.get("DERIVE", {}).get("scope")
    if not isinstance(derive_scope, Mapping):
        raise MonthlyImmutableDeployError("DERIVE scope is unavailable")
    registry_ref = derive_scope.get("derived_asset_registry_ref")
    if not isinstance(registry_ref, Mapping):
        raise MonthlyImmutableDeployError("derived asset registry reference is missing")
    registry_item = files.get("derived/derived_asset_registry.json")
    if (
        registry_item is None
        or registry_item.sha256 != registry_ref.get("sha256")
        or registry_item.size != registry_ref.get("size")
    ):
        raise MonthlyImmutableDeployError("candidate-local derived registry differs")
    registry = _read_canonical(
        registry_item.source_path,
        label="derived asset registry",
    )
    if (
        registry.get("schema_version") != DERIVED_ASSET_REGISTRY_SCHEMA
        or registry.get("source_dataset_manifest_sha256") != dataset_manifest_sha256
    ):
        raise MonthlyImmutableDeployError("derived asset registry identity differs")
    assets = registry.get("assets")
    if not isinstance(assets, list) or not assets:
        raise MonthlyImmutableDeployError("derived asset registry is empty")
    asset_paths: set[str] = set()
    registry_root = registry_item.source_path.parent
    for raw in assets:
        if not isinstance(raw, Mapping):
            raise MonthlyImmutableDeployError("derived asset registry row is invalid")
        registry_relative = Path(str(raw.get("path") or ""))
        if (
            registry_relative.is_absolute()
            or not registry_relative.parts
            or ".." in registry_relative.parts
        ):
            raise MonthlyImmutableDeployError("derived asset registry path is invalid")
        try:
            asset_path = (registry_root / registry_relative).resolve(strict=True)
        except OSError as exc:
            raise MonthlyImmutableDeployError("derived asset registry path is unavailable") from exc
        if _is_link(registry_root / registry_relative) or not asset_path.is_relative_to(root):
            raise MonthlyImmutableDeployError("derived asset registry path escapes the candidate")
        relative = asset_path.relative_to(root).as_posix()
        if relative in asset_paths:
            raise MonthlyImmutableDeployError("derived asset paths are ambiguous")
        asset_paths.add(relative)
        observed = files.get(relative)
        if (
            observed is None
            or observed.sha256 != raw.get("sha256")
            or observed.size != raw.get("size")
        ):
            raise MonthlyImmutableDeployError("derived asset registry bytes differ")
    expected_paths = {
        "qe_dataset_manifest.json",
        "derived/derived_asset_registry.json",
        *component_paths,
        *asset_paths,
    }
    if set(files) != expected_paths:
        raise MonthlyImmutableDeployError("candidate contains unregistered release files")
    return root, tuple(files[name] for name in sorted(files))


def _normalized_readback(
    value: NodeTransferReadback,
    *,
    node_id: str,
    candidate_root: str,
    expected: tuple[ReleaseFile, ...],
) -> None:
    if (
        value.node_id != node_id
        or value.candidate_root != candidate_root
        or type(value.bytes_transferred) is not int
        or value.bytes_transferred < 0
    ):
        raise MonthlyImmutableDeployError(f"node deployment identity differs: {node_id}")
    expected_rows = tuple(
        (item.relative_path, item.sha256, item.size) for item in expected
    )
    if value.files != expected_rows:
        raise MonthlyImmutableDeployError(f"node deployment bytes differ: {node_id}")


def _tree_readback(
    root: Path,
    *,
    files: tuple[ReleaseFile, ...],
    label: str,
) -> tuple[tuple[str, str, int], ...]:
    try:
        resolved = root.resolve(strict=True)
    except OSError as exc:
        raise MonthlyImmutableDeployError(f"{label} target is unavailable") from exc
    if _is_link(root) or not resolved.is_dir():
        raise MonthlyImmutableDeployError(f"{label} target must be a plain directory")
    actual: set[str] = set()
    for base, directories, names in os.walk(resolved):
        base_path = Path(base)
        if _is_link(base_path):
            raise MonthlyImmutableDeployError(f"{label} target contains a link")
        for directory in directories:
            if _is_link(base_path / directory):
                raise MonthlyImmutableDeployError(f"{label} target contains a link")
        for name in names:
            path = base_path / name
            if _is_link(path) or not path.is_file():
                raise MonthlyImmutableDeployError(f"{label} target contains a non-file")
            actual.add(path.relative_to(resolved).as_posix())
    expected = {item.relative_path for item in files}
    if actual != expected:
        raise MonthlyImmutableDeployError(f"{label} target file set differs")
    rows = []
    for item in files:
        path = resolved / Path(item.relative_path)
        if path.stat().st_size != item.size or _sha256(path) != item.sha256:
            raise MonthlyImmutableDeployError(f"{label} target bytes differ")
        rows.append((item.relative_path, item.sha256, item.size))
    return tuple(rows)


@dataclass(frozen=True, slots=True)
class ExistingTreeNodeTransport:
    """Read back the already-built controller candidate without copying it."""

    node_id: str = "controller"

    def deploy(
        self,
        context: ProducerContext,
        *,
        candidate_root: str,
        files: tuple[ReleaseFile, ...],
    ) -> NodeTransferReadback:
        del context
        rows = _tree_readback(
            Path(candidate_root),
            files=files,
            label="controller candidate",
        )
        return NodeTransferReadback(self.node_id, candidate_root, rows, 0)


@dataclass(frozen=True, slots=True)
class ImmutableFilesystemNodeTransport:
    """Copy one byte inventory to a new target on an accessible filesystem."""

    node_id: str
    allowed_parent: Path

    def __post_init__(self) -> None:
        parent = self.allowed_parent.resolve(strict=True)
        if _is_link(self.allowed_parent) or not parent.is_dir():
            raise ValueError("deployment parent must be a plain existing directory")

    def deploy(
        self,
        context: ProducerContext,
        *,
        candidate_root: str,
        files: tuple[ReleaseFile, ...],
    ) -> NodeTransferReadback:
        target = Path(candidate_root)
        parent = self.allowed_parent.resolve(strict=True)
        if not target.is_absolute() or target.parent.resolve(strict=True) != parent:
            raise MonthlyImmutableDeployError(f"deployment target parent differs: {self.node_id}")
        if _is_link(target):
            raise MonthlyImmutableDeployError(f"deployment target is linked: {self.node_id}")
        if target.exists():
            rows = _tree_readback(
                target,
                files=files,
                label=f"{self.node_id} resumed deployment",
            )
            return NodeTransferReadback(self.node_id, candidate_root, rows, 0)
        staging = parent / (
            f".{target.name}.{context.operation_id}.attempt-{context.attempt}.deploying"
        )
        if staging.exists() or _is_link(staging):
            raise MonthlyImmutableDeployError(f"deployment staging already exists: {self.node_id}")
        staging.mkdir(parents=False, exist_ok=False)
        try:
            for item in files:
                destination = staging / Path(item.relative_path)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(item.source_path, destination)
                with destination.open("rb+") as handle:
                    handle.flush()
                    os.fsync(handle.fileno())
                if destination.stat().st_size != item.size or _sha256(destination) != item.sha256:
                    raise MonthlyImmutableDeployError(
                        f"deployment copy readback differs: {self.node_id}"
                    )
            staging.rename(target)
        except BaseException:
            if staging.exists():
                shutil.rmtree(staging)
            raise
        rows = tuple(
            (item.relative_path, item.sha256, item.size) for item in files
        )
        return NodeTransferReadback(
            self.node_id,
            candidate_root,
            rows,
            sum(item.size for item in files),
        )


@dataclass(frozen=True, slots=True)
class ImmutableMonthlyDeployExecutor:
    artifact_root: Path
    transports: Mapping[str, MonthlyNodeReleaseTransport]

    def __post_init__(self) -> None:
        if set(self.transports) != set(REQUIRED_NODES) or any(
            transport.node_id != node_id
            for node_id, transport in self.transports.items()
        ):
            raise ValueError("monthly deployment transports must cover exact node registry")
        root = self.artifact_root.resolve(strict=True)
        if _is_link(self.artifact_root) or not root.is_dir():
            raise ValueError("monthly deployment artifact root is unavailable")

    def execute(
        self,
        context: ProducerContext,
        *,
        dataset_manifest_sha256: str,
    ) -> DeployExecution:
        if context.stage != "DEPLOY":
            raise MonthlyImmutableDeployError("deployment executor received another stage")
        ensure_sha256(dataset_manifest_sha256, field="dataset_manifest_sha256")
        source_root, files = _inventory(
            context,
            dataset_manifest_sha256=dataset_manifest_sha256,
        )
        targets = context.plan.get("target_roots")
        if not isinstance(targets, Mapping) or set(targets) != set(REQUIRED_NODES):
            raise MonthlyImmutableDeployError("deployment target registry is incomplete")
        if str(targets["controller"]) != str(source_root):
            raise MonthlyImmutableDeployError("controller deployment target differs")
        receipt_root = (
            self.artifact_root
            / "monthly"
            / context.operation_id
            / "deploy-transfer"
            / f"attempt-{context.attempt}"
        )
        receipt_root.mkdir(parents=True, exist_ok=False)
        nodes: list[NodeDeployment] = []
        bytes_transferred = 0
        for node_id in REQUIRED_NODES:
            target = str(targets[node_id])
            readback = self.transports[node_id].deploy(
                context,
                candidate_root=target,
                files=files,
            )
            _normalized_readback(
                readback,
                node_id=node_id,
                candidate_root=target,
                expected=files,
            )
            bytes_transferred += readback.bytes_transferred
            receipt = receipt_root / f"{node_id}.json"
            payload = {
                "schema_version": DEPLOYMENT_RECEIPT_SCHEMA,
                "status": "PASS",
                "node_id": node_id,
                "candidate_root": target,
                "dataset_manifest_sha256": dataset_manifest_sha256,
                "file_count": len(files),
                "total_bytes": sum(item.size for item in files),
                "files": [
                    {"path": path, "sha256": digest, "size": size}
                    for path, digest, size in readback.files
                ],
                "overwrite_performed": False,
                "inplace_update_performed": False,
            }
            _write_exclusive(receipt, payload)
            nodes.append(
                NodeDeployment(
                    node_id=node_id,
                    candidate_root=target,
                    manifest_sha256=dataset_manifest_sha256,
                    relative_files=tuple(item.source_path for item in files),
                    deployment_receipt=receipt,
                )
            )
        return DeployExecution(
            nodes=tuple(nodes),
            input_artifacts=(
                source_root / "qe_dataset_manifest.json",
                source_root / "derived" / "derived_asset_registry.json",
            ),
            workload=StageWorkload(
                bytes_transferred=bytes_transferred,
            ),
        )


__all__: Sequence[str] = (
    "DEPLOYMENT_RECEIPT_SCHEMA",
    "ExistingTreeNodeTransport",
    "ImmutableFilesystemNodeTransport",
    "ImmutableMonthlyDeployExecutor",
    "MonthlyImmutableDeployError",
    "MonthlyNodeReleaseTransport",
    "NodeTransferReadback",
    "ReleaseFile",
)
