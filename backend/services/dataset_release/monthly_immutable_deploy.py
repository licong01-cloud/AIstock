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
import subprocess
import tarfile
import tempfile
from typing import Any, Callable, Mapping, Protocol, Sequence

from .canonical import canonical_json_bytes, ensure_sha256
from .errors import CanonicalizationError
from .monthly_official_adapters import (
    DeployExecution,
    NodeDeployment,
    StageWorkload,
)
from .monthly_unified import REQUIRED_NODES
from .monthly_worker import ProducerContext
from .monthly_remote_deploy import (
    FRAME_MAGIC,
    REMOTE_DEPLOY_REQUEST_SCHEMA,
    REMOTE_DEPLOY_RESULT_SCHEMA,
)
from .runtime_release_registration import (
    ensure_runtime_release_registration,
    expected_runtime_release_registration,
)


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
    hardlink_source: str | None = None


@dataclass(frozen=True, slots=True)
class NodeTransferReadback:
    node_id: str
    candidate_root: str
    files: tuple[tuple[str, str, int], ...]
    bytes_transferred: int
    registration: Mapping[str, Any]


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
    hardlink_sources: dict[tuple[int, int], str] = {}
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
            metadata = path.stat()
            hardlink_key = (int(metadata.st_dev), int(metadata.st_ino))
            hardlink_source = None
            if metadata.st_nlink > 1 and metadata.st_ino:
                hardlink_source = hardlink_sources.setdefault(hardlink_key, relative)
                if hardlink_source == relative:
                    hardlink_source = None
            files[relative] = ReleaseFile(
                relative_path=relative,
                source_path=path.resolve(strict=True),
                sha256=_sha256(path),
                size=metadata.st_size,
                hardlink_source=hardlink_source,
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

    local_scope = context.prior_receipts.get("LOCAL_VALIDATE", {}).get("scope")
    if not isinstance(local_scope, Mapping):
        raise MonthlyImmutableDeployError("LOCAL_VALIDATE scope is unavailable")
    closure_scope_ref = local_scope.get("release_closure_ref")
    if not isinstance(closure_scope_ref, Mapping):
        raise MonthlyImmutableDeployError("release closure reference is missing")
    closure_item = files.get("release_closure_receipt.json")
    if (
        closure_item is None
        or closure_item.sha256 != closure_scope_ref.get("sha256")
        or closure_item.size != closure_scope_ref.get("size")
    ):
        raise MonthlyImmutableDeployError("candidate-local release closure differs")
    closure = _read_canonical(closure_item.source_path, label="release closure")
    unsigned_closure = dict(closure)
    closure_digest = unsigned_closure.pop("canonical_sha256", None)
    if (
        unsigned_closure.get("schema_version") != "aistock_release_closure_v1"
        or hashlib.sha256(canonical_json_bytes(unsigned_closure)).hexdigest()
        != closure_digest
    ):
        raise MonthlyImmutableDeployError("release closure identity differs")

    closure_paths: set[str] = set()

    def register_closure_ref(raw: object, *, label: str) -> str:
        if not isinstance(raw, Mapping) or set(raw) != {"id", "sha256", "size"}:
            raise MonthlyImmutableDeployError(f"release closure {label} reference is invalid")
        try:
            digest = ensure_sha256(
                str(raw.get("sha256") or ""),
                field=f"release closure {label} sha256",
            )
        except CanonicalizationError as exc:
            raise MonthlyImmutableDeployError(
                f"release closure {label} hash is invalid"
            ) from exc
        size = raw.get("size")
        if type(size) is not int or size < 0:
            raise MonthlyImmutableDeployError(
                f"release closure {label} size is invalid"
            )
        relative = Path(str(raw.get("id") or ""))
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise MonthlyImmutableDeployError(f"release closure {label} path is invalid")
        normalized = relative.as_posix()
        observed = files.get(normalized)
        if (
            observed is None
            or observed.sha256 != digest
            or observed.size != size
        ):
            raise MonthlyImmutableDeployError(f"release closure {label} bytes differ")
        if normalized in closure_paths:
            raise MonthlyImmutableDeployError("release closure contains duplicate paths")
        closure_paths.add(normalized)
        return normalized

    closure_manifest = register_closure_ref(
        closure.get("dataset_manifest_ref"), label="dataset manifest"
    )
    if closure_manifest != "qe_dataset_manifest.json":
        raise MonthlyImmutableDeployError("release closure manifest path differs")
    closure_derived = closure.get("derived_asset_refs")
    if not isinstance(closure_derived, list) or not closure_derived:
        raise MonthlyImmutableDeployError("release closure derived references are empty")
    observed_derived = {
        register_closure_ref(raw, label="derived asset") for raw in closure_derived
    }
    if observed_derived != {"derived/derived_asset_registry.json", *asset_paths}:
        raise MonthlyImmutableDeployError("release closure derived file set differs")
    for field in (
        "consumer_contract_refs",
        "source_readiness_refs",
        "component_validation_refs",
    ):
        raw_refs = closure.get(field)
        if not isinstance(raw_refs, list) or not raw_refs:
            raise MonthlyImmutableDeployError(f"release closure {field} is empty")
        for raw in raw_refs:
            register_closure_ref(raw, label=field)
    register_closure_ref(closure.get("lineage_ref"), label="lineage")
    expected_paths = {
        "qe_dataset_manifest.json",
        "derived/derived_asset_registry.json",
        "release_closure_receipt.json",
        *component_paths,
        *asset_paths,
        *closure_paths,
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
    manifest_identity = _release_manifest_identity(expected)
    manifest = next(
        item for item in expected if item.relative_path == "qe_dataset_manifest.json"
    )
    expected_registration = expected_runtime_release_registration(
        candidate_root_name=Path(candidate_root).name,
        manifest_path=manifest.source_path,
        dataset_manifest_sha256=manifest_identity,
    )
    if dict(value.registration) != expected_registration.as_dict():
        raise MonthlyImmutableDeployError(
            f"node runtime release registration differs: {node_id}"
        )


def _release_manifest_identity(files: tuple[ReleaseFile, ...]) -> str:
    manifest = next(
        (item for item in files if item.relative_path == "qe_dataset_manifest.json"),
        None,
    )
    if manifest is None:
        raise MonthlyImmutableDeployError("deployment manifest is absent")
    try:
        value = json.loads(manifest.source_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyImmutableDeployError("deployment manifest is unreadable") from exc
    identity = str(value.get("dataset_manifest_sha256") or "") if isinstance(value, Mapping) else ""
    try:
        return ensure_sha256(identity, field="dataset_manifest_sha256")
    except CanonicalizationError as exc:
        raise MonthlyImmutableDeployError("deployment manifest identity is invalid") from exc


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
        registration = ensure_runtime_release_registration(
            allowed_parent=Path(candidate_root).parent,
            candidate_root=Path(candidate_root),
            dataset_manifest_sha256=_release_manifest_identity(files),
        )
        return NodeTransferReadback(
            self.node_id,
            candidate_root,
            rows,
            0,
            registration.as_dict(),
        )


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
            registration = ensure_runtime_release_registration(
                allowed_parent=parent,
                candidate_root=target,
                dataset_manifest_sha256=_release_manifest_identity(files),
            )
            return NodeTransferReadback(
                self.node_id,
                candidate_root,
                rows,
                0,
                registration.as_dict(),
            )
        staging = parent / (
            f".{target.name}.{context.operation_id}.attempt-{context.attempt}.deploying"
        )
        if staging.exists() or _is_link(staging):
            raise MonthlyImmutableDeployError(f"deployment staging already exists: {self.node_id}")
        staging.mkdir(parents=False, exist_ok=False)
        bytes_transferred = 0
        try:
            for item in files:
                destination = staging / Path(item.relative_path)
                destination.parent.mkdir(parents=True, exist_ok=True)
                if item.hardlink_source is None:
                    shutil.copyfile(item.source_path, destination)
                    bytes_transferred += item.size
                else:
                    linked_source = staging / Path(item.hardlink_source)
                    if not linked_source.is_file() or _is_link(linked_source):
                        raise MonthlyImmutableDeployError(
                            f"deployment hardlink source differs: {self.node_id}"
                        )
                    os.link(linked_source, destination)
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
        registration = ensure_runtime_release_registration(
            allowed_parent=parent,
            candidate_root=target,
            dataset_manifest_sha256=_release_manifest_identity(files),
        )
        return NodeTransferReadback(
            self.node_id,
            candidate_root,
            rows,
            bytes_transferred,
            registration.as_dict(),
        )


def _run_command(
    command: Sequence[str],
    *,
    payload: bytes,
    timeout_seconds: int,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        list(command),
        input=payload,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_seconds,
        check=False,
        shell=False,
    )


def _stream_command(
    command: Sequence[str],
    *,
    request: bytes,
    files: tuple[ReleaseFile, ...],
    timeout_seconds: int,
) -> subprocess.CompletedProcess[bytes]:
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        process = subprocess.Popen(
            list(command),
            stdin=subprocess.PIPE,
            stdout=stdout,
            stderr=stderr,
            shell=False,
        )
        try:
            if process.stdin is None:  # pragma: no cover - subprocess contract
                raise MonthlyImmutableDeployError("remote deployment stdin is unavailable")
            process.stdin.write(FRAME_MAGIC)
            process.stdin.write(f"{len(request):x}\n".encode("ascii"))
            process.stdin.write(request)
            with tarfile.open(fileobj=process.stdin, mode="w|") as archive:
                for item in files:
                    if item.hardlink_source is not None:
                        continue
                    info = tarfile.TarInfo(item.relative_path)
                    info.size = item.size
                    info.mode = 0o440
                    info.mtime = 0
                    with item.source_path.open("rb") as handle:
                        archive.addfile(info, handle)
            process.stdin.close()
            returncode = process.wait(timeout=timeout_seconds)
        except BaseException:
            process.kill()
            process.wait()
            raise
        finally:
            if process.stdin is not None and not process.stdin.closed:
                process.stdin.close()
        stdout.seek(0)
        stderr.seek(0)
        return subprocess.CompletedProcess(
            list(command),
            returncode,
            stdout=stdout.read(),
            stderr=stderr.read(),
        )


@dataclass(frozen=True, slots=True)
class ImmutableStreamingNodeTransport:
    """Stream one release to a POSIX node and atomically publish after SHA readback."""

    node_id: str
    allowed_parent: str
    command_prefix: tuple[str, ...]
    timeout_seconds: int = 6 * 60 * 60
    command_runner: Callable[..., subprocess.CompletedProcess[bytes]] = _run_command
    stream_runner: Callable[..., subprocess.CompletedProcess[bytes]] = _stream_command

    def __post_init__(self) -> None:
        if self.node_id not in {"wsl2-5080", "rdagent-node1"}:
            raise ValueError("streaming deployment node is invalid")
        if (
            not self.allowed_parent.startswith("/")
            or "\x00" in self.allowed_parent
            or ".." in Path(self.allowed_parent).parts
            or not self.command_prefix
            or any(not isinstance(value, str) or not value or "\x00" in value for value in self.command_prefix)
            or type(self.timeout_seconds) is not int
            or self.timeout_seconds < 60
        ):
            raise ValueError("streaming deployment configuration is invalid")

    def _request(
        self,
        context: ProducerContext,
        *,
        candidate_root: str,
        files: tuple[ReleaseFile, ...],
    ) -> dict[str, Any]:
        manifest = next(
            (item for item in files if item.relative_path == "qe_dataset_manifest.json"),
            None,
        )
        if manifest is None:
            raise MonthlyImmutableDeployError("streaming deployment manifest is absent")
        return {
            "schema_version": REMOTE_DEPLOY_REQUEST_SCHEMA,
            "operation_id": context.operation_id,
            "attempt": context.attempt,
            "node_id": self.node_id,
            "allowed_parent": self.allowed_parent,
            "candidate_root": candidate_root,
            "dataset_manifest_sha256": str(
                json.loads(manifest.source_path.read_text(encoding="utf-8")).get(
                    "dataset_manifest_sha256"
                )
                or ""
            ),
            "files": [
                {
                    "path": item.relative_path,
                    "sha256": item.sha256,
                    "size": item.size,
                    "hardlink_source": item.hardlink_source,
                }
                for item in files
            ],
        }

    def _result(
        self,
        completed: subprocess.CompletedProcess[bytes],
        *,
        request: Mapping[str, Any],
        statuses: set[str],
    ) -> Mapping[str, Any]:
        if completed.returncode != 0:
            error = completed.stderr.decode("utf-8", errors="replace")[-4_000:]
            raise MonthlyImmutableDeployError(
                f"streaming deployment command failed: {self.node_id}: {error}"
            )
        try:
            value = json.loads(completed.stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MonthlyImmutableDeployError(
                "streaming deployment result is invalid JSON"
            ) from exc
        expected = {
            "schema_version",
            "status",
            "node_id",
            "candidate_root",
            "dataset_manifest_sha256",
            "request_sha256",
            "files",
            "bytes_transferred",
            "registration",
        }
        if (
            not isinstance(value, Mapping)
            or completed.stdout != canonical_json_bytes(value) + b"\n"
            or set(value) != expected
            or value.get("schema_version") != REMOTE_DEPLOY_RESULT_SCHEMA
            or value.get("status") not in statuses
            or value.get("node_id") != self.node_id
            or value.get("candidate_root") != request["candidate_root"]
            or value.get("dataset_manifest_sha256")
            != request["dataset_manifest_sha256"]
            or value.get("request_sha256")
            != hashlib.sha256(canonical_json_bytes(request)).hexdigest()
            or type(value.get("bytes_transferred")) is not int
            or value["bytes_transferred"] < 0
            or not isinstance(value.get("files"), list)
            or (
                value.get("status") == "PASS"
                and not isinstance(value.get("registration"), Mapping)
            )
            or (
                value.get("status") in {"ABSENT", "UNREGISTERED"}
                and value.get("registration") is not None
            )
        ):
            raise MonthlyImmutableDeployError("streaming deployment result identity differs")
        return value

    def deploy(
        self,
        context: ProducerContext,
        *,
        candidate_root: str,
        files: tuple[ReleaseFile, ...],
    ) -> NodeTransferReadback:
        request = self._request(context, candidate_root=candidate_root, files=files)
        payload = canonical_json_bytes(request) + b"\n"
        probe = self.command_runner(
            (*self.command_prefix, "readback"),
            payload=payload,
            timeout_seconds=min(self.timeout_seconds, 30 * 60),
        )
        observed = self._result(
            probe,
            request=request,
            statuses={"PASS", "ABSENT", "UNREGISTERED"},
        )
        if observed["status"] == "ABSENT":
            if observed["files"] or observed["bytes_transferred"] != 0:
                raise MonthlyImmutableDeployError("absent node readback contains data")
            completed = self.stream_runner(
                (*self.command_prefix, "deploy"),
                request=canonical_json_bytes(request),
                files=files,
                timeout_seconds=self.timeout_seconds,
            )
            observed = self._result(completed, request=request, statuses={"PASS"})
        elif observed["status"] == "UNREGISTERED":
            registered = self.command_runner(
                (*self.command_prefix, "register"),
                payload=payload,
                timeout_seconds=min(self.timeout_seconds, 30 * 60),
            )
            observed = self._result(
                registered,
                request=request,
                statuses={"PASS"},
            )
        rows = tuple(
            (str(item.get("path") or ""), str(item.get("sha256") or ""), item.get("size"))
            for item in observed["files"]
            if isinstance(item, Mapping)
        )
        return NodeTransferReadback(
            node_id=self.node_id,
            candidate_root=candidate_root,
            files=rows,  # type: ignore[arg-type]
            bytes_transferred=int(observed["bytes_transferred"]),
            registration=dict(observed["registration"]),
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
                "runtime_registration": dict(readback.registration),
            }
            _write_exclusive(receipt, payload)
            nodes.append(
                NodeDeployment(
                    node_id=node_id,
                    candidate_root=target,
                    manifest_sha256=dataset_manifest_sha256,
                    relative_files=tuple(item.source_path for item in files),
                    deployment_receipt=receipt,
                    runtime_registration=dict(readback.registration),
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
    "ImmutableStreamingNodeTransport",
    "ImmutableMonthlyDeployExecutor",
    "MonthlyImmutableDeployError",
    "MonthlyNodeReleaseTransport",
    "NodeTransferReadback",
    "ReleaseFile",
)
