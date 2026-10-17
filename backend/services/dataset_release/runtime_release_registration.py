"""Create-exclusive node registry entries for immutable dataset releases."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping, Sequence

from .canonical import canonical_json_bytes


RUNTIME_RELEASE_REGISTRY_DIRECTORY = ".aistock-release-registry"
RUNTIME_RELEASE_REGISTRATION_SCHEMA = (
    "aistock_dataset_release_runtime_registration_v1"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


class RuntimeReleaseRegistrationError(RuntimeError):
    """A node release cannot be registered without weakening path identity."""


@dataclass(frozen=True, slots=True)
class RuntimeReleaseRegistration:
    relative_path: str
    sha256: str
    size: int
    registration_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size": self.size,
            "registration_sha256": self.registration_sha256,
        }


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(
        int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _manifest(manifest_path: Path, *, expected_identity: str) -> tuple[Mapping[str, Any], bytes]:
    if _SHA256.fullmatch(expected_identity) is None:
        raise RuntimeReleaseRegistrationError("dataset manifest identity is invalid")
    if _is_link(manifest_path) or not manifest_path.is_file():
        raise RuntimeReleaseRegistrationError("candidate manifest is unavailable")
    try:
        raw = manifest_path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeReleaseRegistrationError("candidate manifest is unreadable") from exc
    if not isinstance(value, Mapping) or raw != canonical_json_bytes(value) + b"\n":
        raise RuntimeReleaseRegistrationError("candidate manifest is not canonical")
    unsigned = dict(value)
    claimed = str(unsigned.pop("dataset_manifest_sha256", ""))
    if claimed != expected_identity or _sha256_bytes(canonical_json_bytes(unsigned)) != claimed:
        raise RuntimeReleaseRegistrationError("candidate manifest canonical identity differs")
    if not str(value.get("release_id") or "") or not str(value.get("cutoff_trade_date") or ""):
        raise RuntimeReleaseRegistrationError("candidate manifest release identity is incomplete")
    return value, raw


def _payload(
    *,
    candidate_root_name: str,
    manifest_path: Path,
    dataset_manifest_sha256: str,
) -> tuple[dict[str, Any], bytes]:
    if not candidate_root_name or Path(candidate_root_name).name != candidate_root_name:
        raise RuntimeReleaseRegistrationError("candidate root name is invalid")
    manifest, manifest_raw = _manifest(
        manifest_path,
        expected_identity=dataset_manifest_sha256,
    )
    value: dict[str, Any] = {
        "schema_version": RUNTIME_RELEASE_REGISTRATION_SCHEMA,
        "candidate_root_name": candidate_root_name,
        "release_id": str(manifest["release_id"]),
        "cutoff_trade_date": str(manifest["cutoff_trade_date"]),
        "dataset_manifest_sha256": dataset_manifest_sha256,
        "manifest_file_sha256": _sha256_bytes(manifest_raw),
        "manifest_size": len(manifest_raw),
    }
    value["registration_sha256"] = _sha256_bytes(canonical_json_bytes(value))
    return value, canonical_json_bytes(value) + b"\n"


def expected_runtime_release_registration(
    *,
    candidate_root_name: str,
    manifest_path: Path,
    dataset_manifest_sha256: str,
) -> RuntimeReleaseRegistration:
    value, raw = _payload(
        candidate_root_name=candidate_root_name,
        manifest_path=manifest_path,
        dataset_manifest_sha256=dataset_manifest_sha256,
    )
    return RuntimeReleaseRegistration(
        relative_path=(
            f"{RUNTIME_RELEASE_REGISTRY_DIRECTORY}/{dataset_manifest_sha256}.json"
        ),
        sha256=_sha256_bytes(raw),
        size=len(raw),
        registration_sha256=str(value["registration_sha256"]),
    )


def _registry_root(allowed_parent: Path, *, create: bool) -> Path:
    try:
        parent = allowed_parent.resolve(strict=True)
    except OSError as exc:
        raise RuntimeReleaseRegistrationError("release parent is unavailable") from exc
    if _is_link(allowed_parent) or not parent.is_dir():
        raise RuntimeReleaseRegistrationError("release parent must be a plain directory")
    registry = parent / RUNTIME_RELEASE_REGISTRY_DIRECTORY
    if not registry.exists() and create:
        try:
            registry.mkdir(mode=0o750)
        except FileExistsError:
            pass
        if os.name != "nt":
            descriptor = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    if _is_link(registry) or not registry.is_dir() or registry.resolve(strict=True).parent != parent:
        raise RuntimeReleaseRegistrationError("release registry must be a plain directory")
    return registry.resolve(strict=True)


def ensure_runtime_release_registration(
    *,
    allowed_parent: Path,
    candidate_root: Path,
    dataset_manifest_sha256: str,
) -> RuntimeReleaseRegistration:
    if _is_link(candidate_root) or not candidate_root.is_dir():
        raise RuntimeReleaseRegistrationError("candidate root must be a plain directory")
    parent = allowed_parent.resolve(strict=True)
    root = candidate_root.resolve(strict=True)
    if root.parent != parent:
        raise RuntimeReleaseRegistrationError("candidate root differs from release parent")
    value, raw = _payload(
        candidate_root_name=root.name,
        manifest_path=root / "qe_dataset_manifest.json",
        dataset_manifest_sha256=dataset_manifest_sha256,
    )
    registry = _registry_root(parent, create=True)
    path = registry / f"{dataset_manifest_sha256}.json"
    try:
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        if os.name != "nt":
            descriptor = os.open(registry, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    except FileExistsError:
        if _is_link(path) or not path.is_file() or path.read_bytes() != raw:
            raise RuntimeReleaseRegistrationError("existing release registration differs") from None
    return RuntimeReleaseRegistration(
        relative_path=path.relative_to(parent).as_posix(),
        sha256=_sha256_bytes(raw),
        size=len(raw),
        registration_sha256=str(value["registration_sha256"]),
    )


def read_runtime_release_registration(
    *,
    allowed_parent: Path,
    candidate_root: Path,
    dataset_manifest_sha256: str,
) -> RuntimeReleaseRegistration | None:
    if _is_link(candidate_root) or not candidate_root.is_dir():
        raise RuntimeReleaseRegistrationError("candidate root must be a plain directory")
    parent = allowed_parent.resolve(strict=True)
    root = candidate_root.resolve(strict=True)
    if root.parent != parent:
        raise RuntimeReleaseRegistrationError("candidate root differs from release parent")
    value, raw = _payload(
        candidate_root_name=root.name,
        manifest_path=root / "qe_dataset_manifest.json",
        dataset_manifest_sha256=dataset_manifest_sha256,
    )
    registry_path = parent / RUNTIME_RELEASE_REGISTRY_DIRECTORY
    if not registry_path.exists():
        return None
    registry = _registry_root(parent, create=False)
    path = registry / f"{dataset_manifest_sha256}.json"
    if not path.exists():
        return None
    if _is_link(path) or not path.is_file() or path.read_bytes() != raw:
        raise RuntimeReleaseRegistrationError("release registration bytes differ")
    return RuntimeReleaseRegistration(
        relative_path=path.relative_to(parent).as_posix(),
        sha256=_sha256_bytes(raw),
        size=len(raw),
        registration_sha256=str(value["registration_sha256"]),
    )


__all__: Sequence[str] = (
    "RUNTIME_RELEASE_REGISTRATION_SCHEMA",
    "RUNTIME_RELEASE_REGISTRY_DIRECTORY",
    "RuntimeReleaseRegistration",
    "RuntimeReleaseRegistrationError",
    "ensure_runtime_release_registration",
    "expected_runtime_release_registration",
    "read_runtime_release_registration",
)
