"""POSIX node endpoint for immutable streamed monthly release deployment."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sys
import tarfile
from typing import Any, BinaryIO, Mapping, Sequence

from .canonical import canonical_json_bytes
from .runtime_release_registration import (
    RuntimeReleaseRegistrationError,
    ensure_runtime_release_registration,
    read_runtime_release_registration,
)


REMOTE_DEPLOY_REQUEST_SCHEMA = "aistock_monthly_remote_deploy_request_v1"
REMOTE_DEPLOY_RESULT_SCHEMA = "aistock_monthly_remote_deploy_result_v1"
FRAME_MAGIC = b"AISTOCK_MONTHLY_DEPLOY_V1\n"
_REQUEST_FIELDS = {
    "schema_version",
    "operation_id",
    "attempt",
    "node_id",
    "allowed_parent",
    "candidate_root",
    "dataset_manifest_sha256",
    "files",
}
_FILE_FIELDS = {"path", "sha256", "size", "hardlink_source"}


class MonthlyRemoteDeployError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _absolute_path(value: object, *, label: str) -> Path:
    text = str(value or "")
    if "\x00" in text:
        raise MonthlyRemoteDeployError(f"{label} is not canonical")
    posix = PurePosixPath(text)
    path = Path(text)
    if not ((posix.is_absolute() and os.name != "nt") or path.is_absolute()):
        raise MonthlyRemoteDeployError(f"{label} must be absolute")
    if any(part in {"", ".", ".."} for part in posix.parts[1:]):
        raise MonthlyRemoteDeployError(f"{label} is not canonical")
    return path


def _plain_parent(path: Path) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise MonthlyRemoteDeployError("deployment parent is unavailable") from exc
    if path.is_symlink() or not resolved.is_dir():
        raise MonthlyRemoteDeployError("deployment parent must be a plain directory")
    return resolved


def _validated_request(value: Any) -> tuple[dict[str, Any], Path, Path]:
    if not isinstance(value, Mapping) or set(value) != _REQUEST_FIELDS:
        raise MonthlyRemoteDeployError("remote deployment request fields differ")
    request = dict(value)
    if request["schema_version"] != REMOTE_DEPLOY_REQUEST_SCHEMA:
        raise MonthlyRemoteDeployError("remote deployment request schema differs")
    if request["node_id"] not in {"wsl2-5080", "rdagent-node1"}:
        raise MonthlyRemoteDeployError("remote deployment node is invalid")
    operation = str(request["operation_id"] or "")
    attempt = request["attempt"]
    digest = str(request["dataset_manifest_sha256"] or "")
    if (
        re.fullmatch(r"dmr_[0-9a-f]{32}", operation) is None
        or type(attempt) is not int
        or attempt <= 0
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise MonthlyRemoteDeployError("remote deployment identity is invalid")
    allowed = _plain_parent(_absolute_path(request["allowed_parent"], label="allowed_parent"))
    target = _absolute_path(request["candidate_root"], label="candidate_root")
    if target.parent.resolve(strict=True) != allowed or target == allowed:
        raise MonthlyRemoteDeployError("remote deployment target parent differs")
    files = request["files"]
    if not isinstance(files, list) or not files:
        raise MonthlyRemoteDeployError("remote deployment file inventory is empty")
    paths: set[str] = set()
    rows: dict[str, Mapping[str, Any]] = {}
    for raw in files:
        if not isinstance(raw, Mapping) or set(raw) != _FILE_FIELDS:
            raise MonthlyRemoteDeployError("remote deployment file fields differ")
        relative = PurePosixPath(str(raw["path"] or ""))
        source = raw["hardlink_source"]
        if (
            relative.is_absolute()
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
            or relative.as_posix() in paths
            or type(raw["size"]) is not int
            or raw["size"] < 0
            or len(str(raw["sha256"])) != 64
            or any(character not in "0123456789abcdef" for character in str(raw["sha256"]))
            or (source is not None and not isinstance(source, str))
        ):
            raise MonthlyRemoteDeployError("remote deployment file identity is invalid")
        paths.add(relative.as_posix())
        rows[relative.as_posix()] = raw
    for relative, raw in rows.items():
        source = raw["hardlink_source"]
        if source is not None and (
            source not in rows
            or source == relative
            or rows[source]["hardlink_source"] is not None
            or rows[source]["sha256"] != raw["sha256"]
            or rows[source]["size"] != raw["size"]
        ):
            raise MonthlyRemoteDeployError("remote deployment hardlink identity is invalid")
    if "qe_dataset_manifest.json" not in paths:
        raise MonthlyRemoteDeployError("remote deployment manifest is absent")
    return request, allowed, target


def _tree_readback(target: Path, files: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    try:
        root = target.resolve(strict=True)
    except OSError as exc:
        raise MonthlyRemoteDeployError("remote deployment target is unavailable") from exc
    if target.is_symlink() or not root.is_dir():
        raise MonthlyRemoteDeployError("remote deployment target is not a plain directory")
    actual: set[str] = set()
    for base, directories, names in os.walk(root):
        directories.sort()
        names.sort()
        base_path = Path(base)
        if base_path.is_symlink() or any((base_path / name).is_symlink() for name in directories):
            raise MonthlyRemoteDeployError("remote deployment tree contains a linked directory")
        for name in names:
            path = base_path / name
            if path.is_symlink() or not path.is_file():
                raise MonthlyRemoteDeployError("remote deployment tree contains a non-file")
            actual.add(path.relative_to(root).as_posix())
    expected = {str(item["path"]) for item in files}
    if actual != expected:
        raise MonthlyRemoteDeployError("remote deployment file set differs")
    rows: list[dict[str, Any]] = []
    for item in files:
        path = root / PurePosixPath(str(item["path"]))
        if path.stat().st_size != item["size"] or _sha256(path) != item["sha256"]:
            raise MonthlyRemoteDeployError("remote deployment file bytes differ")
        hardlink_source = item.get("hardlink_source")
        if hardlink_source is not None and not os.path.samefile(
            path,
            root / PurePosixPath(str(hardlink_source)),
        ):
            raise MonthlyRemoteDeployError("remote deployment hardlink topology differs")
        rows.append({"path": item["path"], "sha256": item["sha256"], "size": item["size"]})
    return rows


def _result(
    request: Mapping[str, Any],
    *,
    status: str,
    files: Sequence[Mapping[str, Any]],
    bytes_transferred: int,
    registration: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema_version": REMOTE_DEPLOY_RESULT_SCHEMA,
        "status": status,
        "node_id": request["node_id"],
        "candidate_root": request["candidate_root"],
        "dataset_manifest_sha256": request["dataset_manifest_sha256"],
        "request_sha256": hashlib.sha256(canonical_json_bytes(request)).hexdigest(),
        "files": list(files),
        "bytes_transferred": bytes_transferred,
        "registration": dict(registration) if registration is not None else None,
    }


def readback(value: Any) -> dict[str, Any]:
    request, allowed, target = _validated_request(value)
    if not target.exists():
        return _result(
            request,
            status="ABSENT",
            files=(),
            bytes_transferred=0,
            registration=None,
        )
    rows = _tree_readback(target, request["files"])
    registration = read_runtime_release_registration(
        allowed_parent=allowed,
        candidate_root=target,
        dataset_manifest_sha256=str(request["dataset_manifest_sha256"]),
    )
    return _result(
        request,
        status="PASS" if registration is not None else "UNREGISTERED",
        files=rows,
        bytes_transferred=0,
        registration=registration.as_dict() if registration is not None else None,
    )


def register(value: Any) -> dict[str, Any]:
    request, allowed, target = _validated_request(value)
    if not target.exists():
        return _result(
            request,
            status="ABSENT",
            files=(),
            bytes_transferred=0,
            registration=None,
        )
    rows = _tree_readback(target, request["files"])
    registration = ensure_runtime_release_registration(
        allowed_parent=allowed,
        candidate_root=target,
        dataset_manifest_sha256=str(request["dataset_manifest_sha256"]),
    )
    return _result(
        request,
        status="PASS",
        files=rows,
        bytes_transferred=0,
        registration=registration.as_dict(),
    )


def deploy(value: Any, stream: BinaryIO) -> dict[str, Any]:
    request, allowed, target = _validated_request(value)
    if target.exists() or target.is_symlink():
        raise MonthlyRemoteDeployError("remote deployment target already exists")
    staging = allowed / (
        f".{target.name}.{request['operation_id']}.attempt-{request['attempt']}.deploying"
    )
    if staging.exists() or staging.is_symlink():
        raise MonthlyRemoteDeployError("remote deployment staging already exists")
    staging.mkdir(mode=0o750)
    by_path = {str(item["path"]): item for item in request["files"]}
    regular = {path: item for path, item in by_path.items() if item["hardlink_source"] is None}
    observed: set[str] = set()
    transferred = 0
    try:
        with tarfile.open(fileobj=stream, mode="r|*") as archive:
            for member in archive:
                relative = PurePosixPath(member.name)
                normalized = relative.as_posix()
                expected = regular.get(normalized)
                if (
                    expected is None
                    or normalized in observed
                    or not member.isfile()
                    or member.size != expected["size"]
                    or relative.is_absolute()
                    or any(part in {"", ".", ".."} for part in relative.parts)
                ):
                    raise MonthlyRemoteDeployError("remote deployment archive differs")
                source = archive.extractfile(member)
                if source is None:
                    raise MonthlyRemoteDeployError("remote deployment archive member is unreadable")
                destination = staging / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                written = 0
                with destination.open("xb") as handle:
                    for block in iter(lambda: source.read(1024 * 1024), b""):
                        handle.write(block)
                        digest.update(block)
                        written += len(block)
                    handle.flush()
                    os.fsync(handle.fileno())
                if written != expected["size"] or digest.hexdigest() != expected["sha256"]:
                    raise MonthlyRemoteDeployError("remote deployment archive bytes differ")
                transferred += written
                observed.add(normalized)
        if observed != set(regular):
            raise MonthlyRemoteDeployError("remote deployment archive is incomplete")
        for relative, item in by_path.items():
            source_relative = item["hardlink_source"]
            if source_relative is None:
                continue
            source = staging / PurePosixPath(source_relative)
            destination = staging / PurePosixPath(relative)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not source.is_file() or source.is_symlink():
                raise MonthlyRemoteDeployError("remote deployment hardlink source differs")
            os.link(source, destination)
        rows = _tree_readback(staging, request["files"])
        staging.rename(target)
        if os.name != "nt":
            descriptor = os.open(allowed, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        registration = ensure_runtime_release_registration(
            allowed_parent=allowed,
            candidate_root=target,
            dataset_manifest_sha256=str(request["dataset_manifest_sha256"]),
        )
    except BaseException:
        if staging.exists() and staging.parent.resolve(strict=True) == allowed:
            shutil.rmtree(staging)
        raise
    return _result(
        request,
        status="PASS",
        files=rows,
        bytes_transferred=transferred,
        registration=registration.as_dict(),
    )


def _read_canonical(stream: BinaryIO) -> Any:
    raw = stream.read()
    value = json.loads(raw.decode("utf-8"))
    if raw != canonical_json_bytes(value) + b"\n":
        raise MonthlyRemoteDeployError("remote deployment stdin is not canonical JSON")
    return value


def _read_frame(stream: BinaryIO) -> Any:
    if stream.readline() != FRAME_MAGIC:
        raise MonthlyRemoteDeployError("remote deployment frame magic differs")
    raw_length = stream.readline()
    try:
        length = int(raw_length.rstrip(b"\n"), 16)
    except ValueError as exc:
        raise MonthlyRemoteDeployError("remote deployment frame length is invalid") from exc
    if not 1 <= length <= 256 * 1024 * 1024:
        raise MonthlyRemoteDeployError("remote deployment frame length is out of bounds")
    raw = stream.read(length)
    value = json.loads(raw.decode("utf-8"))
    if raw != canonical_json_bytes(value):
        raise MonthlyRemoteDeployError("remote deployment frame is not canonical")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("readback", "deploy", "register"))
    args = parser.parse_args(argv)
    try:
        if args.mode == "readback":
            result = readback(_read_canonical(sys.stdin.buffer))
        elif args.mode == "register":
            result = register(_read_canonical(sys.stdin.buffer))
        else:
            result = deploy(_read_frame(sys.stdin.buffer), sys.stdin.buffer)
    except (
        OSError,
        ValueError,
        json.JSONDecodeError,
        tarfile.TarError,
        MonthlyRemoteDeployError,
        RuntimeReleaseRegistrationError,
    ) as exc:
        print(f"MONTHLY_REMOTE_DEPLOY_ERROR: {exc}", file=sys.stderr)
        return 2
    sys.stdout.buffer.write(canonical_json_bytes(result) + b"\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__: Sequence[str] = (
    "FRAME_MAGIC",
    "MonthlyRemoteDeployError",
    "REMOTE_DEPLOY_REQUEST_SCHEMA",
    "REMOTE_DEPLOY_RESULT_SCHEMA",
    "deploy",
    "register",
    "readback",
)
