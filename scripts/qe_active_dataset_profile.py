"""Validate or atomically activate one repository-external QE dataset profile."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import posixpath
import sys
import tempfile
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.quantevolver.qe_active_dataset_profile import (  # noqa: E402
    ACTIVE_PROFILE_ENV,
    QEActiveDatasetProfile,
    load_active_qe_profile,
    validate_controller_snapshot,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_link_or_junction(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _validate(path: Path) -> dict[str, object]:
    profile = _load_validated_profile(path)
    return {
        "status": "valid",
        "path": str(path),
        "profile_sha256": profile.profile_sha256,
        "generation": profile.generation,
        "release_id": profile.release_id,
        "cutoff": profile.cutoff.isoformat(),
    }


def _load_validated_profile(path: Path) -> QEActiveDatasetProfile:
    previous = os.environ.get(ACTIVE_PROFILE_ENV)
    os.environ[ACTIVE_PROFILE_ENV] = str(path)
    try:
        profile = load_active_qe_profile()
    finally:
        if previous is None:
            os.environ.pop(ACTIVE_PROFILE_ENV, None)
        else:
            os.environ[ACTIVE_PROFILE_ENV] = previous
    if profile is None:  # pragma: no cover - environment is set above
        raise RuntimeError("profile validation unexpectedly resolved legacy mode")
    validate_controller_snapshot(profile)
    return profile


def _runtime_bindings(path: Path, *, node_id: str | None = None) -> dict[str, Any]:
    """Derive mutable runtime bindings from one validated active profile.

    The profile remains the only release identity authority.  Runtime cache/state
    locations such as QE_FACTOR_DATA_DIR are intentionally not emitted here.
    """

    profile = _load_validated_profile(path)
    raw_nodes = profile.raw["node_bindings"]
    selected_ids = [node_id] if node_id else sorted(raw_nodes)
    nodes: dict[str, dict[str, object]] = {}
    for selected_id in selected_ids:
        raw = raw_nodes.get(selected_id)
        if not isinstance(raw, Mapping):
            raise RuntimeError(f"node is absent from active profile: {selected_id}")
        candidate_root = str(raw["candidate_root"]).rstrip("/")
        day = posixpath.join(candidate_root, "components/daily_bin_candidate")
        minute = posixpath.join(candidate_root, "components/minute_bin_candidate")
        factor = posixpath.join(candidate_root, "components/factor_h5_static_candidate_v2")
        nodes[selected_id] = {
            "candidate_root": candidate_root,
            "factor_data_dir": factor,
            "qlib_data_path": day,
            "qlib_minute_path": minute,
            "environment": {
                "QE_DATASET_IDENTITY_ROOTS": candidate_root,
                "QE_QLIB_DATA_PATH": day,
                "QLIB_DATA_PATH_WSL": day,
                "QLIB_DAY_DATA": day,
                "QLIB_MINUTE_PATH_WSL": minute,
                "QLIB_MINUTE_DATA": minute,
                "RDAGENT_FACTOR_DATA_WSL": factor,
            },
        }
    return {
        "schema_version": "aistock_qe_runtime_bindings_v1",
        "profile_path": str(path),
        "profile_sha256": profile.profile_sha256,
        "generation": profile.generation,
        "release_id": profile.release_id,
        "cutoff": profile.cutoff.isoformat(),
        "nodes": nodes,
    }


def _audit_runtime_binding(
    path: Path,
    *,
    node_id: str,
    actual: Mapping[str, str],
) -> dict[str, object]:
    plan = _runtime_bindings(path, node_id=node_id)
    expected = plan["nodes"][node_id]
    expected_primary = {
        key: str(expected[key])
        for key in ("factor_data_dir", "qlib_data_path", "qlib_minute_path")
    }
    mismatches = {
        key: {"expected": expected_primary[key], "actual": str(actual.get(key) or "")}
        for key in expected_primary
        if str(actual.get(key) or "") != expected_primary[key]
    }
    if mismatches:
        raise RuntimeError(
            "runtime bindings differ from active profile: "
            + json.dumps(mismatches, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        )
    return {
        "status": "runtime_bindings_match",
        "node_id": node_id,
        "profile_sha256": plan["profile_sha256"],
        "generation": plan["generation"],
        "release_id": plan["release_id"],
    }


def _activate(
    *,
    source: Path,
    target: Path,
    expected_source_sha256: str,
    expected_current_sha256: str | None,
) -> dict[str, object]:
    source_result = _validate(source)
    if source_result["profile_sha256"] != expected_source_sha256:
        raise RuntimeError("source profile digest differs from --expected-source-sha256")
    if not target.is_absolute() or source.resolve() == target.resolve(strict=False):
        raise RuntimeError("activation target must be a different absolute path")
    project_root = Path(__file__).resolve().parents[1]
    try:
        target.resolve(strict=False).relative_to(project_root)
    except ValueError:
        pass
    else:
        raise RuntimeError("activation target must be outside the repository")
    if _is_link_or_junction(target.parent) or not target.parent.exists() or not target.parent.is_dir():
        raise RuntimeError("activation target parent must be an existing regular directory")
    if _is_link_or_junction(target):
        raise RuntimeError("activation target must not be a symlink or junction")
    if target.exists():
        if not target.is_file():
            raise RuntimeError("activation target must be a regular file")
        if expected_current_sha256 is None:
            raise RuntimeError("existing target requires --expected-current-sha256")
        if _sha256(target) != expected_current_sha256:
            raise RuntimeError("current target digest differs from --expected-current-sha256")
    elif expected_current_sha256 is not None:
        raise RuntimeError("--expected-current-sha256 was supplied but target is absent")
    payload = source.read_bytes()
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if _sha256(temp_path) != expected_source_sha256:
            raise RuntimeError("temporary profile digest differs before replace")
        os.replace(temp_path, target)
        try:
            directory_fd = os.open(target.parent, os.O_RDONLY)
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    result = _validate(target)
    if result["profile_sha256"] != expected_source_sha256:
        raise RuntimeError("activated target digest differs after replace")
    return {**result, "status": "activated"}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--source", type=Path, required=True)
    activate = subparsers.add_parser("activate")
    activate.add_argument("--source", type=Path, required=True)
    activate.add_argument("--target", type=Path, required=True)
    activate.add_argument("--expected-source-sha256", required=True)
    activate.add_argument("--expected-current-sha256")
    bindings = subparsers.add_parser("runtime-bindings")
    bindings.add_argument("--source", type=Path, required=True)
    bindings.add_argument("--node-id")
    audit = subparsers.add_parser("audit-runtime-bindings")
    audit.add_argument("--source", type=Path, required=True)
    audit.add_argument("--node-id", required=True)
    audit.add_argument("--factor-data-dir", required=True)
    audit.add_argument("--qlib-data-path", required=True)
    audit.add_argument("--qlib-minute-path", required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "validate":
        result = _validate(args.source)
    elif args.command == "activate":
        result = _activate(
            source=args.source,
            target=args.target,
            expected_source_sha256=args.expected_source_sha256,
            expected_current_sha256=args.expected_current_sha256,
        )
    elif args.command == "runtime-bindings":
        result = _runtime_bindings(args.source, node_id=args.node_id)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        return
    else:
        result = _audit_runtime_binding(
            args.source,
            node_id=args.node_id,
            actual={
                "factor_data_dir": args.factor_data_dir,
                "qlib_data_path": args.qlib_data_path,
                "qlib_minute_path": args.qlib_minute_path,
            },
        )
    for key, value in result.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
