"""Validate or atomically activate one repository-external QE dataset profile."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import tempfile

from backend.services.quantevolver.qe_active_dataset_profile import (
    ACTIVE_PROFILE_ENV,
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
    return {
        "status": "valid",
        "path": str(path),
        "profile_sha256": profile.profile_sha256,
        "generation": profile.generation,
        "release_id": profile.release_id,
        "cutoff": profile.cutoff.isoformat(),
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
        _validate(target)
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
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "validate":
        result = _validate(args.source)
    else:
        result = _activate(
            source=args.source,
            target=args.target,
            expected_source_sha256=args.expected_source_sha256,
            expected_current_sha256=args.expected_current_sha256,
        )
    for key, value in result.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
