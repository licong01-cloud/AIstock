"""Run one supervised, provider-free candidate build stage."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.dataset_release.build_stage import (  # noqa: E402
    BuildStageInvocation,
    run_build_stage,
)
from backend.services.dataset_release.cas_store import CASStore  # noqa: E402
from backend.services.dataset_release.profile import load_dataset_profile  # noqa: E402
from backend.services.dataset_release.resource_gate import (  # noqa: E402
    ChildResourceCheckpoint,
)


_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_STAGES = {
    "prepare": "build-prepare",
    "finalize-bins": "build-finalize-bins",
    "validate": "build-validate",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=tuple(_STAGES))
    parser.add_argument("--control-root", required=True)
    parser.add_argument("--candidate-root", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--plan-ref", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--attempt-fence", required=True, type=int)
    parser.add_argument("--pressure-rung", required=True, type=int)
    parser.add_argument("--stage-timeout-seconds", required=True, type=int)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--release-digest", required=True)
    parser.add_argument("--staging-relative-path", required=True)
    parser.add_argument("--result-path", required=True)
    parser.add_argument("--prerequisite-ref", action="append", default=[])
    return parser


def _run(args: argparse.Namespace) -> int:
    for value in (args.run_id, args.attempt_id, args.release_id):
        if not _IDENTITY.fullmatch(value):
            raise ValueError("build-stage identity is invalid")
    if args.attempt_fence <= 0 or not re.fullmatch(r"[0-9a-f]{64}", args.release_digest):
        raise ValueError("build-stage fence/release digest is invalid")
    profile = load_dataset_profile(args.profile)
    control_root = _plain_root(Path(args.control_root))
    candidate_root = _plain_root(Path(args.candidate_root))
    if control_root != Path(str(profile.control_root)).resolve(strict=True):
        raise ValueError("build-stage control root differs from profile")
    if candidate_root != Path(str(profile.candidate_root)).resolve(strict=True):
        raise ValueError("build-stage candidate root differs from profile")
    expected_staging = f".staging/{args.attempt_id}/{args.attempt_fence}"
    if args.staging_relative_path.replace("\\", "/") != expected_staging:
        raise ValueError("build-stage staging identity differs")
    staging = Path(os.path.abspath(os.fspath(candidate_root / Path(expected_staging))))
    if candidate_root not in staging.parents:
        raise ValueError("build-stage staging escapes candidate root")
    _assert_existing_plain_chain(staging)
    execution_id = _STAGES[args.stage]
    result = Path(os.path.abspath(os.fspath(Path(args.result_path).expanduser())))
    expected_result = (
        control_root
        / "attempt_runs"
        / f"{args.attempt_id}-{args.attempt_fence}"
        / execution_id
        / "semantic_result.json"
    )
    if result != expected_result or not result.parent.is_dir():
        raise ValueError("build-stage result path differs from supervised root")
    _assert_plain_chain(result.parent)
    if result.exists():
        raise FileExistsError("build-stage result already exists")
    prerequisites: dict[str, str] = {}
    for raw in args.prerequisite_ref:
        name, separator, reference = raw.partition("=")
        if (
            separator != "="
            or not _IDENTITY.fullmatch(name)
            or re.fullmatch(r"[0-9a-f]{64}", reference) is None
            or name in prerequisites
        ):
            raise ValueError("build-stage prerequisite is invalid")
        prerequisites[name] = reference
    cas = CASStore(control_root)
    plan = cas.get_json_bounded(args.plan_ref, max_bytes=32 * 1024 * 1024)
    if not isinstance(plan, Mapping):
        raise ValueError("build-stage plan is not an object")
    checkpoint = ChildResourceCheckpoint(
        attempt_id=args.attempt_id,
        fence=args.attempt_fence,
        execution_id=execution_id,
    )
    payload = run_build_stage(
        BuildStageInvocation(
            stage=args.stage,
            run_id=args.run_id,
            attempt_id=args.attempt_id,
            attempt_fence=args.attempt_fence,
            pressure_rung=args.pressure_rung,
            stage_timeout_seconds=args.stage_timeout_seconds,
            release_id=args.release_id,
            release_digest=args.release_digest,
            staging_relative_path=expected_staging,
            project_root=REPOSITORY_ROOT,
            candidate_root=candidate_root,
            staging_root=staging,
            profile=profile,
            cas=cas,
            plan=plan,
            prerequisites=prerequisites,
        ),
        checkpoint=checkpoint.checkpoint,
    )
    _atomic_json(result, payload)
    return 0


def _plain_root(path: Path) -> Path:
    requested = Path(os.path.abspath(os.fspath(path.expanduser())))
    _assert_plain_chain(requested)
    root = requested.resolve(strict=True)
    if not root.is_dir():
        raise ValueError("build-stage root is not a directory")
    _assert_plain_chain(root)
    return root


def _assert_plain_chain(path: Path) -> None:
    requested = Path(os.path.abspath(os.fspath(path.expanduser())))
    current = Path(requested.anchor)
    for part in requested.parts[1:]:
        current /= part
        value = current.lstat()
        if stat.S_ISLNK(value.st_mode) or (int(getattr(value, "st_file_attributes", 0)) & 0x0400):
            raise ValueError("build-stage path contains a link/reparse point")


def _assert_existing_plain_chain(path: Path) -> None:
    requested = Path(os.path.abspath(os.fspath(path.expanduser())))
    current = Path(requested.anchor)
    for part in requested.parts[1:]:
        current /= part
        if not current.exists():
            return
        value = current.lstat()
        if stat.S_ISLNK(value.st_mode) or (int(getattr(value, "st_file_attributes", 0)) & 0x0400):
            raise ValueError("build-stage path contains a link/reparse point")


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".partial", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _error(exc: BaseException) -> Mapping[str, Any]:
    return {
        "schema_version": "dataset_release_build_stage_error_v1",
        "error_code": str(getattr(exc, "code", "BUILD_STAGE_UNHANDLED_ERROR")),
        "exception_type": type(exc).__name__,
        "message_sha256": hashlib.sha256(str(exc).encode("utf-8", errors="replace")).hexdigest(),
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return _run(_parser().parse_args(argv))
    except BaseException as exc:
        sys.stderr.write(json.dumps(_error(exc), sort_keys=True, separators=(",", ":")) + "\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
