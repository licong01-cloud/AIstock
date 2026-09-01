#!/usr/bin/env python3
"""Run the candidate QE/HMM consumer contract inside supervised WSL."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Sequence

from backend.services.dataset_release.candidate_consumer_smoke import (
    CandidateConsumerSmokeSpec,
    run_candidate_consumer_smoke,
)
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.resource_gate import ChildResourceCheckpoint


_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily-provider-uri", required=True)
    parser.add_argument("--minute-provider-uri", required=True)
    parser.add_argument("--index-h5-path", required=True)
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--stock-instrument", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--attempt-fence", required=True, type=int)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--release-digest", required=True)
    parser.add_argument("--staging-relative-path", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--max-h5-rows", required=True, type=int)
    parser.add_argument("--stage-timeout-seconds", required=True, type=int)
    parser.add_argument("--result-path", required=True)
    parser.add_argument("--control-root", required=True)
    parser.add_argument("--candidate-root", required=True)
    return parser


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    if not path.is_absolute():
        raise ValueError("result path must be absolute")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise ValueError("result already exists") from exc
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _plain_existing_root(path: Path) -> Path:
    requested = path.expanduser()
    if not requested.is_absolute():
        raise ValueError("root path must be absolute")
    _assert_plain_existing_chain(requested)
    resolved = requested.resolve(strict=True)
    if not resolved.is_dir():
        raise ValueError("root is not a directory")
    _assert_plain_node(resolved)
    return resolved


def _assert_plain_node(path: Path) -> None:
    metadata = os.lstat(path)
    if stat.S_ISLNK(metadata.st_mode) or bool(int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise ValueError("path traverses a symlink/reparse point")


def _assert_plain_existing_chain(path: Path) -> None:
    current = Path(path.anchor)
    if current.exists():
        _assert_plain_node(current)
    for part in path.parts[1:]:
        current = current / part
        if not current.exists():
            raise ValueError(f"path component is missing: {current}")
        _assert_plain_node(current)


def _assert_exact_path(value: str, expected: Path, *, must_exist: bool) -> Path:
    raw = Path(value)
    if not raw.is_absolute() or ".." in raw.parts:
        raise ValueError("path is not absolute/contained")
    _assert_plain_existing_chain(raw if must_exist else raw.parent)
    resolved = raw.resolve(strict=must_exist)
    expected_resolved = expected.resolve(strict=must_exist)
    if resolved != expected_resolved:
        raise ValueError("path differs from fenced identity")
    _assert_plain_existing_chain(resolved if must_exist else resolved.parent)
    return resolved


def _run(args: argparse.Namespace) -> int:
    control_root = _plain_existing_root(Path(args.control_root))
    candidate_root = _plain_existing_root(Path(args.candidate_root))
    expected_relative = f".staging/{args.attempt_id}/{args.attempt_fence}"
    expected_staging = candidate_root / ".staging" / args.attempt_id / str(args.attempt_fence)
    if args.staging_relative_path.replace("\\", "/") != expected_relative:
        raise ValueError("staging relative identity differs")
    staging = _assert_exact_path(str(expected_staging), expected_staging, must_exist=True)
    daily = _assert_exact_path(
        args.daily_provider_uri,
        staging / "daily_bin" / "qlib",
        must_exist=True,
    )
    minute = _assert_exact_path(
        args.minute_provider_uri,
        staging / "minute_bin" / "qlib",
        must_exist=True,
    )
    index_h5 = _assert_exact_path(
        args.index_h5_path,
        staging / "index_context" / "index_daily.h5",
        must_exist=True,
    )
    expected_result = (
        control_root
        / "attempt_runs"
        / f"{args.attempt_id}-{args.attempt_fence}"
        / args.execution_id
        / "semantic_result.json"
    )
    result_path = _assert_exact_path(args.result_path, expected_result, must_exist=False)
    if result_path.exists():
        raise ValueError("result already exists")
    checkpoint = ChildResourceCheckpoint(
        attempt_id=args.attempt_id,
        fence=args.attempt_fence,
        execution_id=args.execution_id,
    )
    spec = CandidateConsumerSmokeSpec(
        daily_provider_uri=str(daily),
        minute_provider_uri=str(minute),
        index_h5_path=index_h5,
        cutoff=date.fromisoformat(args.cutoff),
        stock_instrument=args.stock_instrument,
        expected_index_codes=tuple(item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS),
        profile=args.profile,
        run_id=args.run_id,
        attempt_id=args.attempt_id,
        attempt_fence=args.attempt_fence,
        release_id=args.release_id,
        release_digest=args.release_digest,
        staging_relative_path=args.staging_relative_path,
        max_h5_rows=args.max_h5_rows,
        stage_timeout_seconds=args.stage_timeout_seconds,
    )
    result = run_candidate_consumer_smoke(spec, checkpoint=checkpoint.checkpoint)
    _atomic_json(result_path, result)
    return 0


def _sanitized_error(args: argparse.Namespace | None, exc: Exception) -> dict[str, Any]:
    identity = {
        "run_id": str(getattr(args, "run_id", "invalid")),
        "attempt_id": str(getattr(args, "attempt_id", "invalid")),
        "execution_id": str(getattr(args, "execution_id", "invalid")),
    }
    context_ref = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": "dataset_release_child_error_envelope_v1",
        "error_code": "BLOCKED_CANDIDATE_CONSUMER_SMOKE",
        "exception_type": type(exc).__name__,
        "context_ref": context_ref,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args: argparse.Namespace | None = None
    try:
        args = _parser().parse_args(argv)
        return _run(args)
    except Exception as exc:
        sys.stderr.write(
            json.dumps(
                _sanitized_error(args, exc),
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
