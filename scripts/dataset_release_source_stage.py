"""Supervised read-only source-freeze child for monthly dataset resolution.

This command is intentionally narrow: it accepts one immutable profile/control
root/cutoff identity, freezes the allowlisted sources through the source
authority, and returns one bounded CAS receipt.  It never writes source tables,
candidate datasets, production pointers, or process state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.dataset_release.cas_store import CASStore  # noqa: E402
from backend.services.dataset_release.artifact_ready_source import (  # noqa: E402
    ArtifactReadySourceBuilder,
    load_artifact_ready_contract,
)
from backend.services.dataset_release.profile import load_dataset_profile  # noqa: E402
from backend.services.dataset_release.resource_gate import (  # noqa: E402
    ChildResourceCheckpoint,
    DiskSpaceGuard,
)
from backend.services.dataset_release.source_authority import (  # noqa: E402
    MAX_SOURCE_STAGE_ARTIFACT_BYTES,
    SOURCE_REUSE_MANIFEST_SCHEMA,
    build_source_authority,
    seal_source_stage_receipt,
)


SOURCE_STAGE_RESULT_SCHEMA = "dataset_release_source_stage_result_v1"
SOURCE_STAGE_ERROR_SCHEMA = "dataset_release_source_stage_error_v1"
_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_ZERO_SAFETY = {
    "database_writes": 0,
    "provider_database_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
    "candidate_writes": 0,
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--control-root", required=True)
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--attempt-fence", required=True, type=int)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--result-path", required=True)
    parser.add_argument("--baseline-reuse-ref")
    parser.add_argument("--predicted-new-bytes", required=True, type=int)
    parser.add_argument("--pressure-rung", required=True, type=int)
    parser.add_argument("--stage-timeout-seconds", required=True, type=int)
    parser.add_argument("--sample-instrument", action="append", default=[])
    return parser


def _baseline_partitions(
    cas: CASStore,
    reference: str | None,
    *,
    profile: str,
) -> tuple[Mapping[str, Any], ...]:
    if reference is None:
        return ()
    value = cas.get_json_bounded(reference, max_bytes=MAX_SOURCE_STAGE_ARTIFACT_BYTES)
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != SOURCE_REUSE_MANIFEST_SCHEMA
        or value.get("profile") != profile
        or not isinstance(value.get("partitions"), list)
        or value.get("safety") != _ZERO_SAFETY
    ):
        raise ValueError("baseline source reuse manifest contract differs")
    if not all(isinstance(item, Mapping) for item in value["partitions"]):
        raise ValueError("baseline source reuse partitions are invalid")
    return tuple(dict(item) for item in value["partitions"])


def _write_result(
    path: Path,
    payload: Mapping[str, Any],
    *,
    control_root: Path,
    attempt_id: str,
    attempt_fence: int,
    execution_id: str,
) -> None:
    resolved = path.resolve(strict=False)
    if not _IDENTITY.fullmatch(attempt_id) or not _IDENTITY.fullmatch(execution_id):
        raise ValueError("source-stage execution identity is invalid")
    expected = control_root / "attempt_runs" / f"{attempt_id}-{attempt_fence}" / execution_id / "semantic_result.json"
    if resolved != expected:
        raise ValueError("source-stage result path differs from exact execution root")
    _assert_plain_existing_chain(control_root)
    _assert_plain_existing_chain(resolved.parent)
    if resolved.exists():
        raise FileExistsError("source-stage result already exists")
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{resolved.name}.", suffix=".partial", dir=resolved.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, resolved)
    finally:
        temporary.unlink(missing_ok=True)


def _assert_plain_existing_chain(path: Path) -> None:
    resolved = path.resolve(strict=True)
    current = Path(resolved.anchor)
    for part in resolved.parts[1:]:
        current /= part
        metadata = current.lstat()
        attributes = int(getattr(metadata, "st_file_attributes", 0))
        reparse = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        if stat.S_ISLNK(metadata.st_mode) or attributes & reparse:
            raise ValueError("source-stage path contains a link/reparse point")


def _sanitized_error_envelope(exc: BaseException) -> Mapping[str, Any]:
    error_code = str(getattr(exc, "code", "SOURCE_STAGE_UNHANDLED_ERROR"))
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", error_code):
        error_code = "SOURCE_STAGE_UNHANDLED_ERROR"
    exception_type = type(exc).__name__
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", exception_type):
        exception_type = "Exception"
    fingerprint = hashlib.sha256(f"{exception_type}\0{exc}".encode("utf-8", errors="replace")).hexdigest()
    return {
        "schema_version": SOURCE_STAGE_ERROR_SCHEMA,
        "error_code": error_code,
        "exception_type": exception_type,
        "message_sha256": fingerprint,
        "context_ref": None,
        "safety": _ZERO_SAFETY,
    }


def _execute_stage(args: argparse.Namespace, *, profile: Any, control_root: Path) -> int:
    cas = CASStore(control_root)
    cutoff = date.fromisoformat(args.cutoff)
    checkpoint = ChildResourceCheckpoint(
        attempt_id=args.attempt_id,
        fence=args.attempt_fence,
        execution_id=args.execution_id,
    )
    checkpoint.checkpoint()
    disk_guard = DiskSpaceGuard(profile)
    baseline = _baseline_partitions(
        cas,
        args.baseline_reuse_ref,
        profile=profile.profile,
    )
    snapshot = build_source_authority(profile, cas).freeze(
        cutoff=cutoff,
        checkpoint=checkpoint.checkpoint,
        baseline_partitions=baseline,
        predicted_new_bytes=args.predicted_new_bytes,
        disk_checkpoint=disk_guard.checkpoint,
        pressure_rung=args.pressure_rung,
        sample_instruments=tuple(args.sample_instrument),
    )
    checkpoint.checkpoint()
    artifact_ready = ArtifactReadySourceBuilder(profile, cas).build(
        snapshot,
        checkpoint=checkpoint.checkpoint,
    )
    loaded_artifact = load_artifact_ready_contract(
        cas,
        profile,
        artifact_ready.artifact_ready_contract_ref,
        expected_source_content_root=snapshot.source_content_root,
        expected_pit_snapshot_digest=snapshot.pit_snapshot_digest,
    )
    snapshot = replace(
        snapshot,
        artifact_ready_contract_ref=artifact_ready.artifact_ready_contract_ref,
        artifact_ready_content_root=artifact_ready.artifact_ready_content_root,
        artifact_ready_provenance_root=(loaded_artifact.artifact_ready_provenance_root),
        provider_receipt_refs=artifact_ready.provider_receipt_refs,
        artifact_ready_derived_source_receipt_refs=(artifact_ready.derived_source_receipt_refs),
    )
    checkpoint.checkpoint()
    stage_ref = seal_source_stage_receipt(cas, snapshot, profile=profile.profile)
    _write_result(
        Path(args.result_path),
        {
            "schema_version": SOURCE_STAGE_RESULT_SCHEMA,
            "source_stage_receipt_ref": stage_ref.as_dict(),
            "stage_timeout_seconds": args.stage_timeout_seconds,
            "safety": _ZERO_SAFETY,
        },
        control_root=control_root,
        attempt_id=args.attempt_id,
        attempt_fence=args.attempt_fence,
        execution_id=args.execution_id,
    )
    return 0


def _write_error_result(
    args: argparse.Namespace,
    *,
    control_root: Path,
    error: BaseException,
) -> None:
    _write_result(
        Path(args.result_path),
        _sanitized_error_envelope(error),
        control_root=control_root,
        attempt_id=args.attempt_id,
        attempt_fence=args.attempt_fence,
        execution_id=args.execution_id,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    profile = load_dataset_profile(args.profile)
    control_root = Path(args.control_root).resolve(strict=True)
    expected_control_root = Path(str(profile.control_root)).resolve(strict=True)
    if control_root != expected_control_root:
        raise ValueError("control root differs from versioned profile")
    if args.stage_timeout_seconds != profile.stage_timeouts_seconds["source_freeze"]:
        raise ValueError("source-stage timeout differs from versioned profile")
    if args.predicted_new_bytes < 0:
        raise ValueError("predicted new bytes must be non-negative")
    try:
        return _execute_stage(args, profile=profile, control_root=control_root)
    except BaseException as exc:
        _write_error_result(args, control_root=control_root, error=exc)
        raise


if __name__ == "__main__":
    try:
        exit_code = main(sys.argv[1:])
    except SystemExit:
        raise
    except BaseException as exc:  # sanitized process boundary; no traceback/raw message
        sys.stderr.write(
            json.dumps(
                _sanitized_error_envelope(exc),
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
        exit_code = 2
    raise SystemExit(exit_code)
