"""DB-only prepublish source recheck over the original immutable overlays."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.dataset_release.artifact_ready_source import (  # noqa: E402
    ArtifactReadySourceBuilder,
    load_artifact_ready_contract,
    load_artifact_ready_recheck_expectations,
)
from backend.services.dataset_release.canonical import ensure_sha256  # noqa: E402
from backend.services.dataset_release.cas_store import CASStore  # noqa: E402
from backend.services.dataset_release.profile import load_dataset_profile  # noqa: E402
from backend.services.dataset_release.resource_gate import (  # noqa: E402
    ChildResourceCheckpoint,
    DiskSpaceGuard,
)
from backend.services.dataset_release.source_authority import (  # noqa: E402
    build_source_authority,
)


SOURCE_RECHECK_RESULT_SCHEMA = "dataset_release_source_recheck_result_v1"
SOURCE_RECHECK_ERROR_SCHEMA = "dataset_release_source_recheck_error_v1"
EXECUTION_ID = "prepublish-source-recheck"
_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_ZERO_SAFETY = {
    "database_writes": 0,
    "provider_database_writes": 0,
    "candidate_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--control-root", required=True)
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--artifact-ready-contract-ref", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--attempt-fence", required=True, type=int)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--result-path", required=True)
    parser.add_argument("--stage-timeout-seconds", required=True, type=int)
    parser.add_argument("--pressure-rung", required=True, type=int)
    return parser


def _write_result(
    path: Path,
    payload: Mapping[str, Any],
    *,
    control_root: Path,
    attempt_id: str,
    attempt_fence: int,
) -> None:
    resolved = path.resolve(strict=False)
    if not _IDENTITY.fullmatch(attempt_id):
        raise ValueError("source-recheck attempt identity is invalid")
    expected = (
        control_root / "attempt_runs" / f"{attempt_id}-{attempt_fence}" / EXECUTION_ID / "semantic_result.json"
    ).resolve(strict=False)
    if resolved != expected:
        raise ValueError("source-recheck result path differs")
    _assert_plain_existing_chain(control_root)
    _assert_plain_existing_chain(resolved.parent)
    if resolved.exists():
        raise FileExistsError("source-recheck result already exists")
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
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
            raise ValueError("source-recheck path contains a link/reparse point")


def _sanitized_error(exc: BaseException) -> Mapping[str, Any]:
    code = str(getattr(exc, "code", "SOURCE_RECHECK_UNHANDLED_ERROR"))
    if re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", code) is None:
        code = "SOURCE_RECHECK_UNHANDLED_ERROR"
    exception_type = type(exc).__name__
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", exception_type) is None:
        exception_type = "Exception"
    return {
        "schema_version": SOURCE_RECHECK_ERROR_SCHEMA,
        "error_code": code,
        "exception_type": exception_type,
        "message_sha256": hashlib.sha256(f"{exception_type}\0{exc}".encode("utf-8", errors="replace")).hexdigest(),
        "safety": dict(_ZERO_SAFETY),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.execution_id != EXECUTION_ID or not _IDENTITY.fullmatch(args.run_id):
        raise ValueError("source-recheck execution/run identity differs")
    if "TUSHARE_TOKEN" in os.environ or "TDX_HTTP_PORT" in os.environ:
        raise ValueError("source-recheck provider credential/HTTP authority is forbidden")
    profile = load_dataset_profile(args.profile)
    control_root = Path(args.control_root).resolve(strict=True)
    if control_root != Path(str(profile.control_root)).resolve(strict=True):
        raise ValueError("source-recheck control root differs from profile")
    if args.stage_timeout_seconds != profile.stage_timeouts_seconds["source_freeze"]:
        raise ValueError("source-recheck timeout differs from profile")
    cutoff = date.fromisoformat(args.cutoff)
    cas = CASStore(control_root)
    contract_ref = cas.verify(
        ensure_sha256(
            args.artifact_ready_contract_ref,
            field="artifact_ready_contract_ref",
        )
    )
    raw_contract = cas.get_json_bounded(contract_ref, max_bytes=32 * 1024 * 1024)
    if not isinstance(raw_contract, Mapping):
        raise ValueError("source-recheck artifact-ready contract is invalid")
    initial_raw_root = ensure_sha256(
        str(raw_contract.get("source_content_root", "")),
        field="initial_source_content_root",
    )
    loaded = load_artifact_ready_contract(
        cas,
        profile,
        contract_ref,
        expected_source_content_root=initial_raw_root,
        expected_pit_snapshot_digest=ensure_sha256(
            str(raw_contract.get("pit_snapshot_digest", "")),
            field="pit_snapshot_digest",
        ),
        verify_partition_payloads=False,
    )
    if loaded.qfq_denominator_authority.cutoff != cutoff:
        raise ValueError("source-recheck cutoff differs from artifact-ready contract")
    checkpoint = ChildResourceCheckpoint(
        attempt_id=args.attempt_id,
        fence=args.attempt_fence,
        execution_id=args.execution_id,
    )
    disk_guard = DiskSpaceGuard(profile)
    recheck_expectations = load_artifact_ready_recheck_expectations(cas, loaded)
    fresh_snapshot = build_source_authority(profile, cas).freeze(
        cutoff=cutoff,
        checkpoint=checkpoint.checkpoint,
        disk_checkpoint=disk_guard.checkpoint,
        pressure_rung=args.pressure_rung,
        recheck_partition_expectations=recheck_expectations,
        expected_source_content_root=initial_raw_root,
        expected_pit_snapshot_digest=loaded.qfq_denominator_authority.pit_spans_sha256,
    )
    observed_at = datetime.now(UTC)
    result = ArtifactReadySourceBuilder(profile, cas).verify_current_exact(
        loaded,
        fresh_snapshot=fresh_snapshot,
        observed_at=observed_at,
        execution_id=args.execution_id,
        run_id=args.run_id,
        attempt_id=args.attempt_id,
        attempt_fence=args.attempt_fence,
        checkpoint=checkpoint.checkpoint,
    )
    _write_result(
        Path(args.result_path),
        {
            "schema_version": SOURCE_RECHECK_RESULT_SCHEMA,
            "status": "PASS",
            "run_id": args.run_id,
            "attempt_id": args.attempt_id,
            "attempt_fence": args.attempt_fence,
            "execution_id": args.execution_id,
            "artifact_ready_contract_ref": loaded.reference.as_dict(),
            "artifact_ready_content_root": result.artifact_ready_content_root,
            "fresh_raw_source_content_root": fresh_snapshot.source_content_root,
            "source_probe_key": result.source_probe_key,
            "source_probe_ref": result.source_probe_ref.as_dict(),
            "stage_timeout_seconds": args.stage_timeout_seconds,
            "safety": dict(_ZERO_SAFETY),
        },
        control_root=control_root,
        attempt_id=args.attempt_id,
        attempt_fence=args.attempt_fence,
    )
    return 0


if __name__ == "__main__":
    try:
        exit_code = main(sys.argv[1:])
    except BaseException as exc:
        sys.stderr.write(json.dumps(_sanitized_error(exc), sort_keys=True, separators=(",", ":")) + "\n")
        exit_code = 2
    raise SystemExit(exit_code)
