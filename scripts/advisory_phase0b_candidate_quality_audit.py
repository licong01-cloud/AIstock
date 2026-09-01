"""Run the isolated, read-only Advisory Phase 0B candidate-quality audit."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

import psycopg2
from dotenv import dotenv_values

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.services.advisory_historical_range.runtime_factories import (
    historical_range_store_identity,
)
from backend.services.advisory_phase0b.contracts import (
    Phase0BCandidateQualityAuditRequestV1,
    Phase0BDatasetStoreIdentityV1,
)
from backend.services.advisory_phase0b.errors import (
    Phase0BAuditError,
    REASON_CONFIG_MISSING,
    REASON_TARGET_SET_CONFLICT,
)
from backend.services.advisory_phase0b.service import Phase0BCandidateQualityAuditService
from backend.services.advisory_phase0b.snapshot_reader import (
    Phase0BClientDatabaseTargetV1,
    Phase0BSnapshotReader,
    PostgresPhase0BSnapshotCatalog,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DB_KEYS = (
    "TDX_DB_HOST",
    "TDX_DB_PORT",
    "TDX_DB_NAME",
    "TDX_DB_USER",
    "TDX_DB_PASSWORD",
)


def _read_env(path: Path) -> dict[str, str]:
    try:
        resolved = path.expanduser().resolve(strict=True)
    except OSError as error:
        raise Phase0BAuditError(
            REASON_CONFIG_MISSING,
            "env file does not exist",
        ) from error
    if not resolved.is_file():
        raise Phase0BAuditError(REASON_CONFIG_MISSING, "env file must be an existing file")
    values = {
        str(key): str(value)
        for key, value in dotenv_values(resolved, interpolate=False).items()
        if key and value is not None
    }
    missing = tuple(key for key in DB_KEYS if not values.get(key))
    if missing:
        raise Phase0BAuditError(
            REASON_CONFIG_MISSING,
            "env file is missing required PostgreSQL configuration",
            context={"missing_keys": missing},
        )
    for key in DB_KEYS:
        os.environ[key] = values[key]
    if "AISTOCK_ADVISORY_DATASET_STORE_ROOT" in values:
        os.environ["AISTOCK_ADVISORY_DATASET_STORE_ROOT"] = values[
            "AISTOCK_ADVISORY_DATASET_STORE_ROOT"
        ]
    return values


def _database_config(values: Mapping[str, str]) -> dict[str, Any]:
    try:
        port = int(values["TDX_DB_PORT"])
    except (KeyError, ValueError) as error:
        raise Phase0BAuditError(
            REASON_CONFIG_MISSING,
            "TDX_DB_PORT must be a valid integer",
        ) from error
    return {
        "host": values["TDX_DB_HOST"],
        "port": port,
        "dbname": values["TDX_DB_NAME"],
        "user": values["TDX_DB_USER"],
        "password": values["TDX_DB_PASSWORD"],
    }


def _read_request(path: Path) -> Phase0BCandidateQualityAuditRequestV1:
    try:
        payload = json.loads(path.expanduser().resolve(strict=True).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("request JSON must contain one object")
        return Phase0BCandidateQualityAuditRequestV1.model_validate(payload)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        raise Phase0BAuditError(
            REASON_CONFIG_MISSING,
            "Phase 0B request file is missing or invalid",
            context={"error_type": type(error).__name__},
        ) from error


def _resolve_dataset_root(*, argument: Path | None, env_values: Mapping[str, str]) -> Path:
    env_value = env_values.get("AISTOCK_ADVISORY_DATASET_STORE_ROOT")
    if argument is not None and not argument.expanduser().is_absolute():
        raise Phase0BAuditError(REASON_CONFIG_MISSING, "dataset root argument must be absolute")
    if env_value and not Path(env_value).expanduser().is_absolute():
        raise Phase0BAuditError(REASON_CONFIG_MISSING, "env dataset root must be absolute")
    try:
        argument_root = argument.expanduser().resolve(strict=True) if argument is not None else None
        env_root = Path(env_value).expanduser().resolve(strict=True) if env_value else None
    except OSError as error:
        raise Phase0BAuditError(
            REASON_CONFIG_MISSING,
            "dataset root does not exist",
        ) from error
    if argument_root is not None and env_root is not None and argument_root != env_root:
        raise Phase0BAuditError(
            REASON_CONFIG_MISSING,
            "dataset root argument differs from the authoritative env file",
        )
    resolved = argument_root or env_root
    if resolved is None or not resolved.is_dir() or not resolved.is_absolute():
        raise Phase0BAuditError(
            REASON_CONFIG_MISSING,
            "dataset root must be an existing absolute directory",
        )
    return resolved


def _validate_explicit_identity(
    *,
    request: Phase0BCandidateQualityAuditRequestV1,
    snapshot_ids: tuple[str, ...],
    target_hashes: tuple[str, ...],
) -> None:
    if tuple(sorted(snapshot_ids)) != request.snapshot_ids or len(snapshot_ids) != len(set(snapshot_ids)):
        raise Phase0BAuditError(
            REASON_TARGET_SET_CONFLICT,
            "explicit CLI snapshot ids differ from the frozen request",
        )
    expected_targets = tuple(sorted(str(item.target_hash) for item in request.audit_targets))
    if tuple(sorted(target_hashes)) != expected_targets or len(target_hashes) != len(set(target_hashes)):
        raise Phase0BAuditError(
            REASON_TARGET_SET_CONFLICT,
            "explicit CLI audit target hashes differ from the frozen request",
        )


def _git_source(repository_root: Path) -> tuple[str, str]:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return head, "dirty" if status.strip() else "clean"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--snapshot-id", action="append", required=True)
    parser.add_argument("--audit-target-hash", action="append", required=True)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--dataset-root", type=Path)
    parser.add_argument("--output-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        env_file = args.env_file or (REPOSITORY_ROOT / ".env")
        env_values = _read_env(env_file)
        request = _read_request(args.request)
        _validate_explicit_identity(
            request=request,
            snapshot_ids=tuple(args.snapshot_id),
            target_hashes=tuple(args.audit_target_hash),
        )
        authoritative_identity = Phase0BDatasetStoreIdentityV1.from_authoritative_factory()
        if request.dataset_store_identity != authoritative_identity:
            raise Phase0BAuditError(
                REASON_CONFIG_MISSING,
                "request dataset store identity differs from the authoritative factory",
            )
        dataset_root = _resolve_dataset_root(argument=args.dataset_root, env_values=env_values)
        if not args.output_root.expanduser().is_absolute():
            raise Phase0BAuditError(REASON_CONFIG_MISSING, "output root must be absolute")
        output_root = args.output_root.expanduser().resolve()
        dataset_store = LocalContentAddressedStore(
            root=dataset_root,
            repository_root=REPOSITORY_ROOT,
            store_identity=historical_range_store_identity(),
        )
        database_config = _database_config(env_values)
        client_target = Phase0BClientDatabaseTargetV1(
            env_file_path_hash=hashlib.sha256(
                str(env_file.expanduser().resolve(strict=True)).encode("utf-8")
            ).hexdigest(),
            configured_host_hash=hashlib.sha256(
                str(database_config["host"]).encode("utf-8")
            ).hexdigest(),
            configured_port=int(database_config["port"]),
            configured_database_hash=hashlib.sha256(
                str(database_config["dbname"]).encode("utf-8")
            ).hexdigest(),
            configured_user_hash=hashlib.sha256(
                str(database_config["user"]).encode("utf-8")
            ).hexdigest(),
        )
        reader = Phase0BSnapshotReader(
            catalog=PostgresPhase0BSnapshotCatalog(
                conn_factory=lambda: psycopg2.connect(**database_config),
                client_target=client_target,
            ),
            dataset_store=dataset_store,
        )
        source_git_commit, source_state = _git_source(REPOSITORY_ROOT)
        receipt = Phase0BCandidateQualityAuditService(snapshot_reader=reader).run(
            request=request,
            repository_root=REPOSITORY_ROOT,
            dataset_root=dataset_root,
            output_root=output_root,
            source_git_commit=source_git_commit,
            source_state=source_state,
        )
        print(
            json.dumps(
                {
                    "status": "COMPLETE",
                    "report_semantic_hash": receipt.report_semantic_hash,
                    "receipt_hash": receipt.receipt_hash,
                },
                sort_keys=True,
            )
        )
        return 0
    except Phase0BAuditError as error:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "reason_code": error.reason_code,
                    "message": str(error),
                    "context": error.context,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    except Exception as error:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "reason_code": "ADVISORY_PHASE0B_UNEXPECTED_FAILURE",
                    "message": "Phase 0B audit failed unexpectedly; inspect the exception type and local runtime diagnostics.",
                    "error_type": type(error).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
