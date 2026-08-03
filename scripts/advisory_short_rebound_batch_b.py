"""Materialize the Advisory SHORT_REBOUND Batch B dataset and training files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import dotenv_values

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.composition import (
    build_environment_historical_range_r5_application_service,
)
from backend.services.advisory_historical_range.runtime_factories import (
    historical_range_store_identity,
)
from backend.services.advisory_modeling.base_snapshot import (
    HistoricalCandidateArtifactResolver,
    RerankerBaseSnapshotReader,
)
from backend.services.advisory_modeling.batch_b import (
    BATCH_B_CANDIDATE_PREFETCH_PER_PROGRAM,
    BatchBDatasetMaterializationRequestV1,
    BatchBHistoricalRangeDriver,
    BatchBMaterializationService,
)
from backend.services.advisory_modeling.errors import AdvisoryModelingError
from backend.services.advisory_modeling.feature_sources import PostgresFeatureSourceReader
from backend.services.advisory_phase0b.snapshot_reader import (
    Phase0BClientDatabaseTargetV1,
    PostgresPhase0BSnapshotCatalog,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore


DB_KEYS = (
    "TDX_DB_HOST",
    "TDX_DB_PORT",
    "TDX_DB_NAME",
    "TDX_DB_USER",
    "TDX_DB_PASSWORD",
)
REQUIRED_RUNTIME_KEYS = (
    "AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT",
    "AISTOCK_ADVISORY_HISTORICAL_RANGE_TASK_RUNTIME_ROOT",
    "AISTOCK_PACKAGE_ASSET_STORE_ROOT",
    "AISTOCK_ADVISORY_HISTORICAL_RANGE_POLICY_COMPONENT_ROOT",
    "AISTOCK_ADVISORY_CALCULATION_EVIDENCE_ROOT",
    "AISTOCK_ADVISORY_DATASET_STORE_ROOT",
)


def _existing_directory(path: Path, *, field_name: str) -> Path:
    if not path.expanduser().is_absolute():
        raise ValueError(f"{field_name} must be an explicit absolute path")
    resolved = path.expanduser().resolve(strict=True)
    if not resolved.is_dir():
        raise ValueError(f"{field_name} must be an existing directory")
    return resolved


def _load_environment(path: Path) -> tuple[dict[str, str], Path]:
    env_path = path.expanduser().resolve(strict=True)
    if not env_path.is_file():
        raise ValueError("env_file must be an existing file")
    values = {
        str(key): str(value)
        for key, value in dotenv_values(env_path, interpolate=False).items()
        if key and value is not None
    }
    missing = tuple(key for key in (*DB_KEYS, *REQUIRED_RUNTIME_KEYS) if not values.get(key))
    if missing:
        raise ValueError(f"env_file is missing required Batch B configuration keys: {missing}")
    for key in (*DB_KEYS, *REQUIRED_RUNTIME_KEYS):
        os.environ[key] = values[key]
    return values, env_path


def _database_config(values: dict[str, str]) -> dict[str, Any]:
    return {
        "host": values["TDX_DB_HOST"],
        "port": int(values["TDX_DB_PORT"]),
        "dbname": values["TDX_DB_NAME"],
        "user": values["TDX_DB_USER"],
        "password": values["TDX_DB_PASSWORD"],
    }


def _request(path: Path) -> BatchBDatasetMaterializationRequestV1:
    payload = json.loads(path.expanduser().resolve(strict=True).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("request file must contain one JSON object")
    return BatchBDatasetMaterializationRequestV1.model_validate(payload)


def _verify_repository(repository_root: Path, request: BatchBDatasetMaterializationRequestV1) -> None:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if head != request.dataset_intent.repository_commit:
        raise ValueError("repository HEAD differs from the frozen dataset intent commit")
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise ValueError("repository must be clean before durable Batch B materialization")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--spool-root", type=Path, required=True)
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        request = _request(args.request)
        values, env_file = _load_environment(args.env_file)
        repository_root = _existing_directory(args.repository_root, field_name="repository_root")
        artifact_root = _existing_directory(args.artifact_root, field_name="artifact_root")
        spool_root = _existing_directory(args.spool_root, field_name="spool_root")
        dataset_root = _existing_directory(
            Path(values["AISTOCK_ADVISORY_DATASET_STORE_ROOT"]),
            field_name="dataset_store_root",
        )
        _verify_repository(repository_root, request)
        config = _database_config(values)

        def conn_factory() -> Any:
            return psycopg2.connect(**config)

        client_target = Phase0BClientDatabaseTargetV1(
            env_file_path_hash=hashlib.sha256(str(env_file).encode("utf-8")).hexdigest(),
            configured_host_hash=hashlib.sha256(config["host"].encode("utf-8")).hexdigest(),
            configured_port=int(config["port"]),
            configured_database_hash=hashlib.sha256(config["dbname"].encode("utf-8")).hexdigest(),
            configured_user_hash=hashlib.sha256(config["user"].encode("utf-8")).hexdigest(),
        )
        dataset_store = LocalContentAddressedStore(
            root=dataset_root,
            repository_root=repository_root,
            store_identity=historical_range_store_identity(),
        )
        historical_artifacts = HistoricalRangeArtifactStore.from_environment()
        service = BatchBMaterializationService(
            historical_driver=BatchBHistoricalRangeDriver(
                service=build_environment_historical_range_r5_application_service(
                    candidate_prefetch_per_program=BATCH_B_CANDIDATE_PREFETCH_PER_PROGRAM
                )
            ),
            base_reader=RerankerBaseSnapshotReader(
                catalog=PostgresPhase0BSnapshotCatalog(
                    conn_factory=conn_factory,
                    client_target=client_target,
                ),
                dataset_store=dataset_store,
                candidate_artifacts=HistoricalCandidateArtifactResolver(
                    artifact_store=historical_artifacts
                ),
            ),
            feature_source_reader=PostgresFeatureSourceReader(
                conn_factory=conn_factory,
                configured_host_hash=client_target.configured_host_hash,
                configured_port=client_target.configured_port,
                configured_database_hash=client_target.configured_database_hash,
                configured_user_hash=client_target.configured_user_hash,
                statement_timeout_ms=args.statement_timeout_ms,
            ),
        )
        result = service.execute(
            request=request,
            repository_root=repository_root,
            artifact_root=artifact_root,
            spool_root=spool_root,
        )
        print(json.dumps(result.model_dump(mode="json"), sort_keys=True))
        return 0
    except AdvisoryModelingError as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "reason_code": exc.reason_code,
                    "message": str(exc),
                    "context": exc.context,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "reason_code": "MODEL_BATCH_B_EXECUTION_FAILED",
                    "message": str(exc),
                    "error_type": type(exc).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
