"""Materialize the Advisory SHORT_REBOUND Batch B dataset and training files."""

from __future__ import annotations

import argparse
from datetime import date, datetime
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
    _R5_CODE_RELEASE_CLOSURE,
    _R5_SELECTION_CLOSURE,
    _historical_range_code_set_hash,
    build_environment_historical_range_r5_application_service,
)
from backend.services.advisory_historical_range.code_release import (
    HistoricalRangeCodeReleaseResolver,
)
from backend.services.advisory_historical_range.models import (
    ExistingProgramSpecV1,
    HistoricalRangeResearchBatchRequestV1,
)
from backend.services.advisory_historical_range.request_resolver import (
    HistoricalRangeAdmittedPackageResolver,
    HistoricalRangeProgramResolver,
)
from backend.services.advisory_historical_range.runtime_factories import (
    _calendar,
    historical_range_store_identity,
)
from backend.services.advisory_historical_range.semantics import (
    canonical_list_semantics_v2,
)
from backend.services.advisory_modeling.base_snapshot import (
    HistoricalCandidateArtifactResolver,
    RerankerBaseSnapshotReader,
)
from backend.services.advisory_modeling.batch_b import (
    BatchBDatasetMaterializationRequestV1,
    BatchBHistoricalRangeDriver,
    BatchBMaterializationService,
)
from backend.services.advisory_modeling.errors import AdvisoryModelingError
from backend.services.advisory_modeling.feature_sources import PostgresFeatureSourceReader
from backend.services.advisory_modeling.request_builder import (
    BatchBRequestBuilder,
    publish_batch_b_request,
)
from backend.services.advisory_phase0b.snapshot_reader import (
    Phase0BClientDatabaseTargetV1,
    PostgresPhase0BSnapshotCatalog,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from backend.services.advisory_program import AdvisoryProgramPGRepository
from backend.services.strategy_package.historical_selection_providers import (
    historical_read_only_connection_factory,
)
from backend.services.strategy_package.repository import StrategyPackageRepository


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


def _load_environment(
    path: Path,
    *,
    require_runtime: bool = True,
) -> tuple[dict[str, str], Path]:
    env_path = path.expanduser().resolve(strict=True)
    if not env_path.is_file():
        raise ValueError("env_file must be an existing file")
    values = {
        str(key): str(value)
        for key, value in dotenv_values(env_path, interpolate=False).items()
        if key and value is not None
    }
    required_keys = (*DB_KEYS, *REQUIRED_RUNTIME_KEYS) if require_runtime else DB_KEYS
    missing = tuple(key for key in required_keys if not values.get(key))
    if missing:
        raise ValueError(f"env_file is missing required Batch B configuration keys: {missing}")
    for key in required_keys:
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


def _verify_repository(
    repository_root: Path,
    request: BatchBDatasetMaterializationRequestV1 | None = None,
) -> str:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if request is not None and head != request.dataset_intent.repository_commit:
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
    return head


def _date(value: str) -> date:
    return date.fromisoformat(value)


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--request", type=Path)
    mode.add_argument("--prepare-program-id")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--spool-root", type=Path)
    parser.add_argument("--decision-date-start", type=_date)
    parser.add_argument("--decision-date-end", type=_date)
    parser.add_argument("--final-fit-as-of", type=_datetime)
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        values, env_file = _load_environment(
            args.env_file,
            require_runtime=args.prepare_program_id is None,
        )
        repository_root = _existing_directory(args.repository_root, field_name="repository_root")
        artifact_root = _existing_directory(args.artifact_root, field_name="artifact_root")
        config = _database_config(values)

        def conn_factory() -> Any:
            return psycopg2.connect(**config)

        if args.prepare_program_id is not None:
            missing = tuple(
                name
                for name, value in (
                    ("decision_date_start", args.decision_date_start),
                    ("decision_date_end", args.decision_date_end),
                    ("final_fit_as_of", args.final_fit_as_of),
                )
                if value is None
            )
            if missing:
                raise ValueError(
                    f"Batch B request preparation is missing required arguments: {missing}"
                )
            repository_commit = _verify_repository(repository_root)
            read_only_factory = historical_read_only_connection_factory(conn_factory)
            package_resolver = HistoricalRangeAdmittedPackageResolver(
                package_reader=StrategyPackageRepository(conn_factory=read_only_factory)
            )
            program_repository = AdvisoryProgramPGRepository(conn_factory=read_only_factory)
            program_resolver = HistoricalRangeProgramResolver(
                package_resolver=package_resolver,
                program_reader=program_repository,
            )
            code_release_resolver = HistoricalRangeCodeReleaseResolver(
                repository_root=repository_root,
                closure_paths=_R5_CODE_RELEASE_CLOSURE,
            )
            selection_hash = _historical_range_code_set_hash(
                repository_root,
                _R5_SELECTION_CLOSURE,
            )
            list_semantics = canonical_list_semantics_v2()

            def freeze_program(
                spec: ExistingProgramSpecV1,
                start_trade_date: date,
                end_trade_date: date,
            ) -> Any:
                release = code_release_resolver.resolve()
                request = HistoricalRangeResearchBatchRequestV1(
                    client_idempotency_key=(
                        f"adv-reranker-batch-b-prepare-{spec.program_id}-{repository_commit[:16]}"
                    ),
                    program_specs=(spec,),
                    start_trade_date=start_trade_date,
                    end_trade_date=end_trade_date,
                    requested_by="advisory-modeling-batch-b-request-builder",
                )
                programs = program_resolver.freeze_programs(
                    request=request,
                    code_release_id=release.code_release_id,
                    code_release_hash=release.code_release_hash,
                    selection_semantics_version="strategy_package_selection_semantics_v1",
                    selection_semantics_hash=selection_hash,
                    list_semantics_version=list_semantics.schema_version,
                    list_semantics_hash=str(list_semantics.semantics_hash),
                )
                if len(programs) != 1:
                    raise ValueError("Batch B request preparation did not freeze exactly one Program")
                return programs[0]

            builder = BatchBRequestBuilder(
                program_reader=program_repository,
                frozen_program_provider=freeze_program,
                package_created_at_provider=lambda package_id: package_resolver.resolve(
                    package_id
                ).record.created_at,
                calendar_provider=lambda start, end: _calendar(
                    conn_factory=read_only_factory,
                    start_trade_date=start,
                    end_trade_date=end,
                ),
                repository_commit=repository_commit,
            )
            prepared = builder.build(
                program_id=args.prepare_program_id,
                decision_date_start=args.decision_date_start,
                decision_date_end=args.decision_date_end,
                final_fit_as_of=args.final_fit_as_of,
            )
            request_path = publish_batch_b_request(
                request=prepared,
                artifact_root=artifact_root,
                repository_root=repository_root,
            )
            print(
                json.dumps(
                    {
                        "status": "PREPARED",
                        "request_hash": prepared.request_hash,
                        "request_path": str(request_path),
                        "program_id": prepared.existing_program.program_id,
                        "program_version": prepared.existing_program.expected_program_version,
                        "binding_version_id": (
                            prepared.existing_program.expected_binding_version_id
                        ),
                        "package_id": prepared.dataset_intent.package_id,
                        "decision_date_start": (
                            prepared.dataset_intent.decision_date_start.isoformat()
                        ),
                        "decision_date_end": prepared.dataset_intent.decision_date_end.isoformat(),
                        "final_fit_as_of": prepared.dataset_intent.final_fit_as_of.isoformat(),
                    },
                    sort_keys=True,
                )
            )
            return 0

        request = _request(args.request)
        if args.spool_root is None:
            raise ValueError("spool_root is required for Batch B materialization")
        spool_root = _existing_directory(args.spool_root, field_name="spool_root")
        dataset_root = _existing_directory(
            Path(values["AISTOCK_ADVISORY_DATASET_STORE_ROOT"]),
            field_name="dataset_store_root",
        )
        _verify_repository(repository_root, request)

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
                service=build_environment_historical_range_r5_application_service()
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
