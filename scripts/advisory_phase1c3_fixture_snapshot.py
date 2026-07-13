"""Offline Batch D fixture snapshot builder; never imported by FastAPI."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys
from typing import Any, Iterator

from dotenv import load_dotenv


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

load_dotenv(REPOSITORY_ROOT / ".env", override=False)

_DEV_DB_ENV_KEYS = (
    "TDX_DB_DEV_HOST",
    "TDX_DB_DEV_PORT",
    "TDX_DB_DEV_NAME",
    "TDX_DB_DEV_USER",
    "TDX_DB_DEV_PASSWORD",
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build or verify one historical Advisory Phase 1C-3 fixture snapshot")
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--request", required=True, type=Path)
    build.add_argument("--actor", default=f"batch-d-cli-{os.getpid()}")
    for command in ("verify", "resume"):
        child = subparsers.add_parser(command)
        child.add_argument("--build-id", required=True)
        child.add_argument("--actor", default=f"batch-d-cli-{os.getpid()}")
    return parser


def _store() -> Any:
    from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore

    raw_root = os.getenv("AISTOCK_ADVISORY_DATASET_STORE_ROOT", "").strip()
    if not raw_root:
        raise RuntimeError("ADVISORY_PHASE1C3_DATASET_STORE_INVALID: AISTOCK_ADVISORY_DATASET_STORE_ROOT is required")
    root = Path(raw_root)
    return LocalContentAddressedStore(
        root=root,
        repository_root=REPOSITORY_ROOT,
        store_identity={
            "backend": "LOCAL_FILESYSTEM_V1",
            "durability_mode": LocalContentAddressedStore.expected_durability_mode(),
            "atomic_publish_mode": "HARDLINK_CREATE_IF_ABSENT_V1",
            "writer_compatibility": "ADVISORY_PHASE1C3_PYARROW21_PARQUET_V1",
        },
    )


@contextmanager
def _dev_conn_factory() -> Iterator[Any]:
    import psycopg2

    missing = [key for key in _DEV_DB_ENV_KEYS if not os.getenv(key, "").strip()]
    if missing:
        raise RuntimeError(
            "ADVISORY_PHASE1C3_DEV_DB_ENV_MISSING: " + ",".join(missing)
        )
    try:
        port = int(os.environ["TDX_DB_DEV_PORT"])
    except ValueError as error:
        raise RuntimeError("ADVISORY_PHASE1C3_DEV_DB_ENV_INVALID: TDX_DB_DEV_PORT") from error
    conn = psycopg2.connect(
        host=os.environ["TDX_DB_DEV_HOST"],
        port=port,
        dbname=os.environ["TDX_DB_DEV_NAME"],
        user=os.environ["TDX_DB_DEV_USER"],
        password=os.environ["TDX_DB_DEV_PASSWORD"],
        connect_timeout=10,
        application_name="aistock_advisory_phase1c3_batch_d_dev",
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _components() -> tuple[Any, Any]:
    from backend.services.advisory_phase1.dataset_build_postgres import PostgresDatasetBuildRepository
    from backend.services.advisory_phase1.snapshot_writer import (
        DatasetSnapshotMaterializer,
        DatasetSnapshotPipeline,
        DescriptorCalculationEvidenceReader,
        DeterministicParquetWriter,
        PostgresSnapshotSourceReader,
    )

    repository = PostgresDatasetBuildRepository(conn_factory=_dev_conn_factory)
    source = PostgresSnapshotSourceReader(
        conn_factory=_dev_conn_factory,
        evidence_reader=DescriptorCalculationEvidenceReader(repository_root=REPOSITORY_ROOT),
    )
    pipeline = DatasetSnapshotPipeline(
        repository=repository,
        materializer=DatasetSnapshotMaterializer(source_reader=source, writer=DeterministicParquetWriter()),
        store=_store(),
    )
    return repository, pipeline


def _verify(repository: Any, pipeline: Any, build_id: str) -> dict[str, Any]:
    build = repository.get_build(build_id)
    receipt = pipeline.verify_read_only(build_id=build_id)
    return {
        "status": "VERIFIED_READ_ONLY",
        "build_id": build_id,
        "checkpoint": build.checkpoint.value,
        "verification_receipt_hash": receipt.receipt_hash,
        "verified_content_set_hash": receipt.verified_content_set_hash,
        "file_count": len(receipt.files),
    }


def main() -> int:
    args = _parser().parse_args()
    try:
        from backend.services.advisory_phase1.dataset_build import FixtureDatasetBuildRequest

        repository, pipeline = _components()
        if args.command == "build":
            payload = json.loads(args.request.read_text(encoding="utf-8"))
            request = FixtureDatasetBuildRequest.model_validate(payload)
            build = repository.create_or_get(request, actor=args.actor)
            result = pipeline.run(build_id=build.build_id, actor=args.actor)
            output = {
                "status": result.lifecycle.value,
                "build_id": result.build_id,
                "checkpoint": result.checkpoint.value,
                "snapshot_id": result.sealed_snapshot_id,
            }
        elif args.command == "verify":
            output = _verify(repository, pipeline, args.build_id)
        else:
            result = pipeline.run(build_id=args.build_id, actor=args.actor)
            output = {
                "status": result.lifecycle.value,
                "build_id": result.build_id,
                "checkpoint": result.checkpoint.value,
                "snapshot_id": result.sealed_snapshot_id,
            }
        print(json.dumps(output, sort_keys=True, ensure_ascii=False))
        return 0
    except Exception as error:
        reason_code = getattr(error, "reason_code", "ADVISORY_PHASE1C3_CLI_FAILED")
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "reason_code": reason_code,
                    "error_type": type(error).__name__,
                    "message": str(error),
                },
                sort_keys=True,
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
