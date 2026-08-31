from __future__ import annotations

import hashlib
from pathlib import Path

from backend.services.advisory_historical_range.canonical import canonical_json_sha256

from .errors import Phase0BAuditError, REASON_WINNER_REGISTRY_CONFLICT


PRODUCER_CLOSURE_SCHEMA_VERSION = "advisory_phase0b_producer_closure_v1"
PRODUCER_CLOSURE_PATHS = (
    "backend/services/advisory_historical_range/canonical.py",
    "backend/services/advisory_historical_range/runtime_factories.py",
    "backend/services/advisory_historical_range/summary_service.py",
    "backend/services/advisory_phase0b/__init__.py",
    "backend/services/advisory_phase0b/audit_service.py",
    "backend/services/advisory_phase0b/contracts.py",
    "backend/services/advisory_phase0b/errors.py",
    "backend/services/advisory_phase0b/metrics.py",
    "backend/services/advisory_phase0b/producer_closure.py",
    "backend/services/advisory_phase0b/report_store.py",
    "backend/services/advisory_phase0b/service.py",
    "backend/services/advisory_phase0b/snapshot_reader.py",
    "backend/services/advisory_phase0b/spool.py",
    "backend/services/advisory_phase1/dataset_store.py",
    "backend/services/advisory_phase1/label_policy.py",
    "backend/services/advisory_phase1/snapshot_writer.py",
    "scripts/advisory_phase0b_candidate_quality_audit.py",
)


def phase0b_producer_code_closure_hash(*, repository_root: Path) -> str:
    if not repository_root.is_absolute():
        raise Phase0BAuditError(
            REASON_WINNER_REGISTRY_CONFLICT,
            "producer closure repository root must be absolute",
        )
    root = repository_root.resolve()
    members: list[dict[str, str]] = []
    for relative_path in PRODUCER_CLOSURE_PATHS:
        path = (root / relative_path).resolve()
        try:
            path.relative_to(root)
        except ValueError as error:
            raise Phase0BAuditError(
                REASON_WINNER_REGISTRY_CONFLICT,
                "producer closure path escapes the repository",
                context={"path": relative_path},
            ) from error
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise Phase0BAuditError(
                REASON_WINNER_REGISTRY_CONFLICT,
                "producer closure member cannot be read",
                context={"path": relative_path, "error_type": type(error).__name__},
            ) from error
        members.append(
            {
                "path": relative_path,
                "file_sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return canonical_json_sha256(
        {
            "schema_version": PRODUCER_CLOSURE_SCHEMA_VERSION,
            "members": tuple(members),
        }
    )
