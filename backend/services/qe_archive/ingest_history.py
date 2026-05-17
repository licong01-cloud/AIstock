"""Convenience helpers for QE archive ingest history."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .models import ArchivePolicyDecision, IngestHistoryRecord
from .repository import QEArchiveRepository


def record_ingest_history(
    *,
    source_system: str,
    source_type: str,
    source_id: str,
    ingest_status: str,
    trigger_reason: str,
    repository: QEArchiveRepository | None = None,
    source_sub_id: str | None = None,
    archive_policy: str | None = None,
    run_id: str | None = None,
    event_id: str | None = None,
    job_id: str | None = None,
    backfill_run_id: str | None = None,
    payload_sha256: str | None = None,
    runtime_config_sha256: str | None = None,
    result_fingerprint: str | None = None,
    stats: Mapping[str, Any] | None = None,
    error_message: str | None = None,
    created_by: str | None = None,
) -> str:
    repo = repository or QEArchiveRepository()
    if not hasattr(repo, "insert_ingest_history"):
        return ""
    return repo.insert_ingest_history(
        IngestHistoryRecord(
            source_system=source_system,
            source_type=source_type,
            source_id=source_id,
            source_sub_id=source_sub_id,
            trigger_reason=trigger_reason,
            archive_policy=archive_policy,
            ingest_status=ingest_status,
            run_id=run_id,
            event_id=event_id,
            job_id=job_id,
            backfill_run_id=backfill_run_id,
            payload_sha256=payload_sha256,
            runtime_config_sha256=runtime_config_sha256,
            result_fingerprint=result_fingerprint,
            stats=dict(stats or {}),
            error_message=error_message,
            created_by=created_by,
        )
    )


def record_decision_skip(
    decision: ArchivePolicyDecision,
    *,
    trigger_reason: str,
    repository: QEArchiveRepository | None = None,
) -> str:
    status = "manual_only" if decision.archive_policy == "MANUAL_ONLY" else "skipped"
    return record_ingest_history(
        source_system=decision.source_system,
        source_type=decision.source_type,
        source_id=decision.source_id,
        source_sub_id=decision.source_sub_id,
        trigger_reason=trigger_reason,
        archive_policy=decision.archive_policy,
        ingest_status=status,
        payload_sha256=decision.payload_sha256,
        runtime_config_sha256=decision.runtime_config_sha256,
        stats={"archive_policy_source": decision.archive_policy_source, "reason": decision.reason},
        created_by="qe_archive_policy",
        repository=repository,
    )
