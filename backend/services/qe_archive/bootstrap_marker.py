"""Bootstrap marker helpers for historical QE archive backfill."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .models import BootstrapMarkerRecord
from .repository import QEArchiveRepository

REBOOTSTRAP_CONFIRM_TEXT = "QE_ARCHIVE_REBOOTSTRAP"


class BootstrapAlreadyCompletedError(ValueError):
    pass


def assert_can_broad_backfill(
    source_type: str,
    *,
    force_token: str | None = None,
    repository: QEArchiveRepository | None = None,
) -> None:
    repo = repository or QEArchiveRepository()
    marker = repo.get_bootstrap_marker(source_type)
    if marker and marker.get("status") == "completed" and force_token != REBOOTSTRAP_CONFIRM_TEXT:
        raise BootstrapAlreadyCompletedError(
            f"broad backfill for {source_type!r} already completed; use force_rebackfill={REBOOTSTRAP_CONFIRM_TEXT} to rebootstrap"
        )


def mark_bootstrap(
    *,
    source_type: str,
    mode: str,
    backfill_run_id: str,
    status: str,
    operator: str | None = None,
    stats: Mapping[str, Any] | None = None,
    ingested_count: int = 0,
    skipped_count: int = 0,
    failed_count: int = 0,
    repository: QEArchiveRepository | None = None,
) -> str:
    repo = repository or QEArchiveRepository()
    return repo.upsert_bootstrap_marker(
        BootstrapMarkerRecord(
            source_type=source_type,
            mode=mode,
            backfill_run_id=backfill_run_id,
            status=status,
            operator=operator,
            stats=dict(stats or {}),
            ingested_count=ingested_count,
            skipped_count=skipped_count,
            failed_count=failed_count,
        )
    )
