"""Audit helpers for QE archive SKIP/MANUAL_ONLY policy decisions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .models import ArchivePolicyDecision, SkipRegistryRecord
from .repository import QEArchiveRepository


def record_policy_skip(
    decision: ArchivePolicyDecision,
    *,
    event_type: str | None = None,
    trigger_reason: str = "realtime",
    repository: QEArchiveRepository | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> str:
    repo = repository or QEArchiveRepository()
    return repo.upsert_skip_registry(
        SkipRegistryRecord(
            source_system=decision.source_system,
            source_type=decision.source_type,
            source_id=decision.source_id,
            source_sub_id=decision.source_sub_id,
            event_type=event_type,
            archive_policy=decision.archive_policy,
            archive_policy_source=decision.archive_policy_source,
            skip_reason=decision.reason,
            allow_override=decision.allow_override,
            override_required_token=None,
            trigger_reason=trigger_reason,
            payload_sha256=decision.payload_sha256,
            runtime_config_sha256=decision.runtime_config_sha256,
            created_by="qe_archive_policy",
            metadata=dict(metadata or {}),
        )
    )
