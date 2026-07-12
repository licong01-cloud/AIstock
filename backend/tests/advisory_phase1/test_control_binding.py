"""Append-only control-binding contracts for Phase 1 sidecars."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.control_binding import (
    ControlBindingRequest,
    ControlType,
    InMemoryControlBindingRepository,
    REASON_CONTROL_BINDING_CHAIN_INVALID,
    REASON_CONTROL_BINDING_CONFLICT,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError


UTC = timezone.utc


def _request(
    *,
    revision_no: int = 1,
    predecessor: str | None = None,
    enabled: bool = True,
    config: dict[str, object] | None = None,
) -> ControlBindingRequest:
    config = config or {"capture_policy": {"max_candidates": 20, "max_bytes": 1_000_000}}
    return ControlBindingRequest(
        control_type=ControlType.TRACE_CAPTURE,
        environment="DEV",
        admission_scope_set_hash="a" * 64,
        config_source="test",
        config_payload=config,
        config_or_store_backend_hash=canonical_json_sha256(config),
        enabled=enabled,
        binding_event_revision_no=revision_no,
        predecessor_binding_event_hash=predecessor,
        created_by_service_principal="test",
    )


def test_control_binding_is_idempotent_current_and_single_chain() -> None:
    now = datetime(2026, 7, 10, tzinfo=UTC)
    repository = InMemoryControlBindingRepository(now_provider=lambda: now)
    first = repository.append(_request())

    assert repository.append(_request()) == first
    assert repository.current(
        control_type=ControlType.TRACE_CAPTURE,
        environment="DEV",
        admission_scope_set_hash="a" * 64,
        governance_scope_hash=None,
    ) == first
    second = repository.append(
        _request(
            revision_no=2,
            predecessor=first.binding_event_hash,
            config={"capture_policy": {"max_candidates": 10, "max_bytes": 1_000_000}},
        )
    )
    assert second.request.binding_event_revision_no == 2


def test_control_binding_rejects_conflict_and_noop_successor() -> None:
    repository = InMemoryControlBindingRepository(now_provider=lambda: datetime(2026, 7, 10, tzinfo=UTC))
    first = repository.append(_request())
    with pytest.raises(SourceLedgerError, match=REASON_CONTROL_BINDING_CONFLICT):
        repository.append(_request(enabled=False))
    with pytest.raises(SourceLedgerError, match=REASON_CONTROL_BINDING_CONFLICT):
        repository.append(_request(revision_no=2, predecessor=first.binding_event_hash))
    with pytest.raises(SourceLedgerError, match=REASON_CONTROL_BINDING_CHAIN_INVALID):
        repository.append(
            _request(
                revision_no=3,
                predecessor=first.binding_event_hash,
                config={"capture_policy": {"max_candidates": 10, "max_bytes": 1_000_000}},
            )
        )
