"""Contract tests for the append-only Advisory source availability ledger."""

from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.services.advisory_phase1.source_ledger import (
    REASON_EVENT_CHAIN_INVALID,
    REASON_EVENT_CONFLICT,
    REASON_SOURCE_INVALIDATED,
    REASON_SOURCE_UNAVAILABLE,
    InMemorySourceAvailabilityLedger,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
    SourceLedgerError,
)


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)


def _request(
    *,
    revision_no: int = 1,
    event_type: SourceAvailabilityEventType = SourceAvailabilityEventType.INGESTED,
    predecessor_event_hash: str | None = None,
    revision_id: str = "r1",
    content_hash: str = "a" * 64,
    provider_published_at: datetime | None = None,
    quality_status: str = "PASS",
) -> SourceAvailabilityEventRequest:
    return SourceAvailabilityEventRequest(
        dataset_name="market.kline_daily_raw",
        source_role="FEATURE_T",
        partition_key={"trade_date": "2026-06-30"},
        revision_id=revision_id,
        event_revision_no=revision_no,
        event_type=event_type,
        predecessor_event_hash=predecessor_event_hash,
        provider_job_id="ingest-1",
        refresh_job_id="refresh-1",
        provider_published_at=provider_published_at,
        schema_fingerprint="schema-v1",
        row_count=5000,
        partition_content_hash=content_hash,
        quality_status=quality_status,
        reason_codes=(),
        created_by_service_principal="ingestion-observer",
    )


def test_append_is_idempotent_and_formal_time_never_backdates() -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    item = _request(provider_published_at=OBSERVED_AT - timedelta(hours=2))

    event = ledger.append(item)

    assert ledger.append(item) == event
    assert event.formal_available_at == OBSERVED_AT
    selector = {
        "dataset_name": item.dataset_name,
        "source_role": item.source_role,
        "partition_key": item.partition_key,
    }
    assert ledger.select_as_of(**selector, cutoff=OBSERVED_AT) == event
    with pytest.raises(SourceLedgerError, match=REASON_SOURCE_UNAVAILABLE):
        ledger.select_as_of(**selector, cutoff=OBSERVED_AT - timedelta(seconds=1))


def test_correct_invalidate_and_revalidate_preserve_exact_as_of_history() -> None:
    observed_times = iter([OBSERVED_AT, OBSERVED_AT + timedelta(days=1), OBSERVED_AT + timedelta(days=2), OBSERVED_AT + timedelta(days=3)])
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: next(observed_times))
    ingested = ledger.append(_request())
    corrected_at = OBSERVED_AT + timedelta(days=1)
    corrected = ledger.append(
        _request(
            revision_no=2,
            event_type=SourceAvailabilityEventType.CORRECTED,
            predecessor_event_hash=ingested.event_content_hash,
            revision_id="r2",
            content_hash="b" * 64,
        )
    )
    invalidated = ledger.append(
        _request(
            revision_no=3,
            event_type=SourceAvailabilityEventType.INVALIDATED,
            predecessor_event_hash=corrected.event_content_hash,
            revision_id="r2",
            content_hash="b" * 64,
        )
    )

    selector = {
        "dataset_name": ingested.input.dataset_name,
        "source_role": ingested.input.source_role,
        "partition_key": ingested.input.partition_key,
    }
    assert ledger.select_as_of(**selector, cutoff=corrected_at) == corrected
    with pytest.raises(SourceLedgerError, match=REASON_SOURCE_INVALIDATED):
        ledger.select_as_of(**selector, cutoff=invalidated.formal_available_at)

    revalidated = ledger.append(
        _request(
            revision_no=4,
            event_type=SourceAvailabilityEventType.REVALIDATED,
            predecessor_event_hash=invalidated.event_content_hash,
            revision_id="r3",
            content_hash="c" * 64,
        )
    )
    assert ledger.select_as_of(**selector, cutoff=revalidated.formal_available_at) == revalidated


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {
                "revision_no": 2,
                "event_type": SourceAvailabilityEventType.CORRECTED,
                "predecessor_event_hash": "f" * 64,
                "revision_id": "r2",
                "content_hash": "b" * 64,
            },
            "next sequence",
        ),
        (
            {
                "revision_no": 2,
                "event_type": SourceAvailabilityEventType.REVALIDATED,
                "revision_id": "r2",
                "content_hash": "b" * 64,
            },
            "REVALIDATED requires",
        ),
    ],
)
def test_chain_failures_are_loud(kwargs: dict[str, object], message: str) -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    first = ledger.append(_request())
    if kwargs.get("predecessor_event_hash") == "f" * 64:
        with pytest.raises(SourceLedgerError, match=REASON_EVENT_CHAIN_INVALID):
            ledger.append(_request(**kwargs))
        return
    kwargs["predecessor_event_hash"] = first.event_content_hash
    with pytest.raises(SourceLedgerError, match=message):
        ledger.append(_request(**kwargs))


def test_correction_requires_a_new_revision_and_content_hash() -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    first = ledger.append(_request())
    with pytest.raises(SourceLedgerError, match=REASON_EVENT_CHAIN_INVALID):
        ledger.append(
            _request(
                revision_no=2,
                event_type=SourceAvailabilityEventType.CORRECTED,
                predecessor_event_hash=first.event_content_hash,
                revision_id="r1",
                content_hash="a" * 64,
            )
        )


def test_observation_time_and_chain_identity_are_repository_controlled() -> None:
    request = _request()
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SourceAvailabilityEventRequest.model_validate({**request.model_dump(), "first_observed_at": OBSERVED_AT})
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SourceAvailabilityEventRequest.model_validate({**request.model_dump(), "partition_chain_key": "alternate"})


def test_same_natural_partition_revision_with_different_request_conflicts() -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    ledger.append(_request())
    with pytest.raises(SourceLedgerError, match=REASON_EVENT_CONFLICT):
        ledger.append(_request(quality_status="FAILED"))


def test_new_ledger_does_not_import_execution_or_realtime_modules() -> None:
    package_root = Path(__file__).parents[2] / "services" / "advisory_phase1"
    imports: set[str] = set()
    for module_path in package_root.glob("*.py"):
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        imports.update(
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        )
        imports.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
    forbidden = ("paper_trading", "simulation", "qmt", "xtquant", "broker", "realtime", "miniqmt")
    assert not [name for name in imports if any(token in name.lower() for token in forbidden)]


def test_migration_enforces_append_only_chain_contract() -> None:
    migration = (Path(__file__).parents[2] / "db" / "migrations" / "add_advisory_source_availability_ledger_20260712.sql").read_text(
        encoding="utf-8"
    )
    assert "UNIQUE (partition_chain_key, event_revision_no)" in migration
    assert "ux_advisory_source_availability_natural_revision_idx" in migration
    assert "clock_timestamp()" in migration
    assert "ux_advisory_source_availability_one_successor" in migration
    assert "BEFORE UPDATE OR DELETE" in migration
    assert "ck_advisory_source_availability_formal_time" in migration
    assert "CREATE TABLE IF NOT EXISTS app.advisory_source_availability_event" in migration
