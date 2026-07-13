"""Explicitly gated dev-DB durable evidence integration checks.

They never target production and are intentionally skipped unless an operator
sets AISTOCK_RUN_MINIQMT_QUOTE_EVIDENCE_DEV_DB=1 with a disposable dev DSN.
P1-D local unit validation does not execute this module.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime
import os
from uuid import uuid4

import pytest


if os.getenv("AISTOCK_RUN_MINIQMT_QUOTE_EVIDENCE_DEV_DB") != "1":
    pytest.skip("requires explicitly authorized disposable MiniQMT dev DB", allow_module_level=True)

psycopg2 = pytest.importorskip("psycopg2")

from backend.services.miniqmt_execution_runtime.models import (  # noqa: E402
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeRecord,
)
from backend.tests.miniqmt_execution_runtime.test_quote_evidence import _evidence  # noqa: E402
from backend.execution_algos.adaptive_is.contracts import EvidenceCaptureType, canonical_sha256  # noqa: E402
from backend.services.miniqmt_execution_runtime.quote_event_schema import read_quote_event_schema  # noqa: E402
from backend.services.miniqmt_execution_runtime.repository import (  # noqa: E402
    PostgresMiniQMTExecutionRuntimeRepository,
    QuoteEvidenceEventCandidate,
    QuoteEvidenceIdempotencyConflict,
)


def _dsn() -> str:
    dsn = str(os.getenv("AISTOCK_MINIQMT_QUOTE_EVIDENCE_DEV_DB_DSN") or "").strip()
    if not dsn:
        pytest.skip("requires AISTOCK_MINIQMT_QUOTE_EVIDENCE_DEV_DB_DSN")
    return dsn


@contextmanager
def _conn_factory(*, autocommit: bool = False, manage_transaction: bool = False):
    connection = psycopg2.connect(_dsn(), connect_timeout=5)
    connection.autocommit = autocommit
    try:
        yield connection
        if manage_transaction and not autocommit:
            connection.commit()
    except Exception:
        if not autocommit:
            connection.rollback()
        raise
    finally:
        connection.close()


def _repo() -> PostgresMiniQMTExecutionRuntimeRepository:
    with _conn_factory(autocommit=True) as connection:
        receipt = read_quote_event_schema(connection)
    if receipt.production_ddl_gate != "applied_and_verified":
        pytest.skip("P1-D CHECK migration is not applied to the explicitly selected dev DB")
    return PostgresMiniQMTExecutionRuntimeRepository(conn_factory=_conn_factory)


def _candidate(*, runtime_id: str, action_id: str = "action-dev-db") -> QuoteEvidenceEventCandidate:
    evidence = _evidence(
        runtime_id=runtime_id,
        market_data_id="md_dev_db",
        action_id=action_id,
        source_input_sha256=None,
    )
    return QuoteEvidenceEventCandidate(
        event_id="mqrtevt_" + canonical_sha256({"schema": "miniqmt_quote_event_v1", "evidence_id": evidence.evidence_id}),
        runtime_id=runtime_id,
        event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED,
        event_time=evidence.event_time_utc,
        payload=evidence.runtime_payload(),
        evidence_sha256=evidence.evidence_sha256,
        evidence_contract=evidence,
    )


def _runtime(repo: PostgresMiniQMTExecutionRuntimeRepository, runtime_id: str) -> None:
    repo.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            **MiniQMTExecutionRuntimeConfig(
                runtime_id=runtime_id,
                account_group_id="dev-db-disposable",
                trade_date=date(2026, 7, 13),
                runtime_config_hash="a" * 64,
            ).model_dump()
        )
    )


def test_append_evidence_allocates_sequence_and_runtime_update_in_one_transaction() -> None:
    repo = _repo()
    runtime_id = f"mqrt_p1d_dev_{uuid4().hex}"
    _runtime(repo, runtime_id)
    candidate = _candidate(runtime_id=runtime_id)
    receipt = repo.append_evidence_event_idempotent(candidate)
    assert receipt.durable_ack is True and receipt.readback_verified is True
    assert repo.get_runtime(runtime_id).last_event_sequence == receipt.event.sequence


def test_same_event_retry_readbacks_original_row_and_hash_conflict_rolls_back() -> None:
    repo = _repo()
    runtime_id = f"mqrt_p1d_dev_{uuid4().hex}"
    _runtime(repo, runtime_id)
    candidate = _candidate(runtime_id=runtime_id)
    first = repo.append_evidence_event_idempotent(candidate)
    second = repo.append_evidence_event_idempotent(candidate)
    assert second.event.sequence == first.event.sequence
    conflicting = _candidate(runtime_id=runtime_id, action_id="action-dev-db-conflict")
    object.__setattr__(conflicting, "event_id", candidate.event_id)
    with pytest.raises(QuoteEvidenceIdempotencyConflict):
        repo.append_evidence_event_idempotent(conflicting)
    assert repo.get_runtime(runtime_id).last_event_sequence == first.event.sequence


def test_dev_db_chain_pagination_diagnostics_and_retention_queries_are_executable() -> None:
    repo = _repo()
    runtime_id = f"mqrt_p1d_dev_{uuid4().hex}"
    _runtime(repo, runtime_id)
    action_candidate = _candidate(runtime_id=runtime_id)
    action_receipt = repo.append_evidence_event_idempotent(action_candidate)
    action = action_candidate.evidence_contract
    assert action is not None
    source_child_event = MiniQMTExecutionEvent(
        event_id=f"child_evt_{uuid4().hex}",
        runtime_id=runtime_id,
        sequence=action_receipt.event.sequence + 1,
        event_type=MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED,
        event_time=datetime(2026, 7, 13, 1, 31, tzinfo=UTC),
        source="oms",
        payload={"child_order_id": "child-1"},
    )
    repo.append_event(source_child_event)
    child = _evidence(
        EvidenceCaptureType.CHILD_RECEIPT,
        runtime_id=runtime_id,
        market_data_id=f"md_receipt_{uuid4().hex}",
        action_evidence_id=action.evidence_id,
        source_child_event_id=source_child_event.event_id,
    )
    child_candidate = QuoteEvidenceEventCandidate(
        event_id="mqrtevt_" + canonical_sha256({"schema": "miniqmt_quote_event_v1", "evidence_id": child.evidence_id}),
        runtime_id=runtime_id,
        event_type=MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED,
        event_time=child.event_time_utc,
        payload=child.runtime_payload(),
        evidence_sha256=child.evidence_sha256,
        evidence_contract=child,
    )
    repo.append_evidence_event_idempotent(child_candidate)
    chain = repo.list_evidence_receipts(runtime_id, market_data_id="md_dev_db", limit=10)
    assert {item.event.payload["evidence"]["capture_type"] for item in chain} == {"ACTION_INPUT", "CHILD_RECEIPT"}
    assert repo.list_quote_events_page(runtime_id, symbol="000001.SZ", after_sequence=0, after_event_id="", limit=10)
    summary = repo.quote_diagnostics_summary(runtime_id, symbol="000001.SZ")
    assert summary["per_symbol"][0]["capture_count"] == 2
    assert action.evidence_id in repo.existing_evidence_ids(runtime_id, evidence_ids=(action.evidence_id,))
    assert repo.list_events_by_ids(runtime_id, event_ids=(source_child_event.event_id,))[0].event_id == source_child_event.event_id
    assert repo.quote_event_schema_gate() == "applied_and_verified"
    assert isinstance(repo.prune_runtime(runtime_id=runtime_id, reason="p1d_dev_db_query_validation"), dict)
