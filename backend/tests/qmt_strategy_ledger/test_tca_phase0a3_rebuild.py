from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from backend.services.qmt_strategy_ledger.tca_rebuild import (
    TCA_CANONICAL_QUERY_SHA256,
    TcaRebuildRequest,
    _completed_receipt_values,
    _reject_stale_snapshot,
    _sanitize_failure_context,
    build_rebuild_draft,
)
from backend.services.qmt_strategy_ledger.reconciliation import QmtStrategyLedgerReconciliationService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.qmt_strategy_ledger.sync_service import SyncSummary
from backend.services.qmt_strategy_ledger.tca_repository import (
    ExecutionTcaEvidenceRepository,
    ExecutionTcaRebuildScope,
    ExecutionTcaSourceSnapshot,
)


TRADE_DATE = date(2026, 7, 10)
DEADLINE = datetime(2026, 7, 10, 7, 0, tzinfo=UTC)
AS_OF = DEADLINE + timedelta(minutes=10)


def test_rebuild_draft_is_order_independent_and_covers_exact_memberships() -> None:
    snapshot = _snapshot()
    reversed_snapshot = ExecutionTcaSourceSnapshot(
        **{name: tuple(reversed(getattr(snapshot, name))) for name in snapshot.__dataclass_fields__}
    )
    request = _request("RECONCILED_FINAL")

    first = build_rebuild_draft(
        snapshot=snapshot,
        request=request,
        source_snapshot_started_at=AS_OF,
        source_snapshot_completed_at=AS_OF + timedelta(milliseconds=10),
    )
    second = build_rebuild_draft(
        snapshot=reversed_snapshot,
        request=request,
        source_snapshot_started_at=AS_OF,
        source_snapshot_completed_at=AS_OF + timedelta(milliseconds=20),
    )

    assert first.canonical_input_sha256 == second.canonical_input_sha256
    assert first.canonical_output_sha256 == second.canonical_output_sha256
    assert first.results[0].values["result_status"] == "FINAL"
    assert first.results[0].values["finality_evidence"]["broker_query_evidence_complete"] is True
    assert {item["observation_role"] for item in first.results[0].observation_memberships} == {
        "CORE",
        "TIMING",
        "FEE",
        "ATTRIBUTION",
    }
    assert first.coverage["planning_subject_membership_ratio"] == "1.000000000000"
    assert first.source_row_counts["runtime_events"] == 2
    assert first.source_row_counts["child_orders"] == 1
    receipt_values = _completed_receipt_values(
        request=request,
        draft=first,
        receipt_id="receipt-1",
        receipt_generation=1,
        supersedes_receipt_id=None,
        started_at=AS_OF,
        completed_at=AS_OF,
        attempt_started_at=AS_OF,
        attempt_completed_at=AS_OF,
    )
    assert receipt_values["eligible_notional_cny"] == Decimal("1010.0")
    assert receipt_values["coverage"]["completion_by_deadline_notional"] == Decimal("0.4")


def test_latest_reconciliation_without_query_proof_remains_provisional() -> None:
    snapshot = _snapshot()
    reconciliation = dict(snapshot.reconciliations[0])
    reconciliation["summary_json"] = {"issue_count": 0, "sync_summary": {"orders_seen": 1, "trades_seen": 1}}
    snapshot = ExecutionTcaSourceSnapshot(
        **{
            **{name: getattr(snapshot, name) for name in snapshot.__dataclass_fields__},
            "reconciliations": (reconciliation,),
        }
    )
    draft = build_rebuild_draft(
        snapshot=snapshot,
        request=_request("RECONCILED_FINAL"),
        source_snapshot_started_at=AS_OF,
        source_snapshot_completed_at=AS_OF,
    )

    assert draft.results[0].values["result_status"] == "PROVISIONAL"
    assert draft.results[0].values["finality_evidence"]["broker_query_evidence_complete"] is False


def test_invalid_arrival_quality_never_uses_leftover_mid_price() -> None:
    snapshot = _snapshot()
    parent = {**snapshot.parents[0], "arrival_quality": "CLOCK_SKEW"}
    snapshot = ExecutionTcaSourceSnapshot(
        **{**{name: getattr(snapshot, name) for name in snapshot.__dataclass_fields__}, "parents": (parent,)}
    )
    draft = build_rebuild_draft(
        snapshot=snapshot,
        request=_request("RECONCILED_FINAL"),
        source_snapshot_started_at=AS_OF,
        source_snapshot_completed_at=AS_OF,
    )
    result = draft.results[0].values

    assert result["arrival_is_gross_cny"] is None
    assert result["decision_calculation_mode"] == "DIRECT"
    assert result["benchmark_coverage"]["arrival_quality"] == "CLOCK_SKEW"


def test_late_predeadline_trade_creates_new_parent_and_receipt_hashes() -> None:
    initial = _snapshot()
    late_trade = {
        **initial.trades[0],
        "trade_id": "trade-2",
        "quantity": 60,
        "amount": Decimal("612"),
        "trade_time": DEADLINE - timedelta(seconds=10),
        "canonical_trade_fact_sha256": "2" * 64,
    }
    late_observation = {
        **initial.trade_observations[0],
        "trade_observation_id": "obs-2",
        "trade_id": "trade-2",
        "quantity": 60,
        "amount": Decimal("612"),
        "broker_trade_time": DEADLINE - timedelta(seconds=10),
        "canonical_trade_fact_sha256": "2" * 64,
        "timing_observation_sha256": "6" * 64,
        "attribution_sha256": "7" * 64,
        "fee_observation_sha256": "8" * 64,
        "raw_observation_sha256": "9" * 64,
    }
    order = {**initial.orders[0], "traded_volume": 100}
    late = ExecutionTcaSourceSnapshot(
        **{
            **{name: getattr(initial, name) for name in initial.__dataclass_fields__},
            "orders": (order,),
            "trades": (*initial.trades, late_trade),
            "trade_observations": (*initial.trade_observations, late_observation),
        }
    )
    request = _request("RECONCILED_FINAL")
    first = build_rebuild_draft(snapshot=initial, request=request, source_snapshot_started_at=AS_OF, source_snapshot_completed_at=AS_OF)
    second = build_rebuild_draft(snapshot=late, request=request, source_snapshot_started_at=AS_OF, source_snapshot_completed_at=AS_OF)

    assert first.canonical_input_sha256 != second.canonical_input_sha256
    assert first.canonical_output_sha256 != second.canonical_output_sha256
    assert first.results[0].canonical_input_sha256 != second.results[0].canonical_input_sha256
    assert first.results[0].values["deadline_filled_quantity"] == 40
    assert second.results[0].values["deadline_filled_quantity"] == 100


def test_late_fee_correction_supersedes_parent_input_without_changing_core_trade() -> None:
    initial = _snapshot()
    correction = {
        **initial.trade_observations[0],
        "trade_observation_id": "obs-fee-correction",
        "ingest_source": "BROKER_SNAPSHOT_SYNC",
        "observed_at": AS_OF + timedelta(minutes=1),
        "commission": Decimal("2.00"),
        "fee_observation_sha256": "f" * 64,
        "raw_observation_sha256": "e" * 64,
    }
    corrected = ExecutionTcaSourceSnapshot(
        **{
            **{name: getattr(initial, name) for name in initial.__dataclass_fields__},
            "trade_observations": (*initial.trade_observations, correction),
        }
    )
    request = _request("RECONCILED_FINAL")
    first = build_rebuild_draft(snapshot=initial, request=request, source_snapshot_started_at=AS_OF, source_snapshot_completed_at=AS_OF)
    second = build_rebuild_draft(snapshot=corrected, request=request, source_snapshot_started_at=AS_OF, source_snapshot_completed_at=AS_OF)

    assert first.results[0].canonical_input_sha256 != second.results[0].canonical_input_sha256
    assert first.results[0].values["deadline_fee_actual_cny"] == Decimal("1.00000000")
    assert second.results[0].values["deadline_fee_actual_cny"] == Decimal("2.00000000")


def test_advisory_lock_uses_sha256_first_eight_bytes_as_signed_big_endian() -> None:
    cursor = _LockCursor()
    repository = ExecutionTcaEvidenceRepository()
    positive = repository.acquire_scope_lock(cursor=cursor, receipt_scope_hash="00" * 32)
    negative = repository.acquire_scope_lock(cursor=cursor, receipt_scope_hash="ff" * 32)

    assert positive == 0
    assert negative == -1
    assert cursor.calls == [
        ("SELECT pg_advisory_xact_lock(%s)", (0,)),
        ("SELECT pg_advisory_xact_lock(%s)", (-1,)),
    ]
    assert len(TCA_CANONICAL_QUERY_SHA256) == 64


def test_stale_source_snapshot_is_rejected_before_idempotent_reuse() -> None:
    head = {"source_snapshot_started_at": AS_OF}

    _reject_stale_snapshot(head, AS_OF, "receipt")
    with pytest.raises(RuntimeError, match="ADAPTIVE_IS_TCA_STALE_SNAPSHOT_WRITE"):
        _reject_stale_snapshot(head, AS_OF - timedelta(microseconds=1), "receipt")
    assert _sanitize_failure_context(
        {"account_id": "account-1", "nested": {"broker_account": "unknown"}},
        {"account-1": "acct-hmac-1"},
    ) == {"account_id": "acct-hmac-1", "nested": {"broker_account": "REDACTED"}}


def test_db_generated_audit_timestamps_do_not_change_canonical_source_hash() -> None:
    first = _snapshot()
    parent = {**first.parents[0], "created_at": AS_OF}
    changed = {**parent, "created_at": AS_OF + timedelta(hours=1), "last_synced_at": AS_OF + timedelta(hours=2)}
    first = ExecutionTcaSourceSnapshot(
        **{**{name: getattr(first, name) for name in first.__dataclass_fields__}, "parents": (parent,)}
    )
    second = ExecutionTcaSourceSnapshot(
        **{**{name: getattr(first, name) for name in first.__dataclass_fields__}, "parents": (changed,)}
    )
    request = _request("RECONCILED_FINAL")
    first_draft = build_rebuild_draft(snapshot=first, request=request, source_snapshot_started_at=AS_OF, source_snapshot_completed_at=AS_OF)
    second_draft = build_rebuild_draft(snapshot=second, request=request, source_snapshot_started_at=AS_OF, source_snapshot_completed_at=AS_OF)

    assert first_draft.canonical_input_sha256 == second_draft.canonical_input_sha256
    assert first_draft.canonical_output_sha256 == second_draft.canonical_output_sha256


def test_reconciliation_receipt_proves_conflict_head_scan_even_when_empty() -> None:
    zero = Decimal(0)
    summary = SyncSummary(
        account_id="account-1",
        trade_date=TRADE_DATE,
        orders_seen=0,
        orders_upserted=0,
        trades_seen=0,
        trades_inserted=0,
        trades_existing=0,
        unattributed_orders=0,
        unattributed_trades=0,
        status_events_appended=0,
        orders_trade_reconciled=0,
        lots_created=0,
        cash_entries_appended=0,
        buy_fill_settled_amount=zero,
        buy_fill_fee_amount=zero,
        sell_fill_received_amount=zero,
        sell_fill_fee_amount=zero,
        sell_fill_realized_pnl=zero,
        buy_freeze_released_amount=zero,
        accounts_revalued=0,
        positions_seen=0,
        orders_query_succeeded=True,
        trades_query_succeeded=True,
        orders_snapshot_count=0,
        trades_snapshot_count=0,
        orders_snapshot_sha256="a" * 64,
        trades_snapshot_sha256="b" * 64,
    )
    report = QmtStrategyLedgerReconciliationService(
        repository=InMemoryQmtStrategyLedgerRepository()
    ).reconcile_snapshot(
        account_id="account-1",
        trade_date=TRADE_DATE,
        broker_positions=[],
        sync_summary=summary,
    )
    evidence = report.run.summary_json["sync_summary"]

    assert evidence["trade_conflict_heads_scanned"] is True
    assert evidence["trade_conflict_head_count"] == 0
    assert len(evidence["trade_conflict_heads_sha256"]) == 64


class _LockCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[int]]] = []

    def execute(self, sql: str, params: tuple[int]) -> None:
        self.calls.append((sql, params))


def _request(snapshot_kind: str) -> TcaRebuildRequest:
    return TcaRebuildRequest(
        scope=ExecutionTcaRebuildScope(
            binding_ids=("binding-1",),
            trade_date_from=TRADE_DATE,
            trade_date_to=TRADE_DATE,
        ),
        snapshot_kind=snapshot_kind,
        as_of_time=AS_OF,
        account_pseudonyms={"account-1": "acct-hmac-1"},
        account_pseudonym_key_version="hmac-v1",
        operator_pseudonym="operator-hmac-1",
        code_commit="a" * 40,
    )


def _snapshot() -> ExecutionTcaSourceSnapshot:
    parent = {
        "parent_intent_id": "parent-1",
        "parent_revision": 1,
        "runtime_id": "runtime-1",
        "account_id": "account-1",
        "trade_date": TRADE_DATE,
        "symbol": "000001.SZ",
        "side": "BUY",
        "eligible_quantity": 100,
        "decision_mid_price": Decimal("10.00"),
        "decision_quality": "VALID",
        "arrival_mid_price": Decimal("10.10"),
        "arrival_quality": "VALID",
        "deadline": DEADLINE,
        "deadline_mark_max_age_ms": 10_000,
        "clock_skew_tolerance_ms": 1_000,
        "execution_policy_sha256": "e" * 64,
        "eligibility_class": "ELIGIBLE_NOW",
        "eligibility_quality": "VALID",
        "eligibility_evidence": {},
    }
    trade = {
        "account_id": "account-1",
        "trade_date": TRADE_DATE,
        "trade_id": "trade-1",
        "intent_id": "parent-1",
        "qmt_order_id": "order-1",
        "price": Decimal("10.20"),
        "quantity": 40,
        "amount": Decimal("408"),
        "trade_time": DEADLINE - timedelta(minutes=1),
        "canonical_trade_fact_sha256": "1" * 64,
    }
    observation = {
        "trade_observation_id": "obs-1",
        "account_id": "account-1",
        "trade_date": TRADE_DATE,
        "trade_id": "trade-1",
        "intent_id": "parent-1",
        "qmt_order_id": "order-1",
        "ingest_source": "BROKER_CALLBACK",
        "observed_at": DEADLINE,
        "broker_trade_time": trade["trade_time"],
        "commission": Decimal("1.00"),
        "fee_evidence_level": "TRADE_LEVEL",
        "canonical_trade_fact_sha256": "1" * 64,
        "timing_observation_sha256": "2" * 64,
        "attribution_sha256": "3" * 64,
        "fee_observation_sha256": "4" * 64,
        "raw_observation_sha256": "5" * 64,
    }
    tick = {
        "event_id": "tick-1",
        "runtime_id": "runtime-1",
        "sequence": 1,
        "event_type": "TICK",
        "event_time": DEADLINE - timedelta(seconds=1),
        "source": "gateway",
        "archived_at": AS_OF,
        "payload": {
            "symbol": "000001.SZ",
            "market_time": (DEADLINE - timedelta(seconds=1)).isoformat(),
            "bid_price_1": "10.29",
            "ask_price_1": "10.31",
        },
    }
    child_submitted_at = trade["trade_time"] - timedelta(seconds=30)
    child_tick = {
        **tick,
        "event_id": "tick-child-1",
        "sequence": 0,
        "event_time": child_submitted_at,
        "payload": {
            "symbol": "000001.SZ",
            "market_time": child_submitted_at.isoformat(),
            "bid_price_1": "10.09",
            "ask_price_1": "10.11",
        },
    }
    reconciliation = {
        "run_id": "recon-1",
        "account_id": "account-1",
        "trade_date": TRADE_DATE,
        "status": "SUCCEEDED",
        "started_at": DEADLINE + timedelta(minutes=1),
        "completed_at": DEADLINE + timedelta(minutes=5),
        "summary_json": {
            "issue_count": 0,
            "sync_summary": {
                "orders_query_succeeded": True,
                "trades_query_succeeded": True,
                "orders_snapshot_count": 1,
                "trades_snapshot_count": 1,
                "orders_snapshot_sha256": "a" * 64,
                "trades_snapshot_sha256": "b" * 64,
                "trade_conflict_heads_scanned": True,
                "trade_conflict_heads_sha256": "c" * 64,
            },
        },
    }
    return ExecutionTcaSourceSnapshot(
        planning_subjects=(
            {
                "planning_subject_id": "subject-1",
                "planning_class": "EMITTED_PARENT",
                "emitted_parent_intent_id": "parent-1",
            },
        ),
        parents=(parent,),
        runtime_events=(tick, child_tick),
        child_orders=(
            {
                "child_order_id": "child-1",
                "parent_intent_id": "parent-1",
                "runtime_id": "runtime-1",
                "broker_order_id": "order-1",
                "submitted_at": child_submitted_at,
                "archived_at": AS_OF,
            },
        ),
        orders=(
            {
                "intent_id": "parent-1",
                "account_id": "account-1",
                "qmt_order_id": "order-1",
                "order_status": 56,
                "traded_volume": 40,
            },
        ),
        order_status_events=(),
        trades=(trade,),
        trade_observations=(observation,),
        trade_conflicts=(),
        reconciliations=(reconciliation,),
        reconciliation_issues=(),
        unattributed_orders=(),
        unattributed_trades=(),
    )
