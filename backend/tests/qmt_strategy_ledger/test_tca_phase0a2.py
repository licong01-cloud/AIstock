from __future__ import annotations

import re
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.qmt_strategy_ledger.tca_models import (
    ExecutionPlanningSubject,
    TcaInsertOutcome,
    build_trade_observation,
    canonical_trade_fact_sha256,
)
from backend.services.qmt_strategy_ledger.models import ReconciliationIssueRecord
from backend.services.qmt_strategy_ledger.reconciliation import QmtStrategyLedgerReconciliationService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.qmt_strategy_ledger.tca_projector import (
    TcaProjectionPolicy,
    project_execution_tca_evidence,
)
from backend.services.qmt_strategy_ledger.tca_repository import ExecutionTcaEvidenceRepository
from backend.services.simulation_runtime.models import (
    ExecutionPlan,
    ExecutionPlanIntent,
    SimulationBrokerBackend,
    SimulationDailyRun,
    TradingRuleDecision,
    canonical_json_sha256,
)
from backend.services.trading_core.models import OrderSide
from backend.services.trading_core.tca_sidecar import TCA_OBSERVATION_KEY


FORWARD = Path("backend/migrations/miniqmt_execution_tca_phase0a_20260711.sql")
ROLLBACK = Path("backend/migrations/miniqmt_execution_tca_phase0a_20260711.rollback.sql")
EXPECTED_TABLES = {
    "execution_planning_subject",
    "execution_parent_benchmark",
    "execution_tca_trade_observation",
    "execution_tca_trade_conflict",
    "execution_tca_mark",
    "execution_tca_rebuild_receipt",
    "execution_parent_tca",
    "execution_tca_receipt_planning_subject",
    "execution_tca_receipt_result",
    "execution_tca_result_mark",
    "execution_tca_result_trade_observation",
}


def test_phase0a2_migration_has_exact_additive_schema_and_symmetric_rollback() -> None:
    forward = FORWARD.read_text(encoding="utf-8")
    rollback = ROLLBACK.read_text(encoding="utf-8")
    created = set(
        re.findall(
            r"CREATE TABLE IF NOT EXISTS qmt_strategy\.([a-z_]+)",
            forward,
        )
    )

    assert created == EXPECTED_TABLES
    for table in EXPECTED_TABLES:
        assert f"DROP TABLE IF EXISTS qmt_strategy.{table}" in rollback
        assert f"COMMENT ON TABLE qmt_strategy.{table} IS" in forward
    for column in (
        "first_ingest_source",
        "first_ingested_at",
        "canonical_trade_fact_sha256",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in forward
        assert f"DROP COLUMN IF EXISTS {column}" in rollback
        assert f"COMMENT ON COLUMN qmt_strategy.trade_ledger.{column} IS" in forward
    assert "ON DELETE CASCADE" not in forward
    assert "reject_execution_tca_mutation" in forward
    assert "information_schema.columns" in forward
    assert "execution_tca_result_trade_observation" in forward
    assert "ux_tca_parent_result_successor" in forward
    assert "ux_tca_receipt_successor" in forward
    assert "ux_tca_mark_successor" in forward
    assert "ux_tca_trade_conflict_successor" in forward
    assert "ck_tca_trade_ledger_provenance" in forward


def test_trade_observation_separates_canonical_transport_timing_and_fee_hashes() -> None:
    observed_at = datetime(2026, 7, 11, 2, 30, tzinfo=UTC)
    common = dict(
        account_id="sim-account",
        trade_date=date(2026, 7, 11),
        trade_id="trade-1",
        intent_id="parent-1",
        qmt_order_id="order-1",
        child_order_id="child-1",
        symbol="000001.SZ",
        side="BUY",
        observed_at=observed_at,
        broker_trade_time=observed_at,
        price=Decimal("10.01"),
        quantity=100,
        commission=Decimal("5.00"),
    )
    callback = build_trade_observation(
        **common,
        ingest_source="BROKER_CALLBACK",
        raw_payload={"transport": "callback", "commission": "5.00"},
    )
    snapshot = build_trade_observation(
        **common,
        ingest_source="BROKER_SNAPSHOT_SYNC",
        raw_payload={"transport": "snapshot", "commission": "5.00"},
    )

    assert callback.values["canonical_trade_fact_sha256"] == snapshot.values["canonical_trade_fact_sha256"]
    assert callback.values["timing_observation_sha256"] == snapshot.values["timing_observation_sha256"]
    assert callback.values["raw_observation_sha256"] != snapshot.values["raw_observation_sha256"]
    assert callback.values["trade_observation_id"] != snapshot.values["trade_observation_id"]
    assert callback.values["fee_evidence_level"] == "TRADE_LEVEL"


def test_trade_canonical_hash_excludes_transport_and_commission() -> None:
    digest = canonical_trade_fact_sha256(
        account_id="sim-account",
        trade_date=date(2026, 7, 11),
        trade_id="trade-1",
        qmt_order_id="order-1",
        symbol="000001.SZ",
        side="BUY",
        price=Decimal("10.01"),
        quantity=100,
    )
    assert re.fullmatch(r"[0-9a-f]{64}", digest)


def test_immutable_row_rejects_missing_identity_and_mutation() -> None:
    values = _planning_subject_values()
    row = ExecutionPlanningSubject(values)

    with pytest.raises(TypeError):
        row.values["decision"] = "REJECT"  # type: ignore[index]
    with pytest.raises(ValueError, match="planning_subject_id"):
        ExecutionPlanningSubject({**values, "planning_subject_id": ""})


def test_repository_returns_inserted_idempotent_and_conflict_without_overwrite() -> None:
    values = _planning_subject_values()
    row = ExecutionPlanningSubject(values)
    cursor = _ImmutableInsertCursor(existing={})
    repo = ExecutionTcaEvidenceRepository()

    assert repo.insert_immutable(cursor=cursor, row=row) == TcaInsertOutcome.INSERTED
    assert repo.insert_immutable(cursor=cursor, row=row) == TcaInsertOutcome.IDEMPOTENT
    conflicting = ExecutionPlanningSubject({**values, "evidence_sha256": "b" * 64})
    assert repo.insert_immutable(cursor=cursor, row=conflicting) == TcaInsertOutcome.CONFLICT
    assert cursor.existing[(values["planning_subject_id"],)]["evidence_sha256"] == "a" * 64


def test_projector_keeps_rejected_subject_and_materializes_emitted_parent() -> None:
    plan, run, batch_metadata = _plan_run_and_carrier()
    result = project_execution_tca_evidence(
        execution_plan=plan,
        run=run,
        account_id="sim-account",
        policy=_projection_policy(plan),
        batch_metadata_by_id={"batch-1": batch_metadata},
        known_order_intent_ids=frozenset({"parent-emitted"}),
    )

    assert len(result.planning_subjects) == 2
    assert {row.values["decision"] for row in result.planning_subjects} == {"ADJUST", "REJECT"}
    rejected = next(row for row in result.planning_subjects if row.values["decision"] == "REJECT")
    assert rejected.values["emitted_parent_intent_id"] is None
    assert len(result.parent_benchmarks) == 1
    benchmark = result.parent_benchmarks[0].values
    assert benchmark["decision_quality"] == "VALID"
    assert benchmark["arrival_quality"] == "VALID"
    assert benchmark["eligible_quantity"] == 100
    assert benchmark["qmt_order_intent_id"] == "parent-emitted"
    assert result.issues == ()


def test_projector_missing_carrier_is_loud_but_does_not_drop_parent() -> None:
    plan, run, _ = _plan_run_and_carrier()
    run = run.model_copy(update={"run_payload_json": {}})
    result = project_execution_tca_evidence(
        execution_plan=plan,
        run=run,
        account_id="sim-account",
        policy=_projection_policy(plan),
        batch_metadata_by_id={},
    )

    assert len(result.parent_benchmarks) == 1
    assert result.parent_benchmarks[0].values["arrival_quality"] == "CAPTURE_FAILED"
    assert {issue.reason_code for issue in result.issues} == {
        "ADAPTIVE_IS_TCA_DECISION_CAPTURE_MISSING",
        "ADAPTIVE_IS_TCA_BATCH_CARRIER_MISSING",
        "ADAPTIVE_IS_TCA_ARRIVAL_CAPTURE_MISSING",
        "ADAPTIVE_IS_TCA_ELIGIBILITY_CAPTURE_MISSING",
    }


def test_projector_rejects_non_miniqmt_scope_before_materialization() -> None:
    plan, run, batch_metadata = _plan_run_and_carrier()
    run = run.model_copy(update={"broker_backend": SimulationBrokerBackend.LOCAL_SIM})

    with pytest.raises(ValueError, match="ADAPTIVE_IS_TCA_NON_MINIQMT_SCOPE_DENIED"):
        project_execution_tca_evidence(
            execution_plan=plan,
            run=run,
            account_id="sim-account",
            policy=_projection_policy(plan),
            batch_metadata_by_id={"batch-1": batch_metadata},
        )


def test_reconciliation_maps_open_tca_conflict_and_cannot_report_success() -> None:
    repo = _ConflictMappingRepository()
    report = QmtStrategyLedgerReconciliationService(repository=repo).reconcile_snapshot(
        account_id="sim-account",
        trade_date=date(2026, 7, 11),
        broker_positions=[],
    )

    assert report.run.status == "WARNING"
    assert report.run.summary_json["issue_count"] == 1
    assert report.issues[0].issue_type == "TRADE_KEY_CONFLICT"
    assert report.issues[0].context["stage"] == "reconciliation_trade_conflict_mapping"


def _planning_subject_values() -> dict[str, object]:
    return {
        "planning_subject_id": "subject-1",
        "trading_rule_decision_id": "decision-1",
        "run_id": "run-1",
        "execution_plan_id": "plan-1",
        "execution_plan_hash": "1" * 64,
        "binding_id": "binding-1",
        "binding_hash": "2" * 64,
        "strategy_id": "strategy-1",
        "portfolio_id": "portfolio-1",
        "package_id": "package-1",
        "release_id": "release-1",
        "selection_evidence_id": "selection-1",
        "trade_date": date(2026, 7, 11),
        "symbol": "000001.SZ",
        "side": "BUY",
        "planning_requested_quantity": 100,
        "trading_rule_legal_quantity": 100,
        "decision": "EMIT",
        "planning_class": "EMITTED_PARENT",
        "reason_code": "OK",
        "emitted_parent_intent_id": "parent-1",
        "trading_rule_version": "trading-rule-v1",
        "evidence": {"decision": "EMIT"},
        "evidence_sha256": "a" * 64,
    }


class _ImmutableInsertCursor:
    def __init__(self, *, existing: dict[tuple[object, ...], dict[str, object]]) -> None:
        self.existing = existing
        self._result: object = None
        self.description: list[tuple[str]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        if sql.lstrip().startswith("INSERT"):
            columns = re.search(r"\((.*?)\)\s*VALUES", sql, re.DOTALL)
            assert columns is not None
            names = [name.strip() for name in columns.group(1).split(",")]
            values = dict(zip(names, params, strict=True))
            key = (values["planning_subject_id"],)
            if key in self.existing:
                self._result = None
            else:
                self.existing[key] = values
                self._result = key
            return
        key = tuple(params)
        row = self.existing.get(key)
        self.description = [(name,) for name in row] if row else []
        self._result = tuple(row.values()) if row else None

    def fetchone(self) -> object:
        result = self._result
        self._result = None
        return result


class _ConflictMappingRepository(InMemoryQmtStrategyLedgerRepository):
    def append_open_tca_conflicts_to_reconciliation(
        self,
        *,
        run_id: str,
        account_id: str,
        trade_date: date,
    ) -> tuple[ReconciliationIssueRecord, ...]:
        _ = (account_id, trade_date)
        issue = ReconciliationIssueRecord(
            issue_id="issue-tca-conflict",
            run_id=run_id,
            issue_type="TRADE_KEY_CONFLICT",
            severity="ERROR",
            message="canonical trade fact conflict",
            trade_id="trade-1",
            context={
                "reason_code": "ADAPTIVE_IS_TCA_TRADE_KEY_CONFLICT",
                "stage": "reconciliation_trade_conflict_mapping",
            },
        )
        self.append_reconciliation_issue(issue)
        return (issue,)


def _plan_run_and_carrier() -> tuple[ExecutionPlan, SimulationDailyRun, dict[str, object]]:
    emitted = _decision(
        symbol="000001.SZ",
        requested=200,
        legal=100,
        decision="ADJUST",
        reason="BOARD_LOT_ADJUSTED",
    )
    rejected = _decision(
        symbol="000002.SZ",
        requested=100,
        legal=0,
        decision="REJECT",
        reason="SUSPENDED_BY_SUSPEND_D",
    )
    plan_payload = {"schema_version": "test_execution_plan_v1"}
    plan_hash = canonical_json_sha256(plan_payload)
    plan_id = f"plan_{plan_hash[:16]}"
    intent = ExecutionPlanIntent(
        intent_id="parent-emitted",
        plan_id=plan_id,
        strategy_id="strategy",
        portfolio_id="portfolio",
        package_id="package",
        release_id="release",
        release_hash="3" * 64,
        binding_id="binding",
        binding_hash="4" * 64,
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        delta_quantity=100,
        order_quantity=100,
        current_quantity=0,
        rebalance_reason="target_rebalance",
        trading_rule_decision_id=emitted.decision_id,
        schedule_window={"mode": "full_day"},
        price_policy={"order_type": "LIMIT"},
    )
    plan = ExecutionPlan(
        plan_id=plan_id,
        strategy_id="strategy",
        portfolio_id="portfolio",
        package_id="package",
        release_id="release",
        release_hash="3" * 64,
        binding_id="binding",
        binding_hash="4" * 64,
        selection_evidence_id="selection",
        selection_evidence_hash="5" * 64,
        target_trade_date=date(2026, 7, 11),
        execution_policy_version_id="execution-policy-v1",
        execution_policy_sha256="6" * 64,
        tail_policy_version_id="tail-policy-v1",
        tail_policy_sha256="7" * 64,
        intents=[intent],
        trading_rule_decisions=[emitted, rejected],
        plan_payload_json=plan_payload,
        plan_hash=plan_hash,
    )
    now = datetime(2026, 7, 11, 2, 30, tzinfo=UTC)
    decision_capture = {
        "benchmark_type": "EXECUTION_PLAN_COMMIT_MID",
        "capture_fetch_started_at": now.isoformat(),
        "benchmark_event_at": now.isoformat(),
        "quote_market_time": now.isoformat(),
        "quote_received_at": now.isoformat(),
        "bid_price_1": 10.00,
        "ask_price_1": 10.02,
        "mid_price": 10.01,
        "quote_source": "MINIQMT_REALTIME.broker_quote",
        "quote_age_ms": 0,
        "transport_latency_ms": 0,
        "quality": "VALID",
        "raw_quote_sha256": "8" * 64,
    }
    arrival_capture = {
        **decision_capture,
        "benchmark_type": "OPERATIONAL_FIRST_TICK_MID",
        "quote_offset_ms": 0,
    }
    eligibility = {
        "eligibility_as_of": now.isoformat(),
        "managed_request_quantity_before_cash": 100,
        "managed_request_quantity_after_cash": 100,
        "eligible_now_quantity": 100,
        "conditional_eligible_quantity": 0,
        "execution_ineligible_quantity": 0,
        "eligibility_class": "ELIGIBLE_NOW",
        "eligibility_rule_version": "preflight-v1",
        "deadline": now.replace(hour=6, minute=55).isoformat(),
        "dependency_parent_ids": [],
        "preflight_result": {"allowed": True},
        "preflight_result_sha256": "9" * 64,
    }
    run = SimulationDailyRun(
        run_id="run-1",
        trade_date=plan.target_trade_date,
        strategy_id=plan.strategy_id,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        package_id=plan.package_id,
        manifest_sha256="a" * 64,
        release_id=plan.release_id,
        release_hash=plan.release_hash,
        binding_id=plan.binding_id,
        binding_hash=plan.binding_hash,
        selection_evidence_id=plan.selection_evidence_id,
        selection_artifact_hash=plan.selection_evidence_hash,
        execution_plan_id=plan.plan_id,
        execution_plan_hash=plan.plan_hash,
        run_payload_json={
            TCA_OBSERVATION_KEY: {
                "execution_plan_id": plan.plan_id,
                "execution_plan_hash": plan.plan_hash,
                "decision_capture_by_parent": {"parent-emitted": decision_capture},
                "capture_batch_id_by_parent": {"parent-emitted": "batch-1"},
                "capture_errors": {},
            }
        },
    )
    batch_metadata = {
        TCA_OBSERVATION_KEY: {
            "logical_tca_scope_hash": "b" * 64,
            "capture_batch_id": "batch-1",
            "arrival_capture_by_parent": {"parent-emitted": arrival_capture},
            "managed_preflight_eligibility_by_parent": {"parent-emitted": eligibility},
            "capture_errors": {},
        }
    }
    return plan, run, batch_metadata


def _decision(
    *,
    symbol: str,
    requested: int,
    legal: int,
    decision: str,
    reason: str,
) -> TradingRuleDecision:
    payload = {
        "schema_version": "trading_rule_decision_v1",
        "symbol": symbol,
        "market_board": "MAIN",
        "side": "BUY",
        "requested_quantity": requested,
        "legal_quantity": legal,
        "lot_rule": {"lot_size": 100},
        "price_limit_rule": {},
        "tplus1_available_quantity": None,
        "decision": decision,
        "reason_code": reason,
        "source_version": "trading-rule-v1",
    }
    digest = canonical_json_sha256(payload)
    return TradingRuleDecision(
        decision_id=f"trd_{digest[:16]}",
        decision_hash=digest,
        **{key: value for key, value in payload.items() if key != "schema_version"},
    )


def _projection_policy(plan: ExecutionPlan) -> TcaProjectionPolicy:
    return TcaProjectionPolicy(
        benchmark_schema_version="execution_benchmark_capture_v1",
        benchmark_policy_version="phase0a-test-v1",
        capture_code_version="phase0a1",
        execution_policy_id=plan.execution_policy_version_id,
        execution_policy_sha256=plan.execution_policy_sha256,
        runtime_config_sha256="c" * 64,
        time_parser_version="xtquant-trade-time-v1",
        unit_mapping_version="shares-v1",
        calendar_version="cn-a-calendar-v1",
        deadline_mark_policy_version="deadline-mark-v1",
        deadline_mark_max_age_ms=5_000,
        arrival_forward_window_ms=2_000,
        clock_skew_tolerance_ms=1_000,
        benchmark_max_transport_latency_ms=3_000,
    )
