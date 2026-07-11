"""Deterministic SIM-only Phase 0A TCA rebuild and immutable materialization."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

import psycopg2.extras

from backend.db.pg_pool import get_conn

from .models import is_terminal_order_status
from .tca_calculator import (
    CALCULATOR_VERSION,
    FORMULA_VERSION,
    MARK_POLICY_VERSION,
    QuoteCandidate,
    SelectedMark,
    TcaCalculationError,
    TcaCalculationInput,
    TcaFill,
    calculate_parent_tca,
    select_mark,
)
from .tca_models import (
    TCA_SCHEMA_VERSION,
    ExecutionParentTca,
    ExecutionTcaMark,
    ExecutionTcaRebuildReceipt,
    ExecutionTcaReceiptPlanningSubject,
    ExecutionTcaReceiptResult,
    ExecutionTcaResultMark,
    ExecutionTcaResultTradeObservation,
    TcaMaterializationBundle,
    canonical_tca_manifest_sha256 as canonical_json_sha256,
    content_id,
)
from .tca_repository import (
    ExecutionTcaEvidenceRepository,
    ExecutionTcaRebuildScope,
    ExecutionTcaSourceRepository,
    ExecutionTcaSourceSnapshot,
)


TCA_QUERY_VERSION = "miniqmt_execution_tca_query_v1"
TCA_TRADE_PROVENANCE_POLICY_VERSION = "miniqmt_execution_tca_trade_provenance_v1"
TCA_CANONICAL_QUERY_SHA256 = canonical_json_sha256(
    {
        "query_version": TCA_QUERY_VERSION,
        "root": "planning_subject LEFT JOIN execution_parent_benchmark",
        "sources": [
            "execution_runtime_event_including_archived",
            "execution_child_order_including_archived",
            "order_ledger",
            "order_status_event",
            "trade_ledger",
            "execution_tca_trade_observation",
            "execution_tca_trade_conflict",
            "reconciliation_run",
            "reconciliation_issue",
            "unattributed_order",
            "unattributed_trade",
        ],
    }
)
_CHINA_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True, slots=True)
class TcaRebuildRequest:
    scope: ExecutionTcaRebuildScope
    snapshot_kind: str
    as_of_time: datetime
    account_pseudonyms: Mapping[str, str]
    account_pseudonym_key_version: str
    operator_pseudonym: str
    code_commit: str
    fee_policy_by_execution_policy_sha256: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    markout_max_lag_ms: int = 10_000
    benchmark_policy_version: str = "miniqmt_execution_tca_benchmark_v1"
    mark_policy_version: str = MARK_POLICY_VERSION
    fee_policy_version: str = "miniqmt_execution_tca_fee_policy_v1"
    trade_provenance_policy_version: str = TCA_TRADE_PROVENANCE_POLICY_VERSION
    query_version: str = TCA_QUERY_VERSION
    schema_version: str = TCA_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.snapshot_kind not in {"DEADLINE", "RECONCILED_FINAL"}:
            raise TcaCalculationError("ADAPTIVE_IS_TCA_SNAPSHOT_KIND_INVALID", "tca_rebuild_request")
        if self.scope.environment != "SIM":
            raise TcaCalculationError("ADAPTIVE_IS_TCA_LIVE_SCOPE_DENIED", "tca_rebuild_request")
        if not self.account_pseudonym_key_version or not self.operator_pseudonym or not self.code_commit:
            raise TcaCalculationError("ADAPTIVE_IS_TCA_AUDIT_IDENTITY_MISSING", "tca_rebuild_request")


@dataclass(frozen=True, slots=True)
class TcaDraftMark:
    series_key: str
    source_input_sha256: str
    parent_intent_id: str
    parent_revision: int
    mark_type: str
    horizon_ms: int | None
    trade_key: tuple[str, date, str] | None
    values: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class TcaDraftResult:
    series_key: str
    canonical_input_sha256: str
    canonical_output_sha256: str
    parent_intent_id: str
    parent_revision: int
    values: Mapping[str, Any]
    marks: tuple[TcaDraftMark, ...]
    observation_memberships: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class TcaRebuildDraft:
    receipt_scope_hash: str
    canonical_input_sha256: str
    canonical_output_sha256: str
    source_row_counts: Mapping[str, int]
    source_content_hashes: Mapping[str, str]
    coverage: Mapping[str, Any]
    orphan_counts: Mapping[str, int]
    duplicate_counts: Mapping[str, int]
    conflict_counts: Mapping[str, int]
    invalid_counts: Mapping[str, int]
    invariant_results: Mapping[str, Any]
    planning_memberships: tuple[Mapping[str, Any], ...]
    results: tuple[TcaDraftResult, ...]


@dataclass(frozen=True, slots=True)
class TcaRebuildOutcome:
    receipt_id: str
    receipt_status: str
    reused: bool
    receipt_generation: int
    result_ids: tuple[str, ...]
    canonical_input_sha256: str | None
    canonical_output_sha256: str
    reason_code: str | None = None
    stage: str | None = None


class ExecutionTcaRebuildService:
    """Read one repeatable snapshot, then materialize under deterministic locks."""

    def __init__(
        self,
        conn_factory: Callable[[], Any] | None = None,
        source_repository: ExecutionTcaSourceRepository | None = None,
        evidence_repository: ExecutionTcaEvidenceRepository | None = None,
    ) -> None:
        self._conn_factory = conn_factory or get_conn
        self._source = source_repository or ExecutionTcaSourceRepository()
        self._evidence = evidence_repository or ExecutionTcaEvidenceRepository(self._conn_factory)

    def rebuild(self, request: TcaRebuildRequest) -> TcaRebuildOutcome:
        attempt_started_at = datetime.now(UTC)
        snapshot, started_at, completed_at = self._read_snapshot(request.scope)
        try:
            draft = build_rebuild_draft(
                snapshot=snapshot,
                request=request,
                source_snapshot_started_at=started_at,
                source_snapshot_completed_at=completed_at,
            )
        except TcaCalculationError as exc:
            return self._materialize_failed(
                request=request,
                snapshot=snapshot,
                started_at=started_at,
                completed_at=completed_at,
                attempt_started_at=attempt_started_at,
                failure=exc,
            )
        return self._materialize_completed(request, draft, started_at, completed_at, attempt_started_at)

    def _read_snapshot(
        self, scope: ExecutionTcaRebuildScope
    ) -> tuple[ExecutionTcaSourceSnapshot, datetime, datetime]:
        with self._conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("SELECT transaction_timestamp() AS source_snapshot_started_at")
                started_at = cursor.fetchone()["source_snapshot_started_at"]
                snapshot = self._source.read_scope(cursor=cursor, scope=scope)
                cursor.execute("SELECT clock_timestamp() AS source_snapshot_completed_at")
                completed_at = cursor.fetchone()["source_snapshot_completed_at"]
        return snapshot, started_at, completed_at

    def _materialize_completed(
        self,
        request: TcaRebuildRequest,
        draft: TcaRebuildDraft,
        started_at: datetime,
        completed_at: datetime,
        attempt_started_at: datetime,
    ) -> TcaRebuildOutcome:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                self._evidence.acquire_scope_lock(cursor=cursor, receipt_scope_hash=draft.receipt_scope_hash)
                receipt_head = self._evidence.receipt_head(cursor=cursor, receipt_scope_hash=draft.receipt_scope_hash)
                receipt_latest = self._evidence.receipt_latest(cursor=cursor, receipt_scope_hash=draft.receipt_scope_hash)
                _reject_stale_snapshot(receipt_head, started_at, "receipt")
                if receipt_head and receipt_head.get("canonical_input_sha256") == draft.canonical_input_sha256:
                    if receipt_head.get("canonical_output_sha256") != draft.canonical_output_sha256:
                        raise RuntimeError(
                            "reason_code=ADAPTIVE_IS_TCA_SAME_INPUT_OUTPUT_DRIFT, stage=tca_rebuild_materialize"
                        )
                    result_ids = self._receipt_result_ids(cursor, str(receipt_head["receipt_id"]))
                    return TcaRebuildOutcome(
                        str(receipt_head["receipt_id"]),
                        "COMPLETED",
                        True,
                        int(receipt_head["receipt_generation"]),
                        result_ids,
                        draft.canonical_input_sha256,
                        draft.canonical_output_sha256,
                    )
                receipt_generation = int(receipt_latest["receipt_generation"]) + 1 if receipt_latest else 1
                receipt_id = content_id("tcareceipt_", draft.receipt_scope_hash, draft.canonical_input_sha256)

                marks, mark_ids = self._materialize_marks(cursor, draft.results, started_at, completed_at)
                results, result_ids = self._materialize_results(
                    cursor, request, draft.results, mark_ids, started_at, completed_at
                )
                receipt = ExecutionTcaRebuildReceipt(
                    _completed_receipt_values(
                        request=request,
                        draft=draft,
                        receipt_id=receipt_id,
                        receipt_generation=receipt_generation,
                        supersedes_receipt_id=str(receipt_latest["receipt_id"]) if receipt_latest else None,
                        started_at=started_at,
                        completed_at=completed_at,
                        attempt_started_at=attempt_started_at,
                        attempt_completed_at=datetime.now(UTC),
                    )
                )
                receipt_subjects = tuple(
                    ExecutionTcaReceiptPlanningSubject(
                        {
                            "receipt_id": receipt_id,
                            "receipt_status": "COMPLETED",
                            **membership,
                            "membership_hash": canonical_json_sha256(membership),
                        }
                    )
                    for membership in draft.planning_memberships
                )
                receipt_results = tuple(
                    ExecutionTcaReceiptResult(
                        {
                            "receipt_id": receipt_id,
                            "receipt_status": "COMPLETED",
                            "tca_result_id": result_ids[result.series_key],
                            "parent_intent_id": result.parent_intent_id,
                            "parent_revision": result.parent_revision,
                            "snapshot_kind": request.snapshot_kind,
                            "membership_hash": canonical_json_sha256(
                                {
                                    "receipt_input": draft.canonical_input_sha256,
                                    "result_output": result.canonical_output_sha256,
                                    "parent_intent_id": result.parent_intent_id,
                                }
                            ),
                        }
                    )
                    for result in draft.results
                )
                result_marks = _result_mark_memberships(draft.results, result_ids, mark_ids)
                trade_memberships = _trade_observation_memberships(draft.results, result_ids)
                self._evidence.materialize_receipt(
                    cursor=cursor,
                    bundle=TcaMaterializationBundle(
                        receipt=receipt,
                        marks=marks,
                        results=results,
                        receipt_subjects=receipt_subjects,
                        receipt_results=receipt_results,
                        result_marks=result_marks,
                        result_trade_observations=trade_memberships,
                    ),
                )
        return TcaRebuildOutcome(
            receipt_id,
            "COMPLETED",
            False,
            receipt_generation,
            tuple(result_ids[result.series_key] for result in draft.results),
            draft.canonical_input_sha256,
            draft.canonical_output_sha256,
        )

    def _materialize_marks(
        self,
        cursor: Any,
        draft_results: Sequence[TcaDraftResult],
        started_at: datetime,
        completed_at: datetime,
    ) -> tuple[tuple[ExecutionTcaMark, ...], dict[str, str]]:
        rows: list[ExecutionTcaMark] = []
        ids: dict[str, str] = {}
        unique = {mark.series_key: mark for result in draft_results for mark in result.marks}
        for series_key in sorted(unique):
            draft = unique[series_key]
            self._evidence.acquire_scope_lock(cursor=cursor, receipt_scope_hash=series_key)
            head = self._evidence.mark_head(cursor=cursor, mark_series_key=series_key)
            _reject_stale_snapshot(head, started_at, "mark")
            if head and head.get("source_input_sha256") == draft.source_input_sha256:
                ids[series_key] = str(head["mark_id"])
                continue
            revision = int(head["mark_revision"]) + 1 if head else 1
            mark_id = content_id("tcamark_", series_key, draft.source_input_sha256)
            values = {
                "mark_id": mark_id,
                "mark_series_key": series_key,
                "mark_revision": revision,
                "supersedes_mark_id": str(head["mark_id"]) if head else None,
                **draft.values,
                "source_snapshot_started_at": started_at,
                "source_snapshot_completed_at": completed_at,
                "mark_policy_version": draft.values.get("mark_policy_version") or MARK_POLICY_VERSION,
                "source_input_sha256": draft.source_input_sha256,
            }
            values["evidence_sha256"] = canonical_json_sha256(
                {key: value for key, value in values.items() if key not in {"mark_id", "mark_revision", "supersedes_mark_id", "source_snapshot_started_at", "source_snapshot_completed_at"}}
            )
            rows.append(ExecutionTcaMark(values))
            ids[series_key] = mark_id
        return tuple(rows), ids

    def _materialize_results(
        self,
        cursor: Any,
        request: TcaRebuildRequest,
        drafts: Sequence[TcaDraftResult],
        mark_ids: Mapping[str, str],
        started_at: datetime,
        completed_at: datetime,
    ) -> tuple[tuple[ExecutionParentTca, ...], dict[str, str]]:
        rows: list[ExecutionParentTca] = []
        ids: dict[str, str] = {}
        for draft in sorted(drafts, key=lambda item: item.series_key):
            self._evidence.acquire_scope_lock(cursor=cursor, receipt_scope_hash=draft.series_key)
            head = self._evidence.result_head(cursor=cursor, result_series_key=draft.series_key)
            _reject_stale_snapshot(head, started_at, "result")
            if head and head.get("canonical_input_sha256") == draft.canonical_input_sha256:
                if head.get("canonical_output_sha256") != draft.canonical_output_sha256:
                    raise RuntimeError(
                        "reason_code=ADAPTIVE_IS_TCA_SAME_PARENT_INPUT_OUTPUT_DRIFT, stage=tca_rebuild_result"
                    )
                ids[draft.series_key] = str(head["tca_result_id"])
                continue
            generation = int(head["result_generation"]) + 1 if head else 1
            result_id = content_id("tcaresult_", draft.series_key, draft.canonical_input_sha256)
            values = {
                "tca_result_id": result_id,
                "result_series_key": draft.series_key,
                "result_generation": generation,
                "supersedes_tca_result_id": str(head["tca_result_id"]) if head else None,
                **draft.values,
                "source_snapshot_started_at": started_at,
                "source_snapshot_completed_at": completed_at,
                "schema_version": request.schema_version,
                "query_version": request.query_version,
                "benchmark_policy_version": request.benchmark_policy_version,
                "mark_policy_version": request.mark_policy_version,
                "fee_policy_version": request.fee_policy_version,
                "trade_provenance_policy_version": request.trade_provenance_policy_version,
                "canonical_input_sha256": draft.canonical_input_sha256,
                "canonical_output_sha256": draft.canonical_output_sha256,
            }
            rows.append(ExecutionParentTca(values))
            ids[draft.series_key] = result_id
        _ = mark_ids
        return tuple(rows), ids

    def _materialize_failed(
        self,
        *,
        request: TcaRebuildRequest,
        snapshot: ExecutionTcaSourceSnapshot,
        started_at: datetime,
        completed_at: datetime,
        attempt_started_at: datetime,
        failure: TcaCalculationError,
    ) -> TcaRebuildOutcome:
        source_counts, source_hashes = _source_manifest(snapshot)
        scope_hash = _receipt_scope_hash(request)
        failure_context = _sanitize_failure_context(failure.context, request.account_pseudonyms)
        failure_manifest = {
            "reason_code": failure.reason_code,
            "stage": failure.stage,
            "context": failure_context,
            "source_content_hashes": source_hashes,
            "snapshot_kind": request.snapshot_kind,
        }
        failure_sha = canonical_json_sha256(failure_manifest)
        output_sha = canonical_json_sha256({"receipt_status": "FAILED", **failure_manifest})
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                self._evidence.acquire_scope_lock(cursor=cursor, receipt_scope_hash=scope_hash)
                head = self._evidence.receipt_latest(cursor=cursor, receipt_scope_hash=scope_hash)
                generation = int(head["receipt_generation"]) + 1 if head else 1
                receipt_id = content_id("tcafailed_", scope_hash, generation, failure_sha)
                values = _receipt_base_values(
                    request,
                    receipt_id,
                    scope_hash,
                    generation,
                    started_at,
                    completed_at,
                    attempt_started_at,
                    datetime.now(UTC),
                )
                values.update(
                    {
                        "supersedes_receipt_id": str(head["receipt_id"]) if head else None,
                        "receipt_status": "FAILED",
                        "source_snapshot_complete": True,
                        "source_watermarks": {},
                        "source_row_counts": source_counts,
                        "source_content_hashes": source_hashes,
                        "coverage": {},
                        "orphan_counts": {},
                        "duplicate_counts": {},
                        "conflict_counts": {},
                        "invalid_counts": {},
                        "invariant_results": {},
                        "numeric_tolerances": {"cny": "0.00000001"},
                        "canonical_input_sha256": None,
                        "canonical_output_sha256": output_sha,
                        "failure_attempt_sha256": failure_sha,
                        "failure_reason_code": failure.reason_code,
                        "failure_stage": failure.stage,
                        "failure_class": "DOMAIN",
                        "failure_context": failure_context,
                    }
                )
                self._evidence.materialize_receipt(
                    cursor=cursor,
                    bundle=TcaMaterializationBundle(receipt=ExecutionTcaRebuildReceipt(values)),
                )
        return TcaRebuildOutcome(receipt_id, "FAILED", False, generation, (), None, output_sha, failure.reason_code, failure.stage)

    @staticmethod
    def _receipt_result_ids(cursor: Any, receipt_id: str) -> tuple[str, ...]:
        cursor.execute(
            "SELECT tca_result_id FROM qmt_strategy.execution_tca_receipt_result WHERE receipt_id=%s ORDER BY tca_result_id",
            (receipt_id,),
        )
        return tuple(str(row["tca_result_id"] if isinstance(row, Mapping) else row[0]) for row in cursor.fetchall())


def build_rebuild_draft(
    *,
    snapshot: ExecutionTcaSourceSnapshot,
    request: TcaRebuildRequest,
    source_snapshot_started_at: datetime,
    source_snapshot_completed_at: datetime,
) -> TcaRebuildDraft:
    """Pure deterministic projection from one source snapshot to canonical rows."""

    source_counts, source_hashes = _source_manifest(snapshot)
    quotes_by_runtime = _quote_candidates(snapshot.runtime_events)
    trades_by_parent = _group(snapshot.trades, "intent_id")
    orders_by_parent = _group(snapshot.orders, "intent_id")
    child_orders_by_broker_id = _group(snapshot.child_orders, "broker_order_id")
    events_by_parent = _group(snapshot.order_status_events, "intent_id")
    observations_by_trade = _group_trade(snapshot.trade_observations)
    results: list[TcaDraftResult] = []
    invalid_count = 0
    mark_quality_counts: dict[str, int] = {}

    for parent in sorted(snapshot.parents, key=lambda row: (str(row["parent_intent_id"]), int(row["parent_revision"]))):
        parent_id = str(parent["parent_intent_id"])
        parent_revision = int(parent["parent_revision"])
        account_id = str(parent["account_id"])
        if account_id not in request.account_pseudonyms:
            raise TcaCalculationError(
                "ADAPTIVE_IS_TCA_ACCOUNT_PSEUDONYM_MISSING", "tca_rebuild_pseudonym", account_id=account_id
            )
        deadline = _dt(parent.get("deadline"))
        terminal_as_of, reconciliation_run_id, finality, finality_evidence = _finality(
            parent=parent,
            orders=orders_by_parent.get(parent_id, ()),
            status_events=events_by_parent.get(parent_id, ()),
            trades=trades_by_parent.get(parent_id, ()),
            snapshot=snapshot,
            snapshot_kind=request.snapshot_kind,
            as_of_time=request.as_of_time,
        )
        runtime_quotes = quotes_by_runtime.get(str(parent.get("runtime_id") or ""), ())
        deadline_mark = (
            select_mark(
                candidates=runtime_quotes,
                symbol=str(parent["symbol"]),
                mark_type="DEADLINE",
                target_time=deadline,
                max_distance_ms=int(parent["deadline_mark_max_age_ms"]),
                trade_date=_date(parent["trade_date"]),
                session_ended=request.as_of_time >= deadline,
                clock_skew_tolerance_ms=int(parent["clock_skew_tolerance_ms"]),
            )
            if deadline is not None
            else None
        )
        drafts: list[TcaDraftMark] = []
        if deadline_mark is not None:
            drafts.append(_draft_mark(parent, deadline_mark, None, None, request))

        fills: list[TcaFill] = []
        memberships: list[Mapping[str, Any]] = []
        for trade in sorted(trades_by_parent.get(parent_id, ()), key=lambda row: (str(row.get("trade_id")), str(row.get("qmt_order_id")))):
            trade_key = (str(trade["account_id"]), _date(trade["trade_date"]), str(trade["trade_id"]))
            selected = _select_observations(trade, observations_by_trade.get(trade_key, ()))
            trade_time = selected["trade_time"]
            child_mark = None
            child_order = _select_child_order(
                child_orders_by_broker_id.get(str(trade.get("qmt_order_id") or ""), ()),
                selected["memberships"].get("ATTRIBUTION", {}).get("child_order_id"),
            )
            markout_mids: dict[int, Decimal | None] = {}
            child_submitted_at = _dt(child_order.get("submitted_at")) if child_order else None
            if child_submitted_at is not None:
                child_mark = select_mark(
                    candidates=runtime_quotes,
                    symbol=str(parent["symbol"]),
                    mark_type="CHILD_RECEIPT",
                    target_time=child_submitted_at,
                    max_distance_ms=int(parent["deadline_mark_max_age_ms"]),
                    trade_date=_date(parent["trade_date"]),
                    clock_skew_tolerance_ms=int(parent["clock_skew_tolerance_ms"]),
                )
                drafts.append(
                    _draft_mark(
                        parent,
                        child_mark,
                        trade_key,
                        None,
                        request,
                        child_order_id=str(child_order["child_order_id"]),
                    )
                )
            if trade_time is not None:
                for horizon in (60_000, 300_000, 900_000):
                    mark_type = f"FILL_MARKOUT_{horizon // 1000}S"
                    selected_mark = select_mark(
                        candidates=runtime_quotes,
                        symbol=str(parent["symbol"]),
                        mark_type=mark_type,
                        target_time=trade_time + timedelta(milliseconds=horizon),
                        max_distance_ms=request.markout_max_lag_ms,
                        trade_date=_date(parent["trade_date"]),
                        session_ended=request.as_of_time.date() > _date(parent["trade_date"]),
                        clock_skew_tolerance_ms=int(parent["clock_skew_tolerance_ms"]),
                    )
                    drafts.append(_draft_mark(parent, selected_mark, trade_key, horizon, request))
                    markout_mids[horizon] = selected_mark.mid_price if selected_mark.quality == "VALID" else None
            for role, observation in selected["memberships"].items():
                memberships.append(
                    {
                        "trade_observation_id": observation["trade_observation_id"],
                        "trade_account_id": trade_key[0],
                        "trade_date": trade_key[1],
                        "trade_id": trade_key[2],
                        "observation_role": role,
                        "selected_content_sha256": observation[_role_hash_field(role)],
                    }
                )
            fee_observation = selected["memberships"].get("FEE")
            fills.append(
                TcaFill(
                    trade_id=trade_key[2],
                    order_id=str(trade.get("qmt_order_id") or ""),
                    price=_decimal(trade["price"]),
                    quantity=int(trade["quantity"]),
                    trade_time=trade_time,
                    canonical_fact_sha256=str(trade.get("canonical_trade_fact_sha256") or "LEGACY_PROVENANCE_MISSING"),
                    observation_ids={role: str(row["trade_observation_id"]) for role, row in selected["memberships"].items()},
                    observation_hashes={role: str(row[_role_hash_field(role)]) for role, row in selected["memberships"].items()},
                    actual_fee_cny=_decimal(fee_observation["commission"]) if fee_observation and fee_observation.get("commission") is not None else None,
                    fee_provenance=(
                        "ACTUAL"
                        if fee_observation
                        and fee_observation.get("fee_evidence_level") in {"TRADE_LEVEL", "ORDER_LEVEL"}
                        else ("UNKNOWN_LEGACY" if not observations_by_trade.get(trade_key) else "MISSING")
                    ),
                    actual_fee_scope=(
                        str(fee_observation.get("fee_evidence_level"))
                        if fee_observation
                        and fee_observation.get("fee_evidence_level") in {"TRADE_LEVEL", "ORDER_LEVEL"}
                        else "MISSING"
                    ),
                    child_receipt_mid=child_mark.mid_price if child_mark and child_mark.quality == "VALID" else None,
                    markout_mid_by_horizon_ms=markout_mids,
                )
            )

        required_roles = {"CORE", "TIMING", "ATTRIBUTION"}
        provenance_complete = all(required_roles.issubset(fill.observation_ids) for fill in fills)
        if not provenance_complete:
            finality = False
        finality_evidence = {
            **finality_evidence,
            "required_trade_observation_roles": sorted(required_roles),
            "trade_provenance_membership_complete": provenance_complete,
            "finality_satisfied": finality,
        }

        fee_policy = request.fee_policy_by_execution_policy_sha256.get(str(parent["execution_policy_sha256"]))
        eligible_quantity = (
            _int_or_none(parent.get("eligible_quantity"))
            if parent.get("eligibility_quality") == "VALID"
            else None
        )
        decision_price = (
            _positive_decimal_or_none(parent.get("decision_mid_price"))
            if parent.get("decision_quality") == "VALID"
            else None
        )
        arrival_price = (
            _positive_decimal_or_none(parent.get("arrival_mid_price"))
            if parent.get("arrival_quality") == "VALID"
            else None
        )
        calc = calculate_parent_tca(
            TcaCalculationInput(
                parent_intent_id=parent_id,
                trade_date=_date(parent["trade_date"]),
                side=str(parent["side"]),
                eligible_quantity=eligible_quantity,
                decision_price=decision_price,
                arrival_price=arrival_price,
                deadline=deadline,
                as_of_time=request.as_of_time,
                snapshot_kind=request.snapshot_kind,
                fills=tuple(fills),
                deadline_mark=deadline_mark,
                estimated_fee_policy=fee_policy,
                terminal_as_of=terminal_as_of,
                reconciliation_run_id=reconciliation_run_id,
                finality_satisfied=finality,
                residual_reason=_residual_reason(parent, fills),
                residual_executability_class=_residual_class(parent, fills),
            )
        )
        values = {
            "parent_intent_id": parent_id,
            "parent_revision": parent_revision,
            "snapshot_kind": request.snapshot_kind,
            "as_of_time": deadline if request.snapshot_kind == "DEADLINE" else terminal_as_of,
            "deadline": deadline,
            "terminal_as_of": terminal_as_of,
            "reconciliation_run_id": reconciliation_run_id,
            **calc.values,
            "join_coverage": {
                "order_count": len(orders_by_parent.get(parent_id, ())),
                "trade_count": len(fills),
                "observation_membership_count": len(memberships),
                "arrival_notional_valid": bool(eligible_quantity and arrival_price),
                "eligible_arrival_notional_cny": (
                    Decimal(eligible_quantity) * arrival_price
                    if eligible_quantity and arrival_price
                    else None
                ),
                "deadline_filled_arrival_notional_cny": (
                    Decimal(int(calc.values["deadline_filled_quantity"])) * arrival_price
                    if arrival_price is not None
                    else None
                ),
            },
            "benchmark_coverage": {
                **calc.values["benchmark_coverage"],
                "decision_quality": parent.get("decision_quality"),
                "arrival_quality": parent.get("arrival_quality"),
                "eligibility_quality": parent.get("eligibility_quality"),
            },
            "finality_evidence": finality_evidence,
        }
        parent_input_sha = canonical_json_sha256(
            {
                "calculator_input_sha256": calc.canonical_input_sha256,
                "selected_marks": sorted((mark.series_key, mark.source_input_sha256) for mark in drafts),
                "selected_observations": sorted(canonical_json_sha256(item) for item in memberships),
                "finality_evidence": finality_evidence,
                "versions": _version_manifest(request),
            }
        )
        output_sha = canonical_json_sha256(
            {
                "values": values,
                "mark_membership_content": sorted((mark.series_key, mark.source_input_sha256) for mark in drafts),
                "observation_membership_content": sorted(canonical_json_sha256(item) for item in memberships),
            }
        )
        series_key = canonical_json_sha256(
            {
                "parent_intent_id": parent_id,
                "parent_revision": parent_revision,
                "snapshot_kind": request.snapshot_kind,
                "calculator_version": CALCULATOR_VERSION,
                "formula_version": FORMULA_VERSION,
                "schema_version": request.schema_version,
                "query_version": request.query_version,
                "benchmark_policy_version": request.benchmark_policy_version,
                "mark_policy_version": request.mark_policy_version,
                "fee_policy_version": request.fee_policy_version,
                "trade_provenance_policy_version": request.trade_provenance_policy_version,
            }
        )
        results.append(TcaDraftResult(series_key, parent_input_sha, output_sha, parent_id, parent_revision, values, tuple(drafts), tuple(memberships)))
        invalid_count += int(calc.values["result_status"] == "INVALID")
        for mark in drafts:
            quality = str(mark.values["quality"])
            mark_quality_counts[quality] = mark_quality_counts.get(quality, 0) + 1

    planning_memberships = tuple(
        {
            "planning_subject_id": str(row["planning_subject_id"]),
            "classification": str(row.get("planning_class") or "INVALID_SOURCE"),
        }
        for row in sorted(snapshot.planning_subjects, key=lambda row: str(row["planning_subject_id"]))
    )
    emitted_subjects = sum(item["classification"] == "EMITTED_PARENT" for item in planning_memberships)
    coverage = {
        "planning_subject_membership_ratio": "1.000000000000" if planning_memberships else None,
        "emitted_subject_count": emitted_subjects,
        "parent_count": len(results),
        "mark_quality_counts": mark_quality_counts,
        **_receipt_notional_coverage(results),
    }
    orphan_counts = {
        "unattributed_order": len(snapshot.unattributed_orders),
        "unattributed_trade": len(snapshot.unattributed_trades),
    }
    duplicate_counts = {"exact_trade_duplicates": _exact_duplicate_count(snapshot.trades)}
    conflict_counts = {
        "open_trade_conflicts": sum(
            str(row.get("conflict_status")) == "OPEN" for row in snapshot.trade_conflicts
        )
    }
    invalid_counts = {"parents": invalid_count}
    invariant_results = {"planning_emitted_parent_count_matches": emitted_subjects == len(snapshot.parents)}
    receipt_scope_hash = _receipt_scope_hash(request)
    canonical_input = canonical_json_sha256(
        {
            "receipt_scope_hash": receipt_scope_hash,
            "source_content_hashes": source_hashes,
            "planning_memberships": planning_memberships,
            "parent_inputs": [(item.series_key, item.canonical_input_sha256) for item in results],
            "versions": _version_manifest(request),
        }
    )
    canonical_output = canonical_json_sha256(
        {
            "planning_memberships": planning_memberships,
            "results": [(item.series_key, item.canonical_output_sha256) for item in results],
            "mark_outputs": sorted(
                (mark.series_key, mark.source_input_sha256, canonical_json_sha256(mark.values))
                for result in results
                for mark in result.marks
            ),
            "coverage": coverage,
            "orphan_counts": orphan_counts,
            "duplicate_counts": duplicate_counts,
            "conflict_counts": conflict_counts,
            "invalid_counts": invalid_counts,
            "invariant_results": invariant_results,
        }
    )
    return TcaRebuildDraft(
        receipt_scope_hash=receipt_scope_hash,
        canonical_input_sha256=canonical_input,
        canonical_output_sha256=canonical_output,
        source_row_counts=source_counts,
        source_content_hashes=source_hashes,
        coverage=coverage,
        orphan_counts=orphan_counts,
        duplicate_counts=duplicate_counts,
        conflict_counts=conflict_counts,
        invalid_counts=invalid_counts,
        invariant_results=invariant_results,
        planning_memberships=planning_memberships,
        results=tuple(results),
    )


def _source_manifest(snapshot: ExecutionTcaSourceSnapshot) -> tuple[dict[str, int], dict[str, str]]:
    names = (
        "planning_subjects",
        "parents",
        "runtime_events",
        "child_orders",
        "orders",
        "order_status_events",
        "trades",
        "trade_observations",
        "trade_conflicts",
        "reconciliations",
        "reconciliation_issues",
        "unattributed_orders",
        "unattributed_trades",
    )
    counts: dict[str, int] = {}
    hashes: dict[str, str] = {}
    for name in names:
        rows = getattr(snapshot, name)
        canonical_rows = sorted(canonical_json_sha256(_canonical_source_row(row)) for row in rows)
        counts[name] = len(rows)
        hashes[name] = canonical_json_sha256(canonical_rows)
    return counts, hashes


def _canonical_source_row(row: Mapping[str, Any]) -> dict[str, Any]:
    audit_only = {
        "created_at",
        "persisted_at",
        "last_synced_at",
        "first_ingested_at",
        "updated_at",
    }
    return {key: value for key, value in row.items() if key not in audit_only}


def _quote_candidates(rows: Sequence[Mapping[str, Any]]) -> dict[str, tuple[QuoteCandidate, ...]]:
    result: dict[str, list[QuoteCandidate]] = {}
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
        symbol = str(payload.get("symbol") or "")
        candidate = QuoteCandidate(
            evidence_id=str(row.get("event_id") or ""),
            symbol=symbol,
            market_time=_dt(payload.get("market_time") or payload.get("quote_market_time")),
            received_at=_dt(row.get("event_time")),
            bid_price_1=_positive_decimal_or_none(payload.get("bid_price_1")),
            ask_price_1=_positive_decimal_or_none(payload.get("ask_price_1")),
            last_price=_positive_decimal_or_none(payload.get("last_price") or payload.get("price")),
            quote_source=str(payload.get("quote_source") or row.get("source") or "") or None,
            raw_quote_sha256=canonical_json_sha256(payload),
            market_phase=str(payload.get("market_phase") or "") or None,
            stock_status=str(payload.get("stock_status") or "") or None,
        )
        result.setdefault(str(row.get("runtime_id") or ""), []).append(candidate)
    return {
        key: tuple(sorted(values, key=lambda item: (item.market_time.isoformat() if item.market_time else "", item.evidence_id)))
        for key, values in result.items()
    }


def _group(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, tuple[Mapping[str, Any], ...]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(key) or ""), []).append(row)
    return {group_key: tuple(values) for group_key, values in grouped.items()}


def _group_trade(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, date, str], tuple[Mapping[str, Any], ...]]:
    grouped: dict[tuple[str, date, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        key = (str(row["account_id"]), _date(row["trade_date"]), str(row["trade_id"]))
        grouped.setdefault(key, []).append(row)
    return {key: tuple(values) for key, values in grouped.items()}


def _select_child_order(
    candidates: Sequence[Mapping[str, Any]], preferred_child_order_id: Any
) -> Mapping[str, Any] | None:
    if preferred_child_order_id:
        preferred = [
            row for row in candidates if str(row.get("child_order_id")) == str(preferred_child_order_id)
        ]
        if preferred:
            candidates = preferred
    submitted = [row for row in candidates if _dt(row.get("submitted_at")) is not None]
    if not submitted:
        return None
    return min(
        submitted,
        key=lambda row: (_dt(row["submitted_at"]), str(row.get("child_order_id") or "")),
    )


def _select_observations(
    trade: Mapping[str, Any], observations: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    canonical = str(trade.get("canonical_trade_fact_sha256") or "")
    matching = [row for row in observations if str(row.get("canonical_trade_fact_sha256")) == canonical]
    memberships: dict[str, Mapping[str, Any]] = {}
    if matching:
        memberships["CORE"] = _stable_observation(matching, "canonical_trade_fact_sha256")
        timing = [row for row in matching if row.get("broker_trade_time") is not None]
        if timing:
            timing_hashes = {str(row["timing_observation_sha256"]) for row in timing}
            if len(timing_hashes) > 1:
                raise TcaCalculationError(
                    "ADAPTIVE_IS_TCA_TRADE_TIME_CONFLICT",
                    "tca_rebuild_observation_select",
                    trade_id=trade.get("trade_id"),
                )
            memberships["TIMING"] = _latest_observation(timing)
        fee = [
            row
            for row in matching
            if row.get("fee_evidence_level") in {"TRADE_LEVEL", "ORDER_LEVEL"}
            and row.get("commission") is not None
        ]
        if fee:
            latest_fee_time = max(_dt(row.get("observed_at")) for row in fee)
            latest_fee = [row for row in fee if _dt(row.get("observed_at")) == latest_fee_time]
            if len({str(row["fee_observation_sha256"]) for row in latest_fee}) > 1:
                raise TcaCalculationError(
                    "ADAPTIVE_IS_TCA_FEE_OBSERVATION_CONFLICT",
                    "tca_rebuild_observation_select",
                    trade_id=trade.get("trade_id"),
                )
            memberships["FEE"] = _latest_observation(fee)
        attribution = [row for row in matching if row.get("intent_id") == trade.get("intent_id")]
        if attribution:
            memberships["ATTRIBUTION"] = _latest_observation(attribution)
    trade_time = memberships.get("TIMING", {}).get("broker_trade_time") or trade.get("trade_time")
    return {"memberships": memberships, "trade_time": _dt(trade_time)}


def _stable_observation(rows: Sequence[Mapping[str, Any]], role_hash: str) -> Mapping[str, Any]:
    return min(rows, key=lambda row: (str(row.get(role_hash) or ""), str(row.get("trade_observation_id") or "")))


def _latest_observation(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    return max(
        rows,
        key=lambda row: (
            1 if row.get("ingest_source") == "BROKER_SNAPSHOT_SYNC" else 0,
            _dt(row.get("observed_at")).isoformat() if _dt(row.get("observed_at")) else "",
            str(row.get("trade_observation_id") or ""),
        ),
    )


def _role_hash_field(role: str) -> str:
    return {
        "CORE": "canonical_trade_fact_sha256",
        "TIMING": "timing_observation_sha256",
        "FEE": "fee_observation_sha256",
        "ATTRIBUTION": "attribution_sha256",
    }[role]


def _draft_mark(
    parent: Mapping[str, Any],
    selected: SelectedMark,
    trade_key: tuple[str, date, str] | None,
    horizon_ms: int | None,
    request: TcaRebuildRequest,
    child_order_id: str | None = None,
) -> TcaDraftMark:
    candidate = selected.candidate
    parent_id = str(parent["parent_intent_id"])
    parent_revision = int(parent["parent_revision"])
    mark_scope = {
        "parent_intent_id": parent_id,
        "parent_revision": parent_revision,
        "mark_type": selected.mark_type,
        "trade_key": trade_key,
        "horizon_ms": horizon_ms,
        "child_order_id": child_order_id,
        "mark_policy_version": request.mark_policy_version,
    }
    series_key = canonical_json_sha256(mark_scope)
    values = {
        "parent_intent_id": parent_id,
        "parent_revision": parent_revision,
        "mark_scope_key": canonical_json_sha256(mark_scope),
        "mark_type": selected.mark_type,
        "trade_account_id": trade_key[0] if trade_key else None,
        "trade_date": trade_key[1] if trade_key else None,
        "trade_id": trade_key[2] if trade_key else None,
        "child_order_id": child_order_id,
        "horizon_ms": horizon_ms,
        "target_time": selected.target_time,
        "market_time": candidate.market_time if candidate else None,
        "received_at": candidate.received_at if candidate else None,
        "bid_price_1": candidate.bid_price_1 if candidate else None,
        "ask_price_1": candidate.ask_price_1 if candidate else None,
        "mid_price": selected.mid_price,
        "last_price": candidate.last_price if candidate else None,
        "quote_source": candidate.quote_source if candidate else None,
        "age_or_lag_ms": selected.age_or_lag_ms,
        "quality": selected.quality,
        "market_phase": candidate.market_phase if candidate else None,
        "stock_status": candidate.stock_status if candidate else None,
        "raw_quote_sha256": candidate.raw_quote_sha256 if candidate else None,
        "market_data_id": candidate.evidence_id if candidate else None,
        "mark_policy_version": request.mark_policy_version,
    }
    source_input_sha = canonical_json_sha256({"mark_scope": mark_scope, "selected": selected.as_manifest()})
    return TcaDraftMark(series_key, source_input_sha, parent_id, parent_revision, selected.mark_type, horizon_ms, trade_key, values)


def _finality(
    *,
    parent: Mapping[str, Any],
    orders: Sequence[Mapping[str, Any]],
    status_events: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    snapshot: ExecutionTcaSourceSnapshot,
    snapshot_kind: str,
    as_of_time: datetime,
) -> tuple[datetime | None, str | None, bool, Mapping[str, Any]]:
    deadline = _dt(parent.get("deadline"))
    account = str(parent["account_id"])
    trade_date = _date(parent["trade_date"])
    candidates = [
        row
        for row in snapshot.reconciliations
        if str(row.get("account_id")) == account
        and _date(row.get("trade_date")) == trade_date
        and _dt(row.get("completed_at")) is not None
        and _dt(row.get("completed_at")).astimezone(_CHINA_TZ).date() == trade_date
        and (deadline is None or _dt(row.get("completed_at")) >= deadline)
    ]
    selected = max(candidates, key=lambda row: (_dt(row["completed_at"]), str(row["run_id"]))) if candidates else None
    selected_run_id = str(selected["run_id"]) if selected else None
    summary = selected.get("summary_json") if selected and isinstance(selected.get("summary_json"), Mapping) else {}
    sync_summary = summary.get("sync_summary") if isinstance(summary.get("sync_summary"), Mapping) else {}
    query_evidence = bool(
        sync_summary.get("orders_query_succeeded") is True
        and sync_summary.get("trades_query_succeeded") is True
        and sync_summary.get("orders_snapshot_sha256")
        and sync_summary.get("trades_snapshot_sha256")
        and _int_or_none(sync_summary.get("orders_snapshot_count")) is not None
        and _int_or_none(sync_summary.get("trades_snapshot_count")) is not None
        and sync_summary.get("trade_conflict_heads_scanned") is True
        and sync_summary.get("trade_conflict_heads_sha256")
    )
    issues = [row for row in snapshot.reconciliation_issues if str(row.get("run_id")) == selected_run_id]
    open_conflicts = [
        row
        for row in snapshot.trade_conflicts
        if str(row.get("account_id")) == account
        and _date(row.get("trade_date")) == trade_date
        and str(row.get("conflict_status")) == "OPEN"
    ]
    unattributed_orders = [row for row in snapshot.unattributed_orders if str(row.get("account_id")) == account and _date(row.get("trade_date")) == trade_date]
    unattributed_trades = [row for row in snapshot.unattributed_trades if str(row.get("account_id")) == account and _date(row.get("trade_date")) == trade_date]
    terminal_orders = all(is_terminal_order_status(row.get("order_status")) for row in orders)
    trade_times_complete = all(
        _dt(row.get("trade_time")) is not None
        and _dt(row.get("trade_time")).astimezone(_CHINA_TZ).date() == trade_date
        for row in trades
    )
    aggregate_match = all(
        int(order.get("traded_volume") or 0)
        == sum(int(trade.get("quantity") or 0) for trade in trades if str(trade.get("qmt_order_id")) == str(order.get("qmt_order_id")))
        for order in orders
    )
    deadline_passed = deadline is not None and as_of_time >= deadline
    succeeded = bool(
        selected
        and selected.get("status") == "SUCCEEDED"
        and _int_or_none(summary.get("issue_count")) == 0
    )
    final = bool(
        snapshot_kind == "RECONCILED_FINAL"
        and deadline_passed
        and succeeded
        and query_evidence
        and not issues
        and not open_conflicts
        and not unattributed_orders
        and not unattributed_trades
        and terminal_orders
        and trade_times_complete
        and aggregate_match
    )
    evidence = {
        "deadline_passed": deadline_passed,
        "selected_reconciliation_run_id": selected_run_id,
        "selected_reconciliation_status": selected.get("status") if selected else None,
        "broker_query_evidence_complete": query_evidence,
        "reconciliation_issue_count": len(issues),
        "open_trade_conflict_count": len(open_conflicts),
        "unattributed_order_count": len(unattributed_orders),
        "unattributed_trade_count": len(unattributed_trades),
        "all_orders_terminal": terminal_orders,
        "authoritative_trade_time_complete": trade_times_complete,
        "order_trade_aggregate_match": aggregate_match,
        "finality_satisfied": final,
    }
    if snapshot_kind == "DEADLINE":
        return None, None, False, {**evidence, "finality_satisfied": False}
    return (_dt(selected["completed_at"]) if selected else None), selected_run_id, final, evidence


def _residual_reason(parent: Mapping[str, Any], fills: Sequence[TcaFill]) -> str:
    eligible = _int_or_none(parent.get("eligible_quantity"))
    filled = sum(fill.quantity for fill in fills)
    if eligible is not None and filled >= eligible:
        return "COMPLETED"
    evidence = parent.get("eligibility_evidence") if isinstance(parent.get("eligibility_evidence"), Mapping) else {}
    return str(evidence.get("reason_code") or parent.get("eligibility_class") or "UNKNOWN")


def _residual_class(parent: Mapping[str, Any], fills: Sequence[TcaFill]) -> str:
    eligible = _int_or_none(parent.get("eligible_quantity"))
    if eligible is not None and sum(fill.quantity for fill in fills) >= eligible:
        return "COMPLETED"
    candidate = str(parent.get("eligibility_class") or "UNKNOWN")
    allowed = {
        "POLICY_BLOCKED",
        "MARKET_EXTERNAL_BLOCKED",
        "BROKER_REJECTED",
        "DEPENDENCY_UNSATISFIED",
        "BATCH_ABORTED_BY_PEER",
        "UNKNOWN",
    }
    return candidate if candidate in allowed else "UNKNOWN"


def _receipt_scope_hash(request: TcaRebuildRequest) -> str:
    return canonical_json_sha256(
        {
            "binding_ids": sorted(request.scope.binding_ids),
            "account_pseudonyms": sorted(request.account_pseudonyms.values()),
            "trade_date_from": request.scope.trade_date_from,
            "trade_date_to": request.scope.trade_date_to,
            "parent_intent_ids": sorted(request.scope.parent_intent_ids),
            "snapshot_kind": request.snapshot_kind,
            "versions": _version_manifest(request),
        }
    )


def _version_manifest(request: TcaRebuildRequest) -> dict[str, str]:
    return {
        "calculator_version": CALCULATOR_VERSION,
        "formula_version": FORMULA_VERSION,
        "schema_version": request.schema_version,
        "query_version": request.query_version,
        "benchmark_policy_version": request.benchmark_policy_version,
        "mark_policy_version": request.mark_policy_version,
        "fee_policy_version": request.fee_policy_version,
        "trade_provenance_policy_version": request.trade_provenance_policy_version,
    }


def _receipt_notional_coverage(results: Sequence[TcaDraftResult]) -> dict[str, Any]:
    covered = [
        result for result in results if result.values["join_coverage"].get("arrival_notional_valid") is True
    ]
    eligible_notional_partial = sum(
        (result.values["join_coverage"]["eligible_arrival_notional_cny"] for result in covered),
        Decimal(0),
    )
    deadline_notional_partial = sum(
        (result.values["join_coverage"]["deadline_filled_arrival_notional_cny"] for result in covered),
        Decimal(0),
    )
    total_eligible_quantity = sum((int(result.values.get("eligible_quantity") or 0) for result in results), 0)
    covered_eligible_quantity = sum((int(result.values.get("eligible_quantity") or 0) for result in covered), 0)
    complete = bool(results) and len(covered) == len(results)
    return {
        "eligible_notional_cny": eligible_notional_partial if complete else None,
        "eligible_notional_partial_cny": eligible_notional_partial,
        "deadline_filled_arrival_notional_partial_cny": deadline_notional_partial,
        "arrival_notional_parent_coverage_ratio": (
            Decimal(len(covered)) / Decimal(len(results)) if results else None
        ),
        "arrival_notional_eligible_quantity_coverage_ratio": (
            Decimal(covered_eligible_quantity) / Decimal(total_eligible_quantity)
            if total_eligible_quantity > 0
            else None
        ),
        "arrival_notional_coverage_status": "FULL" if complete else ("PARTIAL" if covered else "MISSING"),
        "completion_by_deadline_notional": (
            deadline_notional_partial / eligible_notional_partial
            if complete and eligible_notional_partial > 0
            else None
        ),
        "completion_by_deadline_notional_partial": (
            deadline_notional_partial / eligible_notional_partial
            if eligible_notional_partial > 0
            else None
        ),
    }


def _completed_receipt_values(
    *,
    request: TcaRebuildRequest,
    draft: TcaRebuildDraft,
    receipt_id: str,
    receipt_generation: int,
    supersedes_receipt_id: str | None,
    started_at: datetime,
    completed_at: datetime,
    attempt_started_at: datetime,
    attempt_completed_at: datetime,
) -> dict[str, Any]:
    values = _receipt_base_values(
        request,
        receipt_id,
        draft.receipt_scope_hash,
        receipt_generation,
        started_at,
        completed_at,
        attempt_started_at,
        attempt_completed_at,
    )
    eligible = sum((int(result.values.get("eligible_quantity") or 0) for result in draft.results), 0)
    deadline_filled = sum((int(result.values.get("deadline_filled_quantity") or 0) for result in draft.results), 0)
    terminal_filled = sum((int(result.values.get("terminal_filled_quantity") or 0) for result in draft.results), 0)
    notional_coverage = _receipt_notional_coverage(draft.results)
    values.update(
        {
            "supersedes_receipt_id": supersedes_receipt_id,
            "receipt_status": "COMPLETED",
            "source_snapshot_complete": True,
            "source_watermarks": {},
            "source_row_counts": draft.source_row_counts,
            "source_content_hashes": draft.source_content_hashes,
            "parent_count": len(draft.results),
            "planning_subject_count": len(draft.planning_memberships),
            "planning_excluded_count": sum(item["classification"] == "PLANNING_RULE_EXCLUDED" for item in draft.planning_memberships),
            "order_event_count": draft.source_row_counts["order_status_events"],
            "trade_count": draft.source_row_counts["trades"],
            "trade_observation_count": draft.source_row_counts["trade_observations"],
            "trade_conflict_count": draft.source_row_counts["trade_conflicts"],
            "mark_count": sum(len(result.marks) for result in draft.results),
            "eligible_quantity": eligible,
            "deadline_filled_quantity": deadline_filled,
            "terminal_filled_quantity": terminal_filled,
            "eligible_notional_cny": notional_coverage["eligible_notional_cny"],
            "deadline_filled_notional_cny": sum((result.values["deadline_fill_notional_cny"] for result in draft.results), Decimal(0)),
            "terminal_filled_notional_cny": sum((result.values["terminal_fill_notional_cny"] for result in draft.results), Decimal(0)),
            "coverage": draft.coverage,
            "orphan_counts": draft.orphan_counts,
            "duplicate_counts": draft.duplicate_counts,
            "conflict_counts": draft.conflict_counts,
            "invalid_counts": draft.invalid_counts,
            "invariant_results": draft.invariant_results,
            "numeric_tolerances": {"cny": "0.00000001", "bps": "0.00000001"},
            "canonical_input_sha256": draft.canonical_input_sha256,
            "canonical_output_sha256": draft.canonical_output_sha256,
            "failure_attempt_sha256": None,
            "final_parent_count": sum(item.values["result_status"] == "FINAL" for item in draft.results),
            "provisional_parent_count": sum(item.values["result_status"] == "PROVISIONAL" for item in draft.results),
            "invalid_parent_count": sum(item.values["result_status"] == "INVALID" for item in draft.results),
            "failure_reason_code": None,
            "failure_stage": None,
            "failure_class": None,
            "failure_context": {},
        }
    )
    return values


def _receipt_base_values(
    request: TcaRebuildRequest,
    receipt_id: str,
    scope_hash: str,
    generation: int,
    source_started_at: datetime,
    source_completed_at: datetime,
    attempt_started_at: datetime,
    attempt_completed_at: datetime,
) -> dict[str, Any]:
    return {
        "receipt_id": receipt_id,
        "receipt_scope_hash": scope_hash,
        "receipt_generation": generation,
        "snapshot_kind": request.snapshot_kind,
        "environment": "SIM",
        "binding_ids": list(sorted(request.scope.binding_ids)),
        "account_pseudonyms": list(sorted(request.account_pseudonyms.values())),
        "account_pseudonym_key_version": request.account_pseudonym_key_version,
        "trade_date_from": request.scope.trade_date_from,
        "trade_date_to": request.scope.trade_date_to,
        "selection_predicates": {
            "parent_intent_ids": sorted(request.scope.parent_intent_ids),
            "account_scope_count": len(request.scope.account_ids),
        },
        "db_snapshot_identity": {},
        "source_snapshot_started_at": source_started_at,
        "source_snapshot_completed_at": source_completed_at,
        "calculator_version": CALCULATOR_VERSION,
        "formula_version": FORMULA_VERSION,
        "schema_version": request.schema_version,
        "query_version": request.query_version,
        "benchmark_policy_version": request.benchmark_policy_version,
        "mark_policy_version": request.mark_policy_version,
        "fee_policy_version": request.fee_policy_version,
        "trade_provenance_policy_version": request.trade_provenance_policy_version,
        "code_commit": request.code_commit,
        "canonical_query_sha256": TCA_CANONICAL_QUERY_SHA256,
        "started_at": attempt_started_at,
        "completed_at": attempt_completed_at,
        "operator_pseudonym": request.operator_pseudonym,
        "source_snapshot_read_only": True,
        "broker_side_effect": False,
        "source_mutation": False,
        "evidence_write_performed": True,
    }


def _result_mark_memberships(
    drafts: Sequence[TcaDraftResult], result_ids: Mapping[str, str], mark_ids: Mapping[str, str]
) -> tuple[ExecutionTcaResultMark, ...]:
    rows: list[ExecutionTcaResultMark] = []
    for result in drafts:
        result_id = result_ids[result.series_key]
        for mark in result.marks:
            role = mark.mark_type
            membership = {
                "tca_result_id": result_id,
                "mark_id": mark_ids[mark.series_key],
                "parent_intent_id": result.parent_intent_id,
                "parent_revision": result.parent_revision,
                "mark_role": role,
            }
            rows.append(ExecutionTcaResultMark({**membership, "membership_hash": canonical_json_sha256(membership)}))
    return tuple(rows)


def _trade_observation_memberships(
    drafts: Sequence[TcaDraftResult], result_ids: Mapping[str, str]
) -> tuple[ExecutionTcaResultTradeObservation, ...]:
    rows: list[ExecutionTcaResultTradeObservation] = []
    for result in drafts:
        for membership in result.observation_memberships:
            values = {
                "tca_result_id": result_ids[result.series_key],
                "parent_intent_id": result.parent_intent_id,
                "parent_revision": result.parent_revision,
                **membership,
            }
            rows.append(ExecutionTcaResultTradeObservation({**values, "membership_hash": canonical_json_sha256(values)}))
    return tuple(rows)


def _reject_stale_snapshot(head: Mapping[str, Any] | None, started_at: datetime, entity: str) -> None:
    if not head:
        return
    watermark = _dt(head.get("source_snapshot_started_at"))
    if watermark is not None and started_at < watermark:
        raise RuntimeError(
            "reason_code=ADAPTIVE_IS_TCA_STALE_SNAPSHOT_WRITE, "
            f"stage=tca_rebuild_{entity}, source_snapshot_started_at={started_at.isoformat()}, watermark={watermark.isoformat()}"
        )


def _sanitize_failure_context(
    context: Mapping[str, Any], account_pseudonyms: Mapping[str, str]
) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in context.items():
        if "account" in key.lower():
            sanitized[key] = account_pseudonyms.get(str(value), "REDACTED")
        elif isinstance(value, Mapping):
            sanitized[key] = _sanitize_failure_context(value, account_pseudonyms)
        else:
            sanitized[key] = value
    return sanitized


def _exact_duplicate_count(rows: Sequence[Mapping[str, Any]]) -> int:
    seen: set[tuple[str, str, str, str]] = set()
    duplicates = 0
    for row in rows:
        key = (
            str(row.get("account_id")),
            str(row.get("trade_date")),
            str(row.get("trade_id")),
            str(row.get("canonical_trade_fact_sha256")),
        )
        if key in seen:
            duplicates += 1
        seen.add(key)
    return duplicates


def _dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _decimal(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _positive_decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    parsed = _decimal(value)
    return parsed if parsed > 0 else None


def _int_or_none(value: Any) -> int | None:
    return int(value) if value is not None else None
