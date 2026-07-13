"""PostgreSQL authority for immutable Phase 1C-3 outcome-label revisions.

The repository deliberately consumes only a frozen ``LabelAppendRequest`` and
the content-addressed calculation-evidence descriptor.  It never queries a
current market/source table and never falls back to the in-memory repository.
"""

from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
import logging
from typing import Any, Iterator, Mapping, Protocol

import psycopg2
import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.calculation_evidence import CalculationEvidenceStoreError, LocalCalculationEvidenceStore
from backend.services.advisory_phase1.label_builder import (
    LabelAppendRequest,
    LabelBuilderError,
    OutcomeLabelAuthorityHeader,
    OutcomeLabelPayload,
    OutcomeLabelVersion,
    REASON_LABEL_APPEND_REQUEST_CONFLICT,
    REASON_LABEL_HEADER_PAYLOAD_CLOSURE_INVALID,
    REASON_LABEL_PREDECESSOR_INVALID,
    REASON_LABEL_REVISION_CHAIN_INVALID,
    REASON_LABEL_STATE_TRANSITION_INVALID,
    _ALLOWED_TRANSITIONS,
    _canonical_revalidate,
    _validate_header_payload,
)
from backend.services.advisory_phase1.label_policy import Projection
from backend.services.advisory_phase1.outcome_engine import (
    BarrierResult,
    BarrierStatus,
    CalculationEvidenceBundle,
    CashflowResult,
    EntryStatus,
    MaturityStatus,
    OutcomeCalculationResult,
    OutcomeEventStatus,
    OutcomeOwner,
    OwnerType,
)

logger = logging.getLogger(__name__)


REASON_LABEL_PARTITION_MISSING = "ADVISORY_PHASE1C3_LABEL_PARTITION_MISSING"
REASON_CALCULATION_EVIDENCE_INVALID = "ADVISORY_PHASE1C3_CALCULATION_EVIDENCE_INVALID"
REASON_DATABASE_INVARIANT_VIOLATION = "ADVISORY_PHASE1C3_DATABASE_INVARIANT_VIOLATION"


class CalculationEvidenceReader(Protocol):
    def get(
        self,
        *,
        uri: str,
        sha256: str,
        size_bytes: int,
        store_backend_hash: str,
    ) -> CalculationEvidenceBundle: ...


class LocalCalculationEvidenceReader:
    """Explicit adapter kept separate from database persistence wiring."""

    def __init__(self, store: LocalCalculationEvidenceStore) -> None:
        self._store = store

    def get(
        self,
        *,
        uri: str,
        sha256: str,
        size_bytes: int,
        store_backend_hash: str,
    ) -> CalculationEvidenceBundle:
        return self._store.get(
            uri=uri,
            sha256=sha256,
            size_bytes=size_bytes,
            store_backend_hash=store_backend_hash,
        )


@contextmanager
def _transactional_conn_factory() -> Iterator[Any]:
    with get_conn(autocommit=False, manage_transaction=True) as conn:
        yield conn


class PostgresOutcomeLabelRepository:
    """Append-only label header/payload authority with exact retry readback."""

    def __init__(
        self,
        *,
        evidence_reader: CalculationEvidenceReader,
        conn_factory: Any | None = None,
    ) -> None:
        self._evidence_reader = evidence_reader
        self._conn_factory = conn_factory or _transactional_conn_factory

    def append(self, *, request: LabelAppendRequest, created_by_capture_batch_id: str) -> OutcomeLabelVersion:
        _canonical_revalidate(request, reason_code=REASON_LABEL_APPEND_REQUEST_CONFLICT, label="label append request")
        try:
            persisted_evidence = self._evidence_reader.get(
                uri=request.calculation_evidence_uri,
                sha256=request.calculation_evidence_sha256,
                size_bytes=request.calculation_evidence_size_bytes,
                store_backend_hash=request.calculation_evidence_store_backend_hash,
            )
        except CalculationEvidenceStoreError as error:
            raise LabelBuilderError(REASON_CALCULATION_EVIDENCE_INVALID, "calculation evidence readback failed") from error
        if persisted_evidence != request.outcome_result.calculation_evidence:
            raise LabelBuilderError(REASON_CALCULATION_EVIDENCE_INVALID, "calculation evidence readback differs from label result")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (request.label_key_hash,))
                existing = self._select_by_append_hash_locked(cur, str(request.label_append_request_hash))
                if existing is not None:
                    self._assert_exact_retry(existing=existing, request=request)
                    return existing
                self._require_creator_batch_active(cur, capture_batch_id=created_by_capture_batch_id)
                predecessor = self._select_terminal_locked(cur, request.label_key_hash)
                self._validate_predecessor(request=request, predecessor=predecessor)
                self._insert_or_compare_blob(cur, request=request)
                cur.execute("SELECT clock_timestamp() AS database_now")
                computed_at = cur.fetchone()["database_now"]
                revision = 1 if predecessor is None else predecessor.label_revision_no + 1
                version = OutcomeLabelVersion.from_append(
                    request,
                    label_revision_no=revision,
                    predecessor=predecessor,
                    created_by_capture_batch_id=created_by_capture_batch_id,
                    computed_at=computed_at,
                )
                try:
                    self._insert_header(cur, version)
                    self._insert_payload(cur, version)
                except psycopg2.IntegrityError as error:
                    self._raise_integrity(error, label="append outcome label")
                loaded = self._select_by_version_locked(cur, str(version.label_version_id))
                if loaded is None:
                    raise LabelBuilderError(REASON_LABEL_HEADER_PAYLOAD_CLOSURE_INVALID, "label append did not persist complete authority")
                if loaded != version:
                    raise LabelBuilderError(REASON_LABEL_HEADER_PAYLOAD_CLOSURE_INVALID, "label append readback differs from persisted authority")
                return loaded

    def get(self, label_version_id: str) -> OutcomeLabelVersion | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._select_by_version_locked(cur, label_version_id)

    def chain_for(self, label_key_hash: str) -> tuple[OutcomeLabelVersion, ...]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    self._select_join_sql() + " WHERE h.label_key_hash = %s ORDER BY h.label_revision_no FOR KEY SHARE",
                    (label_key_hash,),
                )
                chain = tuple(self._from_row(dict(row)) for row in cur.fetchall())
                self._validate_chain(chain)
                return chain

    def header_for(self, label_version_id: str) -> OutcomeLabelAuthorityHeader | None:
        version = self.get(label_version_id)
        return OutcomeLabelAuthorityHeader.from_version(version) if version is not None else None

    def payload_for(self, label_version_id: str) -> OutcomeLabelPayload | None:
        version = self.get(label_version_id)
        return OutcomeLabelPayload.from_version(version) if version is not None else None

    def _select_by_append_hash_locked(self, cur: Any, request_hash: str) -> OutcomeLabelVersion | None:
        cur.execute(
            self._select_join_sql() + " WHERE h.label_append_request_hash = %s FOR KEY SHARE",
            (request_hash,),
        )
        row = cur.fetchone()
        return self._from_row(dict(row)) if row is not None else None

    def _select_by_version_locked(self, cur: Any, label_version_id: str) -> OutcomeLabelVersion | None:
        cur.execute(self._select_join_sql() + " WHERE h.label_version_id = %s FOR KEY SHARE", (label_version_id,))
        row = cur.fetchone()
        return self._from_row(dict(row)) if row is not None else None

    def _select_terminal_locked(self, cur: Any, label_key_hash: str) -> OutcomeLabelVersion | None:
        cur.execute(
            self._select_join_sql()
            + " WHERE h.label_key_hash = %s ORDER BY h.label_revision_no DESC LIMIT 1 FOR UPDATE OF h",
            (label_key_hash,),
        )
        row = cur.fetchone()
        return self._from_row(dict(row)) if row is not None else None

    @staticmethod
    def _select_join_sql() -> str:
        return """
            SELECT h.*, p.scheduled_maturity_ts, p.source_closed_at, p.event_closed_at,
                   p.failure_observed_at, p.missing_source_receipt_hash,
                   p.projection_value_decimal, p.projection_event_code,
                   p.entry_price_raw_yuan, p.entry_adj_factor, p.exit_price_raw_yuan, p.exit_adj_factor,
                   p.entry_quantity, p.exit_quantity, p.buy_execution_price_yuan, p.sell_execution_price_yuan,
                   p.buy_notional_yuan, p.sell_notional_yuan, p.buy_fee_yuan, p.sell_fee_yuan,
                   p.entry_cash_yuan, p.residual_cash_yuan, p.exit_cash_yuan, p.terminal_value_yuan,
                   p.cost_breakdown_hash, p.benchmark_gross_total_return, p.benchmark_net_total_return,
                   p.entry_day_touch_status, p.executable_barrier_status,
                   p.executable_event_trade_date, p.time_to_executable_hit_trading_days,
                   p.observed_holding_trading_days, p.calculation_evidence_uri, p.reason_codes
            FROM app.advisory_outcome_label h
            JOIN app.advisory_outcome_label_payload p
              ON p.label_version_id = h.label_version_id
             AND p.decision_as_of_trade_date = h.decision_as_of_trade_date
        """

    def _require_creator_batch_active(self, cur: Any, *, capture_batch_id: str) -> None:
        cur.execute(
            """
            SELECT capture_status, capture_request_schema_version, capture_purpose,
                   lease_expires_at, fencing_token, clock_timestamp() AS database_now
              FROM app.advisory_capture_batch
             WHERE capture_batch_id = %s
             FOR KEY SHARE
            """,
            (capture_batch_id,),
        )
        row = cur.fetchone()
        if (
            row is None
            or row["capture_status"] != "RUNNING"
            or row["capture_request_schema_version"] != "advisory_phase1_capture_batch_v2"
            or row["capture_purpose"] != "LABEL_CAPTURE_V1"
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] <= row["database_now"]
        ):
            raise LabelBuilderError(REASON_LABEL_APPEND_REQUEST_CONFLICT, "label creator capture batch is not an active v2 label batch")

    @staticmethod
    def _validate_predecessor(*, request: LabelAppendRequest, predecessor: OutcomeLabelVersion | None) -> None:
        expected = (
            request.expected_predecessor_version_id,
            request.expected_predecessor_version_hash,
            request.expected_predecessor_revision_no,
        )
        if predecessor is None:
            if any(item is not None for item in expected):
                raise LabelBuilderError(REASON_LABEL_PREDECESSOR_INVALID, "first label revision cannot name a predecessor")
            return
        if any(item is None for item in expected):
            raise LabelBuilderError(REASON_LABEL_PREDECESSOR_INVALID, "next label revision requires the terminal predecessor")
        if (
            request.expected_predecessor_version_id != predecessor.label_version_id
            or request.expected_predecessor_version_hash != predecessor.label_content_hash
            or request.expected_predecessor_revision_no != predecessor.label_revision_no
        ):
            raise LabelBuilderError(REASON_LABEL_PREDECESSOR_INVALID, "label append predecessor is stale or not terminal")
        allowed = _ALLOWED_TRANSITIONS[predecessor.outcome_result.maturity_status]
        if request.outcome_result.maturity_status not in allowed:
            raise LabelBuilderError(REASON_LABEL_STATE_TRANSITION_INVALID, "label maturity transition is not allowed")
        if (
            request.label_source_revision_set_hash == predecessor.label_source_revision_set_hash
            and request.calculation_evidence_sha256 == predecessor.calculation_evidence_sha256
        ):
            raise LabelBuilderError(REASON_LABEL_APPEND_REQUEST_CONFLICT, "non-idempotent revision requires new source or evidence")

    def _insert_or_compare_blob(self, cur: Any, *, request: LabelAppendRequest) -> None:
        cur.execute(
            """
            INSERT INTO app.advisory_dataset_blob (store_backend_hash, blob_sha256, size_bytes)
            VALUES (%s, %s, %s)
            ON CONFLICT (store_backend_hash, blob_sha256) DO NOTHING
            """,
            (
                request.calculation_evidence_store_backend_hash,
                request.calculation_evidence_sha256,
                request.calculation_evidence_size_bytes,
            ),
        )
        cur.execute(
            """
            SELECT size_bytes FROM app.advisory_dataset_blob
             WHERE store_backend_hash = %s AND blob_sha256 = %s
             FOR KEY SHARE
            """,
            (request.calculation_evidence_store_backend_hash, request.calculation_evidence_sha256),
        )
        row = cur.fetchone()
        if row is None or int(row["size_bytes"]) != request.calculation_evidence_size_bytes:
            raise LabelBuilderError(REASON_LABEL_APPEND_REQUEST_CONFLICT, "calculation evidence blob identity conflicts with stored size")

    def _insert_header(self, cur: Any, version: OutcomeLabelVersion) -> None:
        owner = version.owner
        result = version.outcome_result
        cur.execute(
            """
            INSERT INTO app.advisory_outcome_label (
                label_version_id, label_content_hash, label_key_hash, label_revision_no,
                supersedes_label_version_id, supersedes_label_version_hash, label_append_request_hash,
                label_policy_bundle_id, label_policy_bundle_hash, label_policy_hash,
                label_source_revision_set_id, label_source_revision_set_hash,
                owner_type, owner_key, canonical_signal_id, observation_version_id,
                candidate_stage_evidence_id, symbol, universe_layer, decision_as_of_trade_date, evidence_scope,
                horizon_trading_days, projection, projection_schema_version,
                intended_entry_trade_date, earliest_sell_eligible_trade_date, exit_trade_date,
                maturity_status, outcome_event_status, entry_status, projection_payload_hash,
                calculation_evidence_sha256, calculation_evidence_size_bytes,
                calculation_evidence_store_backend_hash, created_by_capture_batch_id, computed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                version.label_version_id, version.label_content_hash, version.label_key_hash, version.label_revision_no,
                version.supersedes_label_version_id, version.supersedes_label_version_hash, version.label_append_request_hash,
                version.label_policy_bundle_id, version.label_policy_bundle_hash, version.label_policy_hash,
                version.label_source_revision_set_id, version.label_source_revision_set_hash,
                owner.owner_type.value, owner.owner_key, owner.canonical_signal_id, owner.observation_version_id,
                owner.candidate_stage_evidence_id, owner.symbol, owner.universe_layer, owner.decision_as_of_trade_date, owner.evidence_scope,
                version.horizon_trading_days, version.projection.value, version.projection_schema_version,
                result.intended_entry_trade_date, result.earliest_sell_eligible_trade_date, result.exit_trade_date,
                result.maturity_status.value, result.outcome_event_status.value, result.entry_status.value,
                result.projection_payload_hash, version.calculation_evidence_sha256, version.calculation_evidence_size_bytes,
                version.calculation_evidence_store_backend_hash, version.created_by_capture_batch_id, version.computed_at,
            ),
        )

    def _insert_payload(self, cur: Any, version: OutcomeLabelVersion) -> None:
        result = version.outcome_result
        cashflow = result.cashflow
        barrier = result.barrier
        evidence = result.calculation_evidence.evidence_payload
        terminal = evidence.get("terminal")
        if not isinstance(terminal, dict):
            raise LabelBuilderError(REASON_CALCULATION_EVIDENCE_INVALID, "calculation evidence has no terminal descriptor")
        price_path = evidence.get("price_path")
        corporate_actions = evidence.get("corporate_actions")
        benchmark = evidence.get("benchmark")
        if price_path is None or corporate_actions is None:
            raise LabelBuilderError(REASON_CALCULATION_EVIDENCE_INVALID, "calculation evidence has incomplete source slices")
        columns = (
            "decision_as_of_trade_date", "label_version_id", "label_content_hash",
            "projection", "projection_schema_version", "horizon_trading_days",
            "scheduled_maturity_ts", "source_closed_at", "event_closed_at", "failure_observed_at",
            "maturity_status", "outcome_event_status", "entry_status", "missing_source_receipt_hash",
            "projection_value_decimal", "projection_event_code", "projection_payload_hash",
            "entry_price_raw_yuan", "entry_adj_factor", "exit_price_raw_yuan", "exit_adj_factor",
            "entry_quantity", "exit_quantity", "buy_execution_price_yuan", "sell_execution_price_yuan",
            "buy_notional_yuan", "sell_notional_yuan", "buy_fee_yuan", "sell_fee_yuan",
            "entry_cash_yuan", "residual_cash_yuan", "exit_cash_yuan", "terminal_value_yuan",
            "cost_breakdown_hash", "benchmark_gross_total_return", "benchmark_net_total_return",
            "entry_day_touch_status", "executable_barrier_status", "executable_event_trade_date",
            "time_to_executable_hit_trading_days", "observed_holding_trading_days",
            "terminal_disposition", "terminal_symbol", "terminal_event_trade_date", "terminal_event_closed_at",
            "terminal_source_hash", "terminal_settlement_raw_li", "terminal_settlement_adj_factor",
            "terminal_settlement_quantity_multiplier", "terminal_settlement_cashflow_yuan_per_share", "censor_reason_code",
            "policy_bundle_hash", "price_path_hash", "corporate_actions_hash", "benchmark_bundle_hash",
            "formula_schema_version", "calculation_evidence_schema_version",
            "calculation_evidence_uri", "calculation_evidence_sha256", "calculation_evidence_size_bytes",
            "calculation_evidence_store_backend_hash", "reason_codes",
        )
        values = (
            version.owner.decision_as_of_trade_date, version.label_version_id, version.label_content_hash,
            version.projection.value, version.projection_schema_version, version.horizon_trading_days,
            result.scheduled_maturity_ts, result.source_closed_at, result.event_closed_at, result.failure_observed_at,
            result.maturity_status.value, result.outcome_event_status.value, result.entry_status.value,
            result.missing_source_receipt_hash, result.projection_value_decimal, result.projection_event_code,
            result.projection_payload_hash, result.entry_price_raw_yuan, result.entry_adj_factor,
            result.exit_price_raw_yuan, result.exit_adj_factor,
            cashflow.entry_quantity if cashflow else None, cashflow.exit_quantity if cashflow else None,
            cashflow.buy_execution_price_yuan if cashflow else None, cashflow.sell_execution_price_yuan if cashflow else None,
            cashflow.buy_notional_yuan if cashflow else None, cashflow.sell_notional_yuan if cashflow else None,
            cashflow.buy_fee_yuan if cashflow else None, cashflow.sell_fee_yuan if cashflow else None,
            cashflow.entry_cash_yuan if cashflow else None, cashflow.residual_cash_yuan if cashflow else None,
            cashflow.exit_cash_yuan if cashflow else None, cashflow.terminal_value_yuan if cashflow else None,
            cashflow.cost_breakdown_hash if cashflow else None, result.benchmark_gross_total_return,
            result.benchmark_net_total_return, barrier.entry_day_touch_status.value if barrier else None,
            barrier.executable_status.value if barrier else None, barrier.executable_event_trade_date if barrier else None,
            barrier.time_to_executable_hit_trading_days if barrier else None, result.observed_holding_trading_days,
            terminal.get("disposition"), terminal.get("symbol"), terminal.get("event_trade_date"), terminal.get("event_closed_at"),
            terminal.get("source_hash"), terminal.get("settlement_raw_li"), terminal.get("settlement_adj_factor"),
            terminal.get("settlement_quantity_multiplier"), terminal.get("settlement_cashflow_yuan_per_share"), terminal.get("censor_reason_code"),
            evidence.get("policy_bundle_hash"), canonical_json_sha256(price_path), canonical_json_sha256(corporate_actions),
            canonical_json_sha256(benchmark) if benchmark is not None else None, evidence.get("formula_schema_version"),
            result.calculation_evidence.schema_version, version.calculation_evidence_uri,
            version.calculation_evidence_sha256, version.calculation_evidence_size_bytes,
            version.calculation_evidence_store_backend_hash, psycopg2.extras.Json(list(result.reason_codes)),
        )
        cur.execute(
            f"INSERT INTO app.advisory_outcome_label_payload ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
            values,
        )

    def _from_row(self, row: Mapping[str, Any]) -> OutcomeLabelVersion:
        try:
            evidence = self._evidence_reader.get(
                uri=str(row["calculation_evidence_uri"]),
                sha256=str(row["calculation_evidence_sha256"]),
                size_bytes=int(row["calculation_evidence_size_bytes"]),
                store_backend_hash=str(row["calculation_evidence_store_backend_hash"]),
            )
        except CalculationEvidenceStoreError as error:
            raise LabelBuilderError(REASON_CALCULATION_EVIDENCE_INVALID, "stored calculation evidence cannot be read") from error
        owner = OutcomeOwner(
            owner_type=OwnerType(str(row["owner_type"])),
            owner_key=str(row["owner_key"]),
            canonical_signal_id=str(row["canonical_signal_id"]),
            observation_version_id=str(row["observation_version_id"]) if row["observation_version_id"] else None,
            candidate_stage_evidence_id=(str(row["candidate_stage_evidence_id"]) if row["candidate_stage_evidence_id"] else None),
            symbol=str(row["symbol"]),
            decision_as_of_trade_date=row["decision_as_of_trade_date"],
            universe_layer=str(row["universe_layer"]) if row["universe_layer"] else None,
            evidence_scope=str(row["evidence_scope"]),
        )
        cashflow = None
        if row["entry_quantity"] is not None:
            cashflow = CashflowResult(
                entry_quantity=Decimal(row["entry_quantity"]), exit_quantity=Decimal(row["exit_quantity"]),
                buy_execution_price_yuan=Decimal(row["buy_execution_price_yuan"]),
                sell_execution_price_yuan=Decimal(row["sell_execution_price_yuan"]),
                buy_notional_yuan=Decimal(row["buy_notional_yuan"]), sell_notional_yuan=Decimal(row["sell_notional_yuan"]),
                buy_fee_yuan=Decimal(row["buy_fee_yuan"]), sell_fee_yuan=Decimal(row["sell_fee_yuan"]),
                entry_cash_yuan=Decimal(row["entry_cash_yuan"]), residual_cash_yuan=Decimal(row["residual_cash_yuan"]),
                exit_cash_yuan=Decimal(row["exit_cash_yuan"]), terminal_value_yuan=Decimal(row["terminal_value_yuan"]),
                cost_breakdown_hash=str(row["cost_breakdown_hash"]),
            )
        barrier = None
        if row["entry_day_touch_status"] is not None:
            barrier = BarrierResult(
                entry_day_touch_status=BarrierStatus(str(row["entry_day_touch_status"])),
                executable_status=BarrierStatus(str(row["executable_barrier_status"])),
                executable_event_trade_date=row["executable_event_trade_date"],
                time_to_executable_hit_trading_days=row["time_to_executable_hit_trading_days"],
            )
        result = OutcomeCalculationResult(
            owner=owner,
            projection=Projection(str(row["projection"])),
            horizon_trading_days=int(row["horizon_trading_days"]),
            decision_trade_date=row["decision_as_of_trade_date"],
            intended_entry_trade_date=row["intended_entry_trade_date"],
            earliest_sell_eligible_trade_date=row["earliest_sell_eligible_trade_date"],
            exit_trade_date=row["exit_trade_date"],
            scheduled_maturity_ts=row["scheduled_maturity_ts"],
            maturity_status=MaturityStatus(str(row["maturity_status"])),
            outcome_event_status=OutcomeEventStatus(str(row["outcome_event_status"])),
            entry_status=EntryStatus(str(row["entry_status"])),
            projection_value_decimal=Decimal(row["projection_value_decimal"]) if row["projection_value_decimal"] is not None else None,
            projection_event_code=str(row["projection_event_code"]) if row["projection_event_code"] else None,
            entry_price_raw_yuan=Decimal(row["entry_price_raw_yuan"]) if row["entry_price_raw_yuan"] is not None else None,
            entry_adj_factor=Decimal(row["entry_adj_factor"]) if row["entry_adj_factor"] is not None else None,
            exit_price_raw_yuan=Decimal(row["exit_price_raw_yuan"]) if row["exit_price_raw_yuan"] is not None else None,
            exit_adj_factor=Decimal(row["exit_adj_factor"]) if row["exit_adj_factor"] is not None else None,
            source_closed_at=row["source_closed_at"], event_closed_at=row["event_closed_at"],
            failure_observed_at=row["failure_observed_at"],
            missing_source_receipt_hash=str(row["missing_source_receipt_hash"]) if row["missing_source_receipt_hash"] else None,
            cashflow=cashflow,
            benchmark_gross_total_return=(Decimal(row["benchmark_gross_total_return"]) if row["benchmark_gross_total_return"] is not None else None),
            benchmark_net_total_return=(Decimal(row["benchmark_net_total_return"]) if row["benchmark_net_total_return"] is not None else None),
            barrier=barrier,
            observed_holding_trading_days=row["observed_holding_trading_days"],
            reason_codes=tuple(str(item) for item in (row["reason_codes"] or [])),
            projection_payload_hash=str(row["projection_payload_hash"]),
            calculation_evidence=evidence,
        )
        version = OutcomeLabelVersion(
            label_key_hash=str(row["label_key_hash"]), label_revision_no=int(row["label_revision_no"]),
            supersedes_label_version_id=(str(row["supersedes_label_version_id"]) if row["supersedes_label_version_id"] else None),
            supersedes_label_version_hash=(str(row["supersedes_label_version_hash"]) if row["supersedes_label_version_hash"] else None),
            label_append_request_hash=str(row["label_append_request_hash"]),
            label_policy_bundle_id=str(row["label_policy_bundle_id"]), label_policy_bundle_hash=str(row["label_policy_bundle_hash"]),
            label_policy_hash=str(row["label_policy_hash"]), label_source_revision_set_id=str(row["label_source_revision_set_id"]),
            label_source_revision_set_hash=str(row["label_source_revision_set_hash"]), owner=owner,
            horizon_trading_days=int(row["horizon_trading_days"]), projection=Projection(str(row["projection"])),
            projection_schema_version=str(row["projection_schema_version"]), outcome_result=result,
            calculation_evidence_sha256=str(row["calculation_evidence_sha256"]),
            calculation_evidence_size_bytes=int(row["calculation_evidence_size_bytes"]),
            calculation_evidence_store_backend_hash=str(row["calculation_evidence_store_backend_hash"]),
            calculation_evidence_uri=str(row["calculation_evidence_uri"]),
            created_by_capture_batch_id=str(row["created_by_capture_batch_id"]), computed_at=row["computed_at"],
            label_content_hash=str(row["label_content_hash"]), label_version_id=str(row["label_version_id"]),
        )
        _validate_header_payload(
            version=version,
            header=OutcomeLabelAuthorityHeader.from_version(version),
            payload=OutcomeLabelPayload.from_version(version),
        )
        return version

    @staticmethod
    def _assert_exact_retry(*, existing: OutcomeLabelVersion, request: LabelAppendRequest) -> None:
        candidate = LabelAppendRequest(
            label_key_hash=existing.label_key_hash,
            expected_predecessor_version_id=existing.supersedes_label_version_id,
            expected_predecessor_version_hash=existing.supersedes_label_version_hash,
            expected_predecessor_revision_no=(existing.label_revision_no - 1 if existing.label_revision_no > 1 else None),
            label_policy_bundle_id=existing.label_policy_bundle_id,
            label_policy_bundle_hash=existing.label_policy_bundle_hash,
            label_policy_hash=existing.label_policy_hash,
            label_source_revision_set_id=existing.label_source_revision_set_id,
            label_source_revision_set_hash=existing.label_source_revision_set_hash,
            owner=existing.owner, horizon_trading_days=existing.horizon_trading_days,
            projection=existing.projection, projection_schema_version=existing.projection_schema_version,
            outcome_result=existing.outcome_result,
            projection_payload_hash=str(existing.outcome_result.projection_payload_hash),
            calculation_evidence_sha256=existing.calculation_evidence_sha256,
            calculation_evidence_size_bytes=existing.calculation_evidence_size_bytes,
            calculation_evidence_store_backend_hash=existing.calculation_evidence_store_backend_hash,
            calculation_evidence_uri=existing.calculation_evidence_uri,
            label_append_request_hash=existing.label_append_request_hash,
        )
        if candidate.canonical_payload() != request.canonical_payload() or candidate.label_key_hash != request.label_key_hash:
            raise LabelBuilderError(REASON_LABEL_APPEND_REQUEST_CONFLICT, "same append hash has different semantic request")

    @staticmethod
    def _validate_chain(chain: tuple[OutcomeLabelVersion, ...]) -> None:
        for expected_revision, version in enumerate(chain, start=1):
            if version.label_revision_no != expected_revision:
                raise LabelBuilderError(REASON_LABEL_REVISION_CHAIN_INVALID, "label revision chain is not continuous")
            if expected_revision == 1:
                if version.supersedes_label_version_id is not None:
                    raise LabelBuilderError(REASON_LABEL_REVISION_CHAIN_INVALID, "first label revision has predecessor")
            else:
                predecessor = chain[expected_revision - 2]
                if (
                    version.supersedes_label_version_id != predecessor.label_version_id
                    or version.supersedes_label_version_hash != predecessor.label_content_hash
                ):
                    raise LabelBuilderError(REASON_LABEL_REVISION_CHAIN_INVALID, "label predecessor chain is invalid")

    @staticmethod
    def _raise_integrity(error: psycopg2.IntegrityError, *, label: str) -> None:
        constraint = getattr(error.diag, "constraint_name", None)
        message = str(error).lower()
        logger.error(
            "advisory_phase1c3 label database invariant violation operation=%s pgcode=%s constraint=%s",
            label,
            getattr(error, "pgcode", None),
            constraint or "unknown",
        )
        if "no partition of relation" in message or constraint == "advisory_outcome_label_payload_month_partition_missing":
            raise LabelBuilderError(REASON_LABEL_PARTITION_MISSING, f"{label} target month partition is missing") from error
        raise LabelBuilderError(REASON_DATABASE_INVARIANT_VIOLATION, f"{label} violated database invariant {constraint or 'unknown'}") from error
