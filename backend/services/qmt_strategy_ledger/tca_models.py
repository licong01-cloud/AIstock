"""Typed immutable records for MiniQMT Phase 0A TCA evidence.

The records in this module are persistence DTOs.  They deliberately contain no
broker operations and expose no mutation API.  Calculator-specific construction
belongs to Phase 0A-3; Phase 0A-2 freezes the database row contract and hashes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from enum import Enum
from hashlib import sha256
from types import MappingProxyType
from typing import Any, ClassVar, Mapping


TCA_SCHEMA_VERSION = "miniqmt_execution_tca_phase0a_v1"
TCA_NORMALIZATION_VERSION = "miniqmt_trade_normalization_v1"
TCA_BROKER_TIME_PARSER_VERSION = "xtquant_trade_time_v1"


class TcaInsertOutcome(str, Enum):
    """Immutable insert result; conflicts are never last-write-wins."""

    INSERTED = "INSERTED"
    IDEMPOTENT = "IDEMPOTENT"
    CONFLICT = "CONFLICT"
    SOURCE_MISSING = "SOURCE_MISSING"


class TcaTradeObservationOutcome(str, Enum):
    """Result of atomically recording one transport observation."""

    INSERTED = "INSERTED"
    IDEMPOTENT = "IDEMPOTENT"
    CANONICAL_CONFLICT = "CANONICAL_CONFLICT"
    TRADE_TIME_CONFLICT = "TRADE_TIME_CONFLICT"


def canonical_json_sha256(value: Any) -> str:
    """Hash JSON-compatible evidence without binary-float coercion."""

    encoded = json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def canonical_json_value(value: Any) -> Any:
    """Return the same JSON-safe value used by canonical evidence hashing."""

    return _json_safe(value)


def canonical_tca_manifest_sha256(value: Any) -> str:
    """Hash Phase 0A-3 manifests with UTC milliseconds and fixed Decimal text."""

    return canonical_json_sha256(_tca_manifest_safe(value))


def content_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}{canonical_json_sha256(parts)[:32]}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return normalized.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def _tca_manifest_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _tca_manifest_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_tca_manifest_safe(item) for item in value]
    if isinstance(value, Decimal):
        with localcontext() as context:
            context.prec = max(context.prec, 38)
            context.rounding = ROUND_HALF_EVEN
            return format(value.quantize(Decimal("0.00000001")), "f")
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return normalized.astimezone(UTC).isoformat(timespec="milliseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


@dataclass(frozen=True, slots=True)
class ImmutableTcaRow:
    """A schema-bound immutable row with explicit identity and evidence hash."""

    values: Mapping[str, Any]
    table_name: ClassVar[str] = ""
    identity_fields: ClassVar[tuple[str, ...]] = ()
    evidence_hash_field: ClassVar[str] = ""
    required_fields: ClassVar[frozenset[str]] = frozenset()

    def __post_init__(self) -> None:
        copied = dict(self.values)
        missing = sorted(self.required_fields.difference(copied))
        if missing:
            raise ValueError(f"{self.table_name} missing required fields: {missing}")
        for field_name in self.identity_fields:
            value = copied.get(field_name)
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"{self.table_name}.{field_name} is required")
        evidence_hash = copied.get(self.evidence_hash_field) if self.evidence_hash_field else None
        if self.evidence_hash_field and not _is_sha256(evidence_hash):
            raise ValueError(f"{self.table_name}.{self.evidence_hash_field} must be lowercase sha256")
        object.__setattr__(self, "values", MappingProxyType(copied))

    @property
    def identity(self) -> tuple[Any, ...]:
        return tuple(self.values[field_name] for field_name in self.identity_fields)

    @property
    def evidence_sha256(self) -> str:
        if not self.evidence_hash_field:
            return canonical_json_sha256(self.values)
        return str(self.values[self.evidence_hash_field])


class ExecutionPlanningSubject(ImmutableTcaRow):
    table_name = "execution_planning_subject"
    identity_fields = ("planning_subject_id",)
    evidence_hash_field = "evidence_sha256"
    required_fields = frozenset(
        {
            "planning_subject_id",
            "trading_rule_decision_id",
            "run_id",
            "execution_plan_id",
            "execution_plan_hash",
            "binding_id",
            "binding_hash",
            "trade_date",
            "symbol",
            "side",
            "decision",
            "evidence",
            "evidence_sha256",
        }
    )


class ExecutionParentBenchmark(ImmutableTcaRow):
    table_name = "execution_parent_benchmark"
    identity_fields = ("parent_intent_id", "parent_revision")
    evidence_hash_field = "evidence_sha256"
    required_fields = frozenset(
        {
            "parent_intent_id",
            "parent_revision",
            "run_id",
            "execution_plan_id",
            "execution_plan_hash",
            "binding_id",
            "binding_hash",
            "account_id",
            "trade_date",
            "environment",
            "symbol",
            "side",
            "decision_quality",
            "arrival_quality",
            "eligibility_class",
            "eligibility_quality",
            "evidence_sha256",
        }
    )


class ExecutionTcaTradeObservation(ImmutableTcaRow):
    table_name = "execution_tca_trade_observation"
    identity_fields = ("trade_observation_id",)
    evidence_hash_field = "raw_observation_sha256"
    required_fields = frozenset(
        {
            "trade_observation_id",
            "account_id",
            "trade_date",
            "trade_id",
            "symbol",
            "side",
            "ingest_source",
            "observed_at",
            "price",
            "quantity",
            "amount",
            "canonical_trade_fact_sha256",
            "timing_observation_sha256",
            "attribution_sha256",
            "fee_observation_sha256",
            "raw_observation_sha256",
            "normalized_payload",
            "raw_payload",
            "normalization_version",
            "broker_time_parser_version",
        }
    )


class ExecutionTcaTradeConflict(ImmutableTcaRow):
    table_name = "execution_tca_trade_conflict"
    identity_fields = ("trade_conflict_fact_id",)
    evidence_hash_field = "fact_sha256"
    required_fields = frozenset(
        {
            "trade_conflict_fact_id",
            "conflict_series_key",
            "conflict_generation",
            "account_id",
            "trade_date",
            "trade_id",
            "conflict_type",
            "conflict_status",
            "incoming_observation_id",
            "existing_ingest_source",
            "incoming_ingest_source",
            "existing_canonical_sha256",
            "incoming_canonical_sha256",
            "existing_timing_sha256",
            "incoming_timing_sha256",
            "detected_at",
            "fact_sha256",
        }
    )


class ExecutionTcaMark(ImmutableTcaRow):
    table_name = "execution_tca_mark"
    identity_fields = ("mark_id",)
    evidence_hash_field = "evidence_sha256"
    required_fields = frozenset({"mark_id", "parent_intent_id", "parent_revision", "mark_type", "evidence_sha256"})


class ExecutionTcaRebuildReceipt(ImmutableTcaRow):
    table_name = "execution_tca_rebuild_receipt"
    identity_fields = ("receipt_id",)
    evidence_hash_field = "canonical_output_sha256"
    required_fields = frozenset(
        {"receipt_id", "receipt_scope_hash", "receipt_generation", "receipt_status", "snapshot_kind", "canonical_output_sha256"}
    )


class ExecutionParentTca(ImmutableTcaRow):
    table_name = "execution_parent_tca"
    identity_fields = ("tca_result_id",)
    evidence_hash_field = "canonical_output_sha256"
    required_fields = frozenset(
        {"tca_result_id", "result_series_key", "result_generation", "parent_intent_id", "parent_revision", "snapshot_kind", "canonical_input_sha256", "canonical_output_sha256"}
    )


class ExecutionTcaReceiptPlanningSubject(ImmutableTcaRow):
    table_name = "execution_tca_receipt_planning_subject"
    identity_fields = ("receipt_id", "planning_subject_id")
    evidence_hash_field = "membership_hash"
    required_fields = frozenset({"receipt_id", "receipt_status", "planning_subject_id", "classification", "membership_hash"})


class ExecutionTcaReceiptResult(ImmutableTcaRow):
    table_name = "execution_tca_receipt_result"
    identity_fields = ("receipt_id", "tca_result_id")
    evidence_hash_field = "membership_hash"
    required_fields = frozenset(
        {"receipt_id", "receipt_status", "tca_result_id", "parent_intent_id", "parent_revision", "snapshot_kind", "membership_hash"}
    )


class ExecutionTcaResultMark(ImmutableTcaRow):
    table_name = "execution_tca_result_mark"
    identity_fields = ("tca_result_id", "mark_id", "mark_role")
    evidence_hash_field = "membership_hash"
    required_fields = frozenset(
        {"tca_result_id", "mark_id", "parent_intent_id", "parent_revision", "mark_role", "membership_hash"}
    )


class ExecutionTcaResultTradeObservation(ImmutableTcaRow):
    table_name = "execution_tca_result_trade_observation"
    identity_fields = ("tca_result_id", "trade_observation_id", "observation_role")
    evidence_hash_field = "membership_hash"
    required_fields = frozenset(
        {
            "tca_result_id",
            "trade_observation_id",
            "parent_intent_id",
            "parent_revision",
            "trade_account_id",
            "trade_date",
            "trade_id",
            "observation_role",
            "selected_content_sha256",
            "membership_hash",
        }
    )


@dataclass(frozen=True, slots=True)
class TcaMaterializationBundle:
    """Rows written atomically by one rebuild receipt transaction."""

    receipt: ExecutionTcaRebuildReceipt
    planning_subjects: tuple[ExecutionPlanningSubject, ...] = ()
    parent_benchmarks: tuple[ExecutionParentBenchmark, ...] = ()
    marks: tuple[ExecutionTcaMark, ...] = ()
    results: tuple[ExecutionParentTca, ...] = ()
    receipt_subjects: tuple[ExecutionTcaReceiptPlanningSubject, ...] = ()
    receipt_results: tuple[ExecutionTcaReceiptResult, ...] = ()
    result_marks: tuple[ExecutionTcaResultMark, ...] = ()
    result_trade_observations: tuple[ExecutionTcaResultTradeObservation, ...] = ()


@dataclass(frozen=True, slots=True)
class TcaMaterializationOutcome:
    receipt: TcaInsertOutcome
    inserted_rows: int
    idempotent_rows: int
    conflicts: tuple[str, ...] = field(default_factory=tuple)


def canonical_trade_fact_payload(
    *,
    account_id: str,
    trade_date: date,
    trade_id: str,
    qmt_order_id: str,
    symbol: str,
    side: str,
    price: Decimal,
    quantity: int,
) -> dict[str, Any]:
    amount = price * Decimal(quantity)
    return {
        "account_id": account_id,
        "trade_date": trade_date,
        "trade_id": trade_id,
        "qmt_order_id": qmt_order_id,
        "symbol": symbol,
        "side": side,
        "price": price,
        "quantity": quantity,
        "amount": amount,
        "normalization_version": TCA_NORMALIZATION_VERSION,
    }


def canonical_trade_fact_sha256(**values: Any) -> str:
    return canonical_json_sha256(canonical_trade_fact_payload(**values))


def build_trade_observation(
    *,
    account_id: str,
    trade_date: date,
    trade_id: str,
    intent_id: str | None,
    qmt_order_id: str,
    child_order_id: str | None,
    symbol: str,
    side: str,
    ingest_source: str,
    observed_at: datetime,
    broker_trade_time: datetime | None,
    price: Decimal,
    quantity: int,
    commission: Decimal | None,
    raw_payload: Mapping[str, Any],
    reconciliation_run_id: str | None = None,
) -> ExecutionTcaTradeObservation:
    """Create role-separated hashes for one callback/snapshot observation."""

    if ingest_source not in {"BROKER_CALLBACK", "BROKER_SNAPSHOT_SYNC"}:
        raise ValueError(f"unsupported TCA trade ingest source: {ingest_source}")
    if quantity <= 0 or price <= 0:
        raise ValueError("TCA trade observation requires positive price and quantity")
    canonical_payload = canonical_trade_fact_payload(
        account_id=account_id,
        trade_date=trade_date,
        trade_id=trade_id,
        qmt_order_id=qmt_order_id,
        symbol=symbol,
        side=side,
        price=price,
        quantity=quantity,
    )
    raw_hash = canonical_json_sha256(raw_payload)
    timing_hash = canonical_json_sha256(
        {
            "broker_trade_time": broker_trade_time,
            "parser_version": TCA_BROKER_TIME_PARSER_VERSION,
        }
    )
    attribution_hash = canonical_json_sha256(
        {"intent_id": intent_id, "child_order_id": child_order_id, "policy_version": "intent_attribution_v1"}
    )
    raw_fee_present = any(key in raw_payload for key in ("commission", "fee", "traded_commission"))
    fee_level = "TRADE_LEVEL" if raw_fee_present and commission is not None else "MISSING"
    fee_hash = canonical_json_sha256(
        {"commission": commission if raw_fee_present else None, "fee_evidence_level": fee_level, "policy_version": "fee_observation_v1"}
    )
    canonical_hash = canonical_json_sha256(canonical_payload)
    values = {
        "trade_observation_id": content_id("tcaobs_", account_id, trade_date, trade_id, ingest_source, raw_hash),
        "account_id": account_id,
        "trade_date": trade_date,
        "trade_id": trade_id,
        "intent_id": intent_id,
        "qmt_order_id": qmt_order_id,
        "child_order_id": child_order_id,
        "symbol": symbol,
        "side": side,
        "ingest_source": ingest_source,
        "observed_at": observed_at,
        "broker_trade_time": broker_trade_time,
        "price": price,
        "quantity": quantity,
        "amount": price * Decimal(quantity),
        "commission": commission if raw_fee_present else None,
        "fee_evidence_level": fee_level,
        "canonical_trade_fact_sha256": canonical_hash,
        "timing_observation_sha256": timing_hash,
        "attribution_sha256": attribution_hash,
        "fee_observation_sha256": fee_hash,
        "raw_observation_sha256": raw_hash,
        "normalized_payload": canonical_payload,
        "raw_payload": dict(raw_payload),
        "reconciliation_run_id": reconciliation_run_id,
        "normalization_version": TCA_NORMALIZATION_VERSION,
        "broker_time_parser_version": TCA_BROKER_TIME_PARSER_VERSION,
    }
    return ExecutionTcaTradeObservation(values)


def _is_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)
