"""Read-only, request-scoped evidence for the derived Advisory ST PIT universe."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
import hashlib
import json
from typing import Any, Callable, Mapping

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_dev_input_onboarding.contracts import validate_sha256
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.source_ledger import (
    SourceAvailabilityEvent,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
    source_partition_chain_key,
)
from backend.services.advisory_phase1.source_observer import (
    AuditRowSnapshot,
    ObservationDecision,
    ObservationOutcome,
    SourceObserverConfigBundle,
    SourceObserverError,
    SourcePartitionDescriptor,
    SOURCE_QUERY_TEMPLATES,
)


DERIVED_PIT_EVIDENCE_SCHEMA_VERSION = "advisory_phase1e_derived_pit_evidence_v1"
DERIVED_PIT_SERVICE_PRINCIPAL = "advisory-phase1e-derived-pit-evidence"

REASON_DERIVED_PIT_STATE_MISSING = "ADVISORY_PHASE1E_DERIVED_PIT_STATE_MISSING"
REASON_DERIVED_PIT_STATE_BUILDING = "ADVISORY_PHASE1E_DERIVED_PIT_STATE_BUILDING"
REASON_DERIVED_PIT_STATE_CONFLICT = "ADVISORY_PHASE1E_DERIVED_PIT_STATE_CONFLICT"
REASON_DERIVED_PIT_STATE_NOT_AVAILABLE_AS_OF = "ADVISORY_PHASE1E_DERIVED_PIT_STATE_NOT_AVAILABLE_AS_OF"
REASON_DERIVED_PIT_UPSTREAM_AUDIT_MISSING = "ADVISORY_PHASE1E_DERIVED_PIT_UPSTREAM_AUDIT_MISSING"
REASON_DERIVED_PIT_UPSTREAM_AUDIT_CONFLICT = "ADVISORY_PHASE1E_DERIVED_PIT_UPSTREAM_AUDIT_CONFLICT"
REASON_DERIVED_PIT_SPANS_CONFLICT = "ADVISORY_PHASE1E_DERIVED_PIT_SPANS_CONFLICT"
REASON_DERIVED_PIT_IDENTITY_CONFLICT = "ADVISORY_PHASE1E_DERIVED_PIT_IDENTITY_CONFLICT"

_REQUIRED_UPSTREAM_AUDITS = ("stock_basic", "stock_st_events", "trading_calendar")
_DERIVED_RELATIONS = (
    ("pit_universe", "market.stock_universe_pit_spans"),
    ("pit_universe_build_state", "market.stock_universe_pit_state"),
)
_STATE_COLUMNS = (
    "universe_key",
    "rule_version",
    "scope",
    "start_date",
    "end_date",
    "status",
    "dirty",
    "source_fingerprint",
    "source_fingerprint_sha256",
    "last_build_summary",
    "last_error",
    "generated_at",
    "updated_at",
)
_SPAN_COLUMNS = (
    "universe_key",
    "ts_code",
    "eligible_start",
    "eligible_end",
    "entry_reason",
    "exit_reason",
    "base_list_date",
    "ipo_eligible_date",
    "entry_event_date",
    "exit_event_date",
    "terminal_exit",
    "rule_version",
    "generated_at",
    "metadata",
)

ConnFactory = Callable[[], AbstractContextManager[Any]]


class DerivedPitEvidenceStatus(str, Enum):
    READY = "READY"
    PENDING = "PENDING"
    BLOCKED = "BLOCKED"


class DerivedPitEvidenceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    target_database_identity_hash: str = Field(min_length=64, max_length=64)
    program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    universe_key: str = Field(min_length=1, max_length=160)
    decision_cutoff_ts: datetime
    observer_config_hash: str = Field(min_length=64, max_length=64)
    query_registry_hash: str = Field(min_length=64, max_length=64)

    @field_validator("target_database_identity_hash", "observer_config_hash", "query_registry_hash")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return validate_sha256(value, field_name=info.field_name)

    @field_validator("decision_cutoff_ts")
    @classmethod
    def _cutoff(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("decision_cutoff_ts must be timezone-aware")
        return value.astimezone(timezone.utc)


class DerivedPitEvidenceReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = DERIVED_PIT_EVIDENCE_SCHEMA_VERSION
    target_database_identity_hash: str = Field(min_length=64, max_length=64)
    program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    universe_key: str = Field(min_length=1, max_length=160)
    decision_cutoff_ts: datetime
    observer_config_hash: str = Field(min_length=64, max_length=64)
    query_registry_hash: str = Field(min_length=64, max_length=64)
    status: DerivedPitEvidenceStatus
    state_row_hash: str | None = Field(default=None, min_length=64, max_length=64)
    state_source_fingerprint_hash: str | None = Field(default=None, min_length=64, max_length=64)
    spans_content_hash: str | None = Field(default=None, min_length=64, max_length=64)
    spans_row_count: int | None = Field(default=None, ge=0)
    upstream_audit_row_hashes: dict[str, str] = Field(default_factory=dict)
    evidence_available_at: datetime | None = None
    reason_codes: tuple[str, ...] = ()
    evidence_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "target_database_identity_hash",
        "observer_config_hash",
        "query_registry_hash",
        "state_row_hash",
        "state_source_fingerprint_hash",
        "spans_content_hash",
        "evidence_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validate_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("decision_cutoff_ts", "evidence_available_at")
    @classmethod
    def _times(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("derived PIT evidence timestamps must be timezone-aware")
        return value.astimezone(timezone.utc)

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"evidence_hash"})

    @model_validator(mode="after")
    def _close(self) -> "DerivedPitEvidenceReceipt":
        reasons = tuple(sorted(set(self.reason_codes)))
        hashes = {key: validate_sha256(value, field_name=f"upstream_audit_row_hashes[{key}]") for key, value in self.upstream_audit_row_hashes.items()}
        if set(hashes) - set(_REQUIRED_UPSTREAM_AUDITS):
            raise ValueError("derived PIT evidence contains an unknown upstream audit identity")
        ready_values = (
            self.state_row_hash,
            self.state_source_fingerprint_hash,
            self.spans_content_hash,
            self.spans_row_count,
            self.evidence_available_at,
        )
        if self.status is DerivedPitEvidenceStatus.READY:
            if any(value is None for value in ready_values) or set(hashes) != set(_REQUIRED_UPSTREAM_AUDITS) or reasons:
                raise ValueError("READY derived PIT evidence requires complete state, spans and upstream audit closure")
        elif not reasons:
            raise ValueError("non-ready derived PIT evidence requires stable reason codes")
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "upstream_audit_row_hashes", dict(sorted(hashes.items())))
        digest = canonical_json_sha256(self.canonical_payload())
        if self.evidence_hash is not None and self.evidence_hash != digest:
            raise ValueError("evidence_hash does not match derived PIT evidence")
        object.__setattr__(self, "evidence_hash", digest)
        return self


@dataclass(frozen=True)
class DerivedPitEvidenceResult:
    receipt: DerivedPitEvidenceReceipt
    state_row: Mapping[str, Any] | None = None
    span_rows: tuple[Mapping[str, Any], ...] = ()


class AdvisoryDerivedPitEvidenceProbe:
    """Observe exact DEV PIT state/spans and their build-time audit closure without writes."""

    def __init__(self, *, conn_factory: ConnFactory) -> None:
        self._conn_factory = conn_factory

    def probe(
        self,
        *,
        request: DerivedPitEvidenceRequest,
        config: SourceObserverConfigBundle,
    ) -> DerivedPitEvidenceResult:
        if request.observer_config_hash != config.config_hash(SOURCE_QUERY_TEMPLATES):
            raise SourceObserverError(
                REASON_DERIVED_PIT_IDENTITY_CONFLICT,
                "derived PIT request does not bind the selected observer config",
            )
        if request.query_registry_hash != config.query_registry_hash(SOURCE_QUERY_TEMPLATES):
            raise SourceObserverError(
                REASON_DERIVED_PIT_IDENTITY_CONFLICT,
                "derived PIT request does not bind the selected query registry",
            )
        specs = {spec.audit_dataset_name: spec for spec in config.dataset_specs}
        if any(dataset not in specs for dataset in _REQUIRED_UPSTREAM_AUDITS):
            raise SourceObserverError(
                REASON_DERIVED_PIT_IDENTITY_CONFLICT,
                "observer config does not cover every derived PIT upstream audit",
            )

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cur.execute(
                    f"SELECT {', '.join(_STATE_COLUMNS)} FROM market.stock_universe_pit_state WHERE universe_key = %s",
                    (request.universe_key,),
                )
                raw_state = cur.fetchone()
                if raw_state is None:
                    return self._result(request, DerivedPitEvidenceStatus.PENDING, (REASON_DERIVED_PIT_STATE_MISSING,))
                state = dict(raw_state)
                state_result = self._validate_state(request=request, state=state)
                if state_result is not None:
                    return state_result

                fingerprint_end_date = _fingerprint_end_date(state)
                evidence_cutoff = min(request.decision_cutoff_ts, state["generated_at"].astimezone(timezone.utc))
                audits: dict[str, AuditRowSnapshot] = {}
                for dataset in _REQUIRED_UPSTREAM_AUDITS:
                    audit_result = self._select_upstream_audit(
                        cur=cur,
                        dataset=dataset,
                        trade_date=fingerprint_end_date,
                        allowed_data_sources=specs[dataset].allowed_data_sources,
                        eligible_quality_statuses=specs[dataset].eligible_quality_statuses,
                        cutoff=evidence_cutoff,
                    )
                    if isinstance(audit_result, tuple):
                        status, reason = audit_result
                        return self._result(
                            request,
                            status,
                            (reason,),
                            state=state,
                        )
                    audits[dataset] = audit_result

                cur.execute(
                    f"""
                    SELECT {', '.join(_SPAN_COLUMNS)}
                    FROM market.stock_universe_pit_spans
                    WHERE universe_key = %s
                      AND eligible_start <= %s
                      AND eligible_end >= %s
                    ORDER BY ts_code, eligible_start, eligible_end
                    """,
                    (request.universe_key, request.decision_trade_date, request.decision_trade_date),
                )
                spans = tuple(dict(row) for row in cur.fetchall())

        return self._build_ready_result(request=request, state=state, spans=spans, audits=audits)

    @staticmethod
    def _validate_state(
        *,
        request: DerivedPitEvidenceRequest,
        state: Mapping[str, Any],
    ) -> DerivedPitEvidenceResult | None:
        status = str(state.get("status") or "").lower()
        if status == "building":
            return AdvisoryDerivedPitEvidenceProbe._result(
                request,
                DerivedPitEvidenceStatus.PENDING,
                (REASON_DERIVED_PIT_STATE_BUILDING,),
                state=state,
            )
        if (
            status != "ready"
            or bool(state.get("dirty"))
            or state.get("start_date") is None
            or state.get("end_date") is None
            or state["start_date"] > request.decision_trade_date
            or state["end_date"] < request.decision_trade_date
            or not state.get("generated_at")
            or not isinstance(state.get("source_fingerprint"), dict)
            or not state.get("source_fingerprint_sha256")
        ):
            return AdvisoryDerivedPitEvidenceProbe._result(
                request,
                DerivedPitEvidenceStatus.BLOCKED,
                (REASON_DERIVED_PIT_STATE_CONFLICT,),
                state=state,
            )
        generated_at = state["generated_at"].astimezone(timezone.utc)
        if generated_at > request.decision_cutoff_ts:
            return AdvisoryDerivedPitEvidenceProbe._result(
                request,
                DerivedPitEvidenceStatus.PENDING,
                (REASON_DERIVED_PIT_STATE_NOT_AVAILABLE_AS_OF,),
                state=state,
            )
        fingerprint = state["source_fingerprint"]
        fingerprint_end = fingerprint.get("fingerprint_end_date")
        if (
            _stock_universe_fingerprint_hash(fingerprint) != str(state["source_fingerprint_sha256"])
            or (fingerprint_end is not None and str(fingerprint_end) != state["end_date"].isoformat())
        ):
            return AdvisoryDerivedPitEvidenceProbe._result(
                request,
                DerivedPitEvidenceStatus.BLOCKED,
                (REASON_DERIVED_PIT_STATE_CONFLICT,),
                state=state,
            )
        return None

    @staticmethod
    def _select_upstream_audit(
        *,
        cur: Any,
        dataset: str,
        trade_date: date,
        allowed_data_sources: tuple[str, ...],
        eligible_quality_statuses: tuple[str, ...],
        cutoff: datetime,
    ) -> AuditRowSnapshot | tuple[DerivedPitEvidenceStatus, str]:
        cur.execute(
            """
            SELECT dataset, trade_date, data_source, job_id, status, row_count, refreshed_at, error_message,
                   metadata, data_max_at, written_rows, expected_rows, coverage_ratio, quality_status, failure_category
            FROM market.dataset_date_refresh_audit
            WHERE dataset = %s
              AND trade_date = %s
              AND data_source = ANY(%s)
              AND refreshed_at <= %s
            ORDER BY refreshed_at DESC, data_source
            LIMIT 2
            """,
            (dataset, trade_date, list(allowed_data_sources), cutoff),
        )
        rows = tuple(dict(row) for row in cur.fetchall())
        if not rows:
            return DerivedPitEvidenceStatus.PENDING, REASON_DERIVED_PIT_UPSTREAM_AUDIT_MISSING
        if len(rows) > 1 and rows[0]["refreshed_at"] == rows[1]["refreshed_at"]:
            return DerivedPitEvidenceStatus.BLOCKED, REASON_DERIVED_PIT_UPSTREAM_AUDIT_CONFLICT
        row = rows[0]
        audit = AuditRowSnapshot(
            dataset_name=str(row["dataset"]),
            trade_date=row["trade_date"],
            data_source=str(row["data_source"]),
            job_id=str(row["job_id"]) if row.get("job_id") is not None else None,
            status=str(row["status"]),
            row_count=int(row["row_count"] or 0),
            refreshed_at=row["refreshed_at"],
            error_message=row.get("error_message"),
            metadata=dict(row.get("metadata") or {}),
            data_max_at=row.get("data_max_at"),
            written_rows=row.get("written_rows"),
            expected_rows=row.get("expected_rows"),
            coverage_ratio=row.get("coverage_ratio"),
            quality_status=str(row.get("quality_status") or "unknown"),
            failure_category=row.get("failure_category"),
        )
        if audit.status != "success" or audit.quality_status not in eligible_quality_statuses:
            return DerivedPitEvidenceStatus.BLOCKED, REASON_DERIVED_PIT_UPSTREAM_AUDIT_CONFLICT
        return audit

    @staticmethod
    def _build_ready_result(
        *,
        request: DerivedPitEvidenceRequest,
        state: Mapping[str, Any],
        spans: tuple[Mapping[str, Any], ...],
        audits: Mapping[str, AuditRowSnapshot],
    ) -> DerivedPitEvidenceResult:
        if not spans:
            return AdvisoryDerivedPitEvidenceProbe._result(
                request,
                DerivedPitEvidenceStatus.BLOCKED,
                (REASON_DERIVED_PIT_SPANS_CONFLICT,),
                state=state,
                audits=audits,
            )
        symbols = tuple(str(row["ts_code"]) for row in spans)
        generated_times = tuple(row.get("generated_at") for row in spans)
        if (
            len(symbols) != len(set(symbols))
            or any(row.get("rule_version") != state.get("rule_version") for row in spans)
            or any(value is None for value in generated_times)
            or max(generated_times) > state["generated_at"]
        ):
            return AdvisoryDerivedPitEvidenceProbe._result(
                request,
                DerivedPitEvidenceStatus.BLOCKED,
                (REASON_DERIVED_PIT_SPANS_CONFLICT,),
                state=state,
                spans=spans,
                audits=audits,
            )
        evidence_available_at = max(
            state["generated_at"].astimezone(timezone.utc),
            *(audit.refreshed_at for audit in audits.values()),
            *(value.astimezone(timezone.utc) for value in generated_times),
        )
        return AdvisoryDerivedPitEvidenceProbe._result(
            request,
            DerivedPitEvidenceStatus.READY,
            (),
            state=state,
            spans=spans,
            audits=audits,
            evidence_available_at=evidence_available_at,
        )

    @staticmethod
    def _result(
        request: DerivedPitEvidenceRequest,
        status: DerivedPitEvidenceStatus,
        reasons: tuple[str, ...],
        *,
        state: Mapping[str, Any] | None = None,
        spans: tuple[Mapping[str, Any], ...] = (),
        audits: Mapping[str, AuditRowSnapshot] | None = None,
        evidence_available_at: datetime | None = None,
    ) -> DerivedPitEvidenceResult:
        state_row_hash = canonical_json_sha256(canonicalize(dict(state))) if state is not None else None
        span_payload = [canonicalize(dict(row)) for row in spans]
        receipt = DerivedPitEvidenceReceipt(
            target_database_identity_hash=request.target_database_identity_hash,
            program_id=request.program_id,
            decision_trade_date=request.decision_trade_date,
            universe_key=request.universe_key,
            decision_cutoff_ts=request.decision_cutoff_ts,
            observer_config_hash=request.observer_config_hash,
            query_registry_hash=request.query_registry_hash,
            status=status,
            state_row_hash=state_row_hash,
            state_source_fingerprint_hash=(
                str(state.get("source_fingerprint_sha256")) if state and state.get("source_fingerprint_sha256") else None
            ),
            spans_content_hash=canonical_json_sha256(span_payload) if spans else None,
            spans_row_count=len(spans) if spans else None,
            upstream_audit_row_hashes={
                dataset: audit.audit_row_hash for dataset, audit in (audits or {}).items()
            },
            evidence_available_at=evidence_available_at,
            reason_codes=reasons,
        )
        return DerivedPitEvidenceResult(receipt=receipt, state_row=state, span_rows=spans)


def build_derived_pit_observation_decisions(
    *,
    evidence: DerivedPitEvidenceResult,
    terminal_events: Mapping[str, SourceAvailabilityEvent | None],
) -> tuple[ObservationDecision, ...]:
    """Convert one READY evidence closure into normal source-ledger append/unchanged decisions."""

    receipt = evidence.receipt
    if receipt.status is not DerivedPitEvidenceStatus.READY:
        raise SourceObserverError(
            REASON_DERIVED_PIT_IDENTITY_CONFLICT,
            "derived PIT availability decisions require READY evidence",
            context={"status": receipt.status.value, "reason_codes": list(receipt.reason_codes)},
        )
    if set(terminal_events) != {role for role, _dataset in _DERIVED_RELATIONS}:
        raise SourceObserverError(
            REASON_DERIVED_PIT_IDENTITY_CONFLICT,
            "derived PIT terminal event set differs from the two derived roles",
        )
    content_by_role = {
        "pit_universe": str(receipt.spans_content_hash),
        "pit_universe_build_state": str(receipt.state_row_hash),
    }
    row_count_by_role = {
        "pit_universe": int(receipt.spans_row_count or 0),
        "pit_universe_build_state": 1,
    }
    decisions: list[ObservationDecision] = []
    for source_role, dataset_name in _DERIVED_RELATIONS:
        partition_key = {
            "as_of_date": receipt.decision_trade_date.isoformat(),
            "universe_key": receipt.universe_key,
        }
        partition_content_hash = canonical_json_sha256(
            {
                "derived_evidence_hash": receipt.evidence_hash,
                "role_content_hash": content_by_role[source_role],
            }
        )
        descriptor = SourcePartitionDescriptor(
            schema_fingerprint=canonical_json_sha256(
                list(_SPAN_COLUMNS if source_role == "pit_universe" else _STATE_COLUMNS)
            ),
            row_count=row_count_by_role[source_role],
            partition_content_hash=partition_content_hash,
            canonical_bytes=0,
        )
        terminal = terminal_events[source_role]
        if terminal is not None:
            expected_chain = source_partition_chain_key(
                dataset_name=dataset_name,
                source_role=source_role,
                partition_key=partition_key,
            )
            if terminal.partition_chain_key != expected_chain:
                raise SourceObserverError(
                    REASON_DERIVED_PIT_IDENTITY_CONFLICT,
                    "derived PIT terminal event identity differs from the current request",
                    context={"source_role": source_role},
                )
            if terminal.input.partition_content_hash == partition_content_hash and terminal.input.quality_status == "PASS":
                decisions.append(
                    ObservationDecision(
                        outcome=ObservationOutcome.UNCHANGED,
                        partition_key=partition_key,
                        descriptor=descriptor,
                        terminal_event=terminal,
                    )
                )
                continue
            event_revision_no = terminal.event_revision_no + 1
            predecessor = terminal.event_content_hash
            event_type = (
                SourceAvailabilityEventType.REVALIDATED
                if terminal.event_type is SourceAvailabilityEventType.INVALIDATED
                else SourceAvailabilityEventType.CORRECTED
            )
        else:
            event_revision_no = 1
            predecessor = None
            event_type = SourceAvailabilityEventType.INGESTED
        event_request = SourceAvailabilityEventRequest(
            dataset_name=dataset_name,
            source_role=source_role,
            partition_key=partition_key,
            revision_id=f"derived_pit:{receipt.evidence_hash}",
            event_revision_no=event_revision_no,
            event_type=event_type,
            predecessor_event_hash=predecessor,
            provider_job_id=f"derived_pit:{receipt.evidence_hash}",
            refresh_job_id=f"state:{receipt.state_row_hash}",
            provider_published_at=receipt.evidence_available_at,
            schema_fingerprint=descriptor.schema_fingerprint,
            row_count=descriptor.row_count,
            partition_content_hash=descriptor.partition_content_hash,
            quality_status="PASS",
            created_by_service_principal=DERIVED_PIT_SERVICE_PRINCIPAL,
        )
        decisions.append(
            ObservationDecision(
                outcome=ObservationOutcome.EVENT_APPENDED,
                partition_key=partition_key,
                descriptor=descriptor,
                event_request=event_request,
            )
        )
    return tuple(decisions)


def _stock_universe_fingerprint_hash(payload: Mapping[str, Any]) -> str:
    """Match the authoritative StockUniversePitService fingerprint serialization."""

    encoded = json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _fingerprint_end_date(state: Mapping[str, Any]) -> date:
    fingerprint = state["source_fingerprint"]
    explicit = fingerprint.get("fingerprint_end_date")
    if explicit is not None:
        return date.fromisoformat(str(explicit))
    return state["end_date"]
