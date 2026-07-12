"""Manual historical Advisory research batch contracts and orchestration.

This module is intentionally independent of Paper Trading, simulation, QMT,
MiniQMT, broker, order, and real-time provider modules.  It can only consume
read-only, already-captured ``daily_selection_evidence_v2`` records through an
injected adapter.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Callable, Protocol
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.trading_core.errors import InvalidStateTransitionError, RuntimeConfigInvalidError, TradingCoreError


HISTORICAL_RESEARCH_DATA_SOURCE = "DB_HISTORICAL"
HISTORICAL_RESEARCH_ORIGIN = "MANUAL_HISTORICAL_RESEARCH"
HISTORICAL_RESEARCH_SCOPE = "HISTORICAL_RESEARCH_ONLY"

REASON_HISTORICAL_DATE_REQUIRED = "ADVISORY_PHASE0A2D_HISTORICAL_DATE_REQUIRED"
REASON_HISTORICAL_DATA_REQUIRED = "ADVISORY_PHASE0A2D_HISTORICAL_DATA_REQUIRED"
REASON_MANUAL_ORIGIN_REQUIRED = "ADVISORY_PHASE0A2D_MANUAL_ORIGIN_REQUIRED"
REASON_RESEARCH_RUN_CONFLICT = "ADVISORY_PHASE0A2D_RESEARCH_RUN_CONFLICT"
REASON_PROGRAM_INPUT_UNAVAILABLE = "ADVISORY_PHASE0A2D_PROGRAM_INPUT_UNAVAILABLE"
REASON_PROGRAM_EVIDENCE_INVALID = "ADVISORY_PHASE0A2D_PROGRAM_EVIDENCE_INVALID"
REASON_FORBIDDEN_EXECUTION_DEPENDENCY = "ADVISORY_PHASE0A2D_FORBIDDEN_EXECUTION_DEPENDENCY"


class HistoricalResearchRunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    WAITING_INPUT = "WAITING_INPUT"
    FAILED = "FAILED"


class HistoricalResearchBatchRequest(BaseModel):
    """Strict manual request for a completed historical trading date."""

    model_config = ConfigDict(extra="forbid")

    request_id: UUID = Field(default_factory=uuid4)
    decision_trade_date: date
    program_ids: list[str] = Field(min_length=1)
    data_source: str = HISTORICAL_RESEARCH_DATA_SOURCE
    origin: str = HISTORICAL_RESEARCH_ORIGIN
    requested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    research_scope: str = HISTORICAL_RESEARCH_SCOPE
    execution_prohibited: bool = True
    request_payload_sha256: str | None = None

    @field_validator("program_ids")
    @classmethod
    def _program_ids_are_unique_and_nonblank(cls, values: list[str]) -> list[str]:
        normalized = [str(value or "").strip() for value in values]
        if not all(normalized):
            raise ValueError("program_ids cannot contain blank values")
        if len(set(normalized)) != len(normalized):
            raise ValueError("program_ids must be unique")
        return sorted(normalized)

    @field_validator("requested_at")
    @classmethod
    def _requested_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("requested_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def _historical_only_contract(self) -> "HistoricalResearchBatchRequest":
        if self.data_source != HISTORICAL_RESEARCH_DATA_SOURCE:
            raise _request_error(
                REASON_HISTORICAL_DATA_REQUIRED,
                "historical research requires data_source=DB_HISTORICAL",
                data_source=self.data_source,
            )
        if self.origin != HISTORICAL_RESEARCH_ORIGIN:
            raise _request_error(
                REASON_MANUAL_ORIGIN_REQUIRED,
                "historical research requires manual origin",
                origin=self.origin,
            )
        if self.research_scope != HISTORICAL_RESEARCH_SCOPE or self.execution_prohibited is not True:
            raise _request_error(
                REASON_HISTORICAL_DATA_REQUIRED,
                "historical research scope must prohibit execution",
                research_scope=self.research_scope,
                execution_prohibited=self.execution_prohibited,
            )
        expected = canonical_json_sha256(self.semantic_payload())
        if self.request_payload_sha256 is not None and self.request_payload_sha256 != expected:
            raise ValueError("request_payload_sha256 does not match the historical research request")
        object.__setattr__(self, "request_payload_sha256", expected)
        return self

    def semantic_payload(self) -> dict[str, Any]:
        """Exclude request id/time so equivalent manual retries share one batch."""

        return {
            "decision_trade_date": self.decision_trade_date,
            "program_ids": self.program_ids,
            "data_source": self.data_source,
            "origin": self.origin,
            "research_scope": self.research_scope,
            "execution_prohibited": self.execution_prohibited,
        }

    @property
    def batch_key(self) -> str:
        return canonical_json_sha256(self.semantic_payload())


class HistoricalResearchCandidate(BaseModel):
    """Research-only candidate projection; execution and price fields are absent."""

    model_config = ConfigDict(extra="forbid")

    symbol: str
    rank: int = Field(gt=0)
    score: float
    stock_name: str | None = None
    component_scores: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def _symbol_required(cls, value: str) -> str:
        normalized = str(value or "").strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        return normalized


@dataclass(frozen=True)
class HistoricalResearchProgramContext:
    program_id: str
    binding_version_id: str
    binding_payload_hash: str
    package_id: str
    manifest_sha256: str
    policy_hash: str
    effective_runtime_config_hash: str


@dataclass(frozen=True)
class HistoricalSelectionEvidence:
    evidence_id: str
    evidence_hash: str
    artifact_id: str
    artifact_payload_hash: str
    source_watermark_hash: str
    candidate_outcome: str
    candidates: list[HistoricalResearchCandidate]


@dataclass(frozen=True)
class HistoricalResearchProgramRun:
    program_run_id: str
    program_id: str
    decision_trade_date: date
    research_scope: str
    status: HistoricalResearchRunStatus
    program_payload_sha256: str | None = None
    binding_version_id: str | None = None
    binding_payload_hash: str | None = None
    package_id: str | None = None
    manifest_sha256: str | None = None
    policy_hash: str | None = None
    effective_runtime_config_hash: str | None = None
    source_watermark_hash: str | None = None
    evidence_id: str | None = None
    evidence_hash: str | None = None
    artifact_id: str | None = None
    artifact_payload_hash: str | None = None
    research_list_version_id: str | None = None
    research_candidates: list[HistoricalResearchCandidate] = field(default_factory=list)
    candidate_outcome: str | None = None
    reason_codes: list[str] = field(default_factory=list)
    error_json: dict[str, Any] | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class HistoricalResearchBatch:
    batch_id: str
    request_id: UUID
    batch_key: str
    decision_trade_date: date
    program_ids: list[str]
    data_source: str
    origin: str
    request_payload_sha256: str
    research_scope: str
    execution_prohibited: bool
    status: HistoricalResearchRunStatus = HistoricalResearchRunStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class HistoricalResearchBatchReceipt:
    receipt_id: str
    batch_id: str
    batch_key: str
    status: HistoricalResearchRunStatus
    program_runs: list[HistoricalResearchProgramRun]
    receipt_hash: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class HistoricalResearchInputUnavailable(RuntimeError):
    """A retriable missing historical input, never a fabricated empty result."""

    def __init__(self, detail: str, *, reason_code: str = REASON_PROGRAM_INPUT_UNAVAILABLE, context: dict[str, Any] | None = None) -> None:
        super().__init__(detail)
        self.reason_code = reason_code
        self.context = context or {}


class HistoricalResearchTradingDateResolver(Protocol):
    def require_completed_historical_trading_date(self, *, decision_trade_date: date, requested_at: datetime) -> None: ...


class HistoricalResearchProgramResolver(Protocol):
    def resolve(self, *, program_id: str, decision_trade_date: date, cursor: Any | None = None) -> HistoricalResearchProgramContext: ...


class HistoricalSelectionEvidenceAdapter(Protocol):
    def load(
        self,
        *,
        context: HistoricalResearchProgramContext,
        decision_trade_date: date,
        cursor: Any | None = None,
    ) -> HistoricalSelectionEvidence: ...


@dataclass(frozen=True)
class _ProgramOutcome:
    status: HistoricalResearchRunStatus
    context: HistoricalResearchProgramContext | None = None
    evidence: HistoricalSelectionEvidence | None = None
    program_payload_sha256: str | None = None
    reason_codes: list[str] = field(default_factory=list)
    error_json: dict[str, Any] | None = None


class HistoricalResearchRepository(Protocol):
    def get_or_create_batch(self, request: HistoricalResearchBatchRequest) -> HistoricalResearchBatch: ...

    def execute_program(
        self,
        *,
        batch: HistoricalResearchBatch,
        program_id: str,
        worker: Callable[[Any | None], _ProgramOutcome],
    ) -> HistoricalResearchProgramRun: ...

    def save_batch_receipt(
        self,
        *,
        batch: HistoricalResearchBatch,
        program_runs: list[HistoricalResearchProgramRun],
    ) -> HistoricalResearchBatchReceipt: ...

    def get_batch(self, batch_id: str) -> HistoricalResearchBatch: ...

    def get_batch_receipt(self, batch_id: str) -> HistoricalResearchBatchReceipt | None: ...

    def get_program_run(self, *, program_id: str, decision_trade_date: date) -> HistoricalResearchProgramRun | None: ...


class InMemoryHistoricalResearchRepository:
    """Deterministic repository used by runner contract tests."""

    def __init__(self) -> None:
        self.batches_by_key: dict[str, HistoricalResearchBatch] = {}
        self.batches_by_id: dict[str, HistoricalResearchBatch] = {}
        self.program_runs: dict[tuple[str, date, str], HistoricalResearchProgramRun] = {}
        self.receipts_by_batch_id: dict[str, HistoricalResearchBatchReceipt] = {}

    def get_or_create_batch(self, request: HistoricalResearchBatchRequest) -> HistoricalResearchBatch:
        existing = self.batches_by_key.get(request.batch_key)
        if existing is not None:
            return existing
        now = datetime.now(UTC)
        batch = HistoricalResearchBatch(
            batch_id=f"arb_{request.batch_key[:16]}",
            request_id=request.request_id,
            batch_key=request.batch_key,
            decision_trade_date=request.decision_trade_date,
            program_ids=list(request.program_ids),
            data_source=request.data_source,
            origin=request.origin,
            request_payload_sha256=str(request.request_payload_sha256),
            research_scope=request.research_scope,
            execution_prohibited=request.execution_prohibited,
            created_at=now,
            updated_at=now,
        )
        self.batches_by_key[batch.batch_key] = batch
        self.batches_by_id[batch.batch_id] = batch
        return batch

    def execute_program(
        self,
        *,
        batch: HistoricalResearchBatch,
        program_id: str,
        worker: Callable[[Any | None], _ProgramOutcome],
    ) -> HistoricalResearchProgramRun:
        outcome = worker(None)
        key = (program_id, batch.decision_trade_date, batch.research_scope)
        existing = self.program_runs.get(key)
        if existing is not None:
            self._assert_compatible(existing=existing, outcome=outcome)
            if existing.status in {HistoricalResearchRunStatus.COMPLETE, HistoricalResearchRunStatus.FAILED}:
                return existing
        run = _program_run_from_outcome(batch=batch, program_id=program_id, outcome=outcome, existing=existing)
        self.program_runs[key] = run
        return run

    def save_batch_receipt(
        self,
        *,
        batch: HistoricalResearchBatch,
        program_runs: list[HistoricalResearchProgramRun],
    ) -> HistoricalResearchBatchReceipt:
        status = _aggregate_status(program_runs)
        payload = _batch_receipt_payload(batch=batch, status=status, program_runs=program_runs)
        receipt = HistoricalResearchBatchReceipt(
            receipt_id=f"arr_{canonical_json_sha256(payload)[:16]}",
            batch_id=batch.batch_id,
            batch_key=batch.batch_key,
            status=status,
            program_runs=list(program_runs),
            receipt_hash=canonical_json_sha256(payload),
        )
        existing = self.receipts_by_batch_id.get(batch.batch_id)
        if existing is not None and existing.status not in {
            HistoricalResearchRunStatus.PENDING,
            HistoricalResearchRunStatus.RUNNING,
            HistoricalResearchRunStatus.WAITING_INPUT,
        }:
            if existing.receipt_hash != receipt.receipt_hash:
                raise _conflict_error(batch=batch, program_id=None)
            return existing
        self.receipts_by_batch_id[batch.batch_id] = receipt
        updated_batch = replace(batch, status=status, updated_at=datetime.now(UTC))
        self.batches_by_key[batch.batch_key] = updated_batch
        self.batches_by_id[batch.batch_id] = updated_batch
        return receipt

    def get_batch(self, batch_id: str) -> HistoricalResearchBatch:
        try:
            return self.batches_by_id[batch_id]
        except KeyError as exc:
            raise RuntimeConfigInvalidError("historical research batch does not exist", context={"batch_id": batch_id}) from exc

    def get_batch_receipt(self, batch_id: str) -> HistoricalResearchBatchReceipt | None:
        return self.receipts_by_batch_id.get(batch_id)

    def get_program_run(self, *, program_id: str, decision_trade_date: date) -> HistoricalResearchProgramRun | None:
        return self.program_runs.get((program_id, decision_trade_date, HISTORICAL_RESEARCH_SCOPE))

    @staticmethod
    def _assert_compatible(*, existing: HistoricalResearchProgramRun, outcome: _ProgramOutcome) -> None:
        if (
            existing.program_payload_sha256 is not None
            and outcome.program_payload_sha256 is not None
            and existing.program_payload_sha256 != outcome.program_payload_sha256
        ):
            raise _conflict_error(batch=None, program_id=existing.program_id)


class HistoricalAdvisoryResearchRunner:
    """Run independent manual historical research jobs for multiple Programs."""

    def __init__(
        self,
        *,
        repository: HistoricalResearchRepository,
        trading_date_resolver: HistoricalResearchTradingDateResolver,
        program_resolver: HistoricalResearchProgramResolver,
        evidence_adapter: HistoricalSelectionEvidenceAdapter,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = repository
        self._trading_date_resolver = trading_date_resolver
        self._program_resolver = program_resolver
        self._evidence_adapter = evidence_adapter
        self._now_provider = now_provider or (lambda: datetime.now(UTC))

    def run(self, request: HistoricalResearchBatchRequest) -> HistoricalResearchBatchReceipt:
        server_requested_at = self._now_provider()
        if server_requested_at.tzinfo is None:
            raise RuntimeConfigInvalidError("historical research server clock must be timezone-aware")
        request = request.model_copy(update={"requested_at": server_requested_at})
        self._trading_date_resolver.require_completed_historical_trading_date(
            decision_trade_date=request.decision_trade_date,
            requested_at=request.requested_at,
        )
        batch = self._repository.get_or_create_batch(request)
        program_runs = [
            self._repository.execute_program(
                batch=batch,
                program_id=program_id,
                worker=lambda cursor, program_id=program_id: self._run_one(
                    program_id=program_id,
                    decision_trade_date=request.decision_trade_date,
                    cursor=cursor,
                ),
            )
            for program_id in batch.program_ids
        ]
        return self._repository.save_batch_receipt(batch=batch, program_runs=program_runs)

    def get_batch(self, batch_id: str) -> HistoricalResearchBatch:
        return self._repository.get_batch(batch_id)

    def get_program_run(self, *, program_id: str, decision_trade_date: date) -> HistoricalResearchProgramRun | None:
        return self._repository.get_program_run(program_id=program_id, decision_trade_date=decision_trade_date)

    def get_batch_receipt(self, batch_id: str) -> HistoricalResearchBatchReceipt | None:
        return self._repository.get_batch_receipt(batch_id)

    def _run_one(self, *, program_id: str, decision_trade_date: date, cursor: Any | None) -> _ProgramOutcome:
        context: HistoricalResearchProgramContext | None = None
        try:
            context = self._program_resolver.resolve(
                program_id=program_id,
                decision_trade_date=decision_trade_date,
                cursor=cursor,
            )
            evidence = self._evidence_adapter.load(
                context=context,
                decision_trade_date=decision_trade_date,
                cursor=cursor,
            )
            _validate_evidence(evidence)
            return _ProgramOutcome(
                status=HistoricalResearchRunStatus.COMPLETE,
                context=context,
                evidence=evidence,
                program_payload_sha256=_program_payload_hash(context=context, evidence=evidence),
            )
        except HistoricalResearchInputUnavailable as exc:
            return _ProgramOutcome(
                status=HistoricalResearchRunStatus.WAITING_INPUT,
                context=context,
                reason_codes=[exc.reason_code],
                error_json={"reason_code": exc.reason_code, "message": str(exc), "context": exc.context},
            )
        except TradingCoreError as exc:
            detail = exc.to_dict()
            error_context = detail.get("context") if isinstance(detail.get("context"), dict) else {}
            return _ProgramOutcome(
                status=HistoricalResearchRunStatus.FAILED,
                context=context,
                reason_codes=[
                    str(
                        error_context.get("reason_code")
                        or detail.get("reason_code")
                        or detail.get("error_code")
                        or "ADVISORY_PHASE0A2D_PROGRAM_FAILED"
                    )
                ],
                error_json=detail,
            )
        except Exception as exc:  # noqa: BLE001
            return _ProgramOutcome(
                status=HistoricalResearchRunStatus.FAILED,
                context=context,
                reason_codes=["ADVISORY_PHASE0A2D_PROGRAM_FAILED"],
                error_json={"reason_code": "ADVISORY_PHASE0A2D_PROGRAM_FAILED", "message": f"{type(exc).__name__}: {exc}"},
            )


def _program_payload_hash(*, context: HistoricalResearchProgramContext, evidence: HistoricalSelectionEvidence) -> str:
    return canonical_json_sha256(
        {
            "program_id": context.program_id,
            "binding_version_id": context.binding_version_id,
            "binding_payload_hash": context.binding_payload_hash,
            "manifest_sha256": context.manifest_sha256,
            "policy_hash": context.policy_hash,
            "effective_runtime_config_hash": context.effective_runtime_config_hash,
            "source_watermark_hash": evidence.source_watermark_hash,
            "data_source": HISTORICAL_RESEARCH_DATA_SOURCE,
            "research_scope": HISTORICAL_RESEARCH_SCOPE,
        }
    )


def _program_run_from_outcome(
    *,
    batch: HistoricalResearchBatch,
    program_id: str,
    outcome: _ProgramOutcome,
    existing: HistoricalResearchProgramRun | None,
) -> HistoricalResearchProgramRun:
    now = datetime.now(UTC)
    context = outcome.context
    evidence = outcome.evidence
    payload_hash = outcome.program_payload_sha256
    list_version_id = None
    candidates: list[HistoricalResearchCandidate] = []
    candidate_outcome = None
    if evidence is not None:
        candidates = list(evidence.candidates)
        candidate_outcome = evidence.candidate_outcome
        list_version_id = f"arlv_{canonical_json_sha256({'program_payload_sha256': payload_hash, 'evidence_hash': evidence.evidence_hash, 'candidates': candidates})[:16]}"
    return HistoricalResearchProgramRun(
        program_run_id=(existing.program_run_id if existing else f"arpr_{canonical_json_sha256({'program_id': program_id, 'decision_trade_date': batch.decision_trade_date, 'research_scope': batch.research_scope})[:16]}"),
        program_id=program_id,
        decision_trade_date=batch.decision_trade_date,
        research_scope=batch.research_scope,
        status=outcome.status,
        program_payload_sha256=payload_hash,
        binding_version_id=context.binding_version_id if context else None,
        binding_payload_hash=context.binding_payload_hash if context else None,
        package_id=context.package_id if context else None,
        manifest_sha256=context.manifest_sha256 if context else None,
        policy_hash=context.policy_hash if context else None,
        effective_runtime_config_hash=context.effective_runtime_config_hash if context else None,
        source_watermark_hash=evidence.source_watermark_hash if evidence else None,
        evidence_id=evidence.evidence_id if evidence else None,
        evidence_hash=evidence.evidence_hash if evidence else None,
        artifact_id=evidence.artifact_id if evidence else None,
        artifact_payload_hash=evidence.artifact_payload_hash if evidence else None,
        research_list_version_id=list_version_id,
        research_candidates=candidates,
        candidate_outcome=candidate_outcome,
        reason_codes=sorted(set(outcome.reason_codes)),
        error_json=outcome.error_json,
        created_at=existing.created_at if existing else now,
        updated_at=now,
    )


def _validate_evidence(evidence: HistoricalSelectionEvidence) -> None:
    if evidence.candidate_outcome not in {"CANDIDATES_PRESENT", "VALID_NO_CANDIDATE"}:
        raise RuntimeConfigInvalidError(
            "historical selection evidence has invalid candidate outcome",
            context={"candidate_outcome": evidence.candidate_outcome},
        )
    if evidence.candidate_outcome == "CANDIDATES_PRESENT" and not evidence.candidates:
        raise RuntimeConfigInvalidError("historical selection evidence cannot claim candidates without rows")
    if evidence.candidate_outcome == "VALID_NO_CANDIDATE" and evidence.candidates:
        raise RuntimeConfigInvalidError("valid no-candidate evidence cannot contain research candidates")


def _aggregate_status(program_runs: list[HistoricalResearchProgramRun]) -> HistoricalResearchRunStatus:
    statuses = {run.status for run in program_runs}
    if HistoricalResearchRunStatus.FAILED in statuses:
        return HistoricalResearchRunStatus.FAILED
    if HistoricalResearchRunStatus.WAITING_INPUT in statuses:
        return HistoricalResearchRunStatus.WAITING_INPUT
    if HistoricalResearchRunStatus.RUNNING in statuses:
        return HistoricalResearchRunStatus.RUNNING
    if HistoricalResearchRunStatus.PENDING in statuses:
        return HistoricalResearchRunStatus.PENDING
    return HistoricalResearchRunStatus.COMPLETE


def _batch_receipt_payload(
    *,
    batch: HistoricalResearchBatch,
    status: HistoricalResearchRunStatus,
    program_runs: list[HistoricalResearchProgramRun],
) -> dict[str, Any]:
    return {
        "batch_id": batch.batch_id,
        "batch_key": batch.batch_key,
        "decision_trade_date": batch.decision_trade_date,
        "data_source": batch.data_source,
        "origin": batch.origin,
        "research_scope": batch.research_scope,
        "execution_prohibited": batch.execution_prohibited,
        "status": status.value,
        "program_runs": [
            {
                "program_id": run.program_id,
                "program_run_id": run.program_run_id,
                "status": run.status.value,
                "program_payload_sha256": run.program_payload_sha256,
                "evidence_id": run.evidence_id,
                "evidence_hash": run.evidence_hash,
                "research_list_version_id": run.research_list_version_id,
                "candidate_outcome": run.candidate_outcome,
                "candidate_count": len(run.research_candidates),
                "reason_codes": run.reason_codes,
            }
            for run in sorted(program_runs, key=lambda item: item.program_id)
        ],
    }


def _request_error(reason_code: str, message: str, **context: Any) -> PydanticCustomError:
    return PydanticCustomError(reason_code, message, context)


def _conflict_error(*, batch: HistoricalResearchBatch | None, program_id: str | None) -> InvalidStateTransitionError:
    return InvalidStateTransitionError(
        "historical research program run conflicts with immutable business identity",
        context={
            "reason_code": REASON_RESEARCH_RUN_CONFLICT,
            "batch_id": batch.batch_id if batch else None,
            "program_id": program_id,
        },
    )


def historical_research_batch_to_dict(batch: HistoricalResearchBatch) -> dict[str, Any]:
    return {
        "batch_id": batch.batch_id,
        "request_id": str(batch.request_id),
        "batch_key": batch.batch_key,
        "decision_trade_date": batch.decision_trade_date.isoformat(),
        "program_ids": list(batch.program_ids),
        "data_source": batch.data_source,
        "origin": batch.origin,
        "research_scope": batch.research_scope,
        "execution_prohibited": batch.execution_prohibited,
        "status": batch.status.value,
        "created_at": batch.created_at.isoformat(),
        "updated_at": batch.updated_at.isoformat(),
    }


def historical_research_program_run_to_dict(run: HistoricalResearchProgramRun) -> dict[str, Any]:
    return {
        "program_run_id": run.program_run_id,
        "program_id": run.program_id,
        "decision_trade_date": run.decision_trade_date.isoformat(),
        "research_scope": run.research_scope,
        "execution_prohibited": True,
        "status": run.status.value,
        "program_payload_sha256": run.program_payload_sha256,
        "binding_version_id": run.binding_version_id,
        "binding_payload_hash": run.binding_payload_hash,
        "package_id": run.package_id,
        "manifest_sha256": run.manifest_sha256,
        "policy_hash": run.policy_hash,
        "effective_runtime_config_hash": run.effective_runtime_config_hash,
        "source_watermark_hash": run.source_watermark_hash,
        "evidence_id": run.evidence_id,
        "evidence_hash": run.evidence_hash,
        "artifact_id": run.artifact_id,
        "artifact_payload_hash": run.artifact_payload_hash,
        "research_list_version_id": run.research_list_version_id,
        "candidate_outcome": run.candidate_outcome,
        "research_candidates": [item.model_dump(mode="json") for item in run.research_candidates],
        "reason_codes": list(run.reason_codes),
        "error": run.error_json,
        "created_at": run.created_at.isoformat(),
        "updated_at": run.updated_at.isoformat(),
    }


def historical_research_receipt_to_dict(receipt: HistoricalResearchBatchReceipt) -> dict[str, Any]:
    return {
        "receipt_id": receipt.receipt_id,
        "batch_id": receipt.batch_id,
        "batch_key": receipt.batch_key,
        "status": receipt.status.value,
        "research_scope": HISTORICAL_RESEARCH_SCOPE,
        "execution_prohibited": True,
        "program_runs": [historical_research_program_run_to_dict(run) for run in receipt.program_runs],
        "receipt_hash": receipt.receipt_hash,
        "created_at": receipt.created_at.isoformat(),
    }
