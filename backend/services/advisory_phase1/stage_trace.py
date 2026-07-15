"""Bounded, immutable Phase 1 stage-trace capture contracts.

This module is intentionally a pure in-memory boundary.  It receives already
computed Selection stage receipts, copies their payloads, and either creates a
canonical envelope or an explicit capture receipt.  It never recomputes a
score, changes a candidate, writes a database row, or calls a market/runtime
service.  Persistence is delegated to :mod:`trace_outbox` after business work
has completed.
"""

from __future__ import annotations

from copy import deepcopy
from concurrent.futures import Executor, ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import Enum
import json
from threading import BoundedSemaphore
from typing import Any, Mapping, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PHASE1_STAGE_TRACE_SCHEMA_VERSION = "advisory_phase1_stage_trace_v1"
MULTI_ALPHA_COMPONENT_EVIDENCE_SCHEMA_VERSION = "multi_alpha_component_evidence_v1"
TRACE_CAPTURE_BINDING_SCHEMA_VERSION = "advisory_phase1_trace_capture_binding_v1"

REASON_TRACE_CONTEXT_INVALID = "ADVISORY_PHASE1_TRACE_CONTEXT_INVALID"
REASON_TRACE_CAPTURE_FAILED = "ADVISORY_PHASE1_TRACE_CAPTURE_FAILED"
REASON_TRACE_CAPTURE_TIMEOUT = "ADVISORY_PHASE1_TRACE_CAPTURE_TIMEOUT"
REASON_TRACE_CANDIDATE_LIMIT_EXCEEDED = "ADVISORY_PHASE1_TRACE_CANDIDATE_LIMIT_EXCEEDED"
REASON_TRACE_BYTE_LIMIT_EXCEEDED = "ADVISORY_PHASE1_TRACE_BYTE_LIMIT_EXCEEDED"
REASON_TRACE_OUTBOX_WRITER_UNCONFIGURED = "ADVISORY_PHASE1_TRACE_OUTBOX_WRITER_UNCONFIGURED"
REASON_TRACE_OUTBOX_WRITE_FAILED = "ADVISORY_PHASE1_TRACE_OUTBOX_WRITE_FAILED"
REASON_TRACE_OUTBOX_BLOCKING_WRITER = "ADVISORY_PHASE1_TRACE_OUTBOX_BLOCKING_WRITER"
REASON_COMPONENT_EVIDENCE_INCOMPLETE = "ADVISORY_PHASE1_COMPONENT_EVIDENCE_INCOMPLETE"
REASON_COMPONENT_PARENT_PARITY_INVALID = "ADVISORY_PHASE1_COMPONENT_PARENT_PARITY_INVALID"
REASON_COMPONENT_WEIGHT_MISMATCH = "ADVISORY_PHASE1_COMPONENT_WEIGHT_MISMATCH"
REASON_COMPONENT_LEG_MISSING = "ADVISORY_PHASE1_COMPONENT_LEG_MISSING"
REASON_COMPONENT_RANK_MISSING = "ADVISORY_PHASE1_COMPONENT_RANK_MISSING"
REASON_COMPONENT_RUNTIME_VARIANT_INVALID = "ADVISORY_PHASE1_COMPONENT_RUNTIME_VARIANT_INVALID"


class TraceCaptureState(str, Enum):
    DISABLED = "DISABLED"
    ENVELOPE_READY = "ENVELOPE_READY"
    PARTIAL = "PARTIAL"
    CAPTURE_FAILED = "CAPTURE_FAILED"
    OUTBOX_QUEUED = "OUTBOX_QUEUED"
    OUTBOX_WRITTEN = "OUTBOX_WRITTEN"
    OUTBOX_WRITE_FAILED = "OUTBOX_WRITE_FAILED"


class ComponentCapability(str, Enum):
    FULL = "FULL"
    PARTIAL = "PARTIAL"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class TraceCapturePolicy(BaseModel):
    """Frozen per-binding bounds for a single in-memory capture."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    policy_id: str = Field(min_length=1, max_length=160)
    policy_version: str = Field(min_length=1, max_length=80)
    max_candidates: int = Field(ge=1, le=100_000)
    max_bytes: int = Field(ge=1_024, le=64 * 1024 * 1024)
    max_capture_ms: int = Field(ge=1, le=60_000)
    policy_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("policy_hash")
    @classmethod
    def _sha256(cls, value: str | None) -> str | None:
        if value is not None and (len(value) != 64 or any(char not in "0123456789abcdef" for char in value)):
            raise ValueError("policy_hash must be lowercase sha256 hex")
        return value

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "max_candidates": self.max_candidates,
            "max_bytes": self.max_bytes,
            "max_capture_ms": self.max_capture_ms,
        }

    @model_validator(mode="after")
    def _validate_hash(self) -> "TraceCapturePolicy":
        digest = _canonical_json_sha256(self.canonical_payload())
        if self.policy_hash is not None and self.policy_hash != digest:
            raise ValueError("policy_hash does not match capture policy")
        object.__setattr__(self, "policy_hash", digest)
        return self


class TraceCaptureBinding(BaseModel):
    """Explicit, versioned TRACE_CAPTURE binding; it is not an approval record."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = TRACE_CAPTURE_BINDING_SCHEMA_VERSION
    control_type: str = "TRACE_CAPTURE"
    control_binding_event_hash: str = Field(min_length=64, max_length=64)
    binding_id: str = Field(min_length=1, max_length=160)
    binding_version: str = Field(min_length=1, max_length=80)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    capture_batch_id: str = Field(min_length=1, max_length=160)
    capture_fencing_token: int = Field(ge=1)
    capture_policy: TraceCapturePolicy
    binding_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("control_binding_event_hash", "handoff_readiness_hash", "admission_scope_hash", "binding_hash")
    @classmethod
    def _sha256(cls, value: str | None) -> str | None:
        if value is not None and (len(value) != 64 or any(char not in "0123456789abcdef" for char in value)):
            raise ValueError("binding hashes must be lowercase sha256 hex")
        return value

    @model_validator(mode="after")
    def _validate_binding(self) -> "TraceCaptureBinding":
        if self.schema_version != TRACE_CAPTURE_BINDING_SCHEMA_VERSION or self.control_type != "TRACE_CAPTURE":
            raise ValueError("trace binding schema/control type is invalid")
        # The event hash is a provenance pointer created from this binding hash;
        # excluding it avoids a circular identity while preserving the pointer
        # in the immutable envelope and persisted outbox row.
        payload = self.model_dump(mode="json", exclude={"binding_hash", "control_binding_event_hash"})
        digest = _canonical_json_sha256(payload)
        if self.binding_hash is not None and self.binding_hash != digest:
            raise ValueError("binding_hash does not match trace capture binding")
        object.__setattr__(self, "binding_hash", digest)
        return self


class TraceCaptureContext(BaseModel):
    """Selection identity required before a Phase 1 envelope can exist."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    selection_run_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    decision_as_of_trade_date: date
    data_source: str = Field(min_length=1, max_length=80)
    execution_origin: str = Field(min_length=1, max_length=80)
    research_scope: str = Field(min_length=1, max_length=80)
    execution_prohibited: bool
    binding: TraceCaptureBinding

    @field_validator("manifest_sha256")
    @classmethod
    def _manifest_sha(cls, value: str) -> str:
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("manifest_sha256 must be lowercase sha256 hex")
        return value

    @model_validator(mode="after")
    def _historical_boundary(self) -> "TraceCaptureContext":
        if self.data_source != "DB_HISTORICAL":
            raise ValueError("Phase 1 trace capture requires DB_HISTORICAL data")
        if self.execution_origin != "ADVISORY_RUN" or self.research_scope != "HISTORICAL_RESEARCH_ONLY":
            raise ValueError("Phase 1 trace capture is restricted to historical ADVISORY_RUN research")
        if self.execution_prohibited is not True:
            raise ValueError("Phase 1 trace capture requires execution_prohibited=true")
        return self


class ComponentEvidenceResult(BaseModel):
    """A candidate-level component provenance result that never changes parent authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    capability: ComponentCapability
    schema_version: str | None = None
    component_evidence: dict[str, Any] | None = None
    component_evidence_hash: str | None = None
    reason_codes: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _validate_shape(self) -> "ComponentEvidenceResult":
        if self.capability is ComponentCapability.FULL:
            if (
                self.schema_version != MULTI_ALPHA_COMPONENT_EVIDENCE_SCHEMA_VERSION
                or self.component_evidence is None
                or self.component_evidence_hash is None
                or self.reason_codes
            ):
                raise ValueError("FULL component evidence requires a complete immutable payload")
            if _canonical_json_sha256(self.component_evidence) != self.component_evidence_hash:
                raise ValueError("component_evidence_hash does not match payload")
        elif self.component_evidence is not None or self.component_evidence_hash is not None:
            raise ValueError("non-FULL component evidence must not carry a fabricated payload")
        return self


class StageTraceEnvelope(BaseModel):
    """Canonical immutable trace content ready for append-only outbox persistence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    trace_outbox_id: str
    trace_content_hash: str
    trace_content: dict[str, Any]
    candidate_count: int = Field(ge=0)
    size_bytes: int = Field(ge=0)

    @model_validator(mode="after")
    def _validate_content(self) -> "StageTraceEnvelope":
        if _canonical_json_sha256(self.trace_content) != self.trace_content_hash:
            raise ValueError("trace_content_hash does not match canonical trace content")
        if self.trace_outbox_id != f"sto_{self.trace_content_hash[:20]}":
            raise ValueError("trace_outbox_id does not match trace content hash")
        actual_size = _canonical_json_size(self.trace_content)
        if self.size_bytes != actual_size:
            raise ValueError("size_bytes does not match canonical trace content")
        return self


class TraceCaptureResult(BaseModel):
    """Visible capture/outbox outcome; it is intentionally separate from Selection output."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    package_id: str
    state: TraceCaptureState
    envelope: StageTraceEnvelope | None = None
    outbox_id: str | None = None
    reason_codes: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _result_coherent(self) -> "TraceCaptureResult":
        if self.state in {
            TraceCaptureState.ENVELOPE_READY,
            TraceCaptureState.OUTBOX_QUEUED,
            TraceCaptureState.OUTBOX_WRITTEN,
        } and self.envelope is None:
            raise ValueError("successful trace state requires an envelope")
        if self.state is TraceCaptureState.OUTBOX_WRITTEN and not self.outbox_id:
            raise ValueError("OUTBOX_WRITTEN requires outbox_id")
        if self.state in {TraceCaptureState.DISABLED, TraceCaptureState.PARTIAL, TraceCaptureState.CAPTURE_FAILED} and self.outbox_id:
            raise ValueError("non-outbox trace state cannot expose an outbox_id")
        return self


class Phase1TraceCaptureReceipt(BaseModel):
    """Per-request visibility without mutating the existing Selection payload."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = PHASE1_STAGE_TRACE_SCHEMA_VERSION
    requested: bool
    results_by_package: dict[str, TraceCaptureResult] = Field(default_factory=dict)
    reason_codes: tuple[str, ...] = ()

    @classmethod
    def disabled(cls) -> "Phase1TraceCaptureReceipt":
        return cls(requested=False)


class StageTraceSink(Protocol):
    """Pure in-memory sink; implementations must not perform I/O or business DML."""

    def capture(
        self,
        *,
        context: TraceCaptureContext,
        manifest: Any,
        artifact: Any,
        stage_trace: Any,
        runtime_config: Mapping[str, Any],
    ) -> TraceCaptureResult: ...


class TraceOutboxWriter(Protocol):
    """Non-blocking dispatch boundary used after business selection completed."""

    non_blocking: bool

    def append(self, envelope: StageTraceEnvelope, *, binding: TraceCaptureBinding) -> Any: ...


class NullSelectionStageTraceSink:
    """Default sink: explicit no-op with no allocation beyond its receipt."""

    def capture(
        self,
        *,
        context: TraceCaptureContext,
        manifest: Any,
        artifact: Any,
        stage_trace: Any,
        runtime_config: Mapping[str, Any],
    ) -> TraceCaptureResult:
        return TraceCaptureResult(package_id=context.package_id, state=TraceCaptureState.DISABLED)


class BoundedSelectionStageTraceSink:
    """Build a copied, bounded envelope and convert every capture error to a receipt."""

    def __init__(self, *, executor: Executor | None = None, max_inflight: int = 1) -> None:
        if max_inflight < 1:
            raise ValueError("max_inflight must be positive")
        self._executor = executor or ThreadPoolExecutor(max_workers=max_inflight, thread_name_prefix="advisory-trace")
        self._owns_executor = executor is None
        self._slots = BoundedSemaphore(max_inflight)

    def capture(
        self,
        *,
        context: TraceCaptureContext,
        manifest: Any,
        artifact: Any,
        stage_trace: Any,
        runtime_config: Mapping[str, Any],
    ) -> TraceCaptureResult:
        if not self._slots.acquire(blocking=False):
            return TraceCaptureResult(
                package_id=context.package_id,
                state=TraceCaptureState.PARTIAL,
                reason_codes=(REASON_TRACE_CAPTURE_TIMEOUT,),
            )
        try:
            future = self._executor.submit(
                self._capture_sync,
                context=context,
                manifest=manifest,
                artifact=artifact,
                stage_trace=stage_trace,
                runtime_config=runtime_config,
            )
        except Exception:
            self._slots.release()
            return TraceCaptureResult(
                package_id=context.package_id,
                state=TraceCaptureState.CAPTURE_FAILED,
                reason_codes=(REASON_TRACE_CAPTURE_FAILED,),
            )
        future.add_done_callback(lambda _future: self._slots.release())
        try:
            return future.result(timeout=context.binding.capture_policy.max_capture_ms / 1000.0)
        except FutureTimeoutError:
            return TraceCaptureResult(
                package_id=context.package_id,
                state=TraceCaptureState.PARTIAL,
                reason_codes=(REASON_TRACE_CAPTURE_TIMEOUT,),
            )
        except Exception:
            return TraceCaptureResult(
                package_id=context.package_id,
                state=TraceCaptureState.CAPTURE_FAILED,
                reason_codes=(REASON_TRACE_CAPTURE_FAILED,),
            )

    @staticmethod
    def _capture_sync(
        *,
        context: TraceCaptureContext,
        manifest: Any,
        artifact: Any,
        stage_trace: Any,
        runtime_config: Mapping[str, Any],
    ) -> TraceCaptureResult:
        try:
            preflight_reason = _capture_preflight_reason(
                artifact=artifact,
                stage_trace=stage_trace,
                policy=context.binding.capture_policy,
            )
            if preflight_reason is not None:
                return TraceCaptureResult(
                    package_id=context.package_id,
                    state=TraceCaptureState.PARTIAL,
                    reason_codes=(preflight_reason,),
                )
            envelope = build_stage_trace_envelope(
                context=context,
                manifest=manifest,
                artifact=artifact,
                stage_trace=stage_trace,
                runtime_config=runtime_config,
            )
            if envelope.size_bytes > context.binding.capture_policy.max_bytes:
                return TraceCaptureResult(
                    package_id=context.package_id,
                    state=TraceCaptureState.PARTIAL,
                    reason_codes=(REASON_TRACE_BYTE_LIMIT_EXCEEDED,),
                )
            return TraceCaptureResult(
                package_id=context.package_id,
                state=TraceCaptureState.ENVELOPE_READY,
                envelope=envelope,
            )
        except ComponentEvidenceError as exc:
            return TraceCaptureResult(
                package_id=context.package_id,
                state=TraceCaptureState.PARTIAL,
                reason_codes=(exc.reason_code,),
            )
        except Exception:
            return TraceCaptureResult(
                package_id=context.package_id,
                state=TraceCaptureState.CAPTURE_FAILED,
                reason_codes=(REASON_TRACE_CAPTURE_FAILED,),
            )

    def shutdown(self, *, wait: bool = True) -> None:
        if self._owns_executor and isinstance(self._executor, ThreadPoolExecutor):
            self._executor.shutdown(wait=wait, cancel_futures=True)


@dataclass(frozen=True)
class ComponentEvidenceError(ValueError):
    reason_code: str
    capability: ComponentCapability = ComponentCapability.PARTIAL


class Phase1TraceCaptureService:
    """Explicit opt-in bridge from Selection to a pure sink and independent outbox writer."""

    def __init__(
        self,
        *,
        binding: TraceCaptureBinding | None = None,
        sink: StageTraceSink | None = None,
        outbox_writer: TraceOutboxWriter | None = None,
    ) -> None:
        self._binding = binding
        self._sink = sink or NullSelectionStageTraceSink()
        self._outbox_writer = outbox_writer

    @property
    def enabled(self) -> bool:
        return self._binding is not None and not isinstance(self._sink, NullSelectionStageTraceSink)

    def capture_package(
        self,
        *,
        selection_run_id: str | None,
        package_id: str,
        manifest_sha256: str,
        decision_as_of_trade_date: date,
        data_source: str,
        execution_origin: str,
        research_scope: str,
        execution_prohibited: bool,
        manifest: Any,
        artifact: Any,
        stage_trace: Any,
        runtime_config: Mapping[str, Any],
    ) -> TraceCaptureResult:
        if not self.enabled:
            return TraceCaptureResult(package_id=package_id, state=TraceCaptureState.DISABLED)
        try:
            context = TraceCaptureContext(
                selection_run_id=str(selection_run_id or ""),
                package_id=package_id,
                manifest_sha256=manifest_sha256,
                decision_as_of_trade_date=decision_as_of_trade_date,
                data_source=data_source,
                execution_origin=execution_origin,
                research_scope=research_scope,
                execution_prohibited=execution_prohibited,
                binding=self._binding,
            )
        except Exception:
            return TraceCaptureResult(
                package_id=package_id,
                state=TraceCaptureState.CAPTURE_FAILED,
                reason_codes=(REASON_TRACE_CONTEXT_INVALID,),
            )
        result = self._sink.capture(
            context=context,
            manifest=manifest,
            artifact=artifact,
            stage_trace=stage_trace,
            runtime_config=runtime_config,
        )
        if result.state is not TraceCaptureState.ENVELOPE_READY or result.envelope is None:
            return result
        if self._outbox_writer is None:
            return TraceCaptureResult(
                package_id=package_id,
                state=TraceCaptureState.OUTBOX_WRITE_FAILED,
                envelope=result.envelope,
                reason_codes=(REASON_TRACE_OUTBOX_WRITER_UNCONFIGURED,),
            )
        if getattr(self._outbox_writer, "non_blocking", False) is not True:
            return TraceCaptureResult(
                package_id=package_id,
                state=TraceCaptureState.OUTBOX_WRITE_FAILED,
                envelope=result.envelope,
                reason_codes=(REASON_TRACE_OUTBOX_BLOCKING_WRITER,),
            )
        try:
            self._outbox_writer.append(result.envelope, binding=context.binding)
            return TraceCaptureResult(
                package_id=package_id,
                state=TraceCaptureState.OUTBOX_QUEUED,
                envelope=result.envelope,
            )
        except Exception as exc:
            return TraceCaptureResult(
                package_id=package_id,
                state=TraceCaptureState.OUTBOX_WRITE_FAILED,
                envelope=result.envelope,
                reason_codes=(str(getattr(exc, "reason_code", REASON_TRACE_OUTBOX_WRITE_FAILED)),),
            )


def build_stage_trace_envelope(
    *,
    context: TraceCaptureContext,
    manifest: Any,
    artifact: Any,
    stage_trace: Any,
    runtime_config: Mapping[str, Any],
) -> StageTraceEnvelope:
    """Copy a completed Selection trace into the deterministic Phase 1 envelope."""

    artifact_payload = _artifact_payload(artifact)
    stages = []
    component_capabilities: list[ComponentCapability] = []
    for name in ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective"):
        receipt = getattr(stage_trace, name, None)
        if receipt is None:
            raise ComponentEvidenceError(REASON_TRACE_CONTEXT_INVALID)
        stage_payload = _copy_json(receipt.model_dump(mode="json") if hasattr(receipt, "model_dump") else receipt)
        candidate_components: dict[str, dict[str, Any]] = {}
        stage_members = [
            *(stage_payload.get("candidates") or []),
            *(stage_payload.get("exclusions") or []),
        ]
        for candidate in stage_members:
            result = build_component_evidence(
                manifest=manifest,
                artifact=artifact,
                candidate=candidate,
                runtime_config=runtime_config,
                stage_name=name,
            )
            symbol = str(candidate["symbol"])
            if symbol in candidate_components:
                raise ComponentEvidenceError(REASON_TRACE_CONTEXT_INVALID)
            candidate_components[symbol] = result.model_dump(mode="json")
            component_capabilities.append(result.capability)
        stages.append(
            {
                "stage": name,
                "receipt": stage_payload,
                "candidate_component_evidence": candidate_components,
            }
        )
    trace_content = {
        "schema_version": PHASE1_STAGE_TRACE_SCHEMA_VERSION,
        "selection_identity": {
            "selection_run_id": context.selection_run_id,
            "package_id": context.package_id,
            "manifest_sha256": context.manifest_sha256,
            "decision_as_of_trade_date": context.decision_as_of_trade_date.isoformat(),
            "data_source": context.data_source,
            "execution_origin": context.execution_origin,
            "research_scope": context.research_scope,
            "execution_prohibited": context.execution_prohibited,
        },
        "trace_capture_binding": context.binding.model_dump(mode="json"),
        "raw_score_artifact": artifact_payload,
        "stage_trace": stages,
        "hmm_metadata": _copy_json(getattr(stage_trace, "hmm_metadata", {})),
        "risk_metadata": _copy_json(getattr(stage_trace, "risk_metadata", {})),
        "universe_metadata": _copy_json(getattr(stage_trace, "universe_metadata", {})),
        "component_capability": _aggregate_component_capability(component_capabilities).value,
    }
    trace_content = _canonicalize(trace_content)
    trace_content_hash = _canonical_json_sha256(trace_content)
    return StageTraceEnvelope(
        trace_outbox_id=f"sto_{trace_content_hash[:20]}",
        trace_content_hash=trace_content_hash,
        trace_content=trace_content,
        candidate_count=(
            len(stages[0]["receipt"].get("candidates") or [])
            + len(stages[0]["receipt"].get("exclusions") or [])
        ),
        size_bytes=_canonical_json_size(trace_content),
    )


def build_component_evidence(
    *,
    manifest: Any,
    artifact: Any,
    candidate: Mapping[str, Any],
    runtime_config: Mapping[str, Any],
    stage_name: str = "alpha_raw",
) -> ComponentEvidenceResult:
    """Freeze one native multi-alpha parent candidate's component provenance.

    Missing component inputs never rewrite the parent candidate.  The caller
    receives PARTIAL/UNAVAILABLE with a stable reason and can preserve parent
    authority while blocking component attribution/ablation.
    """

    alpha_mode = _enum_value(getattr(manifest, "alpha_mode", None))
    if alpha_mode != "multi_alpha":
        return ComponentEvidenceResult(capability=ComponentCapability.NOT_APPLICABLE)
    try:
        payload = _build_multi_alpha_component_payload(
            manifest=manifest,
            artifact=artifact,
            candidate=candidate,
            runtime_config=runtime_config,
            stage_name=stage_name,
        )
    except ComponentEvidenceError as exc:
        return ComponentEvidenceResult(capability=exc.capability, reason_codes=(exc.reason_code,))
    return ComponentEvidenceResult(
        capability=ComponentCapability.FULL,
        schema_version=MULTI_ALPHA_COMPONENT_EVIDENCE_SCHEMA_VERSION,
        component_evidence=payload,
        component_evidence_hash=_canonical_json_sha256(payload),
    )


def _build_multi_alpha_component_payload(
    *,
    manifest: Any,
    artifact: Any,
    candidate: Mapping[str, Any],
    runtime_config: Mapping[str, Any],
    stage_name: str,
) -> dict[str, Any]:
    metadata = _mapping(getattr(artifact, "metadata", None), REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    package_id = str(getattr(manifest, "package_id", "") or "").strip()
    manifest_sha256 = str(getattr(manifest, "manifest_sha256", "") or "").strip().lower()
    if not package_id or not _is_sha256(manifest_sha256):
        raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    if str(getattr(artifact, "package_id", "") or "") != package_id or str(getattr(artifact, "manifest_sha256", "") or "").lower() != manifest_sha256:
        raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    artifact_rows = getattr(artifact, "scores_json", None)
    if not isinstance(artifact_rows, list):
        raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    matching_rows = [
        row
        for row in artifact_rows
        if isinstance(row, Mapping)
        and str(row.get("symbol") or "") == str(candidate.get("symbol") or "")
    ]
    if len(matching_rows) != 1:
        raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    raw_candidate = dict(matching_rows[0])
    if stage_name == "alpha_raw":
        if raw_candidate.get("rank") != candidate.get("rank"):
            raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
        if _decimal_text(raw_candidate.get("score"), REASON_COMPONENT_EVIDENCE_INCOMPLETE) != _decimal_text(
            candidate.get("score"), REASON_COMPONENT_EVIDENCE_INCOMPLETE
        ):
            raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    parent_parity = _mapping(metadata.get("multi_alpha_parent_parity"), REASON_COMPONENT_PARENT_PARITY_INVALID)
    parent_parity_hash = str(metadata.get("multi_alpha_parent_parity_hash") or "")
    if not _is_sha256(parent_parity_hash) or _canonical_json_sha256(parent_parity) != parent_parity_hash:
        raise ComponentEvidenceError(REASON_COMPONENT_PARENT_PARITY_INVALID)
    if parent_parity.get("parent_package_id") != package_id or parent_parity.get("parent_manifest_sha256") != manifest_sha256:
        raise ComponentEvidenceError(REASON_COMPONENT_PARENT_PARITY_INVALID)

    components = list(getattr(manifest, "alpha_components", []) or [])
    expected_leg_ids = sorted(str(getattr(component, "alpha_id", "") or "") for component in components)
    if len(expected_leg_ids) < 2 or not all(expected_leg_ids) or len(set(expected_leg_ids)) != len(expected_leg_ids):
        raise ComponentEvidenceError(REASON_COMPONENT_LEG_MISSING)
    source_evidence = _mapping(getattr(manifest, "source_evidence", None), REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    multi_alpha = _mapping(source_evidence.get("multi_alpha"), REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    source_legs = _indexed_legs(multi_alpha.get("legs"), expected_leg_ids)
    artifact_components = _mapping(metadata.get("component_artifacts"), REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    metadata_weights = _mapping(metadata.get("weights"), REASON_COMPONENT_WEIGHT_MISMATCH)
    parity_component_hashes = _mapping(
        parent_parity.get("component_score_artifact_sha256"), REASON_COMPONENT_PARENT_PARITY_INVALID
    )
    metadata_component_hashes = _mapping(
        metadata.get("component_score_artifact_sha256"), REASON_COMPONENT_PARENT_PARITY_INVALID
    )
    if (
        parity_component_hashes != metadata_component_hashes
        or parent_parity.get("weight_artifact_id") != metadata.get("weight_artifact_id")
        or parent_parity.get("weight_artifact_sha256") != metadata.get("weight_artifact_sha256")
        or parent_parity.get("combined_score_artifact_sha256") != metadata.get("combined_score_artifact_sha256")
        or _canonicalize(parent_parity.get("weights")) != _canonicalize(metadata_weights)
    ):
        raise ComponentEvidenceError(REASON_COMPONENT_PARENT_PARITY_INVALID)
    policy = getattr(manifest, "alpha_combination_policy", None)
    policy_payload = _copy_json(policy.model_dump(mode="json") if hasattr(policy, "model_dump") else policy)
    if not policy_payload:
        raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    raw_candidate_components = raw_candidate.get("component_scores")
    if not isinstance(raw_candidate_components, Mapping) or not raw_candidate_components:
        raise ComponentEvidenceError(REASON_COMPONENT_LEG_MISSING, ComponentCapability.UNAVAILABLE)
    candidate_components = dict(raw_candidate_components)
    if sorted(candidate_components) != expected_leg_ids:
        raise ComponentEvidenceError(REASON_COMPONENT_LEG_MISSING)
    parity_leg_ids = sorted(str(item) for item in parent_parity.get("leg_ids") or [])
    if parity_leg_ids != expected_leg_ids:
        raise ComponentEvidenceError(REASON_COMPONENT_PARENT_PARITY_INVALID)

    runtime_variant = _runtime_variant(runtime_config)
    combined_score = _decimal_text(raw_candidate.get("score"), REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    components_payload: list[dict[str, Any]] = []
    for leg_id in expected_leg_ids:
        source_leg = source_legs[leg_id]
        leg_candidate = _mapping(candidate_components.get(leg_id), REASON_COMPONENT_LEG_MISSING)
        artifact_entry = _mapping(artifact_components.get(leg_id), REASON_COMPONENT_EVIDENCE_INCOMPLETE)
        weight = _decimal_text(leg_candidate.get("weight"), REASON_COMPONENT_WEIGHT_MISMATCH)
        expected_weight = _decimal_text(metadata_weights.get(leg_id), REASON_COMPONENT_WEIGHT_MISMATCH)
        if weight != expected_weight:
            raise ComponentEvidenceError(REASON_COMPONENT_WEIGHT_MISMATCH)
        leg_rank = leg_candidate.get("leg_rank")
        if not isinstance(leg_rank, int) or leg_rank < 1:
            raise ComponentEvidenceError(REASON_COMPONENT_RANK_MISSING)
        child_package_id = str(source_leg.get("child_package_id") or package_id).strip()
        child_manifest_sha256 = str(source_leg.get("child_manifest_sha256") or manifest_sha256).strip().lower()
        if not child_package_id or not _is_sha256(child_manifest_sha256):
            raise ComponentEvidenceError(REASON_COMPONENT_LEG_MISSING)
        component_artifact_hash = str(
            artifact_entry.get("component_score_artifact_sha256")
            or (metadata.get("component_score_artifact_sha256") or {}).get(leg_id)
            or ""
        ).lower()
        if not _is_sha256(component_artifact_hash):
            raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
        components_payload.append(
            {
                "leg_package_id": child_package_id,
                "leg_manifest_sha256": child_manifest_sha256,
                "leg_role": leg_id,
                "weight_decimal": weight,
                "weight_source": f"weight_artifact:{metadata.get('weight_artifact_id')}",
                "component_score_artifact_sha256": component_artifact_hash,
                "leg_score_decimal": _decimal_text(leg_candidate.get("raw_score"), REASON_COMPONENT_EVIDENCE_INCOMPLETE),
                "leg_normalized_score_decimal": _decimal_text(
                    leg_candidate.get("normalized_score"), REASON_COMPONENT_EVIDENCE_INCOMPLETE
                ),
                "leg_rank": leg_rank,
                "availability_status": "FULL",
                "reason_codes": [],
            }
        )
    components_payload.sort(key=lambda item: (item["leg_package_id"], item["leg_manifest_sha256"], item["leg_role"]))
    weight_vector_hash = _canonical_json_sha256(
        [{"leg_role": item["leg_role"], "weight_decimal": item["weight_decimal"]} for item in components_payload]
    )
    component_set_hash = _canonical_json_sha256(components_payload)
    combined_score_content_hash = _canonical_json_sha256(
        {
            "symbol": str(candidate.get("symbol") or ""),
            "parent_alpha_raw_rank": raw_candidate.get("rank"),
            "combined_score_decimal": combined_score,
            "components": components_payload,
        }
    )
    requested_top_k = metadata.get("final_topk")
    if not isinstance(requested_top_k, int) or requested_top_k < 1:
        raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    return _canonicalize(
        {
            "schema_version": MULTI_ALPHA_COMPONENT_EVIDENCE_SCHEMA_VERSION,
            "parent_package_id": package_id,
            "parent_manifest_sha256": manifest_sha256,
            "combination_policy_id": "manifest_alpha_combination_policy",
            "combination_policy_version": str(getattr(manifest, "manifest_version", "") or "alpha_core_v1"),
            "combination_policy_hash": _canonical_json_sha256(policy_payload),
            "runtime_variant_id": runtime_variant["variant_id"],
            "runtime_variant_hash": runtime_variant["variant_hash"],
            "requested_top_k": requested_top_k,
            "effective_top_k": requested_top_k,
            "component_order_policy": "canonical_leg_identity_v1",
            "components": components_payload,
            "weight_vector_hash": weight_vector_hash,
            "component_set_hash": component_set_hash,
            "combined_score_decimal": combined_score,
            "combined_score_content_hash": combined_score_content_hash,
            "parent_combine_parity_hash": parent_parity_hash,
        }
    )


def _artifact_payload(artifact: Any) -> dict[str, Any]:
    contract_version = str(getattr(artifact, "artifact_contract_version", "") or "")
    if contract_version != "selection_score_artifact_v2":
        raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    payload_hash = str(getattr(artifact, "artifact_payload_sha256", "") or "").lower()
    artifact_hash = str(getattr(artifact, "artifact_sha256", "") or "").lower()
    if not _is_sha256(payload_hash) or not _is_sha256(artifact_hash):
        raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    scores = _copy_json(getattr(artifact, "scores_json", []))
    if not isinstance(scores, list):
        raise ComponentEvidenceError(REASON_COMPONENT_EVIDENCE_INCOMPLETE)
    return {
        "artifact_id": str(getattr(artifact, "artifact_id", "") or ""),
        "artifact_contract_version": contract_version,
        "artifact_payload_sha256": payload_hash,
        "artifact_sha256": artifact_hash,
        "artifact_input_context_hash": str(getattr(artifact, "artifact_input_context_hash", "") or ""),
        "source_revision_set_hash": str(getattr(artifact, "source_revision_set_hash", "") or ""),
        "asset_closure_hash": str(getattr(artifact, "asset_closure_hash", "") or ""),
        "scores_json": scores,
    }


def _indexed_legs(raw: Any, expected_leg_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, list):
        raise ComponentEvidenceError(REASON_COMPONENT_LEG_MISSING)
    indexed: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, Mapping):
            raise ComponentEvidenceError(REASON_COMPONENT_LEG_MISSING)
        leg_id = str(item.get("leg_id") or "").strip()
        if not leg_id or leg_id in indexed:
            raise ComponentEvidenceError(REASON_COMPONENT_LEG_MISSING)
        indexed[leg_id] = dict(item)
    if sorted(indexed) != expected_leg_ids:
        raise ComponentEvidenceError(REASON_COMPONENT_LEG_MISSING)
    return indexed


def _runtime_variant(runtime_config: Mapping[str, Any]) -> dict[str, str | None]:
    raw = runtime_config.get("runtime_variant")
    if raw is None:
        return {"variant_id": None, "variant_hash": None}
    if not isinstance(raw, Mapping):
        raise ComponentEvidenceError(REASON_COMPONENT_RUNTIME_VARIANT_INVALID)
    variant_id = str(raw.get("variant_id") or "").strip()
    variant_hash = str(raw.get("variant_hash") or "").strip().lower()
    if not variant_id or not _is_sha256(variant_hash):
        raise ComponentEvidenceError(REASON_COMPONENT_RUNTIME_VARIANT_INVALID)
    return {"variant_id": variant_id, "variant_hash": variant_hash}


def _mapping(value: Any, reason_code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ComponentEvidenceError(reason_code)
    return dict(value)


def _decimal_text(value: Any, reason_code: str) -> str:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ComponentEvidenceError(reason_code) from exc
    if not decimal_value.is_finite():
        raise ComponentEvidenceError(reason_code)
    return format(decimal_value.normalize(), "f")


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "")


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _copy_json(value: Any) -> Any:
    copied = deepcopy(value)
    try:
        return _canonicalize(copied)
    except (TypeError, ValueError) as exc:
        raise ComponentEvidenceError(REASON_TRACE_CAPTURE_FAILED) from exc


def _canonical_json_size(value: Any) -> int:
    return len(json.dumps(_canonicalize(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8"))


def _capture_preflight_reason(*, artifact: Any, stage_trace: Any, policy: TraceCapturePolicy) -> str | None:
    """Reject oversized capture input before copying its immutable projection."""

    alpha_receipt = getattr(stage_trace, "alpha_raw", None)
    alpha_count = getattr(alpha_receipt, "output_count", None)
    scores = getattr(artifact, "scores_json", None)
    if not isinstance(alpha_count, int) or not isinstance(scores, list):
        return REASON_TRACE_CAPTURE_FAILED
    if alpha_count > policy.max_candidates or len(scores) > policy.max_candidates:
        return REASON_TRACE_CANDIDATE_LIMIT_EXCEEDED
    raw_trace = stage_trace.model_dump(mode="json") if hasattr(stage_trace, "model_dump") else stage_trace
    raw_payload = {"scores_json": scores, "stage_trace": raw_trace}
    try:
        if _bounded_json_size(raw_payload, max_bytes=policy.max_bytes) > policy.max_bytes:
            return REASON_TRACE_BYTE_LIMIT_EXCEEDED
    except (TypeError, ValueError):
        return REASON_TRACE_CAPTURE_FAILED
    return None


def _bounded_json_size(value: Any, *, max_bytes: int) -> int:
    """Encode incrementally so a hostile payload cannot allocate an unbounded string."""

    total = 0
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    for chunk in encoder.iterencode(value):
        total += len(chunk.encode("utf-8"))
        if total > max_bytes:
            return total
    return total


def _aggregate_component_capability(values: list[ComponentCapability]) -> ComponentCapability:
    if not values or all(value is ComponentCapability.NOT_APPLICABLE for value in values):
        return ComponentCapability.NOT_APPLICABLE
    if all(value is ComponentCapability.FULL for value in values):
        return ComponentCapability.FULL
    if any(value is ComponentCapability.UNAVAILABLE for value in values):
        return ComponentCapability.UNAVAILABLE
    return ComponentCapability.PARTIAL


def _canonicalize(value: Any) -> Any:
    # Phase 0A owns the canonical serializer. Delay this import so the default
    # Null Selection sink cannot introduce an Advisory/Paper import cycle.
    from backend.services.advisory_phase0a.policy import canonicalize

    return canonicalize(value)


def _canonical_json_sha256(value: Any) -> str:
    from backend.services.advisory_phase0a.policy import canonical_json_sha256

    return canonical_json_sha256(value)
