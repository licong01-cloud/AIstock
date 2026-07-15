"""Strict P1-E B0_QUOTE_V2 binding, identity and tick-projection contracts.

This module deliberately owns no scheduler, database connection, callback, or
gateway.  The scheduler-owned controller added by P1-E composes these immutable
contracts with the existing ingress/evidence/runtime owners.
"""

from __future__ import annotations

from dataclasses import MISSING as DATACLASS_SENTINEL
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, get_type_hints
from zoneinfo import ZoneInfo

from backend.execution_algos.adaptive_is.contracts import (
    MARKET_DATA_EVIDENCE_SCHEMA_VERSION,
    ActionQuoteEligibility,
    ControlRevision,
    EvidenceCaptureType,
    EligibilityState,
    FiveLevelQuote,
    MarketDataEvidenceV1,
    TradabilitySnapshot,
    canonical_sha256,
)
from backend.execution_algos.adaptive_is.reasons import (
    QuoteContractError,
    QuoteContractReasonCode,
    quote_contract_error,
)
from backend.execution_algos.vnpy_style.models import VnpyAction, VnpyActionType, VnpyTick
from backend.miniqmt_quote_contract_config import QuoteContractPolicy, QuoteIngressRuntimeConfig
from backend.services.miniqmt_execution_runtime.models import (
    MiniQMTChildOrder,
    MiniQMTExecutionAlgoInstance,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
)
from backend.services.miniqmt_execution_runtime.quote_eligibility import (
    ActionQuoteEvaluator,
    ActionQuoteRequest,
    BoundedNormalizedQuoteStore,
    EligibilityEvaluation,
    NormalizedQuoteObservation,
    QuoteEvaluationContextStore,
)
from backend.services.miniqmt_execution_runtime.quote_evidence import MarkoutAnchor, QuoteEvidenceCoordinator
from backend.services.miniqmt_execution_runtime.repository import DurableEvidenceReceipt

QUOTE_CONTROL_BINDING_KEY = "miniqmt_quote_control"
QUOTE_CONTROL_BINDING_SCHEMA_VERSION = "miniqmt_quote_control_binding_v1"
B0_QUOTE_V2_REVISION_SCHEMA_VERSION = "b0_quote_v2_revision_v1"
PARENT_QUOTE_CONTROL_ASSIGNMENT_SCHEMA_VERSION = "parent_quote_control_assignment_v1"
B0_QUOTE_V2_TICK_PROJECTION_SCHEMA_VERSION = "b0_quote_v2_tick_projection_v1"
PARITY_EXCLUSIONS_VERSION = "b0_quote_v2_parity_exclusions_v1"
QUOTE_EVIDENCE_POLICY_KEY = "quote_evidence"
QUOTE_EVIDENCE_POLICY_SCHEMA_VERSION = "miniqmt_quote_evidence_policy_v1"
B0_QUOTE_V2_ACTION_ENVELOPE_SCHEMA_VERSION = "b0_quote_v2_action_envelope_v1"
B0_QUOTE_V2_ACTION_PENDING_SCHEMA_VERSION = "b0_quote_v2_action_pending_v1"
PARITY_EXCLUDED_FIELDS = frozenset(
    {
        "action_id",
        "vt_orderid",
        "algo_name",
        "event_id",
        "evaluation_id",
        "evidence_id",
        "market_data_id",
        "clock_event_id",
        "tradability_id",
        "created_at",
        "updated_at",
        "submitted_at",
        "trace_id",
    }
)


class B0QuoteV2RuntimePort(Protocol):
    config: Any
    repository: Any
    events: Any

    def dispatch_b0_quote_v2_tick(self, *, instance: MiniQMTExecutionAlgoInstance, tick: VnpyTick) -> None: ...

    def submit_b0_quote_v2_child(
        self,
        *,
        instance: MiniQMTExecutionAlgoInstance,
        action: VnpyAction,
        child_order_id: str,
        metadata: dict[str, Any],
    ) -> tuple[MiniQMTChildOrder, MiniQMTExecutionEvent]: ...


def _required_text(value: Any, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise quote_contract_error(
            QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
            f"{field_name} is required",
        )
    return text


def _required_sha256(value: Any, *, field_name: str) -> str:
    text = _required_text(value, field_name=field_name)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise quote_contract_error(
            QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
            f"{field_name} must be a lowercase sha256",
            context={"field_name": field_name},
        )
    return text


@dataclass(frozen=True)
class QuoteControlBindingV1:
    """Explicit binding-side control-revision selection.

    It intentionally does not copy execution-policy fields: policy ownership
    remains with the immutable runtime release and execution plan.
    """

    control_revision: ControlRevision
    explicitly_configured: bool

    @classmethod
    def from_binding_config(cls, binding_config: Mapping[str, Any]) -> "QuoteControlBindingV1":
        if not isinstance(binding_config, Mapping):
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID, "binding_config must be a mapping"
            )
        raw = binding_config.get(QUOTE_CONTROL_BINDING_KEY)
        if raw is None:
            # The sole documented compatibility rule: historic bindings are
            # legacy; an explicit invalid value must never share this branch.
            return cls(control_revision=ControlRevision.LEGACY_B0, explicitly_configured=False)
        if not isinstance(raw, Mapping):
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID, "miniqmt_quote_control must be a mapping"
            )
        expected = {"schema_version", "control_revision"}
        received = {str(key) for key in raw}
        if received != expected:
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "miniqmt_quote_control must contain exactly schema_version and control_revision",
                context={"unknown": sorted(received - expected), "missing": sorted(expected - received)},
            )
        if raw.get("schema_version") != QUOTE_CONTROL_BINDING_SCHEMA_VERSION:
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "miniqmt_quote_control schema_version is unsupported",
                context={"schema_version": raw.get("schema_version")},
            )
        try:
            revision = ControlRevision(raw.get("control_revision"))
        except (TypeError, ValueError) as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "miniqmt_quote_control control_revision is invalid",
            ) from exc
        return cls(control_revision=revision, explicitly_configured=True)

    def canonical_payload(self) -> dict[str, str]:
        return {
            "schema_version": QUOTE_CONTROL_BINDING_SCHEMA_VERSION,
            "control_revision": self.control_revision.value,
        }


@dataclass(frozen=True)
class B0QuoteV2RevisionV1:
    revision_id: str
    execution_policy_version_id: str
    execution_policy_sha256: str
    quote_policy_sha256: str
    adapter_version: str
    adapter_sha256: str
    code_revision: str
    code_sha256: str
    evidence_schema_version: str
    evidence_schema_sha256: str
    benchmark_policy_version: str
    mark_policy_version: str
    markout_max_lag_ms: int

    @classmethod
    def build(
        cls,
        *,
        execution_policy: Mapping[str, Any],
        execution_policy_version_id: str,
        execution_policy_sha256: str,
        adapter_version: str,
        adapter_sha256: str,
        code_revision: str,
        code_sha256: str,
        evidence_schema_version: str,
        evidence_schema_sha256: str,
        benchmark_policy_version: str,
        mark_policy_version: str,
        markout_max_lag_ms: int,
    ) -> "B0QuoteV2RevisionV1":
        policy = QuoteContractPolicy.from_execution_policy(execution_policy)
        if policy.control_revision != ControlRevision.B0_QUOTE_V2.value:
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "B0_QUOTE_V2 revision requires an explicit B0_QUOTE_V2 quote_contract policy",
            )
        if isinstance(markout_max_lag_ms, bool) or not isinstance(markout_max_lag_ms, int) or markout_max_lag_ms < 0:
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "markout_max_lag_ms must be a non-negative integer",
            )
        payload = {
            "schema_version": B0_QUOTE_V2_REVISION_SCHEMA_VERSION,
            "control_revision": ControlRevision.B0_QUOTE_V2.value,
            "execution_policy_version_id": _required_text(
                execution_policy_version_id, field_name="execution_policy_version_id"
            ),
            "execution_policy_sha256": _required_sha256(execution_policy_sha256, field_name="execution_policy_sha256"),
            "quote_policy_sha256": policy.policy_sha256,
            "adapter_version": _required_text(adapter_version, field_name="adapter_version"),
            "adapter_sha256": _required_sha256(adapter_sha256, field_name="adapter_sha256"),
            "code_revision": _required_text(code_revision, field_name="code_revision"),
            "code_sha256": _required_sha256(code_sha256, field_name="code_sha256"),
            "evidence_schema_version": _required_text(evidence_schema_version, field_name="evidence_schema_version"),
            "evidence_schema_sha256": _required_sha256(evidence_schema_sha256, field_name="evidence_schema_sha256"),
            "benchmark_policy_version": _required_text(benchmark_policy_version, field_name="benchmark_policy_version"),
            "mark_policy_version": _required_text(mark_policy_version, field_name="mark_policy_version"),
            "markout_max_lag_ms": markout_max_lag_ms,
        }
        revision_id = "b0qrev_" + canonical_sha256(payload)
        return cls(
            revision_id=revision_id,
            **{key: value for key, value in payload.items() if key not in {"schema_version", "control_revision"}},
        )

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": B0_QUOTE_V2_REVISION_SCHEMA_VERSION,
            "control_revision": ControlRevision.B0_QUOTE_V2.value,
            "revision_id": self.revision_id,
            "execution_policy_version_id": self.execution_policy_version_id,
            "execution_policy_sha256": self.execution_policy_sha256,
            "quote_policy_sha256": self.quote_policy_sha256,
            "adapter_version": self.adapter_version,
            "adapter_sha256": self.adapter_sha256,
            "code_revision": self.code_revision,
            "code_sha256": self.code_sha256,
            "evidence_schema_version": self.evidence_schema_version,
            "evidence_schema_sha256": self.evidence_schema_sha256,
            "benchmark_policy_version": self.benchmark_policy_version,
            "mark_policy_version": self.mark_policy_version,
            "markout_max_lag_ms": self.markout_max_lag_ms,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "B0QuoteV2RevisionV1":
        expected = set(cls.__dataclass_fields__) | {"schema_version", "control_revision"}
        received = {str(key) for key in payload}
        if received != expected:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 revision payload has non-exact fields",
                context={"unknown": sorted(received - expected), "missing": sorted(expected - received)},
            )
        if (
            payload.get("schema_version") != B0_QUOTE_V2_REVISION_SCHEMA_VERSION
            or payload.get("control_revision") != ControlRevision.B0_QUOTE_V2.value
        ):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 revision schema or control revision is invalid",
            )
        values = {name: payload[name] for name in cls.__dataclass_fields__}
        revision = cls(**values)
        canonical = revision.canonical_payload()
        expected_id = "b0qrev_" + canonical_sha256(
            {key: value for key, value in canonical.items() if key != "revision_id"}
        )
        if revision.revision_id != expected_id:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 revision identity does not match its canonical payload",
                context={"revision_id": revision.revision_id, "expected_revision_id": expected_id},
            )
        return revision


@dataclass(frozen=True)
class B0QuoteV2BuildManifestV1:
    adapter_version: str
    adapter_sha256: str
    code_revision: str
    code_sha256: str
    evidence_schema_version: str
    evidence_schema_sha256: str


def source_build_manifest() -> B0QuoteV2BuildManifestV1:
    """Build the versioned manifest from fixed repository-relative source paths."""

    root = Path(__file__).resolve().parents[3]

    def file_hash(relative_path: str) -> str:
        path = root / relative_path
        if not path.is_file():
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "B0_QUOTE_V2 build manifest source file is missing",
                context={"relative_path": relative_path},
            )
        return sha256(path.read_bytes()).hexdigest()

    adapter_paths = (
        "backend/services/miniqmt_execution_runtime/b0_quote_v2.py",
        "backend/services/miniqmt_execution_runtime/quote_eligibility.py",
        "backend/services/miniqmt_execution_runtime/quote_normalizer.py",
    )
    code_paths = (
        "backend/services/miniqmt_execution_runtime/runtime.py",
        "backend/services/miniqmt_execution_runtime/client.py",
        "backend/services/miniqmt_execution_runtime/gateway.py",
        "backend/services/simulation_runtime/bridges.py",
        "backend/services/simulation_runtime/scheduler.py",
    )
    adapter_files = {path: file_hash(path) for path in adapter_paths}
    code_files = {path: file_hash(path) for path in code_paths}
    adapter_payload = {
        "schema_version": "b0_quote_v2_adapter_manifest_v1",
        "files": adapter_files,
        "adapter_contract_version": B0_QUOTE_V2_TICK_PROJECTION_SCHEMA_VERSION,
    }
    code_payload = {
        "schema_version": "b0_quote_v2_code_manifest_v1",
        "files": code_files,
        "parity_exclusions_version": PARITY_EXCLUSIONS_VERSION,
    }
    evidence_payload = evidence_schema_manifest_payload()
    vnpy_assets = _vnpy_asset_manifest()
    code_payload["vnpy_assets"] = vnpy_assets
    return B0QuoteV2BuildManifestV1(
        adapter_version="b0_quote_v2_adapter_manifest_v1",
        adapter_sha256=canonical_sha256(adapter_payload),
        code_revision="b0_quote_v2_code_manifest_v1",
        code_sha256=canonical_sha256(code_payload),
        evidence_schema_version="b0_quote_v2_evidence_schema_manifest_v1",
        evidence_schema_sha256=canonical_sha256(evidence_payload),
    )


def evidence_schema_manifest_payload() -> dict[str, Any]:
    """Canonical schema registry; version labels alone are never schema evidence."""

    hints = get_type_hints(MarketDataEvidenceV1)
    field_registry = []
    for item in fields(MarketDataEvidenceV1):
        if not item.init:
            continue
        annotation = hints[item.name]
        nullable = type(None) in getattr(annotation, "__args__", ())
        field_registry.append(
            {
                "name": item.name,
                "type": str(annotation).replace("typing.", ""),
                "required": item.default is DATACLASS_SENTINEL and item.default_factory is DATACLASS_SENTINEL,
                "nullable": nullable,
            }
        )
    event_mapping = {
        "ACTION_INPUT": "QUOTE_ELIGIBILITY_EVALUATED",
        "ACTION_REJECT": "QUOTE_REJECTED",
        "CHILD_RECEIPT": "QUOTE_MARK_CAPTURED",
        "PROTECTION_BAND_TRIGGER": "QUOTE_MARK_CAPTURED",
        "MARKOUT_60S": "QUOTE_MARK_CAPTURED",
        "MARKOUT_300S": "QUOTE_MARK_CAPTURED",
        "MARKOUT_900S": "QUOTE_MARK_CAPTURED",
        "CADENCE_AGGREGATE": "QUOTE_OBSERVED",
    }
    return {
        "schema_version": "b0_quote_v2_evidence_schema_manifest_v1",
        "evidence_schema_version": MARKET_DATA_EVIDENCE_SCHEMA_VERSION,
        "market_data_evidence_fields": field_registry,
        "event_payload_schema_version": "miniqmt_quote_runtime_event_payload_v1",
        "event_source": "quote_ingress",
        "event_type_by_capture_type": event_mapping,
        "evidence_identity_version": "miniqmt_market_data_evidence_identity_v1",
        "evidence_hash_version": "miniqmt_market_data_evidence_hash_v1",
        "assignment_schema_version": PARENT_QUOTE_CONTROL_ASSIGNMENT_SCHEMA_VERSION,
        "action_schema_version": "b0_quote_v2_action_envelope_v1",
        "tick_projection_schema_version": B0_QUOTE_V2_TICK_PROJECTION_SCHEMA_VERSION,
        "phase0b_record_schema_version": "miniqmt_execution_tca_evidence_record_v2",
        "phase0b_manifest_schema_version": "miniqmt_execution_tca_evidence_manifest_v2",
    }


def _vnpy_asset_manifest() -> dict[str, Any]:
    from backend.execution_algos.vnpy_style.registry import VNPY_STYLE_ASSETS

    return {
        code: {
            "asset_version": spec.version,
            "source_attribution": spec.metadata()["source_attribution"],
        }
        for code, spec in sorted(VNPY_STYLE_ASSETS.items())
        if code in {"SNIPER_MINIQMT", "BEST_LIMIT_MINIQMT", "TWAP_LITE_MINIQMT"}
    }


def quote_evidence_policy(execution_policy: Mapping[str, Any]) -> tuple[str, str, int]:
    raw = execution_policy.get(QUOTE_EVIDENCE_POLICY_KEY)
    if not isinstance(raw, Mapping):
        raise quote_contract_error(
            QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
            "execution_policy.quote_evidence must be explicit for B0_QUOTE_V2",
        )
    expected = {"schema_version", "benchmark_policy_version", "mark_policy_version", "markout_max_lag_ms"}
    received = {str(key) for key in raw}
    if received != expected or raw.get("schema_version") != QUOTE_EVIDENCE_POLICY_SCHEMA_VERSION:
        raise quote_contract_error(
            QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
            "execution_policy.quote_evidence schema is invalid",
            context={"unknown": sorted(received - expected), "missing": sorted(expected - received)},
        )
    lag = raw.get("markout_max_lag_ms")
    if isinstance(lag, bool) or not isinstance(lag, int) or lag < 0:
        raise quote_contract_error(
            QuoteContractReasonCode.POLICY_SCHEMA_INVALID, "markout_max_lag_ms must be a non-negative integer"
        )
    return (
        _required_text(raw.get("benchmark_policy_version"), field_name="benchmark_policy_version"),
        _required_text(raw.get("mark_policy_version"), field_name="mark_policy_version"),
        lag,
    )


@dataclass(frozen=True)
class ParentQuoteControlAssignmentV1:
    assignment_id: str
    binding_id: str
    binding_hash: str
    trade_date: date
    parent_intent_id: str
    control_revision: ControlRevision
    revision: B0QuoteV2RevisionV1 | None

    @classmethod
    def build(
        cls,
        *,
        binding_id: str,
        binding_hash: str,
        trade_date: date,
        parent_intent_id: str,
        control_revision: ControlRevision,
        revision: B0QuoteV2RevisionV1 | None,
    ) -> "ParentQuoteControlAssignmentV1":
        if not isinstance(trade_date, date):
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID, "assignment trade_date must be a date"
            )
        if control_revision == ControlRevision.B0_QUOTE_V2 and revision is None:
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID, "B0_QUOTE_V2 assignment requires a revision"
            )
        if control_revision == ControlRevision.LEGACY_B0 and revision is not None:
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "LEGACY_B0 assignment cannot carry a B0_QUOTE_V2 revision",
            )
        assignment_seed = {
            "schema_version": PARENT_QUOTE_CONTROL_ASSIGNMENT_SCHEMA_VERSION,
            "binding_id": _required_text(binding_id, field_name="binding_id"),
            "binding_hash": _required_sha256(binding_hash, field_name="binding_hash"),
            "trade_date": trade_date.isoformat(),
            "parent_intent_id": _required_text(parent_intent_id, field_name="parent_intent_id"),
            "control_revision": ControlRevision(control_revision).value,
            "revision_id": revision.revision_id if revision else None,
        }
        return cls(
            assignment_id="b0qassign_" + canonical_sha256(assignment_seed),
            binding_id=assignment_seed["binding_id"],
            binding_hash=assignment_seed["binding_hash"],
            trade_date=trade_date,
            parent_intent_id=assignment_seed["parent_intent_id"],
            control_revision=ControlRevision(control_revision),
            revision=revision,
        )

    def canonical_payload(self) -> dict[str, Any]:
        revision = self.revision
        payload = {
            "schema_version": PARENT_QUOTE_CONTROL_ASSIGNMENT_SCHEMA_VERSION,
            "assignment_id": self.assignment_id,
            "binding_id": self.binding_id,
            "binding_hash": self.binding_hash,
            "trade_date": self.trade_date.isoformat(),
            "parent_intent_id": self.parent_intent_id,
            "control_revision": self.control_revision.value,
            "revision_id": revision.revision_id if revision else None,
            "execution_policy_version_id": revision.execution_policy_version_id if revision else None,
            "execution_policy_sha256": revision.execution_policy_sha256 if revision else None,
            "quote_policy_sha256": revision.quote_policy_sha256 if revision else None,
            "adapter_sha256": revision.adapter_sha256 if revision else None,
            "code_sha256": revision.code_sha256 if revision else None,
            "evidence_schema_sha256": revision.evidence_schema_sha256 if revision else None,
        }
        payload["assignment_sha256"] = canonical_sha256(
            {key: value for key, value in payload.items() if key != "assignment_sha256"}
        )
        return payload

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "ParentQuoteControlAssignmentV1":
        expected = {
            "schema_version",
            "assignment_id",
            "binding_id",
            "binding_hash",
            "trade_date",
            "parent_intent_id",
            "control_revision",
            "revision_id",
            "execution_policy_version_id",
            "execution_policy_sha256",
            "quote_policy_sha256",
            "adapter_sha256",
            "code_sha256",
            "evidence_schema_sha256",
            "assignment_sha256",
        }
        received = {str(key) for key in payload}
        if received != expected or payload.get("schema_version") != PARENT_QUOTE_CONTROL_ASSIGNMENT_SCHEMA_VERSION:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "parent quote-control assignment payload has non-exact fields or schema",
                context={"unknown": sorted(received - expected), "missing": sorted(expected - received)},
            )
        try:
            revision_kind = ControlRevision(payload.get("control_revision"))
            parsed_date = date.fromisoformat(str(payload.get("trade_date")))
        except (TypeError, ValueError) as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "parent quote-control assignment identity is invalid",
            ) from exc
        revision = None
        if revision_kind == ControlRevision.B0_QUOTE_V2:
            revision_payload = {
                "schema_version": B0_QUOTE_V2_REVISION_SCHEMA_VERSION,
                "control_revision": ControlRevision.B0_QUOTE_V2.value,
                "revision_id": payload.get("revision_id"),
                "execution_policy_version_id": payload.get("execution_policy_version_id"),
                "execution_policy_sha256": payload.get("execution_policy_sha256"),
                "quote_policy_sha256": payload.get("quote_policy_sha256"),
                "adapter_version": payload.get("adapter_version"),
                "adapter_sha256": payload.get("adapter_sha256"),
                "code_revision": payload.get("code_revision"),
                "code_sha256": payload.get("code_sha256"),
                "evidence_schema_version": payload.get("evidence_schema_version"),
                "evidence_schema_sha256": payload.get("evidence_schema_sha256"),
                "benchmark_policy_version": payload.get("benchmark_policy_version"),
                "mark_policy_version": payload.get("mark_policy_version"),
                "markout_max_lag_ms": payload.get("markout_max_lag_ms"),
            }
            # Compact assignment payload intentionally does not carry the full revision.
            # Callers must use from_plan_payload(), which supplies the frozen revision.
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 assignment requires its frozen plan revision for readback",
                context={"assignment_id": payload.get("assignment_id"), "revision_payload": revision_payload},
            )
        assignment = cls.build(
            binding_id=str(payload.get("binding_id")),
            binding_hash=str(payload.get("binding_hash")),
            trade_date=parsed_date,
            parent_intent_id=str(payload.get("parent_intent_id")),
            control_revision=revision_kind,
            revision=revision,
        )
        if assignment.canonical_payload() != dict(payload):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "parent quote-control assignment hash readback failed",
            )
        return assignment

    @classmethod
    def from_plan_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        revision: B0QuoteV2RevisionV1 | None,
    ) -> "ParentQuoteControlAssignmentV1":
        try:
            control_revision = ControlRevision(payload.get("control_revision"))
            parsed_date = date.fromisoformat(str(payload.get("trade_date")))
        except (TypeError, ValueError) as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "parent quote-control assignment identity is invalid",
            ) from exc
        assignment = cls.build(
            binding_id=str(payload.get("binding_id")),
            binding_hash=str(payload.get("binding_hash")),
            trade_date=parsed_date,
            parent_intent_id=str(payload.get("parent_intent_id")),
            control_revision=control_revision,
            revision=revision,
        )
        if assignment.canonical_payload() != dict(payload):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "parent quote-control assignment readback differs from frozen plan payload",
                context={"assignment_id": payload.get("assignment_id")},
            )
        return assignment


def project_vnpy_tick(
    *,
    observation: NormalizedQuoteObservation,
    eligibility: ActionQuoteEligibility,
    assignment: ParentQuoteControlAssignmentV1,
) -> VnpyTick:
    """Project only a proven READY B0 quote; no legacy fallback is permitted."""

    if assignment.control_revision != ControlRevision.B0_QUOTE_V2 or assignment.revision is None:
        raise quote_contract_error(
            QuoteContractReasonCode.POLICY_SCHEMA_INVALID, "B0 quote tick projection requires a B0_QUOTE_V2 assignment"
        )
    if eligibility.control_revision != ControlRevision.B0_QUOTE_V2 or eligibility.state != EligibilityState.READY:
        raise quote_contract_error(
            QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
            "B0 quote tick projection requires READY B0_QUOTE_V2 eligibility",
            context={"state": eligibility.state.value, "reason_code": eligibility.reason_code},
        )
    quote = observation.quote
    revision = assignment.revision
    identity_conflicts = {
        "symbol": quote.symbol != eligibility.symbol or quote.symbol != observation.frame.symbol,
        "market_data_id": eligibility.market_data_id != observation.market_data_id,
        "parent_intent_id": eligibility.parent_intent_id != assignment.parent_intent_id,
        "policy_sha256": eligibility.policy_sha256 != revision.quote_policy_sha256,
        "adapter_sha256": eligibility.adapter_sha256 != revision.adapter_sha256,
    }
    if any(identity_conflicts.values()):
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_TICK_PROJECTION_INVALID,
            "quote projection identities do not belong to the same frozen observation and assignment",
            context={"conflicts": sorted(key for key, conflict in identity_conflicts.items() if conflict)},
        )
    if quote.source_exchange_time_utc is None:
        raise quote_contract_error(
            QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE, "quote projection cannot use a missing exchange timestamp"
        )
    if not quote.bid_prices or not quote.bid_quantities or not quote.ask_prices or not quote.ask_quantities:
        raise quote_contract_error(
            QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE, "quote projection requires first-level depth"
        )
    bid_price, bid_quantity = quote.bid_prices[0], quote.bid_quantities[0]
    ask_price, ask_quantity = quote.ask_prices[0], quote.ask_quantities[0]
    if bid_price <= 0 or ask_price <= 0 or bid_quantity <= 0 or ask_quantity <= 0:
        raise quote_contract_error(
            QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE, "quote projection requires positive opposite-book values"
        )
    raw = {
        "schema_version": B0_QUOTE_V2_TICK_PROJECTION_SCHEMA_VERSION,
        "market_data_id": observation.market_data_id,
        "clock_event_id": eligibility.clock_event_id,
        "tradability_id": eligibility.tradability_id,
        "evaluation_id": action_quote_evaluation_id(observation=observation, eligibility=eligibility),
        "source_session_id": quote.source_session_id,
        "ingress_generation": quote.ingress_generation,
        "ingress_sequence": quote.ingress_sequence,
        "normalized_quote_sha256": quote.normalized_quote_sha256,
        "control_revision": ControlRevision.B0_QUOTE_V2.value,
        "revision_id": revision.revision_id,
        "assignment_id": assignment.assignment_id,
        "policy_sha256": revision.quote_policy_sha256,
        "config_sha256": eligibility.config_sha256,
        "adapter_sha256": revision.adapter_sha256,
        "code_sha256": revision.code_sha256,
        "evidence_schema_sha256": revision.evidence_schema_sha256,
    }
    raw["projection_sha256"] = canonical_sha256(raw)
    return VnpyTick(
        symbol=quote.symbol,
        datetime=quote.source_exchange_time_utc,
        bid_price_1=float(bid_price),
        bid_volume_1=int(bid_quantity),
        ask_price_1=float(ask_price),
        ask_volume_1=int(ask_quantity),
        raw=raw,
    )


def action_quote_evaluation_id(
    *,
    observation: NormalizedQuoteObservation | None,
    eligibility: ActionQuoteEligibility,
) -> str:
    return "qeval_" + canonical_sha256(
        {
            "runtime_id": eligibility.runtime_id,
            "parent_intent_id": eligibility.parent_intent_id,
            "algo_instance_id": eligibility.algo_instance_id,
            "symbol": eligibility.symbol,
            "side": eligibility.side,
            "clock_event_id": eligibility.clock_event_id,
            "market_data_id": eligibility.market_data_id,
            "source_payload_sha256": observation.quote.source_payload_sha256 if observation is not None else None,
            "policy_sha256": eligibility.policy_sha256,
        }
    )


def quote_ingress_config_sha256(config: QuoteIngressRuntimeConfig) -> str:
    return canonical_sha256(
        {
            "schema_version": "miniqmt_quote_ingress_runtime_config_v1",
            **asdict(config),
        }
    )


def canonical_parity_payload(payload: Any) -> Any:
    if isinstance(payload, Mapping):
        return {
            str(key): canonical_parity_payload(value)
            for key, value in sorted(payload.items(), key=lambda item: str(item[0]))
            if str(key) not in PARITY_EXCLUDED_FIELDS
        }
    if isinstance(payload, (list, tuple)):
        return [canonical_parity_payload(value) for value in payload]
    return payload


def assert_b0_quote_v2_parity(*, legacy_payload: Mapping[str, Any], v2_payload: Mapping[str, Any]) -> None:
    legacy = canonical_parity_payload(legacy_payload)
    v2 = canonical_parity_payload(v2_payload)
    if legacy == v2:
        return
    differences = _bounded_parity_differences(legacy, v2)
    raise quote_contract_error(
        QuoteContractReasonCode.PARITY_VIOLATION,
        "B0_QUOTE_V2 fresh/valid business payload differs from LEGACY_B0",
        context={
            "exclusions_version": PARITY_EXCLUSIONS_VERSION,
            "legacy_sha256": canonical_sha256(legacy),
            "v2_sha256": canonical_sha256(v2),
            "differences": differences,
        },
    )


def _bounded_parity_differences(left: Any, right: Any, *, path: str = "$", limit: int = 25) -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []

    def visit(first: Any, second: Any, current_path: str) -> None:
        if len(differences) >= limit or first == second:
            return
        if isinstance(first, Mapping) and isinstance(second, Mapping):
            for key in sorted(set(first) | set(second), key=str):
                visit(first.get(key), second.get(key), f"{current_path}.{key}")
            return
        if isinstance(first, list) and isinstance(second, list):
            for index in range(max(len(first), len(second))):
                visit(
                    first[index] if index < len(first) else None,
                    second[index] if index < len(second) else None,
                    f"{current_path}[{index}]",
                )
            return
        differences.append(
            {
                "path": current_path,
                "legacy": str(first)[:256],
                "b0_quote_v2": str(second)[:256],
            }
        )

    visit(left, right, path)
    return differences


@dataclass(frozen=True)
class B0QuoteV2ActionEnvelopeV1:
    runtime_id: str
    binding_id: str
    trade_date: date
    parent_intent_id: str
    algo_instance_id: str
    assignment_id: str
    revision_id: str
    action_id: str
    action_type: str
    direction: str | None
    price: float | None
    volume: int | None
    order_type: str
    reason: str
    evaluation_id: str
    action_evidence_id: str
    action_market_data_id: str
    clock_event_id: str
    tradability_id: str
    policy_sha256: str
    config_sha256: str
    adapter_sha256: str
    code_sha256: str
    evidence_schema_sha256: str
    action_business_sha256: str

    @classmethod
    def build(
        cls,
        *,
        instance: MiniQMTExecutionAlgoInstance,
        assignment: ParentQuoteControlAssignmentV1,
        action: VnpyAction,
        action_id: str,
        eligibility: ActionQuoteEligibility,
        evidence: MarketDataEvidenceV1,
    ) -> "B0QuoteV2ActionEnvelopeV1":
        revision = assignment.revision
        if revision is None or evidence.evaluation_id is None or evidence.evidence_id is None:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                "B0_QUOTE_V2 action envelope requires a frozen revision and action evidence identities",
            )
        business_payload = _action_business_payload(action)
        return cls(
            runtime_id=eligibility.runtime_id,
            binding_id=assignment.binding_id,
            trade_date=assignment.trade_date,
            parent_intent_id=assignment.parent_intent_id,
            algo_instance_id=instance.algo_instance_id,
            assignment_id=assignment.assignment_id,
            revision_id=revision.revision_id,
            action_id=action_id,
            action_type=action.action_type.value,
            direction=action.direction.value if action.direction is not None else None,
            price=float(action.price) if action.price is not None else None,
            volume=int(action.volume) if action.volume is not None else None,
            order_type=action.order_type.value,
            reason=str(action.reason or ""),
            evaluation_id=evidence.evaluation_id,
            action_evidence_id=evidence.evidence_id,
            action_market_data_id=evidence.market_data_id or "",
            clock_event_id=eligibility.clock_event_id,
            tradability_id=eligibility.tradability_id or "",
            policy_sha256=revision.quote_policy_sha256,
            config_sha256=eligibility.config_sha256,
            adapter_sha256=revision.adapter_sha256,
            code_sha256=revision.code_sha256,
            evidence_schema_sha256=revision.evidence_schema_sha256,
            action_business_sha256=canonical_sha256(business_payload),
        )

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": B0_QUOTE_V2_ACTION_ENVELOPE_SCHEMA_VERSION,
            "control_revision": ControlRevision.B0_QUOTE_V2.value,
            **asdict(self),
            "trade_date": self.trade_date.isoformat(),
        }


@dataclass
class _PendingB0QuoteV2Action:
    instance: MiniQMTExecutionAlgoInstance
    assignment: ParentQuoteControlAssignmentV1
    action: VnpyAction
    action_id: str
    envelope: B0QuoteV2ActionEnvelopeV1
    evidence: MarketDataEvidenceV1 | "_RecoveredActionEvidence"
    durable_receipt: DurableEvidenceReceipt | None = None


@dataclass(frozen=True)
class _RecoveredActionEvidence:
    evidence_id: str
    market_data_id: str | None
    evaluation_id: str


class B0QuoteV2Controller:
    """Per-runtime evidence-first coordinator; it never owns a broker gateway."""

    def __init__(
        self,
        *,
        runtime: B0QuoteV2RuntimePort,
        assignments: Mapping[str, ParentQuoteControlAssignmentV1],
        normalized_store: BoundedNormalizedQuoteStore,
        context_store: QuoteEvaluationContextStore,
        evidence_coordinator: QuoteEvidenceCoordinator,
        config: QuoteIngressRuntimeConfig,
        symbols: tuple[str, ...],
        evaluator: ActionQuoteEvaluator | None = None,
        release_callback: Callable[[str], None] | None = None,
    ) -> None:
        self.runtime = runtime
        self.runtime_id = str(runtime.config.runtime_id)
        self.assignments = dict(assignments)
        self.normalized_store = normalized_store
        self.context_store = context_store
        self.evidence_coordinator = evidence_coordinator
        self.config = config
        self.config_sha256 = quote_ingress_config_sha256(config)
        self.symbols = frozenset(symbols)
        self.evaluator = evaluator or ActionQuoteEvaluator()
        self._pending: dict[str, _PendingB0QuoteV2Action] = {}
        self._pending_by_algo: dict[str, str] = {}
        self._release_callback = release_callback
        self._closed = False
        self._duplicate_prevented_action_ids: set[str] = set()
        self._last_durable_to_submit_latency_ms: int | None = None
        self._recover_pending_actions()

    def observe(self, observation: NormalizedQuoteObservation) -> None:
        if observation.quote.symbol in self.symbols:
            self.evidence_coordinator.observe(observation)

    def lifecycle_tick(self, *, now_utc: datetime | None = None) -> dict[str, Any]:
        if self._closed:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                "closed B0_QUOTE_V2 controller cannot run a lifecycle tick",
                context={"runtime_id": self.runtime_id},
            )
        current_time = (now_utc or datetime.now(UTC)).astimezone(UTC)
        self.evidence_coordinator.drain_markouts(now_utc=current_time)
        receipts = self.evidence_coordinator.flush(now_utc=current_time)
        self._apply_receipts(receipts)
        for pending in tuple(self._pending.values()):
            self._resume_pending(pending, now_utc=current_time)
        for instance in self._active_instances():
            if instance.algo_instance_id in self._pending_by_algo:
                continue
            assignment = self._assignment_for(instance)
            evaluation, observation = self._evaluate(instance=instance, assignment=assignment)
            if evaluation.eligibility.state != EligibilityState.READY:
                self._persist_reject(
                    instance=instance,
                    assignment=assignment,
                    evaluation=evaluation,
                    observation=observation,
                    action_id=None,
                    now_utc=current_time,
                )
                continue
            if observation is None:
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_TICK_PROJECTION_INVALID,
                    "READY eligibility cannot be projected without its normalized observation",
                )
            tick = project_vnpy_tick(
                observation=observation,
                eligibility=evaluation.eligibility,
                assignment=assignment,
            )
            self.runtime.dispatch_b0_quote_v2_tick(instance=instance, tick=tick)
        final_receipts = self.evidence_coordinator.flush(now_utc=current_time)
        self._apply_receipts(final_receipts)
        for pending in tuple(self._pending.values()):
            self._resume_pending(pending, now_utc=current_time)
        return self.health()

    def handle_submit_action(self, *, instance: MiniQMTExecutionAlgoInstance, action: VnpyAction) -> None:
        if action.action_type != VnpyActionType.SUBMIT:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                "controller accepts only SUBMIT actions",
            )
        existing_action_id = self._pending_by_algo.get(instance.algo_instance_id)
        if existing_action_id is not None:
            existing = self._pending[existing_action_id]
            if existing.envelope.action_business_sha256 != canonical_sha256(_action_business_payload(action)):
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                    "one algo emitted a different SUBMIT while its prior action is pending",
                    context={"algo_instance_id": instance.algo_instance_id, "pending_action_id": existing_action_id},
                )
            return
        assignment = self._assignment_for(instance)
        evaluation, observation = self._evaluate(instance=instance, assignment=assignment)
        if evaluation.eligibility.state != EligibilityState.READY or observation is None:
            self._persist_reject(
                instance=instance,
                assignment=assignment,
                evaluation=evaluation,
                observation=observation,
                action_id=None,
                now_utc=evaluation.eligibility.evaluated_at_utc,
            )
            return
        ordinal = self._next_action_ordinal(instance.algo_instance_id)
        business_sha256 = canonical_sha256(_action_business_payload(action))
        action_id = "b0qact_" + canonical_sha256(
            {
                "runtime_id": self.runtime_id,
                "parent_intent_id": instance.parent_intent_id,
                "algo_instance_id": instance.algo_instance_id,
                "ordinal": ordinal,
                "action_business_sha256": business_sha256,
            }
        )
        evidence = self._action_evidence(
            instance=instance,
            assignment=assignment,
            evaluation=evaluation,
            observation=observation,
            action_id=action_id,
            capture_type=EvidenceCaptureType.ACTION_INPUT,
        )
        envelope = B0QuoteV2ActionEnvelopeV1.build(
            instance=instance,
            assignment=assignment,
            action=action,
            action_id=action_id,
            eligibility=evaluation.eligibility,
            evidence=evidence,
        )
        self.runtime.events.append(
            runtime_id=self.runtime_id,
            event_type=MiniQMTExecutionEventType.ALGO_ACTION_EMITTED,
            source="algo",
            payload={
                "schema_version": B0_QUOTE_V2_ACTION_PENDING_SCHEMA_VERSION,
                "b0_quote_v2_action": envelope.canonical_payload(),
                "vnpy_action": _action_payload(action),
                "action_evidence_candidate": _evidence_recovery_payload(evidence),
                "broker_called": False,
            },
        )
        pending = _PendingB0QuoteV2Action(
            instance=instance,
            assignment=assignment,
            action=action,
            action_id=action_id,
            envelope=envelope,
            evidence=evidence,
        )
        self._pending[action_id] = pending
        self._pending_by_algo[instance.algo_instance_id] = action_id
        self.evidence_coordinator.assert_action_gate_open(instance.symbol)
        self.evidence_coordinator.enqueue(
            evidence,
            event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED,
        )

    def close(self) -> None:
        if self._pending:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                "controller with pending actions cannot release its quote consumer",
                context={"runtime_id": self.runtime_id, "pending_action_ids": sorted(self._pending)},
            )
        self._closed = True
        if self._release_callback is not None:
            self._release_callback(self.runtime_id)

    def health(self) -> dict[str, Any]:
        evidence_health = asdict(self.evidence_coordinator.health())
        return {
            "schema_version": "b0_quote_v2_controller_health_v1",
            "runtime_id": self.runtime_id,
            "control_revision": ControlRevision.B0_QUOTE_V2.value,
            "status": "CLOSED" if self._closed else evidence_health["status"],
            "assignment_count": len(self.assignments),
            "pending_action_count": len(self._pending),
            "pending_action_ids": sorted(self._pending),
            "config_sha256": self.config_sha256,
            "b0_quote_v2_pending_actions": len(self._pending),
            "b0_quote_v2_duplicate_prevented_total": len(self._duplicate_prevented_action_ids),
            "b0_quote_v2_durable_to_submit_latency_ms": self._last_durable_to_submit_latency_ms,
            "evidence": evidence_health,
        }

    def _evaluate(
        self,
        *,
        instance: MiniQMTExecutionAlgoInstance,
        assignment: ParentQuoteControlAssignmentV1,
    ) -> tuple[EligibilityEvaluation, NormalizedQuoteObservation | None]:
        context = self.context_store.snapshot()
        if context is None:
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "B0_QUOTE_V2 controller requires scheduler-published context",
                context={"runtime_id": self.runtime_id, "symbol": instance.symbol},
            )
        revision = assignment.revision
        if revision is None or context.policy.policy_sha256 != revision.quote_policy_sha256:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "controller context policy differs from the frozen parent assignment",
                context={"runtime_id": self.runtime_id, "parent_intent_id": instance.parent_intent_id},
            )
        observation = self.normalized_store.get(instance.symbol, context_id=context.context_id)
        request = ActionQuoteRequest(
            runtime_id=self.runtime_id,
            parent_intent_id=instance.parent_intent_id,
            algo_instance_id=instance.algo_instance_id,
            symbol=instance.symbol,
            side=instance.side.value,
            control_revision=ControlRevision.B0_QUOTE_V2,
            policy_sha256=revision.quote_policy_sha256,
            config_sha256=self.config_sha256,
            adapter_sha256=revision.adapter_sha256,
        )
        return self.evaluator.evaluate(request=request, context=context, observation=observation), observation

    def _action_evidence(
        self,
        *,
        instance: MiniQMTExecutionAlgoInstance,
        assignment: ParentQuoteControlAssignmentV1,
        evaluation: EligibilityEvaluation,
        observation: NormalizedQuoteObservation | None,
        action_id: str | None,
        capture_type: EvidenceCaptureType,
    ) -> MarketDataEvidenceV1:
        eligibility = evaluation.eligibility
        context = self.context_store.snapshot()
        revision = assignment.revision
        if context is None or revision is None:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "action evidence cannot be built without frozen context and revision",
            )
        freshness = evaluation.freshness
        return MarketDataEvidenceV1(
            market_data_id=observation.market_data_id if observation is not None else None,
            evidence_schema_version=MARKET_DATA_EVIDENCE_SCHEMA_VERSION,
            capture_type=capture_type,
            runtime_id=self.runtime_id,
            binding_id=assignment.binding_id,
            trade_date=assignment.trade_date,
            parent_intent_id=assignment.parent_intent_id,
            child_order_id=None,
            action_id=action_id,
            quote=observation.quote if observation is not None else None,
            tradability=observation.tradability if observation is not None else None,
            clock_event_id=eligibility.clock_event_id,
            quality_reason_code=eligibility.reason_code,
            stage=eligibility.stage,
            control_revision=ControlRevision.B0_QUOTE_V2,
            policy_sha256=revision.quote_policy_sha256,
            config_sha256=self.config_sha256,
            adapter_sha256=revision.adapter_sha256,
            code_sha256=revision.code_sha256,
            schema_sha256=revision.evidence_schema_sha256,
            calendar_sha256=context.calendar_snapshot_set.set_sha256,
            captured_at_utc=eligibility.evaluated_at_utc,
            persisted_at_utc=None,
            quote_age_ms=freshness.wall_receive_age_ms if freshness is not None else None,
            source_lag_ms=freshness.source_lag_ms if freshness is not None else None,
            transport_lag_ms=(
                int(
                    (observation.quote.received_at_utc - observation.quote.source_exchange_time_utc).total_seconds()
                    * 1000
                )
                if observation is not None and observation.quote.source_exchange_time_utc is not None
                else None
            ),
            benchmark_policy_version=revision.benchmark_policy_version,
            mark_policy_version=revision.mark_policy_version,
            source_input_sha256=None,
            algo_instance_id=instance.algo_instance_id,
            evaluation_id=action_quote_evaluation_id(observation=observation, eligibility=eligibility),
            symbol=instance.symbol,
            side=instance.side.value,
            tradability_id=eligibility.tradability_id,
            eligibility_state=eligibility.state,
            exchange_age_ms=freshness.exchange_age_ms if freshness is not None else None,
            clock_age_divergence_ms=freshness.clock_age_divergence_ms if freshness is not None else None,
        )

    def _persist_reject(
        self,
        *,
        instance: MiniQMTExecutionAlgoInstance,
        assignment: ParentQuoteControlAssignmentV1,
        evaluation: EligibilityEvaluation,
        observation: NormalizedQuoteObservation | None,
        action_id: str | None,
        now_utc: datetime,
    ) -> None:
        evidence = self._action_evidence(
            instance=instance,
            assignment=assignment,
            evaluation=evaluation,
            observation=observation,
            action_id=action_id,
            capture_type=EvidenceCaptureType.ACTION_REJECT,
        )
        self.evidence_coordinator.enqueue(evidence, event_type=MiniQMTExecutionEventType.QUOTE_REJECTED)
        self.evidence_coordinator.flush(now_utc=now_utc)

    def _apply_receipts(self, receipts: tuple[DurableEvidenceReceipt, ...]) -> None:
        by_evidence_id = {
            str(receipt.event.payload.get("evidence", {}).get("evidence_id")): receipt
            for receipt in receipts
            if isinstance(receipt.event.payload.get("evidence"), dict)
        }
        for pending in self._pending.values():
            receipt = by_evidence_id.get(str(pending.evidence.evidence_id))
            if receipt is not None:
                if not receipt.durable_ack or not receipt.readback_verified:
                    raise quote_contract_error(
                        QuoteContractReasonCode.EVIDENCE_PERSIST_FAILED,
                        "action evidence receipt is not a verified durable acknowledgement",
                        context={"action_id": pending.action_id, "event_id": receipt.event.event_id},
                    )
                pending.durable_receipt = receipt

    def _resume_pending(self, pending: _PendingB0QuoteV2Action, *, now_utc: datetime) -> None:
        existing_children = [
            child
            for child in self.runtime.repository.list_child_orders(self.runtime_id, active_only=False)
            if child.child_order_id
            == _deterministic_child_id(self.runtime_id, pending.assignment.parent_intent_id, pending.action_id)
        ]
        if existing_children:
            self._duplicate_prevented_action_ids.add(pending.action_id)
            self._repair_child_receipt(pending=pending, child=existing_children[0], now_utc=now_utc)
            return
        if pending.durable_receipt is None:
            return
        child_id = _deterministic_child_id(self.runtime_id, pending.assignment.parent_intent_id, pending.action_id)
        child, child_event = self.runtime.submit_b0_quote_v2_child(
            instance=pending.instance,
            action=pending.action,
            child_order_id=child_id,
            metadata={
                "quote_control_assignment": pending.assignment.canonical_payload(),
                "b0_quote_v2_action": pending.envelope.canonical_payload(),
                "action_evidence_event_id": pending.durable_receipt.event.event_id,
                "action_evidence_id": pending.evidence.evidence_id,
                "action_market_data_id": pending.evidence.market_data_id,
                "evaluation_id": pending.evidence.evaluation_id,
            },
        )
        if pending.durable_receipt is not None:
            self._last_durable_to_submit_latency_ms = max(
                0,
                int((now_utc - pending.durable_receipt.persisted_at_utc).total_seconds() * 1000),
            )
        self._persist_child_receipt(
            pending=pending,
            child=child,
            child_event=child_event,
            now_utc=now_utc,
        )

    def _persist_child_receipt(
        self,
        *,
        pending: _PendingB0QuoteV2Action,
        child: MiniQMTChildOrder,
        child_event: MiniQMTExecutionEvent,
        now_utc: datetime,
    ) -> None:
        context = self.context_store.snapshot()
        revision = pending.assignment.revision
        if context is None or revision is None:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                "child receipt cannot be built without context and frozen revision",
            )
        receipt_observation = self.normalized_store.get(child.symbol, context_id=context.context_id)
        receipt = MarketDataEvidenceV1(
            market_data_id=receipt_observation.market_data_id if receipt_observation is not None else None,
            evidence_schema_version=MARKET_DATA_EVIDENCE_SCHEMA_VERSION,
            capture_type=EvidenceCaptureType.CHILD_RECEIPT,
            runtime_id=self.runtime_id,
            binding_id=pending.assignment.binding_id,
            trade_date=pending.assignment.trade_date,
            parent_intent_id=pending.assignment.parent_intent_id,
            child_order_id=child.child_order_id,
            action_id=pending.action_id,
            quote=receipt_observation.quote if receipt_observation is not None else None,
            tradability=receipt_observation.tradability if receipt_observation is not None else None,
            clock_event_id=context.clock.clock_event_id,
            quality_reason_code=None,
            stage=None,
            control_revision=ControlRevision.B0_QUOTE_V2,
            policy_sha256=revision.quote_policy_sha256,
            config_sha256=self.config_sha256,
            adapter_sha256=revision.adapter_sha256,
            code_sha256=revision.code_sha256,
            schema_sha256=revision.evidence_schema_sha256,
            calendar_sha256=context.calendar_snapshot_set.set_sha256,
            captured_at_utc=now_utc,
            persisted_at_utc=None,
            quote_age_ms=None,
            source_lag_ms=None,
            transport_lag_ms=None,
            benchmark_policy_version=revision.benchmark_policy_version,
            mark_policy_version=revision.mark_policy_version,
            source_input_sha256=None,
            algo_instance_id=child.algo_instance_id,
            source_child_event_id=child_event.event_id,
            broker_order_id=child.broker_order_id,
            symbol=child.symbol,
            side=child.side.value,
            anchor_market_data_id=pending.evidence.market_data_id,
            action_evidence_id=pending.evidence.evidence_id,
            tradability_id=receipt_observation.tradability.tradability_id
            if receipt_observation and receipt_observation.tradability
            else None,
        )
        self.evidence_coordinator.enqueue(receipt, event_type=MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED)
        receipts = self.evidence_coordinator.flush(now_utc=now_utc)
        if not any(
            item.durable_ack
            and item.readback_verified
            and item.event.payload.get("evidence", {}).get("evidence_id") == receipt.evidence_id
            for item in receipts
        ):
            return
        self.runtime.repository.upsert_child_order(
            child.model_copy(
                update={
                    "metadata": {
                        **dict(child.metadata),
                        "child_receipt_evidence_id": receipt.evidence_id,
                    }
                }
            )
        )
        self._pending.pop(pending.action_id, None)
        self._pending_by_algo.pop(pending.instance.algo_instance_id, None)

    def _repair_child_receipt(
        self,
        *,
        pending: _PendingB0QuoteV2Action,
        child: MiniQMTChildOrder,
        now_utc: datetime,
    ) -> None:
        child_events = [
            event
            for event in self.runtime.repository.list_events(self.runtime_id, include_archived=True)
            if event.event_type
            in {MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED, MiniQMTExecutionEventType.CHILD_ORDER_REJECTED}
            and event.payload.get("child_order_id") == child.child_order_id
        ]
        if not child_events:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                "durable child exists without its child event",
                context={"child_order_id": child.child_order_id},
            )
        receipts = self.runtime.repository.list_evidence_receipts(
            self.runtime_id,
            market_data_id=pending.evidence.market_data_id,
            include_archived=True,
        )
        if any(
            item.event.payload.get("evidence", {}).get("child_order_id") == child.child_order_id
            and item.event.payload.get("evidence", {}).get("capture_type") == EvidenceCaptureType.CHILD_RECEIPT.value
            for item in receipts
        ):
            self._pending.pop(pending.action_id, None)
            self._pending_by_algo.pop(pending.instance.algo_instance_id, None)
            return
        self._persist_child_receipt(
            pending=pending,
            child=child,
            child_event=child_events[-1],
            now_utc=now_utc,
        )

    def _recover_pending_actions(self) -> None:
        events = self.runtime.repository.list_events(self.runtime_id, include_archived=True)
        instances = {
            item.algo_instance_id: item
            for item in self.runtime.repository.list_algo_instances(self.runtime_id, active_only=False)
        }
        action_evidence_ids = {
            str(event.payload.get("b0_quote_v2_action", {}).get("action_evidence_id"))
            for event in events
            if event.event_type == MiniQMTExecutionEventType.ALGO_ACTION_EMITTED
            and isinstance(event.payload.get("b0_quote_v2_action"), dict)
            and event.payload["b0_quote_v2_action"].get("action_evidence_id")
        }
        evidence_receipts = tuple(
            receipt
            for evidence_id in sorted(action_evidence_ids)
            for receipt in self.runtime.repository.list_evidence_receipts(
                self.runtime_id,
                evidence_id=evidence_id,
                include_archived=True,
            )
        )
        receipt_by_evidence_id = {
            str(item.event.payload.get("evidence", {}).get("evidence_id")): item
            for item in evidence_receipts
            if isinstance(item.event.payload.get("evidence"), dict)
        }
        for event in events:
            if event.event_type != MiniQMTExecutionEventType.ALGO_ACTION_EMITTED:
                continue
            envelope_payload = event.payload.get("b0_quote_v2_action")
            action_payload = event.payload.get("vnpy_action")
            if not isinstance(envelope_payload, dict) or not isinstance(action_payload, dict):
                continue
            action_id = str(envelope_payload.get("action_id") or "")
            algo_instance_id = str(envelope_payload.get("algo_instance_id") or "")
            instance = instances.get(algo_instance_id)
            assignment = self.assignments.get(str(envelope_payload.get("parent_intent_id") or ""))
            if not action_id or instance is None or assignment is None:
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                    "pending action cannot be linked to its algo instance and assignment",
                    context={"event_id": event.event_id, "action_id": action_id},
                )
            evidence_id = str(envelope_payload.get("action_evidence_id") or "")
            evidence_receipt = receipt_by_evidence_id.get(evidence_id)
            candidate_payload = event.payload.get("action_evidence_candidate")
            if evidence_receipt is None and not isinstance(candidate_payload, dict):
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                    "pending action without durable acknowledgement is missing its immutable evidence candidate",
                    context={"action_id": action_id, "evidence_id": evidence_id},
                )
            if evidence_receipt is not None and not isinstance(evidence_receipt.event.payload.get("evidence"), dict):
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                    "pending action durable receipt is missing typed evidence",
                    context={"action_id": action_id, "evidence_id": evidence_id},
                )
            action = _action_from_payload(action_payload)
            evidence = (
                _evidence_from_receipt(evidence_receipt)
                if evidence_receipt is not None
                else _market_data_evidence_from_runtime_payload(candidate_payload)
            )
            envelope = _action_envelope_from_payload(envelope_payload)
            if evidence.evidence_id != evidence_id or evidence.market_data_id != envelope.action_market_data_id:
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                    "recovered action and evidence candidate identities conflict",
                    context={"action_id": action_id, "evidence_id": evidence_id},
                )
            pending = _PendingB0QuoteV2Action(
                instance=instance,
                assignment=assignment,
                action=action,
                action_id=action_id,
                envelope=envelope,
                evidence=evidence,
                durable_receipt=evidence_receipt,
            )
            existing = self._pending.get(action_id)
            if existing is not None and existing.envelope.canonical_payload() != envelope.canonical_payload():
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                    "recovered action id has conflicting canonical payloads",
                    context={"action_id": action_id},
                )
            self._pending[action_id] = pending
            self._pending_by_algo[algo_instance_id] = action_id
            if evidence_receipt is None:
                if not isinstance(evidence, MarketDataEvidenceV1):
                    raise quote_contract_error(
                        QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                        "undurable action recovery requires the full validated evidence contract",
                        context={"action_id": action_id},
                    )
                self.evidence_coordinator.enqueue(
                    evidence,
                    event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED,
                )

    def _assignment_for(self, instance: MiniQMTExecutionAlgoInstance) -> ParentQuoteControlAssignmentV1:
        assignment = self.assignments.get(instance.parent_intent_id)
        if assignment is None or assignment.control_revision != ControlRevision.B0_QUOTE_V2:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "runtime algo has no matching B0_QUOTE_V2 parent assignment",
                context={"runtime_id": self.runtime_id, "parent_intent_id": instance.parent_intent_id},
            )
        return assignment

    def build_trade_anchor_payload(
        self,
        *,
        child: MiniQMTChildOrder,
        quantity: int,
        price: float,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        assignment_payload = child.metadata.get("quote_control_assignment")
        action_payload = child.metadata.get("b0_quote_v2_action")
        if not isinstance(assignment_payload, Mapping) or not isinstance(action_payload, Mapping):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                "B0_QUOTE_V2 trade child is missing assignment or action evidence metadata",
                context={"child_order_id": child.child_order_id},
            )
        assignment = self.assignments.get(child.parent_intent_id)
        context = self.context_store.snapshot()
        observation = self.normalized_store.get(child.symbol, context_id=context.context_id if context else None)
        revision = assignment.revision if assignment is not None else None
        if assignment is None or revision is None or context is None or observation is None:
            raise quote_contract_error(
                QuoteContractReasonCode.MARKOUT_HISTORY_UNAVAILABLE,
                "B0_QUOTE_V2 trade anchor cannot prove assignment, context, and observation",
                context={"child_order_id": child.child_order_id, "symbol": child.symbol},
            )
        trade_id = _required_text(
            payload.get("trade_id") or payload.get("broker_trade_id") or payload.get("qmt_trade_id"),
            field_name="trade_id",
        )
        trade_time = _trade_time_utc(payload)
        segment_end = _continuous_segment_end_utc(context=context, symbol=child.symbol, trade_time_utc=trade_time)
        return {
            "schema_version": "miniqmt_quote_markout_anchor_v1",
            "control_revision": ControlRevision.B0_QUOTE_V2.value,
            "binding_id": assignment.binding_id,
            "trade_date": assignment.trade_date.isoformat(),
            "parent_intent_id": assignment.parent_intent_id,
            "algo_instance_id": child.algo_instance_id,
            "action_id": action_payload.get("action_id"),
            "child_order_id": child.child_order_id,
            "trade_id": trade_id,
            "action_evidence_id": _required_text(
                child.metadata.get("action_evidence_id"), field_name="action_evidence_id"
            ),
            "child_receipt_evidence_id": _required_text(
                child.metadata.get("child_receipt_evidence_id"), field_name="child_receipt_evidence_id"
            ),
            "anchor_market_data_id": _required_text(
                child.metadata.get("action_market_data_id"), field_name="anchor_market_data_id"
            ),
            "symbol": child.symbol,
            "side": child.side.value,
            "fill_price": float(price),
            "fill_quantity": int(quantity),
            "source_session_id": observation.quote.source_session_id,
            "ingress_generation": observation.quote.ingress_generation,
            "trade_time_utc": trade_time.isoformat(),
            "continuous_segment_end_utc": segment_end.isoformat(),
            "clock_event_id": context.clock.clock_event_id,
            "benchmark_policy_version": revision.benchmark_policy_version,
            "mark_policy_version": revision.mark_policy_version,
            "markout_max_lag_ms": revision.markout_max_lag_ms,
            "policy_sha256": revision.quote_policy_sha256,
            "config_sha256": self.config_sha256,
            "adapter_sha256": revision.adapter_sha256,
            "code_sha256": revision.code_sha256,
            "schema_sha256": revision.evidence_schema_sha256,
            "calendar_sha256": context.calendar_snapshot_set.set_sha256,
        }

    def schedule_trade_event(self, event: MiniQMTExecutionEvent) -> None:
        anchor = MarkoutAnchor.from_trade_event(event)
        if anchor is None:
            raise quote_contract_error(
                QuoteContractReasonCode.MARKOUT_HISTORY_UNAVAILABLE,
                "B0_QUOTE_V2 trade event is missing its markout anchor",
                context={"event_id": event.event_id, "runtime_id": self.runtime_id},
            )
        self.evidence_coordinator.schedule_markouts(anchor)

    def _active_instances(self) -> tuple[MiniQMTExecutionAlgoInstance, ...]:
        return tuple(self.runtime.repository.list_algo_instances(self.runtime_id, active_only=True))

    def _next_action_ordinal(self, algo_instance_id: str) -> int:
        return 1 + sum(
            event.event_type == MiniQMTExecutionEventType.ALGO_ACTION_EMITTED
            and isinstance(event.payload.get("b0_quote_v2_action"), dict)
            and event.payload["b0_quote_v2_action"].get("algo_instance_id") == algo_instance_id
            for event in self.runtime.repository.list_events(self.runtime_id, include_archived=True)
        )


class B0QuoteV2ControllerFactory:
    """Scheduler-owned construction root and `(data_session_key, runtime_id)` registry."""

    def __init__(
        self,
        *,
        supervisor: Any,
        config: QuoteIngressRuntimeConfig,
        data_session_key: str,
        context_release_callback: Callable[[str], None] | None = None,
    ) -> None:
        self.supervisor = supervisor
        self.config = config
        self.data_session_key = _required_text(data_session_key, field_name="data_session_key")
        self._controllers: dict[tuple[str, str], B0QuoteV2Controller] = {}
        self._assignment_conflict_count = 0
        self._accept_new_assignments = True
        self._invalid_revision_ids: set[str] = set()
        self._parity_violation_count = 0
        self._context_release_callback = context_release_callback

    def create(
        self,
        *,
        runtime: B0QuoteV2RuntimePort,
        assignments: Mapping[str, ParentQuoteControlAssignmentV1],
        symbols: tuple[str, ...],
        recovering_active: bool = False,
    ) -> B0QuoteV2Controller:
        key = (self.data_session_key, str(runtime.config.runtime_id))
        if key in self._controllers:
            self._assignment_conflict_count += 1
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "a runtime cannot construct a second B0_QUOTE_V2 controller",
                context={"data_session_key": self.data_session_key, "runtime_id": key[1]},
            )
        if not self._accept_new_assignments and not recovering_active:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 switch is disabled for new assignments; only durable active runtimes may drain",
                context={"data_session_key": self.data_session_key, "runtime_id": key[1]},
            )
        invalid_assignments = sorted(
            assignment.revision.revision_id
            for assignment in assignments.values()
            if assignment.revision is not None and assignment.revision.revision_id in self._invalid_revision_ids
        )
        if invalid_assignments:
            raise quote_contract_error(
                QuoteContractReasonCode.PARITY_VIOLATION,
                "B0_QUOTE_V2 revision was invalidated by a business-field parity violation",
                context={"runtime_id": key[1], "revision_ids": invalid_assignments, "legacy_fallback": False},
            )
        exact_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}))
        if not exact_symbols:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 controller requires the exact parent symbol set before lease acquisition",
                context={"runtime_id": key[1]},
            )
        coordinator = QuoteEvidenceCoordinator(repository=runtime.repository, config=self.config)
        controller = B0QuoteV2Controller(
            runtime=runtime,
            assignments=assignments,
            normalized_store=self.supervisor.normalized_store,
            context_store=self.supervisor.context_store,
            evidence_coordinator=coordinator,
            config=self.config,
            symbols=exact_symbols,
            release_callback=self.release,
        )
        consumer_id = f"b0qv2:{key[1]}"
        self.supervisor.register_observation_sink(consumer_id=consumer_id, sink=controller.observe)
        try:
            self.supervisor.acquire_consumer(
                consumer_id=consumer_id,
                symbols=list(exact_symbols),
            )
        except Exception:
            self.supervisor.unregister_observation_sink(consumer_id=consumer_id)
            raise
        self._controllers[key] = controller
        return controller

    def get(self, runtime_id: str) -> B0QuoteV2Controller | None:
        return self._controllers.get((self.data_session_key, str(runtime_id)))

    def set_accept_new_assignments(self, enabled: bool) -> None:
        """Apply the process switch without interrupting durable active runtimes."""

        self._accept_new_assignments = bool(enabled)

    def assert_accepts_new_assignments(self) -> None:
        """Fail before runtime construction when the process is drain-only."""

        if not self._accept_new_assignments:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 switch is disabled for new assignments; only durable active runtimes may drain",
                context={"data_session_key": self.data_session_key, "recovering_active": False},
            )

    def validate_revision_parity(
        self,
        *,
        revision_id: str,
        legacy_payload: Mapping[str, Any],
        v2_payload: Mapping[str, Any],
    ) -> None:
        exact_revision_id = _required_text(revision_id, field_name="revision_id")
        try:
            assert_b0_quote_v2_parity(legacy_payload=legacy_payload, v2_payload=v2_payload)
        except QuoteContractError as exc:
            if exc.reason_code != QuoteContractReasonCode.PARITY_VIOLATION:
                raise
            self._invalid_revision_ids.add(exact_revision_id)
            self._parity_violation_count += 1
            raise

    def release(self, runtime_id: str) -> None:
        key = (self.data_session_key, str(runtime_id))
        controller = self._controllers.pop(key, None)
        if controller is None:
            return
        consumer_id = f"b0qv2:{runtime_id}"
        self.supervisor.release_consumer(consumer_id=consumer_id)
        self.supervisor.unregister_observation_sink(consumer_id=consumer_id)
        if self._context_release_callback is not None:
            self._context_release_callback(str(runtime_id))

    def health(self) -> dict[str, Any]:
        return {
            "schema_version": "b0_quote_v2_controller_registry_v1",
            "data_session_key": self.data_session_key,
            "lifecycle_state": "ACTIVE" if self._accept_new_assignments else "DRAINING",
            "accept_new_assignments": self._accept_new_assignments,
            "controller_count": len(self._controllers),
            "b0_quote_v2_assignment_conflicts_total": self._assignment_conflict_count,
            "b0_quote_v2_parity_violations_total": self._parity_violation_count,
            "invalid_revision_ids": sorted(self._invalid_revision_ids),
            "controllers": {
                runtime_id: controller.health() for (_, runtime_id), controller in sorted(self._controllers.items())
            },
        }


def _action_payload(action: VnpyAction) -> dict[str, Any]:
    return {
        "action_type": action.action_type.value,
        "action_id": action.action_id,
        "vt_orderid": action.vt_orderid,
        "direction": action.direction.value if action.direction is not None else None,
        "price": action.price,
        "volume": action.volume,
        "order_type": action.order_type.value,
        "reason": action.reason,
        "metadata": dict(action.metadata),
    }


def _action_business_payload(action: VnpyAction) -> dict[str, Any]:
    payload = _action_payload(action)
    payload.pop("action_id", None)
    payload.pop("vt_orderid", None)
    return payload


def _deterministic_child_id(runtime_id: str, parent_intent_id: str, action_id: str) -> str:
    return "mqchild_" + canonical_sha256(
        {
            "schema": "b0_quote_v2_child_v1",
            "runtime_id": runtime_id,
            "parent_intent_id": parent_intent_id,
            "action_id": action_id,
        }
    )


def _action_from_payload(payload: Mapping[str, Any]) -> VnpyAction:
    from backend.execution_algos.vnpy_style.models import VnpyDirection, VnpyOrderType

    return VnpyAction(
        action_type=VnpyActionType(str(payload.get("action_type"))),
        action_id=str(payload.get("action_id") or ""),
        vt_orderid=str(payload["vt_orderid"]) if payload.get("vt_orderid") is not None else None,
        direction=VnpyDirection(str(payload["direction"])) if payload.get("direction") is not None else None,
        price=float(payload["price"]) if payload.get("price") is not None else None,
        volume=int(payload["volume"]) if payload.get("volume") is not None else None,
        order_type=VnpyOrderType(str(payload.get("order_type"))),
        reason=str(payload.get("reason") or ""),
        metadata=dict(payload.get("metadata") or {}),
    )


def _action_envelope_from_payload(payload: Mapping[str, Any]) -> B0QuoteV2ActionEnvelopeV1:
    expected = set(B0QuoteV2ActionEnvelopeV1.__dataclass_fields__) | {"schema_version", "control_revision"}
    received = {str(key) for key in payload}
    if (
        received != expected
        or payload.get("schema_version") != B0_QUOTE_V2_ACTION_ENVELOPE_SCHEMA_VERSION
        or payload.get("control_revision") != ControlRevision.B0_QUOTE_V2.value
    ):
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
            "recovered action envelope has non-exact schema fields",
            context={"unknown": sorted(received - expected), "missing": sorted(expected - received)},
        )
    values = {name: payload[name] for name in B0QuoteV2ActionEnvelopeV1.__dataclass_fields__}
    values["trade_date"] = date.fromisoformat(str(values["trade_date"]))
    return B0QuoteV2ActionEnvelopeV1(**values)


def _evidence_recovery_payload(evidence: MarketDataEvidenceV1) -> dict[str, Any]:
    payload = _recovery_json_value(
        {item.name: getattr(evidence, item.name) for item in fields(MarketDataEvidenceV1) if item.init}
    )
    return {
        "schema_version": "b0_quote_v2_evidence_recovery_v1",
        "evidence": payload,
        "evidence_sha256": evidence.evidence_sha256,
    }


def _recovery_json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
                "recovery payload cannot serialize a naive datetime",
            )
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if is_dataclass(value):
        return _recovery_json_value({item.name: getattr(value, item.name) for item in fields(value) if item.init})
    if isinstance(value, Mapping):
        return {
            str(key): _recovery_json_value(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        items = value if not isinstance(value, (set, frozenset)) else sorted(value, key=str)
        return [_recovery_json_value(item) for item in items]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise quote_contract_error(
        QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
        "recovery payload contains an unsupported value",
        context={"value_type": type(value).__name__},
    )


def _market_data_evidence_from_runtime_payload(payload: Mapping[str, Any]) -> MarketDataEvidenceV1:
    expected_keys = {"schema_version", "evidence", "evidence_sha256"}
    received_keys = {str(key) for key in payload}
    evidence_payload = payload.get("evidence")
    if (
        received_keys != expected_keys
        or payload.get("schema_version") != "b0_quote_v2_evidence_recovery_v1"
        or not isinstance(evidence_payload, Mapping)
    ):
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
            "pending action evidence recovery payload has a non-exact schema",
            context={
                "unknown": sorted(received_keys - expected_keys),
                "missing": sorted(expected_keys - received_keys),
            },
        )
    values = dict(evidence_payload)
    expected_evidence_fields = {item.name for item in fields(MarketDataEvidenceV1) if item.init}
    if set(values) != expected_evidence_fields:
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
            "pending action evidence candidate fields differ from MarketDataEvidenceV1",
            context={
                "unknown": sorted(set(values) - expected_evidence_fields),
                "missing": sorted(expected_evidence_fields - set(values)),
            },
        )
    quote_payload = values.get("quote")
    if quote_payload is not None:
        if not isinstance(quote_payload, Mapping):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT, "recovered quote must be an object"
            )
        quote_values = dict(quote_payload)
        quote_values["source_exchange_time_utc"] = _optional_utc_datetime(quote_values.get("source_exchange_time_utc"))
        quote_values["source_trade_date"] = _optional_date(quote_values.get("source_trade_date"))
        quote_values["clock_trade_date"] = _required_date(
            quote_values.get("clock_trade_date"), field_name="quote.clock_trade_date"
        )
        quote_values["received_at_utc"] = _required_utc_datetime(
            quote_values.get("received_at_utc"), field_name="quote.received_at_utc"
        )
        for field_name in ("bid_prices", "ask_prices", "bid_quantities_raw", "ask_quantities_raw"):
            raw_values = quote_values.get(field_name)
            if raw_values is not None:
                quote_values[field_name] = tuple(
                    Decimal(str(item)) if item is not None else None for item in raw_values
                )
        values["quote"] = FiveLevelQuote(**quote_values)
    tradability_payload = values.get("tradability")
    if tradability_payload is not None:
        if not isinstance(tradability_payload, Mapping):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT, "recovered tradability must be an object"
            )
        tradability_values = dict(tradability_payload)
        tradability_values["trade_date"] = _required_date(
            tradability_values.get("trade_date"), field_name="tradability.trade_date"
        )
        tradability_values["observed_at_utc"] = _required_utc_datetime(
            tradability_values.get("observed_at_utc"), field_name="tradability.observed_at_utc"
        )
        values["tradability"] = TradabilitySnapshot(**tradability_values)
    values["trade_date"] = _required_date(values.get("trade_date"), field_name="evidence.trade_date")
    for field_name in ("captured_at_utc", "persisted_at_utc", "target_time_utc", "cadence_window_start_utc"):
        values[field_name] = (
            _required_utc_datetime(values.get(field_name), field_name=f"evidence.{field_name}")
            if field_name == "captured_at_utc"
            else _optional_utc_datetime(values.get(field_name))
        )
    evidence = MarketDataEvidenceV1(**values)
    if evidence.evidence_sha256 != str(payload.get("evidence_sha256") or ""):
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
            "recovered MarketDataEvidenceV1 hash differs from the pending event",
            context={"evidence_id": evidence.evidence_id},
        )
    return evidence


def _required_date(value: Any, *, field_name: str) -> date:
    parsed = _optional_date(value)
    if parsed is None:
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT, f"{field_name} is required"
        )
    return parsed


def _optional_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT, "date field cannot be datetime"
        )
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT, "recovery date is invalid"
        ) from exc


def _required_utc_datetime(value: Any, *, field_name: str) -> datetime:
    parsed = _optional_utc_datetime(value)
    if parsed is None:
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT, f"{field_name} is required"
        )
    return parsed


def _optional_utc_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT, "recovery datetime is invalid"
            ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT, "recovery datetime must be timezone-aware"
        )
    return parsed.astimezone(UTC)


def _evidence_from_receipt(receipt: DurableEvidenceReceipt) -> _RecoveredActionEvidence:
    evidence_payload = receipt.event.payload.get("evidence")
    if not isinstance(evidence_payload, dict):
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT,
            "durable receipt is missing evidence payload",
        )
    evidence_id = _required_text(evidence_payload.get("evidence_id"), field_name="evidence_id")
    evaluation_id = _required_text(evidence_payload.get("evaluation_id"), field_name="evaluation_id")
    market_data_id = evidence_payload.get("market_data_id")
    return _RecoveredActionEvidence(
        evidence_id=evidence_id,
        market_data_id=str(market_data_id) if market_data_id is not None else None,
        evaluation_id=evaluation_id,
    )


def _trade_time_utc(payload: Mapping[str, Any]) -> datetime:
    raw = payload.get("trade_time_utc") or payload.get("trade_time") or payload.get("traded_at")
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            raise quote_contract_error(
                QuoteContractReasonCode.MARKOUT_HISTORY_UNAVAILABLE, "trade time must be timezone-aware"
            )
        return raw.astimezone(UTC)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.MARKOUT_HISTORY_UNAVAILABLE, "trade time is invalid"
            ) from exc
        if parsed.tzinfo is None:
            raise quote_contract_error(
                QuoteContractReasonCode.MARKOUT_HISTORY_UNAVAILABLE, "trade time must be timezone-aware"
            )
        return parsed.astimezone(UTC)
    raise quote_contract_error(
        QuoteContractReasonCode.MARKOUT_HISTORY_UNAVAILABLE, "trade time is required for markout anchor"
    )


def _continuous_segment_end_utc(
    *,
    context: Any,
    symbol: str,
    trade_time_utc: datetime,
) -> datetime:
    from backend.execution_algos.adaptive_is.contracts import exact_symbol

    market = exact_symbol(symbol)[1]
    calendar = context.calendar_snapshot_set.snapshot_by_market[market]
    local_time = trade_time_utc.astimezone(ZoneInfo(calendar.timezone))
    for segment in calendar.session_segments:
        if segment.start_local <= local_time.time() < segment.end_local:
            return datetime.combine(
                calendar.trade_date, segment.end_local, tzinfo=ZoneInfo(calendar.timezone)
            ).astimezone(UTC)
    raise quote_contract_error(
        QuoteContractReasonCode.MARKOUT_MARKET_SESSION_ENDED,
        "trade time is outside a registered continuous segment",
        context={"symbol": symbol, "trade_time_utc": trade_time_utc.isoformat()},
    )
