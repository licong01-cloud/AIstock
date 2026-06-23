"""Phase 6 per portfolio/strategy-slot gray switch controller.

The controller is deliberately policy-only: it resolves which runtime kind a
portfolio/slot may use, audits switch/rollback decisions as durable runtime
events, and refuses unsafe cuts loudly. It does not start services or place
orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field

from .config import (
    MINIQMT_EXECUTION_RUNTIME_ENV,
    MiniQMTExecutionRuntimeKind,
    get_miniqmt_execution_runtime_kind,
)
from .models import (
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeMode,
    MiniQMTExecutionRuntimeRecord,
    MiniQMTExecutionRuntimeState,
)
from .repository import MiniQMTExecutionRuntimeRepository

_GRAY_SCOPE_METADATA_KEY = "gray_runtime_overrides"
_LAST_GRAY_DECISION_METADATA_KEY = "last_gray_runtime_decision"
_LAST_SHADOW_METADATA_KEY = "last_shadow_reconciliation"
_DEFAULT_RUNTIME_CONFIG_HASH = "miniqmt_gray_switch"


class MiniQMTGrayDecisionType(str, Enum):
    SWITCH = "switch"
    ROLLBACK = "rollback"


class MiniQMTGrayDecisionStatus(str, Enum):
    APPLIED = "APPLIED"
    BLOCKED = "BLOCKED"


class MiniQMTGrayDecision(BaseModel):
    """Auditable Phase 6 gray/canary switch decision."""

    model_config = ConfigDict(extra="forbid")

    decision_id: str
    runtime_id: str
    portfolio_id: str
    strategy_slot_id: str
    decision_type: MiniQMTGrayDecisionType
    status: MiniQMTGrayDecisionStatus
    runtime_kind: MiniQMTExecutionRuntimeKind
    previous_runtime_kind: MiniQMTExecutionRuntimeKind
    reason_code: str
    reason: str
    mode: MiniQMTExecutionRuntimeMode
    shadow_report_id: str | None = None
    shadow_event_id: str | None = None
    active_child_order_ids: list[str] = Field(default_factory=list)
    active_algo_instance_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    decided_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @property
    def applied(self) -> bool:
        return self.status == MiniQMTGrayDecisionStatus.APPLIED


@dataclass(frozen=True)
class MiniQMTGraySwitchController:
    """Durable gray switch policy for one MiniQMT runtime repository."""

    repository: MiniQMTExecutionRuntimeRepository

    def resolve_runtime_kind(
        self,
        *,
        portfolio_id: str,
        strategy_slot_id: str,
        runtime_id: str | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> MiniQMTExecutionRuntimeKind:
        """Resolve global default plus per-scope override, defaulting to compiler."""

        portfolio_id = _required_scope_text(portfolio_id, "portfolio_id")
        strategy_slot_id = _required_scope_text(strategy_slot_id, "strategy_slot_id")
        base_kind = get_miniqmt_execution_runtime_kind(environ)
        if runtime_id is None:
            return base_kind
        runtime = self.repository.get_runtime(runtime_id)
        if runtime is None:
            return base_kind
        raw_scope = _runtime_scope_overrides(runtime).get(_scope_key(portfolio_id, strategy_slot_id))
        if raw_scope is None:
            return base_kind
        try:
            return MiniQMTExecutionRuntimeKind(str(raw_scope))
        except ValueError as exc:
            raise RuntimeError(
                "MiniQMT gray runtime override is invalid; "
                f"reason_code=MINIQMT_GRAY_RUNTIME_OVERRIDE_INVALID, runtime_id={runtime_id}, "
                f"portfolio_id={portfolio_id}, strategy_slot_id={strategy_slot_id}, raw_runtime_kind={raw_scope!r}"
            ) from exc

    def switch_to_event_loop(
        self,
        *,
        runtime_id: str,
        portfolio_id: str,
        strategy_slot_id: str,
        mode: MiniQMTExecutionRuntimeMode | str = MiniQMTExecutionRuntimeMode.SIM,
        trade_date: date | None = None,
        account_group_id: str | None = None,
        reason: str = "phase6_canary_switch",
        metadata: dict[str, Any] | None = None,
    ) -> MiniQMTGrayDecision:
        """Apply a canary switch only after durable no-fatal shadow evidence."""

        return self._decide(
            decision_type=MiniQMTGrayDecisionType.SWITCH,
            runtime_id=runtime_id,
            portfolio_id=portfolio_id,
            strategy_slot_id=strategy_slot_id,
            target_kind=MiniQMTExecutionRuntimeKind.EVENT_LOOP,
            mode=mode,
            trade_date=trade_date,
            account_group_id=account_group_id,
            reason=reason,
            metadata=metadata,
            require_shadow_evidence=True,
            require_no_in_flight=True,
        )

    def rollback_to_compiler(
        self,
        *,
        runtime_id: str,
        portfolio_id: str,
        strategy_slot_id: str,
        mode: MiniQMTExecutionRuntimeMode | str = MiniQMTExecutionRuntimeMode.SIM,
        trade_date: date | None = None,
        account_group_id: str | None = None,
        reason: str = "phase6_one_click_rollback",
        metadata: dict[str, Any] | None = None,
    ) -> MiniQMTGrayDecision:
        """Rollback a gray-switched slot to compiler with the same audit rules."""

        return self._decide(
            decision_type=MiniQMTGrayDecisionType.ROLLBACK,
            runtime_id=runtime_id,
            portfolio_id=portfolio_id,
            strategy_slot_id=strategy_slot_id,
            target_kind=MiniQMTExecutionRuntimeKind.COMPILER,
            mode=mode,
            trade_date=trade_date,
            account_group_id=account_group_id,
            reason=reason,
            metadata=metadata,
            require_shadow_evidence=False,
            require_no_in_flight=True,
        )

    def _decide(
        self,
        *,
        decision_type: MiniQMTGrayDecisionType,
        runtime_id: str,
        portfolio_id: str,
        strategy_slot_id: str,
        target_kind: MiniQMTExecutionRuntimeKind,
        mode: MiniQMTExecutionRuntimeMode | str,
        trade_date: date | None,
        account_group_id: str | None,
        reason: str,
        metadata: dict[str, Any] | None,
        require_shadow_evidence: bool,
        require_no_in_flight: bool,
    ) -> MiniQMTGrayDecision:
        runtime_id = _required_scope_text(runtime_id, "runtime_id")
        portfolio_id = _required_scope_text(portfolio_id, "portfolio_id")
        strategy_slot_id = _required_scope_text(strategy_slot_id, "strategy_slot_id")
        mode_value = mode if isinstance(mode, MiniQMTExecutionRuntimeMode) else MiniQMTExecutionRuntimeMode(str(mode).upper())
        runtime = _ensure_runtime(
            self.repository,
            runtime_id=runtime_id,
            account_group_id=account_group_id or portfolio_id,
            trade_date=trade_date or datetime.now(UTC).date(),
            mode=mode_value,
        )
        effective_mode = mode_value if mode_value != MiniQMTExecutionRuntimeMode.SIM else runtime.mode
        previous_kind = self.resolve_runtime_kind(
            portfolio_id=portfolio_id,
            strategy_slot_id=strategy_slot_id,
            runtime_id=runtime_id,
            environ={MINIQMT_EXECUTION_RUNTIME_ENV: MiniQMTExecutionRuntimeKind.COMPILER.value},
        )
        evidence = _shadow_evidence_for_scope(runtime, portfolio_id=portfolio_id, strategy_slot_id=strategy_slot_id)
        active_child_order_ids = [
            child.child_order_id
            for child in self.repository.list_child_orders(runtime_id, active_only=True)
            if child.strategy_slot_id == strategy_slot_id
            and child.status
            not in {
                MiniQMTChildOrderStatus.FILLED,
                MiniQMTChildOrderStatus.CANCELLED,
                MiniQMTChildOrderStatus.REJECTED,
            }
        ]
        active_algo_instance_ids = [
            instance.algo_instance_id
            for instance in self.repository.list_algo_instances(runtime_id, active_only=True)
            if instance.strategy_slot_id == strategy_slot_id and instance.status == MiniQMTAlgoInstanceStatus.ACTIVE
        ]
        rejection = _first_rejection_reason(
            decision_type=decision_type,
            mode=effective_mode,
            evidence=evidence,
            active_child_order_ids=active_child_order_ids,
            active_algo_instance_ids=active_algo_instance_ids,
            require_shadow_evidence=require_shadow_evidence,
            require_no_in_flight=require_no_in_flight,
        )
        if rejection is not None:
            reason_code, rejection_reason = rejection
            decision = self._record_decision(
                runtime=runtime,
                portfolio_id=portfolio_id,
                strategy_slot_id=strategy_slot_id,
                decision_type=decision_type,
                status=MiniQMTGrayDecisionStatus.BLOCKED,
                target_kind=previous_kind,
                previous_kind=previous_kind,
                reason_code=reason_code,
                reason=rejection_reason,
                mode=effective_mode,
                shadow_report=evidence.report,
                active_child_order_ids=active_child_order_ids,
                active_algo_instance_ids=active_algo_instance_ids,
                metadata={**dict(metadata or {}), "requested_reason": reason},
            )
            raise RuntimeError(_decision_error_message(decision))

        decision = self._record_decision(
            runtime=runtime,
            portfolio_id=portfolio_id,
            strategy_slot_id=strategy_slot_id,
            decision_type=decision_type,
            status=MiniQMTGrayDecisionStatus.APPLIED,
            target_kind=target_kind,
            previous_kind=previous_kind,
            reason_code=(
                "MINIQMT_GRAY_SWITCH_APPLIED"
                if decision_type == MiniQMTGrayDecisionType.SWITCH
                else "MINIQMT_GRAY_ROLLBACK_APPLIED"
            ),
            reason=reason,
            mode=effective_mode,
            shadow_report=evidence.report,
            active_child_order_ids=active_child_order_ids,
            active_algo_instance_ids=active_algo_instance_ids,
            metadata=dict(metadata or {}),
        )
        return decision

    def _record_decision(
        self,
        *,
        runtime: MiniQMTExecutionRuntimeRecord,
        portfolio_id: str,
        strategy_slot_id: str,
        decision_type: MiniQMTGrayDecisionType,
        status: MiniQMTGrayDecisionStatus,
        target_kind: MiniQMTExecutionRuntimeKind,
        previous_kind: MiniQMTExecutionRuntimeKind,
        reason_code: str,
        reason: str,
        mode: MiniQMTExecutionRuntimeMode,
        shadow_report: dict[str, Any] | None,
        active_child_order_ids: list[str],
        active_algo_instance_ids: list[str],
        metadata: dict[str, Any],
    ) -> MiniQMTGrayDecision:
        decision = MiniQMTGrayDecision(
            decision_id=f"mqrt_gray_{_short_hash([runtime.runtime_id, portfolio_id, strategy_slot_id, decision_type.value, reason_code])}",
            runtime_id=runtime.runtime_id,
            portfolio_id=portfolio_id,
            strategy_slot_id=strategy_slot_id,
            decision_type=decision_type,
            status=status,
            runtime_kind=target_kind,
            previous_runtime_kind=previous_kind,
            reason_code=reason_code,
            reason=reason,
            mode=mode,
            shadow_report_id=str((shadow_report or {}).get("report_id") or "") or None,
            shadow_event_id=str((shadow_report or {}).get("durable_event_id") or "") or None,
            active_child_order_ids=list(active_child_order_ids),
            active_algo_instance_ids=list(active_algo_instance_ids),
            metadata=dict(metadata),
        )
        event = self.repository.append_event(
            MiniQMTExecutionEvent(
                runtime_id=runtime.runtime_id,
                sequence=self.repository.next_event_sequence(runtime.runtime_id),
                event_type=_event_type_for_decision(decision_type, status),
                source="runtime",
                payload=decision.model_dump(mode="json"),
            )
        )
        stored_decision = decision.model_copy(update={"metadata": {**decision.metadata, "audit_event_id": event.event_id}})
        latest_runtime = self.repository.get_runtime(runtime.runtime_id) or runtime
        scope_overrides = dict(_runtime_scope_overrides(latest_runtime))
        if status == MiniQMTGrayDecisionStatus.APPLIED:
            if target_kind == MiniQMTExecutionRuntimeKind.COMPILER:
                scope_overrides.pop(_scope_key(portfolio_id, strategy_slot_id), None)
            else:
                scope_overrides[_scope_key(portfolio_id, strategy_slot_id)] = target_kind.value
        self.repository.upsert_runtime(
            latest_runtime.model_copy(
                update={
                    "metadata": {
                        **dict(latest_runtime.metadata),
                        _GRAY_SCOPE_METADATA_KEY: scope_overrides,
                        _LAST_GRAY_DECISION_METADATA_KEY: stored_decision.model_dump(mode="json"),
                    },
                }
            )
        )
        return stored_decision


@dataclass(frozen=True)
class _ShadowEvidence:
    report: dict[str, Any] | None
    missing: bool
    fatal: bool
    scope_mismatch: bool


def _shadow_evidence_for_scope(
    runtime: MiniQMTExecutionRuntimeRecord,
    *,
    portfolio_id: str,
    strategy_slot_id: str,
) -> _ShadowEvidence:
    raw_report = runtime.metadata.get(_LAST_SHADOW_METADATA_KEY)
    if not isinstance(raw_report, dict):
        return _ShadowEvidence(report=None, missing=True, fatal=False, scope_mismatch=False)
    metadata = raw_report.get("metadata")
    report_metadata = dict(metadata) if isinstance(metadata, dict) else {}
    scope_ok = (
        str(report_metadata.get("portfolio_id") or "").strip() == portfolio_id
        and str(report_metadata.get("strategy_slot_id") or "").strip() == strategy_slot_id
    )
    differences = raw_report.get("differences")
    fatal = any(
        isinstance(item, dict) and str(item.get("severity") or "").upper() == "FATAL"
        for item in (differences if isinstance(differences, list) else [])
    )
    return _ShadowEvidence(report=dict(raw_report), missing=False, fatal=fatal, scope_mismatch=not scope_ok)


def _first_rejection_reason(
    *,
    decision_type: MiniQMTGrayDecisionType,
    mode: MiniQMTExecutionRuntimeMode,
    evidence: _ShadowEvidence,
    active_child_order_ids: list[str],
    active_algo_instance_ids: list[str],
    require_shadow_evidence: bool,
    require_no_in_flight: bool,
) -> tuple[str, str] | None:
    if mode != MiniQMTExecutionRuntimeMode.SIM:
        return "MINIQMT_GRAY_LIVE_FORBIDDEN", "MiniQMT gray switch is SIM-only until live admission gates pass"
    if require_shadow_evidence:
        if evidence.missing:
            return (
                "MINIQMT_GRAY_SHADOW_EVIDENCE_MISSING",
                "MiniQMT gray switch requires durable no-fatal shadow evidence for this portfolio/strategy slot",
            )
        if evidence.scope_mismatch:
            return (
                "MINIQMT_GRAY_SHADOW_SCOPE_MISMATCH",
                "MiniQMT gray switch shadow evidence belongs to a different portfolio/strategy slot",
            )
        if evidence.fatal:
            return "MINIQMT_GRAY_SHADOW_EVIDENCE_FATAL", "MiniQMT gray switch blocked by fatal A/B shadow drift"
    if require_no_in_flight and (active_child_order_ids or active_algo_instance_ids):
        return (
            "MINIQMT_GRAY_IN_FLIGHT_AMBIGUOUS",
            (
                "MiniQMT gray "
                f"{decision_type.value} requires no in-flight child orders or algo instances; "
                "run operator cancel/reset first"
            ),
        )
    return None


def _ensure_runtime(
    repository: MiniQMTExecutionRuntimeRepository,
    *,
    runtime_id: str,
    account_group_id: str,
    trade_date: date,
    mode: MiniQMTExecutionRuntimeMode,
) -> MiniQMTExecutionRuntimeRecord:
    runtime = repository.get_runtime(runtime_id)
    if runtime is not None:
        return runtime
    return repository.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            runtime_id=runtime_id,
            account_group_id=account_group_id,
            trade_date=trade_date,
            mode=mode,
            event_loop_state=MiniQMTExecutionRuntimeState.READY,
            runtime_config_hash=_DEFAULT_RUNTIME_CONFIG_HASH,
            metadata={},
        )
    )


def _runtime_scope_overrides(runtime: MiniQMTExecutionRuntimeRecord) -> dict[str, str]:
    raw = runtime.metadata.get(_GRAY_SCOPE_METADATA_KEY)
    if not isinstance(raw, dict):
        return {}
    return {str(key): str(value) for key, value in raw.items()}


def _event_type_for_decision(
    decision_type: MiniQMTGrayDecisionType,
    status: MiniQMTGrayDecisionStatus,
) -> MiniQMTExecutionEventType:
    if decision_type == MiniQMTGrayDecisionType.SWITCH:
        return (
            MiniQMTExecutionEventType.GRAY_SWITCH_APPLIED
            if status == MiniQMTGrayDecisionStatus.APPLIED
            else MiniQMTExecutionEventType.GRAY_SWITCH_REJECTED
        )
    return (
        MiniQMTExecutionEventType.GRAY_ROLLBACK_APPLIED
        if status == MiniQMTGrayDecisionStatus.APPLIED
        else MiniQMTExecutionEventType.GRAY_ROLLBACK_REJECTED
    )


def _decision_error_message(decision: MiniQMTGrayDecision) -> str:
    return (
        "MiniQMT gray runtime decision rejected; "
        f"reason_code={decision.reason_code}, runtime_id={decision.runtime_id}, "
        f"portfolio_id={decision.portfolio_id}, strategy_slot_id={decision.strategy_slot_id}, "
        f"decision_type={decision.decision_type.value}"
    )


def _required_scope_text(value: str, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required for MiniQMT gray runtime control")
    return text


def _scope_key(portfolio_id: str, strategy_slot_id: str) -> str:
    return f"{portfolio_id}::{strategy_slot_id}"


def _short_hash(parts: list[Any]) -> str:
    import hashlib
    import json

    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
