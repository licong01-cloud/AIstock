from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Mapping, Sequence

import pandas as pd

from backend.services.advisory_model_first.action_value_contracts import (
    AdvisoryActionInterventionSupportV1,
    AdvisoryActionRole,
    AdvisoryActionValueStatus,
    AdvisoryEvidenceLevel,
    AdvisoryIncrementalValueLabelV1,
    build_incremental_value_label,
    build_intervention_support,
)
from backend.services.advisory_model_first.entry_guard_decision import (
    AdvisoryEntryGuardDecisionV1,
    EntryGuardAction,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control_contracts import DecisionUse
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


POLICY_EPISODE_SIMULATOR_SHA256 = canonical_json_sha256(
    {
        "simulator": "build_policy_episode_labels+AdvisoryListTransitionEngine",
        "semantics": "ADVISORY_N2_PAIRED_ACTION_VALUE_V1",
    }
)


@dataclass(frozen=True)
class EntryIncrementalValueLabelResult:
    labels: tuple[AdvisoryIncrementalValueLabelV1, ...]
    frame: pd.DataFrame
    coverage: pd.DataFrame


def build_entry_incremental_value_labels(
    *,
    decisions: Sequence[AdvisoryEntryGuardDecisionV1],
    baseline_episode_labels: pd.DataFrame,
    baseline_policy_sha256: str,
    cost_policy_sha256: str,
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY,
    evidence_level: AdvisoryEvidenceLevel = AdvisoryEvidenceLevel.HISTORICAL_REPLAY,
) -> EntryIncrementalValueLabelResult:
    if evidence_level != AdvisoryEvidenceLevel.HISTORICAL_REPLAY:
        raise AdvisoryModelFirstError(
            "N2 Entry label builder only emits historical replay evidence",
            reason_code="ADVISORY_EVIDENCE_LEVEL_VIOLATION",
        )
    if not decisions:
        raise AdvisoryModelFirstError(
            "entry incremental labels require at least one decision",
            reason_code="ADVISORY_ENTRY_LABEL_PAIR_MISSING",
        )
    baseline = _normalize_baseline_labels(baseline_episode_labels)
    key_columns = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    duplicates = baseline.duplicated(key_columns, keep=False)
    if duplicates.any():
        raise AdvisoryModelFirstError(
            "entry baseline labels contain duplicate candidate identities",
            reason_code="ADVISORY_ENTRY_LABEL_PAIR_MISSING",
            context={"duplicate_count": int(duplicates.sum())},
        )
    rows_by_key = {
        (row.decision_as_of_trade_date.date(), row.target_trade_date.date(), row.instrument): row
        for row in baseline.itertuples(index=False)
    }
    output: list[AdvisoryIncrementalValueLabelV1] = []
    for decision in decisions:
        key = (decision.decision_date, decision.target_trade_date, decision.instrument)
        row = rows_by_key.get(key)
        if row is None:
            raise AdvisoryModelFirstError(
                "entry guard decision has no exact baseline episode label",
                reason_code="ADVISORY_ENTRY_LABEL_PAIR_MISSING",
                context={
                    "decision_date": decision.decision_date.isoformat(),
                    "target_trade_date": decision.target_trade_date.isoformat(),
                    "instrument": decision.instrument,
                },
            )
        _require_policy_identity(
            row=row,
            baseline_policy_sha256=baseline_policy_sha256,
            cost_policy_sha256=cost_policy_sha256,
        )
        output.append(
            _build_entry_label(
                decision=decision,
                row=row,
                baseline_policy_sha256=baseline_policy_sha256,
                cost_policy_sha256=cost_policy_sha256,
                decision_use=decision_use,
                evidence_level=evidence_level,
            )
        )
    frame = pd.DataFrame([item.model_dump(mode="python") for item in output])
    coverage = (
        frame.groupby(["intervention_action", "status"], observed=True, sort=True)
        .size()
        .rename("label_count")
        .reset_index()
    )
    return EntryIncrementalValueLabelResult(labels=tuple(output), frame=frame, coverage=coverage)


def build_intervention_support_from_labels(
    *,
    labels: Sequence[AdvisoryIncrementalValueLabelV1],
    intervention_policy_sha256: str,
    regimes_by_decision_date: Mapping[date, str],
    required_regimes: Sequence[str],
    minimum_intervention_count: int,
    minimum_intervention_day_fraction: float,
    minimum_days_per_required_regime: int,
    block_length_trading_days: int,
    minimum_effective_intervention_block_count: int,
) -> AdvisoryActionInterventionSupportV1:
    if block_length_trading_days < 1:
        raise AdvisoryModelFirstError(
            "intervention support block length must be positive",
            reason_code="ADVISORY_ACTION_VALUE_POLICY_MISMATCH",
        )
    selected = [item for item in labels if item.intervention_policy_sha256 == intervention_policy_sha256]
    if not selected:
        raise AdvisoryModelFirstError(
            "intervention support has no labels for the declared policy",
            reason_code="ADVISORY_ACTION_VALUE_POLICY_MISMATCH",
        )
    roles = {item.role for item in selected}
    if len(roles) != 1:
        raise AdvisoryModelFirstError(
            "intervention support cannot mix action roles",
            reason_code="ADVISORY_ACTION_VALUE_POLICY_MISMATCH",
        )
    decision_days = {item.decision_date for item in selected}
    intervention_items = [item for item in selected if _is_real_intervention(item)]
    intervention_days = {item.decision_date for item in intervention_items}
    regime_counts: dict[str, int] = {}
    for value in intervention_days:
        regime = regimes_by_decision_date.get(value)
        if regime is None:
            continue
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
    day_fraction = len(intervention_days) / len(decision_days)
    effective_blocks = math.ceil(len(intervention_days) / block_length_trading_days)
    return build_intervention_support(
        role=next(iter(roles)),
        intervention_policy_sha256=intervention_policy_sha256,
        total_decision_count=len(selected),
        intervention_count=len(intervention_items),
        decision_day_count=len(decision_days),
        intervention_day_count=len(intervention_days),
        intervention_day_fraction=day_fraction,
        intervention_days_by_regime=regime_counts,
        required_regimes=tuple(required_regimes),
        minimum_intervention_count=minimum_intervention_count,
        minimum_intervention_day_fraction=minimum_intervention_day_fraction,
        minimum_days_per_required_regime=minimum_days_per_required_regime,
        block_length_trading_days=block_length_trading_days,
        effective_intervention_block_count=effective_blocks,
        minimum_effective_intervention_block_count=minimum_effective_intervention_block_count,
    )


def _build_entry_label(
    *,
    decision: AdvisoryEntryGuardDecisionV1,
    row: object,
    baseline_policy_sha256: str,
    cost_policy_sha256: str,
    decision_use: DecisionUse,
    evidence_level: AdvisoryEvidenceLevel,
) -> AdvisoryIncrementalValueLabelV1:
    baseline_status = str(getattr(row, "label_status"))
    baseline_value = _finite(getattr(row, "net_return_bps", None))
    information_end = _as_date(getattr(row, "label_information_end", None)) or decision.target_trade_date
    episode_id = str(getattr(row, "episode_label_id"))
    common = {
        "role": AdvisoryActionRole.ENTRY_GUARD,
        "decision_use": decision_use,
        "evidence_level": evidence_level,
        "sealed_holdout_accessed": evidence_level == AdvisoryEvidenceLevel.SEALED_HOLDOUT_CONFIRMATION,
        "decision_date": decision.decision_date,
        "target_action_date": decision.target_trade_date,
        "effective_action_date": (None if decision.action == EntryGuardAction.WAITING else decision.target_trade_date),
        "instrument": decision.instrument,
        "episode_id": episode_id,
        "baseline_action": "ENTER",
        "intervention_action": decision.action.value,
        "baseline_policy_sha256": baseline_policy_sha256,
        "intervention_policy_sha256": decision.policy_sha256,
        "cost_policy_sha256": cost_policy_sha256,
        "shadow_simulator_sha256": POLICY_EPISODE_SIMULATOR_SHA256,
        "information_start": decision.decision_date,
        "information_end": information_end,
    }
    if baseline_status != "MATURED" or baseline_value is None:
        return build_incremental_value_label(
            **common,
            status=AdvisoryActionValueStatus.BASELINE_UNAVAILABLE,
            baseline_net_value_bps=None,
            action_net_value_bps=None,
            incremental_net_value_bps=None,
            reason_code=f"BASELINE_{baseline_status}",
        )
    if decision.action == EntryGuardAction.ACCEPT:
        return build_incremental_value_label(
            **common,
            status=AdvisoryActionValueStatus.AVAILABLE,
            baseline_net_value_bps=baseline_value,
            action_net_value_bps=baseline_value,
            incremental_net_value_bps=0.0,
            reason_code=decision.reason_code,
        )
    if decision.action == EntryGuardAction.SKIP:
        return build_incremental_value_label(
            **common,
            status=AdvisoryActionValueStatus.AVAILABLE,
            baseline_net_value_bps=baseline_value,
            action_net_value_bps=0.0,
            incremental_net_value_bps=-baseline_value,
            reason_code=decision.reason_code,
        )
    if decision.action == EntryGuardAction.REDUCE:
        return build_incremental_value_label(
            **common,
            status=AdvisoryActionValueStatus.NON_NUMERIC_ADVICE_ONLY,
            baseline_net_value_bps=None,
            action_net_value_bps=None,
            incremental_net_value_bps=None,
            reason_code="REDUCE_HAS_NO_POSITION_SIZE_SEMANTICS",
        )
    return build_incremental_value_label(
        **common,
        status=AdvisoryActionValueStatus.WAITING,
        baseline_net_value_bps=None,
        action_net_value_bps=None,
        incremental_net_value_bps=None,
        reason_code=decision.reason_code,
    )


def _normalize_baseline_labels(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "episode_label_id",
        "shadow_policy_sha256",
        "cost_policy_sha256",
        "label_status",
        "label_information_end",
        "net_return_bps",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise AdvisoryModelFirstError(
            "entry baseline labels omit required columns",
            reason_code="ADVISORY_ENTRY_LABEL_PAIR_MISSING",
            context={"missing_columns": missing},
        )
    output = frame.copy()
    output["decision_as_of_trade_date"] = pd.to_datetime(output["decision_as_of_trade_date"]).dt.normalize()
    output["target_trade_date"] = pd.to_datetime(output["target_trade_date"]).dt.normalize()
    output["instrument"] = output["instrument"].astype(str).str.strip().str.upper()
    return output


def _require_policy_identity(*, row: object, baseline_policy_sha256: str, cost_policy_sha256: str) -> None:
    if str(getattr(row, "shadow_policy_sha256")) != baseline_policy_sha256:
        raise AdvisoryModelFirstError(
            "entry baseline policy hash differs from the declared baseline",
            reason_code="ADVISORY_ACTION_VALUE_POLICY_MISMATCH",
        )
    if str(getattr(row, "cost_policy_sha256")) != cost_policy_sha256:
        raise AdvisoryModelFirstError(
            "entry baseline cost policy hash differs from the declared cost policy",
            reason_code="ADVISORY_ACTION_VALUE_POLICY_MISMATCH",
        )


def _as_date(value: object) -> date | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).date()


def _finite(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _is_real_intervention(item: AdvisoryIncrementalValueLabelV1) -> bool:
    if item.status != AdvisoryActionValueStatus.AVAILABLE:
        return False
    if item.role == AdvisoryActionRole.ENTRY_GUARD:
        return item.intervention_action == "SKIP"
    return (
        item.intervention_action == "EXIT_NEXT_OPEN"
        and item.incremental_net_value_bps is not None
        and item.incremental_net_value_bps > 0.0
    )
