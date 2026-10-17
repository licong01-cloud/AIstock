from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Literal, Sequence

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_list_transition import AdvisoryTransitionPolicyV1
from backend.services.advisory_model_first.action_value_contracts import (
    AdvisoryActionRole,
    AdvisoryActionValueStatus,
    AdvisoryEvidenceLevel,
    AdvisoryIncrementalValueLabelV1,
    build_incremental_value_label,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.incremental_value_labels import (
    POLICY_EPISODE_SIMULATOR_SHA256,
)
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.advisory_model_first.policy_episode_labels import (
    PolicyEpisodeLabelResult,
    build_policy_episode_labels,
    is_one_price_limit_down,
)
from backend.services.advisory_model_first.research_control_contracts import DecisionUse
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"


class ExitOracleAction(str, Enum):
    HOLD = "HOLD"
    EXIT_NEXT_OPEN = "EXIT_NEXT_OPEN"
    WAITING = "WAITING"


class ExitOracleExecutionState(str, Enum):
    BASELINE_CONTINUE = "BASELINE_CONTINUE"
    EXECUTED_NEXT_OPEN = "EXECUTED_NEXT_OPEN"
    DEFERRED_TO_FIRST_EXECUTABLE = "DEFERRED_TO_FIRST_EXECUTABLE"
    CENSORED = "CENSORED"
    DATA_UNAVAILABLE = "DATA_UNAVAILABLE"
    BASELINE_UNAVAILABLE = "BASELINE_UNAVAILABLE"


class AdvisoryExitDecisionV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_exit_decision_v1"] = "advisory_exit_decision_v1"
    decision_sha256: str = Field(pattern=SHA256_PATTERN)
    decision_date: date
    target_action_date: date
    effective_action_date: date | None = None
    instrument: str = Field(min_length=1)
    episode_id: str = Field(min_length=1)
    action: ExitOracleAction
    execution_state: ExitOracleExecutionState
    reason_code: str = Field(min_length=1)
    deferred_trading_days: int = Field(ge=0)
    incremental_label_id: str = Field(pattern=r"^advincr_[0-9a-f]{24}$")
    baseline_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    intervention_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    cost_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    future_information_ceiling: Literal[True] = True
    deployable: Literal[False] = False
    dynamic_position_authorized: Literal[False] = False

    @model_validator(mode="after")
    def validate_decision(self) -> "AdvisoryExitDecisionV1":
        object.__setattr__(self, "instrument", self.instrument.strip().upper())
        if self.target_action_date <= self.decision_date:
            raise ValueError("Exit target_action_date must follow decision_date")
        if self.action == ExitOracleAction.WAITING:
            if (
                self.execution_state
                not in {
                    ExitOracleExecutionState.CENSORED,
                    ExitOracleExecutionState.DATA_UNAVAILABLE,
                    ExitOracleExecutionState.BASELINE_UNAVAILABLE,
                }
                or self.effective_action_date is not None
            ):
                raise ValueError("WAITING Exit decision must be unavailable without an effective action date")
        elif self.effective_action_date is None:
            raise ValueError("available Exit decision requires effective_action_date")
        if self.action == ExitOracleAction.HOLD and self.execution_state != ExitOracleExecutionState.BASELINE_CONTINUE:
            raise ValueError("HOLD Exit decision must continue the baseline")
        if self.action == ExitOracleAction.HOLD and self.deferred_trading_days != 0:
            raise ValueError("HOLD Exit decision cannot report exit deferral")
        if self.action == ExitOracleAction.EXIT_NEXT_OPEN and self.execution_state not in {
            ExitOracleExecutionState.EXECUTED_NEXT_OPEN,
            ExitOracleExecutionState.DEFERRED_TO_FIRST_EXECUTABLE,
        }:
            raise ValueError("EXIT_NEXT_OPEN has an invalid execution_state")
        if self.execution_state == ExitOracleExecutionState.EXECUTED_NEXT_OPEN and self.deferred_trading_days != 0:
            raise ValueError("next-open execution cannot report deferred trading days")
        if self.execution_state == ExitOracleExecutionState.DEFERRED_TO_FIRST_EXECUTABLE:
            if self.deferred_trading_days < 1:
                raise ValueError("deferred exit requires at least one deferred trading day")
        digest = canonical_json_sha256(self.functional_payload())
        if self.decision_sha256 != digest:
            raise ValueError("Exit decision identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"decision_sha256"})


@dataclass(frozen=True)
class ExitLabelOracleResult:
    baseline: PolicyEpisodeLabelResult
    labels: tuple[AdvisoryIncrementalValueLabelV1, ...]
    decisions: tuple[AdvisoryExitDecisionV1, ...]
    label_frame: pd.DataFrame
    decision_frame: pd.DataFrame
    coverage: pd.DataFrame


def build_exit_label_oracle(
    *,
    rankings: pd.DataFrame,
    daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    policy: AdvisoryTransitionPolicyV1,
    policy_sha256: str,
    intervention_policy_sha256: str,
    cost_policy: AdvisoryPolicyCostV1,
    request_identity: dict[str, str],
    candidate_decision_dates: Sequence[pd.Timestamp] | None = None,
    candidate_depth: int = 5,
    rank_depth: int = 40,
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY,
    evidence_level: AdvisoryEvidenceLevel = AdvisoryEvidenceLevel.HISTORICAL_REPLAY,
) -> ExitLabelOracleResult:
    if candidate_depth != 5 or rank_depth != 40:
        raise AdvisoryModelFirstError(
            "N2 Exit oracle requires frozen Top5 candidates inside exact Top40 rank context",
            reason_code="ADVISORY_EXIT_BASELINE_UNAVAILABLE",
            context={"candidate_depth": candidate_depth, "rank_depth": rank_depth},
        )
    if evidence_level != AdvisoryEvidenceLevel.HISTORICAL_REPLAY:
        raise AdvisoryModelFirstError(
            "N2 Exit oracle only emits historical replay evidence",
            reason_code="ADVISORY_EVIDENCE_LEVEL_VIOLATION",
        )
    baseline = build_policy_episode_labels(
        rankings=rankings,
        daily=daily,
        benchmark_daily=benchmark_daily,
        suspend_rows=suspend_rows,
        trading_calendar=trading_calendar,
        policy=policy,
        policy_sha256=policy_sha256,
        cost_policy=cost_policy,
        request_identity=request_identity,
        candidate_decision_dates=candidate_decision_dates,
        candidate_depth=candidate_depth,
        rank_depth=rank_depth,
    )
    return _build_exit_label_oracle_from_baseline(
        baseline=baseline,
        daily=daily,
        suspend_rows=suspend_rows,
        trading_calendar=trading_calendar,
        policy_sha256=policy_sha256,
        intervention_policy_sha256=intervention_policy_sha256,
        cost_policy=cost_policy,
        decision_use=decision_use,
        evidence_level=evidence_level,
    )


def _build_exit_label_oracle_from_baseline(
    *,
    baseline: PolicyEpisodeLabelResult,
    daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    policy_sha256: str,
    intervention_policy_sha256: str,
    cost_policy: AdvisoryPolicyCostV1,
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY,
    evidence_level: AdvisoryEvidenceLevel = AdvisoryEvidenceLevel.HISTORICAL_REPLAY,
) -> ExitLabelOracleResult:
    if evidence_level != AdvisoryEvidenceLevel.HISTORICAL_REPLAY:
        raise AdvisoryModelFirstError(
            "N2 Exit label builder only emits historical replay evidence",
            reason_code="ADVISORY_EVIDENCE_LEVEL_VIOLATION",
        )
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    if len(calendar) < 2:
        raise AdvisoryModelFirstError(
            "Exit oracle requires at least two trading dates",
            reason_code="ADVISORY_EXIT_MARKET_DATA_INVALID",
        )
    positions = {value: index for index, value in enumerate(calendar)}
    market = _normalize_market(daily)
    required_suspend_columns = {"trade_date", "instrument"}
    if not required_suspend_columns.issubset(suspend_rows.columns):
        raise AdvisoryModelFirstError(
            "Exit suspend rows omit required columns",
            reason_code="ADVISORY_EXIT_MARKET_DATA_INVALID",
            context={"missing_columns": sorted(required_suspend_columns - set(suspend_rows.columns))},
        )
    suspended = {
        (pd.Timestamp(row.trade_date).normalize(), str(row.instrument).strip().upper())
        for row in suspend_rows.itertuples(index=False)
    }
    labels: list[AdvisoryIncrementalValueLabelV1] = []
    decisions: list[AdvisoryExitDecisionV1] = []
    for row in baseline.labels.itertuples(index=False):
        _require_baseline_identity(row=row, policy_sha256=policy_sha256, cost_policy=cost_policy)
        if str(row.label_status) != "MATURED":
            entry_value = getattr(row, "entry_trade_date", None)
            if entry_value is None or pd.isna(entry_value):
                continue
            label, decision = _unavailable_baseline_label(
                row=row,
                calendar=calendar,
                positions=positions,
                policy_sha256=policy_sha256,
                intervention_policy_sha256=intervention_policy_sha256,
                cost_policy=cost_policy,
                decision_use=decision_use,
                evidence_level=evidence_level,
            )
            if label is not None and decision is not None:
                labels.append(label)
                decisions.append(decision)
            continue
        labels_for_episode, decisions_for_episode = _build_mature_episode_labels(
            row=row,
            calendar=calendar,
            positions=positions,
            market=market,
            suspended=suspended,
            policy_sha256=policy_sha256,
            intervention_policy_sha256=intervention_policy_sha256,
            cost_policy=cost_policy,
            decision_use=decision_use,
            evidence_level=evidence_level,
        )
        labels.extend(labels_for_episode)
        decisions.extend(decisions_for_episode)
    if not labels:
        raise AdvisoryModelFirstError(
            "Exit oracle produced no decision labels",
            reason_code="ADVISORY_EXIT_BASELINE_UNAVAILABLE",
        )
    label_frame = pd.DataFrame([item.model_dump(mode="python") for item in labels])
    decision_frame = pd.DataFrame([item.model_dump(mode="python") for item in decisions])
    coverage = (
        label_frame.groupby(["status", "intervention_action"], observed=True, sort=True)
        .size()
        .rename("label_count")
        .reset_index()
    )
    return ExitLabelOracleResult(
        baseline=baseline,
        labels=tuple(labels),
        decisions=tuple(decisions),
        label_frame=label_frame,
        decision_frame=decision_frame,
        coverage=coverage,
    )


def _build_mature_episode_labels(
    *,
    row: object,
    calendar: pd.DatetimeIndex,
    positions: dict[pd.Timestamp, int],
    market: pd.DataFrame,
    suspended: set[tuple[pd.Timestamp, str]],
    policy_sha256: str,
    intervention_policy_sha256: str,
    cost_policy: AdvisoryPolicyCostV1,
    decision_use: DecisionUse,
    evidence_level: AdvisoryEvidenceLevel,
) -> tuple[list[AdvisoryIncrementalValueLabelV1], list[AdvisoryExitDecisionV1]]:
    entry_date = pd.Timestamp(row.entry_trade_date).normalize()
    exit_signal_date = pd.Timestamp(row.exit_signal_date).normalize()
    baseline_exit_date = pd.Timestamp(row.effective_exit_date).normalize()
    for value, field_name in (
        (entry_date, "entry_trade_date"),
        (exit_signal_date, "exit_signal_date"),
        (baseline_exit_date, "effective_exit_date"),
    ):
        if value not in positions:
            raise AdvisoryModelFirstError(
                "Exit baseline date is absent from the trading calendar",
                reason_code="ADVISORY_EXIT_MARKET_DATA_INVALID",
                context={"field": field_name, "value": value.date().isoformat()},
            )
    if not (positions[entry_date] <= positions[exit_signal_date] < positions[baseline_exit_date]):
        raise AdvisoryModelFirstError(
            "Exit baseline decision clock is invalid",
            reason_code="ADVISORY_EXIT_MARKET_DATA_INVALID",
        )
    entry_price = _positive(row.entry_price)
    baseline_exit_price = _positive(row.exit_price)
    baseline_value = _finite(row.net_return_bps)
    if entry_price is None or baseline_exit_price is None or baseline_value is None:
        raise AdvisoryModelFirstError(
            "mature Exit baseline omits prices or net return",
            reason_code="ADVISORY_EXIT_BASELINE_UNAVAILABLE",
        )
    recomputed_baseline = _episode_net_bps(
        entry_price=entry_price,
        exit_price=baseline_exit_price,
        cost_policy=cost_policy,
    )
    if not math.isclose(baseline_value, recomputed_baseline, rel_tol=1e-10, abs_tol=1e-7):
        raise AdvisoryModelFirstError(
            "Exit baseline net value differs from the frozen cost policy",
            reason_code="ADVISORY_ACTION_VALUE_NUMERIC_MISMATCH",
            context={"reported": baseline_value, "recomputed": recomputed_baseline},
        )

    labels: list[AdvisoryIncrementalValueLabelV1] = []
    decisions: list[AdvisoryExitDecisionV1] = []
    for decision_position in range(positions[entry_date], positions[exit_signal_date] + 1):
        decision_date = calendar[decision_position]
        target_date = calendar[decision_position + 1]
        executable = _first_executable_open(
            market=market,
            suspended=suspended,
            calendar=calendar,
            start_position=decision_position + 1,
            end_position=positions[baseline_exit_date],
            instrument=str(row.instrument).strip().upper(),
        )
        if executable[0] is None:
            unavailable = executable[3] in {"EXIT_MARKET_ROW_MISSING", "EXIT_OPEN_PRICE_MISSING"}
            label = _build_exit_label(
                row=row,
                decision_date=decision_date,
                target_date=target_date,
                effective_date=None,
                status=(
                    AdvisoryActionValueStatus.DATA_UNAVAILABLE
                    if unavailable
                    else AdvisoryActionValueStatus.CENSORED_RIGHT_BOUNDARY
                ),
                baseline_value=None,
                action_value=None,
                policy_sha256=policy_sha256,
                intervention_policy_sha256=intervention_policy_sha256,
                cost_policy=cost_policy,
                decision_use=decision_use,
                evidence_level=evidence_level,
                reason_code=executable[3],
            )
            decision = _build_exit_decision(
                label=label,
                action=ExitOracleAction.WAITING,
                execution_state=(
                    ExitOracleExecutionState.DATA_UNAVAILABLE if unavailable else ExitOracleExecutionState.CENSORED
                ),
                effective_action_date=None,
                deferred_trading_days=executable[2],
            )
        else:
            effective_date, exit_price, deferred_days, reason_code = executable
            action_value = _episode_net_bps(
                entry_price=entry_price,
                exit_price=float(exit_price),
                cost_policy=cost_policy,
            )
            label = _build_exit_label(
                row=row,
                decision_date=decision_date,
                target_date=target_date,
                effective_date=effective_date,
                status=AdvisoryActionValueStatus.AVAILABLE,
                baseline_value=baseline_value,
                action_value=action_value,
                policy_sha256=policy_sha256,
                intervention_policy_sha256=intervention_policy_sha256,
                cost_policy=cost_policy,
                decision_use=decision_use,
                evidence_level=evidence_level,
                reason_code=reason_code,
            )
            prefer_exit = float(label.incremental_net_value_bps) > 0.0
            decision = _build_exit_decision(
                label=label,
                action=ExitOracleAction.EXIT_NEXT_OPEN if prefer_exit else ExitOracleAction.HOLD,
                execution_state=(
                    ExitOracleExecutionState.DEFERRED_TO_FIRST_EXECUTABLE
                    if prefer_exit and deferred_days > 0
                    else ExitOracleExecutionState.EXECUTED_NEXT_OPEN
                    if prefer_exit
                    else ExitOracleExecutionState.BASELINE_CONTINUE
                ),
                effective_action_date=(effective_date if prefer_exit else baseline_exit_date.date()),
                deferred_trading_days=deferred_days if prefer_exit else 0,
            )
        labels.append(label)
        decisions.append(decision)
    return labels, decisions


def _unavailable_baseline_label(
    *,
    row: object,
    calendar: pd.DatetimeIndex,
    positions: dict[pd.Timestamp, int],
    policy_sha256: str,
    intervention_policy_sha256: str,
    cost_policy: AdvisoryPolicyCostV1,
    decision_use: DecisionUse,
    evidence_level: AdvisoryEvidenceLevel,
) -> tuple[AdvisoryIncrementalValueLabelV1 | None, AdvisoryExitDecisionV1 | None]:
    entry_date_value = getattr(row, "entry_trade_date", None)
    decision_value = getattr(row, "decision_as_of_trade_date", None)
    source_date = decision_value if entry_date_value is None or pd.isna(entry_date_value) else entry_date_value
    decision_date = pd.Timestamp(source_date).normalize()
    position = positions.get(decision_date)
    if position is None or position + 1 >= len(calendar):
        return None, None
    target = calendar[position + 1]
    label = _build_exit_label(
        row=row,
        decision_date=decision_date,
        target_date=target,
        effective_date=None,
        status=AdvisoryActionValueStatus.BASELINE_UNAVAILABLE,
        baseline_value=None,
        action_value=None,
        policy_sha256=policy_sha256,
        intervention_policy_sha256=intervention_policy_sha256,
        cost_policy=cost_policy,
        decision_use=decision_use,
        evidence_level=evidence_level,
        reason_code=f"BASELINE_{row.label_status}",
    )
    decision = _build_exit_decision(
        label=label,
        action=ExitOracleAction.WAITING,
        execution_state=ExitOracleExecutionState.BASELINE_UNAVAILABLE,
        effective_action_date=None,
        deferred_trading_days=0,
    )
    return label, decision


def _build_exit_label(
    *,
    row: object,
    decision_date: pd.Timestamp,
    target_date: pd.Timestamp,
    effective_date: date | None,
    status: AdvisoryActionValueStatus,
    baseline_value: float | None,
    action_value: float | None,
    policy_sha256: str,
    intervention_policy_sha256: str,
    cost_policy: AdvisoryPolicyCostV1,
    decision_use: DecisionUse,
    evidence_level: AdvisoryEvidenceLevel,
    reason_code: str,
) -> AdvisoryIncrementalValueLabelV1:
    incremental = None if baseline_value is None or action_value is None else action_value - baseline_value
    if incremental is not None and math.isclose(incremental, 0.0, rel_tol=0.0, abs_tol=1e-9):
        incremental = 0.0
    information_end_value = getattr(row, "label_information_end", target_date)
    information_end = pd.Timestamp(information_end_value).date()
    return build_incremental_value_label(
        role=AdvisoryActionRole.EXIT,
        decision_use=decision_use,
        evidence_level=evidence_level,
        sealed_holdout_accessed=evidence_level == AdvisoryEvidenceLevel.SEALED_HOLDOUT_CONFIRMATION,
        decision_date=decision_date.date(),
        target_action_date=target_date.date(),
        effective_action_date=effective_date,
        instrument=str(row.instrument).strip().upper(),
        episode_id=str(row.episode_label_id),
        baseline_action="CONTINUE_BASELINE",
        intervention_action="EXIT_NEXT_OPEN",
        status=status,
        baseline_net_value_bps=baseline_value,
        action_net_value_bps=action_value,
        incremental_net_value_bps=incremental,
        baseline_policy_sha256=policy_sha256,
        intervention_policy_sha256=intervention_policy_sha256,
        cost_policy_sha256=cost_policy.policy_sha256,
        shadow_simulator_sha256=POLICY_EPISODE_SIMULATOR_SHA256,
        information_start=decision_date.date(),
        information_end=information_end,
        reason_code=reason_code,
    )


def _build_exit_decision(
    *,
    label: AdvisoryIncrementalValueLabelV1,
    action: ExitOracleAction,
    execution_state: ExitOracleExecutionState,
    effective_action_date: date | None,
    deferred_trading_days: int,
) -> AdvisoryExitDecisionV1:
    payload = {
        "schema_version": "advisory_exit_decision_v1",
        "decision_date": label.decision_date,
        "target_action_date": label.target_action_date,
        "effective_action_date": effective_action_date,
        "instrument": label.instrument,
        "episode_id": label.episode_id,
        "action": action,
        "execution_state": execution_state,
        "reason_code": str(label.reason_code or label.status.value),
        "deferred_trading_days": deferred_trading_days,
        "incremental_label_id": label.label_id,
        "baseline_policy_sha256": label.baseline_policy_sha256,
        "intervention_policy_sha256": label.intervention_policy_sha256,
        "cost_policy_sha256": label.cost_policy_sha256,
        "future_information_ceiling": True,
        "deployable": False,
        "dynamic_position_authorized": False,
    }
    draft = AdvisoryExitDecisionV1.model_construct(**payload)
    digest = canonical_json_sha256(draft.model_dump(mode="json", exclude={"decision_sha256"}))
    return AdvisoryExitDecisionV1(decision_sha256=digest, **payload)


def _first_executable_open(
    *,
    market: pd.DataFrame,
    suspended: set[tuple[pd.Timestamp, str]],
    calendar: pd.DatetimeIndex,
    start_position: int,
    end_position: int,
    instrument: str,
) -> tuple[date | None, float | None, int, str]:
    deferred = 0
    for position in range(start_position, end_position + 1):
        value = calendar[position]
        row = _market_row(market, value, instrument)
        if row is None:
            return None, None, deferred, "EXIT_MARKET_ROW_MISSING"
        if (value, instrument) in suspended or is_one_price_limit_down(row):
            deferred += 1
            continue
        open_price = _positive(row.get("open"))
        if open_price is None:
            return None, None, deferred, "EXIT_OPEN_PRICE_MISSING"
        return (
            value.date(),
            open_price,
            deferred,
            "EXIT_FIRST_EXECUTABLE_OPEN" if deferred else "EXIT_NEXT_OPEN",
        )
    return None, None, deferred, "EXIT_ACTION_CENSORED_BEFORE_BASELINE_EXIT"


def _normalize_market(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame.index, pd.MultiIndex) or frame.index.nlevels != 2:
        raise AdvisoryModelFirstError(
            "Exit market data must use datetime/instrument MultiIndex",
            reason_code="ADVISORY_EXIT_MARKET_DATA_INVALID",
        )
    output = frame.copy()
    dates = pd.to_datetime(output.index.get_level_values(0)).normalize()
    symbols = output.index.get_level_values(1).astype(str).str.strip().str.upper()
    output.index = pd.MultiIndex.from_arrays([dates, symbols], names=["datetime", "instrument"])
    if output.index.has_duplicates:
        raise AdvisoryModelFirstError(
            "Exit market data contains duplicate datetime/instrument rows",
            reason_code="ADVISORY_EXIT_MARKET_DATA_INVALID",
        )
    return output.sort_index()


def _market_row(frame: pd.DataFrame, value: pd.Timestamp, instrument: str) -> pd.Series | None:
    key = (pd.Timestamp(value).normalize(), instrument)
    try:
        row = frame.loc[key]
    except KeyError:
        return None
    if isinstance(row, pd.DataFrame):
        raise AdvisoryModelFirstError(
            "Exit market lookup returned duplicate rows",
            reason_code="ADVISORY_EXIT_MARKET_DATA_INVALID",
        )
    return row


def _require_baseline_identity(
    *,
    row: object,
    policy_sha256: str,
    cost_policy: AdvisoryPolicyCostV1,
) -> None:
    if str(getattr(row, "shadow_policy_sha256")) != policy_sha256:
        raise AdvisoryModelFirstError(
            "Exit baseline policy hash differs from the declared baseline",
            reason_code="ADVISORY_ACTION_VALUE_POLICY_MISMATCH",
        )
    if str(getattr(row, "cost_policy_sha256")) != cost_policy.policy_sha256:
        raise AdvisoryModelFirstError(
            "Exit baseline cost hash differs from the declared cost policy",
            reason_code="ADVISORY_ACTION_VALUE_POLICY_MISMATCH",
        )


def _episode_net_bps(*, entry_price: float, exit_price: float, cost_policy: AdvisoryPolicyCostV1) -> float:
    return (
        exit_price
        * (1.0 - cost_policy.sell_cost_bps / 10000.0)
        / (entry_price * (1.0 + cost_policy.buy_cost_bps / 10000.0))
        - 1.0
    ) * 10000.0


def _positive(value: object) -> float | None:
    result = _finite(value)
    return result if result is not None and result > 0 else None


def _finite(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    result = float(value)
    return result if math.isfinite(result) else None
