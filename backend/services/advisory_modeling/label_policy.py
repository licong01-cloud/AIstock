from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_FLOOR
from enum import Enum
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_modeling.identity import (
    FrozenModel,
    set_computed_hash,
    strict_identifier,
    validated_hash,
)


LABEL_POLICY_SCHEMA_VERSION = "advisory_ranking_label_policy_v1"
LABEL_INPUT_SCHEMA_VERSION = "advisory_ranking_label_input_v1"
LABEL_RESULT_SCHEMA_VERSION = "advisory_ranking_group_label_result_v1"
RANKING_GROUP_IDENTITY_SCHEMA_VERSION = "advisory_ranking_group_identity_v1"


class RankingGroupStatus(str, Enum):
    MODELABLE = "MODELABLE"
    NO_LABEL_VARIATION = "NO_LABEL_VARIATION"
    GROUP_NOT_MODELABLE = "GROUP_NOT_MODELABLE"


class RankingLabelPolicyV1(FrozenModel):
    schema_version: Literal[LABEL_POLICY_SCHEMA_VERSION] = LABEL_POLICY_SCHEMA_VERSION
    label_policy_id: str = "short_rebound_risk_aware_net_return_h5_v1"
    primary_horizon_trading_days: Literal[5] = 5
    return_projection: Literal["RETURN_NET_EXCESS"] = "RETURN_NET_EXCESS"
    mfe_projection: Literal["EXECUTABLE_MFE"] = "EXECUTABLE_MFE"
    mae_projection: Literal["EXECUTABLE_MAE"] = "EXECUTABLE_MAE"
    return_weight: Decimal = Decimal("1")
    mfe_weight: Decimal = Decimal("0.25")
    mae_loss_weight: Decimal = Decimal("-0.50")
    label_gain: tuple[int, ...] = (0, 1, 3, 7, 15)
    label_policy_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("label_policy_id")
    @classmethod
    def _identifier(cls, value: str) -> str:
        return strict_identifier(value, field_name="label_policy_id")

    @field_validator("label_policy_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validated_hash(value, field_name="label_policy_hash")

    @model_validator(mode="after")
    def _identity(self) -> "RankingLabelPolicyV1":
        if (
            self.return_weight != Decimal("1")
            or self.mfe_weight != Decimal("0.25")
            or self.mae_loss_weight != Decimal("-0.50")
            or self.label_gain != (0, 1, 3, 7, 15)
        ):
            raise ValueError("RankingLabelPolicyV1 weights and gains are frozen")
        set_computed_hash(self, field_name="label_policy_hash", exclude={"label_policy_hash"})
        return self


class RankingLabelInputV1(FrozenModel):
    schema_version: Literal[LABEL_INPUT_SCHEMA_VERSION] = LABEL_INPUT_SCHEMA_VERSION
    symbol: str = Field(pattern=r"^[0-9]{6}\.(SH|SZ|BJ)$")
    return_5: Decimal
    executable_mfe_5: Decimal
    executable_mae_5: Decimal
    label_source_closure_hash: str = Field(min_length=64, max_length=64)

    @field_validator("label_source_closure_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return str(validated_hash(value, field_name="label_source_closure_hash"))


class RankedLabelV1(FrozenModel):
    symbol: str
    raw_utility_5: Decimal
    relevance: int = Field(ge=0, le=4)
    label_source_closure_hash: str


class RankingGroupIdentityV1(FrozenModel):
    schema_version: Literal[RANKING_GROUP_IDENTITY_SCHEMA_VERSION] = (
        RANKING_GROUP_IDENTITY_SCHEMA_VERSION
    )
    decision_as_of_trade_date: date
    target_trade_date: date
    canonical_signal_scope_hash: str = Field(min_length=64, max_length=64)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    group_identity_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("canonical_signal_scope_hash", "label_policy_hash", "group_identity_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "RankingGroupIdentityV1":
        if self.target_trade_date <= self.decision_as_of_trade_date:
            raise ValueError("target_trade_date must be after decision_as_of_trade_date")
        set_computed_hash(self, field_name="group_identity_hash", exclude={"group_identity_hash"})
        return self


class RankingGroupLabelResultV1(FrozenModel):
    schema_version: Literal[LABEL_RESULT_SCHEMA_VERSION] = LABEL_RESULT_SCHEMA_VERSION
    label_policy_hash: str = Field(min_length=64, max_length=64)
    group_identity: RankingGroupIdentityV1
    status: RankingGroupStatus
    labels: tuple[RankedLabelV1, ...]
    distinct_utility_count: int = Field(ge=0)
    reason_codes: tuple[str, ...] = ()
    result_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("label_policy_hash", "result_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "RankingGroupLabelResultV1":
        if self.status is RankingGroupStatus.MODELABLE and len(self.labels) < 2:
            raise ValueError("MODELABLE groups require at least two labels")
        if self.group_identity.label_policy_hash != self.label_policy_hash:
            raise ValueError("ranking group label policy differs from result label policy")
        set_computed_hash(self, field_name="result_hash", exclude={"result_hash"})
        return self


def build_ranking_labels(
    rows: tuple[RankingLabelInputV1, ...],
    *,
    group_identity: RankingGroupIdentityV1,
    policy: RankingLabelPolicyV1 | None = None,
) -> RankingGroupLabelResultV1:
    active_policy = policy or RankingLabelPolicyV1()
    if group_identity.label_policy_hash != active_policy.label_policy_hash:
        raise ValueError("ranking group identity differs from active label policy")
    if len({row.symbol for row in rows}) != len(rows):
        raise ValueError("ranking group contains duplicate symbols")

    utilities: dict[str, Decimal] = {}
    for row in rows:
        mfe = max(Decimal(0), row.executable_mfe_5)
        mae_loss = max(Decimal(0), -row.executable_mae_5)
        utilities[row.symbol] = (
            active_policy.return_weight * row.return_5
            + active_policy.mfe_weight * mfe
            + active_policy.mae_loss_weight * mae_loss
        )

    distinct = sorted(set(utilities.values()))
    if len(rows) < 2:
        status = RankingGroupStatus.GROUP_NOT_MODELABLE
        reason_codes = ("GROUP_NOT_MODELABLE",)
    elif len(distinct) == 1:
        status = RankingGroupStatus.NO_LABEL_VARIATION
        reason_codes = ("NO_LABEL_VARIATION",)
    else:
        status = RankingGroupStatus.MODELABLE
        reason_codes = ()

    relevance_by_utility: dict[Decimal, int] = {}
    if len(distinct) <= 1:
        relevance_by_utility = {value: 0 for value in distinct}
    else:
        denominator = Decimal(len(distinct) - 1)
        for dense_rank, value in enumerate(distinct):
            relevance_by_utility[value] = int(
                (Decimal(4 * dense_rank) / denominator).to_integral_value(rounding=ROUND_FLOOR)
            )

    by_symbol = {row.symbol: row for row in rows}
    labels = tuple(
        RankedLabelV1(
            symbol=symbol,
            raw_utility_5=utilities[symbol],
            relevance=relevance_by_utility[utilities[symbol]],
            label_source_closure_hash=by_symbol[symbol].label_source_closure_hash,
        )
        for symbol in sorted(utilities)
    )
    return RankingGroupLabelResultV1(
        label_policy_hash=str(active_policy.label_policy_hash),
        group_identity=group_identity,
        status=status,
        labels=labels,
        distinct_utility_count=len(distinct),
        reason_codes=reason_codes,
    )
