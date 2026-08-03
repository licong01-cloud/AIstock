from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_modeling.identity import (
    FrozenModel,
    set_computed_hash,
    strict_identifier,
    validated_hash,
)


MARKET_REGIME_TEMPLATE_SCHEMA_VERSION = "advisory_market_regime_policy_template_v1"
FITTED_MARKET_REGIME_SCHEMA_VERSION = "advisory_fitted_market_regime_policy_v1"


class MarketRegime(str, Enum):
    BEAR = "BEAR"
    NEUTRAL = "NEUTRAL"
    BULL = "BULL"
    UNAVAILABLE = "UNAVAILABLE"


class MarketRegimePolicyTemplateV1(FrozenModel):
    schema_version: Literal[MARKET_REGIME_TEMPLATE_SCHEMA_VERSION] = (
        MARKET_REGIME_TEMPLATE_SCHEMA_VERSION
    )
    policy_template_id: str = "short_rebound_market_regime_v1"
    return_feature_id: Literal["market_return_20_mean"] = (
        "market_return_20_mean"
    )
    breadth_feature_id: Literal["market_breadth_above_ma20"] = "market_breadth_above_ma20"
    return_weight: Decimal = Decimal("0.5")
    breadth_weight: Decimal = Decimal("0.5")
    bear_upper_inclusive: Decimal = Decimal("-0.5")
    bull_lower_inclusive: Decimal = Decimal("0.5")
    policy_template_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("policy_template_id")
    @classmethod
    def _identifier(cls, value: str) -> str:
        return strict_identifier(value, field_name="policy_template_id")

    @field_validator("policy_template_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validated_hash(value, field_name="policy_template_hash")

    @model_validator(mode="after")
    def _identity(self) -> "MarketRegimePolicyTemplateV1":
        if (
            self.return_weight != Decimal("0.5")
            or self.breadth_weight != Decimal("0.5")
            or self.bear_upper_inclusive != Decimal("-0.5")
            or self.bull_lower_inclusive != Decimal("0.5")
        ):
            raise ValueError("MarketRegimePolicyTemplateV1 formula and thresholds are frozen")
        set_computed_hash(
            self,
            field_name="policy_template_hash",
            exclude={"policy_template_hash"},
        )
        return self


class FeatureFitStatisticsV1(FrozenModel):
    feature_id: str = Field(min_length=1, max_length=160)
    mean: Decimal
    sample_std: Decimal = Field(ge=0)
    sample_count: int = Field(ge=2)
    statistics_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "FeatureFitStatisticsV1":
        set_computed_hash(self, field_name="statistics_hash", exclude={"statistics_hash"})
        return self


class FittedMarketRegimePolicyV1(FrozenModel):
    schema_version: Literal[FITTED_MARKET_REGIME_SCHEMA_VERSION] = (
        FITTED_MARKET_REGIME_SCHEMA_VERSION
    )
    policy_template_hash: str = Field(min_length=64, max_length=64)
    fold_id: str = Field(min_length=1, max_length=160)
    universe_policy_set_hash: str = Field(min_length=64, max_length=64)
    calendar_hash: str = Field(min_length=64, max_length=64)
    decision_cutoff_hash: str = Field(min_length=64, max_length=64)
    return_statistics: FeatureFitStatisticsV1
    breadth_statistics: FeatureFitStatisticsV1
    bear_upper_inclusive: Decimal = Decimal("-0.5")
    bull_lower_inclusive: Decimal = Decimal("0.5")
    fitted_market_regime_policy_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )

    @field_validator(
        "policy_template_hash",
        "universe_policy_set_hash",
        "calendar_hash",
        "decision_cutoff_hash",
        "fitted_market_regime_policy_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "FittedMarketRegimePolicyV1":
        template = MarketRegimePolicyTemplateV1()
        if self.policy_template_hash != template.policy_template_hash:
            raise ValueError("fitted policy does not reference the frozen v1 template")
        if self.return_statistics.feature_id != template.return_feature_id:
            raise ValueError("return statistics use the wrong feature")
        if self.breadth_statistics.feature_id != template.breadth_feature_id:
            raise ValueError("breadth statistics use the wrong feature")
        if (
            self.bear_upper_inclusive != template.bear_upper_inclusive
            or self.bull_lower_inclusive != template.bull_lower_inclusive
        ):
            raise ValueError("fitted policy thresholds differ from the template")
        set_computed_hash(
            self,
            field_name="fitted_market_regime_policy_hash",
            exclude={"fitted_market_regime_policy_hash"},
        )
        return self

    def classify(
        self,
        *,
        pit_universe_equal_weight_return_20: Decimal | None,
        market_breadth_above_ma20: Decimal | None,
    ) -> MarketRegime:
        if (
            pit_universe_equal_weight_return_20 is None
            or market_breadth_above_ma20 is None
            or self.return_statistics.sample_std == 0
            or self.breadth_statistics.sample_std == 0
        ):
            return MarketRegime.UNAVAILABLE
        return_z = (
            pit_universe_equal_weight_return_20 - self.return_statistics.mean
        ) / self.return_statistics.sample_std
        breadth_z = (
            market_breadth_above_ma20 - self.breadth_statistics.mean
        ) / self.breadth_statistics.sample_std
        trend_z = Decimal("0.5") * return_z + Decimal("0.5") * breadth_z
        if trend_z <= self.bear_upper_inclusive:
            return MarketRegime.BEAR
        if trend_z >= self.bull_lower_inclusive:
            return MarketRegime.BULL
        return MarketRegime.NEUTRAL
