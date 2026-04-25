"""Strategy Package manifest v1 models."""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class SourceType(str, Enum):
    QE_EXPERIMENT = "qe_experiment"
    QE_EVOLUTION_LOOP = "qe_evolution_loop"


class AlphaMode(str, Enum):
    SINGLE_ALPHA = "single_alpha"
    MULTI_ALPHA = "multi_alpha"


class PackageStatus(str, Enum):
    DRAFT = "DRAFT"
    ASSET_VALIDATED = "ASSET_VALIDATED"
    BACKTEST_APPROVED = "BACKTEST_APPROVED"
    SELECTION_ENABLED = "SELECTION_ENABLED"
    PAPER_ENABLED = "PAPER_ENABLED"
    PAPER_RUNNING = "PAPER_RUNNING"
    PAPER_PASSED = "PAPER_PASSED"
    PAPER_FAILED = "PAPER_FAILED"
    RETIRED = "RETIRED"


class StrategyPackageSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_type: SourceType
    source_id: str
    loop_id: str | None = None
    run_id: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class MetricsSnapshot(BaseModel):
    model_config = ConfigDict(extra="allow")

    ic: float | None = None
    rank_ic: float | None = None
    annual_return: float | None = None
    max_drawdown: float | None = None
    turnover: float | None = None
    sample_start: date | None = None
    sample_end: date | None = None


class AlphaLineage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    qe_artifact_id: str | None = None
    factor_artifact_refs: list[str] = Field(default_factory=list)
    model_artifact_ref: str | None = None


class AlphaComponent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alpha_id: str
    alpha_name: str
    component_weight: float = Field(gt=0)
    factor_ids: list[str]
    model_id: str | None = None
    model_ref: str | None = None
    holding_period: str
    rebalance_frequency: str
    score_direction: Literal["higher_better", "lower_better"]
    score_normalization: str = "rank"
    risk_tags: list[str] = Field(default_factory=list)
    metrics_snapshot: MetricsSnapshot = Field(default_factory=MetricsSnapshot)
    lineage: AlphaLineage = Field(default_factory=AlphaLineage)

    @field_validator("factor_ids")
    @classmethod
    def _factor_ids_required(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("alpha component requires at least one factor")
        return value


class AlphaCombinationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    method: str
    weights: dict[str, float] = Field(default_factory=dict)
    score_clip: dict[str, Any] | None = None
    normalization_scope: str = "universe"
    conflict_resolution: str = "weighted_sum"
    explainability: dict[str, bool] = Field(
        default_factory=lambda: {
            "store_component_scores": True,
            "store_component_rank": True,
            "store_component_reason": True,
        }
    )


class FactorAsset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    factor_id: str
    factor_name: str
    artifact_ref: str | None = None
    required: bool = True


class ModelAsset(BaseModel):
    model_id: str
    model_ref: str | None = None
    model_type: str | None = None


class UniversePolicy(BaseModel):
    model_config = ConfigDict(extra="allow")

    stock_pool: str


class PortfolioPolicy(BaseModel):
    model_config = ConfigDict(extra="allow")

    topk: int = Field(gt=0)
    n_drop: int = Field(ge=0)
    rebalance_frequency: str = "1day"


class ExecutionPolicy(BaseModel):
    model_config = ConfigDict(extra="allow")

    backtest_freq: Literal["1min", "5min", "minute"]
    order_lot_size: int = 100


class MinuteDataRequirements(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requires_minute_bar: Literal[True] = True
    requires_limit_price: Literal[True] = True
    requires_trade_calendar: Literal[True] = True
    requires_suspend_status: Literal[True] = True


class MinuteFallbackPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    on_missing_minute_bar: Literal["fail"] = "fail"
    on_algo_error: Literal["fail"] = "fail"


class MinuteExecutionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_level: Literal["minute"] = "minute"
    bar_freq: Literal["1m", "5m"] = "1m"
    algo_code: str
    algo_config: dict[str, Any] = Field(default_factory=dict)
    fallback_algo_code: None = None
    data_requirements: MinuteDataRequirements = Field(default_factory=MinuteDataRequirements)
    fallback_policy: MinuteFallbackPolicy = Field(default_factory=MinuteFallbackPolicy)
    quality_report: dict[str, bool] = Field(
        default_factory=lambda: {
            "record_slippage": True,
            "record_participation_rate": True,
            "record_unfilled_reason": True,
        }
    )

    @field_validator("algo_code")
    @classmethod
    def _algo_code_required(cls, value: str) -> str:
        value = value.strip().upper()
        if not value:
            raise ValueError("minute execution algo_code is required")
        return value


class RiskPolicy(BaseModel):
    model_config = ConfigDict(extra="allow")

    enforce_a_share_rules: Literal[True] = True
    enforce_t_plus_one: Literal[True] = True
    enforce_limit_price: Literal[True] = True
    max_position_weight: float | None = Field(default=None, gt=0)


class BacktestSummary(BaseModel):
    model_config = ConfigDict(extra="allow")

    ic: float | None = None
    rank_ic: float | None = None
    icir: float | None = None
    annual_return: float | None = None
    max_drawdown: float | None = None
    final_nav: float | None = None
    n_trading_days: int | None = Field(default=None, ge=0)
    raw_metrics: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _must_have_some_backtest_metric(self) -> "BacktestSummary":
        if not self.raw_metrics and all(
            value is None
            for value in [
                self.ic,
                self.rank_ic,
                self.icir,
                self.annual_return,
                self.max_drawdown,
                self.final_nav,
            ]
        ):
            raise ValueError("backtest_summary requires metrics")
        return self


class AssetCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    check_name: str
    passed: bool
    message: str
    context: dict[str, Any] = Field(default_factory=dict)


class StrategyPackageManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    manifest_version: Literal["1.0"] = "1.0"
    package_id: str = Field(default_factory=lambda: f"pkg_{uuid4().hex}")
    package_name: str
    package_version: str = "1.0.0"
    source: StrategyPackageSource
    alpha_mode: AlphaMode
    alpha_components: list[AlphaComponent]
    alpha_combination_policy: AlphaCombinationPolicy
    factor_set: list[FactorAsset]
    model_asset: ModelAsset | list[ModelAsset]
    strategy_config: dict[str, Any]
    universe_policy: UniversePolicy
    portfolio_policy: PortfolioPolicy
    execution_policy: ExecutionPolicy
    minute_execution_policy: MinuteExecutionPolicy
    risk_policy: RiskPolicy = Field(default_factory=RiskPolicy)
    backtest_summary: BacktestSummary
    asset_checks: list[AssetCheck] = Field(default_factory=list)
    manifest_sha256: str | None = None
    package_status: PackageStatus = PackageStatus.DRAFT

    @model_validator(mode="after")
    def _validate_alpha_shape(self) -> "StrategyPackageManifest":
        component_count = len(self.alpha_components)
        if self.alpha_mode == AlphaMode.SINGLE_ALPHA and component_count != 1:
            raise ValueError("single_alpha package must have exactly one alpha component")
        if self.alpha_mode == AlphaMode.MULTI_ALPHA and component_count < 2:
            raise ValueError("multi_alpha package must have at least two alpha components")

        ids = [component.alpha_id for component in self.alpha_components]
        if len(ids) != len(set(ids)):
            raise ValueError("alpha component ids must be unique")

        weight_ids = set(self.alpha_combination_policy.weights)
        missing_weight_ids = weight_ids.difference(ids)
        if missing_weight_ids:
            raise ValueError(
                f"alpha_combination_policy references unknown alpha ids: {sorted(missing_weight_ids)}"
            )

        if self.alpha_mode == AlphaMode.SINGLE_ALPHA:
            only_component = self.alpha_components[0]
            if self.alpha_combination_policy.method != "identity":
                raise ValueError("single_alpha package must use identity combination policy")
            expected = {only_component.alpha_id: 1.0}
            if self.alpha_combination_policy.weights != expected:
                raise ValueError("single_alpha combination weights must be exactly 1.0")
        return self
