from __future__ import annotations

import math
from decimal import Decimal
from statistics import stdev
from typing import Iterable, Sequence

from backend.services.advisory_modeling.feature_schema import (
    FeatureFormulaDefinitionV1,
    FeatureFormulaRegistryV1,
)


def frozen_formula_registry_v1() -> FeatureFormulaRegistryV1:
    """Return the complete, canonical v1 formula payload used by export and inference."""

    formulas = (
        FeatureFormulaDefinitionV1(
            formula_id="candidate_rank_percentile_v1",
            formula_version="1",
            expression="n > 1 ? (n - rank) / (n - 1) : 0.5",
            input_roles=("same_day_stage_rank", "same_day_stage_candidate_count"),
            parameters={"single_candidate_value": "0.5", "rank_base": 1},
            pit_constraints=("base snapshot only", "computed independently for each stage"),
            missing_behavior="required rank or candidate count fails the ranking group",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="candidate_score_gap_v1",
            formula_version="1",
            expression="score - previous_rank_score; score - next_rank_score",
            input_roles=("same_day_stage_score", "same_day_stage_summary"),
            parameters={"boundary_value": None},
            pit_constraints=("score direction and tie policy come from the same stage summary",),
            missing_behavior="boundary gaps are null with an explicit missing indicator",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="multi_alpha_consensus_v1",
            formula_version="1",
            expression=(
                "weighted_mean, weighted_std, sign_agreement, "
                "max(abs(weight*score))/sum(abs(weight*score))"
            ),
            input_roles=("frozen_leg_score", "frozen_leg_weight", "component_evidence_json"),
            parameters={"weight_normalization": "sum_positive_weight"},
            pit_constraints=("leg, weight, model and asset identities come from base snapshot",),
            missing_behavior="missing component evidence fails the ranking group",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="adjusted_return_v1",
            formula_version="1",
            expression="adj_close_T / adj_close_T_minus_h - 1",
            input_roles=("raw_close", "adj_factor", "trading_calendar"),
            parameters={"horizons": [1, 3, 5, 10, 20, 60]},
            pit_constraints=("close and adj_factor available_at <= decision_cutoff_ts",),
            missing_behavior="insufficient valid trading days produce null with indicator",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="realized_volatility_v1",
            formula_version="1",
            expression="sample_std(log(adj_close_t / adj_close_t_minus_1), ddof=1)",
            input_roles=("adjusted_close_history", "trading_calendar"),
            parameters={"ddof": 1},
            pit_constraints=("requires h valid trading-day returns",),
            missing_behavior="fewer than h valid returns produce null with indicator",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="distance_to_extreme_v1",
            formula_version="1",
            expression=(
                "adj_close_T/max(adj_high_T_minus_h_plus_1_to_T)-1; "
                "adj_close_T/min(adj_low_T_minus_h_plus_1_to_T)-1"
            ),
            input_roles=("raw_high", "raw_low", "raw_close", "adj_factor"),
            parameters={"windows": [20, 60]},
            pit_constraints=("high, low and close use the same corporate-action revision",),
            missing_behavior="incomplete or non-positive window produces null with indicator",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="liquidity_state_v1",
            formula_version="1",
            expression="amount/turnover mean_5, mean_20, current/mean_20, log_amount_z20",
            input_roles=("amount", "turnover", "trading_calendar"),
            parameters={"windows": [5, 20], "zscore_ddof": 1},
            pit_constraints=("suspension-day zero turnover remains an observed fact",),
            missing_behavior="no forward fill; zero denominator produces null with indicator",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="moneyflow_state_v1",
            formula_version="1",
            expression="main_net_inflow/amount, sum_5, sum_20, sign_consistency",
            input_roles=("main_net_inflow", "amount", "trading_calendar"),
            parameters={"windows": [5, 20]},
            pit_constraints=("moneyflow and amount are converted to the same monetary unit",),
            missing_behavior="zero denominator produces null with indicator, never zero fill",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="industry_context_v1",
            formula_version="1",
            expression="member_equal_weight_return_5_20, breadth_above_ma20, positive_flow_ratio",
            input_roles=("pit_industry_membership", "member_market_history", "member_moneyflow"),
            parameters={"return_windows": [5, 20], "moving_average_window": 20},
            pit_constraints=("membership and member-set hash are available at decision cutoff",),
            missing_behavior="missing membership or source closure fails required industry context",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="market_context_v1",
            formula_version="1",
            expression=(
                "pit_universe_equal_weight_return_5_20, cross_section_volatility, "
                "breadth_above_ma20, up_limit_ratio, down_limit_ratio"
            ),
            input_roles=("pit_universe", "member_market_history", "price_limit_policy"),
            parameters={"return_windows": [5, 20], "moving_average_window": 20},
            pit_constraints=("universe, price-limit policy and effective member set enter identity",),
            missing_behavior="incomplete universe closure fails required market context",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="candidate_group_context_v1",
            formula_version="1",
            expression="candidate_count, industry_herfindahl, stage_score_std, mean_leg_disagreement",
            input_roles=("same_day_base_candidates", "same_day_stage_scores", "same_day_leg_scores"),
            parameters={"stages": [
                "alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective"
            ]},
            pit_constraints=("only same-day base candidates are used",),
            missing_behavior="missing group evidence fails the ranking group",
        ),
    )
    return FeatureFormulaRegistryV1(formulas=formulas)


class ShortReboundFeatureFormulaKernelV1:
    """Pure frozen calculations consumed by the Batch B feature builder."""

    formula_registry = frozen_formula_registry_v1()

    @staticmethod
    def candidate_rank_percentile(*, rank: int, candidate_count: int) -> Decimal:
        if candidate_count < 1 or rank < 1 or rank > candidate_count:
            raise ValueError("rank must be within the candidate group")
        if candidate_count == 1:
            return Decimal("0.5")
        return Decimal(candidate_count - rank) / Decimal(candidate_count - 1)

    @staticmethod
    def adjusted_return(adjusted_closes: Sequence[Decimal]) -> Decimal | None:
        if len(adjusted_closes) < 2 or adjusted_closes[0] <= 0:
            return None
        return adjusted_closes[-1] / adjusted_closes[0] - Decimal(1)

    @staticmethod
    def realized_volatility(adjusted_closes: Sequence[Decimal], *, horizon: int) -> float | None:
        if horizon < 2 or len(adjusted_closes) != horizon + 1:
            return None
        closes = [float(value) for value in adjusted_closes]
        if any(value <= 0 for value in closes):
            return None
        returns = [math.log(current / previous) for previous, current in zip(closes, closes[1:])]
        return stdev(returns) if len(returns) >= horizon else None

    @staticmethod
    def distance_to_extreme(
        *,
        adjusted_close: Decimal,
        adjusted_highs: Sequence[Decimal],
        adjusted_lows: Sequence[Decimal],
    ) -> tuple[Decimal | None, Decimal | None]:
        if not adjusted_highs or len(adjusted_highs) != len(adjusted_lows):
            return None, None
        high = max(adjusted_highs)
        low = min(adjusted_lows)
        if adjusted_close <= 0 or high <= 0 or low <= 0:
            return None, None
        return adjusted_close / high - Decimal(1), adjusted_close / low - Decimal(1)

    @staticmethod
    def multi_alpha_consensus(
        *,
        scores: Sequence[Decimal],
        weights: Sequence[Decimal],
    ) -> dict[str, Decimal]:
        if not scores or len(scores) != len(weights):
            raise ValueError("scores and weights must be non-empty and aligned")
        if any(weight <= 0 for weight in weights):
            raise ValueError("multi-alpha weights must satisfy the admitted positive-weight contract")
        denominator = sum(weights, Decimal(0))
        if denominator == 0:
            raise ValueError("multi-alpha weights must have non-zero absolute sum")
        normalized = [weight / denominator for weight in weights]
        mean = sum((weight * score for weight, score in zip(normalized, scores)), Decimal(0))
        variance = sum(
            (abs(weight) * (score - mean) ** 2 for weight, score in zip(normalized, scores)),
            Decimal(0),
        )
        non_zero = [(score, weight) for score, weight in zip(scores, normalized) if score != 0]
        weighted_positive = sum((abs(weight) for score, weight in non_zero if score > 0), Decimal(0))
        weighted_negative = sum((abs(weight) for score, weight in non_zero if score < 0), Decimal(0))
        sign_agreement = max(weighted_positive, weighted_negative)
        contributions = [abs(weight * score) for score, weight in zip(scores, normalized)]
        contribution_sum = sum(contributions, Decimal(0))
        dominance = max(contributions) / contribution_sum if contribution_sum else Decimal(0)
        return {
            "weighted_mean": mean,
            "weighted_std": variance.sqrt(),
            "sign_agreement": sign_agreement,
            "max_leg_dominance": dominance,
        }

    @staticmethod
    def candidate_group_industry_herfindahl(industry_ids: Iterable[str]) -> Decimal:
        counts: dict[str, int] = {}
        total = 0
        for industry_id in industry_ids:
            if not industry_id:
                raise ValueError("industry ids must be non-empty")
            counts[industry_id] = counts.get(industry_id, 0) + 1
            total += 1
        if total == 0:
            raise ValueError("candidate group must not be empty")
        return sum((Decimal(count) / Decimal(total)) ** 2 for count in counts.values())
