"""Independent research valuation, never a production policy replacement."""
from dataclasses import asdict, dataclass
import math

from backend.services.advisory_list_transition import AdvisoryTransitionPolicyV1
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SCENARIO = "VALUE_REVIEW_5_V1"
RISK_REFERENCE_BPS = 800.0
COST = AdvisoryPolicyCostV1(buy_cost_bps=.95, sell_cost_bps=5.95)


def finite_number(value, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("value anchor requires a finite numeric value, not a bool")
    if positive and value <= 0:
        raise ValueError("value anchor requires a positive value")
    return float(value)


def value_anchor_policy_v1():
    return AdvisoryTransitionPolicyV1(
        target_count=5, rank_enter_threshold=5, rank_exit_threshold=40,
        rank_exit_confirm_days=2, daily_replacement_budget=5,
        stop_loss_bps=0, take_profit_bps=0, trailing_stop_bps=0,
        time_stop_days=5, take_profit_mode="trailing",
    )


def value_anchor_policy_sha256_v1():
    return canonical_json_sha256({"scenario": SCENARIO, "policy": asdict(value_anchor_policy_v1())})


@dataclass(frozen=True)
class ValueAnchorEstimateV1:
    mean_gross_value_ratio: float
    path_min_ratio_q10: float

    def __post_init__(self):
        finite_number(self.mean_gross_value_ratio, positive=True)
        finite_number(self.path_min_ratio_q10, positive=True)


@dataclass(frozen=True)
class ValueAnchorGapSupportV1:
    intervals_bps: tuple[tuple[float, float], ...]

    def __post_init__(self):
        if not isinstance(self.intervals_bps, tuple):
            raise ValueError("gap support must be immutable")
        previous = None
        for interval in self.intervals_bps:
            if not isinstance(interval, tuple) or len(interval) != 2:
                raise ValueError("gap support interval must be an immutable pair")
            low, high = (finite_number(value) for value in interval)
            if low <= -10000 or low > high or (previous is not None and low <= previous):
                raise ValueError("gap support intervals must be positive-price, ordered and disjoint")
            previous = high

    def contains(self, gap_bps):
        gap = finite_number(gap_bps)
        return any(low <= gap <= high for low, high in self.intervals_bps)


@dataclass(frozen=True)
class ValueAnchorPointV1:
    status: str
    expected_net_bps: float | None
    downside_q90_bps: float | None


@dataclass(frozen=True)
class ValueAnchorPriceSetV1:
    status: str
    intervals_cny: tuple[tuple[float, float], ...]
    evidence_use: str = "NAVIGATION_ONLY"
    valuation_semantics: str = "D_VALUE_ESTIMATE_NOT_T_PRICE_CONDITIONAL_EXPECTATION"
    deployable: bool = False
