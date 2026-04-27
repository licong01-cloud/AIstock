"""Shared V25 two-stage execution core.

This module contains market-state classification and plan generation logic that
is independent from Paper v2, Qlib, databases, or API objects. Adapters should
translate their runtime objects into this contract instead of re-implementing
V25 semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

import numpy as np


EARLY_WEIGHT = 0.8879
LATE_WEIGHT = 0.1121
EARLY_LEN = 30
LATE_LEN = 210
TOTAL_LEN = 240
DAY_FEATURE_LEN = 10
GAP_RATIO_EDGES = [-0.70, -0.50, -0.30, -0.10, 0.10, 0.30, 0.50, 0.70]

REASON_SUSPENDED_BY_SUSPEND_D = "suspended_by_suspend_d"
REASON_SUSPENDED_BY_EXCHANGE = "suspended_by_exchange"
REASON_INTRADAY_HALT_OR_NO_BAR = "intraday_halt_or_no_bar"
REASON_LIMIT_UP_BUY_BLOCKED = "limit_up_buy_blocked"
REASON_LIMIT_DOWN_SELL_BLOCKED = "limit_down_sell_blocked"
REASON_P0_LIMIT_BUY_AT_DOWN_LIMIT = "p0_limit_buy_at_down_limit"
REASON_P0_LIMIT_SELL_AT_UP_LIMIT = "p0_limit_sell_at_up_limit"
REASON_PREV_CLOSE_MISSING_WITH_SUSPEND = "prev_close_missing_with_suspend"
REASON_PREV_CLOSE_MISSING_DATA_ERROR = "prev_close_missing_data_error"
REASON_LIMIT_DATA_MISSING_DUE_TO_SUSPEND = "limit_data_missing_due_to_suspend"
REASON_LIMIT_PRICE_MISSING_DATA_ERROR = "limit_price_missing_data_error"
REASON_PRICE_MISSING_WITH_SUSPEND = "price_missing_with_suspend"
REASON_PRICE_MISSING_DATA_ERROR = "price_missing_data_error"
REASON_TRADABLE = "tradable"


class V25MarketAction(str, Enum):
    """Action selected after classifying one minute's market state."""

    TRADE = "TRADE"
    SKIP = "SKIP"
    P0_FORCE = "P0_FORCE"
    DATA_ERROR = "DATA_ERROR"


class V25TwoStageCoreError(RuntimeError):
    """Raised when V25 core inputs, assets, or outputs are invalid."""


@dataclass(frozen=True)
class V25MarketState:
    action: V25MarketAction
    reason: str
    context: dict[str, Any] = field(default_factory=dict)

    @property
    def is_error(self) -> bool:
        return self.action == V25MarketAction.DATA_ERROR


@dataclass(frozen=True)
class V25PlanResult:
    weights: np.ndarray
    metadata: dict[str, Any]


EarlyPredictor = Callable[[int, float, float, float, float, np.ndarray], np.ndarray]
LatePredictor = Callable[[int, float, float, float, float, float], np.ndarray]


def infer_limit_pct(stock_id: str) -> float:
    code = str(stock_id).split(".")[0]
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    return 0.10


def gap_ratio_to_bucket(gap_ratio: float) -> int:
    for i, edge in enumerate(GAP_RATIO_EDGES):
        if gap_ratio < edge:
            return i
    return len(GAP_RATIO_EDGES)


def is_positive_finite(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return bool(np.isfinite(number) and number > 0)


def has_suspend_evidence(suspend_status: Any = None, *, is_suspended: bool = False) -> tuple[bool, str]:
    """Return whether the current context has explicit suspension evidence."""

    if is_suspended:
        return True, REASON_SUSPENDED_BY_EXCHANGE
    if isinstance(suspend_status, dict):
        if bool(suspend_status.get("is_suspended")):
            source = str(suspend_status.get("source") or "").lower()
            if "suspend_d" in source or suspend_status.get("suspend_type"):
                return True, REASON_SUSPENDED_BY_SUSPEND_D
            return True, REASON_SUSPENDED_BY_EXCHANGE
    elif bool(suspend_status):
        return True, REASON_SUSPENDED_BY_EXCHANGE
    return False, ""


def classify_v25_minute_market_state(
    *,
    side: str,
    price: Any,
    prev_close: Any,
    limit_up: Any,
    limit_down: Any,
    is_suspended: bool = False,
    suspend_status: Any = None,
    require_limit_price: bool = True,
    price_epsilon: float = 1e-6,
) -> V25MarketState:
    """Classify one minute before V25 execution.

    The classifier is intentionally strict about true data errors but models
    suspension and limit constraints as business states.
    """

    suspended, suspend_reason = has_suspend_evidence(
        suspend_status,
        is_suspended=is_suspended,
    )

    normalized_side = str(side or "").strip().upper()
    if normalized_side not in {"BUY", "SELL"}:
        return V25MarketState(
            V25MarketAction.DATA_ERROR,
            "unsupported_side",
            {"side": side},
        )

    if not is_positive_finite(price):
        if suspended:
            return V25MarketState(
                V25MarketAction.SKIP,
                REASON_PRICE_MISSING_WITH_SUSPEND,
                {"suspend_reason": suspend_reason, "price": price},
            )
        return V25MarketState(V25MarketAction.DATA_ERROR, REASON_PRICE_MISSING_DATA_ERROR, {"price": price})

    if not is_positive_finite(prev_close):
        if suspended:
            return V25MarketState(
                V25MarketAction.SKIP,
                REASON_PREV_CLOSE_MISSING_WITH_SUSPEND,
                {"suspend_reason": suspend_reason, "prev_close": prev_close},
            )
        return V25MarketState(
            V25MarketAction.DATA_ERROR,
            REASON_PREV_CLOSE_MISSING_DATA_ERROR,
            {"prev_close": prev_close},
        )

    limit_missing = not is_positive_finite(limit_up) or not is_positive_finite(limit_down)
    if limit_missing:
        if suspended:
            return V25MarketState(
                V25MarketAction.SKIP,
                REASON_LIMIT_DATA_MISSING_DUE_TO_SUSPEND,
                {
                    "suspend_reason": suspend_reason,
                    "limit_up": limit_up,
                    "limit_down": limit_down,
                },
            )
        if require_limit_price:
            return V25MarketState(
                V25MarketAction.DATA_ERROR,
                REASON_LIMIT_PRICE_MISSING_DATA_ERROR,
                {"limit_up": limit_up, "limit_down": limit_down},
            )
        return V25MarketState(V25MarketAction.TRADE, REASON_TRADABLE)

    if suspended:
        return V25MarketState(V25MarketAction.SKIP, suspend_reason, {"suspend_status": suspend_status})

    px = float(price)
    up = float(limit_up)
    down = float(limit_down)
    if normalized_side == "BUY":
        if px >= up * (1 - price_epsilon):
            return V25MarketState(V25MarketAction.SKIP, REASON_LIMIT_UP_BUY_BLOCKED, {"price": px, "limit_up": up})
        if px <= down * (1 + price_epsilon):
            return V25MarketState(
                V25MarketAction.P0_FORCE,
                REASON_P0_LIMIT_BUY_AT_DOWN_LIMIT,
                {"price": px, "limit_down": down},
            )
    else:
        if px <= down * (1 + price_epsilon):
            return V25MarketState(
                V25MarketAction.SKIP,
                REASON_LIMIT_DOWN_SELL_BLOCKED,
                {"price": px, "limit_down": down},
            )
        if px >= up * (1 - price_epsilon):
            return V25MarketState(
                V25MarketAction.P0_FORCE,
                REASON_P0_LIMIT_SELL_AT_UP_LIMIT,
                {"price": px, "limit_up": up},
            )

    return V25MarketState(V25MarketAction.TRADE, REASON_TRADABLE)


class V25TwoStageCore:
    """Pure V25 plan-generation core with injected model predictors."""

    def __init__(
        self,
        *,
        early_predictor: EarlyPredictor,
        late_predictor: LatePredictor,
    ) -> None:
        self.early_predictor = early_predictor
        self.late_predictor = late_predictor

    def generate_plan(
        self,
        *,
        open_price: float,
        prev_close: float,
        stock_id: str,
        side: str,
        limit_pct: float | None = None,
        day_features: np.ndarray,
    ) -> V25PlanResult:
        if not is_positive_finite(open_price) or not is_positive_finite(prev_close):
            raise V25TwoStageCoreError("V25 plan requires positive open_price and prev_close")
        normalized_side = str(side or "").strip().upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise V25TwoStageCoreError(f"V25 unsupported side: {side}")
        lp = float(limit_pct if limit_pct is not None else infer_limit_pct(stock_id))
        if not is_positive_finite(lp):
            raise V25TwoStageCoreError("V25 plan requires positive limit_pct")

        features = np.asarray(day_features, dtype=np.float32)
        if features.shape != (DAY_FEATURE_LEN,) or np.isnan(features).any():
            raise V25TwoStageCoreError("V25 day_features must be a 10-element finite array")

        gap_pct = float(np.clip((float(open_price) - float(prev_close)) / float(prev_close), -0.20, 0.20))
        gap_ratio = float(gap_pct / lp)
        gap_bucket = gap_ratio_to_bucket(gap_ratio)
        is_buy_value = 1.0 if normalized_side == "BUY" else 0.0

        pred_early = np.asarray(
            self.early_predictor(
                gap_bucket,
                abs(gap_ratio),
                gap_ratio,
                lp,
                is_buy_value,
                features,
            ),
            dtype=np.float64,
        )
        if pred_early.shape != (EARLY_LEN,) or np.isnan(pred_early).any() or pred_early.sum() <= 1e-8:
            raise V25TwoStageCoreError(
                f"V25 early model returned invalid plan: shape={pred_early.shape} sum={float(pred_early.sum())}"
            )

        early_weight_raw = float(pred_early.sum())
        early_peak_pos = float(pred_early.argmax() / max(EARLY_LEN - 1, 1))
        early_mean = float(pred_early.mean())
        early_concentration = float(pred_early.max() / (early_mean + 1e-8))

        pred_late = np.asarray(
            self.late_predictor(
                gap_bucket,
                abs(gap_ratio),
                is_buy_value,
                early_weight_raw,
                early_peak_pos,
                early_concentration,
            ),
            dtype=np.float64,
        )
        if pred_late.shape != (LATE_LEN,) or np.isnan(pred_late).any() or pred_late.sum() <= 1e-8:
            raise V25TwoStageCoreError(
                f"V25 late model returned invalid plan: shape={pred_late.shape} sum={float(pred_late.sum())}"
            )

        plan = np.concatenate([pred_early * EARLY_WEIGHT, pred_late * LATE_WEIGHT]).astype(np.float64)
        if len(plan) != TOTAL_LEN or np.isnan(plan).any() or plan.sum() <= 1e-8:
            raise V25TwoStageCoreError(
                f"V25 generated invalid plan: len={len(plan)} sum={float(plan.sum())}"
            )
        plan = plan / plan.sum()
        early_sum = float(plan[:EARLY_LEN].sum())
        late_sum = float(plan[EARLY_LEN:].sum())
        if abs(early_sum - EARLY_WEIGHT) > 1e-4 or abs(late_sum - LATE_WEIGHT) > 1e-4:
            raise V25TwoStageCoreError(
                f"V25 plan weight mismatch: early={early_sum:.6f} late={late_sum:.6f}"
            )
        return V25PlanResult(
            weights=plan,
            metadata={
                "gap_ratio": gap_ratio,
                "gap_bucket": gap_bucket,
                "early_sum": early_sum,
                "late_sum": late_sum,
                "plan_horizon_bars": TOTAL_LEN,
            },
        )
