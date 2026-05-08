"""Capacity-parameterized ScoreWeightedTopkStrategyV2 wrapper.

This strategy keeps the V2 sell/buy semantics intact and changes only the
default capacity profile for new QE experiments.  Legacy
``score_weighted_topk_v2`` catalog rows should continue to point at the old
source/defaults with the 5M single-order cap.
"""

from score_weighted_strategy_v2 import ScoreWeightedTopkStrategyV2


class ScoreWeightedTopkStrategyV2CapacityV1(ScoreWeightedTopkStrategyV2):
    """ScoreWeighted V2 with explicit high-capacity defaults."""

    def __init__(
        self,
        *args,
        max_single_order_value=1_000_000_000.0,
        max_weight=0.05,
        max_position_ratio=0.95,
        **kwargs,
    ):
        super().__init__(
            *args,
            max_single_order_value=max_single_order_value,
            max_weight=max_weight,
            max_position_ratio=max_position_ratio,
            **kwargs,
        )
