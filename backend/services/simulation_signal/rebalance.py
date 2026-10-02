"""Pure desired-weight rebalance evidence; no account or broker access."""

from __future__ import annotations

from typing import Mapping

from .contracts import RebalanceIntent, TargetPortfolio, canonical_json_sha256


class RebalanceIntentService:
    def compare_frozen_allocations(
        self,
        *,
        target: TargetPortfolio,
        previous_target_weights: Mapping[str, float],
    ) -> RebalanceIntent:
        previous = {str(symbol): float(weight) for symbol, weight in previous_target_weights.items()}
        if any(weight < 0 or weight > 1 for weight in previous.values()) or sum(previous.values()) > 1.000000001:
            raise ValueError("previous target weights are invalid")
        symbols = sorted(set(target.weights).union(previous))
        deltas = {symbol: float(target.weights.get(symbol, 0.0)) - previous.get(symbol, 0.0) for symbol in symbols}
        payload = {
            "target_hash": target.target_hash,
            "trade_date": target.trade_date.isoformat(),
            "desired_weight_delta": deltas,
        }
        return RebalanceIntent(
            target_hash=target.target_hash,
            trade_date=target.trade_date,
            desired_weight_delta=deltas,
            intent_hash=canonical_json_sha256(payload),
        )
