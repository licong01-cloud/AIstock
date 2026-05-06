"""Paper v2 target helpers for event-risk forced exits."""

from __future__ import annotations

from typing import Any

from backend.services.selection_center.models import TargetPosition


def overlay_risk_forced_exit_targets(
    targets: list[TargetPosition],
    forced_exit_targets: list[TargetPosition],
) -> list[TargetPosition]:
    """Let risk forced exits override existing same-symbol targets once.

    The QE-style target engine can already create a zero target for a holding
    that leaves the score universe. ST PIT risk policy is stricter and should
    be the operator-facing reason, but the final target list must not contain
    duplicate symbols before rebalance intent generation.
    """

    if not forced_exit_targets:
        return list(targets)

    order: list[str] = []
    by_symbol: dict[str, TargetPosition] = {}
    for target in targets:
        if target.symbol not in by_symbol:
            order.append(target.symbol)
        by_symbol[target.symbol] = target

    for forced in forced_exit_targets:
        previous = by_symbol.get(forced.symbol)
        replacement = forced
        if previous is not None:
            metadata: dict[str, Any] = dict(forced.metadata or {})
            metadata["overrode_target"] = {
                "reason": previous.reason,
                "target_quantity": previous.target_quantity,
                "target_weight": previous.target_weight,
                "rank": previous.rank,
            }
            replacement = forced.model_copy(update={"metadata": metadata})
        else:
            order.append(forced.symbol)
        by_symbol[forced.symbol] = replacement

    return [by_symbol[symbol] for symbol in order]
