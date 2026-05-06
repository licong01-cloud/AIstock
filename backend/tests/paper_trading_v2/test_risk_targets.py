from __future__ import annotations

from backend.services.paper_trading_v2.risk_targets import overlay_risk_forced_exit_targets
from backend.services.selection_center.models import TargetPosition


def _target(symbol: str, *, quantity: int, reason: str) -> TargetPosition:
    return TargetPosition(
        symbol=symbol,
        target_quantity=quantity,
        target_weight=0.03 if quantity else None,
        reference_price=10.0 if quantity else None,
        score=0.9,
        rank=1,
        reason=reason,
    )


def test_overlay_risk_forced_exit_replaces_same_symbol_target_once() -> None:
    base = [
        _target("000001.SZ", quantity=0, reason="qe_backtest_score_weighted_ghost_sell"),
        _target("000002.SZ", quantity=300, reason="qe_backtest_score_weighted_buy"),
    ]
    forced = [_target("000001.SZ", quantity=0, reason="risk_policy_forced_exit")]

    merged = overlay_risk_forced_exit_targets(base, forced)

    assert [target.symbol for target in merged] == ["000001.SZ", "000002.SZ"]
    assert merged[0].reason == "risk_policy_forced_exit"
    assert merged[0].metadata["overrode_target"]["reason"] == "qe_backtest_score_weighted_ghost_sell"
