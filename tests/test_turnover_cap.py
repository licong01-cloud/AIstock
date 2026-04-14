"""Unit tests for _apply_turnover_cap() in TopkDropoutWithRiskControlStrategy.

Validates: Requirements 1.4, 2.1, 2.2, 2.3, 2.4, 2.5
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

import pytest

from backend.rebalance_strategies.topk_dropout_rc import (
    TopkDropoutWithRiskControlStrategy,
)

SIGNAL_DATE = date(2025, 6, 1)
NEXT_TRADE_DATE = date(2025, 6, 2)
PORTFOLIO_ID = 1


def _make_close_price_fn(price_map: Dict[str, float]):
    def fn(symbol: str, d: date) -> Optional[float]:
        return price_map.get(symbol)
    return fn


def _sell_signal(symbol: str, qty: int, reason: str | None = None, score=None):
    sig = {
        "portfolio_id": PORTFOLIO_ID,
        "signal_date": SIGNAL_DATE,
        "trade_date": NEXT_TRADE_DATE,
        "symbol": symbol,
        "side": "SELL",
        "target_quantity": qty,
        "target_weight": 0.0,
        "score": score,
    }
    if reason:
        sig["reason"] = reason
    return sig


def _buy_signal(symbol: str, qty: int, score=None):
    return {
        "portfolio_id": PORTFOLIO_ID,
        "signal_date": SIGNAL_DATE,
        "trade_date": NEXT_TRADE_DATE,
        "symbol": symbol,
        "side": "BUY",
        "target_quantity": qty,
        "target_weight": 0.05,
        "score": score,
    }


class TestApplyTurnoverCap:
    """Tests for _apply_turnover_cap static method."""

    def test_no_truncation_when_under_limit(self):
        """When turnover is under the cap, all signals pass through."""
        # portfolio_value=100000, sell 1000 shares @ 10 = 10000 → 10% < 30%
        signals = [
            _sell_signal("A.SH", 1000),
            _buy_signal("B.SH", 500),
        ]
        price_map = {"A.SH": 10.0, "B.SH": 20.0}
        result = TopkDropoutWithRiskControlStrategy._apply_turnover_cap(
            all_signals=signals,
            portfolio_value=100_000,
            max_daily_turnover_pct=0.30,
            close_price_fn=_make_close_price_fn(price_map),
            signal_date=SIGNAL_DATE,
            topk_symbols=set(),
        )
        assert len(result) == 2

    def test_truncation_removes_low_priority_first(self):
        """Dropout sells (priority 2) are removed before force sells (priority 1)."""
        # 3 sell signals: stop_loss, force_sell (not in topk), dropout_sell (in topk)
        # Each sells 1000 shares @ 20 = 20000 each → total 60000 / 100000 = 60%
        # Cap at 30% → max 30000 → can keep at most 1 signal (20000 ≤ 30000)
        signals = [
            _sell_signal("SL.SH", 1000, reason="stop_loss"),
            _sell_signal("FS.SH", 1000),   # force_sell (not in topk)
            _sell_signal("DS.SH", 1000),   # dropout_sell (in topk)
            _buy_signal("B1.SH", 500),
            _buy_signal("B2.SH", 500),
            _buy_signal("B3.SH", 500),
        ]
        price_map = {"SL.SH": 20.0, "FS.SH": 20.0, "DS.SH": 20.0,
                     "B1.SH": 10.0, "B2.SH": 10.0, "B3.SH": 10.0}
        topk_symbols = {"DS.SH"}  # DS is in topk → dropout_sell

        result = TopkDropoutWithRiskControlStrategy._apply_turnover_cap(
            all_signals=signals,
            portfolio_value=100_000,
            max_daily_turnover_pct=0.30,
            close_price_fn=_make_close_price_fn(price_map),
            signal_date=SIGNAL_DATE,
            topk_symbols=topk_symbols,
        )

        sell_result = [s for s in result if s["side"] == "SELL"]
        buy_result = [s for s in result if s["side"] == "BUY"]
        sell_symbols = {s["symbol"] for s in sell_result}

        # stop_loss has highest priority → must be kept
        assert "SL.SH" in sell_symbols
        # Only 1 sell signal fits (20000 ≤ 30000), so 2 truncated
        assert len(sell_result) == 1
        # Buy signals reduced by 2 (3 - 2 = 1)
        assert len(buy_result) == 1

    def test_stop_loss_always_preserved(self):
        """Stop-loss signals have highest priority and are preserved first."""
        # 2 stop_loss + 1 dropout, each 15000 → total 45000 / 100000 = 45%
        # Cap at 30% → max 30000 → keep 2 stop_loss (30000 exactly)
        signals = [
            _sell_signal("SL1.SH", 1000, reason="stop_loss"),
            _sell_signal("SL2.SH", 1000, reason="stop_loss"),
            _sell_signal("DS.SH", 1000),
            _buy_signal("B1.SH", 500),
            _buy_signal("B2.SH", 500),
            _buy_signal("B3.SH", 500),
        ]
        price_map = {"SL1.SH": 15.0, "SL2.SH": 15.0, "DS.SH": 15.0,
                     "B1.SH": 10.0, "B2.SH": 10.0, "B3.SH": 10.0}

        result = TopkDropoutWithRiskControlStrategy._apply_turnover_cap(
            all_signals=signals,
            portfolio_value=100_000,
            max_daily_turnover_pct=0.30,
            close_price_fn=_make_close_price_fn(price_map),
            signal_date=SIGNAL_DATE,
            topk_symbols={"DS.SH"},
        )

        sell_result = [s for s in result if s["side"] == "SELL"]
        sell_symbols = {s["symbol"] for s in sell_result}
        assert "SL1.SH" in sell_symbols
        assert "SL2.SH" in sell_symbols
        assert "DS.SH" not in sell_symbols

    def test_buy_sell_balance_maintained(self):
        """When sells are truncated, buys are reduced by the same amount."""
        # 4 sells @ 10000 each = 40000 / 100000 = 40%, cap at 20% → max 20000
        # Can keep 2 sells → truncate 2 → reduce buys by 2
        signals = [
            _sell_signal("S1.SH", 1000),
            _sell_signal("S2.SH", 1000),
            _sell_signal("S3.SH", 1000),
            _sell_signal("S4.SH", 1000),
            _buy_signal("B1.SH", 500),
            _buy_signal("B2.SH", 500),
            _buy_signal("B3.SH", 500),
            _buy_signal("B4.SH", 500),
        ]
        price_map = {f"S{i}.SH": 10.0 for i in range(1, 5)}
        price_map.update({f"B{i}.SH": 10.0 for i in range(1, 5)})

        result = TopkDropoutWithRiskControlStrategy._apply_turnover_cap(
            all_signals=signals,
            portfolio_value=100_000,
            max_daily_turnover_pct=0.20,
            close_price_fn=_make_close_price_fn(price_map),
            signal_date=SIGNAL_DATE,
            topk_symbols=set(),
        )

        sell_result = [s for s in result if s["side"] == "SELL"]
        buy_result = [s for s in result if s["side"] == "BUY"]
        # 2 sells kept, 2 truncated → buys reduced by 2 → 2 buys
        assert len(sell_result) == 2
        assert len(buy_result) == 2

    def test_portfolio_value_zero_skips_truncation(self):
        """When portfolio_value <= 0, turnover cap is skipped."""
        signals = [
            _sell_signal("A.SH", 1000),
            _buy_signal("B.SH", 500),
        ]
        price_map = {"A.SH": 10.0, "B.SH": 10.0}
        result = TopkDropoutWithRiskControlStrategy._apply_turnover_cap(
            all_signals=signals,
            portfolio_value=0,
            max_daily_turnover_pct=0.30,
            close_price_fn=_make_close_price_fn(price_map),
            signal_date=SIGNAL_DATE,
            topk_symbols=set(),
        )
        assert len(result) == 2

    def test_no_sell_signals_passes_through(self):
        """When there are no sell signals, all signals pass through."""
        signals = [_buy_signal("B.SH", 500)]
        price_map = {"B.SH": 10.0}
        result = TopkDropoutWithRiskControlStrategy._apply_turnover_cap(
            all_signals=signals,
            portfolio_value=100_000,
            max_daily_turnover_pct=0.30,
            close_price_fn=_make_close_price_fn(price_map),
            signal_date=SIGNAL_DATE,
            topk_symbols=set(),
        )
        assert len(result) == 1

    def test_price_none_treated_as_zero_market_value(self):
        """Sell signals with None price have 0 market value (always fit under cap)."""
        signals = [
            _sell_signal("A.SH", 1000),
            _buy_signal("B.SH", 500),
        ]
        price_map: Dict[str, float] = {}  # No prices → all None
        result = TopkDropoutWithRiskControlStrategy._apply_turnover_cap(
            all_signals=signals,
            portfolio_value=100_000,
            max_daily_turnover_pct=0.30,
            close_price_fn=_make_close_price_fn(price_map),
            signal_date=SIGNAL_DATE,
            topk_symbols=set(),
        )
        # 0 market value → 0% turnover → no truncation
        assert len(result) == 2


class TestGenerateOrdersWithTurnoverCap:
    """Integration test: generate_orders applies turnover cap."""

    def test_turnover_cap_applied_in_generate_orders(self):
        """Verify turnover cap is applied when turnover exceeds limit."""
        strategy = TopkDropoutWithRiskControlStrategy()

        # 20 stocks in score_items, 5 currently held
        # Set a very low turnover cap to force truncation
        score_items = [
            {"symbol": f"S{i:03d}.SH", "score": 100 - i, "rank": i + 1}
            for i in range(30)
        ]
        # Hold stocks ranked 16-20 (will be force_sold since they're outside top-5 with topk=5)
        current_positions = {
            f"S{i:03d}.SH": {"quantity": 1000, "avg_cost": 50.0}
            for i in range(16, 21)
        }
        price_map = {f"S{i:03d}.SH": 50.0 for i in range(30)}

        config = {
            "max_positions": 5,
            "max_turnover_pct": 0.20,
            "max_position_pct": 0.25,
            "risk_degree": 0.95,
            "stop_loss_pct": 0.10,
            "max_daily_turnover_pct": 0.05,  # Very low cap: 5%
        }

        signals = strategy.generate_orders(
            score_items=score_items,
            current_positions=current_positions,
            portfolio_value=1_000_000,
            config=config,
            signal_date=SIGNAL_DATE,
            next_trade_date=NEXT_TRADE_DATE,
            portfolio_id=PORTFOLIO_ID,
            close_price_fn=_make_close_price_fn(price_map),
        )

        sell_signals = [s for s in signals if s["side"] == "SELL"]
        total_sell_mv = sum(
            price_map.get(s["symbol"], 0) * s["target_quantity"]
            for s in sell_signals
        )
        turnover = total_sell_mv / 1_000_000

        # Turnover should not exceed the cap
        assert turnover <= 0.05 + 1e-9, f"Turnover {turnover} exceeds cap 0.05"
