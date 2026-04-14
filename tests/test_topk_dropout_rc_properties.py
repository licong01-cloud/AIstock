"""Property-based tests for TopkDropoutWithRiskControlStrategy.

Feature: p4-p1-p5-strategy-enhancement
Uses hypothesis to verify correctness properties of the risk control strategy.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from backend.rebalance_strategies.topk_dropout_rc import (
    TopkDropoutWithRiskControlStrategy,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIGNAL_DATE = date(2025, 6, 1)
NEXT_TRADE_DATE = date(2025, 6, 2)
PORTFOLIO_ID = 1


def _make_close_price_fn(price_map: Dict[str, float]):
    """Create a close_price_fn callable that returns prices from a dict."""
    def close_price_fn(symbol: str, d: date) -> Optional[float]:
        return price_map.get(symbol)
    return close_price_fn


# ---------------------------------------------------------------------------
# Property 1: 止损信号生成
# Feature: p4-p1-p5-strategy-enhancement, Property 1: 止损信号生成
# Validates: Requirements 1.1, 1.2, 1.3
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(
    avg_cost=st.floats(min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
    quantity=st.integers(min_value=100, max_value=10000),
    stop_loss_pct=st.floats(min_value=0.01, max_value=0.99, allow_nan=False, allow_infinity=False),
    # price_ratio relative to threshold: values in [0, 2] cover both trigger and no-trigger
    price_ratio=st.floats(min_value=0.0, max_value=2.0, allow_nan=False, allow_infinity=False),
)
def test_property_1_stop_loss_signal_generation(
    avg_cost: float,
    quantity: int,
    stop_loss_pct: float,
    price_ratio: float,
):
    """Property 1: 止损信号生成

    For any position (avg_cost, quantity) and any price, when
    price ≤ avg_cost × (1 - stop_loss_pct), the signal list must contain
    a SELL signal with reason="stop_loss" and target_quantity == quantity.

    **Validates: Requirements 1.1, 1.2, 1.3**
    """
    # Derive the threshold and the actual price from the ratio
    threshold = avg_cost * (1 - stop_loss_pct)
    # price_ratio maps [0, 2] around the threshold:
    #   ratio < 1.0 → price below threshold (should trigger)
    #   ratio >= 1.0 → price at or above threshold (may or may not trigger)
    price = threshold * price_ratio

    # Ensure price is a valid positive float
    assume(price >= 0.0)

    symbol = "TEST.SH"
    current_positions: Dict[str, Dict[str, Any]] = {
        symbol: {"avg_cost": avg_cost, "quantity": quantity},
    }
    price_map = {symbol: price}

    strategy = TopkDropoutWithRiskControlStrategy()
    signals = strategy._check_stop_loss(
        current_positions=current_positions,
        close_price_fn=_make_close_price_fn(price_map),
        signal_date=SIGNAL_DATE,
        next_trade_date=NEXT_TRADE_DATE,
        portfolio_id=PORTFOLIO_ID,
        stop_loss_pct=stop_loss_pct,
    )

    should_trigger = price <= threshold

    if should_trigger:
        # Must contain exactly one SELL signal for this symbol
        matching = [s for s in signals if s["symbol"] == symbol]
        assert len(matching) == 1, (
            f"Expected 1 stop-loss signal for {symbol}, got {len(matching)}. "
            f"price={price}, threshold={threshold}"
        )
        sig = matching[0]
        assert sig["side"] == "SELL"
        assert sig["reason"] == "stop_loss"
        assert sig["target_quantity"] == quantity
        assert sig["portfolio_id"] == PORTFOLIO_ID
        assert sig["signal_date"] == SIGNAL_DATE
        assert sig["trade_date"] == NEXT_TRADE_DATE
    else:
        # No stop-loss signal should be generated for this symbol
        matching = [s for s in signals if s["symbol"] == symbol]
        assert len(matching) == 0, (
            f"Expected no stop-loss signal for {symbol}, got {len(matching)}. "
            f"price={price}, threshold={threshold}"
        )


# ---------------------------------------------------------------------------
# Property 4: 非法参数回退到默认值
# Feature: p4-p1-p5-strategy-enhancement, Property 4: 非法参数回退到默认值
# Validates: Requirements 3.3, 3.4
# ---------------------------------------------------------------------------

# Default values from topk_dropout_rc.py
_DEFAULT_STOP_LOSS_PCT = 0.10
_DEFAULT_MAX_DAILY_TURNOVER_PCT = 0.30


@settings(max_examples=100)
@given(
    stop_loss_pct=st.one_of(
        # ≤ 0: negative or zero
        st.floats(max_value=0.0, allow_nan=False, allow_infinity=False),
        # ≥ 1: one or above
        st.floats(min_value=1.0, allow_nan=False, allow_infinity=False),
    ),
)
def test_property_4_invalid_stop_loss_pct_fallback(stop_loss_pct: float):
    """Property 4 (stop_loss_pct): 非法参数回退到默认值

    For any stop_loss_pct value ≤ 0 or ≥ 1, _validate_risk_params should
    return the default value 0.10.

    **Validates: Requirements 3.3, 3.4**
    """
    config = {"stop_loss_pct": stop_loss_pct}
    result = TopkDropoutWithRiskControlStrategy._validate_risk_params(config)
    assert result["stop_loss_pct"] == _DEFAULT_STOP_LOSS_PCT, (
        f"stop_loss_pct={stop_loss_pct} should fallback to default "
        f"{_DEFAULT_STOP_LOSS_PCT}, got {result['stop_loss_pct']}"
    )


@settings(max_examples=100)
@given(
    max_daily_turnover_pct=st.one_of(
        # ≤ 0: negative or zero
        st.floats(max_value=0.0, allow_nan=False, allow_infinity=False),
        # > 1: strictly above one
        st.floats(min_value=1.0, allow_nan=False, allow_infinity=False).filter(
            lambda x: x != 1.0
        ),
    ),
)
def test_property_4_invalid_max_daily_turnover_pct_fallback(
    max_daily_turnover_pct: float,
):
    """Property 4 (max_daily_turnover_pct): 非法参数回退到默认值

    For any max_daily_turnover_pct value ≤ 0 or > 1, _validate_risk_params
    should return the default value 0.30.

    **Validates: Requirements 3.3, 3.4**
    """
    config = {"max_daily_turnover_pct": max_daily_turnover_pct}
    result = TopkDropoutWithRiskControlStrategy._validate_risk_params(config)
    assert result["max_daily_turnover_pct"] == _DEFAULT_MAX_DAILY_TURNOVER_PCT, (
        f"max_daily_turnover_pct={max_daily_turnover_pct} should fallback to "
        f"default {_DEFAULT_MAX_DAILY_TURNOVER_PCT}, got "
        f"{result['max_daily_turnover_pct']}"
    )


@settings(max_examples=100)
@given(
    non_numeric=st.one_of(
        st.text(min_size=1, max_size=10),
        st.just(None),
        st.lists(st.integers(), min_size=1, max_size=3),
        st.dictionaries(st.text(max_size=3), st.integers(), min_size=1, max_size=2),
        st.just(object()),
    ),
    param_name=st.sampled_from(["stop_loss_pct", "max_daily_turnover_pct"]),
)
def test_property_4_non_numeric_values_fallback(non_numeric, param_name: str):
    """Property 4 (non-numeric): 非法参数回退到默认值

    For any non-numeric value (strings, None, lists, dicts, objects),
    _validate_risk_params should return the corresponding default value.

    **Validates: Requirements 3.3, 3.4**
    """
    # Skip strings that happen to be valid floats in range
    if isinstance(non_numeric, str):
        try:
            v = float(non_numeric)
            if param_name == "stop_loss_pct" and 0 < v < 1:
                assume(False)
            if param_name == "max_daily_turnover_pct" and 0 < v <= 1:
                assume(False)
        except (ValueError, TypeError):
            pass  # Not a valid float string — good, keep going

    config = {param_name: non_numeric}
    result = TopkDropoutWithRiskControlStrategy._validate_risk_params(config)

    if param_name == "stop_loss_pct":
        expected = _DEFAULT_STOP_LOSS_PCT
        # None triggers the "if raw_sl is None" branch → default
        assert result["stop_loss_pct"] == expected, (
            f"stop_loss_pct with non-numeric {non_numeric!r} should fallback "
            f"to {expected}, got {result['stop_loss_pct']}"
        )
    else:
        expected = _DEFAULT_MAX_DAILY_TURNOVER_PCT
        assert result["max_daily_turnover_pct"] == expected, (
            f"max_daily_turnover_pct with non-numeric {non_numeric!r} should "
            f"fallback to {expected}, got {result['max_daily_turnover_pct']}"
        )


# ---------------------------------------------------------------------------
# Property 2: 规则优先级与 Dropout 排除
# Feature: p4-p1-p5-strategy-enhancement, Property 2: 规则优先级与 Dropout 排除
# Validates: Requirements 1.4
# ---------------------------------------------------------------------------

# Strategy for generating a set of stock symbols
_stock_symbols_st = st.lists(
    st.from_regex(r"[A-Z]{2}[0-9]{4}\.[A-Z]{2}", fullmatch=True),
    min_size=3,
    max_size=15,
    unique=True,
)


@st.composite
def _positions_with_stop_loss(draw):
    """Generate a scenario with some stocks triggering stop-loss and some not.

    Returns (score_items, current_positions, price_map, config, stop_loss_pct,
             expected_stop_loss_symbols).
    """
    # Generate between 3 and 12 unique symbols
    n_stocks = draw(st.integers(min_value=3, max_value=12))
    symbols = [f"S{i:04d}.SH" for i in range(n_stocks)]

    # At least 1 stock triggers stop-loss, at least 1 does not
    n_stop_loss = draw(st.integers(min_value=1, max_value=max(1, n_stocks - 1)))
    stop_loss_symbols = set(symbols[:n_stop_loss])
    safe_symbols = set(symbols[n_stop_loss:])

    # Risk param
    stop_loss_pct = draw(
        st.floats(min_value=0.05, max_value=0.50, allow_nan=False, allow_infinity=False)
    )

    avg_cost = draw(
        st.floats(min_value=10.0, max_value=500.0, allow_nan=False, allow_infinity=False)
    )

    # Build positions and prices
    current_positions: Dict[str, Dict[str, Any]] = {}
    price_map: Dict[str, float] = {}

    for sym in symbols:
        quantity = draw(st.integers(min_value=100, max_value=5000))
        current_positions[sym] = {
            "avg_cost": avg_cost,
            "quantity": quantity,
        }

        threshold = avg_cost * (1 - stop_loss_pct)
        if sym in stop_loss_symbols:
            # Price at or below threshold → triggers stop-loss
            price = draw(
                st.floats(
                    min_value=0.01,
                    max_value=threshold,
                    allow_nan=False,
                    allow_infinity=False,
                )
            )
        else:
            # Price strictly above threshold → does NOT trigger stop-loss
            price = draw(
                st.floats(
                    min_value=threshold * 1.01,
                    max_value=avg_cost * 2.0,
                    allow_nan=False,
                    allow_infinity=False,
                )
            )
        price_map[sym] = price

    # Build score_items: all stocks get a score, descending order
    scores = sorted(
        [
            draw(st.floats(min_value=0.01, max_value=10.0, allow_nan=False, allow_infinity=False))
            for _ in symbols
        ],
        reverse=True,
    )
    score_items = [
        {"symbol": sym, "score": sc, "rank": i + 1}
        for i, (sym, sc) in enumerate(zip(symbols, scores))
    ]

    # Config: topk large enough to include all stocks so dropout logic runs
    config = {
        "max_positions": max(n_stocks, 5),
        "max_turnover_pct": 0.15,
        "max_position_pct": 0.10,
        "risk_degree": 0.95,
        "stop_loss_pct": stop_loss_pct,
        "max_daily_turnover_pct": 1.0,  # no turnover cap interference
    }

    return (
        score_items,
        current_positions,
        price_map,
        config,
        stop_loss_pct,
        stop_loss_symbols,
    )


@settings(max_examples=100)
@given(data=_positions_with_stop_loss())
def test_property_2_rule_priority_and_dropout_exclusion(data):
    """Property 2: 规则优先级与 Dropout 排除

    For any position set, stocks triggered by stop-loss should NOT appear
    in TopK dropout sell signals — there must be no duplicate sell signals
    for the same stock.

    This verifies that generate_orders removes stop-loss stocks from
    score_items and current_positions before calling super().generate_orders().

    **Validates: Requirements 1.4**
    """
    (
        score_items,
        current_positions,
        price_map,
        config,
        stop_loss_pct,
        expected_stop_loss_symbols,
    ) = data

    portfolio_value = 1_000_000.0

    strategy = TopkDropoutWithRiskControlStrategy()
    signals = strategy.generate_orders(
        score_items=score_items,
        current_positions=current_positions,
        portfolio_value=portfolio_value,
        config=config,
        signal_date=SIGNAL_DATE,
        next_trade_date=NEXT_TRADE_DATE,
        portfolio_id=PORTFOLIO_ID,
        close_price_fn=_make_close_price_fn(price_map),
    )

    # Collect stop-loss sell symbols and non-stop-loss sell symbols
    stop_loss_sell_symbols = set()
    other_sell_symbols = set()

    for sig in signals:
        if sig["side"] != "SELL":
            continue
        sym = sig["symbol"]
        reason = sig.get("reason")
        if reason == "stop_loss":
            stop_loss_sell_symbols.add(sym)
        else:
            other_sell_symbols.add(sym)

    # 1) No stock should appear in BOTH stop_loss sells AND other sells
    #    (core property: stop-loss stocks are removed before dropout logic)
    overlap = stop_loss_sell_symbols & other_sell_symbols
    assert not overlap, (
        f"Stocks appear in both stop_loss and dropout/force_sell signals: {overlap}. "
        f"stop_loss_sells={stop_loss_sell_symbols}, other_sells={other_sell_symbols}"
    )

    # 2) Each symbol should appear at most once across all SELL signals
    sell_symbols_list = [sig["symbol"] for sig in signals if sig["side"] == "SELL"]
    assert len(sell_symbols_list) == len(set(sell_symbols_list)), (
        f"Duplicate SELL signals detected: {sell_symbols_list}"
    )

    # 3) Stop-loss stocks must NOT appear in non-stop-loss (dropout/force_sell) signals
    #    even if turnover cap truncates some stop-loss signals themselves
    for sym in expected_stop_loss_symbols:
        assert sym not in other_sell_symbols, (
            f"Stop-loss stock {sym} appeared in dropout/force_sell signals. "
            f"other_sells={other_sell_symbols}"
        )


# ---------------------------------------------------------------------------
# Property 3: 换手率上限截断
# Feature: p4-p1-p5-strategy-enhancement, Property 3: 换手率上限截断
# Validates: Requirements 2.2, 2.3, 2.4
# ---------------------------------------------------------------------------


@st.composite
def _turnover_cap_scenario(draw):
    """Generate a scenario with sell signals that exceed the turnover cap.

    Returns (all_signals, portfolio_value, max_daily_turnover_pct,
             price_map, topk_symbols, original_sell_count, original_buy_count).
    """
    # Number of sell and buy signals
    n_sell = draw(st.integers(min_value=2, max_value=10))
    n_buy = draw(st.integers(min_value=1, max_value=10))

    # Portfolio value — large enough to be meaningful
    portfolio_value = draw(
        st.floats(min_value=100_000.0, max_value=10_000_000.0,
                  allow_nan=False, allow_infinity=False)
    )

    # Turnover cap — intentionally low so truncation is likely
    max_daily_turnover_pct = draw(
        st.floats(min_value=0.05, max_value=0.50,
                  allow_nan=False, allow_infinity=False)
    )

    # Generate sell signals with varying prices and quantities
    sell_signals: List[Dict[str, Any]] = []
    price_map: Dict[str, float] = {}
    topk_symbols: set = set()

    # Decide how many of each priority type
    n_stop_loss = draw(st.integers(min_value=0, max_value=min(3, n_sell)))
    n_remaining = n_sell - n_stop_loss

    for i in range(n_sell):
        symbol = f"SELL{i:04d}.SH"
        price = draw(
            st.floats(min_value=1.0, max_value=200.0,
                      allow_nan=False, allow_infinity=False)
        )
        quantity = draw(st.integers(min_value=100, max_value=5000))
        price_map[symbol] = price

        if i < n_stop_loss:
            reason = "stop_loss"
        else:
            reason = None

        # For non-stop-loss signals, randomly assign to topk or not
        if i >= n_stop_loss:
            in_topk = draw(st.booleans())
            if in_topk:
                topk_symbols.add(symbol)

        sell_signals.append({
            "portfolio_id": PORTFOLIO_ID,
            "signal_date": SIGNAL_DATE,
            "trade_date": NEXT_TRADE_DATE,
            "symbol": symbol,
            "side": "SELL",
            "target_quantity": quantity,
            "target_weight": 0.0,
            "score": None,
            "reason": reason,
        })

    # Generate buy signals
    buy_signals: List[Dict[str, Any]] = []
    for i in range(n_buy):
        symbol = f"BUY{i:04d}.SH"
        price = draw(
            st.floats(min_value=1.0, max_value=200.0,
                      allow_nan=False, allow_infinity=False)
        )
        price_map[symbol] = price
        buy_signals.append({
            "portfolio_id": PORTFOLIO_ID,
            "signal_date": SIGNAL_DATE,
            "trade_date": NEXT_TRADE_DATE,
            "symbol": symbol,
            "side": "BUY",
            "target_quantity": draw(st.integers(min_value=100, max_value=5000)),
            "target_weight": 0.0,
            "score": draw(
                st.floats(min_value=0.01, max_value=10.0,
                          allow_nan=False, allow_infinity=False)
            ),
            "reason": None,
        })

    all_signals = sell_signals + buy_signals

    # Ensure the scenario actually exceeds the turnover cap so truncation fires
    total_sell_mv = sum(
        price_map[s["symbol"]] * s["target_quantity"]
        for s in sell_signals
    )
    assume(total_sell_mv / portfolio_value > max_daily_turnover_pct)

    return (
        all_signals,
        portfolio_value,
        max_daily_turnover_pct,
        price_map,
        topk_symbols,
        len(sell_signals),
        len(buy_signals),
    )


@settings(max_examples=100)
@given(data=_turnover_cap_scenario())
def test_property_3_turnover_cap_truncation(data):
    """Property 3: 换手率上限截断

    For any set of signals that exceeds the turnover cap:
    1. After truncation, total sell market value / portfolio_value
       does not exceed max_daily_turnover_pct.
    2. Buy signal count is reduced by the number of truncated sell signals.
    3. Higher priority signals (stop_loss) are preserved over lower priority ones.

    **Validates: Requirements 2.2, 2.3, 2.4**
    """
    (
        all_signals,
        portfolio_value,
        max_daily_turnover_pct,
        price_map,
        topk_symbols,
        original_sell_count,
        original_buy_count,
    ) = data

    close_price_fn = _make_close_price_fn(price_map)

    result = TopkDropoutWithRiskControlStrategy._apply_turnover_cap(
        all_signals=all_signals,
        portfolio_value=portfolio_value,
        max_daily_turnover_pct=max_daily_turnover_pct,
        close_price_fn=close_price_fn,
        signal_date=SIGNAL_DATE,
        topk_symbols=topk_symbols,
    )

    result_sell = [s for s in result if s["side"] == "SELL"]
    result_buy = [s for s in result if s["side"] == "BUY"]

    # --- Assertion 1: Turnover rate does not exceed the cap ---
    total_sell_mv_after = sum(
        (price_map.get(s["symbol"], 0.0) * s.get("target_quantity", 0))
        for s in result_sell
    )
    actual_turnover = total_sell_mv_after / portfolio_value
    assert actual_turnover <= max_daily_turnover_pct + 1e-9, (
        f"Turnover {actual_turnover:.6f} exceeds cap {max_daily_turnover_pct:.6f}. "
        f"sell_mv={total_sell_mv_after:.2f} portfolio={portfolio_value:.2f}"
    )

    # --- Assertion 2: Buy signals reduced = sell signals truncated ---
    sell_truncated = original_sell_count - len(result_sell)
    expected_buy_count = max(0, original_buy_count - sell_truncated)
    assert len(result_buy) == expected_buy_count, (
        f"Expected {expected_buy_count} buy signals "
        f"(original={original_buy_count} - truncated_sells={sell_truncated}), "
        f"got {len(result_buy)}"
    )

    # --- Assertion 3: Higher priority signals preserved over lower ---
    # Collect which stop_loss signals were in the input
    input_stop_loss_symbols = {
        s["symbol"] for s in all_signals
        if s["side"] == "SELL" and s.get("reason") == "stop_loss"
    }
    result_sell_symbols = {s["symbol"] for s in result_sell}

    # If any non-stop-loss sell signal is kept, then ALL stop_loss signals
    # must also be kept (stop_loss has highest priority = 0).
    result_non_stop_loss_sells = {
        s["symbol"] for s in result_sell
        if s.get("reason") != "stop_loss"
    }
    if result_non_stop_loss_sells:
        missing_stop_loss = input_stop_loss_symbols - result_sell_symbols
        assert not missing_stop_loss, (
            f"Stop-loss signals {missing_stop_loss} were truncated while "
            f"lower-priority signals {result_non_stop_loss_sells} were kept. "
            f"Priority violation: stop_loss should be preserved first."
        )


# ---------------------------------------------------------------------------
# Property 10: 行业热度系数调整评分
# Feature: p4-p1-p5-strategy-enhancement, Property 10: 行业热度系数调整评分
# Validates: Requirements 6.2, 6.3
# ---------------------------------------------------------------------------

from unittest.mock import patch, MagicMock


@st.composite
def _sector_hmm_adjustment_scenario(draw):
    """Generate random score_items, sector coefficients, and stock-sector mapping.

    Returns (score_items, sector_coefficients, stock_sector_map, config).
    """
    # Generate between 2 and 20 stocks
    n_stocks = draw(st.integers(min_value=2, max_value=20))

    # Generate unique symbols
    symbols = [f"STK{i:04d}.SH" for i in range(n_stocks)]

    # Generate 2-5 sector codes
    n_sectors = draw(st.integers(min_value=2, max_value=5))
    sector_codes = [f"80{i:04d}.SI" for i in range(n_sectors)]

    # Generate sector coefficients: each is one of {0.5, 1.0, 1.5}
    sector_coefficients: Dict[str, float] = {}
    for sc in sector_codes:
        sector_coefficients[sc] = draw(st.sampled_from([0.5, 1.0, 1.5]))

    # Map each stock to a sector (some stocks may not have a mapping)
    stock_sector_map: Dict[str, str] = {}
    for sym in symbols:
        has_mapping = draw(st.booleans())
        if has_mapping:
            stock_sector_map[sym] = draw(st.sampled_from(sector_codes))

    # Generate score_items with positive scores
    score_items: List[Dict[str, Any]] = []
    for i, sym in enumerate(symbols):
        score = draw(
            st.floats(min_value=0.01, max_value=100.0,
                      allow_nan=False, allow_infinity=False)
        )
        score_items.append({
            "symbol": sym,
            "score": score,
            "rank": i + 1,
        })

    # Sort by score descending (as expected by the method)
    score_items.sort(key=lambda x: x["score"], reverse=True)

    config = {
        "enable_sector_hmm": True,
        "sector_hmm_model_path": "/fake/model/path",
    }

    return score_items, sector_coefficients, stock_sector_map, config


@settings(max_examples=100)
@given(data=_sector_hmm_adjustment_scenario())
def test_property_10_sector_heat_coefficient_score_adjustment(data):
    """Property 10: 行业热度系数调整评分

    For any score_items list and sector heat coefficient dict, when
    enable_sector_hmm is True, the adjusted score for each stock should
    equal original_score × sector_coefficient, and the returned list
    should be sorted by adjusted score in descending order.

    **Validates: Requirements 6.2, 6.3**
    """
    score_items, sector_coefficients, stock_sector_map, config = data

    strategy = TopkDropoutWithRiskControlStrategy()

    # Build expected adjusted scores before calling the method
    expected_adjusted: Dict[str, float] = {}
    for item in score_items:
        sym = item["symbol"]
        original_score = float(item["score"])
        sector_code = stock_sector_map.get(sym)
        if sector_code and sector_code in sector_coefficients:
            coeff = sector_coefficients[sector_code]
        else:
            coeff = 1.0
        expected_adjusted[sym] = original_score * coeff

    # Mock SectorHMMInference to return our generated coefficients
    mock_inference_instance = MagicMock()
    mock_inference_instance.get_sector_coefficients.return_value = sector_coefficients

    with patch(
        "backend.rebalance_strategies.topk_dropout_rc.TopkDropoutWithRiskControlStrategy._get_stock_sector_map",
        return_value=stock_sector_map,
    ), patch(
        "backend.quant_models.hmm.sector_hmm.SectorHMMInference",
        return_value=mock_inference_instance,
    ):
        result = strategy._apply_sector_hmm_adjustment(
            score_items=score_items,
            signal_date=SIGNAL_DATE,
            config=config,
        )

    # --- Assertion 1: Each adjusted score = original score × coefficient ---
    assert len(result) == len(score_items), (
        f"Result length {len(result)} != input length {len(score_items)}"
    )

    for item in result:
        sym = item["symbol"]
        actual_score = float(item["score"])
        expected_score = expected_adjusted[sym]
        assert abs(actual_score - expected_score) < 1e-9, (
            f"Stock {sym}: adjusted score {actual_score} != "
            f"expected {expected_score} "
            f"(original × coeff)"
        )

    # --- Assertion 2: Result is sorted by adjusted score descending ---
    scores = [float(item["score"]) for item in result]
    for i in range(len(scores) - 1):
        assert scores[i] >= scores[i + 1], (
            f"Result not sorted descending at index {i}: "
            f"{scores[i]} < {scores[i + 1]}. "
            f"Full scores: {scores}"
        )


# ---------------------------------------------------------------------------
# Property 11: 行业热度开关关闭时评分不变
# Feature: p4-p1-p5-strategy-enhancement, Property 11: 行业热度开关关闭时评分不变
# Validates: Requirements 6.4
# ---------------------------------------------------------------------------


@st.composite
def _score_items_for_hmm_disabled(draw):
    """Generate random score_items for testing HMM-disabled scenario.

    Returns (score_items, config_variant) where config_variant is one of:
    - "explicit_false": enable_sector_hmm=False
    - "missing_key": enable_sector_hmm not present in config
    - "falsy_value": enable_sector_hmm set to a falsy value (0, None, "")
    """
    n_stocks = draw(st.integers(min_value=1, max_value=30))
    symbols = [f"HMM{i:04d}.SH" for i in range(n_stocks)]

    score_items: List[Dict[str, Any]] = []
    for i, sym in enumerate(symbols):
        score = draw(
            st.floats(
                min_value=-100.0, max_value=100.0,
                allow_nan=False, allow_infinity=False,
            )
        )
        score_items.append({
            "symbol": sym,
            "score": score,
            "rank": i + 1,
        })

    # Sort descending by score (as expected by the method)
    score_items.sort(key=lambda x: x["score"], reverse=True)

    config_variant = draw(st.sampled_from([
        "explicit_false",
        "missing_key",
        "falsy_value",
    ]))

    return score_items, config_variant


@settings(max_examples=100)
@given(data=_score_items_for_hmm_disabled())
def test_property_11_scores_unchanged_when_sector_hmm_disabled(data):
    """Property 11: 行业热度开关关闭时评分不变

    For any score_items list, when enable_sector_hmm is False or not set
    in config, _apply_sector_hmm_adjustment should return score_items with
    scores identical to the original input — no adjustment should occur.

    Feature: p4-p1-p5-strategy-enhancement, Property 11: 行业热度开关关闭时评分不变

    **Validates: Requirements 6.4**
    """
    score_items, config_variant = data

    # Build config based on variant
    if config_variant == "explicit_false":
        config: Dict[str, Any] = {"enable_sector_hmm": False}
    elif config_variant == "missing_key":
        config = {}  # enable_sector_hmm not present at all
    else:  # falsy_value
        config = {"enable_sector_hmm": 0}

    # Record original scores keyed by symbol for comparison
    original_scores = {item["symbol"]: item["score"] for item in score_items}

    strategy = TopkDropoutWithRiskControlStrategy()
    result = strategy._apply_sector_hmm_adjustment(
        score_items=score_items,
        signal_date=SIGNAL_DATE,
        config=config,
    )

    # --- Assertion 1: Same number of items returned ---
    assert len(result) == len(score_items), (
        f"Result length {len(result)} != input length {len(score_items)} "
        f"when enable_sector_hmm is disabled (variant={config_variant})"
    )

    # --- Assertion 2: Each score is identical to the original ---
    for item in result:
        sym = item["symbol"]
        assert sym in original_scores, (
            f"Unexpected symbol {sym} in result"
        )
        assert item["score"] == original_scores[sym], (
            f"Score changed for {sym}: original={original_scores[sym]}, "
            f"got={item['score']} when enable_sector_hmm is disabled "
            f"(variant={config_variant})"
        )

    # --- Assertion 3: Result is the same object (early return, no copy) ---
    # The implementation returns score_items directly when HMM is disabled,
    # so the returned list should be the exact same object.
    assert result is score_items, (
        f"Expected _apply_sector_hmm_adjustment to return the same list "
        f"object when HMM is disabled (variant={config_variant}), "
        f"but got a different object"
    )
