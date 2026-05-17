"""HMM Risk Gate helper for QE strategy integration.

Provides a drop-in function that QE strategies call instead of (or alongside)
_apply_hmm_adjustment() to filter blocked stocks from the candidate pool.

Usage in strategy code:
    from hmm_risk_gate_qe import apply_hmm_risk_gate

    if self.enable_hmm_risk_gate:
        pred_score, blocked = apply_hmm_risk_gate(
            pred_score, trade_date_str, self._risk_gate_config,
            current_holdings, protect_top=30
        )
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def load_risk_gate_artifact(path: str) -> dict[str, Any]:
    """Load and validate a risk gate artifact JSON file."""
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"HMM risk gate artifact not found: {path}")
    with open(p, "r", encoding="utf-8") as f:
        artifact = json.load(f)
    if artifact.get("artifact_type") != "hmm_risk_gate_v1":
        raise RuntimeError(
            f"Invalid artifact type: {artifact.get('artifact_type')}"
        )
    return artifact


def apply_hmm_risk_gate(
    pred_score: pd.Series,
    trade_date_str: str,
    risk_gate_artifact: dict[str, Any],
    current_holdings: set[str] | None = None,
    protect_top: int = 30,
) -> tuple[pd.Series, list[str]]:
    """Filter out stocks in blocked sectors (new buys only, with alpha protection).

    Args:
        pred_score: Series indexed by stock symbol with prediction scores
        trade_date_str: ISO date string (YYYY-MM-DD)
        risk_gate_artifact: Loaded risk gate artifact dict
        current_holdings: Set of currently held stock symbols (protected from gate)
        protect_top: Number of top-ranked stocks to protect from gate (default 30).
            These stocks are never blocked regardless of sector state.

    Returns:
        (filtered_pred_score, blocked_symbols)
        - filtered_pred_score: pred_score with blocked symbols removed
        - blocked_symbols: list of symbols that were blocked
    """
    daily_gates = risk_gate_artifact.get("daily_gates", {})
    stock_sector_map = risk_gate_artifact.get("stock_sector_map", {})

    gates_today = daily_gates.get(trade_date_str)
    if gates_today is None:
        return pred_score, []

    blocked_sectors = {
        sector_code
        for sector_code, gate in gates_today.items()
        if gate.get("blocked", False)
    }
    if not blocked_sectors:
        return pred_score, []

    holdings = current_holdings or set()

    sorted_scores = pred_score.sort_values(ascending=False)
    protected_symbols = set(sorted_scores.head(protect_top).index) if protect_top > 0 else set()

    blocked_symbols = []
    for symbol in sorted_scores.index:
        if symbol in holdings:
            continue
        if symbol in protected_symbols:
            continue
        sector = stock_sector_map.get(symbol)
        if sector and sector in blocked_sectors:
            blocked_symbols.append(symbol)

    if not blocked_symbols:
        return pred_score, []

    filtered = pred_score.drop(blocked_symbols, errors="ignore")
    return filtered, blocked_symbols
