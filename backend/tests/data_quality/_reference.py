"""Reference implementations of the synthesize / classify functions.

These are *pipeline-foundation owned* reference implementations of the
schema-synthesize and regime-classify logic that the dw-foundation
T14b handler (origin/claude/dw-foundation-20260510 commit bd098f8) and
the scripts/regime_label_daily.py script implement.

Owning the spec here lets the data-quality tests assert that archived
rows match the canonical behavior independently of any specific
handler implementation. When dw-foundation merges and both
implementations exist on main, the test compares archive rows against
this reference — any drift surfaces as a test failure.

If the dw-foundation handler module is later imported here directly,
that's a code-duplication clean-up and the tests should still pass
(behavior is the same). Until then, this file is the canonical source
of truth for the data-quality spec.

Cross-reference:
- backend/services/qe_archive/handlers/_synthesize.py (dw-foundation
  branch) - the runtime implementation.
- scripts/regime_label_daily.py (dw-foundation branch) - the regime
  classifier.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


CASH_LEDGER_ENTRY_TYPES = (
    "deposit", "withdraw", "fee", "fill_credit", "fill_debit",
    "dividend", "adjustment",
)
RESET_AUDIT_RESET_TYPES = (
    "full_reset", "partial_reset", "position_only", "cash_only", "config_only",
)
SESSION_DAY_QUALITY = ("ok", "low_coverage", "partial", "missing")
REGIME_VALUES = ("bull", "bear", "oscillation", "high_vol", "low_vol")


def _to_decimal(x: Any) -> Decimal | None:
    """Coerce ``x`` to Decimal or return None for known-coerceable failures.

    Matches the dw-foundation handler synthesize helper contract: archive
    rows can carry NaN-equivalent placeholders that must be treated as
    None at synthesize time rather than raising. Only ``InvalidOperation``
    (malformed numeric strings) and ``TypeError`` / ``ValueError`` (unsupported
    Python types) are absorbed; any other exception propagates.
    """
    if x is None:
        return None
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError, ValueError):
        return None


def synthesize_cash_ledger_entry_type(
    side: str | None,
    notional: Any,
    fee: Any,
    cash_delta: Any,
) -> str:
    """Derive paper_v2_cash_ledger.entry_type from source columns."""
    notional_d = _to_decimal(notional) or Decimal(0)
    fee_d = _to_decimal(fee) or Decimal(0)
    cash_delta_d = _to_decimal(cash_delta) or Decimal(0)

    if fee_d > 0 and notional_d == 0:
        return "fee"
    if side == "SELL" and cash_delta_d > 0:
        return "fill_credit"
    if side == "BUY" and cash_delta_d < 0:
        return "fill_debit"
    if side is None or side == "":
        if cash_delta_d > 0:
            return "deposit"
        if cash_delta_d < 0:
            return "withdraw"
    return "adjustment"


def synthesize_reset_audit_reset_type(
    rerun_policy: str | None,
    deleted_counts: Mapping[str, Any] | None,
) -> str:
    """Derive paper_v2_reset_audit.reset_type from source columns."""
    policy = (rerun_policy or "").lower()
    counts = deleted_counts or {}
    pos_n = int(counts.get("positions", 0) or 0)
    cash_n = int(counts.get("cash_ledger", counts.get("cash", 0)) or 0)
    fills_n = int(counts.get("fills", 0) or 0)
    orders_n = int(counts.get("orders", 0) or 0)

    if "full" in policy or "all" in policy:
        return "full_reset"
    has_pos = pos_n > 0
    has_cash = cash_n > 0 or fills_n > 0 or orders_n > 0
    if has_pos and has_cash:
        return "partial_reset"
    if has_pos and not has_cash:
        return "position_only"
    if has_cash and not has_pos:
        return "cash_only"
    if "config" in policy or (not has_pos and not has_cash):
        return "config_only"
    return "partial_reset"


def synthesize_session_day_data_quality(
    expected_bar_count: int | None,
    actual_bar_count: int | None,
) -> str:
    """Derive paper_v2_session_day.data_quality from bar counts.

    Per BUG-008 (kept open): ``actual_bar_count`` is itself derived at
    archive time and may under-report on sparse-capture days; this
    function is the *contract*, the under-reporting concern is upstream
    of the contract.
    """
    if actual_bar_count is None:
        return "missing"
    if expected_bar_count is None or expected_bar_count <= 0:
        return "ok" if actual_bar_count > 0 else "missing"
    ratio = actual_bar_count / expected_bar_count
    if ratio >= 1.0:
        return "ok"
    if ratio >= 0.5:
        return "partial"
    return "low_coverage"


def classify_simple_quadrant(
    ret_pct_5y: float | None,
    vol_pct_5y: float | None,
) -> tuple[str, float]:
    """Map (ret_pct, vol_pct) to one of 5 regime buckets + confidence."""
    if ret_pct_5y is None or vol_pct_5y is None:
        return ("oscillation", 0.0)
    if ret_pct_5y > 0.6 and vol_pct_5y < 0.4:
        regime = "bull"
    elif ret_pct_5y < 0.4 and vol_pct_5y > 0.6:
        regime = "bear"
    elif vol_pct_5y > 0.6:
        regime = "high_vol"
    elif vol_pct_5y < 0.4:
        regime = "low_vol"
    else:
        regime = "oscillation"
    centre_dist = ((ret_pct_5y - 0.5) ** 2 + (vol_pct_5y - 0.5) ** 2) ** 0.5
    confidence = min(centre_dist / 0.5, 1.0)
    return (regime, round(confidence, 3))


def compute_slippage_bps(
    intended_price: Any,
    fill_price: Any,
    side: str | None = None,  # accepted for ABI compatibility; ignored
) -> float | None:
    """Slippage in bps per D5 ``data_warehouse_extension_design_20260510.md`` §507.

    Canonical formula (no side branch):

        slippage_bps = (fill_price - intended_price) / intended_price * 10000

    Returns ``None`` when ``intended_price`` is NULL (MARKET orders have no
    reference price -- per D5 §502 ``intended_price`` is a first-class
    NULL signal, not missing data).

    ``side`` is accepted as a keyword for ABI stability with the earlier
    flipped-sign implementation but is INTENTIONALLY ignored: per Codex
    r2 review (drawer 46553d25), the D5 raw formula has no BUY/SELL
    branch. Sign interpretation (positive = adverse for the side) is the
    *consumer's* job downstream, not the storage contract.
    """
    if intended_price is None or fill_price is None:
        return None
    intended_d = _to_decimal(intended_price)
    fill_d = _to_decimal(fill_price)
    if intended_d is None or fill_d is None or intended_d == 0:
        return None
    delta = (fill_d - intended_d) / intended_d
    _ = side  # explicitly ignored — see docstring rationale
    return float(delta) * 10000.0


FILL_MARKET_CONTEXT_KEYS: dict[str, tuple[type, ...]] = {
    "stock_id": (str,),
    "trade_date": (str,),
    "data_source": (str,),
    "prev_close": (int, float),
    "limit_up": (int, float),
    "limit_down": (int, float),
    "suspend_status": (bool, int, str),  # source has been observed as bool / 0/1 / "Y"
    "full_day_open": (int, float),
    "full_day_close": (int, float),
    "full_day_volume": (int, float),
    "full_day_high": (int, float),
    "full_day_low": (int, float),
    "generated_at": (str,),
}
