"""Schema synthesize helpers for fields the source paper_v2.* lacks.

Per Codex T12+T14a fix round 2 (P1.4) decisions and BUG-006..008 follow-ups:

- paper_v2_cash_ledger.entry_type: source has no entry_type column.
  Derived from (side, notional, fee, cash_delta) at archive time.

- paper_v2_reset_audit.reset_type: source has no reset_type column.
  Derived from (rerun_policy, deleted_counts) at archive time.

- paper_v2_session_day.data_quality: source has no data_quality column.
  Derived from (expected_bar_count, actual_bar_count) at archive time. Source
  also has no actual_bar_count — derived from latest_available_bar_time vs
  expected windows. For now we use expected_bar_count vs a heuristic
  count_or_none placeholder; downstream rework via BUG-008.

These helpers are pure functions (no DB access), so unit-testable in isolation.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

# enum value catalogs (kept here so tests + handlers reference one source)
CASH_LEDGER_ENTRY_TYPES = (
    "deposit", "withdraw", "fee", "fill_credit", "fill_debit",
    "dividend", "adjustment",
)
RESET_AUDIT_RESET_TYPES = (
    "full_reset", "partial_reset", "position_only", "cash_only", "config_only",
)
SESSION_DAY_QUALITY = ("ok", "low_coverage", "partial", "missing")


def _to_decimal(x: Any) -> Decimal | None:
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
    """Derive paper_v2_cash_ledger.entry_type from the source columns.

    Rules (matching source semantics observed in Batch A real data):
      - fee row:      notional == 0 (or absent) AND fee > 0   -> 'fee'
      - fill_credit:  side == 'SELL' AND cash_delta > 0       -> 'fill_credit'
      - fill_debit:   side == 'BUY'  AND cash_delta < 0       -> 'fill_debit'
      - deposit:      side is NULL AND cash_delta > 0          -> 'deposit'
      - withdraw:     side is NULL AND cash_delta < 0          -> 'withdraw'
      - fallback:     'adjustment' (catch-all)

    Source paper_v2.cash_ledger has no dividend payments today; if a future
    source row carries side='DIVIDEND' or similar, downstream caller should
    extend this synth function alongside an enum widening.
    """
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
    """Derive paper_v2_reset_audit.reset_type from source columns.

    Rules (deterministic — same input always yields same output):
      - rerun_policy carries the operator's intent at reset time; if it
        contains 'full' or 'all' -> 'full_reset'
      - if deleted_counts has both positions and cash entries -> 'partial_reset'
      - if only positions deleted -> 'position_only'
      - if only cash entries deleted -> 'cash_only'
      - if rerun_policy mentions 'config' or no row counts deleted -> 'config_only'
      - fallback: 'partial_reset'
    """
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

    Source paper_v2.session_day has expected_bar_count but NO actual_bar_count.
    Caller must derive actual_bar_count separately (typically from a count of
    intraday_snapshots or similar). If actual_bar_count is None we return
    'missing' so downstream alerts can flag it.

    Rules:
      - actual is None  -> 'missing'
      - actual >= expected -> 'ok'
      - actual >= expected * 0.5 -> 'partial'
      - else -> 'low_coverage'
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


def normalize_status(value: str | None, allowed: tuple[str, ...]) -> str | None:
    """Pass-through validator: returns value unchanged if in allowed set,
    raises ValueError otherwise. Used at the archive write boundary so any
    source enum drift surfaces immediately rather than silent CHECK failure
    at INSERT time.
    """
    if value is None:
        return None
    if value in allowed:
        return value
    raise ValueError(
        f"status {value!r} is not in allowed set {allowed!r}"
    )


def derive_error_class(error_code: str | None, message: str | None) -> str:
    """Guess error_class from source paper_v2.errors.error_code (which Batch A
    shows has free-form values like 'BROKER_REJECTED', 'PACKAGE_INVALID').

    Rules:
      - starts with BROKER -> BrokerBackendError
      - starts with PACKAGE -> StrategyPackageError
      - starts with VALIDATION -> ValidationError
      - else -> GenericError
    """
    code = (error_code or "").upper()
    if code.startswith("BROKER"):
        return "BrokerBackendError"
    if code.startswith("PACKAGE") or code.startswith("STRATEGY"):
        return "StrategyPackageError"
    if code.startswith("VALIDATION") or code.startswith("VALIDATE"):
        return "ValidationError"
    return "GenericError"
