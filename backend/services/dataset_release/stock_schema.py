"""Frozen QE Qlib stock feature schema and value semantics.

Daily and one-minute stock bins intentionally expose the same twelve fields.
The schema is shared by producers, validators and the real-Qlib consumer smoke
so a local writer cannot silently reduce the consumer contract.
"""

from __future__ import annotations

from typing import Any

from .canonical import digest_named_fields


QLIB_STOCK_SCHEMA_VERSION = "qe_qlib_stock_12_v1"
QLIB_STOCK_FIELDS: tuple[str, ...] = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "factor",
    "up_limit_price",
    "down_limit_price",
    "prev_close",
    "limit_up",
    "limit_down",
)
QLIB_STOCK_DTYPES = {field: "float32" for field in QLIB_STOCK_FIELDS}
QLIB_STOCK_VALUE_CONTRACT: dict[str, Any] = {
    "price_unit_divisor": 1000.0,
    "qfq_denominator": "per_instrument_max_adj_factor_through_cutoff_v1",
    "qfq_numerator": "latest_adj_factor_at_or_before_row_v1",
    "adjusted_ohlc": "raw_price_li/1000*qfq_factor",
    "adjusted_volume": "raw_volume_hand*100/qfq_factor",
    "amount": "raw_amount_li/1000_cny_unadjusted",
    "limit_prices_and_prev_close": "raw_cny_unadjusted",
    "limit_flags": "raw_close_close_only_abs_tol_1e-4_v1",
    "minute_session": "240_rows_0931_1130_1301_1500_asia_shanghai_v1",
    "full_day_suspend": "suspend_type_S_and_suspend_timing_null_v1",
    "full_day_suspend_fill": "prev_close_qfq_zero_volume_amount_240_v1",
    "partial_or_unexplained_minute_gap": "fail_closed_v1",
}


def qlib_stock_schema_payload() -> dict[str, Any]:
    return {
        "schema_version": QLIB_STOCK_SCHEMA_VERSION,
        "daily_fields": list(QLIB_STOCK_FIELDS),
        "minute_fields": list(QLIB_STOCK_FIELDS),
        "dtypes": dict(QLIB_STOCK_DTYPES),
        "value_contract": dict(QLIB_STOCK_VALUE_CONTRACT),
    }


def qlib_stock_schema_digest() -> str:
    return digest_named_fields(
        "dataset_release_qlib_stock_schema_v1",
        qlib_stock_schema_payload(),
    )


__all__ = [
    "QLIB_STOCK_DTYPES",
    "QLIB_STOCK_FIELDS",
    "QLIB_STOCK_SCHEMA_VERSION",
    "QLIB_STOCK_VALUE_CONTRACT",
    "qlib_stock_schema_digest",
    "qlib_stock_schema_payload",
]
