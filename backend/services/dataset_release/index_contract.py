from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Iterable, Mapping, Sequence

from .canonical import digest_named_fields
from .errors import IndexContractError, IndexOverlapConflict
from .stock_schema import QLIB_STOCK_FIELDS


INDEX_UNIVERSE_VERSION = "qe_hmm_domestic_core_v1"
INDEX_SCHEMA_VERSION = "qe_index_context_v1"
HMM_BENCHMARK_CODE = "000300.SH"
INDEX_QLIB_FIELDS: tuple[str, ...] = QLIB_STOCK_FIELDS
INDEX_QLIB_VALUE_CONTRACT: Mapping[str, str] = {
    "factor": "constant_1_unadjusted_index_points_v1",
    "prev_close": "tushare_index_daily_pre_close_points_v1",
    "up_limit_price": "pre_close_neutral_no_index_limit_v1",
    "down_limit_price": "pre_close_neutral_no_index_limit_v1",
    "limit_up": "constant_0_no_index_limit_v1",
    "limit_down": "constant_0_no_index_limit_v1",
}


@dataclass(frozen=True, order=True)
class IndexDefinition:
    daily_code: str
    semantic_role: str
    required_from: date
    hmm_benchmark: bool = False
    weight_api_code: str | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["required_from"] = self.required_from.isoformat()
        return payload


DOMESTIC_INDEX_DEFINITIONS: tuple[IndexDefinition, ...] = (
    IndexDefinition("000001.SH", "shanghai_composite", date(2018, 8, 1)),
    IndexDefinition("000016.SH", "super_large_cap", date(2018, 8, 1)),
    IndexDefinition(
        "000300.SH",
        "hmm_benchmark_large_cap",
        date(2018, 8, 1),
        hmm_benchmark=True,
        weight_api_code="399300.SZ",
    ),
    IndexDefinition("000688.SH", "star_50", date(2020, 1, 2)),
    IndexDefinition("000852.SH", "small_cap_1000", date(2018, 8, 1)),
    IndexDefinition("000905.SH", "mid_cap_500", date(2018, 8, 1)),
    IndexDefinition("000985.CSI", "all_a_proxy", date(2018, 8, 1)),
    IndexDefinition("932000.CSI", "micro_cap_2000", date(2018, 8, 1)),
    IndexDefinition("399001.SZ", "shenzhen_component", date(2018, 8, 1)),
    IndexDefinition("399006.SZ", "chinext_component", date(2018, 8, 1)),
    IndexDefinition("399102.SZ", "chinext_composite", date(2018, 8, 1)),
    IndexDefinition("399107.SZ", "shenzhen_a_composite", date(2018, 8, 1)),
)

INDEX_H5_COLUMNS: tuple[str, ...] = (
    "idx_open_point",
    "idx_high_point",
    "idx_low_point",
    "idx_close_point",
    "idx_pre_close_point",
    "idx_return_1d",
    "idx_volume_hand_source",
    "idx_volume_share_equiv",
    "idx_amount_cny",
)
INDEX_H5_DTYPES: Mapping[str, str] = {field: "float32" for field in INDEX_H5_COLUMNS}

INDEX_UNIT_CONTRACT = {
    "point_fields": [
        "idx_open_point",
        "idx_high_point",
        "idx_low_point",
        "idx_close_point",
        "idx_pre_close_point",
    ],
    "idx_return_1d": "pct_chg/100",
    "idx_volume_hand_source": "tushare_vol_hand",
    "idx_volume_share_equiv": "tushare_vol*100",
    "idx_amount_cny": "tushare_amount_thousand_cny*1000",
}

INDEX_SOURCE_VALUE_FIELDS: tuple[str, ...] = (
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "pct_chg",
    "vol",
    "amount",
)

INDEX_OVERLAP_COMPARISON_CONTRACT: Mapping[str, Any] = {
    "contract_id": "index_source_unit_equivalence_v1",
    "point_and_return_fields": {
        "fields": ("open", "high", "low", "close", "pre_close", "pct_chg"),
        "comparison": "absolute_tolerance",
        "default_abs_tolerance": 1e-8,
    },
    "vol": {
        "source_unit": "hand",
        "comparison": "database_precision_guarded_provider_quantize",
        "quantum": "1",
        "rounding": "ROUND_DOWN",
    },
    "amount": {
        "source_unit": "thousand_cny",
        "comparison": "database_precision_guarded_provider_quantize",
        "quantum": "0.1",
        "rounding": "ROUND_HALF_UP",
    },
}

_INDEX_OVERLAP_DECIMAL_RULES = {
    "vol": (Decimal("1"), ROUND_DOWN),
    "amount": (Decimal("0.1"), ROUND_HALF_UP),
}


def parse_index_definitions(rows: Sequence[Mapping[str, Any]]) -> tuple[IndexDefinition, ...]:
    output: list[IndexDefinition] = []
    for position, row in enumerate(rows):
        try:
            output.append(
                IndexDefinition(
                    daily_code=str(row["daily_code"]).strip().upper(),
                    semantic_role=str(row["semantic_role"]).strip(),
                    required_from=date.fromisoformat(str(row["required_from"])),
                    hmm_benchmark=bool(row.get("hmm_benchmark", False)),
                    weight_api_code=(
                        str(row["weight_api_code"]).strip().upper() if row.get("weight_api_code") is not None else None
                    ),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise IndexContractError(f"invalid index definition at position {position}") from exc
    return tuple(output)


def validate_index_definitions(
    definitions: Sequence[IndexDefinition],
) -> tuple[IndexDefinition, ...]:
    actual = tuple(definitions)
    if actual != DOMESTIC_INDEX_DEFINITIONS:
        raise IndexContractError(
            "index definitions must exactly match qe_hmm_domestic_core_v1",
            context={
                "expected": [item.as_dict() for item in DOMESTIC_INDEX_DEFINITIONS],
                "actual": [item.as_dict() for item in actual],
            },
        )
    benchmark = [item.daily_code for item in actual if item.hmm_benchmark]
    if benchmark != [HMM_BENCHMARK_CODE]:
        raise IndexContractError("HMM benchmark must remain exactly 000300.SH")
    return actual


def index_contract_payload() -> dict[str, Any]:
    return {
        "schema_version": INDEX_SCHEMA_VERSION,
        "universe_version": INDEX_UNIVERSE_VERSION,
        "benchmark_code": HMM_BENCHMARK_CODE,
        "codes": [item.as_dict() for item in DOMESTIC_INDEX_DEFINITIONS],
        "h5_columns": list(INDEX_H5_COLUMNS),
        "h5_dtypes": dict(INDEX_H5_DTYPES),
        "qlib_fields": list(INDEX_QLIB_FIELDS),
        "qlib_value_contract": dict(INDEX_QLIB_VALUE_CONTRACT),
        "units": INDEX_UNIT_CONTRACT,
        "index_weight_consumed": False,
    }


def index_contract_digest() -> str:
    return digest_named_fields("dataset_release_index_contract_v1", index_contract_payload())


def _decimal_source_value(value: float) -> Decimal:
    return Decimal(str(value))


def _overlap_values_equivalent(
    field: str,
    left: float,
    right: float,
    *,
    point_abs_tolerance: float,
) -> bool:
    decimal_rule = _INDEX_OVERLAP_DECIMAL_RULES.get(field)
    if decimal_rule is None:
        return math.isclose(left, right, rel_tol=0.0, abs_tol=point_abs_tolerance)
    quantum, rounding = decimal_rule
    database_value = _decimal_source_value(left)
    canonical_database_value = database_value.quantize(quantum, rounding=rounding)
    if database_value != canonical_database_value:
        return math.isclose(left, right, rel_tol=0.0, abs_tol=point_abs_tolerance)
    return canonical_database_value == _decimal_source_value(right).quantize(quantum, rounding=rounding)


def _normalize_index_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    source: str,
) -> dict[tuple[str, date], dict[str, Any]]:
    output: dict[tuple[str, date], dict[str, Any]] = {}
    for ordinal, raw in enumerate(rows):
        try:
            code = str(raw["ts_code"]).strip().upper()
            trade_date = raw["trade_date"]
            if not isinstance(trade_date, date):
                trade_date = date.fromisoformat(str(trade_date))
            row = {"ts_code": code, "trade_date": trade_date}
            for field in INDEX_SOURCE_VALUE_FIELDS:
                value = float(raw[field])
                if not math.isfinite(value):
                    raise ValueError(f"non-finite {field}")
                row[field] = value
        except (KeyError, TypeError, ValueError) as exc:
            raise IndexContractError(f"invalid {source} index row at ordinal {ordinal}: {exc}") from exc
        key = (code, trade_date)
        if code not in {item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS}:
            raise IndexContractError(f"index code is outside qe_hmm_domestic_core_v1: {code}")
        if key in output:
            raise IndexContractError(f"duplicate {source} index key: {key}")
        output[key] = row
    return output


def merge_index_rows_missing_only(
    database_rows: Iterable[Mapping[str, Any]],
    provider_rows: Iterable[Mapping[str, Any]],
    *,
    abs_tolerance: float = 1e-8,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Use provider rows only for missing keys; conflicting overlaps are terminal."""

    database = _normalize_index_rows(database_rows, source="database")
    provider = _normalize_index_rows(provider_rows, source="provider")
    point_abs_tolerance = float(abs_tolerance)
    if not math.isfinite(point_abs_tolerance) or point_abs_tolerance < 0.0:
        raise IndexContractError("index overlap point tolerance must be finite and non-negative")
    overlap = sorted(set(database).intersection(provider))
    conflicts: list[dict[str, Any]] = []
    max_abs_delta_by_field = {field: Decimal("0") for field in INDEX_SOURCE_VALUE_FIELDS}
    for key in overlap:
        for field in INDEX_SOURCE_VALUE_FIELDS:
            left = float(database[key][field])
            right = float(provider[key][field])
            decimal_delta = abs(_decimal_source_value(left) - _decimal_source_value(right))
            max_abs_delta_by_field[field] = max(max_abs_delta_by_field[field], decimal_delta)
            if not _overlap_values_equivalent(
                field,
                left,
                right,
                point_abs_tolerance=point_abs_tolerance,
            ):
                conflicts.append(
                    {
                        "ts_code": key[0],
                        "trade_date": key[1].isoformat(),
                        "field": field,
                        "database": left,
                        "provider": right,
                    }
                )
    if conflicts:
        raise IndexOverlapConflict(
            "provider/database index overlap differs",
            context={
                "comparison_contract": INDEX_OVERLAP_COMPARISON_CONTRACT["contract_id"],
                "point_abs_tolerance": point_abs_tolerance,
                "conflict_count": len(conflicts),
                "samples": conflicts[:20],
            },
        )
    provider_only = sorted(set(provider).difference(database))
    merged = dict(database)
    for key in provider_only:
        merged[key] = provider[key]
    return [merged[key] for key in sorted(merged)], {
        "database_rows": len(database),
        "provider_rows": len(provider),
        "overlap_rows_verified": len(overlap),
        "provider_fill_rows": len(provider_only),
        "overlap_mismatch_cells": 0,
        "overlap_comparison_contract": INDEX_OVERLAP_COMPARISON_CONTRACT["contract_id"],
        "overlap_point_abs_tolerance": point_abs_tolerance,
        "overlap_max_abs_delta_by_field": {
            field: str(max_abs_delta_by_field[field])
            for field in INDEX_SOURCE_VALUE_FIELDS
            if max_abs_delta_by_field[field] > 0
        },
        "source_precedence": "database_then_provider_missing_keys_conflict_fail_v1",
    }
