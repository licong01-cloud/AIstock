from __future__ import annotations

from datetime import date

import pytest

from backend.services.dataset_release.errors import IndexContractError, IndexOverlapConflict
from backend.services.dataset_release.index_contract import (
    DOMESTIC_INDEX_DEFINITIONS,
    HMM_BENCHMARK_CODE,
    INDEX_H5_COLUMNS,
    INDEX_UNIT_CONTRACT,
    index_contract_payload,
    merge_index_rows_missing_only,
    validate_index_definitions,
)


def _row(trade_date: str, close: float, code: str = "000300.SH") -> dict[str, object]:
    return {
        "ts_code": code,
        "trade_date": trade_date,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "pre_close": close,
        "pct_chg": 0.0,
        "vol": 10.0,
        "amount": 20.0,
    }


def test_exact_12_index_contract_and_hmm_benchmark_are_frozen() -> None:
    assert validate_index_definitions(DOMESTIC_INDEX_DEFINITIONS) == DOMESTIC_INDEX_DEFINITIONS
    assert len(DOMESTIC_INDEX_DEFINITIONS) == 12
    assert [item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS if item.hmm_benchmark] == [HMM_BENCHMARK_CODE]
    assert DOMESTIC_INDEX_DEFINITIONS[3].required_from == date(2020, 1, 2)
    assert index_contract_payload()["index_weight_consumed"] is False
    assert len(INDEX_H5_COLUMNS) == 9
    assert INDEX_UNIT_CONTRACT["idx_volume_share_equiv"] == "tushare_vol*100"


def test_runtime_popularity_expansion_is_rejected() -> None:
    expanded = DOMESTIC_INDEX_DEFINITIONS + (DOMESTIC_INDEX_DEFINITIONS[0],)
    with pytest.raises(IndexContractError, match="exactly match"):
        validate_index_definitions(expanded)


def test_provider_fills_only_missing_index_keys() -> None:
    database = [_row("2026-07-30", 10.0), _row("2026-07-31", 11.0)]
    provider = [_row("2026-07-29", 9.0), _row("2026-07-30", 10.0)]
    merged, stats = merge_index_rows_missing_only(database, provider)
    values = {row["trade_date"]: row["close"] for row in merged}
    assert values[date(2026, 7, 29)] == 9.0
    assert values[date(2026, 7, 30)] == 10.0
    assert stats["provider_fill_rows"] == 1
    assert stats["overlap_rows_verified"] == 1


def test_provider_database_overlap_conflict_fails_closed() -> None:
    with pytest.raises(IndexOverlapConflict) as captured:
        merge_index_rows_missing_only(
            [_row("2026-07-30", 10.0)],
            [_row("2026-07-30", 10.5)],
        )
    assert captured.value.code == "DATASET_RELEASE_INDEX_PROVIDER_CONFLICT"
    assert captured.value.context["samples"][0]["field"] in {
        "open",
        "high",
        "low",
        "close",
        "pre_close",
    }


def test_provider_database_overlap_accepts_canonical_historical_unit_precision() -> None:
    database = [_row("2025-11-03", 3915.6648, code="399102.SZ")]
    database[0]["vol"] = 208_981_990.0
    database[0]["amount"] = 535_801_515.3
    provider = [_row("2025-11-03", 3915.6648, code="399102.SZ")]
    provider[0]["vol"] = 208_981_990.89
    provider[0]["amount"] = 535_801_515.31351

    merged, stats = merge_index_rows_missing_only(database, provider)

    assert merged == [
        {
            **database[0],
            "trade_date": date(2025, 11, 3),
        }
    ]
    assert stats["overlap_rows_verified"] == 1
    assert stats["overlap_comparison_contract"] == "index_source_unit_equivalence_v1"
    assert stats["overlap_max_abs_delta_by_field"] == {
        "amount": "0.01351",
        "vol": "0.89",
    }


@pytest.mark.parametrize(
    ("field", "database_value", "provider_value"),
    [
        ("vol", 208_981_990.0, 208_981_991.0),
        ("amount", 535_801_515.3, 535_801_515.36),
    ],
)
def test_provider_database_overlap_material_unit_difference_fails_closed(
    field: str,
    database_value: float,
    provider_value: float,
) -> None:
    database = [_row("2025-11-03", 3915.6648, code="399102.SZ")]
    provider = [_row("2025-11-03", 3915.6648, code="399102.SZ")]
    database[0][field] = database_value
    provider[0][field] = provider_value

    with pytest.raises(IndexOverlapConflict) as captured:
        merge_index_rows_missing_only(database, provider)

    assert captured.value.context["conflict_count"] == 1
    assert captured.value.context["comparison_contract"] == "index_source_unit_equivalence_v1"
    assert captured.value.context["samples"][0]["field"] == field


@pytest.mark.parametrize(
    ("field", "database_value", "provider_value"),
    [
        ("vol", 208_981_990.89, 208_981_990.01),
        ("amount", 535_801_515.31351, 535_801_515.34999),
    ],
)
def test_provider_database_overlap_does_not_quantize_precise_database_values(
    field: str,
    database_value: float,
    provider_value: float,
) -> None:
    database = [_row("2025-11-03", 3915.6648, code="399102.SZ")]
    provider = [_row("2025-11-03", 3915.6648, code="399102.SZ")]
    database[0][field] = database_value
    provider[0][field] = provider_value

    with pytest.raises(IndexOverlapConflict) as captured:
        merge_index_rows_missing_only(database, provider)

    assert captured.value.context["samples"][0]["field"] == field


@pytest.mark.parametrize("abs_tolerance", [-1.0, float("inf"), float("nan")])
def test_provider_database_overlap_rejects_invalid_point_tolerance(abs_tolerance: float) -> None:
    with pytest.raises(IndexContractError, match="finite and non-negative"):
        merge_index_rows_missing_only([], [], abs_tolerance=abs_tolerance)
