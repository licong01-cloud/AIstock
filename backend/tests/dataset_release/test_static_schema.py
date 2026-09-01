from __future__ import annotations

from backend.data_service.moneyflow_contract import MONEYFLOW_FIELD_MAP
from backend.services.dataset_release.factor_materializer import _AUX_RENAMES
from backend.services.dataset_release.factor_materializer import FACTOR_SOURCE_SCHEMAS
from backend.qlib_exporter.field_map import _sector_data_source_to_export_map
from backend.services.dataset_release.static_schema import (
    STATIC_BAK_BASIC_COLUMNS,
    STATIC_CYQ_PERF_COLUMNS,
    STATIC_DAILY_BASIC_COLUMNS,
    STATIC_DEFAULT_NUMERIC_DTYPE,
    STATIC_COLUMN_DTYPES,
    STATIC_MARGIN_COLUMNS,
    STATIC_MONEYFLOW_DERIVED_COLUMNS,
    STATIC_MONEYFLOW_RAW_COLUMNS,
    STATIC_ORDERED_COLUMNS,
    STATIC_PRECOMPUTED_COLUMNS,
    STATIC_SCHEMA_VERSION,
    STATIC_SECTOR_COLUMNS,
    static_schema_digest,
)


def test_static_121_schema_is_exact_ordered_and_has_no_placeholders(
    dataset_profile,
) -> None:
    assert len(STATIC_ORDERED_COLUMNS) == 121
    assert len(set(STATIC_ORDERED_COLUMNS)) == 121
    assert not any(value.startswith(("unused_", "factor_")) for value in STATIC_ORDERED_COLUMNS)
    assert STATIC_ORDERED_COLUMNS == (
        *STATIC_DAILY_BASIC_COLUMNS,
        *STATIC_MONEYFLOW_RAW_COLUMNS,
        *STATIC_BAK_BASIC_COLUMNS,
        *STATIC_CYQ_PERF_COLUMNS,
        *STATIC_SECTOR_COLUMNS,
        *STATIC_MARGIN_COLUMNS,
        *STATIC_MONEYFLOW_DERIVED_COLUMNS,
        *STATIC_PRECOMPUTED_COLUMNS,
    )
    assert dataset_profile.static_schema_version == STATIC_SCHEMA_VERSION
    assert dataset_profile.static_schema_digest == static_schema_digest()
    assert dataset_profile.static_ordered_columns == STATIC_ORDERED_COLUMNS
    assert STATIC_DEFAULT_NUMERIC_DTYPE == "float32"
    assert set(STATIC_COLUMN_DTYPES.values()) == {"float32", "int16"}
    assert STATIC_COLUMN_DTYPES["l2_code_id"] == "int16"


def test_static_schema_matches_checked_in_source_and_formula_contracts() -> None:
    assert STATIC_DAILY_BASIC_COLUMNS == tuple(_AUX_RENAMES["daily_basic"].values())
    assert STATIC_MONEYFLOW_RAW_COLUMNS == tuple(MONEYFLOW_FIELD_MAP.values())
    assert STATIC_BAK_BASIC_COLUMNS == tuple(_AUX_RENAMES["bak_basic"].values())
    assert STATIC_CYQ_PERF_COLUMNS == tuple(_AUX_RENAMES["cyq_perf"].values())
    assert STATIC_MARGIN_COLUMNS == tuple(_AUX_RENAMES["margin_detail"].values())
    assert STATIC_SECTOR_COLUMNS[-1] == "l2_code_id"
    assert len(STATIC_SECTOR_COLUMNS) == 23
    assert FACTOR_SOURCE_SCHEMAS["sector_data"] == tuple(_sector_data_source_to_export_map())
    assert STATIC_MONEYFLOW_DERIVED_COLUMNS[:2] == (
        "mf_total_net_amt",
        "mf_total_net_vol",
    )
    assert STATIC_MONEYFLOW_DERIVED_COLUMNS[-6:] == (
        "mf_total_net_amt_20d",
        "mf_main_net_amt_20d",
        "mf_elg_net_amt_20d",
        "mf_total_net_amt_ratio_20d",
        "mf_main_net_amt_ratio_20d",
        "mf_elg_net_amt_ratio_20d",
    )
