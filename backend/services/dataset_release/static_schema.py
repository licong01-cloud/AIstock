"""Versioned ordered schema authority for QE ``static_factors.parquet``.

This list is derived from the checked-in QE loader/export contracts, not from
an existing candidate file.  Changing order, units, formulas, or names is a
semantic schema migration and requires a new version.
"""

from __future__ import annotations

from .canonical import digest_named_fields


STATIC_SCHEMA_VERSION = "qe_static_factors_121_v1"
STATIC_DEFAULT_NUMERIC_DTYPE = "float32"
STATIC_L2_CODE_ID_DTYPE = "int16"

STATIC_DAILY_BASIC_COLUMNS = (
    "db_close",
    "db_turnover_rate",
    "db_turnover_rate_f",
    "db_volume_ratio",
    "db_pe",
    "db_pe_ttm",
    "db_pb",
    "db_ps",
    "db_ps_ttm",
    "db_dv_ratio",
    "db_dv_ttm",
    "db_total_share",
    "db_float_share",
    "db_free_share",
    "db_total_mv",
    "db_circ_mv",
)

STATIC_MONEYFLOW_RAW_COLUMNS = (
    "mf_sm_buy_vol",
    "mf_sm_buy_amt",
    "mf_sm_sell_vol",
    "mf_sm_sell_amt",
    "mf_md_buy_vol",
    "mf_md_buy_amt",
    "mf_md_sell_vol",
    "mf_md_sell_amt",
    "mf_lg_buy_vol",
    "mf_lg_buy_amt",
    "mf_lg_sell_vol",
    "mf_lg_sell_amt",
    "mf_elg_buy_vol",
    "mf_elg_buy_amt",
    "mf_elg_sell_vol",
    "mf_elg_sell_amt",
    "mf_net_vol",
    "mf_net_amt",
)

STATIC_BAK_BASIC_COLUMNS = (
    "bb_pe_dyn",
    "bb_total_assets",
    "bb_liquid_assets",
    "bb_fixed_assets",
    "bb_reserved",
    "bb_reserved_pershare",
    "bb_eps",
    "bb_bvps",
    "bb_undp",
    "bb_per_undp",
    "bb_rev_yoy",
    "bb_profit_yoy",
    "bb_gpr",
    "bb_npr",
    "bb_holder_num",
)

STATIC_CYQ_PERF_COLUMNS = (
    "cp_his_low",
    "cp_his_high",
    "cp_cost_5pct",
    "cp_cost_15pct",
    "cp_cost_50pct",
    "cp_cost_85pct",
    "cp_cost_95pct",
    "cp_weight_avg",
    "cp_winner_rate",
)

STATIC_SECTOR_COLUMNS = (
    "sw2_open",
    "sw2_high",
    "sw2_low",
    "sw2_close",
    "sw2_pct_change",
    "sw2_vol",
    "sw2_amount",
    "sw2_pe",
    "sw2_pb",
    "sw2_total_mv",
    "sw2_mf_buy_sm_amt",
    "sw2_mf_sell_sm_amt",
    "sw2_mf_buy_md_amt",
    "sw2_mf_sell_md_amt",
    "sw2_mf_buy_lg_amt",
    "sw2_mf_sell_lg_amt",
    "sw2_mf_buy_elg_amt",
    "sw2_mf_sell_elg_amt",
    "sw2_mf_net_amt",
    "sw2_mf_buy_elg_vol",
    "sw2_mf_sell_elg_vol",
    "sw2_mf_net_vol",
    "l2_code_id",
)

STATIC_MARGIN_COLUMNS = (
    "md_rzye",
    "md_rqye",
    "md_rzmre",
    "md_rqyl",
    "md_rzche",
    "md_rqchl",
    "md_rqmcl",
    "md_rzrqye",
)

STATIC_MONEYFLOW_DERIVED_COLUMNS = (
    "mf_total_net_amt",
    "mf_total_net_vol",
    "mf_total_net_amt_ratio",
    "mf_total_net_vol_ratio",
    "mf_main_net_amt",
    "mf_main_net_vol",
    "mf_main_net_amt_ratio",
    "mf_main_net_vol_ratio",
    "mf_elg_net_amt",
    "mf_elg_net_vol",
    "mf_elg_net_amt_ratio",
    "mf_elg_net_vol_ratio",
    "mf_elg_share_in_main_amt",
    "mf_elg_share_in_main_vol",
    "mf_total_net_amt_5d",
    "mf_main_net_amt_5d",
    "mf_elg_net_amt_5d",
    "mf_total_net_amt_ratio_5d",
    "mf_main_net_amt_ratio_5d",
    "mf_elg_net_amt_ratio_5d",
    "mf_total_net_amt_20d",
    "mf_main_net_amt_20d",
    "mf_elg_net_amt_20d",
    "mf_total_net_amt_ratio_20d",
    "mf_main_net_amt_ratio_20d",
    "mf_elg_net_amt_ratio_20d",
)

STATIC_PRECOMPUTED_COLUMNS = (
    "value_pe_inv",
    "value_pb_inv",
    "size_log_mv",
    "liquidity_turnover",
    "liquidity_vol_ratio",
    "PriceStrength_10D",
)

STATIC_ORDERED_COLUMNS = (
    *STATIC_DAILY_BASIC_COLUMNS,
    *STATIC_MONEYFLOW_RAW_COLUMNS,
    *STATIC_BAK_BASIC_COLUMNS,
    *STATIC_CYQ_PERF_COLUMNS,
    *STATIC_SECTOR_COLUMNS,
    *STATIC_MARGIN_COLUMNS,
    *STATIC_MONEYFLOW_DERIVED_COLUMNS,
    *STATIC_PRECOMPUTED_COLUMNS,
)

STATIC_COLUMN_DTYPES = {
    column: (STATIC_L2_CODE_ID_DTYPE if column == "l2_code_id" else STATIC_DEFAULT_NUMERIC_DTYPE)
    for column in STATIC_ORDERED_COLUMNS
}

if len(STATIC_ORDERED_COLUMNS) != 121 or len(set(STATIC_ORDERED_COLUMNS)) != 121:
    raise RuntimeError("QE static ordered schema must contain 121 unique columns")


def static_schema_digest() -> str:
    return digest_named_fields(
        "dataset_release_static_schema_v1",
        {
            "schema_version": STATIC_SCHEMA_VERSION,
            "ordered_columns": list(STATIC_ORDERED_COLUMNS),
            "default_numeric_dtype": STATIC_DEFAULT_NUMERIC_DTYPE,
            "column_dtypes": [[column, STATIC_COLUMN_DTYPES[column]] for column in STATIC_ORDERED_COLUMNS],
            "l2_code_id_dtype": STATIC_L2_CODE_ID_DTYPE,
            "l2_code_id_missing": -1,
        },
    )


__all__ = [
    "STATIC_BAK_BASIC_COLUMNS",
    "STATIC_CYQ_PERF_COLUMNS",
    "STATIC_COLUMN_DTYPES",
    "STATIC_DAILY_BASIC_COLUMNS",
    "STATIC_DEFAULT_NUMERIC_DTYPE",
    "STATIC_MARGIN_COLUMNS",
    "STATIC_MONEYFLOW_DERIVED_COLUMNS",
    "STATIC_MONEYFLOW_RAW_COLUMNS",
    "STATIC_ORDERED_COLUMNS",
    "STATIC_PRECOMPUTED_COLUMNS",
    "STATIC_SCHEMA_VERSION",
    "STATIC_SECTOR_COLUMNS",
    "STATIC_L2_CODE_ID_DTYPE",
    "static_schema_digest",
]
