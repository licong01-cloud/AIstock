"""Tushare dataset specifications — configuration-driven metadata for each dataset.

Each DatasetSpec describes how to fetch from Tushare and upsert into PostgreSQL.
The TushareSyncEngine consumes these specs to replace per-dataset scripts.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class QueryMode(str, Enum):
    """How the engine iterates over the Tushare API."""
    BY_DATE = "by_date"            # one API call per trade_date
    BY_CODE = "by_code"            # one API call per ts_code
    SINGLE_CALL = "single_call"    # one API call fetches everything


@dataclass(frozen=True)
class DatasetSpec:
    name: str                              # logical name, e.g. "daily_basic"
    tushare_api: str                       # pro.daily_basic(...)
    target_table: str                      # "market.daily_basic"
    primary_keys: List[str]                # ["ts_code", "trade_date"]
    query_mode: QueryMode
    columns: Dict[str, str]                # {col_name: pg_type}
    api_params: Dict[str, str] = field(default_factory=dict)
    single_call_param_sets: List[Dict[str, str]] = field(default_factory=list)
    api_field_map: Dict[str, str] = field(default_factory=dict)  # {db_col: tushare_field} when names differ
    batch_sleep: float = 0.2               # seconds between batches
    supports_incremental: bool = True
    date_column: str = "trade_date"        # cursor column for incremental
    code_source_sql: str = ""              # SQL to fetch ts_code list for BY_CODE mode
    row_limit: int = 0                     # if >0, fail when single API call returns >= this many rows
    code_param_name: str = "ts_code"       # BY_CODE: API parameter name for the code value
    skip_date_params: bool = False         # True = don't pass start_date/end_date to API
    replace_existing_dates: bool = False   # True = replace one date window instead of append-only upsert
    rate_per_minute: int = 500             # per-API 频率限制，默认 500/min (10000积分)
    date_param_name: str = "trade_date"    # BY_DATE: API date parameter name
    incremental_cursor_from_audit: bool = False  # sparse BY_DATE datasets can advance through successful empty-date refreshes


# ---------------------------------------------------------------------------
# 6 dataset definitions
# ---------------------------------------------------------------------------

DAILY_BASIC = DatasetSpec(
    name="daily_basic",
    tushare_api="daily_basic",
    target_table="market.daily_basic",
    primary_keys=["trade_date", "ts_code"],
    query_mode=QueryMode.BY_DATE,
    columns={
        "trade_date": "date",
        "ts_code": "text",
        "close": "numeric",
        "turnover_rate": "numeric",
        "turnover_rate_f": "numeric",
        "volume_ratio": "numeric",
        "pe": "numeric",
        "pe_ttm": "numeric",
        "pb": "numeric",
        "ps": "numeric",
        "ps_ttm": "numeric",
        "dv_ratio": "numeric",
        "dv_ttm": "numeric",
        "total_share": "numeric",
        "float_share": "numeric",
        "free_share": "numeric",
        "total_mv": "numeric",
        "circ_mv": "numeric",
    },
    batch_sleep=0.12,
    rate_per_minute=500,
)

ADJ_FACTOR = DatasetSpec(
    name="adj_factor",
    tushare_api="adj_factor",
    target_table="market.adj_factor",
    primary_keys=["trade_date", "ts_code"],
    query_mode=QueryMode.BY_DATE,
    columns={
        "trade_date": "date",
        "ts_code": "text",
        "adj_factor": "numeric",
    },
    batch_sleep=0.12,
    rate_per_minute=500,
)

INDEX_DAILY = DatasetSpec(
    name="index_daily",
    tushare_api="index_daily",
    target_table="market.index_daily",
    primary_keys=["ts_code", "trade_date"],
    query_mode=QueryMode.BY_CODE,
    columns={
        "ts_code": "text",
        "trade_date": "date",
        "close": "numeric",
        "open": "numeric",
        "high": "numeric",
        "low": "numeric",
        "pre_close": "numeric",
        "change": "numeric",
        "pct_chg": "numeric",
        "vol": "numeric",
        "amount": "numeric",
    },
    code_source_sql=(
        "SELECT ts_code FROM market.index_basic "
        "WHERE market IN ('SSE', 'SZSE', 'CSI') "
        "AND (exp_date IS NULL OR exp_date::date >= CURRENT_DATE) "
        "ORDER BY ts_code"
    ),
    row_limit=8000,
    batch_sleep=0.12,
    rate_per_minute=500,
)

STOCK_BASIC = DatasetSpec(
    name="stock_basic",
    tushare_api="stock_basic",
    target_table="market.stock_basic",
    primary_keys=["ts_code"],
    query_mode=QueryMode.SINGLE_CALL,
    columns={
        "ts_code": "text",
        "symbol": "text",
        "name": "text",
        "area": "text",
        "industry": "text",
        "fullname": "text",
        "enname": "text",
        "market": "text",
        "exchange": "text",
        "curr_type": "text",
        "list_status": "text",
        "list_date": "date",
        "delist_date": "date",
        "is_hs": "text",
    },
    single_call_param_sets=[
        {"exchange": "", "list_status": "L"},
        {"exchange": "", "list_status": "D"},
        {"exchange": "", "list_status": "P"},
    ],
    supports_incremental=False,
    date_column="list_date",
)

STOCK_ST = DatasetSpec(
    name="stock_st",
    tushare_api="stock_st",
    target_table="market.stock_st",
    primary_keys=["ts_code", "ann_date"],
    query_mode=QueryMode.BY_DATE,
    columns={
        "ann_date": "date",
        "ts_code": "text",
        "start_date": "date",
        "end_date": "date",
        "market": "text",
        "exchange": "text",
    },
    api_field_map={"ann_date": "trade_date"},  # Tushare returns "trade_date", DB column is "ann_date"
    date_column="ann_date",
    batch_sleep=0.3,
    rate_per_minute=200,
)

STOCK_ST_EVENTS = DatasetSpec(
    name="stock_st_events",
    tushare_api="st",
    target_table="market.stock_st_events",
    primary_keys=["ts_code", "pub_date", "imp_date", "st_type"],
    query_mode=QueryMode.BY_DATE,
    columns={
        "ts_code": "text",
        "name": "text",
        "pub_date": "date",
        "imp_date": "date",
        "st_type": "text",
        "st_reason": "text",
        "st_explain": "text",
    },
    api_field_map={"st_type": "st_tpye"},
    date_column="pub_date",
    date_param_name="pub_date",
    batch_sleep=0.3,
    rate_per_minute=200,
    row_limit=1000,
    replace_existing_dates=True,
    incremental_cursor_from_audit=True,
)

BAK_BASIC = DatasetSpec(
    name="bak_basic",
    tushare_api="bak_basic",
    target_table="market.bak_basic",
    primary_keys=["trade_date", "ts_code"],
    query_mode=QueryMode.BY_DATE,
    columns={
        "trade_date": "date",
        "ts_code": "text",
        "name": "text",
        "industry": "text",
        "area": "text",
        "pe_dyn": "numeric",
        "total_assets": "numeric",
        "liquid_assets": "numeric",
        "fixed_assets": "numeric",
        "reserved": "numeric",
        "reserved_pershare": "numeric",
        "eps": "numeric",
        "bvps": "numeric",
        "list_date": "date",
        "undp": "numeric",
        "per_undp": "numeric",
        "rev_yoy": "numeric",
        "profit_yoy": "numeric",
        "gpr": "numeric",
        "npr": "numeric",
        "holder_num": "bigint",
    },
    api_field_map={"pe_dyn": "pe"},  # Tushare returns "pe", DB column is "pe_dyn"
    batch_sleep=0.2,
    rate_per_minute=300,
)

MARGIN_DETAIL = DatasetSpec(
    name="margin_detail",
    tushare_api="margin_detail",
    target_table="market.margin_detail",
    primary_keys=["trade_date", "ts_code"],
    query_mode=QueryMode.BY_DATE,
    columns={
        "trade_date": "date",
        "ts_code": "text",
        "rzye": "numeric",
        "rqye": "numeric",
        "rzmre": "numeric",
        "rqyl": "numeric",
        "rzche": "numeric",
        "rqchl": "numeric",
        "rqmcl": "numeric",
        "rzrqye": "numeric",
    },
    batch_sleep=0.2,
    rate_per_minute=500,
)

STK_LIMIT = DatasetSpec(
    name="stk_limit",
    tushare_api="stk_limit",
    target_table="market.stk_limit",
    primary_keys=["trade_date", "ts_code"],
    query_mode=QueryMode.BY_DATE,
    columns={
        "trade_date": "date",
        "ts_code": "text",
        "pre_close": "numeric",
        "up_limit": "numeric",
        "down_limit": "numeric",
    },
    batch_sleep=0.3,
    rate_per_minute=200,
)

SUSPEND_D = DatasetSpec(
    name="suspend_d",
    tushare_api="suspend_d",
    target_table="market.suspend_d",
    primary_keys=["trade_date", "ts_code", "suspend_type"],
    query_mode=QueryMode.BY_DATE,
    columns={
        "trade_date": "date",
        "ts_code": "text",
        "suspend_type": "text",
        "suspend_timing": "text",
    },
    batch_sleep=0.3,
    rate_per_minute=200,
    replace_existing_dates=True,
)


# ---------------------------------------------------------------------------
# 3 Shenwan (申万) sector dataset definitions
# ---------------------------------------------------------------------------

SW_INDEX_CLASSIFY = DatasetSpec(
    name="sw_index_classify",
    tushare_api="index_classify",
    target_table="market.sw_index_classify",
    primary_keys=["index_code"],
    query_mode=QueryMode.SINGLE_CALL,
    columns={
        "index_code": "text",
        "industry_name": "text",
        "parent_code": "text",
        "level": "text",
        "industry_code": "text",
        "is_pub": "text",
        "src": "text",
    },
    api_params={"src": "SW2021"},
    supports_incremental=False,
    batch_sleep=0.2,
)

SW_INDEX_MEMBER = DatasetSpec(
    name="sw_index_member",
    tushare_api="index_member_all",
    target_table="market.sw_index_member",
    primary_keys=["l2_code", "ts_code", "in_date"],
    query_mode=QueryMode.BY_CODE,
    columns={
        "l1_code": "text",
        "l1_name": "text",
        "l2_code": "text",
        "l2_name": "text",
        "l3_code": "text",
        "l3_name": "text",
        "ts_code": "text",
        "name": "text",
        "in_date": "date",
        "out_date": "date",
        "is_new": "text",
    },
    code_source_sql=(
        "SELECT index_code FROM market.sw_index_classify "
        "WHERE level = 'L2' AND src = 'SW2021' ORDER BY index_code"
    ),
    code_param_name="l2_code",
    skip_date_params=True,
    supports_incremental=False,
    batch_sleep=0.2,
    rate_per_minute=300,
)

SW_DAILY = DatasetSpec(
    name="sw_daily",
    tushare_api="sw_daily",
    target_table="market.sw_daily",
    primary_keys=["ts_code", "trade_date"],
    query_mode=QueryMode.BY_CODE,
    columns={
        "ts_code": "text",
        "trade_date": "date",
        "name": "text",
        "open": "numeric",
        "low": "numeric",
        "high": "numeric",
        "close": "numeric",
        "change": "numeric",
        "pct_change": "numeric",
        "vol": "numeric",
        "amount": "numeric",
        "pe": "numeric",
        "pb": "numeric",
        "float_mv": "numeric",
        "total_mv": "numeric",
    },
    code_source_sql=(
        "SELECT index_code FROM market.sw_index_classify "
        "WHERE level = 'L2' AND src = 'SW2021' ORDER BY index_code"
    ),
    row_limit=4000,
    batch_sleep=0.2,
    rate_per_minute=300,
)


# ---------------------------------------------------------------------------
# Registry: name -> spec  (used by engine & API)
# ---------------------------------------------------------------------------

DATASET_REGISTRY: Dict[str, DatasetSpec] = {
    s.name: s
    for s in [
        DAILY_BASIC,
        ADJ_FACTOR,
        INDEX_DAILY,
        STOCK_BASIC,
        STOCK_ST,
        STOCK_ST_EVENTS,
        BAK_BASIC,
        STK_LIMIT,
        SUSPEND_D,
        MARGIN_DETAIL,
        SW_INDEX_CLASSIFY,
        SW_INDEX_MEMBER,
        SW_DAILY,
    ]
}
