from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import pandas as pd

from app_pg import get_conn  # type: ignore[attr-defined]


@dataclass(frozen=True)
class FieldMapRow:
    name: str
    meaning_cn: str
    unit: str = ""
    source_table: str = ""
    comment: str = ""
    dtype_hint: str = ""


def _fetch_pg_column_comments(schema: str, table: str) -> Dict[str, str]:
    """Fetch column comments via pg_catalog.

    Returns mapping: column_name -> comment (empty string if no comment).
    """

    sql = """
    SELECT
      a.attname AS column_name,
      COALESCE(d.description, '') AS comment
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    LEFT JOIN pg_catalog.pg_description d
      ON d.objoid = a.attrelid AND d.objsubid = a.attnum
    WHERE n.nspname = %(schema)s
      AND c.relname = %(table)s
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum;
    """

    with get_conn() as conn:  # type: ignore[attr-defined]
        df = pd.read_sql(sql, conn, params={"schema": schema, "table": table})

    out: Dict[str, str] = {}
    for _, r in df.iterrows():
        col = str(r.get("column_name") or "").strip()
        if not col:
            continue
        out[col] = str(r.get("comment") or "").strip().replace("\n", " ")
    return out


def _infer_dtype_hints(df: pd.DataFrame) -> Dict[str, str]:
    return {c: str(df[c].dtype) for c in df.columns}


def _daily_basic_source_to_export_map() -> Dict[str, str]:
    # DB column -> exported column
    return {
        "close": "db_close",
        "turnover_rate": "db_turnover_rate",
        "turnover_rate_f": "db_turnover_rate_f",
        "volume_ratio": "db_volume_ratio",
        "pe": "db_pe",
        "pe_ttm": "db_pe_ttm",
        "pb": "db_pb",
        "ps": "db_ps",
        "ps_ttm": "db_ps_ttm",
        "dv_ratio": "db_dv_ratio",
        "dv_ttm": "db_dv_ttm",
        "total_share": "db_total_share",
        "float_share": "db_float_share",
        "free_share": "db_free_share",
        "total_mv": "db_total_mv",
        "circ_mv": "db_circ_mv",
    }


def _daily_basic_comment_fallback() -> Dict[str, str]:
    """Fallback Chinese meanings for market.daily_basic columns.

    Some deployments created market.daily_basic with inline SQL comments ("-- ...")
    rather than PostgreSQL COMMENT ON COLUMN, which means pg_catalog does not store
    them. This mapping ensures we can still export a useful field map.
    """

    return {
        "trade_date": "交易日期",
        "ts_code": "TS股票代码",
        "close": "当日收盘价",
        "turnover_rate": "换手率(%)",
        "turnover_rate_f": "换手率(自由流通股)",
        "volume_ratio": "量比",
        "pe": "市盈率(总市值/净利润, 亏损的PE为空)",
        "pe_ttm": "市盈率(TTM,亏损的PE为空)",
        "pb": "市净率(总市值/净资产)",
        "ps": "市销率",
        "ps_ttm": "市销率(TTM)",
        "dv_ratio": "股息率(%)",
        "dv_ttm": "股息率(TTM)(%)",
        "total_share": "总股本(万股)",
        "float_share": "流通股本(万股)",
        "free_share": "自由流通股本(万)",
        "total_mv": "总市值(万元)",
        "circ_mv": "流通市值(万元)",
    }


def _moneyflow_source_to_export_map() -> Dict[str, str]:
    # DB column -> exported column
    return {
        "buy_sm_vol": "mf_sm_buy_vol",
        "sell_sm_vol": "mf_sm_sell_vol",
        "buy_sm_amount": "mf_sm_buy_amt",
        "sell_sm_amount": "mf_sm_sell_amt",
        "buy_md_vol": "mf_md_buy_vol",
        "sell_md_vol": "mf_md_sell_vol",
        "buy_md_amount": "mf_md_buy_amt",
        "sell_md_amount": "mf_md_sell_amt",
        "buy_lg_vol": "mf_lg_buy_vol",
        "sell_lg_vol": "mf_lg_sell_vol",
        "buy_lg_amount": "mf_lg_buy_amt",
        "sell_lg_amount": "mf_lg_sell_amt",
        "buy_elg_vol": "mf_elg_buy_vol",
        "sell_elg_vol": "mf_elg_sell_vol",
        "buy_elg_amount": "mf_elg_buy_amt",
        "sell_elg_amount": "mf_elg_sell_amt",
        "net_mf_vol": "mf_net_vol",
        "net_mf_amount": "mf_net_amt",
    }


def _normalize_moneyflow_comment(comment: str) -> str:
    # DB is in 手/万元, but exporter converts to 股/元.
    c = (comment or "").strip()
    c = c.replace("（手）", "（股）")
    c = c.replace("(手)", "(股)")
    c = c.replace("（万元）", "（元）")
    c = c.replace("(万元)", "(元)")
    return c


def build_field_map_rows_for_snapshot(
    *,
    daily_basic_columns: Sequence[str] | None,
    moneyflow_columns: Sequence[str] | None,
) -> List[FieldMapRow]:
    rows: List[FieldMapRow] = []

    if daily_basic_columns is not None:
        comments = _fetch_pg_column_comments("market", "daily_basic")
        if not comments or all(not (v or "").strip() for v in comments.values()):
            comments = _daily_basic_comment_fallback()
        src2exp = _daily_basic_source_to_export_map()
        exp2src = {v: k for k, v in src2exp.items()}
        for col in daily_basic_columns:
            src = exp2src.get(col)
            if not src:
                continue
            cn = comments.get(src) or col
            rows.append(
                FieldMapRow(
                    name=col,
                    meaning_cn=cn,
                    source_table="daily_basic",
                    comment=cn,
                )
            )

    if moneyflow_columns is not None:
        comments = _fetch_pg_column_comments("market", "moneyflow_ts")
        src2exp = _moneyflow_source_to_export_map()
        exp2src = {v: k for k, v in src2exp.items()}
        for col in moneyflow_columns:
            src = exp2src.get(col)
            if not src:
                continue
            raw = comments.get(src) or col
            cn = _normalize_moneyflow_comment(raw)
            unit = ""
            if col.endswith("_amt"):
                unit = "元"
            elif col.endswith("_vol"):
                unit = "股"
            rows.append(
                FieldMapRow(
                    name=col,
                    meaning_cn=cn,
                    unit=unit,
                    source_table="moneyflow",
                    comment=cn,
                )
            )

    # Ensure uniqueness by `name` (last wins)
    dedup: Dict[str, FieldMapRow] = {}
    for r in rows:
        dedup[r.name] = r
    return [dedup[k] for k in sorted(dedup.keys())]


def write_field_map_csv(rows: Iterable[FieldMapRow], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["name", "meaning_cn", "unit", "source_table", "comment", "dtype_hint"]
    # Use UTF-8 BOM for better compatibility with Windows Excel.
    # Use QUOTE_ALL to avoid column shifting in naive CSV readers when fields contain commas/quotes.
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for r in rows:
            meaning_cn = (r.meaning_cn or "").replace("\r", " ").replace("\n", " ")
            unit = (r.unit or "").replace("\r", " ").replace("\n", " ")
            source_table = (r.source_table or "").replace("\r", " ").replace("\n", " ")
            comment = (r.comment or "").replace("\r", " ").replace("\n", " ")
            dtype_hint = (r.dtype_hint or "").replace("\r", " ").replace("\n", " ")
            w.writerow(
                {
                    "name": r.name,
                    "meaning_cn": meaning_cn,
                    "unit": unit,
                    "source_table": source_table,
                    "comment": comment,
                    "dtype_hint": dtype_hint,
                }
            )


def attach_column_comments_to_h5(h5_path: Path, column_to_cn: Dict[str, str]) -> None:
    """Attach column comment mapping to an existing HDF5 (pandas HDFStore).

    Stores JSON on storer attrs for key='data'.
    """

    if not h5_path.exists():
        raise FileNotFoundError(str(h5_path))

    payload = json.dumps(column_to_cn, ensure_ascii=False)
    with pd.HDFStore(str(h5_path), mode="a") as store:
        storer = store.get_storer("data")
        # keep both a raw dict and json for portability
        storer.attrs.column_comments_json = payload
        storer.attrs.column_comments = column_to_cn
