from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import pandas as pd

from db.pg_pool import get_conn  # type: ignore[attr-defined]


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
    If DB connection fails, returns empty dict to trigger fallback.
    """
    try:
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
    except Exception as e:
        # DB connection failed, return empty dict to trigger fallback
        print(f"[WARN] Failed to fetch comments from DB for {schema}.{table}: {e}")
        return {}


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


def _bak_basic_source_to_export_map() -> Dict[str, str]:
    """DB column -> exported column (bb_* prefix)"""
    return {
        "pe_dyn": "bb_pe_dyn",
        "total_assets": "bb_total_assets",
        "liquid_assets": "bb_liquid_assets",
        "fixed_assets": "bb_fixed_assets",
        "reserved": "bb_reserved",
        "reserved_pershare": "bb_reserved_pershare",
        "eps": "bb_eps",
        "bvps": "bb_bvps",
        "undp": "bb_undp",
        "per_undp": "bb_per_undp",
        "rev_yoy": "bb_rev_yoy",
        "profit_yoy": "bb_profit_yoy",
        "gpr": "bb_gpr",
        "npr": "bb_npr",
        "holder_num": "bb_holder_num",
    }


def _cyq_perf_source_to_export_map() -> Dict[str, str]:
    """DB column -> exported column (cp_* prefix)"""
    return {
        "his_low": "cp_his_low",
        "his_high": "cp_his_high",
        "cost_5pct": "cp_cost_5pct",
        "cost_15pct": "cp_cost_15pct",
        "cost_50pct": "cp_cost_50pct",
        "cost_85pct": "cp_cost_85pct",
        "cost_95pct": "cp_cost_95pct",
        "weight_avg": "cp_weight_avg",
        "winner_rate": "cp_winner_rate",
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
    bak_basic_columns: Sequence[str] | None = None,
    cyq_perf_columns: Sequence[str] | None = None,
    daily_basic_dtypes: Dict[str, str] | None = None,
    moneyflow_dtypes: Dict[str, str] | None = None,
    bak_basic_dtypes: Dict[str, str] | None = None,
    cyq_perf_dtypes: Dict[str, str] | None = None,
) -> List[FieldMapRow]:
    rows: List[FieldMapRow] = []

    if daily_basic_columns is not None:
        comments = _fetch_pg_column_comments("market", "daily_basic")
        # 移除 fallback：如果数据库无 comment，保持为空，不使用硬编码
        src2exp = _daily_basic_source_to_export_map()
        exp2src = {v: k for k, v in src2exp.items()}
        for col in daily_basic_columns:
            src = exp2src.get(col)
            if not src:
                continue
            cn = comments.get(src, "")
            dtype = daily_basic_dtypes.get(col, "float64") if daily_basic_dtypes else "float64"
            rows.append(
                FieldMapRow(
                    name=col,
                    meaning_cn=cn,
                    source_table="daily_basic",
                    comment=cn,
                    dtype_hint=dtype,
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
            raw = comments.get(src, "")
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
                    dtype_hint=moneyflow_dtypes.get(col, "float64") if moneyflow_dtypes else "float64",
                )
            )

    if bak_basic_columns is not None:
        comments = _fetch_pg_column_comments("market", "bak_basic")
        # 移除 fallback：如果数据库无 comment，保持为空，不使用硬编码
        src2exp = _bak_basic_source_to_export_map()
        exp2src = {v: k for k, v in src2exp.items()}
        for col in bak_basic_columns:
            src = exp2src.get(col)
            if not src:
                continue
            cn = comments.get(src, "")
            rows.append(
                FieldMapRow(
                    name=col,
                    meaning_cn=cn,
                    source_table="bak_basic",
                    comment=cn,
                    dtype_hint=bak_basic_dtypes.get(col, "float64") if bak_basic_dtypes else "float64",
                )
            )

    if cyq_perf_columns is not None:
        comments = _fetch_pg_column_comments("market", "cyq_perf")
        # 移除 fallback：如果数据库无 comment，保持为空，不使用硬编码
        src2exp = _cyq_perf_source_to_export_map()
        exp2src = {v: k for k, v in src2exp.items()}
        for col in cyq_perf_columns:
            src = exp2src.get(col)
            if not src:
                continue
            cn = comments.get(src, "")
            rows.append(
                FieldMapRow(
                    name=col,
                    meaning_cn=cn,
                    source_table="cyq_perf",
                    comment=cn,
                    dtype_hint=cyq_perf_dtypes.get(col, "float64") if cyq_perf_dtypes else "float64",
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
