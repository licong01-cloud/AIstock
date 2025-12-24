from __future__ import annotations

import json
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from psycopg2.extras import execute_values

from ..db.pg_pool import get_conn


def read_signals_parquet(path: str) -> pd.DataFrame:
    """Read signals.parquet robustly.

    1. Try pandas.read_parquet (fast path).
    2. If it fails due to Arrow extension types, fall back to pyarrow.parquet.read_table,
       and cast ExtensionType columns to their storage_type (or string) before
       converting to pandas.
    """

    try:
        return pd.read_parquet(path)
    except Exception as e:
        msg = str(e)
        if "arrow.py_extension_type" not in msg:
            # other errors bubble up
            raise

    # fallback: use pyarrow directly and normalize extension columns
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    fields = table.schema
    cols: Dict[str, pa.Array] = {}
    for i, field in enumerate(fields):
        name = field.name
        col = table.column(i)
        t = field.type
        if isinstance(t, pa.ExtensionType):
            try:
                # try cast to underlying storage_type
                casted = pa.compute.cast(col, t.storage_type)
            except Exception:
                # last resort: cast to string
                casted = pa.compute.cast(col, pa.string())
            cols[name] = casted
        else:
            cols[name] = col

    fixed = pa.table(cols)
    return fixed.to_pandas()


def _pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        key = c.lower()
        if key in lower_map:
            return lower_map[key]
    return None


def _to_trade_date(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return series.dt.date
    # try parse
    parsed = pd.to_datetime(series, errors="coerce")
    if pd.api.types.is_datetime64_any_dtype(parsed):
        return parsed.dt.date
    return series


def normalize_signals_df(df: pd.DataFrame, output_mode: str) -> pd.DataFrame:
    df = df.copy()

    date_col = _pick_col(df, ["trade_date", "date", "datetime", "time"])
    if not date_col:
        raise ValueError("signals parquet missing date column (trade_date/date/datetime/time)")

    symbol_col = _pick_col(df, ["symbol", "ts_code", "stock_code", "code", "instrument"])
    if not symbol_col:
        raise ValueError("signals parquet missing symbol column (symbol/ts_code/stock_code/code)")

    df["trade_date"] = _to_trade_date(df[date_col])
    df["symbol"] = df[symbol_col].astype(str)

    rank_col = _pick_col(df, ["rank", "topk_rank", "k", "position"])
    score_col = _pick_col(df, ["score", "pred", "signal", "alpha", "value", "pred_return"])
    weight_col = _pick_col(df, ["target_weight", "weight", "w"])
    action_col = _pick_col(df, ["action", "side", "direction"])

    out: Dict[str, Any] = {
        "trade_date": df["trade_date"],
        "symbol": df["symbol"],
        "rank": df[rank_col] if rank_col else None,
        "score": df[score_col] if score_col else None,
        "target_weight": df[weight_col] if weight_col else None,
        "action": df[action_col].astype(str) if action_col else None,
    }

    norm = pd.DataFrame({k: v for k, v in out.items() if v is not None or k in {"trade_date", "symbol"}})

    # Ensure dtypes
    if "rank" in norm.columns:
        norm["rank"] = pd.to_numeric(norm["rank"], errors="coerce").astype("Int64")
    if "score" in norm.columns:
        norm["score"] = pd.to_numeric(norm["score"], errors="coerce")
    if "target_weight" in norm.columns:
        norm["target_weight"] = pd.to_numeric(norm["target_weight"], errors="coerce")

    norm["output_mode"] = output_mode

    # drop rows with missing core fields
    norm = norm.dropna(subset=["trade_date", "symbol"]).copy()
    return norm


def persist_rdagent_signals(
    strategy_id: str,
    strategy_version_id: str,
    output_mode: str,
    df: pd.DataFrame,
) -> int:
    """Upsert signals into trading.rdagent_signal. Returns inserted row count (best-effort)."""

    norm = normalize_signals_df(df, output_mode=output_mode)
    if norm.empty:
        return 0

    rows: List[Tuple[Any, ...]] = []
    for r in norm.itertuples(index=False):
        # r contains: trade_date, symbol, maybe rank, score, target_weight, action, output_mode
        trade_date = getattr(r, "trade_date")
        symbol = getattr(r, "symbol")
        rank = getattr(r, "rank", None)
        score = getattr(r, "score", None)
        target_weight = getattr(r, "target_weight", None)
        action = getattr(r, "action", None)
        rows.append(
            (
                strategy_id,
                strategy_version_id,
                trade_date,
                symbol,
                output_mode,
                int(rank) if rank is not None and not pd.isna(rank) else None,
                float(score) if score is not None and not pd.isna(score) else None,
                float(target_weight)
                if target_weight is not None and not pd.isna(target_weight)
                else None,
                action,
                None,
            )
        )

    sql = """
        INSERT INTO trading.rdagent_signal (
            strategy_id,
            strategy_version_id,
            trade_date,
            symbol,
            output_mode,
            rank,
            score,
            target_weight,
            action,
            meta
        )
        VALUES %s
        ON CONFLICT (strategy_version_id, trade_date, symbol)
        DO UPDATE SET
            output_mode = EXCLUDED.output_mode,
            rank = EXCLUDED.rank,
            score = EXCLUDED.score,
            target_weight = EXCLUDED.target_weight,
            action = EXCLUDED.action,
            meta = EXCLUDED.meta
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()

    return len(rows)


def load_signals_overview(strategy_version_id: str) -> Dict[str, Any]:
    sql = """
        SELECT
            MIN(trade_date) AS start_date,
            MAX(trade_date) AS end_date,
            COUNT(*) AS row_count,
            COUNT(DISTINCT trade_date) AS date_count,
            COUNT(DISTINCT symbol) AS symbol_count
        FROM trading.rdagent_signal
        WHERE strategy_version_id = %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (strategy_version_id,))
            row = cur.fetchone()
    return {
        "start_date": row[0].isoformat() if row and row[0] else None,
        "end_date": row[1].isoformat() if row and row[1] else None,
        "row_count": int(row[2] or 0) if row else 0,
        "date_count": int(row[3] or 0) if row else 0,
        "symbol_count": int(row[4] or 0) if row else 0,
    }


def load_signals_for_date(strategy_version_id: str, trade_date: str, k: int = 50) -> List[Dict[str, Any]]:
    sql = """
        SELECT symbol, output_mode, rank, score, target_weight, action
        FROM trading.rdagent_signal
        WHERE strategy_version_id = %s
          AND trade_date = %s
        ORDER BY
          CASE WHEN rank IS NULL THEN 1 ELSE 0 END,
          rank ASC NULLS LAST,
          score DESC NULLS LAST
        LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (strategy_version_id, trade_date, k))
            rows = cur.fetchall()
    return [
        {
            "symbol": r[0],
            "output_mode": r[1],
            "rank": r[2],
            "score": r[3],
            "target_weight": float(r[4]) if r[4] is not None else None,
            "action": r[5],
        }
        for r in rows
    ]


def load_symbol_series(strategy_version_id: str, symbol: str, limit: int = 200) -> List[Dict[str, Any]]:
    sql = """
        SELECT trade_date, output_mode, rank, score, target_weight, action
        FROM trading.rdagent_signal
        WHERE strategy_version_id = %s
          AND symbol = %s
        ORDER BY trade_date DESC
        LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (strategy_version_id, symbol, limit))
            rows = cur.fetchall()
    return [
        {
            "trade_date": r[0].isoformat() if r[0] else None,
            "output_mode": r[1],
            "rank": r[2],
            "score": r[3],
            "target_weight": float(r[4]) if r[4] is not None else None,
            "action": r[5],
        }
        for r in rows
    ]
