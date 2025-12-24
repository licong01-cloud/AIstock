from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app_pg import get_conn  # type: ignore[attr-defined]


def fetch_column_comments(schema: str, table: str) -> pd.DataFrame:
    sql = """
    SELECT
      a.attnum,
      a.attname AS column_name,
      pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
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
        return pd.read_sql(sql, conn, params={"schema": schema, "table": table})


def summarize(df: pd.DataFrame) -> Tuple[int, int, List[str]]:
    total = int(df.shape[0])
    missing = df[df["comment"].fillna("").astype(str).str.strip() == ""]
    missing_cols = missing["column_name"].astype(str).tolist()
    return total, int(missing.shape[0]), missing_cols


def main() -> None:
    parser = argparse.ArgumentParser(description="Check PostgreSQL column comments for target tables")
    parser.add_argument("--schema", type=str, default="market")
    parser.add_argument(
        "--tables",
        type=str,
        default="daily_basic,moneyflow_ts",
        help="Comma separated table names under schema",
    )
    args = parser.parse_args()

    schema = args.schema.strip()
    tables = [t.strip() for t in (args.tables or "").split(",") if t.strip()]
    if not tables:
        raise SystemExit("No tables specified")

    for tbl in tables:
        print(f"\n== {schema}.{tbl} ==")
        df = fetch_column_comments(schema, tbl)
        total, miss_cnt, miss_cols = summarize(df)
        print(f"columns: {total}, missing_comment: {miss_cnt}")
        if miss_cols:
            print("missing:")
            for c in miss_cols:
                print(f"- {c}")
        # print detail table (trim)
        show = df.copy()
        show["comment"] = show["comment"].astype(str).str.replace("\n", " ")
        print("\npreview:")
        with pd.option_context("display.max_rows", 200, "display.max_colwidth", 80):
            print(show[["attnum", "column_name", "data_type", "comment"]].to_string(index=False))


if __name__ == "__main__":
    main()
