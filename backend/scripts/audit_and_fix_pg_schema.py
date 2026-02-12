import os
from typing import Dict, List, Tuple

import psycopg2  # type: ignore[import-untyped]
from dotenv import load_dotenv


def _connect():
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"), override=True)
    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
        application_name="AIstock-schema-audit",
    )


def _get_columns(cur, table: str) -> Dict[str, str]:
    cur.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return {r[0]: r[1] for r in cur.fetchall()}


def _add_missing_columns(cur, table: str, missing: List[Tuple[str, str]]) -> None:
    for col, ddl in missing:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {ddl};")


def audit_and_fix() -> None:
    # 只做“补列”，不做删列/改类型，避免破坏已有数据。
    expected: Dict[str, List[Tuple[str, str]]] = {
        "aistock_loop_catalog": [
            ("asset_bundle_id", "TEXT"),
            ("is_solidified", "BOOLEAN DEFAULT FALSE"),
            ("sync_status", "TEXT DEFAULT 'pending'"),
            ("manifest_schema_version", "INTEGER"),
            ("manifest_primary_workspace_id", "TEXT"),
            ("manifest_factor_entry_relpath", "TEXT"),
            ("manifest_model_weight_relpath", "TEXT"),
            ("manifest_config_relpath", "TEXT"),
            ("source_workspace_path", "TEXT"),
            ("log_dir", "TEXT"),
            ("log_uri", "TEXT"),
        ],
        "aistock_factor_catalog": [
            ("asset_bundle_id", "TEXT"),
        ],
        "aistock_strategy_catalog": [
            ("in_selection_center", "BOOLEAN DEFAULT FALSE"),
        ],
        "aistock_model_catalog": [
            ("asset_bundle_id", "TEXT"),
        ],
    }

    conn = _connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for table, cols in expected.items():
                existing = _get_columns(cur, table)
                missing = [(c, ddl) for c, ddl in cols if c not in existing]
                if missing:
                    _add_missing_columns(cur, table, missing)

            # 输出核查结果
            print("PG schema audit done. Current columns snapshot:")
            for table in expected:
                col_map = _get_columns(cur, table)
                print(f"- {table}: {len(col_map)} columns")
    finally:
        conn.close()


if __name__ == "__main__":
    audit_and_fix()
