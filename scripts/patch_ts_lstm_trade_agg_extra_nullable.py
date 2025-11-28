import os
from typing import Any, Dict

import psycopg2
from psycopg2.extras import RealDictCursor


DB_CFG: Dict[str, Any] = {
    "host": os.getenv("TDX_DB_HOST", "localhost"),
    "port": int(os.getenv("TDX_DB_PORT", "5432")),
    "user": os.getenv("TDX_DB_USER", "postgres"),
    "password": os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    "dbname": os.getenv("TDX_DB_NAME", "aistock"),
}


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> None:
    _print_header("Connecting to PostgreSQL")
    print("Using config:", DB_CFG)

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            _print_header("Checking column extra_json nullability on app.ts_lstm_trade_agg")
            cur.execute(
                """
                SELECT is_nullable, column_default
                  FROM information_schema.columns
                 WHERE table_schema = 'app'
                   AND table_name   = 'ts_lstm_trade_agg'
                   AND column_name  = 'extra_json'
                """
            )
            row = cur.fetchone() or {}
            print("Before patch:", row)

            _print_header("Dropping NOT NULL on extra_json (if present)")
            cur.execute("ALTER TABLE app.ts_lstm_trade_agg ALTER COLUMN extra_json DROP NOT NULL")
            # 确保默认值为 '{}'::jsonb（幂等操作）
            cur.execute(
                "ALTER TABLE app.ts_lstm_trade_agg ALTER COLUMN extra_json SET DEFAULT '{}'::jsonb"
            )
            print("Column extra_json is now nullable with default '{}'::jsonb.")

            _print_header("After patch")
            cur.execute(
                """
                SELECT is_nullable, column_default
                  FROM information_schema.columns
                 WHERE table_schema = 'app'
                   AND table_name   = 'ts_lstm_trade_agg'
                   AND column_name  = 'extra_json'
                """
            )
            row2 = cur.fetchone() or {}
            print("After patch:", row2)

    _print_header("patch_ts_lstm_trade_agg_extra_nullable completed")


if __name__ == "__main__":
    main()
