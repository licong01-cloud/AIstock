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
            _print_header("Checking if app.ts_lstm_trade_agg exists")
            cur.execute("SELECT to_regclass('app.ts_lstm_trade_agg') AS reg")
            row = cur.fetchone() or {}
            reg = row.get("reg")
            if reg:
                print("Table app.ts_lstm_trade_agg already exists (regclass=", reg, ")")
            else:
                print("Table app.ts_lstm_trade_agg does not exist, creating...")
                cur.execute(
                    """
                    CREATE TABLE app.ts_lstm_trade_agg (
                        symbol              TEXT            NOT NULL,
                        bucket_start_time   TIMESTAMPTZ     NOT NULL,
                        freq                TEXT            NOT NULL,
                        buy_volume          BIGINT          NOT NULL,
                        sell_volume         BIGINT          NOT NULL,
                        neutral_volume      BIGINT          NOT NULL,
                        order_flow_imbalance DOUBLE PRECISION NOT NULL,
                        big_trade_volume    BIGINT          NOT NULL,
                        big_trade_count     INTEGER         NOT NULL,
                        big_trade_ratio     DOUBLE PRECISION NOT NULL,
                        realized_vol        DOUBLE PRECISION NOT NULL,
                        trade_count         INTEGER         NOT NULL,
                        avg_trade_size      DOUBLE PRECISION NOT NULL,
                        intensity           DOUBLE PRECISION NOT NULL,
                        extra_json          JSONB           NOT NULL DEFAULT '{}'::jsonb
                    )
                    """
                )
                print("Base table created.")

                _print_header("Creating UNIQUE constraint on (symbol, bucket_start_time, freq)")
                cur.execute(
                    """
                    ALTER TABLE app.ts_lstm_trade_agg
                      ADD CONSTRAINT ts_lstm_trade_agg_uq
                      UNIQUE(symbol, bucket_start_time, freq)
                    """
                )
                print("Unique constraint created.")

                _print_header("Converting to Timescale hypertable (if extension available)")
                try:
                    # if_not_exists avoids errors if already a hypertable
                    cur.execute(
                        "SELECT create_hypertable('app.ts_lstm_trade_agg', 'bucket_start_time', if_not_exists => TRUE)"
                    )
                    print("create_hypertable executed.")
                except Exception as exc:  # noqa: BLE001
                    print("create_hypertable failed (continuing anyway):", exc)

            _print_header("Final check")
            cur.execute(
                """
                SELECT column_name, data_type
                  FROM information_schema.columns
                 WHERE table_schema = 'app'
                   AND table_name   = 'ts_lstm_trade_agg'
                 ORDER BY ordinal_position
                """
            )
            cols = cur.fetchall() or []
            for c in cols:
                print("-", c.get("column_name"), "::", c.get("data_type"))

    _print_header("patch_create_ts_lstm_trade_agg completed")


if __name__ == "__main__":
    main()
