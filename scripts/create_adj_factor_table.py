import os
import sys

from dotenv import load_dotenv
import psycopg2


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS market.adj_factor (
    ts_code    text    NOT NULL,
    trade_date date    NOT NULL,
    adj_factor double precision NOT NULL,
    PRIMARY KEY (ts_code, trade_date)
);
"""


def get_db_cfg():
    load_dotenv()
    return dict(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
    )


def main() -> None:
    cfg = get_db_cfg()
    print("[INFO] Connecting to DB with config:", {k: (v if k != "password" else "***") for k, v in cfg.items()})
    try:
        with psycopg2.connect(**cfg) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                print("[INFO] Creating table market.adj_factor if not exists ...")
                cur.execute(CREATE_SQL)
                print("[RESULT] Table market.adj_factor is ensured (created or already existed).")
    except Exception as e:  # noqa: BLE001
        print("[ERROR] Failed to create market.adj_factor:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
