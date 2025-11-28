from __future__ import annotations

import os
import psycopg2
import psycopg2.extras as pgx

pgx.register_uuid()

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

TARGET_PID = 20580


def main() -> None:
    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s);", (TARGET_PID,))
            row = cur.fetchone()
    result = row[0] if row else None
    print(f"pg_terminate_backend({TARGET_PID}) => {result}")


if __name__ == "__main__":
    main()
