"""Create the market.data_alerts table for data health alerting.

Safe to run multiple times — uses IF NOT EXISTS.
"""
from __future__ import annotations

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

DDL = """
CREATE TABLE IF NOT EXISTS market.data_alerts (
    alert_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    severity     TEXT NOT NULL CHECK (severity IN ('info','warning','error','critical')),
    dataset      TEXT,
    alert_type   TEXT NOT NULL CHECK (alert_type IN (
        'stale','low_coverage','gap','zero_rows','api_failure','retry_exhausted'
    )),
    title        TEXT NOT NULL,
    message      TEXT NOT NULL,
    details      JSONB,
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    ack_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_data_alerts_active
    ON market.data_alerts (acknowledged, severity, created_at DESC);
"""


def main() -> None:
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()
    cur.close()
    conn.close()
    print("[OK] market.data_alerts table and index created")


if __name__ == "__main__":
    main()
