"""Bootstrap the isolated PostgreSQL service used by the Watchlist CI lane."""

from __future__ import annotations

import os

from backend.db.init_watchlist_schema import init_watchlist_schema
from backend.db.pg_pool import get_conn


_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def _require_disposable_ci_target() -> None:
    if os.environ.get("GITHUB_ACTIONS", "").lower() != "true":
        raise RuntimeError("Watchlist CI schema bootstrap is restricted to GitHub Actions")
    if os.environ.get("AISTOCK_CI_DISPOSABLE_POSTGRES") != "1":
        raise RuntimeError("Watchlist CI schema bootstrap requires an explicit disposable target")
    if os.environ.get("TDX_DB_HOST", "").strip().lower() not in _LOOPBACK_HOSTS:
        raise RuntimeError("Watchlist CI schema bootstrap requires a loopback PostgreSQL host")
    if os.environ.get("TDX_DB_NAME") != "aistock_dev":
        raise RuntimeError("Watchlist CI schema bootstrap requires the fixed aistock_dev database")


def bootstrap_disposable_schema() -> None:
    _require_disposable_ci_target()
    init_watchlist_schema()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS app.analysis_records (
                    id BIGSERIAL PRIMARY KEY,
                    ts_code TEXT NOT NULL,
                    analysis_date TIMESTAMPTZ,
                    final_decision JSONB,
                    agents_results JSONB,
                    discussion_result JSONB
                )
                """
            )


if __name__ == "__main__":
    bootstrap_disposable_schema()
