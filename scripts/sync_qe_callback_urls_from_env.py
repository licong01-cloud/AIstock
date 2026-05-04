#!/usr/bin/env python
"""Sync QE compute-node callback URLs from the project .env file.

This keeps WSL and remote QE workers pointed at the same AIstock FastAPI
callback base without baking an address or port into code or migrations.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_env() -> None:
    load_dotenv(REPO_ROOT / ".env", override=True)


def _callback_base_from_env() -> tuple[str, str]:
    env_names = (
        "AISTOCK_QE_CALLBACK_BASE_URL",
        "AISTOCK_BACKEND_CALLBACK_BASE_URL",
        "AISTOCK_BACKEND_BASE_URL",
    )
    for name in env_names:
        value = (os.environ.get(name) or "").strip().rstrip("/")
        if not value:
            continue
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise SystemExit(f"{name} must be an absolute http(s) URL, got: {value!r}")
        return name, value
    raise SystemExit(
        "Set AISTOCK_QE_CALLBACK_BASE_URL or AISTOCK_BACKEND_CALLBACK_BASE_URL in .env first."
    )


def _connect():
    return psycopg2.connect(
        host=os.environ.get("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("TDX_DB_PORT", "5432")),
        dbname=os.environ.get("TDX_DB_NAME", "aistock"),
        user=os.environ.get("TDX_DB_USER", "postgres"),
        password=os.environ.get("TDX_DB_PASSWORD", ""),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--nodes",
        nargs="*",
        help="Optional node_id list. Defaults to every row in infra.compute_nodes.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without updating the DB.")
    args = parser.parse_args()

    _load_env()
    env_name, callback_base = _callback_base_from_env()

    with _connect() as conn:
        with conn.cursor() as cur:
            if args.nodes:
                cur.execute(
                    """
                    SELECT node_id, callback_url
                    FROM infra.compute_nodes
                    WHERE node_id = ANY(%s)
                    ORDER BY node_id
                    """,
                    (args.nodes,),
                )
            else:
                cur.execute(
                    """
                    SELECT node_id, callback_url
                    FROM infra.compute_nodes
                    ORDER BY node_id
                    """
                )
            rows = cur.fetchall()
            if not rows:
                raise SystemExit("No compute nodes matched.")

            for node_id, old_url in rows:
                print(f"{node_id}: {old_url or '<empty>'} -> {callback_base} ({env_name})")

            if args.dry_run:
                conn.rollback()
                return 0

            node_ids = [row[0] for row in rows]
            cur.execute(
                """
                UPDATE infra.compute_nodes
                SET callback_url = %s,
                    updated_at = NOW()
                WHERE node_id = ANY(%s)
                """,
                (callback_base, node_ids),
            )
            print(f"updated_rows={cur.rowcount}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
