"""Shared fixtures for dev_db tests (Batch A/C helper validation)."""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import pytest

# Make scripts/ importable so tests can `from scripts.dev_db._seq_reset_helpers import ...`
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

ENV_FILE = Path("F:/Dev/AIstock/.env")


def _parse_env() -> dict[str, str]:
    cfg: dict[str, str] = {}
    if not ENV_FILE.exists():
        return cfg
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def _dev_creds() -> dict[str, Any] | None:
    cfg = _parse_env()
    keys = ("TDX_DB_DEV_HOST", "TDX_DB_DEV_PORT", "TDX_DB_DEV_NAME",
            "TDX_DB_DEV_USER", "TDX_DB_DEV_PASSWORD")
    if not all(k in cfg for k in keys):
        return None
    return {
        "host": cfg["TDX_DB_DEV_HOST"],
        "port": int(cfg["TDX_DB_DEV_PORT"]),
        "dbname": cfg["TDX_DB_DEV_NAME"],
        "user": cfg["TDX_DB_DEV_USER"],
        "password": cfg["TDX_DB_DEV_PASSWORD"],
    }


@pytest.fixture(scope="session")
def dev_db_creds() -> dict[str, Any]:
    creds = _dev_creds()
    if creds is None or creds["port"] != 5433 or "dev" not in creds["dbname"]:
        pytest.skip("dev DB creds missing or not on port 5433 with 'dev' in dbname")
    return creds


@pytest.fixture
def dev_conn(dev_db_creds: dict[str, Any]):
    @contextmanager
    def _provider() -> Iterator[Any]:
        conn = psycopg2.connect(**dev_db_creds, connect_timeout=3)
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass
    return _provider
