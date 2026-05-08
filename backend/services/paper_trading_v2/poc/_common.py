"""Shared bootstrap for PoC scripts: load .env.poc + put repo xtquant on sys.path.

Fail-fast: any missing prereq raises immediately (no silent fallback).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def bootstrap() -> dict:
    poc_dir = Path(__file__).resolve().parent
    env_poc = poc_dir / ".env.poc"
    if not env_poc.is_file():
        raise FileNotFoundError(f".env.poc not found at {env_poc}")

    from dotenv import load_dotenv
    load_dotenv(env_poc, override=True)

    xt_dir = Path(os.environ["MINIQMT_XTQUANT_DIR"])
    if not xt_dir.is_dir():
        raise RuntimeError(f"MINIQMT_XTQUANT_DIR missing: {xt_dir}")
    if str(xt_dir.parent) not in sys.path:
        sys.path.insert(0, str(xt_dir.parent))

    userdata = Path(os.environ["MINIQMT_USERDATA_PATH"])
    if not userdata.is_dir():
        raise RuntimeError(f"MINIQMT_USERDATA_PATH missing: {userdata}")

    return {
        "userdata_path": str(userdata),
        "session_id": int(os.environ["MINIQMT_SESSION_ID"]),
        "account_id": os.environ["MINIQMT_ACCOUNT_ID"],
        "test_stock": os.environ.get("POC_TEST_STOCK", "600000.SH"),
        "limit_price_offset": float(os.environ.get("POC_LIMIT_PRICE_OFFSET", "-1.50")),
        "order_volume": int(os.environ.get("POC_ORDER_VOLUME", "100")),
        "connect_timeout_s": int(os.environ.get("MINIQMT_CONNECT_TIMEOUT_SECONDS", "15")),
    }
