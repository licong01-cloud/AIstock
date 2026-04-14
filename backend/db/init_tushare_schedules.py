"""Idempotent initializer for default Tushare ingestion schedules.

Inserts/updates schedule rows into market.ingestion_schedules.
Safe to run multiple times — uses ON CONFLICT DO UPDATE to refresh times.
"""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List

from .pg_pool import get_conn


_DEFAULT_SCHEDULES: List[Dict[str, Any]] = [
    # 盘前
    {"dataset": "stk_limit",          "mode": "incremental", "frequency": "daily", "at": "09:10"},

    # Tushare 盘后数据（16:00-16:30 更新 +10min 缓冲）
    {"dataset": "daily_basic",        "mode": "incremental", "frequency": "daily", "at": "16:40"},
    {"dataset": "adj_factor",         "mode": "incremental", "frequency": "daily", "at": "16:43"},
    {"dataset": "index_daily",        "mode": "incremental", "frequency": "daily", "at": "16:46"},
    {"dataset": "sw_sector",          "mode": "incremental", "frequency": "daily", "at": "16:49"},

    # Tushare 较晚更新数据（16:30-17:00 更新 +10min 缓冲）
    {"dataset": "margin_detail",      "mode": "incremental", "frequency": "daily", "at": "17:07"},
    {"dataset": "moneyflow_ts",       "mode": "incremental", "frequency": "daily", "at": "17:10"},
    {"dataset": "bak_basic",          "mode": "incremental", "frequency": "daily", "at": "17:13"},

    # 不定期数据集 → 17:30 之后
    {"dataset": "stock_basic",        "mode": "init",        "frequency": "daily", "at": "17:30"},
    {"dataset": "stock_st",           "mode": "incremental", "frequency": "daily", "at": "17:33"},

    # 派生数据（等上游全部完成）
    {"dataset": "sector_data",        "mode": "incremental", "frequency": "daily", "at": "17:50"},

    # 数据新鲜度检查
    {"dataset": "_data_freshness_check", "mode": "check",    "frequency": "daily", "at": "18:00"},
]


def ensure_tushare_schedules() -> int:
    """Insert or update default Tushare schedules.

    Returns the number of rows actually inserted or updated.
    """
    affected = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for entry in _DEFAULT_SCHEDULES:
                sid = uuid.uuid4()
                options = json.dumps({"at": entry["at"]}, ensure_ascii=False)
                cur.execute(
                    """
                    INSERT INTO market.ingestion_schedules
                        (schedule_id, dataset, mode, frequency, enabled, options,
                         created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (dataset, mode) DO UPDATE SET
                        frequency = EXCLUDED.frequency,
                        options = EXCLUDED.options,
                        enabled = EXCLUDED.enabled,
                        updated_at = NOW()
                    """,
                    (sid, entry["dataset"], entry["mode"],
                     entry["frequency"], entry.get("enabled", True), options),
                )
                if cur.rowcount > 0:
                    affected += 1
    return affected


if __name__ == "__main__":
    n = ensure_tushare_schedules()
    print(f"[DONE] upserted {n} tushare schedule(s)")
