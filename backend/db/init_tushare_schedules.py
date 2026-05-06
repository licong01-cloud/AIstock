"""Idempotent initializer for default Tushare ingestion schedules.

Inserts/updates schedule rows into market.ingestion_schedules.
Safe to run multiple times — uses ON CONFLICT DO UPDATE to refresh times.
"""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List

from dotenv import load_dotenv

from .pg_pool import get_conn


_DEFAULT_SCHEDULES: List[Dict[str, Any]] = [
    # ── Phase 1 — 盘前 ──────────────────────────────────────────────
    {"dataset": "stk_limit",             "mode": "incremental", "frequency": "daily", "at": "09:01"},
    {"dataset": "suspend_d",             "mode": "incremental", "frequency": "1h",
     "date_strategy": "current_and_next_trading_day", "skip_auto_range": True},
    {"dataset": "_suspend_d_tminus1_1730", "mode": "incremental", "frequency": "daily", "at": "17:30",
     "date_strategy": "next_trading_day", "skip_auto_range": True},
    {"dataset": "_suspend_d_morning_0730", "mode": "incremental", "frequency": "daily", "at": "07:30",
     "date_strategy": "current_or_next_trading_day", "skip_auto_range": True},
    {"dataset": "_suspend_d_preopen_0850", "mode": "incremental", "frequency": "daily", "at": "08:50",
     "date_strategy": "current_or_next_trading_day", "skip_auto_range": True},
    {"dataset": "_suspend_d_preopen_0905", "mode": "incremental", "frequency": "daily", "at": "09:05",
     "date_strategy": "current_or_next_trading_day", "skip_auto_range": True},
    {"dataset": "_suspend_d_midday_1240", "mode": "incremental", "frequency": "daily", "at": "12:40",
     "date_strategy": "current_or_next_trading_day", "skip_auto_range": True},
    {"dataset": "_suspend_d_close_1610", "mode": "incremental", "frequency": "daily", "at": "16:10",
     "date_strategy": "current_and_next_trading_day", "skip_auto_range": True},

    # ── Phase 2a — 周末补偿检查（每天10:00触发，仅周六实际执行）────
    {"dataset": "_weekend_compensation", "mode": "incremental", "frequency": "daily", "at": "10:00"},

    # ── Phase 2 — TDX 数据（收盘后即可） ────────────────────────────
    {"dataset": "kline_daily_raw",       "mode": "incremental", "frequency": "daily", "at": "16:10"},
    {"dataset": "kline_minute_raw",      "mode": "incremental", "frequency": "daily", "at": "16:20"},

    # ── Phase 3 — Tushare 早期数据（16:00-16:30 更新 +10min 缓冲） ──
    {"dataset": "daily_basic",           "mode": "incremental", "frequency": "daily", "at": "16:40"},
    {"dataset": "adj_factor",            "mode": "incremental", "frequency": "daily", "at": "16:45"},
    {"dataset": "index_daily",           "mode": "incremental", "frequency": "daily", "at": "16:50"},
    {"dataset": "stock_basic",           "mode": "init",        "frequency": "daily", "at": "16:55"},
    {"dataset": "stock_st",              "mode": "incremental", "frequency": "daily", "at": "17:00"},
    {"dataset": "stock_st_events",       "mode": "incremental", "frequency": "daily", "at": "20:40"},
    {"dataset": "anns_metadata",         "mode": "incremental", "frequency": "1h",
     "lookback_days": 2, "source": "eastmoney", "workers": 1, "request_sleep": 0.05,
     "skip_auto_range": True},
    {"dataset": "tushare_forecast_raw",       "mode": "incremental", "frequency": "daily", "at": "20:45"},
    {"dataset": "tushare_express_raw",        "mode": "incremental", "frequency": "daily", "at": "20:50"},
    {"dataset": "tushare_fina_indicator_raw", "mode": "incremental", "frequency": "daily", "at": "21:00"},

    # ── Phase 4 — Tushare 中期数据（16:30-17:00 更新 +10min 缓冲） ──
    {"dataset": "sw_sector",             "mode": "incremental", "frequency": "daily", "at": "17:10"},
    {"dataset": "stock_moneyflow_ts",    "mode": "incremental", "frequency": "daily", "at": "17:20"},
    {"dataset": "bak_basic",             "mode": "incremental", "frequency": "daily", "at": "19:30"},

    # ── Phase 5 — 派生数据 + 筹码（等上游完成，≥30min 间隔） ────────
    {"dataset": "sector_data",           "mode": "incremental", "frequency": "daily", "at": "18:00"},
    {"dataset": "cyq_perf",              "mode": "incremental", "frequency": "daily", "at": "18:10"},

    # ── Phase 6 — 数据新鲜度检查 ────────────────────────────────────
    {"dataset": "_data_freshness_check", "mode": "incremental", "frequency": "daily", "at": "22:00"},

    # ── Phase 7 — Tushare 延迟数据（融资融券通常 18:00+ 才可用） ────
    {"dataset": "margin_detail",         "mode": "incremental", "frequency": "daily", "at": "19:00"},

    # ── Phase 8 — 自动补齐（检查+重试所有 stale/failed） ────────────
    {"dataset": "_auto_retry_stale",     "mode": "incremental", "frequency": "daily", "at": "23:00"},
]


def ensure_tushare_schedules() -> int:
    """Insert or update default Tushare schedules.

    Returns the number of rows actually inserted or updated.
    """
    load_dotenv(override=True)
    affected = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for entry in _DEFAULT_SCHEDULES:
                sid = str(uuid.uuid4())
                options_payload = {
                    k: v
                    for k, v in entry.items()
                    if k not in {"dataset", "mode", "frequency", "enabled"} and v is not None
                }
                options = json.dumps(options_payload, ensure_ascii=False)
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
