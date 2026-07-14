"""Idempotent initializer for default Tushare ingestion schedules.

Inserts/updates schedule rows into market.ingestion_schedules.
Safe to run multiple times — uses ON CONFLICT DO UPDATE to refresh times.
"""
from __future__ import annotations

import hashlib
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

    # ── Phase 2a — 周末补偿检查（周六10:00触发）────────────────
    {"dataset": "_weekend_compensation", "mode": "incremental", "frequency": "weekly",
     "day_of_week": "saturday", "at": "10:00"},

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

DEFAULT_SCHEDULE_CATALOG_VERSION = "tushare-defaults-v1"
DEFAULT_SCHEDULE_CATALOG_FINGERPRINT = "5b5e3aec97d4c833a67b7351b3f0d5284e80aaef4f102c9101f314e48c33dfc3"


def get_default_schedule_templates() -> List[Dict[str, Any]]:
    """Return the canonical schedule catalog in ingestion API shape.

    Callers receive fresh dictionaries so planning and API serialization cannot
    mutate the initializer's process-global defaults.
    """
    templates: List[Dict[str, Any]] = []
    for entry in _DEFAULT_SCHEDULES:
        options = {
            key: value
            for key, value in entry.items()
            if key not in {"dataset", "mode", "frequency", "enabled"} and value is not None
        }
        templates.append(
            {
                "dataset": entry["dataset"],
                "mode": entry["mode"],
                "frequency": entry["frequency"],
                "enabled": entry.get("enabled", True),
                "options": options,
            }
        )
    return templates


def get_default_schedule_catalog() -> Dict[str, Any]:
    """Return a validated, fingerprinted view of the canonical defaults."""
    templates = get_default_schedule_templates()
    errors: List[str] = []
    keys: List[tuple[str, str]] = []
    for index, template in enumerate(templates):
        missing = [field for field in ("dataset", "mode", "frequency", "enabled", "options") if field not in template]
        if missing:
            errors.append(f"entry[{index}] missing fields: {','.join(missing)}")
            continue
        key = (str(template["dataset"]), str(template["mode"]))
        if key in keys:
            errors.append(f"duplicate schedule key: {key[0]}/{key[1]}")
        keys.append(key)
        if template["frequency"] == "daily" and not template["options"].get("at"):
            errors.append(f"daily schedule missing fixed time: {key[0]}/{key[1]}")
    fingerprint = hashlib.sha256(
        json.dumps(templates, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if fingerprint != DEFAULT_SCHEDULE_CATALOG_FINGERPRINT:
        errors.append("catalog fingerprint does not match the reviewed canonical manifest")
    return {
        "version": DEFAULT_SCHEDULE_CATALOG_VERSION,
        "fingerprint": fingerprint,
        "complete": not errors and bool(templates),
        "errors": errors,
        "templates": templates,
    }
_MODE_INSENSITIVE_DEFAULT_DATASETS = frozenset({"stock_basic"})


def _validate_default_schedules(entries: List[Dict[str, Any]]) -> None:
    """Fail before opening a DB connection when canonical defaults conflict."""
    exact_keys: set[tuple[str, str]] = set()
    mode_insensitive_seen: set[str] = set()
    for entry in entries:
        dataset = str(entry.get("dataset") or "").strip().lower()
        mode = str(entry.get("mode") or "incremental").strip().lower()
        if not dataset:
            raise ValueError("default schedule dataset is required")
        key = (dataset, mode)
        if key in exact_keys:
            raise ValueError(f"duplicate default schedule: {dataset}/{mode}")
        exact_keys.add(key)
        if dataset in _MODE_INSENSITIVE_DEFAULT_DATASETS:
            if dataset in mode_insensitive_seen:
                raise ValueError(f"mode-insensitive default dataset has multiple schedules: {dataset}")
            mode_insensitive_seen.add(dataset)


def ensure_tushare_schedules() -> int:
    """Insert or update default Tushare schedules.

    Returns the number of rows actually inserted or updated.
    """
    _validate_default_schedules(_DEFAULT_SCHEDULES)
    load_dotenv(override=True)
    affected = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for template in get_default_schedule_templates():
                sid = str(uuid.uuid4())
                options = json.dumps(template["options"], ensure_ascii=False)
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
                    (sid, template["dataset"], template["mode"],
                     template["frequency"], template["enabled"], options),
                )
                if cur.rowcount > 0:
                    affected += 1
    return affected


if __name__ == "__main__":
    n = ensure_tushare_schedules()
    print(f"[DONE] upserted {n} tushare schedule(s)")
