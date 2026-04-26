"""Utilities for persisting QE industry blacklist snapshots.

The stock pool blacklist is mutable global state.  QE experiment records must
store a point-in-time snapshot so detail pages can show the industries that
were actually screened for that experiment.
"""

from __future__ import annotations

import re
from copy import deepcopy
from datetime import date, datetime
from typing import Any

from ...db.pg_pool import get_conn


def _date_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _parse_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    raise ValueError(f"Unsupported date value: {value!r}")


def infer_stock_pool_date(stock_pool: str | None, fallback: date | None = None) -> date:
    """Infer the generation date from filtered_pool_YYYYMMDD names."""
    fallback_date = fallback or date.today()
    if not stock_pool:
        return fallback_date
    match = re.search(r"filtered_pool_(\d{8})", str(stock_pool))
    if not match:
        return fallback_date
    return datetime.strptime(match.group(1), "%Y%m%d").date()


def get_effective_blacklist_snapshot(query_date: date | str | None = None) -> dict[str, Any]:
    """Return active sw2 blacklist rows for a date, with serialized values."""
    target_date = _parse_date(query_date) if query_date is not None else date.today()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.sw2_code,
                    COALESCE(NULLIF(c.sw2_name, ''), MAX(NULLIF(m.l2_name, ''))) AS sw2_name,
                    COALESCE(NULLIF(c.sw1_code, ''), MAX(NULLIF(m.l1_code, ''))) AS sw1_code,
                    COALESCE(NULLIF(c.sw1_name, ''), MAX(NULLIF(m.l1_name, ''))) AS sw1_name,
                    c.status,
                    c.effective_from,
                    c.effective_to,
                    c.is_active,
                    c.reason,
                    c.updated_at,
                    c.updated_by
                FROM sw2_pool_config c
                LEFT JOIN market.sw_index_member m ON m.l2_code = c.sw2_code
                WHERE c.status = 'blocked'
                  AND c.is_active = TRUE
                  AND (c.effective_from IS NULL OR c.effective_from <= %s)
                  AND (c.effective_to IS NULL OR c.effective_to >= %s)
                GROUP BY
                    c.sw2_code, c.sw2_name, c.sw1_code, c.sw1_name,
                    c.status, c.effective_from, c.effective_to,
                    c.is_active, c.reason, c.updated_at, c.updated_by
                ORDER BY c.sw1_code, c.sw2_code
                """,
                (target_date, target_date),
            )
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    for row in rows:
        for key in ("effective_from", "effective_to", "updated_at"):
            row[key] = _date_to_iso(row.get(key))

    return {
        "enabled": bool(rows),
        "as_of_date": target_date.isoformat(),
        "count": len(rows),
        "items": rows,
        "source": "sw2_pool_config",
    }


def validate_blacklist_snapshot(snapshot: Any) -> dict[str, Any]:
    """Validate a user/backend supplied snapshot object."""
    if not isinstance(snapshot, dict):
        raise ValueError("sector_blacklist_snapshot must be a JSON object")
    items = snapshot.get("items")
    if items is None:
        items = []
    if not isinstance(items, list):
        raise ValueError("sector_blacklist_snapshot.items must be a list")
    normalized = dict(snapshot)
    normalized["items"] = items
    normalized["count"] = int(snapshot.get("count", len(items)) or 0)
    normalized["enabled"] = bool(snapshot.get("enabled", normalized["count"] > 0))
    return normalized


def attach_persistent_blacklist_snapshot(custom_params: dict[str, Any]) -> dict[str, Any]:
    """Attach a point-in-time blacklist snapshot before experiment persistence."""
    params = custom_params
    stock_pool = params.get("stock_pool")
    if not stock_pool or stock_pool == "all":
        return params

    existing = params.get("sector_blacklist_snapshot")
    if existing is not None:
        snapshot = validate_blacklist_snapshot(existing)
        snapshot.setdefault("source", "submitted_payload")
    else:
        snapshot_date = infer_stock_pool_date(str(stock_pool))
        snapshot = get_effective_blacklist_snapshot(snapshot_date)
        snapshot["source"] = "config_generate_snapshot"

    params["sector_blacklist_enabled"] = True
    params["sector_blacklist_snapshot"] = snapshot
    return params


def enrich_blacklist_snapshot_for_display(custom_params: Any) -> Any:
    """Add a display-only reconstructed snapshot when old records lack one."""
    if not isinstance(custom_params, dict):
        return custom_params
    params = deepcopy(custom_params)
    stock_pool = params.get("stock_pool")
    if not stock_pool or stock_pool == "all":
        return params

    existing = params.get("sector_blacklist_snapshot")
    if existing is not None:
        params["sector_blacklist_snapshot"] = validate_blacklist_snapshot(existing)
        params["sector_blacklist_enabled"] = True
        return params

    snapshot_date = infer_stock_pool_date(str(stock_pool))
    snapshot = get_effective_blacklist_snapshot(snapshot_date)
    snapshot["source"] = "reconstructed_from_current_sw2_pool_config"
    snapshot["warning"] = (
        "历史实验未持久化行业黑名单快照；此处按 stock_pool 日期和当前 "
        "sw2_pool_config 重建，可能与提交当时不完全一致。"
    )
    params["sector_blacklist_enabled"] = True
    params["sector_blacklist_snapshot"] = snapshot
    return params
