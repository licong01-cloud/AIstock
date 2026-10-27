"""Published membership, change and execution progress are separate facts."""
from __future__ import annotations

from collections import Counter
from datetime import date
import hashlib
import json
from typing import Any


class WorkbenchError(ValueError):
    def __init__(self, code: str, message: str, status: int = 422):
        super().__init__(message)
        self.code, self.status = code, status


def identity(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, ensure_ascii=False).encode()).hexdigest()


def membership(row: dict) -> str:
    action, reason = row.get("action"), row.get("reason_code")
    if action in {"ENTER", "HOLD"} and row.get("item_state") == "ACTIVE":
        return "IN_SCOPE"
    if action == "WATCH" and reason == "PENDING_TARGET_OPEN_ENTRY":
        return "IN_SCOPE"
    if action == "WATCH" and reason == "OUTSIDE_ENTRY_THRESHOLD":
        return "OBSERVATION"
    if action == "WAITING" and reason == "PENDING_TARGET_OPEN_REVIEW":
        return "OUTSIDE_PENDING"
    if action == "EXIT":
        return "OUTSIDE_EXIT"
    return "UNKNOWN"


def daily_projection(items, previous, *, prior_known: bool, streaks=None):
    counts = Counter(row["symbol"] for row in items)
    prev_counts = Counter(row["symbol"] for row in previous)
    current = {row["symbol"] for row in items if membership(row) == "IN_SCOPE" and counts[row["symbol"]] == 1}
    prior = {row["symbol"] for row in previous if membership(row) == "IN_SCOPE" and prev_counts[row["symbol"]] == 1}
    prior_known = prior_known and all(membership(row) != "UNKNOWN" and prev_counts[row["symbol"]] == 1 for row in previous)
    prior_known = prior_known and all(membership(row) != "UNKNOWN" and counts[row["symbol"]] == 1 for row in items)
    rows = []
    for original in items:
        row = dict(original)
        symbol = row["symbol"]
        group = membership(row) if counts[symbol] == 1 else "UNKNOWN"
        change = "UNKNOWN"
        if prior_known and group == "IN_SCOPE":
            change = "CONTINUING" if symbol in prior else "NEW"
        elif prior_known and symbol in prior and group != "UNKNOWN":
            change = "REMOVED"
        progress = "WAITING_REVIEW"
        if row.get("action") == "ENTER" or row.get("reason_code") == "PENDING_TARGET_OPEN_ENTRY":
            progress = "PENDING_ENTRY"
        if row.get("action") == "HOLD":
            progress = "TRACKING"
        if row.get("action") == "EXIT":
            progress = "PENDING_EXIT"
        if group == "OBSERVATION":
            progress = "OBSERVATION"
        if group == "UNKNOWN":
            progress = "UNKNOWN"
        row.update(membership=group, change=change, progress=progress,
                   consecutive_days=(streaks or {}).get(symbol),
                   membership_reason="DUPLICATE_SYMBOL" if counts[symbol] > 1 else None)
        rows.append(row)
    present = {row["symbol"] for row in rows}
    if prior_known:
        for row in previous:
            if row["symbol"] in prior and row["symbol"] not in present:
                rows.append({"symbol": row["symbol"], "stock_name": row.get("stock_name"),
                    "episode_id": row.get("episode_id"), "rank": None, "previous_rank": row.get("rank"),
                    "membership": "OUTSIDE_PENDING", "change": "REMOVED", "progress": "WAITING_REVIEW",
                    "reason_code": "REMOVAL_WITHOUT_EXIT_DECISION", "consecutive_days": None})
    summary = {"in_scope": len(current), "new": sum(r["change"] == "NEW" for r in rows),
               "continuing": sum(r["change"] == "CONTINUING" for r in rows),
               "removed": sum(r["change"] == "REMOVED" for r in rows),
               "unknown_change": sum(r["membership"] == "IN_SCOPE" and r["change"] == "UNKNOWN" for r in rows),
               "observation": sum(r["membership"] == "OBSERVATION" for r in rows),
               "outside_pending": sum(r["membership"] == "OUTSIDE_PENDING" for r in rows),
               "outside_exit": sum(r["membership"] == "OUTSIDE_EXIT" for r in rows),
               "unknown": sum(r["membership"] == "UNKNOWN" for r in rows),
               "previous_count": len(prior) if prior_known else None, "comparison_known": prior_known}
    return rows, summary


def run_state(run, *, has_target: bool, configured: str):
    if not run:
        return {"state": "PUBLISHED" if has_target else "NO_CURRENT_LIST", "reason": None, "settlement": None}
    error = run.get("last_error_json") or {}
    reason = (error.get("context") or {}).get("reason_code") or run.get("last_reason_code")
    published = has_target and run.get("publication_status") == "PUBLISHED"
    if published:
        state = "PUBLISHED"
    elif reason and ("asset" in reason.lower() or "seed" in reason.lower()):
        state = "ASSET_BLOCKED"
    elif run.get("publication_status") == "WAITING_DATA":
        state = "WAITING_DATA"
    elif run.get("publication_status") == "FAILED":
        state = "FAILED"
    else:
        state = "NO_CURRENT_LIST"
    return {"state": state, "reason": reason, "settlement": run.get("settlement_status"),
            "data_date": run.get("decision_as_of_trade_date"),
            "target_date": run.get("target_trade_date"), "stage": run.get("last_stage"),
            "configured": configured, "attempt_count": run.get("attempt_count"), "updated_at": run.get("updated_at")}


def calendar_context(calendar, today: date):
    by_day = {str(row["cal_date"]): row["is_trading"] for row in calendar}
    future = sorted(day for day, trading in by_day.items() if trading and day >= today.isoformat())
    if today.isoformat() not in by_day or not future:
        raise WorkbenchError("CALENDAR_UNAVAILABLE", "正式交易日历未覆盖当前日期，请核对数据管理。", 503)
    return future[0]


def coverage(versions, calendar, target):
    published = [v for v in versions if v["version_status"] == "PUBLISHED"]
    dates = {str(v["trade_date"]) for v in published}
    first = min(dates) if dates else None
    calendar_map = {str(row["cal_date"]): row["is_trading"] for row in calendar}
    result = {"first_published_target_date": first, "through": target, "missing_dates": [], "status": "NO_LISTS"}
    if not first or first > target:
        return result
    days = (date.fromisoformat(target) - date.fromisoformat(first)).days + 1
    known = [d for d in calendar_map if first <= d <= target]
    if len(known) != days:
        return {**result, "status": "CALENDAR_INCOMPLETE"}
    missing = sorted(d for d in known if calendar_map[d] and d not in dates)
    return {**result, "status": "COMPLETE" if not missing else "GAPS", "missing_dates": missing, "missing_count": len(missing)}


def streak_counts(selected, versions, items_by_version, calendar):
    """Return proven consecutive counts; a missing prior trading day is unknown."""
    if coverage(versions, calendar, str(selected["trade_date"]))["status"] == "CALENDAR_INCOMPLETE":
        return {row["symbol"]: None for row in items_by_version.get(selected["list_version_id"], [])}
    trading = sorted(str(r["cal_date"]) for r in calendar if r["is_trading"] and str(r["cal_date"]) <= str(selected["trade_date"]))
    valid = [v for v in versions if v["version_status"] == "PUBLISHED" and v["binding_version_id"] == selected["binding_version_id"]]
    by_date = {}
    for version in valid:
        by_date.setdefault(str(version["trade_date"]), version)
    first = min(by_date) if by_date else None
    result = {}
    for row in items_by_version.get(selected["list_version_id"], []):
        if membership(row) != "IN_SCOPE":
            continue
        count, known = 0, True
        for day in reversed(trading):
            if day < first:
                break
            version = by_date.get(day)
            if version is None:
                known = False
                break
            matched = [item for item in items_by_version.get(version["list_version_id"], []) if item["symbol"] == row["symbol"]]
            if any(membership(item) == "UNKNOWN" for item in matched) or len(matched) > 1:
                known = False
                break
            if not matched or membership(matched[0]) != "IN_SCOPE":
                break
            count += 1
            if day == first and (version.get("previous_list_version_id") or any(
                    str(v["trade_date"]) < first for v in versions)):
                known = False
                break
        result[row["symbol"]] = count if known and count else None
    return result
