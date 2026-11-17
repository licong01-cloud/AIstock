"""Read-only integrity and recovery scope for existing LocalSIM economic facts.

An incomplete bridge is never repaired by inventing capital, trades or marks.
The sealed scope is a recovery input, not permission to change an account.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from zoneinfo import ZoneInfo

from .localsim_workbench_metrics import _bridge, _day, _money

TZ = ZoneInfo("Asia/Shanghai")


def inspect_integrity(data: dict, *, positions, is_close, now: datetime | None = None) -> dict:
    now = now or datetime.now(TZ)
    today = now.astimezone(TZ).date().isoformat()
    snapshots = sorted(data["snapshots"], key=lambda s: (_day(s["trade_date"]), str(s.get("snapshot_time", ""))))
    latest = snapshots[-1] if snapshots else None
    run_dates = {r["run_id"]: _day(r["trade_date"]) for r in data["runs"]}
    lifecycle = sorted(data.get("valuation", []), key=lambda r: (
        _day(r.get("trade_date") or run_dates.get(r["run_id"])), str(r.get("updated_at") or "")))
    pending = []
    outbox_states = {r["run_id"]: r.get("outbox_status") for r in lifecycle}
    for run in lifecycle:
        value = run.get("pending") or {}
        if value.get("status") != "WAITING_FOR_AUTHORITATIVE_MARKS":
            continue
        error = value.get("valuation_error") or {}
        pending.append({"run_id": run["run_id"], "trade_date": _day(run.get("trade_date") or run_dates.get(run["run_id"])),
            "status": run["status"], "symbols": list(value.get("missing_symbols") or []),
            "reason_code": error.get("reason_code") or (error.get("context") or {}).get("reason_code"),
            "cause": (error.get("context") or {}).get("cause"), "last_attempt_at": value.get("last_attempt_at")})
    # Completed projections can retain historical pending evidence. Current
    # completion always takes precedence; do not revive a resolved warning.
    pending = [p for p in pending if outbox_states[p["run_id"]] != "PROJECTED"]
    indexed = defaultdict(list)
    for row in data["positions"]:
        indexed[(row["run_id"], _day(row["trade_date"]))].append(row)
    def at_snapshot(snapshot):
        rows = indexed.get((snapshot["run_id"], _day(snapshot["trade_date"])), [])
        return positions({**data, "positions": rows}, snapshot)
    closes = {_day(s["trade_date"]) for s in snapshots if is_close(s) and at_snapshot(s)[1]}
    missing = sorted({_day(d["cal_date"]) for d in data.get("calendar", [])
                      if _day(d["cal_date"]) < today and _day(d["cal_date"]) not in closes})
    issues = []
    residual = None
    mismatches = []
    if latest:
        finish = _day(latest["trade_date"])
        closing, valid = at_snapshot(latest)
        origin = data.get("capital_origin") or {}
        initial = _money(data["account"]["initial_capital"])
        capital = initial if data["native"] or (origin.get("initial_cash") is not None and _money(origin["initial_cash"]) == initial) else None
        if valid:
            _, _, reason = _bridge(data, latest, closing, capital=capital)
            if reason:
                issues.append({"code": "ECONOMIC_BRIDGE_UNVERIFIED", "detail": reason})
            if capital is not None:
                flows = [c for c in data["cash"] if _day(c["trade_date"]) <= finish]
                residual = _money(latest["cash"])-capital-sum((_money(c["cash_delta"]) for c in flows), Decimal(0))
            expected = defaultdict(int)
            for fill in data["fills"]:
                if fill["run_id"] in run_dates and run_dates[fill["run_id"]] <= finish and fill["side"] in {"BUY", "SELL"}:
                    expected[fill["symbol"]] += fill["quantity"] * (1 if fill["side"] == "BUY" else -1)
            mismatches = [{"symbol": s, "fills_quantity": expected[s], "snapshot_quantity": int(closing.get(s, {}).get("quantity", 0))}
                for s in sorted(set(expected) | set(closing)) if expected[s] != int(closing.get(s, {}).get("quantity", 0))]
        else:
            issues.append({"code": "SNAPSHOT_POSITIONS_UNVERIFIED", "detail": "持仓集合与估值未对齐"})
    if missing:
        issues.append({"code": "MISSING_AUTHORITATIVE_CLOSE", "detail": "历史交易日缺少已核验收盘估值"})
    if pending:
        issues.append({"code": "AUTHORITATIVE_MARKS_PENDING", "detail": "已提交经济事实仍等待权威估值发布"})
    latest_run = lifecycle[-1] if lifecycle else (max(data["runs"], key=lambda r: _day(r["trade_date"])) if data["runs"] else None)
    return {"schema_version": "localsim_integrity_inspection_v1", "account_id": data["account"]["account_id"],
        "valuation_day": _day(latest["trade_date"]) if latest else None, "cash_bridge_residual": str(residual) if residual is not None else None,
        "inventory_mismatch_count": len(mismatches), "inventory_mismatches": mismatches,
        "missing_close_dates": missing, "pending_valuations": pending,
        "latest_run_status": latest_run.get("status") if latest_run else None,
        "latest_run_date": _day(latest_run["trade_date"]) if latest_run and latest_run.get("trade_date") else None,
        "issues": issues, "read_only": True, "economic_recomputation_performed": False}


def prepare_recovery_scope(data: dict, *, positions, is_close, now: datetime | None = None) -> dict:
    """Seal exact current facts before choosing any target-specific correction."""
    report = inspect_integrity(data, positions=positions, is_close=is_close, now=now)
    raw = json.dumps(data, sort_keys=True, default=str, separators=(",", ":")).encode()
    body = {"schema_version": "localsim_integrity_recovery_scope_v1", "account_id": report["account_id"],
        "ledger_scope": data.get("scope"), "source_facts_sha256": hashlib.sha256(raw).hexdigest(),
        "inspection": report, "new_capital_added": "0", "economic_replay_allowed": False,
        "production_write_authorized": False,
        "required_evidence": ["exact durable economic generation", "authoritative historical price evidence",
                              "DEV validation receipt", "target-specific production authorization"]}
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return body | {"identity": hashlib.sha256(encoded).hexdigest()}
