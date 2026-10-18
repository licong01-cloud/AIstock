"""Freeze bounded DB corrections and fill retained minute-bin holes.

The production database is the fact authority. SOURCE captures exact requested
keys in one read-only snapshot; BUILD reads only those frozen bytes and the
same candidate's daily construction fields. No supplier re-certification,
active-profile equality, full export or historical market scan is involved.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
import hashlib
import json
import re
from zoneinfo import ZoneInfo

import numpy as np

from .canonical import canonical_json_bytes
from .canonical_stock_transformer import _minute_session_times, _normalize_raw_values, _transform_raw_row
from .monthly_build_resume import MonthlyBuildResumeError, _atomic, _signature, load_sealed_build_json
from .monthly_prev_close_repair import recover_prev_close
from .qlib_bounded_update import read_calendar
from .stock_schema import QLIB_STOCK_FIELDS

SCHEMA = "aistock_monthly_database_minute_correction_v1"
BINDING = "monthly_minute_repairs"
_ZONE = ZoneInfo("Asia/Shanghai")
_SESSION = _minute_session_times()
_SESSION_SET = frozenset(_SESSION)
_FIELDS = ("trade_time", "ts_code", "open_li", "high_li", "low_li", "close_li",
           "volume_hand", "amount_li", "volume_shares", "volume_shares_source", "volume_shares_sha256")


def capture_database_minutes(inputs, *, cutoff, connection_factory):
    """Capture DB facts, not vendor responses or handwritten correction values."""
    if set(inputs) != {"stock_dates"} or not isinstance(inputs["stock_dates"], list) or not inputs["stock_dates"]:
        raise ValueError("minute corrections require explicit stock_dates")
    keys = set()
    for item in inputs["stock_dates"]:
        if not isinstance(item, dict) or set(item) != {"ts_code", "trade_date"}:
            raise ValueError("minute correction key fields differ")
        code, day = item["ts_code"], date.fromisoformat(item["trade_date"])
        if (not isinstance(code, str) or re.fullmatch(r"\d{6}\.(SH|SZ)", code) is None
                or day >= cutoff.replace(day=1) or (code, day) in keys):
            raise ValueError("historical minute correction key is duplicated or outside historical scope")
        keys.add((code, day))
    grouped = defaultdict(list)
    for code, day in sorted(keys):
        grouped[day].append(code)
    facts = []
    connection = connection_factory()
    try:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database(), txid_current_snapshot()::text")
            database, snapshot = cursor.fetchone()
            for day, codes in sorted(grouped.items()):
                cursor.execute(
                    "SELECT " + ",".join(_FIELDS) + " FROM market.kline_minute_raw "
                    "WHERE ts_code=ANY(%s::bpchar[]) AND freq='1m' "
                    "AND trade_time>=%s AND trade_time<%s ORDER BY ts_code,trade_time",
                    (codes, datetime.combine(day, datetime.min.time(), _ZONE),
                     datetime.combine(day + timedelta(days=1), datetime.min.time(), _ZONE)),
                )
                by_code = defaultdict(list)
                for values in cursor.fetchall():
                    row = dict(zip(_FIELDS, values))
                    stamp = row["trade_time"].astimezone(_ZONE)
                    if stamp.time() not in _SESSION_SET:
                        continue  # Existing canonical 240-bar session; no auction/13:00 bars.
                    _normalize_raw_values(row, source="minute", ordinal=len(facts))
                    row["trade_time"] = stamp.replace(tzinfo=None).isoformat(sep=" ")
                    for field in ("volume_shares",):
                        if row[field] is not None:
                            row[field] = float(row[field])
                    by_code[row["ts_code"]].append(row)
                for code in codes:
                    rows = by_code[code]
                    expected = [datetime.combine(day, t).isoformat(sep=" ") for t in _SESSION]
                    if [r["trade_time"] for r in rows] != expected:
                        raise ValueError(f"database minute session is incomplete or duplicated: {code}/{day}")
                    facts.extend(rows)
    finally:
        try:
            connection.rollback()
        finally:
            connection.close()
    return {"schema_version": SCHEMA, "inputs": inputs, "database": database,
            "database_snapshot": snapshot, "table": "market.kline_minute_raw",
            "units": {"price_amount": "li_1_1000_CNY", "volume_shares": "shares", "volume_hand": "100_shares"},
            "stock_dates": len(keys), "row_count": len(facts), "facts": facts,
            "captured_at": datetime.now(_ZONE).isoformat(), "database_read": True, "database_write": False}


def _pending(root):
    return root.parent / f".{root.name}.minute-correction-commit.json"


def _allowed(relative):
    return (relative == "minute_bin/materialization_receipt.json"
            or re.fullmatch(r"minute_bin/qlib/features/\d{6}\.(sz|sh)/("
                            + "|".join(QLIB_STOCK_FIELDS) + r")\.1min\.bin", relative) is not None)


def recover_minute_correction(*, root, journal, record, cas):
    return recover_prev_close(root=root, journal=journal, record=record, cas=cas,
                              schema=SCHEMA, intent_path=_pending(root), allowed_path=_allowed)


def _read_values(path):
    before = _signature(path)
    raw = path.read_bytes()
    if _signature(path) != before or len(raw) < 8 or len(raw) % 4:
        raise MonthlyBuildResumeError("minute correction input changed or has invalid dimensions")
    values = np.frombuffer(raw, dtype="<f4")
    if not np.isfinite(values[0]) or values[0] != int(values[0]) or values[0] < 0:
        raise MonthlyBuildResumeError("minute correction offset is invalid")
    return before, raw, values


def apply_minute_corrections(resume, *, finalized, cas, plan):
    binding = plan.get(BINDING)
    if binding is None:
        return finalized
    if resume.path is None or resume.root.parent.name != ".staging":
        raise MonthlyBuildResumeError("minute corrections require private retained staging")
    if (binding.get("operation_id") != resume.context.operation_id
            or binding.get("target_cutoff") != plan["target_cutoff"]
            or binding.get("predecessor_manifest_sha256") != plan["predecessor"]["dataset_manifest_sha256"]):
        raise MonthlyBuildResumeError("minute correction binding differs")
    authority = load_sealed_build_json(cas, binding["authority_ref"])
    if authority.get("schema_version") != SCHEMA:
        raise MonthlyBuildResumeError("minute correction schema differs")
    inner = dict(load_sealed_build_json(cas, finalized["finalize_receipt_ref"]))
    materialization = load_sealed_build_json(cas, inner["minute_receipt_ref"])
    if materialization.get("minute_correction") is not None:
        if materialization["minute_correction"]["authority_ref"] != binding["authority_ref"]:
            raise MonthlyBuildResumeError("minute correction authority changed")
        return finalized
    if resume.record.get("finalization_started"):
        raise MonthlyBuildResumeError("cannot amend publication after finalization")
    root = resume.root
    provider = root / "minute_bin/qlib"
    positions = {v: i for i, v in enumerate(read_calendar(provider / "calendars/1min.txt"))}
    daily = root / "daily_bin/qlib"
    day_positions = {v[:10]: i for i, v in enumerate(read_calendar(daily / "calendars/day.txt"))}
    grouped = defaultdict(list)
    for row in authority["facts"]:
        grouped[row["ts_code"].lower()].append(row)
    files, amendments, filled, checked = {}, [], 0, 0
    for code, rows in sorted(grouped.items()):
        resume.context.checkpoint()
        anchors = {}
        for field in ("factor", "up_limit_price", "down_limit_price", "prev_close"):
            path = daily / f"features/{code}/{field}.day.bin"
            _, _, values = _read_values(path)
            anchors[field] = {}
            for row in rows:
                day = row["trade_time"][:10]
                position = day_positions.get(day)
                index = -1 if position is None else position - int(values[0]) + 1
                if index <= 0 or index >= len(values):
                    raise MonthlyBuildResumeError(f"frozen daily construction field missing: {code}/{day}/{field}")
                anchors[field][day] = float(values[index])
        transformed = []
        for row in rows:
            day = row["trade_time"][:10]
            qfq = anchors["factor"][day]
            if not np.isfinite(qfq) or qfq <= 0:
                raise MonthlyBuildResumeError("frozen daily factor is invalid")
            normalized = _normalize_raw_values(row, source="minute", ordinal=len(transformed))
            result = _transform_raw_row(normalized, timestamp=datetime.fromisoformat(row["trade_time"]), qfq=qfq,
                limit={"up_limit": anchors["up_limit_price"][day], "down_limit": anchors["down_limit_price"][day],
                       "pre_close": anchors["prev_close"][day]})
            transformed.append((row["trade_time"], result))
        for field in QLIB_STOCK_FIELDS:
            path = provider / f"features/{code}/{field}.1min.bin"
            signature, raw, values = _read_values(path)
            patched, changes = values.copy(), 0
            for stamp, result in transformed:
                index = positions.get(stamp, -1) - int(values[0]) + 1
                if stamp not in positions or index <= 0 or index >= len(values):
                    raise MonthlyBuildResumeError(f"minute correction needs a bounded feature rebuild: {code}/{stamp}")
                with np.errstate(over="ignore", invalid="ignore"):
                    value = np.float32(result[field])
                if not np.isfinite(value):
                    raise MonthlyBuildResumeError("minute correction cannot be represented as float32")
                previous = values[index]
                if np.isfinite(previous) and previous != value:
                    raise MonthlyBuildResumeError(f"minute correction would overwrite a valid fact: {code}/{stamp}/{field}")
                if not np.isfinite(previous):
                    if not np.isnan(previous):
                        raise MonthlyBuildResumeError("minute correction found infinity rather than a missing cell")
                    patched[index], changes = value, changes + 1
                checked += 1
            if changes:
                relative = path.relative_to(root).as_posix()
                entry = {"original_signature": signature, "original_sha256": hashlib.sha256(raw).hexdigest(),
                         "target_ref": cas.put_bytes(patched.tobytes()).as_dict()}
                files[relative] = entry
                amendments.append({"relative_path": relative, **entry, "changed_cells": changes})
                filled += changes
    correction = {"schema_version": SCHEMA, "authority_ref": binding["authority_ref"], "files": amendments,
                  "stock_dates": authority["stock_dates"], "minute_rows": authority["row_count"],
                  "filled_cells": filled, "checked_cells": checked,
                  "construction_basis": "same_candidate_frozen_daily_float32_v1",
                  "database_read": False, "database_write": False, "existing_finite_values_unchanged": True}
    updated_materialization = {**materialization, "minute_correction": correction}
    relative = "minute_bin/materialization_receipt.json"
    path = root / relative
    signature, raw = _signature(path), path.read_bytes()
    if json.loads(raw) != materialization or _signature(path) != signature:
        raise MonthlyBuildResumeError("minute materialization receipt changed")
    files[relative] = {"original_signature": signature, "original_sha256": hashlib.sha256(raw).hexdigest(),
                      "target_ref": cas.put_bytes(canonical_json_bytes(updated_materialization) + b"\n").as_dict()}
    ref = cas.put_json(updated_materialization).as_dict()
    inner.update(minute_receipt_ref=ref, minute_materialization_receipt_file_ref=ref)
    updated = {**finalized, "finalize_receipt_ref": cas.put_json(inner).as_dict()}
    steps = {**resume.record["steps"], "finalize-bins": cas.put_json(updated).as_dict()}
    _atomic(_pending(root), {"schema_version": SCHEMA, "binding_digest": resume.record["binding_digest"],
                            "original_steps": resume.record["steps"], "target_steps": steps, "files": files})
    resume.record = recover_minute_correction(root=root, journal=resume.path, record=resume.record, cas=cas)
    return updated


def correction_pins(materialization, *, provider, cas):
    correction = materialization.get("minute_correction")
    if correction is None:
        return {}
    authority = load_sealed_build_json(cas, correction["authority_ref"])
    if authority.get("schema_version") != SCHEMA or correction.get("schema_version") != SCHEMA:
        raise MonthlyBuildResumeError("minute correction identity differs")
    result = {}
    for entry in correction["files"]:
        relative = entry["relative_path"]
        if not _allowed(relative) or relative == "minute_bin/materialization_receipt.json":
            raise MonthlyBuildResumeError("minute correction escaped feature scope")
        path = provider.parent.parent / relative
        signature, raw = _signature(path), path.read_bytes()
        ref = entry["target_ref"]
        if (len(raw) != ref["size"] or hashlib.sha256(raw).hexdigest() != ref["sha256"]
                or _signature(path) != signature):
            raise MonthlyBuildResumeError("minute correction bytes changed")
        result[path] = {"target_sha256": ref["sha256"], "target_size": ref["size"], "target_signature": signature}
    return result
