"""Apply approved exact reference-price corrections without rerunning exports.

This is a bounded, explicit amendment to a sealed SOURCE, not a database
fallback or a historical quality scan. Raw CNY prev_close is never QFQ-scaled.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import copy
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re

import numpy as np

from .canonical import canonical_json_bytes
from .monthly_build_resume import MonthlyBuildResumeError, _atomic, _plain, _signature, load_sealed_build_json
from .monthly_quote_catalog import _replace
from .qlib_bounded_update import read_calendar

SCHEMA = "aistock_monthly_prev_close_correction_v1"
BINDING = "monthly_prev_close_repairs"
SUPPLEMENT_KEYS = {BINDING, BINDING + "_ref", "monthly_industry_history", "monthly_industry_history_ref",
                   "monthly_industry_quote_source", "monthly_industry_quote_source_ref"}


def capture_approved_corrections(inputs, *, repair_root: Path, cutoff: date):
    """Read only exact provider plans closed by an actual production readback."""
    if set(inputs) != {"plan_path", "plan_sha256", "receipt_path", "receipt_sha256"}:
        raise ValueError("prev_close repair input fields differ")

    def pinned(raw_path, digest):
        path = Path(raw_path)
        if not path.is_absolute() or not path.is_relative_to(repair_root):
            raise ValueError("prev_close source escaped registered repair root")
        _plain(path)
        before = _signature(path)
        if before[2] > 32 * 1024 * 1024:
            raise ValueError("prev_close source plan exceeds bounded size")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != digest or _signature(path) != before:
            raise ValueError("prev_close source changed")
        return json.loads(raw)

    plan = pinned(inputs["plan_path"], inputs["plan_sha256"])
    receipt = pinned(inputs["receipt_path"], inputs["receipt_sha256"])
    if (plan.get("schema_version") != "local_data_stk_limit_history_production_plan_v1"
            or plan.get("target_database") != "aistock" or plan.get("target_table") != "market.stk_limit"
            or plan.get("changed_columns") != ["pre_close"]
            or receipt.get("schema_version") != "local_data_stk_limit_history_production_apply_v1"
            or receipt.get("plan_sha256") != inputs["plan_sha256"]
            or receipt.get("production_apply") != "PASS"
            or receipt.get("production_readback") != "PASS_INDEPENDENT_FRESH_CONNECTION"
            or receipt.get("non_target_columns_unchanged") is not True):
        raise ValueError("prev_close production correction is not closed")
    facts, sources, seen = [], [], set()
    for entry in plan["daily"]:
        source = pinned(entry["source_plan_path"], entry["source_plan_sha256"])
        source_hash = hashlib.sha256(canonical_json_bytes(source["source_rows"])).hexdigest()
        if (source.get("schema_version") != "local_data_stk_limit_pre_close_repair_v1"
                or source.get("provider") != "tushare.stk_limit" or source.get("units") != "CNY_per_share"
                or source_hash != source.get("source_rows_sha256") or source_hash != entry["source_rows_sha256"]
                or source.get("trade_date") != entry["trade_date"] or len(source["patches"]) != entry["keys"]):
            raise ValueError("prev_close provider source identity differs")
        by_key = {(r["ts_code"], r["trade_date"]): r for r in source["source_rows"]}
        if len(by_key) != len(source["source_rows"]):
            raise ValueError("prev_close source keys are duplicated")
        for row in source["patches"]:
            day = datetime.strptime(row["trade_date"], "%Y%m%d").date()
            key = row["ts_code"], row["trade_date"]
            value = float(row["pre_close"])
            if (key in seen or by_key.get(key) != row or day.isoformat() != entry["trade_date"] or day > cutoff
                    or re.fullmatch(r"\d{6}\.(SZ|SH|BJ)", row["ts_code"]) is None
                    or not np.isfinite(value) or value <= 0):
                raise ValueError("prev_close patch is not an exact finite provider fact")
            with np.errstate(over="ignore", invalid="ignore"):
                stored = np.float32(value)
            if not np.isfinite(stored) or stored <= 0:
                raise ValueError("prev_close fact cannot be represented in Qlib precision")
            seen.add(key)
            facts.append({**row, "trade_date": day.isoformat()})
        sources.append({"sha256": entry["source_plan_sha256"], "source_rows_sha256": source_hash,
                        "trade_date": entry["trade_date"], "keys": entry["keys"],
                        "captured_at": source.get("captured_at"), "historical_publication_time_proven": False})
    if not facts or len(facts) != receipt.get("verified_keys"):
        raise ValueError("prev_close correction key count differs from production readback")
    return {"schema_version": SCHEMA, "inputs": dict(inputs), "sources": sources, "facts": facts,
            "provider": "tushare.stk_limit", "units": "CNY_per_share", "database_write": False}


def _pending(root):
    return root.parent / f".{root.name}.prev-close-commit.json"


def _allowed(relative):
    return (relative in {f"{d}/materialization_receipt.json" for d in ("daily_bin", "minute_bin")}
            or re.fullmatch(r"daily_bin/qlib/features/\d{6}\.(sz|sh)/prev_close\.day\.bin|minute_bin/qlib/features/\d{6}\.(sz|sh)/prev_close\.1min\.bin", relative) is not None)


def recover_prev_close(*, root, journal, record, cas):
    intent = _pending(root)
    _plain(intent)
    if not intent.exists():
        return record
    if intent.stat().st_size > 8 * 1024 * 1024:
        raise MonthlyBuildResumeError("prev_close commit intent is oversized")
    commit = json.loads(intent.read_bytes())
    if (commit.get("schema_version") != SCHEMA or commit.get("binding_digest") != record["binding_digest"]
            or record["steps"] not in (commit["original_steps"], commit["target_steps"])
            or not commit.get("files") or not all(_allowed(p) for p in commit["files"])):
        raise MonthlyBuildResumeError("prev_close commit identity differs")
    payloads = {}
    for relative, entry in commit["files"].items():
        path = root / relative
        signature = _signature(path)
        raw = path.read_bytes()
        target = cas.get_bytes(cas.validate_reference_metadata(entry["target_ref"]))
        digest = hashlib.sha256(raw).hexdigest()
        if digest != entry["target_ref"]["sha256"] and (
                digest != entry["original_sha256"] or list(signature) != entry["original_signature"]):
            raise MonthlyBuildResumeError("prev_close commit input changed")
        if _signature(path) != signature:
            raise MonthlyBuildResumeError("prev_close input changed during read")
        payloads[relative] = target
    result = copy.deepcopy(record)
    for relative, payload in payloads.items():
        path = root / relative
        raw = path.read_bytes()
        entry = commit["files"][relative]
        if raw != payload:
            if (hashlib.sha256(raw).hexdigest() != entry["original_sha256"]
                    or list(_signature(path)) != entry["original_signature"]):
                raise MonthlyBuildResumeError("prev_close target changed before replacement")
            _replace(path, payload)
        result["files"][relative] = list(_signature(path))
    result["steps"] = commit["target_steps"]
    _atomic(journal, result)
    intent.unlink()  # Completed transaction intent only; exports are retained.
    return result


def apply_prev_close_corrections(resume, *, finalized, cas, plan):
    binding = plan.get(BINDING)
    if binding is None:
        return finalized
    if resume.path is None or resume.root.parent.name != ".staging":
        raise MonthlyBuildResumeError("prev_close amendment requires private retained staging")
    authority = load_sealed_build_json(cas, binding["authority_ref"])
    if (authority.get("schema_version") != SCHEMA or binding.get("target_cutoff") != plan["target_cutoff"]
            or binding.get("operation_id") != resume.context.operation_id
            or binding.get("predecessor_manifest_sha256") != plan["predecessor"]["dataset_manifest_sha256"]):
        raise MonthlyBuildResumeError("prev_close authority schema differs")
    root, pending, files = resume.root, _pending(resume.root), {}
    inner = load_sealed_build_json(cas, finalized["finalize_receipt_ref"])
    if not any(key in inner for key in ("daily_receipt_ref", "minute_receipt_ref")):
        raise MonthlyBuildResumeError("prev_close correction has no native completion receipt")
    by_code = defaultdict(list)
    for row in authority["facts"]:
        if row["ts_code"].endswith((".SZ", ".SH")):
            by_code[row["ts_code"].lower()].append(row)
    for dataset, freq in (("daily_bin", "day"), ("minute_bin", "1min")):
        prefix = dataset.removesuffix("_bin")
        if f"{prefix}_receipt_ref" not in inner:
            continue
        materialization = load_sealed_build_json(cas, inner[f"{prefix}_receipt_ref"])
        if materialization.get("prev_close_correction") is not None:
            if materialization["prev_close_correction"]["authority_ref"] != binding["authority_ref"]:
                raise MonthlyBuildResumeError("prev_close correction authority changed")
            continue
        if resume.record.get("finalization_started") is True:
            raise MonthlyBuildResumeError("prev_close correction cannot amend publication already started")
        counts, amendments = Counter(), []
        provider = root / dataset / "qlib"
        calendar = read_calendar(provider / f"calendars/{freq}.txt")
        positions = defaultdict(list)
        for i, stamp in enumerate(calendar):
            positions[stamp[:10]].append(i)
        for code, rows in sorted(by_code.items()):
            resume.context.checkpoint()
            path = provider / f"features/{code}/prev_close.{freq}.bin"
            if not path.exists():
                counts["no_exported_feature"] += len(rows)
                continue
            signature = _signature(path)
            values = np.memmap(path, dtype="<f4", mode="r")
            if len(values) < 2 or not np.isfinite(values[0]) or values[0] != int(values[0]):
                raise MonthlyBuildResumeError("prev_close feature offset is invalid")
            offset, changes = int(values[0]), []
            close_path = path.with_name(f"close.{freq}.bin")
            close_signature = _signature(close_path)
            close = np.memmap(close_path, dtype="<f4", mode="r")
            if len(close) != len(values) or close[0] != values[0]:
                raise MonthlyBuildResumeError("prev_close and quote dimensions differ")
            for row in rows:
                indices = np.array(positions.get(row["trade_date"], []), dtype=np.int64) - offset + 1
                indices = indices[(indices > 0) & (indices < len(values))]
                indices = indices[np.isfinite(close[indices]) & (close[indices] > 0)]
                if not len(indices):
                    counts["no_traded_quote"] += 1
                    continue
                value = np.float32(float(row["pre_close"]))
                changed = indices[values[indices] != value]
                if len(changed):
                    changes.extend((int(index), value) for index in changed)
                    counts["repaired_stock_dates"] += 1
                else:
                    counts["already_exact"] += 1
            del values, close
            if _signature(path) != signature or _signature(close_path) != close_signature:
                raise MonthlyBuildResumeError("prev_close or quote changed during correction")
            if not changes:
                continue
            raw = path.read_bytes()
            patched = np.frombuffer(raw, dtype="<f4").copy()
            for index, value in changes:
                patched[index] = value
            payload = patched.tobytes()
            relative = path.relative_to(root).as_posix()
            target_ref = cas.put_bytes(payload).as_dict()
            entry = {"original_signature": list(signature), "original_sha256": hashlib.sha256(raw).hexdigest(),
                     "target_ref": target_ref}
            files[relative] = entry
            amendments.append({"relative_path": relative, **entry, "changed_cells": len(changes)})
        correction = {"schema_version": SCHEMA, "authority_ref": binding["authority_ref"],
                      "counts": dict(counts), "files": amendments, "only_field": "prev_close",
                      "historical_business_audit_performed": False, "database_read": False,
                      "database_write": False, "active_profile_write": False}
        updated = {**materialization, "prev_close_correction": correction}
        relative = f"{dataset}/materialization_receipt.json"
        path = root / relative
        raw = path.read_bytes()
        if json.loads(raw) != materialization:
            raise MonthlyBuildResumeError("prev_close materialization provenance differs")
        ref = cas.put_json(updated).as_dict()
        files[relative] = {"original_signature": list(_signature(path)), "original_sha256": hashlib.sha256(raw).hexdigest(),
                           "target_ref": cas.put_bytes(canonical_json_bytes(updated) + b"\n").as_dict()}
        inner[f"{prefix}_receipt_ref"] = ref
        inner[f"{prefix}_materialization_receipt_file_ref"] = ref
    if not files:
        return finalized
    updated = {**finalized, "finalize_receipt_ref": cas.put_json(inner).as_dict()}
    steps = {**resume.record["steps"], "finalize-bins": cas.put_json(updated).as_dict()}
    _atomic(pending, {"schema_version": SCHEMA, "binding_digest": resume.record["binding_digest"],
                     "original_steps": resume.record["steps"], "target_steps": steps, "files": files})
    resume.record = recover_prev_close(root=root, journal=resume.path, record=resume.record, cas=cas)
    return updated


def correction_pins(materialization, *, provider, cas):
    """Verify amended files and return pins; original append proof stays intact."""
    correction = materialization.get("prev_close_correction")
    if correction is None:
        return {}, {}
    authority = load_sealed_build_json(cas, correction["authority_ref"])
    if authority.get("schema_version") != SCHEMA or correction.get("only_field") != "prev_close":
        raise MonthlyBuildResumeError("prev_close validation authority differs")
    result = {}
    for entry in correction["files"]:
        relative = entry["relative_path"]
        if not _allowed(relative) or not relative.startswith(materialization["dataset"] + "/qlib/features/"):
            raise MonthlyBuildResumeError("prev_close correction escaped its field")
        path = provider.parent.parent / relative
        before = _signature(path)
        raw = path.read_bytes()
        ref = entry["target_ref"]
        if hashlib.sha256(raw).hexdigest() != ref["sha256"] or len(raw) != ref["size"] or _signature(path) != before:
            raise MonthlyBuildResumeError("prev_close corrected bytes differ")
        result[path] = {"target_sha256": ref["sha256"], "target_size": ref["size"], "target_signature": list(before)}
    month_start = materialization["cutoff"][:7] + "-01"
    overrides = {(r["ts_code"].lower(), r["trade_date"]): float(r["pre_close"])
                 for r in authority["facts"] if month_start <= r["trade_date"] <= materialization["cutoff"]}
    return result, overrides
