"""Adopt bounded, missing-only quote outputs without rerunning an exporter.

Only private staging is writable. Frozen source captures and the actual old
manifest stay bound; this neither grants readiness nor accesses a database.
"""
from __future__ import annotations

import copy
from collections import defaultdict
import hashlib
import json
import re

import numpy as np

from .canonical import canonical_json_bytes
from .monthly_build_resume import _atomic, _plain, _signature, load_sealed_build_json
from .stock_schema import QLIB_STOCK_FIELDS

BINDING = "monthly_quote_fact_repairs"
SCHEMA = "aistock_monthly_bounded_quote_correction_v1"
PRODUCER_SCHEMA = "aistock_private_bounded_quote_repair_v1"
SMALL_FILES = {"daily_bin/materialization_receipt.json", "minute_bin/materialization_receipt.json",
               "daily_bin/qlib/instruments/all.txt"}


def allowed(relative):
    return re.fullmatch(r"(daily_bin|minute_bin)/qlib/features/\d{6}\.(sz|sh)/("
        + "|".join(QLIB_STOCK_FIELDS) + r")\.(day|1min)\.bin", relative) is not None and (
            relative.startswith("daily_bin/") == relative.endswith(".day.bin"))


def _array(raw):
    if len(raw) < 8 or len(raw) % 4:
        raise ValueError("quote repair feature dimensions are invalid")
    values = np.frombuffer(raw, dtype="<f4")
    if not np.isfinite(values[0]) or values[0] < 0 or values[0] != int(values[0]) or np.isinf(values[1:]).any():
        raise ValueError("quote repair feature offset or values are invalid")
    return values


def validate_missing_only(original, target):
    """Compare just one changed file; never revalidate unaffected history."""
    after = _array(target)
    added = int(np.isfinite(after[1:]).sum())
    if original is not None:
        before = _array(original)
        left = int(before[0]) - int(after[0]) + 1
        if left < 1 or left + len(before) - 1 > len(after):
            raise ValueError("quote repair truncated existing observations")
        observed = after[left:left + len(before) - 1]
        finite = np.isfinite(before[1:])
        if not np.array_equal(before[1:][finite], observed[finite]):
            raise ValueError("quote repair would overwrite an existing finite fact")
        added -= int(finite.sum())
    if added <= 0:
        raise ValueError("quote repair has no missing facts to fill")
    return added


def capture_quote_repairs(inputs, *, repair_root, base_manifest_ref, cutoff, cas):
    from .monthly_fact_correction import _under
    if set(inputs) != {"receipt_path", "receipt_sha256"}:
        raise ValueError("quote repair requires an existing producer receipt")
    path = _under(inputs["receipt_path"], repair_root)
    signature = _signature(path)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != inputs["receipt_sha256"] or _signature(path) != signature:
        raise ValueError("quote repair receipt changed")
    receipt = json.loads(raw)
    if (receipt.get("schema_version") != PRODUCER_SCHEMA or receipt.get("status") != "PRIVATE_COMPONENTS_NOT_A_RELEASE"
            or receipt.get("base_manifest_file_sha256") != base_manifest_ref["sha256"]
            or receipt.get("cutoff") != cutoff
            or re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("daily_pv_sha256"))) is None
            or any(receipt.get(k) is not False for k in ("database_write", "profile_write", "runtime_action"))
            or not receipt.get("source_captures") or not receipt.get("files")):
        raise ValueError("quote repair is not bound to the existing private producer")
    sources = []
    for item in receipt["source_captures"]:
        source = _under(item["path"], repair_root)
        before = _signature(source)
        with source.open("rb") as reader:
            ref = cas.put_stream(iter(lambda: reader.read(1024 * 1024), b""))
        if ref.sha256 != item["sha256"] or ref.size != item["size"] or _signature(source) != before:
            raise ValueError("quote repair frozen source capture changed")
        sources.append(ref.as_dict())
    files, seen = [], set()
    for row in receipt["files"]:
        relative = row["relative_path"]
        if not allowed(relative) or relative in seen:
            raise ValueError("quote repair file escaped its component or is duplicated")
        seen.add(relative)
        source = _under(row["source_path"], repair_root)
        before = _signature(source)
        raw = source.read_bytes()  # One feature, not an all-market frame.
        if hashlib.sha256(raw).hexdigest() != row["sha256"] or _signature(source) != before:
            raise ValueError("quote repair output changed")
        _array(raw)
        files.append({"relative_path": relative, "original_sha256": row["original_sha256"],
            "target_ref": cas.put_bytes(raw).as_dict()})
    return {"schema_version": SCHEMA, "producer_receipt": inputs,
        "base_manifest_ref": dict(base_manifest_ref), "source_capture_refs": sources, "files": files,
        "daily_pv_sha256": receipt["daily_pv_sha256"],
        "coverage": receipt["coverage"], "database_write": False}


def _pending(root):
    return root.parent / f".{root.name}.quote-fact-commit.json"


def recover_quote_repairs(*, root, journal, record, cas):
    from .monthly_prev_close_repair import recover_prev_close
    path = _pending(root)
    if path.exists():
        commit = json.loads(path.read_bytes())
        if commit.get('schema_version') != SCHEMA or commit.get('binding_digest') != record['binding_digest']:
            raise ValueError('quote repair commit identity differs')
        # A completed journal can also include later finalization receipts.
        completed = record["steps"].get("quote-facts")
        if completed is not None and completed == commit["target_steps"].get("quote-facts"):
            for relative, entry in commit["files"].items():
                if not (allowed(relative) or relative in SMALL_FILES):
                    raise ValueError("quote repair commit escaped its component")
                target = root / relative
                _plain(target)
                if (hashlib.sha256(target.read_bytes()).hexdigest() != entry["target_ref"]["sha256"]
                        or list(_signature(target)) != record['files'].get(relative)):
                    raise ValueError("completed quote repair output changed")
            path.unlink()
            return record
    return recover_prev_close(root=root, journal=journal, record=record, cas=cas, schema=SCHEMA,
        intent_path=path, allowed_path=lambda r: allowed(r) or r in SMALL_FILES, allow_create=True)


def daily_catalog_payload(original, entries, calendar, cas):
    """Replace only amended stocks' quote spans; PIT buy pools are untouched."""
    spans = defaultdict(list)
    for line in original.decode('utf-8-sig').splitlines():
        code, start, end = line.split()
        spans[code.upper()].append((start, end))
    for row in entries:
        if not row['relative_path'].endswith('/close.day.bin'):
            continue
        code = row['relative_path'].split('/')[-2].upper()
        values = _array(cas.get_bytes(cas.validate_reference_metadata(row['target_ref'])))
        offset = int(values[0])
        valid = np.flatnonzero(np.isfinite(values[1:]) & (values[1:] > 0)) + offset
        if not len(valid) or valid[-1] >= len(calendar):
            raise ValueError('daily quote catalog has invalid real-price bounds')
        groups = np.split(valid, np.flatnonzero(np.diff(valid) != 1) + 1)
        spans[code] = [(calendar[int(group[0])],calendar[int(group[-1])]) for group in groups]
    return ''.join(f'{code}\t{start}\t{end}\n' for code in sorted(spans)
                   for start,end in sorted(spans[code])).encode('utf-8')


def apply_quote_repairs(resume, *, finalized, plan, cas, prepare=None):
    binding = plan.get(BINDING)
    if binding is None:
        return finalized
    root = resume.root
    if (resume.path is None or root.parent.name != ".staging" or (root / "qe_dataset_manifest.json").exists()
            or binding["operation_id"] != resume.context.operation_id or binding["target_cutoff"] != plan["target_cutoff"]):
        raise ValueError("quote corrections require the same unpublished build")
    authority = load_sealed_build_json(cas, binding["authority_ref"])
    if authority.get("schema_version") != SCHEMA:
        raise ValueError("quote correction authority differs from its retained export")
    prepared = prepare or load_sealed_build_json(cas, resume.record["steps"]["prepare"])
    factor = load_sealed_build_json(cas, load_sealed_build_json(cas,
        prepared["prepare_receipt_ref"])["refs"]["factor_receipt"])
    basis = [row for row in factor["files"] if row["dataset"] == "daily_pv"]
    if len(basis) != 1 or basis[0]["sha256"] != authority.get("daily_pv_sha256"):
        raise ValueError("quote QFQ basis differs from the retained daily facts")
    if resume.record["steps"].get("quote-facts") == binding["authority_ref"]:
        return load_sealed_build_json(cas, resume.record["steps"]["finalize-bins"])
    files, amendments = {}, {"daily_bin": [], "minute_bin": []}
    inner = copy.deepcopy(load_sealed_build_json(cas, finalized["finalize_receipt_ref"]))
    datasets = {row["relative_path"].split("/")[0] for row in authority["files"]}
    inherited = all(load_sealed_build_json(cas, inner[dataset.removesuffix("_bin") + "_receipt_ref"])
        .get("quote_fact_correction", {}).get("authority_ref") == binding["authority_ref"] for dataset in datasets)
    if inherited and plan["monthly_ready_correction"].get("base_plan", {}).get(BINDING) == binding:
        record = copy.deepcopy(resume.record)
        record["steps"]["quote-facts"] = binding["authority_ref"]
        _atomic(resume.path, record)
        resume.record = record
        return finalized
    if authority["base_manifest_ref"] != plan["monthly_ready_correction"]["base_manifest_ref"]:
        raise ValueError("quote correction authority differs from its retained export")
    from .qlib_bounded_update import read_calendar
    lengths = {dataset: len(read_calendar(root / dataset / "qlib/calendars" /
        ("day.txt" if dataset == "daily_bin" else "1min.txt"))) for dataset in datasets}
    for row in authority["files"]:
        resume.context.checkpoint()
        relative = row["relative_path"]
        if not allowed(relative):
            raise ValueError("quote repair escaped feature scope")
        path = root / relative
        _plain(path)
        old_sig = list(_signature(path)) if path.exists() else None
        old = path.read_bytes() if path.exists() else None
        if (None if old is None else hashlib.sha256(old).hexdigest()) != row["original_sha256"]:
            raise ValueError("quote repair target no longer matches the old export")
        target = cas.get_bytes(cas.validate_reference_metadata(row["target_ref"]))
        changed = validate_missing_only(old, target)
        values = _array(target)
        if int(values[0]) + len(values) - 1 > lengths[relative.split("/")[0]]:
            raise ValueError("quote correction exceeds the frozen calendar")
        field = path.name.split(".")[0]
        finite = values[1:][np.isfinite(values[1:])]
        valid = np.isin(finite, (0, 1)) if field in {"limit_up", "limit_down"} else (
            finite >= 0 if field in {"volume", "amount"} else finite > 0)
        if not valid.all():
            raise ValueError("quote correction field values violate their existing contract")
        if path.exists() and list(_signature(path)) != old_sig:
            raise ValueError("quote repair target changed during preparation")
        if old_sig != resume.record["files"].get(relative):
            raise ValueError("quote repair lost original writer provenance")
        files[relative] = {**row, "original_signature": old_sig}
        amendments[relative.split("/")[0]].append({**row, "filled_cells": changed})
    for dataset, entries in amendments.items():
        if not entries:
            continue
        prefix = dataset.removesuffix("_bin")
        key = prefix + "_receipt_ref"
        materialization = load_sealed_build_json(cas, inner[key])
        correction = {"schema_version": SCHEMA, "authority_ref": binding["authority_ref"],
            "files": entries, "coverage": authority["coverage"], "existing_finite_values_unchanged": True}
        if dataset == 'daily_bin':
            relative = 'daily_bin/qlib/instruments/all.txt'
            catalog = root / relative
            _plain(catalog)
            before = list(_signature(catalog))
            original = catalog.read_bytes()
            payload = daily_catalog_payload(original,entries,
                read_calendar(root/'daily_bin/qlib/calendars/day.txt'),cas)
            if before != list(_signature(catalog)):
                raise ValueError('daily quote catalog changed during correction')
            correction['catalog_before_ref'] = cas.put_bytes(original).as_dict()
            correction['catalog_after_ref'] = cas.put_bytes(payload).as_dict()
            files[relative] = {'original_signature':before,'original_sha256':hashlib.sha256(original).hexdigest(),
                'target_ref':correction['catalog_after_ref']}
        updated = {**materialization, "quote_fact_correction": correction}
        relative = dataset + "/materialization_receipt.json"
        path = root / relative
        before = list(_signature(path))
        raw = path.read_bytes()
        if json.loads(raw) != materialization or before != list(_signature(path)):
            raise ValueError("quote materialization receipt changed")
        ref = cas.put_json(updated).as_dict()
        files[relative] = {"original_signature": before, "original_sha256": hashlib.sha256(raw).hexdigest(),
            "target_ref": cas.put_bytes(canonical_json_bytes(updated) + b"\n").as_dict()}
        inner.update({key: ref, prefix + "_materialization_receipt_file_ref": ref})
    result = {**finalized, "finalize_receipt_ref": cas.put_json(inner).as_dict()}
    steps = {**resume.record["steps"], "finalize-bins": cas.put_json(result).as_dict(), "quote-facts": binding["authority_ref"]}
    _atomic(_pending(root), {"schema_version": SCHEMA, "binding_digest": resume.record["binding_digest"],
        "original_steps": resume.record["steps"], "target_steps": steps, "files": files})
    resume.record = recover_quote_repairs(root=root, journal=resume.path, record=resume.record, cas=cas)
    return result


def correction_pins(materialization, *, provider, cas):
    correction = materialization.get("quote_fact_correction")
    if correction is None:
        return {}
    authority = load_sealed_build_json(cas, correction["authority_ref"])
    if authority.get("schema_version") != SCHEMA or correction.get("schema_version") != SCHEMA:
        raise ValueError("quote repair validation identity differs")
    result = {}
    expected = {r["relative_path"]: r for r in authority["files"]}
    for row in correction["files"]:
        relative = row["relative_path"]
        if not allowed(relative) or expected.get(relative, {}).get("target_ref") != row["target_ref"]:
            raise ValueError("quote repair validation file identity differs")
        path = provider.parent.parent / relative
        _plain(path)
        before, raw = _signature(path), path.read_bytes()
        values = _array(raw)
        if hashlib.sha256(raw).hexdigest() != row["target_ref"]["sha256"] or _signature(path) != before:
            raise ValueError("quote repair bytes changed after construction")
        result[path] = {"target_sha256": row["target_ref"]["sha256"], "target_size": len(raw),
            "target_signature": list(before), "start_index": int(values[0]), "end_index": int(values[0]) + len(values) - 2}
    return result
