"""Refresh small quote catalogs on retained native exports, never price bins."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile

from backend.qlib_exporter.quote_catalog import build_minute_quote_catalog
from .canonical import canonical_json_bytes
from .monthly_build_resume import MonthlyBuildResumeError, _atomic, _plain, _signature, load_sealed_build_json


_CATALOG = "minute_bin/qlib/instruments/all.txt"
_META = "minute_bin/qlib/meta_export.json"
_MATERIALIZATION = "minute_bin/materialization_receipt.json"
_FILES = {_CATALOG, _META, _MATERIALIZATION}


def _pending(root):
    return root.parent / f".{root.name}.quote-catalog-commit.json"


def _replace(path: Path, payload: bytes):
    _plain(path)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as writer:
            writer.write(payload)
            writer.flush()
            os.fsync(writer.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def recover_quote_catalog(*, root, journal, record, cas):
    """Replay a fenced, small-file commit after a crash at any replacement."""
    pending = _pending(root)
    _plain(pending)
    if not pending.exists():
        return record
    if pending.stat().st_size > 32768:
        raise MonthlyBuildResumeError("quote catalog commit is oversized")
    commit = json.loads(pending.read_text(encoding="utf-8"))
    if (commit.get("schema_version") != "aistock_monthly_quote_catalog_commit_v1"
            or commit.get("binding_digest") != record["binding_digest"]
            or set(commit.get("files", {})) != _FILES):
        raise MonthlyBuildResumeError("quote catalog commit identity differs")
    if record["steps"] not in (commit["original_steps"], commit["target_steps"]):
        raise MonthlyBuildResumeError("quote catalog commit lost step provenance")
    payloads = {}
    # Validate the complete small transaction before replacing any file.
    for relative, entry in commit["files"].items():
        path = root / relative
        _plain(path)
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        target_ref = cas.validate_reference_metadata(entry["target_ref"])
        if target_ref.size <= 0:
            raise MonthlyBuildResumeError("quote catalog commit payload is empty")
        target = cas.get_bytes(target_ref)
        if digest != entry["target_ref"]["sha256"] and (
            digest != entry["original_sha256"] or list(_signature(path)) != entry["original_signature"]
        ):
            raise MonthlyBuildResumeError("quote catalog commit input changed")
        payloads[relative] = target
    result = copy.deepcopy(record)
    for relative, payload in payloads.items():
        path = root / relative
        if path.read_bytes() != payload:
            _replace(path, payload)
        if relative != _META:
            result["files"][relative] = list(_signature(path))
    result["steps"] = commit["target_steps"]
    _atomic(journal, result)
    pending.unlink()  # Completed small-file intent only, never an export.
    return result


def refresh_retained_quote_catalog(resume, *, finalized, cas):
    """Upgrade both new and retained native completion receipts in place privately.

    Old CAS blobs, SOURCE, CSVs, writer-completed prices, pools and immutable
    predecessor remain unchanged. Only three small private output files and
    the resume references are committed. Physical edges reuse the shared
    catalog contract; this is not a full-history price-quality audit.
    """
    if resume.path is None or "finalize_receipt_ref" not in finalized:
        return finalized
    inner = load_sealed_build_json(cas, finalized["finalize_receipt_ref"])
    if "minute_receipt_ref" not in inner:
        return finalized
    materialization = load_sealed_build_json(cas, inner["minute_receipt_ref"])
    if materialization.get("schema_version") != "aistock_monthly_native_qlib_materialization_v1":
        return finalized
    root = resume.root
    if root.parent.name != ".staging" or resume.path.parent != root.parent:
        raise MonthlyBuildResumeError("quote catalog repair is not private retained staging")
    before = {relative: (list(_signature(root / relative)), (root / relative).read_bytes()) for relative in _FILES}
    if json.loads(before[_MATERIALIZATION][1]) != materialization:
        raise MonthlyBuildResumeError("quote catalog materialization provenance differs")
    native = materialization["native_writer"]
    prepared = load_sealed_build_json(cas, materialization["sealed_canonical_rows"]["month_preparation_ref"])
    physical = build_minute_quote_catalog(root / "minute_bin/qlib", checkpoint=resume.context.checkpoint)
    authority = {**physical.receipt(), "input_instruments_sha256": prepared["instruments_ref"]["sha256"]}
    meta = json.loads(before[_META][1])
    if (materialization.get("quote_catalog_repair", native.get("quote_catalog")) == authority
            and meta.get("provider_catalog_contract") == authority["mode"]
            and meta.get("all_txt_rewrite") == authority):
        if before[_CATALOG][1] != physical.payload:
            raise MonthlyBuildResumeError("sealed quote catalog bytes differ")
        return finalized
    # Preserve the actual append-writer receipt, including its original
    # catalog signature. A separately sealed projection describes the repair.
    materialization = {**materialization, "quote_catalog_repair": authority}
    materialization_ref = cas.put_json(materialization)
    inner = {**inner, "minute_receipt_ref": materialization_ref.as_dict(),
             "minute_materialization_receipt_file_ref": materialization_ref.as_dict()}
    updated = {**finalized, "finalize_receipt_ref": cas.put_json(inner).as_dict()}
    steps = copy.deepcopy(resume.record["steps"])
    steps["finalize-bins"] = cas.put_json(updated).as_dict()
    payloads = {
        _CATALOG: physical.payload,
        _META: canonical_json_bytes({**meta, "provider_catalog_contract": authority["mode"], "all_txt_rewrite": authority}) + b"\n",
        _MATERIALIZATION: canonical_json_bytes(materialization) + b"\n",
    }
    files = {}
    for relative, payload in payloads.items():
        signature, raw = before[relative]
        if list(_signature(root / relative)) != signature:
            raise MonthlyBuildResumeError("quote catalog input changed during preparation")
        files[relative] = dict(original_signature=signature, original_sha256=hashlib.sha256(raw).hexdigest(),
                               target_ref=cas.put_bytes(payload).as_dict())
    commit = dict(schema_version="aistock_monthly_quote_catalog_commit_v1",
                  binding_digest=resume.record["binding_digest"], original_steps=resume.record["steps"],
                  target_steps=steps, files=files)
    _atomic(_pending(root), commit)
    resume.record = recover_quote_catalog(root=root, journal=resume.path, record=resume.record, cas=cas)
    return updated
