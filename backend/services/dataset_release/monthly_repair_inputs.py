"""Explicit approved repair inputs for ONE ordinary monthly successor.

This is not a baseline gate, source selection heuristic, or activation grant.
Only small pinned metadata and exact authorized repair keys are read. The
active release remains the logical predecessor; physical repaired aggregates
can come from independently approved immutable candidates of that release.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from .canonical import canonical_json_bytes
from .monthly_legacy_prefix import (
    LegacyMonthlyPrefix, _plain_chain, _relative, _signature, load_legacy_prefix,
)

SCHEMA = "aistock_monthly_repair_inputs_v1"
FIELDS = {"db_dv_ratio": "dv_ratio", "db_dv_ttm": "dv_ttm", "db_turnover_rate_f": "turnover_rate_f"}
RECEIPTS = {
    "daily_basic_source_history_repair": {"qe_daily_basic_source_history_repair_v1"},
    "moneyflow_alias_repair": {"aistock_moneyflow_source_alias_repair_v1", "aistock_moneyflow_source_alias_repair_v2"},
    "daily_basic_fields_repair": {"aistock_daily_basic_fields_selective_repair_v1"},
}
DATASET_RECEIPTS = {"daily_basic": {"daily_basic_source_history_repair"},
    "moneyflow": {"moneyflow_alias_repair"}, "static_factors": {"daily_basic_fields_repair"}}


def deferred_margin_authority(payload: Mapping[str, Any] | None, *, target_cutoff: date) -> str | None:
    entries = (payload or {}).get("deferred_source_dates", [])
    if entries == []:
        return None
    if not isinstance(entries, list) or entries != [{"dataset": "margin_detail",
        "trade_date": target_cutoff.isoformat(), "reason_code": "USER_DEFERRED_COLLECTION"}]:
        raise ValueError("only an explicitly bound financing cutoff deferral is supported")
    # The authenticated preparation binding is the authority, NOT a claim
    # that the provider has no data or the fact is economically zero.
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _read(path: Path, sha256: str, *, max_bytes: int = 16 * 1024 * 1024) -> bytes:
    _plain_chain(path)
    before = _signature(path)
    if before[2] > max_bytes:
        raise ValueError("monthly repair input exceeds bounded file size")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != sha256 or _signature(path) != before:
        raise ValueError("monthly repair input bytes differ")
    return raw


def _candidate(entry: Mapping[str, Any], predecessor: LegacyMonthlyPrefix) -> tuple[LegacyMonthlyPrefix, Mapping[str, Any]]:
    if not isinstance(entry, Mapping) or set(entry) != {
        "candidate_root", "dataset_manifest_sha256", "dataset_manifest_file_sha256", "receipt_component", "receipt_sha256",
    }:
        raise ValueError("monthly repair candidate binding fields differ")
    root = Path(entry["candidate_root"])
    if not root.is_absolute() or root.parent != predecessor.root.parent or root == predecessor.root:
        raise ValueError("monthly repair candidate must be an explicit plain catalog sibling")
    prefix = load_legacy_prefix(root, expected_manifest_sha256=entry["dataset_manifest_sha256"],
        expected_file_sha256=entry["dataset_manifest_file_sha256"], expected_cutoff=predecessor.cutoff,
        expected_release_id=predecessor.release_id)
    component = entry["receipt_component"]
    pin = prefix.manifest["components"].get(component)
    if component not in RECEIPTS or not isinstance(pin, Mapping) or pin.get("sha256") != entry["receipt_sha256"]:
        raise ValueError("monthly repair receipt is not an approved manifest pin")
    raw = _read(prefix.root / _relative(pin.get("path")), entry["receipt_sha256"])
    if pin.get("size") != len(raw):
        raise ValueError("monthly repair receipt dimensions differ")
    receipt = json.loads(raw)
    if not isinstance(receipt, Mapping) or receipt.get("schema_version") not in RECEIPTS[component]:
        raise ValueError("monthly repair receipt schema differs")
    if receipt.get("active_profile_write") is True or receipt.get("runtime_action") is True:
        raise ValueError("monthly repair candidate crossed publication boundary")
    if component == "moneyflow_alias_repair" and (
        receipt.get("status") != "PASS" or receipt.get("source_dataset_manifest_sha256") != predecessor.manifest_sha256
    ):
        raise ValueError("monthly moneyflow repair is not closed against the logical predecessor")
    if component == "daily_basic_fields_repair" and (
        receipt.get("status") != "PASS" or receipt.get("baseline_manifest_identity") != predecessor.manifest_sha256
        or receipt.get("existing_finite_preserved") is not True or receipt.get("source_null_preserved") is not True
    ):
        raise ValueError("monthly field repair is not closed against the logical predecessor")
    return prefix, receipt


def repair_materials(payload: Mapping[str, Any], *, predecessor: LegacyMonthlyPrefix) -> tuple[dict[str, LegacyMonthlyPrefix], list[dict[str, Any]]]:
    prefixes = {}
    for dataset, entry in payload["factor_prefixes"].items():
        if not isinstance(entry, Mapping) or dataset not in DATASET_RECEIPTS or entry.get("receipt_component") not in DATASET_RECEIPTS[dataset]:
            raise ValueError("monthly repair dataset/receipt is not registered")
        prefixes[dataset], _ = _candidate(entry, predecessor)
    repair = payload["daily_basic_null_repairs"]
    entries = []
    if repair is not None:
        if not isinstance(repair, Mapping) or set(repair) != {"candidate", "date_fields"}:
            raise ValueError("monthly exact field repair shape differs")
        if not isinstance(repair["candidate"], Mapping) or repair["candidate"].get("receipt_component") != "daily_basic_fields_repair":
            raise ValueError("monthly field repair receipt is unsupported")
        _, receipt = _candidate(repair["candidate"], predecessor)
        artifacts = receipt.get("source_artifacts")
        requested = repair["date_fields"]
        if not isinstance(artifacts, list) or not isinstance(requested, list) or not 0 < len(requested) <= 128:
            raise ValueError("monthly exact source repair scope is invalid")
        by_date = {item["trade_date"]: item for item in artifacts}
        if len(by_date) != len(artifacts):
            raise ValueError("monthly repair source dates are duplicated")
        seen = set()
        for request in requested:
            if not isinstance(request, Mapping) or set(request) != {"trade_date", "fields"}:
                raise ValueError("monthly exact repair fields differ")
            day = date.fromisoformat(request["trade_date"])
            fields = request["fields"]
            if day > predecessor.cutoff or day in seen or not isinstance(fields, list) or not fields or len(set(fields)) != len(fields):
                raise ValueError("monthly exact repair date/field scope differs")
            if any(field not in FIELDS or field not in receipt.get("h5_filled_cells", {}) for field in fields):
                raise ValueError("monthly repair field is not approved by source receipt")
            artifact = by_date.get(day.isoformat())
            if not isinstance(artifact, Mapping):
                raise ValueError("monthly repair date lacks a frozen provider source")
            source = Path(artifact["source_path"])
            root = predecessor.root.parent.parent / "data_repair_receipts"
            if not source.is_absolute() or not source.is_relative_to(root):
                raise ValueError("monthly repair provider source escaped registered repair roots")
            _read(source, artifact["provider_sha256"])
            entries.append({"source_path": str(source), "provider_sha256": artifact["provider_sha256"],
                            "trade_date": day.isoformat(), "fields": sorted(fields)})
            seen.add(day)
    return prefixes, entries


def validate_monthly_repair_inputs(payload: Mapping[str, Any], *, predecessor: LegacyMonthlyPrefix, target_cutoff: date) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != {
        "schema_version", "target_cutoff", "predecessor_manifest_sha256", "factor_prefixes",
        "daily_basic_null_repairs", "deferred_source_dates",
    } or payload.get("schema_version") != SCHEMA:
        raise ValueError("monthly repair inputs schema differs")
    if payload["target_cutoff"] != target_cutoff.isoformat() or payload["predecessor_manifest_sha256"] != predecessor.manifest_sha256:
        raise ValueError("monthly repair inputs predecessor/month identity differs")
    if not isinstance(payload["factor_prefixes"], Mapping) or len(payload["factor_prefixes"]) > 3:
        raise ValueError("monthly repair prefix scope is invalid")
    deferred_margin_authority(payload, target_cutoff=target_cutoff)
    repair_materials(payload, predecessor=predecessor)
    return json.loads(canonical_json_bytes(payload))


def apply_daily_basic_null_repairs(target: Path, *, entries: Sequence[Mapping[str, Any]],
                                  checkpoint: Callable[[], None] = lambda: None) -> Mapping[str, Any]:
    """Patch only indexed, existing, approved cells in a PRIVATE writer copy.

    No row creation, old population audit, finite overwrite or historical
    formula recomputation. Provider-NA remains NA; each exact patch is read back.
    """
    counts: dict[str, int] = {}
    provider_refs = []
    for entry in entries:
        checkpoint()
        source = Path(entry["source_path"])
        raw = _read(source, entry["provider_sha256"])
        fields = tuple(entry["fields"])
        if not fields or len(set(fields)) != len(fields) or any(field not in FIELDS for field in fields):
            raise ValueError("monthly source repair fields differ")
        source_frame = pd.read_parquet(source, columns=["ts_code", "trade_date", *[FIELDS[field] for field in fields]])
        if _read(source, entry["provider_sha256"]) != raw:
            raise ValueError("monthly provider facts changed during read")
        day = pd.Timestamp(entry["trade_date"])
        days = pd.to_datetime(source_frame["trade_date"], format="mixed")
        if source_frame["ts_code"].duplicated().any() or not (days == day).all():
            raise ValueError("monthly provider repair date/key identity differs")
        facts = source_frame.set_index("ts_code")
        _plain_chain(target)
        if _signature(target)[2] == 0 or target.stat().st_nlink != 1:
            raise ValueError("monthly field repair requires a private regular aggregate")
        with pd.HDFStore(target, "a") as store:
            storer = store.get_storer("data")
            if not storer.is_table or "datetime" not in storer.table.colindexes:
                raise ValueError("monthly exact repair requires an indexed table")
            coordinates = store.select_as_coordinates("data", where=[f"datetime == {day.isoformat()!r}"])
            frame = store.select("data", where=coordinates)
            if frame.index.has_duplicates or tuple(frame.index.names) != ("datetime", "instrument"):
                raise ValueError("monthly repair target key identity differs")
            physical_coordinates = coordinates.to_numpy(dtype=np.int64)
            records = storer.table.read_coordinates(physical_coordinates)
            codes = frame.index.get_level_values("instrument")
            expected = frame.copy()
            modified_blocks = set()
            for field in fields:
                existing = frame[field].to_numpy()
                if np.isinf(existing).any():
                    raise ValueError("monthly repair cannot hide an infinite stored value")
                provider = facts[FIELDS[field]].reindex(codes).to_numpy(dtype=float)
                selected = np.isnan(existing) & np.isfinite(provider)
                values = provider[selected].astype(frame[field].dtype)
                if not np.isfinite(values).all():
                    raise ValueError("monthly repair exceeds storage precision")
                expected.loc[selected, field] = values
                counts[field] = counts.get(field, 0) + int(selected.sum())
                if selected.any():
                    blocks = [name for name in storer.table.colnames if name.startswith("values_block_")
                              and field in getattr(storer.table.attrs, name + "_kind")]
                    if len(blocks) != 1:
                        raise ValueError("monthly repair storage block ownership differs")
                    block = blocks[0]
                    offset = list(getattr(storer.table.attrs, block + "_kind")).index(field)
                    records[block][selected, offset] = values
                    modified_blocks.add(block)
            if modified_blocks:
                # modify_coordinates reindexes EVERY column (including the
                # unchanged date/instrument indexes) for every repair date.
                # Update only value blocks in contiguous physical runs: small
                # exact writes, no full-history index rebuilding or row QA.
                breaks = np.r_[0, np.flatnonzero(np.diff(physical_coordinates) != 1) + 1, len(coordinates)]
                for left, right in zip(breaks[:-1], breaks[1:], strict=True):
                    for block in sorted(modified_blocks):
                        storer.table.modify_columns(start=int(physical_coordinates[left]),
                            stop=int(physical_coordinates[right - 1]) + 1,
                            columns=[records[block][left:right]], names=[block])
                storer.table.flush()
            pd.testing.assert_frame_equal(store.select("data", where=coordinates), expected, check_exact=True)
        provider_refs.append({"trade_date": entry["trade_date"], "sha256": entry["provider_sha256"], "fields": list(fields)})
    return {"schema_version": "aistock_monthly_exact_field_repair_readback_v1", "filled_cells": counts,
            "provider_refs": provider_refs, "existing_finite_preserved": True, "source_null_preserved": True,
            "historical_business_audit_performed": False, "database_write": False}
