"""Shared, release-pinned SW2021 quote facts, distinct from stock classification.

The stock sector table retains authentic SW2014 history.  This projection
deduplicates its published industry observations without rewriting any stock
membership or generating prices.  After bootstrap, only the new month's stock
rows are read; the small pinned predecessor facts supply the immutable prefix.
"""

from __future__ import annotations

from datetime import date
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Any, Callable, Mapping

from .monthly_industry_history import BOUNDARY
from .shared_sector_context import validate_release_sw_l2_code_map

QUOTE_FACTS_SCHEMA = "aistock_release_sw_l2_quote_facts_v1"
QUOTE_FACTS_PATH = "components/sector_context_candidate_v1/sector_quote_facts.h5"
_METRICS = ("sw2_pct_change", "sw2_vol", "sw2_amount", "sw2_mf_net_amt",
            "sw2_mf_buy_elg_amt", "sw2_mf_sell_elg_amt")


class SectorQuoteFactsError(RuntimeError):
    """The shared projection cannot preserve frozen fact identity."""


def _signature(path: Path) -> list[int]:
    for parent in (path, *path.parents):
        if parent.is_symlink() or bool(getattr(parent, "is_junction", lambda: False)()):
            raise SectorQuoteFactsError("quote facts path is linked")
    metadata = path.stat()
    if not path.is_file():
        raise SectorQuoteFactsError("quote facts source is not a regular file")
    return [metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns]


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _prefix_pin(prefix, name: str) -> tuple[Path, Mapping[str, Any]]:
    pin = prefix.manifest["components"].get(name)
    if not isinstance(pin, Mapping):
        # Manifest component names may be generated from the relative filename.
        matches = [value for value in prefix.manifest["components"].values()
                   if isinstance(value, Mapping) and Path(str(value.get("path", ""))).stem == name]
        if len(matches) != 1:
            raise SectorQuoteFactsError(f"predecessor {name} pin is missing or ambiguous")
        pin = matches[0]
    path = prefix.root / str(pin.get("path", ""))
    if not path.resolve(strict=True).is_relative_to(prefix.root.resolve(strict=True)):
        raise SectorQuoteFactsError("predecessor quote pin escapes release")
    before = _signature(path)
    if pin.get("sha256") != _sha(path) or before[2] != pin.get("size", pin.get("byte_size")):
        raise SectorQuoteFactsError(f"predecessor {name} pin differs")
    if before != _signature(path):
        raise SectorQuoteFactsError("predecessor quote pin changed")
    return path, pin


def _collapse(frame):
    """Choose actual finite observations only; never infer NA or average facts."""
    grouped = frame.groupby(["datetime", "l2_code_id"], sort=True)
    if (grouped[list(_METRICS)].nunique(dropna=True) > 1).any().any():
        raise SectorQuoteFactsError("contradictory frozen industry facts")
    return grouped[list(_METRICS)].first().reset_index()


def build_sector_quote_facts(
    *, source_path: Path, target_path: Path, source_pin: Mapping[str, Any],
    code_map, cutoff: date, chunk_rows: int, prefix=None, month_range=None,
    checkpoint: Callable[[], None] = lambda: None,
    progress: Callable[[Mapping[str, Any]], None] = lambda _value: None,
) -> dict[str, Any]:
    import numpy as np
    import pandas as pd

    before = _signature(source_path)
    if (source_pin.get("signature") != before or source_pin.get("size", source_pin.get("byte_size")) != before[2]
        or len(str(source_pin.get("sha256", ""))) != 64):
        raise SectorQuoteFactsError("source pin is incomplete or stale")
    if type(chunk_rows) is not int or chunk_rows < 1:
        raise SectorQuoteFactsError("quote projection chunk size is invalid")
    if target_path.exists():
        raise SectorQuoteFactsError("quote facts target already exists")
    for parent in (target_path, *target_path.parents):
        if parent.is_symlink() or bool(getattr(parent, "is_junction", lambda: False)()):
            raise SectorQuoteFactsError("quote facts target is linked")
    inherited = None
    old_sha = None
    pins = {} if prefix is None else prefix.manifest["components"]
    has_prior = "sector_quote_facts" in pins or any(
        isinstance(pin, Mapping) and str(pin.get("path", "")).endswith("/sector_quote_facts.h5")
        for pin in pins.values()
    )
    read_start = BOUNDARY
    if has_prior:
        old_path, old_pin = _prefix_pin(prefix, "sector_quote_facts")
        map_path, _ = _prefix_pin(prefix, "sector_code_map")
        old_map = validate_release_sw_l2_code_map(json.loads(map_path.read_bytes()))
        if old_map.id_to_code != code_map.id_to_code:
            raise SectorQuoteFactsError("predecessor quote mapping differs")
        inherited = pd.read_hdf(old_path, "data").reset_index()
        if _sha(old_path) != old_pin["sha256"]:
            raise SectorQuoteFactsError("predecessor quote pin changed during read")
        old_sha = old_pin["sha256"]
        read_start = cutoff.replace(day=1)
        if prefix.cutoff >= read_start or prefix.cutoff < BOUNDARY:
            raise SectorQuoteFactsError("quote predecessor is not an earlier monthly release")
    pieces = []
    source_rows = classification_only = 0
    last_progress = time.monotonic()
    with pd.HDFStore(source_path, mode="r") as store:
        storer = store.get_storer("data")
        if not storer.is_table:
            raise SectorQuoteFactsError("quote source must support bounded table reads")
        arguments = {"columns": ["l2_code_id", *_METRICS], "chunksize": chunk_rows, "iterator": True}
        if has_prior and month_range is not None:
            start, count = month_range.get("start_row"), month_range.get("row_count")
            if type(start) is not int or type(count) is not int or start < 0 or count < 0 or start + count != storer.nrows:
                raise SectorQuoteFactsError("quote source monthly physical range differs")
            arguments.update(start=start, stop=start + count)
        else:
            arguments["where"] = [f"datetime >= Timestamp('{read_start.isoformat()}')",
                                  f"datetime <= Timestamp('{cutoff.isoformat()}')"]
        for chunk in store.select("data", **arguments):
            checkpoint()
            frame = chunk.reset_index()
            source_rows += len(frame)
            if frame.empty:
                continue
            days = pd.to_datetime(frame["datetime"], errors="raise")
            if (days < pd.Timestamp(read_start)).any() or (days > pd.Timestamp(cutoff)).any():
                raise SectorQuoteFactsError("quote fact date escapes source scope")
            values = frame[list(_METRICS)].to_numpy(dtype=float)
            if np.isinf(values).any():
                raise SectorQuoteFactsError("non-finite frozen quote value")
            factual = np.isfinite(values).any(axis=1)
            classification_only += int((~factual).sum())
            frame = frame.loc[factual, ["datetime", "l2_code_id", *_METRICS]].copy()
            ids = pd.to_numeric(frame["l2_code_id"], errors="raise")
            if (~ids.isin(code_map.id_to_code)).any():
                raise SectorQuoteFactsError("unknown quote industry ID")
            frame["l2_code_id"] = ids.astype("int16")
            if not frame.empty:
                pieces.append(_collapse(frame))
            if len(pieces) >= 8:
                pieces = [_collapse(pd.concat(pieces, ignore_index=True))]
            if time.monotonic() - last_progress >= 2:
                progress({"phase": "BUILD_SHARED_SECTOR_QUOTE_FACTS", "dataset": "sector_quote_facts",
                          "physical_rows_read": source_rows})
                last_progress = time.monotonic()
    if _signature(source_path) != before:
        raise SectorQuoteFactsError("quote source changed during projection")
    checkpoint()
    delta = _collapse(pd.concat(pieces, ignore_index=True)) if pieces else pd.DataFrame(
        columns=["datetime", "l2_code_id", *_METRICS])
    delta["instrument"] = delta.l2_code_id.map(code_map.id_to_code)
    frame = pd.concat([inherited, delta], ignore_index=True) if inherited is not None else delta
    if frame.empty or frame.duplicated(["datetime", "l2_code_id"]).any():
        raise SectorQuoteFactsError("shared quote facts are empty or duplicated")
    frame["l2_code_id"] = frame.l2_code_id.astype("int16")
    frame = frame.set_index(["datetime", "instrument"])[["l2_code_id", *_METRICS]].sort_index()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target_path.open("xb"):
            pass
    except FileExistsError as exc:
        raise SectorQuoteFactsError("quote facts target already exists") from exc
    frame.to_hdf(target_path, key="data", mode="w", format="table", data_columns=["datetime", "instrument"])
    with target_path.open("r+b") as handle:
        os.fsync(handle.fileno())
    return {
        "schema_version": QUOTE_FACTS_SCHEMA, "sha256": _sha(target_path),
        "byte_size": target_path.stat().st_size, "row_count": len(frame),
        "start": frame.index.get_level_values("datetime").min().date().isoformat(),
        "end": frame.index.get_level_values("datetime").max().date().isoformat(),
        "mapping_authority": dict(code_map.mapping_authority), "code_map_digest": code_map.code_map_digest,
        "source_sha256": source_pin["sha256"], "source_rows_read": source_rows,
        "classification_only_rows": classification_only,
        "validation_scope": "month_delta" if has_prior else "quote_fact_projection_bootstrap",
        "historical_business_audit_performed": False,
        "inherited_row_count": 0 if inherited is None else len(inherited),
        "predecessor_manifest_sha256": None if prefix is None else prefix.manifest_sha256,
        "inherited_quote_facts_sha256": old_sha,
        "no_fabrication": True, "database_read": False, "database_write": False,
    }
