"""Exact construction facts for month-only SOURCE, never historical QA."""
from __future__ import annotations

from datetime import date
from contextlib import AbstractContextManager
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

from .monthly_legacy_factor import read_legacy_factor_tail
from .errors import SourceManifestError
from .index_contract import DOMESTIC_INDEX_DEFINITIONS
from .monthly_legacy_prefix import (
    LegacyMonthlyPrefix, LegacyMonthlyPrefixError, _plain_chain, _relative, _signature, read_legacy_qfq_anchors,
)


def collect_suspended_daily_facts(
    *,
    descriptors: Sequence[Mapping[str, Any]],
    partition_rows: Callable[[Mapping[str, Any]], AbstractContextManager[Iterable[Mapping[str, Any]]]],
    suspended: frozenset[tuple[str, date]],
    trading_dates: Sequence[date],
    checkpoint: Callable[[], None],
) -> Mapping[tuple[str, date], tuple[Mapping[str, Any], ...]]:
    """Retain independent no-trade facts only inside this construction window.

    A sealed empty current partition proves absence; a missing or old-only
    partition does not. The supplied context verifies CAS streams on exit.
    """
    if not suspended:
        return {}
    if not descriptors:
        raise SourceManifestError("sealed daily partitions are missing for suspension proof")
    facts: dict[tuple[str, date], list[Mapping[str, Any]]] = {}
    covered_dates: set[date] = set()
    for descriptor in descriptors:
        match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})", str(descriptor.get("partition_key", "")))
        if match is None:
            raise SourceManifestError("daily suspension proof partition identity is invalid")
        start, end = (date.fromisoformat(value) for value in match.groups())
        if end < trading_dates[0] or start > trading_dates[-1]:
            continue
        covered_dates.update(day for day in trading_dates if start <= day <= end)
        with partition_rows(descriptor) as rows:
            for ordinal, row in enumerate(rows):
                key = (str(row.get("ts_code", "")).upper(), date.fromisoformat(str(row.get("trade_date"))[:10]))
                if key in suspended:
                    values = facts.setdefault(key, [])
                    values.append(dict(row))
                    if len(values) > 1:
                        raise SourceManifestError(
                            "daily suspension proof is duplicated",
                            context={"ts_code": key[0], "trade_date": key[1].isoformat()},
                        )
                if ordinal % 10_000 == 0:
                    checkpoint()
        checkpoint()
    if any(day not in covered_dates for _, day in suspended if day in trading_dates):
        raise SourceManifestError("sealed daily partitions do not cover the suspension proof window")
    return {key: tuple(values) for key, values in facts.items()}


def collect_qfq_construction_anchors(
    prefix: LegacyMonthlyPrefix, *, month_start: date,
    instruments: tuple[str, ...],
    checkpoint: Callable[[], None] = lambda: None,
    progress: Callable[[Mapping], None] = lambda _: None,
) -> tuple[tuple[str, date], ...]:
    """Collect dates, not invented raw factors or a historical completeness PASS.

    The provider catalog bounds existing physical stocks. New-month IPOs have
    no inherited files. Each Bin contributes its last real factor date; the
    factor aggregate contributes its actual one-row rolling boundary. SOURCE
    then queries precisely these raw facts in the same snapshot as the month.
    """
    if prefix.cutoff != month_start - date.resolution:
        raise LegacyMonthlyPrefixError("construction prefix does not precede target month")
    provider = _relative(prefix.manifest["components"]["day_meta_export"]["path"]).parent
    catalog = prefix.root / provider / "instruments/all.txt"
    _plain_chain(catalog)
    lines = [line.split("\t") for line in catalog.read_text(encoding="utf-8").splitlines()]
    if any(len(row) != 3 for row in lines):
        raise LegacyMonthlyPrefixError("construction provider catalog schema differs")
    # Ended securities have no September tail to rebase. Do not look up their
    # old Bin/H5 values; SQL still computes genuine cutoff denominators for PIT.
    index_codes = {item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS}
    codes = tuple(sorted({row[0].upper() for row in lines
                          if row[0].upper().endswith((".SH", ".SZ"))
                          and row[0].upper() not in index_codes
                          and row[0].upper() in instruments}))
    result = set()
    for dataset in ("daily_bin", "minute_bin"):
        progress({"phase": "SOURCE_CONSTRUCTION_BOUNDARIES",
                  "query_id": "adj_factor_construction", "partition_key": dataset})
        for code, row in read_legacy_qfq_anchors(prefix, dataset=dataset, instruments=codes, checkpoint=checkpoint).items():
            result.add((code, date.fromisoformat(row["trade_date"])))
    factor_path = prefix.root / Path(prefix.manifest["components"]["factor_meta"]["path"]).parent / "daily_pv.h5"
    _plain_chain(factor_path)
    inventory_pin = prefix.manifest["components"]["factor_content_manifest"]
    inventory_path = prefix.root / _relative(inventory_pin["path"])
    _plain_chain(inventory_path)
    before = _signature(inventory_path)
    if before[2] != inventory_pin["size"] or before[2] > 16 * 1024 * 1024:
        raise LegacyMonthlyPrefixError("construction factor inventory size differs")
    raw = inventory_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != inventory_pin["sha256"] or _signature(inventory_path) != before:
        raise LegacyMonthlyPrefixError("construction factor inventory identity differs")
    inventory = json.loads(raw)
    pins = [item for item in inventory.get("files", ())
            if item.get("path") == factor_path.relative_to(prefix.root).as_posix()]
    if len(pins) != 1 or factor_path.stat().st_size != pins[0]["size"]:
        raise LegacyMonthlyPrefixError("construction factor boundary physical pin differs")
    def rolling_progress(_value: Mapping) -> None:
        # The rolling reader emits local physical-read/index counters. Those
        # are not validated/sealed SOURCE facts and must neither reset the
        # durable monotonic counters nor expand the strict progress schema.
        progress({"phase": "SOURCE_CONSTRUCTION_ROLLING_SEED",
                  "query_id": "adj_factor_construction", "partition_key": factor_path.stem})

    tail = read_legacy_factor_tail(factor_path, instruments=codes, rows_per_instrument=1,
                                  before=month_start, checkpoint=checkpoint, progress=rolling_progress)
    result.update((str(code), pd.Timestamp(stamp).date()) for stamp, code in tail.index)
    if any(day >= month_start for _, day in result):
        raise LegacyMonthlyPrefixError("construction boundary is not prior to target month")
    return tuple(sorted(result))
