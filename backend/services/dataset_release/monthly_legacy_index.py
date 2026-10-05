"""Inherited raw index context plus a strictly frozen new-month tail.

The approved legacy H5 stores eight raw fields, share volume and thousand-CNY
amount. It has no historical pre_close/pct_chg. Preserve that representation;
the normalized nine-field Parquet/benchmark CSV is explicitly MONTH ONLY.
Never invent missing old fields or call a runtime/provider source in BUILD.
"""
from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path
from typing import Callable, Sequence

import numpy as np
import pandas as pd

from .canonical import canonical_json_bytes, ensure_sha256
from .index_contract import DOMESTIC_INDEX_DEFINITIONS, IndexDefinition, index_contract_digest, merge_index_rows_missing_only, validate_index_definitions
from .index_materializer import IndexContextSource, IndexMaterializationReceipt, _index_csv_frames, _normalized_rows, _to_context_frame
from .monthly_legacy_prefix import LegacyMonthlyPrefix, LegacyMonthlyPrefixError, _plain_chain, _relative, _signature


RAW_COLUMNS = ("trade_date", "ts_code", "open", "high", "low", "close", "volume", "amount")


def append_legacy_index_context(
    prefix: LegacyMonthlyPrefix, *, source: IndexContextSource, output_root: Path,
    cutoff: date, source_bundle_sha256: str,
    definitions: Sequence[IndexDefinition] = DOMESTIC_INDEX_DEFINITIONS,
    checkpoint: Callable[[], None] = lambda: None,
) -> IndexMaterializationReceipt:
    ensure_sha256(source_bundle_sha256, field="monthly index source bundle")
    definitions = validate_index_definitions(definitions)
    start = cutoff.replace(day=1)
    if prefix.cutoff != start - date.resolution:
        raise LegacyMonthlyPrefixError("native index predecessor is not the preceding month")
    _plain_chain(output_root.parent)
    if output_root.exists() or output_root.resolve().is_relative_to(prefix.root.resolve()) or prefix.root.resolve().is_relative_to(output_root.resolve()):
        raise LegacyMonthlyPrefixError("native index output root overlaps predecessor or exists")
    pin = prefix.manifest["components"]["index_daily"]
    path = prefix.root / _relative(pin["path"])
    _plain_chain(path)
    signature = _signature(path)
    ensure_sha256(pin.get("sha256"), field="inherited raw index H5")
    if signature[2] != pin.get("size"):
        raise LegacyMonthlyPrefixError("inherited raw index size differs")
    # Small fixed-format H5 must be read to serialize an appended successor,
    # not re-validated against historical business facts.
    inherited = pd.read_hdf(path, "data")
    if tuple(inherited.columns) != RAW_COLUMNS:
        raise LegacyMonthlyPrefixError("inherited raw index storage schema differs")
    days = tuple(source.trading_dates(start, cutoff))
    if not days or days != tuple(sorted(set(days))) or any(not start <= day <= cutoff for day in days):
        raise LegacyMonthlyPrefixError("native index month calendar differs")
    rows = []
    for definition in definitions:
        checkpoint()
        merged, _ = merge_index_rows_missing_only(_normalized_rows(source.database_rows(definition, start, cutoff)), [])
        expected = {(definition.daily_code, day) for day in days}
        keys = {(str(row["ts_code"]), row["trade_date"]) for row in merged}
        if keys != expected or len(merged) != len(expected):
            raise LegacyMonthlyPrefixError(f"native index month coverage differs: {definition.daily_code}")
        rows.extend(merged)
    context = _to_context_frame(rows)
    if not np.isfinite(context.to_numpy()).all():
        raise LegacyMonthlyPrefixError("native index month values are non-finite")
    raw_tail = pd.DataFrame.from_records([{
        "trade_date": pd.Timestamp(row["trade_date"]), "ts_code": row["ts_code"],
        **{name: float(row[name]) for name in ("open", "high", "low", "close", "amount")},
        "volume": float(row["vol"]) * 100.0,
    } for row in rows], columns=RAW_COLUMNS)
    # Existing physical dtypes are preserved; no historical field synthesis.
    raw_tail = raw_tail.astype(inherited.dtypes.to_dict())
    combined = pd.concat([inherited, raw_tail], ignore_index=True)
    if _signature(path) != signature:
        raise LegacyMonthlyPrefixError("inherited raw index changed during append")
    output_root.mkdir(exist_ok=False)
    h5 = output_root / "index_daily.h5"
    combined.to_hdf(h5, "data", mode="w", format="fixed")
    parquet = output_root / "index_context.parquet"
    context.to_parquet(parquet)
    csv_root = output_root / "index_csv"
    csv_root.mkdir()
    for code, frame in _index_csv_frames(context).items():
        with (csv_root / f"{code}.csv").open("x", encoding="utf-8", newline="") as writer:
            frame.to_csv(writer, index=False)
    try:
        pd.testing.assert_frame_equal(pd.read_hdf(h5, "data").iloc[len(inherited):].reset_index(drop=True),
                                      raw_tail.reset_index(drop=True), check_exact=True)
        pd.testing.assert_frame_equal(pd.read_parquet(parquet), context, check_exact=True)
    except AssertionError as exc:
        raise LegacyMonthlyPrefixError("native index month readback differs from sealed facts") from exc
    if _signature(path) != signature:
        raise LegacyMonthlyPrefixError("inherited raw index changed during serialization")
    files = [{"relative_path": file.relative_to(output_root).as_posix(), "size": file.stat().st_size,
              "sha256": hashlib.sha256(file.read_bytes()).hexdigest(), "target_signature": list(_signature(file))}
             for file in (h5, parquet, *sorted(csv_root.iterdir()))]
    details = {
        "schema_version": "aistock_monthly_native_index_materialization_v1", "status": "PASS",
        "predecessor_manifest_sha256": prefix.manifest_sha256, "source_bundle_sha256": source_bundle_sha256,
        "cutoff": cutoff.isoformat(), "month_start": start.isoformat(), "validation_scope": "month_delta",
        "h5_storage_contract": "inherited_raw_8_share_volume_thousand_cny_v1",
        "normalized_parquet_scope": "month_delta", "inherited_rows": len(inherited), "month_rows": len(rows),
        "rows": len(combined), "index_count": len(definitions), "files": files,
        "inherited_h5_sha256": pin["sha256"], "historical_business_audit_performed": False,
        "publication_allowed": False, "database_access": False, "provider_access": False,
        "month_output_values_verified": True,
    }
    with (output_root / "index_materialization_receipt.json").open("xb") as writer:
        writer.write(canonical_json_bytes(details) + b"\n")
    checkpoint()
    return IndexMaterializationReceipt(output_root, h5, parquet, csv_root, len(combined), 0, index_contract_digest(), details)
