"""Append the sealed month to shared legacy factor aggregates.

Historical rows are inherited physical content, not a fresh business audit.
Inputs are explicitly pinned frozen facts. Only daily_pv may need a uniform
QFQ basis conversion. This
writer neither calculates factor formulas nor applies historical restatements.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .canonical import ensure_sha256
from .factor_materializer import (
    FACTOR_H5_DTYPES, FACTOR_H5_SCHEMAS, RollingFactorState, SealedFactorChunk,
    _canonicalize_moneyflow_artifact_frame,
)
from backend.data_service.security_source_identity import MONEYFLOW_DATASET, SecuritySourceIdentityManifest
from .monthly_legacy_prefix import LegacyMonthlyPrefix, LegacyMonthlyPrefixError, _plain_chain, _relative, _signature
from .static_schema import STATIC_COLUMN_DTYPES, STATIC_ORDERED_COLUMNS
from .streaming_artifacts import iter_parquet_frames


def _read_prefix_inventory(prefix: LegacyMonthlyPrefix, pin: Mapping[str, Any]) -> bytes:
    path = prefix.root / _relative(pin["path"])
    _plain_chain(path)
    before = _signature(path)
    if before[2] != pin["size"] or before[2] > 16 * 1024 * 1024:
        raise LegacyMonthlyPrefixError("repaired rolling seed inventory dimensions differ")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != pin["sha256"] or _signature(path) != before:
        raise LegacyMonthlyPrefixError("repaired rolling seed inventory bytes differ")
    if json.loads(raw).get("schema_version") != "qe_factor_component_pin_inventory_v1":
        raise LegacyMonthlyPrefixError("repaired rolling seed inventory schema differs")
    return raw


def restore_legacy_factor_state(
    prefix: LegacyMonthlyPrefix, *, instruments: Sequence[str], before: date,
    denominators: Mapping[str, float], security_identity: SecuritySourceIdentityManifest,
    raw_adj_reader: Callable[[pd.DataFrame], pd.DataFrame], max_rows: int = 100_000,
    checkpoint: Callable[[], None] = lambda: None,
    progress: Callable[[Mapping[str, Any]], None] = lambda _value: None,
    dataset_prefixes: Mapping[str, LegacyMonthlyPrefix] | None = None,
) -> tuple[RollingFactorState, Mapping[str, tuple[float, float]], list[Mapping[str, Any]]]:
    """Restore bounded native aggregate tails using sealed raw anchor facts.

    The stored normalized factor is NOT an upstream adj_factor numerator.
    Rebase price seeds before rolling calculations, using the same scalar as
    the inherited aggregate writer. Never multiply an old factor by the new
    denominator and pretend the resulting number was a source observation.
    """
    pin = prefix.manifest["components"]["factor_content_manifest"]
    inventory_path = prefix.root / _relative(pin["path"])
    _plain_chain(inventory_path)
    signature = _signature(inventory_path)
    if signature[2] != pin["size"] or signature[2] > 16 * 1024 * 1024:
        raise LegacyMonthlyPrefixError("legacy factor seed inventory dimensions differ")
    raw = inventory_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != pin["sha256"] or _signature(inventory_path) != signature:
        raise LegacyMonthlyPrefixError("legacy factor seed inventory bytes differ")
    inventory = json.loads(raw.decode("utf-8"))
    if inventory.get("schema_version") != "qe_factor_component_pin_inventory_v1" or not isinstance(inventory.get("files"), list):
        raise LegacyMonthlyPrefixError("legacy factor seed inventory schema differs")
    factor_root = _relative(prefix.manifest["components"]["factor_meta"]["path"]).parent

    def tail(dataset: str, count: int, codes: Sequence[str]) -> pd.DataFrame:
        physical = (dataset_prefixes or {}).get(dataset, prefix)
        if physical is not prefix:
            pin = physical.manifest["components"]["factor_content_manifest"]
            inventory_raw = _read_prefix_inventory(physical, pin)
            local_inventory = json.loads(inventory_raw)
            local_root = _relative(physical.manifest["components"]["factor_meta"]["path"]).parent
        else:
            local_inventory, local_root = inventory, factor_root
        relative = local_root / f"{dataset}.h5"
        pins = [item for item in local_inventory["files"] if isinstance(item, Mapping) and item.get("path") == relative.as_posix()]
        if len(pins) != 1:
            raise LegacyMonthlyPrefixError("legacy factor seed aggregate pin is missing/ambiguous")
        path = physical.root / relative
        _plain_chain(path)
        ensure_sha256(pins[0].get("sha256"), field="inherited factor seed")
        if _signature(path)[2] != pins[0].get("size"):
            raise LegacyMonthlyPrefixError("legacy factor seed aggregate size differs")
        return read_legacy_factor_tail(path, instruments=codes, rows_per_instrument=count,
            before=before, max_rows=max_rows, checkpoint=checkpoint, progress=progress)

    price = tail("daily_pv", 19, instruments)
    latest = price.groupby(level="instrument", sort=False).tail(1)
    adj = raw_adj_reader(latest)
    required = {"ts_code", "trade_date", "adj_factor"}
    if not required.issubset(adj.columns):
        raise LegacyMonthlyPrefixError("legacy factor raw QFQ anchor fields are missing")
    adj = adj.loc[:, sorted(required)].copy()
    adj["trade_date"] = pd.to_datetime(adj["trade_date"]).dt.date
    if adj.duplicated(["ts_code", "trade_date"]).any():
        raise LegacyMonthlyPrefixError("legacy factor raw QFQ anchor key is duplicated")
    values = {(row.ts_code, row.trade_date): float(row.adj_factor) for row in adj.itertuples(index=False)}
    expected = {(str(code), pd.Timestamp(stamp).date()) for stamp, code in latest.index}
    if set(values) != expected:
        raise LegacyMonthlyPrefixError("legacy factor raw QFQ anchor keys differ")
    bases: dict[str, tuple[float, float]] = {}
    boundaries: list[Mapping[str, Any]] = []
    for (stamp, code), row in latest.iterrows():
        raw_factor = values[(str(code), pd.Timestamp(stamp).date())]
        normalized = float(row["factor"])
        denominator = float(denominators.get(str(code), float("nan")))
        if any(not math.isfinite(value) or value <= 0 for value in (raw_factor, normalized, denominator)):
            raise LegacyMonthlyPrefixError("legacy factor raw QFQ anchor value is invalid")
        old = raw_factor / normalized
        # Compare in the existing file's actual precision.
        dtype = price["factor"].dtype
        if np.asarray(raw_factor / denominator, dtype=dtype).item() != normalized:
            bases[str(code)] = (old, denominator)
        boundaries.append({"instrument": str(code), "trade_date": pd.Timestamp(stamp).date().isoformat(),
                           "normalized_factor": normalized, "source_adj_factor": raw_factor,
                           "effective_old_denominator": old, "target_denominator": denominator})
    price = price.copy()
    for code, (old, new) in bases.items():
        selected = price.index.get_level_values("instrument") == code
        scalar = old / new
        if not 1e-20 <= scalar <= 1e20:
            raise LegacyMonthlyPrefixError("legacy factor seed QFQ scalar is unsafe")
        before_values = price.loc[selected, ["open", "high", "low", "close", "factor", "volume"]].to_numpy().copy()
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            price.loc[selected, ["open", "high", "low", "close", "factor"]] *= scalar
            price.loc[selected, "volume"] /= scalar
        after_values = price.loc[selected, ["open", "high", "low", "close", "factor", "volume"]].to_numpy()
        if np.any(np.isfinite(before_values) & ~np.isfinite(after_values)):
            raise LegacyMonthlyPrefixError("legacy factor seed basis conversion creates non-finite values")
    source_codes = security_identity.query_source_codes(instruments, date.min, before - date.resolution, MONEYFLOW_DATASET)
    moneyflow = tail("moneyflow", 19, source_codes)
    if not moneyflow.empty:
        moneyflow = _canonicalize_moneyflow_artifact_frame(moneyflow, identity=security_identity, requested=set(instruments))
        moneyflow = moneyflow.sort_index().groupby(level="instrument", sort=False).tail(19)
    pv = price.reindex(moneyflow.index).dropna(how="all") if not moneyflow.empty else pd.DataFrame()
    slow = {dataset: tail(dataset, 1, instruments) for dataset in ("bak_basic", "cyq_perf", "sector_data", "margin_detail")}
    return RollingFactorState(price.groupby(level="instrument", sort=False).tail(10), moneyflow, pv,
                              adj.sort_values(["ts_code", "trade_date"]), slow), bases, boundaries


def read_legacy_factor_tail(
    path: Path, *, instruments: Sequence[str], rows_per_instrument: int,
    before: date, max_rows: int = 100_000,
    checkpoint: Callable[[], None] = lambda: None,
    progress: Callable[[Mapping[str, Any]], None] = lambda _value: None,
) -> pd.DataFrame:
    """Read real rolling seeds, with indexed sparse-tail lookups when needed.

    No absent key is synthesized. HDF files without an instrument index use a
    bounded reverse scan only if necessary, not an all-partition re-audit.
    """
    if type(max_rows) is not int or not 0 < max_rows <= 100_000 or type(rows_per_instrument) is not int or not 0 < rows_per_instrument <= 19:
        raise LegacyMonthlyPrefixError("legacy rolling-tail row bound is invalid")
    if len(set(instruments)) != len(instruments) or any(not isinstance(code, str) or re.fullmatch(r"[0-9]{6}\.(SH|SZ)", code) is None for code in instruments):
        raise LegacyMonthlyPrefixError("legacy rolling-tail instruments are invalid")
    _plain_chain(path)
    signature = _signature(path)
    physical_rows = requests = 0
    last_pulse = time.monotonic()

    def pulse(*, force: bool = False):
        nonlocal last_pulse
        if force or time.monotonic() - last_pulse >= 2:
            checkpoint()
            progress({"phase": "FACTOR_ROLLING_SEED", "dataset": path.stem,
                      "physical_rows_read": physical_rows, "indexed_tail_requests": requests})
            last_pulse = time.monotonic()

    pulse(force=True)
    with pd.HDFStore(path, "r") as store:
        storer = store.get_storer("data")
        if not storer.is_table:
            raise LegacyMonthlyPrefixError("legacy rolling seeds require table H5")
        current = store.select("data", start=0, stop=0)
        if tuple(current.index.names) != ("datetime", "instrument"):
            raise LegacyMonthlyPrefixError("legacy rolling-tail index identity differs")
        pending = set(instruments)
        end = int(storer.nrows)
        blocks = 0
        while pending and end:
            pulse()
            start = max(0, end - max_rows)
            frame = store.select("data", start=start, stop=end)
            physical_rows += len(frame)
            selected = frame.loc[frame.index.get_level_values("instrument").isin(pending)]
            selected = selected.loc[selected.index.get_level_values("datetime") < pd.Timestamp(before)]
            if not selected.empty:
                current = pd.concat([selected, current]).sort_index().groupby(level="instrument", sort=False).tail(rows_per_instrument)
                counts = current.groupby(level="instrument").size()
                pending.difference_update(counts.index[counts >= rows_per_instrument])
            end = start
            blocks += 1
            if blocks == 2 and pending and "instrument" in storer.table.colindexes:
                for code in sorted(pending):
                    pulse()
                    coordinates = store.select_as_coordinates("data", where=[
                        f"instrument == {code!r}", f"datetime < {before.isoformat()!r}",
                    ])
                    requests += 1
                    if len(coordinates):
                        coordinates = coordinates[-rows_per_instrument:]
                        selected = store.select("data", where=coordinates)
                        physical_rows += len(selected)
                        current = current.loc[current.index.get_level_values("instrument") != code]
                        current = pd.concat([current, selected]).sort_index()
                # Empty coordinate sets mean no stored seed, not a zero row.
                pending.clear()
        if _signature(path) != signature:
            raise LegacyMonthlyPrefixError("legacy rolling-tail file changed during read")
    pulse(force=True)
    return current


def append_legacy_factor_aggregate(
    prefix: LegacyMonthlyPrefix, *, dataset: str, source_root: Path,
    chunks: Sequence[SealedFactorChunk], target_root: Path, target_cutoff: date,
    source_receipt_sha256: str, qfq_basis_changes: Mapping[str, tuple[float, float]] | None = None,
    max_rows: int = 100_000, checkpoint: Callable[[], None] = lambda: None,
    progress: Callable[[Mapping[str, Any]], None] = lambda _value: None,
    exact_null_repairs: Sequence[Mapping[str, Any]] = (),
    logical_predecessor_manifest_sha256: str | None = None,
) -> Mapping[str, Any]:
    """Publish one new private aggregate; release readiness stays with caller."""
    if dataset not in {*FACTOR_H5_SCHEMAS, "static_factors"} or type(max_rows) is not int or not 0 < max_rows <= 100_000:
        raise LegacyMonthlyPrefixError("legacy factor dataset/row bound is invalid")
    if exact_null_repairs and dataset != "daily_basic":
        raise LegacyMonthlyPrefixError("exact field repairs apply only to daily_basic")
    ensure_sha256(source_receipt_sha256, field="monthly factor source receipt")
    logical_predecessor = ensure_sha256(logical_predecessor_manifest_sha256 or prefix.manifest_sha256,
                                        field="logical monthly predecessor")
    month_start = target_cutoff.replace(day=1)
    if prefix.cutoff != month_start - date.resolution:
        raise LegacyMonthlyPrefixError("legacy factor cutoff is not the preceding month")
    _plain_chain(target_root)
    _plain_chain(source_root)
    output_root = Path(target_root).resolve()
    immutable_root = prefix.root.resolve()
    input_root = Path(source_root).resolve()
    if output_root.is_relative_to(immutable_root) or immutable_root.is_relative_to(output_root):
        raise LegacyMonthlyPrefixError("legacy factor target overlaps predecessor")
    if output_root.is_relative_to(input_root) or input_root.is_relative_to(output_root):
        raise LegacyMonthlyPrefixError("legacy factor target overlaps sealed month source")
    pins = prefix.manifest["components"]
    pin = pins.get("factor_content_manifest")
    meta = pins.get("factor_meta")
    if not isinstance(pin, Mapping) or not isinstance(meta, Mapping):
        raise LegacyMonthlyPrefixError("legacy factor manifest pins are missing")
    inventory_path = prefix.root / _relative(pin.get("path"))
    _plain_chain(inventory_path)
    before_inventory = _signature(inventory_path)
    if before_inventory[2] != pin.get("size") or before_inventory[2] > 16 * 1024 * 1024:
        raise LegacyMonthlyPrefixError("legacy factor inventory dimensions differ")
    raw = inventory_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != pin.get("sha256") or _signature(inventory_path) != before_inventory:
        raise LegacyMonthlyPrefixError("legacy factor inventory bytes differ")
    inventory = json.loads(raw.decode("utf-8"))
    if inventory.get("schema_version") != "qe_factor_component_pin_inventory_v1" or not isinstance(inventory.get("files"), list):
        raise LegacyMonthlyPrefixError("legacy factor inventory schema differs")
    extension = "parquet" if dataset == "static_factors" else "h5"
    relative = _relative(meta.get("path")).parent / f"{dataset}.{extension}"
    entries = [item for item in inventory["files"] if isinstance(item, Mapping) and item.get("path") == relative.as_posix()]
    if len(entries) != 1:
        raise LegacyMonthlyPrefixError("legacy factor aggregate pin is missing/ambiguous")
    old_path = prefix.root / relative
    _plain_chain(old_path)
    before = _signature(old_path)
    old_pin = entries[0]
    ensure_sha256(old_pin.get("sha256"), field="inherited factor aggregate")
    if before[2] != old_pin.get("size"):
        raise LegacyMonthlyPrefixError("legacy factor aggregate size differs")
    columns = tuple(STATIC_ORDERED_COLUMNS if dataset == "static_factors" else FACTOR_H5_SCHEMAS[dataset])
    dtypes = STATIC_COLUMN_DTYPES if dataset == "static_factors" else FACTOR_H5_DTYPES[dataset]
    target = target_root / f"{dataset}.{extension}"
    if target.exists():
        raise LegacyMonthlyPrefixError("legacy factor target already exists")
    source_paths: list[Path] = []
    source_signatures: dict[Path, tuple[int, int, int, int]] = {}
    last_pulse = time.monotonic()
    inherited_rows = month_rows = inherited_bytes_copied = inherited_rows_serialized = 0
    exact_repair_readback = None

    def pulse(*, force: bool = False) -> None:
        nonlocal last_pulse
        if force or time.monotonic() - last_pulse >= 2:
            checkpoint()
            progress({"phase": "FACTOR_MONTH_APPEND", "dataset": dataset,
                      "inherited_rows_serialized": inherited_rows_serialized,
                      "inherited_bytes_copied": inherited_bytes_copied,
                      "month_rows_written": month_rows})
            last_pulse = time.monotonic()

    def digest(path: Path) -> str:
        value = hashlib.sha256()
        with path.open("rb") as reader:
            while block := reader.read(1024 * 1024):
                value.update(block)
                pulse()
        return value.hexdigest()

    for chunk in chunks:
        if chunk.dataset != dataset or chunk.ordered_columns != columns:
            raise LegacyMonthlyPrefixError("legacy factor month chunk identity differs")
        path = source_root / _relative(chunk.relative_path)
        _plain_chain(path)
        if path in source_signatures:
            raise LegacyMonthlyPrefixError("legacy factor month chunk is duplicated")
        source_signatures[path] = _signature(path)
        if digest(path) != chunk.sha256 or _signature(path) != source_signatures[path]:
            raise LegacyMonthlyPrefixError("legacy factor month chunk bytes differ")
        source_paths.append(path)
    if not source_paths:
        raise LegacyMonthlyPrefixError("legacy factor month chunk set is empty")
    scales: dict[str, float] = {}
    for code, pair in (qfq_basis_changes or {}).items():
        if len(pair) != 2 or any(not math.isfinite(float(value)) or float(value) <= 0 for value in pair):
            raise LegacyMonthlyPrefixError("legacy factor QFQ basis is invalid")
        scalar = float(pair[0]) / float(pair[1])
        if not 1e-20 <= scalar <= 1e20:
            raise LegacyMonthlyPrefixError("legacy factor QFQ scalar is unsafe")
        scales[code] = scalar
    previous_key = None
    storage_columns = columns
    storage_dtypes = dict(dtypes)

    def bind_storage_template(template: pd.DataFrame) -> None:
        nonlocal storage_columns, storage_dtypes
        actual_columns = tuple(template.columns)
        actual_types = {str(name): str(kind) for name, kind in template.dtypes.items()}
        # The frozen v15 price aggregate has genuine float64 storage; sector
        # columns are reordered. Preserve both, without narrowing old facts.
        legacy_price_types = dataset == "daily_pv" and actual_types == dict.fromkeys(columns, "float64")
        if len(actual_columns) != len(columns) or set(actual_columns) != set(columns) or (actual_types != dtypes and not legacy_price_types):
            raise LegacyMonthlyPrefixError("legacy factor storage schema differs")
        storage_columns, storage_dtypes = actual_columns, actual_types

    def scale_column(values: np.ndarray, scalar: np.ndarray, dtype: str) -> np.ndarray:
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            transformed = (values * scalar).astype(dtype)
        if np.any(np.isfinite(values) & ~np.isfinite(transformed)):
            raise LegacyMonthlyPrefixError("legacy factor basis conversion creates non-finite values")
        return transformed

    def month_frames():
        nonlocal previous_key, month_rows
        for frame in iter_parquet_frames(source_paths, max_rows=max_rows):
            pulse()
            if frame.empty:
                continue
            if tuple(frame.columns) != columns or {str(name): str(kind) for name, kind in frame.dtypes.items()} != dtypes:
                raise LegacyMonthlyPrefixError("legacy factor month schema/dtypes differ")
            if tuple(frame.index.names) != ("datetime", "instrument") or frame.index.has_duplicates or not frame.index.is_monotonic_increasing:
                raise LegacyMonthlyPrefixError("legacy factor month index is invalid")
            stamps = frame.index.get_level_values("datetime")
            if stamps.min().date() < month_start or stamps.max().date() > target_cutoff:
                raise LegacyMonthlyPrefixError("legacy factor month chunk contains dates outside month")
            first, last = frame.index[0], frame.index[-1]
            if previous_key is not None and first <= previous_key:
                raise LegacyMonthlyPrefixError("legacy factor month chunks overlap or are unordered")
            previous_key = last
            month_rows += len(frame)
            yield frame.loc[:, list(storage_columns)].astype(storage_dtypes)
        if month_rows != sum(chunk.rows for chunk in chunks):
            raise LegacyMonthlyPrefixError("legacy factor month row count differs")

    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{dataset}.", suffix=f".partial.{extension}", dir=target_root)
    os.close(descriptor)
    temporary = Path(temporary_name)
    pulse(force=True)
    try:
        if extension == "h5":
            with pd.HDFStore(old_path, "r") as inherited:
                storer = inherited.get_storer("data")
                if not storer.is_table:
                    raise LegacyMonthlyPrefixError("legacy factor H5 is not appendable table format")
                inherited_rows = int(storer.nrows)
                template = inherited.select("data", start=0, stop=1)
                bind_storage_template(template)
                if dataset != "daily_pv" or not scales:
                    with old_path.open("rb") as reader, temporary.open("wb") as writer:
                        while block := reader.read(1024 * 1024):
                            writer.write(block)
                            inherited_bytes_copied += len(block)
                            pulse()
                    if exact_null_repairs:
                        from .monthly_repair_inputs import apply_daily_basic_null_repairs
                        exact_repair_readback = apply_daily_basic_null_repairs(
                            temporary, entries=exact_null_repairs, checkpoint=checkpoint,
                        )
                    with pd.HDFStore(temporary, "a") as output:
                        for frame in month_frames():
                            output.append("data", frame, format="table", data_columns=["datetime", "instrument"], index=False)
                else:
                    with pd.HDFStore(temporary, "w") as output:
                        # Re-serialization is only basis conversion; no price,
                        # population, or other historical business QA occurs.
                        for frame in inherited.select("data", chunksize=max_rows):
                            pulse()
                            scalar = pd.Series(frame.index.get_level_values("instrument"), index=frame.index).map(scales).fillna(1.0).to_numpy()
                            frame = frame.copy()
                            for name in ("open", "high", "low", "close", "factor"):
                                frame[name] = scale_column(frame[name].to_numpy(), scalar, storage_dtypes[name])
                            frame["volume"] = scale_column(frame["volume"].to_numpy(), 1.0 / scalar, storage_dtypes["volume"])
                            output.append("data", frame, format="table", data_columns=["datetime", "instrument"], index=False, min_itemsize={"instrument": 16})
                            inherited_rows_serialized += len(frame)
                        for frame in month_frames():
                            output.append("data", frame, format="table", data_columns=["datetime", "instrument"], index=False, min_itemsize={"instrument": 16})
        else:
            inherited = pq.ParquetFile(old_path)
            bind_storage_template(pa.Table.from_batches([], schema=inherited.schema_arrow).to_pandas())
            writer = pq.ParquetWriter(temporary, inherited.schema_arrow)
            try:
                for batch in inherited.iter_batches(batch_size=max_rows):
                    pulse()
                    inherited_rows += batch.num_rows
                    writer.write_table(pa.Table.from_batches([batch]), row_group_size=max_rows)
                    inherited_rows_serialized += batch.num_rows
                for frame in month_frames():
                    table = pa.Table.from_pandas(frame, preserve_index=True)
                    if not table.schema.equals(inherited.schema_arrow, check_metadata=False):
                        raise LegacyMonthlyPrefixError("legacy static physical schema differs")
                    writer.write_table(table, row_group_size=max_rows)
            finally:
                writer.close()
                inherited.close()
        # Read back the serialized MONTH only. Existing H5 rows/Parquet groups
        # are skipped by physical row offset, not scanned for business QA.
        if extension == "h5":
            with pd.HDFStore(temporary, "r") as output:
                if int(output.get_storer("data").nrows) != inherited_rows + month_rows:
                    raise LegacyMonthlyPrefixError("legacy factor appended physical row count differs")
                offset = inherited_rows
                for expected in iter_parquet_frames(source_paths, max_rows=max_rows):
                    if expected.empty:
                        continue
                    pulse()
                    actual = output.select("data", start=offset, stop=offset + len(expected))
                    try:
                        pd.testing.assert_frame_equal(actual, expected.loc[:, list(storage_columns)].astype(storage_dtypes), check_exact=True)
                    except AssertionError as exc:
                        raise LegacyMonthlyPrefixError("legacy factor month readback differs from sealed facts") from exc
                    offset += len(expected)
        else:
            with pq.ParquetFile(temporary) as output:
                if output.metadata.num_rows != inherited_rows + month_rows:
                    raise LegacyMonthlyPrefixError("legacy static appended physical row count differs")
                groups = []
                offset = 0
                skipped = 0
                for number in range(output.num_row_groups):
                    count = output.metadata.row_group(number).num_rows
                    if offset + count > inherited_rows:
                        if not groups:
                            skipped = inherited_rows - offset
                        groups.append(number)
                    offset += count
                actual = output.read_row_groups(groups).to_pandas().iloc[skipped:] if groups else pd.DataFrame()
                offset = 0
                for expected in iter_parquet_frames(source_paths, max_rows=max_rows):
                    if expected.empty:
                        continue
                    pulse()
                    try:
                        pd.testing.assert_frame_equal(actual.iloc[offset:offset + len(expected)], expected.loc[:, list(storage_columns)].astype(storage_dtypes), check_exact=True)
                    except AssertionError as exc:
                        raise LegacyMonthlyPrefixError("legacy static month readback differs from sealed facts") from exc
                    offset += len(expected)
        if _signature(old_path) != before or any(_signature(path) != value for path, value in source_signatures.items()):
            raise LegacyMonthlyPrefixError("legacy factor inputs changed during append")
        result = {
            "schema_version": "aistock_monthly_legacy_factor_append_v1", "dataset": dataset,
            "predecessor_manifest_sha256": logical_predecessor, "source_bundle_sha256": source_receipt_sha256,
            "physical_prefix_manifest_sha256": prefix.manifest_sha256,
            "inherited_sha256": old_pin["sha256"], "inherited_rows": inherited_rows,
            "qfq_basis_changes": {code: list(pair) for code, pair in sorted((qfq_basis_changes or {}).items())}
                if dataset == "daily_pv" else {},
            "month_rows": month_rows, "cutoff": target_cutoff.isoformat(),
            "schema_columns": list(columns), "sha256": digest(temporary), "size": temporary.stat().st_size,
            "storage_columns": list(storage_columns), "storage_dtypes": storage_dtypes,
            "historical_business_audit_performed": False, "publication_allowed": False,
            "validation_scope": "month_delta", "month_output_values_verified": True,
            "database_read": False, "database_write": False,
            "exact_field_repair_readback": exact_repair_readback,
        }
        # Hardlink is an atomic create-exclusive publish, not os.replace.
        os.link(temporary, target)
        result["target_signature"] = list(_signature(target))
        pulse(force=True)
        return result
    finally:
        temporary.unlink(missing_ok=True)


def seal_factor_month_metadata(
    *, target_root: Path, source_root: Path, chunks: Sequence[SealedFactorChunk],
    identity: SecuritySourceIdentityManifest, moneyflow_receipt: Mapping[str, Any],
    predecessor_manifest_sha256: str, source_bundle_sha256: str, cutoff: date,
    max_rows: int, checkpoint: Callable[[], None] = lambda: None,
) -> Mapping[str, Any]:
    """Use the shared alias auditor on only the genuinely produced month.

    The combined moneyflow file is already verified by its writer. Never feed
    that whole historical file to a second alias/content scan.
    """
    from .factor_materializer import _audit_moneyflow_alias_coverage
    from .canonical import canonical_json_bytes

    target_root = Path(target_root).absolute()
    _plain_chain(target_root)
    identity_path = identity.source_path
    _plain_chain(identity_path)
    before = _signature(identity_path)
    raw = identity_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != identity.file_sha256 or _signature(identity_path) != before:
        raise LegacyMonthlyPrefixError("factor security source identity changed before sealing")
    identity_target = target_root / "security_source_identity.json"
    with identity_target.open("xb") as writer:
        writer.write(raw)
    month_moneyflow = source_root / ".alias-month-readback.h5"
    daily_paths = tuple(source_root / chunk.relative_path for chunk in chunks if chunk.dataset == "daily_pv")
    moneyflow_paths = tuple(source_root / chunk.relative_path for chunk in chunks if chunk.dataset == "moneyflow")
    if not daily_paths or not moneyflow_paths or month_moneyflow.exists():
        raise LegacyMonthlyPrefixError("factor alias month inputs are incomplete or conflicting")
    try:
        with pd.HDFStore(month_moneyflow, "w") as output:
            for frame in iter_parquet_frames(moneyflow_paths, max_rows=max_rows):
                checkpoint()
                if not frame.empty:
                    dates = pd.to_datetime(frame.index.get_level_values("datetime")).date
                    if min(dates) < cutoff.replace(day=1) or max(dates) > cutoff:
                        raise LegacyMonthlyPrefixError("factor alias audit received historical rows")
                    output.append("data", frame, format="table", index=False)
        with pd.HDFStore(month_moneyflow, "r") as output:
            if "data" not in output:
                raise LegacyMonthlyPrefixError("factor alias month contains no moneyflow facts")
        alias = _audit_moneyflow_alias_coverage(
            daily_paths=daily_paths, paths=moneyflow_paths, moneyflow_h5=month_moneyflow,
            identity=identity, max_rows=max_rows,
        )
        alias = {**alias, "validation_scope": "month_delta", "month_start": cutoff.replace(day=1).isoformat(),
            "cutoff": cutoff.isoformat(), "predecessor_manifest_sha256": predecessor_manifest_sha256,
            "source_bundle_sha256": source_bundle_sha256, "month_output_sha256": alias["moneyflow_sha256"],
            "moneyflow_sha256": moneyflow_receipt["sha256"], "historical_business_audit_performed": False}
        alias_target = target_root / "moneyflow_alias_coverage_v1.json"
        with alias_target.open("xb") as writer:
            writer.write(canonical_json_bytes(alias) + b"\n")
        return {"alias_coverage": alias, "metadata_files": [{"relative_path": path.name,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "size": path.stat().st_size,
            "target_signature": list(_signature(path))} for path in (identity_target, alias_target)]}
    finally:
        month_moneyflow.unlink(missing_ok=True)
