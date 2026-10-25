"""Bounded shared sector facts without the deferred financing factor bundle.

Only the existing frozen source, factor schema/unit normalizer and table-H5
writer are used. This is a private preparation primitive, not a candidate or
a complete factor component; no SOURCE/BUILD PASS can be produced here.
"""

from __future__ import annotations

from itertools import islice
from pathlib import Path
from typing import Any, Callable, Mapping

import pandas as pd
import numpy as np

from .artifact_ready_source import _SealedSnapshotView
from .canonical import digest_named_fields, ensure_sha256
from .cas_store import CASStore
from .external_ordered_rows import ExternalOrderedRowsError, OrderedMappingPartition, external_merge_ordered_rows
from .factor_materializer import FACTOR_H5_DTYPES, FACTOR_H5_SCHEMAS, FactorMaterializationError, _normalize_aux_frame
from .monthly_component_preparation import ComponentPreparationError, _plain_path
from .monthly_preparation_artifacts import require_preparation_domain_audit
from .monthly_preparation_source import PreparationSourceSnapshot
from .streaming_artifacts import (
    MAX_ROW_GROUP_ROWS,
    finalize_h5_from_parquet_chunks,
    iter_parquet_frames,
    write_frame_parquet_atomic,
)


SECTOR_FACTS_SCHEMA = "aistock_monthly_private_sector_facts_v1"
_REQUIRED_GATES = frozenset({"calendar_lifecycle", "sector_authority", "pit_stock_pools"})


def _rows(path: Path, bound: int):
    for frame in iter_parquet_frames((path,), max_rows=bound):
        frame = frame.reset_index().rename(columns={"datetime": "trade_date", "instrument": "ts_code"})
        for values in frame.itertuples(index=False, name=None):
            row = dict(zip(frame.columns, values, strict=True))
            row["trade_date"] = pd.Timestamp(row["trade_date"]).date().isoformat()
            for key, value in tuple(row.items()):
                if pd.isna(value):
                    row[key] = None
            yield row


def materialize_preparation_sector_facts(
    *,
    cas: CASStore,
    snapshot: PreparationSourceSnapshot,
    audit: Mapping[str, Any],
    profile: Any,
    output_root: Path,
    max_rows_in_memory: int = 100_000,
    checkpoint: Callable[[], None] = lambda: None,
) -> Mapping[str, Any]:
    """Generate the canonical sector H5 from healthy private frozen facts.

    Partition order need not be date-first. Bounded sorted runs are externally
    merged; neither raw nor normalized whole-market history is resident.
    Quote NA is retained verbatim (validated by the official source domain
    audit); its presence never clears unrelated real moneyflow fields.
    """
    require_preparation_domain_audit(profile=profile, snapshot=snapshot, audit=audit, required_gates=_REQUIRED_GATES)
    if type(max_rows_in_memory) is not int or not 1 <= max_rows_in_memory <= MAX_ROW_GROUP_ROWS:
        raise ComponentPreparationError("private sector memory bound differs")
    parent = _plain_path(output_root.parent, root=output_root.parent, directory=True)
    if output_root.parent != parent or output_root.exists():
        raise ComponentPreparationError("private sector output already exists or is not absolute")
    view = _SealedSnapshotView(cas, snapshot)
    descriptors = view.descriptors("sector_data")
    if not descriptors:
        raise ComponentPreparationError("private sector frozen inputs are missing")
    output_root.mkdir(exist_ok=False)
    _plain_path(output_root, root=parent, directory=True)
    work = output_root / ".sector-work"
    work.mkdir(exist_ok=False)
    owned: list[Path] = []
    sorted_runs: list[Path] = []
    normalized_runs: list[Path] = []
    maximum = 0
    previous = None
    columns = FACTOR_H5_SCHEMAS["sector_data"]

    def normalize(batch):
        nonlocal maximum
        maximum = max(maximum, len(batch))
        raw = pd.DataFrame(batch)
        if not {"ts_code", "trade_date", *columns} <= set(raw.columns):
            raise ComponentPreparationError("private sector canonical schema is incomplete")
        days = pd.to_datetime(raw["trade_date"], errors="raise").dt.date
        ids = pd.to_numeric(raw["l2_code_id"], errors="raise")
        if (
            ids.isna().any()
            or ((ids % 1) != 0).any()
            or (ids < -1).any()
            or (ids > 32767).any()
            or any(day < profile.start_date or day > snapshot.official_cutoff for day in days)
        ):
            raise ComponentPreparationError("private sector dates or code IDs differ")
        try:
            result = _normalize_aux_frame(raw, dataset="sector_data")
        except (ValueError, FactorMaterializationError) as exc:
            raise ComponentPreparationError("private sector facts fail canonical normalization") from exc
        indexed = raw.assign(
            datetime=pd.to_datetime(raw["trade_date"]), instrument=raw["ts_code"].astype(str).str.upper()
        ).set_index(["datetime", "instrument"])
        for column in columns:
            original = pd.to_numeric(indexed[column], errors="raise").reindex(result.index)
            if (np.isfinite(original.to_numpy(dtype=float)) & ~np.isfinite(result[column].to_numpy(dtype=float))).any():
                raise ComponentPreparationError("private sector finite facts overflow canonical storage")
        return result

    try:
        for descriptor in descriptors:
            rows = iter(view.iter_partition_rows(descriptor))
            try:
                while batch := list(islice(rows, max_rows_in_memory)):
                    checkpoint()
                    normalized = normalize(batch)
                    # Preserve formal canonical values and dtypes in a bounded
                    # sorted run, then recreate source keys for the merger.
                    path = work / f"raw-{len(sorted_runs):06d}.parquet"
                    write_frame_parquet_atomic(normalized, path, row_group_size=max_rows_in_memory)
                    owned.append(path)
                    sorted_runs.append(path)
            finally:
                close = getattr(rows, "close", None)
                if close is not None:
                    close()
        if not sorted_runs:
            raise ComponentPreparationError("private sector facts are empty")
        streams = [OrderedMappingPartition(path.name, _rows(path, max_rows_in_memory)) for path in sorted_runs]
        ordered = external_merge_ordered_rows(
            streams,
            key=lambda row: (str(row["trade_date"]), str(row["ts_code"])),
            spool_root=work,
            max_open_streams=2,
            checkpoint=checkpoint,
        )
        try:
            while batch := list(islice(ordered, max_rows_in_memory)):
                checkpoint()
                for row in batch:
                    key = (str(row["trade_date"]), str(row["ts_code"]))
                    if previous is not None and key <= previous:
                        raise ComponentPreparationError("private sector keys are duplicated or unordered")
                    previous = key
                frame = normalize(batch)
                path = work / f"h5-{len(normalized_runs):06d}.parquet"
                write_frame_parquet_atomic(frame, path, row_group_size=max_rows_in_memory)
                owned.append(path)
                normalized_runs.append(path)
        finally:
            ordered.close()
        result = finalize_h5_from_parquet_chunks(
            normalized_runs,
            output_root / "sector_data.h5",
            expected_columns=columns,
            dtype_overrides=FACTOR_H5_DTYPES["sector_data"],
            max_rows_in_memory=max_rows_in_memory,
        )
        path = _plain_path(output_root / "sector_data.h5", root=output_root)
        # The official writer already hashes its new file. Avoid another
        # full content pass here; final component sealing/adoption still
        # performs the normal independent pinned-file checks.
        ref = {
            "path": path.name,
            "sha256": ensure_sha256(str(result["sha256"]), field="sector H5"),
            "size": path.stat().st_size,
        }
        body = {
            "schema_version": SECTOR_FACTS_SCHEMA,
            "operation_id": snapshot.operation_id,
            "cutoff": snapshot.official_cutoff.isoformat(),
            "source_manifest_ref": snapshot.source_manifest_ref.as_dict(),
            "pit_snapshot_digest": snapshot.pit_snapshot_digest,
            "sector_data": ref,
            "row_count": result["rows"],
            "max_batch_rows": maximum,
            "bounded_merge_fan_in": 2,
            "whole_history_materialized": False,
            "publication_allowed": False,
            "consistent_input_set_complete": False,
            "database_write_performed": False,
        }
        return {**body, "canonical_digest": digest_named_fields(SECTOR_FACTS_SCHEMA, body)}
    except ExternalOrderedRowsError as exc:
        raise ComponentPreparationError("private sector keys are duplicated or unordered") from exc
    finally:
        # Only exact scratch files created by this invocation are removed.
        # Interrupted output dirs remain private and cannot be adopted.
        for path in owned:
            path.unlink(missing_ok=True)
        if work.exists() and not any(work.iterdir()):
            work.rmdir()
