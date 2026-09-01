"""Bounded, resumable materialization of the QE factor bundle.

The build worker never queries a source database from this module.  Resolution
must first freeze artifact-ready, date-partitioned Parquet chunks.  This module
verifies those immutable chunks and streams them into candidate-local HDF5 and
Parquet aggregates.  No full-market frame is retained across chunks.
"""

from __future__ import annotations

import json
import hashlib
import math
import os
import re
import shutil
import stat
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backend.data_service.moneyflow_contract import (
    MONEYFLOW_FIELD_MAP,
    assert_moneyflow_frame_parity,
    derive_moneyflow_factors,
    moneyflow_unit_contract_receipt,
    normalize_tushare_moneyflow_units,
)

from .canonical import digest_named_fields, ensure_sha256
from .canonical_stock_transformer import QfqDenominatorAuthority
from .errors import DatasetReleaseError
from .pit import FrozenPitSnapshot, filter_frame_to_pit_spans
from .streaming_artifacts import (
    ArtifactChunkTooLarge,
    ArtifactSchemaDrift,
    finalize_h5_from_parquet_chunks,
    finalize_parquet_chunks,
    iter_hdf_frames,
    iter_parquet_frames,
    sha256_file,
    write_frame_parquet_atomic,
)
from .static_schema import (
    STATIC_COLUMN_DTYPES,
    STATIC_ORDERED_COLUMNS,
    STATIC_SCHEMA_VERSION,
    STATIC_SECTOR_COLUMNS,
    static_schema_digest,
)


FACTOR_MATERIALIZATION_SCHEMA = "dataset_release_factor_materialization_v1"
FACTOR_CHECKPOINT_SCHEMA = "dataset_release_factor_checkpoint_v1"
FACTOR_H5_DATASETS: tuple[str, ...] = (
    "daily_pv",
    "daily_basic",
    "moneyflow",
    "bak_basic",
    "cyq_perf",
    "sector_data",
    "margin_detail",
)
STATIC_DATASET = "static_factors"
_SAFE_PARTITION = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.=-]{0,159}\Z")
_STOCK_CODE = re.compile(r"[0-9]{6}\.(?:SH|SZ)\Z")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_UNKNOWN_L2_CODE_ID = -1

_AUX_RENAMES: dict[str, Mapping[str, str]] = {
    "daily_basic": {
        "close": "db_close",
        "turnover_rate": "db_turnover_rate",
        "turnover_rate_f": "db_turnover_rate_f",
        "volume_ratio": "db_volume_ratio",
        "pe": "db_pe",
        "pe_ttm": "db_pe_ttm",
        "pb": "db_pb",
        "ps": "db_ps",
        "ps_ttm": "db_ps_ttm",
        "dv_ratio": "db_dv_ratio",
        "dv_ttm": "db_dv_ttm",
        "total_share": "db_total_share",
        "float_share": "db_float_share",
        "free_share": "db_free_share",
        "total_mv": "db_total_mv",
        "circ_mv": "db_circ_mv",
    },
    "bak_basic": {
        "pe_dyn": "bb_pe_dyn",
        "total_assets": "bb_total_assets",
        "liquid_assets": "bb_liquid_assets",
        "fixed_assets": "bb_fixed_assets",
        "reserved": "bb_reserved",
        "reserved_pershare": "bb_reserved_pershare",
        "eps": "bb_eps",
        "bvps": "bb_bvps",
        "undp": "bb_undp",
        "per_undp": "bb_per_undp",
        "rev_yoy": "bb_rev_yoy",
        "profit_yoy": "bb_profit_yoy",
        "gpr": "bb_gpr",
        "npr": "bb_npr",
        "holder_num": "bb_holder_num",
    },
    "cyq_perf": {
        "his_low": "cp_his_low",
        "his_high": "cp_his_high",
        "cost_5pct": "cp_cost_5pct",
        "cost_15pct": "cp_cost_15pct",
        "cost_50pct": "cp_cost_50pct",
        "cost_85pct": "cp_cost_85pct",
        "cost_95pct": "cp_cost_95pct",
        "weight_avg": "cp_weight_avg",
        "winner_rate": "cp_winner_rate",
    },
    "margin_detail": {
        "rzye": "md_rzye",
        "rqye": "md_rqye",
        "rzmre": "md_rzmre",
        "rqyl": "md_rqyl",
        "rzche": "md_rzche",
        "rqchl": "md_rqchl",
        "rqmcl": "md_rqmcl",
        "rzrqye": "md_rzrqye",
    },
}

FACTOR_H5_SCHEMA_VERSION = "qe_factor_h5_schemas_v1"
FACTOR_H5_SCHEMAS: dict[str, tuple[str, ...]] = {
    "daily_pv": ("open", "high", "low", "close", "volume", "amount", "factor"),
    "daily_basic": tuple(_AUX_RENAMES["daily_basic"].values()),
    "moneyflow": tuple(MONEYFLOW_FIELD_MAP.values()),
    "bak_basic": tuple(_AUX_RENAMES["bak_basic"].values()),
    "cyq_perf": tuple(_AUX_RENAMES["cyq_perf"].values()),
    "sector_data": tuple(STATIC_SECTOR_COLUMNS),
    "margin_detail": tuple(_AUX_RENAMES["margin_detail"].values()),
}
if tuple(FACTOR_H5_SCHEMAS) != FACTOR_H5_DATASETS:
    raise RuntimeError("factor H5 schema authority does not cover ordered datasets")
FACTOR_H5_DTYPES: dict[str, dict[str, str]] = {
    dataset: {column: "int16" if column == "l2_code_id" else "float32" for column in columns}
    for dataset, columns in FACTOR_H5_SCHEMAS.items()
}
FACTOR_H5_DENSITY_CONTRACTS: dict[str, str] = {
    dataset: ("dense_exact_pit_trading_day_keys_v1" if dataset == "daily_pv" else "sparse_unique_subset_of_pit_keys_v1")
    for dataset in FACTOR_H5_DATASETS
}
FACTOR_SOURCE_SCHEMAS: dict[str, tuple[str, ...]] = {
    "daily_basic": tuple(_AUX_RENAMES["daily_basic"]),
    "moneyflow": tuple(MONEYFLOW_FIELD_MAP),
    "bak_basic": tuple(_AUX_RENAMES["bak_basic"]),
    "cyq_perf": tuple(_AUX_RENAMES["cyq_perf"]),
    "sector_data": tuple(STATIC_SECTOR_COLUMNS),
    "margin_detail": tuple(_AUX_RENAMES["margin_detail"]),
}


class FactorMaterializationError(DatasetReleaseError):
    code = "DATASET_RELEASE_FACTOR_MATERIALIZATION_INVALID"


class FactorCheckpointConflict(FactorMaterializationError):
    code = "BLOCKED_FACTOR_CHECKPOINT_CONFLICT"


@dataclass(frozen=True, slots=True)
class SealedFactorChunk:
    dataset: str
    partition_key: str
    relative_path: str
    sha256: str
    rows: int
    ordered_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.dataset not in {*FACTOR_H5_DATASETS, STATIC_DATASET}:
            raise FactorMaterializationError(f"unknown factor dataset: {self.dataset}")
        if _SAFE_PARTITION.fullmatch(self.partition_key) is None:
            raise FactorMaterializationError("factor partition key is unsafe")
        _safe_relative(self.relative_path)
        ensure_sha256(self.sha256, field="factor_chunk.sha256")
        if type(self.rows) is not int or self.rows < 0:
            raise FactorMaterializationError("factor chunk rows must be non-negative")
        if not self.ordered_columns or len(self.ordered_columns) != len(set(self.ordered_columns)):
            raise FactorMaterializationError("factor chunk ordered columns must be non-empty and unique")

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "partition_key": self.partition_key,
            "relative_path": _safe_relative(self.relative_path),
            "sha256": self.sha256,
            "rows": self.rows,
            "ordered_columns": list(self.ordered_columns),
        }


@dataclass(frozen=True, slots=True)
class FactorMaterializationSpec:
    source_root: Path
    staging_root: Path
    chunks: tuple[SealedFactorChunk, ...]
    static_ordered_columns: tuple[str, ...]
    row_group_rows: int

    def __post_init__(self) -> None:
        if type(self.row_group_rows) is not int or not 0 < self.row_group_rows <= 100_000:
            raise FactorMaterializationError("factor row-group bound must be in [1,100000]")
        if self.static_ordered_columns != STATIC_ORDERED_COLUMNS:
            raise FactorMaterializationError("static schema authority differs from qe_static_factors_121_v1")
        if "l2_code_id" not in self.static_ordered_columns:
            raise FactorMaterializationError("static schema omits l2_code_id")
        identities = [(item.dataset, item.partition_key) for item in self.chunks]
        if len(identities) != len(set(identities)):
            raise FactorMaterializationError("factor plan contains duplicate chunks")
        grouped = {name: 0 for name in (*FACTOR_H5_DATASETS, STATIC_DATASET)}
        for chunk in self.chunks:
            grouped[chunk.dataset] += 1
        missing = sorted(name for name, count in grouped.items() if count == 0)
        if missing:
            raise FactorMaterializationError(f"factor plan omits required datasets: {missing}")
        static = [item for item in self.chunks if item.dataset == STATIC_DATASET]
        if any(item.ordered_columns != self.static_ordered_columns for item in static):
            raise FactorMaterializationError("static chunk schema/order drifted")
        for chunk in self.chunks:
            if chunk.dataset in FACTOR_H5_SCHEMAS and chunk.ordered_columns != FACTOR_H5_SCHEMAS[chunk.dataset]:
                raise FactorMaterializationError(f"{chunk.dataset} chunk schema/order differs from authority")

    @property
    def digest(self) -> str:
        return digest_named_fields(
            FACTOR_MATERIALIZATION_SCHEMA,
            {
                "chunks": [
                    item.as_dict()
                    for item in sorted(
                        self.chunks,
                        key=lambda value: (value.dataset, value.partition_key),
                    )
                ],
                "static_ordered_columns": list(self.static_ordered_columns),
                "static_schema_version": STATIC_SCHEMA_VERSION,
                "static_schema_digest": static_schema_digest(),
                "factor_h5_schema_version": FACTOR_H5_SCHEMA_VERSION,
                "factor_h5_schemas": {key: list(value) for key, value in FACTOR_H5_SCHEMAS.items()},
                "factor_h5_dtypes": FACTOR_H5_DTYPES,
                "factor_h5_density_contracts": FACTOR_H5_DENSITY_CONTRACTS,
                "row_group_rows": self.row_group_rows,
            },
        )


@dataclass(frozen=True, slots=True)
class FactorMaterializationReceipt:
    receipt: Mapping[str, Any]

    @property
    def rows(self) -> int:
        return sum(int(value["rows"]) for value in self.receipt["outputs"].values() if isinstance(value, Mapping))


class FactorSourcePartitionReader(Protocol):
    """Read one immutable, already hash-verified source partition."""

    def iter_frames(
        self,
        dataset: str,
        partition_key: str,
        *,
        start: date,
        end: date,
        max_rows: int,
    ) -> Iterable[pd.DataFrame]: ...


@dataclass(frozen=True, slots=True)
class FactorSourcePartition:
    partition_key: str
    start: date
    end: date
    datasets: tuple[str, ...] = (
        "daily_raw",
        "adj_factor",
        "daily_basic",
        "moneyflow",
        "bak_basic",
        "cyq_perf",
        "sector_data",
        "margin_detail",
    )
    source_partition_key: str | None = None

    def __post_init__(self) -> None:
        if _SAFE_PARTITION.fullmatch(self.partition_key) is None or self.end < self.start:
            raise FactorMaterializationError("factor source partition identity is invalid")
        expected = {
            "daily_raw",
            "adj_factor",
            "daily_basic",
            "moneyflow",
            "bak_basic",
            "cyq_perf",
            "sector_data",
            "margin_detail",
        }
        if set(self.datasets) != expected or len(self.datasets) != len(expected):
            raise FactorMaterializationError("factor source datasets differ from v1")
        if self.source_partition_key is not None and _SAFE_PARTITION.fullmatch(self.source_partition_key) is None:
            raise FactorMaterializationError("factor backing source partition is invalid")

    @property
    def backing_partition_key(self) -> str:
        return self.source_partition_key or self.partition_key


@dataclass(frozen=True, slots=True)
class FactorPartitionProducerSpec:
    output_root: Path
    partitions: tuple[FactorSourcePartition, ...]
    pit_snapshot: FrozenPitSnapshot
    qfq_denominator_authority: QfqDenominatorAuthority
    static_ordered_columns: tuple[str, ...]
    row_group_rows: int
    max_source_partition_rows: int = 250_000
    qfq_source_summary: Mapping[str, Any] | None = None
    overlay_summary: Mapping[str, Any] | None = None
    allow_partial_ranges: bool = False
    instrument_filter: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.partitions:
            raise FactorMaterializationError("factor producer requires source partitions")
        ordered = tuple(sorted(self.partitions, key=lambda value: (value.start, value.end)))
        if ordered != self.partitions:
            raise FactorMaterializationError("factor source partitions must be date ordered")
        for previous, current in zip(self.partitions, self.partitions[1:]):
            if current.start <= previous.end:
                raise FactorMaterializationError("factor source partitions overlap")
        if not self.allow_partial_ranges and self.partitions[-1].end != self.pit_snapshot.cutoff:
            raise FactorMaterializationError("factor partitions do not end at PIT cutoff")
        if self.partitions[-1].end > self.pit_snapshot.cutoff:
            raise FactorMaterializationError("factor partitions exceed PIT cutoff")
        if (
            self.qfq_denominator_authority.cutoff != self.pit_snapshot.cutoff
            or self.qfq_denominator_authority.pit_spans_sha256 != self.pit_snapshot.spans_sha256
            or set(self.qfq_denominator_authority.by_code) != {span.ts_code for span in self.pit_snapshot.spans}
        ):
            raise FactorMaterializationError("factor QFQ authority differs from PIT/cutoff")
        if self.static_ordered_columns != STATIC_ORDERED_COLUMNS:
            raise FactorMaterializationError("factor producer requires 121-column static authority")
        if type(self.row_group_rows) is not int or not 0 < self.row_group_rows <= 100_000:
            raise FactorMaterializationError("factor producer row-group bound is invalid")
        if type(self.max_source_partition_rows) is not int or not 0 < self.max_source_partition_rows <= 1_000_000:
            raise FactorMaterializationError("factor source partition row bound is invalid")
        qfq = dict(self.qfq_source_summary or {})
        if qfq.get("source_precedence") != "db_then_tushare_missing_keys_conflict_fail_v1":
            raise FactorMaterializationError("QFQ source precedence evidence is missing")
        if int(qfq.get("overlap_mismatch_cells", -1)) != 0:
            raise FactorMaterializationError("QFQ provider/DB overlap conflicts")
        overlay = dict(self.overlay_summary or {})
        if (
            overlay.get("source_precedence") != "database_then_provider_missing_keys_conflict_fail_v1"
            or int(overlay.get("overlap_mismatch_cells", -1)) != 0
            or int(overlay.get("provider_override_rows", -1)) != 0
        ):
            raise FactorMaterializationError("factor overlay evidence is unsafe")
        if (
            self.instrument_filter != tuple(sorted(set(self.instrument_filter)))
            or any(_STOCK_CODE.fullmatch(value) is None for value in self.instrument_filter)
            or not set(self.instrument_filter).issubset(self.qfq_denominator_authority.by_code)
        ):
            raise FactorMaterializationError("factor producer instrument filter is invalid")

    @property
    def digest(self) -> str:
        return digest_named_fields(
            "dataset_release_factor_partition_producer_v1",
            {
                "partitions": [
                    {
                        "partition_key": value.partition_key,
                        "start": value.start,
                        "end": value.end,
                        "datasets": list(value.datasets),
                        "source_partition_key": value.backing_partition_key,
                    }
                    for value in self.partitions
                ],
                "pit_snapshot_digest": self.pit_snapshot.spans_sha256,
                "qfq_denominator_authority_digest": self.qfq_denominator_authority.digest,
                "static_ordered_columns": list(self.static_ordered_columns),
                "static_schema_version": STATIC_SCHEMA_VERSION,
                "static_schema_digest": static_schema_digest(),
                "factor_h5_schema_version": FACTOR_H5_SCHEMA_VERSION,
                "factor_h5_density_contracts": FACTOR_H5_DENSITY_CONTRACTS,
                "row_group_rows": self.row_group_rows,
                "max_source_partition_rows": self.max_source_partition_rows,
                "qfq_source_summary": dict(self.qfq_source_summary or {}),
                "overlay_summary": dict(self.overlay_summary or {}),
                "allow_partial_ranges": self.allow_partial_ranges,
                "instrument_filter": list(self.instrument_filter),
            },
        )


@dataclass(slots=True)
class RollingFactorState:
    price_tail: pd.DataFrame
    moneyflow_tail: pd.DataFrame
    moneyflow_pv_tail: pd.DataFrame
    adj_factor_tail: pd.DataFrame
    slow_tails: dict[str, pd.DataFrame]

    @classmethod
    def empty(cls) -> "RollingFactorState":
        return cls(
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            {},
        )


def restore_rolling_factor_state_from_bundle(
    factor_root: Path,
    *,
    qfq_denominator_authority: QfqDenominatorAuthority,
    before: date,
    max_rows: int,
    instrument_filter: Sequence[str] = (),
) -> RollingFactorState:
    """Restore only bounded rolling tails from sealed baseline partitions."""

    root = _plain_root(factor_root, must_exist=True)
    partitions = _plain_root(root / "partitions", must_exist=True)
    requested = {str(value).upper() for value in instrument_filter}

    def tail(dataset: str, rows: int) -> pd.DataFrame:
        dataset_root = _plain_root(partitions / dataset, must_exist=True)
        current = pd.DataFrame()
        paths = sorted(dataset_root.glob("*.parquet"), key=lambda value: value.name)
        if not paths:
            raise FactorMaterializationError(f"baseline factor partitions are missing: {dataset}")
        for frame in iter_parquet_frames(paths, max_rows=max_rows):
            if frame.empty:
                continue
            if requested:
                frame = frame.loc[frame.index.get_level_values("instrument").isin(requested)]
                if frame.empty:
                    continue
            dates = pd.to_datetime(frame.index.get_level_values("datetime")).date
            bounded = frame.loc[np.asarray(dates) < before]
            if bounded.empty:
                continue
            current = _tail_by_instrument(_combine_indexed_frames(current, bounded), rows)
        return current

    price = tail("daily_pv", 19)
    moneyflow = tail("moneyflow", 19)
    pv_for_moneyflow = price.reindex(moneyflow.index).dropna(how="all") if not moneyflow.empty else pd.DataFrame()
    slow = {
        dataset: tail(dataset, 1)
        for dataset in (
            "daily_basic",
            "moneyflow",
            "bak_basic",
            "cyq_perf",
            "sector_data",
            "margin_detail",
        )
    }
    latest = _tail_by_instrument(price, 1)
    adj_rows: list[dict[str, Any]] = []
    denominators = qfq_denominator_authority.by_code
    for (timestamp, code), row in latest.iterrows():
        factor = float(row["factor"])
        denominator = float(denominators[str(code)])
        numerator = factor * denominator
        if not math.isfinite(numerator) or numerator <= 0:
            raise FactorMaterializationError(f"baseline factor cannot seed QFQ numerator: {code}")
        adj_rows.append(
            {
                "ts_code": str(code),
                "trade_date": pd.Timestamp(timestamp).date(),
                "adj_factor": numerator,
            }
        )
    if not adj_rows:
        raise FactorMaterializationError("baseline QFQ rolling seed is empty")
    adj = pd.DataFrame.from_records(adj_rows).sort_values(["ts_code", "trade_date"])
    return RollingFactorState(
        price_tail=_tail_by_instrument(price, 10),
        moneyflow_tail=moneyflow,
        moneyflow_pv_tail=pv_for_moneyflow,
        adj_factor_tail=adj,
        slow_tails=slow,
    )


def _rolling_factor_state_identity(state: RollingFactorState) -> str:
    def frame_identity(frame: pd.DataFrame) -> Mapping[str, Any]:
        if frame.empty:
            return {"rows": 0, "columns": [], "sha256": hashlib.sha256().hexdigest()}
        hashed = pd.util.hash_pandas_object(frame, index=True).to_numpy(dtype=np.uint64, copy=False)
        return {
            "rows": len(frame),
            "columns": [str(value) for value in frame.columns],
            "sha256": hashlib.sha256(hashed.tobytes()).hexdigest(),
        }

    return digest_named_fields(
        "dataset_release_factor_rolling_state_v1",
        {
            "price_tail": frame_identity(state.price_tail),
            "moneyflow_tail": frame_identity(state.moneyflow_tail),
            "moneyflow_pv_tail": frame_identity(state.moneyflow_pv_tail),
            "adj_factor_tail": frame_identity(state.adj_factor_tail),
            "slow_tails": {key: frame_identity(value) for key, value in sorted(state.slow_tails.items())},
        },
    )


def restore_rolling_factor_state_from_produced_partition(
    source_root: Path,
    *,
    partition_key: str,
    max_rows: int,
) -> RollingFactorState:
    root = _plain_root(source_root, must_exist=True)
    target = (root / partition_key).resolve(strict=True)
    if root not in target.parents or not target.is_dir():
        raise FactorMaterializationError("produced rolling-state partition path is invalid")
    return _restore_factor_state(target, max_rows=max_rows)


def merge_rolling_factor_states_by_instrument(
    baseline: RollingFactorState,
    replacement: RollingFactorState,
    *,
    affected_instruments: Sequence[str],
) -> RollingFactorState:
    affected = {str(value).upper() for value in affected_instruments}
    if not affected:
        raise FactorMaterializationError("rolling-state merge requires affected instruments")

    def indexed(base: pd.DataFrame, patch: pd.DataFrame) -> pd.DataFrame:
        values: list[pd.DataFrame] = []
        if not base.empty:
            values.append(base.loc[~base.index.get_level_values("instrument").isin(affected)])
        if not patch.empty:
            observed = {str(value).upper() for value in patch.index.get_level_values("instrument")}
            if not observed.issubset(affected):
                raise FactorMaterializationError("rolling-state replacement exceeds affected instruments")
            values.append(patch)
        return _combine_indexed_frames(*values)

    def adj(base: pd.DataFrame, patch: pd.DataFrame) -> pd.DataFrame:
        values: list[pd.DataFrame] = []
        if not base.empty:
            values.append(base.loc[~base["ts_code"].isin(affected)])
        if not patch.empty:
            if not set(patch["ts_code"].astype(str)).issubset(affected):
                raise FactorMaterializationError("rolling adj replacement exceeds affected instruments")
            values.append(patch)
        if not values:
            return pd.DataFrame(columns=["ts_code", "trade_date", "adj_factor"])
        return pd.concat(values).sort_values(["ts_code", "trade_date"])

    slow_keys = set(baseline.slow_tails).union(replacement.slow_tails)
    return RollingFactorState(
        price_tail=indexed(baseline.price_tail, replacement.price_tail),
        moneyflow_tail=indexed(baseline.moneyflow_tail, replacement.moneyflow_tail),
        moneyflow_pv_tail=indexed(baseline.moneyflow_pv_tail, replacement.moneyflow_pv_tail),
        adj_factor_tail=adj(baseline.adj_factor_tail, replacement.adj_factor_tail),
        slow_tails={
            key: indexed(
                baseline.slow_tails.get(key, _empty_multiindex_frame()),
                replacement.slow_tails.get(key, _empty_multiindex_frame()),
            )
            for key in slow_keys
        },
    )


@dataclass(slots=True)
class QfqDailyTransformMetrics:
    """Operation counts proving code-alignment remains O(rows + codes)."""

    daily_rows: int = 0
    adj_rows: int = 0
    daily_code_groups: int = 0
    adj_code_groups: int = 0
    adj_group_advances: int = 0


@dataclass(frozen=True, slots=True)
class FactorPartitionProductionReceipt:
    source_root: Path
    chunks: tuple[SealedFactorChunk, ...]
    receipt: Mapping[str, Any]


class FactorPartitionProducer:
    """Transform frozen raw source partitions into eight sealed QE chunks.

    The reader is injected and has no database or provider API. QFQ fallback
    and overlay acquisition therefore finish during resolution and arrive only
    as immutable rows plus zero-conflict provenance evidence.
    """

    def produce(
        self,
        spec: FactorPartitionProducerSpec,
        *,
        reader: FactorSourcePartitionReader,
        initial_state: RollingFactorState | None = None,
        checkpoint: Callable[[], None] = lambda: None,
    ) -> FactorPartitionProductionReceipt:
        output_root = _plain_root(spec.output_root, must_exist=True)
        producer_root = output_root / "factor_source_chunks"
        producer_root.mkdir(exist_ok=True)
        sealed_root = producer_root / "sealed"
        sealed_root.mkdir(exist_ok=True)
        checkpoint_path = producer_root / "producer_checkpoint.json"
        durable = _load_producer_checkpoint(checkpoint_path, spec.digest)
        state = initial_state or RollingFactorState.empty()
        state_identity = _rolling_factor_state_identity(state)
        if durable.get("initial_state_identity") not in {None, state_identity}:
            raise FactorCheckpointConflict("factor producer initial rolling state identity differs")
        durable["initial_state_identity"] = state_identity
        base_factors = self._qfq_denominators(
            spec,
            reader,
            require_complete=initial_state is None and not spec.allow_partial_ranges,
            checkpoint=checkpoint,
        )
        durable["qfq_base_digest"] = digest_named_fields("dataset_release_qfq_denominators_v1", base_factors)
        durable["qfq_denominator_authority"] = {
            "authority_digest": spec.qfq_denominator_authority.digest,
            "ordered_adj_series_sha256": (spec.qfq_denominator_authority.source_rows_sha256),
            "source_row_count": spec.qfq_denominator_authority.source_row_count,
            "per_code_series": [
                {
                    "ts_code": code,
                    "adj_row_count": rows,
                    "ordered_adj_series_sha256": digest,
                }
                for code, rows, digest in spec.qfq_denominator_authority.per_code_series
            ],
        }
        completed = list(durable.get("completed_partitions") or [])
        chunks: list[SealedFactorChunk] = []

        for ordinal, partition in enumerate(spec.partitions):
            checkpoint()
            target = sealed_root / partition.partition_key
            if ordinal < len(completed):
                receipt = completed[ordinal]
                if receipt.get("partition_key") != partition.partition_key:
                    raise FactorCheckpointConflict("factor producer checkpoint is not a contiguous prefix")
                _verify_produced_partition(target, receipt)
            elif target.exists():
                receipt = _load_json_file(target / "partition_receipt.json")
                if receipt.get("partition_key") != partition.partition_key:
                    raise FactorCheckpointConflict("sealed factor partition identity differs")
                _verify_produced_partition(target, receipt)
                completed.append(receipt)
                durable["completed_partitions"] = completed
                _write_checkpoint(checkpoint_path, durable)
            else:
                receipt, state = self._produce_one(
                    spec,
                    partition,
                    reader=reader,
                    base_factors=base_factors,
                    previous_state=state,
                    working_root=producer_root,
                    target=target,
                    checkpoint=checkpoint,
                )
                completed.append(receipt)
                durable["completed_partitions"] = completed
                _write_checkpoint(checkpoint_path, durable)
            state = _restore_factor_state(target, max_rows=spec.row_group_rows)
            chunks.extend(_produced_chunks(target, receipt, sealed_root))

        durable["status"] = "PASS"
        durable["actual_work"] = {
            "recomputed_source_partitions": [item.partition_key for item in spec.partitions],
            "recomputed_partition_count": len(spec.partitions),
            "initial_state_identity": state_identity,
            "whole_history_source_partitions_read": 0,
            "instrument_filter": list(spec.instrument_filter),
        }
        durable["completed_partitions"] = completed
        durable["memory_contract"] = {
            "mode": "bounded_date_slice_sequential_aux_plus_rolling_tails_v2",
            "source_read_chunk_rows": spec.row_group_rows,
            "max_source_date_slice_rows": spec.max_source_partition_rows,
            "peak_retained_source_rows": max(
                (int(item.get("memory_contract", {}).get("peak_retained_source_rows", 0)) for item in completed),
                default=0,
            ),
            "hard_retained_source_rows": spec.max_source_partition_rows * 4,
            "whole_history_frames_retained": 0,
            "whole_source_partition_frames_retained": 0,
            "rolling_price_rows_per_instrument": 10,
            "rolling_moneyflow_rows_per_instrument": 19,
        }
        durable["moneyflow_unit_contract"] = moneyflow_unit_contract_receipt()
        durable["schema_authority"] = _factor_schema_authority()
        durable["safety"] = {
            "database_writes": 0,
            "production_writes": 0,
            "production_deletes": 0,
            "production_pointer_changes": 0,
            "service_process_controls": 0,
        }
        _write_checkpoint(checkpoint_path, durable)
        return FactorPartitionProductionReceipt(sealed_root, tuple(chunks), durable)

    def _qfq_denominators(
        self,
        spec: FactorPartitionProducerSpec,
        reader: FactorSourcePartitionReader,
        *,
        require_complete: bool,
        checkpoint: Callable[[], None],
    ) -> dict[str, float]:
        denominators: dict[str, float] = {}
        source_ranges: dict[str, tuple[date, date]] = {}
        for partition in spec.partitions:
            key = partition.backing_partition_key
            existing = source_ranges.get(key)
            source_ranges[key] = (
                min(existing[0], partition.start) if existing else partition.start,
                max(existing[1], partition.end) if existing else partition.end,
            )
        for source_key, (start, end) in sorted(source_ranges.items()):
            observed = 0
            for frame in reader.iter_frames(
                "adj_factor",
                source_key,
                start=start,
                end=end,
                max_rows=spec.row_group_rows,
            ):
                bounded = _bounded_source_frame(
                    frame,
                    dataset="adj_factor",
                    partition=FactorSourcePartition(
                        source_key,
                        start,
                        end,
                        source_partition_key=source_key,
                    ),
                    max_rows=spec.row_group_rows,
                )
                normalized = _normalize_source_table(bounded, dataset="adj_factor")
                observed += len(normalized)
                for code, value in normalized.groupby("ts_code")["adj_factor"].max().items():
                    numeric = float(value)
                    if not math.isfinite(numeric) or numeric <= 0:
                        raise FactorMaterializationError(f"invalid QFQ denominator: {code}")
                    denominators[str(code)] = max(denominators.get(str(code), 0.0), numeric)
                checkpoint()
            if observed <= 0 and require_complete:
                raise FactorMaterializationError(f"adj_factor partition is empty: {source_key}")
        if not denominators and require_complete:
            raise FactorMaterializationError("QFQ denominator scan found no instruments")
        expected = dict(spec.qfq_denominator_authority.by_code)
        observed = dict(sorted(denominators.items()))
        if require_complete and observed != expected:
            changed = sorted(code for code in set(observed).union(expected) if observed.get(code) != expected.get(code))
            raise FactorMaterializationError(
                "factor adj_factor scan differs from artifact-ready QFQ authority",
                context={
                    "changed_count": len(changed),
                    "changed_sample": changed[:20],
                    "qfq_denominator_authority_digest": (spec.qfq_denominator_authority.digest),
                },
            )
        if not require_complete:
            invalid = sorted(code for code, value in observed.items() if code not in expected or value > expected[code])
            if invalid:
                raise FactorMaterializationError(
                    "bounded factor adj-factor rows exceed frozen QFQ authority",
                    context={"changed_sample": invalid[:20]},
                )
        return expected

    def _produce_one(
        self,
        spec: FactorPartitionProducerSpec,
        partition: FactorSourcePartition,
        *,
        reader: FactorSourcePartitionReader,
        base_factors: Mapping[str, float],
        previous_state: RollingFactorState,
        working_root: Path,
        target: Path,
        checkpoint: Callable[[], None],
    ) -> tuple[dict[str, Any], RollingFactorState]:
        working = working_root / f".working.{partition.partition_key}"
        if working.exists():
            raise FactorCheckpointConflict(
                "unsealed factor partition working directory exists; use a new fenced attempt"
            )
        working.mkdir()
        raw_daily = _read_source_slice(
            reader,
            dataset="daily_raw",
            partition=partition,
            max_rows=spec.max_source_partition_rows,
            read_chunk_rows=spec.row_group_rows,
        )
        raw_adj = _read_source_slice(
            reader,
            dataset="adj_factor",
            partition=partition,
            max_rows=spec.max_source_partition_rows,
            read_chunk_rows=spec.row_group_rows,
            allow_empty=not previous_state.adj_factor_tail.empty,
        )
        daily, adj_tail = _build_qfq_daily(
            raw_daily,
            raw_adj,
            base_factors=base_factors,
            previous_adj_tail=previous_state.adj_factor_tail,
        )
        daily, daily_pit = filter_frame_to_pit_spans(daily, spec.pit_snapshot)
        if daily.empty:
            raise FactorMaterializationError(f"PIT removed every daily row: {partition.partition_key}")
        if len(daily) > spec.max_source_partition_rows:
            raise FactorMaterializationError("daily PIT partition exceeds bounded row contract")
        del raw_daily, raw_adj

        storage: dict[str, np.ndarray] = {
            column: (
                np.full(len(daily), _UNKNOWN_L2_CODE_ID, dtype=np.int16)
                if column == "l2_code_id"
                else np.full(len(daily), np.nan, dtype=np.float32)
            )
            for column in spec.static_ordered_columns
        }
        claimed_columns: set[str] = set()
        retained_aux: dict[str, pd.DataFrame] = {}
        slow_tails: dict[str, pd.DataFrame] = {}
        pit_receipts: dict[str, Any] = {}
        static_fill: dict[str, Any] = {}
        artifact_receipts: dict[str, Any] = {}
        artifact_rows: dict[str, int] = {"daily_pv": int(len(daily))}
        peak_retained_source_rows = len(daily)

        artifact_receipts["daily_pv"] = _write_produced_artifact(
            working,
            dataset="daily_pv",
            partition_key=partition.partition_key,
            frame=daily,
            row_group_rows=spec.row_group_rows,
        )
        checkpoint()
        for dataset in (
            "daily_basic",
            "moneyflow",
            "bak_basic",
            "cyq_perf",
            "sector_data",
            "margin_detail",
        ):
            frame = _read_source_slice(
                reader,
                dataset=dataset,
                partition=partition,
                max_rows=spec.max_source_partition_rows,
                allow_empty=True,
                read_chunk_rows=spec.row_group_rows,
            )
            frame = _normalize_aux_frame(frame, dataset=dataset)
            frame, pit_receipts[dataset] = filter_frame_to_pit_spans(frame, spec.pit_snapshot)
            frame = frame.sort_index() if not frame.empty else frame
            static_frame, tail = _static_asof_frame(
                dataset,
                frame,
                daily.index,
                previous_state.slow_tails.get(dataset, pd.DataFrame()),
            )
            static_fill[dataset] = _fill_static(
                storage,
                daily.index,
                claimed_columns,
                static_frame,
                source=dataset,
            )
            slow_tails[dataset] = tail
            artifact_rows[dataset] = int(len(frame))
            artifact_receipts[dataset] = _write_produced_artifact(
                working,
                dataset=dataset,
                partition_key=partition.partition_key,
                frame=frame,
                row_group_rows=spec.row_group_rows,
            )
            if dataset in {"daily_basic", "moneyflow"}:
                retained_aux[dataset] = frame
            peak_retained_source_rows = max(
                peak_retained_source_rows,
                len(daily)
                + sum(len(value) for value in retained_aux.values())
                + (0 if dataset in retained_aux else len(frame)),
            )
            if peak_retained_source_rows > spec.max_source_partition_rows * 4:
                raise FactorMaterializationError("factor retained source rows exceed hard memory contract")
            del static_frame, frame
            checkpoint()

        daily_basic = retained_aux["daily_basic"]
        moneyflow = retained_aux["moneyflow"]
        moneyflow_context = _combine_indexed_frames(previous_state.moneyflow_tail, moneyflow)
        pv_context = _combine_indexed_frames(previous_state.moneyflow_pv_tail, daily)
        derived_moneyflow = (
            derive_moneyflow_factors(moneyflow_context, pv_context).reindex(moneyflow.index).sort_index()
            if not moneyflow.empty
            else pd.DataFrame()
        )
        static_fill["moneyflow_derived"] = _fill_static(
            storage,
            daily.index,
            claimed_columns,
            derived_moneyflow,
            source="moneyflow_derived",
        )
        daily_basic_derived = _daily_basic_derived(daily_basic)
        static_fill["daily_basic_precomputed"] = _fill_static(
            storage,
            daily.index,
            claimed_columns,
            daily_basic_derived,
            source="daily_basic_precomputed",
        )
        price_context = _combine_indexed_frames(previous_state.price_tail, daily)
        price = pd.DataFrame(index=price_context.index)
        price["PriceStrength_10D"] = price_context["close"].groupby(level="instrument").pct_change(10)
        static_fill["price_momentum"] = _fill_static(
            storage,
            daily.index,
            claimed_columns,
            price.reindex(daily.index),
            source="price_momentum",
        )
        static = pd.DataFrame(
            storage,
            index=daily.index,
            columns=list(spec.static_ordered_columns),
            copy=False,
        )
        if (
            tuple(static.columns) != tuple(spec.static_ordered_columns)
            or {str(column): str(dtype) for column, dtype in static.dtypes.items()} != STATIC_COLUMN_DTYPES
        ):
            raise FactorMaterializationError("static schema/dtype authority drifted")
        if not moneyflow.empty:
            assert_moneyflow_frame_parity(moneyflow, static)
        artifact_rows[STATIC_DATASET] = int(len(static))
        artifact_receipts[STATIC_DATASET] = _write_produced_artifact(
            working,
            dataset=STATIC_DATASET,
            partition_key=partition.partition_key,
            frame=static,
            row_group_rows=spec.row_group_rows,
        )
        checkpoint()

        next_state = _advance_factor_state(
            previous_state,
            daily,
            moneyflow,
            adj_tail=adj_tail,
            slow_tails=slow_tails,
        )
        state_receipts = _write_factor_state(working / "state", next_state, row_group_rows=spec.row_group_rows)
        receipt = {
            "schema_version": "dataset_release_factor_partition_receipt_v1",
            "producer_spec_digest": spec.digest,
            "partition_key": partition.partition_key,
            "start": partition.start.isoformat(),
            "end": partition.end.isoformat(),
            "status": "PASS",
            "artifacts": artifact_receipts,
            "state": state_receipts,
            "daily_pit": daily_pit,
            "aux_pit": pit_receipts,
            "static_fill": static_fill,
            "rows": artifact_rows,
            "memory_contract": {
                "mode": "one_date_slice_sequential_aux_v2",
                "source_read_chunk_rows": spec.row_group_rows,
                "max_source_date_slice_rows": spec.max_source_partition_rows,
                "peak_retained_source_rows": peak_retained_source_rows,
                "hard_retained_source_rows": spec.max_source_partition_rows * 4,
                "whole_source_partition_frames_retained": 0,
            },
            "moneyflow_unit_contract": moneyflow_unit_contract_receipt(),
            "schema_authority": _factor_schema_authority(),
            "safety": {
                "database_writes": 0,
                "production_writes": 0,
                "production_deletes": 0,
            },
        }
        _write_checkpoint(working / "partition_receipt.json", receipt)
        os.rename(working, target)
        _verify_produced_partition(target, receipt)
        return receipt, next_state


class FactorBundleMaterializer:
    """Materialize one complete factor bundle below an attempt staging root."""

    def materialize(
        self,
        spec: FactorMaterializationSpec,
        *,
        checkpoint: Callable[[], None] = lambda: None,
    ) -> FactorMaterializationReceipt:
        source_root = _plain_root(spec.source_root, must_exist=True)
        staging_root = _plain_root(spec.staging_root, must_exist=True)
        if source_root == staging_root or source_root in staging_root.parents:
            raise FactorMaterializationError("factor staging must not be inside source root")
        factor_root = staging_root / "factor_bundle"
        if factor_root.exists():
            _assert_plain(factor_root)
        else:
            factor_root.mkdir()
        chunks_root = factor_root / "partitions"
        chunks_root.mkdir(exist_ok=True)
        checkpoint_path = factor_root / "factor_checkpoint.json"
        durable = _load_checkpoint(checkpoint_path, spec.digest)

        local: dict[tuple[str, str], Path] = {}
        chunk_receipts: list[dict[str, Any]] = []
        for item in sorted(spec.chunks, key=lambda value: (value.dataset, value.partition_key)):
            checkpoint()
            source = _contained_source(source_root, item.relative_path)
            audit = _audit_parquet_chunk(source, item, spec.row_group_rows)
            dataset_root = chunks_root / item.dataset
            dataset_root.mkdir(exist_ok=True)
            target = dataset_root / f"{item.partition_key}.parquet"
            _publish_sealed_copy(source, target, expected_sha256=item.sha256)
            local[(item.dataset, item.partition_key)] = target
            chunk_receipts.append(
                {
                    **item.as_dict(),
                    "candidate_relative_path": target.relative_to(staging_root).as_posix(),
                    "max_row_group_rows": audit["max_row_group_rows"],
                    "size_bytes": audit["size_bytes"],
                }
            )
        durable["chunks"] = chunk_receipts
        _write_checkpoint(checkpoint_path, durable)

        outputs = dict(durable.get("outputs") or {})
        for dataset in FACTOR_H5_DATASETS:
            checkpoint()
            target = factor_root / f"{dataset}.h5"
            paths = _dataset_paths(spec, local, dataset)
            expected_columns = next(item.ordered_columns for item in spec.chunks if item.dataset == dataset)
            if dataset in outputs:
                receipt = _audit_existing_h5(
                    target,
                    expected_columns=expected_columns,
                    expected_dtypes=FACTOR_H5_DTYPES[dataset],
                    max_rows=spec.row_group_rows,
                )
                _require_recorded_output(outputs[dataset], receipt, dataset)
            elif target.exists():
                # Crash may occur after atomic aggregate publish and before the
                # checkpoint.  Reconstruct evidence rather than overwrite it.
                receipt = _audit_existing_h5(
                    target,
                    expected_columns=expected_columns,
                    expected_dtypes=FACTOR_H5_DTYPES[dataset],
                    max_rows=spec.row_group_rows,
                )
                outputs[dataset] = _portable_output_receipt(receipt, root=staging_root)
            else:
                receipt = finalize_h5_from_parquet_chunks(
                    paths,
                    target,
                    expected_columns=expected_columns,
                    dtype_overrides=FACTOR_H5_DTYPES[dataset],
                    max_rows_in_memory=spec.row_group_rows,
                )
                outputs[dataset] = _portable_output_receipt(receipt, root=staging_root)
            durable["outputs"] = outputs
            _write_checkpoint(checkpoint_path, durable)

        checkpoint()
        static_target = factor_root / "static_factors.parquet"
        static_paths = _dataset_paths(spec, local, STATIC_DATASET)
        if STATIC_DATASET in outputs:
            static_receipt = _audit_existing_parquet(
                static_target,
                expected_columns=spec.static_ordered_columns,
                max_rows=spec.row_group_rows,
            )
            _require_recorded_output(outputs[STATIC_DATASET], static_receipt, STATIC_DATASET)
        elif static_target.exists():
            static_receipt = _audit_existing_parquet(
                static_target,
                expected_columns=spec.static_ordered_columns,
                max_rows=spec.row_group_rows,
            )
            outputs[STATIC_DATASET] = _portable_output_receipt(static_receipt, root=staging_root)
        else:
            static_receipt = finalize_parquet_chunks(
                static_paths,
                static_target,
                max_rows_in_memory=spec.row_group_rows,
            )
            static_receipt = {
                **static_receipt,
                "columns": list(spec.static_ordered_columns),
            }
            _assert_parquet_dtypes(
                static_target,
                expected_dtypes=STATIC_COLUMN_DTYPES,
                max_rows=spec.row_group_rows,
            )
            outputs[STATIC_DATASET] = _portable_output_receipt(static_receipt, root=staging_root)

        durable["outputs"] = outputs
        durable["status"] = "PASS"
        durable["memory_contract"] = {
            "mode": "partitioned_parquet_to_new_aggregate_v1",
            "max_rows_in_memory": spec.row_group_rows,
            "whole_panel_frames_retained": 0,
        }
        durable["moneyflow_unit_contract"] = moneyflow_unit_contract_receipt()
        durable["schema_authority"] = _factor_schema_authority()
        durable["safety"] = {
            "database_writes": 0,
            "production_writes": 0,
            "production_deletes": 0,
            "production_pointer_changes": 0,
            "service_process_controls": 0,
        }
        _write_checkpoint(checkpoint_path, durable)
        return FactorMaterializationReceipt(receipt=durable)


def _read_source_slice(
    reader: FactorSourcePartitionReader,
    *,
    dataset: str,
    partition: FactorSourcePartition,
    max_rows: int,
    read_chunk_rows: int,
    allow_empty: bool = False,
) -> pd.DataFrame:
    """Read only one output date slice from a possibly larger sealed partition."""

    pieces: list[pd.DataFrame] = []
    rows = 0
    for frame in reader.iter_frames(
        dataset,
        partition.backing_partition_key,
        start=partition.start,
        end=partition.end,
        max_rows=read_chunk_rows,
    ):
        bounded = _bounded_source_frame(
            frame,
            dataset=dataset,
            partition=partition,
            max_rows=read_chunk_rows,
            allow_empty=True,
        )
        rows += len(bounded)
        if rows > max_rows:
            raise FactorMaterializationError(
                f"source date slice exceeds row bound: {dataset}:{partition.partition_key} rows>{max_rows}"
            )
        if not bounded.empty:
            pieces.append(bounded)
    if not pieces:
        if allow_empty:
            return pd.DataFrame()
        raise FactorMaterializationError(f"required source date slice is empty: {dataset}:{partition.partition_key}")
    # The sum is checked before concat. Source chunks are already canonical;
    # concat preserves their order and never triggers a full-market sort.
    return pd.concat(pieces, ignore_index=True, copy=False)


def _bounded_source_frame(
    frame: pd.DataFrame,
    *,
    dataset: str,
    partition: FactorSourcePartition,
    max_rows: int,
    allow_empty: bool = False,
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise FactorMaterializationError(f"source reader returned non-DataFrame: {dataset}:{partition.partition_key}")
    if len(frame) > max_rows:
        raise FactorMaterializationError(
            f"source partition exceeds row bound: {dataset}:{partition.partition_key} rows={len(frame)} max={max_rows}"
        )
    if frame.empty and not allow_empty:
        raise FactorMaterializationError(f"required source partition is empty: {dataset}:{partition.partition_key}")
    return frame.copy()


def _normalize_source_table(frame: pd.DataFrame, *, dataset: str) -> pd.DataFrame:
    if dataset != "adj_factor":
        raise FactorMaterializationError(f"unsupported source table normalization: {dataset}")
    value = _reset_source_index(frame)
    required = {"ts_code", "trade_date", "adj_factor"}
    if not required.issubset(value.columns):
        raise FactorMaterializationError("adj_factor source schema is incomplete")
    value = value.loc[:, ["ts_code", "trade_date", "adj_factor"]].copy()
    value["ts_code"] = value["ts_code"].astype(str).str.upper()
    value["trade_date"] = pd.to_datetime(value["trade_date"], errors="raise").dt.date
    value["adj_factor"] = pd.to_numeric(value["adj_factor"], errors="raise").astype(float)
    if (
        value.duplicated(["ts_code", "trade_date"]).any()
        or not np.isfinite(value["adj_factor"]).all()
        or (value["adj_factor"] <= 0).any()
    ):
        raise FactorMaterializationError("adj_factor keys/values are invalid")
    return value.sort_values(["ts_code", "trade_date"], kind="mergesort").reset_index(drop=True)


def _build_qfq_daily(
    raw_daily: pd.DataFrame,
    raw_adj: pd.DataFrame,
    *,
    base_factors: Mapping[str, float],
    previous_adj_tail: pd.DataFrame,
    metrics: QfqDailyTransformMetrics | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = _reset_source_index(raw_daily)
    required = {
        "ts_code",
        "trade_date",
        "open_li",
        "high_li",
        "low_li",
        "close_li",
        "volume_hand",
        "amount_li",
    }
    if not required.issubset(daily.columns):
        raise FactorMaterializationError(
            f"daily_raw source schema is incomplete: {sorted(required.difference(daily.columns))}"
        )
    daily = daily.loc[:, sorted(required)].copy()
    daily["ts_code"] = daily["ts_code"].astype(str).str.upper()
    daily["trade_date"] = pd.to_datetime(daily["trade_date"], errors="raise").dt.date
    if daily.duplicated(["ts_code", "trade_date"]).any():
        raise FactorMaterializationError("daily_raw contains duplicate keys")
    daily = daily.sort_values(["ts_code", "trade_date"], kind="mergesort").reset_index(drop=True)
    adj = _normalize_source_table(raw_adj, dataset="adj_factor")
    prior = (
        _normalize_source_table(previous_adj_tail, dataset="adj_factor")
        if not previous_adj_tail.empty
        else adj.iloc[0:0].copy()
    )
    all_adj = (
        pd.concat([prior, adj], ignore_index=True)
        .drop_duplicates(["ts_code", "trade_date"], keep="last")
        .sort_values(["ts_code", "trade_date"], kind="mergesort")
        .reset_index(drop=True)
    )
    report = metrics or QfqDailyTransformMetrics()
    if any(
        (
            report.daily_rows,
            report.adj_rows,
            report.daily_code_groups,
            report.adj_code_groups,
            report.adj_group_advances,
        )
    ):
        raise FactorMaterializationError("QFQ transform metrics must be fresh")
    report.daily_rows = len(daily)
    report.adj_rows = len(all_adj)
    values = {
        name: np.empty(len(daily), dtype=np.float64)
        for name in ("open", "high", "low", "close", "volume", "amount", "factor")
    }
    missing: list[dict[str, Any]] = []
    adj_groups = iter(all_adj.groupby("ts_code", sort=False, observed=True))
    current_adj = next(adj_groups, None)
    tail_rows: list[pd.Series] = []

    def advance_adj() -> None:
        nonlocal current_adj
        assert current_adj is not None
        _code, group = current_adj
        tail_rows.append(group.iloc[-1])
        report.adj_code_groups += 1
        report.adj_group_advances += 1
        current_adj = next(adj_groups, None)

    for code, group in daily.groupby("ts_code", sort=True):
        report.daily_code_groups += 1
        code = str(code)
        while current_adj is not None and str(current_adj[0]) < code:
            advance_adj()
        factors = None
        if current_adj is not None and str(current_adj[0]) == code:
            factors = current_adj[1]
        denominator = float(base_factors.get(str(code), 0.0))
        if not math.isfinite(denominator) or denominator <= 0:
            raise FactorMaterializationError(f"QFQ denominator missing: {code}")
        if factors is None:
            aligned = np.full(len(group), np.nan, dtype=np.float64)
        else:
            factor_dates = np.asarray(factors["trade_date"], dtype="datetime64[D]")
            factor_values = factors["adj_factor"].to_numpy(dtype=np.float64)
            target_dates = np.asarray(group["trade_date"], dtype="datetime64[D]")
            positions = np.searchsorted(factor_dates, target_dates, side="right") - 1
            aligned = np.full(len(group), np.nan, dtype=np.float64)
            available = positions >= 0
            aligned[available] = factor_values[positions[available]]
        invalid = ~np.isfinite(aligned) | (aligned <= 0)
        if invalid.any():
            missing.extend(
                {"ts_code": str(code), "trade_date": str(value)}
                for value in group["trade_date"].iloc[np.flatnonzero(invalid)[:10]]
            )
            continue
        positions = group.index.to_numpy(dtype=np.int64)
        qfq = aligned / denominator
        for source, target in (
            ("open_li", "open"),
            ("high_li", "high"),
            ("low_li", "low"),
            ("close_li", "close"),
        ):
            values[target][positions] = (
                pd.to_numeric(group[source], errors="raise").to_numpy(dtype=float) / 1000.0 * qfq
            )
        values["volume"][positions] = (
            pd.to_numeric(group["volume_hand"], errors="raise").to_numpy(dtype=float) * 100.0 / qfq
        )
        values["amount"][positions] = pd.to_numeric(group["amount_li"], errors="raise").to_numpy(dtype=float) / 1000.0
        values["factor"][positions] = qfq
    while current_adj is not None:
        advance_adj()
    if missing:
        raise FactorMaterializationError(
            "adj_factor is incomplete after frozen provider fallback",
            context={"missing_sample": missing[:20]},
        )
    if daily.empty:
        raise FactorMaterializationError("QFQ daily transform produced no rows")
    output = pd.DataFrame(values)
    output["datetime"] = pd.to_datetime(daily["trade_date"])
    output["instrument"] = daily["ts_code"].to_numpy()
    output = output.set_index(["datetime", "instrument"]).sort_index()
    if output.index.has_duplicates:
        raise FactorMaterializationError("QFQ daily output contains duplicate keys")
    output = output.astype("float32")
    tail = pd.DataFrame(tail_rows, columns=all_adj.columns).reset_index(drop=True)
    return output, tail


def _normalize_aux_frame(frame: pd.DataFrame, *, dataset: str) -> pd.DataFrame:
    raw = _reset_source_index(frame)
    if raw.empty:
        return _empty_aux_frame(dataset)
    if not {"ts_code", "trade_date"}.issubset(raw.columns):
        raise FactorMaterializationError(f"{dataset} source keys are incomplete")
    raw["ts_code"] = raw["ts_code"].astype(str).str.upper()
    raw["trade_date"] = pd.to_datetime(raw["trade_date"], errors="raise")
    if raw.duplicated(["ts_code", "trade_date"]).any():
        raise FactorMaterializationError(f"{dataset} contains duplicate keys")
    if dataset == "moneyflow":
        if set(MONEYFLOW_FIELD_MAP).issubset(raw.columns):
            raw = normalize_tushare_moneyflow_units(raw, require_all=True)
            raw = raw.rename(columns=MONEYFLOW_FIELD_MAP)
        missing = sorted(set(MONEYFLOW_FIELD_MAP.values()).difference(raw.columns))
        if missing:
            raise FactorMaterializationError(f"moneyflow canonical fields missing: {missing}")
        columns = list(MONEYFLOW_FIELD_MAP.values())
    elif dataset in _AUX_RENAMES:
        mapping = _AUX_RENAMES[dataset]
        raw = raw.rename(columns={key: value for key, value in mapping.items() if key in raw.columns})
        columns = list(mapping.values())
        missing = sorted(set(columns).difference(raw.columns))
        if missing:
            raise FactorMaterializationError(f"{dataset} canonical fields missing: {missing}")
    elif dataset == "sector_data":
        columns = list(STATIC_SECTOR_COLUMNS)
        missing = sorted(set(columns).difference(raw.columns))
        if missing:
            raise FactorMaterializationError(f"sector_data canonical fields missing: {missing}")
    else:
        raise FactorMaterializationError(f"unsupported auxiliary dataset: {dataset}")
    for column in columns:
        numeric = pd.to_numeric(raw[column], errors="coerce")
        raw[column] = (
            numeric.fillna(_UNKNOWN_L2_CODE_ID).astype("int16") if column == "l2_code_id" else numeric.astype("float32")
        )
    raw["datetime"] = raw["trade_date"]
    raw["instrument"] = raw["ts_code"]
    output = raw.set_index(["datetime", "instrument"])[columns].sort_index()
    if output.index.has_duplicates:
        raise FactorMaterializationError(f"{dataset} output contains duplicate keys")
    return output


def _static_asof_frame(
    dataset: str,
    current: pd.DataFrame,
    target_index: pd.MultiIndex,
    previous_tail: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if dataset == "moneyflow":
        return current, pd.DataFrame()
    slow_columns: list[str] = []
    if dataset == "daily_basic":
        slow_columns = [value for value in ("db_dv_ratio", "db_dv_ttm") if value in current.columns]
    elif dataset in {"bak_basic", "cyq_perf", "sector_data", "margin_detail"}:
        slow_columns = list(current.columns)
    if not slow_columns:
        return current, _tail_by_instrument(current, 1)
    prior = previous_tail.loc[:, slow_columns] if not previous_tail.empty else pd.DataFrame()
    context = _combine_indexed_frames(prior, current.loc[:, slow_columns])
    if context.empty:
        return current, pd.DataFrame()
    union = context.index.union(target_index).sort_values()
    filled = context.reindex(union).groupby(level="instrument", sort=False).ffill()
    slow = filled.reindex(target_index).loc[:, slow_columns]
    exact = current.drop(columns=slow_columns, errors="ignore")
    output = pd.concat([exact, slow], axis=1).sort_index()
    tail = _tail_by_instrument(context, 1)
    return output, tail


def _build_static_frame(
    daily: pd.DataFrame,
    *,
    raw_aux: Mapping[str, pd.DataFrame],
    static_aux: Mapping[str, pd.DataFrame],
    previous_state: RollingFactorState,
    expected_columns: Sequence[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    storage: dict[str, np.ndarray] = {
        column: (
            np.full(len(daily), _UNKNOWN_L2_CODE_ID, dtype=np.int16)
            if column == "l2_code_id"
            else np.full(len(daily), np.nan, dtype=np.float32)
        )
        for column in expected_columns
    }
    claimed: set[str] = set()
    stats: dict[str, Any] = {}
    for dataset in (
        "daily_basic",
        "moneyflow",
        "bak_basic",
        "cyq_perf",
        "sector_data",
        "margin_detail",
    ):
        stats[dataset] = _fill_static(storage, daily.index, claimed, static_aux[dataset], source=dataset)
    moneyflow_context = _combine_indexed_frames(previous_state.moneyflow_tail, raw_aux["moneyflow"])
    pv_context = _combine_indexed_frames(previous_state.moneyflow_pv_tail, daily)
    derived_moneyflow = (
        derive_moneyflow_factors(moneyflow_context, pv_context).reindex(raw_aux["moneyflow"].index).sort_index()
        if not raw_aux["moneyflow"].empty
        else pd.DataFrame()
    )
    stats["moneyflow_derived"] = _fill_static(
        storage,
        daily.index,
        claimed,
        derived_moneyflow,
        source="moneyflow_derived",
    )
    daily_basic_derived = _daily_basic_derived(raw_aux["daily_basic"])
    stats["daily_basic_precomputed"] = _fill_static(
        storage,
        daily.index,
        claimed,
        daily_basic_derived,
        source="daily_basic_precomputed",
    )
    price_context = _combine_indexed_frames(previous_state.price_tail, daily)
    price = pd.DataFrame(index=price_context.index)
    price["PriceStrength_10D"] = price_context["close"].groupby(level="instrument").pct_change(10)
    stats["price_momentum"] = _fill_static(
        storage,
        daily.index,
        claimed,
        price.reindex(daily.index),
        source="price_momentum",
    )
    static = pd.DataFrame(
        storage,
        index=daily.index,
        columns=list(expected_columns),
        copy=False,
    )
    if (
        tuple(static.columns) != tuple(expected_columns)
        or {str(column): str(dtype) for column, dtype in static.dtypes.items()} != STATIC_COLUMN_DTYPES
    ):
        raise FactorMaterializationError("static schema/dtype authority drifted")
    return static, stats


def _daily_basic_derived(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    output = pd.DataFrame(index=frame.index)
    pe = frame["db_pe_ttm"] if "db_pe_ttm" in frame else frame.get("db_pe", pd.Series(np.nan, index=frame.index))
    output["value_pe_inv"] = 1.0 / pd.to_numeric(pe, errors="coerce").replace(0, np.nan)
    pb = frame.get("db_pb", pd.Series(np.nan, index=frame.index))
    output["value_pb_inv"] = 1.0 / pd.to_numeric(pb, errors="coerce").replace(0, np.nan)
    mv = (
        frame["db_circ_mv"] if "db_circ_mv" in frame else frame.get("db_total_mv", pd.Series(np.nan, index=frame.index))
    )
    mv = pd.to_numeric(mv, errors="coerce")
    output["size_log_mv"] = np.where(mv > 0, np.log(mv), np.nan).astype("float32")
    output["liquidity_turnover"] = pd.to_numeric(
        frame.get("db_turnover_rate", pd.Series(np.nan, index=frame.index)),
        errors="coerce",
    ).astype("float32")
    output["liquidity_vol_ratio"] = pd.to_numeric(
        frame.get("db_volume_ratio", pd.Series(np.nan, index=frame.index)),
        errors="coerce",
    ).astype("float32")
    return output.sort_index()


def _fill_static(
    storage: Mapping[str, np.ndarray],
    target_index: pd.MultiIndex,
    claimed: set[str],
    frame: pd.DataFrame,
    *,
    source: str,
) -> dict[str, Any]:
    if frame.empty:
        return {"source": source, "rows": 0, "matched_rows": 0, "columns": []}
    columns = [str(value) for value in frame.columns if str(value) in storage and str(value) not in claimed]
    positions = target_index.get_indexer(frame.index)
    valid = positions >= 0
    for column in columns:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        values = (
            numeric.fillna(_UNKNOWN_L2_CODE_ID).to_numpy(dtype=np.int16, copy=False)
            if column == "l2_code_id"
            else numeric.to_numpy(dtype=np.float32, na_value=np.nan, copy=False)
        )
        storage[column][positions[valid]] = values[valid]
    claimed.update(columns)
    return {
        "source": source,
        "rows": len(frame),
        "matched_rows": int(valid.sum()),
        "columns": columns,
    }


def _advance_factor_state(
    previous: RollingFactorState,
    daily: pd.DataFrame,
    moneyflow: pd.DataFrame,
    *,
    adj_tail: pd.DataFrame,
    slow_tails: Mapping[str, pd.DataFrame],
) -> RollingFactorState:
    price_tail = _tail_by_instrument(_combine_indexed_frames(previous.price_tail, daily), 10)
    moneyflow_tail = _tail_by_instrument(_combine_indexed_frames(previous.moneyflow_tail, moneyflow), 19)
    pv = _combine_indexed_frames(previous.moneyflow_pv_tail, daily)
    pv_tail = pv.reindex(moneyflow_tail.index) if not moneyflow_tail.empty else pd.DataFrame()
    return RollingFactorState(
        price_tail,
        moneyflow_tail,
        pv_tail,
        adj_tail,
        dict(slow_tails),
    )


def _tail_by_instrument(frame: pd.DataFrame, rows: int) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    return frame.sort_index().groupby(level="instrument", sort=False, group_keys=False).tail(rows).sort_index()


def _combine_indexed_frames(*frames: pd.DataFrame) -> pd.DataFrame:
    material = [value for value in frames if isinstance(value, pd.DataFrame) and not value.empty]
    if not material:
        return _empty_multiindex_frame()
    output = pd.concat(material).sort_index()
    return output[~output.index.duplicated(keep="last")]


def _write_factor_state(root: Path, state: RollingFactorState, *, row_group_rows: int) -> dict[str, Any]:
    root.mkdir()
    frames = {
        "price_tail": state.price_tail,
        "moneyflow_tail": state.moneyflow_tail,
        "moneyflow_pv_tail": state.moneyflow_pv_tail,
    }
    frames.update({f"slow_{key}": value for key, value in state.slow_tails.items()})
    receipts: dict[str, Any] = {}
    for name, frame in frames.items():
        if frame.empty:
            receipts[name] = None
            continue
        receipt = write_frame_parquet_atomic(frame, root / f"{name}.parquet", row_group_size=row_group_rows)
        receipt.pop("path", None)
        receipt["relative_path"] = f"state/{name}.parquet"
        receipts[name] = receipt
    adj_path = root / "adj_factor_tail.json"
    adj_records = []
    if not state.adj_factor_tail.empty:
        for row in state.adj_factor_tail.itertuples(index=False):
            adj_records.append(
                {
                    "ts_code": str(row.ts_code),
                    "trade_date": str(row.trade_date),
                    "adj_factor": float(row.adj_factor),
                }
            )
    _write_checkpoint(adj_path, {"rows": adj_records})
    receipts["adj_factor_tail"] = {
        "relative_path": "state/adj_factor_tail.json",
        "sha256": sha256_file(adj_path),
        "rows": len(adj_records),
    }
    return receipts


def _restore_factor_state(root: Path, *, max_rows: int) -> RollingFactorState:
    state_root = root / "state"

    def read(name: str) -> pd.DataFrame:
        path = state_root / f"{name}.parquet"
        if not path.exists():
            return _empty_multiindex_frame()
        frames = list(iter_parquet_frames([path], max_rows=max_rows))
        return pd.concat(frames).sort_index() if frames else _empty_multiindex_frame()

    adj_payload = _load_json_file(state_root / "adj_factor_tail.json")
    adj = pd.DataFrame(adj_payload.get("rows") or [], columns=["ts_code", "trade_date", "adj_factor"])
    if not adj.empty:
        adj["trade_date"] = pd.to_datetime(adj["trade_date"]).dt.date
    slow = {
        dataset: read(f"slow_{dataset}")
        for dataset in ("daily_basic", "bak_basic", "cyq_perf", "sector_data", "margin_detail")
    }
    return RollingFactorState(
        read("price_tail"),
        read("moneyflow_tail"),
        read("moneyflow_pv_tail"),
        adj,
        slow,
    )


def _produced_chunks(root: Path, receipt: Mapping[str, Any], sealed_root: Path) -> list[SealedFactorChunk]:
    output: list[SealedFactorChunk] = []
    for dataset, artifact in sorted((receipt.get("artifacts") or {}).items()):
        path = root / f"{dataset}.parquet"
        parquet = pq.ParquetFile(path)
        output.append(
            SealedFactorChunk(
                dataset=dataset,
                partition_key=str(receipt["partition_key"]),
                relative_path=path.relative_to(sealed_root).as_posix(),
                sha256=str(artifact["sha256"]),
                rows=int(artifact["rows"]),
                ordered_columns=_parquet_frame_columns(parquet),
            )
        )
    return output


def merge_factor_partition_by_instrument(
    *,
    baseline_path: Path,
    replacement_path: Path,
    target_path: Path,
    dataset: str,
    partition_key: str,
    affected_instruments: Sequence[str],
    row_group_rows: int,
    max_rows: int,
) -> tuple[SealedFactorChunk, Mapping[str, Any]]:
    """Boundedly replace selected MultiIndex rows inside one monthly chunk."""

    affected = tuple(sorted({str(value).upper() for value in affected_instruments}))
    if (
        not affected
        or any(_STOCK_CODE.fullmatch(value) is None for value in affected)
        or type(max_rows) is not int
        or max_rows <= 0
    ):
        raise FactorMaterializationError("selective factor partition instruments/bound are invalid")
    baseline = Path(baseline_path).resolve(strict=True)
    replacement = Path(replacement_path).resolve(strict=True)
    for path in (baseline, replacement):
        _assert_plain(path)
        if not path.is_file():
            raise FactorMaterializationError("selective factor partition input is not a plain file")
    baseline_parquet = pq.ParquetFile(baseline)
    replacement_parquet = pq.ParquetFile(replacement)
    baseline_rows = int(baseline_parquet.metadata.num_rows)
    replacement_rows = int(replacement_parquet.metadata.num_rows)
    if baseline_rows + replacement_rows > max_rows:
        raise FactorMaterializationError("selective factor partition merge exceeds bounded rows")
    baseline_frames = list(iter_parquet_frames([baseline], max_rows=row_group_rows))
    replacement_frames = list(iter_parquet_frames([replacement], max_rows=row_group_rows))
    if not baseline_frames or not replacement_frames:
        raise FactorMaterializationError("selective factor partition merge input is empty")
    baseline_frame = pd.concat(baseline_frames).sort_index()
    replacement_frame = pd.concat(replacement_frames).sort_index()
    del baseline_frames, replacement_frames
    if (
        list(baseline_frame.index.names) != ["datetime", "instrument"]
        or list(replacement_frame.index.names) != ["datetime", "instrument"]
        or tuple(baseline_frame.columns) != tuple(replacement_frame.columns)
        or {str(key): str(value) for key, value in baseline_frame.dtypes.items()}
        != {str(key): str(value) for key, value in replacement_frame.dtypes.items()}
        or baseline_frame.index.has_duplicates
        or replacement_frame.index.has_duplicates
    ):
        raise FactorMaterializationError("selective factor partition schema/index differs")
    observed_codes = {str(value).upper() for value in replacement_frame.index.get_level_values("instrument")}
    if not observed_codes or not observed_codes.issubset(affected):
        raise FactorMaterializationError("selective factor replacement exceeds affected instruments")
    baseline_unaffected = baseline_frame.loc[~baseline_frame.index.get_level_values("instrument").isin(affected)]
    unaffected_rows = len(baseline_unaffected)
    inherited_digest = _factor_frame_digest(baseline_unaffected)
    merged = pd.concat([baseline_unaffected, replacement_frame]).sort_index()
    if merged.index.has_duplicates or len(merged) > max_rows:
        raise FactorMaterializationError("selective factor partition merge is duplicate/oversized")
    if (
        _factor_frame_digest(merged.loc[~merged.index.get_level_values("instrument").isin(affected)])
        != inherited_digest
    ):
        raise FactorMaterializationError("selective factor unaffected rows changed")
    target = Path(target_path)
    if target.exists():
        raise FactorMaterializationError("selective factor partition target already exists")
    receipt = write_frame_parquet_atomic(merged, target, row_group_size=row_group_rows)
    del baseline_frame, replacement_frame, baseline_unaffected, merged
    chunk = SealedFactorChunk(
        dataset=dataset,
        partition_key=partition_key,
        relative_path=target.name,
        sha256=str(receipt["sha256"]),
        rows=int(receipt["rows"]),
        ordered_columns=_parquet_frame_columns(pq.ParquetFile(target)),
    )
    return chunk, {
        "schema_version": "dataset_release_factor_partition_selective_merge_v1",
        "dataset": dataset,
        "partition_key": partition_key,
        "affected_instruments": list(affected),
        "baseline_rows": baseline_rows,
        "replacement_rows": replacement_rows,
        "output_rows": chunk.rows,
        "unaffected_rows": unaffected_rows,
        "unaffected_semantic_digest": inherited_digest,
        "output_sha256": chunk.sha256,
        "peak_rows_in_memory": baseline_rows + replacement_rows,
        "whole_market_history_frames_retained": 0,
    }


def _factor_frame_digest(frame: pd.DataFrame) -> str:
    hashed = pd.util.hash_pandas_object(frame, index=True).to_numpy(dtype=np.uint64, copy=False)
    return digest_named_fields(
        "dataset_release_factor_frame_semantics_v1",
        {
            "rows": len(frame),
            "columns": [str(value) for value in frame.columns],
            "hash": hashlib.sha256(hashed.tobytes()).hexdigest(),
        },
    )


def _verify_produced_partition(root: Path, receipt: Mapping[str, Any]) -> None:
    if receipt.get("status") != "PASS" or not root.is_dir():
        raise FactorCheckpointConflict("sealed factor partition is incomplete")
    for dataset, artifact in (receipt.get("artifacts") or {}).items():
        path = root / f"{dataset}.parquet"
        if (
            not path.is_file()
            or sha256_file(path) != artifact.get("sha256")
            or int(pq.ParquetFile(path).metadata.num_rows) != int(artifact.get("rows", -1))
        ):
            raise FactorCheckpointConflict(f"sealed factor partition readback differs: {dataset}")
    partition_receipt = root / "partition_receipt.json"
    if not partition_receipt.is_file():
        raise FactorCheckpointConflict("sealed factor partition receipt is missing")


def _load_producer_checkpoint(path: Path, digest: str) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": "dataset_release_factor_producer_checkpoint_v1",
            "producer_spec_digest": digest,
            "completed_partitions": [],
            "status": "IN_PROGRESS",
        }
    value = _load_json_file(path)
    if (
        value.get("schema_version") != "dataset_release_factor_producer_checkpoint_v1"
        or value.get("producer_spec_digest") != digest
    ):
        raise FactorCheckpointConflict("factor producer checkpoint identity differs")
    return value


def _load_json_file(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FactorCheckpointConflict(f"factor JSON evidence is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise FactorCheckpointConflict("factor JSON evidence must be an object")
    return value


def _empty_like_aux(
    dataset: str,
    raw_aux: Mapping[str, pd.DataFrame],
    _target_index: pd.MultiIndex,
) -> pd.DataFrame:
    return raw_aux[dataset].copy()


def _factor_schema_authority() -> dict[str, Any]:
    return {
        "factor_h5_schema_version": FACTOR_H5_SCHEMA_VERSION,
        "factor_h5_schemas": {key: list(value) for key, value in FACTOR_H5_SCHEMAS.items()},
        "factor_h5_dtypes": {key: dict(value) for key, value in FACTOR_H5_DTYPES.items()},
        "factor_h5_density_contracts": dict(FACTOR_H5_DENSITY_CONTRACTS),
        "static_schema_version": STATIC_SCHEMA_VERSION,
        "static_schema_digest": static_schema_digest(),
        "static_ordered_columns": list(STATIC_ORDERED_COLUMNS),
        "static_column_dtypes": dict(STATIC_COLUMN_DTYPES),
    }


def _empty_aux_frame(dataset: str) -> pd.DataFrame:
    if dataset == "moneyflow":
        columns = tuple(MONEYFLOW_FIELD_MAP.values())
    elif dataset == "sector_data":
        columns = STATIC_SECTOR_COLUMNS
    elif dataset in _AUX_RENAMES:
        columns = tuple(_AUX_RENAMES[dataset].values())
    else:
        raise FactorMaterializationError(f"unsupported empty auxiliary dataset: {dataset}")
    frame = pd.DataFrame(index=_empty_multiindex_frame().index)
    for column in columns:
        frame[column] = pd.Series([], dtype="int16" if column == "l2_code_id" else "float32")
    return frame


def _write_produced_artifact(
    root: Path,
    *,
    dataset: str,
    partition_key: str,
    frame: pd.DataFrame,
    row_group_rows: int,
) -> dict[str, Any]:
    expected_columns = STATIC_ORDERED_COLUMNS if dataset == STATIC_DATASET else FACTOR_H5_SCHEMAS[dataset]
    expected_dtypes = STATIC_COLUMN_DTYPES if dataset == STATIC_DATASET else FACTOR_H5_DTYPES[dataset]
    actual_columns = tuple(str(value) for value in frame.columns)
    actual_dtypes = {str(column): str(dtype) for column, dtype in frame.dtypes.items()}
    if actual_columns != expected_columns or actual_dtypes != expected_dtypes:
        raise FactorMaterializationError(f"produced {dataset} schema/dtype differs from authority")
    path = root / f"{dataset}.parquet"
    receipt = write_frame_parquet_atomic(frame, path, row_group_size=row_group_rows)
    receipt.pop("path", None)
    receipt["relative_path"] = f"{partition_key}/{dataset}.parquet"
    return receipt


def _empty_multiindex_frame() -> pd.DataFrame:
    index = pd.MultiIndex.from_arrays([[], []], names=["datetime", "instrument"])
    return pd.DataFrame(index=index)


def _reset_source_index(frame: pd.DataFrame) -> pd.DataFrame:
    if isinstance(frame.index, pd.MultiIndex) and list(frame.index.names) == [
        "datetime",
        "instrument",
    ]:
        value = frame.reset_index().rename(columns={"datetime": "trade_date", "instrument": "ts_code"})
    else:
        value = frame.reset_index(drop=True)
    return value


def _dataset_paths(
    spec: FactorMaterializationSpec,
    local: Mapping[tuple[str, str], Path],
    dataset: str,
) -> list[Path]:
    return [
        local[(item.dataset, item.partition_key)]
        for item in sorted(spec.chunks, key=lambda value: value.partition_key)
        if item.dataset == dataset
    ]


def _audit_parquet_chunk(
    path: Path,
    item: SealedFactorChunk,
    max_rows: int,
) -> dict[str, Any]:
    if sha256_file(path) != item.sha256:
        raise FactorMaterializationError(f"sealed factor chunk hash mismatch: {item.dataset}:{item.partition_key}")
    parquet = pq.ParquetFile(path)
    actual_rows = int(parquet.metadata.num_rows)
    largest = max(
        (int(parquet.metadata.row_group(index).num_rows) for index in range(parquet.num_row_groups)),
        default=0,
    )
    if largest > max_rows:
        raise ArtifactChunkTooLarge(f"sealed factor row group exceeds bound before read: {path} rows={largest}")
    actual_columns = _parquet_frame_columns(parquet)
    if actual_rows != item.rows or actual_columns != item.ordered_columns:
        raise ArtifactSchemaDrift(f"sealed factor chunk metadata drift: {item.dataset}:{item.partition_key}")
    expected_dtypes = STATIC_COLUMN_DTYPES if item.dataset == STATIC_DATASET else FACTOR_H5_DTYPES[item.dataset]
    _assert_parquet_dtypes(path, expected_dtypes=expected_dtypes, max_rows=max_rows)
    return {
        "rows": actual_rows,
        "max_row_group_rows": largest,
        "size_bytes": int(path.stat().st_size),
    }


def _audit_existing_h5(
    path: Path,
    *,
    expected_columns: Sequence[str],
    expected_dtypes: Mapping[str, str],
    max_rows: int,
) -> dict[str, Any]:
    if not path.is_file():
        raise FactorCheckpointConflict(f"recorded H5 output is missing: {path}")
    rows = 0
    chunks = 0
    dtypes: dict[str, str] | None = None
    for frame in iter_hdf_frames(path, chunksize=max_rows):
        actual = tuple(str(value) for value in frame.columns)
        if actual != tuple(expected_columns):
            raise ArtifactSchemaDrift(f"existing H5 schema drift: {path}")
        current = {str(column): str(dtype) for column, dtype in frame.dtypes.items()}
        if current != dict(expected_dtypes):
            raise ArtifactSchemaDrift(f"existing H5 authority dtype drift: {path}")
        if dtypes is None:
            dtypes = current
        elif current != dtypes:
            raise ArtifactSchemaDrift(f"existing H5 dtype drift: {path}")
        rows += len(frame)
        chunks += 1
    if rows <= 0:
        raise FactorCheckpointConflict(f"existing H5 contains no rows: {path}")
    return {
        "schema_version": "dataset_release_hdf_table_v1",
        "path": str(path),
        "sha256": sha256_file(path),
        "rows": rows,
        "columns": list(expected_columns),
        "dtypes": dtypes or {},
        "stream_chunks": chunks,
        "max_rows_in_memory": max_rows,
        "format": "pandas_hdf_table_v1",
        "size_bytes": int(path.stat().st_size),
    }


def _audit_existing_parquet(
    path: Path,
    *,
    expected_columns: Sequence[str],
    max_rows: int,
) -> dict[str, Any]:
    if not path.is_file():
        raise FactorCheckpointConflict(f"recorded Parquet output is missing: {path}")
    parquet = pq.ParquetFile(path)
    columns = _parquet_frame_columns(parquet)
    if columns != tuple(expected_columns):
        raise ArtifactSchemaDrift(f"existing static schema drift: {path}")
    largest = max(
        (int(parquet.metadata.row_group(index).num_rows) for index in range(parquet.num_row_groups)),
        default=0,
    )
    if largest > max_rows:
        raise ArtifactChunkTooLarge(f"existing static row group exceeds bound: {largest}>{max_rows}")
    _assert_parquet_dtypes(path, expected_dtypes=STATIC_COLUMN_DTYPES, max_rows=max_rows)
    return {
        "schema_version": "dataset_release_parquet_aggregate_v1",
        "path": str(path),
        "sha256": sha256_file(path),
        "rows": int(parquet.metadata.num_rows),
        "row_groups": int(parquet.num_row_groups),
        "max_rows_in_memory": largest,
        "size_bytes": int(path.stat().st_size),
        "columns": list(columns),
    }


def _assert_l2_int16(path: Path, max_rows: int) -> None:
    seen = False
    for frame in iter_parquet_frames([path], max_rows=max_rows):
        if "l2_code_id" not in frame.columns:
            raise ArtifactSchemaDrift(f"l2_code_id missing: {path}")
        if str(frame["l2_code_id"].dtype) != "int16":
            raise ArtifactSchemaDrift(f"l2_code_id must be int16: {path}")
        seen = seen or not frame.empty
    if not seen:
        raise FactorMaterializationError(f"l2_code_id validation read no rows: {path}")


def _assert_parquet_dtypes(
    path: Path,
    *,
    expected_dtypes: Mapping[str, str],
    max_rows: int,
) -> None:
    checked = False
    for frame in iter_parquet_frames([path], max_rows=max_rows):
        actual = {str(column): str(dtype) for column, dtype in frame.dtypes.items()}
        if actual != dict(expected_dtypes):
            raise ArtifactSchemaDrift(f"sealed Parquet authority dtype drift: {path}")
        checked = True
    if not checked:
        empty = pd.read_parquet(path)
        actual = {str(column): str(dtype) for column, dtype in empty.dtypes.items()}
        if actual != dict(expected_dtypes):
            raise ArtifactSchemaDrift(f"sealed Parquet authority dtype drift: {path}")


def _parquet_frame_columns(parquet: pq.ParquetFile) -> tuple[str, ...]:
    names = tuple(str(value) for value in parquet.schema_arrow.names)
    index_names = {"datetime", "instrument", "__index_level_0__"}
    return tuple(value for value in names if value not in index_names)


def _publish_sealed_copy(source: Path, target: Path, *, expected_sha256: str) -> None:
    if target.exists():
        _assert_plain(target)
        if not target.is_file() or sha256_file(target) != expected_sha256:
            raise FactorCheckpointConflict(f"sealed chunk target conflicts: {target}")
        return
    # Source authorities and candidate bytes deliberately never share an
    # inode. A source-cache defect must not mutate staged candidate bytes.
    descriptor, name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".partial", dir=target.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as output, source.open("rb") as input_:
            shutil.copyfileobj(input_, output, length=1024 * 1024)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError:
            if sha256_file(target) != expected_sha256:
                raise FactorCheckpointConflict(f"sealed chunk target conflicts: {target}")
    finally:
        temporary.unlink(missing_ok=True)
    if sha256_file(target) != expected_sha256:
        raise FactorCheckpointConflict(f"sealed chunk copy readback differs: {target}")


def _load_checkpoint(path: Path, spec_digest: str) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": FACTOR_CHECKPOINT_SCHEMA,
            "spec_digest": spec_digest,
            "chunks": [],
            "outputs": {},
            "status": "IN_PROGRESS",
        }
    _assert_plain(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FactorCheckpointConflict("factor checkpoint is unreadable") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != FACTOR_CHECKPOINT_SCHEMA
        or payload.get("spec_digest") != spec_digest
    ):
        raise FactorCheckpointConflict("factor checkpoint identity differs")
    return payload


def _write_checkpoint(path: Path, payload: Mapping[str, Any]) -> None:
    raw = (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".partial", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    if path.read_bytes() != raw:
        raise FactorCheckpointConflict("factor checkpoint readback differs")


def _require_recorded_output(recorded: Mapping[str, Any], actual: Mapping[str, Any], dataset: str) -> None:
    for field in ("sha256", "rows", "columns"):
        if field in recorded and recorded.get(field) != actual.get(field):
            raise FactorCheckpointConflict(f"recorded factor output differs: dataset={dataset} field={field}")


def _portable_output_receipt(receipt: Mapping[str, Any], *, root: Path) -> dict[str, Any]:
    payload = dict(receipt)
    raw_path = payload.pop("path", None)
    if raw_path is None:
        raise FactorMaterializationError("factor output receipt omits path")
    path = Path(str(raw_path)).resolve(strict=True)
    try:
        relative = path.relative_to(root).as_posix()
    except ValueError as exc:
        raise FactorMaterializationError("factor output escapes attempt staging") from exc
    payload["artifact_relative_path"] = relative
    return payload


def _safe_relative(value: str) -> str:
    raw = str(value).replace("\\", "/").strip()
    path = PurePosixPath(raw)
    if (
        not raw
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or any(character in raw for character in ("*", "?", "[", "]", ":"))
    ):
        raise FactorMaterializationError(f"unsafe sealed factor path: {value!r}")
    return path.as_posix()


def _contained_source(root: Path, relative: str) -> Path:
    path = (root / Path(_safe_relative(relative))).resolve(strict=True)
    if root not in path.parents or not path.is_file():
        raise FactorMaterializationError("sealed factor path escapes source root")
    _assert_plain(path)
    return path


def _plain_root(path: Path, *, must_exist: bool) -> Path:
    requested = Path(path).expanduser()
    if not requested.is_absolute():
        requested = requested.absolute()
    resolved = requested.resolve(strict=must_exist)
    if must_exist and not resolved.is_dir():
        raise FactorMaterializationError(f"factor root is not a directory: {resolved}")
    current = Path(resolved.anchor)
    if current.exists():
        _assert_plain(current)
    for part in resolved.parts[1:]:
        current = current / part
        if current.exists():
            _assert_plain(current)
    return resolved


def _assert_plain(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise FactorMaterializationError(f"factor path is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or (int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise FactorMaterializationError(f"factor path traverses reparse/symlink: {path}")


__all__ = [
    "FACTOR_H5_DATASETS",
    "FACTOR_H5_DTYPES",
    "FACTOR_H5_DENSITY_CONTRACTS",
    "FACTOR_H5_SCHEMAS",
    "FACTOR_H5_SCHEMA_VERSION",
    "FACTOR_SOURCE_SCHEMAS",
    "FactorBundleMaterializer",
    "FactorCheckpointConflict",
    "FactorMaterializationError",
    "FactorMaterializationReceipt",
    "FactorMaterializationSpec",
    "FactorPartitionProducer",
    "FactorPartitionProducerSpec",
    "FactorSourcePartition",
    "RollingFactorState",
    "STATIC_DATASET",
    "SealedFactorChunk",
    "merge_factor_partition_by_instrument",
    "merge_rolling_factor_states_by_instrument",
    "restore_rolling_factor_state_from_bundle",
    "restore_rolling_factor_state_from_produced_partition",
]
