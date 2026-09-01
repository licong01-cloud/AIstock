"""Immutable C-012-RL1 training input bundle.

The bundle is deliberately smaller than the source dataset and deliberately
different from a model/capability bundle.  It freezes only the approved L1/L2
feature panels, benchmark calendar and compact source identities required by
the two fresh model processes.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
import re
import shutil
import struct
import tempfile
import time
import unicodedata
import uuid
from bisect import bisect_right
from collections import defaultdict, deque
from collections.abc import Iterator, Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pandas as pd
import psutil
import tables

from backend.qlib_exporter.authoritative_bin_exporter import read_qlib_bin
from backend.services.canonical_pit_dataset_consumer import FormalDatasetUsage
from backend.services.dataset_release.cas_store import canonical_json_bytes
from backend.services.dataset_release.copy_on_write import CopyOnWriteError, tree_merkle
from backend.services.dataset_release.stock_schema import (
    QLIB_STOCK_FIELDS,
    QLIB_STOCK_SCHEMA_VERSION,
    QLIB_STOCK_VALUE_CONTRACT,
    qlib_stock_schema_digest,
)
from backend.services.hmm_risk.industry_pit_adapter import HMMIndustryPitAdapter, HMM_MAPPING_MANIFEST_SCHEMA
from backend.services.hmm_risk.provider_absence import load_provider_absence_manifest
from backend.services.hmm_risk.security_identity import load_security_source_identity_manifest
from backend.services.hmm_risk.state_model_set import canonical_sha256
from backend.services.hmm_risk.stock_fact_observation import (
    C010_APPROVED_TRAIN_END,
    C010_APPROVED_TRAIN_START,
    C010_FORMULA_VERSION,
    MIN_COVERAGE,
    ObservationCoverageError,
    aggregate_l1_day,
    build_c010_feature_domain_panel,
)
from backend.services.quantevolver.qe_dataset_contract import QEFormalDatasetBinding


INPUT_CONTRACT_VERSION = "C-012-RL1-IB-D1-D6"
ALGORITHM_VERSION = "hmm_risk_rotation_l1_input_bundle_v1"
MANIFEST_SCHEMA_VERSION = "hmm_risk_rotation_l1_input_bundle_manifest_v2"
BUILD_RECEIPT_SCHEMA_VERSION = "hmm_risk_rotation_l1_input_bundle_build_receipt_v1"
CANONICAL_SERIALIZATION_VERSION = "hmm_risk_rotation_l1_input_bundle_canonical_v1"
SOURCE_REVISION = "c013-g2a-hmm-input-bundle-v1"
SOURCE_ASSET_SCHEMA_VERSION = "hmm_risk_dataset_release_asset_binding_v1"
SOURCE_INVENTORY_SCHEMA_VERSION = "hmm_risk_rotation_l1_source_inventory_v1"
SOURCE_BUILD_STAGES = (
    "source_inventory",
    "qlib_month_spool",
    "stock_fact_aggregation",
    "feature_panel_complete",
)
BUILD_MAX_SECONDS = 20 * 60
BUILD_MAX_RSS_BYTES = 4 * 1024**3
READBACK_MAX_SECONDS = 120
READBACK_MAX_RSS_BYTES = 2 * 1024**3
SOURCE_START = date(2020, 7, 30)
SOURCE_END = date(2026, 3, 31)
HOLDOUT_START = date(2026, 4, 1)

FEATURE_NAMES = (
    "daily_return",
    "volatility_Nd",
    "net_mf_ratio",
    "sf_breadth_5d",
    "sf_dispersion_5d_neg",
    "excess_return_Nd",
    "elg_net_mf_ratio",
    "sf_excess_breadth_5d",
    "sf_turnover_pctile_120d_neg",
)

REASON_MANIFEST_INVALID = "hmm_risk_rotation_l1_input_bundle_manifest_invalid"
REASON_SOURCE_COMPONENT_MISSING = "hmm_risk_rotation_l1_input_bundle_source_component_missing"
REASON_SOURCE_SCHEMA_INVALID = "hmm_risk_rotation_l1_input_bundle_source_schema_invalid"
REASON_SOURCE_UNIT_INVALID = "hmm_risk_rotation_l1_input_bundle_source_unit_invalid"
REASON_SOURCE_RANGE_INCOMPLETE = "hmm_risk_rotation_l1_input_bundle_source_range_incomplete"
REASON_AUTHORITY_AMBIGUOUS = "hmm_risk_rotation_l1_input_bundle_authority_ambiguous"
REASON_HOLDOUT_CONTAMINATION = "hmm_risk_rotation_l1_input_bundle_holdout_contamination"
REASON_DUPLICATE_KEY = "hmm_risk_rotation_l1_input_bundle_duplicate_key"
REASON_NON_FINITE = "hmm_risk_rotation_l1_input_bundle_non_finite"
REASON_MASK_MISMATCH = "hmm_risk_rotation_l1_input_bundle_mask_mismatch"
REASON_HASH_MISMATCH = "hmm_risk_rotation_l1_input_bundle_hash_mismatch"
REASON_COLLISION = "hmm_risk_rotation_l1_input_bundle_collision"
REASON_INCOMPLETE = "hmm_risk_rotation_l1_input_bundle_incomplete"
REASON_DB_FALLBACK = "hmm_risk_rotation_l1_input_bundle_db_fallback_forbidden"
REASON_RESOURCE_BUDGET = "hmm_risk_rotation_l1_input_bundle_resource_budget_exceeded"

_SHA256_HEX = frozenset("0123456789abcdef")
_STOCK_CODE = re.compile(r"^[0-9]{6}[.](?:SH|SZ)$")
_PANEL_DTYPE = np.dtype(
    [("trade_date", "<i4"), ("sector_code", "S16"), *[(name, "<f8") for name in FEATURE_NAMES]],
    align=False,
)
_VALIDITY_DTYPE = np.dtype(
    [("trade_date", "<i4"), ("sector_code", "S16"), *[(name, "u1") for name in FEATURE_NAMES]],
    align=False,
)
_UNAVAILABLE_DTYPE = np.dtype(
    [
        ("trade_date", "<i4"),
        ("level", "S2"),
        ("sector_code", "S16"),
        ("field", "S64"),
        ("reason_code", "S128"),
        ("source_observation_date", "<i4"),
    ],
    align=False,
)
_SECURITY_INTERVAL_DTYPE = np.dtype(
    [
        ("canonical_security_id", "S16"),
        ("source_dataset", "S32"),
        ("valid_from", "<i4"),
        ("valid_to", "<i4"),
        ("source_code", "S16"),
    ],
    align=False,
)
_INDUSTRY_INTERVAL_DTYPE = np.dtype(
    [
        ("canonical_security_id", "S16"),
        ("effective_from", "<i4"),
        ("effective_to", "<i4"),
        ("l1_code", "S16"),
        ("l2_code", "S16"),
    ],
    align=False,
)
_SOURCE_STATUS_INTERVAL_DTYPE = np.dtype(
    [
        ("canonical_security_id", "S16"),
        ("valid_from", "<i4"),
        ("valid_to", "<i4"),
        ("status", "S32"),
        ("reason_code", "S128"),
        ("provider", "S64"),
    ],
    align=False,
)

_INTERVAL_DATASETS = (
    "security_identity_intervals",
    "industry_projection_intervals",
    "source_status_intervals",
)
_SECURITY_INTERVAL_SOURCE_DATASETS = frozenset({"market.daily_basic", "market.moneyflow_ts"})
_QLIB_SOURCE_DTYPE = np.dtype(
    [("trade_date", "<i4"), ("symbol", "S16"), *[(field, "<f4") for field in QLIB_STOCK_FIELDS]],
    align=False,
)

_DAILY_BASIC_COLUMNS = (
    "db_close",
    "db_turnover_rate",
    "db_turnover_rate_f",
    "db_volume_ratio",
    "db_pe",
    "db_pe_ttm",
    "db_pb",
    "db_ps",
    "db_ps_ttm",
    "db_dv_ratio",
    "db_dv_ttm",
    "db_total_share",
    "db_float_share",
    "db_free_share",
    "db_total_mv",
    "db_circ_mv",
)
_MONEYFLOW_COLUMNS = (
    "mf_sm_buy_vol",
    "mf_sm_buy_amt",
    "mf_sm_sell_vol",
    "mf_sm_sell_amt",
    "mf_md_buy_vol",
    "mf_md_buy_amt",
    "mf_md_sell_vol",
    "mf_md_sell_amt",
    "mf_lg_buy_vol",
    "mf_lg_buy_amt",
    "mf_lg_sell_vol",
    "mf_lg_sell_amt",
    "mf_elg_buy_vol",
    "mf_elg_buy_amt",
    "mf_elg_sell_vol",
    "mf_elg_sell_amt",
    "mf_net_vol",
    "mf_net_amt",
)
_UNAVAILABLE_REASON_CODES = frozenset(
    {
        "hmm_risk_rotation_l1_feature_warmup",
        "hmm_risk_rotation_l1_moneyflow_feature_unavailable",
        "hmm_risk_rotation_l1_cross_section_feature_unavailable",
        "hmm_risk_rotation_l1_stock_fact_aggregate_unavailable",
        "hmm_risk_c010_price_domain_weight_denominator_invalid",
        "hmm_risk_c010_price_domain_coverage_insufficient",
        "hmm_risk_c010_moneyflow_domain_weight_denominator_invalid",
    }
)
_SOURCE_STATUS_VALUES = frozenset(
    {"available", "suspended", "industry_unavailable", "provider_absence", "source_invalid"}
)
_SOURCE_STATUS_PROVIDERS = frozenset({"frozen_release", "suspend_d", "c013", "tushare", "moneyflow_h5"})
_SOURCE_INVALID_REASONS = frozenset(
    {
        "hmm_risk_rotation_l1_moneyflow_invalid",
        "hmm_risk_rotation_l1_daily_basic_invalid",
        "hmm_risk_c010_price_unavailable_for_opportunity",
    }
)


class RotationL1InputBundleError(RuntimeError):
    """Fail-closed input bundle error with a stable reason code."""

    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


def _fail(reason_code: str, message: str, **context: Any) -> RotationL1InputBundleError:
    return RotationL1InputBundleError(reason_code, message, context=context)


def _resource_checkpoint(started: float, *, stage: str, max_seconds: float, max_rss_bytes: int) -> dict[str, Any]:
    elapsed = time.perf_counter() - started
    rss = int(psutil.Process(os.getpid()).memory_info().rss)
    if elapsed > max_seconds or rss > max_rss_bytes:
        raise _fail(
            REASON_RESOURCE_BUDGET,
            f"input bundle resource budget exceeded during {stage}",
            stage=stage,
            elapsed_seconds=elapsed,
            rss_bytes=rss,
            max_seconds=max_seconds,
            max_rss_bytes=max_rss_bytes,
        )
    return {"stage": stage, "elapsed_seconds": elapsed, "rss_bytes": rss}


def _sha256_file(path: Path, block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(block_size):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json_object(path: Path, *, reason: str = REASON_MANIFEST_INVALID) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _fail(reason, f"cannot read JSON object: {path.name}") from exc
    if not isinstance(value, dict):
        raise _fail(reason, f"JSON source is not an object: {path.name}")
    return value


def _verify_bound_file(root: Path, value: Any, field: str) -> Path:
    if not isinstance(value, Mapping) or set(value) != {"relative_path", "sha256"}:
        raise _fail(REASON_MANIFEST_INVALID, f"{field} file binding differs")
    relative = Path(str(value.get("relative_path") or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise _fail(REASON_MANIFEST_INVALID, f"{field} relative path escapes the release root")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise _fail(REASON_MANIFEST_INVALID, f"{field} path escapes the release root") from exc
    if not path.is_file():
        raise _fail(REASON_SOURCE_COMPONENT_MISSING, f"{field} file is missing")
    expected = _require_sha256(value.get("sha256"), f"{field}.sha256")
    if _sha256_file(path) != expected:
        raise _fail(REASON_HASH_MISMATCH, f"{field} file hash differs")
    return path


def _iter_fixed_h5_frames(
    path: Path,
    *,
    expected_columns: Sequence[str],
    expected_dtype: str | np.dtype[Any],
    max_rows: int = 100_000,
) -> Iterator[pd.DataFrame]:
    """Yield bounded frames from the frozen pandas fixed H5 physical schema.

    This is intentionally a narrow reader for the release's one-block,
    component-declared numeric dtype and two-level canonical frame.  Any other
    fixed-H5 layout fails instead of falling back to ``pd.read_hdf`` and
    loading the file in full.
    """

    if max_rows <= 0:
        raise _fail(REASON_RESOURCE_BUDGET, "fixed H5 max_rows must be positive")
    with tables.open_file(path, mode="r") as handle:
        try:
            group = handle.root.data
            axis0 = group.axis0.read()
            date_levels = group.axis1_level0.read()
            code_levels = group.axis1_level1.read()
            date_labels = group.axis1_label0
            code_labels = group.axis1_label1
            values = group.block0_values
        except tables.NoSuchNodeError as exc:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{path.name} fixed H5 nodes differ") from exc
        columns = tuple(bytes(value).rstrip(b"\x00").decode("utf-8") for value in axis0)
        if columns != tuple(expected_columns):
            raise _fail(
                REASON_SOURCE_SCHEMA_INVALID,
                f"{path.name} ordered columns differ",
                expected=list(expected_columns),
                actual=list(columns),
            )
        dtype = np.dtype(expected_dtype)
        if (
            values.dtype != dtype
            or values.ndim != 2
            or int(values.shape[1]) != len(columns)
            or int(date_labels.shape[0]) != int(values.shape[0])
            or int(code_labels.shape[0]) != int(values.shape[0])
            or np.asarray(date_levels).dtype != np.dtype("<i8")
        ):
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{path.name} fixed H5 dtype/shape differs")
        row_count = int(values.shape[0])
        previous: tuple[pd.Timestamp, str] | None = None
        for start in range(0, row_count, max_rows):
            stop = min(row_count, start + max_rows)
            date_index = np.asarray(date_labels.read(start, stop), dtype=np.int64)
            code_index = np.asarray(code_labels.read(start, stop), dtype=np.int64)
            if (
                np.any(date_index < 0)
                or np.any(date_index >= len(date_levels))
                or np.any(code_index < 0)
                or np.any(code_index >= len(code_levels))
            ):
                raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{path.name} fixed H5 labels escape levels")
            dates = pd.to_datetime(np.asarray(date_levels)[date_index], unit="ns")
            codes = [bytes(value).rstrip(b"\x00").decode("ascii") for value in np.asarray(code_levels)[code_index]]
            frame = pd.DataFrame(np.asarray(values.read(start, stop), dtype=dtype), columns=columns)
            frame.index = pd.MultiIndex.from_arrays([dates, codes], names=["datetime", "instrument"])
            if frame.index.has_duplicates or not frame.index.is_monotonic_increasing:
                raise _fail(REASON_DUPLICATE_KEY, f"{path.name} fixed H5 keys are not sorted and unique")
            first = (pd.Timestamp(frame.index[0][0]), str(frame.index[0][1]))
            last = (pd.Timestamp(frame.index[-1][0]), str(frame.index[-1][1]))
            if previous is not None and first <= previous:
                raise _fail(REASON_DUPLICATE_KEY, f"{path.name} fixed H5 chunks overlap")
            previous = last
            yield frame


def _fixed_h5_label_lower_bound(label_node: Any, value: int) -> int:
    lower = 0
    upper = int(label_node.shape[0])
    while lower < upper:
        middle = (lower + upper) // 2
        if int(label_node[middle]) < value:
            lower = middle + 1
        else:
            upper = middle
    return lower


def _validate_fixed_h5_date_labels(label_node: Any, *, level_count: int, max_rows: int = 100_000) -> None:
    if max_rows <= 0 or level_count <= 0:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "fixed H5 date label validation contract differs")
    previous: int | None = None
    row_count = int(label_node.shape[0])
    for start in range(0, row_count, max_rows):
        labels = np.asarray(label_node.read(start, min(row_count, start + max_rows)), dtype=np.int64)
        if (
            np.any(labels < 0)
            or np.any(labels >= level_count)
            or np.any(labels[1:] < labels[:-1])
            or (previous is not None and len(labels) and int(labels[0]) < previous)
        ):
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "fixed H5 date labels are not sorted within levels")
        if len(labels):
            previous = int(labels[-1])


def _load_fixed_h5_window(
    path: Path,
    *,
    expected_columns: Sequence[str],
    expected_dtype: str | np.dtype[Any],
    start: date,
    end: date,
    max_rows: int = 100_000,
    labels_prevalidated: bool = False,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    lower_ns = int(pd.Timestamp(start).value)
    upper_ns = int(pd.Timestamp(end).value)
    with tables.open_file(path, mode="r") as handle:
        try:
            group = handle.root.data
            columns = tuple(bytes(value).rstrip(b"\x00").decode("utf-8") for value in group.axis0.read())
            date_levels = np.asarray(group.axis1_level0.read(), dtype=np.int64)
            code_levels = np.asarray(group.axis1_level1.read())
            date_label_node = group.axis1_label0
            code_labels = group.axis1_label1
            values = group.block0_values
        except tables.NoSuchNodeError as exc:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{path.name} fixed H5 nodes differ") from exc
        dtype = np.dtype(expected_dtype)
        if (
            columns != tuple(expected_columns)
            or values.dtype != dtype
            or values.ndim != 2
            or int(values.shape[1]) != len(columns)
            or int(date_label_node.shape[0]) != int(values.shape[0])
            or int(code_labels.shape[0]) != int(values.shape[0])
            or date_levels.ndim != 1
            or np.any(date_levels[1:] <= date_levels[:-1])
        ):
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{path.name} fixed H5 layout differs")
        date_labels: np.ndarray | None
        if labels_prevalidated:
            date_labels = None
        else:
            date_labels = np.asarray(date_label_node.read(), dtype=np.int64)
            if (
                np.any(date_labels < 0)
                or np.any(date_labels >= len(date_levels))
                or np.any(date_labels[1:] < date_labels[:-1])
            ):
                raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{path.name} fixed H5 date labels differ")
        first_level = int(np.searchsorted(date_levels, lower_ns, side="left"))
        after_level = int(np.searchsorted(date_levels, upper_ns, side="right"))
        if date_labels is None:
            row_start = _fixed_h5_label_lower_bound(date_label_node, first_level)
            row_stop = _fixed_h5_label_lower_bound(date_label_node, after_level)
        else:
            row_start = int(np.searchsorted(date_labels, first_level, side="left"))
            row_stop = int(np.searchsorted(date_labels, after_level, side="left"))
        for chunk_start in range(row_start, row_stop, max_rows):
            chunk_stop = min(row_stop, chunk_start + max_rows)
            date_index = (
                np.asarray(date_label_node.read(chunk_start, chunk_stop), dtype=np.int64)
                if date_labels is None
                else date_labels[chunk_start:chunk_stop]
            )
            code_index = np.asarray(code_labels.read(chunk_start, chunk_stop), dtype=np.int64)
            if np.any(code_index < 0) or np.any(code_index >= len(code_levels)):
                raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{path.name} fixed H5 code labels escape levels")
            dates = pd.to_datetime(date_levels[date_index], unit="ns")
            codes = [bytes(value).rstrip(b"\x00").decode("ascii") for value in code_levels[code_index]]
            frame = pd.DataFrame(
                np.asarray(values.read(chunk_start, chunk_stop), dtype=dtype),
                columns=columns,
            )
            frame.index = pd.MultiIndex.from_arrays([dates, codes], names=["datetime", "instrument"])
            parts.append(frame)
    if not parts:
        return pd.DataFrame(
            columns=list(expected_columns), index=pd.MultiIndex.from_arrays([[], []], names=["datetime", "instrument"])
        )
    result = pd.concat(parts, axis=0)
    if result.index.has_duplicates or not result.index.is_monotonic_increasing:
        raise _fail(REASON_DUPLICATE_KEY, f"{path.name} window keys are not sorted and unique")
    return result


def _fixed_h5_inventory(
    path: Path,
    *,
    expected_columns: Sequence[str],
    expected_dtype: str | np.dtype[Any],
) -> dict[str, Any]:
    with tables.open_file(path, mode="r") as handle:
        try:
            group = handle.root.data
            columns = tuple(bytes(value).rstrip(b"\x00").decode("utf-8") for value in group.axis0.read())
            dates = np.asarray(group.axis1_level0.read(), dtype=np.int64)
            codes = np.asarray(group.axis1_level1.read())
            values = group.block0_values
            date_labels = group.axis1_label0
            rows = int(date_labels.shape[0])
        except tables.NoSuchNodeError as exc:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{path.name} fixed H5 inventory nodes differ") from exc
        dtype = np.dtype(expected_dtype)
        if (
            columns != tuple(expected_columns)
            or values.dtype != dtype
            or values.ndim != 2
            or int(values.shape[0]) != rows
            or int(values.shape[1]) != len(columns)
            or len(dates) == 0
            or len(codes) == 0
            or np.any(dates[1:] <= dates[:-1])
        ):
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{path.name} fixed H5 inventory contract differs")
        _validate_fixed_h5_date_labels(date_labels, level_count=len(dates))
        return {
            "columns": list(columns),
            "dtype": dtype.name,
            "date_min": pd.Timestamp(int(dates.min()), unit="ns").date().isoformat(),
            "date_max": pd.Timestamp(int(dates.max()), unit="ns").date().isoformat(),
            "code_count": len(codes),
            "row_count": rows,
        }


def _parse_instrument_spans(path: Path) -> dict[str, tuple[tuple[date, date], ...]]:
    spans: dict[str, list[tuple[date, date]]] = defaultdict(list)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        raise _fail(REASON_SOURCE_COMPONENT_MISSING, "Qlib instruments file cannot be read") from exc
    for line_number, raw in enumerate(lines, start=1):
        parts = raw.split("\t")
        if len(parts) != 3:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "Qlib instrument row schema differs", line=line_number)
        symbol = parts[0].strip().upper()
        _ascii(symbol, "instrument.symbol", width=16)
        if _STOCK_CODE.fullmatch(symbol) is None:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "Qlib instrument symbol is outside canonical sh/sz scope")
        start = _as_date(parts[1], "instrument.start")
        end = _as_date(parts[2], "instrument.end")
        if start > end:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "Qlib instrument span is reversed", line=line_number)
        spans[symbol].append((start, end))
    normalized: dict[str, tuple[tuple[date, date], ...]] = {}
    for symbol, values in sorted(spans.items()):
        ordered = sorted(values)
        for previous, current in zip(ordered, ordered[1:]):
            if current[0] <= previous[1]:
                raise _fail(REASON_AUTHORITY_AMBIGUOUS, f"Qlib instrument spans overlap for {symbol}")
        normalized[symbol] = tuple(ordered)
    if not normalized:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "Qlib instrument span set is empty")
    return normalized


def _load_qlib_calendar(path: Path) -> tuple[date, ...]:
    try:
        values = tuple(_as_date(item, "qlib.calendar") for item in path.read_text(encoding="utf-8").splitlines())
    except (OSError, UnicodeDecodeError) as exc:
        raise _fail(REASON_SOURCE_COMPONENT_MISSING, "Qlib day calendar cannot be read") from exc
    if not values or values != tuple(sorted(set(values))):
        raise _fail(REASON_DUPLICATE_KEY, "Qlib day calendar is not sorted and unique")
    if SOURCE_START not in values or SOURCE_END not in values:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "Qlib calendar omits the approved source boundary")
    return values


def _qlib_code_directory(symbol: str) -> str:
    return symbol.lower()


def _read_qlib_stock_rows(
    qlib_root: Path,
    *,
    symbol: str,
    calendar: Sequence[date],
    active_spans: Sequence[tuple[date, date]],
) -> np.ndarray:
    feature_root = qlib_root / "features" / _qlib_code_directory(symbol)
    arrays: dict[str, tuple[int, np.ndarray]] = {}
    for field in QLIB_STOCK_FIELDS:
        try:
            start, raw = read_qlib_bin(feature_root / f"{field}.day.bin")
        except (FileNotFoundError, ValueError, OSError) as exc:
            raise _fail(
                REASON_SOURCE_COMPONENT_MISSING, f"Qlib stock feature is unavailable: {symbol}/{field}"
            ) from exc
        if raw.dtype != np.dtype("<f4") or len(raw) < 2:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"Qlib stock feature dtype/shape differs: {symbol}/{field}")
        arrays[field] = (start, raw[1:])
    starts = {value[0] for value in arrays.values()}
    lengths = {len(value[1]) for value in arrays.values()}
    if len(starts) != 1 or len(lengths) != 1:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"Qlib stock fields are not row aligned: {symbol}")
    start_index = next(iter(starts))
    length = next(iter(lengths))
    if start_index < 0 or start_index + length > len(calendar):
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, f"Qlib stock feature range escapes calendar: {symbol}")
    expected_positions = [
        index
        for index, day in enumerate(calendar)
        if SOURCE_START <= day <= SOURCE_END
        and any(span_start <= day <= span_end for span_start, span_end in active_spans)
    ]
    positions = [index for index in expected_positions if start_index <= index < start_index + length]
    if positions != expected_positions:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, f"Qlib stock feature omits active span dates: {symbol}")
    calendar_positions = np.asarray(positions, dtype=np.int64)
    local_positions = calendar_positions - start_index
    result = np.zeros(len(calendar_positions), dtype=_QLIB_SOURCE_DTYPE)
    result["trade_date"] = np.fromiter(
        (
            calendar[index].year * 10_000 + calendar[index].month * 100 + calendar[index].day
            for index in calendar_positions
        ),
        dtype="<i4",
        count=len(calendar_positions),
    )
    result["symbol"] = _ascii(symbol, "qlib.symbol", width=16)
    for field in QLIB_STOCK_FIELDS:
        result[field] = arrays[field][1][local_positions]
    return result


def _preflight_qlib_feature_inventory(
    qlib_root: Path,
    *,
    calendar: Sequence[date],
    spans: Mapping[str, Sequence[tuple[date, date]]],
) -> dict[str, Any]:
    failures: list[dict[str, str]] = []
    file_count = 0
    value_count = 0
    for symbol, active_spans in sorted(spans.items()):
        expected_positions = [
            index
            for index, day in enumerate(calendar)
            if SOURCE_START <= day <= SOURCE_END
            and any(span_start <= day <= span_end for span_start, span_end in active_spans)
        ]
        aligned: set[tuple[int, int]] = set()
        for field in QLIB_STOCK_FIELDS:
            path = qlib_root / "features" / _qlib_code_directory(symbol) / f"{field}.day.bin"
            if not path.is_file():
                failures.append({"symbol": symbol, "field": field, "reason": "missing_file"})
                continue
            size = path.stat().st_size
            if size < 8 or size % 4:
                failures.append({"symbol": symbol, "field": field, "reason": "invalid_size"})
                continue
            start_values = np.fromfile(path, dtype="<f4", count=1)
            if len(start_values) != 1 or not math.isfinite(float(start_values[0])) or float(start_values[0]) % 1:
                failures.append({"symbol": symbol, "field": field, "reason": "invalid_start_index"})
                continue
            start_index = int(start_values[0])
            length = size // 4 - 1
            aligned.add((start_index, length))
            file_count += 1
            value_count += length
        if len(aligned) > 1:
            failures.append({"symbol": symbol, "field": "*", "reason": "field_alignment_drift"})
        elif aligned and expected_positions:
            start_index, length = next(iter(aligned))
            if expected_positions[0] < start_index or expected_positions[-1] >= start_index + length:
                failures.append({"symbol": symbol, "field": "*", "reason": "active_span_range_missing"})
    if failures:
        failures.sort(key=lambda item: (item["symbol"], item["field"], item["reason"]))
        raise _fail(
            REASON_SOURCE_RANGE_INCOMPLETE,
            "Qlib required field preflight failed",
            failure_count=len(failures),
            failure_sha256=canonical_sha256(failures),
            failures=failures[:100],
            failures_truncated=len(failures) > 100,
        )
    expected_files = len(spans) * len(QLIB_STOCK_FIELDS)
    if file_count != expected_files:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "Qlib feature file count differs")
    return {
        "field_file_count": file_count,
        "field_value_count": value_count,
        "stock_count": len(spans),
        "field_count": len(QLIB_STOCK_FIELDS),
        "preflight_sha256": canonical_sha256(
            {
                "field_file_count": file_count,
                "field_value_count": value_count,
                "stock_count": len(spans),
                "field_count": len(QLIB_STOCK_FIELDS),
            }
        ),
    }


def _raw_qlib_values(raw: np.void) -> dict[str, float]:
    values = {field: float(raw[field]) for field in QLIB_STOCK_FIELDS}
    factor = values["factor"]
    if not math.isfinite(factor) or factor <= 0:
        raise _fail(REASON_SOURCE_UNIT_INVALID, "Qlib adjustment factor must be finite and positive")
    for field in ("open", "high", "low", "close"):
        values[field] = values[field] / factor
    values["volume"] = values["volume"] * factor
    if any(not math.isfinite(values[field]) for field in QLIB_STOCK_FIELDS):
        raise _fail(REASON_NON_FINITE, "Qlib required stock values contain non-finite data")
    if any(
        values[field] <= 0
        for field in ("open", "high", "low", "close", "prev_close", "up_limit_price", "down_limit_price")
    ):
        raise _fail(REASON_SOURCE_UNIT_INVALID, "Qlib reconstructed raw prices must be positive")
    if values["volume"] < 0 or values["amount"] < 0:
        raise _fail(REASON_SOURCE_UNIT_INVALID, "Qlib volume/amount must be non-negative")
    if values["limit_up"] not in (0.0, 1.0) or values["limit_down"] not in (0.0, 1.0):
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "Qlib limit flags must be exact 0/1")
    return values


def _qlib_row_is_fully_missing(raw: np.void) -> bool:
    values = np.asarray([float(raw[field]) for field in QLIB_STOCK_FIELDS], dtype=np.float64)
    if bool(np.isnan(values).all()):
        return True
    finite = np.isfinite(values)
    if not bool(finite.all()):
        raise _fail(REASON_SOURCE_UNIT_INVALID, "Qlib stock row is only partially finite")
    return False


def load_rotation_l1_source_assets(manifest_path: Path) -> dict[str, Any]:
    """Read and verify the explicit dataset-release asset binding.

    ``release_root`` is a locator and is intentionally excluded from the
    canonical body.  Every consumed file or tree is instead bound by its
    release-relative name and SHA/Merkle identity.
    """

    manifest = _read_json_object(Path(manifest_path))
    required = {
        "schema_version",
        "release_root",
        "release_identity",
        "daily_bin",
        "files",
        "source_end",
        "manifest_body_sha256",
    }
    if set(manifest) != required or manifest.get("schema_version") != SOURCE_ASSET_SCHEMA_VERSION:
        raise _fail(REASON_MANIFEST_INVALID, "dataset release asset binding schema differs")
    body = {key: value for key, value in manifest.items() if key not in {"release_root", "manifest_body_sha256"}}
    if canonical_sha256(body) != _require_sha256(manifest.get("manifest_body_sha256"), "manifest_body_sha256"):
        raise _fail(REASON_HASH_MISMATCH, "dataset release asset binding body hash differs")
    try:
        release_binding = QEFormalDatasetBinding.from_mapping(manifest.get("release_identity"))
    except (TypeError, ValueError) as exc:
        raise _fail(REASON_MANIFEST_INVALID, "dataset release identity is incomplete") from exc
    if release_binding.usage_mode != FormalDatasetUsage.TRAINING.value:
        raise _fail(REASON_MANIFEST_INVALID, "dataset release identity is not formal-training authority")
    release_identity = release_binding.as_dict()
    if _as_date(manifest.get("source_end"), "source_end") != SOURCE_END:
        raise _fail(REASON_HOLDOUT_CONTAMINATION, "dataset release source_end differs from approved bundle boundary")
    root = Path(str(manifest.get("release_root") or ""))
    if not root.is_absolute():
        raise _fail(REASON_MANIFEST_INVALID, "dataset release root must be an absolute locator")
    try:
        root = root.resolve(strict=True)
    except OSError as exc:
        raise _fail(REASON_SOURCE_COMPONENT_MISSING, "dataset release root does not exist") from exc
    daily = manifest.get("daily_bin")
    if not isinstance(daily, Mapping) or set(daily) != {
        "relative_root",
        "tree_merkle_sha256",
        "schema_version",
        "schema_sha256",
    }:
        raise _fail(REASON_MANIFEST_INVALID, "daily Bin binding schema differs")
    relative_root = Path(str(daily.get("relative_root") or ""))
    if relative_root.is_absolute() or ".." in relative_root.parts:
        raise _fail(REASON_MANIFEST_INVALID, "daily Bin root escapes release root")
    qlib_root = (root / relative_root).resolve()
    try:
        qlib_root.relative_to(root)
    except ValueError as exc:
        raise _fail(REASON_MANIFEST_INVALID, "daily Bin root escapes release root") from exc
    if not qlib_root.is_dir():
        raise _fail(REASON_SOURCE_COMPONENT_MISSING, "daily Bin root is missing")
    if (
        daily.get("schema_version") != QLIB_STOCK_SCHEMA_VERSION
        or _require_sha256(daily.get("schema_sha256"), "daily_bin.schema_sha256") != qlib_stock_schema_digest()
    ):
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "daily Bin stock schema differs")
    try:
        _files, observed_merkle = tree_merkle(qlib_root)
    except CopyOnWriteError as exc:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "daily Bin tree cannot be verified") from exc
    if observed_merkle != _require_sha256(daily.get("tree_merkle_sha256"), "daily_bin.tree_merkle_sha256"):
        raise _fail(REASON_HASH_MISMATCH, "daily Bin tree Merkle differs")
    files = manifest.get("files")
    expected_files = {
        "daily_basic",
        "moneyflow",
        "index_context",
        "suspend_data",
        "suspend_manifest",
        "security_identity",
        "provider_absence",
    }
    if not isinstance(files, Mapping) or set(files) != expected_files:
        raise _fail(REASON_MANIFEST_INVALID, "dataset release file binding set differs")
    resolved = {name: _verify_bound_file(root, files[name], name) for name in sorted(expected_files)}
    calendar = _load_qlib_calendar(qlib_root / "calendars" / "day.txt")
    spans = _parse_instrument_spans(qlib_root / "instruments" / "all.txt")
    qlib_preflight = _preflight_qlib_feature_inventory(qlib_root, calendar=calendar, spans=spans)
    daily_basic_inventory = _fixed_h5_inventory(
        resolved["daily_basic"], expected_columns=_DAILY_BASIC_COLUMNS, expected_dtype="<f4"
    )
    moneyflow_inventory = _fixed_h5_inventory(
        resolved["moneyflow"], expected_columns=_MONEYFLOW_COLUMNS, expected_dtype="<f4"
    )
    index_inventory = _fixed_h5_inventory(
        resolved["index_context"],
        expected_columns=(
            "idx_open_point",
            "idx_high_point",
            "idx_low_point",
            "idx_close_point",
            "idx_pre_close_point",
            "idx_return_1d",
            "idx_volume_hand_source",
            "idx_volume_share_equiv",
            "idx_amount_cny",
        ),
        expected_dtype="<f8",
    )
    for component, inventory in (
        ("daily_basic", daily_basic_inventory),
        ("moneyflow", moneyflow_inventory),
        ("index_context", index_inventory),
    ):
        if (
            _as_date(inventory["date_min"], f"{component}.date_min") > SOURCE_START
            or _as_date(inventory["date_max"], f"{component}.date_max") < SOURCE_END
        ):
            raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, f"{component} does not cover the approved source range")
    qlib_unit = {
        "open": "qfq_adjusted_cny",
        "high": "qfq_adjusted_cny",
        "low": "qfq_adjusted_cny",
        "close": "qfq_adjusted_cny",
        "volume": "qfq_adjusted_shares",
        "amount": "cny_unadjusted",
        "factor": "qfq_ratio",
        "up_limit_price": "raw_cny_unadjusted",
        "down_limit_price": "raw_cny_unadjusted",
        "prev_close": "raw_cny_unadjusted",
        "limit_up": "boolean_float32_0_1",
        "limit_down": "boolean_float32_0_1",
    }
    required_fields = [
        {
            "field": field,
            "component_id": "daily_bin",
            "component_hash": observed_merkle,
            "dtype": "float32",
            "unit": qlib_unit[field],
            "date_min": calendar[0].isoformat(),
            "date_max": calendar[-1].isoformat(),
            "code_count": len(spans),
        }
        for field in QLIB_STOCK_FIELDS
    ]
    required_fields.extend(
        {
            "field": field,
            "component_id": component,
            "component_hash": _sha256_file(resolved[component]),
            "dtype": inventory["dtype"],
            "unit": unit,
            "date_min": inventory["date_min"],
            "date_max": inventory["date_max"],
            "code_count": inventory["code_count"],
        }
        for component, inventory, fields_with_units in (
            (
                "daily_basic",
                daily_basic_inventory,
                (("db_total_mv", "ten_thousand_cny"), ("db_circ_mv", "ten_thousand_cny")),
            ),
            (
                "moneyflow",
                moneyflow_inventory,
                (
                    ("mf_sm_buy_amt", "cny"),
                    ("mf_sm_sell_amt", "cny"),
                    ("mf_elg_buy_amt", "cny"),
                    ("mf_elg_sell_amt", "cny"),
                    ("mf_net_amt", "cny"),
                ),
            ),
            (
                "index_context",
                index_inventory,
                (
                    ("idx_close_point", "index_point"),
                    ("idx_pre_close_point", "index_point"),
                    ("idx_return_1d", "decimal_return"),
                ),
            ),
        )
        for field, unit in fields_with_units
    )
    inventory_body = {
        "schema_version": SOURCE_INVENTORY_SCHEMA_VERSION,
        "release_identity": dict(release_identity),
        "source_end": SOURCE_END.isoformat(),
        "qlib": {
            "schema_version": QLIB_STOCK_SCHEMA_VERSION,
            "schema_sha256": qlib_stock_schema_digest(),
            "tree_merkle_sha256": observed_merkle,
            "calendar_sha256": _sha256_file(qlib_root / "calendars" / "day.txt"),
            "instruments_sha256": _sha256_file(qlib_root / "instruments" / "all.txt"),
            "field_preflight": qlib_preflight,
        },
        "files": {name: {"sha256": _sha256_file(path)} for name, path in sorted(resolved.items())},
        "required_fields": sorted(required_fields, key=lambda item: (item["component_id"], item["field"])),
        "qlib_value_contract": dict(QLIB_STOCK_VALUE_CONTRACT),
        "unit_contracts": {
            "moneyflow": "tushare_moneyflow_shares_yuan_v1",
            "causal_circ_mv": "hmm_risk_causal_circ_mv_source_window_v1",
            "daily_basic_market_value": "tushare_ten_thousand_cny_to_cny_v1",
            "qlib_raw_reconstruction": "qe_qlib_stock_qfq_inverse_v1",
        },
    }
    return {
        "release_root": root,
        "qlib_root": qlib_root,
        "files": resolved,
        "release_identity": dict(release_identity),
        "formal_dataset_binding": release_binding,
        "inventory": {**inventory_body, "inventory_sha256": canonical_sha256(inventory_body)},
        "binding_manifest_sha256": canonical_sha256(body),
    }


def _industry_adapter(authority: Mapping[str, Any], *, forbidden_roots: Sequence[Path]) -> HMMIndustryPitAdapter:
    if not isinstance(authority, Mapping) or set(authority) != {
        "artifact_root",
        "identity",
        "research_basis",
        "l1_projection",
        "l2_projection",
    }:
        raise _fail(REASON_MANIFEST_INVALID, "C-013 industry authority schema differs")
    root = Path(str(authority.get("artifact_root") or ""))
    if not root.is_absolute():
        raise _fail(REASON_MANIFEST_INVALID, "C-013 artifact root must be absolute")
    try:
        adapter = HMMIndustryPitAdapter.from_artifact_root(
            artifact_root=root,
            forbidden_roots=forbidden_roots,
            expected_identity=authority["identity"],
        )
        adapter.bind_research_basis_contract(authority["research_basis"])
        adapter.bind_l1_code_projection(authority["l1_projection"])
        adapter.bind_l2_code_projection(authority["l2_projection"])
    except Exception as exc:
        raise _fail(REASON_AUTHORITY_AMBIGUOUS, "C-013 authority cannot be bound") from exc
    return adapter


def _load_suspend_keys(
    data_path: Path, manifest_path: Path, *, calendar: Sequence[date]
) -> frozenset[tuple[date, str]]:
    manifest = _read_json_object(manifest_path, reason=REASON_SOURCE_SCHEMA_INVALID)
    if (
        manifest.get("schema_version") != "suspend_d_dataset_manifest_v1"
        or (manifest.get("source") or {}).get("contract") != "tushare_suspend_d_shsz_S_v1"
        or _as_date(manifest.get("start"), "suspend.start") > SOURCE_START
        or _as_date(manifest.get("end"), "suspend.end") < SOURCE_END
        or (manifest.get("artifacts") or {}).get("suspend_d.parquet", {}).get("sha256") != _sha256_file(data_path)
    ):
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "suspend_d authority differs from the approved contract")
    try:
        frame = pd.read_parquet(data_path, columns=["ts_code", "trade_date", "suspend_type", "suspend_timing"])
    except Exception as exc:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "suspend_d parquet cannot be read") from exc
    if frame.empty:
        return frozenset()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="raise").dt.date
    frame["ts_code"] = frame["ts_code"].astype(str).str.upper()
    if (frame["suspend_type"] != "S").any():
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "suspend_d includes rows outside suspend_type S")
    intraday = frame["suspend_timing"].notna()
    if frame.loc[intraday, "suspend_timing"].map(lambda value: not isinstance(value, str) or not value.strip()).any():
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "suspend_d intraday timing is invalid")
    full_day = frame.loc[~intraday]
    calendar_set = set(calendar)
    keys = [(row.trade_date, row.ts_code) for row in full_day.itertuples(index=False) if row.trade_date in calendar_set]
    if len(keys) != len(set(keys)):
        raise _fail(REASON_DUPLICATE_KEY, "suspend_d contains duplicate stock/date rows")
    return frozenset(keys)


def _load_benchmark_returns(path: Path, *, calendar: Sequence[date]) -> dict[date, float]:
    frame = _load_fixed_h5_window(
        path,
        expected_columns=(
            "idx_open_point",
            "idx_high_point",
            "idx_low_point",
            "idx_close_point",
            "idx_pre_close_point",
            "idx_return_1d",
            "idx_volume_hand_source",
            "idx_volume_share_equiv",
            "idx_amount_cny",
        ),
        expected_dtype="<f8",
        start=SOURCE_START,
        end=SOURCE_END,
    )
    try:
        benchmark = frame.xs("000300.SH", level="instrument")
    except KeyError as exc:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "index context omits CSI300 benchmark") from exc
    close = pd.to_numeric(benchmark["idx_close_point"], errors="coerce")
    previous = pd.to_numeric(benchmark["idx_pre_close_point"], errors="coerce")
    provided = pd.to_numeric(benchmark["idx_return_1d"], errors="coerce")
    recomputed = close / previous - 1.0
    if (
        not np.isfinite(close.to_numpy(dtype=np.float64)).all()
        or not np.isfinite(previous.to_numpy(dtype=np.float64)).all()
        or not np.isfinite(provided.to_numpy(dtype=np.float64)).all()
        or bool((previous <= 0).any())
    ):
        raise _fail(REASON_SOURCE_UNIT_INVALID, "CSI300 benchmark price/return values are invalid")
    output = {timestamp.date(): float(value) for timestamp, value in recomputed.items()}
    expected = set(calendar)
    if set(output) != expected or any(not math.isfinite(value) for value in output.values()):
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "CSI300 benchmark calendar/value coverage differs")
    return output


class _SecurityResolutionIndex:
    def __init__(self, manifest: Any) -> None:
        self._manifest = manifest
        self.rows = manifest.rows
        grouped: dict[tuple[str, str], list[Any]] = defaultdict(list)
        for row in self.rows:
            grouped[(row.source_dataset, row.canonical_ts_code)].append(row)
        self._rows_by_key: dict[tuple[str, str], tuple[Any, ...]] = {}
        for key, rows in grouped.items():
            ordered = tuple(sorted(rows, key=lambda row: (row.effective_start, row.effective_end)))
            for previous, current in zip(ordered, ordered[1:]):
                if current.effective_start <= previous.effective_end:
                    raise _fail(REASON_AUTHORITY_AMBIGUOUS, f"security source intervals overlap: {key}")
            self._rows_by_key[key] = ordered
        self._default_cache: dict[tuple[str, str], Any] = {}

    def resolve(self, canonical_ts_code: str, trade_date: date, source_dataset: str) -> Any:
        key = (source_dataset, canonical_ts_code)
        for row in self._rows_by_key.get(key, ()):
            if row.effective_start <= trade_date <= row.effective_end:
                return row
        cached = self._default_cache.get(key)
        if cached is None:
            cached = self._manifest.resolve(canonical_ts_code, trade_date, source_dataset)
            self._default_cache[key] = cached
        return cached

    def evidence(self) -> dict[str, Any]:
        return self._manifest.evidence()


def _industry_projection_identity(value: Any) -> tuple[Any, ...]:
    return (
        value.status,
        value.l1_code,
        value.l1_name,
        value.l2_code,
        value.l2_name,
        value.reason_code,
        value.classification_receipt_hash,
        value.index_membership_receipt_hash,
        tuple(value.classification_row_hashes),
        tuple(value.index_membership_row_hashes),
        value.alignment_state,
        value.classification_research_basis,
        value.non_as_known_taxonomy,
    )


class _IndustryProjectionIndex:
    def __init__(self, adapter: HMMIndustryPitAdapter, *, calendar: Sequence[date]) -> None:
        self._adapter = adapter
        if not calendar or tuple(calendar) != tuple(sorted(set(calendar))):
            raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "industry projection calendar is not sorted and unique")
        self._start = calendar[0]
        self._end = calendar[-1]
        self._intervals: dict[str, tuple[tuple[date, date, Any], ...]] = {}
        self._starts: dict[str, tuple[date, ...]] = {}

    def _build(self, symbol: str) -> None:
        boundaries = {self._start, self._end + timedelta(days=1)}
        for resolver in (self._adapter.classification_resolver, self._adapter.index_membership_resolver):
            boundaries.update(day for day in resolver.transition_dates(symbol) if self._start < day <= self._end)
        ordered = sorted(boundaries)
        intervals: list[tuple[date, date, Any]] = []
        identities: list[tuple[Any, ...]] = []
        for start, after_end in zip(ordered, ordered[1:]):
            projection = self._adapter.resolve(symbol, start)
            identity = _industry_projection_identity(projection)
            end = after_end - timedelta(days=1)
            if intervals and identities[-1] == identity and intervals[-1][1] + timedelta(days=1) == start:
                previous_start, _previous_end, previous = intervals[-1]
                intervals[-1] = (previous_start, end, previous)
            else:
                intervals.append((start, end, projection))
                identities.append(identity)
        self._intervals[symbol] = tuple(intervals)
        self._starts[symbol] = tuple(start for start, _end, _projection in intervals)

    def resolve(self, symbol: str, trade_date: date) -> Any:
        if trade_date < self._start or trade_date > self._end:
            raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "industry projection date escapes source calendar")
        if symbol not in self._intervals:
            self._build(symbol)
        starts = self._starts[symbol]
        position = bisect_right(starts, trade_date) - 1
        if position < 0:
            raise _fail(REASON_AUTHORITY_AMBIGUOUS, "industry projection interval is unavailable")
        start, end, projection = self._intervals[symbol][position]
        if not start <= trade_date <= end:
            raise _fail(REASON_AUTHORITY_AMBIGUOUS, "industry projection interval does not cover date")
        return projection


def _spool_qlib_months(
    qlib_root: Path,
    *,
    calendar: Sequence[date],
    spans: Mapping[str, Sequence[tuple[date, date]]],
    spool_root: Path,
    resource_started: float | None = None,
) -> tuple[Path, ...]:
    spool_root.mkdir(parents=True, exist_ok=False)
    handles: dict[str, Any] = {}
    paths: dict[str, Path] = {}
    try:
        for symbol_index, (symbol, active_spans) in enumerate(sorted(spans.items())):
            if resource_started is not None and symbol_index % 64 == 0:
                _resource_checkpoint(
                    resource_started,
                    stage="qlib_month_spool",
                    max_seconds=BUILD_MAX_SECONDS,
                    max_rss_bytes=BUILD_MAX_RSS_BYTES,
                )
            rows = _read_qlib_stock_rows(
                qlib_root,
                symbol=symbol,
                calendar=calendar,
                active_spans=active_spans,
            )
            if not len(rows):
                continue
            month_codes = rows["trade_date"] // 100
            boundaries = np.flatnonzero(month_codes[1:] != month_codes[:-1]) + 1
            for month_rows in np.split(rows, boundaries):
                month = str(int(month_rows["trade_date"][0]) // 100)
                path = spool_root / f"{month}.bin"
                handle = handles.get(month)
                if handle is None:
                    handle = path.open("xb")
                    handles[month] = handle
                    paths[month] = path
                month_rows.tofile(handle)
    finally:
        for handle in handles.values():
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
    if not paths:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "Qlib source produced no approved stock rows")
    return tuple(path for _, path in sorted(paths.items()))


def _read_spooled_month(path: Path) -> np.ndarray:
    size = path.stat().st_size
    if size <= 0 or size % _QLIB_SOURCE_DTYPE.itemsize:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "Qlib month spool size differs")
    rows = np.fromfile(path, dtype=_QLIB_SOURCE_DTYPE)
    order = np.lexsort((rows["symbol"], rows["trade_date"]))
    rows = rows[order]
    identities = [(int(row["trade_date"]), bytes(row["symbol"])) for row in rows]
    if len(identities) != len(set(identities)):
        raise _fail(REASON_DUPLICATE_KEY, "Qlib month spool contains duplicate stock/date rows")
    return rows


def _month_bounds(path: Path) -> tuple[date, date]:
    month = path.stem
    if len(month) != 6 or not month.isdigit():
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "Qlib month spool name differs")
    start = date(int(month[:4]), int(month[4:]), 1)
    next_month = (pd.Timestamp(start) + pd.DateOffset(months=1)).date()
    return max(start, SOURCE_START), min(next_month - timedelta(days=1), SOURCE_END)


def _h5_lookup(frame: pd.DataFrame) -> dict[tuple[date, str], tuple[float, ...]]:
    return {
        (pd.Timestamp(timestamp).date(), str(symbol)): tuple(float(value) for value in row)
        for (timestamp, symbol), row in zip(frame.index, frame.to_numpy(dtype=np.float32), strict=True)
    }


def _advance_interval(
    active: dict[str, tuple[date, date, tuple[str, ...]]],
    completed: list[tuple[str, date, date, tuple[str, ...]]],
    *,
    symbol: str,
    day: date,
    payload: tuple[str, ...],
    previous_trading_day: date | None,
) -> None:
    current = active.get(symbol)
    if current is not None and current[2] == payload and previous_trading_day == current[1]:
        active[symbol] = (current[0], day, payload)
        return
    if current is not None:
        completed.append((symbol, current[0], current[1], current[2]))
    active[symbol] = (day, day, payload)


def _finish_intervals(
    active: Mapping[str, tuple[date, date, tuple[str, ...]]],
    completed: list[tuple[str, date, date, tuple[str, ...]]],
) -> list[tuple[str, date, date, tuple[str, ...]]]:
    output = list(completed)
    output.extend((symbol, value[0], value[1], value[2]) for symbol, value in active.items())
    return sorted(output, key=lambda item: (item[0], item[1], item[2], item[3]))


def _append_feature_domain_aggregate(
    rows: Sequence[Mapping[str, Any]],
    *,
    level: str,
    aggregates: list[Any],
    unavailable: dict[tuple[date, str, str], str],
    contributor_eligibility: Mapping[str, bool],
) -> None:
    if not rows:
        return
    try:
        aggregate = aggregate_l1_day(
            rows,
            min_coverage=MIN_COVERAGE,
            moneyflow_contributor_eligibility=contributor_eligibility,
        )
    except ObservationCoverageError as exc:
        unavailable[(exc.trade_date, level, exc.l1_code)] = exc.reason_code
        return
    except Exception as exc:
        first = rows[0]
        unavailable[(first["trade_date"], level, str(first["l1_code"]))] = (
            "hmm_risk_rotation_l1_stock_fact_aggregate_unavailable"
        )
        if "no observed denominator" not in str(exc):
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{level} stock fact aggregation failed") from exc
        return
    aggregates.append(aggregate)


def _append_day_level_aggregates(
    day_rows: Sequence[dict[str, Any]],
    *,
    l1_aggregates: list[Any],
    l2_aggregates: list[Any],
    unavailable: dict[tuple[date, str, str], str],
    contributor_eligibility: Mapping[str, bool],
) -> None:
    l1_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    l2_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in day_rows:
        l1_groups[str(row["l1_code"])].append(row)
        l2_groups[str(row["l2_code"])].append(row)
    for code in sorted(l1_groups):
        _append_feature_domain_aggregate(
            l1_groups[code],
            level="L1",
            aggregates=l1_aggregates,
            unavailable=unavailable,
            contributor_eligibility=contributor_eligibility,
        )
    for code in sorted(l2_groups):
        rows = l2_groups[code]
        for row in rows:
            row["l1_code"] = row["l2_code"]
            row["l1_name"] = row["l2_name"]
        _append_feature_domain_aggregate(
            rows,
            level="L2",
            aggregates=l2_aggregates,
            unavailable=unavailable,
            contributor_eligibility=contributor_eligibility,
        )


def _build_train_only_contributor_eligibility(
    *,
    spans: Mapping[str, Sequence[tuple[date, date]]],
    calendar: Sequence[date],
    adapter: HMMIndustryPitAdapter,
    security: Any,
    provider_absence: Any,
    suspension_keys: frozenset[tuple[date, str]],
) -> tuple[dict[str, bool], dict[str, Any]]:
    train_dates = tuple(day for day in calendar if C010_APPROVED_TRAIN_START <= day <= C010_APPROVED_TRAIN_END)
    if not train_dates:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "C-010 train-only eligibility calendar is empty")
    for row in provider_absence.rows:
        if C010_APPROVED_TRAIN_START <= row.trade_date <= C010_APPROVED_TRAIN_END:
            resolution = security.resolve(row.canonical_ts_code, row.trade_date, "market.moneyflow_ts")
            if resolution.source_ts_code != row.source_ts_code:
                raise _fail(REASON_AUTHORITY_AMBIGUOUS, "provider absence source identity drifted")
    provider_keys = {(row.canonical_ts_code, row.source_ts_code, row.trade_date) for row in provider_absence.rows}
    eligibility: dict[str, bool] = {}
    entries: list[dict[str, Any]] = []
    for symbol, active_spans in sorted(spans.items()):
        expected_dates: list[date] = []
        absent_dates: list[date] = []
        for day in train_dates:
            if not any(start <= day <= end for start, end in active_spans) or (day, symbol) in suspension_keys:
                continue
            projection = adapter.resolve(symbol, day)
            if projection.status != "resolved":
                continue
            resolution = security.resolve(symbol, day, "market.moneyflow_ts")
            expected_dates.append(day)
            if (symbol, resolution.source_ts_code, day) in provider_keys:
                absent_dates.append(day)
        expected_count = len(expected_dates)
        available_count = expected_count - len(absent_dates)
        accepted = expected_count > 0 and 10 * available_count >= 9 * expected_count
        eligibility[symbol] = accepted
        entry = {
            "canonical_ts_code": symbol,
            "expected_opportunity_count": expected_count,
            "expected_opportunity_date_sha256": canonical_sha256([day.isoformat() for day in expected_dates]),
            "provider_absence_count": len(absent_dates),
            "provider_absence_date_sha256": canonical_sha256([day.isoformat() for day in absent_dates]),
            "availability_integer_contract": "10*(expected-missing) >= 9*expected",
            "moneyflow_contributor_eligible": accepted,
        }
        entries.append({**entry, "entry_sha256": canonical_sha256(entry)})
    body = {
        "schema_version": "hmm_risk_rotation_l1_bundle_train_eligibility_v1",
        "policy_version": "hmm_risk_c010_feature_domain_policy_v2",
        "train_start": C010_APPROVED_TRAIN_START.isoformat(),
        "train_end": C010_APPROVED_TRAIN_END.isoformat(),
        "minimum_availability_ratio": MIN_COVERAGE,
        "entry_count": len(entries),
        "eligible_count": sum(eligibility.values()),
        "entries_sha256": canonical_sha256(entries),
        "excluded_symbols": sorted(symbol for symbol, accepted in eligibility.items() if not accepted),
    }
    return eligibility, _receipt_from_body(body)


def _build_stock_fact_aggregates(
    *,
    month_paths: Sequence[Path],
    assets: Mapping[str, Any],
    calendar: Sequence[date],
    spans: Mapping[str, Sequence[tuple[date, date]]],
    adapter: HMMIndustryPitAdapter,
    security: Any,
    provider_absence: Any,
    suspension_keys: frozenset[tuple[date, str]],
    contributor_eligibility: Mapping[str, bool],
    resource_started: float | None = None,
) -> tuple[list[Any], list[Any], dict[tuple[date, str, str], str], dict[str, list[dict[str, Any]]]]:
    history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=10))
    active_span_start: dict[str, date] = {}
    circ_state: dict[str, tuple[date, float]] = {}
    l1_aggregates: list[Any] = []
    l2_aggregates: list[Any] = []
    unavailable: dict[tuple[date, str, str], str] = {}
    industry_active: dict[str, tuple[date, date, tuple[str, ...]]] = {}
    industry_done: list[tuple[str, date, date, tuple[str, ...]]] = []
    status_active: dict[str, tuple[date, date, tuple[str, ...]]] = {}
    status_done: list[tuple[str, date, date, tuple[str, ...]]] = []
    calendar_position = {day: index for index, day in enumerate(calendar)}

    for month_path in month_paths:
        if resource_started is not None:
            _resource_checkpoint(
                resource_started,
                stage="stock_fact_aggregation",
                max_seconds=BUILD_MAX_SECONDS,
                max_rss_bytes=BUILD_MAX_RSS_BYTES,
            )
        month_start, month_end = _month_bounds(month_path)
        basic = _h5_lookup(
            _load_fixed_h5_window(
                assets["files"]["daily_basic"],
                expected_columns=_DAILY_BASIC_COLUMNS,
                expected_dtype="<f4",
                start=month_start,
                end=month_end,
                labels_prevalidated=True,
            )
        )
        moneyflow = _h5_lookup(
            _load_fixed_h5_window(
                assets["files"]["moneyflow"],
                expected_columns=_MONEYFLOW_COLUMNS,
                expected_dtype="<f4",
                start=month_start,
                end=month_end,
                labels_prevalidated=True,
            )
        )
        source_rows = _read_spooled_month(month_path)
        for raw_day, raw_group in itertools.groupby(source_rows, key=lambda row: int(row["trade_date"])):
            day = _date_from_yyyymmdd(raw_day, "qlib.trade_date")
            previous_day = calendar[calendar_position[day] - 1] if calendar_position[day] > 0 else None
            day_rows: list[dict[str, Any]] = []
            current_basic_updates: list[tuple[str, float]] = []
            for raw in raw_group:
                symbol = bytes(raw["symbol"]).rstrip(b"\x00").decode("ascii")
                matching_spans = [value for value in spans[symbol] if value[0] <= day <= value[1]]
                if len(matching_spans) != 1:
                    raise _fail(REASON_AUTHORITY_AMBIGUOUS, f"instrument span resolution differs: {symbol}/{day}")
                eligible_start = matching_spans[0][0]
                if active_span_start.get(symbol) != eligible_start:
                    history[symbol].clear()
                    active_span_start[symbol] = eligible_start
                suspended = (day, symbol) in suspension_keys
                daily_resolution = security.resolve(symbol, day, "market.daily_basic")
                flow_resolution = security.resolve(symbol, day, "market.moneyflow_ts")
                basic_row = basic.get((day, daily_resolution.source_ts_code))
                flow_row = moneyflow.get((day, flow_resolution.source_ts_code))
                qlib_missing = _qlib_row_is_fully_missing(raw)
                if basic_row is not None and math.isfinite(float(basic_row[15])) and float(basic_row[15]) > 0:
                    current_basic_updates.append((symbol, float(basic_row[15]) * 10_000.0))
                projection = adapter.resolve(symbol, day)
                if projection.status != "resolved":
                    if not suspended and not qlib_missing:
                        history[symbol].append(_raw_qlib_values(raw)["close"])
                    _advance_interval(
                        status_active,
                        status_done,
                        symbol=symbol,
                        day=day,
                        payload=("industry_unavailable", str(projection.reason_code or ""), "c013"),
                        previous_trading_day=previous_day,
                    )
                    continue
                assert projection.l1_code and projection.l1_name and projection.l2_code and projection.l2_name
                _advance_interval(
                    industry_active,
                    industry_done,
                    symbol=symbol,
                    day=day,
                    payload=(projection.l1_code, projection.l2_code),
                    previous_trading_day=previous_day,
                )
                if suspended:
                    _advance_interval(
                        status_active,
                        status_done,
                        symbol=symbol,
                        day=day,
                        payload=("suspended", "hmm_risk_rotation_l1_suspended", "suspend_d"),
                        previous_trading_day=previous_day,
                    )
                    day_rows.append(
                        {
                            "trade_date": day,
                            "symbol": symbol,
                            "l1_code": projection.l1_code,
                            "l1_name": projection.l1_name,
                            "l2_code": projection.l2_code,
                            "l2_name": projection.l2_name,
                            "is_suspended": True,
                            "moneyflow_fact_status": "not_applicable_suspended",
                        }
                    )
                    continue
                prior = circ_state.get(symbol)
                prior_circ = prior[1] if prior is not None and prior[0] >= eligible_start else None
                prices = history[symbol]
                previous_5 = prices[-5] if len(prices) >= 5 else None
                previous_10 = prices[-10] if len(prices) >= 10 else None
                moneyflow_status = "available"
                provider_evidence = None
                if qlib_missing:
                    moneyflow_status = "not_applicable_price_unavailable"
                elif flow_row is None:
                    try:
                        provider_evidence = provider_absence.resolve(
                            canonical_ts_code=symbol,
                            source_dataset="market.moneyflow_ts",
                            source_ts_code=flow_resolution.source_ts_code,
                            trade_date=day,
                        )
                    except Exception as exc:
                        raise _fail(
                            REASON_AUTHORITY_AMBIGUOUS,
                            f"moneyflow absence lacks exact provider authority: {symbol}/{day}",
                        ) from exc
                    else:
                        moneyflow_status = "provider_absence"
                elif any(not math.isfinite(float(flow_row[index])) for index in (1, 3, 13, 15, 17)):
                    moneyflow_status = "required_fields_invalid"
                if qlib_missing:
                    source_status = (
                        "source_invalid",
                        "hmm_risk_c010_price_unavailable_for_opportunity",
                        "frozen_release",
                    )
                elif basic_row is None or any(not math.isfinite(float(basic_row[index])) for index in (14, 15)):
                    source_status = (
                        "source_invalid",
                        "hmm_risk_rotation_l1_daily_basic_invalid",
                        "frozen_release",
                    )
                elif moneyflow_status == "provider_absence":
                    source_status = (
                        "provider_absence",
                        "hmm_risk_rotation_l1_provider_absence",
                        "tushare",
                    )
                elif moneyflow_status == "required_fields_invalid":
                    source_status = (
                        "source_invalid",
                        "hmm_risk_rotation_l1_moneyflow_invalid",
                        "moneyflow_h5",
                    )
                else:
                    source_status = ("available", "", "frozen_release")
                _advance_interval(
                    status_active,
                    status_done,
                    symbol=symbol,
                    day=day,
                    payload=source_status,
                    previous_trading_day=previous_day,
                )
                if qlib_missing:
                    day_rows.append(
                        {
                            "trade_date": day,
                            "symbol": symbol,
                            "l1_code": projection.l1_code,
                            "l1_name": projection.l1_name,
                            "l2_code": projection.l2_code,
                            "l2_name": projection.l2_name,
                            "is_suspended": False,
                            "open_yuan": None,
                            "high_yuan": None,
                            "low_yuan": None,
                            "close_yuan": None,
                            "volume_shares": None,
                            "amount_cny": None,
                            "prev_close_yuan": None,
                            "prev_close_5_yuan": previous_5,
                            "prev_close_10_yuan": previous_10,
                            "total_mv_cny": None if basic_row is None else float(basic_row[14]) * 10_000.0,
                            "prev_circ_mv_cny": prior_circ,
                            "up_limit_yuan": None,
                            "buy_sm_amount_cny": None,
                            "sell_sm_amount_cny": None,
                            "buy_elg_amount_cny": None,
                            "sell_elg_amount_cny": None,
                            "net_mf_amount_cny": None,
                            "moneyflow_fact_status": moneyflow_status,
                            "moneyflow_source_identity": flow_resolution.evidence(),
                            "moneyflow_provider_absence": (
                                None if provider_evidence is None else provider_evidence.evidence()
                            ),
                        }
                    )
                    continue
                qlib = _raw_qlib_values(raw)
                row = {
                    "trade_date": day,
                    "symbol": symbol,
                    "l1_code": projection.l1_code,
                    "l1_name": projection.l1_name,
                    "l2_code": projection.l2_code,
                    "l2_name": projection.l2_name,
                    "is_suspended": False,
                    "open_yuan": qlib["open"],
                    "high_yuan": qlib["high"],
                    "low_yuan": qlib["low"],
                    "close_yuan": qlib["close"],
                    "volume_shares": qlib["volume"],
                    "amount_cny": qlib["amount"],
                    "prev_close_yuan": qlib["prev_close"] if prices else None,
                    "prev_close_5_yuan": previous_5,
                    "prev_close_10_yuan": previous_10,
                    "total_mv_cny": None if basic_row is None else float(basic_row[14]) * 10_000.0,
                    "prev_circ_mv_cny": prior_circ,
                    "up_limit_yuan": qlib["up_limit_price"],
                    "buy_sm_amount_cny": None if flow_row is None else float(flow_row[1]),
                    "sell_sm_amount_cny": None if flow_row is None else float(flow_row[3]),
                    "buy_elg_amount_cny": None if flow_row is None else float(flow_row[13]),
                    "sell_elg_amount_cny": None if flow_row is None else float(flow_row[15]),
                    "net_mf_amount_cny": None if flow_row is None else float(flow_row[17]),
                    "moneyflow_fact_status": moneyflow_status,
                    "moneyflow_source_identity": flow_resolution.evidence(),
                    "moneyflow_provider_absence": None if provider_evidence is None else provider_evidence.evidence(),
                }
                day_rows.append(row)
                prices.append(qlib["close"])
            for symbol, value in current_basic_updates:
                circ_state[symbol] = (day, value)
            _append_day_level_aggregates(
                day_rows,
                l1_aggregates=l1_aggregates,
                l2_aggregates=l2_aggregates,
                unavailable=unavailable,
                contributor_eligibility=contributor_eligibility,
            )
    interval_evidence = {
        "industry": [
            {
                "canonical_security_id": symbol,
                "effective_from": start.isoformat(),
                "effective_to": end.isoformat(),
                "l1_code": payload[0],
                "l2_code": payload[1],
            }
            for symbol, start, end, payload in _finish_intervals(industry_active, industry_done)
        ],
        "status": [
            {
                "canonical_security_id": symbol,
                "valid_from": start.isoformat(),
                "valid_to": end.isoformat(),
                "status": payload[0],
                "reason_code": payload[1],
                "provider": payload[2],
            }
            for symbol, start, end, payload in _finish_intervals(status_active, status_done)
        ],
    }
    return l1_aggregates, l2_aggregates, unavailable, interval_evidence


def _security_intervals(security: Any, spans: Mapping[str, Sequence[tuple[date, date]]]) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[Any]] = defaultdict(list)
    for row in security.rows:
        if row.source_dataset in {"market.daily_basic", "market.moneyflow_ts"}:
            by_symbol[row.canonical_ts_code].append(row)
    output: list[dict[str, Any]] = []
    for symbol, active_spans in sorted(spans.items()):
        aliases = by_symbol.get(symbol, [])
        for span_start, span_end in active_spans:
            start = max(span_start, SOURCE_START)
            end = min(span_end, SOURCE_END)
            if start > end:
                continue
            boundaries = {start, end + timedelta(days=1)}
            for row in aliases:
                assert row.effective_start is not None and row.effective_end is not None
                if row.effective_end < start or row.effective_start > end:
                    continue
                boundaries.add(max(start, row.effective_start))
                if row.effective_end < end:
                    boundaries.add(row.effective_end + timedelta(days=1))
            ordered = sorted(boundaries)
            for left, right in zip(ordered, ordered[1:]):
                segment_end = right - timedelta(days=1)
                for dataset in ("market.daily_basic", "market.moneyflow_ts"):
                    resolution = security.resolve(symbol, left, dataset)
                    output.append(
                        {
                            "canonical_security_id": symbol,
                            "source_dataset": dataset,
                            "valid_from": left.isoformat(),
                            "valid_to": segment_end.isoformat(),
                            "source_code": resolution.source_ts_code,
                        }
                    )
    return sorted(
        output,
        key=lambda row: (
            row["canonical_security_id"],
            row["source_dataset"],
            row["valid_from"],
            row["source_code"],
        ),
    )


def _canonical_sector_codes(adapter: HMMIndustryPitAdapter) -> tuple[tuple[str, ...], tuple[str, ...]]:
    l1_codes = tuple(sorted(adapter.constituents))
    l2_codes = tuple(
        sorted(
            {
                str(code)
                for l1_code, value in adapter.constituents.items()
                if str(value.get("l1_code") or "") == l1_code
                for code in value.get("l2_codes", [])
            }
        )
    )
    return l1_codes, l2_codes


def _unavailable_rows(
    panel: pd.DataFrame,
    *,
    level: str,
    aggregate_unavailable: Mapping[tuple[date, str, str], str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    frame = panel.reset_index()
    code_field = "l1_code" if "l1_code" in frame.columns else "sector_code"
    for raw in frame.itertuples(index=False):
        day = pd.Timestamp(getattr(raw, "trade_date")).date()
        code = str(getattr(raw, code_field))
        aggregate_reason = aggregate_unavailable.get((day, level, code))
        for feature in FEATURE_NAMES:
            value = getattr(raw, feature)
            if pd.notna(value) and math.isfinite(float(value)):
                continue
            if aggregate_reason:
                reason = aggregate_reason
            elif feature in {"net_mf_ratio", "elg_net_mf_ratio"}:
                reason = "hmm_risk_rotation_l1_moneyflow_feature_unavailable"
            elif feature in {"sf_excess_breadth_5d", "sf_turnover_pctile_120d_neg"}:
                reason = "hmm_risk_rotation_l1_cross_section_feature_unavailable"
            else:
                reason = "hmm_risk_rotation_l1_feature_warmup"
            rows.append(
                {
                    "trade_date": day.isoformat(),
                    "level": level,
                    "sector_code": code,
                    "field": feature,
                    "reason_code": reason,
                    "source_observation_date": day.isoformat(),
                }
            )
    return sorted(
        rows,
        key=lambda row: (row["trade_date"], row["level"], row["sector_code"], row["field"], row["reason_code"]),
    )


def _receipt_from_body(body: Mapping[str, Any]) -> dict[str, Any]:
    return {**dict(body), "receipt_sha256": canonical_sha256(body)}


def build_rotation_l1_inputs_from_assets(
    *,
    dataset_release_manifest: Path,
    industry_authority: Mapping[str, Any],
    forbidden_roots: Sequence[Path],
    work_parent: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Build the existing RW1 in-memory input interface from frozen assets only."""

    started = time.perf_counter()
    assets = load_rotation_l1_source_assets(dataset_release_manifest)
    resource_receipts = [
        _resource_checkpoint(
            started,
            stage="source_inventory",
            max_seconds=BUILD_MAX_SECONDS,
            max_rss_bytes=BUILD_MAX_RSS_BYTES,
        )
    ]
    qlib_root = assets["qlib_root"]
    calendar_all = _load_qlib_calendar(qlib_root / "calendars" / "day.txt")
    calendar = tuple(day for day in calendar_all if SOURCE_START <= day <= SOURCE_END)
    if not calendar or calendar[0] != SOURCE_START or calendar[-1] != SOURCE_END:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "approved Qlib calendar slice is incomplete")
    spans = _parse_instrument_spans(qlib_root / "instruments" / "all.txt")
    adapter = _industry_adapter(industry_authority, forbidden_roots=forbidden_roots)
    security_payload = _read_json_object(assets["files"]["security_identity"], reason=REASON_SOURCE_SCHEMA_INVALID)
    provider_payload = _read_json_object(assets["files"]["provider_absence"], reason=REASON_SOURCE_SCHEMA_INVALID)
    try:
        security = load_security_source_identity_manifest(
            assets["files"]["security_identity"], expected_sha256=canonical_sha256(security_payload)
        )
        provider_absence = load_provider_absence_manifest(
            assets["files"]["provider_absence"], expected_sha256=canonical_sha256(provider_payload)
        )
    except Exception as exc:
        raise _fail(REASON_AUTHORITY_AMBIGUOUS, "security/provider authority cannot be bound") from exc
    security = _SecurityResolutionIndex(security)
    suspension_keys = _load_suspend_keys(
        assets["files"]["suspend_data"],
        assets["files"]["suspend_manifest"],
        calendar=calendar,
    )
    benchmark = _load_benchmark_returns(assets["files"]["index_context"], calendar=calendar)
    projection_index = _IndustryProjectionIndex(adapter, calendar=calendar)
    contributor_eligibility, eligibility_receipt = _build_train_only_contributor_eligibility(
        spans=spans,
        calendar=calendar,
        adapter=projection_index,
        security=security,
        provider_absence=provider_absence,
        suspension_keys=suspension_keys,
    )
    work_parent = Path(work_parent).resolve()
    work_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="hmm-rotation-l1-source-", dir=work_parent) as raw_temporary:
        month_paths = _spool_qlib_months(
            qlib_root,
            calendar=calendar_all,
            spans=spans,
            spool_root=Path(raw_temporary) / "qlib-months",
            resource_started=started,
        )
        resource_receipts.append(
            _resource_checkpoint(
                started,
                stage="qlib_month_spool",
                max_seconds=BUILD_MAX_SECONDS,
                max_rss_bytes=BUILD_MAX_RSS_BYTES,
            )
        )
        l1_aggregates, l2_aggregates, aggregate_unavailable, intervals = _build_stock_fact_aggregates(
            month_paths=month_paths,
            assets=assets,
            calendar=calendar,
            spans=spans,
            adapter=projection_index,
            security=security,
            provider_absence=provider_absence,
            suspension_keys=suspension_keys,
            contributor_eligibility=contributor_eligibility,
            resource_started=started,
        )
        observed_l1 = {(item.trade_date, item.l1_code) for item in l1_aggregates}
        observed_l2 = {(item.trade_date, item.l1_code) for item in l2_aggregates}
        l1_codes, l2_codes = _canonical_sector_codes(adapter)
        if len(l1_codes) != 31 or len(l2_codes) != 131:
            raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "C-013 canonical 31/131 sector set differs")
        for day in calendar:
            for code in l1_codes:
                if (day, code) not in observed_l1:
                    aggregate_unavailable.setdefault(
                        (day, "L1", code), "hmm_risk_rotation_l1_stock_fact_aggregate_unavailable"
                    )
            for code in l2_codes:
                if (day, code) not in observed_l2:
                    aggregate_unavailable.setdefault(
                        (day, "L2", code), "hmm_risk_rotation_l1_stock_fact_aggregate_unavailable"
                    )
        resource_receipts.append(
            _resource_checkpoint(
                started,
                stage="stock_fact_aggregation",
                max_seconds=BUILD_MAX_SECONDS,
                max_rss_bytes=BUILD_MAX_RSS_BYTES,
            )
        )
    try:
        l1_panel, l1_definition, l1_cross = build_c010_feature_domain_panel(
            l1_aggregates,
            trading_dates=calendar,
            csi300_returns=benchmark,
            expected_sector_count=31,
            direct_sector_level="L1",
            diagnostic_only=False,
        )
        l2_panel, l2_definition, l2_cross = build_c010_feature_domain_panel(
            l2_aggregates,
            trading_dates=calendar,
            csi300_returns=benchmark,
            expected_sector_count=131,
            direct_sector_level="L2",
            diagnostic_only=False,
        )
    except Exception as exc:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "C-010 feature panels cannot be built from frozen assets") from exc
    mapping_manifest = dict(
        adapter.mapping_manifest(
            universe_key=assets["formal_dataset_binding"].frozen_universe_key,
            source_start=SOURCE_START,
            source_end=SOURCE_END,
        )
    )
    unavailable_rows = [
        *_unavailable_rows(l1_panel, level="L1", aggregate_unavailable=aggregate_unavailable),
        *_unavailable_rows(l2_panel, level="L2", aggregate_unavailable=aggregate_unavailable),
    ]
    unavailable_rows.sort(
        key=lambda row: (row["trade_date"], row["level"], row["sector_code"], row["field"], row["reason_code"])
    )
    aggregate_body = {
        "schema_version": "hmm_risk_rotation_l1_bundle_aggregate_receipt_v1",
        "formula_version": C010_FORMULA_VERSION,
        "l1_aggregate_count": len(l1_aggregates),
        "l2_aggregate_count": len(l2_aggregates),
        "unavailable_identity_count": len(aggregate_unavailable),
        "unavailable_identity_sha256": canonical_sha256(
            [
                [day.isoformat(), level, code, reason]
                for (day, level, code), reason in sorted(aggregate_unavailable.items())
            ]
        ),
    }
    source = {
        "source_start": SOURCE_START.isoformat(),
        "source_end": SOURCE_END.isoformat(),
        "source_revision": SOURCE_REVISION,
        "circ_mv_history_start": SOURCE_START.isoformat(),
        "universe_key": assets["formal_dataset_binding"].frozen_universe_key,
        "universe_rule_version": assets["formal_dataset_binding"].rule_version,
    }
    source_identity = {
        "release_identity": dict(assets["release_identity"]),
        "source_inventory_sha256": assets["inventory"]["inventory_sha256"],
        "source_binding_manifest_sha256": assets["binding_manifest_sha256"],
        "qlib_schema_version": QLIB_STOCK_SCHEMA_VERSION,
        "qlib_schema_sha256": qlib_stock_schema_digest(),
        "c013_bundle_sha256": adapter.authority_bundle.manifest["bundle_hash"],
    }
    inputs = {
        "panel": l1_panel,
        "l2_panel": l2_panel,
        "trading_dates": calendar,
        "dataset_manifest": {
            "schema_version": "hmm_risk_rotation_l1_frozen_asset_dataset_manifest_v1",
            "calendar_benchmark": {
                "rows": [[day.isoformat(), benchmark[day]] for day in calendar],
            },
            "source_inventory": dict(assets["inventory"]),
        },
        "mapping_manifest": mapping_manifest,
        "security_identity_manifest": security.evidence(),
        "provider_absence_manifest": provider_absence.evidence(),
        "feature_definition": l1_definition,
        "l2_feature_definition": l2_definition,
        "c010_diagnostic": {
            "eligibility": eligibility_receipt,
            "aggregate_evidence": _receipt_from_body(aggregate_body),
            "l1_cross_section_evidence": l1_cross,
            "l2_cross_section_evidence": l2_cross,
        },
        "input_bundle_evidence": {
            "unavailable_reason": unavailable_rows,
            "security_identity_intervals": _security_intervals(security, spans),
            "industry_projection_intervals": intervals["industry"],
            "source_status_intervals": intervals["status"],
        },
        "source_build_resource_receipts": resource_receipts,
    }
    inputs["source_build_resource_receipts"].append(
        _resource_checkpoint(
            started,
            stage="feature_panel_complete",
            max_seconds=BUILD_MAX_SECONDS,
            max_rss_bytes=BUILD_MAX_RSS_BYTES,
        )
    )
    return inputs, source, source_identity


def _require_sha256(value: Any, field: str) -> str:
    normalized = str(value or "")
    if len(normalized) != 64 or any(char not in _SHA256_HEX for char in normalized):
        raise _fail(REASON_MANIFEST_INVALID, f"{field} is not a canonical SHA-256")
    return normalized


def _external_root(path: Path, forbidden_roots: Sequence[Path]) -> Path:
    value = path.resolve()
    if not value.is_absolute():
        raise _fail(REASON_MANIFEST_INVALID, "bundle root must be absolute")
    for forbidden in forbidden_roots:
        root = forbidden.resolve()
        try:
            value.relative_to(root)
        except ValueError:
            continue
        raise _fail(REASON_MANIFEST_INVALID, "bundle root is inside a forbidden repository root")
    return value


def _as_date(value: Any, field: str) -> date:
    if isinstance(value, pd.Timestamp):
        value = value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{field} is not an ISO date") from exc


def _date_from_yyyymmdd(value: Any, field: str) -> date:
    try:
        text = f"{int(value):08d}"
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except (TypeError, ValueError) as exc:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{field} is not YYYYMMDD") from exc


def _panel_records(panel: pd.DataFrame, *, expected_sector_count: int, level: str) -> tuple[np.ndarray, np.ndarray]:
    if not isinstance(panel, pd.DataFrame) or panel.empty:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, f"{level} panel is empty")
    frame = panel.reset_index() if isinstance(panel.index, pd.MultiIndex) else panel.copy()
    if "trade_date" not in frame.columns:
        if "date" in frame.columns:
            frame = frame.rename(columns={"date": "trade_date"})
        elif "datetime" in frame.columns:
            frame = frame.rename(columns={"datetime": "trade_date"})
    if "sector_code" not in frame.columns:
        for candidate in ("l1_code", "l2_code"):
            if candidate in frame.columns:
                frame = frame.rename(columns={candidate: "sector_code"})
                break
    required = {"trade_date", "sector_code", *FEATURE_NAMES}
    if not required.issubset(frame.columns):
        missing = sorted(required - set(frame.columns))
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{level} panel omits required fields", missing=missing)
    frame = frame.loc[:, ["trade_date", "sector_code", *FEATURE_NAMES]].copy()
    frame["trade_date"] = frame["trade_date"].map(lambda item: _as_date(item, f"{level}.trade_date"))
    frame["sector_code"] = frame["sector_code"].map(str)
    if frame[["trade_date", "sector_code"]].duplicated().any():
        raise _fail(REASON_DUPLICATE_KEY, f"{level} panel contains duplicate date/sector rows")
    frame = frame.sort_values(["trade_date", "sector_code"], kind="mergesort").reset_index(drop=True)
    codes = tuple(sorted(frame["sector_code"].unique().tolist()))
    if len(codes) != expected_sector_count:
        raise _fail(
            REASON_SOURCE_RANGE_INCOMPLETE,
            f"{level} canonical sector count differs",
            expected=expected_sector_count,
            actual=len(codes),
        )
    if any(len(code.encode("ascii", errors="strict")) > 16 for code in codes):
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{level} sector code exceeds fixed ASCII width")
    if any(day < SOURCE_START or day >= HOLDOUT_START for day in frame["trade_date"]):
        reason = (
            REASON_HOLDOUT_CONTAMINATION
            if any(day >= HOLDOUT_START for day in frame["trade_date"])
            else REASON_SOURCE_RANGE_INCOMPLETE
        )
        raise _fail(reason, f"{level} panel date range differs from the approved source range")

    values = np.zeros(len(frame), dtype=_PANEL_DTYPE)
    validity = np.zeros(len(frame), dtype=_VALIDITY_DTYPE)
    date_values = np.asarray([int(day.strftime("%Y%m%d")) for day in frame["trade_date"]], dtype="<i4")
    code_values = np.asarray([code.encode("ascii") for code in frame["sector_code"]], dtype="S16")
    values["trade_date"] = date_values
    validity["trade_date"] = date_values
    values["sector_code"] = code_values
    validity["sector_code"] = code_values
    for feature in FEATURE_NAMES:
        numeric = pd.to_numeric(frame[feature], errors="coerce").to_numpy(dtype=np.float64)
        mask = np.isfinite(numeric)
        values[feature] = np.where(mask, numeric, 0.0).astype("<f8", copy=False)
        validity[feature] = mask.astype("u1", copy=False)
    return values, validity


def _validate_panel_calendar_coverage(
    values: np.ndarray,
    calendar: Sequence[Mapping[str, Any]],
    *,
    level: str,
    expected_sector_count: int,
) -> None:
    calendar_days = tuple(int(row["trade_date"]) for row in calendar)
    codes = tuple(sorted({bytes(row["sector_code"]).rstrip(b"\x00") for row in values}))
    expected_keys = {(day, code) for day in calendar_days for code in codes}
    observed_keys = {(int(row["trade_date"]), bytes(row["sector_code"]).rstrip(b"\x00")) for row in values}
    if (
        len(codes) != expected_sector_count
        or len(values) != len(calendar_days) * expected_sector_count
        or observed_keys != expected_keys
    ):
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, f"{level} panel does not cover every calendar/sector key")


def _ascii(value: Any, field: str, *, width: int, allow_empty: bool = False) -> bytes:
    text = str(value or "")
    if not text and not allow_empty:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{field} is empty")
    try:
        encoded = text.encode("ascii", errors="strict")
    except UnicodeEncodeError as exc:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{field} is not ASCII") from exc
    if b"\x00" in encoded or len(encoded) > width:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{field} exceeds its fixed ASCII contract")
    return encoded


def _yyyymmdd(value: Any, field: str, *, allow_open_end: bool = False) -> int:
    if allow_open_end and value in (None, ""):
        return 99991231
    return int(_as_date(value, field).strftime("%Y%m%d"))


def _strict_structured_rows(
    rows: Any,
    *,
    dataset: str,
    dtype: np.dtype[Any],
    fields: Sequence[str],
    date_fields: frozenset[str],
    open_end_fields: frozenset[str] = frozenset(),
    widths: Mapping[str, int],
) -> np.ndarray:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)) or not rows:
        raise _fail(REASON_INCOMPLETE, f"{dataset} is empty")
    normalized: list[tuple[Any, ...]] = []
    expected = set(fields)
    for position, raw in enumerate(rows):
        if not isinstance(raw, Mapping) or set(raw) != expected:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{dataset} row schema differs", position=position)
        values: list[Any] = []
        for field in fields:
            value = raw[field]
            if field in date_fields:
                values.append(_yyyymmdd(value, f"{dataset}.{field}", allow_open_end=field in open_end_fields))
            else:
                values.append(
                    _ascii(
                        value,
                        f"{dataset}.{field}",
                        width=widths[field],
                        allow_empty=field in {"reason_code", "provider"},
                    )
                )
        normalized.append(tuple(values))
    if normalized != sorted(normalized) or len(normalized) != len(set(normalized)):
        raise _fail(REASON_DUPLICATE_KEY, f"{dataset} rows are not sorted and unique")
    result = np.zeros(len(normalized), dtype=dtype)
    for index, values in enumerate(normalized):
        result[index] = values
    return result


def _bundle_evidence_arrays(inputs: Mapping[str, Any]) -> dict[str, np.ndarray]:
    evidence = inputs.get("input_bundle_evidence")
    if not isinstance(evidence, Mapping) or set(evidence) != {
        "unavailable_reason",
        *_INTERVAL_DATASETS,
    }:
        raise _fail(REASON_INCOMPLETE, "bundle unavailable/authority evidence is incomplete")
    unavailable = _strict_structured_rows(
        evidence["unavailable_reason"],
        dataset="unavailable_reason",
        dtype=_UNAVAILABLE_DTYPE,
        fields=("trade_date", "level", "sector_code", "field", "reason_code", "source_observation_date"),
        date_fields=frozenset({"trade_date", "source_observation_date"}),
        widths={"level": 2, "sector_code": 16, "field": 64, "reason_code": 128},
    )
    security = _strict_structured_rows(
        evidence["security_identity_intervals"],
        dataset="security_identity_intervals",
        dtype=_SECURITY_INTERVAL_DTYPE,
        fields=("canonical_security_id", "source_dataset", "valid_from", "valid_to", "source_code"),
        date_fields=frozenset({"valid_from", "valid_to"}),
        open_end_fields=frozenset({"valid_to"}),
        widths={"canonical_security_id": 16, "source_dataset": 32, "source_code": 16},
    )
    industry = _strict_structured_rows(
        evidence["industry_projection_intervals"],
        dataset="industry_projection_intervals",
        dtype=_INDUSTRY_INTERVAL_DTYPE,
        fields=("canonical_security_id", "effective_from", "effective_to", "l1_code", "l2_code"),
        date_fields=frozenset({"effective_from", "effective_to"}),
        open_end_fields=frozenset({"effective_to"}),
        widths={"canonical_security_id": 16, "l1_code": 16, "l2_code": 16},
    )
    status = _strict_structured_rows(
        evidence["source_status_intervals"],
        dataset="source_status_intervals",
        dtype=_SOURCE_STATUS_INTERVAL_DTYPE,
        fields=("canonical_security_id", "valid_from", "valid_to", "status", "reason_code", "provider"),
        date_fields=frozenset({"valid_from", "valid_to"}),
        open_end_fields=frozenset({"valid_to"}),
        widths={"canonical_security_id": 16, "status": 32, "reason_code": 128, "provider": 64},
    )
    unavailable_reasons = {bytes(value).rstrip(b"\x00").decode("ascii") for value in unavailable["reason_code"]}
    if not unavailable_reasons.issubset(_UNAVAILABLE_REASON_CODES):
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "unavailable evidence contains an unknown reason code")
    security_datasets = {bytes(value).rstrip(b"\x00").decode("ascii") for value in security["source_dataset"]}
    if security_datasets != _SECURITY_INTERVAL_SOURCE_DATASETS:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "security identity evidence source dataset set differs")
    for raw in status:
        status_value = bytes(raw["status"]).rstrip(b"\x00").decode("ascii")
        reason_value = bytes(raw["reason_code"]).rstrip(b"\x00").decode("ascii")
        provider_value = bytes(raw["provider"]).rstrip(b"\x00").decode("ascii")
        if (
            status_value not in _SOURCE_STATUS_VALUES
            or provider_value not in _SOURCE_STATUS_PROVIDERS
            or (status_value == "available" and reason_value)
            or (status_value == "suspended" and reason_value != "hmm_risk_rotation_l1_suspended")
            or (status_value == "industry_unavailable" and not reason_value.startswith("classification:"))
            or (status_value == "provider_absence" and reason_value != "hmm_risk_rotation_l1_provider_absence")
            or (status_value == "source_invalid" and reason_value not in _SOURCE_INVALID_REASONS)
        ):
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "source status evidence contains an unknown status/reason")
    for name, values, start_field, end_field in (
        ("security_identity_intervals", security, "valid_from", "valid_to"),
        ("industry_projection_intervals", industry, "effective_from", "effective_to"),
        ("source_status_intervals", status, "valid_from", "valid_to"),
    ):
        starts = values[start_field].astype(np.int64)
        ends = values[end_field].astype(np.int64)
        if np.any(starts > ends):
            raise _fail(REASON_AUTHORITY_AMBIGUOUS, f"{name} contains a reversed interval")
    return {
        "unavailable_reason": unavailable,
        "security_identity_intervals": security,
        "industry_projection_intervals": industry,
        "source_status_intervals": status,
    }


def _encode_scalar(value: Any, kind: str) -> bytes:
    if kind == "i64":
        return struct.pack("<q", int(value))
    if kind == "u8":
        number = int(value)
        if number not in (0, 1):
            raise _fail(REASON_MASK_MISMATCH, "boolean field is not 0/1")
        return bytes((number,))
    if kind == "f64":
        number = float(value)
        if not math.isfinite(number):
            raise _fail(REASON_NON_FINITE, "valid numeric payload is non-finite")
        return struct.pack("<d", number)
    if kind == "str":
        normalized = unicodedata.normalize("NFC", str(value))
        encoded = normalized.encode("utf-8")
        if b"\x00" in encoded:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "canonical string contains NUL")
        return struct.pack("<I", len(encoded)) + encoded
    raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"unsupported canonical kind: {kind}")


def _logical_hash(name: str, schema: Sequence[tuple[str, str]], rows: Sequence[Mapping[str, Any]]) -> str:
    schema_payload = [{"name": field, "kind": kind} for field, kind in schema]
    schema_bytes = canonical_json_bytes(schema_payload)
    name_bytes = name.encode("utf-8")
    digest = hashlib.sha256()
    digest.update(struct.pack("<I", len(name_bytes)))
    digest.update(name_bytes)
    digest.update(struct.pack("<I", len(schema_bytes)))
    digest.update(schema_bytes)
    digest.update(struct.pack("<I", 1))
    digest.update(struct.pack("<Q", len(rows)))
    for row in rows:
        for field, kind in schema:
            digest.update(_encode_scalar(row[field], kind))
    return digest.hexdigest()


def _records_from_panel(values: np.ndarray, validity: np.ndarray) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    value_rows: list[dict[str, Any]] = []
    mask_rows: list[dict[str, Any]] = []
    for value, mask in zip(values, validity, strict=True):
        day = int(value["trade_date"])
        code = bytes(value["sector_code"]).rstrip(b"\x00").decode("ascii")
        row: dict[str, Any] = {"trade_date": day, "sector_code": code}
        mask_row: dict[str, Any] = {"trade_date": day, "sector_code": code}
        for feature in FEATURE_NAMES:
            valid = int(mask[feature])
            number = float(value[feature])
            if valid not in (0, 1) or (not valid and number != 0.0) or (valid and not math.isfinite(number)):
                raise _fail(REASON_MASK_MISMATCH, "panel payload and validity mask do not close")
            row[feature] = number
            mask_row[feature] = valid
        value_rows.append(row)
        mask_rows.append(mask_row)
    return value_rows, mask_rows


def _panel_hashes(level: str, values: np.ndarray, validity: np.ndarray) -> dict[str, str]:
    value_rows, mask_rows = _records_from_panel(values, validity)
    value_schema = (("trade_date", "i64"), ("sector_code", "str"), *[(name, "f64") for name in FEATURE_NAMES])
    mask_schema = (("trade_date", "i64"), ("sector_code", "str"), *[(name, "u8") for name in FEATURE_NAMES])
    return {
        f"{level}_panel": _logical_hash(f"/{level}_panel", value_schema, value_rows),
        f"{level}_validity": _logical_hash(f"/{level}_validity", mask_schema, mask_rows),
    }


def _structured_rows(values: np.ndarray, schema: Sequence[tuple[str, str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in values:
        row: dict[str, Any] = {}
        for field, kind in schema:
            value = raw[field]
            row[field] = (
                bytes(value).rstrip(b"\x00").decode("ascii")
                if kind == "str"
                else int(value)
                if kind == "i64"
                else float(value)
            )
        rows.append(row)
    return rows


_EVIDENCE_SCHEMAS: dict[str, tuple[tuple[str, str], ...]] = {
    "unavailable_reason": (
        ("trade_date", "i64"),
        ("level", "str"),
        ("sector_code", "str"),
        ("field", "str"),
        ("reason_code", "str"),
        ("source_observation_date", "i64"),
    ),
    "security_identity_intervals": (
        ("canonical_security_id", "str"),
        ("source_dataset", "str"),
        ("valid_from", "i64"),
        ("valid_to", "i64"),
        ("source_code", "str"),
    ),
    "industry_projection_intervals": (
        ("canonical_security_id", "str"),
        ("effective_from", "i64"),
        ("effective_to", "i64"),
        ("l1_code", "str"),
        ("l2_code", "str"),
    ),
    "source_status_intervals": (
        ("canonical_security_id", "str"),
        ("valid_from", "i64"),
        ("valid_to", "i64"),
        ("status", "str"),
        ("reason_code", "str"),
        ("provider", "str"),
    ),
}


def _evidence_hashes(evidence: Mapping[str, np.ndarray]) -> dict[str, str]:
    return {
        name: _logical_hash(f"/{name}", schema, _structured_rows(evidence[name], schema))
        for name, schema in _EVIDENCE_SCHEMAS.items()
    }


def _validate_evidence_readback(evidence: Mapping[str, np.ndarray]) -> None:
    expected_dtypes = {
        "unavailable_reason": _UNAVAILABLE_DTYPE,
        "security_identity_intervals": _SECURITY_INTERVAL_DTYPE,
        "industry_projection_intervals": _INDUSTRY_INTERVAL_DTYPE,
        "source_status_intervals": _SOURCE_STATUS_INTERVAL_DTYPE,
    }
    for name, dtype in expected_dtypes.items():
        values = evidence[name]
        if values.dtype != dtype or values.ndim != 1 or len(values) == 0:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{name} dtype/shape differs")
        rows = [tuple(raw[field].item() for field in dtype.names or ()) for raw in values]
        if rows != sorted(rows) or len(rows) != len(set(rows)):
            raise _fail(REASON_DUPLICATE_KEY, f"{name} is not sorted and unique")
    unavailable = evidence["unavailable_reason"]
    if np.any(unavailable["trade_date"] >= int(HOLDOUT_START.strftime("%Y%m%d"))):
        raise _fail(REASON_HOLDOUT_CONTAMINATION, "unavailable evidence contains holdout dates")
    unavailable_reasons = {bytes(value).rstrip(b"\x00").decode("ascii") for value in unavailable["reason_code"]}
    if not unavailable_reasons.issubset(_UNAVAILABLE_REASON_CODES):
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "unavailable evidence contains an unknown reason code")
    security_datasets = {
        bytes(value).rstrip(b"\x00").decode("ascii")
        for value in evidence["security_identity_intervals"]["source_dataset"]
    }
    if security_datasets != _SECURITY_INTERVAL_SOURCE_DATASETS:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "security identity evidence source dataset set differs")
    for raw in evidence["source_status_intervals"]:
        status_value = bytes(raw["status"]).rstrip(b"\x00").decode("ascii")
        reason_value = bytes(raw["reason_code"]).rstrip(b"\x00").decode("ascii")
        provider_value = bytes(raw["provider"]).rstrip(b"\x00").decode("ascii")
        if (
            status_value not in _SOURCE_STATUS_VALUES
            or provider_value not in _SOURCE_STATUS_PROVIDERS
            or (status_value == "available" and reason_value)
            or (status_value == "suspended" and reason_value != "hmm_risk_rotation_l1_suspended")
            or (status_value == "industry_unavailable" and not reason_value.startswith("classification:"))
            or (status_value == "provider_absence" and reason_value != "hmm_risk_rotation_l1_provider_absence")
            or (status_value == "source_invalid" and reason_value not in _SOURCE_INVALID_REASONS)
        ):
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "source status evidence contains an unknown status/reason")
    for name, start_field, end_field, identity_fields in (
        (
            "security_identity_intervals",
            "valid_from",
            "valid_to",
            ("canonical_security_id", "source_dataset"),
        ),
        ("industry_projection_intervals", "effective_from", "effective_to", ("canonical_security_id",)),
        ("source_status_intervals", "valid_from", "valid_to", ("canonical_security_id",)),
    ):
        values = evidence[name]
        if np.any(values[start_field] > values[end_field]):
            raise _fail(REASON_AUTHORITY_AMBIGUOUS, f"{name} contains a reversed interval")
        previous: dict[tuple[bytes, ...], int] = {}
        for raw in values:
            identity = tuple(bytes(raw[field]).rstrip(b"\x00") for field in identity_fields)
            start = int(raw[start_field])
            end = int(raw[end_field])
            if identity in previous and start <= previous[identity]:
                raise _fail(REASON_AUTHORITY_AMBIGUOUS, f"{name} contains overlapping intervals")
            previous[identity] = end


def _validate_authority_daily_coverage(
    evidence: Mapping[str, np.ndarray],
    trading_dates: Sequence[date],
    *,
    resource_started: float | None = None,
) -> None:
    def grouped(name: str) -> dict[bytes, list[np.void]]:
        result: dict[bytes, list[np.void]] = defaultdict(list)
        for row in evidence[name]:
            result[bytes(row["canonical_security_id"]).rstrip(b"\x00")].append(row)
        return result

    security: dict[bytes, dict[bytes, list[np.void]]] = defaultdict(lambda: defaultdict(list))
    for row in evidence["security_identity_intervals"]:
        symbol = bytes(row["canonical_security_id"]).rstrip(b"\x00")
        dataset = bytes(row["source_dataset"]).rstrip(b"\x00")
        security[symbol][dataset].append(row)
    industry = grouped("industry_projection_intervals")
    statuses = grouped("source_status_intervals")
    calendar_values = [int(day.strftime("%Y%m%d")) for day in trading_dates]
    expected_datasets = {value.encode("ascii") for value in _SECURITY_INTERVAL_SOURCE_DATASETS}
    for symbol_index, (symbol, spans_by_dataset) in enumerate(security.items()):
        if resource_started is not None and symbol_index % 64 == 0:
            _resource_checkpoint(
                resource_started,
                stage="bundle_readback_authority",
                max_seconds=READBACK_MAX_SECONDS,
                max_rss_bytes=READBACK_MAX_RSS_BYTES,
            )
        industry_rows = industry.get(symbol, [])
        status_rows = statuses.get(symbol, [])
        if set(spans_by_dataset) != expected_datasets:
            raise _fail(REASON_AUTHORITY_AMBIGUOUS, "security identity dataset coverage differs")
        security_indices = {dataset: 0 for dataset in expected_datasets}
        industry_index = 0
        status_index = 0
        for day in calendar_values:
            security_hits: dict[bytes, np.void | None] = {}
            for dataset in expected_datasets:
                spans = spans_by_dataset[dataset]
                index = security_indices[dataset]
                while index < len(spans) and int(spans[index]["valid_to"]) < day:
                    index += 1
                security_indices[dataset] = index
                security_hits[dataset] = (
                    spans[index]
                    if index < len(spans) and int(spans[index]["valid_from"]) <= day <= int(spans[index]["valid_to"])
                    else None
                )
            if all(hit is None for hit in security_hits.values()):
                continue
            if any(hit is None for hit in security_hits.values()):
                raise _fail(REASON_AUTHORITY_AMBIGUOUS, "security identity daily dataset coverage differs")
            while status_index < len(status_rows) and int(status_rows[status_index]["valid_to"]) < day:
                status_index += 1
            status_hit = (
                status_rows[status_index]
                if status_index < len(status_rows)
                and int(status_rows[status_index]["valid_from"]) <= day <= int(status_rows[status_index]["valid_to"])
                else None
            )
            if status_hit is None:
                raise _fail(REASON_AUTHORITY_AMBIGUOUS, "source status does not have one daily interval hit")
            status = bytes(status_hit["status"]).rstrip(b"\x00").decode("ascii")
            while industry_index < len(industry_rows) and int(industry_rows[industry_index]["effective_to"]) < day:
                industry_index += 1
            industry_hit = (
                industry_rows[industry_index]
                if industry_index < len(industry_rows)
                and int(industry_rows[industry_index]["effective_from"])
                <= day
                <= int(industry_rows[industry_index]["effective_to"])
                else None
            )
            if (status == "industry_unavailable") != (industry_hit is None):
                raise _fail(REASON_AUTHORITY_AMBIGUOUS, "industry projection daily interval coverage differs")
    if set(industry).difference(security) or set(statuses).difference(security):
        raise _fail(REASON_AUTHORITY_AMBIGUOUS, "authority intervals contain securities outside the frozen denominator")


def _calendar_records(dataset_manifest: Mapping[str, Any], trading_dates: Sequence[Any]) -> list[dict[str, Any]]:
    calendar = tuple(_as_date(value, "trading_dates") for value in trading_dates)
    if calendar != tuple(sorted(set(calendar))) or not calendar:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "trading calendar is not sorted, unique and non-empty")
    if calendar[0] != SOURCE_START or calendar[-1] != SOURCE_END or any(day >= HOLDOUT_START for day in calendar):
        reason = (
            REASON_HOLDOUT_CONTAMINATION
            if any(day >= HOLDOUT_START for day in calendar)
            else REASON_SOURCE_RANGE_INCOMPLETE
        )
        raise _fail(reason, "trading calendar range differs from the approved bundle boundary")
    source = dataset_manifest.get("calendar_benchmark")
    rows = source.get("rows") if isinstance(source, Mapping) else None
    if not isinstance(rows, list):
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "calendar benchmark rows are missing")
    benchmark: dict[date, float] = {}
    for item in rows:
        if not isinstance(item, list) or len(item) != 2:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "calendar benchmark row is invalid")
        day = _as_date(item[0], "calendar_benchmark.trade_date")
        value = float(item[1])
        if day in benchmark or not math.isfinite(value):
            raise _fail(REASON_DUPLICATE_KEY, "calendar benchmark is duplicate or non-finite")
        benchmark[day] = value
    if set(benchmark) != set(calendar):
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "calendar and benchmark date sets differ")
    return [{"trade_date": int(day.strftime("%Y%m%d")), "benchmark_return": benchmark[day]} for day in calendar]


def _validated_source_identity(value: Any) -> dict[str, Any]:
    expected = {
        "release_identity",
        "source_inventory_sha256",
        "source_binding_manifest_sha256",
        "qlib_schema_version",
        "qlib_schema_sha256",
        "c013_bundle_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise _fail(REASON_MANIFEST_INVALID, "bundle source identity schema differs")
    try:
        release_binding = QEFormalDatasetBinding.from_mapping(value.get("release_identity"))
    except (TypeError, ValueError) as exc:
        raise _fail(REASON_MANIFEST_INVALID, "bundle release identity is incomplete") from exc
    if release_binding.usage_mode != FormalDatasetUsage.TRAINING.value:
        raise _fail(REASON_MANIFEST_INVALID, "bundle release identity is not formal-training authority")
    for field in (
        "source_inventory_sha256",
        "source_binding_manifest_sha256",
        "qlib_schema_sha256",
        "c013_bundle_sha256",
    ):
        _require_sha256(value.get(field), f"source_identity.{field}")
    if (
        value.get("qlib_schema_version") != QLIB_STOCK_SCHEMA_VERSION
        or value.get("qlib_schema_sha256") != qlib_stock_schema_digest()
    ):
        raise _fail(REASON_MANIFEST_INVALID, "bundle Qlib source schema identity differs")
    return {**dict(value), "release_identity": release_binding.as_dict()}


def _validated_source_build_receipts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) != len(SOURCE_BUILD_STAGES):
        raise _fail(REASON_INCOMPLETE, "source build resource receipts are incomplete")
    normalized: list[dict[str, Any]] = []
    previous_elapsed = -1.0
    for expected_stage, raw in zip(SOURCE_BUILD_STAGES, value, strict=True):
        if not isinstance(raw, Mapping) or set(raw) != {"stage", "elapsed_seconds", "rss_bytes"}:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "source build resource receipt schema differs")
        elapsed = float(raw["elapsed_seconds"])
        rss = int(raw["rss_bytes"])
        if (
            raw.get("stage") != expected_stage
            or not math.isfinite(elapsed)
            or elapsed < previous_elapsed
            or elapsed > BUILD_MAX_SECONDS
            or rss < 0
            or rss > BUILD_MAX_RSS_BYTES
        ):
            raise _fail(REASON_RESOURCE_BUDGET, "source build resource receipt exceeds its contract")
        previous_elapsed = elapsed
        normalized.append({"stage": expected_stage, "elapsed_seconds": elapsed, "rss_bytes": rss})
    return normalized


def _validated_mapping_manifest(value: Any, *, source: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        "schema_version",
        "universe_key",
        "source_window_start",
        "source_window_end",
        "canonical_l1_count",
        "canonical_l2_count",
        "source_classification_authority_receipt_hash",
        "classification_authority_receipt_hash",
        "index_membership_authority_receipt_hash",
        "classification_candidate_hash",
        "stable_backcast_candidate_sha256",
        "index_membership_candidate_hash",
        "candidate_bundle_hash",
        "candidate_preflight_canonical_hash",
        "research_basis_contract_sha256",
        "active_classification_basis",
        "non_as_known_taxonomy",
        "l1_code_projection_sha256",
        "l2_code_projection_sha256",
        "constituent_manifest_hash",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise _fail(REASON_MANIFEST_INVALID, "bundle mapping manifest schema differs")
    if (
        value.get("schema_version") != HMM_MAPPING_MANIFEST_SCHEMA
        or value.get("universe_key") != source.get("universe_key")
        or value.get("source_window_start") != SOURCE_START.isoformat()
        or value.get("source_window_end") != SOURCE_END.isoformat()
        or value.get("canonical_l1_count") != 31
        or value.get("canonical_l2_count") != 131
        or value.get("active_classification_basis") != "stable_taxonomy_backcast"
        or value.get("non_as_known_taxonomy") is not True
    ):
        raise _fail(REASON_MANIFEST_INVALID, "bundle mapping manifest business authority differs")
    for field in expected - {
        "schema_version",
        "universe_key",
        "source_window_start",
        "source_window_end",
        "canonical_l1_count",
        "canonical_l2_count",
        "active_classification_basis",
        "non_as_known_taxonomy",
    }:
        _require_sha256(value.get(field), f"mapping_manifest.{field}")
    return dict(value)


def _input_metadata(
    inputs: Mapping[str, Any], source: Mapping[str, Any], source_identity: Mapping[str, Any]
) -> dict[str, Any]:
    required = (
        "mapping_manifest",
        "security_identity_manifest",
        "provider_absence_manifest",
        "feature_definition",
        "l2_feature_definition",
    )
    if any(not isinstance(inputs.get(field), Mapping) or not inputs.get(field) for field in required):
        raise _fail(REASON_INCOMPLETE, "bundle input metadata is incomplete")
    c010 = inputs.get("c010_diagnostic")
    if not isinstance(c010, Mapping):
        raise _fail(REASON_INCOMPLETE, "C-010 formal evidence is missing")
    identities: dict[str, Any] = {}
    for name in ("eligibility", "aggregate_evidence", "l1_cross_section_evidence"):
        value = c010.get(name)
        if not isinstance(value, Mapping):
            raise _fail(REASON_INCOMPLETE, f"C-010 {name} is missing")
        receipt = _require_sha256(value.get("receipt_sha256"), f"c010.{name}.receipt_sha256")
        if canonical_sha256({key: item for key, item in value.items() if key != "receipt_sha256"}) != receipt:
            raise _fail(REASON_HASH_MISMATCH, f"C-010 {name} receipt hash differs")
        identities[f"{name}_receipt_sha256"] = receipt
    identities["l1_feature_definition_sha256"] = canonical_sha256(inputs["feature_definition"])
    identities["provider_absence_manifest_sha256"] = canonical_sha256(inputs["provider_absence_manifest"])
    identities["security_identity_manifest_sha256"] = canonical_sha256(inputs["security_identity_manifest"])
    identities["receipt_sha256"] = canonical_sha256(identities)
    return {
        "source": dict(source),
        "source_identity": _validated_source_identity(source_identity),
        "mapping_manifest": _validated_mapping_manifest(inputs["mapping_manifest"], source=source),
        "security_identity_manifest": dict(inputs["security_identity_manifest"]),
        "provider_absence_manifest": dict(inputs["provider_absence_manifest"]),
        "feature_definition": dict(inputs["feature_definition"]),
        "l2_feature_definition": dict(inputs["l2_feature_definition"]),
        "c010_bundle_identity": identities,
    }


def _write_h5(
    path: Path,
    calendar: Sequence[Mapping[str, Any]],
    l1: np.ndarray,
    l1_valid: np.ndarray,
    l2: np.ndarray,
    l2_valid: np.ndarray,
    evidence: Mapping[str, np.ndarray],
) -> None:
    calendar_dtype = np.dtype([("trade_date", "<i4"), ("benchmark_return", "<f8")], align=False)
    calendar_values = np.zeros(len(calendar), dtype=calendar_dtype)
    calendar_values["trade_date"] = [int(row["trade_date"]) for row in calendar]
    calendar_values["benchmark_return"] = [float(row["benchmark_return"]) for row in calendar]
    with h5py.File(path, "x", libver="latest") as handle:
        handle.create_dataset("calendar_benchmark", data=calendar_values, chunks=True, shuffle=True)
        handle.create_dataset("l1_panel", data=l1, chunks=True, shuffle=True)
        handle.create_dataset("l1_validity", data=l1_valid, chunks=True, shuffle=True)
        handle.create_dataset("l2_panel", data=l2, chunks=True, shuffle=True)
        handle.create_dataset("l2_validity", data=l2_valid, chunks=True, shuffle=True)
        for name in ("unavailable_reason", *_INTERVAL_DATASETS):
            handle.create_dataset(name, data=evidence[name], chunks=True, shuffle=True)
        handle.attrs["schema_version"] = MANIFEST_SCHEMA_VERSION
        handle.attrs["canonical_serialization_version"] = CANONICAL_SERIALIZATION_VERSION
        handle.flush()
    with path.open("r+b") as raw:
        os.fsync(raw.fileno())


def _manifest_body(
    *,
    h5_path: Path,
    calendar: Sequence[Mapping[str, Any]],
    l1: np.ndarray,
    l1_valid: np.ndarray,
    l2: np.ndarray,
    l2_valid: np.ndarray,
    evidence: Mapping[str, np.ndarray],
    metadata: Mapping[str, Any],
    producer_commit: str,
) -> dict[str, Any]:
    if len(producer_commit) != 40 or any(char not in _SHA256_HEX for char in producer_commit):
        raise _fail(REASON_MANIFEST_INVALID, "producer commit must be a full lowercase Git SHA")
    logical = {
        "calendar_benchmark": _logical_hash(
            "/calendar_benchmark",
            (("trade_date", "i64"), ("benchmark_return", "f64")),
            calendar,
        ),
        **_panel_hashes("l1", l1, l1_valid),
        **_panel_hashes("l2", l2, l2_valid),
        **_evidence_hashes(evidence),
    }
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "input_contract_version": INPUT_CONTRACT_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "canonical_serialization_version": CANONICAL_SERIALIZATION_VERSION,
        "source_revision": SOURCE_REVISION,
        "producer_commit": producer_commit,
        "date_range": {"start": SOURCE_START.isoformat(), "end": SOURCE_END.isoformat()},
        "feature_names": list(FEATURE_NAMES),
        "calendar_row_count": len(calendar),
        "l1_row_count": len(l1),
        "l2_row_count": len(l2),
        "l1_sector_count": len({bytes(row["sector_code"]).rstrip(b"\x00") for row in l1}),
        "l2_sector_count": len({bytes(row["sector_code"]).rstrip(b"\x00") for row in l2}),
        "unavailable_reason_row_count": len(evidence["unavailable_reason"]),
        "security_identity_interval_count": len(evidence["security_identity_intervals"]),
        "industry_projection_interval_count": len(evidence["industry_projection_intervals"]),
        "source_status_interval_count": len(evidence["source_status_intervals"]),
        "logical_dataset_sha256": logical,
        "h5_file": {
            "name": "rotation_l1_input.h5",
            "size_bytes": h5_path.stat().st_size,
            "sha256": _sha256_file(h5_path),
        },
        "metadata": dict(metadata),
        "database_read_performed": False,
        "database_write_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "runtime_action_performed": False,
    }


def _complete_manifest(body: Mapping[str, Any]) -> dict[str, Any]:
    body_hash = canonical_sha256(body)
    bundle_hash = canonical_sha256(
        {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "manifest_body_sha256": body_hash,
            "h5_sha256": body["h5_file"]["sha256"],
        }
    )
    return {**dict(body), "manifest_body_sha256": body_hash, "bundle_canonical_sha256": bundle_hash}


def _write_json_once(path: Path, value: Mapping[str, Any]) -> None:
    payload = canonical_json_bytes(dict(value)) + b"\n"
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _failure_receipt(error: Exception) -> dict[str, Any]:
    reason = error.reason_code if isinstance(error, RotationL1InputBundleError) else REASON_INCOMPLETE
    context = (
        error.context if isinstance(error, RotationL1InputBundleError) else {"exception_type": type(error).__name__}
    )
    body = {
        "schema_version": BUILD_RECEIPT_SCHEMA_VERSION,
        "status": "failed",
        "primary_reason_code": reason,
        "failure_reason_codes": [reason],
        "message": str(error),
        "context": context,
        "bundle_write_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def write_rotation_l1_input_bundle(
    *,
    inputs: Mapping[str, Any],
    source: Mapping[str, Any],
    source_identity: Mapping[str, Any],
    output_root: Path,
    producer_commit: str,
    forbidden_roots: Sequence[Path],
) -> dict[str, Any]:
    """Write one immutable bundle via same-parent temporary directory and readback."""

    final = _external_root(output_root, forbidden_roots)
    parent = final.parent
    parent.mkdir(parents=True, exist_ok=True)
    temporary = parent / f".{final.name}.partial.{uuid.uuid4().hex}"
    temporary.mkdir(parents=False, exist_ok=False)
    try:
        dataset_manifest = inputs.get("dataset_manifest")
        if not isinstance(dataset_manifest, Mapping):
            raise _fail(REASON_INCOMPLETE, "dataset manifest is missing")
        calendar = _calendar_records(dataset_manifest, inputs.get("trading_dates") or ())
        l1, l1_valid = _panel_records(inputs.get("panel"), expected_sector_count=31, level="L1")
        l2, l2_valid = _panel_records(inputs.get("l2_panel"), expected_sector_count=131, level="L2")
        _validate_panel_calendar_coverage(l1, calendar, level="L1", expected_sector_count=31)
        _validate_panel_calendar_coverage(l2, calendar, level="L2", expected_sector_count=131)
        evidence = _bundle_evidence_arrays(inputs)
        resource_receipts = _validated_source_build_receipts(inputs.get("source_build_resource_receipts"))
        metadata = _input_metadata(inputs, source, source_identity)
        h5_path = temporary / "rotation_l1_input.h5"
        _write_h5(h5_path, calendar, l1, l1_valid, l2, l2_valid, evidence)
        manifest = _complete_manifest(
            _manifest_body(
                h5_path=h5_path,
                calendar=calendar,
                l1=l1,
                l1_valid=l1_valid,
                l2=l2,
                l2_valid=l2_valid,
                evidence=evidence,
                metadata=metadata,
                producer_commit=producer_commit,
            )
        )
        _write_json_once(temporary / "manifest.json", manifest)
        receipt_body = {
            "schema_version": BUILD_RECEIPT_SCHEMA_VERSION,
            "status": "success",
            "bundle_canonical_sha256": manifest["bundle_canonical_sha256"],
            "manifest_body_sha256": manifest["manifest_body_sha256"],
            "h5_sha256": manifest["h5_file"]["sha256"],
            "calendar_row_count": manifest["calendar_row_count"],
            "l1_row_count": manifest["l1_row_count"],
            "l2_row_count": manifest["l2_row_count"],
            "source_build_resource_receipts": resource_receipts,
            "peak_rss_bytes": max(
                (int(item["rss_bytes"]) for item in resource_receipts),
                default=int(psutil.Process(os.getpid()).memory_info().rss),
            ),
            "database_read_performed": False,
            "database_write_performed": False,
            "model_write_performed": False,
            "ready_artifact_write_performed": False,
            "runtime_action_performed": False,
        }
        receipt = {**receipt_body, "receipt_sha256": canonical_sha256(receipt_body)}
        _write_json_once(temporary / "build.receipt.json", receipt)
        read_rotation_l1_input_bundle(temporary, forbidden_roots=forbidden_roots)
        if final.exists():
            existing = read_rotation_l1_input_bundle(final, forbidden_roots=forbidden_roots)
            if existing["input_bundle_identity"]["bundle_canonical_sha256"] != manifest["bundle_canonical_sha256"]:
                raise _fail(REASON_COLLISION, "existing bundle identity differs")
            shutil.rmtree(temporary)
            return {**receipt, "status": "EXISTING_BUNDLE"}
        os.replace(temporary, final)
        return receipt
    except Exception as exc:
        try:
            _write_json_once(temporary / "build.failure.json", _failure_receipt(exc))
        except Exception as receipt_exc:
            raise _fail(
                REASON_INCOMPLETE,
                "bundle failure receipt could not be persisted",
                primary_reason_code=getattr(exc, "reason_code", REASON_INCOMPLETE),
                primary_exception_type=type(exc).__name__,
                receipt_exception_type=type(receipt_exc).__name__,
            ) from receipt_exc
        raise


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _fail(REASON_MANIFEST_INVALID, f"cannot read {path.name}") from exc
    if not isinstance(value, dict):
        raise _fail(REASON_MANIFEST_INVALID, f"{path.name} is not an object")
    return value


def _frame_from_arrays(values: np.ndarray, validity: np.ndarray) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for value, mask in zip(values, validity, strict=True):
        day = _date_from_yyyymmdd(value["trade_date"], "panel.trade_date")
        code = bytes(value["sector_code"]).rstrip(b"\x00").decode("ascii")
        row: dict[str, Any] = {"trade_date": day, "sector_code": code}
        for feature in FEATURE_NAMES:
            valid = int(mask[feature])
            number = float(value[feature])
            if valid not in (0, 1) or (valid == 0 and number != 0.0) or (valid == 1 and not math.isfinite(number)):
                raise _fail(REASON_MASK_MISMATCH, "readback panel mask differs from payload")
            row[feature] = number if valid else np.nan
        rows.append(row)
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise _fail(REASON_INCOMPLETE, "readback panel is empty")
    return frame.set_index(["trade_date", "sector_code"]).sort_index()


def _validate_panel_readback(
    values: np.ndarray,
    validity: np.ndarray,
    *,
    level: str,
    expected_sector_count: int,
) -> None:
    if (
        values.dtype != _PANEL_DTYPE
        or validity.dtype != _VALIDITY_DTYPE
        or values.ndim != 1
        or validity.ndim != 1
        or len(values) != len(validity)
        or len(values) == 0
    ):
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, f"{level} panel dtype/shape differs")
    value_keys = [(int(row["trade_date"]), bytes(row["sector_code"]).rstrip(b"\x00")) for row in values]
    mask_keys = [(int(row["trade_date"]), bytes(row["sector_code"]).rstrip(b"\x00")) for row in validity]
    if value_keys != mask_keys or value_keys != sorted(value_keys) or len(value_keys) != len(set(value_keys)):
        raise _fail(REASON_DUPLICATE_KEY, f"{level} panel keys are not aligned, sorted and unique")
    codes = {code for _, code in value_keys}
    if len(codes) != expected_sector_count:
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, f"{level} panel canonical sector count differs")
    start = int(SOURCE_START.strftime("%Y%m%d"))
    end = int(SOURCE_END.strftime("%Y%m%d"))
    if any(day < start or day > end for day, _ in value_keys):
        reason = (
            REASON_HOLDOUT_CONTAMINATION
            if any(day >= 20260401 for day, _ in value_keys)
            else REASON_SOURCE_RANGE_INCOMPLETE
        )
        raise _fail(reason, f"{level} panel date range escapes the approved source window")
    _records_from_panel(values, validity)


def read_rotation_l1_input_bundle(root: Path, *, forbidden_roots: Sequence[Path]) -> dict[str, Any]:
    """Verify every bundle layer before returning any panel."""

    started = time.perf_counter()
    bundle_root = _external_root(root, forbidden_roots)
    if not bundle_root.is_dir():
        raise _fail(REASON_INCOMPLETE, "bundle root does not exist")
    expected_files = {"rotation_l1_input.h5", "manifest.json", "build.receipt.json"}
    actual_files = {item.name for item in bundle_root.iterdir() if item.is_file()}
    if actual_files != expected_files or any(item.is_dir() for item in bundle_root.iterdir()):
        raise _fail(REASON_INCOMPLETE, "bundle file set differs")
    manifest = _load_json(bundle_root / "manifest.json")
    manifest_keys = {
        "schema_version",
        "input_contract_version",
        "algorithm_version",
        "canonical_serialization_version",
        "source_revision",
        "producer_commit",
        "date_range",
        "feature_names",
        "calendar_row_count",
        "l1_row_count",
        "l2_row_count",
        "l1_sector_count",
        "l2_sector_count",
        "unavailable_reason_row_count",
        "security_identity_interval_count",
        "industry_projection_interval_count",
        "source_status_interval_count",
        "logical_dataset_sha256",
        "h5_file",
        "metadata",
        "database_read_performed",
        "database_write_performed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "runtime_action_performed",
        "manifest_body_sha256",
        "bundle_canonical_sha256",
    }
    if set(manifest) != manifest_keys or manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise _fail(REASON_MANIFEST_INVALID, "bundle manifest schema differs")
    if (
        manifest.get("input_contract_version") != INPUT_CONTRACT_VERSION
        or manifest.get("algorithm_version") != ALGORITHM_VERSION
        or manifest.get("canonical_serialization_version") != CANONICAL_SERIALIZATION_VERSION
        or manifest.get("source_revision") != SOURCE_REVISION
        or manifest.get("date_range") != {"start": SOURCE_START.isoformat(), "end": SOURCE_END.isoformat()}
        or manifest.get("feature_names") != list(FEATURE_NAMES)
        or manifest.get("database_read_performed") is not False
        or manifest.get("database_write_performed") is not False
        or manifest.get("model_write_performed") is not False
        or manifest.get("ready_artifact_write_performed") is not False
        or manifest.get("runtime_action_performed") is not False
        or len(str(manifest.get("producer_commit") or "")) != 40
        or any(character not in _SHA256_HEX for character in str(manifest.get("producer_commit") or ""))
    ):
        raise _fail(REASON_MANIFEST_INVALID, "bundle manifest business contract differs")
    body = {
        key: value for key, value in manifest.items() if key not in {"manifest_body_sha256", "bundle_canonical_sha256"}
    }
    body_hash = _require_sha256(manifest.get("manifest_body_sha256"), "manifest_body_sha256")
    if canonical_sha256(body) != body_hash:
        raise _fail(REASON_HASH_MISMATCH, "bundle manifest body hash differs")
    h5_identity = manifest.get("h5_file")
    h5_path = bundle_root / "rotation_l1_input.h5"
    if (
        not isinstance(h5_identity, Mapping)
        or set(h5_identity) != {"name", "size_bytes", "sha256"}
        or h5_identity.get("name") != h5_path.name
        or h5_identity.get("size_bytes") != h5_path.stat().st_size
        or _require_sha256(h5_identity.get("sha256"), "h5_file.sha256") != _sha256_file(h5_path)
    ):
        raise _fail(REASON_HASH_MISMATCH, "bundle H5 identity differs")
    expected_bundle_hash = canonical_sha256(
        {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "manifest_body_sha256": body_hash,
            "h5_sha256": h5_identity["sha256"],
        }
    )
    if _require_sha256(manifest.get("bundle_canonical_sha256"), "bundle_canonical_sha256") != expected_bundle_hash:
        raise _fail(REASON_HASH_MISMATCH, "bundle canonical hash differs")
    with h5py.File(h5_path, "r") as handle:
        expected_datasets = {
            "calendar_benchmark",
            "l1_panel",
            "l1_validity",
            "l2_panel",
            "l2_validity",
            "unavailable_reason",
            *_INTERVAL_DATASETS,
        }
        if set(handle.keys()) != expected_datasets:
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "bundle H5 dataset set differs")
        if (
            set(handle.attrs) != {"schema_version", "canonical_serialization_version"}
            or handle.attrs.get("schema_version") != MANIFEST_SCHEMA_VERSION
            or handle.attrs.get("canonical_serialization_version") != CANONICAL_SERIALIZATION_VERSION
        ):
            raise _fail(REASON_SOURCE_SCHEMA_INVALID, "bundle H5 attributes differ")
        calendar_values = handle["calendar_benchmark"][:]
        l1 = handle["l1_panel"][:]
        l1_valid = handle["l1_validity"][:]
        l2 = handle["l2_panel"][:]
        l2_valid = handle["l2_validity"][:]
        evidence = {name: handle[name][:] for name in ("unavailable_reason", *_INTERVAL_DATASETS)}
    calendar_dtype = np.dtype([("trade_date", "<i4"), ("benchmark_return", "<f8")], align=False)
    if calendar_values.dtype != calendar_dtype or calendar_values.ndim != 1 or len(calendar_values) == 0:
        raise _fail(REASON_SOURCE_SCHEMA_INVALID, "calendar benchmark dtype/shape differs")
    _validate_panel_readback(l1, l1_valid, level="L1", expected_sector_count=31)
    _validate_panel_readback(l2, l2_valid, level="L2", expected_sector_count=131)
    _validate_evidence_readback(evidence)
    calendar = [
        {"trade_date": int(row["trade_date"]), "benchmark_return": float(row["benchmark_return"])}
        for row in calendar_values
    ]
    calendar_days = [_date_from_yyyymmdd(row["trade_date"], "calendar.trade_date") for row in calendar]
    if (
        calendar_days != sorted(set(calendar_days))
        or calendar_days[0] != SOURCE_START
        or calendar_days[-1] != SOURCE_END
        or any(not math.isfinite(float(row["benchmark_return"])) for row in calendar)
    ):
        raise _fail(REASON_SOURCE_RANGE_INCOMPLETE, "calendar benchmark contract differs")
    _validate_panel_calendar_coverage(l1, calendar, level="L1", expected_sector_count=31)
    _validate_panel_calendar_coverage(l2, calendar, level="L2", expected_sector_count=131)
    _validate_authority_daily_coverage(evidence, calendar_days, resource_started=started)
    if (
        manifest.get("calendar_row_count") != len(calendar)
        or manifest.get("l1_row_count") != len(l1)
        or manifest.get("l2_row_count") != len(l2)
        or manifest.get("l1_sector_count") != 31
        or manifest.get("l2_sector_count") != 131
    ):
        raise _fail(REASON_HASH_MISMATCH, "bundle manifest panel counts differ")
    observed_logical = {
        "calendar_benchmark": _logical_hash(
            "/calendar_benchmark", (("trade_date", "i64"), ("benchmark_return", "f64")), calendar
        ),
        **_panel_hashes("l1", l1, l1_valid),
        **_panel_hashes("l2", l2, l2_valid),
        **_evidence_hashes(evidence),
    }
    if manifest.get("logical_dataset_sha256") != observed_logical:
        raise _fail(REASON_HASH_MISMATCH, "bundle logical dataset hash differs")
    expected_counts = {
        "unavailable_reason_row_count": len(evidence["unavailable_reason"]),
        "security_identity_interval_count": len(evidence["security_identity_intervals"]),
        "industry_projection_interval_count": len(evidence["industry_projection_intervals"]),
        "source_status_interval_count": len(evidence["source_status_intervals"]),
    }
    if any(manifest.get(field) != value for field, value in expected_counts.items()):
        raise _fail(REASON_HASH_MISMATCH, "bundle evidence row counts differ")
    receipt = _load_json(bundle_root / "build.receipt.json")
    receipt_keys = {
        "schema_version",
        "status",
        "bundle_canonical_sha256",
        "manifest_body_sha256",
        "h5_sha256",
        "calendar_row_count",
        "l1_row_count",
        "l2_row_count",
        "source_build_resource_receipts",
        "peak_rss_bytes",
        "database_read_performed",
        "database_write_performed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "runtime_action_performed",
        "receipt_sha256",
    }
    if set(receipt) != receipt_keys or receipt.get("schema_version") != BUILD_RECEIPT_SCHEMA_VERSION:
        raise _fail(REASON_MANIFEST_INVALID, "bundle build receipt schema differs")
    receipt_hash = _require_sha256(receipt.get("receipt_sha256"), "receipt_sha256")
    if canonical_sha256({key: value for key, value in receipt.items() if key != "receipt_sha256"}) != receipt_hash:
        raise _fail(REASON_HASH_MISMATCH, "bundle build receipt hash differs")
    if (
        receipt.get("status") != "success"
        or receipt.get("bundle_canonical_sha256") != expected_bundle_hash
        or receipt.get("h5_sha256") != h5_identity["sha256"]
        or receipt.get("manifest_body_sha256") != body_hash
        or receipt.get("calendar_row_count") != len(calendar)
        or receipt.get("l1_row_count") != len(l1)
        or receipt.get("l2_row_count") != len(l2)
        or any(
            receipt.get(field) is not False
            for field in (
                "database_read_performed",
                "database_write_performed",
                "model_write_performed",
                "ready_artifact_write_performed",
                "runtime_action_performed",
            )
        )
    ):
        raise _fail(REASON_INCOMPLETE, "bundle build receipt is not successful or differs")
    metadata = manifest.get("metadata")
    metadata_keys = {
        "source",
        "source_identity",
        "mapping_manifest",
        "security_identity_manifest",
        "provider_absence_manifest",
        "feature_definition",
        "l2_feature_definition",
        "c010_bundle_identity",
    }
    if not isinstance(metadata, Mapping) or set(metadata) != metadata_keys:
        raise _fail(REASON_MANIFEST_INVALID, "bundle metadata is missing")
    source = metadata.get("source")
    source_identity = _validated_source_identity(metadata.get("source_identity"))
    release_binding = QEFormalDatasetBinding.from_mapping(source_identity["release_identity"])
    source_build_receipts = _validated_source_build_receipts(receipt.get("source_build_resource_receipts"))
    c010_identity = metadata.get("c010_bundle_identity")
    if not isinstance(source, Mapping) or not isinstance(c010_identity, Mapping):
        raise _fail(REASON_MANIFEST_INVALID, "bundle compact authority is incomplete")
    if (
        set(source)
        != {
            "source_start",
            "source_end",
            "source_revision",
            "circ_mv_history_start",
            "universe_key",
            "universe_rule_version",
        }
        or source.get("source_start") != SOURCE_START.isoformat()
        or source.get("source_end") != SOURCE_END.isoformat()
        or source.get("source_revision") != SOURCE_REVISION
        or source.get("circ_mv_history_start") != SOURCE_START.isoformat()
        or source.get("universe_key") != release_binding.frozen_universe_key
        or source.get("universe_rule_version") != release_binding.rule_version
    ):
        raise _fail(REASON_MANIFEST_INVALID, "bundle source authority differs")
    mapping_manifest = _validated_mapping_manifest(metadata.get("mapping_manifest"), source=source)
    c010_keys = {
        "eligibility_receipt_sha256",
        "aggregate_evidence_receipt_sha256",
        "l1_cross_section_evidence_receipt_sha256",
        "l1_feature_definition_sha256",
        "provider_absence_manifest_sha256",
        "security_identity_manifest_sha256",
        "receipt_sha256",
    }
    if set(c010_identity) != c010_keys:
        raise _fail(REASON_MANIFEST_INVALID, "bundle C-010 identity schema differs")
    for field in c010_keys:
        _require_sha256(c010_identity.get(field), f"c010_bundle_identity.{field}")
    if (
        canonical_sha256({key: value for key, value in c010_identity.items() if key != "receipt_sha256"})
        != (c010_identity["receipt_sha256"])
    ):
        raise _fail(REASON_HASH_MISMATCH, "bundle C-010 identity hash differs")
    if (
        c010_identity["l1_feature_definition_sha256"] != canonical_sha256(metadata["feature_definition"])
        or c010_identity["provider_absence_manifest_sha256"] != canonical_sha256(metadata["provider_absence_manifest"])
        or c010_identity["security_identity_manifest_sha256"]
        != canonical_sha256(metadata["security_identity_manifest"])
    ):
        raise _fail(REASON_HASH_MISMATCH, "bundle C-010 identity does not close over embedded metadata")
    peak_rss = max(int(item["rss_bytes"]) for item in source_build_receipts)
    if (
        receipt.get("source_build_resource_receipts") != source_build_receipts
        or receipt.get("peak_rss_bytes") != peak_rss
    ):
        raise _fail(REASON_HASH_MISMATCH, "bundle resource receipt readback differs")
    trading_dates = tuple(_date_from_yyyymmdd(row["trade_date"], "calendar.trade_date") for row in calendar)
    dataset_manifest = {
        "schema_version": "hmm_risk_rotation_l1_input_bundle_dataset_manifest_v1",
        "calendar_benchmark": {
            "schema_version": "hmm_risk_calendar_benchmark_manifest_v1",
            "rows": [
                [day.isoformat(), float(row["benchmark_return"])]
                for day, row in zip(trading_dates, calendar, strict=True)
            ],
        },
        "input_bundle_identity": {
            "bundle_canonical_sha256": expected_bundle_hash,
            "manifest_body_sha256": body_hash,
            "h5_sha256": h5_identity["sha256"],
        },
    }
    result = {
        "panel": _frame_from_arrays(l1, l1_valid),
        "l2_panel": _frame_from_arrays(l2, l2_valid),
        "trading_dates": trading_dates,
        "dataset_manifest": dataset_manifest,
        "mapping_manifest": mapping_manifest,
        "feature_definition": dict(metadata.get("feature_definition") or {}),
        "l2_feature_definition": dict(metadata.get("l2_feature_definition") or {}),
        "security_identity_manifest": dict(metadata.get("security_identity_manifest") or {}),
        "provider_absence_manifest": dict(metadata.get("provider_absence_manifest") or {}),
        "c010_bundle_identity": dict(c010_identity),
        "source": dict(source),
        "source_identity": dict(source_identity),
        "input_bundle_identity": dataset_manifest["input_bundle_identity"],
        "input_bundle_evidence": {
            name: _structured_rows(evidence[name], _EVIDENCE_SCHEMAS[name])
            for name in ("unavailable_reason", *_INTERVAL_DATASETS)
        },
    }
    _resource_checkpoint(
        started,
        stage="bundle_readback",
        max_seconds=READBACK_MAX_SECONDS,
        max_rss_bytes=READBACK_MAX_RSS_BYTES,
    )
    return result


__all__ = [
    "ALGORITHM_VERSION",
    "BUILD_RECEIPT_SCHEMA_VERSION",
    "CANONICAL_SERIALIZATION_VERSION",
    "FEATURE_NAMES",
    "INPUT_CONTRACT_VERSION",
    "MANIFEST_SCHEMA_VERSION",
    "SOURCE_ASSET_SCHEMA_VERSION",
    "RotationL1InputBundleError",
    "build_rotation_l1_inputs_from_assets",
    "load_rotation_l1_source_assets",
    "read_rotation_l1_input_bundle",
    "write_rotation_l1_input_bundle",
]
