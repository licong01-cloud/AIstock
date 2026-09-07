"""Approved G2-A v1.2 development executor for L1 sector rotation.

This module owns the model-facing, development-only contract. It consumes an
already materialised causal panel and fits the approved target-free market
context inside each fresh process; it never queries a database, searches a
release, reads a tail, or silently manufactures missing features.
"""

from __future__ import annotations

import importlib.metadata
import hashlib
import json
import math
import os
import platform
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from backend.services.hmm_risk.market_relative_jump_spike import (
    PreparedComponent,
    Preprocessor,
    SequenceData,
    causal_states,
    fit_jump_model,
)
from backend.services.hmm_risk.state_model_set import canonical_sha256
from backend.services.dataset_release.cas_store import canonical_json_bytes

CONTRACT_VERSION = "hmm_risk_rotation_l1_g2a_v1_2"
INPUT_SCHEMA_VERSION = "hmm_risk_rotation_l1_g2a_input_bundle_v1"
PROCESS_SCHEMA_VERSION = "hmm_risk_rotation_l1_g2a_process_v1"
ACCEPTANCE_SCHEMA_VERSION = "hmm_risk_rotation_l1_g2a_acceptance_v1"
INPUT_MANIFEST_SCHEMA_VERSION = "hmm_risk_rotation_l1_g2a_input_manifest_v1"

CONTINUOUS_FEATURES = (
    "relative_momentum_5d",
    "relative_momentum_10d",
    "relative_momentum_20d",
    "relative_momentum_60d",
    "relative_downside_volatility_20d",
    "relative_max_drawdown_20d",
    "pit_breadth_above_ma20",
    "moneyflow_intensity_20d",
)
FEATURES = (*CONTINUOUS_FEATURES, "market_regime_sign")
TARGET_COLUMNS = ("target_5d", "target_10d")
VALUE_COLUMNS = (*CONTINUOUS_FEATURES, *TARGET_COLUMNS)
REASON_COLUMNS = tuple(f"reason__{column}" for column in VALUE_COLUMNS)
MATURITY_COLUMNS = ("target_5d_mature", "target_10d_mature")
MARKET_FEATURES = ("daily_return", "volatility_3d")
HORIZONS = (5, 10)
ROLLING_WINDOW_OPEN_DAYS = 504
MINIMUM_METRIC_SECTORS = 28
CANONICAL_SECTOR_COUNT = 31
MINIMUM_VALID_CONTINUOUS_FEATURES = 7
MINIMUM_DEVELOPMENT_COVERAGE = 0.90
MINIMUM_LEAF_DISTINCT_DATES = 20
BINDING_MBE_IC = 0.02
STATE_TIE_EPSILON = 1e-12
SINGLE_THREAD_ENVIRONMENT = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)

FOLDS = (
    ("fold-1", date(2023, 9, 4), date(2024, 3, 14)),
    ("fold-2", date(2024, 3, 15), date(2024, 9, 18)),
    ("fold-3", date(2024, 9, 19), date(2025, 3, 31)),
    ("fold-4", date(2025, 4, 1), date(2025, 9, 30)),
    ("fold-5", date(2025, 10, 1), date(2026, 3, 31)),
)

REASON_INPUT = "hmm_risk_rotation_input_contract_invalid"
REASON_LABEL = "hmm_risk_rotation_label_incomplete"
REASON_HORIZON = "hmm_risk_rotation_horizon_selection_failed"
REASON_FEATURE = "hmm_risk_rotation_feature_contract_invalid"
REASON_FIT = "hmm_risk_rotation_fit_failed"
REASON_REPRODUCIBILITY = "hmm_risk_rotation_reproducibility_mismatch"
REASON_SCORE = "hmm_risk_rotation_score_non_finite"
REASON_LEAF = "hmm_risk_rotation_leaf_date_coverage_insufficient"
REASON_COVERAGE = "hmm_risk_rotation_coverage_insufficient"


class RotationL1G2AError(RuntimeError):
    """Typed fail-closed G2-A error."""

    def __init__(self, reason_code: str, message: str, *, stage: str, evidence: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.stage = stage
        self.evidence = dict(evidence or {})


def _fail(reason: str, message: str, *, stage: str, **evidence: Any) -> RotationL1G2AError:
    return RotationL1G2AError(reason, message, stage=stage, evidence=evidence)


@dataclass(frozen=True)
class FoldSlice:
    name: str
    train_dates: tuple[date, ...]
    purge_dates: tuple[date, ...]
    validation_dates: tuple[date, ...]

    def receipt(self) -> dict[str, Any]:
        body = {
            "fold": self.name,
            "train_start": self.train_dates[0].isoformat(),
            "train_end": self.train_dates[-1].isoformat(),
            "train_count": len(self.train_dates),
            "train_date_sha256": canonical_sha256([item.isoformat() for item in self.train_dates]),
            "purge_dates": [item.isoformat() for item in self.purge_dates],
            "validation_start": self.validation_dates[0].isoformat(),
            "validation_end": self.validation_dates[-1].isoformat(),
            "validation_count": len(self.validation_dates),
            "validation_date_sha256": canonical_sha256([item.isoformat() for item in self.validation_dates]),
        }
        return {**body, "receipt_sha256": canonical_sha256(body)}


@dataclass(frozen=True)
class MarketContext:
    signs: Mapping[date, float]
    receipt: Mapping[str, Any]


@dataclass
class FitProgress:
    planned: int
    started: int = 0
    completed: int = 0
    failed: int = 0
    active_fit: str | None = None

    def execute(self, identity: str, operation: Any) -> Any:
        self.started += 1
        self.active_fit = identity
        try:
            result = operation()
        except Exception:
            self.failed += 1
            raise
        self.completed += 1
        self.active_fit = None
        return result

    def receipt(self) -> dict[str, Any]:
        return {
            "planned": self.planned,
            "started": self.started,
            "completed": self.completed,
            "failed": self.failed,
            "active_fit": self.active_fit,
        }


def _fit_progress_error(error: BaseException, progress: FitProgress) -> RotationL1G2AError:
    evidence = dict(getattr(error, "evidence", {}) or {})
    evidence["fit_progress"] = progress.receipt()
    return RotationL1G2AError(
        str(getattr(error, "reason_code", REASON_FIT)),
        str(error),
        stage=str(getattr(error, "stage", "execution")),
        evidence=evidence,
    )


def _normalise_panel(panel: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(panel.index, pd.MultiIndex) or tuple(panel.index.names) != ("trade_date", "sector_code"):
        raise _fail(REASON_INPUT, "G2-A panel index must be (trade_date, sector_code)", stage="input")
    frame = panel.copy()
    dates = pd.to_datetime(frame.index.get_level_values("trade_date"), errors="raise")
    if dates.tz is not None or not dates.equals(dates.normalize()):
        raise _fail(REASON_INPUT, "G2-A trade dates are not canonical sessions", stage="input")
    codes = frame.index.get_level_values("sector_code").astype(str)
    frame.index = pd.MultiIndex.from_arrays([dates.date, codes], names=["trade_date", "sector_code"])
    if frame.index.has_duplicates:
        raise _fail(REASON_INPUT, "G2-A panel contains duplicate date/sector rows", stage="input")
    if not frame.index.is_monotonic_increasing:
        frame = frame.sort_index()
    return frame


def build_materialised_panel(
    *,
    calendar: Sequence[date],
    sector_close: Mapping[tuple[date, str], float],
    benchmark_close: Mapping[date, float],
    stock_daily_inputs: Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    """Build the exact nine-column model panel from already bound components.

    The CSI300 market regime is fitted later inside each fresh process. Missing
    values stay missing; this function never imputes or shortens a lookback.
    """

    ordered = tuple(calendar)
    if ordered != tuple(sorted(set(ordered))) or not ordered:
        raise _fail(REASON_INPUT, "G2-A materialisation calendar differs", stage="materialisation")
    sectors = tuple(sorted({str(code) for _day, code in sector_close}))
    if len(sectors) != CANONICAL_SECTOR_COUNT:
        raise _fail(REASON_INPUT, "G2-A materialisation sector denominator differs", stage="materialisation")
    stock_by_key: dict[tuple[date, str], Mapping[str, Any]] = {}
    for raw in stock_daily_inputs:
        key = (raw.get("source_date"), str(raw.get("sector_code")))
        if not isinstance(key[0], date) or key in stock_by_key:
            raise _fail(REASON_INPUT, "G2-A stock-derived input keys differ", stage="materialisation")
        stock_by_key[key] = raw
    position = {day: index for index, day in enumerate(ordered)}

    def finite_close(day: date, sector: str | None = None) -> float | None:
        raw = benchmark_close.get(day) if sector is None else sector_close.get((day, sector))
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        return value if math.isfinite(value) and value > 0 else None

    def relative_returns(days: Sequence[date], sector: str) -> list[float] | None:
        values: list[float] = []
        for day in days:
            index = position[day]
            if index == 0:
                return None
            previous = ordered[index - 1]
            sector_now, sector_previous = finite_close(day, sector), finite_close(previous, sector)
            market_now, market_previous = finite_close(day), finite_close(previous)
            if None in {sector_now, sector_previous, market_now, market_previous}:
                return None
            assert sector_now is not None and sector_previous is not None
            assert market_now is not None and market_previous is not None
            values.append(sector_now / sector_previous - market_now / market_previous)
        return values

    rows: list[dict[str, Any]] = []
    for decision_index, decision_day in enumerate(ordered):
        raw_targets: dict[int, dict[str, float]] = {item: {} for item in HORIZONS}
        for horizon in HORIZONS:
            future_index = decision_index + horizon
            if future_index >= len(ordered):
                continue
            future_day = ordered[future_index]
            market_now, market_future = finite_close(decision_day), finite_close(future_day)
            if market_now is None or market_future is None:
                continue
            for sector in sectors:
                sector_now, sector_future = finite_close(decision_day, sector), finite_close(future_day, sector)
                if sector_now is None or sector_future is None:
                    raw_targets[horizon].clear()
                    break
                raw_targets[horizon][sector] = sector_future / sector_now - market_future / market_now
        centered_targets: dict[int, dict[str, float]] = {}
        for horizon in HORIZONS:
            values = raw_targets[horizon]
            if len(values) == CANONICAL_SECTOR_COUNT:
                median = float(np.median(list(values.values())))
                centered_targets[horizon] = {sector: value - median for sector, value in values.items()}
            else:
                centered_targets[horizon] = {}

        for sector in sectors:
            row: dict[str, Any] = {"trade_date": decision_day, "sector_code": sector}
            for lookback in (5, 10, 20, 60):
                if decision_index < lookback:
                    value = math.nan
                else:
                    source_days = ordered[decision_index - lookback : decision_index]
                    sector_start = (
                        finite_close(ordered[decision_index - lookback - 1], sector)
                        if decision_index > lookback
                        else None
                    )
                    sector_end = finite_close(ordered[decision_index - 1], sector)
                    market_start = (
                        finite_close(ordered[decision_index - lookback - 1]) if decision_index > lookback else None
                    )
                    market_end = finite_close(ordered[decision_index - 1])
                    value = (
                        sector_end / sector_start - market_end / market_start
                        if None not in {sector_start, sector_end, market_start, market_end}
                        and len(source_days) == lookback
                        else math.nan
                    )
                row[f"relative_momentum_{lookback}d"] = value
            if decision_index >= 20:
                source_days = ordered[decision_index - 20 : decision_index]
                excess = relative_returns(source_days, sector)
                sector_path = [finite_close(item, sector) for item in source_days]
                market_path = [finite_close(item) for item in source_days]
                if excess is not None:
                    row["relative_downside_volatility_20d"] = -math.sqrt(
                        math.fsum(min(item, 0.0) ** 2 for item in excess) / 20.0
                    )
                else:
                    row["relative_downside_volatility_20d"] = math.nan
                if None not in sector_path and None not in market_path:
                    sector_array = np.asarray(sector_path, dtype=np.float64)
                    market_array = np.asarray(market_path, dtype=np.float64)
                    sector_mdd = float(np.max(1.0 - sector_array / np.maximum.accumulate(sector_array)))
                    market_mdd = float(np.max(1.0 - market_array / np.maximum.accumulate(market_array)))
                    row["relative_max_drawdown_20d"] = market_mdd - sector_mdd
                else:
                    row["relative_max_drawdown_20d"] = math.nan
                daily_window = [stock_by_key.get((item, sector)) for item in source_days]
                if all(item is not None for item in daily_window):
                    typed_window = [item for item in daily_window if item is not None]
                    net = [item.get("moneyflow_net_amount_cny") for item in typed_window]
                    amount = [item.get("moneyflow_traded_amount_cny") for item in typed_window]
                    if (
                        all(value is not None and math.isfinite(float(value)) for value in (*net, *amount))
                        and math.fsum(float(value) for value in amount) > 0
                    ):
                        row["moneyflow_intensity_20d"] = math.fsum(float(value) for value in net) / math.fsum(
                            float(value) for value in amount
                        )
                    else:
                        row["moneyflow_intensity_20d"] = math.nan
                else:
                    row["moneyflow_intensity_20d"] = math.nan
            else:
                row["relative_downside_volatility_20d"] = math.nan
                row["relative_max_drawdown_20d"] = math.nan
                row["moneyflow_intensity_20d"] = math.nan
            previous_day = ordered[decision_index - 1] if decision_index else None
            breadth = stock_by_key.get((previous_day, sector)) if previous_day is not None else None
            breadth_value = breadth.get("pit_breadth_above_ma20") if breadth is not None else None
            row["pit_breadth_above_ma20"] = (
                float(breadth_value) if breadth_value is not None and math.isfinite(float(breadth_value)) else math.nan
            )
            for horizon in HORIZONS:
                row[f"target_{horizon}d"] = centered_targets[horizon].get(sector, math.nan)
                row[f"target_{horizon}d_mature"] = math.isfinite(float(row[f"target_{horizon}d"]))
            for feature in CONTINUOUS_FEATURES:
                value = float(row[feature])
                if math.isfinite(value):
                    row[f"reason__{feature}"] = None
                elif feature == "pit_breadth_above_ma20":
                    row[f"reason__{feature}"] = (
                        str(breadth.get("breadth_reason_code"))
                        if breadth is not None and breadth.get("breadth_reason_code")
                        else "hmm_risk_rotation_breadth_coverage_insufficient"
                    )
                elif feature == "moneyflow_intensity_20d":
                    daily_window = (
                        [stock_by_key.get((item, sector)) for item in ordered[decision_index - 20 : decision_index]]
                        if decision_index >= 20
                        else []
                    )
                    reasons = [
                        str(item["moneyflow_reason_code"])
                        for item in daily_window
                        if item is not None and item.get("moneyflow_reason_code")
                    ]
                    row[f"reason__{feature}"] = (
                        sorted(reasons)[0] if reasons else "hmm_risk_rotation_moneyflow_history_incomplete"
                    )
                else:
                    row[f"reason__{feature}"] = "hmm_risk_rotation_price_history_incomplete"
            for target in TARGET_COLUMNS:
                row[f"reason__{target}"] = (
                    None if bool(row[f"{target}_mature"]) else "hmm_risk_rotation_label_not_mature_or_incomplete"
                )
            rows.append(row)
    return pd.DataFrame.from_records(rows).set_index(["trade_date", "sector_code"]).sort_index()


def validate_input_bundle(
    bundle: Mapping[str, Any],
) -> tuple[pd.DataFrame, tuple[date, ...], tuple[str, ...], dict[date, float]]:
    """Validate the materialised development panel without accepting aliases."""

    if (
        set(bundle) != {"schema_version", "panel", "benchmark_close", "identity"}
        or bundle.get("schema_version") != INPUT_SCHEMA_VERSION
    ):
        raise _fail(REASON_INPUT, "G2-A input bundle envelope differs", stage="input")
    identity = bundle.get("identity")
    if not isinstance(identity, Mapping):
        raise _fail(REASON_INPUT, "G2-A input identity is missing", stage="input")
    required_identity = {
        "source_sha256",
        "mapping_sha256",
        "feature_contract_sha256",
        "development_end",
        "source_cutoff",
        "tail_mature_decision_counts",
        "tail_mature_date_sha256",
    }
    if set(identity) != required_identity or identity.get("development_end") != "2026-03-31":
        raise _fail(REASON_INPUT, "G2-A input identity differs", stage="input")
    for field in {"source_sha256", "mapping_sha256", "feature_contract_sha256", "tail_mature_date_sha256"}:
        value = identity.get(field)
        if not isinstance(value, str) or len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
            raise _fail(REASON_INPUT, f"G2-A {field} is not canonical SHA-256", stage="input")
    try:
        source_cutoff = date.fromisoformat(str(identity["source_cutoff"]))
    except ValueError as exc:
        raise _fail(REASON_INPUT, "G2-A source cutoff is invalid", stage="input") from exc
    tail_counts = identity.get("tail_mature_decision_counts")
    if (
        source_cutoff < date(2026, 4, 1)
        or not isinstance(tail_counts, Mapping)
        or set(tail_counts) != {"5", "10"}
        or any(not isinstance(tail_counts[key], int) or tail_counts[key] < 0 for key in ("5", "10"))
    ):
        raise _fail(REASON_INPUT, "G2-A tail maturity identity differs", stage="input")
    panel = bundle.get("panel")
    if not isinstance(panel, pd.DataFrame):
        raise _fail(REASON_INPUT, "G2-A panel is missing", stage="input")
    frame = _normalise_panel(panel)
    expected_columns = {*VALUE_COLUMNS, *REASON_COLUMNS, *MATURITY_COLUMNS}
    if set(frame.columns) != expected_columns:
        raise _fail(REASON_FEATURE, "G2-A feature/target columns differ", stage="input")
    calendar = tuple(sorted(set(frame.index.get_level_values("trade_date"))))
    sectors = tuple(sorted(set(frame.index.get_level_values("sector_code"))))
    if len(sectors) != CANONICAL_SECTOR_COUNT or not calendar or calendar[-1] != date(2026, 3, 31):
        raise _fail(REASON_INPUT, "G2-A canonical sector/calendar boundary differs", stage="input")
    counts = frame.groupby(level="trade_date", sort=True).size()
    if not counts.eq(CANONICAL_SECTOR_COUNT).all():
        raise _fail(REASON_INPUT, "G2-A daily canonical denominator differs", stage="input")
    if any(not pd.api.types.is_numeric_dtype(frame[column]) for column in VALUE_COLUMNS):
        raise _fail(REASON_FEATURE, "G2-A numeric value column dtype differs", stage="input")
    try:
        numeric = frame.loc[:, list(VALUE_COLUMNS)].apply(pd.to_numeric, errors="raise")
    except (TypeError, ValueError) as exc:
        raise _fail(REASON_FEATURE, "G2-A numeric value columns contain non-numeric data", stage="input") from exc
    for column in VALUE_COLUMNS:
        values = numeric[column].to_numpy(dtype=np.float64)
        if np.isinf(values).any():
            raise _fail(REASON_FEATURE, f"G2-A {column} contains infinite values", stage="input")
        reasons = frame[f"reason__{column}"]
        valid_reason = reasons.map(lambda value: value is None or (isinstance(value, str) and bool(value)))
        finite = np.isfinite(values)
        reason_missing = reasons.isna().to_numpy()
        if not bool(valid_reason.all()) or np.any(finite != reason_missing):
            raise _fail(REASON_FEATURE, f"G2-A {column} validity/reason differs", stage="input")
    for horizon in HORIZONS:
        target = f"target_{horizon}d"
        maturity = frame[f"{target}_mature"]
        if not maturity.map(lambda value: isinstance(value, (bool, np.bool_))).all():
            raise _fail(REASON_LABEL, f"G2-A {target} maturity type differs", stage="input")
        if not np.array_equal(maturity.to_numpy(dtype=bool), np.isfinite(numeric[target].to_numpy(dtype=np.float64))):
            raise _fail(REASON_LABEL, f"G2-A {target} maturity/value differs", stage="input")
    frame.loc[:, list(VALUE_COLUMNS)] = numeric
    raw_benchmark = bundle.get("benchmark_close")
    if not isinstance(raw_benchmark, Mapping):
        raise _fail(REASON_INPUT, "G2-A CSI300 close input is missing", stage="input")
    benchmark: dict[date, float] = {}
    for raw_day, raw_value in raw_benchmark.items():
        if not isinstance(raw_day, date):
            raise _fail(REASON_INPUT, "G2-A CSI300 date identity differs", stage="input")
        try:
            value = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise _fail(REASON_INPUT, "G2-A CSI300 close is invalid", stage="input") from exc
        if not math.isfinite(value) or value <= 0:
            raise _fail(REASON_INPUT, "G2-A CSI300 close is invalid", stage="input")
        benchmark[raw_day] = value
    if any(day not in benchmark for day in calendar):
        raise _fail(REASON_INPUT, "G2-A CSI300 calendar coverage differs", stage="input")
    return frame, calendar, sectors, benchmark


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _logical_input_sha256(panel: pd.DataFrame, benchmark: Mapping[date, float]) -> str:
    rows = []
    for identity, raw in panel.sort_index().iterrows():
        values = []
        for column in VALUE_COLUMNS:
            value = float(raw[column])
            values.append(value if math.isfinite(value) else None)
        reasons = [raw[column] if isinstance(raw[column], str) else None for column in REASON_COLUMNS]
        maturity = [bool(raw[column]) for column in MATURITY_COLUMNS]
        rows.append([identity[0].isoformat(), str(identity[1]), *values, *reasons, *maturity])
    return canonical_sha256(
        {
            "columns": [*VALUE_COLUMNS, *REASON_COLUMNS, *MATURITY_COLUMNS],
            "rows": rows,
            "benchmark_close": [[day.isoformat(), float(benchmark[day])] for day in sorted(benchmark)],
        }
    )


def _external_output_root(path: Path, forbidden_roots: Sequence[Path]) -> Path:
    raw = Path(path)
    if not raw.is_absolute():
        raise _fail(REASON_INPUT, "G2-A bundle output must be absolute", stage="writer")
    parent = raw.parent.resolve(strict=True)
    output = parent / raw.name
    for forbidden in forbidden_roots:
        try:
            output.relative_to(Path(forbidden).resolve(strict=True))
        except ValueError:
            continue
        raise _fail(REASON_INPUT, "G2-A bundle output cannot be inside the repository", stage="writer")
    if output.exists() or output.is_symlink():
        raise _fail(REASON_INPUT, "G2-A bundle output already exists", stage="writer")
    return output


def write_input_bundle(
    bundle: Mapping[str, Any],
    output_root: Path,
    *,
    forbidden_roots: Sequence[Path],
) -> dict[str, Any]:
    panel, calendar, sectors, benchmark = validate_input_bundle(bundle)
    output = _external_output_root(output_root, forbidden_roots)
    logical_sha256 = _logical_input_sha256(panel, benchmark)
    with tempfile.TemporaryDirectory(prefix=f".{output.name}.tmp-", dir=output.parent) as raw_temporary:
        temporary = Path(raw_temporary)
        panel_path = temporary / "panel.h5"
        benchmark_path = temporary / "benchmark.h5"
        storage_panel = panel.copy()
        storage_panel.index = pd.MultiIndex.from_arrays(
            [
                pd.to_datetime(storage_panel.index.get_level_values("trade_date")),
                storage_panel.index.get_level_values("sector_code"),
            ],
            names=["trade_date", "sector_code"],
        )
        for column in REASON_COLUMNS:
            storage_panel[column] = storage_panel[column].fillna("").astype(str)
        storage_panel.to_hdf(
            panel_path,
            key="data",
            mode="w",
            format="table",
            data_columns=["trade_date", "sector_code"],
        )
        pd.DataFrame(
            {"close": [benchmark[day] for day in calendar]},
            index=pd.DatetimeIndex(pd.to_datetime(calendar), name="trade_date"),
        ).to_hdf(benchmark_path, key="data", mode="w", format="table", data_columns=["trade_date"])
        body = {
            "schema_version": INPUT_MANIFEST_SCHEMA_VERSION,
            "input_schema_version": INPUT_SCHEMA_VERSION,
            "identity": dict(bundle["identity"]),
            "files": {
                "panel": {"path": "panel.h5", "sha256": _sha256_file(panel_path)},
                "benchmark": {"path": "benchmark.h5", "sha256": _sha256_file(benchmark_path)},
            },
            "calendar_start": calendar[0].isoformat(),
            "calendar_end": calendar[-1].isoformat(),
            "calendar_count": len(calendar),
            "sector_count": len(sectors),
            "row_count": len(panel),
            "logical_input_sha256": logical_sha256,
        }
        manifest = {**body, "manifest_sha256": canonical_sha256(body)}
        (temporary / "manifest.json").write_bytes(canonical_json_bytes(manifest) + b"\n")
        readback = read_input_bundle(temporary, forbidden_roots=())
        if readback["manifest"]["manifest_sha256"] != manifest["manifest_sha256"]:
            raise _fail(REASON_INPUT, "G2-A bundle readback differs", stage="writer")
        temporary.rename(output)
    return manifest


def read_input_bundle(root: Path, *, forbidden_roots: Sequence[Path]) -> dict[str, Any]:
    raw = Path(root)
    if not raw.is_absolute() or raw.is_symlink():
        raise _fail(REASON_INPUT, "G2-A bundle root is invalid", stage="reader")
    resolved = raw.resolve(strict=True)
    for forbidden in forbidden_roots:
        try:
            resolved.relative_to(Path(forbidden).resolve(strict=True))
        except ValueError:
            continue
        raise _fail(REASON_INPUT, "G2-A bundle root cannot be inside the repository", stage="reader")
    manifest_path = resolved / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _fail(REASON_INPUT, "G2-A bundle manifest cannot be read", stage="reader") from exc
    body = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    if (
        set(manifest)
        != {
            "schema_version",
            "input_schema_version",
            "identity",
            "files",
            "calendar_start",
            "calendar_end",
            "calendar_count",
            "sector_count",
            "row_count",
            "logical_input_sha256",
            "manifest_sha256",
        }
        or manifest.get("schema_version") != INPUT_MANIFEST_SCHEMA_VERSION
        or manifest.get("input_schema_version") != INPUT_SCHEMA_VERSION
        or manifest.get("manifest_sha256") != canonical_sha256(body)
    ):
        raise _fail(REASON_INPUT, "G2-A bundle manifest differs", stage="reader")
    files = manifest.get("files")
    if not isinstance(files, Mapping) or set(files) != {"panel", "benchmark"}:
        raise _fail(REASON_INPUT, "G2-A bundle file manifest differs", stage="reader")
    resolved_files: dict[str, Path] = {}
    for name, expected_name in (("panel", "panel.h5"), ("benchmark", "benchmark.h5")):
        entry = files.get(name)
        if not isinstance(entry, Mapping) or set(entry) != {"path", "sha256"} or entry.get("path") != expected_name:
            raise _fail(REASON_INPUT, "G2-A bundle file entry differs", stage="reader")
        path = (resolved / expected_name).resolve(strict=True)
        if path.parent != resolved or path.is_symlink() or _sha256_file(path) != entry.get("sha256"):
            raise _fail(REASON_INPUT, "G2-A bundle file identity differs", stage="reader")
        resolved_files[name] = path
    try:
        panel = pd.read_hdf(resolved_files["panel"])
        benchmark_frame = pd.read_hdf(resolved_files["benchmark"])
    except Exception as exc:
        raise _fail(REASON_INPUT, "G2-A bundle H5 cannot be read", stage="reader") from exc
    if tuple(benchmark_frame.columns) != ("close",) or benchmark_frame.index.name != "trade_date":
        raise _fail(REASON_INPUT, "G2-A benchmark readback schema differs", stage="reader")
    for column in REASON_COLUMNS:
        if column not in panel:
            raise _fail(REASON_INPUT, "G2-A panel reason readback differs", stage="reader")
        panel[column] = panel[column].replace("", None)
    benchmark = {pd.Timestamp(day).date(): float(value) for day, value in benchmark_frame["close"].items()}
    bundle = {
        "schema_version": INPUT_SCHEMA_VERSION,
        "panel": panel,
        "benchmark_close": benchmark,
        "identity": dict(manifest["identity"]),
    }
    validated_panel, calendar, sectors, validated_benchmark = validate_input_bundle(bundle)
    if (
        manifest.get("calendar_start") != calendar[0].isoformat()
        or manifest.get("calendar_end") != calendar[-1].isoformat()
        or manifest.get("calendar_count") != len(calendar)
        or manifest.get("sector_count") != len(sectors)
        or manifest.get("row_count") != len(validated_panel)
        or manifest.get("logical_input_sha256") != _logical_input_sha256(validated_panel, validated_benchmark)
    ):
        raise _fail(REASON_INPUT, "G2-A bundle logical readback differs", stage="reader")
    return {"bundle": bundle, "manifest": manifest}


def fold_slices(calendar: Sequence[date], *, horizon: int) -> tuple[FoldSlice, ...]:
    if horizon not in HORIZONS:
        raise _fail(REASON_HORIZON, "unsupported G2-A horizon", stage="fold")
    ordered = tuple(calendar)
    if ordered != tuple(sorted(set(ordered))):
        raise _fail(REASON_INPUT, "G2-A calendar is not sorted and unique", stage="fold")
    position = {item: index for index, item in enumerate(ordered)}
    output: list[FoldSlice] = []
    for name, start, end in FOLDS:
        validation = tuple(item for item in ordered if start <= item <= end)
        if not validation or validation[0] != next((item for item in ordered if item >= start), None):
            raise _fail(REASON_HORIZON, f"{name} validation calendar is incomplete", stage="fold")
        first_index = position[validation[0]]
        train_end_index = first_index - horizon - 1
        train_start_index = train_end_index - ROLLING_WINDOW_OPEN_DAYS + 1
        if train_start_index < 0:
            raise _fail(REASON_HORIZON, f"{name} rolling train is incomplete", stage="fold")
        train = ordered[train_start_index : train_end_index + 1]
        purge = ordered[train_end_index + 1 : first_index]
        if len(train) != ROLLING_WINDOW_OPEN_DAYS or len(purge) != horizon:
            raise _fail(REASON_HORIZON, f"{name} purge/train arithmetic differs", stage="fold")
        output.append(FoldSlice(name, train, purge, validation))
    return tuple(output)


def _market_raw_features(benchmark: Mapping[date, float], calendar: Sequence[date]) -> pd.DataFrame:
    ordered = tuple(calendar)
    rows: list[dict[str, Any]] = []
    for index, day in enumerate(ordered):
        daily_return = math.nan
        volatility = math.nan
        if index >= 1:
            daily_return = (
                benchmark[ordered[index - 1]] / benchmark[ordered[index - 2]] - 1.0 if index >= 2 else math.nan
            )
        if index >= 4:
            returns = [
                benchmark[ordered[item]] / benchmark[ordered[item - 1]] - 1.0 for item in range(index - 3, index)
            ]
            volatility = float(np.std(np.asarray(returns, dtype=np.float64), ddof=0))
        rows.append({"trade_date": day, "daily_return": daily_return, "volatility_3d": volatility})
    return pd.DataFrame.from_records(rows).set_index("trade_date")


def _prepared_market_component(
    raw: pd.DataFrame,
    *,
    dates: Sequence[date],
    mean: np.ndarray,
    std: np.ndarray,
    preprocessor: Preprocessor,
) -> PreparedComponent:
    selected = raw.reindex(list(dates))
    values = selected.loc[:, list(MARKET_FEATURES)].to_numpy(dtype=np.float64)
    valid = np.isfinite(values).all(axis=1)
    transformed = np.full(values.shape, np.nan, dtype=np.float64)
    transformed[valid] = (values[valid] - mean) / std
    sequences: list[SequenceData] = []
    segment_dates: list[date] = []
    segment_values: list[np.ndarray] = []
    previous_position: int | None = None
    calendar_position = {day: index for index, day in enumerate(raw.index)}

    def flush() -> None:
        if not segment_dates:
            return
        sequence_index = len(sequences)
        sequences.append(
            SequenceData(
                key=f"000300.SH:{sequence_index}",
                dates=tuple(segment_dates),
                ordinals=tuple(calendar_position[item] for item in segment_dates),
                values=np.vstack(segment_values).astype(np.float64),
            )
        )
        segment_dates.clear()
        segment_values.clear()

    for row_index, day in enumerate(dates):
        position = calendar_position[day]
        if not valid[row_index]:
            flush()
            previous_position = None
            continue
        if previous_position is not None and position != previous_position + 1:
            flush()
        segment_dates.append(day)
        segment_values.append(transformed[row_index])
        previous_position = position
    flush()
    valid_dates = [day for day, accepted in zip(dates, valid, strict=True) if accepted]
    return PreparedComponent(
        component="market",
        level="MARKET",
        feature_names=MARKET_FEATURES,
        expected_sector_count=1,
        minimum_daily_count=1,
        canonical_codes=("000300.SH",),
        sequences=tuple(sequences),
        preprocessor=preprocessor,
        unavailable_items=tuple(
            {"trade_date": day.isoformat(), "reason_code": "hmm_risk_rotation_market_context_unavailable"}
            for day, accepted in zip(dates, valid, strict=True)
            if not accepted
        ),
        valid_row_count=len(valid_dates),
        valid_identity_sha256=canonical_sha256([item.isoformat() for item in valid_dates]),
    )


def fit_market_context(
    benchmark: Mapping[date, float],
    calendar: Sequence[date],
    *,
    train_dates: Sequence[date],
    apply_dates: Sequence[date],
) -> MarketContext:
    """Fit the approved target-free CSI300 MARKET-CONTEXT-A once."""

    raw = _market_raw_features(benchmark, calendar)
    train = raw.reindex(list(train_dates)).loc[:, list(MARKET_FEATURES)]
    values = train.to_numpy(dtype=np.float64)
    if values.shape != (ROLLING_WINDOW_OPEN_DAYS, len(MARKET_FEATURES)) or not np.isfinite(values).all():
        raise _fail(REASON_FEATURE, "market context train features are incomplete", stage="market_context")
    mean = np.asarray([math.fsum(column.tolist()) / len(column) for column in values.T], dtype=np.float64)
    variance = np.asarray(
        [
            math.fsum((float(value) - float(center)) ** 2 for value in column) / len(column)
            for column, center in zip(values.T, mean, strict=True)
        ],
        dtype=np.float64,
    )
    std = np.sqrt(variance)
    if not np.isfinite(std).all() or np.any(std <= 1e-12):
        raise _fail(REASON_FEATURE, "market context train scale is invalid", stage="market_context")
    preprocessor = Preprocessor(
        feature_names=MARKET_FEATURES,
        lower=tuple(float(value) for value in np.min(values, axis=0)),
        upper=tuple(float(value) for value in np.max(values, axis=0)),
        mean=tuple(float(value) for value in mean),
        std=tuple(float(value) for value in std),
        valid_row_count=len(values),
        valid_identity_sha256=canonical_sha256([item.isoformat() for item in train_dates]),
    )
    train_component = _prepared_market_component(raw, dates=train_dates, mean=mean, std=std, preprocessor=preprocessor)
    if len(train_component.sequences) != 1 or train_component.valid_row_count != ROLLING_WINDOW_OPEN_DAYS:
        raise _fail(REASON_FEATURE, "market context train sequence is not contiguous", stage="market_context")
    try:
        fit = fit_jump_model(train_component, state_count=2, jump_penalty=4.0, seed=42)
    except Exception as exc:
        raise _fail(REASON_FIT, "market context jump fit failed", stage="market_context") from exc
    semantic_scores = fit.centers[:, 0] - fit.centers[:, 1]
    if not np.isfinite(semantic_scores).all() or abs(float(semantic_scores[0] - semantic_scores[1])) <= 1e-8:
        raise _fail(REASON_FIT, "market context semantic states are tied", stage="market_context")
    risk_on_state = int(np.argmax(semantic_scores))
    apply_component = _prepared_market_component(raw, dates=apply_dates, mean=mean, std=std, preprocessor=preprocessor)
    try:
        paths = causal_states(apply_component, fit.centers, 4.0)
    except Exception as exc:
        raise _fail(REASON_FIT, "market context causal recursion failed", stage="market_context") from exc
    signs: dict[date, float] = {}
    for sequence, path in zip(apply_component.sequences, paths, strict=True):
        for day, state in zip(sequence.dates, path, strict=True):
            signs[day] = 1.0 if int(state) == risk_on_state else -1.0
    body = {
        "schema_version": "hmm_risk_rotation_l1_market_context_a_v1",
        "feature_names": list(MARKET_FEATURES),
        "as_of_policy": "decision_t_reads_through_t_minus_1",
        "volatility_window": 3,
        "volatility_ddof": 0,
        "zscore_ddof": 0,
        "train_start": train_dates[0].isoformat(),
        "train_end": train_dates[-1].isoformat(),
        "train_count": len(train_dates),
        "train_date_sha256": canonical_sha256([item.isoformat() for item in train_dates]),
        "mean": mean.tolist(),
        "std": std.tolist(),
        "centers": fit.centers.tolist(),
        "centers_sha256": canonical_sha256(fit.centers.tolist()),
        "jump_penalty": 4.0,
        "seed": 42,
        "risk_on_state": risk_on_state,
        "semantic_scores": semantic_scores.tolist(),
        "apply_count": len(apply_dates),
        "available_count": len(signs),
        "available_date_sha256": canonical_sha256([item.isoformat() for item in sorted(signs)]),
        "target_accessed": False,
    }
    return MarketContext(signs=signs, receipt={**body, "receipt_sha256": canonical_sha256(body)})


def _with_market_signs(frame: pd.DataFrame, signs: Mapping[date, float]) -> pd.DataFrame:
    result = frame.copy()
    dates = result.index.get_level_values("trade_date")
    result["market_regime_sign"] = [signs.get(day, math.nan) for day in dates]
    return result


def cross_section_rank_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for feature in CONTINUOUS_FEATURES:
        grouped = result[feature].groupby(level="trade_date", sort=True)
        ranks = grouped.rank(method="average")
        counts = grouped.transform("count")
        result[feature] = ((ranks - 1.0) / (counts - 1.0)) - 0.5
        result.loc[counts <= 1, feature] = np.nan
    return result


def _spearman(left: np.ndarray, right: np.ndarray) -> float:
    if len(left) < MINIMUM_METRIC_SECTORS or len(left) != len(right):
        raise _fail(REASON_COVERAGE, "daily Rank IC denominator is insufficient", stage="metric")
    if not np.isfinite(left).all() or not np.isfinite(right).all():
        raise _fail(REASON_SCORE, "daily Rank IC input is non-finite", stage="metric")
    left_rank = pd.Series(left).rank(method="average").to_numpy(dtype=np.float64)
    right_rank = pd.Series(right).rank(method="average").to_numpy(dtype=np.float64)
    if np.std(left_rank) == 0 or np.std(right_rank) == 0:
        raise _fail(REASON_COVERAGE, "daily Rank IC is undefined", stage="metric")
    value = float(np.corrcoef(left_rank, right_rank)[0, 1])
    if not math.isfinite(value):
        raise _fail(REASON_SCORE, "daily Rank IC is non-finite", stage="metric")
    return value


def newey_west(values: Sequence[float], *, lag: int) -> dict[str, float]:
    array = np.asarray(values, dtype=np.float64)
    if array.ndim != 1 or len(array) <= lag or lag < 0 or not np.isfinite(array).all():
        raise _fail(REASON_HORIZON, "HAC input is not computable", stage="metric")
    mean = float(array.mean())
    centered = array - mean
    lrv = float(np.dot(centered, centered) / len(array))
    for offset in range(1, lag + 1):
        covariance = float(np.dot(centered[offset:], centered[:-offset]) / len(array))
        lrv += 2.0 * (1.0 - offset / (lag + 1.0)) * covariance
    if not math.isfinite(lrv) or lrv <= 0:
        raise _fail(REASON_HORIZON, "HAC long-run variance is not positive", stage="metric")
    standard_error = math.sqrt(lrv / len(array))
    return {
        "mean": mean,
        "long_run_variance": lrv,
        "standard_error": standard_error,
        "t_stat": mean / standard_error,
        "lag": float(lag),
        "count": float(len(array)),
    }


def _eligible_rows(frame: pd.DataFrame, *, ridge: bool) -> pd.Series:
    market = frame["market_regime_sign"].isin((-1.0, 1.0))
    finite_continuous = np.isfinite(frame.loc[:, CONTINUOUS_FEATURES].to_numpy(dtype=np.float64)).sum(axis=1)
    minimum = len(CONTINUOUS_FEATURES) if ridge else MINIMUM_VALID_CONTINUOUS_FEATURES
    return pd.Series(market.to_numpy() & (finite_continuous >= minimum), index=frame.index)


def project_states(predictions: pd.Series) -> tuple[dict[tuple[date, str], str], dict[str, Any]]:
    if not isinstance(predictions.index, pd.MultiIndex) or tuple(predictions.index.names) != (
        "trade_date",
        "sector_code",
    ):
        raise _fail(REASON_SCORE, "state projection score identity differs", stage="state_projection")
    states: dict[tuple[date, str], str] = {}
    daily: list[dict[str, Any]] = []
    for day, group in predictions.groupby(level="trade_date", sort=True):
        valid = [(str(code), float(value)) for (_raw_day, code), value in group.items() if math.isfinite(float(value))]
        valid.sort(key=lambda item: (item[1], item[0]))
        count = len(valid)
        q = max(5, math.ceil(0.20 * count)) if count else 5
        labels = ["neutral"] * count
        for index in range(min(q, count)):
            labels[index] = "fading"
        for index in range(max(0, count - q), count):
            labels[index] = "trending"
        start = 0
        boundary_ties: list[list[str]] = []
        while start < count:
            end = start + 1
            anchor = valid[start][1]
            while end < count and valid[end][1] - anchor <= STATE_TIE_EPSILON:
                end += 1
            if start < q < end or start < count - q < end:
                for index in range(start, end):
                    labels[index] = "neutral"
                boundary_ties.append([valid[index][0] for index in range(start, end)])
            start = end
        for (code, _score), label in zip(valid, labels, strict=True):
            states[(day, code)] = label
        counts = {label: labels.count(label) for label in ("fading", "neutral", "trending")}
        daily.append(
            {
                "trade_date": day.isoformat(),
                "available_count": count,
                "q": q,
                "state_counts": counts,
                "boundary_tie_groups": boundary_ties,
                "spread_available": counts["fading"] >= 5 and counts["trending"] >= 5,
            }
        )
    body = {
        "daily": daily,
        "state_rows_sha256": canonical_sha256(
            [[day.isoformat(), code, state] for (day, code), state in sorted(states.items())]
        ),
    }
    return states, {**body, "receipt_sha256": canonical_sha256(body)}


def _metrics(
    predictions: pd.Series,
    target: pd.Series,
    sectors: Sequence[str],
    states: Mapping[tuple[date, str], str] | None = None,
) -> dict[str, Any]:
    joined = pd.concat({"score": predictions, "target": target}, axis=1)
    daily: list[dict[str, Any]] = []
    available_by_sector = {code: 0 for code in sectors}
    total_by_sector = {code: 0 for code in sectors}
    for day, group in joined.groupby(level="trade_date", sort=True):
        for code in sectors:
            total_by_sector[code] += 1
        valid = group[np.isfinite(group["score"]) & np.isfinite(group["target"])]
        for code in valid.index.get_level_values("sector_code"):
            available_by_sector[str(code)] += 1
        row = {"trade_date": day.isoformat(), "available_sector_count": len(valid), "metric_valid": False}
        if len(valid) >= MINIMUM_METRIC_SECTORS:
            rank_ic = _spearman(valid["score"].to_numpy(dtype=np.float64), valid["target"].to_numpy(dtype=np.float64))
            row.update(
                {
                    "metric_valid": True,
                    "rank_ic": rank_ic,
                }
            )
            if states is not None:
                fading = [
                    float(value)
                    for (_raw_day, code), value in valid["target"].items()
                    if states.get((day, str(code))) == "fading"
                ]
                trending = [
                    float(value)
                    for (_raw_day, code), value in valid["target"].items()
                    if states.get((day, str(code))) == "trending"
                ]
                if len(fading) >= 5 and len(trending) >= 5:
                    row["spread"] = float(np.mean(trending) - np.mean(fading))
                    row["spread_valid"] = True
                else:
                    row["spread"] = None
                    row["spread_valid"] = False
        daily.append(row)
    valid_ic = [float(item["rank_ic"]) for item in daily if item["metric_valid"]]
    daily_ratio = len(valid_ic) / len(daily) if daily else 0.0
    sector_ratios = {code: available_by_sector[code] / total_by_sector[code] for code in sectors}
    spreads = [float(item["spread"]) for item in daily if item.get("spread_valid") is True]
    mean_ic = float(np.mean(valid_ic)) if valid_ic else None
    mean_spread = float(np.mean(spreads)) if spreads else None
    return {
        "daily": daily,
        "daily_metric_coverage": daily_ratio,
        "minimum_sector_coverage": min(sector_ratios.values()) if sector_ratios else 0.0,
        "sector_coverage": sector_ratios,
        "mean_rank_ic": mean_ic,
        "mean_spread": mean_spread,
        "metric_direction_divergence_observed": bool(
            mean_ic is not None and mean_spread is not None and mean_ic * mean_spread < 0
        ),
        "rank_ic_sha256": canonical_sha256(valid_ic),
        "coverage_accepted": daily_ratio >= MINIMUM_DEVELOPMENT_COVERAGE
        and all(value >= MINIMUM_DEVELOPMENT_COVERAGE for value in sector_ratios.values()),
    }


def require_formal_runtime() -> dict[str, Any]:
    if any(os.environ.get(name) != "1" for name in SINGLE_THREAD_ENVIRONMENT):
        raise _fail(REASON_FIT, "formal runtime is not configured for one thread", stage="environment")
    try:
        from threadpoolctl import threadpool_info
    except ImportError as exc:
        raise _fail(REASON_FIT, "threadpoolctl is unavailable", stage="environment") from exc
    pools = threadpool_info()
    if any(int(item.get("num_threads", 0)) != 1 for item in pools):
        raise _fail(REASON_FIT, "effective native threadpool count is not one", stage="environment", pools=pools)
    identity = runtime_identity()
    if identity["packages"].get("lightgbm") != "4.6.0":
        raise _fail(REASON_FIT, "LightGBM version differs from approved contract", stage="environment")
    return {
        **identity,
        "thread_environment": {name: os.environ[name] for name in SINGLE_THREAD_ENVIRONMENT},
        "pools": pools,
    }


def _run_ridge_battery_impl(
    bundle: Mapping[str, Any],
    *,
    producer_commit: str,
    progress: FitProgress,
    runtime_validator: Any = require_formal_runtime,
) -> dict[str, Any]:
    if len(producer_commit) != 40 or any(character not in "0123456789abcdef" for character in producer_commit):
        raise _fail(REASON_INPUT, "G2-A producer commit is invalid", stage="battery")
    runtime = runtime_validator()
    frame, calendar, sectors, benchmark = validate_input_bundle(bundle)
    ranked = cross_section_rank_features(frame)
    calendar_position = {day: index for index, day in enumerate(calendar)}
    market_contexts: dict[str, MarketContext] = {}
    for name, validation_start, _validation_end in FOLDS:
        first_day = next((day for day in calendar if day >= validation_start), None)
        if first_day is None:
            raise _fail(REASON_HORIZON, f"{name} market validation start is missing", stage="battery")
        first_index = calendar_position[first_day]
        train_dates = calendar[first_index - ROLLING_WINDOW_OPEN_DAYS : first_index]
        fold_index = len(market_contexts)
        apply_dates = tuple(
            sorted(
                set(fold_slices(calendar, horizon=5)[fold_index].train_dates)
                | set(fold_slices(calendar, horizon=10)[fold_index].train_dates)
                | set(train_dates)
                | {day for day in calendar if validation_start <= day <= _validation_end}
            )
        )
        market_contexts[name] = progress.execute(
            f"battery:{name}:market_context",
            lambda train_dates=train_dates, apply_dates=apply_dates: fit_market_context(
                benchmark, calendar, train_dates=train_dates, apply_dates=apply_dates
            ),
        )
    horizons: dict[str, Any] = {}
    for horizon in HORIZONS:
        predictions: list[pd.Series] = []
        fold_receipts: list[dict[str, Any]] = []
        for fold in fold_slices(calendar, horizon=horizon):
            context = market_contexts[fold.name]
            train = _with_market_signs(ranked.loc[(list(fold.train_dates), slice(None)), :], context.signs)
            validation = _with_market_signs(ranked.loc[(list(fold.validation_dates), slice(None)), :], context.signs)
            train_mask = _eligible_rows(train, ridge=True) & np.isfinite(train[f"target_{horizon}d"])
            validation_mask = _eligible_rows(validation, ridge=True)
            if not train_mask.any() or not validation_mask.any():
                raise _fail(REASON_HORIZON, "Ridge complete-case fold is empty", stage="battery", fold=fold.name)
            estimator = Ridge(alpha=100.0, fit_intercept=True, solver="svd", tol=1e-4, max_iter=None, positive=False)
            progress.execute(
                f"battery:{fold.name}:ridge_{horizon}d",
                lambda: estimator.fit(train.loc[train_mask, FEATURES], train.loc[train_mask, f"target_{horizon}d"]),
            )
            scores = pd.Series(np.nan, index=validation.index, dtype=np.float64)
            scores.loc[validation_mask] = estimator.predict(validation.loc[validation_mask, FEATURES])
            predictions.append(scores)
            fold_receipts.append(
                {
                    **fold.receipt(),
                    "fit_row_count": int(train_mask.sum()),
                    "prediction_row_count": int(validation_mask.sum()),
                    "coefficient_sha256": canonical_sha256(estimator.coef_.astype(float).tolist()),
                    "intercept": float(estimator.intercept_),
                    "market_context_receipt_sha256": context.receipt["receipt_sha256"],
                }
            )
        all_predictions = pd.concat(predictions).sort_index()
        target = ranked.loc[all_predictions.index, f"target_{horizon}d"]
        metrics = _metrics(all_predictions, target, sectors)
        if not metrics["coverage_accepted"]:
            raise _fail(REASON_COVERAGE, "Ridge comparator coverage failed", stage="battery", horizon=horizon)
        rank_ic_values = [float(item["rank_ic"]) for item in metrics["daily"] if item["metric_valid"]]
        tail_count = int(bundle["identity"]["tail_mature_decision_counts"][str(horizon)])
        try:
            power_hac = newey_west(rank_ic_values, lag=horizon - 1)
            if tail_count <= 0:
                raise _fail(
                    REASON_HORIZON,
                    "tail maturity count is insufficient for power projection",
                    stage="battery_power",
                )
            tail_standard_error = math.sqrt(float(power_hac["long_run_variance"]) / tail_count)
            mde = (1.6448536269514722 + 0.8416212335729143) * tail_standard_error
            forward_power = {
                "lrv_source": "ridge_development_daily_rank_ic",
                "hac_lag": horizon - 1,
                "long_run_variance": power_hac["long_run_variance"],
                "tail_mature_decision_count": tail_count,
                "tail_standard_error": tail_standard_error,
                "minimum_detectable_effect": mde,
                "binding_mbe_rank_ic": BINDING_MBE_IC,
                "status": "INSUFFICIENT" if BINDING_MBE_IC < mde else "SUFFICIENT",
                "reason_code": None,
                "tail_outcome_accessed": False,
            }
        except RotationL1G2AError as exc:
            forward_power = {
                "lrv_source": "ridge_development_daily_rank_ic",
                "hac_lag": horizon - 1,
                "long_run_variance": None,
                "tail_mature_decision_count": tail_count,
                "tail_standard_error": None,
                "minimum_detectable_effect": None,
                "binding_mbe_rank_ic": BINDING_MBE_IC,
                "status": "UNAVAILABLE",
                "reason_code": exc.reason_code,
                "tail_outcome_accessed": False,
            }
        horizons[str(horizon)] = {
            "folds": fold_receipts,
            "metrics": metrics,
            "forward_power": forward_power,
        }
    five = {item["trade_date"]: item for item in horizons["5"]["metrics"]["daily"] if item["metric_valid"]}
    ten = {item["trade_date"]: item for item in horizons["10"]["metrics"]["daily"] if item["metric_valid"]}
    common = tuple(sorted(set(five) & set(ten)))
    if not common:
        raise _fail(REASON_HORIZON, "Ridge horizons lack common metric dates", stage="horizon_selection")
    differences = [float(five[day]["rank_ic"]) - float(ten[day]["rank_ic"]) for day in common]
    hac = newey_west(differences, lag=9)
    selected = 5 if hac["mean"] > 0.005 and hac["t_stat"] >= 1.645 else 10
    body = {
        "schema_version": "hmm_risk_rotation_l1_g2a_battery_v1",
        "contract_version": CONTRACT_VERSION,
        "runtime_identity": runtime,
        "producer_commit": producer_commit,
        "input_identity": dict(bundle["identity"]),
        "fit_count": 15,
        "ridge_fit_count": 10,
        "market_fit_count": 5,
        "fit_progress": progress.receipt(),
        "market_context_receipts": [market_contexts[name].receipt for name, _start, _end in FOLDS],
        "horizons": horizons,
        "selection": {
            "selected_horizon": selected,
            "model_class": "RIDGE_COMPARATOR",
            "gbdt_horizon_optimality_not_claimed": True,
            "paired_date_count": len(common),
            "paired_difference_hac": hac,
        },
        "tail_accessed": False,
        "model_write_performed": False,
        "database_write_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def run_ridge_battery(
    bundle: Mapping[str, Any],
    *,
    producer_commit: str,
    runtime_validator: Any = require_formal_runtime,
) -> dict[str, Any]:
    progress = FitProgress(planned=15)
    try:
        return _run_ridge_battery_impl(
            bundle,
            producer_commit=producer_commit,
            progress=progress,
            runtime_validator=runtime_validator,
        )
    except Exception as exc:
        raise _fit_progress_error(exc, progress) from exc


def validate_battery_report(report: Mapping[str, Any], *, expected_identity: Mapping[str, Any]) -> None:
    body = {key: value for key, value in report.items() if key != "receipt_sha256"}
    if (
        report.get("schema_version") != "hmm_risk_rotation_l1_g2a_battery_v1"
        or report.get("contract_version") != CONTRACT_VERSION
        or report.get("receipt_sha256") != canonical_sha256(body)
        or report.get("input_identity") != dict(expected_identity)
        or report.get("fit_count") != 15
        or report.get("ridge_fit_count") != 10
        or report.get("market_fit_count") != 5
        or report.get("fit_progress")
        != {"planned": 15, "started": 15, "completed": 15, "failed": 0, "active_fit": None}
        or not isinstance(report.get("producer_commit"), str)
        or len(report["producer_commit"]) != 40
        or any(character not in "0123456789abcdef" for character in report["producer_commit"])
        or report.get("tail_accessed") is not False
        or report.get("model_write_performed") is not False
        or report.get("database_write_performed") is not False
    ):
        raise _fail(REASON_INPUT, "G2-A battery receipt differs", stage="battery_readback")
    horizons = report.get("horizons")
    selection = report.get("selection")
    if (
        not isinstance(horizons, Mapping)
        or set(horizons) != {"5", "10"}
        or not isinstance(selection, Mapping)
        or selection.get("selected_horizon") not in HORIZONS
        or selection.get("model_class") != "RIDGE_COMPARATOR"
        or selection.get("gbdt_horizon_optimality_not_claimed") is not True
    ):
        raise _fail(REASON_HORIZON, "G2-A battery selection differs", stage="battery_readback")
    for horizon in HORIZONS:
        horizon_report = horizons[str(horizon)]
        power = horizon_report.get("forward_power") if isinstance(horizon_report, Mapping) else None
        if (
            not isinstance(power, Mapping)
            or power.get("status") not in {"INSUFFICIENT", "SUFFICIENT", "UNAVAILABLE"}
            or power.get("tail_outcome_accessed") is not False
            or power.get("tail_mature_decision_count") != expected_identity["tail_mature_decision_counts"][str(horizon)]
        ):
            raise _fail(REASON_HORIZON, "G2-A battery power receipt differs", stage="battery_readback")
    receipts = report.get("market_context_receipts")
    if not isinstance(receipts, list) or len(receipts) != 5:
        raise _fail(REASON_HORIZON, "G2-A battery market receipt count differs", stage="battery_readback")
    for receipt in receipts:
        if not isinstance(receipt, Mapping):
            raise _fail(REASON_HORIZON, "G2-A battery market receipt differs", stage="battery_readback")
        receipt_body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
        if (
            receipt.get("schema_version") != "hmm_risk_rotation_l1_market_context_a_v1"
            or receipt.get("receipt_sha256") != canonical_sha256(receipt_body)
            or receipt.get("target_accessed") is not False
        ):
            raise _fail(REASON_HORIZON, "G2-A battery market receipt differs", stage="battery_readback")


def _lightgbm_profile() -> dict[str, Any]:
    return {
        "boosting_type": "gbdt",
        "objective": "regression_l1",
        "class_weight": None,
        "max_depth": 3,
        "num_leaves": 7,
        "learning_rate": 0.03,
        "n_estimators": 240,
        "subsample_for_bin": 200000,
        "min_child_samples": 310,
        "min_child_weight": 0.001,
        "min_split_gain": 0.0,
        "reg_alpha": 1.0,
        "reg_lambda": 10.0,
        "subsample": 1.0,
        "subsample_freq": 0,
        "colsample_bytree": 1.0,
        "max_bin": 63,
        "min_data_in_bin": 3,
        "feature_pre_filter": False,
        "use_missing": True,
        "zero_as_missing": False,
        "extra_trees": False,
        "path_smooth": 0.0,
        "max_delta_step": 0.0,
        "random_state": 42,
        "n_jobs": 1,
        "deterministic": True,
        "force_col_wise": True,
        "importance_type": "split",
        "verbosity": -1,
    }


def _leaf_date_coverage(
    estimator: Any,
    features: pd.DataFrame,
    dates: Sequence[date],
    *,
    fit_identity: str,
) -> dict[str, Any]:
    leaves = np.asarray(estimator.predict(features, pred_leaf=True))
    if leaves.ndim == 1:
        leaves = leaves.reshape(-1, 1)
    observations: list[tuple[int, int, int]] = []
    date_array = np.asarray(dates, dtype=object)
    for tree_index in range(leaves.shape[1]):
        for leaf_id in sorted(set(int(value) for value in leaves[:, tree_index])):
            distinct = len(set(date_array[leaves[:, tree_index] == leaf_id]))
            observations.append((tree_index, leaf_id, distinct))
    counts = np.asarray([item[2] for item in observations], dtype=np.float64)
    violating = [[tree, leaf, count] for tree, leaf, count in observations if count < MINIMUM_LEAF_DISTINCT_DATES]
    body = {
        "minimum": int(counts.min()) if len(counts) else 0,
        "p05": float(np.quantile(counts, 0.05)) if len(counts) else 0.0,
        "median": float(np.median(counts)) if len(counts) else 0.0,
        "leaf_count": len(observations),
        "violating_leaf_ids": violating,
        "distribution_sha256": canonical_sha256(observations),
    }
    if violating:
        raise _fail(
            REASON_LEAF,
            "GBDT leaf distinct-date coverage failed",
            stage="leaf_date_coverage",
            fit_identity=fit_identity,
            summary=body,
        )
    return body


def _contribution_receipt(
    estimator: Any, features: pd.DataFrame, scores: np.ndarray
) -> tuple[dict[str, Any], np.ndarray]:
    contributions = np.asarray(estimator.predict(features, pred_contrib=True), dtype=np.float64)
    if contributions.shape != (len(features), len(FEATURES) + 1) or not np.isfinite(contributions).all():
        raise _fail(REASON_SCORE, "GBDT feature contributions are invalid", stage="prediction")
    reconstructed = contributions[:, :-1].sum(axis=1) + contributions[:, -1]
    tolerance = 1e-12 + 1e-10 * np.maximum(1.0, np.abs(scores))
    if np.any(np.abs(reconstructed - scores) > tolerance):
        raise _fail(REASON_SCORE, "GBDT feature contributions do not reconstruct score", stage="prediction")
    receipt = {
        "shape": list(contributions.shape),
        "canonical_sha256": canonical_sha256(contributions.tolist()),
        "maximum_reconstruction_error": float(np.max(np.abs(reconstructed - scores), initial=0.0)),
    }
    return receipt, contributions


def _run_gbdt_process_impl(
    bundle: Mapping[str, Any],
    *,
    battery_report: Mapping[str, Any],
    process_index: int,
    progress: FitProgress,
    estimator_factory: Any | None = None,
    runtime_validator: Any = require_formal_runtime,
) -> dict[str, Any]:
    validate_battery_report(battery_report, expected_identity=bundle.get("identity", {}))
    selection = battery_report["selection"]
    selected_horizon = int(selection["selected_horizon"])
    forward_power_status = str(battery_report["horizons"][str(selected_horizon)]["forward_power"]["status"])
    if process_index not in (1, 2) or selected_horizon not in HORIZONS:
        raise _fail(REASON_INPUT, "GBDT process identity differs", stage="input")
    frame, calendar, sectors, benchmark = validate_input_bundle(bundle)
    ranked = cross_section_rank_features(frame)
    runtime = runtime_validator()
    if estimator_factory is None:
        try:
            from lightgbm import LGBMRegressor
        except (ImportError, OSError) as exc:
            raise _fail(REASON_FIT, "lightgbm==4.6.0 is unavailable", stage="environment") from exc
        if importlib.metadata.version("lightgbm") != "4.6.0":
            raise _fail(REASON_FIT, "LightGBM version differs from approved contract", stage="environment")
        estimator_factory = LGBMRegressor
    predictions: list[pd.Series] = []
    fold_receipts: list[dict[str, Any]] = []
    market_receipts: list[Mapping[str, Any]] = []
    contributions_by_identity: dict[tuple[date, str], list[float]] = {}
    validation_market_signs: dict[date, float] = {}
    profile = _lightgbm_profile()
    for fold in fold_slices(calendar, horizon=selected_horizon):
        first_index = calendar.index(fold.validation_dates[0])
        market_train_dates = calendar[first_index - ROLLING_WINDOW_OPEN_DAYS : first_index]
        market_apply_dates = tuple(sorted(set(fold.train_dates) | set(market_train_dates) | set(fold.validation_dates)))
        context = progress.execute(
            f"process-{process_index}:{fold.name}:market_context",
            lambda market_train_dates=market_train_dates, market_apply_dates=market_apply_dates: fit_market_context(
                benchmark,
                calendar,
                train_dates=market_train_dates,
                apply_dates=market_apply_dates,
            ),
        )
        market_receipts.append(context.receipt)
        for day in fold.validation_dates:
            if day in context.signs:
                validation_market_signs[day] = context.signs[day]
        train = _with_market_signs(ranked.loc[(list(fold.train_dates), slice(None)), :], context.signs)
        validation = _with_market_signs(ranked.loc[(list(fold.validation_dates), slice(None)), :], context.signs)
        train_mask = _eligible_rows(train, ridge=False) & np.isfinite(train[f"target_{selected_horizon}d"])
        validation_mask = _eligible_rows(validation, ridge=False)
        if not train_mask.any() or not validation_mask.any():
            raise _fail(REASON_FIT, "GBDT eligible fold is empty", stage="fit", fold=fold.name)
        estimator = estimator_factory(**profile)
        try:
            progress.execute(
                f"process-{process_index}:{fold.name}:gbdt",
                lambda: estimator.fit(
                    train.loc[train_mask, FEATURES],
                    train.loc[train_mask, f"target_{selected_horizon}d"],
                ),
            )
        except Exception as exc:
            raise _fail(REASON_FIT, "GBDT fit failed", stage="fit", fold=fold.name) from exc
        try:
            raw_scores = np.asarray(estimator.predict(validation.loc[validation_mask, FEATURES]), dtype=np.float64)
        except Exception as exc:
            raise _fail(REASON_SCORE, "GBDT prediction failed", stage="prediction", fold=fold.name) from exc
        if raw_scores.shape != (int(validation_mask.sum()),) or not np.isfinite(raw_scores).all():
            raise _fail(REASON_SCORE, "GBDT score is non-finite or mis-shaped", stage="prediction", fold=fold.name)
        train_dates = train.loc[train_mask].index.get_level_values("trade_date")
        leaf = _leaf_date_coverage(
            estimator,
            train.loc[train_mask, FEATURES],
            train_dates,
            fit_identity=f"process-{process_index}:{fold.name}:gbdt",
        )
        contribution, contribution_values = _contribution_receipt(
            estimator, validation.loc[validation_mask, FEATURES], raw_scores
        )
        for identity, values in zip(
            validation.loc[validation_mask].index,
            contribution_values,
            strict=True,
        ):
            key = (identity[0], str(identity[1]))
            if key in contributions_by_identity:
                raise _fail(REASON_SCORE, "GBDT OOF contribution identity is duplicated", stage="prediction")
            contributions_by_identity[key] = [float(value) for value in values]
        scores = pd.Series(np.nan, index=validation.index, dtype=np.float64)
        scores.loc[validation_mask] = raw_scores
        predictions.append(scores)
        fold_receipts.append(
            {
                **fold.receipt(),
                "fit_row_count": int(train_mask.sum()),
                "prediction_row_count": int(validation_mask.sum()),
                "leaf_date_coverage": leaf,
                "feature_contributions": contribution,
                "model_sha256": canonical_sha256(estimator.booster_.model_to_string()),
                "market_context_receipt_sha256": context.receipt["receipt_sha256"],
            }
        )
    all_predictions = pd.concat(predictions).sort_index()
    states, state_receipt = project_states(all_predictions)
    metrics = _metrics(
        all_predictions,
        ranked.loc[all_predictions.index, f"target_{selected_horizon}d"],
        sectors,
        states,
    )
    if not metrics["coverage_accepted"]:
        raise _fail(REASON_COVERAGE, "GBDT development coverage failed", stage="research_product_gate")
    metric_rows = [item for item in metrics["daily"] if item["metric_valid"]]
    metric_hac = newey_west([float(item["rank_ic"]) for item in metric_rows], lag=selected_horizon - 1)
    monthly_groups: dict[str, list[float]] = {}
    for item in metric_rows:
        month = str(item["trade_date"])[:7]
        monthly_groups.setdefault(month, []).append(float(item["rank_ic"]))
    monthly = [
        {"month": month, "mean_rank_ic": float(np.mean(values)), "metric_date_count": len(values)}
        for month, values in sorted(monthly_groups.items())
    ]
    development_summary = {
        "mean_rank_ic": metrics["mean_rank_ic"],
        "hac_lag": selected_horizon - 1,
        "hac_standard_error": metric_hac["standard_error"],
        "hac_lower_two_sided_95pct": float(metric_hac["mean"] - 1.959963984540054 * metric_hac["standard_error"]),
        "hac_upper_two_sided_95pct": float(metric_hac["mean"] + 1.959963984540054 * metric_hac["standard_error"]),
        "monthly": monthly,
        "positive_month_ratio": (
            sum(float(item["mean_rank_ic"]) > 0 for item in monthly) / len(monthly) if monthly else None
        ),
        "worst_month": min(monthly, key=lambda item: float(item["mean_rank_ic"])) if monthly else None,
        "promotion_gate_applied": False,
    }
    full_market_train = calendar[-ROLLING_WINDOW_OPEN_DAYS:]
    full_train_end = len(calendar) - selected_horizon
    full_train_dates = calendar[full_train_end - ROLLING_WINDOW_OPEN_DAYS : full_train_end]
    if len(full_market_train) != ROLLING_WINDOW_OPEN_DAYS or len(full_train_dates) != ROLLING_WINDOW_OPEN_DAYS:
        raise _fail(REASON_FIT, "GBDT final rolling train is incomplete", stage="final_fit")
    final_apply_dates = tuple(sorted(set(full_train_dates) | set(full_market_train)))
    final_context = progress.execute(
        f"process-{process_index}:full-development:market_context",
        lambda: fit_market_context(
            benchmark,
            calendar,
            train_dates=full_market_train,
            apply_dates=final_apply_dates,
        ),
    )
    market_receipts.append(final_context.receipt)
    final_train = _with_market_signs(ranked.loc[(list(full_train_dates), slice(None)), :], final_context.signs)
    final_mask = _eligible_rows(final_train, ridge=False) & np.isfinite(final_train[f"target_{selected_horizon}d"])
    if not final_mask.any():
        raise _fail(REASON_FIT, "GBDT final eligible train is empty", stage="final_fit")
    final_estimator = estimator_factory(**profile)
    try:
        progress.execute(
            f"process-{process_index}:full-development:gbdt",
            lambda: final_estimator.fit(
                final_train.loc[final_mask, FEATURES],
                final_train.loc[final_mask, f"target_{selected_horizon}d"],
            ),
        )
    except Exception as exc:
        raise _fail(REASON_FIT, "GBDT final fit failed", stage="final_fit") from exc
    final_leaf = _leaf_date_coverage(
        final_estimator,
        final_train.loc[final_mask, FEATURES],
        final_train.loc[final_mask].index.get_level_values("trade_date"),
        fit_identity=f"process-{process_index}:full-development:gbdt",
    )
    final_model_text = final_estimator.booster_.model_to_string()
    if not isinstance(final_model_text, str) or not final_model_text:
        raise _fail(REASON_FIT, "GBDT final model serialization failed", stage="final_fit")
    final_model = {
        "train_start": full_train_dates[0].isoformat(),
        "train_end": full_train_dates[-1].isoformat(),
        "train_count": len(full_train_dates),
        "fit_row_count": int(final_mask.sum()),
        "leaf_date_coverage": final_leaf,
        "model_sha256": canonical_sha256(final_model_text),
        "market_context": final_context.receipt,
    }
    oof_rows: list[dict[str, Any]] = []
    for (day, raw_code), raw_score in all_predictions.items():
        code = str(raw_code)
        key = (day, code)
        score = float(raw_score)
        if math.isfinite(score):
            contribution_values = contributions_by_identity.get(key)
            state = states.get(key)
            if contribution_values is None or state not in {"fading", "neutral", "trending"}:
                raise _fail(REASON_SCORE, "GBDT available OOF row lacks state or contribution", stage="prediction")
            oof_rows.append(
                {
                    "trade_date": day.isoformat(),
                    "sector_code": code,
                    "availability": "available",
                    "reason_code": None,
                    "rotation_score": score,
                    "forecast_state": state,
                    "feature_contributions": contribution_values,
                }
            )
        else:
            reason = (
                "hmm_risk_rotation_market_context_unavailable" if day not in validation_market_signs else REASON_FEATURE
            )
            oof_rows.append(
                {
                    "trade_date": day.isoformat(),
                    "sector_code": code,
                    "availability": "unavailable",
                    "reason_code": reason,
                    "rotation_score": None,
                    "forecast_state": None,
                    "feature_contributions": None,
                }
            )
    payload = {
        "contract_version": CONTRACT_VERSION,
        "selected_horizon": selected_horizon,
        "profile": profile,
        "runtime_identity": runtime,
        "producer_commit": battery_report["producer_commit"],
        "battery_receipt_sha256": battery_report["receipt_sha256"],
        "forward_power_status": forward_power_status,
        "input_identity": dict(bundle["identity"]),
        "folds": fold_receipts,
        "market_context_receipts": market_receipts,
        "metrics": metrics,
        "development_summary": development_summary,
        "state_projection": state_receipt,
        "oof_prediction_rows": oof_rows,
        "oof_prediction_rows_sha256": canonical_sha256(oof_rows),
        "prediction_sha256": canonical_sha256(
            [
                [day.isoformat(), code, None if not math.isfinite(value) else float(value)]
                for (day, code), value in all_predictions.items()
            ]
        ),
        "research_product_gate": {"passed": True, "effect_threshold_applied": False},
        "tail_access_gate": {
            "passed": float(metrics["mean_rank_ic"]) >= BINDING_MBE_IC,
            "binding_mbe_rank_ic": BINDING_MBE_IC,
        },
        "final_model": final_model,
        "gbdt_fit_count": 6,
        "market_fit_count": 6,
        "fit_count": 12,
        "fit_progress": progress.receipt(),
        "tail_accessed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    body = {
        "schema_version": PROCESS_SCHEMA_VERSION,
        "process_index": process_index,
        "reproducibility_payload": payload,
        "reproducibility_payload_sha256": canonical_sha256(payload),
        "final_model_text": final_model_text,
    }
    return {**body, "report_sha256": canonical_sha256(body)}


def run_gbdt_process(
    bundle: Mapping[str, Any],
    *,
    battery_report: Mapping[str, Any],
    process_index: int,
    estimator_factory: Any | None = None,
    runtime_validator: Any = require_formal_runtime,
) -> dict[str, Any]:
    progress = FitProgress(planned=12)
    try:
        return _run_gbdt_process_impl(
            bundle,
            battery_report=battery_report,
            process_index=process_index,
            progress=progress,
            estimator_factory=estimator_factory,
            runtime_validator=runtime_validator,
        )
    except Exception as exc:
        raise _fit_progress_error(exc, progress) from exc


def _validated_process(child: Mapping[str, Any], *, expected_index: int) -> Mapping[str, Any]:
    expected_keys = {
        "schema_version",
        "process_index",
        "reproducibility_payload",
        "reproducibility_payload_sha256",
        "final_model_text",
        "report_sha256",
    }
    body = {key: value for key, value in child.items() if key != "report_sha256"}
    payload = child.get("reproducibility_payload")
    model_text = child.get("final_model_text")
    if (
        set(child) != expected_keys
        or child.get("schema_version") != PROCESS_SCHEMA_VERSION
        or child.get("process_index") != expected_index
        or not isinstance(payload, Mapping)
        or child.get("reproducibility_payload_sha256") != canonical_sha256(payload)
        or child.get("report_sha256") != canonical_sha256(body)
        or not isinstance(model_text, str)
        or not model_text
        or payload.get("fit_count") != 12
        or payload.get("gbdt_fit_count") != 6
        or payload.get("market_fit_count") != 6
        or payload.get("fit_progress")
        != {"planned": 12, "started": 12, "completed": 12, "failed": 0, "active_fit": None}
        or payload.get("tail_accessed") is not False
        or payload.get("database_write_performed") is not False
        or payload.get("runtime_action_performed") is not False
        or payload.get("selected_horizon") not in HORIZONS
        or payload.get("forward_power_status") not in {"INSUFFICIENT", "SUFFICIENT", "UNAVAILABLE"}
        or not isinstance(payload.get("producer_commit"), str)
        or len(payload["producer_commit"]) != 40
        or any(character not in "0123456789abcdef" for character in payload["producer_commit"])
        or not isinstance(payload.get("battery_receipt_sha256"), str)
        or len(payload["battery_receipt_sha256"]) != 64
        or any(character not in "0123456789abcdef" for character in payload["battery_receipt_sha256"])
        or payload.get("research_product_gate") != {"passed": True, "effect_threshold_applied": False}
        or not isinstance(payload.get("tail_access_gate"), Mapping)
        or not isinstance(payload["tail_access_gate"].get("passed"), bool)
        or payload["tail_access_gate"].get("binding_mbe_rank_ic") != BINDING_MBE_IC
        or not isinstance(payload.get("final_model"), Mapping)
        or payload["final_model"].get("model_sha256") != canonical_sha256(model_text)
    ):
        raise _fail(REASON_REPRODUCIBILITY, "GBDT child envelope or receipt differs", stage="closure")
    return payload


def close_processes(first: Mapping[str, Any], second: Mapping[str, Any]) -> dict[str, Any]:
    first_payload = _validated_process(first, expected_index=1)
    second_payload = _validated_process(second, expected_index=2)
    if first.get("reproducibility_payload_sha256") != second.get("reproducibility_payload_sha256"):
        raise _fail(REASON_REPRODUCIBILITY, "GBDT fresh-process payloads differ", stage="closure")
    if first.get("final_model_text") != second.get("final_model_text") or first_payload != second_payload:
        raise _fail(REASON_REPRODUCIBILITY, "GBDT fresh-process model or payload differs", stage="closure")
    payload = first_payload
    mean_ic = float(payload["metrics"]["mean_rank_ic"])
    tail_allowed = bool(payload["tail_access_gate"]["passed"])
    forward_power_status = str(payload["forward_power_status"])
    body = {
        "schema_version": ACCEPTANCE_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "status": "development_complete",
        "selected_horizon": payload["selected_horizon"],
        "research_surface_status": "AVAILABLE_EXPERIMENTAL",
        "rotation_l1_capability_status": (
            "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED" if tail_allowed else "NOT_AVAILABLE"
        ),
        "forward_power_status": forward_power_status,
        "forward_confirmation": (
            "PENDING_INSUFFICIENT_POWER"
            if tail_allowed and forward_power_status == "INSUFFICIENT"
            else "PENDING_INCONCLUSIVE"
            if tail_allowed
            else "NOT_STARTED"
        ),
        "development_oof_mean_rank_ic": mean_ic,
        "tail_access_gate_passed": tail_allowed,
        "tail_accessed": False,
        "child_sha256s": [first["report_sha256"], second["report_sha256"]],
        "reproducibility_payload_sha256": first["reproducibility_payload_sha256"],
        "battery_receipt_sha256": payload["battery_receipt_sha256"],
        "producer_commit": payload["producer_commit"],
        "model_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "acceptance_sha256": canonical_sha256(body)}


def runtime_identity() -> dict[str, Any]:
    packages = {}
    for name in ("lightgbm", "numpy", "scipy", "scikit-learn"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    return {"python": platform.python_version(), "platform": platform.platform(), "packages": packages}


__all__ = [
    "ACCEPTANCE_SCHEMA_VERSION",
    "BINDING_MBE_IC",
    "CONTINUOUS_FEATURES",
    "FEATURES",
    "FOLDS",
    "HORIZONS",
    "INPUT_SCHEMA_VERSION",
    "PROCESS_SCHEMA_VERSION",
    "RotationL1G2AError",
    "build_materialised_panel",
    "close_processes",
    "cross_section_rank_features",
    "fold_slices",
    "fit_market_context",
    "newey_west",
    "project_states",
    "read_input_bundle",
    "require_formal_runtime",
    "run_gbdt_process",
    "run_ridge_battery",
    "runtime_identity",
    "validate_battery_report",
    "validate_input_bundle",
    "write_input_bundle",
]
