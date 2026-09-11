"""Research-only orchestration for complete candidate evaluation views."""
from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import pandas as pd

from .models import ResearchError, json_object
from .runner import load_values


_FACTOR_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_]{2,80}")


def validate_full_evaluation_spec(
    value: Any,
    *,
    candidate_names: set[str],
    repo_root: Path,
) -> dict[str, Any]:
    spec = json_object(value)
    allowed = {
        "reference_value_artifacts",
        "correlation_batch_size",
        "correlation_half_life",
        "correlation_min_stocks",
        "correlation_min_effective_days",
    }
    if set(spec) != allowed:
        raise ResearchError(
            "invalid_full_evaluation",
            f"full_evaluation requires exactly {sorted(allowed)}",
        )
    artifacts = spec["reference_value_artifacts"]
    if not isinstance(artifacts, dict) or not artifacts:
        raise ResearchError(
            "invalid_full_evaluation",
            "reference_value_artifacts must be a non-empty factor-to-file mapping",
        )
    normalized: dict[str, str] = {}
    for name, raw_path in artifacts.items():
        if not isinstance(name, str) or not _FACTOR_NAME.fullmatch(name):
            raise ResearchError("invalid_full_evaluation", "Invalid reference factor name")
        if not isinstance(raw_path, str):
            raise ResearchError("invalid_full_evaluation", f"Reference path missing for {name}")
        path = Path(raw_path).expanduser().resolve()
        if path.is_symlink() or not path.is_file():
            raise ResearchError(
                "full_evaluation_reference_missing",
                f"Reference artifact is not a regular file: {name}",
            )
        if path.is_relative_to(repo_root.resolve()):
            raise ResearchError(
                "invalid_full_evaluation",
                "Reference values must be stable repo-external artifacts",
            )
        normalized[name] = str(path)
    if not set(normalized) - candidate_names and len(candidate_names) == 1:
        raise ResearchError(
            "invalid_full_evaluation",
            "Complete evaluation requires at least one non-self reference factor",
        )
    for field in (
        "correlation_batch_size",
        "correlation_min_stocks",
        "correlation_min_effective_days",
    ):
        if type(spec[field]) is not int or spec[field] < 1:
            raise ResearchError("invalid_full_evaluation", f"{field} must be a positive integer")
    half_life = spec["correlation_half_life"]
    if type(half_life) not in (int, float) or not math.isfinite(half_life) or half_life <= 0:
        raise ResearchError(
            "invalid_full_evaluation",
            "correlation_half_life must be a positive finite number",
        )
    spec["reference_value_artifacts"] = dict(sorted(normalized.items()))
    return spec


def build_standard_windows(
    dates: pd.DatetimeIndex,
    *,
    signal_start: str,
    signal_end: str,
) -> dict[str, dict[str, Any]]:
    index = pd.DatetimeIndex(dates).dropna().unique().sort_values()
    start = pd.Timestamp(signal_start)
    end = pd.Timestamp(signal_end)
    index = index[(index >= start) & (index <= end)]
    if index.empty:
        raise ResearchError("evaluation_empty", "No trading dates in the declared signal range")
    actual_start, actual_end = index[0], index[-1]

    windows: dict[str, dict[str, Any]] = {}

    def add(name: str, lower: pd.Timestamp, upper: pd.Timestamp) -> None:
        selected = index[(index >= lower) & (index <= upper)]
        if selected.empty:
            return
        windows[name] = {
            "start": str(selected[0].date()),
            "end": str(selected[-1].date()),
            "required_days": 0,
        }

    add("full", actual_start, actual_end)
    add("from_2024", pd.Timestamp("2024-01-01"), actual_end)
    for year in range(max(2024, actual_start.year), actual_end.year + 1):
        add(f"year_{year}", pd.Timestamp(year=year, month=1, day=1), pd.Timestamp(year=year, month=12, day=31))
    for months in (6, 3, 1):
        add(f"recent_{months}m", actual_end - pd.DateOffset(months=months), actual_end)
    month = pd.Timestamp(year=max(2024, actual_start.year), month=1, day=1)
    final_month = pd.Timestamp(year=actual_end.year, month=actual_end.month, day=1)
    while month <= final_month:
        add(
            f"month_{month:%Y_%m}",
            month,
            month + pd.offsets.MonthEnd(1),
        )
        month += pd.offsets.MonthBegin(1)
    return windows


def compute_candidate_metrics(
    factor_name: str,
    values: pd.DataFrame,
    ctx: dict[str, Any],
    windows: dict[str, dict[str, Any]],
    *,
    compute,
) -> dict[str, Any]:
    return compute(
        factor_name,
        values,
        ctx,
        evaluation_windows=windows,
        include_horizon_metrics=True,
    )


def _slice(values: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    dates = values.index.get_level_values("datetime")
    return values.loc[(dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end))]


def compute_correlation_views(
    candidate_paths: dict[str, Path],
    spec: dict[str, Any],
    windows: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    from backend.services.quantevolver.correlation_engine import CorrelationEngine

    candidate_frames = [load_values(path, name) for name, path in sorted(candidate_paths.items())]
    candidate_panel = pd.concat(candidate_frames, axis=1, join="outer").sort_index()
    reference_items = [
        (name, Path(path))
        for name, path in spec["reference_value_artifacts"].items()
        if name not in candidate_paths
    ]
    batch_size = spec["correlation_batch_size"]
    # Monthly all-horizon metrics are part of the independent evaluation, but
    # correlations are intentionally limited to the declared full, annual and
    # recent comparison views. Computing every candidate/reference pair again
    # for every month would add cost without satisfying a separate research
    # question.
    correlation_windows = {
        name: window for name, window in windows.items() if not name.startswith("month_")
    }
    states: dict[str, dict[str, Any]] = {}
    candidate_pair_results: list[dict[str, Any]] = []
    for window_name, window in correlation_windows.items():
        candidate_window = _slice(candidate_panel, window["start"], window["end"])
        state = {
            "window": window,
            "candidate_panel": candidate_window,
            "engine": None,
            "records": [],
        }
        states[window_name] = state
        if candidate_window.empty:
            internal_empty = [
                {
                    "candidate": left,
                    "reference": right,
                    "correlation": None,
                    "status": "unavailable",
                    "reason": "candidate_window_empty",
                    "effective_days": 0,
                    "avg_stocks_per_day": 0.0,
                }
                for position, left in enumerate(sorted(candidate_paths))
                for right in sorted(candidate_paths)[position + 1 :]
            ]
            candidate_pair_results.append(
                {
                    "window": window_name,
                    "start": window["start"],
                    "end": window["end"],
                    "records": internal_empty,
                    "reason": "candidate_window_empty",
                    "requested_pairs": len(candidate_paths)
                    * (len(candidate_paths) - 1)
                    // 2,
                    "available_pairs": 0,
                    "unavailable_pairs": len(internal_empty),
                }
            )
            continue
        engine = CorrelationEngine(
            object(),
            window=len(
                pd.DatetimeIndex(
                    candidate_window.index.get_level_values("datetime")
                ).unique()
            ),
            half_life=spec["correlation_half_life"],
            min_stocks=spec["correlation_min_stocks"],
            min_days=spec["correlation_min_effective_days"],
        )
        state["engine"] = engine
        internal_records: list[dict[str, Any]] = []
        if len(candidate_window.columns) > 1:
            prefix = "candidate_reference__"
            renamed = candidate_window.rename(
                columns={name: f"{prefix}{name}" for name in candidate_window.columns}
            )
            internal = engine.compute_selected_submatrix(
                candidate_window, renamed, as_of_date=window["end"]
            )
            for row in internal.records():
                reference = row["reference"].removeprefix(prefix)
                if row["candidate"] < reference:
                    internal_records.append({**row, "reference": reference})
        candidate_pair_results.append(
            {
                "window": window_name,
                "start": window["start"],
                "end": window["end"],
                "records": internal_records,
                "requested_pairs": len(candidate_paths) * (len(candidate_paths) - 1) // 2,
                "available_pairs": sum(
                    row["status"] == "available" for row in internal_records
                ),
                "unavailable_pairs": sum(
                    row["status"] != "available" for row in internal_records
                ),
            }
        )

    # Each reference batch is loaded once, then sliced across all declared
    # windows. This preserves full cross-sections without rereading the same
    # multi-year artifact for every annual/recent view.
    for offset in range(0, len(reference_items), batch_size):
        batch = reference_items[offset : offset + batch_size]
        reference_frames = [load_values(path, name) for name, path in batch]
        reference_full = pd.concat(reference_frames, axis=1, join="outer").sort_index()
        for state in states.values():
            window = state["window"]
            candidate_window = state["candidate_panel"]
            records = state["records"]
            if candidate_window.empty:
                reason = "candidate_window_empty"
                reference_window = None
            else:
                reference_window = _slice(reference_full, window["start"], window["end"])
                reason = "reference_window_empty" if reference_window.empty else None
            if reason is not None:
                for candidate in candidate_paths:
                    for reference, _ in batch:
                        records.append(
                            {
                                "candidate": candidate,
                                "reference": reference,
                                "correlation": None,
                                "status": "unavailable",
                                "reason": reason,
                                "effective_days": 0,
                                "avg_stocks_per_day": 0.0,
                            }
                        )
                continue
            result = state["engine"].compute_selected_submatrix(
                candidate_window,
                reference_window,
                as_of_date=window["end"],
            )
            records.extend(result.records())
        del reference_frames, reference_full

    window_results: list[dict[str, Any]] = []
    for window_name, state in states.items():
        window = state["window"]
        records = state["records"]
        window_results.append(
            {
                "window": window_name,
                "start": window["start"],
                "end": window["end"],
                "records": records,
                "requested_pairs": len(candidate_paths) * len(reference_items),
                "available_pairs": sum(row["status"] == "available" for row in records),
                "unavailable_pairs": sum(row["status"] != "available" for row in records),
            }
        )
        del state["candidate_panel"], state["engine"]
    return {
        "method": "cross_sectional_spearman_ewma_selected_pairs",
        "reference_count": len(reference_items),
        "reference_names": [name for name, _ in reference_items],
        "parameters": {
            key: spec[key]
            for key in (
                "correlation_batch_size",
                "correlation_half_life",
                "correlation_min_stocks",
                "correlation_min_effective_days",
            )
        },
        "windows": window_results,
        "window_names": list(correlation_windows),
        "candidate_candidate_windows": candidate_pair_results,
        "excluded_self_reference_names": sorted(set(candidate_paths) & set(spec["reference_value_artifacts"])),
        "reference_reference_pairs_computed": 0,
    }


def build_full_evaluation_result(
    *,
    run_spec: dict[str, Any],
    evaluation_spec: dict[str, Any],
    ctx: dict[str, Any],
    candidate_results: list[dict[str, Any]],
    candidate_paths: dict[str, Path],
) -> dict[str, Any]:
    windows = build_standard_windows(
        pd.DatetimeIndex(ctx["dates"]),
        signal_start=run_spec["signal_start"],
        signal_end=run_spec["signal_end"],
    )
    correlations = compute_correlation_views(candidate_paths, evaluation_spec, windows)
    dates = pd.DatetimeIndex(ctx["dates"]).dropna().unique().sort_values()
    actual_start = str(dates[0].date())
    actual_end = str(dates[-1].date())
    return {
        "schema_version": "factor_research_full_evaluation_v1",
        "scope": "research_only_not_official_metrics_correlations_or_qe_result",
        "requested_signal_range": {
            "start": run_spec["signal_start"],
            "end": run_spec["signal_end"],
        },
        "actual_price_context_range": {
            "start": actual_start,
            "end": actual_end,
        },
        "price_context_covers_requested_range": (
            actual_start <= run_spec["signal_start"] and actual_end >= run_spec["signal_end"]
        ),
        "instrument_coverage": ctx["instrument_coverage"],
        "all_requested_instruments_have_physical_prices": (
            ctx["instrument_coverage"]["missing_price_instrument_count"] == 0
        ),
        "windows": windows,
        "candidate_names": [row["factor_name"] for row in candidate_results],
        "correlations": correlations,
        "official_database_writes": 0,
    }
