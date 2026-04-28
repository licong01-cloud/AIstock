"""Run StrategyPackage live/latest-data inference and emit ranked scores.

This script is intentionally small so the Windows FastAPI backend can delegate
Qlib-dependent inference to the existing WSL conda environment. It does not read
QE backtest ``pred.pkl`` artifacts.
"""

from __future__ import annotations

import argparse
import builtins
import json
import math
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.inference_engine import InferenceEngine
import backend.inference_engine as inference_engine_module


def _score_rows_from_frame(df_scores: pd.DataFrame, expected_date) -> list[dict[str, Any]]:
    if df_scores is None or df_scores.empty:
        raise ValueError(f"live inference produced no score rows for {expected_date}")
    if isinstance(df_scores, pd.Series):
        df_scores = df_scores.to_frame(name="score")
    if "score" not in df_scores.columns:
        if len(df_scores.columns) == 1:
            df_scores = df_scores.rename(columns={df_scores.columns[0]: "score"})
        else:
            raise ValueError(f"score column missing from inference output: {list(df_scores.columns)}")
    if not isinstance(df_scores.index, pd.MultiIndex):
        raise ValueError(f"inference output index must be MultiIndex, got {type(df_scores.index).__name__}")
    if "datetime" not in df_scores.index.names or "instrument" not in df_scores.index.names:
        raise ValueError(f"inference output index names must include datetime/instrument: {df_scores.index.names}")
    actual_dates = sorted(set(pd.to_datetime(df_scores.index.get_level_values("datetime")).date))
    if actual_dates != [expected_date]:
        raise ValueError(
            "live inference did not score requested trade_date exactly: "
            f"expected={expected_date}, actual={actual_dates}"
        )
    day = df_scores.reset_index()
    day["symbol"] = day["instrument"].astype(str)
    day["score"] = pd.to_numeric(day["score"], errors="coerce")
    if not day["score"].map(lambda value: pd.notna(value) and math.isfinite(float(value))).all():
        raise ValueError("live inference output contains invalid scores")
    day = day.sort_values(["score", "symbol"], ascending=[False, True]).reset_index(drop=True)
    return [
        {"symbol": str(item.symbol), "score": float(item.score), "rank": int(rank)}
        for rank, item in enumerate(day.itertuples(index=False), start=1)
    ]


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _patch_windows_debug_open() -> None:
    original_open = builtins.open

    def patched_open(file, *args, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(file, str) and file.startswith("f:/Dev/AIstock/debug_tools/"):
            redirected = Path("/mnt/f/Dev/AIstock/debug_tools") / Path(file).name
            redirected.parent.mkdir(parents=True, exist_ok=True)
            return original_open(redirected, *args, **kwargs)
        return original_open(file, *args, **kwargs)

    builtins.open = patched_open


def _strategy_package_required_window(factor_order: list[str] | None = None) -> int:
    max_window = 61
    for factor in factor_order or []:
        for match in re.findall(r"(\d+)\s*d", str(factor), flags=re.IGNORECASE):
            max_window = max(max_window, int(match) + 5)
        if "250" in str(factor):
            max_window = max(max_window, 260)
    return max_window


def _patch_strategy_package_data_window() -> None:
    """Use full DB-backed feature data for StrategyPackage live inference.

    The legacy shared inference path caps some static data reads for UI latency.
    StrategyPackage authoritative inference must honor long-window factors such
    as 250-day turnover percentile, so the WSL runner patches only this process.
    """

    from backend.data_service import qe_data_service
    from backend.data_service import timescaledb_adapter

    def fetch_static(universe, start_date, end_date):
        return qe_data_service.build_static_factors(universe, start_date, end_date)

    inference_engine_module.get_required_data_window = _strategy_package_required_window
    timescaledb_adapter.fetch_fundamental_data_ts = fetch_static


def main() -> int:
    parser = argparse.ArgumentParser(description="StrategyPackage live inference runner")
    parser.add_argument("--runtime-workspace", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--cutoff-date")
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    runtime_workspace = Path(args.runtime_workspace)
    if not runtime_workspace.exists():
        raise FileNotFoundError(f"runtime workspace does not exist: {runtime_workspace}")
    trade_dt = _parse_date(args.trade_date)
    cutoff_dt = _parse_date(args.cutoff_date) if args.cutoff_date else None
    expected_date = (cutoff_dt or trade_dt).date()

    os.environ["AISTOCK_STRICT_INFERENCE"] = "1"
    os.environ.setdefault("AISTOCK_INFERENCE_NATURAL_DAY_MULTIPLIER", "1.8")
    os.environ.setdefault("AISTOCK_INFERENCE_NATURAL_DAY_BUFFER", "20")
    # The shared InferenceEngine writes diagnostics to a Windows-style path.
    # Redirect only that path while running inside WSL.
    _patch_windows_debug_open()
    _patch_strategy_package_data_window()
    df_scores = InferenceEngine().run_inference(
        strategy_id="",
        version_tag="strategy_package_live",
        trade_date=trade_dt,
        cutoff_date=cutoff_dt,
        experiment_id="strategy_package_live",
        workspace_path=str(runtime_workspace),
    )
    if not isinstance(df_scores, pd.DataFrame):
        raise TypeError(f"InferenceEngine returned {type(df_scores).__name__}, expected DataFrame")
    scores = _score_rows_from_frame(df_scores, expected_date)
    payload: dict[str, Any] = {
        "ok": True,
        "scores": scores,
        "metadata": {
            "runtime_workspace": str(runtime_workspace),
            "trade_date": trade_dt.date().isoformat(),
            "cutoff_date": cutoff_dt.date().isoformat() if cutoff_dt else None,
            "effective_trade_date": expected_date.isoformat(),
            "score_count": len(scores),
            "strict_feature_filter": getattr(inference_engine_module, "LAST_STRICT_FEATURE_FILTER", None),
        },
    }
    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
