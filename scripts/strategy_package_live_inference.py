"""Run StrategyPackage live/latest-data inference and emit ranked scores.

This script is intentionally small so the Windows FastAPI backend can delegate
Qlib-dependent inference to the existing WSL conda environment. It does not read
QE backtest ``pred.pkl`` artifacts.
"""

from __future__ import annotations

import argparse
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

import backend.inference_engine as inference_engine_module  # noqa: E402
from backend.inference_engine import InferenceEngine  # noqa: E402
from backend.services.strategy_package.advisory_input_projection import (  # noqa: E402
    get_strategy_package_inference_required_window,
)


def _score_rows_from_frame(
    df_scores: pd.DataFrame,
    expected_date,
    *,
    allow_empty: bool = False,
) -> list[dict[str, Any]]:
    if df_scores is None or df_scores.empty:
        if allow_empty:
            return []
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


def _strategy_package_required_window(factor_order: list[str] | None = None) -> int:
    return get_strategy_package_inference_required_window(factor_order or [])


def _patch_strategy_package_data_window() -> None:
    """Use full DB-backed feature data for StrategyPackage live inference.

    The legacy shared inference path caps some static data reads for UI latency.
    StrategyPackage authoritative inference must honor long-window factors such
    as 250-day turnover percentile, so the WSL runner patches only this process.
    """

    from backend.data_service import qe_data_service
    from backend.data_service import timescaledb_adapter

    def fetch_static(universe, start_date, end_date, max_natural_days: int | None = 180):
        # The inference engine already resolved the authoritative trading-day
        # window. Keep the shared adapter signature without truncating it again.
        return qe_data_service.build_static_factors(
            universe,
            start_date,
            end_date,
            asof_fill_slow_static=True,
        )

    inference_engine_module.get_required_data_window = _strategy_package_required_window
    timescaledb_adapter.fetch_fundamental_data_ts = fetch_static


def main() -> int:
    parser = argparse.ArgumentParser(description="StrategyPackage live inference runner")
    parser.add_argument("--runtime-workspace", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--cutoff-date")
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--historical-read-only", action="store_true")
    args = parser.parse_args()

    runtime_workspace = Path(args.runtime_workspace)
    if not runtime_workspace.exists():
        raise FileNotFoundError(f"runtime workspace does not exist: {runtime_workspace}")
    trade_dt = _parse_date(args.trade_date)
    cutoff_dt = _parse_date(args.cutoff_date) if args.cutoff_date else None
    expected_date = (cutoff_dt or trade_dt).date()
    output_path = Path(args.output_path)

    os.environ["AISTOCK_STRICT_INFERENCE"] = "1"
    os.environ.setdefault("AISTOCK_INFERENCE_NATURAL_DAY_MULTIPLIER", "1.8")
    os.environ.setdefault("AISTOCK_INFERENCE_NATURAL_DAY_BUFFER", "20")
    diagnostic_path = (
        output_path.parent / "diagnostics" / "qe_diagnosis.txt"
        if args.historical_read_only
        else Path("/mnt/f/Dev/AIstock/debug_tools/qe_diagnosis.txt")
    )
    diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
    _patch_strategy_package_data_window()
    engine = InferenceEngine()
    df_scores = engine.run_inference(
        strategy_id="",
        version_tag="strategy_package_live",
        trade_date=trade_dt,
        cutoff_date=cutoff_dt,
        experiment_id="strategy_package_live",
        workspace_path=str(runtime_workspace),
        persist_signals=not args.historical_read_only,
        universe_ensure=not args.historical_read_only,
        receipt_admissibility=(
            "RETROSPECTIVE_DB_CONTENT_HASH"
            if args.historical_read_only
            else "PROSPECTIVE_FIRST_OBSERVED"
        ),
        allow_external_market_fallback=not args.historical_read_only,
        use_selection_data_cache=not args.historical_read_only,
        diagnostic_output_path=str(diagnostic_path),
    )
    if not isinstance(df_scores, pd.DataFrame):
        raise TypeError(f"InferenceEngine returned {type(df_scores).__name__}, expected DataFrame")
    execution_receipt = engine.last_inference_receipt
    if not isinstance(execution_receipt, dict):
        raise RuntimeError("live inference did not produce a source/universe execution receipt")
    universe_count = execution_receipt.get("universe_count")
    source_read_receipts = execution_receipt.get("source_read_receipts")
    input_context = execution_receipt.get("input_context")
    if isinstance(universe_count, bool) or not isinstance(universe_count, int) or universe_count < 0:
        raise RuntimeError("live inference execution receipt has no valid universe_count")
    if args.historical_read_only and universe_count == 0:
        raise RuntimeError("historical inference cannot prove an empty candidate result with an empty universe")
    if not isinstance(source_read_receipts, list) or not all(isinstance(item, dict) for item in source_read_receipts):
        raise RuntimeError("live inference execution receipt has no valid source_read_receipts")
    if not isinstance(input_context, dict):
        raise RuntimeError("live inference execution receipt has no valid input_context")
    scores = _score_rows_from_frame(
        df_scores,
        expected_date,
        allow_empty=args.historical_read_only,
    )
    payload: dict[str, Any] = {
        "ok": True,
        "scores": scores,
        "universe_count": universe_count,
        "source_read_receipts": source_read_receipts,
        "input_context": input_context,
        "metadata": {
            "runtime_workspace": str(runtime_workspace),
            "trade_date": trade_dt.date().isoformat(),
            "cutoff_date": cutoff_dt.date().isoformat() if cutoff_dt else None,
            "effective_trade_date": expected_date.isoformat(),
            "score_count": len(scores),
            "strict_feature_filter": getattr(inference_engine_module, "LAST_STRICT_FEATURE_FILTER", None),
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return 0


def _extract_malformed_ts_code_samples(text: str, limit: int = 10) -> list[str]:
    pattern = re.compile(r"\b\d{6}\.[A-Z]{1,4}\d{4}-\d{2}-\d{2}T[^\s,;\"'\]\)}]+")
    samples: list[str] = []
    seen: set[str] = set()
    for match in pattern.finditer(text or ""):
        value = match.group(0)
        if value not in seen:
            samples.append(value)
            seen.add(value)
        if len(samples) >= limit:
            break
    return samples


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        diagnostic = {
            "ok": False,
            "error_type": type(exc).__name__,
            "message": str(exc),
            "malformed_ts_code_samples": _extract_malformed_ts_code_samples(str(exc)),
        }
        print("AISTOCK_LIVE_INFERENCE_ERROR=" + json.dumps(diagnostic, ensure_ascii=False), file=sys.stderr)
        raise
