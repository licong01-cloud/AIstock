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
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import backend.inference_engine as inference_engine_module  # noqa: E402
from backend.inference_engine import InferenceEngine  # noqa: E402
from backend.services.advisory_historical_range.inference_session import InferenceResourceSession  # noqa: E402
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
    dates = parser.add_mutually_exclusive_group(required=True)
    dates.add_argument("--trade-date")
    dates.add_argument("--historical-plan", help="JSON list of trade_date/cutoff_date requests for one frozen workspace")
    parser.add_argument("--cutoff-date")
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--historical-read-only", action="store_true")
    parser.add_argument("--temp-root", help="Task-owned X-drive temporary root; required for a historical plan")
    parser.add_argument("--resource-cache-mib", type=int, default=512)
    parser.add_argument("--performance-reference", action="store_true", help="Isolated read-only comparison with the pre-optimization algorithms")
    args = parser.parse_args()

    runtime_workspace = Path(args.runtime_workspace)
    if not runtime_workspace.exists():
        raise FileNotFoundError(f"runtime workspace does not exist: {runtime_workspace}")
    os.environ["AISTOCK_STRICT_INFERENCE"] = "1"
    os.environ.setdefault("AISTOCK_INFERENCE_NATURAL_DAY_MULTIPLIER", "1.8")
    os.environ.setdefault("AISTOCK_INFERENCE_NATURAL_DAY_BUFFER", "20")
    if args.performance_reference and not args.historical_read_only:
        parser.error("--performance-reference is read-only and cannot publish business signals")
    default_temp = (Path("X:/AIstock_temp") if os.name == "nt" else Path("/mnt/x/AIstock_temp")) / "advisory_inference" / runtime_workspace.name
    _configure_temporary_root(args.temp_root or str(default_temp))
    if args.historical_plan:
        if not args.historical_read_only or args.cutoff_date or not args.temp_root:
            parser.error("--historical-plan requires --historical-read-only and --temp-root, without --cutoff-date")
        if args.resource_cache_mib < 0:
            parser.error("--resource-cache-mib must be non-negative")
        _patch_strategy_package_data_window()
        return _run_historical_plan(args, runtime_workspace)
    _patch_strategy_package_data_window()
    engine = InferenceEngine(performance_reference=True) if args.performance_reference else InferenceEngine()
    payload = _run_request(engine, runtime_workspace, args.trade_date, args.cutoff_date, args.historical_read_only, Path(args.output_path))
    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return 0


def _run_request(
    engine: InferenceEngine, runtime_workspace: Path, trade_date: str, cutoff_date: str | None,
    historical_read_only: bool, output_path: Path,
) -> dict[str, Any]:
    trade_dt = _parse_date(trade_date)
    cutoff_dt = _parse_date(cutoff_date) if cutoff_date else None
    expected_date = (cutoff_dt or trade_dt).date()
    diagnostic_path = (
        output_path.parent / "diagnostics" / "qe_diagnosis.txt"
        if historical_read_only
        else Path.cwd() / "debug_tools" / "qe_diagnosis.txt"
    )
    diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
    df_scores = engine.run_inference(
        strategy_id="",
        version_tag="strategy_package_live",
        trade_date=trade_dt,
        cutoff_date=cutoff_dt,
        experiment_id="strategy_package_live",
        workspace_path=str(runtime_workspace),
        persist_signals=not historical_read_only,
        universe_ensure=not historical_read_only,
        receipt_admissibility=(
            "RETROSPECTIVE_DB_CONTENT_HASH"
            if historical_read_only
            else "PROSPECTIVE_FIRST_OBSERVED"
        ),
        allow_external_market_fallback=not historical_read_only,
        use_selection_data_cache=not historical_read_only,
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
    if historical_read_only and universe_count == 0:
        raise RuntimeError("historical inference cannot prove an empty candidate result with an empty universe")
    if not isinstance(source_read_receipts, list) or not all(isinstance(item, dict) for item in source_read_receipts):
        raise RuntimeError("live inference execution receipt has no valid source_read_receipts")
    if not isinstance(input_context, dict):
        raise RuntimeError("live inference execution receipt has no valid input_context")
    scores = _score_rows_from_frame(
        df_scores,
        expected_date,
        allow_empty=historical_read_only,
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
            "inference_performance": getattr(engine, "last_inference_performance", None),
        },
    }
    return payload


def _configure_temporary_root(value: str) -> Path:
    root = Path(value).expanduser().resolve()
    on_x = root.drive.upper() == "X:" if os.name == "nt" else root.is_relative_to(Path("/mnt/x"))
    if not on_x:
        raise ValueError("inference temporary root must resolve to X: or /mnt/x, never the default/C drive")
    if os.name != "nt" and not Path("/mnt/x").is_mount():
        raise FileNotFoundError("X drive is not mounted at /mnt/x; default temporary storage is forbidden")
    root.mkdir(parents=True, exist_ok=True)
    for key in ("TEMP", "TMP", "TMPDIR"):
        os.environ[key] = str(root)
    for key, directory in {
        "XDG_CACHE_HOME": "xdg", "TORCH_HOME": "torch", "TRITON_CACHE_DIR": "triton",
        "NUMBA_CACHE_DIR": "numba", "CUDA_CACHE_PATH": "cuda", "MPLCONFIGDIR": "matplotlib",
    }.items():
        cache = (root / "library_cache" / directory).resolve()
        if not cache.is_relative_to(root):
            raise ValueError("inference library cache cannot escape its X-drive task root")
        cache.mkdir(parents=True, exist_ok=True)
        os.environ[key] = str(cache)
    tempfile.tempdir = str(root)  # Libraries may already have resolved the previous default.
    return root


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=".inference-", delete=False) as stream:
        json.dump(payload, stream, ensure_ascii=False, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
        staged = Path(stream.name)
    os.replace(staged, path)


def _run_historical_plan(args: argparse.Namespace, runtime_workspace: Path) -> int:
    plan = json.loads(Path(args.historical_plan).read_text(encoding="utf-8"))
    if not isinstance(plan, list) or not plan:
        raise ValueError("historical plan must be a non-empty JSON list")
    parsed = []
    for request in plan:
        if not isinstance(request, dict) or set(request) - {"trade_date", "cutoff_date"}:
            raise ValueError("historical plan entries only contain trade_date and optional cutoff_date")
        trade_dt = _parse_date(request["trade_date"])
        cutoff_dt = _parse_date(request["cutoff_date"]) if request.get("cutoff_date") else None
        if cutoff_dt is not None and cutoff_dt > trade_dt:
            raise ValueError("historical request cutoff cannot be later than its trade date")
        parsed.append((trade_dt, cutoff_dt))
    if any(left[0] >= right[0] for left, right in zip(parsed, parsed[1:])):
        raise ValueError("historical plan must contain unique increasing trade dates; no dates are removed")
    output = Path(args.output_path).resolve()
    root = Path(args.temp_root).resolve()
    if not output.is_relative_to(root):
        raise ValueError("historical plan output must be within its task-owned X temporary root")
    results_root = output.parent / (output.stem + "_dates")
    if output.exists() or results_root.exists():
        raise FileExistsError("historical output already exists; use a new task output without overwriting an old run")
    session = InferenceResourceSession(max_bytes=args.resource_cache_mib * 1024 * 1024)
    engine_kwargs = {"resource_session": session}
    if args.performance_reference:
        engine_kwargs["performance_reference"] = True
    engine = InferenceEngine(**engine_kwargs)
    started = time.perf_counter()
    index: dict[str, Any] = {"ok": False, "status": "RUNNING", "execution_mode": "historical_resource_session", "results": []}
    _atomic_json(output, index)
    try:
        for trade_dt, cutoff_dt in parsed:
            day_output = results_root / trade_dt.date().isoformat() / "scores.json"
            payload = _run_request(
                engine, runtime_workspace, trade_dt.date().isoformat(),
                cutoff_dt.date().isoformat() if cutoff_dt else None, True, day_output,
            )
            _atomic_json(day_output, payload)
            index["results"].append({"trade_date": trade_dt.date().isoformat(), "output_path": str(day_output)})
            _atomic_json(output, index)
        index.update(ok=True, status="COMPLETE", total_seconds=time.perf_counter() - started,
                     resource_loads=session.loads, resource_hits=session.hits, resource_retained_bytes=session.cache_bytes)
        _atomic_json(output, index)
    except Exception as exc:
        index.update(status="FAILED", error_type=type(exc).__name__)
        _atomic_json(output, index)
        raise
    finally:
        session.close()
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
