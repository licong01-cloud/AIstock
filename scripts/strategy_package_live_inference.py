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
import hashlib
import uuid
from contextlib import ExitStack
from collections import Counter
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import backend.inference_engine as inference_engine_module  # noqa: E402
from backend.inference_engine import InferenceEngine  # noqa: E402
from backend.services.advisory_historical_range.factor_input_materialization import FactorInputSession  # noqa: E402
from backend.services.advisory_historical_range.inference_session import InferenceResourceSession, inference_output_lease, frozen_code_identity  # noqa: E402
from backend.services.advisory_historical_range.inference_inputs import database_input_session  # noqa: E402
from backend.services.advisory_historical_range.inference_features import InferenceFeatureSession  # noqa: E402
from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1  # noqa: E402
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
    if day["symbol"].duplicated().any():
        raise ValueError("live inference output contains duplicate candidate identities")
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
    parser.add_argument("--data-cache-mib", type=int, default=2048)
    parser.add_argument("--feature-cache-mib", type=int, default=512)
    parser.add_argument("--consumer-scope", choices=("ADVISORY", "LOCALSIM"), default="ADVISORY")
    parser.add_argument("--run-id", help="This consumer's independent run identity")
    parser.add_argument("--input-revision", help="Read-only source/revision identity; not a package approval")
    parser.add_argument("--resume", action="store_true", help="Resume this same consumer/run's exact input and code only")
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
    default_temp = ((Path("X:/AIstock_temp") if os.name == "nt" else Path("/mnt/x/AIstock_temp"))
                    / "advisory_inference" / args.consumer_scope / (args.run_id or uuid.uuid4().hex))
    temporary_root = _configure_temporary_root(args.temp_root or str(default_temp))
    if args.historical_plan:
        if not args.historical_read_only or args.cutoff_date or not args.temp_root:
            parser.error("--historical-plan requires --historical-read-only and --temp-root, without --cutoff-date")
        if min(args.resource_cache_mib, args.data_cache_mib, args.feature_cache_mib) < 0:
            parser.error("inference cache budgets must be non-negative")
        if args.resume and (not args.run_id or not args.input_revision):
            parser.error("exact resume requires its own --run-id and the caller's immutable --input-revision")
        return _run_historical_plan(args, runtime_workspace)
    if args.resume:
        parser.error("--resume applies only to an explicit historical plan")
    with ExitStack() as stack:
        # Same legal DB input adapter for standalone and batch. A standalone
        # session retains no history; there is no shared cache or fresh fit.
        inputs = database_input_session(consumer_scope=args.consumer_scope, run_id=args.run_id or uuid.uuid4().hex,
                                        source_identity=args.input_revision or "standalone_read", max_bytes=0)
        stack.callback(inputs.close)
        engine = InferenceEngine(temp_root=temporary_root, performance_reference=args.performance_reference,
                                 required_window_resolver=_strategy_package_required_window, input_session=inputs)
        output_path = Path(args.output_path)
        with inference_output_lease(output_path):
            payload = _run_request(engine, runtime_workspace, args.trade_date, args.cutoff_date, args.historical_read_only, output_path)
            _atomic_json(output_path, payload)
    return 0


def _strategy_package_fundamental_reader(*, universe, start_date, end_date):
    from backend.data_service import qe_data_service
    return qe_data_service.build_static_factors(universe, start_date, end_date, asof_fill_slow_static=True)


def _run_request(
    engine: InferenceEngine, runtime_workspace: Path, trade_date: str, cutoff_date: str | None,
    historical_read_only: bool, output_path: Path,
    pit_identity: dict[str, Any] | None = None,
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
    inference_arguments = dict(
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
        allow_external_market_fallback=False,
        use_selection_data_cache=False,
        diagnostic_output_path=str(diagnostic_path),
    )
    if pit_identity is not None:
        inference_arguments["pit_identity"] = pit_identity
    df_scores = engine.run_inference(**inference_arguments)
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
            "strict_feature_filter": getattr(engine, "last_strict_feature_filter", None),
            "inference_performance": getattr(engine, "last_inference_performance", None),
            "universe_members": execution_receipt.get("universe_members"),
            "unscored_members": execution_receipt.get("unscored_members"),
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
    staged = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=".inference-", delete=False) as stream:
            staged = Path(stream.name)
            json.dump(payload, stream, ensure_ascii=False, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(staged, path)
    finally:
        if staged is not None and staged.exists():
            staged.unlink()  # Exact file created by this call, never recursive.


def _run_historical_plan(args: argparse.Namespace, runtime_workspace: Path) -> int:
    root = Path(args.temp_root).resolve()
    output = Path(args.output_path).resolve()
    if not output.is_relative_to(root):
        raise ValueError("historical plan output must be within its task-owned X temporary root")
    with inference_output_lease(output):
        return _run_owned_historical_plan(args, runtime_workspace)


def _run_owned_historical_plan(args: argparse.Namespace, runtime_workspace: Path) -> int:
    plan = json.loads(Path(args.historical_plan).read_text(encoding="utf-8"))
    if not isinstance(plan, list) or not plan:
        raise ValueError("historical plan must be a non-empty JSON list")
    parsed = []
    for request in plan:
        allowed = {"trade_date", "cutoff_date", "decision_date", "data_cutoff", "target_trade_date", "runtime_workspace", "pit_identity", "request_id"}
        if not isinstance(request, dict) or set(request) - allowed:
            raise ValueError("historical plan has unsupported fields")
        legacy = "trade_date" in request
        if (legacy and ({"decision_date", "data_cutoff"} & set(request))) or (not legacy and "cutoff_date" in request):
            raise ValueError("historical plan cannot mix legacy and explicit clocks")
        if request.get("pit_identity") is not None and not isinstance(request["pit_identity"], dict):
            raise ValueError("historical plan PIT identity must be an object")
        decision = _parse_date(request["trade_date"] if legacy else request["decision_date"]).date()
        cutoff = _parse_date(request.get("cutoff_date", request["trade_date"]) if legacy else request["data_cutoff"]).date()
        target = _parse_date(request.get("target_trade_date", decision.isoformat())).date()
        clocks = SelectionSignalDayV1(decision, cutoff, target)
        workspace = Path(request.get("runtime_workspace", runtime_workspace)).resolve(strict=True)
        request_id = request.get("request_id")
        if request_id is not None and (not isinstance(request_id, str) or not request_id.strip()):
            raise ValueError("explicit model request identity must be a nonempty string")
        parsed.append((clocks, workspace, request.get("pit_identity"), request_id))
    identities = [(item[0].decision_date, item[3]) for item in parsed]
    if (len(identities) != len(set(identities))
            or any(left[0].decision_date > right[0].decision_date for left, right in zip(parsed, parsed[1:]))):
        raise ValueError("historical plan must preserve increasing dates and unique model request identities")
    duplicate_days = {day for day, count in Counter(day for day, _ in identities).items() if count > 1}
    if any(day in duplicate_days and request_id is None for day, request_id in identities):
        raise ValueError("multiple models on one date require explicit request identities")
    output = Path(args.output_path).resolve()
    root = Path(args.temp_root).resolve()
    if not output.is_relative_to(root):
        raise ValueError("historical plan output must be within its task-owned X temporary root")
    results_root = (output.parent / (output.stem + "_dates")).resolve()
    if not results_root.is_relative_to(root):
        raise ValueError("daily output cannot escape its own task root")
    if not args.resume and (output.exists() or results_root.exists()):
        raise FileExistsError("historical output already exists; use a new task output without overwriting an old run")
    run_id = args.run_id or uuid.uuid4().hex
    source_revision = args.input_revision or f"run_snapshot:{args.consumer_scope}:{run_id}"
    binding = _plan_binding(plan, parsed, args.consumer_scope, run_id, source_revision, args.performance_reference)
    started = time.perf_counter()
    index: dict[str, Any] = {"ok": False, "status": "RUNNING", "execution_mode": "historical_resource_session",
                           "consumer_scope": args.consumer_scope, "run_id": run_id, "binding": binding, "results": []}
    if args.resume:
        previous = json.loads(output.read_text(encoding="utf-8"))
        if previous.get("binding") != binding or previous.get("consumer_scope") != args.consumer_scope or previous.get("run_id") != run_id:
            raise ValueError("exact retry input/code/consumer/run identity changed")
        previous_results = previous.get("results")
        if not isinstance(previous_results, list) or len(previous_results) > len(parsed):
            raise ValueError("own-run checkpoint is not the requested date prefix")
        for ordinal, item in enumerate(previous_results):
            clocks = parsed[ordinal][0]
            request_id = parsed[ordinal][3]
            expected_path = _day_output(results_root, clocks, request_id)
            day_path = Path(item["output_path"]).resolve(strict=True)
            if (day_path != expected_path or item["trade_date"] != clocks.decision_date.isoformat()
                    or item.get("request_id") != request_id or not day_path.is_relative_to(results_root)
                    or hashlib.sha256(day_path.read_bytes()).hexdigest() != item["sha256"]):
                raise ValueError("own-run checkpoint path/content identity mismatch")
            _verify_owned_day(day_path, clocks=clocks, binding=binding, consumer_scope=args.consumer_scope, run_id=run_id, request_id=request_id)
        index["results"] = previous_results
    if len(index["results"]) > len(parsed):
        raise ValueError("own-run checkpoint exceeds the complete requested date plan")
    stack = ExitStack()
    clocks = parsed[0][0]
    request_id = parsed[0][3]
    stage = "session_initialization"
    try:
        session = InferenceResourceSession(max_bytes=args.resource_cache_mib * 1024 * 1024)
        stack.callback(session.close)
        inputs = database_input_session(consumer_scope=args.consumer_scope, run_id=run_id, source_identity=source_revision,
                                        max_bytes=0 if args.performance_reference else args.data_cache_mib * 1024**2)
        stack.callback(inputs.close)
        features = InferenceFeatureSession(max_bytes=0 if args.performance_reference else args.feature_cache_mib * 1024**2)
        stack.callback(features.close)
        # The reference keeps the original calculations, not an unsafe
        # duplicate-file layout. Both arms write a legal physical view once.
        materializations = FactorInputSession(root=root)
        stack.callback(materializations.close)
        engine = InferenceEngine(resource_session=session, input_session=inputs, feature_session=features,
                                 materialization_session=materializations,
                                 performance_reference=args.performance_reference, temp_root=root,
                                 required_window_resolver=_strategy_package_required_window)
        stage = "checkpoint_publication"
        _atomic_json(output, index)
        for ordinal, (clocks, workspace, pit_identity, request_id) in enumerate(parsed):
            stage = "checkpoint_verification"
            day_output = _day_output(results_root, clocks, request_id)
            if not day_output.is_relative_to(results_root):
                raise ValueError("daily artifact path escaped its own run")
            if ordinal < len(index["results"]):
                if index["results"][ordinal]["trade_date"] != clocks.decision_date.isoformat():
                    raise ValueError("own-run checkpoint dates are not the planned prefix")
                continue
            commit = day_output.with_name("commit.json")
            if day_output.exists() or commit.exists():
                # Recover a publish-before-index crash only from this run's
                # exact content-bound commit, never from arbitrary file presence.
                _verify_owned_day(day_output, clocks=clocks, binding=binding, consumer_scope=args.consumer_scope, run_id=run_id, request_id=request_id)
                day_hash = hashlib.sha256(day_output.read_bytes()).hexdigest()
                marker = json.loads(commit.read_text(encoding="utf-8")) if commit.exists() else None
                if marker is not None and (marker.get("binding") != binding or marker.get("clocks") != clocks.to_dict() or marker.get("sha256") != day_hash):
                    raise ValueError("uncommitted or conflicting daily artifact")
                if marker is None:
                    _atomic_json(commit, {"binding": binding, "clocks": clocks.to_dict(), "sha256": day_hash})
            else:
                stage = "raw_inference"
                payload = _run_request(
                    engine, workspace, clocks.decision_date.isoformat(), clocks.data_cutoff.isoformat(),
                    True, day_output, pit_identity=pit_identity,
                )
                payload["metadata"].update(consumer_scope=args.consumer_scope, run_id=run_id, clocks=clocks.to_dict(), binding=binding, request_id=request_id)
                stage = "daily_publication"
                _atomic_json(day_output, payload)
                _atomic_json(commit, {"binding": binding, "clocks": clocks.to_dict(), "sha256": hashlib.sha256(day_output.read_bytes()).hexdigest()})
            index["results"].append({"trade_date": clocks.decision_date.isoformat(), "output_path": str(day_output),
                                     "sha256": hashlib.sha256(day_output.read_bytes()).hexdigest(), "request_id": request_id})
            _atomic_json(output, index)
        if binding != _plan_binding(plan, parsed, args.consumer_scope, run_id, source_revision, args.performance_reference):
            raise ValueError("inference code or assets changed during the batch")
        index.update(ok=True, status="COMPLETE", total_seconds=time.perf_counter() - started,
                     resource_loads=session.loads, resource_hits=session.hits, resource_retained_bytes=session.cache_bytes)
        _atomic_json(output, index)
    except Exception as exc:
        index.update(status="FAILED", error_type=type(exc).__name__, reason_code=getattr(exc, "reason_code", type(exc).__name__),
                     failed_stage=stage, failed_clocks=clocks.to_dict(), failed_request_id=request_id)
        try:
            _atomic_json(output, index)
        except OSError:
            pass  # Preserve the original failure; never report a success.
        raise
    finally:
        stack.close()
    return 0


def _day_output(root: Path, clocks: SelectionSignalDayV1, request_id: str | None) -> Path:
    directory = root / clocks.decision_date.isoformat()
    if request_id is not None:
        directory /= hashlib.sha256(request_id.encode()).hexdigest()
    return (directory / "scores.json").resolve()


def _verify_owned_day(path: Path, *, clocks: SelectionSignalDayV1, binding: str, consumer_scope: str, run_id: str, request_id: str | None) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    metadata = payload.get("metadata", {})
    count = payload.get("universe_count")
    if (payload.get("ok") is not True or not isinstance(payload.get("scores"), list)
            or isinstance(count, bool) or not isinstance(count, int) or count <= 0
            or not isinstance(payload.get("input_context"), dict) or not isinstance(payload.get("source_read_receipts"), list)
            or metadata.get("binding") != binding or metadata.get("clocks") != clocks.to_dict()
            or metadata.get("consumer_scope") != consumer_scope or metadata.get("run_id") != run_id
            or metadata.get("request_id") != request_id
            or metadata.get("effective_trade_date") != clocks.data_cutoff.isoformat()):
        raise ValueError("daily artifact is not a complete result from this same consumer/run/clock/input")


def _plan_binding(plan, parsed, consumer_scope, run_id, source_revision, reference):
    files = [Path(__file__), Path(inference_engine_module.__file__)]
    leaf = REPO_ROOT / "backend/services/advisory_historical_range"
    files += [leaf / (name + ".py") for name in (
        "alpha158_batch", "inference_session", "inference_inputs", "inference_features", "selection_signal_kernel", "factor_input_materialization")]
    assets = {}
    for _, workspace, _, _ in parsed:
        manifest = json.loads((workspace / "manifest.json").read_text(encoding="utf-8"))
        assets[str(workspace) + "/frozen_code"] = frozen_code_identity(workspace)
        files.extend([workspace / "manifest.json", workspace / "factor_order.json"])
        # Small frozen model code is part of numerical identity. Blobs stay in
        # place; their admitted hashes remain in the native manifest/receipts.
        files.extend(path for path in (workspace / "model").rglob("*.py") if path.is_file())
        primary = manifest["primary_assets"]
        for name in ("model_weight_relpath", "dataset_processor_relpath", "dataset_relpath"):
            if primary.get(name):
                path = (workspace / primary[name]).resolve(strict=True)
                stat = path.stat()
                assets[str(path)] = [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns]
        if primary.get("factor_entry_relpath"):
            path = workspace / primary["factor_entry_relpath"]
            files.append(path)
            import ast
            for node in ast.parse(path.read_text(encoding="utf-8")).body:
                if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "_FACTOR_FILES" for target in node.targets):
                    files.extend(Path(value) for value in ast.literal_eval(node.value).values())
    environment = {}
    for package in ("numpy", "pandas", "torch", "pyqlib", "lightgbm", "tables", "scipy"):
        try:
            environment[package] = version(package)
        except PackageNotFoundError:
            environment[package] = None
    identity = {"plan": plan, "consumer": consumer_scope, "run": run_id, "source": source_revision, "reference": reference,
                "code": {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in set(files)}, "assets": assets,
                "python": sys.version, "environment": environment, "window": _strategy_package_required_window([]),
                "window_config": {key: os.getenv(key) for key in ("AISTOCK_INFERENCE_NATURAL_DAY_MULTIPLIER", "AISTOCK_INFERENCE_NATURAL_DAY_BUFFER", "AISTOCK_STRICT_INFERENCE")}}
    return hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()


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
