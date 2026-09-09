"""Reviewed candidate scripts in fresh processes; reuse the existing metric engine."""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from .models import ResearchError, encode, identifier, json_object

CANONICAL_UNIVERSE = "aistock_equity_pit_canonical_v2"
REPO_ROOT = Path(__file__).resolve().parents[3]


def validate_spec(value):
    spec = json_object(value)
    allowed = {"task_id", "record_id", "attempt_id", "expected_revision", "universe_key", "method_version",
               "read_start", "signal_start", "signal_end", "read_end", "cutoff", "instruments",
               "data_dir", "qlib_bin_path", "artifact_root", "candidates", "timeout_seconds", "comparison"}
    if set(spec) - allowed:
        raise ResearchError("invalid_request", f"Unknown run fields: {sorted(set(spec) - allowed)}")
    for key in ("task_id", "record_id", "attempt_id"):
        spec[key] = identifier(spec.get(key), key)
    if type(spec.get("expected_revision")) is not int or spec["expected_revision"] < 1:
        raise ResearchError("invalid_request", "expected_revision must be positive")
    if spec.get("universe_key") != CANONICAL_UNIVERSE:
        raise ResearchError("unsupported_authority", "Use the existing canonical v2 read-only source; no PIT bootstrap")
    if not isinstance(spec.get("method_version"), str) or not spec["method_version"]:
        raise ResearchError("invalid_request", "method_version is required")
    try:
        dates = [date.fromisoformat(spec[k]) for k in (
            "read_start", "signal_start", "signal_end", "read_end", "cutoff")]
    except (KeyError, ValueError, TypeError) as exc:
        raise ResearchError("invalid_request", "Explicit ISO dates are required") from exc
    if dates != sorted(dates):
        raise ResearchError("invalid_request", "Require read_start <= signal_start <= signal_end <= read_end <= cutoff")
    instruments = spec.get("instruments")
    if not isinstance(instruments, list) or not instruments or any(
            not isinstance(s, str) or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", s) for s in instruments):
        raise ResearchError("invalid_request", "Explicit canonical instruments are required")
    spec["instruments"] = sorted(set(instruments))
    for key in ("data_dir", "qlib_bin_path", "artifact_root"):
        if not isinstance(spec.get(key), str):
            raise ResearchError("invalid_request", f"{key} path is required")
        spec[key] = str(Path(spec[key]).expanduser().resolve())
    for key in ("data_dir", "qlib_bin_path"):
        if not Path(spec[key]).is_dir():
            raise ResearchError("input_missing", f"{key} does not exist")
    output = Path(spec["artifact_root"]) / spec["task_id"] / spec["attempt_id"]
    if output.resolve() != output:
        raise ResearchError("output_conflict", "Task output contains a redirected parent")
    if any((parent / ".git").exists() for parent in (output, *output.parents)):
        raise ResearchError("output_conflict", "Artifacts must not be written inside any repository")
    for source in (REPO_ROOT, Path(spec["data_dir"]), Path(spec["qlib_bin_path"])):
        if output.is_relative_to(source) or source.is_relative_to(output):
            raise ResearchError("output_conflict", "Output must be outside repository and input data")
    candidates = spec.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise ResearchError("invalid_request", "candidates must be a non-empty list")
    names = []
    for item in candidates:
        if not isinstance(item, dict) or set(item) != {"factor_name", "script"}:
            raise ResearchError("invalid_request", "Each candidate requires factor_name and script")
        if not isinstance(item["factor_name"], str) or not re.fullmatch(r"[a-z][a-z0-9_]{2,80}", item["factor_name"]):
            raise ResearchError("invalid_request", "Invalid factor name")
        script = Path(item["script"]).expanduser().resolve()
        if not script.is_file():
            raise ResearchError("input_missing", "Candidate script not found")
        item["script"] = str(script)
        names.append(item["factor_name"])
    if len(names) != len(set(names)):
        raise ResearchError("invalid_request", "Candidate names must be unique within an attempt")
    if spec.get("comparison") is not None:
        from .comparison import validate_comparison_spec

        spec["comparison"] = validate_comparison_spec(
            spec["comparison"], candidate_names=set(names), repo_root=REPO_ROOT,
        )
        comparison_start = min(item["start"] for item in spec["comparison"]["fit_windows"])
        comparison_end = max(item["end"] for item in spec["comparison"]["evaluation_windows"])
        if comparison_start < spec["signal_start"] or comparison_end > spec["signal_end"]:
            raise ResearchError("invalid_comparison", "Comparison windows must stay inside the declared signal window")
        if spec["comparison"]["knowledge_cutoff"]["date"] > spec["cutoff"]:
            raise ResearchError("invalid_comparison", "Comparison knowledge cutoff cannot exceed the run cutoff")
    timeout = spec.get("timeout_seconds", 600)
    if type(timeout) not in (int, float) or timeout <= 0:
        raise ResearchError("invalid_request", "timeout_seconds must be positive")
    spec["timeout_seconds"] = timeout
    return spec, output


def write_json(path, payload):
    """Exclusive immutable result creation, not a dataset identity/hash operation."""
    with Path(path).open("x", encoding="utf-8") as stream:
        stream.write(encode(payload))


def load_values(path, name):
    import numpy as np
    import pandas as pd

    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ResearchError("candidate_output_missing", "Expected a regular candidate result file")
    frame = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_hdf(path, key="data")
    if not isinstance(frame, pd.DataFrame) or list(frame.columns) != [name]:
        raise ResearchError("candidate_schema_invalid", "Expected exactly the named factor column")
    if not isinstance(frame.index, pd.MultiIndex) or frame.index.names != ["datetime", "instrument"]:
        raise ResearchError("candidate_schema_invalid", "Expected MultiIndex(datetime,instrument)")
    if frame.empty or not frame.index.is_unique:
        raise ResearchError("candidate_schema_invalid", "Candidate is empty or has duplicate index rows")
    dates = frame.index.get_level_values("datetime")
    if not isinstance(dates, pd.DatetimeIndex) or dates.hasnans or dates.tz is not None:
        raise ResearchError("candidate_schema_invalid", "Expected non-null timezone-naive market dates")
    if not dates.equals(dates.normalize()):
        raise ResearchError("candidate_schema_invalid", "Daily values must use normalized market dates")
    values = frame[name]
    if not pd.api.types.is_numeric_dtype(values) or pd.api.types.is_complex_dtype(values):
        raise ResearchError("candidate_schema_invalid", "Expected real numeric values")
    if np.isinf(values.to_numpy(dtype=float)).any():
        raise ResearchError("candidate_nonfinite", "Infinite candidate values; NaN coverage is reported separately")
    return frame.sort_index()


def evaluation_context(ctx, spec):
    """Slice engine-produced labels, never recompute labels after truncation."""
    import pandas as pd

    start, end = pd.Timestamp(spec["signal_start"]), pd.Timestamp(spec["signal_end"])
    dates = ctx["close_unstacked"].index
    selected = dates[(dates >= start) & (dates <= end)]
    if selected.empty:
        raise ResearchError("evaluation_empty", "No signal dates available")
    view = dict(ctx)
    view["label_calendar"] = pd.DatetimeIndex(dates)
    for key in ("close_unstacked", "st_pit_eligible_mask"):
        view[key] = ctx[key].loc[selected]
    view["fwd_ret_mats"] = {key: value.loc[selected] for key, value in ctx["fwd_ret_mats"].items()}
    view.update(dates=selected, data_start=str(selected[0].date()), data_end=str(selected[-1].date()))
    return view


def execute(spec, output, *, prepare=None, compute=None):
    """Called only after a committed, newly-applied attempt. No database writes here."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "execution.json", {
        "status": "started", "pid": os.getpid(), "python": sys.executable,
        "started_at": datetime.now(timezone.utc), "request": spec})
    if prepare is None or compute is None:
        from backend.services.quantevolver.qe_eval_v2_metric_engine import (
            compute_single_factor_metrics, prepare_shared_context,
        )
        prepare = prepare or prepare_shared_context
        compute = compute or compute_single_factor_metrics
    ctx = None
    results = []
    for candidate in spec["candidates"]:
        name = candidate["factor_name"]
        folder = output / name
        folder.mkdir()
        script = folder / "factor.py"
        shutil.copyfile(candidate["script"], script)
        result_path = folder / "values.h5"
        command = [sys.executable, str(script), "--data-dir", spec["data_dir"],
                   "--output", str(result_path), "--start-date", spec["read_start"],
                   "--end-date", spec["read_end"], "--instruments", json.dumps(spec["instruments"])]
        # Logs belong only to this attempt; no capture of huge subprocess output in RAM.
        with (folder / "stdout.log").open("x", encoding="utf-8") as stdout, (
                folder / "stderr.log").open("x", encoding="utf-8") as stderr:
            completed = subprocess.run(command, cwd=folder, stdout=stdout, stderr=stderr,
                                       timeout=spec["timeout_seconds"], check=False)
        if completed.returncode:
            raise ResearchError("candidate_execution_failed", "Reviewed script returned non-zero",
                                returncode=completed.returncode, artifact=str(folder))
        frame = load_values(result_path, name)
        import pandas as pd
        dates = frame.index.get_level_values("datetime")
        symbols = set(frame.index.get_level_values("instrument"))
        if not symbols.issubset(spec["instruments"]) or dates.min() < pd.Timestamp(spec["read_start"]) or dates.max() > pd.Timestamp(spec["read_end"]):
            raise ResearchError("candidate_scope_mismatch", "Candidate contains undeclared symbols or dates")
        if ctx is None:
            ctx = prepare(qlib_bin_path=Path(spec["qlib_bin_path"]), start_date=spec["read_start"],
                          end_date=spec["read_end"], instrument_hint=set(spec["instruments"]),
                          load_suspend_d=True, load_st_pit_mask=True, universe_key=spec["universe_key"])
            ctx = evaluation_context(ctx, spec)
        selected = frame.loc[(dates >= pd.Timestamp(spec["signal_start"])) &
                             (dates <= pd.Timestamp(spec["signal_end"]))]
        metrics = compute(name, selected, ctx)
        result = {"factor_name": name, "scope": "research_candidate", "metrics": metrics,
                  "rows": len(frame), "nan_rows": int(frame[name].isna().sum()),
                  "signal_rows": len(selected), "source_script": str(script),
                  "values": str(result_path), "actual_signal_start": ctx["data_start"],
                  "actual_signal_end": ctx["data_end"], "correlation_status": "not_computed"}
        write_json(folder / "metrics.json", result)
        results.append(result)
        del frame, selected
    comparison = None
    if spec.get("comparison") is not None:
        import pandas as pd
        from .comparison import compute_comparison

        candidate_paths = {item["factor_name"]: Path(item["values"]) for item in results}
        signals = {}
        requested = set(spec["comparison"]["baseline"]) | {spec["comparison"]["candidate"]}
        requested.update(spec["comparison"]["controls"]["style"])
        requested.update(spec["comparison"]["controls"]["neighbors"])
        requested.update(spec["comparison"]["controls"]["categorical"])
        if spec["comparison"].get("state"):
            requested.add(spec["comparison"]["state"])
        for name in requested:
            source = candidate_paths.get(name) or Path(spec["comparison"]["value_artifacts"][name])
            values = load_values(source, name)
            dates = values.index.get_level_values("datetime")
            symbols = values.index.get_level_values("instrument")
            selected = ((dates >= pd.Timestamp(spec["signal_start"]))
                        & (dates <= pd.Timestamp(spec["signal_end"]))
                        & symbols.isin(spec["instruments"]))
            values = values.loc[selected]
            if values.empty:
                raise ResearchError("comparison_unavailable", f"Comparison signal {name} has no rows in run scope")
            signals[name] = values
        comparison = compute_comparison(spec["comparison"], signals, ctx, spec)
    payload = {"status": "computed", "scope": "research_candidate", "task_id": spec["task_id"],
               "attempt_id": spec["attempt_id"], "request": spec, "candidates": results,
               "finished_at": datetime.now(timezone.utc).isoformat()}
    if comparison is not None:
        payload["research_comparison"] = comparison
    return payload
