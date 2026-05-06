import pickle
import os
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import qlib
import yaml
from mlflow.entities import ViewType
from mlflow.tracking import MlflowClient

_conf_path = Path(__file__).resolve().parent / "conf_baseline.yaml"
if not _conf_path.exists():
    _conf_path = Path(__file__).resolve().parent / "conf.yaml"
if not _conf_path.exists():
    _conf_path = Path.cwd() / "conf_baseline.yaml"
if not _conf_path.exists():
    _conf_path = Path.cwd() / "conf.yaml"

_provider_uri = None
_region = None
_conf_obj = None
if _conf_path.exists():
    try:
        _conf_obj = yaml.safe_load(_conf_path.read_text(encoding="utf-8"))
        _qi = (_conf_obj or {}).get("qlib_init", {})
        _provider_uri = _qi.get("provider_uri")
        _region = _qi.get("region")
    except Exception:
        _conf_obj = None
        _provider_uri = None
        _region = None

if _provider_uri and _region:
    qlib.init(provider_uri=_provider_uri, region=_region)
else:
    qlib.init()

from qlib.workflow import R

# here is the documents of the https://qlib.readthedocs.io/en/latest/component/recorder.html

# TODO: list all the recorder and metrics

_cwd = Path.cwd()
_tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
if not _tracking_uri:
    _local_mlruns = _cwd / "mlruns"
    if _local_mlruns.exists():
        os.environ["MLFLOW_TRACKING_URI"] = str(_local_mlruns)

# Assuming you have already listed the experiments
experiments = R.list_experiments()


def _truthy_env(name: str) -> bool:
    return str(os.environ.get(name, "")).strip().lower() in {"1", "true", "yes", "on"}


def _load_bound_recorder_ref() -> dict:
    """Load the recorder id that belongs to this QE loop, if available."""
    env_rid = os.environ.get("QE_RECORDER_ID")
    if env_rid and env_rid.strip():
        return {"recorder_id": env_rid.strip(), "source": "env:QE_RECORDER_ID"}

    json_path = Path.cwd() / "qe_current_recorder.json"
    if json_path.exists():
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as exc:
            if _truthy_env("QE_REQUIRE_RECORDER_ID"):
                raise SystemExit(f"ERROR: failed to parse {json_path}: {exc}")
            print(f"Warning: failed to parse {json_path}: {exc}; falling back to legacy latest-recorder mode")
        else:
            rid = str(payload.get("recorder_id") or payload.get("id") or "").strip()
            if rid:
                payload = dict(payload)
                payload["recorder_id"] = rid
                payload["source"] = str(json_path)
                return payload
            if _truthy_env("QE_REQUIRE_RECORDER_ID"):
                raise SystemExit(f"ERROR: {json_path} does not contain recorder_id")
            print(f"Warning: {json_path} does not contain recorder_id; falling back to legacy latest-recorder mode")

    txt_path = Path.cwd() / "qe_recorder_id.txt"
    if txt_path.exists():
        rid = txt_path.read_text(encoding="utf-8").strip()
        if rid:
            return {"recorder_id": rid, "source": str(txt_path)}

    return {}


def _recorder_id_matches(recorder_id: str, target_rid: str) -> bool:
    return recorder_id == target_rid or recorder_id.startswith(target_rid)


def _write_extracted_recorder_ref(recorder, experiment_name: str, binding: dict) -> None:
    if not binding and not _truthy_env("QE_WRITE_EXTRACTED_RECORDER"):
        return
    info = getattr(recorder, "info", {}) or {}
    payload = {
        "schema_version": 1,
        "selected_recorder_id": str(info.get("id") or ""),
        "selected_experiment_name": experiment_name,
        "selected_experiment_id": str(info.get("experiment_id") or ""),
        "binding_source": binding.get("source"),
        "target_recorder_id": binding.get("recorder_id"),
        "strict_required": _truthy_env("QE_REQUIRE_RECORDER_ID"),
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    path = Path.cwd() / "qe_extracted_recorder.json"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


_binding = _load_bound_recorder_ref()
_target_rid = str(_binding.get("recorder_id") or "").strip()
_require_bound_recorder = _truthy_env("QE_REQUIRE_RECORDER_ID")

if _require_bound_recorder and not _target_rid:
    raise SystemExit(
        "ERROR: QE_REQUIRE_RECORDER_ID=1 but no QE_RECORDER_ID, qe_current_recorder.json, "
        "or qe_recorder_id.txt was found. Refusing legacy latest-recorder fallback."
    )

experiment_name = None
latest_recorder = None
matched_recorders = []
scan_experiments = experiments
# Scan all visible experiments when a recorder id is bound. Concurrent first-use
# MLflow file-store creation can leave duplicate experiment names, so narrowing
# by name can hide the target recorder even when the id is correct.

for experiment in scan_experiments:
    recorders = R.list_recorders(experiment_name=experiment)
    for recorder_id in recorders:
        if recorder_id is None:
            continue
        try:
            recorder = R.get_recorder(recorder_id=recorder_id, experiment_name=experiment)
            if _target_rid:
                if _recorder_id_matches(str(recorder_id), _target_rid):
                    matched_recorders.append((experiment, recorder_id, recorder))
                continue

            end_time = recorder.info.get("end_time")
            if end_time is not None:
                if latest_recorder is None or end_time > latest_recorder.info["end_time"]:
                    latest_recorder = recorder
                    experiment_name = experiment
            else:
                print(f"Warning: Recorder {recorder_id} has no valid end time")
        except Exception as e:
            print(f"Error: {e}")

if _target_rid:
    if len(matched_recorders) == 1:
        experiment_name, _selected_recorder_id, latest_recorder = matched_recorders[0]
        print(
            f"Bound recorder selected: recorder_id={_selected_recorder_id} "
            f"experiment={experiment_name} source={_binding.get('source')}"
        )
    elif len(matched_recorders) == 0:
        raise SystemExit(
            f"ERROR: target recorder {_target_rid} from {_binding.get('source')} was not found; "
            "refusing to extract another recorder."
        )
    else:
        matches = ", ".join(f"{exp}/{rid}" for exp, rid, _ in matched_recorders[:10])
        raise SystemExit(
            f"ERROR: target recorder prefix {_target_rid} matched multiple recorders: {matches}. "
            "Use a full recorder id."
        )
elif not _require_bound_recorder:
    print("Warning: no bound recorder id found; using legacy latest-recorder fallback for old experiments")

# Check if the latest recorder is found
if latest_recorder is None:
    print("No recorders found")
else:
    print(f"Latest recorder: {latest_recorder}")
    _write_extracted_recorder_ref(latest_recorder, experiment_name or "", _binding)

    # Load the specified file from the latest recorder
    metrics = pd.Series(latest_recorder.list_metrics())

    output_path = Path.cwd() / "qlib_res.csv"
    metrics.to_csv(output_path)

    print(f"Output has been saved to {output_path}")

    try:
        ret_data_frame = latest_recorder.load_object("portfolio_analysis/report_normal_1day.pkl")
        ret_data_frame.to_pickle("ret.pkl")

        def _normalize_ret_df(obj: object) -> pd.DataFrame:
            if isinstance(obj, pd.Series):
                df = obj.to_frame(name=obj.name or "value")
            elif isinstance(obj, pd.DataFrame):
                df = obj
            else:
                try:
                    df = pd.DataFrame(obj)  # type: ignore[arg-type]
                except Exception:
                    df = pd.DataFrame({"value": [obj]})

            if isinstance(df.index, pd.MultiIndex):
                idx_names = [n if n else f"index_{i}" for i, n in enumerate(df.index.names)]
                df = df.reset_index()
                for n in idx_names:
                    if n in df.columns:
                        if pd.api.types.is_datetime64_any_dtype(df[n]):
                            df[n] = pd.to_datetime(df[n], utc=True, errors="coerce")
            else:
                idx_name = df.index.name if df.index.name else "index"
                df = df.reset_index().rename(columns={"index": idx_name})
                if idx_name in df.columns and pd.api.types.is_datetime64_any_dtype(df[idx_name]):
                    df[idx_name] = pd.to_datetime(df[idx_name], utc=True, errors="coerce")

            for c in list(df.columns):
                if df[c].dtype == object:
                    try:
                        df[c] = pd.to_numeric(df[c], errors="ignore")
                    except Exception:
                        pass
            return df

        ret_schema_df = _normalize_ret_df(ret_data_frame)

        try:
            ret_schema_df.to_parquet("ret_schema.parquet", index=False)
        except Exception as e:
            print(f"Warning: failed to write ret_schema.parquet: {e}")

        try:
            Path("ret_schema.json").write_text(
                ret_schema_df.to_json(orient="table", date_format="iso"),
                encoding="utf-8",
            )
        except Exception as e:
            print(f"Warning: failed to write ret_schema.json: {e}")

        try:
            pred_obj = latest_recorder.load_object("pred.pkl")

            def _normalize_pred_df(obj: object) -> pd.DataFrame:
                if isinstance(obj, pd.Series):
                    df = obj.to_frame(name=obj.name or "score")
                elif isinstance(obj, pd.DataFrame):
                    df = obj
                else:
                    df = pd.DataFrame(obj)  # type: ignore[arg-type]

                if "score" not in df.columns:
                    if df.shape[1] >= 1:
                        df = df.rename(columns={df.columns[0]: "score"})
                    else:
                        df["score"] = pd.NA

                if isinstance(df.index, pd.MultiIndex):
                    idx_names = [n if n else f"index_{i}" for i, n in enumerate(df.index.names)]
                    df = df.reset_index()
                    cols = set(df.columns)
                    if "datetime" not in cols:
                        for n in idx_names:
                            if "date" in str(n).lower() and n in cols:
                                df = df.rename(columns={n: "datetime"})
                                break
                    if "instrument" not in cols:
                        for n in idx_names:
                            if "inst" in str(n).lower() and n in cols:
                                df = df.rename(columns={n: "instrument"})
                                break
                else:
                    idx_name = df.index.name if df.index.name else "index"
                    df = df.reset_index().rename(columns={"index": idx_name})

                if "datetime" in df.columns:
                    df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
                return df

            pred_df = _normalize_pred_df(pred_obj)

            _topk = 50
            _n_drop = 0
            try:
                _pa = (_conf_obj or {}).get("port_analysis_config", {})
                _st = (_pa or {}).get("strategy", {})
                _kw = (_st or {}).get("kwargs", {})
                if isinstance(_kw, dict):
                    _topk = int(_kw.get("topk", _topk))
                    _n_drop = int(_kw.get("n_drop", _n_drop))
            except Exception:
                _topk = 50
                _n_drop = 0

            if "datetime" not in pred_df.columns or "instrument" not in pred_df.columns:
                raise ValueError("pred.pkl missing required index columns for signals (datetime/instrument)")

            pred_df = pred_df[["datetime", "instrument", "score"]].copy()
            pred_df = pred_df.dropna(subset=["datetime", "instrument"]).copy()

            pred_df["trade_date"] = pred_df["datetime"].dt.date.astype(str)
            pred_df["score"] = pd.to_numeric(pred_df["score"], errors="coerce")

            pred_df["rank"] = (
                pred_df.groupby("trade_date")["score"].rank(ascending=False, method="first").astype("Int64")
            )

            topk_df = pred_df[pred_df["rank"].notna() & (pred_df["rank"] <= _topk)].copy()

            topk_df["signal"] = topk_df["score"]
            topk_df["target_weight"] = 1.0 / float(_topk) if _topk > 0 else 0.0
            topk_df["target_position"] = pd.NA
            topk_df["price_ref"] = pd.NA
            topk_df["universe_flag"] = 1
            topk_df["pred_return"] = topk_df["score"]
            topk_df["confidence"] = pd.NA
            topk_df["volatility_est"] = pd.NA
            topk_df["max_weight"] = pd.NA
            topk_df["min_weight"] = pd.NA
            topk_df["sector"] = pd.NA
            topk_df["industry"] = pd.NA

            now_utc = datetime.now(tz=timezone.utc).isoformat()
            topk_df["generated_at_utc"] = now_utc
            topk_df["task_run_id"] = pd.NA
            topk_df["loop_id"] = pd.NA
            topk_df["workspace_id"] = pd.NA
            topk_df["model_version"] = pd.NA

            topk_df["weight_method"] = "topk_equal_weight"
            topk_df["topk"] = _topk
            topk_df["n_drop"] = _n_drop
            topk_df["rebalance_freq"] = "1d"

            signals_cols = [
                "trade_date",
                "instrument",
                "signal",
                "target_weight",
                "target_position",
                "price_ref",
                "universe_flag",
                "score",
                "rank",
                "pred_return",
                "confidence",
                "volatility_est",
                "max_weight",
                "min_weight",
                "sector",
                "industry",
                "generated_at_utc",
                "task_run_id",
                "loop_id",
                "workspace_id",
                "model_version",
            ]
            weight_meta_cols = ["weight_method", "topk", "n_drop", "rebalance_freq"]

            signals_df = topk_df[signals_cols + weight_meta_cols].copy()

            try:
                signals_df.to_parquet("signals.parquet", index=False)
            except Exception as e:
                print(f"Warning: failed to write signals.parquet: {e}")

            try:
                Path("signals.json").write_text(
                    signals_df.to_json(orient="table", date_format="iso"),
                    encoding="utf-8",
                )
            except Exception as e:
                print(f"Warning: failed to write signals.json: {e}")
        except Exception as e:
            print(f"Warning: failed to generate signals from pred.pkl: {e}")
    except Exception as e:
        print(f"Warning: failed to load portfolio_analysis/report_normal_1day.pkl: {e}")

# ============================================================
# Phase 3: Enhanced Diagnostics Output
# ============================================================
import json as _json
import re as _re
import math as _math

import numpy as _np


def _json_safe(obj):
    """Convert numpy/pandas types to JSON-serializable Python types."""
    if isinstance(obj, (_np.integer,)):
        return int(obj)
    if isinstance(obj, (_np.floating,)):
        v = float(obj)
        if _math.isnan(v) or _math.isinf(v):
            return None
        return v
    if isinstance(obj, _np.ndarray):
        return [_json_safe(x) for x in obj.tolist()]
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, (pd.Series, pd.Index)):
        return [_json_safe(x) for x in obj.tolist()]
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, float):
        if _math.isnan(obj) or _math.isinf(obj):
            return None
        return obj
    return obj


def _extract_ic_diagnostics(recorder) -> dict:
    """Extract IC time series diagnostics from sig_analysis pkl files."""
    result = {}
    try:
        ic_obj = recorder.load_object("sig_analysis/ic.pkl")
        if isinstance(ic_obj, pd.Series):
            ic_s = ic_obj.dropna()
        elif isinstance(ic_obj, pd.DataFrame):
            ic_s = ic_obj.iloc[:, 0].dropna()
        else:
            ic_s = pd.Series(ic_obj).dropna()

        dates = [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in ic_s.index]
        vals = ic_s.values.astype(float)
        rolling_mean = pd.Series(vals).rolling(30, min_periods=5).mean().tolist()
        rolling_std = pd.Series(vals).rolling(30, min_periods=5).std().tolist()

        result["ic_series"] = vals.tolist()
        result["ic_dates"] = dates
        result["ic_mean"] = float(vals.mean())
        result["ic_std"] = float(vals.std())
        result["ic_positive_ratio"] = float((vals > 0).sum() / len(vals)) if len(vals) > 0 else None
        result["ic_rolling_30d_mean"] = rolling_mean
        result["ic_rolling_30d_std"] = rolling_std
    except Exception as e:
        print(f"Warning: failed to extract IC diagnostics: {e}")

    try:
        ric_obj = recorder.load_object("sig_analysis/ric.pkl")
        if isinstance(ric_obj, pd.Series):
            ric_s = ric_obj.dropna()
        elif isinstance(ric_obj, pd.DataFrame):
            ric_s = ric_obj.iloc[:, 0].dropna()
        else:
            ric_s = pd.Series(ric_obj).dropna()

        ric_vals = ric_s.values.astype(float)
        result["rank_ic_series"] = ric_vals.tolist()
        result["rank_ic_mean"] = float(ric_vals.mean())
        result["rank_ic_std"] = float(ric_vals.std())
        result["rank_ic_positive_ratio"] = (
            float((ric_vals > 0).sum() / len(ric_vals)) if len(ric_vals) > 0 else None
        )
    except Exception as e:
        print(f"Warning: failed to extract Rank IC diagnostics: {e}")

    return result


def _extract_training_diagnostics() -> dict:
    """Extract training loss curves from stdout/log files via regex."""
    result = {}
    # Search for log files in current directory
    log_candidates = list(Path.cwd().glob("*.log")) + list(Path.cwd().glob("*.txt"))
    # Also check stdout captured by RD-Agent
    for name in ("stdout.log", "output.log", "run.log", "exp_output.log"):
        p = Path.cwd() / name
        if p.exists() and p not in log_candidates:
            log_candidates.append(p)

    all_text = ""
    for lf in log_candidates:
        try:
            all_text += lf.read_text(encoding="utf-8", errors="replace") + "\n"
        except Exception:
            pass

    if not all_text:
        return result

    # Pattern 1: PyTorch-style "Epoch X: train_loss=Y, valid_loss=Z"
    train_losses, val_losses = [], []
    for m in _re.finditer(
        r"[Ee]poch\s*(\d+).*?train[_ ]?loss[=:\s]+([\d.]+).*?val(?:id)?[_ ]?loss[=:\s]+([\d.]+)",
        all_text,
    ):
        train_losses.append(float(m.group(2)))
        val_losses.append(float(m.group(3)))

    # Pattern 2: qlib TabNet/ALSTM style "[Epoch N]: train_loss=X, valid_loss=Y"
    if not train_losses:
        for m in _re.finditer(
            r"\[Epoch\s+(\d+)\].*?train_loss[=:\s]+([\d.eE+-]+).*?valid_loss[=:\s]+([\d.eE+-]+)",
            all_text,
        ):
            train_losses.append(float(m.group(2)))
            val_losses.append(float(m.group(3)))

    # Pattern 3: generic "epoch N ... loss X ... val Y"
    if not train_losses:
        for m in _re.finditer(
            r"epoch[:\s]+(\d+).*?loss[:\s]+([\d.eE+-]+).*?(?:val|test)[_ ]?loss[:\s]+([\d.eE+-]+)",
            all_text, _re.IGNORECASE,
        ):
            train_losses.append(float(m.group(2)))
            val_losses.append(float(m.group(3)))

    # Pattern 4: GeneralPTNN style "Epoch0: train 0.994649, valid 0.992367" (single line)
    if not train_losses:
        for m in _re.finditer(
            r"[Ee]poch\s*(\d+):\s*train\s+([\d.eE+-]+),?\s*valid\s+([\d.eE+-]+)",
            all_text,
        ):
            train_losses.append(float(m.group(2)))
            val_losses.append(float(m.group(3)))

    # Pattern 5: Most Qlib models (ALSTM/GRU/LSTM/GATs/TCN/Transformer/TabNet/etc.)
    # print "train X, valid Y" on a standalone line (Epoch header is on a separate line)
    if not train_losses:
        for m in _re.finditer(
            r"^train\s+([\d.eE+-]+),?\s*valid\s+([\d.eE+-]+)",
            all_text, _re.MULTILINE,
        ):
            train_losses.append(float(m.group(1)))
            val_losses.append(float(m.group(2)))

    # Pattern 6: GeneralPTNN with skipped train loss
    # "Epoch0: train N/A (skipped), valid 0.999092"
    if not train_losses and not val_losses:
        for m in _re.finditer(
            r"[Ee]poch\s*(\d+):\s*train\s+N/A[^,]*,\s*valid\s+([\d.eE+-]+)",
            all_text,
        ):
            val_losses.append(float(m.group(2)))

    if train_losses or val_losses:
        result["val_loss_curve"] = val_losses if val_losses else []
        result["total_epochs"] = len(train_losses) if train_losses else len(val_losses)
        total_epochs = result["total_epochs"]

        if val_losses:
            best_epoch = int(_np.argmin(val_losses)) + 1
        elif train_losses:
            best_epoch = len(train_losses)
        else:
            best_epoch = total_epochs
        result["best_epoch"] = best_epoch
        result["convergence_ratio"] = round(best_epoch / total_epochs, 4) if total_epochs else None

        if train_losses:
            result["train_loss_curve"] = train_losses
            result["final_train_loss"] = train_losses[-1]
            result["final_val_loss"] = val_losses[-1] if val_losses else None
            if train_losses[-1] > 0 and val_losses:
                result["overfit_ratio"] = round(val_losses[-1] / train_losses[-1], 4)
            else:
                result["overfit_ratio"] = None
        else:
            # train loss skipped for speed; infer from val_loss curve only
            result["final_val_loss"] = val_losses[-1] if val_losses else None
            result["overfit_ratio"] = None
            # Infer training_failed: val loss never improved meaningfully (>0.1%)
            if len(val_losses) >= 3:
                first_val = val_losses[0]
                best_val = min(val_losses)
                improvement = (first_val - best_val) / (abs(first_val) + 1e-9)
                result["training_failed"] = bool(improvement < 0.001)
            else:
                result["training_failed"] = True

        # Check early stopping
        result["early_stop_triggered"] = False
        if _re.search(r"early.?stop", all_text, _re.IGNORECASE):
            result["early_stop_triggered"] = True

    return result


def _extract_return_curves(recorder) -> dict:
    """Extract cumulative return/NAV curves from report_normal_1day.pkl."""
    result = {}
    try:
        report = recorder.load_object("portfolio_analysis/report_normal_1day.pkl")
        if isinstance(report, pd.DataFrame):
            df = report
        elif isinstance(report, pd.Series):
            df = report.to_frame()
        else:
            return result

        # --- Date extraction ---
        # DatetimeIndex is NOT in df.columns; detect it before reset_index
        if isinstance(df.index, pd.DatetimeIndex):
            dates = df.index.strftime("%Y-%m-%d").tolist()
        elif isinstance(df.index, pd.MultiIndex):
            # Try to find a datetime level
            dt_level = None
            for i, level in enumerate(df.index.levels):
                if isinstance(level, pd.DatetimeIndex):
                    dt_level = i
                    break
            if dt_level is not None:
                dates = [idx[dt_level].strftime("%Y-%m-%d") if hasattr(idx[dt_level], "strftime") else str(idx[dt_level]) for idx in df.index]
            else:
                dates = [str(i) for i in range(len(df))]
        else:
            # Plain RangeIndex or other – try parsing as datetime
            try:
                dates = pd.to_datetime(df.index).strftime("%Y-%m-%d").tolist()
            except Exception:
                dates = [str(i) for i in range(len(df))]

        result["dates"] = dates

        # --- Cumulative excess returns (kept for backward compat) ---
        col_lower = {str(c).lower(): c for c in df.columns}

        for src_key, dst_key in [
            ("excess_return_without_cost", "cumulative_excess_no_cost"),
            ("excess_return_with_cost", "cumulative_excess_with_cost"),
        ]:
            matched = [col_lower[k] for k in col_lower if src_key in k]
            if matched:
                series = pd.to_numeric(df[matched[0]], errors="coerce").fillna(0)
                result[dst_key] = (1 + series).cumprod().subtract(1).tolist()

        # --- Portfolio NAV (from "return" column = portfolio daily return) ---
        ret_cols = [col_lower[k] for k in col_lower if k == "return"]
        if ret_cols:
            port_ret = pd.to_numeric(df[ret_cols[0]], errors="coerce").fillna(0)
            result["cumulative_portfolio"] = (1 + port_ret).cumprod().tolist()

            # Portfolio NAV with cost = return - cost_rate
            cost_cols = [col_lower[k] for k in col_lower if "cost" in k and "excess" not in k and "return" not in k]
            if cost_cols:
                cost_s = pd.to_numeric(df[cost_cols[0]], errors="coerce").fillna(0)
                result["cumulative_portfolio_with_cost"] = (1 + port_ret - cost_s.abs()).cumprod().tolist()
            elif "cumulative_excess_with_cost" in result and "cumulative_excess_no_cost" in result:
                # Derive from excess curves: nav_with_cost = nav * (1+excess_with_cost)/(1+excess_no_cost)
                exc_wc = _np.array(result["cumulative_excess_with_cost"])
                exc_nc = _np.array(result["cumulative_excess_no_cost"])
                nav = _np.array(result["cumulative_portfolio"])
                denom = exc_nc + 1
                denom[denom == 0] = 1
                result["cumulative_portfolio_with_cost"] = (nav * (exc_wc + 1) / denom).tolist()

        # --- Benchmark NAV (from "bench" column if present, fallback to parquet) ---
        bench_ret = None
        bench_cols = [col_lower[k] for k in col_lower if k == "bench"]
        if bench_cols:
            _raw_bench = pd.to_numeric(df[bench_cols[0]], errors="coerce")
            if _raw_bench.notna().any():
                bench_ret = _raw_bench.fillna(0)

        # Fallback: bench 列全为 None/NaN（benchmark=~ 导致），从本地 parquet 加载
        if bench_ret is None:
            _bench_path = Path(__file__).parent / "benchmark_sh000300.parquet"
            if _bench_path.exists():
                try:
                    _bench_df = pd.read_parquet(_bench_path)
                    _bench_sr = _bench_df["bench"]
                    _bench_sr.index = pd.to_datetime(_bench_sr.index)
                    # 对齐到 report 日期
                    _report_dates = pd.to_datetime(df.index)
                    _bench_aligned = _bench_sr.reindex(_report_dates).fillna(0)
                    bench_ret = _bench_aligned
                    print(f"[INFO] Loaded benchmark from parquet fallback: {_bench_path.name}")
                except Exception as _e:
                    print(f"Warning: failed to load benchmark parquet: {_e}")

        if bench_ret is not None:
            result["cumulative_benchmark"] = (1 + bench_ret).cumprod().tolist()
            # 补算超额收益（当 Qlib 原生 excess_return 为 NaN 时）
            if "cumulative_excess_no_cost" not in result:
                ret_cols_ex = [col_lower[k] for k in col_lower if k == "return"]
                if ret_cols_ex:
                    _port_ret = pd.to_numeric(df[ret_cols_ex[0]], errors="coerce").fillna(0)
                    _excess_ret = _port_ret - bench_ret
                    result["cumulative_excess_no_cost"] = (1 + _excess_ret).cumprod().subtract(1).tolist()
                    result["cumulative_excess_with_cost"] = result["cumulative_excess_no_cost"]  # NestedExecutor 模式下 cost 已扣
                    result["_benchmark_fallback"] = True

        # --- Drawdown from portfolio NAV with cost (or excess_with_cost) ---
        dd_source = None
        if "cumulative_portfolio_with_cost" in result:
            dd_source = _np.array(result["cumulative_portfolio_with_cost"])
        elif "cumulative_portfolio" in result:
            dd_source = _np.array(result["cumulative_portfolio"])
        elif "cumulative_excess_with_cost" in result:
            dd_source = _np.array(result["cumulative_excess_with_cost"]) + 1

        if dd_source is not None:
            running_max = _np.maximum.accumulate(dd_source)
            running_max[running_max == 0] = 1
            dd = dd_source / running_max - 1
            result["drawdown_series"] = dd.tolist()

    except Exception as e:
        print(f"Warning: failed to extract return curves: {e}")
    return result


def _positions_to_dict(holdings) -> dict:
    """Normalize qlib Position object or plain dict to {stock: {amount, price}, cash: float}.

    Handles two formats:
    1. Plain dict: {"cash": float, "SH600000": {"amount": int, "price": float}}
    2. qlib Position object: has .position attr → {"cash": float, "now_account_value": float, "SH600000": {"amount": float, "price": float, "weight": float}}
    """
    if isinstance(holdings, dict):
        return holdings
    if hasattr(holdings, "position") and isinstance(holdings.position, dict):
        return holdings.position
    return {}


def _extract_trade_diagnostics(recorder, summary_metrics: dict) -> dict:
    """Extract trading efficiency diagnostics from positions and report."""
    result = {}
    try:
        # --- avg_turnover ---
        # Priority 1: summary metrics (most reliable when present)
        turnover_key = [k for k in summary_metrics if "turnover" in str(k).lower()]
        if turnover_key:
            val = float(summary_metrics[turnover_key[0]])
            if val > 0:
                result["avg_turnover"] = val

        # Priority 2: report total_turnover column (limit_threshold mode has turnover=0 but total_turnover valid)
        if "avg_turnover" not in result:
            try:
                report = recorder.load_object("portfolio_analysis/report_normal_1day.pkl")
                if isinstance(report, pd.DataFrame):
                    for col_name in ["total_turnover", "turnover"]:
                        if col_name in report.columns:
                            val = report[col_name].dropna().mean()
                            if val > 0:
                                result["avg_turnover"] = round(float(val), 6)
                                break
            except Exception as e:
                print(f"Warning: failed to load report for turnover: {e}")

        # --- cost_drag_annualized & daily_trade_count from indicators ---
        try:
            indicators = recorder.load_object("portfolio_analysis/indicators_normal_1day.pkl")
            if isinstance(indicators, pd.DataFrame):
                # daily_trade_count from indicators count column (priority over positions)
                if "count" in indicators.columns:
                    count_vals = indicators["count"].dropna()
                    if len(count_vals) > 0:
                        result["daily_trade_count_avg"] = round(float(count_vals.mean()), 2)

                # cost drag from total_cost / value
                if "total_cost" in indicators.columns and "value" in indicators.columns:
                    total_cost = abs(float(indicators["total_cost"].dropna().sum()))
                    avg_value = float(indicators["value"].dropna().mean())
                    if avg_value > 0 and total_cost > 0:
                        n_days = len(indicators)
                        annual_cost = (total_cost / avg_value) * (252 / max(n_days, 1))
                        result["cost_drag_annualized"] = round(annual_cost, 6)
        except Exception as e:
            print(f"Warning: failed to load indicators for cost_drag/trade_count: {e}")

        # cost_drag fallback: from summary annualized returns difference
        if "cost_drag_annualized" not in result:
            ann_no_cost = summary_metrics.get("1day.excess_return_without_cost.annualized_return")
            ann_with_cost = summary_metrics.get("1day.excess_return_with_cost.annualized_return")
            if ann_no_cost is not None and ann_with_cost is not None:
                drag = float(ann_no_cost) - float(ann_with_cost)
                if abs(drag) > 1e-9:
                    result["cost_drag_annualized"] = round(drag, 6)

        # --- positions fallback (load once, used for both trade_count and turnover) ---
        pos_obj = None
        if "daily_trade_count_avg" not in result or "avg_turnover" not in result:
            try:
                pos_obj = recorder.load_object("portfolio_analysis/positions_normal_1day.pkl")
            except Exception as e:
                print(f"Warning: failed to load positions for trade diagnostics: {e}")

        _SKIP_KEYS = {"cash", "now_account_value"}

        if pos_obj is not None and isinstance(pos_obj, dict):
            # daily_trade_count from positions
            if "daily_trade_count_avg" not in result:
                counts = []
                for dt, raw_holdings in pos_obj.items():
                    holdings = _positions_to_dict(raw_holdings)
                    n = sum(1 for k, v in holdings.items() if k not in _SKIP_KEYS and isinstance(v, dict) and v.get("amount", 0) != 0)
                    counts.append(n)
                if counts:
                    result["daily_trade_count_avg"] = round(float(_np.mean(counts)), 2)

            # avg_turnover from position weight changes (last resort)
            if "avg_turnover" not in result:
                daily_weights = {}
                for dt, raw_holdings in sorted(pos_obj.items()):
                    holdings = _positions_to_dict(raw_holdings)
                    if not holdings:
                        continue
                    total_val = sum(
                        float(v["amount"]) * float(v["price"]) for k, v in holdings.items()
                        if k not in _SKIP_KEYS and isinstance(v, dict) and "amount" in v and "price" in v
                    ) + float(holdings.get("cash", 0))
                    if total_val <= 0:
                        continue
                    weights = {}
                    for k, v in holdings.items():
                        if k not in _SKIP_KEYS and isinstance(v, dict) and "amount" in v and "price" in v:
                            weights[k] = float(v["amount"]) * float(v["price"]) / total_val
                    daily_weights[dt] = weights

                dates_sorted = sorted(daily_weights.keys())
                if len(dates_sorted) > 1:
                    turnovers = []
                    for i in range(1, len(dates_sorted)):
                        prev_w = daily_weights[dates_sorted[i - 1]]
                        curr_w = daily_weights[dates_sorted[i]]
                        all_keys = set(prev_w) | set(curr_w)
                        turnover = sum(abs(curr_w.get(k, 0) - prev_w.get(k, 0)) for k in all_keys) / 2
                        turnovers.append(turnover)
                    result["avg_turnover"] = round(float(_np.mean(turnovers)), 6)

        # --- total_turnover & annualized_turnover ---
        if "avg_turnover" in result:
            avg_to = result["avg_turnover"]
            # Determine n_trading_days from positions or report
            n_trading = 0
            if pos_obj and isinstance(pos_obj, dict):
                n_trading = len(pos_obj)
            elif "daily_trade_count_avg" in result:
                # Fallback: not precise but better than nothing
                n_trading = 252
            if n_trading > 0:
                result["total_turnover"] = round(avg_to * n_trading, 4)
                result["annualized_turnover"] = round(avg_to * 252, 4)

    except Exception as e:
        print(f"Warning: failed to extract trade diagnostics: {e}")
    return result


def _compute_cagr(recorder) -> dict:
    """Compute CAGR (Compound Annual Growth Rate) from portfolio NAV."""
    result = {}
    try:
        report = recorder.load_object("portfolio_analysis/report_normal_1day.pkl")
        if isinstance(report, pd.DataFrame) and "return" in report.columns:
            ret = pd.to_numeric(report["return"], errors="coerce").fillna(0)
            nav = (1 + ret).cumprod()
            final_nav = float(nav.iloc[-1])
            n_days = len(nav)
            if n_days > 0 and final_nav > 0:
                cagr = final_nav ** (252.0 / n_days) - 1
                result["cagr"] = round(float(cagr), 6)
                result["final_nav"] = round(final_nav, 6)
                result["n_trading_days"] = n_days
    except Exception as e:
        print(f"Warning: failed to compute CAGR: {e}")
    return result


def _extract_top_stocks(recorder) -> dict:
    """Extract top 10 profitable and bottom 10 losing stocks.

    Uses average-cost accounting across daily position snapshots:
    - Buy  (amount increases): new shares added at that day's price, avg_cost recalculated
    - Sell (amount decreases): realized P&L = sold_shares * (sell_price - avg_cost),
      cost basis reduced proportionally, avg_cost per share unchanged
    - Full exit (amount → 0): all remaining cost settled as realized P&L
    - Re-entry after exit: treated as a fresh buy batch on top of any prior realized P&L

    profit     = realized_pnl + unrealized_pnl (remaining_amount * (last_price - avg_cost))
    profit_pct = total_profit / total_cost_invested
    """
    result = {}
    try:
        pos_obj = recorder.load_object("portfolio_analysis/positions_normal_1day.pkl")
        if not isinstance(pos_obj, dict) or len(pos_obj) == 0:
            return result

        _SKIP_KEYS = {"cash", "now_account_value"}

        # ── Per-stock state ──
        stock_first_date = {}      # first date with holding
        stock_last_date = {}       # last date with holding
        stock_holding_days = {}    # actual days held (count of snapshots with amount > 0)
        stock_trades = {}          # per-stock trade events [{date, type, price, amount, pnl}]
        # Cost tracking (moving-average cost method)
        stock_cost_basis = {}      # current total cost of remaining shares
        stock_amount = {}          # current holding amount
        stock_avg_cost = {}        # cost_basis / amount (per share)
        stock_realized_pnl = {}    # cumulative realized P&L from sells
        stock_total_invested = {}  # cumulative total buy cost (for pct denominator)
        stock_last_price = {}      # last observed price

        sorted_dates = sorted(pos_obj.keys())
        for dt in sorted_dates:
            holdings = _positions_to_dict(pos_obj[dt])
            if not holdings:
                continue
            dt_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)

            # Stocks present today
            today_stocks = set()
            for stock, info in holdings.items():
                if stock in _SKIP_KEYS or not isinstance(info, dict):
                    continue
                amount = float(info.get("amount", 0))
                price = float(info.get("price", 0))
                if amount == 0:
                    continue
                today_stocks.add(stock)

                if stock not in stock_first_date:
                    stock_first_date[stock] = dt_str
                stock_last_date[stock] = dt_str
                stock_last_price[stock] = price
                stock_holding_days[stock] = stock_holding_days.get(stock, 0) + 1

                prev_amt = stock_amount.get(stock, 0.0)

                if prev_amt == 0:
                    # New position or re-entry after full exit
                    cost = amount * price
                    stock_cost_basis[stock] = cost
                    stock_amount[stock] = amount
                    stock_avg_cost[stock] = price
                    stock_total_invested[stock] = stock_total_invested.get(stock, 0.0) + cost
                    if stock not in stock_trades:
                        stock_trades[stock] = []
                    stock_trades[stock].append({"date": dt_str, "type": "buy", "price": round(price, 4), "amount": round(cost, 2), "pnl": None})
                elif amount > prev_amt:
                    # Buy / add to position
                    added = amount - prev_amt
                    add_cost = added * price
                    stock_cost_basis[stock] = stock_cost_basis.get(stock, 0.0) + add_cost
                    stock_amount[stock] = amount
                    stock_avg_cost[stock] = stock_cost_basis[stock] / amount
                    stock_total_invested[stock] = stock_total_invested.get(stock, 0.0) + add_cost
                    if stock not in stock_trades:
                        stock_trades[stock] = []
                    stock_trades[stock].append({"date": dt_str, "type": "buy", "price": round(price, 4), "amount": round(add_cost, 2), "pnl": None})
                elif amount < prev_amt:
                    # Sell / reduce position
                    sold = prev_amt - amount
                    avg_c = stock_avg_cost.get(stock, 0.0)
                    realized = sold * (price - avg_c)
                    stock_realized_pnl[stock] = stock_realized_pnl.get(stock, 0.0) + realized
                    # Reduce cost basis proportionally (avg_cost per share unchanged)
                    stock_cost_basis[stock] = avg_c * amount
                    stock_amount[stock] = amount
                    # avg_cost stays the same
                    if stock not in stock_trades:
                        stock_trades[stock] = []
                    stock_trades[stock].append({"date": dt_str, "type": "sell", "price": round(price, 4), "amount": round(sold * price, 2), "pnl": round(realized, 2)})
                else:
                    # Same amount, just update tracking
                    stock_amount[stock] = amount

            # Detect full exits: stocks held yesterday but absent today
            for stock in list(stock_amount.keys()):
                if stock_amount[stock] > 0 and stock not in today_stocks:
                    # Fully exited — we don't have today's sell price from positions,
                    # use last known price as approximation
                    amt = stock_amount[stock]
                    avg_c = stock_avg_cost.get(stock, 0.0)
                    last_p = stock_last_price.get(stock, 0.0)
                    realized = amt * (last_p - avg_c)
                    stock_realized_pnl[stock] = stock_realized_pnl.get(stock, 0.0) + realized
                    stock_cost_basis[stock] = 0.0
                    stock_amount[stock] = 0.0
                    if stock not in stock_trades:
                        stock_trades[stock] = []
                    stock_trades[stock].append({"date": dt_str, "type": "sell", "price": round(last_p, 4), "amount": round(amt * last_p, 2), "pnl": round(realized, 2)})

        # ── Compute total P&L per stock ──
        stock_pnl = {}
        for stock in stock_first_date:
            realized = stock_realized_pnl.get(stock, 0.0)
            amt = stock_amount.get(stock, 0.0)
            avg_c = stock_avg_cost.get(stock, 0.0)
            last_p = stock_last_price.get(stock, 0.0)
            unrealized = amt * (last_p - avg_c) if amt > 0 else 0.0
            stock_pnl[stock] = realized + unrealized

        def _build_stock_entry(stock, pnl):
            first_d = stock_first_date.get(stock, "")
            last_d = stock_last_date.get(stock, "")
            holding_days = stock_holding_days.get(stock, 0)
            avg_cost = stock_avg_cost.get(stock, 0.0)
            last_price = stock_last_price.get(stock, 0.0)
            total_invested = stock_total_invested.get(stock, 0.0)
            # profit_pct = total P&L / total cost invested over lifetime
            pct = pnl / total_invested if total_invested > 0 else 0.0

            return {
                "code": stock,
                "profit": round(pnl, 2),
                "profit_pct": round(pct, 6),
                "avg_cost": round(avg_cost, 4),
                "last_price": round(last_price, 4),
                "holding_days": holding_days,
                "first_date": first_d,
                "last_date": last_d,
            }

        # Top 10 gainers (P&L descending)
        sorted_desc = sorted(stock_pnl.items(), key=lambda x: x[1], reverse=True)[:10]
        result["top_stocks"] = [_build_stock_entry(s, p) for s, p in sorted_desc]

        # Bottom 10 (P&L ascending — worst performers, regardless of sign)
        sorted_asc = sorted(stock_pnl.items(), key=lambda x: x[1])[:10]
        result["bottom_stocks"] = [_build_stock_entry(s, p) for s, p in sorted_asc]

        # All stocks (for full holdings table in frontend, sorted by first_date)
        all_sorted = sorted(stock_pnl.items(), key=lambda x: stock_first_date.get(x[0], ""))
        result["all_stocks"] = [_build_stock_entry(s, p) for s, p in all_sorted]

        # All stock trade events (for click-to-expand in frontend)
        result["stock_trades"] = stock_trades

    except Exception as e:
        print(f"Warning: failed to extract top stocks: {e}")
    return result


def _extract_prediction_diagnostics(recorder) -> dict:
    """Extract prediction behavior diagnostics from pred.pkl."""
    result = {}
    try:
        pred_obj = recorder.load_object("pred.pkl")
        if isinstance(pred_obj, pd.Series):
            pdf = pred_obj.to_frame(name="score")
        elif isinstance(pred_obj, pd.DataFrame):
            pdf = pred_obj
        else:
            pdf = pd.DataFrame(pred_obj)

        if "score" not in pdf.columns and pdf.shape[1] >= 1:
            pdf = pdf.rename(columns={pdf.columns[0]: "score"})

        scores = pd.to_numeric(pdf["score"], errors="coerce").dropna()
        result["pred_std"] = round(float(scores.std()), 6)

        # Prediction autocorrelation (1-day lag)
        if isinstance(pdf.index, pd.MultiIndex):
            pdf_flat = pdf.reset_index()
            date_cols = [c for c in pdf_flat.columns if "date" in str(c).lower()]
            inst_cols = [c for c in pdf_flat.columns if "inst" in str(c).lower()]
            if date_cols and inst_cols:
                dc, ic = date_cols[0], inst_cols[0]
                pivoted = pdf_flat.pivot_table(
                    index=dc, columns=ic, values="score", aggfunc="first"
                )
                # Rank turnover: correlation of ranks between consecutive days
                ranks = pivoted.rank(axis=1)
                rank_corrs = []
                for i in range(1, len(ranks)):
                    prev = ranks.iloc[i - 1].dropna()
                    curr = ranks.iloc[i].dropna()
                    common = prev.index.intersection(curr.index)
                    if len(common) > 5:
                        rank_corrs.append(float(prev[common].corr(curr[common])))
                if rank_corrs:
                    result["pred_autocorr_1d"] = round(float(_np.mean(rank_corrs)), 4)
                    result["pred_rank_turnover"] = round(1.0 - float(_np.mean(rank_corrs)), 4)

                # Top30 stability
                top30_sets = []
                for i in range(len(ranks)):
                    row = ranks.iloc[i].dropna().nlargest(30)
                    top30_sets.append(set(row.index))
                if len(top30_sets) > 1:
                    overlaps = []
                    for i in range(1, len(top30_sets)):
                        overlap = len(top30_sets[i] & top30_sets[i - 1]) / 30.0
                        overlaps.append(overlap)
                    result["top30_stability"] = round(float(_np.mean(overlaps)), 4)

    except Exception as e:
        print(f"Warning: failed to extract prediction diagnostics: {e}")
    return result


def _read_parquet_feature_names(parquet_name: str) -> list:
    """Search for a parquet file and return its feature column names."""
    if not parquet_name:
        return []
    for search_dir in [Path.cwd(), Path(__file__).parent,
                       Path.cwd() / "factors", Path(__file__).parent / "factors",
                       Path.cwd().parent]:
        pq = search_dir / parquet_name
        if pq.exists():
            _pq_df = pd.read_parquet(pq)
            cols = [c for c in _pq_df.columns if c not in ("LABEL0",)]
            # Handle MultiIndex columns: extract last level
            return [c[-1] if isinstance(c, tuple) else c for c in cols]
    return []


def _resolve_feature_names(n_features: int) -> list:
    """Resolve real feature names from conf.yaml + combined_factors_df.parquet.

    LightGBM Booster stores features as Column_0..N. This maps them back to
    Alpha158 names (from conf.yaml) + custom factor names (from parquet columns).
    Supports:
    - NestedDataLoader with Alpha158DL + StaticDataLoader
    - DynamicFactorsOnlyLoader with dynamic_path pointing to parquet
    - Any custom loader with a parquet config
    Returns list of length n_features, or empty list if resolution fails.
    """
    try:
        # Find conf.yaml
        for candidate in [Path.cwd() / "conf.yaml", Path(__file__).parent / "conf.yaml",
                          Path.cwd() / "conf_baseline.yaml"]:
            if candidate.exists():
                _conf = yaml.safe_load(candidate.read_text(encoding="utf-8"))
                break
        else:
            return []

        dh = (_conf or {}).get("data_handler_config", {})
        dl_cfg = dh.get("data_loader", {})
        dl_cls = dl_cfg.get("class", "")
        dl_kw = dl_cfg.get("kwargs", {})

        real_names = []

        # Case 1: NestedDataLoader with dataloader_l list
        dl = dl_kw.get("dataloader_l", [])
        if dl:
            for loader in dl:
                cls = loader.get("class", "")
                kw = loader.get("kwargs", {})
                if "Alpha158" in cls:
                    feat_cfg = kw.get("config", {}).get("feature", [])
                    if len(feat_cfg) >= 2:
                        real_names.extend(feat_cfg[1])
                elif "StaticDataLoader" in cls:
                    real_names.extend(_read_parquet_feature_names(kw.get("config", "")))

        # Case 2: Direct parquet loader (DynamicFactorsOnlyLoader, etc.)
        if not real_names:
            for pq_key in ["dynamic_path", "config"]:
                pq_name = dl_kw.get(pq_key, "")
                if pq_name and isinstance(pq_name, str) and pq_name.endswith(".parquet"):
                    real_names.extend(_read_parquet_feature_names(pq_name))
                    if real_names:
                        break

        if len(real_names) == n_features:
            return real_names
        elif len(real_names) > 0:
            print(f"Warning: resolved {len(real_names)} feature names but model has {n_features}")
        return []
    except Exception as e:
        print(f"Warning: feature name resolution failed: {e}")
        return []


def _extract_feature_importance(recorder) -> list:
    """Extract feature importance from trained model.

    Strategy (ordered):
      1. LightGBM native gain/split (when model.feature_importance exists)
      2. Correlation-based importance from pred.pkl × combined_factors_df.parquet
         (works for PyTorch/GeneralPTNN/SimpleGRU and any model).

    Output schema (frontend-compatible):
      [{"name": str, "gain": float, "gain_pct": float, "split": int, "method": str}, ...]
    """
    # ── Path 1: LightGBM native ──
    try:
        params = recorder.load_object("params.pkl")
        model = getattr(params, "model", None)
        if model is not None and hasattr(model, "feature_importance"):
            fi_gain = model.feature_importance(importance_type="gain")
            fi_split = model.feature_importance(importance_type="split")
            raw_names = model.feature_name() if hasattr(model, "feature_name") else [f"f{i}" for i in range(len(fi_gain))]

            if raw_names and raw_names[0].startswith("Column_"):
                resolved = _resolve_feature_names(len(raw_names))
                if resolved:
                    raw_names = resolved

            total_gain = sum(fi_gain) or 1.0
            result = []
            for i, name in enumerate(raw_names):
                result.append({
                    "name": name,
                    "gain": round(float(fi_gain[i]), 4),
                    "gain_pct": round(float(fi_gain[i]) / total_gain * 100, 2),
                    "split": int(fi_split[i]),
                    "method": "lightgbm_gain",
                })
            result.sort(key=lambda x: -x["gain"])
            print(f"[INFO] Extracted LightGBM feature_importance for {len(result)} features")
            return result
        else:
            _model_type = type(model).__name__ if model is not None else "None"
            print(f"[INFO] LightGBM path unavailable (model type={_model_type}), falling back to correlation-based importance")
    except Exception as e:
        print(f"[INFO] LightGBM path failed: {e}, falling back to correlation-based importance")

    # ── Path 2: Correlation-based importance (model-agnostic) ──
    return _extract_feature_importance_correlation(recorder)


def _extract_feature_importance_correlation(recorder) -> list:
    """Compute correlation-based feature importance.

    For each input factor, compute |corr(factor_value, model_prediction)| on
    the test/validation period. This is a model-agnostic signal showing how
    strongly each factor drove the model's output.
    """
    try:
        import pandas as _pd
        import numpy as _np
        import os as _os

        # 1. Load model predictions (signals)
        pred = None
        try:
            pred = recorder.load_object("pred.pkl")
        except Exception as e:
            print(f"Warning: correlation importance skipped — failed to load pred.pkl: {e}")
            return []

        if isinstance(pred, _pd.DataFrame):
            # pred typically has a single 'score' column with MultiIndex(datetime, instrument)
            if "score" in pred.columns:
                pred_series = pred["score"]
            else:
                pred_series = pred.iloc[:, 0]
        else:
            pred_series = pred

        if pred_series is None or len(pred_series) == 0:
            print("Warning: correlation importance skipped — empty pred")
            return []

        # 2. Load raw factor data — try combined_factors_df.parquet in workspace
        factors_df = None
        candidate_paths = []
        # Resolve workspace dir via recorder artifact_uri if possible
        try:
            art_uri = getattr(recorder, "artifact_uri", None) or getattr(recorder, "uri", None)
            if art_uri:
                # artifact_uri looks like file:///.../Loop1/mlruns/.../artifacts
                ws_dir = _os.path.abspath(_os.path.join(str(art_uri).replace("file://", "").replace("file:", ""), "..", "..", "..", "..", ".."))
                candidate_paths.append(_os.path.join(ws_dir, "combined_factors_df.parquet"))
        except Exception:
            pass
        # Fallback: current working directory
        candidate_paths.append(_os.path.abspath("combined_factors_df.parquet"))
        candidate_paths.append(_os.path.abspath(_os.path.join(_os.path.dirname(__file__), "combined_factors_df.parquet")) if "__file__" in globals() else "")

        for p in candidate_paths:
            if p and _os.path.exists(p):
                try:
                    factors_df = _pd.read_parquet(p)
                    print(f"[INFO] Loaded factor data from: {p} (shape={factors_df.shape})")
                    break
                except Exception as e:
                    print(f"Warning: failed to read {p}: {e}")
                    continue

        if factors_df is None or factors_df.empty:
            print("Warning: correlation importance skipped — combined_factors_df.parquet not found")
            return []

        # 3. Align pred and factors on common index
        # Both typically have MultiIndex (datetime, instrument)
        try:
            common_idx = pred_series.index.intersection(factors_df.index)
        except Exception as e:
            print(f"Warning: index intersection failed: {e}")
            return []

        if len(common_idx) == 0:
            print("Warning: correlation importance skipped — no common index between pred and factors")
            return []

        # Use .loc to guarantee row-aligned pred/factors on the common index.
        # Both must be re-indexed identically — never rely on iloc alignment after
        # independent .loc calls (MultiIndex sort order is not guaranteed).
        pred_aligned = pred_series.loc[common_idx]
        factors_aligned = factors_df.loc[common_idx]
        # Explicitly reindex factors by pred's (now canonical) index to lock row order.
        factors_aligned = factors_aligned.reindex(pred_aligned.index)

        # Cap sample size for performance
        if len(pred_aligned) > 200000:
            sample_idx = _np.random.RandomState(42).choice(len(pred_aligned), 200000, replace=False)
            pred_aligned = pred_aligned.iloc[sample_idx]
            factors_aligned = factors_aligned.iloc[sample_idx]
            print(f"[INFO] Sampled {len(pred_aligned)} rows for correlation computation")

        # Keep NaN — mask per-factor below. NEVER fill NaN with 0 (would bias corr).
        pred_values = pred_aligned.to_numpy(dtype=float, copy=False)

        # 4. Compute |correlation| for each factor column
        scores = []
        skipped_cols: list[tuple[str, str]] = []
        for col in factors_aligned.columns:
            try:
                col_values = factors_aligned[col].to_numpy(dtype=float, copy=False)
            except (ValueError, TypeError) as e:
                # Non-numeric column — surface loudly, do NOT silently assign 0.
                skipped_cols.append((str(col), f"non-numeric: {e}"))
                continue
            mask = _np.isfinite(col_values) & _np.isfinite(pred_values)
            n_valid = int(mask.sum())
            if n_valid < 100:
                skipped_cols.append((str(col), f"insufficient valid samples: {n_valid}"))
                continue
            # np.corrcoef raises on degenerate input; catch narrowly.
            cv = col_values[mask]
            pv = pred_values[mask]
            if cv.std() == 0 or pv.std() == 0:
                skipped_cols.append((str(col), "zero variance"))
                continue
            c = _np.corrcoef(cv, pv)[0, 1]
            if not _np.isfinite(c):
                skipped_cols.append((str(col), "corrcoef returned non-finite"))
                continue
            scores.append((col, abs(float(c))))

        if skipped_cols:
            print(f"[WARN] correlation importance skipped {len(skipped_cols)} factors:")
            for name, reason in skipped_cols[:20]:
                print(f"       - {name}: {reason}")
            if len(skipped_cols) > 20:
                print(f"       ... and {len(skipped_cols) - 20} more")

        if not scores:
            print("Warning: correlation importance returned no scores (all factors skipped)")
            return []

        # 5. Normalize & format output
        def _clean_col_name(col):
            if isinstance(col, tuple):
                return str(col[-1])
            return str(col)

        total = sum(s for _, s in scores) or 1.0
        result = []
        for name, s in scores:
            result.append({
                "name": _clean_col_name(name),
                "gain": round(float(s), 6),
                "gain_pct": round(float(s) / total * 100, 2),
                "split": 0,
                "method": "pytorch_correlation",
            })
        result.sort(key=lambda x: -x["gain"])
        print(f"[INFO] Extracted correlation-based feature_importance for {len(result)} features")
        return result
    except Exception as e:
        import traceback as _tb
        print(f"Warning: correlation feature_importance extraction failed: {e}")
        print(_tb.format_exc())
        return []


def _extract_absolute_returns(recorder) -> dict:
    """Extract absolute return metrics from positions_normal_1day.pkl.

    Computes: initial_capital, final_total_value, CAGR, max_drawdown, sharpe,
    annualized_volatility, total_return, final_cash, final_stock_value, avg_cash_ratio.
    """
    try:
        pos_obj = recorder.load_object("portfolio_analysis/positions_normal_1day.pkl")
        if not isinstance(pos_obj, dict) or len(pos_obj) == 0:
            return {}

        _SKIP_KEYS = {"cash", "now_account_value"}

        sorted_dates = sorted(pos_obj.keys())
        nav_list = []         # daily total value (cash + stock)
        cash_ratios = []

        for dt in sorted_dates:
            holdings = _positions_to_dict(pos_obj[dt])
            if not holdings:
                continue

            # Cash
            cash_raw = holdings.get("cash", 0)
            if isinstance(cash_raw, dict):
                cash_val = float(cash_raw.get("amount", 0))
            else:
                cash_val = float(cash_raw)

            # now_account_value 是 qlib 直接提供的总市值（如果有的话直接用）
            nav_raw = holdings.get("now_account_value", None)
            if nav_raw is not None:
                if isinstance(nav_raw, dict):
                    total_val = float(nav_raw.get("amount", 0))
                else:
                    total_val = float(nav_raw)
                if total_val > 0:
                    nav_list.append(total_val)
                    cash_ratios.append(cash_val / total_val if total_val > 0 else 0)
                    continue

            # Fallback: 手动加总
            stock_val = 0.0
            for k, v in holdings.items():
                if k in _SKIP_KEYS or not isinstance(v, dict):
                    continue
                amt = float(v.get("amount", 0))
                price = float(v.get("price", 0))
                stock_val += amt * price

            total_val = cash_val + stock_val
            if total_val > 0:
                nav_list.append(total_val)
                cash_ratios.append(cash_val / total_val)

        if len(nav_list) < 2:
            return {}

        nav = _np.array(nav_list, dtype=float)
        initial_capital = nav[0]
        final_val = nav[-1]
        n_days = len(nav)
        total_return = (final_val - initial_capital) / initial_capital if initial_capital > 0 else 0

        n_years = n_days / 252.0
        cagr = (final_val / initial_capital) ** (1.0 / n_years) - 1 if n_years > 0 and initial_capital > 0 else 0

        daily_ret = _np.diff(nav) / nav[:-1]
        volatility = float(_np.std(daily_ret) * _np.sqrt(252))
        sharpe = float(_np.mean(daily_ret) / _np.std(daily_ret) * _np.sqrt(252)) if _np.std(daily_ret) > 0 else 0

        # Max drawdown
        cummax = _np.maximum.accumulate(nav)
        drawdown = (nav - cummax) / cummax
        max_dd = float(_np.min(drawdown))
        max_dd_idx = int(_np.argmin(drawdown))
        max_dd_date_raw = sorted_dates[max_dd_idx]
        max_dd_date = max_dd_date_raw.strftime("%Y-%m-%d") if hasattr(max_dd_date_raw, "strftime") else str(max_dd_date_raw)

        # Final cash / stock split
        last_holdings = _positions_to_dict(pos_obj[sorted_dates[-1]])
        cash_raw = last_holdings.get("cash", 0)
        final_cash = float(cash_raw.get("amount", 0)) if isinstance(cash_raw, dict) else float(cash_raw)
        final_stock = final_val - final_cash

        avg_cash_ratio = float(_np.mean(cash_ratios)) if cash_ratios else 0.0
        final_stock_count = len([k for k in last_holdings if k not in _SKIP_KEYS])

        result = {
            "initial_capital": round(float(initial_capital), 2),
            "final_total_value": round(float(final_val), 2),
            "total_return": round(float(total_return), 6),
            "n_trading_days": n_days,
            "cagr": round(float(cagr), 6),
            "max_drawdown": round(float(max_dd), 6),
            "max_drawdown_date": max_dd_date,
            "annualized_volatility": round(float(volatility), 6),
            "sharpe": round(float(sharpe), 4),
            "final_cash": round(float(final_cash), 2),
            "final_stock_value": round(float(final_stock), 2),
            "avg_cash_ratio": round(float(avg_cash_ratio), 4),
            "final_stock_count": final_stock_count,
        }
        print(f"[INFO] Extracted absolute_returns: CAGR={result['cagr']:.4f}, MaxDD={result['max_drawdown']:.4f}, Sharpe={result['sharpe']:.2f}")
        return result
    except Exception as e:
        print(f"Warning: absolute_returns extraction failed: {e}")
        return {}


def _generate_llm_summary(enhanced: dict) -> dict:
    """Generate a compact LLM-friendly summary (no long time series)."""
    summary = {}

    # Copy original summary metrics
    if "summary" in enhanced:
        summary["summary"] = enhanced["summary"]

    # IC diagnostics summary (no series)
    ic = enhanced.get("ic_diagnostics", {})
    if ic:
        ic_mean = ic.get("ic_mean")
        ic_std = ic.get("ic_std")
        icir = round(ic_mean / ic_std, 4) if ic_mean and ic_std and ic_std > 0 else None

        # Determine IC trend from rolling mean
        rolling = ic.get("ic_rolling_30d_mean", [])
        valid_rolling = [v for v in rolling if v is not None]
        trend = "stable"
        if len(valid_rolling) >= 10:
            recent = _np.mean(valid_rolling[-10:])
            earlier = _np.mean(valid_rolling[:10])
            if recent > earlier * 1.1:
                trend = "rising"
            elif recent < earlier * 0.9:
                trend = "declining"

        summary["ic_diagnostics_summary"] = {
            "ic_mean": ic.get("ic_mean"),
            "ic_std": ic.get("ic_std"),
            "ic_positive_ratio": ic.get("ic_positive_ratio"),
            "ic_stability_score": icir,
            "rank_ic_mean": ic.get("rank_ic_mean"),
            "ic_trend": trend,
        }

    # Training diagnostics summary
    td = enhanced.get("training_diagnostics", {})
    if td:
        diag_parts = []
        ofr = td.get("overfit_ratio")
        if ofr is not None:
            if ofr > 1.3:
                diag_parts.append("严重过拟合")
            elif ofr > 1.1:
                diag_parts.append("轻微过拟合")
            else:
                diag_parts.append("拟合正常")
        cr = td.get("convergence_ratio")
        if cr is not None:
            if cr < 0.3:
                diag_parts.append("过早收敛")
            elif cr > 0.95:
                diag_parts.append("可能未充分收敛")
            else:
                diag_parts.append("收敛正常")

        summary["training_diagnostics_summary"] = {
            "overfit_ratio": ofr,
            "convergence_ratio": cr,
            "early_stop_triggered": td.get("early_stop_triggered"),
            "diagnosis": "，".join(diag_parts) if diag_parts else None,
        }

    # Trade diagnostics summary
    trd = enhanced.get("trade_diagnostics", {})
    if trd:
        diag_parts = []
        turnover = trd.get("avg_turnover")
        if turnover is not None and turnover > 0.5:
            diag_parts.append(f"换手率过高({turnover:.2f}>0.5)")
        cost_drag = trd.get("cost_drag_annualized")
        if cost_drag is not None and cost_drag > 0.05:
            diag_parts.append(f"成本侵蚀严重({cost_drag*100:.1f}pp)")

        summary["trade_diagnostics_summary"] = {
            "avg_turnover": turnover,
            "cost_drag": cost_drag,
            "diagnosis": "，".join(diag_parts) if diag_parts else "交易效率正常",
        }

    # Prediction diagnostics summary
    pred = enhanced.get("prediction_diagnostics", {})
    if pred:
        stability = pred.get("top30_stability")
        diag = "预测排名稳定" if stability and stability > 0.7 else "预测排名中等稳定" if stability and stability > 0.4 else "预测排名不稳定"
        summary["prediction_diagnostics_summary"] = {
            "pred_rank_turnover": pred.get("pred_rank_turnover"),
            "top30_stability": stability,
            "diagnosis": diag,
        }

    # Auto prescription
    issues = []
    recommendations = []
    dims = []

    if trd.get("avg_turnover") and trd["avg_turnover"] > 0.5:
        issues.append("high_turnover")
        dims.append("loss_function")
        recommendations.append("切换到Huber Loss或增加换手率惩罚项")

    ic_summary = summary.get("ic_diagnostics_summary", {})
    if ic_summary.get("ic_trend") == "declining":
        issues.append("ic_unstable")
        dims.append("non_stationarity")
        recommendations.append("增加RevIN处理因子非平稳性")

    td_summary = summary.get("training_diagnostics_summary", {})
    if td_summary.get("overfit_ratio") and td_summary["overfit_ratio"] > 1.3:
        issues.append("overfitting")
        dims.append("regularization")
        recommendations.append("增加Dropout或L2正则化")

    summary["auto_prescription"] = {
        "issues": issues,
        "recommended_dimensions": dims,
        "recommended_actions": recommendations,
    }

    return summary


# ============================================================
# Generate enhanced output files
# ============================================================
if latest_recorder is not None:
    try:
        _summary_dict = metrics.to_dict() if metrics is not None else {}

        # Merge CAGR into summary
        _cagr_data = _compute_cagr(latest_recorder)
        _summary_dict.update(_cagr_data)

        # Fallback: 当 excess_return 指标为 NaN 时，用 portfolio return - benchmark 手动计算
        _excess_keys = [k for k in _summary_dict if "excess_return" in str(k)]
        _all_excess_nan = all(
            _summary_dict.get(k) is None or (isinstance(_summary_dict.get(k), float) and _np.isnan(_summary_dict[k]))
            for k in _excess_keys
        ) if _excess_keys else False

        if _all_excess_nan:
            try:
                _report = latest_recorder.load_object("portfolio_analysis/report_normal_1day.pkl")
                if isinstance(_report, pd.DataFrame) and "return" in _report.columns:
                    _port_ret = pd.to_numeric(_report["return"], errors="coerce").fillna(0)
                    # 加载 benchmark
                    _bench_ret = None
                    _bp = Path(__file__).parent / "benchmark_sh000300.parquet"
                    if _bp.exists():
                        _bdf = pd.read_parquet(_bp)
                        _bsr = _bdf["bench"]
                        _bsr.index = pd.to_datetime(_bsr.index)
                        _bench_ret = _bsr.reindex(pd.to_datetime(_report.index)).fillna(0)
                    if _bench_ret is not None:
                        _excess = _port_ret - _bench_ret
                        _n_days = len(_excess)
                        _ann_factor = 252.0 / max(_n_days, 1)
                        _exc_mean = float(_excess.mean())
                        _exc_std = float(_excess.std())
                        _exc_cum = (1 + _excess).cumprod()
                        _exc_ann_ret = float(_exc_cum.iloc[-1] ** _ann_factor - 1) if _exc_cum.iloc[-1] > 0 else 0.0
                        _exc_ir = float(_exc_mean / _exc_std * _np.sqrt(252)) if _exc_std > 0 else 0.0
                        _exc_cummax = _exc_cum.cummax()
                        _exc_dd = ((_exc_cum - _exc_cummax) / _exc_cummax).min()
                        # 回填 summary（NestedExecutor 模式下 with_cost ≈ without_cost）
                        for _prefix in ["1day.excess_return_without_cost", "1day.excess_return_with_cost"]:
                            _summary_dict[f"{_prefix}.annualized_return"] = round(_exc_ann_ret, 6)
                            _summary_dict[f"{_prefix}.information_ratio"] = round(_exc_ir, 6)
                            _summary_dict[f"{_prefix}.max_drawdown"] = round(float(_exc_dd), 6)
                            _summary_dict[f"{_prefix}.mean"] = round(_exc_mean, 6)
                            _summary_dict[f"{_prefix}.std"] = round(_exc_std, 6)
                        _summary_dict["_excess_return_source"] = "benchmark_parquet_fallback"
                        print("[INFO] Computed excess_return from benchmark parquet fallback")
            except Exception as _fb_e:
                print(f"Warning: excess_return fallback failed: {_fb_e}")

        _enhanced = {"summary": _summary_dict}
        _enhanced["ic_diagnostics"] = _extract_ic_diagnostics(latest_recorder)
        _enhanced["training_diagnostics"] = _extract_training_diagnostics()
        _enhanced["return_curves"] = _extract_return_curves(latest_recorder)
        _enhanced["trade_diagnostics"] = _extract_trade_diagnostics(latest_recorder, _summary_dict)
        _enhanced["prediction_diagnostics"] = _extract_prediction_diagnostics(latest_recorder)
        _enhanced.update(_extract_top_stocks(latest_recorder))

        # Feature importance (LightGBM gain/split)
        _fi = _extract_feature_importance(latest_recorder)
        if _fi:
            _enhanced["factor_analysis"] = {"feature_importance": _fi}

        # Absolute returns (from positions)
        _ar = _extract_absolute_returns(latest_recorder)
        if _ar:
            _enhanced["absolute_returns"] = _ar

        # 如果通过 fallback 补算了 excess_return，更新 qlib_res.csv
        if _summary_dict.get("_excess_return_source") == "benchmark_parquet_fallback":
            _updated_metrics = pd.Series(_summary_dict)
            _updated_metrics.drop("_excess_return_source", errors="ignore", inplace=True)
            _updated_metrics.to_csv(Path.cwd() / "qlib_res.csv")
            print("[INFO] Updated qlib_res.csv with fallback excess_return metrics")

        # Write full enhanced file (with time series, for dashboard charts)
        _enhanced_path = Path.cwd() / "qlib_results_enhanced.json"
        _enhanced_path.write_text(
            _json.dumps(_json_safe(_enhanced), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Enhanced results saved to {_enhanced_path}")

        # Write compact LLM summary (no long series, for feedback prompt)
        _llm_summary = _generate_llm_summary(_enhanced)
        _llm_path = Path.cwd() / "qlib_results_llm.json"
        _llm_path.write_text(
            _json.dumps(_json_safe(_llm_summary), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"LLM summary saved to {_llm_path}")

    except Exception as e:
        print(f"Warning: failed to generate enhanced output files: {e}")
