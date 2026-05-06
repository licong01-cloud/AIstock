#!/usr/bin/env python
"""Collect QE evolution loop artifacts into a compact JSON summary.

This helper is intentionally read-only. It combines AIstock evolution API data
with local QE workspace files so analysis can detect configuration/runtime
drift such as configured label_horizon differing from the actual label formula
written to conf.yaml or qe_custom_loaders.py.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

try:
    import requests
except Exception:  # pragma: no cover - requests is optional for offline mode
    requests = None  # type: ignore[assignment]


LABEL_RE = re.compile(
    r"Ref\(\$close,\s*-(?P<offset>\d+)\)\s*/\s*Ref\(\$close,\s*-1\)\s*-\s*1"
)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _api_get(api_base: str, endpoint: str, timeout: int = 60) -> dict[str, Any]:
    if requests is None:
        raise RuntimeError("requests is not installed; use --no-fetch with cached files")
    url = f"{api_base.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _load_or_fetch(
    *,
    cache_dir: Path,
    cache_name: str,
    api_base: str,
    endpoint: str,
    no_fetch: bool,
) -> dict[str, Any] | None:
    path = cache_dir / cache_name
    if not no_fetch:
        try:
            payload = _api_get(api_base, endpoint)
            _write_json(path, payload)
            return payload
        except Exception as exc:
            print(f"WARN: API fetch failed for {endpoint}: {exc}", file=sys.stderr)
    if path.exists():
        return _read_json(path)
    return None


def _extract_actual_label(loop_dir: Path) -> dict[str, Any]:
    conf_path = loop_dir / "conf.yaml"
    loader_path = loop_dir / "qe_custom_loaders.py"
    conf_text = conf_path.read_text(encoding="utf-8", errors="replace") if conf_path.exists() else ""
    loader_text = loader_path.read_text(encoding="utf-8", errors="replace") if loader_path.exists() else ""

    if "DynamicFactorsOnlyLoader" in conf_text:
        loader_class = "DynamicFactorsOnlyLoader"
    elif "NestedDataLoader" in conf_text:
        loader_class = "NestedDataLoader"
    else:
        loader_class = None

    source = None
    match = LABEL_RE.search(conf_text)
    if match:
        source = "conf.yaml"
    elif loader_text:
        match = LABEL_RE.search(loader_text)
        if match:
            source = "qe_custom_loaders.py"

    actual_horizon = None
    actual_expr = None
    if match:
        offset = int(match.group("offset"))
        actual_horizon = offset - 1
        actual_expr = match.group(0)

    return {
        "data_loader_class": loader_class,
        "actual_label_horizon": actual_horizon,
        "actual_label_expr": actual_expr,
        "actual_label_source": source,
    }


def _extract_feature_runtime(loop_dir: Path) -> dict[str, Any]:
    """Extract runtime feature-count evidence from generated logs/config."""
    conf_path = loop_dir / "conf.yaml"
    run_log_path = loop_dir / "run.log"
    factor_env_path = loop_dir / ".factor_env"
    conf_text = conf_path.read_text(encoding="utf-8", errors="replace") if conf_path.exists() else ""
    run_text = run_log_path.read_text(encoding="utf-8", errors="replace") if run_log_path.exists() else ""
    env_text = factor_env_path.read_text(encoding="utf-8", errors="replace") if factor_env_path.exists() else ""

    runtime_total = None
    runtime_alpha = None
    runtime_custom = None
    match = re.search(r"num_features\s*=\s*(\d+)\s*\(Alpha158\)\s*\+\s*(\d+)\s*\(custom\)\s*=\s*(\d+)", run_text)
    if match:
        runtime_alpha = int(match.group(1))
        runtime_custom = int(match.group(2))
        runtime_total = int(match.group(3))
    else:
        match = re.search(r"num_features\s*=\s*(\d+)", env_text or run_text)
        if match:
            runtime_total = int(match.group(1))

    importance_scope = None
    match = re.search(r"Extracted .*feature_importance for (\d+) features", run_text)
    if match:
        extracted = int(match.group(1))
        if runtime_total and extracted < runtime_total:
            importance_scope = f"partial_{extracted}_of_{runtime_total}"
        else:
            importance_scope = f"all_{extracted}"

    alpha_alias_count = 0
    if "Alpha158DL" in conf_text:
        alias_match = re.search(r'- \["RESI5".*?"KLOW"\]', conf_text, re.DOTALL)
        if alias_match:
            alpha_alias_count = len(re.findall(r'"([^"]+)"', alias_match.group(0)))

    return {
        "runtime_feature_count": runtime_total,
        "runtime_alpha158_feature_count": runtime_alpha if runtime_alpha is not None else alpha_alias_count,
        "runtime_custom_feature_count": runtime_custom,
        "feature_importance_scope": importance_scope,
    }


def _model_family(model_id: str | None) -> str:
    value = (model_id or "").lower()
    if "lgb" in value:
        return "LGB"
    if "xgboost" in value or "xgb" in value:
        return "XGB"
    if "catboost" in value or "cat" in value:
        return "CAT"
    if "tcn" in value:
        return "TCN"
    if "gru" in value:
        return "GRU"
    if "lstm" in value:
        return "LSTM"
    return model_id or "UNKNOWN"


def _unwrap_api_data(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return {}
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def collect_summary(
    task_id: str,
    *,
    workspace: Path,
    api_base: str,
    cache_dir: Path,
    no_fetch: bool,
) -> dict[str, Any]:
    task_payload = _load_or_fetch(
        cache_dir=cache_dir,
        cache_name=f"{task_id}_task.json",
        api_base=api_base,
        endpoint=f"quantevolver/evolution/tasks/{task_id}",
        no_fetch=no_fetch,
    )
    task = _unwrap_api_data(task_payload)
    loops = task.get("loops") or []
    if not loops:
        raise RuntimeError(f"No loops found for task {task_id}")

    rows: list[dict[str, Any]] = []
    for loop in sorted(loops, key=lambda item: int(item.get("loop_index") or 0)):
        loop_index = int(loop.get("loop_index") or 0)
        cfg = loop.get("config_json") or {}
        if isinstance(cfg, str):
            cfg = json.loads(cfg)
        model_params = cfg.get("model_params") or {}
        factor_list = cfg.get("factor_list") or []
        factor_set = set(factor_list)
        loop_dir = workspace / f"Loop{loop_index}"

        enhanced_payload = _load_or_fetch(
            cache_dir=cache_dir,
            cache_name=f"{task_id}_Loop{loop_index}_enhanced.json",
            api_base=api_base,
            endpoint=f"quantevolver/evolution/tasks/{task_id}/loops/Loop{loop_index}/enhanced-metrics",
            no_fetch=no_fetch,
        )
        enhanced = _unwrap_api_data(enhanced_payload)
        summary = enhanced.get("summary") or {}
        abs_ret = enhanced.get("absolute_returns") or {}
        pred = enhanced.get("prediction_diagnostics") or {}
        trade = enhanced.get("trade_diagnostics") or {}
        train = enhanced.get("training_diagnostics") or {}
        importance = (enhanced.get("factor_analysis") or {}).get("feature_importance") or []
        alpha_features = [item for item in importance if item.get("name") not in factor_set]
        custom_features = [item for item in importance if item.get("name") in factor_set]

        label_info = _extract_actual_label(loop_dir)
        feature_runtime = _extract_feature_runtime(loop_dir)
        configured_horizon = cfg.get("label_horizon") or model_params.get("label_horizon")
        actual_horizon = label_info.get("actual_label_horizon")
        label_mismatch = (
            configured_horizon is not None
            and actual_horizon is not None
            and int(configured_horizon) != int(actual_horizon)
        )

        row = {
            "loop": loop_index,
            "status": loop.get("status"),
            "label": cfg.get("label"),
            "model_id": cfg.get("model_id"),
            "model_family": _model_family(cfg.get("model_id")),
            "disable_alpha158": bool(cfg.get("disable_alpha158", False)),
            "alpha158_enabled": not bool(cfg.get("disable_alpha158", False)),
            "configured_label_horizon": configured_horizon,
            "model_params_label_horizon": model_params.get("label_horizon"),
            "actual_label_horizon": actual_horizon,
            "actual_label_expr": label_info.get("actual_label_expr"),
            "actual_label_source": label_info.get("actual_label_source"),
            "data_loader_class": label_info.get("data_loader_class"),
            "label_horizon_mismatch": label_mismatch,
            "strategy_id": cfg.get("strategy_id"),
            "execution_algo": cfg.get("execution_algo"),
            "node_id": cfg.get("node_id") or loop.get("node_id"),
            "factor_count": len(factor_list),
            "runtime_feature_count": feature_runtime.get("runtime_feature_count"),
            "runtime_alpha158_feature_count": feature_runtime.get("runtime_alpha158_feature_count"),
            "runtime_custom_feature_count": feature_runtime.get("runtime_custom_feature_count"),
            "feature_importance_count": len(importance),
            "feature_importance_scope": feature_runtime.get("feature_importance_scope"),
            "custom_gain_pct": round(sum(_safe_float(item.get("gain_pct")) or 0 for item in custom_features), 4),
            "alpha158_gain_pct": round(sum(_safe_float(item.get("gain_pct")) or 0 for item in alpha_features), 4),
            "alpha158_feature_count": len(alpha_features),
            "top_features": [
                {"name": item.get("name"), "gain_pct": item.get("gain_pct")}
                for item in importance[:10]
            ],
            "top_alpha158_features": [
                {"name": item.get("name"), "gain_pct": item.get("gain_pct")}
                for item in sorted(alpha_features, key=lambda item: _safe_float(item.get("gain_pct")) or 0, reverse=True)[:10]
            ],
            "metrics": {
                "IC": summary.get("IC"),
                "ICIR": summary.get("ICIR"),
                "Rank_IC": summary.get("Rank IC"),
                "Rank_ICIR": summary.get("Rank ICIR"),
                "ann_return_cost": summary.get("1day.excess_return_with_cost.annualized_return"),
                "max_drawdown_cost": summary.get("1day.excess_return_with_cost.max_drawdown"),
                "cagr": summary.get("cagr") or abs_ret.get("cagr"),
                "abs_sharpe": abs_ret.get("sharpe"),
                "abs_max_drawdown": abs_ret.get("max_drawdown"),
                "final_total_value": abs_ret.get("final_total_value"),
                "final_cash": abs_ret.get("final_cash"),
                "final_stock_value": abs_ret.get("final_stock_value"),
                "final_cash_ratio": abs_ret.get("final_cash_ratio"),
                "avg_cash_ratio": abs_ret.get("avg_cash_ratio"),
                "final_stock_count": abs_ret.get("final_stock_count"),
                "position_count_min": abs_ret.get("position_count_min"),
                "position_count_avg": abs_ret.get("position_count_avg"),
                "position_count_max": abs_ret.get("position_count_max"),
                "position_count_p95": abs_ret.get("position_count_p95"),
                "position_count_days": abs_ret.get("position_count_days"),
                "avg_turnover": trade.get("avg_turnover"),
                "annualized_turnover": trade.get("annualized_turnover"),
                "daily_trade_count_avg": trade.get("daily_trade_count_avg"),
            },
            "prediction": {
                "pred_std": pred.get("pred_std"),
                "top30_stability": pred.get("top30_stability"),
                "pred_autocorr_1d": pred.get("pred_autocorr_1d"),
                "pred_rank_turnover": pred.get("pred_rank_turnover"),
            },
            "training": train,
        }
        rows.append(row)

    return {
        "task": {
            "task_id": task_id,
            "task_name": task.get("task_name"),
            "status": task.get("status"),
            "max_loops": task.get("max_loops"),
            "current_loop": task.get("current_loop"),
            "workspace": str(workspace),
        },
        "loops": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id")
    parser.add_argument(
        "--workspace",
        type=Path,
        default=None,
        help="QE workspace path; defaults to F:/Dev/RD-Agent-main/qe_workspace/<task_id>",
    )
    parser.add_argument("--api-base", default="http://127.0.0.1:8001/api/v1")
    parser.add_argument("--cache-dir", type=Path, default=Path(".codex_tmp"))
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--no-fetch", action="store_true", help="Read cached API JSON only")
    args = parser.parse_args()

    workspace = args.workspace or Path("F:/Dev/RD-Agent-main/qe_workspace") / args.task_id
    summary = collect_summary(
        args.task_id,
        workspace=workspace,
        api_base=args.api_base,
        cache_dir=args.cache_dir,
        no_fetch=args.no_fetch,
    )
    output = args.output or args.cache_dir / f"{args.task_id}_loop_artifact_summary.json"
    _write_json(output, summary)
    print(output)


if __name__ == "__main__":
    main()
