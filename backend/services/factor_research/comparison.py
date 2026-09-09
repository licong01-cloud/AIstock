"""Research-only factor comparisons; no official metrics, catalog or strategy writes."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .models import ResearchError, json_object


ROLES = {"predictive_increment", "replacement", "conditional"}
DIRECTION_SOURCES = {"declared", "fitted"}
_MIN_CROSS_SECTION = 6


def _iso_window(value: object, name: str) -> dict:
    if not isinstance(value, dict) or set(value) != {"start", "end"}:
        raise ResearchError("invalid_comparison", f"{name} requires start and end")
    try:
        start, end = pd.Timestamp(value["start"]), pd.Timestamp(value["end"])
    except (TypeError, ValueError) as exc:
        raise ResearchError("invalid_comparison", f"{name} requires ISO dates") from exc
    if start.tz is not None or end.tz is not None or start.normalize() != start or end.normalize() != end or start > end:
        raise ResearchError("invalid_comparison", f"{name} requires ordered timezone-naive market dates")
    return {"start": start.date().isoformat(), "end": end.date().isoformat()}


def _knowledge_cutoff(value: object, name: str) -> dict:
    if not isinstance(value, dict) or set(value) != {"date", "phase"} or value.get("phase") not in {
        "pre_open", "post_close",
    }:
        raise ResearchError("invalid_comparison", f"{name} requires date and pre_open/post_close phase")
    cutoff_date = _iso_window({"start": value["date"], "end": value["date"]}, f"{name}.date")["start"]
    return {"date": cutoff_date, "phase": value["phase"]}


def _fit_window(value: object, name: str) -> dict:
    if not isinstance(value, dict) or set(value) != {"start", "end", "knowledge_cutoff"}:
        raise ResearchError("invalid_comparison", f"{name} requires start, end and knowledge_cutoff")
    window = _iso_window({"start": value["start"], "end": value["end"]}, name)
    window["knowledge_cutoff"] = _knowledge_cutoff(value["knowledge_cutoff"], f"{name}.knowledge_cutoff")
    cutoff = window["knowledge_cutoff"]
    if cutoff["date"] < window["end"] or (cutoff["date"] == window["end"] and cutoff["phase"] == "pre_open"):
        raise ResearchError("invalid_comparison", f"{name} signals must be available by its knowledge cutoff")
    return window


def validate_comparison_spec(value: object, *, candidate_names: set[str], repo_root: Path) -> dict:
    spec = json_object(value)
    allowed = {"research_role", "horizon", "baseline", "candidate", "replacement_target", "state",
               "controls", "value_artifacts", "fit_windows", "evaluation_windows", "hac_maxlags",
               "direction", "knowledge_cutoff", "construction", "cost"}
    if set(spec) - allowed:
        raise ResearchError("invalid_comparison", f"Unknown comparison fields: {sorted(set(spec) - allowed)}")
    if spec.get("research_role") not in ROLES:
        raise ResearchError("invalid_comparison", "Unknown research_role")
    if spec.get("horizon") not in {"1d", "5d", "10d", "20d"}:
        raise ResearchError("invalid_comparison", "horizon must use the shared label names")
    baseline = spec.get("baseline")
    candidate = spec.get("candidate")
    if (not isinstance(baseline, list) or not baseline or any(not isinstance(v, str) or not v for v in baseline)
            or len(set(baseline)) != len(baseline) or not isinstance(candidate, str) or not candidate):
        raise ResearchError("invalid_comparison", "Unique baseline signals and one candidate are required")
    if candidate in baseline:
        raise ResearchError("invalid_comparison", "candidate must be distinct from baseline signal names")
    artifacts = spec.get("value_artifacts", {})
    if not isinstance(artifacts, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in artifacts.items()):
        raise ResearchError("invalid_comparison", "value_artifacts must map signal names to files")
    if set(artifacts) & candidate_names:
        raise ResearchError("invalid_comparison", "A signal cannot identify both a current candidate and an artifact")
    controls = spec.get("controls", {"style": [], "neighbors": []})
    if not isinstance(controls, dict) or set(controls) - {"style", "neighbors", "categorical"}:
        raise ResearchError("invalid_comparison", "controls supports style, neighbors and categorical only")
    for key in ("style", "neighbors", "categorical"):
        controls.setdefault(key, [])
        if (not isinstance(controls[key], list) or any(not isinstance(v, str) or not v for v in controls[key])
                or len(set(controls[key])) != len(controls[key])):
            raise ResearchError("invalid_comparison", f"controls.{key} must be signal names")
    if ((set(controls["style"]) & set(controls["neighbors"]))
            or (set(controls["categorical"]) & (set(controls["style"]) | set(controls["neighbors"])))):
        raise ResearchError("invalid_comparison", "A control signal must have one declared control role")
    referenced = set(baseline) | {candidate} | set(controls["style"]) | set(controls["neighbors"]) | set(controls["categorical"])
    role = spec["research_role"]
    if role == "replacement":
        if spec.get("replacement_target") not in baseline:
            raise ResearchError("invalid_comparison", "replacement requires a baseline replacement_target")
    elif spec.get("replacement_target") is not None:
        raise ResearchError("invalid_comparison", "replacement_target is only valid for replacement research")
    if role == "conditional":
        if not isinstance(spec.get("state"), str) or not spec["state"]:
            raise ResearchError("invalid_comparison", "conditional research requires one predeclared state signal")
        referenced.add(spec["state"])
    elif spec.get("state") is not None:
        raise ResearchError("invalid_comparison", "state is only valid for conditional research")
    unknown = referenced - candidate_names - set(artifacts)
    if unknown:
        raise ResearchError("invalid_comparison", f"Unbound comparison signals: {sorted(unknown)}")
    if set(artifacts) - referenced:
        raise ResearchError("invalid_comparison", "value_artifacts contains an unreferenced signal")
    for name, raw_path in artifacts.items():
        path = Path(raw_path).expanduser().resolve()
        if Path(raw_path).expanduser().is_symlink() or not path.is_file() or path.is_relative_to(repo_root.resolve()):
            raise ResearchError("invalid_comparison", f"Artifact for {name} must be a repo-external regular file")
        artifacts[name] = str(path)
    raw_fit_windows, raw_evaluation_windows = spec.get("fit_windows"), spec.get("evaluation_windows")
    if not isinstance(raw_fit_windows, list) or not raw_fit_windows:
        raise ResearchError("invalid_comparison", "fit_windows must be a non-empty list")
    if not isinstance(raw_evaluation_windows, list) or not raw_evaluation_windows:
        raise ResearchError("invalid_comparison", "evaluation_windows must be a non-empty list")
    fit_windows = [_fit_window(item, f"fit_windows[{index}]") for index, item in enumerate(raw_fit_windows)]
    evaluation_windows = []
    for index, item in enumerate(raw_evaluation_windows):
        if not isinstance(item, dict) or set(item) != {"start", "end", "fit_window_index"}:
            raise ResearchError("invalid_comparison", f"evaluation_windows[{index}] requires start, end and fit_window_index")
        fit_index = item["fit_window_index"]
        if type(fit_index) is not int or not 0 <= fit_index < len(fit_windows):
            raise ResearchError("invalid_comparison", f"evaluation_windows[{index}] has an invalid fit_window_index")
        window = _iso_window({"start": item["start"], "end": item["end"]}, f"evaluation_windows[{index}]")
        window["fit_window_index"] = fit_index
        fit_cutoff = fit_windows[fit_index]["knowledge_cutoff"]
        if fit_cutoff["date"] >= window["start"]:
            raise ResearchError("invalid_comparison", "Each fit knowledge cutoff must precede its evaluation window")
        evaluation_windows.append(window)
    lag = spec.get("hac_maxlags", 0)
    if type(lag) is not int or lag < 0:
        raise ResearchError("invalid_comparison", "hac_maxlags must be a non-negative integer")
    direction = spec.get("direction")
    if not isinstance(direction, dict) or set(direction) != {"source", "sign", "locked_at"}:
        raise ResearchError("invalid_comparison", "direction requires source, sign and locked_at")
    if direction["source"] not in DIRECTION_SOURCES or direction["sign"] not in (-1, 1):
        raise ResearchError("invalid_comparison", "direction source/sign is invalid")
    if type(direction["sign"]) is not int:
        raise ResearchError("invalid_comparison", "direction sign must be integer -1 or 1")
    locked = _iso_window({"start": direction["locked_at"], "end": direction["locked_at"]}, "direction.locked_at")
    direction["locked_at"] = locked["start"]
    if any(direction["locked_at"] > fit_windows[item["fit_window_index"]]["knowledge_cutoff"]["date"]
           for item in evaluation_windows):
        raise ResearchError("invalid_comparison", "direction must be locked by every referenced fit cutoff")
    knowledge = _knowledge_cutoff(spec.get("knowledge_cutoff"), "knowledge_cutoff")
    spec["knowledge_cutoff"] = knowledge
    knowledge_date = knowledge["date"]
    for fit_window in fit_windows:
        fit_cutoff = fit_window["knowledge_cutoff"]
        if (fit_cutoff["date"] > knowledge_date
                or (fit_cutoff["date"] == knowledge_date
                    and fit_cutoff["phase"] == "post_close" and knowledge["phase"] == "pre_open")):
            raise ResearchError("invalid_comparison", "Fit knowledge cutoff cannot exceed the comparison knowledge cutoff")
    latest_signal = max(item["end"] for item in evaluation_windows)
    if latest_signal > knowledge_date or (latest_signal == knowledge_date and knowledge["phase"] == "pre_open"):
        raise ResearchError("invalid_comparison", "Evaluation signals must be available by the knowledge cutoff")
    if any(direction["locked_at"] == fit_windows[item["fit_window_index"]]["knowledge_cutoff"]["date"]
           and fit_windows[item["fit_window_index"]]["knowledge_cutoff"]["phase"] == "pre_open"
           for item in evaluation_windows):
        raise ResearchError("invalid_comparison", "A same-date direction lock is unavailable at a pre-open fit cutoff")
    spec.update(controls=controls, value_artifacts=artifacts, fit_windows=fit_windows,
                evaluation_windows=evaluation_windows, hac_maxlags=lag)
    construction = spec.get("construction")
    if construction is not None:
        if not isinstance(construction, dict) or construction.get("relation") not in {"exact_duplicate", "known_transform"}:
            raise ResearchError("invalid_comparison", "construction must declare exact_duplicate or known_transform")
        required = {"relation", "baseline", "evidence"}
        if construction["relation"] == "known_transform":
            required.add("monotonicity")
        if set(construction) != required or construction.get("baseline") not in baseline:
            raise ResearchError("invalid_comparison", "construction must identify one declared baseline and evidence")
        if not isinstance(construction.get("evidence"), str) or not construction["evidence"].strip():
            raise ResearchError("invalid_comparison", "construction evidence is required")
        if (construction["relation"] == "known_transform"
                and construction.get("monotonicity") not in {"increasing", "decreasing"}):
            raise ResearchError("invalid_comparison", "known transform monotonicity is invalid")
    if spec.get("cost") is not None:
        spec["cost"] = _validate_cost_spec(spec["cost"], repo_root)
    return spec


def _validate_cost_spec(value: object, repo_root: Path) -> dict:
    cost = json_object(value)
    allowed = {"baseline_weights", "augmented_weights", "baseline_gross_returns",
               "augmented_gross_returns", "scenarios", "liquidate_at_end", "currency",
               "capital_normalization", "return_basis"}
    if set(cost) != allowed:
        raise ResearchError("invalid_comparison", "cost requires four paths, scenarios, basis and liquidation policy")
    for key in ("baseline_weights", "augmented_weights", "baseline_gross_returns", "augmented_gross_returns"):
        raw = Path(cost[key]).expanduser()
        path = raw.resolve()
        if raw.is_symlink() or not path.is_file() or path.is_relative_to(repo_root.resolve()):
            raise ResearchError("invalid_comparison", f"{key} must be a repo-external regular file")
        cost[key] = str(path)
    if type(cost["liquidate_at_end"]) is not bool or not isinstance(cost["scenarios"], list) or not cost["scenarios"]:
        raise ResearchError("invalid_comparison", "cost scenarios and liquidation policy are required")
    if (not isinstance(cost["currency"], str) or not cost["currency"].strip()
            or not isinstance(cost["capital_normalization"], str) or not cost["capital_normalization"].strip()
            or cost["return_basis"] != "arithmetic_same_frequency"):
        raise ResearchError("invalid_comparison", "Cost currency, capital normalization and arithmetic return basis are required")
    names = set()
    for scenario in cost["scenarios"]:
        if not isinstance(scenario, dict) or set(scenario) != {"name", "buy_bps", "sell_bps", "fixed_cost"}:
            raise ResearchError("invalid_comparison", "Each cost scenario requires name, buy_bps, sell_bps and fixed_cost")
        if not isinstance(scenario["name"], str) or not scenario["name"] or scenario["name"] in names:
            raise ResearchError("invalid_comparison", "Cost scenario names must be unique")
        names.add(scenario["name"])
        for key in ("buy_bps", "sell_bps", "fixed_cost"):
            if type(scenario[key]) not in (int, float) or not np.isfinite(scenario[key]) or scenario[key] < 0:
                raise ResearchError("invalid_comparison", "Cost values must be finite and non-negative")
    return cost


def _rank_frame(frame: pd.DataFrame) -> pd.DataFrame:
    counts = frame.notna().sum(axis=1)
    ranked = frame.rank(axis=1, method="average")
    return ranked.div(counts.add(1), axis=0) - 0.5


def _rank_series(series: pd.Series) -> pd.Series:
    valid = series.dropna()
    result = pd.Series(np.nan, index=series.index, dtype=float)
    if not valid.empty:
        result.loc[valid.index] = valid.rank(method="average") / (len(valid) + 1) - 0.5
    return result


def _corr(left: np.ndarray, right: np.ndarray) -> float | None:
    valid = np.isfinite(left) & np.isfinite(right)
    if valid.sum() < _MIN_CROSS_SECTION:
        return None
    x, y = left[valid], right[valid]
    if np.allclose(x, x[0]) or np.allclose(y, y[0]):
        return None
    value = float(np.corrcoef(x, y)[0, 1])
    return value if np.isfinite(value) else None


def _spearman(left: pd.Series, right: pd.Series) -> float | None:
    common = left.notna() & right.notna()
    return _corr(_rank_series(left.where(common)).to_numpy(), _rank_series(right.where(common)).to_numpy())


def _residual(values: np.ndarray, design: np.ndarray) -> tuple[np.ndarray | None, dict]:
    if len(values) <= design.shape[1] or np.linalg.matrix_rank(design) < design.shape[1]:
        return None, {"status": "unavailable", "reason": "design_rank_or_degrees_of_freedom"}
    residual = values - design @ np.linalg.lstsq(design, values, rcond=None)[0]
    scale = max(float(np.nanmax(np.abs(values))), 1.0)
    tolerance = np.finfo(float).eps * max(design.shape) * scale * 100
    if float(np.linalg.norm(residual)) <= tolerance:
        return None, {"status": "unavailable", "reason": "zero_residual_variation", "tolerance": tolerance}
    return residual, {"status": "computed", "reason": None, "tolerance": tolerance}


def _partial_rank_daily(candidate: pd.DataFrame, returns: pd.DataFrame,
                        continuous: list[pd.DataFrame], categorical: list[pd.DataFrame],
                        eligible: pd.DataFrame) -> dict:
    values: list[dict] = []
    raw_values: list[dict] = []
    reasons: dict[str, int] = {}
    for day in candidate.index:
        columns = [candidate.loc[day], returns.loc[day], eligible.loc[day].astype(bool)]
        columns.extend(frame.loc[day] for frame in continuous + categorical)
        joined = pd.concat(columns, axis=1)
        value_columns = [0, 1, *range(3, joined.shape[1])]
        joined = joined.loc[joined.iloc[:, 2].astype(bool) & joined.iloc[:, value_columns].notna().all(axis=1)]
        if len(joined) < _MIN_CROSS_SECTION:
            reasons["insufficient_cross_section"] = reasons.get("insufficient_cross_section", 0) + 1
            continue
        x = _rank_series(joined.iloc[:, 0]).to_numpy()
        y = _rank_series(joined.iloc[:, 1]).to_numpy()
        raw_correlation = _corr(x, y)
        if raw_correlation is not None:
            raw_values.append({"date": day.date().isoformat(), "value": raw_correlation, "n": len(joined)})
        controls: list[np.ndarray] = []
        cursor = 3
        for _ in continuous:
            control = _rank_series(joined.iloc[:, cursor])
            cursor += 1
            controls.append(control.to_numpy())
        for _ in categorical:
            raw = joined.iloc[:, cursor]
            cursor += 1
            dummies = pd.get_dummies(raw, prefix="category", drop_first=True, dtype=float)
            dummies.loc[raw.isna(), :] = np.nan
            controls.extend(dummies[column].to_numpy() for column in dummies.columns)
        if not controls:
            reasons["control_set_empty"] = reasons.get("control_set_empty", 0) + 1
            continue
        design = np.column_stack([np.ones(len(joined)), *controls])
        complete = np.isfinite(design).all(axis=1) & np.isfinite(x) & np.isfinite(y)
        x_res, x_state = _residual(x[complete], design[complete])
        y_res, y_state = _residual(y[complete], design[complete])
        reason = x_state["reason"] or y_state["reason"]
        if reason:
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        correlation = _corr(x_res, y_res)
        if correlation is None:
            reasons["residual_correlation_undefined"] = reasons.get("residual_correlation_undefined", 0) + 1
            continue
        values.append({"date": day.date().isoformat(), "value": correlation, "n": int(complete.sum())})
    return {"status": "computed" if values else "unavailable", "daily": values,
            "mean": float(np.mean([v["value"] for v in values])) if values else None,
            "raw_rank_ic_on_control_sample": {
                "daily": raw_values,
                "mean": float(np.mean([v["value"] for v in raw_values])) if raw_values else None,
            },
            "unavailable_by_reason": reasons}


def _daily_rank_ic(signal: pd.DataFrame, returns: pd.DataFrame, eligible: pd.DataFrame) -> list[dict]:
    result = []
    for day in signal.index:
        mask = eligible.loc[day].astype(bool)
        x = _rank_series(signal.loc[day].where(mask)).to_numpy()
        y = _rank_series(returns.loc[day].where(mask)).to_numpy()
        value = _corr(x, y)
        if value is not None:
            result.append({"date": day.date().isoformat(), "value": value,
                           "n": int((mask.to_numpy() & np.isfinite(x) & np.isfinite(y)).sum())})
    return result


def _weighted_fit(features: pd.DataFrame, labels: pd.Series) -> tuple[np.ndarray, dict]:
    joined = features.copy()
    joined["__label__"] = labels
    joined = joined.dropna()
    if joined.empty:
        raise ResearchError("comparison_unavailable", "No mature common fit observations")
    day_counts = joined.groupby(level="datetime").size()
    weights = joined.index.get_level_values("datetime").map(lambda day: 1.0 / day_counts.loc[day]).to_numpy()
    design = np.column_stack([np.ones(len(joined)), joined.drop(columns="__label__").to_numpy(dtype=float)])
    target = joined["__label__"].to_numpy(dtype=float)
    weighted_design = design * np.sqrt(weights)[:, None]
    weighted_target = target * np.sqrt(weights)
    rank = int(np.linalg.matrix_rank(weighted_design))
    if len(joined) <= 1:
        raise ResearchError("comparison_unavailable", "Fit sample has insufficient observations")
    return np.linalg.lstsq(weighted_design, weighted_target, rcond=None)[0], {
        "rows": len(joined), "dates": int(day_counts.size), "design_rank": rank,
        "features": list(features.columns), "date_weighting": "equal_by_1_over_n_t",
        "fit_status": "computed" if rank == design.shape[1] else "computed_rank_deficient",
        "fit_reason": None if rank == design.shape[1] else "non_unique_coefficients_predictions_use_lstsq_solution",
    }


def _predict(features: pd.DataFrame, coefficients: np.ndarray) -> pd.Series:
    complete = features.notna().all(axis=1)
    result = pd.Series(np.nan, index=features.index, dtype=float)
    design = np.column_stack([np.ones(int(complete.sum())), features.loc[complete].to_numpy(dtype=float)])
    result.loc[complete] = design @ coefficients
    return result


def _hac(values: list[dict], maxlags: int, calendar: pd.DatetimeIndex) -> dict:
    if not values:
        return {"status": "unavailable", "reason": "no_paired_dates", "segments": []}
    position = {pd.Timestamp(day): index for index, day in enumerate(calendar)}
    segments: list[list[dict]] = []
    for item in values:
        day = pd.Timestamp(item["date"])
        if not segments or position.get(day) != position.get(pd.Timestamp(segments[-1][-1]["date"]), -2) + 1:
            segments.append([])
        segments[-1].append(item)
    reports = []
    for segment in segments:
        array = np.asarray([item["value"] for item in segment], dtype=float)
        report: dict[str, Any] = {"start": segment[0]["date"], "end": segment[-1]["date"],
                                  "n_dates": len(segment), "mean": float(array.mean()),
                                  "hac_maxlags": maxlags}
        if len(array) <= maxlags + 1 or np.allclose(array, array[0]):
            report.update(standard_error=None, t_value=None, reason="insufficient_or_zero_variance")
        else:
            centered = array - array.mean()
            lrv = float(np.dot(centered, centered) / len(array))
            for lag in range(1, maxlags + 1):
                covariance = float(np.dot(centered[lag:], centered[:-lag]) / len(array))
                lrv += 2 * (1 - lag / (maxlags + 1)) * covariance
            if not np.isfinite(lrv) or lrv <= 0:
                report.update(standard_error=None, t_value=None, reason="invalid_long_run_variance")
            else:
                standard_error = float(np.sqrt(lrv / len(array)))
                report.update(standard_error=standard_error,
                              t_value=float(array.mean() / standard_error), reason=None)
        reports.append(report)
    return {"status": "computed", "reason": None if len(reports) == 1 else "reported_by_contiguous_segment",
            "segments": reports, "overall_mean": float(np.mean([item["value"] for item in values])),
            "overall_t_value": reports[0]["t_value"] if len(reports) == 1 else None}


def _load_table(path: str) -> pd.DataFrame:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise ResearchError("comparison_unavailable", "Cost artifact changed or is no longer a regular file")
    value = pd.read_parquet(source) if source.suffix == ".parquet" else pd.read_hdf(source, key="data")
    if not isinstance(value, pd.DataFrame) or value.empty:
        raise ResearchError("comparison_unavailable", "Cost artifact must be a non-empty DataFrame")
    if (not isinstance(value.index, pd.DatetimeIndex) or value.index.tz is not None
            or value.index.hasnans or value.index.has_duplicates or not value.index.equals(value.index.normalize())):
        raise ResearchError("comparison_unavailable", "Cost artifact requires unique timezone-naive market dates")
    if value.columns.has_duplicates:
        raise ResearchError("comparison_unavailable", "Cost artifact columns must be unique")
    if not value.index.is_monotonic_increasing:
        raise ResearchError("comparison_unavailable", "Cost artifact dates must be increasing")
    if any(not pd.api.types.is_numeric_dtype(value[column]) or pd.api.types.is_complex_dtype(value[column])
           for column in value.columns):
        raise ResearchError("comparison_unavailable", "Cost artifact values must be real numeric columns")
    return value


def _cost_report(spec: dict | None, evaluation_windows: list[dict] | None = None) -> dict:
    if spec is None:
        return {"status": "unavailable", "reason": "weight_and_return_paths_not_declared"}
    baseline_weights, augmented_weights = _load_table(spec["baseline_weights"]), _load_table(spec["augmented_weights"])
    baseline_returns, augmented_returns = _load_table(spec["baseline_gross_returns"]), _load_table(spec["augmented_gross_returns"])
    if baseline_returns.shape[1] != 1 or augmented_returns.shape[1] != 1:
        raise ResearchError("comparison_unavailable", "Gross return artifacts require exactly one column")
    dates = baseline_weights.index
    if (dates.empty or not dates.equals(augmented_weights.index)
            or not dates.equals(baseline_returns.index) or not dates.equals(augmented_returns.index)):
        raise ResearchError("comparison_unavailable", "Cost artifacts must use the same explicit date path")
    if evaluation_windows is not None:
        inside = pd.Series(False, index=dates)
        for window in evaluation_windows:
            inside |= (dates >= pd.Timestamp(window["start"])) & (dates <= pd.Timestamp(window["end"]))
        if not bool(inside.all()):
            raise ResearchError("comparison_unavailable", "Cost path extends beyond declared evaluation windows")
    columns = baseline_weights.columns.union(augmented_weights.columns)
    left = baseline_weights.reindex(index=dates, columns=columns, fill_value=0.0).astype(float)
    right = augmented_weights.reindex(index=dates, columns=columns, fill_value=0.0).astype(float)
    if not np.isfinite(left.to_numpy()).all() or not np.isfinite(right.to_numpy()).all():
        raise ResearchError("comparison_unavailable", "Weights must be finite")

    def trades(weights: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        previous = weights.shift(1).fillna(0.0)
        delta = weights - previous
        buy, sell = delta.clip(lower=0).sum(axis=1), (-delta.clip(upper=0)).sum(axis=1)
        if spec["liquidate_at_end"]:
            sell.iloc[-1] += weights.iloc[-1].clip(lower=0).sum()
            buy.iloc[-1] += (-weights.iloc[-1].clip(upper=0)).sum()
        return buy, sell

    left_buy, left_sell = trades(left)
    right_buy, right_sell = trades(right)
    left_gross = baseline_returns.iloc[:, 0].reindex(dates).astype(float)
    right_gross = augmented_returns.iloc[:, 0].reindex(dates).astype(float)
    if not np.isfinite(left_gross).all() or not np.isfinite(right_gross).all():
        raise ResearchError("comparison_unavailable", "Gross returns must be finite on common dates")
    scenarios = []
    turnover_delta = float((right_buy + right_sell).sum() - (left_buy + left_sell).sum())
    for item in spec["scenarios"]:
        left_fee = left_buy * item["buy_bps"] / 10000 + left_sell * item["sell_bps"] / 10000
        right_fee = right_buy * item["buy_bps"] / 10000 + right_sell * item["sell_bps"] / 10000
        left_fee += np.where((left_buy + left_sell) > 0, item["fixed_cost"], 0)
        right_fee += np.where((right_buy + right_sell) > 0, item["fixed_cost"], 0)
        fixed_delta = float(item["fixed_cost"] * (
            ((right_buy + right_sell) > 0).sum() - ((left_buy + left_sell) > 0).sum()))
        break_even = (
            (float((right_gross - left_gross).sum()) - fixed_delta) / turnover_delta * 10000
            if turnover_delta != 0 else None
        )
        scenarios.append({"name": item["name"], "baseline_net_arithmetic": float((left_gross - left_fee).sum()),
                          "augmented_net_arithmetic": float((right_gross - right_fee).sum()),
                          "net_delta_arithmetic": float(((right_gross - right_fee) - (left_gross - left_fee)).sum()),
                          "break_even_common_variable_bps": break_even,
                          "break_even_reason": None if turnover_delta != 0 else "equal_total_traded_weight"})
    return {"status": "computed", "reason": None, "n_dates": len(dates),
            "currency": spec["currency"], "capital_normalization": spec["capital_normalization"],
            "return_basis": spec["return_basis"],
            "turnover_definition": "sum_absolute_weight_change_split_buy_sell",
            "fixed_cost_definition": "capital_normalized_return_deduction_once_per_active_trade_date",
            "liquidate_at_end": spec["liquidate_at_end"], "baseline_buy": float(left_buy.sum()),
            "baseline_sell": float(left_sell.sum()), "augmented_buy": float(right_buy.sum()),
            "augmented_sell": float(right_sell.sum()),
            "baseline_participation": float(((left.abs().sum(axis=1)) > 0).mean()),
            "augmented_participation": float(((right.abs().sum(axis=1)) > 0).mean()),
            "baseline_mean_gross_exposure": float(left.abs().sum(axis=1).mean()),
            "augmented_mean_gross_exposure": float(right.abs().sum(axis=1).mean()),
            "scenarios": scenarios}


def _construction_relation(spec: dict, matrices: dict[str, pd.DataFrame], eligible: pd.DataFrame) -> dict:
    construction = spec.get("construction")
    if construction is None:
        return {"classification": "unresolved_statistical_evidence", "basis": "no_constructive_relation_declared"}
    selected = pd.Series(False, index=eligible.index)
    for window in [*spec["fit_windows"], *spec["evaluation_windows"]]:
        selected |= (eligible.index >= pd.Timestamp(window["start"])) & (eligible.index <= pd.Timestamp(window["end"]))
    dates = eligible.index[selected]
    candidate = matrices[spec["candidate"]].reindex(index=dates).where(eligible.reindex(index=dates))
    baseline = matrices[construction["baseline"]].reindex(index=dates).where(eligible.reindex(index=dates))
    common = candidate.notna() & baseline.notna()
    if int(common.sum().sum()) < _MIN_CROSS_SECTION:
        raise ResearchError("construction_mismatch", "Declared construction has insufficient comparable values")
    if construction["relation"] == "exact_duplicate":
        if (not candidate.notna().equals(baseline.notna())
                or not np.allclose(candidate.where(common).to_numpy(), baseline.where(common).to_numpy(),
                                   equal_nan=True, rtol=0, atol=0)):
            raise ResearchError("construction_mismatch", "Declared exact duplicate differs from its baseline")
        classification = "declared_exact_duplicate_values_verified"
    else:
        expected = {"increasing": 1.0, "decreasing": -1.0}.get(construction["monotonicity"])
        if expected is not None:
            observed = []
            for day in candidate.index:
                day_common = common.loc[day]
                value = _corr(_rank_series(candidate.loc[day].where(day_common)).to_numpy(),
                              _rank_series(baseline.loc[day].where(day_common)).to_numpy())
                if value is not None:
                    observed.append(value)
            if not observed or any(not np.isclose(value, expected, atol=1e-12) for value in observed):
                raise ResearchError("construction_mismatch", "Declared monotonic transform does not preserve the expected ranks")
        classification = "declared_known_transform_ranks_verified"
    return {"classification": classification, "basis": "caller_declaration_with_value_consistency_check",
            "independent_source_review": "not_performed_by_comparison_runtime",
            "evidence": construction["evidence"], "baseline": construction["baseline"],
            "monotonicity": construction.get("monotonicity")}


def _last_available_price_position(calendar: pd.DatetimeIndex, cutoff: dict) -> int:
    side = "left" if cutoff["phase"] == "pre_open" else "right"
    return int(calendar.searchsorted(pd.Timestamp(cutoff["date"]), side=side)) - 1


def _mature_dates(calendar: pd.DatetimeIndex, last_price_position: int, shift_n: int) -> pd.Series:
    return pd.Series(np.arange(len(calendar)) + shift_n <= last_price_position, index=calendar)


def _coverage_report(*, evaluation_dates: pd.DatetimeIndex, eligible: pd.DataFrame,
                     matrices: dict[str, pd.DataFrame], model_inputs: list[str], candidate: str,
                     returns: pd.DataFrame, mature_dates: pd.Series) -> dict:
    pit = eligible.reindex(index=evaluation_dates).astype(bool)
    candidate_available = matrices[candidate].reindex(index=evaluation_dates).notna()
    model_available = pit.copy()
    missing_by_signal = {}
    for name in model_inputs:
        available = matrices[name].reindex(index=evaluation_dates).notna()
        missing_by_signal[name] = int((pit & ~available).to_numpy().sum())
        model_available &= available
    mature = pd.DataFrame(
        np.repeat(mature_dates.reindex(evaluation_dates, fill_value=False).to_numpy()[:, None], pit.shape[1], axis=1),
        index=pit.index,
        columns=pit.columns,
    )
    label_available = returns.reindex(index=evaluation_dates).notna()
    pit_count = int(pit.to_numpy().sum())
    candidate_count = int((pit & candidate_available).to_numpy().sum())
    model_count = int(model_available.to_numpy().sum())
    paired = model_available & mature & label_available
    candidate_missing = int((pit & ~candidate_available).to_numpy().sum())
    other_missing = int((pit & candidate_available & ~model_available).to_numpy().sum())
    immature = int((model_available & ~mature).to_numpy().sum())
    missing_mature_label = int((model_available & mature & ~label_available).to_numpy().sum())
    paired_count = int(paired.to_numpy().sum())
    return {
        "grid_opportunities": int(pit.size),
        "pit_eligible_opportunities": pit_count,
        "candidate_available_on_pit": candidate_count,
        "common_model_input_opportunities": model_count,
        "paired_mature_label_opportunities": paired_count,
        "model_input_missing_on_pit_by_signal": missing_by_signal,
        "losses": {
            "pit_ineligible": int(pit.size - pit_count),
            "candidate_missing_on_pit": candidate_missing,
            "other_model_input_missing_after_candidate": other_missing,
            "label_not_mature_on_common_input": immature,
            "label_missing_despite_maturity": missing_mature_label,
        },
        "closure_holds": (
            pit_count == candidate_count + candidate_missing
            and candidate_count == model_count + other_missing
            and model_count == paired_count + immature + missing_mature_label
        ),
        "denominator_semantics": "declared_grid_then_canonical_pit_then_observed_inputs_and_mature_label",
    }


def _compute_window_result(*, spec: dict, fit_window: dict, evaluation_window: dict,
                           full: pd.DataFrame, target_rank: pd.Series, common: pd.Series,
                           matrices: dict[str, pd.DataFrame], returns: pd.DataFrame,
                           eligible: pd.DataFrame, calendar: pd.DatetimeIndex,
                            instruments: pd.Index, mature_dates: pd.Series, shift_n: int,
                            model_inputs: list[str]) -> dict:
    fit_start, fit_end = pd.Timestamp(fit_window["start"]), pd.Timestamp(fit_window["end"])
    eval_start, eval_end = pd.Timestamp(evaluation_window["start"]), pd.Timestamp(evaluation_window["end"])
    index_dates = full.index.get_level_values("datetime")
    fit_mask = (index_dates >= fit_start) & (index_dates <= fit_end)
    eval_mask = (index_dates >= eval_start) & (index_dates <= eval_end)
    fit_last_price_position = _last_available_price_position(calendar, fit_window["knowledge_cutoff"])
    if fit_last_price_position < 0:
        raise ResearchError("comparison_unavailable", "No close price is available by the fit knowledge cutoff")
    fit_mature_dates = _mature_dates(calendar, fit_last_price_position, shift_n)
    fit_mature_rows = full.index.get_level_values("datetime").map(fit_mature_dates).to_numpy(dtype=bool)
    fit_rows = fit_mask & common & fit_mature_rows & target_rank.reindex(full.index).notna()
    baseline_features = full.loc[fit_rows, spec["baseline"]]
    augmented_names = list(spec["baseline"])
    if spec["research_role"] == "replacement":
        augmented_names.remove(spec["replacement_target"])
        augmented_names.append(spec["candidate"])
        augmented_features = full.loc[:, augmented_names]
    elif spec["research_role"] == "conditional":
        interaction = "__candidate_state_interaction__"
        augmented_names.append(interaction)
        augmented_features = full.loc[:, spec["baseline"]].copy()
        augmented_features[interaction] = full[spec["candidate"]] * full[spec["state"]]
    else:
        augmented_names.append(spec["candidate"])
        augmented_features = full.loc[:, augmented_names]
    baseline_coef, baseline_fit = _weighted_fit(baseline_features, target_rank.reindex(full.index).loc[fit_rows])
    augmented_coef, augmented_fit = _weighted_fit(augmented_features.loc[fit_rows, augmented_names],
                                                   target_rank.reindex(full.index).loc[fit_rows])
    evaluation_rows = eval_mask & common
    baseline_prediction = _predict(full.loc[evaluation_rows, spec["baseline"]], baseline_coef)
    augmented_prediction = _predict(augmented_features.loc[evaluation_rows, augmented_names], augmented_coef)
    eval_target = target_rank.reindex(full.index).loc[evaluation_rows]
    daily = []
    similarity = []
    for day in sorted(set(baseline_prediction.index.get_level_values("datetime"))):
        left = baseline_prediction.xs(day, level="datetime")
        right = augmented_prediction.xs(day, level="datetime")
        actual = eval_target.xs(day, level="datetime")
        paired = left.notna() & right.notna() & actual.notna()
        baseline_ic = _spearman(left.where(paired), actual.where(paired))
        augmented_ic = _spearman(right.where(paired), actual.where(paired))
        if baseline_ic is not None and augmented_ic is not None:
            daily.append({"date": day.date().isoformat(), "baseline_rank_ic": baseline_ic,
                          "augmented_rank_ic": augmented_ic, "value": augmented_ic - baseline_ic,
                          "n": int(paired.sum())})
        similarity_value = _spearman(left, right)
        if similarity_value is not None:
            similarity.append({"date": day.date().isoformat(), "value": similarity_value,
                               "n": int((left.notna() & right.notna()).sum())})
    eval_dates = calendar[(calendar >= eval_start) & (calendar <= eval_end)]
    candidate_eval = (matrices[spec["candidate"]].reindex(index=eval_dates, columns=instruments)
                      * spec["direction"]["sign"])
    returns_eval = returns.reindex(index=eval_dates, columns=instruments)
    eligible_eval = eligible.reindex(index=eval_dates, columns=instruments, fill_value=False)
    original_ic = _daily_rank_ic(candidate_eval, returns_eval, eligible_eval)
    style_controls = [matrices[name].reindex(index=eval_dates, columns=instruments)
                      for name in spec["controls"]["style"]]
    neighbor_controls = style_controls + [matrices[name].reindex(index=eval_dates, columns=instruments)
                                          for name in spec["controls"]["neighbors"]]
    categorical = [matrices[name].reindex(index=eval_dates, columns=instruments)
                   for name in spec["controls"]["categorical"]]
    partial_style = _partial_rank_daily(candidate_eval, returns_eval, style_controls, categorical, eligible_eval)
    partial_neighbor = _partial_rank_daily(candidate_eval, returns_eval, neighbor_controls, categorical, eligible_eval)
    pairwise_signal_similarity = {}
    for baseline_name in spec["baseline"]:
        baseline_eval = matrices[baseline_name].reindex(index=eval_dates, columns=instruments)
        daily_similarity = _daily_rank_ic(candidate_eval, baseline_eval, eligible_eval)
        pairwise_signal_similarity[baseline_name] = {
            "daily": daily_similarity,
            "mean": float(np.mean([item["value"] for item in daily_similarity])) if daily_similarity else None,
            "scope": "declared_evaluation_window_and_pit_sample",
        }
    span_rows = fit_mask & common
    fit_candidate = full.loc[span_rows, spec["candidate"]].to_numpy(dtype=float)
    fit_baseline = np.column_stack([
        np.ones(int(span_rows.sum())), full.loc[span_rows, spec["baseline"]].to_numpy(dtype=float),
    ])
    span_residual, span_state = _residual(fit_candidate, fit_baseline)
    span_r2 = None
    if span_residual is not None:
        denominator = float(np.sum((fit_candidate - fit_candidate.mean()) ** 2))
        span_r2 = float(1 - np.sum(span_residual ** 2) / denominator) if denominator > 0 else None
    return {
        "fit_window_index": evaluation_window["fit_window_index"],
        "fit_window": fit_window,
        "evaluation_window": {key: evaluation_window[key] for key in ("start", "end")},
        "baseline_fit": baseline_fit,
        "augmented_fit": augmented_fit,
        "fit_label_maturity": {
            "knowledge_cutoff": fit_window["knowledge_cutoff"],
            "last_available_price_date": calendar[fit_last_price_position].date().isoformat(),
            "label_shift_n": shift_n,
            "mature_fit_rows": int(fit_rows.sum()),
        },
        "paired_rank_ic_delta": {"daily": daily, "hac": _hac(daily, spec["hac_maxlags"], calendar)},
        "prediction_similarity": {
            "daily": similarity,
            "mean": float(np.mean([item["value"] for item in similarity])) if similarity else None,
            "interpretation": "similarity_diagnostic_not_identity_or_value_evidence",
        },
        "candidate_raw_rank_ic": {"daily": original_ic,
                                  "mean": float(np.mean([item["value"] for item in original_ic])) if original_ic else None},
        "partial_rank_ic_style": partial_style,
        "partial_rank_ic_style_and_neighbors": partial_neighbor,
        "candidate_to_declared_baseline_similarity": pairwise_signal_similarity,
        "span_diagnostic": {"status": span_state["status"], "reason": span_state["reason"],
                            "r2": span_r2, "n": int(span_rows.sum()),
                            "classification": ("sample_dependent_r2" if span_r2 is not None
                                               else "spanning_undetermined"),
                            "interpretation": "sample_dependent_diagnostic_not_information_verdict"},
        "coverage": _coverage_report(
            evaluation_dates=eval_dates,
            eligible=eligible,
            matrices=matrices,
            model_inputs=model_inputs,
            candidate=spec["candidate"],
            returns=returns,
            mature_dates=mature_dates,
        ),
    }


def compute_comparison(spec: dict, signals: dict[str, pd.DataFrame], ctx: dict, run_spec: dict) -> dict:
    """Compute predeclared B versus B+F/replacement/interaction windows."""
    calendar = pd.DatetimeIndex(ctx["close_unstacked"].index)
    instruments = ctx["close_unstacked"].columns
    if (calendar.hasnans or calendar.has_duplicates or calendar.tz is not None
            or not calendar.is_monotonic_increasing or not calendar.equals(calendar.normalize())
            or instruments.has_duplicates):
        raise ResearchError("comparison_unavailable", "Comparison context requires ordered unique daily dates and instruments")
    matrices = {name: frame.iloc[:, 0].unstack("instrument").reindex(index=calendar, columns=instruments)
                for name, frame in signals.items()}
    cutoff = pd.Timestamp(run_spec["cutoff"])
    if any(pd.Timestamp(item["end"]) > cutoff for item in [*spec["fit_windows"], *spec["evaluation_windows"]]):
        raise ResearchError("comparison_unavailable", "Comparison window extends beyond cutoff")
    returns = ctx["fwd_ret_mats"][spec["horizon"]].reindex(index=calendar, columns=instruments)
    eligible = (ctx["st_pit_eligible_mask"].reindex(index=calendar, columns=instruments, fill_value=False)
                .fillna(False).astype(bool))
    for day, symbol in ctx.get("suspended_pairs") or set():
        day = pd.Timestamp(day)
        if day in eligible.index and symbol in eligible.columns:
            eligible.loc[day, symbol] = False
    last_price_position = _last_available_price_position(calendar, spec["knowledge_cutoff"])
    if last_price_position < 0:
        raise ResearchError("comparison_unavailable", "No close price is available by the knowledge cutoff")
    from backend.services.quantevolver.qe_eval_v2_metric_engine import HOLDING_PERIODS

    shift_n = HOLDING_PERIODS[spec["horizon"]]
    mature_rows = _mature_dates(calendar, last_price_position, shift_n)
    returns = returns.where(mature_rows, axis=0)
    model_inputs = list(dict.fromkeys([*spec["baseline"], spec["candidate"],
                                       *([spec["state"]] if spec.get("state") else [])]))
    model_available = eligible.copy()
    for name in model_inputs:
        model_available &= matrices[name].notna()
    ranked = {name: _rank_frame(matrices[name].where(model_available)) for name in model_inputs}
    ranked[spec["candidate"]] *= spec["direction"]["sign"]
    full = pd.concat({name: ranked[name].stack(future_stack=True) for name in model_inputs}, axis=1)
    full.index.names = ["datetime", "instrument"]
    target_rank = _rank_frame(returns.where(model_available)).stack(future_stack=True).rename("target")
    target_rank.index.names = ["datetime", "instrument"]
    eligibility = eligible.stack(future_stack=True).reindex(full.index).fillna(False).astype(bool)
    common = full[model_inputs].notna().all(axis=1) & eligibility
    windows = [
        _compute_window_result(
            spec=spec,
            fit_window=spec["fit_windows"][evaluation["fit_window_index"]],
            evaluation_window=evaluation,
            full=full,
            target_rank=target_rank,
            common=common,
            matrices=matrices,
            returns=returns,
            eligible=eligible,
            calendar=calendar,
            instruments=instruments,
            mature_dates=mature_rows,
            shift_n=shift_n,
            model_inputs=model_inputs,
        )
        for evaluation in spec["evaluation_windows"]
    ]
    information_relation = _construction_relation(spec, matrices, eligible)
    return {
        "schema_version": "factor_research_comparison_v1",
        "method_version": run_spec.get("method_version"),
        "research_role": spec["research_role"],
        "horizon": spec["horizon"],
        "fit_windows": spec["fit_windows"],
        "evaluation_windows": spec["evaluation_windows"],
        "direction": spec["direction"],
        "knowledge_cutoff": {**spec["knowledge_cutoff"],
                             "last_available_price_date": calendar[last_price_position].date().isoformat(),
                             "label_shift_n": shift_n},
        "baseline": spec["baseline"],
        "candidate": spec["candidate"],
        "controls": spec["controls"],
        "comparison_model": "separately_fitted_cross_sectional_rank_ols_equal_date_weight_v1",
        "run_scope": {
            "universe_key": run_spec.get("universe_key"),
            "instrument_count": len(run_spec.get("instruments", instruments)),
            "signal_start": run_spec.get("signal_start"),
            "signal_end": run_spec.get("signal_end"),
            "cutoff": run_spec["cutoff"],
        },
        "windows": windows,
        "cost": _cost_report(spec.get("cost"), spec["evaluation_windows"]),
        "information_relation": information_relation,
        "use_value": {
            "classification": "evidence_insufficient_until_interpreted_for_declared_role",
            "research_role": spec["research_role"],
            "not_claimed": ["qe_strategy_value", "tradability", "production_promotion"],
        },
        "next_step": "interpret_window_evidence_for_the_declared_role_before_any_expansion",
        "scope": "research_comparison_not_official_metrics_or_qe_result",
    }
