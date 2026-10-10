"""Two bounded L2 economic candidates. No database, product activation or search."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
import math
from pathlib import Path
from typing import Any, Mapping
import warnings

import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression

from backend.services.hmm_risk import risk_l2 as risk
from backend.services.hmm_risk import risk_l2_value_replay as rv
from backend.services.hmm_risk import rotation_l2 as rotation
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as hashes
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json
from scripts.hmm_risk import rotation_l2_reference_value as reference

VERSION = "hmm_risk_l2_economic_candidates_v1"
MANIFEST = "97df6acdbe43dc20f577e85f8cceb2814d73fca13b0f12cda7aefd90cfb62f2c"
BINDING = "52eca131ef3a678fb2ab68682801dbb01be454166b7804f20a91dceee3ef69b3"
ORIGINAL_MODEL = "37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d"
PINS = {
    "base_features": ("receipt_sha256", "2e9911a5fd2a83c15803e120b1e3c9a21a1a9ffed7f336e53e156c9acdde1c70"),
    "base_outcomes": ("receipt_sha256", "3101fcd9152eb87b8b207ac60071df5d241043cecb71fb5008d9a3f2dcd562f1"),
    "new_features": ("receipt_sha256", "1d182582ffad2144545e1f8b3cca8074d278f7ceb3fc4d32e43a4cb15aef60c7"),
    "new_outcomes": ("receipt_sha256", "d64b4913d4233ebe57390c0b94d15f599d900423d05fbbcbd3424f27fa5c52d7"),
    "rotation_input": ("input_hash", "5272571953d5977b25b4cafaf6e233ee4c7294346e26692a943c791f970671ed"),
    "rotation_outcomes": ("outcome_sha256", "2c889d380eccd55d655948aa9874b54883e52b78ff6587dbaf8fe8811da5d47a"),
}
TREE_PARAMS = {
    "loss": "squared_error",
    "learning_rate": 0.05,
    "n_estimators": 64,
    "subsample": 1.0,
    "criterion": "friedman_mse",
    "min_samples_split": 620,
    "min_samples_leaf": 310,
    "min_weight_fraction_leaf": 0.0,
    "max_depth": 2,
    "min_impurity_decrease": 0.0,
    "init": None,
    "random_state": 42,
    "max_features": None,
    "alpha": 0.9,
    "verbose": 0,
    "max_leaf_nodes": None,
    "warm_start": False,
    "validation_fraction": 0.1,
    "n_iter_no_change": None,
    "tol": 0.0001,
    "ccp_alpha": 0.0,
}
CONTRACT = {
    "version": VERSION,
    "manifest": MANIFEST,
    "binding": BINDING,
    "feature_names": list(ALL_CORE_FEATURES),
    "catalog_count": 131,
    "horizon": 10,
    "cost_sensitive_score_threshold": 0.2,
    "cost_positive": "min(-drawdown/0.08,3)",
    "cost_negative": "1+min(max(return,0)/0.08,2)",
    "weights": "date_equal_times_cost_normalized_mean_one",
    "risk_parameters": risk.PARAMS,
    "rotation_parameters": TREE_PARAMS,
    "rotation_train_days": 126,
    "rotation_months": 5,
    "window": ["2026-04-01", "2026-08-31"],
    "fresh_processes": 2,
    "planned_risk_fits": 2,
    "planned_rotation_fits": 10,
    "selection_basis": "RETROSPECTIVE_PREQUENTIAL_DEVELOPMENT",
    "advisory_status": "NOT_AVAILABLE",
    "forward_confirmation": "NOT_STARTED",
}
CONTRACT_HASH = canonical_sha256(CONTRACT)


def require(ok: bool, message: str, suffix: str = "identity_invalid") -> None:
    if not ok:
        raise rotation.RotationL2Error("hmm_risk_l2_economic_" + suffix, message)


def finite(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def load_sources(request: Mapping, names: tuple[str, ...]) -> dict:
    hashes.verify(request, "request_sha256")
    require(request.get("schema_version") == VERSION + "_request", "request schema differs")
    require(request.get("contract_hash") == CONTRACT_HASH, "request contract differs")
    require(set(request["sources"]) == set(PINS), "explicit six source paths required")
    result = {}
    for name in names:
        path = Path(request["sources"][name])
        require(path.is_absolute() and path.is_file(), name + " must be an absolute ordinary file")
        import os

        for parent in (path, *path.parents):
            require(not parent.is_symlink() and not os.path.isjunction(parent), "indirect source path")
        value = read_json(path)
        field, expected = PINS[name]
        hashes.verify(value, field)
        require(value[field] == expected, name + " pin differs")
        result[name] = value
    return result


def _release(identity: Mapping) -> None:
    require(identity.get("dataset_manifest_sha256") == MANIFEST, "release manifest differs")
    require(identity.get("frozen_release_binding_sha256") == BINDING, "release binding differs")
    require(identity.get("frozen_release_generation") == "20261002-v17-unified-basic-history1", "generation differs")
    require(identity.get("cutoff") == "2026-08-31", "cutoff differs")


def _grid(rows: Mapping, days: list[str], codes: list[str], calendar: list[str]) -> None:
    require(set(rows) == set(days), "observation dates differ")
    offsets = {d: i for i, d in enumerate(calendar)}
    for day in days:
        require(set(rows[day]) == set(codes), "131-sector observation grid differs")
        require(day in offsets and offsets[day] > 0, "observation calendar lacks prior context")
        for source in rows[day].values():
            require(source.get("as_of_date") == calendar[offsets[day] - 1], "feature as-of is not previous open date")
            x = source.get("features")
            if x is None:
                require(
                    isinstance(source.get("reason_code"), str) and bool(source["reason_code"]), "NA needs typed reason"
                )
            else:
                require(
                    isinstance(x, list) and len(x) == 20 and all(finite(v) for v in x),
                    "20D feature invalid",
                    "source_invalid",
                )


def monthly_schedule(calendar: list[str]) -> list[dict]:
    result = []
    for month in range(4, 9):
        days = [d for d in calendar if d.startswith(f"2026-{month:02d}-")]
        require(bool(days), "month has no frozen open dates")
        index = calendar.index(days[0])
        require(index >= 136, "126 train plus purge context is absent")
        result.append(
            {
                "origin": days[0],
                "as_of": calendar[index - 1],
                "train_days": calendar[index - 136 : index - 10],
                "prediction_days": days,
            }
        )
    return result


def rotation_shell(raw: Mapping, observation: Mapping) -> dict:
    structural = raw["structural_eligible"]
    require(type(structural) is bool, "structural eligibility must be boolean")
    available = structural and observation["features"] is not None
    result = {
        k: v
        for k, v in raw.items()
        if k not in {"x", "feature_contributions", "feature_diagnostics", "relative_return_10d"}
    }
    result.update(
        availability="available" if available else "unavailable",
        feature_eligible=available,
        rotation_score=0.0 if available else None,
        forecast_state="neutral" if available else None,
        reason_code=None if available else raw["reason_code"] if not structural else observation["reason_code"],
        outcome_status="PENDING_EVALUATION",
    )
    require(available or bool(result["reason_code"]), "unavailable rotation needs reason")
    return result


def prepare(request: Mapping, source_commit: str) -> dict:
    values = load_sources(request, ("base_features", "new_features", "rotation_input"))
    base, new, monthly = (values[k] for k in ("base_features", "new_features", "rotation_input"))
    require(
        base["schema_version"] == risk.VERSION + "_features" and base["contract"] == risk.CONTRACT,
        "base risk schema/contract differs",
    )
    require(new["schema_version"] == "hmm_risk_frozen_l2_history_v1_risk_features", "new risk schema differs")
    require(monthly["schema_version"] == "hmm_risk_rotation_l2_rolling_return_v1_input", "monthly schema differs")
    identities = [
        base["input_identity"]["release_identity"],
        new["new_source_identity"]["release_identity"],
        monthly["source_identity"]["release"],
    ]
    for identity in identities:
        _release(identity)
    require(identities[0] == identities[1] == identities[2], "cross-release source identity mismatch")
    require(base["input_identity"] == new["source_identity"], "base feature lineage differs")
    require(canonical_sha256(new["parameters"]) == ORIGINAL_MODEL, "old risk parameters differ")
    codes, calendar = base["catalog"], new["calendar"]
    require(len(codes) == 131 and codes == sorted(set(codes)), "canonical 131 L2 catalog required")
    require(new["catalog"] == codes == monthly["catalog"], "catalog identities differ")
    require(calendar == sorted(set(calendar)) and calendar[-1] == "2026-08-31", "calendar invalid")
    require(
        base["calendar"] == calendar[: len(base["calendar"])] and monthly["calendar"] == calendar,
        "calendar lineage differs",
    )
    require(base["feature_names"] == new["feature_names"] == list(ALL_CORE_FEATURES), "feature definition differs")
    plan = risk.schedule(base["calendar"])
    old_days = plan["train"] + plan["dev"]
    new_days = [d for d in calendar if "2026-04-01" <= d <= "2026-08-31"]
    require(len(new_days) == 104, "104-date historical ledger differs")
    _grid(base["rows"], old_days, codes, calendar)
    _grid(new["rows"], new_days, codes, calendar)
    rows = {**base["rows"], **new["rows"]}
    require(set(base["train_labels"]) == set(plan["train"]), "risk train target dates differ")
    risk_train = []
    for day in plan["train"]:
        require(set(base["train_labels"][day]) == set(codes), "risk train label catalog differs")
        for code in codes:
            target = base["train_labels"][day][code]
            risk._validate_target(target, mature=day in plan["fit"])
            if day in plan["fit"] and target["status"] == "AVAILABLE" and rows[day][code]["features"] is not None:
                risk_train.append({"trade_date": day, "sector_code": code, **target})
    months = []
    schedules = monthly_schedule(calendar)
    require([m["schedule"] for m in monthly["months"]] == schedules, "monthly causal schedule differs")
    for old, schedule in zip(monthly["months"], schedules, strict=True):
        facts = old["training_facts"]
        bound = [d for d in calendar if schedule["train_days"][0] <= d <= schedule["as_of"]]
        quote_keys = [(r["trade_date"], r["sector_code"]) for r in facts["sector_returns"]]
        require(
            len(quote_keys) == len(set(quote_keys)) and set(quote_keys) == {(d, c) for d in bound for c in codes},
            "monthly train quote scope differs",
        )
        require([r["trade_date"] for r in facts["benchmark_close"]] == bound, "train benchmark scope differs")
        shells = []
        for raw in old["training_rows"]:
            shells.append(rotation_shell(raw, rows[raw["trade_date"]][raw["sector_code"]]))
        require(
            {(r["trade_date"], r["sector_code"]) for r in shells}
            == {(d, c) for d in schedule["train_days"] for c in codes},
            "train prediction grid differs",
        )
        require(len(shells) == 126 * 131, "duplicate train shell")
        evaluated = rotation.evaluate_predictions_for_calendar(
            calendar=[date.fromisoformat(d) for d in calendar],
            sector_returns=facts["sector_returns"],
            benchmark_close=facts["benchmark_close"],
            predictions=shells,
            decision_start=date.fromisoformat(schedule["train_days"][0]),
            decision_end=date.fromisoformat(schedule["train_days"][-1]),
            outcome_end=date.fromisoformat(schedule["as_of"]),
            report_blocks=(),
        )
        train = [
            {"trade_date": r["trade_date"], "sector_code": r["sector_code"], "target": r["relative_return_10d"]}
            for r in evaluated["evaluated_rows"]
            if r["outcome_status"] == "available"
        ]
        prediction = [rotation_shell(r, rows[r["trade_date"]][r["sector_code"]]) for r in old["prediction_rows"]]
        require(
            len(prediction) == len(schedule["prediction_days"]) * 131
            and {(r["trade_date"], r["sector_code"]) for r in prediction}
            == {(d, c) for d in schedule["prediction_days"] for c in codes},
            "prediction full grid differs",
        )
        months.append({"schedule": schedule, "training": train, "prediction_shells": prediction})
    return hashes.seal(
        {
            "schema_version": VERSION + "_input",
            "contract_hash": CONTRACT_HASH,
            "request": request,
            "source_commit": source_commit,
            "calendar": calendar,
            "catalog": codes,
            "rows": rows,
            "risk_training": risk_train,
            "risk_windows": [plan["dev"], new_days],
            "months": months,
            "original_risk_parameters": new["parameters"],
            "fixed_return_parameters": monthly["fixed_parameters"],
            "delta_rows": monthly["delta_rows"],
            "source_identity": monthly["source_identity"],
        },
        "input_sha256",
    )


def validate_input(value: Mapping) -> None:
    hashes.verify(value, "input_sha256")
    require(
        value.get("schema_version") == VERSION + "_input" and value.get("contract_hash") == CONTRACT_HASH,
        "prepared contract differs",
    )
    hashes.verify(value["request"], "request_sha256")
    require(value["request"]["contract_hash"] == CONTRACT_HASH, "prepared request differs")
    require(
        len(value["catalog"]) == 131 and value["catalog"] == sorted(set(value["catalog"])), "prepared catalog differs"
    )
    require(
        value["months"] and [m["schedule"] for m in value["months"]] == monthly_schedule(value["calendar"]),
        "prepared schedule differs",
    )
    _grid(value["rows"], sorted(value["rows"]), value["catalog"], value["calendar"])
    _release(value["source_identity"]["release"])
    require(
        canonical_sha256(value["original_risk_parameters"]) == ORIGINAL_MODEL, "prepared original parameters differ"
    )


def date_weights(entries: list[dict]) -> np.ndarray:
    require(bool(entries), "no mature usable training rows", "training_insufficient")
    counts = Counter(e["trade_date"] for e in entries)
    return np.asarray([len(entries) / (len(counts) * counts[e["trade_date"]]) for e in entries], dtype=np.float64)


def cost_weights(entries: list[dict]) -> np.ndarray:
    costs = []
    for row in entries:
        risk._validate_target({k: row[k] for k in ("status", "event", "drawdown", "return")}, mature=True)
        costs.append(min(-row["drawdown"] / 0.08, 3.0) if row["event"] else 1 + min(max(row["return"], 0) / 0.08, 2.0))
    weights = date_weights(entries) * np.asarray(costs)
    require(np.isfinite(weights).all() and (weights > 0).all(), "training costs invalid", "fit_failed")
    return weights / weights.mean()


def scaler(x: np.ndarray) -> dict:
    require(
        x.ndim == 2 and x.shape[1] == 20 and len(x) > 0 and np.isfinite(x).all(),
        "train 20D matrix invalid",
        "fit_failed",
    )
    active = np.any(x != x[0], axis=0)
    mean, scale = x.mean(axis=0), x.std(axis=0, ddof=0)
    mean[~active], scale[~active] = x[0, ~active], 0.0
    require(
        active.any() and (scale[active] > 0).all() and np.isfinite(mean).all() and np.isfinite(scale).all(),
        "train scaling invalid",
        "fit_failed",
    )
    return {"mean": mean.tolist(), "scale": scale.tolist(), "active": active.tolist()}


def transform(x: np.ndarray, parameters: Mapping) -> np.ndarray:
    require(
        len(parameters["active"]) == 20 and all(type(v) is bool for v in parameters["active"]), "active mask invalid"
    )
    active = np.asarray(parameters["active"], dtype=bool)
    mean, scale = np.asarray(parameters["mean"]), np.asarray(parameters["scale"])
    require(
        mean.shape == scale.shape == (20,)
        and active.any()
        and (scale[active] > 0).all()
        and (scale[~active] == 0).all()
        and np.isfinite(mean).all()
        and np.isfinite(scale).all(),
        "restored scaler invalid",
    )
    require(x.ndim == 2 and x.shape[1] == 20 and np.isfinite(x).all(), "inference feature shape/values invalid")
    z = (x[:, active] - mean[active]) / scale[active]
    require(np.isfinite(z).all(), "nonfinite transformed features", "numeric_invalid")
    return z


def matrix(entries: list[dict], rows: Mapping) -> np.ndarray:
    return np.asarray([rows[e["trade_date"]][e["sector_code"]]["features"] for e in entries], dtype=np.float64)


def fit_risk(bundle: Mapping, progress: dict) -> dict:
    entries = bundle["risk_training"]
    y = np.asarray([r["event"] for r in entries], dtype=np.int64)
    require(set(y) == {0, 1}, "risk training classes insufficient", "training_insufficient")
    x = matrix(entries, bundle["rows"])
    parameters = scaler(x)
    model = LogisticRegression(**risk.PARAMS)
    weights = cost_weights(entries)
    progress["started_fits"] += 1
    print("fit_start risk 1/1", flush=True)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ConvergenceWarning)
        model.fit(transform(x, parameters), y, sample_weight=weights)
    progress["completed_fits"] += 1
    require(
        model.classes_.tolist() == [0, 1] and np.isfinite(model.coef_).all() and np.isfinite(model.intercept_).all(),
        "fitted logistic values invalid",
        "fit_failed",
    )
    parameters.update(
        coef=model.coef_.tolist(),
        intercept=model.intercept_.tolist(),
        classes=[0, 1],
        iterations=model.n_iter_.tolist(),
    )
    print("fit_done risk 1/1", flush=True)
    return {
        "parameters": parameters,
        "fit_rows": len(entries),
        "fit_dates": len({e["trade_date"] for e in entries}),
        "weights_sha256": canonical_sha256(weights.tolist()),
        "weight_sum": float(weights.sum()),
    }


def risk_predict(bundle: Mapping, parameters: Mapping) -> list[dict]:
    days = [d for window in bundle["risk_windows"] for d in window]
    rows, _ = risk.predictions_from_parameters(bundle["rows"], days, bundle["catalog"], parameters)
    for row in rows:
        row["risk_action_score"] = row.pop("probability")
        row["score_semantics"] = "COST_SENSITIVE_ACTION_SCORE_NOT_EVENT_PROBABILITY"
    return rows


def tree_predict(x: np.ndarray, parameters: Mapping) -> np.ndarray:
    z = transform(x, parameters).astype(np.float32)
    require(np.isfinite(z).all(), "tree float32 features overflow", "numeric_invalid")
    require(
        parameters.get("learning_rate") == TREE_PARAMS["learning_rate"]
        and len(parameters["trees"]) == TREE_PARAMS["n_estimators"],
        "tree contract differs",
    )
    require(finite(parameters["init"]), "tree initial value invalid")
    result = np.full(len(z), parameters["init"], dtype=np.float64)
    for tree in parameters["trees"]:
        left, right, features, thresholds, values = (
            tree[k] for k in ("left", "right", "feature", "threshold", "value")
        )
        n = len(left)
        require(
            n > 0
            and all(len(v) == n for v in (right, features, thresholds, values))
            and all(finite(v) for v in values + thresholds),
            "tree arrays invalid",
        )
        require(all(type(v) is int for v in left + right + features), "tree indices invalid")
        nodes = np.zeros(len(z), dtype=int)
        for _ in range(n):
            active_rows = np.flatnonzero(np.asarray(left)[nodes] != -1)
            if len(active_rows) == 0:
                break
            for row in active_rows:
                node = int(nodes[row])
                feature = features[node]
                require(0 <= feature < z.shape[1], "tree feature out of range")
                child = left[node] if z[row, feature] <= thresholds[node] else right[node]
                require(0 <= child < n and child > node, "cyclic/out-of-range tree")
                nodes[row] = child
        require(all(left[i] == right[i] == -1 for i in nodes), "tree traversal did not reach leaves")
        result += parameters["learning_rate"] * np.asarray(values)[nodes]
    require(np.isfinite(result).all(), "tree output nonfinite", "prediction_failed")
    return result


def fit_rotation(bundle: Mapping, progress: dict) -> list[dict]:
    result = []
    for number, month in enumerate(bundle["months"], 1):
        entries = month["training"]
        require(
            bool(entries)
            and all(e["trade_date"] in month["schedule"]["train_days"] and finite(e["target"]) for e in entries),
            "rotation train labels invalid",
        )
        x = matrix(entries, bundle["rows"])
        parameters = scaler(x)
        z = transform(x, parameters)
        model = GradientBoostingRegressor(**TREE_PARAMS)
        progress["started_fits"] += 1
        print(f"fit_start rotation {number}/5 origin={month['schedule']['origin']}", flush=True)
        model.fit(z, np.asarray([e["target"] for e in entries]), sample_weight=date_weights(entries))
        progress["completed_fits"] += 1
        trees, dates = [], np.asarray([e["trade_date"] for e in entries])
        leaf_dates = []
        for estimator in model.estimators_[:, 0]:
            tree = estimator.tree_
            trees.append(
                {
                    "left": tree.children_left.tolist(),
                    "right": tree.children_right.tolist(),
                    "feature": tree.feature.tolist(),
                    "threshold": tree.threshold.tolist(),
                    "value": tree.value[:, 0, 0].tolist(),
                }
            )
            leaves = estimator.apply(z)
            leaf_dates.append({str(leaf): len(set(dates[leaves == leaf])) for leaf in sorted(set(leaves))})
        parameters.update(
            init=float(model.init_.constant_[0, 0]), learning_rate=TREE_PARAMS["learning_rate"], trees=trees
        )
        require(
            np.array_equal(model.predict(z), tree_predict(x, parameters)),
            "JSON tree restore differs from sklearn",
            "prediction_failed",
        )
        result.append(
            {
                "origin": month["schedule"]["origin"],
                "parameters": parameters,
                "fit_rows": len(entries),
                "fit_dates": len(set(dates)),
                "leaf_distinct_dates": leaf_dates,
                "training_sha256": canonical_sha256(entries),
                "sklearn_parameters": model.get_params(),
            }
        )
        print(f"fit_done rotation {number}/5", flush=True)
    return result


def rotation_predict(bundle: Mapping, models: list[dict]) -> list[dict]:
    require(
        [m["origin"] for m in models] == [m["schedule"]["origin"] for m in bundle["months"]], "restored origins differ"
    )
    output = []
    for month, model in zip(bundle["months"], models, strict=True):
        rows = [dict(r) for r in month["prediction_shells"]]
        eligible = [r for r in rows if r["availability"] == "available"]
        scores = tree_predict(matrix(eligible, bundle["rows"]), model["parameters"]) if eligible else []
        by_day = defaultdict(dict)
        for row, score in zip(eligible, scores, strict=True):
            by_day[row["trade_date"]][row["sector_code"]] = float(score)
        for day in month["schedule"]["prediction_days"]:
            try:
                ranks, states = rotation._score_and_states(by_day[day])
            except rotation.RotationL2Error as exc:
                if exc.reason_code != "hmm_risk_rotation_l2_cross_section_insufficient":
                    raise
                ranks, states = {}, {}
            for row in [r for r in rows if r["trade_date"] == day]:
                code = row["sector_code"]
                if code in ranks:
                    row.update(rotation_score=ranks[code], forecast_state=states[code], raw_score=by_day[day][code])
                elif row["availability"] == "available":
                    row.update(
                        availability="unavailable",
                        feature_eligible=False,
                        rotation_score=None,
                        forecast_state=None,
                        reason_code="hmm_risk_rotation_l2_cross_section_insufficient",
                    )
                output.append(row)
    return output


def execute(bundle: Mapping, candidate: str, progress: dict) -> dict:
    validate_input(bundle)
    require(candidate in {"risk", "rotation"}, "unknown candidate")
    models = fit_risk(bundle, progress) if candidate == "risk" else fit_rotation(bundle, progress)
    predictions = (
        risk_predict(bundle, models["parameters"]) if candidate == "risk" else rotation_predict(bundle, models)
    )
    return hashes.seal(
        {
            "schema_version": VERSION + "_sealed",
            "candidate": candidate,
            "input_sha256": bundle["input_sha256"],
            "contract_hash": CONTRACT_HASH,
            "models": models,
            "model_sha256": canonical_sha256(models),
            "predictions": predictions,
            "prediction_sha256": canonical_sha256(predictions),
            "fit_counts": dict(progress),
        },
        "sealed_sha256",
    )


def readback(bundle: Mapping, sealed: Mapping) -> None:
    hashes.verify(sealed, "sealed_sha256")
    require(
        sealed["input_sha256"] == bundle["input_sha256"] and sealed["contract_hash"] == CONTRACT_HASH,
        "child input/contract mismatch",
    )
    require(sealed["model_sha256"] == canonical_sha256(sealed["models"]), "child model hash differs")
    candidate = sealed["candidate"]
    require(candidate in {"risk", "rotation"}, "child candidate invalid")
    if candidate == "risk":
        trained = sealed["models"]
        entries = bundle["risk_training"]
        weights = cost_weights(entries)
        require(
            trained["fit_rows"] == len(entries)
            and trained["fit_dates"] == len({e["trade_date"] for e in entries})
            and trained["weights_sha256"] == canonical_sha256(weights.tolist())
            and trained["weight_sum"] == float(weights.sum()),
            "risk training ledger differs",
        )
    else:
        for month, trained in zip(bundle["months"], sealed["models"], strict=True):
            entries = month["training"]
            require(
                trained["training_sha256"] == canonical_sha256(entries)
                and trained["fit_rows"] == len(entries)
                and trained["sklearn_parameters"] == TREE_PARAMS,
                "rotation training/parameter ledger differs",
            )
    expected = (
        risk_predict(bundle, sealed["models"]["parameters"])
        if candidate == "risk"
        else rotation_predict(bundle, sealed["models"])
    )
    require(
        canonical_sha256(expected) == sealed["prediction_sha256"] == canonical_sha256(sealed["predictions"]),
        "parent zero-fit prediction readback differs",
    )
    fits = 1 if candidate == "risk" else 5
    require(sealed["fit_counts"] == {"started_fits": fits, "completed_fits": fits}, "child fit ledger differs")


def risk_reference(
    codes: list[str], days: list[str], candidate: list[dict], original: list[dict], returns: Mapping
) -> dict:
    require(set(returns) == set(days[1:]), "risk valuation dates differ")
    cgrid = {(r["trade_date"], r["sector_code"]): r for r in candidate}
    rgrid = {(r["trade_date"], r["sector_code"]): r for r in original}
    require(
        len(cgrid) == len(candidate)
        and len(rgrid) == len(original)
        and set(cgrid) == set(rgrid) == {(d, c) for d in days for c in codes},
        "risk prediction full grid differs",
    )
    arms = ("B", "C", "R", "XC", "XR")
    paths = {a: {bp: [] for bp in rv.COST_BPS} for a in arms}
    previous = {a: {c: 0.0 for c in codes} for a in arms}
    depleted = dict.fromkeys(arms, False)
    daily = []
    for s, t in zip(days[:-1], days[1:], strict=True):
        eligible = []
        for code in codes:
            new, old = cgrid[s, code], rgrid[s, code]
            require(
                new["availability"] == old["availability"] and new["as_of_date"] == old["as_of_date"] < s,
                "risk ex-ante population mismatch",
            )
            if new["availability"] == "available":
                require(
                    finite(new["risk_action_score"])
                    and 0 <= new["risk_action_score"] <= 1
                    and new["warning"] == (new["risk_action_score"] >= 0.2),
                    "candidate action score invalid",
                )
                require(
                    finite(old["probability"])
                    and 0 <= old["probability"] <= 1
                    and old["warning"] == (old["probability"] >= 0.2),
                    "old probability invalid",
                )
                eligible.append(code)
            else:
                require(
                    new["risk_action_score"] is None and new["warning"] is None and bool(new["reason_code"]),
                    "candidate NA semantics invalid",
                )
        values = returns[t]
        require(
            set(values) == set(codes) and all(v is None or (finite(v) and v >= -1) for v in values.values()),
            "reference return invalid",
        )
        targets = {a: dict.fromkeys(codes, 0.0) for a in arms}
        for c in eligible:
            targets["B"][c] = 1 / 131
            targets["C"][c] = 0.0 if cgrid[s, c]["warning"] else 1 / 131
            targets["R"][c] = 0.0 if rgrid[s, c]["warning"] else 1 / 131
        for a, control in (("C", "XC"), ("R", "XR")):
            exposure = math.fsum(targets[a].values())
            for c in eligible:
                targets[control][c] = exposure / len(eligible)
        available = all(values[c] is not None for c in eligible)
        summary = {}
        for a in arms:
            summary[a], previous[a], depleted[a] = rv.arm_day(
                codes, targets[a], values, previous[a], depleted[a], available
            )
        paired = all(summary[a]["gross_return"] is not None for a in arms)
        for a in arms:
            for bp in rv.COST_BPS:
                paths[a][bp].append(summary[a]["cost_sensitivity_return"][str(bp)] if paired else None)
        daily.append(
            {
                "signal_date": s,
                "return_date": t,
                "eligible": len(eligible),
                "candidate_warnings": sum(cgrid[s, c]["warning"] for c in eligible),
                "original_warnings": sum(rgrid[s, c]["warning"] for c in eligible),
                "arms": summary,
                "paired_return_available": paired,
            }
        )
    summaries = {
        a: {str(bp): rv._blocks(days[1:], values) for bp, values in prices.items()} for a, prices in paths.items()
    }
    comparisons = {}
    for bp in rv.COST_BPS:
        comparisons[str(bp)] = {}
        for a, control in (("C", "XC"), ("R", "XR"), ("C", "R"), ("C", "B")):
            first, second = summaries[a][str(bp)], summaries[control][str(bp)]
            require(
                [(b["start"], b["end"], b["date_count"]) for b in first]
                == [(b["start"], b["end"], b["date_count"]) for b in second],
                "paired cost block dates differ",
            )
            diffs = {
                date.fromisoformat(d): x - y
                for d, x, y in zip(days[1:], paths[a][bp], paths[control][bp], strict=True)
                if x is not None and y is not None
            }
            comparisons[str(bp)][a + "_minus_" + control] = {
                "daily_return_hac": rotation._newey_west([date.fromisoformat(d) for d in days[1:]], diffs, lag=9),
                "blocks": [
                    {
                        "start": x["start"],
                        "end": x["end"],
                        "date_count": x["date_count"],
                        **{
                            k: x[k] - y[k]
                            for k in (
                                "cumulative_return",
                                "maximum_drawdown",
                                "worst_daily_return",
                                "downside_squared_loss",
                            )
                        },
                    }
                    for x, y in zip(first, second, strict=True)
                ],
            }
    return {
        "daily": daily,
        "cost_blocks": summaries,
        "comparisons": comparisons,
        "full_path_available": all(v is not None for a in paths.values() for p in a.values() for v in p),
        "net_return_status": "UNASSESSED",
    }


def risk_events(predictions: list[dict], labels: Mapping) -> dict:
    known = [
        (r, labels[r["trade_date"]][r["sector_code"]])
        for r in predictions
        if r["availability"] == "available" and labels[r["trade_date"]][r["sector_code"]]["status"] == "AVAILABLE"
    ]
    tp = sum(t["event"] == 1 and r["warning"] for r, t in known)
    fp = sum(t["event"] == 0 and r["warning"] for r, t in known)
    fn = sum(t["event"] == 1 and not r["warning"] for r, t in known)
    false_upside = [t["return"] for r, t in known if r["warning"] and t["event"] == 0]
    missed_loss = [t["drawdown"] for r, t in known if not r["warning"] and t["event"] == 1]
    return {
        "M": len(known),
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "false_alert_future_return": rv._distribution(false_upside),
        "missed_event_drawdown": rv._distribution(missed_loss),
    }


def fixed_predictions(bundle: Mapping, rotation_input: Mapping) -> list[dict]:
    parameters = bundle["fixed_return_parameters"]
    hashes.verify(parameters, "parameter_sha256")
    names = parameters["feature_names"]
    require(len(names) == len(parameters["coefficients"]) == 4, "fixed four-feature control differs")
    rows = [r for m in rotation_input["months"] for r in m["prediction_rows"]]
    for row in rows:
        if row["availability"] == "available":
            require(
                isinstance(row["x"], list) and len(row["x"]) == 4 and all(finite(v) for v in row["x"]),
                "fixed control feature invalid",
            )
    predictions = hashes.linear_predictions_for_rows(rows, parameters, term_names=tuple(names))
    for row in predictions:
        if row["availability"] == "available" and row["rotation_score"] is None:
            row.update(
                availability="unavailable",
                feature_eligible=False,
                forecast_state=None,
                reason_code="hmm_risk_rotation_l2_cross_section_insufficient",
            )
    return predictions


def evaluate(bundle: Mapping, sealed: Mapping) -> dict:
    readback(bundle, sealed)
    days, codes = bundle["risk_windows"][1], bundle["catalog"]
    calendar = [date.fromisoformat(d) for d in bundle["calendar"]]
    if sealed["candidate"] == "risk":
        facts = load_sources(bundle["request"], ("base_outcomes", "new_outcomes"))
        base, new = facts["base_outcomes"], facts["new_outcomes"]
        require(
            base["feature_sha256"] == PINS["base_features"][1] and new["feature_sha256"] == PINS["new_features"][1],
            "risk outcome feature parent differs",
        )
        old = risk.predictions_from_parameters(
            bundle["rows"], [d for w in bundle["risk_windows"] for d in w], codes, bundle["original_risk_parameters"]
        )[0]
        result = []
        for window, outcome in zip(bundle["risk_windows"], (base, new), strict=True):
            candidate = [r for r in sealed["predictions"] if r["trade_date"] in set(window)]
            original = [r for r in old if r["trade_date"] in set(window)]
            event_returns = outcome["returns"] if outcome is base else outcome["event_returns"]
            labels = risk.drawdown_outcomes(bundle["calendar"], window, codes, event_returns, window[-1])
            result.append(
                {
                    "start": window[0],
                    "end": window[-1],
                    "candidate_events": risk_events(candidate, labels),
                    "original_events": risk_events(original, labels),
                    "reference": risk_reference(codes, window, candidate, original, outcome["returns"]),
                }
            )
        return {"windows": result, "effect_status": "ECONOMIC_VECTOR_REPORTED_NOT_PROMOTED"}
    sources = load_sources(bundle["request"], ("rotation_input", "rotation_outcomes"))
    original, facts = sources["rotation_input"], sources["rotation_outcomes"]
    require(facts["input_hash"] == original["input_hash"], "rotation outcome parent differs")
    arms = {
        "candidate": sealed["predictions"],
        "fixed_return": fixed_predictions(bundle, original),
        "delta": bundle["delta_rows"],
    }
    metrics = {}
    for arm, rows in arms.items():
        evaluation = rotation.evaluate_predictions_for_calendar(
            calendar=calendar,
            sector_returns=facts["sector_returns"],
            benchmark_close=facts["benchmark_close"],
            predictions=rows,
            decision_start=date.fromisoformat(days[0]),
            decision_end=date.fromisoformat(days[-1]),
            outcome_end=date.fromisoformat(days[-1]),
            report_blocks=(),
        )
        metrics[arm] = {k: v for k, v in evaluation.items() if k != "evaluated_rows"}
    grids = {a: {(r["trade_date"], r["sector_code"]): r for r in rows} for a, rows in arms.items()}
    require(
        all(len(g) == 104 * 131 and set(g) == {(d, c) for d in days for c in codes} for g in grids.values()),
        "rotation comparison grid differs",
    )
    groups = {a: {} for a in (*arms, "no_order")}
    common_rows = {a: [] for a in arms}
    for day in days:
        common = {c for c in codes if all(g[day, c]["availability"] == "available" for g in grids.values())}
        for arm, grid in grids.items():
            for code in codes:
                row = dict(grid[day, code])
                if row["availability"] == "available" and code not in common:
                    row.update(
                        availability="unavailable",
                        feature_eligible=False,
                        forecast_state=None,
                        rotation_score=None,
                        reason_code="hmm_risk_l2_economic_common_input_unavailable",
                    )
                common_rows[arm].append(row)
    common_metrics = {}
    for arm, rows in common_rows.items():
        evaluation = rotation.evaluate_predictions_for_calendar(
            calendar=calendar,
            sector_returns=facts["sector_returns"],
            benchmark_close=facts["benchmark_close"],
            predictions=rows,
            decision_start=date.fromisoformat(days[0]),
            decision_end=date.fromisoformat(days[-1]),
            outcome_end=date.fromisoformat(days[-1]),
            report_blocks=(),
        )
        common_metrics[arm] = {k: v for k, v in evaluation.items() if k != "evaluated_rows"}
    for day in days[:-10]:
        common = [c for c in codes if all(g[day, c]["availability"] == "available" for g in grids.values())]
        groups["no_order"][day] = common
        for arm in arms:
            groups[arm][day] = [c for c in common if grids[arm][day, c]["forecast_state"] == "trending"]
    returns = facts["synthetic_returns"]
    require(
        set(returns) == set(days) and all(set(v) == set(codes) for v in returns.values()),
        "synthetic return grid differs",
    )
    quotes = {(d, c): v for d, r in returns.items() for c, v in r.items()}
    paths, summaries, comparisons = {}, {}, {}
    for bp in reference.COSTS:
        paths[bp] = {
            a: reference.cohort_reference_path(days, days[:-10], g, quotes, cost_bps=bp) for a, g in groups.items()
        }
        summaries[str(bp)] = {a: reference._summary(path, initial_nav=1.0) for a, path in paths[bp].items()}
        comparisons[str(bp)] = {
            a: reference.paired(days, paths[bp]["candidate"], paths[bp][a])
            for a in ("fixed_return", "delta", "no_order")
        }
    return {
        "metrics": metrics,
        "common_population_metrics": common_metrics,
        "reference_summaries": summaries,
        "paired": comparisons,
        "reference_basis": "PIT_AGGREGATE_SYNTHETIC_NOT_OFFICIAL_INDEX_OR_QE_NET",
        "effect_status": metrics["candidate"]["effect_status"],
        "net_return_status": "UNASSESSED",
    }
