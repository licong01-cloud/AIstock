"""Approved, single-candidate L2 moneyflow Ridge increment. File-only, no tail."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import math
from pathlib import Path
import sys
from typing import Any, Mapping

import numpy as np

from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk import rotation_l2 as baseline

VERSION = "hmm_risk_rotation_l2_moneyflow_supervised_v1"
INPUT_SCHEMA = VERSION + "_input"
PROCESS_SCHEMA = VERSION + "_process"
ACCEPTANCE_SCHEMA = VERSION + "_acceptance"
BASIS = "HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT"
TRAIN_START, TRAIN_END = date(2024, 9, 19), date(2025, 3, 31)
TRAIN_OUTCOME_END = date(2025, 4, 15)
PREDICTION_START, PREDICTION_END = date(2025, 4, 16), date(2026, 3, 31)
BLOCKS = (("first", PREDICTION_START, date(2025, 9, 30)), ("second", date(2025, 10, 1), PREDICTION_END))
FEATURE_NAMES = ("moneyflow_level_rank", "moneyflow_delta_rank")
CONTRACT = {
    "version": VERSION,
    "model_type": "pooled_ridge_moneyflow_rank",
    "population_contract_hash": baseline.MODEL_HASH,
    "feature_names": list(FEATURE_NAMES),
    "feature_source_sessions": 25,
    "intensity_sessions": 20,
    "delta_lag": 5,
    "horizon": 10,
    "target": "daily_average_rank_of_official_l2_relative_10d_return",
    "train_start": TRAIN_START.isoformat(),
    "train_end": TRAIN_END.isoformat(),
    "train_outcome_end": TRAIN_OUTCOME_END.isoformat(),
    "train_sessions": 126,
    "purge_sessions": 10,
    "prediction_start": PREDICTION_START.isoformat(),
    "prediction_end": PREDICTION_END.isoformat(),
    "prediction_sessions": 232,
    "mature_sessions": 222,
    "weights": "1/(usable_dates*date_samples)",
    "alpha": 0.01,
    "fit_intercept": True,
    "solver": "svd",
    "positive": False,
    "seed": "not_applicable",
    "refit": False,
    "planned_fits": 2,
    "rank": "exact_average_rank_scaled_minus_half",
    "state": "baseline_20pct_tie_neutral",
    "tail_forbidden_from": "2026-04-01",
}
MODEL_CONTRACT_HASH = canonical_sha256(CONTRACT)
EVALUATION_CONTRACT = {
    "version": VERSION,
    "start": PREDICTION_START.isoformat(),
    "end": PREDICTION_END.isoformat(),
    "horizon": 10,
    "hac_lag": 9,
    "binding_mean_rank_ic": 0.02,
    "coverage": 0.90,
    "blocks": [[n, a.isoformat(), b.isoformat()] for n, a, b in BLOCKS],
    "basis": BASIS,
    "selection_basis": "RETROSPECTIVE_DEVELOPMENT_SELECTED",
    "comparison": "same_input_window_population_delta_baseline",
}
EVALUATION_HASH = canonical_sha256(EVALUATION_CONTRACT)


def fail(message: str, suffix: str = "identity_invalid") -> baseline.RotationL2Error:
    return baseline.RotationL2Error(f"hmm_risk_rotation_l2_supervised_{suffix}", message)


def seal(body: Mapping[str, Any], field: str) -> dict[str, Any]:
    result = dict(body)
    result[field] = canonical_sha256(result)
    return result


def verify(value: Mapping[str, Any], field: str) -> None:
    if not isinstance(value, Mapping) or value.get(field) != canonical_sha256(
        {k: v for k, v in value.items() if k != field}
    ):
        raise fail(f"{field} differs from canonical content")


def schedule(calendar: list[date]) -> tuple[list[date], list[date]]:
    if calendar != sorted(set(calendar)):
        raise fail("calendar is not sorted unique")
    try:
        first, last = calendar.index(TRAIN_START), calendar.index(TRAIN_END)
        pred, end = calendar.index(PREDICTION_START), calendar.index(PREDICTION_END)
    except ValueError as exc:
        raise fail("approved calendar boundary is missing") from exc
    if (
        first < 25
        or last - first + 1 != 126
        or pred - last != 11
        or calendar[pred - 1] != TRAIN_OUTCOME_END
        or end - pred + 1 != 232
        or calendar[end - 10] != date(2026, 3, 17)
        or calendar[first - 25] != date(2024, 8, 13)
    ):
        raise fail("126/10/232/222 calendar accounting differs")
    return calendar[first : last + 1], calendar[pred : end + 1]


def validate_input(bundle: Mapping[str, Any]) -> dict[str, Any]:
    verify(bundle, "input_hash")
    if bundle.get("schema_version") != INPUT_SCHEMA or bundle.get("contract") != CONTRACT:
        raise fail("input schema or approved contract differs")
    source = bundle.get("source")
    if not isinstance(source, Mapping):
        raise fail("source is absent")
    validated = baseline.validate_input_bundle(source)
    schedule(list(validated["calendar"]))
    if source.get("outcome_bounds") != {"start": TRAIN_START.isoformat(), "end": TRAIN_OUTCOME_END.isoformat()}:
        raise fail("training outcome boundary differs")
    for section in (source["sector_returns"], source["benchmark_close"]):
        if any(not TRAIN_START <= date.fromisoformat(row["trade_date"]) <= TRAIN_OUTCOME_END for row in section):
            raise fail("fit child received non-training outcomes", "causal_boundary_invalid")
    if not isinstance(source.get("evaluation_source_binding"), Mapping):
        raise fail("evaluation source binding is absent")
    return validated


def prepare_inputs(**source_args: Any) -> dict[str, Any]:
    from backend.services.hmm_risk.rotation_l2_input import build_rotation_l2_input_bundle

    source = build_rotation_l2_input_bundle(
        **source_args,
        outcome_end=TRAIN_OUTCOME_END,
        bounded_benchmark=True,
    )
    bundle = seal({"schema_version": INPUT_SCHEMA, "contract": CONTRACT, "source": source}, "input_hash")
    validate_input(bundle)
    return bundle


def feature_rows(bundle: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[date], list[date]]:
    parsed = validate_input(bundle)
    calendar = list(parsed["calendar"])
    train, pred = schedule(calendar)
    rows = baseline.moneyflow_features_for_calendar(
        calendar=calendar,
        catalog=parsed["catalog_codes"],
        names=parsed["sector_names"],
        daily_rows=bundle["source"]["daily_aggregates"],
        decision_days=train + pred,
    )
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_day[row["trade_date"]].append(row)
    for daily in by_day.values():
        available = [row for row in daily if row["availability"] == "available"]
        ranks = [
            baseline._score_and_states({row["sector_code"]: row["moneyflow_values"][name] for row in available})[0]
            if len(available) >= 2
            else {}
            for name in ("level", "delta")
        ]
        for row in daily:
            row["x"] = [rank[row["sector_code"]] for rank in ranks] if row["availability"] == "available" else None
            row.pop("moneyflow_values")
    return rows, train, pred


def training_matrix(bundle: Mapping[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    parsed = validate_input(bundle)
    train, _ = schedule(list(parsed["calendar"]))
    train_rows = [row for row in rows if date.fromisoformat(row["trade_date"]) in set(train)]
    evaluated = baseline.evaluate_predictions_for_calendar(
        calendar=parsed["calendar"],
        sector_returns=bundle["source"]["sector_returns"],
        benchmark_close=bundle["source"]["benchmark_close"],
        predictions=train_rows,
        decision_start=TRAIN_START,
        decision_end=TRAIN_END,
        outcome_end=TRAIN_OUTCOME_END,
        report_blocks=(("train", TRAIN_START, TRAIN_END),),
    )
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evaluated["evaluated_rows"]:
        if row["outcome_status"] == "available":
            groups[row["trade_date"]].append(row)
    groups = {day: daily for day, daily in groups.items() if len(daily) >= 2}
    if not groups:
        raise fail("no legal training dates with at least two outcomes", "training_data_insufficient")
    entries = []
    for day, daily in sorted(groups.items()):
        ranks = baseline._score_and_states({row["sector_code"]: row["relative_return_10d"] for row in daily})[0]
        for row in sorted(daily, key=lambda row: row["sector_code"]):
            entries.append(
                {
                    "trade_date": day,
                    "sector_code": row["sector_code"],
                    "x": row["x"],
                    "y": ranks[row["sector_code"]],
                    "weight": 1.0 / (len(groups) * len(daily)),
                }
            )
    return {
        "entries": entries,
        "usable_dates": len(groups),
        "planned_dates": 126,
        "excluded_dates": [day.isoformat() for day in train if day.isoformat() not in groups],
        "training_sha256": canonical_sha256(entries),
    }


def _arrays(training: Mapping[str, Any]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    entries = training["entries"]
    x = np.asarray([row["x"] for row in entries], dtype=np.float64)
    y = np.asarray([row["y"] for row in entries], dtype=np.float64)
    w = np.asarray([row["weight"] for row in entries], dtype=np.float64)
    if x.shape != (len(entries), 2) or not all(np.isfinite(a).all() for a in (x, y, w)):
        raise fail("training matrix is non-finite or has an invalid shape", "fit_failed")
    if (w <= 0).any() or not math.isclose(math.fsum(w), 1.0, rel_tol=1e-12, abs_tol=1e-12):
        raise fail("date-equal training weights do not sum to one", "fit_failed")
    return x, y, w


def _validate_parameter_identity(parameters: Mapping[str, Any]) -> None:
    verify(parameters, "parameter_sha256")
    coefficients, intercept = parameters.get("coefficients"), parameters.get("intercept")
    if (
        parameters.get("contract_hash") != MODEL_CONTRACT_HASH
        or parameters.get("feature_names") != list(FEATURE_NAMES)
        or not isinstance(coefficients, list)
        or len(coefficients) != 2
        or any(type(v) not in (int, float) or not math.isfinite(v) for v in coefficients)
        or type(intercept) not in (int, float)
        or not math.isfinite(intercept)
    ):
        raise fail("model parameter identity/shape differs", "fit_failed")


def _validate_parameters(parameters: Mapping[str, Any], training: Mapping[str, Any]) -> None:
    _validate_parameter_identity(parameters)
    if (
        parameters.get("training_sha256") != training["training_sha256"]
        or parameters.get("train_rows") != len(training["entries"])
        or parameters.get("usable_train_dates") != training["usable_dates"]
    ):
        raise fail("model parameter training authority differs")
    beta = np.asarray(parameters.get("coefficients"), dtype=np.float64)
    intercept = parameters.get("intercept")
    x, y, w = _arrays(training)
    residual = x @ beta + intercept - y
    gradient = x.T @ (w * residual) + CONTRACT["alpha"] * beta
    if not np.allclose(gradient, 0, rtol=0, atol=1e-10) or abs(float(w @ residual)) > 1e-10:
        raise fail("parameters do not satisfy the frozen weighted Ridge objective")


def predictions_from_parameters(rows: list[dict[str, Any]], parameters: Mapping[str, Any]) -> list[dict[str, Any]]:
    beta, b = parameters["coefficients"], parameters["intercept"]
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if PREDICTION_START <= date.fromisoformat(row["trade_date"]) <= PREDICTION_END:
            groups[row["trade_date"]].append(row)
    output = []
    for _, daily in sorted(groups.items()):
        raw: dict[str, float] = {}
        terms: dict[str, tuple[float, float]] = {}
        for row in daily:
            if row["availability"] == "available":
                terms[row["sector_code"]] = (beta[0] * row["x"][0], beta[1] * row["x"][1])
                raw[row["sector_code"]] = math.fsum((b, *terms[row["sector_code"]]))
        if not all(math.isfinite(value) for value in raw.values()):
            raise fail("raw prediction is non-finite", "score_failed")
        scores, states = baseline._score_and_states(raw) if len(raw) >= 2 else ({}, {})
        for row in daily:
            result = {key: value for key, value in row.items() if key != "x"}
            code = row["sector_code"]
            if code in scores:
                result.update(
                    rotation_score=scores[code],
                    forecast_state=states[code],
                    feature_contributions={
                        "raw_prediction": raw[code],
                        "intercept": b,
                        "moneyflow_level_linear_term": terms[code][0],
                        "moneyflow_delta_linear_term": terms[code][1],
                        "average_rank_score": scores[code],
                        "daily_rank_group": states[code],
                        "model_parameter_sha256": parameters["parameter_sha256"],
                    },
                )
            output.append(result)
    return output


def numeric_environment() -> dict[str, Any]:
    import os
    from importlib.metadata import version
    from threadpoolctl import threadpool_info

    return {
        "python": sys.version.split()[0],
        "versions": {name: version(name) for name in ("numpy", "scipy", "scikit-learn", "threadpoolctl")},
        "thread_variables": {
            k: os.environ.get(k)
            for k in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS")
        },
        "thread_pools": [{k: v for k, v in item.items() if k != "filepath"} for item in threadpool_info()],
    }


def run_process(bundle: Mapping[str, Any], *, process_index: int) -> dict[str, Any]:
    from sklearn.linear_model import Ridge
    from threadpoolctl import threadpool_limits

    if process_index not in (1, 2):
        raise fail("process index differs")
    rows, _, _ = feature_rows(bundle)
    training = training_matrix(bundle, rows)
    x, y, w = _arrays(training)
    with threadpool_limits(limits=1):
        model = Ridge(alpha=0.01, fit_intercept=True, solver="svd", positive=False)
        model.fit(x, y, sample_weight=w)
        parameters = seal(
            {
                "contract_hash": MODEL_CONTRACT_HASH,
                "training_sha256": training["training_sha256"],
                "feature_names": list(FEATURE_NAMES),
                "coefficients": [float(v) for v in model.coef_],
                "intercept": float(model.intercept_),
                "train_rows": len(training["entries"]),
                "usable_train_dates": training["usable_dates"],
            },
            "parameter_sha256",
        )
        _validate_parameters(parameters, training)
        predictions = predictions_from_parameters(rows, parameters)
        environment = numeric_environment()
    return seal(
        {
            "schema_version": PROCESS_SCHEMA,
            "contract": CONTRACT,
            "input_hash": bundle["input_hash"],
            "source_commit": bundle["source"]["identity"]["source_git_commit"],
            "process_index": process_index,
            "numeric_environment": environment,
            "parameters": parameters,
            "predictions": predictions,
            "prediction_sha256": canonical_sha256(predictions),
            "training_summary": {k: v for k, v in training.items() if k != "entries"},
            "planned_fits": 1,
            "started_fits": 1,
            "completed_fits": 1,
            "failed_fits": 0,
            "tail_accessed": False,
            "database_write": False,
            "runtime_action": False,
        },
        "report_sha256",
    )


def read_evaluation_facts(bundle: Mapping[str, Any]) -> dict[str, Any]:
    from backend.services.hmm_risk.rotation_l2_input import _require_file, _sector_returns, bounded_benchmark_close

    parsed = validate_input(bundle)
    binding = bundle["source"]["evaluation_source_binding"]
    root = Path(binding["root"])
    if not root.is_absolute():
        raise fail("evaluation root is not absolute")
    files = {name: _require_file(root, binding[name]["path"], binding[name]["sha256"]) for name in ("sector", "index")}
    facts = {
        "schema_version": VERSION + "_outcomes",
        "input_hash": bundle["input_hash"],
        "sector_returns": _sector_returns(
            files["sector"],
            id_to_code={int(k): v for k, v in binding["id_to_code"].items()},
            quote_entries={
                code: tuple((date.fromisoformat(a), date.fromisoformat(b)) for a, b in spans)
                for code, spans in binding["quote_entries"].items()
            },
            catalog=list(parsed["catalog_codes"]),
            calendar=list(parsed["calendar"]),
            start=PREDICTION_START,
            end=PREDICTION_END,
        ),
        "benchmark_close": bounded_benchmark_close(files["index"], start=PREDICTION_START, end=PREDICTION_END),
        "tail_accessed": False,
    }
    for name in files:
        _require_file(root, binding[name]["path"], binding[name]["sha256"])
    return seal(facts, "outcome_sha256")


def verify_processes(
    first: Mapping[str, Any], second: Mapping[str, Any], *, input_bundle: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    parsed = validate_input(input_bundle)
    rows, _, _ = feature_rows(input_bundle)
    training = training_matrix(input_bundle, rows)
    for index, child in enumerate((first, second), 1):
        verify(child, "report_sha256")
        if (
            child.get("schema_version") != PROCESS_SCHEMA
            or child.get("contract") != CONTRACT
            or child.get("input_hash") != input_bundle["input_hash"]
            or child.get("process_index") != index
            or child.get("source_commit") != parsed["identity"]["source_git_commit"]
            or any(
                type(child.get(k)) is not int for k in ("planned_fits", "started_fits", "completed_fits", "failed_fits")
            )
            or any(
                child.get(k) != v
                for k, v in {
                    "planned_fits": 1,
                    "started_fits": 1,
                    "completed_fits": 1,
                    "failed_fits": 0,
                    "tail_accessed": False,
                    "database_write": False,
                    "runtime_action": False,
                }.items()
            )
        ):
            raise fail("child differs from parent authority or fit budget")
        _validate_parameters(child["parameters"], training)
        if child.get("training_summary") != {k: v for k, v in training.items() if k != "entries"}:
            raise fail("child training summary differs from parent")
        environment = child.get("numeric_environment", {})
        if (
            environment.get("versions") != numeric_environment()["versions"]
            or environment.get("python") != sys.version.split()[0]
            or not environment.get("thread_pools")
            or any(pool.get("num_threads") != 1 for pool in environment["thread_pools"])
            or set(environment.get("thread_variables", {}))
            != {"OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"}
            or any(value != "1" for value in environment.get("thread_variables", {}).values())
        ):
            raise fail("child numeric environment differs from the fixed single-thread contract")
        expected = predictions_from_parameters(rows, child["parameters"])
        if child.get("predictions") != expected or child.get("prediction_sha256") != canonical_sha256(expected):
            raise fail("child predictions differ from parent zero-fit readback")
    comparable = {k: v for k, v in first.items() if k not in {"process_index", "report_sha256"}}
    if canonical_sha256(comparable) != canonical_sha256(
        {k: v for k, v in second.items() if k not in {"process_index", "report_sha256"}}
    ):
        raise fail("two fresh processes are not bitwise equal", "reproducibility_failed")
    return parsed, rows


def close_processes(
    first: Mapping[str, Any], second: Mapping[str, Any], *, input_bundle: Mapping[str, Any], facts: Mapping[str, Any]
) -> dict[str, Any]:
    parsed, rows = verify_processes(first, second, input_bundle=input_bundle)
    verify(facts, "outcome_sha256")
    if (
        facts.get("schema_version") != VERSION + "_outcomes"
        or facts.get("input_hash") != input_bundle["input_hash"]
        or facts.get("tail_accessed") is not False
    ):
        raise fail("evaluation facts identity differs")
    for section in (facts["sector_returns"], facts["benchmark_close"]):
        if any(not PREDICTION_START <= date.fromisoformat(r["trade_date"]) <= PREDICTION_END for r in section):
            raise fail("evaluation facts exceed the approved range", "causal_boundary_invalid")
    baseline_rows = [
        {k: v for k, v in row.items() if k != "x"}
        for row in rows
        if PREDICTION_START <= date.fromisoformat(row["trade_date"]) <= PREDICTION_END
    ]
    evaluations = [
        baseline.evaluate_predictions_for_calendar(
            calendar=parsed["calendar"],
            sector_returns=facts["sector_returns"],
            benchmark_close=facts["benchmark_close"],
            predictions=p,
            decision_start=PREDICTION_START,
            decision_end=PREDICTION_END,
            outcome_end=PREDICTION_END,
            report_blocks=BLOCKS,
        )
        for p in (first["predictions"], baseline_rows)
    ]
    candidate, reference = evaluations
    candidate_ic, reference_ic = [evaluation["metrics"]["daily_rank_ic"] for evaluation in evaluations]
    paired = {
        date.fromisoformat(day): candidate_ic[day] - reference_ic[day]
        for day in sorted(set(candidate_ic) & set(reference_ic))
    }
    parameters = first["parameters"]
    model_hash = canonical_sha256(
        {"contract_hash": MODEL_CONTRACT_HASH, "parameter_sha256": parameters["parameter_sha256"]}
    )
    identity = parsed["identity"]
    run_id = canonical_sha256(
        {
            "model_hash": model_hash,
            "evaluation_hash": EVALUATION_HASH,
            "input_hash": input_bundle["input_hash"],
            "outcome_hash": facts["outcome_sha256"],
        }
    )
    qualified = candidate["effect_status"] == "DEVELOPMENT_EFFECT_QUALIFIED"
    return seal(
        {
            "schema_version": ACCEPTANCE_SCHEMA,
            "contract_version": VERSION,
            "contract": CONTRACT,
            "model_contract_hash": MODEL_CONTRACT_HASH,
            "model_hash": model_hash,
            "parameters": parameters,
            "model_parameter_sha256": parameters["parameter_sha256"],
            "evaluation_contract_hash": EVALUATION_HASH,
            "evaluation_contract": EVALUATION_CONTRACT,
            "run_id": run_id,
            "input_hash": input_bundle["input_hash"],
            "input_identity": identity,
            "mapping_hash": identity["mapping_hash"],
            "quote_authority_hash": identity["quote_authority_hash"],
            "outcome_sha256": facts["outcome_sha256"],
            "training_summary": first["training_summary"],
            "numeric_environment": first["numeric_environment"],
            "execution_status": "COMPLETED",
            "effect_status": candidate["effect_status"],
            "metrics": candidate["metrics"],
            "baseline_metrics": reference["metrics"],
            "paired_increment": {
                "valid_date_count": len(paired),
                "hac": baseline._newey_west(parsed["calendar"], paired),
                "daily_ic_difference": {day.isoformat(): value for day, value in paired.items()},
            },
            "predictions": candidate["evaluated_rows"],
            "planned_fits": 2,
            "started_fits": 2,
            "completed_fits": 2,
            "failed_fits": 0,
            "tail_accessed": False,
            "research_surface_status": "NOT_AVAILABLE",
            "rotation_l2_capability_status": "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
            if qualified
            else "NOT_AVAILABLE",
            "forward_power_status": "UNAVAILABLE",
            "forward_confirmation": "NOT_STARTED",
            "advisory_status": "NOT_AVAILABLE",
            "validation_basis": BASIS,
            "selection_basis": "RETROSPECTIVE_DEVELOPMENT_SELECTED",
            "database_write": False,
            "runtime_action": False,
        },
        "acceptance_sha256",
    )


def validate_acceptance(value: Mapping[str, Any]) -> None:
    verify(value, "acceptance_sha256")
    _validate_parameter_identity(value["parameters"])
    if (
        value.get("schema_version") != ACCEPTANCE_SCHEMA
        or value.get("contract_version") != VERSION
        or value.get("contract") != CONTRACT
        or value.get("evaluation_contract") != EVALUATION_CONTRACT
        or value.get("evaluation_contract_hash") != EVALUATION_HASH
        or value.get("model_contract_hash") != MODEL_CONTRACT_HASH
        or value.get("model_parameter_sha256") != value["parameters"]["parameter_sha256"]
        or value.get("model_hash")
        != canonical_sha256({"contract_hash": MODEL_CONTRACT_HASH, "parameter_sha256": value["model_parameter_sha256"]})
        or any(type(value.get(k)) is not int for k in ("planned_fits", "started_fits", "completed_fits", "failed_fits"))
        or value.get("planned_fits") != 2
        or value.get("started_fits") != 2
        or value.get("completed_fits") != 2
        or value.get("failed_fits") != 0
        or value.get("tail_accessed") is not False
        or value.get("validation_basis") != BASIS
        or value.get("selection_basis") != "RETROSPECTIVE_DEVELOPMENT_SELECTED"
        or any(value.get(k) is not False for k in ("database_write", "runtime_action"))
    ):
        raise fail("acceptance model/budget contract differs")
    if len(value.get("predictions", [])) != 232 * 131:
        raise fail("acceptance does not contain the full 232 by 131 prediction grid")
    expected_run = canonical_sha256(
        {
            "model_hash": value["model_hash"],
            "evaluation_hash": EVALUATION_HASH,
            "input_hash": value["input_hash"],
            "outcome_hash": value["outcome_sha256"],
        }
    )
    if value.get("run_id") != expected_run:
        raise fail("acceptance run identity differs")
    states = {
        "execution_status": "COMPLETED",
        "research_surface_status": "NOT_AVAILABLE",
        "forward_power_status": "UNAVAILABLE",
        "forward_confirmation": "NOT_STARTED",
        "advisory_status": "NOT_AVAILABLE",
    }
    if any(value.get(k) != v for k, v in states.items()):
        raise fail("acceptance capability states differ")
    metrics = value["metrics"]
    mean = metrics["overall"]["mean_daily_rank_ic"]
    expected_effect = (
        "NO_USABLE_PREDICTIONS"
        if not any(r["availability"] == "available" for r in value["predictions"])
        else "EVIDENCE_INSUFFICIENT"
        if metrics["evidence_sufficient"] is not True or mean is None
        else "BELOW_BINDING_MBE"
        if mean < baseline.BINDING_MBE_RANK_IC
        else "DEVELOPMENT_EFFECT_QUALIFIED"
    )
    expected_capability = (
        "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
        if expected_effect == "DEVELOPMENT_EFFECT_QUALIFIED"
        else "NOT_AVAILABLE"
    )
    if (
        value.get("effect_status") != expected_effect
        or value.get("rotation_l2_capability_status") != expected_capability
    ):
        raise fail("acceptance effect does not match the frozen metric/state policy")
    grid: dict[str, set[str]] = defaultdict(set)
    for row in value["predictions"]:
        day, code = row["trade_date"], row["sector_code"]
        if not PREDICTION_START <= date.fromisoformat(day) <= PREDICTION_END or code in grid[day]:
            raise fail("acceptance prediction identity is duplicated or outside its window")
        grid[day].add(code)
    if len(grid) != 232 or any(len(codes) != 131 or codes != next(iter(grid.values())) for codes in grid.values()):
        raise fail("acceptance daily L2 directory is not complete")


def validate_product_explanation(row: Mapping[str, Any]) -> None:
    """Exact new version branch; never impersonate the zero-fit baseline."""
    summary = row["run_summary"]
    parameters = summary.get("parameters")
    if not isinstance(parameters, Mapping):
        raise fail("product parameter identity is absent")
    _validate_parameter_identity(parameters)
    if (
        parameters.get("contract_hash") != MODEL_CONTRACT_HASH
        or summary.get("model_contract_hash") != MODEL_CONTRACT_HASH
        or row["model_hash"]
        != canonical_sha256({"contract_hash": MODEL_CONTRACT_HASH, "parameter_sha256": parameters["parameter_sha256"]})
        or row["evaluation_contract_hash"] != EVALUATION_HASH
    ):
        raise fail("product model/evaluation contract differs")
    if (row["availability"] == "available" and (not row["structural_eligible"] or not row["feature_eligible"])) or (
        row["availability"] == "unavailable" and row["feature_eligible"]
    ):
        raise fail("product availability/eligibility coupling differs")
    if row["availability"] == "available":
        contribution = row["feature_contributions"]
        fields = {
            "raw_prediction",
            "intercept",
            "moneyflow_level_linear_term",
            "moneyflow_delta_linear_term",
            "average_rank_score",
            "daily_rank_group",
            "model_parameter_sha256",
        }
        if not isinstance(contribution, Mapping) or set(contribution) != fields:
            raise fail("product linear/rank explanation fields differ")
        for key in fields - {"daily_rank_group", "model_parameter_sha256"}:
            if (
                not isinstance(contribution[key], (int, float))
                or isinstance(contribution[key], bool)
                or not math.isfinite(contribution[key])
            ):
                raise fail("product explanation has an invalid number")
        if (
            contribution["model_parameter_sha256"] != parameters["parameter_sha256"]
            or contribution["intercept"] != parameters["intercept"]
            or contribution["daily_rank_group"] != row["forecast_state"]
            or contribution["average_rank_score"] != row["rotation_score"]
            or not math.isclose(
                contribution["raw_prediction"],
                math.fsum(
                    contribution[k] for k in ("intercept", "moneyflow_level_linear_term", "moneyflow_delta_linear_term")
                ),
                rel_tol=1e-10,
                abs_tol=1e-12,
            )
        ):
            raise fail("product raw prediction/parameter/rank identity differs")
