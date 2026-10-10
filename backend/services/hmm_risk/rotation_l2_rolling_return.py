"""One pre-registered L2 rolling-return candidate; file-only, never a product default.

Contract: hmm_evolution_phase2_l2_risk_value_and_rotation_next_detailed_design_20261010,
D1-D6. Old fixed-train contracts and their hashes are not reinterpreted here.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import math
from pathlib import Path
import sys
from typing import Any, Mapping

from backend.services.hmm_risk import rotation_l2 as baseline
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as price
from backend.services.hmm_risk import rotation_l2_moneyflow_price_return_supervised as fixed
from backend.services.hmm_risk.contracts import canonical_sha256

VERSION = "hmm_risk_rotation_l2_rolling_return_v1"
START, END = date(2026, 4, 1), date(2026, 8, 31)
FEATURE_NAMES = price.FEATURE_NAMES
THREADS = price.THREADS
MODEL_CONTRACT = {
    "version": VERSION,
    "features": list(FEATURE_NAMES),
    "target": "official_l2_relative_10d_raw_return",
    "alpha": 0.01,
    "fit_intercept": True,
    "solver": "svd",
    "positive": False,
    "seed": "not_applicable",
    "train_sessions": 126,
    "horizon": 10,
    "update": "first_open_session_each_calendar_month",
    "prediction_window": [START.isoformat(), END.isoformat()],
    "prediction_sessions": 104,
    "mature_sessions": 94,
    "fits_per_process": 5,
    "processes": 2,
    "planned_fits": 10,
    "weights": "1/(usable_dates*date_samples)",
    "binding_mean_rank_ic": 0.02,
    "coverage": 0.90,
    "qualification_blocks": "overall_only_months_diagnostic",
    "delta_native_population": "E0_before_price_filter",
    "selection_basis": "RETROSPECTIVE_DEVELOPMENT_SELECTED",
    "valuation_basis": "GROSS_SYNTHETIC_L2_REFERENCE",
    "costs_bps": [0, 5, 10, 20],
    "forward_confirmed": False,
    "net_value_status": "UNASSESSED",
}
CONTRACT = MODEL_CONTRACT
MODEL_CONTRACT_HASH = canonical_sha256(CONTRACT)
SOURCE_BINDING_SHA = "52eca131ef3a678fb2ab68682801dbb01be454166b7804f20a91dceee3ef69b3"
SOURCE_HISTORY_REQUEST_SHA = "aba569372de679261a1c6ffff6de024954d4c7cb7959c7e6b646cbf81a8aec23"
FIXED_ACCEPTANCE_SHA = "767f81ea422fcb8da5021d2f45d12060498804fd2f6579c282295b4a7fdef86c"
FIXED_MODEL_SHA = "858f41a9d2e4e22e8c88268602cd6c01cfb995ef46e47aa3c26d4176446a02b8"
FIXED_PARAMETER_SHA = "c2952173c9415f0cb177bb22a4fc1a256dd13fc9f4c75aef8c205d565611d7ab"
SYNTHETIC_PINS = {
    "acceptance": "b0271ff977d5eee671c5c87ea02fabd8fac48e87c0d6bb313e508ef8955635bf",
    "features": "1d182582ffad2144545e1f8b3cca8074d278f7ceb3fc4d32e43a4cb15aef60c7",
    "outcomes": "d64b4913d4233ebe57390c0b94d15f599d900423d05fbbcbd3424f27fa5c52d7",
}
ARMS = ("monthly_return", "frozen_return", "delta", "no_order")


def fail(message: str, suffix: str = "identity_invalid") -> baseline.RotationL2Error:
    return baseline.RotationL2Error(f"hmm_risk_rotation_l2_rolling_{suffix}", message)


def require(condition: bool, message: str, suffix: str = "identity_invalid") -> None:
    if not condition:
        raise fail(message, suffix)


def monthly_schedule(calendar: list[date]) -> list[dict[str, Any]]:
    require(calendar == sorted(set(calendar)) and START in calendar and END in calendar, "calendar boundary differs")
    days = [d for d in calendar if START <= d <= END]
    origins = [d for i, d in enumerate(days) if i == 0 or days[i - 1].month != d.month]
    require(len(origins) == 5, "monthly origin count differs")
    output = []
    for origin in origins:
        i = calendar.index(origin)
        require(i >= 162, "rolling feature/label prehistory is insufficient", "source_insufficient")
        train = calendar[i - 136 : i - 10]
        pred = [d for d in days if d.month == origin.month]
        output.append(
            {
                "origin": origin.isoformat(),
                "as_of": calendar[i - 1].isoformat(),
                "train_days": [d.isoformat() for d in train],
                "prediction_days": [d.isoformat() for d in pred],
            }
        )
    return output


def _rows(rows, *, days, calendar, catalog, feature_count=4):
    keys = {(d, c) for d in days for c in catalog}
    require(len(rows) == len(keys), "feature population grid count differs")
    seen = set()
    positions = {d.isoformat(): i for i, d in enumerate(calendar)}
    for row in rows:
        key = row.get("trade_date"), row.get("sector_code")
        require(key in keys and key not in seen, "feature grid has unknown/duplicate identity")
        seen.add(key)
        i = positions[key[0]]
        require(row.get("as_of_date") == calendar[i - 1].isoformat(), "feature is not strictly lagged")
        require(row.get("availability") in ("available", "unavailable"), "unknown availability")
        if row["availability"] == "available":
            x = row.get("x")
            require(
                isinstance(x, list)
                and len(x) == feature_count
                and all(type(v) in (int, float) and math.isfinite(v) for v in x),
                "available four-feature vector is invalid",
                "score_failed",
            )
        else:
            require(row.get("x") is None and row.get("reason_code"), "unavailable feature has no typed reason")


def month_training(month, calendar, catalog):
    plan = month["schedule"]
    _rows(month["training_rows"], days=plan["train_days"], calendar=calendar, catalog=catalog)
    train = [date.fromisoformat(d) for d in plan["train_days"]]
    facts = month["training_facts"]
    as_of = plan["as_of"]
    require(set(facts) == {"sector_returns", "benchmark_close"}, "unexpected training/evaluation view")
    for values in facts.values():
        require(
            all(train[0].isoformat() <= r["trade_date"] <= as_of for r in values),
            "training view includes future labels",
        )
    fact_days = [d.isoformat() for d in calendar if train[0] <= d <= date.fromisoformat(as_of)]
    keys = [(r.get("trade_date"), r.get("sector_code")) for r in facts["sector_returns"]]
    require(
        len(keys) == len(set(keys)) and set(keys) == {(d, c) for d in fact_days for c in catalog},
        "training quote grid is missing, duplicated or unknown",
    )
    require([r.get("trade_date") for r in facts["benchmark_close"]] == fact_days, "training benchmark grid differs")
    for row in facts["sector_returns"]:
        available, value = row.get("quote_available"), row.get("pct_change")
        require(type(available) is bool, "quote availability must be explicit boolean")
        require(
            (type(value) in (int, float) and math.isfinite(value) and value > -100) if available else value is None,
            "training quote value contradicts its availability authority",
        )
    return ridge.training_for_calendar(
        calendar=calendar,
        train=train,
        rows=month["training_rows"],
        sector_returns=facts["sector_returns"],
        benchmark_close=facts["benchmark_close"],
        outcome_end=date.fromisoformat(as_of),
        raw_return_target=True,
    )


def validate_input(bundle: Mapping[str, Any]):
    ridge.verify(bundle, "input_hash")
    require(
        bundle.get("schema_version") == VERSION + "_input" and bundle.get("contract") == CONTRACT,
        "rolling input contract differs",
    )
    require(
        set(bundle)
        == {
            "schema_version",
            "contract",
            "calendar",
            "catalog",
            "months",
            "delta_rows",
            "source_identity",
            "fixed_parameters",
            "numeric_environment",
            "request_sha256",
            "source_commit",
            "input_hash",
        },
        "unknown input/evaluation fields",
    )
    calendar = [date.fromisoformat(d) for d in bundle["calendar"]]
    catalog = bundle["catalog"]
    require(len(catalog) == 131 and catalog == sorted(set(catalog)), "131 official text-code catalog differs")
    plans = monthly_schedule(calendar)
    require(len(bundle["months"]) == 5, "month population differs")
    require(bundle["source_identity"]["frozen_binding_sha256"] == SOURCE_BINDING_SHA, "release binding differs")
    ridge._validate_parameter_identity(bundle["fixed_parameters"], variant=fixed)
    require(bundle["fixed_parameters"]["parameter_sha256"] == FIXED_PARAMETER_SHA, "fixed control parameter differs")
    _rows(
        bundle["delta_rows"],
        days=[d.isoformat() for d in calendar if START <= d <= END],
        calendar=calendar,
        catalog=catalog,
        feature_count=2,
    )
    for month, plan in zip(bundle["months"], plans, strict=True):
        require(
            set(month) == {"schedule", "training_rows", "training_facts", "prediction_rows"}
            and month["schedule"] == plan,
            "monthly authority differs",
        )
        _rows(month["prediction_rows"], days=plan["prediction_days"], calendar=calendar, catalog=catalog)
        month_training(month, calendar, catalog)
    return calendar, catalog


def numeric_environment():
    # Load the same numerical pool before prepare and child comparison, without fit.
    from sklearn.linear_model import Ridge  # noqa: F401
    from threadpoolctl import threadpool_limits

    with threadpool_limits(limits=1):
        result = price.numeric_environment()
    require(
        {k: result[k] for k in ("python", "versions")} == price.APPROVED_NUMERIC,
        "fixed numerical versions differ",
        "numeric_environment_mismatch",
    )
    require(
        all(v == "1" for v in result["thread_variables"].values())
        and result["thread_pools"]
        and all(p["num_threads"] == 1 for p in result["thread_pools"]),
        "single-thread numerical environment differs",
        "numeric_environment_mismatch",
    )
    return result


def run_process(bundle: Mapping[str, Any], *, process_index: int, progress=None):
    require(type(process_index) is int and process_index in (1, 2), "child index differs")
    calendar, catalog = validate_input(bundle)
    environment = numeric_environment()
    require(environment == bundle["numeric_environment"], "prepare/child numeric identity differs")
    parameters, predictions = [], []
    for number, month in enumerate(bundle["months"], 1):
        training = month_training(month, calendar, catalog)
        if progress:
            progress("started", number, month["schedule"]["origin"])
        parameter = ridge.fit_parameters(training, variant=sys.modules[__name__])
        if progress:
            progress("completed", number, month["schedule"]["origin"])
        parameters.append(
            {
                "schedule": month["schedule"],
                "parameters": parameter,
                "training_summary": {k: v for k, v in training.items() if k != "entries"},
            }
        )
        output = ridge.linear_predictions_for_rows(month["prediction_rows"], parameter, term_names=price.LINEAR_TERMS)
        for row in output:
            row["fit_origin"] = month["schedule"]["origin"]
            row["model_as_of"] = month["schedule"]["as_of"]
        predictions.extend(output)
    return ridge.seal(
        {
            "schema_version": VERSION + "_process",
            "contract": CONTRACT,
            "input_hash": bundle["input_hash"],
            "request_sha256": bundle["request_sha256"],
            "source_commit": bundle["source_commit"],
            "numeric_environment": environment,
            "process_index": process_index,
            "models": parameters,
            "predictions": predictions,
            "prediction_sha256": canonical_sha256(predictions),
            "started_fits": 5,
            "completed_fits": 5,
            "failed_fits": 0,
            "database_write": False,
            "dataset_write": False,
            "runtime_action": False,
        },
        "report_sha256",
    )


def verify_processes(first, second, *, input_bundle):
    calendar, catalog = validate_input(input_bundle)
    payloads = []
    for index, child in enumerate((first, second), 1):
        ridge.verify(child, "report_sha256")
        expected = {
            "schema_version": VERSION + "_process",
            "contract": CONTRACT,
            "input_hash": input_bundle["input_hash"],
            "request_sha256": input_bundle["request_sha256"],
            "source_commit": input_bundle["source_commit"],
            "numeric_environment": input_bundle["numeric_environment"],
            "process_index": index,
            "started_fits": 5,
            "completed_fits": 5,
            "failed_fits": 0,
            "database_write": False,
            "dataset_write": False,
            "runtime_action": False,
        }
        require(
            all(type(child.get(k)) is type(v) and child[k] == v for k, v in expected.items()),
            "child metadata/budget does not close against authority",
        )
        require(len(child["models"]) == 5, "child month/model count differs")
        rebuilt = []
        for month, model in zip(input_bundle["months"], child["models"], strict=True):
            training = month_training(month, calendar, catalog)
            require(
                model["schedule"] == month["schedule"]
                and model["training_summary"] == {k: v for k, v in training.items() if k != "entries"},
                "child training date/summary authority differs",
            )
            ridge._validate_parameters(model["parameters"], training, variant=sys.modules[__name__])
            output = ridge.linear_predictions_for_rows(
                month["prediction_rows"], model["parameters"], term_names=price.LINEAR_TERMS
            )
            for row in output:
                row["fit_origin"] = month["schedule"]["origin"]
                row["model_as_of"] = month["schedule"]["as_of"]
            rebuilt.extend(output)
        require(
            child["predictions"] == rebuilt and child["prediction_sha256"] == canonical_sha256(rebuilt),
            "child prediction differs from parent reconstruction",
        )
        payloads.append({k: v for k, v in child.items() if k not in {"process_index", "report_sha256"}})
    require(canonical_sha256(payloads[0]) == canonical_sha256(payloads[1]), "two fresh-process payloads differ")
    return first["predictions"]


def _source(request):
    from backend.services.hmm_risk import frozen_l2_history_input as carrier
    from backend.services.hmm_risk.formal_state_executor import frozen_release_binding
    from backend.services.hmm_risk import rotation_l1_input_bundle as loader
    from backend.services.hmm_risk import rotation_l2_input as reader

    history_request = carrier._asset(request["source_history_request_path"], SOURCE_HISTORY_REQUEST_SHA)
    source, old_features, _, strings = carrier._risk_source(history_request)
    binding = frozen_release_binding()
    require(canonical_sha256(binding) == SOURCE_BINDING_SHA, "formal release contract has drifted")
    assets = loader.load_rotation_l1_direct_v2_source_assets(
        Path(source["source"]["candidate_root"]),
        security_identity_manifest=Path(source["source"]["security_identity_manifest"]),
        provider_absence_manifest=Path(source["source"]["provider_absence_manifest"]),
        data_window_end=END,
        frozen_release_binding=binding,
    )
    require(
        assets["release_identity"] == old_features["input_identity"]["release_identity"],
        "frozen source release differs from original P2 authority",
    )
    root = assets["release_root"]
    manifest = reader._json(reader._require_file(root, "qe_dataset_manifest.json", binding["manifest_file_sha256"]))
    files = dict(assets["files"])
    for name in ("sector_data", "sector_code_map", "sector_quote_availability", "sector_membership_spans"):
        pin = manifest["components"][name]
        files[name] = reader._require_file(root, pin["path"], pin["sha256"])
    mapping = reader.load_release_sw_l2_code_map(files["sector_code_map"])
    quote = reader.load_sector_quote_availability(
        files["sector_quote_availability"], code_map=mapping, required_end=END
    )
    require(list(mapping.member_backed_codes) == source["frozen"]["catalog"], "official text-code projection differs")
    return source, assets, manifest, files, mapping, quote, [date.fromisoformat(d) for d in strings]


def _fixed_control(path):
    from scripts.hmm_risk.rotation_l2_reference_value import RETURN_PINS

    # fixed.REFERENCE_PINS authenticates its older rank-target control, not
    # the fixed return model itself. Use the established return-model pins.
    controls, _ = price._reference(Path(path), variant=fixed, pins=RETURN_PINS)
    require(
        controls["acceptance_sha256"] == FIXED_ACCEPTANCE_SHA and controls["model_hash"] == FIXED_MODEL_SHA,
        "frozen return control differs",
    )
    return controls["parameters"]


def prepare_inputs(request, *, source_commit):
    import pandas as pd
    from backend.services.hmm_risk import frozen_l2_history_input as carrier
    from backend.services.hmm_risk import rotation_l2_input as reader
    from backend.services.hmm_risk.formal_state_effect import verify_receipt
    from backend.services.hmm_risk.provider_absence import load_provider_absence_manifest
    from backend.services.hmm_risk.security_identity import load_security_source_identity_manifest

    verify_receipt(request)
    require(
        request.get("schema_version") == VERSION + "_request" and request.get("contract") == CONTRACT,
        "explicit rolling request differs",
    )
    source, assets, manifest, files, mapping, quote, calendar = _source(request)
    stamps = {k: carrier._stamp(v) for k, v in files.items()}
    schedule = monthly_schedule(calendar)
    first = date.fromisoformat(schedule[0]["train_days"][0])
    source_days = calendar[calendar.index(first) - 25 : calendar.index(END)]
    catalog = list(mapping.member_backed_codes)
    membership = pd.read_parquet(files["sector_membership_spans"])
    reader.validate_membership_frame(
        membership, id_to_code=mapping.id_to_code, required_start=source_days[0], required_end=END
    )
    membership["instrument"] = membership["instrument"].astype(str).str.strip().str.upper()
    for field in ("start_date", "end_date"):
        membership[field] = pd.to_datetime(membership[field]).dt.date
    daily, amount_hash = reader._daily_aggregates(
        root=assets["release_root"],
        manifest=manifest,
        membership=membership,
        id_to_code=mapping.id_to_code,
        quote_entries=quote.entries,
        catalog=catalog,
        source_days=source_days,
        provider_path=assets["instrument_universe_path"],
        provider_catalog_path=assets["qlib_root"] / "instruments/all.txt",
        suspend_path=files["suspend_data"],
        moneyflow_path=files["moneyflow"],
        qlib_root=assets["qlib_root"],
        calendar=calendar,
        security_identity=load_security_source_identity_manifest(
            files["security_identity"], expected_sha256=source["frozen"]["security_identity_sha256"]
        ),
        provider_absence=load_provider_absence_manifest(
            files["provider_absence"], expected_sha256=source["frozen"]["provider_absence_sha256"]
        ),
        bounded_suspend=True,
    )
    sector_returns = reader._sector_returns(
        files["sector_data"],
        id_to_code=mapping.id_to_code,
        quote_entries=quote.entries,
        catalog=catalog,
        calendar=calendar,
        start=source_days[0],
        end=source_days[-1],
    )
    benchmark = reader.bounded_benchmark_close(
        files["index_context"], start=calendar[calendar.index(source_days[0]) - 1], end=source_days[-1]
    )
    decisions = sorted({date.fromisoformat(d) for m in schedule for d in m["train_days"] + m["prediction_days"]})
    rows = ridge.moneyflow_rank_rows(
        calendar=calendar, catalog=catalog, names=source["frozen"]["names"], daily_rows=daily, decision_days=decisions
    )
    delta_rows = [dict(r) for r in rows if START.isoformat() <= r["trade_date"] <= END.isoformat()]
    price.add_price_features(rows, calendar, {"sector_returns": sector_returns, "benchmark_close": benchmark})
    by_day = defaultdict(list)
    for row in rows:
        by_day[row["trade_date"]].append(row)
    controls = _fixed_control(request["fixed_acceptance_path"])
    months = [
        {
            "schedule": m,
            "training_rows": [r for d in m["train_days"] for r in by_day[d]],
            "prediction_rows": [r for d in m["prediction_days"] for r in by_day[d]],
            "training_facts": {
                "sector_returns": [r for r in sector_returns if m["train_days"][0] <= r["trade_date"] <= m["as_of"]],
                "benchmark_close": [r for r in benchmark if m["train_days"][0] <= r["trade_date"] <= m["as_of"]],
            },
        }
        for m in schedule
    ]
    carrier._stable_files(files, stamps)
    result = ridge.seal(
        {
            "schema_version": VERSION + "_input",
            "contract": CONTRACT,
            "calendar": [d.isoformat() for d in calendar],
            "catalog": catalog,
            "months": months,
            "delta_rows": delta_rows,
            "source_identity": {
                "release": assets["release_identity"],
                "frozen_binding_sha256": SOURCE_BINDING_SHA,
                "mapping_sha256": mapping.member_backed_digest,
                "quote_authority_sha256": quote.quote_availability_digest,
                "amount_window_sha256": amount_hash,
                "component_pins": manifest["components"],
            },
            "fixed_parameters": controls,
            "numeric_environment": numeric_environment(),
            "request_sha256": request["receipt_sha256"],
            "source_commit": source_commit,
        },
        "input_hash",
    )
    validate_input(result)
    return result


def read_evaluation_facts(request, bundle):
    """Parent only, after two model/prediction closures; no fit or source repair."""
    from backend.services.hmm_risk import frozen_l2_history_input as carrier
    from backend.services.hmm_risk import rotation_l2_input as reader

    require(request["receipt_sha256"] == bundle["request_sha256"], "evaluation request linkage differs")
    source, assets, _, files, mapping, quote, calendar = _source(request)
    require(
        assets["release_identity"] == bundle["source_identity"]["release"]
        and list(mapping.member_backed_codes) == bundle["catalog"]
        and [d.isoformat() for d in calendar] == bundle["calendar"],
        "evaluation source identity differs",
    )
    stamps = {k: carrier._stamp(v) for k, v in files.items()}
    facts = {
        "sector_returns": reader._sector_returns(
            files["sector_data"],
            id_to_code=mapping.id_to_code,
            quote_entries=quote.entries,
            catalog=bundle["catalog"],
            calendar=calendar,
            start=START,
            end=END,
        ),
        "benchmark_close": reader.bounded_benchmark_close(files["index_context"], start=START, end=END),
    }
    synthetic = {k: carrier._asset(request["synthetic_" + k + "_path"], pin) for k, pin in SYNTHETIC_PINS.items()}
    features, outcome, accepted = synthetic["features"], synthetic["outcomes"], synthetic["acceptance"]
    require(
        features["catalog"] == bundle["catalog"] and features["calendar"] == bundle["calendar"],
        "synthetic valuation population/calendar differs",
    )
    require(
        features["source_identity"]["release_identity"] == assets["release_identity"]
        and outcome["feature_sha256"] == features["receipt_sha256"]
        and accepted["features_sha256"] == features["receipt_sha256"],
        "synthetic source lineage differs",
    )
    require(
        outcome["sealed_prediction_sha256"] == accepted["result"]["prediction_sha256"]
        and outcome["receipt_sha256"] == accepted["result"]["outcome_sha256"],
        "synthetic sealed-prediction lineage differs",
    )
    days = [d.isoformat() for d in calendar if START <= d <= END]
    require(list(sorted(outcome["event_returns"])) == days, "synthetic event-return calendar differs")
    require(
        all(set(outcome["event_returns"][d]) == set(bundle["catalog"]) for d in days),
        "synthetic return population differs",
    )
    facts["synthetic_returns"] = outcome["event_returns"]
    facts["synthetic_outcome_sha256"] = outcome["receipt_sha256"]
    carrier._stable_files(files, stamps)
    return ridge.seal(
        {"schema_version": VERSION + "_outcomes", "input_hash": bundle["input_hash"], **facts}, "outcome_sha256"
    )


def evaluate(predictions, facts, calendar):
    blocks = tuple(
        (str(m), date(2026, m, 1), max(d for d in calendar if d.year == 2026 and d.month == m)) for m in range(4, 9)
    )
    full = baseline.evaluate_predictions_for_calendar(
        calendar=calendar,
        sector_returns=facts["sector_returns"],
        benchmark_close=facts["benchmark_close"],
        predictions=predictions,
        decision_start=START,
        decision_end=END,
        outcome_end=END,
        report_blocks=blocks,
    )
    # The old evaluator treats requested blocks as coverage AND gates. Here
    # months are descriptive only: preserve the pure statistic, not that gate.
    result = baseline.summarize_evaluated_predictions(
        calendar=calendar,
        evaluated_rows=full["evaluated_rows"],
        decision_start=START,
        decision_end=END,
        outcome_end=END,
        report_blocks=(),
    )
    result["metrics"]["diagnostic_blocks"] = full["metrics"]["blocks"]
    result["metrics"]["block_role"] = "DIAGNOSTIC_ONLY_NOT_QUALIFICATION_AND"
    return result


def close_processes(first, second, *, input_bundle, facts):
    from scripts.hmm_risk import rotation_l2_reference_value as reference

    predictions = verify_processes(first, second, input_bundle=input_bundle)
    ridge.verify(facts, "outcome_sha256")
    require(facts["input_hash"] == input_bundle["input_hash"], "outcome/input identity differs")
    calendar = [date.fromisoformat(d) for d in input_bundle["calendar"]]
    rows = [r for m in input_bundle["months"] for r in m["prediction_rows"]]
    arms = {
        "monthly_return": predictions,
        "frozen_return": ridge.linear_predictions_for_rows(
            rows, input_bundle["fixed_parameters"], term_names=price.LINEAR_TERMS
        ),
    }
    daily = defaultdict(list)
    for row in input_bundle["delta_rows"]:
        daily[row["trade_date"]].append(row)
    delta = []
    for _, values in sorted(daily.items()):
        available = {r["sector_code"]: r["x"][1] for r in values if r["availability"] == "available"}
        scores, states = baseline._score_and_states(available) if len(available) >= 2 else ({}, {})
        for row in values:
            r = {k: v for k, v in row.items() if k != "x"}
            code = r["sector_code"]
            if code in scores:
                r.update(rotation_score=scores[code], forecast_state=states[code])
            delta.append(r)
    arms["delta"] = delta
    indexes = {arm: {(r["trade_date"], r["sector_code"]): r for r in values} for arm, values in arms.items()}
    days = [d.isoformat() for d in calendar if START <= d <= END]
    require(len(days) == 104, "valuation calendar count differs")
    decisions = days[:-10]
    common = {
        d: sorted(
            c
            for c in input_bundle["catalog"]
            if all(indexes[arm][d, c]["availability"] == "available" for arm in indexes)
        )
        for d in days
    }
    groups = {
        arm: {d: [c for c in common[d] if indexes[arm][d, c]["forecast_state"] == "trending"] for d in decisions}
        for arm in indexes
    }
    groups["no_order"] = {d: common[d] for d in decisions}
    quotes = {(d, c): facts["synthetic_returns"][d][c] for d in days for c in input_bundle["catalog"]}
    paths = {
        arm: {
            str(cost): reference.cohort_reference_path(
                days, decisions, groups[arm], quotes, cost_bps=cost, valuation_basis=CONTRACT["valuation_basis"]
            )
            for cost in CONTRACT["costs_bps"]
        }
        for arm in ARMS
    }
    blocks = tuple(
        (str(m), date(2026, m, 1), max(d for d in calendar if d.year == 2026 and d.month == m)) for m in range(4, 9)
    )
    comparisons = [(a, "no_order") for a in ARMS[:-1]] + [("monthly_return", a) for a in ("frozen_return", "delta")]
    metrics = {arm: evaluate(values, facts, calendar) for arm, values in arms.items()}
    common_metrics = {
        arm: evaluate([r for r in values if r["sector_code"] in common[r["trade_date"]]], facts, calendar)
        for arm, values in arms.items()
    }
    summary = {
        a: {c: reference.summarize_for_blocks(p, blocks=blocks) for c, p in values.items()}
        for a, values in paths.items()
    }
    paired = {
        a + "_minus_" + b: {
            str(c): reference.paired_for_blocks(days, paths[a][str(c)], paths[b][str(c)], blocks=blocks)
            for c in CONTRACT["costs_bps"]
        }
        for a, b in comparisons
    }
    return ridge.seal(
        {
            "schema_version": VERSION + "_acceptance",
            "contract": CONTRACT,
            "input_hash": input_bundle["input_hash"],
            "request_sha256": input_bundle["request_sha256"],
            "executor_commit": input_bundle["source_commit"],
            "outcome_sha256": facts["outcome_sha256"],
            "two_process_business_bitwise_equal": True,
            "planned_fits": 10,
            "started_fits": 10,
            "completed_fits": 10,
            "failed_fits": 0,
            "models": first["models"],
            "prediction_sha256": first["prediction_sha256"],
            "catalog_count": 131,
            "prediction_days": 104,
            "mature_days": 94,
            "source_identity": input_bundle["source_identity"],
            "native_metrics": metrics,
            "common_metrics": common_metrics,
            "reference_summary": summary,
            "paired_reference": paired,
            "common_population_by_date": {d: len(c) for d, c in common.items()},
            "synthetic_outcome_sha256": facts["synthetic_outcome_sha256"],
            "valuation_basis": CONTRACT["valuation_basis"],
            "cost_basis": "ILLUSTRATIVE_BUDGET_COST_NOT_EXECUTION_NET",
            "net_value_status": "UNASSESSED",
            "forward_confirmed": False,
            "selection_basis": CONTRACT["selection_basis"],
            "database_write": False,
            "dataset_write": False,
            "runtime_action": False,
            "QE_action": False,
        },
        "receipt_sha256",
    )
