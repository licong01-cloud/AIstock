"""One paired, retrospective L2 interaction hypothesis; four fits, file-only."""

from __future__ import annotations

from copy import deepcopy
from datetime import date
import hashlib
import math
from typing import Any, Mapping

from backend.services.hmm_risk import rotation_l2 as baseline
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as price
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
from backend.services.hmm_risk.contracts import canonical_sha256

VERSION = "hmm_risk_rotation_l2_interaction_v1"
INPUT_SCHEMA, PROCESS_SCHEMA, ACCEPTANCE_SCHEMA = (VERSION + s for s in ("_input", "_process", "_acceptance"))
ARMS = ("control", "interaction")
FEATURE_NAMES = (*price.FEATURE_NAMES, "moneyflow_momentum_interaction")
LINEAR_TERMS = (*price.LINEAR_TERMS, "moneyflow_momentum_interaction_linear_term")
APPROVED_INPUT = {
    "generation": "20261015-v18-unified-quote1",
    "manifest_sha256": "f82670728cd6cde06bbc834481187794d081070689648c06b88786a7782e435b",
    "profile_sha256": "807ce03afc9838e5d1c6e71e3a46c34b41f4b77032bf42abee0dbb2e30aa0e23",
    "mapping_hash": "ed23788ae6684bdd559d3a0a9088b9b0d9fd7dde37cc8a37932c582b0c632cb1",
    "quote_authority_hash": "796c0036b1b1aea5592eb6f3be48a84c9aab26a3ad1b120c33dc205fb06fff97",
}
FILE_PINS = {
    "moneyflow_h5": "88467d29634f9f1ddbbdaba6a9ee8f759d68b6f109960a77023ab957b6daab60",
    "sector_data_h5": "575ad57d52567857a028ff71809ea776894dce3f00421182ecf9ebdb4e3402f3",
    "security_identity": "21a47581be3aec0f6d7f41de4f863b549aa227f1a8026e4398ce4a5c20095a87",
    "provider_absence": "494a9b3ff725792592cf668505691e57a000ef10f160015dc3c45c28552565ba",
}
CONTRACT = {
    **price.CONTRACT,
    "version": VERSION,
    "model_type": "paired_raw_return_ridge_single_interaction",
    "feature_names": list(FEATURE_NAMES),
    "control_feature_names": list(price.FEATURE_NAMES),
    "interaction": "moneyflow_delta_rank*max(relative_momentum_rank,0)",
    "target": "official_l2_10d_return_minus_csi300_10d_return",
    "arms": list(ARMS),
    "planned_fits": 4,
    "validation_basis": "RETROSPECTIVE_DEVELOPMENT_RESEARCH",
}
EVALUATION_CONTRACT = {
    **ridge.EVALUATION_CONTRACT,
    "version": VERSION,
    "basis": CONTRACT["validation_basis"],
    "comparison": "interaction_control_delta_three_way_common_mature_population",
    "cost_bps_one_way": [0, 5, 10, 20],
    "reference_entry": "decision_close_reference_then_next_10_returns_not_executable_price",
}
seal, verify = ridge.seal, ridge.verify
numeric_environment = price.numeric_environment


def fail(message: str, suffix: str = "identity_invalid") -> baseline.RotationL2Error:
    return baseline.RotationL2Error(f"hmm_risk_rotation_l2_interaction_{suffix}", message)


def validate_input(bundle: Mapping[str, Any]) -> dict[str, Any]:
    verify(bundle, "input_hash")
    if bundle.get("schema_version") != INPUT_SCHEMA or bundle.get("contract") != CONTRACT:
        raise fail("paired input contract differs")
    parsed = ridge.validate_training_source(bundle["source"])
    identity = parsed["identity"]
    mismatches = [k for k, v in APPROVED_INPUT.items() if identity.get(k) != v]
    mismatches += [k for k, v in FILE_PINS.items() if identity["source_file_hashes"].get(k) != v]
    if mismatches:
        raise fail("paired input differs from the approved current release: " + ", ".join(mismatches))
    price.validate_price_view(bundle["source"], bundle["price_features"], parsed)
    price.validate_numeric_environment(bundle.get("numeric_environment", {}))
    return parsed


def prepare_inputs(**source_args: Any) -> dict[str, Any]:
    from sklearn.linear_model import Ridge  # noqa: F401
    from threadpoolctl import threadpool_limits
    from backend.services.hmm_risk.rotation_l2_input import bounded_price_features, build_rotation_l2_input_bundle

    if hashlib.sha256(source_args["active_profile_path"].read_bytes()).hexdigest() != APPROVED_INPUT["profile_sha256"]:
        raise fail("active profile byte identity changed before source preparation")
    source = build_rotation_l2_input_bundle(**source_args, outcome_end=ridge.TRAIN_OUTCOME_END, bounded_benchmark=True)
    with threadpool_limits(limits=1):
        environment = numeric_environment()
    bundle = seal(
        {
            "schema_version": INPUT_SCHEMA,
            "contract": CONTRACT,
            "source": source,
            "price_features": bounded_price_features(source),
            "numeric_environment": environment,
        },
        "input_hash",
    )
    validate_input(bundle)
    return bundle


def append_interaction(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = deepcopy(rows)
    for row in result:
        x = row.get("x")
        if row["availability"] == "available":
            if (
                not isinstance(x, list)
                or len(x) != 4
                or any(type(v) not in (int, float) or not math.isfinite(v) for v in x)
            ):
                raise fail("interaction requires four finite original features")
            x.append(x[1] * max(x[2], 0))
        elif x is not None:
            raise fail("unavailable feature row unexpectedly contains a vector")
    return result


def feature_rows(bundle: Mapping[str, Any], *, interaction: bool = True):
    parsed = validate_input(bundle)
    calendar = list(parsed["calendar"])
    train, pred = ridge.schedule(calendar)
    rows = ridge.moneyflow_rank_rows(
        calendar=calendar,
        catalog=parsed["catalog_codes"],
        names=parsed["sector_names"],
        daily_rows=bundle["source"]["daily_aggregates"],
        decision_days=train + pred,
    )
    price.add_price_features(rows, calendar, bundle["price_features"])
    return (append_interaction(rows) if interaction else rows), train, pred


def training_matrix(bundle: Mapping[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    return ridge.training_matrix_for_source(bundle["source"], validate_input(bundle), rows, raw_return_target=True)


class _Arm:
    """A concrete frozen estimator contract, not an alias for an old candidate."""

    numeric_environment = staticmethod(numeric_environment)
    validate_input = staticmethod(validate_input)
    training_matrix = staticmethod(training_matrix)

    def __init__(self, name: str):
        self.name = name
        self.FEATURE_NAMES = price.FEATURE_NAMES if name == "control" else FEATURE_NAMES
        self.LINEAR_TERMS = price.LINEAR_TERMS if name == "control" else LINEAR_TERMS
        self.CONTRACT = {**CONTRACT, "arm": name, "feature_names": list(self.FEATURE_NAMES), "planned_fits": 2}
        self.MODEL_CONTRACT_HASH = canonical_sha256(self.CONTRACT)
        self.PROCESS_SCHEMA = VERSION + "_" + name + "_process"

    def feature_rows(self, bundle):
        return feature_rows(bundle, interaction=self.name == "interaction")

    def predictions_from_parameters(self, rows, parameters):
        return ridge.predictions_from_parameters(rows, parameters, term_names=self.LINEAR_TERMS)


def run_process(bundle: Mapping[str, Any], *, process_index: int, on_completed=None) -> dict[str, Any]:
    arms = {}
    for name in ARMS:
        arms[name] = ridge.run_process(bundle, process_index=process_index, variant=_Arm(name))
        if on_completed is not None:
            on_completed(len(arms))
    return seal(
        {
            "schema_version": PROCESS_SCHEMA,
            "input_hash": bundle["input_hash"],
            "process_index": process_index,
            "arms": arms,
            "planned_fits": 2,
            "completed_fits": 2,
            "tail_accessed": False,
        },
        "report_sha256",
    )


def verify_processes(first, second, *, input_bundle):
    parsed = validate_input(input_bundle)
    for i, report in enumerate((first, second), 1):
        verify(report, "report_sha256")
        if (
            report.get("schema_version") != PROCESS_SCHEMA
            or report.get("input_hash") != input_bundle["input_hash"]
            or type(report.get("process_index")) is not int
            or report["process_index"] != i
            or type(report.get("planned_fits")) is not int
            or report["planned_fits"] != 2
            or type(report.get("completed_fits")) is not int
            or report["completed_fits"] != 2
            or report.get("tail_accessed") is not False
            or set(report.get("arms", {})) != set(ARMS)
        ):
            raise fail("child authority/arm/fit budget differs")
    for name in ARMS:
        ridge.verify_processes(first["arms"][name], second["arms"][name], input_bundle=input_bundle, variant=_Arm(name))
    summaries = [first["arms"][name]["training_summary"] for name in ARMS]
    if any(summaries[0][k] != summaries[1][k] for k in ("usable_dates", "planned_dates", "excluded_dates")):
        raise fail("matched training dates differ")
    predictions = [first["arms"][name]["predictions"] for name in ARMS]
    if [(r["trade_date"], r["sector_code"], r["availability"], r["reason_code"]) for r in predictions[0]] != [
        (r["trade_date"], r["sector_code"], r["availability"], r["reason_code"]) for r in predictions[1]
    ]:
        raise fail("matched prediction population differs")
    return parsed


def read_evaluation_facts(bundle: Mapping[str, Any]) -> dict[str, Any]:
    return ridge.read_evaluation_for_source(
        bundle["source"], validate_input(bundle), input_hash=bundle["input_hash"], version=VERSION
    )


def validate_evaluation_facts(bundle, facts, parsed):
    verify(facts, "outcome_sha256")
    if (
        facts.get("schema_version") != VERSION + "_outcomes"
        or facts.get("input_hash") != bundle["input_hash"]
        or facts.get("tail_accessed") is not False
    ):
        raise fail("outcome identity differs")
    days = {d.isoformat() for d in ridge.schedule(list(parsed["calendar"]))[1]}
    codes = set(parsed["catalog_codes"])
    spans = bundle["source"]["evaluation_source_binding"]["quote_entries"]
    grid = set()
    for row in facts["sector_returns"]:
        day, code = row["trade_date"], row["sector_code"]
        key = day, code
        if day not in days or code not in codes or key in grid:
            raise fail("outcome directory exceeds the authorized grid", "causal_boundary_invalid")
        available = any(a <= day <= b for a, b in spans[code])
        value = row.get("pct_change")
        if (
            type(row.get("quote_available")) is not bool
            or row["quote_available"] != available
            or (not available and value is not None)
            or (available and (type(value) not in (int, float) or not math.isfinite(value) or 1 + value / 100 <= 0))
        ):
            raise fail("outcome quote/domain differs from frozen availability")
        grid.add(key)
    if grid != {(day, code) for day in days for code in codes}:
        raise fail("outcome grid has missing dates or sectors")
    benchmark = set()
    for row in facts["benchmark_close"]:
        day, close = row["trade_date"], row.get("close")
        if (
            day not in days
            or day in benchmark
            or type(close) not in (int, float)
            or not math.isfinite(close)
            or close <= 0
        ):
            raise fail("outcome benchmark is missing/duplicated/non-finite/out of range")
        benchmark.add(day)
    if benchmark != days:
        raise fail("outcome benchmark coverage differs")


def evidence_status(hac: Mapping[str, Any]) -> str:
    mean = hac.get("mean")
    if mean is None:
        return "EVIDENCE_INSUFFICIENT"
    if mean <= 0:
        return "NOT_SUPPORTED_BY_CURRENT_EVIDENCE"
    lower = hac.get("lower")
    return "RETROSPECTIVE_INCREMENT_OBSERVED" if lower is not None and lower > 0 else "POSITIVE_INCREMENT_UNCONFIRMED"


def _reference_paths(evaluations, facts, calendar):
    from scripts.hmm_risk import rotation_l2_reference_value as reference

    days = [d.isoformat() for d in calendar if ridge.PREDICTION_START <= d <= ridge.PREDICTION_END]
    decisions = days[:-10]
    tables = {
        name: {(r["trade_date"], r["sector_code"]): r for r in e["evaluated_rows"]} for name, e in evaluations.items()
    }
    groups = {name: {} for name in (*evaluations, "no_order")}
    for day in decisions:
        common = set.intersection(
            *(
                {c for (d, c), r in table.items() if d == day and r["availability"] == "available"}
                for table in tables.values()
            )
        )
        groups["no_order"][day] = sorted(common)
        for name, table in tables.items():
            groups[name][day] = sorted(c for c in common if table[day, c]["forecast_state"] == "trending")
    quotes = {
        (r["trade_date"], r["sector_code"]): r["pct_change"] / 100 if r["quote_available"] else None
        for r in facts["sector_returns"]
    }
    result = {}
    for cost in EVALUATION_CONTRACT["cost_bps_one_way"]:
        paths = {
            name: reference.cohort_reference_path(days, decisions, group, quotes, cost_bps=cost)
            for name, group in groups.items()
        }
        result[str(cost)] = {
            "arms": {name: reference.summarize(path) for name, path in paths.items()},
            "interaction_minus_control": reference.paired(days, paths["interaction"], paths["control"]),
        }
    return {
        "contract": reference.CONTRACT,
        "basis": "INDEX_CLOSE_REFERENCE_NOT_ACTUAL_NET_RETURNS",
        "common_population_sha256": canonical_sha256(groups["no_order"]),
        "costs": result,
    }


def close_processes(first, second, *, input_bundle, facts):
    parsed = verify_processes(first, second, input_bundle=input_bundle)
    validate_evaluation_facts(input_bundle, facts, parsed)
    calendar = list(parsed["calendar"])
    _, pred = ridge.schedule(calendar)
    delta = ridge.moneyflow_rank_rows(
        calendar=calendar,
        catalog=parsed["catalog_codes"],
        names=parsed["sector_names"],
        daily_rows=input_bundle["source"]["daily_aggregates"],
        decision_days=pred,
    )
    raw = {name: first["arms"][name]["predictions"] for name in ARMS}
    raw["delta"] = [{k: v for k, v in r.items() if k != "x"} for r in delta]
    evaluations = {
        name: baseline.evaluate_predictions_for_calendar(
            calendar=calendar,
            sector_returns=facts["sector_returns"],
            benchmark_close=facts["benchmark_close"],
            predictions=rows,
            decision_start=ridge.PREDICTION_START,
            decision_end=ridge.PREDICTION_END,
            outcome_end=ridge.PREDICTION_END,
            report_blocks=ridge.BLOCKS,
        )
        for name, rows in raw.items()
    }
    paired = price._paired(
        calendar,
        [evaluations[name] for name in ("interaction", "control", "delta")],
        comparison_names=("interaction_minus_control", "interaction_minus_delta"),
    )
    paired["evidence_sufficient"] = (
        all(evaluations[name]["metrics"]["evidence_sufficient"] for name in ARMS)
        and paired["interaction_minus_control"]["valid_date_count"] >= math.ceil(0.90 * 222)
        and all(
            share is not None and share >= 0.90
            for day, row in paired["daily"].items()
            if day <= "2026-03-17"
            for share in row["common_coverage"][:2]
        )
    )
    for j, name in enumerate(("interaction_minus_control", "interaction_minus_delta"), 1):
        differences = {
            date.fromisoformat(day): r["spread"][0] - r["spread"][j]
            for day, r in paired["daily"].items()
            if r["spread"][0] is not None and r["spread"][j] is not None
        }
        paired[name]["spread"] = {
            "valid_date_count": len(differences),
            "hac": baseline._newey_west(calendar, differences),
        }
    value = {
        "schema_version": ACCEPTANCE_SCHEMA,
        "contract": CONTRACT,
        "evaluation_contract": EVALUATION_CONTRACT,
        "input_hash": input_bundle["input_hash"],
        "input_identity": parsed["identity"],
        "outcome_sha256": facts["outcome_sha256"],
        "models": {
            name: {
                **{k: first["arms"][name][k] for k in ("parameters", "prediction_sha256", "training_summary")},
                "model_hash": canonical_sha256(
                    {
                        "contract_hash": _Arm(name).MODEL_CONTRACT_HASH,
                        "parameter_sha256": first["arms"][name]["parameters"]["parameter_sha256"],
                    }
                ),
            }
            for name in ARMS
        },
        "process_sha256": [first["report_sha256"], second["report_sha256"]],
        "numeric_environment": input_bundle["numeric_environment"],
        "metrics": {name: e["metrics"] for name, e in evaluations.items()},
        "paired_increment": paired,
        "reference_value": _reference_paths(evaluations, facts, calendar),
        "effect_status": evidence_status(paired["interaction_minus_control"]["hac"])
        if paired["evidence_sufficient"]
        else "EVIDENCE_INSUFFICIENT",
        "execution_status": "COMPLETED",
        "planned_fits": 4,
        "completed_fits": 4,
        "validation_basis": CONTRACT["validation_basis"],
        "forward_confirmation": "NOT_STARTED",
        "advisory_status": "NOT_AVAILABLE",
        "production_promoted": False,
        "tail_accessed": False,
        "database_write": False,
        "runtime_action": False,
    }
    return seal(value, "acceptance_sha256")


def validate_acceptance(value):
    verify(value, "acceptance_sha256")
    if set(value.get("models", {})) != set(ARMS):
        raise fail("acceptance model directory differs")
    for name in ARMS:
        arm = _Arm(name)
        model = value["models"][name]
        ridge._validate_parameter_identity(model["parameters"], variant=arm)
        if model.get("model_hash") != canonical_sha256(
            {
                "contract_hash": arm.MODEL_CONTRACT_HASH,
                "parameter_sha256": model["parameters"]["parameter_sha256"],
            }
        ):
            raise fail("acceptance parameter/model identity differs")
    price.validate_numeric_environment(value["numeric_environment"])
    identity = value["input_identity"]
    if any(identity.get(k) != v for k, v in APPROVED_INPUT.items()) or any(
        identity.get("source_file_hashes", {}).get(k) != v for k, v in FILE_PINS.items()
    ):
        raise fail("acceptance release identity differs")
    if (
        value.get("schema_version") != ACCEPTANCE_SCHEMA
        or value.get("contract") != CONTRACT
        or value.get("evaluation_contract") != EVALUATION_CONTRACT
        or value.get("execution_status") != "COMPLETED"
        or value.get("validation_basis") != CONTRACT["validation_basis"]
        or type(value.get("planned_fits")) is not int
        or value["planned_fits"] != 4
        or type(value.get("completed_fits")) is not int
        or value["completed_fits"] != 4
        or value.get("effect_status")
        != (
            evidence_status(value["paired_increment"]["interaction_minus_control"]["hac"])
            if value["paired_increment"]["evidence_sufficient"]
            else "EVIDENCE_INSUFFICIENT"
        )
        or value.get("forward_confirmation") != "NOT_STARTED"
        or value.get("advisory_status") != "NOT_AVAILABLE"
        or any(
            value.get(k) is not False
            for k in ("production_promoted", "tail_accessed", "database_write", "runtime_action")
        )
    ):
        raise fail("acceptance contract, budget or capability state differs")
