"""One approved raw-return target variant; frozen inputs and sealed comparisons."""

from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path
import re
import sys
from typing import Any, Mapping

from backend.services.hmm_risk import rotation_l2 as baseline
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as price
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json

VERSION = "hmm_risk_rotation_l2_moneyflow_price_return_supervised_v1"
INPUT_SCHEMA, PROCESS_SCHEMA, ACCEPTANCE_SCHEMA = (VERSION + s for s in ("_input", "_process", "_acceptance"))
FEATURE_NAMES, LINEAR_TERMS = price.FEATURE_NAMES, price.LINEAR_TERMS
CONTRACT = {
    **price.CONTRACT,
    "version": VERSION,
    "model_type": "pooled_ridge_moneyflow_price_return",
    "target": "raw_official_l2_relative_10d_return_decimal",
}
MODEL_CONTRACT_HASH = canonical_sha256(CONTRACT)
EVALUATION_CONTRACT = {
    **price.EVALUATION_CONTRACT,
    "version": VERSION,
    "comparison": "frozen_four_feature_rank_primary_and_delta_secondary_common_mature_population",
}
EVALUATION_HASH = canonical_sha256(EVALUATION_CONTRACT)
ORIGINAL_INPUT_HASH = "4ac21b8c54efce631a81718cd4541e9c986ae1a1e2e77cfea16923f1551ceb06"
REFERENCE_PINS = {
    "acceptance_sha256": "4512525706fbead2f43460751affd6e34071336047b158660699fb4b3f5c45ab",
    "model_hash": "8a61e30e0474783b0677a08d946ea3d06561ef29df1ddf1f51e557cc23742b75",
    "model_parameter_sha256": "650784c6b0ea5ea4cd4d055f0a2cd1d3de24fa8d09895f88fe14202762a87222",
    "prediction_sha256": "ecc05eb78d145ea2c135356ebaf4f3a493ac1a07aeefc1b19334a66f1ea41db1",
    "outcome_sha256": "4a900bac2e104326572471aacaa67b649ebd45a03b8a90ac751dd8fc22b32d1c",
    "input_hash": ORIGINAL_INPUT_HASH,
}
FROZEN_REFERENCE_INPUT = True
fail, seal, verify = ridge.fail, ridge.seal, ridge.verify
numeric_environment = price.numeric_environment


def _ordinary(path: Path) -> Path:
    if (
        not path.is_absolute()
        or not path.is_file()
        or any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()) for p in (path, *path.parents))
    ):
        raise fail("frozen reference requires an explicit ordinary absolute file")
    return path


def _original(bundle: Mapping[str, Any]) -> dict[str, Any]:
    binding = bundle.get("original_input", {})
    if binding.get("input_hash") != ORIGINAL_INPUT_HASH:
        raise fail("original input pin differs")
    original = read_json(_ordinary(Path(binding.get("path", ""))))
    if original.get("input_hash") != ORIGINAL_INPUT_HASH:
        raise fail("frozen four-feature input hash differs")
    price.validate_input(original)
    for row in original["source"]["sector_returns"]:
        if row.get("quote_available") is True and type(row.get("pct_change")) not in (int, float):
            raise fail("raw training quote must be numeric, not a boolean or missing value")
    if any(type(row.get("close")) not in (int, float) for row in original["source"]["benchmark_close"]):
        raise fail("raw training benchmark must be numeric, not a boolean or missing value")
    return original


def validate_input(bundle: Mapping[str, Any]) -> dict[str, Any]:
    verify(bundle, "input_hash")
    if bundle.get("schema_version") != INPUT_SCHEMA or bundle.get("contract") != CONTRACT:
        raise fail("return-target input contract differs")
    if set(bundle) != {
        "schema_version",
        "contract",
        "source",
        "original_input",
        "reference",
        "numeric_environment",
        "input_hash",
    }:
        raise fail("return-target wrapper contains unapproved fields")
    original = _original(bundle)
    parsed = price.validate_input(original)
    reference = bundle.get("reference", {})
    if reference.get("pins") != REFERENCE_PINS:
        raise fail("return-target comparison pins differ")
    _ordinary(Path(reference.get("path", "")))
    source = bundle.get("source", {})
    executor = source.get("identity", {}).get("source_git_commit")
    if not isinstance(executor, str) or not re.fullmatch("[0-9a-f]{40}", executor):
        raise fail("executor commit is absent")
    if (
        set(source) != {"identity", "evaluation_source_binding"}
        or set(source.get("identity", {})) != {"source_git_commit"}
        or source.get("evaluation_source_binding") != {"root": original["source"]["evaluation_source_binding"]["root"]}
        or bundle.get("numeric_environment") != original["numeric_environment"]
    ):
        raise fail("wrapper source/numeric binding differs")
    parsed = {
        **parsed,
        "identity": {
            **parsed["identity"],
            "frozen_source_git_commit": parsed["identity"]["source_git_commit"],
            "source_git_commit": executor,
            "source_commit": hashlib.sha256(executor.encode()).hexdigest(),
        },
    }
    return parsed


def prepare_inputs(*, original_input_path: Path, reference_acceptance_path: Path, source_commit: str) -> dict[str, Any]:
    original = read_json(_ordinary(original_input_path))
    price.validate_input(original)
    if original["input_hash"] != ORIGINAL_INPUT_HASH:
        raise fail("preflight original input differs")
    _ordinary(reference_acceptance_path)
    # No parsed prediction-period outcomes are read here or by either fit child.
    from sklearn.linear_model import Ridge  # noqa: F401
    from threadpoolctl import threadpool_limits

    with threadpool_limits(limits=1):
        environment = numeric_environment()
    if environment != original["numeric_environment"]:
        raise fail("preflight numeric payload differs from the frozen four-feature environment")
    bundle = seal(
        {
            "schema_version": INPUT_SCHEMA,
            "contract": CONTRACT,
            "source": {
                "identity": {"source_git_commit": source_commit},
                "evaluation_source_binding": {"root": original["source"]["evaluation_source_binding"]["root"]},
            },
            "original_input": {"path": str(original_input_path), "input_hash": ORIGINAL_INPUT_HASH},
            "reference": {"path": str(reference_acceptance_path), "pins": REFERENCE_PINS},
            "numeric_environment": environment,
        },
        "input_hash",
    )
    validate_input(bundle)
    return bundle


def feature_rows(bundle: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[date], list[date]]:
    validate_input(bundle)
    return price.feature_rows(_original(bundle))


def training_matrix(bundle: Mapping[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    validate_input(bundle)
    return ridge.training_matrix(price._old_bundle(_original(bundle)), rows, raw_return_target=True)


def predictions_from_parameters(rows: list[dict[str, Any]], parameters: Mapping[str, Any]) -> list[dict[str, Any]]:
    return ridge.predictions_from_parameters(rows, parameters, term_names=LINEAR_TERMS)


def run_process(bundle: Mapping[str, Any], *, process_index: int) -> dict[str, Any]:
    return ridge.run_process(bundle, process_index=process_index, variant=sys.modules[__name__])


def verify_processes(
    first: Mapping[str, Any], second: Mapping[str, Any], *, input_bundle: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    return ridge.verify_processes(first, second, input_bundle=input_bundle, variant=sys.modules[__name__])


def _reference(bundle: Mapping[str, Any]) -> dict[str, Any]:
    acceptance, _ = price._reference(Path(bundle["reference"]["path"]), variant=price, pins=REFERENCE_PINS)
    original = _original(bundle)
    if (
        acceptance["input_hash"] != original["input_hash"]
        or acceptance["input_identity"] != original["source"]["identity"]
    ):
        raise fail("frozen rank-target comparison does not share input authority")
    return acceptance


def read_evaluation_facts(bundle: Mapping[str, Any]) -> dict[str, Any]:
    validate_input(bundle)
    reference = _reference(bundle)
    return seal(
        {
            "schema_version": VERSION + "_outcomes",
            "input_hash": bundle["input_hash"],
            "reference_pins": REFERENCE_PINS,
            "rows": [
                {k: row[k] for k in ("trade_date", "sector_code", "outcome_status", "relative_return_10d") if k in row}
                for row in reference["predictions"]
            ],
            "tail_accessed": False,
        },
        "outcome_sha256",
    )


def _evaluate(predictions: list[dict[str, Any]], facts: Mapping[str, Any], calendar: list[date]) -> dict[str, Any]:
    outcomes = {(r["trade_date"], r["sector_code"]): r for r in facts["rows"]}
    if len(outcomes) != len(facts["rows"]) or set(outcomes) != {
        (r["trade_date"], r["sector_code"]) for r in predictions
    }:
        raise fail("sealed outcome directory differs from predictions")
    evaluated = []
    for row in predictions:
        labels = outcomes[row["trade_date"], row["sector_code"]]
        # The wider delta population can include rows excluded by price eligibility.
        # Such outcomes are unavailable, never manufactured from a neutral value.
        if row["availability"] != "available" and labels["outcome_status"] != "outcome_not_mature":
            labels = {**labels, "outcome_status": "prediction_unavailable"}
            labels.pop("relative_return_10d", None)
        elif row["availability"] == "available" and labels["outcome_status"] == "prediction_unavailable":
            raise fail("sealed reference has no outcome for a newly eligible comparison row")
        evaluated.append({**row, **labels})
    return baseline.summarize_evaluated_predictions(
        calendar=calendar,
        evaluated_rows=evaluated,
        decision_start=ridge.PREDICTION_START,
        decision_end=ridge.PREDICTION_END,
        outcome_end=ridge.PREDICTION_END,
        report_blocks=ridge.BLOCKS,
    )


def _spread_increments(paired: dict[str, Any], calendar: list[date]) -> None:
    for j, name in enumerate(("candidate_minus_rank_target", "candidate_minus_delta"), 1):
        values = {
            date.fromisoformat(day): row["spread"][0] - row["spread"][j]
            for day, row in paired["daily"].items()
            if row["spread"][0] is not None and row["spread"][j] is not None
        }
        paired[name]["spread"] = {
            "valid_date_count": len(values),
            "daily_difference": {d.isoformat(): v for d, v in values.items()},
            "hac": baseline._newey_west(calendar, values),
            "blocks": {
                n: baseline._newey_west(calendar, {d: v for d, v in values.items() if a <= d <= b})
                for n, a, b in ridge.BLOCKS
            },
        }


def close_processes(
    first: Mapping[str, Any], second: Mapping[str, Any], *, input_bundle: Mapping[str, Any], facts: Mapping[str, Any]
) -> dict[str, Any]:
    parsed, _ = verify_processes(first, second, input_bundle=input_bundle)
    verify(facts, "outcome_sha256")
    if facts != read_evaluation_facts(input_bundle):
        raise fail("sealed evaluation facts differ from the frozen rank-target authority")
    reference = _reference(input_bundle)
    calendar = list(parsed["calendar"])
    delta_rows, _, _ = ridge.feature_rows(price._old_bundle(_original(input_bundle)))
    delta_rows = [
        {k: v for k, v in r.items() if k != "x"}
        for r in delta_rows
        if ridge.PREDICTION_START <= date.fromisoformat(r["trade_date"]) <= ridge.PREDICTION_END
    ]
    evaluations = [
        _evaluate(predictions, facts, calendar)
        for predictions in (first["predictions"], reference["predictions"], delta_rows)
    ]
    if (
        evaluations[1]["metrics"] != reference["metrics"]
        or evaluations[1]["evaluated_rows"] != reference["predictions"]
        or evaluations[2]["metrics"] != reference["baseline_metrics"]
    ):
        raise fail("zero-fit rank-target/delta comparison readback differs")
    candidate = evaluations[0]
    model_hash = canonical_sha256(
        {"contract_hash": MODEL_CONTRACT_HASH, "parameter_sha256": first["parameters"]["parameter_sha256"]}
    )
    paired = price._paired(
        calendar, evaluations, comparison_names=("candidate_minus_rank_target", "candidate_minus_delta")
    )
    _spread_increments(paired, calendar)
    body = {
        k: reference[k]
        for k in (
            "planned_fits",
            "started_fits",
            "completed_fits",
            "failed_fits",
            "tail_accessed",
            "research_surface_status",
            "forward_power_status",
            "forward_confirmation",
            "advisory_status",
            "validation_basis",
            "selection_basis",
            "database_write",
            "runtime_action",
            "execution_status",
        )
    }
    body.update(
        schema_version=ACCEPTANCE_SCHEMA,
        contract_version=VERSION,
        contract=CONTRACT,
        model_contract_hash=MODEL_CONTRACT_HASH,
        model_hash=model_hash,
        parameters=first["parameters"],
        model_parameter_sha256=first["parameters"]["parameter_sha256"],
        evaluation_contract_hash=EVALUATION_HASH,
        evaluation_contract=EVALUATION_CONTRACT,
        input_hash=input_bundle["input_hash"],
        input_identity=parsed["identity"],
        mapping_hash=parsed["identity"]["mapping_hash"],
        quote_authority_hash=parsed["identity"]["quote_authority_hash"],
        outcome_sha256=facts["outcome_sha256"],
        training_summary=first["training_summary"],
        numeric_environment=first["numeric_environment"],
        effect_status=candidate["effect_status"],
        metrics=candidate["metrics"],
        rank_target_metrics=evaluations[1]["metrics"],
        baseline_metrics=evaluations[2]["metrics"],
        reference_pins=REFERENCE_PINS,
        paired_increment=paired,
        feature_populations=reference["feature_populations"],
        predictions=candidate["evaluated_rows"],
        net_value_status="UNASSESSED",
        raw_prediction_unit="relative_return_decimal",
        rotation_l2_capability_status="RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
        if candidate["effect_status"] == "DEVELOPMENT_EFFECT_QUALIFIED"
        else "NOT_AVAILABLE",
    )
    body["run_id"] = canonical_sha256(
        {
            "model_hash": model_hash,
            "evaluation_hash": EVALUATION_HASH,
            "input_hash": body["input_hash"],
            "outcome_hash": body["outcome_sha256"],
        }
    )
    body["economic_metrics"] = {
        name: {
            "native_spread_hac": baseline._newey_west(
                calendar, {date.fromisoformat(d): v for d, v in evaluation["metrics"]["daily_spread"].items()}
            ),
            "blocks": {
                n: baseline._newey_west(
                    calendar,
                    {
                        date.fromisoformat(d): v
                        for d, v in evaluation["metrics"]["daily_spread"].items()
                        if a <= date.fromisoformat(d) <= b
                    },
                )
                for n, a, b in ridge.BLOCKS
            },
        }
        for name, evaluation in zip(("candidate", "rank_target", "delta"), evaluations, strict=True)
    }
    body["diagnostics"] = [
        {"model": name, "reason_code": "hmm_risk_rotation_l2_ic_spread_sign_disagreement", "promotion_gate": False}
        for name, evaluation in zip(("candidate", "rank_target", "delta"), evaluations, strict=True)
        if evaluation["metrics"]["overall"]["mean_daily_rank_ic"] is not None
        and evaluation["metrics"]["overall"]["mean_daily_spread"] is not None
        and evaluation["metrics"]["overall"]["mean_daily_rank_ic"]
        * evaluation["metrics"]["overall"]["mean_daily_spread"]
        < 0
    ]
    return seal(body, "acceptance_sha256")


def validate_acceptance(value: Mapping[str, Any]) -> None:
    ridge.validate_acceptance(value, variant=sys.modules[__name__])
    if (
        value.get("reference_pins") != REFERENCE_PINS
        or value.get("net_value_status") != "UNASSESSED"
        or value.get("raw_prediction_unit") != "relative_return_decimal"
        or set(value.get("paired_increment", {}))
        != {"population", "daily", "candidate_minus_rank_target", "candidate_minus_delta"}
    ):
        raise fail("return-target result comparison/unit identity differs")
