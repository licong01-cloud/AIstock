"""One approved four-feature L2 Ridge; existing terminal Ridge is read-only."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import math
import os
from pathlib import Path
import sys
from typing import Any, Mapping

from backend.services.hmm_risk import rotation_l2 as baseline
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json

VERSION = "hmm_risk_rotation_l2_moneyflow_price_supervised_v1"
INPUT_SCHEMA, PROCESS_SCHEMA, ACCEPTANCE_SCHEMA = (VERSION + suffix for suffix in ("_input", "_process", "_acceptance"))
BASIS = ridge.BASIS
TRAIN_END, TRAIN_OUTCOME_END = ridge.TRAIN_END, ridge.TRAIN_OUTCOME_END
FEATURE_NAMES = (*ridge.FEATURE_NAMES, "relative_momentum_rank", "relative_downside_rank")
LINEAR_TERMS = (*ridge.LINEAR_TERMS, "relative_momentum_linear_term", "relative_downside_linear_term")
CONTRACT = {
    **ridge.CONTRACT,
    "version": VERSION,
    "model_type": "pooled_ridge_moneyflow_price_rank",
    "feature_names": list(FEATURE_NAMES),
    "price_sessions": 20,
    "momentum": "product(1+official_l2_return)-product(1+benchmark_return)",
    "downside": "sqrt(fsum(min(l2_return-benchmark_return,0)**2)/20)",
    "moneyflow_rank_population": "E0_before_price_eligibility",
    "price_rank_population": "E_plus_before_outcomes",
}
MODEL_CONTRACT_HASH = canonical_sha256(CONTRACT)
EVALUATION_CONTRACT = {
    **ridge.EVALUATION_CONTRACT,
    "version": VERSION,
    "comparison": "delta_primary_and_frozen_two_feature_ridge_three_way_common_mature_population",
}
EVALUATION_HASH = canonical_sha256(EVALUATION_CONTRACT)
THREADS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "BLIS_NUM_THREADS",
)
APPROVED_NUMERIC = {
    "python": "3.13.5",
    "versions": {"numpy": "2.3.3", "scipy": "1.16.3", "scikit-learn": "1.8.0", "threadpoolctl": "3.6.0"},
}
APPROVED_INPUT = {
    "generation": "20260928-v15-unified-moneyflow1",
    "manifest_sha256": "2225e1ea28f099f4972b6a465e4aa093d3767592e2651484b79700586bf358fc",
    "profile_sha256": "56b4741044aa98750468b2d5b2b9ae888cf2abf9ba4df00e36019ebc3dd0cc6f",
}
REFERENCE_PINS = {
    "acceptance_sha256": "b05adf612f6d355c6fe82e82012c91eca006a5ae68e156470ae509abc69b18df",
    "model_hash": "518fbb2ac7cf1b574d1a2fafe9545f442cafe2ad5ec05a872d0435133cc69aa9",
    "model_parameter_sha256": "cf572c524548fb3705917fdd074aa84fa86a8db8351c17497d26119c4a80e3aa",
    "prediction_sha256": "199db281e8c5a1307e6c073426c0d7904d2853f12e7fd06fa9308f9f2a3e1b1c",
    "outcome_sha256": "0d97ba50d67631be0dc05f9ae3961ccefb21828603f22ab9062e66b477c24484",
    "input_hash": "a9aed4ab2bcedec3bde8b550d0e5fe997b966d9694d2e1834d832f0c4f43177f",
}
seal, verify, fail = ridge.seal, ridge.verify, ridge.fail


def _old_bundle(bundle: Mapping[str, Any]) -> dict[str, Any]:
    return seal(
        {"schema_version": ridge.INPUT_SCHEMA, "contract": ridge.CONTRACT, "source": bundle["source"]}, "input_hash"
    )


def _reference(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    if not path.is_absolute() or path.name != "acceptance.json":
        raise fail("reference requires an explicit absolute acceptance.json path")
    for target in (path, path.parent / "process_1.json", path.parent / "process_2.json"):
        if not target.is_file() or any(
            p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()) for p in (target, *target.parents)
        ):
            raise fail("reference must be ordinary immutable files")
    acceptance = read_json(path)
    ridge.validate_acceptance(acceptance)
    children = [read_json(path.parent / f"process_{i}.json") for i in (1, 2)]
    for i, child in enumerate(children, 1):
        verify(child, "report_sha256")
        if (
            child.get("schema_version") != ridge.PROCESS_SCHEMA
            or child.get("contract") != ridge.CONTRACT
            or child.get("process_index") != i
            or child.get("parameters") != acceptance["parameters"]
            or child.get("numeric_environment") != acceptance["numeric_environment"]
            or child.get("training_summary") != acceptance["training_summary"]
            or child.get("source_commit") != acceptance["input_identity"]["source_git_commit"]
            or child.get("input_hash") != acceptance["input_hash"]
            or child.get("prediction_sha256") != canonical_sha256(child.get("predictions"))
            or any(
                type(child.get(k)) is not int or child[k] != v
                for k, v in {"planned_fits": 1, "started_fits": 1, "completed_fits": 1, "failed_fits": 0}.items()
            )
            or any(child.get(k) is not False for k in ("tail_accessed", "database_write", "runtime_action"))
        ):
            raise fail("frozen reference child/acceptance linkage differs")

    def comparable(child):
        return {k: v for k, v in child.items() if k not in {"process_index", "report_sha256"}}

    if comparable(children[0]) != comparable(children[1]):
        raise fail("frozen reference children differ")
    observed = {
        **{k: acceptance.get(k) for k in REFERENCE_PINS if k != "prediction_sha256"},
        "prediction_sha256": children[0]["prediction_sha256"],
    }
    if observed != REFERENCE_PINS:
        raise fail("frozen reference differs from approved pins")
    raw = children[0]["predictions"]
    if [
        {k: v for k, v in r.items() if k not in {"outcome_status", "relative_return_10d"}}
        for r in acceptance["predictions"]
    ] != [{k: v for k, v in r.items() if k not in {"outcome_status", "relative_return_10d"}} for r in raw]:
        raise fail("reference evaluated and sealed prediction grids differ")
    return acceptance, children[0]


def numeric_environment() -> dict[str, Any]:
    environment = ridge.numeric_environment()
    environment["thread_variables"] = {key: os.environ.get(key) for key in THREADS}
    return environment


def prepare_inputs(*, reference_acceptance_path: Path, **source_args: Any) -> dict[str, Any]:
    from threadpoolctl import threadpool_limits
    from backend.services.hmm_risk.rotation_l2_input import bounded_price_features

    old = ridge.prepare_inputs(**source_args)
    reference, _ = _reference(reference_acceptance_path)
    # Load the same estimator dependencies as the fit child before discovering
    # and limiting native pools. A cold preflight otherwise omits sklearn's
    # OpenMP library from the frozen environment; importing does not fit.
    from sklearn.linear_model import Ridge  # noqa: F401

    with threadpool_limits(limits=1):
        environment = numeric_environment()
    expected = {**reference["numeric_environment"], "thread_variables": {key: "1" for key in THREADS}}
    if environment != expected:
        raise fail("preflight numeric payload differs from the approved prior environment")
    bundle = seal(
        {
            "schema_version": INPUT_SCHEMA,
            "contract": CONTRACT,
            "source": old["source"],
            "price_features": bounded_price_features(old["source"]),
            "numeric_environment": environment,
            "reference": {
                "path": str(reference_acceptance_path),
                "pins": REFERENCE_PINS,
                "input_identity": reference["input_identity"],
            },
        },
        "input_hash",
    )
    validate_input(bundle)
    return bundle


def validate_input(bundle: Mapping[str, Any]) -> dict[str, Any]:
    verify(bundle, "input_hash")
    if bundle.get("schema_version") != INPUT_SCHEMA or bundle.get("contract") != CONTRACT:
        raise fail("four-feature input contract differs")
    parsed = ridge.validate_input(_old_bundle(bundle))
    identity = parsed["identity"]
    if any(identity.get(k) != v for k, v in APPROVED_INPUT.items()):
        raise fail("four-feature input differs from approved release/profile")
    reference = bundle.get("reference", {})
    if reference.get("pins") != REFERENCE_PINS or not Path(reference.get("path", "")).is_absolute():
        raise fail("reference input binding is absent or differs")
    # Executor commit legitimately changes. Every common source pin must not.
    keys = (
        "profile_sha256",
        "manifest_sha256",
        "mapping_hash",
        "quote_authority_hash",
        "calendar_hash",
        "release_id",
        "generation",
        "cutoff",
        "source_file_hashes",
    )
    if any(identity.get(k) != reference.get("input_identity", {}).get(k) for k in keys):
        raise fail("reference and candidate do not share source identities")
    view = bundle["price_features"]
    verify(view, "price_sha256")
    calendar, codes = parsed["calendar"], parsed["catalog_codes"]
    days = {d.isoformat() for d in calendar[:-1]}
    binding = bundle["source"]["evaluation_source_binding"]
    if any(
        binding[name].get("sha256") != identity["source_file_hashes"].get(key)
        for name, key in (("sector", "sector_data_h5"), ("index", "index_daily_h5"))
    ):
        raise fail("price reader binding differs from source file identities")
    if (
        view.get("schema_version") != "hmm_risk_rotation_l2_official_price_features_v1"
        or view.get("start") != calendar[0].isoformat()
        or view.get("end") != calendar[-2].isoformat()
        or view.get("source_pins") != {k: binding[k] for k in ("sector", "index")}
    ):
        raise fail("price view bounds or source pins differ")
    grid = {}
    for row in view["sector_returns"]:
        key = row["trade_date"], row["sector_code"]
        if key in grid or key[0] not in days or key[1] not in codes or type(row.get("quote_available")) is not bool:
            raise fail("price grid has duplicate/unknown/out-of-range identity")
        spans = binding["quote_entries"][key[1]]
        authority = any(a <= key[0] <= b for a, b in spans)
        pct = row.get("pct_change")
        if (
            row["quote_available"] != authority
            or (authority and (type(pct) not in (int, float) or not math.isfinite(pct) or 1 + pct / 100 <= 0))
            or (not authority and pct is not None)
        ):
            raise fail("price value differs from quote authority/domain")
        grid[key] = row
    if set(grid) != {(day, code) for day in days for code in codes}:
        raise fail("price view is missing required official rows")
    benchmark = {}
    for row in view["benchmark_close"]:
        day, close = row["trade_date"], row.get("close")
        if (
            day in benchmark
            or day not in days
            or type(close) not in (int, float)
            or not math.isfinite(close)
            or close <= 0
        ):
            raise fail("price benchmark is duplicated/missing/non-finite/non-positive")
        benchmark[day] = close
    if set(benchmark) != days:
        raise fail("price benchmark does not cover the frozen calendar")
    environment = bundle.get("numeric_environment", {})
    if (
        environment.get("python") != APPROVED_NUMERIC["python"]
        or environment.get("versions") != APPROVED_NUMERIC["versions"]
        or environment.get("thread_variables") != {k: "1" for k in THREADS}
        or not environment.get("thread_pools")
        or any(p.get("num_threads") != 1 for p in environment["thread_pools"])
    ):
        raise fail("request numeric contract differs")
    return parsed


def feature_rows(bundle: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[date], list[date]]:
    parsed = validate_input(bundle)
    rows, train, prediction = ridge.feature_rows(_old_bundle(bundle))
    calendar = parsed["calendar"]
    positions = {d.isoformat(): i for i, d in enumerate(calendar)}
    view = bundle["price_features"]
    quotes = {(r["trade_date"], r["sector_code"]): r for r in view["sector_returns"]}
    close = {r["trade_date"]: r["close"] for r in view["benchmark_close"]}
    groups = defaultdict(list)
    for row in rows:
        row["feature_diagnostics"] = {
            **(row["feature_diagnostics"] or {}),
            "moneyflow_feature_eligible": row["availability"] == "available",
        }
        groups[row["trade_date"]].append(row)
    for day, daily in groups.items():
        offset = positions[day]
        window = calendar[offset - 20 : offset]
        market_returns = [
            close[u.isoformat()] / close[calendar[positions[u.isoformat()] - 1].isoformat()] - 1 for u in window
        ]
        market_growth = math.prod(1 + r for r in market_returns)
        momentum, downside = {}, {}
        for row in daily:
            if row["availability"] != "available":
                continue
            code = row["sector_code"]
            official = [quotes[u.isoformat(), code] for u in window]
            if not all(q["quote_available"] for q in official):
                row.update(
                    availability="unavailable",
                    feature_eligible=False,
                    reason_code="hmm_risk_rotation_l2_price_history_unavailable",
                    rotation_score=None,
                    forecast_state=None,
                    feature_contributions=None,
                )
                row.pop("x", None)
                continue
            returns = [q["pct_change"] / 100 for q in official]
            momentum[code] = math.prod(1 + r for r in returns) - market_growth
            downside[code] = math.sqrt(
                math.fsum(min(a - b, 0) ** 2 for a, b in zip(returns, market_returns, strict=True)) / 20
            )
            if not math.isfinite(momentum[code]) or not math.isfinite(downside[code]):
                raise fail("price feature overflow/non-finite", "score_failed")
            row["feature_diagnostics"] = {
                **row["feature_diagnostics"],
                "relative_momentum_20d": momentum[code],
                "relative_downside_20d": downside[code],
                "moneyflow_E0_ranks": list(row["x"]),
            }
        if len(momentum) < 2:
            for row in daily:
                row.update(
                    availability="unavailable",
                    feature_eligible=False,
                    reason_code="hmm_risk_rotation_l2_price_cross_section_insufficient",
                    rotation_score=None,
                    forecast_state=None,
                    feature_contributions=None,
                )
                row.pop("x", None)
            continue
        m, d = (baseline._score_and_states(v)[0] for v in (momentum, downside))
        for row in daily:
            if row["sector_code"] in m:
                row["x"] = [*row["x"], m[row["sector_code"]], d[row["sector_code"]]]
    return rows, train, prediction


def training_matrix(bundle: Mapping[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    return ridge.training_matrix(_old_bundle(bundle), rows)


def predictions_from_parameters(rows: list[dict[str, Any]], parameters: Mapping[str, Any]) -> list[dict[str, Any]]:
    return ridge.predictions_from_parameters(rows, parameters, term_names=LINEAR_TERMS)


def run_process(bundle: Mapping[str, Any], *, process_index: int) -> dict[str, Any]:
    return ridge.run_process(bundle, process_index=process_index, variant=sys.modules[__name__])


def verify_processes(
    first: Mapping[str, Any], second: Mapping[str, Any], *, input_bundle: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    return ridge.verify_processes(first, second, input_bundle=input_bundle, variant=sys.modules[__name__])


def read_evaluation_facts(bundle: Mapping[str, Any]) -> dict[str, Any]:
    validate_input(bundle)
    facts = ridge.read_evaluation_facts(_old_bundle(bundle))
    return seal(
        {
            **{k: v for k, v in facts.items() if k != "outcome_sha256"},
            "schema_version": VERSION + "_outcomes",
            "input_hash": bundle["input_hash"],
        },
        "outcome_sha256",
    )


def _paired(calendar: list[date], evaluations: list[Mapping[str, Any]]) -> dict[str, Any]:
    indexed = [{(r["trade_date"], r["sector_code"]): r for r in e["evaluated_rows"]} for e in evaluations]
    if any(set(table) != set(indexed[0]) for table in indexed[1:]):
        raise fail("three-way date/sector directories differ")
    native_counts = [defaultdict(int) for _ in evaluations]
    unavailable_counts = [defaultdict(int) for _ in evaluations]
    outcome_counts = [defaultdict(int) for _ in evaluations]
    for j, table in enumerate(indexed):
        for (day, _), row in table.items():
            native_counts[j][day] += row["availability"] == "available"
            unavailable_counts[j][day] += row["availability"] != "available"
            outcome_counts[j][day] += row["availability"] == "available" and row["outcome_status"] != "available"
    by_day = defaultdict(list)
    for key in indexed[0]:
        rows = [table[key] for table in indexed]
        if all(r["availability"] == "available" and r["outcome_status"] == "available" for r in rows):
            if len({r["relative_return_10d"] for r in rows}) != 1:
                raise fail("three-way common outcome differs")
            by_day[key[0]].append(rows)
    daily, differences = {}, [dict(), dict()]
    for day in sorted({key[0] for key in indexed[0]}):
        triples = by_day[day]
        outcomes = {rows[0]["sector_code"]: rows[0]["relative_return_10d"] for rows in triples}
        ic = [
            baseline._rank_ic({rows[j]["sector_code"]: rows[j]["rotation_score"] for rows in triples}, outcomes)
            for j in range(3)
        ]
        spreads = []
        for j in range(3):
            high = [rows[j]["relative_return_10d"] for rows in triples if rows[j]["forecast_state"] == "trending"]
            low = [rows[j]["relative_return_10d"] for rows in triples if rows[j]["forecast_state"] == "fading"]
            spreads.append(math.fsum(high) / len(high) - math.fsum(low) / len(low) if high and low else None)
        native = [counts[day] for counts in native_counts]
        daily[day] = {
            "common_count": len(triples),
            "native_counts": native,
            "excluded_counts": [n - len(triples) for n in native],
            "common_coverage": [len(triples) / n if n else None for n in native],
            "excluded_reasons": [
                {
                    "prediction_unavailable": unavailable_counts[j][day],
                    "outcome_unavailable": outcome_counts[j][day],
                    "other_model_not_eligible": native[j] - len(triples) - outcome_counts[j][day],
                }
                for j in range(3)
            ],
            "rank_ic": ic,
            "spread": spreads,
            "spread_reason_codes": [
                None if value is not None else "hmm_risk_rotation_l2_common_spread_group_empty" for value in spreads
            ],
            "reason_code": None if all(v is not None for v in ic) else "hmm_risk_rotation_l2_common_ic_not_computable",
        }
        if all(v is not None for v in ic):
            for j in range(2):
                differences[j][date.fromisoformat(day)] = ic[0] - ic[j + 1]
    result = {"population": "three_way_common_mature_eligible", "daily": daily}
    for name, values in zip(("candidate_minus_delta", "candidate_minus_old_ridge"), differences, strict=True):
        result[name] = {
            "valid_date_count": len(values),
            "hac": baseline._newey_west(calendar, values),
            "daily_ic_difference": {d.isoformat(): v for d, v in values.items()},
            "blocks": {
                n: baseline._newey_west(calendar, {d: v for d, v in values.items() if a <= d <= b})
                for n, a, b in ridge.BLOCKS
            },
        }
    return result


def close_processes(
    first: Mapping[str, Any], second: Mapping[str, Any], *, input_bundle: Mapping[str, Any], facts: Mapping[str, Any]
) -> dict[str, Any]:
    parsed, feature_data = verify_processes(first, second, input_bundle=input_bundle)
    verify(facts, "outcome_sha256")
    if (
        facts.get("schema_version") != VERSION + "_outcomes"
        or facts.get("input_hash") != input_bundle["input_hash"]
        or facts.get("tail_accessed") is not False
    ):
        raise fail("evaluation facts differ")
    old, child = _reference(Path(input_bundle["reference"]["path"]))
    prior_facts = {
        **{k: v for k, v in facts.items() if k != "outcome_sha256"},
        "schema_version": ridge.VERSION + "_outcomes",
        "input_hash": REFERENCE_PINS["input_hash"],
    }
    if canonical_sha256(prior_facts) != REFERENCE_PINS["outcome_sha256"]:
        raise fail("evaluation facts differ from frozen mature outcome authority")
    delta_rows, _, _ = ridge.feature_rows(_old_bundle(input_bundle))
    delta_rows = [
        {k: v for k, v in r.items() if k != "x"}
        for r in delta_rows
        if ridge.PREDICTION_START <= date.fromisoformat(r["trade_date"]) <= ridge.PREDICTION_END
    ]
    evaluations = [
        baseline.evaluate_predictions_for_calendar(
            calendar=parsed["calendar"],
            sector_returns=facts["sector_returns"],
            benchmark_close=facts["benchmark_close"],
            predictions=p,
            decision_start=ridge.PREDICTION_START,
            decision_end=ridge.PREDICTION_END,
            outcome_end=ridge.PREDICTION_END,
            report_blocks=ridge.BLOCKS,
        )
        for p in (first["predictions"], delta_rows, child["predictions"])
    ]
    if evaluations[2]["evaluated_rows"] != old["predictions"] or evaluations[2]["metrics"] != old["metrics"]:
        raise fail("old Ridge zero-fit outcome readback differs")
    candidate = evaluations[0]
    parameters = first["parameters"]
    model_hash = canonical_sha256(
        {"contract_hash": MODEL_CONTRACT_HASH, "parameter_sha256": parameters["parameter_sha256"]}
    )
    run_id = canonical_sha256(
        {
            "model_hash": model_hash,
            "evaluation_hash": EVALUATION_HASH,
            "input_hash": input_bundle["input_hash"],
            "outcome_hash": facts["outcome_sha256"],
        }
    )
    body = {
        k: old[k]
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
        parameters=parameters,
        model_parameter_sha256=parameters["parameter_sha256"],
        evaluation_contract_hash=EVALUATION_HASH,
        evaluation_contract=EVALUATION_CONTRACT,
        run_id=run_id,
        input_hash=input_bundle["input_hash"],
        input_identity=parsed["identity"],
        mapping_hash=parsed["identity"]["mapping_hash"],
        quote_authority_hash=parsed["identity"]["quote_authority_hash"],
        outcome_sha256=facts["outcome_sha256"],
        training_summary=first["training_summary"],
        numeric_environment=first["numeric_environment"],
        effect_status=candidate["effect_status"],
        metrics=candidate["metrics"],
        baseline_metrics=evaluations[1]["metrics"],
        old_ridge_metrics=evaluations[2]["metrics"],
        reference_pins=REFERENCE_PINS,
        paired_increment=_paired(parsed["calendar"], evaluations),
        predictions=candidate["evaluated_rows"],
        rotation_l2_capability_status="RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
        if candidate["effect_status"] == "DEVELOPMENT_EFFECT_QUALIFIED"
        else "NOT_AVAILABLE",
    )
    populations = defaultdict(lambda: {"S": 0, "E0": 0, "E_plus": 0, "exclusions": defaultdict(int)})
    for row in feature_data:
        counts = populations[row["trade_date"]]
        counts["S"] += row["structural_eligible"]
        counts["E0"] += row["feature_diagnostics"]["moneyflow_feature_eligible"]
        counts["E_plus"] += row["feature_eligible"]
        if row["availability"] != "available":
            counts["exclusions"][row["reason_code"]] += 1
    body["feature_populations"] = dict(populations)
    body["diagnostics"] = []
    for name, evaluation in zip(("candidate", "delta", "old_ridge"), evaluations, strict=True):
        overall = evaluation["metrics"]["overall"]
        ic, spread = overall["mean_daily_rank_ic"], overall["mean_daily_spread"]
        if ic is not None and spread is not None and ic * spread < 0:
            body["diagnostics"].append(
                {
                    "model": name,
                    "reason_code": "hmm_risk_rotation_l2_ic_spread_sign_disagreement",
                    "promotion_gate": False,
                }
            )
    return seal(body, "acceptance_sha256")


def validate_acceptance(value: Mapping[str, Any]) -> None:
    ridge.validate_acceptance(value, variant=sys.modules[__name__])
    if (
        value.get("reference_pins") != REFERENCE_PINS
        or value.get("paired_increment", {}).get("population") != "three_way_common_mature_eligible"
    ):
        raise fail("acceptance reference/common population identity differs")


def validate_product_explanation(row: Mapping[str, Any]) -> None:
    ridge.validate_product_explanation(row, variant=sys.modules[__name__])
    if row["run_summary"].get("reference_pins") != REFERENCE_PINS:
        raise fail("product frozen reference differs")
