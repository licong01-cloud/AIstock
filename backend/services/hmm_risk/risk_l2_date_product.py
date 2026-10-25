"""Import already sealed, label-free date inference into the existing risk product."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Mapping

from backend.services.hmm_risk import risk_l2 as risk
from backend.services.hmm_risk import risk_l2_date_inference as inference
from backend.services.hmm_risk import risk_l2_prediction as product
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_model import FormalStateError

REQUEST_SCHEMA = "hmm_risk_l2_date_product_import_v1"
BASIS = "CAUSAL_DATE_INFERENCE_NO_OUTCOMES"
EFFECT = "NOT_EVALUATED_FOR_INFERENCE_DATES"
OUTCOME = "OUTCOME_NOT_EVALUATED"
ASSETS = ("inference_request", "features", "acceptance", "prediction_1", "prediction_2")


def from_assets(request: Mapping, assets: Mapping) -> product.RiskL2Product:
    """No prepare, inference, evaluation, label builder or database access."""
    try:
        product._require(
            set(request) == {"schema_version"} | {f"{name}_{key}" for name in ASSETS for key in ("path", "hash")}
            and request["schema_version"] == REQUEST_SCHEMA
            and set(assets) == set(ASSETS),
            "date product import fields/schema differ",
        )
        for name in ASSETS:
            product._receipt(assets[name], request[name + "_hash"])
        source = assets["inference_request"]
        bundle, acceptance = assets["features"], assets["acceptance"]
        first, second = assets["prediction_1"], assets["prediction_2"]
        product._require(
            set(acceptance)
            == {
                "schema_version",
                "execution_status",
                "request_sha256",
                "executor_commit",
                "features_sha256",
                "prediction_sha256",
                "model_identity",
                "inference_identity",
                "dates",
                "two_process_bitwise_equal",
                "completed_children",
                "fits",
                "labels_read",
                "outcomes_read",
                "database_write",
                "dataset_write",
                "active_profile_write",
                "runtime_action",
                "policy_applied",
                "forward_confirmed",
                "receipt_sha256",
            }
            and all(
                set(item)
                == {
                    "schema_version",
                    "request_sha256",
                    "features_sha256",
                    "source_commit",
                    "model_identity",
                    "inference_identity",
                    "capability_state",
                    "predictions",
                    "dates",
                    "inactive_changed_rows",
                    "warning_threshold",
                    "fits",
                    "labels_read",
                    "outcomes_read",
                    "forward_confirmed",
                    "policy_applied",
                    "numeric_environment",
                    "receipt_sha256",
                }
                and isinstance(item["numeric_environment"], dict)
                and bool(item["numeric_environment"])
                for item in (first, second)
            ),
            "inference carriers contain undeclared fields or no numeric identity",
        )
        inference.validate_bundle(source, bundle, request["features_hash"])
        for prediction in (first, second):
            inference.validate_prediction(source, bundle, prediction)
        product._require(
            first == second
            and acceptance["schema_version"] == inference.VERSION + "_acceptance"
            and acceptance["execution_status"] == "COMPLETED"
            and acceptance["two_process_bitwise_equal"] is True
            and type(acceptance["completed_children"]) is int
            and acceptance["completed_children"] == 2
            and type(acceptance["fits"]) is int
            and acceptance["fits"] == 0
            and all(
                acceptance[key] is False
                for key in (
                    "labels_read",
                    "outcomes_read",
                    "database_write",
                    "dataset_write",
                    "active_profile_write",
                    "runtime_action",
                    "policy_applied",
                    "forward_confirmed",
                )
            )
            and acceptance["request_sha256"] == request["inference_request_hash"]
            and acceptance["features_sha256"] == request["features_hash"]
            and acceptance["prediction_sha256"] == request["prediction_1_hash"] == request["prediction_2_hash"]
            and acceptance["executor_commit"] == first["source_commit"] == bundle["source_commit"]
            and acceptance["model_identity"] == first["model_identity"] == bundle["model_identity"]
            and acceptance["inference_identity"] == first["inference_identity"] == bundle["inference_identity"]
            and acceptance["dates"] == first["dates"],
            "date inference parent/children/source identities differ",
            product.REASON_CONFLICT,
        )
        run_id = request["acceptance_hash"]
        rows = product._ordered(
            [
                {
                    "run_id": run_id,
                    **{
                        key: raw[key]
                        for key in (
                            "trade_date",
                            "as_of_date",
                            "sector_code",
                            "probability",
                            "warning",
                            "availability",
                            "reason_code",
                            "structural_eligible",
                        )
                    },
                    "sector_level": "L2",
                    "sector_name": None,
                    "name_authority": "CANONICAL_CODE_ONLY",
                    "outcome_status": OUTCOME,
                    "event": None,
                    "realized_drawdown": None,
                    "realized_return": None,
                }
                for raw in first["predictions"]
            ]
        )
        days, identity = source["decision_dates"], bundle["inference_identity"]
        grouped = {day: [row for row in rows if row["trade_date"] == day] for day in days}
        daily = [
            {
                "trade_date": day,
                "available_count": sum(row["availability"] == "available" for row in grouped[day]),
                "unavailable_count": sum(row["availability"] == "unavailable" for row in grouped[day]),
                "warning_count": sum(row["warning"] is True for row in grouped[day]),
            }
            for day in days
        ]
        summary = {
            "product_schema": REQUEST_SCHEMA,
            "row_hash": canonical_sha256(rows),
            "day_row_hashes": {day: canonical_sha256(items) for day, items in grouped.items()},
            "daily": daily,
            "prediction_coverage": sum(item["available_count"] for item in daily) / len(rows),
            "outcome_status_counts": dict(Counter(row["outcome_status"] for row in rows)),
            "model_identity": bundle["model_identity"],
            "fits": 0,
            "labels_read": False,
            "outcomes_read": False,
            "policy_applied": False,
        }
        run = {
            "run_id": run_id,
            "acceptance_hash": run_id,
            "sealed_prediction_hash": request["prediction_1_hash"],
            "feature_hash": request["features_hash"],
            "model_hash": bundle["model_identity"]["model_sha256"],
            "contract_hash": canonical_sha256(risk.CONTRACT),
            "input_hash": canonical_sha256(identity),
            "mapping_hash": canonical_sha256(
                {key: identity[key] for key in ("industry_authority_sha256", "security_identity_sha256")}
            ),
            "executor_commit": bundle["source_commit"],
            "model_version": risk.VERSION,
            "validation_basis": BASIS,
            "catalog": bundle["catalog"],
            "dates": days,
            "input_identity": identity,
            "compact_summary": summary,
            "expected_row_count": len(rows),
            "effect_status": EFFECT,
            "risk_l2_capability_status": first["capability_state"],
            "forward_power_status": "UNAVAILABLE",
            "forward_confirmation": "NOT_STARTED",
            "advisory_status": "NOT_AVAILABLE",
        }
        summary["run_identity_hash"] = product._run_hash(run)
        result = product.RiskL2Product(run, rows)
        product.validate_product(result)
        return result
    except product.RiskL2PredictionError:
        raise
    except (FormalStateError, KeyError, TypeError, ValueError, IndexError, OSError) as exc:
        raise product.RiskL2PredictionError(product.REASON_INPUT, "date product evidence is invalid") from exc


def load(request_path: Path) -> product.RiskL2Product:
    try:
        request, stamp = product._read(request_path)
        assets, stamps = {}, {}
        for name in ASSETS:
            assets[name], stamps[name] = product._read(Path(request[name + "_path"]))
        product._require(
            stamps["prediction_1"][-1] == stamps["prediction_2"][-1],
            "fresh-process prediction file bytes differ",
            product.REASON_CONFLICT,
        )
        result = from_assets(request, assets)
        for name in ASSETS:
            product._stable(Path(request[name + "_path"]), stamps[name])
        product._stable(request_path, stamp)
        return result
    except product.RiskL2PredictionError:
        raise
    except (OSError, UnicodeError, ValueError, KeyError, TypeError) as exc:
        raise product.RiskL2PredictionError(product.REASON_INPUT, "date product artifact read failed") from exc


def validate_run(run: Mapping) -> None:
    """Stored inference is not an effect re-evaluation or an immature label."""
    summary, identity = run["compact_summary"], run["input_identity"]
    dates = run["dates"]
    product._require(
        run["validation_basis"] == BASIS
        and run["effect_status"] == EFFECT
        and isinstance(dates, list)
        and bool(dates)
        and dates == sorted(set(dates))
        and all(risk.TRAIN_END < product._iso(day) <= identity["release_identity"]["cutoff"] for day in dates)
        and run["expected_row_count"] == 131 * len(dates)
        and summary["product_schema"] == REQUEST_SCHEMA
        and summary["model_identity"]["model_sha256"] == run["model_hash"]
        and type(summary["fits"]) is int
        and summary["fits"] == 0
        and all(summary[key] is False for key in ("labels_read", "outcomes_read", "policy_applied"))
        and summary["outcome_status_counts"] == {OUTCOME: run["expected_row_count"]}
        and 0 <= summary["prediction_coverage"] <= 1
        and run["risk_l2_capability_status"]
        == (product.CAPABILITY if summary["prediction_coverage"] > 0 else "NOT_AVAILABLE")
        and [item["trade_date"] for item in summary["daily"]] == dates,
        "stored date inference contract differs",
        product.REASON_READBACK,
    )
