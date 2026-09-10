"""Durable G2-A L1 rotation predictions and zero-fit inference.

The repository is deliberately model-bound and append-only.  It never chooses
the newest model across identities, fabricates probabilities, or turns an
offline fit receipt into a product-availability claim.
"""

from __future__ import annotations

import importlib.metadata
import json
import math
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.db.pg_pool import get_conn
from backend.services.dataset_release.cas_store import canonical_json_bytes
from backend.services.hmm_risk.rotation_l1_gbdt import (
    BINDING_MBE_IC,
    CANONICAL_SECTOR_COUNT,
    CONTINUOUS_FEATURES,
    FEATURES,
    MARKET_FEATURES,
    V14_FEATURES,
    V16_CONTRACT_VERSION,
    V16_SCORE_FEATURE,
    REASON_FEATURE,
    REASON_SCORE,
    RotationL1G2AError,
    _contribution_receipt,
    _eligible_rows,
    _market_raw_features,
    _lightgbm_profile,
    _prepared_market_component,
    _v16_scoring_contract,
    _with_market_signs,
    build_label_free_feature_panel,
    build_v16_single_date_feature_frame,
    canonical_sha256,
    causal_states,
    close_processes,
    cross_section_rank_features,
    project_states,
)

REASON_WRITER = "hmm_risk_rotation_prediction_write_failed"
REASON_READBACK = "hmm_risk_rotation_prediction_readback_mismatch"
REASON_CONFLICT = "hmm_risk_rotation_prediction_identity_conflict"
REASON_NOT_FOUND = "hmm_risk_rotation_prediction_not_found"
REASON_MODEL_AMBIGUOUS = "hmm_risk_rotation_prediction_model_ambiguous"
REASON_INFERENCE = "hmm_risk_rotation_single_date_inference_failed"
PREDICTION_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "aistock:hmm-risk:rotation-l1-prediction:v1")

PREDICTION_COLUMNS = (
    "prediction_id",
    "product_bundle_id",
    "trade_date",
    "as_of_date",
    "sector_level",
    "sector_code",
    "sector_name",
    "rotation_score",
    "forecast_state",
    "feature_contributions",
    "availability",
    "reason_code",
    "research_surface_status",
    "rotation_l1_capability_status",
    "forward_power_status",
    "forward_confirmation",
    "advisory_status",
    "validation_basis",
    "development_oof_rank_ic",
    "development_oof_rank_ic_hac_lower",
    "development_oof_rank_ic_hac_upper",
    "model_hash",
    "input_hash",
    "mapping_snapshot_hash",
    "tail_accessed",
    "revision",
    "supersedes_prediction_id",
)

_INSERT_SQL = f"""
INSERT INTO hmm_risk.rotation_l1_prediction ({",".join(PREDICTION_COLUMNS)})
VALUES ({",".join(["%s"] * len(PREDICTION_COLUMNS))})
ON CONFLICT (model_hash,trade_date,sector_code,revision) DO NOTHING
"""
_READ_ONE_SQL = f"""
SELECT {",".join(PREDICTION_COLUMNS)}
FROM hmm_risk.rotation_l1_prediction
WHERE model_hash=%s AND trade_date=%s AND sector_code=%s AND revision=%s
"""


class RotationL1PredictionError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _json_value(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    return value


def _validate_v16_model_identity(
    *,
    model_text: str,
    final_model: Mapping[str, Any],
    model_profile: Mapping[str, Any],
) -> str:
    expected_contract = _v16_scoring_contract()
    expected_text = canonical_json_bytes(expected_contract).decode("utf-8")
    expected_hash = canonical_sha256(expected_contract)
    expected_model = {
        "model_kind": "deterministic_cross_section_rank",
        "model_sha256": expected_hash,
        "scoring_contract_sha256": expected_hash,
        "training_performed": False,
    }
    if (
        model_text != expected_text
        or dict(final_model) != expected_model
        or dict(model_profile) != {"model_kind": "deterministic_cross_section_rank", "fit_required": False}
    ):
        raise RotationL1PredictionError(REASON_INFERENCE, "v1.6 deterministic model identity differs")
    return expected_hash


def _row_identity_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {column: _json_value(row.get(column)) for column in PREDICTION_COLUMNS if column != "prediction_id"}


def _prediction_id(row: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        _row_identity_payload(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return str(uuid.uuid5(PREDICTION_ID_NAMESPACE, canonical))


def _canonical_uuid(value: Any, *, field: str, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    try:
        parsed = uuid.UUID(str(value))
    except (AttributeError, TypeError, ValueError) as exc:
        raise RotationL1PredictionError(REASON_WRITER, f"rotation prediction {field} is invalid") from exc
    return str(parsed)


def _validate_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    if set(raw) != set(PREDICTION_COLUMNS):
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction columns differ")
    row = dict(raw)
    row["prediction_id"] = _canonical_uuid(row["prediction_id"], field="prediction_id")
    row["supersedes_prediction_id"] = _canonical_uuid(
        row["supersedes_prediction_id"], field="supersedes_prediction_id", optional=True
    )
    if not isinstance(row["trade_date"], date) or not isinstance(row["as_of_date"], date):
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction dates are invalid")
    if row["as_of_date"] >= row["trade_date"] or row["sector_level"] != "L1":
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction date/level contract differs")
    if not all(isinstance(row[field], str) and row[field].strip() for field in ("sector_code", "sector_name")):
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction sector identity is invalid")
    if not all(_is_sha256(row[field]) for field in ("model_hash", "input_hash", "mapping_snapshot_hash")):
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction hash identity is invalid")
    enums = {
        "research_surface_status": {"NOT_AVAILABLE", "AVAILABLE_EXPERIMENTAL"},
        "rotation_l1_capability_status": {
            "NOT_AVAILABLE",
            "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
            "ADVISORY_PREDICTION_AVAILABLE",
        },
        "forward_power_status": {"UNAVAILABLE", "INSUFFICIENT", "SUFFICIENT"},
        "forward_confirmation": {
            "NOT_STARTED",
            "PENDING_INSUFFICIENT_POWER",
            "PENDING_INCONCLUSIVE",
            "PASSED",
            "FAILED",
        },
        "advisory_status": {"NOT_AVAILABLE", "AVAILABLE"},
        "validation_basis": {"development_causal_oof", "single_date_frozen_model"},
    }
    if any(row[field] not in accepted for field, accepted in enums.items()):
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction state contract differs")
    if row["research_surface_status"] != "NOT_AVAILABLE":
        raise RotationL1PredictionError(
            REASON_WRITER,
            "rotation prediction persisted surface status must be derived from product readback",
        )
    if row["advisory_status"] == "AVAILABLE" and (
        row["rotation_l1_capability_status"] != "ADVISORY_PREDICTION_AVAILABLE"
        or row["forward_confirmation"] != "PASSED"
        or row["product_bundle_id"] is None
        or row["tail_accessed"] is not True
    ):
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction advisory coupling differs")
    if row["product_bundle_id"] is not None and (
        not isinstance(row["product_bundle_id"], str) or not row["product_bundle_id"].strip()
    ):
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction product bundle is invalid")
    if not isinstance(row["tail_accessed"], bool):
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction tail state is invalid")
    metrics = (
        row["development_oof_rank_ic"],
        row["development_oof_rank_ic_hac_lower"],
        row["development_oof_rank_ic_hac_upper"],
    )
    if any(
        value is None
        or not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
        for value in metrics
    ) or not (float(metrics[1]) <= float(metrics[0]) <= float(metrics[2])):
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction development metrics are invalid")
    if not isinstance(row["revision"], int) or isinstance(row["revision"], bool) or row["revision"] < 1:
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction revision is invalid")
    if (row["revision"] == 1) != (row["supersedes_prediction_id"] is None):
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction supersedes chain differs")
    if row["availability"] == "available":
        score = row["rotation_score"]
        contributions = row["feature_contributions"]
        if (
            not isinstance(score, (int, float))
            or isinstance(score, bool)
            or not math.isfinite(float(score))
            or row["forecast_state"] not in {"fading", "neutral", "trending"}
            or not isinstance(contributions, list)
            or len(contributions) not in {len(FEATURES) + 1, len(V14_FEATURES) + 1}
            or not all(isinstance(value, (int, float)) and math.isfinite(float(value)) for value in contributions)
            or row["reason_code"] is not None
        ):
            raise RotationL1PredictionError(REASON_WRITER, "available rotation prediction payload is invalid")
        row["rotation_score"] = float(score)
        row["feature_contributions"] = [float(value) for value in contributions]
    elif row["availability"] == "unavailable":
        if (
            row["rotation_score"] is not None
            or row["forecast_state"] is not None
            or row["feature_contributions"] is not None
            or not isinstance(row["reason_code"], str)
            or not row["reason_code"].strip()
        ):
            raise RotationL1PredictionError(REASON_WRITER, "unavailable rotation prediction payload is invalid")
    else:
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction availability is invalid")
    expected_id = _prediction_id(row)
    if row["prediction_id"] != expected_id:
        raise RotationL1PredictionError(REASON_WRITER, "rotation prediction id does not match payload")
    return row


def _make_row(**values: Any) -> dict[str, Any]:
    row = {column: values.get(column) for column in PREDICTION_COLUMNS}
    row["prediction_id"] = _prediction_id(row)
    return _validate_row(row)


def build_oof_prediction_rows(
    *,
    acceptance: Mapping[str, Any],
    process_reports: Sequence[Mapping[str, Any]],
    sector_names: Mapping[str, str],
    v14_reference: Mapping[str, Any] | None = None,
    input_bundle: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Translate verified OOF output into the only product-row contract."""

    if len(process_reports) != 2:
        raise RotationL1PredictionError(REASON_WRITER, "G2-A product requires two fresh-process reports")
    process_report = process_reports[0]
    try:
        recomputed_acceptance = close_processes(
            process_reports[0],
            process_reports[1],
            v14_reference=v14_reference,
            input_bundle=input_bundle,
        )
    except RotationL1G2AError as exc:
        raise RotationL1PredictionError(exc.reason_code, str(exc)) from exc
    if dict(recomputed_acceptance) != dict(acceptance):
        raise RotationL1PredictionError(REASON_WRITER, "G2-A product acceptance does not match fresh processes")
    payload = process_report.get("reproducibility_payload")
    if not isinstance(payload, Mapping):
        raise RotationL1PredictionError(REASON_WRITER, "G2-A process payload is missing")
    acceptance_body = {key: value for key, value in acceptance.items() if key != "acceptance_sha256"}
    process_body = {key: value for key, value in process_report.items() if key != "report_sha256"}
    model_text = process_report.get("final_model_text")
    if (
        acceptance.get("acceptance_sha256") != canonical_sha256(acceptance_body)
        or process_report.get("report_sha256") != canonical_sha256(process_body)
        or process_report.get("reproducibility_payload_sha256") != canonical_sha256(payload)
        or acceptance.get("reproducibility_payload_sha256") != process_report.get("reproducibility_payload_sha256")
        or acceptance.get("research_surface_status") != "NOT_AVAILABLE"
        or acceptance.get("research_product_gate_passed") is not False
        or payload.get("research_product_gate") != {"passed": True, "effect_threshold_applied": False}
        or payload.get("oof_prediction_rows_sha256") != canonical_sha256(payload.get("oof_prediction_rows"))
    ):
        raise RotationL1PredictionError(REASON_WRITER, "offline/product boundary receipt differs")
    source_rows = payload.get("oof_prediction_rows")
    final_model = payload.get("final_model")
    development = payload.get("development_summary")
    input_identity = payload.get("input_identity")
    if not all(isinstance(item, Mapping) for item in (final_model, development, input_identity)) or not isinstance(
        source_rows, list
    ):
        raise RotationL1PredictionError(REASON_WRITER, "G2-A product evidence is incomplete")
    final_model_hash = final_model.get("model_sha256")
    mapping_hash = input_identity.get("mapping_sha256")
    input_hash = canonical_sha256(input_identity)
    if not all(_is_sha256(item) for item in (final_model_hash, mapping_hash, input_hash)):
        raise RotationL1PredictionError(REASON_WRITER, "G2-A product lineage is invalid")
    if payload.get("contract_version") == V16_CONTRACT_VERSION:
        try:
            validated_hash = _validate_v16_model_identity(
                model_text=model_text,
                final_model=final_model,
                model_profile=payload.get("profile") if isinstance(payload.get("profile"), Mapping) else {},
            )
        except RotationL1PredictionError as exc:
            raise RotationL1PredictionError(REASON_WRITER, "G2-A v1.6 frozen model readback differs") from exc
        if (
            validated_hash != final_model_hash
            or acceptance.get("contract_version") != V16_CONTRACT_VERSION
            or acceptance.get("scoring_contract_sha256") != validated_hash
            or payload.get("scoring_contract_sha256") != validated_hash
        ):
            raise RotationL1PredictionError(REASON_WRITER, "G2-A v1.6 scoring authority differs")
    elif not isinstance(model_text, str) or final_model_hash != canonical_sha256(model_text):
        raise RotationL1PredictionError(REASON_WRITER, "G2-A frozen model readback differs")
    fold_model_hashes = {
        str(fold.get("model_sha256")) for fold in payload.get("folds", []) if isinstance(fold, Mapping)
    }
    if len(sector_names) != CANONICAL_SECTOR_COUNT or not all(
        isinstance(code, str) and isinstance(name, str) and name.strip() for code, name in sector_names.items()
    ):
        raise RotationL1PredictionError(REASON_WRITER, "canonical L1 sector names are incomplete")
    tail_allowed = bool(payload.get("tail_access_gate", {}).get("passed"))
    power = str(payload.get("forward_power_status"))
    capability = "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED" if tail_allowed else "NOT_AVAILABLE"
    confirmation = (
        "PENDING_INSUFFICIENT_POWER"
        if tail_allowed and power == "INSUFFICIENT"
        else ("PENDING_INCONCLUSIVE" if tail_allowed else "NOT_STARTED")
    )
    rows: list[dict[str, Any]] = []
    seen: set[tuple[date, str]] = set()
    for source in source_rows:
        if not isinstance(source, Mapping):
            raise RotationL1PredictionError(REASON_WRITER, "G2-A OOF row is invalid")
        try:
            trade_date = date.fromisoformat(str(source["trade_date"]))
            as_of_date = date.fromisoformat(str(source["as_of_date"]))
        except (KeyError, ValueError) as exc:
            raise RotationL1PredictionError(REASON_WRITER, "G2-A OOF date identity is invalid") from exc
        code = str(source.get("sector_code") or "")
        model_hash = source.get("model_hash")
        identity = (trade_date, code)
        if (
            identity in seen
            or code not in sector_names
            or as_of_date >= trade_date
            or not _is_sha256(model_hash)
            or model_hash not in fold_model_hashes
        ):
            raise RotationL1PredictionError(REASON_WRITER, "G2-A OOF identity is incomplete or duplicated")
        seen.add(identity)
        rows.append(
            _make_row(
                product_bundle_id=None,
                trade_date=trade_date,
                as_of_date=as_of_date,
                sector_level="L1",
                sector_code=code,
                sector_name=sector_names[code],
                rotation_score=source.get("rotation_score"),
                forecast_state=source.get("forecast_state"),
                feature_contributions=source.get("feature_contributions"),
                availability=source.get("availability"),
                reason_code=source.get("reason_code"),
                research_surface_status="NOT_AVAILABLE",
                rotation_l1_capability_status=capability,
                forward_power_status=power,
                forward_confirmation=confirmation,
                advisory_status="NOT_AVAILABLE",
                validation_basis="development_causal_oof",
                development_oof_rank_ic=float(development["mean_rank_ic"]),
                development_oof_rank_ic_hac_lower=float(development["hac_lower_two_sided_95pct"]),
                development_oof_rank_ic_hac_upper=float(development["hac_upper_two_sided_95pct"]),
                model_hash=model_hash,
                input_hash=input_hash,
                mapping_snapshot_hash=mapping_hash,
                tail_accessed=False,
                revision=1,
                supersedes_prediction_id=None,
            )
        )
    counts = (
        pd.Series([row["sector_code"] for row in rows], index=[row["trade_date"] for row in rows])
        .groupby(level=0)
        .nunique()
    )
    if counts.empty or not counts.eq(CANONICAL_SECTOR_COUNT).all():
        raise RotationL1PredictionError(REASON_WRITER, "G2-A OOF daily sector denominator differs")
    return sorted(rows, key=lambda row: (row["trade_date"], row["sector_code"]))


def _load_surface_receipt(path: Path | None) -> Mapping[str, Any] | None:
    if path is None:
        return None
    if not path.is_absolute() or path.is_symlink():
        raise RotationL1PredictionError(REASON_READBACK, "rotation product validation receipt path is indirect")
    try:
        resolved = path.resolve(strict=True)
        if not resolved.is_file():
            raise OSError("not a regular file")
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RotationL1PredictionError(REASON_READBACK, "rotation product validation receipt cannot be read") from exc
    if not isinstance(raw, Mapping):
        raise RotationL1PredictionError(REASON_READBACK, "rotation product validation receipt is not an object")
    return raw


def _surface_status_from_receipt(rows: Sequence[Mapping[str, Any]], *, receipt: Mapping[str, Any] | None) -> str:
    if receipt is None:
        return "NOT_AVAILABLE"
    expected_keys = {
        "schema_version",
        "model_hash",
        "trade_date",
        "repository_readback_passed",
        "api_readback_passed",
        "ui_readback_passed",
        "mock_used",
        "row_count",
        "input_row_sha256",
        "receipt_sha256",
    }
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    validated = [_validate_row(row) for row in rows]
    if (
        set(receipt) != expected_keys
        or not validated
        or receipt.get("schema_version") != "hmm_risk_rotation_l1_product_validation_v1"
        or receipt.get("model_hash") != validated[0]["model_hash"]
        or receipt.get("trade_date") != validated[0]["trade_date"].isoformat()
        or receipt.get("repository_readback_passed") is not True
        or receipt.get("api_readback_passed") is not True
        or receipt.get("ui_readback_passed") is not True
        or receipt.get("mock_used") is not False
        or receipt.get("row_count") != len(validated)
        or receipt.get("input_row_sha256") != canonical_sha256([_row_identity_payload(row) for row in validated])
        or receipt.get("receipt_sha256") != canonical_sha256(body)
    ):
        raise RotationL1PredictionError(REASON_READBACK, "rotation product validation receipt differs")
    return "AVAILABLE_EXPERIMENTAL"


def _validate_write_batch(rows: Sequence[Mapping[str, Any]]) -> None:
    groups: dict[tuple[str, date, int], list[Mapping[str, Any]]] = {}
    for row in rows:
        key = (str(row["model_hash"]), row["trade_date"], int(row["revision"]))
        groups.setdefault(key, []).append(row)
    invariant_fields = (
        "as_of_date",
        "research_surface_status",
        "rotation_l1_capability_status",
        "forward_power_status",
        "forward_confirmation",
        "advisory_status",
        "validation_basis",
        "development_oof_rank_ic",
        "development_oof_rank_ic_hac_lower",
        "development_oof_rank_ic_hac_upper",
        "input_hash",
        "mapping_snapshot_hash",
        "tail_accessed",
        "product_bundle_id",
    )
    for group in groups.values():
        head = group[0]
        if (
            len(group) != CANONICAL_SECTOR_COUNT
            or len({row["sector_code"] for row in group}) != CANONICAL_SECTOR_COUNT
            or any(row[field] != head[field] for row in group[1:] for field in invariant_fields)
        ):
            raise RotationL1PredictionError(
                REASON_CONFLICT,
                "rotation prediction write batch does not contain one consistent 31-sector cross-section",
            )


def build_single_date_raw_features(
    *,
    calendar: Sequence[date],
    sector_close: Mapping[tuple[date, str], float],
    benchmark_close: Mapping[date, float],
    stock_daily_inputs: Sequence[Mapping[str, Any]],
    trade_date: date,
) -> pd.DataFrame:
    """Reuse the formal G2-A feature builder without a future outcome window."""

    ordered = tuple(calendar)
    source_calendar = tuple(ordered[:-1])
    source_calendar_set = set(source_calendar)
    sector_codes = tuple(sorted({str(sector) for _day, sector in sector_close}))
    if (
        len(ordered) != 62
        or ordered[-1] != trade_date
        or ordered != tuple(sorted(set(ordered)))
        or len(sector_codes) != CANONICAL_SECTOR_COUNT
        or set(benchmark_close) != source_calendar_set
        or set(sector_close) != {(day, sector) for day in source_calendar for sector in sector_codes}
        or {(row.get("source_date"), str(row.get("sector_code"))) for row in stock_daily_inputs}
        != {(day, sector) for day in source_calendar[-20:] for sector in sector_codes}
    ):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date label-free source boundary differs")
    try:
        panel = build_label_free_feature_panel(
            calendar=ordered,
            sector_close=sector_close,
            benchmark_close=benchmark_close,
            stock_daily_inputs=stock_daily_inputs,
        )
    except RotationL1G2AError as exc:
        raise RotationL1PredictionError(exc.reason_code, str(exc)) from exc
    selected = panel.loc[(slice(trade_date, trade_date), slice(None)), list(CONTINUOUS_FEATURES)].copy()
    if len(selected) != CANONICAL_SECTOR_COUNT:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date feature denominator differs")
    return selected


def _validate_single_date_authority(
    *,
    model_text: str,
    final_model: Mapping[str, Any],
    model_profile: Mapping[str, Any],
    development_summary: Mapping[str, Any],
    capability_status: str,
    forward_power_status: str,
    forward_confirmation: str,
    product_bundle_id: str | None,
) -> tuple[Mapping[str, Any], tuple[float, float, float]]:
    if (
        capability_status
        not in {
            "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
            "ADVISORY_PREDICTION_AVAILABLE",
        }
        or forward_confirmation == "FAILED"
    ):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date capability does not permit new inference")
    if capability_status == "ADVISORY_PREDICTION_AVAILABLE":
        if forward_confirmation != "PASSED" or not isinstance(product_bundle_id, str) or not product_bundle_id.strip():
            raise RotationL1PredictionError(REASON_INFERENCE, "single-date advisory authority is incomplete")
    elif forward_confirmation not in {"PENDING_INSUFFICIENT_POWER", "PENDING_INCONCLUSIVE"}:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date research confirmation state differs")
    try:
        development_values = tuple(
            float(development_summary[field])
            for field in ("mean_rank_ic", "hac_lower_two_sided_95pct", "hac_upper_two_sided_95pct")
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date development evidence is invalid") from exc
    if not all(math.isfinite(value) for value in development_values):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date development evidence is non-finite")
    if development_values[0] < BINDING_MBE_IC:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date development effect is below binding MBE")
    if forward_power_status not in {"UNAVAILABLE", "INSUFFICIENT", "SUFFICIENT"}:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date forward power status differs")
    if capability_status == "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED" and (
        (forward_confirmation == "PENDING_INSUFFICIENT_POWER" and forward_power_status != "INSUFFICIENT")
        or (forward_confirmation == "PENDING_INCONCLUSIVE" and forward_power_status == "INSUFFICIENT")
    ):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date forward state coupling differs")
    deterministic_v16 = (
        final_model.get("model_kind") == "deterministic_cross_section_rank"
        or model_profile.get("model_kind") == "deterministic_cross_section_rank"
    )
    if deterministic_v16:
        model_hash = _validate_v16_model_identity(
            model_text=model_text,
            final_model=final_model,
            model_profile=model_profile,
        )
        return {
            "contract_version": V16_CONTRACT_VERSION,
            "model_kind": "deterministic_cross_section_rank",
            "model_sha256": model_hash,
        }, (development_values[0], development_values[1], development_values[2])
    if (
        not isinstance(model_text, str)
        or not model_text
        or final_model.get("model_sha256") != canonical_sha256(model_text)
        or dict(model_profile) != _lightgbm_profile()
    ):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date frozen model identity differs")
    context = final_model.get("market_context")
    if not isinstance(context, Mapping):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date market checkpoint is missing")
    context_body = {key: value for key, value in context.items() if key != "receipt_sha256"}
    if context.get("receipt_sha256") != canonical_sha256(context_body):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date market checkpoint hash differs")
    return context, (development_values[0], development_values[1], development_values[2])


def predict_single_date_from_assets(
    *,
    direct_v2_candidate_root: Path,
    security_identity_manifest: Path,
    provider_absence_manifest: Path,
    industry_authority: Mapping[str, Any],
    forbidden_roots: Sequence[Path],
    work_parent: Path,
    trade_date: date,
    as_of_date: date,
    model_text: str,
    final_model: Mapping[str, Any],
    model_profile: Mapping[str, Any],
    development_summary: Mapping[str, Any],
    capability_status: str,
    forward_power_status: str,
    forward_confirmation: str,
    product_bundle_id: str | None = None,
    booster_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Run the controlled zero-fit entry from one explicit direct-v2 release."""

    context, _development_values = _validate_single_date_authority(
        model_text=model_text,
        final_model=final_model,
        model_profile=model_profile,
        development_summary=development_summary,
        capability_status=capability_status,
        forward_power_status=forward_power_status,
        forward_confirmation=forward_confirmation,
        product_bundle_id=product_bundle_id,
    )
    deterministic_v16 = context.get("contract_version") == V16_CONTRACT_VERSION
    if deterministic_v16:
        market_start = None
    else:
        try:
            market_start = date.fromisoformat(str(context["train_start"]))
        except (KeyError, ValueError) as exc:
            raise RotationL1PredictionError(REASON_INFERENCE, "single-date market checkpoint start is invalid") from exc

    from backend.services.hmm_risk.rotation_l1_input_bundle import (
        RotationL1InputBundleError,
        build_rotation_l1_single_date_source_from_assets,
    )

    try:
        source = build_rotation_l1_single_date_source_from_assets(
            direct_v2_candidate_root=direct_v2_candidate_root,
            security_identity_manifest=security_identity_manifest,
            provider_absence_manifest=provider_absence_manifest,
            industry_authority=industry_authority,
            forbidden_roots=forbidden_roots,
            work_parent=work_parent,
            trade_date=trade_date,
            as_of_date=as_of_date,
            market_start=market_start,
            model_contract_version=(
                V16_CONTRACT_VERSION
                if deterministic_v16
                else str(final_model.get("contract_version") or "hmm_risk_rotation_l1_g2a_v1_3")
            ),
        )
    except RotationL1InputBundleError as exc:
        raise RotationL1PredictionError(exc.reason_code, str(exc), context=exc.context) from exc
    source_receipt = source.get("source_receipt")
    if not isinstance(source_receipt, Mapping):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date source receipt is missing")
    source_body = {key: value for key, value in source_receipt.items() if key != "receipt_sha256"}
    source_sha256 = canonical_sha256(source_body)
    expected_source_schema = (
        "hmm_risk_rotation_l1_single_date_source_v2"
        if deterministic_v16
        else "hmm_risk_rotation_l1_single_date_source_v1"
    )
    if (
        source.get("schema_version") != expected_source_schema
        or source_receipt.get("schema_version") != expected_source_schema
        or source_receipt.get("receipt_sha256") != source_sha256
        or source.get("input_hash") != source_sha256
        or source_receipt.get("mapping_snapshot_sha256") != source.get("mapping_snapshot_hash")
        or source_receipt.get("trade_date") != trade_date.isoformat()
        or source_receipt.get("as_of_date") != as_of_date.isoformat()
        or source_receipt.get("target_columns_read") is not False
    ):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date source receipt differs")
    if deterministic_v16:
        if (
            source.get("model_contract_version") != V16_CONTRACT_VERSION
            or source_receipt.get("model_contract_version") != V16_CONTRACT_VERSION
            or source_receipt.get("market_context_used_for_score") is not False
            or source_receipt.get("sector_close_used_for_score") is not False
        ):
            raise RotationL1PredictionError(REASON_INFERENCE, "single-date v1.6 source authority differs")
        raw_features = build_v16_single_date_feature_frame(
            calendar=source["feature_calendar"],
            stock_daily_inputs=source["stock_daily_inputs"],
            trade_date=trade_date,
        )
    else:
        feature_benchmark = {day: source["benchmark_close"][day] for day in source["feature_calendar"][:-1]}
        raw_features = build_single_date_raw_features(
            calendar=source["feature_calendar"],
            sector_close=source["sector_close"],
            benchmark_close=feature_benchmark,
            stock_daily_inputs=source["stock_daily_inputs"],
            trade_date=trade_date,
        )
    rows = predict_single_date_rows(
        model_text=model_text,
        final_model=final_model,
        model_profile=model_profile,
        raw_features=raw_features,
        benchmark_close=source.get("benchmark_close", {}),
        calendar=source["feature_calendar"] if deterministic_v16 else source["market_calendar"],
        trade_date=trade_date,
        as_of_date=as_of_date,
        sector_names=source["sector_names"],
        input_hash=source["input_hash"],
        mapping_snapshot_hash=source["mapping_snapshot_hash"],
        development_summary=development_summary,
        capability_status=capability_status,
        forward_power_status=forward_power_status,
        forward_confirmation=forward_confirmation,
        product_bundle_id=product_bundle_id,
        booster_factory=booster_factory,
    )
    return {"rows": rows, "source_receipt": source_receipt}


@dataclass
class RotationL1PredictionRepository:
    conn_factory: Callable[[], AbstractContextManager[Any]] = lambda: get_conn(
        autocommit=False, manage_transaction=True
    )
    surface_validation_receipt_path: Path | None = None

    def write_rows(self, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        validated = [_validate_row(row) for row in rows]
        if not validated:
            raise RotationL1PredictionError(REASON_WRITER, "rotation prediction write is empty")
        _validate_write_batch(validated)
        keys = [(row["model_hash"], row["trade_date"], row["sector_code"], row["revision"]) for row in validated]
        if len(keys) != len(set(keys)):
            raise RotationL1PredictionError(REASON_CONFLICT, "rotation prediction write contains duplicate keys")
        with self.conn_factory() as conn:
            with conn.cursor() as cursor:
                for row in validated:
                    values = [
                        json.dumps(row[column], ensure_ascii=False, separators=(",", ":"), allow_nan=False)
                        if column == "feature_contributions" and row[column] is not None
                        else row[column]
                        for column in PREDICTION_COLUMNS
                    ]
                    cursor.execute(_INSERT_SQL, values)
                readback: list[dict[str, Any]] = []
                for row in validated:
                    cursor.execute(
                        _READ_ONE_SQL,
                        (row["model_hash"], row["trade_date"], row["sector_code"], row["revision"]),
                    )
                    raw = cursor.fetchone()
                    if raw is None:
                        raise RotationL1PredictionError(
                            REASON_READBACK, "rotation prediction row is missing after write"
                        )
                    stored = dict(zip(PREDICTION_COLUMNS, raw, strict=True))
                    for field in ("model_hash", "input_hash", "mapping_snapshot_hash"):
                        stored[field] = str(stored[field]).strip()
                    if isinstance(stored["feature_contributions"], str):
                        stored["feature_contributions"] = json.loads(stored["feature_contributions"])
                    readback.append(_validate_row(stored))
            expected_hash = canonical_sha256([_row_identity_payload(row) for row in validated])
            actual_hash = canonical_sha256([_row_identity_payload(row) for row in readback])
            if expected_hash != actual_hash:
                raise RotationL1PredictionError(REASON_READBACK, "rotation prediction canonical readback differs")
        return {"row_count": len(readback), "canonical_row_sha256": actual_hash, "idempotency_verified": True}

    def _resolve_model_hash(self, cursor: Any, explicit: str | None, *, trade_date: date | None = None) -> str:
        if explicit is not None:
            if not _is_sha256(explicit):
                raise RotationL1PredictionError(REASON_MODEL_AMBIGUOUS, "explicit model hash is invalid")
            return explicit
        if trade_date is None:
            cursor.execute("SELECT DISTINCT model_hash FROM hmm_risk.rotation_l1_prediction ORDER BY model_hash")
        else:
            cursor.execute(
                "SELECT DISTINCT model_hash FROM hmm_risk.rotation_l1_prediction WHERE trade_date=%s ORDER BY model_hash",
                (trade_date,),
            )
        hashes = [str(row[0]).strip() for row in cursor.fetchall()]
        if len(hashes) != 1:
            raise RotationL1PredictionError(
                REASON_NOT_FOUND if not hashes else REASON_MODEL_AMBIGUOUS,
                "rotation prediction model identity is not uniquely bound",
                context={"model_count": len(hashes)},
            )
        return hashes[0]

    def read_date(self, trade_date: date, *, model_hash: str | None = None) -> dict[str, Any]:
        with self.conn_factory() as conn:
            with conn.cursor() as cursor:
                selected_model = self._resolve_model_hash(cursor, model_hash, trade_date=trade_date)
                cursor.execute(
                    f"""
                    SELECT {",".join("p." + column for column in PREDICTION_COLUMNS)}
                    FROM hmm_risk.rotation_l1_prediction p
                    WHERE p.model_hash=%s AND p.trade_date=%s
                      AND p.revision=(
                        SELECT max(newer.revision) FROM hmm_risk.rotation_l1_prediction newer
                        WHERE newer.model_hash=p.model_hash AND newer.trade_date=p.trade_date
                          AND newer.sector_code=p.sector_code
                      )
                    ORDER BY p.sector_code
                    """,
                    (selected_model, trade_date),
                )
                raw_rows = cursor.fetchall()
        if not raw_rows:
            raise RotationL1PredictionError(REASON_NOT_FOUND, "rotation prediction trade date is not found")
        rows = [dict(zip(PREDICTION_COLUMNS, raw, strict=True)) for raw in raw_rows]
        for row in rows:
            for field in ("model_hash", "input_hash", "mapping_snapshot_hash"):
                row[field] = str(row[field]).strip()
            if isinstance(row["feature_contributions"], str):
                row["feature_contributions"] = json.loads(row["feature_contributions"])
            _validate_row(row)
        if (
            len(rows) != CANONICAL_SECTOR_COUNT
            or len({row["sector_code"] for row in rows}) != CANONICAL_SECTOR_COUNT
            or any(row["model_hash"] != selected_model or row["trade_date"] != trade_date for row in rows)
        ):
            raise RotationL1PredictionError(REASON_READBACK, "rotation prediction date does not contain 31 sectors")
        return {"model_hash": selected_model, "trade_date": trade_date.isoformat(), "rows": rows}

    def overview(self, *, model_hash: str | None = None) -> dict[str, Any]:
        surface_receipt = _load_surface_receipt(self.surface_validation_receipt_path)
        receipt_model_hash = surface_receipt.get("model_hash") if surface_receipt is not None else None
        if model_hash is None and receipt_model_hash is not None:
            if not _is_sha256(receipt_model_hash):
                raise RotationL1PredictionError(
                    REASON_READBACK, "rotation product validation model identity is invalid"
                )
            model_hash = str(receipt_model_hash)
        with self.conn_factory() as conn:
            with conn.cursor() as cursor:
                if model_hash is None:
                    cursor.execute("SELECT max(trade_date) FROM hmm_risk.rotation_l1_prediction")
                    raw = cursor.fetchone()
                    if not raw or raw[0] is None:
                        raise RotationL1PredictionError(REASON_NOT_FOUND, "rotation prediction overview is not found")
                    selected_model = self._resolve_model_hash(cursor, None, trade_date=raw[0])
                else:
                    selected_model = self._resolve_model_hash(cursor, model_hash)
                    cursor.execute(
                        "SELECT max(trade_date) FROM hmm_risk.rotation_l1_prediction WHERE model_hash=%s",
                        (selected_model,),
                    )
                    raw = cursor.fetchone()
        if not raw or raw[0] is None:
            raise RotationL1PredictionError(REASON_NOT_FOUND, "rotation prediction overview is not found")
        detail = self.read_date(raw[0], model_hash=selected_model)
        rows = detail["rows"]
        head = rows[0]
        invariant_fields = (
            "as_of_date",
            "research_surface_status",
            "rotation_l1_capability_status",
            "forward_power_status",
            "forward_confirmation",
            "advisory_status",
            "validation_basis",
            "development_oof_rank_ic",
            "development_oof_rank_ic_hac_lower",
            "development_oof_rank_ic_hac_upper",
            "input_hash",
            "mapping_snapshot_hash",
            "tail_accessed",
        )
        if any(row[field] != head[field] for row in rows[1:] for field in invariant_fields):
            raise RotationL1PredictionError(REASON_READBACK, "rotation prediction overview state is inconsistent")
        surface_status = _surface_status_from_receipt(
            rows,
            receipt=surface_receipt,
        )
        return {
            "model_hash": selected_model,
            "trade_date": detail["trade_date"],
            "as_of_date": head["as_of_date"].isoformat(),
            "sector_count": len(rows),
            "available_count": sum(row["availability"] == "available" for row in rows),
            "binding_mbe_rank_ic": BINDING_MBE_IC,
            "research_surface_status": surface_status,
            **{
                field: head[field]
                for field in (
                    "rotation_l1_capability_status",
                    "forward_power_status",
                    "forward_confirmation",
                    "advisory_status",
                    "validation_basis",
                    "development_oof_rank_ic",
                    "development_oof_rank_ic_hac_lower",
                    "development_oof_rank_ic_hac_upper",
                    "input_hash",
                    "mapping_snapshot_hash",
                    "tail_accessed",
                )
            },
        }


def _predict_v16_single_date_rows(
    *,
    raw_features: pd.DataFrame,
    trade_date: date,
    sector_names: Mapping[str, str],
    input_hash: str,
    mapping_snapshot_hash: str,
    development_values: tuple[float, float, float],
    capability_status: str,
    forward_power_status: str,
    forward_confirmation: str,
    model_hash: str,
    as_of_date: date,
) -> list[dict[str, Any]]:
    reason_column = f"reason__{V16_SCORE_FEATURE}"
    if (
        not isinstance(raw_features.index, pd.MultiIndex)
        or tuple(raw_features.index.names) != ("trade_date", "sector_code")
        or raw_features.index.has_duplicates
        or set(raw_features.columns) != {V16_SCORE_FEATURE, reason_column}
        or set(raw_features.index.get_level_values("trade_date")) != {trade_date}
        or len(raw_features) != CANONICAL_SECTOR_COUNT
        or set(raw_features.index.get_level_values("sector_code")) != set(sector_names)
    ):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date v1.6 feature identity differs")
    try:
        numeric = pd.to_numeric(raw_features[V16_SCORE_FEATURE], errors="raise").astype(np.float64)
    except (TypeError, ValueError) as exc:
        raise RotationL1PredictionError(REASON_FEATURE, "single-date v1.6 feature value is invalid") from exc
    frame = raw_features.copy()
    frame[V16_SCORE_FEATURE] = numeric
    ranked = cross_section_rank_features(frame, continuous_features=(V16_SCORE_FEATURE,))
    scores = ranked[V16_SCORE_FEATURE].sort_index()
    available_count = int(np.isfinite(scores.to_numpy(dtype=np.float64)).sum())
    if available_count < 28:
        raise RotationL1PredictionError(REASON_FEATURE, "single-date v1.6 feature coverage is insufficient")
    try:
        states, _state_receipt = project_states(scores)
    except RotationL1G2AError as exc:
        raise RotationL1PredictionError(exc.reason_code, str(exc)) from exc

    contribution_index = V14_FEATURES.index(V16_SCORE_FEATURE)
    rows: list[dict[str, Any]] = []
    for identity, raw_score in scores.items():
        code = str(identity[1])
        score = float(raw_score)
        available = math.isfinite(score)
        contributions = None
        if available:
            contributions = [0.0] * (len(V14_FEATURES) + 1)
            contributions[contribution_index] = score
        raw_reason = frame.at[identity, reason_column]
        reason = (
            None if available else (str(raw_reason) if isinstance(raw_reason, str) and raw_reason else REASON_FEATURE)
        )
        rows.append(
            _make_row(
                product_bundle_id=None,
                trade_date=trade_date,
                as_of_date=as_of_date,
                sector_level="L1",
                sector_code=code,
                sector_name=sector_names[code],
                rotation_score=score if available else None,
                forecast_state=states.get((trade_date, code)) if available else None,
                feature_contributions=contributions,
                availability="available" if available else "unavailable",
                reason_code=reason,
                research_surface_status="NOT_AVAILABLE",
                rotation_l1_capability_status=capability_status,
                forward_power_status=forward_power_status,
                forward_confirmation=forward_confirmation,
                advisory_status="NOT_AVAILABLE",
                validation_basis="single_date_frozen_model",
                development_oof_rank_ic=development_values[0],
                development_oof_rank_ic_hac_lower=development_values[1],
                development_oof_rank_ic_hac_upper=development_values[2],
                model_hash=model_hash,
                input_hash=input_hash,
                mapping_snapshot_hash=mapping_snapshot_hash,
                tail_accessed=False,
                revision=1,
                supersedes_prediction_id=None,
            )
        )
    return rows


def predict_single_date_rows(
    *,
    model_text: str,
    final_model: Mapping[str, Any],
    model_profile: Mapping[str, Any],
    raw_features: pd.DataFrame,
    benchmark_close: Mapping[date, float],
    calendar: Sequence[date],
    trade_date: date,
    as_of_date: date,
    sector_names: Mapping[str, str],
    input_hash: str,
    mapping_snapshot_hash: str,
    development_summary: Mapping[str, Any],
    capability_status: str,
    forward_power_status: str,
    forward_confirmation: str,
    product_bundle_id: str | None = None,
    booster_factory: Callable[..., Any] | None = None,
) -> list[dict[str, Any]]:
    """Score one explicit date from a frozen model without fit/refit or labels."""

    ordered = tuple(calendar)
    if ordered != tuple(sorted(set(ordered))) or not ordered or ordered[-1] != trade_date:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date calendar identity differs")
    position = ordered.index(trade_date)
    if position == 0 or ordered[position - 1] != as_of_date:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date as-of is not the previous canonical session")
    if not _is_sha256(input_hash) or not _is_sha256(mapping_snapshot_hash):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date lineage is invalid")
    context, development_values = _validate_single_date_authority(
        model_text=model_text,
        final_model=final_model,
        model_profile=model_profile,
        development_summary=development_summary,
        capability_status=capability_status,
        forward_power_status=forward_power_status,
        forward_confirmation=forward_confirmation,
        product_bundle_id=product_bundle_id,
    )
    if context.get("contract_version") == V16_CONTRACT_VERSION:
        return _predict_v16_single_date_rows(
            raw_features=raw_features,
            trade_date=trade_date,
            sector_names=sector_names,
            input_hash=input_hash,
            mapping_snapshot_hash=mapping_snapshot_hash,
            development_values=development_values,
            capability_status=capability_status,
            forward_power_status=forward_power_status,
            forward_confirmation=forward_confirmation,
            model_hash=str(context["model_sha256"]),
            as_of_date=as_of_date,
        )
    try:
        market_start = date.fromisoformat(str(context["train_start"]))
        mean = np.asarray(context["mean"], dtype=np.float64)
        std = np.asarray(context["std"], dtype=np.float64)
        lower = np.asarray(context["lower"], dtype=np.float64)
        upper = np.asarray(context["upper"], dtype=np.float64)
        centers = np.asarray(context["centers"], dtype=np.float64)
        risk_on_state = int(context["risk_on_state"])
        jump_penalty = float(context["jump_penalty"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date market checkpoint is invalid") from exc
    if (
        mean.shape != (len(MARKET_FEATURES),)
        or std.shape != mean.shape
        or lower.shape != mean.shape
        or upper.shape != mean.shape
        or centers.shape != (2, len(MARKET_FEATURES))
        or not np.isfinite(mean).all()
        or not np.isfinite(std).all()
        or np.any(std <= 0)
        or not np.isfinite(lower).all()
        or not np.isfinite(upper).all()
        or np.any(lower > upper)
        or not np.isfinite(centers).all()
        or risk_on_state not in (0, 1)
        or jump_penalty != 4.0
    ):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date market checkpoint shape differs")
    apply_dates = tuple(day for day in ordered if market_start <= day <= trade_date)
    try:
        raw_market = _market_raw_features(benchmark_close, ordered)
    except (KeyError, TypeError, ValueError, ZeroDivisionError) as exc:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date benchmark history is invalid") from exc
    from backend.services.hmm_risk.rotation_l1_gbdt import Preprocessor

    preprocessor = Preprocessor(
        feature_names=MARKET_FEATURES,
        lower=tuple(float(value) for value in lower),
        upper=tuple(float(value) for value in upper),
        mean=tuple(float(value) for value in mean),
        std=tuple(float(value) for value in std),
        valid_row_count=int(context.get("train_count", 0)),
        valid_identity_sha256=str(context.get("train_date_sha256", "")),
    )
    component = _prepared_market_component(raw_market, dates=apply_dates, mean=mean, std=std, preprocessor=preprocessor)
    try:
        paths = causal_states(component, centers, jump_penalty)
    except Exception as exc:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date market causal recursion failed") from exc
    signs = {
        day: (1.0 if int(state) == risk_on_state else -1.0)
        for sequence, path in zip(component.sequences, paths, strict=True)
        for day, state in zip(sequence.dates, path, strict=True)
    }
    if trade_date not in signs:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date market state is unavailable")
    if not isinstance(raw_features.index, pd.MultiIndex) or tuple(raw_features.index.names) != (
        "trade_date",
        "sector_code",
    ):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date feature identity differs")
    if set(raw_features.columns) != set(CONTINUOUS_FEATURES) or set(
        raw_features.index.get_level_values("trade_date")
    ) != {trade_date}:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date feature payload contains extra data")
    try:
        frame = raw_features.loc[(slice(trade_date, trade_date), slice(None)), list(CONTINUOUS_FEATURES)].copy()
    except (KeyError, TypeError, ValueError) as exc:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date feature panel is incomplete") from exc
    if len(frame) != CANONICAL_SECTOR_COUNT or set(frame.index.get_level_values("sector_code")) != set(sector_names):
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date sector denominator differs")
    ranked = cross_section_rank_features(frame)
    prepared = _with_market_signs(ranked, {trade_date: signs[trade_date]})
    eligible = _eligible_rows(prepared, ridge=False)
    if int(eligible.sum()) < 28:
        raise RotationL1PredictionError(REASON_FEATURE, "single-date feature coverage is insufficient")
    if booster_factory is None:
        try:
            from lightgbm import Booster
        except (ImportError, OSError) as exc:
            raise RotationL1PredictionError(REASON_INFERENCE, "LightGBM 4.6.0 is unavailable") from exc
        if importlib.metadata.version("lightgbm") != "4.6.0":
            raise RotationL1PredictionError(REASON_INFERENCE, "LightGBM version differs from frozen model contract")
        booster_factory = Booster
    try:
        booster = booster_factory(model_str=model_text)
    except Exception as exc:
        raise RotationL1PredictionError(REASON_INFERENCE, "single-date frozen model load failed") from exc
    eligible_frame = prepared.loc[eligible, FEATURES]
    scores = np.asarray(booster.predict(eligible_frame), dtype=np.float64)
    if scores.shape != (len(eligible_frame),) or not np.isfinite(scores).all():
        raise RotationL1PredictionError(REASON_SCORE, "single-date score is invalid")
    try:
        _receipt, contributions = _contribution_receipt(booster, eligible_frame, scores)
    except RotationL1G2AError as exc:
        raise RotationL1PredictionError(exc.reason_code, str(exc)) from exc
    score_series = pd.Series(np.nan, index=prepared.index, dtype=np.float64)
    score_series.loc[eligible] = scores
    try:
        states, _state_receipt = project_states(score_series)
    except RotationL1G2AError as exc:
        raise RotationL1PredictionError(exc.reason_code, str(exc)) from exc
    contributions_by_code = {
        str(identity[1]): [float(value) for value in values]
        for identity, values in zip(eligible_frame.index, contributions, strict=True)
    }
    model_hash = str(final_model["model_sha256"])
    rows = []
    for identity, score in score_series.items():
        code = str(identity[1])
        available = math.isfinite(float(score))
        rows.append(
            _make_row(
                product_bundle_id=product_bundle_id,
                trade_date=trade_date,
                as_of_date=as_of_date,
                sector_level="L1",
                sector_code=code,
                sector_name=sector_names[code],
                rotation_score=float(score) if available else None,
                forecast_state=states.get((trade_date, code)) if available else None,
                feature_contributions=contributions_by_code.get(code) if available else None,
                availability="available" if available else "unavailable",
                reason_code=None if available else REASON_FEATURE,
                research_surface_status="NOT_AVAILABLE",
                rotation_l1_capability_status=capability_status,
                forward_power_status=forward_power_status,
                forward_confirmation=forward_confirmation,
                advisory_status=(
                    "AVAILABLE" if capability_status == "ADVISORY_PREDICTION_AVAILABLE" else "NOT_AVAILABLE"
                ),
                validation_basis="single_date_frozen_model",
                development_oof_rank_ic=development_values[0],
                development_oof_rank_ic_hac_lower=development_values[1],
                development_oof_rank_ic_hac_upper=development_values[2],
                model_hash=model_hash,
                input_hash=input_hash,
                mapping_snapshot_hash=mapping_snapshot_hash,
                tail_accessed=capability_status == "ADVISORY_PREDICTION_AVAILABLE",
                revision=1,
                supersedes_prediction_id=None,
            )
        )
    return rows


__all__ = [
    "RotationL1PredictionError",
    "RotationL1PredictionRepository",
    "build_single_date_raw_features",
    "build_oof_prediction_rows",
    "predict_single_date_from_assets",
    "predict_single_date_rows",
]
