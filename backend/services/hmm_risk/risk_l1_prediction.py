"""Append-only G2-B L1 risk predictions and model-bound read APIs."""

from __future__ import annotations

import json
import math
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from backend.db.pg_pool import get_conn
from backend.services.hmm_risk.risk_l1_g2b import (
    CANONICAL_SECTOR_COUNT,
    RISK_FEATURES,
    close_processes,
    load_model,
    predict_single_date,
)
from backend.services.hmm_risk.contracts import canonical_sha256

REASON_WRITER = "hmm_risk_risk_l1_prediction_write_failed"
REASON_READBACK = "hmm_risk_risk_l1_prediction_readback_mismatch"
REASON_CONFLICT = "hmm_risk_risk_l1_prediction_identity_conflict"
REASON_NOT_FOUND = "hmm_risk_risk_l1_prediction_not_found"
REASON_MODEL_AMBIGUOUS = "hmm_risk_risk_l1_prediction_model_ambiguous"
PREDICTION_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "aistock:hmm-risk:risk-l1-prediction:v1")

PREDICTION_COLUMNS = (
    "prediction_id",
    "product_bundle_id",
    "trade_date",
    "as_of_date",
    "sector_level",
    "sector_code",
    "sector_name",
    "risk_score",
    "risk_percentile",
    "risk_level",
    "predicted_warning",
    "feature_contributions",
    "availability",
    "reason_code",
    "risk_l1_research_surface_status",
    "risk_l1_capability_status",
    "forward_power_status",
    "forward_confirmation",
    "advisory_status",
    "validation_basis",
    "development_precision_lift",
    "development_recall",
    "model_hash",
    "input_hash",
    "mapping_snapshot_hash",
    "tail_accessed",
    "revision",
    "supersedes_prediction_id",
)

_INSERT_SQL = f"""
INSERT INTO hmm_risk.risk_l1_prediction ({",".join(PREDICTION_COLUMNS)})
VALUES ({",".join(["%s"] * len(PREDICTION_COLUMNS))})
ON CONFLICT (model_hash,trade_date,sector_code,revision) DO NOTHING
"""
_READ_ONE_SQL = f"""
SELECT {",".join(PREDICTION_COLUMNS)}
FROM hmm_risk.risk_l1_prediction
WHERE model_hash=%s AND trade_date=%s AND sector_code=%s AND revision=%s
"""


class RiskL1PredictionError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _json_value(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _identity_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {column: _json_value(row.get(column)) for column in PREDICTION_COLUMNS if column != "prediction_id"}


def _prediction_id(row: Mapping[str, Any]) -> str:
    canonical = json.dumps(_identity_payload(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return str(uuid.uuid5(PREDICTION_ID_NAMESPACE, canonical))


def _finite_optional(value: Any) -> bool:
    return value is None or (isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value)))


def _validate_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    if set(raw) != set(PREDICTION_COLUMNS):
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction row fields differ")
    row = dict(raw)
    try:
        row["prediction_id"] = str(uuid.UUID(str(row["prediction_id"])))
        if row["supersedes_prediction_id"] is not None:
            row["supersedes_prediction_id"] = str(uuid.UUID(str(row["supersedes_prediction_id"])))
    except (ValueError, TypeError, AttributeError) as exc:
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction UUID differs") from exc
    for field in ("trade_date", "as_of_date"):
        if isinstance(row[field], str):
            try:
                row[field] = date.fromisoformat(row[field])
            except ValueError as exc:
                raise RiskL1PredictionError(REASON_WRITER, f"risk prediction {field} differs") from exc
    if not isinstance(row["trade_date"], date) or not isinstance(row["as_of_date"], date) or row["as_of_date"] >= row["trade_date"]:
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction date boundary differs")
    if row["sector_level"] != "L1" or not str(row["sector_code"]).strip() or not str(row["sector_name"]).strip():
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction sector identity differs")
    if not all(_is_sha256(row[field]) for field in ("model_hash", "input_hash", "mapping_snapshot_hash")):
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction hashes differ")
    if (
        not isinstance(row["revision"], int)
        or isinstance(row["revision"], bool)
        or row["revision"] < 1
        or (row["revision"] == 1) != (row["supersedes_prediction_id"] is None)
    ):
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction revision chain differs")
    if row["availability"] == "available":
        if (
            not all(_finite_optional(row[field]) and row[field] is not None for field in ("risk_score", "risk_percentile"))
            or not 0.0 <= float(row["risk_score"]) <= 1.0
            or not 0.0 <= float(row["risk_percentile"]) <= 1.0
            or row["risk_level"] not in {"normal", "watch", "high"}
            or not isinstance(row["predicted_warning"], bool)
            or row["predicted_warning"] != (row["risk_level"] == "high")
            or not isinstance(row["feature_contributions"], list)
            or len(row["feature_contributions"]) != len(RISK_FEATURES) + 1
            or not all(_finite_optional(value) and value is not None for value in row["feature_contributions"])
            or row["reason_code"] is not None
        ):
            raise RiskL1PredictionError(REASON_WRITER, "available risk prediction payload differs")
    elif row["availability"] == "unavailable":
        if (
            any(row[field] is not None for field in ("risk_score", "risk_percentile", "risk_level", "predicted_warning", "feature_contributions"))
            or not isinstance(row["reason_code"], str)
            or not row["reason_code"].strip()
        ):
            raise RiskL1PredictionError(REASON_WRITER, "unavailable risk prediction payload differs")
    else:
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction availability differs")
    if row["risk_l1_research_surface_status"] != "NOT_AVAILABLE":
        raise RiskL1PredictionError(REASON_WRITER, "offline risk row claims a validated product surface")
    if row["risk_l1_capability_status"] not in {"NOT_AVAILABLE", "RESEARCH_RISK_WARNING_AVAILABLE_FORWARD_UNCONFIRMED", "ADVISORY_RISK_WARNING_AVAILABLE"}:
        raise RiskL1PredictionError(REASON_WRITER, "risk capability status differs")
    if row["forward_power_status"] not in {"UNAVAILABLE", "INSUFFICIENT", "SUFFICIENT"}:
        raise RiskL1PredictionError(REASON_WRITER, "risk forward power status differs")
    if row["forward_confirmation"] not in {"NOT_STARTED", "PENDING_INSUFFICIENT_POWER", "PENDING_INCONCLUSIVE", "PASSED", "FAILED"}:
        raise RiskL1PredictionError(REASON_WRITER, "risk forward confirmation differs")
    if row["advisory_status"] not in {"NOT_AVAILABLE", "AVAILABLE"} or row["validation_basis"] not in {"development_causal_oof", "single_date_frozen_model"}:
        raise RiskL1PredictionError(REASON_WRITER, "risk product status differs")
    if (
        row["tail_accessed"] is not False
        or row["advisory_status"] != "NOT_AVAILABLE"
        or row["product_bundle_id"] is not None
    ):
        raise RiskL1PredictionError(REASON_WRITER, "risk row crosses the approved development boundary")
    if not all(_finite_optional(row[field]) for field in ("development_precision_lift", "development_recall")):
        raise RiskL1PredictionError(REASON_WRITER, "risk development metrics differ")
    if row["prediction_id"] != _prediction_id(row):
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction deterministic identity differs")
    return row


def _validate_batch(rows: Sequence[Mapping[str, Any]]) -> None:
    if len(rows) != CANONICAL_SECTOR_COUNT or len({str(row["sector_code"]) for row in rows}) != CANONICAL_SECTOR_COUNT:
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction batch does not contain 31 sectors")
    invariant = (
        "trade_date", "as_of_date", "model_hash", "input_hash", "mapping_snapshot_hash", "revision",
        "risk_l1_research_surface_status", "risk_l1_capability_status", "forward_power_status",
        "forward_confirmation", "advisory_status", "validation_basis", "development_precision_lift",
        "development_recall", "tail_accessed",
    )
    head = rows[0]
    if any(row[field] != head[field] for row in rows[1:] for field in invariant):
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction batch authority differs")


def _stored_row(raw: Sequence[Any]) -> dict[str, Any]:
    row = dict(zip(PREDICTION_COLUMNS, raw, strict=True))
    if isinstance(row["feature_contributions"], str):
        row["feature_contributions"] = json.loads(row["feature_contributions"])
    for field in ("model_hash", "input_hash", "mapping_snapshot_hash"):
        row[field] = str(row[field]).strip()
    return _validate_row(row)


def _insert_and_readback(cursor: Any, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    readback: list[dict[str, Any]] = []
    for row in rows:
        cursor.execute(_INSERT_SQL, tuple(json.dumps(row[column]) if column == "feature_contributions" else row[column] for column in PREDICTION_COLUMNS))
        cursor.execute(_READ_ONE_SQL, (row["model_hash"], row["trade_date"], row["sector_code"], row["revision"]))
        stored = cursor.fetchone()
        if stored is None:
            raise RiskL1PredictionError(REASON_READBACK, "risk prediction row is missing after write")
        decoded = _stored_row(stored)
        if _identity_payload(decoded) != _identity_payload(row):
            raise RiskL1PredictionError(REASON_CONFLICT, "risk prediction key already contains a different payload")
        readback.append(decoded)
    readback.sort(
        key=lambda row: (str(row["model_hash"]), row["trade_date"], str(row["sector_code"]), row["revision"])
    )
    return readback


def build_oof_prediction_rows(
    *,
    acceptance: Mapping[str, Any],
    process_reports: Sequence[Mapping[str, Any]],
    sector_names: Mapping[str, str],
) -> list[dict[str, Any]]:
    if len(process_reports) != 2:
        raise RiskL1PredictionError(REASON_WRITER, "risk prediction requires two process reports")
    closed, _model = close_processes(process_reports[0], process_reports[1])
    if dict(acceptance) != closed:
        raise RiskL1PredictionError(REASON_WRITER, "risk acceptance does not match child closure")
    payload = process_reports[0]["reproducibility_payload"]
    metrics = payload["metrics"]
    identity = payload["input_identity"]
    if len(sector_names) != CANONICAL_SECTOR_COUNT:
        raise RiskL1PredictionError(REASON_WRITER, "risk sector-name authority differs")
    rows: list[dict[str, Any]] = []
    for raw in payload["oof_prediction_rows"]:
        code = str(raw["sector_code"])
        if code not in sector_names:
            raise RiskL1PredictionError(REASON_WRITER, "risk prediction sector name is missing")
        row = {
            "product_bundle_id": None,
            "trade_date": date.fromisoformat(str(raw["trade_date"])),
            "as_of_date": date.fromisoformat(str(raw["as_of_date"])),
            "sector_level": "L1",
            "sector_code": code,
            "sector_name": str(sector_names[code]),
            "risk_score": raw["risk_score"],
            "risk_percentile": raw["risk_percentile"],
            "risk_level": raw["risk_level"],
            "predicted_warning": raw["predicted_warning"],
            "feature_contributions": raw["feature_contributions"],
            "availability": raw["availability"],
            "reason_code": raw["reason_code"],
            "risk_l1_research_surface_status": "NOT_AVAILABLE",
            "risk_l1_capability_status": payload["risk_l1_capability_status"],
            "forward_power_status": payload["forward_power_status"],
            "forward_confirmation": payload["forward_confirmation"],
            "advisory_status": payload["advisory_status"],
            "validation_basis": "development_causal_oof",
            "development_precision_lift": metrics["precision_lift"],
            "development_recall": metrics["recall"],
            "model_hash": str(raw["model_hash"]),
            "input_hash": str(identity["source_sha256"]),
            "mapping_snapshot_hash": str(identity["mapping_sha256"]),
            "tail_accessed": False,
            "revision": 1,
            "supersedes_prediction_id": None,
        }
        row["prediction_id"] = _prediction_id(row)
        rows.append(_validate_row(row))
    return rows


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
    model_artifact: Mapping[str, Any],
    product_bundle_id: str | None = None,
    booster_factory: Any | None = None,
) -> dict[str, Any]:
    """Execute one target-free G2-B request from an explicit direct-v2 release."""

    from backend.services.hmm_risk.rotation_l1_input_bundle import (
        RotationL1InputBundleError,
        build_rotation_l1_single_date_source_from_assets,
    )
    from backend.services.hmm_risk.rotation_l1_prediction import (
        RotationL1PredictionError,
        build_single_date_raw_features,
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
            model_contract_version="hmm_risk_risk_l1_g2b_v1",
        )
    except RotationL1InputBundleError as exc:
        raise RiskL1PredictionError(exc.reason_code, str(exc), context=exc.context) from exc
    receipt = source.get("source_receipt")
    if not isinstance(receipt, Mapping):
        raise RiskL1PredictionError(REASON_READBACK, "risk single-date source receipt is missing")
    receipt_body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if (
        source.get("schema_version") != "hmm_risk_risk_l1_single_date_source_v1"
        or source.get("model_contract_version") != "hmm_risk_risk_l1_g2b_v1"
        or receipt.get("schema_version") != source.get("schema_version")
        or receipt.get("model_contract_version") != source.get("model_contract_version")
        or receipt.get("receipt_sha256") != canonical_sha256(receipt_body)
        or source.get("input_hash") != canonical_sha256(receipt_body)
        or receipt.get("target_columns_read") is not False
        or receipt.get("trade_date") != trade_date.isoformat()
        or receipt.get("as_of_date") != as_of_date.isoformat()
    ):
        raise RiskL1PredictionError(REASON_READBACK, "risk single-date source receipt differs")
    try:
        raw_features = build_single_date_raw_features(
            calendar=source["feature_calendar"],
            sector_close=source["sector_close"],
            benchmark_close=source["benchmark_close"],
            stock_daily_inputs=source["stock_daily_inputs"],
            trade_date=trade_date,
        )
    except RotationL1PredictionError as exc:
        raise RiskL1PredictionError(exc.reason_code, str(exc), context=exc.context) from exc
    model = load_model(model_artifact, booster_factory=booster_factory)
    inferred = predict_single_date(raw_features.droplevel("trade_date"), trade_date=trade_date, as_of_date=as_of_date, model=model)
    metrics = model_artifact.get("development_metrics")
    identity = model_artifact.get("input_identity")
    if not isinstance(metrics, Mapping) or not isinstance(identity, Mapping):
        raise RiskL1PredictionError(REASON_READBACK, "risk model evidence is incomplete")
    sector_names = source.get("sector_names")
    if not isinstance(sector_names, Mapping) or len(sector_names) != CANONICAL_SECTOR_COUNT:
        raise RiskL1PredictionError(REASON_READBACK, "risk single-date sector names differ")
    rows: list[dict[str, Any]] = []
    for raw in inferred:
        code = str(raw["sector_code"])
        row = {
            "product_bundle_id": product_bundle_id,
            "trade_date": trade_date,
            "as_of_date": as_of_date,
            "sector_level": "L1",
            "sector_code": code,
            "sector_name": str(sector_names[code]),
            "risk_score": raw["risk_score"],
            "risk_percentile": raw["risk_percentile"],
            "risk_level": raw["risk_level"],
            "predicted_warning": raw["predicted_warning"],
            "feature_contributions": raw["feature_contributions"],
            "availability": raw["availability"],
            "reason_code": raw["reason_code"],
            "risk_l1_research_surface_status": "NOT_AVAILABLE",
            "risk_l1_capability_status": model_artifact["risk_l1_capability_status"],
            "forward_power_status": model_artifact["forward_power_status"],
            "forward_confirmation": model_artifact["forward_confirmation"],
            "advisory_status": model_artifact["advisory_status"],
            "validation_basis": "single_date_frozen_model",
            "development_precision_lift": metrics.get("precision_lift"),
            "development_recall": metrics.get("recall"),
            "model_hash": model.model_hash,
            "input_hash": str(source["input_hash"]),
            "mapping_snapshot_hash": str(source["mapping_snapshot_hash"]),
            "tail_accessed": False,
            "revision": 1,
            "supersedes_prediction_id": None,
        }
        row["prediction_id"] = _prediction_id(row)
        rows.append(_validate_row(row))
    _validate_batch(rows)
    return {"rows": rows, "source_receipt": dict(receipt), "model_fit_count": 0, "target_columns_read": False, "tail_accessed": False}


def _load_surface_receipt(path: Path | None) -> Mapping[str, Any] | None:
    if path is None:
        return None
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RiskL1PredictionError(REASON_READBACK, "risk product validation receipt cannot be read") from exc
    if not isinstance(value, Mapping):
        raise RiskL1PredictionError(REASON_READBACK, "risk product validation receipt differs")
    return value


def _surface_status(rows: Sequence[Mapping[str, Any]], receipt: Mapping[str, Any] | None) -> str:
    if receipt is None:
        return "NOT_AVAILABLE"
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    expected_hash = canonical_sha256([_identity_payload(row) for row in rows])
    if (
        receipt.get("schema_version") != "hmm_risk_risk_l1_product_validation_v1"
        or receipt.get("receipt_sha256") != canonical_sha256(body)
        or receipt.get("model_hash") != rows[0]["model_hash"]
        or receipt.get("trade_date") != rows[0]["trade_date"].isoformat()
        or receipt.get("row_count") != CANONICAL_SECTOR_COUNT
        or receipt.get("input_row_sha256") != expected_hash
        or receipt.get("repository_readback_passed") is not True
        or receipt.get("api_readback_passed") is not True
        or receipt.get("ui_readback_passed") is not True
        or receipt.get("mock_used") is not False
    ):
        raise RiskL1PredictionError(REASON_READBACK, "risk product validation receipt differs")
    return "AVAILABLE_EXPERIMENTAL"


@dataclass
class RiskL1PredictionRepository:
    conn_factory: Callable[[], AbstractContextManager[Any]] = lambda: get_conn(autocommit=False, manage_transaction=True)
    surface_validation_receipt_path: Path | None = None

    def write_rows(self, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        validated = sorted(
            (_validate_row(row) for row in rows),
            key=lambda row: (str(row["model_hash"]), row["trade_date"], str(row["sector_code"]), row["revision"]),
        )
        if not validated:
            raise RiskL1PredictionError(REASON_WRITER, "risk prediction write is empty")
        by_date: dict[tuple[str, date, int], list[dict[str, Any]]] = {}
        for row in validated:
            by_date.setdefault((row["model_hash"], row["trade_date"], row["revision"]), []).append(row)
        for batch in by_date.values():
            _validate_batch(batch)
        keys = [(row["model_hash"], row["trade_date"], row["sector_code"], row["revision"]) for row in validated]
        if len(keys) != len(set(keys)):
            raise RiskL1PredictionError(REASON_CONFLICT, "risk prediction write contains duplicate keys")
        with self.conn_factory() as conn:
            with conn.cursor() as cursor:
                readback = _insert_and_readback(cursor, validated)
        return {
            "row_count": len(readback),
            "canonical_row_sha256": canonical_sha256([_identity_payload(row) for row in readback]),
            "idempotency_verified": True,
        }

    def _resolve_model(self, cursor: Any, explicit: str | None, *, trade_date: date | None = None) -> str:
        if explicit is not None:
            if not _is_sha256(explicit):
                raise RiskL1PredictionError(REASON_MODEL_AMBIGUOUS, "explicit risk model hash is invalid")
            return explicit
        if trade_date is None:
            cursor.execute("SELECT DISTINCT model_hash FROM hmm_risk.risk_l1_prediction ORDER BY model_hash")
        else:
            cursor.execute("SELECT DISTINCT model_hash FROM hmm_risk.risk_l1_prediction WHERE trade_date=%s ORDER BY model_hash", (trade_date,))
        hashes = [str(row[0]).strip() for row in cursor.fetchall()]
        if len(hashes) != 1:
            raise RiskL1PredictionError(REASON_NOT_FOUND if not hashes else REASON_MODEL_AMBIGUOUS, "risk prediction model identity is not unique", context={"model_count": len(hashes)})
        return hashes[0]

    def read_date(self, trade_date: date, *, model_hash: str | None = None) -> dict[str, Any]:
        with self.conn_factory() as conn:
            with conn.cursor() as cursor:
                selected = self._resolve_model(cursor, model_hash, trade_date=trade_date)
                cursor.execute(
                    f"""SELECT {','.join('p.' + column for column in PREDICTION_COLUMNS)}
                    FROM hmm_risk.risk_l1_prediction p
                    WHERE p.model_hash=%s AND p.trade_date=%s AND p.revision=(
                      SELECT max(newer.revision) FROM hmm_risk.risk_l1_prediction newer
                      WHERE newer.model_hash=p.model_hash AND newer.trade_date=p.trade_date AND newer.sector_code=p.sector_code)
                    ORDER BY p.sector_code""",
                    (selected, trade_date),
                )
                raw_rows = cursor.fetchall()
        rows = [_stored_row(raw) for raw in raw_rows]
        if not rows:
            raise RiskL1PredictionError(REASON_NOT_FOUND, "risk prediction trade date is not found")
        _validate_batch(rows)
        return {"model_hash": selected, "trade_date": trade_date.isoformat(), "rows": rows}

    def overview(self, *, model_hash: str | None = None) -> dict[str, Any]:
        receipt = _load_surface_receipt(self.surface_validation_receipt_path)
        if model_hash is None and receipt is not None:
            model_hash = receipt.get("model_hash")
        with self.conn_factory() as conn:
            with conn.cursor() as cursor:
                if model_hash is None:
                    cursor.execute("SELECT max(trade_date) FROM hmm_risk.risk_l1_prediction")
                    found = cursor.fetchone()
                    if not found or found[0] is None:
                        raise RiskL1PredictionError(REASON_NOT_FOUND, "risk prediction overview is not found")
                    selected = self._resolve_model(cursor, None, trade_date=found[0])
                else:
                    selected = self._resolve_model(cursor, model_hash)
                    cursor.execute("SELECT max(trade_date) FROM hmm_risk.risk_l1_prediction WHERE model_hash=%s", (selected,))
                    found = cursor.fetchone()
        if not found or found[0] is None:
            raise RiskL1PredictionError(REASON_NOT_FOUND, "risk prediction overview is not found")
        detail = self.read_date(found[0], model_hash=selected)
        rows = detail["rows"]
        head = rows[0]
        return {
            "model_hash": selected,
            "trade_date": detail["trade_date"],
            "as_of_date": head["as_of_date"].isoformat(),
            "sector_count": len(rows),
            "available_count": sum(row["availability"] == "available" for row in rows),
            "high_warning_count": sum(row["predicted_warning"] is True for row in rows),
            "risk_l1_research_surface_status": _surface_status(rows, receipt),
            **{field: head[field] for field in (
                "risk_l1_capability_status", "forward_power_status", "forward_confirmation", "advisory_status",
                "validation_basis", "development_precision_lift", "development_recall", "input_hash",
                "mapping_snapshot_hash", "tail_accessed",
            )},
        }


__all__ = [
    "PREDICTION_COLUMNS",
    "REASON_MODEL_AMBIGUOUS",
    "REASON_NOT_FOUND",
    "RiskL1PredictionError",
    "RiskL1PredictionRepository",
    "build_oof_prediction_rows",
    "predict_single_date_from_assets",
]
