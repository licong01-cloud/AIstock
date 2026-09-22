"""Persistence and readback contract for SW L2 rotation research predictions."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
import uuid

from backend.db.pg_pool import get_conn
from backend.services.hmm_risk.contracts import canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.rotation_l2 import ACCEPTANCE_SCHEMA, BINDING_MBE_RANK_IC


REASON_NOT_FOUND = "hmm_risk_rotation_l2_not_found"
REASON_CONFLICT = "hmm_risk_rotation_l2_conflict"
REASON_WRITER = "hmm_risk_rotation_l2_writer_failed"
REASON_READBACK = "hmm_risk_rotation_l2_readback_failed"
ROW_SCHEMA = "hmm_risk_rotation_l2_prediction_v1"
SURFACE_SCHEMA = "hmm_risk_rotation_l2_product_validation_v1"
CATALOG_COUNT = 131
PREDICTION_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "aistock:hmm-risk:rotation-l2-prediction:v1")

PREDICTION_COLUMNS = (
    "prediction_id",
    "run_id",
    "model_hash",
    "evaluation_contract_hash",
    "input_hash",
    "mapping_hash",
    "quote_authority_hash",
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
    "structural_eligible",
    "feature_eligible",
    "outcome_status",
    "execution_status",
    "effect_status",
    "research_surface_status",
    "rotation_l2_capability_status",
    "forward_power_status",
    "forward_confirmation",
    "advisory_status",
    "validation_basis",
    "run_summary",
    "revision",
    "supersedes_prediction_id",
)


class RotationL2PredictionError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _parse_date(value: Any, field: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise RotationL2PredictionError(REASON_WRITER, f"{field} is not an ISO date") from exc


def _row_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key in PREDICTION_COLUMNS:
        value = row[key]
        if isinstance(value, (date, uuid.UUID)):
            value = str(value)
        output[key] = value
    return output


def _prediction_id(row: Mapping[str, Any]) -> uuid.UUID:
    return uuid.uuid5(
        PREDICTION_ID_NAMESPACE,
        "|".join(
            (
                str(row["run_id"]),
                row["trade_date"].isoformat(),
                str(row["sector_code"]),
                str(row["revision"]),
            )
        ),
    )


def _validate_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(raw)
    missing = [field for field in PREDICTION_COLUMNS if field not in row]
    extra = sorted(set(row) - set(PREDICTION_COLUMNS))
    if missing or extra:
        raise RotationL2PredictionError(
            REASON_WRITER, "prediction fields differ", context={"missing": missing, "extra": extra}
        )
    for field in (
        "run_id",
        "model_hash",
        "evaluation_contract_hash",
        "input_hash",
        "mapping_hash",
        "quote_authority_hash",
    ):
        if not _is_sha256(row[field]):
            raise RotationL2PredictionError(REASON_WRITER, f"{field} is not a SHA-256")
    row["trade_date"] = _parse_date(row["trade_date"], "trade_date")
    row["as_of_date"] = _parse_date(row["as_of_date"], "as_of_date")
    if (
        row["as_of_date"] >= row["trade_date"]
        or row["sector_level"] != "L2"
        or not str(row["sector_code"] or "").strip()
        or not str(row["sector_name"] or "").strip()
    ):
        raise RotationL2PredictionError(REASON_WRITER, "prediction date or level differs")
    if type(row["structural_eligible"]) is not bool or type(row["feature_eligible"]) is not bool:
        raise RotationL2PredictionError(REASON_WRITER, "eligibility flags are not booleans")
    if row["availability"] not in {"available", "unavailable"}:
        raise RotationL2PredictionError(REASON_WRITER, "availability differs")
    available = row["availability"] == "available"
    score = row["rotation_score"]
    if available:
        if not isinstance(score, (float, int)) or not math.isfinite(float(score)) or not -0.5 <= float(score) <= 0.5:
            raise RotationL2PredictionError(REASON_WRITER, "available score is invalid")
        if row["forecast_state"] not in {"trending", "neutral", "fading"} or row["reason_code"] is not None:
            raise RotationL2PredictionError(REASON_WRITER, "available state/reason coupling differs")
        contribution = row["feature_contributions"]
        if (
            not row["structural_eligible"]
            or not isinstance(contribution, Mapping)
            or set(contribution) != {"moneyflow_intensity_delta_5d_rank"}
            or not isinstance(contribution["moneyflow_intensity_delta_5d_rank"], (float, int))
            or not math.isfinite(float(contribution["moneyflow_intensity_delta_5d_rank"]))
            or float(contribution["moneyflow_intensity_delta_5d_rank"]) != float(score)
        ):
            raise RotationL2PredictionError(REASON_WRITER, "available contribution differs from the score contract")
    elif (
        any(value is not None for value in (score, row["forecast_state"], row["feature_contributions"]))
        or not isinstance(row["reason_code"], str)
        or not row["reason_code"].strip()
    ):
        raise RotationL2PredictionError(REASON_WRITER, "unavailable value/reason coupling differs")
    if row["research_surface_status"] != "NOT_AVAILABLE":
        raise RotationL2PredictionError(REASON_WRITER, "database rows may not self-authorize a product surface")
    if row["execution_status"] != "COMPLETED":
        raise RotationL2PredictionError(REASON_WRITER, "execution status differs")
    if row["effect_status"] not in {
        "DEVELOPMENT_EFFECT_QUALIFIED",
        "BELOW_BINDING_MBE",
        "EVIDENCE_INSUFFICIENT",
        "NO_USABLE_PREDICTIONS",
    }:
        raise RotationL2PredictionError(REASON_WRITER, "effect status differs")
    if row["outcome_status"] not in {
        "available",
        "outcome_not_mature",
        "outcome_unavailable_quote_discontinued",
        "prediction_unavailable",
    }:
        raise RotationL2PredictionError(REASON_WRITER, "outcome status differs")
    if row["rotation_l2_capability_status"] not in {
        "NOT_AVAILABLE",
        "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
    }:
        raise RotationL2PredictionError(REASON_WRITER, "L2 capability status differs")
    qualified = row["effect_status"] == "DEVELOPMENT_EFFECT_QUALIFIED"
    research_available = row["rotation_l2_capability_status"] == "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
    if qualified != research_available:
        raise RotationL2PredictionError(REASON_WRITER, "effect and L2 capability states differ")
    if row["forward_power_status"] != "UNAVAILABLE" or row["forward_confirmation"] != "NOT_STARTED":
        raise RotationL2PredictionError(REASON_WRITER, "forward state differs")
    if row["advisory_status"] != "NOT_AVAILABLE" or row["validation_basis"] != "HISTORICAL_CAUSAL_REPLAY_ZERO_FIT":
        raise RotationL2PredictionError(REASON_WRITER, "advisory/validation state differs")
    if type(row["revision"]) is not int or row["revision"] < 1:
        raise RotationL2PredictionError(REASON_WRITER, "revision is invalid")
    if (row["revision"] == 1) != (row["supersedes_prediction_id"] is None):
        raise RotationL2PredictionError(REASON_WRITER, "revision lineage differs")
    expected_id = _prediction_id(row)
    if row["prediction_id"] is None:
        row["prediction_id"] = expected_id
    elif uuid.UUID(str(row["prediction_id"])) != expected_id:
        raise RotationL2PredictionError(REASON_WRITER, "prediction id differs")
    row["prediction_id"] = expected_id
    if row["supersedes_prediction_id"] is not None:
        row["supersedes_prediction_id"] = uuid.UUID(str(row["supersedes_prediction_id"]))
    if not isinstance(row["run_summary"], Mapping):
        raise RotationL2PredictionError(REASON_WRITER, "run summary is absent")
    if (
        row["run_summary"].get("tail_accessed") is not False
        or row["run_summary"].get("planned_fits") != 0
        or row["run_summary"].get("completed_fits") != 0
        or not _is_sha256(row["run_summary"].get("acceptance_sha256"))
        or not isinstance(row["run_summary"].get("metrics"), Mapping)
    ):
        raise RotationL2PredictionError(REASON_WRITER, "run summary violates the zero-fit no-tail contract")
    return row


def rows_from_acceptance(acceptance: Mapping[str, Any]) -> list[dict[str, Any]]:
    body = {key: value for key, value in acceptance.items() if key != "acceptance_sha256"}
    if acceptance.get("schema_version") != ACCEPTANCE_SCHEMA or acceptance.get("acceptance_sha256") != canonical_sha256(
        body
    ):
        raise RotationL2PredictionError(REASON_WRITER, "acceptance receipt is invalid")
    metrics = acceptance.get("metrics")
    if not isinstance(metrics, Mapping):
        raise RotationL2PredictionError(REASON_WRITER, "acceptance metrics are absent")
    compact_metrics = {
        key: metrics[key]
        for key in (
            "blocks",
            "overall",
            "mature_day_count",
            "metric_eligible_day_count",
            "valid_ic_day_count",
            "valid_ic_day_share",
            "hac",
            "outcome_status_counts",
            "coverage_sufficient",
            "evidence_sufficient",
        )
        if key in metrics
    }
    summary = {
        "acceptance_sha256": acceptance["acceptance_sha256"],
        "metrics": compact_metrics,
        "tail_accessed": acceptance["tail_accessed"],
        "planned_fits": acceptance["planned_fits"],
        "completed_fits": acceptance["completed_fits"],
    }
    rows: list[dict[str, Any]] = []
    for prediction in acceptance["predictions"]:
        row = {
            "prediction_id": None,
            "run_id": acceptance["run_id"],
            "model_hash": acceptance["model_hash"],
            "evaluation_contract_hash": acceptance["evaluation_contract_hash"],
            "input_hash": acceptance["input_hash"],
            "mapping_hash": acceptance["mapping_hash"],
            "quote_authority_hash": acceptance["quote_authority_hash"],
            "trade_date": prediction["trade_date"],
            "as_of_date": prediction["as_of_date"],
            "sector_level": "L2",
            "sector_code": prediction["sector_code"],
            "sector_name": prediction["sector_name"],
            "rotation_score": prediction["rotation_score"],
            "forecast_state": prediction["forecast_state"],
            "feature_contributions": prediction["feature_contributions"],
            "availability": prediction["availability"],
            "reason_code": prediction["reason_code"],
            "structural_eligible": prediction["structural_eligible"],
            "feature_eligible": prediction["feature_eligible"],
            "outcome_status": prediction["outcome_status"],
            "execution_status": acceptance["execution_status"],
            "effect_status": acceptance["effect_status"],
            "research_surface_status": "NOT_AVAILABLE",
            "rotation_l2_capability_status": acceptance["rotation_l2_capability_status"],
            "forward_power_status": acceptance["forward_power_status"],
            "forward_confirmation": acceptance["forward_confirmation"],
            "advisory_status": acceptance["advisory_status"],
            "validation_basis": acceptance["validation_basis"],
            "run_summary": summary,
            "revision": 1,
            "supersedes_prediction_id": None,
        }
        rows.append(_validate_row(row))
    _validate_batch(rows)
    return rows


def _validate_batch(rows: Sequence[Mapping[str, Any]]) -> None:
    by_date: dict[date, list[Mapping[str, Any]]] = {}
    for row in rows:
        by_date.setdefault(row["trade_date"], []).append(row)
    for trade_date, daily in by_date.items():
        if len(daily) != CATALOG_COUNT or len({row["sector_code"] for row in daily}) != CATALOG_COUNT:
            raise RotationL2PredictionError(
                REASON_WRITER,
                "prediction date does not contain 131 L2 sectors",
                context={"trade_date": trade_date.isoformat()},
            )
        invariants = (
            "run_id",
            "model_hash",
            "evaluation_contract_hash",
            "input_hash",
            "mapping_hash",
            "quote_authority_hash",
            "execution_status",
            "effect_status",
            "research_surface_status",
            "rotation_l2_capability_status",
            "forward_power_status",
            "forward_confirmation",
            "advisory_status",
            "validation_basis",
            "run_summary",
        )
        head = daily[0]
        if any(row["revision"] != head["revision"] for row in daily[1:]):
            raise RotationL2PredictionError(REASON_WRITER, "prediction date mixes revisions")
        if any(row[field] != head[field] for row in daily[1:] for field in invariants):
            raise RotationL2PredictionError(REASON_WRITER, "prediction date invariants differ")


def _stored_row(raw: Sequence[Any]) -> dict[str, Any]:
    row = dict(zip(PREDICTION_COLUMNS, raw, strict=True))
    for field in (
        "run_id",
        "model_hash",
        "evaluation_contract_hash",
        "input_hash",
        "mapping_hash",
        "quote_authority_hash",
    ):
        row[field] = str(row[field]).strip()
    for field in ("feature_contributions", "run_summary"):
        if isinstance(row[field], str):
            row[field] = json.loads(row[field])
    return _validate_row(row)


def _load_surface_receipt(path: Path | None, *, run_id: str, row_hash: str) -> bool:
    if path is None or not path.is_file() or path.is_symlink():
        return False
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    body = {key: item for key, item in value.items() if key != "receipt_sha256"} if isinstance(value, dict) else {}
    return bool(
        value.get("schema_version") == SURFACE_SCHEMA
        and value.get("run_id") == run_id
        and value.get("canonical_row_sha256") == row_hash
        and value.get("writer_readback") is True
        and value.get("api_readback") is True
        and value.get("browser_no_mock") is True
        and value.get("receipt_sha256") == canonical_sha256(body)
    )


@dataclass
class RotationL2PredictionRepository:
    conn_factory: Callable[[], AbstractContextManager[Any]] = lambda: get_conn(
        autocommit=False, manage_transaction=True
    )
    surface_validation_receipt_path: Path | None = None

    def write_rows(self, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        validated = [_validate_row(row) for row in rows]
        if not validated:
            raise RotationL2PredictionError(REASON_WRITER, "prediction write is empty")
        _validate_batch(validated)
        requested_keys = {(row["run_id"], row["trade_date"], row["sector_code"], row["revision"]) for row in validated}
        if len(requested_keys) != len(validated):
            raise RotationL2PredictionError(REASON_CONFLICT, "prediction write contains duplicate keys")
        sql = f"""
        INSERT INTO hmm_risk.rotation_l2_prediction ({",".join(PREDICTION_COLUMNS)})
        VALUES ({",".join(["%s"] * len(PREDICTION_COLUMNS))})
        ON CONFLICT (run_id,trade_date,sector_code,revision) DO NOTHING
        """
        with self.conn_factory() as conn:
            with conn.cursor() as cursor:
                for row in validated:
                    if row["revision"] > 1:
                        cursor.execute(
                            f"SELECT {','.join(PREDICTION_COLUMNS)} FROM hmm_risk.rotation_l2_prediction "
                            "WHERE prediction_id=%s FOR UPDATE",
                            (row["supersedes_prediction_id"],),
                        )
                        raw_prior = cursor.fetchone()
                        if raw_prior is None:
                            raise RotationL2PredictionError(REASON_CONFLICT, "superseded prediction is absent")
                        prior = _stored_row(raw_prior)
                        if (
                            prior["run_id"],
                            prior["trade_date"],
                            prior["sector_code"],
                            prior["revision"] + 1,
                        ) != (
                            row["run_id"],
                            row["trade_date"],
                            row["sector_code"],
                            row["revision"],
                        ):
                            raise RotationL2PredictionError(
                                REASON_CONFLICT, "prediction revision does not directly supersede its identity"
                            )
                    values = [row[column] for column in PREDICTION_COLUMNS]
                    values[PREDICTION_COLUMNS.index("feature_contributions")] = (
                        canonical_json_bytes(row["feature_contributions"]).decode("utf-8")
                        if row["feature_contributions"] is not None
                        else None
                    )
                    values[PREDICTION_COLUMNS.index("run_summary")] = canonical_json_bytes(row["run_summary"]).decode(
                        "utf-8"
                    )
                    cursor.execute(sql, tuple(values))
                run_id = validated[0]["run_id"]
                cursor.execute(
                    f"SELECT {','.join(PREDICTION_COLUMNS)} FROM hmm_risk.rotation_l2_prediction WHERE run_id=%s ORDER BY trade_date,sector_code,revision",
                    (run_id,),
                )
                stored = [
                    row
                    for row in (_stored_row(raw) for raw in cursor.fetchall())
                    if (row["run_id"], row["trade_date"], row["sector_code"], row["revision"]) in requested_keys
                ]
        if len(stored) != len(validated):
            raise RotationL2PredictionError(REASON_CONFLICT, "stored run row count differs")
        requested_hash = canonical_sha256([_row_identity(row) for row in validated])
        stored_hash = canonical_sha256([_row_identity(row) for row in stored])
        if requested_hash != stored_hash:
            raise RotationL2PredictionError(REASON_CONFLICT, "stored run payload conflicts with requested payload")
        return {
            "run_id": validated[0]["run_id"],
            "row_count": len(stored),
            "canonical_row_sha256": stored_hash,
            "idempotency_verified": True,
        }

    def read_date(self, trade_date: date, *, run_id: str) -> dict[str, Any]:
        if not _is_sha256(run_id):
            raise RotationL2PredictionError(REASON_CONFLICT, "explicit run id is invalid")
        with self.conn_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""SELECT {",".join(PREDICTION_COLUMNS)} FROM hmm_risk.rotation_l2_prediction
                    WHERE run_id=%s AND trade_date=%s
                      AND revision=(
                        SELECT max(latest.revision)
                        FROM hmm_risk.rotation_l2_prediction latest
                        WHERE latest.run_id=hmm_risk.rotation_l2_prediction.run_id
                          AND latest.trade_date=hmm_risk.rotation_l2_prediction.trade_date
                          AND latest.sector_code=hmm_risk.rotation_l2_prediction.sector_code
                      )
                    ORDER BY sector_code""",
                    (run_id, trade_date),
                )
                rows = [_stored_row(raw) for raw in cursor.fetchall()]
        if not rows:
            raise RotationL2PredictionError(REASON_NOT_FOUND, "L2 prediction date is not found")
        _validate_batch(rows)
        return {"run_id": run_id, "trade_date": trade_date.isoformat(), "rows": rows}

    def overview(self, *, run_id: str) -> dict[str, Any]:
        if not _is_sha256(run_id):
            raise RotationL2PredictionError(REASON_CONFLICT, "explicit run id is invalid")
        with self.conn_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT max(trade_date) FROM hmm_risk.rotation_l2_prediction WHERE run_id=%s", (run_id,))
                raw = cursor.fetchone()
        if not raw or raw[0] is None:
            raise RotationL2PredictionError(REASON_NOT_FOUND, "L2 prediction run is not found")
        detail = self.read_date(raw[0], run_id=run_id)
        rows = detail["rows"]
        head = rows[0]
        row_hash = canonical_sha256([_row_identity(row) for row in rows])
        surface = (
            "AVAILABLE_EXPERIMENTAL"
            if _load_surface_receipt(self.surface_validation_receipt_path, run_id=run_id, row_hash=row_hash)
            else "NOT_AVAILABLE"
        )
        return {
            "run_id": run_id,
            "model_hash": head["model_hash"],
            "trade_date": detail["trade_date"],
            "as_of_date": head["as_of_date"].isoformat(),
            "sector_count": len(rows),
            "available_count": sum(row["availability"] == "available" for row in rows),
            "binding_mbe_rank_ic": BINDING_MBE_RANK_IC,
            "research_surface_status": surface,
            "rotation_l2_capability_status": head["rotation_l2_capability_status"],
            "effect_status": head["effect_status"],
            "forward_power_status": head["forward_power_status"],
            "forward_confirmation": head["forward_confirmation"],
            "advisory_status": head["advisory_status"],
            "validation_basis": head["validation_basis"],
            "input_hash": head["input_hash"],
            "mapping_hash": head["mapping_hash"],
            "quote_authority_hash": head["quote_authority_hash"],
            "tail_accessed": bool(head["run_summary"]["tail_accessed"]),
            "metrics": head["run_summary"]["metrics"],
            "canonical_row_sha256": row_hash,
        }


__all__ = [
    "PREDICTION_COLUMNS",
    "REASON_CONFLICT",
    "REASON_NOT_FOUND",
    "RotationL2PredictionError",
    "RotationL2PredictionRepository",
    "rows_from_acceptance",
]
