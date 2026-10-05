"""Immutable historical L2 risk persistence; never generates predictions."""

from __future__ import annotations

from collections import Counter
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import math
from pathlib import Path
import re
import stat
from typing import Any, Callable, Mapping, Sequence

from backend.db.pg_pool import get_conn
from backend.services.hmm_risk.contracts import canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.formal_state_effect import verify_receipt
from backend.services.hmm_risk.formal_state_model import FormalStateError
from backend.services.hmm_risk import risk_l2 as risk

REQUEST_SCHEMA = "hmm_risk_risk_l2_import_request_v1"
SURFACE_SCHEMA = "hmm_risk_risk_l2_product_validation_v1"
REASON_NOT_FOUND = "hmm_risk_risk_l2_not_found"
REASON_CONFLICT = "hmm_risk_risk_l2_identity_conflict"
REASON_READBACK = "hmm_risk_risk_l2_readback_failed"
REASON_INPUT = "hmm_risk_risk_l2_input_invalid"
REASON_WRITER = "hmm_risk_risk_l2_writer_failed"
CAPABILITY = "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
EFFECT = "DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED"
RUN_COLUMNS = (
    "run_id",
    "acceptance_hash",
    "sealed_prediction_hash",
    "feature_hash",
    "model_hash",
    "contract_hash",
    "input_hash",
    "mapping_hash",
    "executor_commit",
    "model_version",
    "validation_basis",
    "catalog",
    "dates",
    "input_identity",
    "compact_summary",
    "expected_row_count",
    "effect_status",
    "risk_l2_capability_status",
    "forward_power_status",
    "forward_confirmation",
    "advisory_status",
)
ROW_COLUMNS = (
    "run_id",
    "trade_date",
    "as_of_date",
    "sector_level",
    "sector_code",
    "sector_name",
    "name_authority",
    "probability",
    "warning",
    "availability",
    "reason_code",
    "structural_eligible",
    "outcome_status",
    "event",
    "realized_drawdown",
    "realized_return",
)
JSON_COLUMNS = {"catalog", "dates", "input_identity", "compact_summary"}


class RiskL2PredictionError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


def _require(condition: bool, message: str, reason: str = REASON_INPUT) -> None:
    if not condition:
        raise RiskL2PredictionError(reason, message)


def _sha(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _iso(value: Any) -> str:
    if type(value) is date:
        return value.isoformat()
    _require(isinstance(value, str), "date must be an ISO day")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise RiskL2PredictionError(REASON_INPUT, "date must be an ISO day") from exc
    _require(parsed.isoformat() == value, "date must be YYYY-MM-DD")
    return value


def _regular(path: Path) -> None:
    _require(path.is_absolute(), "artifact path must be absolute")
    for part in (path, *path.parents):
        info = part.lstat()
        _require(
            not stat.S_ISLNK(info.st_mode) and not getattr(info, "st_file_attributes", 0) & 0x400,
            "artifact symlink/junction is forbidden",
        )
    _require(path.is_file(), "artifact must be a regular file")


def _read(path: Path) -> tuple[dict, tuple]:
    _regular(path)
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()

    def key(s):
        return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns

    _require(key(before) == key(after), "artifact changed during read")
    payload = json.loads(raw.decode("utf-8"))
    _require(isinstance(payload, dict), "artifact must be a JSON object")
    return payload, (*key(after), hashlib.sha256(raw).hexdigest())


def _stable(path: Path, identity: tuple) -> None:
    _regular(path)
    s = path.stat()
    actual = (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
    end = path.stat()
    _require(
        actual == identity and actual[:4] == (end.st_dev, end.st_ino, end.st_size, end.st_mtime_ns),
        "artifact changed during validation",
    )


def _receipt(payload: Mapping[str, Any], expected: str) -> None:
    _require(_sha(expected), "independent canonical pin is required")
    verify_receipt(payload, expected)


def _finite(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def validate_row(raw: Mapping[str, Any]) -> dict:
    _require(set(raw) == set(ROW_COLUMNS), "prediction fields differ")
    row = dict(raw)
    row["trade_date"], row["as_of_date"] = _iso(row["trade_date"]), _iso(row["as_of_date"])
    _require(
        _sha(row["run_id"])
        and row["as_of_date"] < row["trade_date"]
        and row["sector_level"] == "L2"
        and re.fullmatch(r"[0-9]{6}\.SI", row["sector_code"]) is not None
        and row["sector_name"] is None
        and row["name_authority"] == "CANONICAL_CODE_ONLY"
        and type(row["structural_eligible"]) is bool,
        "prediction causal/catalog identity differs",
    )
    p = row["probability"]
    if row["availability"] == "available":
        _require(
            _finite(p)
            and 0 <= p <= 1
            and type(row["warning"]) is bool
            and row["warning"] == (p >= risk.CONTRACT["warning_threshold"])
            and row["reason_code"] is None
            and row["structural_eligible"],
            "available probability/warning differs",
        )
    else:
        _require(
            row["availability"] == "unavailable"
            and p is None
            and row["warning"] is None
            and isinstance(row["reason_code"], str)
            and bool(row["reason_code"].strip()),
            "unavailable must retain null probability/warning and original reason",
        )
    if row["outcome_status"] == "AVAILABLE":
        dd, ret = row["realized_drawdown"], row["realized_return"]
        _require(
            type(row["event"]) is int
            and row["event"] in (0, 1)
            and _finite(dd)
            and -1 <= dd <= 0
            and _finite(ret)
            and ret > -1
            and row["event"] == int(dd <= risk.CONTRACT["drawdown_boundary"]),
            "available outcome/event differs",
        )
    else:
        _require(
            row["outcome_status"] in {"OUTCOME_LEGAL_NA", "OUTCOME_NOT_MATURE"}
            and all(row[k] is None for k in ("event", "realized_drawdown", "realized_return")),
            "legal NA/immature outcome must not become a negative label",
        )
    return row


def _ordered(rows: Sequence[Mapping[str, Any]]) -> list[dict]:
    return sorted((validate_row(r) for r in rows), key=lambda r: (r["trade_date"], r["sector_code"]))


def _validate_day(run: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], day: str) -> list[dict]:
    validated = _ordered(rows)
    _require(
        len(validated) == 131
        and [r["sector_code"] for r in validated] == run["catalog"]
        and all(r["run_id"] == run["run_id"] and r["trade_date"] == day for r in validated)
        and len({r["as_of_date"] for r in validated}) == 1,
        "stored date must contain complete unique same-run 131-sector population",
        REASON_READBACK,
    )
    _require(
        canonical_sha256(validated) == run["compact_summary"]["day_row_hashes"][day],
        "stored date identity differs",
        REASON_READBACK,
    )
    return validated


@dataclass(frozen=True)
class RiskL2Product:
    run: dict
    rows: list[dict]


def product_from_assets(request: Mapping[str, Any], acceptance: dict, sealed: dict, features: dict) -> RiskL2Product:
    """Validate original evidence and reuse its pure evaluator, not a label builder."""
    try:
        _require(request.get("schema_version") == REQUEST_SCHEMA, "unknown import request schema")
        for name, payload in (("acceptance", acceptance), ("sealed", sealed), ("features", features)):
            _receipt(payload, request[name + "_hash"])
        _require(
            acceptance["schema_version"] == risk.VERSION + "_acceptance"
            and sealed["schema_version"] == risk.VERSION + "_sealed"
            and features["schema_version"] == risk.VERSION + "_features"
            and all(p["contract"] == risk.CONTRACT for p in (acceptance, sealed, features)),
            "original schema/contract differs",
        )
        _require(
            acceptance["execution_status"] == "COMPLETED"
            and acceptance["fresh_process_bitwise_equal"] is True
            and type(acceptance["planned_fits"]) is int
            and acceptance["planned_fits"] == 2
            and type(acceptance["completed_fits"]) is int
            and acceptance["completed_fits"] == 2
            and acceptance["tail_accessed"] is False
            and acceptance["database_write"] is False
            and acceptance["runtime_action"] is False
            and acceptance["validation_basis"] == risk.BASIS
            and acceptance["advisory_status"] == "NOT_AVAILABLE"
            and acceptance["research_surface_status"] == "NOT_AVAILABLE",
            "original execution/basis boundary differs",
        )
        model = acceptance["model"]
        _require(
            {k: v for k, v in sealed.items() if k not in {"receipt_sha256", "predictions"}} == model,
            "sealed model differs from acceptance",
        )
        identity = model["input_identity"]
        mapping = {k: identity[k] for k in ("pit_bundle_sha256", "industry_authority_sha256")}
        _require(
            model["feature_sha256"] == request["features_hash"]
            and identity == features["input_identity"]
            and canonical_sha256(model["parameters"]) == model["model_sha256"] == request["model_hash"]
            and canonical_sha256(identity) == request["input_hash"]
            and canonical_sha256(mapping) == request["mapping_hash"]
            and identity["pit_bundle_sha256"] == request["pit_bundle_sha256"]
            and identity["release_identity"]["dataset_manifest_sha256"] == request["dataset_manifest_sha256"]
            and acceptance["executor_commit"] == request["executor_commit"]
            and re.fullmatch(r"[0-9a-f]{40}", request["executor_commit"]) is not None
            and features["feature_names"] == risk.CONTRACT["feature_names"],
            "independent model/input/mapping/source pins differ",
            REASON_CONFLICT,
        )
        catalog, calendar = features["catalog"], features["calendar"]
        _require(len(catalog) == 131 and catalog == sorted(set(catalog)), "catalog must be 131 official codes")
        plan = risk.schedule(calendar)
        by_key = {(r["trade_date"], r["sector_code"]): r for r in sealed["predictions"]}
        labels: dict[str, dict] = {d: {} for d in plan["dev"]}
        rows = []
        for raw in acceptance["result"]["predictions"]:
            day, code = raw["trade_date"], raw["sector_code"]
            _require(day in labels and code in catalog and code not in labels[day], "unknown/duplicate row")
            original = by_key.get((day, code))
            _require(
                original is not None
                and set(raw) == set(original) | {"status", "event", "drawdown", "return"}
                and all(raw.get(k) == v for k, v in original.items()),
                "post-outcome row changed sealed prediction",
            )
            _require(raw["as_of_date"] == plan["as_of"][day], "as-of is not prior frozen open day")
            labels[day][code] = {k: raw[k] for k in ("status", "event", "drawdown", "return")}
            rows.append(
                validate_row(
                    {
                        "run_id": request["acceptance_hash"],
                        "trade_date": day,
                        "as_of_date": raw["as_of_date"],
                        "sector_level": "L2",
                        "sector_code": code,
                        "sector_name": None,
                        "name_authority": "CANONICAL_CODE_ONLY",
                        "probability": raw["probability"],
                        "warning": raw["warning"],
                        "availability": raw["availability"],
                        "reason_code": raw["reason_code"],
                        "structural_eligible": raw["structural_eligible"],
                        "outcome_status": raw["status"],
                        "event": raw["event"],
                        "realized_drawdown": raw["drawdown"],
                        "realized_return": raw["return"],
                    }
                )
            )
        _require(
            len(by_key) == len(sealed["predictions"]) == len(rows) == 131 * len(plan["dev"]),
            "sealed/result population differs",
        )
        recomputed = risk.evaluate(sealed, labels, calendar)
        _require(
            canonical_sha256(recomputed["metrics"]) == canonical_sha256(acceptance["result"]["metrics"])
            and acceptance["result"]["effect_status"] == acceptance["effect_status"] == recomputed["effect_status"],
            "evaluation summary differs",
        )
        rows = _ordered(rows)
        grouped = {day: [] for day in plan["dev"]}
        for row in rows:
            grouped[row["trade_date"]].append(row)
        summary = {
            **recomputed["metrics"],
            "row_hash": canonical_sha256(rows),
            "day_row_hashes": {d: canonical_sha256(rr) for d, rr in grouped.items()},
        }
        run = {
            "run_id": request["acceptance_hash"],
            "acceptance_hash": request["acceptance_hash"],
            "sealed_prediction_hash": request["sealed_hash"],
            "feature_hash": request["features_hash"],
            "model_hash": request["model_hash"],
            "contract_hash": canonical_sha256(risk.CONTRACT),
            "input_hash": request["input_hash"],
            "mapping_hash": request["mapping_hash"],
            "executor_commit": request["executor_commit"],
            "model_version": risk.VERSION,
            "validation_basis": risk.BASIS,
            "catalog": catalog,
            "dates": plan["dev"],
            "input_identity": identity,
            "compact_summary": summary,
            "expected_row_count": len(rows),
            "effect_status": recomputed["effect_status"],
            "risk_l2_capability_status": CAPABILITY if recomputed["effect_status"] == EFFECT else "NOT_AVAILABLE",
            "forward_power_status": "UNAVAILABLE",
            "forward_confirmation": "NOT_STARTED",
            "advisory_status": "NOT_AVAILABLE",
        }
        summary["run_identity_hash"] = _run_hash(run)
        validate_product(RiskL2Product(run, rows))
        return RiskL2Product(run, rows)
    except RiskL2PredictionError:
        raise
    except (FormalStateError, KeyError, TypeError, ValueError, IndexError) as exc:
        raise RiskL2PredictionError(REASON_INPUT, "frozen risk evidence is invalid") from exc


def _run_hash(run: Mapping[str, Any]) -> str:
    return canonical_sha256(
        {**run, "compact_summary": {k: v for k, v in run["compact_summary"].items() if k != "run_identity_hash"}}
    )


def _validate_run(run: Mapping[str, Any]) -> None:
    _require(set(run) == set(RUN_COLUMNS), "run fields differ", REASON_READBACK)
    _require(
        all(_sha(run[k]) for k in RUN_COLUMNS[:8]) and run["run_id"] == run["acceptance_hash"],
        "run identity differs",
        REASON_READBACK,
    )
    _require(
        run["model_version"] == risk.VERSION
        and run["validation_basis"] == risk.BASIS
        and run["contract_hash"] == canonical_sha256(risk.CONTRACT)
        and run["forward_power_status"] == "UNAVAILABLE"
        and run["forward_confirmation"] == "NOT_STARTED"
        and run["advisory_status"] == "NOT_AVAILABLE"
        and run["effect_status"] in {EFFECT, "BELOW_BINDING_RISK_MBE", "EVIDENCE_INSUFFICIENT"}
        and run["risk_l2_capability_status"] == (CAPABILITY if run["effect_status"] == EFFECT else "NOT_AVAILABLE")
        and len(run["catalog"]) == 131
        and run["catalog"] == sorted(set(run["catalog"]))
        and len(run["dates"]) == 424
        and run["dates"] == sorted(set(run["dates"]))
        and run["dates"][0] == risk.DEV_START
        and run["dates"][-1] == risk.DEV_END
        and run["expected_row_count"] == 131 * 424
        and set(run["compact_summary"]["day_row_hashes"]) == set(run["dates"])
        and _run_hash(run) == run["compact_summary"]["run_identity_hash"]
        and canonical_sha256(run["input_identity"]) == run["input_hash"]
        and canonical_sha256({k: run["input_identity"][k] for k in ("pit_bundle_sha256", "industry_authority_sha256")})
        == run["mapping_hash"],
        "stored run schema/summary/state differs",
        REASON_READBACK,
    )


def validate_product(product: RiskL2Product) -> None:
    _validate_run(product.run)
    rows = _ordered(product.rows)
    _require(
        len(rows) == product.run["expected_row_count"]
        and canonical_sha256(rows) == product.run["compact_summary"]["row_hash"],
        "complete row hash differs",
    )
    grouped = {d: [] for d in product.run["dates"]}
    for row in rows:
        _require(row["trade_date"] in grouped, "row outside frozen dates")
        grouped[row["trade_date"]].append(row)
    for day, items in grouped.items():
        _validate_day(product.run, items, day)


def load_product(request_path: Path) -> RiskL2Product:
    try:
        request, stamp = _read(request_path)
        assets, stamps = {}, {}
        for name in ("acceptance", "sealed", "features"):
            path = Path(request[name + "_path"])
            assets[name], stamps[name] = _read(path)
        result = product_from_assets(request, **assets)
        for name in assets:
            _stable(Path(request[name + "_path"]), stamps[name])
        _stable(request_path, stamp)
        return result
    except RiskL2PredictionError:
        raise
    except (OSError, UnicodeError, ValueError, KeyError, TypeError) as exc:
        raise RiskL2PredictionError(REASON_INPUT, "explicit risk import artifact read failed") from exc


def _decode(raw: Sequence, columns: Sequence[str]) -> dict:
    _require(len(raw) == len(columns), "database fields differ", REASON_READBACK)
    row = dict(zip(columns, raw, strict=True))
    for key in JSON_COLUMNS & row.keys():
        if isinstance(row[key], str):
            row[key] = json.loads(row[key])
    for key in ("trade_date", "as_of_date"):
        if key in row:
            row[key] = _iso(row[key])
    return row


def _params(row: Mapping[str, Any], columns: Sequence[str]) -> tuple:
    return tuple(canonical_json_bytes(row[k]).decode("utf-8") if k in JSON_COLUMNS else row[k] for k in columns)


def _read_run(cursor: Any, run_id: str) -> dict:
    cursor.execute(f"SELECT {','.join(RUN_COLUMNS)} FROM hmm_risk.risk_l2_run WHERE run_id=%s", (run_id,))
    raw = cursor.fetchone()
    if raw is None:
        raise RiskL2PredictionError(REASON_NOT_FOUND, "explicit L2 risk run not found")
    result = _decode(raw, RUN_COLUMNS)
    _validate_run(result)
    return result


@dataclass
class RiskL2PredictionRepository:
    conn_factory: Callable[[], AbstractContextManager[Any]] = lambda: get_conn(
        autocommit=False, manage_transaction=True
    )
    surface_validation_receipt_path: Path | None = None
    deployment_commit: str | None = None
    runtime_target: str = "backend-main"

    def write_product(self, product: RiskL2Product, *, database_target: str | None = None) -> dict:
        _require(
            database_target in {"aistock_dev", "aistock"},
            "write requires an explicit existing database target",
            REASON_WRITER,
        )
        validate_product(product)
        run_id = product.run["run_id"]
        try:
            with self.conn_factory() as conn:
                _require(
                    conn.info.dbname == database_target, "database differs from explicit write target", REASON_WRITER
                )
                with conn.cursor() as cursor:
                    # One per-run lock serializes creation/idempotence, not a parallel writer.
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("hmm_risk.risk_l2:" + run_id,)
                    )
                    cursor.execute("SELECT run_id FROM hmm_risk.risk_l2_run WHERE run_id=%s", (run_id,))
                    exists = cursor.fetchone() is not None
                    if not exists:
                        cursor.execute(
                            f"INSERT INTO hmm_risk.risk_l2_run ({','.join(RUN_COLUMNS)}) VALUES ({','.join(['%s'] * len(RUN_COLUMNS))})",
                            _params(product.run, RUN_COLUMNS),
                        )
                        cursor.executemany(
                            f"INSERT INTO hmm_risk.risk_l2_prediction ({','.join(ROW_COLUMNS)}) VALUES ({','.join(['%s'] * len(ROW_COLUMNS))})",
                            [_params(r, ROW_COLUMNS) for r in _ordered(product.rows)],
                        )
                    stored = _read_run(cursor, run_id)
                    cursor.execute(
                        f"SELECT {','.join(ROW_COLUMNS)} FROM hmm_risk.risk_l2_prediction WHERE run_id=%s ORDER BY trade_date,sector_code",
                        (run_id,),
                    )
                    rows = [_decode(raw, ROW_COLUMNS) for raw in cursor.fetchall()]
                    validate_product(RiskL2Product(stored, rows))
                    _require(
                        canonical_sha256(stored) == canonical_sha256(product.run)
                        and canonical_sha256(_ordered(rows)) == product.run["compact_summary"]["row_hash"],
                        "existing run conflicts or transactional readback differs",
                        REASON_CONFLICT,
                    )
                    # All comparisons stay BEFORE connection context commits.
            return {
                "run_id": run_id,
                "row_count": len(rows),
                "row_hash": stored["compact_summary"]["row_hash"],
                "inserted_rows": 0 if exists else len(rows),
                "idempotency_verified": True,
            }
        except RiskL2PredictionError:
            raise
        except Exception as exc:
            raise RiskL2PredictionError(REASON_WRITER, "L2 risk transaction failed") from exc

    def _surface(self, run: Mapping[str, Any]) -> str:
        path = self.surface_validation_receipt_path
        if path is None:
            return "NOT_AVAILABLE"
        try:
            try:
                path.lstat()
            except FileNotFoundError:
                return "NOT_AVAILABLE"
            receipt, stamp = _read(path)
            verify_receipt(receipt)
            expected = {k: run[k] for k in ("run_id", "model_hash", "acceptance_hash", "input_hash")}
            expected.update(
                row_hash=run["compact_summary"]["row_hash"],
                schema_version=SURFACE_SCHEMA,
                deployment_commit=self.deployment_commit,
                target=self.runtime_target,
            )
            zero_dates = [d["trade_date"] for d in run["compact_summary"]["daily"] if d["warning_count"] == 0]
            required_dates = {run["dates"][0], run["dates"][-1]}
            if zero_dates:
                required_dates.add(zero_dates[0])
            _require(
                self.deployment_commit is not None
                and all(receipt.get(k) == v for k, v in expected.items())
                and all(receipt.get(k) is True for k in ("writer_readback", "api_readback", "browser_no_mock"))
                and isinstance(receipt.get("day_row_hashes"), dict)
                and required_dates <= receipt["day_row_hashes"].keys()
                and all(
                    run["compact_summary"]["day_row_hashes"].get(k) == v for k, v in receipt["day_row_hashes"].items()
                ),
                "product validation receipt differs",
                REASON_READBACK,
            )
            _stable(path, stamp)
            return "AVAILABLE_EXPERIMENTAL"
        except Exception as exc:
            raise RiskL2PredictionError(REASON_READBACK, "existing product receipt is invalid") from exc

    def read_date(self, trade_date: date, *, run_id: str) -> dict:
        _require(_sha(run_id), "explicit run id must be SHA-256", REASON_CONFLICT)
        day = _iso(trade_date)
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cursor:
                    run = _read_run(cursor, run_id)
                    if day not in run["dates"]:
                        raise RiskL2PredictionError(REASON_NOT_FOUND, "explicit L2 risk date not found")
                    cursor.execute(
                        f"SELECT {','.join(ROW_COLUMNS)} FROM hmm_risk.risk_l2_prediction WHERE run_id=%s AND trade_date=%s ORDER BY sector_code",
                        (run_id, day),
                    )
                    rows = _validate_day(run, [_decode(raw, ROW_COLUMNS) for raw in cursor.fetchall()], day)
            counts = Counter(r["availability"] for r in rows)
            summary = {
                "sector_count": 131,
                "available_count": counts["available"],
                "unavailable_count": counts["unavailable"],
                "warning_count": sum(r["warning"] is True for r in rows),
                "unknown_warning_count": sum(r["warning"] is None for r in rows),
                "outcome_status_counts": dict(Counter(r["outcome_status"] for r in rows)),
            }
            return {
                **{k: v for k, v in run.items() if k not in {"catalog", "expected_row_count"}},
                "trade_date": day,
                "as_of_date": rows[0]["as_of_date"],
                "rows": rows,
                "day_summary": summary,
                "research_surface_status": self._surface(run),
                "tail_accessed": False,
            }
        except RiskL2PredictionError:
            raise
        except Exception as exc:
            raise RiskL2PredictionError(REASON_READBACK, "L2 risk database read failed") from exc

    def overview(self, *, run_id: str) -> dict:
        _require(_sha(run_id), "explicit run id must be SHA-256", REASON_CONFLICT)
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cursor:
                    run = _read_run(cursor, run_id)
            detail = self.read_date(date.fromisoformat(run["dates"][-1]), run_id=run_id)
            return {k: v for k, v in detail.items() if k != "rows"}
        except RiskL2PredictionError:
            raise
        except Exception as exc:
            raise RiskL2PredictionError(REASON_READBACK, "L2 risk overview read failed") from exc
