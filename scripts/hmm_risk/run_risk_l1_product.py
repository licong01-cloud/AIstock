"""Materialise approved G2-B research predictions or run one zero-fit date.

The default is a zero-write dry run. Database writes require both
``--write-database`` and an exact database name, verified on the opened
connection before any row is sent. This entry never reads a tail or fits a
model.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date, datetime
import json
import os
from pathlib import Path
import sys
from typing import Any, Iterator, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.dataset_release.cas_store import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.risk_l1_prediction import (  # noqa: E402
    PREDICTION_COLUMNS,
    RiskL1PredictionRepository,
    build_oof_prediction_rows,
    predict_single_date_from_assets,
)
from backend.services.hmm_risk.risk_l1_g2b import close_processes  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_sha256  # noqa: E402

RECEIPT_SCHEMA_VERSION = "hmm_risk_risk_l1_product_execution_v1"
FAILURE_SCHEMA_VERSION = "hmm_risk_risk_l1_product_execution_failure_v1"
REASON_INPUT = "hmm_risk_risk_l1_product_executor_input_invalid"
REASON_AUTHORITY = "hmm_risk_risk_l1_product_executor_authority_mismatch"
REASON_DATABASE = "hmm_risk_risk_l1_product_executor_database_target_mismatch"
REASON_RESULT = "hmm_risk_risk_l1_product_executor_result_invalid"


class RiskL1ProductExecutorError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


def _external_file(path: Path, *, label: str) -> Path:
    if not path.is_absolute() or path.is_symlink():
        raise RiskL1ProductExecutorError(REASON_INPUT, f"{label} must be an absolute regular file")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise RiskL1ProductExecutorError(REASON_INPUT, f"{label} cannot be resolved") from exc
    if not resolved.is_file():
        raise RiskL1ProductExecutorError(REASON_INPUT, f"{label} is not a regular file")
    try:
        resolved.relative_to(ROOT.resolve(strict=True))
    except ValueError:
        return resolved
    raise RiskL1ProductExecutorError(REASON_INPUT, f"{label} must be outside the repository")


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    resolved = _external_file(path, label=label)
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RiskL1ProductExecutorError(REASON_INPUT, f"{label} cannot be read") from exc
    if not isinstance(value, dict):
        raise RiskL1ProductExecutorError(REASON_INPUT, f"{label} must be a JSON object")
    return value


def _new_external_output(path: Path) -> Path:
    if not path.is_absolute() or path.is_symlink():
        raise RiskL1ProductExecutorError(REASON_INPUT, "output receipt must be an absolute direct path")
    parent = path.parent.resolve(strict=True)
    resolved = parent / path.name
    try:
        resolved.relative_to(ROOT.resolve(strict=True))
    except ValueError:
        pass
    else:
        raise RiskL1ProductExecutorError(REASON_INPUT, "output receipt must be outside the repository")
    if resolved.exists() or resolved.is_symlink():
        raise RiskL1ProductExecutorError(REASON_INPUT, "output receipt already exists")
    return resolved


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(dict(value)) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())


def _json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _canonical_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(
        rows,
        key=lambda row: (str(row["model_hash"]), row["trade_date"], str(row["sector_code"]), row["revision"]),
    )
    return [
        {column: _json_value(row.get(column)) for column in PREDICTION_COLUMNS if column != "prediction_id"}
        for row in ordered
    ]


@contextmanager
def _database_connection(expected_database: str) -> Iterator[Any]:
    from backend.db.pg_pool import get_conn

    with get_conn(autocommit=False, manage_transaction=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            raw = cursor.fetchone()
        actual = str(raw[0]) if raw and raw[0] is not None else ""
        if actual != expected_database:
            raise RiskL1ProductExecutorError(
                REASON_DATABASE,
                "connected database does not match the explicitly authorised target",
                context={"expected_database": expected_database, "actual_database": actual},
            )
        yield conn


def _repository(expected_database: str) -> RiskL1PredictionRepository:
    return RiskL1PredictionRepository(conn_factory=lambda: _database_connection(expected_database))


def _validate_write_args(args: argparse.Namespace) -> None:
    if args.write_database and not args.expected_database_name:
        raise RiskL1ProductExecutorError(REASON_INPUT, "--expected-database-name is required with --write-database")
    if not args.write_database and args.expected_database_name:
        raise RiskL1ProductExecutorError(REASON_INPUT, "--expected-database-name is forbidden in dry-run mode")


def _oof_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    acceptance = _load_object(args.development_acceptance, label="development acceptance")
    first = _load_object(args.development_child_1, label="development child 1")
    second = _load_object(args.development_child_2, label="development child 2")
    names = _load_object(args.sector_names, label="sector names")
    if acceptance.get("acceptance_sha256") != args.development_acceptance_sha256:
        raise RiskL1ProductExecutorError(REASON_AUTHORITY, "explicit development acceptance hash differs")
    try:
        return build_oof_prediction_rows(
            acceptance=acceptance,
            process_reports=(first, second),
            sector_names={str(key): str(value) for key, value in names.items()},
        )
    except Exception as exc:
        raise RiskL1ProductExecutorError(
            str(getattr(exc, "reason_code", REASON_AUTHORITY)), "development authority validation failed"
        ) from exc


def _single_date_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    model = _load_object(args.model_artifact, label="model artifact")
    acceptance = _load_object(args.development_acceptance, label="development acceptance")
    first = _load_object(args.development_child_1, label="development child 1")
    second = _load_object(args.development_child_2, label="development child 2")
    authority = _load_object(args.industry_pit_authority, label="industry PIT authority")
    try:
        recomputed_acceptance, recomputed_model = close_processes(first, second)
    except Exception as exc:
        raise RiskL1ProductExecutorError(
            str(getattr(exc, "reason_code", REASON_AUTHORITY)), "risk development authority validation failed"
        ) from exc
    if (
        model.get("artifact_sha256") != args.model_artifact_sha256
        or acceptance.get("acceptance_sha256") != args.development_acceptance_sha256
        or model != recomputed_model
        or acceptance != recomputed_acceptance
    ):
        raise RiskL1ProductExecutorError(REASON_AUTHORITY, "single-date model/acceptance authority differs")
    result = predict_single_date_from_assets(
        direct_v2_candidate_root=args.candidate_root,
        security_identity_manifest=args.security_identity_manifest,
        provider_absence_manifest=args.provider_absence_manifest,
        industry_authority=authority,
        forbidden_roots=(ROOT,),
        work_parent=args.work_parent,
        trade_date=args.trade_date,
        as_of_date=args.as_of_date,
        model_artifact=model,
    )
    if (
        result.get("model_fit_count") != 0
        or result.get("target_columns_read") is not False
        or result.get("tail_accessed") is not False
    ):
        raise RiskL1ProductExecutorError(REASON_RESULT, "single-date execution crossed its zero-fit boundary")
    return list(result["rows"])


def execute(args: argparse.Namespace) -> dict[str, Any]:
    _validate_write_args(args)
    output = _new_external_output(args.output_receipt)
    rows = _oof_rows(args) if args.mode == "oof" else _single_date_rows(args)
    if not rows or len(rows) % 31 != 0:
        raise RiskL1ProductExecutorError(REASON_RESULT, "risk product result does not contain complete 31-row batches")
    write_receipt: Mapping[str, Any] | None = None
    if args.write_database:
        write_receipt = _repository(args.expected_database_name).write_rows(rows)
    canonical_row_sha256 = canonical_sha256(_canonical_rows(rows))
    if write_receipt is not None and (
        write_receipt.get("row_count") != len(rows)
        or write_receipt.get("canonical_row_sha256") != canonical_row_sha256
        or write_receipt.get("idempotency_verified") is not True
    ):
        raise RiskL1ProductExecutorError(
            REASON_RESULT, "risk database write/readback differs", context={"database_write_performed": True}
        )
    body = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "status": "complete",
        "execution_kind": args.mode,
        "mode": "database_write" if args.write_database else "dry_run",
        "row_count": len(rows),
        "date_count": len({row["trade_date"] for row in rows}),
        "available_count": sum(row["availability"] == "available" for row in rows),
        "canonical_row_sha256": canonical_row_sha256,
        "model_sha256s": sorted({str(row["model_hash"]) for row in rows}),
        "tail_accessed": False,
        "target_columns_read_for_single_date": False if args.mode == "single-date" else None,
        "model_fit_count": 0,
        "database_target": args.expected_database_name if args.write_database else None,
        "database_write_performed": bool(args.write_database),
        "database_write_receipt": dict(write_receipt) if write_receipt is not None else None,
        "runtime_action_performed": False,
    }
    receipt = {**body, "receipt_sha256": canonical_sha256(body)}
    _write_once(output, receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    oof = subparsers.add_parser("oof")
    oof.add_argument("--development-acceptance", type=Path, required=True)
    oof.add_argument("--development-acceptance-sha256", required=True)
    oof.add_argument("--development-child-1", type=Path, required=True)
    oof.add_argument("--development-child-2", type=Path, required=True)
    oof.add_argument("--sector-names", type=Path, required=True)
    single = subparsers.add_parser("single-date")
    single.add_argument("--development-acceptance", type=Path, required=True)
    single.add_argument("--development-acceptance-sha256", required=True)
    single.add_argument("--development-child-1", type=Path, required=True)
    single.add_argument("--development-child-2", type=Path, required=True)
    single.add_argument("--model-artifact", type=Path, required=True)
    single.add_argument("--model-artifact-sha256", required=True)
    single.add_argument("--candidate-root", type=Path, required=True)
    single.add_argument("--security-identity-manifest", type=Path, required=True)
    single.add_argument("--provider-absence-manifest", type=Path, required=True)
    single.add_argument("--industry-pit-authority", type=Path, required=True)
    single.add_argument("--work-parent", type=Path, required=True)
    single.add_argument("--trade-date", type=date.fromisoformat, required=True)
    single.add_argument("--as-of-date", type=date.fromisoformat, required=True)
    for command in (oof, single):
        command.add_argument("--output-receipt", type=Path, required=True)
        command.add_argument("--write-database", action="store_true")
        command.add_argument("--expected-database-name")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = execute(args)
    except Exception as exc:
        print(
            json.dumps(
                {"status": "failed", "reason_code": str(getattr(exc, "reason_code", REASON_RESULT)), "message": str(exc)},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
