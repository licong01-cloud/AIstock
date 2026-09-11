"""Run one approved HMM rotation-L1 prediction from explicit frozen assets.

The default mode is a zero-write dry run.  ``--write-database`` is available
only for a separately authorised operation and requires an exact database name
that is verified on the opened connection before any prediction row is sent.
This entry never fits a model, reads a target, searches for a latest release,
or activates a runtime service.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.dataset_release.cas_store import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.rotation_l1_gbdt import (  # noqa: E402
    REASON_INPUT,
    V16_CONTRACT_VERSION,
    RotationL1G2AError,
    canonical_sha256,
    close_processes,
    read_input_bundle,
    validate_v14_process_reference,
)
from backend.services.hmm_risk.rotation_l1_prediction import (  # noqa: E402
    PREDICTION_COLUMNS,
    RotationL1PredictionRepository,
    predict_single_date_from_assets,
)

RECEIPT_SCHEMA_VERSION = "hmm_risk_rotation_l1_single_date_execution_v1"
FAILURE_SCHEMA_VERSION = "hmm_risk_rotation_l1_single_date_execution_failure_v1"
REASON_EXECUTOR_INPUT = "hmm_risk_rotation_product_executor_input_invalid"
REASON_EXECUTOR_AUTHORITY = "hmm_risk_rotation_product_executor_authority_mismatch"
REASON_EXECUTOR_DATABASE = "hmm_risk_rotation_product_executor_database_target_mismatch"
REASON_EXECUTOR_RESULT = "hmm_risk_rotation_product_executor_result_invalid"
RESEARCH_CAPABILITY = "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"


class RotationL1ProductExecutorError(RuntimeError):
    """Typed failure at the formal product-execution boundary."""

    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


def _load_external_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_absolute() or path.is_symlink():
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, f"{label} must be an absolute regular file")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, f"{label} cannot be resolved") from exc
    if not resolved.is_file():
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, f"{label} must be an absolute regular file")
    try:
        resolved.relative_to(ROOT.resolve(strict=True))
    except ValueError:
        pass
    else:
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, f"{label} must be outside the repository")
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, f"{label} cannot be read") from exc
    if not isinstance(value, dict):
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, f"{label} must be a JSON object")
    return value


def _external_path(path: Path, *, label: str, existing_directory: bool = False) -> Path:
    if not path.is_absolute() or path.is_symlink():
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, f"{label} must be an absolute direct path")
    try:
        resolved = path.resolve(strict=existing_directory)
    except OSError as exc:
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, f"{label} cannot be resolved") from exc
    try:
        resolved.relative_to(ROOT.resolve(strict=True))
    except ValueError:
        pass
    else:
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, f"{label} must be outside the repository")
    if existing_directory and not resolved.is_dir():
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, f"{label} must be an existing directory")
    return resolved


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(value) + b"\n")
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
    return [
        {column: _json_value(row.get(column)) for column in PREDICTION_COLUMNS if column != "prediction_id"}
        for row in rows
    ]


def _validated_authority(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], Mapping[str, Any], dict[str, Any]]:
    acceptance = _load_external_object(args.development_acceptance, label="development acceptance")
    if (
        len(args.development_acceptance_sha256) != 64
        or any(character not in "0123456789abcdef" for character in args.development_acceptance_sha256)
        or acceptance.get("acceptance_sha256") != args.development_acceptance_sha256
    ):
        raise RotationL1ProductExecutorError(
            REASON_EXECUTOR_AUTHORITY,
            "explicit development acceptance hash differs",
        )
    first = _load_external_object(args.development_child_1, label="development child 1")
    second = _load_external_object(args.development_child_2, label="development child 2")
    v14_reference = _load_external_object(args.v14_process_file, label="v1.4 process reference")
    try:
        validate_v14_process_reference(v14_reference)
        input_bundle = read_input_bundle(args.input_root, forbidden_roots=(ROOT,))["bundle"]
        recomputed = close_processes(
            first,
            second,
            v14_reference=v14_reference,
            input_bundle=input_bundle,
        )
    except (KeyError, RotationL1G2AError) as exc:
        raise RotationL1ProductExecutorError(
            str(getattr(exc, "reason_code", REASON_EXECUTOR_AUTHORITY)),
            "frozen development authority validation failed",
            context=getattr(exc, "evidence", None),
        ) from exc
    if recomputed != acceptance:
        raise RotationL1ProductExecutorError(
            REASON_EXECUTOR_AUTHORITY,
            "development acceptance does not match the two fresh-process authorities",
        )
    payload = first.get("reproducibility_payload")
    if not isinstance(payload, Mapping):
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_AUTHORITY, "development payload is missing")
    tail_gate = payload.get("tail_access_gate")
    if (
        payload.get("contract_version") != V16_CONTRACT_VERSION
        or acceptance.get("contract_version") != V16_CONTRACT_VERSION
        or acceptance.get("status") != "development_complete"
        or acceptance.get("tail_accessed") is not False
        or not isinstance(tail_gate, Mapping)
        or tail_gate.get("passed") is not True
        or payload.get("tail_accessed") is not False
        or payload.get("database_write_performed") is not False
        or payload.get("runtime_action_performed") is not False
    ):
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_AUTHORITY, "v1.6 development authority state differs")
    return first, payload, acceptance


def _forward_state(payload: Mapping[str, Any]) -> tuple[str, str, str]:
    power = str(payload.get("forward_power_status"))
    if power == "INSUFFICIENT":
        confirmation = "PENDING_INSUFFICIENT_POWER"
    elif power in {"UNAVAILABLE", "SUFFICIENT"}:
        confirmation = "PENDING_INCONCLUSIVE"
    else:
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_AUTHORITY, "forward power authority differs")
    return RESEARCH_CAPABILITY, power, confirmation


@contextmanager
def _database_connection(expected_database: str) -> Iterator[Any]:
    from backend.db.pg_pool import get_conn

    with get_conn(autocommit=False, manage_transaction=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            raw = cursor.fetchone()
        actual = str(raw[0]) if raw and raw[0] is not None else ""
        if actual != expected_database:
            raise RotationL1ProductExecutorError(
                REASON_EXECUTOR_DATABASE,
                "connected database does not match the explicitly authorised target",
                context={"expected_database": expected_database, "actual_database": actual},
            )
        yield conn


def _repository(expected_database: str) -> RotationL1PredictionRepository:
    return RotationL1PredictionRepository(conn_factory=lambda: _database_connection(expected_database))


def execute(args: argparse.Namespace) -> dict[str, Any]:
    output = _external_path(args.output_receipt, label="output receipt")
    if output.exists():
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_INPUT, "output receipt already exists")
    work_parent = _external_path(args.work_parent, label="work parent", existing_directory=True)
    if args.write_database and not args.expected_database_name:
        raise RotationL1ProductExecutorError(
            REASON_EXECUTOR_INPUT,
            "--expected-database-name is required with --write-database",
        )
    if not args.write_database and args.expected_database_name:
        raise RotationL1ProductExecutorError(
            REASON_EXECUTOR_INPUT,
            "--expected-database-name is forbidden in dry-run mode",
        )
    first, payload, acceptance = _validated_authority(args)
    industry_authority = _load_external_object(args.industry_pit_authority, label="industry PIT authority")
    final_model = payload.get("final_model")
    model_profile = payload.get("profile")
    development_summary = payload.get("development_summary")
    model_text = first.get("final_model_text")
    if not all(
        isinstance(value, Mapping) for value in (final_model, model_profile, development_summary)
    ) or not isinstance(model_text, str):
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_AUTHORITY, "frozen model authority is incomplete")
    capability, power, confirmation = _forward_state(payload)
    result = predict_single_date_from_assets(
        direct_v2_candidate_root=args.direct_v2_candidate_root,
        security_identity_manifest=args.security_identity_manifest,
        provider_absence_manifest=args.provider_absence_manifest,
        industry_authority=industry_authority,
        forbidden_roots=(ROOT,),
        work_parent=work_parent,
        trade_date=args.trade_date,
        as_of_date=args.as_of_date,
        model_text=model_text,
        final_model=final_model,
        model_profile=model_profile,
        development_summary=development_summary,
        capability_status=capability,
        forward_power_status=power,
        forward_confirmation=confirmation,
    )
    rows = result.get("rows")
    source_receipt = result.get("source_receipt")
    if not isinstance(rows, list) or len(rows) != 31 or not isinstance(source_receipt, Mapping):
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_RESULT, "single-date result is incomplete")
    row_dates = {row.get("trade_date") for row in rows if isinstance(row, Mapping)}
    as_of_dates = {row.get("as_of_date") for row in rows if isinstance(row, Mapping)}
    model_hashes = {row.get("model_hash") for row in rows if isinstance(row, Mapping)}
    input_hashes = {row.get("input_hash") for row in rows if isinstance(row, Mapping)}
    mapping_hashes = {row.get("mapping_snapshot_hash") for row in rows if isinstance(row, Mapping)}
    sector_codes = {row.get("sector_code") for row in rows if isinstance(row, Mapping)}
    if (
        row_dates != {args.trade_date}
        or as_of_dates != {args.as_of_date}
        or len(model_hashes) != 1
        or len(input_hashes) != 1
        or len(mapping_hashes) != 1
        or len(sector_codes) != 31
        or any(row.get("rotation_l1_capability_status") != capability for row in rows)
        or any(row.get("forward_power_status") != power for row in rows)
        or any(row.get("forward_confirmation") != confirmation for row in rows)
        or any(row.get("validation_basis") != "single_date_frozen_model" for row in rows)
        or any(row.get("tail_accessed") is not False for row in rows)
    ):
        raise RotationL1ProductExecutorError(REASON_EXECUTOR_RESULT, "single-date row identity differs")

    write_receipt: Mapping[str, Any] | None = None
    database_write_performed = False
    if args.write_database:
        write_receipt = _repository(args.expected_database_name).write_rows(rows)
        database_write_performed = True

    canonical_row_sha256 = canonical_sha256(_canonical_rows(rows))
    if write_receipt is not None and (
        write_receipt.get("row_count") != 31
        or write_receipt.get("canonical_row_sha256") != canonical_row_sha256
        or write_receipt.get("idempotency_verified") is not True
    ):
        raise RotationL1ProductExecutorError(
            REASON_EXECUTOR_RESULT,
            "database write/readback receipt differs",
            context={"database_write_performed": True},
        )
    body = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "status": "complete",
        "mode": "database_write" if args.write_database else "dry_run",
        "trade_date": args.trade_date.isoformat(),
        "as_of_date": args.as_of_date.isoformat(),
        "row_count": len(rows),
        "available_count": sum(row.get("availability") == "available" for row in rows),
        "sector_count": len(sector_codes),
        "model_sha256": next(iter(model_hashes)),
        "input_sha256": next(iter(input_hashes)),
        "mapping_snapshot_sha256": next(iter(mapping_hashes)),
        "canonical_row_sha256": canonical_row_sha256,
        "source_receipt_sha256": source_receipt.get("receipt_sha256"),
        "development_acceptance_sha256": args.development_acceptance_sha256,
        "development_process_sha256s": list(acceptance["child_sha256s"]),
        "rotation_l1_capability_status": capability,
        "forward_power_status": power,
        "forward_confirmation": confirmation,
        "tail_accessed": False,
        "target_columns_read": False,
        "model_fit_count": 0,
        "database_target": args.expected_database_name if args.write_database else None,
        "database_write_performed": database_write_performed,
        "database_write_receipt": dict(write_receipt) if write_receipt is not None else None,
        "runtime_action_performed": False,
    }
    receipt = {**body, "receipt_sha256": canonical_sha256(body)}
    try:
        _write_once(output, receipt)
    except Exception as exc:
        raise RotationL1ProductExecutorError(
            REASON_EXECUTOR_RESULT,
            "execution receipt cannot be persisted",
            context={"database_write_performed": database_write_performed},
        ) from exc
    return receipt


def _failure(error: BaseException) -> dict[str, Any]:
    reason = str(getattr(error, "reason_code", REASON_INPUT))
    context = getattr(error, "context", None)
    body = {
        "schema_version": FAILURE_SCHEMA_VERSION,
        "status": "failed",
        "reason_code": reason,
        "message": str(error),
        "context": dict(context) if isinstance(context, Mapping) else {"exception_type": type(error).__name__},
        "tail_accessed": False,
        "target_columns_read": False,
        "model_fit_count": 0,
        "database_write_performed": bool(context.get("database_write_performed"))
        if isinstance(context, Mapping)
        else False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _write_failure(output: Path, error: BaseException) -> Path:
    resolved_output = _external_path(output, label="failure receipt target")
    parent = resolved_output.parent
    parent.mkdir(parents=True, exist_ok=True)
    path = parent / f".{resolved_output.name}.failure.{uuid.uuid4().hex}.json"
    _write_once(path, _failure(error))
    return path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--development-acceptance", type=Path, required=True)
    parser.add_argument("--development-acceptance-sha256", required=True)
    parser.add_argument("--development-child-1", type=Path, required=True)
    parser.add_argument("--development-child-2", type=Path, required=True)
    parser.add_argument("--v14-process-file", type=Path, required=True)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--direct-v2-candidate-root", type=Path, required=True)
    parser.add_argument("--security-identity-manifest", type=Path, required=True)
    parser.add_argument("--provider-absence-manifest", type=Path, required=True)
    parser.add_argument("--industry-pit-authority", type=Path, required=True)
    parser.add_argument("--trade-date", type=date.fromisoformat, required=True)
    parser.add_argument("--as-of-date", type=date.fromisoformat, required=True)
    parser.add_argument("--work-parent", type=Path, required=True)
    parser.add_argument("--output-receipt", type=Path, required=True)
    parser.add_argument("--write-database", action="store_true")
    parser.add_argument("--expected-database-name")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        receipt = execute(args)
    except Exception as exc:
        try:
            failure_path = _write_failure(args.output_receipt, exc)
        except Exception:
            failure_path = None
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": str(getattr(exc, "reason_code", REASON_INPUT)),
                    "failure_receipt": str(failure_path) if failure_path is not None else None,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
