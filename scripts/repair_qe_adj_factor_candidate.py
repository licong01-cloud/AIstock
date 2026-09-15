"""Build a new immutable QE candidate from an adj-factor repair inventory.

This CLI is candidate-only.  It opens production in a read-only transaction,
never changes the active profile, and never controls a service process.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg2
from dotenv import dotenv_values

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.dataset_release.adj_factor_candidate_repair import (  # noqa: E402
    AdjFactorCandidateRepairError,
    build_repair_receipt,
    clone_baseline_copy_on_write,
    patch_qlib_bins,
    rebuild_daily_pv_h5,
    rebuild_static_factors,
    scan_inventory,
    sha256_bytes,
    sha256_file,
    validate_inventory,
    verify_zero_factor_drift,
    write_canonical_json_atomic,
    canonical_json_bytes,
)


STATE_SCHEMA = "qe_adj_factor_candidate_build_state_v1"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-root", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--production-job-id", required=True)
    parser.add_argument("--production-job-snapshot-sha256", required=True)
    parser.add_argument("--active-profile", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 8, 1))
    parser.add_argument("--cutoff", type=date.fromisoformat, default=date(2026, 8, 31))
    parser.add_argument("--env-file", type=Path, default=REPOSITORY_ROOT / ".env")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--resume", action="store_true")
    return parser


def _db_config(path: Path) -> dict[str, Any]:
    values = {str(key): str(value) for key, value in dotenv_values(path).items() if value is not None}
    required = ["TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"]
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise AdjFactorCandidateRepairError("database environment is incomplete: " + ",".join(missing))
    if values["TDX_DB_NAME"] != "aistock" or int(values["TDX_DB_PORT"]) != 5432:
        raise AdjFactorCandidateRepairError("candidate repair requires the configured production aistock target")
    return {
        "host": values["TDX_DB_HOST"],
        "port": int(values["TDX_DB_PORT"]),
        "dbname": values["TDX_DB_NAME"],
        "user": values["TDX_DB_USER"],
        "password": values["TDX_DB_PASSWORD"],
        "application_name": "qe-adj-factor-candidate-repair-readonly",
        "connect_timeout": 10,
    }


def _connect_readonly(path: Path):
    connection = psycopg2.connect(**_db_config(path))
    connection.set_session(readonly=True, autocommit=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database(),current_setting('transaction_read_only')")
        database, read_only = cursor.fetchone()
    if database != "aistock" or read_only != "on":
        connection.close()
        raise AdjFactorCandidateRepairError("production read-only session identity differs")
    return connection


def _verify_job(connection: Any, job_id: str, expected_snapshot: str) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT status,summary->'history_reconciliation'
            FROM market.ingestion_jobs WHERE job_id=%s
            """,
            (job_id,),
        )
        row = cursor.fetchone()
    if row is None or row[0] != "success" or not isinstance(row[1], Mapping):
        raise AdjFactorCandidateRepairError("production reconciliation job is unavailable or unsuccessful")
    history = row[1]
    if (
        history.get("status") != "reconciled"
        or history.get("database_write_performed") is not True
        or str(history.get("provider_snapshot_sha256")) != expected_snapshot
        or int(history.get("scanned_symbol_count", 0)) <= 0
    ):
        raise AdjFactorCandidateRepairError("production reconciliation job identity differs")
    return {
        "job_id": job_id,
        "status": history["status"],
        "scanned_symbol_count": int(history["scanned_symbol_count"]),
        "changed_symbol_count": int(history["changed_symbol_count"]),
        "written_row_count": int(history["written_row_count"]),
        "provider_snapshot_sha256": str(history["provider_snapshot_sha256"]),
    }


def _load_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdjFactorCandidateRepairError(f"JSON artifact is unreadable: {path}") from exc
    if not isinstance(value, Mapping):
        raise AdjFactorCandidateRepairError(f"JSON artifact root differs: {path}")
    return value


def _canonical_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    unsigned = dict(value)
    unsigned.pop("canonical_sha256", None)
    return {**unsigned, "canonical_sha256": sha256_bytes(canonical_json_bytes(unsigned))}


def _write_state(path: Path, *, status: str, stages: Mapping[str, Any], error: str | None = None) -> None:
    write_canonical_json_atomic(
        path,
        _canonical_payload(
            {
                "schema_version": STATE_SCHEMA,
                "status": status,
                "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "stages": dict(stages),
                "error": error,
                "production_activation": False,
                "database_write_performed": False,
                "runtime_action_performed": False,
            }
        ),
    )


def _require_new_or_resume(candidate: Path, *, resume: bool) -> Path:
    resolved = candidate.expanduser().resolve(strict=False)
    if resume:
        if not resolved.is_dir() or not (resolved / "reports" / "adj_factor_build_state.json").is_file():
            raise AdjFactorCandidateRepairError("--resume requires this producer's existing candidate state")
    elif resolved.exists():
        raise AdjFactorCandidateRepairError("candidate root already exists; use a new version or explicit --resume")
    else:
        resolved.mkdir(parents=True)
    return resolved


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    baseline = args.baseline_root.expanduser().resolve(strict=True)
    candidate = _require_new_or_resume(args.candidate_root, resume=bool(args.resume))
    if candidate.parent != baseline.parent or candidate == baseline:
        raise AdjFactorCandidateRepairError("baseline and candidate must be different siblings on X drive")
    active_profile = args.active_profile.expanduser().resolve(strict=True)
    active_profile_sha = sha256_file(active_profile)
    reports = candidate / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    state_path = reports / "adj_factor_build_state.json"
    stages: dict[str, Any] = {}
    if args.resume:
        raw_state = _load_json(state_path)
        if raw_state.get("schema_version") != STATE_SCHEMA:
            raise AdjFactorCandidateRepairError("resume state schema differs")
        stages = dict(raw_state.get("stages") or {})
    _write_state(state_path, status="BUILDING", stages=stages)
    connection = _connect_readonly(args.env_file)
    try:
        job = _verify_job(connection, args.production_job_id, args.production_job_snapshot_sha256)
        job_path = reports / "adj_factor_production_job_authority.json"
        if not job_path.exists():
            write_canonical_json_atomic(job_path, _canonical_payload({"schema_version": "qe_adj_factor_production_job_authority_v1", **job}))

        inventory_path = reports / "adj_factor_selective_repair_inventory.json"
        if inventory_path.exists():
            inventory = _load_json(inventory_path)
            validate_inventory(inventory)
        else:
            inventory = scan_inventory(
                connection=connection,
                baseline_root=baseline,
                start=args.start,
                cutoff=args.cutoff,
                production_job_id=args.production_job_id,
                production_job_snapshot_sha256=args.production_job_snapshot_sha256,
            )
            write_canonical_json_atomic(inventory_path, inventory)
        stages["inventory"] = {
            "status": "PASS",
            "path": str(inventory_path),
            "sha256": sha256_file(inventory_path),
            "affected_symbol_count": inventory["affected_symbol_count"],
            "daily_mismatch_cell_count": inventory["daily_mismatch_cell_count"],
            "minute_mismatch_cell_count": inventory["minute_mismatch_cell_count"],
        }
        _write_state(state_path, status="BUILDING", stages=stages)

        clone_path = reports / "adj_factor_clone_receipt.json"
        if clone_path.exists():
            clone = _load_json(clone_path)
        else:
            clone = _canonical_payload(
                {
                    "schema_version": "qe_adj_factor_cow_clone_receipt_v1",
                    **clone_baseline_copy_on_write(
                        baseline_root=baseline,
                        candidate_root=candidate,
                        inventory=inventory,
                    ),
                }
            )
            write_canonical_json_atomic(clone_path, clone)
        stages["clone"] = {"status": "PASS", "path": str(clone_path), "sha256": sha256_file(clone_path)}
        _write_state(state_path, status="BUILDING", stages=stages)

        bin_path = reports / "adj_factor_bin_repair_receipt.json"
        if bin_path.exists():
            bins = _load_json(bin_path)
        else:
            bins = _canonical_payload(
                {
                    "schema_version": "qe_adj_factor_bin_repair_receipt_v1",
                    **patch_qlib_bins(
                        connection=connection,
                        baseline_root=baseline,
                        candidate_root=candidate,
                        inventory=inventory,
                        start=args.start,
                        cutoff=args.cutoff,
                        connection_factory=lambda: _connect_readonly(args.env_file),
                        max_workers=args.max_workers,
                    ),
                }
            )
            write_canonical_json_atomic(bin_path, bins)
        stages["qlib_bins"] = {
            "status": "PASS",
            "path": str(bin_path),
            "sha256": sha256_file(bin_path),
            "daily_symbol_count": bins["daily_symbol_count"],
            "minute_symbol_count": bins["minute_symbol_count"],
            "changed_factor_cells": bins["changed_factor_cells"],
        }
        _write_state(state_path, status="BUILDING", stages=stages)

        symbols = tuple(str(item["symbol"]) for item in inventory["records"])
        daily_h5_path = reports / "adj_factor_daily_h5_repair_receipt.json"
        daily_h5_target = candidate / "components" / "factor_h5_static_candidate_v2" / "daily_pv.h5"
        daily_h5 = _load_json(daily_h5_path) if daily_h5_path.exists() and daily_h5_target.is_file() else {}
        if daily_h5.get("correction_source") != "corrected_daily_qlib_bins":
            daily_h5 = _canonical_payload(
                {
                    "schema_version": "qe_adj_factor_daily_h5_repair_receipt_v1",
                    **rebuild_daily_pv_h5(
                        baseline_path=baseline / "components" / "factor_h5_static_candidate_v2" / "daily_pv.h5",
                        candidate_path=daily_h5_target,
                        baseline_daily_component=baseline / "components" / "daily_bin_candidate",
                        corrected_daily_component=candidate / "components" / "daily_bin_candidate",
                        affected_symbols=symbols,
                    ),
                }
            )
            write_canonical_json_atomic(daily_h5_path, daily_h5)
        stages["daily_h5"] = {"status": "PASS", "path": str(daily_h5_path), "sha256": sha256_file(daily_h5_path), "changed_rows": daily_h5["changed_rows"]}
        _write_state(state_path, status="BUILDING", stages=stages)

        static_path = reports / "adj_factor_static_repair_receipt.json"
        static_target = candidate / "components" / "factor_h5_static_candidate_v2" / "static_factors.parquet"
        static = _load_json(static_path) if static_path.exists() and static_target.is_file() else {}
        if static.get("source_daily_h5_sha256") != sha256_file(daily_h5_target):
            static = _canonical_payload(
                {
                    "schema_version": "qe_adj_factor_static_repair_receipt_v1",
                    **rebuild_static_factors(
                        baseline_path=baseline / "components" / "factor_h5_static_candidate_v2" / "static_factors.parquet",
                        candidate_path=static_target,
                        corrected_daily_h5=daily_h5_target,
                        affected_symbols=set(symbols),
                    ),
                }
            )
            write_canonical_json_atomic(static_path, static)
        stages["static_factors"] = {"status": "PASS", "path": str(static_path), "sha256": sha256_file(static_path), "changed_rows": static["changed_rows"]}
        _write_state(state_path, status="VALIDATING", stages=stages)

        verification = verify_zero_factor_drift(
            connection=connection,
            candidate_root=candidate,
            start=args.start,
            cutoff=args.cutoff,
            symbols=symbols,
        )
        if verification["status"] != "PASS":
            raise AdjFactorCandidateRepairError("post-repair Qlib factor parity failed")
        verification_path = reports / "adj_factor_zero_drift_validation.json"
        verification = _canonical_payload({"schema_version": "qe_adj_factor_zero_drift_validation_v1", **verification})
        write_canonical_json_atomic(verification_path, verification)
        stages["factor_parity"] = {"status": "PASS", "path": str(verification_path), "sha256": sha256_file(verification_path)}

        receipt = build_repair_receipt(
            baseline_root=baseline,
            candidate_root=candidate,
            inventory=inventory,
            clone=clone,
            bins={key: value for key, value in bins.items() if key != "outputs"},
            daily_h5=daily_h5,
            static=static,
            verification=verification,
            active_profile_path=active_profile,
            active_profile_sha256_before=active_profile_sha,
        )
        receipt_path = reports / "adj_factor_candidate_repair_receipt.json"
        write_canonical_json_atomic(receipt_path, receipt)
        stages["repair_receipt"] = {"status": "PASS", "path": str(receipt_path), "sha256": sha256_file(receipt_path), "canonical_sha256": receipt["canonical_sha256"]}
        _write_state(state_path, status="REPAIR_READY_FOR_DATASET_VALIDATION", stages=stages)
        print(
            json.dumps(
                {
                    "ok": True,
                    "status": "REPAIR_READY_FOR_DATASET_VALIDATION",
                    "candidate_root": str(candidate),
                    "affected_symbol_count": inventory["affected_symbol_count"],
                    "daily_mismatch_cell_count": inventory["daily_mismatch_cell_count"],
                    "minute_mismatch_cell_count": inventory["minute_mismatch_cell_count"],
                    "receipt": str(receipt_path),
                },
                ensure_ascii=False,
            )
        )
        return 0
    except Exception as exc:
        _write_state(state_path, status="BLOCKED", stages=stages, error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
