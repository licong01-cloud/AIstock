"""Operator-only DEV PIT evidence/preparation; never activates production."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.prepare_canonical_pit_monthly import _load_database_config
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.canonical_pit_migration import (
    audit_canonical_pit_readiness, audit_eligibility_intervals, plan_forward_profiles, prepare_dev_forward_profiles,
    read_forward_profile_records, read_sealed_json, seal_real_activation, _plain_file,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", choices=("audit", "prepare-profiles", "seal"), required=True)
    parser.add_argument("--target", choices=("dev",), required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 8, 1))
    parser.add_argument("--expected-plan-digest")
    parser.add_argument("--rollback-after-validation", action="store_true")
    parser.add_argument("--bundle-file", type=Path)
    parser.add_argument("--w8-file", type=Path)
    parser.add_argument("--inputs-file", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.absolute()
    if output.drive.upper() != "X:" or not output.parent.is_dir() or output.exists():
        parser.error("output requires a new file under existing X-drive directory")
    for parent in output.parents:
        if parent.is_symlink() or parent.is_junction():
            parser.error("output must not use symlink/junction")
    if args.rollback_after_validation and args.action != "prepare-profiles":
        parser.error("rollback rehearsal applies only to DEV profile preparation")
    # Reserve the exact receipt before any DML: existing/racing outputs must
    # fail before commit, not after durable DB changes have already happened.
    with output.open("xb") as handle:
        try:
            result, status = _execute(args, parser)
        except BaseException as exc:
            handle.write(canonical_json_bytes({"status": "FAILED", "error_type": type(exc).__name__,
                "database_commit_state": "REQUIRES_READBACK" if args.action == "prepare-profiles" else "NOT_ATTEMPTED",
                "production_write": False}) + b"\n")
            raise
        if args.action != "seal":
            result.update(outcomes_read=False, candidate_write=False, active_profile_write=False,
                          authority_activation=False, ddl_performed=False)
        result["status"] = status
        canonical_sha256 = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
        if args.action != "seal":
            result["canonical_sha256"] = canonical_sha256
        handle.write(canonical_json_bytes(result) + b"\n")
        handle.flush()
    print(json.dumps({"receipt": str(output), "status": status,
                      "canonical_sha256": canonical_sha256}, ensure_ascii=False))
    return 2 if status == "BLOCKED" else 0


def _execute(args, parser):
    if args.action == "seal":
        if not all((args.bundle_file, args.w8_file, args.inputs_file)):
            parser.error("seal requires immutable real bundle, independent W8 and readback inputs")
        envelope = seal_real_activation(read_sealed_json(args.bundle_file), read_sealed_json(args.w8_file),
                                        read_sealed_json(args.inputs_file))
        result = envelope.as_dict()
        status = result["status"]
    else:
        import psycopg2
        from backend.services.paper_trading_v2.repository import PaperTradingV2Repository

        config = _load_database_config("dev", args.env_file)
        if config.dbname != "aistock_dev" or config.port != 5433 or config.host not in {"127.0.0.1", "localhost"}:
            parser.error("configured target differs from explicitly authorized DEV endpoint")
        write = args.action == "prepare-profiles"
        if write and not args.expected_plan_digest:
            parser.error("prepare-profiles requires exact dry-run plan digest")
        conn = psycopg2.connect(host=config.host, port=config.port, dbname=config.dbname,
            user=config.user, password=config.password, application_name="canonical-PIT-DEV-operator",
            options="-c statement_timeout=60000 -c lock_timeout=10000")
        conn.set_session(readonly=not write, isolation_level="SERIALIZABLE" if write else "REPEATABLE READ")
        try:
            if write:
                result = prepare_dev_forward_profiles(conn, expected_plan_digest=args.expected_plan_digest)
                if args.rollback_after_validation:
                    conn.rollback()
                    result["database_write"] = False
                    result["transaction_dml_rolled_back"] = True
                    status = "DEV_ROLLBACK_VALIDATED"
                else:
                    conn.commit()
                    result["database_write"] = result["new_version_count"] > 0
                    result["transaction_dml_rolled_back"] = False
                    status = "DEV_PREPARED_AUTHORITY_NOT_ACTIVATED"
            else:
                if args.candidate_root is None:
                    parser.error("audit requires the exact frozen candidate root")
                manifest = read_sealed_json(args.candidate_root / "qe_dataset_manifest.json")
                cutoff = date.fromisoformat(manifest["cutoff_trade_date"])
                member = manifest["st_pit_manifest"]["selection_universe"]
                relative = Path(member["path"])
                if relative.drive or relative.is_absolute() or ".." in relative.parts:
                    raise ValueError("frozen membership path escapes candidate")
                source = _plain_file(args.candidate_root / relative)
                raw = source.read_bytes()
                if hashlib.sha256(raw).hexdigest() != member["sha256"]:
                    raise ValueError("frozen membership file identity differs")
                frozen = [line.split() for line in raw.decode("utf-8").splitlines() if line.strip()]
                with conn.cursor() as cursor:
                    cursor.execute("SELECT ts_code,eligible_start,eligible_end,entry_reason,exit_reason FROM market.stock_universe_pit_spans "
                        "WHERE universe_key=%s AND rule_version=%s AND eligible_start<=%s AND eligible_end>=%s "
                        "ORDER BY ts_code,eligible_start,eligible_end", ("aistock_equity_pit_canonical_v2",
                        "shsz_a_252td_st_delist_asof_v2", cutoff, args.start))
                    rolling = cursor.fetchall()

                @contextmanager
                def factory():
                    yield conn

                repository = PaperTradingV2Repository(conn_factory=factory)
                result = {"schema_version": "local_data_dev_pit_activation_audit_v1",
                          "target": "aistock_dev:5433", "candidate_root": str(args.candidate_root.absolute()),
                          "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
                          "membership_audit": audit_eligibility_intervals(frozen, [row[:3] for row in rolling], start=args.start, cutoff=cutoff),
                          # A direct-v2 stock_universe.txt is a three-field
                          # execution projection, not a FrozenPitSnapshot.
                          "canonical_pit_identity_audit": audit_canonical_pit_readiness(rolling, start=args.start, cutoff=cutoff),
                          "frozen_source_kind": "three_field_execution_projection",
                          "forward_profile_plan": plan_forward_profiles(repository, read_forward_profile_records(conn)),
                          "database_read": True, "database_write": False,
                          "production_write": False, "runtime_action": False}
                status = result["canonical_pit_identity_audit"]["status"]
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
    return result, status


if __name__ == "__main__":
    raise SystemExit(main())
