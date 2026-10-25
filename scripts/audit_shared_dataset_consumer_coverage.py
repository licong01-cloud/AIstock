"""Read-only live PIT/Selection lease gate, independent of component readiness.

This is a pre-activation monthly consumer check, not a source builder.  It never
activates authority, constructs a hypothetical lease or edits historical runs.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.shared_consumer_coverage import validate_live_pit_lease_identity
from backend.services.selection_center.models import SelectionRunStatus
from scripts.prepare_canonical_pit_monthly import _load_database_config


def read_live_pit_consumer(target: str, env_file: Path) -> dict:
    import psycopg2

    config = _load_database_config(target, env_file)
    connection = psycopg2.connect(
        host=config.host, port=config.port, user=config.user, password=config.password,
        dbname=config.dbname, application_name="monthly-pit-consumer-readonly",
        options="-c default_transaction_read_only=on -c statement_timeout=60000",
    )
    connection.set_session(readonly=True, isolation_level="REPEATABLE READ")

    @contextmanager
    def factory():
        yield connection

    try:
        binding = json.loads(json.dumps(asdict(
            CanonicalPitAuthorityResolver(connection_factory=factory).resolve_live_binding()
        ), default=lambda value: value.isoformat() if hasattr(value, "isoformat") else value.value))
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('selection.run')")
            if cursor.fetchone()[0] is None:
                raise ValueError("native Selection run table is absent")
            # The latest successful native run is the consumer evidence. Never
            # select an older matching lease to hide a newer legacy/drifted run.
            cursor.execute(
                "SELECT run_id,trade_date,runtime_config->'canonical_pit_authority' "
                "FROM selection.run WHERE status=%s ORDER BY created_at DESC,run_id DESC LIMIT 1",
                (SelectionRunStatus.SUCCEEDED.value,),
            )
            row = cursor.fetchone()
        lease = row[2] if row is not None else None
        result = validate_live_pit_lease_identity(binding, native_leases=(lease,) if lease is not None else ())
        return {"schema_version": "aistock_monthly_live_pit_consumer_readback_v1", "target": target,
            "binding": binding, "native_selection_run_id": str(row[0]) if row else None,
            "native_selection_trade_date": str(row[1]) if row else None, "native_lease": lease,
            "consumer_gate": result, "database_read": True, "database_write": False,
            "outcomes_read": False, "profile_write": False, "runtime_action": False}
    finally:
        connection.rollback()
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("dev", "production"), required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.absolute()
    if output.drive.upper() != "X:" or not output.parent.is_dir() or output.exists():
        raise ValueError("readback requires a new file under an existing X-drive directory")
    result = read_live_pit_consumer(args.target, args.env_file)
    result["canonical_sha256"] = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
    with output.open("xb") as handle:
        handle.write(canonical_json_bytes(result) + b"\n")
    print(json.dumps({"output": str(output), **result["consumer_gate"]}, ensure_ascii=False))
    return 0 if result["consumer_gate"]["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
