"""Inventory, preflight, apply and read back retained LocalSIM successor lineage."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.simulation_runtime.localsim_control import LocalSimControlPlaneService  # noqa: E402
from backend.services.simulation_runtime.localsim_cutover_inventory import (  # noqa: E402
    LocalSimLegacyInventoryReader,
)
from backend.services.simulation_runtime.successor_repository import LocalSimSuccessorRepository  # noqa: E402


class CutoverPreparationError(RuntimeError):
    pass


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--mode", choices=("inventory", "preflight", "apply", "readback"), default="inventory")
    result.add_argument("--target", choices=("dev", "production"), required=True)
    result.add_argument("--retained-account-id", action="append", required=True)
    result.add_argument("--authority-trade-date", type=date.fromisoformat, required=True)
    result.add_argument("--env-file", type=Path, default=ROOT / ".env")
    result.add_argument("--expected-source-commit")
    result.add_argument("--expected-database-host")
    result.add_argument("--expected-database-port", type=int)
    result.add_argument("--expected-database-name")
    result.add_argument("--authorization")
    result.add_argument("--confirm-production", action="store_true")
    result.add_argument("--created-by", default="localsim_successor_cutover")
    result.add_argument("--receipt", type=Path)
    return result


def _settings(target: str, env_file: Path) -> dict[str, Any]:
    if not env_file.is_file():
        raise CutoverPreparationError(f"environment file does not exist: {env_file}")
    load_dotenv(env_file, override=False)
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    values = {
        key: os.getenv(prefix + suffix)
        for key, suffix in (
            ("host", "HOST"),
            ("port", "PORT"),
            ("dbname", "NAME"),
            ("user", "USER"),
            ("password", "PASSWORD"),
        )
    }
    missing = sorted(key for key, value in values.items() if not value)
    if missing:
        raise CutoverPreparationError(f"missing {target} database settings: {missing}")
    values["port"] = int(values["port"])
    return values


@contextmanager
def _connection(settings: dict[str, Any]) -> Iterator[Any]:
    conn = psycopg2.connect(**settings)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _source_commit(expected: str | None, *, required: bool) -> str:
    observed = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    if required and (not expected or observed != expected.strip()):
        raise CutoverPreparationError(
            f"source identity mismatch: expected={expected or '<missing>'} observed={observed}"
        )
    return observed


def _database_preflight(conn: Any, args: argparse.Namespace, settings: dict[str, Any]) -> dict[str, Any]:
    expected = (args.expected_database_host, args.expected_database_port, args.expected_database_name)
    observed = (str(settings["host"]), int(settings["port"]), str(settings["dbname"]))
    if any(value is None for value in expected) or observed != expected:
        raise CutoverPreparationError(f"database identity mismatch: expected={expected} observed={observed}")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cur.execute(
            """
            SELECT to_regclass('paper_v2.simulation_account_v1') IS NOT NULL AS account_present,
                   to_regclass('paper_v2.legacy_localsim_account_lineage_v1') IS NOT NULL AS lineage_present,
                   to_regclass('paper_v2.simulation_ledger_scope_v1') IS NOT NULL AS scope_present,
                   (SELECT count(*) FROM pg_constraint
                    WHERE contype='f'
                      AND conrelid IN ('paper_v2.run'::regclass, 'paper_v2.intraday_snapshots'::regclass)
                      AND confrelid='paper_v2.simulation_ledger_scope_v1'::regclass) AS runtime_fk_count,
                   obj_description('paper_v2.simulation_account_v1'::regclass, 'pg_class') AS account_comment,
                   obj_description('paper_v2.simulation_ledger_scope_v1'::regclass, 'pg_class') AS scope_comment
            """
        )
        row = dict(cur.fetchone())
    if not all(row[key] for key in ("account_present", "lineage_present", "scope_present")):
        raise CutoverPreparationError("B2 successor schema or C ledger-scope bridge is missing")
    if int(row["runtime_fk_count"]) != 2 or not row["account_comment"] or not row["scope_comment"]:
        raise CutoverPreparationError("successor schema FK/comment readback failed")
    return {
        "database_host": observed[0],
        "database_port": observed[1],
        "database_name": observed[2],
        "runtime_fk_count": int(row["runtime_fk_count"]),
        "schema_comments_present": True,
    }


def _authorization(args: argparse.Namespace, account_ids: tuple[str, ...]) -> None:
    exact = (
        "AUTHORIZE_LOCALSIM_LINEAGE_APPLY:"
        f"{args.target}:{args.expected_database_name}:{args.authority_trade_date.isoformat()}:{','.join(account_ids)}"
    )
    if args.authorization != exact:
        raise CutoverPreparationError(f"exact lineage authorization is required: {exact}")
    if args.target == "production" and not args.confirm_production:
        raise CutoverPreparationError("production apply requires --confirm-production")


def _inventory(
    settings: dict[str, Any],
    account_ids: tuple[str, ...],
    *,
    authority_trade_date: date,
) -> tuple[Any, ...]:
    with _connection(settings) as conn:
        return LocalSimLegacyInventoryReader(conn).read(
            account_ids,
            authority_trade_date=authority_trade_date,
        )


def execute(args: argparse.Namespace) -> dict[str, Any]:
    account_ids = tuple(dict.fromkeys(str(item).strip() for item in args.retained_account_id))
    if any(not item for item in account_ids):
        raise CutoverPreparationError("retained account identities must be non-empty")
    settings = _settings(args.target, args.env_file)
    source_commit = _source_commit(args.expected_source_commit, required=args.mode in {"preflight", "apply", "readback"})
    inventory = _inventory(
        settings,
        account_ids,
        authority_trade_date=args.authority_trade_date,
    )
    preflight = None
    if args.mode != "inventory":
        with _connection(settings) as conn:
            preflight = _database_preflight(conn, args, settings)
    applied: list[dict[str, str]] = []
    if args.mode == "apply":
        _authorization(args, account_ids)
        repository = LocalSimSuccessorRepository(conn_factory=lambda: _connection(settings))
        control = LocalSimControlPlaneService(repository=repository)
        for candidate in inventory:
            account, lineage = control.prepare_legacy_lineage(candidate, created_by=args.created_by)
            applied.append(
                {
                    "legacy_account_id": candidate.legacy_account_id,
                    "account_id": account.account_id,
                    "lineage_id": lineage.lineage_id,
                    "lineage_hash": lineage.lineage_hash,
                }
            )
    readback: list[dict[str, str]] = []
    if args.mode in {"apply", "readback"}:
        repository = LocalSimSuccessorRepository(conn_factory=lambda: _connection(settings))
        current = _inventory(
            settings,
            account_ids,
            authority_trade_date=args.authority_trade_date,
        )
        for candidate in current:
            lineage = repository.get_lineage_by_legacy_account(candidate.legacy_account_id)
            if (
                lineage is None
                or lineage.release_id != candidate.release_id
                or lineage.binding_id != candidate.binding_id
                or lineage.ledger_scope_id != candidate.ledger_scope_id
                or lineage.economic_facts_sha256 != candidate.economic_facts_sha256
            ):
                raise CutoverPreparationError(
                    "lineage readback missing or authority/economic hash drifted: "
                    f"{candidate.legacy_account_id}"
                )
            account = repository.get_account(lineage.account_id)
            if account.package_id != candidate.package_id or account.manifest_sha256 != candidate.manifest_sha256:
                raise CutoverPreparationError(
                    f"lineage account package authority drifted: {candidate.legacy_account_id}"
                )
            readback.append(
                {
                    "legacy_account_id": candidate.legacy_account_id,
                    "account_id": lineage.account_id,
                    "lineage_id": lineage.lineage_id,
                    "lineage_hash": lineage.lineage_hash,
                    "economic_facts_sha256": lineage.economic_facts_sha256,
                }
            )
    return {
        "schema_version": "localsim_successor_cutover_preparation_receipt_v1",
        "mode": args.mode,
        "target": args.target,
        "source_commit": source_commit,
        "authority_trade_date": args.authority_trade_date.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "retained_account_ids": list(account_ids),
        "inventory": [item.model_dump(mode="json") for item in inventory],
        "preflight": preflight,
        "applied": applied,
        "readback": readback,
    }


def main() -> int:
    args = parser().parse_args()
    try:
        receipt = execute(args)
        if args.receipt:
            args.receipt.parent.mkdir(parents=True, exist_ok=True)
            args.receipt.write_text(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True), flush=True)
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
