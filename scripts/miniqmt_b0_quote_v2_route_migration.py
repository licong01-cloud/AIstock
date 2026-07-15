"""Plan or apply one LEGACY_B0 -> B0_QUOTE_V2 binding route migration.

Dry-run is the default.  The tool performs no broker write, service restart,
configuration write, StrategyPackage mutation, database export, or snapshot.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any, Iterator

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.config_manager_compat import ConfigManager  # noqa: E402
from backend.infra.qmt_client import get_qmt_client_singleton  # noqa: E402
from backend.services.simulation_runtime.miniqmt_route_migration import (  # noqa: E402
    MiniQMTRouteMigrationService,
)
from backend.services.simulation_runtime.repository import SimulationRuntimeRepository  # noqa: E402


def _load_env(path: Path | None) -> None:
    if path is None:
        return
    if not path.exists():
        raise FileNotFoundError(path)
    for key, value in ConfigManager(path.resolve()).read_env().items():
        os.environ.setdefault(key, value)


def _db_config(target_db: str) -> dict[str, Any]:
    prefix = "TDX_DB_DEV_" if target_db == "dev" else "TDX_DB_"
    fields = {
        "host": f"{prefix}HOST",
        "port": f"{prefix}PORT",
        "database": f"{prefix}NAME",
        "user": f"{prefix}USER",
        "password": f"{prefix}PASSWORD",
    }
    values = {key: os.getenv(env_name) for key, env_name in fields.items()}
    missing = [fields[key] for key, value in values.items() if value in (None, "")]
    if missing:
        raise RuntimeError(f"database configuration is incomplete: {', '.join(missing)}")
    return {
        "host": values["host"],
        "port": int(str(values["port"])),
        "database": values["database"],
        "user": values["user"],
        "password": values["password"],
        "connect_timeout": 10,
        "application_name": "aistock_miniqmt_b0_route_migration",
    }


def _connection_factory(target_db: str):
    @contextmanager
    def connect(*, autocommit: bool = False, manage_transaction: bool = True) -> Iterator[Any]:
        del manage_transaction
        conn = psycopg2.connect(**_db_config(target_db))
        conn.autocommit = autocommit
        try:
            yield conn
            if not autocommit:
                conn.commit()
        except Exception:
            if not autocommit:
                conn.rollback()
            raise
        finally:
            conn.close()

    return connect


def _broker_open_orders() -> list[dict[str, Any]]:
    client = get_qmt_client_singleton()
    rows = client.get_orders(cancelable_only=True)
    if rows is None:
        raise RuntimeError("MiniQMT cancelable-order readback returned None")
    return [dict(row) for row in rows]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-binding-id", required=True)
    parser.add_argument("--target-release-id", required=True)
    parser.add_argument("--effective-trade-date", required=True, type=date.fromisoformat)
    parser.add_argument("--operator", required=True)
    parser.add_argument("--target-db", choices=("dev", "prod"), default="prod")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--runtime-limit", type=int, default=500)
    parser.add_argument("--apply", action="store_true")
    return parser


def main() -> int:
    args = _parser().parse_args()
    from backend.services.miniqmt_execution_runtime.repository import (
        PostgresMiniQMTExecutionRuntimeRepository,
    )

    _load_env(args.env_file)
    conn_factory = _connection_factory(args.target_db)
    service = MiniQMTRouteMigrationService(
        simulation_repository=SimulationRuntimeRepository(conn_factory=conn_factory),
        runtime_repository=PostgresMiniQMTExecutionRuntimeRepository(conn_factory=conn_factory),
        broker_open_order_reader=_broker_open_orders,
        runtime_limit=args.runtime_limit,
    )
    common = {
        "source_binding_id": args.source_binding_id,
        "target_release_id": args.target_release_id,
        "effective_trade_date": args.effective_trade_date,
        "operator": args.operator,
    }
    if args.apply:
        receipt = service.apply(**common)
        payload = {
            "mode": "apply",
            "target_db": args.target_db,
            "receipt": receipt.model_dump(mode="json"),
            "production_ddl_gate": "noop",
            "broker_write_called": False,
            "service_restarted": False,
        }
    else:
        plan = service.plan(**common)
        payload = {
            "mode": "dry_run",
            "target_db": args.target_db,
            "inventory": plan.inventory.model_dump(mode="json"),
            "target_binding": {
                "binding_id": plan.target_binding.binding_id,
                "binding_hash": plan.target_binding.binding_hash,
                "release_id": plan.target_binding.release_id,
                "effective_from": plan.target_binding.effective_from.isoformat()
                if plan.target_binding.effective_from
                else None,
            },
            "database_written": False,
            "broker_write_called": False,
            "service_restarted": False,
        }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
