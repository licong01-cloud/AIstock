"""Authoritative retained-account inventory for the LocalSIM successor cutover."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import date, datetime
from typing import Any

import psycopg2.extras

from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .successor_models import LegacyLocalSimAccountInventoryV1, SimulationAccountStatus


_ECONOMIC_SCOPES: tuple[tuple[str, str], ...] = (
    ("paper_v2.simulation_daily_run", "strategy_id"),
    ("paper_v2.execution_plan", "portfolio_id"),
    ("paper_v2.run", "portfolio_id"),
    ("paper_v2.positions", "portfolio_id"),
    ("paper_v2.intraday_snapshots", "portfolio_id"),
    ("paper_v2.orders", "portfolio_id"),
    ("paper_v2.fills", "portfolio_id"),
    ("paper_v2.cash_ledger", "portfolio_id"),
    ("paper_v2.errors", "portfolio_id"),
)
_IN_FLIGHT_STATUSES = (
    "CREATED",
    "PRECHECKING",
    "SIGNAL_GENERATING",
    "TARGET_GENERATING",
    "PLANNING_EXECUTION",
    "SUBMITTING",
    "INTRADAY_RUNNING",
    "TAIL_HANDLING",
    "RECONCILING",
)
_STATUS_MAP = {
    "READY": SimulationAccountStatus.ACTIVE,
    "RUNNING": SimulationAccountStatus.ACTIVE,
    "PAUSED": SimulationAccountStatus.PAUSED,
}


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


class LocalSimLegacyInventoryReader:
    """Build lineage inputs from one repeatable-read database snapshot."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def read(self, retained_account_ids: Sequence[str]) -> tuple[LegacyLocalSimAccountInventoryV1, ...]:
        account_ids = tuple(dict.fromkeys(str(item).strip() for item in retained_account_ids if str(item).strip()))
        if not account_ids:
            raise InvalidStateTransitionError(
                "retained LocalSIM account ids must be explicit",
                context={"reason_code": "LOCALSIM_CUTOVER_RETAINED_SET_EMPTY"},
            )
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cur.execute(
                """
                SELECT portfolio.portfolio_id AS legacy_account_id,
                       portfolio.portfolio_name AS account_name,
                       portfolio.package_id,
                       portfolio.manifest_sha256,
                       portfolio.initial_cash,
                       portfolio.status AS portfolio_status,
                       portfolio.auto_run_enabled,
                       release.release_id,
                       release.package_id AS release_package_id,
                       release.manifest_sha256 AS release_manifest_sha256,
                       release.release_hash,
                       binding.binding_id,
                       binding.package_id AS binding_package_id,
                       binding.manifest_sha256 AS binding_manifest_sha256,
                       binding.release_hash AS binding_release_hash,
                       binding.binding_hash,
                       binding.broker_account_id,
                       binding.binding_config_json,
                       scope.ledger_scope_id,
                       scope.scope_kind,
                       scope.source_identity,
                       scope.native_account_id
                FROM paper_v2.portfolio AS portfolio
                JOIN paper_v2.simulation_release_binding AS binding
                  ON binding.strategy_id = portfolio.portfolio_id
                 AND binding.broker_backend = 'local_sim'
                 AND binding.approval_state <> 'RETIRED'
                JOIN strategy_pkg.strategy_runtime_release AS release
                  ON release.release_id = binding.release_id
                JOIN paper_v2.simulation_ledger_scope_v1 AS scope
                  ON scope.ledger_scope_id = portfolio.portfolio_id
                WHERE portfolio.portfolio_id = ANY(%s::text[])
                  AND portfolio.broker_backend = 'local_sim'
                ORDER BY portfolio.portfolio_id, binding.binding_id
                """,
                (list(account_ids),),
            )
            rows = [dict(row) for row in cur.fetchall()]
            grouped: dict[str, list[dict[str, Any]]] = {account_id: [] for account_id in account_ids}
            for row in rows:
                grouped.setdefault(str(row["legacy_account_id"]), []).append(row)
            missing = sorted(account_id for account_id, matches in grouped.items() if not matches)
            duplicate = sorted(account_id for account_id, matches in grouped.items() if len(matches) != 1)
            if missing or duplicate:
                raise DataUnavailableError(
                    "retained LocalSIM inventory is missing or ambiguous",
                    context={
                        "reason_code": "LOCALSIM_CUTOVER_INVENTORY_NOT_UNIQUE",
                        "missing_account_ids": missing,
                        "ambiguous_account_ids": duplicate,
                    },
                )
            result = tuple(self._build(cur, grouped[account_id][0]) for account_id in account_ids)
            return result

    def _build(self, cur: Any, row: dict[str, Any]) -> LegacyLocalSimAccountInventoryV1:
        legacy_account_id = str(row["legacy_account_id"])
        status_text = str(row["portfolio_status"])
        status = _STATUS_MAP.get(status_text)
        if status is None or bool(row["auto_run_enabled"]):
            raise InvalidStateTransitionError(
                "retained LocalSIM account is terminal or still owned by legacy auto-run",
                context={
                    "reason_code": "LOCALSIM_CUTOVER_LEGACY_OWNER_ACTIVE",
                    "legacy_account_id": legacy_account_id,
                    "portfolio_status": status_text,
                    "auto_run_enabled": bool(row["auto_run_enabled"]),
                },
            )
        if (
            str(row["scope_kind"]) != "LEGACY_PORTFOLIO"
            or str(row["source_identity"]) != legacy_account_id
            or row["native_account_id"] is not None
            or str(row["ledger_scope_id"]) != legacy_account_id
        ):
            raise InvalidStateTransitionError(
                "retained LocalSIM account does not own one immutable legacy ledger scope",
                context={
                    "reason_code": "LOCALSIM_CUTOVER_LEDGER_SCOPE_MISMATCH",
                    "legacy_account_id": legacy_account_id,
                },
            )
        if (
            str(row["release_package_id"]) != str(row["package_id"])
            or str(row["binding_package_id"]) != str(row["package_id"])
            or str(row["release_manifest_sha256"]) != str(row["manifest_sha256"])
            or str(row["binding_manifest_sha256"]) != str(row["manifest_sha256"])
            or str(row["binding_release_hash"]) != str(row["release_hash"])
            or str(row["broker_account_id"] or "") != legacy_account_id
        ):
            raise InvalidStateTransitionError(
                "retained LocalSIM release or binding authority is inconsistent",
                context={
                    "reason_code": "LOCALSIM_CUTOVER_RELEASE_BINDING_MISMATCH",
                    "legacy_account_id": legacy_account_id,
                },
            )
        cur.execute(
            """
            SELECT count(*) AS count
            FROM paper_v2.simulation_daily_run
            WHERE strategy_id = %s AND broker_backend = 'local_sim' AND status = ANY(%s::text[])
            """,
            (legacy_account_id, list(_IN_FLIGHT_STATUSES)),
        )
        in_flight = int(cur.fetchone()["count"])
        economic_hash = self._economic_facts_sha256(cur, legacy_account_id)
        binding_config = row.get("binding_config_json") if isinstance(row.get("binding_config_json"), dict) else {}
        metadata = binding_config.get("metadata") if isinstance(binding_config.get("metadata"), dict) else {}
        receipt_id = str(metadata.get("admission_receipt_id") or "").strip()
        if not receipt_id:
            receipt_id = f"legacy_cutover_{economic_hash[:16]}"
        return LegacyLocalSimAccountInventoryV1(
            legacy_account_id=legacy_account_id,
            account_name=str(row["account_name"]),
            package_id=str(row["package_id"]),
            manifest_sha256=str(row["manifest_sha256"]),
            admission_receipt_id=receipt_id,
            initial_capital=float(row["initial_cash"]),
            release_id=str(row["release_id"]),
            release_hash=str(row["release_hash"]),
            binding_id=str(row["binding_id"]),
            binding_hash=str(row["binding_hash"]),
            ledger_scope_id=legacy_account_id,
            economic_facts_sha256=economic_hash,
            current_status=status,
            runtime_owned=in_flight == 0,
            retained_by_user=True,
            in_flight_economic_transactions=in_flight,
        )

    @staticmethod
    def _economic_facts_sha256(cur: Any, ledger_scope_id: str) -> str:
        digest = hashlib.sha256()
        for table, column in _ECONOMIC_SCOPES:
            digest.update(f"{table}:{column}\n".encode())
            cur.execute(
                f"SELECT to_jsonb(scoped) AS payload FROM {table} AS scoped "
                f"WHERE {column} = %s ORDER BY to_jsonb(scoped)::text",
                (ledger_scope_id,),
            )
            count = 0
            for result in cur:
                payload = result["payload"] if isinstance(result, dict) else result[0]
                digest.update(
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        default=_json_default,
                    ).encode("utf-8")
                )
                digest.update(b"\n")
                count += 1
            digest.update(f"count={count}\n".encode())
        return digest.hexdigest()
