"""Bounded read-only K2 diagnostics owned by the public repository facade."""

from __future__ import annotations

from datetime import date
from typing import Any

import psycopg2.extras

from .plugin_canonical import canonical_utc_datetime_v1


class KernelRepositoryDiagnosticsMixin:
    def read_kernel_diagnostics(
        self,
        *,
        runtime_id: str,
        trade_date: date,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if type(runtime_id) is not str or not runtime_id.strip() or runtime_id != runtime_id.strip():
            raise ValueError("runtime_id must be a non-empty trim-stable strict string")
        if type(trade_date) is not date:
            raise TypeError("trade_date must be an exact date")
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("kernel diagnostics limit must be a strict integer in [1, 500]")
        cursor_values = _decode_kernel_cursor(cursor)
        required_tables = (
            "execution_runtime_event",
            "execution_algo_event_delivery",
            "execution_algo_command_outbox",
            "execution_algo_timer_schedule",
            "execution_algo_timer_occurrence",
            "execution_algo_instance",
            "execution_algo_diagnostic_observation",
            "execution_broker_reconciliation_attempt",
        )
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT name,to_regclass('qmt_strategy.' || name) IS NOT NULL AS exists
                    FROM unnest(%s::text[]) AS name
                    ORDER BY name
                    """,
                    (list(required_tables),),
                )
                table_rows = cur.fetchall()
                missing = tuple(str(row["name"]) for row in table_rows if row["exists"] is not True)
                if missing:
                    return {
                        "schema_version": "miniqmt_kernel_diagnostics_v1",
                        "schema_status": "NOT_APPLIED",
                        "runtime_id": runtime_id,
                        "trade_date": trade_date.isoformat(),
                        "missing_tables": list(missing),
                        "event_type_counts": {},
                        "delivery_status_counts": {},
                        "outbox_status_counts": {},
                        "outbox_command_type_counts": {},
                        "timer_status_counts": {},
                        "timer_occurrence_status_counts": {},
                        "diagnostic_reason_family_counts": {},
                        "predecessor_gap_count": 0,
                        "mapping_lineage_pending_count": 0,
                        "expired_dispatching_lease_count": 0,
                        "oldest_delivery_lag_seconds": 0,
                        "oldest_due_timer_lag_seconds": 0,
                        "runtime_status": "UNKNOWN",
                        "recent_command_chains": [],
                        "limit": limit,
                        "truncated": False,
                        "next_cursor": None,
                        "read_only": True,
                    }
                cur.execute(
                    "SELECT trade_date FROM qmt_strategy.execution_runtime WHERE runtime_id=%s",
                    (runtime_id,),
                )
                runtime_row = cur.fetchone()
                if runtime_row is None:
                    runtime_status = "NOT_FOUND"
                elif runtime_row["trade_date"] != trade_date:
                    raise ValueError("runtime_id belongs to a different trade_date")
                else:
                    cur.execute(
                        """
                        SELECT
                          (SELECT COUNT(*) FROM qmt_strategy.execution_runtime_event
                            WHERE runtime_id=%s AND event_contract_version='KERNEL_V2')
                        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_instance
                            WHERE runtime_id=%s AND kernel_contract_version='KERNEL_V2')
                        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_command_outbox
                            WHERE runtime_id=%s)
                        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_timer_schedule
                            WHERE runtime_id=%s) AS count
                        """,
                        (runtime_id, runtime_id, runtime_id, runtime_id),
                    )
                    runtime_status = "ACTIVE" if int(cur.fetchone()["count"]) > 0 else "NOT_ACTIVATED"
                event_type_counts = self._diagnostic_group_counts(
                    cur,
                    table="execution_runtime_event",
                    key_column="event_type",
                    runtime_id=runtime_id,
                    trade_date=trade_date,
                    extra_predicate="target.event_contract_version='KERNEL_V2'",
                )
                delivery_status_counts = self._diagnostic_group_counts(
                    cur,
                    table="execution_algo_event_delivery",
                    key_column="status",
                    runtime_id=runtime_id,
                    trade_date=trade_date,
                )
                outbox_status_counts = self._diagnostic_group_counts(
                    cur,
                    table="execution_algo_command_outbox",
                    key_column="status",
                    runtime_id=runtime_id,
                    trade_date=trade_date,
                )
                outbox_command_type_counts = self._diagnostic_group_counts(
                    cur,
                    table="execution_algo_command_outbox",
                    key_column="command_type",
                    runtime_id=runtime_id,
                    trade_date=trade_date,
                )
                timer_status_counts = self._diagnostic_group_counts(
                    cur,
                    table="execution_algo_timer_schedule",
                    key_column="status",
                    runtime_id=runtime_id,
                    trade_date=trade_date,
                )
                timer_occurrence_status_counts = self._diagnostic_group_counts(
                    cur,
                    table="execution_algo_timer_occurrence",
                    key_column="status",
                    runtime_id=runtime_id,
                    trade_date=trade_date,
                )
                cur.execute(
                    """
                    SELECT COALESCE(observation_json->>'reason_code','UNCLASSIFIED') AS reason_code,
                           COUNT(*)::bigint AS count
                    FROM qmt_strategy.execution_algo_diagnostic_observation AS target
                    JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=target.runtime_id
                    WHERE target.runtime_id=%s AND runtime.trade_date=%s
                    GROUP BY COALESCE(observation_json->>'reason_code','UNCLASSIFIED')
                    ORDER BY reason_code
                    """,
                    (runtime_id, trade_date),
                )
                diagnostic_reason_family_counts: dict[str, int] = {}
                for row in cur.fetchall():
                    family = _reason_family(str(row["reason_code"]))
                    diagnostic_reason_family_counts[family] = diagnostic_reason_family_counts.get(family, 0) + int(
                        row["count"]
                    )
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint AS count
                    FROM qmt_strategy.execution_algo_event_delivery AS target
                    JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=target.runtime_id
                    LEFT JOIN qmt_strategy.execution_algo_event_delivery AS predecessor
                      ON predecessor.algo_instance_id=target.algo_instance_id
                     AND predecessor.algo_delivery_sequence=target.previous_delivery_sequence
                     AND predecessor.delivery_id=target.previous_delivery_id
                    WHERE target.runtime_id=%s AND runtime.trade_date=%s
                      AND target.algo_delivery_sequence > 1 AND predecessor.delivery_id IS NULL
                    """,
                    (runtime_id, trade_date),
                )
                predecessor_gap_count = int(cur.fetchone()["count"])
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint AS count
                    FROM qmt_strategy.execution_algo_command_outbox AS outbox
                    JOIN qmt_strategy.execution_child_order AS child ON child.mapping_id=outbox.mapping_id
                    JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=outbox.runtime_id
                    WHERE outbox.runtime_id=%s AND runtime.trade_date=%s
                      AND outbox.status IN ('ACKED','ACKED_REJECTED')
                      AND child.mapping_status IN ('DISPATCHING','OUTCOME_UNKNOWN')
                    """,
                    (runtime_id, trade_date),
                )
                mapping_lineage_pending_count = int(cur.fetchone()["count"])
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint AS count
                    FROM qmt_strategy.execution_algo_command_outbox AS target
                    JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=target.runtime_id
                    WHERE target.runtime_id=%s AND runtime.trade_date=%s
                      AND target.status='DISPATCHING' AND target.lease_expires_at<CURRENT_TIMESTAMP
                    """,
                    (runtime_id, trade_date),
                )
                expired_dispatching_lease_count = int(cur.fetchone()["count"])
                cur.execute(
                    """
                    SELECT COALESCE(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP-MIN(target.updated_at_utc))),0)::bigint AS lag
                    FROM qmt_strategy.execution_algo_event_delivery AS target
                    JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=target.runtime_id
                    WHERE target.runtime_id=%s AND runtime.trade_date=%s
                      AND target.status IN ('PENDING','CLAIMED','FAILED_RETRYABLE')
                    """,
                    (runtime_id, trade_date),
                )
                oldest_delivery_lag_seconds = max(0, int(cur.fetchone()["lag"]))
                cur.execute(
                    """
                    SELECT COALESCE(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP-MIN(target.due_at_exchange_utc))),0)::bigint AS lag
                    FROM qmt_strategy.execution_algo_timer_schedule AS target
                    JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=target.runtime_id
                    WHERE target.runtime_id=%s AND runtime.trade_date=%s
                      AND target.status IN ('SCHEDULED','EMITTING')
                      AND target.due_at_exchange_utc<CURRENT_TIMESTAMP
                    """,
                    (runtime_id, trade_date),
                )
                oldest_due_timer_lag_seconds = max(0, int(cur.fetchone()["lag"]))
                cursor_predicate = ""
                params: list[Any] = [runtime_id, trade_date]
                if cursor_values is not None:
                    cursor_predicate = (
                        "AND (target.updated_at_utc<%s OR (target.updated_at_utc=%s AND target.command_id>%s))"
                    )
                    params.extend((cursor_values[0], cursor_values[0], cursor_values[1]))
                params.append(limit + 1)
                cur.execute(
                    f"""
                    SELECT command_id,updated_at_utc
                    FROM qmt_strategy.execution_algo_command_outbox AS target
                    JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=target.runtime_id
                    WHERE target.runtime_id=%s AND runtime.trade_date=%s
                    {cursor_predicate}
                    ORDER BY target.updated_at_utc DESC,target.command_id
                    LIMIT %s
                    """,
                    tuple(params),
                )
                command_rows = tuple(cur.fetchall())
        returned_rows = command_rows[:limit]
        returned_ids = tuple(str(row["command_id"]) for row in returned_rows)
        chains = []
        for command_id in returned_ids:
            chain = self.read_command_identity_chain(command_id)
            chains.append(
                {
                    "command_id": command_id,
                    "mapping": chain["mapping"].model_dump(mode="json"),
                    "outbox": chain["outbox"].model_dump(mode="json"),
                }
            )
        return {
            "schema_version": "miniqmt_kernel_diagnostics_v1",
            "schema_status": "READY",
            "runtime_id": runtime_id,
            "trade_date": trade_date.isoformat(),
            "missing_tables": [],
            "event_type_counts": event_type_counts,
            "delivery_status_counts": delivery_status_counts,
            "outbox_status_counts": outbox_status_counts,
            "outbox_command_type_counts": outbox_command_type_counts,
            "timer_status_counts": timer_status_counts,
            "timer_occurrence_status_counts": timer_occurrence_status_counts,
            "diagnostic_reason_family_counts": dict(sorted(diagnostic_reason_family_counts.items())),
            "predecessor_gap_count": predecessor_gap_count,
            "mapping_lineage_pending_count": mapping_lineage_pending_count,
            "expired_dispatching_lease_count": expired_dispatching_lease_count,
            "oldest_delivery_lag_seconds": oldest_delivery_lag_seconds,
            "oldest_due_timer_lag_seconds": oldest_due_timer_lag_seconds,
            "runtime_status": runtime_status,
            "recent_command_chains": chains,
            "limit": limit,
            "truncated": len(command_rows) > limit,
            "next_cursor": (
                _encode_kernel_cursor(returned_rows[-1]["updated_at_utc"], str(returned_rows[-1]["command_id"]))
                if len(command_rows) > limit and returned_rows
                else None
            ),
            "read_only": True,
        }

    @staticmethod
    def _diagnostic_group_counts(
        cur: Any,
        *,
        table: str,
        key_column: str,
        runtime_id: str,
        trade_date: date,
        extra_predicate: str = "TRUE",
    ) -> dict[str, int]:
        authority = {
            ("execution_runtime_event", "event_type"),
            ("execution_algo_event_delivery", "status"),
            ("execution_algo_command_outbox", "status"),
            ("execution_algo_command_outbox", "command_type"),
            ("execution_algo_timer_schedule", "status"),
            ("execution_algo_timer_occurrence", "status"),
        }
        if (table, key_column) not in authority:
            raise ValueError("unsupported diagnostics aggregate authority")
        if extra_predicate not in {"TRUE", "target.event_contract_version='KERNEL_V2'"}:
            raise ValueError("unsupported diagnostics predicate")
        cur.execute(
            f"""
            SELECT target.{key_column} AS key,COUNT(*)::bigint AS count
            FROM qmt_strategy.{table} AS target
            JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=target.runtime_id
            WHERE target.runtime_id=%s AND runtime.trade_date=%s AND {extra_predicate}
            GROUP BY target.{key_column}
            ORDER BY target.{key_column}
            """,
            (runtime_id, trade_date),
        )
        return {str(row["key"]): int(row["count"]) for row in cur.fetchall()}


def _reason_family(reason_code: str) -> str:
    normalized = reason_code.strip().upper()
    if not normalized:
        return "UNCLASSIFIED"
    for family in ("FENCE", "PREDECESSOR", "OUTCOME_UNKNOWN", "RECONCILE", "DISPATCH", "TIMER", "INGRESS"):
        if family in normalized:
            return family
    return "OTHER"


def _encode_kernel_cursor(updated_at_utc: Any, command_id: str) -> str:
    timestamp = canonical_utc_datetime_v1(updated_at_utc, field_name="updated_at_utc")
    if type(command_id) is not str or not command_id.strip() or command_id != command_id.strip():
        raise ValueError("diagnostics cursor command identity is invalid")
    return f"{timestamp}|{command_id}"


def _decode_kernel_cursor(cursor: str | None) -> tuple[str, str] | None:
    if cursor is None:
        return None
    if type(cursor) is not str or cursor != cursor.strip() or cursor.count("|") != 1:
        raise ValueError("kernel diagnostics cursor is invalid")
    timestamp, command_id = cursor.split("|", 1)
    timestamp = canonical_utc_datetime_v1(timestamp, field_name="cursor.updated_at_utc")
    if not command_id or command_id != command_id.strip():
        raise ValueError("kernel diagnostics cursor command identity is invalid")
    return timestamp, command_id
