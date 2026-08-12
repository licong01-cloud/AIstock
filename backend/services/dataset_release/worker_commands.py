"""Atomic Worker-side application of durable cancel and resume commands."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime

from .control_store import ControlStore, StateConflict, append_event
from .lease import LeaseManager
from .worker_identity import WorkerIdentity


@dataclass(frozen=True, slots=True)
class CommandResult:
    command_id: str
    command_type: str
    target_type: str
    target_id: str
    state: str
    target_state: str
    fence: int


class WorkerCommandCoordinator:
    """Claim and apply one short command in one SQLite transaction.

    The command lease fence is durable, but ACTIVE ownership is intentionally
    transaction-local: a crash rolls back the claim and target mutation
    together, so no command-specific orphan can be exposed.
    """

    _UNOWNED_CANCEL_RUN_STATES = {
        "QUEUED",
        "WAITING_RESOURCE",
        "WAITING_PERFORMANCE_REGRESSION",
    }
    _ACTIVE_CANCEL_RUN_STATES = {
        "REATTESTING",
        "FINALIZING_ATTESTATION",
        "EXECUTING",
        "VALIDATING",
        "PREPARING_PUBLISH",
        "CANCEL_REQUESTED",
    }
    _RESUME_TO_QUEUED_STATES = {
        "FAILED_RETRYABLE",
        "WAITING_RESOURCE",
        "WAITING_PERFORMANCE_REGRESSION",
    }

    def __init__(self, store: ControlStore) -> None:
        self.store = store
        self.leases = LeaseManager(store)

    def claim_and_apply_one(
        self,
        *,
        identity: WorkerIdentity,
        ttl_seconds: float,
        now: datetime,
    ) -> CommandResult | None:
        observed = _utc(now)
        stamp = _iso(observed)
        with self.store.transaction() as connection:
            command = connection.execute(
                "SELECT * FROM commands WHERE state='QUEUED' ORDER BY created_at,command_id LIMIT 1"
            ).fetchone()
            if command is None:
                return None
            command_id = str(command["command_id"])
            command_attempt_id = f"command:{command_id}"
            token = self.leases._claim_resource(
                connection,
                resource_key=f"command:{command_id}",
                attempt_id=command_attempt_id,
                run_id=str(command["run_id"]) if command["run_id"] else None,
                attempt_kind="COMMAND",
                attempt_fence=1,
                owner_identity=identity.owner_identity,
                ttl_seconds=ttl_seconds,
                observed=observed,
                host=identity.host,
                owner_pid=identity.pid,
                owner_create_time=identity.process_create_time,
                worker_instance_id=identity.instance_id,
                code_sha=identity.code_sha,
                capability_digest=identity.capability_digest,
            )
            claimed = connection.execute(
                "UPDATE commands SET state='CLAIMED' WHERE command_id=? AND state='QUEUED'",
                (command_id,),
            )
            if claimed.rowcount != 1:
                raise StateConflict("command claim CAS failed")
            target_state, command_state = self._apply(connection, command, stamp=stamp)
            self.leases._release_exact(connection, (token,), observed=observed)
            applied_at = stamp if command_state not in {"QUEUED", "PENDING"} else None
            updated = connection.execute(
                "UPDATE commands SET state=?,applied_at=? WHERE command_id=? AND state='CLAIMED'",
                (command_state, applied_at, command_id),
            )
            if updated.rowcount != 1:
                raise StateConflict("command completion CAS failed")
            append_event(
                connection,
                event_type=f"COMMAND_{command_state}",
                submission_id=command["submission_id"],
                run_id=command["run_id"],
                payload_ref=command_id,
                created_at=stamp,
            )
            return CommandResult(
                command_id=command_id,
                command_type=str(command["type"]),
                target_type=str(command["target_type"]),
                target_id=str(command["target_id"]),
                state=command_state,
                target_state=target_state,
                fence=token.fence,
            )

    def cancellation_requested(self, *, target_type: str, target_id: str) -> bool:
        rows = self.store._many(
            """
            SELECT command_id FROM commands
            WHERE target_type=? AND target_id=? AND type='CANCEL_REQUESTED'
              AND state IN ('QUEUED','PENDING','CLAIMED')
            LIMIT 1
            """,
            (target_type, target_id),
        )
        return bool(rows)

    @staticmethod
    def complete_pending_cancel(
        connection: sqlite3.Connection,
        *,
        target_type: str,
        target_id: str,
        stamp: str,
    ) -> int:
        cursor = connection.execute(
            """
            UPDATE commands SET state='APPLIED',applied_at=?
            WHERE target_type=? AND target_id=? AND type='CANCEL_REQUESTED'
              AND state IN ('QUEUED','PENDING','CLAIMED')
            """,
            (stamp, target_type, target_id),
        )
        return int(cursor.rowcount)

    def _apply(
        self,
        connection: sqlite3.Connection,
        command: sqlite3.Row,
        *,
        stamp: str,
    ) -> tuple[str, str]:
        command_type = str(command["type"])
        target_type = str(command["target_type"])
        target_id = str(command["target_id"])
        if command_type == "CANCEL_REQUESTED" and target_type == "submission":
            return self._cancel_submission(connection, target_id, stamp=stamp)
        if command_type == "CANCEL_REQUESTED" and target_type == "run":
            return self._cancel_run(connection, target_id, stamp=stamp)
        if command_type == "RESUME_REQUESTED" and target_type == "run":
            return self._resume_run(connection, target_id, stamp=stamp)
        raise StateConflict("durable command has an unsupported target/type pair")

    @staticmethod
    def _cancel_submission(
        connection: sqlite3.Connection,
        submission_id: str,
        *,
        stamp: str,
    ) -> tuple[str, str]:
        row = connection.execute("SELECT * FROM submissions WHERE submission_id=?", (submission_id,)).fetchone()
        if row is None:
            raise StateConflict("command target submission disappeared")
        state = str(row["state"])
        if (
            state
            in {
                "QUEUED_RESOLUTION",
                "FAILED_RETRYABLE",
                "WAITING_SOURCE",
                "WAITING_ACTIVE_RUN",
            }
            and row["resolution_attempt_id"] is None
        ):
            updated = connection.execute(
                """
                UPDATE submissions SET state='CANCELLED',row_version=row_version+1,
                    next_retry_at=NULL,updated_at=?
                WHERE submission_id=? AND state=? AND resolution_attempt_id IS NULL
                """,
                (stamp, submission_id, state),
            )
            if updated.rowcount != 1:
                raise StateConflict("submission cancellation CAS failed")
            append_event(
                connection,
                event_type="SUBMISSION_CANCELLED",
                submission_id=submission_id,
                created_at=stamp,
            )
            return "CANCELLED", "APPLIED"
        if state in {"RESOLVING_SOURCE", "WAITING_ORPHAN_QUIESCENCE"}:
            return state, "PENDING"
        return state, "REJECTED_TOO_LATE"

    def _cancel_run(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        stamp: str,
    ) -> tuple[str, str]:
        row = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise StateConflict("command target run disappeared")
        state = str(row["state"])
        if state in self._UNOWNED_CANCEL_RUN_STATES and row["active_attempt_id"] is None:
            updated = connection.execute(
                """
                UPDATE runs SET state='CANCELLED',outcome='CANCELLED',
                    row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state=? AND active_attempt_id IS NULL
                """,
                (stamp, run_id, state),
            )
            if updated.rowcount != 1:
                raise StateConflict("run cancellation CAS failed")
            append_event(
                connection,
                event_type="RUN_CANCELLED",
                run_id=run_id,
                created_at=stamp,
            )
            return "CANCELLED", "APPLIED"
        if state in self._ACTIVE_CANCEL_RUN_STATES and row["active_attempt_id"] is not None:
            return state, "PENDING"
        return state, "REJECTED_TOO_LATE"

    def _resume_run(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        stamp: str,
    ) -> tuple[str, str]:
        row = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise StateConflict("command target run disappeared")
        state = str(row["state"])
        if state in self._RESUME_TO_QUEUED_STATES and row["active_attempt_id"] is None:
            updated = connection.execute(
                """
                UPDATE runs SET state='QUEUED',outcome=NULL,row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state=? AND active_attempt_id IS NULL
                """,
                (stamp, run_id, state),
            )
            if updated.rowcount != 1:
                raise StateConflict("run resume CAS failed")
            append_event(
                connection,
                event_type="RUN_REQUEUED_BY_COMMAND",
                run_id=run_id,
                created_at=stamp,
            )
            return "QUEUED", "APPLIED"
        if row["active_attempt_id"] is not None:
            return state, "REJECTED_ACTIVE"
        if state.startswith("BLOCKED_") or state in {"FAILED_TERMINAL", "CANCELLED"}:
            return state, "REJECTED_NEW_LINEAGE_REQUIRED"
        return state, "REJECTED_INVALID_STATE"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("command timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


__all__ = ["CommandResult", "WorkerCommandCoordinator"]
