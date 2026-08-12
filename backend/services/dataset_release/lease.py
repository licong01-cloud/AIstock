"""Persistent leases, fencing, and orphan-safe ownership transfer."""

from __future__ import annotations

import hashlib
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Iterable

from .control_store import ControlStore, StateConflict, append_event


class LeaseConflict(StateConflict):
    """A resource is already owned or a stale fence attempted a mutation."""


class OrphanNotQuiescent(LeaseConflict):
    """Recovery attempted before every owned descendant was proven dead."""


@dataclass(frozen=True, slots=True)
class LeaseToken:
    resource_key: str
    fence: int
    attempt_id: str
    owner_identity: str


@dataclass(frozen=True, slots=True)
class ClaimedAttempt:
    attempt_id: str
    ordinal: int
    attempt_fence: int
    host: LeaseToken | None = None
    release: LeaseToken | None = None
    resolution: LeaseToken | None = None


class LeaseManager:
    """Own all lease CAS operations; lock files are never authoritative."""

    def __init__(self, store: ControlStore) -> None:
        self.store = store

    def claim_resolution(
        self,
        *,
        submission_id: str,
        owner_identity: str,
        ttl_seconds: float,
        acquire_host: bool = False,
        host: str | None = None,
        owner_pid: int | None = None,
        owner_create_time: str | None = None,
        worker_instance_id: str | None = None,
        code_sha: str | None = None,
        capability_digest: str | None = None,
        requested_ram: int | None = None,
        db_connections: int | None = None,
        io_class: str | None = None,
        hybrid_wsl: bool = False,
        now: datetime | None = None,
    ) -> ClaimedAttempt:
        observed = _as_utc(now)
        with self.store.transaction() as connection:
            submission = connection.execute(
                "SELECT * FROM submissions WHERE submission_id=?", (submission_id,)
            ).fetchone()
            if submission is None or submission["state"] != "QUEUED_RESOLUTION":
                raise LeaseConflict("submission is not claimable for resolution")
            if submission["resolution_attempt_id"] is not None:
                raise LeaseConflict("submission already has an active resolution attempt")
            ordinal = int(
                connection.execute(
                    "SELECT COALESCE(MAX(ordinal),0)+1 FROM resolution_attempts WHERE submission_id=?",
                    (submission_id,),
                ).fetchone()[0]
            )
            attempt_id = f"dsra_{uuid.uuid4().hex}"
            logical = str(submission["logical_request_key"])
            resolution_key = f"resolution:{hashlib.sha256(logical.encode('utf-8')).hexdigest()}"
            resolution = self._claim_resource(
                connection,
                resource_key=resolution_key,
                attempt_id=attempt_id,
                run_id=None,
                attempt_kind="RESOLUTION",
                attempt_fence=ordinal,
                owner_identity=owner_identity,
                ttl_seconds=ttl_seconds,
                observed=observed,
                host=host,
                owner_pid=owner_pid,
                owner_create_time=owner_create_time,
                worker_instance_id=worker_instance_id,
                code_sha=code_sha,
                capability_digest=capability_digest,
                requested_ram=requested_ram,
                db_connections=db_connections,
                io_class=io_class,
                hybrid_wsl=hybrid_wsl,
            )
            host_token = None
            if acquire_host:
                host_token = self._claim_resource(
                    connection,
                    resource_key="host:heavy-dataset",
                    attempt_id=attempt_id,
                    run_id=None,
                    attempt_kind="RESOLUTION",
                    attempt_fence=ordinal,
                    owner_identity=owner_identity,
                    ttl_seconds=ttl_seconds,
                    observed=observed,
                    host=host,
                    owner_pid=owner_pid,
                    owner_create_time=owner_create_time,
                    worker_instance_id=worker_instance_id,
                    code_sha=code_sha,
                    capability_digest=capability_digest,
                    requested_ram=requested_ram,
                    db_connections=db_connections,
                    io_class=io_class,
                    hybrid_wsl=hybrid_wsl,
                )
            stamp = _iso(observed)
            connection.execute(
                """
                INSERT INTO resolution_attempts(
                    resolution_attempt_id,submission_id,logical_request_key,ordinal,state,
                    owner,fence,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    attempt_id,
                    submission_id,
                    logical,
                    ordinal,
                    "CLAIMED",
                    owner_identity,
                    resolution.fence,
                    stamp,
                    stamp,
                ),
            )
            updated = connection.execute(
                """
                UPDATE submissions
                SET state='RESOLVING_SOURCE', resolution_attempt_id=?, row_version=row_version+1, updated_at=?
                WHERE submission_id=? AND state='QUEUED_RESOLUTION' AND resolution_attempt_id IS NULL
                """,
                (attempt_id, stamp, submission_id),
            )
            if updated.rowcount != 1:
                raise LeaseConflict("submission claim CAS failed")
            append_event(
                connection,
                event_type="RESOLUTION_CLAIMED",
                submission_id=submission_id,
                resolution_attempt_id=attempt_id,
                created_at=stamp,
            )
            return ClaimedAttempt(
                attempt_id=attempt_id,
                ordinal=ordinal,
                attempt_fence=resolution.fence,
                host=host_token,
                resolution=resolution,
            )

    def claim_build(
        self,
        *,
        run_id: str,
        release_id: str,
        owner_identity: str,
        ttl_seconds: float,
        attempt_kind: str = "BUILD",
        host: str | None = None,
        owner_pid: int | None = None,
        owner_create_time: str | None = None,
        worker_instance_id: str | None = None,
        code_sha: str | None = None,
        capability_digest: str | None = None,
        requested_ram: int | None = None,
        db_connections: int | None = None,
        io_class: str | None = None,
        hybrid_wsl: bool = False,
        staging_ref: str | None = None,
        now: datetime | None = None,
    ) -> ClaimedAttempt:
        observed = _as_utc(now)
        with self.store.transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if run is None or run["state"] != "QUEUED" or run["active_attempt_id"] is not None:
                raise LeaseConflict("run is not claimable")
            ordinal = int(
                connection.execute(
                    "SELECT COALESCE(MAX(ordinal),0)+1 FROM attempts WHERE run_id=?", (run_id,)
                ).fetchone()[0]
            )
            attempt_id = f"dsa_{uuid.uuid4().hex}"
            stamp = _iso(observed)
            connection.execute(
                """
                INSERT INTO attempts(
                    attempt_id,run_id,ordinal,attempt_kind,state,owner,attempt_fence,
                    staging_ref,owner_pid,owner_create_time,worker_instance_id,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    attempt_id,
                    run_id,
                    ordinal,
                    attempt_kind,
                    "CLAIMED",
                    owner_identity,
                    ordinal,
                    staging_ref,
                    owner_pid,
                    owner_create_time,
                    worker_instance_id,
                    stamp,
                    stamp,
                ),
            )
            common = {
                "attempt_id": attempt_id,
                "run_id": run_id,
                "attempt_kind": attempt_kind,
                "attempt_fence": ordinal,
                "owner_identity": owner_identity,
                "ttl_seconds": ttl_seconds,
                "observed": observed,
                "host": host,
                "owner_pid": owner_pid,
                "owner_create_time": owner_create_time,
                "worker_instance_id": worker_instance_id,
                "code_sha": code_sha,
                "capability_digest": capability_digest,
                "requested_ram": requested_ram,
                "db_connections": db_connections,
                "io_class": io_class,
                "hybrid_wsl": hybrid_wsl,
            }
            # Fixed order and one SQLite transaction guarantee all-or-nothing.
            host_token = self._claim_resource(connection, resource_key="host:heavy-dataset", **common)
            release_token = self._claim_resource(connection, resource_key=f"release:{release_id}", **common)
            connection.execute(
                "UPDATE attempts SET host_fence=?,release_fence=?,state='RUNNING',updated_at=? WHERE attempt_id=?",
                (host_token.fence, release_token.fence, stamp, attempt_id),
            )
            next_state = "REATTESTING" if attempt_kind == "REATTEST" else "EXECUTING"
            updated = connection.execute(
                """
                UPDATE runs SET state=?,active_attempt_id=?,row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state='QUEUED' AND active_attempt_id IS NULL
                """,
                (next_state, attempt_id, stamp, run_id),
            )
            if updated.rowcount != 1:
                raise LeaseConflict("run claim CAS failed")
            append_event(
                connection,
                event_type="ATTEMPT_CLAIMED",
                run_id=run_id,
                attempt_id=attempt_id,
                created_at=stamp,
            )
            return ClaimedAttempt(
                attempt_id=attempt_id,
                ordinal=ordinal,
                attempt_fence=ordinal,
                host=host_token,
                release=release_token,
            )

    def heartbeat(
        self,
        tokens: Iterable[LeaseToken],
        *,
        ttl_seconds: float,
        now: datetime | None = None,
    ) -> None:
        observed = _as_utc(now)
        stamp = _iso(observed)
        expires = _iso(observed + timedelta(seconds=_positive_ttl(ttl_seconds)))
        with self.store.transaction() as connection:
            for token in tokens:
                updated = connection.execute(
                    """
                    UPDATE leases SET heartbeat_at=?,expires_at=?
                    WHERE resource_key=? AND attempt_id=? AND owner_identity=?
                      AND fence_counter=? AND state='ACTIVE'
                    """,
                    (
                        stamp,
                        expires,
                        token.resource_key,
                        token.attempt_id,
                        token.owner_identity,
                        token.fence,
                    ),
                )
                if updated.rowcount != 1:
                    raise LeaseConflict(f"stale lease heartbeat: {token.resource_key}")

    def release_attempt(
        self,
        *,
        run_id: str,
        attempt_id: str,
        tokens: Iterable[LeaseToken],
        attempt_state: str,
        run_state: str,
        outcome: str | None = None,
        now: datetime | None = None,
    ) -> None:
        observed = _as_utc(now)
        with self.store.transaction() as connection:
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?", (attempt_id, run_id)
            ).fetchone()
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if attempt is None or run is None or run["active_attempt_id"] != attempt_id:
                raise LeaseConflict("attempt is not the active run owner")
            self._release_exact(connection, tokens, observed=observed)
            stamp = _iso(observed)
            connection.execute(
                "UPDATE attempts SET state=?,updated_at=? WHERE attempt_id=?",
                (attempt_state, stamp, attempt_id),
            )
            updated = connection.execute(
                """
                UPDATE runs SET state=?,outcome=?,active_attempt_id=NULL,
                    row_version=row_version+1,updated_at=?
                WHERE run_id=? AND active_attempt_id=?
                """,
                (run_state, outcome, stamp, run_id, attempt_id),
            )
            if updated.rowcount != 1:
                raise LeaseConflict("run release CAS failed")
            append_event(
                connection,
                event_type="ATTEMPT_RELEASED",
                run_id=run_id,
                attempt_id=attempt_id,
                created_at=stamp,
            )

    def mark_orphan_hold(
        self,
        *,
        run_id: str,
        attempt_id: str,
        tree_status: str,
        now: datetime | None = None,
    ) -> None:
        if tree_status not in {"alive", "unknown"}:
            raise ValueError("ORPHAN_HOLD requires alive or unknown process-tree status")
        observed = _as_utc(now)
        stamp = _iso(observed)
        with self.store.transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?", (attempt_id, run_id)
            ).fetchone()
            if run is None or attempt is None or run["active_attempt_id"] != attempt_id:
                raise LeaseConflict("orphan attempt is not the active owner")
            updated = connection.execute(
                "UPDATE leases SET state='ORPHAN_HOLD' WHERE attempt_id=? AND state='ACTIVE'",
                (attempt_id,),
            )
            if updated.rowcount < 1:
                raise LeaseConflict("orphan attempt owns no active lease")
            connection.execute(
                "UPDATE attempts SET state='ORPHAN_HOLD',updated_at=? WHERE attempt_id=?",
                (stamp, attempt_id),
            )
            parent_state = (
                "WAITING_PUBLISH_RECOVERY"
                if run["state"] in {"PUBLISHING", "WAITING_PUBLISH_RECOVERY"}
                else "WAITING_ORPHAN_QUIESCENCE"
            )
            connection.execute(
                "UPDATE runs SET state=?,row_version=row_version+1,updated_at=? WHERE run_id=?",
                (parent_state, stamp, run_id),
            )
            append_event(
                connection,
                event_type=("ORPHAN_PROCESS_ACTIVE" if tree_status == "alive" else "OWNER_LIVENESS_UNKNOWN"),
                run_id=run_id,
                attempt_id=attempt_id,
                created_at=stamp,
            )

    def mark_resolution_orphan_hold(
        self,
        *,
        submission_id: str,
        resolution_attempt_id: str,
        tree_status: str,
        now: datetime | None = None,
    ) -> None:
        if tree_status not in {"alive", "unknown"}:
            raise ValueError("ORPHAN_HOLD requires alive or unknown process-tree status")
        observed = _as_utc(now)
        stamp = _iso(observed)
        with self.store.transaction() as connection:
            submission = connection.execute(
                "SELECT * FROM submissions WHERE submission_id=?", (submission_id,)
            ).fetchone()
            attempt = connection.execute(
                "SELECT * FROM resolution_attempts WHERE resolution_attempt_id=?",
                (resolution_attempt_id,),
            ).fetchone()
            if (
                submission is None
                or attempt is None
                or submission["state"] != "RESOLVING_SOURCE"
                or submission["resolution_attempt_id"] != resolution_attempt_id
                or attempt["submission_id"] != submission_id
                or attempt["state"] not in {"CLAIMED", "RUNNING"}
            ):
                raise LeaseConflict("resolution orphan is not the active submission owner")
            updated = connection.execute(
                "UPDATE leases SET state='ORPHAN_HOLD' WHERE attempt_id=? AND state='ACTIVE'",
                (resolution_attempt_id,),
            )
            if updated.rowcount < 1:
                raise LeaseConflict("resolution orphan owns no active lease")
            connection.execute(
                "UPDATE resolution_attempts SET state='ORPHAN_HOLD',updated_at=? WHERE resolution_attempt_id=?",
                (stamp, resolution_attempt_id),
            )
            connection.execute(
                """
                UPDATE submissions SET state='WAITING_ORPHAN_QUIESCENCE',
                    row_version=row_version+1,updated_at=? WHERE submission_id=?
                """,
                (stamp, submission_id),
            )
            append_event(
                connection,
                event_type=("ORPHAN_PROCESS_ACTIVE" if tree_status == "alive" else "OWNER_LIVENESS_UNKNOWN"),
                submission_id=submission_id,
                resolution_attempt_id=resolution_attempt_id,
                created_at=stamp,
            )

    def release_resolution_orphan_after_quiescence(
        self,
        *,
        submission_id: str,
        resolution_attempt_id: str,
        tree_quiescent: bool,
        now: datetime | None = None,
    ) -> None:
        if not tree_quiescent:
            raise OrphanNotQuiescent("full Windows and WSL process tree is not quiescent")
        observed = _as_utc(now)
        stamp = _iso(observed)
        with self.store.transaction() as connection:
            submission = connection.execute(
                "SELECT * FROM submissions WHERE submission_id=?", (submission_id,)
            ).fetchone()
            attempt = connection.execute(
                "SELECT * FROM resolution_attempts WHERE resolution_attempt_id=?",
                (resolution_attempt_id,),
            ).fetchone()
            if (
                submission is None
                or attempt is None
                or submission["state"] != "WAITING_ORPHAN_QUIESCENCE"
                or submission["resolution_attempt_id"] != resolution_attempt_id
                or attempt["state"] != "ORPHAN_HOLD"
            ):
                raise LeaseConflict("resolution orphan is not reclaimable")
            released = self.release_by_attempt_in_transaction(
                connection, attempt_id=resolution_attempt_id, observed=observed
            )
            if released < 1:
                raise LeaseConflict("resolution orphan retained no held leases")
            connection.execute(
                "UPDATE resolution_attempts SET state='EXPIRED',updated_at=? WHERE resolution_attempt_id=?",
                (stamp, resolution_attempt_id),
            )
            connection.execute(
                """
                UPDATE submissions SET state='QUEUED_RESOLUTION',resolution_attempt_id=NULL,
                    row_version=row_version+1,updated_at=? WHERE submission_id=?
                """,
                (stamp, submission_id),
            )
            append_event(
                connection,
                event_type="RESOLUTION_ORPHAN_QUIESCENT_REQUEUED",
                submission_id=submission_id,
                resolution_attempt_id=resolution_attempt_id,
                created_at=stamp,
            )

    def release_orphan_after_quiescence(
        self,
        *,
        run_id: str,
        attempt_id: str,
        tree_quiescent: bool,
        now: datetime | None = None,
    ) -> None:
        if not tree_quiescent:
            raise OrphanNotQuiescent("full Windows and WSL process tree is not quiescent")
        observed = _as_utc(now)
        stamp = _iso(observed)
        with self.store.transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?", (attempt_id, run_id)
            ).fetchone()
            if (
                run is None
                or attempt is None
                or attempt["state"] != "ORPHAN_HOLD"
                or run["active_attempt_id"] != attempt_id
                or run["state"] != "WAITING_ORPHAN_QUIESCENCE"
            ):
                raise LeaseConflict("attempt is not a reclaimable pre-publish orphan")
            self.release_by_attempt_in_transaction(connection, attempt_id=attempt_id, observed=observed)
            connection.execute(
                "UPDATE attempts SET state='EXPIRED',updated_at=? WHERE attempt_id=?",
                (stamp, attempt_id),
            )
            connection.execute(
                """
                UPDATE runs SET state='QUEUED',active_attempt_id=NULL,
                    row_version=row_version+1,updated_at=? WHERE run_id=?
                """,
                (stamp, run_id),
            )
            append_event(
                connection,
                event_type="ORPHAN_QUIESCENT_REQUEUED",
                run_id=run_id,
                attempt_id=attempt_id,
                created_at=stamp,
            )

    def handoff_publish_finalizer(
        self,
        *,
        run_id: str,
        old_attempt_id: str,
        new_owner_identity: str,
        ttl_seconds: float,
        tree_quiescent: bool,
        host: str | None = None,
        owner_pid: int | None = None,
        owner_create_time: str | None = None,
        worker_instance_id: str | None = None,
        code_sha: str | None = None,
        capability_digest: str | None = None,
        now: datetime | None = None,
    ) -> ClaimedAttempt:
        """Atomically transfer held host/release leases without a FREE window."""

        if not tree_quiescent:
            raise OrphanNotQuiescent("cannot adopt finalizer before old tree is quiescent")
        observed = _as_utc(now)
        stamp = _iso(observed)
        expires = _iso(observed + timedelta(seconds=_positive_ttl(ttl_seconds)))
        with self.store.transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            old = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?",
                (old_attempt_id, run_id),
            ).fetchone()
            if (
                run is None
                or old is None
                or run["state"] != "WAITING_PUBLISH_RECOVERY"
                or run["active_attempt_id"] != old_attempt_id
                or old["state"] != "ORPHAN_HOLD"
            ):
                raise LeaseConflict("publish orphan is not eligible for finalizer handoff")
            held = connection.execute(
                "SELECT * FROM leases WHERE attempt_id=? AND state='ORPHAN_HOLD' ORDER BY resource_key",
                (old_attempt_id,),
            ).fetchall()
            if len(held) != 2 or {str(row["resource_key"]).split(":", 1)[0] for row in held} != {
                "host",
                "release",
            }:
                raise LeaseConflict("publish finalizer handoff requires exactly host and release leases")
            ordinal = int(
                connection.execute(
                    "SELECT COALESCE(MAX(ordinal),0)+1 FROM attempts WHERE run_id=?", (run_id,)
                ).fetchone()[0]
            )
            attempt_id = f"dsa_{uuid.uuid4().hex}"
            connection.execute(
                """
                INSERT INTO attempts(
                    attempt_id,run_id,ordinal,attempt_kind,state,owner,attempt_fence,
                    owner_pid,owner_create_time,worker_instance_id,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    attempt_id,
                    run_id,
                    ordinal,
                    "FINALIZER_RECOVERY",
                    "RUNNING",
                    new_owner_identity,
                    ordinal,
                    owner_pid,
                    owner_create_time,
                    worker_instance_id,
                    stamp,
                    stamp,
                ),
            )
            tokens: dict[str, LeaseToken] = {}
            for row in held:
                next_fence = int(row["fence_counter"]) + 1
                updated_lease = connection.execute(
                    """
                    UPDATE leases SET fence_counter=?,state='ACTIVE',attempt_kind='FINALIZER_RECOVERY',
                        attempt_id=?,owner_identity=?,attempt_fence=?,acquired_at=?,heartbeat_at=?,expires_at=?,
                        host=?,owner_pid=?,owner_create_time=?,worker_instance_id=?,code_sha=?,
                        capability_digest=?,requested_ram=0,db_connections=0,
                        io_class='publish-finalizer',hybrid_wsl=0
                    WHERE resource_key=? AND attempt_id=? AND state='ORPHAN_HOLD' AND fence_counter=?
                    """,
                    (
                        next_fence,
                        attempt_id,
                        new_owner_identity,
                        ordinal,
                        stamp,
                        stamp,
                        expires,
                        host,
                        owner_pid,
                        owner_create_time,
                        worker_instance_id,
                        code_sha,
                        capability_digest,
                        row["resource_key"],
                        old_attempt_id,
                        row["fence_counter"],
                    ),
                )
                if updated_lease.rowcount != 1:
                    raise LeaseConflict("publish finalizer lease handoff CAS failed")
                tokens[str(row["resource_key"]).split(":", 1)[0]] = LeaseToken(
                    str(row["resource_key"]), next_fence, attempt_id, new_owner_identity
                )
            expired_old = connection.execute(
                "UPDATE attempts SET state='EXPIRED',updated_at=? WHERE attempt_id=?",
                (stamp, old_attempt_id),
            )
            if expired_old.rowcount != 1:
                raise LeaseConflict("publish finalizer old-attempt handoff CAS failed")
            connection.execute(
                "UPDATE attempts SET host_fence=?,release_fence=? WHERE attempt_id=?",
                (tokens["host"].fence, tokens["release"].fence, attempt_id),
            )
            updated_run = connection.execute(
                """
                UPDATE runs SET state='PUBLISHING',active_attempt_id=?,row_version=row_version+1,updated_at=?
                WHERE run_id=? AND active_attempt_id=? AND state='WAITING_PUBLISH_RECOVERY'
                """,
                (attempt_id, stamp, run_id, old_attempt_id),
            )
            if updated_run.rowcount != 1:
                raise LeaseConflict("publish finalizer run handoff CAS failed")
            updated_publish = connection.execute(
                """
                UPDATE publish_records SET finalized_by_attempt_id=?,finalized_by_fence=?,updated_at=?
                WHERE run_id=? AND state IN ('PREPARED','FILES_COMMITTED')
                """,
                (attempt_id, ordinal, stamp, run_id),
            )
            if updated_publish.rowcount != 1:
                raise LeaseConflict("publish finalizer record handoff CAS failed")
            append_event(
                connection,
                event_type="FINALIZER_RECOVERY_ADOPTED",
                run_id=run_id,
                attempt_id=attempt_id,
                created_at=stamp,
            )
            return ClaimedAttempt(
                attempt_id=attempt_id,
                ordinal=ordinal,
                attempt_fence=ordinal,
                host=tokens["host"],
                release=tokens["release"],
            )

    def release_by_attempt_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        attempt_id: str,
        observed: datetime | None = None,
    ) -> int:
        stamp = _iso(_as_utc(observed))
        cursor = connection.execute(
            """
            UPDATE leases SET state='FREE',attempt_kind=NULL,attempt_id=NULL,run_id=NULL,
                owner_identity=NULL,host=NULL,owner_pid=NULL,owner_create_time=NULL,
                worker_instance_id=NULL,code_sha=NULL,capability_digest=NULL,attempt_fence=NULL,
                acquired_at=NULL,heartbeat_at=?,expires_at=NULL,requested_ram=NULL,
                db_connections=NULL,io_class=NULL,hybrid_wsl=0
            WHERE attempt_id=? AND state IN ('ACTIVE','ORPHAN_HOLD')
            """,
            (stamp, attempt_id),
        )
        return int(cursor.rowcount)

    def _claim_resource(
        self,
        connection: sqlite3.Connection,
        *,
        resource_key: str,
        attempt_id: str,
        run_id: str | None,
        attempt_kind: str,
        attempt_fence: int,
        owner_identity: str,
        ttl_seconds: float,
        observed: datetime,
        host: str | None = None,
        owner_pid: int | None = None,
        owner_create_time: str | None = None,
        worker_instance_id: str | None = None,
        code_sha: str | None = None,
        capability_digest: str | None = None,
        requested_ram: int | None = None,
        db_connections: int | None = None,
        io_class: str | None = None,
        hybrid_wsl: bool = False,
    ) -> LeaseToken:
        existing = connection.execute("SELECT * FROM leases WHERE resource_key=?", (resource_key,)).fetchone()
        if existing is not None and existing["state"] != "FREE":
            # Expiry alone is never sufficient: process-tree quiescence must
            # first be persisted through the explicit orphan recovery path.
            raise LeaseConflict(f"resource lease is not FREE: {resource_key}")
        fence = int(existing["fence_counter"] if existing is not None else 0) + 1
        stamp = _iso(observed)
        expires = _iso(observed + timedelta(seconds=_positive_ttl(ttl_seconds)))
        values = (
            fence,
            "ACTIVE",
            attempt_kind,
            attempt_id,
            run_id,
            owner_identity,
            host,
            owner_pid,
            owner_create_time,
            worker_instance_id,
            code_sha,
            capability_digest,
            attempt_fence,
            stamp,
            stamp,
            expires,
            requested_ram,
            db_connections,
            io_class,
            int(bool(hybrid_wsl)),
            resource_key,
        )
        if existing is None:
            connection.execute(
                """
                INSERT INTO leases(
                    fence_counter,state,attempt_kind,attempt_id,run_id,owner_identity,host,
                    owner_pid,owner_create_time,worker_instance_id,code_sha,capability_digest,
                    attempt_fence,acquired_at,heartbeat_at,expires_at,requested_ram,
                    db_connections,io_class,hybrid_wsl,resource_key
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                values,
            )
        else:
            updated = connection.execute(
                """
                UPDATE leases SET fence_counter=?,state=?,attempt_kind=?,attempt_id=?,run_id=?,
                    owner_identity=?,host=?,owner_pid=?,owner_create_time=?,worker_instance_id=?,
                    code_sha=?,capability_digest=?,attempt_fence=?,acquired_at=?,heartbeat_at=?,
                    expires_at=?,requested_ram=?,db_connections=?,io_class=?,hybrid_wsl=?
                WHERE resource_key=? AND state='FREE'
                """,
                values,
            )
            if updated.rowcount != 1:
                raise LeaseConflict(f"lease claim CAS failed: {resource_key}")
        return LeaseToken(resource_key, fence, attempt_id, owner_identity)

    @staticmethod
    def _release_exact(
        connection: sqlite3.Connection,
        tokens: Iterable[LeaseToken],
        *,
        observed: datetime,
    ) -> None:
        stamp = _iso(observed)
        for token in tokens:
            updated = connection.execute(
                """
                UPDATE leases SET state='FREE',attempt_kind=NULL,attempt_id=NULL,run_id=NULL,
                owner_identity=NULL,host=NULL,owner_pid=NULL,owner_create_time=NULL,
                worker_instance_id=NULL,code_sha=NULL,capability_digest=NULL,attempt_fence=NULL,
                acquired_at=NULL,heartbeat_at=?,expires_at=NULL,requested_ram=NULL,
                db_connections=NULL,io_class=NULL,hybrid_wsl=0
                WHERE resource_key=? AND attempt_id=? AND owner_identity=?
                  AND fence_counter=? AND state IN ('ACTIVE','ORPHAN_HOLD')
                """,
                (
                    stamp,
                    token.resource_key,
                    token.attempt_id,
                    token.owner_identity,
                    token.fence,
                ),
            )
            if updated.rowcount != 1:
                raise LeaseConflict(f"stale lease release: {token.resource_key}")


def _positive_ttl(value: float) -> float:
    ttl = float(value)
    if ttl <= 0:
        raise ValueError("lease TTL must be positive")
    return ttl


def _as_utc(value: datetime | None) -> datetime:
    observed = value or datetime.now(UTC)
    if observed.tzinfo is None:
        raise ValueError("lease timestamps must be timezone-aware")
    return observed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")
