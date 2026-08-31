"""Disabled-by-default, control-only monthly catch-up reconciliation.

This module never starts a Worker or exporter.  An explicitly invoked cycle
uses one SQLite singleton lease, asks the injected official calendar resolver
for at most the configured historical months, and submits only durable
candidate-only logical requests.
"""

from __future__ import annotations

import calendar
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Callable, Literal, Mapping
from zoneinfo import ZoneInfo

from .canonical import digest_named_fields
from .cas_store import CASStore
from .contracts import LogicalRequestIdentity, Scope, SubmissionIdentity
from .control_store import NONTERMINAL_RUN_STATES, ControlStore, StateConflict
from .errors import DatasetReleaseError
from .profile import DatasetProfile
from .resolution import ResolutionService


RECONCILE_SCHEMA_VERSION = "dataset_release_reconcile_v1"
RECONCILE_PRINCIPAL = "dataset-release-reconciler"
RECONCILE_ROUTE = "worker:reconcile"
MAX_RECONCILE_CATCHUP_MONTHS = 3
MAX_ACTIVE_LOGICAL_MATCHES = 2
SHANGHAI = ZoneInfo("Asia/Shanghai")
_ACTIVE_SUBMISSION_STATES = {
    "QUEUED_RESOLUTION",
    "RESOLVING_SOURCE",
    "WAITING_SOURCE",
    "FAILED_RETRYABLE",
    "WAITING_ACTIVE_RUN",
    "WAITING_ORPHAN_QUIESCENCE",
    "CANCEL_REQUESTED",
}


class ReconcileError(DatasetReleaseError):
    code = "DATASET_RELEASE_RECONCILE_ERROR"


@dataclass(frozen=True, slots=True)
class ReconcileLease:
    profile: str
    fence: int
    owner_identity: str
    cycle_id: str


@dataclass(frozen=True, slots=True)
class ReconcileItem:
    cutoff: str
    logical_request_key: str
    disposition: Literal["SUBMITTED", "REPLAYED", "ACTIVE_REUSED", "FAILED"]
    submission_id: str | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ReconcileReport:
    state: Literal["DISABLED", "LEASE_BUSY", "COMPLETED", "PARTIAL_FAILURE"]
    profile: str
    cycle_id: str
    fence: int | None
    items: tuple[ReconcileItem, ...]

    @property
    def submitted_count(self) -> int:
        return sum(item.disposition == "SUBMITTED" for item in self.items)


class MonthlyDatasetReconciler:
    """One bounded reconcile cycle; no background loop and no heavy child."""

    def __init__(
        self,
        *,
        profile: DatasetProfile,
        store: ControlStore,
        cutoff_resolver: Callable[[datetime], date],
        enabled: bool = False,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.profile = profile
        self.store = store
        self.cas = CASStore(store.root)
        self.cutoff_resolver = cutoff_resolver
        self.enabled = bool(enabled)
        self.now = now
        if not 1 <= profile.reconcile_catchup_months <= MAX_RECONCILE_CATCHUP_MONTHS:
            raise ReconcileError("reconcile catch-up exceeds the source-ready v1 bound")
        if not 60 <= profile.reconcile_lease_ttl_seconds <= 3_600:
            raise ReconcileError("reconcile lease TTL is outside the profile contract")

    def run_once(
        self,
        *,
        owner_identity: str,
        cycle_id: str | None = None,
        scope: Scope | str = Scope.FULL,
    ) -> ReconcileReport:
        observed = _aware_utc(self.now())
        normalized_cycle = str(cycle_id or observed.astimezone(SHANGHAI).date().isoformat()).strip()
        owner = str(owner_identity).strip()
        if not owner or not normalized_cycle or len(owner) > 200 or len(normalized_cycle) > 100:
            raise ReconcileError("reconcile owner/cycle identity is invalid")
        if not self.enabled:
            return ReconcileReport("DISABLED", self.profile.profile, normalized_cycle, None, ())
        lease = self._claim(owner, normalized_cycle, observed)
        if lease is None:
            return ReconcileReport("LEASE_BUSY", self.profile.profile, normalized_cycle, None, ())
        items: list[ReconcileItem] = []
        try:
            normalized_scope = Scope(scope)
            cutoffs = self._resolve_cutoffs(observed)
            for cutoff in cutoffs:
                self._heartbeat(lease, _aware_utc(self.now()))
                try:
                    items.append(
                        self._reconcile_cutoff(
                            cutoff=cutoff,
                            scope=normalized_scope,
                            cycle_id=normalized_cycle,
                        )
                    )
                except (DatasetReleaseError, OSError, RuntimeError, ValueError) as exc:
                    logical = self._logical(cutoff, normalized_scope)
                    items.append(
                        ReconcileItem(
                            cutoff.isoformat(),
                            logical.key,
                            "FAILED",
                            detail=type(exc).__name__,
                        )
                    )
        finally:
            self._release(lease, _aware_utc(self.now()))
        state = "PARTIAL_FAILURE" if any(item.disposition == "FAILED" for item in items) else "COMPLETED"
        return ReconcileReport(
            state,
            self.profile.profile,
            normalized_cycle,
            lease.fence,
            tuple(items),
        )

    def _resolve_cutoffs(self, observed: datetime) -> tuple[date, ...]:
        values: list[date] = []
        for offset in range(self.profile.reconcile_catchup_months):
            anchor = _subtract_calendar_months(observed, offset)
            cutoff = self.cutoff_resolver(anchor)
            if not isinstance(cutoff, date):
                raise ReconcileError("official cutoff resolver returned an invalid date")
            if cutoff >= anchor.astimezone(SHANGHAI).date():
                raise ReconcileError("official cutoff must precede its reconcile anchor")
            if cutoff not in values:
                values.append(cutoff)
        if not values or len(values) > MAX_RECONCILE_CATCHUP_MONTHS:
            raise ReconcileError("official cutoff resolver produced an invalid catch-up set")
        return tuple(values)

    def _reconcile_cutoff(
        self,
        *,
        cutoff: date,
        scope: Scope,
        cycle_id: str,
    ) -> ReconcileItem:
        logical = self._logical(cutoff, scope)
        active = self._active_submission(logical.key)
        if active is not None:
            return ReconcileItem(
                cutoff.isoformat(),
                logical.key,
                "ACTIVE_REUSED",
                submission_id=str(active["submission_id"]),
            )
        request = {
            "schema_version": "dataset_release_monthly_request_v1",
            "profile": self.profile.profile,
            "cutoff_policy": "auto-previous-month",
            "cutoff_resolution_policy": self.profile.cutoff_policy,
            "resolved_cutoff": cutoff.isoformat(),
            "scope": scope.value,
            "candidate_only": True,
            "logical_request_key": logical.key,
            "semantic_profile_digest": self.profile.semantic_profile_digest,
            "resolution": "worker_required",
            "operation": "SOURCE_REVISION_PROBE",
            "reconcile_cycle_id": cycle_id,
            "activation": "not_requested",
            "node1": "not_requested",
            "db_repair": "not_requested",
            "restart": "not_requested",
            "cleanup": "not_requested",
        }
        idempotency_key = "dsi_" + digest_named_fields(
            "dataset_release_reconcile_cycle_v1",
            {
                "profile": self.profile.profile,
                "scope": scope.value,
                "logical_request_key": logical.key,
                "cycle_id": cycle_id,
            },
        )
        submission = ResolutionService(self.store, self.cas).submit(
            identity=SubmissionIdentity(
                principal=RECONCILE_PRINCIPAL,
                route=RECONCILE_ROUTE,
                idempotency_key=idempotency_key,
            ),
            logical_request_key=logical.key,
            request_payload=request,
        )
        replayed = bool(submission["replayed"])
        return ReconcileItem(
            cutoff.isoformat(),
            logical.key,
            "REPLAYED" if replayed else "SUBMITTED",
            submission_id=str(submission["submission_id"]),
        )

    def _logical(self, cutoff: date, scope: Scope) -> LogicalRequestIdentity:
        return LogicalRequestIdentity(
            profile=self.profile.profile,
            resolved_cutoff=cutoff,
            scope=scope,
            semantic_profile_digest=self.profile.semantic_profile_digest,
        )

    def _active_submission(self, logical_request_key: str) -> Mapping[str, object] | None:
        submission_states = tuple(sorted(_ACTIVE_SUBMISSION_STATES))
        run_states = tuple(NONTERMINAL_RUN_STATES)
        with self.store.transaction(immediate=False) as connection:
            rows = connection.execute(
                f"""
                SELECT s.*,r.state AS run_state
                FROM submissions s LEFT JOIN runs r ON r.run_id=s.run_id
                WHERE s.logical_request_key=?
                  AND (
                    s.state IN ({",".join("?" for _ in submission_states)})
                    OR r.state IN ({",".join("?" for _ in run_states)})
                  )
                ORDER BY s.created_at DESC,s.submission_id DESC LIMIT ?
                """,
                (
                    logical_request_key,
                    *submission_states,
                    *run_states,
                    MAX_ACTIVE_LOGICAL_MATCHES,
                ),
            ).fetchall()
        if len(rows) > 1:
            raise ReconcileError(
                "multiple active submissions share one logical request key",
                context={"logical_request_key": logical_request_key},
            )
        return dict(rows[0]) if rows else None

    def _claim(
        self,
        owner_identity: str,
        cycle_id: str,
        observed: datetime,
    ) -> ReconcileLease | None:
        stamp = _iso(observed)
        expires = _iso(observed + timedelta(seconds=self.profile.reconcile_lease_ttl_seconds))
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM reconcile_leases WHERE profile=?",
                (self.profile.profile,),
            ).fetchone()
            if row is not None and row["state"] == "ACTIVE" and _parse_time(str(row["expires_at"])) > observed:
                return None
            fence = int(row["fence_counter"] if row is not None else 0) + 1
            if row is None:
                connection.execute(
                    """
                    INSERT INTO reconcile_leases(
                        profile,fence_counter,state,owner_identity,cycle_id,
                        acquired_at,expires_at,updated_at
                    ) VALUES (?,?,'ACTIVE',?,?,?,?,?)
                    """,
                    (
                        self.profile.profile,
                        fence,
                        owner_identity,
                        cycle_id,
                        stamp,
                        expires,
                        stamp,
                    ),
                )
            else:
                updated = connection.execute(
                    """
                    UPDATE reconcile_leases SET fence_counter=?,state='ACTIVE',
                        owner_identity=?,cycle_id=?,acquired_at=?,expires_at=?,updated_at=?
                    WHERE profile=? AND fence_counter=?
                    """,
                    (
                        fence,
                        owner_identity,
                        cycle_id,
                        stamp,
                        expires,
                        stamp,
                        self.profile.profile,
                        row["fence_counter"],
                    ),
                )
                if updated.rowcount != 1:
                    raise StateConflict("reconcile singleton lease CAS failed")
        return ReconcileLease(self.profile.profile, fence, owner_identity, cycle_id)

    def _release(self, lease: ReconcileLease, observed: datetime) -> None:
        stamp = _iso(observed)
        with self.store.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE reconcile_leases SET state='FREE',owner_identity=NULL,cycle_id=NULL,
                    acquired_at=NULL,expires_at=NULL,updated_at=?
                WHERE profile=? AND fence_counter=? AND state='ACTIVE'
                  AND owner_identity=? AND cycle_id=?
                """,
                (
                    stamp,
                    lease.profile,
                    lease.fence,
                    lease.owner_identity,
                    lease.cycle_id,
                ),
            )
            if updated.rowcount != 1:
                raise StateConflict("stale reconcile singleton lease release")

    def _heartbeat(self, lease: ReconcileLease, observed: datetime) -> None:
        stamp = _iso(observed)
        expires = _iso(observed + timedelta(seconds=self.profile.reconcile_lease_ttl_seconds))
        with self.store.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE reconcile_leases SET expires_at=?,updated_at=?
                WHERE profile=? AND fence_counter=? AND state='ACTIVE'
                  AND owner_identity=? AND cycle_id=?
                """,
                (
                    expires,
                    stamp,
                    lease.profile,
                    lease.fence,
                    lease.owner_identity,
                    lease.cycle_id,
                ),
            )
            if updated.rowcount != 1:
                raise StateConflict("stale reconcile singleton lease heartbeat")


def default_reconcile_owner_identity() -> str:
    """Opaque per-invocation owner; it confers no process-control capability."""

    return f"reconcile_{uuid.uuid4().hex}"


def _subtract_calendar_months(value: datetime, months: int) -> datetime:
    observed = value.astimezone(SHANGHAI)
    zero_based = observed.year * 12 + observed.month - 1 - int(months)
    year, month_index = divmod(zero_based, 12)
    month = month_index + 1
    day = min(observed.day, calendar.monthrange(year, month)[1])
    return observed.replace(year=year, month=month, day=day)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ReconcileError("reconcile clock must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return _aware_utc(value).isoformat(timespec="microseconds")


def _parse_time(value: str) -> datetime:
    return _aware_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


__all__ = [
    "MAX_RECONCILE_CATCHUP_MONTHS",
    "MonthlyDatasetReconciler",
    "ReconcileError",
    "ReconcileItem",
    "ReconcileLease",
    "ReconcileReport",
    "default_reconcile_owner_identity",
]
