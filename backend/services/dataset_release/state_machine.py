"""Dataset-release state transitions and cross-entity invariants."""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

from .contracts import attestation_observation_key
from .control_store import (
    ControlStore,
    SourceSnapshotCatalogSpec,
    StateConflict,
    append_event,
    utc_now,
)
from .lease import LeaseManager, LeaseToken


CURRENT_SOURCE_EQUIVALENT_OUTCOMES = {
    "CURRENT_SOURCE_EQUIVALENT",
    "CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED",
}
TERMINAL_RUN_STATES = {
    "SUCCEEDED",
    "BLOCKED_RESOURCE_TIMEOUT",
    "BLOCKED_PERFORMANCE_REGRESSION",
    "BLOCKED_SOURCE_REVISED",
    "BLOCKED_PUBLISH_CONFLICT",
    "BLOCKED_RETRY_EXHAUSTED",
    "BLOCKED_VERSION_MISMATCH",
    "FAILED_TERMINAL",
    "CANCELLED",
}

RUN_TRANSITIONS: dict[str, set[str]] = {
    "QUEUED": {"WAITING_RESOURCE", "REATTESTING", "EXECUTING", "CANCELLED"},
    "WAITING_RESOURCE": {"QUEUED", "BLOCKED_RESOURCE_TIMEOUT", "CANCELLED"},
    "REATTESTING": {"FINALIZING_ATTESTATION", "CANCEL_REQUESTED", "FAILED_RETRYABLE", "FAILED_TERMINAL"},
    "FINALIZING_ATTESTATION": {"SUCCEEDED", "FAILED_TERMINAL"},
    "EXECUTING": {
        "WAITING_RESOURCE",
        "WAITING_PERFORMANCE_REGRESSION",
        "VALIDATING",
        "CANCEL_REQUESTED",
        "FAILED_RETRYABLE",
        "FAILED_TERMINAL",
        "WAITING_ORPHAN_QUIESCENCE",
    },
    "WAITING_PERFORMANCE_REGRESSION": {
        "QUEUED",
        "BLOCKED_PERFORMANCE_REGRESSION",
        "CANCELLED",
    },
    "VALIDATING": {"BLOCKED_SOURCE_REVISED", "PREPARING_PUBLISH", "CANCEL_REQUESTED", "FAILED_TERMINAL"},
    "PREPARING_PUBLISH": {"PUBLISHING", "CANCEL_REQUESTED", "FAILED_TERMINAL"},
    "PUBLISHING": {"SUCCEEDED", "WAITING_PUBLISH_RECOVERY", "BLOCKED_PUBLISH_CONFLICT"},
    "WAITING_PUBLISH_RECOVERY": {"PUBLISHING", "BLOCKED_PUBLISH_CONFLICT"},
    "FAILED_RETRYABLE": {"QUEUED", "BLOCKED_RETRY_EXHAUSTED"},
    "CANCEL_REQUESTED": {"CANCELLED"},
    "WAITING_ORPHAN_QUIESCENCE": {"QUEUED"},
}

ATTEMPT_TERMINAL_STATES = {
    "RELEASED_SUCCEEDED",
    "RELEASED_RETRYABLE",
    "RELEASED_WAITING",
    "RELEASED_CANCELLED",
    "EXPIRED",
    "FAILED_TERMINAL",
}


@dataclass(frozen=True, slots=True)
class IntentSpec:
    logical_request_key: str
    resolved_intent_key: str
    source_content_root: str
    source_provenance_root: str
    pit_snapshot_digest: str
    supersedes_intent_id: str | None = None
    source_revision_reason: str | None = None


@dataclass(frozen=True, slots=True)
class ResolutionSnapshotSpec:
    source_content_root: str
    source_provenance_root: str
    pit_snapshot_digest: str
    source_probe_ordinal: int
    source_probe_key: str
    source_probe_ref: str
    source_probe_valid_until: datetime

    def __post_init__(self) -> None:
        if not all(
            (
                self.source_content_root,
                self.source_provenance_root,
                self.pit_snapshot_digest,
                self.source_probe_key,
                self.source_probe_ref,
            )
        ):
            raise ValueError("resolution snapshot identity fields must be non-empty")
        if type(self.source_probe_ordinal) is not int or self.source_probe_ordinal <= 0:
            raise ValueError("resolution snapshot ordinal must be a positive integer")
        if self.source_probe_valid_until.tzinfo is None or self.source_probe_valid_until.utcoffset() is None:
            raise ValueError("resolution snapshot expiry must be timezone-aware")


@dataclass(frozen=True, slots=True)
class NoOpFinalizeSpec:
    submission_id: str
    resolution_attempt_id: str
    resolution_fence: int
    intent: IntentSpec
    run_generation_digest: str
    candidate_identity: str
    artifact_root: str
    source_probe_ordinal: int
    source_probe_key: str
    source_probe_ref: str
    attestation_key: str
    attestation_target_key: str
    attestation_ref: str
    semantic_profile_digest: str
    validation_fingerprint: str
    decision_schema: str
    terminal_receipt_ref: str
    source_snapshot_catalog: SourceSnapshotCatalogSpec | None = None
    attestation_renewal: AttestationRenewalSpec | None = None

    def __post_init__(self) -> None:
        if type(self.source_probe_ordinal) is not int or self.source_probe_ordinal <= 0:
            raise ValueError("no-op source probe ordinal must be a positive integer")
        if not self.attestation_target_key.strip():
            raise ValueError("no-op attestation target key must be non-empty")


@dataclass(frozen=True, slots=True)
class AttestationObservationSpec:
    attestation_id: str | None
    attestation_key: str
    attestation_target_key: str
    subject_type: str
    subject_digest: str
    candidate_identity: str
    producer_provenance_state: str
    producer_provenance_digest_or_sentinel: str
    candidate_artifact_root: str
    current_source_content_root: str
    source_probe_key: str
    source_probe_ref: str
    pit_snapshot_digest: str
    semantic_profile_digest: str
    validation_fingerprint: str
    observed_at: datetime
    valid_until: datetime
    equivalence_mode: str
    outcome: str
    receipt_ref: str
    committed: bool = True

    def __post_init__(self) -> None:
        identity_values = (
            self.attestation_key,
            self.attestation_target_key,
            self.subject_type,
            self.subject_digest,
            self.candidate_identity,
            self.producer_provenance_state,
            self.producer_provenance_digest_or_sentinel,
            self.candidate_artifact_root,
            self.current_source_content_root,
            self.source_probe_key,
            self.source_probe_ref,
            self.pit_snapshot_digest,
            self.semantic_profile_digest,
            self.validation_fingerprint,
            self.equivalence_mode,
            self.outcome,
            self.receipt_ref,
        )
        if not all(str(value).strip() for value in identity_values):
            raise ValueError("attestation observation identity fields must be non-empty")
        expected_key = attestation_observation_key(
            self.attestation_target_key,
            self.source_probe_key,
        )
        if self.attestation_key != expected_key:
            raise ValueError("attestation key must bind target and fresh source probe")
        observed = _as_utc(self.observed_at)
        valid_until = _as_utc(self.valid_until)
        if valid_until <= observed:
            raise ValueError("attestation observation validity must follow observation time")
        if type(self.committed) is not bool:
            raise ValueError("attestation observation committed must be boolean")


@dataclass(frozen=True, slots=True)
class AttestationRenewalSpec:
    """Fresh-probe observation derived from an unexpired artifact validation.

    The prior observation remains immutable.  ``finalize_noop`` validates the
    prior committed row and inserts this new probe-bound observation in the
    same transaction that resolves the submission.
    """

    prior_attestation_key: str
    prior_attestation_ref: str
    observation: AttestationObservationSpec

    def __post_init__(self) -> None:
        if not self.prior_attestation_key.strip() or not self.prior_attestation_ref.strip():
            raise ValueError("attestation renewal prior identity must be non-empty")
        if self.prior_attestation_key == self.observation.attestation_key:
            raise ValueError("attestation renewal requires a distinct fresh observation")
        if not self.observation.committed:
            raise ValueError("attestation renewal observation must be committed")


@dataclass(frozen=True, slots=True)
class ReattestFinalizeSpec:
    run_id: str
    attempt_id: str
    expected_row_version: int
    attempt_fence: int
    tokens: tuple[LeaseToken, ...]
    observation: AttestationObservationSpec

    def __post_init__(self) -> None:
        if not self.run_id.strip() or not self.attempt_id.strip():
            raise ValueError("reattest finalization ownership identity must be non-empty")
        if type(self.expected_row_version) is not int or self.expected_row_version <= 0:
            raise ValueError("reattest expected row version must be a positive integer")
        if type(self.attempt_fence) is not int or self.attempt_fence <= 0:
            raise ValueError("reattest attempt fence must be a positive integer")


class DatasetReleaseStateMachine:
    """Single authority for durable state transitions."""

    def __init__(self, store: ControlStore) -> None:
        self.store = store
        self.leases = LeaseManager(store)

    def record_resolution_snapshot(
        self,
        *,
        submission_id: str,
        resolution_attempt_id: str,
        resolution_fence: int,
        source_content_root: str,
        source_provenance_root: str,
        pit_snapshot_digest: str,
        source_probe_ordinal: int,
        source_probe_key: str,
        source_probe_ref: str,
        source_probe_valid_until: datetime,
    ) -> None:
        if type(source_probe_ordinal) is not int or source_probe_ordinal <= 0:
            raise StateConflict("source probe ordinal must be a positive integer")
        with self.store.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE resolution_attempts
                SET state='RUNNING',source_content_root=?,source_provenance_root=?,
                    pit_snapshot_digest=?,source_probe_ordinal=?,source_probe_key=?,source_probe_ref=?,
                    source_probe_valid_until=?,updated_at=?
                WHERE resolution_attempt_id=? AND submission_id=? AND fence=?
                  AND state IN ('CLAIMED','RUNNING')
                  AND ? > COALESCE(source_probe_ordinal,0)
                """,
                (
                    source_content_root,
                    source_provenance_root,
                    pit_snapshot_digest,
                    source_probe_ordinal,
                    source_probe_key,
                    source_probe_ref,
                    _iso(source_probe_valid_until),
                    utc_now(),
                    resolution_attempt_id,
                    submission_id,
                    int(resolution_fence),
                    source_probe_ordinal,
                ),
            )
            if updated.rowcount != 1:
                raise StateConflict("resolution snapshot fence/state/ordinal mismatch")

    def register_attestation(
        self,
        *,
        attestation_id: str | None,
        attestation_key: str,
        attestation_target_key: str,
        subject_type: str,
        subject_digest: str,
        candidate_identity: str,
        producer_provenance_state: str,
        producer_provenance_digest_or_sentinel: str,
        candidate_artifact_root: str,
        current_source_content_root: str,
        source_probe_key: str,
        source_probe_ref: str,
        pit_snapshot_digest: str,
        semantic_profile_digest: str,
        validation_fingerprint: str,
        observed_at: datetime,
        valid_until: datetime,
        equivalence_mode: str,
        outcome: str,
        receipt_ref: str,
        committed: bool = True,
    ) -> str:
        observation = AttestationObservationSpec(
            attestation_id=attestation_id,
            attestation_key=attestation_key,
            attestation_target_key=attestation_target_key,
            subject_type=subject_type,
            subject_digest=subject_digest,
            candidate_identity=candidate_identity,
            producer_provenance_state=producer_provenance_state,
            producer_provenance_digest_or_sentinel=producer_provenance_digest_or_sentinel,
            candidate_artifact_root=candidate_artifact_root,
            current_source_content_root=current_source_content_root,
            source_probe_key=source_probe_key,
            source_probe_ref=source_probe_ref,
            pit_snapshot_digest=pit_snapshot_digest,
            semantic_profile_digest=semantic_profile_digest,
            validation_fingerprint=validation_fingerprint,
            observed_at=observed_at,
            valid_until=valid_until,
            equivalence_mode=equivalence_mode,
            outcome=outcome,
            receipt_ref=receipt_ref,
            committed=committed,
        )
        with self.store.transaction() as connection:
            return self._upsert_attestation_observation(connection, observation)

    def finalize_reattest(
        self,
        spec: ReattestFinalizeSpec,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Atomically commit one fresh observation and close its owned REATTEST run."""

        observed = _as_utc(now)
        stamp = _iso(observed)
        observation = spec.observation
        if not observation.committed:
            raise StateConflict("reattest finalization requires a committed observation")
        if (
            observation.outcome not in CURRENT_SOURCE_EQUIVALENT_OUTCOMES
            or observation.equivalence_mode not in CURRENT_SOURCE_EQUIVALENT_OUTCOMES
        ):
            raise StateConflict("reattest observation is not current-source-equivalent")
        if observation.subject_type != "candidate" or observation.subject_digest != observation.candidate_identity:
            raise StateConflict("reattest observation candidate subject is inconsistent")
        if len(spec.tokens) != 2 or any(token.attempt_id != spec.attempt_id for token in spec.tokens):
            raise StateConflict("reattest finalization requires exactly two owned leases")
        tokens_by_kind = {token.resource_key.split(":", 1)[0]: token for token in spec.tokens}
        if set(tokens_by_kind) != {"host", "release"} or tokens_by_kind["host"].resource_key != "host:heavy-dataset":
            raise StateConflict("reattest finalization requires host and release leases")

        with self.store.transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (spec.run_id,)).fetchone()
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?",
                (spec.attempt_id, spec.run_id),
            ).fetchone()
            if run is not None and run["state"] == "SUCCEEDED":
                expected_terminal = {
                    "operation_kind": "REATTEST",
                    "outcome": "REATTESTED",
                    "terminal_receipt_ref": observation.receipt_ref,
                    "candidate_identity": observation.candidate_identity,
                    "artifact_root": observation.candidate_artifact_root,
                    "active_attempt_id": None,
                }
                for field, value in expected_terminal.items():
                    _require_equal(run, field, value)
                if int(run["row_version"]) != int(spec.expected_row_version) + 1:
                    raise StateConflict("reattest terminal replay row version changed")
                if (
                    attempt is None
                    or attempt["state"] != "RELEASED_SUCCEEDED"
                    or attempt["attempt_kind"] != "REATTEST"
                    or int(attempt["attempt_fence"]) != int(spec.attempt_fence)
                ):
                    raise StateConflict("reattest terminal replay attempt changed")
                durable_observation = connection.execute(
                    "SELECT * FROM attestations WHERE attestation_key=? AND committed=1",
                    (observation.attestation_key,),
                ).fetchone()
                if (
                    durable_observation is None
                    or _attestation_identity(durable_observation) != _observation_identity(observation)
                    or (
                        observation.attestation_id is not None
                        and durable_observation["attestation_id"] != observation.attestation_id
                    )
                ):
                    raise StateConflict("reattest terminal replay observation changed")
                for token in spec.tokens:
                    lease = connection.execute(
                        "SELECT * FROM leases WHERE resource_key=?",
                        (token.resource_key,),
                    ).fetchone()
                    if lease is None or lease["state"] != "FREE" or int(lease["fence_counter"]) != int(token.fence):
                        raise StateConflict("reattest terminal replay lease changed")
                event_count = connection.execute(
                    """
                    SELECT COUNT(*) FROM events
                    WHERE run_id=? AND attempt_id=? AND type='RUN_REATTESTED'
                      AND payload_ref=?
                    """,
                    (spec.run_id, spec.attempt_id, observation.receipt_ref),
                ).fetchone()[0]
                if int(event_count) != 1:
                    raise StateConflict("reattest terminal replay event changed")
                return dict(run)
            if not (_as_utc(observation.observed_at) <= observed < _as_utc(observation.valid_until)):
                raise StateConflict("reattest observation is stale or not yet valid")
            if (
                run is None
                or attempt is None
                or run["state"] != "REATTESTING"
                or run["operation_kind"] != "REATTEST"
                or int(run["row_version"]) != int(spec.expected_row_version)
                or run["active_attempt_id"] != spec.attempt_id
                or attempt["state"] != "RUNNING"
                or attempt["attempt_kind"] != "REATTEST"
                or int(attempt["attempt_fence"]) != int(spec.attempt_fence)
                or attempt["host_fence"] is None
                or int(attempt["host_fence"]) != int(tokens_by_kind["host"].fence)
                or attempt["release_fence"] is None
                or int(attempt["release_fence"]) != int(tokens_by_kind["release"].fence)
            ):
                raise StateConflict("reattest finalization ownership changed")
            intent = connection.execute("SELECT * FROM intents WHERE intent_id=?", (run["intent_id"],)).fetchone()
            if intent is None:
                raise StateConflict("reattest run intent is missing")
            _require_equal(
                intent,
                "source_content_root",
                observation.current_source_content_root,
            )
            _require_equal(
                intent,
                "pit_snapshot_digest",
                observation.pit_snapshot_digest,
            )
            self._upsert_attestation_observation(connection, observation)
            LeaseManager._release_exact(connection, spec.tokens, observed=observed)
            attempt_updated = connection.execute(
                """
                UPDATE attempts SET state='RELEASED_SUCCEEDED',updated_at=?
                WHERE attempt_id=? AND run_id=? AND state='RUNNING'
                  AND attempt_kind='REATTEST' AND attempt_fence=?
                """,
                (stamp, spec.attempt_id, spec.run_id, int(spec.attempt_fence)),
            )
            if attempt_updated.rowcount != 1:
                raise StateConflict("reattest attempt terminal CAS failed")
            run_updated = connection.execute(
                """
                UPDATE runs SET state='SUCCEEDED',outcome='REATTESTED',terminal_receipt_ref=?,
                    candidate_identity=?,artifact_root=?,active_attempt_id=NULL,
                    row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state='REATTESTING' AND row_version=?
                  AND active_attempt_id=?
                """,
                (
                    observation.receipt_ref,
                    observation.candidate_identity,
                    observation.candidate_artifact_root,
                    stamp,
                    spec.run_id,
                    int(spec.expected_row_version),
                    spec.attempt_id,
                ),
            )
            if run_updated.rowcount != 1:
                raise StateConflict("reattest run terminal CAS failed")
            append_event(
                connection,
                event_type="RUN_REATTESTED",
                run_id=spec.run_id,
                attempt_id=spec.attempt_id,
                payload_ref=observation.receipt_ref,
                created_at=stamp,
            )
            residual = connection.execute(
                """
                SELECT COUNT(*) FROM leases
                WHERE attempt_id=? AND state IN ('ACTIVE','ORPHAN_HOLD')
                """,
                (spec.attempt_id,),
            ).fetchone()[0]
            if int(residual):
                raise StateConflict("reattest finalization retained active leases")
            return dict(connection.execute("SELECT * FROM runs WHERE run_id=?", (spec.run_id,)).fetchone())

    @staticmethod
    def _upsert_attestation_observation(
        connection: sqlite3.Connection,
        observation: AttestationObservationSpec,
    ) -> str:
        identifier = observation.attestation_id or f"dsat_{uuid.uuid4().hex}"
        values = (identifier, *_observation_identity(observation))
        existing = connection.execute(
            "SELECT * FROM attestations WHERE attestation_key=?",
            (observation.attestation_key,),
        ).fetchone()
        if existing is not None:
            if _attestation_identity(existing) != values[1:] or (
                observation.attestation_id is not None and existing["attestation_id"] != observation.attestation_id
            ):
                raise StateConflict("attestation key is bound to different immutable evidence")
            return str(existing["attestation_id"])
        if observation.attestation_id is not None:
            existing_identifier = connection.execute(
                "SELECT * FROM attestations WHERE attestation_id=?",
                (observation.attestation_id,),
            ).fetchone()
            if existing_identifier is not None:
                raise StateConflict("attestation id is bound to different immutable evidence")
        connection.execute(
            """
            INSERT INTO attestations(
                attestation_id,attestation_key,attestation_target_key,subject_type,
                subject_digest,candidate_identity,
                producer_provenance_state,producer_provenance_digest_or_sentinel,
                candidate_artifact_root,current_source_content_root,source_probe_key,
                source_probe_ref,pit_snapshot_digest,semantic_profile_digest,
                validation_fingerprint,observed_at,valid_until,equivalence_mode,outcome,
                receipt_ref,committed
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            values,
        )
        return identifier

    def create_queued_run(
        self,
        *,
        intent: IntentSpec,
        run_generation_digest: str,
        operation_kind: str,
        plan_ref: str,
        submission_id: str | None = None,
        resolution_attempt_id: str | None = None,
        resolution_fence: int | None = None,
        expected_resolution_snapshot: ResolutionSnapshotSpec | None = None,
        source_snapshot_catalog: SourceSnapshotCatalogSpec | None = None,
    ) -> dict[str, Any]:
        ownership = (submission_id, resolution_attempt_id, resolution_fence)
        if any(value is not None for value in ownership) and not all(value is not None for value in ownership):
            raise StateConflict("queued run resolution ownership must be complete")
        if submission_id is not None and expected_resolution_snapshot is None:
            raise StateConflict("queued run requires the claimed resolution source snapshot")
        if submission_id is None and expected_resolution_snapshot is not None:
            raise StateConflict("unowned queued run cannot carry a resolution source snapshot")
        if submission_id is None and source_snapshot_catalog is not None:
            raise StateConflict("unowned queued run cannot register a source snapshot")
        now = utc_now()
        with self.store.transaction() as connection:
            if source_snapshot_catalog is not None:
                self.store.register_source_snapshot_in_transaction(connection, source_snapshot_catalog)
            intent_id = self._upsert_intent(connection, intent, now=now)
            existing = connection.execute(
                "SELECT * FROM runs WHERE intent_id=? AND run_generation_digest=?",
                (intent_id, run_generation_digest),
            ).fetchone()
            if existing is not None:
                if existing["operation_kind"] != operation_kind or existing["plan_ref"] != plan_ref:
                    raise StateConflict("run generation is bound to another operation")
                if submission_id is not None:
                    self._resolve_submission_to_run(
                        connection,
                        submission_id=submission_id,
                        intent_id=intent_id,
                        run_id=str(existing["run_id"]),
                        state="RESOLVED_TO_EXISTING",
                        resolution_attempt_id=resolution_attempt_id,
                        resolution_fence=resolution_fence,
                        expected_resolution_snapshot=expected_resolution_snapshot,
                        terminal_receipt_ref=None,
                        now=now,
                    )
                return dict(existing)
            run_id = f"dsr_{uuid.uuid4().hex}"
            connection.execute(
                """
                INSERT INTO runs(
                    run_id,intent_id,run_generation_digest,operation_kind,lineage_root_run_id,
                    state,plan_ref,created_at,updated_at
                ) VALUES (?,?,?,?,?,'QUEUED',?,?,?)
                """,
                (run_id, intent_id, run_generation_digest, operation_kind, run_id, plan_ref, now, now),
            )
            connection.execute(
                "INSERT INTO resume_lineages(lineage_root_run_id,latest_run_id,next_ordinal) VALUES (?,?,1)",
                (run_id, run_id),
            )
            append_event(connection, event_type="RUN_QUEUED", run_id=run_id, created_at=now)
            if submission_id is not None:
                self._resolve_submission_to_run(
                    connection,
                    submission_id=submission_id,
                    intent_id=intent_id,
                    run_id=run_id,
                    state="RESOLVED_NEW_RUN",
                    resolution_attempt_id=resolution_attempt_id,
                    resolution_fence=resolution_fence,
                    expected_resolution_snapshot=expected_resolution_snapshot,
                    terminal_receipt_ref=None,
                    now=now,
                )
            return dict(connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())

    def finalize_noop(self, spec: NoOpFinalizeSpec, *, now: datetime | None = None) -> dict[str, Any]:
        """Atomically create/link the attempt-free terminal NO_OP run."""

        observed = _as_utc(now)
        stamp = _iso(observed)
        with self.store.transaction() as connection:
            submission = connection.execute(
                "SELECT * FROM submissions WHERE submission_id=?", (spec.submission_id,)
            ).fetchone()
            resolution = connection.execute(
                "SELECT * FROM resolution_attempts WHERE resolution_attempt_id=?",
                (spec.resolution_attempt_id,),
            ).fetchone()
            if (
                submission is None
                or resolution is None
                or submission["state"] != "RESOLVING_SOURCE"
                or submission["resolution_attempt_id"] != spec.resolution_attempt_id
                or resolution["submission_id"] != spec.submission_id
                or resolution["state"] not in {"CLAIMED", "RUNNING"}
                or int(resolution["fence"]) != int(spec.resolution_fence)
            ):
                raise StateConflict("no-op resolution ownership changed")
            _require_equal(resolution, "source_content_root", spec.intent.source_content_root)
            _require_equal(resolution, "source_provenance_root", spec.intent.source_provenance_root)
            _require_equal(resolution, "pit_snapshot_digest", spec.intent.pit_snapshot_digest)
            _require_equal(resolution, "source_probe_ordinal", spec.source_probe_ordinal)
            _require_equal(resolution, "source_probe_key", spec.source_probe_key)
            _require_equal(resolution, "source_probe_ref", spec.source_probe_ref)
            valid_until = _parse_time(resolution["source_probe_valid_until"])
            if valid_until <= observed:
                raise StateConflict("fresh source probe expired before no-op finalization")

            if spec.attestation_renewal is not None:
                self._renew_noop_attestation(
                    connection,
                    spec=spec,
                    renewal=spec.attestation_renewal,
                    source_probe_valid_until=valid_until,
                    observed=observed,
                )

            attestation = connection.execute(
                "SELECT * FROM attestations WHERE attestation_key=? AND committed=1",
                (spec.attestation_key,),
            ).fetchone()
            if attestation is None:
                raise StateConflict("no-op attestation is not committed")
            expected_attestation = {
                "attestation_target_key": spec.attestation_target_key,
                "candidate_identity": spec.candidate_identity,
                "candidate_artifact_root": spec.artifact_root,
                "current_source_content_root": spec.intent.source_content_root,
                "source_probe_key": spec.source_probe_key,
                "source_probe_ref": spec.source_probe_ref,
                "pit_snapshot_digest": spec.intent.pit_snapshot_digest,
                "semantic_profile_digest": spec.semantic_profile_digest,
                "validation_fingerprint": spec.validation_fingerprint,
                "receipt_ref": spec.attestation_ref,
            }
            for field, value in expected_attestation.items():
                _require_equal(attestation, field, value)
            if attestation["outcome"] not in CURRENT_SOURCE_EQUIVALENT_OUTCOMES:
                raise StateConflict("attestation is not current-source-equivalent")
            if _parse_time(attestation["valid_until"]) <= observed:
                raise StateConflict("attestation expired before no-op finalization")

            if spec.source_snapshot_catalog is not None:
                self.store.register_source_snapshot_in_transaction(connection, spec.source_snapshot_catalog)
            intent_id = self._upsert_intent(connection, spec.intent, now=stamp)
            existing = connection.execute(
                "SELECT * FROM runs WHERE intent_id=? AND run_generation_digest=?",
                (intent_id, spec.run_generation_digest),
            ).fetchone()
            if existing is None:
                run_id = f"dsr_{uuid.uuid4().hex}"
                connection.execute(
                    """
                    INSERT INTO runs(
                        run_id,intent_id,run_generation_digest,operation_kind,lineage_root_run_id,
                        state,outcome,terminal_receipt_ref,candidate_identity,artifact_root,
                        created_at,updated_at
                    ) VALUES (?,?,?,?,?,'SUCCEEDED','NO_OP_VERIFIED',?,?,?,?,?)
                    """,
                    (
                        run_id,
                        intent_id,
                        spec.run_generation_digest,
                        "NO_OP",
                        run_id,
                        spec.terminal_receipt_ref,
                        spec.candidate_identity,
                        spec.artifact_root,
                        stamp,
                        stamp,
                    ),
                )
                connection.execute(
                    "INSERT INTO resume_lineages(lineage_root_run_id,latest_run_id,next_ordinal) VALUES (?,?,1)",
                    (run_id, run_id),
                )
                append_event(
                    connection,
                    event_type="NO_OP_VERIFIED",
                    run_id=run_id,
                    payload_ref=spec.terminal_receipt_ref,
                    created_at=stamp,
                )
            else:
                run_id = str(existing["run_id"])
                expected = {
                    "operation_kind": "NO_OP",
                    "state": "SUCCEEDED",
                    "outcome": "NO_OP_VERIFIED",
                    "terminal_receipt_ref": spec.terminal_receipt_ref,
                    "candidate_identity": spec.candidate_identity,
                    "artifact_root": spec.artifact_root,
                }
                for field, value in expected.items():
                    _require_equal(existing, field, value)
                if existing["active_attempt_id"] is not None:
                    raise StateConflict("NO_OP run unexpectedly owns an attempt")

            self._resolve_submission_to_run(
                connection,
                submission_id=spec.submission_id,
                intent_id=intent_id,
                run_id=run_id,
                state="RESOLVED_NO_OP",
                resolution_attempt_id=spec.resolution_attempt_id,
                resolution_fence=spec.resolution_fence,
                terminal_receipt_ref=spec.terminal_receipt_ref,
                now=stamp,
            )
            # The terminal no-op path owns no build attempt or release lease.
            if connection.execute("SELECT COUNT(*) FROM attempts WHERE run_id=?", (run_id,)).fetchone()[0]:
                raise StateConflict("NO_OP run must not have build attempts")
            return dict(connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())

    @classmethod
    def _renew_noop_attestation(
        cls,
        connection: sqlite3.Connection,
        *,
        spec: NoOpFinalizeSpec,
        renewal: AttestationRenewalSpec,
        source_probe_valid_until: datetime,
        observed: datetime,
    ) -> None:
        """Validate the old artifact proof and stage a fresh probe observation."""

        observation = renewal.observation
        expected_observation = {
            "attestation_key": spec.attestation_key,
            "attestation_target_key": spec.attestation_target_key,
            "subject_type": "candidate",
            "subject_digest": spec.candidate_identity,
            "candidate_identity": spec.candidate_identity,
            "candidate_artifact_root": spec.artifact_root,
            "current_source_content_root": spec.intent.source_content_root,
            "source_probe_key": spec.source_probe_key,
            "source_probe_ref": spec.source_probe_ref,
            "pit_snapshot_digest": spec.intent.pit_snapshot_digest,
            "semantic_profile_digest": spec.semantic_profile_digest,
            "validation_fingerprint": spec.validation_fingerprint,
            "receipt_ref": spec.attestation_ref,
        }
        for field, value in expected_observation.items():
            if getattr(observation, field) != value:
                raise StateConflict(f"no-op attestation renewal differs for {field}")
        if (
            observation.outcome not in CURRENT_SOURCE_EQUIVALENT_OUTCOMES
            or observation.equivalence_mode not in CURRENT_SOURCE_EQUIVALENT_OUTCOMES
            or observation.outcome != observation.equivalence_mode
        ):
            raise StateConflict("no-op attestation renewal is not current-source-equivalent")
        observation_time = _as_utc(observation.observed_at)
        renewal_valid_until = _as_utc(observation.valid_until)
        if not observation_time <= observed < renewal_valid_until:
            raise StateConflict("no-op attestation renewal is stale or not yet valid")
        if renewal_valid_until > source_probe_valid_until:
            raise StateConflict("no-op attestation renewal exceeds fresh probe validity")

        prior = connection.execute(
            "SELECT * FROM attestations WHERE attestation_key=? AND committed=1",
            (renewal.prior_attestation_key,),
        ).fetchone()
        if prior is None:
            raise StateConflict("no-op attestation renewal anchor is not committed")
        expected_prior = {
            "receipt_ref": renewal.prior_attestation_ref,
            "attestation_target_key": observation.attestation_target_key,
            "subject_type": observation.subject_type,
            "subject_digest": observation.subject_digest,
            "candidate_identity": observation.candidate_identity,
            "producer_provenance_state": observation.producer_provenance_state,
            "producer_provenance_digest_or_sentinel": (observation.producer_provenance_digest_or_sentinel),
            "candidate_artifact_root": observation.candidate_artifact_root,
            "current_source_content_root": observation.current_source_content_root,
            "pit_snapshot_digest": observation.pit_snapshot_digest,
            "semantic_profile_digest": observation.semantic_profile_digest,
            "validation_fingerprint": observation.validation_fingerprint,
            "equivalence_mode": observation.equivalence_mode,
            "outcome": observation.outcome,
        }
        for field, value in expected_prior.items():
            _require_equal(prior, field, value)
        if prior["outcome"] not in CURRENT_SOURCE_EQUIVALENT_OUTCOMES:
            raise StateConflict("no-op attestation renewal anchor is not current-source-equivalent")
        if prior["source_probe_key"] == observation.source_probe_key:
            raise StateConflict("no-op attestation renewal did not bind a fresh probe")
        prior_observed_at = _parse_time(prior["observed_at"])
        prior_valid_until = _parse_time(prior["valid_until"])
        if not prior_observed_at <= observed < prior_valid_until:
            raise StateConflict("no-op attestation renewal anchor expired")
        if observation_time < prior_observed_at:
            raise StateConflict("no-op attestation renewal observation time regressed")
        if renewal_valid_until > prior_valid_until:
            raise StateConflict("no-op attestation renewal extended artifact validation validity")
        cls._upsert_attestation_observation(connection, observation)

    def create_resume_run(
        self,
        *,
        resumes_run_id: str,
        run_generation_digest: str,
        plan_ref: str,
        intent: IntentSpec | None = None,
    ) -> dict[str, Any]:
        """Create the unique active leaf for a terminal pre-publish run lineage."""

        now = utc_now()
        with self.store.transaction() as connection:
            source = connection.execute("SELECT * FROM runs WHERE run_id=?", (resumes_run_id,)).fetchone()
            if source is None or source["state"] not in TERMINAL_RUN_STATES:
                raise StateConflict("resume target must be terminal")
            if source["publish_nonce"] is not None:
                raise StateConflict("post-publish-commit runs require same-run finalizer recovery")
            lineage_root = str(source["lineage_root_run_id"])
            lineage = connection.execute(
                "SELECT * FROM resume_lineages WHERE lineage_root_run_id=?", (lineage_root,)
            ).fetchone()
            if lineage is None:
                raise StateConflict("resume lineage is missing")
            if lineage["latest_run_id"] != resumes_run_id:
                latest = connection.execute("SELECT * FROM runs WHERE run_id=?", (lineage["latest_run_id"],)).fetchone()
                if (
                    latest is not None
                    and latest["resumes_run_id"] == resumes_run_id
                    and latest["run_generation_digest"] == run_generation_digest
                    and latest["state"] not in TERMINAL_RUN_STATES
                ):
                    return dict(latest)
                raise StateConflict("resume target is not the latest lineage leaf")
            active = connection.execute(
                f"SELECT * FROM runs WHERE lineage_root_run_id=? AND state NOT IN ({','.join('?' for _ in TERMINAL_RUN_STATES)})",
                (lineage_root, *sorted(TERMINAL_RUN_STATES)),
            ).fetchone()
            if active is not None:
                if active["run_generation_digest"] == run_generation_digest:
                    return dict(active)
                raise StateConflict("RESUME_LINEAGE_ACTIVE")
            intent_id = str(source["intent_id"])
            if intent is not None:
                intent_id = self._upsert_intent(connection, intent, now=now)
            existing = connection.execute(
                "SELECT * FROM runs WHERE intent_id=? AND run_generation_digest=?",
                (intent_id, run_generation_digest),
            ).fetchone()
            if existing is not None:
                return dict(existing)
            ordinal = int(lineage["next_ordinal"])
            run_id = f"dsr_{uuid.uuid4().hex}"
            connection.execute(
                """
                INSERT INTO runs(
                    run_id,intent_id,run_generation_digest,operation_kind,lineage_root_run_id,
                    resume_ordinal,state,plan_ref,resumes_run_id,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,'QUEUED',?,?,?,?)
                """,
                (
                    run_id,
                    intent_id,
                    run_generation_digest,
                    "RESUME_BUILD",
                    lineage_root,
                    ordinal,
                    plan_ref,
                    resumes_run_id,
                    now,
                    now,
                ),
            )
            updated = connection.execute(
                """
                UPDATE resume_lineages SET latest_run_id=?,next_ordinal=next_ordinal+1,
                    row_version=row_version+1
                WHERE lineage_root_run_id=? AND latest_run_id=? AND row_version=?
                """,
                (run_id, lineage_root, resumes_run_id, lineage["row_version"]),
            )
            if updated.rowcount != 1:
                raise StateConflict("resume lineage CAS failed")
            append_event(
                connection,
                event_type="RESUME_RUN_QUEUED",
                run_id=run_id,
                created_at=now,
            )
            return dict(connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())

    def transition_unowned_run(
        self,
        *,
        run_id: str,
        expected_state: str,
        expected_row_version: int,
        next_state: str,
        outcome: str | None = None,
    ) -> dict[str, Any]:
        _require_transition(expected_state, next_state)
        if next_state in TERMINAL_RUN_STATES and not outcome:
            raise ValueError("terminal run transition requires outcome")
        now = utc_now()
        with self.store.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE runs SET state=?,outcome=?,row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state=? AND row_version=? AND active_attempt_id IS NULL
                """,
                (next_state, outcome, now, run_id, expected_state, int(expected_row_version)),
            )
            if updated.rowcount != 1:
                raise StateConflict("unowned run transition CAS failed")
            append_event(connection, event_type=f"RUN_{next_state}", run_id=run_id, created_at=now)
            return dict(connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())

    def transition_owned_and_release(
        self,
        *,
        run_id: str,
        attempt_id: str,
        expected_state: str,
        expected_row_version: int,
        attempt_fence: int,
        tokens: tuple[LeaseToken, ...],
        next_state: str,
        attempt_terminal_state: str,
        outcome: str | None = None,
    ) -> dict[str, Any]:
        _require_transition(expected_state, next_state)
        if attempt_terminal_state not in ATTEMPT_TERMINAL_STATES:
            raise ValueError("owned release requires a terminal attempt state")
        if next_state in TERMINAL_RUN_STATES and not outcome:
            raise ValueError("terminal run transition requires outcome")
        now = datetime.now(UTC)
        stamp = _iso(now)
        with self.store.transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?", (attempt_id, run_id)
            ).fetchone()
            if (
                run is None
                or attempt is None
                or run["state"] != expected_state
                or int(run["row_version"]) != int(expected_row_version)
                or run["active_attempt_id"] != attempt_id
                or int(attempt["attempt_fence"]) != int(attempt_fence)
            ):
                raise StateConflict("owned run transition ownership changed")
            LeaseManager._release_exact(connection, tokens, observed=now)
            connection.execute(
                "UPDATE attempts SET state=?,updated_at=? WHERE attempt_id=?",
                (attempt_terminal_state, stamp, attempt_id),
            )
            updated = connection.execute(
                """
                UPDATE runs SET state=?,outcome=?,active_attempt_id=NULL,
                    row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state=? AND row_version=? AND active_attempt_id=?
                """,
                (
                    next_state,
                    outcome,
                    stamp,
                    run_id,
                    expected_state,
                    int(expected_row_version),
                    attempt_id,
                ),
            )
            if updated.rowcount != 1:
                raise StateConflict("owned run transition CAS failed")
            append_event(
                connection,
                event_type=f"RUN_{next_state}",
                run_id=run_id,
                attempt_id=attempt_id,
                created_at=stamp,
            )
            residual = int(
                connection.execute(
                    "SELECT COUNT(*) FROM leases WHERE attempt_id=? AND state IN ('ACTIVE','ORPHAN_HOLD')",
                    (attempt_id,),
                ).fetchone()[0]
            )
            if residual:
                raise StateConflict("released run retained active leases")
            return dict(connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())

    def transition_owned_keep(
        self,
        *,
        run_id: str,
        attempt_id: str,
        expected_state: str,
        expected_row_version: int,
        attempt_fence: int,
        tokens: tuple[LeaseToken, ...],
        next_state: str,
    ) -> dict[str, Any]:
        """Advance an active atomic stage while retaining the same fenced owner."""

        _require_transition(expected_state, next_state)
        if next_state in TERMINAL_RUN_STATES or next_state.startswith("WAITING_"):
            raise ValueError("terminal/waiting transitions must release or explicitly hold ownership")
        now = utc_now()
        with self.store.transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?", (attempt_id, run_id)
            ).fetchone()
            if (
                run is None
                or attempt is None
                or run["state"] != expected_state
                or int(run["row_version"]) != int(expected_row_version)
                or run["active_attempt_id"] != attempt_id
                or int(attempt["attempt_fence"]) != int(attempt_fence)
            ):
                raise StateConflict("owned stage transition ownership changed")
            for token in tokens:
                lease = connection.execute(
                    """
                    SELECT 1 FROM leases WHERE resource_key=? AND attempt_id=?
                      AND owner_identity=? AND fence_counter=? AND state='ACTIVE'
                    """,
                    (token.resource_key, token.attempt_id, token.owner_identity, token.fence),
                ).fetchone()
                if lease is None:
                    raise StateConflict(f"owned stage lease changed: {token.resource_key}")
            updated = connection.execute(
                """
                UPDATE runs SET state=?,row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state=? AND row_version=? AND active_attempt_id=?
                """,
                (
                    next_state,
                    now,
                    run_id,
                    expected_state,
                    int(expected_row_version),
                    attempt_id,
                ),
            )
            if updated.rowcount != 1:
                raise StateConflict("owned stage transition CAS failed")
            append_event(
                connection,
                event_type=f"RUN_{next_state}",
                run_id=run_id,
                attempt_id=attempt_id,
                created_at=now,
            )
            return dict(connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())

    def _upsert_intent(self, connection: sqlite3.Connection, intent: IntentSpec, *, now: str) -> str:
        existing = connection.execute(
            "SELECT * FROM intents WHERE resolved_intent_key=?", (intent.resolved_intent_key,)
        ).fetchone()
        expected = {
            "logical_request_key": intent.logical_request_key,
            "source_content_root": intent.source_content_root,
            "pit_snapshot_digest": intent.pit_snapshot_digest,
            "supersedes_intent_id": intent.supersedes_intent_id,
            "source_revision_reason": intent.source_revision_reason,
        }
        if existing is not None:
            for field, value in expected.items():
                _require_equal(existing, field, value)
            return str(existing["intent_id"])
        intent_id = f"dsi_{uuid.uuid4().hex}"
        connection.execute(
            """
            INSERT INTO intents(
                intent_id,logical_request_key,resolved_intent_key,source_content_root,
                source_provenance_root,pit_snapshot_digest,supersedes_intent_id,
                source_revision_reason,created_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                intent_id,
                intent.logical_request_key,
                intent.resolved_intent_key,
                intent.source_content_root,
                intent.source_provenance_root,
                intent.pit_snapshot_digest,
                intent.supersedes_intent_id,
                intent.source_revision_reason,
                now,
            ),
        )
        return intent_id

    def _resolve_submission_to_run(
        self,
        connection: sqlite3.Connection,
        *,
        submission_id: str,
        intent_id: str,
        run_id: str,
        state: str,
        resolution_attempt_id: str | None,
        resolution_fence: int | None,
        expected_resolution_snapshot: ResolutionSnapshotSpec | None = None,
        terminal_receipt_ref: str | None,
        now: str,
    ) -> None:
        submission = connection.execute("SELECT * FROM submissions WHERE submission_id=?", (submission_id,)).fetchone()
        durable_intent = connection.execute("SELECT * FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
        if submission is None or submission["state"] != "RESOLVING_SOURCE":
            raise StateConflict("submission is not actively resolving source")
        if durable_intent is None:
            raise StateConflict("resolved intent disappeared during source binding")
        _require_equal(
            durable_intent,
            "logical_request_key",
            submission["logical_request_key"],
        )
        if resolution_attempt_id is not None:
            resolution = connection.execute(
                "SELECT * FROM resolution_attempts WHERE resolution_attempt_id=?",
                (resolution_attempt_id,),
            ).fetchone()
            if resolution is None or resolution["submission_id"] != submission_id:
                raise StateConflict("resolution attempt does not own submission")
            if submission["resolution_attempt_id"] != resolution_attempt_id:
                raise StateConflict("submission resolution pointer changed")
            if resolution_fence is None or int(resolution["fence"]) != int(resolution_fence):
                raise StateConflict("resolution attempt fence changed")
            if resolution["state"] not in {"CLAIMED", "RUNNING"}:
                raise StateConflict("resolution attempt is not active")
            if expected_resolution_snapshot is not None:
                expected = expected_resolution_snapshot
                expected_fields = {
                    "source_content_root": expected.source_content_root,
                    "source_provenance_root": expected.source_provenance_root,
                    "pit_snapshot_digest": expected.pit_snapshot_digest,
                    "source_probe_ordinal": expected.source_probe_ordinal,
                    "source_probe_key": expected.source_probe_key,
                    "source_probe_ref": expected.source_probe_ref,
                    "source_probe_valid_until": _iso(expected.source_probe_valid_until),
                }
                for field, value in expected_fields.items():
                    _require_equal(resolution, field, value)
                for field in (
                    "source_content_root",
                    "pit_snapshot_digest",
                ):
                    _require_equal(durable_intent, field, getattr(expected, field))
                if _parse_time(resolution["source_probe_valid_until"]) <= _parse_time(now):
                    raise StateConflict("fresh source probe expired before run resolution")
            released = self.leases.release_by_attempt_in_transaction(
                connection,
                attempt_id=resolution_attempt_id,
                observed=_parse_time(now),
            )
            if released < 1:
                raise StateConflict("resolution attempt owns no releasable lease")
            connection.execute(
                "UPDATE resolution_attempts SET state='RELEASED_SUCCEEDED',updated_at=? WHERE resolution_attempt_id=?",
                (now, resolution_attempt_id),
            )
            append_event(
                connection,
                event_type="RESOLUTION_RELEASED_SUCCEEDED",
                submission_id=submission_id,
                resolution_attempt_id=resolution_attempt_id,
                created_at=now,
            )
        updated = connection.execute(
            """
            UPDATE submissions SET state=?,intent_id=?,run_id=?,resolution_attempt_id=NULL,
                terminal_receipt_ref=?,row_version=row_version+1,updated_at=?
            WHERE submission_id=? AND state='RESOLVING_SOURCE'
              AND (? IS NULL OR resolution_attempt_id=?)
            """,
            (
                state,
                intent_id,
                run_id,
                terminal_receipt_ref,
                now,
                submission_id,
                resolution_attempt_id,
                resolution_attempt_id,
            ),
        )
        if updated.rowcount != 1:
            raise StateConflict("submission resolution CAS failed")
        append_event(
            connection,
            event_type=state,
            submission_id=submission_id,
            run_id=run_id,
            payload_ref=terminal_receipt_ref,
            created_at=now,
        )


def _attestation_identity(row: Mapping[str, Any]) -> tuple[Any, ...]:
    fields = (
        "attestation_key",
        "attestation_target_key",
        "subject_type",
        "subject_digest",
        "candidate_identity",
        "producer_provenance_state",
        "producer_provenance_digest_or_sentinel",
        "candidate_artifact_root",
        "current_source_content_root",
        "source_probe_key",
        "source_probe_ref",
        "pit_snapshot_digest",
        "semantic_profile_digest",
        "validation_fingerprint",
        "observed_at",
        "valid_until",
        "equivalence_mode",
        "outcome",
        "receipt_ref",
        "committed",
    )
    return tuple(row[field] for field in fields)


def _observation_identity(observation: AttestationObservationSpec) -> tuple[Any, ...]:
    return (
        observation.attestation_key,
        observation.attestation_target_key,
        observation.subject_type,
        observation.subject_digest,
        observation.candidate_identity,
        observation.producer_provenance_state,
        observation.producer_provenance_digest_or_sentinel,
        observation.candidate_artifact_root,
        observation.current_source_content_root,
        observation.source_probe_key,
        observation.source_probe_ref,
        observation.pit_snapshot_digest,
        observation.semantic_profile_digest,
        observation.validation_fingerprint,
        _iso(observation.observed_at),
        _iso(observation.valid_until),
        observation.equivalence_mode,
        observation.outcome,
        observation.receipt_ref,
        int(observation.committed),
    )


def _require_transition(current: str, target: str) -> None:
    if target not in RUN_TRANSITIONS.get(current, set()):
        raise StateConflict(f"invalid run transition: {current} -> {target}")


def _require_equal(row: Mapping[str, Any], field: str, expected: Any) -> None:
    if row[field] != expected:
        raise StateConflict(f"immutable identity mismatch for {field}: expected={expected!r} actual={row[field]!r}")


def _parse_time(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise StateConflict("durable timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise StateConflict("durable timestamp is not timezone-aware")
    return parsed.astimezone(UTC)


def _as_utc(value: datetime | None) -> datetime:
    observed = value or datetime.now(UTC)
    if observed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return observed.astimezone(UTC)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds")
