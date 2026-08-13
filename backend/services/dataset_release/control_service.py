from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import re
import secrets
from typing import Any, Callable, Mapping, Sequence
import uuid
from zoneinfo import ZoneInfo

from .cas_store import CASStore
from .canonical import digest_named_fields
from .contracts import (
    LogicalRequestIdentity,
    Scope,
    SubmissionIdentity,
    canonical_request_hash,
)
from .control_store import CandidateRegistrationSpec, ControlStore
from .errors import DatasetReleaseError
from .log_store import read_log_page
from .profile import DatasetProfile, InitialMigrationPlan, load_initial_migration_plan
from .resolution import ResolutionService
from .retention import DatasetReferenceState, classify_dataset_retention
from .state_machine import RUN_TRANSITIONS, TERMINAL_RUN_STATES
from .worker_identity import WorkerHeartbeatStore


MONTHLY_REQUEST_SCHEMA = "dataset_release_monthly_request_v1"
INITIAL_MIGRATION_REQUEST_SCHEMA = "dataset_release_initial_migration_request_v1"
MAX_RECEIPT_BYTES = 2 * 1024**2
SHANGHAI = ZoneInfo("Asia/Shanghai")
SOURCE_PROBE_POLICY_VERSION = "monthly_source_probe_v1"
PREVIEW_TOKEN_TTL_SECONDS = 300
_PREVIEW_TOKEN = re.compile(r"^dsp1_([0-9]{10})_([0-9a-f]{64})$")


class ProfileNotAllowed(DatasetReleaseError):
    code = "DATASET_RELEASE_PROFILE_NOT_ALLOWED"


class CandidateOnlyRequired(DatasetReleaseError):
    code = "DATASET_RELEASE_CANDIDATE_ONLY_REQUIRED"


class RecordNotFound(DatasetReleaseError):
    code = "DATASET_RELEASE_NOT_FOUND"


class ReceiptNotReady(DatasetReleaseError):
    code = "DATASET_RELEASE_RECEIPT_NOT_READY"


class LogIdentityInvalid(DatasetReleaseError):
    code = "DATASET_RELEASE_LOG_IDENTITY_INVALID"


class CutoffUnavailable(DatasetReleaseError):
    code = "DATASET_RELEASE_CUTOFF_UNAVAILABLE"
    retryable = True


class RunStateInvalid(DatasetReleaseError):
    code = "DATASET_RELEASE_RUN_STATE_INVALID"


@dataclass(frozen=True)
class DatasetReleaseProfileBinding:
    profile_id: str
    semantic_profile_digest: str
    cutoff_policy: str
    store: ControlStore
    cas: CASStore
    cutoff_resolver: Callable[[datetime], date]
    candidate_root_id: str | None = None
    source_content_probe_ttl_seconds: int = 86_400
    reconcile_catchup_months: int = 3
    reconcile_lease_ttl_seconds: int = 300
    config_digest: str | None = None
    worker_heartbeat_ttl_seconds: int = 30
    initial_migration_plans: Mapping[str, InitialMigrationPlan] = field(default_factory=dict)

    @classmethod
    def from_profile(cls, profile: DatasetProfile) -> "DatasetReleaseProfileBinding":
        store = ControlStore(Path(profile.control_root))
        plans = {
            plan_id: load_initial_migration_plan(profile.path.parent / "migrations" / f"{plan_id}.yaml")
            for plan_id in profile.initial_migration_plan_ids
        }
        return cls(
            profile_id=profile.profile,
            semantic_profile_digest=profile.semantic_profile_digest,
            cutoff_policy=profile.cutoff_policy,
            store=store,
            cas=CASStore(store.root),
            cutoff_resolver=resolve_previous_month_trading_cutoff,
            candidate_root_id=profile.candidate_root_id,
            source_content_probe_ttl_seconds=(profile.source_content_probe_ttl_seconds),
            reconcile_catchup_months=profile.reconcile_catchup_months,
            reconcile_lease_ttl_seconds=profile.reconcile_lease_ttl_seconds,
            config_digest=profile.config_digest,
            worker_heartbeat_ttl_seconds=profile.worker_heartbeat_ttl_seconds,
            initial_migration_plans=plans,
        )


class DatasetReleaseControlService:
    """Small durable control plane; it never resolves sources or runs exporters."""

    def __init__(self, bindings: Sequence[DatasetReleaseProfileBinding]) -> None:
        self._bindings = {binding.profile_id: binding for binding in bindings}
        if not self._bindings or len(self._bindings) != len(bindings):
            raise ValueError("profile bindings must be non-empty and unique")

    @property
    def profile_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._bindings))

    def preview_monthly(
        self,
        *,
        profile_id: str,
        cutoff_policy: str,
        scope: Scope | str,
        candidate_only: bool,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        binding = self._binding(profile_id)
        observed_at = now or datetime.now(UTC)
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("preview timestamp must be timezone-aware")
        if not candidate_only:
            raise CandidateOnlyRequired("dataset release control plane accepts candidate-only requests")
        if (
            cutoff_policy != "auto-previous-month"
            or binding.cutoff_policy != "previous_month_last_completed_trading_day"
        ):
            raise ProfileNotAllowed("cutoff policy is not registered for this profile")
        normalized_scope = Scope(scope)
        resolved_cutoff = binding.cutoff_resolver(observed_at)
        logical = LogicalRequestIdentity(
            profile=binding.profile_id,
            resolved_cutoff=resolved_cutoff,
            scope=normalized_scope,
            semantic_profile_digest=binding.semantic_profile_digest,
        )
        preview = {
            "schema_version": MONTHLY_REQUEST_SCHEMA,
            "profile": binding.profile_id,
            "cutoff_policy": cutoff_policy,
            "cutoff_resolution_policy": binding.cutoff_policy,
            "resolved_cutoff": resolved_cutoff.isoformat(),
            "scope": normalized_scope.value,
            "candidate_only": True,
            "logical_request_key": logical.key,
            "semantic_profile_digest": binding.semantic_profile_digest,
            "resolution": "worker_required",
            "activation": "not_requested",
            "node1": "not_requested",
            "db_repair": "not_requested",
            "restart": "not_requested",
            "cleanup": "not_requested",
        }
        expires_at = int(observed_at.astimezone(UTC).timestamp()) + PREVIEW_TOKEN_TTL_SECONDS
        preview["preview_expires_at"] = datetime.fromtimestamp(expires_at, UTC).isoformat(timespec="seconds")
        preview["preview_token"] = _build_preview_token(
            binding,
            preview,
            expires_at=expires_at,
        )
        return preview

    def submit_monthly(
        self,
        *,
        profile_id: str,
        cutoff_policy: str,
        scope: Scope | str,
        candidate_only: bool,
        principal: str,
        idempotency_key: str,
        route: str,
        now: datetime | None = None,
        preview_token: str | None = None,
    ) -> dict[str, Any]:
        observed_at = now or datetime.now(UTC)
        preview = self.preview_monthly(
            profile_id=profile_id,
            cutoff_policy=cutoff_policy,
            scope=scope,
            candidate_only=candidate_only,
            now=observed_at,
        )
        binding = self._binding(profile_id)
        preview_token_status = "not_supplied"
        if preview_token is not None:
            preview_token_status = (
                "valid"
                if _preview_token_matches(
                    binding,
                    preview,
                    preview_token,
                    observed_at=observed_at,
                )
                else "stale_or_mismatch"
            )
        request_payload = {
            key: value for key, value in preview.items() if key not in {"preview_token", "preview_expires_at"}
        }
        request_payload["supplied_preview_token"] = preview_token
        worker_health = self._worker_health(binding, now=observed_at)
        submitted = ResolutionService(binding.store, binding.cas).submit(
            identity=SubmissionIdentity(
                principal=principal,
                route=route,
                idempotency_key=idempotency_key,
            ),
            logical_request_key=str(preview["logical_request_key"]),
            request_payload=request_payload,
            initial_event_type=(
                "PREVIEW_TOKEN_STALE_OR_MISMATCH" if preview_token_status == "stale_or_mismatch" else None
            ),
            response_payload={
                "schema_version": "dataset_release_submission_response_v1",
                "worker_status": worker_health["state"],
                "worker_health": worker_health,
                "preview_token_status": preview_token_status,
            },
        )
        return dict(submitted)

    def submit_initial_migration(
        self,
        *,
        profile_id: str,
        plan_id: str,
        scope: Scope | str,
        candidate_only: bool,
        principal: str,
        idempotency_key: str,
        route: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Submit one fixed, repository-allowlisted first-migration intent."""

        binding = self._binding(profile_id)
        observed_at = now or datetime.now(UTC)
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("initial migration timestamp must be timezone-aware")
        if not candidate_only:
            raise CandidateOnlyRequired("initial migration requires candidate-only")
        try:
            plan = binding.initial_migration_plans[str(plan_id)]
        except KeyError as exc:
            raise ProfileNotAllowed("initial migration plan is not allowlisted for this profile") from exc
        normalized_scope = Scope(scope)
        if plan.profile != binding.profile_id or not plan.allows_scope(normalized_scope.value):
            raise ProfileNotAllowed("initial migration plan profile/scope differs")
        logical_request_key = digest_named_fields(
            "dataset_release_initial_migration_logical_request_v1",
            {
                "profile": binding.profile_id,
                "scope": normalized_scope.value,
                "cutoff": plan.cutoff,
                "semantic_profile_digest": binding.semantic_profile_digest,
                "plan_id": plan.plan_id,
                "plan_digest": plan.plan_digest,
            },
        )
        request_payload = {
            "schema_version": INITIAL_MIGRATION_REQUEST_SCHEMA,
            "profile": binding.profile_id,
            "operation": "initial-migration",
            "cutoff_policy": "fixed-allowlisted-plan",
            "resolved_cutoff": plan.cutoff.isoformat(),
            "scope": normalized_scope.value,
            "candidate_only": True,
            "logical_request_key": logical_request_key,
            "semantic_profile_digest": binding.semantic_profile_digest,
            "plan_id": plan.plan_id,
            "plan_digest": plan.plan_digest,
            "source_identity_policy": plan.source_identity_policy,
            "sample_instruments": list(plan.sample_instruments),
            "event_windows": [dict(item) for item in plan.event_windows],
            "index_windows": [dict(item) for item in plan.index_windows],
            "plan_safety": dict(plan.raw["safety"]),
            "resolution": "worker_required",
            "activation": "not_requested",
            "node1": "not_requested",
            "db_repair": "not_requested",
            "restart": "not_requested",
            "cleanup": "not_requested",
        }
        worker_health = self._worker_health(binding, now=observed_at)
        return dict(
            ResolutionService(binding.store, binding.cas).submit(
                identity=SubmissionIdentity(
                    principal=principal,
                    route=route,
                    idempotency_key=idempotency_key,
                ),
                logical_request_key=logical_request_key,
                request_payload=request_payload,
                response_payload={
                    "schema_version": "dataset_release_initial_migration_submission_response_v1",
                    "worker_status": worker_health["state"],
                    "worker_health": worker_health,
                    "plan_id": plan.plan_id,
                    "plan_digest": plan.plan_digest,
                    "fixed_cutoff": plan.cutoff.isoformat(),
                    "scope": normalized_scope.value,
                },
            )
        )

    def initial_migration_invocation_idempotency_key(
        self,
        *,
        profile_id: str,
        plan_id: str,
        scope: Scope | str,
        observed_at: datetime,
    ) -> str:
        binding = self._binding(profile_id)
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("initial migration timestamp must be timezone-aware")
        try:
            plan = binding.initial_migration_plans[str(plan_id)]
        except KeyError as exc:
            raise ProfileNotAllowed("initial migration plan is not allowlisted for this profile") from exc
        return "dsi_" + digest_named_fields(
            "dataset_release_initial_migration_invocation_v1",
            {
                "profile": profile_id,
                "plan_id": plan.plan_id,
                "plan_digest": plan.plan_digest,
                "scope": Scope(scope).value,
                "invocation_uuid": uuid.uuid4().hex,
            },
        )

    def monthly_invocation_idempotency_key(
        self,
        *,
        profile_id: str,
        scope: Scope | str,
        logical_request_key: str,
        observed_at: datetime,
    ) -> str:
        binding = self._binding(profile_id)
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("monthly invocation timestamp must be timezone-aware")
        return "dsi_" + digest_named_fields(
            "dataset_release_monthly_invocation_v1",
            {
                "profile": binding.profile_id,
                "scope": Scope(scope).value,
                "logical_request_key": logical_request_key,
                "semantic_profile_digest": binding.semantic_profile_digest,
                "probe_policy_version": SOURCE_PROBE_POLICY_VERSION,
                # Every manual invocation is a new durable request unless the
                # operator explicitly supplies an Idempotency-Key.  A TTL
                # bucket here would permanently replay an older submission
                # and suppress the fresh source probe required by the design.
                "invocation_uuid": uuid.uuid4().hex,
            },
        )

    def get_submission(self, profile_id: str, submission_id: str) -> dict[str, Any]:
        row = self._binding(profile_id).store.get_submission(submission_id)
        if row is None:
            raise RecordNotFound("dataset release submission was not found")
        return row

    def latest_status(self, profile_id: str) -> dict[str, Any]:
        """Return one bounded operator status without scanning candidates or logs."""

        binding = self._binding(profile_id)
        submission = binding.store.latest_submission()
        if submission is None:
            raise RecordNotFound("no dataset release submission is cataloged")
        run = None
        if submission.get("run_id"):
            run = self.get_run(profile_id, str(submission["run_id"]))
        run_state = str((run or {}).get("state") or "")
        retention = classify_dataset_retention(
            DatasetReferenceState(
                published=run_state == "SUCCEEDED",
                terminal_failure=bool(run_state in TERMINAL_RUN_STATES and run_state != "SUCCEEDED"),
            )
        )
        return {
            "schema_version": "dataset_release_status_v1",
            "profile": profile_id,
            "submission": submission,
            "run": run,
            "worker_health": self._worker_health(binding),
            "worker_execution": "not_started_by_status",
            "activation": "not_requested",
            "node1": "not_requested",
            "db_repair": "not_requested",
            "restart": "not_requested",
            "cleanup": "not_requested",
            "retention": retention.as_dict(),
        }

    def submit_reattest_latest(
        self,
        *,
        profile_id: str,
        scope: Scope | str,
        principal: str,
        idempotency_key: str,
        route: str,
    ) -> dict[str, Any]:
        """Freeze one catalog row into a durable read-only re-attestation request."""

        binding = self._binding(profile_id)
        normalized_scope = Scope(scope)
        candidate = binding.store.latest_candidate_registration(
            profile=binding.profile_id,
            scope=normalized_scope.value,
        )
        if candidate is None:
            raise RecordNotFound("no cataloged candidate is available for re-attestation")
        if binding.candidate_root_id is not None and candidate["allowlisted_root_id"] != binding.candidate_root_id:
            raise ProfileNotAllowed("cataloged candidate root id is no longer allowlisted")
        identity_fields = {
            "profile": binding.profile_id,
            "scope": normalized_scope.value,
            "resolved_cutoff": str(candidate["cutoff"]),
            "candidate_registration_id": str(candidate["registration_id"]),
            "candidate_identity": str(candidate["candidate_identity"]),
            "artifact_root": str(candidate["artifact_root"]),
            "semantic_profile_digest": binding.semantic_profile_digest,
        }
        logical_request_key = digest_named_fields("dataset_release_reattest_logical_request_v1", identity_fields)
        request = {
            "schema_version": "dataset_release_reattest_request_v1",
            "profile": binding.profile_id,
            "scope": normalized_scope.value,
            "resolved_cutoff": str(candidate["cutoff"]),
            "candidate_only": True,
            "logical_request_key": logical_request_key,
            "semantic_profile_digest": binding.semantic_profile_digest,
            "operation": "reattest-existing",
            "candidate_registration_id": candidate["registration_id"],
            "candidate_identity": candidate["candidate_identity"],
            "allowlisted_root_id": candidate["allowlisted_root_id"],
            "volume_serial": candidate["volume_serial"],
            "root_relative_path": candidate["root_relative_path"],
            "artifact_root": candidate["artifact_root"],
            "lineage_anchor": candidate["lineage_anchor"],
            "pit_provenance_state": candidate["pit_provenance_state"],
            "pit_provenance_digest_or_sentinel": candidate["pit_provenance_digest_or_sentinel"],
            "producer_provenance_state": candidate["producer_provenance_state"],
            "producer_provenance_digest_or_sentinel": candidate["producer_provenance_digest_or_sentinel"],
            "legacy_receipt_ref": candidate["legacy_receipt_ref"],
            "activation": "not_requested",
            "node1": "not_requested",
            "db_repair": "not_requested",
            "restart": "not_requested",
            "cleanup": "not_requested",
        }
        worker_health = self._worker_health(binding)
        submitted = ResolutionService(binding.store, binding.cas).submit(
            identity=SubmissionIdentity(
                principal=principal,
                route=route,
                idempotency_key=idempotency_key,
            ),
            logical_request_key=logical_request_key,
            request_payload=request,
            response_payload={
                "schema_version": "dataset_release_submission_response_v1",
                "candidate_registration_id": candidate["registration_id"],
                "candidate_identity": candidate["candidate_identity"],
                "candidate_write": "forbidden",
                "worker_status": worker_health["state"],
                "worker_health": worker_health,
            },
        )
        return dict(submitted)

    def latest_candidate_registration(
        self,
        *,
        profile_id: str,
        scope: Scope | str,
    ) -> dict[str, Any]:
        binding = self._binding(profile_id)
        row = binding.store.latest_candidate_registration(
            profile=binding.profile_id,
            scope=Scope(scope).value,
        )
        if row is None:
            raise RecordNotFound("no cataloged candidate is available")
        if binding.candidate_root_id is not None and row["allowlisted_root_id"] != binding.candidate_root_id:
            raise ProfileNotAllowed("cataloged candidate root id is no longer allowlisted")
        return row

    def reattest_invocation_idempotency_key(
        self,
        *,
        profile_id: str,
        scope: Scope | str,
        candidate: Mapping[str, Any],
        observed_at: datetime,
    ) -> str:
        binding = self._binding(profile_id)
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("re-attestation invocation timestamp must be timezone-aware")
        return "dsi_" + digest_named_fields(
            "dataset_release_reattest_invocation_v1",
            {
                "profile": binding.profile_id,
                "scope": Scope(scope).value,
                "candidate_registration_id": candidate["registration_id"],
                "candidate_identity": candidate["candidate_identity"],
                "artifact_root": candidate["artifact_root"],
                "semantic_profile_digest": binding.semantic_profile_digest,
                "probe_policy_version": SOURCE_PROBE_POLICY_VERSION,
                "invocation_uuid": uuid.uuid4().hex,
            },
        )

    def catalog_existing(
        self,
        *,
        profile_id: str,
        registration: CandidateRegistrationSpec,
    ) -> dict[str, Any]:
        """Register precomputed read-only evidence; never inspect or mutate a candidate."""

        binding = self._binding(profile_id)
        if registration.profile != binding.profile_id:
            raise ProfileNotAllowed("candidate manifest profile is not allowlisted")
        if binding.candidate_root_id is None or registration.allowlisted_root_id != binding.candidate_root_id:
            raise ProfileNotAllowed("candidate manifest root id is not allowlisted")
        return binding.store.register_candidate(registration)

    def catalog_legacy_candidate(
        self,
        *,
        profile_id: str,
        receipt: Mapping[str, Any],
        registration: CandidateRegistrationSpec,
    ) -> dict[str, Any]:
        """Persist deterministic read-only evidence, then register its exact path.

        The immutable CAS write may precede the SQLite registration: a crash in
        between leaves only an unreferenced content-addressed blob.  Repeating
        the same request reuses that blob and the same path binding.
        """

        binding = self._binding(profile_id)
        if (
            receipt.get("profile") != profile_id
            or receipt.get("root_relative_path") != registration.root_relative_path
            or receipt.get("artifact_root") != registration.artifact_root
            or receipt.get("pit_snapshot_digest") != registration.pit_provenance_digest_or_sentinel
        ):
            raise ProfileNotAllowed("legacy catalog receipt differs from registration")
        receipt_ref = binding.cas.put_json(dict(receipt))
        bound = replace(
            registration,
            lineage_anchor=(f"LEGACY_RECEIPT:dataset_release_legacy_catalog_evidence_v1:{receipt_ref.sha256}"),
            legacy_receipt_ref=receipt_ref.sha256,
        )
        row = self.catalog_existing(profile_id=profile_id, registration=bound)
        return {
            **row,
            "legacy_catalog_receipt_ref": receipt_ref.as_dict(),
            "candidate_write": "forbidden",
            "source_equivalence": "not_claimed_catalog_only",
        }

    def get_run(self, profile_id: str, run_id: str) -> dict[str, Any]:
        row = self._binding(profile_id).store.get_run(run_id)
        if row is None:
            raise RecordNotFound("dataset release run was not found")
        return row

    def list_runs(
        self,
        profile_id: str,
        *,
        states: Sequence[str] = (),
        before_created_at: str | None = None,
        before_run_id: str | None = None,
        limit: int = 51,
    ) -> list[dict[str, Any]]:
        allowed_states = set(RUN_TRANSITIONS).union(TERMINAL_RUN_STATES)
        normalized_states = tuple(dict.fromkeys(str(value).strip() for value in states if str(value).strip()))
        invalid = sorted(set(normalized_states).difference(allowed_states))
        if invalid:
            raise RunStateInvalid("run state filter contains an unsupported value")
        return self._binding(profile_id).store.list_runs(
            states=normalized_states,
            before_created_at=before_created_at,
            before_run_id=before_run_id,
            limit=limit,
        )

    def list_events(
        self,
        profile_id: str,
        *,
        submission_id: str | None = None,
        run_id: str | None = None,
        after_event_id: int = 0,
        limit: int = 51,
    ) -> list[dict[str, Any]]:
        binding = self._binding(profile_id)
        if submission_id is not None:
            self.get_submission(profile_id, submission_id)
        if run_id is not None:
            self.get_run(profile_id, run_id)
        return binding.store.list_events(
            submission_id=submission_id,
            run_id=run_id,
            after_event_id=after_event_id,
            limit=limit,
        )

    def enqueue_command(
        self,
        *,
        profile_id: str,
        target_type: str,
        target_id: str,
        command_type: str,
        principal: str,
        route: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        request_hash = canonical_request_hash(
            {
                "schema_version": "dataset_release_command_request_v1",
                "target_type": target_type,
                "target_id": target_id,
                "command_type": command_type,
            }
        )
        return self._binding(profile_id).store.enqueue_command(
            target_type=target_type,
            target_id=target_id,
            command_type=command_type,
            principal=principal,
            route=route,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            actor=principal,
        )

    def get_run_receipt(self, profile_id: str, run_id: str) -> Mapping[str, Any] | list[Any]:
        run = self.get_run(profile_id, run_id)
        reference = run.get("terminal_receipt_ref")
        if not reference:
            raise ReceiptNotReady("dataset release receipt is not ready")
        payload = self._binding(profile_id).cas.get_json_bounded(str(reference), max_bytes=MAX_RECEIPT_BYTES)
        if not isinstance(payload, (dict, list)):
            raise ReceiptNotReady("dataset release receipt has an invalid shape")
        return payload

    def get_submission_receipt(self, profile_id: str, submission_id: str) -> Mapping[str, Any] | list[Any]:
        submission = self.get_submission(profile_id, submission_id)
        reference = submission.get("terminal_receipt_ref")
        if not reference:
            raise ReceiptNotReady("dataset release submission receipt is not ready")
        payload = self._binding(profile_id).cas.get_json_bounded(str(reference), max_bytes=MAX_RECEIPT_BYTES)
        if not isinstance(payload, (dict, list)):
            raise ReceiptNotReady("dataset release submission receipt has an invalid shape")
        return payload

    def read_run_log(
        self,
        profile_id: str,
        run_id: str,
        *,
        stream: str,
        log_id: int,
        generation: int,
        byte_offset: int,
        max_bytes: int,
        max_lines: int,
    ) -> dict[str, Any]:
        run = self.get_run(profile_id, run_id)
        persisted_id = str(run["run_id"])
        if not re.fullmatch(r"dsr_[0-9a-f]{32}", persisted_id):
            raise LogIdentityInvalid("persisted run id cannot address the log catalog")
        binding = self._binding(profile_id)
        if log_id < 0:
            raise LogIdentityInvalid("run log catalog cursor is invalid")
        rows = binding.store.list_run_log_executions(
            run_id=persisted_id,
            at_or_after_log_id=max(1, int(log_id)),
            limit=2,
        )
        if log_id and (not rows or int(rows[0]["log_id"]) != int(log_id)):
            raise LogIdentityInvalid("run log catalog cursor does not exist")
        if not rows:
            return {
                "log_id": None,
                "attempt_id": None,
                "attempt_fence": None,
                "execution_id": None,
                "stream": stream,
                "generation": generation,
                "byte_offset": byte_offset,
                "text": "",
                "next_log_id": None,
                "next_generation": None,
                "next_byte_offset": None,
                "has_more": False,
            }
        current = rows[0]
        expected_relative = (
            f"attempt_runs/{current['attempt_id']}-{int(current['attempt_fence'])}/{current['execution_id']}/logs"
        )
        if current["relative_log_root"] != expected_relative:
            raise LogIdentityInvalid("durable run log root identity drifted")
        root = (binding.store.root / expected_relative).resolve(strict=False)
        if binding.store.root.resolve(strict=True) not in root.parents:
            raise LogIdentityInvalid("durable run log root escapes the control store")
        page = read_log_page(
            root,
            stream,
            generation=generation,
            byte_offset=byte_offset,
            max_bytes=max_bytes,
            max_lines=max_lines,
        )
        next_log_id = int(current["log_id"]) if page.has_more else None
        next_generation = page.next_generation
        next_byte_offset = page.next_byte_offset
        has_more = page.has_more
        if not has_more and len(rows) > 1:
            next_log_id = int(rows[1]["log_id"])
            next_generation = 1
            next_byte_offset = 0
            has_more = True
        return {
            "log_id": int(current["log_id"]),
            "attempt_id": current["attempt_id"],
            "attempt_fence": int(current["attempt_fence"]),
            "execution_id": current["execution_id"],
            "stream": stream,
            "generation": page.generation,
            "byte_offset": page.byte_offset,
            "text": page.data.decode("utf-8", errors="replace"),
            "next_log_id": next_log_id,
            "next_generation": next_generation,
            "next_byte_offset": next_byte_offset,
            "has_more": has_more,
        }

    def _binding(self, profile_id: str) -> DatasetReleaseProfileBinding:
        try:
            return self._bindings[profile_id]
        except KeyError as exc:
            raise ProfileNotAllowed("dataset release profile is not allowlisted") from exc

    @staticmethod
    def _worker_health(
        binding: DatasetReleaseProfileBinding,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if binding.config_digest is None:
            return {
                "state": "unavailable",
                "reason": "profile_config_digest_not_bound",
                "instance_id": None,
                "worker_status": None,
                "last_poll_at": None,
                "age_seconds": None,
                "capability_digest": None,
                "files_scanned": 0,
            }
        return (
            WorkerHeartbeatStore(binding.store)
            .read_latest(
                profile=binding.profile_id,
                config_digest=binding.config_digest,
                ttl_seconds=binding.worker_heartbeat_ttl_seconds,
                now=now,
            )
            .as_dict()
        )


def previous_month_end(value: datetime) -> date:
    observed = value.astimezone(SHANGHAI) if value.tzinfo is not None else value.replace(tzinfo=SHANGHAI)
    first = date(observed.year, observed.month, 1)
    return first - timedelta(days=1)


def _preview_token_payload(
    binding: DatasetReleaseProfileBinding,
    preview: Mapping[str, Any],
    *,
    expires_at: int,
) -> dict[str, Any]:
    return {
        "profile": preview["profile"],
        "cutoff_policy": preview["cutoff_policy"],
        "resolved_cutoff": preview["resolved_cutoff"],
        "scope": preview["scope"],
        "candidate_only": preview["candidate_only"],
        "logical_request_key": preview["logical_request_key"],
        "semantic_profile_digest": binding.semantic_profile_digest,
        "profile_config_digest": binding.config_digest or "CONFIG_DIGEST_UNBOUND",
        "expires_at": expires_at,
    }


def _build_preview_token(
    binding: DatasetReleaseProfileBinding,
    preview: Mapping[str, Any],
    *,
    expires_at: int,
) -> str:
    digest = digest_named_fields(
        "dataset_release_preview_token_v1",
        _preview_token_payload(binding, preview, expires_at=expires_at),
    )
    return f"dsp1_{expires_at:010d}_{digest}"


def _preview_token_matches(
    binding: DatasetReleaseProfileBinding,
    preview: Mapping[str, Any],
    token: str,
    *,
    observed_at: datetime,
) -> bool:
    match = _PREVIEW_TOKEN.fullmatch(token)
    if match is None:
        return False
    expires_at = int(match.group(1))
    observed_epoch = int(observed_at.astimezone(UTC).timestamp())
    if observed_epoch >= expires_at:
        return False
    expected = _build_preview_token(
        binding,
        preview,
        expires_at=expires_at,
    )
    return secrets.compare_digest(expected, token)


def resolve_previous_month_trading_cutoff(value: datetime) -> date:
    """Resolve the prior month through AIstock's official calendar, never weekday inference."""

    from backend.services.trading_calendar_status import TradingCalendarStatusService

    calendar_end = previous_month_end(value)
    try:
        resolved = TradingCalendarStatusService().latest_trading_day_on_or_before(calendar_end)
    except Exception as exc:
        raise CutoffUnavailable("official trading calendar could not resolve monthly cutoff") from exc
    if resolved is None or resolved.month != calendar_end.month:
        raise CutoffUnavailable("official trading calendar has no completed day in previous month")
    return resolved
