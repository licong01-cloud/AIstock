from __future__ import annotations

import stat
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Callable, Iterable, Mapping

from .canonical import (
    ensure_sha256,
    normalize_root_relative_path,
)
from .cas_store import CASRef, CASStore
from .contracts import (
    UNKNOWN_PRODUCER_PROVENANCE,
    AttestationIdentity,
    CandidateIdentity,
    EquivalenceMode,
    PitProvenanceState,
    ProducerProvenanceState,
)
from .control_store import volume_identity
from .errors import DatasetReleaseError
from .lease import LeaseToken
from .publisher import ArtifactTreeSnapshot, artifact_tree_snapshot
from .state_machine import (
    AttestationObservationSpec,
    DatasetReleaseStateMachine,
    ReattestFinalizeSpec,
)


ATTESTATION_SCHEMA_VERSION = "dataset_release_attestation_v1"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


class CandidateReadOnlyError(DatasetReleaseError):
    code = "DATASET_RELEASE_CANDIDATE_READ_ONLY_VIOLATION"


class ReadOnlyCandidateHandle:
    """Allowlisted candidate reader with no mutation API and before/after roots."""

    def __init__(
        self,
        candidate_path: str | Path,
        *,
        allowlisted_roots: Iterable[str | Path],
        production_roots: Iterable[str | Path] = (),
    ) -> None:
        requested = Path(candidate_path).expanduser()
        if not requested.is_absolute():
            requested = requested.absolute()
        _assert_plain_existing_chain(requested)
        self._root = requested.resolve(strict=True)
        if not self._root.is_dir():
            raise CandidateReadOnlyError("candidate path must be a directory")
        _assert_plain_node(self._root)
        allowed_values = tuple(Path(value).expanduser() for value in allowlisted_roots)
        for value in allowed_values:
            _assert_plain_existing_chain(value.absolute() if not value.is_absolute() else value)
        allowed = tuple(value.resolve(strict=True) for value in allowed_values)
        if not allowed or not any(root != self._root and root in self._root.parents for root in allowed):
            raise CandidateReadOnlyError("candidate path is outside allowlisted roots")
        productions = tuple(Path(value).expanduser().resolve(strict=False) for value in production_roots)
        if any(root == self._root or root in self._root.parents or self._root in root.parents for root in productions):
            raise CandidateReadOnlyError("candidate path overlaps a production root")
        self._entered = False
        self._initial_snapshot: ArtifactTreeSnapshot | None = None

    def __enter__(self) -> "ReadOnlyCandidateHandle":
        if self._entered:
            raise CandidateReadOnlyError("candidate handle is not reentrant")
        self._initial_snapshot = artifact_tree_snapshot(self._root)
        self._entered = True
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        final_snapshot = artifact_tree_snapshot(self._root)
        self._entered = False
        if final_snapshot != self._initial_snapshot:
            raise CandidateReadOnlyError("candidate bytes changed during read-only re-attestation")
        return False

    @property
    def artifact_root(self) -> str:
        if not self._entered:
            raise CandidateReadOnlyError("candidate handle is not open")
        if self._initial_snapshot is None:
            raise CandidateReadOnlyError("candidate snapshot is unavailable")
        return self._initial_snapshot.sha256

    @property
    def file_count(self) -> int:
        if not self._entered:
            raise CandidateReadOnlyError("candidate handle is not open")
        if self._initial_snapshot is None:
            raise CandidateReadOnlyError("candidate snapshot is unavailable")
        return self._initial_snapshot.file_count

    @property
    def total_bytes(self) -> int:
        if not self._entered:
            raise CandidateReadOnlyError("candidate handle is not open")
        if self._initial_snapshot is None:
            raise CandidateReadOnlyError("candidate snapshot is unavailable")
        return self._initial_snapshot.total_bytes

    def open_file(self, relative_path: str | Path) -> BinaryIO:
        if not self._entered:
            raise CandidateReadOnlyError("candidate handle is not open")
        relative = _safe_relative_path(relative_path)
        path = (self._root / Path(relative)).resolve(strict=True)
        if self._root not in path.parents or not path.is_file():
            raise CandidateReadOnlyError(f"candidate file escapes root: {relative}")
        _assert_plain_node(path)
        return path.open("rb")


def _safe_relative_path(value: str | Path) -> str:
    raw = str(value).replace("\\", "/")
    path = PurePosixPath(raw)
    if (
        not raw
        or path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
        or any(character in raw for character in ("*", "?", "[", "]", ":"))
    ):
        raise CandidateReadOnlyError(f"invalid candidate-relative path: {value!r}")
    return path.as_posix()


def _assert_plain_node(path: Path) -> None:
    value = path.lstat()
    if stat.S_ISLNK(value.st_mode) or bool(getattr(value, "st_file_attributes", 0) & _REPARSE_POINT):
        raise CandidateReadOnlyError(f"candidate contains a symlink/reparse point: {path}")


def _assert_plain_existing_chain(path: Path) -> None:
    current = Path(path.anchor)
    if current.exists():
        _assert_plain_node(current)
    for part in path.parts[1:]:
        current = current / part
        if not current.exists():
            raise CandidateReadOnlyError(f"candidate path component is missing: {current}")
        _assert_plain_node(current)


@dataclass(frozen=True)
class LegacyValidationEvidence:
    artifact_identity_complete: bool
    artifact_valid: bool
    artifact_root_matches_catalog: bool
    validation_passed: bool
    full_required_component_coverage: bool
    full_current_source_value_parity: bool
    original_pit_snapshot_digest: str | None
    current_pit_snapshot_digest: str
    original_source_content_root: str | None
    current_source_content_root: str
    original_producer_provenance_digest: str | None
    validation_details: Mapping[str, Any]


@dataclass(frozen=True)
class LegacyAttestationDecision:
    outcome: EquivalenceMode
    eligible_for_noop_reuse: bool
    reason: str
    producer_provenance_state: ProducerProvenanceState
    producer_provenance_digest_or_sentinel: str


def decide_legacy_attestation(
    evidence: LegacyValidationEvidence,
) -> LegacyAttestationDecision:
    """Implement the complete fail-closed legacy provenance truth table."""

    ensure_sha256(evidence.current_source_content_root, field="current_source_content_root")
    ensure_sha256(evidence.current_pit_snapshot_digest, field="current_pit_snapshot_digest")
    producer_missing = evidence.original_producer_provenance_digest is None
    producer_state = ProducerProvenanceState.KNOWN if not producer_missing else ProducerProvenanceState.UNKNOWN
    producer_digest = (
        ensure_sha256(
            evidence.original_producer_provenance_digest,
            field="original_producer_provenance_digest",
        )
        if evidence.original_producer_provenance_digest is not None
        else UNKNOWN_PRODUCER_PROVENANCE
    )

    def decision(
        outcome: EquivalenceMode,
        eligible: bool,
        reason: str,
        *,
        reconstructed: bool = False,
    ) -> LegacyAttestationDecision:
        state = producer_state
        if reconstructed and producer_missing:
            state = ProducerProvenanceState.RECONSTRUCTED_SOURCE_ONLY
        return LegacyAttestationDecision(outcome, eligible, reason, state, producer_digest)

    if not evidence.artifact_identity_complete:
        return decision(
            EquivalenceMode.BLOCKED_LEGACY_PROVENANCE,
            False,
            "artifact identity is incomplete",
        )
    if not evidence.artifact_valid or not evidence.artifact_root_matches_catalog or not evidence.validation_passed:
        return decision(EquivalenceMode.INVALID, False, "artifact/value validation failed")

    # Missing PIT provenance is an unconditional ceiling even when every value happens to match.
    if evidence.original_pit_snapshot_digest is None:
        return decision(
            EquivalenceMode.ARTIFACT_VALID_ONLY,
            False,
            "legacy candidate lacks PIT provenance",
        )
    original_pit = ensure_sha256(
        evidence.original_pit_snapshot_digest,
        field="original_pit_snapshot_digest",
    )
    if original_pit != evidence.current_pit_snapshot_digest:
        return decision(
            EquivalenceMode.ARTIFACT_VALID_SOURCE_CHANGED,
            False,
            "current PIT snapshot differs from candidate PIT",
        )

    source_missing = evidence.original_source_content_root is None
    if not source_missing:
        original_source = ensure_sha256(
            evidence.original_source_content_root,
            field="original_source_content_root",
        )
        if original_source != evidence.current_source_content_root:
            return decision(
                EquivalenceMode.ARTIFACT_VALID_SOURCE_CHANGED,
                False,
                "current source content root differs from candidate input",
            )

    parity_complete = evidence.full_required_component_coverage and evidence.full_current_source_value_parity
    if source_missing or producer_missing:
        if parity_complete:
            return decision(
                EquivalenceMode.CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED,
                True,
                "missing source/producer provenance reconstructed by full-profile value parity",
                reconstructed=True,
            )
        return decision(
            EquivalenceMode.ARTIFACT_VALID_ONLY,
            False,
            "missing source/producer provenance without full-profile value parity",
        )
    if not evidence.full_required_component_coverage:
        return decision(
            EquivalenceMode.ARTIFACT_VALID_ONLY,
            False,
            "validation did not cover every required component",
        )
    return decision(
        EquivalenceMode.CURRENT_SOURCE_EQUIVALENT,
        True,
        "artifact, source, PIT, producer and validation identities are current",
    )


def _validate_candidate_provenance(
    candidate: CandidateIdentity,
    evidence: LegacyValidationEvidence,
) -> None:
    """Reject validator claims that contradict immutable candidate provenance."""

    if candidate.pit_provenance_state is PitProvenanceState.UNKNOWN:
        if evidence.original_pit_snapshot_digest is not None:
            raise CandidateReadOnlyError("validator cannot invent PIT provenance absent from candidate identity")
    elif evidence.original_pit_snapshot_digest != candidate.pit_provenance_digest_or_sentinel:
        raise CandidateReadOnlyError("validator PIT provenance differs from candidate identity")

    producer_known = candidate.producer_provenance_state is ProducerProvenanceState.KNOWN
    if producer_known:
        if evidence.original_producer_provenance_digest != candidate.producer_provenance_digest_or_sentinel:
            raise CandidateReadOnlyError("validator producer provenance differs from candidate identity")
    elif evidence.original_producer_provenance_digest is not None:
        raise CandidateReadOnlyError("validator cannot invent producer provenance absent from candidate identity")


@dataclass(frozen=True)
class AttestationRequest:
    candidate_identity: CandidateIdentity
    candidate_path: Path
    allowlisted_roots: Mapping[str, Path]
    production_roots: tuple[Path, ...]
    current_source_content_root: str
    current_pit_snapshot_digest: str
    source_probe_key: str
    source_probe_ref: str
    semantic_profile_digest: str
    validation_fingerprint: str
    observed_at: datetime
    valid_until: datetime


@dataclass(frozen=True)
class ReattestExecutionContext:
    run_id: str
    attempt_id: str
    expected_row_version: int
    attempt_fence: int
    tokens: tuple[LeaseToken, ...]
    finalized_at: datetime


@dataclass(frozen=True)
class AttestationResult:
    attestation_id: str
    attestation_key: str
    attestation_target_key: str
    candidate_identity: str
    receipt_ref: CASRef
    artifact_root: str
    outcome: EquivalenceMode
    eligible_for_noop_reuse: bool
    current_source_content_root: str
    pit_snapshot_digest: str
    semantic_profile_digest: str
    validation_fingerprint: str
    valid_until: datetime
    run: Mapping[str, Any]


class AttestationService:
    def __init__(
        self,
        cas: CASStore,
        state_machine: DatasetReleaseStateMachine,
    ) -> None:
        self.cas = cas
        self.state_machine = state_machine

    def reattest_existing(
        self,
        request: AttestationRequest,
        validator: Callable[[ReadOnlyCandidateHandle, str], LegacyValidationEvidence],
        *,
        execution: ReattestExecutionContext,
    ) -> AttestationResult:
        observed = _utc(request.observed_at)
        valid_until = _utc(request.valid_until)
        finalized_at = _utc(execution.finalized_at)
        if valid_until <= observed:
            raise CandidateReadOnlyError("attestation validity deadline must be in the future")
        run, plan = self._reattest_run_plan(execution)
        terminal_replay = run["state"] == "SUCCEEDED"
        if terminal_replay:
            return self._replay_terminal(request, execution, run, plan)
        if not terminal_replay and not observed <= finalized_at < valid_until:
            raise CandidateReadOnlyError("re-attestation finalization time is outside observation TTL")
        candidate_key = request.candidate_identity.key
        for name in (
            "current_source_content_root",
            "current_pit_snapshot_digest",
            "source_probe_key",
            "source_probe_ref",
            "semantic_profile_digest",
            "validation_fingerprint",
        ):
            ensure_sha256(getattr(request, name), field=name)
        self.cas.verify(request.source_probe_ref)
        try:
            allowlisted_root = request.allowlisted_roots[request.candidate_identity.allowlisted_root_id]
        except KeyError as exc:
            raise CandidateReadOnlyError("candidate allowlisted_root_id is not configured") from exc
        resolved_root = Path(allowlisted_root).expanduser().resolve(strict=True)
        resolved_candidate = request.candidate_path.expanduser().resolve(strict=True)
        try:
            actual_relative = normalize_root_relative_path(resolved_candidate.relative_to(resolved_root).as_posix())
        except ValueError as exc:
            raise CandidateReadOnlyError("candidate path is outside its identity-bound allowlisted root") from exc
        expected_relative = normalize_root_relative_path(request.candidate_identity.root_relative_path)
        if actual_relative != expected_relative:
            raise CandidateReadOnlyError("candidate path differs from immutable root-relative identity")
        if volume_identity(resolved_candidate) != request.candidate_identity.volume_serial:
            raise CandidateReadOnlyError("candidate volume differs from immutable candidate identity")
        with ReadOnlyCandidateHandle(
            resolved_candidate,
            allowlisted_roots=(resolved_root,),
            production_roots=request.production_roots,
        ) as handle:
            actual_artifact_root = handle.artifact_root
            evidence = validator(handle, actual_artifact_root)
            candidate_file_count = handle.file_count
            candidate_bytes = handle.total_bytes
        if evidence.current_source_content_root != request.current_source_content_root:
            raise CandidateReadOnlyError("validator current source root differs from request")
        if evidence.current_pit_snapshot_digest != request.current_pit_snapshot_digest:
            raise CandidateReadOnlyError("validator current PIT root differs from request")
        _validate_candidate_provenance(request.candidate_identity, evidence)
        evidence = replace(
            evidence,
            artifact_root_matches_catalog=(
                evidence.artifact_root_matches_catalog
                and actual_artifact_root == request.candidate_identity.artifact_root
            ),
        )
        decision = decide_legacy_attestation(evidence)
        identity = AttestationIdentity(
            candidate_identity=candidate_key,
            producer_provenance_state=decision.producer_provenance_state,
            producer_provenance_digest_or_sentinel=(decision.producer_provenance_digest_or_sentinel),
            artifact_root=actual_artifact_root,
            current_source_content_root=request.current_source_content_root,
            pit_digest=request.current_pit_snapshot_digest,
            semantic_profile_digest=request.semantic_profile_digest,
            validation_fingerprint=request.validation_fingerprint,
            equivalence_mode=decision.outcome,
            source_probe_key=request.source_probe_key,
        )
        expected_plan = {
            "attestation_target_key": identity.target_key,
            "attestation_observation_key": identity.key,
            "source_probe_key": request.source_probe_key,
            "source_probe_ref": request.source_probe_ref,
            "source_content_root": request.current_source_content_root,
            "pit_snapshot_digest": request.current_pit_snapshot_digest,
        }
        mismatched_plan = {
            field: {"expected": value, "actual": plan.get(field)}
            for field, value in expected_plan.items()
            if plan.get(field) != value
        }
        if mismatched_plan:
            raise CandidateReadOnlyError(
                "re-attestation result differs from immutable run plan",
                context=mismatched_plan,
            )
        receipt = {
            "schema_version": ATTESTATION_SCHEMA_VERSION,
            "attestation_key": identity.key,
            "attestation_observation_key": identity.key,
            "attestation_target_key": identity.target_key,
            "run_id": execution.run_id,
            "run_generation_digest": run["run_generation_digest"],
            "resolved_intent_key": plan.get("resolved_intent_key"),
            "candidate_identity": candidate_key,
            "candidate_path_identity": request.candidate_identity.root_relative_path,
            "candidate_artifact_root": actual_artifact_root,
            "catalog_artifact_root": request.candidate_identity.artifact_root,
            "producer_provenance_state": decision.producer_provenance_state.value,
            "producer_provenance_digest_or_sentinel": (decision.producer_provenance_digest_or_sentinel),
            "current_source_content_root": request.current_source_content_root,
            "source_probe_key": request.source_probe_key,
            "source_probe_ref": request.source_probe_ref,
            "pit_snapshot_digest": request.current_pit_snapshot_digest,
            "semantic_profile_digest": request.semantic_profile_digest,
            "validation_fingerprint": request.validation_fingerprint,
            "observed_at": observed.isoformat().replace("+00:00", "Z"),
            "valid_until": valid_until.isoformat().replace("+00:00", "Z"),
            "equivalence_mode": decision.outcome.value,
            "outcome": decision.outcome.value,
            "eligible_for_noop_reuse": decision.eligible_for_noop_reuse,
            "reason": decision.reason,
            "legacy_truth_inputs": {
                "artifact_identity_complete": evidence.artifact_identity_complete,
                "artifact_valid": evidence.artifact_valid,
                "artifact_root_matches_catalog": evidence.artifact_root_matches_catalog,
                "validation_passed": evidence.validation_passed,
                "full_required_component_coverage": evidence.full_required_component_coverage,
                "full_current_source_value_parity": evidence.full_current_source_value_parity,
                "original_pit_snapshot_digest": evidence.original_pit_snapshot_digest,
                "original_source_content_root": evidence.original_source_content_root,
                "original_producer_provenance_digest": (evidence.original_producer_provenance_digest),
            },
            "validation_details": dict(evidence.validation_details),
            "read_only": {
                "candidate_files": candidate_file_count,
                "candidate_bytes": candidate_bytes,
                "candidate_writes": 0,
            },
            "safety": _zero_safety(),
        }
        reference = self.cas.put_json(receipt)
        self.cas.verify(reference)
        observation = AttestationObservationSpec(
            attestation_id=None,
            attestation_key=identity.key,
            attestation_target_key=identity.target_key,
            subject_type="candidate",
            subject_digest=candidate_key,
            candidate_identity=candidate_key,
            producer_provenance_state=decision.producer_provenance_state.value,
            producer_provenance_digest_or_sentinel=(decision.producer_provenance_digest_or_sentinel),
            candidate_artifact_root=actual_artifact_root,
            current_source_content_root=request.current_source_content_root,
            source_probe_key=request.source_probe_key,
            source_probe_ref=request.source_probe_ref,
            pit_snapshot_digest=request.current_pit_snapshot_digest,
            semantic_profile_digest=request.semantic_profile_digest,
            validation_fingerprint=request.validation_fingerprint,
            observed_at=observed,
            valid_until=valid_until,
            equivalence_mode=decision.outcome.value,
            outcome=decision.outcome.value,
            receipt_ref=reference.sha256,
            committed=True,
        )
        terminal_run = self.state_machine.finalize_reattest(
            ReattestFinalizeSpec(
                run_id=execution.run_id,
                attempt_id=execution.attempt_id,
                expected_row_version=execution.expected_row_version,
                attempt_fence=execution.attempt_fence,
                tokens=execution.tokens,
                observation=observation,
            ),
            now=finalized_at,
        )
        with self.state_machine.store.transaction(immediate=False) as connection:
            row = connection.execute(
                "SELECT attestation_id FROM attestations WHERE attestation_key=?",
                (identity.key,),
            ).fetchone()
        if row is None:
            raise CandidateReadOnlyError("reattest finalization did not commit its observation")
        attestation_id = str(row["attestation_id"])
        return AttestationResult(
            attestation_id=attestation_id,
            attestation_key=identity.key,
            attestation_target_key=identity.target_key,
            candidate_identity=candidate_key,
            receipt_ref=reference,
            artifact_root=actual_artifact_root,
            outcome=decision.outcome,
            eligible_for_noop_reuse=decision.eligible_for_noop_reuse,
            current_source_content_root=request.current_source_content_root,
            pit_snapshot_digest=request.current_pit_snapshot_digest,
            semantic_profile_digest=request.semantic_profile_digest,
            validation_fingerprint=request.validation_fingerprint,
            valid_until=valid_until,
            run=terminal_run,
        )

    def _reattest_run_plan(
        self,
        execution: ReattestExecutionContext,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        run = self.state_machine.store.get_run(execution.run_id)
        if run is None:
            raise CandidateReadOnlyError("re-attestation run does not exist")
        active = (
            run["state"] == "REATTESTING"
            and run["active_attempt_id"] == execution.attempt_id
            and int(run["row_version"]) == execution.expected_row_version
        )
        terminal_replay = (
            run["state"] == "SUCCEEDED"
            and run["outcome"] == "REATTESTED"
            and run["active_attempt_id"] is None
            and int(run["row_version"]) == execution.expected_row_version + 1
        )
        if run["operation_kind"] != "REATTEST" or not (active or terminal_replay):
            raise CandidateReadOnlyError("re-attestation run ownership or operation kind is invalid")
        plan = self.cas.get_json_bounded(run["plan_ref"], max_bytes=16 * 1024 * 1024)
        if not isinstance(plan, Mapping) or plan.get("operation_kind") != "REATTEST":
            raise CandidateReadOnlyError("re-attestation run plan is invalid")
        return run, plan

    def _replay_terminal(
        self,
        request: AttestationRequest,
        execution: ReattestExecutionContext,
        run: Mapping[str, Any],
        plan: Mapping[str, Any],
    ) -> AttestationResult:
        candidate_key = request.candidate_identity.key
        with self.state_machine.store.transaction(immediate=False) as connection:
            rows = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM attestations WHERE attestation_key=? AND committed=1",
                    (plan.get("attestation_observation_key"),),
                ).fetchall()
            ]
        if len(rows) != 1:
            raise CandidateReadOnlyError("terminal re-attestation observation is missing or ambiguous")
        row = rows[0]
        expected = {
            "attestation_target_key": plan.get("attestation_target_key"),
            "candidate_identity": candidate_key,
            "candidate_artifact_root": request.candidate_identity.artifact_root,
            "current_source_content_root": request.current_source_content_root,
            "source_probe_key": request.source_probe_key,
            "source_probe_ref": request.source_probe_ref,
            "pit_snapshot_digest": request.current_pit_snapshot_digest,
            "semantic_profile_digest": request.semantic_profile_digest,
            "validation_fingerprint": request.validation_fingerprint,
            "receipt_ref": run["terminal_receipt_ref"],
        }
        mismatch = {
            field: {"expected": value, "actual": row.get(field)}
            for field, value in expected.items()
            if row.get(field) != value
        }
        if mismatch:
            raise CandidateReadOnlyError(
                "terminal re-attestation replay identity differs",
                context=mismatch,
            )
        reference = self.cas.verify(str(row["receipt_ref"]))
        receipt = self.cas.get_json_bounded(reference, max_bytes=16 * 1024 * 1024)
        if (
            not isinstance(receipt, Mapping)
            or receipt.get("schema_version") != ATTESTATION_SCHEMA_VERSION
            or receipt.get("attestation_observation_key") != row["attestation_key"]
            or receipt.get("attestation_target_key") != row["attestation_target_key"]
            or receipt.get("run_id") != execution.run_id
        ):
            raise CandidateReadOnlyError("terminal re-attestation receipt identity is invalid")
        observation = AttestationObservationSpec(
            attestation_id=str(row["attestation_id"]),
            attestation_key=str(row["attestation_key"]),
            attestation_target_key=str(row["attestation_target_key"]),
            subject_type=str(row["subject_type"]),
            subject_digest=str(row["subject_digest"]),
            candidate_identity=str(row["candidate_identity"]),
            producer_provenance_state=str(row["producer_provenance_state"]),
            producer_provenance_digest_or_sentinel=str(row["producer_provenance_digest_or_sentinel"]),
            candidate_artifact_root=str(row["candidate_artifact_root"]),
            current_source_content_root=str(row["current_source_content_root"]),
            source_probe_key=str(row["source_probe_key"]),
            source_probe_ref=str(row["source_probe_ref"]),
            pit_snapshot_digest=str(row["pit_snapshot_digest"]),
            semantic_profile_digest=str(row["semantic_profile_digest"]),
            validation_fingerprint=str(row["validation_fingerprint"]),
            observed_at=_parse_utc(row["observed_at"]),
            valid_until=_parse_utc(row["valid_until"]),
            equivalence_mode=str(row["equivalence_mode"]),
            outcome=str(row["outcome"]),
            receipt_ref=str(row["receipt_ref"]),
            committed=True,
        )
        terminal_run = self.state_machine.finalize_reattest(
            ReattestFinalizeSpec(
                run_id=execution.run_id,
                attempt_id=execution.attempt_id,
                expected_row_version=execution.expected_row_version,
                attempt_fence=execution.attempt_fence,
                tokens=execution.tokens,
                observation=observation,
            ),
            now=_utc(execution.finalized_at),
        )
        try:
            outcome = EquivalenceMode(str(row["outcome"]))
        except ValueError as exc:
            raise CandidateReadOnlyError("terminal re-attestation outcome is invalid") from exc
        return AttestationResult(
            attestation_id=str(row["attestation_id"]),
            attestation_key=str(row["attestation_key"]),
            attestation_target_key=str(row["attestation_target_key"]),
            candidate_identity=candidate_key,
            receipt_ref=reference,
            artifact_root=str(row["candidate_artifact_root"]),
            outcome=outcome,
            eligible_for_noop_reuse=bool(receipt.get("eligible_for_noop_reuse", False)),
            current_source_content_root=str(row["current_source_content_root"]),
            pit_snapshot_digest=str(row["pit_snapshot_digest"]),
            semantic_profile_digest=str(row["semantic_profile_digest"]),
            validation_fingerprint=str(row["validation_fingerprint"]),
            valid_until=_parse_utc(row["valid_until"]),
            run=terminal_run,
        )


def _zero_safety() -> dict[str, int]:
    return {
        "database_writes": 0,
        "production_writes": 0,
        "production_deletes": 0,
        "production_pointer_changes": 0,
        "service_process_controls": 0,
        "candidate_writes": 0,
    }


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CandidateReadOnlyError("attestation timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _parse_utc(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise CandidateReadOnlyError("attestation timestamp is not valid ISO-8601") from exc
    return _utc(parsed)
