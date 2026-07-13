"""Fixture-only terminal-first selector for immutable Advisory observations."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.source_ledger import SourceLedgerError


OBSERVATION_SELECTOR_SCHEMA_VERSION = "advisory_phase1_observation_selector_v1"
OBSERVATION_SELECTOR_POLICY_VERSION = "advisory_phase1_observation_selector_policy_v1"
OBSERVATION_SELECTOR_POLICY_HASH = canonical_json_sha256(
    {
        "schema_version": OBSERVATION_SELECTOR_POLICY_VERSION,
        "terminal_resolution": "unique_max_observation_revision_no_as_of_evidence_available_at",
        "policies": ["EXACT_REVISION_V1", "LATEST_ELIGIBLE_REVISION_V1"],
        "capability_check": "after_terminal_resolution",
        "fallback": "forbidden",
    }
)

REASON_OBSERVATION_VERSION_UNAVAILABLE_AS_OF = "ADVISORY_PHASE1_OBSERVATION_VERSION_UNAVAILABLE_AS_OF"
REASON_OBSERVATION_VERSION_CHAIN_INVALID = "ADVISORY_PHASE1_OBSERVATION_VERSION_CHAIN_INVALID"
REASON_OBSERVATION_TERMINAL_CONFLICT = "ADVISORY_PHASE1_OBSERVATION_TERMINAL_CONFLICT"
REASON_OBSERVATION_EXACT_VERSION_MISMATCH = "ADVISORY_PHASE1_OBSERVATION_EXACT_VERSION_MISMATCH"
REASON_OBSERVATION_CAPABILITY_UNAVAILABLE = "ADVISORY_PHASE1_OBSERVATION_CAPABILITY_UNAVAILABLE"
REASON_OBSERVATION_CAPTURE_RECORD_INVALID = "ADVISORY_PHASE1_OBSERVATION_CAPTURE_RECORD_INVALID"
REASON_OBSERVATION_MAPPING_CONFLICT = "ADVISORY_PHASE1_OBSERVATION_MAPPING_CONFLICT"


class ObservationSelectionPolicy(str, Enum):
    EXACT_REVISION_V1 = "EXACT_REVISION_V1"
    LATEST_ELIGIBLE_REVISION_V1 = "LATEST_ELIGIBLE_REVISION_V1"


class ObservationSelectionStatus(str, Enum):
    SELECTED = "SELECTED"
    UNAVAILABLE = "UNAVAILABLE"
    CONFLICT = "CONFLICT"


class ObservationStatus(str, Enum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"


def _require_sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return value.astimezone(timezone.utc)


def _normalized_reason_codes(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(value) for value in values if str(value)}))


class FixtureObservationVersion(BaseModel):
    """A typed local projection of one immutable observation version."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_signal_id: str = Field(min_length=1, max_length=160)
    observation_version_id: str = Field(min_length=1, max_length=160)
    observation_content_hash: str = Field(min_length=64, max_length=64)
    observation_revision_no: int = Field(ge=1)
    supersedes_observation_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    evidence_available_at: datetime
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    signal_source_revision_set_id: str = Field(min_length=1, max_length=160)
    signal_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    observation_status: ObservationStatus
    capability: str = Field(min_length=1, max_length=160)
    stage_content_hashes: tuple[str, ...] = Field(min_length=1)
    stage_evidence_bundle_hash: str = Field(min_length=64, max_length=64)
    observation_payload: dict[str, Any]

    @field_validator(
        "observation_content_hash",
        "admission_scope_hash",
        "handoff_readiness_hash",
        "signal_source_revision_set_hash",
        "stage_evidence_bundle_hash",
    )
    @classmethod
    def _sha256(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name)

    @field_validator("stage_content_hashes")
    @classmethod
    def _stage_hashes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_require_sha256(value, field_name="stage_content_hash") for value in values)

    @field_validator("evidence_available_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="evidence_available_at")

    @model_validator(mode="after")
    def _validate_payload_closure(self) -> "FixtureObservationVersion":
        if self.observation_revision_no == 1 and self.supersedes_observation_version_id is not None:
            raise ValueError("first observation revision cannot have a predecessor")
        if self.observation_revision_no > 1 and self.supersedes_observation_version_id is None:
            raise ValueError("non-first observation revision requires predecessor")
        if canonical_json_sha256(canonicalize(self.observation_payload)) != self.observation_content_hash:
            raise ValueError("observation_content_hash does not match immutable observation payload")
        if self.observation_version_id != f"osv_{self.observation_content_hash[:20]}":
            raise ValueError("observation_version_id does not match immutable content hash")
        payload = self.observation_payload
        plan = payload.get("plan")
        stages = payload.get("stages")
        if not isinstance(plan, Mapping) or not isinstance(stages, list):
            raise ValueError("observation payload lacks frozen plan or stages")
        if (
            payload.get("canonical_signal_id") != self.canonical_signal_id
            or payload.get("observation_revision_no") != self.observation_revision_no
            or payload.get("supersedes_observation_version_id") != self.supersedes_observation_version_id
            or payload.get("observation_status") != self.observation_status.value
            or payload.get("stage_evidence_bundle_hash") != self.stage_evidence_bundle_hash
            or plan.get("admission_scope_id") != self.admission_scope_id
            or plan.get("admission_scope_hash") != self.admission_scope_hash
            or plan.get("handoff_readiness_hash") != self.handoff_readiness_hash
            or plan.get("signal_source_revision_set_id") != self.signal_source_revision_set_id
            or plan.get("signal_source_revision_set_hash") != self.signal_source_revision_set_hash
            or plan.get("capability") != self.capability
            or plan.get("evidence_available_at") != self.evidence_available_at.isoformat().replace("+00:00", "Z")
        ):
            raise ValueError("observation payload does not match typed immutable projection")
        stage_hashes = tuple(str(stage.get("content_hash") or "") for stage in stages if isinstance(stage, Mapping))
        if len(stage_hashes) != len(stages) or stage_hashes != self.stage_content_hashes:
            raise ValueError("observation stage content hashes are not closed")
        if canonical_json_sha256(list(self.stage_content_hashes)) != self.stage_evidence_bundle_hash:
            raise ValueError("stage_evidence_bundle_hash does not match stage content hashes")
        return self

    @classmethod
    def from_capture_record(cls, record: Any) -> "FixtureObservationVersion":
        """Adapt the existing immutable Phase 1C-1 capture record without a latest lookup."""

        try:
            payload = dict(record.observation_payload)
            plan = payload["plan"]
            stages = payload["stages"]
            if not isinstance(plan, Mapping) or not isinstance(stages, list):
                raise TypeError("capture payload plan/stages have invalid types")
            evidence_available_at = datetime.fromisoformat(str(plan["evidence_available_at"]).replace("Z", "+00:00"))
            return cls(
                canonical_signal_id=str(record.canonical_signal_id),
                observation_version_id=str(record.observation_version_id),
                observation_content_hash=str(record.observation_content_hash),
                observation_revision_no=int(payload["observation_revision_no"]),
                supersedes_observation_version_id=payload["supersedes_observation_version_id"],
                evidence_available_at=evidence_available_at,
                admission_scope_id=str(plan["admission_scope_id"]),
                admission_scope_hash=str(plan["admission_scope_hash"]),
                handoff_readiness_hash=str(plan["handoff_readiness_hash"]),
                signal_source_revision_set_id=str(plan["signal_source_revision_set_id"]),
                signal_source_revision_set_hash=str(plan["signal_source_revision_set_hash"]),
                observation_status=ObservationStatus(str(payload["observation_status"])),
                capability=str(plan["capability"]),
                stage_content_hashes=tuple(str(stage["content_hash"]) for stage in stages),
                stage_evidence_bundle_hash=str(payload["stage_evidence_bundle_hash"]),
                observation_payload=payload,
            )
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise SourceLedgerError(
                REASON_OBSERVATION_CAPTURE_RECORD_INVALID,
                "capture record cannot be adapted to one immutable observation version",
                context={"error_type": type(error).__name__},
            ) from error


def build_fixture_observation_version(
    *,
    canonical_signal_id: str,
    observation_revision_no: int,
    supersedes_observation_version_id: str | None,
    evidence_available_at: datetime,
    admission_scope_id: str,
    admission_scope_hash: str,
    handoff_readiness_hash: str,
    signal_source_revision_set_id: str,
    signal_source_revision_set_hash: str,
    observation_status: ObservationStatus,
    capability: str,
    stage_content_hashes: tuple[str, ...],
) -> FixtureObservationVersion:
    """Create a canonical local fixture version for selector contract tests."""

    normalized_available_at = _require_aware(evidence_available_at, field_name="evidence_available_at")
    stage_hashes = tuple(_require_sha256(value, field_name="stage_content_hash") for value in stage_content_hashes)
    stage_bundle_hash = canonical_json_sha256(list(stage_hashes))
    payload: dict[str, Any] = {
        "schema_version": "advisory_signal_observation_version_v1",
        "canonical_signal_id": canonical_signal_id,
        "plan": {
            "admission_scope_id": admission_scope_id,
            "admission_scope_hash": admission_scope_hash,
            "handoff_readiness_hash": handoff_readiness_hash,
            "signal_source_revision_set_id": signal_source_revision_set_id,
            "signal_source_revision_set_hash": signal_source_revision_set_hash,
            "capability": capability,
            "evidence_available_at": normalized_available_at.isoformat().replace("+00:00", "Z"),
        },
        "stage_evidence_bundle_hash": stage_bundle_hash,
        "observation_status": observation_status.value,
        "stages": [
            {"stage": f"fixture_stage_{index}", "content_hash": content_hash}
            for index, content_hash in enumerate(stage_hashes, start=1)
        ],
        "observation_revision_no": observation_revision_no,
        "supersedes_observation_version_id": supersedes_observation_version_id,
    }
    content_hash = canonical_json_sha256(canonicalize(payload))
    return FixtureObservationVersion(
        canonical_signal_id=canonical_signal_id,
        observation_version_id=f"osv_{content_hash[:20]}",
        observation_content_hash=content_hash,
        observation_revision_no=observation_revision_no,
        supersedes_observation_version_id=supersedes_observation_version_id,
        evidence_available_at=normalized_available_at,
        admission_scope_id=admission_scope_id,
        admission_scope_hash=admission_scope_hash,
        handoff_readiness_hash=handoff_readiness_hash,
        signal_source_revision_set_id=signal_source_revision_set_id,
        signal_source_revision_set_hash=signal_source_revision_set_hash,
        observation_status=observation_status,
        capability=capability,
        stage_content_hashes=stage_hashes,
        stage_evidence_bundle_hash=stage_bundle_hash,
        observation_payload=payload,
    )


class ObservationSelectionRequest(BaseModel):
    """Frozen v1 selector request; arbitrary ordering and current reads are excluded."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    selection_policy: ObservationSelectionPolicy
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    requested_source_cutoff: datetime
    required_observation_status: ObservationStatus
    required_capability: str = Field(min_length=1, max_length=160)
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    signal_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    explicit_observation_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    selection_policy_hash: str = OBSERVATION_SELECTOR_POLICY_HASH
    selector_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "admission_scope_hash",
        "handoff_readiness_hash",
        "signal_source_revision_set_hash",
        "selection_policy_hash",
        "selector_request_hash",
    )
    @classmethod
    def _sha256(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("requested_source_cutoff")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="requested_source_cutoff")

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": OBSERVATION_SELECTOR_SCHEMA_VERSION,
            "selection_policy": self.selection_policy.value,
            "selection_policy_hash": self.selection_policy_hash,
            "canonical_signal_id": self.canonical_signal_id,
            "requested_source_cutoff": self.requested_source_cutoff,
            "required_observation_status": self.required_observation_status.value,
            "required_capability": self.required_capability,
            "admission_scope_id": self.admission_scope_id,
            "admission_scope_hash": self.admission_scope_hash,
            "handoff_readiness_hash": self.handoff_readiness_hash,
            "signal_source_revision_set_hash": self.signal_source_revision_set_hash,
            "explicit_observation_version_id": self.explicit_observation_version_id,
        }

    @model_validator(mode="after")
    def _derive_hash(self) -> "ObservationSelectionRequest":
        if self.selection_policy_hash != OBSERVATION_SELECTOR_POLICY_HASH:
            raise ValueError("observation selector policy hash is invalid")
        if self.selection_policy is ObservationSelectionPolicy.EXACT_REVISION_V1 and self.explicit_observation_version_id is None:
            raise ValueError("EXACT_REVISION_V1 requires an explicit observation version")
        if self.selection_policy is ObservationSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1 and self.explicit_observation_version_id is not None:
            raise ValueError("LATEST_ELIGIBLE_REVISION_V1 cannot carry an explicit observation version")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.selector_request_hash is not None and self.selector_request_hash != digest:
            raise ValueError("selector_request_hash does not match selector request")
        object.__setattr__(self, "selector_request_hash", digest)
        return self


class SelectedObservationMapping(BaseModel):
    """Immutable selector result, including terminal and rejected reason evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    selector_request_hash: str = Field(min_length=64, max_length=64)
    selection_policy: ObservationSelectionPolicy
    selection_policy_hash: str = OBSERVATION_SELECTOR_POLICY_HASH
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    requested_source_cutoff: datetime
    required_observation_status: ObservationStatus
    required_capability: str = Field(min_length=1, max_length=160)
    explicit_observation_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    signal_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    terminal_observation_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    terminal_observation_content_hash: str | None = Field(default=None, min_length=64, max_length=64)
    terminal_revision_no: int | None = Field(default=None, ge=1)
    selection_status: ObservationSelectionStatus
    reason_codes: tuple[str, ...] = ()
    selected_mapping_id: str | None = Field(default=None, min_length=1, max_length=160)
    selected_mapping_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "selector_request_hash",
        "selection_policy_hash",
        "admission_scope_hash",
        "handoff_readiness_hash",
        "signal_source_revision_set_hash",
        "terminal_observation_content_hash",
        "selected_mapping_hash",
    )
    @classmethod
    def _sha256(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("requested_source_cutoff")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="requested_source_cutoff")

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": OBSERVATION_SELECTOR_SCHEMA_VERSION,
            "selector_request_hash": self.selector_request_hash,
            "selection_policy": self.selection_policy.value,
            "selection_policy_hash": self.selection_policy_hash,
            "canonical_signal_id": self.canonical_signal_id,
            "requested_source_cutoff": self.requested_source_cutoff,
            "required_observation_status": self.required_observation_status.value,
            "required_capability": self.required_capability,
            "explicit_observation_version_id": self.explicit_observation_version_id,
            "admission_scope_id": self.admission_scope_id,
            "admission_scope_hash": self.admission_scope_hash,
            "handoff_readiness_hash": self.handoff_readiness_hash,
            "signal_source_revision_set_hash": self.signal_source_revision_set_hash,
            "terminal_observation_version_id": self.terminal_observation_version_id,
            "terminal_observation_content_hash": self.terminal_observation_content_hash,
            "terminal_revision_no": self.terminal_revision_no,
            "selection_status": self.selection_status.value,
            "reason_codes": list(_normalized_reason_codes(self.reason_codes)),
        }

    @model_validator(mode="after")
    def _derive_identity(self) -> "SelectedObservationMapping":
        if self.selection_policy_hash != OBSERVATION_SELECTOR_POLICY_HASH:
            raise ValueError("observation selector policy hash is invalid")
        terminal_values = (
            self.terminal_observation_version_id,
            self.terminal_observation_content_hash,
            self.terminal_revision_no,
        )
        if any(value is None for value in terminal_values) and any(value is not None for value in terminal_values):
            raise ValueError("terminal observation mapping values must be present together")
        if self.selection_status is ObservationSelectionStatus.SELECTED:
            if any(value is None for value in terminal_values) or self.reason_codes:
                raise ValueError("selected mapping requires one terminal and no rejection reason")
        elif not self.reason_codes:
            raise ValueError("unavailable/conflict mapping requires a stable reason")
        digest = canonical_json_sha256(self.canonical_payload())
        mapping_id = f"som_{digest[:20]}"
        if self.selected_mapping_hash is not None and self.selected_mapping_hash != digest:
            raise ValueError("selected_mapping_hash does not match selector result")
        if self.selected_mapping_id is not None and self.selected_mapping_id != mapping_id:
            raise ValueError("selected_mapping_id does not match selector result")
        object.__setattr__(self, "reason_codes", _normalized_reason_codes(self.reason_codes))
        object.__setattr__(self, "selected_mapping_hash", digest)
        object.__setattr__(self, "selected_mapping_id", mapping_id)
        return self


class FixtureObservationVersionSelector:
    """Resolve one as-of terminal before checking capability or exact policy."""

    def select(
        self,
        *,
        request: ObservationSelectionRequest,
        observation_versions: Iterable[FixtureObservationVersion],
    ) -> SelectedObservationMapping:
        versions = tuple(observation_versions)
        if not versions:
            return self._mapping(
                request=request,
                terminal=None,
                selection_status=ObservationSelectionStatus.UNAVAILABLE,
                reason_codes=(REASON_OBSERVATION_VERSION_UNAVAILABLE_AS_OF,),
            )
        if any(version.canonical_signal_id != request.canonical_signal_id for version in versions):
            return self._mapping(
                request=request,
                terminal=None,
                selection_status=ObservationSelectionStatus.CONFLICT,
                reason_codes=(REASON_OBSERVATION_TERMINAL_CONFLICT,),
            )
        try:
            ordered = self._validate_chain(versions)
        except SourceLedgerError as error:
            return self._mapping(
                request=request,
                terminal=None,
                selection_status=ObservationSelectionStatus.CONFLICT,
                reason_codes=(error.reason_code,),
            )
        eligible = [version for version in ordered if version.evidence_available_at <= request.requested_source_cutoff]
        if not eligible:
            return self._mapping(
                request=request,
                terminal=None,
                selection_status=ObservationSelectionStatus.UNAVAILABLE,
                reason_codes=(REASON_OBSERVATION_VERSION_UNAVAILABLE_AS_OF,),
            )
        terminal = eligible[-1]
        if not self._matches_identity(request=request, terminal=terminal):
            return self._mapping(
                request=request,
                terminal=terminal,
                selection_status=ObservationSelectionStatus.CONFLICT,
                reason_codes=(REASON_OBSERVATION_TERMINAL_CONFLICT,),
            )
        if request.selection_policy is ObservationSelectionPolicy.EXACT_REVISION_V1 and (
            request.explicit_observation_version_id != terminal.observation_version_id
        ):
            return self._mapping(
                request=request,
                terminal=terminal,
                selection_status=ObservationSelectionStatus.CONFLICT,
                reason_codes=(REASON_OBSERVATION_EXACT_VERSION_MISMATCH,),
            )
        if (
            terminal.observation_status is not request.required_observation_status
            or terminal.capability != request.required_capability
        ):
            return self._mapping(
                request=request,
                terminal=terminal,
                selection_status=ObservationSelectionStatus.UNAVAILABLE,
                reason_codes=(REASON_OBSERVATION_CAPABILITY_UNAVAILABLE,),
            )
        return self._mapping(
            request=request,
            terminal=terminal,
            selection_status=ObservationSelectionStatus.SELECTED,
            reason_codes=(),
        )

    @staticmethod
    def _validate_chain(versions: tuple[FixtureObservationVersion, ...]) -> tuple[FixtureObservationVersion, ...]:
        by_revision: dict[int, FixtureObservationVersion] = {}
        by_version_id: dict[str, FixtureObservationVersion] = {}
        for version in versions:
            if version.observation_revision_no in by_revision or version.observation_version_id in by_version_id:
                raise SourceLedgerError(REASON_OBSERVATION_TERMINAL_CONFLICT, "observation revisions are ambiguous")
            by_revision[version.observation_revision_no] = version
            by_version_id[version.observation_version_id] = version
        ordered = tuple(by_revision[number] for number in sorted(by_revision))
        for expected_revision, version in enumerate(ordered, start=1):
            if version.observation_revision_no != expected_revision:
                raise SourceLedgerError(REASON_OBSERVATION_VERSION_CHAIN_INVALID, "observation revision numbers are not continuous")
            if expected_revision == 1:
                if version.supersedes_observation_version_id is not None:
                    raise SourceLedgerError(REASON_OBSERVATION_VERSION_CHAIN_INVALID, "first observation revision has predecessor")
                continue
            predecessor = ordered[expected_revision - 2]
            if (
                version.supersedes_observation_version_id != predecessor.observation_version_id
                or version.evidence_available_at < predecessor.evidence_available_at
            ):
                raise SourceLedgerError(REASON_OBSERVATION_VERSION_CHAIN_INVALID, "observation predecessor chain is invalid")
        return ordered

    @staticmethod
    def _matches_identity(*, request: ObservationSelectionRequest, terminal: FixtureObservationVersion) -> bool:
        return (
            terminal.admission_scope_id == request.admission_scope_id
            and terminal.admission_scope_hash == request.admission_scope_hash
            and terminal.handoff_readiness_hash == request.handoff_readiness_hash
            and terminal.signal_source_revision_set_hash == request.signal_source_revision_set_hash
        )

    @staticmethod
    def _mapping(
        *,
        request: ObservationSelectionRequest,
        terminal: FixtureObservationVersion | None,
        selection_status: ObservationSelectionStatus,
        reason_codes: tuple[str, ...],
    ) -> SelectedObservationMapping:
        return SelectedObservationMapping(
            selector_request_hash=str(request.selector_request_hash),
            selection_policy=request.selection_policy,
            selection_policy_hash=request.selection_policy_hash,
            canonical_signal_id=request.canonical_signal_id,
            requested_source_cutoff=request.requested_source_cutoff,
            required_observation_status=request.required_observation_status,
            required_capability=request.required_capability,
            explicit_observation_version_id=request.explicit_observation_version_id,
            admission_scope_id=request.admission_scope_id,
            admission_scope_hash=request.admission_scope_hash,
            handoff_readiness_hash=request.handoff_readiness_hash,
            signal_source_revision_set_hash=request.signal_source_revision_set_hash,
            terminal_observation_version_id=terminal.observation_version_id if terminal else None,
            terminal_observation_content_hash=terminal.observation_content_hash if terminal else None,
            terminal_revision_no=terminal.observation_revision_no if terminal else None,
            selection_status=selection_status,
            reason_codes=reason_codes,
        )


class InMemoryFixtureObservationRepository:
    """Append-only local repository used by selector and fixture tests."""

    def __init__(self) -> None:
        self._versions_by_signal: dict[str, list[FixtureObservationVersion]] = {}
        self._versions_by_hash: dict[str, FixtureObservationVersion] = {}

    def append(self, version: FixtureObservationVersion) -> FixtureObservationVersion:
        existing = self._versions_by_hash.get(version.observation_content_hash)
        if existing is not None:
            return existing
        prior = self._versions_by_signal.setdefault(version.canonical_signal_id, [])
        FixtureObservationVersionSelector._validate_chain(tuple([*prior, version]))
        prior.append(version)
        self._versions_by_hash[version.observation_content_hash] = version
        return version

    def for_signal(self, canonical_signal_id: str) -> tuple[FixtureObservationVersion, ...]:
        return tuple(sorted(self._versions_by_signal.get(canonical_signal_id, []), key=lambda item: item.observation_revision_no))


class InMemorySelectedObservationMappingRepository:
    """Immutable local oracle for selected-mapping retry and conflict semantics."""

    def __init__(self) -> None:
        self._by_id: dict[str, SelectedObservationMapping] = {}
        self._by_hash: dict[str, SelectedObservationMapping] = {}

    def save(self, mapping: SelectedObservationMapping) -> SelectedObservationMapping:
        identity = str(mapping.selected_mapping_id)
        content_hash = str(mapping.selected_mapping_hash)
        existing_by_id = self._by_id.get(identity)
        existing_by_hash = self._by_hash.get(content_hash)
        if existing_by_id is not None and existing_by_id.selected_mapping_hash != content_hash:
            raise SourceLedgerError(REASON_OBSERVATION_MAPPING_CONFLICT, "selected mapping id already binds different content")
        if existing_by_hash is not None and existing_by_hash.selected_mapping_id != identity:
            raise SourceLedgerError(REASON_OBSERVATION_MAPPING_CONFLICT, "selected mapping hash already binds different identity")
        if existing_by_id is not None:
            return existing_by_id
        self._by_id[identity] = mapping
        self._by_hash[content_hash] = mapping
        return mapping

    def get(self, mapping_id: str) -> SelectedObservationMapping | None:
        return self._by_id.get(mapping_id)
