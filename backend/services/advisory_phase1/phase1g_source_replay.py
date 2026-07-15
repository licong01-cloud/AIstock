"""Pure Phase 1G G2 source-operation parsing and same-cutoff replay."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal, Mapping, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.phase1g_contract import (
    Phase1GTargetExecutionRequest,
)
from backend.services.advisory_phase1.phase1g_phase1e_projection import (
    Phase1EExecutionPlanProjection,
    Phase1EOperationDisposition,
    Phase1EPlannedOperationType,
)
from backend.services.advisory_phase1.source_ledger import (
    SourceAvailabilityEvent,
    source_partition_chain_key,
)
from backend.services.advisory_phase1.source_resolution import (
    FixtureSourceRevisionResolver,
    RequirementResolutionStatus,
    SourceRequirement,
    SourceRequirementSet,
    SourceResolutionReceipt,
)
from backend.services.advisory_phase1.source_revision import (
    SourceRevisionMemberInput,
    SourceRevisionSet,
)


PHASE1E_SOURCE_RESOLUTION_OPERATION_SCHEMA_VERSION = (
    "advisory_phase1e_source_resolution_operation_v1"
)
SOURCE_OPERATION_PROJECTION_SCHEMA_VERSION = (
    "advisory_phase1g_source_operation_projection_v1"
)
SOURCE_EVENT_REFERENCE_SCHEMA_VERSION = "advisory_phase1g_source_event_reference_v1"
SOURCE_FREEZE_INTENT_SCHEMA_VERSION = (
    "advisory_phase1g_source_revision_freeze_intent_v1"
)
SOURCE_REPLAY_RESULT_SCHEMA_VERSION = "advisory_phase1g_source_replay_result_v1"

REASON_SOURCE_OPERATION_INVALID = "ADVISORY_PHASE1G_SOURCE_OPERATION_INVALID"
REASON_SOURCE_REPLAY_INPUT_INVALID = "ADVISORY_PHASE1G_SOURCE_REPLAY_INPUT_INVALID"
REASON_SOURCE_REPLAY_UNAVAILABLE = "ADVISORY_PHASE1G_SOURCE_REPLAY_UNAVAILABLE"
REASON_SOURCE_REPLAY_MISMATCH = "ADVISORY_PHASE1G_SOURCE_REPLAY_MISMATCH"
REASON_SOURCE_REVISION_CONFLICT = "ADVISORY_PHASE1G_SOURCE_REVISION_CONFLICT"
REASON_G2_UNEXPECTED_ERROR = "ADVISORY_PHASE1G_G2_UNEXPECTED_ERROR"


class Phase1GSourceReplayError(RuntimeError):
    """Fail-closed source replay error with redacted structured context."""

    def __init__(
        self, reason_code: str, detail: str, *, context: dict[str, Any] | None = None
    ) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.context = canonicalize(context or {})


class _FrozenList(list[Any]):
    """JSON-compatible list that cannot invalidate a frozen contract in place."""

    @staticmethod
    def _immutable(*_args: Any, **_kwargs: Any) -> None:
        raise TypeError("frozen contract collections cannot be mutated")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable

    def __deepcopy__(self, _memo: dict[int, Any]) -> "_FrozenList":
        return self


class _FrozenDict(dict[str, Any]):
    """JSON-compatible dict that cannot invalidate a frozen contract in place."""

    @staticmethod
    def _immutable(*_args: Any, **_kwargs: Any) -> None:
        raise TypeError("frozen contract collections cannot be mutated")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __ior__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable

    def __deepcopy__(self, _memo: dict[int, Any]) -> "_FrozenDict":
        return self


def _deep_freeze_contract_value(value: Any) -> Any:
    if isinstance(value, BaseModel):
        frozen = value.model_copy(deep=True)
        for field_name in type(frozen).model_fields:
            object.__setattr__(
                frozen,
                field_name,
                _deep_freeze_contract_value(getattr(frozen, field_name)),
            )
        return frozen
    if isinstance(value, dict):
        return _FrozenDict(
            {str(key): _deep_freeze_contract_value(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return _FrozenList(_deep_freeze_contract_value(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_deep_freeze_contract_value(item) for item in value)
    if isinstance(value, set | frozenset):
        return frozenset(_deep_freeze_contract_value(item) for item in value)
    return value


class _StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="after")
    def _deep_freeze_nested_values(self) -> "_StrictContract":
        for field_name in type(self).model_fields:
            object.__setattr__(
                self,
                field_name,
                _deep_freeze_contract_value(getattr(self, field_name)),
            )
        return self

    def model_copy(
        self, *, update: Mapping[str, Any] | None = None, deep: bool = False
    ) -> Self:
        if not update:
            return super().model_copy(deep=deep)
        payload = self.model_dump(mode="python")
        payload.update(dict(update))
        return type(self).model_validate(payload)


def _sha256(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(
        char not in "0123456789abcdef" for char in normalized
    ):
        raise ValueError(f"{field_name} must be lowercase sha256")
    return normalized


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


class Phase1GSourceSetRef(_StrictContract):
    source_revision_set_id: str = Field(min_length=1, max_length=160)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    capture_plan_hash: str = Field(min_length=64, max_length=64)

    @field_validator("source_revision_set_hash", "capture_plan_hash")
    @classmethod
    def _hashes(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)


class Phase1GSourceOperationProjection(_StrictContract):
    schema_version: Literal[SOURCE_OPERATION_PROJECTION_SCHEMA_VERSION] = (
        SOURCE_OPERATION_PROJECTION_SCHEMA_VERSION
    )
    target_request_hash: str = Field(min_length=64, max_length=64)
    phase1e_plan_id: str = Field(min_length=1, max_length=160)
    phase1e_plan_hash: str = Field(min_length=64, max_length=64)
    phase1e_operation_hash: str = Field(min_length=64, max_length=64)
    program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: Literal["single_alpha", "multi_alpha"]
    source_operation_contract_version: Literal[
        PHASE1E_SOURCE_RESOLUTION_OPERATION_SCHEMA_VERSION
    ]
    requirement_set: SourceRequirementSet
    embedded_receipt: SourceResolutionReceipt
    expected_capture_source_sets: tuple[Phase1GSourceSetRef, ...]
    source_operation_projection_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )

    @field_validator(
        "target_request_hash",
        "phase1e_plan_hash",
        "phase1e_operation_hash",
        "admission_scope_hash",
        "manifest_sha256",
        "source_operation_projection_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_hash(self) -> "Phase1GSourceOperationProjection":
        refs = tuple(
            sorted(
                self.expected_capture_source_sets,
                key=lambda item: (
                    item.capture_plan_hash,
                    item.source_revision_set_hash,
                ),
            )
        )
        if len({item.capture_plan_hash for item in refs}) != len(refs):
            raise ValueError("capture plan source refs must have unique plan hashes")
        object.__setattr__(self, "expected_capture_source_sets", refs)
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"source_operation_projection_hash"})
        )
        if (
            self.source_operation_projection_hash is not None
            and self.source_operation_projection_hash != digest
        ):
            raise ValueError(
                "source_operation_projection_hash does not match projection"
            )
        object.__setattr__(self, "source_operation_projection_hash", digest)
        return self


class Phase1GSourceEventReference(_StrictContract):
    schema_version: Literal[SOURCE_EVENT_REFERENCE_SCHEMA_VERSION] = (
        SOURCE_EVENT_REFERENCE_SCHEMA_VERSION
    )
    requirement_id: str = Field(min_length=1, max_length=160)
    consumer_scope_id: str = Field(min_length=1, max_length=160)
    dataset_name: str = Field(min_length=1, max_length=160)
    source_role: str = Field(min_length=1, max_length=80)
    partition_key_hash: str = Field(min_length=64, max_length=64)
    partition_chain_key: str = Field(min_length=64, max_length=64)
    revision_id: str = Field(min_length=1, max_length=160)
    event_type: str = Field(min_length=1, max_length=40)
    event_revision_no: int = Field(ge=1)
    availability_event_id: str = Field(min_length=1, max_length=160)
    event_content_hash: str = Field(min_length=64, max_length=64)
    predecessor_event_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    business_min_date: date
    business_max_date: date
    provider_published_at: datetime | None = None
    first_observed_at: datetime
    formal_available_at: datetime
    schema_fingerprint: str = Field(min_length=1, max_length=160)
    row_count: int = Field(ge=0)
    partition_content_hash: str = Field(min_length=64, max_length=64)
    quality_status: str = Field(min_length=1, max_length=32)
    research_only: Literal[True] = True

    @field_validator(
        "partition_key_hash",
        "partition_chain_key",
        "event_content_hash",
        "predecessor_event_hash",
        "partition_content_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator(
        "provider_published_at", "first_observed_at", "formal_available_at"
    )
    @classmethod
    def _timestamps(cls, value: datetime | None, info) -> datetime | None:  # type: ignore[no-untyped-def]
        return _aware(value, field_name=info.field_name) if value is not None else None


class Phase1GSourceRevisionFreezeIntent(_StrictContract):
    schema_version: Literal[SOURCE_FREEZE_INTENT_SCHEMA_VERSION] = (
        SOURCE_FREEZE_INTENT_SCHEMA_VERSION
    )
    target_request_hash: str = Field(min_length=64, max_length=64)
    requirement_set_id: str = Field(min_length=1, max_length=160)
    requirement_set_hash: str = Field(min_length=64, max_length=64)
    resolution_receipt_id: str = Field(min_length=1, max_length=160)
    resolution_receipt_hash: str = Field(min_length=64, max_length=64)
    source_revision_set: SourceRevisionSet
    expected_member_count: int = Field(ge=1)
    expected_member_hash: str = Field(min_length=64, max_length=64)
    research_only: Literal[True] = True
    execution_prohibited: Literal[True] = True
    freeze_intent_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "target_request_hash",
        "requirement_set_hash",
        "resolution_receipt_hash",
        "expected_member_hash",
        "freeze_intent_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_hash(self) -> "Phase1GSourceRevisionFreezeIntent":
        if self.expected_member_count != len(self.source_revision_set.members):
            raise ValueError("freeze intent member count does not match source set")
        member_hash = canonical_json_sha256(
            [member.content_payload() for member in self.source_revision_set.members]
        )
        if member_hash != self.expected_member_hash:
            raise ValueError("freeze intent member hash does not match source set")
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"freeze_intent_hash"})
        )
        if self.freeze_intent_hash is not None and self.freeze_intent_hash != digest:
            raise ValueError("freeze_intent_hash does not match freeze intent")
        object.__setattr__(self, "freeze_intent_hash", digest)
        return self


class Phase1GSourceReplayResult(_StrictContract):
    schema_version: Literal[SOURCE_REPLAY_RESULT_SCHEMA_VERSION] = (
        SOURCE_REPLAY_RESULT_SCHEMA_VERSION
    )
    target_request_hash: str = Field(min_length=64, max_length=64)
    phase1e_plan_id: str = Field(min_length=1, max_length=160)
    phase1e_plan_hash: str = Field(min_length=64, max_length=64)
    source_operation_projection_hash: str = Field(min_length=64, max_length=64)
    requirement_set_id: str = Field(min_length=1, max_length=160)
    requirement_set_hash: str = Field(min_length=64, max_length=64)
    embedded_resolution_receipt: SourceResolutionReceipt
    replayed_resolution_receipt: SourceResolutionReceipt
    source_revision_set: SourceRevisionSet
    source_revision_member_count: int = Field(ge=1)
    source_revision_member_hash: str = Field(min_length=64, max_length=64)
    expected_source_event_refs: tuple[Phase1GSourceEventReference, ...]
    freeze_intent: Phase1GSourceRevisionFreezeIntent
    source_replay_result_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )

    @field_validator(
        "target_request_hash",
        "phase1e_plan_hash",
        "source_operation_projection_hash",
        "requirement_set_hash",
        "source_revision_member_hash",
        "source_replay_result_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_hash(self) -> "Phase1GSourceReplayResult":
        refs = tuple(
            sorted(
                self.expected_source_event_refs,
                key=lambda item: (
                    item.dataset_name,
                    item.source_role,
                    item.partition_key_hash,
                    item.event_revision_no,
                    item.event_content_hash,
                ),
            )
        )
        if len({item.requirement_id for item in refs}) != len(refs):
            raise ValueError(
                "source event refs must have unique requirement identities"
            )
        object.__setattr__(self, "expected_source_event_refs", refs)
        if self.source_revision_member_count != len(self.source_revision_set.members):
            raise ValueError("source replay member count does not match source set")
        member_hash = canonical_json_sha256(
            [member.content_payload() for member in self.source_revision_set.members]
        )
        if self.source_revision_member_hash != member_hash:
            raise ValueError("source replay member hash does not match source set")
        if self.embedded_resolution_receipt != self.replayed_resolution_receipt:
            raise ValueError("source replay receipts are not exactly equal")
        receipt = self.replayed_resolution_receipt
        if (
            self.requirement_set_id != receipt.source_requirement_set_id
            or self.requirement_set_hash != receipt.source_requirement_set_hash
            or self.source_revision_set.source_revision_set_id
            != receipt.source_revision_set_id
            or self.source_revision_set.source_revision_set_hash
            != receipt.source_revision_set_hash
        ):
            raise ValueError("source replay receipt identities do not close")
        freeze = self.freeze_intent
        if (
            freeze.target_request_hash != self.target_request_hash
            or freeze.requirement_set_id != self.requirement_set_id
            or freeze.requirement_set_hash != self.requirement_set_hash
            or freeze.resolution_receipt_id != receipt.source_resolution_receipt_id
            or freeze.resolution_receipt_hash != receipt.source_resolution_receipt_hash
            or freeze.source_revision_set != self.source_revision_set
            or freeze.expected_member_count != self.source_revision_member_count
            or freeze.expected_member_hash != self.source_revision_member_hash
        ):
            raise ValueError(
                "source replay freeze intent does not close to replay result"
            )
        _validate_replay_event_refs(
            refs=refs,
            receipt=receipt,
            source_revision_set=self.source_revision_set,
        )
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"source_replay_result_hash"})
        )
        if (
            self.source_replay_result_hash is not None
            and self.source_replay_result_hash != digest
        ):
            raise ValueError("source_replay_result_hash does not match result")
        object.__setattr__(self, "source_replay_result_hash", digest)
        return self


def _validate_replay_event_refs(
    *,
    refs: tuple[Phase1GSourceEventReference, ...],
    receipt: SourceResolutionReceipt,
    source_revision_set: SourceRevisionSet,
) -> None:
    available = {
        item.requirement_id: item
        for item in receipt.requirement_resolutions
        if item.resolution_status is RequirementResolutionStatus.AVAILABLE
    }
    if set(available) != {item.requirement_id for item in refs}:
        raise ValueError("source replay event refs differ from available resolutions")
    members = {item.member_key: item for item in source_revision_set.members}
    if len(members) != len(source_revision_set.members) or len(refs) != len(members):
        raise ValueError("source replay event refs do not map one-to-one to members")
    for ref in refs:
        resolution = available[ref.requirement_id]
        member = members.get(str(resolution.selected_source_member_key or ""))
        event = member.availability_event if member is not None else None
        if member is None or event is None:
            raise ValueError("source replay event ref has no exact member event")
        expected = (
            resolution.consumer_scope_id,
            member.dataset_name,
            member.source_role,
            member.partition_key_hash,
            event.partition_chain_key,
            member.revision_id,
            event.event_type.value,
            event.event_revision_no,
            event.availability_event_id,
            event.event_content_hash,
            event.input.predecessor_event_hash,
            member.business_min_date,
            member.business_max_date,
            event.input.provider_published_at,
            event.input.first_observed_at,
            event.formal_available_at,
            member.schema_fingerprint,
            member.row_count,
            member.partition_content_hash,
            member.quality_status,
        )
        actual = (
            ref.consumer_scope_id,
            ref.dataset_name,
            ref.source_role,
            ref.partition_key_hash,
            ref.partition_chain_key,
            ref.revision_id,
            ref.event_type,
            ref.event_revision_no,
            ref.availability_event_id,
            ref.event_content_hash,
            ref.predecessor_event_hash,
            ref.business_min_date,
            ref.business_max_date,
            ref.provider_published_at,
            ref.first_observed_at,
            ref.formal_available_at,
            ref.schema_fingerprint,
            ref.row_count,
            ref.partition_content_hash,
            ref.quality_status,
        )
        if actual != expected or (
            resolution.selected_availability_event_hash != event.event_content_hash
        ):
            raise ValueError("source replay event ref differs from member/resolution")


def parse_phase1g_source_operation(
    *,
    phase1e_plan: Phase1EExecutionPlanProjection,
    target_request: Phase1GTargetExecutionRequest,
) -> Phase1GSourceOperationProjection:
    """Parse the exact Phase 1E source operation and close it to the G1 request."""

    try:
        source_operations = [
            item
            for item in phase1e_plan.planned_operations
            if item.operation_type is Phase1EPlannedOperationType.SOURCE_RESOLUTION
        ]
        if len(source_operations) != 1:
            raise ValueError("Phase 1E plan must contain exactly one source operation")
        operation = source_operations[0]
        if (
            operation.operation_disposition
            is not Phase1EOperationDisposition.COMPLETE_REQUEST
            or operation.contract_schema_version
            != PHASE1E_SOURCE_RESOLUTION_OPERATION_SCHEMA_VERSION
            or operation.complete_request_payload is None
            or operation.complete_request_hash is None
        ):
            raise ValueError("Phase 1E source operation is not a complete v1 request")
        payload = canonicalize(operation.complete_request_payload)
        if (
            payload.get("schema_version")
            != PHASE1E_SOURCE_RESOLUTION_OPERATION_SCHEMA_VERSION
        ):
            raise ValueError("source operation payload schema is invalid")
        scope_context = payload.get("scope_context")
        if not isinstance(scope_context, dict):
            raise ValueError("source operation scope_context is required")
        requirement_set = SourceRequirementSet.model_validate(
            payload.get("source_requirement_set")
        )
        receipt = SourceResolutionReceipt.model_validate(
            payload.get("source_resolution_receipt")
        )
        binding = phase1e_plan.evidence_binding
        _require_equal(
            "target plan id", target_request.phase1e_plan_id, phase1e_plan.plan_id
        )
        _require_equal(
            "target plan hash", target_request.phase1e_plan_hash, phase1e_plan.plan_hash
        )
        _require_equal(
            "target source operation hash",
            target_request.source_operation_hash,
            operation.complete_request_hash,
        )
        program_id = phase1e_plan_program_id(phase1e_plan)
        _require_equal("target program", target_request.program_id, program_id)
        _require_equal(
            "target decision date",
            target_request.decision_trade_date,
            phase1e_plan.decision_trade_date,
        )
        _require_equal(
            "target scope id",
            target_request.admission_scope_id,
            binding.admission_scope_id,
        )
        _require_equal(
            "target scope hash",
            target_request.admission_scope_hash,
            binding.admission_scope_hash,
        )
        _require_scope_context(scope_context=scope_context, phase1e_plan=phase1e_plan)
        _require_requirement_closure(
            requirement_set=requirement_set, phase1e_plan=phase1e_plan
        )
        if (
            payload.get("source_requirement_set_id")
            != requirement_set.source_requirement_set_id
            or payload.get("source_requirement_set_hash")
            != requirement_set.source_requirement_set_hash
            or receipt.source_requirement_set_id
            != requirement_set.source_requirement_set_id
            or receipt.source_requirement_set_hash
            != requirement_set.source_requirement_set_hash
            or receipt.requested_source_cutoff
            != requirement_set.requested_source_cutoff
        ):
            raise ValueError("source requirement/receipt identities do not close")
        capture_refs = _capture_source_set_refs(phase1e_plan)
        if receipt.can_create_capture_plan and not capture_refs:
            raise ValueError(
                "capture-ready source operation has no Phase 1E capture plan source ref"
            )
        return Phase1GSourceOperationProjection(
            target_request_hash=str(target_request.request_hash),
            phase1e_plan_id=phase1e_plan.plan_id,
            phase1e_plan_hash=phase1e_plan.plan_hash,
            phase1e_operation_hash=operation.complete_request_hash,
            program_id=program_id,
            decision_trade_date=phase1e_plan.decision_trade_date,
            admission_scope_id=str(binding.admission_scope_id),
            admission_scope_hash=str(binding.admission_scope_hash),
            package_id=binding.package_id,
            manifest_sha256=binding.manifest_sha256,
            alpha_mode=binding.alpha_mode,
            source_operation_contract_version=operation.contract_schema_version,
            requirement_set=requirement_set,
            embedded_receipt=receipt,
            expected_capture_source_sets=capture_refs,
        )
    except Phase1GSourceReplayError:
        raise
    except (TypeError, ValueError) as exc:
        raise Phase1GSourceReplayError(
            REASON_SOURCE_OPERATION_INVALID,
            "Phase 1E source operation failed strict projection",
            context={
                "phase1e_plan_id": phase1e_plan.plan_id,
                "exception_type": type(exc).__name__,
            },
        ) from exc


def replay_phase1g_source_operation(
    *,
    projection: Phase1GSourceOperationProjection,
    availability_events: (
        tuple[SourceAvailabilityEvent, ...] | list[SourceAvailabilityEvent]
    ),
) -> Phase1GSourceReplayResult:
    """Replay one source operation using only explicit immutable events."""

    expected_chains = {
        source_partition_chain_key(
            dataset_name=item.dataset_name,
            source_role=item.source_role,
            partition_key=item.partition_key,
        )
        for item in projection.requirement_set.requirements
    }
    events = tuple(
        sorted(
            availability_events,
            key=lambda item: (
                item.partition_chain_key,
                item.event_revision_no,
                item.event_content_hash,
            ),
        )
    )
    unexpected_chains = sorted(
        {item.partition_chain_key for item in events}.difference(expected_chains)
    )
    if unexpected_chains:
        raise Phase1GSourceReplayError(
            REASON_SOURCE_REPLAY_INPUT_INVALID,
            "source replay received events outside the frozen requirement set",
            context={"unexpected_chain_count": len(unexpected_chains)},
        )
    try:
        replayed = FixtureSourceRevisionResolver().resolve(
            requirement_set=projection.requirement_set,
            availability_events=events,
        )
    except Exception as exc:
        raise Phase1GSourceReplayError(
            REASON_G2_UNEXPECTED_ERROR,
            "authoritative source resolver raised an unexpected exception",
            context={"exception_type": type(exc).__name__},
        ) from exc
    if replayed.receipt.model_dump(
        mode="json"
    ) != projection.embedded_receipt.model_dump(mode="json"):
        raise Phase1GSourceReplayError(
            REASON_SOURCE_REPLAY_MISMATCH,
            "same-cutoff replay receipt differs from the Phase 1E receipt",
            context={
                "embedded_receipt_hash": projection.embedded_receipt.source_resolution_receipt_hash,
                "replayed_receipt_hash": replayed.receipt.source_resolution_receipt_hash,
            },
        )
    if replayed.source_revision_set is None or not replayed.can_create_capture_plan:
        raise Phase1GSourceReplayError(
            REASON_SOURCE_REPLAY_UNAVAILABLE,
            "same-cutoff replay did not produce a capture-ready source revision set",
            context={"readiness": replayed.receipt.readiness.value},
        )
    source_set = replayed.source_revision_set
    for item in projection.expected_capture_source_sets:
        if (
            item.source_revision_set_id != source_set.source_revision_set_id
            or item.source_revision_set_hash != source_set.source_revision_set_hash
        ):
            raise Phase1GSourceReplayError(
                REASON_SOURCE_REPLAY_MISMATCH,
                "Phase 1E capture plan references a different source revision set",
                context={"capture_plan_hash": item.capture_plan_hash},
            )
    refs = _event_refs(
        requirement_set=projection.requirement_set,
        receipt=replayed.receipt,
        source_revision_set=source_set,
    )
    member_hash = canonical_json_sha256(
        [member.content_payload() for member in source_set.members]
    )
    freeze_intent = Phase1GSourceRevisionFreezeIntent(
        target_request_hash=projection.target_request_hash,
        requirement_set_id=str(projection.requirement_set.source_requirement_set_id),
        requirement_set_hash=str(
            projection.requirement_set.source_requirement_set_hash
        ),
        resolution_receipt_id=str(replayed.receipt.source_resolution_receipt_id),
        resolution_receipt_hash=str(replayed.receipt.source_resolution_receipt_hash),
        source_revision_set=source_set,
        expected_member_count=len(source_set.members),
        expected_member_hash=member_hash,
    )
    return Phase1GSourceReplayResult(
        target_request_hash=projection.target_request_hash,
        phase1e_plan_id=projection.phase1e_plan_id,
        phase1e_plan_hash=projection.phase1e_plan_hash,
        source_operation_projection_hash=str(
            projection.source_operation_projection_hash
        ),
        requirement_set_id=str(projection.requirement_set.source_requirement_set_id),
        requirement_set_hash=str(
            projection.requirement_set.source_requirement_set_hash
        ),
        embedded_resolution_receipt=projection.embedded_receipt,
        replayed_resolution_receipt=replayed.receipt,
        source_revision_set=source_set,
        source_revision_member_count=len(source_set.members),
        source_revision_member_hash=member_hash,
        expected_source_event_refs=refs,
        freeze_intent=freeze_intent,
    )


def _require_equal(label: str, left: Any, right: Any) -> None:
    if left != right:
        raise ValueError(f"{label} does not match")


def phase1e_plan_program_id(phase1e_plan: Phase1EExecutionPlanProjection) -> str:
    raw = phase1e_plan.scope_key.get("program_id") if phase1e_plan.scope_key else None
    program_id = str(raw or "").strip()
    if not program_id:
        raise ValueError("Phase 1E scope_key.program_id is required")
    return program_id


def _require_scope_context(
    *, scope_context: dict[str, Any], phase1e_plan: Phase1EExecutionPlanProjection
) -> None:
    binding = phase1e_plan.evidence_binding
    expected = {
        "program_id": phase1e_plan_program_id(phase1e_plan),
        "decision_trade_date": phase1e_plan.decision_trade_date.isoformat(),
        "evidence_binding_hash": binding.evidence_binding_hash,
        "package_id": binding.package_id,
        "manifest_sha256": binding.manifest_sha256,
        "alpha_mode": binding.alpha_mode,
        "admission_scope_id": binding.admission_scope_id,
        "admission_scope_hash": binding.admission_scope_hash,
    }
    for field_name, expected_value in expected.items():
        value = scope_context.get(field_name)
        if field_name == "decision_trade_date":
            value = str(value)
        if value != expected_value:
            raise ValueError(f"scope_context.{field_name} does not match Phase 1E")


def _require_requirement_closure(
    *,
    requirement_set: SourceRequirementSet,
    phase1e_plan: Phase1EExecutionPlanProjection,
) -> None:
    binding = phase1e_plan.evidence_binding
    expected = {
        "program_id": phase1e_plan_program_id(phase1e_plan),
        "decision_as_of_trade_date": phase1e_plan.decision_trade_date,
        "binding_version_id": binding.binding_version_id,
        "package_id": binding.package_id,
        "manifest_sha256": binding.manifest_sha256,
        "alpha_mode": binding.alpha_mode,
        "admission_scope_id": binding.admission_scope_id,
        "admission_scope_hash": binding.admission_scope_hash,
        "handoff_readiness_hash": binding.handoff_readiness_report_hash,
    }
    for field_name, expected_value in expected.items():
        if getattr(requirement_set, field_name) != expected_value:
            raise ValueError(
                f"source requirement set {field_name} does not match Phase 1E"
            )


def _capture_source_set_refs(
    phase1e_plan: Phase1EExecutionPlanProjection,
) -> tuple[Phase1GSourceSetRef, ...]:
    refs: list[Phase1GSourceSetRef] = []
    for operation in phase1e_plan.planned_operations:
        if (
            operation.operation_type
            is not Phase1EPlannedOperationType.OBSERVATION_CAPTURE
        ):
            continue
        payload = (
            operation.request_template_payload or operation.complete_request_payload
        )
        capture_plan = (
            payload.get("capture_plan") if isinstance(payload, dict) else None
        )
        if not isinstance(capture_plan, dict):
            continue
        source_id = str(capture_plan.get("signal_source_revision_set_id") or "").strip()
        source_hash = (
            str(capture_plan.get("signal_source_revision_set_hash") or "")
            .strip()
            .lower()
        )
        plan_hash = str(capture_plan.get("plan_hash") or "").strip().lower()
        if source_id or source_hash or plan_hash:
            refs.append(
                Phase1GSourceSetRef(
                    source_revision_set_id=source_id,
                    source_revision_set_hash=source_hash,
                    capture_plan_hash=plan_hash,
                )
            )
    return tuple(refs)


def _event_refs(
    *,
    requirement_set: SourceRequirementSet,
    receipt: SourceResolutionReceipt,
    source_revision_set: SourceRevisionSet,
) -> tuple[Phase1GSourceEventReference, ...]:
    requirements = {
        str(item.requirement_id): item for item in requirement_set.requirements
    }
    members = {item.member_key: item for item in source_revision_set.members}
    refs: list[Phase1GSourceEventReference] = []
    for resolution in receipt.requirement_resolutions:
        if resolution.resolution_status is not RequirementResolutionStatus.AVAILABLE:
            continue
        requirement = requirements.get(resolution.requirement_id)
        member = members.get(str(resolution.selected_source_member_key))
        if requirement is None or member is None or member.availability_event is None:
            raise Phase1GSourceReplayError(
                REASON_SOURCE_REVISION_CONFLICT,
                "available source resolution has no exact requirement/member/event closure",
                context={"requirement_id": resolution.requirement_id},
            )
        event = member.availability_event
        if event.event_content_hash != resolution.selected_availability_event_hash:
            raise Phase1GSourceReplayError(
                REASON_SOURCE_REVISION_CONFLICT,
                "source resolution event hash differs from the source member event",
                context={"requirement_id": resolution.requirement_id},
            )
        refs.append(_event_ref(requirement=requirement, member=member, event=event))
    if len(refs) != len(source_revision_set.members):
        raise Phase1GSourceReplayError(
            REASON_SOURCE_REVISION_CONFLICT,
            "source revision members do not map one-to-one to available requirements",
            context={
                "available_ref_count": len(refs),
                "member_count": len(source_revision_set.members),
            },
        )
    return tuple(refs)


def _event_ref(
    *,
    requirement: SourceRequirement,
    member: SourceRevisionMemberInput,
    event: SourceAvailabilityEvent,
) -> Phase1GSourceEventReference:
    return Phase1GSourceEventReference(
        requirement_id=str(requirement.requirement_id),
        consumer_scope_id=requirement.consumer_scope_id,
        dataset_name=event.input.dataset_name,
        source_role=event.input.source_role,
        partition_key_hash=event.input.partition_key_hash,
        partition_chain_key=event.partition_chain_key,
        revision_id=event.input.revision_id,
        event_type=event.event_type.value,
        event_revision_no=event.event_revision_no,
        availability_event_id=event.availability_event_id,
        event_content_hash=event.event_content_hash,
        predecessor_event_hash=event.input.predecessor_event_hash,
        business_min_date=member.business_min_date,
        business_max_date=member.business_max_date,
        provider_published_at=event.input.provider_published_at,
        first_observed_at=event.input.first_observed_at,
        formal_available_at=event.formal_available_at,
        schema_fingerprint=event.input.schema_fingerprint,
        row_count=event.input.row_count,
        partition_content_hash=event.input.partition_content_hash,
        quality_status=event.input.quality_status,
    )
