"""Fixture-only immutable observation capture from a durable Phase 1 trace."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.capture_foundation import (
    CapturePlan,
    TraceCaptureGap,
)
from backend.services.advisory_phase1.phase1g_source_replay import (
    _deep_freeze_contract_value,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.stage_trace import (
    StageTraceEnvelope,
    TraceCaptureBinding,
)
from backend.services.advisory_phase1.trace_outbox import (
    ScopeAwareExpectedTraceIdentityV2,
)


REASON_OBSERVATION_PLAN_MISMATCH = "ADVISORY_PHASE1_OBSERVATION_PLAN_MISMATCH"
REASON_OBSERVATION_TRACE_INVALID = "ADVISORY_PHASE1_OBSERVATION_TRACE_INVALID"
REASON_OBSERVATION_CONFLICT = "ADVISORY_PHASE1_OBSERVATION_CONFLICT"
REASON_OBSERVATION_CALENDAR_INVALID = "ADVISORY_PHASE1_OBSERVATION_CALENDAR_INVALID"
REASON_OBSERVATION_COMPONENT_EVIDENCE_MISSING = (
    "ADVISORY_PHASE1_COMPONENT_EVIDENCE_MISSING"
)
REASON_ADVISORY_MODEL_UNAVAILABLE = "ADVISORY_PHASE1_ADVISORY_MODEL_NOT_IMPLEMENTED"

_TRACE_STAGES = (
    "alpha_raw",
    "hmm_adjusted",
    "risk_policy_adjusted",
    "selection_effective",
)
_STAGE_STATUSES = {"COMPLETE", "PARTIAL", "UNAVAILABLE", "NOT_APPLICABLE"}


class _ObservationContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="after")
    def _deep_freeze(self) -> "_ObservationContract":
        for field_name in type(self).model_fields:
            object.__setattr__(
                self, field_name, _deep_freeze_contract_value(getattr(self, field_name))
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


class Phase1GObservationSemanticDraft(_ObservationContract):
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    canonical_signal_header: dict[str, Any]
    semantic_observation_payload: dict[str, Any]
    semantic_observation_key: str = Field(min_length=64, max_length=64)
    stage_semantic_rows: tuple[dict[str, Any], ...] = Field(min_length=5, max_length=5)
    candidate_semantic_rows: tuple[dict[str, Any], ...] = ()
    draft_row_count: int = Field(ge=7)
    draft_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _close_draft(self) -> "Phase1GObservationSemanticDraft":
        if self.semantic_observation_key != canonical_json_sha256(
            self.semantic_observation_payload
        ):
            raise ValueError("semantic_observation_key does not match semantic payload")
        stage_names = tuple(
            str(row.get("stage") or "") for row in self.stage_semantic_rows
        )
        if (
            stage_names != (*_TRACE_STAGES, "advisory_model")
            or len(set(stage_names)) != 5
        ):
            raise ValueError(
                "semantic draft must contain the five ordered observation stages"
            )
        candidate_keys = tuple(
            (str(row.get("stage") or ""), str(row.get("symbol") or ""))
            for row in self.candidate_semantic_rows
        )
        if any(not stage or not symbol for stage, symbol in candidate_keys) or len(
            candidate_keys
        ) != len(set(candidate_keys)):
            raise ValueError(
                "semantic draft candidate identities are missing or duplicated"
            )
        if any(stage not in stage_names for stage, _symbol in candidate_keys):
            raise ValueError("semantic draft candidate references an unknown stage")
        reconstructed_stages = []
        for stage_row in self.stage_semantic_rows:
            stage_name = str(stage_row["stage"])
            reconstructed_stages.append(
                {
                    **dict(stage_row),
                    "candidates": [
                        {
                            key: value
                            for key, value in dict(candidate).items()
                            if key != "stage"
                        }
                        for candidate in self.candidate_semantic_rows
                        if candidate["stage"] == stage_name
                    ],
                }
            )
        if canonicalize(
            self.semantic_observation_payload.get("stages")
        ) != canonicalize(reconstructed_stages):
            raise ValueError(
                "semantic draft rows do not reconstruct the observation payload"
            )
        if self.draft_row_count != 2 + len(self.stage_semantic_rows) + len(
            self.candidate_semantic_rows
        ):
            raise ValueError("draft_row_count does not match complete semantic rows")
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"draft_content_hash"})
        )
        if self.draft_content_hash is not None and self.draft_content_hash != digest:
            raise ValueError("draft_content_hash does not match semantic draft")
        object.__setattr__(self, "draft_content_hash", digest)
        return self


class Phase1GObservationRowBundle(_ObservationContract):
    semantic_observation_key: str = Field(min_length=64, max_length=64)
    canonical_signal_header: dict[str, Any]
    observation_version: dict[str, Any]
    lineage_identity: dict[str, Any]
    lineage_payload: dict[str, Any]
    stage_evidence_rows: tuple[dict[str, Any], ...] = Field(min_length=5, max_length=5)
    candidate_identity_rows: tuple[dict[str, Any], ...] = ()
    candidate_payload_rows: tuple[dict[str, Any], ...] = ()
    bundle_row_count: int = Field(ge=9)
    bundle_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _close_bundle(self) -> "Phase1GObservationRowBundle":
        if len(self.candidate_identity_rows) != len(self.candidate_payload_rows):
            raise ValueError("candidate identity and payload rows must be one-to-one")
        identity_keys = tuple(
            (
                row.get("decision_as_of_trade_date"),
                row.get("stage_evidence_id"),
                row.get("symbol"),
            )
            for row in self.candidate_identity_rows
        )
        payload_keys = tuple(
            (
                row.get("decision_as_of_trade_date"),
                row.get("stage_evidence_id"),
                row.get("symbol"),
            )
            for row in self.candidate_payload_rows
        )
        if identity_keys != payload_keys or len(identity_keys) != len(
            set(identity_keys)
        ):
            raise ValueError(
                "candidate identity and payload keys must be ordered, unique, and equal"
            )
        if self.bundle_row_count != 4 + len(self.stage_evidence_rows) + 2 * len(
            identity_keys
        ):
            raise ValueError("bundle_row_count does not match complete PostgreSQL rows")
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"bundle_content_hash"})
        )
        if self.bundle_content_hash is not None and self.bundle_content_hash != digest:
            raise ValueError(
                "bundle_content_hash does not match observation row bundle"
            )
        object.__setattr__(self, "bundle_content_hash", digest)
        return self


@dataclass(frozen=True)
class ObservationCaptureRecord:
    canonical_signal_id: str
    canonical_header: dict[str, Any]
    observation_version_id: str
    observation_content_hash: str
    observation_payload: dict[str, Any]
    lineage_id: str
    lineage_content_hash: str
    stages: tuple[dict[str, Any], ...]


class TradingCalendarVerifier(Protocol):
    def verify(self, plan: CapturePlan) -> None: ...


def canonical_signal_id_for_plan(plan: CapturePlan) -> str:
    """Derive the stable signal identity from the frozen capture scope."""

    return f"acs_{plan.canonical_signal_scope_hash[:20]}"


class TraceGapRepository(Protocol):
    def record(
        self, *, identity: ScopeAwareExpectedTraceIdentityV2, reason_code: str
    ) -> TraceCaptureGap: ...


@dataclass(frozen=True)
class FrozenTradingCalendarVerifier:
    """Verify adjacency against an explicitly frozen calendar slice."""

    calendar_version: str
    calendar_hash: str
    next_trade_dates: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if len(self.calendar_hash) != 64 or any(
            char not in "0123456789abcdef" for char in self.calendar_hash
        ):
            raise ValueError("frozen trading calendar hash must be lowercase sha256")
        decisions: set[str] = set()
        for decision_value, target_value in self.next_trade_dates:
            decision = date.fromisoformat(decision_value)
            target = date.fromisoformat(target_value)
            if decision_value in decisions or target <= decision:
                raise ValueError(
                    "frozen trading calendar adjacency rows must be unique and forward"
                )
            decisions.add(decision_value)

    def verify(self, plan: CapturePlan) -> None:
        if (
            plan.calendar_version != self.calendar_version
            or plan.calendar_hash != self.calendar_hash
        ):
            raise SourceLedgerError(
                REASON_OBSERVATION_CALENDAR_INVALID,
                "capture plan calendar identity is divergent",
            )
        expected = dict(self.next_trade_dates).get(plan.decision_as_of_trade_date)
        if expected is None or expected != plan.target_trade_date:
            raise SourceLedgerError(
                REASON_OBSERVATION_CALENDAR_INVALID,
                "target trade date is not the frozen calendar's immediate next trading day",
            )


class InMemoryObservationCaptureRepository:
    """Append-only fixture oracle; no mutable Selection/Advisory read path exists."""

    def __init__(
        self,
        *,
        calendar_verifier: TradingCalendarVerifier,
        gap_repository: TraceGapRepository,
    ) -> None:
        self._calendar_verifier = calendar_verifier
        self._gap_repository = gap_repository
        self._headers_by_scope: dict[str, dict[str, Any]] = {}
        self._records_by_semantic_hash: dict[str, ObservationCaptureRecord] = {}
        self._records_by_content_hash: dict[str, ObservationCaptureRecord] = {}
        self._records_by_signal: dict[str, list[ObservationCaptureRecord]] = {}

    def append(
        self,
        *,
        plan: CapturePlan,
        envelope: StageTraceEnvelope,
        binding: TraceCaptureBinding,
    ) -> ObservationCaptureRecord:
        try:
            return self._append_validated(plan=plan, envelope=envelope, binding=binding)
        except SourceLedgerError as exc:
            self._gap_repository.record(
                identity=ScopeAwareExpectedTraceIdentityV2(
                    selection_run_id=plan.selection_run_id,
                    package_id=plan.package_id,
                    manifest_sha256=plan.manifest_sha256,
                    decision_as_of_trade_date=date.fromisoformat(
                        plan.decision_as_of_trade_date
                    ),
                    capture_policy_hash=str(binding.capture_policy.policy_hash),
                    admission_scope_id=binding.admission_scope_id,
                    admission_scope_hash=binding.admission_scope_hash,
                ),
                reason_code=exc.reason_code,
            )
            raise

    def _append_validated(
        self,
        *,
        plan: CapturePlan,
        envelope: StageTraceEnvelope,
        binding: TraceCaptureBinding,
    ) -> ObservationCaptureRecord:
        self._calendar_verifier.verify(plan)
        draft = build_observation_semantic_draft(
            plan=plan, envelope=envelope, binding=binding
        )
        header = dict(draft.canonical_signal_header)
        canonical_signal_id = draft.canonical_signal_id
        existing_header = self._headers_by_scope.get(plan.canonical_signal_scope_hash)
        if existing_header is not None and existing_header != header:
            raise SourceLedgerError(
                REASON_OBSERVATION_CONFLICT,
                "canonical signal scope has conflicting stable header",
            )
        existing = self._records_by_semantic_hash.get(draft.semantic_observation_key)
        if existing is not None:
            return existing
        prior = self._records_by_signal.get(canonical_signal_id, [])
        bundle = materialize_observation_row_bundle(
            draft=draft,
            observation_revision_no=len(prior) + 1,
            supersedes_observation_version_id=(
                prior[-1].observation_version_id if prior else None
            ),
            created_by_capture_batch_id=binding.capture_batch_id,
        )
        stages = _stages_from_draft(draft)
        record = ObservationCaptureRecord(
            canonical_signal_id=canonical_signal_id,
            canonical_header=header,
            observation_version_id=str(
                bundle.observation_version["observation_version_id"]
            ),
            observation_content_hash=str(
                bundle.observation_version["observation_content_hash"]
            ),
            observation_payload=canonicalize(
                {
                    **dict(draft.semantic_observation_payload),
                    "observation_revision_no": bundle.observation_version[
                        "observation_revision_no"
                    ],
                    "supersedes_observation_version_id": bundle.observation_version[
                        "supersedes_observation_version_id"
                    ],
                }
            ),
            lineage_id=str(bundle.lineage_identity["lineage_id"]),
            lineage_content_hash=str(bundle.lineage_identity["lineage_content_hash"]),
            stages=tuple(stages),
        )
        self._headers_by_scope[plan.canonical_signal_scope_hash] = header
        self._records_by_semantic_hash[draft.semantic_observation_key] = record
        self._records_by_content_hash[record.observation_content_hash] = record
        self._records_by_signal.setdefault(canonical_signal_id, []).append(record)
        return record


def build_observation_semantic_draft(
    *,
    plan: CapturePlan,
    envelope: StageTraceEnvelope,
    binding: TraceCaptureBinding,
) -> Phase1GObservationSemanticDraft:
    """Build the complete revision-independent observation semantics."""

    _validate_plan_trace(plan=plan, envelope=envelope, binding=binding)
    header = _canonical_header(plan)
    canonical_signal_id = canonical_signal_id_for_plan(plan)
    stages = _capture_stages(envelope, alpha_mode=plan.alpha_mode)
    stage_bundle_hash = canonical_json_sha256(
        [stage["content_hash"] for stage in stages]
    )
    observation_status = (
        "COMPLETE"
        if all(
            stage["capability_status"] in {"FULL", "NOT_APPLICABLE"}
            for stage in stages
            if stage["stage"] != "advisory_model"
        )
        else "PARTIAL"
    )
    semantic_payload = canonicalize(
        {
            "schema_version": "advisory_signal_observation_version_v1",
            "canonical_signal_id": canonical_signal_id,
            "plan": plan.model_dump(mode="json"),
            "trace_outbox_id": envelope.trace_outbox_id,
            "trace_content_hash": envelope.trace_content_hash,
            "stage_evidence_bundle_hash": stage_bundle_hash,
            "observation_status": observation_status,
            "stages": stages,
        }
    )
    stage_rows = tuple(
        {key: value for key, value in stage.items() if key != "candidates"}
        for stage in stages
    )
    candidate_rows = tuple(
        {"stage": stage["stage"], **candidate}
        for stage in stages
        for candidate in stage["candidates"]
    )
    return Phase1GObservationSemanticDraft(
        canonical_signal_id=canonical_signal_id,
        canonical_signal_header=header,
        semantic_observation_payload=semantic_payload,
        semantic_observation_key=canonical_json_sha256(semantic_payload),
        stage_semantic_rows=stage_rows,
        candidate_semantic_rows=candidate_rows,
        draft_row_count=2 + len(stage_rows) + len(candidate_rows),
    )


def materialize_observation_row_bundle(
    *,
    draft: Phase1GObservationSemanticDraft,
    observation_revision_no: int,
    supersedes_observation_version_id: str | None,
    created_by_capture_batch_id: str,
) -> Phase1GObservationRowBundle:
    """Materialize revision-scoped identities after the signal chain is locked."""

    if observation_revision_no < 1:
        raise ValueError("observation_revision_no must be positive")
    if (observation_revision_no == 1) != (supersedes_observation_version_id is None):
        raise ValueError("observation predecessor does not match revision number")
    if not created_by_capture_batch_id:
        raise ValueError("created_by_capture_batch_id is required")
    semantic_payload = dict(draft.semantic_observation_payload)
    plan = dict(semantic_payload["plan"])
    observation_payload = canonicalize(
        {
            **semantic_payload,
            "observation_revision_no": observation_revision_no,
            "supersedes_observation_version_id": supersedes_observation_version_id,
        }
    )
    observation_content_hash = canonical_json_sha256(observation_payload)
    observation_version_id = f"osv_{observation_content_hash[:20]}"
    observation_version = {
        "observation_version_id": observation_version_id,
        "canonical_signal_id": draft.canonical_signal_id,
        "observation_schema_version": semantic_payload["schema_version"],
        "observation_revision_no": observation_revision_no,
        "supersedes_observation_version_id": supersedes_observation_version_id,
        "signal_source_revision_set_id": plan["signal_source_revision_set_id"],
        "signal_source_revision_set_hash": plan["signal_source_revision_set_hash"],
        "phase0a_signal_context_hash": plan["phase0a_signal_context_hash"],
        "evidence_bundle_hash": plan["evidence_bundle_hash"],
        "stage_evidence_bundle_hash": semantic_payload["stage_evidence_bundle_hash"],
        "selection_evidence_id": plan["selection_evidence_id"],
        "selection_evidence_hash": plan["selection_evidence_hash"],
        "selection_run_id": plan["selection_run_id"],
        "selection_run_content_hash": plan["selection_run_content_hash"],
        "selection_score_artifact_id": plan["selection_score_artifact_id"],
        "selection_score_artifact_hash": plan["selection_score_artifact_hash"],
        "runtime_profile_version_id": plan["runtime_profile_version_id"],
        "runtime_profile_version_hash": plan["runtime_profile_version_hash"],
        "hmm_snapshot_id": plan["hmm_snapshot_id"],
        "hmm_snapshot_hash": plan["hmm_snapshot_hash"],
        "hmm_snapshot_status": plan["hmm_snapshot_status"],
        "risk_policy_hash": plan["risk_policy_hash"],
        "universe_policy_hash": plan["universe_policy_hash"],
        "symbol_normalization_policy_hash": plan["symbol_normalization_policy_hash"],
        "valid_no_candidate": plan["valid_no_candidate"],
        "observation_status": semantic_payload["observation_status"],
        "evidence_available_at": datetime.fromisoformat(
            str(plan["evidence_available_at"]).replace("Z", "+00:00")
        ),
        "observation_content_hash": observation_content_hash,
        "reason_codes": sorted(
            {
                code
                for stage in draft.stage_semantic_rows
                for code in stage.get("reason_codes", ())
            }
        ),
        "created_by_capture_batch_id": created_by_capture_batch_id,
    }
    lineage_payload_for_hash = canonicalize(
        {
            "canonical_signal_id": draft.canonical_signal_id,
            "observation_version_id": observation_version_id,
            "phase0a_audit_id": plan["phase0a_audit_id"],
            "phase0a_audit_manifest_hash": plan["phase0a_audit_manifest_hash"],
            "handoff_readiness_hash": plan["handoff_readiness_hash"],
            "admission_scope_id": plan["admission_scope_id"],
            "admission_scope_hash": plan["admission_scope_hash"],
            "audit_target_id": plan["audit_target_id"],
            "target_scope_hash": plan["target_scope_hash"],
            "capability": plan["capability"],
            "stable_signal_semantics_hash": plan["stable_signal_semantics_hash"],
            "canonical_signal_scope_hash": plan["canonical_signal_scope_hash"],
            "phase0a_signal_context_hash": plan["phase0a_signal_context_hash"],
            "oos_interval_id": plan["oos_interval_id"],
            "oos_interval_hash": plan["oos_interval_hash"],
            "evidence_scope": plan["evidence_scope"],
            "signal_evidence_level": plan["signal_evidence_level"],
            "effective_cutoff_date": plan["effective_cutoff_date"],
            "program_id": plan["program_id"],
            "binding_version_id": plan["binding_version_id"],
            "lineage_source_type": plan["lineage_source_type"],
            "source_run_id": plan["source_run_id"],
            "review_run_id": plan["review_run_id"],
            "list_version_id": plan["list_version_id"],
        }
    )
    lineage_content_hash = canonical_json_sha256(lineage_payload_for_hash)
    lineage_id = f"osl_{lineage_content_hash[:20]}"
    decision_date = plan["decision_as_of_trade_date"]
    lineage_identity = {
        "lineage_id": lineage_id,
        "decision_as_of_trade_date": decision_date,
        "observation_version_id": observation_version_id,
        "phase0a_audit_id": plan["phase0a_audit_id"],
        "admission_scope_id": plan["admission_scope_id"],
        "program_id": plan["program_id"],
        "binding_version_id": plan["binding_version_id"],
        "lineage_source_type": plan["lineage_source_type"],
        "source_run_id": plan["source_run_id"],
        "lineage_content_hash": lineage_content_hash,
    }
    lineage_payload = {
        "decision_as_of_trade_date": decision_date,
        "lineage_id": lineage_id,
        **{
            key: value
            for key, value in lineage_payload_for_hash.items()
            if key
            not in {
                "observation_version_id",
                "phase0a_audit_id",
                "admission_scope_id",
                "program_id",
                "binding_version_id",
                "lineage_source_type",
                "source_run_id",
            }
        },
    }
    stage_evidence_rows = []
    stage_ids: dict[str, str] = {}
    for semantic_stage in draft.stage_semantic_rows:
        stage = str(semantic_stage["stage"])
        stage_evidence_id = f"ase_{canonical_json_sha256({'observation_version_id': observation_version_id, 'stage': stage})[:20]}"
        stage_ids[stage] = stage_evidence_id
        stage_evidence_rows.append(
            {
                "stage_evidence_id": stage_evidence_id,
                "observation_version_id": observation_version_id,
                **dict(semantic_stage),
            }
        )
    candidate_identity_rows = []
    candidate_payload_rows = []
    for semantic_candidate in draft.candidate_semantic_rows:
        candidate = dict(semantic_candidate)
        stage = str(candidate.pop("stage"))
        identity = {
            "stage_evidence_id": stage_ids[stage],
            "symbol": candidate["symbol"],
            "decision_as_of_trade_date": decision_date,
        }
        candidate_identity_rows.append(identity)
        component_evidence_json = candidate.pop("component_evidence")
        candidate_payload_rows.append(
            {
                "decision_as_of_trade_date": decision_date,
                "stage_evidence_id": stage_ids[stage],
                "symbol": candidate.pop("symbol"),
                "component_evidence_json": component_evidence_json,
                **candidate,
            }
        )
    ordered_pairs = sorted(
        zip(candidate_identity_rows, candidate_payload_rows, strict=True),
        key=lambda pair: (str(pair[0]["stage_evidence_id"]), str(pair[0]["symbol"])),
    )
    candidate_identity_rows = [pair[0] for pair in ordered_pairs]
    candidate_payload_rows = [pair[1] for pair in ordered_pairs]
    return Phase1GObservationRowBundle(
        semantic_observation_key=draft.semantic_observation_key,
        canonical_signal_header=draft.canonical_signal_header,
        observation_version=observation_version,
        lineage_identity=lineage_identity,
        lineage_payload=lineage_payload,
        stage_evidence_rows=tuple(stage_evidence_rows),
        candidate_identity_rows=tuple(candidate_identity_rows),
        candidate_payload_rows=tuple(candidate_payload_rows),
        bundle_row_count=4
        + len(stage_evidence_rows)
        + 2 * len(candidate_identity_rows),
    )


def _stages_from_draft(draft: Phase1GObservationSemanticDraft) -> list[dict[str, Any]]:
    stages = []
    for stage_row in draft.stage_semantic_rows:
        stage_name = str(stage_row["stage"])
        stages.append(
            {
                **dict(stage_row),
                "candidates": [
                    {
                        key: value
                        for key, value in dict(candidate).items()
                        if key != "stage"
                    }
                    for candidate in draft.candidate_semantic_rows
                    if candidate["stage"] == stage_name
                ],
            }
        )
    return stages


def expected_evidence_bundle_hash(*, plan: CapturePlan, trace_content_hash: str) -> str:
    """Build the legacy combined-evidence digest used by pre-Phase 1E callers.

    Phase 1E freezes ``CapturePlan.evidence_bundle_hash`` as the authoritative
    Phase 0A handoff bundle hash before a G4 batch identity exists.  New G4
    observation identity closes the trace independently through
    ``trace_content_hash`` and ``stage_evidence_bundle_hash``.  Keeping this
    helper preserves old artifact readers without reinterpreting the frozen
    Phase 1E field during observation capture.
    """

    return canonical_json_sha256(
        {
            "selection_evidence_id": plan.selection_evidence_id,
            "selection_evidence_hash": plan.selection_evidence_hash,
            "selection_run_id": plan.selection_run_id,
            "selection_run_content_hash": plan.selection_run_content_hash,
            "selection_score_artifact_id": plan.selection_score_artifact_id,
            "selection_score_artifact_hash": plan.selection_score_artifact_hash,
            "runtime_profile_version_id": plan.runtime_profile_version_id,
            "runtime_profile_version_hash": plan.runtime_profile_version_hash,
            "hmm_snapshot_id": plan.hmm_snapshot_id,
            "hmm_snapshot_hash": plan.hmm_snapshot_hash,
            "hmm_snapshot_status": plan.hmm_snapshot_status,
            "risk_policy_hash": plan.risk_policy_hash,
            "universe_policy_hash": plan.universe_policy_hash,
            "trace_content_hash": trace_content_hash,
        }
    )


def _validate_plan_trace(
    *, plan: CapturePlan, envelope: StageTraceEnvelope, binding: TraceCaptureBinding
) -> None:
    identity = envelope.trace_content.get("selection_identity")
    embedded_binding = envelope.trace_content.get("trace_capture_binding")
    if not isinstance(identity, Mapping) or canonicalize(
        embedded_binding
    ) != canonicalize(binding.model_dump(mode="json")):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID,
            "trace envelope identity or binding is invalid",
        )
    if (
        str(identity.get("selection_run_id") or "") != plan.selection_run_id
        or str(identity.get("package_id") or "") != plan.package_id
        or str(identity.get("manifest_sha256") or "") != plan.manifest_sha256
        or str(identity.get("decision_as_of_trade_date") or "")
        != plan.decision_as_of_trade_date
        or binding.handoff_readiness_hash != plan.handoff_readiness_hash
        or binding.admission_scope_id != plan.admission_scope_id
        or binding.admission_scope_hash != plan.admission_scope_hash
    ):
        raise SourceLedgerError(
            REASON_OBSERVATION_PLAN_MISMATCH,
            "frozen capture plan does not match trace identity",
        )


def _canonical_header(plan: CapturePlan) -> dict[str, Any]:
    return canonicalize(
        {
            "canonical_signal_id": canonical_signal_id_for_plan(plan),
            "signal_schema_version": "advisory_canonical_signal_v1",
            "stable_signal_semantics_hash": plan.stable_signal_semantics_hash,
            "canonical_signal_scope_hash": plan.canonical_signal_scope_hash,
            "decision_as_of_trade_date": plan.decision_as_of_trade_date,
            "selection_as_of_trade_date": plan.selection_as_of_trade_date,
            "target_trade_date": plan.target_trade_date,
            "decision_cutoff_ts": plan.decision_cutoff_ts,
            "package_id": plan.package_id,
            "manifest_sha256": plan.manifest_sha256,
            "alpha_mode": plan.alpha_mode,
            "selection_runtime_semantics_hash": plan.selection_runtime_semantics_hash,
            "package_effective_config_hash": plan.package_effective_config_hash,
            "calendar_version": plan.calendar_version,
            "calendar_hash": plan.calendar_hash,
        }
    )


def _capture_stages(
    envelope: StageTraceEnvelope, *, alpha_mode: str
) -> list[dict[str, Any]]:
    raw_stages = envelope.trace_content.get("stage_trace")
    if not isinstance(raw_stages, list):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace does not contain stage rows"
        )
    indexed: dict[str, Mapping[str, Any]] = {}
    for raw in raw_stages:
        if not isinstance(raw, Mapping):
            raise SourceLedgerError(
                REASON_OBSERVATION_TRACE_INVALID, "trace stage is not an object"
            )
        stage_name = str(raw.get("stage") or "")
        if stage_name in indexed or stage_name not in _TRACE_STAGES:
            raise SourceLedgerError(
                REASON_OBSERVATION_TRACE_INVALID,
                "trace stages are duplicated or unsupported",
            )
        indexed[stage_name] = raw
    if tuple(sorted(indexed)) != tuple(sorted(_TRACE_STAGES)):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID,
            "trace does not provide every required stage",
        )
    captured = [
        _capture_one_stage(
            stage_name=stage_name, raw=indexed[stage_name], alpha_mode=alpha_mode
        )
        for stage_name in _TRACE_STAGES
    ]
    captured.append(
        {
            "stage": "advisory_model",
            "capability_status": "UNAVAILABLE",
            "input_count": 0,
            "output_count": 0,
            "excluded_count": 0,
            "observed_max_rank": None,
            "source_artifact_id": None,
            "source_artifact_hash": None,
            "semantic_hash": canonical_json_sha256(
                {"state": REASON_ADVISORY_MODEL_UNAVAILABLE}
            ),
            "score_direction": "NOT_APPLICABLE",
            "tie_break_policy_id": "not_applicable",
            "tie_break_policy_hash": canonical_json_sha256({"state": "NOT_APPLICABLE"}),
            "reason_codes": [REASON_ADVISORY_MODEL_UNAVAILABLE],
            "candidates": [],
            "content_hash": canonical_json_sha256(
                {"stage": "advisory_model", "reason": REASON_ADVISORY_MODEL_UNAVAILABLE}
            ),
        }
    )
    return captured


def _capture_one_stage(
    *, stage_name: str, raw: Mapping[str, Any], alpha_mode: str
) -> dict[str, Any]:
    receipt = raw.get("receipt")
    if (
        not isinstance(receipt, Mapping)
        or str(receipt.get("stage") or "") != stage_name
    ):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace stage receipt is invalid"
        )
    status = str(receipt.get("status") or "")
    if status not in _STAGE_STATUSES:
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace stage status is invalid"
        )
    try:
        input_count = int(receipt["input_count"])
        output_count = int(receipt["output_count"])
        excluded_count = int(receipt["excluded_count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace stage counts are invalid"
        ) from exc
    if min(input_count, output_count, excluded_count) < 0:
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace stage counts must be non-negative"
        )
    raw_candidates = receipt.get("candidates") or []
    raw_exclusions = receipt.get("exclusions") or []
    if not isinstance(raw_candidates, list) or not isinstance(raw_exclusions, list):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace stage candidates are invalid"
        )
    if status == "COMPLETE" and (
        output_count != len(raw_candidates)
        or input_count != output_count + excluded_count
    ):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID,
            "complete trace stage counts do not reconcile",
        )
    component_rows = raw.get("candidate_component_evidence") or {}
    if not isinstance(component_rows, Mapping):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace component evidence is invalid"
        )
    candidates = [
        _capture_candidate(
            raw=item,
            membership_status="INCLUDED",
            component=component_rows.get(str(item.get("symbol") or "")),
            alpha_mode=alpha_mode,
        )
        for item in raw_candidates
        if isinstance(item, Mapping)
    ]
    if len(candidates) != len(raw_candidates):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace candidate is not an object"
        )
    candidates.extend(
        _capture_candidate(
            raw=item,
            membership_status="EXCLUDED",
            component=component_rows.get(str(item.get("symbol") or "")),
            alpha_mode=alpha_mode,
        )
        for item in raw_exclusions
        if isinstance(item, Mapping)
    )
    if len(candidates) != len(raw_candidates) + len(raw_exclusions):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace exclusion is not an object"
        )
    if len({item["symbol"] for item in candidates}) != len(candidates):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace stage contains duplicate symbols"
        )
    included_ranks = [
        item["rank"] for item in candidates if item["membership_status"] == "INCLUDED"
    ]
    if included_ranks and sorted(included_ranks) != list(
        range(1, len(included_ranks) + 1)
    ):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "included trace ranks are not contiguous"
        )
    capability_status = {
        "COMPLETE": "FULL",
        "PARTIAL": "PARTIAL",
        "UNAVAILABLE": "UNAVAILABLE",
        "NOT_APPLICABLE": "NOT_APPLICABLE",
    }[status]
    if alpha_mode == "multi_alpha" and any(
        item["component_capability"] != "FULL" for item in candidates
    ):
        capability_status = (
            "PARTIAL" if capability_status == "FULL" else capability_status
        )
    semantic_payload = receipt.get("semantic_payload") or {}
    if not isinstance(semantic_payload, Mapping):
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace stage semantic payload is invalid"
        )
    candidate_hashes = [
        item["candidate_content_hash"]
        for item in sorted(candidates, key=lambda item: item["symbol"])
    ]
    content_payload = {
        "stage": stage_name,
        "capability_status": capability_status,
        "input_count": input_count,
        "output_count": output_count,
        "excluded_count": excluded_count,
        "candidates": candidate_hashes,
        "semantic_payload": canonicalize(semantic_payload),
        "reason_codes": sorted(str(item) for item in receipt.get("reason_codes") or []),
    }
    return {
        "stage": stage_name,
        "capability_status": capability_status,
        "input_count": input_count,
        "output_count": output_count,
        "excluded_count": excluded_count,
        "observed_max_rank": max(included_ranks) if included_ranks else None,
        "source_artifact_id": (
            semantic_payload.get("artifact_id")
            if isinstance(semantic_payload, Mapping)
            else None
        ),
        "source_artifact_hash": (
            semantic_payload.get("artifact_sha256")
            if isinstance(semantic_payload, Mapping)
            else None
        ),
        "semantic_hash": canonical_json_sha256(canonicalize(semantic_payload)),
        "score_direction": "DESCENDING_SCORE_ASCENDING_SYMBOL_V1",
        "tie_break_policy_id": "selection_candidate_rank_v1",
        "tie_break_policy_hash": canonical_json_sha256(
            {"policy": "selection_candidate_rank_v1"}
        ),
        "reason_codes": content_payload["reason_codes"],
        "candidates": sorted(candidates, key=lambda item: item["symbol"]),
        "content_hash": canonical_json_sha256(content_payload),
    }


def _capture_candidate(
    *, raw: Mapping[str, Any], membership_status: str, component: Any, alpha_mode: str
) -> dict[str, Any]:
    symbol = str(raw.get("symbol") or "").strip()
    if not symbol:
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace candidate symbol is missing"
        )
    try:
        rank = int(raw["rank"])
        score = Decimal(str(raw["score"]))
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace candidate rank or score is invalid"
        ) from exc
    if rank < 1 or not score.is_finite():
        raise SourceLedgerError(
            REASON_OBSERVATION_TRACE_INVALID, "trace candidate rank or score is invalid"
        )
    component_capability = (
        "NOT_APPLICABLE" if alpha_mode == "single_alpha" else "UNAVAILABLE"
    )
    component_schema_version = None
    component_evidence = None
    component_hash = None
    component_reason_codes = (
        [REASON_OBSERVATION_COMPONENT_EVIDENCE_MISSING]
        if alpha_mode == "multi_alpha"
        else []
    )
    if component is not None:
        if not isinstance(component, Mapping):
            raise SourceLedgerError(
                REASON_OBSERVATION_TRACE_INVALID, "trace component evidence is invalid"
            )
        component_capability = str(component.get("capability") or "")
        if component_capability not in {
            "FULL",
            "PARTIAL",
            "UNAVAILABLE",
            "NOT_APPLICABLE",
        }:
            raise SourceLedgerError(
                REASON_OBSERVATION_TRACE_INVALID,
                "trace component capability is invalid",
            )
        component_schema_version = component.get("schema_version")
        component_evidence = component.get("component_evidence")
        component_hash = component.get("component_evidence_hash")
        raw_reason_codes = component.get("reason_codes") or []
        if not isinstance(raw_reason_codes, list):
            raise SourceLedgerError(
                REASON_OBSERVATION_TRACE_INVALID,
                "trace component reason codes are invalid",
            )
        component_reason_codes = sorted(str(item) for item in raw_reason_codes)
        if alpha_mode == "multi_alpha" and component_capability == "NOT_APPLICABLE":
            raise SourceLedgerError(
                REASON_OBSERVATION_TRACE_INVALID,
                "multi-alpha component evidence cannot be not applicable",
            )
        if component_capability == "FULL":
            if (
                component_schema_version != "multi_alpha_component_evidence_v1"
                or not isinstance(component_evidence, Mapping)
                or not isinstance(component_hash, str)
                or canonical_json_sha256(canonicalize(component_evidence))
                != component_hash
                or component_reason_codes
            ):
                raise SourceLedgerError(
                    REASON_OBSERVATION_TRACE_INVALID,
                    "trace component evidence hash is invalid",
                )
        elif (
            component_schema_version is not None
            or component_evidence is not None
            or component_hash is not None
        ):
            raise SourceLedgerError(
                REASON_OBSERVATION_TRACE_INVALID,
                "non-full trace component evidence cannot carry a payload",
            )
        elif alpha_mode == "multi_alpha" and not component_reason_codes:
            raise SourceLedgerError(
                REASON_OBSERVATION_TRACE_INVALID,
                "degraded multi-alpha component evidence requires reason codes",
            )
    included = membership_status == "INCLUDED"
    candidate_payload = {
        "symbol": symbol,
        "membership_status": membership_status,
        "rank": rank if included else None,
        "score_decimal": format(score.normalize(), "f") if included else None,
        "input_rank": None if included else rank,
        "input_score_decimal": None if included else format(score.normalize(), "f"),
        "exclusion_reason_code": None if included else str(raw.get("reason") or ""),
        "component_capability": component_capability,
        "component_evidence_schema_version": component_schema_version,
        "component_evidence": (
            canonicalize(component_evidence) if component_evidence is not None else None
        ),
        "component_evidence_hash": component_hash,
        "component_reason_codes": component_reason_codes,
    }
    return {
        **candidate_payload,
        "candidate_content_hash": canonical_json_sha256(candidate_payload),
    }
