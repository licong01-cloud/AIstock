"""Pure Phase 1G G2 historical DSE/artifact/manifest trace projection."""

from __future__ import annotations

from datetime import date, datetime
import json
import math
from typing import Any, Literal, Mapping, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.evidence_projection import (
    ProjectedHistoricalEvidenceV2Strict,
    canonical_evidence_json_sha256,
    parse_projected_historical_evidence_v2_strict,
    projected_manifest_json_sha256,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.phase1g_phase1e_projection import (
    Phase1EExecutionPlanProjection,
)
from backend.services.advisory_phase1.phase1g_source_replay import (
    Phase1GSourceOperationProjection,
    Phase1GSourceReplayResult,
    Phase1GSourceRevisionFreezeIntent,
    _deep_freeze_contract_value,
    phase1e_plan_program_id,
)
from backend.services.advisory_phase1.stage_trace import (
    ComponentCapability,
    StageTraceEnvelope,
    TraceCaptureContext,
    build_component_evidence,
    build_stage_trace_envelope,
)


DSE_PROJECTION_SCHEMA_VERSION = "advisory_phase1g_dse_projection_v1"
ARTIFACT_PROJECTION_SCHEMA_VERSION = "advisory_phase1g_selection_artifact_projection_v1"
MANIFEST_PROJECTION_SCHEMA_VERSION = "advisory_phase1g_package_manifest_projection_v1"
STAGE_INPUT_PROJECTION_SCHEMA_VERSION = (
    "advisory_phase1g_stage_trace_builder_input_projection_v1"
)
HISTORICAL_TRACE_PROJECTION_SCHEMA_VERSION = (
    "advisory_phase1g_historical_trace_projection_v1"
)
TARGET_PROJECTION_SNAPSHOT_SCHEMA_VERSION = (
    "advisory_phase1g_target_projection_snapshot_v1"
)

REASON_DSE_NOT_FOUND = "ADVISORY_PHASE1G_DSE_NOT_FOUND"
REASON_DSE_INVALID = "ADVISORY_PHASE1G_DSE_INVALID"
REASON_ARTIFACT_NOT_FOUND = "ADVISORY_PHASE1G_SELECTION_ARTIFACT_NOT_FOUND"
REASON_ARTIFACT_INVALID = "ADVISORY_PHASE1G_SELECTION_ARTIFACT_INVALID"
REASON_PACKAGE_NOT_FOUND = "ADVISORY_PHASE1G_PACKAGE_MANIFEST_NOT_FOUND"
REASON_PACKAGE_INVALID = "ADVISORY_PHASE1G_PACKAGE_MANIFEST_INVALID"
REASON_TRACE_MISMATCH = "ADVISORY_PHASE1G_HISTORICAL_TRACE_MISMATCH"
REASON_NO_CANDIDATE_INVALID = "ADVISORY_PHASE1G_VALID_NO_CANDIDATE_INVALID"

_STAGE_NAMES = (
    "alpha_raw",
    "hmm_adjusted",
    "risk_policy_adjusted",
    "selection_effective",
)


class Phase1GHistoricalTraceError(RuntimeError):
    """Typed fail-closed projection error with redacted context."""

    def __init__(
        self, reason_code: str, detail: str, *, context: dict[str, Any] | None = None
    ) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.context = canonicalize(context or {})


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


class Phase1GStageReceiptProjection(_StrictContract):
    stage: str
    status: str
    input_count: int = Field(ge=0)
    output_count: int = Field(ge=0)
    excluded_count: int = Field(ge=0)
    candidates: tuple[dict[str, Any], ...] = ()
    exclusions: tuple[dict[str, Any], ...] = ()
    semantic_payload: dict[str, Any] = Field(default_factory=dict)
    reason_codes: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _close_counts(self) -> "Phase1GStageReceiptProjection":
        if self.status == "COMPLETE" and (
            self.output_count != len(self.candidates)
            or self.excluded_count != len(self.exclusions)
            or self.input_count != self.output_count + self.excluded_count
        ):
            raise ValueError(
                "complete stage counts do not match candidate/exclusion rows"
            )
        if self.status == "NOT_APPLICABLE" and (
            self.output_count
            or self.excluded_count
            or self.candidates
            or self.exclusions
        ):
            raise ValueError("not-applicable stage cannot contain output or exclusions")
        rows = (*self.candidates, *self.exclusions)
        symbols: list[str] = []
        for row in rows:
            symbol = str(row.get("symbol") or "").strip()
            rank = row.get("rank")
            score = row.get("score")
            if (
                not symbol
                or not isinstance(rank, int)
                or isinstance(rank, bool)
                or rank < 1
                or not isinstance(score, int | float)
                or isinstance(score, bool)
                or not math.isfinite(float(score))
            ):
                raise ValueError(
                    "stage rows require canonical symbol/rank/score fields"
                )
            symbols.append(symbol)
        if len(symbols) != len(set(symbols)):
            raise ValueError("stage candidate and exclusion symbols must be unique")
        return self


class Phase1GDseProjection(_StrictContract):
    schema_version: Literal[DSE_PROJECTION_SCHEMA_VERSION] = (
        DSE_PROJECTION_SCHEMA_VERSION
    )
    evidence_id: str
    artifact_hash: str
    target_trade_date: date
    cutoff_date: date
    package_id: str
    manifest_sha256: str
    runtime_profile_version_id: str
    runtime_profile_hash: str
    source_type: str
    data_source: str
    candidate_count: int = Field(ge=0)
    excluded_count: int = Field(ge=0)
    evidence: ProjectedHistoricalEvidenceV2Strict
    dse_projection_hash: str | None = None

    @field_validator(
        "artifact_hash",
        "manifest_sha256",
        "runtime_profile_hash",
        "dse_projection_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_hash(self) -> "Phase1GDseProjection":
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"dse_projection_hash"})
        )
        if self.dse_projection_hash is not None and self.dse_projection_hash != digest:
            raise ValueError("dse_projection_hash does not match projection")
        object.__setattr__(self, "dse_projection_hash", digest)
        return self


class Phase1GSelectionArtifactProjection(_StrictContract):
    schema_version: Literal[ARTIFACT_PROJECTION_SCHEMA_VERSION] = (
        ARTIFACT_PROJECTION_SCHEMA_VERSION
    )
    artifact_id: str
    package_id: str
    manifest_sha256: str
    trade_date: date
    data_source: str
    runtime_config_hash: str
    scores_json: list[dict[str, Any]]
    artifact_sha256: str
    score_count: int = Field(ge=0)
    universe_count: int = Field(ge=0)
    top_score_symbol: str | None
    status: Literal["SUCCEEDED"]
    metadata: dict[str, Any]
    artifact_contract_version: Literal["selection_score_artifact_v2"]
    artifact_payload_sha256: str
    artifact_input_context_hash: str
    source_revision_set_hash: str
    asset_closure_hash: str
    created_at: datetime | None = None
    artifact_projection_hash: str | None = None

    @field_validator(
        "manifest_sha256",
        "runtime_config_hash",
        "artifact_sha256",
        "artifact_payload_sha256",
        "artifact_input_context_hash",
        "source_revision_set_hash",
        "asset_closure_hash",
        "artifact_projection_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @property
    def candidate_outcome(self) -> str:
        return str(self.metadata.get("candidate_outcome") or "")

    def canonical_v2_header(self, *, score_hash: str | None = None) -> dict[str, Any]:
        return {
            "schema_version": self.artifact_contract_version,
            "package_id": self.package_id,
            "manifest_sha256": self.manifest_sha256,
            "trade_date": self.trade_date,
            "data_source": self.data_source,
            "runtime_config_hash": self.runtime_config_hash,
            "artifact_sha256": score_hash or self.artifact_sha256,
            "score_count": self.score_count,
            "universe_count": self.universe_count,
            "top_score_symbol": self.top_score_symbol,
            "status": self.status,
            "authority_scope": self.metadata.get("authority_scope"),
            "candidate_outcome": self.metadata.get("candidate_outcome"),
            "artifact_input_context_hash": self.artifact_input_context_hash,
            "source_revision_set_hash": self.source_revision_set_hash,
            "asset_closure_hash": self.asset_closure_hash,
            "provider_semantics_id": self.metadata.get("provider_semantics_id"),
            "provider_semantics_hash": self.metadata.get("provider_semantics_hash"),
            "multi_alpha_parent_parity_hash": self.metadata.get(
                "multi_alpha_parent_parity_hash"
            ),
        }

    @model_validator(mode="after")
    def _close_artifact(self) -> "Phase1GSelectionArtifactProjection":
        if (
            self.score_count != len(self.scores_json)
            or self.universe_count < self.score_count
        ):
            raise ValueError("artifact score counts do not close")
        symbols: list[str] = []
        ranks: list[int] = []
        for row in self.scores_json:
            symbol = str(row.get("symbol") or "").strip()
            rank = row.get("rank")
            score = row.get("score")
            if (
                not symbol
                or not isinstance(rank, int)
                or rank < 1
                or not isinstance(score, int | float)
                or isinstance(score, bool)
                or not math.isfinite(float(score))
                or not isinstance(row.get("component_scores", {}), dict)
            ):
                raise ValueError(
                    "artifact score row is missing canonical symbol/rank/score/component fields"
                )
            symbols.append(symbol)
            ranks.append(rank)
        if len(symbols) != len(set(symbols)) or ranks != list(range(1, len(ranks) + 1)):
            raise ValueError(
                "artifact score rows must have unique symbols and continuous canonical ranks"
            )
        score_hash = canonical_evidence_json_sha256(self.scores_json)
        if score_hash != self.artifact_sha256:
            raise ValueError("artifact_sha256 does not match canonical score rows")
        if (
            canonical_evidence_json_sha256(
                self.canonical_v2_header(score_hash=score_hash)
            )
            != self.artifact_payload_sha256
        ):
            raise ValueError(
                "artifact_payload_sha256 does not match canonical v2 header"
            )
        if self.score_count:
            first_symbol = str(self.scores_json[0].get("symbol") or "")
            if not first_symbol or self.top_score_symbol != first_symbol:
                raise ValueError(
                    "artifact top_score_symbol does not match the first score row"
                )
        elif self.top_score_symbol is not None:
            raise ValueError("empty artifact cannot have top_score_symbol")
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"artifact_projection_hash"})
        )
        if (
            self.artifact_projection_hash is not None
            and self.artifact_projection_hash != digest
        ):
            raise ValueError("artifact_projection_hash does not match projection")
        object.__setattr__(self, "artifact_projection_hash", digest)
        return self


class Phase1GAlphaComponentProjection(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)
    alpha_id: str = Field(min_length=1)


class Phase1GCombinationPolicyProjection(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)
    method: str = Field(min_length=1)


class Phase1GPackageManifestProjection(_StrictContract):
    schema_version: Literal[MANIFEST_PROJECTION_SCHEMA_VERSION] = (
        MANIFEST_PROJECTION_SCHEMA_VERSION
    )
    package_id: str
    manifest_sha256: str
    alpha_mode: Literal["single_alpha", "multi_alpha"]
    manifest_version: str
    style_family: str | None = None
    source_evidence: dict[str, Any]
    alpha_components: tuple[Phase1GAlphaComponentProjection, ...]
    alpha_combination_policy: Phase1GCombinationPolicyProjection
    declared_runtime_assets: tuple[dict[str, Any], ...]
    manifest_payload: dict[str, Any]
    package_manifest_projection_hash: str | None = None

    @field_validator("manifest_sha256", "package_manifest_projection_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_manifest(self) -> "Phase1GPackageManifestProjection":
        if (
            projected_manifest_json_sha256(self.manifest_payload)
            != self.manifest_sha256
        ):
            raise ValueError(
                "manifest_sha256 does not match raw persisted manifest JSON"
            )
        if (
            str(self.manifest_payload.get("package_id") or "") != self.package_id
            or str(self.manifest_payload.get("alpha_mode") or "") != self.alpha_mode
            or str(self.manifest_payload.get("manifest_version") or "")
            != self.manifest_version
        ):
            raise ValueError(
                "manifest projection identity differs from raw manifest JSON"
            )
        component_ids = tuple(sorted(item.alpha_id for item in self.alpha_components))
        if len(component_ids) != len(set(component_ids)):
            raise ValueError("manifest alpha component ids must be unique")
        if self.alpha_mode == "single_alpha" and len(component_ids) != 1:
            raise ValueError("single Alpha manifest requires exactly one component")
        if self.alpha_mode == "multi_alpha" and len(component_ids) < 2:
            raise ValueError("multi Alpha manifest requires at least two components")
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"package_manifest_projection_hash"})
        )
        if (
            self.package_manifest_projection_hash is not None
            and self.package_manifest_projection_hash != digest
        ):
            raise ValueError(
                "package_manifest_projection_hash does not match projection"
            )
        object.__setattr__(self, "package_manifest_projection_hash", digest)
        return self


class Phase1GStageTraceBuilderInputProjection(_StrictContract):
    schema_version: Literal[STAGE_INPUT_PROJECTION_SCHEMA_VERSION] = (
        STAGE_INPUT_PROJECTION_SCHEMA_VERSION
    )
    alpha_raw: Phase1GStageReceiptProjection
    hmm_adjusted: Phase1GStageReceiptProjection
    risk_policy_adjusted: Phase1GStageReceiptProjection
    selection_effective: Phase1GStageReceiptProjection
    hmm_metadata: dict[str, Any]
    risk_metadata: dict[str, Any]
    universe_metadata: dict[str, Any]
    runtime_config: dict[str, Any]
    component_evidence_by_stage_and_symbol: dict[str, dict[str, dict[str, Any]]]
    builder_input_projection_hash: str | None = None

    @field_validator("builder_input_projection_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return (
            _sha256(value, field_name="builder_input_projection_hash")
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def _derive_hash(self) -> "Phase1GStageTraceBuilderInputProjection":
        for name in _STAGE_NAMES:
            if getattr(self, name).stage != name:
                raise ValueError("stage receipt field does not match stage identity")
        if set(self.component_evidence_by_stage_and_symbol) != set(_STAGE_NAMES):
            raise ValueError("component evidence must contain exactly four stages")
        for name in _STAGE_NAMES:
            stage = getattr(self, name)
            expected_symbols = {
                str(item["symbol"]) for item in (*stage.candidates, *stage.exclusions)
            }
            if (
                set(self.component_evidence_by_stage_and_symbol[name])
                != expected_symbols
            ):
                raise ValueError(
                    "component evidence symbols do not match stage candidate/exclusion rows"
                )
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"builder_input_projection_hash"})
        )
        if (
            self.builder_input_projection_hash is not None
            and self.builder_input_projection_hash != digest
        ):
            raise ValueError("builder_input_projection_hash does not match projection")
        object.__setattr__(self, "builder_input_projection_hash", digest)
        return self


class Phase1GHistoricalTraceProjection(_StrictContract):
    schema_version: Literal[HISTORICAL_TRACE_PROJECTION_SCHEMA_VERSION] = (
        HISTORICAL_TRACE_PROJECTION_SCHEMA_VERSION
    )
    target_request_hash: str
    phase1e_plan_id: str
    phase1e_plan_hash: str
    dse: Phase1GDseProjection
    artifact: Phase1GSelectionArtifactProjection
    package_manifest: Phase1GPackageManifestProjection
    stage_trace_builder_input: Phase1GStageTraceBuilderInputProjection
    candidate_outcome: Literal["CANDIDATES_PRESENT", "VALID_NO_CANDIDATE"]
    component_capability_summary: ComponentCapability
    candidate_count: int = Field(ge=0)
    stage_candidate_count: int = Field(ge=0)
    stage_exclusion_count: int = Field(ge=0)
    canonical_payload_bytes: int = Field(ge=1)
    projection_content_hash: str | None = None

    @field_validator(
        "target_request_hash", "phase1e_plan_hash", "projection_content_hash"
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_hash(self) -> "Phase1GHistoricalTraceProjection":
        if self.candidate_outcome != self.dse.evidence.candidate_outcome:
            raise ValueError("historical outcome differs from DSE outcome")
        if self.candidate_count != self.dse.candidate_count:
            raise ValueError("historical candidate count differs from DSE")
        expected_stage_candidate_count = sum(
            len(getattr(self.stage_trace_builder_input, name).candidates)
            for name in _STAGE_NAMES
        )
        expected_stage_exclusion_count = sum(
            len(getattr(self.stage_trace_builder_input, name).exclusions)
            for name in _STAGE_NAMES
        )
        if (
            self.stage_candidate_count != expected_stage_candidate_count
            or self.stage_exclusion_count != expected_stage_exclusion_count
        ):
            raise ValueError(
                "historical stage row counts do not match stage projection"
            )
        stages = {
            name: getattr(self.stage_trace_builder_input, name) for name in _STAGE_NAMES
        }
        _close_artifact_to_dse(
            dse=self.dse,
            artifact=self.artifact,
            package_manifest=self.package_manifest,
        )
        _close_declared_assets(dse=self.dse, package_manifest=self.package_manifest)
        _close_candidate_transitions(
            dse=self.dse, artifact=self.artifact, stages=stages
        )
        capabilities = []
        for stage_name in _STAGE_NAMES:
            for (
                evidence
            ) in self.stage_trace_builder_input.component_evidence_by_stage_and_symbol[
                stage_name
            ].values():
                try:
                    capabilities.append(
                        ComponentCapability(str(evidence["capability"]))
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    raise ValueError(
                        "component evidence contains an invalid capability"
                    ) from exc
        if self.component_capability_summary is not _aggregate_component_capability(
            capabilities
        ):
            raise ValueError(
                "component capability summary does not match stage evidence"
            )
        payload = self.model_dump(
            mode="json", exclude={"canonical_payload_bytes", "projection_content_hash"}
        )
        encoded = json.dumps(
            canonicalize(payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if self.canonical_payload_bytes != len(encoded):
            raise ValueError(
                "canonical_payload_bytes does not match complete projection payload"
            )
        digest = canonical_json_sha256(payload)
        if (
            self.projection_content_hash is not None
            and self.projection_content_hash != digest
        ):
            raise ValueError("projection_content_hash does not match projection")
        object.__setattr__(self, "projection_content_hash", digest)
        return self


class Phase1GTargetProjectionSnapshot(_StrictContract):
    schema_version: Literal[TARGET_PROJECTION_SNAPSHOT_SCHEMA_VERSION] = (
        TARGET_PROJECTION_SNAPSHOT_SCHEMA_VERSION
    )
    target_request_hash: str
    source_operation_projection: Phase1GSourceOperationProjection
    source_replay_result: Phase1GSourceReplayResult
    source_revision_freeze_intent: Phase1GSourceRevisionFreezeIntent
    historical_trace_projection: Phase1GHistoricalTraceProjection
    expected_capture_plan_count: int = Field(ge=0)
    expected_capture_plan_set_hash: str
    projected_candidate_rows: int = Field(ge=0)
    projected_stage_rows: int = Field(ge=0)
    projected_bytes: int = Field(ge=1)
    target_projection_snapshot_hash: str | None = None

    @field_validator(
        "target_request_hash",
        "expected_capture_plan_set_hash",
        "target_projection_snapshot_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_hash(self) -> "Phase1GTargetProjectionSnapshot":
        if not (
            self.target_request_hash
            == self.source_operation_projection.target_request_hash
            == self.source_replay_result.target_request_hash
            == self.historical_trace_projection.target_request_hash
        ):
            raise ValueError("target snapshot request identities do not close")
        if (
            self.source_revision_freeze_intent
            != self.source_replay_result.freeze_intent
        ):
            raise ValueError("target snapshot freeze intent differs from source replay")
        operation = self.source_operation_projection
        replay = self.source_replay_result
        historical = self.historical_trace_projection
        if not (
            operation.phase1e_plan_id
            == replay.phase1e_plan_id
            == historical.phase1e_plan_id
            and operation.phase1e_plan_hash
            == replay.phase1e_plan_hash
            == historical.phase1e_plan_hash
            and replay.source_operation_projection_hash
            == operation.source_operation_projection_hash
            and replay.requirement_set_id
            == operation.requirement_set.source_requirement_set_id
            and replay.requirement_set_hash
            == operation.requirement_set.source_requirement_set_hash
            and replay.embedded_resolution_receipt == operation.embedded_receipt
        ):
            raise ValueError(
                "target snapshot plan/source/historical identities do not close"
            )
        _close_source_evidence(dse=historical.dse, source_replay=replay)
        capture_payload = [
            item.model_dump(mode="json")
            for item in operation.expected_capture_source_sets
        ]
        if self.expected_capture_plan_count != len(
            capture_payload
        ) or self.expected_capture_plan_set_hash != canonical_json_sha256(
            capture_payload
        ):
            raise ValueError("target snapshot capture plan summary does not close")
        if any(
            item.source_revision_set_id
            != replay.source_revision_set.source_revision_set_id
            or item.source_revision_set_hash
            != replay.source_revision_set.source_revision_set_hash
            for item in operation.expected_capture_source_sets
        ):
            raise ValueError("target snapshot capture plan source set does not close")
        if (
            self.projected_candidate_rows != historical.candidate_count
            or self.projected_stage_rows
            != historical.stage_candidate_count + historical.stage_exclusion_count
            or self.projected_bytes != historical.canonical_payload_bytes
        ):
            raise ValueError("target snapshot projected counts/bytes do not close")
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"target_projection_snapshot_hash"})
        )
        if (
            self.target_projection_snapshot_hash is not None
            and self.target_projection_snapshot_hash != digest
        ):
            raise ValueError("target_projection_snapshot_hash does not match snapshot")
        object.__setattr__(self, "target_projection_snapshot_hash", digest)
        return self


def project_phase1g_dse(row: Mapping[str, Any]) -> Phase1GDseProjection:
    try:
        payload = dict(row["evidence_payload_json"])
        evidence = parse_projected_historical_evidence_v2_strict(payload)
        if canonical_evidence_json_sha256(payload) != str(row["artifact_hash"]):
            raise ValueError("DSE row artifact hash differs from payload")
        package_lineage = evidence.phase0a_package_lineage
        decision_clock = evidence.decision_clock
        config_chain = evidence.phase0a_effective_config_chain
        if (
            str(package_lineage.get("package_id") or "") != str(row["package_id"])
            or str(package_lineage.get("manifest_sha256") or "")
            != str(row["manifest_sha256"])
            or decision_clock.target_trade_date != row["target_trade_date"]
            or decision_clock.effective_cutoff_date != row["cutoff_date"]
            or config_chain.runtime_profile_version_id
            != str(row["runtime_profile_version_id"])
            or config_chain.runtime_profile_hash != str(row["runtime_profile_hash"])
            or evidence.evidence_contract.market_data_scope != str(row["data_source"])
            or len(evidence.selected_candidates) != int(row["candidate_count"])
            or len(evidence.excluded_candidates) != int(row["excluded_count"])
        ):
            raise ValueError("DSE row columns do not close to strict payload")
        return Phase1GDseProjection(
            evidence_id=str(row["evidence_id"]),
            artifact_hash=str(row["artifact_hash"]),
            target_trade_date=row["target_trade_date"],
            cutoff_date=row["cutoff_date"],
            package_id=str(row["package_id"]),
            manifest_sha256=str(row["manifest_sha256"]),
            runtime_profile_version_id=str(row["runtime_profile_version_id"]),
            runtime_profile_hash=str(row["runtime_profile_hash"]),
            source_type=str(row["source_type"]),
            data_source=str(row["data_source"]),
            candidate_count=int(row["candidate_count"]),
            excluded_count=int(row["excluded_count"]),
            evidence=evidence,
        )
    except Phase1GHistoricalTraceError:
        raise
    except Exception as exc:
        raise Phase1GHistoricalTraceError(
            REASON_DSE_INVALID,
            "daily selection evidence failed exact projection",
            context={
                "evidence_id": str(row.get("evidence_id") or ""),
                "exception_type": type(exc).__name__,
            },
        ) from exc


def project_phase1g_artifact(
    row: Mapping[str, Any],
) -> Phase1GSelectionArtifactProjection:
    try:
        return Phase1GSelectionArtifactProjection(
            artifact_id=str(row["artifact_id"]),
            package_id=str(row["package_id"]),
            manifest_sha256=str(row["manifest_sha256"]),
            trade_date=row["trade_date"],
            data_source=str(row["data_source"]),
            runtime_config_hash=str(row["runtime_config_hash"]),
            scores_json=[dict(item) for item in row["scores_json"]],
            artifact_sha256=str(row["artifact_sha256"]),
            score_count=int(row["score_count"]),
            universe_count=int(row["universe_count"]),
            top_score_symbol=(
                str(row["top_score_symbol"])
                if row["top_score_symbol"] is not None
                else None
            ),
            status=str(row["status"]),
            metadata=dict(row["metadata"]),
            artifact_contract_version=str(row["artifact_contract_version"]),
            artifact_payload_sha256=str(row["artifact_payload_sha256"]),
            artifact_input_context_hash=str(row["artifact_input_context_hash"]),
            source_revision_set_hash=str(row["source_revision_set_hash"]),
            asset_closure_hash=str(row["asset_closure_hash"]),
            created_at=row.get("created_at"),
        )
    except Exception as exc:
        raise Phase1GHistoricalTraceError(
            REASON_ARTIFACT_INVALID,
            "selection artifact failed exact projection",
            context={
                "artifact_id": str(row.get("artifact_id") or ""),
                "exception_type": type(exc).__name__,
            },
        ) from exc


def project_phase1g_manifest(
    row: Mapping[str, Any],
) -> Phase1GPackageManifestProjection:
    try:
        payload = dict(row["manifest_json"])
        components = tuple(
            Phase1GAlphaComponentProjection.model_validate(item)
            for item in payload.get("alpha_components") or []
        )
        combination = Phase1GCombinationPolicyProjection.model_validate(
            payload.get("alpha_combination_policy")
        )
        assets = tuple(_declared_runtime_assets(payload))
        return Phase1GPackageManifestProjection(
            package_id=str(row["package_id"]),
            manifest_sha256=str(row["manifest_sha256"]),
            alpha_mode=str(row["alpha_mode"]),
            manifest_version=str(payload.get("manifest_version") or ""),
            style_family=(
                str(
                    payload.get("style_family")
                    or payload.get("strategy_style")
                    or (payload.get("source_evidence") or {}).get("style_family")
                    or ""
                ).strip()
                or None
            ),
            source_evidence=canonicalize(dict(payload.get("source_evidence") or {})),
            alpha_components=components,
            alpha_combination_policy=combination,
            declared_runtime_assets=assets,
            manifest_payload=payload,
        )
    except Exception as exc:
        raise Phase1GHistoricalTraceError(
            REASON_PACKAGE_INVALID,
            "strategy package manifest failed exact projection",
            context={
                "package_id": str(row.get("package_id") or ""),
                "exception_type": type(exc).__name__,
            },
        ) from exc


def build_phase1g_historical_trace_projection(
    *,
    phase1e_plan: Phase1EExecutionPlanProjection,
    source_operation: Phase1GSourceOperationProjection,
    source_replay: Phase1GSourceReplayResult,
    dse: Phase1GDseProjection,
    artifact: Phase1GSelectionArtifactProjection,
    package_manifest: Phase1GPackageManifestProjection,
    binding_row: Mapping[str, Any],
) -> Phase1GHistoricalTraceProjection:
    try:
        _close_binding(phase1e_plan=phase1e_plan, binding_row=binding_row, dse=dse)
        _close_parent_identities(
            phase1e_plan=phase1e_plan,
            source_operation=source_operation,
            source_replay=source_replay,
            dse=dse,
            artifact=artifact,
            package_manifest=package_manifest,
        )
        _close_artifact_to_dse(
            dse=dse, artifact=artifact, package_manifest=package_manifest
        )
        _close_source_evidence(dse=dse, source_replay=source_replay)
        _close_declared_assets(dse=dse, package_manifest=package_manifest)
        stages = _stage_receipts(dse.evidence)
        _close_candidate_transitions(dse=dse, artifact=artifact, stages=stages)
        runtime_config = canonicalize(
            dse.evidence.phase0a_effective_config_chain.package_effective_config
        )
        component_evidence: dict[str, dict[str, dict[str, Any]]] = {}
        capabilities: list[ComponentCapability] = []
        for stage_name in _STAGE_NAMES:
            stage = stages[stage_name]
            stage_evidence: dict[str, dict[str, Any]] = {}
            for candidate in (*stage.candidates, *stage.exclusions):
                symbol = str(candidate.get("symbol") or "")
                if not symbol or symbol in stage_evidence:
                    raise ValueError(
                        "stage candidate/exclusion symbols must be non-empty and unique"
                    )
                result = build_component_evidence(
                    manifest=package_manifest,
                    artifact=artifact,
                    candidate=candidate,
                    runtime_config=runtime_config,
                    stage_name=stage_name,
                )
                stage_evidence[symbol] = result.model_dump(mode="json")
                capabilities.append(result.capability)
            component_evidence[stage_name] = dict(sorted(stage_evidence.items()))
        stage_input = Phase1GStageTraceBuilderInputProjection(
            alpha_raw=stages["alpha_raw"],
            hmm_adjusted=stages["hmm_adjusted"],
            risk_policy_adjusted=stages["risk_policy_adjusted"],
            selection_effective=stages["selection_effective"],
            hmm_metadata=canonicalize(dse.evidence.phase0a_hmm_metadata),
            risk_metadata=canonicalize(dse.evidence.phase0a_risk_policy_metadata),
            universe_metadata=canonicalize(
                dse.evidence.phase0a_universe_evidence.model_dump(mode="json")
            ),
            runtime_config=runtime_config,
            component_evidence_by_stage_and_symbol=component_evidence,
        )
        candidate_count = dse.candidate_count
        stage_candidate_count = sum(
            len(getattr(stage_input, name).candidates) for name in _STAGE_NAMES
        )
        stage_exclusion_count = sum(
            len(getattr(stage_input, name).exclusions) for name in _STAGE_NAMES
        )
        capability = _aggregate_component_capability(capabilities)
        payload = {
            "schema_version": HISTORICAL_TRACE_PROJECTION_SCHEMA_VERSION,
            "target_request_hash": source_operation.target_request_hash,
            "phase1e_plan_id": phase1e_plan.plan_id,
            "phase1e_plan_hash": phase1e_plan.plan_hash,
            "dse": dse.model_dump(mode="json"),
            "artifact": artifact.model_dump(mode="json"),
            "package_manifest": package_manifest.model_dump(mode="json"),
            "stage_trace_builder_input": stage_input.model_dump(mode="json"),
            "candidate_outcome": dse.evidence.candidate_outcome,
            "component_capability_summary": capability.value,
            "candidate_count": candidate_count,
            "stage_candidate_count": stage_candidate_count,
            "stage_exclusion_count": stage_exclusion_count,
        }
        encoded = json.dumps(
            canonicalize(payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return Phase1GHistoricalTraceProjection(
            **payload,
            canonical_payload_bytes=len(encoded),
        )
    except Phase1GHistoricalTraceError:
        raise
    except Exception as exc:
        raise Phase1GHistoricalTraceError(
            REASON_TRACE_MISMATCH,
            "historical trace identities or candidate stages do not close",
            context={
                "phase1e_plan_id": phase1e_plan.plan_id,
                "exception_type": type(exc).__name__,
            },
        ) from exc


def build_phase1g_target_projection_snapshot(
    *,
    source_operation: Phase1GSourceOperationProjection,
    source_replay: Phase1GSourceReplayResult,
    historical_trace: Phase1GHistoricalTraceProjection,
) -> Phase1GTargetProjectionSnapshot:
    capture_payload = [
        item.model_dump(mode="json")
        for item in source_operation.expected_capture_source_sets
    ]
    return Phase1GTargetProjectionSnapshot(
        target_request_hash=source_operation.target_request_hash,
        source_operation_projection=source_operation,
        source_replay_result=source_replay,
        source_revision_freeze_intent=source_replay.freeze_intent,
        historical_trace_projection=historical_trace,
        expected_capture_plan_count=len(capture_payload),
        expected_capture_plan_set_hash=canonical_json_sha256(capture_payload),
        projected_candidate_rows=historical_trace.candidate_count,
        projected_stage_rows=(
            historical_trace.stage_candidate_count
            + historical_trace.stage_exclusion_count
        ),
        projected_bytes=historical_trace.canonical_payload_bytes,
    )


def materialize_phase1g_stage_trace_envelope(
    *,
    context: TraceCaptureContext,
    projection: Phase1GHistoricalTraceProjection,
) -> StageTraceEnvelope:
    dse = projection.dse
    lineage = dse.evidence.phase0a_candidate_lineage
    if (
        context.selection_run_id != str(lineage.get("selection_run_id") or "")
        or context.package_id != dse.package_id
        or context.manifest_sha256 != dse.manifest_sha256
        or context.decision_as_of_trade_date
        != dse.evidence.decision_clock.decision_as_of_trade_date
    ):
        raise Phase1GHistoricalTraceError(
            REASON_TRACE_MISMATCH,
            "trace capture context does not match frozen historical lineage",
            context={"evidence_id": dse.evidence_id},
        )
    envelope = build_stage_trace_envelope(
        context=context,
        manifest=projection.package_manifest,
        artifact=projection.artifact,
        stage_trace=projection.stage_trace_builder_input,
        runtime_config=projection.stage_trace_builder_input.runtime_config,
    )
    actual = {
        item["stage"]: item["candidate_component_evidence"]
        for item in envelope.trace_content["stage_trace"]
    }
    if (
        actual
        != projection.stage_trace_builder_input.component_evidence_by_stage_and_symbol
    ):
        raise Phase1GHistoricalTraceError(
            REASON_TRACE_MISMATCH,
            "materialized envelope component evidence differs from frozen projection",
            context={"evidence_id": dse.evidence_id},
        )
    return envelope


def _stage_receipts(
    evidence: ProjectedHistoricalEvidenceV2Strict,
) -> dict[str, Phase1GStageReceiptProjection]:
    return {
        name: Phase1GStageReceiptProjection.model_validate(
            evidence.phase0a_stage_evidence[name].model_dump(mode="json")
        )
        for name in _STAGE_NAMES
    }


def _close_parent_identities(
    *,
    phase1e_plan: Phase1EExecutionPlanProjection,
    source_operation: Phase1GSourceOperationProjection,
    source_replay: Phase1GSourceReplayResult,
    dse: Phase1GDseProjection,
    artifact: Phase1GSelectionArtifactProjection,
    package_manifest: Phase1GPackageManifestProjection,
) -> None:
    binding = phase1e_plan.evidence_binding
    lineage = dse.evidence.phase0a_candidate_lineage
    package_lineage = dse.evidence.phase0a_package_lineage
    expected = (binding.package_id, binding.manifest_sha256)
    if any(
        pair != expected
        for pair in (
            (source_operation.package_id, source_operation.manifest_sha256),
            (dse.package_id, dse.manifest_sha256),
            (artifact.package_id, artifact.manifest_sha256),
            (package_manifest.package_id, package_manifest.manifest_sha256),
            (
                str(package_lineage.get("package_id") or ""),
                str(package_lineage.get("manifest_sha256") or ""),
            ),
        )
    ):
        raise ValueError("package/manifest identities do not close")
    if source_replay.phase1e_plan_hash != phase1e_plan.plan_hash:
        raise ValueError("source replay does not belong to Phase 1E plan")
    if (
        dse.evidence_id != binding.selection_evidence_id
        or dse.artifact_hash != binding.selection_evidence_hash
        or artifact.artifact_id != binding.selection_artifact_id
        or artifact.artifact_payload_sha256 != binding.selection_artifact_payload_hash
        or str(lineage.get("selection_score_artifact_id") or "") != artifact.artifact_id
        or str(lineage.get("selection_score_artifact_sha256") or "")
        != artifact.artifact_sha256
        or str(lineage.get("selection_score_artifact_payload_sha256") or "")
        != artifact.artifact_payload_sha256
        or str(package_lineage.get("alpha_mode") or "") != package_manifest.alpha_mode
        or binding.alpha_mode != package_manifest.alpha_mode
    ):
        raise ValueError("DSE/artifact/package lineage identities do not close")
    component_ids = tuple(
        sorted(item.alpha_id for item in package_manifest.alpha_components)
    )
    if (
        binding.alpha_mode == "multi_alpha"
        and component_ids != binding.manifest_alpha_component_ids
    ):
        raise ValueError(
            "multi Alpha component identities differ from Phase 1E binding"
        )
    multi_keys = (
        "component_score_artifact_ids",
        "component_score_artifact_sha256",
        "weight_artifact_id",
        "weight_artifact_sha256",
        "combined_score_artifact_sha256",
        "multi_alpha_parent_parity_hash",
        "multi_alpha_parent_parity",
        "component_artifacts",
        "weights",
    )
    if binding.alpha_mode == "multi_alpha":
        expected_multi = {key: artifact.metadata.get(key) for key in multi_keys}
        if any(value in (None, "", {}, []) for value in expected_multi.values()):
            raise ValueError("multi Alpha artifact lineage is incomplete")
        if canonicalize(package_lineage.get("multi_alpha")) != canonicalize(
            expected_multi
        ):
            raise ValueError("multi Alpha DSE lineage differs from artifact metadata")
    elif "multi_alpha" in package_lineage or any(
        artifact.metadata.get(key) not in (None, "", {}, []) for key in multi_keys
    ):
        raise ValueError("single Alpha evidence carries multi Alpha parent lineage")
    if (
        binding.package_asset_closure_hash is not None
        and binding.package_asset_closure_hash != artifact.asset_closure_hash
    ):
        raise ValueError(
            "Phase 1E package asset closure differs from frozen artifact closure"
        )


def _close_artifact_to_dse(
    *,
    dse: Phase1GDseProjection,
    artifact: Phase1GSelectionArtifactProjection,
    package_manifest: Phase1GPackageManifestProjection,
) -> None:
    evidence = dse.evidence
    runtime_hash = _artifact_runtime_config_hash(
        selection_artifact_config=evidence.selection_artifact_config,
        runtime_profile=evidence.runtime_profile,
        package_manifest=package_manifest,
    )
    if (
        artifact.trade_date != evidence.decision_clock.score_trade_date
        or artifact.data_source != dse.data_source
        or artifact.runtime_config_hash != runtime_hash
        or artifact.artifact_input_context_hash
        != canonical_evidence_json_sha256(evidence.point_in_time_context)
        or artifact.source_revision_set_hash
        != canonical_evidence_json_sha256(
            _without_observation_time(evidence.phase0a_source_evidence)
        )
        or artifact.asset_closure_hash
        != canonical_evidence_json_sha256(
            _without_observation_time(evidence.phase0a_asset_closure)
        )
    ):
        raise ValueError("artifact PIT/source/asset identities do not close to DSE")


def _close_source_evidence(
    *, dse: Phase1GDseProjection, source_replay: Phase1GSourceReplayResult
) -> None:
    receipts = list(dse.evidence.phase0a_source_evidence)
    for event in source_replay.expected_source_event_refs:
        matches = [
            item
            for item in receipts
            if item.dataset_id == event.dataset_name
            and item.source_role == event.source_role
            and str(item.content_hash or "") == event.partition_content_hash
            and (
                item.available_at is None
                or item.available_at == event.formal_available_at
            )
        ]
        if len(matches) != 1:
            raise ValueError(
                "source revision member does not close to exactly one DSE source receipt"
            )
        explicit_ref = str(matches[0].phase1_availability_event_ref or "")
        if explicit_ref and explicit_ref not in {
            event.availability_event_id,
            event.event_content_hash,
        }:
            raise ValueError(
                "DSE Phase 1 availability event reference does not match source replay"
            )


def _close_declared_assets(
    *, dse: Phase1GDseProjection, package_manifest: Phase1GPackageManifestProjection
) -> None:
    closure = dse.evidence.phase0a_asset_closure
    closure_pairs = {
        (
            str(item.get("asset_ref") or ""),
            str(item.get("asset_sha256") or item.get("sha256") or ""),
        )
        for item in closure
    }
    missing = [
        item
        for item in package_manifest.declared_runtime_assets
        if (str(item.get("asset_ref") or ""), str(item.get("asset_sha256") or ""))
        not in closure_pairs
    ]
    if missing:
        raise ValueError(
            "DSE asset closure does not include every manifest-declared runtime asset"
        )


def _close_candidate_transitions(
    *,
    dse: Phase1GDseProjection,
    artifact: Phase1GSelectionArtifactProjection,
    stages: Mapping[str, Phase1GStageReceiptProjection],
) -> None:
    alpha = stages["alpha_raw"]
    hmm = stages["hmm_adjusted"]
    risk = stages["risk_policy_adjusted"]
    effective = stages["selection_effective"]
    if alpha.status != "COMPLETE":
        raise ValueError("alpha_raw stage must be complete")
    if list(alpha.candidates) != list(artifact.scores_json):
        raise ValueError("alpha_raw candidates differ from artifact score rows")
    if list(effective.candidates) != _canonical_selected_rows(
        dse.evidence.selected_candidates
    ):
        raise ValueError(
            "selection_effective candidates differ from DSE selected candidates"
        )
    if (
        alpha.output_count != artifact.score_count
        or effective.output_count != dse.candidate_count
    ):
        raise ValueError("artifact/DSE counts differ from stage receipts")
    alpha_symbols = _candidate_symbols(alpha.candidates)
    if hmm.status == "NOT_APPLICABLE":
        if hmm.input_count != len(alpha_symbols):
            raise ValueError("not-applicable HMM input count differs from alpha output")
        hmm_output_symbols = alpha_symbols
    elif hmm.status == "COMPLETE":
        hmm_output_symbols = _close_complete_stage_partition(
            stage=hmm,
            incoming_symbols=alpha_symbols,
        )
    else:
        raise ValueError("HMM stage must be complete or explicitly not applicable")
    risk_output_symbols = _close_complete_stage_partition(
        stage=risk,
        incoming_symbols=hmm_output_symbols,
    )
    _close_selection_effective_partition(
        stage=effective,
        incoming_rows=risk.candidates,
        incoming_symbols=risk_output_symbols,
    )
    if dse.evidence.candidate_outcome == "CANDIDATES_PRESENT":
        if (
            artifact.candidate_outcome != "CANDIDATES_PRESENT"
            or not artifact.scores_json
        ):
            raise ValueError(
                "candidate-present DSE requires candidate-present artifact"
            )
        return
    raw_empty = (
        artifact.candidate_outcome == "VALID_NO_CANDIDATE"
        and artifact.metadata.get("empty_stage") == "alpha_raw"
        and not artifact.scores_json
        and artifact.score_count == 0
        and artifact.universe_count > 0
        and alpha.output_count == 0
        and not hmm_output_symbols
        and (risk.input_count, risk.output_count, risk.excluded_count) == (0, 0, 0)
        and (effective.input_count, effective.output_count, effective.excluded_count)
        == (0, 0, 0)
    )
    filtered_empty = (
        artifact.candidate_outcome == "CANDIDATES_PRESENT"
        and bool(artifact.scores_json)
        and artifact.score_count > 0
        and effective.output_count == 0
        and (risk.output_count == 0 or effective.output_count == 0)
    )
    if raw_empty == filtered_empty:
        raise Phase1GHistoricalTraceError(
            REASON_NO_CANDIDATE_INVALID,
            "valid-no-candidate evidence does not match exactly one legal transition",
            context={"evidence_id": dse.evidence_id},
        )


def _candidate_symbols(rows: tuple[dict[str, Any], ...]) -> set[str]:
    return {str(item["symbol"]) for item in rows}


def _close_complete_stage_partition(
    *,
    stage: Phase1GStageReceiptProjection,
    incoming_symbols: set[str],
) -> set[str]:
    if stage.status != "COMPLETE":
        raise ValueError(f"{stage.stage} stage must be complete")
    output_symbols = _candidate_symbols(stage.candidates)
    excluded_symbols = _candidate_symbols(stage.exclusions)
    if (
        stage.input_count != len(incoming_symbols)
        or output_symbols.intersection(excluded_symbols)
        or output_symbols.union(excluded_symbols) != incoming_symbols
    ):
        raise ValueError(f"{stage.stage} candidate partition does not conserve input")
    return output_symbols


def _close_selection_effective_partition(
    *,
    stage: Phase1GStageReceiptProjection,
    incoming_rows: tuple[dict[str, Any], ...],
    incoming_symbols: set[str],
) -> None:
    if stage.status != "COMPLETE":
        raise ValueError("selection_effective stage must be complete")
    inspected_symbols = _candidate_symbols(stage.candidates).union(
        _candidate_symbols(stage.exclusions)
    )
    if not inspected_symbols.issubset(incoming_symbols):
        raise ValueError("selection_effective contains symbols outside risk output")
    ordered_incoming = sorted(
        incoming_rows,
        key=lambda item: (
            int(item["rank"]),
            -float(item["score"]),
            str(item["symbol"]),
        ),
    )
    expected_inspected = {
        str(item["symbol"]) for item in ordered_incoming[: stage.input_count]
    }
    if inspected_symbols != expected_inspected:
        raise ValueError(
            "selection_effective rows do not match the inspected risk candidate prefix"
        )
    metadata = stage.semantic_payload
    summary_keys = (
        "candidate_pool_count",
        "inspected_count",
        "unprocessed_tail_count",
    )
    requires_summary = stage.input_count < len(incoming_symbols) or any(
        key in metadata for key in summary_keys
    )
    if requires_summary:
        expected = {
            "candidate_pool_count": len(incoming_symbols),
            "inspected_count": stage.input_count,
            "unprocessed_tail_count": len(incoming_symbols) - stage.input_count,
        }
        if any(
            not isinstance(metadata.get(key), int)
            or isinstance(metadata.get(key), bool)
            or metadata.get(key) != value
            for key, value in expected.items()
        ):
            raise ValueError(
                "selection_effective pool/inspection/tail summary does not close"
            )


def _close_binding(
    *,
    phase1e_plan: Phase1EExecutionPlanProjection,
    binding_row: Mapping[str, Any],
    dse: Phase1GDseProjection,
) -> None:
    binding = phase1e_plan.evidence_binding
    decision_date = phase1e_plan.decision_trade_date
    payload = canonicalize(dict(binding_row["binding_payload_json"]))
    package_ids = tuple(str(item) for item in binding_row["package_ids"])
    effective_from = binding_row.get("effective_from_trade_date")
    effective_to = binding_row.get("effective_to_trade_date")
    if (
        str(binding_row["binding_version_id"]) != binding.binding_version_id
        or str(binding_row["program_id"]) != phase1e_plan_program_id(phase1e_plan)
        or canonical_json_sha256(payload) != binding.binding_payload_hash
        or str(binding_row["package_mode"]) != "single_package"
        or package_ids != (binding.package_id,)
        or canonicalize(dict(binding_row["runtime_config_json"]))
        != canonicalize(dse.evidence.phase0a_effective_config_chain.binding_base_config)
        or (effective_from is not None and decision_date < effective_from)
        or (effective_to is not None and decision_date > effective_to)
    ):
        raise ValueError(
            "exact advisory binding row does not close to Phase 1E binding"
        )


def _artifact_runtime_config_hash(
    *,
    selection_artifact_config: dict[str, Any],
    runtime_profile: dict[str, Any],
    package_manifest: Phase1GPackageManifestProjection,
) -> str:
    normalized = canonicalize(selection_artifact_config)
    normalized.pop("auto_generate", None)
    normalized.pop("force_regenerate", None)
    normalized.pop("signal_data_source", None)
    if package_manifest.alpha_mode == "multi_alpha":
        selection = (
            runtime_profile.get("selection")
            if isinstance(runtime_profile, dict)
            else None
        )
        requested_topk = selection.get("top_k") if isinstance(selection, dict) else None
        daily_strategy = package_manifest.manifest_payload.get(
            "backtest_context", {}
        ).get("daily_strategy")
        manifest_topk = (
            daily_strategy.get("topk") if isinstance(daily_strategy, dict) else None
        )
        topk = requested_topk if requested_topk is not None else manifest_topk
        if not isinstance(topk, int) or topk < 1:
            raise ValueError(
                "multi Alpha runtime top_k is absent from frozen DSE/manifest"
            )
        normalized["multi_alpha_final_topk"] = topk
        normalized["multi_alpha_provider_version"] = (
            "multi_alpha_live_selection_provider_v3"
        )
    normalized["artifact_contract_version"] = "selection_score_artifact_v2"
    return canonical_evidence_json_sha256(normalized)


def _canonical_selected_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [
            {
                "symbol": item["symbol"],
                "score": item["score"],
                "rank": item["rank"],
                "reason": item.get("reason"),
                "component_scores": item.get("component_scores") or {},
            }
            for item in rows
        ],
        key=lambda item: (int(item["rank"]), str(item["symbol"])),
    )


def _without_observation_time(rows: Any) -> list[dict[str, Any]]:
    values = rows if isinstance(rows, list | tuple) else []
    return [
        {
            key: value
            for key, value in (
                item.model_dump(mode="json").items()
                if hasattr(item, "model_dump")
                else dict(item).items()
            )
            if key != "first_observed_at"
        }
        for item in values
    ]


def _declared_runtime_assets(payload: dict[str, Any]) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []
    for item in payload.get("factor_set") or []:
        if isinstance(item, dict) and item.get("asset_ref") and item.get("sha256"):
            assets.append(
                {
                    "asset_type": "factor_code",
                    "asset_ref": item["asset_ref"],
                    "asset_sha256": item["sha256"],
                }
            )
    model_assets = payload.get("model_asset")
    for item in model_assets if isinstance(model_assets, list) else [model_assets]:
        if not isinstance(item, dict):
            continue
        if item.get("asset_ref") and item.get("sha256"):
            assets.append(
                {
                    "asset_type": "model_weight",
                    "asset_ref": item["asset_ref"],
                    "asset_sha256": item["sha256"],
                }
            )
        for code in item.get("model_code_assets") or []:
            if isinstance(code, dict) and code.get("asset_ref") and code.get("sha256"):
                assets.append(
                    {
                        "asset_type": "model_code",
                        "asset_ref": code["asset_ref"],
                        "asset_sha256": code["sha256"],
                    }
                )
    runtime_assets = payload.get("runtime_assets") or {}
    alpha158 = (
        runtime_assets.get("alpha158") if isinstance(runtime_assets, dict) else None
    )
    if (
        isinstance(alpha158, dict)
        and alpha158.get("enabled")
        and alpha158.get("asset_ref")
        and alpha158.get("sha256")
    ):
        assets.append(
            {
                "asset_type": "factor_schema",
                "asset_ref": alpha158["asset_ref"],
                "asset_sha256": alpha158["sha256"],
            }
        )
    return sorted(
        assets,
        key=lambda item: (item["asset_type"], item["asset_ref"], item["asset_sha256"]),
    )


def _aggregate_component_capability(
    values: list[ComponentCapability],
) -> ComponentCapability:
    if not values:
        return ComponentCapability.NOT_APPLICABLE
    if ComponentCapability.UNAVAILABLE in values:
        return ComponentCapability.UNAVAILABLE
    if ComponentCapability.PARTIAL in values:
        return ComponentCapability.PARTIAL
    if ComponentCapability.FULL in values:
        return ComponentCapability.FULL
    return ComponentCapability.NOT_APPLICABLE
