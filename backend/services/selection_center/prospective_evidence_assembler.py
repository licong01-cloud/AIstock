"""Pure assembler for immutable prospective DailySelectionEvidence v2 records.

The assembler receives only the objects produced during the current selection
execution.  It deliberately has no data, model, HMM, risk or calendar I/O.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName,
    DailySelectionEvidenceV2Payload,
    DecisionClockEvidenceV2,
    EffectiveConfigChainV2,
    EvidenceCaptureMode,
    EvidenceCaptureReceipt,
    EvidenceCaptureStatus,
    HISTORICAL_RESEARCH_DATA_SOURCE,
    HISTORICAL_RESEARCH_SCOPE,
    ProspectiveExecutionOrigin,
    ProspectiveSelectionContext,
    REASON_ARTIFACT_V2_REQUIRED,
    REASON_ASSET_CLOSURE_INCOMPLETE,
    REASON_CAPTURE_FAILED,
    REASON_CONFIG_CHAIN_INCOMPLETE,
    REASON_CONTEXT_MISSING,
    REASON_DECISION_CLOCK_INVALID,
    REASON_HMM_RECEIPT_INCOMPLETE,
    REASON_HISTORICAL_RESEARCH_ONLY,
    REASON_LINEAGE_MISMATCH,
    REASON_SOURCE_RECEIPT_INCOMPLETE,
    REASON_STAGE_RECEIPT_INCOMPLETE,
    REASON_UNIVERSE_RECEIPT_INCOMPLETE,
    REASON_VALID_NO_CANDIDATE_DECLARATION_FORBIDDEN,
    REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE,
    SelectionStageTrace,
    SourceReadReceipt,
    StageReceiptStatus,
    UniverseEvidenceV2,
    build_stage_receipt,
    canonical_candidate_rows,
    canonical_evidence_json_sha256,
)
from backend.services.strategy_package.models import AlphaMode, StrategyPackageManifest
from backend.services.strategy_package.selection_artifact import SELECTION_SCORE_ARTIFACT_CONTRACT_V2, SelectionScoreArtifact

if TYPE_CHECKING:
    from backend.services.simulation_runtime.models import DailySelectionEvidence


SERIALIZER_VERSION = "advisory_phase0a2c_canonical_v2"


class ProspectiveEvidenceValidationError(ValueError):
    """A fail-closed capture error carrying the design reason code."""

    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class ProspectiveSelectionEvidenceAssembler:
    """Validate one execution trace and construct a v2 DSE without replaying it."""

    def assemble(
        self,
        *,
        context: ProspectiveSelectionContext,
        manifest: StrategyPackageManifest,
        selection_run_id: str,
        artifact: SelectionScoreArtifact,
        stage_trace: SelectionStageTrace,
        runtime_config: dict[str, Any],
        selected: list[SelectionCandidate],
        excluded: list[SelectionExclusion],
        created_by: str | None,
        candidate_outcome: str = "CANDIDATES_PRESENT",
    ) -> "DailySelectionEvidence":
        self._require_context(context=context, selection_run_id=selection_run_id)
        if any(key in runtime_config for key in ("valid_no_candidate", "no_candidate_reason")):
            raise ProspectiveEvidenceValidationError(
                REASON_VALID_NO_CANDIDATE_DECLARATION_FORBIDDEN,
                "prospective evidence cannot be driven by a runtime no-candidate declaration",
                context={"declared_keys": [key for key in ("valid_no_candidate", "no_candidate_reason") if key in runtime_config]},
            )
        self._require_artifact(artifact=artifact, manifest=manifest)
        self._require_historical_research_artifact(context=context, artifact=artifact)
        decision_clock = self._decision_clock(context)
        config_chain = self._config_chain(context=context, runtime_config=runtime_config)
        source_receipts = self._source_receipts(artifact)
        self._validate_source_clock(source_receipts=source_receipts, decision_clock=decision_clock)
        universe = self._universe_evidence(context=context, artifact=artifact, stage_trace=stage_trace)
        self._validate_hmm(stage_trace=stage_trace, decision_clock=decision_clock)
        self._validate_stage_trace(
            stage_trace=stage_trace,
            artifact=artifact,
            selected=selected,
            excluded=excluded,
            candidate_outcome=candidate_outcome,
        )

        metadata = dict(artifact.metadata or {})
        advisory_receipt = build_stage_receipt(
            stage=CandidateStageName.ADVISORY_MODEL,
            status=StageReceiptStatus.NOT_APPLICABLE,
            input_count=0,
            candidates=[],
            semantic_payload={"phase": "0A.2C", "reason": "advisory model is out of scope"},
        )
        runtime_profile = runtime_config.get("runtime_profile")
        runtime_binding = runtime_config.get("runtime_profile_binding")
        if not isinstance(runtime_profile, dict) or not isinstance(runtime_binding, dict):
            raise ProspectiveEvidenceValidationError(
                REASON_CONFIG_CHAIN_INCOMPLETE,
                "prospective capture requires normalized runtime profile and binding",
            )
        if runtime_binding.get("profile_version_id") != config_chain.runtime_profile_version_id:
            raise ProspectiveEvidenceValidationError(
                REASON_LINEAGE_MISMATCH,
                "runtime profile binding version does not match effective config chain",
            )
        if runtime_binding.get("config_sha256") != config_chain.runtime_profile_hash:
            raise ProspectiveEvidenceValidationError(
                REASON_LINEAGE_MISMATCH,
                "runtime profile binding hash does not match effective config chain",
            )

        selected_payload = [item.model_dump(mode="json") for item in selected]
        excluded_payload = [item.model_dump(mode="json") for item in excluded]
        candidate_lineage = {
            "selection_run_id": selection_run_id,
            "selection_score_artifact_id": artifact.artifact_id,
            "selection_score_artifact_sha256": artifact.artifact_sha256,
            "selection_score_artifact_payload_sha256": artifact.artifact_payload_sha256,
            "package_id": manifest.package_id,
            "manifest_sha256": manifest.manifest_sha256,
            "runtime_profile_version_id": config_chain.runtime_profile_version_id,
            "runtime_profile_hash": config_chain.runtime_profile_hash,
        }
        package_lineage = self._package_lineage(
            context=context,
            manifest=manifest,
            artifact=artifact,
            candidate_lineage=candidate_lineage,
        )
        stage_payload = {
            CandidateStageName.ALPHA_RAW.value: stage_trace.alpha_raw,
            CandidateStageName.HMM_ADJUSTED.value: stage_trace.hmm_adjusted,
            CandidateStageName.RISK_POLICY_ADJUSTED.value: stage_trace.risk_policy_adjusted,
            CandidateStageName.SELECTION_EFFECTIVE.value: stage_trace.selection_effective,
            CandidateStageName.ADVISORY_MODEL.value: advisory_receipt,
        }
        evidence_contract = {
            "capture_mode": context.capture_mode,
            "execution_origin": context.execution_origin,
            "prospective_eligible": context.execution_origin == ProspectiveExecutionOrigin.ADVISORY_RUN,
            "research_scope": context.research_scope,
            "execution_prohibited": True,
            "market_data_scope": HISTORICAL_RESEARCH_DATA_SOURCE,
            "serializer_version": SERIALIZER_VERSION,
            "producer_code_release_id": config_chain.code_release_id,
            "producer_code_release_hash": config_chain.code_release_hash,
            # This is the frozen selection event timestamp, not assembler wall-clock
            # time, so an exact retry produces the same immutable DSE payload.
            "captured_at": decision_clock.decision_generated_at,
        }
        payload_model = DailySelectionEvidenceV2Payload.model_validate(
            {
                "evidence_contract": evidence_contract,
                "decision_clock": decision_clock,
                "point_in_time_context": dict(artifact.metadata.get("artifact_input_context") or {}),
                "runtime_profile": runtime_profile,
                "runtime_profile_binding": runtime_binding,
                "selection_artifact_config": _selection_artifact_config(runtime_config),
                "phase0a_effective_config_chain": config_chain,
                "phase0a_hmm_metadata": stage_trace.hmm_metadata,
                "phase0a_risk_policy_metadata": stage_trace.risk_metadata,
                "phase0a_universe_evidence": universe,
                "phase0a_package_lineage": package_lineage,
                "phase0a_asset_closure": metadata.get("asset_closure"),
                "phase0a_source_evidence": source_receipts,
                "phase0a_candidate_lineage": candidate_lineage,
                "phase0a_stage_evidence": stage_payload,
                "candidate_outcome": candidate_outcome,
                "selected_candidates": selected_payload,
                "excluded_candidates": excluded_payload,
            }
        )
        from backend.services.simulation_runtime.models import DailySelectionEvidence, canonical_json_sha256

        payload = payload_model.model_dump(mode="json")
        artifact_hash = canonical_json_sha256(payload)
        return DailySelectionEvidence(
            evidence_id=f"dse_{artifact_hash[:16]}",
            target_trade_date=decision_clock.target_trade_date,
            cutoff_date=decision_clock.effective_cutoff_date,
            package_id=manifest.package_id,
            manifest_sha256=str(manifest.manifest_sha256 or ""),
            release_id=_optional_text(runtime_config.get("strategy_runtime_release", {}).get("release_id")),
            release_hash=_optional_text(runtime_config.get("strategy_runtime_release", {}).get("release_hash")),
            runtime_profile_version_id=config_chain.runtime_profile_version_id,
            runtime_profile_hash=config_chain.runtime_profile_hash,
            source_type=str(metadata.get("source_type") or ""),
            data_source=artifact.data_source,
            candidate_count=len(selected),
            excluded_count=len(excluded),
            artifact_hash=artifact_hash,
            evidence_payload_json=payload,
            created_by=created_by,
        )

    @staticmethod
    def not_requested_receipt() -> EvidenceCaptureReceipt:
        detail = {"requested": False, "status": EvidenceCaptureStatus.NOT_REQUESTED.value}
        return EvidenceCaptureReceipt(
            requested=False,
            schema_version="daily_selection_evidence_v1",
            status=EvidenceCaptureStatus.NOT_REQUESTED,
            detail_hash=canonical_evidence_json_sha256(detail),
        )

    @staticmethod
    def complete_receipt(evidence_ids_by_package: dict[str, str]) -> EvidenceCaptureReceipt:
        detail = {
            "requested": True,
            "status": EvidenceCaptureStatus.COMPLETE.value,
            "evidence_ids_by_package": dict(sorted(evidence_ids_by_package.items())),
        }
        return EvidenceCaptureReceipt(
            requested=True,
            schema_version="daily_selection_evidence_v2",
            status=EvidenceCaptureStatus.COMPLETE,
            evidence_ids_by_package=dict(sorted(evidence_ids_by_package.items())),
            detail_hash=canonical_evidence_json_sha256(detail),
        )

    @staticmethod
    def failed_receipt(*, failures_by_package: dict[str, list[str]]) -> EvidenceCaptureReceipt:
        normalized = {
            package_id: sorted({str(code) for code in codes if str(code)})
            for package_id, codes in sorted(failures_by_package.items())
        }
        reason_codes = sorted({REASON_CAPTURE_FAILED, *(code for codes in normalized.values() for code in codes)})
        detail = {
            "requested": True,
            "status": EvidenceCaptureStatus.FAILED.value,
            "failures_by_package": normalized,
            "reason_codes": reason_codes,
        }
        return EvidenceCaptureReceipt(
            requested=True,
            schema_version="daily_selection_evidence_v2",
            status=EvidenceCaptureStatus.FAILED,
            reason_codes=reason_codes,
            detail_hash=canonical_evidence_json_sha256(detail),
        )

    @staticmethod
    def failure_reason(exc: Exception) -> str:
        return getattr(exc, "reason_code", None) or REASON_CAPTURE_FAILED

    @staticmethod
    def _require_context(*, context: ProspectiveSelectionContext, selection_run_id: str) -> None:
        if context.capture_mode != EvidenceCaptureMode.PROSPECTIVE:
            raise ProspectiveEvidenceValidationError(REASON_CONTEXT_MISSING, "prospective assembler requires capture_mode=PROSPECTIVE")
        if context.selection_run_id != selection_run_id:
            raise ProspectiveEvidenceValidationError(
                REASON_LINEAGE_MISMATCH,
                "prospective context selection_run_id does not match the current SelectionRun",
                context={"context_selection_run_id": context.selection_run_id, "selection_run_id": selection_run_id},
            )
        for field_name in ("decision_clock_seed", "effective_config_seed", "policy_registry_ref", "binding_ref", "source_watermark_seed"):
            if not getattr(context, field_name):
                raise ProspectiveEvidenceValidationError(
                    REASON_CONTEXT_MISSING,
                    f"prospective context {field_name} is required",
                )

    @staticmethod
    def _require_artifact(*, artifact: SelectionScoreArtifact, manifest: StrategyPackageManifest) -> None:
        if artifact.artifact_contract_version != SELECTION_SCORE_ARTIFACT_CONTRACT_V2 or not artifact.artifact_payload_sha256:
            raise ProspectiveEvidenceValidationError(REASON_ARTIFACT_V2_REQUIRED, "prospective DSE requires a v2 score artifact")
        if artifact.package_id != manifest.package_id or artifact.manifest_sha256 != manifest.manifest_sha256:
            raise ProspectiveEvidenceValidationError(REASON_LINEAGE_MISMATCH, "score artifact identity does not match manifest")
        metadata = artifact.metadata or {}
        candidate_outcome = str(metadata.get("candidate_outcome") or "").strip()
        if candidate_outcome not in {"CANDIDATES_PRESENT", "VALID_NO_CANDIDATE"}:
            raise ProspectiveEvidenceValidationError(
                REASON_ARTIFACT_V2_REQUIRED,
                "score artifact candidate_outcome is invalid",
            )
        if candidate_outcome == "VALID_NO_CANDIDATE":
            if (
                artifact.status.value != "SUCCEEDED"
                or artifact.score_count != 0
                or artifact.scores_json
                or artifact.universe_count <= 0
                or metadata.get("empty_stage") != "alpha_raw"
                or artifact.top_score_symbol is not None
            ):
                raise ProspectiveEvidenceValidationError(
                    REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE,
                    "raw-empty artifact does not satisfy the formal no-candidate state machine",
                )
        elif artifact.score_count <= 0 or not artifact.scores_json:
            raise ProspectiveEvidenceValidationError(
                REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE,
                "CANDIDATES_PRESENT artifact cannot be empty",
            )
        if metadata.get("asset_closure_status") != "COMPLETE" or metadata.get("capture_prerequisite_reason_codes"):
            raise ProspectiveEvidenceValidationError(
                REASON_ASSET_CLOSURE_INCOMPLETE,
                "prospective DSE requires a complete artifact asset closure",
            )
        closure = metadata.get("asset_closure")
        if not isinstance(closure, list) or not closure:
            raise ProspectiveEvidenceValidationError(REASON_ASSET_CLOSURE_INCOMPLETE, "artifact asset closure is missing")
        semantic_closure = [{key: value for key, value in item.items() if key != "first_observed_at"} for item in closure if isinstance(item, dict)]
        if len(semantic_closure) != len(closure) or canonical_evidence_json_sha256(semantic_closure) != artifact.asset_closure_hash:
            raise ProspectiveEvidenceValidationError(REASON_ASSET_CLOSURE_INCOMPLETE, "artifact asset closure hash is invalid")

    @staticmethod
    def _require_historical_research_artifact(
        *,
        context: ProspectiveSelectionContext,
        artifact: SelectionScoreArtifact,
    ) -> None:
        if context.execution_origin != ProspectiveExecutionOrigin.ADVISORY_RUN:
            raise ProspectiveEvidenceValidationError(
                REASON_HISTORICAL_RESEARCH_ONLY,
                "prospective evidence is restricted to historical ADVISORY_RUN research",
            )
        if context.research_scope != HISTORICAL_RESEARCH_SCOPE:
            raise ProspectiveEvidenceValidationError(
                REASON_HISTORICAL_RESEARCH_ONLY,
                "prospective evidence research scope is invalid",
            )
        if artifact.data_source != HISTORICAL_RESEARCH_DATA_SOURCE:
            raise ProspectiveEvidenceValidationError(
                REASON_HISTORICAL_RESEARCH_ONLY,
                "prospective evidence requires a DB_HISTORICAL score artifact",
                context={
                    "artifact_id": artifact.artifact_id,
                    "data_source": artifact.data_source,
                    "required_data_source": HISTORICAL_RESEARCH_DATA_SOURCE,
                },
            )

    @staticmethod
    def _decision_clock(context: ProspectiveSelectionContext) -> DecisionClockEvidenceV2:
        try:
            return DecisionClockEvidenceV2.model_validate(context.decision_clock_seed)
        except Exception as exc:
            raise ProspectiveEvidenceValidationError(REASON_DECISION_CLOCK_INVALID, "decision_clock_seed is incomplete or invalid") from exc

    @staticmethod
    def _config_chain(*, context: ProspectiveSelectionContext, runtime_config: dict[str, Any]) -> EffectiveConfigChainV2:
        try:
            chain = EffectiveConfigChainV2.model_validate(context.effective_config_seed)
        except Exception as exc:
            raise ProspectiveEvidenceValidationError(REASON_CONFIG_CHAIN_INCOMPLETE, "effective_config_seed is incomplete or invalid") from exc
        runtime_profile = runtime_config.get("runtime_profile")
        if runtime_profile != chain.selection_normalized_config:
            raise ProspectiveEvidenceValidationError(
                REASON_LINEAGE_MISMATCH,
                "selection_normalized_config does not equal this execution runtime_profile",
            )
        if runtime_config != chain.package_effective_config:
            raise ProspectiveEvidenceValidationError(
                REASON_LINEAGE_MISMATCH,
                "package_effective_config does not equal this execution runtime_config",
            )
        return chain

    @staticmethod
    def _source_receipts(artifact: SelectionScoreArtifact) -> list[dict[str, Any]]:
        raw = (artifact.metadata or {}).get("source_read_receipts")
        if not isinstance(raw, list) or not raw:
            raise ProspectiveEvidenceValidationError(REASON_SOURCE_RECEIPT_INCOMPLETE, "artifact source read receipts are missing")
        try:
            receipts = [SourceReadReceipt.model_validate(item) for item in raw]
        except Exception as exc:
            raise ProspectiveEvidenceValidationError(REASON_SOURCE_RECEIPT_INCOMPLETE, "artifact source read receipt is invalid") from exc
        payload = [item.model_dump(mode="json") for item in receipts]
        if canonical_evidence_json_sha256(payload) != artifact.source_revision_set_hash:
            raise ProspectiveEvidenceValidationError(REASON_SOURCE_RECEIPT_INCOMPLETE, "artifact source receipt hash is invalid")
        return payload

    @staticmethod
    def _validate_source_clock(*, source_receipts: list[dict[str, Any]], decision_clock: DecisionClockEvidenceV2) -> None:
        observed = []
        for raw in source_receipts:
            receipt = SourceReadReceipt.model_validate(raw)
            observed.append(receipt.available_at or receipt.first_observed_at)
        if not observed or any(item is None for item in observed):
            raise ProspectiveEvidenceValidationError(REASON_SOURCE_RECEIPT_INCOMPLETE, "source receipt availability is incomplete")
        if max(item for item in observed if item is not None) != decision_clock.data_available_at:
            raise ProspectiveEvidenceValidationError(
                REASON_DECISION_CLOCK_INVALID,
                "decision clock data_available_at does not match mandatory source receipts",
            )

    @staticmethod
    def _universe_evidence(
        *,
        context: ProspectiveSelectionContext,
        artifact: SelectionScoreArtifact,
        stage_trace: SelectionStageTrace,
    ) -> UniverseEvidenceV2:
        raw = context.source_watermark_seed.get("universe_evidence")
        try:
            universe = UniverseEvidenceV2.model_validate(raw)
        except Exception as exc:
            raise ProspectiveEvidenceValidationError(REASON_UNIVERSE_RECEIPT_INCOMPLETE, "six-layer universe evidence is incomplete") from exc
        by_name = {item.layer: item for item in universe.layers}
        package_layer = by_name["package_eligible_universe"]
        if package_layer.output_count != artifact.universe_count:
            raise ProspectiveEvidenceValidationError(
                REASON_LINEAGE_MISMATCH,
                "package_eligible_universe output count does not match artifact universe_count",
            )
        input_context = (artifact.metadata or {}).get("artifact_input_context") or {}
        if package_layer.output_symbol_set_hash != input_context.get("universe_input_hash"):
            raise ProspectiveEvidenceValidationError(
                REASON_LINEAGE_MISMATCH,
                "package_eligible_universe symbol hash does not match artifact input context",
            )
        risk_layer = by_name["risk_can_buy_universe"]
        if (
            risk_layer.input_count != stage_trace.risk_policy_adjusted.input_count
            or risk_layer.output_count != stage_trace.risk_policy_adjusted.output_count
            or risk_layer.excluded_count != stage_trace.risk_policy_adjusted.excluded_count
        ):
            raise ProspectiveEvidenceValidationError(REASON_LINEAGE_MISMATCH, "risk universe layer does not match risk stage")
        tradability_layer = by_name["tradability_industry_universe"]
        if (
            tradability_layer.input_count != stage_trace.selection_effective.input_count
            or tradability_layer.output_count != stage_trace.selection_effective.output_count
            or tradability_layer.excluded_count != stage_trace.selection_effective.excluded_count
        ):
            raise ProspectiveEvidenceValidationError(REASON_LINEAGE_MISMATCH, "tradability universe layer does not match effective stage")
        return universe

    @staticmethod
    def _validate_hmm(*, stage_trace: SelectionStageTrace, decision_clock: DecisionClockEvidenceV2) -> None:
        receipt = stage_trace.hmm_adjusted
        metadata = stage_trace.hmm_metadata
        if receipt.status == StageReceiptStatus.NOT_APPLICABLE:
            if metadata.get("enabled") and metadata.get("generation_mode") not in {"NO_ALPHA_CANDIDATES"}:
                raise ProspectiveEvidenceValidationError(REASON_HMM_RECEIPT_INCOMPLETE, "enabled HMM cannot be silently not applicable")
            return
        if receipt.status != StageReceiptStatus.COMPLETE or metadata.get("enabled") is not True:
            raise ProspectiveEvidenceValidationError(REASON_HMM_RECEIPT_INCOMPLETE, "HMM stage receipt is incomplete")
        required = (
            "model_snapshot_id",
            "signal_preset",
            "snapshot_status",
            "snapshot_trained_at",
            "available_at",
            "training_information_cutoff",
            "as_of_trade_date",
            "effective_trade_date",
            "model_sha256",
            "model_artifact_sha256",
            "coefficients_sha256",
            "coefficient_sha256",
            "coefficient_trade_date",
            "input_data_max_dates_hash",
            "freshness_lag",
        )
        if any(metadata.get(field) in (None, "") for field in required):
            raise ProspectiveEvidenceValidationError(REASON_HMM_RECEIPT_INCOMPLETE, "HMM metadata is missing mandatory vintage fields")
        if metadata.get("generation_mode") != "EXACT_SNAPSHOT":
            raise ProspectiveEvidenceValidationError(REASON_HMM_RECEIPT_INCOMPLETE, "prospective HMM requires an exact snapshot")
        if str(metadata.get("coefficient_trade_date")) != decision_clock.decision_as_of_trade_date.isoformat():
            raise ProspectiveEvidenceValidationError(REASON_LINEAGE_MISMATCH, "HMM coefficient date does not match decision date")
        if str(metadata.get("as_of_trade_date")) != decision_clock.decision_as_of_trade_date.isoformat():
            raise ProspectiveEvidenceValidationError(REASON_LINEAGE_MISMATCH, "HMM as-of date does not match decision date")
        if str(metadata.get("effective_trade_date")) != decision_clock.effective_entry_trade_date.isoformat():
            raise ProspectiveEvidenceValidationError(REASON_LINEAGE_MISMATCH, "HMM effective trade date does not match decision clock")

    @staticmethod
    def _validate_stage_trace(
        *,
        stage_trace: SelectionStageTrace,
        artifact: SelectionScoreArtifact,
        selected: list[SelectionCandidate],
        excluded: list[SelectionExclusion],
        candidate_outcome: str,
    ) -> None:
        alpha = stage_trace.alpha_raw
        if alpha.status != StageReceiptStatus.COMPLETE:
            raise ProspectiveEvidenceValidationError(REASON_STAGE_RECEIPT_INCOMPLETE, "alpha_raw stage is incomplete")
        if alpha.candidates != canonical_candidate_rows([SelectionCandidate.model_validate(row) for row in artifact.scores_json]):
            raise ProspectiveEvidenceValidationError(REASON_LINEAGE_MISMATCH, "alpha_raw stage does not match score artifact rows")
        for receipt in (
            stage_trace.risk_policy_adjusted,
            stage_trace.selection_effective,
        ):
            if receipt.status != StageReceiptStatus.COMPLETE:
                raise ProspectiveEvidenceValidationError(REASON_STAGE_RECEIPT_INCOMPLETE, "executed stage receipt is incomplete")
        if stage_trace.selection_effective.candidates != canonical_candidate_rows(selected):
            raise ProspectiveEvidenceValidationError(REASON_LINEAGE_MISMATCH, "selection_effective stage does not match selected candidates")
        expected_exclusions = [
            *stage_trace.hmm_adjusted.exclusions,
            *stage_trace.risk_policy_adjusted.exclusions,
            *stage_trace.selection_effective.exclusions,
        ]
        actual_exclusions = sorted(
            [item.model_dump(mode="json") for item in excluded],
            key=lambda item: (str(item["source"]), str(item["reason"]), int(item["rank"]), str(item["symbol"])),
        )
        if expected_exclusions != actual_exclusions:
            raise ProspectiveEvidenceValidationError(REASON_LINEAGE_MISMATCH, "stage exclusions do not match selected execution exclusions")
        if candidate_outcome == "CANDIDATES_PRESENT" and not selected:
            raise ProspectiveEvidenceValidationError(REASON_STAGE_RECEIPT_INCOMPLETE, "candidate outcome contradicts selected candidates")
        if candidate_outcome == "VALID_NO_CANDIDATE":
            if selected:
                raise ProspectiveEvidenceValidationError(
                    REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE,
                    "valid no-candidate outcome cannot contain selected candidates",
                )
            raw_empty = alpha.output_count == 0
            if raw_empty:
                metadata = artifact.metadata or {}
                if (
                    artifact.score_count != 0
                    or artifact.universe_count <= 0
                    or metadata.get("candidate_outcome") != "VALID_NO_CANDIDATE"
                    or metadata.get("empty_stage") != "alpha_raw"
                    or stage_trace.risk_policy_adjusted.input_count != 0
                    or stage_trace.risk_policy_adjusted.output_count != 0
                    or stage_trace.risk_policy_adjusted.excluded_count != 0
                    or stage_trace.selection_effective.input_count != 0
                    or stage_trace.selection_effective.output_count != 0
                    or stage_trace.selection_effective.excluded_count != 0
                ):
                    raise ProspectiveEvidenceValidationError(
                        REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE,
                        "raw-empty trace is incomplete or contradictory",
                    )
                return
            metadata = artifact.metadata or {}
            if metadata.get("candidate_outcome") != "CANDIDATES_PRESENT" or artifact.score_count <= 0:
                raise ProspectiveEvidenceValidationError(
                    REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE,
                    "filtered-empty trace requires a non-empty raw artifact",
                )
            risk_empty = stage_trace.risk_policy_adjusted.output_count == 0
            selection_empty = stage_trace.selection_effective.output_count == 0
            if not (risk_empty or selection_empty):
                raise ProspectiveEvidenceValidationError(
                    REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE,
                    "valid no-candidate outcome has no formal empty stage",
                )

    @staticmethod
    def _package_lineage(
        *,
        context: ProspectiveSelectionContext,
        manifest: StrategyPackageManifest,
        artifact: SelectionScoreArtifact,
        candidate_lineage: dict[str, Any],
    ) -> dict[str, Any]:
        metadata = dict(artifact.metadata or {})
        _require_ref(context.policy_registry_ref, label="policy_registry_ref")
        _require_ref(context.binding_ref, label="binding_ref")
        lineage = {
            "package_id": manifest.package_id,
            "manifest_sha256": manifest.manifest_sha256,
            "alpha_mode": manifest.alpha_mode.value,
            "provider_semantics_id": metadata.get("provider_semantics_id"),
            "provider_semantics_hash": metadata.get("provider_semantics_hash"),
            "policy_registry_ref": context.policy_registry_ref,
            "binding_ref": context.binding_ref,
            "candidate_lineage": candidate_lineage,
        }
        if manifest.alpha_mode == AlphaMode.MULTI_ALPHA:
            required = (
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
            if any(metadata.get(field) in (None, "", {}, []) for field in required):
                raise ProspectiveEvidenceValidationError(REASON_LINEAGE_MISMATCH, "multi-alpha package lineage is incomplete")
            lineage["multi_alpha"] = {field: metadata[field] for field in required}
        return lineage


def _selection_artifact_config(runtime_config: dict[str, Any]) -> dict[str, Any]:
    value = runtime_config.get("selection_artifact_config", runtime_config.get("selection_artifact", {}))
    if not isinstance(value, dict):
        raise ProspectiveEvidenceValidationError(REASON_CONFIG_CHAIN_INCOMPLETE, "selection artifact config must be an object")
    return dict(value)


def _optional_text(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _require_ref(value: Mapping[str, Any], *, label: str) -> None:
    identifiers = [key for key, item in value.items() if str(key).endswith("_id") and str(item or "").strip()]
    hashes = [
        key
        for key, item in value.items()
        if str(key).endswith("_hash") and len(str(item or "").strip()) == 64
    ]
    if not identifiers or not hashes:
        raise ProspectiveEvidenceValidationError(REASON_CONTEXT_MISSING, f"{label} requires an id and sha256 hash")
