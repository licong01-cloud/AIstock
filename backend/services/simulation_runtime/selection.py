"""Shared StrategyPackage daily selection service for simulation runtimes.

This module is broker-neutral. Selection Center, LocalSim and MiniQMT must use
this service to produce the same daily signal evidence before any target,
rebalance, execution-plan or broker-specific logic runs.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.advisory_phase1.stage_trace import (
    Phase1TraceCaptureReceipt,
    Phase1TraceCaptureService,
    REASON_TRACE_CONTEXT_INVALID,
    REASON_TRACE_CAPTURE_FAILED,
    TraceCaptureResult,
    TraceCaptureState,
)
from backend.services.paper_trading_v2.market_data import TradeCalendarProvider
from backend.services.selection_center.canonical_pit_runtime import (
    CANONICAL_PIT_RUNTIME_LEASE_KEY,
    freeze_canonical_pit_runtime_binding,
    has_canonical_pit_runtime_profile,
    require_canonical_pit_generation_current,
    require_canonical_pit_runtime_binding,
)
from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion, SelectionMode
from backend.services.selection_center.package_health import SelectionPackageHealthService
from backend.services.selection_center.prospective_evidence import (
    EvidenceCaptureMode,
    EvidenceCaptureReceipt,
    ProspectiveSelectionContext,
    SelectionStageTrace,
    require_historical_research_data_source,
)
from backend.services.selection_center.prospective_evidence_assembler import ProspectiveSelectionEvidenceAssembler
from backend.services.selection_center.risk_policy import StockRiskPolicyService
from backend.services.selection_center.runtime_profile import (
    GENERATED_RUNTIME_PROFILE_VERSION_ID,
    RUNTIME_PROFILE_BINDING_KEY,
    SelectionRuntimeProfile,
    attach_default_runtime_profile_binding,
    is_non_trading_runtime_config,
    mark_non_trading_preview_runtime_config,
    normalize_selection_runtime_config,
    parse_selection_runtime_profile,
    refresh_generated_runtime_profile_binding,
    runtime_profile_config_sha256,
    validate_runtime_profile_binding,
)
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.backtest_contract import normalize_runtime_config_with_backtest_contract
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
)
from backend.services.strategy_package.multi_alpha_live import LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime, apply_runtime_variant_config
from backend.services.strategy_package.runtime_variant import RuntimeVariantValidationStatus
from backend.services.strategy_package.selection_computation import (
    PreparedPackageComponentLineageV1,
    PreparedPackageSignalV1,
    SelectionArtifactHeaderV1,
    StrategyPackageSelectionComputation,
    StrategyPackageSelectionComputationRequestV1,
    StrategyPackageSelectionReadOnlyProvidersV1,
    package_weights_from_runtime_config,
    selection_runtime_profile_sha256,
)
from backend.services.strategy_package.selection_artifact import (
    StrategyPackageSelectionArtifactService,
    selection_artifact_runtime_hash_for_manifest,
    selection_artifact_runtime_hash_v2_for_manifest,
)
from backend.services.trading_core.errors import (
    DataUnavailableError,
    RuntimeConfigInvalidError,
    TradingCalendarUnavailableError,
)

from .models import (
    DailySelectionEvidence,
    StrategyRuntimeRelease,
    assert_selection_only_payload_boundary,
    canonical_json_sha256,
)
from .repository import InMemorySimulationRuntimeRepository, SimulationRuntimeRepository


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StrategyPackageSelectionResult:
    runtime_config: dict[str, Any]
    package_results: dict[str, list[SelectionCandidate]]
    aggregate_results: list[SelectionCandidate]
    excluded_results: dict[str, list[SelectionExclusion]]
    manifest_sha256_by_package: dict[str, str]
    evidence_by_package: dict[str, DailySelectionEvidence]
    valid_no_candidate: bool = False
    no_candidate_reason: str | None = None
    stage_trace_by_package: dict[str, SelectionStageTrace] = field(default_factory=dict)
    evidence_capture_receipt: EvidenceCaptureReceipt = field(
        default_factory=ProspectiveSelectionEvidenceAssembler.not_requested_receipt
    )
    phase1_trace_capture_receipt: Phase1TraceCaptureReceipt = field(default_factory=Phase1TraceCaptureReceipt.disabled)


class DailySelectionSignalService:
    """Load or generate the authoritative signal snapshot for one package."""

    def __init__(
        self,
        *,
        runtime: StrategyPackageRuntime | None = None,
        selection_artifact_service: StrategyPackageSelectionArtifactService | Any | None = None,
    ) -> None:
        self.runtime = runtime or StrategyPackageRuntime()
        self.selection_artifact_service = selection_artifact_service or StrategyPackageSelectionArtifactService(
            artifact_repository=getattr(self.runtime, "artifact_repository", None),
        )

    def build_signal_snapshot(
        self,
        *,
        record: Any,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> Any:
        return self.build_signal_snapshot_with_trace(
            record=record,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
        ).snapshot

    def build_signal_snapshot_with_trace(
        self,
        *,
        record: Any,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
        require_frozen_hmm_snapshot: bool = False,
        hmm_effective_trade_date: date | None = None,
    ) -> Any:
        self._ensure_authoritative_selection_artifact(
            record=record,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
        )
        return self.runtime.build_signal_snapshot_with_trace(
            manifest=record.current_manifest(),
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
            require_frozen_hmm_snapshot=require_frozen_hmm_snapshot,
            hmm_effective_trade_date=hmm_effective_trade_date,
            prohibit_no_candidate_declaration=require_frozen_hmm_snapshot,
        )

    def _ensure_authoritative_selection_artifact(
        self,
        *,
        record: Any,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> None:
        artifact_config = StrategyPackageSelectionService.selection_artifact_config(runtime_config)
        if not bool(artifact_config.get("auto_generate")):
            return

        manifest = record.current_manifest()
        runtime_hashes = [
            selection_artifact_runtime_hash_v2_for_manifest(manifest, runtime_config),
            selection_artifact_runtime_hash_for_manifest(manifest, runtime_config),
        ]
        cutoff_date = StrategyPackageSelectionService.parse_selection_cutoff_date(
            artifact_config,
            trade_date=trade_date,
            strict_before=True,
        )
        force_regenerate = bool(artifact_config.get("force_regenerate"))
        artifact_repository = getattr(self.runtime, "artifact_repository", None)
        if artifact_repository is not None and not force_regenerate:
            for runtime_hash in dict.fromkeys(runtime_hashes):
                try:
                    artifact = artifact_repository.get(
                        package_id=record.package_id,
                        manifest_sha256=manifest.manifest_sha256 or record.manifest_sha256,
                        trade_date=trade_date,
                        data_source=data_source,
                        runtime_config_hash=runtime_hash,
                    )
                    metadata = artifact.metadata or {}
                    expected_source_type = (
                        LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE
                        if getattr(getattr(manifest, "alpha_mode", None), "value", None) == "multi_alpha"
                        else AUTHORITATIVE_SELECTION_SOURCE_TYPE
                    )
                    if (
                        artifact.status.value == "SUCCEEDED"
                        and (artifact.scores_json or StrategyPackageRuntime._is_natural_raw_empty_artifact(artifact))
                        and metadata.get("source_type") == expected_source_type
                        and metadata.get("authority_scope") == AUTHORITATIVE_SELECTION_SCOPE
                    ):
                        return
                except DataUnavailableError:
                    continue

        self.selection_artifact_service.generate_from_live_inference(
            package_id=record.package_id,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
            include_reference_price=bool(artifact_config.get("include_reference_price", True)),
            cutoff_date=cutoff_date,
        )


class StrategyPackageSelectionService:
    """Broker-neutral StrategyPackage selection entry shared by all simulators."""

    def __init__(
        self,
        *,
        package_repository: StrategyPackageRepository | Any | None = None,
        runtime: StrategyPackageRuntime | None = None,
        tradability_filter: TradabilityFilter | Any | None = None,
        refresh_audit: DataRefreshAuditRepository | Any | None = None,
        selection_artifact_service: StrategyPackageSelectionArtifactService | Any | None = None,
        calendar_provider: TradeCalendarProvider | Any | None = None,
        risk_policy_service: StockRiskPolicyService | Any | None = None,
        package_health_service: SelectionPackageHealthService | Any | None = None,
        repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
        signal_service: DailySelectionSignalService | None = None,
        phase1_trace_capture_service: Phase1TraceCaptureService | Any | None = None,
        selection_computation: StrategyPackageSelectionComputation | Any | None = None,
        pit_authority_resolver: Any | None = None,
    ) -> None:
        self.package_repository = package_repository or StrategyPackageRepository()
        self.runtime = runtime or StrategyPackageRuntime()
        self.tradability_filter = tradability_filter or TradabilityFilter()
        self.refresh_audit = refresh_audit or DataRefreshAuditRepository()
        self.selection_artifact_service = selection_artifact_service or StrategyPackageSelectionArtifactService(
            package_repository=self.package_repository,
            artifact_repository=getattr(self.runtime, "artifact_repository", None),
        )
        self.calendar_provider = calendar_provider or TradeCalendarProvider()
        self.risk_policy_service = risk_policy_service or StockRiskPolicyService()
        self.package_health_service = package_health_service or SelectionPackageHealthService(
            artifact_repository=getattr(self.runtime, "artifact_repository", None),
            runtime_source_resolver=getattr(self.selection_artifact_service, "runtime_asset_resolver", None),
            hmm_runtime=getattr(self.runtime, "hmm_runtime", None),
        )
        self.repository = repository or SimulationRuntimeRepository()
        self.signal_service = signal_service or DailySelectionSignalService(
            runtime=self.runtime,
            selection_artifact_service=self.selection_artifact_service,
        )
        self.phase1_trace_capture_service = phase1_trace_capture_service or Phase1TraceCaptureService()
        self.selection_computation = selection_computation or StrategyPackageSelectionComputation()
        self.pit_authority_resolver = pit_authority_resolver

    def run_selection(
        self,
        *,
        package_ids: list[str],
        mode: SelectionMode,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any] | None = None,
        runtime_release: StrategyRuntimeRelease | None = None,
        created_by: str | None = None,
        prospective_context: ProspectiveSelectionContext | None = None,
    ) -> StrategyPackageSelectionResult:
        raw_config = dict(runtime_config or {})
        require_historical_research_data_source(
            context=prospective_context,
            data_source=data_source,
        )
        if runtime_release is not None:
            raw_config = self._attach_runtime_release_binding(raw_config, runtime_release, package_ids=package_ids)
        behavior_context = {
            "trade_date": trade_date.isoformat(),
            "data_source": data_source,
            "mode": mode.value,
            "path": "strategy_package_selection.run_selection",
        }
        assert_selection_only_payload_boundary(raw_config, context=behavior_context)
        if prospective_context is not None and prospective_context.capture_mode == EvidenceCaptureMode.PROSPECTIVE:
            raw_config = mark_non_trading_preview_runtime_config(
                raw_config,
                reason="historical advisory research evidence",
            )
        elif is_non_trading_runtime_config(raw_config):
            raw_config = mark_non_trading_preview_runtime_config(
                raw_config,
                reason="selection center non-trading preview",
            )
        else:
            raw_config = attach_default_runtime_profile_binding(raw_config)
        if has_canonical_pit_runtime_profile(raw_config):
            raw_config, _pit_runtime_lease = freeze_canonical_pit_runtime_binding(
                raw_config,
                trade_date=trade_date,
                authority_resolver=self.pit_authority_resolver,
            )
        config = normalize_selection_runtime_config(raw_config)
        validate_runtime_profile_binding(
            config,
            context=behavior_context,
            require_trade_enabled=not is_non_trading_runtime_config(config),
        )
        config = self._apply_point_in_time_selection_config(config, trade_date=trade_date)
        config = refresh_generated_runtime_profile_binding(config)
        validate_runtime_profile_binding(
            config,
            context={**behavior_context, "phase": "post_pit_selection_config"},
            require_trade_enabled=not is_non_trading_runtime_config(config),
        )
        self._validate_request_shape(package_ids=package_ids, mode=mode)
        weights = (
            package_weights_from_runtime_config(config, package_ids) if mode == SelectionMode.WEIGHTED_FUSION else None
        )
        records_by_id, package_configs, package_health = self._prepare_package_runtime_configs(
            package_ids=package_ids,
            config=config,
            trade_date=trade_date,
            data_source=data_source,
        )
        if package_configs:
            config["package_runtime_configs"] = package_configs
        if package_health:
            config["package_health"] = package_health

        global_profile = parse_selection_runtime_profile(config)
        self._require_data_ready(trade_date=trade_date, runtime_config=config)
        artifact_by_package: dict[str, Any] = {}
        prepared_signals: dict[str, PreparedPackageSignalV1] = {}
        package_profiles: dict[str, SelectionRuntimeProfile] = {}
        package_profile_hashes: dict[str, str] = {}
        package_top_k: dict[str, int] = {}
        for package_id in package_ids:
            record = records_by_id[package_id]
            manifest = record.current_manifest()
            package_config = package_configs[package_id]
            package_profile = parse_selection_runtime_profile(package_config)
            top_k = self._top_k_for_package(manifest, package_profile, global_profile, package_config)
            signal_trace = self.signal_service.build_signal_snapshot_with_trace(
                record=record,
                trade_date=trade_date,
                data_source=data_source,
                runtime_config=package_config,
                require_frozen_hmm_snapshot=bool(
                    prospective_context and prospective_context.capture_mode == EvidenceCaptureMode.PROSPECTIVE
                ),
                hmm_effective_trade_date=self._prospective_hmm_effective_trade_date(prospective_context),
            )
            snapshot = signal_trace.snapshot
            artifact_by_package[package_id] = signal_trace.score_artifact
            artifact = signal_trace.score_artifact
            artifact_metadata = getattr(artifact, "metadata", None)
            artifact_input_context = (
                artifact_metadata.get("artifact_input_context") if isinstance(artifact_metadata, dict) else None
            )
            prepared_signals[package_id] = PreparedPackageSignalV1(
                package_id=manifest.package_id,
                package_version=record.package_version,
                manifest_sha256=snapshot.manifest_sha256,
                alpha_mode=manifest.alpha_mode,
                component_lineage=tuple(
                    PreparedPackageComponentLineageV1(
                        component_id=component.alpha_id,
                        component_weight=component.component_weight,
                        factor_ids=tuple(component.factor_ids),
                        score_normalization=component.score_normalization,
                    )
                    for component in manifest.alpha_components
                ),
                alpha_raw_candidates=tuple(signal_trace.alpha_raw_candidates),
                hmm_adjusted_candidates=tuple(snapshot.candidates),
                hmm_receipt=signal_trace.hmm_result.receipt,
                hmm_metadata=signal_trace.hmm_result.hmm_metadata,
                artifact_header=SelectionArtifactHeaderV1(
                    artifact_id=getattr(artifact, "artifact_id", None),
                    artifact_sha256=getattr(artifact, "artifact_sha256", None),
                    package_id=getattr(artifact, "package_id", ""),
                    manifest_sha256=getattr(artifact, "manifest_sha256", ""),
                    trade_date=getattr(artifact, "trade_date", None),
                    data_source=getattr(artifact, "data_source", ""),
                    runtime_config_hash=getattr(artifact, "runtime_config_hash", ""),
                    artifact_payload_sha256=getattr(artifact, "artifact_payload_sha256", None),
                    artifact_contract_version=getattr(artifact, "artifact_contract_version", None),
                    artifact_input_context_hash=getattr(artifact, "artifact_input_context_hash", None),
                    source_revision_set_hash=getattr(artifact, "source_revision_set_hash", None),
                    asset_closure_hash=getattr(artifact, "asset_closure_hash", None),
                    universe_identity_hash=(
                        artifact_input_context.get("universe_input_hash")
                        if isinstance(artifact_input_context, dict)
                        else None
                    ),
                ),
                input_context_hash=getattr(artifact, "artifact_input_context_hash", None),
                source_revision_set_hash=getattr(artifact, "source_revision_set_hash", None),
                universe_identity_hash=(
                    artifact_input_context.get("universe_input_hash")
                    if isinstance(artifact_input_context, dict)
                    else None
                ),
                valid_no_candidate=snapshot.valid_no_candidate,
                no_candidate_reason=snapshot.no_candidate_reason,
            )
            package_profiles[package_id] = package_profile
            package_profile_hashes[package_id] = selection_runtime_profile_sha256(package_profile)
            package_top_k[package_id] = top_k

        computation = self.selection_computation.compute(
            request=StrategyPackageSelectionComputationRequestV1(
                trade_date=trade_date,
                data_source=data_source,
                selection_mode=mode,
                ordered_package_ids=tuple(package_ids),
                package_runtime_profiles=package_profiles,
                package_runtime_profile_hashes=package_profile_hashes,
                package_top_k=package_top_k,
                package_weights=weights,
            ),
            prepared_signals=prepared_signals,
            providers=StrategyPackageSelectionReadOnlyProvidersV1(
                risk_policy=self.risk_policy_service,
                tradability=self.tradability_filter,
            ),
        )
        package_results = {package_id: list(rows) for package_id, rows in computation.package_results.items()}
        excluded_results = {package_id: list(rows) for package_id, rows in computation.excluded_results.items()}
        aggregate = list(computation.aggregate_results)
        manifest_sha = dict(computation.manifest_sha256_by_package)
        stage_trace_by_package = dict(computation.stage_trace_by_package)
        candidate_outcome_by_package = dict(computation.candidate_outcome_by_package)
        if has_canonical_pit_runtime_profile(config):
            require_canonical_pit_generation_current(
                config,
                authority_resolver=self.pit_authority_resolver,
            )

        evidence_by_package, capture_receipt = self._build_operational_evidence_by_package(
            package_ids=package_ids,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=config,
            package_runtime_configs=package_configs,
            package_results=package_results,
            excluded_results=excluded_results,
            manifest_sha256_by_package=manifest_sha,
            runtime_release=runtime_release,
            created_by=created_by,
            prospective_context=prospective_context,
            artifact_by_package=artifact_by_package,
            stage_trace_by_package=stage_trace_by_package,
            candidate_outcome_by_package=candidate_outcome_by_package,
        )
        phase1_trace_receipt = self._capture_phase1_stage_traces(
            prospective_context=prospective_context,
            data_source=data_source,
            trade_date=trade_date,
            records_by_id=records_by_id,
            package_runtime_configs=package_configs,
            artifact_by_package=artifact_by_package,
            stage_trace_by_package=stage_trace_by_package,
        )
        config = self._attach_evidence_refs(config, evidence_by_package, capture_receipt=capture_receipt)
        return StrategyPackageSelectionResult(
            runtime_config=config,
            package_results=package_results,
            aggregate_results=aggregate,
            excluded_results=excluded_results,
            manifest_sha256_by_package=manifest_sha,
            evidence_by_package=evidence_by_package,
            valid_no_candidate=computation.valid_no_candidate,
            no_candidate_reason=computation.no_candidate_reason,
            stage_trace_by_package=stage_trace_by_package,
            evidence_capture_receipt=capture_receipt,
            phase1_trace_capture_receipt=phase1_trace_receipt,
        )

    def _capture_phase1_stage_traces(
        self,
        *,
        prospective_context: ProspectiveSelectionContext | None,
        data_source: str,
        trade_date: date,
        records_by_id: dict[str, Any],
        package_runtime_configs: dict[str, dict[str, Any]],
        artifact_by_package: dict[str, Any],
        stage_trace_by_package: dict[str, SelectionStageTrace],
    ) -> Phase1TraceCaptureReceipt:
        """Capture after Selection/DSE work; all capture faults remain explicit side receipts."""

        if prospective_context is None or prospective_context.capture_mode is not EvidenceCaptureMode.PROSPECTIVE:
            return Phase1TraceCaptureReceipt.disabled()
        if not self.phase1_trace_capture_service.enabled:
            return Phase1TraceCaptureReceipt.disabled()
        results: dict[str, TraceCaptureResult] = {}
        decision_as_of_trade_date = self._prospective_decision_as_of_trade_date(prospective_context)
        for package_id, stage_trace in stage_trace_by_package.items():
            if decision_as_of_trade_date is None:
                results[package_id] = TraceCaptureResult(
                    package_id=package_id,
                    state=TraceCaptureState.CAPTURE_FAILED,
                    reason_codes=(REASON_TRACE_CONTEXT_INVALID,),
                )
                continue
            try:
                record = records_by_id[package_id]
                manifest = record.current_manifest()
                results[package_id] = self.phase1_trace_capture_service.capture_package(
                    selection_run_id=prospective_context.selection_run_id,
                    package_id=package_id,
                    manifest_sha256=manifest.manifest_sha256,
                    decision_as_of_trade_date=decision_as_of_trade_date,
                    data_source=data_source,
                    execution_origin=prospective_context.execution_origin.value,
                    research_scope=prospective_context.research_scope,
                    execution_prohibited=True,
                    manifest=manifest,
                    artifact=artifact_by_package[package_id],
                    stage_trace=stage_trace,
                    runtime_config=package_runtime_configs[package_id],
                )
            except Exception:
                logger.exception(
                    "phase1 stage trace capture boundary failed",
                    extra={"package_id": package_id, "selection_run_id": prospective_context.selection_run_id},
                )
                results[package_id] = TraceCaptureResult(
                    package_id=package_id,
                    state=TraceCaptureState.CAPTURE_FAILED,
                    reason_codes=(REASON_TRACE_CAPTURE_FAILED,),
                )
        reason_codes = tuple(sorted({reason for result in results.values() for reason in result.reason_codes}))
        return Phase1TraceCaptureReceipt(
            requested=self.phase1_trace_capture_service.enabled,
            results_by_package=results,
            reason_codes=reason_codes,
        )

    @staticmethod
    def _prospective_decision_as_of_trade_date(context: ProspectiveSelectionContext) -> date | None:
        raw = context.decision_clock_seed.get("decision_as_of_trade_date")
        if isinstance(raw, datetime):
            return None
        if isinstance(raw, date):
            return raw
        try:
            return date.fromisoformat(str(raw))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _prospective_hmm_effective_trade_date(context: ProspectiveSelectionContext | None) -> date | None:
        if context is None or context.capture_mode != EvidenceCaptureMode.PROSPECTIVE:
            return None
        raw = context.decision_clock_seed.get("effective_entry_trade_date")
        if isinstance(raw, date):
            return raw
        try:
            return date.fromisoformat(str(raw))
        except (TypeError, ValueError):
            # The assembler will return an explicit decision-clock capture failure.
            return None

    @staticmethod
    def release_selection_runtime_config(runtime_release: StrategyRuntimeRelease) -> dict[str, Any]:
        """Return the broker-neutral selection config persisted with a runtime release."""

        release_config = (
            runtime_release.release_config_json if isinstance(runtime_release.release_config_json, dict) else {}
        )
        metadata = release_config.get("metadata") if isinstance(release_config.get("metadata"), dict) else {}
        candidate = StrategyPackageSelectionService._first_release_selection_config(
            (
                ("release_config_json.selection_runtime_config", release_config.get("selection_runtime_config")),
                ("release_config_json.metadata.selection_runtime_config", metadata.get("selection_runtime_config")),
            ),
            runtime_release=runtime_release,
        )
        if candidate is None:
            candidate = StrategyPackageSelectionService._first_release_selection_artifact_config(
                (
                    ("release_config_json.selection_artifact_config", release_config.get("selection_artifact_config")),
                    ("release_config_json.selection_artifact", release_config.get("selection_artifact")),
                    (
                        "release_config_json.metadata.selection_artifact_config",
                        metadata.get("selection_artifact_config"),
                    ),
                    ("release_config_json.metadata.selection_artifact", metadata.get("selection_artifact")),
                ),
                runtime_release=runtime_release,
            )
        if candidate is None:
            return {}
        assert_selection_only_payload_boundary(
            candidate,
            context={
                "release_id": runtime_release.release_id,
                "package_id": runtime_release.package_id,
                "path": "strategy_runtime_release.selection_runtime_config",
            },
        )
        return candidate

    @staticmethod
    def _first_release_selection_config(
        candidates: tuple[tuple[str, Any], ...],
        *,
        runtime_release: StrategyRuntimeRelease,
    ) -> dict[str, Any] | None:
        for source, value in candidates:
            if value is None:
                continue
            if not isinstance(value, dict):
                raise RuntimeConfigInvalidError(
                    "StrategyRuntimeRelease selection_runtime_config must be an object",
                    context={
                        "release_id": runtime_release.release_id,
                        "package_id": runtime_release.package_id,
                        "source": source,
                        "selection_runtime_config_type": type(value).__name__,
                    },
                )
            return deepcopy(value)
        return None

    @staticmethod
    def _first_release_selection_artifact_config(
        candidates: tuple[tuple[str, Any], ...],
        *,
        runtime_release: StrategyRuntimeRelease,
    ) -> dict[str, Any] | None:
        for source, value in candidates:
            if value is None:
                continue
            if not isinstance(value, dict):
                raise RuntimeConfigInvalidError(
                    "StrategyRuntimeRelease selection_artifact_config must be an object",
                    context={
                        "release_id": runtime_release.release_id,
                        "package_id": runtime_release.package_id,
                        "source": source,
                        "selection_artifact_config_type": type(value).__name__,
                    },
                )
            return {"selection_artifact_config": deepcopy(value)}
        return None

    @staticmethod
    def _merge_selection_runtime_config(
        release_config: dict[str, Any],
        request_config: dict[str, Any],
    ) -> dict[str, Any]:
        merged = deepcopy(release_config)
        for key, value in request_config.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                nested = deepcopy(merged[key])
                nested.update(deepcopy(value))
                merged[key] = nested
            else:
                merged[key] = deepcopy(value)
        return merged

    @staticmethod
    def _attach_runtime_release_binding(
        runtime_config: dict[str, Any],
        runtime_release: StrategyRuntimeRelease,
        *,
        package_ids: list[str],
    ) -> dict[str, Any]:
        if package_ids and (len(package_ids) != 1 or package_ids[0] != runtime_release.package_id):
            raise RuntimeConfigInvalidError(
                "StrategyRuntimeRelease can only bind selection for its own single StrategyPackage",
                context={"release_package_id": runtime_release.package_id, "package_ids": package_ids},
            )
        release_selection_config = StrategyPackageSelectionService.release_selection_runtime_config(runtime_release)
        config = StrategyPackageSelectionService._merge_selection_runtime_config(
            release_selection_config,
            dict(runtime_config),
        )
        existing = config.get(RUNTIME_PROFILE_BINDING_KEY)
        if isinstance(existing, dict):
            existing_version = str(existing.get("profile_version_id") or "")
            existing_hash = str(existing.get("config_sha256") or "")
            if (
                existing_version
                and existing_version != runtime_release.runtime_profile_version_id
                or existing_hash
                and existing_hash != runtime_release.runtime_profile_sha256
            ):
                raise RuntimeConfigInvalidError(
                    "runtime_profile_binding conflicts with StrategyRuntimeRelease runtime profile",
                    context={
                        "runtime_profile_binding": existing,
                        "release_id": runtime_release.release_id,
                        "release_runtime_profile_version_id": runtime_release.runtime_profile_version_id,
                        "release_runtime_profile_hash": runtime_release.runtime_profile_sha256,
                    },
                )
        config[RUNTIME_PROFILE_BINDING_KEY] = {
            "source": "package_runtime_release",
            "release_id": runtime_release.release_id,
            "release_hash": runtime_release.release_hash,
            "profile_id": runtime_release.runtime_profile_id,
            "profile_version_id": runtime_release.runtime_profile_version_id,
            "config_sha256": runtime_release.runtime_profile_sha256,
            "trade_enabled": True,
        }
        config["strategy_runtime_release"] = {
            "release_id": runtime_release.release_id,
            "release_hash": runtime_release.release_hash,
            "package_id": runtime_release.package_id,
            "manifest_sha256": runtime_release.manifest_sha256,
        }
        return config

    @staticmethod
    def _validate_request_shape(*, package_ids: list[str], mode: SelectionMode) -> None:
        if not package_ids:
            raise RuntimeConfigInvalidError("selection run requires package_ids")
        if len(set(package_ids)) != len(package_ids):
            raise RuntimeConfigInvalidError("selection run package_ids must be unique")
        if mode == SelectionMode.SINGLE_PACKAGE and len(package_ids) != 1:
            raise RuntimeConfigInvalidError("single package selection requires exactly one package")
        if (
            mode in {SelectionMode.INTERSECTION, SelectionMode.UNION, SelectionMode.WEIGHTED_FUSION}
            and len(package_ids) < 2
        ):
            raise RuntimeConfigInvalidError("package aggregation requires at least two packages")

    def _require_data_ready(self, *, trade_date: date, runtime_config: dict[str, Any]) -> None:
        if parse_selection_runtime_profile(runtime_config).tradability.exclude_suspended:
            self.refresh_audit.require_success(dataset="suspend_d", trade_date=trade_date)
        artifact_config = self.selection_artifact_config(runtime_config)
        for dataset in artifact_config.get("required_cutoff_audit_datasets") or []:
            cutoff = artifact_config.get("cutoff_date")
            if not cutoff:
                raise RuntimeConfigInvalidError(
                    "required_cutoff_audit_datasets requires selection_artifact_config.cutoff_date",
                    context={"required_cutoff_audit_datasets": artifact_config.get("required_cutoff_audit_datasets")},
                )
            self.refresh_audit.require_success(dataset=str(dataset), trade_date=date.fromisoformat(str(cutoff)))

    def _prepare_package_runtime_configs(
        self,
        *,
        package_ids: list[str],
        config: dict[str, Any],
        trade_date: date,
        data_source: str,
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        records_by_id: dict[str, Any] = {}
        package_configs: dict[str, dict[str, Any]] = {}
        package_health: dict[str, dict[str, Any]] = {}
        for package_id in package_ids:
            record = self.package_repository.get(package_id)
            manifest = record.current_manifest()
            raw_package_config = self._package_runtime_config(config, package_id)
            variant = self._resolve_runtime_variant(record, raw_package_config)
            raw_package_config = apply_runtime_variant_config(raw_package_config, variant)
            raw_package_config.pop("runtime_variant_id", None)
            package_config = self._normalize_package_runtime_config(
                manifest=manifest,
                runtime_config=raw_package_config,
                package_id=package_id,
            )
            package_config = refresh_generated_runtime_profile_binding(package_config)
            validate_runtime_profile_binding(
                package_config,
                context={"package_id": package_id, "path": "strategy_package_selection.package_runtime_config"},
                require_trade_enabled=not is_non_trading_runtime_config(package_config),
            )
            health = self.package_health_service.summarize(
                record,
                runtime_config=package_config,
                trade_date=trade_date,
                data_source=data_source,
            )
            records_by_id[package_id] = record
            package_configs[package_id] = package_config
            package_health[package_id] = health
        return records_by_id, package_configs, package_health

    def _normalize_package_runtime_config(
        self,
        *,
        manifest: Any,
        runtime_config: dict[str, Any],
        package_id: str,
    ) -> dict[str, Any]:
        if not self._st_pit_authoritative(runtime_config):
            return normalize_runtime_config_with_backtest_contract(
                manifest,
                runtime_config,
                context={"package_id": package_id, "check": "strategy_package_selection"},
                include_contract=True,
            )

        contract_input, display_top_n = self._contract_input_with_display_top_n(runtime_config)
        normalized = normalize_runtime_config_with_backtest_contract(
            manifest,
            contract_input,
            context={"package_id": package_id, "check": "strategy_package_selection"},
            include_contract=True,
            inherit_source_defaults=False,
        )
        normalized["st_pit_authoritative"] = True
        if display_top_n is not None:
            normalized["display_top_n"] = self._validate_display_top_n(display_top_n, package_id=package_id)
        return normalized

    @staticmethod
    def _st_pit_authoritative(runtime_config: dict[str, Any]) -> bool:
        return bool(runtime_config.get("st_pit_authoritative") or runtime_config.get("enforce_st_pit_contract"))

    @staticmethod
    def _contract_input_with_display_top_n(runtime_config: dict[str, Any]) -> tuple[dict[str, Any], Any | None]:
        config = deepcopy(runtime_config)
        display_top_n = config.get("display_top_n")
        if display_top_n is None and "top_k" in config:
            display_top_n = config["top_k"]
        config.pop("top_k", None)

        raw_profile = config.get("runtime_profile")
        if raw_profile is None:
            profile: dict[str, Any] = {}
        elif isinstance(raw_profile, dict):
            profile = dict(raw_profile)
        else:
            raise RuntimeConfigInvalidError(
                "runtime_config.runtime_profile must be an object",
                context={"runtime_profile_type": type(raw_profile).__name__},
            )

        selection_payload = dict(profile.get("selection") or {})
        if display_top_n is None and "top_k" in selection_payload:
            display_top_n = selection_payload["top_k"]
        selection_payload.pop("top_k", None)
        if selection_payload:
            profile["selection"] = selection_payload
        else:
            profile.pop("selection", None)
        config["runtime_profile"] = profile
        return config, display_top_n

    @staticmethod
    def _validate_display_top_n(value: Any, *, package_id: str) -> int:
        try:
            display_top_n = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeConfigInvalidError(
                "selection display_top_n must be an integer",
                context={"package_id": package_id, "display_top_n": value},
            ) from exc
        if display_top_n <= 0 or display_top_n > 50:
            raise RuntimeConfigInvalidError(
                "selection display_top_n must be between 1 and 50",
                context={"package_id": package_id, "display_top_n": display_top_n, "max_display_top_n": 50},
            )
        return display_top_n

    @staticmethod
    def _package_runtime_config(config: dict[str, Any], package_id: str) -> dict[str, Any]:
        package_config = config.get(package_id)
        if isinstance(package_config, dict):
            merged = dict(config)
            merged.update(package_config)
            if RUNTIME_PROFILE_BINDING_KEY in config and RUNTIME_PROFILE_BINDING_KEY not in package_config:
                merged[RUNTIME_PROFILE_BINDING_KEY] = config[RUNTIME_PROFILE_BINDING_KEY]
            return merged
        return config

    def _resolve_runtime_variant(self, record: Any, runtime_config: dict[str, Any]) -> Any | None:
        raw_id = runtime_config.get("runtime_variant_id")
        if raw_id is None:
            per_package = runtime_config.get(str(record.package_id))
            if isinstance(per_package, dict):
                raw_id = per_package.get("runtime_variant_id")
        if raw_id is None:
            return None
        variant_id = str(raw_id).strip()
        if not variant_id:
            raise RuntimeConfigInvalidError(
                "runtime_variant_id cannot be empty",
                context={"package_id": record.package_id},
            )
        variant = self.package_repository.get_runtime_variant(record.package_id, variant_id)
        if variant.validation_status != RuntimeVariantValidationStatus.VALIDATION_PASSED:
            raise RuntimeConfigInvalidError(
                "runtime variant must be validated before Selection Center use",
                context={
                    "package_id": record.package_id,
                    "runtime_variant_id": variant.variant_id,
                    "validation_status": variant.validation_status.value,
                },
            )
        if variant.manifest_sha256 != record.manifest_sha256:
            raise RuntimeConfigInvalidError(
                "runtime variant manifest hash does not match StrategyPackage",
                context={
                    "package_id": record.package_id,
                    "runtime_variant_id": variant.variant_id,
                    "variant_manifest_sha256": variant.manifest_sha256,
                    "package_manifest_sha256": record.manifest_sha256,
                },
            )
        return variant

    @staticmethod
    def _top_k_for_package(
        manifest: Any,
        package_profile: Any,
        global_profile: Any,
        package_config: dict[str, Any] | None = None,
    ) -> int:
        configured = package_profile.selection.top_k or global_profile.selection.top_k
        if configured is None:
            configured = StrategyPackageSelectionService._contract_top_k(package_config)
        if configured is None:
            raise RuntimeConfigInvalidError(
                "selection runtime_profile.selection.top_k is required; StrategyPackage manifest cannot provide runtime top_k",
                context={
                    "package_id": manifest.package_id,
                    "manifest_version": getattr(manifest, "manifest_version", None),
                },
            )
        try:
            top_k = int(configured)
        except (TypeError, ValueError) as exc:
            raise RuntimeConfigInvalidError(
                "selection top_k must be an integer",
                context={"package_id": manifest.package_id, "top_k": configured},
            ) from exc
        if top_k <= 0 or top_k > 50:
            raise RuntimeConfigInvalidError(
                "selection top_k must be between 1 and 50",
                context={"package_id": manifest.package_id, "top_k": top_k, "max_top_k": 50},
            )
        return top_k

    @staticmethod
    def _contract_top_k(package_config: dict[str, Any] | None) -> int | None:
        contract = (package_config or {}).get("qe_backtest_runtime_contract")
        if not isinstance(contract, dict):
            return None
        strategy = contract.get("portfolio_strategy")
        if not isinstance(strategy, dict):
            return None
        params = strategy.get("params")
        if not isinstance(params, dict) or params.get("topk") is None:
            return None
        return int(params["topk"])

    def resolve_point_in_time_context(
        self,
        *,
        trade_date: date,
        pit_mode: str,
        explicit_cutoff_date: date | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_pit_mode(pit_mode)
        if normalized == "NONE":
            return {
                "pit_mode": "NONE",
                "trade_date": trade_date.isoformat(),
                "cutoff_date": explicit_cutoff_date.isoformat() if explicit_cutoff_date else None,
                "score_trade_date": explicit_cutoff_date.isoformat()
                if explicit_cutoff_date
                else trade_date.isoformat(),
                "reference_price_trade_date": explicit_cutoff_date.isoformat()
                if explicit_cutoff_date
                else trade_date.isoformat(),
            }
        effective_trade_date = self._selection_effective_trade_date(
            trade_date=trade_date,
            explicit_cutoff_date=explicit_cutoff_date,
        )
        cutoff = explicit_cutoff_date or self._previous_trading_day(effective_trade_date)
        if cutoff >= effective_trade_date:
            raise RuntimeConfigInvalidError(
                "point-in-time selection cutoff_date must be before trade_date",
                context={
                    "trade_date": trade_date.isoformat(),
                    "effective_trade_date": effective_trade_date.isoformat(),
                    "cutoff_date": cutoff.isoformat(),
                    "pit_mode": normalized,
                },
            )
        return {
            "pit_mode": normalized,
            "trade_date": trade_date.isoformat(),
            "requested_trade_date": trade_date.isoformat(),
            "effective_trade_date": effective_trade_date.isoformat(),
            "cutoff_date": cutoff.isoformat(),
            "score_trade_date": cutoff.isoformat(),
            "reference_price_trade_date": cutoff.isoformat(),
            "calendar_source": "market.trading_calendar",
        }

    def _apply_point_in_time_selection_config(self, config: dict[str, Any], *, trade_date: date) -> dict[str, Any]:
        artifact_config = self.selection_artifact_config(config)
        pit_mode = self.selection_pit_mode(config, artifact_config=artifact_config)
        if pit_mode == "NONE":
            cutoff = artifact_config.get("cutoff_date")
            if cutoff:
                self.parse_selection_cutoff_date(artifact_config, trade_date=trade_date, strict_before=True)
            return config
        explicit_cutoff = None
        if self.is_fixed_cutoff_replay_config(config, artifact_config=artifact_config):
            explicit_cutoff = self.parse_selection_cutoff_date(
                artifact_config,
                trade_date=trade_date,
                strict_before=True,
            )
        context = self.resolve_point_in_time_context(
            trade_date=trade_date,
            pit_mode=pit_mode,
            explicit_cutoff_date=explicit_cutoff,
        )
        updated = dict(config)
        updated_artifact = dict(artifact_config)
        requested_cutoff = artifact_config.get("cutoff_date")
        if requested_cutoff and explicit_cutoff is None and str(requested_cutoff) != context["cutoff_date"]:
            context["requested_cutoff_date"] = str(requested_cutoff)
            context["cutoff_override_reason"] = "daily_previous_trading_day_resolution"
        updated_artifact["pit_mode"] = context["pit_mode"]
        updated_artifact["cutoff_date"] = context["cutoff_date"]
        updated["selection_artifact_config"] = updated_artifact
        if "selection_artifact" in updated:
            updated["selection_artifact"] = updated_artifact
        updated["point_in_time_context"] = context
        return updated

    @staticmethod
    def selection_artifact_config(config: dict[str, Any]) -> dict[str, Any]:
        artifact_config = config.get("selection_artifact_config")
        if artifact_config is None:
            artifact_config = config.get("selection_artifact")
        if artifact_config is None:
            return {}
        if not isinstance(artifact_config, dict):
            raise RuntimeConfigInvalidError(
                "runtime_config.selection_artifact_config must be an object",
                context={"selection_artifact_config_type": type(artifact_config).__name__},
            )
        return artifact_config

    @staticmethod
    def selection_pit_mode(config: dict[str, Any], artifact_config: dict[str, Any] | None = None) -> str:
        artifact = artifact_config or StrategyPackageSelectionService.selection_artifact_config(config)
        raw_mode = artifact.get("pit_mode") or config.get("pit_mode")
        if raw_mode is None:
            cutoff_policy = artifact.get("cutoff_policy")
            if cutoff_policy is not None and not StrategyPackageSelectionService.is_fixed_cutoff_replay_config(
                config,
                artifact_config=artifact,
            ):
                raw_mode = cutoff_policy
        if (
            raw_mode is None
            and artifact.get("cutoff_date")
            and StrategyPackageSelectionService.is_fixed_cutoff_replay_config(
                config,
                artifact_config=artifact,
            )
        ):
            raw_mode = "PREVIOUS_TRADING_DAY_CLOSE"
        if raw_mode is None:
            raw_mode = "NONE"
        return StrategyPackageSelectionService._normalize_pit_mode(str(raw_mode))

    @staticmethod
    def is_fixed_cutoff_replay_config(
        config: dict[str, Any],
        *,
        artifact_config: dict[str, Any] | None = None,
    ) -> bool:
        artifact = artifact_config or StrategyPackageSelectionService.selection_artifact_config(config)
        policy_values = (
            artifact.get("cutoff_policy"),
            artifact.get("cutoff_mode"),
            artifact.get("selection_cutoff_policy"),
            config.get("cutoff_policy"),
        )
        fixed_policies = {
            "PINNED",
            "FIXED",
            "FIXED_CUTOFF",
            "FIXED_REPLAY",
            "HISTORICAL_REPLAY",
            "FIXED_HISTORICAL_REPLAY",
            "PINNED_HISTORICAL_REPLAY",
        }
        if any(str(value or "").strip().upper() in fixed_policies for value in policy_values):
            return True
        fixed_flags = (
            "fixed_cutoff",
            "fixed_replay",
            "allow_fixed_cutoff",
            "historical_replay",
            "pinned_cutoff",
        )
        return any(bool(artifact.get(flag) or config.get(flag)) for flag in fixed_flags)

    @staticmethod
    def _normalize_pit_mode(pit_mode: str) -> str:
        text = str(pit_mode or "NONE").strip().upper()
        aliases = {
            "": "NONE",
            "NONE": "NONE",
            "DISABLED": "NONE",
            "PREVIOUS_TRADING_DAY_CUTOFF": "PREVIOUS_TRADING_DAY_CLOSE",
            "PREVIOUS_TRADING_DAY": "PREVIOUS_TRADING_DAY_CLOSE",
            "PREVIOUS_TRADING_DAY_CLOSE": "PREVIOUS_TRADING_DAY_CLOSE",
            "PREV_TRADING_DAY_CLOSE": "PREVIOUS_TRADING_DAY_CLOSE",
        }
        normalized = aliases.get(text)
        if normalized is None:
            raise RuntimeConfigInvalidError(
                "unsupported point-in-time selection mode",
                context={"pit_mode": pit_mode, "supported": ["NONE", "PREVIOUS_TRADING_DAY_CLOSE"]},
            )
        return normalized

    def _previous_trading_day(self, trade_date: date) -> date:
        lookup_start = trade_date - timedelta(days=31)
        days = self.calendar_provider.list_trading_days(lookup_start, trade_date - timedelta(days=1))
        if not days:
            raise TradingCalendarUnavailableError(
                "trading calendar has no previous trading day for point-in-time selection",
                context={"trade_date": trade_date.isoformat(), "lookup_start": lookup_start.isoformat()},
            )
        return days[-1]

    def _selection_effective_trade_date(self, *, trade_date: date, explicit_cutoff_date: date | None) -> date:
        if explicit_cutoff_date is not None:
            return trade_date
        try:
            self.calendar_provider.ensure_trading_day(trade_date)
            return trade_date
        except DataUnavailableError:
            return self._latest_trading_day_on_or_before(trade_date)

    def _latest_trading_day_on_or_before(self, trade_date: date) -> date:
        lookup_start = trade_date - timedelta(days=31)
        days = self.calendar_provider.list_trading_days(lookup_start, trade_date)
        eligible = [day for day in days if day <= trade_date]
        if not eligible:
            raise TradingCalendarUnavailableError(
                "trading calendar has no completed trading day on or before requested selection date",
                context={"trade_date": trade_date.isoformat(), "lookup_start": lookup_start.isoformat()},
            )
        return eligible[-1]

    @staticmethod
    def parse_selection_cutoff_date(
        artifact_config: dict[str, Any],
        *,
        trade_date: date,
        strict_before: bool = False,
    ) -> date | None:
        raw = artifact_config.get("cutoff_date")
        if raw is None or raw == "":
            return None
        try:
            parsed = date.fromisoformat(str(raw))
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "selection_artifact_config.cutoff_date must be YYYY-MM-DD",
                context={"cutoff_date": raw},
            ) from exc
        if parsed > trade_date or (strict_before and parsed >= trade_date):
            raise RuntimeConfigInvalidError(
                "selection_artifact_config.cutoff_date must be before trade_date",
                context={"trade_date": trade_date.isoformat(), "cutoff_date": parsed.isoformat()},
            )
        return parsed

    def _build_evidence_by_package(
        self,
        *,
        package_ids: list[str],
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
        package_runtime_configs: dict[str, dict[str, Any]],
        package_results: dict[str, list[SelectionCandidate]],
        excluded_results: dict[str, list[SelectionExclusion]],
        manifest_sha256_by_package: dict[str, str],
        runtime_release: StrategyRuntimeRelease | None,
        created_by: str | None,
    ) -> dict[str, DailySelectionEvidence]:
        evidence_by_package: dict[str, DailySelectionEvidence] = {}
        for package_id in package_ids:
            package_config = package_runtime_configs[package_id]
            release_ref = runtime_release if runtime_release and runtime_release.package_id == package_id else None
            evidence = self._build_evidence(
                package_id=package_id,
                manifest_sha256=manifest_sha256_by_package[package_id],
                trade_date=trade_date,
                data_source=data_source,
                runtime_config=package_config,
                selected=package_results.get(package_id, []),
                excluded=excluded_results.get(package_id, []),
                runtime_release=release_ref,
                created_by=created_by,
            )
            evidence_by_package[package_id] = self.repository.save_daily_selection_evidence(evidence)
        return evidence_by_package

    def _build_operational_evidence_by_package(
        self,
        *,
        package_ids: list[str],
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
        package_runtime_configs: dict[str, dict[str, Any]],
        package_results: dict[str, list[SelectionCandidate]],
        excluded_results: dict[str, list[SelectionExclusion]],
        manifest_sha256_by_package: dict[str, str],
        runtime_release: StrategyRuntimeRelease | None,
        created_by: str | None,
        prospective_context: ProspectiveSelectionContext | None,
        artifact_by_package: dict[str, Any],
        stage_trace_by_package: dict[str, SelectionStageTrace],
        candidate_outcome_by_package: dict[str, str],
    ) -> tuple[dict[str, DailySelectionEvidence], EvidenceCaptureReceipt]:
        """Persist exactly one operational DSE per package without changing candidates.

        A prospective assembly failure is visible in the capture receipt, while
        the already-calculated selection remains available through the legacy
        operational evidence for that package.
        """
        if prospective_context is None or prospective_context.capture_mode != EvidenceCaptureMode.PROSPECTIVE:
            return (
                self._build_evidence_by_package(
                    package_ids=package_ids,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=runtime_config,
                    package_runtime_configs=package_runtime_configs,
                    package_results=package_results,
                    excluded_results=excluded_results,
                    manifest_sha256_by_package=manifest_sha256_by_package,
                    runtime_release=runtime_release,
                    created_by=created_by,
                ),
                ProspectiveSelectionEvidenceAssembler.not_requested_receipt(),
            )

        assembler = ProspectiveSelectionEvidenceAssembler()
        captured: dict[str, DailySelectionEvidence] = {}
        failures: dict[str, list[str]] = {}
        for package_id in package_ids:
            try:
                record = self.package_repository.get(package_id)
                evidence = assembler.assemble(
                    context=prospective_context,
                    manifest=record.current_manifest(),
                    selection_run_id=str(prospective_context.selection_run_id or ""),
                    artifact=artifact_by_package[package_id],
                    stage_trace=stage_trace_by_package[package_id],
                    runtime_config=package_runtime_configs[package_id],
                    selected=package_results.get(package_id, []),
                    excluded=excluded_results.get(package_id, []),
                    created_by=created_by,
                    candidate_outcome=candidate_outcome_by_package[package_id],
                )
                captured[package_id] = self.repository.save_daily_selection_evidence(evidence)
            except Exception as exc:  # Capture must never rewrite an already-computed selection result.
                logger.exception(
                    "prospective selection evidence capture failed",
                    extra={
                        "selection_run_id": prospective_context.selection_run_id,
                        "package_id": package_id,
                        "manifest_sha256": manifest_sha256_by_package.get(package_id),
                        "capture_status": "FAILED",
                        "reason_code": ProspectiveSelectionEvidenceAssembler.failure_reason(exc),
                    },
                )
                failures[package_id] = [ProspectiveSelectionEvidenceAssembler.failure_reason(exc)]

        if not failures:
            return captured, ProspectiveSelectionEvidenceAssembler.complete_receipt(
                {package_id: evidence.evidence_id for package_id, evidence in captured.items()}
            )
        legacy_package_ids = [package_id for package_id in package_ids if package_id in failures]
        legacy = self._build_evidence_by_package(
            package_ids=legacy_package_ids,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
            package_runtime_configs=package_runtime_configs,
            package_results=package_results,
            excluded_results=excluded_results,
            manifest_sha256_by_package=manifest_sha256_by_package,
            runtime_release=runtime_release,
            created_by=created_by,
        )
        combined = {**legacy, **captured}
        return (
            {package_id: combined[package_id] for package_id in package_ids},
            ProspectiveSelectionEvidenceAssembler.failed_receipt(failures_by_package=failures),
        )

    def _build_evidence(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
        selected: list[SelectionCandidate],
        excluded: list[SelectionExclusion],
        runtime_release: StrategyRuntimeRelease | None,
        created_by: str | None,
    ) -> DailySelectionEvidence:
        binding = self._runtime_profile_binding_for_evidence(runtime_config, package_id=package_id)
        cutoff_date = self._evidence_cutoff_date(runtime_config, trade_date=trade_date)
        release_id = (
            runtime_release.release_id
            if runtime_release
            else self._runtime_release_config(runtime_config).get("release_id")
        )
        release_hash = (
            runtime_release.release_hash
            if runtime_release
            else self._runtime_release_config(runtime_config).get("release_hash")
        )
        runtime_profile_version_id = (
            runtime_release.runtime_profile_version_id
            if runtime_release
            else str(binding.get("profile_version_id") or "")
        )
        runtime_profile_hash = (
            runtime_release.runtime_profile_sha256 if runtime_release else str(binding.get("config_sha256") or "")
        )
        payload = {
            "schema_version": "daily_selection_evidence_v1",
            "target_trade_date": trade_date.isoformat(),
            "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
            "package_id": package_id,
            "manifest_sha256": manifest_sha256,
            "release_id": release_id,
            "release_hash": release_hash,
            "runtime_profile_version_id": runtime_profile_version_id,
            "runtime_profile_hash": runtime_profile_hash,
            "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
            "data_source": data_source,
            "runtime_profile": runtime_config.get("runtime_profile"),
            "runtime_profile_binding": binding,
            "point_in_time_context": runtime_config.get("point_in_time_context"),
            "selection_artifact_config": self.selection_artifact_config(runtime_config),
            "selected_candidates": [self._candidate_payload(item) for item in selected],
            "excluded_candidates": [item.model_dump(mode="json") for item in excluded],
        }
        if CANONICAL_PIT_RUNTIME_LEASE_KEY in runtime_config:
            payload[CANONICAL_PIT_RUNTIME_LEASE_KEY] = require_canonical_pit_runtime_binding(
                runtime_config,
                trade_date=trade_date,
            ).as_dict()
        assert_selection_only_payload_boundary(payload, context={"package_id": package_id})
        artifact_hash = canonical_json_sha256(payload)
        return DailySelectionEvidence(
            evidence_id=f"dse_{artifact_hash[:16]}",
            target_trade_date=trade_date,
            cutoff_date=cutoff_date,
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            release_id=release_id,
            release_hash=release_hash,
            runtime_profile_version_id=runtime_profile_version_id,
            runtime_profile_hash=runtime_profile_hash,
            source_type=AUTHORITATIVE_SELECTION_SOURCE_TYPE,
            data_source=data_source,
            candidate_count=len(selected),
            excluded_count=len(excluded),
            artifact_hash=artifact_hash,
            evidence_payload_json=payload,
            created_by=created_by,
        )

    @staticmethod
    def _runtime_profile_binding_for_evidence(runtime_config: dict[str, Any], *, package_id: str) -> dict[str, Any]:
        binding = runtime_config.get(RUNTIME_PROFILE_BINDING_KEY)
        if not isinstance(binding, dict):
            return {
                "source": "generated_effective_runtime_config",
                "profile_version_id": GENERATED_RUNTIME_PROFILE_VERSION_ID,
                "config_sha256": runtime_profile_config_sha256(runtime_config),
                "trade_enabled": True,
                "generated_for": "daily_selection_evidence",
                "package_id": package_id,
            }
        if binding.get("source") in {"platform_default", "generated_effective_runtime_config"}:
            effective = dict(binding)
            effective["config_sha256"] = runtime_profile_config_sha256(runtime_config)
            return effective
        return validate_runtime_profile_binding(
            runtime_config,
            context={"package_id": package_id, "path": "strategy_package_selection.evidence"},
            require_trade_enabled=not is_non_trading_runtime_config(runtime_config),
        )

    @staticmethod
    def _runtime_release_config(runtime_config: dict[str, Any]) -> dict[str, Any]:
        for key in ("strategy_runtime_release", "runtime_release"):
            value = runtime_config.get(key)
            if isinstance(value, dict):
                return value
        binding = runtime_config.get(RUNTIME_PROFILE_BINDING_KEY)
        if isinstance(binding, dict) and binding.get("release_id"):
            return {"release_id": binding.get("release_id"), "release_hash": binding.get("release_hash")}
        return {}

    @staticmethod
    def _evidence_cutoff_date(runtime_config: dict[str, Any], *, trade_date: date) -> date | None:
        pit_context = runtime_config.get("point_in_time_context")
        raw = pit_context.get("cutoff_date") if isinstance(pit_context, dict) else None
        if raw is None:
            raw = StrategyPackageSelectionService.selection_artifact_config(runtime_config).get("cutoff_date")
        if raw is None or raw == "":
            return None
        return StrategyPackageSelectionService.parse_selection_cutoff_date(
            {"cutoff_date": raw},
            trade_date=trade_date,
            strict_before=True,
        )

    @staticmethod
    def _candidate_payload(candidate: SelectionCandidate) -> dict[str, Any]:
        return candidate.model_dump(mode="json")

    @staticmethod
    def _attach_evidence_refs(
        runtime_config: dict[str, Any],
        evidence_by_package: dict[str, DailySelectionEvidence],
        *,
        capture_receipt: EvidenceCaptureReceipt,
    ) -> dict[str, Any]:
        config = dict(runtime_config)
        config["daily_selection_evidence"] = {
            "schema_version": "daily_selection_evidence_refs_v2",
            "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
            "evidence_ids_by_package": {
                package_id: evidence.evidence_id for package_id, evidence in evidence_by_package.items()
            },
            "artifact_hash_by_package": {
                package_id: evidence.artifact_hash for package_id, evidence in evidence_by_package.items()
            },
            "evidence_schema_version_by_package": {
                package_id: str(
                    (evidence.evidence_payload_json or {}).get("schema_version") or "daily_selection_evidence_v1"
                )
                for package_id, evidence in evidence_by_package.items()
            },
            "evidence_capture_status": capture_receipt.status.value,
            "evidence_capture_requested": capture_receipt.requested,
            "evidence_reason_codes": list(capture_receipt.reason_codes),
        }
        return config
