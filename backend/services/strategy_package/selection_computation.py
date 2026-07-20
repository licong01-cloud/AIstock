"""Repository-free StrategyPackage candidate computation shared by consumers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from math import isfinite
from typing import TYPE_CHECKING, Any, Mapping, Protocol

from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion, SelectionMode
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName,
    REASON_VALID_NO_CANDIDATE,
    RiskAdjustmentResult,
    SelectionStageTrace,
    StageEvidenceReceipt,
    StageReceiptStatus,
    TradabilityResult,
    build_stage_receipt,
    canonical_candidate_rows,
    canonical_evidence_json_sha256,
)
from backend.services.selection_center.runtime_profile import RuntimeRiskPolicyProfile, SelectionRuntimeProfile
from backend.services.strategy_package.models import AlphaMode
from backend.services.trading_core.errors import ArtifactGenerationFailedError, RuntimeConfigInvalidError

if TYPE_CHECKING:
    from backend.services.selection_center.risk_policy import RiskDecision


_SELECTION_SCORE_ARTIFACT_CONTRACT_V2 = "selection_score_artifact_v2"


@dataclass(frozen=True)
class PreparedPackageComponentLineageV1:
    component_id: str
    component_weight: float
    factor_ids: tuple[str, ...]
    score_normalization: str

    def __post_init__(self) -> None:
        if not str(self.component_id or "").strip():
            raise RuntimeConfigInvalidError("prepared component lineage requires component_id")
        if not isfinite(float(self.component_weight)) or float(self.component_weight) <= 0:
            raise RuntimeConfigInvalidError(
                "prepared component lineage requires a positive finite weight",
                context={"component_id": self.component_id, "component_weight": self.component_weight},
            )
        if not self.factor_ids or any(not str(item or "").strip() for item in self.factor_ids):
            raise RuntimeConfigInvalidError(
                "prepared component lineage requires non-empty factor_ids",
                context={"component_id": self.component_id},
            )
        if not str(self.score_normalization or "").strip():
            raise RuntimeConfigInvalidError(
                "prepared component lineage requires score_normalization",
                context={"component_id": self.component_id},
            )


@dataclass(frozen=True)
class SelectionArtifactHeaderV1:
    artifact_id: str
    artifact_sha256: str
    package_id: str
    manifest_sha256: str
    trade_date: date
    data_source: str
    runtime_config_hash: str
    artifact_payload_sha256: str | None = None
    artifact_contract_version: str | None = None
    artifact_input_context_hash: str | None = None
    source_revision_set_hash: str | None = None
    asset_closure_hash: str | None = None
    universe_identity_hash: str | None = None

    def __post_init__(self) -> None:
        if not str(self.artifact_id or "").strip():
            raise RuntimeConfigInvalidError("prepared selection artifact header requires artifact_id")
        if not str(self.package_id or "").strip():
            raise RuntimeConfigInvalidError("prepared selection artifact header requires package_id")
        if not str(self.data_source or "").strip():
            raise RuntimeConfigInvalidError("prepared selection artifact header requires data_source")
        if not isinstance(self.trade_date, date):
            raise RuntimeConfigInvalidError("prepared selection artifact header requires trade_date")
        for field_name in (
            "artifact_sha256",
            "manifest_sha256",
            "runtime_config_hash",
            "artifact_payload_sha256",
            "artifact_input_context_hash",
            "source_revision_set_hash",
            "asset_closure_hash",
            "universe_identity_hash",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _require_sha256(value, field_name=field_name, package_id="artifact_header")
        if self.artifact_contract_version not in {None, _SELECTION_SCORE_ARTIFACT_CONTRACT_V2}:
            raise RuntimeConfigInvalidError(
                "prepared selection artifact header has unsupported contract version",
                context={"artifact_contract_version": self.artifact_contract_version},
            )
        if self.artifact_contract_version is None and any(
            value is not None
            for value in (
                self.artifact_payload_sha256,
                self.artifact_input_context_hash,
                self.source_revision_set_hash,
                self.asset_closure_hash,
                self.universe_identity_hash,
            )
        ):
            raise RuntimeConfigInvalidError(
                "legacy selection artifact header cannot carry v2 identity fields",
                context={"artifact_id": self.artifact_id},
            )

    def stage_semantic_payload(self, *, package_id: str, manifest_sha256: str) -> dict[str, Any]:
        return {
            "package_id": package_id,
            "manifest_sha256": manifest_sha256,
            "artifact_id": self.artifact_id,
            "artifact_sha256": self.artifact_sha256,
            "artifact_payload_sha256": self.artifact_payload_sha256,
            "artifact_contract_version": self.artifact_contract_version,
        }


@dataclass(frozen=True)
class PreparedPackageSignalV1:
    package_id: str
    package_version: str
    manifest_sha256: str
    alpha_mode: AlphaMode
    component_lineage: tuple[PreparedPackageComponentLineageV1, ...]
    alpha_raw_candidates: tuple[SelectionCandidate, ...]
    hmm_adjusted_candidates: tuple[SelectionCandidate, ...]
    hmm_receipt: StageEvidenceReceipt
    hmm_metadata: Mapping[str, Any]
    artifact_header: SelectionArtifactHeaderV1
    input_context_hash: str | None = None
    source_revision_set_hash: str | None = None
    universe_identity_hash: str | None = None
    valid_no_candidate: bool = False
    no_candidate_reason: str | None = None

    def __post_init__(self) -> None:
        if not str(self.package_id or "").strip():
            raise RuntimeConfigInvalidError("prepared package signal requires package_id")
        if not str(self.package_version or "").strip():
            raise RuntimeConfigInvalidError(
                "prepared package signal requires package_version",
                context={"package_id": self.package_id},
            )
        _require_sha256(self.manifest_sha256, field_name="manifest_sha256", package_id=self.package_id)
        if self.artifact_header.package_id != self.package_id:
            raise RuntimeConfigInvalidError(
                "prepared signal artifact package identity mismatch",
                context={
                    "package_id": self.package_id,
                    "artifact_package_id": self.artifact_header.package_id,
                },
            )
        if self.artifact_header.manifest_sha256 != self.manifest_sha256:
            raise RuntimeConfigInvalidError(
                "prepared signal artifact manifest identity mismatch",
                context={
                    "package_id": self.package_id,
                    "manifest_sha256": self.manifest_sha256,
                    "artifact_manifest_sha256": self.artifact_header.manifest_sha256,
                },
            )
        component_ids = [item.component_id for item in self.component_lineage]
        if len(component_ids) != len(set(component_ids)):
            raise RuntimeConfigInvalidError(
                "prepared package signal component lineage must be unique",
                context={"package_id": self.package_id, "component_ids": component_ids},
            )
        if self.alpha_mode is AlphaMode.SINGLE_ALPHA and len(component_ids) != 1:
            raise RuntimeConfigInvalidError(
                "single-alpha prepared package signal requires exactly one component",
                context={"package_id": self.package_id, "component_count": len(component_ids)},
            )
        if self.alpha_mode is AlphaMode.MULTI_ALPHA and len(component_ids) < 2:
            raise RuntimeConfigInvalidError(
                "multi-alpha prepared package signal requires at least two components",
                context={"package_id": self.package_id, "component_count": len(component_ids)},
            )
        for field_name in ("input_context_hash", "source_revision_set_hash", "universe_identity_hash"):
            value = getattr(self, field_name)
            if value is not None:
                _require_sha256(value, field_name=field_name, package_id=self.package_id)
        if self.input_context_hash != self.artifact_header.artifact_input_context_hash:
            raise RuntimeConfigInvalidError(
                "prepared signal input context hash does not match artifact header",
                context={"package_id": self.package_id},
            )
        if self.source_revision_set_hash != self.artifact_header.source_revision_set_hash:
            raise RuntimeConfigInvalidError(
                "prepared signal source revision hash does not match artifact header",
                context={"package_id": self.package_id},
            )
        if self.universe_identity_hash != self.artifact_header.universe_identity_hash:
            raise RuntimeConfigInvalidError(
                "prepared signal universe identity hash does not match artifact header",
                context={"package_id": self.package_id},
            )
        if self.artifact_header.artifact_contract_version == _SELECTION_SCORE_ARTIFACT_CONTRACT_V2:
            missing_identity_fields = [
                field_name
                for field_name, value in (
                    ("artifact_payload_sha256", self.artifact_header.artifact_payload_sha256),
                    ("artifact_input_context_hash", self.artifact_header.artifact_input_context_hash),
                    ("source_revision_set_hash", self.artifact_header.source_revision_set_hash),
                    ("asset_closure_hash", self.artifact_header.asset_closure_hash),
                    ("universe_identity_hash", self.artifact_header.universe_identity_hash),
                )
                if value is None
            ]
            if missing_identity_fields:
                raise RuntimeConfigInvalidError(
                    "prepared v2 signal identity closure is incomplete",
                    context={"package_id": self.package_id, "missing_fields": missing_identity_fields},
                )
        if self.valid_no_candidate:
            if self.alpha_raw_candidates or self.hmm_adjusted_candidates:
                raise RuntimeConfigInvalidError(
                    "valid_no_candidate prepared signal cannot contain alpha or HMM candidates",
                    context={
                        "package_id": self.package_id,
                        "alpha_raw_candidate_count": len(self.alpha_raw_candidates),
                        "hmm_adjusted_candidate_count": len(self.hmm_adjusted_candidates),
                    },
                )
            if not str(self.no_candidate_reason or "").strip():
                raise RuntimeConfigInvalidError(
                    "valid_no_candidate prepared signal requires no_candidate_reason",
                    context={"package_id": self.package_id},
                )
        elif not self.hmm_adjusted_candidates:
            raise RuntimeConfigInvalidError(
                "prepared package signal requires candidates or valid_no_candidate",
                context={"package_id": self.package_id},
            )
        _validate_hmm_receipt_consistency(
            package_id=self.package_id,
            alpha_raw_candidates=self.alpha_raw_candidates,
            hmm_adjusted_candidates=self.hmm_adjusted_candidates,
            hmm_receipt=self.hmm_receipt,
            hmm_metadata=self.hmm_metadata,
        )


@dataclass(frozen=True)
class StrategyPackageSelectionComputationRequestV1:
    trade_date: date
    data_source: str
    selection_mode: SelectionMode
    ordered_package_ids: tuple[str, ...]
    package_runtime_profiles: Mapping[str, SelectionRuntimeProfile]
    package_runtime_profile_hashes: Mapping[str, str]
    package_top_k: Mapping[str, int]
    package_weights: Mapping[str, float] | None = None

    def __post_init__(self) -> None:
        if not str(self.data_source or "").strip():
            raise RuntimeConfigInvalidError("selection computation requires data_source")
        package_ids = tuple(str(item or "").strip() for item in self.ordered_package_ids)
        if not package_ids or any(not item for item in package_ids):
            raise RuntimeConfigInvalidError("selection computation requires package_ids")
        if len(package_ids) != len(set(package_ids)):
            raise RuntimeConfigInvalidError(
                "selection computation package_ids must be unique",
                context={"package_ids": list(package_ids)},
            )
        if self.selection_mode is SelectionMode.SINGLE_PACKAGE and len(package_ids) != 1:
            raise RuntimeConfigInvalidError("single package selection requires exactly one package")
        if self.selection_mode in {
            SelectionMode.INTERSECTION,
            SelectionMode.UNION,
            SelectionMode.WEIGHTED_FUSION,
        } and len(package_ids) < 2:
            raise RuntimeConfigInvalidError("package aggregation requires at least two packages")
        expected = set(package_ids)
        _require_exact_keys(self.package_runtime_profiles, expected, field_name="package_runtime_profiles")
        _require_exact_keys(
            self.package_runtime_profile_hashes,
            expected,
            field_name="package_runtime_profile_hashes",
        )
        _require_exact_keys(self.package_top_k, expected, field_name="package_top_k")
        for package_id in package_ids:
            _require_sha256(
                self.package_runtime_profile_hashes[package_id],
                field_name="package_runtime_profile_hash",
                package_id=package_id,
            )
            expected_profile_hash = selection_runtime_profile_sha256(self.package_runtime_profiles[package_id])
            if self.package_runtime_profile_hashes[package_id] != expected_profile_hash:
                raise RuntimeConfigInvalidError(
                    "selection runtime profile hash does not match profile payload",
                    context={
                        "package_id": package_id,
                        "expected_profile_hash": expected_profile_hash,
                        "actual_profile_hash": self.package_runtime_profile_hashes[package_id],
                    },
                )
            top_k = int(self.package_top_k[package_id])
            if top_k <= 0 or top_k > 50:
                raise RuntimeConfigInvalidError(
                    "selection top_k must be between 1 and 50",
                    context={"package_id": package_id, "top_k": top_k, "max_top_k": 50},
                )
        if self.selection_mode is SelectionMode.WEIGHTED_FUSION:
            if self.package_weights is None:
                raise RuntimeConfigInvalidError("weighted package fusion requires package_weights")
            _require_exact_keys(self.package_weights, expected, field_name="package_weights")
            for package_id, weight in self.package_weights.items():
                if not isfinite(float(weight)) or float(weight) <= 0:
                    raise RuntimeConfigInvalidError(
                        "package weights must be positive finite numbers",
                        context={"package_id": package_id, "weight": weight},
                    )


class RiskPolicyComputationProvider(Protocol):
    def evaluate(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        profile: RuntimeRiskPolicyProfile,
    ) -> dict[str, RiskDecision]: ...

    def apply_to_candidates_with_receipt(
        self,
        *,
        candidates: list[SelectionCandidate],
        decisions: dict[str, RiskDecision],
        trade_date: date,
        top_k: int,
        package_id: str,
        manifest_sha256: str,
        allow_empty: bool,
        profile: RuntimeRiskPolicyProfile,
    ) -> RiskAdjustmentResult: ...


class TradabilityComputationProvider(Protocol):
    def select_top_k_with_receipt(
        self,
        *,
        candidates: list[SelectionCandidate],
        top_k: int,
        trade_date: date,
        package_id: str,
        manifest_sha256: str,
    ) -> TradabilityResult: ...

    def filter_candidates_with_receipt(
        self,
        *,
        candidates: list[SelectionCandidate],
        trade_date: date,
        top_k: int,
        package_id: str,
        manifest_sha256: str,
        enabled: bool,
        industry_blacklist: list[str],
        allow_empty: bool,
    ) -> TradabilityResult: ...


@dataclass(frozen=True)
class StrategyPackageSelectionReadOnlyProvidersV1:
    risk_policy: RiskPolicyComputationProvider
    tradability: TradabilityComputationProvider

    def __post_init__(self) -> None:
        if self.risk_policy is None or self.tradability is None:
            raise RuntimeConfigInvalidError("selection computation requires explicit read-only providers")


@dataclass(frozen=True)
class StrategyPackageSelectionComputationResultV1:
    package_results: Mapping[str, tuple[SelectionCandidate, ...]]
    aggregate_results: tuple[SelectionCandidate, ...]
    excluded_results: Mapping[str, tuple[SelectionExclusion, ...]]
    manifest_sha256_by_package: Mapping[str, str]
    stage_trace_by_package: Mapping[str, SelectionStageTrace]
    candidate_outcome_by_package: Mapping[str, str]
    valid_no_candidate: bool = False
    no_candidate_reason: str | None = None


class StrategyPackageSelectionComputation:
    """Compute candidates without constructing or writing any persistence dependency."""

    def compute(
        self,
        *,
        request: StrategyPackageSelectionComputationRequestV1,
        prepared_signals: Mapping[str, PreparedPackageSignalV1],
        providers: StrategyPackageSelectionReadOnlyProvidersV1,
    ) -> StrategyPackageSelectionComputationResultV1:
        expected = set(request.ordered_package_ids)
        _require_exact_keys(prepared_signals, expected, field_name="prepared_signals")
        package_results: dict[str, tuple[SelectionCandidate, ...]] = {}
        excluded_results: dict[str, tuple[SelectionExclusion, ...]] = {}
        manifest_sha256_by_package: dict[str, str] = {}
        stage_trace_by_package: dict[str, SelectionStageTrace] = {}
        candidate_outcome_by_package: dict[str, str] = {}

        for package_id in request.ordered_package_ids:
            prepared = prepared_signals[package_id]
            if prepared.package_id != package_id:
                raise RuntimeConfigInvalidError(
                    "prepared signal package identity mismatch",
                    context={"expected_package_id": package_id, "prepared_package_id": prepared.package_id},
                )
            profile = request.package_runtime_profiles[package_id]
            if prepared.artifact_header.trade_date != request.trade_date:
                raise RuntimeConfigInvalidError(
                    "prepared signal artifact trade date mismatch",
                    context={
                        "package_id": package_id,
                        "request_trade_date": request.trade_date.isoformat(),
                        "artifact_trade_date": prepared.artifact_header.trade_date.isoformat(),
                    },
                )
            if prepared.artifact_header.data_source != request.data_source:
                raise RuntimeConfigInvalidError(
                    "prepared signal artifact data source mismatch",
                    context={
                        "package_id": package_id,
                        "request_data_source": request.data_source,
                        "artifact_data_source": prepared.artifact_header.data_source,
                    },
                )
            _validate_hmm_profile_alignment(package_id=package_id, prepared=prepared, profile=profile)
            top_k = int(request.package_top_k[package_id])
            alpha_raw_receipt = build_stage_receipt(
                stage=CandidateStageName.ALPHA_RAW,
                status=StageReceiptStatus.COMPLETE,
                input_count=len(prepared.alpha_raw_candidates),
                candidates=list(prepared.alpha_raw_candidates),
                semantic_payload=prepared.artifact_header.stage_semantic_payload(
                    package_id=package_id,
                    manifest_sha256=prepared.manifest_sha256,
                ),
            )
            if prepared.valid_no_candidate:
                risk_result = providers.risk_policy.apply_to_candidates_with_receipt(
                    candidates=[],
                    decisions={},
                    trade_date=request.trade_date,
                    top_k=top_k,
                    package_id=package_id,
                    manifest_sha256=prepared.manifest_sha256,
                    allow_empty=True,
                    profile=profile.risk_policy,
                )
                selection_result = providers.tradability.select_top_k_with_receipt(
                    candidates=[],
                    top_k=top_k,
                    trade_date=request.trade_date,
                    package_id=package_id,
                    manifest_sha256=prepared.manifest_sha256,
                )
                candidate_outcome_by_package[package_id] = "VALID_NO_CANDIDATE"
            else:
                risk_decisions = providers.risk_policy.evaluate(
                    symbols=[item.symbol for item in prepared.hmm_adjusted_candidates],
                    trade_date=request.trade_date,
                    profile=profile.risk_policy,
                )
                risk_result = providers.risk_policy.apply_to_candidates_with_receipt(
                    candidates=list(prepared.hmm_adjusted_candidates),
                    decisions=risk_decisions,
                    trade_date=request.trade_date,
                    top_k=top_k,
                    package_id=package_id,
                    manifest_sha256=prepared.manifest_sha256,
                    profile=profile.risk_policy,
                    allow_empty=True,
                )
                if not (profile.tradability.exclude_suspended or profile.industry_blacklist):
                    selection_result = providers.tradability.select_top_k_with_receipt(
                        candidates=risk_result.candidates,
                        top_k=top_k,
                        trade_date=request.trade_date,
                        package_id=package_id,
                        manifest_sha256=prepared.manifest_sha256,
                    )
                else:
                    selection_result = providers.tradability.filter_candidates_with_receipt(
                        candidates=risk_result.candidates,
                        trade_date=request.trade_date,
                        top_k=top_k,
                        package_id=package_id,
                        manifest_sha256=prepared.manifest_sha256,
                        enabled=profile.tradability.exclude_suspended,
                        industry_blacklist=profile.industry_blacklist,
                        allow_empty=True,
                    )
                candidate_outcome_by_package[package_id] = (
                    "CANDIDATES_PRESENT" if selection_result.candidates else "VALID_NO_CANDIDATE"
                )

            package_results[package_id] = tuple(selection_result.candidates)
            excluded_results[package_id] = tuple([*risk_result.exclusions, *selection_result.exclusions])
            manifest_sha256_by_package[package_id] = prepared.manifest_sha256
            stage_trace_by_package[package_id] = SelectionStageTrace(
                alpha_raw=alpha_raw_receipt,
                hmm_adjusted=prepared.hmm_receipt,
                risk_policy_adjusted=risk_result.receipt,
                selection_effective=selection_result.receipt,
                hmm_metadata=dict(prepared.hmm_metadata),
                risk_metadata=risk_result.risk_metadata,
                universe_metadata=selection_result.universe_metadata,
            )

        aggregate_results = aggregate_selection_candidates(
            mode=request.selection_mode,
            package_results=package_results,
            package_weights=request.package_weights,
        )
        valid_no_candidate = False
        no_candidate_reason = None
        if not aggregate_results:
            if all(
                candidate_outcome_by_package.get(package_id) == "VALID_NO_CANDIDATE"
                for package_id in request.ordered_package_ids
            ):
                valid_no_candidate = True
                no_candidate_reason = REASON_VALID_NO_CANDIDATE
            else:
                raise ArtifactGenerationFailedError(
                    "selection aggregation produced no candidates",
                    context={
                        "mode": request.selection_mode.value,
                        "package_ids": list(request.ordered_package_ids),
                    },
                )

        return StrategyPackageSelectionComputationResultV1(
            package_results=package_results,
            aggregate_results=aggregate_results,
            excluded_results=excluded_results,
            manifest_sha256_by_package=manifest_sha256_by_package,
            stage_trace_by_package=stage_trace_by_package,
            candidate_outcome_by_package=candidate_outcome_by_package,
            valid_no_candidate=valid_no_candidate,
            no_candidate_reason=no_candidate_reason,
        )


def package_weights_from_runtime_config(
    runtime_config: Mapping[str, Any],
    package_ids: tuple[str, ...] | list[str],
) -> dict[str, float]:
    raw = runtime_config.get("package_weights")
    if not isinstance(raw, dict):
        raise RuntimeConfigInvalidError(
            "weighted package fusion requires runtime_config.package_weights",
            context={"package_ids": list(package_ids)},
        )
    expected = set(package_ids)
    actual = {str(key) for key in raw}
    if actual != expected:
        raise RuntimeConfigInvalidError(
            "runtime_config.package_weights must match package_ids exactly",
            context={"package_ids": list(package_ids), "weight_keys": sorted(actual)},
        )
    weights: dict[str, float] = {}
    for package_id in package_ids:
        try:
            value = float(raw[package_id])
        except (TypeError, ValueError) as exc:
            raise RuntimeConfigInvalidError(
                "package weights must be numeric",
                context={"package_id": package_id, "weight": raw[package_id]},
            ) from exc
        if not isfinite(value) or value <= 0:
            raise RuntimeConfigInvalidError(
                "package weights must be positive finite numbers",
                context={"package_id": package_id, "weight": raw[package_id]},
            )
        weights[package_id] = value
    return weights


def selection_runtime_profile_sha256(profile: SelectionRuntimeProfile) -> str:
    return canonical_evidence_json_sha256(profile.model_dump(mode="json"))


def aggregate_selection_candidates(
    *,
    mode: SelectionMode,
    package_results: Mapping[str, tuple[SelectionCandidate, ...] | list[SelectionCandidate]],
    package_weights: Mapping[str, float] | None = None,
) -> tuple[SelectionCandidate, ...]:
    if mode is SelectionMode.SINGLE_PACKAGE:
        return tuple(next(iter(package_results.values())))
    if mode is SelectionMode.WEIGHTED_FUSION:
        if package_weights is None:
            raise RuntimeConfigInvalidError("weighted package fusion requires package_weights")
        return _weighted_rank_fusion(package_results=package_results, package_weights=package_weights)
    symbol_sets = [set(candidate.symbol for candidate in rows) for rows in package_results.values()]
    symbols = set.union(*symbol_sets) if mode is SelectionMode.UNION else set.intersection(*symbol_sets)
    rows_by_symbol: dict[str, list[tuple[str, SelectionCandidate]]] = {}
    for package_id, rows in package_results.items():
        for row in rows:
            if row.symbol in symbols:
                rows_by_symbol.setdefault(row.symbol, []).append((package_id, row))
    aggregate: list[SelectionCandidate] = []
    for symbol, rows in rows_by_symbol.items():
        rows.sort(key=lambda item: item[1].rank)
        best = rows[0][1]
        source_package_ids = [package_id for package_id, _ in rows]
        aggregate.append(
            SelectionCandidate(
                symbol=symbol,
                score=sum(row.score for _, row in rows) / len(rows),
                rank=best.rank,
                target_weight=best.target_weight,
                target_quantity=best.target_quantity,
                reference_price=best.reference_price,
                component_scores={
                    "source_package_ids": source_package_ids,
                    "package_ranks": {package_id: row.rank for package_id, row in rows},
                },
                reason=f"{mode.value}_aggregate",
            )
        )
    aggregate.sort(key=lambda item: (-item.score, item.rank, item.symbol))
    return tuple(item.model_copy(update={"rank": idx}) for idx, item in enumerate(aggregate, start=1))


def _weighted_rank_fusion(
    *,
    package_results: Mapping[str, tuple[SelectionCandidate, ...] | list[SelectionCandidate]],
    package_weights: Mapping[str, float],
) -> tuple[SelectionCandidate, ...]:
    total_weight = sum(package_weights.values())
    normalized_weights = {package_id: weight / total_weight for package_id, weight in package_weights.items()}
    package_ids = list(package_results)
    fusion_policy_sha256 = _canonical_sha256(
        {
            "method": "weighted_rank_fusion",
            "package_weights": dict(package_weights),
            "normalized_package_weights": normalized_weights,
            "candidate_top_k": None,
            "missing_rank_policy": "not_selected_zero_score",
        }
    )
    rows_by_symbol: dict[str, list[tuple[str, SelectionCandidate, float]]] = {}
    for package_id, rows in package_results.items():
        candidate_count = len(rows)
        if candidate_count <= 0:
            continue
        denominator = max(candidate_count - 1, 1)
        for row in rows:
            normalized_rank_score = 1.0 - ((row.rank - 1) / denominator)
            rows_by_symbol.setdefault(row.symbol, []).append((package_id, row, normalized_rank_score))

    aggregate: list[SelectionCandidate] = []
    for symbol, rows in rows_by_symbol.items():
        rows.sort(key=lambda item: item[1].rank)
        best = rows[0][1]
        source_package_ids = [package_id for package_id, _, _ in rows]
        package_scores = {package_id: row.score for package_id, row, _ in rows}
        package_ranks = {package_id: row.rank for package_id, row, _ in rows}
        rank_scores = {package_id: 0.0 for package_id in package_ids}
        rank_scores.update({package_id: rank_score for package_id, _, rank_score in rows})
        package_presence = {
            package_id: ("selected_topK" if package_id in package_ranks else "not_selected_in_full_evidence")
            for package_id in package_ids
        }
        support_count = len(source_package_ids)
        rank_values = list(package_ranks.values())
        rank_dispersion = max(rank_values) - min(rank_values) if len(rank_values) > 1 else 0
        fusion_score = sum(
            normalized_weights[package_id] * rank_scores.get(package_id, 0.0) for package_id in package_ids
        )
        aggregate.append(
            SelectionCandidate(
                symbol=symbol,
                score=fusion_score,
                rank=best.rank,
                target_weight=best.target_weight,
                target_quantity=best.target_quantity,
                reference_price=best.reference_price,
                component_scores={
                    "fusion_method": "weighted_rank_fusion",
                    "source_package_ids": source_package_ids,
                    "package_ranks": package_ranks,
                    "package_raw_scores": package_scores,
                    "package_rank_scores": rank_scores,
                    "package_presence": package_presence,
                    "package_weights": dict(package_weights),
                    "normalized_package_weights": normalized_weights,
                    "support_count": support_count,
                    "rank_dispersion": rank_dispersion,
                    "fusion_policy_sha256": fusion_policy_sha256,
                    "fusion_score": fusion_score,
                },
                reason="weighted_fusion_aggregate",
            )
        )
    aggregate.sort(
        key=lambda item: (
            -item.score,
            -int((item.component_scores or {}).get("support_count") or 0),
            item.rank,
            item.symbol,
        )
    )
    return tuple(item.model_copy(update={"rank": index}) for index, item in enumerate(aggregate, start=1))


def _require_exact_keys(values: Mapping[str, Any], expected: set[str], *, field_name: str) -> None:
    actual = {str(key) for key in values}
    if actual != expected:
        raise RuntimeConfigInvalidError(
            f"selection computation {field_name} must match package_ids exactly",
            context={"package_ids": sorted(expected), "actual_keys": sorted(actual)},
        )


def _require_sha256(value: str, *, field_name: str, package_id: str) -> None:
    raw = str(value or "")
    normalized = raw.strip()
    if (
        raw != normalized
        or normalized != normalized.lower()
        or len(normalized) != 64
        or any(character not in "0123456789abcdef" for character in normalized)
    ):
        raise RuntimeConfigInvalidError(
            f"prepared package signal {field_name} must be a lowercase sha256",
            context={"package_id": package_id, field_name: value},
        )


def _validate_hmm_receipt_consistency(
    *,
    package_id: str,
    alpha_raw_candidates: tuple[SelectionCandidate, ...],
    hmm_adjusted_candidates: tuple[SelectionCandidate, ...],
    hmm_receipt: StageEvidenceReceipt,
    hmm_metadata: Mapping[str, Any],
) -> None:
    if hmm_receipt.stage is not CandidateStageName.HMM_ADJUSTED:
        raise RuntimeConfigInvalidError(
            "prepared HMM receipt has an invalid stage",
            context={"package_id": package_id, "stage": hmm_receipt.stage.value},
        )
    expected_input_count = len(alpha_raw_candidates)
    if hmm_receipt.input_count != expected_input_count:
        raise RuntimeConfigInvalidError(
            "prepared HMM receipt input count does not match alpha candidates",
            context={
                "package_id": package_id,
                "alpha_raw_candidate_count": expected_input_count,
                "receipt_input_count": hmm_receipt.input_count,
            },
        )
    if hmm_receipt.status is StageReceiptStatus.COMPLETE:
        expected_rows = canonical_candidate_rows(list(hmm_adjusted_candidates))
        if hmm_receipt.candidates != expected_rows:
            raise RuntimeConfigInvalidError(
                "prepared HMM receipt candidates do not match adjusted candidates",
                context={"package_id": package_id},
            )
    elif hmm_receipt.status is StageReceiptStatus.NOT_APPLICABLE:
        if canonical_candidate_rows(list(hmm_adjusted_candidates)) != canonical_candidate_rows(
            list(alpha_raw_candidates)
        ):
            raise RuntimeConfigInvalidError(
                "not-applicable HMM result must preserve alpha candidates",
                context={"package_id": package_id},
            )
    else:
        raise RuntimeConfigInvalidError(
            "prepared HMM receipt status is unsupported",
            context={"package_id": package_id, "status": hmm_receipt.status.value},
        )
    missing_metadata_keys = [key for key in hmm_receipt.semantic_payload if key not in hmm_metadata]
    if missing_metadata_keys:
        raise RuntimeConfigInvalidError(
            "prepared HMM metadata is missing receipt semantics",
            context={"package_id": package_id, "missing_keys": sorted(missing_metadata_keys)},
        )
    for key, value in hmm_receipt.semantic_payload.items():
        if canonical_evidence_json_sha256(hmm_metadata[key]) != canonical_evidence_json_sha256(value):
            raise RuntimeConfigInvalidError(
                "prepared HMM metadata does not match receipt semantics",
                context={"package_id": package_id, "field": key},
            )


def _validate_hmm_profile_alignment(
    *,
    package_id: str,
    prepared: PreparedPackageSignalV1,
    profile: SelectionRuntimeProfile,
) -> None:
    metadata_enabled = prepared.hmm_metadata.get("enabled")
    if metadata_enabled is not profile.hmm.enabled:
        raise RuntimeConfigInvalidError(
            "prepared HMM evidence does not match runtime profile",
            context={
                "package_id": package_id,
                "profile_hmm_enabled": profile.hmm.enabled,
                "evidence_hmm_enabled": metadata_enabled,
            },
        )
    if profile.hmm.enabled and prepared.alpha_raw_candidates:
        if prepared.hmm_receipt.status is not StageReceiptStatus.COMPLETE:
            raise RuntimeConfigInvalidError(
                "enabled HMM profile requires a complete adjusted receipt",
                context={"package_id": package_id, "status": prepared.hmm_receipt.status.value},
            )
    elif prepared.hmm_receipt.status is not StageReceiptStatus.NOT_APPLICABLE:
        raise RuntimeConfigInvalidError(
            "disabled or empty-input HMM requires a not-applicable receipt",
            context={"package_id": package_id, "status": prepared.hmm_receipt.status.value},
        )


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
