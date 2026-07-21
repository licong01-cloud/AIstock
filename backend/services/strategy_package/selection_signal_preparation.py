"""Repository-free raw StrategyPackage signal preparation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Protocol

from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.runtime_profile import (
    normalize_selection_runtime_config,
    parse_selection_runtime_profile,
)
from backend.services.strategy_package.selection_computation import (
    PreparedPackageComponentLineageV1,
    PreparedPackageSignalV1,
    SelectionArtifactHeaderV1,
    selection_runtime_profile_sha256,
)
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    HMMRuntimeUnavailableError,
    RuntimeConfigInvalidError,
)

from .live_inference import QEExperimentRuntimeAssetResolver, WslStrategyPackageInferenceProvider
from .models import StrategyPackageManifest
from .package_asset_store import PackageAssetStore
from .selection_artifact import (
    SELECTION_SCORE_ARTIFACT_CONTRACT_V2,
    SelectionScoreArtifact,
    StrategyPackageSelectionArtifactService,
    canonical_evidence_json_sha256,
)


class RawSelectionArtifactPreparer(Protocol):
    def prepare_from_live_inference_dates(
        self,
        *,
        package_id: str,
        trade_dates: list[date],
        data_source: str,
        runtime_config: dict[str, Any] | None,
        include_reference_price: bool,
        cutoff_date: date | None,
        historical_read_only: bool,
    ) -> list[SelectionScoreArtifact]: ...


class StrategyPackageIdentityReader(Protocol):
    def get(self, package_id: str) -> Any: ...


@dataclass(frozen=True)
class PreparedRawSelectionArtifactV2:
    """Stable raw result with operational UUID/time/path excluded from identity."""

    artifact: SelectionScoreArtifact
    semantic_header: Mapping[str, Any]
    raw_inference_receipt: Mapping[str, Any]
    source_read_receipts: tuple[Mapping[str, Any], ...]

    @property
    def signal_identity_hash(self) -> str:
        return _canonical_sha256(self.semantic_header)

    @property
    def signal_id(self) -> str:
        return f"ahrsig_{self.signal_identity_hash[:32]}"


@dataclass(frozen=True)
class StrategyPackageSignalPreparationResultV1:
    raw: PreparedRawSelectionArtifactV2
    prepared_signal: PreparedPackageSignalV1


class StrategyPackageSelectionSignalPreparation:
    """Run one frozen package without reading or writing an artifact repository."""

    def __init__(
        self,
        *,
        package_reader: StrategyPackageIdentityReader,
        raw_artifact_preparer: RawSelectionArtifactPreparer,
        hmm_runtime: SectorHMMRuntime,
    ) -> None:
        if package_reader is None or raw_artifact_preparer is None or hmm_runtime is None:
            raise ValueError("signal preparation requires package, raw inference, and HMM dependencies")
        self._package_reader = package_reader
        self._raw_artifact_preparer = raw_artifact_preparer
        self._hmm_runtime = hmm_runtime

    def prepare_historical(
        self,
        *,
        package_id: str,
        trade_date: date,
        runtime_config: dict[str, Any],
        data_source: str = "DB_HISTORICAL",
    ) -> StrategyPackageSignalPreparationResultV1:
        normalized_config = normalize_selection_runtime_config(runtime_config)
        record = self._package_reader.get(package_id)
        manifest = record.current_manifest()
        artifacts = self._raw_artifact_preparer.prepare_from_live_inference_dates(
            package_id=package_id,
            trade_dates=[trade_date],
            data_source=data_source,
            runtime_config=normalized_config,
            include_reference_price=False,
            cutoff_date=trade_date,
            historical_read_only=True,
        )
        if len(artifacts) != 1:
            raise ArtifactGenerationFailedError(
                "historical raw signal preparation did not return exactly one artifact",
                context={"package_id": package_id, "trade_date": trade_date.isoformat(), "count": len(artifacts)},
            )
        artifact = _seal_unsaved_v2_artifact(artifacts[0])
        if artifact.package_id != package_id or artifact.manifest_sha256 != manifest.manifest_sha256:
            raise RuntimeConfigInvalidError(
                "historical raw artifact differs from the frozen package identity",
                context={"package_id": package_id, "trade_date": trade_date.isoformat()},
            )
        if artifact.trade_date != trade_date or artifact.data_source != data_source:
            raise RuntimeConfigInvalidError(
                "historical raw artifact differs from the requested day identity",
                context={
                    "package_id": package_id,
                    "requested_trade_date": trade_date.isoformat(),
                    "artifact_trade_date": artifact.trade_date.isoformat(),
                    "artifact_data_source": artifact.data_source,
                },
            )
        metadata = dict(artifact.metadata or {})
        input_context = metadata.get("artifact_input_context")
        source_receipts = metadata.get("source_read_receipts")
        if not isinstance(input_context, dict) or not isinstance(source_receipts, list):
            raise ArtifactGenerationFailedError(
                "historical raw artifact evidence is incomplete",
                context={"package_id": package_id, "trade_date": trade_date.isoformat()},
            )
        universe_identity_hash = str(input_context.get("universe_input_hash") or "")
        profile = parse_selection_runtime_profile(normalized_config)
        runtime_profile_hash = selection_runtime_profile_sha256(profile)
        raw_semantic_header = {
            "schema_version": "strategy_package_prepared_raw_selection_artifact_v2",
            "package_id": artifact.package_id,
            "package_version": record.package_version,
            "manifest_sha256": artifact.manifest_sha256,
            "alpha_mode": manifest.alpha_mode.value,
            "trade_date": trade_date.isoformat(),
            "data_source": data_source,
            "runtime_profile_hash": runtime_profile_hash,
            "artifact_runtime_config_hash": artifact.runtime_config_hash,
            "artifact_sha256": artifact.artifact_sha256,
            "artifact_payload_sha256": artifact.artifact_payload_sha256,
            "artifact_input_context_hash": artifact.artifact_input_context_hash,
            "source_revision_set_hash": artifact.source_revision_set_hash,
            "asset_closure_hash": artifact.asset_closure_hash,
            "calendar_identity_hash": input_context.get("calendar_hash"),
            "universe_identity_hash": universe_identity_hash,
            "provider_semantics_hash": metadata.get("provider_semantics_hash"),
            "candidate_outcome": metadata.get("candidate_outcome"),
            "score_count": artifact.score_count,
            "universe_count": artifact.universe_count,
        }
        raw = PreparedRawSelectionArtifactV2(
            artifact=artifact,
            semantic_header=raw_semantic_header,
            raw_inference_receipt={
                "status": "COMPLETE",
                "score_count": artifact.score_count,
                "universe_count": artifact.universe_count,
                "artifact_input_context_hash": artifact.artifact_input_context_hash,
                "source_revision_set_hash": artifact.source_revision_set_hash,
            },
            source_read_receipts=tuple(dict(item) for item in source_receipts),
        )
        alpha_candidates = tuple(_candidate_from_row(item, package_id=package_id) for item in artifact.scores_json)
        frozen_hmm_evidence = _validate_historical_hmm_evidence(
            runtime_config=normalized_config,
            profile=profile,
            trade_date=trade_date,
            package_id=package_id,
        )
        try:
            hmm_result = self._hmm_runtime.adjust_candidates_with_receipt(
                candidates=list(alpha_candidates),
                trade_date=trade_date,
                profile=profile.hmm,
                package_id=package_id,
                manifest_sha256=str(manifest.manifest_sha256),
                require_frozen_snapshot=True,
                effective_trade_date=trade_date,
                receipt_admissibility="FORMAL_FROZEN_HISTORICAL",
            )
            if frozen_hmm_evidence is not None and not alpha_candidates:
                preflight = self._hmm_runtime.preflight_coefficients(
                    trade_date=trade_date,
                    profile=profile.hmm,
                    package_id=package_id,
                    require_frozen_snapshot=True,
                )
                hmm_metadata = {
                    **hmm_result.hmm_metadata,
                    **preflight,
                    "status": "NOT_APPLICABLE",
                    "reason": "NO_ALPHA_CANDIDATES",
                    "admissibility": "FORMAL_FROZEN_HISTORICAL",
                }
                hmm_receipt = hmm_result.receipt.model_copy(
                    update={
                        "semantic_payload": {
                            key: hmm_metadata.get(key)
                            for key in (
                                "enabled",
                                "status",
                                "reason",
                                "generation_mode",
                                "model_snapshot_id",
                                "signal_preset",
                                "model_artifact_sha256",
                                "coefficient_sha256",
                                "input_data_max_dates_hash",
                                "snapshot_trained_at",
                                "available_at",
                                "training_information_cutoff",
                                "as_of_trade_date",
                                "effective_trade_date",
                                "admissibility",
                            )
                        }
                    }
                )
                hmm_result = hmm_result.model_copy(
                    update={"receipt": hmm_receipt, "hmm_metadata": hmm_metadata}
                )
        except HMMRuntimeUnavailableError as exc:
            raise HMMRuntimeUnavailableError(
                "historical HMM frozen input is unavailable",
                context={
                    "reason_code": "ADVISORY_HR_HMM_INPUT_UNAVAILABLE",
                    "package_id": package_id,
                    "trade_date": trade_date.isoformat(),
                    "original_reason_code": exc.context.get("reason_code"),
                    "original_context": dict(exc.context),
                },
            ) from exc
        _validate_hmm_execution_matches_frozen_evidence(
            frozen_evidence=frozen_hmm_evidence,
            actual_metadata=hmm_result.hmm_metadata,
            package_id=package_id,
            trade_date=trade_date,
        )
        header = SelectionArtifactHeaderV1(
            artifact_id=raw.signal_id,
            artifact_sha256=str(artifact.artifact_sha256),
            package_id=artifact.package_id,
            manifest_sha256=artifact.manifest_sha256,
            trade_date=artifact.trade_date,
            data_source=artifact.data_source,
            runtime_config_hash=artifact.runtime_config_hash,
            artifact_payload_sha256=artifact.artifact_payload_sha256,
            artifact_contract_version=artifact.artifact_contract_version,
            artifact_input_context_hash=artifact.artifact_input_context_hash,
            source_revision_set_hash=artifact.source_revision_set_hash,
            asset_closure_hash=artifact.asset_closure_hash,
            universe_identity_hash=universe_identity_hash,
        )
        prepared = PreparedPackageSignalV1(
            package_id=package_id,
            package_version=record.package_version,
            manifest_sha256=str(manifest.manifest_sha256),
            alpha_mode=manifest.alpha_mode,
            component_lineage=_component_lineage(manifest),
            alpha_raw_candidates=alpha_candidates,
            hmm_adjusted_candidates=tuple(hmm_result.candidates),
            hmm_receipt=hmm_result.receipt,
            hmm_metadata=hmm_result.hmm_metadata,
            artifact_header=header,
            input_context_hash=artifact.artifact_input_context_hash,
            source_revision_set_hash=artifact.source_revision_set_hash,
            universe_identity_hash=universe_identity_hash,
            valid_no_candidate=artifact.score_count == 0,
            no_candidate_reason=(
                "authoritative historical raw inference produced no candidates"
                if artifact.score_count == 0
                else None
            ),
        )
        return StrategyPackageSignalPreparationResultV1(raw=raw, prepared_signal=prepared)


class _ForbiddenSelectionArtifactRepository:
    """Prevent historical preparation from entering ordinary Selection persistence."""

    @staticmethod
    def _reject(operation: str) -> None:
        raise RuntimeConfigInvalidError(
            "historical range signal preparation cannot access the Selection artifact repository",
            context={
                "reason_code": "ADVISORY_HR_SELECTION_ARTIFACT_REPOSITORY_FORBIDDEN",
                "operation": operation,
            },
        )

    def save(self, _artifact: Any) -> Any:
        self._reject("save")

    def get(self, _artifact_id: str) -> Any:
        self._reject("get")

    def list_for_package(self, *_args: Any, **_kwargs: Any) -> Any:
        self._reject("list_for_package")


def build_historical_strategy_package_signal_preparation(
    *,
    package_reader: StrategyPackageIdentityReader,
    package_asset_store: PackageAssetStore,
    runtime_root: Path,
    repository_root: Path,
    hmm_snapshot_provider: Any | None,
    wsl_inference_provider: WslStrategyPackageInferenceProvider | None = None,
) -> StrategyPackageSelectionSignalPreparation:
    """Compose the repository-free historical signal path from explicit dependencies."""

    if package_reader is None or package_asset_store is None:
        raise ValueError("historical signal preparation requires package and asset readers")
    runtime_asset_resolver = QEExperimentRuntimeAssetResolver(
        cache_root=runtime_root,
        asset_store=package_asset_store,
    )
    inference_provider = wsl_inference_provider or WslStrategyPackageInferenceProvider(
        repo_root=repository_root,
        safe_artifact_roots=(runtime_root,),
    )
    raw_artifact_preparer = StrategyPackageSelectionArtifactService(
        package_repository=package_reader,
        artifact_repository=_ForbiddenSelectionArtifactRepository(),
        runtime_asset_resolver=runtime_asset_resolver,
        live_inference_provider=inference_provider,
    )
    return StrategyPackageSelectionSignalPreparation(
        package_reader=package_reader,
        raw_artifact_preparer=raw_artifact_preparer,
        hmm_runtime=SectorHMMRuntime(snapshot_provider=hmm_snapshot_provider),
    )


def _seal_unsaved_v2_artifact(artifact: SelectionScoreArtifact) -> SelectionScoreArtifact:
    if artifact.artifact_contract_version != SELECTION_SCORE_ARTIFACT_CONTRACT_V2:
        raise ArtifactGenerationFailedError("raw signal preparation requires selection artifact v2")
    score_hash = _canonical_sha256(artifact.scores_json)
    payload_hash = canonical_evidence_json_sha256(artifact.canonical_v2_header(score_hash=score_hash))
    if artifact.artifact_sha256 not in {None, score_hash}:
        raise ArtifactGenerationFailedError("raw score hash conflicts with the unsaved artifact")
    if artifact.artifact_payload_sha256 not in {None, payload_hash}:
        raise ArtifactGenerationFailedError("raw payload hash conflicts with the unsaved artifact")
    return artifact.model_copy(update={"artifact_sha256": score_hash, "artifact_payload_sha256": payload_hash})


def _component_lineage(manifest: StrategyPackageManifest) -> tuple[PreparedPackageComponentLineageV1, ...]:
    return tuple(
        PreparedPackageComponentLineageV1(
            component_id=component.alpha_id,
            component_weight=component.component_weight,
            factor_ids=tuple(component.factor_ids),
            score_normalization=component.score_normalization,
        )
        for component in manifest.alpha_components
    )


def _candidate_from_row(row: Mapping[str, Any], *, package_id: str) -> SelectionCandidate:
    missing = [key for key in ("symbol", "score", "rank") if row.get(key) is None]
    if missing:
        raise ArtifactGenerationFailedError(
            "historical raw candidate is missing required score fields",
            context={"package_id": package_id, "missing": missing},
        )
    return SelectionCandidate(
        symbol=str(row["symbol"]),
        score=float(row["score"]),
        rank=int(row["rank"]),
        target_weight=float(row["target_weight"]) if row.get("target_weight") is not None else None,
        target_quantity=int(row["target_quantity"]) if row.get("target_quantity") is not None else None,
        reference_price=float(row["reference_price"]) if row.get("reference_price") is not None else None,
        component_scores=dict(row.get("component_scores") or {}),
        reason=str(row.get("reason")) if row.get("reason") is not None else None,
    )


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _validate_historical_hmm_evidence(
    *,
    runtime_config: Mapping[str, Any],
    profile: Any,
    trade_date: date,
    package_id: str,
) -> dict[str, Any] | None:
    if not profile.hmm.enabled:
        return None
    by_date = runtime_config.get("phase0a_hmm_metadata_by_date")
    metadata: dict[str, Any] = {}
    if isinstance(by_date, Mapping) and isinstance(by_date.get(trade_date.isoformat()), Mapping):
        metadata = dict(by_date[trade_date.isoformat()])
    elif isinstance(runtime_config.get("phase0a_hmm_metadata"), Mapping):
        candidate = dict(runtime_config["phase0a_hmm_metadata"])
        if str(candidate.get("as_of_trade_date") or "") == trade_date.isoformat():
            metadata = candidate
    required = (
        "model_snapshot_id",
        "signal_preset",
        "model_artifact_sha256",
        "coefficient_sha256",
        "snapshot_trained_at",
        "available_at",
        "training_information_cutoff",
        "as_of_trade_date",
        "effective_trade_date",
        "generation_mode",
        "input_data_max_dates",
    )
    missing = [
        field
        for field in required
        if metadata.get(field) is None or metadata.get(field) == "" or metadata.get(field) == {}
    ]
    if missing:
        raise RuntimeConfigInvalidError(
            "historical HMM requires exact frozen Phase 0A evidence",
            context={
                "reason_code": "ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE",
                "package_id": package_id,
                "trade_date": trade_date.isoformat(),
                "missing_fields": missing,
            },
        )
    if (
        str(metadata["model_snapshot_id"]) != str(profile.hmm.model_snapshot_id or "")
        or str(metadata["signal_preset"]) != str(profile.hmm.signal_preset or "")
        or str(metadata["as_of_trade_date"])[:10] != trade_date.isoformat()
        or str(metadata["effective_trade_date"])[:10] != trade_date.isoformat()
    ):
        raise RuntimeConfigInvalidError(
            "historical HMM frozen evidence differs from the runtime profile/day",
            context={
                "reason_code": "ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE",
                "package_id": package_id,
                "trade_date": trade_date.isoformat(),
            },
        )
    for hash_field in ("model_artifact_sha256", "coefficient_sha256"):
        value = str(metadata[hash_field]).strip().lower()
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise RuntimeConfigInvalidError(
                "historical HMM frozen evidence contains an invalid artifact hash",
                context={"reason_code": "ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE", "field": hash_field},
            )
    cutoff_values = [
        _date_value(metadata["snapshot_trained_at"]),
        _date_value(metadata["available_at"]),
        _date_value(metadata["training_information_cutoff"]),
        *(_date_value(value) for value in dict(metadata["input_data_max_dates"]).values()),
    ]
    if any(value > trade_date for value in cutoff_values):
        raise RuntimeConfigInvalidError(
            "historical HMM frozen evidence uses information after the decision day",
            context={
                "reason_code": "ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE",
                "package_id": package_id,
                "trade_date": trade_date.isoformat(),
                "max_evidence_date": max(cutoff_values).isoformat(),
            },
        )
    expected_input_hash = canonical_evidence_json_sha256(metadata["input_data_max_dates"])
    supplied_input_hash = metadata.get("input_data_max_dates_hash")
    if supplied_input_hash is not None and str(supplied_input_hash) != expected_input_hash:
        raise RuntimeConfigInvalidError(
            "historical HMM input_data_max_dates hash is inconsistent",
            context={"reason_code": "ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE", "package_id": package_id},
        )
    metadata["input_data_max_dates_hash"] = expected_input_hash
    return metadata


def _validate_hmm_execution_matches_frozen_evidence(
    *,
    frozen_evidence: dict[str, Any] | None,
    actual_metadata: Mapping[str, Any],
    package_id: str,
    trade_date: date,
) -> None:
    if frozen_evidence is None:
        return
    expected = {
        "model_snapshot_id": frozen_evidence["model_snapshot_id"],
        "signal_preset": frozen_evidence["signal_preset"],
        "model_artifact_sha256": frozen_evidence["model_artifact_sha256"],
        "coefficient_sha256": frozen_evidence["coefficient_sha256"],
        "input_data_max_dates_hash": frozen_evidence["input_data_max_dates_hash"],
        "snapshot_trained_at": frozen_evidence["snapshot_trained_at"],
        "available_at": frozen_evidence["available_at"],
        "training_information_cutoff": frozen_evidence["training_information_cutoff"],
        "as_of_trade_date": frozen_evidence["as_of_trade_date"],
        "effective_trade_date": frozen_evidence["effective_trade_date"],
    }
    mismatches = {
        field: {"expected": value, "actual": actual_metadata.get(field)}
        for field, value in expected.items()
        if actual_metadata.get(field) != value
    }
    if mismatches or actual_metadata.get("generation_mode") != "EXACT_SNAPSHOT":
        raise RuntimeConfigInvalidError(
            "historical HMM execution differs from frozen evidence",
            context={
                "reason_code": "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                "package_id": package_id,
                "trade_date": trade_date.isoformat(),
                "mismatches": mismatches,
                "generation_mode": actual_metadata.get("generation_mode"),
            },
        )


def _date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])
