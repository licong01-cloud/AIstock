"""HMM runtime adjustment for package-based selection.

This runtime consumes completed HMM model snapshots and resolves daily
coefficients automatically. Missing daily coefficients are generated through
the platform HMM service and then reused from the artifact cache; neutral
fallback coefficients are never fabricated when HMM is enabled.
"""

from __future__ import annotations

import json
import hashlib
import math
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from backend.services.selection_center.hmm_overlay_contract import (
    HMMOverlayDay, NO_L2_OVERLAY_POLICY, validate_overlay_day,
)
from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName,
    HMMAdjustmentResult,
    StageReceiptStatus,
    build_stage_receipt,
    canonical_evidence_json_sha256,
)
from backend.services.selection_center.runtime_profile import RuntimeHMMProfile
from backend.services.strategy_package.workspace_policy import ensure_not_forbidden_worker_workspace_path
from backend.services.trading_core.errors import ArtifactGenerationFailedError, HMMRuntimeUnavailableError


class HMMSnapshotProvider(Protocol):
    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        ...


@dataclass(frozen=True)
class HMMCoefficientArtifact:
    path: Path
    payload: dict[str, Any]
    sha256: str


class SectorHMMRuntime:
    """Apply precomputed sector-HMM coefficients to ranked candidates."""

    _READY_STATUSES = {"completed", "success", "succeeded", "ready"}
    _GENERATION_LOCKS: dict[tuple[str, str, str], threading.Lock] = {}
    _GENERATION_LOCKS_GUARD = threading.RLock()

    def __init__(self, snapshot_provider: HMMSnapshotProvider | None = None) -> None:
        self._snapshot_provider = snapshot_provider

    def adjust_candidates(
        self,
        *,
        candidates: list[SelectionCandidate],
        trade_date: date,
        profile: RuntimeHMMProfile,
        package_id: str,
        manifest_sha256: str,
        effective_trade_date: date | None = None,
    ) -> list[SelectionCandidate]:
        return self.adjust_candidates_with_receipt(
            candidates=candidates,
            trade_date=trade_date,
            profile=profile,
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            effective_trade_date=effective_trade_date,
        ).candidates

    def adjust_candidates_with_receipt(
        self,
        *,
        candidates: list[SelectionCandidate],
        trade_date: date,
        profile: RuntimeHMMProfile,
        package_id: str,
        manifest_sha256: str,
        require_frozen_snapshot: bool = False,
        effective_trade_date: date | None = None,
        receipt_admissibility: str = "PROSPECTIVE_FIRST_OBSERVED",
    ) -> HMMAdjustmentResult:
        if not profile.enabled:
            return HMMAdjustmentResult(
                candidates=list(candidates),
                receipt=build_stage_receipt(
                    stage=CandidateStageName.HMM_ADJUSTED,
                    status=StageReceiptStatus.NOT_APPLICABLE,
                    input_count=len(candidates),
                    candidates=[],
                    semantic_payload={"enabled": False, "generation_mode": "NOT_APPLICABLE"},
                ),
                hmm_metadata={
                    "enabled": False,
                    "status": "NOT_APPLICABLE",
                    "generation_mode": "NOT_APPLICABLE",
                },
            )
        if not candidates:
            return HMMAdjustmentResult(
                candidates=[],
                receipt=build_stage_receipt(
                    stage=CandidateStageName.HMM_ADJUSTED,
                    status=StageReceiptStatus.NOT_APPLICABLE,
                    input_count=0,
                    candidates=[],
                    semantic_payload={"enabled": True, "generation_mode": "NO_ALPHA_CANDIDATES"},
                    reason_codes=["ADVISORY_PHASE0A2C_HMM_NO_ALPHA_CANDIDATES"],
                ),
                hmm_metadata={
                    "enabled": True,
                    "status": "NOT_APPLICABLE",
                    "generation_mode": "NO_ALPHA_CANDIDATES",
                },
            )
        if require_frozen_snapshot and profile.model_config_id and not profile.model_snapshot_id:
            raise HMMRuntimeUnavailableError(
                "prospective HMM capture requires an explicit model_snapshot_id; dynamic latest resolution is forbidden",
                context={
                    "reason_code": "ADVISORY_PHASE0A2C_HMM_RECEIPT_INCOMPLETE",
                    "package_id": package_id,
                    "model_config_id": profile.model_config_id,
                },
            )
        requested_snapshot_id = profile.model_snapshot_id
        profile = self._resolve_profile_snapshot(profile)
        if not profile.model_snapshot_id:
            raise HMMRuntimeUnavailableError(
                "HMM runtime profile requires model_snapshot_id or model_config_id when enabled",
                context={"package_id": package_id, "runtime_profile": profile.model_dump(mode="json")},
            )
        if not profile.signal_preset:
            raise HMMRuntimeUnavailableError(
                "HMM runtime profile requires signal_preset when enabled",
                context={"package_id": package_id, "runtime_profile": profile.model_dump(mode="json")},
            )

        snapshot = self._load_snapshot(profile.model_snapshot_id)
        status = str(snapshot.get("status") or "").strip().casefold()
        if status not in self._READY_STATUSES:
            raise HMMRuntimeUnavailableError(
                "HMM snapshot is not ready",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "snapshot_status": snapshot.get("status"),
                },
            )
        model_path = _resolve_local_path(str(snapshot.get("model_path") or ""))
        if model_path is None or not model_path.exists() or not model_path.is_file():
            raise HMMRuntimeUnavailableError(
                "HMM model artifact does not exist",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "model_path": snapshot.get("model_path"),
                },
            )

        artifact = self._load_coefficients(
            model_path=model_path,
            profile=profile,
            trade_date=trade_date,
            package_id=package_id,
            allow_generate=not require_frozen_snapshot,
        )
        payload = artifact.payload
        day_key = trade_date.isoformat()
        overlay = self._validated_overlay_day(
            artifact=artifact, snapshot=snapshot, model_path=model_path,
            profile=profile, trade_date=trade_date, package_id=package_id,
            effective_trade_date=effective_trade_date,
            empty_mapping_context={"symbol": candidates[0].symbol, "raw_rank": candidates[0].rank,
                                   "manifest_sha256": manifest_sha256},
        )
        stock_sector_map = overlay.membership
        day_coefficients = overlay.coefficients

        adjusted: list[SelectionCandidate] = []
        exclusions: list[SelectionExclusion] = []
        for candidate in candidates:
            raw_score = _finite_float(
                candidate.score,
                message="HMM runtime requires finite candidate scores",
                context={"package_id": package_id, "symbol": candidate.symbol, "raw_score": candidate.score},
            )
            sector_code = stock_sector_map.get(candidate.symbol)
            if sector_code is None or not str(sector_code).strip():
                if profile.missing_sector_policy == "exclude_candidate" and not overlay.metadata:
                    exclusions.append(
                        SelectionExclusion(
                            symbol=candidate.symbol,
                            score=raw_score,
                            rank=candidate.rank,
                            reason="HMM_SECTOR_MAPPING_MISSING",
                            source="sector_hmm_runtime",
                            context={
                                "trade_date": day_key,
                                "model_snapshot_id": profile.model_snapshot_id,
                                "missing_sector_policy": profile.missing_sector_policy,
                            },
                        )
                    )
                    continue
                raise HMMRuntimeUnavailableError(
                    "HMM coefficient artifact is missing stock sector mapping",
                    context={
                        "package_id": package_id,
                        "manifest_sha256": manifest_sha256,
                        "snapshot_id": profile.model_snapshot_id,
                        "trade_date": day_key,
                        "symbol": candidate.symbol,
                        "raw_rank": candidate.rank,
                        "missing_sector_policy": profile.missing_sector_policy,
                        "coefficients_path": str(artifact.path),
                    },
                )
            apply_overlay = sector_code not in overlay.no_overlay_sectors
            coefficient = day_coefficients[sector_code] if apply_overlay else None
            component_scores = dict(candidate.component_scores or {})
            component_scores.setdefault("raw_rank", candidate.rank)
            component_scores["hmm"] = {
                "enabled": True,
                "model_snapshot_id": profile.model_snapshot_id,
                "signal_preset": profile.signal_preset,
                "snapshot_status": snapshot.get("status"),
                "model_path": str(model_path),
                "coefficients_path": str(artifact.path),
                "sector_code": str(sector_code),
                "coefficient": coefficient,
                "raw_score": raw_score,
                "raw_rank": candidate.rank,
                "source_reason": candidate.reason,
            }
            reason = _hmm_adjusted_reason(candidate.reason)
            if not apply_overlay:
                component_scores["hmm"].update({
                    "overlay_applied": False,
                    "reason_code": "HMM_L2_QUOTES_UNAVAILABLE",
                    "l2_overlay_unavailable_policy": overlay.metadata["l2_overlay_unavailable_policy"],
                    "quote_availability_digest": overlay.metadata["quote_availability_digest"],
                })
                reason = f"{candidate.reason}|hmm_overlay_not_applied" if candidate.reason else "hmm_overlay_not_applied"
            adjusted.append(
                candidate.model_copy(
                    update={
                        "score": raw_score * coefficient if apply_overlay else raw_score,
                        "component_scores": component_scores,
                        "reason": reason,
                    }
                )
            )

        adjusted.sort(key=lambda item: (-item.score, item.component_scores.get("raw_rank", item.rank), item.symbol))
        reranked = [item.model_copy(update={"rank": rank}) for rank, item in enumerate(adjusted, start=1)]
        observed_at = datetime.now(timezone.utc)
        coefficients_sha256 = artifact.sha256
        model_sha256 = _file_sha256(model_path)
        generation_mode = "EXACT_SNAPSHOT" if requested_snapshot_id else "DYNAMIC_LATEST_RESOLVED"
        input_data_max_dates = _input_data_max_dates_for_day(
            payload,
            snapshot=snapshot,
            day_key=day_key,
            package_id=package_id,
            coefficients_path=artifact.path,
        )
        excluded_symbols = sorted(item.symbol for item in exclusions)
        hmm_metadata = {
            "enabled": True,
            "status": "COMPLETE",
            "generation_mode": generation_mode,
            "model_snapshot_id": profile.model_snapshot_id,
            "model_config_id": profile.model_config_id,
            "signal_preset": profile.signal_preset,
            "snapshot_status": snapshot.get("status"),
            "snapshot_trained_at": _time_value(snapshot.get("trained_at")),
            "available_at": _time_value(snapshot.get("available_at")),
            "training_information_cutoff": _time_value(snapshot.get("training_information_cutoff")),
            "as_of_trade_date": trade_date.isoformat(),
            "effective_trade_date": effective_trade_date.isoformat() if effective_trade_date else None,
            "first_observed_at": observed_at.isoformat(),
            "admissibility": receipt_admissibility,
            "model_sha256": model_sha256,
            "model_artifact_sha256": model_sha256,
            "coefficients_sha256": coefficients_sha256,
            "coefficient_sha256": coefficients_sha256,
            "coefficients_payload_sha256": canonical_evidence_json_sha256(artifact.payload),
            "coefficient_trade_date": day_key,
            "input_data_max_dates": input_data_max_dates or None,
            "input_data_max_dates_hash": (
                canonical_evidence_json_sha256(input_data_max_dates)
                if input_data_max_dates
                else None
            ),
            "freshness_lag": snapshot.get("freshness_lag"),
            "missing_sector_policy": profile.missing_sector_policy,
            "missing_sector_excluded_count": len(excluded_symbols),
            "missing_sector_excluded_symbols_hash": canonical_evidence_json_sha256(
                excluded_symbols
            ),
            # Paths are diagnostic only and intentionally omitted from receipt semantics.
            "model_path": str(model_path),
            "coefficients_path": str(artifact.path),
        }
        if overlay.metadata:
            hmm_metadata.update(overlay.metadata)
            hmm_metadata["no_overlay_candidate_count"] = sum(
                row.component_scores["hmm"].get("overlay_applied") is False for row in reranked
            )
        receipt = build_stage_receipt(
            stage=CandidateStageName.HMM_ADJUSTED,
            status=StageReceiptStatus.COMPLETE,
            input_count=len(candidates),
            candidates=reranked,
            exclusions=exclusions,
            semantic_payload={
                **({**overlay.metadata, "no_overlay_candidate_count": hmm_metadata["no_overlay_candidate_count"]}
                   if overlay.metadata else {}),
                **{
                    key: hmm_metadata[key]
                    for key in (
                    "enabled",
                    "status",
                    "generation_mode",
                    "model_snapshot_id",
                    "model_config_id",
                    "signal_preset",
                    "snapshot_status",
                    "snapshot_trained_at",
                    "available_at",
                    "training_information_cutoff",
                    "as_of_trade_date",
                    "effective_trade_date",
                    "admissibility",
                    "model_sha256",
                    "model_artifact_sha256",
                    "coefficients_sha256",
                    "coefficient_sha256",
                    "coefficients_payload_sha256",
                    "coefficient_trade_date",
                    "input_data_max_dates_hash",
                    "freshness_lag",
                    "missing_sector_policy",
                    "missing_sector_excluded_count",
                    "missing_sector_excluded_symbols_hash",
                    )
                },
            },
        )
        return HMMAdjustmentResult(candidates=reranked, receipt=receipt, hmm_metadata=hmm_metadata)

    def preflight_coefficients(
        self,
        *,
        trade_date: date,
        profile: RuntimeHMMProfile,
        package_id: str,
        require_frozen_snapshot: bool = False,
    ) -> dict[str, Any]:
        """Validate the HMM artifact shape before live selection inference starts."""

        if not profile.enabled:
            return {"enabled": False}
        if require_frozen_snapshot and profile.model_config_id and not profile.model_snapshot_id:
            raise HMMRuntimeUnavailableError(
                "historical HMM preflight requires an explicit model_snapshot_id",
                context={
                    "reason_code": "ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE",
                    "package_id": package_id,
                    "model_config_id": profile.model_config_id,
                },
            )
        profile = self._resolve_profile_snapshot(profile)
        if not profile.model_snapshot_id:
            raise HMMRuntimeUnavailableError(
                "HMM runtime profile requires model_snapshot_id or model_config_id when enabled",
                context={"package_id": package_id, "runtime_profile": profile.model_dump(mode="json")},
            )
        if not profile.signal_preset:
            raise HMMRuntimeUnavailableError(
                "HMM runtime profile requires signal_preset when enabled",
                context={"package_id": package_id, "runtime_profile": profile.model_dump(mode="json")},
            )

        snapshot = self._load_snapshot(profile.model_snapshot_id)
        status = str(snapshot.get("status") or "").strip().casefold()
        if status not in self._READY_STATUSES:
            raise HMMRuntimeUnavailableError(
                "HMM snapshot is not ready",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "snapshot_status": snapshot.get("status"),
                },
            )
        model_path = _resolve_local_path(str(snapshot.get("model_path") or ""))
        if model_path is None or not model_path.exists() or not model_path.is_file():
            raise HMMRuntimeUnavailableError(
                "HMM model artifact does not exist",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "model_path": snapshot.get("model_path"),
                },
            )

        artifact = self._load_coefficients(
            model_path=model_path,
            profile=profile,
            trade_date=trade_date,
            package_id=package_id,
            allow_generate=not require_frozen_snapshot,
        )
        day_key = trade_date.isoformat()
        overlay = self._validated_overlay_day(
            artifact=artifact, snapshot=snapshot, model_path=model_path,
            profile=profile, trade_date=trade_date, package_id=package_id,
            effective_trade_date=trade_date,
        )
        return {
            "enabled": True,
            "model_config_id": profile.model_config_id,
            "snapshot_id": profile.model_snapshot_id,
            "model_snapshot_id": profile.model_snapshot_id,
            "snapshot_status": snapshot.get("status"),
            "signal_preset": profile.signal_preset,
            "model_path": str(model_path),
            "coefficients_path": str(artifact.path),
            "trade_date": day_key,
            "sector_count": len(overlay.coefficients),
            "coefficient_count": len(overlay.coefficients),
            "stock_sector_map_count": len(overlay.membership),
            "generation_mode": "EXACT_SNAPSHOT" if require_frozen_snapshot else "PREFLIGHT",
            "model_artifact_sha256": _file_sha256(model_path),
            "coefficient_sha256": artifact.sha256,
            "input_data_max_dates_hash": _input_data_max_dates_hash(
                artifact.payload,
                snapshot=snapshot,
                day_key=day_key,
                package_id=package_id,
                coefficients_path=artifact.path,
            ),
            "snapshot_trained_at": _time_value(snapshot.get("trained_at")),
            "available_at": _time_value(snapshot.get("available_at")),
            "training_information_cutoff": _time_value(snapshot.get("training_information_cutoff")),
            "as_of_trade_date": day_key,
            "effective_trade_date": day_key,
            **overlay.metadata,
        }

    def _validated_overlay_day(
        self, *, artifact: HMMCoefficientArtifact, snapshot: dict[str, Any],
        model_path: Path, profile: RuntimeHMMProfile, trade_date: date,
        package_id: str, effective_trade_date: date | None,
        empty_mapping_context: dict[str, Any] | None = None,
    ) -> HMMOverlayDay:
        membership = _stock_sector_map_for_day(
            artifact.payload, day_key=trade_date.isoformat(),
            package_id=package_id, coefficients_path=artifact.path,
        )
        context = {"package_id": package_id, "snapshot_id": profile.model_snapshot_id,
                   "trade_date": trade_date.isoformat(), "coefficients_path": str(artifact.path)}
        if isinstance(membership, dict) and not membership:
            # Keep the native candidate-specific failure diagnostics when every
            # candidate is unmapped. Validation still rejects the entire asset.
            context.update(empty_mapping_context or {})
        return validate_overlay_day(
            payload=artifact.payload,
            membership=membership,
            trade_date=trade_date, effective_trade_date=effective_trade_date,
            snapshot_id=str(profile.model_snapshot_id),
            config_id=profile.model_config_id or snapshot.get("config_id"),
            signal_preset=str(profile.signal_preset), coefficient_filename=artifact.path.name,
            coefficient_sha256=artifact.sha256, model_sha256=_file_sha256(model_path),
            registered_windows=lambda: self._registered_coefficient_windows(snapshot),
            context=context,
        )

    def _registered_coefficient_windows(self, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        provider = self._snapshot_provider
        if provider is None:
            from backend.services.hmm_training_service import HMMTrainingService

            provider = HMMTrainingService()
        get_config = getattr(provider, "get_config", None)
        if not callable(get_config):
            # Snapshot-only consumers (including LocalSIM) use the same public
            # read-only registration API, never a consumer-specific adapter or
            # the provider's train/generate entry points.
            from backend.services.hmm_training_service import HMMTrainingService

            get_config = HMMTrainingService().get_config
        if not snapshot.get("config_id"):
            raise HMMRuntimeUnavailableError("HMM no-overlay registration reader is unavailable")
        record = get_config(str(snapshot["config_id"]))
        config = record.get("config_json") if isinstance(record, dict) else None
        windows = config.get("coefficient_windows") if isinstance(config, dict) else None
        if not isinstance(windows, list) or any(not isinstance(row, dict) for row in windows):
            raise HMMRuntimeUnavailableError("HMM no-overlay registered windows are unavailable")
        return windows

    def _load_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        provider = self._snapshot_provider
        if provider is None:
            from backend.services.hmm_training_service import HMMTrainingService

            provider = HMMTrainingService()
        snapshot = provider.get_snapshot(snapshot_id)
        if not snapshot:
            raise HMMRuntimeUnavailableError(
                "HMM snapshot does not exist",
                context={"snapshot_id": snapshot_id},
            )
        return dict(snapshot)

    def _resolve_profile_snapshot(self, profile: RuntimeHMMProfile) -> RuntimeHMMProfile:
        if profile.model_snapshot_id or not profile.model_config_id:
            return profile
        provider = self._snapshot_provider
        if provider is None:
            from backend.services.hmm_training_service import HMMTrainingService

            provider = HMMTrainingService()
        list_snapshots = getattr(provider, "list_snapshots", None)
        if not callable(list_snapshots):
            raise HMMRuntimeUnavailableError(
                "HMM runtime profile uses model_config_id but provider cannot list snapshots",
                context={"model_config_id": profile.model_config_id},
            )
        rows = list_snapshots(profile.model_config_id)
        ready = [
            dict(row)
            for row in rows or []
            if str(row.get("status") or "").strip().casefold() in self._READY_STATUSES
            and str(row.get("snapshot_id") or "").strip()
        ]
        if not ready:
            raise HMMRuntimeUnavailableError(
                "HMM model config has no ready snapshot for runtime use",
                context={"model_config_id": profile.model_config_id},
            )
        ready.sort(key=lambda row: str(row.get("trained_at") or row.get("created_at") or ""), reverse=True)
        return profile.model_copy(update={"model_snapshot_id": str(ready[0]["snapshot_id"]).strip()})

    def _load_coefficients(
        self,
        *,
        model_path: Path,
        profile: RuntimeHMMProfile,
        trade_date: date,
        package_id: str,
        allow_generate: bool = True,
    ) -> HMMCoefficientArtifact:
        if profile.coefficients_path:
            explicit = _resolve_local_path(profile.coefficients_path, base_dir=model_path.parent)
            if explicit is None or not explicit.exists() or not explicit.is_file():
                raise HMMRuntimeUnavailableError(
                    "explicit HMM coefficient artifact does not exist",
                    context={
                        "package_id": package_id,
                        "snapshot_id": profile.model_snapshot_id,
                        "coefficients_path": profile.coefficients_path,
                    },
                )
            artifact = _read_coefficient_artifact(explicit, package_id=package_id)
            _validate_preset(artifact.payload, profile=profile, path=explicit, package_id=package_id)
            return artifact

        found = self._load_existing_coefficients(
            model_path=model_path,
            profile=profile,
            trade_date=trade_date,
            package_id=package_id,
        )
        if found["artifact"] is not None:
            return found["artifact"]
        if not profile.auto_compute or not allow_generate:
            raise HMMRuntimeUnavailableError(
                "HMM coefficient artifact is missing and prospective capture cannot generate it",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "trade_date": trade_date.isoformat(),
                    "model_dir": str(model_path.parent),
                    "candidate_paths": found["candidate_paths"],
                    "reason": found["reason"],
                    "reason_code": "ADVISORY_PHASE0A2C_HMM_RECEIPT_INCOMPLETE" if not allow_generate else None,
                },
            )
        if found["reason"] == "missing_artifact":
            return self._generate_coefficients_on_miss(
                model_path=model_path,
                profile=profile,
                trade_date=trade_date,
                package_id=package_id,
                reason="missing_artifact",
                candidate_paths=[],
            )
        return self._generate_coefficients_on_miss(
            model_path=model_path,
            profile=profile,
            trade_date=trade_date,
            package_id=package_id,
            reason="no_artifact_covers_trade_date",
            candidate_paths=found["candidate_paths"],
        )

    def _load_existing_coefficients(
        self,
        *,
        model_path: Path,
        profile: RuntimeHMMProfile,
        trade_date: date,
        package_id: str,
    ) -> dict[str, Any]:
        pattern = f"coefficients_{_safe_key(profile.signal_preset)}_*.json"
        matches = sorted(model_path.parent.glob(pattern))
        if not matches:
            return {"artifact": None, "reason": "missing_artifact", "candidate_paths": []}
        trade_key = trade_date.isoformat()
        valid: list[HMMCoefficientArtifact] = []
        for path in matches:
            artifact = _read_coefficient_artifact(path, package_id=package_id)
            payload = artifact.payload
            _validate_preset(payload, profile=profile, path=path, package_id=package_id)
            daily_coefficients = payload.get("daily_coefficients")
            if isinstance(daily_coefficients, dict) and trade_key in daily_coefficients:
                valid.append(artifact)
        if not valid:
            return {
                "artifact": None,
                "reason": "no_artifact_covers_trade_date",
                "candidate_paths": [str(path) for path in matches],
            }
        if len(valid) > 1:
            raise HMMRuntimeUnavailableError(
                "multiple HMM coefficient artifacts cover trade_date; specify coefficients_path",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "trade_date": trade_key,
                    "matching_paths": [str(item.path) for item in valid],
                },
            )
        return {"artifact": valid[0], "reason": None, "candidate_paths": [str(path) for path in matches]}

    def _generate_coefficients_on_miss(
        self,
        *,
        model_path: Path,
        profile: RuntimeHMMProfile,
        trade_date: date,
        package_id: str,
        reason: str,
        candidate_paths: list[str],
    ) -> HMMCoefficientArtifact:
        provider = self._snapshot_provider
        if provider is None:
            from backend.services.hmm_training_service import HMMTrainingService

            provider = HMMTrainingService()
        generator = getattr(provider, "generate_daily_coefficients", None)
        if generator is None:
            raise HMMRuntimeUnavailableError(
                "HMM coefficient artifact is missing and provider cannot auto-generate",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "trade_date": trade_date.isoformat(),
                    "model_dir": str(model_path.parent),
                    "candidate_paths": candidate_paths,
                    "reason": reason,
                },
            )
        lock = self._generation_lock(
            str(profile.model_snapshot_id),
            str(profile.signal_preset),
            trade_date.isoformat(),
        )
        with lock:
            existing = self._load_existing_coefficients(
                model_path=model_path,
                profile=profile,
                trade_date=trade_date,
                package_id=package_id,
            )
            if existing["artifact"] is not None:
                return existing["artifact"]
            as_of_date = self._previous_trading_day_for_provider(provider, trade_date)
            try:
                result = generator(
                    profile.model_snapshot_id,
                    signal_preset=profile.signal_preset,
                    confirm_generate=True,
                    as_of_date=as_of_date,
                    effective_trade_date=trade_date,
                )
            except Exception as exc:
                raise ArtifactGenerationFailedError(
                    "HMM coefficient auto-generation failed",
                    context={
                        "package_id": package_id,
                        "snapshot_id": profile.model_snapshot_id,
                        "trade_date": trade_date.isoformat(),
                        "as_of_date": as_of_date.isoformat() if as_of_date else None,
                        "reason": reason,
                        "error": str(exc),
                    },
                ) from exc
            output_path = _resolve_local_path(str(result.get("output_path") or ""), base_dir=model_path.parent)
        if output_path is None or not output_path.exists() or not output_path.is_file():
            raise ArtifactGenerationFailedError(
                "HMM coefficient auto-generation did not produce an artifact",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "trade_date": trade_date.isoformat(),
                    "output_path": result.get("output_path"),
                    "status": result.get("status"),
                },
            )
        artifact = _read_coefficient_artifact(output_path, package_id=package_id)
        _validate_preset(artifact.payload, profile=profile, path=output_path, package_id=package_id)
        return artifact

    @classmethod
    def _generation_lock(cls, snapshot_id: str, signal_preset: str, trade_date: str) -> threading.Lock:
        key = (snapshot_id, signal_preset, trade_date)
        with cls._GENERATION_LOCKS_GUARD:
            lock = cls._GENERATION_LOCKS.get(key)
            if lock is None:
                lock = threading.Lock()
                cls._GENERATION_LOCKS[key] = lock
            return lock

    @staticmethod
    def _previous_trading_day_for_provider(provider: Any, trade_date: date) -> date | None:
        list_days = getattr(provider, "_list_trading_days", None)
        if not callable(list_days):
            return None
        days = list_days(trade_date - timedelta(days=31), trade_date - timedelta(days=1))
        return days[-1] if days else None


def _read_coefficient_artifact(path: Path, *, package_id: str) -> HMMCoefficientArtifact:
    try:
        content = path.read_bytes()
        payload = json.loads(content.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HMMRuntimeUnavailableError(
            "HMM coefficient artifact is not valid JSON",
            context={"package_id": package_id, "coefficients_path": str(path)},
        ) from exc
    except OSError as exc:
        raise HMMRuntimeUnavailableError(
            "HMM coefficient artifact cannot be read",
            context={"package_id": package_id, "coefficients_path": str(path)},
        ) from exc
    if not isinstance(payload, dict):
        raise HMMRuntimeUnavailableError(
            "HMM coefficient artifact must be a JSON object",
            context={"package_id": package_id, "coefficients_path": str(path)},
        )
    # Identity describes the exact bytes parsed above, not a second filesystem
    # read that could race with an asset replacement.
    return HMMCoefficientArtifact(path=path, payload=payload, sha256=hashlib.sha256(content).hexdigest())


def _stock_sector_map_for_day(
    payload: dict[str, Any],
    *,
    day_key: str,
    package_id: str,
    coefficients_path: Path,
) -> dict[str, Any] | Any:
    maps_by_date = payload.get("stock_sector_map_by_date")
    if maps_by_date is not None:
        if not isinstance(maps_by_date, dict) or not isinstance(maps_by_date.get(day_key), dict):
            raise HMMRuntimeUnavailableError(
                "HMM coefficient artifact has no point-in-time stock sector map for trade_date",
                context={
                    "package_id": package_id,
                    "trade_date": day_key,
                    "coefficients_path": str(coefficients_path),
                },
            )
        return maps_by_date[day_key]
    flat = payload.get("stock_sector_map")
    if flat is not None or payload.get("l2_overlay_unavailable_policy") != NO_L2_OVERLAY_POLICY:
        return flat
    spans = payload.get("stock_sector_membership_spans")
    if not isinstance(spans, dict) or not spans:
        return None
    membership: dict[str, str] = {}
    try:
        for symbol, rows in spans.items():
            if not isinstance(rows, list) or not rows:
                raise ValueError("missing membership spans")
            previous_end = ""
            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError("invalid membership span")
                start, end, sector = (row.get(key) for key in ("start_date", "end_date", "sector_code"))
                if (not isinstance(start, str) or not isinstance(end, str)
                        or date.fromisoformat(start).isoformat() != start or date.fromisoformat(end).isoformat() != end
                        or not isinstance(sector, str) or not sector.strip()
                        or start > end or start <= previous_end):
                    raise ValueError("invalid or overlapping membership spans")
                if start <= day_key <= end:
                    membership[symbol] = sector
                previous_end = end
    except (TypeError, ValueError) as exc:
        raise HMMRuntimeUnavailableError(
            "HMM coefficient artifact has invalid point-in-time membership spans",
            context={"package_id": package_id, "trade_date": day_key, "coefficients_path": str(coefficients_path)},
        ) from exc
    return membership


def _input_data_max_dates_for_day(
    payload: dict[str, Any],
    *,
    snapshot: dict[str, Any],
    day_key: str,
    package_id: str,
    coefficients_path: Path,
) -> dict[str, Any]:
    snapshot_dates = snapshot.get("input_data_max_dates")
    base = dict(snapshot_dates) if isinstance(snapshot_dates, dict) else {}
    by_date = payload.get("input_data_max_dates_by_date")
    if by_date is not None:
        day_dates = by_date.get(day_key) if isinstance(by_date, dict) else None
        if not isinstance(day_dates, dict) or not day_dates:
            raise HMMRuntimeUnavailableError(
                "HMM coefficient artifact has no point-in-time input watermark for trade_date",
                context={
                    "package_id": package_id,
                    "trade_date": day_key,
                    "coefficients_path": str(coefficients_path),
                },
            )
        base.update(day_dates)
    return base


def _input_data_max_dates_hash(
    payload: dict[str, Any],
    *,
    snapshot: dict[str, Any],
    day_key: str,
    package_id: str,
    coefficients_path: Path,
) -> str | None:
    dates = _input_data_max_dates_for_day(
        payload,
        snapshot=snapshot,
        day_key=day_key,
        package_id=package_id,
        coefficients_path=coefficients_path,
    )
    return canonical_evidence_json_sha256(dates) if dates else None


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _time_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _safe_key(value: str | None) -> str:
    return str(value or "").strip().replace("/", "_").replace("\\", "_").replace("..", "_")


def _hmm_adjusted_reason(reason: str | None) -> str:
    text = str(reason or "").strip()
    if not text:
        return "hmm_adjusted"
    if "hmm" in text.casefold():
        return text
    return f"{text}|hmm_adjusted"


def _validate_preset(
    payload: dict[str, Any],
    *,
    profile: RuntimeHMMProfile,
    path: Path,
    package_id: str,
) -> None:
    payload_preset = payload.get("preset_key")
    if payload_preset is not None and str(payload_preset).strip() != profile.signal_preset:
        raise HMMRuntimeUnavailableError(
            "HMM coefficient artifact preset does not match runtime profile",
            context={
                "package_id": package_id,
                "snapshot_id": profile.model_snapshot_id,
                "runtime_signal_preset": profile.signal_preset,
                "artifact_preset_key": payload_preset,
                "coefficients_path": str(path),
            },
        )


def _resolve_local_path(raw_path: str, *, base_dir: Path | None = None) -> Path | None:
    text = str(raw_path or "").strip()
    if not text:
        return None
    ensure_not_forbidden_worker_workspace_path(text, purpose="selection center HMM artifact path")
    path = Path(text)
    if not path.is_absolute() and base_dir is not None:
        path = base_dir / path
    return path


def _finite_float(value: Any, *, message: str, context: dict[str, Any]) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise HMMRuntimeUnavailableError(message, context=context) from exc
    if not math.isfinite(number):
        raise HMMRuntimeUnavailableError(message, context=context)
    return number


