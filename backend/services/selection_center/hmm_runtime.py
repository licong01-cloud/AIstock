"""HMM runtime adjustment for package-based selection.

This runtime consumes completed HMM model snapshots and resolves daily
coefficients automatically. Missing daily coefficients are generated through
the platform HMM service and then reused from the artifact cache; neutral
fallback coefficients are never fabricated when HMM is enabled.
"""

from __future__ import annotations

import json
import math
import threading
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Protocol

from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.runtime_profile import RuntimeHMMProfile
from backend.services.strategy_package.workspace_policy import ensure_not_forbidden_worker_workspace_path
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError


class HMMSnapshotProvider(Protocol):
    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        ...


@dataclass(frozen=True)
class HMMCoefficientArtifact:
    path: Path
    payload: dict[str, Any]


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
    ) -> list[SelectionCandidate]:
        if not profile.enabled:
            return candidates
        if not candidates:
            return candidates
        profile = self._resolve_profile_snapshot(profile)
        if not profile.model_snapshot_id:
            raise StrategyPackageValidationError(
                "HMM runtime profile requires model_snapshot_id or model_config_id when enabled",
                context={"package_id": package_id, "runtime_profile": profile.model_dump(mode="json")},
            )
        if not profile.signal_preset:
            raise StrategyPackageValidationError(
                "HMM runtime profile requires signal_preset when enabled",
                context={"package_id": package_id, "runtime_profile": profile.model_dump(mode="json")},
            )

        snapshot = self._load_snapshot(profile.model_snapshot_id)
        status = str(snapshot.get("status") or "").strip().casefold()
        if status not in self._READY_STATUSES:
            raise DataUnavailableError(
                "HMM snapshot is not ready",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "snapshot_status": snapshot.get("status"),
                },
            )
        model_path = _resolve_local_path(str(snapshot.get("model_path") or ""))
        if model_path is None or not model_path.exists() or not model_path.is_file():
            raise DataUnavailableError(
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
        )
        payload = artifact.payload
        day_key = trade_date.isoformat()
        daily_coefficients = payload.get("daily_coefficients")
        stock_sector_map = payload.get("stock_sector_map")
        if not isinstance(daily_coefficients, dict) or not isinstance(stock_sector_map, dict):
            raise StrategyPackageValidationError(
                "HMM coefficient artifact is missing required keys",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "coefficients_path": str(artifact.path),
                    "keys": sorted(str(key) for key in payload),
                },
            )
        day_coefficients = daily_coefficients.get(day_key)
        if not isinstance(day_coefficients, dict):
            raise DataUnavailableError(
                "HMM coefficient artifact has no coefficients for trade_date",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "trade_date": day_key,
                    "coefficients_path": str(artifact.path),
                },
            )

        adjusted: list[SelectionCandidate] = []
        for candidate in candidates:
            sector_code = stock_sector_map.get(candidate.symbol)
            if sector_code is None or not str(sector_code).strip():
                raise DataUnavailableError(
                    "HMM coefficient artifact is missing stock sector mapping",
                    context={
                        "package_id": package_id,
                        "manifest_sha256": manifest_sha256,
                        "snapshot_id": profile.model_snapshot_id,
                        "trade_date": day_key,
                        "symbol": candidate.symbol,
                        "raw_rank": candidate.rank,
                        "coefficients_path": str(artifact.path),
                    },
                )
            coeff_value = day_coefficients.get(str(sector_code))
            coefficient = _positive_finite_float(
                coeff_value,
                message="HMM sector coefficient must be positive finite",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "trade_date": day_key,
                    "symbol": candidate.symbol,
                    "sector_code": str(sector_code),
                    "coefficient": coeff_value,
                },
            )
            raw_score = _finite_float(
                candidate.score,
                message="HMM runtime requires finite candidate scores",
                context={"package_id": package_id, "symbol": candidate.symbol, "raw_score": candidate.score},
            )
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
            }
            adjusted.append(
                candidate.model_copy(
                    update={
                        "score": raw_score * coefficient,
                        "component_scores": component_scores,
                        "reason": candidate.reason or "hmm_adjusted",
                    }
                )
            )

        adjusted.sort(key=lambda item: (-item.score, item.component_scores.get("raw_rank", item.rank), item.symbol))
        return [item.model_copy(update={"rank": rank}) for rank, item in enumerate(adjusted, start=1)]

    def preflight_coefficients(
        self,
        *,
        trade_date: date,
        profile: RuntimeHMMProfile,
        package_id: str,
    ) -> dict[str, Any]:
        """Validate the HMM artifact shape before live selection inference starts."""

        if not profile.enabled:
            return {"enabled": False}
        profile = self._resolve_profile_snapshot(profile)
        if not profile.model_snapshot_id:
            raise StrategyPackageValidationError(
                "HMM runtime profile requires model_snapshot_id or model_config_id when enabled",
                context={"package_id": package_id, "runtime_profile": profile.model_dump(mode="json")},
            )
        if not profile.signal_preset:
            raise StrategyPackageValidationError(
                "HMM runtime profile requires signal_preset when enabled",
                context={"package_id": package_id, "runtime_profile": profile.model_dump(mode="json")},
            )

        snapshot = self._load_snapshot(profile.model_snapshot_id)
        status = str(snapshot.get("status") or "").strip().casefold()
        if status not in self._READY_STATUSES:
            raise DataUnavailableError(
                "HMM snapshot is not ready",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "snapshot_status": snapshot.get("status"),
                },
            )
        model_path = _resolve_local_path(str(snapshot.get("model_path") or ""))
        if model_path is None or not model_path.exists() or not model_path.is_file():
            raise DataUnavailableError(
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
        )
        daily_coefficients = artifact.payload.get("daily_coefficients")
        stock_sector_map = artifact.payload.get("stock_sector_map")
        if not isinstance(daily_coefficients, dict) or not isinstance(stock_sector_map, dict):
            raise StrategyPackageValidationError(
                "HMM coefficient artifact is missing required keys",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "coefficients_path": str(artifact.path),
                    "keys": sorted(str(key) for key in artifact.payload),
                },
            )
        day_key = trade_date.isoformat()
        day_coefficients = daily_coefficients.get(day_key)
        if not isinstance(day_coefficients, dict):
            raise DataUnavailableError(
                "HMM coefficient artifact has no coefficients for trade_date",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "trade_date": day_key,
                    "coefficients_path": str(artifact.path),
                },
            )
        if not stock_sector_map:
            raise DataUnavailableError(
                "HMM coefficient artifact is missing stock sector mapping",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "trade_date": day_key,
                    "coefficients_path": str(artifact.path),
                },
            )
        return {
            "enabled": True,
            "model_config_id": profile.model_config_id,
            "snapshot_id": profile.model_snapshot_id,
            "snapshot_status": snapshot.get("status"),
            "signal_preset": profile.signal_preset,
            "model_path": str(model_path),
            "coefficients_path": str(artifact.path),
            "trade_date": day_key,
            "sector_count": len(day_coefficients),
            "stock_sector_map_count": len(stock_sector_map),
        }

    def _load_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        provider = self._snapshot_provider
        if provider is None:
            from backend.services.hmm_training_service import HMMTrainingService

            provider = HMMTrainingService()
        snapshot = provider.get_snapshot(snapshot_id)
        if not snapshot:
            raise DataUnavailableError(
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
            raise DataUnavailableError(
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
            raise DataUnavailableError(
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
    ) -> HMMCoefficientArtifact:
        if profile.coefficients_path:
            explicit = _resolve_local_path(profile.coefficients_path, base_dir=model_path.parent)
            if explicit is None or not explicit.exists() or not explicit.is_file():
                raise DataUnavailableError(
                    "explicit HMM coefficient artifact does not exist",
                    context={
                        "package_id": package_id,
                        "snapshot_id": profile.model_snapshot_id,
                        "coefficients_path": profile.coefficients_path,
                    },
                )
            payload = _read_coefficients(explicit, package_id=package_id)
            _validate_preset(payload, profile=profile, path=explicit, package_id=package_id)
            return HMMCoefficientArtifact(path=explicit, payload=payload)

        found = self._load_existing_coefficients(
            model_path=model_path,
            profile=profile,
            trade_date=trade_date,
            package_id=package_id,
        )
        if found["artifact"] is not None:
            return found["artifact"]
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
            payload = _read_coefficients(path, package_id=package_id)
            _validate_preset(payload, profile=profile, path=path, package_id=package_id)
            daily_coefficients = payload.get("daily_coefficients")
            if isinstance(daily_coefficients, dict) and trade_key in daily_coefficients:
                valid.append(HMMCoefficientArtifact(path=path, payload=payload))
        if not valid:
            return {
                "artifact": None,
                "reason": "no_artifact_covers_trade_date",
                "candidate_paths": [str(path) for path in matches],
            }
        if len(valid) > 1:
            raise StrategyPackageValidationError(
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
            raise DataUnavailableError(
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
                raise DataUnavailableError(
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
            raise DataUnavailableError(
                "HMM coefficient auto-generation did not produce an artifact",
                context={
                    "package_id": package_id,
                    "snapshot_id": profile.model_snapshot_id,
                    "trade_date": trade_date.isoformat(),
                    "output_path": result.get("output_path"),
                    "status": result.get("status"),
                },
            )
        payload = _read_coefficients(output_path, package_id=package_id)
        _validate_preset(payload, profile=profile, path=output_path, package_id=package_id)
        return HMMCoefficientArtifact(path=output_path, payload=payload)

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


def _read_coefficients(path: Path, *, package_id: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise StrategyPackageValidationError(
            "HMM coefficient artifact is not valid JSON",
            context={"package_id": package_id, "coefficients_path": str(path)},
        ) from exc
    except OSError as exc:
        raise DataUnavailableError(
            "HMM coefficient artifact cannot be read",
            context={"package_id": package_id, "coefficients_path": str(path)},
        ) from exc
    if not isinstance(payload, dict):
        raise StrategyPackageValidationError(
            "HMM coefficient artifact must be a JSON object",
            context={"package_id": package_id, "coefficients_path": str(path)},
        )
    return payload


def _safe_key(value: str | None) -> str:
    return str(value or "").strip().replace("/", "_").replace("\\", "_").replace("..", "_")


def _validate_preset(
    payload: dict[str, Any],
    *,
    profile: RuntimeHMMProfile,
    path: Path,
    package_id: str,
) -> None:
    payload_preset = payload.get("preset_key")
    if payload_preset is not None and str(payload_preset).strip() != profile.signal_preset:
        raise StrategyPackageValidationError(
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
        raise StrategyPackageValidationError(message, context=context) from exc
    if not math.isfinite(number):
        raise StrategyPackageValidationError(message, context=context)
    return number


def _positive_finite_float(value: Any, *, message: str, context: dict[str, Any]) -> float:
    number = _finite_float(value, message=message, context=context)
    if number <= 0:
        raise StrategyPackageValidationError(message, context=context)
    return number
