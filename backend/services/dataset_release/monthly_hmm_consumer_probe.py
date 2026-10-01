"""File-only HMM consumer preflight for a sealed monthly release."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .canonical import canonical_json_bytes
from .monthly_consumer_validation import (
    CONSUMER_PROBE_RESULT_SCHEMA,
    ConsumerProbeRequest,
    MonthlyConsumerValidationError,
)
from .monthly_hmm_derive import FROZEN_HMM_INPUT_SCHEMA, MARKET_VOLUME_DEFINITION


_PROBE_ID = "aistock.monthly.hmm.file_only"
_PROBE_VERSION = "1"
_FILES = {
    "sector_data_h5": "components/factor_h5_static_candidate_v2/sector_data.h5",
    "index_daily_h5": "components/index_context/index_daily.h5",
    "sector_code_map_json": "components/sector_context_candidate_v1/sector_code_map.json",
    "market_context_parquet": "components/sector_context_candidate_v1/market_context.parquet",
    "sector_membership_spans_parquet": "components/sector_context_candidate_v1/sector_membership_spans.parquet",
    "sector_quote_availability_json": "components/sector_context_candidate_v1/sector_quote_availability.json",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_canonical_or_identical(path: Path, value: Mapping[str, Any]) -> Path:
    payload = canonical_json_bytes(value) + b"\n"
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
    except FileExistsError as exc:
        if not path.is_file() or path.is_symlink() or path.read_bytes() != payload:
            raise MonthlyConsumerValidationError("HMM probe result already exists with different bytes") from exc
    return path


def _profile_loader(path: Path) -> Any:
    from backend.services.quantevolver.qe_active_dataset_profile import (
        load_qe_profile,
        validate_controller_snapshot,
    )

    profile = load_qe_profile(path)
    validate_controller_snapshot(profile)
    return profile


def _binding_resolver(**kwargs: Any) -> Mapping[str, Any]:
    from backend.services.quantevolver.qe_active_dataset_profile import (
        resolve_active_dataset_consumer_binding,
    )

    return resolve_active_dataset_consumer_binding(**kwargs)


def _frozen_loader(
    bundle: dict[str, Any],
    *,
    history_start: date,
    test_start: date,
    backtest_end: date,
) -> Mapping[str, Any]:
    from scripts.precompute_hmm_coefficients import load_frozen_coefficient_inputs

    return load_frozen_coefficient_inputs(
        bundle,
        history_start=history_start,
        test_start=test_start,
        backtest_end=backtest_end,
    )


def _raw(profile: Any) -> Mapping[str, Any]:
    value = getattr(profile, "raw", None)
    if not isinstance(value, Mapping):
        raise MonthlyConsumerValidationError("HMM probe profile has no frozen payload")
    return value


def _plain_file(root: Path, relative: str, *, allowed: set[Path]) -> Path:
    requested = root / Path(relative)
    try:
        resolved = requested.resolve(strict=True)
    except OSError as exc:
        raise MonthlyConsumerValidationError(f"HMM frozen input is unavailable: {relative}") from exc
    if (
        requested.is_symlink()
        or not resolved.is_file()
        or not resolved.is_relative_to(root)
        or resolved not in allowed
    ):
        raise MonthlyConsumerValidationError(f"HMM frozen input is not consumer-pinned: {relative}")
    return resolved


@dataclass(frozen=True, slots=True)
class MonthlyHMMConsumerProbe:
    profile_loader: Callable[[Path], Any] = _profile_loader
    binding_resolver: Callable[..., Mapping[str, Any]] = _binding_resolver
    frozen_loader: Callable[..., Mapping[str, Any]] = _frozen_loader
    probe_id: str = _PROBE_ID
    probe_version: str = _PROBE_VERSION

    def run(self, request: ConsumerProbeRequest) -> Path:
        if request.consumer_id != "hmm_file_only":
            raise MonthlyConsumerValidationError("HMM probe received another consumer")
        profile = self.profile_loader(request.profile_path)
        raw = _raw(profile)
        resolved_binding = dict(
            self.binding_resolver(
                consumer_id=request.consumer_id,
                node_id=request.node_id,
                profile=profile,
            )
        )
        if (
            resolved_binding.get("dataset_manifest_sha256") != request.dataset_manifest_sha256
            or resolved_binding.get("candidate_root") != request.node_candidate_root
            or resolved_binding.get("consumer_id") != request.consumer_id
            or resolved_binding.get("legacy_fallback") is not False
        ):
            raise MonthlyConsumerValidationError("HMM consumer binding differs from the monthly release")

        defaults = raw.get("consumers", {}).get("qe", {}).get("defaults")
        if not isinstance(defaults, Mapping):
            raise MonthlyConsumerValidationError("HMM probe window is unavailable")
        try:
            test_start = date.fromisoformat(str(defaults["test_start"]))
            backtest_end = date.fromisoformat(str(defaults["backtest_end"]))
        except (KeyError, ValueError) as exc:
            raise MonthlyConsumerValidationError("HMM probe window is invalid") from exc
        if backtest_end < test_start:
            raise MonthlyConsumerValidationError("HMM probe window is inverted")
        history_start = test_start - timedelta(days=int(3.0 * 365 + 30))

        root = request.controller_candidate_root.resolve(strict=True)
        allowed = {path.resolve(strict=True) for path in request.resolved_component_paths}
        files: dict[str, dict[str, str]] = {}
        evidence: list[dict[str, Any]] = [
            {
                "id": request.binding_path.name,
                "sha256": _sha256(request.binding_path),
                "size": request.binding_path.stat().st_size,
            }
        ]
        for key, relative in _FILES.items():
            path = _plain_file(root, relative, allowed=allowed)
            digest = _sha256(path)
            files[key] = {"relative_path": relative, "sha256": digest}
            evidence.append({"id": relative, "sha256": digest, "size": path.stat().st_size})
        bundle = {
            "schema_version": FROZEN_HMM_INPUT_SCHEMA,
            "dataset_root": str(root),
            "dataset_identity": {
                "generation": raw.get("generation"),
                "release_id": raw.get("release_id"),
                "cutoff": raw.get("cutoff"),
                "dataset_manifest_sha256": request.dataset_manifest_sha256,
            },
            "market_volume_definition": MARKET_VOLUME_DEFINITION,
            "files": files,
        }
        try:
            loaded = self.frozen_loader(
                bundle,
                history_start=history_start,
                test_start=test_start,
                backtest_end=backtest_end,
            )
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise MonthlyConsumerValidationError(f"HMM frozen input preflight failed: {exc}") from exc
        expected_dates = sorted(
            str(value) for value in loaded.get("stock_sector_maps_by_date", {})
        )
        active_sectors = loaded.get("active_sector_codes")
        coefficient_sectors = loaded.get("coefficient_sector_codes")
        if (
            loaded.get("dataset_identity") != bundle["dataset_identity"]
            or not expected_dates
            or expected_dates[0] < test_start.isoformat()
            or expected_dates[-1] != backtest_end.isoformat()
            or not isinstance(active_sectors, list)
            or not isinstance(coefficient_sectors, list)
            or not set(coefficient_sectors).issubset(active_sectors)
        ):
            raise MonthlyConsumerValidationError("HMM frozen input readback is incomplete")

        result = {
            "schema_version": CONSUMER_PROBE_RESULT_SCHEMA,
            "status": "PASS",
            "consumer_id": request.consumer_id,
            "node_id": request.node_id,
            "dataset_manifest_sha256": request.dataset_manifest_sha256,
            "binding_sha256": _sha256(request.binding_path),
            "required_window": {
                "history_start": history_start.isoformat(),
                "test_start": test_start.isoformat(),
                "backtest_end": backtest_end.isoformat(),
            },
            "coverage_counts": {
                "unresolved_count": 0,
                "input_file_count": len(files),
                "trade_date_count": len(expected_dates),
                "active_sector_count": len(active_sectors),
                "coefficient_sector_count": len(coefficient_sectors),
            },
            "adapter": {"id": self.probe_id, "version": self.probe_version},
            "evidence_refs": evidence,
            "side_effect_flags": {
                "outcomes_read": False,
                "training_started": False,
                "experiment_started": False,
                "runtime_action_performed": False,
            },
        }
        return _write_canonical_or_identical(
            request.binding_path.parent / "hmm-file-only-probe-result.json",
            result,
        )


__all__: Sequence[str] = ("MonthlyHMMConsumerProbe",)
