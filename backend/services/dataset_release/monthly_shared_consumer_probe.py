"""Shared file-binding probes for non-QE monthly dataset consumers."""

from __future__ import annotations

from dataclasses import dataclass
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
from .profile_contract import ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS


SHARED_DATA_CONSUMERS = (
    "factor_research",
    "selection",
    "advisory",
    "position_timing",
    "unified_backtest",
)
_PROBE_ID = "aistock.monthly.shared_dataset.file_binding"
_PROBE_VERSION = "1"
_SENTINELS = {
    "manifest": ("qe_dataset_manifest.json",),
    "day": (
        "components/daily_bin_candidate/calendars/day.txt",
        "components/daily_bin_candidate/instruments/all.txt",
        "components/daily_bin_candidate/meta_export.json",
    ),
    "minute": (
        "components/minute_bin_candidate/calendars/1min.txt",
        "components/minute_bin_candidate/instruments/all.txt",
        "components/minute_bin_candidate/meta_export.json",
    ),
    "factor": (
        "components/factor_h5_static_candidate_v2/meta.json",
        "components/factor_h5_static_candidate_v2/static_factors.parquet",
    ),
    "index": (
        "components/index_context/meta.json",
        "components/index_context/index_daily.h5",
    ),
    "suspend": (
        "components/suspend_d_daily_candidate_v2/meta.json",
        "components/suspend_d_daily_candidate_v2/suspend_d.parquet",
    ),
    "benchmark": ("components/daily_bin_candidate/instruments/benchmark.txt",),
    "coverage": ("reports/qe_index_pool_coverage_receipt.json",),
    "stock_pools": ("stock_pools/stock_universe.txt",),
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
            raise MonthlyConsumerValidationError("shared consumer probe result bytes differ") from exc
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


def _read_sentinel(path: Path) -> int:
    """Use the production file readers without loading complete factor tables."""

    suffix = path.suffix.lower()
    if suffix == ".json":
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, Mapping):
            raise ValueError("JSON sentinel is not an object")
        return len(value)
    if suffix in {".txt", ".csv"}:
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if not lines:
            raise ValueError("text sentinel has no rows")
        return len(lines)
    if suffix == ".parquet":
        import pyarrow.parquet as pq

        metadata = pq.ParquetFile(path).metadata
        if metadata is None or metadata.num_rows <= 0 or metadata.num_columns <= 0:
            raise ValueError("Parquet sentinel has no readable rows or columns")
        return int(metadata.num_rows)
    if suffix == ".h5":
        import pandas as pd

        with pd.HDFStore(path, mode="r") as store:
            keys = store.keys()
            if not keys:
                raise ValueError("HDF sentinel has no keys")
            storer = store.get_storer(keys[0])
            rows = int(getattr(storer, "nrows", 0) or 0)
            if rows <= 0:
                # Fixed-format stores do not always expose nrows.  Read only a
                # single object in that case; index_daily is intentionally
                # small and this is a release-time probe, not a task hot path.
                value = store.get(keys[0])
                rows = len(value)
            if rows <= 0:
                raise ValueError("HDF sentinel has no rows")
            return rows
    raise ValueError(f"unsupported sentinel type: {path.name}")


def _raw(profile: Any) -> Mapping[str, Any]:
    value = getattr(profile, "raw", None)
    if not isinstance(value, Mapping):
        raise MonthlyConsumerValidationError("shared consumer profile has no frozen payload")
    return value


@dataclass(frozen=True, slots=True)
class MonthlySharedDatasetConsumerProbe:
    profile_loader: Callable[[Path], Any] = _profile_loader
    binding_resolver: Callable[..., Mapping[str, Any]] = _binding_resolver
    sentinel_reader: Callable[[Path], int] = _read_sentinel
    probe_id: str = _PROBE_ID
    probe_version: str = _PROBE_VERSION

    def run(self, request: ConsumerProbeRequest) -> Path:
        if request.consumer_id not in SHARED_DATA_CONSUMERS:
            raise MonthlyConsumerValidationError("shared dataset probe received another consumer")
        profile = self.profile_loader(request.profile_path)
        raw = _raw(profile)
        binding = dict(
            self.binding_resolver(
                consumer_id=request.consumer_id,
                node_id=request.node_id,
                profile=profile,
            )
        )
        required = sorted(ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[request.consumer_id])
        if (
            binding.get("dataset_manifest_sha256") != request.dataset_manifest_sha256
            or binding.get("candidate_root") != request.node_candidate_root
            or binding.get("consumer_id") != request.consumer_id
            or binding.get("required_components") != required
            or binding.get("resolved_once") is not True
            or binding.get("legacy_fallback") is not False
        ):
            raise MonthlyConsumerValidationError("shared consumer binding differs from the monthly release")

        root = request.controller_candidate_root.resolve(strict=True)
        allowed = {path.resolve(strict=True) for path in request.resolved_component_paths}
        evidence = [
            {
                "id": request.binding_path.name,
                "sha256": _sha256(request.binding_path),
                "size": request.binding_path.stat().st_size,
            }
        ]
        rows_read = 0
        sentinels_read = 0
        for component in required:
            if component not in _SENTINELS:
                raise MonthlyConsumerValidationError(
                    f"shared consumer has no registered sentinel: {component}"
                )
            for relative in _SENTINELS[component]:
                requested = root / Path(relative)
                try:
                    path = requested.resolve(strict=True)
                except OSError as exc:
                    raise MonthlyConsumerValidationError(
                        f"shared consumer sentinel is unavailable: {relative}"
                    ) from exc
                if (
                    requested.is_symlink()
                    or not path.is_file()
                    or not path.is_relative_to(root)
                    or path not in allowed
                ):
                    raise MonthlyConsumerValidationError(
                        f"shared consumer sentinel is not contract-pinned: {relative}"
                    )
                try:
                    rows_read += self.sentinel_reader(path)
                except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
                    raise MonthlyConsumerValidationError(
                        f"shared consumer cannot read sentinel: {relative}: {exc}"
                    ) from exc
                sentinels_read += 1
                evidence.append(
                    {"id": relative, "sha256": _sha256(path), "size": path.stat().st_size}
                )

        defaults = raw.get("consumers", {}).get("qe", {}).get("defaults")
        if not isinstance(defaults, Mapping):
            raise MonthlyConsumerValidationError("shared consumer window is unavailable")
        window = {
            key: str(defaults[key])
            for key in (
                "train_start",
                "train_end",
                "valid_start",
                "valid_end",
                "test_start",
                "backtest_end",
                "test_end",
            )
            if key in defaults
        }
        if not window:
            raise MonthlyConsumerValidationError("shared consumer window is empty")

        result = {
            "schema_version": CONSUMER_PROBE_RESULT_SCHEMA,
            "status": "PASS",
            "consumer_id": request.consumer_id,
            "node_id": request.node_id,
            "dataset_manifest_sha256": request.dataset_manifest_sha256,
            "binding_sha256": _sha256(request.binding_path),
            "required_window": window,
            "coverage_counts": {
                "unresolved_count": 0,
                "required_component_count": len(required),
                "sentinel_file_count": sentinels_read,
                "readable_row_count": rows_read,
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
            request.binding_path.parent / f"{request.consumer_id}-shared-probe-result.json",
            result,
        )


def monthly_shared_dataset_consumer_probes() -> Mapping[str, MonthlySharedDatasetConsumerProbe]:
    return {
        consumer_id: MonthlySharedDatasetConsumerProbe()
        for consumer_id in SHARED_DATA_CONSUMERS
    }


__all__: Sequence[str] = (
    "MonthlySharedDatasetConsumerProbe",
    "SHARED_DATA_CONSUMERS",
    "monthly_shared_dataset_consumer_probes",
)
