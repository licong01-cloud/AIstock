"""Build one fully validated active-profile v4 candidate from sealed release bytes."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from typing import Any, Mapping

from .canonical import canonical_json_bytes, ensure_sha256
from .monthly_worker import ProducerContext
from .profile_contract import (
    ACTIVE_PROFILE_SCHEMA_V4,
    ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS,
)
from .shared_sector_context import SECTOR_CONTEXT_COMPONENT_ROOT


PROFILE_SCHEMA = ACTIVE_PROFILE_SCHEMA_V4
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_POOL_FILES = {
    "stock_universe": "stock_universe.txt",
    "csi300": "index_pool__csi300.txt",
    "csi500": "index_pool__csi500.txt",
    "csi1000": "index_pool__csi1000.txt",
    "star50": "index_pool__star50.txt",
    "star100": "index_pool__star100.txt",
}
_POOL_LABELS = {
    "stock_universe": "全市场股票池",
    "csi300": "沪深300",
    "csi500": "中证500",
    "csi1000": "中证1000",
    "star50": "科创50",
    "star100": "科创100",
}
class MonthlyProfileCandidateError(RuntimeError):
    """The sealed candidate cannot be represented by one valid profile."""


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(
        int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_canonical(path: Path, *, label: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyProfileCandidateError(f"{label} is unreadable") from exc
    if not isinstance(value, dict) or raw != canonical_json_bytes(value) + b"\n":
        raise MonthlyProfileCandidateError(f"{label} is not canonical JSON")
    return value


def _plain_file(root: Path, relative: str, *, label: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise MonthlyProfileCandidateError(f"{label} path is invalid")
    requested = root / path
    current = root
    for part in path.parts:
        current /= part
        if _is_link(current):
            raise MonthlyProfileCandidateError(f"{label} path is linked")
    try:
        resolved = requested.resolve(strict=True)
    except OSError as exc:
        raise MonthlyProfileCandidateError(f"{label} is unavailable") from exc
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise MonthlyProfileCandidateError(f"{label} must be a regular candidate file")
    return resolved


def _manifest_identity(value: Mapping[str, Any]) -> str:
    unsigned = dict(value)
    unsigned.pop("dataset_manifest_sha256", None)
    return hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()


def _calendar(path: Path, *, cutoff: date) -> tuple[str, str]:
    try:
        values = [date.fromisoformat(row.strip()) for row in path.read_text(encoding="utf-8").splitlines() if row.strip()]
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise MonthlyProfileCandidateError("daily calendar is invalid") from exc
    if len(values) < 2 or values != sorted(set(values)) or values[-1] != cutoff:
        raise MonthlyProfileCandidateError("daily calendar does not close the target cutoff")
    return values[0].isoformat(), values[-2].isoformat()


@dataclass(frozen=True, slots=True)
class SealedMonthlyProfileCandidateBuilder:
    """Derive every mutable profile pin from one sealed candidate."""

    def execute(
        self,
        context: ProducerContext,
        *,
        release_closure_path: Path,
        derived_asset_registry_path: Path,
    ) -> Path:
        if context.stage != "LOCAL_VALIDATE":
            raise MonthlyProfileCandidateError("profile builder received another stage")
        candidate = Path(str(context.plan.get("candidate_root") or ""))
        output = Path(str(context.plan.get("profile_candidate") or ""))
        predecessor = context.plan.get("predecessor")
        predecessor_ref = context.plan.get("predecessor_profile_ref")
        if (
            not candidate.is_absolute()
            or not output.is_absolute()
            or not isinstance(predecessor, Mapping)
            or not isinstance(predecessor_ref, Mapping)
        ):
            raise MonthlyProfileCandidateError("profile plan identity is incomplete")
        try:
            root = candidate.resolve(strict=True)
        except OSError as exc:
            raise MonthlyProfileCandidateError("candidate root is unavailable") from exc
        if _is_link(candidate) or not root.is_dir() or output.exists():
            raise MonthlyProfileCandidateError("profile target or candidate root is invalid")
        predecessor_path = Path(str(predecessor.get("profile_path") or ""))
        if (
            not predecessor_path.is_absolute()
            or _is_link(predecessor_path)
            or not predecessor_path.is_file()
            or _sha256(predecessor_path) != predecessor_ref.get("sha256")
            or predecessor_path.stat().st_size != predecessor_ref.get("size")
        ):
            raise MonthlyProfileCandidateError("predecessor profile bytes differ")
        base = _read_canonical(predecessor_path, label="predecessor profile")

        manifest_path = _plain_file(root, "qe_dataset_manifest.json", label="dataset manifest")
        manifest = _read_canonical(manifest_path, label="dataset manifest")
        manifest_sha = ensure_sha256(
            str(manifest.get("dataset_manifest_sha256") or ""),
            field="dataset_manifest_sha256",
        )
        if (
            _manifest_identity(manifest) != manifest_sha
            or manifest.get("release_id") != context.plan.get("release_id")
            or manifest.get("revision") != context.plan.get("revision")
            or manifest.get("cutoff_trade_date") != context.plan.get("target_cutoff")
        ):
            raise MonthlyProfileCandidateError("dataset manifest identity differs")
        manifest_components = manifest.get("components")
        if not isinstance(manifest_components, Mapping):
            raise MonthlyProfileCandidateError("dataset manifest components are invalid")

        def pinned(relative: str, *, label: str) -> Path:
            path = _plain_file(root, relative, label=label)
            matches = [
                raw
                for raw in manifest_components.values()
                if isinstance(raw, Mapping) and raw.get("path") == relative
            ]
            if (
                len(matches) != 1
                or matches[0].get("sha256") != _sha256(path)
                or matches[0].get("size") != path.stat().st_size
            ):
                raise MonthlyProfileCandidateError(f"{label} is not manifest-pinned")
            return path

        day_root = "components/daily_bin_candidate"
        minute_root = "components/minute_bin_candidate"
        factor_root = "components/factor_h5_static_candidate_v2"
        index_root = "components/index_context"
        suspend_root = "components/suspend_d_daily_candidate_v2"
        day_calendar = pinned(f"{day_root}/calendars/day.txt", label="daily calendar")
        day_instruments = pinned(f"{day_root}/instruments/all.txt", label="daily instruments")
        day_meta_path = pinned(f"{day_root}/meta_export.json", label="daily metadata")
        minute_calendar = pinned(f"{minute_root}/calendars/1min.txt", label="minute calendar")
        minute_instruments = pinned(f"{minute_root}/instruments/all.txt", label="minute instruments")
        minute_meta_path = pinned(f"{minute_root}/meta_export.json", label="minute metadata")
        factor_meta_path = pinned(f"{factor_root}/meta.json", label="factor metadata")
        sector_data = pinned(f"{factor_root}/sector_data.h5", label="sector data")
        index_data = pinned(f"{index_root}/index_daily.h5", label="index data")
        index_meta_path = pinned(f"{index_root}/meta.json", label="index metadata")
        suspend_data = pinned(f"{suspend_root}/suspend_d.parquet", label="suspend data")
        suspend_meta_path = pinned(f"{suspend_root}/meta.json", label="suspend metadata")
        benchmark = pinned(f"{day_root}/instruments/benchmark.txt", label="benchmark instruments")
        coverage = pinned(
            "reports/qe_index_pool_coverage_receipt.json",
            label="QE six-pool coverage receipt",
        )
        try:
            registry_relative = derived_asset_registry_path.resolve(strict=True).relative_to(root)
            closure_relative = release_closure_path.resolve(strict=True).relative_to(root)
        except (OSError, ValueError) as exc:
            raise MonthlyProfileCandidateError(
                "registry and closure must be regular candidate files"
            ) from exc
        registry = _plain_file(root, registry_relative.as_posix(), label="derived asset registry")
        closure = _plain_file(root, closure_relative.as_posix(), label="release closure")
        derive_scope = context.prior_receipts.get("DERIVE", {}).get("scope")
        if (
            not isinstance(derive_scope, Mapping)
            or derive_scope.get("derived_asset_registry_sha256") != _sha256(registry)
        ):
            raise MonthlyProfileCandidateError("derived asset registry identity differs")
        sector_receipt_path = pinned(
            f"{SECTOR_CONTEXT_COMPONENT_ROOT}/component_receipt.json",
            label="sector context receipt",
        )
        sector_receipt = _read_canonical(sector_receipt_path, label="sector context receipt")

        cutoff = date.fromisoformat(str(context.plan["target_cutoff"]))
        dataset_start, backtest_end = _calendar(day_calendar, cutoff=cutoff)
        day_meta = _read_canonical(day_meta_path, label="daily metadata")
        minute_meta = _read_canonical(minute_meta_path, label="minute metadata")
        factor_meta_raw = _read_canonical(factor_meta_path, label="factor metadata")
        index_meta = _read_canonical(index_meta_path, label="index metadata")
        suspend_meta = _read_canonical(suspend_meta_path, label="suspend metadata")
        coverage_payload = _read_canonical(coverage, label="QE coverage receipt")
        if (
            coverage_payload.get("schema_version") != "qe_index_pool_coverage_receipt_v1"
            or coverage_payload.get("release_id") != context.plan.get("release_id")
            or coverage_payload.get("cutoff") != cutoff.isoformat()
            or set(coverage_payload.get("pools") or {}) != set(_POOL_FILES)
        ):
            raise MonthlyProfileCandidateError("QE coverage receipt identity differs")

        factor_meta = {
            "schema_version": str(factor_meta_raw.get("schema_version") or ""),
            "start": str(factor_meta_raw.get("start") or ""),
            "end": str(factor_meta_raw.get("end") or ""),
            "universe_key": str(factor_meta_raw.get("universe_key") or ""),
        }
        if factor_meta != {
            "schema_version": "qe_direct_factor_h5_static_v2",
            "start": dataset_start,
            "end": cutoff.isoformat(),
            "universe_key": "aistock_equity_pit_canonical_v2",
        }:
            raise MonthlyProfileCandidateError("factor metadata identity differs")
        if index_meta.get("end") != cutoff.isoformat() or not isinstance(index_meta.get("codes"), list):
            raise MonthlyProfileCandidateError("index metadata identity differs")
        if (
            suspend_meta.get("schema_version") != "qe_direct_suspend_d_v1"
            or suspend_meta.get("end") != cutoff.isoformat()
            or suspend_meta.get("start") != dataset_start
        ):
            raise MonthlyProfileCandidateError("suspend metadata identity differs")

        st_pit = manifest.get("st_pit_manifest")
        if not isinstance(st_pit, Mapping) or st_pit.get("cutoff_trade_date") != cutoff.isoformat():
            raise MonthlyProfileCandidateError("ST-PIT manifest identity differs")
        sidecars = st_pit.get("index_membership_sidecars")
        if not isinstance(sidecars, Mapping) or set(sidecars) != set(_POOL_FILES):
            raise MonthlyProfileCandidateError("ST-PIT sidecar set differs")
        universes: dict[str, Any] = {}
        for pool_id, filename in _POOL_FILES.items():
            relative = f"stock_pools/{filename}"
            sidecar = pinned(relative, label=f"{pool_id} sidecar")
            raw_pin = sidecars[pool_id]
            if (
                not isinstance(raw_pin, Mapping)
                or raw_pin.get("path") != relative
                or raw_pin.get("sha256") != _sha256(sidecar)
                or raw_pin.get("size") != sidecar.stat().st_size
            ):
                raise MonthlyProfileCandidateError(f"{pool_id} ST-PIT pin differs")
            universes[pool_id] = {
                "filename": filename,
                "label": _POOL_LABELS[pool_id],
                "membership_revision": str(st_pit.get("snapshot_id") or ""),
                "sha256": _sha256(sidecar),
            }
        stock_in_day = pinned(
            f"{day_root}/instruments/stock_universe.txt",
            label="daily stock universe",
        )
        if _sha256(stock_in_day) != universes["stock_universe"]["sha256"]:
            raise MonthlyProfileCandidateError("daily and shared stock universe differ")

        code_map = sector_receipt.get("sector_code_map")
        market = sector_receipt.get("market_context")
        membership = sector_receipt.get("membership")
        quote = sector_receipt.get("quote_availability")
        sector_info = sector_receipt.get("sector_data")
        if not all(isinstance(value, Mapping) for value in (code_map, market, membership, quote, sector_info)):
            raise MonthlyProfileCandidateError("sector context receipt is incomplete")
        authority = code_map.get("authority")
        if not isinstance(authority, Mapping):
            raise MonthlyProfileCandidateError("sector mapping authority is incomplete")
        sector_pins = {
            "schema_version": "aistock_sector_context_pins_v1",
            "component_root": SECTOR_CONTEXT_COMPONENT_ROOT,
            "code_map_file": "sector_code_map.json",
            "code_map_sha256": str(code_map["sha256"]),
            "code_map_digest": str(code_map["code_map_digest"]),
            "market_context_file": "market_context.parquet",
            "market_context_sha256": str(market["sha256"]),
            "market_volume_definition": str(market["definition"]),
            "market_start": str(market["start"]),
            "market_end": str(market["end"]),
            "membership_file": "sector_membership_spans.parquet",
            "membership_sha256": str(membership["sha256"]),
            "membership_start": str(membership["start"]),
            "membership_end": str(membership["end"]),
            "quote_availability_file": "sector_quote_availability.json",
            "quote_availability_sha256": str(quote["sha256"]),
            "quote_availability_digest": str(quote["canonical_digest"]),
            "quote_availability_schema": str(quote["schema_version"]),
            "receipt_file": "component_receipt.json",
            "receipt_sha256": _sha256(sector_receipt_path),
            "sector_data_sha256": _sha256(sector_data),
            "source_dataset_manifest_sha256": str(
                sector_receipt["source_dataset_manifest_sha256"]
            ),
            "authority_id": str(authority["authority_id"]),
            "authority_sha256": str(authority["authority_sha256"]),
        }
        for filename, key in (
            ("sector_code_map.json", "code_map_sha256"),
            ("market_context.parquet", "market_context_sha256"),
            ("sector_membership_spans.parquet", "membership_sha256"),
            ("sector_quote_availability.json", "quote_availability_sha256"),
        ):
            sector_file = pinned(
                f"{SECTOR_CONTEXT_COMPONENT_ROOT}/{filename}",
                label=f"sector {filename}",
            )
            if _sha256(sector_file) != sector_pins[key]:
                raise MonthlyProfileCandidateError(f"sector {filename} pin differs")

        defaults = copy.deepcopy(base.get("consumers", {}).get("qe", {}).get("defaults"))
        if not isinstance(defaults, dict):
            raise MonthlyProfileCandidateError("predecessor QE defaults are unavailable")
        defaults.update(
            {
                "test_end": cutoff.isoformat(),
                "signal_end": cutoff.isoformat(),
                "backtest_end": backtest_end,
            }
        )
        day_pins = {
            field: str(day_meta.get(field) or "")
            for field in ("snapshot_id", "universe_key", "rule_version")
        }
        day_pins.update(
            {
                "instruments_sha256": _sha256(day_instruments),
                "calendar_sha256": _sha256(day_calendar),
                "meta_export_sha256": _sha256(day_meta_path),
            }
        )
        minute_pins = {
            field: str(minute_meta.get(field) or "")
            for field in ("snapshot_id", "universe_key", "rule_version")
        }
        minute_pins.update(
            {
                "instruments_sha256": _sha256(minute_instruments),
                "calendar_sha256": _sha256(minute_calendar),
                "meta_export_sha256": _sha256(minute_meta_path),
            }
        )
        components = {
            "factor_meta": factor_meta,
            "factor_meta_sha256": _sha256(factor_meta_path),
            "day_pins": day_pins,
            "minute_pins": minute_pins,
            "index_pins": {
                "codes": list(index_meta["codes"]),
                "max_date": cutoff.isoformat(),
                "sha256": _sha256(index_data),
            },
            "suspend_pins": {
                "dataset_id": "suspend_d_daily_candidate_v2",
                "schema_version": str(suspend_meta["schema_version"]),
                "source_contract": str(suspend_meta["source_table"]),
                "metadata_sha256": _sha256(suspend_meta_path),
                "parquet_sha256": _sha256(suspend_data),
            },
            "benchmark_instruments_sha256": _sha256(benchmark),
            "sector_context_pins": sector_pins,
            "dataset_manifest_sha256": manifest_sha,
            "dataset_manifest_file_sha256": _sha256(manifest_path),
            "derived_asset_registry_path": str(registry),
            "derived_asset_registry_sha256": _sha256(registry),
            "release_closure_path": str(closure),
            "release_closure_sha256": _sha256(closure),
        }
        qe = {
            "defaults": defaults,
            "default_universe": {"mode": "stock_universe", "pool_ids": []},
            "universes": universes,
            "coverage_receipt_sha256": _sha256(coverage),
            "required_components": sorted(ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS["qe"]),
        }
        profile = {
            "schema_version": PROFILE_SCHEMA,
            "generation": str(context.plan.get("generation") or ""),
            "release_id": str(context.plan.get("release_id") or ""),
            "cutoff": cutoff.isoformat(),
            "controller_paths": {
                "candidate_root": str(root),
                "stock_pool_root": str(root / "stock_pools"),
                "coverage_receipt_path": str(coverage),
            },
            "components": components,
            "node_bindings": {
                node: {"candidate_root": str(path)}
                for node, path in sorted(dict(context.plan.get("node_roots") or {}).items())
            },
            "consumers": {
                **{
                    name: {"required_components": sorted(requirements)}
                    for name, requirements in sorted(
                        ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS.items()
                    )
                    if name != "qe"
                },
                "qe": qe,
            },
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        payload = canonical_json_bytes(profile) + b"\n"
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{output.name}.", suffix=".validate", dir=output.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            from backend.services.quantevolver.qe_active_dataset_profile import (
                load_qe_profile,
                validate_controller_snapshot,
            )

            validated = load_qe_profile(temporary)
            validate_controller_snapshot(validated)
            with output.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            temporary.unlink(missing_ok=True)
        return output.resolve(strict=True)


__all__ = (
    "MonthlyProfileCandidateError",
    "PROFILE_SCHEMA",
    "SealedMonthlyProfileCandidateBuilder",
)
