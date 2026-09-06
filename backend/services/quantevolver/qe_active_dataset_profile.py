"""Resolve the single active QE dataset profile into immutable run bindings.

The profile is a small repository-external control file.  It is read only while
new work is created; workers consume the persisted direct-v2 binding and never
re-read this module or query a business-data table.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import posixpath
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .qe_dataset_contract import (
    QE_DATASET_START_DATE,
    QE_DIRECT_V2_DATASET_BINDING_PARAM,
    QE_DIRECT_V2_DATASET_BINDING_SCHEMA_V3,
    QEDirectV2DatasetBinding,
)


ACTIVE_PROFILE_ENV = "AISTOCK_ACTIVE_DATASET_PROFILE_PATH"
ACTIVE_PROFILE_SCHEMA = "aistock_active_dataset_profile_v1"
UNIVERSE_COVERAGE_SCHEMA = "qe_index_pool_coverage_receipt_v1"
QE_RUN_STOCK_POOL_CONTENT_PARAM = "_qe_run_stock_pool_content"
QE_RUN_COVERAGE_RECEIPT_PARAM = "_qe_universe_coverage_receipt"
QE_ACTIVE_PROFILE_SUMMARY_PARAM = "_qe_active_dataset_summary"
QE_INTERNAL_DATASET_PARAMS = frozenset(
    {
        QE_DIRECT_V2_DATASET_BINDING_PARAM,
        QE_RUN_STOCK_POOL_CONTENT_PARAM,
        QE_RUN_COVERAGE_RECEIPT_PARAM,
        QE_ACTIVE_PROFILE_SUMMARY_PARAM,
    }
)

LEGACY_QE_DEFAULT_DATA_SPLIT = {
    "train_start": "2018-08-01",
    "train_end": "2022-12-31",
    "valid_start": "2023-01-01",
    "valid_end": "2024-06-30",
    "test_start": "2024-07-01",
    "test_end": "2026-06-30",
    "backtest_end": "2026-06-29",
}

_TOP_LEVEL_FIELDS = {
    "schema_version",
    "generation",
    "release_id",
    "cutoff",
    "controller_paths",
    "components",
    "node_bindings",
    "consumers",
}
_DEFAULT_FIELDS = {*LEGACY_QE_DEFAULT_DATA_SPLIT, "signal_end"}
_UNIVERSE_MODES = {"stock_universe", "single_index", "index_union"}
_POOL_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,31}$")
_SIDECAR_RE = re.compile(r"^(?:stock_universe|index_pool__[a-z0-9_]+)\.txt$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")


class QEActiveDatasetProfileError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None):
        self.reason_code = reason_code
        self.context = dict(context or {})
        super().__init__(f"reason_code={reason_code}: {message}")


def _fail(reason_code: str, message: str, **context: Any) -> QEActiveDatasetProfileError:
    return QEActiveDatasetProfileError(reason_code, message, context=context)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _require_sha256(value: Any, *, field: str) -> str:
    text = str(value or "")
    if not _SHA256_RE.fullmatch(text):
        raise _fail("qe_active_dataset_profile_invalid", f"{field} must be a lowercase sha256")
    return text


def _date(value: Any, *, field: str) -> dt.date:
    try:
        return dt.date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise _fail("qe_active_dataset_profile_invalid", f"{field} must be YYYY-MM-DD") from exc


def _require_exact_mapping(value: Any, *, fields: set[str], field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise _fail(
            "qe_active_dataset_profile_invalid",
            f"{field} fields differ",
            expected=sorted(fields),
            actual=sorted(value) if isinstance(value, Mapping) else type(value).__name__,
        )
    return dict(value)


def _canonical_profile_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _is_link_or_junction(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _require_external_regular_file(path: Path, *, field: str) -> None:
    if not path.is_absolute():
        raise _fail("qe_active_dataset_profile_invalid", f"{field} must be an absolute path")
    project_root = Path(__file__).resolve().parents[3]
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise _fail("qe_active_dataset_profile_missing", f"{field} is not readable: {path}") from exc
    if _is_link_or_junction(path) or not resolved.is_file():
        raise _fail("qe_active_dataset_profile_invalid", f"{field} must be a regular non-link file")
    try:
        resolved.relative_to(project_root)
    except ValueError:
        return
    raise _fail("qe_active_dataset_profile_invalid", f"{field} must be outside the repository")


def _require_external_directory(path: Path, *, field: str) -> None:
    if not path.is_absolute():
        raise _fail("qe_active_dataset_profile_invalid", f"{field} must be an absolute path")
    project_root = Path(__file__).resolve().parents[3]
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise _fail("qe_active_dataset_profile_missing", f"{field} is not readable: {path}") from exc
    if _is_link_or_junction(path) or not resolved.is_dir():
        raise _fail("qe_active_dataset_profile_invalid", f"{field} must be a regular non-link directory")
    try:
        resolved.relative_to(project_root)
    except ValueError:
        return
    raise _fail("qe_active_dataset_profile_invalid", f"{field} must be outside the repository")


def _require_posix_root(value: Any, *, field: str) -> str:
    text = str(value or "")
    if (
        not text.startswith("/")
        or text == "/"
        or "\\" in text
        or "\x00" in text
        or posixpath.normpath(text) != text
    ):
        raise _fail("qe_active_dataset_profile_invalid", f"{field} must be a canonical POSIX root")
    return text


@dataclass(frozen=True, slots=True)
class UniverseSelection:
    mode: str = "stock_universe"
    pool_ids: tuple[str, ...] = ()

    @classmethod
    def from_value(cls, value: Mapping[str, Any] | None) -> "UniverseSelection":
        raw = dict(value or {"mode": "stock_universe", "pool_ids": []})
        if set(raw) != {"mode", "pool_ids"}:
            raise _fail("qe_universe_mode_invalid", "universe_selection fields differ")
        mode = str(raw.get("mode") or "")
        raw_pool_ids = raw.get("pool_ids")
        if mode not in _UNIVERSE_MODES or not isinstance(raw_pool_ids, list):
            raise _fail("qe_universe_mode_invalid", "universe mode or pool_ids is invalid")
        if any(not isinstance(pool_id, str) or not _POOL_ID_RE.fullmatch(pool_id) for pool_id in raw_pool_ids):
            raise _fail("qe_universe_pool_unknown", "pool_ids must be stable public codes")
        pool_ids = tuple(sorted(set(raw_pool_ids)))
        if mode == "stock_universe" and pool_ids:
            raise _fail("qe_universe_mode_invalid", "stock_universe does not accept pool_ids")
        if mode == "single_index" and len(pool_ids) != 1:
            raise _fail("qe_universe_mode_invalid", "single_index requires exactly one pool_id")
        if mode == "index_union" and not pool_ids:
            raise _fail("qe_universe_mode_invalid", "index_union requires at least one pool_id")
        return cls(mode=mode, pool_ids=pool_ids)

    def as_dict(self) -> dict[str, Any]:
        return {"mode": self.mode, "pool_ids": list(self.pool_ids)}


@dataclass(frozen=True, slots=True)
class QEActiveDatasetProfile:
    raw: Mapping[str, Any]
    profile_path: Path
    profile_sha256: str
    generation: str
    release_id: str
    cutoff: dt.date
    controller_candidate_root: Path
    controller_stock_pool_root: Path
    coverage_receipt_path: Path
    coverage_receipt: Mapping[str, Any]
    coverage_receipt_bytes: bytes

    @property
    def qe(self) -> dict[str, Any]:
        return dict(self.raw["consumers"]["qe"])

    @property
    def defaults(self) -> dict[str, str]:
        return {key: str(self.qe["defaults"][key]) for key in LEGACY_QE_DEFAULT_DATA_SPLIT}

    @property
    def universes(self) -> dict[str, dict[str, Any]]:
        return {str(key): dict(value) for key, value in self.qe["universes"].items()}

    def summary(self) -> dict[str, Any]:
        default_selection = UniverseSelection.from_value(self.qe["default_universe"])
        pools = []
        for pool_id, value in sorted(self.universes.items()):
            pools.append(
                {
                    "pool_id": pool_id,
                    "label": value["label"],
                    "available_start": self.coverage_receipt["pools"][pool_id]["available_start"],
                    "available_end": self.coverage_receipt["pools"][pool_id]["available_end"],
                    "gap_count": len(self.coverage_receipt["pools"][pool_id]["gaps"]),
                }
            )
        return {
            "mode": "active_profile",
            "generation": self.generation,
            "release_id": self.release_id,
            "cutoff": self.cutoff.isoformat(),
            "defaults": self.defaults,
            "signal_end": str(self.qe["defaults"]["signal_end"]),
            "default_universe": default_selection.as_dict(),
            "available_nodes": sorted(self.raw["node_bindings"]),
            "universes": pools,
        }


@dataclass(frozen=True, slots=True)
class ResolvedQEDataset:
    data_split: Mapping[str, str]
    binding: QEDirectV2DatasetBinding
    profile_summary: Mapping[str, Any]
    profile_internal_summary: Mapping[str, Any]
    stock_pool_content: str | None
    coverage_receipt_content: str
    outcome_observable_end: str

    def apply(self, custom_params: Mapping[str, Any] | None) -> dict[str, Any]:
        params = dict(custom_params or {})
        params[QE_DIRECT_V2_DATASET_BINDING_PARAM] = self.binding.as_dict()
        params[QE_ACTIVE_PROFILE_SUMMARY_PARAM] = dict(self.profile_internal_summary)
        params[QE_RUN_COVERAGE_RECEIPT_PARAM] = self.coverage_receipt_content
        if self.stock_pool_content is not None:
            params[QE_RUN_STOCK_POOL_CONTENT_PARAM] = self.stock_pool_content
        params["stock_pool"] = self.binding.selection_pins["instrument_name"]
        return params


def _validate_profile(value: Mapping[str, Any], *, path: Path, payload: bytes) -> QEActiveDatasetProfile:
    root = _require_exact_mapping(value, fields=_TOP_LEVEL_FIELDS, field="profile")
    if root["schema_version"] != ACTIVE_PROFILE_SCHEMA:
        raise _fail("qe_active_dataset_profile_invalid", "schema_version differs")
    generation = str(root["generation"] or "")
    release_id = str(root["release_id"] or "")
    if not generation.strip() or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", generation):
        raise _fail("qe_active_dataset_profile_invalid", "generation is invalid")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,95}", release_id):
        raise _fail("qe_active_dataset_profile_invalid", "release_id is invalid")
    cutoff = _date(root["cutoff"], field="cutoff")

    controller = _require_exact_mapping(
        root["controller_paths"],
        fields={"candidate_root", "stock_pool_root", "coverage_receipt_path"},
        field="controller_paths",
    )
    candidate_root = Path(str(controller["candidate_root"]))
    stock_pool_root = Path(str(controller["stock_pool_root"]))
    receipt_path = Path(str(controller["coverage_receipt_path"]))
    _require_external_directory(candidate_root, field="controller_paths.candidate_root")
    _require_external_directory(stock_pool_root, field="controller_paths.stock_pool_root")
    _require_external_regular_file(receipt_path, field="controller_paths.coverage_receipt_path")

    components = _require_exact_mapping(
        root["components"],
        fields={"factor_meta", "factor_meta_sha256", "day_pins", "minute_pins", "index_pins", "suspend_pins", "benchmark_instruments_sha256"},
        field="components",
    )
    for field in ("factor_meta_sha256",):
        _require_sha256(components[field], field=f"components.{field}")
    _require_sha256(
        components["benchmark_instruments_sha256"],
        field="components.benchmark_instruments_sha256",
    )

    node_bindings = root["node_bindings"]
    if not isinstance(node_bindings, Mapping) or not node_bindings:
        raise _fail("qe_active_dataset_profile_invalid", "node_bindings must not be empty")
    for node_id, binding in node_bindings.items():
        if not isinstance(node_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", node_id):
            raise _fail("qe_active_dataset_profile_invalid", "node_id is invalid")
        node = _require_exact_mapping(
            binding,
            fields={"candidate_root"},
            field=f"node_bindings.{node_id}",
        )
        _require_posix_root(node["candidate_root"], field=f"node_bindings.{node_id}.candidate_root")

    consumers = _require_exact_mapping(root["consumers"], fields={"qe"}, field="consumers")
    qe = _require_exact_mapping(
        consumers["qe"],
        fields={"defaults", "default_universe", "universes", "coverage_receipt_sha256"},
        field="consumers.qe",
    )
    defaults = _require_exact_mapping(qe["defaults"], fields=_DEFAULT_FIELDS, field="consumers.qe.defaults")
    dates = {key: _date(value, field=f"defaults.{key}") for key, value in defaults.items()}
    if dates["signal_end"] != dates["test_end"] or dates["signal_end"] > cutoff:
        raise _fail("qe_active_dataset_profile_invalid", "signal_end/test_end/cutoff differ")
    if not (
        dates["train_start"] <= dates["train_end"] < dates["valid_start"] <= dates["valid_end"]
        < dates["test_start"] <= dates["backtest_end"] <= dates["test_end"]
    ):
        raise _fail("qe_active_dataset_profile_invalid", "QE default date ordering is invalid")
    default_selection = UniverseSelection.from_value(qe["default_universe"])

    universes = qe["universes"]
    if not isinstance(universes, Mapping) or "stock_universe" not in universes:
        raise _fail("qe_active_dataset_profile_invalid", "universes must include stock_universe")
    for pool_id, item in universes.items():
        if not isinstance(pool_id, str) or not _POOL_ID_RE.fullmatch(pool_id):
            raise _fail("qe_active_dataset_profile_invalid", f"invalid pool_id={pool_id!r}")
        universe = _require_exact_mapping(
            item,
            fields={"label", "filename", "sha256", "membership_revision"},
            field=f"universes.{pool_id}",
        )
        if not str(universe["label"] or "").strip() or not str(universe["membership_revision"] or "").strip():
            raise _fail("qe_active_dataset_profile_invalid", f"universe {pool_id} identity is empty")
        filename = str(universe["filename"] or "")
        if not _SIDECAR_RE.fullmatch(filename):
            raise _fail("qe_active_dataset_profile_invalid", f"universe {pool_id} filename is invalid")
        _require_sha256(universe["sha256"], field=f"universes.{pool_id}.sha256")
        if pool_id == "stock_universe" and filename != "stock_universe.txt":
            raise _fail("qe_active_dataset_profile_invalid", "stock_universe filename differs")
        if pool_id != "stock_universe" and filename != f"index_pool__{pool_id}.txt":
            raise _fail("qe_active_dataset_profile_invalid", f"universe {pool_id} filename differs")
    if set(default_selection.pool_ids) - set(universes):
        raise _fail("qe_active_dataset_profile_invalid", "default_universe contains unknown pool ids")

    receipt_bytes = receipt_path.read_bytes()
    receipt_sha = _sha256_bytes(receipt_bytes)
    if receipt_sha != _require_sha256(qe["coverage_receipt_sha256"], field="coverage_receipt_sha256"):
        raise _fail("qe_active_dataset_profile_invalid", "coverage receipt hash differs")
    try:
        receipt = json.loads(receipt_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _fail("qe_active_dataset_profile_invalid", "coverage receipt is invalid JSON") from exc
    receipt = _require_exact_mapping(
        receipt,
        fields={"schema_version", "release_id", "cutoff", "pools"},
        field="coverage_receipt",
    )
    if receipt["schema_version"] != UNIVERSE_COVERAGE_SCHEMA or receipt["release_id"] != release_id or receipt["cutoff"] != cutoff.isoformat():
        raise _fail("qe_active_dataset_profile_invalid", "coverage receipt identity differs")
    if not isinstance(receipt["pools"], Mapping) or set(receipt["pools"]) != set(universes):
        raise _fail("qe_active_dataset_profile_invalid", "coverage receipt pools differ")
    for pool_id, coverage_raw in receipt["pools"].items():
        coverage = _require_exact_mapping(
            coverage_raw,
            fields={"available_start", "available_end", "gaps"},
            field=f"coverage_receipt.pools.{pool_id}",
        )
        available_start = _date(coverage["available_start"], field=f"coverage.{pool_id}.available_start")
        available_end = _date(coverage["available_end"], field=f"coverage.{pool_id}.available_end")
        if available_start < QE_DATASET_START_DATE or available_end > cutoff or available_end < available_start:
            raise _fail("qe_active_dataset_profile_invalid", f"coverage window for {pool_id} is invalid")
        if not isinstance(coverage["gaps"], list):
            raise _fail("qe_active_dataset_profile_invalid", f"coverage gaps for {pool_id} are invalid")
        for gap in coverage["gaps"]:
            row = _require_exact_mapping(
                gap,
                fields={"symbol", "start", "end", "components"},
                field=f"coverage.{pool_id}.gap",
            )
            if not _SYMBOL_RE.fullmatch(str(row["symbol"] or "")):
                raise _fail("qe_active_dataset_profile_invalid", "coverage gap symbol is invalid")
            if _date(row["end"], field="gap.end") < _date(row["start"], field="gap.start"):
                raise _fail("qe_active_dataset_profile_invalid", "coverage gap dates are invalid")
            if not isinstance(row["components"], list) or not set(row["components"]).issubset({"day", "1min"}):
                raise _fail("qe_active_dataset_profile_invalid", "coverage gap components are invalid")

    try:
        QEDirectV2DatasetBinding(
            release_id=release_id,
            cutoff=cutoff,
            candidate_root="/profile-contract-validation",
            provider_uri_day="/profile-contract-validation/components/daily_bin_candidate",
            provider_uri_1min="/profile-contract-validation/components/minute_bin_candidate",
            factor_data_dir="/profile-contract-validation/components/factor_h5_static_candidate_v2",
            index_context_path="/profile-contract-validation/components/index_context/index_daily.h5",
            suspend_data_dir="/profile-contract-validation/components/suspend_d_daily_candidate_v2",
            factor_meta=dict(components["factor_meta"]),
            factor_meta_sha256=str(components["factor_meta_sha256"]),
            day_pins=dict(components["day_pins"]),
            minute_pins=dict(components["minute_pins"]),
            selection_pins={
                "mode": "stock_universe",
                "pool_ids": [],
                "instrument_name": "stock_universe",
                "instruments_file": "stock_universe.txt",
                "instruments_sha256": str(universes["stock_universe"]["sha256"]),
                "membership_revision": str(universes["stock_universe"]["membership_revision"]),
                "coverage_receipt_sha256": receipt_sha,
                "benchmark_code": "000300.SH",
                "benchmark_instruments_sha256": str(components["benchmark_instruments_sha256"]),
            },
            index_pins=dict(components["index_pins"]),
            suspend_pins=dict(components["suspend_pins"]),
            schema_version=QE_DIRECT_V2_DATASET_BINDING_SCHEMA_V3,
        ).validated()
    except ValueError as exc:
        raise _fail("qe_active_dataset_profile_invalid", f"component contract is invalid: {exc}") from exc

    return QEActiveDatasetProfile(
        raw=root,
        profile_path=path,
        profile_sha256=_sha256_bytes(payload),
        generation=generation,
        release_id=release_id,
        cutoff=cutoff,
        controller_candidate_root=candidate_root,
        controller_stock_pool_root=stock_pool_root,
        coverage_receipt_path=receipt_path,
        coverage_receipt=receipt,
        coverage_receipt_bytes=receipt_bytes,
    )


def load_active_qe_profile() -> QEActiveDatasetProfile | None:
    raw_path = os.getenv(ACTIVE_PROFILE_ENV)
    if raw_path is None or not raw_path.strip():
        return None
    path = Path(raw_path.strip())
    if not path.is_absolute():
        raise _fail("qe_active_dataset_profile_invalid", f"{ACTIVE_PROFILE_ENV} must be an absolute path")
    _require_external_regular_file(path, field=ACTIVE_PROFILE_ENV)
    payload = path.read_bytes()
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _fail("qe_active_dataset_profile_invalid", "profile is not UTF-8 JSON") from exc
    if not isinstance(value, Mapping) or payload != _canonical_profile_bytes(value):
        raise _fail("qe_active_dataset_profile_invalid", "profile must use canonical JSON plus one newline")
    return _validate_profile(value, path=path, payload=payload)


def get_qe_dataset_profile_summary() -> dict[str, Any]:
    profile = load_active_qe_profile()
    if profile is None:
        return {
            "mode": "legacy_default_not_activated",
            "generation": None,
            "release_id": None,
            "cutoff": LEGACY_QE_DEFAULT_DATA_SPLIT["test_end"],
            "defaults": dict(LEGACY_QE_DEFAULT_DATA_SPLIT),
            "signal_end": LEGACY_QE_DEFAULT_DATA_SPLIT["test_end"],
            "default_universe": {"mode": "stock_universe", "pool_ids": []},
            "available_nodes": [],
            "universes": [{"pool_id": "stock_universe", "label": "全市场股票池", "gap_count": 0}],
        }
    validate_controller_snapshot(profile)
    return profile.summary()


def get_qe_default_data_split(
    *, legacy_default: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Return active defaults, preserving the caller's pre-activation legacy contract."""

    profile = load_active_qe_profile()
    if profile is not None:
        return profile.defaults
    return dict(legacy_default or LEGACY_QE_DEFAULT_DATA_SPLIT)


def reject_client_dataset_internals(custom_params: Mapping[str, Any] | None) -> None:
    """Reject server-owned binding material at public task-creation boundaries."""

    supplied = sorted(QE_INTERNAL_DATASET_PARAMS.intersection(custom_params or {}))
    if supplied:
        raise _fail(
            "qe_dataset_internal_input_forbidden",
            "dataset paths, hashes, receipts, and run bindings are server-owned",
            fields=supplied,
        )


def _resolve_data_split(profile: QEActiveDatasetProfile, override: Mapping[str, Any] | None) -> dict[str, str]:
    split = profile.defaults
    if override is not None:
        if not isinstance(override, Mapping):
            raise _fail("qe_dataset_window_outside_release", "data_split must be an object")
        unknown = set(override) - set(LEGACY_QE_DEFAULT_DATA_SPLIT)
        if unknown:
            raise _fail("qe_dataset_window_outside_release", "data_split contains unknown fields", fields=sorted(unknown))
        for key, value in override.items():
            if value not in (None, ""):
                split[key] = _date(value, field=f"data_split.{key}").isoformat()
    if override and override.get("test_end") and not override.get("backtest_end"):
        split["backtest_end"] = (
            profile.defaults["backtest_end"]
            if split["test_end"] == profile.qe["defaults"]["signal_end"]
            else split["test_end"]
        )
    parsed = {key: _date(value, field=f"data_split.{key}") for key, value in split.items()}
    if any(value < QE_DATASET_START_DATE or value > profile.cutoff for value in parsed.values()):
        raise _fail("qe_dataset_window_outside_release", "requested dates exceed active release")
    if not (
        parsed["train_start"] <= parsed["train_end"] < parsed["valid_start"] <= parsed["valid_end"]
        < parsed["test_start"] <= parsed["backtest_end"] <= parsed["test_end"]
    ):
        raise _fail("qe_dataset_window_outside_release", "requested date ordering is invalid")
    return split


def _read_pinned_text(path: Path, expected_sha256: str, *, reason_code: str) -> str:
    if _is_link_or_junction(path) or not path.is_file():
        raise _fail(reason_code, f"sidecar is missing or not a regular file: {path}")
    payload = path.read_bytes()
    actual = _sha256_bytes(payload)
    if actual != expected_sha256:
        raise _fail("qe_universe_sidecar_hash_mismatch", f"sidecar hash differs: {path}", expected=expected_sha256, actual=actual)
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _fail(reason_code, f"sidecar is not UTF-8: {path}") from exc


def _require_pinned_file(path: Path, expected_sha256: str) -> None:
    if _is_link_or_junction(path) or not path.is_file():
        raise _fail("qe_dataset_component_identity_mismatch", f"required component file is missing: {path}")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise _fail("qe_dataset_component_identity_mismatch", f"cannot read component file: {path}") from exc
    if digest.hexdigest() != expected_sha256:
        raise _fail("qe_dataset_component_identity_mismatch", f"component hash differs: {path}")


def validate_controller_snapshot(profile: QEActiveDatasetProfile) -> None:
    """Verify the pinned controller-side snapshot without scanning feature directories."""

    root = profile.controller_candidate_root / "components"
    components = profile.raw["components"]
    day = root / "daily_bin_candidate"
    minute = root / "minute_bin_candidate"
    factor = root / "factor_h5_static_candidate_v2"
    index = root / "index_context"
    suspend = root / "suspend_d_daily_candidate_v2"
    for component_root in (day, minute, factor, index, suspend):
        if _is_link_or_junction(component_root) or not component_root.is_dir():
            raise _fail("qe_dataset_component_identity_mismatch", f"component root is invalid: {component_root}")
    for component_root, pins, calendar_name in (
        (day, components["day_pins"], "day.txt"),
        (minute, components["minute_pins"], "1min.txt"),
    ):
        _require_pinned_file(component_root / "instruments" / "all.txt", str(pins["instruments_sha256"]))
        _require_pinned_file(component_root / "calendars" / calendar_name, str(pins["calendar_sha256"]))
        _require_pinned_file(component_root / "meta_export.json", str(pins["meta_export_sha256"]))
    _require_pinned_file(
        day / "instruments" / "benchmark.txt",
        str(components["benchmark_instruments_sha256"]),
    )
    _require_pinned_file(
        factor / "meta.json",
        str(components["factor_meta_sha256"]),
    )
    _require_pinned_file(
        index / "index_daily.h5",
        str(components["index_pins"]["sha256"]),
    )
    _require_pinned_file(suspend / "meta.json", str(components["suspend_pins"]["metadata_sha256"]))
    _require_pinned_file(suspend / "suspend_d.parquet", str(components["suspend_pins"]["parquet_sha256"]))
    for pool_id, item in profile.universes.items():
        path = (
            day / "instruments" / str(item["filename"])
            if pool_id == "stock_universe"
            else profile.controller_stock_pool_root / str(item["filename"])
        )
        content = _read_pinned_text(
            path,
            str(item["sha256"]),
            reason_code="qe_universe_sidecar_not_deployed",
        )
        rows = _parse_intervals(content, source=str(item["filename"]))
        if any(symbol == "000300.SH" for symbol, _start, _end in rows):
            raise _fail("qe_direct_v2_selection_contains_benchmark", f"pool {pool_id} contains 000300.SH")


def _parse_intervals(content: str, *, source: str) -> list[tuple[str, dt.date, dt.date]]:
    rows: list[tuple[str, dt.date, dt.date]] = []
    prior: dict[str, dt.date] = {}
    for line_number, raw in enumerate(content.splitlines(), start=1):
        if not raw.strip():
            continue
        parts = raw.split("\t")
        if len(parts) != 3 or not _SYMBOL_RE.fullmatch(parts[0].strip().upper()):
            raise _fail("qe_universe_sidecar_hash_mismatch", f"invalid sidecar row {source}:{line_number}")
        symbol = parts[0].strip().upper()
        start = _date(parts[1].strip(), field=f"{source}:{line_number}.start")
        end = _date(parts[2].strip(), field=f"{source}:{line_number}.end")
        if end < start or (symbol in prior and start <= prior[symbol]):
            raise _fail("qe_universe_sidecar_hash_mismatch", f"overlapping or unsorted row {source}:{line_number}")
        prior[symbol] = end
        rows.append((symbol, start, end))
    if not rows:
        raise _fail("qe_universe_sidecar_not_deployed", f"sidecar contains no intervals: {source}")
    return rows


def _union_content(contents: list[str]) -> str:
    by_symbol: dict[str, list[tuple[dt.date, dt.date]]] = {}
    for index, content in enumerate(contents):
        for symbol, start, end in _parse_intervals(content, source=f"pool[{index}]"):
            by_symbol.setdefault(symbol, []).append((start, end))
    rows: list[tuple[str, dt.date, dt.date]] = []
    for symbol, spans in sorted(by_symbol.items()):
        current_start, current_end = sorted(spans)[0]
        for start, end in sorted(spans)[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                rows.append((symbol, current_start, current_end))
                current_start, current_end = start, end
        rows.append((symbol, current_start, current_end))
    return "".join(f"{symbol}\t{start.isoformat()}\t{end.isoformat()}\n" for symbol, start, end in rows)


def _require_window_coverage(profile: QEActiveDatasetProfile, selection: UniverseSelection, start: dt.date, end: dt.date) -> None:
    for pool_id in selection.pool_ids or ("stock_universe",):
        coverage = profile.coverage_receipt["pools"][pool_id]
        available_start = _date(coverage["available_start"], field="coverage.available_start")
        available_end = _date(coverage["available_end"], field="coverage.available_end")
        if start < available_start or end > available_end:
            raise _fail("qe_universe_window_coverage_incomplete", f"pool {pool_id} does not cover requested window")
        hits = []
        for gap in coverage["gaps"]:
            gap_start = _date(gap["start"], field="gap.start")
            gap_end = _date(gap["end"], field="gap.end")
            if gap_start <= end and gap_end >= start:
                hits.append({"symbol": gap["symbol"], "components": list(gap["components"])})
        if hits:
            raise _fail(
                "qe_universe_window_coverage_incomplete",
                f"pool {pool_id} has physical data gaps in the requested window",
                pool_id=pool_id,
                affected_count=len(hits),
                affected=hits[:20],
            )


def _calendar_dates(profile: QEActiveDatasetProfile) -> list[dt.date]:
    calendar_path = profile.controller_candidate_root / "components" / "daily_bin_candidate" / "calendars" / "day.txt"
    expected = str(profile.raw["components"]["day_pins"]["calendar_sha256"])
    content = _read_pinned_text(calendar_path, expected, reason_code="qe_dataset_component_identity_mismatch")
    dates = [_date(line.strip(), field="calendar") for line in content.splitlines() if line.strip()]
    if not dates or dates != sorted(set(dates)) or dates[-1] != profile.cutoff:
        raise _fail("qe_dataset_component_identity_mismatch", "daily calendar identity differs")
    return dates


def resolve_active_qe_dataset(
    *,
    node_id: str,
    data_split: Mapping[str, Any] | None = None,
    universe_selection: Mapping[str, Any] | None = None,
    label_horizon: int = 1,
    profile: QEActiveDatasetProfile | None = None,
) -> ResolvedQEDataset | None:
    selected_profile = profile if profile is not None else load_active_qe_profile()
    if selected_profile is None:
        return None
    validate_controller_snapshot(selected_profile)
    node_key = str(node_id or "").strip()
    node_raw = selected_profile.raw["node_bindings"].get(node_key)
    if not isinstance(node_raw, Mapping):
        raise _fail("qe_dataset_node_binding_missing", f"node is absent from active profile: {node_key}")
    split = _resolve_data_split(selected_profile, data_split)
    selection = UniverseSelection.from_value(
        universe_selection if universe_selection is not None else selected_profile.qe["default_universe"]
    )
    unknown = sorted(set(selection.pool_ids) - set(selected_profile.universes))
    if unknown:
        raise _fail("qe_universe_pool_unknown", f"unknown pool ids: {unknown}")
    window_start = _date(split["train_start"], field="train_start")
    window_end = _date(split["backtest_end"], field="backtest_end")
    _require_window_coverage(selected_profile, selection, window_start, window_end)

    universe_rows = selected_profile.universes
    stock_pool_content: str | None = None
    if selection.mode == "stock_universe":
        selected = universe_rows["stock_universe"]
        instrument_name = "stock_universe"
        instruments_file = "stock_universe.txt"
        instruments_sha256 = str(selected["sha256"])
        membership_revision = str(selected["membership_revision"])
    else:
        contents: list[str] = []
        revisions: list[str] = []
        for pool_id in selection.pool_ids:
            item = universe_rows[pool_id]
            filename = str(item["filename"])
            contents.append(
                _read_pinned_text(
                    selected_profile.controller_stock_pool_root / filename,
                    str(item["sha256"]),
                    reason_code="qe_universe_sidecar_not_deployed",
                )
            )
            revisions.append(str(item["membership_revision"]))
        if selection.mode == "single_index":
            item = universe_rows[selection.pool_ids[0]]
            instrument_name = str(item["filename"]).removesuffix(".txt")
            instruments_file = str(item["filename"])
            stock_pool_content = contents[0]
            membership_revision = revisions[0]
        else:
            stock_pool_content = _union_content(contents)
            content_sha = _sha256_bytes(stock_pool_content.encode("utf-8"))
            instrument_name = f"index_pool__union_{content_sha[:12]}"
            instruments_file = f"{instrument_name}.txt"
            membership_revision = "union:" + ",".join(revisions)
        _parse_intervals(stock_pool_content, source=instruments_file)
        instruments_sha256 = _sha256_bytes(stock_pool_content.encode("utf-8"))

    candidate_root = _require_posix_root(node_raw["candidate_root"], field=f"node_bindings.{node_key}.candidate_root")
    components = selected_profile.raw["components"]
    binding = QEDirectV2DatasetBinding(
        release_id=selected_profile.release_id,
        cutoff=selected_profile.cutoff,
        candidate_root=candidate_root,
        provider_uri_day=posixpath.join(candidate_root, "components/daily_bin_candidate"),
        provider_uri_1min=posixpath.join(candidate_root, "components/minute_bin_candidate"),
        factor_data_dir=posixpath.join(candidate_root, "components/factor_h5_static_candidate_v2"),
        index_context_path=posixpath.join(candidate_root, "components/index_context/index_daily.h5"),
        suspend_data_dir=posixpath.join(candidate_root, "components/suspend_d_daily_candidate_v2"),
        factor_meta=dict(components["factor_meta"]),
        factor_meta_sha256=str(components["factor_meta_sha256"]),
        day_pins=dict(components["day_pins"]),
        minute_pins=dict(components["minute_pins"]),
        selection_pins={
            "mode": selection.mode,
            "pool_ids": list(selection.pool_ids),
            "instrument_name": instrument_name,
            "instruments_file": instruments_file,
            "instruments_sha256": instruments_sha256,
            "membership_revision": membership_revision,
            "coverage_receipt_sha256": _sha256_bytes(selected_profile.coverage_receipt_bytes),
            "benchmark_code": str(selected_profile.qe.get("benchmark_code") or "000300.SH"),
            "benchmark_instruments_sha256": str(components["benchmark_instruments_sha256"]),
        },
        index_pins=dict(components["index_pins"]),
        suspend_pins=dict(components["suspend_pins"]),
        schema_version=QE_DIRECT_V2_DATASET_BINDING_SCHEMA_V3,
    ).validated()

    calendar = _calendar_dates(selected_profile)
    test_end = _date(split["test_end"], field="test_end")
    try:
        test_index = calendar.index(test_end)
    except ValueError as exc:
        raise _fail("qe_dataset_window_outside_release", "test_end is not in the release calendar") from exc
    if isinstance(label_horizon, bool):
        raise _fail("qe_dataset_window_outside_release", "label_horizon must be a positive integer")
    try:
        horizon = int(label_horizon)
    except (TypeError, ValueError) as exc:
        raise _fail("qe_dataset_window_outside_release", "label_horizon must be a positive integer") from exc
    if horizon < 1:
        raise _fail("qe_dataset_window_outside_release", "label_horizon must be a positive integer")
    observable_index = test_index - horizon
    if observable_index < 0:
        raise _fail("qe_dataset_window_outside_release", "label horizon exceeds available calendar")
    return ResolvedQEDataset(
        data_split=split,
        binding=binding,
        profile_summary=selected_profile.summary(),
        profile_internal_summary={
            **selected_profile.summary(),
            "profile_sha256": selected_profile.profile_sha256,
            "resolved_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        },
        stock_pool_content=stock_pool_content,
        coverage_receipt_content=selected_profile.coverage_receipt_bytes.decode("utf-8"),
        outcome_observable_end=calendar[observable_index].isoformat(),
    )


def resolve_and_apply_active_qe_dataset(
    *,
    node_id: str,
    data_split: Mapping[str, Any] | None,
    universe_selection: Mapping[str, Any] | None,
    custom_params: Mapping[str, Any] | None,
    label_horizon: int = 1,
    profile: QEActiveDatasetProfile | None = None,
) -> tuple[dict[str, str] | None, dict[str, Any], dict[str, Any] | None]:
    params = dict(custom_params or {})
    if QE_DIRECT_V2_DATASET_BINDING_PARAM in params:
        return (dict(data_split) if data_split else None), params, None
    if params.get("stock_pool") not in (None, "", "all"):
        raise _fail(
            "qe_universe_mode_invalid",
            "active-profile tasks must use universe_selection instead of a legacy stock_pool path",
        )
    resolved = resolve_active_qe_dataset(
        node_id=node_id,
        data_split=data_split,
        universe_selection=universe_selection,
        label_horizon=label_horizon,
        profile=profile,
    )
    if resolved is None:
        return (dict(data_split) if data_split else None), params, None
    return dict(resolved.data_split), resolved.apply(params), {
        **dict(resolved.profile_summary),
        "resolved_dates": dict(resolved.data_split),
        "resolved_universe": {
            "mode": resolved.binding.selection_pins["mode"],
            "pool_ids": list(resolved.binding.selection_pins["pool_ids"]),
            "label": resolved.binding.selection_pins["instrument_name"],
        },
        "node_id": str(node_id),
        "outcome_observable_end": resolved.outcome_observable_end,
    }
