"""Read native monthly releases for the existing L2 stock-fact kernel.

This is a consumer adapter, not a release producer or a model implementation.
The manifest pins every consumed file; the closure supplies native readiness
receipts. Neither legacy state nor an unused L1 price panel is manufactured.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
import json
from pathlib import Path, PurePosixPath
from typing import Any

from backend.services.hmm_risk import rotation_l1_input_bundle as reader
from backend.services.hmm_risk.contracts import canonical_sha256


_MANIFEST = "qe_dataset_manifest.json"
_CLOSURE = "release_closure_receipt.json"
_BINDING_FIELDS = {"generation", "release_id", "revision", "cutoff", "manifest_sha256", "manifest_file_sha256"}
_METADATA = {
    "daily_meta": "components/daily_bin_candidate/meta_export.json",
    "factor_meta": "components/factor_h5_static_candidate_v2/meta.json",
    "index_meta": "components/index_context/meta.json",
    "suspend_meta": "components/suspend_d_daily_candidate_v2/meta.json",
    "calendar": "components/daily_bin_candidate/calendars/day.txt",
    "instruments": "components/daily_bin_candidate/instruments/all.txt",
    "selection_universe": "stock_pools/stock_universe.txt",
}
_COMPONENTS = {
    "daily_basic": "components/factor_h5_static_candidate_v2/daily_basic.h5",
    "moneyflow": "components/factor_h5_static_candidate_v2/moneyflow.h5",
    "index_context": "components/index_context/index_daily.h5",
    "suspend_data": "components/suspend_d_daily_candidate_v2/suspend_d.parquet",
}
_RECEIPTS = {"dataset_manifest", "release_closure", "consumer_contract", "source_readiness", "validation", "lineage"}


def _invalid(message: str):
    return reader._fail(reader.REASON_MANIFEST_INVALID, f"monthly L2 input: {message}")


def _relative(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or ":" in value
        or "\x00" in value
        or PurePosixPath(value).is_absolute()
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise _invalid("file reference is not a canonical release-relative path")
    return value


def _reference(value: Any, *, field: str = "path") -> tuple[str, str, int]:
    if not isinstance(value, Mapping) or not {field, "sha256", "size"} <= set(value):
        raise _invalid("file reference is incomplete")
    path = _relative(value[field])
    digest = reader._require_sha256(value["sha256"], f"monthly reference {path}")
    if type(value["size"]) is not int or value["size"] < 0:
        raise _invalid("file reference size is invalid")
    return path, digest, value["size"]


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


class _Files:
    """Bounded consumer read set; detect replacement during verification/readback."""

    def __init__(self, root: Path):
        self.root = root
        self.stamps: dict[Path, tuple[int, ...]] = {}
        self.hashes: dict[Path, str] = {}

    @staticmethod
    def _stamp(path: Path) -> tuple[int, ...]:
        stat = path.stat()
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns

    def path(self, relative: str) -> Path:
        return self.external(self.root / _relative(relative))

    def external(self, path: Path) -> Path:
        path = Path(path)
        if not path.is_absolute() or any(reader._is_indirect_path(p) for p in (path, *path.parents)):
            raise _invalid("input path is relative or indirect")
        try:
            if not path.is_file():
                raise FileNotFoundError(path)
            path = path.resolve(strict=True)
            stamp = self._stamp(path)
        except OSError as exc:
            raise reader._fail(
                reader.REASON_SOURCE_COMPONENT_MISSING, f"monthly L2 input file is unavailable: {path}"
            ) from exc
        if path in self.stamps and stamp != self.stamps[path]:
            raise reader._fail(reader.REASON_HASH_MISMATCH, "monthly L2 input changed during readback")
        self.stamps[path] = stamp
        return path

    def digest(self, path: Path) -> str:
        path = self.external(path)
        if path not in self.hashes:
            try:
                self.hashes[path] = reader._sha256_file(path)
            except OSError as exc:
                raise reader._fail(reader.REASON_HASH_MISMATCH, f"monthly L2 input cannot be verified: {path}") from exc
        self.external(path)
        return self.hashes[path]

    def verify(self, ref: tuple[str, str, int]) -> Path:
        relative, expected, size = ref
        path = self.path(relative)
        if path.stat().st_size != size or self.digest(path) != expected:
            raise reader._fail(reader.REASON_HASH_MISMATCH, f"monthly L2 pinned file differs: {relative}")
        return path

    def read(self, path: Path) -> dict[str, Any]:
        path = self.external(path)
        try:
            value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
            if not isinstance(value, dict):
                raise ValueError("JSON object required")
            # Also rejects NaN/Infinity, which Python's JSON parser accepts.
            canonical_sha256(value)
        except (OSError, UnicodeError, ValueError, TypeError) as exc:
            raise _invalid(f"invalid JSON object: {path.name}") from exc
        self.external(path)
        return value

    def unchanged(self) -> None:
        for path in tuple(self.stamps):
            self.external(path)


_ZERO_COUNTERS = ("hash_mismatch_count", "duplicate_path_count", "unpinned_core_file_count")
_AMENDMENTS = {
    "EXACT_LATE_FACT_DATE_AND_INHERITED_PINS": (
        "late_fact_amendment_ref", "late_fact_amendment", "aistock_monthly_late_financing_amendment_v1"
    ),
    "FULL_EXECUTION_WINDOW_AND_EXACT_MINUTE_REPAIRS": (
        "late_fact_amendment_ref", "minute_coverage_amendment", "aistock_monthly_minute_coverage_amendment_v1"
    ),
    "EXACT_REVIEWED_SUSPENSION_KEYS_AND_INHERITED_PINS": (
        "suspension_amendment_ref", "suspension_amendment", "aistock_monthly_suspension_amendment_v1"
    ),
    "ALL_CAPTURED_SPARSE_SUSPENSION_FACTS_AND_INHERITED_PINS": (
        "suspension_amendment_ref", "suspension_amendment", "aistock_monthly_suspension_amendment_v1"
    ),
}


def _manifest_pins(manifest):
    identity = reader._require_sha256(manifest.get("dataset_manifest_sha256"), "monthly manifest identity")
    if canonical_sha256({k: v for k, v in manifest.items() if k != "dataset_manifest_sha256"}) != identity:
        raise _invalid("inherited manifest canonical identity differs")
    raw = manifest.get("components")
    if not isinstance(raw, Mapping) or not raw:
        raise _invalid("inherited manifest components are missing")
    pins = {}
    for value in raw.values():
        ref = _reference(value)
        if ref[0] in pins:
            raise _invalid("inherited manifest repeats a file path")
        pins[ref[0]] = ref
    return pins


def _inherited_validation(files, manifest, pins, validation, readiness, lineage, ancestry):
    """Verify a sealed amendment DAG, then hash every added/changed physical pin.

    Unchanged pins inherit an explicitly validated predecessor. No absent counter
    is converted to zero, and no current release readiness receipt is rewritten.
    """
    inherited = validation.get("inherited_unchanged_readiness")
    if (
        validation.get("validation_scope") not in _AMENDMENTS
        or validation.get("historical_content_revalidated") is not False
        or not isinstance(inherited, Mapping)
        or set(inherited) != {"path", "sha256"}
        or inherited != readiness.get("inherited_unchanged_readiness")
        or not isinstance(inherited.get("path"), str)
    ):
        raise _invalid("monthly inherited validation is incomplete or unknown")
    expected = reader._require_sha256(inherited["sha256"], "inherited closure")
    parent_path = files.external(Path(inherited["path"]))
    if parent_path.name != _CLOSURE or files.digest(parent_path) != expected:
        raise _invalid("inherited closure path or hash differs")
    parent_files = _Files(parent_path.parent)
    parent = parent_files.read(parent_files.path(_MANIFEST))
    parent_pins = _manifest_pins(parent)
    predecessor = lineage.get(
        "component_revision_predecessor_manifest_sha256", lineage.get("predecessor_dataset_manifest_sha256")
    )
    if (
        predecessor != parent["dataset_manifest_sha256"]
        or parent.get("schema_version") != manifest.get("schema_version")
        or any(parent.get(k) != manifest.get(k) for k in ("release_id", "cutoff_trade_date"))
        or parent.get("revision") == manifest.get("revision")
        or set(parent_pins) - set(pins)
        or type(validation.get("manifest_component_count")) is not int
        or validation["manifest_component_count"] != len(pins)
        or validation.get("deployment_content_sha256") != manifest.get("deployment_content_sha256")
    ):
        raise _invalid("inherited lineage or manifest inventory differs")
    receipt_key, contract_key, schema = _AMENDMENTS[validation["validation_scope"]]
    ref = _reference(readiness.get(receipt_key))
    if pins.get(ref[0]) != ref:
        raise _invalid("amendment evidence is not manifest-pinned")
    amendment = files.read(files.verify(ref))
    contract = manifest.get("source_contract", {}).get(contract_key)
    if (
        amendment.get("schema_version") != schema
        or amendment.get("predecessor_manifest_sha256") != predecessor
        or not isinstance(contract, Mapping)
        or contract.get("predecessor_manifest_sha256") != predecessor
        or contract.get("receipt_sha256") != ref[1]
    ):
        raise _invalid("amendment evidence identity differs")
    _closure(parent_files, parent, parent_pins, ancestry=ancestry)
    # Unlike a PASS-only check, this verifies the complete physical delta,
    # including inputs not used by HMM. Current consumed files are also verified
    # by their ordinary reader path before use.
    for relative, pin in pins.items():
        if parent_pins.get(relative) != pin:
            files.verify(pin)
    parent_files.unchanged()
    for path, stamp in parent_files.stamps.items():
        if path in files.stamps and files.stamps[path] != stamp:
            raise _invalid("inherited input changed during validation")
        files.stamps[path] = stamp
    files.hashes.update(parent_files.hashes)


def _closure(
    files: _Files, manifest: Mapping[str, Any], pins: Mapping[str, tuple], *, ancestry=()
) -> dict[str, str]:
    closure_path = files.path(_CLOSURE)
    if closure_path in ancestry or len(ancestry) >= 8:
        raise _invalid("inherited closure is cyclic or exceeds the bounded depth")
    ancestry = (*ancestry, closure_path)
    closure = files.read(closure_path)
    body = {k: v for k, v in closure.items() if k != "canonical_sha256"}
    if closure.get("schema_version") != "aistock_release_closure_v1" or closure.get(
        "canonical_sha256"
    ) != canonical_sha256(body):
        raise _invalid("closure schema or canonical hash differs")
    manifest_ref = _reference(closure.get("dataset_manifest_ref"), field="id")
    if manifest_ref[0] != _MANIFEST:
        raise _invalid("closure names a different manifest")
    files.verify(manifest_ref)
    hashes = {"dataset_manifest": manifest_ref[1], "release_closure": files.digest(closure_path)}
    identity = manifest["dataset_manifest_sha256"]
    receipts = {}
    for group, schema, name in (
        ("consumer_contract_refs", "aistock_monthly_consumer_contract_v1", "consumer_contract"),
        ("source_readiness_refs", "aistock_monthly_source_readiness_v1", "source_readiness"),
        ("component_validation_refs", "aistock_monthly_component_validation_v1", "validation"),
    ):
        refs = closure.get(group)
        if not isinstance(refs, list) or not refs:
            raise _invalid(f"closure is missing {group}")
        found = []
        seen = set()
        for raw in refs:
            ref = _reference(raw, field="id")
            if ref[0] in seen:
                raise _invalid("closure repeats a receipt path")
            seen.add(ref[0])
            receipt = files.read(files.verify(ref))
            if receipt.get("schema_version") != schema or receipt.get("dataset_manifest_sha256") != identity:
                raise _invalid("receipt schema or release identity differs")
            if name == "consumer_contract" and receipt.get("consumer_id") != "hmm_file_only":
                continue
            if name == "consumer_contract":
                _consumer(receipt, pins, manifest_ref)
            elif receipt.get("status") != "PASS":
                raise _invalid("monthly readiness/validation has not passed")
            elif name == "source_readiness" and (
                type(receipt.get("unexplained_gap_count")) is not int or receipt["unexplained_gap_count"] != 0
            ):
                raise _invalid("monthly source readiness has unexplained gaps")
            elif name == "validation":
                present = [k in receipt for k in _ZERO_COUNTERS]
                if any(present) and (
                    not all(present) or any(type(receipt[k]) is not int or receipt[k] != 0 for k in _ZERO_COUNTERS)
                ):
                    raise _invalid("monthly component validation is incomplete")
                if not any(present) and "inherited_unchanged_readiness" not in receipt:
                    raise _invalid("monthly component validation is incomplete")
            found.append(ref)
            receipts[name] = receipt
        if len(found) != 1:
            raise _invalid(f"closure must contain one unambiguous {name}")
        hashes[name] = found[0][1]
    lineage_ref = _reference(closure.get("lineage_ref"), field="id")
    lineage = files.read(files.verify(lineage_ref))
    if (
        lineage.get("schema_version") != "aistock_monthly_release_lineage_v1"
        or lineage.get("status") != "SEALED_SUCCESSOR"
        or any(
            lineage.get(k) != manifest.get(k)
            for k in ("dataset_manifest_sha256", "release_id", "revision", "cutoff_trade_date")
        )
    ):
        raise _invalid("monthly lineage identity is inconsistent")
    if "inherited_unchanged_readiness" in receipts["validation"]:
        _inherited_validation(
            files, manifest, pins, receipts["validation"], receipts["source_readiness"], lineage, ancestry
        )
    return {**hashes, "lineage": lineage_ref[1]}


def _consumer(receipt, pins, manifest_ref) -> None:
    required = {"factor", "index", "manifest", "sector_context"}
    groups = receipt.get("component_refs")
    if (
        receipt.get("status") != "READY_FOR_NODE_READBACK"
        or not isinstance(receipt.get("required_components"), list)
        or any(not isinstance(group, str) for group in receipt["required_components"])
        or set(receipt["required_components"]) != required
        or not isinstance(groups, Mapping)
        or set(groups) != required
    ):
        raise _invalid("HMM native consumer contract is incomplete")
    for group, refs in groups.items():
        if not isinstance(refs, list) or not refs:
            raise _invalid(f"HMM consumer component is empty: {group}")
        seen = set()
        for raw in refs:
            ref = _reference(raw)
            if ref[0] in seen or ref != (manifest_ref if ref[0] == _MANIFEST else pins.get(ref[0])):
                raise _invalid("HMM consumer file pin differs from the manifest")
            seen.add(ref[0])
        expected = {
            "factor": {v for v in _COMPONENTS.values() if "/factor_h5_static_" in v} | {_METADATA["factor_meta"]},
            "index": {_COMPONENTS["index_context"], _METADATA["index_meta"]},
            "manifest": {_MANIFEST},
            "sector_context": {"components/sector_context_candidate_v1/sector_code_map.json"},
        }[group]
        if not expected <= seen:
            raise _invalid(f"HMM consumer omits required {group} inputs")


def _binding(files: _Files, manifest, manifest_file_hash, closure_hash, frozen):
    cutoff = reader._as_date(manifest.get("cutoff_trade_date"), "monthly cutoff")
    if (
        manifest.get("schema_version") != reader.DIRECT_V2_DATASET_MANIFEST_SCHEMA_VERSION
        or manifest.get("release_id") != f"qe_hmm_full_v2_{cutoff:%Y%m%d}"
        or manifest.get("cutoff_trade_date") != cutoff.isoformat()
        or not isinstance(manifest.get("revision"), str)
        or not manifest["revision"].strip()
        or cutoff < reader.DIRECT_V2_MINIMUM_CUTOFF
    ):
        raise _invalid("manifest release/profile/cutoff is invalid")
    if frozen is not None:
        if (
            not isinstance(frozen, Mapping)
            or set(frozen) != _BINDING_FIELDS
            or any(not isinstance(v, str) or not v or v != v.strip() for v in frozen.values())
            or any(
                frozen[k] != manifest[m]
                for k, m in (
                    ("release_id", "release_id"),
                    ("revision", "revision"),
                    ("cutoff", "cutoff_trade_date"),
                    ("manifest_sha256", "dataset_manifest_sha256"),
                )
            )
            or frozen["manifest_file_sha256"] != manifest_file_hash
        ):
            raise _invalid("explicit frozen release binding differs")
        return cutoff, {
            "frozen_release_id": frozen["release_id"],
            "frozen_release_generation": frozen["generation"],
            "frozen_release_binding_sha256": canonical_sha256(frozen),
        }
    profile = reader.load_active_hmm_dataset_identity()
    allowed = {reader._portable_external_locator(p, field="active candidate root") for p in profile["candidate_roots"]}
    payload = files.read(files.external(profile["profile_path"]))
    components = payload.get("components", {})
    if (
        files.digest(profile["profile_path"]) != profile["profile_sha256"]
        or reader._portable_external_locator(str(files.root), field="monthly candidate root") not in allowed
        or profile["release_id"] != manifest["release_id"]
        or profile["cutoff"] != cutoff
        or not isinstance(components, Mapping)
        or components.get("dataset_manifest_sha256") != manifest["dataset_manifest_sha256"]
        or components.get("dataset_manifest_file_sha256") != manifest_file_hash
        or components.get("release_closure_sha256") != closure_hash
    ):
        raise _invalid("active profile does not pin this monthly manifest/closure")
    return cutoff, {
        "active_release_id": profile["release_id"],
        "active_profile_generation": profile["generation"],
        "active_profile_sha256": profile["profile_sha256"],
    }


def load_monthly_l2_source_assets(
    candidate_root: Path,
    *,
    security_identity_manifest: Path,
    provider_absence_manifest: Path,
    data_window_end: date | None = None,
    frozen_release_binding: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Use a pinned monthly release without reading labels or changing active."""
    root = Path(candidate_root)
    if not root.is_absolute() or not root.is_dir() or any(reader._is_indirect_path(p) for p in (root, *root.parents)):
        raise _invalid("candidate root must be an existing direct absolute directory")
    files = _Files(root.resolve(strict=True))
    manifest_path = files.path(_MANIFEST)
    manifest = files.read(manifest_path)
    identity_hash = reader._require_sha256(manifest.get("dataset_manifest_sha256"), "monthly manifest identity")
    if canonical_sha256({k: v for k, v in manifest.items() if k != "dataset_manifest_sha256"}) != identity_hash:
        raise reader._fail(reader.REASON_HASH_MISMATCH, "monthly manifest canonical identity differs")
    raw_pins = manifest.get("components")
    if not isinstance(raw_pins, Mapping) or not raw_pins:
        raise _invalid("manifest components are missing")
    pins = {}
    for raw in raw_pins.values():
        ref = _reference(raw)
        if ref[0] in pins:
            raise _invalid("manifest has duplicate file paths")
        pins[ref[0]] = ref
    receipt_hashes = _closure(files, manifest, pins)
    cutoff, binding = _binding(
        files, manifest, files.digest(manifest_path), receipt_hashes["release_closure"], frozen_release_binding
    )
    read_end = cutoff if data_window_end is None else data_window_end
    if type(read_end) is not date or not reader.SOURCE_START <= read_end <= cutoff:
        raise reader._fail(reader.REASON_SOURCE_RANGE_INCOMPLETE, "monthly L2 read window escapes the release")

    paths = {}
    for name, relative in {**_METADATA, **_COMPONENTS}.items():
        if relative not in pins:
            raise _invalid(f"manifest does not pin the consumed file: {relative}")
        paths[name] = files.verify(pins[relative])
    security = files.external(security_identity_manifest)
    try:
        security_relative = security.relative_to(files.root).as_posix()
    except ValueError as exc:
        raise _invalid("security source identity must belong to the selected monthly release") from exc
    if security_relative not in pins:
        raise _invalid("security identity is not manifest-pinned")
    files.verify(pins[security_relative])
    paths["security_identity"] = security
    paths["provider_absence"] = files.external(provider_absence_manifest)
    daily_meta = files.read(paths["daily_meta"])
    factor_meta = files.read(paths["factor_meta"])
    index_meta = files.read(paths["index_meta"])
    suspend_meta = files.read(paths["suspend_meta"])
    common = {"start": reader.DIRECT_V2_RELEASE_START.isoformat(), "end": cutoff.isoformat()}
    for meta in (daily_meta, factor_meta, index_meta, suspend_meta):
        if any(meta.get(k) != v for k, v in common.items()):
            raise _invalid("component date identity differs from the monthly release")
    if (
        daily_meta.get("snapshot_id") != "daily_bin_candidate"
        or not isinstance(daily_meta.get("rule_version"), str)
        or not daily_meta["rule_version"].strip()
        or any(
            meta.get("universe_key") != reader.DIRECT_V2_UNIVERSE_KEY
            for meta in (daily_meta, factor_meta, suspend_meta)
        )
        or factor_meta.get("schema_version") != reader.DIRECT_V2_FACTOR_SCHEMA_VERSION
        or suspend_meta.get("schema_version") != reader.DIRECT_V2_SUSPEND_SCHEMA_VERSION
        or index_meta.get("schema_version") != "qe_index_context_v1"
        or not isinstance(index_meta.get("codes"), list)
        or any(not isinstance(code, str) for code in index_meta["codes"])
        or tuple(sorted(index_meta.get("codes") or ())) != reader.DIRECT_V2_INDEX_CODES
    ):
        raise _invalid("component schema/universe/benchmark identity differs")
    calendar = reader._load_qlib_calendar(paths["calendar"])
    if calendar[-1] != cutoff or read_end not in calendar:
        raise reader._fail(reader.REASON_SOURCE_RANGE_INCOMPLETE, "monthly read boundary is not a published session")
    spans = reader._parse_instrument_spans(paths["selection_universe"])
    provider_spans = reader._parse_instrument_spans(
        paths["instruments"], allowed_nonstock_codes=reader.DIRECT_V2_INDEX_CODES
    )
    if set(spans).intersection(reader.DIRECT_V2_INDEX_CODES) or set(spans) - set(provider_spans):
        raise _invalid("PIT selection contains unknown provider stocks or benchmark indices")
    qlib_root = files.root / "components/daily_bin_candidate"
    # PIT membership and actual quote availability are different contracts.
    # A suspension can split the provider's quote spans without removing a
    # stock from PIT membership. Keep PIT intact for the existing observation
    # kernel; only use quote spans for physical Bin layout verification.
    qlib_preflight = reader._preflight_qlib_feature_inventory(
        qlib_root, calendar=calendar, spans={symbol: provider_spans[symbol] for symbol in spans}
    )
    inventories = {}
    for name, columns in (("daily_basic", reader._DAILY_BASIC_COLUMNS), ("moneyflow", reader._MONEYFLOW_COLUMNS)):
        inventories[name] = reader._fixed_h5_inventory(paths[name], expected_columns=columns, expected_dtype="<f4")
        if (
            reader._as_date(inventories[name]["date_min"], "H5 start") > reader.SOURCE_START
            or reader._as_date(inventories[name]["date_max"], "H5 end") < cutoff
        ):
            raise reader._fail(reader.REASON_SOURCE_RANGE_INCOMPLETE, f"monthly {name} date coverage differs")
    index_inventory, benchmark, benchmark_close = reader._load_direct_v2_index_context(
        paths["index_context"],
        expected_release_cutoff=cutoff,
        release_calendar=calendar,
        calendar=tuple(day for day in calendar if reader.SOURCE_START <= day <= read_end),
    )
    metadata_hashes = {
        **receipt_hashes,
        **{k: files.digest(paths[k]) for k in _METADATA},
        **{k: files.digest(paths[k]) for k in ("security_identity", "provider_absence")},
    }
    release_identity = {
        "schema_version": reader.MONTHLY_IDENTITY_SCHEMA_VERSION,
        "release_id": manifest["release_id"],
        "dataset_manifest_sha256": identity_hash,
        "dataset_revision": manifest["revision"],
        "profile": reader.DIRECT_V2_PROFILE,
        "cutoff": cutoff.isoformat(),
        "universe_key": reader.DIRECT_V2_UNIVERSE_KEY,
        "rule_version": daily_meta["rule_version"],
        "metadata_sha256": metadata_hashes,
        "component_sha256": {k: files.digest(paths[k]) for k in _COMPONENTS},
        **binding,
    }
    validate_monthly_release_identity(release_identity)
    inventory = {
        "schema_version": reader.SOURCE_INVENTORY_SCHEMA_VERSION,
        "release_identity": release_identity,
        "release_cutoff": cutoff.isoformat(),
        "source_window_end": reader.SOURCE_END.isoformat(),
        "qlib": {
            "schema_version": reader.QLIB_STOCK_SCHEMA_VERSION,
            "schema_sha256": reader.qlib_stock_schema_digest(),
            "calendar_sha256": metadata_hashes["calendar"],
            "instruments_sha256": metadata_hashes["instruments"],
            "training_instruments_sha256": metadata_hashes["selection_universe"],
            "meta_export_sha256": metadata_hashes["daily_meta"],
            "field_preflight": qlib_preflight,
            "full_history_content_hash": False,
        },
        "factor_meta_sha256": metadata_hashes["factor_meta"],
        **inventories,
        "index_context": index_inventory,
        "suspend_meta_sha256": metadata_hashes["suspend_meta"],
        "external_authority_sha256": {k: metadata_hashes[k] for k in ("security_identity", "provider_absence")},
        "full_history_content_hash": False,
    }
    files.unchanged()
    return {
        "release_root": files.root,
        "qlib_root": qlib_root,
        "instrument_universe_path": paths["selection_universe"],
        "files": {
            **{k: paths[k] for k in _COMPONENTS},
            "suspend_manifest": paths["suspend_meta"],
            "security_identity": security,
            "provider_absence": paths["provider_absence"],
        },
        "release_identity": release_identity,
        "release_cutoff": cutoff,
        "source_window_end": reader.SOURCE_END,
        "data_window_end": read_end,
        "universe_key": reader.DIRECT_V2_UNIVERSE_KEY,
        "universe_rule_version": daily_meta["rule_version"],
        "source_revision": reader.MONTHLY_SOURCE_REVISION,
        "suspend_contract": "direct_v2",
        "benchmark": benchmark,
        "benchmark_close": benchmark_close,
        "sector_index_close": None,
        "sector_index_code_by_sector": None,
        "inventory": {**inventory, "inventory_sha256": canonical_sha256(inventory)},
        "binding_manifest_sha256": canonical_sha256(release_identity),
    }


def validate_monthly_release_identity(value: Any) -> dict[str, Any]:
    """Strict readback identity, separate from legacy L1/direct-v3 schemas."""
    if not isinstance(value, Mapping):
        raise _invalid("release identity must be an object")
    expected = {
        "schema_version",
        "release_id",
        "dataset_manifest_sha256",
        "dataset_revision",
        "profile",
        "cutoff",
        "universe_key",
        "rule_version",
        "metadata_sha256",
        "component_sha256",
    }
    frozen = {"frozen_release_id", "frozen_release_generation", "frozen_release_binding_sha256"}
    active = {"active_release_id", "active_profile_generation", "active_profile_sha256"}
    variant = frozen if set(value) == expected | frozen else active
    cutoff = reader._as_date(value.get("cutoff"), "monthly identity cutoff")
    if (
        set(value) != expected | variant
        or value.get("schema_version") != reader.MONTHLY_IDENTITY_SCHEMA_VERSION
        or value.get("profile") != reader.DIRECT_V2_PROFILE
        or value.get("universe_key") != reader.DIRECT_V2_UNIVERSE_KEY
        or value.get("release_id") != f"qe_hmm_full_v2_{cutoff:%Y%m%d}"
        or cutoff < reader.DIRECT_V2_MINIMUM_CUTOFF
        or value.get("cutoff") != cutoff.isoformat()
        or any(
            not isinstance(value.get(k), str) or not value[k].strip()
            for k in ("dataset_revision", "rule_version", *variant)
        )
        or value.get("frozen_release_id" if variant == frozen else "active_release_id") != value["release_id"]
    ):
        raise _invalid("release identity fields differ")
    reader._require_sha256(value["dataset_manifest_sha256"], "monthly manifest identity")
    reader._require_sha256(
        value["frozen_release_binding_sha256" if variant == frozen else "active_profile_sha256"], "monthly binding hash"
    )
    for field, keys in (
        ("metadata_sha256", _RECEIPTS | set(_METADATA) | {"security_identity", "provider_absence"}),
        ("component_sha256", set(_COMPONENTS)),
    ):
        hashes = value.get(field)
        if not isinstance(hashes, Mapping) or set(hashes) != keys:
            raise _invalid("release identity hash set differs")
        for digest in hashes.values():
            reader._require_sha256(digest, f"monthly identity {field}")
    return dict(value)
