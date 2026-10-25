"""Typed proof for quote-catalog-only immutable release hotfix adoption.

No source database, consumer model rebinding, price rewrite, or activation.
Unchanged components inherit the exact active release's pinned identities.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

import numpy as np
from backend.qlib_exporter.quote_catalog import build_minute_quote_catalog
from .canonical import canonical_json_bytes

REPAIR_SCHEMA = "aistock_minute_quote_catalog_repair_v1"
READBACK_SCHEMA = "aistock_minute_quote_catalog_readback_v1"
QUOTE_SCHEMAS = {REPAIR_SCHEMA, READBACK_SCHEMA}
_MUTABLE = {"minute_provider_catalog", "minute_meta_export"}
_ADDED = "minute_quote_catalog_repair"
_PROVIDER = "components/minute_bin_candidate"


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise ValueError(message)


def _plain(path: Path) -> None:
    for ancestor in (path, *path.parents):
        value = ancestor.lstat()
        _require(not ancestor.is_symlink() and not int(getattr(value, "st_file_attributes", 0)) & 0x400,
                 "quote adoption contains a link or junction")


def _read(path: Path) -> dict[str, Any]:
    _plain(path)
    before = path.stat()
    _require(path.is_file(), "quote adoption JSON is invalid")
    value = json.loads(path.read_text(encoding="utf-8"))
    after = path.stat()
    _require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
             == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
             and isinstance(value, dict), "quote adoption JSON changed")
    return value


def _sha(path: Path) -> str:
    _plain(path)
    digest = hashlib.sha256()
    with path.open("rb") as reader:
        while block := reader.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _pinned(root: Path, pin: Mapping[str, Any], *, expected_path: str | None = None) -> Path:
    relative = pin.get("path")
    _require(isinstance(relative, str) and bool(relative), "quote adoption pin path is missing")
    parsed = PurePosixPath(relative)
    _require(not parsed.is_absolute() and ".." not in parsed.parts and "\\" not in relative
             and ":" not in relative and (expected_path is None or relative == expected_path),
             "quote adoption pin path differs")
    path = root / parsed
    _plain(path)
    _require(path.is_file() and path.stat().st_size == pin.get("size") and _sha(path) == pin.get("sha256"),
             "quote adoption pin identity differs")
    return path


def validate_quote_adoption(*, predecessor_root: Path, predecessor_profile: Mapping[str, Any],
                            candidate: Path, profile: Mapping[str, Any], manifest: Mapping[str, Any],
                            evidence: Mapping[str, Mapping[str, Any]], required_nodes: tuple[str, ...]) -> None:
    """Validate a narrowly typed successor, not an operator-supplied PASS flag."""
    _require(QUOTE_SCHEMAS.issubset(evidence), "quote repair/readback evidence is incomplete")
    old = _read(predecessor_root / "qe_dataset_manifest.json")
    _require(_sha(predecessor_root / "qe_dataset_manifest.json") == predecessor_profile["components"]["dataset_manifest_file_sha256"],
             "quote predecessor manifest file identity differs")
    for value in (old, manifest):
        body = {key: item for key, item in value.items() if key != "dataset_manifest_sha256"}
        _require(hashlib.sha256(canonical_json_bytes(body)).hexdigest() == value.get("dataset_manifest_sha256"),
                 "quote manifest canonical identity differs")
    _require(old["dataset_manifest_sha256"] == predecessor_profile["components"]["dataset_manifest_sha256"],
             "quote predecessor manifest identity differs")
    excluded = {"components", "dataset_manifest_sha256", "deployment_snapshot_id"}
    _require({k: v for k, v in manifest.items() if k not in excluded} == {k: v for k, v in old.items() if k not in excluded},
             "quote-only manifest changed unrelated fields")
    before, after = old["components"], manifest["components"]
    _require(set(after) == set(before) | {_ADDED} and all(after[k] == pin for k, pin in before.items() if k not in _MUTABLE),
             "quote-only hotfix changed unrelated component pins")
    report = evidence[REPAIR_SCHEMA]
    _require(report.get("predecessor_manifest_identity") == old["dataset_manifest_sha256"]
             and report.get("cutoff") == manifest["cutoff_trade_date"], "quote repair lineage differs")
    for flag in ("price_write", "stock_pool_write", "database_write", "active_profile_write", "runtime_action"):
        _require(report.get(flag) is False, f"quote repair has an invalid {flag} flag")
    catalog = _pinned(candidate, after["minute_provider_catalog"], expected_path=_PROVIDER + "/instruments/all.txt")
    _pinned(candidate, after["minute_meta_export"], expected_path=_PROVIDER + "/meta_export.json")
    report_path = _pinned(candidate, after[_ADDED], expected_path="reports/minute_quote_catalog_repair.json")
    _require(_read(report_path) == report, "quote repair evidence differs from its manifest pin")
    _require(report["catalog"]["sha256"] == after["minute_provider_catalog"]["sha256"], "quote repair catalog hash differs")
    rebuilt = build_minute_quote_catalog(candidate / _PROVIDER)
    _require(catalog.read_bytes() == rebuilt.payload, "minute quote catalog differs from physical quote boundaries")
    # Prove reuse by inode, not by reading/hashing the full historical data.
    mutable_paths = {before[key]["path"] for key in _MUTABLE} | {"qe_dataset_manifest.json", "direct_monthly_state.json"}
    for parent, dirs, files in os.walk(predecessor_root, followlinks=False):
        for name in dirs:
            _plain(Path(parent) / name)
        for name in files:
            path = Path(parent) / name
            relative = path.relative_to(predecessor_root).as_posix()
            if relative not in mutable_paths:
                _plain(path)
                target = candidate / relative
                _plain(target)
                _require(os.path.samefile(path, target), f"quote-only hotfix did not reuse unchanged file: {relative}")
    # Consumer paths may change, never their non-minute component identities.
    excluded_profile = {"generation", "controller_paths", "node_bindings", "components"}
    _require({k: v for k, v in profile.items() if k not in excluded_profile} == {k: v for k, v in predecessor_profile.items() if k not in excluded_profile},
             "quote hotfix changed profile semantics")
    def rebound(value: Any, source: str, target: str) -> Any:
        if isinstance(value, Mapping):
            return {k: rebound(v, source, target) for k, v in value.items()}
        if isinstance(value, list):
            return [rebound(v, source, target) for v in value]
        if isinstance(value, str):
            return value.replace(source, target)
        return value
    _require(profile['controller_paths'] == rebound(predecessor_profile['controller_paths'],
             str(predecessor_root), str(candidate)), "quote controller path mapping differs")
    old_nodes = predecessor_profile.get('node_bindings', {})
    _require(set(profile['node_bindings']) == set(old_nodes), "quote node mapping set differs")
    for name, binding in old_nodes.items():
        _require(profile['node_bindings'][name] == rebound(binding, binding['candidate_root'],
                 profile['node_bindings'][name]['candidate_root']), "quote node path mapping differs")
    old_components, new_components = predecessor_profile["components"], profile["components"]
    changed = {"dataset_manifest_sha256", "dataset_manifest_file_sha256", "minute_pins"}
    _require(set(old_components) == set(new_components)
             and all(new_components[k] == value for k, value in old_components.items() if k not in changed),
             "quote hotfix changed unrelated profile pins")
    if "minute_pins" in old_components:
        unchanged = set(old_components["minute_pins"]) - {"instruments_sha256", "meta_export_sha256"}
        _require(set(old_components["minute_pins"]) == set(new_components["minute_pins"])
                 and all(old_components["minute_pins"][k] == new_components["minute_pins"][k] for k in unchanged)
                 and new_components["minute_pins"]["instruments_sha256"] == after["minute_provider_catalog"]["sha256"]
                 and new_components["minute_pins"]["meta_export_sha256"] == after["minute_meta_export"]["sha256"],
                 "quote hotfix minute profile pins differ")
    proof = evidence[READBACK_SCHEMA]
    _require(proof.get("status") == "PASS" and proof.get("dataset_manifest_sha256") == manifest["dataset_manifest_sha256"]
             and proof.get("manifest_file_sha256") == _sha(candidate / "qe_dataset_manifest.json")
             and proof.get("minute_catalog_sha256") == after["minute_provider_catalog"]["sha256"],
             "quote readback release identity differs")
    _require(all(proof.get(flag) is False for flag in ("database_write", "runtime_action", "training_experiment")),
             "quote readback has side effects")
    nodes = proof.get("nodes")
    _require(isinstance(nodes, Mapping) and set(nodes) == set(required_nodes), "quote readback nodes are incomplete")
    quote_samples = None
    sample_start, sample_end = proof.get("quote_sample_start"), proof.get("quote_sample_end")
    _require(isinstance(sample_start, str) and isinstance(sample_end, str)
             and sample_start <= sample_end and sample_end[:10] <= manifest["cutoff_trade_date"],
             "quote readback sample dates are invalid")
    sample_date = datetime.fromisoformat(sample_start).date().isoformat()
    calendar = (candidate / _PROVIDER / 'calendars/1min.txt').read_text(encoding='utf-8').splitlines()
    positions = [i for i, stamp in enumerate(calendar) if sample_start <= stamp <= sample_end]
    _require(len(positions) >= 5, "quote sample window has fewer than five calendar rows")
    pools = (candidate / "stock_pools/stock_universe.txt").read_text(encoding="utf-8").splitlines()
    for node in nodes.values():
        _require(node.get("status") == "PASS" and node.get("dataset_identity_complete") is True
                 and node.get("manifest_identity") == manifest["dataset_manifest_sha256"]
                 and node.get("manifest_file_sha256") == proof["manifest_file_sha256"]
                 and node.get("component_hashes", {}).get("minute_provider_catalog") == proof["minute_catalog_sha256"],
                 "quote node readback identity differs")
        samples = node.get("catalog_quotes")
        _require(isinstance(samples, Mapping) and bool(samples)
                 and set(node.get("PIT_buy_excluded_samples", [])) == set(samples), "quote/PIT readback samples are incomplete")
        for symbol, result in samples.items():
            prices = result.get("close", [])
            _require(result.get("rows") == len(prices) and len(prices) >= 5
                     and all(isinstance(x, (float, int)) and not isinstance(x, bool) and math.isfinite(x) and x > 0 for x in prices),
                     "quote node readback prices are invalid")
            _require(symbol in rebuilt.ranges and len(prices) == len(positions), "quote sample symbol/window differs")
            path = candidate / _PROVIDER / 'features' / symbol.lower() / 'close.1min.bin'
            with path.open('rb') as reader:
                offset = int(np.frombuffer(reader.read(4), dtype='<f4')[0])
                actual = []
                for index in positions:
                    _require(index >= offset, "quote sample predates physical file")
                    reader.seek(4 * (index - offset + 1))
                    actual.append(float(np.frombuffer(reader.read(4), dtype='<f4')[0]))
            _require(actual == prices, "quote readback does not match frozen price bytes")
            _require(not any(parts[0] == symbol and parts[1] <= sample_date <= parts[2]
                             for line in pools if (parts := line.split())), "quote readback changed PIT buy eligibility")
        _require(quote_samples is None or quote_samples == samples, "quote readback node prices differ")
        quote_samples = samples
