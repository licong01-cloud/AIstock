"""Explicit fixed-HMM file extension and DB subscription contracts; never fit."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from copy import deepcopy
from datetime import date
from pathlib import Path
from typing import Any, Mapping

from backend.services.quantevolver.qe_active_dataset_profile import (
    _require_plain_existing_chain,
    load_active_qe_profile,
)

POLICY_SCHEMA = "hmm_coefficient_extension_policy_v1"
EXTENSION_MODE = "frozen_window_extension_v1"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_PRESET = re.compile(r"[A-Za-z0-9_-]+\Z")
_ASSET_FIELDS = {
    "coefficient_extension",
    "coefficient_windows",
    "qe_frozen_input_bundle",
    "daily_coefficient_subscription",
}
_FILE_COMPONENTS = {
    "sector_data_h5": "sector_data",
    "index_daily_h5": "index_daily",
    "sector_code_map_json": "sector_code_map",
    "market_context_parquet": "market_context",
    "sector_membership_spans_parquet": "sector_membership_spans",
    "sector_quote_availability_json": "sector_quote_availability",
}


class CoefficientExtensionError(RuntimeError):
    def __init__(self, reason: str, message: str) -> None:
        self.reason_code = reason
        super().__init__(f"{reason}: {message}")


def _fail(message: str, reason: str = "hmm_coefficient_extension_contract_invalid") -> None:
    raise CoefficientExtensionError(reason, message)


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def config_fingerprint(config: Mapping[str, Any]) -> str:
    return fingerprint({k: v for k, v in config.items() if k not in _ASSET_FIELDS})


def make_daily_subscription(
    *,
    config: Mapping[str, Any],
    snapshot_id: str,
    model_sha256: str,
    signal_presets: list[str],
    enabled: bool,
) -> dict[str, Any]:
    if (
        type(enabled) is not bool
        or not isinstance(snapshot_id, str)
        or not snapshot_id.strip()
        or not isinstance(model_sha256, str)
        or not _SHA.fullmatch(model_sha256)
        or not isinstance(signal_presets, list)
        or not signal_presets
        or any(not isinstance(p, str) or not _PRESET.fullmatch(p) for p in signal_presets)
        or len(set(signal_presets)) != len(signal_presets)
        or config.get("strict_no_leakage") is not True
    ):
        _fail("explicit fixed-model daily subscription is required", "hmm_daily_subscription_identity_changed")
    return {
        "schema_version": "hmm_database_daily_subscription_v1",
        "source_mode": "database_daily",
        "enabled": enabled,
        "snapshot_id": snapshot_id,
        "model_sha256": model_sha256,
        "config_sha256": config_fingerprint(config),
        "signal_presets": sorted(signal_presets),
    }


def validate_daily_subscription(config: Mapping[str, Any], policy: Any, *, model_sha256: str) -> None:
    if not isinstance(policy, dict):
        _fail("daily subscription is missing", "hmm_daily_subscription_identity_changed")
    expected = make_daily_subscription(
        config=config,
        snapshot_id=policy.get("snapshot_id"),
        model_sha256=model_sha256,
        signal_presets=policy.get("signal_presets"),
        enabled=policy.get("enabled"),
    )
    if policy != expected or policy["enabled"] is not True:
        _fail("daily subscription/model/config changed or disabled", "hmm_daily_subscription_identity_changed")


def make_policy(
    *,
    config: Mapping[str, Any],
    snapshot_id: str,
    model_sha256: str,
    coefficient_start: str,
    signal_presets: list[str],
    enabled: bool,
) -> dict[str, Any]:
    if type(enabled) is not bool or not snapshot_id or not _SHA.fullmatch(model_sha256):
        _fail("enabled, snapshot identity and model SHA are required")
    try:
        start = date.fromisoformat(coefficient_start)
    except (TypeError, ValueError) as exc:
        raise CoefficientExtensionError(
            "hmm_coefficient_extension_contract_invalid", "invalid coefficient_start"
        ) from exc
    if (
        not isinstance(signal_presets, list)
        or not signal_presets
        or any(not isinstance(p, str) for p in signal_presets)
        or len(set(signal_presets)) != len(signal_presets)
    ):
        _fail("explicit unique signal_presets are required")
    if any(not isinstance(p, str) or not _PRESET.fullmatch(p) for p in signal_presets):
        _fail("invalid preset name")
    # The existing coefficient contract requires calibration to precede replay.
    # No model-age threshold or automatic retraining is introduced here.
    for key in ("train_end", "val_end"):
        if key not in config or date.fromisoformat(str(config[key])) >= start:
            _fail(f"{key} must precede coefficient_start", "hmm_coefficient_extension_model_asof_invalid")
    if config.get("strict_no_leakage") is not True:
        _fail("automatic extension requires the explicit strict_no_leakage contract")
    return {
        "schema_version": POLICY_SCHEMA,
        "enabled": enabled,
        "snapshot_id": snapshot_id,
        "model_sha256": model_sha256,
        "config_sha256": config_fingerprint(config),
        "coefficient_start": start.isoformat(),
        "signal_presets": sorted(signal_presets),
    }


def validate_policy(config: Mapping[str, Any], policy: Mapping[str, Any]) -> None:
    expected = make_policy(
        config=config,
        snapshot_id=str(policy.get("snapshot_id") or ""),
        model_sha256=str(policy.get("model_sha256") or ""),
        coefficient_start=str(policy.get("coefficient_start") or ""),
        signal_presets=policy.get("signal_presets"),
        enabled=policy.get("enabled"),
    )
    if dict(policy) != expected:
        _fail("frozen model/config policy changed", "hmm_coefficient_extension_model_contract_drift")


def _plain_file(root: Path, relative: Any) -> Path:
    if not isinstance(relative, str) or not relative or "\\" in relative:
        _fail("component path must be an explicit relative POSIX path")
    path = Path(relative)
    if path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        _fail("component path is not contained")
    target = root / path
    _require_plain_existing_chain(target, field="HMM coefficient input")
    resolved = target.resolve(strict=True)
    if not resolved.is_relative_to(root.resolve(strict=True)) or not resolved.is_file():
        _fail("input is not a regular contained file")
    return resolved


def resolve_active_input(profile: Any | None = None) -> dict[str, Any]:
    """Read only small manifest/calendar metadata; worker verifies input bytes."""
    profile = profile if profile is not None else load_active_qe_profile()
    if profile is None:
        _fail("active dataset profile is not configured", "hmm_coefficient_extension_input_unavailable")
    root = profile.controller_candidate_root
    components = profile.raw["components"]
    manifest_path = _plain_file(root, "qe_dataset_manifest.json")
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    if (
        hashlib.sha256(manifest_bytes).hexdigest() != components["dataset_manifest_file_sha256"]
        or manifest.get("schema_version") != "qe_dataset_manifest_v1"
        or manifest.get("dataset_manifest_sha256") != components["dataset_manifest_sha256"]
        or manifest.get("release_id") != profile.release_id
        or manifest.get("cutoff_trade_date") != profile.cutoff.isoformat()
    ):
        _fail("manifest differs from active profile", "hmm_coefficient_extension_input_identity_mismatch")
    specs: dict[str, Any] = {}
    for field, component in _FILE_COMPONENTS.items():
        spec = manifest.get("components", {}).get(component)
        if not isinstance(spec, dict) or not _SHA.fullmatch(str(spec.get("sha256") or "")):
            _fail(f"missing shared component {component}", "hmm_coefficient_extension_input_unavailable")
        _plain_file(root, spec.get("path"))
        specs[field] = {"relative_path": spec["path"], "sha256": spec["sha256"]}
    pins = components.get("sector_context_pins") or {}
    shared = {
        "sector_data_h5": "sector_data_sha256",
        "sector_code_map_json": "code_map_sha256",
        "market_context_parquet": "market_context_sha256",
        "sector_membership_spans_parquet": "membership_sha256",
        "sector_quote_availability_json": "quote_availability_sha256",
    }
    if any(specs[key]["sha256"] != pins.get(pin) for key, pin in shared.items()):
        _fail("shared sector component pins differ from profile")
    if specs["index_daily_h5"]["sha256"] != components["index_pins"]["sha256"]:
        _fail("benchmark component pin differs from profile")
    calendar_path = _plain_file(root, "components/daily_bin_candidate/calendars/day.txt")
    raw_calendar = calendar_path.read_bytes()
    if hashlib.sha256(raw_calendar).hexdigest() != manifest.get("qlib_calendar_sha256"):
        _fail("file calendar hash differs from manifest")
    calendar = [
        date.fromisoformat(line.strip()).isoformat()
        for line in raw_calendar.decode("utf-8").splitlines()
        if line.strip()
    ]
    if not calendar or calendar != sorted(set(calendar)) or calendar[-1] != profile.cutoff.isoformat():
        _fail("calendar has duplicates, is unordered, or does not cover cutoff")
    identity = {
        "generation": profile.generation,
        "release_id": profile.release_id,
        "dataset_manifest_sha256": components["dataset_manifest_sha256"],
        "dataset_manifest_file_sha256": components["dataset_manifest_file_sha256"],
        "active_profile_sha256": profile.profile_sha256,
    }
    node = profile.raw["node_bindings"].get("wsl2-5080")
    if not isinstance(node, dict) or not str(node.get("candidate_root") or "").startswith("/"):
        _fail("active profile has no explicit wsl2-5080 root")
    return {
        "identity": identity,
        "calendar": calendar,
        "qe_default_window": {
            "test_start": profile.defaults["test_start"],
            "backtest_end": profile.defaults["backtest_end"],
        },
        "controller_root": str(root),
        "bundle": {
            "schema_version": "qe_hmm_frozen_input_v1",
            "dataset_root": node["candidate_root"],
            "dataset_identity": identity,
            "files": specs,
            "market_volume_definition": pins.get("market_volume_definition"),
        },
    }


def build_plan(
    *,
    config_id: str,
    config: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    signal_preset: str,
    preset_coeffs: Mapping[str, Any],
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    policy = config.get("coefficient_extension")
    if not isinstance(policy, dict):
        _fail("automatic extension policy is not configured")
    validate_policy(config, policy)
    if policy["enabled"] is not True or signal_preset not in policy["signal_presets"]:
        _fail("extension policy/preset is not enabled")
    if str(snapshot.get("snapshot_id")) != policy["snapshot_id"] or str(snapshot.get("config_id")) != config_id:
        _fail("snapshot differs from the fixed policy")
    if str(snapshot.get("status") or "").lower() not in {"completed", "success", "succeeded", "ready"}:
        _fail("snapshot is not completed")
    model_path = Path(str(snapshot.get("model_path") or ""))
    if not model_path.is_absolute():
        _fail("model path must be absolute")
    _require_plain_existing_chain(model_path, field="HMM frozen model")
    if not model_path.is_file() or file_sha256(model_path) != policy["model_sha256"]:
        _fail("model bytes changed", "hmm_coefficient_extension_model_identity_mismatch")
    start = policy["coefficient_start"]
    dates = [d for d in inputs["calendar"] if d >= start]
    if not dates or dates[0] != start:
        _fail("coefficient_start must exist in the frozen calendar")
    end = dates[-1]
    windows = {(start, end)}
    default_window = inputs["qe_default_window"]
    lo, hi = default_window["test_start"], default_window["backtest_end"]
    if start <= lo <= hi <= end and lo in dates and hi in dates:
        windows.add((lo, hi))
    for w in config.get("coefficient_windows", []):
        if (
            isinstance(w, dict)
            and w.get("snapshot_id") == policy["snapshot_id"]
            and w.get("preset") == signal_preset
            and w.get("strict_no_leakage") is True
        ):
            lo, hi = str(w.get("test_start") or ""), str(w.get("backtest_end") or "")
            if start <= lo <= hi <= end and lo in dates and hi in dates:
                windows.add((lo, hi))
    key = fingerprint(
        {
            "policy": policy,
            "preset": signal_preset,
            "preset_coeffs": preset_coeffs,
            "input_identity": inputs["identity"],
            "start": start,
            "end": end,
            "generator_sha256": file_sha256(
                Path(__file__).resolve().parents[3] / "scripts" / "precompute_hmm_coefficients.py"
            ),
        }
    )
    filename = f"coefficients_{signal_preset}_{start}_{end}__auto_{key[:24]}.json"
    return {
        "generation_mode": EXTENSION_MODE,
        "source_mode": "qe_frozen_file",
        "extension_key": key,
        "policy": dict(policy),
        "snapshot_id": policy["snapshot_id"],
        "config_id": config_id,
        "signal_preset": signal_preset,
        "preset_coeffs": dict(preset_coeffs),
        "model_path": str(model_path),
        "model_sha256": policy["model_sha256"],
        "generator_sha256": file_sha256(
            Path(__file__).resolve().parents[3] / "scripts" / "precompute_hmm_coefficients.py"
        ),
        "config_json": dict(config),
        "test_start": start,
        "backtest_end": end,
        "trade_dates": dates,
        "windows": [list(w) for w in sorted(windows)],
        "frozen_input_bundle": deepcopy(inputs["bundle"]),
        "dataset_identity": dict(inputs["identity"]),
        "controller_root": inputs["controller_root"],
        "output_path": str(model_path.parent / filename),
        # Reuse the existing job table: historical windows need no next-day row.
        "as_of_trade_date": end,
        "effective_trade_date": end,
        "input_data_max_dates": {"frozen_release": end},
    }


def window_payload(payload: Mapping[str, Any], plan: Mapping[str, Any], start: str, end: str) -> dict[str, Any]:
    expected_identity = plan["dataset_identity"]
    if payload.get("dataset_identity") != expected_identity or payload.get("model_sha256") != plan["model_sha256"]:
        _fail("generated artifact identity differs from the frozen plan")
    if payload.get("preset_key") != plan["signal_preset"] or payload.get("preset_coeffs") != plan["preset_coeffs"]:
        _fail("generated preset differs from the frozen plan")
    expected_files = {k: v["sha256"] for k, v in plan["frozen_input_bundle"]["files"].items()}
    if payload.get("input_file_sha256") != expected_files:
        _fail("generated input hashes differ from the frozen plan")
    dates = [d for d in plan["trade_dates"] if start <= d <= end]
    if not dates or dates[0] != start or dates[-1] != end:
        _fail("requested window is not fully covered")
    daily = payload.get("daily_coefficients")
    maps = payload.get("stock_sector_map_by_date")
    spans = payload.get("stock_sector_membership_spans")
    if not isinstance(spans, dict) or not spans:
        _fail("missing complete PIT membership spans")
    for rows in spans.values():
        if not isinstance(rows, list) or not rows:
            _fail("invalid PIT membership spans")
        previous_end = ""
        for span in rows:
            if not isinstance(span, dict) or not all(
                isinstance(span.get(k), str) for k in ("from", "to", "sector_code")
            ):
                _fail("invalid PIT membership span")
            date.fromisoformat(span["from"])
            date.fromisoformat(span["to"])
            if span["from"] > span["to"] or span["from"] <= previous_end:
                _fail("overlapping or unordered PIT membership spans")
            previous_end = span["to"]
    unavailable = payload.get("quote_unavailable_sector_codes_by_date")
    asofs = payload.get("input_data_max_dates_by_date")
    if any(
        not isinstance(value, dict) or set(value) != set(plan["trade_dates"]) for value in (daily, unavailable, asofs)
    ):
        _fail("daily coefficients, PIT maps, quote authority and as-of dates must cover the full calendar")
    if maps is not None and (not isinstance(maps, dict) or set(maps) != set(plan["trade_dates"])):
        _fail("PIT maps do not cover the full calendar")
    for d in plan["trade_dates"]:
        # Keep the official compact membership representation; do not expand
        # millions of stock/date rows into the persisted coefficient asset.
        day_map = {
            symbol: span["sector_code"]
            for symbol, rows in spans.items()
            for span in rows
            if span["from"] <= d <= span["to"]
        }
        if maps is not None and maps[d] != day_map:
            _fail(f"PIT maps differ from membership spans on {d}")
        if not isinstance(daily[d], dict) or not daily[d] or not day_map:
            _fail(f"empty coefficients or membership on {d}")
        if any(
            isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0
            for v in daily[d].values()
        ):
            _fail(f"non-finite/nonpositive coefficient on {d}")
        if not isinstance(unavailable[d], list) or len(set(unavailable[d])) != len(unavailable[d]):
            _fail(f"invalid quote availability on {d}")
        missing = set(day_map.values()) - set(daily[d])
        if missing != set(unavailable[d]) or set(unavailable[d]) & set(daily[d]):
            _fail(f"unexplained coefficient gap or fabricated unavailable quote on {d}")
        if (
            not isinstance(asofs[d], dict)
            or not asofs[d]
            or any(not isinstance(v, str) or date.fromisoformat(v) > date.fromisoformat(d) for v in asofs[d].values())
        ):
            _fail(f"future or missing source date on {d}")
    result = deepcopy(dict(payload))
    for field in ("daily_coefficients", "quote_unavailable_sector_codes_by_date", "input_data_max_dates_by_date"):
        result[field] = {d: result[field][d] for d in dates}
    if maps is not None:
        result["stock_sector_map_by_date"] = {d: result["stock_sector_map_by_date"][d] for d in dates}
    result["stock_sector_membership_spans"] = {
        symbol: [
            dict(span, **{"from": max(span["from"], start), "to": min(span["to"], end)})
            for span in rows
            if span["from"] <= end and span["to"] >= start
        ]
        for symbol, rows in spans.items()
        if any(span["from"] <= end and span["to"] >= start for span in rows)
    }
    result.update(
        {
            "test_start": start,
            "backtest_end": end,
            "coefficient_start": start,
            "coefficient_end": end,
            "strict_no_leakage": True,
            "snapshot_id": plan["snapshot_id"],
            "config_id": plan["config_id"],
            "extension_key": plan["extension_key"],
            "generation_mode": EXTENSION_MODE,
        }
    )
    return result


def publish_coefficient_content(path: Path, payload: Mapping[str, Any]) -> tuple[str, int]:
    """One immutable atomic writer for both explicit HMM input modes."""
    content = canonical_bytes(payload)
    _require_plain_existing_chain(path, field="HMM coefficient output")
    staged: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=".hmm-coefficient-", suffix=".pending", delete=False
        ) as stream:
            staged = Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(staged, path)  # Atomic publication, no replacement of an existing asset.
        except FileExistsError:
            if not path.is_file() or path.read_bytes() != content:
                _fail("refusing to overwrite a different coefficient asset")
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)
    if path.read_bytes() != content:
        _fail("coefficient writer/readback mismatch")
    return hashlib.sha256(content).hexdigest(), len(content)


def publish_window(payload: Mapping[str, Any], plan: Mapping[str, Any], start: str, end: str) -> dict[str, Any]:
    result = window_payload(payload, plan, start, end)
    sha = fingerprint(result)
    filename = f"coefficients_{plan['signal_preset']}_{start}_{end}__auto_{plan['extension_key'][:16]}__{sha[:16]}.json"
    path = Path(plan["model_path"]).parent / filename
    actual_sha, byte_count = publish_coefficient_content(path, result)
    if actual_sha != sha:
        _fail("coefficient identity/readback mismatch")
    identity = plan["dataset_identity"]
    return {
        "role": "automatic_frozen_model_extension",
        "preset": plan["signal_preset"],
        "test_start": start,
        "backtest_end": end,
        "strict_no_leakage": True,
        "snapshot_id": plan["snapshot_id"],
        "config_id": plan["config_id"],
        "model_sha256": plan["model_sha256"],
        "dataset_generation": identity["generation"],
        "release_id": identity["release_id"],
        "dataset_manifest_identity": identity["dataset_manifest_sha256"],
        "dataset_manifest_file_sha256": identity["dataset_manifest_file_sha256"],
        "active_profile_sha256": identity["active_profile_sha256"],
        "coefficient_filename": filename,
        "coefficient_sha256": sha,
        "coefficient_bytes": byte_count,
        "extension_key": plan["extension_key"],
        "generator_sha256": plan["generator_sha256"],
    }
