#!/usr/bin/env python
"""Sync QE model artifacts into the AIstock per-algorithm model cache.

The utility is intentionally conservative: it plans by default, writes only
with --apply, requires an explicit destination cache root, and never searches
alternate model names or fallback locations.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import shutil
import sys
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Iterable


SIDECAR_SUFFIX = ".aistock-sync.json"


class SyncError(RuntimeError):
    """Raised for invalid sync input or unsafe write attempts."""


@dataclass(frozen=True)
class ModelPlan:
    model: str
    source: str
    destination: str
    source_sha256: str
    source_size_bytes: int
    destination_exists: bool
    destination_sha256: str | None
    action: str
    sidecar: str


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_windows_runtime() -> bool:
    return os.name == "nt" or platform.system().lower().startswith("win")


def translate_wsl_path(path_text: str, *, wsl_distro: str | None = None) -> str:
    """Translate common WSL paths for Windows callers without probing fallback paths."""

    text = path_text.strip()
    if not text:
        raise SyncError("path must not be empty")

    posix = PurePosixPath(text)
    parts = posix.parts
    if (
        _is_windows_runtime()
        and len(parts) >= 3
        and parts[0] == "/"
        and parts[1] == "mnt"
        and len(parts[2]) == 1
    ):
        drive = parts[2].upper()
        remainder = parts[3:]
        return str(PureWindowsPath(f"{drive}:/", *remainder))

    if _is_windows_runtime() and text.startswith("/") and not text.startswith("//"):
        distro = wsl_distro or os.environ.get("AISTOCK_WSL_DISTRO") or os.environ.get("WSL_DISTRO_NAME")
        if not distro:
            raise SyncError(
                f"Linux path {text!r} requires --wsl-distro or AISTOCK_WSL_DISTRO on Windows"
            )
        return str(PureWindowsPath("//wsl.localhost", distro, *parts[1:]))

    return text


def normalize_input_path(path_text: str, *, wsl_distro: str | None = None) -> Path:
    return Path(translate_wsl_path(path_text, wsl_distro=wsl_distro)).expanduser()


def _validate_algo_code(algo_code: str) -> str:
    if not algo_code:
        raise SyncError("--algo-code is required")
    if any(sep in algo_code for sep in ("/", "\\", ":")) or algo_code in {".", ".."}:
        raise SyncError(f"algo code must be one path segment, got {algo_code!r}")
    return algo_code


def _validate_model_name(model: str) -> str:
    path = PurePosixPath(model.replace("\\", "/"))
    if ":" in model or path.is_absolute() or ".." in path.parts or path.name != model.replace("\\", "/"):
        raise SyncError(f"model names must be plain filenames under --source-dir, got {model!r}")
    if not path.name:
        raise SyncError("model name must not be empty")
    return path.name


def parse_expected_hashes(values: Iterable[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise SyncError(f"--expected-sha256 must use filename=sha256, got {item!r}")
        name, value = item.split("=", 1)
        name = _validate_model_name(name)
        value = value.lower()
        if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
            raise SyncError(f"expected hash for {name!r} must be a 64-character hex sha256")
        result[name] = value
    return result


def build_plan(
    *,
    source_dir: Path,
    cache_root: Path,
    algo_code: str,
    models: Iterable[str],
    expected_hashes: dict[str, str] | None = None,
    overwrite: bool = False,
) -> list[ModelPlan]:
    algo_code = _validate_algo_code(algo_code)
    expected_hashes = expected_hashes or {}
    model_names = [_validate_model_name(model) for model in models]
    if not model_names:
        raise SyncError("at least one --model must be provided; no implicit model discovery is allowed")

    if not source_dir.exists() or not source_dir.is_dir():
        raise SyncError(f"source directory does not exist or is not a directory: {source_dir}")

    destination_dir = cache_root / algo_code
    plans: list[ModelPlan] = []
    for model in model_names:
        source = source_dir / model
        if not source.exists() or not source.is_file():
            raise SyncError(f"required source model file is missing: {source}")
        source_hash = _sha256_file(source)
        expected_hash = expected_hashes.get(model)
        if expected_hash and source_hash != expected_hash:
            raise SyncError(
                f"sha256 mismatch for {model}: expected {expected_hash}, got {source_hash}"
            )

        destination = destination_dir / model
        destination_exists = destination.exists()
        destination_hash = _sha256_file(destination) if destination_exists and destination.is_file() else None
        if destination_exists and not destination.is_file():
            raise SyncError(f"destination exists but is not a file: {destination}")

        if destination_hash == source_hash:
            action = "skip_same_hash"
        elif destination_exists and not overwrite:
            action = "blocked_existing_different_hash"
        elif destination_exists:
            action = "overwrite"
        else:
            action = "copy"

        plans.append(
            ModelPlan(
                model=model,
                source=str(source),
                destination=str(destination),
                source_sha256=source_hash,
                source_size_bytes=source.stat().st_size,
                destination_exists=destination_exists,
                destination_sha256=destination_hash,
                action=action,
                sidecar=str(destination.with_name(destination.name + SIDECAR_SUFFIX)),
            )
        )

    return plans


def _sidecar_payload(*, plan: ModelPlan, algo_code: str, source_dir: Path, cache_root: Path) -> dict[str, object]:
    return {
        "schema_version": 1,
        "tool": "scripts/sync_qe_models_to_aistock_cache.py",
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "algo_code": algo_code,
        "model": plan.model,
        "source": plan.source,
        "destination": plan.destination,
        "source_dir": str(source_dir),
        "cache_root": str(cache_root),
        "sha256": plan.source_sha256,
        "size_bytes": plan.source_size_bytes,
        "action": plan.action,
        "write_mode": "apply",
    }


def apply_plan(
    plans: Iterable[ModelPlan],
    *,
    algo_code: str,
    source_dir: Path,
    cache_root: Path,
) -> list[dict[str, object]]:
    applied: list[dict[str, object]] = []
    blocked = [plan for plan in plans if plan.action == "blocked_existing_different_hash"]
    if blocked:
        names = ", ".join(plan.model for plan in blocked)
        raise SyncError(f"destination hash differs for {names}; pass --overwrite to replace explicitly")

    for plan in plans:
        destination = Path(plan.destination)
        sidecar = Path(plan.sidecar)
        payload = _sidecar_payload(plan=plan, algo_code=algo_code, source_dir=source_dir, cache_root=cache_root)
        if plan.action in {"copy", "overwrite"}:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(plan.source, destination)
            copied_hash = _sha256_file(destination)
            if copied_hash != plan.source_sha256:
                raise SyncError(
                    f"post-copy sha256 mismatch for {plan.model}: expected {plan.source_sha256}, got {copied_hash}"
                )
            sidecar.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        elif plan.action == "skip_same_hash":
            # Refresh sidecar so the cache remains auditable without mutating the model bytes.
            destination.parent.mkdir(parents=True, exist_ok=True)
            sidecar.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        else:
            raise SyncError(f"unsupported plan action {plan.action!r} for {plan.model}")
        applied.append(payload)

    return applied


def _json_default(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", required=True, help="Directory containing QE model files")
    parser.add_argument("--cache-root", required=True, help="AIstock model cache root; algo subdir is created below it")
    parser.add_argument("--algo-code", required=True, help="Execution algorithm code, e.g. V25_1_SMALL_CAP")
    parser.add_argument("--model", action="append", default=[], help="Model filename to sync; repeat for multiple files")
    parser.add_argument(
        "--expected-sha256",
        action="append",
        default=[],
        help="Optional source hash assertion as filename=64hex; repeat per model",
    )
    parser.add_argument("--wsl-distro", help="WSL distro name used to translate Linux absolute paths on Windows")
    parser.add_argument("--overwrite", action="store_true", help="Allow replacing an existing destination file")
    parser.add_argument("--apply", action="store_true", help="Perform writes. Omit for dry-run planning")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        source_dir = normalize_input_path(args.source_dir, wsl_distro=args.wsl_distro)
        cache_root = normalize_input_path(args.cache_root, wsl_distro=args.wsl_distro)
        expected_hashes = parse_expected_hashes(args.expected_sha256)
        plans = build_plan(
            source_dir=source_dir,
            cache_root=cache_root,
            algo_code=args.algo_code,
            models=args.model,
            expected_hashes=expected_hashes,
            overwrite=args.overwrite,
        )
        result: dict[str, object] = {
            "mode": "apply" if args.apply else "dry_run",
            "source_dir": str(source_dir),
            "cache_root": str(cache_root),
            "algo_code": args.algo_code,
            "plans": [asdict(plan) for plan in plans],
        }
        if args.apply:
            result["applied"] = apply_plan(
                plans,
                algo_code=args.algo_code,
                source_dir=source_dir,
                cache_root=cache_root,
            )

        if args.json:
            print(json.dumps(result, indent=2, sort_keys=True, default=_json_default))
        else:
            print(f"mode: {result['mode']}")
            for plan in plans:
                print(f"{plan.action}: {plan.source} -> {plan.destination} sha256={plan.source_sha256}")
        return 0
    except SyncError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
