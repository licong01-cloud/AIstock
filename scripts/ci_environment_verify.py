"""Verify the prebuilt Windows AIstock-CI environment without installing anything."""

from __future__ import annotations

import importlib.util
import json
import os
import platform
import sys
from pathlib import Path
from typing import Mapping, Sequence


DEFAULT_ENVIRONMENT_NAME = "AIstock-CI"
DEFAULT_REQUIRED_MODULES = ("nox", "pytest", "yaml")


def verify_environment(
    environ: Mapping[str, str] | None = None,
    *,
    system: str | None = None,
    prefix: str | None = None,
    required_modules: Sequence[str] = DEFAULT_REQUIRED_MODULES,
) -> dict[str, object]:
    """Return a compact, non-secret readiness record for a prebuilt CI env."""
    env = dict(os.environ if environ is None else environ)
    expected_name = env.get("AISTOCK_CI_ENV_NAME", DEFAULT_ENVIRONMENT_NAME).strip() or DEFAULT_ENVIRONMENT_NAME
    observed_system = system or platform.system()
    observed_prefix = prefix or sys.prefix
    observed_name = env.get("CONDA_DEFAULT_ENV", "").strip() or Path(observed_prefix).name
    expected_fingerprint = env.get("AISTOCK_CI_EXPECTED_FINGERPRINT", "").strip()
    actual_fingerprint = env.get("AISTOCK_CI_ENV_FINGERPRINT", "").strip()
    env_root = env.get("AISTOCK_CI_ENV_ROOT", "").strip()
    failures: list[str] = []

    if observed_system != "Windows":
        failures.append(f"platform={observed_system or 'unknown'} (expected Windows)")
    if observed_name != expected_name:
        failures.append(f"environment={observed_name or 'unknown'} (expected {expected_name})")
    env_root_path = Path(env_root).resolve() if env_root else None
    prefix_path = Path(observed_prefix).resolve()
    if env_root and not env_root_path.is_dir():
        failures.append("AISTOCK_CI_ENV_ROOT does not exist")
    elif not env_root:
        failures.append("AISTOCK_CI_ENV_ROOT is missing")
    elif prefix_path != env_root_path and env_root_path not in prefix_path.parents:
        failures.append("python prefix is outside AISTOCK_CI_ENV_ROOT")
    if not expected_fingerprint:
        failures.append("AISTOCK_CI_EXPECTED_FINGERPRINT is missing")
    elif not actual_fingerprint:
        failures.append("AISTOCK_CI_ENV_FINGERPRINT is missing")
    elif actual_fingerprint != expected_fingerprint:
        failures.append("environment fingerprint mismatch")

    missing_modules = [name for name in required_modules if importlib.util.find_spec(name) is None]
    if missing_modules:
        failures.append("missing prebuilt modules=" + ",".join(missing_modules))

    payload: dict[str, object] = {
        "schema_version": "aistock_ci_environment_receipt_v1",
        "status": "ready" if not failures else "environment_mismatch",
        "platform": observed_system,
        "environment_name": observed_name,
        "environment_root_present": bool(env_root and Path(env_root).is_dir()),
        "python_executable": str(sys.executable),
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "required_modules": list(required_modules),
        "missing_modules": missing_modules,
        "failure_reasons": failures,
    }
    return payload


def main() -> int:
    payload = verify_environment()
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
