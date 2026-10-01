"""Verify the prebuilt Windows AIstock-CI environment without installing anything."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import platform
import re
import sys
import tempfile
from pathlib import Path
from typing import Mapping, Sequence


DEFAULT_ENVIRONMENT_NAME = "AIstock-CI"
DEFAULT_REQUIRED_MODULES = ("nox", "pytest", "yaml")
TEST_TEMP_ROOT = Path("X:/AIstock-CI/tmp")
CODEQL_BUNDLE_REQUIRED_ENV = "AISTOCK_CI_CODEQL_BUNDLE_REQUIRED"
CODEQL_BUNDLE_PATH_ENV = "AISTOCK_CI_CODEQL_BUNDLE_PATH"
CODEQL_BUNDLE_SHA256_ENV = "AISTOCK_CI_CODEQL_BUNDLE_SHA256"
CODEQL_BUNDLE_VERSION_ENV = "AISTOCK_CI_CODEQL_BUNDLE_VERSION"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
    codeql_bundle_required = env.get(CODEQL_BUNDLE_REQUIRED_ENV, "").strip() == "1"
    codeql_bundle_path = env.get(CODEQL_BUNDLE_PATH_ENV, "").strip()
    codeql_bundle_sha256 = env.get(CODEQL_BUNDLE_SHA256_ENV, "").strip().casefold()
    codeql_bundle_version = env.get(CODEQL_BUNDLE_VERSION_ENV, "").strip()
    codeql_bundle_observed_sha256: str | None = None
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

    if codeql_bundle_required:
        bundle = Path(codeql_bundle_path) if codeql_bundle_path else None
        if bundle is None:
            failures.append(f"{CODEQL_BUNDLE_PATH_ENV} is missing")
        elif not bundle.exists():
            failures.append("prebuilt CodeQL bundle does not exist")
        elif not codeql_bundle_sha256:
            failures.append(f"{CODEQL_BUNDLE_SHA256_ENV} is missing")
        elif not codeql_bundle_version:
            failures.append(f"{CODEQL_BUNDLE_VERSION_ENV} is missing")
        else:
            identity_file = bundle / ".codeqlmanifest.json" if bundle.is_dir() else bundle
            if not identity_file.is_file():
                failures.append("prebuilt CodeQL bundle identity manifest does not exist")
            elif bundle.is_dir() and (
                not (bundle / "codeql.cmd").is_file()
                or not bundle.parent.with_name(f"{bundle.parent.name}.complete").is_file()
            ):
                failures.append("prebuilt CodeQL toolcache entry is incomplete")
            else:
                codeql_bundle_observed_sha256 = _sha256(identity_file)
                if codeql_bundle_observed_sha256.casefold() != codeql_bundle_sha256:
                    failures.append("prebuilt CodeQL bundle SHA-256 mismatch")
            if codeql_bundle_version.casefold() not in str(bundle).casefold():
                failures.append("prebuilt CodeQL bundle version is not reflected in its path")

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
        "codeql_bundle_required": codeql_bundle_required,
        "codeql_bundle_present": bool(codeql_bundle_path and Path(codeql_bundle_path).exists()),
        "codeql_bundle_kind": (
            "toolcache" if codeql_bundle_path and Path(codeql_bundle_path).is_dir() else "archive"
        ),
        "codeql_bundle_version": codeql_bundle_version or None,
        "codeql_bundle_sha256_match": (
            codeql_bundle_observed_sha256 is not None
            and codeql_bundle_observed_sha256.casefold() == codeql_bundle_sha256
        ),
        "failure_reasons": failures,
    }
    return payload


def prepare_test_temp(environ: Mapping[str, str]) -> dict[str, str]:
    """Prepare writable job-scoped X storage; never use tempfile's fallback."""
    repository = environ.get("GITHUB_REPOSITORY", "").split("/")
    parts = [
        *repository,
        environ.get("GITHUB_RUN_ID", ""),
        environ.get("GITHUB_RUN_ATTEMPT", ""),
        environ.get("GITHUB_JOB", ""),
    ]
    if len(repository) != 2 or any(not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}", part) or part in {".", ".."} for part in parts):
        raise ValueError("CI temporary storage requires valid repository/run/attempt/job identity")
    # strict drive resolution prevents missing X mounts and junctions onto C.
    drive = Path(TEST_TEMP_ROOT.anchor).resolve(strict=True)
    root = TEST_TEMP_ROOT.resolve()
    if drive.drive.casefold() != TEST_TEMP_ROOT.drive.casefold() or not root.is_relative_to(drive):
        raise ValueError("CI temporary storage escaped the configured X drive")
    root.mkdir(parents=True, exist_ok=True)
    job_root = root.joinpath(*parts)
    directories = {"temporary": job_root / "tmp", "pytest": job_root / "pytest"}
    for directory in directories.values():
        resolved = directory.resolve()
        if resolved != directory:
            raise ValueError("CI temporary storage escaped its job root")
        directory.mkdir(parents=True, exist_ok=True)
        # Probe precisely this directory, not the ambient TEMP location.
        with tempfile.TemporaryFile(dir=directory) as handle:
            handle.write(b"ci-temp-write-probe")
            handle.flush()
    return {
        "TEMP": str(directories["temporary"]),
        "TMP": str(directories["temporary"]),
        "TMPDIR": str(directories["temporary"]),
        "PYTEST_DEBUG_TEMPROOT": str(directories["pytest"]),
    }


def main() -> int:
    payload = verify_environment()
    if payload["status"] == "ready" and os.environ.get("AISTOCK_CI_TEST_TEMP_REQUIRED") == "1":
        try:
            github_env = os.environ.get("GITHUB_ENV", "")
            if not github_env:
                raise ValueError("GITHUB_ENV is missing; cannot propagate CI temporary storage")
            updates = prepare_test_temp(os.environ)
            with Path(github_env).open("a", encoding="utf-8") as handle:
                for key, value in updates.items():
                    handle.write(f"{key}={value}\n")
            payload["test_temp_environment"] = updates
        except (OSError, ValueError) as exc:
            payload["status"] = "environment_mismatch"
            payload["failure_reasons"].append(f"X-only CI temporary storage unavailable: {exc}")
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
