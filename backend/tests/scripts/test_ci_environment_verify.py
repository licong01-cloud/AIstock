from __future__ import annotations

import hashlib
from pathlib import Path

from scripts.ci_environment_verify import verify_environment


def _env(tmp_path: Path, **overrides: str) -> dict[str, str]:
    values = {
        "AISTOCK_CI_ENV_NAME": "AIstock-CI",
        "CONDA_DEFAULT_ENV": "AIstock-CI",
        "AISTOCK_CI_ENV_ROOT": str(tmp_path),
        "AISTOCK_CI_EXPECTED_FINGERPRINT": "fp-v1",
        "AISTOCK_CI_ENV_FINGERPRINT": "fp-v1",
    }
    values.update(overrides)
    return values


def test_prebuilt_windows_environment_is_ready_without_installing(tmp_path: Path) -> None:
    payload = verify_environment(_env(tmp_path), system="Windows", prefix=str(tmp_path), required_modules=())

    assert payload["status"] == "ready"
    assert payload["environment_name"] == "AIstock-CI"
    assert payload["missing_modules"] == []


def test_environment_fingerprint_mismatch_fails_closed(tmp_path: Path) -> None:
    payload = verify_environment(
        _env(tmp_path, AISTOCK_CI_ENV_FINGERPRINT="fp-old"),
        system="Windows",
        prefix=str(tmp_path),
        required_modules=(),
    )

    assert payload["status"] == "environment_mismatch"
    assert "environment fingerprint mismatch" in payload["failure_reasons"]


def test_non_windows_environment_fails_closed_without_fallback(tmp_path: Path) -> None:
    payload = verify_environment(_env(tmp_path), system="Linux", prefix=str(tmp_path), required_modules=())

    assert payload["status"] == "environment_mismatch"
    assert any("expected Windows" in reason for reason in payload["failure_reasons"])


def test_named_environment_cannot_mask_a_production_prefix(tmp_path: Path) -> None:
    production_prefix = tmp_path / "AIstock"
    production_prefix.mkdir()
    ci_root = tmp_path / "AIstock-CI"
    ci_root.mkdir()
    payload = verify_environment(
        _env(ci_root), system="Windows", prefix=str(production_prefix), required_modules=()
    )

    assert payload["status"] == "environment_mismatch"
    assert "python prefix is outside AISTOCK_CI_ENV_ROOT" in payload["failure_reasons"]


def test_required_codeql_bundle_is_hash_verified_without_installing(tmp_path: Path) -> None:
    bundle = tmp_path / "codeql-bundle-win64-2.26.3.tar.gz"
    bundle.write_bytes(b"prebuilt-codeql-bundle")
    expected = hashlib.sha256(bundle.read_bytes()).hexdigest()
    payload = verify_environment(
        _env(
            tmp_path,
            AISTOCK_CI_CODEQL_BUNDLE_REQUIRED="1",
            AISTOCK_CI_CODEQL_BUNDLE_PATH=str(bundle),
            AISTOCK_CI_CODEQL_BUNDLE_SHA256=expected,
            AISTOCK_CI_CODEQL_BUNDLE_VERSION="2.26.3",
        ),
        system="Windows",
        prefix=str(tmp_path),
        required_modules=(),
    )

    assert payload["status"] == "ready"
    assert payload["codeql_bundle_present"] is True
    assert payload["codeql_bundle_sha256_match"] is True


def test_required_codeql_bundle_fails_closed_on_hash_drift(tmp_path: Path) -> None:
    bundle = tmp_path / "codeql-bundle-win64-2.26.3.tar.gz"
    bundle.write_bytes(b"drifted")
    payload = verify_environment(
        _env(
            tmp_path,
            AISTOCK_CI_CODEQL_BUNDLE_REQUIRED="1",
            AISTOCK_CI_CODEQL_BUNDLE_PATH=str(bundle),
            AISTOCK_CI_CODEQL_BUNDLE_SHA256="0" * 64,
            AISTOCK_CI_CODEQL_BUNDLE_VERSION="2.26.3",
        ),
        system="Windows",
        prefix=str(tmp_path),
        required_modules=(),
    )

    assert payload["status"] == "environment_mismatch"
    assert "prebuilt CodeQL bundle SHA-256 mismatch" in payload["failure_reasons"]
