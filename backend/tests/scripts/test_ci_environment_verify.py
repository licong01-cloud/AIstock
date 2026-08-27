from __future__ import annotations

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
