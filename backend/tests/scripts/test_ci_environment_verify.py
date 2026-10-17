from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import ci_environment_verify as ci_env
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


def _job_env(**overrides: str) -> dict[str, str]:
    return {
        "GITHUB_REPOSITORY": "licong01-cloud/AIstock",
        "GITHUB_RUN_ID": "36782788857",
        "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_JOB": "ci-verdict",
        **overrides,
    }


def test_job_temp_environment_reaches_fresh_python_and_pytest(tmp_path, monkeypatch):
    monkeypatch.setattr(ci_env, "TEST_TEMP_ROOT", tmp_path / "ci-temp")
    updates = ci_env.prepare_test_temp(_job_env())
    assert updates["TEMP"] == updates["TMP"] == updates["TMPDIR"]
    test = tmp_path / "test_child_temp.py"
    test.write_text(
        "import os, tempfile\nfrom pathlib import Path\n"
        "def test_temp(tmp_path):\n"
        "    assert Path(tempfile.gettempdir()) == Path(os.environ['TEMP'])\n"
        "    assert tmp_path.is_relative_to(Path(os.environ['PYTEST_DEBUG_TEMPROOT'])) and tmp_path.is_relative_to(Path(tempfile.gettempdir()))\n",
        encoding="utf-8",
    )
    env = {**os.environ, **updates, "PYTEST_ADDOPTS": ""}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", str(test), "-q", "-p", "no:cacheprovider"],
        env=env, capture_output=True, text=True, timeout=40,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_public_ci_prepares_x_only_temp_before_any_tests():
    import yaml

    assert ci_env.TEST_TEMP_ROOT.drive.upper() == "X:"
    steps = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))["jobs"]["ci-verdict"]["steps"]
    index, prep = next((i, step) for i, step in enumerate(steps) if step.get("name") == "Verify prebuilt AIstock-CI environment")
    assert prep["env"]["AISTOCK_CI_TEST_TEMP_REQUIRED"] == "1" and not prep.get("continue-on-error")
    assert prep["run"] == "python scripts/ci_environment_verify.py"
    assert index < next(i for i, step in enumerate(steps) if step.get("id") == "l0_validation")


@pytest.mark.parametrize("job,execution", [
    ("nightly-l3", "Run selected Nightly sessions once"),
    ("paper-v2-live", "nox -s paper_v2_live"),
])
def test_nightly_prepares_same_x_only_job_storage_before_tests(job, execution):
    import yaml

    steps = yaml.safe_load(Path(".github/workflows/nightly.yml").read_text(encoding="utf-8"))["jobs"][job]["steps"]
    index, prep = next((i, step) for i, step in enumerate(steps)
                       if step.get("env", {}).get("AISTOCK_CI_TEST_TEMP_REQUIRED") == "1")
    assert prep["env"]["AISTOCK_CI_ENV_NAME"] == "AIstock-CI"
    assert not prep.get("continue-on-error")
    assert "conda run -n AIstock-CI python scripts/ci_environment_verify.py" in prep["run"]
    assert "if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }" in prep["run"]
    execution_index = next(i for i, step in enumerate(steps) if step.get("name") == execution)
    assert index < execution_index
    if job == "paper-v2-live":
        assert "conda run -n AIstock-CI python -m nox" in steps[execution_index]["run"]


@pytest.mark.parametrize("key,value", [("GITHUB_RUN_ID", "2"), ("GITHUB_RUN_ATTEMPT", "2"),
                                      ("GITHUB_JOB", "other-job"), ("GITHUB_REPOSITORY", "other/repo")])
def test_job_temp_isolation(tmp_path, monkeypatch, key, value):
    monkeypatch.setattr(ci_env, "TEST_TEMP_ROOT", tmp_path)
    assert ci_env.prepare_test_temp(_job_env())["TEMP"] != ci_env.prepare_test_temp(_job_env(**{key: value}))["TEMP"]


@pytest.mark.parametrize("value", ["", "../escape", "job\nTEMP=C:/tmp", "a" * 161])
def test_job_temp_rejects_unbound_or_escaping_identity(tmp_path, monkeypatch, value):
    monkeypatch.setattr(ci_env, "TEST_TEMP_ROOT", tmp_path)
    with pytest.raises(ValueError, match="identity"):
        ci_env.prepare_test_temp(_job_env(GITHUB_JOB=value))


def test_job_temp_unavailable_or_redirected_storage_never_falls_back(tmp_path, monkeypatch):
    monkeypatch.setattr(ci_env, "TEST_TEMP_ROOT", tmp_path / "ci-temp")
    resolve = Path.resolve

    def redirected(path, **kwargs):
        if path == ci_env.TEST_TEMP_ROOT:
            return Path("Z:/elsewhere")
        return resolve(path, **kwargs)

    monkeypatch.setattr(Path, "resolve", redirected)
    with pytest.raises(ValueError, match="escaped"):
        ci_env.prepare_test_temp(_job_env())
    monkeypatch.setattr(Path, "resolve", resolve)
    monkeypatch.setattr(ci_env.tempfile, "TemporaryFile", lambda **_: (_ for _ in ()).throw(PermissionError("unwritable")))
    with pytest.raises(PermissionError, match="unwritable"):
        ci_env.prepare_test_temp(_job_env())


@pytest.mark.parametrize("missing_drive", [True, False])
def test_job_temp_missing_drive_and_cross_job_redirect_fail(tmp_path, monkeypatch, missing_drive):
    monkeypatch.setattr(ci_env, "TEST_TEMP_ROOT", tmp_path / "ci-temp")
    resolve = Path.resolve
    def checked_resolve(path, **kwargs):
        if missing_drive and path == Path(ci_env.TEST_TEMP_ROOT.anchor):
            raise FileNotFoundError("missing drive")
        return tmp_path if not missing_drive and path.name == "tmp" else resolve(path, **kwargs)
    monkeypatch.setattr(Path, "resolve", checked_resolve)
    with pytest.raises((FileNotFoundError, ValueError)):
        ci_env.prepare_test_temp(_job_env())


def test_main_publishes_temp_only_after_successful_preflight(tmp_path, monkeypatch):
    monkeypatch.setattr(ci_env, "TEST_TEMP_ROOT", tmp_path / "ci-temp")
    monkeypatch.setattr(ci_env, "verify_environment", lambda: {"status": "ready", "failure_reasons": []})
    for key, value in _job_env().items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("AISTOCK_CI_TEST_TEMP_REQUIRED", "1")
    output = tmp_path / "github-env"
    monkeypatch.setenv("GITHUB_ENV", str(output))
    assert ci_env.main() == 0
    assert "PYTEST_DEBUG_TEMPROOT=" in output.read_text(encoding="utf-8")
    prior = output.read_bytes()
    monkeypatch.setenv("GITHUB_JOB", "../escape")
    assert ci_env.main() == 1
    assert output.read_bytes() == prior


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


def test_preexpanded_codeql_toolcache_identity_is_ready(tmp_path: Path) -> None:
    codeql = tmp_path / "CodeQL" / "2.26.3" / "x64" / "codeql"
    codeql.mkdir(parents=True)
    manifest = codeql / ".codeqlmanifest.json"
    manifest.write_text('{"version":"2.26.3"}', encoding="utf-8")
    (codeql / "codeql.cmd").write_text("@echo off", encoding="utf-8")
    codeql.parent.with_name("x64.complete").touch()
    expected = hashlib.sha256(manifest.read_bytes()).hexdigest()

    payload = verify_environment(
        _env(
            tmp_path,
            AISTOCK_CI_CODEQL_BUNDLE_REQUIRED="1",
            AISTOCK_CI_CODEQL_BUNDLE_PATH=str(codeql),
            AISTOCK_CI_CODEQL_BUNDLE_SHA256=expected,
            AISTOCK_CI_CODEQL_BUNDLE_VERSION="2.26.3",
        ),
        system="Windows",
        prefix=str(tmp_path),
        required_modules=(),
    )

    assert payload["status"] == "ready"
    assert payload["codeql_bundle_kind"] == "toolcache"
