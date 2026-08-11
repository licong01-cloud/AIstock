from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from backend.services.multi_alpha import combine_backtest as combine_backtest_module
from backend.services.multi_alpha import remote_dispatch as remote_dispatch_module
from backend.services.multi_alpha.qe_subprocess_env import (
    DB_CREDENTIAL_KEYS,
    DB_CREDENTIAL_PREFIXES,
    QE_SECRET_CREDENTIAL_KEYS,
    db_credential_scrub_command,
    is_db_credential_key,
    is_qe_subprocess_credential_key,
    qe_subprocess_credential_scrub_command,
    scrubbed_qe_subprocess_env,
)

DB_VARS = [
    "TDX_DB_HOST",
    "TDX_DB_PORT",
    "TDX_DB_USER",
    "TDX_DB_PASSWORD",
    "TDX_DB_NAME",
    "DATABASE_URL",
    "PGHOST",
    "PGPORT",
    "PGUSER",
    "PGPASSWORD",
    "PGDATABASE",
    "PGSERVICE",
    "PGPASSFILE",
    "PGHOSTADDR",
    "PGSERVICEFILE",
    "PGSSLKEY",
    "PGSSLCERT",
    "PGOPTIONS",
    "POSTGRES_PASSWORD",
    "DB_PASSWORD",
    "SQLALCHEMY_DATABASE_URL",
]
SECRET_VARS = [
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GITHUB_TOKEN",
    "HF_TOKEN",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AZURE_CLIENT_SECRET",
    "SERVICE_ACCESS_KEY_ID",
    "QE_RESOURCE_SESSION_TOKEN",
]

PRESERVED_VARS = {
    "AISTOCK_PREDICTION_STORE_BASE_URL": "http://prediction-store:9000",
    "QLIB_DATA_PATH": "C:/qlib_data",
    "FACTOR_CACHE_DIR": "C:/factor_cache",
    "AISTOCK_TASK_ID": "task-1",
    "QE_RESOURCE_SESSION_ID": "qers-1",
    "QE_RESOURCE_SOURCE_RUN_KEY": "task-1_L1",
}


@pytest.fixture()
def _poison_db_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in DB_VARS:
        monkeypatch.setenv(name, f"fake-{name.lower()}")
    for name in SECRET_VARS:
        monkeypatch.setenv(name, f"fake-{name.lower()}")
    for name, value in PRESERVED_VARS.items():
        monkeypatch.setenv(name, value)


def test_scrubbed_qe_subprocess_env_removes_credentials_and_preserves_control_plane(
    _poison_db_env: None,
) -> None:
    env = scrubbed_qe_subprocess_env()
    for name in [*DB_VARS, *SECRET_VARS]:
        assert name not in env, f"credential variable leaked: {name}"
    for name, value in PRESERVED_VARS.items():
        assert env.get(name) == value, f"control-plane/file-path variable lost: {name}"


def test_is_db_credential_key() -> None:
    assert is_db_credential_key("TDX_DB_HOST")
    assert is_db_credential_key("TDX_DB_PASSWORD")
    assert is_db_credential_key("DATABASE_URL")
    assert is_db_credential_key("PGHOST")
    assert is_db_credential_key("PGPASSWORD")
    assert is_db_credential_key("PGHOSTADDR")
    assert is_db_credential_key("PGSERVICEFILE")
    assert is_db_credential_key("PGSSLKEY")
    assert is_db_credential_key("pgsslcert")
    assert is_db_credential_key("pgoptions")
    assert is_db_credential_key("POSTGRES_PASSWORD")
    assert is_db_credential_key("DB_PASSWORD")
    assert is_db_credential_key("SQLALCHEMY_DATABASE_URL")
    assert not is_db_credential_key("AISTOCK_PREDICTION_STORE_BASE_URL")
    assert not is_db_credential_key("QLIB_DATA_PATH")
    assert not is_db_credential_key("AISTOCK_TASK_ID")


def test_is_qe_subprocess_credential_key_covers_non_database_secrets() -> None:
    assert is_qe_subprocess_credential_key("OPENAI_API_KEY")
    assert is_qe_subprocess_credential_key("SERVICE_AUTH_TOKEN")
    assert is_qe_subprocess_credential_key("GITHUB_TOKEN")
    assert is_qe_subprocess_credential_key("AWS_ACCESS_KEY_ID")
    assert is_qe_subprocess_credential_key("AZURE_CLIENT_SECRET")
    assert is_qe_subprocess_credential_key("SERVICE_ACCESS_KEY_ID")
    assert is_qe_subprocess_credential_key("BASH_ENV")
    assert is_qe_subprocess_credential_key("BASH_FUNC_evil%%")
    assert is_qe_subprocess_credential_key("LD_PRELOAD")
    assert not is_qe_subprocess_credential_key("AISTOCK_PREDICTION_STORE_BASE_URL")
    assert not is_qe_subprocess_credential_key("QE_RESOURCE_SESSION_ID")
    assert not is_qe_subprocess_credential_key("FACTOR_CACHE_DIR")


def test_launched_isolated_subprocess_sees_no_db_credentials(_poison_db_env: None) -> None:
    """A real child process launched with the scrubbed env must not see any
    PostgreSQL credential variable while still seeing the file/control plane."""
    # sanity: the fixture poisoned the parent environment first
    assert all(name in os.environ for name in DB_VARS)
    probe = (
        "import os, sys; "
        "leaked=[k for k in sys.argv[1:] if k in os.environ]; "
        "print('LEAKED=' + ','.join(leaked)); "
        "print('PRESERVED=' + os.environ.get('AISTOCK_PREDICTION_STORE_BASE_URL',''))"
    )
    child = subprocess.run(
        [sys.executable, "-c", probe, *DB_VARS, *SECRET_VARS],
        capture_output=True,
        text=True,
        env=scrubbed_qe_subprocess_env(),
        check=False,
    )
    assert child.returncode == 0, child.stderr
    assert child.stdout.splitlines()[0] == "LEAKED="
    assert "PRESERVED=http://prediction-store:9000" in child.stdout


def test_isolated_subprocess_still_reads_file_data_and_prediction_store(_poison_db_env: None) -> None:
    """The scrub must not break the file-only data plane or the Prediction Store
    HTTP control plane URL used to record task state/results."""
    workspace = Path(__file__).resolve().parent
    marker = workspace / "_qe_isolation_marker.tmp"
    marker.write_text("frozen-h5", encoding="utf-8")
    try:
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import os, pathlib, sys; "
                    "p=pathlib.Path(sys.argv[1]); "
                    "data=p.read_text(encoding='utf-8'); "
                    "base=os.getenv('AISTOCK_PREDICTION_STORE_BASE_URL',''); "
                    "print('DATA=' + data); print('BASE=' + base)"
                ),
                str(marker),
            ],
            capture_output=True,
            text=True,
            env=scrubbed_qe_subprocess_env(),
            check=False,
        )
        assert child.returncode == 0, child.stderr
        assert "DATA=frozen-h5" in child.stdout
        assert "BASE=http://prediction-store:9000" in child.stdout
    finally:
        marker.unlink(missing_ok=True)


def test_db_connect_poison_fails_with_scrubbed_env(_poison_db_env: None) -> None:
    """With the scrubbed env, attempting to establish a database connection must
    fail (no credentials / no fallback), proving the QE subprocess cannot reach
    market.* or any database."""
    code = (
        "import os\n"
        "host = os.getenv('TDX_DB_HOST')\n"
        "user = os.getenv('PGUSER')\n"
        "url = os.getenv('DATABASE_URL')\n"
        "if host is not None or user is not None or url is not None:\n"
        "    raise SystemExit('credential leaked')\n"
        "print('NO_DB_CREDENTIALS')\n"
    )
    child = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=scrubbed_qe_subprocess_env(),
        check=False,
    )
    assert child.returncode == 0, child.stderr
    assert "NO_DB_CREDENTIALS" in child.stdout


def test_local_qrun_read_env_is_scrubbed_on_windows(_poison_db_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(combine_backtest_module, "_is_windows_host", lambda: True)
    workspace = Path(__file__).resolve().parent
    qrun_command, read_command, read_env = combine_backtest_module._default_local_pred_backtest_commands(
        workspace=workspace,
        pred_name="combined_prediction.pkl",
        backtest_config={
            "wsl_distro": "Ubuntu",
            "wsl_conda_sh": "/home/test/miniconda3/etc/profile.d/conda.sh",
            "wsl_conda_env": "rdagent-gpu",
        },
    )
    assert read_env is not None
    for name in DB_VARS:
        assert name not in read_env
    assert read_env.get("AISTOCK_PREDICTION_STORE_BASE_URL") == "http://prediction-store:9000"
    for command in (qrun_command, read_command):
        assert command[3:7] == ["bash", "--noprofile", "--norc", "-c"]
        script = command[-1]
        assert script.index(". ./.factor_env") < script.index("reason_code=qe_subprocess_bash_required")
        assert "for __qe_credvar in $(compgen -e)" in script


def test_local_qrun_read_env_is_scrubbed_on_posix(_poison_db_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(combine_backtest_module, "_is_windows_host", lambda: False)
    workspace = Path(__file__).resolve().parent
    qrun_command, _read_command, read_env = combine_backtest_module._default_local_pred_backtest_commands(
        workspace=workspace,
        pred_name="combined_prediction.pkl",
        backtest_config={},
    )
    assert read_env is not None
    assert read_env.get("QE_REQUIRE_RECORDER_ID") == "1"
    for name in DB_VARS:
        assert name not in read_env
    assert read_env.get("AISTOCK_PREDICTION_STORE_BASE_URL") == "http://prediction-store:9000"
    assert qrun_command[1] == "qrun_limit_minute.py"


def test_remote_wsl_command_scrubs_db_credentials(_poison_db_env: None) -> None:
    """The remote/WSL QE workspace command construction path unsets the database
    credential variables at the final qrun/read boundaries in a clean Bash."""
    command = remote_dispatch_module._remote_wsl_command(
        workspace=Path(__file__).resolve().parent,
        remote_paths={
            "artifact_path": "/artifacts/combined_factors_df.parquet",
            "prediction_artifact_path": "/artifacts/combined_prediction.pkl",
            "qlib_data_path": "/qlib_data",
            "factor_cache_dir": "/factor_cache",
        },
        backtest_config={"node_id": "wsl2-5080"},
        runtime_artifact_bindings=(),
    )
    assert command.startswith("bash --noprofile --norc -c ")
    assert "unset" in command
    for prefix in DB_CREDENTIAL_PREFIXES:
        assert f"${{{prefix}*}}" in command or prefix in command
    for name in sorted(DB_CREDENTIAL_KEYS):
        assert name in command
    assert "python qrun_limit_minute.py conf.yaml --pred-backtest combined_prediction.pkl" in command
    assert "QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py" in command
    assert command.count("reason_code=qe_subprocess_bash_required") >= 4


def test_db_credential_scrub_command_runs_in_bash(_poison_db_env: None) -> None:
    """The generated bash fragment actually unsets the credential variables when
    executed inside a real shell."""
    if sys.platform == "win32" and not _bash_available():
        pytest.skip("bash is unavailable on this host")
    probe = (
        "export TDX_DB_HOST=fake PGHOST=fake DATABASE_URL=postgresql://fake OPENAI_API_KEY=fake GITHUB_TOKEN=fake; "
        + db_credential_scrub_command()
        + '; test -z "${TDX_DB_HOST+x}" && test -z "${PGHOST+x}" '
        + '&& test -z "${DATABASE_URL+x}" && test -z "${OPENAI_API_KEY+x}" '
        + '&& test -z "${GITHUB_TOKEN+x}" && echo SCRUBBED'
    )
    child = subprocess.run(
        [*_shell_command("bash"), "--noprofile", "--norc", "-c", probe],
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 0, child.stderr
    assert "SCRUBBED" in child.stdout


def test_db_credential_scrub_command_is_errexit_safe_with_bash_readonly_vars() -> None:
    """Bash creates readonly BASHOPTS/SHELLOPTS itself; defense-in-depth
    scrubbing must not try to unset them and abort the qrun command chain."""

    if not _bash_available():
        pytest.skip("bash is unavailable on this host")
    probe = "set -e; " + db_credential_scrub_command() + "; echo QRUN_CAN_START"
    child = subprocess.run(
        [*_shell_command("bash"), "--noprofile", "--norc", "-c", probe],
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 0, child.stderr
    assert child.stdout.strip() == "QRUN_CAN_START"


def test_db_credential_scrub_command_removes_mixed_case_exports() -> None:
    """Shell-side defense matches Python's case-insensitive classifier."""

    if not _bash_available():
        pytest.skip("bash is unavailable on this host")
    probe = (
        "export tdx_db_password=fake pgsslkey=fake PgServiceFile=fake OpenAI_Api_Key=fake ld_preload=fake; "
        + db_credential_scrub_command()
        + '; test -z "${tdx_db_password+x}"; '
        + 'test -z "${pgsslkey+x}"; test -z "${PgServiceFile+x}"; '
        + 'test -z "${OpenAI_Api_Key+x}"; test -z "${ld_preload+x}"; '
        + "echo MIXED_CASE_SCRUBBED"
    )
    child = subprocess.run(
        [*_shell_command("bash"), "--noprofile", "--norc", "-c", probe],
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 0, child.stderr
    assert child.stdout.strip() == "MIXED_CASE_SCRUBBED"


def test_db_credential_scrub_command_fails_closed_under_posix_sh(_poison_db_env: None) -> None:
    """The real RD-Agent legacy /bin/sh boundary must stop, never skip scrub."""

    if not _shell_available("sh"):
        pytest.skip("sh is unavailable on this host")
    probe = (
        "export TDX_DB_PASSWORD=fake OPENAI_API_KEY=fake; "
        + db_credential_scrub_command()
        + "; echo SHOULD_NOT_RUN"
    )
    child = subprocess.run(
        [*_shell_command("sh"), "-c", probe],
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 70
    assert "reason_code=qe_subprocess_bash_required" in child.stderr
    assert "SHOULD_NOT_RUN" not in child.stdout


def test_db_credential_scrub_command_fails_closed_when_compgen_is_unusable() -> None:
    if not _bash_available():
        pytest.skip("bash is unavailable on this host")
    probe = (
        "compgen() { return 127; }; set -e; "
        + db_credential_scrub_command()
        + "; echo SHOULD_NOT_RUN"
    )
    child = subprocess.run(
        [*_shell_command("bash"), "--noprofile", "--norc", "-c", probe],
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 70
    assert "reason_code=qe_subprocess_bash_compgen_missing" in child.stderr
    assert "SHOULD_NOT_RUN" not in child.stdout


def test_final_bash_scrub_removes_credentials_reinjected_by_sourced_env(tmp_path: Path) -> None:
    if not _bash_available():
        pytest.skip("bash is unavailable on this host")
    factor_env = tmp_path / ".factor_env"
    factor_env.write_text(
        "export TDX_DB_PASSWORD=factor-poison\nexport OPENAI_API_KEY=factor-secret\n",
        encoding="utf-8",
    )
    script = (
        "export TDX_DB_PASSWORD=parent-poison OPENAI_API_KEY=parent-secret; "
        "export TDX_DB_PASSWORD=conda-poison OPENAI_API_KEY=conda-secret; "
        f". {_shell_path(factor_env)!r}; "
        + db_credential_scrub_command()
        + "; test -z \"${TDX_DB_PASSWORD+x}\"; "
        "test -z \"${OPENAI_API_KEY+x}\"; echo FINAL_SCRUB_OK"
    )
    child = subprocess.run(
        [*_shell_command("bash"), "--noprofile", "--norc", "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 0, child.stderr
    assert child.stdout.strip() == "FINAL_SCRUB_OK"


def _bash_available() -> bool:
    try:
        subprocess.run(
            [*_shell_command("bash"), "--noprofile", "--norc", "-c", "true"],
            capture_output=True,
            check=False,
            timeout=10,
        )
        return True
    except (OSError, subprocess.TimeoutExpired):
        return False


def _shell_available(name: str) -> bool:
    try:
        subprocess.run([*_shell_command(name), "-c", "true"], capture_output=True, check=False, timeout=10)
        return True
    except (OSError, subprocess.TimeoutExpired):
        return False


def _shell_command(name: str) -> list[str]:
    if sys.platform == "win32":
        return ["wsl.exe", "--exec", name]
    return [name]


def _shell_path(path: Path) -> str:
    resolved = path.resolve().as_posix()
    if sys.platform == "win32" and len(resolved) >= 3 and resolved[1:3] == ":/":
        return f"/mnt/{resolved[0].lower()}{resolved[2:]}"
    return resolved


def test_qe_subprocess_env_never_emits_values(_poison_db_env: None) -> None:
    """The scrub helpers must not leak credential values in their outputs."""
    env = scrubbed_qe_subprocess_env()
    serialized = repr(env)
    for value in (
        "fake-tdx_db_host",
        "fake-pgpassword",
        "fake-tdx_db_password",
        "fake-openai_api_key",
        "fake-github_token",
    ):
        assert value not in serialized
    scrub = db_credential_scrub_command()
    for value in (
        "fake-tdx_db_host",
        "fake-pgpassword",
        "fake-tdx_db_password",
        "fake-openai_api_key",
        "fake-github_token",
    ):
        assert value not in scrub
    assert qe_subprocess_credential_scrub_command() == scrub
    for name in QE_SECRET_CREDENTIAL_KEYS:
        assert name in scrub


def test_market_database_access_impossible_without_credentials(_poison_db_env: None) -> None:
    """Without credentials the QE subprocess cannot open any PostgreSQL cursor,
    so it cannot access market.* tables. This is the fail-closed isolation."""
    code = (
        "import os\n"
        "def _has_cred():\n"
        "    for k in os.environ:\n"
        "        if k.startswith(('TDX_DB_',)) or k in ('DATABASE_URL','PGHOST','PGPORT','PGUSER','PGPASSWORD','PGDATABASE','PGSERVICE','PGPASSFILE'):\n"
        "            return True\n"
        "    return False\n"
        "print('FAIL_CLOSED' if not _has_cred() else 'LEAK')\n"
    )
    child = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=scrubbed_qe_subprocess_env(),
        check=False,
    )
    assert child.returncode == 0, child.stderr
    assert "FAIL_CLOSED" in child.stdout
