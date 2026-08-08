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
    db_credential_scrub_command,
    is_db_credential_key,
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
]

PRESERVED_VARS = {
    "AISTOCK_PREDICTION_STORE_BASE_URL": "http://prediction-store:9000",
    "QLIB_DATA_PATH": "C:/qlib_data",
    "FACTOR_CACHE_DIR": "C:/factor_cache",
    "AISTOCK_TASK_ID": "task-1",
}


@pytest.fixture()
def _poison_db_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in DB_VARS:
        monkeypatch.setenv(name, f"fake-{name.lower()}")
    for name, value in PRESERVED_VARS.items():
        monkeypatch.setenv(name, value)


def test_scrubbed_qe_subprocess_env_removes_db_credentials_only(_poison_db_env: None) -> None:
    env = scrubbed_qe_subprocess_env()
    for name in DB_VARS:
        assert name not in env, f"DB credential variable leaked: {name}"
    for name, value in PRESERVED_VARS.items():
        assert env.get(name) == value, f"control-plane/file-path variable lost: {name}"


def test_is_db_credential_key() -> None:
    assert is_db_credential_key("TDX_DB_HOST")
    assert is_db_credential_key("TDX_DB_PASSWORD")
    assert is_db_credential_key("DATABASE_URL")
    assert is_db_credential_key("PGHOST")
    assert is_db_credential_key("PGPASSWORD")
    assert not is_db_credential_key("AISTOCK_PREDICTION_STORE_BASE_URL")
    assert not is_db_credential_key("QLIB_DATA_PATH")
    assert not is_db_credential_key("AISTOCK_TASK_ID")


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
        [sys.executable, "-c", probe, *DB_VARS],
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
    _qrun_command, _read_command, read_env = combine_backtest_module._default_local_pred_backtest_commands(
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
    credential variables before qrun/read_exp_res run inside bash -lc."""
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
    assert "unset" in command
    for prefix in DB_CREDENTIAL_PREFIXES:
        assert f"${{{prefix}*}}" in command or prefix in command
    for name in sorted(DB_CREDENTIAL_KEYS):
        assert name in command
    assert "python qrun_limit_minute.py conf.yaml --pred-backtest combined_prediction.pkl" in command
    assert "QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py" in command


def test_db_credential_scrub_command_runs_in_bash(_poison_db_env: None) -> None:
    """The generated bash fragment actually unsets the credential variables when
    executed inside a real shell."""
    if sys.platform == "win32" and not _bash_available():
        pytest.skip("bash is unavailable on this host")
    probe = (
        "export TDX_DB_HOST=fake PGHOST=fake DATABASE_URL=postgresql://fake; "
        + db_credential_scrub_command()
        + 'test -z "${TDX_DB_HOST+x}" && test -z "${PGHOST+x}" && test -z "${DATABASE_URL+x}" && echo SCRUBBED'
    )
    child = subprocess.run(
        ["bash", "-c", probe],
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 0, child.stderr
    assert "SCRUBBED" in child.stdout


def _bash_available() -> bool:
    try:
        subprocess.run(["bash", "-c", "true"], capture_output=True, check=False, timeout=10)
        return True
    except (OSError, subprocess.TimeoutExpired):
        return False


def test_qe_subprocess_env_never_emits_values(_poison_db_env: None) -> None:
    """The scrub helpers must not leak credential values in their outputs."""
    env = scrubbed_qe_subprocess_env()
    serialized = repr(env)
    for value in ("fake-tdx_db_host", "fake-pgpassword", "fake-tdx_db_password"):
        assert value not in serialized
    scrub = db_credential_scrub_command()
    for value in ("fake-tdx_db_host", "fake-pgpassword", "fake-tdx_db_password"):
        assert value not in scrub


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
