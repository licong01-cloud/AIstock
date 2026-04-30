import pytest

from backend.routers import quantevolver as qe_router


def test_collect_factor_cache_wsl_db_env_maps_tdx_and_pg_aliases() -> None:
    source_env = {
        "TDX_DB_HOST": "172.20.1.1",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "aistock",
        "TDX_DB_USER": "postgres",
        "TDX_DB_PASSWORD": "dummy-secret",
        "AISTOCK_PG_STATEMENT_TIMEOUT_MS": "30000",
    }

    env = qe_router._collect_factor_cache_wsl_db_env(source_env)

    assert env["TDX_DB_PASSWORD"] == "dummy-secret"
    assert env["PGHOST"] == "172.20.1.1"
    assert env["PGPORT"] == "5432"
    assert env["PGDATABASE"] == "aistock"
    assert env["PGUSER"] == "postgres"
    assert env["PGPASSWORD"] == "dummy-secret"
    assert env["AISTOCK_PG_STATEMENT_TIMEOUT_MS"] == "30000"


def test_collect_factor_cache_wsl_db_env_fails_fast_when_password_missing() -> None:
    source_env = {
        "TDX_DB_HOST": "172.20.1.1",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "aistock",
        "TDX_DB_USER": "postgres",
    }

    with pytest.raises(RuntimeError, match="TDX_DB_PASSWORD"):
        qe_router._collect_factor_cache_wsl_db_env(source_env)


def test_build_factor_cache_wsl_process_env_uses_wslenv_without_cli_secret() -> None:
    base_env = {"PATH": "/usr/bin", "WSLENV": "EXISTING/u:TDX_DB_USER/w"}
    db_env = {
        "TDX_DB_USER": "postgres",
        "TDX_DB_PASSWORD": "dummy-secret",
        "PGPASSWORD": "dummy-secret",
    }

    proc_env = qe_router._build_factor_cache_wsl_process_env(base_env, db_env)

    assert proc_env["TDX_DB_PASSWORD"] == "dummy-secret"
    assert proc_env["PGPASSWORD"] == "dummy-secret"
    wslenv_parts = proc_env["WSLENV"].split(":")
    assert "EXISTING/u" in wslenv_parts
    assert "TDX_DB_USER/wu" in wslenv_parts
    assert "TDX_DB_PASSWORD/u" in wslenv_parts
    assert "PGPASSWORD/u" in wslenv_parts
    assert base_env["WSLENV"] == "EXISTING/u:TDX_DB_USER/w"


def test_build_factor_cache_wsl_shell_command_quotes_paths_and_omits_secret_values() -> None:
    command = qe_router._build_factor_cache_wsl_shell_command(
        project_root_wsl="/mnt/f/Dev/AI stock",
        backfill_args=[
            "/mnt/f/Dev/AI stock/scripts/backfill_factor_cache.py",
            "--factor-data-dir",
            "/data/factor dir",
            "--task-id",
            "cache_1",
        ],
        log_path_wsl="/tmp/cache log.log",
    )

    assert "'/mnt/f/Dev/AI stock'" in command
    assert "'/data/factor dir'" in command
    assert "> '/tmp/cache log.log' 2>&1" in command
    assert "TDX_DB_PASSWORD" in command
    assert "TDX_DB_PASSWORD=" not in command
    assert "dummy-secret" not in command
