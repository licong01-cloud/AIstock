import json

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


def _write_cache_meta(root, factors) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "single").mkdir(parents=True, exist_ok=True)
    (root / "_meta.json").write_text(
        json.dumps({"factors": factors}, ensure_ascii=False),
        encoding="utf-8",
    )


def _cache_source_specs(tmp_path):
    backtest_root = tmp_path / "factor_values"
    realtime_root = tmp_path / "factor_values_realtime"
    return (
        {
            "key": "backtest",
            "label": "回测缓存",
            "single_dir": backtest_root / "single",
            "meta_path": backtest_root / "_meta.json",
        },
        {
            "key": "realtime_snapshot",
            "label": "官方快照",
            "single_dir": realtime_root / "single",
            "meta_path": realtime_root / "_meta.json",
        },
    )


def test_factor_cache_prefers_realtime_valid_cache_over_backtest_error(tmp_path) -> None:
    qe_router._invalidate_cache_meta()
    specs = _cache_source_specs(tmp_path)
    backtest_root = tmp_path / "factor_values"
    realtime_root = tmp_path / "factor_values_realtime"
    _write_cache_meta(
        backtest_root,
        {
            "RESI5": {
                "status": "error",
                "computed_at": "2026-05-01T00:22:26",
                "error": "factor.py did not produce output",
            }
        },
    )
    _write_cache_meta(
        realtime_root,
        {
            "RESI5": {
                "status": "ok",
                "computed_at": "2026-05-01T08:21:46",
                "date_range": "2018-08-07~2026-04-10",
                "as_of_date": "2026-04-10",
                "data_source_mode": "snapshot",
                "rows": 7247352,
            }
        },
    )
    (realtime_root / "single" / "RESI5.parquet").write_bytes(b"PAR1")

    selected = qe_router._choose_best_factor_cache_candidate(
        qe_router._collect_factor_cache_candidates("RESI5", source_specs=specs),
        "2018-08-01",
        "2026-04-10",
    )

    assert selected is not None
    assert selected["valid_cache"] is True
    assert selected["source_key"] == "realtime_snapshot"
    assert selected["cache_start_date"] == "2018-08-07"
    assert selected["cache_end_date"] == "2026-04-10"
    assert selected["cache_status"] == "ok"


def test_factor_cache_uses_error_only_when_no_valid_cache_exists(tmp_path) -> None:
    qe_router._invalidate_cache_meta()
    specs = _cache_source_specs(tmp_path)
    backtest_root = tmp_path / "factor_values"
    realtime_root = tmp_path / "factor_values_realtime"
    _write_cache_meta(
        backtest_root,
        {
            "KLEN": {
                "status": "error",
                "computed_at": "2026-05-01T00:22:26",
                "error": "runtime failed",
            }
        },
    )
    _write_cache_meta(realtime_root, {})

    selected = qe_router._choose_best_factor_cache_candidate(
        qe_router._collect_factor_cache_candidates("KLEN", source_specs=specs)
    )

    assert selected is not None
    assert selected["valid_cache"] is False
    assert selected["cache_status"] == "error"
    assert selected["source_key"] == "backtest"
