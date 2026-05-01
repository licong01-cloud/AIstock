import json

import pytest

from backend.routers import quantevolver as qe_router
from backend.services.quantevolver.config_composer import (
    ConfigComposer,
    QE_DEFAULT_SIGNAL_END,
    RDAGENT_DEFAULT_DATA_SPLIT,
)
from scripts import backfill_factor_cache


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


def test_factor_cache_covers_recorded_window_even_with_long_warmup(tmp_path) -> None:
    qe_router._invalidate_cache_meta()
    specs = _cache_source_specs(tmp_path)
    backtest_root = tmp_path / "factor_values"
    _write_cache_meta(
        backtest_root,
        {
            "LongWarmupFactor": {
                "status": "ok",
                "computed_at": "2026-05-01T08:21:46",
                "date_range": "2018-12-31~2026-04-28",
                "as_of_date": "2026-04-28",
                "window_train_start": "2018-08-01",
                "window_backtest_end": "2026-04-28",
                "rows": 100,
            }
        },
    )
    (backtest_root / "single" / "LongWarmupFactor.parquet").write_bytes(b"PAR1")

    selected = qe_router._choose_best_factor_cache_candidate(
        qe_router._collect_factor_cache_candidates("LongWarmupFactor", source_specs=specs),
        "2018-08-01",
        "2026-04-28",
    )

    assert selected is not None
    assert qe_router._factor_cache_candidate_covers(selected, "2018-08-01", "2026-04-28")


def test_factor_cache_backfill_skips_recorded_warmup_window(monkeypatch, tmp_path) -> None:
    single_dir = tmp_path / "single"
    single_dir.mkdir()
    monkeypatch.setattr(backfill_factor_cache, "SINGLE_DIR", single_dir)
    (single_dir / "LongWarmupFactor.parquet").write_bytes(b"PAR1")

    plan = backfill_factor_cache.plan_factor_action(
        "LongWarmupFactor",
        "2018-08-01",
        "2026-04-28",
        {
            "factors": {
                "LongWarmupFactor": {
                    "date_range": "2018-12-31~2026-04-28",
                    "window_train_start": "2018-08-01",
                    "window_backtest_end": "2026-04-28",
                }
            }
        },
        incremental=True,
        force=False,
    )

    assert plan["action"] == "skip"
    assert plan["reason"] == "covered_warmup_window"


def test_factor_cache_backfill_extends_forward_after_warmup(monkeypatch, tmp_path) -> None:
    single_dir = tmp_path / "single"
    single_dir.mkdir()
    monkeypatch.setattr(backfill_factor_cache, "SINGLE_DIR", single_dir)
    (single_dir / "LongWarmupFactor.parquet").write_bytes(b"PAR1")

    plan = backfill_factor_cache.plan_factor_action(
        "LongWarmupFactor",
        "2018-08-01",
        "2026-04-28",
        {
            "factors": {
                "LongWarmupFactor": {
                    "date_range": "2018-12-31~2026-04-10",
                    "window_train_start": "2018-08-01",
                    "window_backtest_end": "2026-04-10",
                }
            }
        },
        incremental=True,
        force=False,
    )

    assert plan["action"] == "extend_forward"


def test_qe_prepare_factors_default_window_uses_current_signal_end_and_records_cache_window() -> None:
    code = (
        "def calculate_DemoFactor(instruments, start_date, end_date):\n"
        "    import pandas as pd\n"
        "    return pd.DataFrame(index=pd.MultiIndex.from_arrays([[], []], names=['datetime', 'instrument']), columns=['DemoFactor'])\n"
    )
    script = ConfigComposer()._compose_prepare_factors(
        [{"factor_name": "DemoFactor", "code_text": code}],
        factor_data_dir="/tmp/factor_data",
        data_split=dict(RDAGENT_DEFAULT_DATA_SPLIT),
    )

    assert script is not None
    assert f"TEST_END = '{QE_DEFAULT_SIGNAL_END}'" in script
    assert "TEST_END = '2026-03-10'" not in script
    assert "'window_train_start': TRAIN_START" in script
    assert "'window_backtest_end': TEST_END" in script
    assert "entry.get('window_train_start')" in script
    assert "open(FACTOR_CACHE_META, 'r', encoding='utf-8')" in script
    assert "os.makedirs(FACTOR_CACHE_SINGLE_DIR, exist_ok=True)" in script
    assert "os.fdopen(tmp_fd, 'w', encoding='utf-8')" in script


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
