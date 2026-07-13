import importlib

import pytest

from backend.config_manager_compat import ConfigManager
from backend.db.migrations import create_dispatch_tables


ENV_KEYS = (
    "AISTOCK_QE_CALLBACK_BASE_URL",
    "AISTOCK_BACKEND_CALLBACK_BASE_URL",
    "AISTOCK_BACKEND_BASE_URL",
    "QLIB_RDAGENT_ROOT_WSL",
    "QE_WORKSPACE_WSL",
    "RDAGENT_FACTOR_DATA_WSL",
    "QLIB_DATA_PATH_WSL",
    "QLIB_DAY_DATA",
    "QLIB_MINUTE_PATH_WSL",
    "QLIB_MINUTE_DATA",
    "RDAGENT_LOG_ROOT",
    "QLIB_RDAGENT_LOG_ROOT",
    "AISTOCK_ENV_FILE",
)


@pytest.fixture(autouse=True)
def clean_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_compute_node_seed_uses_env_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AISTOCK_QE_CALLBACK_BASE_URL", "http://127.0.0.1:8011/")
    monkeypatch.setenv("QLIB_RDAGENT_ROOT_WSL", "/opt/rdagent")
    monkeypatch.setenv("QE_WORKSPACE_WSL", "/data/qe_workspace")
    monkeypatch.setenv("RDAGENT_FACTOR_DATA_WSL", "/data/factors")
    monkeypatch.setenv("QLIB_DATA_PATH_WSL", "/data/qlib_day")
    monkeypatch.setenv("QLIB_MINUTE_PATH_WSL", "/data/qlib_minute")

    rows = create_dispatch_tables.build_compute_node_seed_rows()
    local = rows[0]

    assert local[0] == "wsl2-5080"
    assert local[7] == "/data/qe_workspace"
    assert local[8] == "/data/factors"
    assert local[9] == "/data/qlib_day"
    assert local[10] == "/data/qlib_minute"
    assert local[11] == "/opt/rdagent"
    assert local[12] == "http://127.0.0.1:8011"


def test_compute_node_seed_import_loads_explicit_env_file(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                'AISTOCK_QE_CALLBACK_BASE_URL="http://127.0.0.1:8011"',
                'QLIB_RDAGENT_ROOT_WSL="/opt/rdagent"',
                'QLIB_DATA_PATH_WSL="/data/day"',
                'QLIB_MINUTE_DATA="/data/minute"',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("AISTOCK_ENV_FILE", str(env_file))

    importlib.reload(create_dispatch_tables)
    try:
        local = create_dispatch_tables.build_compute_node_seed_rows()[0]
    finally:
        importlib.reload(create_dispatch_tables)

    assert local[7] == "/opt/rdagent/qe_workspace"
    assert local[9] == "/data/day"
    assert local[10] == "/data/minute"


def test_compute_node_seed_derives_safe_paths_from_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AISTOCK_QE_CALLBACK_BASE_URL", "http://127.0.0.1:8011")
    monkeypatch.setenv("QLIB_RDAGENT_ROOT_WSL", "/opt/rdagent")
    monkeypatch.setenv("QLIB_DAY_DATA", "/data/qlib_day")
    monkeypatch.setenv("QLIB_MINUTE_DATA", "/data/qlib_minute")

    local = create_dispatch_tables.build_compute_node_seed_rows()[0]

    assert local[7] == "/opt/rdagent/qe_workspace"
    assert local[8] == "/opt/rdagent/git_ignore_folder/factor_implementation_source_data"
    assert local[9] == "/data/qlib_day"
    assert local[10] == "/data/qlib_minute"


def test_compute_node_seed_fails_only_when_builder_invoked() -> None:
    with pytest.raises(RuntimeError, match="AISTOCK_QE_CALLBACK_BASE_URL"):
        create_dispatch_tables.build_compute_node_seed_rows()


def test_backfill_log_root_resolution_order(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    empty_env = tmp_path / ".env"
    empty_env.write_text("", encoding="utf-8")
    monkeypatch.setenv("AISTOCK_ENV_FILE", str(empty_env))
    module = importlib.import_module("backend.scripts.backfill_model_training_from_logs")

    assert str(module.resolve_log_root("/explicit/log")).replace("\\", "/") == "/explicit/log"

    monkeypatch.setenv("RDAGENT_LOG_ROOT", "/env/log")
    assert str(module.resolve_log_root()).replace("\\", "/") == "/env/log"

    monkeypatch.delenv("RDAGENT_LOG_ROOT", raising=False)
    monkeypatch.delenv("QLIB_RDAGENT_LOG_ROOT", raising=False)
    monkeypatch.setenv("QLIB_RDAGENT_ROOT_WSL", "/opt/rdagent")
    assert str(module.resolve_log_root()).replace("\\", "/") == "/opt/rdagent/log"


def test_backfill_log_root_missing_is_clear(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("backend.scripts.backfill_model_training_from_logs")
    empty_env = tmp_path / ".env"
    empty_env.write_text("", encoding="utf-8")
    monkeypatch.setenv("AISTOCK_ENV_FILE", str(empty_env))

    with pytest.raises(RuntimeError, match="RD-Agent log root is not configured"):
        module.resolve_log_root()


def test_factor_metrics_log_root_resolution(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    empty_env = tmp_path / ".env"
    empty_env.write_text("", encoding="utf-8")
    monkeypatch.setenv("AISTOCK_ENV_FILE", str(empty_env))
    module = importlib.import_module("backend.scripts.extract_factor_metrics_wsl")

    assert module.resolve_log_root("/explicit/log") == "/explicit/log"
    monkeypatch.setenv("QLIB_RDAGENT_LOG_ROOT", "/alias/log")
    assert module.resolve_log_root() == "/alias/log"


def test_config_manager_preserves_runtime_path_keys(tmp_path) -> None:
    env_file = tmp_path / ".env"
    manager = ConfigManager(env_file)
    config = manager.read_env()
    config.update(
        {
            "QLIB_RDAGENT_ROOT_WSL": "/opt/rdagent",
            "QE_WORKSPACE_WSL": "/opt/rdagent/qe_workspace",
            "RDAGENT_FACTOR_DATA_WSL": "/opt/rdagent/git_ignore_folder/factor_implementation_source_data",
            "QLIB_DATA_PATH_WSL": "/data/day",
            "QLIB_MINUTE_PATH_WSL": "/data/minute",
            "RDAGENT_LOG_ROOT": "/opt/rdagent/log",
        }
    )

    assert manager.write_env(config)
    reread = manager.read_env()

    assert reread["QLIB_RDAGENT_ROOT_WSL"] == "/opt/rdagent"
    assert reread["QE_WORKSPACE_WSL"] == "/opt/rdagent/qe_workspace"
    assert reread["RDAGENT_FACTOR_DATA_WSL"].endswith("factor_implementation_source_data")
    assert reread["QLIB_DATA_PATH_WSL"] == "/data/day"
    assert reread["QLIB_MINUTE_PATH_WSL"] == "/data/minute"
    assert reread["RDAGENT_LOG_ROOT"] == "/opt/rdagent/log"
