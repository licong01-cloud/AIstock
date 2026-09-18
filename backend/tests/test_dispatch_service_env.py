import pytest

from backend.services import dispatch_service
from backend.services.dispatch_service import (
    _normalize_running_task_ids,
    build_rdagent_env_overrides,
)


@pytest.fixture(autouse=True)
def _isolate_active_dataset_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        dispatch_service,
        "resolve_active_dataset_node_binding",
        lambda *, node_id: None,
    )


def test_build_rdagent_env_overrides_maps_ui_config_to_rdagent_env() -> None:
    env = build_rdagent_env_overrides(
        data={"custom_env": {"EXISTING": "1"}},
        node={
            "qlib_data_path": "/data/day",
            "qlib_minute_path": "/data/minute",
            "qlib_rdagent_root": "/home/lc999/projects/RD-Agent-main",
        },
        config={
            "app_tpl": "../app_tpl/all/v25/rdagent",
            "multi_proc_n": 2,
            "costeer_max_loop": 7,
        },
    )

    assert env["EXISTING"] == "1"
    assert env["RD_AGENT_SETTINGS__APP_TPL"] == "../app_tpl/all/v25/rdagent"
    assert env["RD_AGENT_SETTINGS__MULTI_PROC_N"] == "2"
    assert env["CoSTEER_MAX_LOOP"] == "7"
    assert env["FACTOR_CoSTEER_MAX_LOOP"] == "7"
    assert env["MODEL_CoSTEER_MAX_LOOP"] == "7"
    assert env["QLIB_DAY_DATA"] == "/data/day"
    assert env["QLIB_DATA_PATH_WSL"] == "/data/day"
    assert env["QLIB_MINUTE_DATA"] == "/data/minute"
    assert env["QLIB_MINUTE_PATH_WSL"] == "/data/minute"
    assert env["QLIB_WSL_CONDA_ENV"] == "rdagent-gpu"
    assert env["QLIB_WSL_CONDA_SH"] == "/home/lc999/miniconda3/etc/profile.d/conda.sh"
    assert env["QLIB_RDAGENT_ROOT_WSL"] == "/home/lc999/projects/RD-Agent-main"
    assert env["QLIB_SCRIPTS_SUBDIR"] == "scripts"
    assert env["CONDA_DEFAULT_ENV"] == "rdagent-gpu"
    assert env["PATH"].startswith("/home/lc999/miniconda3/envs/rdagent-gpu/bin:")
    assert "/home/lc999/miniconda3/condabin" in env["PATH"]
    assert env["PATH"].endswith(":/bin")


def test_build_rdagent_env_overrides_preserves_explicit_custom_env() -> None:
    env = build_rdagent_env_overrides(
        data={
            "custom_env": {
                "RD_AGENT_SETTINGS__APP_TPL": "../app_tpl/custom/rdagent",
                "QLIB_MINUTE_DATA": "/custom/minute",
                "PATH": "/custom/bin:/usr/bin",
            }
        },
        node={"linux_home": "/home/rdagent", "qlib_minute_path": "/node/minute"},
        config={"app_tpl": "../app_tpl/all/v25/rdagent"},
    )

    assert env["RD_AGENT_SETTINGS__APP_TPL"] == "../app_tpl/custom/rdagent"
    assert env["QLIB_MINUTE_DATA"] == "/custom/minute"
    assert env["QLIB_MINUTE_PATH_WSL"] == "/node/minute"
    assert env["PATH"] == "/custom/bin:/usr/bin"


def test_build_rdagent_env_overrides_derives_remote_linux_home() -> None:
    env = build_rdagent_env_overrides(
        data={},
        node={
            "qlib_rdagent_root": "/home/quant/projects/RD-Agent-main",
            "qlib_data_path": "/home/quant/data/qlib_bin",
        },
        config={},
    )

    assert env["PATH"].startswith("/home/quant/miniconda3/envs/rdagent-gpu/bin:")
    assert "/home/quant/miniconda3/bin" in env["PATH"]


def test_normalize_running_task_ids_drops_null_sentinels() -> None:
    metrics = {"running_tasks": [None, "None", " null ", "", 178, "abc"]}

    assert _normalize_running_task_ids(metrics) == ["178", "abc"]


def test_build_rdagent_env_overrides_uses_active_release_binding(monkeypatch) -> None:
    monkeypatch.setattr(
        dispatch_service,
        "resolve_active_dataset_node_binding",
        lambda *, node_id: {
            "node_id": node_id,
            "candidate_root": "/releases/r8",
            "qlib_data_path": "/releases/r8/components/daily_bin_candidate",
            "qlib_minute_path": "/releases/r8/components/minute_bin_candidate",
            "factor_data_dir": "/releases/r8/components/factor_h5_static_candidate_v2",
            "sector_context_dir": "/releases/r8/components/sector_context_candidate_v1",
        },
    )

    env = build_rdagent_env_overrides(
        data={},
        node={
            "node_id": "rdagent-node1",
            "qlib_data_path": "/legacy/day",
            "qlib_minute_path": "/legacy/minute",
            "qlib_rdagent_root": "/home/lc999/projects/RD-Agent-main",
        },
        config={},
    )

    assert env["AISTOCK_DATASET_ROOT"] == "/releases/r8"
    assert env["QLIB_DATA_PATH_WSL"].endswith("/daily_bin_candidate")
    assert env["QLIB_MINUTE_PATH_WSL"].endswith("/minute_bin_candidate")
    assert env["RDAGENT_FACTOR_DATA_WSL"].endswith("/factor_h5_static_candidate_v2")
    assert env["AISTOCK_SECTOR_CONTEXT_DIR"].endswith("/sector_context_candidate_v1")


def test_build_rdagent_env_overrides_rejects_active_release_override(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        dispatch_service,
        "resolve_active_dataset_node_binding",
        lambda *, node_id: {
            "node_id": node_id,
            "candidate_root": "/releases/r8",
            "qlib_data_path": "/releases/r8/day",
            "qlib_minute_path": "/releases/r8/minute",
            "factor_data_dir": "/releases/r8/factor",
        },
    )

    with pytest.raises(ValueError, match="differ from the active dataset profile"):
        build_rdagent_env_overrides(
            data={"custom_env": {"QLIB_DATA_PATH_WSL": "/legacy/day"}},
            node={
                "node_id": "rdagent-node1",
                "qlib_rdagent_root": "/home/lc999/projects/RD-Agent-main",
            },
            config={},
        )
