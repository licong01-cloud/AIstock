from backend.services.dispatch_service import (
    _normalize_running_task_ids,
    build_rdagent_env_overrides,
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
