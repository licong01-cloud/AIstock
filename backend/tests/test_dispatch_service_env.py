import asyncio

import pytest

from backend.services import dispatch_service
from backend.services.dispatch_service import (
    DispatchService,
    _normalize_running_task_ids,
    build_rdagent_env_overrides,
)
from backend.services.dataset_release.active_task_binding import (
    freeze_active_dataset_task_binding,
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


def test_frozen_dispatch_binding_does_not_reresolve_active_profile(monkeypatch) -> None:
    frozen = freeze_active_dataset_task_binding(
        consumer_id="factor_research",
        node_id="rdagent-node1",
        resolver=lambda **kwargs: {
            "schema_version": "aistock_active_dataset_consumer_binding_v1",
            "consumer_id": kwargs["consumer_id"],
            "node_id": kwargs["node_id"],
            "generation": "20260930-monthly-v2-unified",
            "release_id": "qe_hmm_full_v2_20260930",
            "cutoff": "2026-09-30",
            "dataset_manifest_sha256": "b" * 64,
            "profile_sha256": "a" * 64,
            "candidate_root": "/releases/frozen",
            "required_components": ["day", "factor", "manifest", "stock_pools"],
            "derived_asset_registry_sha256": "c" * 64,
            "derived_asset_registry_path": "/releases/frozen/derived/registry.json",
            "release_closure_sha256": "d" * 64,
            "release_closure_path": "/releases/frozen/release_closure_receipt.json",
            "derived_assets": [],
            "resolved_once": True,
            "legacy_fallback": False,
        },
    )

    def forbidden(**_kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("retry path must not resolve the current active profile")

    monkeypatch.setattr(dispatch_service, "resolve_active_dataset_node_binding", forbidden)
    env = build_rdagent_env_overrides(
        data={},
        node={
            "node_id": "rdagent-node1",
            "qlib_rdagent_root": "/home/lc999/projects/RD-Agent-main",
        },
        config={},
        frozen_dataset_binding=frozen,
    )

    assert env["AISTOCK_DATASET_ROOT"] == "/releases/frozen"
    assert env["AISTOCK_DATASET_MANIFEST_SHA256"] == "b" * 64
    assert env["AISTOCK_DATASET_PROFILE_SHA256"] == "a" * 64


def _frozen_factor_binding() -> dict:
    return freeze_active_dataset_task_binding(
        consumer_id="factor_research",
        node_id="rdagent-node1",
        resolver=lambda **kwargs: {
            "schema_version": "aistock_active_dataset_consumer_binding_v1",
            "consumer_id": kwargs["consumer_id"],
            "node_id": kwargs["node_id"],
            "generation": "20260930-monthly-v2-unified",
            "release_id": "qe_hmm_full_v2_20260930",
            "cutoff": "2026-09-30",
            "dataset_manifest_sha256": "b" * 64,
            "profile_sha256": "a" * 64,
            "candidate_root": "/releases/frozen",
            "required_components": ["day", "factor", "manifest", "stock_pools"],
            "derived_asset_registry_sha256": "c" * 64,
            "derived_asset_registry_path": "/releases/frozen/derived/registry.json",
            "release_closure_sha256": "d" * 64,
            "release_closure_path": "/releases/frozen/release_closure_receipt.json",
            "derived_assets": [],
            "resolved_once": True,
            "legacy_fallback": False,
        },
    )


class _FailingComputeNodeClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    async def create_task(self, _payload):  # type: ignore[no-untyped-def]
        raise RuntimeError("submission stopped after local persistence")


def _capture_local_task(monkeypatch: pytest.MonkeyPatch) -> tuple[DispatchService, list[dict]]:
    captured: list[dict] = []
    service = DispatchService()

    def insert_task(data):  # type: ignore[no-untyped-def]
        captured.append(data)
        return {**data, "task_id": "local-task", "status": "pending"}

    monkeypatch.setattr(service, "_insert_task", insert_task)
    monkeypatch.setattr(service, "_add_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_append_local_log_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_update_task_fields", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(dispatch_service, "ComputeNodeClient", _FailingComputeNodeClient)
    frozen = _frozen_factor_binding()
    monkeypatch.setattr(
        dispatch_service,
        "_freeze_dispatch_dataset_binding",
        lambda **_kwargs: frozen,
    )
    return service, captured


def test_rdagent_task_persists_frozen_binding_before_remote_submission(monkeypatch) -> None:
    service, captured = _capture_local_task(monkeypatch)

    result = asyncio.run(
        service._create_rdagent_task(
            {
                "task_name": "factor task",
                "task_type": "fin_factor",
                "evolving_n": 1,
            },
            {
                "node_id": "rdagent-node1",
                "api_base_url": "http://node.invalid",
                "qlib_rdagent_root": "/home/lc999/projects/RD-Agent-main",
            },
        )
    )

    assert result["status"] == "failed"
    assert len(captured) == 1
    assert captured[0]["config"]["dataset_binding"]["profile_sha256"] == "a" * 64
    assert captured[0]["env_overrides"]["AISTOCK_DATASET_ROOT"] == "/releases/frozen"
    assert captured[0]["env_overrides"]["AISTOCK_DATASET_MANIFEST_SHA256"] == "b" * 64


def test_custom_task_persists_same_frozen_binding_and_environment(monkeypatch) -> None:
    service, captured = _capture_local_task(monkeypatch)

    result = asyncio.run(
        service._create_custom_task(
            {
                "task_name": "official factor task",
                "task_type": "official_evaluation",
                "payload": {"factor_name": "example"},
            },
            {
                "node_id": "rdagent-node1",
                "api_base_url": "http://node.invalid",
            },
        )
    )

    assert result["status"] == "failed"
    assert len(captured) == 1
    assert captured[0]["config"]["dataset_binding"]["binding_sha256"]
    assert captured[0]["env_overrides"]["AISTOCK_DATASET_ROOT"] == "/releases/frozen"
