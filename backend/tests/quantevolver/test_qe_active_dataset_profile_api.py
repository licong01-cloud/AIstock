from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from backend.routers import quantevolver as router
from backend.services.quantevolver import config_composer as composer_module
from backend.services.quantevolver import qe_active_dataset_profile as profile_module


def test_active_single_create_response_hides_internal_binding_and_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = SimpleNamespace(
        generation="generation-1",
        release_id="release-1",
        cutoff=SimpleNamespace(isoformat=lambda: "2026-08-31"),
        qe={"default_universe": {"mode": "stock_universe", "pool_ids": []}},
    )
    resolved_params = {
        "random_seed": 123,
        "_qe_direct_v2_dataset_binding": {"secret": "binding"},
    }
    monkeypatch.setattr(profile_module, "load_active_qe_profile", lambda: profile)
    monkeypatch.setattr(
        profile_module,
        "resolve_and_apply_active_qe_dataset",
        lambda **_kwargs: (
            {
                "train_start": "2018-08-01",
                "train_end": "2022-12-31",
                "valid_start": "2023-01-01",
                "valid_end": "2024-06-30",
                "test_start": "2024-07-01",
                "test_end": "2026-08-31",
                "backtest_end": "2026-08-28",
            },
            resolved_params,
            {"generation": "generation-1", "release_id": "release-1"},
        ),
    )
    monkeypatch.setattr(router, "_validate_qe_catalog_refs", lambda *_args: None)
    monkeypatch.setattr(router, "resolve_default_qe_node_id", lambda: "wsl2-5080")

    class FakeComposer:
        def compose_experiment_in_memory(self, **_kwargs):
            return {
                "experiment_id": "qe_exp",
                "experiment_name": "qe_exp",
                "experiment_files": {"qe_direct_v2_dataset_binding.json": "secret"},
                "wsl_command": "cd /private/path",
                "wsl_command_core": "secret",
                "wsl_workdir": "/private/path",
                "direct_v2_dataset_binding": {"secret": "binding"},
            }

    monkeypatch.setattr(composer_module, "ConfigComposer", FakeComposer)
    result = router.generate_config(
        router.GenerateConfigRequest(
            factor_names=["factor_a"],
            model_id="model_a",
            custom_params={"random_seed": 123},
            node_id="wsl2-5080",
        )
    )

    assert result["ok"] is True
    assert result["resolved_dataset"]["release_id"] == "release-1"
    serialized = repr(result)
    assert "experiment_files" not in result
    assert "wsl_command" not in result
    assert "direct_v2_dataset_binding" not in result
    assert "/private/path" not in serialized


def test_semantic_universe_is_not_silently_ignored_without_active_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(profile_module, "load_active_qe_profile", lambda: None)
    monkeypatch.setattr(router, "_validate_qe_catalog_refs", lambda *_args: None)

    with pytest.raises(HTTPException) as exc_info:
        router.generate_config(
            router.GenerateConfigRequest(
                factor_names=["factor_a"],
                model_id="model_a",
                custom_params={"random_seed": 123},
                universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
            )
        )

    assert exc_info.value.status_code == 400
    assert "qe_active_dataset_profile_missing" in str(exc_info.value.detail)


def test_active_single_create_rejects_client_supplied_internal_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = SimpleNamespace()
    monkeypatch.setattr(profile_module, "load_active_qe_profile", lambda: profile)
    monkeypatch.setattr(router, "_validate_qe_catalog_refs", lambda *_args: None)

    with pytest.raises(HTTPException) as exc_info:
        router.generate_config(
            router.GenerateConfigRequest(
                factor_names=["factor_a"],
                model_id="model_a",
                custom_params={
                    "random_seed": 123,
                    "_qe_direct_v2_dataset_binding": {"provider_uri_day": "/client/path"},
                },
            )
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["reason_code"] == "qe_dataset_internal_input_forbidden"
