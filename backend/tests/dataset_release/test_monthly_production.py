from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path
from typing import Any, Mapping
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_production import (
    MONTHLY_HMM_AUTHORITY_SCHEMA,
    MonthlyProductionSettings,
    load_monthly_hmm_authority,
)
from backend.services.dataset_release.monthly_runtime import (
    MonthlyRuntimeConfigurationError,
)


def _file(path: Path, value: bytes) -> dict[str, Any]:
    path.write_bytes(value)
    return {
        "path": str(path.resolve()),
        "sha256": hashlib.sha256(value).hexdigest(),
        "size": len(value),
    }


def _authority(tmp_path: Path, *, coefficient: object = -0.25) -> tuple[Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    script = tmp_path / "producer.py"
    script.write_bytes(b"print('producer')\n")
    model = _file(tmp_path / "model.pkl", b"frozen-model")
    config = _file(tmp_path / "config.json", b'{"version":1}\n')
    value: Mapping[str, Any] = {
        "schema_version": MONTHLY_HMM_AUTHORITY_SCHEMA,
        "authority_id": "hmm-g2a-v16-monthly",
        "model": model,
        "config": config,
        "producer_script_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
        "products": [
            {
                "asset_id": "preset-a-full-window",
                "preset_key": "preset_A",
                "preset_coefficients": {"801011.SI": coefficient},
                "test_start": "2024-07-01",
                "backtest_lag_trade_days": 1,
            }
        ],
    }
    path = tmp_path / "authority.json"
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return path, script


def test_hmm_authority_loader_binds_real_files_and_products(tmp_path: Path) -> None:
    path, script = _authority(tmp_path)

    authority = load_monthly_hmm_authority(path, producer_script=script)

    assert authority.authority_id == "hmm-g2a-v16-monthly"
    assert authority.model_path == (tmp_path / "model.pkl").resolve()
    assert authority.config_path == (tmp_path / "config.json").resolve()
    assert authority.products[0].preset_key == "preset_A"
    assert authority.products[0].preset_coefficients == {"801011.SI": -0.25}
    assert len(authority.authority_sha256) == 64


def test_hmm_authority_loader_rejects_file_drift_and_noncanonical_values(
    tmp_path: Path,
) -> None:
    path, script = _authority(tmp_path)
    (tmp_path / "model.pkl").write_bytes(b"changed")
    with pytest.raises(MonthlyRuntimeConfigurationError, match="model authority bytes differ"):
        load_monthly_hmm_authority(path, producer_script=script)

    boolean_path, boolean_script = _authority(tmp_path / "boolean", coefficient=True)
    with pytest.raises(MonthlyRuntimeConfigurationError, match="coefficients are invalid"):
        load_monthly_hmm_authority(boolean_path, producer_script=boolean_script)


def test_production_settings_require_external_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    profile = project / "configs" / "datasets" / "qe_backtest_monthly_v2.yaml"
    profile.parent.mkdir(parents=True)
    profile.write_text("profile: fixture\n", encoding="utf-8")
    authority, _script = _authority(tmp_path / "external")
    monkeypatch.setenv("AISTOCK_MONTHLY_HMM_AUTHORITY_PATH", str(authority))

    settings = MonthlyProductionSettings.from_env(project_root=project)
    assert settings.hmm_authority_path == authority
    monkeypatch.delenv("AISTOCK_MONTHLY_HMM_AUTHORITY_PATH")
    with pytest.raises(MonthlyRuntimeConfigurationError, match="environment is incomplete"):
        MonthlyProductionSettings.from_env(project_root=project)


def test_production_settings_reject_repository_owned_authority(tmp_path: Path) -> None:
    project = tmp_path / "project"
    profile = project / "configs" / "datasets" / "qe_backtest_monthly_v2.yaml"
    profile.parent.mkdir(parents=True)
    profile.write_text("profile: fixture\n", encoding="utf-8")
    authority, _script = _authority(project / "frozen")

    with pytest.raises(MonthlyRuntimeConfigurationError, match="repository-external"):
        MonthlyProductionSettings(
            project_root=project,
            profile_path=profile,
            hmm_authority_path=authority,
        )


def test_default_registry_installs_private_executor_and_shares_formal_wsl_scope(tmp_path, monkeypatch):
    import backend.services.dataset_release.monthly_production as module
    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.monthly_preparation_composition import MonthlyPrivatePreparationExecutor

    artifact = tmp_path / "control"
    ControlStore.initialize(artifact)
    catalog = tmp_path / "candidates"
    catalog.mkdir()
    script = tmp_path / "scripts/precompute_hmm_coefficients.py"
    script.parent.mkdir()
    script.write_text("# unused composition test boundary\n", encoding="utf-8")
    profile = SimpleNamespace(
        profile=module.CANONICAL_PROFILE_ID, candidate_root=catalog, control_root=artifact,
        semantic_profile_digest="a" * 64,
        qlib_toolchain=SimpleNamespace(build_verified=lambda _root: object()),
    )
    monkeypatch.setattr(module, "load_dataset_profile", lambda _path: profile)
    # These are composition boundaries only. Actual writers and fresh WSL
    # dump/recovery have their own real-file tests; no provider is opened here.
    for name in ("ResourceSupervisedMonthlyBuildScopeFactory", "MatureMonthlyPhysicalBuildRunner",
                 "SealedMonthlyBuildExecutor", "MonthlyHMMCoefficientExecutor", "WSLPythonHMMCoefficientProcess"):
        monkeypatch.setattr(module, name, lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(module, "load_monthly_hmm_authority", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(module.shutil, "which", lambda _name: str(Path(__file__).resolve()))
    monkeypatch.setattr(module, "build_monthly_worker_registry", lambda **kwargs: kwargs["executors"])
    result = module.build_monthly_production_registry(
        runtime=SimpleNamespace(controller_release_root=catalog, artifact_root=artifact),
        nodes=SimpleNamespace(wsl_distro="test-unused", wsl_python="test-unused"),
        production=SimpleNamespace(profile_path=tmp_path / "unused", project_root=tmp_path,
                                   hmm_authority_path=tmp_path / "unused-authority",
                                   sector_membership_start=date(2024, 7, 1)),
    )
    private = result.source.adapter.preparation_executor
    assert isinstance(private, MonthlyPrivatePreparationExecutor)
    assert private.execution_scope_factory is result.build.runner.execution_scope_factory
    assert private.cas.root == artifact
