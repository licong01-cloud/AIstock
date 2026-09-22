from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

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
