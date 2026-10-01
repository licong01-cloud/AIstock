from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_hmm_authority_bootstrap import (
    BOOTSTRAP_RECEIPT_SCHEMA,
    MonthlyHMMAuthorityBootstrapError,
    bootstrap_monthly_hmm_authority,
)
from backend.services.dataset_release.monthly_production import (
    load_monthly_hmm_authority,
)


HEX = "a" * 64


def _write_json(path: Path, value: object) -> Path:
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return path


def _inputs(tmp_path: Path, *, dynamic: bool = False) -> dict[str, Path]:
    source = tmp_path / "source"
    source.mkdir()
    model = _write_json(
        source / "models.json",
        {
            "801011.SI": {
                "state_labels": {"0": "fading", "1": "neutral", "2": "trending"},
                "means": [[0.0], [1.0], [2.0]],
            }
        },
    )
    model_sha = hashlib.sha256(model.read_bytes()).hexdigest()
    coefficients = _write_json(
        source / "coefficients.json",
        {
            "model_sha256": model_sha,
            "preset_key": "preset_A",
            "preset_coeffs": {"trending": 1.0, "neutral": 1.0, "fading": 0.955},
            "test_start": "2024-07-01",
            "backtest_end": "2024-07-02",
            "dynamic_coefficients": dynamic,
            "dataset_identity": {
                "generation": "fixture-v1",
                "release_id": "fixture-release",
                "cutoff": "2024-07-03",
                "dataset_manifest_sha256": HEX,
            },
            "daily_coefficients": {
                "2024-07-01": {"801011.SI": 1.0},
                "2024-07-02": {"801011.SI": 0.955},
            },
        },
    )
    script = tmp_path / "producer.py"
    script.write_bytes(b"print('producer')\n")
    project = tmp_path / "project"
    project.mkdir()
    external = tmp_path / "external"
    external.mkdir()
    config = _write_json(source / "config.json", {"method": "pup"})
    return {
        "coefficients": coefficients,
        "model": model,
        "script": script,
        "project": project,
        "external": external,
        "config": config,
    }


def test_bootstrap_packages_static_approved_product_create_exclusive(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    target = inputs["external"] / "hmm-v1"

    result = bootstrap_monthly_hmm_authority(
        source_coefficients=inputs["coefficients"],
        model_path=inputs["model"],
        producer_script=inputs["script"],
        output_root=target,
        project_root=inputs["project"],
        authority_id="hmm-static-monthly-v1",
        asset_id="preset-a-full-window",
        expected_dataset_manifest_sha256=HEX,
    )

    assert result.root == target
    assert result.authority_path.read_bytes().endswith(b"\n")
    assert (target / "models.json").read_bytes() == inputs["model"].read_bytes()
    assert (target / "approved_coefficients.json").read_bytes() == inputs[
        "coefficients"
    ].read_bytes()
    assert (target / "config.json").read_bytes() == b"{}\n"
    authority = load_monthly_hmm_authority(
        result.authority_path,
        producer_script=inputs["script"],
    )
    assert authority.authority_id == "hmm-static-monthly-v1"
    assert authority.products[0].preset_coefficients == {
        "fading": 0.955,
        "neutral": 1.0,
        "trending": 1.0,
    }
    receipt = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    assert receipt["schema_version"] == BOOTSTRAP_RECEIPT_SCHEMA
    assert receipt["trade_date_count"] == 2
    assert receipt["coefficient_row_count"] == 2
    assert receipt["model_sector_count"] == 1
    assert receipt["config_mode"] == "static_state_labels_empty_config_v1"
    assert receipt["database_read_performed"] is False
    assert receipt["database_write_performed"] is False
    assert receipt["training_started"] is False
    assert receipt["runtime_action_performed"] is False

    with pytest.raises(MonthlyHMMAuthorityBootstrapError, match="already exists"):
        bootstrap_monthly_hmm_authority(
            source_coefficients=inputs["coefficients"],
            model_path=inputs["model"],
            producer_script=inputs["script"],
            output_root=target,
            project_root=inputs["project"],
            authority_id="hmm-static-monthly-v1",
            asset_id="preset-a-full-window",
            expected_dataset_manifest_sha256=HEX,
        )


def test_bootstrap_rejects_manifest_or_model_drift(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    with pytest.raises(MonthlyHMMAuthorityBootstrapError, match="manifest differs"):
        bootstrap_monthly_hmm_authority(
            source_coefficients=inputs["coefficients"],
            model_path=inputs["model"],
            producer_script=inputs["script"],
            output_root=inputs["external"] / "manifest-drift",
            project_root=inputs["project"],
            authority_id="hmm-static-monthly-v1",
            asset_id="preset-a-full-window",
            expected_dataset_manifest_sha256="b" * 64,
        )

    inputs["model"].write_bytes(b"{}\n")
    with pytest.raises(MonthlyHMMAuthorityBootstrapError, match="model SHA256 differs"):
        bootstrap_monthly_hmm_authority(
            source_coefficients=inputs["coefficients"],
            model_path=inputs["model"],
            producer_script=inputs["script"],
            output_root=inputs["external"] / "model-drift",
            project_root=inputs["project"],
            authority_id="hmm-static-monthly-v1",
            asset_id="preset-a-full-window",
            expected_dataset_manifest_sha256=HEX,
        )


def test_bootstrap_requires_explicit_config_for_dynamic_model(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path, dynamic=True)
    with pytest.raises(MonthlyHMMAuthorityBootstrapError, match="explicit frozen config"):
        bootstrap_monthly_hmm_authority(
            source_coefficients=inputs["coefficients"],
            model_path=inputs["model"],
            producer_script=inputs["script"],
            output_root=inputs["external"] / "dynamic-missing-config",
            project_root=inputs["project"],
            authority_id="hmm-dynamic-monthly-v1",
            asset_id="preset-a-full-window",
            expected_dataset_manifest_sha256=HEX,
        )

    result = bootstrap_monthly_hmm_authority(
        source_coefficients=inputs["coefficients"],
        model_path=inputs["model"],
        config_path=inputs["config"],
        producer_script=inputs["script"],
        output_root=inputs["external"] / "dynamic-with-config",
        project_root=inputs["project"],
        authority_id="hmm-dynamic-monthly-v1",
        asset_id="preset-a-full-window",
        expected_dataset_manifest_sha256=HEX,
    )
    assert (result.root / "config.json").read_bytes() == inputs[
        "config"
    ].read_bytes()


def test_bootstrap_rejects_truncated_daily_window(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    value = json.loads(inputs["coefficients"].read_text(encoding="utf-8"))
    del value["daily_coefficients"]["2024-07-01"]
    _write_json(inputs["coefficients"], value)

    with pytest.raises(MonthlyHMMAuthorityBootstrapError, match="window differs"):
        bootstrap_monthly_hmm_authority(
            source_coefficients=inputs["coefficients"],
            model_path=inputs["model"],
            producer_script=inputs["script"],
            output_root=inputs["external"] / "truncated-window",
            project_root=inputs["project"],
            authority_id="hmm-static-monthly-v1",
            asset_id="preset-a-full-window",
            expected_dataset_manifest_sha256=HEX,
        )


def test_bootstrap_rejects_repository_owned_output(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    with pytest.raises(MonthlyHMMAuthorityBootstrapError, match="repository-external"):
        bootstrap_monthly_hmm_authority(
            source_coefficients=inputs["coefficients"],
            model_path=inputs["model"],
            producer_script=inputs["script"],
            output_root=inputs["project"] / "authority",
            project_root=inputs["project"],
            authority_id="hmm-static-monthly-v1",
            asset_id="preset-a-full-window",
            expected_dataset_manifest_sha256=HEX,
        )
