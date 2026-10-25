"""HMM producer updates must preserve the installed monthly consumer boundary."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_hmm_authority_bootstrap import (
    bootstrap_monthly_hmm_authority,
)
from backend.services.dataset_release.monthly_production import load_monthly_hmm_authority
from backend.services.dataset_release.monthly_runtime import MonthlyRuntimeConfigurationError
from scripts import dataset_release_hmm_authority as cli


def _installed(tmp_path: Path) -> tuple[Path, Path]:
    project = tmp_path / "project"
    script = project / "scripts/precompute_hmm_coefficients.py"
    script.parent.mkdir(parents=True)
    script.write_bytes(b"# approved old producer\n")
    model = tmp_path / "models.json"
    model.write_bytes(canonical_json_bytes({"801011.SI": {"state_labels": {"0": "neutral"}}}))
    coefficients = tmp_path / "coefficients.json"
    coefficients.write_bytes(
        canonical_json_bytes(
            {
                "model_sha256": hashlib.sha256(model.read_bytes()).hexdigest(),
                "preset_key": "preset_A",
                "preset_coeffs": {"neutral": 1.0},
                "test_start": "2024-07-01",
                "backtest_end": "2024-07-02",
                "dynamic_coefficients": False,
                "dataset_identity": {"dataset_manifest_sha256": "a" * 64},
                "daily_coefficients": {"2024-07-01": {"801011.SI": 1.0}, "2024-07-02": {"801011.SI": 1.0}},
            }
        )
    )
    result = bootstrap_monthly_hmm_authority(
        source_coefficients=coefficients,
        model_path=model,
        producer_script=script,
        output_root=tmp_path / "installed",
        project_root=project,
        authority_id="approved-v1",
        asset_id="qe-hmm-preset-a-full-window",
        expected_dataset_manifest_sha256="a" * 64,
    )
    return result.authority_path, script


def test_stale_installed_authority_is_detected_before_restart_and_rebound_without_model_change(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path, script = _installed(tmp_path)
    original = {p.name: p.read_bytes() for p in path.parent.iterdir()}
    # Use this checkout's real producer bytes, not a mocked monthly loader or
    # an unrelated toy producer. CI does not read host production assets.
    repository_producer = Path(__file__).resolve().parents[3] / "scripts/precompute_hmm_coefficients.py"
    script.write_bytes(repository_producer.read_bytes())
    with pytest.raises(MonthlyRuntimeConfigurationError, match="producer hash differs"):
        load_monthly_hmm_authority(path, producer_script=script)

    assert cli.main(["--verify-only", "--authority-path", str(path), "--producer-script", str(script)]) == 2
    failed = json.loads(capsys.readouterr().err)
    assert failed["status"] == "FAILED"
    assert "producer hash differs" in failed["message"]
    assert {p.name: p.read_bytes() for p in path.parent.iterdir()} == original

    successor = tmp_path / "successor"
    assert (
        cli.main(
            [
                "--rebind-from-authority",
                str(path),
                "--expected-previous-authority-sha256",
                hashlib.sha256(original[path.name]).hexdigest(),
                "--expected-producer-sha256",
                hashlib.sha256(script.read_bytes()).hexdigest(),
                "--producer-script",
                str(script),
                "--output-root",
                str(successor),
                "--authority-id",
                "approved-v2",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"
    new_path = successor / path.name
    authority = load_monthly_hmm_authority(new_path, producer_script=script)
    assert authority.script_sha256 == hashlib.sha256(script.read_bytes()).hexdigest()
    for name in ("models.json", "config.json", "approved_coefficients.json"):
        assert (successor / name).read_bytes() == original[name]
    assert {p.name: p.read_bytes() for p in path.parent.iterdir()} == original
    assert cli.main(["--verify-only", "--authority-path", str(new_path), "--producer-script", str(script)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"
    script.write_bytes(b"# unreviewed third producer\n")
    assert cli.main(["--verify-only", "--authority-path", str(new_path), "--producer-script", str(script)]) == 2
    assert "producer hash differs" in json.loads(capsys.readouterr().err)["message"]
