from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.services.dataset_release.monthly_hmm_authority_bootstrap import (
    MonthlyHMMAuthorityBootstrapError,
    MonthlyHMMAuthorityBootstrapResult,
)
from scripts import dataset_release_hmm_authority as cli


def test_cli_emits_compact_success_receipt(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    authority = root / "monthly_hmm_coefficient_authority.json"
    receipt = root / "bootstrap_receipt.json"
    captured: dict[str, object] = {}

    def bootstrap(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return MonthlyHMMAuthorityBootstrapResult(
            root=root,
            authority_path=authority,
            receipt_path=receipt,
            authority_sha256="a" * 64,
            receipt_sha256="b" * 64,
        )

    monkeypatch.setattr(cli, "bootstrap_monthly_hmm_authority", bootstrap)
    code = cli.main(
        [
            "--source-coefficients",
            str(tmp_path / "source.json"),
            "--model-path",
            str(tmp_path / "models.json"),
            "--producer-script",
            str(tmp_path / "producer.py"),
            "--output-root",
            str(root),
            "--authority-id",
            "hmm-monthly-v1",
            "--asset-id",
            "preset-a-full-window",
            "--expected-dataset-manifest-sha256",
            "c" * 64,
        ]
    )

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "PASS"
    assert payload["authority_sha256"] == "a" * 64
    assert captured["project_root"] == cli.PROJECT_ROOT
    assert captured["backtest_lag_trade_days"] == 1


def test_cli_emits_structured_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    def fail(**_kwargs):  # type: ignore[no-untyped-def]
        raise MonthlyHMMAuthorityBootstrapError("model identity differs")

    monkeypatch.setattr(cli, "bootstrap_monthly_hmm_authority", fail)
    code = cli.main(
        [
            "--source-coefficients",
            str(tmp_path / "source.json"),
            "--model-path",
            str(tmp_path / "models.json"),
            "--producer-script",
            str(tmp_path / "producer.py"),
            "--output-root",
            str(tmp_path / "authority"),
            "--authority-id",
            "hmm-monthly-v1",
            "--asset-id",
            "preset-a-full-window",
            "--expected-dataset-manifest-sha256",
            "c" * 64,
        ]
    )

    assert code == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["status"] == "FAILED"
    assert payload["reason_code"] == "MONTHLY_HMM_AUTHORITY_BOOTSTRAP_FAILED"
