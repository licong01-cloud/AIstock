from __future__ import annotations

import json
from types import SimpleNamespace

from backend.services.advisory_model_first import adaptive_price_calibration_cli as cli
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def _args(tmp_path):
    env = tmp_path / ".env"
    env.write_text("TEST_ONLY=1\n", encoding="utf-8")
    return [
        "run",
        "--env-file",
        str(env),
        "--source-replay-root",
        str(tmp_path / "models" / "price_range_historical_replays" / ("advprhist_" + "1" * 24)),
        "--output-root",
        str(tmp_path / "models"),
        "--registry-path",
        str(tmp_path / "n0" / "registry.jsonl"),
        "--repository-root",
        str(tmp_path),
    ]


def test_cli_returns_published_delivery(monkeypatch, tmp_path, capsys) -> None:
    receipt = SimpleNamespace(
        model_dump=lambda **_kwargs: {
            "status": "PUBLISHED",
            "request_id": "advpradapt_" + "1" * 24,
        }
    )
    delivery = SimpleNamespace(
        artifact=SimpleNamespace(receipt=receipt, path=tmp_path / "artifact"),
        registry_delivery={"appended_count": 1},
    )
    monkeypatch.setattr(cli, "_repository_commit", lambda _root: "a" * 40)
    monkeypatch.setattr(cli, "prepare_adaptive_price_calibration_request", lambda **_kwargs: object())
    monkeypatch.setattr(
        cli,
        "AdvisoryAdaptivePriceCalibrationService",
        lambda: SimpleNamespace(run=lambda **_kwargs: delivery),
    )
    assert cli.main(_args(tmp_path)) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "PUBLISHED"
    assert payload["registry_delivery"] == {"appended_count": 1}


def test_cli_returns_typed_failure(monkeypatch, tmp_path, capsys) -> None:
    monkeypatch.setattr(cli, "_repository_commit", lambda _root: "a" * 40)

    def fail(**_kwargs):
        raise AdvisoryModelFirstError(
            "source changed",
            reason_code="ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
            context={"field": "actual_open"},
        )

    monkeypatch.setattr(cli, "prepare_adaptive_price_calibration_request", fail)
    assert cli.main(_args(tmp_path)) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["status"] == "FAILED"
    assert payload["reason_code"] == "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT"
    assert payload["context"] == {"field": "actual_open"}


def test_cli_rejects_missing_env_file(tmp_path, capsys) -> None:
    args = _args(tmp_path)
    (tmp_path / ".env").unlink()
    assert cli.main(args) == 2
    assert json.loads(capsys.readouterr().err)["reason_code"] == (
        "ADVISORY_ADAPTIVE_PRICE_REQUEST_INVALID"
    )
