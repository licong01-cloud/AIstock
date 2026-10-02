from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first import prospective_price_cli
from backend.services.advisory_model_first.prospective_price_prediction import (
    write_prospective_request,
)
from backend.tests.advisory_model_first.test_price_range_prospective import _request


def test_prepare_writes_frozen_request_and_compact_receipt(tmp_path: Path, capsys) -> None:
    request = _request()
    service = SimpleNamespace(prepare_request=lambda **_kwargs: request)
    env_file = tmp_path / ".env"
    env_file.write_text("# empty test environment\n", encoding="utf-8")
    output = tmp_path / "request.json"

    result = prospective_price_cli.main(
        [
            "prepare",
            "--env-file",
            str(env_file),
            "--model-root",
            str(tmp_path / "models"),
            "--program-id",
            request.program_id,
            "--target-trade-date",
            request.target_trade_date.isoformat(),
            "--parent-bundle-id",
            request.parent_bundle_id,
            "--outcome-bundle-id",
            request.outcome_bundle_id,
            "--price-range-bundle-id",
            request.price_range_bundle_id,
            "--request-output",
            str(output),
        ],
        service_factory=lambda: service,
    )

    assert result == 0
    assert json.loads(output.read_text(encoding="utf-8"))["request_id"] == request.request_id
    payload = json.loads(capsys.readouterr().out)
    assert payload["request_id"] == request.request_id
    assert payload["realized_outcome_access_allowed"] is False


def test_capture_reads_exact_request_and_reports_artifact_path(tmp_path: Path, capsys) -> None:
    request = _request()
    request_path = write_prospective_request(request, tmp_path / "request.json")
    receipt = SimpleNamespace(
        model_dump=lambda **_kwargs: {
            "status": "PUBLISHED",
            "request_id": request.request_id,
            "database_written": False,
        }
    )
    service = SimpleNamespace(capture=lambda **_kwargs: receipt)
    env_file = tmp_path / ".env"
    env_file.write_text("# empty test environment\n", encoding="utf-8")
    model_root = tmp_path / "models"

    result = prospective_price_cli.main(
        [
            "capture",
            "--env-file",
            str(env_file),
            "--model-root",
            str(model_root),
            "--request",
            str(request_path),
        ],
        service_factory=lambda: service,
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "PUBLISHED"
    assert payload["artifact_path"].endswith(request.request_id)


def test_capture_reports_typed_failure_without_traceback(tmp_path: Path, capsys) -> None:
    request = _request()
    request_path = write_prospective_request(request, tmp_path / "request.json")

    def fail(**_kwargs):
        raise AdvisoryModelFirstError(
            "target already opened",
            reason_code="ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID",
        )

    env_file = tmp_path / ".env"
    env_file.write_text("# empty test environment\n", encoding="utf-8")
    service = SimpleNamespace(capture=fail)

    result = prospective_price_cli.main(
        [
            "capture",
            "--env-file",
            str(env_file),
            "--model-root",
            str(tmp_path / "models"),
            "--request",
            str(request_path),
        ],
        service_factory=lambda: service,
    )

    assert result == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["reason_code"] == "ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID"
