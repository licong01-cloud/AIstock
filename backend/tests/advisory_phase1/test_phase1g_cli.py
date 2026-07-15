from __future__ import annotations

from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from backend.services.advisory_phase1.phase1g_contract import (
    REASON_UNEXPECTED_ERROR,
    Phase1GAttemptReceipt,
    Phase1GAttemptStatus,
)
from backend.tests.advisory_phase1.phase1g_test_support import capture_result
from scripts import advisory_phase1g_capture_observations as cli


def _write_json(path, model) -> None:  # type: ignore[no-untyped-def]
    path.write_text(
        json.dumps(model.model_dump(mode="json"), sort_keys=True), encoding="utf-8"
    )


def _failed_attempt() -> Phase1GAttemptReceipt:
    started = datetime(2026, 7, 15, 1, tzinfo=timezone.utc)
    return Phase1GAttemptReceipt(
        target_plan_hash="1" * 64,
        target_request_hash="2" * 64,
        attempt_invocation_id="attempt-cli-test",
        started_at=started,
        finished_at=started,
        operation_status=Phase1GAttemptStatus.FAILED,
        reason_codes=("ADVISORY_PHASE1G_PLAN_STALE",),
        dml_executed=False,
    )


def test_cli_exposes_only_the_four_frozen_commands_and_no_bypass_options() -> None:
    parser = cli._parser()
    help_text = parser.format_help().lower()

    for command in ("plan", "capture", "verify-result", "verify-attempt"):
        assert command in help_text
    for forbidden in (
        "--force",
        "--skip",
        "--approval",
        "--role",
        "--latest",
        "--run-selection",
        "--backup",
    ):
        assert forbidden not in help_text


def test_verify_result_is_strict_offline_typed_validation(tmp_path, capsys) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "result.json"
    result = capture_result()
    _write_json(path, result)

    exit_code = cli.main(["verify-result", "--result", str(path)])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == cli.EXIT_SUCCESS
    assert output == {
        "artifact_kind": "CAPTURE_RESULT",
        "capture_result_hash": result.capture_result_hash,
        "ok": True,
    }


@pytest.mark.parametrize(
    "partial_args",
    (
        ("--db-readback",),
        ("--result-root", "C:/missing"),
        ("--env-file", "C:/missing.env"),
        ("--target-db", "dev"),
    ),
)
def test_verify_attempt_db_readback_arguments_are_atomic(
    tmp_path, capsys, partial_args
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "attempt.json"
    _write_json(path, _failed_attempt())

    exit_code = cli.main(
        ["verify-attempt", "--attempt", str(path), *partial_args]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == cli.EXIT_COMMAND_ERROR
    assert output["ok"] is False
    assert output["reason_code"] == "ADVISORY_PHASE1G_COMMAND_INVALID"


def test_verify_attempt_offline_does_not_open_database(tmp_path, capsys, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "attempt.json"
    attempt = _failed_attempt()
    _write_json(path, attempt)
    monkeypatch.setattr(
        cli.psycopg2,
        "connect",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("database forbidden")),
    )

    exit_code = cli.main(["verify-attempt", "--attempt", str(path)])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == cli.EXIT_SUCCESS
    assert output["identity"] == attempt.attempt_receipt_hash
    assert output["db_readback"] is False


def test_cli_error_context_uses_a_fixed_redaction_allowlist() -> None:
    error = RuntimeError("safe message")
    error.reason_code = "ADVISORY_PHASE1G_PLAN_INVALID"  # type: ignore[attr-defined]
    error.context = {  # type: ignore[attr-defined]
        "cause_reason_code": "ADVISORY_PHASE1G_PLAN_INVALID",
        "field_name": "phase1e_plan_hash",
        "password": "must-not-escape",
        "dsn": "must-not-escape",
        "target_failures": [
            {
                "target_request_hash": "1" * 64,
                "reason_code": "ADVISORY_PHASE1G_PLAN_STALE",
                "password": "must-not-escape",
            }
        ],
    }

    document = cli._error_document("plan", error)

    assert document["context"] == {
        "cause_reason_code": "ADVISORY_PHASE1G_PLAN_INVALID",
        "field_name": "phase1e_plan_hash",
        "target_failures": [
            {
                "target_request_hash": "1" * 64,
                "reason_code": "ADVISORY_PHASE1G_PLAN_STALE",
            }
        ],
    }


@pytest.mark.parametrize(
    ("exit_class", "reason_codes", "expected"),
    (
        (cli.Phase1GExitClass.SUCCESS, (), cli.EXIT_SUCCESS),
        (
            cli.Phase1GExitClass.PARTIAL_FAILURE,
            ("ADVISORY_PHASE1G_PLAN_STALE",),
            cli.EXIT_TARGET_FAILURE,
        ),
        (
            cli.Phase1GExitClass.INFRASTRUCTURE_FAILURE,
            ("ADVISORY_PHASE1G_ATTEMPT_RECEIPT_STORE_FAILED",),
            cli.EXIT_RECEIPT_INCOMPLETE,
        ),
        (
            cli.Phase1GExitClass.PARTIAL_FAILURE,
            (REASON_UNEXPECTED_ERROR,),
            cli.EXIT_INTERNAL,
        ),
        (
            cli.Phase1GExitClass.INFRASTRUCTURE_FAILURE,
            ("ADVISORY_PHASE1G_G3_UNEXPECTED_ERROR",),
            cli.EXIT_INTERNAL,
        ),
    ),
)
def test_capture_exit_code_preserves_frozen_priority(
    exit_class, reason_codes, expected
) -> None:  # type: ignore[no-untyped-def]
    result = SimpleNamespace(
        exit_class=exit_class,
        target_outcomes=(SimpleNamespace(reason_codes=reason_codes),),
    )

    assert cli._capture_exit_code(result) == expected
