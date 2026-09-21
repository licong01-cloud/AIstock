from __future__ import annotations

import json
from typing import Any

import pytest

from scripts import monthly_unified_dataset_release as cli


OPERATION_ID = "dmr_" + "a" * 32
AUTHORIZATION_REF = "dsauth_" + "b" * 32


def _capture(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake_call(**kwargs: Any) -> dict[str, Any]:
        calls.append(dict(kwargs))
        return {"ok": True}

    monkeypatch.setattr(cli, "_call", fake_call)
    return calls


def test_plan_calls_the_single_backend_api_without_activation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls = _capture(monkeypatch)

    assert (
        cli.main(
            [
                "--api-root",
                "https://dataset.example/api/v1",
                "plan",
                "--cutoff",
                "2026-09-30",
                "--idempotency-key",
                "monthly-202609",
            ]
        )
        == 0
    )

    assert calls == [
        {
            "root": "https://dataset.example/api/v1",
            "method": "POST",
            "suffix": "/plan",
            "body": {
                "schema_version": "aistock_monthly_release_request_v1",
                "target_cutoff": "2026-09-30",
                "product_profile": "qe_hmm_full_v2",
                "activation_mode": "prepare_only",
                "activation_authorization_ref": None,
                "repair_authorization_refs": [],
            },
            "idempotency": "monthly-202609",
        }
    ]
    assert json.loads(capsys.readouterr().out) == {"ok": True}


def test_run_can_request_exact_auto_activation_and_repair_authorizations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _capture(monkeypatch)
    repair_ref = "dsauth_" + "c" * 32

    assert (
        cli.main(
            [
                "run",
                "--cutoff",
                "2026-09-30",
                "--idempotency-key",
                "monthly-202609",
                "--repair-authorization-ref",
                repair_ref,
                "--activate",
                "--authorization-ref",
                AUTHORIZATION_REF,
            ]
        )
        == 0
    )

    assert calls[0]["suffix"] == ""
    assert calls[0]["body"]["activation_mode"] == "activate_when_ready"
    assert calls[0]["body"]["activation_authorization_ref"] == AUTHORIZATION_REF
    assert calls[0]["body"]["repair_authorization_refs"] == [repair_ref]


@pytest.mark.parametrize(
    "arguments",
    (
        ["run", "--cutoff", "2026-09-30", "--idempotency-key", "x", "--activate"],
        [
            "run",
            "--cutoff",
            "2026-09-30",
            "--idempotency-key",
            "x",
            "--authorization-ref",
            AUTHORIZATION_REF,
        ],
        ["status", "--operation-id", "../escape"],
        [
            "activate",
            "--operation-id",
            OPERATION_ID,
            "--authorization-ref",
            "not-an-authorization",
        ],
    ),
)
def test_cli_rejects_incomplete_or_noncanonical_action_inputs(arguments: list[str]) -> None:
    with pytest.raises(ValueError):
        cli.main(arguments)


@pytest.mark.parametrize(
    ("command", "method", "suffix", "body"),
    (
        ("status", "GET", f"/{OPERATION_ID}", None),
        ("receipts", "GET", f"/{OPERATION_ID}/receipts", None),
        (
            "resume",
            "POST",
            f"/{OPERATION_ID}/resume",
            {"schema_version": "dataset_release_command_request_v1"},
        ),
        (
            "cancel",
            "POST",
            f"/{OPERATION_ID}/cancel",
            {"schema_version": "dataset_release_command_request_v1"},
        ),
    ),
)
def test_operation_commands_route_without_accepting_paths_or_stage_controls(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    method: str,
    suffix: str,
    body: dict[str, str] | None,
) -> None:
    calls = _capture(monkeypatch)

    assert cli.main([command, "--operation-id", OPERATION_ID]) == 0

    assert calls[0]["method"] == method
    assert calls[0]["suffix"] == suffix
    assert calls[0]["body"] == body
    assert calls[0]["idempotency"] is None


@pytest.mark.parametrize("command", ("activate", "rollback"))
def test_privileged_actions_send_only_the_exact_authorization_reference(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    calls = _capture(monkeypatch)

    assert (
        cli.main(
            [
                command,
                "--operation-id",
                OPERATION_ID,
                "--authorization-ref",
                AUTHORIZATION_REF,
            ]
        )
        == 0
    )

    assert calls[0]["suffix"] == f"/{OPERATION_ID}/{command}"
    assert calls[0]["body"] == {
        "schema_version": "aistock_monthly_release_action_v1",
        "authorization_ref": AUTHORIZATION_REF,
    }


@pytest.mark.parametrize(
    "root",
    (
        "file:///tmp/api",
        "http://example.test/api?token=secret",
        "http://example.test/api#fragment",
        "https://token@example.test/api",
        "/relative/api",
    ),
)
def test_api_root_must_be_an_absolute_http_url_without_secret_bearing_suffixes(root: str) -> None:
    with pytest.raises(ValueError):
        cli._api_url(root, "")


def test_token_file_rejects_control_characters_and_excessive_values(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = tmp_path / "operator.token"
    monkeypatch.setenv("DATASET_RELEASE_OPERATOR_TOKEN_FILE", str(token.absolute()))
    token.write_text("x" * 31, encoding="utf-8")
    with pytest.raises(RuntimeError):
        cli._token()
    token.write_text("x" * 32 + "\ninside", encoding="utf-8")
    with pytest.raises(RuntimeError):
        cli._token()
    token.write_text("x" * 4097, encoding="utf-8")
    with pytest.raises(RuntimeError):
        cli._token()
