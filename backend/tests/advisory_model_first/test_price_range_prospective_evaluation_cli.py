from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prospective_price_evaluation_cli import main
from backend.services.advisory_model_first.prospective_price_evaluation_contracts import build_confirmation


def test_settle_cli_returns_three_for_not_mature(tmp_path: Path, capsys) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    service = SimpleNamespace(
        settle=lambda **_kwargs: (_ for _ in ()).throw(
            AdvisoryModelFirstError(
                "not ready",
                reason_code="ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE",
            )
        )
    )
    code = main(
        [
            "settle",
            "--env-file",
            str(env_file),
            "--model-root",
            str(tmp_path),
            "--request-id",
            "advprpros_" + "1" * 24,
        ],
        service_factory=lambda: service,
    )
    payload = json.loads(capsys.readouterr().err)
    assert code == 3
    assert payload["status"] == "WAITING"


def test_settle_cli_returns_two_for_typed_failure(tmp_path: Path, capsys) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    service = SimpleNamespace(
        settle=lambda **_kwargs: (_ for _ in ()).throw(
            AdvisoryModelFirstError(
                "bad identity",
                reason_code="ADVISORY_PRICE_PROSPECTIVE_OUTCOME_IDENTITY_MISMATCH",
            )
        )
    )
    code = main(
        [
            "settle",
            "--env-file",
            str(env_file),
            "--model-root",
            str(tmp_path),
            "--request-id",
            "advprpros_" + "1" * 24,
        ],
        service_factory=lambda: service,
    )
    payload = json.loads(capsys.readouterr().err)
    assert code == 2
    assert payload["status"] == "FAILED"


def test_aggregate_cli_writes_accumulating_contract(tmp_path: Path, capsys) -> None:
    bundle_id = "8" * 64

    def build(**_kwargs):
        return build_confirmation(
            status="ACCUMULATING",
            price_range_bundle_id=bundle_id,
            package_id=None,
            review_policy_sha256=None,
            target_trade_dates=(),
            target_date_count=0,
            available_candidate_count=0,
            not_applicable_count=0,
            support_date_count=0,
            support_date_ratio=0.0,
            support_gaps=("target_dates:0/20",),
            metrics=None,
            cluster_bootstrap=None,
        )

    output = tmp_path / "confirmation.json"
    code = main(
        [
            "aggregate",
            "--model-root",
            str(tmp_path),
            "--price-range-bundle-id",
            bundle_id,
            "--output",
            str(output),
        ],
        confirmation_builder=build,
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["status"] == "ACCUMULATING"
    assert payload["activation_recommended"] is False
    assert json.loads(output.read_text(encoding="utf-8"))["metrics"] is None


def test_settle_cli_returns_two_for_missing_explicit_env(tmp_path: Path, capsys) -> None:
    code = main(
        [
            "settle",
            "--env-file",
            str(tmp_path / "missing.env"),
            "--model-root",
            str(tmp_path),
            "--request-id",
            "advprpros_" + "1" * 24,
        ]
    )
    payload = json.loads(capsys.readouterr().err)
    assert code == 2
    assert payload["reason_code"] == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID"
