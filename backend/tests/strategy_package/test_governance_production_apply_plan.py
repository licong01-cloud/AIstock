from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.governance_production_apply_plan as apply_plan


def _without_generated_at(plan: dict) -> dict:
    stable = dict(plan)
    stable.pop("generated_at", None)
    return stable


def test_apply_plan_prepared_mode_accepts_token_and_env(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(apply_plan.ENV_PRODUCTION_PLAN, "true")

    exit_code = apply_plan.main(
        [
            "--prepare-production-plan",
            "--confirm-production-plan",
            apply_plan.CONFIRM_PRODUCTION_PLAN,
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "passed"
    assert payload["mode"] == "production_plan_prepared"
    assert payload["prepared_for_production"] is True
    assert payload["ddl_executed"] is False
    assert payload["db_writes_executed"] is False
    assert payload["required_confirmation_token_for_this_plan"] == apply_plan.CONFIRM_PRODUCTION_PLAN


def test_apply_plan_output_json_roundtrip(tmp_path: Path) -> None:
    output_path = tmp_path / "apply-plan.json"

    exit_code = apply_plan.main(["--output", str(output_path)])

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "aistock_qe_governance_production_apply_plan_v1"
    assert payload["mode"] == "static_preview"
    assert payload["ddl_executed"] is False
    assert payload["migration_apply_order"][-1] == "model_registry_phase5_20260509.sql"


def test_apply_plan_build_plan_has_stable_idempotent_fields() -> None:
    first = apply_plan.build_plan()
    second = apply_plan.build_plan()

    assert _without_generated_at(first) == _without_generated_at(second)
    assert first["generated_at"]
    assert second["generated_at"]


def test_apply_plan_static_smoke_failure_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail_static_smoke() -> object:
        raise apply_plan.migration_smoke.GovernanceMigrationSmokeError("static smoke failed")

    monkeypatch.setattr(apply_plan.migration_smoke, "run_static_smoke", fail_static_smoke)

    exit_code = apply_plan.main(["--json"])

    assert exit_code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "status": "failed",
        "mode": "production_apply_plan",
        "error": "static smoke failed",
    }


def test_apply_plan_operator_guard_failure_returns_3(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(apply_plan.ENV_PRODUCTION_PLAN, "true")

    exit_code = apply_plan.main(["--prepare-production-plan", "--confirm-production-plan", "wrong", "--json"])

    assert exit_code == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert payload["mode"] == "production_apply_plan"
    assert "--confirm-production-plan" in payload["error"]
