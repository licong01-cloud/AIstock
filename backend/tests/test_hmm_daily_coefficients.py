from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from backend.services.hmm_training_service import HMM_DAILY_COEFFICIENT_MODE, HMMTrainingService


class _Proc:
    returncode = 0
    stderr = "generated"


def _service_with_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[HMMTrainingService, Path]:
    model_path = tmp_path / "models.json"
    model_path.write_text('{"801010.SI": {}}', encoding="utf-8")
    svc = HMMTrainingService()
    monkeypatch.setattr(
        svc,
        "get_snapshot",
        lambda snapshot_id: {
            "snapshot_id": snapshot_id,
            "config_id": "cfg_1",
            "display_name": "HMM snapshot",
            "status": "COMPLETED",
            "model_path": str(model_path),
        },
    )
    monkeypatch.setattr(
        svc,
        "_get_config",
        lambda config_id: {
            "config_id": config_id,
            "display_name": "HMM config",
            "model_type": "sector_hmm",
            "config_json": {
                "method": "pup",
                "horizon_weights": {"5": 0.2, "10": 0.3, "20": 0.5},
                "signal_presets": {
                    "preset_A": {"trending": 1.05, "neutral": 1.0, "fading": 0.96},
                    "preset_nested": {"coefficients": {"1": {"trending": 1.02, "neutral": 1.0, "fading": 0.98}}},
                }
            },
        },
    )
    monkeypatch.setattr(
        svc,
        "_latest_completed_hmm_data_date",
        lambda as_of_date: (
            date(2026, 4, 27),
            {
                "sector_data": date(2026, 4, 27),
                "sw_daily": date(2026, 4, 27),
                "index_daily_000300": date(2026, 4, 27),
            },
        ),
    )

    def list_trading_days(start: date, end: date) -> list[date]:
        days = [date(2026, 4, 27), date(2026, 4, 28), date(2026, 4, 29)]
        selected = [item for item in days if start <= item <= end]
        if not selected:
            raise RuntimeError("no trading days")
        return selected

    monkeypatch.setattr(svc, "_list_trading_days", list_trading_days)
    return svc, model_path


def test_extract_signal_preset_coefficients_supports_nested_shape() -> None:
    coeffs = HMMTrainingService._extract_signal_preset_coefficients(
        {"signal_presets": {"preset_X": {"coefficients": {"1": {"trending": "1.02", "neutral": 1, "fading": 0.98}}}}},
        "preset_X",
    )

    assert coeffs == {"trending": 1.02, "neutral": 1.0, "fading": 0.98}


def test_preview_daily_coefficients_uses_latest_asof_and_next_trading_day(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    svc, model_path = _service_with_snapshot(tmp_path, monkeypatch)

    plan = svc.preview_daily_coefficients("snapshot_1", signal_preset="preset_A")

    assert plan["as_of_trade_date"] == "2026-04-27"
    assert plan["effective_trade_date"] == "2026-04-28"
    assert plan["generation_mode"] == HMM_DAILY_COEFFICIENT_MODE
    assert plan["output_filename"] == "coefficients_preset_A_2026-04-28_2026-04-28.json"
    assert plan["output_path"] == str(model_path.parent / plan["output_filename"])
    assert plan["existing_artifact"] is False


def test_preview_daily_coefficients_rejects_same_day_effective(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    svc, _ = _service_with_snapshot(tmp_path, monkeypatch)

    with pytest.raises(ValueError, match="effective_trade_date must be later"):
        svc.preview_daily_coefficients(
            "snapshot_1",
            signal_preset="preset_A",
            as_of_date=date(2026, 4, 27),
            effective_trade_date=date(2026, 4, 27),
        )


def test_generate_daily_coefficients_is_idempotent_for_matching_existing_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    svc, model_path = _service_with_snapshot(tmp_path, monkeypatch)
    output_path = model_path.parent / "coefficients_preset_A_2026-04-28_2026-04-28.json"
    output_path.write_text(json.dumps({
        "generation_mode": HMM_DAILY_COEFFICIENT_MODE,
        "snapshot_id": "snapshot_1",
        "config_id": "cfg_1",
        "preset_key": "preset_A",
        "as_of_trade_date": "2026-04-27",
        "effective_trade_date": "2026-04-28",
        "daily_coefficients": {"2026-04-28": {"801010.SI": 1.05}},
        "stock_sector_map": {"600000.SH": "801010.SI"},
    }, ensure_ascii=False), encoding="utf-8")

    def fail_run(*args, **kwargs):  # pragma: no cover - should not be called
        raise AssertionError("subprocess must not run when matching artifact already exists")

    monkeypatch.setattr("backend.services.hmm_training_service.subprocess.run", fail_run)

    result = svc.generate_daily_coefficients(
        "snapshot_1",
        signal_preset="preset_A",
        confirm_text="snapshot_1",
    )

    assert result["status"] == "EXISTS"
    assert result["created"] is False
    assert len(result["artifact_sha256"]) == 64


def test_generate_daily_coefficients_passes_pit_dates_to_wsl_script(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    svc, model_path = _service_with_snapshot(tmp_path, monkeypatch)
    output_path = model_path.parent / "coefficients_preset_A_2026-04-28_2026-04-28.json"
    captured = {}

    def fake_run(cmd, input, **kwargs):
        params = json.loads(input)
        captured.update({"cmd": cmd, "params": params})
        output_path.write_text(json.dumps({
            "generation_mode": HMM_DAILY_COEFFICIENT_MODE,
            "snapshot_id": "snapshot_1",
            "config_id": "cfg_1",
            "preset_key": "preset_A",
            "as_of_trade_date": "2026-04-27",
            "effective_trade_date": "2026-04-28",
            "daily_coefficients": {"2026-04-28": {"801010.SI": 1.05}},
            "stock_sector_map": {"600000.SH": "801010.SI"},
        }, ensure_ascii=False), encoding="utf-8")
        return _Proc()

    monkeypatch.setattr("backend.services.hmm_training_service.subprocess.run", fake_run)

    result = svc.generate_daily_coefficients(
        "snapshot_1",
        signal_preset="preset_A",
        confirm_text="snapshot_1",
    )

    assert result["status"] == "CREATED"
    assert result["created"] is True
    assert captured["params"]["test_start"] == "2026-04-27"
    assert captured["params"]["backtest_end"] == "2026-04-27"
    assert captured["params"]["as_of_trade_date"] == "2026-04-27"
    assert captured["params"]["output_trade_date"] == "2026-04-28"
    assert captured["params"]["generation_mode"] == HMM_DAILY_COEFFICIENT_MODE
    assert captured["params"]["config_json"]["method"] == "pup"
    assert captured["params"]["config_json"]["horizon_weights"]["20"] == pytest.approx(0.5)
    assert captured["params"]["preset_coeffs"]["trending"] == pytest.approx(1.05)


def test_start_daily_coefficients_job_persists_validated_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    svc, _ = _service_with_snapshot(tmp_path, monkeypatch)
    captured: dict[str, Any] = {}

    def fake_insert(plan: dict[str, Any]) -> dict[str, Any]:
        captured.update(plan)
        return {
            "job_id": "job_1",
            "snapshot_id": plan["snapshot_id"],
            "config_id": plan["config_id"],
            "signal_preset": plan["signal_preset"],
            "as_of_trade_date": plan["as_of_trade_date"],
            "effective_trade_date": plan["effective_trade_date"],
            "generation_mode": plan["generation_mode"],
            "status": "PENDING",
            "result_status": None,
        }

    monkeypatch.setattr(svc, "_insert_daily_coefficient_job", fake_insert)

    job = svc.start_daily_coefficients_job(
        "snapshot_1",
        signal_preset="preset_A",
        confirm_text="snapshot_1",
    )

    assert job["status"] == "PENDING"
    assert captured["as_of_trade_date"] == "2026-04-27"
    assert captured["effective_trade_date"] == "2026-04-28"
    assert captured["generation_mode"] == HMM_DAILY_COEFFICIENT_MODE


def test_run_daily_coefficients_job_marks_completed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    svc, _ = _service_with_snapshot(tmp_path, monkeypatch)
    events: dict[str, Any] = {}
    job = {
        "job_id": "job_1",
        "snapshot_id": "snapshot_1",
        "config_id": "cfg_1",
        "signal_preset": "preset_A",
        "as_of_trade_date": "2026-04-27",
        "effective_trade_date": "2026-04-28",
        "status": "PENDING",
    }

    monkeypatch.setattr(svc, "get_daily_coefficient_job", lambda job_id: job)
    monkeypatch.setattr(svc, "_mark_daily_coefficient_job_running", lambda job_id: events.update({"running": job_id}))

    def fake_execute(plan: dict[str, Any]) -> dict[str, Any]:
        return {**plan, "status": "CREATED", "artifact_sha256": "a" * 64}

    monkeypatch.setattr(svc, "_execute_daily_coefficient_plan", fake_execute)
    monkeypatch.setattr(svc, "_complete_daily_coefficient_job", lambda job_id, result: events.update({"completed": (job_id, result)}))

    svc.run_daily_coefficients_job("job_1")

    assert events["running"] == "job_1"
    assert events["completed"][0] == "job_1"
    assert events["completed"][1]["status"] == "CREATED"
    assert events["completed"][1]["artifact_sha256"] == "a" * 64


def test_run_daily_coefficients_job_marks_failed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    svc, _ = _service_with_snapshot(tmp_path, monkeypatch)
    events: dict[str, Any] = {}
    job = {
        "job_id": "job_1",
        "snapshot_id": "snapshot_1",
        "config_id": "cfg_1",
        "signal_preset": "preset_A",
        "as_of_trade_date": "2026-04-27",
        "effective_trade_date": "2026-04-28",
        "status": "PENDING",
    }

    monkeypatch.setattr(svc, "get_daily_coefficient_job", lambda job_id: job)
    monkeypatch.setattr(svc, "_mark_daily_coefficient_job_running", lambda job_id: events.update({"running": job_id}))
    monkeypatch.setattr(svc, "_execute_daily_coefficient_plan", lambda plan: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(svc, "_fail_daily_coefficient_job", lambda job_id, message, context=None: events.update({"failed": (job_id, message, context)}))

    svc.run_daily_coefficients_job("job_1")

    assert events["running"] == "job_1"
    assert events["failed"][0] == "job_1"
    assert "boom" in events["failed"][1]
    assert events["failed"][2]["exception_type"] == "RuntimeError"
