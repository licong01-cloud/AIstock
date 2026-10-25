from __future__ import annotations

import json
from contextlib import nullcontext
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
                "qe_frozen_input_bundle": {
                    "schema_version": "qe_hmm_frozen_input_v1",
                    "dataset_root": "/mnt/x/qe-release",
                    "files": {"fixture": {"relative_path": "fixture", "sha256": "0" * 64}},
                },
                "signal_presets": {
                    "preset_A": {"trending": 1.05, "neutral": 1.0, "fading": 0.96},
                    "preset_nested": {"coefficients": {"1": {"trending": 1.02, "neutral": 1.0, "fading": 0.98}}},
                },
            },
        },
    )
    monkeypatch.setattr(
        svc,
        "_database_daily_completion",
        lambda as_of_date: (
            date(2026, 4, 27),
            {
                "completion_authority": "market.dataset_date_refresh_audit",
                "max_dates": {
                    "sector_data": "2026-04-27",
                    "sw_daily": "2026-04-27",
                    "index_daily_000300": "2026-04-27",
                },
                "max_dates_diagnostic_only": True,
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
    monkeypatch.setattr(
        "backend.services.hmm_data_source.daily_input.validate_database_models", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        svc,
        "_database_daily_inputs",
        lambda **kwargs: {
            "ordered_sector_codes": ["801010.SI"],
            "input_identity": {
                "input_sha256": "1" * 64,
                "read_cutoff_at": "2026-04-28T00:00:00+00:00",
            },
        },
    )
    monkeypatch.setattr(svc, "_daily_execution_lock", lambda job_id: nullcontext(True))
    return svc, model_path


def test_extract_signal_preset_coefficients_supports_nested_shape() -> None:
    coeffs = HMMTrainingService._extract_signal_preset_coefficients(
        {"signal_presets": {"preset_X": {"coefficients": {"1": {"trending": "1.02", "neutral": 1, "fading": 0.98}}}}},
        "preset_X",
    )

    assert coeffs == {"trending": 1.02, "neutral": 1.0, "fading": 0.98}


def test_qe_frozen_coefficient_bundle_rejects_database_fallback() -> None:
    with pytest.raises(RuntimeError, match="database-backed market data is not an allowed QE fallback"):
        HMMTrainingService._qe_frozen_coefficient_bundle({})


def test_fill_default_config_preserves_only_explicit_frozen_qe_binding() -> None:
    frozen = {
        "schema_version": "qe_hmm_frozen_input_v1",
        "dataset_root": "/mnt/x/qe-release",
        "files": {},
    }

    result = HMMTrainingService._fill_default_config(
        {"n_states": 3, "qe_frozen_input_bundle": frozen, "unexpected": "discard"}
    )

    assert result["qe_frozen_input_bundle"] == frozen
    assert "unexpected" not in result


def test_extract_signal_preset_coefficients_ignores_metadata_keys() -> None:
    coeffs = HMMTrainingService._extract_signal_preset_coefficients(
        {
            "signal_presets": {
                "preset_A": {
                    "label": "HMM preset A",
                    "description": "operator-facing metadata must not be parsed as a coefficient",
                    "coefficients": {
                        "1": {"trending": "1.05", "neutral": 1.0, "fading": 0.96},
                    },
                }
            }
        },
        "preset_A",
    )

    assert coeffs == {"trending": 1.05, "neutral": 1.0, "fading": 0.96}


def test_extract_signal_preset_coefficients_ignores_flat_metadata_without_coefficients_key() -> None:
    coeffs = HMMTrainingService._extract_signal_preset_coefficients(
        {
            "signal_presets": {
                "preset_A": {
                    "label": "HMM preset A",
                    "trending": "1.05",
                    "neutral": 1.0,
                    "fading": 0.96,
                }
            }
        },
        "preset_A",
    )

    assert coeffs == {"trending": 1.05, "neutral": 1.0, "fading": 0.96}


def test_extract_signal_preset_coefficients_rejects_builtin_metadata_only_preset_by_default() -> None:
    with pytest.raises(ValueError, match="HMM signal_preset has no coefficients: preset_A"):
        HMMTrainingService._extract_signal_preset_coefficients(
            {
                "signal_presets": {
                    "preset_A": {
                        "label": "Preset A",
                        "description": "metadata-only runtime preset",
                    }
                }
            },
            "preset_A",
        )


def test_extract_signal_preset_coefficients_can_use_default_for_selection_auto_generation() -> None:
    coeffs = HMMTrainingService._extract_signal_preset_coefficients(
        {
            "signal_presets": {
                "preset_A": {
                    "label": "Preset A",
                    "description": "metadata-only runtime preset",
                }
            }
        },
        "preset_A",
        allow_builtin_metadata_default=True,
    )

    assert coeffs == {"trending": 1.05, "neutral": 1.0, "fading": 0.96}


def test_extract_signal_preset_coefficients_uses_metadata_range_for_precomputed_builtin_metadata() -> None:
    coeffs = HMMTrainingService._extract_signal_preset_coefficients(
        {
            "signal_presets": {
                "preset_A": {
                    "label": "Stage3 sparse penalty preset_A only",
                    "description": "Precomputed sparse coefficients for QE default split only.",
                    "coefficients": {
                        "precomputed_daily": True,
                        "source_score_column": "score",
                        "score_method": "sparse_penalty",
                        "range": [0.955, 1.0],
                    },
                }
            }
        },
        "preset_A",
        allow_builtin_metadata_default=True,
    )

    assert coeffs == {"trending": 1.0, "neutral": 1.0, "fading": 0.955}


def test_extract_signal_preset_coefficients_rejects_custom_metadata_only_preset() -> None:
    with pytest.raises(ValueError, match="HMM signal_preset has no coefficients: custom_A"):
        HMMTrainingService._extract_signal_preset_coefficients(
            {
                "signal_presets": {
                    "custom_A": {
                        "label": "Custom preset",
                        "description": "metadata-only custom preset must fail fast",
                    }
                }
            },
            "custom_A",
        )


def test_preview_daily_coefficients_uses_latest_asof_and_next_trading_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    svc, model_path = _service_with_snapshot(tmp_path, monkeypatch)

    plan = svc.preview_daily_coefficients("snapshot_1", signal_preset="preset_A")

    assert plan["as_of_trade_date"] == "2026-04-27"
    assert plan["effective_trade_date"] == "2026-04-28"
    assert plan["generation_mode"] == HMM_DAILY_COEFFICIENT_MODE
    assert plan["output_filename"].startswith("coefficients_preset_A_2026-04-28_2026-04-28__daily_")
    assert plan["output_path"] == str(model_path.parent / plan["output_filename"])
    assert plan["existing_artifact"] is False
    assert plan["source_mode"] == "database_daily"
    assert plan["input_validation_status"] == "VALIDATED"
    assert plan["generation_available"] is True
    assert plan["blocking_reason"] is None


def test_preview_daily_coefficients_accepts_precomputed_builtin_metadata_preset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    svc, _ = _service_with_snapshot(tmp_path, monkeypatch)
    monkeypatch.setattr(
        svc,
        "_get_config",
        lambda config_id: {
            "config_id": config_id,
            "display_name": "HMM precomputed metadata config",
            "model_type": "sector_hmm",
            "config_json": {
                "runtime_generation_supported": False,
                "precomputed_only": True,
                "signal_presets": {
                    "preset_A": {
                        "label": "Stage3 sparse penalty preset_A only",
                        "description": "Precomputed sparse coefficients for QE default split only.",
                        "coefficients": {
                            "precomputed_daily": True,
                            "source_score_column": "score",
                            "score_method": "sparse_penalty",
                            "range": [0.955, 1.0],
                        },
                    }
                },
            },
        },
    )

    plan = svc.preview_daily_coefficients(
        "snapshot_1",
        signal_preset="preset_A",
        as_of_date=date(2026, 4, 27),
        effective_trade_date=date(2026, 4, 28),
    )

    assert plan["preset_coeffs"] == {"trending": 1.0, "neutral": 1.0, "fading": 0.955}
    assert "__daily_" in plan["output_filename"]


def test_preview_daily_coefficients_rejects_same_day_effective(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    svc, _ = _service_with_snapshot(tmp_path, monkeypatch)

    with pytest.raises(ValueError, match="effective_trade_date must be after"):
        svc.preview_daily_coefficients(
            "snapshot_1",
            signal_preset="preset_A",
            as_of_date=date(2026, 4, 27),
            effective_trade_date=date(2026, 4, 27),
        )


def test_daily_plan_does_not_reuse_or_overwrite_old_unbound_asset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    svc, model_path = _service_with_snapshot(tmp_path, monkeypatch)
    output_path = model_path.parent / "coefficients_preset_A_2026-04-28_2026-04-28.json"
    output_path.write_text(
        json.dumps(
            {
                "generation_mode": HMM_DAILY_COEFFICIENT_MODE,
                "snapshot_id": "snapshot_1",
                "config_id": "cfg_1",
                "preset_key": "preset_A",
                "as_of_trade_date": "2026-04-27",
                "effective_trade_date": "2026-04-28",
                "daily_coefficients": {"2026-04-28": {"801010.SI": 1.05}},
                "stock_sector_map": {"600000.SH": "801010.SI"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fail_run(*args, **kwargs):  # pragma: no cover - should not be called
        raise AssertionError("subprocess must not run when matching artifact already exists")

    monkeypatch.setattr("backend.services.hmm_training_service.subprocess.run", fail_run)

    before = output_path.read_bytes()
    plan = svc.preview_daily_coefficients("snapshot_1", signal_preset="preset_A")
    assert plan["existing_artifact"] is False
    assert plan["output_path"] != str(output_path)
    assert output_path.read_bytes() == before


def test_generate_daily_coefficients_never_dispatches_database_day_to_file_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    svc, model_path = _service_with_snapshot(tmp_path, monkeypatch)
    output_path = model_path.parent / "coefficients_preset_A_2026-04-28_2026-04-28.json"
    captured = {}

    def fake_run(cmd, input, **kwargs):
        params = json.loads(input)
        captured.update({"cmd": cmd, "params": params})
        output_path.write_text(
            json.dumps(
                {
                    "generation_mode": HMM_DAILY_COEFFICIENT_MODE,
                    "snapshot_id": "snapshot_1",
                    "config_id": "cfg_1",
                    "preset_key": "preset_A",
                    "as_of_trade_date": "2026-04-27",
                    "effective_trade_date": "2026-04-28",
                    "daily_coefficients": {"2026-04-28": {"801010.SI": 1.05}},
                    "stock_sector_map": {"600000.SH": "801010.SI"},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return _Proc()

    monkeypatch.setattr("backend.services.hmm_training_service.subprocess.run", fake_run)
    monkeypatch.setattr(
        svc,
        "_database_daily_inputs",
        lambda **kwargs: (_ for _ in ()).throw(
            RuntimeError("hmm_database_daily_input_unavailable: missing required fact")
        ),
    )
    with pytest.raises(RuntimeError, match="hmm_database_daily_input_unavailable"):
        svc.generate_daily_coefficients(
            "snapshot_1",
            signal_preset="preset_A",
            confirm_generate=True,
        )
    assert captured == {}
    assert not output_path.exists()


def test_snapshot_precompute_passes_only_frozen_market_inputs(tmp_path: Path, monkeypatch) -> None:
    model_path = tmp_path / "models.json"
    model_path.write_text('{"801010.SI": {}}', encoding="utf-8")
    captured: list[dict[str, Any]] = []

    def fake_run(_cmd, input, **_kwargs):
        captured.append(json.loads(input))
        return _Proc()

    monkeypatch.setattr("backend.services.hmm_training_service.subprocess.run", fake_run)
    HMMTrainingService()._precompute_coefficients_for_snapshot(
        str(model_path),
        {
            "coefficient_start": "2026-06-01",
            "coefficient_end": "2026-06-30",
            "qe_frozen_input_bundle": {
                "schema_version": "qe_hmm_frozen_input_v1",
                "dataset_root": "/mnt/x/qe-release",
                "files": {"fixture": {}},
            },
        },
    )

    assert len(captured) == 2
    assert all(item["source_mode"] == "qe_frozen_file" for item in captured)
    assert all(item["model_sha256"] == HMMTrainingService._file_sha256(str(model_path)) for item in captured)
    assert all(item["frozen_input_bundle"]["dataset_root"] == "/mnt/x/qe-release" for item in captured)
    assert all(not any(key.startswith("db_") for key in item) for item in captured)


def test_start_daily_coefficients_job_rejects_unvalidated_db_input_before_insert(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    monkeypatch.setattr(svc, "_resolve_daily_coefficient_plan", lambda **kwargs: {"generation_available": False})

    with pytest.raises(RuntimeError, match="no daily job has been created"):
        svc.start_daily_coefficients_job(
            "snapshot_1",
            signal_preset="preset_A",
            confirm_generate=True,
        )
    assert captured == {}


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
        "plan_json": {"daily_key": "key", "source_mode": "database_daily"},
    }

    monkeypatch.setattr(svc, "get_daily_coefficient_job", lambda job_id: job)
    monkeypatch.setattr(svc, "_mark_daily_coefficient_job_running", lambda job_id: events.update({"running": job_id}))

    def fake_execute(plan: dict[str, Any]) -> dict[str, Any]:
        return {**plan, "status": "CREATED", "artifact_sha256": "a" * 64}

    monkeypatch.setattr(svc, "_execute_daily_coefficient_plan", fake_execute)
    monkeypatch.setattr(
        svc, "_complete_daily_coefficient_job", lambda job_id, result: events.update({"completed": (job_id, result)})
    )

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
        "plan_json": {"daily_key": "key", "source_mode": "database_daily"},
    }

    monkeypatch.setattr(svc, "get_daily_coefficient_job", lambda job_id: job)
    monkeypatch.setattr(svc, "_mark_daily_coefficient_job_running", lambda job_id: events.update({"running": job_id}))
    monkeypatch.setattr(
        svc, "_execute_daily_coefficient_plan", lambda plan: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    monkeypatch.setattr(
        svc,
        "_fail_daily_coefficient_job",
        lambda job_id, message, context=None: events.update({"failed": (job_id, message, context)}),
    )

    svc.run_daily_coefficients_job("job_1")

    assert events["running"] == "job_1"
    assert events["failed"][0] == "job_1"
    assert "boom" in events["failed"][1]
    assert events["failed"][2]["exception_type"] == "RuntimeError"


def test_manual_daily_api_keeps_later_trading_day_compatibility(tmp_path, monkeypatch):
    svc, _ = _service_with_snapshot(tmp_path, monkeypatch)
    plan = svc.preview_daily_coefficients(
        "snapshot_1",
        signal_preset="preset_A",
        as_of_date=date(2026, 4, 27),
        effective_trade_date=date(2026, 4, 29),
    )
    assert plan["effective_trade_date"] == "2026-04-29"


@pytest.mark.parametrize(
    "status,acquired,execute",
    [("RUNNING", True, True), ("PENDING", False, False), ("COMPLETED", True, False), ("FAILED", True, False)],
)
def test_daily_job_uses_frozen_plan_and_exclusive_lock(tmp_path, monkeypatch, status, acquired, execute):
    svc, _ = _service_with_snapshot(tmp_path, monkeypatch)
    frozen = {"daily_key": "key", "source_mode": "database_daily", "as_of_trade_date": "2026-04-27"}
    job = {"status": status, "plan_json": frozen}
    monkeypatch.setattr(svc, "get_daily_coefficient_job", lambda job_id: job)
    monkeypatch.setattr(svc, "_daily_execution_lock", lambda job_id: nullcontext(acquired))
    monkeypatch.setattr(svc, "_mark_daily_coefficient_job_running", lambda job_id: None)
    monkeypatch.setattr(svc, "_resolve_daily_coefficient_plan", lambda **k: pytest.fail("replanning forbidden"))
    seen = []
    monkeypatch.setattr(
        svc, "_execute_daily_coefficient_plan", lambda plan: seen.append(plan) or {"status": "GENERATED"}
    )
    monkeypatch.setattr(svc, "_complete_daily_coefficient_job", lambda *a: None)
    svc.run_daily_coefficients_job("job")
    assert seen == ([frozen] if execute else [])
