from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.runtime_profile import RuntimeHMMProfile
from backend.services.trading_core.errors import ArtifactGenerationFailedError, HMMRuntimeUnavailableError


TRADE_DATE = date(2026, 6, 1)


class _SnapshotProvider:
    def __init__(self, model_path: Path) -> None:
        self.model_path = model_path

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        return {
            "snapshot_id": snapshot_id,
            "model_path": str(self.model_path),
            "status": "completed",
        }


class _FailingGenerationProvider(_SnapshotProvider):
    def generate_daily_coefficients(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise ValueError("HMM signal_preset has no coefficients: preset_A")


def _model_path(tmp_path: Path) -> Path:
    model_path = tmp_path / "models.json"
    model_path.write_text("{}", encoding="utf-8")
    return model_path


def _profile(**overrides: Any) -> RuntimeHMMProfile:
    payload: dict[str, Any] = {
        "enabled": True,
        "model_snapshot_id": "hmm_001",
        "signal_preset": "preset_A",
        "auto_compute": False,
    }
    payload.update(overrides)
    return RuntimeHMMProfile.model_validate(payload)


def _write_coefficients(tmp_path: Path, payload: dict[str, Any], *, name: str = "coefficients_preset_A_2026-06-01_2026-06-01.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _valid_payload() -> dict[str, Any]:
    return {
        "preset_key": "preset_A",
        "daily_coefficients": {
            TRADE_DATE.isoformat(): {
                "801780.SI": 1.05,
                "801750.SI": "0.98",
            }
        },
        "stock_sector_map": {
            "000001.SZ": "801780.SI",
            "000002.SZ": "801750.SI",
        },
    }


def test_preflight_coefficients_returns_ready_context_for_valid_artifact(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    coeff_path = _write_coefficients(tmp_path, _valid_payload())
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    result = runtime.preflight_coefficients(
        trade_date=TRADE_DATE,
        profile=_profile(),
        package_id="pkg_hmm_preflight_ok",
    )

    assert result["enabled"] is True
    assert result["snapshot_id"] == "hmm_001"
    assert result["signal_preset"] == "preset_A"
    assert result["coefficients_path"] == str(coeff_path)
    assert result["sector_count"] == 2
    assert result["coefficient_count"] == 2
    assert result["stock_sector_map_count"] == 2


def test_preflight_coefficients_rejects_missing_daily_coefficients_key(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    coeff_path = _write_coefficients(
        tmp_path,
        {
            "preset_key": "preset_A",
            "stock_sector_map": {"000001.SZ": "801780.SI"},
        },
    )
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    with pytest.raises(HMMRuntimeUnavailableError, match="missing required keys") as exc_info:
        runtime.preflight_coefficients(
            trade_date=TRADE_DATE,
            profile=_profile(coefficients_path=str(coeff_path)),
            package_id="pkg_hmm_missing_coefficients",
        )

    assert exc_info.value.error_code == "HMM_RUNTIME_UNAVAILABLE"
    assert exc_info.value.context["coefficients_path"] == str(coeff_path)


def test_preflight_coefficients_rejects_no_trade_date_coverage(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    coeff_path = _write_coefficients(
        tmp_path,
        {
            "preset_key": "preset_A",
            "daily_coefficients": {"2026-05-29": {"801780.SI": 1.0}},
            "stock_sector_map": {"000001.SZ": "801780.SI"},
        },
    )
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    with pytest.raises(HMMRuntimeUnavailableError, match="no coefficients for trade_date") as exc_info:
        runtime.preflight_coefficients(
            trade_date=TRADE_DATE,
            profile=_profile(coefficients_path=str(coeff_path)),
            package_id="pkg_hmm_no_coverage",
        )

    assert exc_info.value.context["trade_date"] == TRADE_DATE.isoformat()
    assert exc_info.value.context["coefficients_path"] == str(coeff_path)


def test_preflight_coefficients_rejects_empty_trade_date_coefficients(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    coeff_path = _write_coefficients(
        tmp_path,
        {
            "preset_key": "preset_A",
            "daily_coefficients": {TRADE_DATE.isoformat(): {}},
            "stock_sector_map": {"000001.SZ": "801780.SI"},
        },
    )
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    with pytest.raises(HMMRuntimeUnavailableError, match="empty coefficients") as exc_info:
        runtime.preflight_coefficients(
            trade_date=TRADE_DATE,
            profile=_profile(coefficients_path=str(coeff_path)),
            package_id="pkg_hmm_empty_coefficients",
        )

    assert exc_info.value.context["trade_date"] == TRADE_DATE.isoformat()
    assert exc_info.value.context["coefficients_path"] == str(coeff_path)


def test_preflight_coefficients_rejects_non_numeric_sector_coefficient(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    coeff_path = _write_coefficients(
        tmp_path,
        {
            "preset_key": "preset_A",
            "daily_coefficients": {TRADE_DATE.isoformat(): {"801780.SI": "label"}},
            "stock_sector_map": {"000001.SZ": "801780.SI"},
        },
    )
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    with pytest.raises(HMMRuntimeUnavailableError, match="positive finite") as exc_info:
        runtime.preflight_coefficients(
            trade_date=TRADE_DATE,
            profile=_profile(coefficients_path=str(coeff_path)),
            package_id="pkg_hmm_non_numeric",
        )

    assert exc_info.value.context["sector_code"] == "801780.SI"
    assert exc_info.value.context["coefficient"] == "label"


def test_preflight_coefficients_rejects_empty_stock_sector_mapping_value(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    coeff_path = _write_coefficients(
        tmp_path,
        {
            "preset_key": "preset_A",
            "daily_coefficients": {TRADE_DATE.isoformat(): {"801780.SI": 1.0}},
            "stock_sector_map": {"000001.SZ": ""},
        },
    )
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    with pytest.raises(HMMRuntimeUnavailableError, match="empty stock sector mapping") as exc_info:
        runtime.preflight_coefficients(
            trade_date=TRADE_DATE,
            profile=_profile(coefficients_path=str(coeff_path)),
            package_id="pkg_hmm_empty_sector_mapping",
        )

    assert exc_info.value.context["symbol_samples"] == ["000001.SZ"]


def test_preflight_coefficients_rejects_stock_sector_without_coefficient_coverage(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    coeff_path = _write_coefficients(
        tmp_path,
        {
            "preset_key": "preset_A",
            "daily_coefficients": {TRADE_DATE.isoformat(): {"801780.SI": 1.0}},
            "stock_sector_map": {
                "000001.SZ": "801780.SI",
                "000002.SZ": "801750.SI",
            },
        },
    )
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    with pytest.raises(HMMRuntimeUnavailableError, match="without coefficients") as exc_info:
        runtime.preflight_coefficients(
            trade_date=TRADE_DATE,
            profile=_profile(coefficients_path=str(coeff_path)),
            package_id="pkg_hmm_missing_sector_coverage",
        )

    assert exc_info.value.context["missing_sector_samples"] == ["801750.SI"]


def test_preflight_coefficients_preserves_metadata_only_generation_failure_context(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    runtime = SectorHMMRuntime(snapshot_provider=_FailingGenerationProvider(model_path))

    with pytest.raises(ArtifactGenerationFailedError, match="auto-generation failed") as exc_info:
        runtime.preflight_coefficients(
            trade_date=TRADE_DATE,
            profile=_profile(auto_compute=True),
            package_id="pkg_hmm_metadata_only_preset",
        )

    assert exc_info.value.error_code == "ARTIFACT_GENERATION_FAILED"
    assert exc_info.value.context["reason"] == "missing_artifact"
    assert exc_info.value.context["error"] == "HMM signal_preset has no coefficients: preset_A"


def test_adjust_candidates_marks_existing_reason_as_hmm_adjusted(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    _write_coefficients(tmp_path, _valid_payload())
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    adjusted = runtime.adjust_candidates(
        candidates=[
            SelectionCandidate(
                symbol="000001.SZ",
                score=1.0,
                rank=1,
                component_scores={"artifact_source": "live_qe_model_inference"},
                reason="live_qe_model_inference_score",
            )
        ],
        trade_date=TRADE_DATE,
        profile=_profile(),
        package_id="pkg_hmm_reason",
        manifest_sha256="manifest_sha",
    )

    assert adjusted[0].reason == "live_qe_model_inference_score|hmm_adjusted"
    assert adjusted[0].component_scores["hmm"]["source_reason"] == "live_qe_model_inference_score"
