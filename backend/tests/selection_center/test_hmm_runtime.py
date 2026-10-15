from __future__ import annotations

import json
import hashlib
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.prospective_evidence import canonical_evidence_json_sha256
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


def test_preflight_coefficients_prefers_point_in_time_stock_sector_map(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    payload = _valid_payload()
    payload["stock_sector_map"] = {"000001.SZ": "FUTURE.SI"}
    payload["stock_sector_map_by_date"] = {
        TRADE_DATE.isoformat(): {
            "000001.SZ": "801780.SI",
            "000002.SZ": "801750.SI",
        }
    }
    coeff_path = _write_coefficients(tmp_path, payload)
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    result = runtime.preflight_coefficients(
        trade_date=TRADE_DATE,
        profile=_profile(coefficients_path=str(coeff_path)),
        package_id="pkg_hmm_pit_map",
    )

    assert result["stock_sector_map_count"] == 2


def test_preflight_coefficients_does_not_fallback_when_pit_map_omits_day(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    payload = _valid_payload()
    payload["stock_sector_map_by_date"] = {"2026-05-29": payload["stock_sector_map"]}
    coeff_path = _write_coefficients(tmp_path, payload)
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    with pytest.raises(HMMRuntimeUnavailableError, match="no point-in-time") as exc_info:
        runtime.preflight_coefficients(
            trade_date=TRADE_DATE,
            profile=_profile(coefficients_path=str(coeff_path)),
            package_id="pkg_hmm_pit_map_missing_day",
        )

    assert exc_info.value.context["trade_date"] == TRADE_DATE.isoformat()


def test_preflight_coefficients_hashes_merged_model_and_daily_input_watermarks(tmp_path: Path) -> None:
    model_path = _model_path(tmp_path)
    payload = _valid_payload()
    payload["input_data_max_dates_by_date"] = {
        TRADE_DATE.isoformat(): {
            "sector_data": TRADE_DATE.isoformat(),
            "index_daily": TRADE_DATE.isoformat(),
        }
    }
    coeff_path = _write_coefficients(tmp_path, payload)

    class _EvidenceProvider(_SnapshotProvider):
        def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
            snapshot = super().get_snapshot(snapshot_id)
            assert snapshot is not None
            snapshot["input_data_max_dates"] = {"model_training_panel": "2025-03-31"}
            return snapshot

    runtime = SectorHMMRuntime(snapshot_provider=_EvidenceProvider(model_path))
    result = runtime.preflight_coefficients(
        trade_date=TRADE_DATE,
        profile=_profile(coefficients_path=str(coeff_path)),
        package_id="pkg_hmm_input_watermark",
    )

    assert result["input_data_max_dates_hash"] == canonical_evidence_json_sha256(
        {
            "model_training_panel": "2025-03-31",
            "sector_data": TRADE_DATE.isoformat(),
            "index_daily": TRADE_DATE.isoformat(),
        }
    )


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


def test_adjust_candidates_can_explicitly_exclude_missing_sector_with_receipt(
    tmp_path: Path,
) -> None:
    model_path = _model_path(tmp_path)
    coeff_path = _write_coefficients(tmp_path, _valid_payload())
    runtime = SectorHMMRuntime(snapshot_provider=_SnapshotProvider(model_path))

    result = runtime.adjust_candidates_with_receipt(
        candidates=[
            SelectionCandidate(symbol="000001.SZ", score=1.0, rank=1),
            SelectionCandidate(symbol="000003.SZ", score=0.9, rank=2),
        ],
        trade_date=TRADE_DATE,
        profile=_profile(
            coefficients_path=str(coeff_path),
            missing_sector_policy="exclude_candidate",
        ),
        package_id="pkg_hmm_explicit_missing_sector_exclusion",
        manifest_sha256="manifest_sha",
        require_frozen_snapshot=True,
        effective_trade_date=TRADE_DATE,
        receipt_admissibility="FORMAL_FROZEN_HISTORICAL",
    )

    assert [candidate.symbol for candidate in result.candidates] == ["000001.SZ"]
    assert result.receipt.input_count == 2
    assert result.receipt.output_count == 1
    assert result.receipt.excluded_count == 1
    assert result.receipt.exclusions[0]["symbol"] == "000003.SZ"
    assert result.receipt.exclusions[0]["reason"] == "HMM_SECTOR_MAPPING_MISSING"
    assert result.hmm_metadata["missing_sector_policy"] == "exclude_candidate"
    assert result.hmm_metadata["missing_sector_excluded_count"] == 1


def _registered_no_overlay_runtime(
    tmp_path: Path, *, trade_date: date = TRADE_DATE, historical: bool = False,
):  # noqa: ANN202
    model_path = _model_path(tmp_path)
    day = trade_date.isoformat()
    as_of = day if historical else "2026-05-29"
    authority = {"authority_id": "test_frozen_authority", "authority_sha256": "a" * 64}
    identity = {
        "schema_version": "hmm_database_daily_input_v1",
        "source_mode": "database_daily",
        "as_of_trade_date": as_of,
        "effective_trade_date": day,
        "mapping_authority": authority,
    }
    payload = {
        **_valid_payload(),
        "source_mode": "qe_frozen_file" if historical else "database_daily",
        "generation_mode": "frozen_file" if historical else "daily_asof_prediction_v1",
        "snapshot_id": "hmm_001",
        "config_id": "config-1",
        "as_of_trade_date": as_of,
        "effective_trade_date": day,
        "model_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(),
        "l2_overlay_unavailable_policy": "explicit_no_l2_overlay_v1",
        "sector_code_map_schema": "aistock_release_sw_l2_code_map_v1",
        "sector_code_map_authority": authority,
        "sector_code_map_digest": "b" * 64,
        "quote_availability_schema": "aistock_release_sw_l2_quote_availability_v1",
        "quote_availability_authority": authority,
        "quote_availability_digest": "c" * 64,
        "quote_unavailable_sector_codes_by_date": {day: ["801750.SI"]},
        "input_identity": identity,
        "input_data_max_dates_by_date": {day: {"market": as_of}},
    }
    coefficients = payload["daily_coefficients"].pop(TRADE_DATE.isoformat())
    del coefficients["801750.SI"]
    payload["daily_coefficients"][day] = coefficients
    path = _write_coefficients(tmp_path, payload)
    window = {
        "source_mode": "database_daily", "role": "database_daily_prediction",
        "snapshot_id": "hmm_001", "config_id": "config-1", "preset": "preset_A",
        "test_start": day, "backtest_end": day,
        "as_of_trade_date": as_of, "model_sha256": payload["model_sha256"],
        "coefficient_filename": path.name,
        "coefficient_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "input_identity": identity,
    }

    class RegisteredProvider(_SnapshotProvider):
        def get_snapshot(self, snapshot_id: str) -> dict[str, Any]:
            return {**super().get_snapshot(snapshot_id), "config_id": "config-1",
                    "trained_at": "2026-05-28T00:00:00+00:00", "available_at": "2026-05-29T00:00:00+00:00",
                    "training_information_cutoff": "2026-05-28"}

        def get_config(self, config_id: str) -> dict[str, Any]:
            assert config_id == "config-1"
            return {"config_id": config_id, "config_json": {"coefficient_windows": [window]}}

        def generate_daily_coefficients(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise AssertionError("read-only HMM consumption must not generate coefficients")

    return SectorHMMRuntime(RegisteredProvider(model_path)), _profile(coefficients_path=str(path)), payload, path, window


def test_registered_no_overlay_preserves_scores_and_preflight_execution_agree(tmp_path: Path) -> None:
    runtime, profile, _payload, _path, _window = _registered_no_overlay_runtime(tmp_path)
    preflight = runtime.preflight_coefficients(
        trade_date=TRADE_DATE, profile=profile, package_id="advisory-probe", require_frozen_snapshot=True,
    )
    result = runtime.adjust_candidates_with_receipt(
        candidates=[SelectionCandidate(symbol="000001.SZ", score=2.0, rank=1, reason="alpha"),
                    SelectionCandidate(symbol="000002.SZ", score=3.0, rank=2, reason="alpha")],
        trade_date=TRADE_DATE, effective_trade_date=TRADE_DATE, profile=profile,
        package_id="advisory-probe", manifest_sha256="manifest", require_frozen_snapshot=True,
    )
    assert [(row.symbol, row.score) for row in result.candidates] == [("000002.SZ", 3.0), ("000001.SZ", 2.1)]
    info = result.candidates[0].component_scores["hmm"]
    assert info["overlay_applied"] is False and info["coefficient"] is None
    assert info["reason_code"] == "HMM_L2_QUOTES_UNAVAILABLE"
    assert "hmm_adjusted" not in result.candidates[0].reason
    assert result.candidates[1].reason == "alpha|hmm_adjusted"
    assert result.receipt.excluded_count == 0
    assert preflight["no_overlay_sector_codes"] == ["801750.SI"]
    assert result.hmm_metadata["no_overlay_candidate_count"] == 1
    assert preflight["as_of_trade_date"] == result.hmm_metadata["as_of_trade_date"] == "2026-05-29"
    assert preflight["effective_trade_date"] == TRADE_DATE.isoformat()


@pytest.mark.parametrize("mutation", [
    "unregistered_gap", "forged_policy", "hash_drift", "wrong_date", "wrong_authority",
    "nonfinite", "missing_mapping", "wrong_snapshot", "future_input",
])
def test_no_overlay_invalid_declarations_fail_in_both_paths(tmp_path: Path, mutation: str) -> None:
    runtime, profile, payload, path, _window = _registered_no_overlay_runtime(tmp_path)
    if mutation == "unregistered_gap":
        payload["quote_unavailable_sector_codes_by_date"][TRADE_DATE.isoformat()] = []
    elif mutation == "forged_policy":
        payload["l2_overlay_unavailable_policy"] = "neutral_fallback"
    elif mutation == "wrong_date":
        payload["effective_trade_date"] = "2026-06-02"
    elif mutation == "wrong_authority":
        payload["quote_availability_authority"] = {"authority_id": "forged", "authority_sha256": "d" * 64}
    elif mutation == "nonfinite":
        payload["daily_coefficients"][TRADE_DATE.isoformat()]["801780.SI"] = float("nan")
    elif mutation == "missing_mapping":
        payload["stock_sector_map"]["000002.SZ"] = None
    elif mutation == "wrong_snapshot":
        payload["snapshot_id"] = "another-snapshot"
    elif mutation == "future_input":
        payload["input_data_max_dates_by_date"][TRADE_DATE.isoformat()]["market"] = "2026-06-02"
    else:
        payload["quote_availability_digest"] = "d" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(HMMRuntimeUnavailableError):
        runtime.preflight_coefficients(trade_date=TRADE_DATE, profile=profile, package_id="probe", require_frozen_snapshot=True)
    with pytest.raises(HMMRuntimeUnavailableError):
        runtime.adjust_candidates_with_receipt(
            candidates=[SelectionCandidate(symbol="000002.SZ", score=3.0, rank=1)], trade_date=TRADE_DATE,
            effective_trade_date=TRADE_DATE, profile=profile, package_id="probe", manifest_sha256="manifest",
            require_frozen_snapshot=True,
        )


def test_localsim_uses_same_overlay_contract_and_never_generates(tmp_path: Path, monkeypatch) -> None:
    from types import SimpleNamespace

    from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
    from backend.services.hmm_training_service import HMMTrainingService
    from backend.services.simulation_runtime.localsim_selection_inference import (
        LOCALSIM_INFERENCE_SCOPE, LocalSimInferenceScope,
    )
    from backend.services.simulation_runtime.localsim_selection_signal import LocalSimReadOnlyHmmRuntime

    shared, profile, _payload, path, _window = _registered_no_overlay_runtime(tmp_path)
    provider = shared._snapshot_provider
    # Match the native LocalSIM snapshot-only provider (no get_config/generator).
    runtime = LocalSimReadOnlyHmmRuntime(SimpleNamespace(get_snapshot=provider.get_snapshot))
    monkeypatch.setattr(HMMTrainingService, "get_config", lambda self, config_id: provider.get_config(config_id))
    def forbidden(*_args, **_kwargs):
        raise AssertionError("LocalSIM must not generate coefficients even with auto_compute=True")
    monkeypatch.setattr(HMMTrainingService, "generate_daily_coefficients", forbidden)
    profile = profile.model_copy(update={"auto_compute": True})
    scope = LocalSimInferenceScope("localsim-own-run", tmp_path, SelectionSignalDayV1(
        TRADE_DATE, date(2026, 5, 29), TRADE_DATE), {})
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        preflight = runtime.preflight_coefficients(trade_date=TRADE_DATE, profile=profile, package_id="local-package")
        result = runtime.adjust_candidates_with_receipt(
            candidates=[SelectionCandidate(symbol="000002.SZ", score=3.0, rank=1)], trade_date=TRADE_DATE,
            profile=profile, effective_trade_date=TRADE_DATE, package_id="local-package", manifest_sha256="local-manifest",
        )
        assert result.candidates[0].score == 3.0
        assert result.candidates[0].component_scores["hmm"]["overlay_applied"] is False
        assert preflight["no_overlay_sector_codes"] == result.hmm_metadata["no_overlay_sector_codes"]
        assert scope.run_id == "localsim-own-run" and not scope.artifacts
        with pytest.raises(ValueError, match="own signal clocks"):
            runtime.adjust_candidates_with_receipt(
                candidates=result.candidates, trade_date=date(2026, 6, 2), profile=profile,
                package_id="local-package", manifest_sha256="local-manifest",
            )
        # A missing next-day asset cannot invoke the legacy auto-generation path.
        missing_profile = profile.model_copy(update={"coefficients_path": None})
        with pytest.raises(HMMRuntimeUnavailableError, match="missing"):
            runtime.preflight_coefficients(trade_date=date(2026, 6, 2), profile=missing_profile, package_id="local-package")
        assert path.exists()
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)


def test_hmm_disabled_does_not_read_assets_or_change_candidates() -> None:
    class ForbiddenProvider:
        def get_snapshot(self, *_args):
            raise AssertionError("disabled HMM must not read assets")

    runtime = SectorHMMRuntime(ForbiddenProvider())
    rows = [SelectionCandidate(symbol="000001.SZ", score=-0.2, rank=1, reason="alpha")]
    profile = RuntimeHMMProfile(enabled=False)
    assert runtime.preflight_coefficients(trade_date=TRADE_DATE, profile=profile, package_id="disabled") == {"enabled": False}
    result = runtime.adjust_candidates_with_receipt(
        candidates=rows, trade_date=TRADE_DATE, profile=profile, package_id="disabled", manifest_sha256="manifest",
    )
    assert result.candidates == rows and result.hmm_metadata["status"] == "NOT_APPLICABLE"


def test_declared_no_overlay_does_not_hide_an_unmapped_candidate(tmp_path: Path) -> None:
    runtime, profile, _payload, _path, _window = _registered_no_overlay_runtime(tmp_path)
    profile = profile.model_copy(update={"missing_sector_policy": "exclude_candidate"})
    with pytest.raises(HMMRuntimeUnavailableError, match="missing stock sector mapping"):
        runtime.adjust_candidates_with_receipt(
            candidates=[SelectionCandidate(symbol="000099.SZ", score=3.0, rank=1)], trade_date=TRADE_DATE,
            profile=profile, package_id="missing-map", manifest_sha256="manifest", require_frozen_snapshot=True,
        )


@pytest.mark.parametrize("invalid_span", [None, "overlap", "invalid_date"])
def test_no_overlay_compact_membership_selects_only_requested_day(tmp_path: Path, invalid_span) -> None:
    runtime, profile, payload, path, window = _registered_no_overlay_runtime(tmp_path)
    membership = payload.pop("stock_sector_map")
    payload["stock_sector_membership_spans"] = {
        symbol: [
            {"start_date": "2026-05-29", "end_date": TRADE_DATE.isoformat(), "sector_code": sector},
            {"start_date": "2026-06-02", "end_date": "2026-06-05", "sector_code": "FUTURE.SI"},
        ] for symbol, sector in membership.items()
    }
    rows = payload["stock_sector_membership_spans"]["000002.SZ"]
    if invalid_span == "overlap":
        rows[1]["start_date"] = TRADE_DATE.isoformat()
    elif invalid_span == "invalid_date":
        rows[0]["start_date"] = "2026-02-30"
    path.write_text(json.dumps(payload), encoding="utf-8")
    window["coefficient_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    if invalid_span:
        with pytest.raises(HMMRuntimeUnavailableError, match="membership spans"):
            runtime.preflight_coefficients(trade_date=TRADE_DATE, profile=profile, package_id="compact")
        with pytest.raises(HMMRuntimeUnavailableError, match="membership spans"):
            runtime.adjust_candidates_with_receipt(
                candidates=[SelectionCandidate(symbol="000002.SZ", score=3.0, rank=1)], trade_date=TRADE_DATE,
                profile=profile, package_id="compact", manifest_sha256="manifest", require_frozen_snapshot=True,
            )
        return
    preflight = runtime.preflight_coefficients(trade_date=TRADE_DATE, profile=profile, package_id="compact")
    result = runtime.adjust_candidates_with_receipt(
        candidates=[SelectionCandidate(symbol="000002.SZ", score=3.0, rank=1)], trade_date=TRADE_DATE,
        profile=profile, package_id="compact", manifest_sha256="manifest", require_frozen_snapshot=True,
    )
    assert preflight["stock_sector_map_count"] == 2
    assert result.candidates[0].score == 3.0
    assert result.candidates[0].component_scores["hmm"]["sector_code"] == "801750.SI"
