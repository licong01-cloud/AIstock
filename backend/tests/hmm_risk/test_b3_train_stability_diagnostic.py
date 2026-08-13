from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pytest

from backend.services.hmm_risk.b3_train_stability_diagnostic import (
    EXPECTED_SECTOR_COUNT,
    FRESH_PROCESS_RECEIPT_HASHES,
    REASON_RUN,
    REASON_TRANSITION,
    TRAINING_AUTHORITY_RECEIPT_SHA256,
    TRAIN_SOURCE_IDENTITIES,
    build_report,
    evaluate_window,
    source_drift_report,
    validate_report,
)
from backend.services.hmm_risk.b3_acceptance import RESTART_SCHEDULE
from backend.services.hmm_risk.b3_mixed_dimension import build_projection_receipt
from backend.services.hmm_risk.b3_training import B3FittedModel, B3TrainOnlySeries
from backend.services.hmm_risk.state_model_set import ALL_CORE_FEATURES, StateModelSetError, canonical_sha256


def _model(*, startprob: tuple[float, float, float] = (1.0, 0.0, 0.0)) -> B3FittedModel:
    return B3FittedModel(
        family="autocycle_all_core",
        level="L2",
        seed=42,
        sector_code="801011.SI",
        feature_names=("feature",),
        preprocess={"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None},
        startprob=np.asarray(startprob, dtype=np.float64),
        transmat=np.eye(3, dtype=np.float64),
        means=np.asarray([[0.0], [10.0], [20.0]], dtype=np.float64),
        covars=np.ones((3, 1), dtype=np.float64),
        parameter_profile_sha256="a" * 64,
        numeric_environment_sha256="b" * 64,
        observation_manifest_hash="c" * 64,
        pit_constituent_manifest_hash="d" * 64,
        model_payload_sha256="e" * 64,
    )


def _calendar(rows: int = 400) -> tuple[date, ...]:
    return tuple(date(2022, 1, 1) + timedelta(days=index) for index in range(rows))


def test_each_window_resets_from_fitted_startprob() -> None:
    calendar = _calendar()
    dates = calendar[:182]
    observations = np.full((182, 1), 10.0, dtype=np.float64)
    first = evaluate_window(observations, dates, calendar, _model(startprob=(1.0, 0.0, 0.0)), window="early")
    second = evaluate_window(observations, dates, calendar, _model(startprob=(0.0, 1.0, 0.0)), window="late")

    assert first["hard_assignment_sha256"] != second["hard_assignment_sha256"]
    assert first["row_count"] == second["row_count"] == 182


def test_calendar_gap_breaks_runs_and_is_not_compressed_into_transition() -> None:
    calendar = _calendar()
    dates = tuple((*calendar[:91], *calendar[92:183]))
    evidence = evaluate_window(np.zeros((182, 1)), dates, calendar, _model(), window="early")

    assert evidence["calendar_gap_count"] == 1
    assert evidence["transition_counts"][0][0] == 180
    assert evidence["states"]["0"]["contiguous_run_count"] == 2
    assert REASON_RUN in evidence["states"]["1"]["reason_codes"]
    assert REASON_TRANSITION in evidence["states"]["0"]["reason_codes"]


def test_posterior_normalization_failure_is_a_typed_profile_result(monkeypatch) -> None:
    calendar = _calendar()
    monkeypatch.setattr(
        "backend.services.hmm_risk.b3_train_stability_diagnostic.causal_forward_posteriors",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            StateModelSetError("causal posterior normalization failed at row 7")
        ),
    )

    evidence = evaluate_window(np.zeros((182, 1)), calendar[:182], calendar, _model(), window="early")

    assert evidence["status"] == "train_window_structurally_unobserved"
    assert evidence["reason_codes"] == ["hmm_risk_train_stability_posterior_normalization_failed"]
    assert evidence["states"] is None


def test_report_validator_rejects_noncanonical_and_action_flags() -> None:
    report = source_drift_report(error=StateModelSetError("source drift"), diagnostic_producer_commit="f" * 40)
    validate_report(report)

    report["fit_performed"] = True
    with pytest.raises(StateModelSetError, match="report contract is invalid"):
        validate_report(report)


def test_report_validator_rejects_embedded_model_or_matrix_payload() -> None:
    clean = source_drift_report(error=StateModelSetError("source drift"), diagnostic_producer_commit="f" * 40)
    body = {key: value for key, value in clean.items() if key != "receipt_sha256"}
    body["means"] = [[0.0]]
    report = {**body, "receipt_sha256": canonical_sha256(body)}

    with pytest.raises(StateModelSetError, match="forbidden field"):
        validate_report(report)


def test_full_report_evaluates_exact_8_by_131_without_refit_or_selection(monkeypatch) -> None:
    dates = _calendar(420)
    feature_names = ALL_CORE_FEATURES
    series: dict[str, B3TrainOnlySeries] = {}
    first_models: dict[tuple[int, str], B3FittedModel] = {}
    second_models: dict[tuple[int, str], B3FittedModel] = {}
    model_hashes: list[str] = []
    preprocess = {"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None}
    for index in range(EXPECTED_SECTOR_COUNT):
        code = f"S{index:03d}"
        values = np.tile(
            np.asarray(
                [np.zeros(len(feature_names)), np.full(len(feature_names), 10.0), np.full(len(feature_names), 20.0)],
                dtype=np.float64,
            ),
            (140, 1),
        )
        date_strings = [value.isoformat() for value in dates]
        train_manifest = {
            **TRAIN_SOURCE_IDENTITIES,
            "formula_version": "hmm_risk_l1_sector_factor_formula_v2_c010",
            "schema_version": "hmm_risk_d4_train_frozen_input_manifest_v1",
            "direct_sector_level": "L2",
            "sector_code": code,
            "train_dates": date_strings,
            "train_dates_sha256": canonical_sha256(date_strings),
            "train_observation_sha256": canonical_sha256(values.tolist()),
        }
        item = B3TrainOnlySeries(
            sector_code=code,
            sector_name=code,
            train_observations=values,
            train_dates=dates,
            pit_l2_constituents=(code,),
            pit_constituent_manifest_hash="d" * 64,
            observation_manifest_hash=canonical_sha256({"sector": code}),
            train_input_manifest=train_manifest,
        )
        projection, _ = build_projection_receipt(
            family="autocycle_all_core",
            level="L2",
            sector_code=code,
            full_feature_names=feature_names,
            preprocess=preprocess,
            raw_observations=values,
            preprocessed_observations=values,
            train_input_manifest=train_manifest,
        )
        series[code] = item
        for seed in RESTART_SCHEDULE:
            model_hash = canonical_sha256({"seed": seed, "sector_code": code})
            model = B3FittedModel(
                family="autocycle_all_core",
                level="L2",
                seed=seed,
                sector_code=code,
                feature_names=feature_names,
                preprocess=preprocess,
                startprob=np.asarray([1 / 3, 1 / 3, 1 / 3]),
                transmat=np.full((3, 3), 1 / 3),
                means=np.asarray(
                    [
                        np.zeros(projection["likelihood_feature_count"]),
                        np.full(projection["likelihood_feature_count"], 10.0),
                        np.full(projection["likelihood_feature_count"], 20.0),
                    ]
                ),
                covars=np.ones((3, projection["likelihood_feature_count"])),
                parameter_profile_sha256="a" * 64,
                numeric_environment_sha256="b" * 64,
                observation_manifest_hash=item.observation_manifest_hash,
                pit_constituent_manifest_hash=item.pit_constituent_manifest_hash,
                model_payload_sha256=model_hash,
                projection_receipt=projection,
            )
            first_models[(seed, code)] = model
            second_models[(seed, code)] = model
            if seed == 43:
                model_hashes.append(model_hash)
    training_authority = {
        **TRAIN_SOURCE_IDENTITIES,
        "receipt_sha256": TRAINING_AUTHORITY_RECEIPT_SHA256,
        "fresh_process_receipt_hashes": list(FRESH_PROCESS_RECEIPT_HASHES),
        "selection": {"receipt_sha256": "8ec3967bb775329bcd277c440a8cfc11f1b15888777e677c4612820d34085cbc"},
    }
    zero_refit = {
        "training_authority_receipt_sha256": TRAINING_AUTHORITY_RECEIPT_SHA256,
        "d5_selection_receipt_sha256": "8ec3967bb775329bcd277c440a8cfc11f1b15888777e677c4612820d34085cbc",
        "selected_model_payload_hashes": model_hashes,
        "selected_model_payload_hashes_sha256": canonical_sha256(model_hashes),
        "family": "autocycle_all_core",
        "level": "L2",
        "selected_seed": 43,
        "fit_performed": False,
        "selection_reexecuted": False,
        "ready_artifact_write_performed": False,
        "selected_artifact": {
            "entries": [
                {
                    "sector_code": code,
                    "semantic": {
                        "semantic_evidence": {
                            "semantic_evidence_valid": index >= 11,
                            "failure_reason_codes": ["frozen_d6_failure"] if index < 11 else [],
                        }
                    },
                }
                for index, code in enumerate(sorted(series))
            ]
        },
    }
    monkeypatch.setattr(
        "backend.services.hmm_risk.b3_train_stability_diagnostic.ZERO_REFIT_REPORT_SHA256",
        canonical_sha256(zero_refit),
    )
    monkeypatch.setattr(
        "backend.services.hmm_risk.b3_train_stability_diagnostic.SELECTED_MODEL_HASHES_SHA256",
        canonical_sha256(model_hashes),
    )

    report = build_report(
        training_authority=training_authority,
        zero_refit_report=zero_refit,
        first_models=first_models,
        second_models=second_models,
        series=series,
        trading_dates=dates,
        diagnostic_producer_commit="f" * 40,
    )

    validate_report(report)
    assert report["status"] == "diagnostic_complete"
    assert report["profile_count"] == 1048
    assert len(report["d6_blocker_summary"]) == 11
    assert report["fit_performed"] is False
    assert report["selection_performed"] is False
    assert report["d6_executed"] is False
