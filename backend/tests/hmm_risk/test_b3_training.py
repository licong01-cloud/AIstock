from __future__ import annotations

import json
from collections import deque
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk.b3_acceptance import (
    D3_CONTRACT_VERSION,
    D5_SELECTION_VERSION,
    D6_SEMANTIC_VERSION,
)
from backend.services.hmm_risk.b3_training import (
    B3TrainingStageError,
    B3TrainOnlySeries,
    B3FittedModel,
    models_from_repeat,
    run_level_repeat,
    write_b3_ready_model_set,
)
from backend.services.hmm_risk import b3_training as training_subject
from backend.services.hmm_risk.state_model_set import StateModelSetError, canonical_sha256
from backend.services.hmm_risk.state_model_set import BASE_FEATURES
from backend.services.hmm_risk.b3_training import build_train_only_series


def _model(*, family: str = "legacy_covfix", level: str = "L1", seed: int = 42, code: str = "S001") -> B3FittedModel:
    body = {
        "schema_version": "hmm_risk_b3_fitted_model_v1",
        "contract_version": D3_CONTRACT_VERSION,
        "family": family,
        "level": level,
        "seed": seed,
        "sector_code": code,
        "feature_names": ["f1", "f2"],
        "preprocess": {"family": "identity"},
        "startprob": [1 / 3, 1 / 3, 1 / 3],
        "transmat": [[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]],
        "means": [[-1.0, -1.0], [0.0, 0.0], [1.0, 1.0]],
        "covariance_type": "diag",
        "covars": [[1.0, 1.0], [1.0, 1.0], [1.0, 1.0]],
        "parameter_profile_sha256": "a" * 64,
        "numeric_environment_sha256": "b" * 64,
        "observation_manifest_hash": "c" * 64,
        "pit_constituent_manifest_hash": "d" * 64,
    }
    return B3FittedModel(
        family=family,
        level=level,
        seed=seed,
        sector_code=code,
        feature_names=("f1", "f2"),
        preprocess={"family": "identity"},
        startprob=np.asarray(body["startprob"], dtype=np.float64),
        transmat=np.asarray(body["transmat"], dtype=np.float64),
        means=np.asarray(body["means"], dtype=np.float64),
        covars=np.asarray(body["covars"], dtype=np.float64),
        parameter_profile_sha256=body["parameter_profile_sha256"],
        numeric_environment_sha256=body["numeric_environment_sha256"],
        observation_manifest_hash=body["observation_manifest_hash"],
        pit_constituent_manifest_hash=body["pit_constituent_manifest_hash"],
        model_payload_sha256=canonical_sha256(body),
    )


def test_repeat_model_payload_roundtrip_rejects_hash_drift() -> None:
    model = _model()
    payloads = [model.payload()]
    repeat = {
        "family": "legacy_covfix",
        "level": "L1",
        "canonical_sector_codes": ["S001"],
        "feature_names": ["f1", "f2"],
        "models": payloads,
        "model_payload_sha256": canonical_sha256(payloads),
    }
    restored = models_from_repeat(repeat)
    assert restored[(42, "S001")].model_payload_sha256 == model.model_payload_sha256

    drifted = json.loads(json.dumps(repeat))
    drifted["models"][0]["covars"][0][0] = 2.0
    with pytest.raises(StateModelSetError, match="payload hash mismatch"):
        models_from_repeat(drifted)


def test_train_only_builder_does_not_materialize_validation_or_future_utility() -> None:
    dates = pd.bdate_range("2022-01-03", periods=120)
    codes = [f"L1-{index:02d}" for index in range(31)]
    index = pd.MultiIndex.from_product([dates, codes], names=["trade_date", "l1_code"])
    panel = pd.DataFrame(index=index)
    panel["l1_name"] = [f"Sector {code}" for _, code in index]
    for feature_index, feature in enumerate(BASE_FEATURES):
        panel[feature] = np.arange(len(panel), dtype=np.float64) + feature_index + 1.0
    constituents = {
        code: {"schema_version": "pit_v1", "l2_codes": [f"L2-{position:03d}"]} for position, code in enumerate(codes)
    }
    result = build_train_only_series(
        panel,
        feature_names=BASE_FEATURES,
        train_start=dates[0].date(),
        train_end=dates[-1].date(),
        constituent_manifest=constituents,
        expected_sector_count=31,
        direct_sector_level="L1",
    )
    assert len(result) == 31
    assert not hasattr(result[codes[0]], "validation_observations")
    assert not hasattr(result[codes[0]], "validation_future_utility")


def test_formal_fit_uses_monitor_terminal_likelihood_and_never_validation(monkeypatch) -> None:
    class _Monitor:
        converged = True
        iter = 2
        n_iter = 300
        tol = 0.01
        history = deque([-1.0, -0.995])

    class _GaussianHMM:
        def __init__(self, **kwargs) -> None:
            assert kwargs["min_covar"] == 0.0
            assert kwargs["covars_weight"] == 2.0
            assert kwargs["init_params"] == ""
            self.monitor_ = _Monitor()

        @property
        def covars_(self):
            return self._covars_

        @covars_.setter
        def covars_(self, value):
            self._covars_ = np.asarray(value, dtype=np.float64)

        def fit(self, train):
            return self

    monkeypatch.setattr("hmmlearn.hmm.GaussianHMM", _GaussianHMM)
    monkeypatch.setattr(
        training_subject,
        "_manual_b3_diag04_initialization",
        lambda train, sector_reference_variance, random_seed: (
            np.full(3, 1 / 3),
            np.asarray([[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]),
            np.asarray([[-1.0] * 7, [0.0] * 7, [1.0] * 7]),
            np.ones((3, 7)),
            {"schema_version": "diagnostic_init", "cluster_counts": [60, 60, 60]},
        ),
    )
    monkeypatch.setattr(training_subject, "_sector_local_reference_variance", lambda train: np.ones(7))
    covariance_evidence = {
        "raw_covars": np.ones((3, 7)).tolist(),
        "sector_local_reference_variance_R_sj": np.ones(7).tolist(),
        "state_posterior_mass": [60.0, 60.0, 60.0],
        "posterior_second_moment_about_fitted_mean": np.ones((3, 7)).tolist(),
        "nu": 1.0,
        "postfit_projection_performed": False,
    }
    monkeypatch.setattr(
        training_subject,
        "_b3_diag04_covariance_evidence",
        lambda model, train, raw_covars, sector_reference_variance: (
            covariance_evidence,
            np.full((180, 3), 1 / 3),
            -123.0,
        ),
    )
    states = np.asarray([index % 3 for index in range(180)])
    train_posteriors = np.zeros((180, 3))
    train_posteriors[np.arange(180), states] = 1.0
    monkeypatch.setattr(training_subject, "causal_forward_posteriors", lambda *args, **kwargs: train_posteriors)
    item = B3TrainOnlySeries(
        sector_code="S001",
        sector_name="Sector 1",
        train_observations=np.ones((180, 7)),
        train_dates=tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(180)),
        pit_l2_constituents=("L2-001",),
        pit_constituent_manifest_hash="a" * 64,
        observation_manifest_hash="b" * 64,
    )
    entry, _ = training_subject._fit_b3_train_only(
        item,
        family="legacy_covfix",
        level="L1",
        feature_names=BASE_FEATURES,
        preprocess={"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None},
        seed=42,
        numeric_environment={"packages": {"hmmlearn": "0.3.3"}},
    )

    assert entry["final_train_log_likelihood"] == -0.995
    assert entry["final_train_log_likelihood_source"] == "monitor_history_terminal_value"
    assert entry["validation_accessed"] is False
    assert entry["future_utility_accessed"] is False
    assert entry["parameter_profile"]["numeric_contract_status"] == "USER_APPROVED_FORMAL_CONTRACT"


def _train_only_level() -> dict[str, B3TrainOnlySeries]:
    dates = tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(120))
    return {
        f"S{index:03d}": B3TrainOnlySeries(
            sector_code=f"S{index:03d}",
            sector_name=f"Sector {index}",
            train_observations=np.ones((120, 7), dtype=np.float64),
            train_dates=dates,
            pit_l2_constituents=(f"L2-{index:03d}",),
            pit_constituent_manifest_hash="a" * 64,
            observation_manifest_hash="b" * 64,
        )
        for index in range(31)
    }


def test_level_repeat_records_only_typed_candidate_failures_and_propagates_programming_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        training_subject,
        "c008_b3_diag04_fixed_numeric_environment",
        lambda: {"packages": {"hmmlearn": "0.3.3"}},
    )
    monkeypatch.setattr(
        training_subject,
        "_fit_b3_train_only",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            B3TrainingStageError("covariance", "hmm_risk_model_covariance_invalid", ValueError("invalid"))
        ),
    )
    repeat, models = run_level_repeat(
        _train_only_level(),
        family="legacy_covfix",
        level="L1",
        feature_names=BASE_FEATURES,
        preprocess_family="identity",
        process_identity="test_process",
    )
    assert models == {}
    assert len(repeat["entries"]) == 8 * 31
    assert {entry["failure_stage"] for entry in repeat["entries"]} == {"covariance"}
    assert {tuple(entry["failure_reason_codes"]) for entry in repeat["entries"]} == {
        ("hmm_risk_model_covariance_invalid",)
    }

    monkeypatch.setattr(
        training_subject,
        "_fit_b3_train_only",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("programming defect")),
    )
    with pytest.raises(RuntimeError, match="programming defect"):
        run_level_repeat(
            _train_only_level(),
            family="legacy_covfix",
            level="L1",
            feature_names=BASE_FEATURES,
            preprocess_family="identity",
            process_identity="test_process",
        )


def _accepted_receipt(kind: str) -> dict:
    body = {
        "contract_version": D6_SEMANTIC_VERSION,
        "failure_reason_codes": [],
        "blocking_reason_codes": [],
        "warning_reason_codes": [],
        "primary_reason_code": None,
        "evidence": {},
        f"{kind}_status": "accepted",
        f"{kind}_valid": True,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _selection(family: str, level: str) -> dict:
    body = {
        "contract_version": D5_SELECTION_VERSION,
        "failure_reason_codes": [],
        "blocking_reason_codes": [],
        "warning_reason_codes": [],
        "primary_reason_code": None,
        "evidence": {"family": family, "level": level, "selected_seed": 42},
        "level_selection_status": "accepted",
        "level_selection_valid": True,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _selected_artifact(family: str, level: str, selection: dict) -> dict:
    count = 31 if level == "L1" else 131
    entries = []
    for index in range(count):
        model = _model(family=family, level=level, code=f"{level}-{index:03d}")
        semantic_mapping = {"0": "fading", "1": "neutral", "2": "trending"}
        semantic = {
            "contract_version": D6_SEMANTIC_VERSION,
            "assignment": _accepted_receipt("semantic_assignment"),
            "semantic_evidence": _accepted_receipt("semantic_evidence"),
            "semantic_mapping": semantic_mapping,
        }
        entry_body = {
            **model.payload(),
            "semantic": semantic,
            "validation_accessed_after_selection": True,
            "future_utility_accessed_after_selection": True,
            "selection_reexecuted": False,
            "semantic_mapping": semantic_mapping,
        }
        entries.append({**entry_body, "selected_entry_sha256": canonical_sha256(entry_body)})
    body = {
        "schema_version": "hmm_risk_b3_selected_level_artifact_v1",
        "family": family,
        "level": level,
        "selected_seed": 42,
        "selection_receipt_sha256": selection["receipt_sha256"],
        "status": "accepted",
        "entry_count": count,
        "entries": entries,
        "selection_reexecuted": False,
        "ready": False,
    }
    return {**body, "artifact_sha256": canonical_sha256(body)}


def test_ready_writer_requires_both_families_and_direct_levels(tmp_path) -> None:
    keys = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    selections = {key: _selection(*key) for key in keys}
    artifacts = {key: _selected_artifact(*key, selections[key]) for key in keys}
    manifest_path = write_b3_ready_model_set(
        tmp_path,
        selected_artifacts=artifacts,
        selection_receipts=selections,
        dataset_manifest_hash="d" * 64,
        mapping_manifest_hash="e" * 64,
        producer_commit="c" * 40,
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "READY"
    assert len(manifest["layers"]) == 4

    incomplete = dict(artifacts)
    incomplete.pop(("legacy_covfix", "L2"))
    with pytest.raises(StateModelSetError, match="both families and both direct levels"):
        write_b3_ready_model_set(
            tmp_path / "incomplete",
            selected_artifacts=incomplete,
            selection_receipts=selections,
            dataset_manifest_hash="d" * 64,
            mapping_manifest_hash="e" * 64,
            producer_commit="c" * 40,
        )


def test_ready_writer_rejects_declared_count_without_durable_entries(tmp_path) -> None:
    keys = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    selections = {key: _selection(*key) for key in keys}
    artifacts = {key: _selected_artifact(*key, selections[key]) for key in keys}
    target = dict(artifacts[("legacy_covfix", "L1")])
    target["entries"] = []
    target_body = {key: value for key, value in target.items() if key != "artifact_sha256"}
    target["artifact_sha256"] = canonical_sha256(target_body)
    artifacts[("legacy_covfix", "L1")] = target

    with pytest.raises(StateModelSetError, match="selected entry count"):
        write_b3_ready_model_set(
            tmp_path,
            selected_artifacts=artifacts,
            selection_receipts=selections,
            dataset_manifest_hash="d" * 64,
            mapping_manifest_hash="e" * 64,
            producer_commit="c" * 40,
        )
