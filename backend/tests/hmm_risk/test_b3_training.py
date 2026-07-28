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
    RESTART_SCHEDULE,
    evaluate_covariance_acceptance,
    evaluate_likelihood_acceptance,
    evaluate_semantic_validation,
    evaluate_train_occupancy,
)
from backend.services.hmm_risk.b3_training import (
    B3TrainingStageError,
    B3TrainOnlySeries,
    B3FittedModel,
    audit_train_only_coverage,
    build_train_only_series,
    models_from_repeat,
    run_level_repeat,
    write_b3_ready_model_set,
)
from backend.services.hmm_risk import b3_training as training_subject
from backend.services.hmm_risk.state_model_set import StateModelSetError, canonical_sha256
from backend.services.hmm_risk.state_model_set import ALL_CORE_FEATURES, BASE_FEATURES


def _train_manifest(
    dates: tuple[date, ...],
    observations: np.ndarray,
    *,
    sector_code: str = "S001",
    direct_sector_level: str = "L1",
) -> dict:
    values = [item.isoformat() for item in dates]
    return {
        "schema_version": "hmm_risk_d4_train_frozen_input_manifest_v1",
        "direct_sector_level": direct_sector_level,
        "sector_code": sector_code,
        "train_dates": values,
        "train_dates_sha256": canonical_sha256(values),
        "train_observation_sha256": canonical_sha256(np.asarray(observations, dtype=np.float64).tolist()),
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "feature_domain_policy_sha256": TEST_POLICY_SHA256,
    }


def _feature_domain_policy_manifest() -> dict:
    ledger_entry = {"canonical_ts_code": "000001.SZ", "moneyflow_contributor_eligible": True}
    ledger = [{**ledger_entry, "entry_sha256": canonical_sha256(ledger_entry)}]
    body = {
        "schema_version": "hmm_risk_c010_feature_domain_policy_v1",
        "formula_version": "hmm_risk_l1_sector_factor_formula_v2_c010",
        "eligibility_entry_count": len(ledger),
        "contributor_ledger": ledger,
        "contributor_ledger_sha256": canonical_sha256(ledger),
        "excluded_moneyflow_symbols": [],
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


TEST_POLICY_MANIFEST = _feature_domain_policy_manifest()
TEST_POLICY_SHA256 = TEST_POLICY_MANIFEST["receipt_sha256"]


def _model(*, family: str = "legacy_covfix", level: str = "L1", seed: int = 42, code: str = "S001") -> B3FittedModel:
    feature_names = BASE_FEATURES if family == "legacy_covfix" else ALL_CORE_FEATURES
    feature_count = len(feature_names)
    preprocess = (
        {"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None}
        if family == "legacy_covfix"
        else {
            "family": "winsor_zscore_1_99_train_global_v1",
            "winsor_low": [-3.0] * feature_count,
            "winsor_high": [3.0] * feature_count,
            "center": [0.0] * feature_count,
            "scale": [1.0] * feature_count,
        }
    )
    body = {
        "schema_version": "hmm_risk_b3_fitted_model_v1",
        "contract_version": D3_CONTRACT_VERSION,
        "family": family,
        "level": level,
        "seed": seed,
        "sector_code": code,
        "feature_names": list(feature_names),
        "preprocess": preprocess,
        "startprob": [1 / 3, 1 / 3, 1 / 3],
        "transmat": [[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]],
        "means": [[-1.0] * feature_count, [0.0] * feature_count, [1.0] * feature_count],
        "covariance_type": "diag",
        "covars": [[1.0] * feature_count, [1.0] * feature_count, [1.0] * feature_count],
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
        feature_names=feature_names,
        preprocess=preprocess,
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
        "feature_names": list(BASE_FEATURES),
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


def test_train_only_coverage_audit_collects_all_sectors_without_fit_or_validation() -> None:
    dates = pd.bdate_range("2022-01-03", periods=120)
    codes = [f"L1-{index:02d}" for index in range(31)]
    index = pd.MultiIndex.from_product([dates, codes], names=["trade_date", "l1_code"])
    panel = pd.DataFrame(index=index)
    for feature_index, feature in enumerate(ALL_CORE_FEATURES):
        panel[feature] = np.arange(len(panel), dtype=np.float64) + feature_index + 1.0
    failing_code = codes[0]
    failing_index = panel.xs(failing_code, level="l1_code").index[10:]
    panel.loc[(failing_index, failing_code), list(ALL_CORE_FEATURES)] = np.nan

    report = audit_train_only_coverage(
        panel,
        feature_names=ALL_CORE_FEATURES,
        train_start=dates[0].date(),
        train_end=dates[-1].date(),
        expected_sector_count=31,
        direct_sector_level="L1",
    )

    assert report["entry_count"] == 31
    assert report["minimum_observed_train_row_count"] == 10
    assert report["maximum_observed_train_row_count"] == 120
    assert report["insufficient_sector_codes"] == [failing_code]
    assert report["train_coverage_valid"] is False
    assert report["failure_reason_codes"] == ["hmm_risk_model_train_observation_coverage_insufficient"]
    assert report["fit_performed"] is False
    assert report["validation_accessed"] is False
    assert report["selection_performed"] is False


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
        train_input_manifest=_train_manifest(
            tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(180)),
            np.ones((180, 7)),
        ),
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
            train_input_manifest=_train_manifest(
                dates,
                np.ones((120, 7), dtype=np.float64),
                sector_code=f"S{index:03d}",
            ),
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


def _validation_dates() -> tuple[date, ...]:
    available = pd.bdate_range("2024-07-01", "2025-03-31")
    indexes = np.linspace(0, len(available) - 1, 182, dtype=int)
    return tuple(available[index].date() for index in indexes)


def _semantic_receipt(model_hash: str, *, level: str, sector_code: str) -> dict:
    dates = _validation_dates()
    states = [(index // 3) % 3 for index in range(182)]
    posterior = np.zeros((182, 3), dtype=np.float64)
    posterior[np.arange(182), states] = 1.0
    values = np.asarray([(-0.02, 0.0, 0.02)[state] for state in states], dtype=np.float64)
    utility = {
        "excess_return_5d": values,
        "excess_return_10d": values,
        "excess_return_20d": values,
        "source_cutoff": "2025-04-30",
        "formula_version": "hmm_risk_hard_future_excess_035_035_030_v1",
    }
    component_hashes = {
        key: canonical_sha256(values.tolist()) for key in ("excess_return_5d", "excess_return_10d", "excess_return_20d")
    }
    encoded_dates = [item.isoformat() for item in dates]
    manifest = {
        "schema_version": "hmm_risk_d6_frozen_input_manifest_v1",
        "direct_sector_level": level,
        "sector_code": sector_code,
        "benchmark_identity": "000300.SH",
        "validation_observation_sha256": "f" * 64,
        "validation_dates": encoded_dates,
        "validation_dates_sha256": canonical_sha256(encoded_dates),
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "l2_stock_fact_manifest_hash": "d" * 64,
        "feature_domain_policy_sha256": TEST_POLICY_SHA256,
        "source_cutoff": utility["source_cutoff"],
        "formula_version": utility["formula_version"],
        "utility_component_sha256": component_hashes,
        "combined_utility_sha256": canonical_sha256((0.35 * values + 0.35 * values + 0.30 * values).tolist()),
    }
    return evaluate_semantic_validation(
        posterior,
        dates,
        utility,
        frozen_input_manifest=manifest,
        selected_model_payload_sha256=model_hash,
    )


def _training_receipt(model: B3FittedModel) -> dict:
    dates = tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(180))
    states = [index % 3 for index in range(180)]
    posterior = np.zeros((180, 3), dtype=np.float64)
    posterior[np.arange(180), states] = 1.0
    likelihood = evaluate_likelihood_acceptance(
        {"converged": True, "iterations": 2, "maximum_iterations": 300, "history": [-1.0, -0.995]}
    )
    covariance = evaluate_covariance_acceptance(
        {
            "raw_covars": np.ones((3, len(model.feature_names))).tolist(),
            "sector_local_reference_variance_R_sj": [1.0] * len(model.feature_names),
            "state_posterior_mass": [60.0, 60.0, 60.0],
            "posterior_second_moment_about_fitted_mean": np.ones((3, len(model.feature_names))).tolist(),
            "train_rows": 180,
            "nu": 1.0,
            "postfit_projection_performed": False,
        }
    )
    occupancy = evaluate_train_occupancy(
        posterior,
        dates,
        frozen_input_manifest=_train_manifest(
            dates,
            np.ones((180, len(model.feature_names)), dtype=np.float64),
            sector_code=model.sector_code,
            direct_sector_level=model.level,
        ),
    )
    body = {
        "family": model.family,
        "level": model.level,
        "seed": model.seed,
        "sector_code": model.sector_code,
        "fit_status": "accepted",
        "model_entry_status": "accepted",
        "model_entry_valid": True,
        "model_payload_sha256": model.model_payload_sha256,
        "likelihood": likelihood,
        "covariance": covariance,
        "train_occupancy": occupancy,
        "final_train_log_likelihood": -0.995,
        "training_rows": 180,
        "feature_count": len(model.feature_names),
    }
    return {**body, "entry_receipt_sha256": canonical_sha256(body)}


def _selection(family: str, level: str, training_receipts: list[dict]) -> dict:
    codes = [str(receipt["sector_code"]) for receipt in training_receipts]
    hashes = [str(receipt["entry_receipt_sha256"]) for receipt in training_receipts]
    candidates = [
        {
            "seed": seed,
            "schedule_index": index,
            "eligible": seed == 42,
            "aggregate": {"minimum": -1.0, "median": -1.0, "mean": -1.0} if seed == 42 else None,
            "entry_receipt_hashes": hashes if seed == 42 else [],
            "warning_reason_codes": [],
        }
        for index, seed in enumerate(RESTART_SCHEDULE)
    ]
    body = {
        "contract_version": D5_SELECTION_VERSION,
        "failure_reason_codes": [],
        "blocking_reason_codes": [],
        "warning_reason_codes": [],
        "primary_reason_code": None,
        "evidence": {
            "family": family,
            "level": level,
            "feature_domain_policy_sha256": TEST_POLICY_SHA256,
            "selected_seed": 42,
            "selected_schedule_index": 0,
            "canonical_sector_codes": codes,
            "canonical_sector_set_sha256": canonical_sha256(codes),
            "schedule": list(RESTART_SCHEDULE),
            "feature_count": 7 if family == "legacy_covfix" else 20,
            "repeat_entries_sha256": canonical_sha256(training_receipts),
            "candidates": candidates,
            "lexicographic_filters": [
                {"component": component, "best": -1.0, "survivor_seeds": [42]}
                for component in ("minimum", "median", "mean")
            ],
            "validation_accessed": False,
            "future_utility_accessed": False,
            "semantic_labelability_accessed": False,
            "d6_status_accessed": False,
            "selection_followed_by_refit": False,
        },
        "level_selection_status": "accepted",
        "level_selection_valid": True,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _selected_artifact(family: str, level: str) -> tuple[dict, dict]:
    count = 31 if level == "L1" else 131
    entries = []
    training_receipts = []
    for index in range(count):
        model = _model(family=family, level=level, code=f"{level}-{index:03d}")
        training_receipt = _training_receipt(model)
        training_receipts.append(training_receipt)
        semantic_mapping = {"0": "fading", "1": "neutral", "2": "trending"}
        semantic = _semantic_receipt(model.model_payload_sha256, level=level, sector_code=model.sector_code)
        entry_body = {
            **model.payload(),
            "training_receipt": training_receipt,
            "semantic": semantic,
            "validation_accessed_after_selection": True,
            "future_utility_accessed_after_selection": True,
            "selection_reexecuted": False,
            "semantic_mapping": semantic_mapping,
        }
        entries.append({**entry_body, "selected_entry_sha256": canonical_sha256(entry_body)})
    selection = _selection(family, level, training_receipts)
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
    return {**body, "artifact_sha256": canonical_sha256(body)}, selection


def test_ready_writer_requires_both_families_and_direct_levels(tmp_path) -> None:
    keys = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    pairs = {key: _selected_artifact(*key) for key in keys}
    artifacts = {key: pair[0] for key, pair in pairs.items()}
    selections = {key: pair[1] for key, pair in pairs.items()}
    policy_manifest = _feature_domain_policy_manifest()
    manifest_path = write_b3_ready_model_set(
        tmp_path,
        selected_artifacts=artifacts,
        selection_receipts=selections,
        dataset_manifest_hash="a" * 64,
        mapping_manifest_hash="b" * 64,
        calendar_manifest_hash="c" * 64,
        l2_stock_fact_manifest_hash="d" * 64,
        feature_domain_policy_sha256=policy_manifest["receipt_sha256"],
        feature_domain_policy_manifest=policy_manifest,
        producer_commit="c" * 40,
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "hmm_risk_state_model_set_v1"
    assert manifest["status"] == "READY"
    assert len(manifest["layers"]) == 4
    assert manifest["feature_domain_policy_sha256"] == policy_manifest["receipt_sha256"]
    assert manifest["feature_domain_policy_manifest"] == policy_manifest

    invalid_policy = json.loads(json.dumps(policy_manifest))
    invalid_entry = invalid_policy["contributor_ledger"][0]
    invalid_entry["moneyflow_contributor_eligible"] = "true"
    invalid_entry_body = {key: value for key, value in invalid_entry.items() if key != "entry_sha256"}
    invalid_entry["entry_sha256"] = canonical_sha256(invalid_entry_body)
    invalid_policy["contributor_ledger_sha256"] = canonical_sha256(invalid_policy["contributor_ledger"])
    invalid_policy_body = {key: value for key, value in invalid_policy.items() if key != "receipt_sha256"}
    invalid_policy["receipt_sha256"] = canonical_sha256(invalid_policy_body)
    with pytest.raises(StateModelSetError, match="contributor ledger entry is invalid"):
        write_b3_ready_model_set(
            tmp_path / "invalid-policy",
            selected_artifacts=artifacts,
            selection_receipts=selections,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            feature_domain_policy_sha256=invalid_policy["receipt_sha256"],
            feature_domain_policy_manifest=invalid_policy,
            producer_commit="c" * 40,
        )

    incomplete = dict(artifacts)
    incomplete.pop(("legacy_covfix", "L2"))
    with pytest.raises(StateModelSetError, match="both families and both direct levels"):
        write_b3_ready_model_set(
            tmp_path / "incomplete",
            selected_artifacts=incomplete,
            selection_receipts=selections,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            feature_domain_policy_sha256=policy_manifest["receipt_sha256"],
            feature_domain_policy_manifest=policy_manifest,
            producer_commit="c" * 40,
        )


def test_ready_writer_rejects_declared_count_without_durable_entries(tmp_path) -> None:
    keys = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    pairs = {key: _selected_artifact(*key) for key in keys}
    artifacts = {key: pair[0] for key, pair in pairs.items()}
    selections = {key: pair[1] for key, pair in pairs.items()}
    target = dict(artifacts[("legacy_covfix", "L1")])
    target["entries"] = []
    target_body = {key: value for key, value in target.items() if key != "artifact_sha256"}
    target["artifact_sha256"] = canonical_sha256(target_body)
    artifacts[("legacy_covfix", "L1")] = target
    policy_manifest = _feature_domain_policy_manifest()

    with pytest.raises(StateModelSetError, match="selected entry count"):
        write_b3_ready_model_set(
            tmp_path,
            selected_artifacts=artifacts,
            selection_receipts=selections,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            feature_domain_policy_sha256=policy_manifest["receipt_sha256"],
            feature_domain_policy_manifest=policy_manifest,
            producer_commit="c" * 40,
        )


def test_ready_layer_rejects_rehashed_but_empty_semantic_evidence() -> None:
    artifact, selection = _selected_artifact("legacy_covfix", "L1")
    entry = dict(artifact["entries"][0])
    semantic = dict(entry["semantic"])
    semantic_evidence = dict(semantic["semantic_evidence"])
    semantic_evidence["evidence"] = {}
    semantic_evidence_body = {key: value for key, value in semantic_evidence.items() if key != "receipt_sha256"}
    semantic["semantic_evidence"] = {
        **semantic_evidence_body,
        "receipt_sha256": canonical_sha256(semantic_evidence_body),
    }
    entry["semantic"] = semantic
    entry_body = {key: value for key, value in entry.items() if key != "selected_entry_sha256"}
    artifact["entries"][0] = {**entry_body, "selected_entry_sha256": canonical_sha256(entry_body)}
    artifact_body = {key: value for key, value in artifact.items() if key != "artifact_sha256"}
    artifact["artifact_sha256"] = canonical_sha256(artifact_body)

    with pytest.raises(StateModelSetError, match="semantic evidence is not accepted"):
        training_subject._validate_ready_layer(
            artifact,
            selection,
            family="legacy_covfix",
            level="L1",
            expected_count=31,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            feature_domain_policy_sha256=TEST_POLICY_SHA256,
        )


def test_ready_layer_rejects_selection_receipt_hashes_not_linked_to_durable_training_receipts() -> None:
    artifact, selection = _selected_artifact("legacy_covfix", "L1")
    selection = json.loads(json.dumps(selection))
    selected = next(candidate for candidate in selection["evidence"]["candidates"] if candidate["seed"] == 42)
    selected["entry_receipt_hashes"] = ["0" * 64 for _ in range(31)]
    selection_body = {key: value for key, value in selection.items() if key != "receipt_sha256"}
    selection = {**selection_body, "receipt_sha256": canonical_sha256(selection_body)}
    artifact["selection_receipt_sha256"] = selection["receipt_sha256"]
    artifact_body = {key: value for key, value in artifact.items() if key != "artifact_sha256"}
    artifact["artifact_sha256"] = canonical_sha256(artifact_body)

    with pytest.raises(StateModelSetError, match="selection receipt lineage is invalid"):
        training_subject._validate_ready_layer(
            artifact,
            selection,
            family="legacy_covfix",
            level="L1",
            expected_count=31,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            feature_domain_policy_sha256=TEST_POLICY_SHA256,
        )


def test_ready_layer_rejects_feature_domain_policy_lineage_drift() -> None:
    artifact, selection = _selected_artifact("legacy_covfix", "L1")

    with pytest.raises(StateModelSetError, match="selection contract is invalid"):
        training_subject._validate_ready_layer(
            artifact,
            selection,
            family="legacy_covfix",
            level="L1",
            expected_count=31,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            feature_domain_policy_sha256="0" * 64,
        )
