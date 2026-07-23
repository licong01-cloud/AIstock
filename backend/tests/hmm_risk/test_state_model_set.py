from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, timedelta
from types import SimpleNamespace

import numpy as np
import pytest

from backend.services.hmm_risk import state_model_set as subject
from scripts.hmm_risk import prepare_state_model_set as preparation


def _l2_payload(*, include_startprob: bool, feature_names=subject.BASE_FEATURES) -> tuple[bytes, tuple[str, ...]]:
    codes = tuple(f"L2-{index:03d}" for index in range(subject.EXPECTED_L2_COUNT))
    models = {}
    for index, code in enumerate(codes):
        entry = {
            "sector_code": code,
            "sector_name": f"Sector {index}",
            "n_states": 3,
            "covariance_type": "diag",
            "feature_names": list(feature_names),
            "means": [[-1.0] * len(feature_names), [0.0] * len(feature_names), [1.0] * len(feature_names)],
            "covars": [[0.5] * len(feature_names)] * 3,
            "transmat": [[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]],
            "state_labels": {"0": "fading", "1": "neutral", "2": "trending"},
            "training_days": 600,
        }
        if include_startprob:
            entry["startprob"] = [0.5, 0.3, 0.2]
        models[code] = entry
    return subject.canonical_json_bytes(models), codes


def test_legacy_parser_materializes_only_the_explicit_uniform_startprob_contract() -> None:
    payload, codes = _l2_payload(include_startprob=False)

    parsed = subject.parse_l2_artifact(
        payload,
        parser_contract=subject.PARSER_LEGACY_UNIFORM,
        expected_sha256=subject.sha256_bytes(payload),
        expected_sector_codes=codes,
        expected_features=subject.BASE_FEATURES,
    )

    assert parsed["sector_count"] == 131
    assert parsed["models"][codes[0]]["startprob_source"] == "legacy_uniform_startprob_v1"
    assert parsed["models"][codes[0]]["startprob"] == pytest.approx([1 / 3, 1 / 3, 1 / 3])

    with pytest.raises(subject.StateModelSetError, match="startprob is required"):
        subject.parse_l2_artifact(
            payload,
            parser_contract=subject.PARSER_AUTOCYCLE,
            expected_sha256=subject.sha256_bytes(payload),
            expected_sector_codes=codes,
            expected_features=subject.BASE_FEATURES,
        )


def test_l2_parser_rejects_hash_coverage_feature_and_semantic_drift() -> None:
    payload, codes = _l2_payload(include_startprob=True)
    kwargs = {
        "payload_bytes": payload,
        "parser_contract": subject.PARSER_AUTOCYCLE,
        "expected_sha256": subject.sha256_bytes(payload),
        "expected_sector_codes": codes,
        "expected_features": subject.BASE_FEATURES,
    }

    with pytest.raises(subject.StateModelSetError, match="SHA-256 mismatch"):
        subject.parse_l2_artifact(**{**kwargs, "expected_sha256": "0" * 64})
    with pytest.raises(subject.StateModelSetError, match="exactly 131"):
        subject.parse_l2_artifact(**{**kwargs, "expected_sector_codes": codes[:-1]})
    with pytest.raises(subject.StateModelSetError, match="feature definition"):
        subject.parse_l2_artifact(**{**kwargs, "expected_features": tuple(reversed(subject.BASE_FEATURES))})

    raw = json.loads(payload)
    raw[codes[0]]["state_labels"] = {"0": "neutral", "1": "neutral", "2": "trending"}
    drifted = subject.canonical_json_bytes(raw)
    with pytest.raises(subject.StateModelSetError, match="bijectively"):
        subject.parse_l2_artifact(
            drifted,
            parser_contract=subject.PARSER_AUTOCYCLE,
            expected_sha256=subject.sha256_bytes(drifted),
            expected_sector_codes=codes,
            expected_features=subject.BASE_FEATURES,
        )


def test_causal_forward_filter_prefix_is_invariant_and_rejects_bad_parameters() -> None:
    observations = np.asarray([[-1.0], [0.0], [1.0], [0.5]], dtype=np.float64)
    kwargs = {
        "startprob": np.asarray([0.5, 0.3, 0.2]),
        "transmat": np.asarray([[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]),
        "means": np.asarray([[-1.0], [0.0], [1.0]]),
        "covars": np.asarray([[0.3], [0.3], [0.3]]),
    }

    full = subject.causal_forward_posteriors(observations, **kwargs)
    prefix = subject.causal_forward_posteriors(observations[:-1], **kwargs)

    assert np.allclose(full[:-1], prefix, atol=1e-12, rtol=0)
    assert np.allclose(full.sum(axis=1), 1.0)
    with pytest.raises(subject.StateModelSetError, match="covariance must be positive"):
        subject.causal_forward_posteriors(observations, **{**kwargs, "covars": np.zeros((3, 1))})


def _training_series(feature_count: int = 7) -> dict[str, subject.L1TrainingSeries]:
    output = {}
    for index in range(subject.EXPECTED_L1_COUNT):
        rng = np.random.default_rng(1000 + index)
        train = np.vstack(
            [
                rng.normal(-2.0, 0.12, size=(50, feature_count)),
                rng.normal(0.0, 0.12, size=(50, feature_count)),
                rng.normal(2.0, 0.12, size=(50, feature_count)),
            ]
        )
        validation = np.vstack(
            [
                rng.normal(-2.0, 0.08, size=(20, feature_count)),
                rng.normal(0.0, 0.08, size=(20, feature_count)),
                rng.normal(2.0, 0.08, size=(20, feature_count)),
            ]
        )
        utility = validation[:, 0].copy()
        code = f"L1-{index:02d}"
        constituents = (f"L2-{index * 4:03d}", f"L2-{index * 4 + 1:03d}")
        output[code] = subject.L1TrainingSeries(
            sector_code=code,
            sector_name=f"L1 Sector {index}",
            train_observations=train,
            train_dates=tuple(date(2022, 1, 1) + timedelta(days=row) for row in range(train.shape[0])),
            validation_observations=validation,
            validation_dates=tuple(date(2024, 7, 1) + timedelta(days=row) for row in range(validation.shape[0])),
            validation_future_utility=utility,
            pit_l2_constituents=constituents,
            pit_constituent_manifest_hash=subject.canonical_sha256(constituents),
            observation_manifest_hash=subject.canonical_sha256({"sector": code, "rows": 210}),
        )
    return output


@pytest.fixture(scope="module")
def trained_l1() -> dict:
    return subject.train_l1_models(
        _training_series(),
        feature_names=subject.BASE_FEATURES,
        preprocess_family="identity",
        random_seed=42,
        observation_version="hmm_risk_l1_stock_fact_observation_v1",
    )


def test_train_l1_models_produces_31_direct_causal_three_state_entries(trained_l1: dict) -> None:
    assert trained_l1["sector_count"] == 31
    assert trained_l1["preprocess"]["family"] == "identity"
    for entry in trained_l1["models"].values():
        assert entry["state_origin"] == "direct_hmm"
        assert set(entry["state_labels"].values()) == subject.SEMANTIC_LABELS
        assert entry["causal_replay"] == "passed"
        assert entry["training_rows"] == 150


def test_train_l1_models_rejects_partial_layer_and_semantic_tie() -> None:
    incomplete = _training_series()
    incomplete.pop(next(iter(incomplete)))
    with pytest.raises(subject.StateModelSetError, match="exactly 31"):
        subject.train_l1_models(
            incomplete,
            feature_names=subject.BASE_FEATURES,
            preprocess_family="identity",
            random_seed=42,
            observation_version="hmm_risk_l1_stock_fact_observation_v1",
        )

    tied = _training_series()
    first = next(iter(tied))
    tied[first] = replace(tied[first], validation_future_utility=np.zeros(60))
    with pytest.raises(subject.StateModelSetError, match="semantic utility tie"):
        subject.train_l1_models(
            tied,
            feature_names=subject.BASE_FEATURES,
            preprocess_family="identity",
            random_seed=42,
            observation_version="hmm_risk_l1_stock_fact_observation_v1",
        )


def test_c008_seed_diagnostic_records_all_seeds_without_selection(monkeypatch) -> None:
    series = _training_series()

    def fake_fit(item, *, preprocess, feature_count, random_seed):
        del preprocess, feature_count, random_seed
        train = np.asarray(item.train_observations, dtype=np.float64)
        validation = np.asarray(item.validation_observations, dtype=np.float64)
        posteriors = np.zeros((validation.shape[0], 3), dtype=np.float64)
        posteriors[:20, 0] = 1.0
        posteriors[20:40, 1] = 1.0
        posteriors[40:, 2] = 1.0
        covars = np.ones((3, 7), dtype=np.float64)
        monitor_diagnostic = {
            "converged": True,
            "reason": "monitor_delta_below_tolerance",
            "iterations": 3,
            "maximum_iterations": 300,
            "tolerance": 0.01,
            "history": [-100.0, -90.0, -91.0],
            "deltas": [
                {
                    "history_index": 1,
                    "previous": -100.0,
                    "current": -90.0,
                    "absolute_delta": 10.0,
                    "relative_delta": 0.1,
                    "negative": False,
                    "terminal": False,
                },
                {
                    "history_index": 2,
                    "previous": -90.0,
                    "current": -91.0,
                    "absolute_delta": -1.0,
                    "relative_delta": -1.0 / 90.0,
                    "negative": True,
                    "terminal": True,
                },
            ],
            "negative_delta_count": 1,
            "minimum_absolute_delta": -1.0,
            "minimum_relative_delta": -1.0 / 90.0,
            "negative_delta_terminal_count": 1,
        }
        return subject._L1FitEvidence(
            train=train,
            validation=validation,
            startprob=np.asarray([1.0, 0.0, 0.0]),
            transmat=np.asarray([[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]),
            means=np.asarray([[-2.0] * 7, [0.0] * 7, [2.0] * 7]),
            raw_covars=covars,
            covars=covars,
            posteriors=posteriors,
            covariance_anomaly_count=0,
            covariance_diagnostic={
                **subject._covariance_diagnostic(covars, expected_shape=(3, 7)),
                "clip_performed": False,
                "bounded_min": 1.0,
                "bounded_max": 1.0,
            },
            monitor_converged=True,
            monitor_iterations=3,
            monitor_history=(-100.0, -90.0, -91.0),
            monitor_diagnostic=monitor_diagnostic,
        )

    monkeypatch.setattr(subject, "_fit_l1_evidence", fake_fit)
    report = subject.diagnose_l1_seed_grid(
        series,
        feature_names=subject.BASE_FEATURES,
        preprocess_family="identity",
    )

    assert report["seeds"] == list(range(42, 50))
    assert report["selection_performed"] is False
    assert report["artifact_write_performed"] is False
    assert set(report["seed_results"]) == {str(seed) for seed in range(42, 50)}
    for seed_result in report["seed_results"].values():
        assert seed_result["sector_count"] == 31
        assert seed_result["labelable_count"] == 31
        assert seed_result["semantic_unlabelable_count"] == 0
        assert seed_result["negative_likelihood_delta_sector_count"] == 31

    b1_report = subject.diagnose_l1_seed_grid_b1(
        series,
        feature_names=subject.BASE_FEATURES,
        preprocess_family="identity",
    )
    assert "diagnostic_algorithm_version" not in report
    assert b1_report["diagnostic_contract"] == "C-008-B1"
    assert b1_report["diagnostic_algorithm_version"] == subject.C008_B1_DIAGNOSTIC_VERSION
    b1_sector = b1_report["seed_results"]["42"]["sectors"]["L1-00"]
    assert sum(
        state["posterior_mass"] for state in b1_sector["train_posterior_evidence"]["states"].values()
    ) == pytest.approx(150.0)
    assert b1_sector["validation_posterior_evidence"]["states"]["1"]["effective_sample_size"] == pytest.approx(20.0)
    assert b1_sector["validation_time_segment_evidence"][2]["end_row_inclusive"] == 59
    assert b1_sector["convergence_evidence"]["negative_delta_terminal_count"] == 1
    assert b1_sector["covariance_evidence"]["clip_performed"] is False

    with pytest.raises(subject.StateModelSetError, match="must be exactly"):
        subject.diagnose_l1_seed_grid(
            series,
            feature_names=subject.BASE_FEATURES,
            preprocess_family="identity",
            seeds=(42,),
        )


def test_c008_b1_soft_evidence_exposes_mass_when_hard_state_is_missing() -> None:
    posteriors = np.asarray(
        [
            [0.6, 0.4, 0.0],
            [0.6, 0.4, 0.0],
            [0.0, 0.4, 0.6],
        ],
        dtype=np.float64,
    )
    utility = np.asarray([-1.0, 0.0, 1.0], dtype=np.float64)

    evidence = subject._posterior_state_evidence(posteriors, utility)

    assert evidence["states"]["1"]["hard_count"] == 0
    assert evidence["states"]["1"]["posterior_mass"] == pytest.approx(1.2)
    assert evidence["states"]["1"]["effective_sample_size"] == pytest.approx(3.0)
    assert evidence["states"]["1"]["posterior_weighted_utility"] == pytest.approx(0.0)
    assert evidence["row_sum_max_abs_error"] == pytest.approx(0.0)
    assert evidence["posterior_weighted_utility_pair_separation"]["0-2"] > 0.0


def test_c008_b1_covariance_diagnostic_separates_raw_failure_dimensions() -> None:
    raw = np.asarray(
        [
            [0.0, np.nan, 11.0],
            [1e-4, 0.5, 1.0],
            [0.1, 0.2, 0.3],
        ],
        dtype=np.float64,
    )

    evidence = subject._covariance_diagnostic(raw, expected_shape=(3, 3))

    assert evidence["shape_valid"] is True
    assert evidence["non_finite_count"] == 1
    assert evidence["non_positive_count"] == 1
    assert evidence["lower_bound_anomaly_count"] == 2
    assert evidence["upper_bound_anomaly_count"] == 1
    assert evidence["valid_for_bounding"] is False
    assert len(evidence["anomaly_mask_sha256"]) == 64


def test_c008_b1_monitor_diagnostic_serializes_non_finite_failure_history() -> None:
    model = SimpleNamespace(
        monitor_=SimpleNamespace(
            history=(-100.0, float("nan")),
            converged=True,
            iter=2,
            n_iter=300,
            tol=0.01,
        )
    )

    evidence = subject._monitor_diagnostic(model)

    assert evidence["history"] == [-100.0, None]
    assert evidence["history_non_finite_count"] == 1
    assert evidence["deltas"][0]["comparable"] is False
    assert evidence["deltas"][0]["absolute_delta"] is None
    assert evidence["negative_delta_count"] == 0
    subject.canonical_json_bytes(evidence)


def test_c008_b3_diag02_manual_initialization_is_explicit_and_seeded() -> None:
    rng = np.random.default_rng(20260723)
    train = np.vstack(
        [
            rng.normal(-2.0, 0.1, size=(20, 2)),
            rng.normal(0.0, 0.1, size=(20, 2)),
            rng.normal(2.0, 0.1, size=(20, 2)),
        ]
    )

    startprob, transmat, means, covars, evidence = subject._manual_b3_diag02_initialization(
        train,
        random_seed=42,
    )

    assert startprob == pytest.approx([1 / 3, 1 / 3, 1 / 3])
    assert transmat.sum(axis=1) == pytest.approx([1.0, 1.0, 1.0])
    assert np.all(np.diag(transmat) >= subject.HMM_MIN_SELF_TRANSITION)
    assert means.shape == covars.shape == (3, 2)
    assert sorted(evidence["cluster_counts"]) == [20, 20, 20]
    assert evidence["kmeans_parameters"]["n_init"] == 1
    profile = subject.c008_b3_diag02_parameter_profile()
    assert profile["gaussian_hmm"]["init_params"] == ""
    assert profile["numeric_contract_status"] == "DIAGNOSTIC_ONLY_NOT_APPROVED"


def test_c008_b3_diag02_fixed_environment_rejects_thread_drift(monkeypatch) -> None:
    import threadpoolctl

    for key in subject.C008_B3_DIAG02_FIXED_THREAD_ENV:
        monkeypatch.setenv(key, "1")
    monkeypatch.setattr(
        threadpoolctl,
        "threadpool_info",
        lambda: [{"user_api": "blas", "internal_api": "openblas", "num_threads": 1}],
    )

    evidence = subject.c008_b3_diag02_fixed_numeric_environment()

    assert evidence["scope"] == "same_host_same_fixed_numeric_environment_only"
    assert evidence["thread_pools"][0]["num_threads"] == 1
    monkeypatch.setenv("OPENBLAS_NUM_THREADS", "2")
    with pytest.raises(subject.StateModelSetError, match="requires fixed thread env value 1"):
        subject.c008_b3_diag02_fixed_numeric_environment()


def test_c008_b3_diag02_explicit_hmm_fit_preserves_raw_parameters() -> None:
    item = next(iter(_training_series().values()))
    preprocess = subject._fit_preprocess({item.sector_code: item}, preprocess_family="identity")
    family_variance = np.var(item.train_observations, axis=0, ddof=0)

    evidence = subject._fit_l1_b3_diag02_evidence(
        item,
        preprocess=preprocess,
        feature_count=len(subject.BASE_FEATURES),
        family_feature_variance=family_variance,
        random_seed=42,
    )

    assert evidence.train_posteriors.shape == (150, 3)
    assert evidence.validation_posteriors.shape == (60, 3)
    assert evidence.covariance_evidence["postfit_projection_performed"] is False
    assert evidence.covariance_evidence["formal_bounds_applied"] is False
    assert len(evidence.model_numeric_payload_sha256) == 64


def test_c008_b3_diag02_records_full_grid_without_selection(monkeypatch) -> None:
    series = _training_series()

    def fake_fit(item, *, preprocess, feature_count, family_feature_variance, random_seed):
        del preprocess, family_feature_variance, random_seed
        train = np.asarray(item.train_observations, dtype=np.float64)
        validation = np.asarray(item.validation_observations, dtype=np.float64)
        train_posteriors = np.zeros((train.shape[0], 3), dtype=np.float64)
        validation_posteriors = np.zeros((validation.shape[0], 3), dtype=np.float64)
        for state, indices in enumerate(np.array_split(np.arange(train.shape[0]), 3)):
            train_posteriors[indices, state] = 1.0
        for state, indices in enumerate(np.array_split(np.arange(validation.shape[0]), 3)):
            validation_posteriors[indices, state] = 1.0
        covars = np.ones((3, feature_count), dtype=np.float64)
        monitor = {
            "converged": True,
            "reason": "monitor_delta_below_tolerance",
            "iterations": 2,
            "maximum_iterations": 300,
            "tolerance": 0.01,
            "history": [-100.0, -90.0],
            "history_non_finite_count": 0,
            "deltas": [],
            "negative_delta_count": 0,
            "minimum_absolute_delta": None,
            "minimum_relative_delta": None,
            "negative_delta_terminal_count": 0,
        }
        return subject._B3Diag02FitEvidence(
            train=train,
            validation=validation,
            train_posteriors=train_posteriors,
            validation_posteriors=validation_posteriors,
            startprob=np.asarray([1 / 3, 1 / 3, 1 / 3]),
            transmat=np.asarray([[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]),
            means=np.zeros((3, feature_count)),
            raw_covars=covars,
            initialization_evidence={"thresholds_applied": False},
            monitor_evidence=monitor,
            covariance_evidence={"formal_bounds_applied": False},
            model_numeric_payload_sha256="a" * 64,
        )

    monkeypatch.setattr(subject, "_fit_l1_b3_diag02_evidence", fake_fit)
    report = subject.diagnose_l1_seed_grid_b3_diag02(
        series,
        feature_names=subject.BASE_FEATURES,
        preprocess_family="identity",
    )

    assert report["diagnostic_contract"] == "C-008-B3-DIAG-02"
    assert report["seeds"] == list(range(42, 50))
    assert report["all_restarts_completed"] is True
    assert report["selection_performed"] is False
    assert report["formal_acceptance_thresholds_applied"] is False
    assert report["hard_semantic_authority_changed"] is False
    assert report["d4_exact_contract_approved"] is False
    assert report["d5_01_exact_contract_approved"] is False
    assert report["d6_exact_contract_approved"] is False
    for seed in report["seed_results"].values():
        assert seed["sector_count"] == 31
        assert seed["fit_completed_count"] == 31
        assert seed["family_candidate_eligibility_evaluated"] is False
        assert seed["selection_performed"] is False
    sector = report["seed_results"]["42"]["sectors"]["L1-00"]
    assert sector["train_hard_sequence_evidence"]["states"]["0"]["contiguous_run_count"] == 1
    assert sector["validation_hard_sequence_evidence"]["states"]["0"]["calendar_month_count"] >= 1


def test_c008_b3_diag02_repeated_runner_requires_bitwise_canonical_equality(monkeypatch, tmp_path) -> None:
    payload = subject.canonical_json_bytes(
        {"schema_version": "single", "status": "diagnostic_complete", "families": []}
    )
    calls = []

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout=payload, stderr=b"warning\n")

    monkeypatch.setattr(preparation.subprocess, "run", fake_run)
    args = SimpleNamespace(
        request=str(tmp_path / "request.json"),
        artifact_root=str(tmp_path / "artifacts"),
        output_root=str(tmp_path / "output"),
        env_file=str(tmp_path / ".env"),
        db_env_prefix="TDX_DB_",
    )

    report, reproducible = preparation._run_c008_b3_diag02_repeated(args)

    assert reproducible is True
    assert len(calls) == 2
    assert report["reproducibility"]["fresh_process_repeat_count"] == 2
    assert report["reproducibility"]["canonical_payload_bitwise_equal"] is True
    assert report["reproducibility"]["numeric_tolerance_used_for_acceptance"] is False


def test_c008_diagnostic_report_is_immutable_and_content_hashed(tmp_path) -> None:
    report = {
        "schema_version": "hmm_risk_c008_seed_diagnostic_report_v1",
        "status": "diagnostic_complete",
        "selection_performed": False,
        "ready_artifact_write_performed": False,
        "families": [],
    }
    path = tmp_path / "diagnostic.json"

    digest = preparation._write_diagnostic_report(path, report)
    repeated = preparation._write_diagnostic_report(path, report)

    assert repeated == digest == subject.canonical_sha256(report)
    assert json.loads(path.read_text(encoding="utf-8")) == report
    with pytest.raises(subject.StateModelSetError, match="diagnostic report collision"):
        preparation._write_diagnostic_report(path, {**report, "status": "different"})


def _spec(source_sha: str) -> subject.StateModelSetSpec:
    return subject.StateModelSetSpec(
        family="legacy_covfix",
        family_version="legacy_covfix_l1_stock_fact_v1",
        producer_commit="a" * 40,
        created_at="2026-07-23T02:00:00+08:00",
        candidate_ids=("hmmc_001",),
        parser_contract=subject.PARSER_LEGACY_UNIFORM,
        source_l2_artifact_uri="configured://approved/models.json",
        source_l2_artifact_sha256=source_sha,
        train_start=date(2022, 1, 1),
        train_end=date(2024, 6, 30),
        validation_start=date(2024, 7, 1),
        validation_end=date(2025, 3, 31),
        common_data_watermark=date(2025, 4, 30),
        dataset_manifest={"schema_version": "dataset_v1", "hashes": ["a" * 64]},
        mapping_manifest={"schema_version": "mapping_v1", "sector_count": 31},
        feature_definition={"schema_version": "feature_v1", "features": list(subject.BASE_FEATURES)},
        observation_version="hmm_risk_l1_stock_fact_observation_v1",
        preprocess_family="identity",
    )


def test_build_and_write_ready_content_addressed_set(tmp_path, trained_l1: dict) -> None:
    payload, codes = _l2_payload(include_startprob=False)
    source_sha = subject.sha256_bytes(payload)
    l2 = subject.parse_l2_artifact(
        payload,
        parser_contract=subject.PARSER_LEGACY_UNIFORM,
        expected_sha256=source_sha,
        expected_sector_codes=codes,
        expected_features=subject.BASE_FEATURES,
    )

    manifest, l1_bytes, l2_bytes = subject.build_state_model_set(
        spec=_spec(source_sha),
        l1_artifact=trained_l1,
        l2_artifact=l2,
    )
    manifest_path = subject.write_state_model_set(
        tmp_path,
        manifest=manifest,
        l1_bytes=l1_bytes,
        l2_bytes=l2_bytes,
    )
    repeated = subject.write_state_model_set(
        tmp_path,
        manifest=manifest,
        l1_bytes=l1_bytes,
        l2_bytes=l2_bytes,
    )

    assert repeated == manifest_path
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["status"] == "READY"
    assert manifest["layers"]["L1"]["sector_count"] == 31
    assert manifest["layers"]["L2"]["sector_count"] == 131
    assert manifest["state_model_set_id"].startswith("hmms_")

    with pytest.raises(subject.StateModelSetError, match="artifact bytes differ"):
        subject.write_state_model_set(
            tmp_path / "bad",
            manifest=manifest,
            l1_bytes=l1_bytes + b" ",
            l2_bytes=l2_bytes,
        )


def test_build_model_set_rejects_cross_family_features(trained_l1: dict) -> None:
    payload, codes = _l2_payload(include_startprob=True, feature_names=subject.ALL_CORE_FEATURES)
    source_sha = subject.sha256_bytes(payload)
    l2 = subject.parse_l2_artifact(
        payload,
        parser_contract=subject.PARSER_AUTOCYCLE,
        expected_sha256=source_sha,
        expected_sector_codes=codes,
        expected_features=subject.ALL_CORE_FEATURES,
    )

    with pytest.raises(subject.StateModelSetError, match="feature families differ"):
        subject.build_state_model_set(
            spec=replace(
                _spec(source_sha),
                parser_contract=subject.PARSER_AUTOCYCLE,
                preprocess_family="winsor_zscore_1_99_train_global_v1",
            ),
            l1_artifact=trained_l1,
            l2_artifact=l2,
        )
