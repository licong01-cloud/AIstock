from __future__ import annotations

import json
from dataclasses import replace
from datetime import date

import numpy as np
import pytest

from backend.services.hmm_risk import state_model_set as subject


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
            validation_observations=validation,
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
