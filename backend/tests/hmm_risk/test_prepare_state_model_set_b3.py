from __future__ import annotations

import pytest

from backend.services.hmm_risk.state_model_set import ALL_CORE_FEATURES, BASE_FEATURES, StateModelSetError
from scripts.hmm_risk import prepare_state_model_set as subject


def _request() -> dict:
    dataset = {"schema_version": "dataset_v1"}
    mapping = {"schema_version": "mapping_v1"}
    return {
        "producer_commit": "c" * 40,
        "dataset_manifest_hash": subject.canonical_sha256(dataset),
        "mapping_manifest_hash": subject.canonical_sha256(mapping),
        "families": [
            {
                "family": "legacy_covfix",
                "feature_names": list(BASE_FEATURES),
                "preprocess_family": "identity",
            },
            {
                "family": "autocycle_all_core",
                "feature_names": list(ALL_CORE_FEATURES),
                "preprocess_family": "winsor_zscore_1_99_train_global_v1",
            },
        ],
    }


def test_legacy_fixed_seed_ready_writer_is_disabled() -> None:
    with pytest.raises(StateModelSetError, match="legacy fixed-seed preparation is disabled"):
        subject.prepare({}, artifact_root=None, output_root=None, db_prefix="TDX_DB_DEV_")


def test_formal_single_pass_runs_both_families_and_levels_without_selection_or_validation(monkeypatch) -> None:
    request = _request()
    inputs = {
        "dataset_manifest": {"schema_version": "dataset_v1"},
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
    }
    monkeypatch.setattr(subject, "_git_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)
    monkeypatch.setattr(
        subject,
        "_direct_train_series_for_family",
        lambda inputs, family: {"L1": {"L1": object()}, "L2": {"L2": object()}},
    )
    calls = []

    def fake_repeat(series, *, family, level, feature_names, preprocess_family, process_identity):
        calls.append((family, level, process_identity, len(feature_names), preprocess_family))
        return ({"family": family, "level": level, "schedule": list(range(42, 50))}, {})

    monkeypatch.setattr(subject, "run_level_repeat", fake_repeat)
    receipt = subject.prepare_b3_single_pass(
        request,
        db_prefix="TDX_DB_DEV_",
        process_identity="fresh_process_1",
    )

    assert len(calls) == 4
    assert set(receipt["level_repeats"]) == {
        "legacy_covfix:L1",
        "legacy_covfix:L2",
        "autocycle_all_core:L1",
        "autocycle_all_core:L2",
    }
    assert receipt["selection_performed"] is False
    assert receipt["validation_accessed_for_selection"] is False
    assert receipt["future_utility_accessed_for_selection"] is False
    assert receipt["ready_artifact_write_performed"] is False


def test_formal_single_pass_rejects_frozen_manifest_drift(monkeypatch) -> None:
    request = _request()
    monkeypatch.setattr(subject, "_git_commit", lambda: "c" * 40)
    monkeypatch.setattr(
        subject,
        "_load_l1_source_inputs",
        lambda request, db_prefix: {
            "dataset_manifest": {"schema_version": "drifted"},
            "mapping_manifest": {"schema_version": "mapping_v1"},
        },
    )
    with pytest.raises(StateModelSetError, match="dataset manifest hash mismatch"):
        subject.prepare_b3_single_pass(
            request,
            db_prefix="TDX_DB_DEV_",
            process_identity="fresh_process_1",
        )
