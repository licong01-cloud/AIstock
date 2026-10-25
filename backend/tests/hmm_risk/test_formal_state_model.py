from __future__ import annotations

import copy
import os
from datetime import date, timedelta
from importlib import import_module

import numpy as np
import pytest
from threadpoolctl import threadpool_info, threadpool_limits

from backend.services.hmm_risk import formal_state_model as subject
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_executor import THREAD_VARIABLES


@pytest.fixture(scope="module", autouse=True)
def single_thread_numerics():
    # These synthetic fits test the approved single-thread contract, not host defaults.
    # Scope both loaded native pools and env vars; restore them when the module ends.
    # fit_entry imports these lazily. Load them before capturing native pool limits.
    import_module("hmmlearn.hmm")
    import_module("sklearn.cluster")
    with pytest.MonkeyPatch.context() as patch:
        for key in THREAD_VARIABLES:
            patch.setenv(key, "1")
        with threadpool_limits(limits=1):
            yield


def test_numerical_fixtures_use_approved_single_thread_pools():
    pools = threadpool_info()
    assert pools and all(pool["num_threads"] == 1 for pool in pools)
    assert all(os.environ[key] == "1" for key in THREAD_VARIABLES)


def _selection_candidates():
    return [
        {
            "seed": seed,
            "entries": {
                "a": {"accepted": True, "final_train_likelihood": float(seed), "training_rows": 1, "feature_count": 1}
            },
        }
        for seed in subject.SEEDS
    ]


@pytest.fixture(scope="module")
def fitted(single_thread_numerics):
    rng = np.random.RandomState(42)
    means = np.tile(np.repeat([-3.0, 0.0, 3.0], 30), 6)
    values = np.column_stack((means + rng.normal(0, 0.4, len(means)), means * 0.7 + rng.normal(0, 0.3, len(means))))
    dates = [(date(2022, 1, 3) + timedelta(days=i)).isoformat() for i in range(len(values))]
    entry = subject.fit_entry(values, dates, 42)
    return values, dates, entry


def test_map_joint_stop_and_zero_refit_readback(fitted):
    values, dates, entry = fitted
    subject.validate_fit_entry(entry, values, dates)
    history = entry["history"]
    assert 2 <= len(history) <= 300
    assert abs(history[-1]["delta"]) <= history[-1]["tolerance"]
    assert entry["covariance"]["covariance_valid"]
    assert entry["final_train_likelihood"] == history[-1]["raw_likelihood"]
    repeated = subject.fit_entry(values, dates, 42)
    assert repeated == entry


@pytest.mark.parametrize("field", ["accepted", "final_train_likelihood", "train_posterior", "structure"])
def test_rehashed_candidate_cannot_forge_acceptance(fitted, field):
    values, dates, original = fitted
    entry = copy.deepcopy(original)
    if field == "accepted":
        entry[field] = not entry[field]
    elif field == "final_train_likelihood":
        entry[field] += 1
    elif field == "train_posterior":
        entry[field][0] = [1.0, 0.0, 0.0]
    else:
        entry[field]["states"][0]["count"] += 1
        entry[field] = subject.receipt({k: v for k, v in entry[field].items() if k != "receipt_sha256"})
    entry = subject.receipt({k: v for k, v in entry.items() if k != "receipt_sha256"})
    with pytest.raises(subject.FormalStateError, match="authority differs"):
        subject.validate_fit_entry(entry, values, dates)


@pytest.mark.parametrize("field", ["nu", "parameters", "initial_covariance", "initial_transmat"])
def test_rehashed_initialization_cannot_change_approved_profile(fitted, field):
    values, dates, entry = fitted
    changed = copy.deepcopy(entry)
    initial = changed["initialization"]
    if field == "nu":
        initial[field] = 2.0
    elif field == "parameters":
        initial[field]["tol"] = 1.0
    else:
        initial[field][0][0] += 0.01
    changed["initialization"] = subject.receipt({k: v for k, v in initial.items() if k != "receipt_sha256"})
    changed = subject.receipt({k: v for k, v in changed.items() if k != "receipt_sha256"})
    with pytest.raises(subject.FormalStateError, match="initialization formula/profile"):
        subject.validate_fit_entry(changed, values, dates)


def test_boolean_acceptance_and_integer_seed_are_not_numeric_equivalents(fitted):
    values, dates, entry = fitted
    changed = copy.deepcopy(entry)
    changed["accepted"] = int(entry["accepted"])
    with pytest.raises(subject.FormalStateError, match="authority differs"):
        subject.validate_fit_entry(
            subject.receipt({k: v for k, v in changed.items() if k != "receipt_sha256"}), values, dates
        )
    with pytest.raises(subject.FormalStateError, match="undeclared seed"):
        subject.parameter_profile(42.0, np.ones(2))
    candidates = _selection_candidates()
    candidates[0]["seed"] = 42.0
    with pytest.raises(subject.FormalStateError, match="schedule differs"):
        subject.select_restart(candidates, ["A", "B"])


def test_d3_sector_reference_prior_no_projection(fitted):
    values, _, _ = fitted
    model, initialization = subject.initialize(values, 42)
    reference = values.var(axis=0, ddof=0)
    n = np.asarray(initialization["counts"])[:, None]
    s = np.asarray(initialization["cluster_variance"])
    np.testing.assert_array_equal(model._covars_, (n * s + reference) / (n + 1))
    np.testing.assert_array_equal(model.covars_prior, np.tile(reference, (3, 1)))
    assert model.covars_weight == 2 and model.min_covar == 0 and model.init_params == ""


@pytest.mark.parametrize("bad", [0.0, float("nan"), float("inf"), -1.0])
def test_covariance_invalid_never_projected(fitted, bad):
    values, _, entry = fitted
    model = subject.restore_model(entry["model"])
    model._covars_[0, 0] = bad
    with pytest.raises(subject.FormalStateError):
        subject.covariance_audit(model, values, values.var(axis=0))
    assert model._covars_[0, 0] == bad or np.isnan(model._covars_[0, 0])


@pytest.mark.parametrize("bad", ([[True, False]], [[True, 2]], [["1", "2"]], [[None, 1]], [[1 + 2j, 1]]))
def test_numerical_contract_rejects_coercible_non_numeric_payloads(bad):
    with pytest.raises(subject.FormalStateError, match="real numeric"):
        subject.array(bad, (1, 2), "request values")


def test_preprocess_is_train_global_and_immutable():
    x = np.arange(240, dtype=float).reshape(120, 2)
    parameters = subject.preprocess_fit([x, x + 7], subject.FAMILIES[1])
    before = canonical_sha256(parameters)
    assert subject.preprocess_apply(np.array([[1e10, -1e10]]), parameters).shape == (1, 2)
    assert canonical_sha256(parameters) == before
    with pytest.raises(subject.FormalStateError):
        subject.preprocess_fit([np.ones((120, 2))], subject.FAMILIES[1])


def _projection_fixture():
    rng = np.random.RandomState(42)
    raw = rng.normal(size=(120, 20))
    raw[:, 19] = -0.0
    other_sector = rng.normal(loc=-3.0, size=(800, 20))
    parameters = subject.preprocess_fit([raw, other_sector], subject.FAMILIES[1])
    return raw, parameters


def test_d1_fixed_projection_after_full_preprocess_and_d6_zero_refit():
    raw, parameters = _projection_fixture()
    projected, projection = subject.project_training(
        raw, parameters, family=subject.FAMILIES[1], level="L2", sector="801207.SI", source_receipt_sha256="a" * 64
    )
    full = subject.preprocess_apply(raw, parameters)
    assert np.all(full[:, 19] == full[0, 19]) and full[0, 19] != 0
    assert projected.shape == (120, 19)
    assert projection["feature_count"] == 20 and projection["likelihood_feature_count"] == 19
    assert projection["inactive_feature_names"] == ["sf_dispersion_5d_neg"]
    assert projection["all_raw_inactive_values_exact_zero"]
    np.testing.assert_array_equal(projected, full[:, :19])
    np.testing.assert_array_equal(subject.project_validation(raw[:3], parameters, projection), projected[:3])
    assert subject.project_validation(np.empty((0, 20)), parameters, projection).shape == (0, 19)


@pytest.mark.parametrize("bad", [1e-300, 1.0, float("nan"), float("inf")])
def test_d1_no_near_zero_nonzero_or_nonfinite_allowlist(bad):
    raw, parameters = _projection_fixture()
    raw[0, 19] = bad
    with pytest.raises(subject.FormalStateError):
        subject.project_training(
            raw, parameters, family=subject.FAMILIES[1], level="L2", sector="801207.SI", source_receipt_sha256="a" * 64
        )


def test_d1_no_dynamic_projection_for_other_sector_or_level():
    raw, parameters = _projection_fixture()
    for level, sector in (("L2", "801206.SI"), ("L1", "801207.SI")):
        values, projection = subject.project_training(
            raw, parameters, family=subject.FAMILIES[1], level=level, sector=sector, source_receipt_sha256="a" * 64
        )
        assert values.shape == (120, 20) and projection["inactive_feature_indices"] == []
        with pytest.raises(subject.FormalStateError, match="sector-local variance"):
            subject.initialize(values, 42)
    rng = np.random.RandomState(43)
    full = rng.normal(size=(120, 20))
    identity_values, identity = subject.project_training(
        full, parameters, family=subject.FAMILIES[1], level="L2", sector="801206.SI", source_receipt_sha256="b" * 64
    )
    assert identity["likelihood_feature_count"] == 20 and identity["inactive_feature_indices"] == []
    np.testing.assert_array_equal(identity_values, subject.preprocess_apply(full, parameters))


def test_d1_future_finite_inactive_values_never_activate_or_mutate_frozen_model():
    raw, parameters = _projection_fixture()
    expected, projection = subject.project_training(
        raw, parameters, family=subject.FAMILIES[1], level="L2", sector="801207.SI", source_receipt_sha256="a" * 64
    )
    identities = canonical_sha256([parameters, projection])
    future = raw[:3].copy()
    future[:, 19] = [1e-300, -0.03452899677713199, 100.0]
    before = future.tobytes()
    np.testing.assert_array_equal(subject.project_validation(future, parameters, projection), expected[:3])
    assert future.tobytes() == before and canonical_sha256([parameters, projection]) == identities


@pytest.mark.parametrize("bad", [float("nan"), float("inf")])
def test_d1_inactive_inference_nonfinite_still_fails(bad):
    raw, parameters = _projection_fixture()
    _, projection = subject.project_training(
        raw, parameters, family=subject.FAMILIES[1], level="L2", sector="801207.SI", source_receipt_sha256="a" * 64
    )
    raw[0, 19] = bad
    with pytest.raises(subject.FormalStateError, match="full validation features"):
        subject.project_validation(raw, parameters, projection)


@pytest.mark.parametrize("drift", ["mask", "algorithm", "preprocess", "validation", "full_features"])
def test_d1_rehashed_drift_does_not_authorize_projection(drift):
    raw, parameters = _projection_fixture()
    _, projection = subject.project_training(
        raw, parameters, family=subject.FAMILIES[1], level="L2", sector="801207.SI", source_receipt_sha256="a" * 64
    )
    raw = raw[:3].copy()
    if drift == "mask":
        projection["active_feature_mask"][0] = 1
    elif drift == "algorithm":
        projection["algorithm_version"] = "unknown"
    elif drift == "preprocess":
        parameters = {**parameters, "center": [v + 0.1 for v in parameters["center"]]}
    elif drift == "validation":
        raw[0, 19] = float("nan")
    else:
        raw = raw[:, :19]
    projection = subject.receipt({k: v for k, v in projection.items() if k != "receipt_sha256"})
    with pytest.raises(subject.FormalStateError):
        subject.project_validation(raw, parameters, projection)


def test_d5_mixed_dimension_uses_effective_dimension_not_global_twenty():
    candidates = _selection_candidates()
    for candidate in candidates:
        candidate["entries"]["a"].update(final_train_likelihood=-1900.0, training_rows=100, feature_count=19)
    result = subject.select_restart(candidates, ["a"])
    assert result["selected_seed"] == 42
    assert result["candidates"][0]["scores"] == [-1.0]
