"""Synthetic mathematical/export/price contracts, not research fits or PnL."""
from copy import deepcopy
import math

import numpy as np
import pytest
from scipy.integrate import quad
from scipy.stats import beta as beta_distribution
from threadpoolctl import threadpool_limits

from backend.tests.advisory_model_first.test_original_slot_hurdle_price_5td_inputs_v1 import prepared_study as prepared_study
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_contracts_v1 import ARMS, COMPONENTS, FEATURES, ROSTER_KEY, sha
from backend.services.advisory_model_first import original_slot_hurdle_price_5td_model_v1 as model


@pytest.fixture(scope="session")
def fitted_study(prepared_study):
    plan, p, _ = prepared_study
    with threadpool_limits(limits=2):
        models = {a: {c: model.fit_component_v1(rows=p["domain"], encoding=p["encoding"], arm=a, component=c,
            plan_sha256=plan.plan_sha256) for c in COMPONENTS} for a in ARMS}
    return model.bundle_payload_v1(models=models, encoding=p["encoding"], plan_sha256=plan.plan_sha256)


def constant_bundle(bundle, *, probability=.55, gain=500., loss=100.):
    value = deepcopy(bundle)
    for arm in ARMS:
        sign = value["models"][arm]["sign"]["model"]
        width = len(value["models"][arm]["positive"]["model"]["coef"])
        sign.update(classes=[-1, 1], coef=[[0.]*width], intercept=[math.log(probability/(1-probability))],
            zero_class_status="ZERO_CLASS_NOT_OBSERVED")
        value["models"][arm]["positive"]["model"].update(coef=[0.]*width, intercept=math.log(gain/100))
        value["models"][arm]["negative"]["model"].update(coef=[0.]*width,
            intercept=math.log(loss/(10000-loss)), precision=10.)
        for component in COMPONENTS:
            entry = value["models"][arm][component]
            entry["component_sha256"] = sha({n: v for n, v in entry.items() if n != "component_sha256"})
    return model.bundle_payload_v1(models=value["models"], encoding=value["encoding"], plan_sha256=value["plan_sha256"])


@pytest.mark.parametrize("offset", [0., .3])
def test_beta_analytic_gradient_matches_finite_difference(offset):
    x = np.array([[-.2, .5], [.1, -.2], [.7, .3], [-.8, .2]])
    z, weights = np.array([.03, .11, .37, .64]), np.array([1., .5, 1., 2.])
    theta = np.array([-1.+offset, .2, -.1, math.log(10.)])
    _, grad = model.beta_objective_gradient_v1(theta, x, z, weights)
    finite = []
    for i in range(len(theta)):
        h = np.eye(len(theta))[i]*1e-6
        finite.append((model.beta_objective_gradient_v1(theta+h, x, z, weights)[0]
            - model.beta_objective_gradient_v1(theta-h, x, z, weights)[0])/2e-6)
    np.testing.assert_allclose(grad, finite, atol=1e-7, rtol=1e-6)


@pytest.mark.parametrize("mu,precision", [(.01, 10.), (.2, 12.), (.7, 3.), (.5, 50.)])
def test_terminal_tail_integral_matches_quadrature(fitted_study, mu, precision):
    bundle = constant_bundle(fitted_study, loss=10000*mu)
    for arm in ARMS:
        models = deepcopy(bundle["models"][arm])
        models["negative"]["model"]["precision"] = precision
        values, _ = model.predict_values_v1(models, np.zeros((1, len(models["positive"]["model"]["coef"]))))
        integral = quad(lambda z: (10000*z-800)*beta_distribution.pdf(z, mu*precision, (1-mu)*precision),
            .08, 1., epsabs=1e-8)[0]*(1-.55)
        assert values[0, 7] == pytest.approx(integral, abs=1e-5)


def test_native_estimator_and_nonexecuting_json_parity(prepared_study, monkeypatch):
    from sklearn.linear_model import GammaRegressor, LogisticRegression
    plan, p, _ = prepared_study
    native, original_sign, original_positive = {}, LogisticRegression.fit, GammaRegressor.fit
    def sign_fit(self, *args, **kwargs):
        output = original_sign(self, *args, **kwargs)
        native["sign"] = self
        return output
    def positive_fit(self, *args, **kwargs):
        output = original_positive(self, *args, **kwargs)
        native["positive"] = self
        return output
    monkeypatch.setattr(LogisticRegression, "fit", sign_fit)
    monkeypatch.setattr(GammaRegressor, "fit", positive_fit)
    rows = p["domain"].copy()
    rows.loc[rows.label_cluster.eq(rows.label_cluster.iloc[0]), "net_return_bps"] = 0.
    with threadpool_limits(limits=2):
        heads = {c: model.fit_component_v1(rows=rows, encoding=p["encoding"], arm=ARMS[0], component=c,
            plan_sha256=plan.plan_sha256) for c in COMPONENTS}
    x = model.matrix_v1(rows, rows.observed_gap_bps, p["encoding"], ARMS[0])
    values, _ = model.predict_values_v1(heads, x)
    probability = native["sign"].predict_proba(x)
    np.testing.assert_allclose(values[:, [1, 2, 0]], probability, atol=1e-12)
    np.testing.assert_allclose(values[:, 3], native["positive"].predict(x)*100, atol=1e-10)
    assert heads["sign"]["model"]["zero_class_status"] == "ZERO_CLASS_OBSERVED"
    assert np.all((values[:, 4] > 0) & (values[:, 4] < 10000))


def test_cross_package_same_inputs_and_unknown_domains(prepared_study, fitted_study):
    _, p, _ = prepared_study
    features = p["domain"].iloc[[0, len(p["domain"])//2]].copy()
    for arm in ARMS:
        prediction = model.query_hurdle_nodes_v1(bundle=fitted_study, features=features,
            scenario_gap_bps=features.observed_gap_bps, arm=arm)
        np.testing.assert_array_equal(prediction[list(model.VALUE_FIELDS)].iloc[0], prediction[list(model.VALUE_FIELDS)].iloc[1])
        prediction = model.query_hurdle_nodes_v1(bundle=fitted_study, features=features, scenario_gap_bps=[-800, 800], arm=arm)
        assert prediction.status.eq("UNKNOWN_GAP_SUPPORT").all() and prediction.expected_net_bps.isna().all()
    features.loc[:, list(FEATURES)] = np.nan
    prediction = model.query_hurdle_nodes_v1(bundle=fitted_study, features=features, scenario_gap_bps=[0., 0.], arm=ARMS[0])
    assert prediction.status.eq("UNKNOWN_STOCK_INPUT").all()


def test_full_decimal_grid_preserves_unknown_holes_and_empty_set(prepared_study, fitted_study):
    _, p, _ = prepared_study
    bundle = constant_bundle(fitted_study)
    bundle["encoding"]["intervals_bps"] = [[-300., -100.], [100., 300.]]
    for arm in ARMS:
        for component in COMPONENTS:
            entry = bundle["models"][arm][component]
            entry["recipe"]["encoding_sha256"] = sha(bundle["encoding"])
            entry["component_sha256"] = sha({n: v for n, v in entry.items() if n != "component_sha256"})
    bundle = model.bundle_payload_v1(models=bundle["models"], encoding=bundle["encoding"], plan_sha256=bundle["plan_sha256"])
    row = p["domain"].iloc[0]
    kwargs = dict(bundle=bundle, d_features={n: row[n] for n in FEATURES}, source_context={n: row[n] for n in ROSTER_KEY},
        reference_cny="10", legal_low_cny="9.70", legal_high_cny="10.30", tick_cny=".01", arm=ARMS[1])
    result = model.hurdle_price_set_v1(**kwargs)
    assert result["original_tick_count"] == 61 and result["unknown_nodes"] == 19
    assert result["intervals_cny"] == [["9.70", "9.90"], ["10.10", "10.30"]]
    assert result["probabilities_calibrated"] is False and result["path_loss_guaranteed"] is False
    result = model.hurdle_price_set_v1(**{**kwargs, "legal_low_cny": "10.001", "legal_high_cny": "10.009"})
    assert result["status"] == "EMPTY_LEGAL_GRID" and result["nodes"] == []
    result = model.hurdle_price_set_v1(**{**kwargs, "reference_cny": None})
    assert result["status"] == "UNKNOWN" and result["original_tick_count"] == 61
    result = model.hurdle_price_set_v1(**{**kwargs, "tick_cny": None})
    assert result["status"] == "UNKNOWN" and not result["complete_grid"] and result["original_tick_count"] is None
    with pytest.raises(ValueError, match="100000"):
        model.hurdle_price_set_v1(**{**kwargs, "legal_high_cny": "10000"})


def test_zero_or_missing_heads_never_fallback(prepared_study, fitted_study):
    plan, p, _ = prepared_study
    rows = p["domain"].copy()
    rows.net_return_bps = 0.
    with pytest.raises(ValueError, match="PREPARED_NO_FIT"):
        model.fit_component_v1(rows=rows, encoding=p["encoding"], arm=ARMS[0], component="sign", plan_sha256=plan.plan_sha256)
    rows.observed_gap_bps = np.nan
    unavailable = model.fit_encoding_v1(rows, ())
    assert unavailable["preprocessing_status"] == "UNAVAILABLE_NO_OBSERVED_TRAIN_PRICE"
    with pytest.raises(ValueError, match="PREPARED_NO_FIT"):
        model.bundle_payload_v1(models=fitted_study["models"], encoding=unavailable, plan_sha256=plan.plan_sha256)
    corrupt = deepcopy(fitted_study)
    del corrupt["models"][ARMS[0]]["negative"]
    with pytest.raises(ValueError, match="six"):
        model.validate_bundle_v1(corrupt)
    corrupt = deepcopy(fitted_study)
    corrupt["models"][ARMS[0]]["positive"]["model"]["intercept"] += 1
    with pytest.raises(ValueError, match="identity"):
        model.validate_bundle_v1(corrupt)


def test_beta_optimizer_failure_is_visible(prepared_study, monkeypatch):
    import scipy.optimize
    from types import SimpleNamespace
    plan, p, _ = prepared_study
    monkeypatch.setattr(scipy.optimize, "minimize", lambda *a, **k: SimpleNamespace(success=False))
    with pytest.raises(ValueError, match="MODEL_FIT_FAILED:negative"):
        model.fit_component_v1(rows=p["domain"], encoding=p["encoding"], arm=ARMS[0], component="negative", plan_sha256=plan.plan_sha256)
