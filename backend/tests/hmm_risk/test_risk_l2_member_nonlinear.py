"""N直接合同：所有单测禁真实fit/外部访问，不复制历史大矩阵。"""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pytest

from backend.services.hmm_risk import risk_l2_member_nonlinear as h
from backend.services.hmm_risk.formal_state_executor import read_json
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt
from scripts.hmm_risk import run_risk_l2_member_nonlinear as cli

HEAD = "a" * 40
NUMERIC = dict(
    versions=h.prior.loss.EXPECTED_VERSIONS,
    thread_variables=dict.fromkeys(cli.THREAD_VARIABLES, "1"),
    thread_pools=[dict(num_threads=1)],
)


@pytest.fixture(autouse=True)
def no_external_actions():
    def forbidden(*args, **kwargs):
        raise AssertionError("unit tests forbid real fit/DB/network")

    with (
        patch("socket.socket.connect", forbidden),
        patch("psycopg2.connect", forbidden),
        patch("sklearn.linear_model.Ridge.fit", forbidden),
        patch("sklearn.ensemble.GradientBoostingRegressor.fit", forbidden),
    ):
        yield


def parameters():
    active = [True] * 20
    leaf = dict(left=[-1], right=[-1], feature=[-2], threshold=[-2.0], value=[0.25])
    return dict(
        original_scaler=dict(mean=[2.0] * 20, scale=[3.0] * 20, active=active),
        tree=dict(
            **h.identity_scaler(active), trees=[deepcopy(leaf) for _ in range(64)], init=-0.8, learning_rate=0.05
        ),
        tree_parameters=deepcopy(h.PARAMS),
    )


def test_original_within_math_and_identity_no_second_scaler(monkeypatch):
    rows = [
        ["d1", "A", [1.0] * 20, -0.7],
        ["d1", "B", [3.0] * 20, 0.2],
        ["d2", "A", [2.0] * 20, 0.5],
        ["d2", "B", [6.0] * 20, -0.1],
    ]
    monkeypatch.setattr(h.prior, "validate_packet", lambda p: p["rows"])
    monkeypatch.setattr(h.prior, "validate_model", lambda _: None)
    _, _, _, scaler = h.prior.loss._ridge_inputs(rows)
    packet = dict(rows=rows, vintage=h.prior.VINTAGES[0], receipt_sha256="p", mask_sha256="m")
    reference = dict(arm="S", vintage=packet["vintage"], packet_sha256="p", mask_sha256="m", parameters=scaler)
    x, y, weights, actual = h.prior.within_training(packet, reference)
    assert actual == scaler and np.allclose(weights, 1)
    assert np.allclose(x[:2].mean(axis=0), 0) and np.allclose(x[2:].mean(axis=0), 0)
    assert y.tolist() == pytest.approx([-0.45, 0.45, 0.3, -0.3])
    p = parameters()
    p["original_scaler"] = scaler
    h.validate_parameters(p)
    assert np.array_equal(h.trees.transform(h.pad(x, scaler["active"]), p["tree"]), x)
    changed = deepcopy(reference)
    changed["parameters"]["mean"][0] += 1
    with pytest.raises(FormalStateError, match="scaler"):
        h.prior.within_training(packet, changed)


def test_tree_json_float32_boundary_and_all_node_validation():
    p = parameters()
    t = dict(
        left=[1, -1, -1], right=[2, -1, -1], feature=[0, -2, -2], threshold=[1.0, -2.0, -2.0], value=[0.0, -0.2, 0.3]
    )
    p["tree"]["trees"][0] = t
    h.validate_parameters(p)
    x = np.zeros((2, 20))
    x[:, 0] = [1 + 1e-9, 1 + 1e-6]
    out = h.trees.tree_predict(x, p["tree"])
    assert out[1] - out[0] == pytest.approx(0.05 * 0.5)
    for key, value in (("left", [0, -1, -1]), ("right", [1, -1, -1]), ("feature", [20, -2, -2])):
        bad = deepcopy(p)
        bad["tree"]["trees"][0][key] = value
        with pytest.raises(FormalStateError):
            h.validate_parameters(bad)
    bad = deepcopy(p)
    bad["tree"]["mean"][0] = 2.0
    with pytest.raises(FormalStateError, match="identity"):
        h.validate_parameters(bad)
    with np.errstate(over="ignore"), pytest.raises(FormalStateError, match="input"):
        h.pad(np.full((1, 20), 1e100), [True] * 20)


def test_failure_consumes_fit_attempt_and_retry_never_refits(monkeypatch, tmp_path):
    wrapper = dict(
        source_commit=HEAD, receipt_sha256="w", training_sha256="train", reference_S={}, reference_W_model_sha256="old"
    )
    packet = dict(vintage=h.prior.VINTAGES[0])
    monkeypatch.setattr(h, "load_packet", lambda _: (packet, np.zeros((2, 20)), np.zeros(2), np.ones(2), {}))
    attempts = []

    def failing(*args, **kwargs):
        attempts.append(True)
        raise ValueError("injected fit failure")

    with patch("sklearn.ensemble.GradientBoostingRegressor.fit", failing):
        with pytest.raises(ValueError, match="injected"):
            h.fit_packet(wrapper, tmp_path / "out", tmp_path / "ledger", HEAD, NUMERIC)
        with pytest.raises(FileExistsError):
            h.fit_packet(wrapper, tmp_path / "other", tmp_path / "ledger", HEAD, NUMERIC)
    assert len(attempts) == 1
    assert read_json(tmp_path / "ledger/2026Q2.N.json")["fit_kernel_attempts"] == 1


def test_fit_export_close_and_identity_validation_without_real_fit(monkeypatch, tmp_path):
    x = np.zeros((4, 20))
    y, weights = np.array([-2.0, 2.0, -1.0, 1.0]), np.ones(4)
    scaler = h.identity_scaler([True] * 20)
    packet = dict(
        vintage=h.prior.VINTAGES[0],
        receipt_sha256="p",
        mask_sha256="m",
        catalog=[f"801{i:03d}.SI" for i in range(131)],
        rows=[[d] for d in ("a", "a", "b", "b")],
        release_binding=dict(manifest_sha256=h.CONTRACT["manifest_sha256"]),
    )
    wrapper = receipt(
        dict(
            source_commit=HEAD,
            training_sha256="u",
            reference_S=dict(model_sha256="s"),
            reference_W_model_sha256="w",
            vintage=packet["vintage"],
        )
    )
    monkeypatch.setattr(h, "load_packet", lambda _: (packet, x, y, weights, scaler))
    leaf = SimpleNamespace(
        children_left=np.array([-1]),
        children_right=np.array([-1]),
        feature=np.array([-2]),
        threshold=np.array([-2.0]),
        value=np.array([[[0.0]]]),
    )
    estimator = SimpleNamespace(tree_=leaf, apply=lambda z: np.zeros(len(z), dtype=int))
    fake = SimpleNamespace(
        estimators_=np.array([[estimator]] * 64),
        init_=SimpleNamespace(constant_=np.array([[0.0]])),
        get_params=lambda: h.PARAMS,
        predict=lambda z: np.zeros(len(z)),
    )
    called = []

    def fit(_, actual_x, actual_y, *, sample_weight):
        called.append(True)
        assert actual_x is x and actual_y is y and sample_weight is weights
        return fake

    with patch("sklearn.ensemble.GradientBoostingRegressor.fit", fit):
        model = h.fit_packet(wrapper, tmp_path / "output", tmp_path / "ledger", HEAD, NUMERIC)
    assert called == [True] and model["fit_dates"] == 2 and model["fits"] == 1
    assert model["leaf_diagnostics"][0] == {"0": dict(rows=4, distinct_dates=2)}
    assert read_json(tmp_path / "output/model.json") == model
    h.validate_model(model, HEAD)
    changed = deepcopy(model)
    changed["parameters"]["tree_parameters"]["max_depth"] = 3
    changed = receipt({k: v for k, v in changed.items() if k != "receipt_sha256"})
    with pytest.raises(FormalStateError, match="identity"):
        h.validate_model(changed, HEAD)


def test_request_bound_claim_and_six_attempt_ceiling(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "validate_output_location", lambda x: x)
    p = receipt(
        dict(
            fit_ledger_root=str(tmp_path / "ledger"),
            request_sha256="request",
            attempts=[dict(process=n, quarter=v["id"], arm="N") for n in (1, 2) for v in h.prior.VINTAGES],
        )
    )
    cli.claim_budget(p, HEAD)
    with pytest.raises(FileExistsError):
        cli.claim_budget(p, HEAD)
    monkeypatch.setattr(
        h,
        "asset",
        lambda *_: dict(
            p,
            schema_version=h.VERSION + "_preflight",
            contract=h.CONTRACT,
            source_commit=HEAD,
            status="PASS",
            fits=0,
            evaluation_outcomes_read=False,
        ),
    )
    args = SimpleNamespace(preflight="path", preflight_sha256="pin")
    assert len(cli.preflight(args, HEAD)["attempts"]) == 6
    p["attempts"].append(dict(process=3, quarter="2026Q2", arm="N"))
    with pytest.raises(FormalStateError, match="preflight"):
        cli.preflight(args, HEAD)


def test_fit_child_only_loads_explicit_quarter_and_validates_math(monkeypatch):
    packet = dict(vintage=h.prior.VINTAGES[0])
    x, y, w, scaler = np.ones((2, 20)), np.array([-2.0, 2.0]), np.ones(2), {}
    wrapper = receipt(
        dict(
            schema_version=h.VERSION + "_train",
            contract=h.CONTRACT,
            vintage=packet["vintage"],
            original_packet=dict(path="q2", sha256="p"),
            reference_S="q2S",
            training_sha256=h.training_identity(x, y, w, scaler),
        )
    )
    accessed = []
    monkeypatch.setattr(h, "asset", lambda path, pin: accessed.append((path, pin)) or packet)
    monkeypatch.setattr(h.prior, "within_training", lambda p, r: (x, y, w, scaler))
    h.load_packet(wrapper)
    assert accessed == [("q2", "p")]
    packet["vintage"] = h.prior.VINTAGES[1]
    with pytest.raises(FormalStateError, match="quarter"):
        h.load_packet(wrapper)


def test_full_grid_causal_center_no_outcome_population(monkeypatch):
    codes = [f"801{i:03d}.SI" for i in range(131)]
    days = ["2026-04-01", "2026-07-01", "2026-10-09"]
    p = parameters()
    models = {
        v["id"]: dict(catalog=codes, parameters=p, model_sha256=v["id"], release_binding={}) for v in h.prior.VINTAGES
    }
    accepted = dict(models=models, features_sha256="f", receipt_sha256="a")
    features = dict(
        catalog=codes,
        calendar=days,
        prediction_dates=days,
        receipt_sha256="f",
        release_binding={},
        as_of={d: "prior" for d in days},
        rows={},
    )
    for d in days:
        features["rows"][d] = {
            c: dict(features=[float(i)] * 20 if i < 2 else None, reason_code=None if i < 2 else "no_member")
            for i, c in enumerate(codes)
        }
    features["rows"][days[-1]][codes[1]] = dict(features=None, reason_code="no_member")
    baseline = dict(
        predictions=dict(C=[dict(trade_date=d, sector_code=c, expected_member_loss=0.1) for d in days for c in codes]),
        receipt_sha256="b",
    )
    monkeypatch.setattr(h, "validate_acceptance", lambda *_: None)
    monkeypatch.setattr(h.prior, "validate_prediction_features", lambda _: None)
    monkeypatch.setattr(h.core, "verify_receipt", lambda *_: None)
    monkeypatch.setattr(h.prior, "schedule", lambda _, v: dict(predict=[days[h.prior.VINTAGES.index(v)]]))
    observed = []

    def predict(x, tree):
        observed.append(x.copy())
        return x[:, 0]

    monkeypatch.setattr(h.trees, "tree_predict", predict)
    result = h.predict(accepted, features, baseline, HEAD)
    assert len(result["predictions"]) == 393 and result["evaluation_outcomes_read"] is False
    assert len(observed) == 2 and np.allclose(observed[0][:, 0], [-1 / 6, 1 / 6])
    last = result["predictions"][-131:]
    assert all(r["relative_member_return_score"] is None for r in last)
    assert last[0]["reason_code"] == "insufficient_cross_section"
    assert last[1]["reason_code"] == "no_member"


def test_ex_ante_ties_equal_budgets_value_algebra_and_legal_na():
    day, codes = "d", list("ABCDEFG")
    rows = [
        dict(trade_date=day, sector_code=c, relative_member_return_score=s)
        for c, s in zip(codes, [0, 0, 0, 1, 2, 3, None], strict=True)
    ]
    baseline = [dict(trade_date=day, sector_code=c, expected_member_loss=i / 10) for i, c in enumerate(codes)]
    groups = h.freeze_groups(rows, baseline, [day], codes)[day]
    assert len(groups["codes"]) == 6 and all(sum(groups[a].values()) == pytest.approx(2) for a in ("N", "C", "X"))
    assert [groups["N"][c] for c in "ABC"] == [2 / 3] * 3
    labels = {
        c: dict(status="AVAILABLE", reason=None, C=0.01, upside=i / 10, B=max(0, 0.01 - i / 10))
        for i, c in enumerate(codes)
    }
    values = h.compare_groups(groups, labels)
    expected = (
        sum((groups["C"][c] - groups["N"][c]) * (labels[c]["upside"] - labels[c]["C"]) for c in groups["codes"]) / 131
    )
    assert values["N_minus_C"]["J"] == pytest.approx(expected)
    assert values["N_minus_C"]["J"] == pytest.approx(
        values["N_minus_C"]["avoided_loss"] - values["N_minus_C"]["missed_upside"]
    )
    labels["A"] = dict(status="UNAVAILABLE", reason="FIXED_COHORT_ENDPOINT_NA")
    assert h.compare_groups(groups, labels)["N_minus_C"]["J"] is None
    immature = {c: dict(status="OUTCOME_NOT_MATURE") for c in codes}
    identical = dict(N=groups["N"], C=groups["N"], X=groups["X"])
    assert h.compare_groups(identical, immature)["N_minus_C"]["J"] is None


def test_guards_reject_network_db_and_all_unapproved_fit():
    import socket
    import psycopg2
    from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor
    from sklearn.linear_model import Ridge

    with cli.guard():
        for call in (
            lambda: socket.socket().connect(("localhost", 1)),
            lambda: psycopg2.connect(""),
            lambda: Ridge().fit([[0]], [0]),
            lambda: GradientBoostingRegressor().fit([[0]], [0]),
            lambda: HistGradientBoostingRegressor().fit([[0]], [0]),
        ):
            with pytest.raises(Exception, match="unauthorized|forbidden"):
                call()


def test_unsealed_evaluation_fails_before_any_outcome_access(monkeypatch):
    monkeypatch.setattr(h, "validate_acceptance", lambda *_: None)
    monkeypatch.setattr(h, "load_request", lambda *_: pytest.fail("must not load source before seal"))
    with pytest.raises((FormalStateError, KeyError)):
        h.evaluate(receipt({}), {}, receipt({}), receipt(dict(schema_version="unsealed")), HEAD)


def test_old_metric_role_adapter_does_not_copy_old_contract():
    assert h.relabel_statistics(dict(W_mse=1, paired_WC=2, W=dict(W_on_WS=3))) == dict(
        N_mse=1, paired_NC=2, N=dict(N_on_NS=3)
    )
