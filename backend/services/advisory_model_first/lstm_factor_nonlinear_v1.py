"""A single restricted nonlinear candidate on the frozen 54-coordinate population."""
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import lstm_factor_information_v1 as info

DAY, KEY, GROUP, FEATURES = info.DAY, info.KEY, info.GROUP, info.FEATURES
PARAMS = dict(
    ridge=dict(alpha=100., fit_intercept=False, copy_X=True, max_iter=None, tol=.0001,
               solver="cholesky", positive=False, random_state=None),
    hgb=dict(loss="squared_error", quantile=None, learning_rate=.05, max_iter=120,
             max_leaf_nodes=3, max_depth=2, min_samples_leaf=100, l2_regularization=10.,
             max_features=1., max_bins=255, categorical_features=None, monotonic_cst=None,
             interaction_cst=None, warm_start=False, early_stopping=False, scoring="loss",
             validation_fraction=.1, n_iter_no_change=10, tol=1e-7, verbose=0, random_state=20261021),
)
CONTRACT = dict(schema="lstm_factor_nonlinear_v1", hypothesis="FIXED54_RESTRICTED_NONLINEAR_MSE_V1",
    target=info.prior.LSTM, fields=FEATURES, folds=info.spo.base.FOLDS, params=PARAMS,
    parent_input_contract_sha256=info.spo.sha(info.CONTRACT), max_new_fits=6,
    prior_fits=251, prior_index=1, development=info.CONTRACT["development"], stress=info.CONTRACT["stress"],
    label="GROUP_CENTERED_FIXED5_NET_BPS", encoding="FROZEN54_TRAIN_MEDIAN_IQR_MISSING_GROUP_CENTERED",
    support="ORIGINAL_EIGHT_UNCHANGED", score="UNCALIBRATED_GROUP_RELATIVE_BPS_DECISION_SCORE",
    final_fit=False, target_bps=100., production_ready=False, independent_confirmation=False)


def factory(arm):
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.linear_model import Ridge
    if arm not in PARAMS:
        raise ValueError("unregistered learner")
    estimator = (Ridge if arm == "ridge" else HistGradientBoostingRegressor)(**PARAMS[arm])
    if estimator.get_params(deep=False) != PARAMS[arm]:
        raise ValueError("installed constructor parameters differ from the frozen contract")
    return estimator


def training(rows, fold, expected_keys):
    if type(fold) is not int or fold not in (1, 2, 3):
        raise ValueError("registered fold required")
    info.check_inputs(rows)
    cutoff = info.spo.base.FOLDS[fold - 1][0]
    if not rows[DAY].le(pd.Timestamp(cutoff)).all():
        raise ValueError("future training rows")
    part, stats = info.spo.training(rows, cutoff)
    keys = info.spo.sha([[str(v) for v in row] for row in part[KEY].itertuples(index=False, name=None)])
    if keys != expected_keys:
        raise ValueError("original complete training keys differ")
    raw = info.numeric(part)
    active, median, scale = info.encoding(raw)
    fields = [f for f, a in zip(FEATURES, active, strict=True) if a]
    x = info.spo.group_center(info.transform(raw[:, active], median, scale), part)
    y = info.spo.group_center(part.hypothetical_liquidation_net_bps.to_numpy(float)[:, None], part).ravel()
    if not np.isfinite(x).all() or not np.isfinite(y).all():
        raise ValueError("nonfinite supervised values")
    return x, y, dict(fields=fields, median=median.tolist(), scale=scale.tolist(),
        inactive_fields=[f for f, a in zip(FEATURES, active, strict=True) if not a],
        cutoff=cutoff, train_rows=len(part), train_days=int(part[DAY].nunique()),
        training_support=stats, training_keys_sha256=keys)


def export_hgb(estimator):
    if estimator.n_iter_ != 120 or len(estimator._predictors) != 120:
        raise ValueError("HGB did not complete the fixed 120 iterations")
    trees = []
    for iteration in estimator._predictors:
        if len(iteration) != 1:
            raise ValueError("regression requires one numeric tree per iteration")
        nodes = []
        for node in iteration[0].nodes:
            if node["is_categorical"]:
                raise ValueError("categorical tree outside the frozen numeric contract")
            if node["is_leaf"]:
                nodes.append(dict(leaf=True, value=float(node["value"])))
            else:
                nodes.append(dict(leaf=False, feature=int(node["feature_idx"]), threshold=float(node["num_threshold"]),
                                  left=int(node["left"]), right=int(node["right"])))
        trees.append(nodes)
    return dict(bias=float(estimator._baseline_prediction.ravel()[0]), trees=trees)


def validate_tree(nodes, dimensions):
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= 5:
        raise ValueError("tree size exceeds three-leaf contract")
    reached, stack, leaves = set(), [(0, 0)], 0
    while stack:
        i, depth = stack.pop()
        if i in reached or not 0 <= i < len(nodes) or depth > 2:
            raise ValueError("cyclic, shared, invalid or excessively deep tree")
        reached.add(i)
        n = nodes[i]
        if type(n.get("leaf")) is not bool:
            raise ValueError("invalid leaf discriminator")
        if n["leaf"]:
            if set(n) != {"leaf", "value"} or isinstance(n["value"], bool) or not np.isfinite(n["value"]):
                raise ValueError("invalid numeric leaf")
            leaves += 1
        else:
            if (set(n) != {"leaf", "feature", "threshold", "left", "right"}
                    or type(n["feature"]) is not int or not 0 <= n["feature"] < dimensions
                    or isinstance(n["threshold"], bool) or not np.isfinite(n["threshold"])
                    or any(type(n[k]) is not int or n[k] <= i for k in ("left", "right"))):
                raise ValueError("invalid numeric split")
            stack.extend([(n["left"], depth + 1), (n["right"], depth + 1)])
    if len(reached) != len(nodes) or leaves > 3:
        raise ValueError("orphan node or excess leaves")


def validate_model(packet):
    arm, fold = packet.get("arm"), packet.get("fold")
    if arm not in PARAMS or type(fold) is not int or fold not in (1, 2, 3):
        raise ValueError("model learner/fold differs")
    if (packet.get("schema") != CONTRACT["schema"] or packet.get("contract_sha256") != info.spo.sha(CONTRACT)
            or packet.get("payload_sha256") != info.spo.sha({k: v for k, v in packet.items() if k != "payload_sha256"})
            or packet.get("target") != info.prior.LSTM or packet.get("params") != PARAMS[arm]
            or packet.get("cutoff") != info.spo.base.FOLDS[fold - 1][0]):
        raise ValueError("model contract/parameters/hash/clock differ")
    fields = packet["fields"]
    if not fields or fields != [f for f in FEATURES if f in fields]:
        raise ValueError("model field order/duplicates differ")
    median, scale = (np.asarray(packet[k], float) for k in ("median", "scale"))
    if (median.shape != (len(fields),) or scale.shape != median.shape
            or not np.isfinite(np.r_[median, scale]).all() or (scale <= 0).any()):
        raise ValueError("invalid model encoding")
    if arm == "ridge":
        coef = np.asarray(packet["learner"]["coef"], float)
        if coef.shape != (2 * len(fields),) or not np.isfinite(coef).all():
            raise ValueError("invalid Ridge coefficients")
    else:
        learner = packet["learner"]
        if (isinstance(learner["bias"], bool) or not np.isfinite(learner["bias"])
                or not isinstance(learner["trees"], list) or len(learner["trees"]) != 120):
            raise ValueError("invalid HGB ensemble")
        for tree in learner["trees"]:
            validate_tree(tree, 2 * len(fields))


def score_matrix(x, arm, learner):
    if arm == "ridge":
        return x @ np.asarray(learner["coef"], float)
    result = np.full(len(x), learner["bias"], dtype=float)
    for nodes in learner["trees"]:
        stack = [(0, np.arange(len(x)))]
        while stack:
            index, rows = stack.pop()
            if not len(rows):
                continue
            node = nodes[index]
            if node["leaf"]:
                result[rows] += node["value"]
            else:
                left = x[rows, node["feature"]] <= node["threshold"]
                stack.extend([(node["left"], rows[left]), (node["right"], rows[~left])])
    return result


def fit(rows, fold, arm, expected_keys, *, before_fit, budget=lambda: None):
    estimator = factory(arm)
    x, y, meta = training(rows, fold, expected_keys)
    budget()
    before_fit()
    estimator.fit(x, y, sample_weight=np.ones(len(y)))
    budget()
    learner = dict(coef=estimator.coef_.tolist()) if arm == "ridge" else export_hgb(estimator)
    result = score_matrix(x, arm, learner)
    np.testing.assert_allclose(result, estimator.predict(x), rtol=1e-11, atol=1e-8)
    certificate = dict(training_completed=True, export_max_error=float(np.max(np.abs(result - estimator.predict(x)))),
                       train_mse=float(np.mean((result - y) ** 2)), optimization="FIXED_RECIPE_NOT_GLOBAL_OPTIMALITY_PROOF")
    if arm == "ridge":
        lhs, rhs = x.T @ x + 100. * np.eye(x.shape[1]), x.T @ y
        residual = float(np.max(np.abs(lhs @ estimator.coef_ - rhs)) / max(1., np.max(np.abs(rhs))))
        if residual > 1e-8:
            raise ValueError("Ridge normal equation residual exceeds the frozen tolerance")
        certificate["relative_normal_equation_residual"] = residual
    else:
        certificate["fixed_iterations"] = int(estimator.n_iter_)
    packet = dict(schema=CONTRACT["schema"], contract_sha256=info.spo.sha(CONTRACT), target=info.prior.LSTM,
        arm=arm, fold=fold, params=PARAMS[arm], learner=learner, certificate=certificate, **meta,
        production_ready=False, score_semantics=CONTRACT["score"])
    packet["payload_sha256"] = info.spo.sha(packet)
    validate_model(packet)
    return packet, estimator


def feature_matrix(inputs, packet):
    info.check_inputs(inputs)
    info.numeric(inputs)  # Reject unreviewed infinities even in an inactive coordinate.
    validate_model(packet)
    eligible = np.isfinite(info.spo.numeric(inputs)).any(axis=1)
    raw = info.numeric(inputs.loc[eligible], packet["fields"])
    x = info.transform(raw, np.asarray(packet["median"]), np.asarray(packet["scale"]))
    x = info.spo.group_center(x, inputs.loc[eligible]) if len(x) else x
    return x, eligible


def predict(inputs, packet, *, phase, oracle=None):
    x, eligible = feature_matrix(inputs, packet)
    fold = packet["fold"]
    _, first, last = info.spo.base.FOLDS[fold - 1]
    if phase == "stress" and fold == 3:
        first, last = CONTRACT["stress"]
    elif phase != "fold":
        raise ValueError("registered offline phase required")
    if not inputs[DAY].between(pd.Timestamp(first), pd.Timestamp(last)).all():
        raise ValueError("prediction date outside registered fold/stress")
    values = score_matrix(x, packet["arm"], packet["learner"])
    if oracle is not None and len(x):
        np.testing.assert_allclose(values, oracle.predict(x), rtol=1e-11, atol=1e-8)
    if len(values):
        values = info.spo.group_center(values[:, None], inputs.loc[eligible]).ravel()
    if not np.isfinite(values).all():
        raise ValueError("nonfinite model scores")
    result = inputs.loc[:, [*info.spo.IDENTITY, "parent_score", "source_id"]].copy().reset_index(drop=True)
    result["decision_score"], result["no_rank_advice"] = np.nan, ~eligible
    result["advisory_rank"] = result.selection_effective_rank.copy()
    result.loc[eligible, "decision_score"] = values
    for _, group in result.groupby(GROUP, sort=False):
        active = group.loc[~group.no_rank_advice]
        order = active.sort_values(["decision_score", "selection_effective_rank"], ascending=[False, True], kind="stable").index
        result.loc[order, "advisory_rank"] = sorted(active.selection_effective_rank)
    result["selected"] = result.advisory_rank.le(10)
    info.spo.evaluation.same_population(inputs, result)
    return result


def summarize(panel, ridge_panel):
    result = info.summarize(panel, ridge_panel)
    result["versus_matched_ridge"] = result.pop("versus_eight_fields")
    result["comparison_semantics"] = "HGB_MINUS_MATCHED_54_FIELD_MSE_RIDGE"
    return result


def decision(development, stress, fold_means):
    previous = info.decision(development, stress, fold_means)
    mapping = dict(STOP_THIS_FACTOR_CANDIDATE="STOP_NONLINEAR_CANDIDATE_PAUSE_CURRENT_FACTOR_MODEL_SEARCH",
        BELOW_BUSINESS_TARGET_NO_EXPANSION="BELOW_TARGET_PAUSE_CURRENT_FACTOR_MODEL_SEARCH")
    previous["action"] = mapping.get(previous["action"], previous["action"])
    previous["current_factor_model_search_paused"] = previous["action"] in set(mapping.values())
    return previous


class FitClaims:
    """One owner, six create-only identities; all STARTED consume financial budget."""
    def __init__(self, root, digest):
        self.root, self.digest = Path(root), digest

    def write(self, arm, fold, stage_sha256=None):
        if arm not in PARAMS or type(fold) is not int or fold not in (1, 2, 3):
            raise ValueError("unregistered fit identity")
        self.root.mkdir(parents=True, exist_ok=True)
        starts = list(self.root.glob("*_started.json"))
        allowed = {f"{a}{f}_started.json" for a in PARAMS for f in (1, 2, 3)}
        for path in starts:
            if path.name not in allowed or json.loads(path.read_bytes())["plan_sha256"] != self.digest:
                raise ValueError("foreign fit claim")
        name = self.root / f"{arm}{fold}_started.json"
        if stage_sha256 is None:
            if len(starts) >= 6:
                raise ValueError("six-start financial budget exhausted")
            payload = dict(plan_sha256=self.digest, arm=arm, fold=fold, status="STARTED",
                           observed_at=datetime.now(timezone.utc).isoformat())
        else:
            if not name.exists() or len(stage_sha256) != 64:
                raise ValueError("finished claim requires its actual start and stage")
            name = self.root / f"{arm}{fold}_finished.json"
            payload = dict(plan_sha256=self.digest, arm=arm, fold=fold, status="FINISHED", stage_sha256=stage_sha256)
        with name.open("x", encoding="utf8") as handle:
            json.dump(payload, handle, sort_keys=True)
