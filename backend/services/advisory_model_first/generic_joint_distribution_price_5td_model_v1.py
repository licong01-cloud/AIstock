"""One honest joint-distribution learner; paired sample mass, not two unrelated regressors."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import time

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_volume_path_price_5td_contracts_v1 import (
    FEATURES, KEY, MINUTE_FEATURES, POLICY, POLICY_SHA256, SCHEMA_SHA256 as INPUT_SCHEMA_SHA256,
    GenericPrice5TDConfigurationV1, check_resource_budget_v1,
)
from backend.services.advisory_model_first.generic_volume_path_price_5td_model_v1 import (
    matrix_v1, validate_fit_v1 as validate_control_v1, values_v1,
)
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

PARAMETERS = dict(n_estimators=128, max_depth=6, min_samples_leaf=30, max_features=1.,
                  bootstrap=True, random_state=20261007, n_jobs=2)
SCHEMA = "generic_joint_distribution_price_5td_v1"
SCHEMA_SHA256 = sha(dict(schema=SCHEMA, input_schema=INPUT_SCHEMA_SHA256, parameters=PARAMETERS,
    partition="MATURE_D_HALF_THEN_STRUCTURE_LABEL_END_STRICTLY_BEFORE_ESTIMATION_D",
    distribution="SAME_PAIRED_SAMPLES_TREE_LEAF_WEIGHTS_NORMALIZED_ONCE", path_quantile=.1))
ARMS = ("candidate", "matched")


@dataclass(frozen=True)
class GenericJointDistributionPrice5TDFitV1:
    recipe: dict
    forest: tuple
    calibration: dict
    diagnostics: dict
    model_sha256: str


def fitted_identity(fitted):
    return sha(dict(recipe=fitted.recipe, forest=fitted.forest, calibration=fitted.calibration))


def forest_leaf_ids_v1(forest, matrix):
    x = np.asarray(matrix, dtype=np.float32)
    if x.ndim != 2 or x.shape[1] != 39 or not np.isfinite(x).all() or len(x) > 100000:
        raise ValueError("joint forest query matrix contradicts finite float32 coordinates")
    output = np.zeros((len(x), len(forest)), dtype=np.int64)
    for ordinal, tree in enumerate(forest):
        feature, threshold, left, right = (np.asarray(tree[name]) for name in ("feature", "threshold", "left", "right"))
        if (not len(left) or len(left) > 127 or any(len(v) != len(left) for v in (feature, threshold, right))):
            raise ValueError("joint forest JSON node budget/shape differs")
        nodes = np.zeros(len(x), dtype=np.int64)
        for _ in range(7):  # Declared maximum depth six plus leaves; cannot follow a malicious cycle.
            active = np.flatnonzero(left[nodes] >= 0)
            if not len(active):
                break
            current = nodes[active]
            fields = feature[current].astype(int)
            if ((fields < 0) | (fields >= 39)).any():
                raise ValueError("joint forest JSON feature index differs")
            nodes[active] = np.where(x[active, fields] <= threshold[current], left[current], right[current])
            if ((nodes < 0) | (nodes >= len(left))).any():
                raise ValueError("joint forest JSON child index differs")
        if (left[nodes] >= 0).any():
            raise ValueError("joint forest JSON depth/cycle differs")
        output[:, ordinal] = nodes
    return output


def train_joint_price_5td_v1(*, rows, frozen_control, configuration, before_fit):
    from sklearn.ensemble import RandomForestRegressor
    import sklearn
    started = time.monotonic()
    check_resource_budget_v1(started)
    support = validate_control_v1(frozen_control)
    config = GenericPrice5TDConfigurationV1.model_validate(configuration)
    required = {*KEY, *FEATURES, *MINUTE_FEATURES, "observed_gap_bps", "gross_terminal_ratio",
                "path_min_ratio", "label_status", "label_information_end", "policy_sha256", "label_contract"}
    if (not isinstance(rows, pd.DataFrame) or len(rows) > 7720 or not rows.columns.is_unique
            or not required.issubset(rows) or rows.duplicated(list(KEY)).any()
            or not rows.policy_sha256.eq(POLICY_SHA256).all()
            or not rows.label_contract.eq(POLICY["label_contract"]).all()
            or frozen_control.recipe["configuration"] != config.model_dump(mode="json")):
        raise ValueError("joint training supervision/control policy identity differs")
    data = rows.copy(deep=True)
    for key in KEY[:2]:
        data[key] = pd.to_datetime(data[key])
    end = pd.Timestamp(config.train_end)
    domain = data.loc[data[KEY[0]].between(pd.Timestamp(config.train_start), end) & data[KEY[1]].le(end)].copy()
    # Do not parse features or label maturity outside the training clock.
    _, available = values_v1(domain)
    domain = domain.loc[available].reset_index(drop=True)
    maturity = pd.to_datetime(domain.label_information_end)
    if maturity[domain.label_status.eq("AVAILABLE")].isna().any():
        raise ValueError("joint AVAILABLE training label has no maturity")
    selected = domain.label_status.eq("AVAILABLE") & maturity.le(end)
    selected &= domain.observed_gap_bps.map(lambda value: False if pd.isna(value) else support.contains(float(value)))
    pool = domain.loc[selected].sort_values(list(KEY)).copy()
    pool_sha = sha([[str(v) for v in row] for row in pool.loc[:, KEY].itertuples(index=False, name=None)])
    if pool_sha != frozen_control.diagnostics.get("training_keys_sha256"):
        raise ValueError("joint mature pool differs from frozen control training KEY identity")
    dates = sorted(pool[KEY[0]].unique())
    if len(dates) < 2:
        raise ValueError("joint distribution lacks disjoint training date roles; no physical fit")
    split = pd.Timestamp(dates[len(dates)//2])
    structure = pool.loc[pool[KEY[0]].lt(split) & pd.to_datetime(pool.label_information_end).lt(split)]
    estimation = pool.loc[pool[KEY[0]].ge(split)]
    if structure.empty or estimation.empty:
        raise ValueError("joint distribution has an empty honest role; no physical fit")
    targets = pool.loc[:, ["gross_terminal_ratio", "path_min_ratio"]].map(lambda v: _number(v, positive=True))
    if (targets.isna().any().any()
            or (targets.path_min_ratio > targets.gross_terminal_ratio+1e-6).any()):
        raise ValueError("joint paired training targets contradict prices")
    medians = frozen_control.recipe["medians"]
    xs = matrix_v1(structure, medians, gaps=structure.observed_gap_bps, arm="candidate")
    xe = matrix_v1(estimation, medians, gaps=estimation.observed_gap_bps, arm="candidate")
    before_fit("candidate_joint_forest")
    estimator = RandomForestRegressor(**PARAMETERS)
    estimator.fit(xs, targets.loc[structure.index, "gross_terminal_ratio"])
    check_resource_budget_v1(started)
    leaves = estimator.apply(xe)
    forest = []
    for ordinal, tree in enumerate(estimator.estimators_):
        body = {name: getattr(tree.tree_, original).tolist() for name, original in
                (("feature", "feature"), ("threshold", "threshold"), ("left", "children_left"), ("right", "children_right"))}
        body["members"] = {str(int(leaf)): np.flatnonzero(leaves[:, ordinal] == leaf).tolist()
                           for leaf in np.unique(leaves[:, ordinal])}
        forest.append(body)
    if (not np.array_equal(forest_leaf_ids_v1(forest, xe), leaves)
            or not np.array_equal(forest_leaf_ids_v1(forest, xs), estimator.apply(xs))):
        raise ValueError("joint non-executing JSON leaf/apply parity failed")
    calibration = dict(terminal=targets.loc[estimation.index, "gross_terminal_ratio"].tolist(),
                       path=targets.loc[estimation.index, "path_min_ratio"].tolist(),
                       keys=[[str(v) for v in row] for row in estimation.loc[:, KEY].itertuples(index=False, name=None)])
    recipe = dict(schema=SCHEMA, schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256,
        input_schema_sha256=INPUT_SCHEMA_SHA256, parameters=PARAMETERS, medians=medians,
        intervals_bps=support.intervals_bps, configuration=config.model_dump(mode="json"),
        frozen_control_model_sha256=frozen_control.model_sha256, candidate_dimensions=39, path_quantile=.1)
    diagnostics = dict(physical_fit_count=1, internal_tree_count=len(forest), original_mature_pool_rows=len(pool),
        structure_rows=len(structure), estimation_rows=len(estimation), estimation_start=str(split.date()),
        structure_latest_label_end=str(pd.to_datetime(structure.label_information_end).max().date()),
        structure_keys_sha256=sha([[str(v) for v in row] for row in structure.loc[:, KEY].itertuples(index=False, name=None)]),
        estimation_keys_sha256=sha(calibration["keys"]), sklearn_version=sklearn.__version__,
        test_used_for_training_or_calibration=False, validation_used=False, deployable=False)
    fitted = GenericJointDistributionPrice5TDFitV1(recipe, tuple(forest), calibration, diagnostics, "")
    return GenericJointDistributionPrice5TDFitV1(recipe, fitted.forest, calibration, diagnostics, fitted_identity(fitted))


def validate_fit_v1(fitted):
    if (not isinstance(fitted, GenericJointDistributionPrice5TDFitV1) or fitted_identity(fitted) != fitted.model_sha256
            or fitted.recipe.get("schema_sha256") != SCHEMA_SHA256 or fitted.recipe.get("policy_sha256") != POLICY_SHA256
            or fitted.recipe.get("schema") != SCHEMA or fitted.recipe.get("input_schema_sha256") != INPUT_SCHEMA_SHA256
            or fitted.recipe.get("path_quantile") != .1
            or fitted.recipe.get("parameters") != PARAMETERS or fitted.recipe.get("candidate_dimensions") != 39
            or len(fitted.forest) != 128 or len(fitted.calibration["terminal"]) != len(fitted.calibration["path"])
            or not 0 < len(fitted.calibration["terminal"]) <= 7720):
        raise ValueError("joint distribution bundle/schema/policy identity differs")
    terminal, path = (np.asarray(fitted.calibration[name], dtype=float) for name in ("terminal", "path"))
    medians = np.asarray(fitted.recipe["medians"], dtype=float)
    if (medians.shape != (19,) or not np.isfinite(medians).all()
            or not np.isfinite([terminal, path]).all() or (terminal <= 0).any() or (path <= 0).any()
            or (path > terminal+1e-6).any() or len(fitted.calibration["keys"]) != len(terminal)):
        raise ValueError("joint serialized paired samples/encoding contradict prices")
    return ValueAnchorGapSupportV1(tuple(tuple(value) for value in fitted.recipe["intervals_bps"]))


def joint_distribution_v1(*, forest, calibration, matrix):
    """One mass per original estimation sample; trees do not create independent observations."""
    if len(matrix) > 128:
        raise ValueError("joint distribution query batch exceeds 128")
    leaves = forest_leaf_ids_v1(forest, matrix)
    n = len(calibration["terminal"])
    weights, known_trees = np.zeros((len(matrix), n)), np.zeros(len(matrix), dtype=int)
    for ordinal, tree in enumerate(forest):
        for leaf in np.unique(leaves[:, ordinal]):
            members = np.asarray(tree["members"].get(str(int(leaf)), ()), dtype=int)
            if not len(members):
                continue
            if (members < 0).any() or (members >= n).any() or len(np.unique(members)) != len(members):
                raise ValueError("joint estimation leaf member identity differs")
            selected = np.flatnonzero(leaves[:, ordinal] == leaf)
            weights[np.ix_(selected, members)] += 1/len(members)
            known_trees[selected] += 1
    total = weights.sum(axis=1)
    supported = total > 0
    weights[supported] /= total[supported, None]
    path = np.asarray(calibration["path"], dtype=float)
    order = np.argsort(path, kind="stable")
    cdf = np.cumsum(weights[:, order], axis=1)
    quantile = path[order[np.argmax(cdf >= .1-1e-12, axis=1)]]
    quantile[~supported] = np.nan
    mean = weights @ np.asarray(calibration["terminal"], dtype=float)
    mean[~supported] = np.nan
    return weights, mean, quantile, known_trees


def query_joint_price_nodes_v1(*, fitted, features, scenario_gap_bps):
    started = time.monotonic()
    support = validate_fit_v1(fitted)
    if not features.index.is_unique:
        raise ValueError("joint price query index differs")
    raw, available = values_v1(features)
    gaps = np.asarray([_number(value) for value in scenario_gap_bps], dtype=float)
    if gaps.shape != (len(features),) or (gaps[~np.isnan(gaps)] <= -10000).any():
        raise ValueError("joint hypothetical price coordinates differ")
    selected = np.flatnonzero(available & np.array([False if np.isnan(g) else support.contains(float(g)) for g in gaps]))
    result = pd.DataFrame(dict(status=["UNKNOWN_INPUT_OR_SUPPORT"]*len(features),
        expected_net_bps=np.full(len(features), np.nan), downside_q90_bps=np.full(len(features), np.nan),
        profit_probability=np.full(len(features), np.nan), distribution_unique_samples=np.zeros(len(features), dtype=int),
        distribution_known_trees=np.zeros(len(features), dtype=int),
        weight_effective_sample_size=np.full(len(features), np.nan)), index=features.index)
    for name in (*KEY, "selection_effective_rank", "candidate_group_size", "package_id", "run_id", "list_version_id",
                 "universe_identity", "source_evidence", "volume_path_reason", "volume_path_partial"):
        if name in features:
            result[name] = features[name].copy()
    result["input_unknown_fields"] = [tuple(name for name, value in zip((*FEATURES, *MINUTE_FEATURES), row, strict=True)
                                           if np.isnan(value)) for row in raw]
    result["model_sha256"], result["schema_sha256"], result["policy_sha256"] = fitted.model_sha256, SCHEMA_SHA256, POLICY_SHA256
    result["label_contract"], result["holding_period"] = POLICY["label_contract"], "T_OPEN_THROUGH_T_PLUS_4_CLOSE"
    terminal = np.asarray(fitted.calibration["terminal"])
    for start in range(0, len(selected), 128):
        check_resource_budget_v1(started)
        indices = selected[start:start+128]
        matrix = matrix_v1(features.iloc[indices], fitted.recipe["medians"], gaps=gaps[indices], arm="candidate")
        weights, mean, lower, trees = joint_distribution_v1(forest=fitted.forest, calibration=fitted.calibration, matrix=matrix)
        known = trees > 0
        entry = 1+gaps[indices]/10000
        net = 10000*(mean*(1-POLICY["sell_bps"]/10000)/(entry*(1+POLICY["buy_bps"]/10000))-1)
        risk = 10000*np.maximum(0., 1-lower/entry)
        sample_net = terminal[None, :]*(1-POLICY["sell_bps"]/10000)/(entry[:, None]*(1+POLICY["buy_bps"]/10000))-1
        probability = np.clip(np.sum(weights*(sample_net > 0), axis=1), 0., 1.)
        effective = np.full(len(indices), np.nan)
        effective[known] = 1/np.square(weights[known]).sum(axis=1)
        if not np.isfinite([net[known], risk[known], probability[known]]).all():
            raise ValueError("joint distribution price arithmetic is nonfinite")
        state = np.where(known, np.where((net > 0) & (risk <= POLICY["risk_bps"]), "ACCEPTABLE", "AVOID"),
                         "UNKNOWN_MODEL_DISTRIBUTION")
        result.iloc[indices, result.columns.get_loc("status")] = state
        for name, value in (("expected_net_bps", net), ("downside_q90_bps", risk),
                            ("profit_probability", np.where(known, probability, np.nan)),
                            ("distribution_unique_samples", (weights > 0).sum(axis=1)), ("distribution_known_trees", trees),
                            ("weight_effective_sample_size", effective)):
            result.iloc[indices, result.columns.get_loc(name)] = value
    result["effective_sample_size_is_time_independence"] = False
    return result


def joint_price_set_5td_v1(*, fitted, d_features, reference_cny, legal_low_cny, legal_high_cny, tick_cny):
    numbers = [_number(value, positive=True) for value in (reference_cny, legal_low_cny, legal_high_cny, tick_cny)]
    if any(value is None for value in numbers) or set(d_features) != {*FEATURES, *MINUTE_FEATURES}:
        raise ValueError("joint legal price coordinates or D features differ")
    reference, low, high, tick = numbers
    if low > high:
        raise ValueError("joint legal interval contradicts itself")
    step = Decimal(str(tick))
    first = int((Decimal(str(low))/step).to_integral_value(rounding=ROUND_CEILING))
    last = int((Decimal(str(high))/step).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("joint full legal tick grid exceeds budget")
    prices = [float(step*number) for number in range(first, last+1)]
    gaps = [float((Decimal(str(price))/Decimal(str(reference))-1)*10000) for price in prices]
    nodes = query_joint_price_nodes_v1(fitted=fitted,
        features=pd.DataFrame([d_features]*len(prices), columns=[*FEATURES, *MINUTE_FEATURES]), scenario_gap_bps=gaps)
    bands, start, end = [], None, None
    for price, state in zip(prices, nodes.status, strict=True):
        if state == "ACCEPTABLE":
            start = price if start is None else start
            end = price
        elif start is not None:
            bands.append((start, end))
            start = end = None
    if start is not None:
        bands.append((start, end))
    unknown = int(nodes.status.str.startswith("UNKNOWN_").sum())
    status = ("EMPTY_LEGAL_GRID" if not prices else "ACCEPTABLE_PRICE_SET" if bands
              else "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" if 0 < unknown < len(prices)
              else "UNKNOWN_INPUT_OR_SUPPORT" if unknown else "NO_ACCEPTABLE_PRICE")
    return dict(status=status, intervals_cny=tuple(bands), legal_node_count=len(prices), unknown_node_count=unknown,
        model_sha256=fitted.model_sha256, schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256,
        holding_sessions=5, price_basis="D_ANCHORED_CNY", label_contract=POLICY["label_contract"],
        evidence_use="NAVIGATION_ONLY", deployable=False, valuation_semantics="OBSERVED_OPEN_SCENARIO_ASSOCIATION_NOT_LIMIT_FILL")
