"""Independent 19D honest joint-price model; no QE, execution, package gate or study runner."""
from copy import deepcopy
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import re
import time
from types import MappingProxyType

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_models_v1 import feature_values, matrix
from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import MATRIX_ORDER
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

ARMS = ("matched_anchor", "candidate_transfer")
PARAMETERS = MappingProxyType(dict(n_estimators=128, max_depth=6, min_samples_leaf=30, max_features=1.,
    bootstrap=True, random_state=20261007, n_jobs=2, criterion="squared_error"))
SCHEMA = "generic_population_price_5td_model_v1"
SCHEMA_SHA256 = sha(dict(schema=SCHEMA, matrix_order=MATRIX_ORDER, parameters=dict(PARAMETERS),
    policy_sha256=POLICY_SHA256, distribution="PAIRED_ESTIMATION_SAMPLES_EQUAL_WITHIN_LEAF_EQUAL_KNOWN_TREES",
    quantile="INVERTED_CDF_DIRECT_PATH_RISK", partition="P26_SHARED_SPLIT_STRICT_LABEL_END_PURGE"))
POOLS = {"STRUCTURE", "ESTIMATION", "PURGED_LABEL_OVERLAP", "NOT_TRAIN_SUPERVISION"}


@dataclass(frozen=True)
class PopulationPrice5TDFitV1:
    recipe: dict
    forest: tuple
    calibration: dict
    diagnostics: dict
    model_sha256: str


def fitted_identity_v1(fitted):
    return sha(dict(recipe=fitted.recipe, forest=fitted.forest, calibration=fitted.calibration))


def _budget(started):
    import psutil
    if time.monotonic()-started > 1800 or psutil.Process().memory_info().rss > 8*1024**3:
        raise ValueError("population model exceeds its 30-minute/8GiB process budget")


def _encoding(encoding):
    if not isinstance(encoding, dict) or encoding.get("matrix_order") != list(MATRIX_ORDER):
        raise ValueError("population model requires its independent 19D encoding")
    medians = np.asarray([_number(v) for v in encoding.get("medians", ())], dtype=float)
    if medians.shape != (9,) or not np.isfinite(medians).all():
        raise ValueError("population shared train-only medians differ")
    support = ValueAnchorGapSupportV1(tuple(tuple(v) for v in encoding["intervals_bps"]))
    first = pd.Timestamp(encoding["estimation_first_D"])
    if pd.isna(first) or first.tz is not None or first != first.normalize():
        raise ValueError("population honest split requires an original D")
    return medians.tolist(), support, first


def _integers(values):
    if not isinstance(values, (list, tuple)) or any(not isinstance(v, (int, np.integer))
            or isinstance(v, (bool, np.bool_)) for v in values):
        raise ValueError("population JSON node/member indices must be integers")
    return np.asarray(values, dtype=np.int64)


def _tree_arrays(tree):
    if not isinstance(tree, dict) or set(tree) != {"feature", "threshold", "left", "right", "members"}:
        raise ValueError("population JSON tree fields differ")
    feature, left, right = (_integers(tree[n]) for n in ("feature", "left", "right"))
    threshold = np.asarray([_number(v) for v in tree["threshold"]], dtype=float)
    n = len(left)
    if not 0 < n <= 127 or any(len(a) != n for a in (feature, right, threshold)) or not np.isfinite(threshold).all():
        raise ValueError("population JSON tree shape/node budget differs")
    leaf = left == -1
    nodes = np.arange(n)
    if (not np.array_equal(leaf, right == -1) or (feature[leaf] != -2).any()
            or ((feature[~leaf] < 0) | (feature[~leaf] >= 19)).any()
            or any(((a[~leaf] <= nodes[~leaf]) | (a[~leaf] >= n)).any() for a in (left, right))):
        raise ValueError("population JSON child/feature/cycle differs")
    children = np.concatenate((left[~leaf], right[~leaf]))
    if not np.array_equal(np.bincount(children, minlength=n), np.r_[0, np.ones(n-1, dtype=int)]):
        raise ValueError("population JSON tree has shared or unreachable nodes")
    depth = np.zeros(n, dtype=int)
    for node in nodes[~leaf]:
        depth[left[node]] = depth[right[node]] = depth[node]+1
    if depth.max() > 6 or not isinstance(tree["members"], dict):
        raise ValueError("population JSON tree depth/members differ")
    return feature, threshold, left, right


def forest_leaf_ids_v1(forest, x):
    values = np.asarray(x, dtype=np.float32)
    if values.ndim != 2 or values.shape[1] != 19 or len(values) > 100000 or not np.isfinite(values).all():
        raise ValueError("population forest requires finite float32 19D coordinates")
    if not isinstance(forest, (list, tuple)) or not 0 < len(forest) <= 128:
        raise ValueError("population forest tree count differs")
    output = np.zeros((len(values), len(forest)), dtype=np.int64)
    for ordinal, tree in enumerate(forest):
        fields, thresholds, left, right = _tree_arrays(tree)
        nodes = np.zeros(len(values), dtype=np.int64)
        for _ in range(7):
            active = np.flatnonzero(left[nodes] != -1)
            if not len(active):
                break
            current = nodes[active]
            nodes[active] = np.where(values[active, fields[current]] <= thresholds[current], left[current], right[current])
        if (left[nodes] != -1).any():
            raise ValueError("population forest cannot reach leaves within depth six")
        output[:, ordinal] = nodes
    return output


def train_population_price_5td_v1(*, clusters, encoding, arm, input_plan_sha256, before_fit, after_fit):
    """Exactly one forest fit; callbacks are the caller's attempt/QE hooks, not package admission."""
    from sklearn.ensemble import RandomForestRegressor
    import sklearn
    from threadpoolctl import threadpool_limits
    started = time.monotonic()
    _budget(started)
    medians, support, first = _encoding(encoding)
    field = arm+"_pool" if arm in ARMS else None
    required = {*KEY, *FEATURES, "observed_gap_bps", "gross_terminal_ratio", "path_min_ratio", "label_status",
                "label_information_end", "policy_sha256", "label_contract", field}
    if (field is None or not isinstance(input_plan_sha256, str) or not re.fullmatch("[a-f0-9]{64}", input_plan_sha256)
            or not callable(before_fit) or not callable(after_fit) or not isinstance(clusters, pd.DataFrame)
            or len(clusters) > 200000 or not clusters.columns.is_unique or not required.issubset(clusters)
            or clusters.duplicated(list(KEY)).any() or not clusters[field].isin(POOLS).all()):
        raise ValueError("population model source/pool/attempt identity differs")
    # Do not parse any features or outcomes in purged/held/evaluation rows.
    pool = clusters.loc[clusters[field].isin(["STRUCTURE", "ESTIMATION"])].sort_values(list(KEY)).copy()
    if pool.empty or len(pool) > 100000:
        raise ValueError("population training pool is empty or exceeds unique-cluster budget; no fit")
    for key in (*KEY[:2], "label_information_end"):
        pool[key] = pd.to_datetime(pool[key])
        if pool[key].isna().any() or pool[key].dt.tz is not None or not pool[key].eq(pool[key].dt.normalize()).all():
            raise ValueError("population supervision clocks must be original dates")
    d, t, h = (pool[n] for n in (*KEY[:2], "label_information_end"))
    structure = pool[field].eq("STRUCTURE")
    estimation = pool[field].eq("ESTIMATION")
    if (not structure.any() or not estimation.any() or not d.between("2024-07-04", "2025-05-30").all()
            or not (d.lt(t) & t.le(h) & h.le(pd.Timestamp("2025-05-30")) & h.lt(pd.Timestamp("2025-06-03"))).all()
            or not (d[structure].lt(first) & h[structure].lt(first)).all() or not d[estimation].ge(first).all()
            or not pool.label_status.eq("AVAILABLE").all() or not pool.policy_sha256.eq(POLICY_SHA256).all()
            or not pool.label_contract.eq(POLICY["label_contract"]).all()):
        raise ValueError("population policy/label end/honest role differs; no fit")
    for role, mask in (("STRUCTURE", structure), ("ESTIMATION", estimation)):
        if encoding["pools"][arm].get(role) != int(mask.sum()):
            raise ValueError("population original encoding pool count differs; no fit")
    _, stock_known = feature_values(pool)
    gaps = np.asarray([_number(v) for v in pool.observed_gap_bps], dtype=float)
    targets = pool.loc[:, ["gross_terminal_ratio", "path_min_ratio"]].map(lambda v: _number(v, positive=True)).astype(float)
    if (not stock_known.all() or not np.isfinite(gaps).all() or not all(support.contains(float(g)) for g in gaps)
            or targets.isna().any().any() or targets.path_min_ratio.gt(targets.gross_terminal_ratio).any()):
        raise ValueError("population paired supervision or price support differs; no fit")
    xs = matrix(pool.loc[structure], medians, gaps=gaps[structure])
    xe = matrix(pool.loc[estimation], medians, gaps=gaps[estimation])
    estimator = RandomForestRegressor(**PARAMETERS)
    before_fit(arm)
    try:
        with threadpool_limits(limits=2):
            estimator.fit(xs, targets.loc[structure, "gross_terminal_ratio"])
    finally:
        after_fit(arm)
    _budget(started)
    leaves = estimator.apply(xe)
    forest = []
    for ordinal, tree in enumerate(estimator.estimators_):
        body = {n: getattr(tree.tree_, old).tolist() for n, old in
                (("feature", "feature"), ("threshold", "threshold"), ("left", "children_left"), ("right", "children_right"))}
        body["members"] = {str(int(leaf)): np.flatnonzero(leaves[:, ordinal] == leaf).tolist() for leaf in np.unique(leaves[:, ordinal])}
        forest.append(body)
    if (not np.array_equal(forest_leaf_ids_v1(forest, xs), estimator.apply(xs))
            or not np.array_equal(forest_leaf_ids_v1(forest, xe), leaves)):
        raise ValueError("population independent JSON/float32 leaf parity differs")
    calibration = dict(terminal=targets.loc[estimation, "gross_terminal_ratio"].tolist(),
        path=targets.loc[estimation, "path_min_ratio"].tolist(),
        keys=[[str(v) for v in row] for row in pool.loc[estimation, KEY].itertuples(index=False, name=None)],
        label_ends=[v.date().isoformat() for v in h[estimation]])
    recipe = dict(schema=SCHEMA, schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256,
        matrix_order=list(MATRIX_ORDER), dimensions=19, arm=arm, parameters=dict(PARAMETERS),
        input_plan_sha256=input_plan_sha256, encoding_sha256=sha(encoding), medians=medians,
        intervals_bps=support.intervals_bps, estimation_first_D=first.date().isoformat(),
        quantile="INVERTED_CDF_DIRECT_PATH_RISK")
    diagnostics = dict(physical_fit_count=1, internal_tree_count=128, structure_rows=int(structure.sum()),
        estimation_rows=int(estimation.sum()), structure_latest_label_end=h[structure].max().date().isoformat(),
        structure_keys_sha256=sha([[str(v) for v in row] for row in pool.loc[structure, KEY].itertuples(index=False, name=None)]),
        estimation_keys_sha256=sha(calibration["keys"]), sklearn_version=sklearn.__version__,
        evaluation_used_for_training=False, deployable=False)
    fitted = PopulationPrice5TDFitV1(recipe, tuple(forest), calibration, diagnostics, "")
    return PopulationPrice5TDFitV1(recipe, fitted.forest, calibration, diagnostics, fitted_identity_v1(fitted))


def validate_fit_v1(fitted):
    if (not isinstance(fitted, PopulationPrice5TDFitV1) or fitted_identity_v1(fitted) != fitted.model_sha256
            or fitted.recipe.get("schema") != SCHEMA or fitted.recipe.get("schema_sha256") != SCHEMA_SHA256
            or fitted.recipe.get("policy_sha256") != POLICY_SHA256 or fitted.recipe.get("matrix_order") != list(MATRIX_ORDER)
            or fitted.recipe.get("dimensions") != 19 or fitted.recipe.get("parameters") != dict(PARAMETERS)
            or fitted.recipe.get("arm") not in ARMS or len(fitted.forest) != 128
            or fitted.recipe.get("quantile") != "INVERTED_CDF_DIRECT_PATH_RISK"):
        raise ValueError("population model JSON/schema/policy identity differs")
    if any(not isinstance(fitted.recipe.get(n), str) or not re.fullmatch("[a-f0-9]{64}", fitted.recipe[n])
           for n in ("input_plan_sha256", "encoding_sha256")):
        raise ValueError("population model input/encoding identity differs")
    medians, support, first = _encoding(fitted.recipe)
    del medians
    calibration = fitted.calibration
    terminal, path = (np.asarray([_number(v, positive=True) for v in calibration[n]], dtype=float) for n in ("terminal", "path"))
    n = len(terminal)
    if (not 0 < n <= 100000 or terminal.shape != (n,) or path.shape != (n,) or not np.isfinite([terminal, path]).all()
            or (path <= 0).any() or (terminal <= 0).any() or (path > terminal).any()
            or len(calibration["keys"]) != n or len(calibration["label_ends"]) != n
            or any(not isinstance(k, (list, tuple)) or len(k) != 3 or any(not isinstance(v, str) or not v for v in k)
                   for k in calibration["keys"])
            or len({tuple(k) for k in calibration["keys"]}) != n
            or fitted.diagnostics.get("estimation_rows") != n):
        raise ValueError("population serialized paired estimation mass differs")
    clocks = pd.DataFrame(calibration["keys"], columns=KEY)
    d, t, h = (pd.to_datetime(pd.Series(v)) for v in (clocks[KEY[0]], clocks[KEY[1]], calibration["label_ends"]))
    if (any(v.isna().any() or v.dt.tz is not None or not v.eq(v.dt.normalize()).all() for v in (d, t, h))
            or not (d.ge(first) & d.lt(t) & t.le(h) & h.le(pd.Timestamp("2025-05-30"))).all()):
        raise ValueError("population serialized estimation clock differs")
    diagnostic = fitted.diagnostics
    latest = pd.Timestamp(diagnostic.get("structure_latest_label_end"))
    if (diagnostic.get("physical_fit_count") != 1 or diagnostic.get("internal_tree_count") != 128
            or not isinstance(diagnostic.get("structure_rows"), int) or not 0 < diagnostic["structure_rows"] <= 100000
            or diagnostic.get("evaluation_used_for_training") is not False or diagnostic.get("deployable") is not False
            or diagnostic.get("estimation_keys_sha256") != sha(calibration["keys"])
            or not isinstance(diagnostic.get("structure_keys_sha256"), str)
            or not re.fullmatch("[a-f0-9]{64}", diagnostic["structure_keys_sha256"])
            or pd.isna(latest) or latest.tz is not None or not latest < first):
        raise ValueError("population serialized training diagnostics differ")
    for tree in fitted.forest:
        _, _, left, _ = _tree_arrays(tree)
        members = []
        for key, ids in tree["members"].items():
            if not isinstance(key, str) or not re.fullmatch("0|[1-9][0-9]*", key) or int(key) >= len(left) or left[int(key)] != -1:
                raise ValueError("population serialized leaf membership key differs")
            values = _integers(ids)
            members.extend(values.tolist())
        if sorted(members) != list(range(n)):
            raise ValueError("population tree duplicates or drops original estimation mass")
    return support


def model_payload_v1(fitted):
    validate_fit_v1(fitted)
    return asdict(fitted)


def model_from_payload_v1(payload):
    if not isinstance(payload, dict) or set(payload) != {"recipe", "forest", "calibration", "diagnostics", "model_sha256"}:
        raise ValueError("population non-executing JSON payload differs")
    values = deepcopy(payload)
    values["forest"] = tuple(values["forest"])
    fitted = PopulationPrice5TDFitV1(**values)
    validate_fit_v1(fitted)
    return fitted


def joint_weights_v1(*, forest, calibration, x):
    if len(x) > 128:
        raise ValueError("population distribution batch exceeds 128")
    leaves = forest_leaf_ids_v1(forest, x)
    n = len(calibration["terminal"])
    weights, known = np.zeros((len(x), n)), np.zeros(len(x), dtype=int)
    for ordinal, tree in enumerate(forest):
        for leaf in np.unique(leaves[:, ordinal]):
            ids = _integers(tree["members"].get(str(int(leaf)), ()))
            if not len(ids):
                continue
            if (ids < 0).any() or (ids >= n).any() or len(np.unique(ids)) != len(ids):
                raise ValueError("population query leaf member mass differs")
            selected = np.flatnonzero(leaves[:, ordinal] == leaf)
            weights[np.ix_(selected, ids)] += 1/len(ids)
            known[selected] += 1
    good = known > 0
    weights[good] /= known[good, None]
    return weights, known


def weighted_quantile_v1(values, weights, probability):
    values = np.asarray(values, dtype=float)
    weights = np.asarray(weights, dtype=float)
    if (values.ndim != 1 or weights.shape != values.shape or not np.isfinite([values, weights]).all()
            or (weights < 0).any() or not 0 < probability <= 1):
        raise ValueError("population weighted quantile coordinates differ")
    total = weights.sum()
    if total <= 0:
        return np.nan
    positive = np.flatnonzero(weights > 0)
    order = positive[np.argsort(values[positive], kind="stable")]
    cdf = np.cumsum(weights[order])/total
    cdf[-1] = 1.
    pos = np.searchsorted(cdf, probability, side="left")
    return float(values[order[min(pos, len(order)-1)]])


def query_population_price_nodes_v1(*, fitted, features, scenario_gap_bps):
    started = time.monotonic()
    support = validate_fit_v1(fitted)
    if not features.index.is_unique or len(features) > 100000:
        raise ValueError("population query original index/row budget differs")
    raw, stock_known = feature_values(features)
    gaps = np.asarray([_number(v) for v in scenario_gap_bps], dtype=float)
    if gaps.shape != (len(features),) or (gaps[~np.isnan(gaps)] <= -10000).any():
        raise ValueError("population hypothetical price coordinates differ")
    supported = np.array([False if np.isnan(g) else support.contains(float(g)) for g in gaps], dtype=bool)
    status = np.where(~stock_known, "UNKNOWN_STOCK_INPUT", np.where(np.isnan(gaps), "UNKNOWN_PRICE_SCENARIO",
        np.where(~supported, "UNKNOWN_GAP_SUPPORT", "UNKNOWN_MODEL_DISTRIBUTION")))
    result = pd.DataFrame(dict(status=status,
        expected_net_bps=np.nan, downside_q90_bps=np.nan, profit_probability=np.nan, path_min_ratio_q10=np.nan,
        distribution_unique_samples=0, distribution_known_trees=0, weight_effective_sample_size=np.nan), index=features.index)
    result["input_unknown_fields"] = [tuple(n for n, v in zip(FEATURES, row, strict=True) if np.isnan(v)) for row in raw]
    for name in (*KEY, "source_id", "package_id", "run_id", "list_version_id", "selection_effective_rank", "candidate_group_size"):
        if name in features:
            result[name] = features[name].copy()
    terminal, path = (np.asarray(fitted.calibration[n], dtype=float) for n in ("terminal", "path"))
    selected = np.flatnonzero(stock_known & supported)
    for start in range(0, len(selected), 128):
        _budget(started)
        indices = selected[start:start+128]
        weights, known = joint_weights_v1(forest=fitted.forest, calibration=fitted.calibration,
            x=matrix(features.iloc[indices], fitted.recipe["medians"], gaps=gaps[indices]))
        for index, w, trees, gap in zip(indices, weights, known, gaps[indices], strict=True):
            if not trees:
                continue
            entry = 1+gap/10000
            net = 10000*(terminal*(1-POLICY["sell_bps"]/10000)/(entry*(1+POLICY["buy_bps"]/10000))-1)
            risk = 10000*np.maximum(0., 1-path/entry)
            mean, downside = float(w @ net), weighted_quantile_v1(risk, w, .9)
            probability = float(np.clip(w @ (net > 0), 0., 1.))
            values = dict(status="ACCEPTABLE" if mean > 0 and downside <= POLICY["risk_bps"] else "AVOID",
                expected_net_bps=mean, downside_q90_bps=downside, profit_probability=probability,
                path_min_ratio_q10=weighted_quantile_v1(path, w, .1), distribution_unique_samples=int((w > 0).sum()),
                distribution_known_trees=int(trees), weight_effective_sample_size=float(1/np.square(w).sum()))
            if not np.isfinite([mean, downside, probability, values["weight_effective_sample_size"]]).all():
                raise ValueError("population price value arithmetic is nonfinite")
            for name, value in values.items():
                result.iloc[index, result.columns.get_loc(name)] = value
    result["model_sha256"], result["schema_sha256"], result["policy_sha256"] = fitted.model_sha256, SCHEMA_SHA256, POLICY_SHA256
    result["holding_sessions"], result["effective_sample_size_is_time_independence"] = 5, False
    result["deployable"] = False
    return result


def population_price_set_5td_v1(*, fitted, d_features, reference_cny, legal_low_cny, legal_high_cny, tick_cny):
    original = (reference_cny, legal_low_cny, legal_high_cny, tick_cny)
    if any(_number(v, positive=True) is None for v in original) or set(d_features) != set(FEATURES):
        raise ValueError("population legal D-anchor price inputs differ")
    reference, low, high, tick = (Decimal(str(v)) for v in original)
    if low > high:
        raise ValueError("population legal price interval is inverted")
    first = int((low/tick).to_integral_value(rounding=ROUND_CEILING))
    last = int((high/tick).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("population complete legal tick grid exceeds 100000 nodes")
    prices = [tick*n for n in range(first, last+1)]
    gaps = [float((p/reference-1)*10000) for p in prices]
    nodes = query_population_price_nodes_v1(fitted=fitted, features=pd.DataFrame([d_features]*len(prices), columns=FEATURES),
                                           scenario_gap_bps=gaps)
    bands, left, right = [], None, None
    for price, state in zip(prices, nodes.status, strict=True):
        if state == "ACCEPTABLE":
            left, right = price if left is None else left, price
        elif left is not None:
            bands.append((str(left), str(right)))
            left = right = None
    if left is not None:
        bands.append((str(left), str(right)))
    unknown = int(nodes.status.str.startswith("UNKNOWN_").sum())
    status = "EMPTY_LEGAL_GRID" if not prices else "ACCEPTABLE_PRICE_SET" if bands else (
        "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" if 0 < unknown < len(prices) else "UNKNOWN_INPUT_OR_SUPPORT" if unknown else "NO_ACCEPTABLE_PRICE")
    return dict(status=status, intervals_cny=tuple(bands), legal_node_count=len(prices), unknown_node_count=unknown,
        price_basis="D_ANCHORED_CNY", price_encoding="DECIMAL_STRING_CNY", holding_sessions=5,
        model_sha256=fitted.model_sha256, schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256,
        evidence_use="NAVIGATION_ONLY", valuation_semantics="OBSERVED_OPEN_SCENARIO_ASSOCIATION_NOT_LIMIT_FILL", deployable=False)
