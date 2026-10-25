"""Independent 20/22D honest paired-price forest and complete D-anchored price sets."""
from copy import deepcopy
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import re
import time

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_models_v1 import feature_values, matrix
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.generic_population_price_5td_model_v1 import weighted_quantile_v1
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import (
    ARMS, MATRIX_ORDERS, PARAMETERS, SCHEMA, SCHEMA_SHA256,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@dataclass(frozen=True)
class ParentScorePriceFitV1:
    recipe: dict
    forest: tuple
    calibration: dict
    diagnostics: dict
    model_sha256: str


def fit_identity(fitted):
    return sha({k: v for k, v in asdict(fitted).items() if k != "model_sha256"})


def _budget(started):
    import psutil
    if time.monotonic()-started > 1800 or psutil.Process().memory_info().rss > 8*1024**3:
        raise ValueError("parent score model exceeds its 30-minute/8GiB budget")


def _encoding(encoding):
    if (encoding.get("matrix_orders") != {a: list(v) for a, v in MATRIX_ORDERS.items()}
            or encoding.get("contrast_status") != "PREPARED_IDENTIFIABLE_NO_FIT"):
        raise ValueError("independent 20/22D identifiable encoding is required")
    medians = np.asarray([_number(v) for v in encoding["medians"]], dtype=float)
    if medians.shape != (9,) or not np.isfinite(medians).all():
        raise ValueError("shared train-only medians differ")
    if len(encoding["adapters"]) != 2 or {a["context_bit"] for a in encoding["adapters"].values()} != {0, 1}:
        raise ValueError("matched package context differs")
    for adapter in encoding["adapters"].values():
        if (_number(adapter.get("center")) is None or _number(adapter.get("scale"), positive=True) is None
                or adapter.get("status") != "AVAILABLE"):
            raise ValueError("parent score scale is not identifiable")
    clocks = [pd.Timestamp(encoding[n]) for n in ("train_start", "train_end", "estimation_first_D", "evaluation_start", "evaluation_end")]
    if any(pd.isna(v) or v.tz is not None or v != v.normalize() for v in clocks) or not clocks[0] <= clocks[2] <= clocks[1] < clocks[3] <= clocks[4]:
        raise ValueError("train-only honest clocks differ")
    return ValueAnchorGapSupportV1(tuple(tuple(v) for v in encoding["intervals_bps"]))


def _matrix(rows, encoding, arm, gaps):
    if arm not in ARMS or not {"package_id", "manifest_sha256", "run_id"}.issubset(rows):
        raise ValueError("parent model requires explicit original package adapter identity")
    contexts, scores, missing, known = [], [], [], []
    for row in rows.to_dict("records"):
        adapter = encoding["adapters"].get(row["package_id"])
        ok = adapter is not None and all(row[n] == adapter[n] for n in ("manifest_sha256", "run_id"))
        known.append(ok)
        contexts.append(adapter["context_bit"] if ok else 0)
        if arm == ARMS[1]:
            value = _number(row.get("parent_score"))
            missing.append(value is None)
            scores.append((value-adapter["center"])/adapter["scale"] if ok and value is not None else 0.)
    out = np.column_stack((matrix(rows, encoding["medians"], gaps=gaps), contexts))
    if arm == ARMS[1]:
        out = np.column_stack((out, scores, missing))
    out = out.astype(np.float32)
    if not np.isfinite(out).all():
        raise ValueError("encoded float32 price coordinates overflow")
    return out, np.asarray(known, dtype=bool), np.asarray(missing, dtype=bool)


def _integers(values):
    if not isinstance(values, (list, tuple)) or any(type(v) is not int for v in values):
        raise ValueError("JSON node and member indices must be integers")
    return np.asarray(values, dtype=np.int64)


def _tree_arrays(tree, dimensions):
    if set(tree) != {"feature", "threshold", "left", "right", "members"}:
        raise ValueError("non-executing tree schema differs")
    feature, left, right = (_integers(tree[n]) for n in ("feature", "left", "right"))
    threshold = np.asarray([_number(v) for v in tree["threshold"]], dtype=float)
    n = len(left)
    if not 0 < n <= 127 or any(len(v) != n for v in (feature, right, threshold)) or not np.isfinite(threshold).all():
        raise ValueError("tree node/shape budget differs")
    leaf, nodes = left == -1, np.arange(n)
    if (not np.array_equal(leaf, right == -1) or (feature[leaf] != -2).any()
            or ((feature[~leaf] < 0) | (feature[~leaf] >= dimensions)).any()
            or any(((v[~leaf] <= nodes[~leaf]) | (v[~leaf] >= n)).any() for v in (left, right))):
        raise ValueError("tree feature, child or cycle differs")
    children = np.concatenate((left[~leaf], right[~leaf]))
    if not np.array_equal(np.bincount(children, minlength=n), np.r_[0, np.ones(n-1, dtype=int)]):
        raise ValueError("tree has unreachable/shared nodes")
    depth = np.zeros(n, dtype=int)
    for i in nodes[~leaf]:
        depth[left[i]] = depth[right[i]] = depth[i]+1
    if depth.max() > 6 or not isinstance(tree["members"], dict):
        raise ValueError("tree depth/membership differs")
    return feature, threshold, left, right


def forest_leaf_ids(*, forest, x, dimensions):
    values = np.asarray(x, dtype=np.float32)
    if (dimensions not in (20, 22) or values.ndim != 2 or values.shape[1] != dimensions
            or len(values) > 100000 or not np.isfinite(values).all() or not 0 < len(forest) <= 128):
        raise ValueError("independent forest needs bounded finite 20/22D float32 input")
    output = np.zeros((len(values), len(forest)), dtype=np.int64)
    for ordinal, tree in enumerate(forest):
        fields, thresholds, left, right = _tree_arrays(tree, dimensions)
        nodes = np.zeros(len(values), dtype=np.int64)
        for _ in range(7):
            active = np.flatnonzero(left[nodes] != -1)
            if not len(active):
                break
            current = nodes[active]
            nodes[active] = np.where(values[active, fields[current]] <= thresholds[current], left[current], right[current])
        if (left[nodes] != -1).any():
            raise ValueError("JSON walk failed to reach a leaf")
        output[:, ordinal] = nodes
    return output


def train_parent_score_model(*, rows, encoding, arm, plan_sha256, before_fit, after_fit):
    """One physical fit; only STRUCTURE labels split trees, ESTIMATION labels give mass."""
    from sklearn.ensemble import RandomForestRegressor
    import sklearn
    from threadpoolctl import threadpool_limits
    started = time.monotonic()
    _budget(started)
    support = _encoding(encoding)
    if arm not in ARMS or not re.fullmatch(r"[a-f0-9]{64}", plan_sha256):
        raise ValueError("study arm/identity differs")
    # Select before decoding labels/features; held rows can contain opaque poisoned values.
    pool = rows.loc[rows.pool.isin(["STRUCTURE", "ESTIMATION"])].copy()
    if not 0 < len(pool) <= 100000 or pool.duplicated(["package_id", *KEY]).any():
        raise ValueError("original supervision rows are empty/duplicate/over budget")
    structure, estimation = pool.pool.eq("STRUCTURE"), pool.pool.eq("ESTIMATION")
    d, t, h = (pd.to_datetime(pool[n]) for n in (*KEY[:2], "label_information_end"))
    first = pd.Timestamp(encoding["estimation_first_D"])
    if (not structure.any() or not estimation.any()
            or any(v.isna().any() or v.dt.tz is not None or not v.eq(v.dt.normalize()).all() for v in (d, t, h))
            or not d.between(encoding["train_start"], encoding["train_end"]).all()
            or not (d.lt(t) & t.le(h) & h.le(pd.Timestamp(encoding["train_end"])) & h.lt(pd.Timestamp(encoding["evaluation_start"]))).all()
            or not (d[structure].lt(first) & h[structure].lt(first)).all() or not d[estimation].ge(first).all()
            or not pool.valuation_status.eq("AVAILABLE").all()
            or not pool.valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all()
            or any(int(mask.sum()) != encoding["pools"][role] for role, mask in (("STRUCTURE", structure), ("ESTIMATION", estimation)))):
        raise ValueError("supervision policy/honest purge/role identity differs; no fit")
    targets = pool.loc[:, ["valuation_gross_terminal_ratio", "valuation_path_min_ratio"]].map(lambda v: _number(v, positive=True)).astype(float)
    masses = pool.cluster_mass.map(lambda v: _number(v, positive=True)).to_numpy(float)
    if (targets.isna().any().any() or targets.iloc[:, 1].gt(targets.iloc[:, 0]).any() or not np.isfinite(masses).all()
            or not np.allclose(pool.assign(m=masses).groupby("label_cluster").m.sum(), 1.)
            or pool.groupby("label_cluster")[list(targets.columns)+["pool", "label_information_end"]].nunique().gt(1).any().any()):
        raise ValueError("paired stock/D cluster targets or total mass differ")
    gaps = pool.observed_gap_bps.map(_number).to_numpy(float)
    _, stock = feature_values(pool)
    if not stock.all() or not np.isfinite(gaps).all() or not all(support.contains(float(v)) for v in gaps):
        raise ValueError("supervision price support/basic input differs")
    xs, ks, _ = _matrix(pool.loc[structure], encoding, arm, gaps[structure])
    xe, ke, _ = _matrix(pool.loc[estimation], encoding, arm, gaps[estimation])
    if not ks.all() or not ke.all():
        raise ValueError("training package adapter identity differs")
    estimator = RandomForestRegressor(**PARAMETERS)
    before_fit(arm)
    try:
        with threadpool_limits(limits=2):
            estimator.fit(xs, targets.loc[structure, "valuation_gross_terminal_ratio"], sample_weight=masses[structure])
    finally:
        after_fit(arm)
    _budget(started)
    leaves = estimator.apply(xe)
    forest = []
    for i, tree in enumerate(estimator.estimators_):
        body = {name: getattr(tree.tree_, old).tolist() for name, old in
            (("feature", "feature"), ("threshold", "threshold"), ("left", "children_left"), ("right", "children_right"))}
        body["members"] = {str(int(leaf)): np.flatnonzero(leaves[:, i] == leaf).tolist() for leaf in np.unique(leaves[:, i])}
        forest.append(body)
    dimensions = len(MATRIX_ORDERS[arm])
    if (not np.array_equal(forest_leaf_ids(forest=forest, x=xs, dimensions=dimensions), estimator.apply(xs))
            or not np.array_equal(forest_leaf_ids(forest=forest, x=xe, dimensions=dimensions), leaves)):
        raise ValueError("independent JSON/float32 leaf parity differs")
    calibration = dict(terminal=targets.loc[estimation].iloc[:, 0].tolist(), path=targets.loc[estimation].iloc[:, 1].tolist(),
        masses=masses[estimation].tolist(), clusters=pool.loc[estimation, "label_cluster"].tolist(),
        keys=[[str(v) for v in key] for key in pool.loc[estimation, ["package_id", *KEY]].itertuples(index=False, name=None)],
        label_ends=[v.date().isoformat() for v in h[estimation]])
    recipe = dict(schema=SCHEMA, schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256,
        valuation_policy_sha256=VALUATION_POLICY_SHA256, arm=arm, dimensions=dimensions,
        matrix_order=list(MATRIX_ORDERS[arm]), parameters=dict(PARAMETERS), plan_sha256=plan_sha256,
        encoding=deepcopy(encoding), encoding_sha256=sha(encoding), quantile="DIRECT_RISK_INVERTED_CDF")
    diagnostics = dict(physical_fit_count=1, internal_tree_count=128, structure_rows=int(structure.sum()),
        estimation_rows=int(estimation.sum()), structure_latest_label_end=h[structure].max().date().isoformat(),
        estimation_keys_sha256=sha(calibration["keys"]), sklearn_version=sklearn.__version__,
        evaluation_used_for_training=False, deployable=False)
    fitted = ParentScorePriceFitV1(recipe, tuple(forest), calibration, diagnostics, "")
    fitted = ParentScorePriceFitV1(recipe, fitted.forest, calibration, diagnostics, fit_identity(fitted))
    validate_fit(fitted)
    return fitted


def validate_fit(fitted):
    if not isinstance(fitted, ParentScorePriceFitV1):
        raise ValueError("parent model needs its own non-executing payload")
    r, c = fitted.recipe, fitted.calibration
    arm = r.get("arm")
    if (not isinstance(fitted, ParentScorePriceFitV1) or fit_identity(fitted) != fitted.model_sha256
            or arm not in ARMS or r.get("schema") != SCHEMA or r.get("schema_sha256") != SCHEMA_SHA256
            or r.get("policy_sha256") != POLICY_SHA256 or r.get("valuation_policy_sha256") != VALUATION_POLICY_SHA256
            or r.get("parameters") != dict(PARAMETERS) or r.get("dimensions") != len(MATRIX_ORDERS[arm])
            or r.get("matrix_order") != list(MATRIX_ORDERS[arm]) or r.get("encoding_sha256") != sha(r["encoding"])
            or r.get("quantile") != "DIRECT_RISK_INVERTED_CDF" or len(fitted.forest) != 128):
        raise ValueError("parent model non-executing payload/identity differs")
    if not isinstance(r.get("plan_sha256"), str) or not re.fullmatch(r"[a-f0-9]{64}", r["plan_sha256"]):
        raise ValueError("serialized study identity differs")
    support = _encoding(r["encoding"])
    n = len(c["terminal"])
    values = np.asarray([[_number(v, positive=True) for v in c[name]] for name in ("terminal", "path", "masses")], dtype=float)
    if (not 0 < n <= 100000 or values.shape != (3, n) or not np.isfinite(values).all()
            or (values <= 0).any() or (values[1] > values[0]).any()
            or any(len(c[name]) != n for name in ("clusters", "keys", "label_ends"))
            or len({tuple(k) for k in c["keys"]}) != n or any(len(k) != 4 for k in c["keys"])):
        raise ValueError("serialized paired estimation mass differs")
    pairs = pd.DataFrame(dict(cluster=c["clusters"], terminal=c["terminal"], path=c["path"], mass=c["masses"]))
    if pairs.groupby("cluster")[["terminal", "path"]].nunique().gt(1).any().any() or not np.allclose(pairs.groupby("cluster").mass.sum(), 1.):
        raise ValueError("serialized duplicate-label cluster mass differs")
    clocks = pd.DataFrame(c["keys"], columns=["package_id", *KEY])
    d, t, h = (pd.to_datetime(v) for v in (clocks[KEY[0]], clocks[KEY[1]], pd.Series(c["label_ends"])))
    first = pd.Timestamp(r["encoding"]["estimation_first_D"])
    if (any(v.isna().any() or v.dt.tz is not None or not v.eq(v.dt.normalize()).all() for v in (d, t, h))
            or not (d.ge(first) & d.lt(t) & t.le(h) & h.le(pd.Timestamp(r["encoding"]["train_end"]))).all()
            or fitted.diagnostics.get("estimation_keys_sha256") != sha(c["keys"])
            or fitted.diagnostics.get("physical_fit_count") != 1 or fitted.diagnostics.get("estimation_rows") != n
            or fitted.diagnostics.get("internal_tree_count") != 128
            or fitted.diagnostics.get("structure_rows") != r["encoding"]["pools"]["STRUCTURE"]
            or n != r["encoding"]["pools"]["ESTIMATION"]
            or fitted.diagnostics.get("evaluation_used_for_training") is not False or fitted.diagnostics.get("deployable") is not False
            or not pd.Timestamp(fitted.diagnostics["structure_latest_label_end"]) < first):
        raise ValueError("serialized training clock/diagnostics differ")
    for tree in fitted.forest:
        _, _, left, _ = _tree_arrays(tree, r["dimensions"])
        all_members = []
        for key, ids in tree["members"].items():
            if not isinstance(key, str) or not re.fullmatch(r"0|[1-9][0-9]*", key) or int(key) >= len(left) or left[int(key)] != -1:
                raise ValueError("serialized member leaf differs")
            all_members.extend(_integers(ids).tolist())
        if sorted(all_members) != list(range(n)):
            raise ValueError("tree drops/duplicates original estimation members")
    return support


def model_payload(fitted):
    validate_fit(fitted)
    return asdict(fitted)


def model_from_payload(payload):
    if not isinstance(payload, dict) or set(payload) != {"recipe", "forest", "calibration", "diagnostics", "model_sha256"}:
        raise ValueError("parent model JSON fields differ")
    values = deepcopy(payload)
    values["forest"] = tuple(values["forest"])
    fitted = ParentScorePriceFitV1(**values)
    validate_fit(fitted)
    return fitted


def joint_cluster_weights(*, fitted, x):
    if len(x) > 128:
        raise ValueError("paired distribution batch exceeds 128")
    c = fitted.calibration
    leaves = forest_leaf_ids(forest=fitted.forest, x=x, dimensions=fitted.recipe["dimensions"])
    mass = np.asarray(c["masses"], dtype=float)
    row_weights, known = np.zeros((len(x), len(mass))), np.zeros(len(x), dtype=int)
    for i, tree in enumerate(fitted.forest):
        for leaf in np.unique(leaves[:, i]):
            ids = _integers(tree["members"].get(str(int(leaf)), []))
            if not len(ids):
                continue
            selected = np.flatnonzero(leaves[:, i] == leaf)
            row_weights[np.ix_(selected, ids)] += mass[ids]/mass[ids].sum()
            known[selected] += 1
    good = known > 0
    row_weights[good] /= known[good, None]
    _, first, inverse = np.unique(c["clusters"], return_index=True, return_inverse=True)
    weights = np.zeros((len(x), len(first)))
    for i in range(len(x)):
        np.add.at(weights[i], inverse, row_weights[i])
    return weights, known, np.asarray(c["terminal"])[first], np.asarray(c["path"])[first]


def query_parent_score_nodes(*, fitted, features, scenario_gap_bps):
    started = time.monotonic()
    support = validate_fit(fitted)
    if not features.index.is_unique or len(features) > 100000:
        raise ValueError("original query index/row budget differs")
    _, stock = feature_values(features)
    gaps = np.asarray([_number(v) for v in scenario_gap_bps], dtype=float)
    if gaps.shape != (len(features),) or (gaps[np.isfinite(gaps)] <= -10000).any():
        raise ValueError("hypothetical entry coordinates differ")
    supported = np.array([np.isfinite(v) and support.contains(float(v)) for v in gaps], dtype=bool)
    # Invalid g never reaches the encoder; it remains a typed UNKNOWN node.
    encoded, package, missing = _matrix(features, fitted.recipe["encoding"], fitted.recipe["arm"], np.where(np.isfinite(gaps), gaps, 0.))
    score = ~missing if fitted.recipe["arm"] == ARMS[1] else np.ones(len(features), dtype=bool)
    status = np.where(~stock, "UNKNOWN_STOCK_INPUT", np.where(~package, "UNKNOWN_PACKAGE_ADAPTER",
        np.where(~score, "UNKNOWN_PARENT_SCORE", np.where(~np.isfinite(gaps), "UNKNOWN_PRICE_SCENARIO",
        np.where(~supported, "UNKNOWN_GAP_SUPPORT", "UNKNOWN_MODEL_DISTRIBUTION")))))
    result = features.loc[:, [n for n in ("package_id", "manifest_sha256", "run_id", *KEY) if n in features]].copy()
    result["status"] = status
    for name in ("expected_net_bps", "downside_q90_bps", "profit_probability", "path_min_ratio_q10", "weight_effective_sample_size"):
        result[name] = np.nan
    result["distribution_unique_samples"], result["distribution_known_trees"] = 0, 0
    selected = np.flatnonzero(stock & package & score & supported)
    for first in range(0, len(selected), 128):
        _budget(started)
        ids = selected[first:first+128]
        weights, trees, terminal, path = joint_cluster_weights(fitted=fitted, x=encoded[ids])
        for i, w, known in zip(ids, weights, trees, strict=True):
            if not known:
                continue
            a = 1+gaps[i]/10000
            net = 10000*(terminal*(1-POLICY["sell_bps"]/10000)/(a*(1+POLICY["buy_bps"]/10000))-1)
            risk = 10000*np.maximum(0., 1-path/a)
            mean, downside = float(w @ net), weighted_quantile_v1(risk, w, .9)
            values = dict(status="ACCEPTABLE" if mean > 0 and downside <= POLICY["risk_bps"] else "AVOID",
                expected_net_bps=mean, downside_q90_bps=downside, profit_probability=float(np.clip(w @ (net > 0), 0., 1.)),
                path_min_ratio_q10=weighted_quantile_v1(path, w, .1), distribution_unique_samples=int((w > 0).sum()),
                distribution_known_trees=int(known), weight_effective_sample_size=float(1/np.square(w).sum()))
            if not np.isfinite([mean, downside, values["profit_probability"], values["weight_effective_sample_size"]]).all():
                raise ValueError("price value/risk arithmetic is nonfinite")
            for name, value in values.items():
                result.iloc[i, result.columns.get_loc(name)] = value
    for name, value in dict(model_sha256=fitted.model_sha256, schema_sha256=SCHEMA_SHA256,
        policy_sha256=POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256, holding_sessions=5,
        effective_sample_size_is_time_independence=False, deployable=False).items():
        result[name] = value
    return result


def parent_score_price_set(*, fitted, d_features, source_context, reference_cny, legal_low_cny, legal_high_cny, tick_cny):
    coordinates = (reference_cny, legal_low_cny, legal_high_cny, tick_cny)
    try:
        decimals = tuple(Decimal(str(v)) for v in coordinates)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("legal price coordinates must be exact decimal numbers") from exc
    if (any(isinstance(v, (bool, np.bool_)) for v in coordinates)
            or any(not v.is_finite() or v <= 0 or not np.isfinite(float(v)) for v in decimals)
            or set(d_features) != set(FEATURES)):
        raise ValueError("explicit legal D-anchor price coordinates differ")
    reference, low, high, tick = decimals
    if low > high or set(source_context) != {"package_id", "manifest_sha256", "run_id", "parent_score"}:
        raise ValueError("legal grid or original parent adapter context differs")
    first, last = int((low/tick).to_integral_value(rounding=ROUND_CEILING)), int((high/tick).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("complete legal grid exceeds 100000 nodes")
    prices = [tick*n for n in range(first, last+1)]
    gaps = [float((p/reference-1)*10000) for p in prices]
    features = pd.DataFrame([{**d_features, **source_context} for _ in prices], columns=[*FEATURES, *source_context])
    nodes = query_parent_score_nodes(fitted=fitted, features=features, scenario_gap_bps=gaps)
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
    unknown_bands, reason, start, stop = [], None, None, None
    for price, state in zip(prices, nodes.status, strict=True):
        current = state if state.startswith("UNKNOWN_") else None
        if current != reason:
            if reason is not None:
                unknown_bands.append(dict(reason=reason, low_cny=str(start), high_cny=str(stop)))
            reason, start = current, price if current is not None else None
        stop = price
    if reason is not None:
        unknown_bands.append(dict(reason=reason, low_cny=str(start), high_cny=str(stop)))
    return dict(status="EMPTY_LEGAL_GRID" if not prices else "ACCEPTABLE_PRICE_SET" if bands else
        "UNKNOWN_INPUT_OR_SUPPORT" if unknown else "NO_ACCEPTABLE_PRICE", intervals_cny=tuple(bands),
        legal_node_count=len(prices), unknown_node_count=unknown, unknown_intervals_cny=unknown_bands, price_basis="D_ANCHORED_CNY",
        price_encoding="DECIMAL_STRING_CNY", holding_sessions=5, model_sha256=fitted.model_sha256,
        evidence_use="NAVIGATION_ONLY", actual_fill_proven=False, raw_price_bridge_proven=False, deployable=False)
