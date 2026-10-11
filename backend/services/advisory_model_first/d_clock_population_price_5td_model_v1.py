"""Honest 18D mark distribution; price is a payment parameter, never future information."""
from copy import deepcopy
from collections import OrderedDict
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import re
import time

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.d_clock_population_price_5td_contracts_v1 import (
    ARMS, FEATURES, KEY, MARK_POLICY, MARK_POLICY_SHA256, MATRIX_ORDER, PARAMETERS, SCHEMA, SCHEMA_SHA256,
)
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_models_v1 import feature_values, matrix
from backend.services.advisory_model_first.generic_population_price_5td_model_v1 import weighted_quantile_v1
from backend.services.advisory_model_first.parent_score_price_5td_model_v1 import _integers, _tree_arrays
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@dataclass(frozen=True)
class DClockPopulationFitV1:
    recipe: dict
    forest: tuple
    calibration: dict
    diagnostics: dict
    model_sha256: str


def _identity(fitted):
    return sha({k: v for k, v in asdict(fitted).items() if k != "model_sha256"})


def _budget(started):
    import psutil
    if time.monotonic()-started > 1800 or psutil.Process().memory_info().rss > 8*1024**3:
        raise ValueError("D-clock model exceeds its 30-minute/8GiB process budget")


def _encoding(encoding):
    if (not isinstance(encoding, dict) or encoding.get("matrix_order") != list(MATRIX_ORDER)
            or encoding.get("contrast_status") != "PREPARED_IDENTIFIABLE_NO_FIT"):
        raise ValueError("identifiable 18D shared encoding is required")
    medians = np.asarray([_number(v) for v in encoding.get("medians", ())], dtype=float)
    dates = [pd.Timestamp(encoding[n]) for n in ("train_start", "estimation_first_D", "train_end", "evaluation_start", "evaluation_end")]
    if (medians.shape != (9,) or not np.isfinite(medians).all()
            or any(pd.isna(v) or v.tz is not None or v != v.normalize() for v in dates)
            or not dates[0] < dates[1] <= dates[2] < dates[3] <= dates[4]):
        raise ValueError("shared train medians or honest clocks differ")
    if set(encoding["role_clusters"]) != set(ARMS) or set(encoding["pools"]) != set(ARMS):
        raise ValueError("both sampling populations must be explicit")
    for arm in ARMS:
        roles = encoding["role_clusters"][arm]
        if (set(roles) != {"STRUCTURE", "ESTIMATION"} or encoding["role_sha256"][arm] != sha(roles)
                or any(not isinstance(ids, list) or len(ids) != len(set(ids)) or not ids
                    or len(ids) != encoding["pools"][arm][role] for role, ids in roles.items())
                or set(roles["STRUCTURE"]) & set(roles["ESTIMATION"])):
            raise ValueError("shared honest role membership/hash differs")
    if encoding["role_clusters"][ARMS[0]] == encoding["role_clusters"][ARMS[1]]:
        raise ValueError("identical post-purge populations cannot produce a sampling experiment")


def d_clock_forest_leaf_ids_v1(*, forest, x):
    """sklearn apply uses float32. The existing parent walker only accepts 20/22D."""
    x = np.asarray(x, dtype=np.float32)
    if x.ndim != 2 or x.shape[1] != 18 or len(x) > 100000 or not np.isfinite(x).all() or len(forest) != 128:
        raise ValueError("D-clock JSON forest requires finite bounded 18D inputs and 128 trees")
    output = np.zeros((len(x), len(forest)), dtype=np.int64)
    for i, tree in enumerate(forest):
        feature, threshold, left, right = _tree_arrays(tree, 18)
        nodes = np.zeros(len(x), dtype=np.int64)
        for _ in range(7):
            active = np.flatnonzero(left[nodes] != -1)
            if not len(active):
                break
            current = nodes[active]
            nodes[active] = np.where(x[active, feature[current]] <= threshold[current], left[current], right[current])
        if (left[nodes] != -1).any():
            raise ValueError("D-clock JSON forest did not reach a leaf")
        output[:, i] = nodes
    return output


def train_d_clock_population_model_v1(*, rows, encoding, arm, plan_sha256, before_fit, after_fit):
    """Exactly one structure fit per arm; estimation labels never split a tree."""
    from sklearn.ensemble import RandomForestRegressor
    import sklearn
    from threadpoolctl import threadpool_limits
    started = time.monotonic()
    _budget(started)
    _encoding(encoding)
    if arm not in ARMS or re.fullmatch(r"[a-f0-9]{64}", plan_sha256) is None:
        raise ValueError("D-clock arm/plan identity differs")
    column = "pool_"+arm
    # Role and date projection precede every feature or target decoder.
    pool = rows.loc[rows[column].isin(["STRUCTURE", "ESTIMATION"])].copy()
    d, t, h = (pd.to_datetime(pool[n]) for n in (*KEY[:2], "label_information_end"))
    if (not 0 < len(pool) <= 100000 or pool.duplicated(["package_id", *KEY]).any()
            or any(v.isna().any() or v.dt.tz is not None or not v.eq(v.dt.normalize()).all() for v in (d, t, h))
            or not (d.ge(encoding["train_start"]) & d.lt(t) & t.le(h) & h.le(encoding["train_end"])
                    & h.lt(encoding["evaluation_start"])).all()):
        raise ValueError("training source keys or mature train-only clocks differ")
    for role in ("STRUCTURE", "ESTIMATION"):
        block = pool.loc[pool[column].eq(role)]
        if sorted(block.label_cluster.unique()) != sorted(encoding["role_clusters"][arm][role]):
            raise ValueError("role membership changed after input freeze")
        first = pd.Timestamp(encoding["estimation_first_D"])
        if (role == "STRUCTURE" and not (block[KEY[0]].lt(first) & pd.to_datetime(block.label_information_end).lt(first)).all()
                or role == "ESTIMATION" and not block[KEY[0]].ge(first).all()):
            raise ValueError("honest cut or strict horizon purge differs")
    fields = [*FEATURES, "reference_cny", "terminal_mark_ratio", "path_mark_min_ratio", "mark_label_status",
              "mark_policy_sha256", column, "label_information_end"]
    if pool.groupby("label_cluster")[fields].nunique(dropna=False).gt(1).any().any():
        raise ValueError("economic cluster D input or paired label conflicts")
    pool = pool.drop_duplicates("label_cluster").sort_values(list(KEY)).reset_index(drop=True)
    expected_clusters = [sha([pd.Timestamp(d).date().isoformat(), pd.Timestamp(t).date().isoformat(), s, MARK_POLICY_SHA256])
        for d, t, s in pool.loc[:, KEY].itertuples(index=False, name=None)]
    if pool.label_cluster.tolist() != expected_clusters or not pool.mark_policy_sha256.eq(MARK_POLICY_SHA256).all() or not pool.mark_label_status.eq("AVAILABLE").all():
        raise ValueError("independent mark target/cluster identity differs")
    targets = pool.loc[:, ["terminal_mark_ratio", "path_mark_min_ratio"]].map(lambda v: _number(v, positive=True)).astype(float)
    _, basic = feature_values(pool)
    if targets.isna().any().any() or targets.iloc[:, 1].gt(targets.iloc[:, 0]).any() or not basic.all():
        raise ValueError("known D supervision or paired marks are incomplete")
    if arm == ARMS[1]:
        from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
        from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
        support = ValueAnchorGapSupportV1(tuple(tuple(v) for v in encoding["intervals_bps"]))
        if (not pool.legacy_valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all()
                or not pool.legacy_valuation_status.eq("AVAILABLE").all() or not pool.observed_gap_bps.map(
                lambda v: _number(v) is not None and support.contains(float(v))).all()):
            raise ValueError("matched future-episode sampling mask differs")
    structure, estimation = pool[column].eq("STRUCTURE"), pool[column].eq("ESTIMATION")
    x = matrix(pool, encoding["medians"]).astype(np.float32)
    if not np.isfinite(x).all():
        raise ValueError("D float32 encoding overflow")
    estimator = RandomForestRegressor(**PARAMETERS)
    before_fit(arm)
    try:
        with threadpool_limits(limits=2):
            estimator.fit(x[structure], targets.loc[structure, "terminal_mark_ratio"], sample_weight=np.ones(int(structure.sum())))
    finally:
        after_fit(arm)
    _budget(started)
    leaves = estimator.apply(x[estimation])
    forest = []
    for i, tree in enumerate(estimator.estimators_):
        body = {name: getattr(tree.tree_, old).tolist() for name, old in
            (("feature", "feature"), ("threshold", "threshold"), ("left", "children_left"), ("right", "children_right"))}
        body["members"] = {str(int(leaf)): np.flatnonzero(leaves[:, i] == leaf).tolist() for leaf in np.unique(leaves[:, i])}
        forest.append(body)
    if not np.array_equal(d_clock_forest_leaf_ids_v1(forest=forest, x=x), estimator.apply(x)):
        raise ValueError("D-clock JSON float32/apply parity failed")
    cal = dict(terminal=targets.loc[estimation].iloc[:, 0].tolist(), path=targets.loc[estimation].iloc[:, 1].tolist(),
        masses=[1.]*int(estimation.sum()), clusters=pool.loc[estimation, "label_cluster"].tolist(),
        keys=[[str(v) for v in key] for key in pool.loc[estimation, KEY].itertuples(index=False, name=None)],
        label_ends=[pd.Timestamp(v).date().isoformat() for v in pool.loc[estimation, "label_information_end"]])
    recipe = dict(schema=SCHEMA, schema_sha256=SCHEMA_SHA256, arm=arm, mark_policy_sha256=MARK_POLICY_SHA256,
        dimensions=18, matrix_order=list(MATRIX_ORDER), parameters=dict(PARAMETERS), encoding=deepcopy(encoding),
        encoding_sha256=sha(encoding), plan_sha256=plan_sha256, prediction_clock="D_CLOSE_UNCONDITIONAL_MARK_FUNCTION")
    diagnostics = dict(physical_fit_count=1, internal_tree_count=128, structure_unique_clusters=int(structure.sum()),
        estimation_unique_clusters=int(estimation.sum()), structure_latest_label_end=pd.to_datetime(pool.loc[structure, "label_information_end"]).max().date().isoformat(),
        estimation_keys_sha256=sha(cal["keys"]), sklearn_version=sklearn.__version__, evaluation_used_for_training=False, deployable=False)
    fitted = DClockPopulationFitV1(recipe, tuple(forest), cal, diagnostics, "")
    fitted = DClockPopulationFitV1(recipe, fitted.forest, cal, diagnostics, _identity(fitted))
    validate_d_clock_fit_v1(fitted)
    return fitted


def validate_d_clock_fit_v1(fitted):
    if not isinstance(fitted, DClockPopulationFitV1) or _identity(fitted) != fitted.model_sha256:
        raise ValueError("independent D-clock model identity differs")
    r, c = fitted.recipe, fitted.calibration
    arm = r.get("arm")
    if (arm not in ARMS or r.get("schema") != SCHEMA or r.get("schema_sha256") != SCHEMA_SHA256
            or r.get("mark_policy_sha256") != MARK_POLICY_SHA256 or r.get("dimensions") != 18
            or r.get("matrix_order") != list(MATRIX_ORDER) or r.get("parameters") != dict(PARAMETERS)
            or r.get("prediction_clock") != "D_CLOSE_UNCONDITIONAL_MARK_FUNCTION"
            or r.get("encoding_sha256") != sha(r["encoding"]) or len(fitted.forest) != 128
            or re.fullmatch(r"[a-f0-9]{64}", r.get("plan_sha256", "")) is None):
        raise ValueError("D-only schema, recipe or parameters differ")
    _encoding(r["encoding"])
    n = len(c["terminal"])
    values = np.asarray([[_number(v, positive=True) for v in c[name]] for name in ("terminal", "path", "masses")], dtype=float)
    if (not 0 < n <= 100000 or values.shape != (3, n) or not np.isfinite(values).all()
            or (values <= 0).any() or (values[1] > values[0]).any() or not np.equal(values[2], 1.).all()
            or any(len(c[name]) != n for name in ("keys", "clusters", "label_ends"))
            or len(set(c["clusters"])) != n or len({tuple(k) for k in c["keys"]}) != n or any(len(k) != 3 for k in c["keys"])):
        raise ValueError("serialized unique paired mass differs")
    keys = pd.DataFrame(c["keys"], columns=KEY)
    d, t, h = pd.to_datetime(keys[KEY[0]]), pd.to_datetime(keys[KEY[1]]), pd.to_datetime(pd.Series(c["label_ends"]))
    encoding, diagnostic = r["encoding"], fitted.diagnostics
    expected_clusters = [sha([day.date().isoformat(), target.date().isoformat(), symbol, MARK_POLICY_SHA256])
        for day, target, symbol in zip(d, t, keys.instrument, strict=True)]
    if (any(v.isna().any() or v.dt.tz is not None or not v.eq(v.dt.normalize()).all() for v in (d, t, h))
            or c["clusters"] != expected_clusters
            or not (d.ge(encoding["estimation_first_D"]) & d.lt(t) & t.le(h) & h.le(encoding["train_end"]) & h.lt(encoding["evaluation_start"])).all()
            or sorted(c["clusters"]) != sorted(encoding["role_clusters"][arm]["ESTIMATION"])
            or diagnostic.get("physical_fit_count") != 1 or diagnostic.get("internal_tree_count") != 128
            or diagnostic.get("estimation_keys_sha256") != sha(c["keys"])
            or diagnostic.get("estimation_unique_clusters") != n
            or diagnostic.get("structure_unique_clusters") != encoding["pools"][arm]["STRUCTURE"]
            or diagnostic.get("evaluation_used_for_training") is not False or diagnostic.get("deployable") is not False
            or not pd.Timestamp(diagnostic["structure_latest_label_end"]) < pd.Timestamp(encoding["estimation_first_D"])):
        raise ValueError("serialized honest training clocks/counts differ")
    for tree in fitted.forest:
        _, _, left, _ = _tree_arrays(tree, 18)
        members = []
        for leaf, ids in tree["members"].items():
            if (not isinstance(leaf, str) or re.fullmatch(r"0|[1-9][0-9]*", leaf) is None
                    or int(leaf) >= len(left) or left[int(leaf)] != -1):
                raise ValueError("serialized estimation member is not in a valid leaf")
            members.extend(_integers(ids).tolist())
        if sorted(members) != list(range(n)):
            raise ValueError("serialized tree drops/duplicates estimation members")


def d_clock_model_payload_v1(fitted):
    validate_d_clock_fit_v1(fitted)
    return asdict(fitted)


def d_clock_model_from_payload_v1(payload):
    if not isinstance(payload, dict) or set(payload) != {"recipe", "forest", "calibration", "diagnostics", "model_sha256"}:
        raise ValueError("D-clock non-executing payload fields differ")
    body = deepcopy(payload)
    body["forest"] = tuple(body["forest"])
    fitted = DClockPopulationFitV1(**body)
    validate_d_clock_fit_v1(fitted)
    return fitted


def freeze_d_clock_distribution_v1(*, fitted, d_features):
    """Leaf IDs + immutable model/label identity specify the whole D distribution before T."""
    validate_d_clock_fit_v1(fitted)
    if not d_features.index.is_unique or len(d_features) > 100000:
        raise ValueError("D distribution keys or row budget differ")
    _, known = feature_values(d_features)
    x = matrix(d_features, fitted.recipe["encoding"]["medians"]).astype(np.float32)
    if not np.isfinite(x).all():
        raise ValueError("D-only float32 coordinates overflow")
    leaves = d_clock_forest_leaf_ids_v1(forest=fitted.forest, x=x)
    metadata = [n for n in ("package_id", "manifest_sha256", "run_id", "source_id", *KEY) if n in d_features]
    out = d_features.loc[:, metadata].copy()
    out["leaf_ids"] = [row.tolist() for row in leaves]
    out["distribution_status"] = np.where(known, "DISTRIBUTION_FROZEN", "UNKNOWN_STOCK_INPUT")
    out["model_sha256"], out["mark_policy_sha256"], out["schema_sha256"] = fitted.model_sha256, MARK_POLICY_SHA256, SCHEMA_SHA256
    return out


def _weights_for_leaves(fitted, leaves):
    mass = np.asarray(fitted.calibration["masses"], dtype=float)
    w, trees = np.zeros(len(mass)), 0
    for i, leaf in enumerate(leaves):
        tree = fitted.forest[i]
        ids = np.asarray(tree["members"].get(str(int(leaf)), []), dtype=int)
        if len(ids):
            w[ids] += mass[ids]/mass[ids].sum()
            trees += 1
    if trees:
        w /= trees
    return w, trees


def query_frozen_d_clock_nodes_v1(*, fitted, frozen, price_cny, reference_cny):
    validate_d_clock_fit_v1(fitted)
    if (not frozen.index.is_unique or len(frozen) > 100000 or not frozen.model_sha256.eq(fitted.model_sha256).all()
            or not frozen.schema_sha256.eq(SCHEMA_SHA256).all() or not frozen.mark_policy_sha256.eq(MARK_POLICY_SHA256).all()
            or not frozen.distribution_status.isin(["DISTRIBUTION_FROZEN", "UNKNOWN_STOCK_INPUT"]).all()):
        raise ValueError("frozen D distribution/model identity differs")
    prices, refs = (np.asarray([_number(v, positive=True) for v in values], dtype=float) for values in (price_cny, reference_cny))
    if prices.shape != (len(frozen),) or refs.shape != prices.shape:
        raise ValueError("one explicit price and D reference required per query")
    metadata = [n for n in ("package_id", "manifest_sha256", "run_id", "source_id", *KEY) if n in frozen]
    out = frozen.loc[:, metadata].copy()
    out["status"] = np.where(frozen.distribution_status.eq("UNKNOWN_STOCK_INPUT"), "UNKNOWN_STOCK_INPUT",
                             np.where(~np.isfinite(refs), "UNKNOWN_REFERENCE", "UNKNOWN_PRICE"))
    for name in ("expected_net_bps", "profit_probability", "downside_q90_bps", "weight_effective_sample_size"):
        out[name] = np.nan
    out["distribution_known_trees"] = 0
    y, lower = np.asarray(fitted.calibration["terminal"]), np.asarray(fitted.calibration["path"])
    cache = OrderedDict()
    started = time.monotonic()
    for i, row in enumerate(frozen.itertuples(index=False)):
        if i % 128 == 0:
            _budget(started)
        if row.distribution_status != "DISTRIBUTION_FROZEN" or not np.isfinite([prices[i], refs[i]]).all():
            continue
        if any(not isinstance(v, (int, np.integer)) or isinstance(v, (bool, np.bool_)) for v in row.leaf_ids):
            raise ValueError("frozen D leaf indices must be integers")
        leaves = tuple(int(v) for v in row.leaf_ids)
        if len(leaves) != 128:
            raise ValueError("frozen distribution requires every tree's D leaf")
        for tree, leaf in zip(fitted.forest, leaves, strict=True):
            if not 0 <= leaf < len(tree["left"]) or tree["left"][leaf] != -1:
                raise ValueError("frozen distribution points to a non-leaf")
        if leaves not in cache:
            cache[leaves] = _weights_for_leaves(fitted, leaves)
            if len(cache) > 128:
                cache.popitem(last=False)
        cache.move_to_end(leaves)
        w, trees = cache[leaves]
        if not trees:
            out.iloc[i, out.columns.get_loc("status")] = "UNKNOWN_MODEL_DISTRIBUTION"
            continue
        a = prices[i]/refs[i]
        net = 10000*(y*(1-MARK_POLICY["hypothetical_sell_bps"]/10000)/(a*(1+MARK_POLICY["buy_bps"]/10000))-1)
        adverse = 10000*np.maximum(0., 1-lower/a)
        mean, q90 = float(w @ net), weighted_quantile_v1(adverse, w, .9)
        if not np.isfinite([mean, q90]).all():
            raise ValueError("D price payoff arithmetic is nonfinite")
        values = dict(status="ACCEPTABLE" if mean > 0 and q90 <= MARK_POLICY["risk_bps"] else "AVOID",
            expected_net_bps=mean, profit_probability=float(np.clip(w @ (net > 0), 0., 1.)), downside_q90_bps=q90,
            weight_effective_sample_size=float(1/np.square(w).sum()), distribution_known_trees=trees)
        for name, value in values.items():
            out.iloc[i, out.columns.get_loc(name)] = value
    out["model_sha256"], out["mark_policy_sha256"], out["schema_sha256"] = fitted.model_sha256, MARK_POLICY_SHA256, SCHEMA_SHA256
    out["probability_clock"] = "D_HYPOTHETICAL_PAYMENT_NOT_ACTUAL_OPEN_CONDITIONAL"
    return out


def query_d_clock_population_nodes_v1(*, fitted, d_features, price_cny, reference_cny):
    frozen = freeze_d_clock_distribution_v1(fitted=fitted, d_features=d_features)
    return query_frozen_d_clock_nodes_v1(fitted=fitted, frozen=frozen, price_cny=price_cny, reference_cny=reference_cny)


def d_clock_population_price_set_v1(*, fitted, d_features, reference_cny, legal_low_cny, legal_high_cny, tick_cny):
    raw = (reference_cny, legal_low_cny, legal_high_cny, tick_cny)
    try:
        ref, low, high, tick = (Decimal(str(v)) for v in raw)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("explicit Decimal legal price coordinates required") from exc
    if (any(isinstance(v, (bool, np.bool_)) for v in raw) or any(not v.is_finite() or v <= 0 or not np.isfinite(float(v)) for v in (ref, low, high, tick))
            or low > high or set(d_features) != set(FEATURES)):
        raise ValueError("legal positive D-anchor grid or feature schema differs")
    first, last = int((low/tick).to_integral_value(rounding=ROUND_CEILING)), int((high/tick).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("complete legal grid exceeds 100000 nodes")
    prices = [tick*n for n in range(first, last+1)]
    one = freeze_d_clock_distribution_v1(fitted=fitted, d_features=pd.DataFrame([d_features]))
    frozen = one.loc[one.index.repeat(len(prices))].reset_index(drop=True)
    nodes = query_frozen_d_clock_nodes_v1(fitted=fitted, frozen=frozen, price_cny=[float(p) for p in prices], reference_cny=[float(ref)]*len(prices))
    acceptable = [i for i, s in enumerate(nodes.status) if s == "ACCEPTABLE"]
    if acceptable and acceptable != list(range(acceptable[-1]+1)):
        raise ValueError("positive fixed D mark distribution must have an acceptable price prefix")
    return dict(status="ACCEPTABLE_SET" if acceptable else "UNKNOWN" if nodes.status.str.startswith("UNKNOWN").any() else "EMPTY_ACCEPTABLE_SET",
        bands_cny=[dict(low=str(prices[0]), high=str(prices[acceptable[-1]]))] if acceptable else [],
        complete_nodes=len(prices), unknown_nodes=int(nodes.status.str.startswith("UNKNOWN").sum()),
        nodes=nodes.assign(price_cny=[str(p) for p in prices]), model_sha256=fitted.model_sha256,
        probability_clock="D_HYPOTHETICAL_PAYMENT_NOT_ACTUAL_OPEN_CONDITIONAL", low_price_bad_news_identified=False,
        actual_fill_proven=False, deployable=False)
