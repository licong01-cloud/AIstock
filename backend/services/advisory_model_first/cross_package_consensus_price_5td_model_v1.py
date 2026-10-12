"""Honest 19/20D paired mark distribution; payment price never affects D leaf weights."""
from copy import deepcopy
from collections import OrderedDict
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import re
import time

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.cross_package_consensus_price_5td_contracts_v1 import (
    ARMS, BASE_ORDER, CLOCK, CONTEXT_COLUMNS, FEATURES, KEY, MARK_POLICY, MARK_POLICY_SHA256,
    MATRIX_ORDERS, PARAMETERS, RANK_COLUMNS, SCHEMA, SCHEMA_SHA256,
)
from backend.services.advisory_model_first.cross_package_consensus_price_5td_inputs_v1 import validate_context_rows_v1
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_models_v1 import feature_values, matrix
from backend.services.advisory_model_first.generic_population_price_5td_model_v1 import weighted_quantile_v1
from backend.services.advisory_model_first.parent_score_price_5td_model_v1 import _integers, _tree_arrays
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@dataclass(frozen=True)
class CrossPackageConsensusFitV1:
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
        raise ValueError("consensus model exceeds its 30-minute/8GiB process budget")


def _encoding(encoding):
    dates = [pd.Timestamp(encoding[n]) for n in ("train_start", "estimation_first_D", "train_end", "evaluation_start", "evaluation_end")]
    medians = np.asarray([_number(v) for v in encoding.get("medians", ())], dtype=float)
    roles = encoding.get("role_clusters", {})
    if (encoding.get("matrix_order") != list(BASE_ORDER) or encoding.get("contrast_status") != "PREPARED_IDENTIFIABLE_NO_FIT"
            or medians.shape != (9,) or not np.isfinite(medians).all()
            or any(pd.isna(v) or v.tz is not None or v != v.normalize() for v in dates)
            or not dates[0] < dates[1] <= dates[2] < dates[3] <= dates[4]
            or set(roles) != {"STRUCTURE", "ESTIMATION"} or encoding.get("role_sha256") != sha(roles)
            or any(not isinstance(v, list) or not v or len(v) != len(set(v))
                   or len(v) != encoding["pools"][k] for k, v in roles.items())
            or set(roles["STRUCTURE"]) & set(roles["ESTIMATION"])
            or sum(map(len, roles.values())) > 21400
            or re.fullmatch(r"[a-f0-9]{64}", encoding.get("context_pair_sha256", "")) is None):
        raise ValueError("common honest supervision, original 18D medians or context pair differs")


def _matrix(rows, encoding, arm):
    validate_context_rows_v1(rows, pair_sha256=encoding["context_pair_sha256"])
    rank = rows.loc[:, RANK_COLUMNS if arm == ARMS[0] else RANK_COLUMNS[:1]].to_numpy(dtype=float)
    # Placeholder values are only used to walk typed UNKNOWN rows, never to produce known predictions.
    rank = np.where(rows.context_availability.eq("AVAILABLE").to_numpy()[:, None], rank, 0.)
    x = np.column_stack((matrix(rows, encoding["medians"]), rank)).astype(np.float32)
    if not np.isfinite(x).all():
        raise ValueError("19/20D float32 input overflows")
    return x

def consensus_forest_leaf_ids_v1(*, forest, x, dimensions):
    """Independent bounded walker; sklearn apply uses float32, not float64."""
    x = np.asarray(x, dtype=np.float32)
    if x.ndim != 2 or dimensions not in (19, 20) or x.shape[1] != dimensions or len(x) > 100000 or not np.isfinite(x).all() or len(forest) != 128:
        raise ValueError("consensus JSON forest requires finite bounded 19/20D inputs and 128 trees")
    output = np.zeros((len(x), len(forest)), dtype=np.int64)
    for i, tree in enumerate(forest):
        feature, threshold, left, right = _tree_arrays(tree, dimensions)
        nodes = np.zeros(len(x), dtype=np.int64)
        for _ in range(7):
            active = np.flatnonzero(left[nodes] != -1)
            if not len(active):
                break
            current = nodes[active]
            nodes[active] = np.where(x[active, feature[current]] <= threshold[current], left[current], right[current])
        if (left[nodes] != -1).any():
            raise ValueError("consensus JSON forest did not reach a leaf")
        output[:, i] = nodes
    return output


def train_cross_package_consensus_model_v1(*, rows, encoding, arm, plan_sha256, before_fit, after_fit):
    """One STRUCTURE terminal fit; equal unique clusters and honest paired estimation marks."""
    from sklearn.ensemble import RandomForestRegressor
    import sklearn
    from threadpoolctl import threadpool_limits
    started = time.monotonic()
    _budget(started)
    _encoding(encoding)
    if arm not in ARMS or re.fullmatch(r"[a-f0-9]{64}", plan_sha256) is None:
        raise ValueError("consensus arm/plan identity differs")
    # Apply role/time metadata BEFORE any feature/target decoder.
    pool = rows.loc[rows.consensus_training_role.isin(["STRUCTURE", "ESTIMATION"])].copy()
    d, t, h = (pd.to_datetime(pool[n]) for n in (*KEY[:2], "label_information_end"))
    if (not 0 < len(pool) <= 30500 or pool.duplicated(["package_id", *KEY]).any()
            or any(v.isna().any() or v.dt.tz is not None or not v.eq(v.dt.normalize()).all() for v in (d, t, h))
            or not (d.ge(encoding["train_start"]) & d.lt(t) & t.le(h) & h.le(encoding["train_end"])
                    & h.lt(encoding["evaluation_start"])).all()):
        raise ValueError("common training keys or train-only clocks differ")
    for role in ("STRUCTURE", "ESTIMATION"):
        block = pool.loc[pool.consensus_training_role.eq(role)]
        first = pd.Timestamp(encoding["estimation_first_D"])
        if (sorted(block.label_cluster.unique()) != sorted(encoding["role_clusters"][role])
                or role == "STRUCTURE" and not (pd.to_datetime(block[KEY[0]]).lt(first)
                    & pd.to_datetime(block.label_information_end).lt(first)).all()
                or role == "ESTIMATION" and not pd.to_datetime(block[KEY[0]]).ge(first).all()):
            raise ValueError("frozen honest membership or strict horizon purge differs")
    fields = [*FEATURES, *CONTEXT_COLUMNS, "reference_cny", "reference_visible_through", "terminal_mark_ratio",
        "path_mark_min_ratio", "mark_label_status", "mark_policy_sha256", "consensus_training_role", "label_information_end"]
    if pool.groupby("label_cluster")[fields].nunique(dropna=False).gt(1).any().any():
        raise ValueError("economic cluster D features/context/paired mark conflict")
    pool = pool.drop_duplicates("label_cluster").sort_values(list(KEY)).reset_index(drop=True)
    expected = [sha([pd.Timestamp(d).date().isoformat(), pd.Timestamp(t).date().isoformat(), symbol, MARK_POLICY_SHA256])
        for d, t, symbol in pool.loc[:, KEY].itertuples(index=False, name=None)]
    targets = pool.loc[:, ["terminal_mark_ratio", "path_mark_min_ratio"]].map(lambda v: _number(v, positive=True)).astype(float)
    _, basic = feature_values(pool)
    if (pool.label_cluster.tolist() != expected or not pool.mark_policy_sha256.eq(MARK_POLICY_SHA256).all()
            or not pool.mark_label_status.eq("AVAILABLE").all() or not pool.context_availability.eq("AVAILABLE").all()
            or targets.isna().any().any() or targets.iloc[:, 1].gt(targets.iloc[:, 0]).any() or not basic.all()
            or pool.reference_cny.map(lambda v: _number(v, positive=True)).isna().any()
            or not pd.to_datetime(pool.reference_visible_through).le(pd.to_datetime(pool[KEY[0]])).all()):
        raise ValueError("common known D supervision/reference differs")
    structure, estimation = pool.consensus_training_role.eq("STRUCTURE"), pool.consensus_training_role.eq("ESTIMATION")
    if not pool.loc[structure].groupby("strongest_rank_fraction").consensus_rank_fraction.nunique().gt(1).any():
        raise ValueError("NOT_IDENTIFIABLE_CONSENSUS_CONTRAST")
    x = _matrix(pool, encoding, arm)
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
    dimensions = len(MATRIX_ORDERS[arm])
    if not np.array_equal(consensus_forest_leaf_ids_v1(forest=forest, x=x, dimensions=dimensions), estimator.apply(x)):
        raise ValueError("19/20D JSON float32/apply parity failed")
    cal = dict(terminal=targets.loc[estimation].iloc[:, 0].tolist(), path=targets.loc[estimation].iloc[:, 1].tolist(),
        masses=[1.]*int(estimation.sum()), clusters=pool.loc[estimation, "label_cluster"].tolist(),
        keys=[[str(v) for v in key] for key in pool.loc[estimation, KEY].itertuples(index=False, name=None)],
        label_ends=[pd.Timestamp(v).date().isoformat() for v in pool.loc[estimation, "label_information_end"]])
    recipe = dict(schema=SCHEMA, schema_sha256=SCHEMA_SHA256, arm=arm, mark_policy_sha256=MARK_POLICY_SHA256,
        dimensions=dimensions, matrix_order=list(MATRIX_ORDERS[arm]), parameters=dict(PARAMETERS), encoding=deepcopy(encoding),
        encoding_sha256=sha(encoding), plan_sha256=plan_sha256, prediction_clock=CLOCK,
        context_pair_sha256=encoding["context_pair_sha256"])
    diagnostics = dict(physical_fit_count=1, internal_tree_count=128, structure_unique_clusters=int(structure.sum()),
        estimation_unique_clusters=int(estimation.sum()), structure_latest_label_end=pd.to_datetime(pool.loc[structure, "label_information_end"]).max().date().isoformat(),
        estimation_keys_sha256=sha(cal["keys"]), sklearn_version=sklearn.__version__, evaluation_used_for_training=False, deployable=False)
    fitted = CrossPackageConsensusFitV1(recipe, tuple(forest), cal, diagnostics, "")
    fitted = CrossPackageConsensusFitV1(recipe, fitted.forest, cal, diagnostics, _identity(fitted))
    validate_consensus_fit_v1(fitted)
    return fitted

def validate_consensus_fit_v1(fitted):
    if not isinstance(fitted, CrossPackageConsensusFitV1) or _identity(fitted) != fitted.model_sha256:
        raise ValueError("independent consensus model identity differs")
    r, c = fitted.recipe, fitted.calibration
    arm = r.get("arm")
    dimensions = len(MATRIX_ORDERS[arm]) if arm in ARMS else -1
    if (arm not in ARMS or r.get("schema") != SCHEMA or r.get("schema_sha256") != SCHEMA_SHA256
            or r.get("mark_policy_sha256") != MARK_POLICY_SHA256 or r.get("dimensions") != dimensions
            or r.get("matrix_order") != list(MATRIX_ORDERS[arm]) or r.get("parameters") != dict(PARAMETERS)
            or r.get("prediction_clock") != CLOCK
            or r.get("context_pair_sha256") != r["encoding"].get("context_pair_sha256")
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
            or sorted(c["clusters"]) != sorted(encoding["role_clusters"]["ESTIMATION"])
            or diagnostic.get("physical_fit_count") != 1 or diagnostic.get("internal_tree_count") != 128
            or diagnostic.get("estimation_keys_sha256") != sha(c["keys"])
            or diagnostic.get("estimation_unique_clusters") != n
            or diagnostic.get("structure_unique_clusters") != encoding["pools"]["STRUCTURE"]
            or diagnostic.get("evaluation_used_for_training") is not False or diagnostic.get("deployable") is not False
            or not pd.Timestamp(diagnostic["structure_latest_label_end"]) < pd.Timestamp(encoding["estimation_first_D"])):
        raise ValueError("serialized honest training clocks/counts differ")
    for tree in fitted.forest:
        _, _, left, _ = _tree_arrays(tree, dimensions)
        members = []
        for leaf, ids in tree["members"].items():
            if (not isinstance(leaf, str) or re.fullmatch(r"0|[1-9][0-9]*", leaf) is None
                    or int(leaf) >= len(left) or left[int(leaf)] != -1):
                raise ValueError("serialized estimation member is not in a valid leaf")
            members.extend(_integers(ids).tolist())
        if sorted(members) != list(range(n)):
            raise ValueError("serialized tree drops/duplicates estimation members")


def consensus_model_payload_v1(fitted):
    validate_consensus_fit_v1(fitted)
    return asdict(fitted)


def consensus_model_from_payload_v1(payload):
    if not isinstance(payload, dict) or set(payload) != {"recipe", "forest", "calibration", "diagnostics", "model_sha256"}:
        raise ValueError("consensus non-executing payload fields differ")
    body = deepcopy(payload)
    body["forest"] = tuple(body["forest"])
    fitted = CrossPackageConsensusFitV1(**body)
    validate_consensus_fit_v1(fitted)
    return fitted


def _frozen_identity(row):
    if any(not isinstance(v, (int, np.integer)) or isinstance(v, (bool, np.bool_)) for v in row["leaf_ids"]):
        raise ValueError("frozen leaf identity requires true integers")
    body = {n: row[n] if n != "leaf_ids" else [int(v) for v in row[n]] for n in
        ("context_sha256", "context_pair_sha256", "model_sha256", "schema_sha256", "mark_policy_sha256",
         "reference_sha256", "distribution_status", "leaf_ids", *KEY)}
    for name in KEY[:2]:
        clock = pd.Timestamp(body[name])
        if pd.isna(clock) or clock.tz is not None or clock != clock.normalize():
            raise ValueError("frozen distribution has a non-date clock")
        body[name] = clock.date().isoformat()
    if body[KEY[0]] >= body[KEY[1]]:
        raise ValueError("frozen D/T clock order differs")
    return sha(body)


def _reference_identity(row):
    d = pd.Timestamp(row[KEY[0]])
    visible = pd.Timestamp(row["reference_visible_through"])
    if pd.isna(d) or d.tz is not None or d != d.normalize() or pd.notna(visible) and (
            visible.tz is not None or visible != visible.normalize() or visible > d):
        raise ValueError("frozen reference has a future/non-date clock")
    return sha(dict(D=d.date().isoformat(), instrument=row["instrument"],
        reference_cny=_number(row["reference_cny"], positive=True),
        visible_through=visible.date().isoformat() if pd.notna(visible) else None,
        context_sha256=row["context_sha256"], coordinate="D_ANCHORED_CNY"))


def freeze_consensus_distribution_v1(*, fitted, d_features):
    """Original complete D roster only: freeze both leaves and context/reference coordinates."""
    validate_consensus_fit_v1(fitted)
    if not d_features.index.is_unique or len(d_features) > 30500 or not set((*KEY, "reference_cny", "reference_visible_through")).issubset(d_features):
        raise ValueError("unique bounded D query keys are required")
    _, basic = feature_values(d_features)
    x = _matrix(d_features, fitted.recipe["encoding"], fitted.recipe["arm"])
    leaves = consensus_forest_leaf_ids_v1(forest=fitted.forest, x=x, dimensions=fitted.recipe["dimensions"])
    metadata = [n for n in ("package_id", "manifest_sha256", "run_id", "source_id", *KEY, *CONTEXT_COLUMNS,
                          "reference_cny", "reference_visible_through") if n in d_features]
    out = d_features.loc[:, metadata].copy().reset_index(drop=True)
    out[KEY[0]], out[KEY[1]] = pd.to_datetime(out[KEY[0]]), pd.to_datetime(out[KEY[1]])
    if (out.loc[:, KEY].isna().any().any() or not out[KEY[0]].lt(out[KEY[1]]).all()
            or out.reference_cny.map(lambda v: _number(v, positive=True)).notna().any()
            and not pd.to_datetime(out.loc[out.reference_cny.notna(), "reference_visible_through"]).le(
                out.loc[out.reference_cny.notna(), KEY[0]]).all()):
        raise ValueError("D reference/query clocks differ")
    out["leaf_ids"] = [row.tolist() for row in leaves]
    out["distribution_status"] = np.where(~d_features.context_availability.eq("AVAILABLE").to_numpy(), "UNKNOWN_CONTEXT",
        np.where(basic, "DISTRIBUTION_FROZEN", "UNKNOWN_STOCK_INPUT"))
    out["model_sha256"], out["mark_policy_sha256"], out["schema_sha256"] = fitted.model_sha256, MARK_POLICY_SHA256, SCHEMA_SHA256
    out["reference_sha256"] = [_reference_identity(row) for row in out.to_dict("records")]
    out["distribution_sha256"] = [_frozen_identity(row) for row in out.to_dict("records")]
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


def query_frozen_consensus_nodes_v1(*, fitted, frozen, price_cny, reference_cny):
    validate_consensus_fit_v1(fitted)
    if (not frozen.index.is_unique or len(frozen) > 100000 or not frozen.model_sha256.eq(fitted.model_sha256).all()
            or not frozen.schema_sha256.eq(SCHEMA_SHA256).all() or not frozen.mark_policy_sha256.eq(MARK_POLICY_SHA256).all()
            or not frozen.distribution_status.isin(["DISTRIBUTION_FROZEN", "UNKNOWN_STOCK_INPUT", "UNKNOWN_CONTEXT"]).all()
            or not frozen.context_pair_sha256.eq(fitted.recipe["context_pair_sha256"]).all()
            or any(row["distribution_sha256"] != _frozen_identity(row) or row["reference_sha256"] != _reference_identity(row)
                   for row in frozen.to_dict("records"))):
        raise ValueError("frozen D distribution/model identity differs")
    prices, refs = (np.asarray([_number(v, positive=True) for v in values], dtype=float) for values in (price_cny, reference_cny))
    if prices.shape != (len(frozen),) or refs.shape != prices.shape:
        raise ValueError("one explicit price and D reference required per query")
    bound = np.asarray([_number(v, positive=True) for v in frozen.reference_cny], dtype=float)
    if ((np.isfinite(bound) & np.isfinite(refs) & (bound != refs)).any()
            or (np.isfinite(refs) & ~np.isfinite(bound)).any()):
        raise ValueError("query reference differs from the D-frozen coordinate")
    metadata = [n for n in ("package_id", "manifest_sha256", "run_id", "source_id", *KEY, "context_sha256",
                          "context_pair_sha256", "reference_sha256", "distribution_sha256") if n in frozen]
    out = frozen.loc[:, metadata].copy()
    out["status"] = np.where(frozen.distribution_status.ne("DISTRIBUTION_FROZEN"), frozen.distribution_status,
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
            w, trees = _weights_for_leaves(fitted, leaves)
            positive = w > 0
            order = np.argsort(y[positive], kind="stable")
            sorted_y = y[positive][order]
            cumulative = np.cumsum(w[positive][order])
            # Positive affine transforms commute with an inverted-CDF quantile.
            # Sort the paired mark law once per D-leaf tuple, not once per payment tick.
            q90_negative_low = weighted_quantile_v1(-lower, w, .9) if trees else None
            cache[leaves] = w, trees, float(w @ y), sorted_y, cumulative, q90_negative_low
            if len(cache) > 128:
                cache.popitem(last=False)
        cache.move_to_end(leaves)
        w, trees, mean_y, sorted_y, cumulative, q90_negative_low = cache[leaves]
        if not trees:
            out.iloc[i, out.columns.get_loc("status")] = "UNKNOWN_MODEL_DISTRIBUTION"
            continue
        a = prices[i]/refs[i]
        scale = (1-MARK_POLICY["hypothetical_sell_bps"]/10000)/(a*(1+MARK_POLICY["buy_bps"]/10000))
        mean = 10000*(mean_y*scale-1)
        q90 = 10000*max(0., 1+q90_negative_low/a)
        below = np.searchsorted(sorted_y, 1/scale, side="right")
        probability = float(np.clip(1-(cumulative[below-1] if below else 0.), 0., 1.))
        if not np.isfinite([mean, q90]).all():
            raise ValueError("D price payoff arithmetic is nonfinite")
        values = dict(status="ACCEPTABLE" if mean > 0 and q90 <= MARK_POLICY["risk_bps"] else "AVOID",
            expected_net_bps=mean, profit_probability=probability, downside_q90_bps=q90,
            weight_effective_sample_size=float(1/np.square(w).sum()), distribution_known_trees=trees)
        for name, value in values.items():
            out.iloc[i, out.columns.get_loc(name)] = value
    out["model_sha256"], out["mark_policy_sha256"], out["schema_sha256"] = fitted.model_sha256, MARK_POLICY_SHA256, SCHEMA_SHA256
    out["probability_clock"] = "D_HYPOTHETICAL_PAYMENT_NOT_ACTUAL_OPEN_CONDITIONAL"
    out["actual_fill_proven"] = False
    out["cost_sha256"] = sha({k: MARK_POLICY[k] for k in ("buy_bps", "hypothetical_sell_bps")})
    return out


def query_cross_package_consensus_price_nodes_v1(*, fitted, d_features, price_cny, reference_cny):
    frozen = freeze_consensus_distribution_v1(fitted=fitted, d_features=d_features)
    return query_frozen_consensus_nodes_v1(fitted=fitted, frozen=frozen, price_cny=price_cny, reference_cny=reference_cny)


def cross_package_consensus_price_set_v1(*, fitted, d_features, reference_cny, legal_low_cny, legal_high_cny, tick_cny, grid_context):
    raw = (reference_cny, legal_low_cny, legal_high_cny, tick_cny)
    try:
        ref, low, high, tick = (Decimal(str(v)) for v in raw)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("explicit Decimal legal price coordinates required") from exc
    if (any(isinstance(v, (bool, np.bool_)) for v in raw) or any(not v.is_finite() or v <= 0 or not np.isfinite(float(v)) for v in (ref, low, high, tick))
            or low > high or not set(FEATURES).issubset(d_features)):
        raise ValueError("legal positive D-anchor grid or feature schema differs")
    if (not isinstance(grid_context, dict) or set(grid_context) != {"source_sha256", "observed_at", "coordinate", "currency"}
            or re.fullmatch(r"[a-f0-9]{64}", grid_context["source_sha256"]) is None
            or grid_context["coordinate"] != "D_ANCHORED_CNY" or grid_context["currency"] != "CNY"
            or pd.isna(pd.Timestamp(grid_context["observed_at"]))):
        raise ValueError("explicit legal-grid source/observation clock and D coordinate are required")
    first, last = int((low/tick).to_integral_value(rounding=ROUND_CEILING)), int((high/tick).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("complete legal grid exceeds 100000 nodes")
    prices = [tick*n for n in range(first, last+1)]
    one = freeze_consensus_distribution_v1(fitted=fitted, d_features=pd.DataFrame([d_features]))
    batches = []
    for start in range(0, len(prices), 128):
        selected = prices[start:start+128]
        frozen = one.loc[one.index.repeat(len(selected))].reset_index(drop=True)
        batches.append(query_frozen_consensus_nodes_v1(fitted=fitted, frozen=frozen,
            price_cny=[float(p) for p in selected], reference_cny=[float(ref)]*len(selected)))
    nodes = pd.concat(batches, ignore_index=True) if batches else query_frozen_consensus_nodes_v1(
        fitted=fitted, frozen=one.iloc[:0], price_cny=[], reference_cny=[])
    acceptable = [i for i, s in enumerate(nodes.status) if s == "ACCEPTABLE"]
    if acceptable and acceptable != list(range(acceptable[-1]+1)):
        raise ValueError("positive fixed D mark distribution must have an acceptable price prefix")
    return dict(status="ACCEPTABLE_SET" if acceptable else "UNKNOWN" if nodes.status.str.startswith("UNKNOWN").any() else "EMPTY_ACCEPTABLE_SET",
        bands_cny=[dict(low=str(prices[0]), high=str(prices[acceptable[-1]]))] if acceptable else [],
        grid_sha256=sha(dict(context=grid_context, low=str(low), high=str(high), tick=str(tick))),
        reference_sha256=one.reference_sha256.iloc[0], context_sha256=one.context_sha256.iloc[0],
        mark_policy_sha256=MARK_POLICY_SHA256, complete_nodes=len(prices), unknown_nodes=int(nodes.status.str.startswith("UNKNOWN").sum()),
        nodes=nodes.assign(price_cny=[str(p) for p in prices]), model_sha256=fitted.model_sha256,
        probability_clock="D_HYPOTHETICAL_PAYMENT_NOT_ACTUAL_OPEN_CONDITIONAL", low_price_bad_news_identified=False,
        actual_fill_proven=False, deployable=False)
