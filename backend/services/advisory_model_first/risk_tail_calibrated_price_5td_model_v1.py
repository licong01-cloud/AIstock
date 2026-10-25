"""One honest 20D core, one disjoint risk calibration; no execution or activation."""
from copy import deepcopy
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import re
import time
from types import SimpleNamespace

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_models_v1 import feature_values, matrix
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.generic_population_price_5td_model_v1 import weighted_quantile_v1
from backend.services.advisory_model_first.parent_score_price_5td_model_v1 import forest_leaf_ids, joint_cluster_weights
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_contracts_v1 import (
    ARMS, CALIBRATION, MATRIX_ORDER, PARAMETERS, ROSTER_KEY, SCHEMA, SCHEMA_SHA256,
)
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_inputs_v1 import (
    CAL_FINANCE, META, clocks_v1, cluster_v1, sessions_v1, validate_finance_v1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@dataclass(frozen=True)
class RiskTailCoreV1:
    recipe: dict
    forest: tuple
    calibration: dict  # ESTIMATION leaf mass only; never the later CAL risk head.
    diagnostics: dict
    model_sha256: str


def _identity(value, field):
    return sha({k: v for k, v in value.items() if k != field})


def _budget(started):
    import psutil
    if time.monotonic()-started > 1800 or psutil.Process().memory_info().rss > 8*1024**3:
        raise ValueError("risk calibration exceeds its 30-minute/8GiB process budget")


def _encoding(value):
    if value.get("matrix_order") != list(MATRIX_ORDER):
        raise ValueError("risk core requires its independent 20D encoding")
    dates = [pd.Timestamp(value[n]) for n in ("train_start", "estimation_first_D", "train_end", "calibration_first_D",
        "calibration_train_end", "evaluation_start", "evaluation_end")]
    if (any(pd.isna(v) or v.tz is not None or v != v.normalize() for v in dates)
            or not dates[0] <= dates[1] <= dates[2] < dates[3] <= dates[4] < dates[5] <= dates[6]
            or {c["context_bit"] for c in value["contexts"].values()} != {0, 1} or len(value["contexts"]) != 2
            or any(type(c["context_bit"]) is not int for c in value["contexts"].values())):
        raise ValueError("risk core three-pool clocks/context differ")
    medians = np.asarray([_number(v) for v in value["medians"]], dtype=float)
    if medians.shape != (9,) or not np.isfinite(medians).all():
        raise ValueError("risk core needs finite train-only medians")
    expected = clocks_v1(SimpleNamespace(train_start=value["train_start"], train_end=value["calibration_train_end"],
        evaluation_start=value["evaluation_start"], evaluation_end=value["evaluation_end"]), value["calendar"])
    if any(value.get(n) != v for n, v in expected.items()):
        raise ValueError("risk core calendar-derived pool partition differs")
    return ValueAnchorGapSupportV1(tuple(tuple(v) for v in value["intervals_bps"]))


def _matrix(features, encoding, gaps):
    contexts, known = [], []
    for row in features.to_dict("records"):
        context = encoding["contexts"].get(row.get("package_id"))
        good = context is not None and all(row.get(n) == context[n] for n in ("manifest_sha256", "run_id"))
        contexts.append(context["context_bit"] if good else 0)
        known.append(good)
    values = np.column_stack((matrix(features, encoding["medians"], gaps=gaps), contexts)).astype(np.float32)
    if not np.isfinite(values).all():
        raise ValueError("risk core float32 coordinates overflow")
    return values, np.asarray(known, dtype=bool)


def fit_shared_core_v1(*, rows, encoding, plan_sha256, before_fit, after_fit):
    from sklearn.ensemble import RandomForestRegressor
    import sklearn
    from threadpoolctl import threadpool_limits
    started = time.monotonic()
    support = _encoding(encoding)
    pool = rows.loc[rows.pool.isin(["STRUCTURE", "ESTIMATION"])].copy()
    if not 0 < len(pool) <= 100000 or pool.duplicated(["package_id", *KEY]).any() or not re.fullmatch(r"[a-f0-9]{64}", plan_sha256):
        raise ValueError("risk core needs unique bounded original supervision")
    structure, estimation = pool.pool.eq("STRUCTURE"), pool.pool.eq("ESTIMATION")
    d, h = pd.to_datetime(pool[KEY[0]]), pd.to_datetime(pool.label_information_end)
    first, end = pd.Timestamp(encoding["estimation_first_D"]), pd.Timestamp(encoding["train_end"])
    if (not structure.any() or not estimation.any() or not d.between(encoding["train_start"], end).all()
            or not h.le(end).all() or not (d[structure].lt(first) & h[structure].lt(first)).all()
            or not d[estimation].ge(first).all() or not pool.valuation_status.eq("AVAILABLE").all()
            or any(int(mask.sum()) != encoding["pools"][role] for role, mask in (("STRUCTURE", structure), ("ESTIMATION", estimation)))):
        raise ValueError("risk supervision pool/honest purge differs; no fit")
    validate_finance_v1(pool, encoding["calendar"], (*FEATURES, "observed_gap_bps", "valuation_status",
        "valuation_policy_sha256", "label_information_end", "valuation_gross_terminal_ratio", "valuation_path_min_ratio"))
    targets = pool[["valuation_gross_terminal_ratio", "valuation_path_min_ratio"]].map(lambda v: _number(v, positive=True))
    masses = pool.cluster_mass.map(lambda v: _number(v, positive=True)).to_numpy(float)
    if (not np.isfinite(masses).all() or not np.allclose(pool.assign(m=masses).groupby("label_cluster").m.sum(), 1.)
            or not all(c == cluster_v1(*key) for c, key in zip(pool.label_cluster, pool[list(KEY)].itertuples(index=False, name=None), strict=True))):
        raise ValueError("risk supervision unique-cluster mass differs")
    gaps = pool.observed_gap_bps.map(_number).to_numpy(float)
    _, stock = feature_values(pool)
    if not stock.all() or not all(support.contains(v) for v in gaps):
        raise ValueError("risk supervision stock/price support differs")
    xs, ks = _matrix(pool.loc[structure], encoding, gaps[structure])
    xe, ke = _matrix(pool.loc[estimation], encoding, gaps[estimation])
    if not ks.all() or not ke.all():
        raise ValueError("risk supervision package context differs")
    estimator = RandomForestRegressor(**PARAMETERS)
    _budget(started)
    before_fit(ARMS[0])
    try:
        with threadpool_limits(limits=2):
            estimator.fit(xs, targets.loc[structure].iloc[:, 0], sample_weight=masses[structure])
    finally:
        after_fit(ARMS[0])
    _budget(started)
    leaves = estimator.apply(xe)
    forest = []
    for i, tree in enumerate(estimator.estimators_):
        body = {name: getattr(tree.tree_, attr).tolist() for name, attr in
            (("feature", "feature"), ("threshold", "threshold"), ("left", "children_left"), ("right", "children_right"))}
        body["members"] = {str(int(leaf)): np.flatnonzero(leaves[:, i] == leaf).tolist() for leaf in np.unique(leaves[:, i])}
        forest.append(body)
    for x in (xs, xe):
        if not np.array_equal(forest_leaf_ids(forest=forest, x=x, dimensions=20), estimator.apply(x)):
            raise ValueError("risk core JSON float32 leaf parity differs")
    c = dict(terminal=targets.loc[estimation].iloc[:, 0].tolist(), path=targets.loc[estimation].iloc[:, 1].tolist(),
        masses=masses[estimation].tolist(), clusters=pool.loc[estimation, "label_cluster"].tolist(),
        keys=[[str(v) for v in key] for key in pool.loc[estimation, ["package_id", *KEY]].itertuples(index=False, name=None)],
        label_ends=[v.date().isoformat() for v in h[estimation]])
    recipe = dict(schema=SCHEMA, schema_sha256=SCHEMA_SHA256, dimensions=20, matrix_order=list(MATRIX_ORDER),
        parameters=dict(PARAMETERS), plan_sha256=plan_sha256, policy_sha256=POLICY_SHA256,
        valuation_policy_sha256=VALUATION_POLICY_SHA256, encoding=deepcopy(encoding), encoding_sha256=sha(encoding))
    diagnostics = dict(physical_fit_count=1, internal_tree_count=128, structure_rows=int(structure.sum()),
        estimation_rows=int(estimation.sum()), structure_latest_label_end=h[structure].max().date().isoformat(),
        estimation_keys_sha256=sha(c["keys"]), sklearn_version=sklearn.__version__,
        evaluation_used_for_training=False, calibration_used_for_training=False, deployable=False)
    body = dict(recipe=recipe, forest=tuple(forest), calibration=c, diagnostics=diagnostics)
    fitted = RiskTailCoreV1(**body, model_sha256=sha(body))
    validate_core_v1(fitted)
    return fitted


def validate_core_v1(core):
    if not isinstance(core, RiskTailCoreV1) or _identity(asdict(core), "model_sha256") != core.model_sha256:
        raise ValueError("risk core non-executing payload identity differs")
    r, c, diagnostic = core.recipe, core.calibration, core.diagnostics
    if (r.get("schema") != SCHEMA or r.get("schema_sha256") != SCHEMA_SHA256 or r.get("dimensions") != 20
            or r.get("matrix_order") != list(MATRIX_ORDER) or r.get("parameters") != dict(PARAMETERS)
            or r.get("policy_sha256") != POLICY_SHA256 or r.get("valuation_policy_sha256") != VALUATION_POLICY_SHA256
            or r.get("encoding_sha256") != sha(r["encoding"]) or not re.fullmatch(r"[a-f0-9]{64}", r.get("plan_sha256", ""))
            or len(core.forest) != 128):
        raise ValueError("risk core recipe/schema/policy differs")
    support = _encoding(r["encoding"])
    n = len(c["terminal"])
    values = np.asarray([[_number(v, positive=True) for v in c[name]] for name in ("terminal", "path", "masses")], dtype=float)
    if (not 0 < n <= 100000 or values.shape != (3, n) or not np.isfinite(values).all() or (values[1] > values[0]).any()
            or any(len(c[name]) != n for name in ("clusters", "keys", "label_ends"))
            or len({tuple(k) for k in c["keys"]}) != n or any(len(k) != 4 for k in c["keys"])):
        raise ValueError("risk core paired leaf samples differ")
    pairs = pd.DataFrame(dict(cluster=c["clusters"], terminal=c["terminal"], path=c["path"], mass=c["masses"]))
    if pairs.groupby("cluster")[["terminal", "path"]].nunique().gt(1).any().any() or not np.allclose(pairs.groupby("cluster").mass.sum(), 1.):
        raise ValueError("risk core duplicate-cluster observations differ")
    keys = pd.DataFrame(c["keys"], columns=["package_id", *KEY])
    d, t, h = (pd.to_datetime(v) for v in (keys[KEY[0]], keys[KEY[1]], pd.Series(c["label_ends"])))
    encoding = r["encoding"]
    sessions = sessions_v1(encoding["calendar"])
    if (not keys.package_id.isin(encoding["contexts"]).all()
            or any(v.isna().any() or v.dt.tz is not None or not v.eq(v.dt.normalize()).all() for v in (d, t, h))
            or not (d.ge(pd.Timestamp(encoding["estimation_first_D"])) & d.lt(t) & t.le(h)
                & h.le(pd.Timestamp(encoding["train_end"]))).all()
            or not all(ti == sessions[sessions.get_loc(di)+1] and hi == sessions[sessions.get_loc(di)+5]
                and cluster == cluster_v1(di, ti, instrument) for di, ti, hi, instrument, cluster in
                zip(d, t, h, keys.instrument, c["clusters"], strict=True))
            or diagnostic.get("physical_fit_count") != 1 or diagnostic.get("internal_tree_count") != 128
            or diagnostic.get("structure_rows") != encoding["pools"]["STRUCTURE"] or n != encoding["pools"]["ESTIMATION"]
            or diagnostic.get("estimation_rows") != n or diagnostic.get("estimation_keys_sha256") != sha(c["keys"])
            or diagnostic.get("evaluation_used_for_training") is not False or diagnostic.get("calibration_used_for_training") is not False
            or diagnostic.get("deployable") is not False
            or not pd.Timestamp(diagnostic["structure_latest_label_end"]) < pd.Timestamp(encoding["estimation_first_D"])):
        raise ValueError("risk core serialized clocks/diagnostics differ")
    forest_leaf_ids(forest=core.forest, x=np.empty((0, 20)), dimensions=20)
    for tree in core.forest:
        members = []
        for leaf, ids in tree["members"].items():
            if (not isinstance(leaf, str) or not re.fullmatch(r"0|[1-9][0-9]*", leaf)
                    or int(leaf) >= len(tree["left"]) or tree["left"][int(leaf)] != -1
                    or not isinstance(ids, list) or any(type(i) is not int for i in ids)):
                raise ValueError("risk core leaf membership differs")
            members.extend(ids)
        if sorted(members) != list(range(n)):
            raise ValueError("risk core drops/duplicates estimation members")
    return support


def core_payload_v1(core):
    validate_core_v1(core)
    return asdict(core)


def core_from_payload_v1(payload):
    if set(payload) != {"recipe", "forest", "calibration", "diagnostics", "model_sha256"}:
        raise ValueError("risk core JSON fields differ")
    values = deepcopy(payload)
    values["forest"] = tuple(values["forest"])
    core = RiskTailCoreV1(**values)
    validate_core_v1(core)
    return core


def _raw_nodes(core, features, scenario_gap_bps):
    started = time.monotonic()
    support = validate_core_v1(core)
    if not features.index.is_unique or len(features) > 100000 or not set(ROSTER_KEY).issubset(features):
        raise ValueError("risk query original identity/index budget differs")
    gaps = np.asarray([_number(v) for v in scenario_gap_bps], dtype=float)
    if gaps.shape != (len(features),) or (gaps[np.isfinite(gaps)] <= -10000).any():
        raise ValueError("risk hypothetical entry coordinates differ")
    _, stock = feature_values(features)
    encoded, package = _matrix(features, core.recipe["encoding"], np.where(np.isfinite(gaps), gaps, 0.))
    supported = np.array([np.isfinite(v) and support.contains(float(v)) for v in gaps], dtype=bool)
    result = features.loc[:, list(ROSTER_KEY)].copy()
    result["status"] = np.where(~stock, "UNKNOWN_STOCK_INPUT", np.where(~package, "UNKNOWN_PACKAGE_ADAPTER",
        np.where(~np.isfinite(gaps), "UNKNOWN_PRICE_SCENARIO", np.where(~supported, "UNKNOWN_GAP_SUPPORT", "UNKNOWN_MODEL_DISTRIBUTION"))))
    for name in ("expected_net_bps", "profit_probability", "downside_q90_bps", "weight_effective_sample_size"):
        result[name] = np.nan
    result["distribution_unique_samples"], result["distribution_known_trees"] = 0, 0
    selected = np.flatnonzero(stock & package & supported)
    for first in range(0, len(selected), 128):
        _budget(started)
        ids = selected[first:first+128]
        weights, trees, terminal, path = joint_cluster_weights(fitted=core, x=encoded[ids])
        for row, w, known in zip(ids, weights, trees, strict=True):
            if not known:
                continue
            a = 1+gaps[row]/10000
            net = 10000*(terminal*(1-POLICY["sell_bps"]/10000)/(a*(1+POLICY["buy_bps"]/10000))-1)
            risk = 10000*np.maximum(0., 1-path/a)
            mean, downside = float(w @ net), weighted_quantile_v1(risk, w, .9)
            values = dict(expected_net_bps=mean, profit_probability=float(np.clip(w @ (net > 0), 0., 1.)),
                downside_q90_bps=downside, weight_effective_sample_size=float(1/np.square(w).sum()),
                distribution_unique_samples=int((w > 0).sum()), distribution_known_trees=int(known))
            if not np.isfinite(list(values.values())).all():
                raise ValueError("risk value/distribution arithmetic is nonfinite")
            values["status"] = "ACCEPTABLE" if mean > 0 and downside <= POLICY["risk_bps"] else "AVOID"
            for name, value in values.items():
                result.iloc[row, result.columns.get_loc(name)] = value
    return result


def fit_tail_residual_calibration_v1(*, core, calibration_rows, original_calibration_roster, plan_sha256):
    validate_core_v1(core)
    encoding = core.recipe["encoding"]
    original = original_calibration_roster
    if (plan_sha256 != core.recipe["plan_sha256"] or original.duplicated(list(ROSTER_KEY)).any()
            or len(original) != encoding["calibration_original_rows"]
            or sha(original.astype(str).to_dict("records")) != encoding["calibration_roster_sha256"]
            or not original[KEY[0]].isin(pd.to_datetime(encoding["calibration_dates"])).all()
            or not original.selection_effective_rank.between(1, 5).all()
            or calibration_rows.duplicated(list(ROSTER_KEY)).any()
            or set(calibration_rows[list(ROSTER_KEY)].itertuples(index=False, name=None))
                != set(original[list(ROSTER_KEY)].itertuples(index=False, name=None))):
        raise ValueError("risk calibration original Top5/plan identity differs")
    if not calibration_rows[list(META)].set_index(list(ROSTER_KEY)).sort_index().equals(
            original[list(META)].set_index(list(ROSTER_KEY)).sort_index()):
        raise ValueError("risk calibration changed original ranks/slot population")
    validate_finance_v1(calibration_rows, encoding["calendar"], CAL_FINANCE)
    rows = original[list(ROSTER_KEY)+["original_weight"]].merge(calibration_rows, on=list(ROSTER_KEY), validate="one_to_one")
    forecasts = _raw_nodes(core, rows, rows.observed_gap_bps)
    known = rows.valuation_status.eq("AVAILABLE") & forecasts.status.isin(["ACCEPTABLE", "AVOID"])
    weights = rows.original_weight.to_numpy(float)
    if np.isinf(weights).any() or (weights[np.isfinite(weights)] <= 0).any():
        raise ValueError("risk calibration original weights differ")
    known &= np.isfinite(weights)
    residuals = []
    for i in np.flatnonzero(known):
        gap = _number(rows.observed_gap_bps.iloc[i])
        lower = _number(rows.valuation_path_min_ratio.iloc[i], positive=True)
        residuals.append(10000*max(0., 1-lower/(1+gap/10000))-float(forecasts.downside_q90_bps.iloc[i]))
    delta = weighted_quantile_v1(residuals, weights[known], .9) if residuals else None
    card = dict(calibration_status="AVAILABLE" if delta is not None else "UNKNOWN_RISK_CALIBRATION", delta=delta,
        effective_raw_cutoff=POLICY["risk_bps"]-delta if delta is not None else None,
        original_rows=len(original), known_rows=int(known.sum()), original_days=60,
        known_days=int(rows.loc[known, KEY[0]].nunique()), known_original_mass=float(weights[known].sum()),
        weight_rule=CALIBRATION["weight_rule"], calibration_estimation_attempts=1, calibrated_parameter_count=int(delta is not None),
        base_model_sha256=core.model_sha256, plan_sha256=plan_sha256,
        source_refs=dict(**encoding["source_ref"], calibration_roster_sha256=encoding["calibration_roster_sha256"]),
        policy_sha256=POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256,
        schema_sha256=SCHEMA_SHA256, conditional_coverage_guaranteed=False)
    card["calibration_sha256"] = sha(card)
    validate_card_v1(core, card)
    return card


def validate_card_v1(core, card):
    if (_identity(card, "calibration_sha256") != card.get("calibration_sha256")
            or card.get("base_model_sha256") != core.model_sha256 or card.get("plan_sha256") != core.recipe["plan_sha256"]
            or card.get("policy_sha256") != POLICY_SHA256 or card.get("valuation_policy_sha256") != VALUATION_POLICY_SHA256
            or card.get("schema_sha256") != SCHEMA_SHA256 or card.get("weight_rule") != CALIBRATION["weight_rule"]
            or card.get("source_refs") != dict(**core.recipe["encoding"]["source_ref"],
                calibration_roster_sha256=core.recipe["encoding"]["calibration_roster_sha256"])
            or card.get("conditional_coverage_guaranteed") is not False or card.get("calibration_estimation_attempts") != 1
            or any(type(card.get(n)) is not int for n in ("original_rows", "known_rows", "original_days", "known_days"))
            or card["original_rows"] != core.recipe["encoding"]["calibration_original_rows"]
            or not 0 <= card["known_rows"] <= card["original_rows"] <= 100000
            or not 0 <= card["known_days"] <= card["original_days"] == 60):
        raise ValueError("risk calibration card source/base/policy/count identity differs")
    mass = _number(card.get("known_original_mass"), nonnegative=True)
    if mass is None or mass > 1+1e-9:
        raise ValueError("risk calibration original mass differs")
    if card["calibration_status"] == "AVAILABLE":
        delta = _number(card["delta"])
        if (delta is None or not -10000 <= delta <= 10000 or not mass > 0 or not card["known_rows"] > 0
                or card["effective_raw_cutoff"] != POLICY["risk_bps"]-delta or card.get("calibrated_parameter_count") != 1):
            raise ValueError("available risk calibration parameter differs")
    elif (card["calibration_status"] != "UNKNOWN_RISK_CALIBRATION" or card["delta"] is not None
            or card["effective_raw_cutoff"] is not None or mass != 0 or card["known_rows"] != 0
            or card.get("calibrated_parameter_count") != 0):
        raise ValueError("unknown risk calibration cannot claim a parameter")


def bundle_payload_v1(core, card):
    validate_core_v1(core)
    validate_card_v1(core, card)
    value = dict(core=core_payload_v1(core), tail_calibration=deepcopy(card))
    return dict(**value, bundle_sha256=sha(value))


def bundle_from_payload_v1(value):
    if set(value) != {"core", "tail_calibration", "bundle_sha256"} or _identity(value, "bundle_sha256") != value["bundle_sha256"]:
        raise ValueError("risk bundle JSON identity differs")
    core = core_from_payload_v1(value["core"])
    validate_card_v1(core, value["tail_calibration"])
    return core, deepcopy(value["tail_calibration"])


def query_risk_tail_nodes_v1(*, bundle, features, scenario_gap_bps, arm):
    if arm not in ARMS:
        raise ValueError("unknown risk study arm")
    core, card = bundle_from_payload_v1(bundle)
    result = _raw_nodes(core, features, scenario_gap_bps)
    result["uncalibrated_downside_q90_bps"] = result.downside_q90_bps
    known = result.status.isin(["ACCEPTABLE", "AVOID"])
    if arm == ARMS[1]:
        if card["calibration_status"] == "AVAILABLE":
            result.loc[known, "downside_q90_bps"] = np.clip(result.loc[known, "downside_q90_bps"]+card["delta"], 0., 10000.)
            result.loc[known, "status"] = np.where(result.loc[known, "expected_net_bps"].gt(0)
                & result.loc[known, "downside_q90_bps"].le(POLICY["risk_bps"]), "ACCEPTABLE", "AVOID")
        else:
            result.loc[known, "status"] = "UNKNOWN_RISK_CALIBRATION"
            result.loc[known, "downside_q90_bps"] = np.nan
    for name, value in dict(arm=arm, model_sha256=core.model_sha256 if arm == ARMS[0] else bundle["bundle_sha256"],
        base_model_sha256=core.model_sha256, calibration_sha256=card["calibration_sha256"], bundle_sha256=bundle["bundle_sha256"],
        schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256,
        risk_calibration_status=card["calibration_status"], risk_calibration_delta=card["delta"],
        effective_raw_cutoff=card["effective_raw_cutoff"], holding_sessions=5, deployable=False,
        effective_sample_size_is_time_independence=False, conditional_coverage_guaranteed=False).items():
        result[name] = value
    return result


def risk_tail_price_set_v1(*, bundle, d_features, source_context, reference_cny, legal_low_cny, legal_high_cny, tick_cny, arm):
    coordinates = (reference_cny, legal_low_cny, legal_high_cny, tick_cny)
    try:
        values = [Decimal(str(v)) for v in coordinates]
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("risk legal price coordinates need exact decimals") from exc
    if (any(isinstance(v, (bool, np.bool_)) for v in coordinates)
            or any(not v.is_finite() or v <= 0 or not np.isfinite(float(v)) for v in values)
            or set(d_features) != set(FEATURES) or set(source_context) != set(ROSTER_KEY)):
        raise ValueError("risk D-price features/identity/coordinates differ")
    reference, low, high, tick = values
    d, t = (pd.Timestamp(source_context[n]) for n in KEY[:2])
    if any(pd.isna(v) or v.tz is not None or v != v.normalize() for v in (d, t)) or not d < t or low > high:
        raise ValueError("risk D/T or legal price range differs")
    core, _ = bundle_from_payload_v1(bundle)
    calendar = sessions_v1(core.recipe["encoding"]["calendar"])
    if d in calendar and (calendar.get_loc(d)+1 >= len(calendar) or t != calendar[calendar.get_loc(d)+1]):
        raise ValueError("risk research target must be the original next market session")
    first, last = int((low/tick).to_integral_value(rounding=ROUND_CEILING)), int((high/tick).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("risk complete legal grid exceeds 100000 nodes")
    prices = [tick*n for n in range(first, last+1)]
    features = pd.DataFrame([{**d_features, **source_context} for _ in prices], columns=[*FEATURES, *ROSTER_KEY])
    nodes = query_risk_tail_nodes_v1(bundle=bundle, features=features,
        scenario_gap_bps=[float((p/reference-1)*10000) for p in prices], arm=arm)
    bands, left, right = [], None, None
    for price, state in zip(prices, nodes.status, strict=True):
        if state == "ACCEPTABLE":
            left, right = price if left is None else left, price
        elif left is not None:
            bands.append([str(left), str(right)])
            left, right = None, None
    if left is not None:
        bands.append([str(left), str(right)])
    counts = {str(k): int(v) for k, v in nodes.status.value_counts().items()}
    context = dict(source_context)
    context.update({n: pd.Timestamp(context[n]).date().isoformat() for n in KEY[:2]})
    quotes = [dict(price_cny=str(p), status=r["status"], **{n: None if pd.isna(r[n]) else float(r[n]) for n in
        ("expected_net_bps", "profit_probability", "uncalibrated_downside_q90_bps", "downside_q90_bps")})
        for p, r in zip(prices, nodes.to_dict("records"), strict=True)]
    return dict(arm=arm, source_context=context, intervals_cny=bands, nodes=quotes,
        original_tick_count=len(prices), status_counts=counts,
        complete_grid=True, empty_legal_grid=not prices, unknown_nodes=sum(v for k, v in counts.items() if k.startswith("UNKNOWN")),
        base_model_sha256=bundle["core"]["model_sha256"], calibration_sha256=bundle["tail_calibration"]["calibration_sha256"],
        bundle_sha256=bundle["bundle_sha256"], risk_calibration_delta=bundle["tail_calibration"]["delta"],
        effective_raw_cutoff=bundle["tail_calibration"]["effective_raw_cutoff"], risk_limit_bps=800,
        price_basis="D_ANCHORED_CNY", price_encoding="DECIMAL_STRING_CNY", holding_sessions=5,
        actual_fill_proven=False, conditional_coverage_guaranteed=False, deployable=False)
