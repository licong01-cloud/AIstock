"""Matched 26 / candidate 29 dimensions and an explicit conditional price set."""
from decimal import ROUND_CEILING, ROUND_FLOOR
import math
import warnings

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_models_v1 import _support, feature_values
from backend.services.advisory_model_first import original_slot_hurdle_price_5td_model_v1 as core
from backend.services.advisory_model_first.market_open_hurdle_price_5td_model_v1 import market_basis_v1
from backend.services.advisory_model_first.sector_relative_open_price_5td_contracts_v1 import (
    ARMS, CLOCK, COMPONENTS, CONDITION_KINDS, CONTEXT_COLUMNS, FEATURES, KEY,
    MATRIX_ORDERS, PARAMETERS, PEER_POLICY, POLICY_SHA256, ROSTER_KEY,
    SCHEMA_SHA256, VALUATION_POLICY_SHA256, VALUE_FIELDS, sha,
)


def _context_basis(gaps, market, peers):
    g, m = (np.asarray(v, dtype=float) for v in (gaps, market))
    b, w, q = (peers[n].map(_number).to_numpy(dtype=float) for n in CONTEXT_COLUMNS[1:])
    if any(v.shape != g.shape for v in (m, b, w, q)) or not all(np.isfinite(v).all() for v in (g, m, b, w, q)):
        raise ValueError("known context coordinates and lengths must be finite")
    if ((b <= -10000).any() or ((w < 0) | (w > 1)).any() or ((q < 0) | (q > 1)).any()):
        raise ValueError("peer price, breadth or coverage contradicts its domain")
    return np.column_stack((market_basis_v1(g, m), q, b/100, np.abs(g-b)/100, 2*w-1))


def _context_known(rows):
    raw = rows.loc[:, CONTEXT_COLUMNS].map(_number).to_numpy(dtype=float)
    for i in (2, 3):
        known = np.isfinite(raw[:, i])
        if ((raw[known, i] < 0) | (raw[known, i] > 1)).any():
            raise ValueError("known breadth/coverage is outside [0,1]")
    for i in (0, 1):
        if (raw[np.isfinite(raw[:, i]), i] <= -10000).any():
            raise ValueError("context must represent a positive nominal price")
    return np.isfinite(raw).all(axis=1)


def fit_encoding_v1(domain, clocks):
    if not domain[KEY[0]].between(pd.Timestamp(clocks["train_start"]), pd.Timestamp(clocks["train_end"])).all():
        raise ValueError("evaluation input cannot set training encoding")
    unique = domain.drop_duplicates("label_cluster")
    intervals = _support(unique).intervals_bps
    base = core.fit_encoding_v1(unique, intervals)
    g = unique.observed_gap_bps.map(_number).to_numpy(dtype=float)
    valid = _context_known(unique) & np.isfinite(g)
    values = _context_basis(g[valid], unique.loc[valid, CONTEXT_COLUMNS[0]], unique.loc[valid])
    mean, scale = (values.mean(axis=0), values.std(axis=0)) if len(values) else (np.zeros(7), np.zeros(7))
    support = {}
    for name in CONTEXT_COLUMNS[:2]:
        observed = unique.loc[unique[name].notna(), [KEY[0], name]].copy()
        counts = observed.groupby(np.floor(observed[name]/100).astype(int))[KEY[0]].nunique()
        support[name] = dict(min_bps=float(observed[name].min()) if len(observed) else None,
            max_bps=float(observed[name].max()) if len(observed) else None,
            buckets=sorted(int(k) for k, v in counts.items() if v >= PEER_POLICY["minimum_days"]))
    if unique.groupby(KEY[0])[CONTEXT_COLUMNS[0]].nunique(dropna=False).gt(1).any():
        raise ValueError("market condition conflicts within a decision date")
    return dict(core=base, context_mean=mean.tolist(), context_scale=scale.tolist(), context_support=support,
        clocks=clocks, schema_sha256=SCHEMA_SHA256, information_clock=CLOCK,
        preprocessing_status="AVAILABLE" if valid.any() else "UNAVAILABLE_NO_COMMON_INPUT",
        preprocessing_unique_clusters=len(unique), label_based_preprocessing=False)


def validate_encoding_v1(encoding):
    support = core.validate_encoding_v1(encoding["core"])
    mean, scale = (np.asarray(encoding[n], dtype=float) for n in ("context_mean", "context_scale"))
    if (mean.shape != (7,) or scale.shape != (7,) or not np.isfinite(mean).all() or not np.isfinite(scale).all()
            or (scale < 0).any() or encoding.get("schema_sha256") != SCHEMA_SHA256
            or encoding.get("information_clock") != CLOCK or encoding.get("label_based_preprocessing") is not False
            or encoding.get("preprocessing_status") not in ("AVAILABLE", "UNAVAILABLE_NO_COMMON_INPUT")
            or set(encoding["context_support"]) != set(CONTEXT_COLUMNS[:2])):
        raise ValueError("sector-open encoding identity/dimensions differ")
    for s in encoding["context_support"].values():
        lo, hi, bins = s["min_bps"], s["max_bps"], s["buckets"]
        if (bins != sorted(set(bins)) or any(type(b) is not int for b in bins)
                or (lo is None) != (hi is None) or lo is None and bins
                or lo is not None and (_number(lo) <= -10000 or _number(hi) < lo)):
            raise ValueError("training context range/buckets differ")
    return support


def matrix_v1(rows, gaps, encoding, arm, market=None, peers=None):
    if arm not in ARMS:
        raise ValueError("unknown sector-open model arm")
    validate_encoding_v1(encoding)
    base = core.matrix_v1(rows, gaps, encoding["core"], core.ARMS[1])
    context = _context_basis(gaps, rows[CONTEXT_COLUMNS[0]] if market is None else market, rows if peers is None else peers)
    scale = np.asarray(encoding["context_scale"])
    z = (context-np.asarray(encoding["context_mean"]))/np.where(scale == 0, 1, scale)
    z[:, scale == 0] = 0
    return np.column_stack((base, z[:, :4] if arm == ARMS[0] else z))


def supervision_v1(rows, encoding):
    validate_encoding_v1(encoding)
    clocks = encoding["clocks"]
    calendar = pd.to_datetime(clocks["calendar"])
    if (len(rows) > 3050 or not rows.columns.is_unique or rows.duplicated(list(ROSTER_KEY)).any()
            or not rows.selection_effective_rank.between(1, 5).all()
            or not rows[KEY[0]].between(pd.Timestamp(clocks["train_start"]), pd.Timestamp(clocks["train_end"])).all()):
        raise ValueError("training must retain only original Top5 development inputs")
    for d, t, h in rows[[*KEY[:2], "label_information_end"]].itertuples(index=False, name=None):
        i = calendar.get_loc(d)
        if i+5 >= len(calendar) or t != calendar[i+1] or h != calendar[i+5]:
            raise ValueError("training horizon must be exactly T through T+4")
    fields = [*FEATURES, *CONTEXT_COLUMNS, "reference_cny", "observed_gap_bps", "net_return_bps", "valuation_status"]
    if rows.groupby("label_cluster")[fields].nunique(dropna=False).gt(1).any().any():
        raise ValueError("duplicate economic clusters have contradictory inputs or labels")
    _, stock = feature_values(rows)
    g = rows.observed_gap_bps.map(_number).to_numpy(dtype=float)
    mask = stock & _context_known(rows) & np.isfinite(g) & rows.valuation_status.eq("AVAILABLE").to_numpy()
    mask &= rows.label_information_end.le(pd.Timestamp(clocks["train_end"])).to_numpy()
    chosen = rows.loc[mask].copy()
    y, w = (chosen[n].map(_number).to_numpy(dtype=float) for n in ("net_return_bps", "cluster_mass"))
    if (not np.isfinite(y).all() or (y <= -10000).any() or not np.isfinite(w).all() or (w <= 0).any()
            or not np.allclose(chosen.groupby("label_cluster").cluster_mass.sum(), 1., atol=1e-12, rtol=0)):
        raise ValueError("shared supervision or cluster mass differs")
    if not (y > 0).any() or not (y < 0).any():
        raise ValueError("PREPARED_NO_FIT: common positive and negative supervision required")
    return chosen, y, w


def fit_component_v1(*, rows, encoding, arm, component, plan_sha256, before_fit):
    """One existing learner/optimizer, never hyperparameter or seed search."""
    from scipy.optimize import minimize
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.linear_model import GammaRegressor, LogisticRegression
    if arm not in ARMS or component not in COMPONENTS or not callable(before_fit):
        raise ValueError("explicit statistical component and fit journal callback required")
    selected, y, w = supervision_v1(rows, encoding)
    x = matrix_v1(selected, selected.observed_gap_bps, encoding, arm)
    recipe = dict(arm=arm, component=component, plan_sha256=plan_sha256, schema_sha256=SCHEMA_SHA256,
        policy_sha256=POLICY_SHA256, encoding_sha256=sha(encoding), matrix_order=list(MATRIX_ORDERS[arm]),
        parameters=PARAMETERS[component], training_keys_sha256=sha(selected[list(ROSTER_KEY)].astype(str).to_dict("records")),
        training_targets_sha256=sha([y.tolist(), w.tolist()]), unique_clusters=selected.label_cluster.nunique(),
        effective_cluster_mass=float(w.sum()), physical_fit_count=1)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ConvergenceWarning)
        if component == "sign":
            estimator = LogisticRegression(**PARAMETERS[component])
            before_fit()
            estimator.fit(x, np.sign(y).astype(int), sample_weight=w)
            model = dict(classes=estimator.classes_.tolist(), coef=estimator.coef_.tolist(), intercept=estimator.intercept_.tolist(),
                zero_class_status="ZERO_CLASS_OBSERVED" if 0 in estimator.classes_ else "ZERO_CLASS_NOT_OBSERVED")
        elif component == "positive":
            mask = y > 0
            estimator = GammaRegressor(**PARAMETERS[component])
            before_fit()
            estimator.fit(x[mask], y[mask]/100, sample_weight=w[mask])
            model = dict(coef=estimator.coef_.tolist(), intercept=float(estimator.intercept_))
        else:
            mask = y < 0
            z = -y[mask]/10000
            m = float(np.average(z, weights=w[mask]))
            initial = np.r_[math.log(m/(1-m)), np.zeros(x.shape[1]), math.log(10.)]
            bounds = [(None, None)]*(x.shape[1]+1)+[(math.log(.1), math.log(1000.))]
            before_fit()
            result = minimize(core.beta_objective_gradient_v1, initial, args=(x[mask], z, w[mask]),
                method="L-BFGS-B", jac=True, bounds=bounds,
                options={n: PARAMETERS[component][n] for n in ("maxiter", "ftol", "gtol")})
            if (not result.success or not np.isfinite(result.x).all()
                    or min(abs(result.x[-1]-b) for b in bounds[-1]) <= 1e-7):
                raise ValueError("MODEL_FIT_FAILED: Beta optimizer failed or precision reached boundary")
            model = dict(coef=result.x[1:-1].tolist(), intercept=float(result.x[0]),
                precision=float(math.exp(result.x[-1])), optimizer_iterations=int(result.nit))
    payload = dict(recipe=recipe, model=model)
    payload["component_sha256"] = sha(payload)
    return validate_component_v1(payload, encoding=encoding, arm=arm, component=component, plan_sha256=plan_sha256)


def validate_component_v1(payload, *, encoding, arm, component, plan_sha256):
    if (set(payload) != {"recipe", "model", "component_sha256"}
            or payload["component_sha256"] != sha({k: v for k, v in payload.items() if k != "component_sha256"})):
        raise ValueError("component JSON hash differs")
    r, m = payload["recipe"], payload["model"]
    expected = dict(arm=arm, component=component, plan_sha256=plan_sha256, schema_sha256=SCHEMA_SHA256,
        policy_sha256=POLICY_SHA256, encoding_sha256=sha(encoding), matrix_order=list(MATRIX_ORDERS[arm]),
        parameters=PARAMETERS[component], physical_fit_count=1)
    if any(r.get(k) != v for k, v in expected.items()) or r.get("unique_clusters", 0) <= 0 or not math.isclose(
            r.get("effective_cluster_mass", float("nan")), r["unique_clusters"], abs_tol=1e-8):
        raise ValueError("component recipe or cluster identity differs")
    width = len(MATRIX_ORDERS[arm])
    coef, intercept = np.asarray(m["coef"], dtype=float), np.asarray(m["intercept"], dtype=float)
    if not np.isfinite(coef).all() or not np.isfinite(intercept).all():
        raise ValueError("non-finite model coefficients")
    if component == "sign":
        count = 1 if m["classes"] == [-1, 1] else 3 if m["classes"] == [-1, 0, 1] else 0
        if not count or coef.shape != (count, width) or intercept.shape != (count,) or m["zero_class_status"] != (
                "ZERO_CLASS_OBSERVED" if count == 3 else "ZERO_CLASS_NOT_OBSERVED"):
            raise ValueError("sign class/dimension identity differs")
    elif coef.shape != (width,) or intercept.shape != ():
        raise ValueError("magnitude dimensions differ")
    elif component == "negative" and not .1 < _number(m["precision"]) < 1000:
        raise ValueError("Beta precision is outside frozen bounds")
    return payload


def bundle_payload_v1(*, models, encoding, plan_sha256):
    validate_encoding_v1(encoding)
    if set(models) != set(ARMS) or any(set(models[a]) != set(COMPONENTS) for a in ARMS):
        raise ValueError("bundle requires exactly six components")
    for arm in ARMS:
        for c in COMPONENTS:
            validate_component_v1(models[arm][c], encoding=encoding, arm=arm, component=c, plan_sha256=plan_sha256)
    for field in ("training_keys_sha256", "training_targets_sha256"):
        if len({models[a][c]["recipe"][field] for a in ARMS for c in COMPONENTS}) != 1:
            raise ValueError("six components must use identical common supervision")
    body = dict(models=models, encoding=encoding, plan_sha256=plan_sha256, schema_sha256=SCHEMA_SHA256,
        policy_sha256=POLICY_SHA256, physical_fit_count=6, calibration_status="UNCALIBRATED", deployable=False)
    return dict(body, bundle_sha256=sha(body))


def validate_bundle_v1(bundle):
    if bundle != bundle_payload_v1(models=bundle["models"], encoding=bundle["encoding"], plan_sha256=bundle["plan_sha256"]):
        raise ValueError("bundle identity/content differs")
    return bundle


def query_sector_relative_open_nodes_v1(*, bundle, features, price_cny, reference_cny, market_condition, peer_condition, condition_kind, arm):
    validate_bundle_v1(bundle)
    if (arm not in ARMS or condition_kind not in CONDITION_KINDS or len(features) > 500000
            or not set(ROSTER_KEY).issubset(features) or not features.columns.is_unique):
        raise ValueError("query arm, condition clock or original identities differ")
    calendar = pd.to_datetime(bundle["encoding"]["clocks"]["calendar"])
    if features[list(ROSTER_KEY)].isna().any().any():
        raise ValueError("query source identity cannot be unknown")
    for d, t in features[list(KEY[:2])].drop_duplicates().itertuples(index=False, name=None):
        d, t = pd.Timestamp(d), pd.Timestamp(t)
        if (any(pd.isna(v) or v.tz is not None or v != v.normalize() for v in (d, t)) or d >= t
                or d in calendar and (calendar.get_loc(d)+1 >= len(calendar) or calendar[calendar.get_loc(d)+1] != t)):
            raise ValueError("query D/T calendar identity differs")
    arrays = [np.asarray([_number(v, positive=True) for v in values], dtype=float) for values in (price_cny, reference_cny)]
    price, reference = arrays
    market = np.asarray([_number(v) for v in market_condition], dtype=float)
    if any(a.shape != (len(features),) for a in (*arrays, market)) or len(peer_condition) != len(features):
        raise ValueError("conditional query lengths differ")
    peer_condition = peer_condition.reset_index(drop=True).copy()
    peer_condition[CONTEXT_COLUMNS[0]] = market
    common = _context_known(peer_condition)
    g = 10000*(price/reference-1)
    _, stock = feature_values(features)
    support = validate_encoding_v1(bundle["encoding"])
    gap_known = np.isfinite(g)
    supported = np.array([bool(ok and support.contains(float(v))) for v, ok in zip(g, gap_known, strict=True)])
    status = np.where(~stock, "UNKNOWN_STOCK_INPUT", np.where(~gap_known, "UNKNOWN_PRICE_SCENARIO",
        np.where(~supported, "UNKNOWN_GAP_SUPPORT", np.where(~np.isfinite(market), "UNKNOWN_MARKET_OPEN",
        np.where(~common, "UNKNOWN_PEER_SUPPORT", "KNOWN")))))
    for name, reason in zip(CONTEXT_COLUMNS[:2], ("UNKNOWN_MARKET_SUPPORT", "UNKNOWN_PEER_RANGE"), strict=True):
        v = peer_condition[name].map(_number).to_numpy(dtype=float)
        s = bundle["encoding"]["context_support"][name]
        known = np.isfinite(v)
        fits = np.zeros(len(v), dtype=bool)
        if s["min_bps"] is not None:
            fits[known] = ((v[known] >= s["min_bps"]) & (v[known] <= s["max_bps"])
                & np.isin(np.floor(v[known]/100).astype(int), s["buckets"]))
        status = np.where((status == "KNOWN") & ~fits, reason, status)
    known = status == "KNOWN"
    values, raw = core.predict_values_v1(bundle["models"][arm], matrix_v1(features.loc[known], g[known], bundle["encoding"], arm,
        market=market[known], peers=peer_condition.loc[known]))
    result = features.loc[:, ROSTER_KEY].reset_index(drop=True).copy()
    result["status"] = status
    result.loc[known, "status"] = np.where((values[:, -1] > 0) & (values[:, -4] > 0), "ACCEPTABLE_VALUE_PREDICTED", "AVOID")
    result["action"] = np.where(result.status.eq("ACCEPTABLE_VALUE_PREDICTED"), "TAKE", np.where(result.status.eq("AVOID"), "SKIP", "NO_ADVICE"))
    for i, name in enumerate(VALUE_FIELDS):
        result[name] = np.nan
        result.loc[known, name] = values[:, i]
    result["raw_terminal_excess_loss_bps"] = np.nan
    result.loc[known, "raw_terminal_excess_loss_bps"] = raw
    result["observed_gap_bps"] = g
    for name in CONTEXT_COLUMNS:
        result[name] = peer_condition[name].to_numpy()
    for name, value in dict(arm=arm, condition_kind=condition_kind, information_clock=CLOCK,
            context_known_at_D=False, schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256,
            valuation_policy_sha256=VALUATION_POLICY_SHA256, bundle_sha256=bundle["bundle_sha256"],
            calibration_status="UNCALIBRATED", actual_fill_proven=False, deployable=False).items():
        result[name] = value
    return result


def sector_relative_open_price_set_v1(*, bundle, d_features, source_context, reference_cny, market_condition, peer_condition,
                                     condition_kind, legal_low_cny, legal_high_cny, tick_cny, arm):
    coordinates = [core._decimal_coordinate(v) for v in (reference_cny, legal_low_cny, legal_high_cny, tick_cny)]
    reference, low, high, tick = coordinates
    if (set(d_features) != set(FEATURES) or set(source_context) != set(ROSTER_KEY)
            or set(peer_condition) != set(CONTEXT_COLUMNS[1:]) or arm not in ARMS or condition_kind not in CONDITION_KINDS
            or low is not None and high is not None and low > high):
        raise ValueError("price function coordinates, identity or explicit conditions differ")
    validate_bundle_v1(bundle)
    d, t = (pd.Timestamp(source_context[n]) for n in KEY[:2])
    calendar = pd.to_datetime(bundle["encoding"]["clocks"]["calendar"])
    if (any(pd.isna(v) or v.tz is not None or v != v.normalize() for v in (d, t)) or d >= t
            or d in calendar and (calendar.get_loc(d)+1 >= len(calendar) or calendar[calendar.get_loc(d)+1] != t)):
        raise ValueError("original D/T calendar identity differs")
    base = dict(arm=arm, condition_kind=condition_kind, information_clock=CLOCK, source_context={k: str(pd.Timestamp(v).date())
        if k in KEY[:2] else v for k, v in source_context.items()}, market_condition=_number(market_condition),
        peer_condition={k: _number(v) for k, v in peer_condition.items()},
        bundle_sha256=bundle["bundle_sha256"], policy_sha256=POLICY_SHA256,
        schema_sha256=SCHEMA_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256,
        calibration_status="UNCALIBRATED", holding_sessions=5, price_encoding="DECIMAL_STRING_CNY",
        actual_fill_proven=False, deployable=False)
    if any(v is None for v in (low, high, tick)):
        return dict(base, status="UNKNOWN", intervals_cny=[], nodes=[], complete_grid=False, unknown_nodes=None)
    first, last = int((low/tick).to_integral_value(rounding=ROUND_CEILING)), int((high/tick).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("complete legal tick grid exceeds its existing 100000-node budget")
    nodes, intervals, left, right = [], [], None, None
    for start in range(first, last+1, 128):
        prices = [tick*i for i in range(start, min(start+128, last+1))]
        result = query_sector_relative_open_nodes_v1(bundle=bundle,
            features=pd.DataFrame([{**source_context, **d_features} for _ in prices]),
            price_cny=prices, reference_cny=[reference]*len(prices), market_condition=[market_condition]*len(prices),
            peer_condition=pd.DataFrame([peer_condition]*len(prices)), condition_kind=condition_kind, arm=arm)
        for p, row in zip(prices, result.to_dict("records"), strict=True):
            nodes.append(dict(price_cny=str(p), status=row["status"], action=row["action"],
                **{k: None if pd.isna(row[k]) else float(row[k]) for k in VALUE_FIELDS}))
            if row["action"] == "TAKE":
                left, right = p if left is None else left, p
            elif left is not None:
                intervals.append([str(left), str(right)])
                left, right = None, None
    if left is not None:
        intervals.append([str(left), str(right)])
    unknown = sum(n["action"] == "NO_ADVICE" for n in nodes)
    status = ("EMPTY_LEGAL_GRID" if not nodes else "UNKNOWN" if unknown == len(nodes) else
        "ACCEPTABLE_VALUE_PREDICTED" if intervals else "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" if unknown else "NO_ACCEPTABLE_PRICE")
    return dict(base, status=status, intervals_cny=intervals, nodes=nodes, complete_grid=True,
        original_tick_count=len(nodes), unknown_nodes=unknown)
