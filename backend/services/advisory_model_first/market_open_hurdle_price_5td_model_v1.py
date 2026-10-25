"""Sign/Gamma/Beta hurdle and complete non-executing conditional price sets."""
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import math
import warnings

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_models_v1 import feature_values
from backend.services.advisory_model_first import original_slot_hurdle_price_5td_model_v1 as core
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_model_v1 import (
    beta_objective_gradient_v1, predict_values_v1,
)
from backend.services.advisory_model_first.market_open_hurdle_price_5td_contracts_v1 import (
    ARMS, CLOCK, COMPONENTS, CONDITION_KINDS, FEATURES, KEY, MARKET_COLUMN, MARKET_SUPPORT, MATRIX_ORDERS, PARAMETERS, POLICY_SHA256, ROSTER_KEY,
    SCHEMA_SHA256, VALUATION_POLICY_SHA256, VALUE_FIELDS, sha,
)


def market_basis_v1(gaps, market_gaps):
    g, m = (np.asarray(v, dtype=float) for v in (gaps, market_gaps))
    if (g.ndim != 1 or m.shape != g.shape or not np.isfinite(g).all()
            or not np.isfinite(m).all() or (g <= -10000).any() or (m <= -10000).any()):
        raise ValueError("positive stock and market scenario coordinates are required")
    with np.errstate(over="ignore", invalid="ignore"):
        values = np.column_stack((m/100, g*m/10000, np.abs(g-m)/100))
    if not np.isfinite(values).all():
        raise ValueError("market scenario basis is not finite")
    return values


def fit_encoding_v1(domain, intervals):
    base = core.fit_encoding_v1(domain, intervals)
    unique = domain.drop_duplicates("label_cluster")
    g, m = (unique[n].map(_number).astype(float).to_numpy() for n in ("observed_gap_bps", MARKET_COLUMN))
    known = np.isfinite(g) & np.isfinite(m)
    values = market_basis_v1(g[known], m[known])
    mean, scale = (values.mean(axis=0), values.std(axis=0)) if known.any() else (np.zeros(3), np.zeros(3))
    observed = unique.loc[np.isfinite(m), [KEY[0], MARKET_COLUMN]].drop_duplicates(KEY[0])
    if unique.groupby(KEY[0])[MARKET_COLUMN].nunique(dropna=False).gt(1).any():
        raise ValueError("market scenario differs across a decision date")
    counts = observed.groupby(np.floor(observed[MARKET_COLUMN].astype(float)/100).astype(int)).size()
    bins = sorted(int(k) for k, v in counts.items() if v >= MARKET_SUPPORT["minimum_days"])
    lo, hi = (float(observed[MARKET_COLUMN].min()), float(observed[MARKET_COLUMN].max())) if len(observed) else (None, None)
    return dict(core=base, market_mean=mean.tolist(), market_scale=scale.tolist(), market_buckets=bins,
        market_min_bps=lo, market_max_bps=hi, market_observed_days=len(observed),
        schema_sha256=SCHEMA_SHA256, market_support=MARKET_SUPPORT, information_clock=CLOCK,
        preprocessing_status="AVAILABLE" if known.any() and base["preprocessing_status"] == "AVAILABLE"
            else "UNAVAILABLE_NO_OBSERVED_TRAIN_PRICE")


def validate_encoding_v1(encoding):
    support = core.validate_encoding_v1(encoding["core"])
    mean, scale = (np.asarray(encoding[n], dtype=float) for n in ("market_mean", "market_scale"))
    if (mean.shape != (3,) or scale.shape != (3,) or not np.isfinite(mean).all()
            or not np.isfinite(scale).all() or (scale < 0).any()
            or encoding.get("schema_sha256") != SCHEMA_SHA256 or encoding.get("information_clock") != CLOCK
            or encoding.get("market_support") != MARKET_SUPPORT
            or encoding.get("preprocessing_status") not in ("AVAILABLE", "UNAVAILABLE_NO_OBSERVED_TRAIN_PRICE")
            or type(encoding.get("market_observed_days")) is not int or encoding["market_observed_days"] < 0
            or any(type(b) is not int for b in encoding["market_buckets"])
            or encoding["market_buckets"] != sorted(set(encoding["market_buckets"]))):
        raise ValueError("market preprocessing identity differs")
    lo, hi = encoding["market_min_bps"], encoding["market_max_bps"]
    if encoding["market_observed_days"]:
        if any(_number(v) is None or v <= -10000 for v in (lo, hi)) or lo > hi:
            raise ValueError("known training market range differs")
    elif lo is not None or hi is not None or encoding["market_buckets"]:
        raise ValueError("missing market range cannot be invented")
    return support


def matrix_v1(features, gaps, encoding, arm, market_gaps=None):
    if arm not in ARMS:
        raise ValueError("unknown market-open arm")
    validate_encoding_v1(encoding)
    values = core.matrix_v1(features, gaps, encoding["core"], core.ARMS[1])
    if arm == ARMS[0]:
        return values
    market = market_basis_v1(gaps, features[MARKET_COLUMN] if market_gaps is None else market_gaps)
    scale = np.asarray(encoding["market_scale"])
    z = (market-np.asarray(encoding["market_mean"]))/np.where(scale == 0, 1, scale)
    z[:, scale == 0] = 0
    return np.column_stack((values, z))


def supervision_v1(rows, encoding):
    validate_encoding_v1(encoding)
    # Shared calendar, original Top5 and cluster-mass validation precedes common market availability.
    selected, _, _ = core.supervision_v1(rows, dict(encoding["core"], clocks=encoding["clocks"]))
    m = selected[MARKET_COLUMN].map(_number).astype(float)
    if selected.groupby(KEY[0])[MARKET_COLUMN].nunique(dropna=False).gt(1).any():
        raise ValueError("market input contradicts across source packages/stocks")
    selected = selected.loc[np.isfinite(m)].copy()
    if not selected.empty and (m.loc[selected.index] <= -10000).any():
        raise ValueError("market price must be positive")
    y, w = (selected[n].map(_number).to_numpy(dtype=float) for n in ("net_return_bps", "cluster_mass"))
    if not (y > 0).any() or not (y < 0).any():
        raise ValueError("PREPARED_NO_FIT: common market-known positive and negative observations are required")
    return selected, y, w


def fit_component_v1(*, rows, encoding, arm, component, plan_sha256):
    """Exactly one sklearn.fit or one Beta optimizer call, no search/retry."""
    from scipy.optimize import minimize
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.linear_model import GammaRegressor, LogisticRegression
    if arm not in ARMS or component not in COMPONENTS:
        raise ValueError("unknown hurdle component")
    selected, y, w = supervision_v1(rows, encoding)
    x = matrix_v1(selected, selected.observed_gap_bps, encoding, arm)
    recipe = dict(arm=arm, component=component, plan_sha256=plan_sha256, schema_sha256=SCHEMA_SHA256,
        policy_sha256=POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256,
        encoding_sha256=sha(encoding), matrix_order=list(MATRIX_ORDERS[arm]), parameters=PARAMETERS[component],
        training_keys_sha256=sha(selected[list(ROSTER_KEY)].astype(str).to_dict("records")),
        unique_clusters=selected.label_cluster.nunique(), effective_cluster_mass=float(w.sum()),
        physical_fit_count=1, optimizer_fit_count=int(component == "negative"))
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", ConvergenceWarning)
            if component == "sign":
                estimator = LogisticRegression(**PARAMETERS["sign"]).fit(x, np.sign(y).astype(int), sample_weight=w)
                model = dict(classes=estimator.classes_.tolist(), coef=estimator.coef_.tolist(),
                    intercept=estimator.intercept_.tolist(), zero_class_status=(
                        "ZERO_CLASS_OBSERVED" if 0 in estimator.classes_ else "ZERO_CLASS_NOT_OBSERVED"))
            elif component == "positive":
                mask = y > 0
                estimator = GammaRegressor(**PARAMETERS["positive"]).fit(x[mask], y[mask]/100, sample_weight=w[mask])
                model = dict(coef=estimator.coef_.tolist(), intercept=float(estimator.intercept_))
            else:
                mask = y < 0
                z = -y[mask]/10000
                m = float(np.average(z, weights=w[mask]))
                initial = np.r_[math.log(m/(1-m)), np.zeros(x.shape[1]), math.log(10.)]
                bounds = [(None, None)]*(x.shape[1]+1)+[(math.log(.1), math.log(1000.))]
                result = minimize(beta_objective_gradient_v1, initial, args=(x[mask], z, w[mask]),
                    method="L-BFGS-B", jac=True, bounds=bounds,
                    options={n: PARAMETERS["negative"][n] for n in ("maxiter", "ftol", "gtol")})
                if (not result.success or not np.isfinite(result.x).all()
                        or min(abs(result.x[-1]-b) for b in bounds[-1]) <= 1e-7):
                    raise ValueError("MODEL_FIT_FAILED: Beta optimizer failed or precision hit its bounds")
                model = dict(coef=result.x[1:-1].tolist(), intercept=float(result.x[0]),
                    precision=float(math.exp(result.x[-1])), optimizer_iterations=int(result.nit))
    except (ValueError, ArithmeticError, ConvergenceWarning) as exc:
        raise ValueError(f"MODEL_FIT_FAILED:{component}:{type(exc).__name__}") from exc
    payload = dict(recipe=recipe, model=model)
    payload["component_sha256"] = sha(payload)
    validate_component_v1(payload, arm=arm, component=component, encoding=encoding, plan_sha256=plan_sha256)
    return payload


def validate_component_v1(payload, *, arm, component, encoding, plan_sha256):
    if (not isinstance(payload, dict) or set(payload) != {"recipe", "model", "component_sha256"}
            or payload["component_sha256"] != sha({n: v for n, v in payload.items() if n != "component_sha256"})):
        raise ValueError("hurdle component JSON identity differs")
    r, m = payload["recipe"], payload["model"]
    expected = dict(arm=arm, component=component, plan_sha256=plan_sha256, schema_sha256=SCHEMA_SHA256,
        policy_sha256=POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256,
        encoding_sha256=sha(encoding), matrix_order=list(MATRIX_ORDERS[arm]), parameters=PARAMETERS[component],
        physical_fit_count=1, optimizer_fit_count=int(component == "negative"))
    if any(r.get(n) != v for n, v in expected.items()):
        raise ValueError("hurdle component recipe/policy differs")
    import re
    if (not re.fullmatch(r"[a-f0-9]{64}", str(r.get("training_keys_sha256")))
            or type(r.get("unique_clusters")) is not int or r["unique_clusters"] <= 0
            or not math.isclose(r.get("effective_cluster_mass", float("nan")), r["unique_clusters"], abs_tol=1e-8)):
        raise ValueError("component training keys/cluster mass differs")
    width = len(MATRIX_ORDERS[arm])
    coef, intercept = np.asarray(m["coef"], dtype=float), np.asarray(m["intercept"], dtype=float)
    if not np.isfinite(coef).all() or not np.isfinite(intercept).all():
        raise ValueError("hurdle coefficient is non-finite")
    if component == "sign":
        classes = m["classes"]
        count = 1 if classes == [-1, 1] else 3 if classes == [-1, 0, 1] else 0
        if (not count or coef.shape != (count, width) or intercept.shape != (count,)
                or m["zero_class_status"] != ("ZERO_CLASS_OBSERVED" if count == 3 else "ZERO_CLASS_NOT_OBSERVED")):
            raise ValueError("hurdle sign/class mapping differs")
    elif coef.shape != (width,) or intercept.shape != ():
        raise ValueError("hurdle magnitude dimensions differ")
    elif component == "negative" and (not np.isfinite(m["precision"]) or not .1 < m["precision"] < 1000):
        raise ValueError("bounded Beta precision differs")
    return payload


def bundle_payload_v1(*, models, encoding, plan_sha256):
    validate_encoding_v1(encoding)
    if set(models) != set(ARMS) or any(set(models[a]) != set(COMPONENTS) for a in ARMS):
        raise ValueError("complete bundle needs all six statistical components")
    if encoding["preprocessing_status"] != "AVAILABLE":
        raise ValueError("PREPARED_NO_FIT: unknown training coordinates cannot produce a bundle")
    for arm in ARMS:
        for component in COMPONENTS:
            validate_component_v1(models[arm][component], arm=arm, component=component,
                encoding=encoding, plan_sha256=plan_sha256)
    if len({models[a][c]["recipe"]["training_keys_sha256"] for a in ARMS for c in COMPONENTS}) != 1:
        raise ValueError("all six heads must use the same original-slot supervision")
    payload = dict(schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256,
        valuation_policy_sha256=VALUATION_POLICY_SHA256, encoding=encoding, models=models, plan_sha256=plan_sha256,
        physical_fit_count=6, optimizer_fit_count=2, probabilities_calibrated=False, deployable=False)
    payload["bundle_sha256"] = sha(payload)
    return payload


def validate_bundle_v1(bundle):
    expected = bundle_payload_v1(models=bundle["models"], encoding=bundle["encoding"], plan_sha256=bundle["plan_sha256"])
    if bundle != expected:
        raise ValueError("hurdle bundle identity/content differs")
    return bundle



def query_hurdle_nodes_v1(*, bundle, features, scenario_gap_bps, scenario_market_gap_bps, market_condition_kind, arm):
    validate_bundle_v1(bundle)
    if (arm not in ARMS or market_condition_kind not in CONDITION_KINDS or len(features) > 500000
            or not set(ROSTER_KEY).issubset(features)):
        raise ValueError("hurdle query identity/arm/budget differs")
    _, stock = feature_values(features)
    gaps = np.asarray([_number(v) for v in scenario_gap_bps], dtype=float)
    market = np.asarray([_number(v) for v in scenario_market_gap_bps], dtype=float)
    if gaps.shape != (len(features),) or market.shape != gaps.shape:
        raise ValueError("hurdle query/scenario length differs")
    support = validate_encoding_v1(bundle["encoding"])
    valid = np.isfinite(gaps) & (gaps > -10000)
    supported = np.array([bool(ok and support.contains(g)) for g, ok in zip(gaps, valid, strict=True)])
    status = np.where(~stock, "UNKNOWN_STOCK_INPUT", np.where(~valid, "UNKNOWN_PRICE_SCENARIO",
        np.where(~supported, "UNKNOWN_GAP_SUPPORT", "KNOWN")))
    market_valid = np.isfinite(market) & (market > -10000)
    market_support = np.zeros(len(features), dtype=bool)
    e = bundle["encoding"]
    if e["market_observed_days"]:
        market_support[market_valid] = ((market[market_valid] >= e["market_min_bps"])
            & (market[market_valid] <= e["market_max_bps"])
            & np.isin(np.floor(market[market_valid]/100).astype(int), e["market_buckets"]))
    status = np.where(status != "KNOWN", status, np.where(~market_valid, "UNKNOWN_MARKET_OPEN",
        np.where(~market_support, "UNKNOWN_MARKET_SUPPORT", "KNOWN")))
    known = status == "KNOWN"
    values, raw = predict_values_v1(bundle["models"][arm], matrix_v1(features.loc[known], gaps[known], bundle["encoding"], arm, market_gaps=market[known]))
    result = features[list(ROSTER_KEY)].copy().reset_index(drop=True)
    result["status"] = status
    result[MARKET_COLUMN] = market
    result["market_condition_kind"] = market_condition_kind
    result["information_clock"] = CLOCK
    result["market_open_known_at_D"] = False
    result.loc[known, "status"] = np.where(values[:, -1] > 0, "ACCEPTABLE_VALUE_PREDICTED", "AVOID")
    for i, name in enumerate(VALUE_FIELDS):
        result[name] = np.nan
        result.loc[known, name] = values[:, i]
    result["raw_terminal_excess_loss_bps"] = np.nan
    result.loc[known, "raw_terminal_excess_loss_bps"] = raw
    result["numerical_status"] = np.where(known, "OK", "NOT_COMPUTED")
    result.loc[np.flatnonzero(known)[raw < 0], "numerical_status"] = "NUMERICAL_ROUNDOFF_ZERO"
    result["zero_class_status"] = bundle["models"][arm]["sign"]["model"]["zero_class_status"]
    for name, value in dict(arm=arm, schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256,
            valuation_policy_sha256=VALUATION_POLICY_SHA256, bundle_sha256=bundle["bundle_sha256"],
            probabilities_calibrated=False, actual_fill_proven=False).items():
        result[name] = value
    return result


def _decimal_coordinate(value):
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (bool, np.bool_)):
        raise ValueError("a boolean is not a nominal price coordinate")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("explicit Decimal nominal price coordinates are required") from exc
    if parsed.is_nan():
        return None
    if not parsed.is_finite() or parsed <= 0 or not np.isfinite(float(parsed)):
        raise ValueError("nominal price coordinates must be finite and positive")
    return parsed


def hurdle_price_set_v1(*, bundle, d_features, source_context, reference_cny, legal_low_cny, legal_high_cny, tick_cny, scenario_market_gap_bps, market_condition_kind, arm):
    reference, low, high, tick = [_decimal_coordinate(v) for v in (reference_cny, legal_low_cny, legal_high_cny, tick_cny)]
    if (low is not None and high is not None and low > high
            or set(d_features) != set(FEATURES) or set(source_context) != set(ROSTER_KEY)
            or arm not in ARMS or market_condition_kind not in CONDITION_KINDS):
        raise ValueError("hurdle D price/identity coordinates differ")
    d, t = (pd.Timestamp(source_context[n]) for n in KEY[:2])
    if any(pd.isna(v) or v.tz is not None or v != v.normalize() for v in (d, t)) or not d < t:
        raise ValueError("hurdle original D/T differs")
    validate_bundle_v1(bundle)
    from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_inputs_v1 import sessions_v1
    calendar = sessions_v1(bundle["encoding"]["clocks"]["calendar"])
    if d in calendar and (calendar.get_loc(d)+1 >= len(calendar) or calendar[calendar.get_loc(d)+1] != t):
        raise ValueError("hurdle T is not the original next session")
    if any(v is None for v in (low, high, tick)):
        return dict(status="UNKNOWN", reason="UNKNOWN_PRICE_SCENARIO", arm=arm,
            scenario_market_gap_bps=_number(scenario_market_gap_bps), information_clock=CLOCK,
            market_condition_kind=market_condition_kind, market_open_known_at_D=False,
            source_context={n: pd.Timestamp(v).date().isoformat() if n in KEY[:2] else v for n, v in source_context.items()},
            intervals_cny=[], nodes=[], status_counts={}, complete_grid=False, original_tick_count=None,
            unknown_nodes=None, bundle_sha256=bundle["bundle_sha256"], policy_sha256=POLICY_SHA256,
            schema_sha256=SCHEMA_SHA256, probabilities_calibrated=False, actual_fill_proven=False, deployable=False)
    first = int((low/tick).to_integral_value(rounding=ROUND_CEILING))
    last = int((high/tick).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("complete legal price grid exceeds 100000 ticks")
    nodes, bands, left, right = [], [], None, None
    for start in range(first, last+1, 128):
        prices = [tick*i for i in range(start, min(start+128, last+1))]
        frame = pd.DataFrame([{**d_features, **source_context} for _ in prices])
        output = query_hurdle_nodes_v1(bundle=bundle, features=frame,
            scenario_gap_bps=[None if reference is None else float((p/reference-1)*10000) for p in prices], scenario_market_gap_bps=[scenario_market_gap_bps]*len(prices),
            market_condition_kind=market_condition_kind, arm=arm)
        for price, row in zip(prices, output.to_dict("records"), strict=True):
            nodes.append(dict(price_cny=str(price), status=row["status"], numerical_status=row["numerical_status"],
                **{n: None if pd.isna(row[n]) else float(row[n]) for n in (*VALUE_FIELDS, "raw_terminal_excess_loss_bps")}))
            if row["status"] == "ACCEPTABLE_VALUE_PREDICTED":
                left, right = price if left is None else left, price
            elif left is not None:
                bands.append([str(left), str(right)])
                left, right = None, None
    if left is not None:
        bands.append([str(left), str(right)])
    counts = {s: sum(r["status"] == s for r in nodes) for s in sorted({r["status"] for r in nodes})}
    unknown = sum(v for k, v in counts.items() if k.startswith("UNKNOWN"))
    status = ("EMPTY_LEGAL_GRID" if not nodes else "UNKNOWN" if unknown == len(nodes)
        else "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" if not bands and unknown else "NO_ACCEPTABLE_PRICE" if not bands
        else "ACCEPTABLE_VALUE_PREDICTED")
    context = dict(source_context)
    context.update({n: pd.Timestamp(context[n]).date().isoformat() for n in KEY[:2]})
    return dict(status=status, arm=arm, source_context=context,
        scenario_market_gap_bps=_number(scenario_market_gap_bps), information_clock=CLOCK,
        market_condition_kind=market_condition_kind, market_open_known_at_D=False, intervals_cny=bands, nodes=nodes, status_counts=counts,
        original_tick_count=len(nodes), complete_grid=True, unknown_nodes=unknown,
        bundle_sha256=bundle["bundle_sha256"], policy_sha256=POLICY_SHA256, schema_sha256=SCHEMA_SHA256,
        risk_object="TERMINAL_EXCESS_LOSS", terminal_threshold_bps=800, probabilities_calibrated=False,
        price_basis="D_ANCHORED_CNY", price_encoding="DECIMAL_STRING_CNY", holding_sessions=5,
        path_loss_guaranteed=False, actual_fill_proven=False, realized_return_bps=None, deployable=False)


def hurdle_price_sets_v1(*, bundle, requests):
    """Shared batch tick cap; no daily workspace rebuild or cross-product labels."""
    output, count = [], 0
    for request in requests:
        # Check this request's size before allocating its full grid.
        low, high, tick = (_decimal_coordinate(request[n]) for n in ("legal_low_cny", "legal_high_cny", "tick_cny"))
        size = 0 if any(v is None for v in (low, high, tick)) else max(0, int((high/tick).to_integral_value(rounding=ROUND_FLOOR))
            - int((low/tick).to_integral_value(rounding=ROUND_CEILING))+1)
        if count+size > 500000:
            raise ValueError("complete batch price grid exceeds 500000 ticks")
        item = hurdle_price_set_v1(bundle=bundle, **request)
        output.append(item)
        count += len(item["nodes"])
    return output
