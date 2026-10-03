"""Observed T conditions, then three-arm settlement; never confirmation evidence."""
import json
from decimal import Decimal

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_aligned_evaluation import (
    fixed_block_interval_v3, shadow_endpoint_execution_audit_v3, shadow_portfolio_intervention_support_v3,
)
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1
from backend.services.advisory_model_first.economic_entry_evaluation import matched_entry_priorities
from backend.services.advisory_model_first.economic_entry_information_evaluation import _attribution
from backend.services.advisory_model_first.economic_entry_information_source import _numeric
from backend.services.advisory_model_first.economic_entry_labels import KEY, _fail, _frame, _positive
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_entry_timing_contracts_v1 import CANDIDATE_NAMES
from backend.services.advisory_model_first.economic_entry_timing_inference_v1 import predict_timing_entry_nodes_v1
from backend.services.advisory_model_first.economic_entry_timing_pipeline_v1 import (
    load_fitted_timing_v1, load_timing_study_v1, register_timing_stage, timing_inputs, verify_timing_ledger,
)
from backend.services.advisory_model_first.economic_entry_timing_training_v1 import checked_timing_inputs, timing_input_rows_sha256
from backend.services.advisory_model_first.policy_contracts import transition_policy_from_payload
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio


def timing_descriptive_mde_v1(values):
    """Fixed block SE proxy, not observed power or confirmation qualification."""
    values = np.asarray(values, dtype=float)
    if values.ndim != 1 or not len(values) or not np.isfinite(values).all():
        _fail("timing MDE needs a nonempty finite common-day increment")
    random = np.random.default_rng(20261002)
    width = min(5, len(values))
    means = np.empty(2000)
    for index in range(len(means)):
        starts = random.integers(0, len(values)-width+1, size=int(np.ceil(len(values)/width)))
        means[index] = np.concatenate([values[start:start+width] for start in starts])[:len(values)].mean()
    standard_error = float(means.std(ddof=1))
    usable = len(values) >= 10 and standard_error > 0
    return {"common_day_count": len(values), "block_days": 5, "repetitions": 2000, "seed": 20261002,
        "block_standard_error_bps": standard_error, "nominal_two_sided_alpha": .05, "nominal_power": .8,
        "normal_approximation_mde_bps": 2.801585218112967*standard_error if usable else None,
        "status": "DESCRIPTIVE_PROXY" if usable else "UNPROVEN_SHORT_OR_DEGENERATE_SERIES",
        "interpretation": "POST_EVALUATION_VARIANCE_PROXY_NOT_ACHIEVED_POWER_OR_ACTIVATION_EVIDENCE"}


def query_timing_actual_opens_v1(*, fitted, identity, candidates, inputs, references, prices):
    identity = EconomicEntryInputIdentityV1.model_validate(identity.model_dump())
    source = fitted.request.parent_request.source_request
    if (source.input_identity_sha256 != identity.identity_sha256 or timing_input_rows_sha256(inputs) != fitted.request.input_rows_sha256
            or max(len(candidates), len(inputs), len(references)) > source.resource_max_rows or len(prices) > 500000):
        _fail("timing observed condition source/budget differs")
    roster = _frame(candidates.loc[:, KEY+["selection_effective_rank"]], KEY, set(KEY)|{"selection_effective_rank"})
    ranks = roster.selection_effective_rank
    if (not ranks.map(lambda value: isinstance(value, (int, np.integer)) and not isinstance(value, (bool, np.bool_))).all()
            or not ranks.between(1, 20).all() or roster.duplicated([KEY[0], "selection_effective_rank"]).any()
            or roster.duplicated([KEY[0], "instrument"]).any()):
        _fail("timing observed original Top20 differs")
    feature_map = checked_timing_inputs(inputs).set_index(KEY).to_dict("index")
    if not set(roster[KEY].itertuples(index=False, name=None)).issubset(feature_map):
        _fail("timing observed roster contains a foreign frozen candidate")
    ref_fields = [*KEY, "target_reference_raw_cny", "reference_visible_through", "source_sha256"]
    refs = _frame(references.loc[:, ref_fields], KEY, set(ref_fields))
    if not refs.source_sha256.eq(identity.reference_source_sha256).all():
        _fail("timing D reference identity differs")
    refs["target_reference_raw_cny"] = _numeric(refs.target_reference_raw_cny, positive=True)
    ref_map = refs.set_index(KEY).to_dict("index")
    # Projection excludes all future OHLC/episode/label columns, even though the
    # settlement snapshot passed by the caller also contains future valuations.
    quote_fields = ["trade_date", "instrument", "raw_open_cny", "suspended", "tradability_unknown",
        "up_limit", "down_limit", "source_sha256", "price_coordinate_sha256"]
    observed = _frame(prices.loc[:, quote_fields], ["trade_date", "instrument"], set(quote_fields))
    if (not observed.source_sha256.eq(identity.price_source_sha256).all()
            or not observed.price_coordinate_sha256.eq(identity.price_coordinate_sha256).all()
            or any(not observed[name].map(lambda value: type(value) is bool).all() for name in ("suspended", "tradability_unknown"))):
        _fail("timing observed trading/price identity differs")
    for name in ("raw_open_cny", "up_limit", "down_limit"):
        observed[name] = _numeric(observed[name], positive=True)
    quote_map = observed.set_index(["trade_date", "instrument"]).to_dict("index")
    rows, matrices, positions = [], [], []
    for candidate in roster.to_dict("records"):
        decision, target, symbol = (candidate[name] for name in KEY)
        if not source.test_start <= decision.date() <= source.test_end or not decision < target:
            _fail("timing observation leaves frozen consumed test window")
        key = decision, target, symbol
        reference, feature, quote = ref_map.get(key), feature_map.get(key), quote_map.get((target, symbol))
        row = {**candidate, "model_action": "UNAVAILABLE", "reason_code": "SOURCE_UNAVAILABLE",
            "actual_gap_bps": None, "expected_net_return_bps": None, "entry_net_max_loss_q90_bps": None}
        if reference is None or quote is None:
            rows.append(row)
            continue
        if pd.Timestamp(reference["reference_visible_through"]) != decision:
            _fail("timing observed reference must be exactly D visible")
        if quote["tradability_unknown"]:
            row["reason_code"] = "OPEN_TRADABILITY_UNKNOWN"
        elif quote["suspended"]:
            row.update(model_action="NOT_APPLICABLE", reason_code="SUSPENDED")
        else:
            actual, anchor, upper, lower = (_positive(quote["raw_open_cny"]), _positive(reference["target_reference_raw_cny"]),
                _positive(quote["up_limit"]), _positive(quote["down_limit"]))
            if None not in (actual, upper, lower) and (lower > upper or actual < lower-1e-9 or actual > upper+1e-9):
                _fail("timing observed raw quote contradicts legal limits")
            if None in (actual, anchor, upper, lower) or actual >= upper-1e-9:
                row["reason_code"] = "OPEN_LIMIT_OR_EXECUTABILITY_UNPROVEN"
            elif Decimal(str(actual)) % Decimal(".01"):
                row["reason_code"] = "OBSERVED_OPEN_OFF_EQUITY_TICK"
            else:
                gap = (actual/anchor-1)*10000
                row["actual_gap_bps"] = gap
                if feature is not None:
                    positions.append(len(rows))
                    matrices.append({name: gap if name == "query_gap_bps" else feature[name] for name in CANDIDATE_NAMES})
        rows.append(row)
    if matrices:
        predicted = predict_timing_entry_nodes_v1(fitted=fitted, matrix=pd.DataFrame(matrices))
        for position, values in zip(positions, predicted.to_dict("records"), strict=True):
            rows[position].update(values)
    return pd.DataFrame(rows)


def evaluate_timing_study_v1(*, plan_path, output_root):
    plan, root, registered = load_timing_study_v1(plan_path=plan_path, output_root=output_root)
    loaded, inputs = timing_inputs(plan)
    trained = read_stage(root/"trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
    verify_timing_ledger(plan, root, "TRAINED")
    if (root/"evaluated").exists():
        read_stage(root/"evaluated", stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
        register_timing_stage(plan, loaded, root, "EVALUATED", root/"evaluated/manifest.json", generated=2, evaluated=2)
        return root/"evaluated"
    arms = load_fitted_timing_v1(plan_path=plan_path, output_root=output_root)
    parent_root = loaded[2]
    identity = EconomicEntryInputIdentityV1.model_validate_json((parent_root/"prepared/identity.json").read_text(encoding="utf-8"))
    source = plan.training_request.parent_request.source_request
    ranks = pd.read_parquet(parent_root/"prepared/frozen_rankings.parquet")
    candidates = ranks.loc[ranks.is_candidate_decision & ranks.selection_effective_rank.le(20)
        & ranks[KEY[0]].between(pd.Timestamp(source.test_start), pd.Timestamp(source.test_end))].copy()
    prices, references = pd.read_parquet(parent_root/"prepared/prices.parquet"), pd.read_parquet(parent_root/"prepared/references.parquet")
    decisions = {arm: query_timing_actual_opens_v1(fitted=value, identity=identity, candidates=candidates,
        inputs=inputs, references=references, prices=prices) for arm, value in arms.items()}
    market = prices.rename(columns={"trade_date": "datetime"}).copy()
    for field in ("open", "high", "low", "close"):
        market[field] = market[f"raw_{field}_cny"]*market.policy_price_per_raw_cny
    market["factor"], market["limit_up"], market["limit_down"] = market.policy_price_per_raw_cny, 1., 1.
    market["up_limit_price"], market["down_limit_price"] = market.up_limit, market.down_limit
    market = market.set_index(["datetime", "instrument"]).sort_index()
    calendar = pd.DatetimeIndex(json.loads((parent_root/"prepared/calendar.json").read_text(encoding="utf-8")))
    cash = pd.DataFrame({"datetime": calendar, "open": 1.}).set_index("datetime")
    suspend = prices.loc[prices.suspended, ["trade_date", "instrument"]]
    # Retain all forty names for holding reviews and the original terminal dates.
    ranks = ranks.loc[ranks[KEY[0]] >= pd.Timestamp(source.test_start)]
    targets = pd.DatetimeIndex(ranks[KEY[1]].unique()).sort_values()
    candidate_dates = sorted(candidates[KEY[0]].unique())
    artifacts, metrics, daily, episodes = {}, {}, {}, {}
    selections = {"baseline": None, **{arm.lower(): matched_entry_priorities(value, arm="model") for arm, value in decisions.items()}}
    for arm, priorities in selections.items():
        replay = replay_shadow_portfolio(rankings=ranks, daily=market, benchmark_daily=cash, suspend_rows=suspend,
            trading_calendar=calendar, policy=transition_policy_from_payload(identity.shadow_policy), policy_sha256=identity.shadow_policy_sha256,
            cost_policy=identity.cost_policy, request_id=f"{plan.experiment_id}_{arm}", candidate_decision_dates=candidate_dates, entry_priorities=priorities)
        observed = replay.daily.set_index("target_trade_date")
        if not observed.index.is_unique:
            _fail("timing portfolio contains duplicate valuation dates")
        series = observed.net_return_bps.reindex(targets)
        absent = series.isna()
        if absent.any() and (replay.metrics["active_episode_count"] or (absent & (series.index <= observed.index.max())).any()):
            _fail("timing portfolio cannot pad active/missing valuations")
        series = series.fillna(0.)  # Only verified terminal idle-cash dates.
        wealth = pd.Series(np.r_[1., (1+series/10000).cumprod().to_numpy()])
        metrics[arm] = {**replay.metrics, "common_day_count": len(series), "common_horizon_return": float(wealth.iloc[-1]-1),
            "common_horizon_max_drawdown": float((wealth/wealth.cummax()-1).min()), "benchmark": "ZERO_RETURN_CASH_NOT_MARKET_INDEX"}
        daily[arm], episodes[arm] = series, replay.episodes
        artifacts[arm+"_daily.parquet"], artifacts[arm+"_episodes.parquet"] = _parquet_bytes(replay.daily), _parquet_bytes(replay.episodes)
    matched = pd.DataFrame(daily)
    control_lift = fixed_block_interval_v3(matched.timing_fifteen-matched.core_thirteen)
    baseline_lift = fixed_block_interval_v3(matched.timing_fifteen-matched.baseline)
    support = shadow_portfolio_intervention_support_v3(episodes["core_thirteen"], episodes["timing_fifteen"], targets)
    positive_navigation = control_lift["mean_bps"] > 0 and baseline_lift["mean_bps"] > 0 and support["entry_action_difference_day_count"] > 0
    report = {"plan_sha256": plan.plan_sha256, "metrics": metrics, "timing_minus_core": control_lift,
        "timing_minus_baseline": baseline_lift, "model_configuration_count": 2, "fitted_head_count": 4, "economic_candidate_count": 1,
        "top20_rows": len(candidates), "test_decision_days": len(candidate_dates),
        "attribution": {arm: _attribution(decisions[arm.upper()], episodes["baseline"], episodes[arm]) for arm in ("core_thirteen", "timing_fifteen")},
        "actual_shadow_intervention_support": support,
        "portfolio_endpoint_execution_audits": {arm: shadow_endpoint_execution_audit_v3(value, prices) for arm, value in episodes.items()},
        "navigation": "CONSIDER_FURTHER_EVALUATION_NOT_CONFIRMATION" if positive_navigation else "STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION",
        "regime_support": "UNPROVEN_NO_PREREGISTERED_REGIME",
        "power": {"timing_minus_core": timing_descriptive_mde_v1(matched.timing_fifteen-matched.core_thirteen),
            "timing_minus_baseline": timing_descriptive_mde_v1(matched.timing_fifteen-matched.baseline),
            "confirmation_mde_registered": False, "activation_supported": False},
        "source_evidence": "RECOVERED_LIMITED_CURRENT_DB_NON_VINTAGE", "evidence_limitations": list(identity.evidence_limitations),
        "economic_effectiveness": "EXPLORATORY_NOT_CONFIRMED", "decision_use": "NAVIGATION_ONLY", "deployable": False,
        "sealed_holdout_accessed": False, "real_fill_proven": False, "daily_grid_delivery_verified": False}
    for arm, value in decisions.items():
        artifacts[arm.lower()+"_decisions.parquet"] = _parquet_bytes(value)
    artifacts.update({"matched_daily.parquet": _parquet_bytes(matched.reset_index()), "evaluation.json": _json_bytes(report)})
    target = publish_stage(study_root=root, stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"], artifacts=artifacts)
    register_timing_stage(plan, loaded, root, "EVALUATED", target/"manifest.json", generated=2, evaluated=2)
    return target
