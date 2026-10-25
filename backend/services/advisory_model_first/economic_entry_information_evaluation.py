"""Four fixed shadow arms; incremental evidence stays development navigation."""
import json
from decimal import Decimal

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_aligned_evaluation import (
    aligned_intervention_attribution_v3, fixed_block_interval_v3, shadow_endpoint_execution_audit_v3,
    shadow_portfolio_intervention_support_v3,
)
from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES, EconomicEntryInputIdentityV1
from backend.services.advisory_model_first.economic_entry_evaluation import matched_entry_priorities
from backend.services.advisory_model_first.economic_entry_information_inference import predict_information_entry_nodes_v4
from backend.services.advisory_model_first.economic_entry_information_pipeline import (
    _inputs, _ledger, load_fitted_information_v4, load_information_study_v4, register_information_stage,
)
from backend.services.advisory_model_first.economic_entry_information_training import information_rows_sha256
from backend.services.advisory_model_first.economic_entry_labels import KEY, _fail, _frame, _positive
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.policy_contracts import transition_policy_from_payload
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio


def _attribution(decisions,baseline,model):
    # A valid all-active portfolio has no settled return column yet. Preserve
    # unknown episode outcomes; never turn these into zero-profit observations.
    normalized=[]
    for frame in (baseline,model):
        copied=frame.copy()
        if not copied.empty and "net_return_bps" not in copied:
            if not copied.status.eq("ACTIVE").all():
                _fail("v4 settled episodes lack realized returns")
            copied["net_return_bps"]=np.nan
        normalized.append(copied)
    result=aligned_intervention_attribution_v3(decisions,*normalized)
    result["baseline_unsettled_episode_count"]=int(baseline.status.eq("ACTIVE").sum()) if not baseline.empty else 0
    result["model_unsettled_episode_count"]=int(model.status.eq("ACTIVE").sum()) if not model.empty else 0
    return result


def query_information_actual_opens_v4(*, fitted, identity, candidates, features, information, references, prices):
    identity=EconomicEntryInputIdentityV1.model_validate(identity.model_dump())
    source=fitted.request.parent_request.source_request
    if (source.input_identity_sha256!=identity.identity_sha256
            or information_rows_sha256(information)!=fitted.request.information_rows_sha256
            or max(len(candidates),len(features),len(information),len(references))>source.resource_max_rows or len(prices)>500000):
        _fail("v4 observed condition source identity/budget differs")
    roster=_frame(candidates,KEY,set(KEY)|{"selection_effective_rank"}).loc[:,KEY+["selection_effective_rank"]]
    ranks=roster.selection_effective_rank
    if (not ranks.map(lambda value:isinstance(value,(int,np.integer)) and not isinstance(value,(bool,np.bool_))).all()
            or not ranks.between(1,20).all() or roster.duplicated([KEY[0],"selection_effective_rank"]).any()
            or roster.duplicated([KEY[0],"instrument"]).any()):
        _fail("v4 observed Top20 identity differs")
    d_names=[name for name in ECONOMIC_FEATURE_NAMES if name!="query_gap_bps"]
    frozen=_frame(features,KEY,set(KEY+d_names+["feature_visible_through","feature_source_sha256"]))
    if not frozen.feature_source_sha256.eq(source.feature_source_sha256).all() or (pd.to_datetime(frozen.feature_visible_through)>frozen[KEY[0]]).any():
        _fail("v4 original D feature identity/clock differs")
    extras=_frame(information,KEY,set(KEY)|{"information_visible_through"})
    if not pd.to_datetime(extras.information_visible_through).eq(extras[KEY[0]]).all():
        _fail("v4 new information clock differs from D")
    feature_map,extra_map=frozen.set_index(KEY).to_dict("index"),extras.set_index(KEY).to_dict("index")
    refs=_frame(references,KEY,set(KEY)|{"target_reference_raw_cny","reference_visible_through","source_sha256"})
    if not refs.source_sha256.eq(identity.reference_source_sha256).all():
        _fail("v4 historical reference identity differs")
    ref_map=refs.set_index(KEY).to_dict("index")
    fields={"trade_date","instrument","raw_open_cny","suspended","tradability_unknown","up_limit","down_limit","source_sha256","price_coordinate_sha256"}
    observed=_frame(prices,["trade_date","instrument"],fields)
    if (not observed.source_sha256.eq(identity.price_source_sha256).all()
            or not observed.price_coordinate_sha256.eq(identity.price_coordinate_sha256).all()
            or any(not observed[name].map(lambda value:type(value) is bool).all() for name in ("suspended","tradability_unknown"))):
        _fail("v4 observed raw price/trading identity differs")
    market=observed.set_index(["trade_date","instrument"]).to_dict("index")
    rows,matrices,positions=[],[],[]
    for candidate in roster.to_dict("records"):
        decision,target,symbol=(candidate[name] for name in KEY)
        if not source.test_start<=decision.date()<=source.test_end or not decision<target:
            _fail("v4 observed query lies outside its frozen consumed decisions")
        key=decision,target,symbol
        reference,feature,extra,quote=ref_map.get(key),feature_map.get(key),extra_map.get(key),market.get((target,symbol))
        row={**candidate,"model_action":"UNAVAILABLE","rule_action":"UNAVAILABLE","reason_code":"SOURCE_UNAVAILABLE",
             "actual_gap_bps":None,"expected_net_return_bps":None,"entry_net_max_loss_q90_bps":None}
        if reference is None or quote is None:
            rows.append(row)
            continue
        if pd.Timestamp(reference["reference_visible_through"])!=decision:
            _fail("v4 observation reference must be exactly D visible")
        if quote["tradability_unknown"]:
            row["reason_code"]="OPEN_TRADABILITY_UNKNOWN"
        elif quote["suspended"]:
            row.update(model_action="NOT_APPLICABLE",rule_action="NOT_APPLICABLE",reason_code="SUSPENDED")
        else:
            actual,anchor,upper,lower=(_positive(quote["raw_open_cny"]),_positive(reference["target_reference_raw_cny"]),
                _positive(quote["up_limit"]),_positive(quote["down_limit"]))
            if None in (actual,anchor,upper,lower) or lower>upper or actual>=upper-1e-9 or actual<lower-1e-9:
                row["reason_code"]="OPEN_LIMIT_OR_EXECUTABILITY_UNPROVEN"
            elif Decimal(str(actual)) % Decimal(".01"):
                row["reason_code"]="OBSERVED_OPEN_OFF_EQUITY_TICK"
            else:
                gap=(actual/anchor-1)*10000
                row.update(actual_gap_bps=gap,rule_action="TAKE" if -300<=gap<=300 else "SKIP")
                if feature is not None and extra is not None:
                    positions.append(len(rows))
                    matrices.append({name:gap if name=="query_gap_bps" else feature[name] if name in d_names else extra[name]
                        for name in fitted.request.feature_names})
        rows.append(row)
    if matrices:
        prediction=predict_information_entry_nodes_v4(fitted=fitted,matrix=pd.DataFrame(matrices))
        for position,values in zip(positions,prediction.to_dict("records"),strict=True):
            rows[position].update(values)
    return pd.DataFrame(rows)


def evaluate_information_study_v4(*, plan_path, output_root):
    plan,root,registered=load_information_study_v4(plan_path=plan_path,output_root=output_root)
    _,loaded,information=_inputs(plan)
    trained=read_stage(root/"trained",stage="trained",plan_sha256=plan.plan_sha256,parent_sha256=registered["stage_sha256"])
    _ledger(plan,root,"TRAINED")
    if (root/"evaluated").exists():
        read_stage(root/"evaluated",stage="evaluated",plan_sha256=plan.plan_sha256,parent_sha256=trained["stage_sha256"])
        register_information_stage(plan,loaded[1],root,"EVALUATED",root/"evaluated/manifest.json",generated=2,evaluated=2)
        return root/"evaluated"
    arms=load_fitted_information_v4(plan_path=plan_path,output_root=output_root)
    parent_root=loaded[2]
    identity=EconomicEntryInputIdentityV1.model_validate_json((parent_root/"prepared/identity.json").read_text(encoding="utf-8"))
    source=plan.training_request.parent_request.source_request
    ranks=pd.read_parquet(parent_root/"prepared/frozen_rankings.parquet")
    candidates=ranks.loc[ranks.is_candidate_decision & ranks.selection_effective_rank.le(20)
        & ranks[KEY[0]].between(pd.Timestamp(source.test_start),pd.Timestamp(source.test_end))].copy()
    prices=pd.read_parquet(parent_root/"prepared/prices.parquet")
    references=pd.read_parquet(parent_root/"prepared/references.parquet")
    decisions={arm:query_information_actual_opens_v4(fitted=value,identity=identity,candidates=candidates,
        features=loaded[6],information=information,references=references,prices=prices) for arm,value in arms.items()}
    # Only settlement now consumes future realized prices; query above only uses
    # real T observation conditions, never future episode status or label values.
    market=prices.rename(columns={"trade_date":"datetime"}).copy()
    for field in ("open","high","low","close"):
        market[field]=market[f"raw_{field}_cny"]*market.policy_price_per_raw_cny
    market["factor"],market["limit_up"],market["limit_down"]=market.policy_price_per_raw_cny,1.,1.
    market["up_limit_price"],market["down_limit_price"]=market.up_limit,market.down_limit
    market=market.set_index(["datetime","instrument"]).sort_index()
    calendar=pd.DatetimeIndex(json.loads((parent_root/"prepared/calendar.json").read_text(encoding="utf-8")))
    cash=pd.DataFrame({"datetime":calendar,"open":1.}).set_index("datetime")
    suspend=prices.loc[prices.suspended,["trade_date","instrument"]]
    ranks=ranks.loc[ranks[KEY[0]]>=pd.Timestamp(source.test_start)]
    targets=pd.DatetimeIndex(ranks[KEY[1]].unique()).sort_values()
    candidate_dates=sorted(candidates[KEY[0]].unique())
    artifacts,metrics,daily,episodes={},{},{},{}
    selections={"baseline":None,"rule":matched_entry_priorities(decisions["INFORMATION_THIRTEEN"],arm="rule"),
        **{arm.lower():matched_entry_priorities(value,arm="model") for arm,value in decisions.items()}}
    for arm,priorities in selections.items():
        replay=replay_shadow_portfolio(rankings=ranks,daily=market,benchmark_daily=cash,suspend_rows=suspend,
            trading_calendar=calendar,policy=transition_policy_from_payload(identity.shadow_policy),policy_sha256=identity.shadow_policy_sha256,
            cost_policy=identity.cost_policy,request_id=f"{plan.experiment_id}_{arm}",candidate_decision_dates=candidate_dates,entry_priorities=priorities)
        observed=replay.daily.set_index("target_trade_date")
        if not observed.index.is_unique:
            _fail("v4 portfolio has duplicate valuation dates")
        series=observed.net_return_bps.reindex(targets)
        absent=series.isna()
        if absent.any() and (replay.metrics["active_episode_count"] or (absent & (series.index<=observed.index.max())).any()):
            _fail("v4 portfolio cannot pad missing active valuations")
        series=series.fillna(0.)
        wealth=pd.Series(np.r_[1.,(1+series/10000).cumprod().to_numpy()])
        metrics[arm]={**replay.metrics,"common_day_count":len(series),"common_horizon_return":float(wealth.iloc[-1]-1),
            "common_horizon_max_drawdown":float((wealth/wealth.cummax()-1).min()),"benchmark":"ZERO_RETURN_CASH_NOT_MARKET_INDEX"}
        daily[arm],episodes[arm]=series,replay.episodes
        artifacts[arm+"_daily.parquet"],artifacts[arm+"_episodes.parquet"]=_parquet_bytes(replay.daily),_parquet_bytes(replay.episodes)
    matched=pd.DataFrame(daily)
    incremental=matched.information_thirteen-matched.matched_nine
    unavailable=[value for value in loaded[4] if value.original.decision_date>=source.test_start and value.status=="UNAVAILABLE"]
    report={"plan_sha256":plan.plan_sha256,"metrics":metrics,"information_minus_matched_nine":fixed_block_interval_v3(incremental),
        "information_minus_baseline":fixed_block_interval_v3(matched.information_thirteen-matched.baseline),
        "model_configuration_count":2,"fitted_head_count":4,"economic_candidate_count":1,
        "top20_rows":len(candidates),"test_decision_days":len(candidate_dates),
        "attribution":{arm:_attribution(decisions[arm.upper()],episodes["baseline"],episodes[arm])
            for arm in ("matched_nine","information_thirteen")},
        "actual_shadow_intervention_support":shadow_portfolio_intervention_support_v3(episodes["matched_nine"],episodes["information_thirteen"],targets),
        "portfolio_endpoint_execution_audits":{arm:shadow_endpoint_execution_audit_v3(value,prices) for arm,value in episodes.items()},
        "test_original_episode_execution_unknown":len(unavailable),"shadow_returns_are_not_execution_proof":True,
        "source_evidence":"RECOVERED_LIMITED_CURRENT_DB_NON_VINTAGE","evidence_limitations":list(identity.evidence_limitations),
        "economic_effectiveness":"EXPLORATORY_NOT_CONFIRMED","decision_use":"NAVIGATION_ONLY","deployable":False,
        "sealed_holdout_accessed":False,"daily_grid_delivery_verified":False,"real_fill_proven":False}
    for arm,value in decisions.items():
        artifacts[arm.lower()+"_decisions.parquet"]=_parquet_bytes(value)
    artifacts.update({"matched_daily.parquet":_parquet_bytes(matched.reset_index()),"evaluation.json":_json_bytes(report),
        "execution_limitations.json":_json_bytes([value.model_dump(mode="json") for value in unavailable])})
    target=publish_stage(study_root=root,stage="evaluated",plan_sha256=plan.plan_sha256,parent_sha256=trained["stage_sha256"],artifacts=artifacts)
    register_information_stage(plan,loaded[1],root,"EVALUATED",target/"manifest.json",generated=2,evaluated=2)
    return target
