"""Matched nine/thirteen fits over one frozen eligible cohort, no tuning."""
from dataclasses import dataclass
import re
from typing import Any

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_information_v1 import FEATURES
from backend.services.advisory_model_first.economic_entry_aligned_training import prepare_aligned_rows_v3
from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
from backend.services.advisory_model_first.economic_entry_information_contracts import EconomicEntryInformationTrainingRequestV4
from backend.services.advisory_model_first.economic_entry_information_source import _numeric
from backend.services.advisory_model_first.economic_entry_labels import KEY, _fail, _frame
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def information_rows_sha256(frame):
    names = KEY + list(FEATURES) + ["information_visible_through", "information_day_sha256"]
    narrowed = _frame(frame.loc[:, names], KEY, set(names))
    narrowed["information_visible_through"] = pd.to_datetime(narrowed.information_visible_through)
    values = [{name: value.isoformat() if isinstance(value,pd.Timestamp) else None if pd.isna(value) else value
               for name,value in row.items()} for row in narrowed.sort_values(KEY).to_dict("records")]
    return sha(values)


def information_common_fit_rows_sha256(rows):
    names = KEY + ["split", "label_information_end", *ECONOMIC_FEATURE_NAMES, *FEATURES,
                   "return_target_bps", "entry_loss_target_bps"]
    selected = rows.loc[rows.split.isin(["train", "validation"]) & rows.information_training_eligible, names]
    return sha([{name: value.isoformat() if isinstance(value,pd.Timestamp) else value for name,value in row.items()}
                for row in selected.sort_values(KEY).to_dict("records")])


def assemble_information_rows_v4(*, features, labels, information, parent_request):
    original = prepare_aligned_rows_v3(features=features, labels=labels, request=parent_request)
    names = KEY + list(FEATURES) + ["information_visible_through", "information_day_sha256"]
    if not set(names).issubset(information) or len(information)>parent_request.source_request.resource_max_rows:
        _fail("information fit rows lack their source columns or exceed budget")
    extra = _frame(information.loc[:,names], KEY, set(names))
    visible = extra.information_visible_through.map(pd.Timestamp)
    if visible.isna().any() or not visible.eq(extra[KEY[0]]).all():
        _fail("information fit clock must be exactly D")
    if not extra.information_day_sha256.map(lambda value: isinstance(value,str) and re.fullmatch(r"[0-9a-f]{64}",value) is not None).all():
        _fail("information fit rows lack exact daily identities")
    for name in FEATURES:
        extra[name] = _numeric(extra[name])
    merged = original.merge(extra, on=KEY, how="outer", validate="one_to_one", indicator=True)
    if not merged._merge.eq("both").all():
        _fail("information must preserve the exact original candidate keys")
    finite = np.isfinite(merged.loc[:, FEATURES].to_numpy(dtype=float)).all(axis=1)
    merged["information_values_available"] = finite
    merged["information_training_eligible"] = merged.common_training_eligible & finite
    return merged.drop(columns="_merge")


@dataclass(frozen=True)
class EconomicInformationArmV4:
    request: EconomicEntryInformationTrainingRequestV4
    arm: str
    feature_names: tuple[str, ...]
    return_model: Any
    risk_model: Any
    feature_bounds: dict
    price_support: dict
    diagnostics: dict


@dataclass(frozen=True)
class EconomicInformationTrainingResultV4:
    request: EconomicEntryInformationTrainingRequestV4
    arms: dict[str, EconomicInformationArmV4]
    split_receipt: pd.DataFrame
    diagnostics: dict


def train_information_entry_v4(*, features, labels, information, request):
    import lightgbm as lgb
    request = EconomicEntryInformationTrainingRequestV4.model_validate(request.model_dump())
    if request.parent_request.lightgbm_version != lgb.__version__:
        _fail("information fit LightGBM version differs from its frozen parent")
    if information_rows_sha256(information)!=request.information_rows_sha256:
        _fail("information fit source content identity differs")
    rows = assemble_information_rows_v4(features=features, labels=labels, information=information, parent_request=request.parent_request)
    if information_common_fit_rows_sha256(rows)!=request.common_fit_rows_sha256:
        _fail("information common eligible identity differs")
    configuration = request.parent_request.source_request
    train = rows.loc[rows.split.eq("train") & rows.information_training_eligible]
    validation = rows.loc[rows.split.eq("validation") & rows.information_training_eligible]
    if len(train)<configuration.minimum_bin_observations or validation.empty:
        _fail("information fit lacks mature common supervision", "ADVISORY_ECONOMIC_INFORMATION_INSUFFICIENT_SUPPORT")
    support = {}
    bins = np.floor(train.query_gap_bps/configuration.gap_bin_width_bps).astype(int)
    for bin_id,group in train.groupby(bins,sort=True):
        count,days=len(group),group[KEY[0]].nunique()
        if count>=configuration.minimum_bin_observations and days>=configuration.minimum_bin_days:
            support[int(bin_id)]={"observation_count":count,"decision_day_count":days,
                "observed_min_gap_bps":float(group.query_gap_bps.min()),"observed_max_gap_bps":float(group.query_gap_bps.max())}
    arms = {}
    for arm,names in (("MATCHED_NINE",ECONOMIC_FEATURE_NAMES),("INFORMATION_THIRTEEN",request.feature_names)):
        parameters=dict(configuration.effective_parameters)
        rounds=parameters.pop("num_boost_round")
        mean_objective=parameters.pop("return_objective")
        risk_objective,alpha=parameters.pop("risk_objective"),parameters.pop("risk_alpha")
        matrix=train.loc[:,names].astype(float)
        mean=lgb.train({**parameters,"objective":mean_objective},lgb.Dataset(matrix,label=train.return_target_bps.astype(float)),num_boost_round=rounds)
        risk=lgb.train({**parameters,"objective":risk_objective,"alpha":alpha},lgb.Dataset(matrix,label=train.entry_loss_target_bps.astype(float)),num_boost_round=rounds)
        observed=validation.loc[:,names].astype(float)
        predicted,downside=np.asarray(mean.predict(observed,num_threads=2)),np.asarray(risk.predict(observed,num_threads=2))
        if (predicted.shape!=(len(observed),) or downside.shape!=(len(observed),) or not np.isfinite(predicted).all()
                or not np.isfinite(downside).all() or (downside<0).any() or (downside>10000).any()):
            _fail("information validation heads returned malformed values")
        diagnostics={"train_rows":len(train),"validation_rows":len(validation),"common_fit_rows_sha256":request.common_fit_rows_sha256,
            "validation_return_abs_error_p90_bps":float(np.quantile(abs(validation.return_target_bps.to_numpy()-predicted),.9)),
            "validation_entry_loss_q90_coverage":float(np.mean(validation.entry_loss_target_bps.to_numpy()<=downside)),
            "test_used_for_training_or_calibration":False,"economic_effectiveness":"NOT_EVALUATED"}
        arms[arm]=EconomicInformationArmV4(request,arm,names,mean,risk,
            {name:(float(matrix[name].min()),float(matrix[name].max())) for name in names},dict(support),diagnostics)
    return EconomicInformationTrainingResultV4(request,arms,
        rows.loc[:,KEY+["split","label_information_end","common_training_eligible","information_training_eligible","information_values_available"]],
        {"model_configuration_count":2,"fitted_head_count":4,"economic_candidate_count":1,"candidate_rows_preserved":len(rows),
         "information_unknown_rows":int((~rows.information_values_available).sum()),"deployable":False,"decision_use":"NAVIGATION_ONLY"})
