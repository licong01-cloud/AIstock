"""One v4 conditional-node kernel, with matched information availability."""
import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
from backend.services.advisory_model_first.economic_entry_information_contracts import EconomicEntryInformationTrainingRequestV4
from backend.services.advisory_model_first.economic_entry_information_source import _numeric
from backend.services.advisory_model_first.economic_entry_labels import _fail


def predict_information_entry_nodes_v4(*, fitted, matrix):
    request=EconomicEntryInformationTrainingRequestV4.model_validate(fitted.request.model_dump())
    configuration=request.parent_request.source_request
    names={"MATCHED_NINE":ECONOMIC_FEATURE_NAMES,"INFORMATION_THIRTEEN":request.feature_names}.get(fitted.arm)
    if (names is None or fitted.feature_names!=names or set(fitted.feature_bounds)!=set(names)
            or len(matrix)>configuration.resource_max_rows or not set(request.feature_names).issubset(matrix)):
        _fail("v4 node arm/schema/support or row budget differs")
    # New-source UNKNOWN gates both arms; nine-field control cannot gain a larger
    # decision population merely because it does not use the four new features.
    all_values=matrix.loc[:,request.feature_names].copy()
    for name in request.feature_names:
        all_values[name]=_numeric(all_values[name])
    supported=np.isfinite(all_values.to_numpy(dtype=float)).all(axis=1)
    values=all_values.loc[:,names]
    for name,(lower,upper) in fitted.feature_bounds.items():
        if not np.isfinite([lower,upper]).all() or lower>upper:
            _fail("v4 feature support bounds invalid")
        supported &= values[name].between(lower,upper).to_numpy()
    observations,days=np.zeros(len(values),dtype=int),np.zeros(len(values),dtype=int)
    for position,gap in enumerate(values.query_gap_bps.to_numpy()):
        if not supported[position]:
            continue
        item=fitted.price_support.get(int(np.floor(gap/configuration.gap_bin_width_bps)))
        if item is None:
            supported[position]=False
            continue
        count,day_count=item["observation_count"],item["decision_day_count"]
        minimum,maximum=item["observed_min_gap_bps"],item["observed_max_gap_bps"]
        if (type(count) is not int or type(day_count) is not int or count<configuration.minimum_bin_observations
                or day_count<configuration.minimum_bin_days or day_count>count or not np.isfinite([minimum,maximum]).all()
                or minimum>maximum):
            _fail("v4 price support identity invalid")
        observations[position],days[position]=count,day_count
        supported[position]=minimum<=gap<=maximum
    means,risks=np.full(len(values),np.nan),np.full(len(values),np.nan)
    if supported.any():
        usable=values.loc[supported]
        mean=np.asarray(fitted.return_model.predict(usable,num_threads=2),dtype=float)
        risk=np.asarray(fitted.risk_model.predict(usable,num_threads=2),dtype=float)
        if (mean.shape!=(len(usable),) or risk.shape!=(len(usable),) or not np.isfinite(mean).all()
                or not np.isfinite(risk).all() or (risk<0).any() or (risk>10000).any()):
            _fail("v4 numerical estimates invalid")
        means[supported],risks[supported]=mean,risk
    acceptable=supported & (means>0) & (risks<=request.parent_request.risk_reference_bps)
    return pd.DataFrame({"model_action":np.where(acceptable,"TAKE",np.where(supported,"SKIP","UNAVAILABLE")),
        "reason_code":np.where(acceptable,"POSITIVE_VALUE_ACCEPTABLE_ENTRY_LOSS",
            np.where(supported,"VALUE_OR_ENTRY_LOSS_REJECTED","OUT_OF_SUPPORT_OR_D_FEATURES_UNKNOWN")),
        "expected_net_return_bps":means,"entry_net_max_loss_q90_bps":risks,
        "support_observations":observations,"support_decision_days":days},index=matrix.index)
