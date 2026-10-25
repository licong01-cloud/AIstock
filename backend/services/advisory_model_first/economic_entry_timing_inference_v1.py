"""Single conditional node; both arms use the same 15-field train-only domain."""
import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_information_source import _numeric
from backend.services.advisory_model_first.economic_entry_labels import _fail
from backend.services.advisory_model_first.economic_entry_timing_contracts_v1 import ARMS, CANDIDATE_NAMES, TimingTrainingRequestV1


def predict_timing_entry_nodes_v1(*, fitted, matrix):
    request = TimingTrainingRequestV1.model_validate(fitted.request.model_dump())
    config = request.parent_request.source_request
    names = ARMS.get(fitted.arm)
    if (names is None or fitted.feature_names != names or set(fitted.common_bounds) != set(CANDIDATE_NAMES)
            or not matrix.columns.is_unique or set(matrix.columns) != set(CANDIDATE_NAMES) or len(matrix) > config.resource_max_rows):
        _fail("timing arm/order/common bounds or query schema differs")
    values = matrix.loc[:, CANDIDATE_NAMES].copy()
    for name in CANDIDATE_NAMES:
        values[name] = _numeric(values[name])
    supported = np.isfinite(values.to_numpy(dtype=float)).all(axis=1)
    for name, (lower, upper) in fitted.common_bounds.items():
        if not np.isfinite([lower, upper]).all() or lower > upper:
            _fail("timing common bounds malformed")
        supported &= values[name].between(lower, upper).to_numpy()
    counts, days = np.zeros(len(values), dtype=int), np.zeros(len(values), dtype=int)
    for position, gap in enumerate(values.query_gap_bps.to_numpy()):
        if not supported[position]:
            continue
        item = fitted.price_support.get(int(np.floor(gap/config.gap_bin_width_bps)))
        if item is None:
            supported[position] = False
            continue
        count, day_count = item["observation_count"], item["decision_day_count"]
        low, high = item["observed_min_gap_bps"], item["observed_max_gap_bps"]
        if (type(count) is not int or type(day_count) is not int or count < config.minimum_bin_observations
                or day_count < config.minimum_bin_days or day_count > count or not np.isfinite([low, high]).all() or low > high):
            _fail("timing observed gap support malformed")
        counts[position], days[position] = count, day_count
        supported[position] = low <= gap <= high
    means, risks = np.full(len(values), np.nan), np.full(len(values), np.nan)
    if supported.any():
        query = values.loc[supported, names]
        mean, risk = np.asarray(fitted.return_model.predict(query, num_threads=2)), np.asarray(fitted.risk_model.predict(query, num_threads=2))
        if (mean.shape != (len(query),) or risk.shape != (len(query),) or not np.isfinite(mean).all()
                or not np.isfinite(risk).all() or (risk < 0).any() or (risk > 10000).any()):
            _fail("timing heads returned malformed estimates")
        means[supported], risks[supported] = mean, risk
    acceptable = supported & (means > 0) & (risks <= request.parent_request.risk_reference_bps)
    return pd.DataFrame({"model_action": np.where(acceptable, "TAKE", np.where(supported, "SKIP", "UNAVAILABLE")),
        "reason_code": np.where(acceptable, "POSITIVE_VALUE_ACCEPTABLE_ENTRY_LOSS",
            np.where(supported, "VALUE_OR_ENTRY_LOSS_REJECTED", "OUT_OF_COMMON_SUPPORT_OR_D_UNKNOWN")),
        "expected_net_return_bps": means, "entry_net_max_loss_q90_bps": risks,
        "support_observations": counts, "support_decision_days": days}, index=matrix.index)
