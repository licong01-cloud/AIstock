"""Single numerical kernel for v3 conditional price nodes and batch replay."""

from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import _fail
from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryTrainingRequestV3
from backend.services.advisory_model_first.economic_entry_aligned_training import AlignedEntryTrainingResultV3


def predict_aligned_entry_nodes_v3(*, fitted: AlignedEntryTrainingResultV3, matrix: pd.DataFrame) -> pd.DataFrame:
    """Clock/provenance must be bound by the consumer before this numeric kernel.

    Only the frozen nine columns are read. A price node is a hypothesis, never a
    claimed original D prediction or a counterfactual executed trade.
    """
    request = AlignedEntryTrainingRequestV3.model_validate(fitted.request.model_dump())
    configuration = request.source_request
    names = configuration.feature_names
    if len(matrix) > configuration.resource_max_rows or not set(names).issubset(matrix.columns):
        _fail("aligned query is missing model columns or exceeds its row budget")
    values = matrix.loc[:, names].apply(pd.to_numeric, errors="coerce").astype(float)
    finite = np.isfinite(values.to_numpy()).all(axis=1)
    supported = finite.copy()
    if set(fitted.feature_bounds) != set(names):
        _fail("aligned model support schema differs")
    for name, (lower, upper) in fitted.feature_bounds.items():
        if not np.isfinite([lower, upper]).all() or lower > upper:
            _fail("aligned model support bounds invalid")
        supported &= values[name].between(lower, upper).to_numpy()
    observations, days = np.zeros(len(values), dtype=int), np.zeros(len(values), dtype=int)
    for position, gap in enumerate(values.query_gap_bps.to_numpy()):
        if not supported[position]:
            continue
        item = fitted.price_support.get(int(np.floor(gap / configuration.gap_bin_width_bps)))
        if item is None:
            supported[position] = False
            continue
        count, day_count = item["observation_count"], item["decision_day_count"]
        minimum, maximum = item["observed_min_gap_bps"], item["observed_max_gap_bps"]
        if (isinstance(count, bool) or isinstance(day_count, bool) or not isinstance(count, int)
                or not isinstance(day_count, int) or count < configuration.minimum_bin_observations
                or day_count < configuration.minimum_bin_days or day_count > count
                or not np.isfinite([minimum, maximum]).all() or minimum > maximum):
            _fail("aligned price support metadata invalid")
        observations[position], days[position] = count, day_count
        supported[position] = minimum <= gap <= maximum
    means, risks = np.full(len(values), np.nan), np.full(len(values), np.nan)
    if supported.any():
        usable = values.loc[supported]
        returned = np.asarray(fitted.return_model.predict(usable, num_threads=2), dtype=float)
        downside = np.asarray(fitted.risk_model.predict(usable, num_threads=2), dtype=float)
        if (returned.shape != (len(usable),) or downside.shape != (len(usable),)
                or not np.isfinite(returned).all() or not np.isfinite(downside).all()
                or (downside < 0).any() or (downside > 10000).any()):
            _fail("aligned model returned invalid numerical estimates")
        means[supported], risks[supported] = returned, downside
    acceptable = supported & (means > 0) & (risks <= request.risk_reference_bps)
    return pd.DataFrame({
        "model_action": np.where(acceptable, "TAKE", np.where(supported, "SKIP", "UNAVAILABLE")),
        "reason_code": np.where(acceptable, "POSITIVE_VALUE_ACCEPTABLE_ENTRY_LOSS",
                                np.where(supported, "VALUE_OR_ENTRY_LOSS_REJECTED", "OUT_OF_SUPPORT_OR_D_FEATURES_UNKNOWN")),
        "expected_net_return_bps": means, "entry_net_max_loss_q90_bps": risks,
        "support_observations": observations, "support_decision_days": days,
    }, index=matrix.index)
