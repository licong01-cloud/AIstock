"""Projected frozen features and independently maturity-filtered GP5 supervision."""
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_moneyflow_price_5td_contracts_v1 import (
    FEATURES, KEY, MONEYFLOW_FEATURES, POLICY, POLICY_SHA256, ROSTER, GenericPrice5TDConfigurationV1,
)
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import validate_roster

SUPERVISION = ("observed_gap_bps", "gross_terminal_ratio", "path_min_ratio", "label_status", "label_reason")
CLOCK = "label_information_end"
FLOW_META = ("moneyflow_feature_status", "moneyflow_feature_visible_through")


def moneyflow_values(frame):
    if not frame.columns.is_unique or not set(MONEYFLOW_FEATURES).issubset(frame.columns):
        raise ValueError("moneyflow feature schema differs")
    values = frame.loc[:, MONEYFLOW_FEATURES].map(_number).to_numpy(dtype=float)
    for i, (low, high) in enumerate(((-1., 1.), (0., 1.), (-1., 1.))):
        known = values[:, i][~np.isnan(values[:, i])]
        if ((known < low) | (known > high)).any():
            raise ValueError("moneyflow known ratio contradicts dimensionless units")
    return values


def _normalize_keys(frame):
    out = frame.copy(deep=True)
    for name in KEY[:2]:
        out[name] = out[name].map(_day).map(pd.Timestamp)
    if len(out) > 7720 or out.duplicated(list(KEY)).any():
        raise ValueError("moneyflow frozen candidate keys are duplicated or unbounded")
    return out


def join_moneyflow_features_v1(*, original, funding, decision_dates, calendar):
    """No financial target is needed for the complete one-to-one D feature join."""
    original, funding = _normalize_keys(original), _normalize_keys(funding)
    validate_roster(original.loc[:, ROSTER], decision_dates, calendar)
    columns = (*KEY, *MONEYFLOW_FEATURES, *FLOW_META)
    if not funding.columns.is_unique or set(funding.columns) != set(columns):
        raise ValueError("moneyflow source projection includes foreign fields")
    if set(original.loc[:, KEY].itertuples(index=False, name=None)) != set(funding.loc[:, KEY].itertuples(index=False, name=None)):
        raise ValueError("moneyflow source is not the complete original development KEY population")
    values = moneyflow_values(funding)
    clocks = funding.moneyflow_feature_visible_through.map(_day)
    if any(clock > d.date() for clock, d in zip(clocks, funding[KEY[0]], strict=True)):
        raise ValueError("moneyflow source contains information after D")
    statuses = funding.moneyflow_feature_status
    if not statuses.map(lambda value: isinstance(value, str) and (value == "AVAILABLE" or value.startswith("UNKNOWN_"))).all():
        raise ValueError("moneyflow source status is not an explicit known/unknown declaration")
    if (statuses.eq("AVAILABLE").to_numpy() & np.isnan(values).any(axis=1)).any():
        raise ValueError("moneyflow AVAILABLE declaration contains missing ratios")
    if (statuses.ne("AVAILABLE").to_numpy() & ~np.isnan(values).any(axis=1)).any():
        raise ValueError("moneyflow UNKNOWN declaration has no unknown field")
    for i, name in enumerate(MONEYFLOW_FEATURES):
        funding[name] = values[:, i]
    out = original.merge(funding, on=list(KEY), how="left", sort=False, validate="one_to_one")
    if not out.loc[:, ROSTER].equals(original.loc[:, ROSTER]):
        raise ValueError("moneyflow join changed original order/rank/group")
    return out


def read_moneyflow_development_v1(*, gp5_rows, moneyflow_rows, configuration, decision_dates, calendar):
    """Filter dates/columns at Arrow read, never decode test or immature numeric targets."""
    config = GenericPrice5TDConfigurationV1.model_validate(configuration)
    splits = ((config.train_start, config.train_end), (config.validation_start, config.validation_end))
    filters = [[(KEY[0], ">=", pd.Timestamp(a).to_pydatetime()),
                (KEY[0], "<=", pd.Timestamp(b).to_pydatetime())] for a, b in splits]
    metadata = (*ROSTER, *FEATURES, CLOCK, "label_contract", "policy_sha256")
    original = pq.read_table(gp5_rows, columns=list(metadata), filters=filters).to_pandas()
    funding = pq.read_table(moneyflow_rows, columns=[*KEY, *MONEYFLOW_FEATURES, *FLOW_META], filters=filters).to_pandas()
    rows = join_moneyflow_features_v1(original=original, funding=funding, decision_dates=decision_dates, calendar=calendar)
    if (not rows.policy_sha256.eq(POLICY_SHA256).all()
            or not rows.label_contract.eq(POLICY["label_contract"]).all()):
        raise ValueError("moneyflow parent label policy differs")
    rows[CLOCK] = rows[CLOCK].map(lambda value: _day(value, nullable=True)).map(pd.Timestamp)
    days = [_day(day) for day in calendar]
    for row in rows.loc[:, [*KEY, CLOCK]].itertuples(index=False, name=None):
        t = days.index(row[1].date())
        expected = pd.Timestamp(days[t+4]) if t+4 < len(days) else pd.NaT
        if not ((pd.isna(row[3]) and pd.isna(expected)) or row[3] == expected):
            raise ValueError("moneyflow parent endpoint is not original T+4")
    targets = []
    for a, b in splits:
        mature = [(KEY[0], ">=", pd.Timestamp(a).to_pydatetime()),
                  (KEY[0], "<=", pd.Timestamp(b).to_pydatetime()),
                  (CLOCK, "<=", pd.Timestamp(b).to_pydatetime())]
        targets.append(pq.read_table(gp5_rows, columns=[*KEY, *SUPERVISION], filters=mature).to_pandas())
    labels = _normalize_keys(pd.concat(targets, ignore_index=True))
    if labels.label_status.isna().any():
        raise ValueError("moneyflow mature source has no explicit label status")
    expected_keys = rows.loc[(rows[KEY[0]].le(pd.Timestamp(config.train_end)) & rows[CLOCK].le(pd.Timestamp(config.train_end)))
                            | (rows[KEY[0]].ge(pd.Timestamp(config.validation_start)) & rows[CLOCK].le(pd.Timestamp(config.validation_end))), KEY]
    if set(labels.loc[:, KEY].itertuples(index=False, name=None)) != set(expected_keys.itertuples(index=False, name=None)):
        raise ValueError("moneyflow mature supervision projection differs from original metadata")
    rows = rows.merge(labels, on=list(KEY), how="left", sort=False, validate="one_to_one")
    immature = rows.label_status.isna()
    rows.loc[immature, "label_status"] = "IMMATURE"
    rows.loc[immature, "label_reason"] = "DEVELOPMENT_ENDPOINT_NOT_YET_VISIBLE"
    for name in SUPERVISION[:3]:
        rows[name] = rows[name].map(_number).astype(float)
    for name in FEATURES:
        rows[name] = rows[name].map(_number).astype(float)
    if not rows.label_status.isin(("AVAILABLE", "UNKNOWN", "IMMATURE", "ENTRY_NOT_EXECUTABLE")).all():
        raise ValueError("moneyflow original GP5 label status differs")
    available = rows.label_status.eq("AVAILABLE")
    if (rows.loc[available, list(SUPERVISION[:3])].isna().any().any()
            or rows.loc[available, "gross_terminal_ratio"].le(0).any()
            or rows.loc[available, "path_min_ratio"].le(0).any()
            or rows.loc[available, "observed_gap_bps"].le(-10000).any()
            or rows.loc[available, "path_min_ratio"].gt(rows.loc[available, "gross_terminal_ratio"]+1e-6).any()):
        raise ValueError("moneyflow original mature supervision contradicts price units")
    return rows
