"""Compose existing D-only formulas; no data I/O or model qualification."""
from collections.abc import Mapping
from copy import deepcopy
from datetime import date
from decimal import Decimal
from numbers import Real
import re

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import (
    D_FEATURES, SEMANTICS, _records, build_economic_daily_feature_core_v1,
)
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _frame
from backend.services.advisory_model_first.economic_sector_price_source_v1 import (
    SECTOR_FEATURES, sector_dynamic_rows_v1, sector_quotes_v1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

CORE_INPUTS = {'candidates', 'raw_daily', 'market_daily', 'benchmark_daily',
               'suspend_rows', 'component_roles', 'terminal_weights'}
FEATURES = (*D_FEATURES, *SECTOR_FEATURES)
CLASS_FIELDS = (*KEY, 'classification_l2_code', 'classification_known_from')
QUOTE_FIELDS = ('datetime', 'l2_code_id', 'sw2_close')
RECIPE = dict(schema_version='economic_sector_daily_core_v1', feature_names=FEATURES,
              core_semantics_sha256=sha(SEMANTICS), core_sessions=20, sector_sessions=21,
              sector_return_std_ddof=1, company_membership='CALLER_D_VISIBLE_CLASSIFICATION',
              query_coordinate='EXCLUDED_FROM_D_FEATURES', qualification='COMPUTATION_ONLY')


def _crosswalk(values, expected):
    if not isinstance(values, Mapping) or not 1 <= len(values) <= 134:
        raise ValueError('sector daily crosswalk needs bounded explicit code pairs')
    result = dict(values)
    ids = []
    for code, identity in result.items():
        if (not isinstance(code, str) or re.fullmatch(r'\d{6}', code) is None
                or identity is not None and (type(identity) is not int or identity < 0)):
            raise ValueError('sector daily crosswalk code/id differs')
        if identity is not None:
            ids.append(identity)
    if (len(ids) != len(set(ids)) or not isinstance(expected, str)
            or re.fullmatch(r'[0-9a-f]{64}', expected) is None or sha(result) != expected):
        raise ValueError('sector daily crosswalk values disagree with external pin')
    return result


def _calendar(calendar):
    if (not isinstance(calendar, (tuple, list)) or len(calendar) != 22
            or any(type(day) is not date for day in calendar)
            or list(calendar) != sorted(set(calendar))):
        raise ValueError('sector daily needs twenty-one D sessions plus T')


def build_sector_daily_features_v1(*, core_inputs, calendar, classification_rows,
                                   sector_quotes, crosswalk, expected_crosswalk_values_sha256):
    """Compute the same core once, then use the common composition entry."""
    _calendar(calendar)
    if not isinstance(core_inputs, dict) or set(core_inputs) != CORE_INPUTS:
        raise ValueError('sector daily needs exact original core inputs')
    core, receipt = build_economic_daily_feature_core_v1(**core_inputs, calendar=tuple(calendar[1:]))
    return compose_sector_daily_features_v1(core_frame=core, core_receipt=receipt, calendar=calendar,
        classification_rows=classification_rows, sector_quotes=sector_quotes, crosswalk=crosswalk,
        expected_crosswalk_values_sha256=expected_crosswalk_values_sha256)


def compose_sector_daily_features_v1(*, core_frame, core_receipt, calendar, classification_rows,
                                     sector_quotes, crosswalk, expected_crosswalk_values_sha256):
    """Consume existing verified core results without repeating source reads.

    Receipt checking proves content compatibility, never native provenance.
    The caller owns source/PIT qualification and authoritative calendar.
    """
    _calendar(calendar)
    if (not isinstance(core_frame, pd.DataFrame) or len(core_frame) > 20
            or list(core_frame.columns) != [*KEY, *D_FEATURES] or not isinstance(core_receipt, dict)):
        raise ValueError('sector daily computed core schema/budget differs')
    core, core_receipt = core_frame.copy(), deepcopy(core_receipt)
    checked = _frame(core, KEY, set(KEY) | set(D_FEATURES))
    if (not checked[KEY[0]].eq(pd.Timestamp(calendar[-2])).all()
            or not checked[KEY[1]].eq(pd.Timestamp(calendar[-1])).all()
            or core.instrument.duplicated().any()
            or not core.instrument.map(lambda value: isinstance(value, str) and bool(re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', value))).all()
            or len(core) and not core[KEY].equals(checked[KEY])
            or core.loc[:, D_FEATURES].map(lambda value: value is not None and value is not pd.NA
                and (isinstance(value, (bool, np.bool_)) or not isinstance(value, (Real, Decimal)))).any().any()
            or np.isinf(core.loc[:, D_FEATURES].to_numpy(dtype=float)).any()):
        raise ValueError('sector daily computed core keys, clocks or numeric values differ')
    if (core_receipt.get('schema_version') != SEMANTICS['schema_version']
            or core_receipt.get('semantics_sha256') != sha(SEMANTICS)
            or core_receipt.get('feature_sha256') != sha(_records(core))
            or not isinstance(core_receipt.get('input_sha256'), str)
            or re.fullmatch(r'[0-9a-f]{64}', core_receipt['input_sha256']) is None
            or type(core_receipt.get('candidate_count')) is not int or core_receipt['candidate_count'] != len(core)
            or core_receipt.get('decision_date') != calendar[-2].isoformat()
            or core_receipt.get('target_date') != calendar[-1].isoformat()
            or core_receipt.get('status') != ('NO_CANDIDATES' if core.empty else 'COMPUTED')
            or core_receipt.get('source_evidence') != 'COMPUTATION_ONLY'
            or core_receipt.get('old_training_parity') != 'UNPROVEN'
            or any(core_receipt.get(key) is not False for key in ('deployable', 'outcomes_read', 'new_native_receipt'))):
        raise ValueError('sector daily computed core receipt or qualification differs')
    db_source = core_receipt.get('db_source')
    if db_source is not None and (not isinstance(db_source, dict) or db_source.get('readonly') is not True
            or db_source.get('database_written') is not False or db_source.get('native_capture') is not False):
        raise ValueError('sector daily computed core cannot hide a source write or native upgrade')
    mapping = _crosswalk(crosswalk, expected_crosswalk_values_sha256)
    if (not isinstance(classification_rows, pd.DataFrame) or not classification_rows.columns.is_unique
            or set(classification_rows.columns) != set(CLASS_FIELDS) or len(classification_rows) > 20):
        raise ValueError('sector daily classification schema/budget differs')
    classified = _frame(classification_rows.loc[:, CLASS_FIELDS], KEY, set(CLASS_FIELDS))
    if set(classified[KEY].itertuples(index=False, name=None)) != set(core[KEY].itertuples(index=False, name=None)):
        raise ValueError('sector daily classification must preserve exact original candidate keys')
    for index, clock in classified.classification_known_from.items():
        if pd.isna(clock):
            continue
        if (not isinstance(clock, (date, pd.Timestamp, str))
                or isinstance(clock, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}', clock) is None):
            raise ValueError('sector daily classification clock must be an explicit session date')
        clock = _day(clock)
        if clock > pd.Timestamp(calendar[-2]):
            raise ValueError('sector daily classification is not known by D')
        classified.loc[index, 'classification_known_from'] = clock
    if (not isinstance(sector_quotes, pd.DataFrame) or not sector_quotes.columns.is_unique
            or set(sector_quotes.columns) != set(QUOTE_FIELDS) or len(sector_quotes) > 420):
        raise ValueError('sector daily quote schema/budget differs')
    quoted = sector_quotes.loc[:, QUOTE_FIELDS].copy()
    days = pd.to_datetime(quoted.datetime, errors='coerce')
    sessions = pd.DatetimeIndex(calendar[:-1])
    if (days.isna().any() or days.dt.tz is not None or not days.eq(days.dt.normalize()).all()
            or not days.isin(sessions).all()):
        raise ValueError('sector daily quote contains future or foreign session')
    quoted['datetime'] = days
    needed_ids = {mapping[code] for code in classified.classification_l2_code.dropna()
                  if code in mapping and mapping[code] is not None}
    normalized = sector_quotes_v1([quoted], namespace=needed_ids,
                                 first_day=sessions[0], last_day=sessions[-1])
    if core.empty:
        sector = pd.DataFrame(columns=[*KEY, *SECTOR_FEATURES, 'sector_feature_status',
                                      'sector_feature_visible_through'])
        result = core.copy()
        for name in SECTOR_FEATURES:
            result[name] = pd.Series(dtype=float)
    else:
        joined = core.merge(classified, on=KEY, how='left', validate='one_to_one', sort=False)
        sector = sector_dynamic_rows_v1(rows=joined, quotes=normalized,
                                        calendar=calendar[:-1], crosswalk=mapping)
        result = core.merge(sector.loc[:, [*KEY, *SECTOR_FEATURES]], on=KEY,
                            how='left', validate='one_to_one', sort=False)
    result = result.loc[:, [*KEY, *FEATURES]]
    if result[KEY].to_dict('records') != core[KEY].to_dict('records'):
        raise ValueError('sector daily feature composition changed the original candidate order')
    if np.isinf(result.loc[:, FEATURES].to_numpy(dtype=float)).any():
        raise ValueError('sector daily derived features are nonfinite')
    source_hash = sha(dict(core_input_sha256=core_receipt['input_sha256'], calendar=[day.isoformat() for day in calendar],
                          classifications=_records(classified), quotes=_records(quoted),
                          crosswalk_values_sha256=expected_crosswalk_values_sha256))
    sector_states = dict(zip(sector.instrument, sector.sector_feature_status, strict=True))
    receipt = dict(schema_version=RECIPE['schema_version'], semantics_sha256=sha(RECIPE),
                   input_sha256=source_hash, feature_sha256=sha(_records(result)),
                   core_receipt=core_receipt, crosswalk_values_sha256=expected_crosswalk_values_sha256,
                   decision_date=calendar[-2].isoformat(), target_date=calendar[-1].isoformat(),
                   feature_names=list(FEATURES), candidate_count=len(result),
                   unknown_fields=[dict(instrument=row['instrument'],
                                        fields=[name for name in FEATURES if pd.isna(row[name])],
                                        sector_status=sector_states[row['instrument']])
                                   for row in result.to_dict('records')],
                   status='NO_CANDIDATES' if result.empty else 'COMPUTED',
                   source_evidence='COMPUTATION_ONLY', native_identity='UNPROVEN',
                   old_training_parity='UNPROVEN', deployable=False, outcomes_read=False,
                   new_native_receipt=False)
    receipt['identity_sha256'] = sha(receipt)
    return result, receipt
