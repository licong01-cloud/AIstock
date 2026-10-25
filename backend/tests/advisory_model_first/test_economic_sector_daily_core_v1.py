"""Reuse core inputs; only direct sector composition contracts, no source I/O."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES, build_economic_daily_feature_core_v1
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_daily_core_v1 import (
    FEATURES, build_sector_daily_features_v1, compose_sector_daily_features_v1,
)
from backend.services.advisory_model_first.economic_sector_price_source_v1 import SECTOR_FEATURES, sector_dynamic_rows_v1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.tests.advisory_model_first.test_economic_daily_feature_core_v1 import packet as packet


@pytest.fixture
def sector_packet(packet):
    calendar = ((pd.Timestamp(packet['calendar'][0]) - pd.offsets.BDay()).date(), *packet['calendar'])
    classified = packet['candidates'].loc[:, KEY].copy()
    classified['classification_l2_code'] = ['110000', '120000']
    classified['classification_known_from'] = pd.Timestamp(calendar[0])
    quotes = pd.DataFrame([(day, identity, 100. + i + identity)
                           for i, day in enumerate(calendar[:-1]) for identity in (0, 1)],
                          columns=['datetime', 'l2_code_id', 'sw2_close'])
    mapping = {'110000': 0, '120000': 1}
    return dict(core_inputs={key: value for key, value in packet.items() if key != 'calendar'},
                calendar=calendar, classification_rows=classified, sector_quotes=quotes,
                crosswalk=mapping, expected_crosswalk_values_sha256=sha(mapping))


def test_composition_matches_existing_formulas_id_zero_and_repeatable_hash(sector_packet, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('pure composition must not read stored data')
    monkeypatch.setattr(pd, 'read_parquet', forbidden)
    monkeypatch.setattr(pd, 'HDFStore', forbidden)
    frame, receipt = build_sector_daily_features_v1(**sector_packet)
    core, _ = build_economic_daily_feature_core_v1(**sector_packet['core_inputs'], calendar=sector_packet['calendar'][1:])
    joined = core.merge(sector_packet['classification_rows'], on=KEY, validate='one_to_one', sort=False)
    quotes = sector_packet['sector_quotes'].assign(datetime=lambda df: pd.to_datetime(df.datetime))
    sector = sector_dynamic_rows_v1(rows=joined, quotes=quotes,
                                    calendar=sector_packet['calendar'][:-1], crosswalk=sector_packet['crosswalk'])
    expected = core.merge(sector.loc[:, [*KEY, *SECTOR_FEATURES]], on=KEY, validate='one_to_one', sort=False)
    pd.testing.assert_frame_equal(frame, expected.loc[:, [*KEY, *FEATURES]])
    assert list(frame.columns) == [*KEY, *FEATURES] and len(FEATURES) == 15
    assert frame.sector_ret5.iloc[0] == pytest.approx(120/115-1)
    closes = np.arange(100., 121.)
    assert frame.sector_vol20.iloc[0] == pytest.approx(np.std(closes[1:]/closes[:-1]-1, ddof=1))
    again, repeated = build_sector_daily_features_v1(**deepcopy(sector_packet))
    pd.testing.assert_frame_equal(frame, again)
    assert receipt == repeated and receipt['identity_sha256'] == sha({k: v for k, v in receipt.items() if k != 'identity_sha256'})
    assert receipt['source_evidence'] == 'COMPUTATION_ONLY' and receipt['native_identity'] == 'UNPROVEN'
    assert receipt['old_training_parity'] == 'UNPROVEN' and receipt['deployable'] is False
    assert receipt['outcomes_read'] is False and receipt['new_native_receipt'] is False
    assert all(not row['fields'] and row['sector_status'] == 'AVAILABLE' for row in receipt['unknown_fields'])


def test_missing_sector_session_and_classification_never_remove_candidates(sector_packet):
    original, _ = build_sector_daily_features_v1(**sector_packet)
    missing = deepcopy(sector_packet)
    missing['classification_rows'].loc[1, 'classification_l2_code'] = None
    quotes = missing['sector_quotes']
    missing['sector_quotes'] = quotes.loc[quotes.l2_code_id.eq(0) & quotes.datetime.ne(sector_packet['calendar'][6])]
    frame, receipt = build_sector_daily_features_v1(**missing)
    pd.testing.assert_frame_equal(frame.loc[:, [*KEY, *D_FEATURES]], original.loc[:, [*KEY, *D_FEATURES]])
    assert frame.loc[:, SECTOR_FEATURES].isna().all().all()
    assert {row['sector_status'] for row in receipt['unknown_fields']} == {'UNKNOWN_SECTOR_QUOTE', 'UNKNOWN_CLASSIFICATION_OR_MAPPING'}
    assert receipt['candidate_count'] == 2 and receipt['deployable'] is False


def test_normal_raw_missing_preserves_whole_roster_and_sector_unknown(sector_packet):
    original = sector_packet['core_inputs']['candidates'].loc[:, KEY]
    raw = sector_packet['core_inputs']['raw_daily']
    sector_packet['core_inputs']['raw_daily'] = raw.loc[~(raw.instrument.eq('000001.SZ') & raw.trade_date.eq(pd.Timestamp(sector_packet['calendar'][-2])))]
    frame, receipt = build_sector_daily_features_v1(**sector_packet)
    pd.testing.assert_frame_equal(frame.loc[:, KEY], original)
    assert frame.ret_5.iloc[0] is not None and pd.isna(frame.ret_5.iloc[0])
    assert frame.loc[0, list(SECTOR_FEATURES)].isna().all()
    assert receipt['unknown_fields'][0]['fields'] and receipt['candidate_count'] == 2


@pytest.mark.parametrize('case', ['calendar', 'future_quote', 'foreign_quote_id', 'duplicate_quote',
                                 'quote_budget', 'classification_subset', 'classification_duplicate',
                                 'future_classification', 'extra_input', 'crosswalk_pin', 'crosswalk_bool'])
def test_unsafe_inputs_fail_closed(sector_packet, case):
    if case == 'calendar':
        sector_packet['calendar'] = sector_packet['calendar'][1:]
    elif case == 'future_quote':
        sector_packet['sector_quotes'].loc[0, 'datetime'] = sector_packet['calendar'][-1]
    elif case == 'foreign_quote_id':
        sector_packet['sector_quotes'].loc[0, 'l2_code_id'] = 77
    elif case == 'duplicate_quote':
        extra = sector_packet['sector_quotes'].iloc[:1].copy()
        extra['sw2_close'] = 101.
        sector_packet['sector_quotes'] = pd.concat([sector_packet['sector_quotes'], extra], ignore_index=True)
    elif case == 'quote_budget':
        sector_packet['sector_quotes'] = pd.concat([sector_packet['sector_quotes']]*11, ignore_index=True)
    elif case == 'classification_subset':
        sector_packet['classification_rows'] = sector_packet['classification_rows'].iloc[:1]
    elif case == 'classification_duplicate':
        sector_packet['classification_rows'] = pd.concat([sector_packet['classification_rows'].iloc[:1]]*2, ignore_index=True)
    elif case == 'future_classification':
        sector_packet['classification_rows'].loc[0, 'classification_known_from'] = pd.Timestamp(sector_packet['calendar'][-1])
    elif case == 'extra_input':
        sector_packet['core_inputs']['query_gap_bps'] = 1.
    elif case == 'crosswalk_pin':
        sector_packet['crosswalk']['110000'] = 2
    else:
        sector_packet['crosswalk']['110000'] = False
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        build_sector_daily_features_v1(**sector_packet)


@pytest.mark.parametrize('object_classification', [False, True])
def test_no_candidates_remains_empty_computation_not_native_success(sector_packet, object_classification):
    for field in ('candidates', 'raw_daily', 'suspend_rows'):
        sector_packet['core_inputs'][field] = sector_packet['core_inputs'][field].iloc[:0]
    classified = sector_packet['classification_rows']
    sector_packet['classification_rows'] = pd.DataFrame(columns=classified.columns) if object_classification else classified.iloc[:0]
    sector_packet['sector_quotes'] = sector_packet['sector_quotes'].iloc[:0]
    frame, receipt = build_sector_daily_features_v1(**sector_packet)
    assert frame.empty and list(frame.columns) == [*KEY, *FEATURES]
    assert receipt['status'] == 'NO_CANDIDATES' and receipt['native_identity'] == 'UNPROVEN'


@pytest.mark.parametrize('poison', ['foreign_classification', 'future_quote'])
def test_empty_roster_still_validates_classification_and_quote_clock(sector_packet, poison):
    for field in ('candidates', 'raw_daily', 'suspend_rows'):
        sector_packet['core_inputs'][field] = sector_packet['core_inputs'][field].iloc[:0]
    classified = sector_packet['classification_rows']
    sector_packet['classification_rows'] = classified.iloc[:1] if poison == 'foreign_classification' else pd.DataFrame(columns=classified.columns)
    quoted = sector_packet['sector_quotes'].iloc[:1].copy()
    quoted['datetime'] = sector_packet['calendar'][-1]
    sector_packet['sector_quotes'] = quoted
    with pytest.raises(ValueError, match='exact original candidate keys|future or foreign session'):
        build_sector_daily_features_v1(**sector_packet)


def test_precomputed_source_entry_reuses_same_core_without_a_second_calculation(sector_packet, monkeypatch):
    import backend.services.advisory_model_first.economic_sector_daily_core_v1 as module
    expected, expected_receipt = build_sector_daily_features_v1(**sector_packet)
    core, receipt = build_economic_daily_feature_core_v1(**sector_packet['core_inputs'], calendar=sector_packet['calendar'][1:])
    def forbidden(*args, **kwargs):
        raise AssertionError('source results must not trigger another core calculation/read')
    monkeypatch.setattr(module, 'build_economic_daily_feature_core_v1', forbidden)
    result, composed = compose_sector_daily_features_v1(core_frame=core, core_receipt=receipt,
        **{key: value for key, value in sector_packet.items() if key != 'core_inputs'})
    pd.testing.assert_frame_equal(result, expected)
    assert composed == expected_receipt
    receipt['deployable'] = True
    assert composed['core_receipt']['deployable'] is False


@pytest.mark.parametrize('poison', ['feature', 'clock', 'semantics', 'native', 'source_write'])
def test_precomputed_core_cannot_bypass_content_clock_or_qualification(sector_packet, poison):
    core, receipt = build_economic_daily_feature_core_v1(**sector_packet['core_inputs'], calendar=sector_packet['calendar'][1:])
    if poison == 'feature':
        core.loc[0, 'ret_5'] += .01
    elif poison == 'clock':
        core.loc[0, KEY[0]] = pd.Timestamp(sector_packet['calendar'][-1])
    elif poison == 'semantics':
        receipt['semantics_sha256'] = '0'*64
    elif poison == 'native':
        receipt['source_evidence'] = 'NATIVE_COMPLETE'
    else:
        receipt['db_source'] = {'readonly': True, 'database_written': True, 'native_capture': False}
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        compose_sector_daily_features_v1(core_frame=core, core_receipt=receipt,
            **{key: value for key, value in sector_packet.items() if key != 'core_inputs'})
