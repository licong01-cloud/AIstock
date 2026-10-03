import copy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_price_source_v1 import sector_dynamic_rows_v1, sector_quotes_v1, structural_crosswalk_v1


def test_versioned_explicit_crosswalk_zero_id_and_conflicts():
    pairs = [dict(industry_code=str(110100+index), index_code=f'{801000+index}.SI', level='L2', src='SW2021') for index in range(134)]
    snapshot = dict(schema_version='advisory_sw2021_structural_crosswalk_v1', taxonomy_version='SW2021',
        stability_basis='SAME_SW2021_STANDARD_EXPLICIT_CODE_PAIRS', source_table='market.sw_index_classify',
        official_url='https://tushare.pro/document/2?doc_id=181', captured_at='2026-10-04T00:00:00+08:00',
        historical_capture_proven=False, rows=pairs, official_pairs=copy.deepcopy(pairs))
    args = dict(code_map={'entries': [dict(canonical_l2_code='801000.SI', l2_code_id=0)]},
        taxonomy=dict(version='SW2021', contract_id='sw2021_classification_catalog_v1'))
    assert structural_crosswalk_v1(snapshot, **args)['110100'] == 0
    snapshot['official_pairs'][0]['index_code'] = '999999.SI'
    with pytest.raises(ValueError, match='disagree'):
        structural_crosswalk_v1(snapshot, **args)
    snapshot['official_pairs'] = copy.deepcopy(pairs)
    snapshot['rows'].append(copy.deepcopy(pairs[0]))
    with pytest.raises(ValueError, match='one-to-one'):
        structural_crosswalk_v1(snapshot, **args)


def test_chunk_dedup_future_ignored_null_unknown_and_namespace_conflict():
    day = pd.Timestamp('2025-01-02')
    quote = pd.DataFrame(dict(datetime=[day, day, day, day+pd.Timedelta(days=1)],
        l2_code_id=[0, 0, None, 0], sw2_close=[100., 100., 999., -999.]))
    args = dict(namespace={0}, first_day=day, last_day=day)
    normalized = sector_quotes_v1([quote.iloc[:2], quote.iloc[2:]], **args)
    assert normalized.sw2_close.tolist() == [100.]
    with pytest.raises(ValueError, match='namespace'):
        sector_quotes_v1([quote.assign(l2_code_id=999)], **args)
    quote.loc[1, 'sw2_close'] = 101.
    with pytest.raises(ValueError, match='contradiction'):
        sector_quotes_v1([quote.iloc[:1], quote.iloc[1:]], **args)


def test_complete_calendar_D_cutoff_units_missing_and_knowledge_clock():
    days = pd.bdate_range('2025-01-02', periods=23)
    rows = pd.DataFrame([{KEY[0]: day, KEY[1]: day+pd.Timedelta(days=1), KEY[2]: f'{index:06d}.SZ',
        'ret_5': .08, 'classification_l2_code': '110100' if index != 3 else None,
        'classification_known_from': days[0] if index != 3 else None}
        for index, day in enumerate((days[20], days[21], days[0], days[20]))])
    closes = 100*np.power(1.01, np.arange(len(days)))
    quotes = pd.DataFrame(dict(datetime=days, l2_code_id=0, sw2_close=closes))
    args = dict(rows=rows, quotes=quotes, calendar=days, crosswalk={'110100': 0})
    result = sector_dynamic_rows_v1(**args)
    assert len(result) == len(rows) and result.iloc[2].sector_feature_status == 'UNKNOWN_WARMUP'
    assert result.iloc[3].sector_feature_status == 'UNKNOWN_CLASSIFICATION_OR_MAPPING'
    assert result.iloc[0].sector_ret5 == pytest.approx(1.01**5-1)
    assert result.iloc[0].sector_vol20 == pytest.approx(0., abs=1e-14)
    assert result.iloc[0].relative_ret5_sector == pytest.approx(.08-(1.01**5-1))
    poisoned = quotes.copy()
    poisoned.loc[poisoned.datetime.gt(days[21]), 'sw2_close'] = -999.
    pd.testing.assert_frame_equal(result, sector_dynamic_rows_v1(**{**args, 'quotes': poisoned}))
    missing = sector_dynamic_rows_v1(**{**args, 'quotes': quotes.loc[quotes.datetime.ne(days[5])]})
    assert missing.iloc[0].sector_feature_status == 'UNKNOWN_SECTOR_QUOTE'
    assert len(missing) == len(rows) and pd.isna(missing.iloc[0].sector_ret5)
    rows.loc[0, 'classification_known_from'] = days[22]
    with pytest.raises(ValueError, match='known by D'):
        sector_dynamic_rows_v1(**args)
