from dataclasses import replace
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_path_price_v1 import CLOCK, INFORMATION_FEATURES, PATH_FEATURES, STATUS, sector_path_price_fit_identity_v1, sector_path_price_nodes_v1, sector_path_price_price_set_v1, sector_path_price_rows_v1, train_sector_path_price_v1
from backend.services.advisory_model_first.economic_sector_price_source_v1 import SECTOR_FEATURES
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1, information_matrix_v1
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def path_fit_fixture():
    old = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), sector_path_price_features=list(INFORMATION_FEATURES), matched_information_features=list(SECTOR_FEATURES))
    models = {name: {**body, 'features': 16 if name.startswith('matched') else 18} for name, body in old.models.items()}
    return SectorPriceFitV1(recipe, models, old.support, old.diagnostics, sector_path_price_fit_identity_v1(recipe, models, old.support))


def path_rows_fixture():
    days = pd.bdate_range('2025-01-01', periods=23)
    base = pd.DataFrame([{KEY[0]: days[20], KEY[1]: days[21], KEY[2]: stock,
        **dict.fromkeys(SECTOR_FEATURES, .01), 'sector_feature_status': 'AVAILABLE',
        'sector_feature_visible_through': days[20], 'classification_l2_code': '100100',
        'classification_known_from': days[0], 'gross_value_ratio': 1.04} for stock in ('000001.SZ', '000002.SZ')])
    closes = np.r_[np.repeat(100., 18), 120., 100., 115.]
    quotes = pd.DataFrame(dict(datetime=days[:21], l2_code_id=0, sw2_close=closes))
    return dict(base_rows=base, quotes=quotes, candidates=base[KEY], calendar=days, crosswalk={'100100': 0})


def test_path_adds_information_not_recoverable_from_ret5_vol20_exact_21_sessions():
    worlds = [np.r_[np.repeat(0., 18), .1, -.1], np.r_[np.repeat(0., 18), -.1, .1]]
    closes = [np.r_[1., np.cumprod(1+r)] for r in worlds]
    assert np.isclose(closes[0][-1]/closes[0][-6], closes[1][-1]/closes[1][-6])
    assert np.isclose(worlds[0].std(ddof=1), worlds[1].std(ddof=1))
    result = []
    for close in closes:
        args = path_rows_fixture()
        args['quotes']['sw2_close'] = close
        rows = sector_path_price_rows_v1(**args)
        result.append(rows.loc[0, list(PATH_FEATURES)].to_numpy(dtype=float))
        assert len(rows) == 2 and rows.gross_value_ratio.eq(1.04).all() and rows[CLOCK].equals(rows[KEY[0]])
    assert not np.isclose(result[0][0], result[1][0]) and not np.isclose(result[0][1], result[1][1])


def test_unknown_warmup_missing_and_future_unrequested_prices_preserve_original_keys():
    args = path_rows_fixture()
    args['base_rows'].index = [8, 3]
    args['quotes'] = pd.concat([args['quotes'], pd.DataFrame([dict(datetime=args['calendar'][22], l2_code_id=0, sw2_close='future not consumed')])])
    rows = sector_path_price_rows_v1(**args)
    assert rows[STATUS].eq('AVAILABLE').all() and rows[KEY].equals(args['base_rows'][KEY].reset_index(drop=True))
    assert np.allclose(rows.loc[:, PATH_FEATURES], [[.15, 115/120-1]]*2)
    for change in ('old_unknown', 'missing_quote', 'unmapped', 'warmup'):
        one = path_rows_fixture()
        if change == 'old_unknown':
            one['base_rows'].loc[1, 'sector_feature_status'] = 'UNKNOWN_CLASSIFICATION_OR_MAPPING'
        elif change == 'missing_quote':
            one['quotes'] = one['quotes'].iloc[1:]
        elif change == 'unmapped':
            one['crosswalk']['100100'] = None
        else:
            one['base_rows'][KEY[0]] = one['calendar'][2]
            one['base_rows'][KEY[1]] = one['calendar'][3]
            one['base_rows']['sector_feature_visible_through'] = one['calendar'][2]
            one['candidates'] = one['base_rows'][KEY]
        result = sector_path_price_rows_v1(**one)
        assert len(result) == 2 and result[STATUS].iloc[1] == 'UNKNOWN_SOURCE_OR_INPUT'


@pytest.mark.parametrize('value,valid', [(None, True), (Decimal('NaN'), True), (0., False), (-1., False), (True, False), ('100', False), (np.inf, False), (Decimal('sNaN'), False)])
def test_consumed_quote_missing_not_bad_number(value, valid):
    args = path_rows_fixture()
    args['quotes']['sw2_close'] = args['quotes']['sw2_close'].astype(object)
    args['quotes'].at[0, 'sw2_close'] = value
    if valid:
        rows = sector_path_price_rows_v1(**args)
        assert len(rows) == 2 and rows[STATUS].eq('UNKNOWN_SOURCE_OR_INPUT').all()
    else:
        with pytest.raises(ValueError):
            sector_path_price_rows_v1(**args)


def test_original_key_clock_classification_collision_duplicate_and_overflow_are_errors():
    for change in ('key', 'clock', 'classification_clock', 'class_code', 'duplicate_quote', 'field_collision', 'overflow'):
        args = path_rows_fixture()
        if change == 'key':
            args['base_rows'] = args['base_rows'].iloc[:1]
        elif change == 'clock':
            args['base_rows'].loc[0, 'sector_feature_visible_through'] = args['calendar'][21]
        elif change == 'classification_clock':
            args['base_rows'].loc[0, 'classification_known_from'] = args['calendar'][21]
        elif change == 'class_code':
            args['base_rows'].loc[0, 'classification_l2_code'] = 'bad'
        elif change == 'duplicate_quote':
            args['quotes'] = pd.concat([args['quotes'], args['quotes'].iloc[:1]])
        elif change == 'field_collision':
            args['base_rows'][PATH_FEATURES[0]] = 1.
        else:
            args['quotes'].loc[19, 'sw2_close'] = 1e-308
            args['quotes'].loc[20, 'sw2_close'] = 1e308
        with pytest.raises(ValueError):
            sector_path_price_rows_v1(**args)


def test_actual16_18_same_mature_supervision_test_poison_and_matrix_order():
    rows, configuration = rows_fixture()
    for name in INFORMATION_FEATURES:
        rows[name] = .01
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
    events = []
    fitted = train_sector_path_price_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.diagnostics['train_rows'] == 107
    assert fitted.models['matched_mean']['features'] == 16 and fitted.models['candidate_mean']['features'] == 18
    for arm, width in [('matched', 16), ('candidate', 18)]:
        query = rows.iloc[1:3]
        matrix = information_matrix_v1(query, arm=arm, information_features=INFORMATION_FEATURES, matched_information_features=SECTOR_FEATURES)
        assert matrix.shape == (2, width) and np.array_equal(matrix[:, 12:15], query.loc[:, SECTOR_FEATURES].to_numpy())
        assert len(sector_path_price_nodes_v1(fitted=fitted, rows=query, arm=arm)) == 2
    poisoned = rows.copy()
    poisoned.loc[poisoned.split.eq('test'), [*D_FEATURES, *INFORMATION_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert train_sector_path_price_v1(rows=poisoned, configuration=configuration, before_fit=lambda _: None).model_sha256 == fitted.model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_sector_path_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('no fit'))


def test_price_holes_multiband_shared_unknown_empty_and_metadata_numeric_identity():
    fitted = path_fit_fixture()
    features = dict.fromkeys((*D_FEATURES, *INFORMATION_FEATURES), .01)
    query = pd.DataFrame([{**features, 'actual_gap_bps': gap, STATUS: status} for gap, status in [(-100., 'AVAILABLE'), (0., 'AVAILABLE'), (100., 'UNKNOWN_SOURCE_OR_INPUT')]])
    for arm in ('matched', 'candidate'):
        assert sector_path_price_nodes_v1(fitted=fitted, rows=query, arm=arm).status.tolist() == ['ACCEPTABLE', 'UNKNOWN_INPUT_OR_SUPPORT', 'UNKNOWN_INPUT_OR_SUPPORT']
        assert sector_path_price_price_set_v1(fitted=fitted, d_features=features, arm=arm, reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3).intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    corrupt = {**fitted.models, 'matched_mean': {**fitted.models['matched_mean'], 'features': 13}}
    with pytest.raises(ValueError, match='16/18'):
        sector_path_price_nodes_v1(fitted=replace(fitted, models=corrupt), rows=query, arm='matched')
    assert sector_path_price_nodes_v1(fitted=fitted, rows=query.iloc[:0], arm='candidate').empty
    query.at[0, PATH_FEATURES[0]] = np.inf
    with pytest.raises(ValueError, match='finite'):
        sector_path_price_nodes_v1(fitted=fitted, rows=query, arm='candidate')


def test_loader_uses_only_pinned_date_id_columns_streaming_and_detects_source_change(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    from backend.services.advisory_model_first import economic_sector_path_price_v1 as leaf
    args = path_rows_fixture()
    candidate = tmp_path/'candidate'
    profile_path = tmp_path/'profile.json'
    map_path = candidate/'sectorctx/map.json'
    h5 = candidate/'components/factor_h5_static_candidate_v2/sector_data.h5'
    taxonomy = tmp_path/'taxonomy_catalog.json'
    snapshot = tmp_path/'crosswalk.json'
    prepared = tmp_path/'prepared'
    map_path.parent.mkdir(parents=True)
    prepared.mkdir()
    profile = dict(controller_paths=dict(candidate_root=str(candidate)), components=dict(sector_context_pins=dict(component_root='sectorctx', code_map_file='map.json', code_map_sha256='b'*64, sector_data_sha256='c'*64)))
    for path, body in ((profile_path, profile), (map_path, {}), (taxonomy, {}), (snapshot, {})):
        path.write_text(json.dumps(body), encoding='utf-8')
    expected = {profile_path:'a'*64, map_path:'b'*64, h5:'c'*64, snapshot:'d'*64, taxonomy:'e'*64}
    (prepared/'source_summary.json').write_text(json.dumps(dict(source_sha256={str(path): digest for path, digest in expected.items()})), encoding='utf-8')
    plan = SimpleNamespace(sector_prepared_manifest_ref=SimpleNamespace(artifact_uri=str(prepared/'manifest.json')),
        crosswalk_ref=SimpleNamespace(artifact_uri=str(snapshot), sha256='d'*64), profile_path=str(profile_path),
        profile_sha256='a'*64, parameters=dict(h5_chunk_rows=100000, maximum_quote_rows=60000))
    calls = []
    class Store:
        def __init__(self, path, mode):
            assert path == h5 and mode == 'r'
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def select(self, key, *, where, columns, chunksize):
            assert key == 'data' and columns == ['l2_code_id', 'sw2_close'] and chunksize == 100000
            assert str(args['calendar'][0].date()) in where[0] and str(args['calendar'][20].date()) in where[1]
            calls.append('SELECT_PINNED_H5_NO_COMPANY_MEMBERSHIP')
            return iter([args['quotes']])
    monkeypatch.setattr(leaf.pd, 'HDFStore', Store)
    monkeypatch.setattr(leaf, 'file_sha256', lambda path: expected[path])
    monkeypatch.setattr(leaf, 'structural_crosswalk_v1', lambda *_args, **_kwargs: args['crosswalk'])
    quotes, crosswalk, receipt = leaf.load_sector_path_quotes_v1(plan=plan, base_rows=args['base_rows'], calendar=args['calendar'])
    assert len(quotes) == 21 and crosswalk['100100'] == 0
    assert len(calls) == 1 and receipt['selects'] == 0 and not receipt['database_accessed'] and receipt['native_identity'] == 'UNPROVEN'
    plan.profile_sha256 = 'f'*64
    with pytest.raises(ValueError, match='pins'):
        leaf.load_sector_path_quotes_v1(plan=plan, base_rows=args['base_rows'], calendar=args['calendar'])
    assert len(calls) == 1
    plan.profile_sha256 = 'a'*64
    checked = []
    def changing_hash(path):
        checked.append(path)
        return 'f'*64 if path == h5 and checked.count(path) > 1 else expected[path]
    monkeypatch.setattr(leaf, 'file_sha256', changing_hash)
    with pytest.raises(ValueError, match='while reading'):
        leaf.load_sector_path_quotes_v1(plan=plan, base_rows=args['base_rows'], calendar=args['calendar'])

