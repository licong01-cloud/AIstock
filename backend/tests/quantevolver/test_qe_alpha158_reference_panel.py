from __future__ import annotations

import ast
import logging
import sys
import types

import numpy as np
import pandas as pd
import pytest

from backend.services.quantevolver.config_composer import ConfigComposer, RDAGENT_DEFAULT_DATA_SPLIT
from backend.services.quantevolver.qe_custom_loaders import ReferencePanelAlpha158Loader
from backend.tests.quantevolver.test_qe_custom_loader_instruments import _FakeProvider, _install_fake_provider
from backend.tests.quantevolver.test_qe_prepare_factors_cache_contract import _patch_universe


def params(version='v2'):
    return {
        'disable_alpha158': False,
        'label_horizon': 20,
        'observation_panel': {
            'schema_version': 'qe_observation_panel_' + version,
            'panel_id': 'pdh-reference',
            'mode': 'reference_factor_intersection',
            'reference_factor_names': ['PDH'],
        },
        'feature_processing': {'exclude_from_model': ['PDH'], 'clip_outlier': True},
    }


def test_v2_explicit_opt_in_keeps_v1_alpha_rejection_and_raw_controls_boundary():
    c = ConfigComposer()
    with pytest.raises(ValueError, match='alpha158_not_supported'):
        c._normalize_observation_panel_contract(params('v1'), ['F', 'PDH'])
    value = c._normalize_feature_processing(params(), ['F', 'PDH'])
    assert value['model_feature_names'] == ['F']
    p = params()
    p['feature_processing']['raw_state_controls'] = {}
    with pytest.raises(ValueError, match='raw_state_controls requires'):
        c._normalize_feature_processing(p, ['F', 'PDH'])
    p = params()
    p['disable_alpha158'] = True
    with pytest.raises(ValueError, match='requires_explicit_alpha158'):
        c._normalize_observation_panel_contract(p, ['F', 'PDH'])


def test_prepare_v2_reference_mask_is_raw_and_projection_is_later(monkeypatch):
    _patch_universe(monkeypatch)
    code = ConfigComposer()._compose_prepare_factors(
        [{'factor_name': n, 'code_text': f'def calculate_{n}(*a): return None'} for n in ['F', 'PDH']],
        factor_data_dir='/tmp/factors', data_split=dict(RDAGENT_DEFAULT_DATA_SPLIT), custom_params=params(),
    )
    compile(code, 'prepare_factors.py', 'exec')
    tree = ast.parse(code)
    fn = next(x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name == '_apply_observation_panel')
    env = {'QE_OBSERVATION_PANEL_CONTRACT': params()['observation_panel'], 'logger': logging.getLogger(__name__)}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), '<reference>', 'exec'), env)
    raw = pd.DataFrame({'F': [np.nan, 9.0, 3.0], 'PDH': [1.0, np.nan, 0.0]})
    result = env['_apply_observation_panel'](raw)
    assert result.index.tolist() == [0, 2] and pd.isna(result.loc[0, 'F'])
    assert code.index('combined = _apply_observation_panel(combined)') < code.index("combined = combined.loc[:, QE_FEATURE_PROCESSING['model_feature_names']]")


def build_loader(tmp_path, monkeypatch, mutation=None):
    index = pd.MultiIndex.from_product(
        [pd.to_datetime(['2026-01-02', '2026-01-05']), ['000001.SZ', '000002.SZ']],
        names=['datetime', 'instrument'],
    )
    # Reference mask has already removed a row. PIT excludes another stock-date.
    path = tmp_path / 'factors.parquet'
    pd.DataFrame({'F': [np.nan, 3.0, 4.0]}, index=index[[0, 2, 3]]).to_parquet(path)
    provider = _FakeProvider({'000001.SZ': [('2026-01-02', '2026-01-05')],
                              '000002.SZ': [('2026-01-02', '2026-01-02')]})
    _install_fake_provider(monkeypatch, provider)
    alpha = pd.DataFrame(np.ones((4, 21), dtype=float), index=index,
                         columns=pd.MultiIndex.from_tuples([('feature', f'A{i}') for i in range(20)] + [('label', 'LABEL0')]))
    alpha[('label', 'LABEL0')] = 0.01
    if mutation:
        alpha = mutation(alpha)
    utils = types.ModuleType('qlib.utils')
    utils.init_instance_by_config = lambda cfg: types.SimpleNamespace(load=lambda *a: alpha)
    monkeypatch.setitem(sys.modules, 'qlib.utils', utils)
    configs = [{'class': 'qlib.contrib.data.loader.Alpha158DL'},
               {'class': 'qlib.data.dataset.loader.StaticDataLoader', 'kwargs': {'config': str(path)}}]
    return ReferencePanelAlpha158Loader(configs, str(path), label_horizon=20), index


def test_alpha_rows_never_restore_reference_or_pit_exclusions(tmp_path, monkeypatch):
    loader, index = build_loader(tmp_path, monkeypatch)
    result = loader.load('stock_universe', '2026-01-02', '2026-01-05')
    assert result.index.equals(index[[0, 2]])
    assert result.columns.tolist() == [('feature', f'A{i}') for i in range(20)] + [('feature', 'F'), ('label', 'LABEL0')]
    assert pd.isna(result.iloc[0][('feature', 'F')])
    assert result[('label', 'LABEL0')].tolist() == [0.01, 0.01]


@pytest.mark.parametrize('mutation,reason', [
    (lambda x: x.iloc[1:], 'missing_reference_rows'),
    (lambda x: pd.concat([x, x.iloc[:1]]), 'frame_invalid'),
    (lambda x: x.rename(columns={'A0': 'F'}), 'feature_schema_invalid'),
    (lambda x: x.drop(columns=[('feature', 'A0')]), 'feature_schema_invalid'),
    (lambda x: x.rename(columns={'LABEL0': 'OTHER'}), 'frame_invalid'),
])
def test_invalid_alpha_closure_is_never_silently_intersected(tmp_path, monkeypatch, mutation, reason):
    loader, _ = build_loader(tmp_path, monkeypatch, mutation)
    with pytest.raises(ValueError, match=reason):
        loader.load('stock_universe', '2026-01-02', '2026-01-05')


def test_different_labels_fail_instead_of_filling_or_replacing(tmp_path, monkeypatch):
    def mutate(frame):
        frame.loc[frame.index[0], ('label', 'LABEL0')] = np.nan
        return frame
    loader, _ = build_loader(tmp_path, monkeypatch, mutate)
    with pytest.raises(ValueError, match='label_mismatch'):
        loader.load('stock_universe', '2026-01-02', '2026-01-05')


def test_same_immature_label_na_is_retained(tmp_path, monkeypatch):
    def mutate(frame):
        frame.loc[frame.index[0], ('label', 'LABEL0')] = np.nan
        return frame
    loader, _ = build_loader(tmp_path, monkeypatch, mutate)
    provider = sys.modules['qlib.data'].D
    original_features = provider.features

    def features(*args, **kwargs):
        frame = original_features(*args, **kwargs)
        frame.iloc[0, 0] = np.nan
        return frame

    provider.features = features
    result = loader.load('stock_universe', '2026-01-02', '2026-01-05')
    assert pd.isna(result.iloc[0][('label', 'LABEL0')])
