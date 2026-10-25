"""Direct update-clock contracts, sharing the existing cohort fixture."""
from copy import deepcopy
import json
import time
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_multiperiod_top10_5td_v1 import rows as rows
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, POLICY
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE, IDENTITY
from backend.services.advisory_model_first import prequential_top10_5td_study_v1 as core
from backend.services.advisory_model_first import prequential_top10_5td_pipeline_v1 as pipeline


def test_schedule_uses_exchange_axis_including_unknown_dates():
    calendar = pd.bdate_range('2025-01-01', periods=48).delete([2, 3])
    days = calendar[1:44]
    blocks = core.schedule(days, calendar, calendar[0], 'inner1')
    assert [len(b['days']) for b in blocks] == [20, 20, 3]
    assert blocks[1]['cutoff'] == str(days[19].date())
    assert blocks[2]['cutoff'] == str(days[39].date())
    assert all(pd.Timestamp(b['cutoff']) < pd.Timestamp(b['first']) for b in blocks)


@pytest.mark.parametrize('fault', ['hole', 'duplicate', 'same_day_cutoff'])
def test_schedule_rejects_compacted_dates_and_illegal_cutoff(fault):
    calendar = pd.bdate_range('2025-01-01', periods=45).tolist()
    days, cutoff = calendar[1:], calendar[0]
    if fault == 'hole':
        days = days[:5]+days[6:]
    elif fault == 'duplicate':
        days = days[:5]+days[4:]
    else:
        cutoff = days[0]
    with pytest.raises(ValueError, match='schedule|cutoff'):
        core.schedule(days, calendar, cutoff, 'outer')


def test_source_reader_projects_D_and_H_before_loading_labels(tmp_path, rows):
    source = pipeline.Source.__new__(pipeline.Source)
    source.root = tmp_path
    (tmp_path/'prepared').mkdir()
    (tmp_path/'evaluated').mkdir()
    inputs = rows.drop(columns=list(FINANCE))
    inputs.to_parquet(tmp_path/'prepared/inputs.parquet', index=False)
    finance = rows.loc[:, ['package_id', *KEY, *FINANCE]].copy()
    finance.loc[finance[KEY[0]].eq('2025-01-03'), 'label_information_end'] = pd.Timestamp('2025-02-10')
    future = finance.label_information_end.gt('2025-01-31')
    finance.loc[future, 'hypothetical_liquidation_net_bps'] = np.inf
    source.metadata = finance.loc[:, ['package_id', *KEY, 'label_information_end']]
    finance.loc[finance[KEY[0]].lt('2026-01-01')].to_parquet(source.finance_paths[0], index=False)
    finance.loc[finance[KEY[0]].ge('2026-01-01')].to_parquet(source.finance_paths[1], index=False)
    training = source.training('2025-01-31')
    assert len(training) == 80 and training[KEY[0]].max() == pd.Timestamp('2025-01-03')
    assert not np.isinf(training.hypothetical_liquidation_net_bps).any()
    assert training.loc[training[KEY[0]].eq('2025-01-03'), 'valuation_status'].eq('IMMATURE').all()


def test_future_label_metadata_cannot_be_relabelled_as_past(rows):
    metadata = rows.loc[:, ['package_id', *KEY, 'label_information_end']].copy()
    observed = rows.loc[:, ['package_id', *KEY, *FINANCE]].copy()
    observed['label_information_end'] = pd.Timestamp('2025-01-10')
    with pytest.raises(ValueError, match='maturity identity'):
        core.projected_finance(metadata, observed, '2025-01-31')


def test_missing_mature_finance_is_not_silent_cash(rows):
    metadata = rows.loc[:, ['package_id', *KEY, 'label_information_end']]
    observed = rows.iloc[1:].loc[:, ['package_id', *KEY, *FINANCE]]
    with pytest.raises(ValueError, match='mature finance disappeared'):
        core.projected_finance(metadata, observed, '2027-01-01')


def test_boundary_only_replaces_its_mature_original_rows(tmp_path, rows):
    source = pipeline.Source.__new__(pipeline.Source)
    source.root = tmp_path
    (tmp_path/'prepared').mkdir()
    (tmp_path/'evaluated').mkdir()
    finance = rows.loc[:, ['package_id', *KEY, *FINANCE]].copy()
    boundary = finance.loc[finance[KEY[0]].eq('2025-01-03')].copy()
    finance.loc[finance[KEY[0]].eq('2025-01-03'), 'valuation_status'] = 'IMMATURE'
    finance.loc[finance[KEY[0]].eq('2025-01-03'), 'hypothetical_liquidation_net_bps'] = np.nan
    finance.iloc[:80].to_parquet(source.finance_paths[0], index=False)
    finance.iloc[80:].to_parquet(source.finance_paths[1], index=False)
    source.metadata = finance.loc[:, ['package_id', *KEY, 'label_information_end']]
    before = source.finance('2025-01-02', '2025-01-03', '2025-01-09', boundary)
    after = source.finance('2025-01-02', '2025-01-03', '2025-01-10', boundary)
    assert before.loc[before[KEY[0]].eq('2025-01-03'), 'valuation_status'].eq('IMMATURE').all()
    assert after.valuation_status.eq('AVAILABLE').all() and len(after) == 80
    assert pd.read_parquet(source.finance_paths[0]).valuation_status.eq('IMMATURE').sum() == 40


def arms(rows):
    finance = rows.copy()
    finance['hypothetical_liquidation_net_bps'] = (200*finance.selection_effective_rank.gt(10)-finance.selection_effective_rank).astype(float)
    finance['valuation_gross_terminal_ratio'] = (1+finance.hypothetical_liquidation_net_bps/10000)*(1+POLICY['buy_bps']/10000)/(1-POLICY['sell_bps']/10000)
    p = rows.loc[:, IDENTITY].copy()
    p['advisory_rank'] = 21-p.selection_effective_rank
    p['selected'] = p.advisory_rank.le(10)
    p['predicted_relative_net_bps'] = p.selection_effective_rank.astype(float)
    p['no_rank_advice'] = False
    p['fold'] = p[KEY[0]].rank(method='dense').astype(int)
    static = p.copy()
    static['advisory_rank'] = static.selection_effective_rank
    static['selected'] = static.advisory_rank.le(10)
    finances = {i: finance.loc[p.fold.eq(i), ['package_id', *KEY, *FINANCE]] for i in range(1, 4)}
    return p, static, finances


def test_inner_positive_update_requires_native_and_static_comparators(rows):
    p, static, finance = arms(rows)
    report, _ = core.inner_report(p, static, finance)
    assert report['proceed_outer'] and report['positive_native_folds'] == 3
    identical, _ = core.inner_report(p, p, finance)
    assert not identical['proceed_outer'] and identical['candidate_minus_static_bps'] == 0


def test_inner_sparse_common_coverage_cannot_drop_unknown_day(rows):
    p, static, finance = arms(rows)
    finance[2] = finance[2].copy()
    finance[2].loc[:, 'valuation_status'] = 'UNKNOWN'
    finance[2].loc[:, ['hypothetical_liquidation_net_bps', 'valuation_gross_terminal_ratio', 'observed_gap_bps']] = np.nan
    finance[2].loc[:, 'exit_execution_status'] = 'UNKNOWN'
    report, daily = core.inner_report(p, static, finance)
    assert not report['proceed_outer'] and report['common_coverage'] == pytest.approx(2/3)
    assert len(daily) == 3 and daily.delta.isna().sum() == 1


@pytest.mark.parametrize('fault', ['missing', 'identity', 'order'])
def test_prediction_identity_and_full_original_order(rows, fault):
    p = rows.loc[:, IDENTITY].copy()
    if fault == 'missing':
        p = p.iloc[1:]
    elif fault == 'identity':
        p.loc[p.index[0], 'manifest_sha256'] = 'c'*64
    else:
        p = p.iloc[::-1]
    with pytest.raises(ValueError, match='identity'):
        core.assert_population(rows, p)


def claims_study(tmp_path):
    study = pipeline.Study.__new__(pipeline.Study)
    study.root, study.digest, study.started = tmp_path, 'a'*64, time.monotonic()
    study.plan = dict(schedules=dict(inner=[dict(key='inner1_00')], outer=[dict(key='outer_00'), dict(key='outer_01')]))
    return study


def test_interrupted_fit_cannot_refit_and_completed_inner_survives_outer(tmp_path):
    study = claims_study(tmp_path)
    study.claim('inner1_00')
    with pytest.raises(ValueError, match='explicit recovery'):
        study.claim('inner1_00')
    with pytest.raises(ValueError, match='incomplete'):
        study.fit_count()
    study.claim('inner1_00', complete=True)
    study.claim('outer_01')
    study.claim('outer_01', complete=True)
    assert study.fit_count() == 2
    assert study.fit_count(prefix='inner') == 1


def test_fit_claim_alias_and_unknown_fit_are_rejected(tmp_path):
    study = claims_study(tmp_path)
    with pytest.raises(ValueError, match='outside'):
        study.claim('outer_00')
    study.claim('inner1_00')
    study.claim('inner1_00', complete=True)
    folder = tmp_path/'fit_claims'
    (folder/'alias_started.json').write_bytes((folder/'inner1_00_started.json').read_bytes())
    (folder/'alias_finished.json').write_bytes((folder/'inner1_00_finished.json').read_bytes())
    with pytest.raises(ValueError, match='identity'):
        study.fit_count()


def test_completed_block_is_read_without_refitting_and_drift_fails(tmp_path):
    study = claims_study(tmp_path)
    spec = dict(key='inner1_00', first='2025-01-01', last='2025-01-02', cutoff='2024-12-31')
    location = 'blocks/inner1_00/trained'
    p = pd.DataFrame(dict(x=[1, 2]))
    digest = study.publish(location, 'trained', None, {'predictions.parquet': pipeline.parquet_bytes(p),
        'receipt.json': pipeline._json_bytes(dict(block=spec))})
    result, read_hash = pipeline.block(study, spec, None)
    assert result.equals(p) and digest == read_hash
    path = tmp_path/location/'receipt.json'
    path.write_text(json.dumps(dict(block='DRIFT')), encoding='utf-8')
    with pytest.raises(AdvisoryModelFirstError, match='hash mismatch'):
        pipeline.block(study, spec, None)


def test_rejected_inner_never_reads_outer_or_materializes_boundary(tmp_path, monkeypatch):
    study = claims_study(tmp_path)
    study.plan.update(experiment_id='test', source_commit='a'*40)
    study.source = SimpleNamespace(verify=lambda: None)
    study.node = lambda: None
    study.fit_count = lambda: 8
    study.register = lambda *a, **k: None
    study.publish = lambda *a, **k: 'receipt'
    monkeypatch.setattr(pipeline, 'Study', lambda root: study)
    monkeypatch.setattr(pipeline, 'inner', lambda s: (dict(proceed_outer=False), 'a'*64))
    monkeypatch.setattr(pipeline, 'boundary_labels', lambda *a: pytest.fail('outer boundary read after rejected inner'))
    result = pipeline.run(tmp_path)
    assert result['classification'] == 'INNER_PROTOCOL_NOT_SUPPORTED'
    assert result['physical_fit_count'] == 8 and not result['outer_past_mature_feedback']


def test_first_outer_prediction_precedes_boundary_outcome_read(tmp_path, monkeypatch):
    study = claims_study(tmp_path)
    study.node = lambda: None
    calls = []
    monkeypatch.setattr(pipeline, 'Study', lambda root: study)
    monkeypatch.setattr(pipeline, 'inner', lambda s: (dict(proceed_outer=True), 'a'*64))
    def first_block(s, spec, parent, **kwargs):
        assert spec['key'] == 'outer_00' and kwargs == dict(initial=True)
        calls.append('initial_predictions_frozen')
        return pd.DataFrame(), 'b'*64
    def boundary(s, parent):
        assert parent == 'b'*64
        calls.append('read_boundary_labels')
        raise RuntimeError('test boundary reached')
    monkeypatch.setattr(pipeline, 'block', first_block)
    monkeypatch.setattr(pipeline, 'boundary_labels', boundary)
    with pytest.raises(RuntimeError, match='test boundary reached'):
        pipeline.run(tmp_path)
    assert calls == ['initial_predictions_frozen', 'read_boundary_labels']


def test_model_packet_config_or_hash_drift_fails_before_loading():
    packet = dict(config=deepcopy(core.CONFIG), fit_versions={}, pickle_base64='YQ==', pickle_sha256='a'*64)
    with pytest.raises(ValueError, match='hash changed'):
        core.own_model(packet, {})
    packet['config']['alpha'] = 10
    with pytest.raises(ValueError, match='configuration'):
        core.own_model(packet, {})
