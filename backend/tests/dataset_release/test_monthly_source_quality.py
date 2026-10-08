from __future__ import annotations

from copy import deepcopy
from datetime import date
import hashlib

import pytest

from backend.services.dataset_release.monthly_source_quality import (
    accepted_parity_warning,
    acceptance_file_sha256,
    validate_source_quality_acceptance,
    load_bound_source_quality,
)
from backend.services.dataset_release.monthly_frozen_source_audit import GateCounter, audit_month_rows
from backend.services.dataset_release.monthly_source_audit import cn_a_share_minute_labels
from backend.services.dataset_release.monthly_unified import SOURCE_GATES


OP = 'dmr_' + 'b' * 32
MANIFEST = 'a' * 64
DAY = date(2026, 9, 14)


def acceptance(tmp_path):
    evidence = tmp_path / 'provider-evidence.json'
    evidence.write_bytes(b'{"provider":"independent"}\n')
    return {
        'schema_version': 'aistock_monthly_source_quality_acceptance_v1',
        'operation_id': OP,
        'predecessor_dataset_manifest_sha256': MANIFEST,
        'target_cutoff': '2026-09-30',
        'evidence_refs': [{'path': str(evidence.absolute()), 'sha256': hashlib.sha256(evidence.read_bytes()).hexdigest(), 'size': evidence.stat().st_size}],
        'entries': [{'symbol': '688799.SH', 'trade_date': DAY.isoformat(),
                     'reason_code': 'KNOWN_PROVIDER_PRICE_VARIANCE',
                     'mismatches': {'open': {'daily': 10.0, 'minute': 11.0}}}],
    }


def validate(value):
    return validate_source_quality_acceptance(value, operation_id=OP,
        predecessor_manifest_sha256=MANIFEST, target_cutoff=date(2026, 9, 30))


def test_acceptance_binds_exact_values_operation_month_and_evidence(tmp_path):
    value = validate(acceptance(tmp_path))
    warning = accepted_parity_warning(value, symbol='688799.SH', trade_date=DAY,
        mismatches={'open': {'daily': 10.0, 'minute': 11.0, 'delta': 1.0}})
    assert warning['authority_sha256'] == acceptance_file_sha256(value)
    assert warning['data_modified'] is False
    assert warning['reason_code'] == 'KNOWN_PROVIDER_PRICE_VARIANCE'
    for symbol, stamp, mismatches in [
        ('000001.SZ', DAY, {'open': {'daily': 10.0, 'minute': 11.0}}),
        ('688799.SH', date(2026, 9, 15), {'open': {'daily': 10.0, 'minute': 11.0}}),
        ('688799.SH', DAY, {'open': {'daily': 10.0, 'minute': 11.01}}),
        ('688799.SH', DAY, {'open': {'daily': 10.0, 'minute': 11.0}, 'amount': {'daily': 1.0, 'minute': 2.0}}),
    ]:
        assert accepted_parity_warning(value, symbol=symbol, trade_date=stamp, mismatches=mismatches) is None


@pytest.mark.parametrize('mutation', ['operation', 'manifest', 'month', 'duplicate', 'unknown', 'null', 'nan', 'amount', 'evidence'])
def test_rejects_unapproved_scope_and_bad_evidence(tmp_path, mutation):
    value = acceptance(tmp_path)
    if mutation == 'operation': value['operation_id'] = 'dmr_' + 'c' * 32
    if mutation == 'manifest': value['predecessor_dataset_manifest_sha256'] = 'c' * 64
    if mutation == 'month': value['entries'][0]['trade_date'] = '2026-08-31'
    if mutation == 'duplicate': value['entries'].append(deepcopy(value['entries'][0]))
    if mutation == 'unknown': value['entries'][0]['reason_code'] = 'ANY_MISSING_DATA'
    if mutation == 'null': value['entries'][0]['mismatches']['open']['daily'] = None
    if mutation == 'nan': value['entries'][0]['mismatches']['open']['daily'] = float('nan')
    if mutation == 'amount': value['entries'][0]['mismatches'] = {'amount': {'daily': 1, 'minute': 2}}
    if mutation == 'evidence': value['evidence_refs'][0]['sha256'] = 'f' * 64
    with pytest.raises(ValueError): validate(value)


def _rows(*, bars=240, duplicate=False):
    raw = {'ts_code': '688799.SH', 'trade_date': DAY.isoformat(),
           'open_li': 10000, 'high_li': 11000, 'low_li': 10000, 'close_li': 10000,
           'volume_hand': 240, 'amount_li': 240000}
    minutes = [{**raw, 'trade_time': stamp.isoformat(), 'volume_hand': 1, 'amount_li': 1000,
                'open_li': 11000 if i == 0 else 10000}
               for i, stamp in enumerate(cn_a_share_minute_labels(DAY)[:bars])]
    if duplicate: minutes.append(dict(minutes[0]))
    return {'kline_daily_raw': [raw], 'kline_minute_raw': minutes}


@pytest.mark.parametrize('bars,duplicate,approved,invalid,warnings', [
    (240, False, True, 0, 1), (240, False, False, 1, 0),
    (239, False, True, 1, 0), (240, True, True, 1, 0),
])
def test_only_complete_real_minute_day_can_emit_warning(tmp_path, bars, duplicate, approved, invalid, warnings):
    counters = {name: GateCounter(name) for name in SOURCE_GATES}
    audit_month_rows(_rows(bars=bars, duplicate=duplicate), sessions=(DAY,),
        pools={'stock_universe': {DAY: {'688799.SH'}}}, gates=counters,
        authority_sha256=MANIFEST, minute_start=DAY,
        quality_acceptance=validate(acceptance(tmp_path)) if approved else None)
    minute = counters['minute_price']
    assert (minute.expected_count, minute.observed_count, minute.missing_count) == (1, 1, 0)
    assert minute.invalid_count == invalid
    assert len(minute.quality_warning_refs) == warnings
    assert minute.duplicate_count == int(duplicate)


def test_quality_binding_is_idempotent_and_does_not_rewrite_repairs_or_activate(tmp_path):
    from backend.tests.dataset_release.test_monthly_unified_v2 import _service, _request, Pipeline
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseConflict
    service = _service(tmp_path, Pipeline())
    operation_id = service.submit(_request(), principal='operator')['operation_id']
    plan = service.store.read_plan(operation_id)
    repairs = {'deferred_financing': 'existing-exact-binding'}
    service.store.replace_plan(operation_id, {**plan, 'monthly_repair_inputs': repairs})
    value = acceptance(tmp_path)
    value['operation_id'] = operation_id
    value['predecessor_dataset_manifest_sha256'] = plan['predecessor']['dataset_manifest_sha256']
    active_before = service.active_profile.read_bytes()
    first = service.bind_source_quality_inputs(operation_id, inputs=value, principal='operator')
    second = service.bind_source_quality_inputs(operation_id, inputs=value, principal='operator')
    assert first == second
    assert first['publication_allowed'] is False
    plan = service.store.read_plan(operation_id)
    assert plan['monthly_repair_inputs'] == repairs
    assert load_bound_source_quality(plan, operation_id=operation_id) == first['inputs']
    assert service.active_profile.read_bytes() == active_before
    changed = deepcopy(value)
    changed['entries'][0]['mismatches']['open']['minute'] = 12.0
    with pytest.raises(MonthlyReleaseConflict, match='already bound'):
        service.bind_source_quality_inputs(operation_id, inputs=changed, principal='operator')
    path = service.store.operation_root(operation_id) / plan['monthly_source_quality_inputs_ref']['id']
    path.write_bytes(path.read_bytes() + b' ')
    with pytest.raises(ValueError, match='size differs'):
        load_bound_source_quality(plan, operation_id=operation_id)


def test_quality_binding_rejects_stale_active_and_sealed_source(tmp_path):
    from backend.tests.dataset_release.test_monthly_unified_v2 import _service, _request, Pipeline
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseConflict
    service = _service(tmp_path, Pipeline())
    operation_id = service.submit(_request(), principal='operator')['operation_id']
    plan = service.store.read_plan(operation_id)
    value = acceptance(tmp_path)
    value['operation_id'] = operation_id
    value['predecessor_dataset_manifest_sha256'] = plan['predecessor']['dataset_manifest_sha256']
    service.active_profile.write_bytes(service.active_profile.read_bytes() + b' ')
    with pytest.raises(MonthlyReleaseConflict, match='predecessor changed'):
        service.bind_source_quality_inputs(operation_id, inputs=value, principal='operator')
    service.store.replace_plan(operation_id, {**plan, 'plan_state': 'SOURCE_SNAPSHOT_FROZEN'})
    with pytest.raises(MonthlyReleaseConflict, match='before any SOURCE seal'):
        service.bind_source_quality_inputs(operation_id, inputs=value, principal='operator')


def test_accepted_key_with_nonfinite_minute_remains_blocked(tmp_path):
    counters = {name: GateCounter(name) for name in SOURCE_GATES}
    rows = _rows()
    rows['kline_minute_raw'][50]['amount_li'] = None
    audit_month_rows(rows, sessions=(DAY,), pools={'stock_universe': {DAY: {'688799.SH'}}},
        gates=counters, authority_sha256=MANIFEST, minute_start=DAY,
        quality_acceptance=validate(acceptance(tmp_path)))
    assert counters['minute_price'].invalid_count == 1
    assert counters['minute_price'].quality_warning_refs == []


def test_warning_cannot_close_source_without_acceptance_bytes_pin(tmp_path):
    from types import SimpleNamespace
    from backend.services.dataset_release.monthly_source_audit import SourceGateEvidence
    from backend.services.dataset_release.monthly_source_producer import _require_source_provenance, MonthlySourceProducerError
    value = validate(acceptance(tmp_path))
    warning = accepted_parity_warning(value, symbol='688799.SH', trade_date=DAY,
        mismatches={'open': {'daily': 10.0, 'minute': 11.0}})
    gate = SourceGateEvidence('minute_price', 'snapshot', 'expectation', 'readback', 1, 1,
        quality_warning_refs=(warning,))
    read_set = SimpleNamespace(gates=(gate,), changes=(), repair_receipts=())
    pins = {'expectation': 'd' * 64, 'readback': 'e' * 64}
    with pytest.raises(MonthlySourceProducerError, match='quality acceptance is not pinned'):
        _require_source_provenance(read_set, artifact_hashes=pins)
    _require_source_provenance(read_set, artifact_hashes={**pins, 'acceptance': acceptance_file_sha256(value)})


def test_warning_status_is_not_a_clean_source_claim_and_does_not_waive_missing_day(tmp_path):
    from backend.services.dataset_release.monthly_source_audit import SourceGateEvidence, close_source_audit, MonthlySourceAuditError
    value = validate(acceptance(tmp_path))
    warning = accepted_parity_warning(value, symbol='688799.SH', trade_date=DAY,
        mismatches={'open': {'daily': 10.0, 'minute': 11.0}})
    gates = [SourceGateEvidence(name, 'snapshot', 'expectation', 'readback', 1, 1,
        quality_warning_refs=(warning,) if name == 'minute_price' else ()) for name in SOURCE_GATES]
    audit = close_source_audit(cutoff=date(2026, 9, 30), predecessor_cutoff=date(2026, 8, 31), gates=gates, changes=())
    assert audit['quality_status'] == 'ACCEPTED_WITH_WARNINGS'
    assert audit['quality_warning_count'] == 1
    gates[1] = SourceGateEvidence('daily_price', 'snapshot', 'expectation', 'readback', 1, 0, unexplained_missing_count=1)
    with pytest.raises(MonthlySourceAuditError, match='blocking counts'):
        close_source_audit(cutoff=date(2026, 9, 30), predecessor_cutoff=date(2026, 8, 31), gates=gates, changes=())
