"""Shared TDX integration: no live service, database, or current quotes."""
import datetime as dt
import uuid
from unittest.mock import Mock

import backend.data_service.tdx_adapter as adapter
import backend.ingestion.tdx_scheduler as scheduler


def response(data):
    return Mock(json=Mock(return_value=data), raise_for_status=Mock())


def test_snapshot_uses_current_close_not_previous_close(monkeypatch):
    monkeypatch.setattr(adapter.requests, 'post', Mock(return_value=response({
        'code': 0, 'data': [{'Code': '688526', 'Exchange': 1,
                            'K': {'Last': 12000, 'Close': 12800}}]})))
    got = adapter.fetch_realtime_snapshot_tdx(['688526.SH'])
    assert got.loc['688526.SH', 'close'] == 12.8


def run_scheduler(monkeypatch, reply, options):
    obj = object.__new__(scheduler.TDXScheduler)
    obj._fetchall = Mock(side_effect=lambda sql, *args: [{'latest': dt.date(2026, 10, 7)}]
                        if 'latest' in sql else [{'mx': dt.date(2026, 10, 7)}]
                        if ' AS mx' in sql else [{'nt': dt.date(2026, 10, 8)}]
                        if ' AS nt' in sql else [{'status': 'success'}])
    obj._execute = Mock()
    obj._log_ingestion_run = Mock()
    obj._update_ingestion_schedule = Mock()
    obj._record_refresh_audit_from_table_range = Mock()
    post = Mock(return_value=response(reply))
    monkeypatch.setattr(scheduler.requests, 'post', post)
    monkeypatch.setattr(scheduler.requests, 'get', Mock(return_value=response(
        {'code': 0, 'data': {'status': 'success'}})))
    monkeypatch.setattr(scheduler.time, 'sleep', Mock())
    obj._run_go_incremental(uuid.uuid4(), 'schedule', 'kline_minute_raw', 'test', options)
    return obj, post


def test_explicit_september_window_is_not_replaced_by_global_max(monkeypatch):
    obj, post = run_scheduler(monkeypatch, {'code': 0, 'data': {'task_id': 'exact'}},
                             {'start_date': '2026-09-01', 'end_date': '2026-09-30',
                              'codes': ['688526.SH']})
    payload = post.call_args.kwargs['json']
    assert payload['start_time'].startswith('2026-09-01')
    assert payload['end_time'].startswith('2026-09-30')
    assert payload['codes'] == ['688526.SH']
    assert obj._update_ingestion_schedule.call_args.kwargs['last_status'] == 'success'


def test_missing_task_identity_is_failure_even_if_db_says_success(monkeypatch):
    obj, _ = run_scheduler(monkeypatch, {'code': 0, 'data': {}},
                            {'start_date': '2026-09-01', 'end_date': '2026-09-30'})
    assert obj._update_ingestion_schedule.call_args.kwargs['last_status'] == 'failed'


def test_partial_latest_day_is_replayed_not_skipped(monkeypatch):
    obj, post = run_scheduler(monkeypatch, {'code': 0, 'data': {'task_id': 'exact'}}, {})
    assert post.call_count == 1
    assert post.call_args.kwargs['json']['start_time'].startswith('2026-10-07')


def test_minute_missing_price_is_not_fabricated_zero(monkeypatch):
    import pytest
    monkeypatch.setattr(adapter.requests, 'get', Mock(return_value=response({
        'code': 0, 'data': {'list': [{'Time': '2026-09-10T11:21:00+08:00',
                                    'High': 12800, 'Low': 12800, 'Close': 12800, 'Volume': 0}]}})))
    with pytest.raises(RuntimeError):
        adapter.fetch_minute_kline_tdx('688526.SH', dt.date(2026, 9, 10))


def test_provider_minute_is_bounded_and_uses_li_and_optional_shares(monkeypatch):
    get = Mock(return_value=response({'code': 0, 'data': {'list': [{
        'Time': '2026-09-10T11:21:00+08:00', 'Open': 12800, 'High': 12800,
        'Low': 12800, 'Close': 12800, 'Volume': 0, 'Amount': 13000}]}}))
    monkeypatch.setattr(adapter.requests, 'get', get)
    bars = adapter.fetch_minute_kline_tdx('688526.SH', dt.date(2026, 9, 10))
    assert get.call_args.kwargs['params']['start_date'] == '2026-09-10'
    assert get.call_args.kwargs['params']['end_date'] == '2026-09-10'
    assert bars[0]['close'] == 12.8 and bars[0]['amount'] == 13
    assert 'volume_shares' not in bars[0]
