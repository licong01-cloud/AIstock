from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.advisory_forward.notifications import (
    CN, AdvisoryNotificationService, configured_channels, events_for_day,
)
from backend.services.advisory_forward.notification_transport import (
    AdvisoryNotificationTransport, ExternalNotificationDeliveryError,
)


@pytest.fixture(autouse=True)
def no_external_network(monkeypatch):
    import socket
    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: pytest.fail('real network forbidden'))


@pytest.fixture
def transport():
    obj = AdvisoryNotificationTransport.__new__(AdvisoryNotificationTransport)
    obj.config = dict(email_enabled=True, smtp_server='invalid.test', smtp_port=465,
                      email_from='from@test.invalid', email_to='to@test.invalid',
                      email_password=uuid4().hex, webhook_enabled=True,
                      webhook_url='https://invalid.test/secret', webhook_type='dingtalk',
                      webhook_keyword='test')
    return obj


def published(**extra):
    return dict(program_id='p1', program_name='LSTM', binding_version_id='b1',
                list_version_id='v1', target_trade_date=date(2026, 11, 16),
                publication_status='PUBLISHED', published_at=datetime(2026, 11, 13, 21, 20, tzinfo=CN),
                settlement_status='NOT_DUE', **extra)


def test_email_strict_ssl_tls_acceptance_and_legacy_compatibility(monkeypatch, transport):
    import smtplib
    calls = []

    class SMTP:
        refused = {}

        def __init__(self, *args, **kwargs):
            calls.append(('smtp', args, kwargs))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def starttls(self):
            calls.append('tls')

        def login(self, *args):
            pass

        def send_message(self, msg):
            assert '<script>' not in msg.as_string()
            return self.refused

    class SSL(SMTP):
        pass

    monkeypatch.setattr(smtplib, 'SMTP', SMTP)
    monkeypatch.setattr(smtplib, 'SMTP_SSL', SSL)
    assert transport.send_external_message('email', '荐股', '<script>') is True
    assert calls[0][2] == {'timeout': 10} and 'tls' not in calls
    calls.clear()
    transport.config['smtp_port'] = 587
    assert transport.send_external_message('email', '荐股', 'body') is True
    assert 'tls' in calls
    SMTP.refused = {'to@test.invalid': (550, b'rejected')}
    assert transport.send_external_message('email', '荐股', 'body') is False
    calls.clear()
    # Portfolio callers retain the original constructor and return contract.
    assert transport._send_custom_email('portfolio', '<p>x</p>', 'x') is True
    assert calls[0][2] == {} and 'tls' in calls


def test_email_failure_is_not_ui_success(monkeypatch, transport):
    import smtplib
    monkeypatch.setattr(smtplib, 'SMTP_SSL', lambda *a, **k: (_ for _ in ()).throw(OSError('secret')))
    monkeypatch.setattr(transport, '_show_streamlit_notification', lambda *a: pytest.fail('UI fallback'))
    with pytest.raises(ExternalNotificationDeliveryError, match='EMAIL_TRANSPORT_ERROR'):
        transport.send_external_message('email', 'x', 'y')
    transport.config['email_password'] = ''
    assert transport.send_external_message('email', 'x', 'y') is False


@pytest.mark.parametrize('kind,key', [('dingtalk', 'errcode'), ('feishu', 'code')])
@pytest.mark.parametrize('code,expected', [(0, True), (1, False), (False, False), (None, False)])
def test_webhook_real_business_acceptance(monkeypatch, transport, kind, key, code, expected):
    transport.config['webhook_type'] = kind
    monkeypatch.setattr(transport, '_post_webhook', lambda data: SimpleNamespace(
        status_code=200, json=lambda: {key: code}))
    assert transport.send_external_message('webhook', 'x', 'y') is expected


def test_shared_webhook_http_keeps_existing_monitor_contract(monkeypatch, transport):
    import requests
    calls = []
    monkeypatch.setattr(requests, 'post', lambda *a, **k: (
        calls.append((a, k)) or SimpleNamespace(status_code=200, json=lambda: {'errcode': 0, 'code': 0})))
    n = dict(symbol='000001.SZ', name='test', type='price', message='body', triggered_at='now')
    assert transport._send_dingtalk_webhook(n) is True
    assert transport._send_feishu_webhook(n) is True
    assert transport.send_external_message('webhook', '荐股', 'body') is True
    assert all(c[1]['timeout'] == 10 for c in calls)
    assert calls[0][1]['json']['msgtype'] == 'markdown'
    assert calls[1][1]['json']['msg_type'] == 'interactive'
    assert calls[2][1]['json']['msgtype'] == 'text'


@pytest.mark.parametrize('mode', ['http_error', 'bad_json', 'network_error', 'unsupported'])
def test_webhook_failure_never_becomes_sent(monkeypatch, transport, mode):
    def send(data):
        if mode == 'network_error':
            raise OSError('secret URL')
        def decode():
            raise ValueError('bad json')
        return SimpleNamespace(status_code=503 if mode == 'http_error' else 200, json=decode)
    monkeypatch.setattr(transport, '_post_webhook', send)
    if mode == 'unsupported':
        transport.config['webhook_type'] = 'unknown'
    if mode in {'bad_json', 'network_error'}:
        with pytest.raises(ExternalNotificationDeliveryError, match='WEBHOOK_TRANSPORT_ERROR'):
            transport.send_external_message('webhook', 'x', 'y')
    else:
        assert transport.send_external_message('webhook', 'x', 'y') is False


def test_legacy_send_notification_still_uses_original_fallback(monkeypatch, transport):
    calls = []
    monkeypatch.setattr(transport, '_send_email_notification', lambda n: False)
    monkeypatch.setattr(transport, '_send_webhook_notification', lambda n: False)
    monkeypatch.setattr(transport, '_show_streamlit_notification', lambda n: calls.append(n))
    assert transport.send_notification({'symbol': 'legacy'}) is True
    assert calls == [{'symbol': 'legacy'}]


def test_day_events_unique_publication_summary_overdue_and_unknown():
    now = datetime(2026, 11, 13, 22, tzinfo=CN)
    rows = [published(), dict(program_id='p2', program_name='multi alpha', publication_status=None)]
    es = events_for_day(now, date(2026, 11, 16), rows, 'http://localhost:3000')
    assert [e['kind'] for e in es] == ['PUBLISHED', 'SUMMARY', 'OVERDUE']
    assert 'UNKNOWN' in es[-1]['payload']['message']
    assert '/paper-v2/advisory?program_id=p1' in es[0]['payload']['message']
    assert 'NOT_DUE' in es[0]['payload']['message']
    assert es[0]['event_key'] == events_for_day(now.replace(hour=23), date(2026, 11, 16), rows, 'http://localhost:3000')[0]['event_key']
    row2 = {**published(), 'binding_version_id': 'b2'}
    assert es[0]['event_key'] != events_for_day(now, date(2026, 11, 16), [row2], 'http://localhost:3000')[0]['event_key']
    assert len(events_for_day(now.replace(hour=21, minute=44), date(2026, 11, 16), rows, 'http://localhost:3000')) == 1
    assert len(events_for_day(now.replace(hour=21, minute=45), date(2026, 11, 16), rows, 'http://localhost:3000')) == 2
    assert not events_for_day(now, None, [], 'http://localhost:3000')


@pytest.mark.parametrize('change', [{'list_version_id': None}, {'published_at': None},
                                   {'published_at': datetime(2026, 11, 13, 21)},
                                   {'target_trade_date': date(2026, 11, 13)}])
def test_published_identity_fail_closed(change):
    with pytest.raises(ValueError, match='IDENTITY_INVALID'):
        events_for_day(datetime(2026, 11, 13, 22, tzinfo=CN), date(2026, 11, 16),
                       [{**published(), **change}], 'http://localhost:3000')


@pytest.mark.parametrize('url', ['ftp://host', 'https://user:secret@host', 'https://host/?token=x',
                               'https://host/#fragment', 'https://host/extra'])
def test_notification_base_url_rejects_credentials_or_hidden_configuration(url):
    with pytest.raises(ValueError, match='BASE_URL_INVALID'):
        events_for_day(datetime(2026, 11, 13, 22, tzinfo=CN), date(2026, 11, 16), [published()], url)


def test_disabled_and_unconfigured_do_not_touch_repository(monkeypatch, transport):
    monkeypatch.delenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', raising=False)
    assert AdvisoryNotificationService(repository=object(), transport=object()).run_once() == {'status': 'DISABLED'}
    monkeypatch.setenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', 'true')
    transport.config['email_enabled'] = transport.config['webhook_enabled'] = False
    assert configured_channels(transport.config) == []
    assert AdvisoryNotificationService(repository=object(), transport=transport).run_once() == {'status': 'NOT_CONFIGURED'}


def test_service_bounded_dispatch_and_explicit_failure(monkeypatch, transport):
    monkeypatch.setenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', 'true')
    now = datetime(2026, 11, 13, 22, tzinfo=CN)

    class Repository:
        def __init__(self):
            self.queued, self.finished = [], []

        def snapshot(self, day):
            assert day == now.date()
            return date(2026, 11, 16), [published()]

        def enqueue(self, e, ch, now):
            self.queued.append((e, ch))
            return True

        def claim(self, now, channels):
            return dict(event_key='key', channel='email', payload={'subject': 'test', 'message': 'x'})

        def finish(self, r, **kwargs):
            self.finished.append(kwargs)
            return 'SENT' if kwargs['accepted'] else 'PENDING'

    repo = Repository()
    monkeypatch.setattr(transport, 'send_external_message', lambda *a: (_ for _ in ()).throw(OSError('secret')))
    result = AdvisoryNotificationService(repo, transport, lambda: now).run_once()
    assert len(repo.finished) == 4
    assert all(r['error_code'] == 'EXTERNAL_DELIVERY_EXCEPTION' for r in repo.finished)
    assert result['queued'] == 4  # Two events, two separately tracked channels.
    assert 'secret' not in str(result)
