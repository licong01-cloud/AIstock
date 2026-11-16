from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.advisory_forward.notifications import (
    CN, AdvisoryNotificationService, completion_events_for_day, configured_channels, events_for_day,
)
from backend.services.advisory_forward.notification_repository import FEISHU_COMPLETION_ROUTE
from backend.services.advisory_forward.notification_transport import (
    AdvisoryNotificationTransport, ExternalNotificationDeliveryError,
)


@pytest.fixture(autouse=True)
def no_external_network(monkeypatch):
    import socket
    import requests
    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: pytest.fail('real network forbidden'))
    monkeypatch.setattr(requests.sessions.Session, 'request', lambda *a, **k: pytest.fail('real HTTP forbidden'))
    for key in ('AISTOCK_ADVISORY_FEISHU_ENABLED', 'AISTOCK_ADVISORY_FEISHU_WEBHOOK_URL',
                'AISTOCK_ADVISORY_FEISHU_SIGNING_SECRET'):
        monkeypatch.delenv(key, raising=False)


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


def test_completion_notice_is_brief_daily_and_tracks_original_publications():
    now = datetime(2026, 11, 13, 21, 30, tzinfo=CN)
    rows = [published(), {**published(), 'program_id': 'p2', 'list_version_id': 'v2'}]
    event = completion_events_for_day(now, date(2026, 11, 16), rows)[0]
    assert event['kind'] == 'SUMMARY'
    assert event['payload']['message'] == '2026-11-13 荐股任务已完成。'
    assert event['payload']['subject'] == 'AIstock'
    assert event['payload']['delivery_route'] == FEISHU_COMPLETION_ROUTE
    assert event['expires_at'] == datetime(2026, 11, 16, 9, 30, tzinfo=CN)
    later = completion_events_for_day(now.replace(hour=23), date(2026, 11, 16), list(reversed(rows)))[0]
    assert later['event_key'] == event['event_key']
    assert later['payload'] == event['payload']
    changed = completion_events_for_day(now, date(2026, 11, 16), [{**rows[0], 'list_version_id': 'v3'}, rows[1]])[0]
    assert changed['event_key'] == event['event_key']  # No second completion notice that day.
    assert changed['payload']['completion_roster_sha256'] != event['payload']['completion_roster_sha256']
    assert set(event['payload']) == {
        'schema_version', 'delivery_route', 'completion_roster_sha256', 'subject', 'message',
    }


@pytest.mark.parametrize('state', [None, 'UNKNOWN', 'WAITING_DATA', 'FAILED', 'NOT_STARTED'])
def test_partial_completion_never_becomes_success(state):
    rows = [published(), {'program_id': 'p2', 'publication_status': state}]
    assert not completion_events_for_day(datetime(2026, 11, 13, 23, tzinfo=CN), date(2026, 11, 16), rows)


def test_completion_has_no_empty_or_nontrading_day_notice():
    now = datetime(2026, 11, 13, 22, tzinfo=CN)
    assert not completion_events_for_day(now, date(2026, 11, 16), [])
    assert not completion_events_for_day(now, None, [])
    with pytest.raises(ValueError, match='timezone-aware'):
        completion_events_for_day(now.replace(tzinfo=None), date(2026, 11, 16), [published()])
    with pytest.raises(ValueError, match='TARGET_ORDER'):
        completion_events_for_day(now, now.date(), [published()])


@pytest.mark.parametrize('change', [
    {'program_id': None}, {'binding_version_id': None}, {'list_version_id': None},
    {'published_at': None}, {'published_at': datetime(2026, 11, 13, 21)},
    {'published_at': datetime(2026, 11, 13, 23, tzinfo=CN)},
    {'published_at': datetime(2026, 11, 12, 21, tzinfo=CN)},
    {'target_trade_date': date(2026, 11, 17)},
])
def test_completion_requires_real_same_day_publication_identity(change):
    with pytest.raises(ValueError, match='IDENTITY_INVALID'):
        completion_events_for_day(datetime(2026, 11, 13, 22, tzinfo=CN), date(2026, 11, 16), [{**published(), **change}])


def test_completion_roster_cannot_double_count_programs_or_exceed_source_bound():
    now = datetime(2026, 11, 13, 22, tzinfo=CN)
    with pytest.raises(ValueError, match='ROSTER_INVALID'):
        completion_events_for_day(now, date(2026, 11, 16), [published(), published()])
    with pytest.raises(ValueError, match='ROSTER_INVALID'):
        completion_events_for_day(now, date(2026, 11, 16), [{**published(), 'program_id': str(i)} for i in range(101)])


def test_dedicated_feishu_configuration_and_signed_wire_contract(monkeypatch, transport):
    from backend.services.advisory_forward import notification_transport
    original = dict(transport.config)
    monkeypatch.setenv('AISTOCK_ADVISORY_FEISHU_ENABLED', 'true')
    monkeypatch.setenv('AISTOCK_ADVISORY_FEISHU_WEBHOOK_URL', 'https://open.feishu.cn/open-apis/bot/v2/hook/test-only')
    monkeypatch.setenv('AISTOCK_ADVISORY_FEISHU_SIGNING_SECRET', 'demo')
    sender = AdvisoryNotificationTransport(shared=transport)
    assert transport.config == original
    assert sender.config is not transport.config
    assert sender.config['email_enabled'] == original['email_enabled']
    assert sender.config['webhook_type'] == 'feishu'
    assert sender.config['advisory_feishu_completion_only'] is True
    monkeypatch.setattr(notification_transport.time, 'time', lambda: 1599360473)
    payloads = []
    monkeypatch.setattr(sender, '_post_webhook', lambda payload: (
        payloads.append(payload) or SimpleNamespace(status_code=200, json=lambda: {'code': 0})))
    assert sender.send_external_message('webhook', 'AIstock', '2026-11-13 荐股任务已完成。') is True
    assert payloads == [{
        'msg_type': 'text', 'content': {'text': 'AIstock\n2026-11-13 荐股任务已完成。'},
        'timestamp': '1599360473', 'sign': 'l1N0gAcBjdwBvGm1xMjOF0XSyaLRpR7tuO5dHfhAYc8=',
    }]
    assert 'demo' not in str(payloads)


def test_dedicated_feishu_off_keeps_shared_legacy_configuration(transport):
    sender = AdvisoryNotificationTransport(shared=transport)
    assert sender.config is transport.config
    assert sender.config['webhook_type'] == 'dingtalk'


def test_global_feishu_cannot_enable_advisory_details_without_dedicated_switch(transport):
    transport.config['webhook_type'] = 'feishu'
    assert configured_channels(transport.config) == ['email']
    transport.config['advisory_feishu_completion_only'] = True
    assert configured_channels(transport.config) == ['email', 'webhook']


def test_hundred_completed_programs_still_produce_one_constant_size_message():
    now = datetime(2026, 11, 13, 22, tzinfo=CN)
    rows = [{**published(), 'program_id': str(i)} for i in range(100)]
    events = completion_events_for_day(now, date(2026, 11, 16), rows)
    assert len(events) == 1
    assert events[0]['payload']['message'] == '2026-11-13 荐股任务已完成。'


@pytest.mark.parametrize('url', [
    '', 'https://invalid.test/hook', 'http://open.feishu.cn/open-apis/bot/v2/hook/test',
    'https://open.feishu.cn/open-apis/bot/v2/hook/',
    'https://user:password@open.feishu.cn/open-apis/bot/v2/hook/test',
    'https://open.feishu.cn/open-apis/bot/v2/hook/test?secret=hidden',
    'https://open.feishu.cn:bad/open-apis/bot/v2/hook/test',
])
def test_missing_or_invalid_dedicated_endpoint_never_falls_back(monkeypatch, transport, url):
    monkeypatch.setenv('AISTOCK_ADVISORY_FEISHU_ENABLED', 'true')
    monkeypatch.setenv('AISTOCK_ADVISORY_FEISHU_WEBHOOK_URL', url)
    sender = AdvisoryNotificationTransport(shared=transport)
    sender.config['email_enabled'] = False
    monkeypatch.setenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', 'true')
    result = AdvisoryNotificationService(repository=object(), transport=sender).run_once()
    assert result['status'] == 'NOT_CONFIGURED'
    assert result['feishu_completion']['status'] == 'NOT_CONFIGURED'
    assert result['feishu_completion']['reason_code'].startswith('FEISHU_WEBHOOK_URL_')
    assert transport.config['webhook_type'] == 'dingtalk'


def test_unsigned_feishu_and_encoded_payload_bound(monkeypatch, transport):
    transport.config['webhook_type'] = 'feishu'
    calls = []
    monkeypatch.setattr(transport, '_post_webhook', lambda payload: (
        calls.append(payload) or SimpleNamespace(status_code=200, json=lambda: {'code': 0})))
    assert transport.send_external_message('webhook', 'AIstock', 'completed')
    assert 'sign' not in calls[0] and 'timestamp' not in calls[0]
    with pytest.raises(ExternalNotificationDeliveryError, match='FEISHU_MESSAGE_TOO_LARGE'):
        transport.send_external_message('webhook', 'AIstock', '测'*10000)
    assert len(calls) == 1


def test_completion_dispatch_is_daily_brief_and_does_not_change_email(monkeypatch, transport):
    monkeypatch.setenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', 'true')
    transport.config.update(webhook_type='feishu', advisory_feishu_completion_only=True)
    now = datetime(2026, 11, 13, 22, tzinfo=CN)

    class Repository:
        def __init__(self):
            self.events, self.sent, self.claim_routes = {}, set(), []

        def snapshot(self, day):
            return date(2026, 11, 16), [published()]

        def enqueue(self, event, channel, observed):
            key = (event['event_key'], channel)
            if key in self.events:
                return False
            self.events[key] = dict(event, channel=channel)
            return True

        def claim(self, observed, channels, *, webhook_route):
            self.claim_routes.append(webhook_route)
            return next((e for key, e in self.events.items() if key not in self.sent), None)

        def finish(self, row, *, accepted, **kwargs):
            assert accepted
            self.sent.add((row['event_key'], row['channel']))
            return 'SENT'

    repo, calls = Repository(), []
    monkeypatch.setattr(transport, 'send_external_message', lambda *args: calls.append(args) or True)
    service = AdvisoryNotificationService(repo, transport, lambda: now)
    first = service.run_once()
    assert first['feishu_completion']['status'] == 'EVENT_READY'
    assert first['queued'] == 3  # Original two email events, one brief Feishu event.
    assert len([row for row in calls if row[0] == 'webhook']) == 1
    assert ('webhook', 'AIstock', '2026-11-13 荐股任务已完成。') in calls
    assert any(row[0] == 'email' and 'LSTM' in row[2] for row in calls)
    assert set(repo.claim_routes) == {FEISHU_COMPLETION_ROUTE}
    assert service.run_once()['queued'] == 0
    assert len(calls) == 3


def test_partial_programs_leave_feishu_waiting_even_after_overdue_hour(monkeypatch, transport):
    monkeypatch.setenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', 'true')
    transport.config.update(email_enabled=False, webhook_type='feishu', advisory_feishu_completion_only=True)

    class Repository:
        def snapshot(self, day):
            return date(2026, 11, 16), [published(), {'program_id': 'p2', 'publication_status': 'WAITING_DATA'}]

        def enqueue(self, *args):
            pytest.fail('incomplete daily programs must not notify Feishu')

        def claim(self, *args, **kwargs):
            assert kwargs['webhook_route'] == FEISHU_COMPLETION_ROUTE
            return None

    result = AdvisoryNotificationService(
        Repository(), transport, lambda: datetime(2026, 11, 13, 23, tzinfo=CN),
    ).run_once()
    assert result['feishu_completion']['status'] == 'WAITING_COMPLETION'
    assert result['queued'] == 0 and result['deliveries'] == []
