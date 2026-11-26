"""Actual Advisory orchestration/transport with isolated HTTP and outbox boundaries."""
import copy
import hashlib
import json
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.advisory_forward.feishu_sheets import (
    APP_MESSAGE_BUDGET, DetailSheetDelivery, FeishuSheetsClient, SheetAPIError,
)
from backend.services.advisory_forward.notification_details import DetailDeliveryError, fingerprint
from backend.services.advisory_forward.notification_presentation import card_json_bytes, mobile_card, mobile_rows
from backend.services.advisory_forward.notification_transport import AdvisoryNotificationTransport
from backend.services.advisory_forward.notifications import AdvisoryNotificationService, configured_channels
from backend.tests.advisory_model_first.test_advisory_feishu_sheets import NOW, ROSTER, TARGET, Remote, payload

EVENT_KEY = 'a' * 64
URL = 'https://my.feishu.cn/sheets/doc1'


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    import requests
    monkeypatch.setattr(requests.sessions.Session, 'request', lambda *a, **k: pytest.fail('real HTTP forbidden'))
    for key in ('ENABLED', 'DETAILS_ENABLED', 'APP_ID', 'APP_SECRET', 'CHAT_ID', 'WEBHOOK_URL'):
        monkeypatch.delenv('AISTOCK_ADVISORY_FEISHU_' + key, raising=False)


def configured_transport(monkeypatch):
    for key, value in dict(ENABLED='true', DETAILS_ENABLED='true', APP_ID='app',
                           APP_SECRET='secret', CHAT_ID='chat').items():
        monkeypatch.setenv('AISTOCK_ADVISORY_FEISHU_' + key, value)
    return AdvisoryNotificationTransport(SimpleNamespace(config=dict(email_enabled=False, webhook_enabled=False)))


def response(http=200, code=0, data=None):
    return SimpleNamespace(status_code=http, json=lambda: dict(code=code, data=data if data is not None else {
        'message_id': 'om_test', 'chat_id': 'chat', 'msg_type': 'interactive'}))


def client_with_http(request):
    client = FeishuSheetsClient('app', 'secret', 'chat', request=request)
    client._token = uuid4().hex
    return client


def six_packages():
    data = payload(36)
    source = data['detail_snapshot']['programs'][0]
    for i, row in enumerate(source['cells'][1:]):
        row[5] = f'测试股票名称{i}'
    data['detail_snapshot']['programs'] = [dict(copy.deepcopy(source), program_id=f'p{i}',
        program_name=f'日频荐股完整策略包 {i}') for i in range(6)]
    data['detail_snapshot']['sha256'] = fingerprint({
        k: v for k, v in data['detail_snapshot'].items() if k != 'sha256'})
    return data


def test_app_details_need_no_webhook_and_leave_shared_configuration_untouched(monkeypatch):
    shared = SimpleNamespace(config=dict(email_enabled=False, webhook_enabled=True,
                                        webhook_type='dingtalk', webhook_url='legacy'))
    original = copy.deepcopy(shared.config)
    configured_transport(monkeypatch)
    transport = AdvisoryNotificationTransport(shared)
    assert transport.config['advisory_feishu_app_messages']
    assert not transport.config['advisory_feishu_configuration_error']
    assert transport.config['webhook_url'] == ''
    assert configured_channels(transport.config) == ['webhook']  # Existing daily ledger identity.
    assert shared.config == original
    monkeypatch.delenv('AISTOCK_ADVISORY_FEISHU_APP_SECRET')
    missing = AdvisoryNotificationTransport(shared)
    assert configured_channels(missing.config) == []
    assert missing.config['advisory_feishu_configuration_error'] == 'FEISHU_DETAILS_CONFIGURATION_MISSING'
    with pytest.raises(DetailDeliveryError, match='CONFIGURATION_MISSING'):
        missing.prepare_detail_card({}, EVENT_KEY)


def test_full_six_package_wire_exceeds_old_budget_and_uses_only_app_api(monkeypatch):
    transport, calls = configured_transport(monkeypatch), []
    client = client_with_http(lambda method, url, **kw: calls.append((method, url, kw)) or response())
    transport._feishu_client = client
    monkeypatch.setattr(transport, '_post_webhook', lambda *_: pytest.fail('webhook must not be used'))
    data = six_packages()
    card = mobile_card(data, URL)
    before = copy.deepcopy(data)
    prepared = transport.prepare_detail_card(card, EVENT_KEY)
    receipt = transport.send_detail_card(prepared)
    assert len(calls) == 1
    method, url, kwargs = calls[0]
    assert method == 'POST' and url.endswith('/im/v1/messages')
    wire = kwargs['data']
    assert 18 * 1024 < len(wire) <= APP_MESSAGE_BUDGET
    assert wire == card_json_bytes(prepared) and 'json' not in kwargs
    assert kwargs['params'] == {'receive_id_type': 'chat_id'}
    assert kwargs['headers'] == {'Authorization': 'Bearer ' + client._token,
                                 'Content-Type': 'application/json; charset=utf-8'}
    assert kwargs['timeout'] == 10 and kwargs['allow_redirects'] is False
    sent_card = json.loads(json.loads(wire)['content'])
    rows = next(e['rows'] for e in sent_card['elements'] if e['tag'] == 'table')
    assert len(rows) == 216
    for i, program in enumerate(data['detail_snapshot']['programs'], 1):
        expected, _ = mobile_rows(program)
        assert [{k: r[k] for k in ('s', 't', 'v')} for r in rows if r['p'] == str(i)] == expected
    assert sent_card['elements'][-2]['actions'][0]['url'] == URL
    assert data == before
    assert receipt == dict(message_id='om_test', message_uuid=prepared['uuid'], message_transport='feishu_app')


def test_request_boundary_includes_envelope_and_escaped_card_content():
    calls = []
    client = client_with_http(lambda *a, **k: calls.append(k) or response())
    card = {'text': ''}
    initial = client.prepare_card_message(card, EVENT_KEY)
    card['text'] = 'x' * (APP_MESSAGE_BUDGET - len(card_json_bytes(initial)))
    boundary = client.prepare_card_message(card, EVENT_KEY)
    assert len(card_json_bytes(boundary)) == APP_MESSAGE_BUDGET
    client.send_card_message(boundary)
    assert len(calls[0]['data']) == APP_MESSAGE_BUDGET
    card['text'] += '中'
    with pytest.raises(DetailDeliveryError, match='APP_MESSAGE_TOO_LARGE'):
        client.prepare_card_message(card, EVENT_KEY)
    assert len(calls) == 1


def test_uuid_binds_daily_event_and_destination_without_secrets():
    client = FeishuSheetsClient('app', 'secret', 'chat')
    first = client.prepare_card_message({'text': 'one'}, EVENT_KEY)
    assert first == client.prepare_card_message({'text': 'one'}, EVENT_KEY)
    assert first['uuid'] == client.prepare_card_message({'text': 'two'}, EVENT_KEY)['uuid']
    assert first['uuid'] != client.prepare_card_message({'text': 'one'}, 'b' * 64)['uuid']
    assert first['uuid'] != FeishuSheetsClient('app', 'secret', 'other').prepare_card_message({}, EVENT_KEY)['uuid']
    assert len(first['uuid']) == 32 and 'secret' not in str(first)
    with pytest.raises(DetailDeliveryError, match='IDENTITY_INVALID'):
        client.prepare_card_message({}, 'missing-event-key')


@pytest.mark.parametrize('http,code,rejected', [(400, 99991672, True), (503, 1, False),
                                               (200, False, False), (200, None, False)])
def test_app_rejection_and_ambiguous_response_are_distinct_and_sanitized(http, code, rejected):
    client = client_with_http(lambda *a, **k: response(http, code))
    with pytest.raises(SheetAPIError) as result:
        client.send_card_message(client.prepare_card_message({}, EVENT_KEY))
    assert result.value.rejected is rejected and 'secret' not in str(result.value)


@pytest.mark.parametrize('data', [{}, {'message_id': 'om_test', 'chat_id': 'wrong', 'msg_type': 'interactive'},
                                  {'message_id': 'om_test', 'chat_id': 'chat', 'msg_type': 'text'},
                                  {'message_id': None, 'chat_id': 'chat', 'msg_type': 'interactive'}])
def test_http_success_without_exact_message_receipt_is_not_success(data):
    client = client_with_http(lambda *a, **k: response(data=data))
    with pytest.raises(DetailDeliveryError, match='RECEIPT_INVALID'):
        client.send_card_message(client.prepare_card_message({}, EVENT_KEY))


def test_sheet_and_message_share_auth_client_and_fence_every_request(monkeypatch):
    transport, calls = configured_transport(monkeypatch), []
    def request(method, url, **kwargs):
        calls.append(url)
        if url.endswith('/auth/v3/tenant_access_token/internal'):
            return SimpleNamespace(status_code=200, json=lambda: {
                'code': 0, 'tenant_access_token': 'token', 'expire': 7200})
        return response()
    client = FeishuSheetsClient('app', 'secret', 'chat', request=request)
    transport._feishu_client = client
    assert transport.detail_delivery().client is client
    fenced = []
    client.before_request = lambda: fenced.append(True)
    client.call('GET', '/test-sheet-read')
    transport.send_detail_card(transport.prepare_detail_card({}, EVENT_KEY))
    assert sum('tenant_access_token' in url for url in calls) == 1
    assert len(calls) == len(fenced) == 3


def test_cached_app_token_refreshes_before_expiry_without_retrying_a_message(monkeypatch):
    from backend.services.advisory_forward import feishu_sheets
    clock, calls = [100.0], []
    monkeypatch.setattr(feishu_sheets, 'monotonic', lambda: clock[0])
    def request(method, url, **kwargs):
        calls.append((url, kwargs))
        if url.endswith('/auth/v3/tenant_access_token/internal'):
            return SimpleNamespace(status_code=200, json=lambda: {
                'code': 0, 'tenant_access_token': 'renewed', 'expire': 7200})
        return response()
    client = FeishuSheetsClient('app', 'secret', 'chat', request=request)
    client.call('GET', '/test-read')
    client.call('GET', '/test-read')
    assert sum('tenant_access_token' in url for url, _ in calls) == 1
    clock[0] += 7140
    client.send_card_message(client.prepare_card_message({}, EVENT_KEY))
    assert sum('tenant_access_token' in url for url, _ in calls) == 2
    assert sum(url.endswith('/im/v1/messages') for url, _ in calls) == 1


def deliver_app(client, data, remote=None, checkpoint=None):
    remote = remote or Remote()
    def save(state):
        data['sheet_delivery_state'] = copy.deepcopy(state)
        if checkpoint:
            checkpoint(state)
    return DetailSheetDelivery(remote).deliver(data, save,
        lambda subject, message: client.send_card_message(message),
        prepare_message=lambda p, url: client.prepare_card_message(mobile_card(p, url), EVENT_KEY))


def test_oversize_preflight_does_not_mark_send_started_and_reuses_existing_sheet():
    data, remote = payload(400), Remote()
    client = client_with_http(lambda *a, **k: pytest.fail('oversized request sent'))
    with pytest.raises(DetailDeliveryError, match='APP_MESSAGE_TOO_LARGE'):
        deliver_app(client, data, remote)
    assert not data['sheet_delivery_state'].get('message_started')
    assert remote.created == 1 and data['sheet_delivery_state']['table_verified']


@pytest.mark.parametrize('known_rejection', [True, False])
def test_known_rejection_can_retry_same_uuid_but_timeout_never_blindly_resends(known_rejection):
    data, remote, calls = payload(), Remote(), []
    def request(*args, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            if known_rejection:
                return response(400, 99991672)
            raise RuntimeError('secret transport diagnostic must be suppressed')
        return response()
    client = client_with_http(request)
    with pytest.raises(SheetAPIError):
        deliver_app(client, data, remote)
    assert bool(data['sheet_delivery_state'].get('message_started')) is not known_rejection
    if known_rejection:
        assert deliver_app(client, data, remote)
        assert calls[0]['data'] == calls[1]['data']
        state = data['sheet_delivery_state']
        assert state['message_id'] == 'om_test' and state['message_transport'] == 'feishu_app'
        assert state['message_uuid'] == json.loads(calls[1]['data'])['uuid']
        assert deliver_app(client, data, remote)
        assert len(calls) == 2 and remote.created == 1
    else:
        with pytest.raises(DetailDeliveryError, match='NO_RESEND'):
            deliver_app(client, data, remote)
        assert len(calls) == 1 and remote.created == 1


def test_missing_receipt_and_lost_acceptance_checkpoint_cannot_duplicate_send():
    for invalid_receipt in (True, False):
        data, remote, calls = payload(), Remote(), []
        client = client_with_http(lambda *a, **kw: calls.append(kw) or response(data={} if invalid_receipt else None))
        def lost_after_send(state):
            if state.get('message_accepted'):
                # Model rollback/loss of this checkpoint: only send-started persisted.
                data['sheet_delivery_state'].pop('message_accepted', None)
                raise RuntimeError('LEASE_LOST')
        with pytest.raises((DetailDeliveryError, RuntimeError)):
            deliver_app(client, data, remote, None if invalid_receipt else lost_after_send)
        with pytest.raises(DetailDeliveryError, match='NO_RESEND'):
            deliver_app(client, data, remote)
        assert len(calls) == 1


def test_native_scheduler_entry_sends_full_snapshot_once_with_native_receipt(monkeypatch):
    monkeypatch.setenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', 'true')
    transport, data, calls = configured_transport(monkeypatch), six_packages(), []
    client = client_with_http(lambda *a, **kw: calls.append(kw) or response())
    remote = Remote()
    for name in ('create', 'sheets', 'properties', 'values', 'ensure_size', 'chat_permissions', 'share'):
        monkeypatch.setattr(client, name, getattr(remote, name))
    transport._feishu_client = client
    rows = [dict(ROSTER[0], program_id=p['program_id'], program_name=p['program_name'])
            for p in data['detail_snapshot']['programs']]
    class Repo:
        event = None
        sent = False
        def completion_days(self, day):
            return [NOW.date()]
        def snapshot(self, day):
            return TARGET, rows
        def detail_event_exists(self, key, day):
            return self.event is not None
        def enqueue(self, event, channel, now):
            self.event = dict(copy.deepcopy(event), channel=channel)
            return True
        def claim(self, now, channels, **kwargs):
            return self.event if not self.sent else None
        def checkpoint_detail(self, row, state, now):
            self.event['payload']['sheet_delivery_state'] = copy.deepcopy(state)
        def finish(self, row, **kwargs):
            assert kwargs['accepted'] is True and kwargs['error_code'] is None
            self.sent = True
            return 'SENT'
    repo = Repo()
    builder = SimpleNamespace(build=lambda *a, **kw: copy.deepcopy(data['detail_snapshot']))
    service = AdvisoryNotificationService(repo, transport, lambda: NOW, builder)
    first = service.run_once()
    assert first['queued'] == 1 and first['deliveries'][0]['status'] == 'SENT'
    assert repo.event['event_key'] == hashlib.sha256(json.dumps([
        'advisory_feishu_completion_v1', str(NOW.date())]).encode()).hexdigest()
    state = repo.event['payload']['sheet_delivery_state']
    assert state['message_id'] == 'om_test' and state['message_transport'] == 'feishu_app'
    assert state['message_accepted'] and state['table_verified'] and state['chat_view_verified']
    builder.build = lambda *a, **kw: pytest.fail('frozen snapshot rebuilt')
    assert service.run_once()['queued'] == 0
    assert len(calls) == 1 and remote.created == 1 and len(remote.writes) == 8


@pytest.mark.skipif(os.getenv('AISTOCK_NOTIFICATION_DEV_TEST') != '1', reason='existing DEV opt-in only')
def test_native_app_receipt_checkpoint_readback_and_daily_dedupe_roll_back_in_dev():
    from contextlib import contextmanager
    from datetime import datetime, timedelta, timezone
    from pathlib import Path
    from uuid import uuid4

    import psycopg2
    from psycopg2.extras import RealDictCursor

    from backend.services.advisory_forward.notification_repository import (
        AdvisoryNotificationRepository, FEISHU_DETAIL_ROUTE,
    )
    from scripts.prepare_canonical_pit_monthly import _load_database_config

    cfg = _load_database_config('dev', Path('F:/Dev/AIstock/.env'))
    assert cfg.dbname == 'aistock_dev' and str(cfg.port) == '5433'
    options = dict(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password,
                   dbname=cfg.dbname, connect_timeout=5)
    conn = psycopg2.connect(**options)
    key, now = hashlib.sha256(uuid4().bytes).hexdigest(), datetime.now(timezone.utc)
    @contextmanager
    def connection_factory():
        yield conn
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT current_database()')
            assert cur.fetchone()[0] == 'aistock_dev'
        repo = AdvisoryNotificationRepository(connection_factory)
        event = dict(event_key=key, kind='SUMMARY', decision_date=now.date(),
                     expires_at=now + timedelta(hours=1),
                     payload=dict(subject='DEV ONLY', delivery_route=FEISHU_DETAIL_ROUTE,
                                  detail_snapshot={'sha256': 'frozen-dev-proof'}, sheet_delivery_state={}))
        assert repo.enqueue(event, 'webhook', now)
        assert not repo.enqueue(event, 'webhook', now)
        # Claim only our exact test key; never run the global queue scanner.
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("UPDATE app.advisory_notification_delivery SET status='SENDING',attempts=1,"
                        "lease_token=%s,lease_until=%s WHERE event_key=%s AND channel='webhook' RETURNING *",
                        (uuid4().hex, now + timedelta(seconds=120), key))
            row = dict(cur.fetchone())
        state = dict(message_started=True, message_accepted=True, message_id='om_dev_test',
                     message_uuid='a' * 32, message_transport='feishu_app', table_verified=True)
        repo.checkpoint_detail(row, state, now)
        assert repo.finish(row, accepted=True, now=now) == 'SENT'
        with conn.cursor() as cur:
            cur.execute('SELECT status,payload FROM app.advisory_notification_delivery WHERE event_key=%s', (key,))
            status, saved = cur.fetchone()
        assert status == 'SENT' and saved['sheet_delivery_state'] == state
        assert saved['detail_snapshot'] == event['payload']['detail_snapshot']
        assert not repo.enqueue(event, 'webhook', now)
    finally:
        conn.rollback()
        conn.close()
    with psycopg2.connect(**options) as check:
        with check.cursor() as cur:
            cur.execute('SELECT count(*) FROM app.advisory_notification_delivery WHERE event_key=%s', (key,))
            assert cur.fetchone()[0] == 0
