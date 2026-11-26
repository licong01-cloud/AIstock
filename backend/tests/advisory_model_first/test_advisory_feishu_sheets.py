"""No real network. Exercise the actual capture, delivery and orchestration contracts."""
import copy
from datetime import date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.advisory_forward.feishu_sheets import (
    DetailSheetDelivery, FeishuSheetsClient, SheetAPIError,
)
from backend.services.advisory_forward.notification_details import (
    DetailDeliveryError, DetailSnapshotBuilder, fingerprint, workbook_tables,
)
from backend.services.advisory_forward.notification_repository import FEISHU_DETAIL_ROUTE
from backend.services.advisory_forward.notification_transport import (
    AdvisoryNotificationTransport, ExternalNotificationDeliveryError,
)
from backend.services.advisory_forward.notifications import CN, AdvisoryNotificationService, completion_events_for_day

NOW = datetime(2026, 11, 19, 22, tzinfo=CN)
TARGET = date(2026, 11, 20)
ROSTER = [dict(program_id='p1', program_name='LSTM', list_version_id='l1', binding_version_id='b1',
               publication_status='PUBLISHED', published_at=NOW, target_trade_date=TARGET)]


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    import requests
    monkeypatch.setattr(requests.sessions.Session, 'request', lambda *a, **k: pytest.fail('network forbidden'))
    for k in ('ENABLED', 'DETAILS_ENABLED', 'APP_ID', 'APP_SECRET', 'CHAT_ID', 'WEBHOOK_URL'):
        monkeypatch.delenv('AISTOCK_ADVISORY_FEISHU_' + k, raising=False)


class History:
    def __init__(self, count=3):
        self.count = count

    def detail(self, pid, lid, **kwargs):
        if 'snapshot_id' in kwargs:
            assert kwargs == dict(group='ALL', limit=0, offset=0, snapshot_id='saved-list-hash')
        else:
            assert kwargs == dict(group='ALL', limit=501, offset=0)
        return dict(program_id=pid, list_version_id=lid, binding_version_id='b1', target_date=str(TARGET),
                    status='PUBLISHED', snapshot_id='saved-list-hash', total_count=self.count,
                    items=[dict(symbol=f'{i:06d}.SZ', stock_name='=UNTRUSTED' if i == 0 else '股票',
                                membership='IN_SCOPE' if i % 2 else 'OUTSIDE_PENDING',
                                change='NEW' if i % 2 else 'REMOVED', rank=i or None)
                           for i in range(self.count)])


class Returns:
    def __init__(self):
        self.batches = []

    def get(self, pid, lid, raw):
        self.batches.append(raw.split(','))
        return dict(program_id=pid, list_version_id=lid, items={s: dict(
            price=10, trade_date='2026-11-19', observed_at=NOW.isoformat(), today_return_bps=0,
            recommendation=dict(start_date='2026-11-20', consecutive_days=1, base_price=None,
                                return_bps=None, reason_code='OPEN_NOT_YET_AVAILABLE'),
        ) for s in raw.split(',')})


def payload(count=3):
    builder = DetailSnapshotBuilder(History(count), Returns(), lambda items: items)
    return dict(subject='AIstock 测试', detail_snapshot=builder.build(NOW, TARGET, ROSTER),
                sheet_delivery_state={})


class Remote:
    """Provider substitute preserves remote resources across retries."""
    destination_hash = 'configured-chat-app-hash'

    def __init__(self):
        self.created, self.worksheets, self.writes, self.shared = 0, [], [], False
        self.fail_create = self.fail_values = None
        self.before_request = None

    def create(self, title):
        self.created += 1
        if self.fail_create:
            raise self.fail_create
        self.worksheets = [{'sheet_id': 's0', 'title': 'Sheet1'}]
        return 'doc1', 'https://my.feishu.cn/sheets/doc1'

    def sheets(self, token):
        return copy.deepcopy(self.worksheets)

    def properties(self, token, operations):
        for op in operations:
            if 'addSheet' in op:
                self.worksheets.append(dict(sheet_id='s' + str(len(self.worksheets)),
                                            title=op['addSheet']['properties']['title']))
            else:
                props = op['updateSheet']['properties']
                match = next(s for s in self.worksheets if s['sheet_id'] == props['sheetId'])
                match.update({k: v for k, v in props.items() if k != 'sheetId'})

    def values(self, token, sheet, cells):
        if self.fail_values:
            raise self.fail_values
        self.writes.append((sheet, copy.deepcopy(cells)))

    def ensure_size(self, token, sheet, rows, columns):
        pass

    def chat_permissions(self, token):
        return self.shared

    def share(self, token):
        self.shared = True


def delivery(remote, data, send=lambda *_: True):
    def checkpoint(state):
        data['sheet_delivery_state'] = copy.deepcopy(state)
    return DetailSheetDelivery(remote).deliver(data, checkpoint, send)


def test_all_groups_batched_snapshot_freezes_unknown_zero_identity_and_formula_text():
    history, returns = History(103), Returns()
    snapshot = DetailSnapshotBuilder(history, returns, lambda rows: rows).build(NOW, TARGET, ROSTER)
    assert list(map(len, returns.batches)) == [100, 3]
    rows = snapshot['programs'][0]['cells']
    assert len(rows) == 104 and rows[1][2] == '待复核' and rows[2][2] == '在荐'
    assert rows[1][5] == '\u200b=UNTRUSTED'
    assert rows[1][9] == 'UNKNOWN' and rows[1][11] == 'UNKNOWN'
    assert rows[1][12] == '\u200b+0.00%' and rows[1][17:] == ['l1', 'b1']
    assert '开盘基准尚未形成' in rows[1][16]
    frozen = copy.deepcopy(snapshot)
    workbook_tables(snapshot)
    assert snapshot == frozen
    snapshot['programs'][0]['cells'][1][4] = 'SZ.999999'
    with pytest.raises(DetailDeliveryError, match='HASH_INVALID'):
        workbook_tables(snapshot)


@pytest.mark.parametrize('count', [0, 501])
def test_no_empty_or_silently_truncated_snapshot(count):
    with pytest.raises(DetailDeliveryError, match='EMPTY_OR_LIMIT'):
        payload(count)


def test_mismatched_saved_version_rejected():
    history = History()
    original = history.detail
    history.detail = lambda *a, **k: dict(original(*a, **k), binding_version_id='changed')
    with pytest.raises(DetailDeliveryError, match='IDENTITY_INVALID'):
        DetailSnapshotBuilder(history, Returns(), lambda x: x).build(NOW, TARGET, ROSTER)


def test_wrong_symbol_format_cannot_silently_export_all_unknown():
    from backend.services.advisory_workbench.quotes import symbols_from_query
    assert symbols_from_query('000001.SZ,600000.SH,920001.BJ') == ['000001.SZ', '600000.SH', '920001.BJ']
    history = History(1)
    original = history.detail
    def malformed(*args, **kwargs):
        result = original(*args, **kwargs)
        result['items'][0]['symbol'] = 'SZ.000001'
        return result
    history.detail = malformed
    with pytest.raises(DetailDeliveryError, match='DETAIL_QUOTE_SYMBOL_INVALID'):
        DetailSnapshotBuilder(history, Returns(), lambda x: x).build(NOW, TARGET, ROSTER)


def test_real_symbol_contract_populates_recommendation_fields_without_recomputing_them():
    from backend.services.advisory_workbench.quotes import symbols_from_query
    def values(pid, lid, raw):
        symbols = symbols_from_query(raw)
        return dict(program_id=pid, list_version_id=lid, items={s: dict(price=12, trade_date='2026-11-19',
            observed_at=NOW.isoformat(), today_return_bps=-25, recommendation=dict(consecutive_days=5,
            start_date='2026-11-13', base_price=10, return_bps=2000, valuation_date='2026-11-19')) for s in symbols})
    snapshot = DetailSnapshotBuilder(History(1), SimpleNamespace(get=values), lambda x: x).build(NOW, TARGET, ROSTER)
    row = snapshot['programs'][0]['cells'][1]
    assert row[4] == '000000.SZ' and row[7:12] == [5, '2026-11-13', 10, 12, '\u200b+20.00%']
    assert row[12] == '\u200b-0.25%' and row[16] == '无'


def test_known_non_recommendation_is_not_applicable_while_unknown_stays_unknown():
    returns = Returns()
    original = returns.get
    def values(*args):
        result = original(*args)
        for quote in result['items'].values():
            quote['recommendation'] = {'state': 'NOT_RECOMMENDED', 'reason_code': 'NOT_RECOMMENDED'}
        return result
    returns.get = values
    row = DetailSnapshotBuilder(History(1), returns, lambda x: x).build(NOW, TARGET, ROSTER)['programs'][0]['cells'][1]
    assert row[7:10] == ['不适用（非本次连续推荐）'] * 3
    assert row[11] == '不适用（非本次连续推荐）' and row[10] == 10
    unknown = payload(1)['detail_snapshot']['programs'][0]['cells'][1]
    assert unknown[9] == 'UNKNOWN' and unknown[11] == 'UNKNOWN'


def test_all_strategy_tabs_acl_and_single_message_then_resume_without_resend():
    data = payload()
    second = copy.deepcopy(data['detail_snapshot']['programs'][0])
    second.update(program_id='p2', program_name='LSTM')  # Same display name, distinct tab identity.
    data['detail_snapshot']['programs'].append(second)
    data['detail_snapshot']['sha256'] = fingerprint({k: v for k, v in data['detail_snapshot'].items() if k != 'sha256'})
    remote, messages = Remote(), []
    assert delivery(remote, data, lambda *args: messages.append(args) or True)
    assert remote.created == 1 and len(remote.worksheets) == 4 and len(remote.writes) == 4
    assert all(s.get('frozenRowCount') == 1 for s in remote.worksheets)
    assert remote.shared and 'https://my.feishu.cn/sheets/doc1' in messages[0][1]
    assert data['sheet_delivery_state']['message_accepted'] is True
    assert delivery(remote, data, lambda *_: pytest.fail('duplicate message'))
    assert remote.created == 1


def test_explicit_sent_test_repair_updates_same_document_without_another_message():
    data, remote = payload(), Remote()
    data['subject'] = '【测试】AIstock'
    assert delivery(remote, data)
    data['sheet_delivery_state'].pop('table_verified')
    data['sheet_delivery_state'].pop('completed_tables')
    def checkpoint(state):
        data['sheet_delivery_state'] = state
    assert DetailSheetDelivery(remote).deliver(data, checkpoint,
        lambda *_: pytest.fail('test repair must not send another message'), update_test_table=True)
    assert remote.created == 1 and len(remote.writes) == 6
    data['subject'] = 'AIstock 正式通知'
    with pytest.raises(DetailDeliveryError, match='TEST_UPDATE_REQUIRES'):
        DetailSheetDelivery(remote).deliver(data, checkpoint, lambda *_: True, update_test_table=True)


def test_partial_write_resume_reuses_document_and_never_sends_incomplete_table():
    remote, data = Remote(), payload()
    remote.fail_values = DetailDeliveryError('FEISHU_VALUES_READBACK_MISMATCH')
    with pytest.raises(DetailDeliveryError, match='READBACK_MISMATCH'):
        delivery(remote, data, lambda *_: pytest.fail('incomplete table sent'))
    assert remote.created == 1 and not remote.shared
    remote.fail_values = None
    assert delivery(remote, data)
    assert remote.created == 1


@pytest.mark.parametrize('rejected', [False, True])
def test_uncertain_create_never_repeated_but_known_rejection_retryable(rejected):
    remote, data = Remote(), payload()
    remote.fail_create = SheetAPIError('FEISHU_API_FAILURE', rejected=rejected)
    with pytest.raises(SheetAPIError):
        delivery(remote, data)
    remote.fail_create = None
    if rejected:
        assert delivery(remote, data)
        assert remote.created == 2
    else:
        with pytest.raises(DetailDeliveryError, match='NO_RECREATE'):
            delivery(remote, data)
        assert remote.created == 1


def test_acl_failure_prevents_message_and_false_send_is_retryable():
    remote, data = Remote(), payload()
    remote.share = lambda *_: None
    with pytest.raises(DetailDeliveryError, match='CHAT_VIEW_NOT_CONFIRMED'):
        delivery(remote, data, lambda *_: pytest.fail('no chat permission'))
    remote.shared = True
    with pytest.raises(DetailDeliveryError, match='MESSAGE_REJECTED'):
        delivery(remote, data, lambda *_: False)
    assert not data['sheet_delivery_state']['message_started']
    assert delivery(remote, data)
    assert remote.created == 1


def test_uncertain_message_never_resent_and_lost_checkpoint_stops_remote_work():
    remote, data = Remote(), payload()
    def uncertain(*_):
        raise ExternalNotificationDeliveryError('suppressed')
    with pytest.raises(ExternalNotificationDeliveryError):
        delivery(remote, data, uncertain)
    with pytest.raises(DetailDeliveryError, match='NO_RESEND'):
        delivery(remote, data, lambda *_: pytest.fail('duplicate message'))
    def stale(_):
        raise RuntimeError('LEASE_LOST')
    fresh = Remote()
    with pytest.raises(RuntimeError, match='LEASE_LOST'):
        DetailSheetDelivery(fresh).deliver(payload(), stale, lambda *_: True)
    assert fresh.created == 0


@pytest.mark.parametrize('http,code,rejected', [(200, 0, None), (400, 99991672, True), (503, 1, False),
                                              (200, False, False), (200, None, False)])
def test_provider_strict_http_business_code_and_no_secret_errors(http, code, rejected):
    client = FeishuSheetsClient('app', 'secret', 'chat', request=lambda *a, **k:
        SimpleNamespace(status_code=http, json=lambda: {'code': code, 'data': {'ok': True}}))
    client._token = uuid4().hex
    if rejected is None:
        assert client.call('GET', '/test') == {'ok': True}
    else:
        with pytest.raises(SheetAPIError) as result:
            client.call('GET', '/test')
        assert result.value.rejected is rejected and 'secret' not in str(result.value)


def test_client_readback_and_fence_before_http():
    requests = []
    def request(method, url, **kwargs):
        requests.append((method, url, kwargs))
        return SimpleNamespace(status_code=200, json=lambda: {'code': 0, 'data':
            {'valueRange': {'values': [['code', '0'], ['SZ.000001', 'UNKNOWN']]}}})
    client = FeishuSheetsClient('app', 'secret', 'chat', request=request)
    client._token = uuid4().hex
    client.values('doc', 'sheet', [['code', 0], ['SZ.000001', 'UNKNOWN']])
    assert len(requests) == 2 and requests[0][2]['timeout'] == 10
    def lost():
        raise RuntimeError('LEASE_LOST')
    client.before_request = lost
    with pytest.raises(RuntimeError, match='LEASE_LOST'):
        client.call('GET', '/test')
    assert len(requests) == 2


def test_provider_rectangular_padding_is_not_missing_financial_data():
    client = FeishuSheetsClient('app', 'secret', 'chat', request=lambda *a, **k:
        SimpleNamespace(status_code=200, json=lambda: {'code': 0, 'data': {'valueRange': {
            'values': [['说明', None, None], ['SZ.000001', 'UNKNOWN', 0]],
        }}}))
    client._token = uuid4().hex
    client.values('doc', 'sheet', [['说明'], ['SZ.000001', 'UNKNOWN', 0]])
    with pytest.raises(DetailDeliveryError, match='READBACK_MISMATCH'):
        client.values('doc', 'sheet', [['说明'], ['SZ.000001', 0, 0]])


def test_grid_capacity_above_initial_200_rows_uses_exact_append_dimension():
    calls = []
    client = FeishuSheetsClient('app', 'secret', 'chat', request=lambda *a, **k:
        calls.append(k) or SimpleNamespace(status_code=200, json=lambda: {'code': 0, 'data': {}}))
    client._token = uuid4().hex
    client.ensure_size('doc', {'sheet_id': 'sheet', 'grid_properties': {'row_count': 200, 'column_count': 20}}, 501, 19)
    assert calls[0]['json'] == {'dimension': {'sheetId': 'sheet', 'majorDimension': 'ROWS', 'length': 301}}
    client.ensure_size('doc', {'sheet_id': 'sheet', 'grid_properties': {'row_count': 501, 'column_count': 20}}, 501, 19)
    assert len(calls) == 1


def test_provider_acl_items_requires_exact_configured_chat():
    rows = [{'member_type': 'appid', 'member_id': 'app', 'perm': 'full_access'}]
    client = FeishuSheetsClient('app', 'secret', 'chat', request=lambda *a, **k:
        SimpleNamespace(status_code=200, json=lambda: {'code': 0, 'data': {'items': rows}}))
    client._token = uuid4().hex
    assert not client.chat_permissions('doc')
    rows.append({'member_type': 'openchat', 'member_id': 'other-chat', 'perm': 'view'})
    assert not client.chat_permissions('doc')
    rows.append({'member_type': 'openchat', 'member_id': 'chat', 'perm': 'view'})
    assert client.chat_permissions('doc')


def test_dedicated_detail_configuration_isolated_and_ambiguous_send_not_safe_retry(monkeypatch):
    shared = SimpleNamespace(config={'webhook_type': 'dingtalk', 'email_enabled': False})
    original = copy.deepcopy(shared.config)
    for key, value in {'ENABLED': 'true', 'DETAILS_ENABLED': 'true', 'APP_ID': 'test', 'APP_SECRET': 'secret',
                       'CHAT_ID': 'chat', 'WEBHOOK_URL': 'https://open.feishu.cn/open-apis/bot/v2/hook/test'}.items():
        monkeypatch.setenv('AISTOCK_ADVISORY_FEISHU_' + key, value)
    transport = AdvisoryNotificationTransport(shared)
    assert shared.config == original and transport.config['advisory_feishu_details']
    monkeypatch.setattr(transport, '_post_webhook', lambda *_: SimpleNamespace(status_code=503))
    with pytest.raises(ExternalNotificationDeliveryError):
        transport.send_external_message('webhook', 'title', 'body')
    monkeypatch.delenv('AISTOCK_ADVISORY_FEISHU_APP_SECRET')
    missing = AdvisoryNotificationTransport(shared)
    assert not missing.config['webhook_url']
    assert missing.config['advisory_feishu_configuration_error'] == 'FEISHU_DETAILS_CONFIGURATION_MISSING'


@pytest.mark.parametrize('clock', [NOW, datetime(2026, 11, 20, 8, tzinfo=CN)])
def test_service_queues_details_once_uses_separate_route_and_freezes_retry(monkeypatch, clock):
    monkeypatch.setenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', 'true')
    events, sent = [], []
    class Repo:
        def completion_days(self, day):
            return [NOW.date()]
        def snapshot(self, day):
            assert day == NOW.date()
            return TARGET, ROSTER
        def detail_event_exists(self, key, day):
            assert day == NOW.date()
            return bool(events)
        def enqueue(self, event, channel, now):
            events.append(copy.deepcopy(event))
            return True
        def claim(self, now, channels, *, webhook_route):
            assert webhook_route == FEISHU_DETAIL_ROUTE
            if not events or sent:
                return None
            return dict(events[0], channel='webhook')
        def checkpoint_detail(self, row, state, now):
            row['payload']['sheet_delivery_state'] = state
        def finish(self, row, **kwargs):
            assert kwargs['accepted'] is True
            sent.append(row)
            return 'SENT'
    transport = SimpleNamespace(
        config=dict(advisory_feishu_completion_only=True, advisory_feishu_details=True,
                    advisory_feishu_app_messages=True, webhook_enabled=True, webhook_type='feishu', webhook_url='configured'),
        detail_delivery=lambda: DetailSheetDelivery(Remote()), send_detail_card=lambda *_: True,
        prepare_detail_card=lambda card, key: card,
        send_external_message=lambda *_: True)
    builder = DetailSnapshotBuilder(History(), Returns(), lambda items: items)
    service = AdvisoryNotificationService(Repo(), transport, lambda: clock, builder)
    first = service.run_once()
    assert first['queued'] == 1 and first['deliveries'][0]['status'] == 'SENT'
    assert events[0]['payload']['delivery_route'] == FEISHU_DETAIL_ROUTE
    assert events[0]['event_key'] == completion_events_for_day(NOW, TARGET, ROSTER)[0]['event_key']
    assert first['feishu_detail_sheet']['status'] == 'EVENT_READY'
    snapshot = events[0]['payload']['detail_snapshot']
    assert snapshot['decision_date'] == str(NOW.date())
    assert snapshot['target_date'] == str(TARGET)
    assert snapshot['captured_at'] == clock.isoformat()
    assert len(snapshot['programs'][0]['cells']) == 4  # All recommended and exit rows.
    assert workbook_tables(snapshot)[1][1][1] == ['决策日', str(NOW.date())]
    from backend.services.advisory_forward.notification_presentation import mobile_card
    assert str(NOW.date()) in str(mobile_card(events[0]['payload'], 'https://my.feishu.cn/sheets/doc1'))
    builder.build = lambda *a, **k: pytest.fail('second tick re-fetched frozen quotes')
    assert service.run_once()['queued'] == 0


def test_incomplete_roster_does_not_capture_or_send(monkeypatch):
    monkeypatch.setenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', 'true')
    repo = SimpleNamespace(completion_days=lambda day: [day],
                           snapshot=lambda _: (TARGET, [dict(ROSTER[0], publication_status='WAITING_DATA')]),
                           claim=lambda *a, **k: None)
    transport = SimpleNamespace(config=dict(advisory_feishu_completion_only=True, advisory_feishu_details=True,
                                           advisory_feishu_app_messages=True, webhook_enabled=True, webhook_type='feishu', webhook_url='configured'))
    builder = SimpleNamespace(build=lambda *_: pytest.fail('incomplete publication'))
    assert AdvisoryNotificationService(repo, transport, lambda: NOW, builder).run_once()['queued'] == 0


def test_cross_day_detail_capture_failure_remains_visible_and_never_queues(monkeypatch):
    monkeypatch.setenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', 'true')
    repo = SimpleNamespace(completion_days=lambda day: [NOW.date()],
                           snapshot=lambda day: (TARGET, ROSTER),
                           detail_event_exists=lambda key, day: False,
                           enqueue=lambda *a: pytest.fail('failed capture must not queue'),
                           claim=lambda *a, **k: None)
    transport = SimpleNamespace(config=dict(advisory_feishu_completion_only=True, advisory_feishu_details=True,
                                           advisory_feishu_app_messages=True, webhook_enabled=True, webhook_type='feishu', webhook_url='configured'))

    def failed_build(*args, **kwargs):
        assert kwargs['decision_date'] == NOW.date()
        raise DetailDeliveryError('DETAIL_LIST_IDENTITY_INVALID')

    result = AdvisoryNotificationService(repo, transport, lambda: datetime(2026, 11, 20, 8, tzinfo=CN),
                                         SimpleNamespace(build=failed_build)).run_once()
    assert result['queued'] == 0 and result['deliveries'] == []
    assert result['feishu_detail_sheet'] == {'status': 'DETAIL_SNAPSHOT_FAILED',
                                            'reason_code': 'DETAIL_LIST_IDENTITY_INVALID'}
