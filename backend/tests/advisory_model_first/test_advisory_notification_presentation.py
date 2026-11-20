"""User-visible mobile/desktop contracts; all remote traffic is mocked."""
import copy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.advisory_forward.feishu_sheets import DetailSheetDelivery
from backend.services.advisory_forward.notification_details import DetailDeliveryError, fingerprint, workbook_tables
from backend.services.advisory_forward.notification_presentation import (
    CARD_BUDGET, PRESENTATION_VERSION, card_json_bytes, mobile_card, mobile_rows, overview_table,
)
from backend.services.advisory_forward.notification_transport import (
    AdvisoryNotificationTransport, ExternalNotificationDeliveryError,
)
from backend.tests.advisory_model_first.test_advisory_feishu_sheets import Remote, payload

URL = 'https://my.feishu.cn/sheets/doc1'


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    import requests
    monkeypatch.setattr(requests.sessions.Session, 'request', lambda *a, **k: pytest.fail('real HTTP forbidden'))


def sealed(data):
    snapshot = data['detail_snapshot']
    snapshot['sha256'] = fingerprint({k: v for k, v in snapshot.items() if k != 'sha256'})
    return data


def text(card):
    return '\n'.join(e.get('text', {}).get('content', '') for e in card['elements'])


def test_mobile_table_contains_all_stocks_after_first_page_and_keeps_original_rank():
    data = payload(27)
    rows = data['detail_snapshot']['programs'][0]['cells'][1:]
    for i, row in enumerate(rows):
        row[2], row[3], row[5], row[6] = '在荐', 27 - i, f'股票{i}', '持续推荐'
    sealed(data)
    before = copy.deepcopy(data)
    card = mobile_card(data, URL)
    body = text(card)
    assert '全量股票' in body and '在荐 27' in body
    table = next(e for e in card['elements'] if e['tag'] == 'table')
    assert table['page_size'] == 10 and len(table['rows']) == 27
    assert table['rows'][0]['s'].startswith('1. 股票26')
    assert table['rows'][-1]['s'].startswith('27. 股票0')
    assert {r['s'].split('\n')[-1] for r in table['rows']} == {r[4] for r in rows}
    assert len(workbook_tables(data['detail_snapshot'])[-1][1]) == 28
    assert card['elements'][-2]['actions'][0]['url'] == URL
    assert card['config']['wide_screen_mode'] is False
    assert data == before


def test_table_keeps_unknown_without_fake_zero_and_pending_is_not_an_executed_exit():
    data = payload(3)
    row = data['detail_snapshot']['programs'][0]['cells'][2]
    row[6:10] = ['待核验', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN']
    sealed(data)
    card = mobile_card(data, URL)
    table = next(e for e in card['elements'] if e['tag'] == 'table')
    assert len(table['rows']) == 3  # One active and both pending-review rows.
    assert table['rows'][0]['t'] == '待核验·天数待核验'
    assert table['rows'][0]['v'] == '待开盘'
    assert [r['t'] for r in table['rows'][1:]] == ['已移出·待复核'] * 2
    assert '已退出' not in json.dumps(card, ensure_ascii=False)
    overview = overview_table(data['detail_snapshot'])
    assert overview[1][2] == 1 and overview[1][6] == 1 and overview[1][8] == 2


def test_desktop_overview_is_first_complete_and_not_a_return_ranking():
    data = payload()
    cells = data['detail_snapshot']['programs'][0]['cells']
    before = copy.deepcopy(cells)
    tables = workbook_tables(data['detail_snapshot'])
    assert [t[0] for t in tables[:2]] == ['策略总览', '使用说明']
    head, row = tables[0][1]
    assert row[head.index('名单总行数')] == 3
    assert row[head.index('在荐有效报价数')] == 1
    assert row[head.index('在荐收益可用数')] == 0
    assert row[head.index('名单版本')] == 'l1'
    assert tables[-1][1] == before


def test_too_many_packages_fail_instead_of_omitting_any_package():
    data = payload(7)
    p = data['detail_snapshot']['programs'][0]
    data['detail_snapshot']['programs'] = [dict(copy.deepcopy(p), program_id=f'p{i}',
        program_name='策略包中文名称' * 20 + str(i)) for i in range(100)]
    sealed(data)
    with pytest.raises(DetailDeliveryError, match='FULL_TABLE_COUNT_LIMIT'):
        mobile_card(data, URL)
    assert len(overview_table(data['detail_snapshot'])) == 101
    assert len(workbook_tables(data['detail_snapshot'])) == 102


def test_oversized_full_card_fails_without_top_n_fallback():
    data = payload(400)
    with pytest.raises(DetailDeliveryError, match='FULL_CARD_TOO_LARGE'):
        mobile_card(data, URL)


def test_removed_observation_exit_suggestion_pending_and_ordinary_watch_are_distinct():
    data = payload(5)
    rows = data['detail_snapshot']['programs'][0]['cells'][1:]
    rows[0][2], rows[0][6] = '观察', '已移出'
    rows[1][2], rows[1][6] = '待退出', '已移出'
    rows[2][2], rows[2][6] = '待复核', '待核验'
    rows[3][2], rows[3][6] = '在荐', '持续推荐'
    rows[4][2], rows[4][6] = '观察', '待核验'
    sealed(data)
    selected, counts = mobile_rows(data['detail_snapshot']['programs'][0])
    assert counts == dict(active=1, exits=2, pending=1)
    assert len(selected) == 4 and selected[0]['s'].endswith('000003.SZ')
    assert selected[1]['t'] == '已移出·观察'
    assert selected[2]['t'] == '退出建议·已移出'
    assert selected[3]['t'] == '待复核'
    assert all('000004.SZ' not in r['s'] for r in selected)


@pytest.mark.parametrize('url', ['http://localhost:3000/x', 'https://evil.example/sheets/doc1', URL + '?extra=1'])
def test_card_link_must_be_native_verified_document(url):
    with pytest.raises(DetailDeliveryError, match='DOCUMENT_URL_INVALID'):
        mobile_card(payload(), url)


def test_local_card_failure_is_before_send_started_and_retry_is_safe():
    data, remote = payload(), Remote()
    def checkpoint(state):
        data['sheet_delivery_state'] = copy.deepcopy(state)
    def bad(*_):
        raise DetailDeliveryError('LOCAL_CARD_INVALID')
    with pytest.raises(DetailDeliveryError, match='LOCAL_CARD_INVALID'):
        DetailSheetDelivery(remote).deliver(data, checkpoint, lambda *_: pytest.fail('invalid card sent'), prepare_message=bad)
    assert not data['sheet_delivery_state'].get('message_started')
    sent = []
    assert DetailSheetDelivery(remote).deliver(data, checkpoint, lambda *a: sent.append(a) or True,
                                               prepare_message=mobile_card)
    assert remote.created == 1 and len(sent) == 1
    assert data['sheet_delivery_state']['message_payload_sha256'] == fingerprint(sent[0][1])
    assert data['sheet_delivery_state']['presentation_version'] == PRESENTATION_VERSION


def test_old_unsent_document_gets_overview_without_recreating_or_rewriting_details():
    data, remote = payload(), Remote()
    token, url = remote.create('test')
    old_tables = workbook_tables(data['detail_snapshot'])[1:]
    remote.worksheets = [dict(sheet_id='s' + str(i), title=title) for i, (title, _) in enumerate(old_tables)]
    data['sheet_delivery_state'] = dict(destination_hash=remote.destination_hash, create_started=True,
        document_token=token, document_url=url, table_verified=True, completed_tables=[t[0] for t in old_tables])
    def checkpoint(state):
        data['sheet_delivery_state'] = state
    assert DetailSheetDelivery(remote).deliver(data, checkpoint, lambda *_: True, prepare_message=mobile_card)
    assert remote.created == 1 and len(remote.writes) == 1
    assert remote.writes[0][1][0][0] == '策略包'
    # A crash after accepted cannot send again or upgrade a previous delivery.
    writes = len(remote.writes)
    assert DetailSheetDelivery(remote).deliver(data, checkpoint, lambda *_: pytest.fail('duplicate'), prepare_message=mobile_card)
    assert len(remote.writes) == writes


@pytest.fixture
def transport():
    obj = AdvisoryNotificationTransport.__new__(AdvisoryNotificationTransport)
    obj.config = dict(advisory_feishu_details=True, advisory_feishu_completion_only=True,
                      webhook_enabled=True, webhook_type='feishu', webhook_url='https://invalid.test/test',
                      webhook_signing_secret=uuid4().hex)
    return obj


@pytest.mark.parametrize('http,code,result', [(200, 0, True), (200, 1, False), (200, False, None),
                                            (503, 0, None), (200, None, None)])
def test_card_strict_acceptance_and_shared_signing(monkeypatch, transport, http, code, result):
    sent = []
    def post(data):
        sent.append(data)
        return SimpleNamespace(status_code=http, json=lambda: {'code': code})
    monkeypatch.setattr(transport, '_post_webhook', post)
    card = mobile_card(payload(), URL)
    if result is None:
        with pytest.raises(ExternalNotificationDeliveryError):
            transport.send_detail_card(card)
    else:
        assert transport.send_detail_card(card) is result
    assert sent[0]['msg_type'] == 'interactive' and sent[0]['card'] == card
    assert sent[0]['timestamp'] and sent[0]['sign']


def test_card_size_and_disabled_details_never_send(monkeypatch, transport):
    monkeypatch.setattr(transport, '_post_webhook', lambda *_: pytest.fail('must not send'))
    with pytest.raises(ExternalNotificationDeliveryError, match='TOO_LARGE'):
        transport.send_detail_card({'content': '超限' * 30000})
    transport.config['advisory_feishu_details'] = False
    assert transport.send_detail_card(mobile_card(payload(), URL)) is False


def test_table_test_is_explicitly_labeled_and_within_actual_utf8_budget():
    data = payload()
    data['subject'] = '【测试】AIstock 手机全量表格'
    card = mobile_card(data, URL)
    assert '非新的荐股通知' in text(card)
    assert len(card_json_bytes(card)) <= CARD_BUDGET


def test_card_guard_matches_actual_compact_utf8_wire_and_keeps_legacy_json_path(monkeypatch, transport):
    import requests
    calls = []
    monkeypatch.setattr(requests, 'post', lambda *args, **kwargs: calls.append(kwargs) or SimpleNamespace(
        status_code=200, json=lambda: {'code': 0}))
    card = mobile_card(payload(60), URL)
    assert transport.send_detail_card(card)
    wire = calls[0]['data']
    assert isinstance(wire, bytes) and len(wire) <= 20 * 1024
    parsed = json.loads(wire)
    assert wire == card_json_bytes(parsed) and parsed['card'] == card
    assert '股票'.encode() in wire and b'\\u80a1' not in wire
    assert calls[0]['headers']['Content-Type'] == 'application/json; charset=utf-8'
    transport._post_webhook({'msg_type': 'text', 'content': {'text': 'legacy'}})
    assert 'json' in calls[1] and 'data' not in calls[1]
