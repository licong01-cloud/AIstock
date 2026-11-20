"""User-visible mobile/desktop contracts; all remote traffic is mocked."""
import copy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.advisory_forward.feishu_sheets import DetailSheetDelivery
from backend.services.advisory_forward.notification_details import DetailDeliveryError, fingerprint, workbook_tables
from backend.services.advisory_forward.notification_presentation import (
    CARD_BUDGET, MOBILE_TOP_N, PRESENTATION_VERSION, mobile_card, overview_table,
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


@pytest.mark.parametrize('top_n', [3, 5])
def test_mobile_summary_keeps_original_rank_and_full_desktop_rows(top_n):
    data = payload(7)
    rows = data['detail_snapshot']['programs'][0]['cells'][1:]
    for i, row in enumerate(rows):
        row[2], row[3], row[5], row[6] = '在荐', 7 - i, f'股票{i}', '持续推荐'
    sealed(data)
    before = copy.deepcopy(data)
    card = mobile_card(data, URL, top_n=top_n)
    body = text(card)
    assert f'最多 {top_n} 只' in body and '在荐 7' in body
    assert body.index('1. 股票6') < body.index('2. 股票5')
    assert f'{top_n + 1}. 股票' not in body
    assert len(workbook_tables(data['detail_snapshot'])[-1][1]) == 8
    assert card['elements'][-2]['actions'][0]['url'] == URL
    assert card['config']['wide_screen_mode'] is False
    assert data == before


def test_summary_excludes_observation_and_explains_unknown_without_fake_zero():
    data = payload(3)
    row = data['detail_snapshot']['programs'][0]['cells'][2]
    row[6:10] = ['待核验', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN']
    sealed(data)
    body = text(mobile_card(data, URL))
    assert '另有 1 项推荐状态待核验' in body
    assert '连续天数待核验' in body and '开盘基准待形成' in body
    assert '000000.SZ' not in body and '000002.SZ' not in body  # outside groups
    assert '连续0日' not in body and '累计0.00%' not in body
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


def test_many_packages_still_have_one_bounded_card_and_all_desktop_details():
    data = payload(7)
    p = data['detail_snapshot']['programs'][0]
    data['detail_snapshot']['programs'] = [dict(copy.deepcopy(p), program_id=f'p{i}',
        program_name='策略包中文名称' * 20 + str(i)) for i in range(100)]
    sealed(data)
    card = mobile_card(data, URL, top_n=5)
    assert len(json.dumps(card, ensure_ascii=True).encode()) <= CARD_BUDGET
    assert '/100 个策略包' in text(card) and '其余策略及全部股票' in text(card)
    assert len(overview_table(data['detail_snapshot'])) == 101
    assert len(workbook_tables(data['detail_snapshot'])) == 102


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


def test_preview_is_explicitly_labeled_and_default_top_n_is_finite():
    data = payload()
    data['subject'] = '【测试】AIstock 手机摘要'
    card = mobile_card(data, URL)
    assert '非新的荐股通知' in text(card)
    assert MOBILE_TOP_N in (3, 5)
