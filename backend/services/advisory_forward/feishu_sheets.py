"""Native Feishu spreadsheets, with durable checkpoints owned by the existing outbox."""
from __future__ import annotations

import copy
import hashlib
import re
from decimal import Decimal, InvalidOperation
from time import monotonic
from urllib.parse import urlsplit

import requests

from .notification_details import DetailDeliveryError, fingerprint, workbook_tables

BASE = 'https://open.feishu.cn/open-apis'
RESOURCE = re.compile(r'^[a-zA-Z0-9_-]+$')


class SheetAPIError(DetailDeliveryError):
    def __init__(self, code, *, rejected=False):
        super().__init__(code)
        self.rejected = rejected


def resource(value):
    if not isinstance(value, str) or not RESOURCE.fullmatch(value):
        raise DetailDeliveryError('FEISHU_RESOURCE_INVALID')
    return value


def document_url(value, token):
    parts = urlsplit(value or '')
    if (parts.scheme != 'https' or not parts.hostname or not parts.hostname.endswith('.feishu.cn')
            or parts.username or parts.password or parts.port or parts.query or parts.fragment
            or parts.path != '/sheets/' + resource(token)):
        raise DetailDeliveryError('FEISHU_DOCUMENT_URL_INVALID')
    return value


class FeishuSheetsClient:
    def __init__(self, app_id, app_secret, chat_id, *, request=None):
        if not all((app_id, app_secret, chat_id)):
            raise DetailDeliveryError('FEISHU_DETAILS_CONFIGURATION_MISSING')
        self.app_id, self._secret, self.chat_id = app_id, app_secret, chat_id
        self.request = request or requests.request
        self._token = None
        self.before_request = None

    @property
    def destination_hash(self):
        return hashlib.sha256((self.app_id + '\n' + self.chat_id).encode()).hexdigest()

    def call(self, method, path, *, body=None, params=None, auth=True):
        if auth and not self._token:
            data = self.call('POST', '/auth/v3/tenant_access_token/internal', auth=False,
                             body={'app_id': self.app_id, 'app_secret': self._secret})
            self._token = data.get('tenant_access_token')
            if not isinstance(self._token, str) or not self._token:
                raise SheetAPIError('FEISHU_AUTH_TOKEN_MISSING')
        if self.before_request:
            self.before_request()
        try:
            response = self.request(method, BASE + path, json=body, params=params,
                                    headers={'Authorization': 'Bearer ' + self._token} if auth else {},
                                    timeout=10, allow_redirects=False)
            data = response.json()
        except Exception:
            raise SheetAPIError('FEISHU_API_OUTCOME_UNKNOWN') from None
        code = data.get('code') if isinstance(data, dict) else None
        if type(code) is not int:
            raise SheetAPIError('FEISHU_API_RESPONSE_INVALID')
        if response.status_code == 200 and code == 0:
            return data.get('data', {}) if auth else data
        # Structured business rejection means the operation did not succeed.
        rejected = code != 0 and response.status_code < 500
        raise SheetAPIError('FEISHU_API_REJECTED_' + str(code), rejected=rejected)

    def create(self, title):
        data = self.call('POST', '/sheets/v3/spreadsheets', body={'title': title})['spreadsheet']
        token = resource(data['spreadsheet_token'])
        return token, document_url(data['url'], token)

    def sheets(self, token):
        return self.call('GET', f'/sheets/v3/spreadsheets/{resource(token)}/sheets/query')['sheets']

    def properties(self, token, requests_):
        return self.call('POST', f'/sheets/v2/spreadsheets/{resource(token)}/sheets_batch_update',
                         body={'requests': requests_})

    def ensure_size(self, token, sheet, rows, columns):
        grid = sheet.get('grid_properties') or {}
        for dimension, key, needed in [('ROWS', 'row_count', rows), ('COLUMNS', 'column_count', columns)]:
            current = grid.get(key)
            if not isinstance(current, int) or current <= 0:
                raise DetailDeliveryError('FEISHU_GRID_SIZE_UNKNOWN')
            if needed > current:
                self.call('POST', f'/sheets/v2/spreadsheets/{resource(token)}/dimension_range',
                          body={'dimension': {'sheetId': resource(sheet['sheet_id']),
                                              'majorDimension': dimension, 'length': needed - current}})

    def values(self, token, sheet, cells):
        column = ''
        size = max(len(r) for r in cells)
        while size:
            size, digit = divmod(size - 1, 26)
            column = chr(65 + digit) + column
        scope = f'{resource(sheet)}!A1:{column}{len(cells)}'
        self.call('PUT', f'/sheets/v2/spreadsheets/{resource(token)}/values',
                  body={'valueRange': {'range': scope, 'values': cells}})
        values = self.call('GET', f'/sheets/v2/spreadsheets/{resource(token)}/values/{scope}',
                           params={'valueRenderOption': 'ToString'})['valueRange']['values']
        def same(actual, expected):
            if isinstance(expected, (int, float)):
                try:
                    return not isinstance(actual, bool) and Decimal(str(actual)) == Decimal(str(expected))
                except InvalidOperation:
                    return False
            return actual == expected

        # The API pads short rows to the requested rectangular width with nulls.
        # Only those absent cells are padding; UNKNOWN and numeric zero stay distinct.
        expected = [r + [None] * (max(len(r) for r in cells) - len(r)) for r in cells]
        if (len(values) != len(expected) or any(len(a) != len(b) or any(
                not same(x, y) for x, y in zip(a, b)) for a, b in zip(values, expected))):
            raise DetailDeliveryError('FEISHU_VALUES_READBACK_MISMATCH')

    def chat_permissions(self, token):
        data = self.call('GET', f'/drive/v1/permissions/{resource(token)}/members', params={'type': 'sheet'})
        return any(m.get('member_type') == 'openchat' and m.get('member_id') == self.chat_id
                   and m.get('perm') in {'view', 'edit', 'full_access'} for m in data['items'])

    def share(self, token):
        self.call('POST', f'/drive/v1/permissions/{resource(token)}/members',
                  params={'type': 'sheet', 'need_notification': 'false'},
                  body={'member_type': 'openchat', 'member_id': self.chat_id, 'perm': 'view'})


class DetailSheetDelivery:
    def __init__(self, client, *, timer=monotonic):
        self.client, self.timer = client, timer

    def deliver(self, payload, checkpoint, send, *, update_test_table=False, prepare_message=None):
        """No database transaction spans a remote operation. Checkpoint renews the lease."""
        tables = workbook_tables(payload['detail_snapshot'])
        state = copy.deepcopy(payload.get('sheet_delivery_state') or {})
        if update_test_table and (not payload['subject'].startswith('【测试】')
                                  or not state.get('document_token') or not state.get('message_accepted')):
            raise DetailDeliveryError('FEISHU_TEST_UPDATE_REQUIRES_SENT_TEST_DOCUMENT')
        deadline = self.timer() + 90

        def save(**changes):
            if self.timer() >= deadline:
                raise DetailDeliveryError('FEISHU_DELIVERY_TIME_LIMIT')
            state.update(changes)
            checkpoint(copy.deepcopy(state))

        def call(function, *args):
            save()  # Fence/renew before every external step, including reads.
            return function(*args)

        self.client.before_request = save
        destination = self.client.destination_hash
        if state.get('destination_hash', destination) != destination:
            raise DetailDeliveryError('FEISHU_DETAIL_DESTINATION_CHANGED')
        save(destination_hash=destination)
        if state.get('message_accepted') and not update_test_table:
            return True
        if state.get('message_started') and not update_test_table:
            raise DetailDeliveryError('FEISHU_MESSAGE_OUTCOME_UNKNOWN_NO_RESEND')
        if not state.get('document_token'):
            if state.get('create_started'):
                raise DetailDeliveryError('FEISHU_CREATE_OUTCOME_UNKNOWN_NO_RECREATE')
            save(create_started=True)
            try:
                token, url = self.client.create(payload['subject'])
            except SheetAPIError as exc:
                if exc.rejected:
                    save(create_started=False)
                raise
            save(document_token=token, document_url=url)
        token = resource(state['document_token'])
        url = document_url(state['document_url'], token)
        from .notification_presentation import PRESENTATION_VERSION
        if state.get('presentation_version') != PRESENTATION_VERSION:
            # Old unsent documents may have the five original tabs. Preserve their
            # verified rows and add the overview; never rewrite a sent notification.
            state['table_verified'] = False
        if not state.get('table_verified'):
            done = set(state.get('completed_tables') or [])
            for title, cells in tables:
                if title in done:
                    continue
                sheets = call(self.client.sheets, token)
                matched = [s for s in sheets if s['title'] == title]
                if len(matched) > 1:
                    raise DetailDeliveryError('FEISHU_WORKSHEET_AMBIGUOUS')
                if not matched:
                    # Reuse the initial empty sheet for the explanatory tab. Other tabs
                    # have deterministic names; after a lost add response query before add.
                    if title == tables[0][0] and len(sheets) == 1 and not done:
                        operation = {'updateSheet': {'properties': {'sheetId': sheets[0]['sheet_id'], 'title': title}}}
                    else:
                        properties = {'title': title}
                        if title == '策略总览':
                            properties['index'] = 0
                        operation = {'addSheet': {'properties': properties}}
                    call(self.client.properties, token, [operation])
                    matched = [s for s in call(self.client.sheets, token) if s['title'] == title]
                if len(matched) != 1:
                    raise DetailDeliveryError('FEISHU_WORKSHEET_NOT_FOUND')
                sid = resource(matched[0]['sheet_id'])
                call(self.client.ensure_size, token, matched[0], len(cells), max(map(len, cells)))
                call(self.client.properties, token, [{'updateSheet': {'properties': {
                    'sheetId': sid, 'frozenRowCount': 1,
                }}}])
                call(self.client.values, token, sid, cells)
                done.add(title)
                save(completed_tables=sorted(done))
            save(table_verified=True, presentation_version=PRESENTATION_VERSION)
        # Verify ACL on each attempt; a prior successful write does not prove access.
        if not call(self.client.chat_permissions, token):
            call(self.client.share, token)
        if not call(self.client.chat_permissions, token):
            raise DetailDeliveryError('FEISHU_CHAT_VIEW_NOT_CONFIRMED')
        save(chat_view_verified=True)
        if update_test_table:
            # Explicit operator-only repair of a labeled test. Production callers
            # never select this path and still require actual message acceptance.
            return True
        message = (f"{payload['detail_snapshot']['decision_date']} 荐股任务已完成。\n"
                   f"目标日 {payload['detail_snapshot']['target_date']}，点击查看全部策略包详情：\n{url}")
        if prepare_message is not None:
            message = prepare_message(payload, url)
        message_sha = fingerprint(message)
        if state.get('message_payload_sha256', message_sha) != message_sha:
            raise DetailDeliveryError('FEISHU_MESSAGE_PAYLOAD_CHANGED')
        # Prepare and validate locally before recording an uncertain remote send.
        save(message_started=True, message_payload_sha256=message_sha)
        # Exceptions preserve message_started; never blindly repeat an uncertain send.
        accepted = send(payload['subject'], message) is True
        if not accepted:
            save(message_started=False)
            raise DetailDeliveryError('FEISHU_DETAIL_MESSAGE_REJECTED')
        save(message_accepted=True)
        return True
