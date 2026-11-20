"""Mobile summary and desktop overview of the same immutable published snapshot."""
from __future__ import annotations

import json
from collections import Counter
from urllib.parse import urlsplit

from .notification_details import CHANGES, GROUPS, HEADERS, DetailDeliveryError, cell, validate_snapshot

PRESENTATION_VERSION = 'advisory_feishu_mobile_full_tables_v2'
MAX_MOBILE_TABLES = 5
CARD_BUDGET = 18 * 1024  # Leave room under the existing 20KB sender limit for signing.


def card_json_bytes(value):
    """Exactly the compact UTF-8 representation used on the dedicated card wire."""
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')


def display(value, limit=80):
    return ' '.join(str(value).lstrip('\u200b').split())[:limit]


def program_rows(program):
    cells = program.get('cells') or []
    if not cells or cells[0] != HEADERS or any(len(row) != len(HEADERS) for row in cells[1:]):
        raise DetailDeliveryError('DETAIL_PRESENTATION_COLUMNS_INVALID')
    return [dict(zip(HEADERS, row)) for row in cells[1:]]


def statistics(program):
    rows = program_rows(program)
    active = [r for r in rows if r['分组'] == GROUPS['IN_SCOPE']]
    groups = Counter(r['分组'] for r in rows)
    changes = Counter(r['推荐状态'] for r in rows)
    return dict(rows=rows, active=active, groups=groups, changes=changes,
                quote_known=sum(isinstance(r['行情快照价(元)'], (int, float)) for r in active),
                return_known=sum(r['荐股期间累计涨跌'] != 'UNKNOWN'
                                 and not str(r['荐股期间累计涨跌']).startswith('不适用') for r in active))


def overview_table(snapshot):
    validate_snapshot(snapshot)
    header = ['策略包', '目标交易日', '在荐', '新入选(已确认)', '持续推荐(已确认)', '移出(已确认)',
              '推荐状态待核验', '观察', '待复核', '待退出', '分组待核验', '名单总行数',
              '在荐有效报价数', '在荐收益可用数', '名单版本', '绑定版本', '快照时间']
    cells = [header]
    for p in snapshot['programs']:
        s = statistics(p)
        cells.append([cell(p['program_name']), snapshot['target_date'], len(s['active']),
                      s['changes'][CHANGES['NEW']], s['changes'][CHANGES['CONTINUING']],
                      s['changes'][CHANGES['REMOVED']], s['changes'][CHANGES['UNKNOWN']],
                      s['groups'][GROUPS['OBSERVATION']], s['groups'][GROUPS['OUTSIDE_PENDING']],
                      s['groups'][GROUPS['OUTSIDE_EXIT']], s['groups'][GROUPS['UNKNOWN']], len(s['rows']),
                      s['quote_known'], s['return_known'], p['list_version_id'], p['binding_version_id'],
                      snapshot['captured_at']])
    return cells


def mobile_rows(program):
    """All current recommendations and exit-related rows; never dedupe or slice."""
    rows = program_rows(program)
    active = [r for r in rows if r['分组'] == GROUPS['IN_SCOPE']]
    exits = [r for r in rows if r['分组'] != GROUPS['IN_SCOPE'] and (
        r['推荐状态'] == CHANGES['REMOVED'] or r['分组'] == GROUPS['OUTSIDE_EXIT'])]
    pending = [r for r in rows if r['分组'] == GROUPS['OUTSIDE_PENDING']
               and r['推荐状态'] != CHANGES['REMOVED']]

    def rank(row):
        value = row['排名']
        return value if type(value) in (int, float) and value > 0 else float('inf')

    # Stable ties/unknown ranks retain the saved-list ordering, not invented ranks.
    selected = sorted(active, key=rank) + exits + pending
    table_rows = []
    for row in selected:
        name = display(row['股票名称'], 30)
        if name == 'UNKNOWN':
            name = '名称待核验'
        if row['分组'] == GROUPS['IN_SCOPE']:
            if rank(row) != float('inf'):
                name = str(row['排名']) + '. ' + name
            days = row['连续推荐(交易日)']
            day_label = str(days) + '日' if type(days) in (int, float) else '天数待核验'
            status = display(row['推荐状态']) + '·' + day_label
        elif row['分组'] == GROUPS['OUTSIDE_EXIT']:
            status = '退出建议' + ('·已移出' if row['推荐状态'] == CHANGES['REMOVED'] else '')
        elif row['推荐状态'] == CHANGES['REMOVED']:
            status = '已移出·' + display(row['分组'])
        else:
            status = '待复核'
        value = display(row['荐股期间累计涨跌'])
        if value == 'UNKNOWN':
            value = '待开盘' if '开盘基准尚未形成' in row['缺失原因'] else '待核验'
        elif value.startswith('不适用'):
            value = '不适用'
        table_rows.append(dict(s=name + '\n' + display(row['股票代码'], 16), t=status, v=value))
    return table_rows, dict(active=len(active), exits=len(exits), pending=len(pending))


def mobile_card(payload, url):
    from .feishu_sheets import document_url
    snapshot = validate_snapshot(payload['detail_snapshot'])
    document_url(url, urlsplit(url).path.rsplit('/', 1)[-1])
    programs = snapshot['programs']
    if not programs or len(programs) > MAX_MOBILE_TABLES:
        raise DetailDeliveryError('FEISHU_MOBILE_FULL_TABLE_COUNT_LIMIT')
    elements = [dict(tag='div', text=dict(tag='plain_text', content=(
        ('【测试】历史名单样例，非新的荐股通知。\n' if payload['subject'].startswith('【测试】') else '') +
        f"决策日 {snapshot['decision_date']} · 目标日 {snapshot['target_date']}\n"
        f"全部 {len(programs)} 个策略包；表内含全量股票，每页10行可翻页。")))]
    for p in programs:
        rows, counts = mobile_rows(p)
        heading = (display(p['program_name'], 80) + '\n' +
                   f"在荐 {counts['active']} · 移出/退出建议 {counts['exits']} · 其他待复核 {counts['pending']}")
        elements.extend([dict(tag='hr'), dict(tag='div', text=dict(tag='plain_text', content=heading))])
        if rows:
            elements.append(dict(tag='table', page_size=10, row_height='middle',
                columns=[dict(name=k, display_name=v, data_type='text', width='auto')
                         for k, v in [('s', '股票'), ('t', '状态'), ('v', '累计涨跌')]], rows=rows))
        else:
            elements.append(dict(tag='div', text=dict(tag='plain_text', content='本包暂无在荐或退出相关股票。')))
    elements.extend([
        dict(tag='action', actions=[dict(tag='button', text=dict(tag='plain_text', content='打开完整荐股表格'),
                                        type='primary', url=url)]),
        dict(tag='note', elements=[dict(tag='plain_text', content=(
            f"快照 {snapshot['captured_at']}；累计涨跌按连续荐股期间计算。\n"
            '移出指不在本次推荐范围；退出建议、待复核分别标注，成交结算另看详情。'))]),
    ])
    card = dict(config={'wide_screen_mode': False},
                header=dict(title=dict(tag='plain_text', content=display(payload['subject'], 80)), template='blue'),
                elements=elements)
    if len(card_json_bytes(card)) > CARD_BUDGET:
        raise DetailDeliveryError('FEISHU_MOBILE_FULL_CARD_TOO_LARGE')
    return card
