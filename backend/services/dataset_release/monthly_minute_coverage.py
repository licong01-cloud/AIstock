"""Bounded historical minute coverage repair shared by SOURCE and corrections.

The frozen calendar is the timestamp authority, including inherited 241-bar
days. Facts are read from PostgreSQL, never inferred from daily prices. This
module produces bytes; publication owns their copy-on-write installation.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import numpy as np

from .canonical_stock_transformer import _normalize_raw_values, _transform_raw_row
from .monthly_build_resume import _plain, _signature
from .stock_schema import QLIB_STOCK_FIELDS


def calendar_sessions(path: Path) -> dict[str, list[str]]:
    rows = path.read_text(encoding='utf-8').splitlines()
    if not rows or rows != sorted(set(rows)):
        raise ValueError('frozen minute calendar must be strictly ordered and unique')
    result = defaultdict(list)
    for stamp in rows:
        datetime.fromisoformat(stamp)
        result[stamp[:10]].append(stamp)
    return dict(result)


def historical_catalog_gaps(spans, *, provider: Path, days, before: date):
    """Inspect only catalog intervals, not all historical market payloads."""
    catalog = defaultdict(list)
    for line in (provider / 'instruments/all.txt').read_text(encoding='utf-8').splitlines():
        code, start, end = line.split('\t')
        catalog[code.upper()].append((start[:10], end[:10]))
    days = sorted({str(day)[:10] for day in days if str(day)[:10] < before.isoformat()})
    holes = set()
    for code, start, end in spans:
        start, end = str(start)[:10], str(end)[:10]
        intervals = catalog[code.upper()]
        if any(a <= start and b >= min(end, before.isoformat()) for a, b in intervals):
            continue
        for day in days[bisect_left(days, start):bisect_right(days, end)]:
            if not any(a <= day <= b for a, b in intervals):
                holes.add((code.upper(), day))
    return [{'ts_code': code, 'trade_date': day} for code, day in sorted(holes)]


def capture_historical_coverage(*, prefix, pit, cutoff, connection):
    """Capture only historical catalog holes in the exported SOURCE snapshot.

    Empty source days remain explicit and must still be explained by the
    existing consumer's suspension/exclusion rules. No synthetic bars appear.
    """
    from .monthly_minute_repair import capture_database_minutes
    provider = prefix.root / Path(prefix.manifest['components']['minute_meta_export']['path']).parent
    sessions = calendar_sessions(provider / 'calendars/1min.txt')
    # Some inherited calendars contain a 13:00 placeholder; it is not a bar.
    sessions = {day: [t for t in rows if t[11:] != '13:00:00'] for day, rows in sessions.items()}
    keys = historical_catalog_gaps([(s.ts_code, s.eligible_start, s.eligible_end) for s in pit.spans],
        provider=provider, days=sessions, before=cutoff.replace(day=1))
    if not keys:
        return None
    return capture_database_minutes({'stock_dates': keys}, cutoff=cutoff, frozen_sessions=sessions,
                                    connection=connection, allow_absent=True)


def automatic_repair_plan(plan, *, build_inputs, cas, operation_id):
    """Bridge sealed SOURCE delta to the existing idempotent BUILD patcher."""
    reference = build_inputs.get('historical_minute_coverage_ref')
    if reference is None:
        return plan
    from .monthly_build_resume import load_sealed_build_json
    from .monthly_minute_repair import SCHEMA, BINDING
    authority = load_sealed_build_json(cas, reference)
    if authority.get('schema_version') != SCHEMA:
        raise ValueError('historical minute coverage SOURCE schema differs')
    explicit = plan.get(BINDING)
    if explicit is not None:
        other = load_sealed_build_json(cas, explicit['authority_ref'])
        keyed = {(r['ts_code'], r['trade_time']): r for r in authority['facts']}
        for row in other['facts']:
            key = row['ts_code'], row['trade_time']
            if key in keyed and keyed[key] != row:
                raise ValueError('automatic and explicit minute facts conflict')
            keyed[key] = row
        authority = {**authority, 'facts': [keyed[key] for key in sorted(keyed)],
            'row_count': len(keyed), 'parent_authority_refs': [reference, explicit['authority_ref']]}
        authority['stock_dates'] = len({(r['ts_code'], r['trade_time'][:10]) for r in keyed.values()})
        reference = cas.put_json(authority).as_dict()
    return {**plan, BINDING: {'schema_version': SCHEMA, 'operation_id': operation_id,
        'target_cutoff': plan['target_cutoff'],
        'predecessor_manifest_sha256': plan['predecessor']['dataset_manifest_sha256'],
        'authority_ref': reference, 'inputs': authority['inputs'],
        'automatic_population_history_delta': True}}


def _array(path):
    _plain(path)
    if path.is_symlink():
        raise ValueError('minute repair cannot follow a symlink')
    signature = _signature(path)
    raw = path.read_bytes()
    if _signature(path) != signature or len(raw) < 8 or len(raw) % 4:
        raise ValueError('minute input changed or has invalid dimensions')
    values = np.frombuffer(raw, dtype='<f4')
    if not np.isfinite(values[0]) or values[0] < 0 or values[0] != int(values[0]):
        raise ValueError('minute input offset is invalid')
    return values


def patch_minute_fields(*, rows, daily: Path, minute: Path, day_calendar, minute_calendar, checkpoint=lambda: None):
    """Fill exact missing cells of all 12 fields, preserving finite old facts."""
    positions = {stamp: i for i, stamp in enumerate(minute_calendar)}
    if len(positions) != len(minute_calendar) or list(positions) != sorted(positions):
        raise ValueError('minute repair calendar is duplicated or unordered')
    day_positions = {day[:10]: i for i, day in enumerate(day_calendar)}
    grouped = defaultdict(list)
    keys = set()
    for row in rows:
        code, stamp = row['ts_code'].lower(), row['trade_time']
        if stamp not in positions or (code, stamp) in keys:
            raise ValueError('minute repair timestamp outside frozen calendar or duplicated')
        keys.add((code, stamp))
        grouped[code].append(row)
    patches, filled = {}, 0
    for code, facts in sorted(grouped.items()):
        checkpoint()
        facts.sort(key=lambda row: row['trade_time'])
        anchors = {}
        for field in ('factor', 'up_limit_price', 'down_limit_price', 'prev_close'):
            values = _array(daily / f'features/{code}/{field}.day.bin')
            by_day = {}
            for row in facts:
                day = row['trade_time'][:10]
                pos = day_positions.get(day)
                index = -1 if pos is None else pos - int(values[0]) + 1
                if index <= 0 or index >= len(values) or not np.isfinite(values[index]) or values[index] <= 0:
                    raise ValueError(f'frozen daily construction field invalid: {code}/{day}/{field}')
                by_day[day] = float(values[index])
            anchors[field] = by_day
        transformed = []
        for row in facts:
            stamp, day = row['trade_time'], row['trade_time'][:10]
            normalized = _normalize_raw_values(row, source='minute', ordinal=len(transformed))
            result = _transform_raw_row(normalized, timestamp=datetime.fromisoformat(stamp),
                qfq=anchors['factor'][day], limit={'up_limit': anchors['up_limit_price'][day],
                    'down_limit': anchors['down_limit_price'][day], 'pre_close': anchors['prev_close'][day]})
            transformed.append((positions[stamp], result))
        for field in QLIB_STOCK_FIELDS:
            relative = f'features/{code}/{field}.1min.bin'
            path = minute / relative
            _plain(path)
            values = _array(path) if path.exists() else None
            left, right = min(p for p, _ in transformed), max(p for p, _ in transformed)
            if values is not None:
                old_left, old_right = int(values[0]), int(values[0]) + len(values) - 2
                if old_right >= len(positions):
                    raise ValueError('minute original bounds exceed frozen calendar')
                left, right = min(left, old_left), max(right, old_right)
            patched = np.full(right - left + 2, np.nan, dtype='<f4')
            patched[0] = left
            if values is not None:
                patched[old_left-left+1:old_right-left+2] = values[1:]
            changed = 0
            for position, result in transformed:
                index = position - left + 1
                with np.errstate(over='ignore', invalid='ignore'):
                    value = np.float32(result[field])
                if not np.isfinite(value):
                    raise ValueError('minute correction is not finite float32')
                previous = patched[index]
                if np.isfinite(previous) and previous != value:
                    raise ValueError(f'minute correction would overwrite a valid fact: {code}/{field}')
                if not np.isfinite(previous):
                    if not np.isnan(previous):
                        raise ValueError('minute correction found infinity instead of missing value')
                    patched[index], changed = value, changed + 1
            if changed:
                patches[relative] = patched.tobytes()
                filled += changed
    return patches, {'filled_cells': filled, 'checked_cells': len(keys) * len(QLIB_STOCK_FIELDS),
                     'rows': len(keys), 'fields': list(QLIB_STOCK_FIELDS), 'existing_finite_values_unchanged': True}
