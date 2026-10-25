"""Exact, operator-bound SOURCE quality warnings; never a missing-data waiver."""

from __future__ import annotations

from datetime import date
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping

from .canonical import canonical_json_bytes, ensure_sha256


ACCEPTANCE_SCHEMA = 'aistock_monthly_source_quality_acceptance_v1'
BINDING_SCHEMA = 'aistock_monthly_source_quality_binding_v1'
REPORT_SCHEMA = 'aistock_monthly_source_quality_report_v1'
WARNING_SCHEMA = 'aistock_monthly_source_quality_warning_v1'
SOURCE_GATE_SCHEMA = 'aistock_monthly_source_gate_v2'
SOURCE_GATE_QUALITY_FIELDS = frozenset({'quality_status', 'quality_warning_count', 'quality_warning_refs'})
_REASONS = {
    'PROVIDER_VOLUME_PRECISION_VARIANCE': frozenset({'vol'}),
    'KNOWN_PROVIDER_PRICE_VARIANCE': frozenset({'open', 'high', 'low', 'close'}),
}


def source_gate_quality_contract() -> dict[str, Any]:
    """Expose the installed receipt format, not a new publication gate."""
    return {'source_gate_schema': SOURCE_GATE_SCHEMA, 'quality_warning_schema': WARNING_SCHEMA,
            'quality_fields': sorted(SOURCE_GATE_QUALITY_FIELDS), 'missing_data_waived': False}


def validate_source_gate_quality(
    gate: Mapping[str, Any], plan: Mapping[str, Any], *, pinned_sha256s: set[str],
) -> None:
    """Close optional quality fields against the existing exact authorization.

    Warning metadata describes observed finite facts only. It cannot alter
    any physical-coverage count, typed absence or ordinary gate requirement.
    """
    present = set(gate) & SOURCE_GATE_QUALITY_FIELDS
    if not present:
        return
    warnings = gate.get('quality_warning_refs')
    count = gate.get('quality_warning_count')
    if (present != SOURCE_GATE_QUALITY_FIELDS or gate.get('gate_id') != 'minute_price'
            or gate.get('quality_status') != 'ACCEPTED_WITH_WARNINGS'
            or not isinstance(warnings, list) or not warnings
            or type(count) is not int or count != len(warnings)
            or count > gate['observed_count']):
        raise ValueError('quality gate status/count/scope differs')
    acceptance = load_bound_source_quality(plan, operation_id=str(plan.get('operation_id') or ''))
    if acceptance is None:
        raise ValueError('quality gate lacks an operation-bound acceptance')
    authority_sha256 = acceptance_file_sha256(acceptance)
    if authority_sha256 not in pinned_sha256s:
        raise ValueError('quality acceptance bytes are not pinned by SOURCE')
    seen = set()
    for warning in warnings:
        validate_source_quality_warning(warning)
        key = warning['symbol'], warning['trade_date']
        if (key in seen or warning['authority_sha256'] != authority_sha256
                or accepted_parity_warning(acceptance, symbol=key[0],
                    trade_date=date.fromisoformat(key[1]), mismatches=warning['mismatches']) != warning):
            raise ValueError('quality warning is duplicated or differs from accepted observations')
        seen.add(key)


def _keys(value: Any, expected: set[str], label: str) -> None:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError(f'{label} fields differ')


def _entry(entry: Any, target_cutoff: date) -> dict[str, Any]:
    _keys(entry, {'symbol', 'trade_date', 'reason_code', 'mismatches'}, 'quality entry')
    symbol, stamp = entry['symbol'], entry['trade_date']
    if not isinstance(symbol, str) or re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', symbol) is None:
        raise ValueError('quality entry symbol is invalid')
    if not isinstance(stamp, str):
        raise ValueError('quality entry date is invalid')
    day = date.fromisoformat(stamp)
    if day.isoformat() != stamp or not target_cutoff.replace(day=1) <= day <= target_cutoff:
        raise ValueError('quality entry is outside the target month')
    reason = entry['reason_code']
    allowed = _REASONS.get(reason) if isinstance(reason, str) else None
    mismatches = entry['mismatches']
    if allowed is None or not isinstance(mismatches, Mapping) or not mismatches or not set(mismatches) <= allowed:
        raise ValueError('quality reason/fields are unsupported')
    fields = {}
    for name, pair in mismatches.items():
        _keys(pair, {'daily', 'minute'}, 'quality field values')
        if any(type(item) not in (int, float) or not math.isfinite(item) or item < 0 for item in pair.values()):
            raise ValueError('quality field values must be finite real observations')
        if pair['daily'] == pair['minute'] or (name != 'vol' and min(pair.values()) <= 0):
            raise ValueError('quality entry must describe an actual finite difference')
        fields[name] = {part: float(number) for part, number in pair.items()}
    return {**entry, 'mismatches': fields}


def validate_source_quality_warning(value: Any) -> None:
    _keys(value, {'schema_version', 'symbol', 'trade_date', 'reason_code', 'mismatches',
                  'authority_sha256', 'data_modified'}, 'quality warning')
    if value['schema_version'] != WARNING_SCHEMA or value['data_modified'] is not False:
        raise ValueError('quality warning schema or semantics differ')
    ensure_sha256(value['authority_sha256'], field='quality warning authority')
    _entry({name: value[name] for name in ('symbol', 'trade_date', 'reason_code', 'mismatches')},
           date.fromisoformat(value['trade_date']))


def read_quality_evidence(reference: Mapping[str, Any]) -> bytes:
    """Read only a bounded plain file and detect replacement/mutation."""
    _keys(reference, {'path', 'sha256', 'size'}, 'quality evidence')
    ensure_sha256(reference['sha256'], field='quality evidence sha256')
    path = Path(reference['path'])
    if not path.is_absolute() or not path.is_file():
        raise ValueError('quality evidence is not an absolute regular file')
    for item in (path, *path.parents):
        if item.is_symlink() or getattr(item, 'is_junction', lambda: False)():
            raise ValueError('quality evidence must not be linked')
    size = reference['size']
    if type(size) is not int or not 0 < size <= 8 * 1024 * 1024:
        raise ValueError('quality evidence size is invalid')
    before = path.stat()
    if before.st_size != size:
        raise ValueError('quality evidence size differs')
    with path.open('rb') as handle:
        raw = handle.read(size + 1)
    after = path.stat()
    def signature(item):
        return item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns
    if signature(before) != signature(after) or len(raw) != size or hashlib.sha256(raw).hexdigest() != reference['sha256']:
        raise ValueError('quality evidence bytes differ')
    return raw


def validate_source_quality_acceptance(
    value: Any, *, operation_id: str, predecessor_manifest_sha256: str, target_cutoff: date,
    verify_evidence: bool = True,
) -> dict[str, Any]:
    _keys(value, {'schema_version', 'operation_id', 'predecessor_dataset_manifest_sha256',
                  'target_cutoff', 'evidence_refs', 'entries'}, 'quality acceptance')
    if (value['schema_version'] != ACCEPTANCE_SCHEMA or value['operation_id'] != operation_id
            or re.fullmatch(r'dmr_[0-9a-f]{32}', operation_id) is None
            or value['predecessor_dataset_manifest_sha256'] != predecessor_manifest_sha256
            or value['target_cutoff'] != target_cutoff.isoformat()):
        raise ValueError('quality acceptance operation/predecessor/cutoff differs')
    ensure_sha256(predecessor_manifest_sha256, field='quality predecessor manifest')
    refs = value['evidence_refs']
    if not isinstance(refs, list) or not refs or len(refs) > 32:
        raise ValueError('quality evidence references are missing or unbounded')
    seen_refs: set[str] = set()
    for reference in refs:
        _keys(reference, {'path', 'sha256', 'size'}, 'quality evidence')
        if not isinstance(reference['path'], str) or not Path(reference['path']).is_absolute():
            raise ValueError('quality evidence path is invalid')
        ensure_sha256(reference['sha256'], field='quality evidence sha256')
        if type(reference['size']) is not int or not 0 < reference['size'] <= 8 * 1024 * 1024:
            raise ValueError('quality evidence size is invalid')
        if reference['path'] in seen_refs:
            raise ValueError('quality evidence reference is duplicated')
        seen_refs.add(reference['path'])
        if verify_evidence:
            read_quality_evidence(reference)
    entries = value['entries']
    if not isinstance(entries, list) or not entries or len(entries) > 4096:
        raise ValueError('quality acceptance entries are missing or unbounded')
    seen: set[tuple[str, str]] = set()
    normalized = []
    for entry in entries:
        normalized_entry = _entry(entry, target_cutoff)
        key = normalized_entry['symbol'], normalized_entry['trade_date']
        if key in seen:
            raise ValueError('quality entry is duplicated')
        seen.add(key)
        normalized.append(normalized_entry)
    return {**value, 'evidence_refs': [dict(ref) for ref in refs],
            'entries': sorted(normalized, key=lambda entry: (entry['symbol'], entry['trade_date']))}


def acceptance_file_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(value) + b'\n').hexdigest()


def accepted_parity_warning(
    acceptance: Mapping[str, Any] | None, *, symbol: str, trade_date: date,
    mismatches: Mapping[str, Any],
) -> dict[str, Any] | None:
    if acceptance is None:
        return None
    actual = {name: {part: pair[part] for part in ('daily', 'minute')} for name, pair in mismatches.items()}
    for entry in acceptance['entries']:
        if (entry['symbol'], entry['trade_date']) == (symbol, trade_date.isoformat()) and entry['mismatches'] == actual:
            return {'schema_version': WARNING_SCHEMA, **entry,
                    'mismatches': {name: dict(pair) for name, pair in entry['mismatches'].items()},
                    'authority_sha256': acceptance_file_sha256(acceptance), 'data_modified': False}
    return None


def source_quality_report(acceptance: Mapping[str, Any], warnings: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {'schema_version': REPORT_SCHEMA, 'acceptance': dict(acceptance),
            'acceptance_file_sha256': acceptance_file_sha256(acceptance),
            'quality_status': 'ACCEPTED_WITH_WARNINGS' if warnings else 'NO_ACCEPTED_VARIANCE_OBSERVED',
            'quality_warning_count': len(warnings), 'warnings': warnings,
            'accepted_entry_count': len(acceptance['entries']),
            'unused_acceptance_count': len(acceptance['entries']) - len(warnings),
            'data_modified': False, 'missing_data_waived': False}


def load_bound_source_quality(plan: Mapping[str, Any], *, operation_id: str, operation_root: Path | None = None) -> dict[str, Any] | None:
    value = plan.get('monthly_source_quality_inputs')
    reference = plan.get('monthly_source_quality_inputs_ref')
    if value is None and reference is None:
        return None
    if value is None or not isinstance(reference, Mapping) or set(reference) != {'id', 'sha256', 'size'}:
        raise ValueError('quality binding is incomplete')
    relative = Path(reference['id'])
    if relative.is_absolute() or '..' in relative.parts or '\\' in reference['id'] or ':' in reference['id']:
        raise ValueError('quality binding reference is not portable')
    path = operation_root / relative if operation_root is not None else Path(plan.get('monthly_source_quality_binding_path', ''))
    if (not path.is_absolute() or path.name != relative.name or relative.parts[0] != 'inputs'
            or path.parent.parent.name != operation_id):
        raise ValueError('quality binding path differs from operation')
    raw = read_quality_evidence({'path': str(path),
                                'sha256': reference['sha256'], 'size': reference['size']})
    receipt = json.loads(raw)
    normalized = validate_source_quality_acceptance(value, operation_id=operation_id,
        predecessor_manifest_sha256=plan['predecessor']['dataset_manifest_sha256'],
        target_cutoff=date.fromisoformat(plan['target_cutoff']))
    if reference['id'] != f'inputs/quality-{acceptance_file_sha256(normalized)}.json':
        raise ValueError('quality binding name differs from observations')
    if (receipt.get('schema_version') != BINDING_SCHEMA or receipt.get('operation_id') != operation_id
            or receipt.get('inputs') != normalized
            or receipt.get('input_sha256') != acceptance_file_sha256(normalized)
            or not receipt.get('bound_by')
            or any(receipt.get(flag) is not False for flag in ('database_write', 'active_profile_write', 'runtime_action', 'publication_allowed'))):
        raise ValueError('quality binding receipt identity differs')
    return normalized
