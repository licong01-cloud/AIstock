"""Resume native month CSVs inside one SOURCE-bound unpublished BUILD tree.

Uses the existing canonical writer and patch receipt, not another export format.
Closed legacy files are inspected once; subsequent retries use recorded metadata
signatures and the original writer SHA. No historical price input is reread.
"""
from __future__ import annotations

import hashlib
import itertools
import math
import re
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from .canonical import digest_named_fields
from .daily_minute_materializer import (
    DailyMinuteMaterializationError, DailyMinutePatchPreparationReceipt,
    _assert_plain, _atomic_json, _copy_index_csv_ranges, _load_json, _plain_root,
    _write_ordered_stock_csvs, _zero_safety,
)
from .index_contract import DOMESTIC_INDEX_DEFINITIONS, INDEX_QLIB_FIELDS
from .stock_schema import QLIB_STOCK_FIELDS
from .streaming_artifacts import sha256_file
from .monthly_build_resume import _plain


def _signature(path: Path) -> list[int]:
    _assert_plain(path)
    value = path.stat()
    return [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns]


def _inspect(path: Path, *, code: str, fields: Sequence[str], dataset: str, cutoff) -> dict[str, Any]:
    """Recover absent legacy writer metadata; do not re-export or redo QA."""
    before = _signature(path)
    digest, count, first, last = hashlib.sha256(), 0, None, None
    with path.open('rb') as stream:
        header = stream.readline(1024 * 1024 + 1)
        if header.rstrip(b'\r\n') != (','.join(('date', 'symbol', *fields))).encode('ascii'):
            raise DailyMinuteMaterializationError('retained month CSV header differs')
        digest.update(header)
        while line := stream.readline(1024 * 1024 + 1):
            values = line.rstrip(b'\r\n').split(b',')
            if (len(line) > 1024 * 1024 or not line.endswith(b'\n') or len(values) != len(fields) + 2
                    or values[1] != code.encode('ascii')):
                raise DailyMinuteMaterializationError('retained month CSV row is incomplete or mismatched')
            stamp = values[0].decode('ascii')
            if (len(stamp) != (10 if dataset == 'daily_bin' else 19)
                    or not cutoff.replace(day=1).isoformat() <= stamp[:10] <= cutoff.isoformat()
                    or (last is not None and stamp <= last)):
                raise DailyMinuteMaterializationError('retained month CSV timestamp differs')
            # Only unjournaled legacy output needs this one-time adoption
            # check. Already recorded files are never reparsed on retries.
            try:
                finite = all(math.isfinite(float(value)) for value in values[2:])
            except ValueError:
                finite = False
            if not finite:
                raise DailyMinuteMaterializationError('retained month CSV has a non-finite/non-numeric value')
            digest.update(line)
            first, last, count = first or stamp, stamp, count + 1
    if not count or _signature(path) != before:
        raise DailyMinuteMaterializationError('retained month CSV is empty or changing')
    return {'instrument': code, 'rows': count, 'sha256': digest.hexdigest(),
            'size_bytes': before[2], 'start': first, 'end': last}


class MonthlyCsvResume:
    def __init__(self, root: Path, *, binding: Mapping[str, Any], dataset: str,
                 stock_codes: Sequence[str], index_codes: Sequence[str], cutoff,
                 checkpoint: Callable[[], None] = lambda: None):
        self.root, self.dataset, self.cutoff = Path(root).absolute(), dataset, cutoff
        self.checkpoint = checkpoint
        _plain(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        _plain_root(self.root)
        self.metadata = self.root / '.completed'
        _plain(self.metadata)
        self.metadata.mkdir(exist_ok=True)
        _plain_root(self.metadata)
        self.identity = digest_named_fields('aistock_monthly_csv_completion_v1', binding)
        header = self.metadata / 'binding.json'
        expected = {'schema_version': 'aistock_monthly_csv_completion_v1',
                    'binding': dict(binding), 'binding_digest': self.identity}
        if header.exists():
            _assert_plain(header)
            if _load_json(header) != expected:
                raise DailyMinuteMaterializationError('retained month CSV SOURCE binding differs')
        else:
            _atomic_json(header, expected)
        self.files: list[dict[str, Any]] = []
        allowed, indices = set(stock_codes), set(index_codes)
        if not indices.issubset(item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS):
            raise DailyMinuteMaterializationError('index patch codes exceed authority')
        # A hard kill can leave native temporary files. Preserve their bytes,
        # but do not confuse them with completed CSVs or block a later save.
        for folder, pattern in ((self.root, r'\.([0-9]{6}\.(?:SH|SZ))\.[a-z0-9_]+\.partial\.csv'),
                                (self.metadata, r'\.(?:binding|[0-9]{6}\.(?:SH|SZ|CSI))\.json\.[a-z0-9_]+\.partial')):
            for path in folder.iterdir():
                matched = re.fullmatch(pattern, path.name)
                if matched and (folder == self.metadata or matched[1] in allowed):
                    _signature(path)
                    destination = self.root / '.unfinished'
                    _plain(destination)
                    destination.mkdir(exist_ok=True)
                    _plain_root(destination)
                    target = destination / hashlib.sha256(str(path.relative_to(self.root)).encode('utf-8')).hexdigest()
                    if target.exists():
                        raise DailyMinuteMaterializationError('incomplete CSV preservation target conflicts')
                    path.rename(target)
        last_pulse = time.monotonic()
        for path in sorted(self.root.iterdir()):
            if path.name.startswith('.') or path.suffix != '.csv':
                continue
            code = path.stem
            if code in indices:
                continue
            if code not in allowed:
                raise DailyMinuteMaterializationError('retained month CSV exceeds frozen stock population')
            self.files.append(self.read_file(path, code=code, fields=QLIB_STOCK_FIELDS))
            if time.monotonic() - last_pulse >= 2:
                checkpoint()
                last_pulse = time.monotonic()

    def read_file(self, path: Path, *, code: str, fields: Sequence[str]) -> dict[str, Any]:
        receipt_path = self.metadata / f'{code}.json'
        if receipt_path.exists():
            _assert_plain(receipt_path)
            receipt = _load_json(receipt_path)
            if (receipt.get('binding_digest') != self.identity
                    or receipt.get('file', {}).get('instrument') != code):
                raise DailyMinuteMaterializationError('completed month CSV changed')
            observed = _signature(path)
            if receipt.get('file_signature') != observed:
                if (receipt['file'].get('size_bytes') != observed[2]
                        or sha256_file(path) != receipt['file'].get('sha256')
                        or _signature(path) != observed):
                    raise DailyMinuteMaterializationError('completed month CSV changed')
                _atomic_json(receipt_path, {**receipt, 'file_signature': observed})
            return dict(receipt['file'])
        item = _inspect(path, code=code, fields=fields, dataset=self.dataset, cutoff=self.cutoff)
        self.record(item)
        return item

    def record(self, item: Mapping[str, Any]) -> None:
        path = self.root / f'{item["instrument"]}.csv'
        receipt_path = self.metadata / f'{item["instrument"]}.json'
        receipt = {'binding_digest': self.identity, 'file': dict(item),
                   'file_signature': _signature(path)}
        if receipt_path.exists():
            previous = _load_json(receipt_path)
            if previous.get('binding_digest') != self.identity or previous.get('file') != dict(item):
                raise DailyMinuteMaterializationError('completed month CSV receipt conflicts')
            if previous == receipt:
                return
        _atomic_json(receipt_path, receipt)

    @property
    def completed_codes(self) -> frozenset[str]:
        return frozenset(item['instrument'] for item in self.files)

    def prepare(self, spec, *, rows: Iterable[Mapping[str, Any]], index_codes=(), index_date_ranges=()):
        reused_rows = sum(item['rows'] for item in self.files)
        files = list(self.files)
        # A completed stock is durable independently of a later stock failure.
        # The native writer still performs all value/unit/order validation.
        completed, previous = self.completed_codes, None
        for code, group in itertools.groupby(rows, key=lambda row: str(row.get('instrument', ''))):
            if code in completed or (previous is not None and code <= previous):
                raise DailyMinuteMaterializationError('completed stock was unexpectedly retransformed')
            previous = code
            receipt = _write_ordered_stock_csvs(group, csv_root=self.root, dataset=spec.dataset,
                                               cutoff=spec.cutoff, checkpoint=self.checkpoint)
            if len(receipt['files']) != 1 or receipt['files'][0]['instrument'] != code:
                raise DailyMinuteMaterializationError('native stock CSV completion differs')
            self.record(receipt['files'][0])
            files.extend(receipt['files'])
        files.sort(key=lambda item: item['instrument'])
        index_files, missing = [], []
        for code in index_codes:
            path = self.root / f'{code}.csv'
            if path.exists():
                item = self.read_file(path, code=code, fields=INDEX_QLIB_FIELDS)
                index_files.append({**{k: v for k, v in item.items() if k != 'instrument'}, 'code': code})
            else:
                missing.append(code)
        copied = _copy_index_csv_ranges(spec.index_csv_root, self.root, codes=missing, date_ranges=index_date_ranges)
        for item in copied['files']:
            self.record({**{k: v for k, v in item.items() if k != 'code'}, 'instrument': item['code']})
        index_files.extend(copied['files'])
        index_files.sort(key=lambda item: item['code'])
        total = sum(item['rows'] for item in files)
        if total <= 0 and not index_files:
            raise DailyMinuteMaterializationError('bounded Qlib patch is empty')
        return DailyMinutePatchPreparationReceipt({
            'schema_version': 'dataset_release_daily_minute_patch_csv_v1', 'spec_digest': spec.digest,
            'status': 'PASS', 'dataset': spec.dataset, 'cutoff': spec.cutoff.isoformat(),
            'csv': {'rows': total, 'instruments': [item['instrument'] for item in files], 'files': files,
                    'ranges': {item['instrument']: [item['start'][:10], item['end'][:10]] for item in files}},
            'indices': {'codes': sorted(index_codes), 'files': index_files,
                        'rows': sum(item['rows'] for item in index_files)},
            'actual_work': {'stock_rows_transformed': total - reused_rows,
                            'stock_instruments': len(files) - len(self.files),
                            'stock_rows_reused': reused_rows, 'stock_instruments_reused': len(self.files),
                            'index_rows_copied': copied['rows'], 'index_instruments': len(missing)},
            'memory_contract': {'mode': 'bounded_patch_one_instrument_csv_writer_v1', 'cross_instrument_frames_retained': 0},
            'safety': _zero_safety(),
        })
