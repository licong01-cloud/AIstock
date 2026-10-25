"""Bounded, read-only cockpit sources; never wait for remote I/O in a request."""
from __future__ import annotations

import json
import subprocess
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Callable

from backend.services.validation.module_registry import REPO_ROOT
from backend.services.validation.platform_health import ValidationPlatformHealthService


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class OverviewSnapshot:
    """One shared in-flight collection per source, including across browser windows."""

    ttl_seconds = 60

    def __init__(self, root: Path = REPO_ROOT, providers: dict[str, Callable] | None = None):
        self.root = Path(root).resolve()
        self._lock = Lock()
        self._context_lock = Lock()
        self._context: tuple[str, dict | None] | None = None
        self._pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="validation-overview")
        self._entries: dict[str, dict] = {}
        self._pending: dict[str, Future] = {}
        self._started: dict[str, float] = {}
        self._providers = providers if providers is not None else {
            'bugs': self._bugs, 'worktrees': self._worktrees,
            'ci': self._ci, 'runners': self._runners,
            'nightly': self._nightly, 'prs': self._prs,
        }

    def read(self) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            for key, provider in self._providers.items():
                future = self._pending.get(key)
                if future is not None and future.done():
                    self._entries[key] = future.result()
                    del self._pending[key]
                previous = self._entries.get(key)
                if key not in self._pending and (previous is None or now - previous['_at'] >= self.ttl_seconds):
                    self._pending[key] = self._pool.submit(self._collect, provider, key)
                    self._started[key] = now
            sections = {}
            for key in self._providers:
                entry = deepcopy(self._entries.get(key, {'state': 'loading', 'data': None, 'collected_at': None}))
                at = entry.pop('_at', None)
                entry['age_seconds'] = round(max(0, now - at), 1) if at is not None else None
                entry['stale'] = at is not None and now - at >= self.ttl_seconds
                entry['refreshing'] = key in self._pending
                if key in self._pending and now - self._started[key] > 20:
                    entry['state'] = 'timeout'
                    entry['error'] = 'source_deadline_exceeded'
                    entry['stale'] = True
                sections[key] = entry
        return {'schema_version': 'aistock_pipeline_overview_v1', 'generated_at': _now(),
                'ttl_seconds': self.ttl_seconds, 'sections': sections,
                'read_only': True, 'production_8001_touched': False}

    @staticmethod
    def _collect(provider: Callable, key: str) -> dict:
        try:
            data, source = provider()
            if not isinstance(data, dict):
                raise ValueError('invalid source shape')
            return {'state': 'available', 'data': data, 'source': source,
                    'collected_at': _now(), '_at': time.monotonic()}
        except Exception as exc:
            # Do not expose credentials, stderr, or raw remote responses to browsers.
            return {'state': 'unavailable', 'data': None, 'source': f'overview/{key} (collection failed)',
                    'error': type(exc).__name__, 'collected_at': _now(), '_at': time.monotonic()}

    def _run(self, args: list[str]) -> str:
        return subprocess.check_output(args, cwd=self.root, text=True, encoding='utf-8',
                                       errors='replace', stderr=subprocess.PIPE, timeout=5)

    def _github(self, path: str) -> Any:
        with self._context_lock:
            if self._context is None:
                service = ValidationPlatformHealthService(repo_root=self.root)
                repo = service._github_repository(self.root).get('repository')
                if not repo:
                    raise ValueError('repository unavailable')
                self._context = (repo, service._github_command_env(self.root))
            repo, env = self._context
        import os

        result = subprocess.check_output(
            ['gh', 'api', f'repos/{repo}/{path}'], cwd=self.root,
            env={**os.environ, **(env or {})}, timeout=5,
            text=True, encoding='utf-8', errors='replace', stderr=subprocess.PIPE,
        )
        return json.loads(result)

    def _bugs(self):
        root = self.root / 'tests/aistock_validation/bugs'
        if not root.is_dir():
            raise FileNotFoundError('BUG registry absent')
        counts: Counter = Counter()
        total = 0
        for path in root.glob('*.json'):
            if path.name == '.bug_id_allocator.json':
                continue
            if total >= 20000 or path.stat().st_size > 2 * 1024 * 1024:
                raise ValueError('registry exceeds cockpit budget')
            item = json.loads(path.read_text(encoding='utf-8-sig'))
            if not item.get('bug_id'):
                raise ValueError('invalid BUG record')
            counts[str(item.get('status') or 'unknown')] += 1
            total += 1
        if not total:
            raise ValueError('empty BUG registry')
        pending = sum(count for state, count in counts.items() if state not in {'fixed', 'verified', 'closed'})
        return {'total': total, 'pending': pending, 'by_status': dict(counts)}, 'canonical BUG registry (single scan, no worktree traversal)'

    def _worktrees(self):
        output = self._run(['git', 'worktree', 'list', '--porcelain'])
        count = sum(line.startswith('worktree ') for line in output.splitlines())
        if not count:
            raise ValueError('empty worktree registry')
        return {'total': count, 'task_worktrees': max(0, count - 1),
                'cleanup_eligibility': 'not_evaluated'}, 'git worktree list --porcelain (no per-worktree scan)'

    @staticmethod
    def _runs(payload):
        if not isinstance(payload, dict) or not isinstance(payload.get('workflow_runs'), list):
            raise ValueError('invalid actions response')
        return payload['workflow_runs']

    def _ci(self):
        data = {}
        for status in ['queued', 'in_progress']:
            payload = self._github(f'actions/runs?status={status}&per_page=20')
            runs = self._runs(payload)
            if not isinstance(payload.get('total_count'), int) or payload['total_count'] < len(runs):
                raise ValueError('invalid action count')
            if any(not run.get('id') or run.get('status') != status for run in runs):
                raise ValueError('invalid action state')
            data[status] = {'total': payload['total_count'], 'items': [
                {key: run.get(key) for key in ['id', 'name', 'status', 'head_branch', 'created_at', 'run_started_at', 'html_url']}
                for run in runs], 'truncated': payload['total_count'] > len(runs)}
        return data, 'GitHub REST actions/runs (queued + in_progress)'

    def _runners(self):
        payload = self._github('actions/runners?per_page=100')
        if not isinstance(payload, dict) or not isinstance(payload.get('runners'), list):
            raise ValueError('invalid runner response')
        runners = []
        for item in payload['runners']:
            labels = [label['name'].lower() for label in item.get('labels', [])]
            if 'aistock-ci' not in labels and 'aistock-ci-security' not in labels:
                continue
            if item.get('status') not in {'online', 'offline'} or not isinstance(item.get('busy'), bool):
                raise ValueError('incomplete runner state')
            runners.append({'id': item['id'], 'name': item['name'], 'status': item['status'],
                            'busy': item['busy'], 'role': 'security' if 'aistock-ci-security' in labels else 'ci',
                            'labels': labels})
        return {'items': runners, 'expected_roles': ['ci', 'security'],
                'missing_roles': sorted({'ci', 'security'} - {item['role'] for item in runners})}, 'GitHub REST actions/runners'

    def _nightly(self):
        runs = self._runs(self._github('actions/workflows/nightly.yml/runs?branch=main&per_page=1'))
        if not runs:
            raise ValueError('Nightly has no observed runs')
        run = runs[0]
        if (not run.get('id') or run.get('status') not in {'queued', 'in_progress', 'completed', 'waiting', 'requested', 'pending'}
                or (run['status'] == 'completed' and not run.get('conclusion'))
                or (run['status'] != 'completed' and run.get('conclusion'))):
            raise ValueError('invalid Nightly run')
        return {'latest_run': {key: run.get(key) for key in [
            'id', 'status', 'conclusion', 'created_at', 'updated_at', 'html_url', 'head_sha', 'event',
        ]}}, 'GitHub REST nightly.yml runs (main)'

    def _prs(self):
        payload = self._github('pulls?state=open&per_page=100')
        if not isinstance(payload, list):
            raise ValueError('invalid PR response')
        return {'observed_open': len(payload), 'truncated': len(payload) == 100,
                'items': [{key: item.get(key) for key in ['number', 'title', 'html_url', 'draft', 'updated_at']}
                          for item in payload[:10]]}, 'GitHub REST pulls?state=open (bounded 100)'


overview_snapshot = OverviewSnapshot()
