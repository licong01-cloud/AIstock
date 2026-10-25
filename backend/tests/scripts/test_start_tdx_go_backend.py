"""Launcher preparation never starts a service or exposes credentials."""
import subprocess
import json
from pathlib import Path

import pytest

from scripts import start_tdx_go_backend as launcher


def test_standalone_entrypoint_uses_shared_whole_package_launcher():
    source = (launcher.ROOT / 'tdx-api-main/web/start.bat').read_text(encoding='utf-8')
    assert 'start_tdx_go_backend.py' in source and 'go run server.go' not in source
    assert '--database-target production' in source and '--env-file' in source


@pytest.mark.parametrize('target', ['production', 'dev'])
def test_explicit_target_replaces_inherited_credentials(monkeypatch, target):
    prefix = 'TDX_DB_DEV_' if target == 'dev' else 'TDX_DB_'
    values = {prefix + key: value for key, value in {
        'HOST': 'localhost', 'PORT': '5432', 'NAME': 'aistock_dev' if target == 'dev' else 'production_target',
        'USER': 'test_user', 'PASSWORD': 'test_only'}.items()}
    monkeypatch.setattr(launcher, 'dotenv_values', lambda _: values)
    monkeypatch.setenv('TDX_DB_DSN', 'inherited_must_not_be_used')
    monkeypatch.setenv('TDX_HTTP_PORT', '19081' if target == 'dev' else '19080')
    monkeypatch.setenv('TDX_DATA_DIR', 'X:/AIstock_dataset_candidates/data_repair_staging/test-launcher')
    monkeypatch.setenv('AISTOCK_TDX_RUNTIME_DIR', 'X:/AIstock-CI/tdx-runtime')
    monkeypatch.setattr(launcher.subprocess, 'run', lambda args, **kw: subprocess.CompletedProcess(
        args, 0, stdout='' if 'status' in args else 'a' * 40, stderr=''))
    command, env = launcher.prepare_launch(Path('explicit.env'), target)
    assert 'TDX_DB_DSN' not in env and env['TDX_DB_NAME'] == values[prefix + 'NAME']
    assert env['TDX_DB_PASSWORD'] == 'test_only' and env['GOPROXY'] == 'off'
    assert len(command) == 1 and command[0].endswith('tdx-backend.exe')
    assert env['AISTOCK_TDX_SOURCE_REVISION'] == 'a' * 40
    assert env['GOENV'] == 'off' and env['GOWORK'] == 'off'


@pytest.mark.parametrize('fault', ['missing', 'invalid_port', 'dev_production_db', 'dirty'])
def test_invalid_configuration_is_rejected_without_start(monkeypatch, fault):
    values = {'TDX_DB_DEV_HOST': 'localhost', 'TDX_DB_DEV_PORT': '5432',
              'TDX_DB_DEV_NAME': 'aistock_dev', 'TDX_DB_DEV_USER': 'test', 'TDX_DB_DEV_PASSWORD': 'test_only'}
    if fault == 'missing':
        values.pop('TDX_DB_DEV_PASSWORD')
    elif fault == 'invalid_port':
        values['TDX_DB_DEV_PORT'] = 'bad'
    elif fault == 'dev_production_db':
        values['TDX_DB_DEV_NAME'] = 'production_target'
    monkeypatch.setattr(launcher, 'dotenv_values', lambda _: values)
    monkeypatch.setenv('TDX_HTTP_PORT', '19081')
    monkeypatch.setenv('TDX_DATA_DIR', 'X:/AIstock_dataset_candidates/data_repair_staging/test-launcher')
    monkeypatch.setenv('AISTOCK_TDX_RUNTIME_DIR', 'X:/AIstock-CI/tdx-runtime')
    monkeypatch.setattr(launcher.subprocess, 'run', lambda args, **kw: subprocess.CompletedProcess(
        args, 0, stdout=' M tdx-api-main/web/server.go' if fault == 'dirty' and 'status' in args
        else '' if 'status' in args else 'a' * 40, stderr=''))
    with pytest.raises(launcher.LaunchConfigurationError):
        launcher.prepare_launch(Path('explicit.env'), 'dev')


def _write_cache(binary, fingerprint='inputs'):
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_bytes(b'previous verified executable')
    receipt = {'schema_version': 'aistock_tdx_build_v1', 'source_revision': 'a' * 40,
               'input_sha256': fingerprint, 'binary_sha256': launcher.binary_digest(binary)}
    binary.with_suffix('.json').write_text(json.dumps(receipt), encoding='utf-8')
    return receipt


def test_cache_hit_never_invokes_go_and_keeps_actual_build_revision(tmp_path, monkeypatch):
    binary = tmp_path / 'runtime' / 'tdx-backend.exe'
    _write_cache(binary)
    monkeypatch.setattr(launcher, 'build_fingerprint', lambda _: 'inputs')
    monkeypatch.setattr(launcher.subprocess, 'run', lambda *a, **k: pytest.fail('cache hit must not invoke Go'))
    receipt = launcher.ensure_binary(binary, {'AISTOCK_TDX_SOURCE_REVISION': 'b' * 40})
    assert receipt['build_action'] == 'reused' and receipt['source_revision'] == 'a' * 40


@pytest.mark.parametrize('fault', ['binary_changed', 'missing_binary', 'missing_manifest', 'invalid_json'])
def test_invalid_cache_is_not_silently_executed_or_rebuilt(tmp_path, fault):
    binary = tmp_path / 'tdx-backend.exe'
    _write_cache(binary)
    if fault == 'binary_changed':
        binary.write_bytes(b'corrupt')
    elif fault == 'missing_binary':
        binary.unlink()
    elif fault == 'missing_manifest':
        binary.with_suffix('.json').unlink()
    else:
        binary.with_suffix('.json').write_text('invalid', encoding='utf-8')
    with pytest.raises(launcher.LaunchConfigurationError, match='incomplete or invalid'):
        launcher.cached_build(binary, 'inputs')


@pytest.mark.parametrize('fault', [None, 'build_failure', 'source_drift', 'in_use'])
def test_build_publication_preserves_previous_build_on_failure(tmp_path, monkeypatch, fault):
    binary = tmp_path / 'tdx-backend.exe'
    previous = _write_cache(binary, 'old_inputs')
    reads = iter(['new_inputs', 'drifted' if fault == 'source_drift' else 'new_inputs'])
    monkeypatch.setattr(launcher, 'build_fingerprint', lambda _: next(reads))
    env = {'AISTOCK_TDX_SOURCE_REVISION': 'b' * 40, 'AISTOCK_TDX_ENV_FILE': 'explicit.env',
           'TDX_RUNTIME_ENV': 'production'}
    monkeypatch.setattr(launcher, 'prepare_launch', lambda *a: ([str(binary)], env))
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        if command[1] == 'build':
            assert command[-1] == '.' and '-mod=readonly' in command
            assert kwargs['cwd'] == launcher.ROOT / 'tdx-api-main' / 'web'
            assert Path(kwargs['env']['GOTMPDIR']).is_relative_to(binary.parent)
            if fault == 'build_failure':
                raise subprocess.CalledProcessError(1, command)
            Path(command[command.index('-o') + 1]).write_bytes(b'new executable')
        return subprocess.CompletedProcess(command, 0, stdout='go version test', stderr='')

    monkeypatch.setattr(launcher.subprocess, 'run', run)
    if fault == 'in_use':
        monkeypatch.setattr(launcher.os, 'replace', lambda *a: (_ for _ in ()).throw(PermissionError('in use')))
    if fault:
        with pytest.raises((launcher.LaunchConfigurationError, OSError, subprocess.CalledProcessError)):
            launcher.ensure_binary(binary, env)
        assert binary.read_bytes() == b'previous verified executable'
        assert json.loads(binary.with_suffix('.json').read_text()) == previous
    else:
        receipt = launcher.ensure_binary(binary, env)
        assert receipt['build_action'] == 'built' and receipt['source_revision'] == 'b' * 40
        assert launcher.cached_build(binary, 'new_inputs') == {k: v for k, v in receipt.items() if k != 'build_action'}
    assert [c[1] for c in commands] == ['version', 'build']


def test_build_input_fingerprint_ignores_unrelated_head_but_detects_shared_go_change(tmp_path, monkeypatch):
    source = tmp_path / 'tdx-api-main' / 'protocol.go'
    source.parent.mkdir()
    source.write_text('package tdx', encoding='utf-8')
    monkeypatch.setattr(launcher, 'ROOT', tmp_path)
    monkeypatch.setattr(launcher.subprocess, 'run', lambda *a, **k: subprocess.CompletedProcess(
        a, 0, stdout=b'tdx-api-main/protocol.go\0', stderr=b''))
    first = launcher.build_fingerprint({'AISTOCK_TDX_SOURCE_REVISION': 'a' * 40})
    assert launcher.build_fingerprint({'AISTOCK_TDX_SOURCE_REVISION': 'b' * 40}) == first
    source.write_text('package tdx // changed shared protocol', encoding='utf-8')
    assert launcher.build_fingerprint({}) != first


def test_build_lock_does_not_allow_overlapping_publication(tmp_path):
    with launcher.build_lock(tmp_path):
        with pytest.raises(launcher.LaunchConfigurationError, match='another TDX build/start'):
            with launcher.build_lock(tmp_path):
                pytest.fail('second publisher entered lock')
