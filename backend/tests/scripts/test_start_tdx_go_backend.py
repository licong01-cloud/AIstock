"""Launcher preparation never starts a service or exposes credentials."""
import subprocess
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
    monkeypatch.setattr(launcher.subprocess, 'run', lambda args, **kw: subprocess.CompletedProcess(
        args, 0, stdout='' if 'status' in args else 'a' * 40, stderr=''))
    command, env = launcher.prepare_launch(Path('explicit.env'), target)
    assert 'TDX_DB_DSN' not in env and env['TDX_DB_NAME'] == values[prefix + 'NAME']
    assert env['TDX_DB_PASSWORD'] == 'test_only' and env['GOPROXY'] == 'off'
    assert command[:2] == ['go', 'run'] and command[-1] == '.'
    assert '-X main.buildRevision=' + 'a' * 40 in command


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
    monkeypatch.setattr(launcher.subprocess, 'run', lambda args, **kw: subprocess.CompletedProcess(
        args, 0, stdout=' M tdx-api-main/web/server.go' if fault == 'dirty' and 'status' in args
        else '' if 'status' in args else 'a' * 40, stderr=''))
    with pytest.raises(launcher.LaunchConfigurationError):
        launcher.prepare_launch(Path('explicit.env'), 'dev')
