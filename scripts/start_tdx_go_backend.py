"""User-invoked TDX launcher; explicit existing configuration, no default secrets."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]


class LaunchConfigurationError(RuntimeError):
    pass


def prepare_launch(env_file: Path, target: str) -> tuple[list[str], dict[str, str]]:
    if target not in {'production', 'dev'}:
        raise LaunchConfigurationError('explicit database target is required')
    values = dotenv_values(env_file)
    prefix = 'TDX_DB_DEV_' if target == 'dev' else 'TDX_DB_'
    keys = ('HOST', 'PORT', 'NAME', 'USER', 'PASSWORD')
    if any(not str(values.get(prefix + key) or '').strip() for key in keys):
        raise LaunchConfigurationError('explicit configuration lacks required TDX database fields')
    try:
        port = int(str(values[prefix + 'PORT']))
    except ValueError as exc:
        raise LaunchConfigurationError('invalid database port') from exc
    if not 1 <= port <= 65535:
        raise LaunchConfigurationError('invalid database port')
    env = {k: v for k, v in os.environ.items() if not k.upper().startswith('TDX_DB_')}
    env.update({'TDX_DB_' + key: str(values[prefix + key]) for key in keys})
    env['TDX_RUNTIME_ENV'] = target
    env['TDX_HTTP_PORT'] = env.get('TDX_HTTP_PORT', '19081' if target == 'dev' else '19080').strip()
    if target == 'dev':
        if env['TDX_DB_NAME'] != 'aistock_dev' or env['TDX_HTTP_PORT'] not in {'19081', '19082'}:
            raise LaunchConfigurationError('DEV requires existing aistock_dev and an isolated port')
        data_dir = Path(env.get('TDX_DATA_DIR', ''))
        if not data_dir.is_absolute() or data_dir.drive.upper() != 'X:':
            raise LaunchConfigurationError('DEV requires an explicit X-drive data directory')
        env['TDX_HTTP_HOST'] = '127.0.0.1'
    elif env['TDX_DB_NAME'] == 'aistock_dev' or env['TDX_HTTP_PORT'] != '19080':
        raise LaunchConfigurationError('production target requires its own database and port 19080')
    revision = subprocess.run(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'],
                              check=True, capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(['git', '-C', str(ROOT), 'status', '--porcelain', '--untracked-files=no', '--',
                            'tdx-api-main', 'scripts/start_tdx_go_backend.py'],
                           check=True, capture_output=True, text=True).stdout.strip()
    if not re.fullmatch('[0-9a-f]{40}', revision) or dirty:
        raise LaunchConfigurationError('TDX source must have a clean full Git revision')
    env.update(GOPROXY='off', GOSUMDB='off', GOTOOLCHAIN='local')
    return ['go', 'run', '-mod=readonly', '-ldflags', '-X main.buildRevision=' + revision, '.'], env


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database-target', choices=('production', 'dev'), required=True)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--check', action='store_true', help='read configuration and identity only; no service start')
    args = parser.parse_args()
    try:
        command, env = prepare_launch(args.env_file, args.database_target)
    except (LaunchConfigurationError, OSError, subprocess.CalledProcessError) as exc:
        if isinstance(exc, LaunchConfigurationError):
            parser.error(str(exc))
        parser.error('configuration or source identity read failed')
    print(json.dumps({'target': args.database_target, 'database': env['TDX_DB_NAME'],
                      'port': env['TDX_HTTP_PORT'], 'source_revision': command[-2].split('=')[-1],
                      'configuration_only': args.check}), flush=True)
    if args.check:
        return 0
    return subprocess.run(command, cwd=ROOT / 'tdx-api-main' / 'web', env=env, check=False).returncode


if __name__ == '__main__':
    raise SystemExit(main())
