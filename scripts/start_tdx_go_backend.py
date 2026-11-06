"""User-invoked TDX launcher; explicit existing configuration, no default secrets."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

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
    dirty = subprocess.run(['git', '-C', str(ROOT), 'status', '--porcelain', '--untracked-files=all', '--',
                            'tdx-api-main', 'scripts/start_tdx_go_backend.py'],
                           check=True, capture_output=True, text=True).stdout.strip()
    if not re.fullmatch('[0-9a-f]{40}', revision) or dirty:
        raise LaunchConfigurationError('TDX source must have a clean full Git revision')
    env.update(GOPROXY='off', GOSUMDB='off', GOTOOLCHAIN='local', GOENV='off',
               GOWORK='off', GOFLAGS='', AISTOCK_TDX_SOURCE_REVISION=revision)
    base = Path(env.get('AISTOCK_TDX_RUNTIME_DIR', str(ROOT.parent / 'AIstock_runtime' / 'tdx')))
    if not base.is_absolute() or base.resolve().is_relative_to(ROOT.resolve()):
        raise LaunchConfigurationError('TDX runtime directory must be absolute and outside source')
    if target == 'dev' and base.drive.upper() != 'X:':
        raise LaunchConfigurationError('DEV requires an explicit X-drive TDX runtime directory')
    directory = base / (target if target == 'production' else 'dev-' + env['TDX_HTTP_PORT'])
    return [str(directory / 'tdx-backend.exe')], env


def build_fingerprint(env: dict[str, str]) -> str:
    """Hash tracked TDX inputs, not unrelated changes to the repository HEAD."""
    files = subprocess.run(
        ['git', '-C', str(ROOT), 'ls-files', '-z', '--', 'tdx-api-main',
         'scripts/start_tdx_go_backend.py'], check=True, capture_output=True,
    ).stdout.decode('utf-8').split('\0')
    digest = hashlib.sha256()
    settings = {key: env.get(key, '') for key in (
        'GOOS', 'GOARCH', 'CGO_ENABLED', 'GOEXPERIMENT', 'GOENV', 'GOWORK', 'GOFLAGS',
    )}
    digest.update(json.dumps(settings, sort_keys=True).encode())
    for relative in sorted(path for path in files if path and not path.endswith(('_test.go', '.exe'))):
        path = ROOT / relative
        if path.is_symlink() or not path.resolve().is_relative_to(ROOT.resolve()):
            raise LaunchConfigurationError('TDX build input may not escape source')
        digest.update(relative.encode() + b'\0' + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def binary_digest(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def cached_build(binary: Path, fingerprint: str) -> dict | None:
    manifest = binary.with_suffix('.json')
    if binary.is_symlink() or manifest.is_symlink():
        raise LaunchConfigurationError('TDX executable and receipt may not be links')
    if not binary.exists() and not manifest.exists():
        return None
    try:
        receipt = json.loads(manifest.read_text(encoding='utf-8'))
        if (receipt.get('schema_version') != 'aistock_tdx_build_v1'
                or not re.fullmatch('[0-9a-f]{40}', str(receipt.get('source_revision', '')))
                or receipt.get('binary_sha256') != binary_digest(binary)):
            raise ValueError('invalid receipt')
    except (OSError, ValueError, AttributeError) as exc:
        raise LaunchConfigurationError('TDX executable/receipt is incomplete or invalid; use --build-only --rebuild') from exc
    return receipt if receipt.get('input_sha256') == fingerprint else None


@contextmanager
def build_lock(directory: Path):
    """OS-owned lock is released even if a console closes during compilation."""
    directory.mkdir(parents=True, exist_ok=True)
    lock = directory / 'build.lock'
    if lock.is_symlink():
        raise LaunchConfigurationError('TDX build lock may not be a link')
    with lock.open('a+b') as stream:
        stream.write(b'\0')
        stream.flush()
        stream.seek(0)
        if os.name == 'nt':
            import msvcrt
            try:
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise LaunchConfigurationError('another TDX build/start is checking this executable; retry after it finishes') from exc
            try:
                yield
            finally:
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise LaunchConfigurationError('another TDX build/start owns this executable') from exc
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)


def ensure_binary(binary: Path, env: dict[str, str], *, rebuild: bool = False) -> dict:
    with build_lock(binary.parent):
        fingerprint = build_fingerprint(env)
        receipt = None if rebuild else cached_build(binary, fingerprint)
        if receipt is not None:
            return {**receipt, 'build_action': 'reused'}
        # The running executable is never overwritten in place. Windows denies
        # replacement while in use; build failure leaves the last receipt intact.
        candidate = binary.with_name('tdx-backend.next.exe')
        manifest_candidate = binary.with_name('tdx-backend.next.json')
        if any(path.is_symlink() for path in (binary, candidate, manifest_candidate, binary.with_suffix('.json'))):
            raise LaunchConfigurationError('TDX build outputs may not be links')
        version = subprocess.run(['go', 'version'], env=env, check=True,
                                 capture_output=True, text=True).stdout.strip()
        command = ['go', 'build', '-mod=readonly', '-ldflags',
                   '-X main.buildRevision=' + env['AISTOCK_TDX_SOURCE_REVISION'], '-o', str(candidate), '.']
        build_env = {**env, 'GOTMPDIR': str(binary.parent / 'build-tmp')}
        Path(build_env['GOTMPDIR']).mkdir(exist_ok=True)
        subprocess.run(command, cwd=ROOT / 'tdx-api-main' / 'web', env=build_env, check=True)
        # A concurrent checkout/edit must not publish an incorrectly identified binary.
        current, _ = prepare_launch(Path(env['AISTOCK_TDX_ENV_FILE']), env['TDX_RUNTIME_ENV'])
        if current != [str(binary)] or build_fingerprint(env) != fingerprint:
            raise LaunchConfigurationError('TDX build inputs changed during compilation; executable not published')
        receipt = {'schema_version': 'aistock_tdx_build_v1', 'source_revision': env['AISTOCK_TDX_SOURCE_REVISION'],
                   'input_sha256': fingerprint, 'binary_sha256': binary_digest(candidate),
                   'go_version': version, 'built_at_unix': time.time()}
        manifest_candidate.write_text(json.dumps(receipt, sort_keys=True), encoding='utf-8')
        os.replace(candidate, binary)
        os.replace(manifest_candidate, binary.with_suffix('.json'))
        return {**receipt, 'build_action': 'built'}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database-target', choices=('production', 'dev'), required=True)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--check', action='store_true', help='read configuration and identity only; no service start')
    parser.add_argument('--build-only', action='store_true', help='build or verify the fixed executable, without starting a service')
    parser.add_argument('--rebuild', action='store_true', help='replace an invalid/old build; requires --build-only')
    args = parser.parse_args()
    if args.rebuild and not args.build_only or args.check and args.build_only:
        parser.error('--rebuild requires --build-only; --check and --build-only are exclusive')
    try:
        command, env = prepare_launch(args.env_file, args.database_target)
        env['AISTOCK_TDX_ENV_FILE'] = str(args.env_file.resolve())
        receipt = ({'build_action': 'check_only', 'source_revision': env['AISTOCK_TDX_SOURCE_REVISION']}
                   if args.check else ensure_binary(Path(command[0]), env, rebuild=args.rebuild))
    except (LaunchConfigurationError, OSError, subprocess.CalledProcessError) as exc:
        if isinstance(exc, LaunchConfigurationError):
            parser.error(str(exc))
        parser.error('configuration, source identity, or offline build failed; see build output')
    print(json.dumps({'target': args.database_target, 'database': env['TDX_DB_NAME'],
                      'port': env['TDX_HTTP_PORT'], 'executable': command[0], **receipt,
                      'configuration_only': args.check}), flush=True)
    if args.check or args.build_only:
        return 0
    return subprocess.run(command, cwd=ROOT / 'tdx-api-main' / 'web', env=env, check=False).returncode


if __name__ == '__main__':
    raise SystemExit(main())
