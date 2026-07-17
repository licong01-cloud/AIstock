"""Safe Phase 0 readiness and cache bootstrap helper.

This command intentionally does not connect to PostgreSQL, create roles, grant
privileges, execute DDL, edit .gitignore, install dependencies, or start a
service.  Phase 0 consumes existing read-only QE/market data and needs only an
isolated artifact-cache directory.  Future Phase 1 schemas require a separate
approved Python bootstrap and production DDL gate.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import stat
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_DIR = Path("tmp/hmm_evolution_cache")
CONFIRMATION = "phase0-cache-only"


class BootstrapError(RuntimeError):
    """Raised when a requested bootstrap action violates the Phase 0 boundary."""


@dataclass(frozen=True)
class ReadinessReport:
    command: str
    state: str
    repo_root: str
    cache_dir: str
    cache_exists: bool
    cache_is_directory: bool
    cache_is_reparse_point: bool
    required_python_modules: dict[str, bool]
    production_ddl_gate: str = "noop"
    production_backend_dependency_gate: str = "noop"
    production_frontend_dependency_gate: str = "noop"
    database_mutation: str = "refused"
    phase1_schema_bootstrap: str = "not_part_of_phase0"


def _is_reparse(path: Path) -> bool:
    if path.is_symlink():
        return True
    is_junction = getattr(path, "is_junction", None)
    if callable(is_junction) and is_junction():
        return True
    try:
        attrs = getattr(path.lstat(), "st_file_attributes", 0)
    except FileNotFoundError:
        return False
    return bool(attrs & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _resolve_cache_dir(value: str) -> Path:
    configured = Path(value)
    candidate = configured if configured.is_absolute() else REPO_ROOT / configured
    candidate = candidate.resolve(strict=False)
    allowed_root = (REPO_ROOT / "tmp").resolve(strict=False)
    try:
        common = Path(os.path.commonpath((str(allowed_root), str(candidate))))
    except ValueError as exc:
        raise BootstrapError(f"cache directory is outside repo tmp/: {candidate}") from exc
    if os.path.normcase(str(common)) != os.path.normcase(str(allowed_root)):
        raise BootstrapError(f"cache directory is outside repo tmp/: {candidate}")
    if candidate.exists() and _is_reparse(candidate):
        raise BootstrapError(f"cache directory must not be a reparse point: {candidate}")
    return candidate


def _module_status() -> dict[str, bool]:
    return {
        name: importlib.util.find_spec(name) is not None
        for name in ("pandas", "pydantic", "psycopg2")
    }


def build_report(command: str, cache_dir: Path) -> ReadinessReport:
    return ReadinessReport(
        command=command,
        state="ready" if all(_module_status().values()) else "blocked",
        repo_root=str(REPO_ROOT),
        cache_dir=str(cache_dir),
        cache_exists=cache_dir.exists(),
        cache_is_directory=cache_dir.is_dir(),
        cache_is_reparse_point=_is_reparse(cache_dir),
        required_python_modules=_module_status(),
    )


def _emit(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return
    for key, value in payload.items():
        print(f"{key}: {value}")


def command_plan(args: argparse.Namespace) -> int:
    cache_dir = _resolve_cache_dir(args.cache_dir)
    report = build_report("plan", cache_dir)
    payload = asdict(report)
    payload["planned_actions"] = [
        "verify Python runtime dependencies",
        "optionally create one isolated cache directory",
    ]
    payload["forbidden_actions"] = [
        "connect to or mutate PostgreSQL",
        "create users, schemas, tables, or grants",
        "edit .gitignore or tracked files",
        "start backend/frontend/TDX services",
    ]
    _emit(payload, as_json=args.json)
    return 0 if report.state == "ready" else 2


def command_verify(args: argparse.Namespace) -> int:
    cache_dir = _resolve_cache_dir(args.cache_dir)
    report = build_report("verify", cache_dir)
    blocking: list[str] = []
    if not all(report.required_python_modules.values()):
        blocking.append("required Python module is missing")
    if report.cache_exists and not report.cache_is_directory:
        blocking.append("cache path exists but is not a directory")
    if report.cache_is_reparse_point:
        blocking.append("cache directory is a reparse point")
    payload = asdict(report)
    payload["state"] = "blocked" if blocking else "ready"
    payload["blocking"] = blocking
    _emit(payload, as_json=args.json)
    return 0 if not blocking else 2


def command_bootstrap_cache(args: argparse.Namespace) -> int:
    cache_dir = _resolve_cache_dir(args.cache_dir)
    if not args.apply or args.confirm != CONFIRMATION:
        raise BootstrapError(
            "cache bootstrap requires --apply --confirm phase0-cache-only"
        )
    cache_dir.mkdir(parents=True, exist_ok=True)
    if _is_reparse(cache_dir) or not cache_dir.is_dir():
        raise BootstrapError(f"unsafe cache directory after bootstrap: {cache_dir}")
    payload = asdict(build_report("bootstrap-cache", cache_dir))
    payload["state"] = "applied"
    payload["idempotent"] = True
    payload["tracked_files_modified"] = False
    _emit(payload, as_json=args.json)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plan or bootstrap the Phase 0 HMM cache without DB mutation."
    )
    subparsers = parser.add_subparsers(dest="command")

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
        subparser.add_argument("--json", action="store_true")

    plan = subparsers.add_parser("plan", help="Print the non-mutating Phase 0 plan.")
    add_common(plan)
    plan.set_defaults(func=command_plan)

    verify = subparsers.add_parser("verify", help="Verify dependencies and cache path.")
    add_common(verify)
    verify.set_defaults(func=command_verify)

    bootstrap = subparsers.add_parser(
        "bootstrap-cache",
        help="Create only the isolated cache directory after explicit confirmation.",
    )
    add_common(bootstrap)
    bootstrap.add_argument("--apply", action="store_true")
    bootstrap.add_argument("--confirm")
    bootstrap.set_defaults(func=command_bootstrap_cache)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        args = parser.parse_args(["plan"])
    try:
        return int(args.func(args))
    except BootstrapError as exc:
        print(f"deploy_hmm_data_source: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
