#!/usr/bin/env python
"""Explicit administration entrypoint for the dataset-release control store."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ALLOWLISTED_PROFILE = (REPOSITORY_ROOT / "configs" / "datasets" / "qe_backtest_monthly_v1.yaml").resolve()
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.dataset_release.control_store import (  # noqa: E402
    CONTROL_SCHEMA_VERSION,
    ControlStore,
    ControlStoreError,
    ControlStoreSchemaMismatch,
)
from backend.services.dataset_release.profile import (  # noqa: E402
    DatasetProfile,
    ProfileValidationError,
    load_dataset_profile,
)
from backend.services.dataset_release.errors import public_error_envelope  # noqa: E402


class ControlStoreAdminError(ControlStoreError):
    pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Explicitly initialize or inspect the durable dataset-release control store. "
            "Worker/backend runtime never calls init automatically."
        )
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action, help_text in (
        ("init", "explicitly initialize an empty allowlisted root"),
        ("status", "read and validate an existing allowlisted root"),
        ("migrate", "explicitly apply a registered schema migration"),
    ):
        command = subparsers.add_parser(action, help=help_text)
        command.add_argument("--profile", type=Path, required=True)
        command.add_argument("--control-root", type=Path, required=True)
        command.add_argument("--expected-version", type=int, required=True)
    return parser


def _load_profile(supplied: Path, override: DatasetProfile | None) -> DatasetProfile:
    resolved = supplied.expanduser().resolve(strict=True)
    if resolved != ALLOWLISTED_PROFILE:
        raise ControlStoreAdminError("profile path is not in the admin allowlist")
    return override or load_dataset_profile(resolved)


def _assert_profile_root(profile: DatasetProfile, supplied: Path) -> Path:
    requested = supplied.expanduser().resolve(strict=False)
    configured = str(profile.control_root).replace("/", "\\").casefold()
    actual = str(requested).replace("/", "\\").casefold()
    if configured != actual:
        raise ControlStoreAdminError("control root differs from the allowlisted profile")
    return requested


def _migrate(root: Path, *, expected_version: int) -> tuple[ControlStore, bool]:
    """Fail-closed migration registry; v1->v1 is the sole current no-op path."""

    if expected_version != CONTROL_SCHEMA_VERSION:
        raise ControlStoreAdminError(f"unknown control schema migration target: {expected_version}")
    try:
        return ControlStore(root, expected_version=expected_version), False
    except ControlStoreSchemaMismatch as exc:
        raise ControlStoreAdminError(f"no registered control schema migration to {expected_version}") from exc


def main(
    argv: Sequence[str] | None = None,
    *,
    _profile_override: DatasetProfile | None = None,
) -> int:
    args = _parser().parse_args(argv)
    try:
        profile = _load_profile(args.profile, _profile_override)
        root = _assert_profile_root(profile, args.control_root)
        migration_applied = None
        if args.action == "init":
            store = ControlStore.initialize(root, expected_version=args.expected_version)
        elif args.action == "status":
            store = ControlStore(root, expected_version=args.expected_version)
        else:
            store, migration_applied = _migrate(root, expected_version=args.expected_version)
        result = {
            "ok": True,
            "action": args.action,
            "control_store_id": store.identity["control_store_id"],
            "schema_version": CONTROL_SCHEMA_VERSION,
            "expected_version": args.expected_version,
            "profile": profile.profile,
            "integrity": store.integrity_check(),
        }
        if migration_applied is not None:
            result["migration_applied"] = migration_applied
            result["from_version"] = CONTROL_SCHEMA_VERSION
            result["to_version"] = CONTROL_SCHEMA_VERSION
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except (
        ControlStoreError,
        OSError,
        ProfileValidationError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    **public_error_envelope(
                        exc,
                        fallback_code="DATASET_RELEASE_CONTROL_STORE_ADMIN_FAILED",
                    ),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
