from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv
from pydantic import TypeAdapter

from .entry_price_contracts import Sha256
from .entry_price_role_binding import EntryPriceRoleBindingV1, EntryPriceRoleStore
from .errors import AdvisoryModelFirstError
from .research_control import _exclusive_file_lock, _write_atomic_text


def _hash_or_none(value):
    if value == "NONE":
        return None
    try:
        return TypeAdapter(Sha256).validate_python(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected a lowercase SHA256 or explicit NONE") from exc


def build_parser():
    parser = argparse.ArgumentParser(description="Advisory ENTRY_PRICE role configuration, never backend control")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("inspect", "prepare-binding", "apply-binding", "rollback-binding"):
        command = commands.add_parser(name)
        command.add_argument("--model-root", type=Path, required=True)
        command.add_argument("--program-id", required=True)
        command.add_argument("--binding-version-id", required=True)
        if name != "inspect":
            command.add_argument("--env-file", type=Path, required=True)
            command.add_argument("--expected-current-hash", type=_hash_or_none, required=True)
        if name in {"prepare-binding", "apply-binding"}:
            command.add_argument("--spec", type=Path, required=True)
        if name in {"apply-binding", "rollback-binding"}:
            command.add_argument("--authorization-ref", required=True)
        if name == "rollback-binding":
            target = command.add_mutually_exclusive_group(required=True)
            target.add_argument("--disable", action="store_true")
            target.add_argument("--target-role-hash", type=_hash_or_none)
    return parser


def main(argv: Sequence[str] | None = None):
    args = build_parser().parse_args(argv)
    try:
        if args.command != "inspect":
            if not args.env_file.is_file():
                raise FileNotFoundError("explicit environment file is unavailable")
            load_dotenv(args.env_file, override=True)
        store = EntryPriceRoleStore()
        identity = dict(model_root=args.model_root, program_id=args.program_id, binding_version_id=args.binding_version_id)
        if args.command == "inspect":
            current = store.read(**identity)
            result = {
                "configured": current is not None, "enabled": current[1]["enabled"] if current else False,
                "role": current[0].model_dump(mode="json") if current else None,
                "active": current[1] if current else None,
            }
        elif args.command == "prepare-binding":
            spec = json.loads(args.spec.read_text(encoding="utf-8"))
            role = store.prepare(**identity, expected_current_role_sha256=args.expected_current_hash, **spec)
            root = args.model_root.resolve()
            path = root / "entry_price_binding_requests" / f"{role.role_sha256}.json"
            if path.resolve() != path.absolute():
                raise ValueError("binding request path cannot be redirected by a symlink")
            payload = role.model_dump_json() + "\n"
            path.parent.mkdir(parents=True, exist_ok=True)
            with _exclusive_file_lock(path.with_suffix(".lock")):
                if path.exists():
                    if path.read_text(encoding="utf-8") != payload:
                        raise ValueError("binding request differs from its immutable identity")
                else:
                    _write_atomic_text(path, payload, replace_existing=False)
            result = {"status": "PREPARED_NOT_ACTIVATED", "role_sha256": role.role_sha256, "request_path": path.as_posix()}
        elif args.command == "apply-binding":
            role = EntryPriceRoleBindingV1.model_validate_json(args.spec.read_text(encoding="utf-8"))
            if (role.program_id, role.binding_version_id) != (args.program_id, args.binding_version_id):
                raise ValueError("prepared entry role targets another Program/binding")
            result = store.publish(role, model_root=args.model_root, expected_current_role_sha256=args.expected_current_hash,
                                   authorization_ref=args.authorization_ref)
        else:
            if not args.disable and args.target_role_hash is None:
                raise ValueError("rollback target must be a concrete role hash; use --disable explicitly")
            result = store.rollback(**identity, expected_current_role_sha256=args.expected_current_hash,
                                    target_role_sha256=args.target_role_hash, authorization_ref=args.authorization_ref)
        print(json.dumps({"command": args.command, **result, "database_written": False, "backend_restarted": False}, ensure_ascii=False, sort_keys=True))
        return 0
    except AdvisoryModelFirstError as exc:
        print(json.dumps({"status": "ERROR", "reason_code": exc.reason_code, "message": str(exc)}, ensure_ascii=False))
        return 2
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print(json.dumps({"status": "ERROR", "reason_code": "ADVISORY_ENTRY_ROLE_INPUT_INVALID", "error_type": type(exc).__name__}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
