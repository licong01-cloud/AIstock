from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv
from pydantic import ValidationError

from .entry_price_confirmation import AdvisoryEntryPriceConfirmationService, inspect_entry_price_confirmation
from .errors import AdvisoryModelFirstError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Advisory ENTRY_PRICE frozen historical confirmation; no training or DB writes")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("prepare", "predict", "settle", "evaluate", "inspect"):
        command = commands.add_parser(name)
        command.add_argument("--model-root", type=Path, required=True)
        command.add_argument("--output-root", type=Path, required=True)
        command.add_argument("--spec" if name == "prepare" else "--request", type=Path, required=True)
        if name in {"prepare", "predict", "settle"}:
            command.add_argument("--env-file", type=Path, required=True)
        if name in {"predict", "settle", "evaluate"}:
            command.add_argument("--qe-exclusive-slot", type=Path,
                                 help="Optional legacy coordination evidence; concurrent replay uses local capacity checks")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        env_file = getattr(args, "env_file", None)
        if env_file is not None:
            if not env_file.is_file():
                raise FileNotFoundError("explicit environment file is unavailable")
            load_dotenv(env_file, override=True)
        service = AdvisoryEntryPriceConfirmationService()
        if args.command == "prepare":
            spec = json.loads(args.spec.read_text(encoding="utf-8"))
            path = service.prepare(spec=spec, model_root=args.model_root, output_root=args.output_root)
            payload = {"stage": "PREPARED", "request_path": path.as_posix()}
        elif args.command == "inspect":
            payload = inspect_entry_price_confirmation(request_path=args.request, output_root=args.output_root)
        else:
            slot = json.loads(args.qe_exclusive_slot.read_text(encoding="utf-8")) if args.qe_exclusive_slot else None
            payload = getattr(service, args.command)(request_path=args.request, model_root=args.model_root,
                                                   output_root=args.output_root, exclusive_slot=slot)
        print(json.dumps({"command": args.command, **payload}, ensure_ascii=False, sort_keys=True, allow_nan=False))
        return 0  # Success of a stage is deliberately NOT model confirmation.
    except AdvisoryModelFirstError as exc:
        waiting = exc.reason_code.endswith(("NOT_MATURE", "RESOURCE_WAITING", "DEFERRED_BUDGET"))
        print(json.dumps({"status": "WAITING" if waiting else "ERROR", "reason_code": exc.reason_code,
                          "message": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 3 if waiting else 2
    except (OSError, ValueError, TypeError, KeyError, ValidationError) as exc:
        print(json.dumps({"status": "ERROR", "reason_code": "ADVISORY_ENTRY_CONFIRMATION_INPUT_INVALID",
                          "error_type": type(exc).__name__}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
