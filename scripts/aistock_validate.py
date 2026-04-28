from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import socket
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "tests" / "aistock_validation" / "templates" / "test_run_record.md"


def _safe_slug(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9._-]+", "-", value)
    return re.sub(r"-+", "-", value).strip("-") or "validation"


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


def _is_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def cmd_record(args: argparse.Namespace) -> int:
    if not TEMPLATE.exists():
        raise SystemExit(f"Missing test run template: {TEMPLATE}")
    now = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    module = _safe_slug(args.module)
    level = _safe_slug(args.level.upper())
    title_slug = _safe_slug(args.title)
    out_dir = ROOT / "tests" / "aistock_validation" / "history" / module
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{now}_{level}_{title_slug}.md"

    text = TEMPLATE.read_text(encoding="utf-8")
    replacements = {
        "{{TITLE}}": args.title,
        "{{MODULE}}": args.module,
        "{{LEVEL}}": args.level.upper(),
        "{{DATE}}": dt.datetime.now().isoformat(timespec="seconds"),
        "{{GIT_COMMIT}}": _git_commit(),
        "{{OPERATOR}}": os.environ.get("USERNAME") or os.environ.get("USER") or "unknown",
    }
    for key, value in replacements.items():
        text = text.replace(key, value)
    out_file.write_text(text, encoding="utf-8")
    print(out_file)
    return 0


def cmd_ports(args: argparse.Namespace) -> int:
    failed = False
    for raw_port in args.ports:
        port = int(raw_port)
        occupied = _is_port_open(port)
        status = "occupied" if occupied else "free"
        print(f"127.0.0.1:{port} {status}")
        if occupied and not args.allow_occupied:
            failed = True
    return 1 if failed else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIstock local validation helper.")
    sub = parser.add_subparsers(dest="command", required=True)

    record = sub.add_parser("record", help="Create a validation run record.")
    record.add_argument("--module", required=True)
    record.add_argument("--level", required=True)
    record.add_argument("--title", required=True)
    record.set_defaults(func=cmd_record)

    ports = sub.add_parser("ports", help="Check localhost port occupancy.")
    ports.add_argument("--allow-occupied", action="store_true")
    ports.add_argument("ports", nargs="+")
    ports.set_defaults(func=cmd_ports)

    return parser


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
