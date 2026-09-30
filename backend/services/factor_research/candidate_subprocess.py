"""Run one reviewed factor candidate without OS command-line sized scope data."""
from __future__ import annotations

import argparse
import json
import runpy
import sys
from pathlib import Path


def _regular_file(value: str, label: str) -> Path:
    path = Path(value).expanduser()
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a regular file")
    return path.resolve()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--script", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--instruments-file", required=True)
    args = parser.parse_args()

    script = _regular_file(args.script, "script")
    instruments_path = _regular_file(args.instruments_file, "instruments file")
    instruments = json.loads(instruments_path.read_text(encoding="utf-8"))
    if not isinstance(instruments, list) or not instruments or any(
        not isinstance(item, str) or not item for item in instruments
    ):
        raise ValueError("instruments file must contain a non-empty string list")

    sys.argv = [
        str(script),
        "--data-dir",
        args.data_dir,
        "--output",
        args.output,
        "--start-date",
        args.start_date,
        "--end-date",
        args.end_date,
        "--instruments",
        json.dumps(instruments, ensure_ascii=False, separators=(",", ":")),
    ]
    sys.path.insert(0, str(script.parent))
    runpy.run_path(str(script), run_name="__main__")


if __name__ == "__main__":
    main()
