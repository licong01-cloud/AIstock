from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]


if __name__ == "__main__":
    raise SystemExit(
        subprocess.call(
            [sys.executable, str(ROOT / "scripts" / "aistock_validate.py"), "record", *sys.argv[1:]],
            cwd=ROOT,
        )
    )
