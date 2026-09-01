from __future__ import annotations

import argparse
import shlex
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch Advisory M4A price-range training in WSL."
    )
    parser.add_argument("--request-windows", required=True)
    parser.add_argument("--request-wsl", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--conda-env", default="rdagent-gpu")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    request = Path(args.request_windows).resolve()
    if not request.is_file():
        raise FileNotFoundError(f"price-range training request does not exist: {request}")
    command = " && ".join(
        [
            "source /home/lc999/miniconda3/etc/profile.d/conda.sh",
            f"conda activate {shlex.quote(args.conda_env)}",
            "export MALLOC_ARENA_MAX=2",
            "export PYTHONUNBUFFERED=1",
            f"cd {shlex.quote(args.repository_root_wsl)}",
            "python scripts/wsl/advisory_price_range_train.py "
            f"--request {shlex.quote(args.request_wsl)}",
        ]
    )
    completed = subprocess.run(["wsl", "bash", "-lc", command], check=False)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
