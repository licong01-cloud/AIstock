from __future__ import annotations

import argparse
import shlex
import subprocess
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch Advisory M5B outcome calibration in WSL.")
    parser.add_argument("--request-windows", required=True)
    parser.add_argument("--request-wsl", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--conda-env", default="rdagent-gpu")
    args = parser.parse_args()
    request = Path(args.request_windows).resolve()
    if not request.is_file():
        raise FileNotFoundError(f"outcome calibration request does not exist: {request}")
    command = " && ".join(
        [
            "source /home/lc999/miniconda3/etc/profile.d/conda.sh",
            f"conda activate {shlex.quote(args.conda_env)}",
            "export MALLOC_ARENA_MAX=2",
            "export PYTHONUNBUFFERED=1",
            f"cd {shlex.quote(args.repository_root_wsl)}",
            f"python scripts/wsl/advisory_outcome_calibration_train.py --request {shlex.quote(args.request_wsl)}",
        ]
    )
    return int(subprocess.run(["wsl", "bash", "-lc", command], check=False).returncode)


if __name__ == "__main__":
    raise SystemExit(main())
