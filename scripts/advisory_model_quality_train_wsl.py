from __future__ import annotations

import argparse
import shlex
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the complete M5A train/winner/test/bundle flow in WSL.")
    parser.add_argument("--request-windows", required=True)
    parser.add_argument("--request-wsl", required=True)
    parser.add_argument("--projection-receipt-wsl", required=True)
    parser.add_argument("--test-request-wsl", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--model-root-wsl", required=True)
    parser.add_argument("--conda-env", default="rdagent-gpu")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not Path(args.request_windows).resolve().is_file():
        raise FileNotFoundError("M5A train request does not exist")
    base = "python scripts/wsl/advisory_model_quality_train.py"
    command = " && ".join(
        [
            "source /home/lc999/miniconda3/etc/profile.d/conda.sh",
            f"conda activate {shlex.quote(args.conda_env)}",
            "export MALLOC_ARENA_MAX=2",
            "export PYTHONUNBUFFERED=1",
            f"cd {shlex.quote(args.repository_root_wsl)}",
            f"{base} stage-a --request {shlex.quote(args.request_wsl)}",
            (
                f"{base} prepare-test --train-request {shlex.quote(args.request_wsl)} "
                f"--projection-receipt {shlex.quote(args.projection_receipt_wsl)} "
                f"--test-request-output {shlex.quote(args.test_request_wsl)}"
            ),
            (
                f"{base} stage-b --request {shlex.quote(args.test_request_wsl)} "
                f"--train-request {shlex.quote(args.request_wsl)}"
            ),
            (
                f"{base} publish --test-request {shlex.quote(args.test_request_wsl)} "
                f"--train-request {shlex.quote(args.request_wsl)} "
                f"--model-root {shlex.quote(args.model_root_wsl)}"
            ),
        ]
    )
    completed = subprocess.run(["wsl", "bash", "-lc", command], check=False)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
