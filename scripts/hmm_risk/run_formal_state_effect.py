"""Formal zero-fit neutral re-score of the approved sealed L2 source."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import formal_state_effect as effect  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    read_json,
    validate_output_location,
    write_once,
    numeric_environment,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("score", "run", "child"))
    parser.add_argument("--source-acceptance", type=Path, required=True)
    parser.add_argument("--labels", type=Path)
    parser.add_argument("--source-request", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = validate_output_location(args.output)
    try:
        source = read_json(args.source_acceptance)
        if args.mode == "score":
            write_once(output, effect.neutral_rescore(source))
            print(f"prediction sealed; fits=0; target=false; output={output}")
        else:
            if args.labels is None or args.source_request is None:
                parser.error("run/child require existing --labels and --source-request; no reconstruction")
            if args.mode == "child":
                request = read_json(args.source_request)
                effect.verify_receipt(request)
                environment = numeric_environment()
                environment = {k: environment[k] for k in ("versions", "thread_variables", "thread_pools")}
                environment["thread_pools"] = [
                    {k: v for k, v in p.items() if k != "filepath"} for p in environment["thread_pools"]
                ]
                result = effect.neutral_effect_repeat(
                    source, read_json(args.labels), request["baseline"], numeric_environment=environment
                )
                write_once(output, result)
            else:
                effect.neutral_rescore(source)  # fail before creating outputs on source drift
                if output.exists():
                    raise effect.fail("output must be a new directory")
                output.mkdir(parents=True)
                env = {**os.environ, **{k: "1" for k in THREAD_VARIABLES}, "PYTHONPATH": str(ROOT)}
                paths = []
                for n in (1, 2):
                    child = output / f"process_{n}.json"
                    with (output / f"process_{n}.log").open("xb") as log:
                        result = subprocess.run(
                            [
                                sys.executable,
                                str(Path(__file__).resolve()),
                                "child",
                                "--source-acceptance",
                                str(args.source_acceptance),
                                "--source-request",
                                str(args.source_request),
                                "--labels",
                                str(args.labels),
                                "--output",
                                str(child),
                            ],
                            env=env,
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            check=False,
                        )
                    if result.returncode:
                        raise effect.fail(f"fresh process {n} failed", reason="child_failed")
                    paths.append(child)
                final = effect.close_neutral_processes(*(read_json(p) for p in paths))
                write_once(output / "acceptance.json", final)
                print(f"{final['result']['effect_status']}; fits=0; output={output / 'acceptance.json'}")
    except Exception as exc:
        write_once(
            output.with_name(output.name + ".failure.json"),
            receipt(
                {
                    "status": "FAILED",
                    "reason_code": getattr(exc, "reason_code", "hmm_risk_l2_effect_execution_failed"),
                    "message": str(exc),
                    "fits": 0,
                    "tail_accessed": False,
                    "database_write": False,
                    "runtime_action": False,
                }
            ),
        )
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
