"""Explicit offline L2 risk preparation and two fresh-process fits. No services."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2 as risk  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_effect import verify_receipt  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    numeric_environment,
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "run", "child"))
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--outcome-facts", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-parent", type=Path)
    parser.add_argument("--request-sha256")
    parser.add_argument("--outcome-sha256")
    parser.add_argument("--executor-commit")
    args = parser.parse_args()
    output = validate_output_location(args.output)
    try:
        if args.mode == "prepare":
            if args.work_parent is None:
                parser.error("prepare requires --work-parent")
            if output.exists():
                raise risk.fail("output directory already exists", "output_collision")
            head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
            features, facts = risk.prepare_file_inputs(args.request, work_parent=args.work_parent, source_commit=head)
            write_once(output / "features.json", features)
            write_once(output / "outcome_facts.json", facts)
            print(f"file-only prepared; features={features['receipt_sha256']}; fits=0; tail=false")
        elif args.mode == "child":
            if (
                not args.request_sha256
                or not args.outcome_sha256
                or args.outcome_facts is None
                or not args.executor_commit
            ):
                parser.error("child requires parent feature/outcome hashes and explicit outcome facts")
            panel = read_json(args.request)
            verify_receipt(panel, args.request_sha256)
            head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
            if head != args.executor_commit:
                raise risk.fail("fresh child source commit differs", "identity_mismatch")
            import hmmlearn.hmm  # noqa: F401

            environment = numeric_environment()
            environment = {k: environment[k] for k in ("versions", "thread_variables", "thread_pools")}
            environment["thread_pools"] = [
                {k: v for k, v in p.items() if k != "filepath"} for p in environment["thread_pools"]
            ]
            sealed = risk.fit_predict(panel)
            # Durable prediction readback precedes the first development target access.
            write_once(output.with_suffix(".sealed.json"), sealed)
            if canonical_json_bytes(read_json(output.with_suffix(".sealed.json"))) != canonical_json_bytes(sealed):
                raise risk.fail("sealed prediction readback differs", "identity_mismatch")
            facts = read_json(args.outcome_facts)
            verify_receipt(facts, args.outcome_sha256)
            if (
                facts.get("schema_version") != risk.VERSION + "_outcome_facts"
                or facts.get("feature_sha256") != panel["receipt_sha256"]
                or facts.get("calendar") != panel["calendar"]
                or facts.get("catalog") != panel["catalog"]
                or facts.get("tail_accessed") is not False
            ):
                raise risk.fail("outcome parent identity differs", "identity_mismatch")
            plan = risk.schedule(panel["calendar"])
            labels = risk.drawdown_outcomes(
                panel["calendar"], plan["dev"], panel["catalog"], facts["returns"], risk.DEV_END
            )
            result = risk.evaluate(sealed, labels, panel["calendar"])
            write_once(
                output,
                receipt(
                    {
                        "schema_version": risk.VERSION + "_repeat",
                        "contract": risk.CONTRACT,
                        "numeric_environment": environment,
                        "sealed": sealed,
                        "result": result,
                        "outcome_sha256": facts["receipt_sha256"],
                        "executor_commit": head,
                    }
                ),
            )
        else:
            if args.outcome_facts is None:
                parser.error("run requires --outcome-facts")
            panel, facts = read_json(args.request), read_json(args.outcome_facts)
            verify_receipt(panel)
            verify_receipt(facts)
            head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
            if subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip():
                raise risk.fail("formal run requires immutable clean source", "identity_mismatch")
            if output.exists():
                raise risk.fail("output must be new", "output_collision")
            output.mkdir(parents=True)
            env = {**os.environ, **{k: "1" for k in THREAD_VARIABLES}, "PYTHONPATH": str(ROOT)}
            paths = []
            for number in (1, 2):
                child_output = output / f"process_{number}.json"
                with (output / f"process_{number}.log").open("xb") as log:
                    child = subprocess.run(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            "child",
                            "--request",
                            str(args.request),
                            "--outcome-facts",
                            str(args.outcome_facts),
                            "--output",
                            str(child_output),
                            "--request-sha256",
                            panel["receipt_sha256"],
                            "--outcome-sha256",
                            facts["receipt_sha256"],
                            "--executor-commit",
                            head,
                        ],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=False,
                    )
                if child.returncode:
                    raise risk.fail(f"child {number} exited {child.returncode}", "child_failed")
                paths.append(child_output)
            final = risk.close_processes(
                *(read_json(p) for p in paths),
                feature_sha256=panel["receipt_sha256"],
                outcome_sha256=facts["receipt_sha256"],
                executor_commit=head,
            )
            write_once(output / "acceptance.json", final)
            print(f"{final['effect_status']}; fits={final['completed_fits']}; output={output / 'acceptance.json'}")
    except Exception as exc:
        failure = receipt(
            {
                "status": "FAILED",
                "reason_code": getattr(exc, "reason_code", "hmm_risk_l2_risk_execution_failed"),
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "database_write": False,
                "runtime_action": False,
                "tail_accessed": False,
            }
        )
        write_once(output.with_name(output.name + ".failure.json"), failure)
        print(f"FAILED: {failure['reason_code']}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
