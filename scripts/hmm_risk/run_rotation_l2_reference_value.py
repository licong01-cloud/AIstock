"""Run two fresh zero-fit reference replays; one result, no service activation."""

from __future__ import annotations

import argparse
import importlib.abc
import json
from pathlib import Path
import re
import socket
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from scripts.hmm_risk import rotation_l2_reference_value as value  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import read_json, validate_output_location, write_once  # noqa: E402


class NoComputeImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(
            fullname == n or fullname.startswith(n + ".")
            for n in (
                "sklearn",
                "hmmlearn",
                "psycopg",
                "psycopg2",
                "backend.db",
                "backend.data_service",
            )
        ):
            raise RuntimeError("zero-fit/file-only guard: " + fullname)
        return None


def forbidden(*args, **kwargs):
    raise RuntimeError("zero-fit/file-only guard: fit/filter/predict/database/network is forbidden")


def install_zero_compute_guards() -> None:
    sys.meta_path.insert(0, NoComputeImports())
    socket.create_connection = forbidden
    socket.socket = forbidden
    for api in (value.ridge, value.rank_model, value.return_model):
        api.run_process = forbidden
        api.training_matrix = forbidden
        api.predictions_from_parameters = forbidden


def source_head() -> str:
    head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
    value.require(bool(re.fullmatch("[0-9a-f]{40}", head)), "executor SHA invalid")
    return head


def require_clean_source() -> None:
    paths = ["scripts/hmm_risk/rotation_l2_reference_value.py", "scripts/hmm_risk/run_rotation_l2_reference_value.py"]
    tracked = subprocess.run(["git", "-C", str(ROOT), "ls-files", "--error-unmatch", *paths], capture_output=True)
    value.require(tracked.returncode == 0, "executor files are not committed")
    dirty = subprocess.check_output(
        ["git", "-C", str(ROOT), "status", "--porcelain", "--", "scripts/hmm_risk", "backend/services/hmm_risk"],
        text=True,
    )
    value.require(not dirty.strip(), "executor files differ from declared source commit")


def validate_child(result, head) -> None:
    value.ridge.verify(result, "result_sha256")
    value.require(
        result.get("schema_version") == value.VERSION + "_result"
        and result.get("contract") == value.CONTRACT
        and result.get("executor_commit") == head
        and result.get("zero_compute_poison_active") is True
        and all(
            type(result.get(k)) is int and result[k] == 0 for k in ("new_fits", "new_filter_calls", "new_predict_calls")
        )
        and all(
            result.get(k) is False
            for k in ("tail_accessed", "database_access", "dataset_write", "runtime_action", "production_adoption")
        ),
        "child zero-fit envelope differs",
    )
    pins = result.get("source_pins", {})
    value.require(
        pins.get("rank") == value.RANK_PINS
        and pins.get("return") == value.RETURN_PINS
        and pins.get("original_input_hash") == value.return_model.ORIGINAL_INPUT_HASH
        and pins.get("sector_count") == 131
        and pins.get("mature_decisions") == 222
        and result.get("decision_start") == value.CONTRACT["decision_start"]
        and result.get("decision_end") == value.CONTRACT["decision_end"]
        and result.get("valuation_end") == value.CONTRACT["valuation_end"]
        and result.get("decision_count") == 222
        and result.get("valuation_days") == 232
        and len(result.get("daily_population", {})) == 222
        and set(result.get("cost_paths", {})) == {str(c) for c in value.COSTS},
        "child sealed authority/date/cost directory differs",
    )
    for paths in result["cost_paths"].values():
        value.require(
            set(paths.get("arms", {})) == set(value.ARMS) and len(paths.get("paired", {})) == 5,
            "child four-arm directory differs",
        )
        calendar = None
        for path in paths["arms"].values():
            days = [r["trade_date"] for r in path.get("daily", [])]
            value.require(
                len(days) == 232
                and days == sorted(set(days))
                and days[0] == result["decision_start"]
                and days[-1] == result["valuation_end"],
                "child valuation calendar differs",
            )
            if calendar is not None:
                value.require(days == calendar, "child arm calendars differ")
            calendar = days


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("run", "child"))
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--rank-acceptance", type=Path, required=True)
    parser.add_argument("--return-acceptance", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--executor-commit", required=True)
    args = parser.parse_args(argv)
    output = None
    try:
        head = source_head()
        value.require(head == args.executor_commit, "source differs from explicit executor commit")
        require_clean_source()
        if args.mode == "child":
            install_zero_compute_guards()
            result = value.execute(
                input_path=args.input,
                rank_path=args.rank_acceptance,
                return_path=args.return_acceptance,
                executor_commit=head,
            )
            result = value.ridge.seal(
                {**{k: v for k, v in result.items() if k != "result_sha256"}, "zero_compute_poison_active": True},
                "result_sha256",
            )
            validate_child(result, head)
            sys.stdout.buffer.write(canonical_json_bytes(result))
            return 0
        value.require(args.output is not None, "run requires an explicit output directory")
        output = validate_output_location(args.output)
        value.require(not output.exists(), "output already exists; immutable results cannot be overwritten")
        # No output is created before both independent results have passed readback.
        children = []
        for _ in range(2):
            child = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "child",
                    "--input",
                    str(args.input),
                    "--rank-acceptance",
                    str(args.rank_acceptance),
                    "--return-acceptance",
                    str(args.return_acceptance),
                    "--executor-commit",
                    head,
                ],
                cwd=ROOT,
                capture_output=True,
                check=False,
            )
            value.require(
                child.returncode == 0,
                "fresh child failed: " + child.stderr.decode("utf-8", errors="replace"),
                "execution_failed",
            )
            result = json.loads(child.stdout)
            validate_child(result, head)
            children.append(result)
        value.require(source_head() == head, "source changed during replay")
        require_clean_source()
        value.require(
            canonical_json_bytes(children[0]) == canonical_json_bytes(children[1]),
            "fresh process payloads differ",
            "repeat_mismatch",
        )
        report = value.ridge.seal(
            {
                "schema_version": value.VERSION + "_acceptance",
                "execution_status": "COMPLETED",
                "executor_commit": head,
                "fresh_process_bitwise_equal": True,
                "numeric_tolerance_used": False,
                "planned_fits": 0,
                "completed_fits": 0,
                "result": children[0],
            },
            "acceptance_sha256",
        )
        validate_output_location(output)
        write_once(output / "acceptance.json", report)
        value.require(read_json(output / "acceptance.json") == report, "final result readback differs")
        print(
            json.dumps(
                {
                    "status": children[0]["status"],
                    "acceptance_sha256": report["acceptance_sha256"],
                    "fresh_process_bitwise_equal": True,
                    "new_fits": 0,
                }
            )
        )
        return 0
    except Exception as exc:
        failure = value.ridge.seal(
            {
                "schema_version": value.VERSION + "_failure",
                "execution_status": "FAILED",
                "reason_code": getattr(exc, "reason_code", "hmm_risk_rotation_l2_value_execution_failed"),
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "executor_commit": args.executor_commit,
                "new_fits": 0,
            },
            "failure_sha256",
        )
        if output is not None:
            try:
                failure_path = validate_output_location(output.with_name(output.name + ".failure.json"))
                write_once(failure_path, failure)
            except Exception as receipt_error:
                failure = value.ridge.seal(
                    {
                        **{k: v for k, v in failure.items() if k != "failure_sha256"},
                        "failure_receipt_error": str(receipt_error),
                    },
                    "failure_sha256",
                )
        print(json.dumps(failure, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
