"""Run an approved P2-3B or P2-3C Ridge candidate on read-only inputs."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk.market_relative_ridge_candidate import (  # noqa: E402
    CONTRACT_VERSION,
    DEVELOPMENT_END,
    DEVELOPMENT_START,
    HOLDOUT_END,
    HOLDOUT_START,
    P2_3C_CONTRACT_VERSION,
    P2_3C_REQUEST_SCHEMA_VERSION,
    RL1_CONTRACT_VERSION,
    RL1_ALGORITHM_VERSION,
    RL1_DEVELOPMENT_END,
    RL1_DEVELOPMENT_START,
    RL1_REQUEST_SCHEMA_VERSION,
    RL1_REPORT_SCHEMA_VERSION,
    REQUEST_SCHEMA_VERSION,
    REASON_HOLDOUT,
    REASON_INPUT_IDENTITY,
    REASON_RL1_INPUT,
    RidgeCandidateError,
    canonical_json_bytes,
    canonical_sha256,
    close_c012_rl1_candidate_children,
    failure_report,
    preflight_output_path,
    report_for_write,
    run_p2_3b_candidate,
    run_p2_3c_candidate,
    run_c012_rl1_candidate_process,
    validate_c012_rl1_static_request,
    validate_p2_3c_static_request,
    write_report,
)
from backend.services.hmm_risk.state_model_set import StateModelSetError  # noqa: E402
from scripts.hmm_risk.prepare_state_model_set import _load_l1_source_inputs  # noqa: E402


def _reject_duplicate_keys(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant: {value}")


def _load_request(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite_constant,
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise RidgeCandidateError(
            REASON_INPUT_IDENTITY,
            "request JSON cannot be read canonically",
            stage="input",
            evidence={"exception_type": type(exc).__name__},
        ) from exc
    if not isinstance(value, dict):
        raise RidgeCandidateError(REASON_INPUT_IDENTITY, "request must be an object", stage="input")
    return value


def _producer_commit() -> str:
    status = subprocess.run(
        ["git", "-C", str(ROOT), "status", "--porcelain"],
        check=True,
        capture_output=True,
        text=True,
    )
    if status.stdout.strip():
        raise RidgeCandidateError(
            REASON_INPUT_IDENTITY,
            "producer worktree must be clean",
            stage="input",
        )
    return subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _loader_request(request: dict[str, Any], candidate_mode: str) -> dict[str, Any]:
    expected = {
        "p2-3b": (REQUEST_SCHEMA_VERSION, CONTRACT_VERSION),
        "p2-3c": (P2_3C_REQUEST_SCHEMA_VERSION, P2_3C_CONTRACT_VERSION),
        "c012-rl1": (RL1_REQUEST_SCHEMA_VERSION, RL1_CONTRACT_VERSION),
    }
    if candidate_mode not in expected:
        raise RidgeCandidateError(REASON_INPUT_IDENTITY, "candidate mode is invalid", stage="input")
    expected_schema, expected_contract = expected[candidate_mode]
    if request.get("schema_version") != expected_schema or request.get("contract_version") != expected_contract:
        raise RidgeCandidateError(REASON_INPUT_IDENTITY, "request identity is invalid", stage="input")
    if candidate_mode == "p2-3c":
        validate_p2_3c_static_request(request)
    if candidate_mode == "c012-rl1":
        validate_c012_rl1_static_request(request)
        source = request.get("source")
        if not isinstance(source, dict):
            raise RidgeCandidateError(REASON_INPUT_IDENTITY, "request source is missing", stage="input")
        family = {
            "train_start": RL1_DEVELOPMENT_START.isoformat(),
            "train_end": RL1_DEVELOPMENT_END.isoformat(),
        }
        return {"source": source, "families": [dict(family), dict(family)]}
    if (
        request.get("holdout_start") != HOLDOUT_START.isoformat()
        or request.get("holdout_end") != HOLDOUT_END.isoformat()
    ):
        raise RidgeCandidateError(REASON_HOLDOUT, "request holdout boundary is invalid", stage="input")
    source = request.get("source")
    if not isinstance(source, dict):
        raise RidgeCandidateError(REASON_INPUT_IDENTITY, "request source is missing", stage="input")
    if str(source.get("source_end") or "") != DEVELOPMENT_END.isoformat():
        raise RidgeCandidateError(
            REASON_HOLDOUT,
            f"{candidate_mode} source_end must stop at the development boundary",
            stage="input",
        )
    try:
        source_start = date.fromisoformat(str(source.get("source_start") or ""))
    except ValueError as exc:
        raise RidgeCandidateError(REASON_INPUT_IDENTITY, "source_start must be an ISO date", stage="input") from exc
    if source_start > DEVELOPMENT_START:
        raise RidgeCandidateError(
            REASON_INPUT_IDENTITY,
            "source_start does not cover the development boundary",
            stage="input",
        )
    family = {"train_start": DEVELOPMENT_START.isoformat(), "train_end": DEVELOPMENT_END.isoformat()}
    return {"source": source, "families": [dict(family), dict(family)]}


def _failure_path(output: Path) -> Path:
    return output.with_name(f"{output.stem}.failure.json")


def _child_path(directory: Path, index: int) -> Path:
    return directory.resolve() / f"rotation_l1_candidate.fresh_process_{index}.json"


def _c012_artifact_outputs(args: argparse.Namespace) -> dict[str, str]:
    candidate = args.output.resolve()
    first = _child_path(args.child_dir, 1)
    second = _child_path(args.child_dir, 2)
    return {
        "candidate_output": str(candidate),
        "candidate_failure_output": str(_failure_path(candidate)),
        "child_1_output": str(first),
        "child_1_failure_output": str(_failure_path(first)),
        "child_2_output": str(second),
        "child_2_failure_output": str(_failure_path(second)),
    }


def _load_c012_child_failure(
    path: Path, *, producer_commit: str, request: Mapping[str, Any], process_index: int
) -> dict[str, Any]:
    value = _load_request(path)
    report_hash = str(value.get("report_sha256") or "")
    if (
        len(report_hash) != 64
        or canonical_sha256({key: item for key, item in value.items() if key != "report_sha256"}) != report_hash
    ):
        raise RidgeCandidateError(
            "hmm_risk_rotation_l1_fresh_process_mismatch",
            "child failure receipt hash is invalid",
            stage="fresh_process",
        )
    evidence = value.get("failure_evidence")
    reason = value.get("failure_reason_code")
    stage = value.get("failure_stage")
    if (
        value.get("schema_version") != RL1_REPORT_SCHEMA_VERSION
        or value.get("contract_version") != RL1_CONTRACT_VERSION
        or value.get("algorithm_version") != RL1_ALGORITHM_VERSION
        or value.get("status") != "ROTATION_L1_NOT_AVAILABLE"
        or value.get("producer_commit") != producer_commit
        or value.get("request_sha256") != canonical_sha256(request)
        or value.get("process_index") != process_index
        or value.get("planned_fit_count") != 24
        or not isinstance(value.get("completed_fit_count"), int)
        or not 0 <= int(value["completed_fit_count"]) <= 12
        or value.get("holdout_accessed") is not False
        or value.get("selection_performed") is not False
        or value.get("partial_component_selection_performed") is not False
        or value.get("product_acceptance_performed") is not False
        or value.get("candidate_receipt_write") is not False
        or value.get("failure_receipt_write") is not True
        or value.get("model_write") is not False
        or value.get("bundle_write") is not False
        or value.get("ready_write") is not False
        or value.get("database_write") is not False
        or value.get("runtime_action") is not False
        or not isinstance(reason, str)
        or not reason.startswith("hmm_risk_rotation_l1_")
        or not isinstance(stage, str)
        or not stage
        or not isinstance(evidence, Mapping)
    ):
        raise RidgeCandidateError(
            "hmm_risk_rotation_l1_fresh_process_mismatch",
            "child failure receipt authority is invalid",
            stage="fresh_process",
        )
    return value


def _run_c012_rl1(args: argparse.Namespace) -> int:
    request: dict[str, Any] = {}
    producer_commit = "unknown"
    output = _child_path(args.child_dir, int(args.child_index)) if args.child_index else args.output.resolve()
    failure_output = _failure_path(output)
    try:
        output = preflight_output_path(output, repository_root=ROOT)
        request = _load_request(args.request.resolve())
        validate_c012_rl1_static_request(request)
        failure_key = (
            "child_{}_failure_output".format(int(args.child_index)) if args.child_index else "candidate_failure_output"
        )
        failure_output = Path(str(request["artifact_outputs"][failure_key]))
        if request.get("artifact_outputs") != _c012_artifact_outputs(args):
            raise RidgeCandidateError(
                REASON_RL1_INPUT,
                "rotation L1 CLI outputs differ from request authority",
                stage="input",
            )
        producer_commit = _producer_commit()
        if args.child_index is not None:
            loader_request = _loader_request(request, "c012-rl1")
            try:
                inputs = _load_l1_source_inputs(loader_request, db_prefix=str(args.db_env_prefix), c010_formal=True)
            except StateModelSetError as exc:
                raise RidgeCandidateError(
                    "hmm_risk_rotation_l1_input_identity_mismatch",
                    str(exc),
                    stage="source_preflight",
                    evidence={"exception_type": type(exc).__name__},
                ) from exc
            child = run_c012_rl1_candidate_process(
                inputs,
                request,
                producer_commit=producer_commit,
                process_index=int(args.child_index),
            )
            write_report(output, child, repository_root=ROOT)
            sys.stdout.buffer.write(canonical_json_bytes({"status": child["status"], "output": str(output)}) + b"\n")
            return 0

        child_paths = [_child_path(args.child_dir, index) for index in (1, 2)]
        for path in child_paths:
            preflight_output_path(path, repository_root=ROOT)
        environment = os.environ.copy()
        for key in (
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
            "VECLIB_MAXIMUM_THREADS",
            "NUMEXPR_NUM_THREADS",
        ):
            environment[key] = "1"
        base = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--candidate-mode",
            "c012-rl1",
            "--request",
            str(args.request.resolve()),
            "--output",
            str(args.output.resolve()),
            "--child-dir",
            str(args.child_dir.resolve()),
            "--db-env-prefix",
            str(args.db_env_prefix),
        ]
        for index in (1, 2):
            completed = subprocess.run(
                [*base, "--child-index", str(index)],
                cwd=ROOT,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            if completed.returncode != 0:
                failure_path = _failure_path(child_paths[index - 1])
                child_failure = (
                    _load_c012_child_failure(
                        failure_path,
                        producer_commit=producer_commit,
                        request=request,
                        process_index=index,
                    )
                    if failure_path.exists()
                    else {}
                )
                raise RidgeCandidateError(
                    str(child_failure.get("failure_reason_code") or "hmm_risk_rotation_l1_fresh_process_mismatch"),
                    "rotation L1 fresh process failed",
                    stage=str(child_failure.get("failure_stage") or "fresh_process"),
                    evidence={
                        "process_index": index,
                        "returncode": completed.returncode,
                        "child_failure_receipt": str(failure_path),
                        "child_failure_report_sha256": child_failure.get("report_sha256"),
                        "child_failure_evidence": dict(child_failure.get("failure_evidence") or {}),
                    },
                )
        first, second = (_load_request(path) for path in child_paths)
        report = report_for_write(
            close_c012_rl1_candidate_children(
                first,
                second,
                request=request,
                producer_commit=producer_commit,
            ),
            failure=False,
        )
        write_report(output, report, repository_root=ROOT)
        sys.stdout.buffer.write(canonical_json_bytes({"status": report["status"], "output": str(output)}) + b"\n")
        return 0
    except Exception as exc:
        completed = int(exc.evidence.get("completed_fit_count") or 0) if isinstance(exc, RidgeCandidateError) else 0
        report = report_for_write(
            failure_report(
                request,
                producer_commit=producer_commit,
                error=exc,
                completed_fit_count=completed,
                candidate_mode="c012-rl1",
                process_index=int(args.child_index) if args.child_index is not None else None,
            ),
            failure=True,
        )
        try:
            path = write_report(failure_output, report, repository_root=ROOT)
        except Exception as write_exc:
            sys.stderr.write(
                "c012-rl1 candidate failed and failure receipt could not be written: "
                f"{type(write_exc).__name__}: {write_exc}\n"
            )
            return 2
        sys.stderr.write(f"c012-rl1 candidate failed: {report['failure_reason_code']}; receipt={path}\n")
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-mode", choices=("p2-3b", "p2-3c", "c012-rl1"), required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--db-env-prefix", required=True)
    parser.add_argument("--child-dir", type=Path)
    parser.add_argument("--child-index", type=int, choices=(1, 2))
    args = parser.parse_args(argv)

    if args.candidate_mode == "c012-rl1":
        if args.child_dir is None:
            parser.error("--child-dir is required for c012-rl1")
        return _run_c012_rl1(args)
    if args.child_dir is not None or args.child_index is not None:
        parser.error("--child-dir/--child-index are only valid for c012-rl1")

    request: dict[str, Any] = {}
    producer_commit = "unknown"
    output = args.output
    try:
        output = preflight_output_path(args.output, repository_root=ROOT)
        request = _load_request(args.request.resolve())
        loader_request = _loader_request(request, str(args.candidate_mode))
        producer_commit = _producer_commit()
        try:
            inputs = _load_l1_source_inputs(loader_request, db_prefix=str(args.db_env_prefix), c010_formal=True)
        except StateModelSetError as exc:
            raise RidgeCandidateError(
                REASON_INPUT_IDENTITY,
                str(exc),
                stage="source_preflight",
                evidence={"exception_type": type(exc).__name__},
            ) from exc
        runner = run_p2_3b_candidate if args.candidate_mode == "p2-3b" else run_p2_3c_candidate
        report = report_for_write(runner(inputs, request, producer_commit=producer_commit), failure=False)
        path = write_report(output, report, repository_root=ROOT)
        sys.stdout.buffer.write(canonical_json_bytes({"status": report["status"], "output": str(path)}))
        sys.stdout.buffer.write(b"\n")
        return 0
    except Exception as exc:
        completed = 0
        if isinstance(exc, RidgeCandidateError):
            completed = int(exc.evidence.get("completed_fit_count") or 0)
        report = report_for_write(
            failure_report(
                request,
                producer_commit=producer_commit,
                error=exc,
                completed_fit_count=completed,
                candidate_mode=str(args.candidate_mode),
            ),
            failure=True,
        )
        failure_path = _failure_path(output)
        try:
            path = write_report(failure_path, report, repository_root=ROOT)
        except Exception as write_exc:
            sys.stderr.write(
                f"{args.candidate_mode} candidate failed and failure receipt could not be written: "
                f"{type(write_exc).__name__}: {write_exc}\n"
            )
            return 2
        sys.stderr.write(f"{args.candidate_mode} candidate failed: {report['failure_reason_code']}; receipt={path}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
