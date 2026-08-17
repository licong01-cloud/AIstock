"""Run an approved P2-3B or P2-3C Ridge candidate on read-only inputs."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

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
    REQUEST_SCHEMA_VERSION,
    REASON_HOLDOUT,
    REASON_INPUT_IDENTITY,
    RidgeCandidateError,
    canonical_json_bytes,
    failure_report,
    preflight_output_path,
    report_for_write,
    run_p2_3b_candidate,
    run_p2_3c_candidate,
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


def _load_request(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicate_keys)
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
    }
    if candidate_mode not in expected:
        raise RidgeCandidateError(REASON_INPUT_IDENTITY, "candidate mode is invalid", stage="input")
    expected_schema, expected_contract = expected[candidate_mode]
    if request.get("schema_version") != expected_schema or request.get("contract_version") != expected_contract:
        raise RidgeCandidateError(REASON_INPUT_IDENTITY, "request identity is invalid", stage="input")
    if candidate_mode == "p2-3c":
        validate_p2_3c_static_request(request)
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-mode", choices=("p2-3b", "p2-3c"), required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--db-env-prefix", required=True)
    args = parser.parse_args(argv)

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
